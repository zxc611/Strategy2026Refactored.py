from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

import numpy as np

from ali2026v3_trading.infra.shared_utils import safe_int, safe_float
from ali2026v3_trading.resilience_utils import (
    stable_sum, stable_mean, stable_variance,
    safe_normalize_weights, FLOAT_COMPARE_TOLERANCE,
)


def _safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_float] Error getting %s: %s", attr, e)
        return default


def _safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return int(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_int] Error getting %s: %s", attr, e)
        return default


class RiskComputeService:
    """CC-03/AP-02修复: 风控计算服务

    从RiskService提取的B类方法（风控计算/指标），
    包含压力测试、PnL归因、风险指标计算、决策评分等。
    构造函数接受risk_service引用（用于访问共享状态）。
    """

    RISK_DIMENSION_DEFAULTS = {
        'state_strength':      0.15,
        'order_flow':          0.10,
        'cycle_resonance':     0.10,
        'tri_validation':      0.10,
        'life_expectancy':     0.10,
        'phase_quality':       0.08,
        'greeks_usage':        0.07,
        'asymmetric_drawdown': 0.05,
        'consecutive_loss':    0.07,
        'alpha_decay':         0.10,
        'cross_correlation':   0.08,
    }

    def __init__(self, risk_service: Any):
        self._rs = risk_service

    def _compute_stress_test(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        """R17-P1-DOC-P1-04修复: 计算压力测试。Args: calc: Greeks计算器; positions_dict: 持仓字典; net_delta/gamma/vega/theta: 净Greeks值。Returns: Dict含jump_1sigma_pnl/jump_2sigma_pnl/L1_trigger_prob"""
        L1_JUMP_PROB_HIGH = 0.05
        L1_JUMP_PROB_LOW = 0.01

        try:
            jump_1sigma_pnl = 0.0
            jump_2sigma_pnl = 0.0

            with calc.get_lock():
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    future_product = info.get('future_product', '')
                    multiplier = calc._contract_multiplier.get(future_product, 1)

                    underlying_price = 0.0
                    iv = calc._option_iv.get(inst_id, 0.2)
                    if iv <= 0:
                        iv = 0.2
                    try:
                        from ali2026v3_trading.data.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            underlying_id = info.get('underlying_future_id')
                            if underlying_id:
                                underlying_price = ds.realtime_cache.get_latest_price(str(underlying_id)) or 0.0
                    except (ImportError, AttributeError) as e:
                        logging.debug("[RiskService] underlying price fetch failed: %s", e)

                    if underlying_price <= 0:
                        continue

                    moneyness = 0.0
                    strike = info.get('strike_price', 0.0)
                    if strike > 0 and underlying_price > 0:
                        moneyness = abs(underlying_price - strike) / underlying_price
                    iv_adjusted = iv * (1.0 + 0.5 * moneyness)
                    sigma_1 = underlying_price * iv_adjusted * 0.01
                    sigma_2 = sigma_1 * 2

                    delta_contrib = greeks.get('delta', 0.0) * multiplier * size
                    gamma_contrib = 0.5 * greeks.get('gamma', 0.0) * multiplier * size

                    jump_1sigma_pnl += delta_contrib * sigma_1 + gamma_contrib * sigma_1 * sigma_1
                    jump_2sigma_pnl += delta_contrib * sigma_2 + gamma_contrib * sigma_2 * sigma_2

            l1_prob = L1_JUMP_PROB_HIGH if abs(jump_2sigma_pnl) > abs(jump_1sigma_pnl) * 3 else L1_JUMP_PROB_LOW

            return {
                "jump_1sigma_pnl": round(jump_1sigma_pnl, 2),
                "jump_2sigma_pnl": round(jump_2sigma_pnl, 2),
                "L1_trigger_prob": l1_prob,
            }
        except Exception as e:
            logging.warning("[GreekDashboard._compute_stress_test] Error: %s", e)
            return {"jump_1sigma_pnl": 0.0, "jump_2sigma_pnl": 0.0, "L1_trigger_prob": 0.0}

    def _compute_pnl_attribution(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        """R17-P1-DOC-P1-04修复: 计算PnL归因。Args: calc: Greeks计算器; positions_dict: 持仓字典; net_delta/gamma/vega/theta: 净Greeks值。Returns: Dict含delta/gamma/vega/theta贡献"""
        try:
            delta_contrib = 0.0
            gamma_contrib = 0.0
            vega_contrib = 0.0
            theta_contrib = 0.0

            with calc.get_lock():
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    future_product = info.get('future_product', '')
                    multiplier = calc._contract_multiplier.get(future_product, 1)

                    iv = calc._option_iv.get(inst_id, 0.2)
                    if iv <= 0:
                        iv = 0.2

                    delta_contrib += greeks.get('delta', 0.0) * multiplier * size * 0.01
                    gamma_contrib += 0.5 * greeks.get('gamma', 0.0) * multiplier * size * 0.0001
                    vega_contrib += greeks.get('vega', 0.0) * multiplier * size * (iv * 0.01)
                    theta_contrib += greeks.get('theta', 0.0) * multiplier * size

            total_explained = delta_contrib + gamma_contrib + vega_contrib + theta_contrib
            try:
                for _inst_id, _size in positions_dict.items():
                    if _size == 0:
                        continue
                    _info = calc._option_info_cache.get(_inst_id, {})
                    _entry_price = _info.get('entry_price', _info.get('strike', 0.0))
                    _exit_price = _info.get('exit_price', _info.get('underlying_price', _entry_price))
                    _fp = _info.get('future_product', '')
                    _mult = calc._contract_multiplier.get(_fp, 1)
                    _dir = 'BUY' if _size > 0 else 'SELL'
                    _simple_pnl = self.compute_simplified_pnl(_entry_price, _exit_price, abs(_size), _dir, _mult)
                    if abs(total_explained - _simple_pnl) > abs(total_explained) * 0.1 + 1.0:
                        logging.debug("[R16-P1-006] PnL计算偏差: complex=%.2f simple=%.2f diff=%.2f",
                                      total_explained, _simple_pnl, abs(total_explained - _simple_pnl))
                    break
            except Exception:
                pass
            total_pnl_base = abs(delta_contrib) + abs(gamma_contrib) + abs(vega_contrib) + abs(theta_contrib)
            denominator = total_explained if abs(total_explained) > 1e-10 else total_pnl_base
            unexplained = abs(total_pnl_base - abs(total_explained)) if total_pnl_base > 0 else 0.0
            unexplained_pct = (unexplained / abs(denominator) * 100) if abs(denominator) > 1e-10 else 0.0

            if unexplained_pct > 15.0:
                if not hasattr(self._rs, '_attribution_residual_history'):
                    self._rs._attribution_residual_history = []
                self._rs._attribution_residual_history.append(unexplained_pct)
                if len(self._rs._attribution_residual_history) >= 5:
                    recent = self._rs._attribution_residual_history[-20:]
                    mean_res = stable_mean(recent)
                    var_res = stable_variance(recent)
                    std_res = var_res ** 0.5 if var_res > 0 else 0.0
                    n = len(recent)
                    if std_res > 0:
                        t_stat = (mean_res - 15.0) / (std_res / math.sqrt(n))
                        statistically_significant = t_stat > 2.0
                    else:
                        statistically_significant = mean_res > 15.0
                    if statistically_significant:
                        logging.warning(
                            "[E7_PNL_RESIDUAL] P2-裂缝30: PnL归因残差统计显著! "
                            "均值=%.1f%%, t=%.2f, n=%d, 触发E7告警",
                            mean_res, t_stat if std_res > 0 else 0.0, n,
                        )
                    else:
                        logging.debug(
                            "[E7_PNL_RESIDUAL] P2-裂缝30: 残差%.1f%%>15%%但统计不显著, 降级为观察",
                            unexplained_pct,
                        )
                else:
                    logging.warning("[E7_PNL_RESIDUAL] PnL归因残差%.1f%%>15%%阈值(样本不足,暂触发E7)", unexplained_pct)

            return {
                "delta_contrib": round(delta_contrib, 2),
                "gamma_contrib": round(gamma_contrib, 2),
                "vega_contrib": round(vega_contrib, 2),
                "theta_contrib": round(theta_contrib, 2),
                "unexplained": round(unexplained, 2),
                "unexplained_pct": round(unexplained_pct, 2),
            }
        except Exception as e:
            logging.warning("[GreekDashboard._compute_pnl_attribution] Error: %s", e)
            return {
                "delta_contrib": 0.0, "gamma_contrib": 0.0,
                "vega_contrib": 0.0, "theta_contrib": 0.0,
                "unexplained": 0.0, "unexplained_pct": 0.0,
            }

    def compute_simplified_pnl(self, entry_price: float, exit_price: float,
                               volume: int, direction: str, multiplier: float = 1.0,
                               commission_per_lot: float = 0.0) -> float:
        if direction in ('BUY', 'buy', 'long'):
            raw_pnl = (exit_price - entry_price) * volume * multiplier
        else:
            raw_pnl = (entry_price - exit_price) * volume * multiplier
        return raw_pnl - commission_per_lot * abs(volume) * 2

    def calculate_risk_metrics(self):
        from ali2026v3_trading.risk.risk_service import RiskMetrics
        try:
            if self._rs.position_manager is None:
                return RiskMetrics()

            positions_raw = getattr(self._rs.position_manager, "positions", {})
            positions = dict(positions_raw) if positions_raw else {}

            total_exposure = 0.0
            max_position = 0.0
            count = 0

            for inst_map in positions.values():
                for inst_id, pos in inst_map.items():
                    volume = getattr(pos, "volume", 0)
                    price = getattr(pos, "open_price", 0)
                    multiplier = self._rs._get_contract_multiplier(inst_id)
                    exposure = abs(volume) * price * multiplier
                    total_exposure += exposure
                    max_position = max(max_position, exposure)
                    if volume != 0:
                        count += 1

            limit = self._rs._get_total_position_limit()
            risk_ratio = total_exposure / limit if limit > 0 else 0.0

            return RiskMetrics(
                total_exposure=total_exposure,
                max_single_position=max_position,
                position_count=count,
                risk_ratio=risk_ratio
            )

        except Exception as e:
            logging.error("[RiskService.calculate_risk_metrics] Error: %s, position_count=%d", e, count if 'count' in dir() else -1)
            return RiskMetrics()

    def compute_mode_position_size(
        self, equity: float, entry_price: float, stop_price: float,
        win_rate: float = 0.0, win_loss_ratio: float = 0.0,
        sortino_ratio: float = 0.0, calmar_ratio: float = 0.0, sharpe_ratio: float = 0.0,
        ofi: float = 0.0, cvd_divergence: float = 0.0, smart_money_flow: float = 0.0,
        delta: float = 0.0, gamma: float = 0.0, theta: float = 0.0, vega: float = 0.0,
        prediction_correct: bool = True, price_direction: int = 1,
    ) -> float:
        try:
            from ali2026v3_trading.mode_engine import ModeEngine, PredictiveStateEngine
            me = ModeEngine.get_instance()

            pse = PredictiveStateEngine.get_instance()
            state = pse.classify_state(prediction_correct, price_direction)
            position_multiplier = pse.get_position_multiplier(state)

            tvf_inputs = {
                'L1_risk_return': {'sortino': sortino_ratio, 'calmar': calmar_ratio, 'sharpe': sharpe_ratio},
                'L2_market_micro': {'ofi': ofi, 'cvd_divergence': cvd_divergence, 'smart_money_flow': smart_money_flow},
                'L3_option_greeks': {'delta': delta, 'gamma': gamma, 'theta': theta, 'vega': vega},
            }
            logging.debug("[P-22] TVF六维因子输入: %s", tvf_inputs)

            signal = getattr(self._rs, '_current_signal', {})
            _inst_type = signal.get('instrument_type', 'future') if isinstance(signal, dict) else 'future'
            _opt_discount = 0.6
            try:
                from ali2026v3_trading.config.config_params import get_cached_params
                _params = get_cached_params()
                _opt_discount = _params.get('option_buyer_discount', 0.6)
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass
            _market_state = state if state else ""
            base_size = me.compute_position_size(
                equity, entry_price, stop_price, win_rate, win_loss_ratio,
                sortino=sortino_ratio, calmar=calmar_ratio, sharpe=sharpe_ratio,
                ofi_score=ofi, cvd_divergence=cvd_divergence, smart_money_flow=smart_money_flow,
                delta_exposure=delta, gamma_risk=gamma, theta_decay=theta, vega_exposure=vega,
                instrument_type=_inst_type, option_discount=_opt_discount,
                market_state=_market_state,
            )

            adjusted_size = base_size * position_multiplier

            logging.debug("[P-07] PredictiveState: %s, multiplier=%.2f, base_size=%.2f, adjusted_size=%.2f",
                         state, position_multiplier, base_size, adjusted_size)

            return adjusted_size
        except (ImportError, AttributeError, ZeroDivisionError) as e:
            logging.debug("[RiskService] ModeEngine position size failed: %s", e)
            risk_pct = 0.02
            risk_amount = equity * risk_pct
            stop_distance = abs(entry_price - stop_price)
            if stop_distance < 1e-10:
                return 0.0
            return risk_amount / stop_distance

    def calculate_asymmetric_drawdown_limit(self, current_pnl: float, base_limit: float) -> float:
        if not self._rs._asymmetric_risk_enabled:
            return base_limit
        if current_pnl > 0:
            return base_limit * self._rs._asymmetric_profit_multiplier
        else:
            return base_limit * self._rs._asymmetric_loss_multiplier

    def compute_decision_score(
        self,
        state_strength: float,
        order_flow_consistency: float,
        hmm_state: Optional[str] = None,
        cr_output: Optional[Any] = None,
        greeks_dashboard: Optional[Dict[str, Any]] = None,
        consecutive_losses: int = 0,
        current_pnl: float = 0.0,
        drawdown_pct: float = 0.0,
        alpha_ratio: Optional[float] = None,
        cross_correlation: Optional[float] = None,
        tri_validation_score: Optional[float] = None,
        slippage_source: str = 'LIVE',
    ) -> Dict[str, Any]:
        w = {}
        for dim, default_w in self.RISK_DIMENSION_DEFAULTS.items():
            w[dim] = _safe_get_float(self._rs.params, f"decision.score.{dim}_weight",
                _safe_get_float(self._rs.params, f"decision_{dim}_weight", default_w))

        _LAYER_SIGNAL = ['state_strength', 'order_flow', 'cycle_resonance', 'tri_validation']
        _LAYER_MARKET = ['life_expectancy', 'phase_quality', 'greeks_usage', 'asymmetric_drawdown']
        _LAYER_PORTFOLIO = ['consecutive_loss', 'alpha_decay', 'cross_correlation']
        _signal_sum = stable_sum([w.get(d, 0.0) for d in _LAYER_SIGNAL])
        _market_sum = stable_sum([w.get(d, 0.0) for d in _LAYER_MARKET])
        _portfolio_sum = stable_sum([w.get(d, 0.0) for d in _LAYER_PORTFOLIO])
        _total_sum = stable_sum([_signal_sum, _market_sum, _portfolio_sum])
        if abs(_total_sum - 1.0) > 0.01:
            logging.warning(
                "[RiskService] P1-14: 三层权重偏差>0.01 (signal=%.3f market=%.3f portfolio=%.3f total=%.3f), 自动归一化",
                _signal_sum, _market_sum, _portfolio_sum, _total_sum)

        if slippage_source == 'BACKTEST':
            w['tri_validation'] = w.get('tri_validation', 0.10) * 0.5
            logging.debug("[RiskService] MS-P1-09: BACKTEST模式, D9权重折半=%.3f", w['tri_validation'])
        elif slippage_source == 'LIVE':
            w['tri_validation'] = w.get('tri_validation', 0.10) * 1.2
            logging.debug("[RiskService] MS-P1-09: LIVE模式, D9权重提升=%.3f", w['tri_validation'])

        scores = {}

        scores['state_strength'] = max(0.0, min(1.0, state_strength))

        base_order_flow = (order_flow_consistency + 1.0) / 2.0
        liquidity_score = self._compute_liquidity_score()
        try:
            from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
            _ofb = OrderFlowBridge()
            _imbalance = _ofb.get_instant_imbalance('')
            if isinstance(_imbalance, (int, float)) and abs(_imbalance) > 0.01:
                base_order_flow = base_order_flow * 0.7 + (1.0 - abs(_imbalance)) * 0.3
                logging.debug("[P1-1修复] OrderFlowBridge imbalance接入: imbalance=%.4f", _imbalance)
        except Exception:
            pass
        scores['order_flow'] = base_order_flow * 0.5 + liquidity_score * 0.5

        _hmm_state = hmm_state
        if _hmm_state is None:
            _hmm_state = getattr(self._rs, '_current_hmm_state', None)
        scores['life_expectancy'] = self._compute_life_score(_hmm_state)

        _pse_state = None
        try:
            from ali2026v3_trading.mode_engine import PredictiveStateEngine
            _pse = PredictiveStateEngine.get_instance()
            if _pse is not None:
                _pse_state = getattr(_pse, '_last_state', None)
                if _pse_state is None and hasattr(_pse, 'classify_state'):
                    _pse_state = _pse.classify_state(True, 1)
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass

        scores['cycle_resonance'] = self._compute_cycle_resonance_score(cr_output)
        scores['phase_quality'] = self._compute_phase_quality_score(cr_output)
        scores['greeks_usage'] = self._compute_greeks_usage_score(greeks_dashboard)
        scores['consecutive_loss'] = self._compute_consecutive_loss_score(consecutive_losses)
        scores['asymmetric_drawdown'] = self._compute_asymmetric_drawdown_score(
            current_pnl, drawdown_pct)
        scores['tri_validation'] = self._compute_tri_validation_score(tri_validation_score)

        _alpha_ratio = alpha_ratio
        if _alpha_ratio is None:
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _se = get_shadow_strategy_engine()
                if _se is not None:
                    if hasattr(_se, 'get_alpha_ratio') and callable(_se.get_alpha_ratio):
                        _alpha_ratio = _se.get_alpha_ratio()
                    elif hasattr(_se, 'alpha_decay_rate'):
                        _alpha_ratio = _se.alpha_decay_rate
            except Exception as e:
                logging.warning("[RiskService] R24-P0-TR-07: alpha_decay score computation failed: %s", e)
        scores['alpha_decay'] = self._compute_alpha_decay_score(_alpha_ratio)

        _cross_corr = cross_correlation
        if _cross_corr is None:
            _cross_corr = 0.5
            logging.warning("[P0-R9-04] cross_correlation为None, 降级为0.5(中性), 避免数据缺失时鼓励开仓")
        scores['cross_correlation'] = self._compute_cross_correlation_score(_cross_corr)

        total_weight = stable_sum(list(w.values()))
        if abs(total_weight) < FLOAT_COMPARE_TOLERANCE:
            logging.warning("[RiskService] 权重总和接近零, 使用等权重")
            n = len(w)
            uniform_w = {dim: 1.0 / n for dim in w}
            composite_score = stable_sum([scores[dim] * uniform_w[dim] for dim in w])
        else:
            normalized_w = safe_normalize_weights(w)
            composite_score = stable_sum([scores[dim] * normalized_w[dim] for dim in w])

        position_scale = self._compute_position_scale(composite_score, scores)

        threshold_high = _safe_get_float(self._rs.params, "decision.score.threshold_high",
            _safe_get_float(self._rs.params, "decision_threshold_high", 0.7))
        threshold_low = _safe_get_float(self._rs.params, "decision.score.threshold_low",
            _safe_get_float(self._rs.params, "decision_threshold_low", 0.4))

        if composite_score >= threshold_high:
            action = "normal_open"
        elif composite_score >= threshold_low:
            action = "small_open_tight_stop"
        elif composite_score >= 0.25:
            action = "divergence_warning"
        else:
            action = "no_open_wait"

        return {
            "decision_score": composite_score,
            "action": action,
            "position_scale": position_scale,
            "dimension_scores": scores,
            "dimension_weights": w,
            "state_strength": state_strength,
            "order_flow_consistency": order_flow_consistency,
            "existing_position_policy": "hold_to_original_stop" if action == "no_open_wait" else "normal",
            "allow_add_position": action not in ("no_open_wait", "divergence_warning"),
        }

    def _compute_position_scale(self, composite_score: float,
                                 scores: Dict[str, float]) -> float:
        """R17-P1-DOC-P1-04修复: 计算仓位缩放比例。Args: composite_score: 综合评分0.0~1.0; scores: 各维度评分字典。Returns: float 缩放比例0.0~1.0"""
        base_scale = max(0.0, min(1.0, composite_score))

        penalties = []

        if scores.get('life_expectancy', 0.5) < 0.3:
            penalties.append(0.3)
        elif scores.get('life_expectancy', 0.5) < 0.5:
            penalties.append(0.6)

        if scores.get('phase_quality', 0.5) < 0.3:
            penalties.append(0.5)

        if scores.get('consecutive_loss', 1.0) < 0.3:
            penalties.append(0.3)

        if scores.get('alpha_decay', 1.0) < 0.3:
            if not getattr(self._rs, '_alpha_decay_recovery_confirmed', False):
                penalties.append(0.4)
            else:
                logging.warning("[R24-P0-TR-07] Alpha衰减已人工确认恢复，跳过惩罚")  # R25-P1-TR-07修复: info→warning

        if penalties:
            min_penalty = min(penalties)
            base_scale *= min_penalty

        return round(base_scale, 4)

    def _compute_life_score(self, hmm_state: Optional[str] = None) -> float:
        """R17-P1-DOC-P1-04修复: 计算生命周期评分。Args: hmm_state: HMM状态字符串。Returns: float 0.0~1.0, 降级时返回0.5(跨策略Greeks聚合降级默认值)"""
        estimator = self._rs._get_life_estimator()
        if estimator is None or hmm_state is None:
            if estimator is None and hmm_state is not None:
                logging.warning("[R5-E-08] _compute_life_score降级: estimator不可用，返回中性评分0.5")
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        try:
            life = estimator.get_life_expectancy(hmm_state)
            if life is None or not life.is_valid():
                return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
            return {0: 1.0, 1: 0.7, 2: 0.4, 3: 0.2}.get(life.degradation_level, 0.5)
        except Exception as _life_e:
            logging.warning("[R5-E-08] _compute_life_score异常降级: %s，返回0.5", _life_e)
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)

    def _compute_cycle_resonance_score(self, cr_output: Optional[Any] = None) -> float:
        """R17-P1-DOC-P1-04修复: 计算周期共振评分。Args: cr_output: 周期共振输出对象。Returns: float 0.0~1.0, 降级时返回0.5(跨策略Greeks聚合降级默认值)"""
        if cr_output is None:
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        try:
            strength = getattr(cr_output, 'resonance_strength', None)
            if strength is not None:
                return max(0.0, min(1.0, float(strength)))
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)

    def _compute_phase_quality_score(self, cr_output: Optional[Any] = None) -> float:
        if cr_output is None:
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        try:
            phase = getattr(cr_output, 'phase', None)
            if phase is not None:
                phase_name = phase.name if hasattr(phase, 'name') else str(phase)
                return {'RELEASE': 1.0, 'CHARGE': 0.7, 'EXHAUST': 0.4, 'CHAOS': 0.2}.get(
                    phase_name.upper(), 0.5)
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)

    def compute_otm_sync_exposure(self, conn=None) -> Dict[str, Any]:
        """P1-8修复: 消费ticks_raw.is_otm和future_sync_status，计算OTM期权同步风险敞口"""
        try:
            if conn is None:
                from ali2026v3_trading.data.db_adapter import get_db_adapter
                conn = get_db_adapter()
            result = conn.execute("""
                SELECT
                    underlying_symbol,
                    option_type,
                    COUNT(*) FILTER (WHERE is_otm = true) AS otm_count,
                    COUNT(*) FILTER (WHERE future_sync_status = 'correct_rise') AS correct_rise_count,
                    COUNT(*) FILTER (WHERE future_sync_status = 'wrong_rise') AS wrong_rise_count,
                    COUNT(*) FILTER (WHERE future_sync_status = 'correct_fall') AS correct_fall_count,
                    COUNT(*) FILTER (WHERE future_sync_status = 'wrong_fall') AS wrong_fall_count
                FROM ticks_raw
                WHERE is_otm IS NOT NULL OR future_sync_status IS NOT NULL
                GROUP BY underlying_symbol, option_type
            """).fetchall()
            exposure = {}
            for row in result:
                symbol, opt_type, otm_cnt, cr, wr, cf, wf = row
                total_sync = cr + wr + cf + wf
                wrong_rate = (wr + wf) / total_sync if total_sync > 0 else 0.0
                exposure[f"{symbol}_{opt_type}"] = {
                    'otm_count': otm_cnt,
                    'wrong_sync_rate': wrong_rate,
                }
            logging.debug("[P1-8] OTM同步风险敞口: %d个标的组合", len(exposure))
            return {'otm_exposure': exposure, 'symbol_count': len(exposure)}
        except Exception as e:
            logging.warning("[P1-8] OTM同步风险敞口计算失败: %s", e)
            return {'otm_exposure': {}, 'symbol_count': 0}

    def _compute_greeks_usage_score(self, dashboard: Optional[Dict[str, Any]] = None) -> float:
        """R17-P1-DOC-P1-04修复: 计算Greeks使用率评分。Args: dashboard: Greeks仪表盘字典含portfolio子键。Returns: float 0.0~1.0, 降级时返回0.5(跨策略Greeks聚合降级默认值)"""
        if dashboard is None:
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        try:
            portfolio = dashboard.get('portfolio', {})
            delta_pct = portfolio.get('delta_usage_pct', 50.0)
            gamma_pct = portfolio.get('gamma_usage_pct', 50.0)
            vega_pct = portfolio.get('vega_usage_pct', 50.0)
            max_usage = max(delta_pct, gamma_pct, vega_pct) / 100.0
            if max_usage <= 0.3:
                return 1.0
            elif max_usage < 0.6:
                return 0.7
            elif max_usage < 0.8:
                return 0.4
            else:
                return 0.2
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)

    def _compute_consecutive_loss_score(self, consecutive_losses: int = 0) -> float:
        """R17-P1-DOC-P1-04修复: 计算连续亏损评分。Args: consecutive_losses: 连续亏损次数。Returns: float 0.1~1.0, 0次=1.0/1-2次=0.8/3-4次=0.5/5-6次=0.3/7+次=0.1"""
        if consecutive_losses <= 0:
            return 1.0
        elif consecutive_losses <= 2:
            return 0.8
        elif consecutive_losses <= 4:
            return 0.5
        elif consecutive_losses <= 6:
            return 0.3
        else:
            return 0.1

    def _compute_asymmetric_drawdown_score(self, current_pnl: float = 0.0,
                                            drawdown_pct: float = 0.0) -> float:
        abs_dd = abs(drawdown_pct)
        if current_pnl >= 0:
            if abs_dd < 0.02:
                return 1.0
            elif abs_dd < 0.05:
                return 0.8
            else:
                return 0.6
        else:
            if abs_dd < 0.02:
                return 0.5
            elif abs_dd < 0.05:
                return 0.3
            else:
                return 0.2

    def _compute_tri_validation_score(self, tri_score: Optional[float] = None) -> float:
        if tri_score is None:
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        return max(0.0, min(1.0, float(tri_score)))

    def _compute_alpha_decay_score(self, alpha_ratio: Optional[float] = None) -> float:
        """R17-P1-DOC-P1-04修复: 计算Alpha衰减评分。Args: alpha_ratio: Alpha比率。Returns: float 0.1~1.0, >1.0=1.0/>0.5=0.7/>0.0=0.4/<=0=0.1, 降级时返回0.5"""
        if alpha_ratio is None:
            logging.warning("[R25-P1-TR-07] alpha_ratio为None, 降级为0.5(中性评分), 避免数据缺失时鼓励开仓")
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        if alpha_ratio > 1.0:
            return 1.0
        elif alpha_ratio > 0.5:
            return 0.7
        elif alpha_ratio > 0.0:
            return 0.4
        else:
            return 0.1

    def _compute_cross_correlation_score(self, cross_corr: Optional[float] = None) -> float:
        """R17-P1-DOC-P1-04修复: 计算跨策略相关性评分。Args: cross_corr: 跨策略相关系数。Returns: float 0.1~1.0, 低相关=1.0/高相关=0.1, 降级时返回0.5"""
        if cross_corr is None:
            return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
        abs_corr = abs(cross_corr)
        if abs_corr < 0.2:
            return 1.0
        elif abs_corr < 0.4:
            return 0.8
        elif abs_corr < 0.6:
            return 0.5
        else:
            return 0.2

    def _compute_liquidity_score(self) -> float:
        """R17-P1-DOC-P1-04修复: 计算流动性评分。Returns: float 0.0~1.0, 基于买卖价差评估流动性, 降级时返回0.5"""
        try:
            from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
            bridge = OrderFlowBridge()
            if hasattr(bridge, '_prev_bid') and hasattr(bridge, '_prev_ask'):
                bid = bridge._prev_bid
                ask = bridge._prev_ask
                if bid and ask and bid > 0 and ask > 0:
                    price = (bid + ask) / 2.0
                    spread_ratio = (ask - bid) / price
                    if spread_ratio > 0.005:
                        _base = 0.3
                    elif spread_ratio < 0.001:
                        _base = 1.0
                    else:
                        t = (spread_ratio - 0.001) / (0.005 - 0.001)
                        _base = 1.0 - t * (1.0 - 0.3)
                    # P1-8修复: OTM同步风险敞口影响流动性评分
                    try:
                        _otm_exp = self.compute_otm_sync_exposure()
                        _otm_count = sum(v.get('otm_count', 0) for v in _otm_exp.get('otm_exposure', {}).values())
                        if _otm_count > 50:  # OTM期权过多时降低流动性评分
                            _base *= 0.85
                    except Exception:
                        pass
                    return _base
        except Exception:
            logging.warning("[R22-EP-P1] RiskService exception swallowed")
            pass
        return 0.5  # R17-P1-DOC-P1-07: 跨策略Greeks聚合降级默认值(中性评分)
