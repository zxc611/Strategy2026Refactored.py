# MODULE_ID: M1-072
"""
模式切换引擎 — Facade + re-export门面

解决:
  G1: 封装 ModeEngine 统一入口类，聚合各组件模式相关调用
  G2: ModeConfig dataclass 替代 CAPITAL_SCALE_CONFIGS 字典
  G3: 原子性模式切换（预检查 + 回滚机制）
  G4: 凯利公式仓位计算
  G5: time_decay_cutoff 期权时间价值衰减过滤
  G6: ExitRuleEngine / DrawdownManager 显式抽象

架构: 实现已拆分至子模块，本文件为Facade+re-export门面
  - mode_config.py: 枚举 + ModeConfig + _make_mode_config + 配置常量
  - mode_exit_rules.py: ExitRuleEngine + DrawdownManager + DefensiveDrawdownChecker
  - mode_position_sizing.py: SixDimPositionAdjustmentFactor + kelly_* + PredictiveStateEngine
  - mode_engine_propagation.py: ModeEnginePropagationMixin (9个方法)
"""
from __future__ import annotations

import functools
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.governance.mode_config import (
    CapitalMode,
    TakeProfitMethod,
    StopLossMethod,
    DrawdownAction,
    PyramidingRule,
    ModeConfig,
    _DEFAULT_TVF_PARAMS,
    _get_default_tvf_params,
    _YAML_TO_MODECONFIG_MAP,
    _validate_yaml_modeconfig_mapping,
    _flatten_cache,
    _flatten_cache_ts,
    _get_flattened_param,
    _make_mode_config,
    SHARPE_CONFIG,
    PROFIT_CONFIG,
    BALANCED_CONFIG,
    CAPITAL_MODE_CONFIGS,
)
from ali2026v3_trading.governance.mode_exit_rules import (
    ExitRuleEngine,
    DrawdownManager,
    DefensiveDrawdownChecker,
)
from ali2026v3_trading.governance.mode_position_sizing import (
    SixDimPositionAdjustmentFactor,
    kelly_fraction,
    kelly_position_size,
    PredictiveStateEngine,
)
from ali2026v3_trading.governance.mode_engine_propagation import ModeEnginePropagationService, ModeEnginePropagationMixin

__all__ = [
    'ModeEngine',
    'CapitalMode', 'TakeProfitMethod', 'StopLossMethod',
    'DrawdownAction', 'PyramidingRule', 'ModeConfig',
    'ExitRuleEngine', 'DrawdownManager', 'DefensiveDrawdownChecker',
    'SixDimPositionAdjustmentFactor', 'kelly_fraction', 'kelly_position_size',
    'PredictiveStateEngine', 'ModeEnginePropagationMixin',
    'SHARPE_CONFIG', 'PROFIT_CONFIG', 'BALANCED_CONFIG', 'CAPITAL_MODE_CONFIGS',
]


class ModeEngine:
    _instance: Optional['ModeEngine'] = None
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> 'ModeEngine':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._lock:
            cls._instance = None

    @classmethod
    def create_engine(cls, scale_str: str = 'medium') -> 'ModeEngine':
        engine = cls()
        config = CAPITAL_MODE_CONFIGS.get(scale_str)
        if config is not None:
            engine._config = config
            engine._scale_str = scale_str
            engine._exit_engine = ExitRuleEngine(config.take_profit_method, config.stop_loss_method)
            engine._drawdown_mgr = DrawdownManager(config.drawdown_action, config.recovery_target)
        return engine

    def __init__(self):
        self._propagation_service = ModeEnginePropagationService(self)
        self._config: Optional[ModeConfig] = None
        self._scale_str: str = 'medium'
        self._exit_engine: Optional[ExitRuleEngine] = None
        self._drawdown_mgr: Optional[DrawdownManager] = None
        self._propagation_lock = threading.RLock()
        self._propagated_components: List[str] = []
        self._component_registry: Dict[str, Any] = {}
        self._auto_register_known_components()
        self._degraded_at: Optional[float] = None
        self._degraded_from: Optional[str] = None
        self._auto_recovery_check_interval: float = 300.0
        self._last_recovery_check: float = 0.0

    def __getattr__(self, name):
        _ps = self.__dict__.get('_propagation_service')
        if _ps is not None and hasattr(_ps, name):
            return getattr(_ps, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    @property
    def config(self) -> Optional[ModeConfig]:
        return self._config

    @property
    def scale_str(self) -> str:
        return self._scale_str

    @property
    def exit_engine(self) -> Optional[ExitRuleEngine]:
        return self._exit_engine

    @property
    def drawdown_manager(self) -> Optional[DrawdownManager]:
        return self._drawdown_mgr

    def register_component(self, name: str, component: Any) -> None:
        with self._propagation_lock:
            self._component_registry[name] = component

    def switch_mode(self, scale_str: str, reason: str = 'unknown') -> Dict[str, Any]:
        if not isinstance(scale_str, str):
            logging.error('[ModeEngine] capital_scale type error: expected str, got %s: %s', type(scale_str).__name__, scale_str)
            scale_str = str(scale_str)
        _switch_start = time.time()
        _switch_timeout_sec = 30.0
        with self._propagation_lock:
            config = CAPITAL_MODE_CONFIGS.get(scale_str)
            if config is None:
                logging.error('[ModeEngine] Unknown scale: %s', scale_str)
                return {'success': False, 'error': f'Unknown scale: {scale_str}'}

            old_config = self._config
            old_scale = self._scale_str
            propagated = []
            errors = []

            _registry_before = set(self._component_registry.keys())

            _CRITICAL_COMPONENTS = {'RiskService', 'SignalService'}
            self._apply_to_risk_service(config, scale_str, propagated, errors)
            self._apply_to_predictive_state_engine(config, scale_str, propagated, errors)
            self._apply_to_signal_service(config, scale_str, propagated, errors)
            self._apply_to_state_param_manager(scale_str, propagated, errors)
            self._apply_to_box_spring_strategy(scale_str, propagated, errors)
            _critical_errors = [e for e in errors if any(e.startswith(c + ':') for c in _CRITICAL_COMPONENTS)]
            if _critical_errors:
                logging.error('[ModeEngine] Critical component propagation failed, rolling back: %s', _critical_errors)
                self._rollback(old_config, old_scale, propagated)
                _registry_after = set(self._component_registry.keys())
                if _registry_before != _registry_after:
                    logging.warning('[P-23] 组件注册表不一致: before=%s after=%s',
                                    _registry_before, _registry_after)
                return {'success': False, 'error': str(_critical_errors), 'rolled_back': True}

            self._config = config
            self._scale_str = scale_str
            self._exit_engine = ExitRuleEngine(config.take_profit_method, config.stop_loss_method)
            self._drawdown_mgr = DrawdownManager(config.drawdown_action, config.recovery_target)
            self._propagated_components = propagated
            _scale_order = {'small': 0, 'medium': 1, 'large': 2}
            if old_scale and _scale_order.get(scale_str, 1) < _scale_order.get(old_scale, 1):
                self._degraded_at = time.time()
                self._degraded_from = old_scale
                logging.warning("[R27-P0-DR-08] 降级记录: %s→%s, 将定期检查恢复条件", old_scale, scale_str)
            elif self._degraded_at is not None:
                self._degraded_at = None
                self._degraded_from = None
                logging.info("[R27-P0-DR-08] 降级恢复: 已升级到%s", scale_str)

            logging.info(
                '[ModeEngine] Mode switched: %s -> %s (mode=%s, propagated=%s, reason=%s)',
                old_scale, scale_str, config.mode.name, propagated, reason,
            )
            try:
                from ali2026v3_trading.infra.health_monitor import StructuredJsonlLogger
                _sjl = StructuredJsonlLogger()
                _sjl.log_strategy_switch(old_scale or 'unknown', scale_str, reason)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _sjl_err:
                logging.debug("[R22-EP-06-残留] StructuredJsonlLogger策略切换日志记录失败: %s", _sjl_err)
            _switch_elapsed = time.time() - _switch_start
            if _switch_elapsed > _switch_timeout_sec:
                logging.error("[R23-SM-05-FIX] 模式切换超时: elapsed=%.1fs > timeout=%.1fs, scale=%s->%s",
                             _switch_elapsed, _switch_timeout_sec, old_scale, scale_str)
            return {
                'success': True,
                'old_scale': old_scale,
                'new_scale': scale_str,
                'mode': config.mode.name,
                'propagated': propagated,
                'errors': errors,
                'reason': reason,
            }

    def filter_signal_by_mode(
        self, signal_type: str, estimated_plr: float = 0.0,
        signal_strength: float = 0.0, days_to_expiry: int = 999,
    ) -> Tuple[bool, str]:
        if self._config is None:
            return True, ''

        if self._config.plr_filter_enabled and self._config.min_estimated_plr > 0:
            if signal_type in ('BUY', 'SELL') and estimated_plr < self._config.min_estimated_plr:
                _reason = f'PLR filter: {estimated_plr:.2f} < {self._config.min_estimated_plr:.2f}'
                logging.info("[ModeEngine] LOG-P1-01: Signal filtered - type=%s reason=%s", signal_type, _reason)
                return False, _reason

        if self._config.min_signal_strength > 0 and signal_strength > 0:
            if signal_strength < self._config.min_signal_strength:
                _reason = f'Signal strength: {signal_strength:.2f} < {self._config.min_signal_strength:.2f}'
                logging.info("[ModeEngine] LOG-P1-01: Signal filtered - type=%s reason=%s", signal_type, _reason)
                return False, _reason

        if signal_strength == 0 and self._config.min_signal_strength > 0:
            logging.debug("[R16-P2-3.3] signal_strength=0, 过滤静默跳过: signal_type=%s", signal_type)

        if self._config.time_decay_cutoff > 0 and days_to_expiry < 999:
            if days_to_expiry <= self._config.time_decay_min_days:
                _reason = f'Time decay: days_to_expiry={days_to_expiry} <= {self._config.time_decay_min_days}'
                logging.info("[ModeEngine] LOG-P1-01: Signal filtered - type=%s reason=%s", signal_type, _reason)
                return False, _reason

        return True, ''

    def compute_position_size(
        self, equity: float, entry_price: float, stop_price: float,
        win_rate: float = 0.0, win_loss_ratio: float = 0.0,
        sortino: float = 0.0, calmar: float = 0.0, sharpe: float = 0.0,
        ofi_score: float = 0.0, cvd_divergence: float = 0.0,
        smart_money_flow: float = 0.0,
        delta_exposure: float = 0.0, gamma_risk: float = 0.0,
        theta_decay: float = 0.0, vega_exposure: float = 0.0,
        instrument_type: str = "future", option_discount: float = 0.6,
        market_state: str = "",
    ) -> float:
        if self._config is None:
            logging.info("[ModeEngine.compute_position_size] config为None，返回0.0")
            return 0.0

        if equity <= 0 or entry_price <= 0 or stop_price <= 0:
            logging.info(
                "[ModeEngine.compute_position_size] 边界检查失败: "
                "equity=%.2f, entry_price=%.2f, stop_price=%.2f",
                equity, entry_price, stop_price
            )
            return 0.0

        if self._config.kelly_fraction > 0 and win_rate > 0 and win_loss_ratio > 0:
            logging.info(
                "[ModeEngine.compute_position_size] 使用凯利公式计算仓位: "
                "equity=%.2f, win_rate=%.2f, win_loss_ratio=%.2f, entry_price=%.2f, stop_price=%.2f",
                equity, win_rate, win_loss_ratio, entry_price, stop_price
            )
            base_size = kelly_position_size(
                equity, win_rate, win_loss_ratio,
                entry_price, stop_price,
                kelly_fraction_param=self._config.kelly_fraction,
                max_cap=self._config.single_position_cap,
                instrument_type=instrument_type, option_discount=option_discount,
            )
            logging.info("[ModeEngine.compute_position_size] 凯利公式计算基础仓位: %.4f", base_size)
        else:
            risk_pct = self._config.risk_pct_default
            risk_amount = equity * risk_pct
            stop_distance = abs(entry_price - stop_price)
            if stop_distance < 1e-10:
                logging.info(
                    "[ModeEngine.compute_position_size] 止损距离过小: %.10f，返回0.0",
                    stop_distance
                )
                return 0.0
            base_size = risk_amount / stop_distance
            logging.info(
                "[ModeEngine.compute_position_size] 使用固定风险计算仓位: "
                "risk_pct=%.4f, risk_amount=%.2f, stop_distance=%.2f, base_size=%.4f",
                risk_pct, risk_amount, stop_distance, base_size
            )

        if self._config.tvf_enabled:
            logging.info(
                "[ModeEngine.compute_position_size] TVF调整因子启用，输入参数: "
                "sortino=%.2f, calmar=%.2f, sharpe=%.2f, ofi_score=%.2f, "
                "cvd_divergence=%.2f, smart_money_flow=%.2f, delta_exposure=%.2f, "
                "gamma_risk=%.2f, theta_decay=%.2f, vega_exposure=%.2f",
                sortino, calmar, sharpe, ofi_score, cvd_divergence,
                smart_money_flow, delta_exposure, gamma_risk,
                theta_decay, vega_exposure
            )
            tvf = SixDimPositionAdjustmentFactor().compute_adjustment(
                config=self._config,
                sortino=sortino, calmar=calmar, sharpe=sharpe,
                ofi_score=ofi_score,
                cvd_divergence=cvd_divergence,
                smart_money_flow=smart_money_flow,
                delta_exposure=delta_exposure,
                gamma_risk=gamma_risk,
                theta_decay=theta_decay,
                vega_exposure=vega_exposure,
                market_state=market_state,
            )
            logging.info("[ModeEngine.compute_position_size] TVF调整因子: %.4f", tvf)
            base_size = base_size * tvf
            logging.info("[ModeEngine.compute_position_size] TVF调整后仓位: %.4f", base_size)
        else:
            logging.info("[ModeEngine.compute_position_size] TVF调整因子未启用，使用基础仓位: %.4f", base_size)

        max_shares = equity * self._config.single_position_cap / entry_price
        logging.info(
            "[ModeEngine.compute_position_size] 单仓上限约束: "
            "equity=%.2f, single_position_cap=%.4f, entry_price=%.2f, max_shares=%.4f",
            equity, self._config.single_position_cap, entry_price, max_shares
        )

        final_size = min(base_size, max_shares)
        logging.info(
            "[ModeEngine.compute_position_size] 最终仓位大小: base_size=%.4f, max_shares=%.4f, final_size=%.4f",
            base_size, max_shares, final_size
        )
        return final_size

    def evaluate_strategy_fit(
        self, train_result: Dict[str, Any],
        test_result: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        capital_scale: str = "medium",
    ) -> Optional[Any]:
        if self._config is None:
            return None
        try:
            from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
            judge = CascadeJudge.from_config(capital_scale=capital_scale, params=params)
            metrics = adapt_backtest_result(train_result, test_result, params, strategy_type=params.get('strategy_type', '') if params else '')
            return judge.judge(metrics)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning('[ModeEngine] CascadeJudge evaluation failed: %s', e)
            return None

    def reload_tvf_params(self, config_path: Optional[str] = None, caller_id: str = "unknown") -> Dict[str, Any]:
        _TRUSTED_RELOAD_CALLERS = frozenset({
            'mode_engine', 'config_service', 'strategy_ecosystem', 'risk_service', 'main',
        })
        if caller_id not in _TRUSTED_RELOAD_CALLERS:
            logging.warning("[ModeEngine] reload_tvf_params被非授权调用方拒绝: caller_id=%s", caller_id)
            return {'success': False, 'error': f'Unauthorized caller: {caller_id}'}
        try:
            from ali2026v3_trading.config.tvf_param_loader import get_tvf_param_loader
            loader = get_tvf_param_loader()
            yaml_params = loader.reload(config_path)
            with self._propagation_lock:
                if self._config is None:
                    return {'success': False, 'error': 'No active ModeConfig'}
                new_config = loader.apply_to_mode_config(self._config)
                _TVF_BOUNDS = {
                    'tvf_enabled': (None, None),
                    'tvf_sigmoid_scale': (0.1, 100.0),
                    'tvf_base_multiplier': (0.01, 10.0),
                    'tvf_risk_weight': (0.0, 1.0),
                    'tvf_sharpe_weight': (0.0, 1.0),
                    'tvf_sortino_weight': (0.0, 1.0),
                }
                for attr, (lo, hi) in _TVF_BOUNDS.items():
                    val = getattr(new_config, attr, None)
                    if val is not None and lo is not None and hi is not None:
                        clamped = max(lo, min(hi, val))
                        if clamped != val:
                            logging.warning('[ModeEngine] R14-P1-BIZ-04: TVF参数越界钳位 %s=%.4f→%.4f [%.2f,%.2f]',
                                            attr, val, clamped, lo, hi)
                            setattr(new_config, attr, clamped)
                self._config = new_config
                for comp_name in self._propagated_components:
                    comp = self._component_registry.get(comp_name)
                    if comp is not None:
                        try:
                            if hasattr(comp, 'update_tvf_config'):
                                comp.update_tvf_config(new_config)
                            elif hasattr(comp, 'set_params'):
                                comp.set_params({'tvf_enabled': new_config.tvf_enabled})
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                            logging.warning('[P-24] 更新组件%s参数失败: %s', comp_name, _e)
                logging.info(
                    '[ModeEngine] TVF参数热重载成功: tvf_enabled=%s source=yaml propagated=%s',
                    new_config.tvf_enabled, self._propagated_components,
                )
            return {
                'success': True,
                'tvf_enabled': new_config.tvf_enabled,
                'params_source': 'yaml' if yaml_params else 'default',
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error('[ModeEngine] TVF参数热重载失败: %s', e)
            return {'success': False, 'error': str(e)}

    def _get_active_params(self) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            active = eco.get_active_strategy()
            if active == 'reverse':
                return eco.get_s2_params()
            else:
                return eco.get_s1_params()
        except (ImportError, AttributeError) as e:
            logging.debug('[ModeEngine] get_active_params failed: %s', e)
            return {}

    def _sync_params_to_ecosystem(self) -> None:
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            active = eco.get_active_strategy()
            current_mode = getattr(self, '_current_mode', None)
            if current_mode and hasattr(current_mode, 'value'):
                mode_params = getattr(self, '_mode_configs', {}).get(current_mode.value, {})
                if mode_params:
                    if active == 'reverse':
                        eco.update_s2_params(**mode_params)
                    else:
                        eco.update_s1_params(**mode_params)
        except (ImportError, AttributeError, KeyError) as e:
            logging.warning("[ModeEngine] sync_params_to_ecosystem failed: %s", e)

    def auto_select_mode(
        self, train_result: Dict[str, Any],
        test_result: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        capital_scale: str = "medium",
    ) -> Dict[str, Any]:
        active_params = _get_active_params_cached(self)
        if active_params:
            logging.debug('[ModeEngine] 活跃策略参数: %s', active_params.keys())
            self._sync_params_to_ecosystem()

        report = self.evaluate_strategy_fit(train_result, test_result, params, capital_scale=capital_scale)
        if report is None:
            return {'success': False, 'error': 'CascadeJudge evaluation failed', 'scale': self._scale_str}
        if not hasattr(report, 'passed') or not report.passed:
            return {'success': False, 'error': 'Strategy did not pass cascade', 'scale': self._scale_str, 'report': report}
        metrics = getattr(report, 'metrics', None)
        if metrics is None:
            return {'success': True, 'scale': self._scale_str, 'report': report}
        sortino = getattr(metrics, 'sortino_ratio', 0.0)
        sharpe = getattr(metrics, 'sharpe_ratio', 0.0)
        plr = getattr(metrics, 'profit_loss_ratio', 0.0)
        if sharpe >= 2.0 and sortino >= 1.5:
            target_scale = 'large'
        elif plr >= 2.0:
            target_scale = 'small'
        else:
            target_scale = 'medium'
        if target_scale != self._scale_str:
            return self.switch_mode(target_scale, reason='auto_select')
        return {'success': True, 'scale': self._scale_str, 'report': report}


_active_params_cache = None
_active_params_cache_ts = 0.0
_ACTIVE_PARAMS_CACHE_TTL = 1.0

def _get_active_params_cached(mode_engine_instance):
    global _active_params_cache, _active_params_cache_ts
    _now = time.time()
    if _active_params_cache is not None and (_now - _active_params_cache_ts) < _ACTIVE_PARAMS_CACHE_TTL:
        return _active_params_cache
    _active_params_cache = mode_engine_instance._get_active_params()
    _active_params_cache_ts = _now
    return _active_params_cache


def _cache_result_in_call(func):
    _cached_val = None
    _cached = False

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal _cached_val, _cached
        if _cached:
            return _cached_val
        _cached_val = func(*args, **kwargs)
        _cached = True
        return _cached_val

    return wrapper
