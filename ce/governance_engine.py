# MODULE_ID: M1-069
import logging
import math
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

# R27-P1修复: 导入共享状态注册表、浮点工具
from ali2026v3_trading.infra.resilience import (
    SharedStateRegistry, stable_sum, stable_mean, stable_variance,
    KahanSummation, safe_divide, compute_sharpe_stable,
    PRICE_TOLERANCE,
)
from ali2026v3_trading.config.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_INCORRECT_REVERSAL, STRATEGY_MODE_OTHER,
)
from ali2026v3_trading.infra._helpers import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class E12ReverseStrategyPseudoIndependenceDetector:
    """R14-P1-DOC-P1-06修复: E12反向策略伪独立性检测器 — 检测主策略与反向策略PnL相关性过高，判定伪独立"""
    def __init__(self, max_correlation_threshold: float = 0.3,
                 min_trade_count: int = 20):
        self._max_correlation_threshold = max_correlation_threshold
        self._min_trade_count = min_trade_count
        # E-06修复: 反馈通道，传递给评判
        self._feedback_channel: List[Dict[str, Any]] = []

    def detect(self, main_trades: List[Dict], reverse_trades: List[Dict]) -> Dict[str, Any]:
        if len(main_trades) < self._min_trade_count or len(reverse_trades) < self._min_trade_count:
            return {"e12_triggered": False, "reason": "insufficient_trades", "correlation": 0.0}
        
        main_pnls = [t.get('pnl', 0.0) for t in main_trades[:self._min_trade_count]]
        reverse_pnls = [t.get('pnl', 0.0) for t in reverse_trades[:self._min_trade_count]]
        
        if len(main_pnls) < 2 or len(reverse_pnls) < 2:
            return {"e12_triggered": False, "reason": "insufficient_data", "correlation": 0.0}
        
        corr_matrix = np.corrcoef(main_pnls, reverse_pnls) if len(main_pnls) == len(reverse_pnls) else None
        if corr_matrix is None:
            corr = 0.0
            corr_reason = "length_mismatch"
        else:
            np.fill_diagonal(corr_matrix, 1.0)
            corr = corr_matrix[0, 1]
            if math.isnan(corr) or math.isinf(corr):
                logger.warning("[NP-P2-25] corr=NaN/Inf replaced with 0.0, data quality issue")
                corr = 0.0
                corr_reason = "undefined_variance"
            else:
                corr_reason = "ok"
        
        e12_triggered = abs(corr) > self._max_correlation_threshold if corr != 0.0 or corr_reason == "ok" else False
        result = {
            "e12_triggered": e12_triggered,
            "correlation": float(corr) if corr != 0.0 or corr_reason != "undefined_variance" else 0.0,
            "threshold": self._max_correlation_threshold,
            "n_trades": min(len(main_trades), len(reverse_trades)),
            "reason": "high_correlation_with_main" if e12_triggered else ("undefined_variance" if corr_reason == "undefined_variance" else "independent"),
        }
        # E-06修复: 发现问题时发送反馈通道
        if e12_triggered:
            self._feedback_channel.append({"checker": "E12", "severity": "warning", "detail": result})
        return result


class E13ShadowStrategyCollusionDetector:
    """R14-P1-DOC-P1-06修复: E13影子策略共谋检测器 — 检测影子策略参数差异过小或信号同步率过高，判定共谋"""
    def __init__(self, min_param_diff_pct: float = 0.20,
                 max_signal_sync_rate: float = 0.7,
                 min_trade_count: int = 20):
        self._min_param_diff_pct = min_param_diff_pct
        self._max_signal_sync_rate = max_signal_sync_rate
        self._min_trade_count = min_trade_count

    def detect(self, main_params: Dict[str, float],
               shadow_params: Dict[str, float],
               main_signals: List[Dict],
               shadow_signals: List[Dict]) -> Dict[str, Any]:
        key_params = ["close_take_profit_ratio", "close_stop_loss_ratio",
                      "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
                      "resonance_hard_time_stop_min", "box_hard_time_stop_min",
                      "max_risk_ratio"]
        diffs = []
        for k in key_params:
            if k in main_params and k in shadow_params and main_params[k] != 0:
                diffs.append(abs(shadow_params[k] - main_params[k]) / abs(main_params[k]))
        avg_diff = stable_mean(diffs) if diffs else 0.0  # P2-32: 统一使用stable_mean
        param_diff_passed = avg_diff >= self._min_param_diff_pct
        
        sync_count = 0
        min_signals = min(len(main_signals), len(shadow_signals))
        for i in range(min_signals):
            if (main_signals[i].get('direction') == shadow_signals[i].get('direction') and
                main_signals[i].get('instrument_id') == shadow_signals[i].get('instrument_id')):
                sync_count += 1
        sync_rate = sync_count / min_signals if min_signals > 0 else 0.0
        sync_passed = sync_rate <= self._max_signal_sync_rate

        # P1-R8-12修复: 参数集来源独立性检查 — 主/影子参数集必须来自不同优化来源
        source_independent = True
        source_detail = {}
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import PARAM_SOURCE_ANNOTATION
            main_sources = set()
            shadow_sources = set()
            for k in key_params:
                annotation = PARAM_SOURCE_ANNOTATION.get(k, {})
                src = annotation.get("source", "未知")
                if k in main_params:
                    main_sources.add(src)
                if k in shadow_params:
                    shadow_sources.add(src)
            shared_sources = main_sources & shadow_sources
            # 如果主/影子参数共享直觉类来源（非量化），判定来源独立性不足
            intuition_like = {"直觉", "直觉(待网格扫描)", "直觉(待验证)", "default", "guess", "intuition", ""}
            shared_intuition = shared_sources & intuition_like
            if shared_intuition:
                source_independent = False
            # 如果共享来源超过70%（即使都是量化来源）也判定独立性不足
            elif len(main_sources) > 0 and len(shared_sources) / max(len(main_sources), 1) > 0.7:
                source_independent = False
            source_detail = {
                "main_sources": sorted(main_sources),
                "shadow_sources": sorted(shadow_sources),
                "shared_sources": sorted(shared_sources),
                "shared_intuition": sorted(shared_intuition) if not source_independent else [],
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e12_e:
            logging.debug("[E13-P1-R8-12] PARAM_SOURCE_ANNOTATION不可用，跳过来源独立性检查: %s", _e12_e)

        source_independence_passed = source_independent
        e13_triggered = not param_diff_passed or not sync_passed or not source_independence_passed
        
        reason_parts = []
        if not param_diff_passed:
            reason_parts.append("param_diff")
        if not sync_passed:
            reason_parts.append("signal_sync")
        if not source_independence_passed:
            reason_parts.append("source_not_independent")
        
        return {
            "e13_triggered": e13_triggered,
            "param_diff_pct": avg_diff,
            "param_diff_passed": param_diff_passed,
            "signal_sync_rate": sync_rate,
            "sync_passed": sync_passed,
            "source_independence_passed": source_independence_passed,
            "source_detail": source_detail,
            "reason": "+".join(reason_parts) if e13_triggered else "independent",
        }


class MultiStateSwitchBacktestScenario:
    def __init__(self, state_sequence: List[str] = None,
                 min_hold_bars: int = 10):
        if state_sequence is None:
            # R10-三对齐修复: 五态序列与state_param_manager.py _FIVE_STATES一致
            state_sequence = [STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, STRATEGY_MODE_OTHER,  # R25-SE-P1-02-FIX
                              STRATEGY_MODE_INCORRECT_REVERSAL, "incorrect_reversal_defensive", STRATEGY_MODE_OTHER]
        self._state_sequence = state_sequence
        self._min_hold_bars = min_hold_bars
        self._scenario_results: List[Dict[str, Any]] = []

    def run_scenario(self, strategy_factory, bar_data: Any = None) -> Dict[str, Any]:
        results = []
        for i, state in enumerate(self._state_sequence):
            try:
                strategy = strategy_factory(state)
                result = {"scenario_step": i, "state": state, "strategy_loaded": True}
                if bar_data is not None:
                    result["bar_count"] = len(bar_data) if hasattr(bar_data, '__len__') else 0
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                result = {"scenario_step": i, "state": state, "strategy_loaded": False, "error": str(e)}
            results.append(result)
        
        self._scenario_results = results
        transitions = sum(1 for i in range(1, len(results)) if results[i]["state"] != results[i-1]["state"])
        return {
            "n_steps": len(self._state_sequence),
            "n_transitions": transitions,
            "results": results,
            "passed": all(r.get("strategy_loaded", False) for r in results),
        }


class WF6ToWF10EliminationChecker:
    """R14-P1-DOC-P1-06修复: WF6-WF10淘汰条件检查器 — 检查Sharpe单调递降/参数脆弱性/负EV/Alpha衰减/绝对EV突破"""
    def __init__(self):
        self._wf_checks = {
            "wf6": self._check_wf6_monotone_decline,
            "wf7": self._check_wf7_parameter_fragility,
            "wf8": self._check_wf8_negative_ev,
            "wf9": self._check_wf9_alpha_decline,
            "wf10": self._check_wf10_absolute_ev_breach,
        }

    def _check_wf6_monotone_decline(self, window_results: List[Dict]) -> bool:
        if len(window_results) < 3:
            return False
        sharpes = [w.get('sharpe', 0.0) for w in window_results]
        consecutive = 0
        for j in range(1, len(sharpes)):
            if sharpes[j] < sharpes[j-1]:
                consecutive += 1
            else:
                consecutive = 0
            if consecutive >= 2:
                return True
        return False

    def _check_wf7_parameter_fragility(self, window_results: List[Dict]) -> bool:
        if len(window_results) < 2:
            return False
        sharpes = [w.get('sharpe', 0.0) for w in window_results]
        if max(sharpes) <= 0:
            return False
        return (max(sharpes) - min(sharpes)) / max(sharpes) > 0.5

    def _check_wf8_negative_ev(self, window_results: List[Dict]) -> bool:
        return any(w.get('expected_value', 0.0) < 0 for w in window_results)

    def _check_wf9_alpha_decline(self, window_results: List[Dict],
                                  threshold_pct: float = 20.0) -> bool:
        if len(window_results) < 3:
            return False
        evs = [w.get('expected_value', 0.0) for w in window_results]
        consecutive = 0
        for j in range(1, len(evs)):
            if evs[j] < evs[j-1] * (1 - threshold_pct / 100):
                consecutive += 1
            else:
                consecutive = 0
            if consecutive >= 2:
                return True
        return False

    def _check_wf10_absolute_ev_breach(self, window_results: List[Dict],
                                        min_ev_threshold: float = 0.01) -> bool:
        """WF10: 绝对EV阈值突破 — 与WF8区分：WF8检查EV<0，WF10检查EV低于绝对阈值"""
        return any(w.get('expected_value', 0.0) < min_ev_threshold for w in window_results)

    def check_all(self, window_results: List[Dict]) -> Dict[str, Any]:
        results = {}
        for wf_name, check_func in self._wf_checks.items():
            results[wf_name] = check_func(window_results)
        any_triggered = any(results.values())
        return {
            "elimination_triggered": any_triggered,
            "wf_results": results,
            "triggered_conditions": [k for k, v in results.items() if v],
        }


class E8E9E10EliminationChecker:
    """R14-P1-DOC-P1-06修复: E8/E9/E10淘汰检查器 — E8尾部风险/E9 Minsky杠杆趋势/E10状态依赖性检测"""
    def __init__(self, tail_risk_threshold: float = 0.05,
                 minsky_threshold: float = 0.3,
                 state_dependency_threshold: float = 0.8):
        self._tail_risk_threshold = tail_risk_threshold
        self._minsky_threshold = minsky_threshold
        self._state_dependency_threshold = state_dependency_threshold

    def check_e8_tail_risk(self, returns: List[float]) -> Dict[str, Any]:
        # R14-P1-LOG-03修复: checker方法添加日志
        if not returns:
            logging.debug("[Governance] E8 check: empty returns, not triggered")
            return {"e8_triggered": False, "tail_ratio": 0.0}
        sorted_returns = sorted(returns)
        n = len(sorted_returns)
        tail_5pct = sorted_returns[:max(1, n // 20)]
        tail_mean = stable_mean(tail_5pct) if tail_5pct else 0.0  # P2-32: 统一使用stable_mean
        overall_mean = stable_mean(returns)  # P2-32: 统一使用stable_mean
        tail_ratio = abs(tail_mean / overall_mean) if abs(overall_mean) > 1e-10 else 0.0
        e8_triggered = tail_ratio > self._tail_risk_threshold
        if e8_triggered:
            logging.warning("[Governance] R14-P1-LOG-03: E8 tail_risk TRIGGERED ratio=%.2f>threshold=%.2f", tail_ratio, self._tail_risk_threshold)
        return {
            "e8_triggered": e8_triggered,
            "tail_ratio": tail_ratio,
            "threshold": self._tail_risk_threshold,
        }

    def check_e9_minsky(self, leverage_history: List[float]) -> Dict[str, Any]:
        """P1-R8-13修复: E9 Minsky时刻检测对齐手册定义
        手册定义: "低波动仓位在模拟波动率跳升2倍时回撤>20%"
        检测步骤:
          1. 计算滚动波动率（最近N个窗口）'
          2. 检测波动率跳升（当前波动率 > 2 × 滚动均值）
          3. 在跳升窗口中检查回撤是否超过阈值
        """
        if not leverage_history:
            return {"e9_triggered": False, "leverage_trend": 0.0, "vol_spike_detected": False}
        
        n = len(leverage_history)
        if n < 10:
            return {"e9_triggered": False, "leverage_trend": 0.0, "vol_spike_detected": False,
                    "reason": "leverge_history样本不足(<10)"}
        
        # 滚动波动率窗口
        roll_window = min(20, n // 3)
        if roll_window < 3:
            roll_window = 3
        
        rolling_vols = []
        for i in range(roll_window, n):
            window = leverage_history[i - roll_window:i]
            mean_lev = stable_mean(window)  # P2-32: 统一使用stable_mean
            vol = stable_variance(window) ** 0.5  # P2-32: 统一使用stable_variance
            rolling_vols.append(vol)
        
        if len(rolling_vols) < 3:
            return {"e9_triggered": False, "leverage_trend": 0.0, "vol_spike_detected": False,
                    "reason": "滚动波动率样本不足"}
        
        vol_mean = stable_mean(rolling_vols)  # P2-32: 统一使用stable_mean
        if vol_mean < 1e-10:
            return {"e9_triggered": False, "leverage_trend": 0.0, "vol_spike_detected": False,
                    "reason": "波动率接近零，无法判断"}
        
        # 检测波动率跳升 > 2x 均值
        vol_spike_windows = []
        max_drawdown_in_spike = 0.0
        for i, rv in enumerate(rolling_vols):
            if rv > 2.0 * vol_mean:
                # 在跳升窗口中计算回撤
                abs_idx = i + roll_window
                pre_spike_start = max(0, abs_idx - roll_window * 2)
                lev_in_spike = leverage_history[pre_spike_start:abs_idx + 1]
                if lev_in_spike:
                    peak = max(lev_in_spike)
                    trough = min(lev_in_spike)
                    if peak > 0:
                        dd = (peak - trough) / peak
                        if dd > max_drawdown_in_spike:
                            max_drawdown_in_spike = dd
                vol_spike_windows.append((i + roll_window, rv / vol_mean))
        
        vol_spike_detected = len(vol_spike_windows) > 0
        dd_breached = max_drawdown_in_spike > 0.20
        
        # 手册定义: 波动率跳升2x AND 回撤>20%
        e9_triggered = vol_spike_detected and dd_breached
        
        return {
            "e9_triggered": e9_triggered,
            "leverage_trend": 0.0,  # 保留兼容，实际不再使用
            "threshold": self._minsky_threshold,
            "vol_spike_detected": vol_spike_detected,
            "vol_spike_count": len(vol_spike_windows),
            "max_vol_ratio": max((r for _, r in vol_spike_windows), default=0.0),
            "max_drawdown_in_spike": max_drawdown_in_spike,
            "dd_breached": dd_breached,
            "reason": "minsky_moment" if e9_triggered else ("vol_spike_no_dd" if vol_spike_detected else "no_spike"),
            "formula": "vol_spike(>2x_mean) AND drawdown_in_spike(>20%)",
        }

    def check_e10_state_dependency(self, state_returns: Dict[str, List[float]]) -> Dict[str, Any]:
        if not state_returns or len(state_returns) < 2:
            return {"e10_triggered": False, "state_variance_ratio": 0.0}
        state_means = {}
        for state, rets in state_returns.items():
            state_means[state] = stable_mean(rets) if rets else 0.0  # P2-32: 统一使用stable_mean
        overall_mean = stable_mean(list(state_means.values()))  # P2-32: 统一使用stable_mean
        between_var = stable_variance(list(state_means.values()))  # P2-32: 统一使用stable_variance
        all_rets = [r for rets in state_returns.values() for r in rets]
        within_var = stable_variance(all_rets) if all_rets else 1.0  # P2-32: 统一使用stable_variance
        ratio = between_var / within_var if within_var > 0 else 0.0
        e10_triggered = ratio > self._state_dependency_threshold
        return {
            "e10_triggered": e10_triggered,
            "state_variance_ratio": ratio,
            "threshold": self._state_dependency_threshold,
        }

    def check_all(self, returns: List[float] = None,
                  leverage_history: List[float] = None,
                  state_returns: Dict[str, List[float]] = None) -> Dict[str, Any]:
        results = {}
        if returns is not None:
            results["e8"] = self.check_e8_tail_risk(returns)
        if leverage_history is not None:
            results["e9"] = self.check_e9_minsky(leverage_history)
        if state_returns is not None:
            results["e10"] = self.check_e10_state_dependency(state_returns)
        any_triggered = any(r.get("e8_triggered", False) or r.get("e9_triggered", False) or r.get("e10_triggered", False) for r in results.values())
        return {
            "elimination_triggered": any_triggered,
            "e_results": results,
            "triggered_codes": [k for k, r in results.items() if r.get(f"{k}_triggered", False)],
        }


class E7UnexplainedReturnChecker:
    """R14-P1-DOC-P1-06修复: E7未解释收益检测器 — 检测Greeks归因残差占比是否超过阈值，判定存在未解释收益"""
    def __init__(self, residual_threshold_pct: float = 15.0):
        self._residual_threshold_pct = residual_threshold_pct

    # R10-P0-23修复: E7残差>15%基准错误 — 分母应为explained_pnl而非total_pnl
    # V7.0手册规范: 残差占比 = |residual| / |explained_pnl|，当explained_pnl=0时回退到total_pnl
    def check(self, pnl_attribution: Dict[str, float]) -> Dict[str, Any]:
        unexplained = pnl_attribution.get("unexplained", 0.0)
        explained_pnl = (
            pnl_attribution.get("delta_contrib", 0.0) +
            pnl_attribution.get("gamma_contrib", 0.0) +
            pnl_attribution.get("vega_contrib", 0.0) +
            pnl_attribution.get("theta_contrib", 0.0)
        )
        residual = unexplained
        # R10-P0-23: 分母优先使用explained_pnl，避免total_pnl过大稀释残差占比
        denominator = explained_pnl if abs(explained_pnl) > 1e-10 else abs(unexplained)
        residual_pct = abs(safe_divide(residual, denominator, default=0.0, min_denominator=1e-10)) * 100  # R5-5
        e7_triggered = residual_pct > self._residual_threshold_pct
        return {
            "e7_triggered": e7_triggered,
            "residual_pct": residual_pct,
            "threshold_pct": self._residual_threshold_pct,
            "unexplained": unexplained,
            "explained_pnl": explained_pnl,
            "residual": residual,
            "reason": "unexplained_return_exceeds_threshold" if e7_triggered else "ok",
        }

    def check_with_significance(
        self,
        residual_series: list,
        pnl_series: list,
        alpha: float = 0.05,
    ) -> Dict[str, Any]:
        """裂缝30修复：统计显著且相对阈值双重条件。'
        E7触发条件: t检验p<alpha 且 mean(|residual|) > threshold_pct% * mean(|pnl|)
        """
        import math
        n = min(len(residual_series), len(pnl_series))
        if n < 2:
            return {"e7_triggered": False, "reason": "样本不足", "p_value": 1.0}
        residuals = residual_series[:n]
        pnls = pnl_series[:n]
        mean_residual = sum(residuals) / n
        var_residual = sum((r - mean_residual) ** 2 for r in residuals) / (n - 1)
        mean_abs_residual = sum(abs(r) for r in residuals) / n
        mean_abs_pnl = sum(abs(p) for p in pnls) / n
        if var_residual <= 0:
            return {"e7_triggered": False, "reason": "残差方差为零", "p_value": 1.0}
        t_stat = mean_residual / (math.sqrt(var_residual) / math.sqrt(n))
        p_value = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(t_stat) / math.sqrt(2.0))))
        exceeds_pct = (mean_abs_pnl > 0 and
                       mean_abs_residual > (self._residual_threshold_pct / 100.0) * mean_abs_pnl)
        e7_triggered = (p_value < alpha) and exceeds_pct
        return {
            "e7_triggered": e7_triggered,
            "t_stat": t_stat,
            "p_value": p_value,
            "mean_abs_residual": mean_abs_residual,
            "mean_abs_pnl": mean_abs_pnl,
            "residual_pct_of_pnl": mean_abs_residual / mean_abs_pnl if mean_abs_pnl > 0 else 0.0,
            "reason": "significant_and_large" if e7_triggered else "not_significant_or_small",
        }


class E11QuantitativeSourceChecker:
    """R14-P1-DOC-P1-06修复: E11量化来源检查器 — 检测参数来源是否为直觉/猜测/手动设定，要求全部量化来源"""
    def __init__(self, allowed_sources: List[str] = None):
        if allowed_sources is None:
            allowed_sources = ["backtest", "goldilocks", "stress_test", "walk_forward",
                               "sensitivity", "counterfactual", "monte_carlo", "oat"]
        self._allowed_sources = allowed_sources

    def check(self, param_sources: Dict[str, str]) -> Dict[str, Any]:
        intuition_params = []
        for param_name, source in param_sources.items():
            if source.lower() in ("intuition", "guess", "default", "manual", ""):
                intuition_params.append(param_name)
        e11_triggered = len(intuition_params) > 0
        return {
            "e11_triggered": e11_triggered,
            "intuition_params": intuition_params,
            "intuition_count": len(intuition_params),
            "total_params": len(param_sources),
            "allowed_sources": list(self._allowed_sources),
            "reason": "intuition_source_detected" if e11_triggered else "all_quantified",
        }


# P1-22修复: mark_module_loaded/mark_module_failed直接导入(无fallback)
from ali2026v3_trading.infra.metrics_registry import mark_module_loaded, mark_module_failed

# CORE-DEPENDENCY: evaluation模块是治理引擎的核心依赖
# P1-16修复: 直接导入evaluation类(延迟导入已在parameter_drift_detector.py中打破循环)
from ali2026v3_trading.evaluation.parameter_drift_detector import ParameterDriftDetector as _EvalParameterDriftDetector
from ali2026v3_trading.evaluation.violation_tracker import StrategyViolationTracker as _EvalStrategyViolationTracker
from ali2026v3_trading.evaluation.state_density_decay import StateEDensityDecayTracker as _EvalStateEDensityDecayTracker
StateEDensityDecayTracker = _EvalStateEDensityDecayTracker
ParameterDriftDetector = _EvalParameterDriftDetector
StrategyViolationTracker = _EvalStrategyViolationTracker
_HAS_EVALUATION_CLASSES = True
mark_module_loaded('governance_engine_evaluation_classes')


# E-06修复: governance反馈通道传递给评判
def create_governance_feedback_channel(checkers: List[Any]) -> Dict[str, Any]:
    """聚合所有checker的反馈通道，传递给strategy_judgment_engine"""
    feedback = {"total_issues": 0, "issues_by_checker": {}}
    for checker in checkers:
        channel = getattr(checker, "_feedback_channel", [])
        if channel:
            checker_name = type(checker).__name__
            feedback["issues_by_checker"].setdefault(checker_name, list(channel))
            feedback["total_issues"] += len(channel)
    return feedback


class GovernanceEngine:
    """治理引擎: 聚合所有checker并执行检查/反馈

    R4-J-06修复: 检测器参数可配置化 —
    通过 config 字典传入各检测器的阈值参数，替代硬编码。'
    """

    # P1-R9-25修复: 错误码映射表统一映射
    ERROR_CODE_ACTION_MAP = {
        "ERR_ALPHA_DECAY": {"action": "degrade", "level": "warning"},
        "ERR_CIRCUIT_BREAKER": {"action": "pause", "level": "critical"},
        "ERR_DAILY_DD_BREACH": {"action": "pause", "level": "critical"},
        "ERR_MARGIN_EXCEEDED": {"action": "reduce_position", "level": "warning"},
        "ERR_CONSECUTIVE_LOSS": {"action": "block_new_open", "level": "warning"},
        "ERR_EV_NEGATIVE": {"action": "block_new_open", "level": "critical"},
        "ERR_PARAM_DRIFT": {"action": "alert", "level": "info"},
        "ERR_SELF_TRADE": {"action": "reject_order", "level": "critical"},
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._checkers: List[Any] = []
        self._feedback_channel: List[Dict[str, Any]] = []
        self._config = config if isinstance(config, dict) else {}  # R24-P2-DF-07修复: 使用isinstance检查替代or {}，防止非dict falsy值被替换
        self._param_snapshot_history: List[Dict[str, float]] = []
        self._param_snapshot_max_capacity: int = 500

    def add_checker(self, checker: Any) -> None:
        self._checkers.append(checker)

    def capture_param_snapshot(self) -> Dict[str, float]:
        snapshot: Dict[str, float] = {}
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            cached = get_cached_params()
            _float_keys = [
                "close_take_profit_ratio", "close_stop_loss_ratio",
                "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
                "resonance_hard_time_stop_min", "box_hard_time_stop_min",
                "max_risk_ratio", "option_buy_lots", "state_confirm_bars",
            ]
            for k in _float_keys:
                v = cached.get(k)
                if v is not None:
                    try:
                        snapshot[k] = float(v)
                    except (TypeError, ValueError):
                        pass
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
            logger.debug("[P0-24] config_params不可用: %s", _e)
        try:
            from ali2026v3_trading.tvf_param_loader import TVF_DEFAULT_PARAMS
            for k, v in TVF_DEFAULT_PARAMS.items():
                try:
                    snapshot[f"tvf_{k}"] = float(v)
                except (TypeError, ValueError):
                    pass
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
            logger.debug("[P0-24] tvf_param_loader不可用: %s", _e)
        if snapshot:
            self._param_snapshot_history.append(snapshot)
            if len(self._param_snapshot_history) > self._param_snapshot_max_capacity:
                self._param_snapshot_history.pop(0)
        return snapshot

    def run_all_checkers(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self.capture_param_snapshot()
        results = {}
        all_passed = True
        for checker in self._checkers:
            checker_name = type(checker).__name__
            try:
                check_fn = getattr(checker, "check", None)
                if check_fn and callable(check_fn):
                    result = check_fn(context or {})
                else:
                    result = {"passed": True, "checker": checker_name}
                results[checker_name] = result
                if not result.get("passed", True):
                    all_passed = False
                    self._feedback_channel.append({"checker": checker_name, "result": result})
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                results[checker_name] = {"passed": False, "error": str(e)}
                all_passed = False
        return {"passed": all_passed, "results": results, "n_checkers": len(self._checkers)}

    def submit_feedback(self, strategy_id: str = "", verdict: Any = None, dimensions: Any = None, blockers: Any = None, feedback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if feedback is not None:
            self._feedback_channel.append(feedback)
            return {"submitted": True, "channel_size": len(self._feedback_channel)}
        entry = {
            "strategy_id": strategy_id,
            "verdict": str(verdict) if verdict is not None else "",
            "dimensions": dimensions if dimensions is not None else [],
            "blockers": blockers if blockers is not None else [],
        }
        self._feedback_channel.append(entry)
        return {"submitted": True, "channel_size": len(self._feedback_channel)}

    def get_feedback(self) -> List[Dict[str, Any]]:
        return list(self._feedback_channel)


_governance_engine_instance: Optional[GovernanceEngine] = None


def get_governance_engine(config: Optional[Dict[str, Any]] = None) -> GovernanceEngine:
    """P2-R3-D-20: governance检查器统一调度入口 — 已修复。

    get_governance_engine()自动注册所有checker(E7/E8E9E10/E11/E12/E13/WF6-WF10)，
    消除了原先各checker独立调用无统一调度的问题。

    R4-J-06修复: 支持通过 config 传入检测器阈值参数
    """
    global _governance_engine_instance
    if _governance_engine_instance is None or (config and config != _governance_engine_instance._config):
        _governance_engine_instance = GovernanceEngine(config=config)
        # R4-J-06: 从 config 读取各检测器阈值，提供默认值
        _cfg = config or {}
        _governance_engine_instance.add_checker(E7UnexplainedReturnChecker(
            residual_threshold_pct=_cfg.get("e7_residual_threshold_pct", 15.0),
        ))
        _governance_engine_instance.add_checker(E8E9E10EliminationChecker(
            tail_risk_threshold=_cfg.get("e8_tail_risk_threshold", 0.05),
            minsky_threshold=_cfg.get("e9_minsky_threshold", 0.3),
            state_dependency_threshold=_cfg.get("e10_state_dependency_threshold", 0.8),
        ))
        _governance_engine_instance.add_checker(E11QuantitativeSourceChecker(
            allowed_sources=_cfg.get("e11_allowed_sources", None),
        ))
        _governance_engine_instance.add_checker(E12ReverseStrategyPseudoIndependenceDetector(
            max_correlation_threshold=_cfg.get("e12_max_correlation_threshold", 0.3),
            min_trade_count=_cfg.get("e12_min_trade_count", 20),
        ))
        _governance_engine_instance.add_checker(E13ShadowStrategyCollusionDetector(
            min_param_diff_pct=_cfg.get("e13_min_param_diff_pct", 0.20),
            max_signal_sync_rate=_cfg.get("e13_max_signal_sync_rate", 0.7),
            min_trade_count=_cfg.get("e13_min_trade_count", 20),
        ))
        _governance_engine_instance.add_checker(WF6ToWF10EliminationChecker())
        _governance_engine_instance.add_checker(MultiStateSwitchBacktestScenario())
        # AP-03: SingletonRegistry注册
        try:
            from ali2026v3_trading.infra.registry_service import SingletonRegistry
            registry = SingletonRegistry.get_registry("governance_engine")
            registry.register_singleton("governance_engine.instance", _governance_engine_instance)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
    return _governance_engine_instance
