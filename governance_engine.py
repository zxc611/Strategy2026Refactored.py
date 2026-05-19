import logging
import math
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


class E12ReverseStrategyPseudoIndependenceDetector:
    def __init__(self, max_correlation_threshold: float = 0.3,
                 min_trade_count: int = 20):
        self._max_correlation_threshold = max_correlation_threshold
        self._min_trade_count = min_trade_count

    def detect(self, main_trades: List[Dict], reverse_trades: List[Dict]) -> Dict[str, Any]:
        if len(main_trades) < self._min_trade_count or len(reverse_trades) < self._min_trade_count:
            return {"e12_triggered": False, "reason": "insufficient_trades", "correlation": 0.0}
        
        main_pnls = [t.get('pnl', 0.0) for t in main_trades[:self._min_trade_count]]
        reverse_pnls = [t.get('pnl', 0.0) for t in reverse_trades[:self._min_trade_count]]
        
        if len(main_pnls) < 2 or len(reverse_pnls) < 2:
            return {"e12_triggered": False, "reason": "insufficient_data", "correlation": 0.0}
        
        corr = np.corrcoef(main_pnls, reverse_pnls)[0, 1] if len(main_pnls) == len(reverse_pnls) else 0.0
        if math.isnan(corr):
            corr = 0.0
        
        e12_triggered = abs(corr) > self._max_correlation_threshold
        return {
            "e12_triggered": e12_triggered,
            "correlation": float(corr),
            "threshold": self._max_correlation_threshold,
            "n_trades": min(len(main_trades), len(reverse_trades)),
            "reason": "high_correlation_with_main" if e12_triggered else "independent",
        }


class E13ShadowStrategyCollusionDetector:
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
                      "hard_time_stop_minutes", "max_risk_ratio"]
        diffs = []
        for k in key_params:
            if k in main_params and k in shadow_params and main_params[k] != 0:
                diffs.append(abs(shadow_params[k] - main_params[k]) / abs(main_params[k]))
        avg_diff = sum(diffs) / len(diffs) if diffs else 0.0
        param_diff_passed = avg_diff >= self._min_param_diff_pct
        
        sync_count = 0
        min_signals = min(len(main_signals), len(shadow_signals))
        for i in range(min_signals):
            if (main_signals[i].get('direction') == shadow_signals[i].get('direction') and
                main_signals[i].get('instrument_id') == shadow_signals[i].get('instrument_id')):
                sync_count += 1
        sync_rate = sync_count / min_signals if min_signals > 0 else 0.0
        sync_passed = sync_rate <= self._max_signal_sync_rate
        
        e13_triggered = not param_diff_passed or not sync_passed
        return {
            "e13_triggered": e13_triggered,
            "param_diff_pct": avg_diff,
            "param_diff_passed": param_diff_passed,
            "signal_sync_rate": sync_rate,
            "sync_passed": sync_passed,
            "reason": "collusion_detected" if e13_triggered else "independent",
        }


class MultiStateSwitchBacktestScenario:
    def __init__(self, state_sequence: List[str] = None,
                 min_hold_bars: int = 10):
        if state_sequence is None:
            state_sequence = ["correct_trending", "other", "incorrect_reversal",
                              "correct_trending", "other"]
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
            except Exception as e:
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
    def __init__(self, tail_risk_threshold: float = 0.05,
                 minsky_threshold: float = 0.3,
                 state_dependency_threshold: float = 0.8):
        self._tail_risk_threshold = tail_risk_threshold
        self._minsky_threshold = minsky_threshold
        self._state_dependency_threshold = state_dependency_threshold

    def check_e8_tail_risk(self, returns: List[float]) -> Dict[str, Any]:
        if not returns:
            return {"e8_triggered": False, "tail_ratio": 0.0}
        sorted_returns = sorted(returns)
        n = len(sorted_returns)
        tail_5pct = sorted_returns[:max(1, n // 20)]
        tail_mean = sum(tail_5pct) / len(tail_5pct) if tail_5pct else 0.0
        overall_mean = sum(returns) / len(returns)
        tail_ratio = abs(tail_mean / overall_mean) if overall_mean != 0 else 0.0
        e8_triggered = tail_ratio > self._tail_risk_threshold
        return {
            "e8_triggered": e8_triggered,
            "tail_ratio": tail_ratio,
            "threshold": self._tail_risk_threshold,
        }

    def check_e9_minsky(self, leverage_history: List[float]) -> Dict[str, Any]:
        if not leverage_history:
            return {"e9_triggered": False, "leverage_trend": 0.0}
        x = list(range(len(leverage_history)))
        n = len(x)
        x_mean = sum(x) / n
        y_mean = sum(leverage_history) / n
        num = sum((x[i] - x_mean) * (leverage_history[i] - y_mean) for i in range(n))
        den = sum((x[i] - x_mean) ** 2 for i in range(n))
        slope = num / den if den != 0 else 0.0
        e9_triggered = slope > self._minsky_threshold
        return {
            "e9_triggered": e9_triggered,
            "leverage_trend": slope,
            "threshold": self._minsky_threshold,
        }

    def check_e10_state_dependency(self, state_returns: Dict[str, List[float]]) -> Dict[str, Any]:
        if not state_returns or len(state_returns) < 2:
            return {"e10_triggered": False, "state_variance_ratio": 0.0}
        state_means = {}
        for state, rets in state_returns.items():
            state_means[state] = sum(rets) / len(rets) if rets else 0.0
        overall_mean = sum(state_means.values()) / len(state_means)
        between_var = sum((m - overall_mean) ** 2 for m in state_means.values()) / len(state_means)
        all_rets = [r for rets in state_returns.values() for r in rets]
        within_var = sum((r - overall_mean) ** 2 for r in all_rets) / len(all_rets) if all_rets else 1.0
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
    def __init__(self, residual_threshold_pct: float = 15.0):
        self._residual_threshold_pct = residual_threshold_pct

    def check(self, pnl_attribution: Dict[str, float]) -> Dict[str, Any]:
        total_pnl = pnl_attribution.get("total_pnl", 0.0)
        explained_pnl = (
            pnl_attribution.get("delta_pnl", 0.0) +
            pnl_attribution.get("gamma_pnl", 0.0) +
            pnl_attribution.get("vega_pnl", 0.0) +
            pnl_attribution.get("theta_pnl", 0.0)
        )
        residual = total_pnl - explained_pnl
        residual_pct = abs(residual / total_pnl * 100) if total_pnl != 0 else 0.0
        e7_triggered = residual_pct > self._residual_threshold_pct
        return {
            "e7_triggered": e7_triggered,
            "residual_pct": residual_pct,
            "threshold_pct": self._residual_threshold_pct,
            "total_pnl": total_pnl,
            "explained_pnl": explained_pnl,
            "residual": residual,
            "reason": "unexplained_return_exceeds_threshold" if e7_triggered else "ok",
        }


class E11QuantitativeSourceChecker:
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
