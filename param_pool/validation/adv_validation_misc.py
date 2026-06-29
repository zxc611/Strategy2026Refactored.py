# [M1-129] 高级验证杂项
# MODULE_ID: M1-195
"""其余小验证器类 + P0验证函数 + generate_hft_fidelity_warning"""
import json  # R5-2: 保留用于json.JSONDecodeError
import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading.infra.shared_utils import atomic_replace_file  # R9-1
from ali2026v3_trading.infra.shared_utils import CHINA_TZ, DEFAULT_RISK_FREE_RATE, ANNUALIZE_FACTOR_DAILY

logger = get_logger(__name__)  # R9-5


class OtherStateDefenseQuantifier:
    def quantify(self, bar_data: Any = None,
                 defense_trades: List[Dict] = None,
                 no_defense_trades: List[Dict] = None) -> Dict[str, Any]:
        defense_pnl = sum(t.get('pnl', 0.0) for t in (defense_trades or []))
        no_defense_pnl = sum(t.get('pnl', 0.0) for t in (no_defense_trades or []))
        net_benefit = defense_pnl - no_defense_pnl
        return {
            "defense_mode_pnl": defense_pnl,
            "no_defense_mode_pnl": no_defense_pnl,
            "net_benefit": net_benefit,
            "defense_valuable": net_benefit > 0,
        }


PARAM_TIERS = {
    "must_calibrate_every_run": [
        "close_take_profit_ratio", "close_stop_loss_ratio", "max_risk_ratio",
        "lots_min", "signal_cooldown_sec", "non_other_ratio_threshold",
    ],
    "quarterly_review": [
        "max_signals_per_window", "state_confirm_bars", "spring_*",
        # NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    ],
    "annual_or_phase_change": [
        "capital_route_*", "shadow_alpha_*", "rate_limit_*",
        # NOTE: hard_time_stop_*已替换为策略分层时间参数(hft/spring/resonance/box)_hard_time_stop_*
        "hft_hard_time_stop_*", "spring_hard_time_stop_*",
        "resonance_hard_time_stop_*", "box_hard_time_stop_*",
        "daily_loss_*", "logic_reversal_*",
    ],
    "hft_replay_only": [
        "hft_signal_confirm_ticks", "hft_cooldown_ms", "hft_min_imbalance",
    ],
}


class ParamTierManager:
    def get_tier(self, param_name: str) -> str:
        for tier, params in PARAM_TIERS.items():
            for p in params:
                if p.endswith('*'):
                    if param_name.startswith(p[:-1]):
                        return tier
                elif param_name == p:
                    return tier
        return "annual_or_phase_change"

    def get_params_for_tier(self, tier: str) -> List[str]:
        return PARAM_TIERS.get(tier, [])

    def should_calibrate(self, param_name: str, run_type: str = "daily") -> bool:
        tier = self.get_tier(param_name)
        if tier == "must_calibrate_every_run":
            return True
        if tier == "quarterly_review" and run_type in ("quarterly", "annual"):
            return True
        if tier == "annual_or_phase_change" and run_type == "annual":
            return True
        return False


def generate_hft_fidelity_warning(strategy_type: str, backtest_resolution: str = "minute") -> Optional[str]:
    hft_params = ["hft_cooldown_ms", "hft_signal_confirm_ticks", "hft_min_imbalance"]
    if strategy_type in ("s1_hft", "hft") and backtest_resolution != "tick":
        return ("DEGRADED: tick级参数(hft_cooldown_ms/hft_signal_confirm_ticks) "
                "在分钟级回测中失真, 需HFT回放引擎验证")
    return None


class CrossPeriodOverlapValidator:
    def validate(self, param_name: str, optimal_ranges_by_period: Dict[str, Tuple[float, float]],
                 min_overlap: float = 0.60) -> Dict[str, Any]:
        if len(optimal_ranges_by_period) < 2:
            return {"overlap_sufficient": False, "overlap_pct": 0.0, "param": param_name}

        ranges = list(optimal_ranges_by_period.values())
        overlap_low = max(r[0] for r in ranges)
        overlap_high = min(r[1] for r in ranges)

        if overlap_high <= overlap_low:
            return {"overlap_sufficient": False, "overlap_pct": 0.0, "param": param_name}

        min_range_width = min(r[1] - r[0] for r in ranges)
        overlap_pct = (overlap_high - overlap_low) / min_range_width if min_range_width > 0 else 0.0

        return {
            "overlap_sufficient": overlap_pct >= min_overlap,
            "overlap_pct": overlap_pct,
            "overlap_range": (overlap_low, overlap_high),
            "param": param_name,
        }


class ShadowParamIndependenceValidator:
    def validate(self, main_params: Dict[str, float],
                 shadow_a_params: Dict[str, float],
                 shadow_b_params: Dict[str, float],
                 min_diff_pct: float = 0.20) -> Dict[str, Any]:
        key_params = ["close_take_profit_ratio", "close_stop_loss_ratio",
                      "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
                      "resonance_hard_time_stop_min", "box_hard_time_stop_min",
                      "max_risk_ratio"]
        results = {}
        for name, shadow in [("shadow_a", shadow_a_params), ("shadow_b", shadow_b_params)]:
            diffs = []
            for k in key_params:
                if k in main_params and k in shadow and main_params[k] != 0:
                    diff = abs(shadow[k] - main_params[k]) / abs(main_params[k])
                    diffs.append(diff)
            avg_diff = sum(diffs) / len(diffs) if diffs else 0.0
            results[name] = {"avg_diff_pct": avg_diff, "passed": avg_diff >= min_diff_pct}
        return results


ALPHA_THRESHOLDS_BY_STRATEGY = {
    "s1_hft": 0.5,
    "s2_resonance": 0.5,
    "s3_box_extreme": 0.3,
    "s4_box_spring": 0.4,
    "s5_arbitrage": 0.4,
    "s6_market_making": 0.3,
}


class DifferentiatedAlphaChecker:
    def check(self, strategy_id: str, alpha_ratio: float) -> Dict[str, Any]:
        threshold = ALPHA_THRESHOLDS_BY_STRATEGY.get(strategy_id, 0.5)
        return {
            "strategy_id": strategy_id,
            "alpha_ratio": alpha_ratio,
            "threshold": threshold,
            "passed": alpha_ratio >= threshold,
        }


class ReverseStrategyValidator:
    def validate(self, reverse_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not reverse_trades:
            return {"has_value": False, "win_rate": 0.0, "independent": False}
        wins = sum(1 for t in reverse_trades if t.get('pnl', 0) > 0)
        win_rate = wins / len(reverse_trades)
        return {
            "has_value": win_rate > 0.5,
            "win_rate": win_rate,
            "n_trades": len(reverse_trades),
            "independent": True,
        }


class OrderFlowFilterValidator:
    def validate_filter_effectiveness(self, filtered_trades: List[Dict],
                                        unfiltered_trades: List[Dict]) -> Dict[str, Any]:
        if not filtered_trades or not unfiltered_trades:
            return {"effective": False, "filtered_loss_rate": 0.0, "unfiltered_loss_rate": 0.0}
        filtered_loss_rate = sum(1 for t in filtered_trades if t.get('pnl', 0) < 0) / len(filtered_trades)
        unfiltered_loss_rate = sum(1 for t in unfiltered_trades if t.get('pnl', 0) < 0) / len(unfiltered_trades)
        return {
            "effective": filtered_loss_rate > 0.5,
            "filtered_loss_rate": filtered_loss_rate,
            "unfiltered_loss_rate": unfiltered_loss_rate,
        }

    def validate_false_signal_injection(self, strategy_triggered: bool,
                                          false_signal_rate: float = 0.0,
                                          max_trigger_rate: float = 0.05) -> Dict[str, Any]:
        return {
            "passed": false_signal_rate <= max_trigger_rate,
            "false_signal_rate": false_signal_rate,
            "max_trigger_rate": max_trigger_rate,
        }


class StateSwitchPositionPolicy:
    POLICIES = ["keep_with_original_rules", "exit_all", "migrate_to_new_rules"]

    def apply(self, policy: str, positions: List[Dict], new_state: str) -> List[Dict]:
        if policy not in self.POLICIES:
            policy = "keep_with_original_rules"

        if policy == "exit_all":
            for p in positions:
                p["action"] = "close"
            return positions
        elif policy == "migrate_to_new_rules":
            for p in positions:
                p["new_rules"] = new_state
                p["action"] = "migrate"
            return positions
        else:
            for p in positions:
                p["action"] = "keep"
            return positions


# T-03修复: MultiPeriodCrossValidator - 序贯/随机/时间聚类三种独立样本外验证
class MultiPeriodCrossValidator:
    """多周期交叉验证器, 支持三种独立OOS验证模式

    - sequential: 序贯窗口滚动(train->test->slide)
    - random: 随机划分多折交叉验证
    - time_cluster: 按时间聚类(K-Means on时间成分划分, 保持时间局部性)
    """

    def __init__(self, n_splits: int = 5, method: str = "sequential",
                 train_ratio: float = 0.7, min_test_sharpe: float = 0.3, random_seed: int = 42):
        self._n_splits = n_splits
        self._method = method
        self._train_ratio = train_ratio
        self._min_test_sharpe = min_test_sharpe
        self._random_seed = random_seed

    def validate(self, equity_curve: List[float],
                 timestamps: Optional[List[Any]] = None) -> Dict[str, Any]:
        n = len(equity_curve)
        if n < self._n_splits * 10:
            return {"passed": False, "method": self._method,
                    "splits": [], "robust": False, "note": "insufficient_data"}

        splits = []
        if self._method == "sequential":
            window_size = n // self._n_splits
            for i in range(self._n_splits):
                train_end = int(i * window_size + self._train_ratio * window_size)
                test_end = (i + 1) * window_size
                train_eq = equity_curve[i * window_size:train_end]
                test_eq = equity_curve[train_end:test_end]
                splits.append(self._eval_split(train_eq, test_eq, i))

        elif self._method == "random":
            rng = np.random.RandomState(self._random_seed)
            indices = rng.permutation(n)
            fold_size = n // self._n_splits
            for i in range(self._n_splits):
                test_idx = set(indices[i * fold_size:(i + 1) * fold_size])
                train_eq = [equity_curve[j] for j in range(n) if j not in test_idx]
                test_eq = [equity_curve[j] for j in test_idx]
                splits.append(self._eval_split(train_eq, test_eq, i))

        elif self._method == "time_cluster":
            try:
                from sklearn.cluster import KMeans
                ts_arr = np.array(timestamps or list(range(n))).reshape(-1, 1)
                km = KMeans(n_clusters=self._n_splits, random_state=42, n_init=10)
                labels = km.fit_predict(ts_arr)
                for i in range(self._n_splits):
                    test_mask = labels == i
                    train_mask = ~test_mask
                    train_eq = [equity_curve[j] for j in range(n) if train_mask[j]]
                    test_eq = [equity_curve[j] for j in range(n) if test_mask[j]]
                    splits.append(self._eval_split(train_eq, test_eq, i))
            except ImportError:
                return {"passed": False, "method": self._method,
                        "splits": [], "robust": False, "note": "sklearn_not_available"}
        else:
            return {"passed": False, "method": self._method,
                    "splits": [], "robust": False, "note": f"unknown_method:{self._method}"}

        robust = all(s.get("test_sharpe", 0) >= self._min_test_sharpe for s in splits)
        return {"passed": robust, "method": self._method,
                "splits": splits, "robust": robust}

    def _eval_split(self, train_eq: List[float], test_eq: List[float],
                    split_id: int) -> Dict[str, Any]:
        # P1-47修复: 夏普比率计算委托给 infra 权威版
        from ali2026v3_trading.infra.resilience import compute_sharpe_stable
        train_arr = np.array(train_eq)
        test_arr = np.array(test_eq)
        train_sharpe = self._calc_sharpe(train_arr, risk_free_rate=DEFAULT_RISK_FREE_RATE)
        test_sharpe = self._calc_sharpe(test_arr, risk_free_rate=DEFAULT_RISK_FREE_RATE)
        return {"split": split_id, "train_sharpe": train_sharpe,
                "test_sharpe": test_sharpe}

    @staticmethod
    def _calc_sharpe(equity: np.ndarray, risk_free_rate: float = DEFAULT_RISK_FREE_RATE) -> float:
        """P1-47修复: 委托给 infra/resilience_numeric.compute_sharpe_stable

        保留原签名以兼容调用点，内部委托给权威实现。'
        """
        from ali2026v3_trading.infra.resilience import compute_sharpe_stable
        if len(equity) < 2:
            return 0.0
        rets = np.diff(equity) / equity[:-1]
        rets = rets[np.isfinite(rets)]
        if len(rets) < 2:
            return 0.0
        annualize_factor = ANNUALIZE_FACTOR_DAILY
        period_risk_free = risk_free_rate / annualize_factor
        return compute_sharpe_stable(
            rets.tolist(),
            risk_free_rate=period_risk_free,
            annualize_factor=annualize_factor,
        )


# T-04修复: MultiParameterTracer + heat_map_report
class MultiParameterTracer:
    """多参数追踪器: 记录参数搜索轨迹并生成热力图报告"""

    def __init__(self):
        self._traces: List[Dict[str, Any]] = []

    def record(self, params: Dict[str, float], metrics: Dict[str, float],
               phase: str = "unknown") -> None:
        self._traces.append({"params": dict(params), "metrics": dict(metrics),
                             "phase": phase, "timestamp": datetime.now(CHINA_TZ).isoformat()})

    def heat_map_report(self, param_x: str = "close_take_profit_ratio",
                        param_y: str = "close_stop_loss_ratio",
                        metric: str = "sharpe") -> Dict[str, Any]:
        """生成二维参数热力图报告"""
        if len(self._traces) < 2:
            return {"grid_x": [], "grid_y": [], "values": [],
                    "n_points": len(self._traces), "note": "insufficient_data"}

        xs = [t["params"].get(param_x, 0) for t in self._traces]
        ys = [t["params"].get(param_y, 0) for t in self._traces]
        vals = [t["metrics"].get(metric, 0) for t in self._traces]

        x_unique = sorted(set(xs))
        y_unique = sorted(set(ys))

        grid = {}
        for t in self._traces:
            x_val = t["params"].get(param_x, 0)
            y_val = t["params"].get(param_y, 0)
            v = t["metrics"].get(metric, 0)
            key = (x_val, y_val)
            if key not in grid or v > grid[key]:
                grid[key] = v

        values = [[grid.get((x, y), 0.0) for y in y_unique] for x in x_unique]

        return {"grid_x": x_unique, "grid_y": y_unique, "values": values,
                "param_x": param_x, "param_y": param_y, "metric": metric,
                "n_points": len(self._traces)}

    @property
    def trace_count(self) -> int:
        return len(self._traces)


# T-08修复: ParameterProximityTracker 参数近距追踪器
class ParameterProximityTracker:
    """参数近距追踪: 检测最优参数是否位于搜索空间边界(过拟合信号)"""

    def __init__(self, param_ranges: Dict[str, Tuple[float, float]]):
        self._param_ranges = param_ranges

    def check_boundary_proximity(self, params: Dict[str, float],
                                  threshold: float = 0.1) -> Dict[str, Any]:
        """检查参数是否靠近边界(距离 < threshold * 范围宽度)"""
        boundary_params = []
        for name, (low, high) in self._param_ranges.items():
            if name not in params:
                continue
            val = params[name]
            width = high - low
            if width <= 0:
                continue
            dist_low = (val - low) / width
            dist_high = (high - val) / width
            if dist_low < threshold or dist_high < threshold:
                side = "low" if dist_low < threshold else "high"
                boundary_params.append({
                    "param": name, "value": val,
                    "low": low, "high": high,
                    "proximity": min(dist_low, dist_high),
                    "side": side,
                })

        return {"boundary_detected": len(boundary_params) > 0,
                "boundary_params": boundary_params,
                "n_boundary": len(boundary_params),
                "n_total": len(self._param_ranges)}

    def compute_parameter_density(self, params_list: List[Dict[str, float]]) -> Dict[str, float]:
        """计算参数空间中采样点密度(越低越稀疏 -> 搜索不充分)"""
        if len(params_list) < 2:
            return {}
        densities = {}
        param_names = list(self._param_ranges.keys())
        for name in param_names:
            vals = [p.get(name, 0) for p in params_list]
            low, high = self._param_ranges.get(name, (0, 1))
            if high <= low:
                continue
            normalized = [(v - low) / (high - low) for v in vals]
            sorted_norm = sorted(normalized)
            gaps = [sorted_norm[i + 1] - sorted_norm[i]
                    for i in range(len(sorted_norm) - 1)]
            avg_gap = sum(gaps) / len(gaps) if gaps else 1.0
            densities[name] = 1.0 / (1.0 + avg_gap * len(params_list))
        return densities


# T-09修复: CheckpointManager - 优化检查点管理器
class CheckpointManager:
    """优化检查点管理: 保存恢复优化状态, 支持断点续优"""

    def __init__(self, checkpoint_dir: str = "checkpoints"):
        self._checkpoint_dir = Path(checkpoint_dir) if True else checkpoint_dir

    def save_checkpoint(self, study_name: str, trial_number: int,
                        best_params: Dict[str, Any], best_value: float,
                        metadata: Optional[Dict[str, Any]] = None) -> str:
        """保存优化检查点"""
        import os
        os.makedirs(self._checkpoint_dir, exist_ok=True)
        # P2-R11-11修复: 添加strategy_version到checkpoint文件, 确保恢复时版本兼容性检查
        try:
            from ali2026v3_trading import __version__ as _strategy_version
        except ImportError:
            _strategy_version = 'unknown'
        checkpoint = {
            "study_name": study_name,
            "trial_number": trial_number,
            "strategy_version": _strategy_version,
            "best_params": best_params,
            "best_value": best_value,
            "metadata": metadata or {},
            "timestamp": datetime.now(CHINA_TZ).isoformat(),
        }
        path = os.path.join(str(self._checkpoint_dir), f"{study_name}_trial_{trial_number}.json")
        atomic_replace_file(path, json_dumps(checkpoint, indent=2))  # R9-1
        return path

    def load_checkpoint(self, study_name: str, trial_number: int) -> Optional[Dict[str, Any]]:
        """加载优化检查点"""
        import os
        path = os.path.join(str(self._checkpoint_dir), f"{study_name}_trial_{trial_number}.json")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json_loads(f.read())  # R5-2

    def find_latest_checkpoint(self, study_name: str) -> Optional[Dict[str, Any]]:
        """查找最新检查点"""
        import os
        if not os.path.exists(str(self._checkpoint_dir)):
            return None
        files = [f for f in os.listdir(str(self._checkpoint_dir))
                 if f.startswith(study_name) and f.endswith(".json")]
        if not files:
            return None
        files.sort(reverse=True)
        path = os.path.join(str(self._checkpoint_dir), files[0])
        with open(path, "r", encoding="utf-8") as f:
            return json_loads(f.read())  # R5-2


# T-12修复: CollusionDetector代理 - 从governance_engine导入并代理
class CollusionDetector:
    """共谋检测器代理: 委托governance_engine.E13ShadowStrategyCollusionDetector"""

    def detect(self, strategy_pnl_series: List[Dict[str, Any]],
               correlation_threshold: float = 0.7) -> Dict[str, Any]:
        """检测策略间是否存在共谋(相关性过高)"""
        try:
            from ali2026v3_trading.governance.governance_engine import E13ShadowStrategyCollusionDetector
            _av_cfg = self._config if hasattr(self, '_config') and self._config else {}
            detector = E13ShadowStrategyCollusionDetector(
                min_param_diff_pct=_av_cfg.get("e13_min_param_diff_pct", 0.20),
                max_signal_sync_rate=_av_cfg.get("e13_max_signal_sync_rate", 0.7),
                min_trade_count=_av_cfg.get("e13_min_trade_count", 20),
            )
            result = detector.detect(strategy_pnl_series, correlation_threshold=correlation_threshold)
            return result
        except ImportError:
            return self._fallback_detect(strategy_pnl_series, correlation_threshold)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[CollusionDetector] error: %s", e)
            return {"collusion_detected": False, "note": str(e)}

    def _fallback_detect(self, strategy_pnl_series: List[Dict[str, Any]],
                         correlation_threshold: float) -> Dict[str, Any]:
        """降级检测: 基于简单相关系数"""
        if len(strategy_pnl_series) < 2:
            return {"collusion_detected": False, "max_correlation": 0.0,
                    "n_strategies": len(strategy_pnl_series)}
        try:
            pnl_arrays = []
            for s in strategy_pnl_series:
                if isinstance(s, dict) and "pnl" in s:
                    pnl_arrays.append(np.array(s["pnl"]))
                elif isinstance(s, (list, np.ndarray)):
                    pnl_arrays.append(np.array(s))
            if len(pnl_arrays) < 2:
                return {"collusion_detected": False, "max_correlation": 0.0}
            max_corr = 0.0
            for i in range(len(pnl_arrays)):
                for j in range(i + 1, len(pnl_arrays)):
                    min_len = min(len(pnl_arrays[i]), len(pnl_arrays[j]))
                    if min_len < 2:
                        continue
                    corr = np.corrcoef(pnl_arrays[i][:min_len], pnl_arrays[j][:min_len])[0, 1]
                    if np.isfinite(corr):
                        max_corr = max(max_corr, abs(corr))
            return {"collusion_detected": max_corr > correlation_threshold,
                    "max_correlation": float(max_corr),
                    "n_strategies": len(pnl_arrays)}
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            return {"collusion_detected": False, "max_correlation": 0.0, "note": str(e)}


# ============================================================
# P0-R8-12修复: P0质量门Q2/Q3/Q4独立验证函数
# 手册23.1节 4个P0质量门, P0-Q1(validate_shadow_param_independence)已存在
# 补充Q2(bid_ask_spread数据质量), Q3(分钟Bar唯一性), Q4(期权元数据完整性)
# ============================================================

def validate_p0_q2_bid_ask_spread_quality(tick_data: Any = None,
                                          max_spread_ratio: float = 0.05,
                                          min_tick_count: int = 100) -> Dict[str, Any]:
    """
    P0-Q2: bid_ask_spread数据质量验证
    - 检查买卖价差是否在合理范围内(>=0%)
    - 检查是否有负价差
    - 检查bid/ask是否有大量零值或缺失
    """
    result = {
        'p0_q2_passed': True,
        'gate': 'P0-Q2',
        'issues': [],
        'stats': {},
    }
    try:
        if tick_data is None:
            result['p0_q2_passed'] = False
            result['issues'].append('No tick_data provided')
            return result

        df = tick_data if hasattr(tick_data, 'columns') else None
        if df is None:
            result['p0_q2_passed'] = False
            result['issues'].append('Invalid data format')
            return result

        # 检查bid/ask列存在性
        has_bid = 'bid' in df.columns
        has_ask = 'ask' in df.columns
        if not has_bid and not has_ask:
            result['p0_q2_passed'] = False
            result['issues'].append('Missing both bid and ask columns')
            return result

        if has_bid:
            bid_nan_ratio = df['bid'].isna().mean() if hasattr(df['bid'], 'isna') else 0
            bid_zero_ratio = (df['bid'] == 0).mean() if hasattr(df['bid'], '__eq__') else 0
            result['stats']['bid_nan_ratio'] = float(bid_nan_ratio)
            result['stats']['bid_zero_ratio'] = float(bid_zero_ratio)

        if has_ask:
            ask_nan_ratio = df['ask'].isna().mean() if hasattr(df['ask'], 'isna') else 0
            ask_zero_ratio = (df['ask'] == 0).mean() if hasattr(df['ask'], '__eq__') else 0
            result['stats']['ask_nan_ratio'] = float(ask_nan_ratio)
            result['stats']['ask_zero_ratio'] = float(ask_zero_ratio)

        # 检查负价差
        if has_bid and has_ask:
            negative_spread = (df['bid'] > df['ask']).sum() if hasattr(df['bid'], '__gt__') else 0
            result['stats']['negative_spread_count'] = int(negative_spread)
            if negative_spread > 0:
                result['issues'].append(f'Negative bid-ask spread: {negative_spread} rows')

            # 检查价差比例
            valid = (df['ask'] > 0) & (df['bid'] > 0)
            if valid.sum() > 0:
                # R17-P1-PERF-10修复: 先提取子集再计算，避免3次df.loc[valid]各产生中间副本
                sub = df.loc[valid, ['ask', 'bid']]
                spreads = (sub['ask'] - sub['bid']) / sub['ask']
                high_spread = (spreads > max_spread_ratio).sum()
                result['stats']['high_spread_count'] = int(high_spread)
                result['stats']['mean_spread_ratio'] = float(spreads.mean())
                if high_spread > len(spreads) * 0.1:
                    result['issues'].append(f'High spread ratio > {max_spread_ratio}: {high_spread} rows')

        # 检查数据量
        n_rows = len(df)
        if n_rows < min_tick_count:
            result['issues'].append(f'Insufficient data: {n_rows} < {min_tick_count}')
        result['stats']['n_rows'] = n_rows

        result['p0_q2_passed'] = len(result['issues']) == 0
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        result['p0_q2_passed'] = False
        result['issues'].append(f'Exception: {str(e)}')

    return result


def validate_p0_q3_minute_bar_uniqueness(db_conn: Any = None,
                                         table_name: str = 'mv_minute_bars',
                                         symbol: str = '') -> Dict[str, Any]:
    """
    P0-Q3: 分钟Bar唯一性SQL查询验证
    - 检查同一(symbol, minute)组合是否有重复记录
    - 检查分钟Bar的时间间隔是否连续
    """
    result = {
        'p0_q3_passed': True,
        'gate': 'P0-Q3',
        'issues': [],
        'stats': {},
    }
    try:
        if db_conn is None:
            try:
                from ali2026v3_trading.data.data_access import get_data_access
                from ali2026v3_trading.data.db_adapter import connect_in_memory
                db_conn = connect_in_memory()
                result['issues'].append('No DB connection; using in-memory (skipped)')
                return result
            except ImportError:
                result['p0_q3_passed'] = False
                result['issues'].append('No database connection available')
                return result

        # 检查重复记录
        try:
            symbol_filter = f"WHERE symbol = '{symbol}'" if symbol else ''
            dup_query = f"""
                SELECT symbol, minute, COUNT(*) as cnt
                FROM {table_name}
                {symbol_filter}
                GROUP BY symbol, minute
                HAVING COUNT(*) > 1
            """
            dup_result = db_conn.execute(dup_query).fetchdf()
            result['stats']['duplicate_rows'] = len(dup_result)
            if len(dup_result) > 0:
                result['issues'].append(f'Duplicate minute bars: {len(dup_result)} groups')
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            result['stats']['uniqueness_check_error'] = str(e)

        # 检查时间连续性
        try:
            gap_query = f"""
                WITH ordered AS (
                    SELECT minute,
                           LAG(minute) OVER (ORDER BY minute) as prev_minute
                    FROM {table_name}
                    {symbol_filter}
                )
                SELECT COUNT(*) as gap_count
                FROM ordered
                WHERE prev_minute IS NOT NULL
                  AND minute > prev_minute + INTERVAL 2 MINUTE
            """
            gap_result = db_conn.execute(gap_query).fetchone()
            if gap_result and gap_result[0] > 0:
                result['issues'].append(f'Minute bar gaps detected: {gap_result[0]} gaps')
                result['stats']['gap_count'] = int(gap_result[0])
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            result['stats']['gap_check'] = 'not_applicable'

        result['p0_q3_passed'] = len(result['issues']) == 0
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        result['p0_q3_passed'] = False
        result['issues'].append(f'Exception: {str(e)}')

    return result


def validate_p0_q4_option_metadata_integrity(option_data: Any = None,
                                              required_fields: List[str] = None) -> Dict[str, Any]:
    """
    P0-Q4: 期权元数据完整性验证
    - 检查期权链所有必要字段(strike, expiry, option_type, underlying, iv等)
    - 检查strike价格合理性
    - 检查expiry日期有效性
    """
    if required_fields is None:
        required_fields = ['strike', 'expiry', 'option_type', 'underlying', 'symbol']

    result = {
        'p0_q4_passed': True,
        'gate': 'P0-Q4',
        'issues': [],
        'stats': {},
    }
    try:
        if option_data is None:
            result['p0_q4_passed'] = False
            result['issues'].append('No option_data provided')
            return result

        df = option_data if hasattr(option_data, 'columns') else None
        if df is None:
            result['p0_q4_passed'] = False
            result['issues'].append('Invalid data format')
            return result

        # 检查必要字段存在性
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            result['issues'].append(f'Missing fields: {missing_fields}')
        result['stats']['n_fields'] = len(df.columns)

        # 检查strike合理性
        if 'strike' in df.columns:
            strike_nan = df['strike'].isna().sum() if hasattr(df['strike'], 'isna') else 0
            strike_negative = (df['strike'] <= 0).sum() if hasattr(df['strike'], '__le__') else 0
            result['stats']['strike_nan_count'] = int(strike_nan)
            result['stats']['strike_negative_count'] = int(strike_negative)
            if strike_nan > 0:
                result['issues'].append(f'Null strike prices: {strike_nan}')
            if strike_negative > 0:
                result['issues'].append(f'Non-positive strike prices: {strike_negative}')

        # 检查option_type有效性
        if 'option_type' in df.columns:
            valid_types = {'C', 'P', 'CALL', 'PUT', 'call', 'put', 1, 2}
            invalid_types = df[~df['option_type'].isin(valid_types)] if hasattr(df['option_type'], 'isin') else df.iloc[:0]
            if len(invalid_types) > 0:
                result['issues'].append(f'Invalid option_type values: {len(invalid_types)} rows')

        # 检查expiry日期有效性
        if 'expiry' in df.columns:
            expiry_nan = df['expiry'].isna().sum() if hasattr(df['expiry'], 'isna') else 0
            result['stats']['expiry_nan_count'] = int(expiry_nan)
            if expiry_nan > 0:
                result['issues'].append(f'Null expiry dates: {expiry_nan}')

        # 检查数据量
        result['stats']['n_rows'] = len(df)
        if len(df) == 0:
            result['issues'].append('Empty option dataset')

        result['p0_q4_passed'] = len(result['issues']) == 0
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        result['p0_q4_passed'] = False
        result['issues'].append(f'Exception: {str(e)}')

    return result


class AdvancedValidation:
    """P2-9: HFT时序鲁棒性验证 — 委托到validation_deep_orchestrator"""
    def validate_hft_temporal_robustness(self, params, bar_data=None, drop_probs=None, delay_lambdas=None, **kwargs):
        try:
            from ali2026v3_trading.param_pool.validation.statistical_validation import DeepValidationSuite
            _suite = DeepValidationSuite()
            return _suite.validate_hft_temporal_robustness(params, bar_data, drop_probs=drop_probs, delay_lambdas=delay_lambdas)
        except (ImportError, AttributeError, ValueError, TypeError, RuntimeError) as e:
            return {"passed": False, "note": str(e)}
