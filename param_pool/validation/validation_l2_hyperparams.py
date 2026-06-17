# [M1-130] L2超参验证
# MODULE_ID: M1-199
"""validation_l2_hyperparams.py - L-2超参数处理函数与常量"""

from __future__ import annotations

import itertools
import logging
from ali2026v3_trading.param_pool._param_grids import L2_HYPERPARAMS, L2_PARAM_GRID, L2_CONFLICT_RESOLUTION, PARAM_SOURCE_ANNOTATION
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import re as _re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ali2026v3_trading.param_pool._param_grids import _sync_random_seed, _BacktestState

from ali2026v3_trading.param_pool._param_defaults import PARAM_DEFAULTS

logger = get_logger(__name__)  # R9-5


# ============================================================================
# V7.1 张力修补：L-2超参数处理 + 统计功效 + Alpha置信区间
# ============================================================================



# ============================================================================
# V7.1 Step 1: L-2基岩参数独立优化（目标函数=状态判定准确率，非策略Sharpe）
# ============================================================================






_DDL_COLUMN_SAFE_PATTERN = _re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def check_l2_conflict(
    l2_params_independent: Dict[str, float],
    l2_params_main: Dict[str, float],
    tolerance: float = 0.1,
) -> Dict[str, Any]:
    """比较两套L-2参数是否存在冲突

    对每个共同key，计算相对差异。若任一参数差异超过tolerance，则判定为冲突。
    """
    any_conflict = False
    conflicts = {}
    for key in l2_params_independent:
        if key not in l2_params_main:
            continue
        v_ind = l2_params_independent[key]
        v_main = l2_params_main[key]
        if v_main == 0 and v_ind == 0:
            diff = 0.0
        elif v_main == 0:
            diff = 1.0
        else:
            diff = abs(v_ind - v_main) / abs(v_main)
        conflicts[key] = diff
        if diff > tolerance:
            any_conflict = True
    action = "expand_independent_data_or_manual_review" if any_conflict else "proceed_to_step2"
    return {
        "any_conflict": any_conflict,
        "conflicts": conflicts,
        "action": action,
        "winner": "independent_dataset" if any_conflict else "consistent",
    }


def check_l2_statistical_power(
    bar_data: pd.DataFrame,
    iv_column: str = "iv",
    state_column: str = "state",
    min_transitions_per_regime: int = 100,
    min_fold_overlap: float = 0.60,
    n_folds: int = 5,
) -> Dict[str, Any]:
    """张力二：L-2参数验证的统计功效检查

    通过条件：
    1. 每个市场机制（低/中/高IV）下状态切换次数 >= min_transitions_per_regime
    2. K-fold交叉验证中，最优区间跨fold重叠度 >= min_fold_overlap

    Args:
        min_transitions_per_regime: 每个IV regime下需要的最小状态切换次数
        min_fold_overlap: 跨fold最优区间重叠度阈值
        n_folds: K-fold交叉验证折数
    """
    result = {
        "power_sufficient": False,
        "regime_transitions": {},
        "fold_overlap": 0.0,
        "issues": [],
    }

    if iv_column not in bar_data.columns:
        result["issues"].append(f"缺少{iv_column}列，无法分regime检查")
        return result

    iv_values = bar_data[iv_column].replace(0, np.nan).dropna()
    if len(iv_values) < 100:
        result["issues"].append("IV有效数据不足100条")
        return result

    q33, q66 = iv_values.quantile(0.33), iv_values.quantile(0.66)
    regimes = {
        "low_iv": bar_data[bar_data[iv_column] < q33],
        "mid_iv": bar_data[(bar_data[iv_column] >= q33) & (bar_data[iv_column] < q66)],
        "high_iv": bar_data[bar_data[iv_column] >= q66],
    }

    all_regimes_ok = True
    for name, regime_data in regimes.items():
        n_bars = len(regime_data)
        if state_column in regime_data.columns:
            states = regime_data[state_column]
            valid_mask = states.notna() & states.shift(1).notna()
            transitions = int((states != states.shift(1))[valid_mask].sum())
        else:
            transitions = max(0, n_bars // 240)

        result["regime_transitions"][name] = {
            "bars": n_bars,
            "transitions": int(transitions),
            "sufficient": transitions >= min_transitions_per_regime,
        }
        if transitions < min_transitions_per_regime:
            all_regimes_ok = False
            result["issues"].append(
                f"{name}: 状态切换{transitions}次 < {min_transitions_per_regime}次，功效不足"
            )

    n_total = len(bar_data)
    fold_size = n_total // n_folds
    if fold_size < 50:
        result["issues"].append(f"fold大小{fold_size}不足50，无法做{n_folds}折交叉验证")
        result["fold_overlap"] = 0.0
    else:
        fold_best_indices = []
        for k in range(n_folds):
            start = k * fold_size
            end = min(start + fold_size, n_total)
            fold_data = bar_data.iloc[start:end].copy()
            fold_best_indices.append(set(range(start, end)))

        if len(fold_best_indices) >= 2:
            overlaps = []
            for i in range(len(fold_best_indices)):
                for j in range(i + 1, len(fold_best_indices)):
                    intersection = fold_best_indices[i] & fold_best_indices[j]
                    union = fold_best_indices[i] | fold_best_indices[j]
                    overlap = len(intersection) / len(union) if len(union) > 0 else 0
                    overlaps.append(overlap)
            result["fold_overlap"] = float(np.mean(overlaps))

        if result["fold_overlap"] < min_fold_overlap:
            result["issues"].append(
                f"跨fold重叠度{result['fold_overlap']:.2%} < {min_fold_overlap:.0%}，"
                f"最优区间不稳定"
            )

    result["power_sufficient"] = all_regimes_ok and result["fold_overlap"] >= min_fold_overlap
    return result


def analyze_l2_sensitivity(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    l2_params: Optional[Dict[str, Dict[str, Any]]] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """张力一：L-2超参数敏感性分析

    L-2参数在十八策略扫描中作为超参数固定（不参与网格搜索），
    但需在最终报告中做敏感性分析：±sensitivity_range扰动对结果的影响。
    """
    if l2_params is None:
        l2_params = L2_HYPERPARAMS

    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import run_backtest
    baseline = run_backtest(params, bar_data, train, "main")
    baseline_sharpe = baseline.get("sharpe", 0.0)
    baseline_return = baseline.get("total_return", 0.0)
    results = {"baseline": {"sharpe": baseline_sharpe, "return": baseline_return}, "sensitivity": {}}

    for param_name, meta in l2_params.items():
        base_val = params.get(param_name)
        if base_val is None:
            continue

        sensitivity_range = meta.get("sensitivity_range", 0.05)
        low = base_val - sensitivity_range
        high = base_val + sensitivity_range

        params_low = params.copy()
        params_low[param_name] = low
        params_high = params.copy()
        params_high[param_name] = high

        r_low = run_backtest(params_low, bar_data, train, "main")
        r_high = run_backtest(params_high, bar_data, train, "main")

        sharpe_low = r_low.get("sharpe", 0.0)
        sharpe_high = r_high.get("sharpe", 0.0)
        sharpe_spread = abs(sharpe_high - sharpe_low)
        is_sensitive = sharpe_spread > abs(baseline_sharpe) * 0.3

        results["sensitivity"][param_name] = {
            "base_value": base_val,
            "range": sensitivity_range,
            "low": low,
            "high": high,
            "sharpe_at_low": sharpe_low,
            "sharpe_at_high": sharpe_high,
            "sharpe_spread": sharpe_spread,
            "is_sensitive": is_sensitive,
            "lock_mode": meta.get("lock_mode", "hyperparameter"),
            "warning": f"敏感！{param_name}±{sensitivity_range}导致Sharpe变化{sharpe_spread:.2f}" if is_sensitive else None,
        }

    return results


def compute_alpha_confidence_interval(
    strategy_return: float,
    strategy_sharpe: float,
    n_signals: int,
    confidence: float = 0.95,
) -> Dict[str, float]:
    """Alpha置信区间修正（张力二相关：伪精确性修正）

    不同策略的信号数差异巨大（S1可能10000个，S4可能50个），
    直接比较Sharpe而不给置信区间是伪精确。

    Sharpe的标准误近似: SE(Sharpe) ≈ sqrt((1 + 0.5*Sharpe^2) / n_signals)
    （Bailey & Marquet, 2012）
    """
    if n_signals < 2:
        return {"sharpe_ci_lower": strategy_sharpe, "sharpe_ci_upper": strategy_sharpe,
                "sharpe_se": float("inf"), "warning": "信号数不足，置信区间无意义"}

    sharpe_se = np.sqrt((1 + 0.5 * strategy_sharpe**2) / n_signals)

    z_table = {0.90: 1.645, 0.95: 1.960, 0.99: 2.576}
    z = z_table.get(confidence, 1.960)

    ci_lower = strategy_sharpe - z * sharpe_se
    ci_upper = strategy_sharpe + z * sharpe_se

    ci_width = ci_upper - ci_lower
    if ci_width > 2.0:
        action = "eliminate"
        action_detail = "CI宽度>2.0，Sharpe无统计意义，从策略生态中淘汰"
    elif ci_width > 1.0:
        action = "reduce_weight"
        action_detail = "CI宽度>1.0，Sharpe不可靠，资金分配降权至1/CI_width"
    elif ci_width > 0.5:
        action = "flag"
        action_detail = "CI宽度>0.5，Sharpe中等可靠，标注但可参与分配"
    else:
        action = "reliable"
        action_detail = "CI宽度≤0.5，Sharpe可靠，正常参与分配"

    return {
        "sharpe_ci_lower": ci_lower,
        "sharpe_ci_upper": ci_upper,
        "sharpe_se": sharpe_se,
        "confidence": confidence,
        "n_signals": n_signals,
        "ci_width": ci_width,
        "action": action,
        "action_detail": action_detail,
        "weight_multiplier": 1.0 / ci_width if ci_width > 1.0 else 1.0,
    }


def evaluate_state_accuracy(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    lookahead_bars: int = 10,
) -> Dict[str, Any]:
    """Step 1核心：状态判定准确率评估

    目标函数不是策略Sharpe，而是：
    - correct_trending判定后，后续N分钟价格是否按预期方向运动
    - incorrect_reversal判定后，价格是否反转
    - other判定后，价格是否无明显方向

    Returns:
        state_accuracy: 各状态的预测准确率
        overall_accuracy: 加权整体准确率
        n_transitions: 总状态切换次数
    """
    threshold = params.get("non_other_ratio_threshold", 0.65)
    confirm_bars = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5
    reversal_threshold = params.get("logic_reversal_threshold", 1.5)

    if bar_data.empty or len(bar_data) < lookahead_bars + 20:
        return {"overall_accuracy": 0.0, "n_transitions": 0, "error": "数据不足"}

    closes = bar_data["close"].values
    n = len(closes)

    state_predictions = []
    for i in range(confirm_bars, n - lookahead_bars):
        recent = closes[i - confirm_bars:i + 1]
        if len(recent) < confirm_bars + 1:
            continue

        current_close = closes[i]
        future_close = closes[min(i + lookahead_bars, n - 1)]
        future_return = (future_close - current_close) / current_close if current_close > 0 else 0

        recent_return = (recent[-1] - recent[0]) / recent[0] if recent[0] > 0 else 0
        recent_std = np.std(np.diff(recent) / recent[:-1]) if len(recent) > 1 else 1e-8

        if abs(recent_return) > threshold * recent_std:
            if recent_return > 0:
                predicted_state = "correct_trending"
                predicted_direction = 1
            else:
                predicted_state = "incorrect_reversal"
                predicted_direction = -1
        else:
            predicted_state = "other"
            predicted_direction = 0

        state_predictions.append({
            "state": predicted_state,
            "predicted_direction": predicted_direction,
            "actual_return": future_return,
            "correct": (predicted_direction * future_return > 0) if predicted_direction != 0 else abs(future_return) < threshold * recent_std,
        })

    if not state_predictions:
        return {"overall_accuracy": 0.0, "n_transitions": 0}

    by_state = {}
    total_correct = 0
    total_count = 0
    for pred in state_predictions:
        s = pred["state"]
        if s not in by_state:
            by_state[s] = {"correct": 0, "total": 0}
        by_state[s]["total"] += 1
        if pred["correct"]:
            by_state[s]["correct"] += 1
        total_count += 1
        if pred["correct"]:
            total_correct += 1

    state_accuracy = {}
    for s, counts in by_state.items():
        state_accuracy[s] = {
            "accuracy": counts["correct"] / counts["total"] if counts["total"] > 0 else 0,
            "n": counts["total"],
        }

    overall_accuracy = total_correct / total_count if total_count > 0 else 0

    # P1-裂缝41：增加min_state_accuracy约束
    # 若other占80%且预测准确率仅40%，加权后可能仍>60%，但策略在other下会错误开仓
    # 每个状态的准确率必须 > 50%
    min_state_accuracy_threshold = 0.50
    state_accuracy_pass = True
    for s, acc_info in state_accuracy.items():
        if acc_info["accuracy"] < min_state_accuracy_threshold and acc_info["n"] >= 10:
            state_accuracy_pass = False
            logger.debug(
                "[P1-裂缝41] 状态%s准确率%.2f%% < %.0f%%阈值(n=%d), overall_accuracy不可信",
                s, acc_info["accuracy"] * 100, min_state_accuracy_threshold * 100, acc_info["n"],
            )
    # 若某状态准确率不达标，overall_accuracy降权
    adjusted_accuracy = overall_accuracy if state_accuracy_pass else overall_accuracy * 0.5

    return {
        "state_accuracy": state_accuracy,
        "overall_accuracy": overall_accuracy,
        "adjusted_accuracy": adjusted_accuracy,
        "state_accuracy_pass": state_accuracy_pass,
        "min_state_accuracy_threshold": min_state_accuracy_threshold,
        "n_transitions": total_count,
        "params": {k: params.get(k) for k in L2_PARAM_GRID.keys()},
    }


def optimize_l2_params_step1(
    independent_data: pd.DataFrame,
    lookahead_bars: int = 10,
    min_accuracy: float = 0.55,
    min_transitions: int = 100,
) -> Dict[str, Any]:
    """Step 1: L-2参数独立优化

    在独立数据集上搜索使状态判定准确率最高的L-2参数组合。
    目标函数=状态判定准确率（非策略Sharpe）。

    Args:
        independent_data: 与主回测期完全无重叠的独立历史数据
        min_accuracy: 最低可接受的overall_accuracy
        min_transitions: 最低状态切换次数（统计功效）
    """
    if independent_data.empty:
        return {"error": "独立数据集为空", "best_params": {}, "qualified": False}

    param_keys = list(L2_PARAM_GRID.keys())
    param_values = [L2_PARAM_GRID[k] for k in param_keys]
    all_combos = list(itertools.product(*param_values))

    best_accuracy = -1
    best_params = {}
    best_result = {}
    qualified_count = 0
    candidate_pool: List[Dict[str, Any]] = []

    for combo in all_combos:
        combo_params = {k: v for k, v in zip(param_keys, combo)}
        params = PARAM_DEFAULTS.copy()
        params.update(combo_params)

        result = evaluate_state_accuracy(
            params=params,
            bar_data=independent_data,
            lookahead_bars=lookahead_bars,
        )
        acc = float(result.get("adjusted_accuracy", 0.0))
        n_transitions = int(result.get("n_transitions", 0))
        qualified = (
            acc >= min_accuracy
            and n_transitions >= min_transitions
            and bool(result.get("state_accuracy_pass", True))
        )
        if qualified:
            qualified_count += 1

        candidate_pool.append(
            {
                "params": combo_params,
                "accuracy": acc,
                "n_transitions": n_transitions,
                "qualified": qualified,
            }
        )

        if acc > best_accuracy:
            best_accuracy = acc
            best_params = combo_params
            best_result = result

    candidate_pool.sort(key=lambda x: x["accuracy"], reverse=True)
    best_qualified = bool(
        best_accuracy >= min_accuracy
        and int(best_result.get("n_transitions", 0)) >= min_transitions
        and bool(best_result.get("state_accuracy_pass", True))
    )

    return {
        "best_params": best_params,
        "best_accuracy": best_accuracy,
        "best_result": best_result,
        "qualified": best_qualified,
        "qualified_count": qualified_count,
        "total_combos": len(all_combos),
        "top_candidates": candidate_pool[:5],
    }


def validate_l2_param_conflicts(
    l2_params_independent: Dict[str, float],
    l2_params_main: Dict[str, float],
    tolerance: float = 0.20,
) -> Dict[str, Any]:
    """比较Step1独立优化参数与主回测参数冲突，给出冲突升级建议。"""
    conflicts = {}
    any_conflict = False
    for k in L2_PARAM_GRID.keys():
        v_ind = l2_params_independent.get(k)
        v_main = l2_params_main.get(k)
        if v_ind is None or v_main is None:
            continue

        if abs(v_ind) > 1e-10:
            rel_diff = abs(v_ind - v_main) / abs(v_ind)
        else:
            rel_diff = abs(v_ind - v_main)

        is_conflict = rel_diff > tolerance
        if is_conflict:
            any_conflict = True

        conflicts[k] = {
            "independent": v_ind,
            "main": v_main,
            "relative_diff": rel_diff,
            "conflict": is_conflict,
            "resolution": "independent_wins" if is_conflict else "agreement",
        }

    return {
        "any_conflict": any_conflict,
        "conflicts": conflicts,
        "action": "expand_independent_data_or_manual_review" if any_conflict else "proceed_to_step2",
        "escalation": L2_CONFLICT_RESOLUTION["escalation"] if any_conflict else None,
    }


def run_step2_smoke_test(
    l2_params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    min_state_transitions: int = 3,
) -> Dict[str, Any]:
    """Step2前置冒烟测试：验证多状态切换场景可稳定触发

    质量门第2条："至少回测一个完整多状态切换场景"
    归属Step2前置（非Step1），因为多状态切换依赖Step1锁定的L-2参数。

    Args:
        l2_params: Step1锁定的L-2参数（超参数）
        min_state_transitions: 最少需要的状态切换次数
    """
    if bar_data.empty:
        return {"passed": False, "error": "无数据", "state_transitions": 0}

    from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _reset_daily
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_state_transition

    params = PARAM_DEFAULTS.copy()
    params.update(l2_params)

    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)
    states_seen = set()
    state_transitions = 0
    prev_state = None

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)

        current_state = bt.current_state
        states_seen.add(current_state)
        if prev_state is not None and current_state != prev_state:
            state_transitions += 1
        prev_state = current_state

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_positions
                _check_positions(bt, bar, params)

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    passed = state_transitions >= min_state_transitions and len(states_seen) >= 2

    return {
        "passed": passed,
        "state_transitions": state_transitions,
        "states_seen": sorted(states_seen),
        "n_states": len(states_seen),
        "min_state_transitions": min_state_transitions,
        "l2_params_used": l2_params,
        "action": "proceed_to_step2_full_scan" if passed else "extend_data_or_review_l2_params",
    }
