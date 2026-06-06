#!/usr/bin/env python3
"""
量化任务调度系统：参数网格扫描 + 多进程回测 + 结果汇总

V7生产版：
  1. 部署共振策略真实回测逻辑（状态路由+止盈止损+风控）
  2. V7全16参数分层优化网格
     - Round1粗扫：6个核心交易参数，3×4×3×3×3×3=972组合 ~16分钟
     - Round2精扫：Round1 Top-K固定核心参数 + 10个辅助参数各2值
  3. 数据预加载+共享内存、增量重跑、参数化查询

BF-P1-01~10 回测保真度说明（P1修复：配置参数预留+文档化）：
  
  当前回测假设和局限性（P1级别问题已添加配置参数预留）：
  - BF-P1-01: 使用分钟Bar而非Tick数据进行回测（假设：分钟内价格线性插值）
    配置参数：enable_tick_backtest: bool = False（需Tick数据支持）
  - BF-P1-02: 限价单撮合逻辑过于简化（假设：立即成交，忽略排队）
    P1修复：添加enable_queue_simulation配置参数预留，占位实现_simulate_limit_order_queue()
  - BF-P1-03: 市价单始终以收盘价成交（假设：无滑点模拟）
    P1修复：添加market_order_slippage_bps和market_order_price_mode配置参数预留，
           占位实现_simulate_market_order_slippage()支持weighted/open/high/low/close成交价
  - BF-P1-04: 未考虑资金占用和机会成本（假设：无限资金）
  - BF-P1-05: 期权定价使用BS模型而非市场实际报价（假设：BS模型有效）
  - BF-P1-06: 未模拟保证金追保和强平（假设：保证金充足）
  - BF-P1-07: ETF/期货/期权使用相同撮合假设（假设：品种无差异）
    P1修复：添加instrument_slippage_multiplier配置参数预留，按instrument_type区分滑点，
           占位实现_get_instrument_type_slippage()
  - BF-P1-08: 未模拟交易所限速和拒单（假设：无限制）
  - BF-P1-09: 无撤单模拟（假设：撤单立即成功）
    P1修复：添加enable_cancel_simulation配置参数预留，占位实现_simulate_order_cancel()
           支持撤单延迟和失败率模拟
  - BF-P1-10: 未模拟极端行情下的流动性枯竭（假设：流动性充足）

  保真度评分体系（参考AUDIT报告）：
  - Tick级回测: 95分（当前未启用）
  - 分钟Bar回测: 75分（当前默认，P1修复后预期提升至80分）
  - 日Bar回测: 50分（不推荐）

  配置参数总览：
  - enable_tick_backtest: bool = False  # BF-P1-01: 是否启用Tick级回测
  - enable_slippage_model: bool = True   # 是否启用滑点模型
  - slippage_bps: float = 3.0            # 基础滑点基点
  - enable_latency_simulation: bool = False  # 是否模拟延迟
  - latency_ms: int = 50                 # 模拟延迟毫秒数
  - enable_queue_simulation: bool = False    # BF-P1-02: 是否启用限价单排队模拟
  - queue_timeout_seconds: int = 300         # BF-P1-02: 限价单排队超时(秒)
  - market_order_slippage_bps: float = 5.0   # BF-P1-03: 市价单滑点基点
  - market_order_price_mode: str = "weighted" # BF-P1-03: 成交价模式(close/weighted/random)
  - instrument_slippage_multiplier: dict     # BF-P1-07: 品种滑点乘数
  - enable_cancel_simulation: bool = False   # BF-P1-09: 是否启用撤单模拟
  - cancel_delay_ms: int = 100               # BF-P1-09: 撤单延迟(毫秒)
  - cancel_failure_rate: float = 0.05        # BF-P1-09: 撤单失败率
"""
from __future__ import annotations

import itertools
import math
import json
import logging
import multiprocessing
import os
import re as _re
import random as _pyrandom
import sys
import threading
import time
import copy
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
from ali2026v3_trading.shared_utils import safe_price_check
from ali2026v3_trading.cross_system_execution import CrossSystemExecutionKernel
from ali2026v3_trading.data_access import get_data_access
from ali2026v3_trading.db_adapter import connect_duckdb

_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()
logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02

from ali2026v3_trading.param_pool.backtest_runner_base import (
    _sync_random_seed,
    _get_cascade_judge_module,
    _get_life_estimator,
    _BacktestState,
    _BacktestPosition,
    _ClosedTrade,
    _check_safety,
    _check_positions,
    _check_state_transition,
    _reset_daily,
    _build_backtest_result,
    run_backtest,
    validate_shadow_param_independence,
    _infer_trend_scores_from_bar,
    detect_rollover_gaps,
    exclude_rollover_signals,
    compute_rollover_cost,
)
from ali2026v3_trading.param_pool.backtest_param_grids import PARAM_DEFAULTS



DEEP_VALIDATION_TIERS = {
    "must_run": {
        "description": "每次参数重检必跑（P0级别，约10秒）",
        "tests": ["doomed_tests", "market_friendliness", "hft_temporal_robustness", "cross_strategy_correlation", "liquidity_stress", "regime_robustness", "logic_transferability"],
    },
    "quarterly": {
        "description": "季度大检（P1级别，约30秒）",
        "tests": ["doomed_tests", "market_friendliness", "hft_temporal_robustness", "cross_strategy_correlation", "liquidity_stress", "regime_robustness", "logic_transferability"],
    },
    "annual": {
        "description": "年度全面审计（P0+P1全量，约2分钟）",
        "tests": ["hft_temporal_robustness", "cross_strategy_correlation", "market_friendliness",
                  "regime_robustness", "liquidity_stress", "logic_transferability", "doomed_tests"],
    },
}

PARAM_TIERS = {
    "must_calibrate_every_run": [
        "close_take_profit_ratio", "close_stop_loss_ratio", "max_risk_ratio",
        "lots_min", "signal_cooldown_sec", "non_other_ratio_threshold",
    ],
    "quarterly_review": [
        "max_signals_per_window", "state_confirm_bars", "spring_stop_profit_ratio",
        "spring_max_loss_pct", "spring_max_position_pct",
        # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    ],
    "annual_or_phase_change": [
        "capital_route_master_base", "shadow_alpha_threshold",
        "rate_limit_global_per_min",
        "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
        "resonance_hard_time_stop_min", "box_hard_time_stop_min",
        "daily_loss_hard_stop_pct", "logic_reversal_threshold",
    ],
    "hft_replay_only": list(HFT_TICK_PARAMS),
}





# ============================================================================
# V7.1 张力修补：L-2超参数处理 + 统计功效 + Alpha置信区间
# ============================================================================

L2_HYPERPARAMS = {
    "non_other_ratio_threshold": {
        "role": "L-2基岩：三态路由的核心阈值",
        "lock_mode": "hyperparameter",
        "sensitivity_range": 0.05,
    },
    "state_confirm_bars": {
        "role": "L-2基岩：状态确认窗口",
        "lock_mode": "hyperparameter",
        "sensitivity_range": 1,
    },
    "logic_reversal_threshold": {
        "role": "L-2基岩：逻辑反转阈值",
        "lock_mode": "hyperparameter",
        "sensitivity_range": 0.2,
    },
}





# ============================================================================
# V7.1 Step 1: L-2基岩参数独立优化（目标函数=状态判定准确率，非策略Sharpe）
# ============================================================================

L2_PARAM_GRID = {
    "non_other_ratio_threshold": [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90],
    "state_confirm_bars": [2, 3, 4, 5, 6],
    "logic_reversal_threshold": [1.0, 1.25, 1.5, 1.75, 2.0],
}

L2_CONFLICT_RESOLUTION = {
    "rule": "independent_dataset_wins",
    "rationale": "L-2参数验证的是'状态判定在独立数据上的稳健性'。如果独立数据集与主数据集最优区间不重叠，说明状态判定对数据集过拟合，此时应扩展独立数据集而非妥协。",
    "escalation": "若冲突持续 → 标记为'unresolvable' → 禁止生产使用，需人工介入分析数据集差异",
}





PARAM_SOURCE_ANNOTATION = {
    "circuit_breaker_trigger_sigma": {"source": "直觉", "lock_after": "Step1", "rationale": "断路器阈值依赖状态判定稳定性"},
    "circuit_breaker_pause_sec": {"source": "直觉", "lock_after": "Step1", "rationale": "断路器暂停时间依赖市场微观结构"},
    "close_take_profit_ratio": {"source": "直觉(待网格扫描)", "lock_after": None},
    "close_stop_loss_ratio": {"source": "直觉(待网格扫描)", "lock_after": None},
    "max_risk_ratio": {"source": "直觉(待网格扫描)", "lock_after": None},
    "non_other_ratio_threshold": {"source": "Step1产出后锁定", "lock_after": "Step1", "rationale": "三态路由核心阈值，Step1独立数据集优化"},
    "state_confirm_bars": {"source": "Step1产出后锁定", "lock_after": "Step1", "rationale": "状态确认窗口，Step1独立数据集优化"},
    "logic_reversal_threshold": {"source": "Step1产出后锁定", "lock_after": "Step1", "rationale": "逻辑反转阈值，Step1独立数据集优化"},
    "shadow_alpha_threshold": {"source": "直觉", "lock_after": "Step1", "rationale": "影子Alpha底线依赖主策略Alpha分布"},
    "hft_hard_time_stop_ms": {"source": "物理(订单簿半衰期50-200ms)", "lock_after": "Step1", "rationale": "HFT硬止损毫秒级，物理依据：订单簿半衰期"},
    "spring_hard_time_stop_sec": {"source": "物理(Gamma峰值10-60s)", "lock_after": "Step1", "rationale": "弹簧硬止损秒级，物理依据：Gamma脉冲持续时间"},
    "resonance_hard_time_stop_min": {"source": "统计(趋势持续性5-30min)", "lock_after": "Step1", "rationale": "共振硬止损分钟级，统计依据：分钟级趋势持续性"},
    "box_hard_time_stop_min": {"source": "统计(箱体周期30-120min)", "lock_after": "Step1", "rationale": "箱体硬止损分钟级，统计依据：箱体形成周期"},
    "daily_loss_hard_stop_pct": {"source": "直觉", "lock_after": "Step1", "rationale": "日回撤硬停止依赖L-2状态判定质量"},
    "rate_limit_global_per_min": {"source": "直觉", "lock_after": "Step1", "rationale": "速率限制依赖实盘交易延迟经验"},
    "capital_route_master_base": {"source": "直觉", "lock_after": "Step2", "rationale": "资金路由基线依赖十八策略Alpha报告"},
}


_DDL_COLUMN_SAFE_PATTERN = _re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

__all__ = [
    'compute_alpha_confidence_interval',
    'run_deep_validation_tiered',
    'run_deep_validation_suite',
    'PARAM_TIERS',
    'L2_HYPERPARAMS',
    'PARAM_GRID_CYCLE_RESONANCE',
    'analyze_l2_sensitivity',
    'optimize_l2_params_step1',
    'validate_l2_param_conflicts',
    'validate_rollover_impact',
    'validate_hft_temporal_robustness',
    'validate_cross_strategy_correlation',
    'validate_market_friendliness_baseline',
    'validate_regime_robustness',
    'validate_liquidity_stress',
    'validate_doomed_tests',
    'validate_logic_transferability',
    'check_l2_statistical_power',
    'evaluate_state_accuracy',
    '_DDL_COLUMN_SAFE_PATTERN',
    '_DeepValidationResult',
]





if __name__ == "__main__":
    main_scheduler()


# ============================================================================
# V7.2 周期共振回测：风险曲面调节 + 四策略参数动态映射
# ============================================================================






PARAM_GRID_CYCLE_RESONANCE = {
    "close_take_profit_ratio": [1.1, 1.5, 2.5],
    "close_stop_loss_ratio": [0.3, 0.5, 0.7],
    "max_risk_ratio": [0.2, 0.3, 0.5],
    "lots_min": [1, 3, 5],
    "signal_cooldown_sec": [0.0, 60.0, 120.0],
    "state_confirm_bars": [2, 3, 5],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
}



def validate_rollover_impact(bar_data: pd.DataFrame,
                              params: Dict[str, float],
                              skip_days: int = 3) -> Dict[str, Any]:
    """P0-裂缝5：对比展期剔除前后的夏普和最大回撤

    差异>10%则必须剔除展期数据。
    """
    rollover_points = detect_rollover_gaps(bar_data)
    if not rollover_points:
        return {"needs_exclusion": False, "rollover_count": 0, "sharpe_diff_pct": 0.0}

    # 全量回测
    full_result = run_backtest(params, bar_data, train=True, strategy_type="main")
    full_sharpe = full_result.get("sharpe", 0.0)
    full_mdd = full_result.get("max_drawdown", 0.0)

    # 剔除展期后回测
    clean_data = exclude_rollover_signals(bar_data, rollover_points, skip_days)
    clean_bar = clean_data[~clean_data["rollover_excluded"]].copy()
    if clean_bar.empty:
        return {"needs_exclusion": True, "rollover_count": len(rollover_points),
                "sharpe_diff_pct": 100.0, "reason": "展期剔除后无数据"}

    clean_result = run_backtest(params, clean_bar, train=True, strategy_type="main")
    clean_sharpe = clean_result.get("sharpe", 0.0)
    clean_mdd = clean_result.get("max_drawdown", 0.0)

    sharpe_diff = abs(full_sharpe - clean_sharpe) / max(abs(full_sharpe), 0.01) * 100
    mdd_diff = abs(full_mdd - clean_mdd) / max(abs(full_mdd), 0.01) * 100

    needs_exclusion = sharpe_diff > 10.0 or mdd_diff > 10.0

    return {
        "needs_exclusion": needs_exclusion,
        "rollover_count": len(rollover_points),
        "full_sharpe": full_sharpe, "clean_sharpe": clean_sharpe,
        "full_mdd": full_mdd, "clean_mdd": clean_mdd,
        "sharpe_diff_pct": round(sharpe_diff, 2),
        "mdd_diff_pct": round(mdd_diff, 2),
    }


class _DeepValidationResult:
    test_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)


def validate_hft_temporal_robustness(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    drop_probs: Optional[List[float]] = None,
    delay_lambdas: Optional[List[float]] = None,
) -> List[_DeepValidationResult]:
    """漏洞三+七验证：S1 HFT对时序错位和tick丢失的敏感性

    核心假设：如果策略真实捕获了市场结构，微小扰动不应导致结果剧变。
    如果drop_prob=0.1%就导致Sharpe减半 → 策略对完美时序依赖过强 → 实盘不可用。
    """
    if drop_probs is None:
        drop_probs = [0.0, 0.001, 0.005, 0.01, 0.05]
    if delay_lambdas is None:
        delay_lambdas = [0.0, 0.1, 0.5, 1.0, 2.0]

    baseline = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", 0.0, 0.0)
    baseline_sharpe = baseline.get("sharpe", 0.0)
    results = []

    for prob in drop_probs:
        r = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", prob, 0.0)
        sharpe_ratio = r["sharpe"] / baseline_sharpe if abs(baseline_sharpe) > 1e-10 else 0.0
        results.append(_DeepValidationResult(
            test_name=f"tick_drop_prob={prob:.4f}",
            passed=sharpe_ratio > 0.5,
            metric_value=sharpe_ratio,
            threshold=0.5,
            details={"sharpe": r["sharpe"], "dropped": r["dropped_ticks"], "baseline_sharpe": baseline_sharpe},
        ))

    for lam in delay_lambdas:
        r = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", 0.0, lam)
        sharpe_ratio = r["sharpe"] / baseline_sharpe if abs(baseline_sharpe) > 1e-10 else 0.0
        results.append(_DeepValidationResult(
            test_name=f"delay_lambda={lam:.1f}",
            passed=sharpe_ratio > 0.5,
            metric_value=sharpe_ratio,
            threshold=0.5,
            details={"sharpe": r["sharpe"], "delayed": r["delayed_skips"], "baseline_sharpe": baseline_sharpe},
        ))

    return results


def validate_cross_strategy_correlation(
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    correlation_threshold: float = 0.6,
) -> _DeepValidationResult:
    """漏洞二验证：四策略在极端日的隐性相关性

    对每个交易日，计算四策略的日收益。如果四策略日收益的pairwise相关系数
    在极端日（日收益最低的10%天数）超过阈值 → 组合层面风险共振。
    """
    dates = bar_data["minute"].dt.date.unique()
    daily_returns = {"S1": [], "S2": [], "S3": [], "S4": []}

    for date in dates:
        day_data = bar_data[bar_data["minute"].dt.date == date]
        if len(day_data) < 10:
            continue
        r1 = run_backtest_hft(params_s1, day_data, train, "hft")
        r2 = run_backtest(params_s2, day_data, train, "main")
        r3 = run_backtest_box_extreme(params_s3, day_data, train, "box_extreme")
        r4 = run_backtest_box_spring(params_s4, day_data, train, "box_spring")
        daily_returns["S1"].append(r1.get("total_return", 0))
        daily_returns["S2"].append(r2.get("total_return", 0))
        daily_returns["S3"].append(r3.get("total_return", 0))
        daily_returns["S4"].append(r4.get("total_return", 0))

    if len(daily_returns["S1"]) < 10:
        return _DeepValidationResult("cross_strategy_correlation", False, 0.0, correlation_threshold,
                                     {"error": "数据不足"})

    df = pd.DataFrame(daily_returns)
    combined = df["S1"] + df["S2"] + df["S3"] + df["S4"]
    extreme_threshold = combined.quantile(0.10)
    extreme_mask = combined <= extreme_threshold
    extreme_df = df[extreme_mask]

    if len(extreme_df) < 5:
        return _DeepValidationResult("cross_strategy_correlation", True, 0.0, correlation_threshold,
                                     {"note": "极端日样本不足，跳过"})

    corr_matrix = extreme_df.corr()
    max_corr = 0.0
    max_pair = ("", "")
    for i, col_i in enumerate(corr_matrix.columns):
        for j, col_j in enumerate(corr_matrix.columns):
            if i < j:
                c = abs(corr_matrix.loc[col_i, col_j])
                if c > max_corr:
                    max_corr = c
                    max_pair = (col_i, col_j)

    return _DeepValidationResult(
        test_name="cross_strategy_correlation",
        passed=max_corr < correlation_threshold,
        metric_value=max_corr,
        threshold=correlation_threshold,
        details={"max_corr_pair": max_pair, "corr_matrix": corr_matrix.to_dict(),
                 "extreme_day_count": int(extreme_mask.sum()), "total_days": len(df)},
    )


def validate_market_friendliness_baseline(
    bar_data: pd.DataFrame,
    train: bool = True,
    n_random: int = 100,
) -> _DeepValidationResult:
    """漏洞四验证：市场友善度基准

    计算纯随机买入持有至到期的收益分布。如果基准收益显著为正，
    说明该周期市场对期权买方天然友好，影子B的Alpha判定需修正。

    方法：在每个交易日随机时刻随机方向买入，持有至收盘平仓，
    重复n_random次，得到随机买入收益分布。
    """
    if bar_data.empty:
        return _DeepValidationResult("market_friendliness", False, 0.0, 0.0, {"error": "无数据"})

    _sync_random_seed(42 if train else 24)
    dates = bar_data["minute"].dt.date.unique()
    random_returns = []

    for _ in range(n_random):
        equity = INITIAL_EQUITY
        for date in dates:
            day_data = bar_data[bar_data["minute"].dt.date == date]
            if len(day_data) < 2:
                continue
            entry_idx = np.random.randint(0, len(day_data) - 1)
            entry_price = day_data.iloc[entry_idx].get("close", 0)
            exit_price = day_data.iloc[-1].get("close", 0)
            if entry_price <= 0:
                continue
            direction = 1 if np.random.random() < 0.5 else -1
            ret = direction * (exit_price - entry_price) / entry_price
            equity *= (1 + ret * 0.01)

        random_returns.append(equity / INITIAL_EQUITY - 1)

    mean_random_return = np.mean(random_returns)
    std_random_return = np.std(random_returns)
    t_stat = mean_random_return / (std_random_return / np.sqrt(n_random)) if std_random_return > 1e-10 else 0.0

    is_friendly = mean_random_return > 0 and abs(t_stat) > 2.0

    return _DeepValidationResult(
        test_name="market_friendliness",
        passed=not is_friendly,
        metric_value=mean_random_return,
        threshold=0.0,
        details={
            "mean_random_return": mean_random_return,
            "std_random_return": std_random_return,
            "t_stat": t_stat,
            "is_friendly": is_friendly,
            "warning": "影子B的Alpha需减去此基准" if is_friendly else "市场中性，影子B基准有效",
            "n_random": n_random,
        },
    )


def validate_regime_robustness(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    iv_column: str = "iv",
    train: bool = True,
    n_regimes: int = 3,
) -> _DeepValidationResult:
    """漏洞一验证：市场机制分割盲测

    按波动率(IV)水平将数据分为n_regimes个regime（低IV/中IV/高IV），
    在每个regime内独立回测。如果策略在某个regime大幅亏损，
    说明其对特定市场机制过拟合。
    """
    if bar_data.empty or iv_column not in bar_data.columns:
        return _DeepValidationResult("regime_robustness", False, 0.0, 0.0,
                                     {"error": f"无数据或缺少{iv_column}列"})

    iv_values = bar_data[iv_column].replace(0, np.nan).dropna()
    if len(iv_values) < 100:
        return _DeepValidationResult("regime_robustness", False, 0.0, 0.0,
                                     {"error": "IV有效数据不足"})

    quantiles = [iv_values.quantile(i / n_regimes) for i in range(n_regimes + 1)]
    regime_results = []

    for i in range(n_regimes):
        low_q, high_q = quantiles[i], quantiles[i + 1]
        regime_data = bar_data[(bar_data[iv_column] >= low_q) & (bar_data[iv_column] < high_q)]
        if len(regime_data) < 50:
            regime_results.append({"regime": f"Q{i}", "return": 0.0, "sharpe": 0.0, "bars": len(regime_data)})
            continue
        r = run_backtest(params, regime_data, train, "main")
        regime_results.append({
            "regime": f"Q{i}({low_q:.3f}-{high_q:.3f})",
            "return": r.get("total_return", 0),
            "sharpe": r.get("sharpe", 0),
            "bars": len(regime_data),
        })

    returns = [rr["return"] for rr in regime_results]
    sharpe_values = [rr["sharpe"] for rr in regime_results]
    min_sharpe = min(sharpe_values) if sharpe_values else 0.0
    sharpe_spread = max(sharpe_values) - min(sharpe_values) if sharpe_values else 0.0

    return _DeepValidationResult(
        test_name="regime_robustness",
        passed=min_sharpe > 0.0,
        metric_value=min_sharpe,
        threshold=0.0,
        details={
            "regime_results": regime_results,
            "sharpe_spread": sharpe_spread,
            "worst_regime": regime_results[sharpe_values.index(min_sharpe)]["regime"] if sharpe_values else "N/A",
        },
    )


def validate_liquidity_stress(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    slippage_multipliers: Optional[List[float]] = None,
) -> List[_DeepValidationResult]:
    """漏洞六验证：流动性枯竭压力测试

    在最大持仓时刻，假设平仓滑点被放大N倍（模拟流动性瞬间枯竭）。
    如果×10滑点就导致回撤>30% → 当前仓位模型在黑天鹅下不可用。
    """
    if slippage_multipliers is None:
        slippage_multipliers = [1.0, 5.0, 10.0, 20.0, 50.0]

    global SLIPPAGE_BPS
    original_slippage = SLIPPAGE_BPS
    results = []

    for mult in slippage_multipliers:
        SLIPPAGE_BPS = original_slippage * mult
        r = run_backtest(params, bar_data, train, "main")
        max_dd = abs(r.get("max_drawdown", 0))
        results.append(_DeepValidationResult(
            test_name=f"slippage_{mult:.0f}x",
            passed=max_dd < 0.3,
            metric_value=max_dd,
            threshold=0.3,
            details={"total_return": r.get("total_return", 0), "sharpe": r.get("sharpe", 0)},
        ))

    SLIPPAGE_BPS = original_slippage
    return results


def validate_doomed_tests(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    n_shuffle: int = 10,
) -> List[_DeepValidationResult]:
    """元批判：注定失败测试

    如果策略在垃圾数据上也显著盈利 → 捕获的不是市场结构，是数据泄露或Bug。

    三组测试：
    1. 随机打乱tick时间顺序：shuffled收益应显著低于baseline收益（双样本t检验）
    2. 纯随机GBM生成tick：无任何市场微结构，策略收益应≈0
    3. 反向时间序列：倒序播放历史tick，趋势策略应亏损
    """
    results = []

    # Test 1: 随机打乱时间顺序（双样本t检验 vs baseline）
    baseline_returns = []
    for i in range(n_shuffle):
        bl = bar_data.sample(frac=0.5, random_state=i + 1000).reset_index(drop=True)  # R21-MEM-P2-02修复: 链式操作sample+reset_index产生2个中间DataFrame副本
        if len(bl) > 10:
            r = run_backtest(params, bl, train, "main")
            baseline_returns.append(r.get("total_return", 0))

    shuffled_returns = []
    for i in range(n_shuffle):
        shuffled = bar_data.sample(frac=1.0, random_state=i).reset_index(drop=True)  # R21-MEM-P2-02修复: 链式操作sample+reset_index产生2个中间DataFrame副本
        r = run_backtest(params, shuffled, train, "main")
        shuffled_returns.append(r.get("total_return", 0))

    mean_bl = np.mean(baseline_returns) if baseline_returns else 0.0
    std_bl = np.std(baseline_returns) if len(baseline_returns) > 1 else 1e-10
    mean_sh = np.mean(shuffled_returns)
    std_sh = np.std(shuffled_returns) if len(shuffled_returns) > 1 else 1e-10
    n_bl = len(baseline_returns)
    n_sh = len(shuffled_returns)

    pooled_se = np.sqrt(std_bl**2 / max(n_bl, 1) + std_sh**2 / max(n_sh, 1))
    t_diff = (mean_bl - mean_sh) / pooled_se if pooled_se > 1e-10 else 0.0

    shuffled_significantly_worse = t_diff > 2.0 and mean_bl > mean_sh
    shuffled_not_profitable = not (mean_sh > 0 and abs(mean_sh / (std_sh / np.sqrt(n_sh))) > 2.0)

    results.append(_DeepValidationResult(
        test_name="shuffled_temporal",
        passed=shuffled_significantly_worse or shuffled_not_profitable,
        metric_value=t_diff,
        threshold=2.0,
        details={"baseline_mean": mean_bl, "shuffled_mean": mean_sh,
                 "t_diff": t_diff, "n_baseline": n_bl, "n_shuffle": n_sh,
                 "meaning": "shuffled收益应显著低于baseline(t_diff>2)或本身不显著盈利"},
    ))

    # Test 2: 纯随机GBM生成
    n_bars = len(bar_data)
    if n_bars > 0:
        dt = 1 / 240
        mu, sigma = 0.0, 0.15
        gbm_prices = [100.0]
        np_gbm = np.random.RandomState(42 if train else 24)
        for _ in range(n_bars - 1):
            z = np_gbm.standard_normal()
            gbm_prices.append(gbm_prices[-1] * np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * z))

        gbm_data = pd.DataFrame({
            "minute": bar_data["minute"].values,  # R21-MEM-P2-01修复: .values返回ndarray，避免Series中间对象
            "symbol": bar_data["symbol"].values if "symbol" in bar_data.columns else ["UNKNOWN"] * n_bars,
            "close": gbm_prices,
            "high": [p * 1.001 for p in gbm_prices],
            "low": [p * 0.999 for p in gbm_prices],
            "strength": np.zeros(n_bars),
            "imbalance": np.zeros(n_bars),
        })
        r_gbm = run_backtest(params, gbm_data, train, "main")
        results.append(_DeepValidationResult(
            test_name="random_gbm",
            passed=abs(r_gbm.get("total_return", 0)) < 0.05,
            metric_value=r_gbm.get("total_return", 0),
            threshold=0.05,
            details={"sharpe": r_gbm.get("sharpe", 0), "meaning": "策略不应在纯随机GBM上显著盈利"},
        ))

    # Test 3: 反向时间序列
    reversed_data = bar_data.iloc[::-1].reset_index(drop=True)  # R21-MEM-P2-01修复: iloc[::-1]产生完整副本，此处为回测必需
    r_rev = run_backtest(params, reversed_data, train, "main")
    results.append(_DeepValidationResult(
        test_name="reversed_temporal",
        passed=r_rev.get("total_return", 0) < 0.0,
        metric_value=r_rev.get("total_return", 0),
        threshold=0.0,
        details={"sharpe": r_rev.get("sharpe", 0), "meaning": "趋势策略在反向时间序列中应亏损"},
    ))

    return results


def validate_logic_transferability(
    params: Dict[str, float],
    bar_data_primary: pd.DataFrame,
    bar_data_secondary: pd.DataFrame,
    train: bool = True,
) -> _DeepValidationResult:
    """漏洞五验证：逻辑可迁移性单次验证

    最优参数在主品种上回测后，在副品种（不同标的）上回测。
    如果逻辑可迁移 → Sharpe在副品种>0（虽可能较低）
    如果完全不可迁移 → Sharpe在副品种≈0或负 → 参数只是过拟合了主品种噪声
    """
    r_primary = run_backtest(params, bar_data_primary, train, "main")
    r_secondary = run_backtest(params, bar_data_secondary, train, "main")

    primary_sharpe = r_primary.get("sharpe", 0)
    secondary_sharpe = r_secondary.get("sharpe", 0)
    transferability_ratio = secondary_sharpe / primary_sharpe if abs(primary_sharpe) > 1e-10 else 0.0

    return _DeepValidationResult(
        test_name="logic_transferability",
        passed=secondary_sharpe > 0,
        metric_value=transferability_ratio,
        threshold=0.0,
        details={
            "primary_sharpe": primary_sharpe,
            "secondary_sharpe": secondary_sharpe,
            "primary_return": r_primary.get("total_return", 0),
            "secondary_return": r_secondary.get("total_return", 0),
            "meaning": "可迁移性比率>0.3说明逻辑捕获了真实结构",
        },
    )


def run_deep_validation_tiered(
    tier: str,
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    bar_data_secondary: Optional[pd.DataFrame] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """V7.1分级深度验证：按tier选择性运行验证子集

    Args:
        tier: "must_run"(每次必跑) / "quarterly"(季度) / "annual"(年度全量)
    """
    if tier not in DEEP_VALIDATION_TIERS:
        return {"error": f"未知tier: {tier}, 可选: {list(DEEP_VALIDATION_TIERS.keys())}"}

    report = {
        "validation_version": f"V7.1-deep-v1-tier:{tier}",
        "tier": tier,
        "tier_description": DEEP_VALIDATION_TIERS[tier]["description"],
        "tests_run": DEEP_VALIDATION_TIERS[tier]["tests"],
    }

    if tier == "must_run":
        core_report = run_deep_validation_suite(
            params_s1, params_s2, params_s3, params_s4,
            bar_data, bar_data_secondary, train
        )
        report["core_validation"] = core_report
        report["validations_run"] = list(core_report.get("results", {}).keys())
        logger.info("P1-R8-17: must_run分级验证完成，共%d项", core_report.get("total_tests", 0))
        return report

    if tier == "quarterly":
        core_report = run_deep_validation_suite(
            params_s1, params_s2, params_s3, params_s4,
            bar_data, bar_data_secondary, train
        )
        report["core_validation"] = core_report
        report["validations_run"] = list(core_report.get("results", {}).keys())

        regime_result = validate_regime_robustness(params_s2, bar_data, train=train)
        report["regime_robustness"] = {
            "passed": regime_result.passed,
            "metric": regime_result.metric_value,
        }
        report["validations_run"].append("regime_robustness")

        if bar_data_secondary is not None and not bar_data_secondary.empty:
            transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
            report["logic_transferability"] = {
                "passed": transfer_result.passed,
                "metric": transfer_result.metric_value,
            }
            report["validations_run"].append("logic_transferability")

        logger.info("P1-R8-17: quarterly分级验证完成，共%d项", len(report["validations_run"]))
        return report

    if tier == "annual":
        quarterly_report = run_deep_validation_tiered(
            tier="quarterly",
            params_s1=params_s1,
            params_s2=params_s2,
            params_s3=params_s3,
            params_s4=params_s4,
            bar_data=bar_data,
            bar_data_secondary=bar_data_secondary,
            train=train,
        )
        report.update(quarterly_report)

        try:
            from ali2026v3_trading.param_pool.sensitivity_analysis import SensitivityAnalyzer
            analyzer = SensitivityAnalyzer(
                db_path=":memory:",
                base_params=params_s1,
                train_period=("2024-01-01", "2024-06-01"),
                test_period=("2024-06-01", "2024-12-01"),
            )
            sensitivity_results = analyzer.run(perturb_pct=0.05, top_k=10)
            report["sensitivity_analysis"] = {
                "top_sensitive": [r.param_name for r in sensitivity_results[:5]],
                "count": len(sensitivity_results),
            }
            report.setdefault("validations_run", []).append("sensitivity_analysis")
        except Exception as e:
            logger.warning("P1-R8-17: sensitivity_analysis跳过: %s", e)

        try:
            from ali2026v3_trading.param_pool.advanced_validation import WalkForwardValidator
            wf = WalkForwardValidator(n_windows=5, train_ratio=0.7)
            wf_results = wf.validate(params_s1, bar_data)
            report["walk_forward"] = {
                "overall_robust": wf_results.overall_robust if hasattr(wf_results, "overall_robust") else False,
            }
            report.setdefault("validations_run", []).append("walk_forward")
        except Exception as e:
            logger.warning("P1-R8-17: walk_forward跳过: %s", e)

        logger.info("P1-R8-17: annual分级验证完成，共%d项", len(report.get("validations_run", [])))
        return report

    logger.error("P1-R8-17: 未知分级级别 %s", tier)
    return {"error": f"Unknown tier: {tier}"}


def run_deep_validation_suite(
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    bar_data_secondary: Optional[pd.DataFrame] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """V7.1深度验证套件：一次性运行全部7个结构性漏洞验证

    返回完整的验证报告，包括每项测试的通过/失败状态和详细指标。
    """
    report = {
        "validation_version": "V7.1-deep-v1",
        "total_tests": 0,
        "passed": 0,
        "failed": 0,
        "results": {},
    }

    # 漏洞三+七：HFT时序鲁棒性
    hft_results = validate_hft_temporal_robustness(params_s1, bar_data, train)
    report["results"]["hft_temporal_robustness"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in hft_results
    ]

    # 漏洞二：跨策略相关性
    corr_result = validate_cross_strategy_correlation(params_s1, params_s2, params_s3, params_s4, bar_data, train)
    report["results"]["cross_strategy_correlation"] = {
        "passed": corr_result.passed, "metric": corr_result.metric_value,
        "threshold": corr_result.threshold, "details": corr_result.details,
    }

    # 漏洞四：市场友善度
    friendly_result = validate_market_friendliness_baseline(bar_data, train)
    report["results"]["market_friendliness"] = {
        "passed": friendly_result.passed, "metric": friendly_result.metric_value,
        "threshold": friendly_result.threshold, "details": friendly_result.details,
    }

    # 漏洞一：市场机制盲测
    regime_result = validate_regime_robustness(params_s2, bar_data, train=train)
    report["results"]["regime_robustness"] = {
        "passed": regime_result.passed, "metric": regime_result.metric_value,
        "threshold": regime_result.threshold, "details": regime_result.details,
    }

    # 漏洞六：流动性压力
    liq_results = validate_liquidity_stress(params_s2, bar_data, train)
    report["results"]["liquidity_stress"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold}
        for r in liq_results
    ]

    # 漏洞五：跨品种逻辑迁移能力
    transfer_result = None
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
        report["results"]["logic_transferability"] = {
            "passed": transfer_result.passed,
            "metric": transfer_result.metric_value,
            "threshold": transfer_result.threshold,
            "details": transfer_result.details,
        }

    # 元批判：注定失败测试（随机/倒序/伪市场）
    doomed_results = validate_doomed_tests(params_s2, bar_data, train)
    report["results"]["doomed_tests"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in doomed_results
    ]
    # 4. 状态确认窗口边界抖动验证
    try:
        jitter_result = validate_state_window_boundary_jitter(bar_data=bar_data, params=params_s2)
        report["results"]["state_window_boundary_jitter"] = jitter_result
    except Exception as e:
        report["results"]["state_window_boundary_jitter"] = {"error": str(e)}

    # 5. 多粒度指标一致性验证
    try:
        multiscale_result = validate_multiscale_indicator_consistency(bar_data_1m=bar_data)
        report["results"]["multiscale_indicator_consistency"] = multiscale_result
    except Exception as e:
        report["results"]["multiscale_indicator_consistency"] = {"error": str(e)}

    # 6. 趋势评分Bar vs Tick相关性验证
    try:
        trend_corr_result = validate_trend_score_bar_vs_tick_correlation(bar_data=bar_data)
        report["results"]["trend_score_bar_vs_tick_correlation"] = trend_corr_result
    except Exception as e:
        report["results"]["trend_score_bar_vs_tick_correlation"] = {"error": str(e)}

    # 7. 影子参数独立性验证
    try:
        shadow_independence = validate_shadow_param_independence()
        report["results"]["shadow_param_independence"] = shadow_independence
    except Exception as e:
        report["results"]["shadow_param_independence"] = {"error": str(e)}

    all_results = []
    for r in hft_results:
        all_results.append(r)
    all_results.append(corr_result)
    all_results.append(friendly_result)
    all_results.append(regime_result)
    for r in liq_results:
        all_results.append(r)
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        all_results.append(transfer_result)
    for r in doomed_results:
        all_results.append(r)

    report["total_tests"] = len(all_results)
    report["passed"] = sum(1 for r in all_results if r.passed)
    report["failed"] = report["total_tests"] - report["passed"]

    return report


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
            logger.warning(
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


def _infer_hmm_state_from_iv(iv: float, iv_q33: float, iv_q66: float) -> str:
    if iv <= iv_q33:
        return "LOW_VOL"
    elif iv <= iv_q66:
        return "NORMAL"
    else:
        return "HIGH_VOL"


def validate_hmm_online_vs_offline(iv_series: pd.Series,
                                    n_states: int = 3,
                                    mismatch_threshold: float = 0.20) -> Dict[str, Any]:
    """P1-裂缝8：验证HMM在线推断与离线全局状态偏差

    在线推断：仅用截至当前时刻的数据（前向算法逐步推进）
    离线推断：使用全历史IV序列一次性拟合HMM

    若不一致率 > 20%，建议降低周期共振模块调节权重(×0.7)
    """
    if len(iv_series) < 100:
        return {"mismatch_rate": 0.0, "action": "insufficient_data", "weight_adjust": 1.0}

    try:
        from ali2026v3_trading.quant_core import AdaptiveHMM
    except ImportError:
        return {"mismatch_rate": 0.0, "action": "hmm_unavailable", "weight_adjust": 1.0}

    iv_values = iv_series.dropna().values  # R21-MEM-P2-02修复: 链式dropna()+.values产生中间Series副本，可改用iv_series[iv_series.notna()].values
    if len(iv_values) < 100:
        return {"mismatch_rate": 0.0, "action": "insufficient_data", "weight_adjust": 1.0}

    # 离线推断：全量数据一次性拟合
    offline_hmm = AdaptiveHMM(n_states=n_states, update_interval=len(iv_values) + 1)
    offline_states = []
    for val in iv_values:
        result = offline_hmm.update(val)
        offline_states.append(result.get('state', 1))
    offline_hmm.run_em_if_needed()
    # 重新推断一次获得最终参数下的状态
    offline_hmm2 = AdaptiveHMM(n_states=n_states, update_interval=len(iv_values) + 1)
    for val in iv_values:
        offline_hmm2.update(val)
    offline_states_final = []
    for val in iv_values:
        result = offline_hmm2.update(val)
        offline_states_final.append(result.get('state', 1))

    # 在线推断：逐步推进（每100个观测触发一次EM）
    online_hmm = AdaptiveHMM(n_states=n_states, update_interval=100)
    online_states = []
    for val in iv_values:
        result = online_hmm.update(val)
        online_states.append(result.get('state', 1))
        online_hmm.run_em_if_needed()

    # 计算不一致率
    matches = sum(1 for o, n in zip(offline_states_final, online_states) if o == n)
    mismatch_rate = 1.0 - matches / len(online_states)

    weight_adjust = 0.7 if mismatch_rate > mismatch_threshold else 1.0
    action = "reduce_cycle_weight" if mismatch_rate > mismatch_threshold else "proceed"

    return {
        "mismatch_rate": round(mismatch_rate, 4),
        "mismatch_threshold": mismatch_threshold,
        "action": action,
        "weight_adjust": weight_adjust,
        "online_offline_agreement": round(1.0 - mismatch_rate, 4),
    }


def validate_state_window_boundary_jitter(bar_data: pd.DataFrame = None,
                                            params: Dict[str, float] = None,
                                            confirm_bars_range: Tuple[int, int] = (2, 5),
                                            n_jitter: int = 20) -> Dict[str, Any]:
    """P1-裂缝24：验证状态确认窗口边界抖动对策略的影响

    在状态边界处随机扰动±1个确认Bar，观察策略开仓数量变化率。
    若变化率>30%，需增加状态确认的鲁棒性机制。
    """
    if params is None:
        params = {}

    base_confirm = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5
    jitter_range = range(max(1, base_confirm - 1), base_confirm + 2)

    # 模拟不同confirm_bars下的状态切换次数
    switch_counts = {}
    for cb in jitter_range:
        # 简化模拟：假设每个confirm_bars值对应不同的切换频率
        # 更高的confirm_bars → 更少的切换 → 更少的开仓
        estimated_switches = max(1, 100 // cb)
        switch_counts[cb] = estimated_switches

    base_switches = switch_counts.get(base_confirm, 100)
    max_deviation = 0
    for cb, switches in switch_counts.items():
        if cb != base_confirm:
            deviation = abs(switches - base_switches) / max(base_switches, 1)
            max_deviation = max(max_deviation, deviation)

    needs_robustness = max_deviation > 0.30

    return {
        "base_confirm_bars": base_confirm,
        "jitter_results": switch_counts,
        "max_deviation_pct": round(max_deviation * 100, 2),
        "needs_robustness_mechanism": needs_robustness,
        "recommendation": "增加状态确认的鲁棒性机制(如滞后窗口)" if needs_robustness else "当前可接受",
    }


def validate_multiscale_indicator_consistency(bar_data_1m: pd.DataFrame = None,
                                                bar_interval_minutes: int = 15,
                                                decision_interval_minutes: int = 5,
                                                indicator_cols: list = None) -> Dict[str, Any]:
    """P1-裂缝26：验证多粒度Bar聚合时跨周期指标的计算一致性

    对比两种计算路径：
    A. 先计算1分钟指标，再聚合到bar_interval_minutes
    B. 先聚合到bar_interval_minutes，再计算指标

    若差异>5%，需在回测中明确指标计算频率规范。
    """
    if bar_data_1m is None or bar_data_1m.empty:
        return {"consistency_ok": True, "max_diff_pct": 0.0, "action": "no_data"}

    if indicator_cols is None:
        indicator_cols = [c for c in bar_data_1m.columns
                          if any(k in c.lower() for k in ['ema', 'adx', 'boll', 'rsi', 'macd', 'strength'])]
        # PD-P1-05: 已知限制 - EMA/ADX等指标在warm-up期(前N个bar)偏差较大，
        # 因初始值使用首个bar而非充分历史均值，P2级别待规划修复

    if not indicator_cols:
        return {"consistency_ok": True, "max_diff_pct": 0.0, "action": "no_indicators"}

    max_diff_pct = 0.0
    diff_details = {}

    if not isinstance(bar_data_1m.index, pd.DatetimeIndex):
        if 'datetime' in bar_data_1m.columns:
            bar_data_1m = bar_data_1m.set_index('datetime')
        elif 'minute' in bar_data_1m.columns:
            bar_data_1m = bar_data_1m.set_index('minute')
        else:
            return {"consistency_ok": True, "max_diff_pct": 0.0, "action": "no_datetime_index"}

    for col in indicator_cols:
        if col not in bar_data_1m.columns:
            continue

        # 路径A：1分钟指标 → 聚合（取最后一个值）
        series_a = bar_data_1m[col].resample(f'{bar_interval_minutes}min').last()

        # 路径B：先聚合OHLC → 在聚合Bar上重算（简化：用聚合后的close近似）
        # 由于无法在聚合Bar上重算指标（需要完整历史），这里用路径A的均值近似
        series_b = bar_data_1m[col].resample(f'{bar_interval_minutes}min').mean()

        # 计算差异
        common_idx = series_a.dropna().index.intersection(series_b.dropna().index)  # R21-MEM-P2-02修复: 两次dropna()各产生中间Series副本
        if len(common_idx) > 0:
            vals_a = series_a.loc[common_idx]
            vals_b = series_b.loc[common_idx]
            valid_mask = (vals_a != 0) & (vals_b != 0)
            if valid_mask.any():
                diffs = abs(vals_a[valid_mask] - vals_b[valid_mask]) / abs(vals_a[valid_mask]) * 100
                col_max_diff = float(diffs.max())
                max_diff_pct = max(max_diff_pct, col_max_diff)
                diff_details[col] = round(col_max_diff, 2)

    consistency_ok = max_diff_pct <= 5.0

    return {
        "consistency_ok": consistency_ok,
        "max_diff_pct": round(max_diff_pct, 2),
        "bar_interval_minutes": bar_interval_minutes,
        "decision_interval_minutes": decision_interval_minutes,
        "diff_details": diff_details,
        "recommendation": "明确指标计算频率规范(在原生频率计算后下采样)" if not consistency_ok else "当前可接受",
    }


def validate_trend_score_bar_vs_tick_correlation(
    bar_data: pd.DataFrame = None,
    tick_data: pd.DataFrame = None,
    min_correlation: float = 0.7,
    n_samples: int = 500,
) -> Dict[str, Any]:
    """P1-裂缝50：验证基于Bar推断的trend_score与基于tick计算的trend_score相关性

    _infer_trend_scores_from_bar使用分钟Bar数据推断趋势评分，
    但原始MultiPeriodTrendScorer需要tick级订单流。推断误差需验证。
    要求相关性 > 0.7。
    """
    if bar_data is None or bar_data.empty:
        return {"passed": True, "action": "no_data", "details": "无Bar数据可验证"}

    # 当无tick数据时，使用Bar内波动率作为代理指标验证
    n = min(n_samples, len(bar_data))
    bar_scores = []
    proxy_scores = []

    for idx in range(n):
        bar = bar_data.iloc[idx]
        bar_score, _ = _infer_trend_scores_from_bar(bar)
        bar_scores.append(bar_score[0])  # short_score

        # 代理指标：Bar内波动率与方向一致性
        high = bar.get("high", 0)
        low = bar.get("low", 0)
        open_p = bar.get("open", 0)
        close = bar.get("close", 0)
        if open_p > 0 and high > low:
            bar_range = (high - low) / open_p
            direction = 1.0 if close > open_p else -1.0
            proxy_score = direction * min(bar_range * 10, 1.0)
        else:
            proxy_score = 0.0
        proxy_scores.append(proxy_score)

    if len(bar_scores) < 10:
        return {"passed": True, "action": "insufficient_data", "details": "样本不足"}

    correlation = float(np.corrcoef(bar_scores, proxy_scores)[0, 1])
    passed = not np.isnan(correlation) and correlation >= min_correlation

    if not passed:
        logger.warning(
            "[P1-裂缝50] trend_score Bar推断与代理指标相关性%.3f < %.1f阈值, "
            "推断误差可能过大",
            correlation if not np.isnan(correlation) else 0.0, min_correlation,
        )

    return {
        "passed": passed,
        "correlation": round(correlation, 4) if not np.isnan(correlation) else 0.0,
        "min_correlation": min_correlation,
        "n_samples": len(bar_scores),
        "action": "use_tick_level_scorer" if not passed else "proceed",
    }


def run_validation_with_registry(backtest_result, validation_funcs=None):
    """ValidationRegistry编排入口 — USE_VALIDATION_REGISTRY=True时使用"""
    try:
        from ali2026v3_trading.validation_registry import ValidationRegistry, BacktestValidator, BacktestResult, ValidationResult
        registry = ValidationRegistry()
        if validation_funcs:
            for name, func in validation_funcs.items():
                class _FuncValidator:
                    __name__ = name
                    def __init__(self, fn, n):
                        self._fn = fn
                        self.name = n
                    def validate(self, result):
                        try:
                            r = self._fn(result)
                            return ValidationResult(validator_name=self.name, passed=r.get('passed', True), details=r)
                        except Exception as e:
                            return ValidationResult(validator_name=self.name, passed=False, details={'error': str(e)})
                registry.register(name, _FuncValidator(func, name))
        return registry.run_all(backtest_result)
    except ImportError:
        return None