# [M1-74] 共享交易常量
#!/usr/bin/env python3
"""
R27-CP-01-FIX: 生产/离线双向依赖修复 — 共享交易常量模块

原始定义位置:
  - P0_IRON_RULES:       param_pool/backtest_param_grids.py (原line 647)
  - REASON_MULTIPLIERS:  param_pool/backtest_param_grids.py (原line 619)
  - detect_rollover_gaps:  param_pool/backtest_runner_base.py (原line 2965)
  - compute_rollover_cost: param_pool/backtest_runner_base.py (原line 3048)

提取原因: 生产模块(strategy_core_service.py, position_service.py, risk_circuit_breaker.py)
原先从param_pool/task_scheduler.py导入这些常量，导致生产→回测层反向依赖。
现将共享常量提取到此模块，生产层和回测层均从此处导入，消除反向依赖。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

_logger = logging.getLogger(__name__)


CONTRACT_MULTIPLIER_MAP = {
    'IF': 300.0,
    'IC': 200.0,
    'IH': 300.0,
    'TS': 200.0,
    'TF': 10000.0,
    'T': 10000.0,
    'AU': 1000.0,
    'AG': 15.0,
    'CU': 5.0,
    'RB': 10.0,
}

# 五唯一性修复：期权品种→标的期货品种映射（统一定义）
OPTION_TO_FUTURE_MAP = {'MO': 'IM', 'IO': 'IF', 'HO': 'IH'}


# R16-P1-001修复: REASON_MULTIPLIERS已扩展为7条目(原手册5条目+ARBITRAGE+MARKET_MAKING)
# 新增条目对应策略生态系统的ARBITRAGE和MARKET_MAKING策略类型
REASON_MULTIPLIERS = {
    "CORRECT_RESONANCE":    {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
    "CORRECT_DIVERGENCE":   {"tp_mult": 0.8,  "sl_mult": 0.8,  "time_mult": 0.67},
    "INCORRECT_REVERSAL":   {"tp_mult": 0.87, "sl_mult": 1.2,  "time_mult": 0.67},
    "OTHER_SCALP":          {"tp_mult": 0.73, "sl_mult": 0.6,  "time_mult": 0.33},
    "ARBITRAGE":            {"tp_mult": 0.5,  "sl_mult": 0.5,  "time_mult": 0.25},
    "MARKET_MAKING":        {"tp_mult": 0.4,  "sl_mult": 0.8,  "time_mult": 1.0},
    "MANUAL":               {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
}


# ── P0绿灯铁律阈值（不可妥协，但必须参数化可审计）──
P0_IRON_RULES = {
    "max_oos_decay": -0.30,           # 样本外衰减铁律 (与parameter_attribute_matrix.yaml p0_max_oos_decay对齐)
    "oos_decay_warn": -0.20,          # 样本外衰减警告 (与parameter_attribute_matrix.yaml p0_oos_decay_warn对齐)
    "min_train_sharpe": 0.5,          # 训练夏普铁律 (与parameter_attribute_matrix.yaml p0_min_train_sharpe对齐)
    "min_test_sharpe": 0.3,           # 样本外夏普铁律 (与parameter_attribute_matrix.yaml p0_min_test_sharpe对齐)
    "min_lr_threshold": 0.8,          # 逻辑反转阈值铁律 (与parameter_attribute_matrix.yaml p0_min_lr_threshold对齐)
    "lr_threshold_warn": 1.0,         # 逻辑反转阈值警告 (与parameter_attribute_matrix.yaml p0_lr_threshold_warn对齐)
    "max_drawdown_limit": -0.50,      # 最大回撤生存红线 (P0绿灯检验第4项)
    "min_signal_count": 30,           # 最少信号数统计显著性 (与cascade_config.yaml data_quality.min_trades对齐)
    "max_daily_trigger": 2.0,           # 日均触发上限
    "max_loss_hit_rate": 0.20,          # 亏损命中率上限
    "min_two_x_recovery_rate": 0.30,    # 两倍恢复率下限
    "min_oos_retention": 0.50,          # 样本外保留率下限
    "alpha_threshold_hft": 0.5,              # S1 HFT Alpha最低阈值
    "alpha_threshold_minute": 0.5,           # S2 分钟Alpha最低阈值
    "alpha_threshold_box_extreme": 0.3,      # S3 箱体Alpha最低阈值
    "alpha_threshold_box_spring": 0.4,       # S4 弹簧Alpha最低阈值
    "alpha_threshold_arbitrage": 0.3,        # S5 套利Alpha最低阈值
    "alpha_threshold_market_making": 0.2,    # S6 做市Alpha最低阈值
    "alpha_pct_threshold": 30,               # Alpha占比百分比阈值
}


def detect_rollover_gaps(bar_data: pd.DataFrame,
                          contract_col: str = "symbol",
                          price_col: str = "close",
                          gap_threshold: float = 0.02) -> List[Dict[str, Any]]:
    """P0-裂缝5：检测合约展期数据断裂点

    找出合约代码变化的位置，计算展期跳空幅度。
    若跳空幅度 > gap_threshold(默认2%)，标记为展期点。

    Returns:
        List[Dict]: 每个展期点的详细信息
    """
    if bar_data.empty or contract_col not in bar_data.columns:
        return []

    rollover_points = []
    contract_series = bar_data[contract_col].values
    price_series = bar_data[price_col].values if price_col in bar_data.columns else None

    for i in range(1, len(contract_series)):
        if contract_series[i] != contract_series[i - 1]:
            point = {
                "index": i,
                "prev_contract": str(contract_series[i - 1]),
                "new_contract": str(contract_series[i]),
                "gap_pct": 0.0,
                "is_significant": False,
            }
            if price_series is not None and price_series[i - 1] > 0:
                gap = (price_series[i] - price_series[i - 1]) / price_series[i - 1]
                point["gap_pct"] = round(gap * 100, 2)
                point["is_significant"] = abs(gap) > gap_threshold
            rollover_points.append(point)

    return rollover_points


def compute_rollover_cost(rollover_points: List[Dict[str, Any]],
                           bar_data: pd.DataFrame,
                           params: Dict[str, float],
                           calendar_basis_bps: float = 5.0,
                           rollover_slippage_bps: float = 3.0) -> Dict[str, Any]:
    """P1-1修复：换月成本建模 — 量化calendar_basis(日历基差) + slippage(滑点)

    在换月点计算三部分成本:
      1. gap_cost: 展期跳空价差(已由detect_rollover_gaps检测)
      2. calendar_basis: 近远月合约基差(默认5bps)
      3. rollover_slippage: 换月双边滑点(默认3bps)

    Returns:
        Dict: 换月总成本及各分项
    """
    if not rollover_points:
        return {"total_rollover_cost_bps": 0.0, "rollover_count": 0}

    total_gap_bps = 0.0
    for rp in rollover_points:
        total_gap_bps += abs(rp.get("gap_pct", 0.0)) * 100  # gap_pct是百分比

    n_rollovers = len(rollover_points)
    calendar_basis_total = calendar_basis_bps * n_rollovers
    slippage_total = rollover_slippage_bps * n_rollovers * 2  # 双边(平旧+开新)

    total_cost_bps = total_gap_bps + calendar_basis_total + slippage_total

    _logger.info(
        "[裂缝5-换月成本] gap=%.1fbps calendar_basis=%.1fbps slippage=%.1fbps total=%.1fbps (n=%d)",
        total_gap_bps, calendar_basis_total, slippage_total, total_cost_bps, n_rollovers,
    )

    return {
        "total_rollover_cost_bps": round(total_cost_bps, 2),
        "gap_cost_bps": round(total_gap_bps, 2),
        "calendar_basis_bps": round(calendar_basis_total, 2),
        "calendar_basis_per_rollover_bps": calendar_basis_bps,
        "slippage_bps": round(slippage_total, 2),
        "slippage_per_rollover_bps": rollover_slippage_bps,
        "rollover_count": n_rollovers,
        "annualized_cost_bps": round(total_cost_bps * 12 / max(1, n_rollovers), 2),  # 假设每月换月
    }
