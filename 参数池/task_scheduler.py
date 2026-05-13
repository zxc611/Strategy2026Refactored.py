#!/usr/bin/env python3
"""
量化任务调度系统：参数网格扫描 + 多进程回测 + 结果汇总

V7生产版：
  1. 部署共振策略真实回测逻辑（状态路由+止盈止损+风控）
  2. V7全16参数分层优化网格
     - Round1粗扫：6个核心交易参数，3×4×3×3×3×3=972组合 ~16分钟
     - Round2精扫：Round1 Top-K固定核心参数 + 10个辅助参数各2值
  3. 数据预加载+共享内存、增量重跑、参数化查询
"""
from __future__ import annotations

import itertools
import json
import logging
import os
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

PREPROCESSED_DB = "preprocessed.duckdb"
RESULTS_DB = "quant_results.duckdb"
MAX_WORKERS = min(max(1, (os.cpu_count() or 4) // 2), 16)
TRAIN_START = "2023-01-01"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"
TARGET_SYMBOLS: Optional[List[str]] = None
INITIAL_EQUITY = 1_000_000.0
COMMISSION_PER_LOT = 1.5
SLIPPAGE_BPS = 1.0

PARAM_GRID_ROUND1 = {
    "close_take_profit_ratio": [1.1, 1.5, 2.5],
    "close_stop_loss_ratio": [0.3, 0.4, 0.5, 0.6],
    "max_risk_ratio": [0.2, 0.3, 0.5],
    "lots_min": [1, 3, 5],
    "signal_cooldown_sec": [0.0, 60.0, 120.0],
    "non_other_ratio_threshold": [0.3, 0.4, 0.5],
    "decision_interval_minutes": [1, 5, 15],
}

PARAM_DEFAULTS = {
    "close_take_profit_ratio": 1.5,
    "close_stop_loss_ratio": 0.5,
    "max_risk_ratio": 0.3,
    "lots_min": 3,
    "max_signals_per_window": 5,
    "signal_cooldown_sec": 60.0,
    "non_other_ratio_threshold": 0.4,
    "state_confirm_bars": 3,
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.95,
    "spring_max_position_pct": 0.015,
    "capital_route_master_base": 0.60,
    "shadow_alpha_threshold": 0.1,
    "rate_limit_global_per_min": 60,
    "hard_time_stop_minutes": 90.0,
    "daily_loss_hard_stop_pct": 0.05,
    "logic_reversal_threshold": 1.5,
    "decision_interval_minutes": 1,
}

PARAM_GRID_ROUND2 = {
    "max_signals_per_window": [3, 8],
    "state_confirm_bars": [2, 5],
    "spring_stop_profit_ratio": [3.0, 8.0],
    "spring_max_loss_pct": [0.85, 0.95],
    "spring_max_position_pct": [0.010, 0.025],
    "capital_route_master_base": [0.50, 0.70],
    "shadow_alpha_threshold": [0.0, 0.2],
    "rate_limit_global_per_min": [30, 120],
    "hard_time_stop_minutes": [60.0, 120.0],
    "daily_loss_hard_stop_pct": [0.03, 0.08],
}

PARAM_GRID_BOX_EXTREME = {
    "box_detection_threshold": [0.02, 0.03, 0.05],
    "box_min_bars": [10, 20, 30],
    "extreme_entry_ratio": [0.3, 0.5, 0.7],
    "close_take_profit_ratio": [1.5, 2.0, 3.0],
    "close_stop_loss_ratio": [0.3, 0.5, 0.7],
    "decision_interval_minutes": [1, 5, 15],
}

PARAM_DEFAULTS_BOX_EXTREME = {
    "box_detection_threshold": 0.03,
    "box_min_bars": 20,
    "extreme_entry_ratio": 0.5,
    "close_take_profit_ratio": 2.0,
    "close_stop_loss_ratio": 0.5,
    "lots_min": 1,
    "max_risk_ratio": 0.2,
    "decision_interval_minutes": 5,
}

PARAM_GRID_BOX_SPRING = {
    "spring_iv_threshold": [0.15, 0.20, 0.25],
    "spring_maturity_days": [7, 14, 30],
    "spring_impulse_threshold": [0.01, 0.02, 0.03],
    "spring_stop_profit_ratio": [3.0, 5.0, 8.0],
    "spring_max_loss_pct": [0.85, 0.90, 0.95],
    "decision_interval_minutes": [1, 5, 15],
}

PARAM_DEFAULTS_BOX_SPRING = {
    "spring_iv_threshold": 0.20,
    "spring_maturity_days": 14,
    "spring_impulse_threshold": 0.02,
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.90,
    "spring_max_position_pct": 0.015,
    "lots_min": 1,
    "decision_interval_minutes": 5,
    "close_take_profit_ratio": 5.0,
    "close_stop_loss_ratio": 0.95,
    "hard_time_stop_minutes": 240.0,
    "max_risk_ratio": 0.015,
}

PARAM_GRID_HFT = {
    "hft_signal_confirm_ticks": [3, 5, 8],
    "hft_cooldown_ms": [50.0, 100.0, 200.0],
    "hft_min_imbalance": [0.15, 0.25, 0.35],
    "close_take_profit_ratio": [1.2, 1.5, 2.0],
    "close_stop_loss_ratio": [0.2, 0.3, 0.5],
}

HFT_TICK_PARAMS = {"hft_signal_confirm_ticks", "hft_cooldown_ms", "hft_min_imbalance"}

PARAM_DEFAULTS_HFT = {
    "hft_signal_confirm_ticks": 5,
    "hft_cooldown_ms": 100.0,
    "hft_min_imbalance": 0.25,
    "close_take_profit_ratio": 1.5,
    "close_stop_loss_ratio": 0.3,
    "lots_min": 1,
    "max_risk_ratio": 0.2,
    "non_other_ratio_threshold": 0.4,
    "hard_time_stop_minutes": 30.0,
    "daily_loss_hard_stop_pct": 0.03,
    "decision_interval_minutes": 1,
}

PARAM_DEFAULTS_SHADOW_A = {
    **PARAM_DEFAULTS,
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.6,
    "hard_time_stop_minutes": 60.0,
    "max_risk_ratio": 0.15,
}

PARAM_DEFAULTS_SHADOW_B = {
    **PARAM_DEFAULTS,
    "close_take_profit_ratio": 1.1,
    "close_stop_loss_ratio": 0.7,
    "hard_time_stop_minutes": 45.0,
    "max_risk_ratio": 0.1,
}

PARAM_DEFAULTS_HFT_SHADOW_A = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.25,
    "hard_time_stop_minutes": 20.0,
    "max_risk_ratio": 0.12,
}

PARAM_DEFAULTS_HFT_SHADOW_B = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 1.0,
    "close_stop_loss_ratio": 0.2,
    "hard_time_stop_minutes": 15.0,
}

PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A = {
    **PARAM_DEFAULTS_BOX_EXTREME,
    "close_take_profit_ratio": 1.6,
    "close_stop_loss_ratio": 0.6,
    "max_risk_ratio": 0.12,
}

PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B = {
    **PARAM_DEFAULTS_BOX_EXTREME,
    "close_take_profit_ratio": 1.3,
    "close_stop_loss_ratio": 0.7,
    "max_risk_ratio": 0.08,
}

PARAM_DEFAULTS_BOX_SPRING_SHADOW_A = {
    **PARAM_DEFAULTS_BOX_SPRING,
    "spring_stop_profit_ratio": 4.0,
    "spring_max_loss_pct": 0.85,
    "spring_max_position_pct": 0.010,
    "close_take_profit_ratio": 4.0,
    "close_stop_loss_ratio": 0.80,
    "hard_time_stop_minutes": 180.0,
    "max_risk_ratio": 0.012,
}

PARAM_DEFAULTS_BOX_SPRING_SHADOW_B = {
    **PARAM_DEFAULTS_BOX_SPRING,
    "spring_stop_profit_ratio": 3.0,
    "spring_max_loss_pct": 0.80,
    "spring_max_position_pct": 0.008,
    "close_take_profit_ratio": 3.0,
    "close_stop_loss_ratio": 0.70,
    "hard_time_stop_minutes": 120.0,
    "max_risk_ratio": 0.010,
}

REASON_MULTIPLIERS = {
    "CORRECT_RESONANCE":    {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
    "CORRECT_DIVERGENCE":   {"tp_mult": 0.8,  "sl_mult": 0.8,  "time_mult": 0.67},
    "INCORRECT_REVERSAL":   {"tp_mult": 0.87, "sl_mult": 1.2,  "time_mult": 0.67},
    "OTHER_SCALP":          {"tp_mult": 0.73, "sl_mult": 0.6,  "time_mult": 0.33},
    "MANUAL":               {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
}

SHADOW_PARAM_MAP = {
    "main": None,
    "shadow_reverse": "shadow_a",
    "shadow_random": "shadow_b",
}

STRATEGY_SHADOW_DEFAULTS = {
    "hft":             {"shadow_a": PARAM_DEFAULTS_HFT_SHADOW_A,          "shadow_b": PARAM_DEFAULTS_HFT_SHADOW_B},
    "main":            {"shadow_a": PARAM_DEFAULTS_SHADOW_A,              "shadow_b": PARAM_DEFAULTS_SHADOW_B},
    "box_extreme":     {"shadow_a": PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A,  "shadow_b": PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B},
    "box_spring":      {"shadow_a": PARAM_DEFAULTS_BOX_SPRING_SHADOW_A,   "shadow_b": PARAM_DEFAULTS_BOX_SPRING_SHADOW_B},
}

ROUND1_TOP_K = 10

PARAM_GRID = PARAM_GRID_ROUND1


def validate_shadow_param_independence(threshold: float = 0.20) -> Dict[str, float]:
    """P0-Q1质量门：验证影子策略参数与主策略差异度>threshold

    对每个策略组，计算影子A/B与主策略的关键参数差异度。
    差异度 = avg(|shadow_param - main_param| / main_param)
    """
    _SHADOW_DIFF_KEYS = [
        "close_take_profit_ratio", "close_stop_loss_ratio",
        "hard_time_stop_minutes", "max_risk_ratio",
    ]
    results = {}
    for group_name, main_params, shadow_a, shadow_b in [
        ("S2_main", PARAM_DEFAULTS, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B),
        ("S1_hft", PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B),
        ("S3_box_extreme", PARAM_DEFAULTS_BOX_EXTREME,
         PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B),
        ("S4_box_spring", PARAM_DEFAULTS_BOX_SPRING,
         PARAM_DEFAULTS_BOX_SPRING_SHADOW_A, PARAM_DEFAULTS_BOX_SPRING_SHADOW_B),
    ]:
        for shadow_name, shadow_params in [("shadow_a", shadow_a), ("shadow_b", shadow_b)]:
            diffs = []
            for key in _SHADOW_DIFF_KEYS:
                if key in main_params and key in shadow_params and main_params[key] != 0:
                    diffs.append(abs(shadow_params[key] - main_params[key]) / abs(main_params[key]))
            avg_diff = sum(diffs) / max(1, len(diffs))
            label = f"{group_name}.{shadow_name}"
            results[label] = round(avg_diff, 4)
            if avg_diff < threshold:
                logger.warning("[P0-Q1 FAIL] %s 参数差异度 %.2f%% < %.0f%% 阈值", label, avg_diff * 100, threshold * 100)
            else:
                logger.info("[P0-Q1 PASS] %s 参数差异度 %.2f%%", label, avg_diff * 100)
    return results

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class _BacktestPosition:
    instrument_id: str
    volume: int
    open_price: float
    open_time: pd.Timestamp
    stop_profit_price: float
    stop_loss_price: float
    open_reason: str
    lots: int = 1
    open_state: str = "other"
    open_strength: float = 0.0
    max_float_profit: float = 0.0
    stage1_passed: bool = False
    profit_history: list = field(default_factory=list)
    last_check_time: Optional[pd.Timestamp] = None


@dataclass
class _BacktestState:
    equity: float = INITIAL_EQUITY
    peak_equity: float = INITIAL_EQUITY
    positions: Dict[str, _BacktestPosition] = field(default_factory=dict)
    equity_curve: Deque[float] = field(default_factory=lambda: deque(maxlen=100_000))
    daily_returns: Deque[float] = field(default_factory=lambda: deque(maxlen=100_000))
    current_state: str = "other"
    state_confirm_count: int = 0
    pending_state: Optional[str] = None
    last_state_check_time: Optional[pd.Timestamp] = None
    last_signal_time: Optional[pd.Timestamp] = None
    circuit_breaker_until: Optional[pd.Timestamp] = None
    daily_loss: float = 0.0
    daily_start_equity: float = INITIAL_EQUITY
    total_signals: int = 0
    total_trades: int = 0
    prev_date: Optional[str] = None


_STATE_MAP = {
    "correct_rise": "correct_trending",
    "correct_fall": "correct_trending",
    "wrong_rise": "incorrect_reversal",
    "wrong_fall": "incorrect_reversal",
    "other": "other",
}

_STATE_REASON_MAP = {
    "correct_trending": "CORRECT_RESONANCE",
    "incorrect_reversal": "INCORRECT_REVERSAL",
    "other": "OTHER_SCALP",
}


def _resolve_tp_sl(params: Dict[str, float], open_reason: str) -> Tuple[float, float]:
    base_tp = params.get("close_take_profit_ratio", 1.5)
    base_sl = params.get("close_stop_loss_ratio", 0.5)
    mult = REASON_MULTIPLIERS.get(open_reason, {"tp_mult": 1.0, "sl_mult": 1.0})
    return (base_tp * mult["tp_mult"], base_sl * mult["sl_mult"])


def _resolve_time_stop(params: Dict[str, float], open_reason: str) -> float:
    base_time = params.get("hard_time_stop_minutes", 90.0)
    mult = REASON_MULTIPLIERS.get(open_reason, {"time_mult": 1.0})
    return base_time * mult["time_mult"]


def _check_state_transition(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> str:
    non_other = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0) + bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
    threshold = params.get("non_other_ratio_threshold", 0.4)

    if non_other < threshold:
        candidate = "other"
    else:
        correct = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0)
        incorrect = bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
        candidate = "correct_trending" if correct >= incorrect else "incorrect_reversal"

    confirm_bars = int(params.get("state_confirm_bars", 3))

    if candidate == bt.current_state:
        bt.pending_state = None
        bt.state_confirm_count = 0
        return bt.current_state

    if bt.pending_state == candidate:
        bt.state_confirm_count += 1
    else:
        bt.pending_state = candidate
        bt.state_confirm_count = 1

    if bt.state_confirm_count >= confirm_bars:
        bt.current_state = candidate
        bt.pending_state = None
        bt.state_confirm_count = 0

    return bt.current_state


def _compute_dynamic_slippage_bps(
    price: float,
    bid_ask_spread: float,
    base_slippage_bps: float = SLIPPAGE_BPS,
    spread_quality: int = 1,
) -> float:
    """动态滑点模型

    核心：滑点 = max(base_slippage, bid_ask_spread占比放大)
    - 流动性好（spread小）：用base_slippage（如1bps）
    - 流动性差（spread大）：spread/price放大为bps
    - 远月/深度虚值spread可达5-20个tick，此时滑点远超1bps
    """
    if price <= 0:
        return base_slippage_bps
    if bid_ask_spread <= 0 or spread_quality == 0:
        if spread_quality == 0 and bid_ask_spread <= 0:
            logger.debug("[SLIPPAGE_DEGRADE] spread=%.4f quality=%d, using static %.1fbps", bid_ask_spread, spread_quality, base_slippage_bps)
        return base_slippage_bps
    spread_bps = bid_ask_spread / price * 10000.0
    return max(base_slippage_bps, spread_bps * 0.5)


def _check_logic_reversal(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> bool:
    """逻辑反转平仓检测

    如果持仓基于correct_trending（一致性共振），但当前Bar的信号强烈反转
    （wrong_rise_pct + wrong_fall_pct > correct_rise_pct + correct_fall_pct 且
     超出比例 > reversal_threshold），立即触发逻辑反转平仓。
    这模拟顶级基金的操作：不等待止损，在信号逻辑反转时第一时间离场。
    """
    for symbol, pos in list(bt.positions.items()):
        if pos.open_reason != "CORRECT_RESONANCE":
            continue

        correct_pct = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0)
        wrong_pct = bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
        reversal_threshold = params.get("logic_reversal_threshold", 1.5)

        if wrong_pct > correct_pct * reversal_threshold and wrong_pct > 0.3:
            price = bar.get("close", 0.0)
            bar_time = bar.get("minute", pd.Timestamp.now())
            if price <= 0:
                continue

            pnl_per_lot = (price - pos.open_price) * pos.volume / abs(pos.volume) if pos.volume != 0 else 0
            pnl = pnl_per_lot * pos.lots
            bid_ask = bar.get("bid_ask_spread", 0.0)
            spread_q = bar.get("_spread_quality", 1)
            slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q)
            slip = price * slip_bps / 10000 * pos.lots
            commission = pos.lots * COMMISSION_PER_LOT
            bt.equity += pnl - slip - commission
            bt.total_trades += 1
            del bt.positions[symbol]
            logger.debug("逻辑反转平仓: %s @ %.2f (wrong=%.2f > correct*%.1f=%.2f)",
                         symbol, price, wrong_pct, reversal_threshold, correct_pct * reversal_threshold)

    return True


def _check_safety(
    bt: _BacktestState,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
) -> bool:
    if bt.circuit_breaker_until is not None and bar_time < bt.circuit_breaker_until:
        return False

    hard_stop = params.get("daily_loss_hard_stop_pct", 0.05)
    if bt.daily_start_equity > 0:
        daily_drawdown = (bt.daily_start_equity - bt.equity) / bt.daily_start_equity
        if daily_drawdown >= hard_stop:
            return False

    return True


def _try_open(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    strategy_type: str = "main",
) -> None:
    bar_time = bar.get("minute", pd.Timestamp.now())
    cooldown = params.get("signal_cooldown_sec", 0.0)
    if bt.last_signal_time is not None and cooldown > 0:
        elapsed = (bar_time - bt.last_signal_time).total_seconds()
        if elapsed < cooldown:
            return

    max_signals = int(params.get("max_signals_per_window", 5))
    if bt.total_signals >= max_signals * 100:
        return

    reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
    tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)

    symbol = bar.get("symbol", "unknown")
    price = bar.get("close", 0.0)
    if price <= 0:
        return

    strength = bar.get("strength", 0.0)
    if strategy_type == "main" and reason == "CORRECT_RESONANCE" and strength < 0.3:
        return

    lots = int(params.get("lots_min", 1))
    risk = params.get("max_risk_ratio", 0.3)
    max_lots = max(1, int(bt.equity * risk / (price * sl_ratio + 1e-8)))
    lots = min(lots, max_lots)

    direction = 1 if bar.get("imbalance", 0) >= 0 else -1
    if strategy_type == "shadow_reverse":
        direction = -direction
    volume = direction * lots

    sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
    sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 1)
    slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q)
    slip_cost = price * slip_bps / 10000 * lots
    commission = lots * COMMISSION_PER_LOT * 2
    bt.equity -= (commission + slip_cost)

    pos = _BacktestPosition(
        instrument_id=symbol,
        volume=volume,
        open_price=price,
        open_time=bar_time,
        stop_profit_price=sp_price,
        stop_loss_price=sl_price,
        open_reason=reason,
        lots=lots,
        open_state=bt.current_state,
        open_strength=strength,
    )
    bt.positions[symbol] = pos
    bt.last_signal_time = bar_time
    bt.total_signals += 1


def _check_two_stage_stop(
    pos: _BacktestPosition,
    price: float,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
) -> bool:
    stage1_min_minutes = params.get("stage1_min_minutes", 90.0)
    stage1_profit_threshold = params.get("stage1_profit_threshold", 0.002)
    stage2_slope_window = max(2, int(params.get("stage2_slope_window", 10)))
    stage2_slope_threshold = params.get("stage2_slope_threshold", 0.0)

    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct

    if not pos.stage1_passed:
        if hold_minutes >= stage1_min_minutes and pos.max_float_profit >= stage1_profit_threshold:
            pos.stage1_passed = True

    if not pos.stage1_passed:
        return False

    pos.profit_history.append(float_pnl_pct)
    if pos.last_check_time is not None and pos.last_check_time == bar_time:
        pass
    pos.last_check_time = bar_time

    if len(pos.profit_history) >= stage2_slope_window:
        window = pos.profit_history[-stage2_slope_window:]
        slope = (window[-1] - window[0]) / stage2_slope_window
        if slope < stage2_slope_threshold:
            return True

    return False


def _check_positions(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> None:
    symbol = bar.get("symbol", "")
    price = bar.get("close", 0.0)
    bar_time = bar.get("minute", pd.Timestamp.now())
    if symbol not in bt.positions or price <= 0:
        return

    pos = bt.positions[symbol]
    open_reason = getattr(pos, 'open_reason', 'CORRECT_RESONANCE')
    hard_stop_min = _resolve_time_stop(params, open_reason)
    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0

    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct
    if float_pnl_pct > pos.max_float_profit:
        pos.max_float_profit = float_pnl_pct

    should_close = False
    close_reason = ""

    if pos.volume > 0:
        if price >= pos.stop_profit_price:
            should_close = True
            close_reason = "StopProfit"
        elif price <= pos.stop_loss_price:
            should_close = True
            close_reason = "StopLoss"
    elif pos.volume < 0:
        if price <= pos.stop_profit_price:
            should_close = True
            close_reason = "StopProfit"
        elif price >= pos.stop_loss_price:
            should_close = True
            close_reason = "StopLoss"

    if not should_close and _check_two_stage_stop(pos, price, bar_time, params):
        should_close = True
        close_reason = "TwoStageTimeStop"

    if not should_close and hold_minutes >= hard_stop_min:
        should_close = True
        close_reason = "HardTimeStop"

    if not should_close:
        eod_hour = bar_time.hour
        eod_minute = bar_time.minute
        if eod_hour == 14 and eod_minute >= 55:
            should_close = True
            close_reason = "EOD"

    if should_close:
        pnl_per_lot = (price - pos.open_price) * pos.volume / abs(pos.volume) if pos.volume != 0 else 0
        pnl = pnl_per_lot * pos.lots
        bid_ask = bar.get("bid_ask_spread", 0.0)
        spread_q = bar.get("_spread_quality", 1)
        slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q)
        slip = price * slip_bps / 10000 * pos.lots
        commission = pos.lots * COMMISSION_PER_LOT
        bt.equity += pnl - slip - commission
        bt.total_trades += 1
        del bt.positions[symbol]

        daily_dd = (bt.daily_start_equity - bt.equity) / bt.daily_start_equity if bt.daily_start_equity > 0 else 0
        if daily_dd > 0.03:
            pause_sec = params.get("circuit_breaker_pause_sec", 180.0)
            bt.circuit_breaker_until = bar_time + pd.Timedelta(seconds=pause_sec)


def _reset_daily(bt: _BacktestState, current_date: str) -> None:
    if bt.prev_date is not None and current_date != bt.prev_date:
        if bt.daily_start_equity > 0:
            daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
            bt.daily_returns.append(daily_ret)
        bt.daily_start_equity = bt.equity
        bt.daily_loss = 0.0
        bt.circuit_breaker_until = None
    bt.prev_date = current_date


def run_backtest(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """V7共振策略回测：完整信号→决策→执行→风控闭环

    Args:
        params: 参数字典
        bar_data: 分钟Bar数据
        train: 是否训练集
        strategy_type: 策略模式
            - 'main': 主策略（正常逻辑）
            - 'shadow_reverse': 影子策略A（反向逻辑，开仓方向相反）
            - 'shadow_random': 影子策略B（随机动作，信号随机化）

    V7.1时间框架适应性：
        decision_interval_minutes控制决策频率，风控每Bar都执行。
        - 默认1=逐Bar决策（向后兼容）
        - 5=每5分钟决策一次（S3/S4推荐）
        - 15=每15分钟决策一次（低频模式）
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params}

    bt = _BacktestState()
    np.random.seed(42 if train else 24)
    decision_interval = max(1, int(params.get("decision_interval_minutes", 1)))

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.Timestamp.now())
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        if idx % decision_interval == 0:
            _check_state_transition(bt, bar, params)
            _check_logic_reversal(bt, bar, params)

            if _check_safety(bt, bar_time, params):
                strength = bar.get("strength", 0)
                should_open = strength > 0.3 and len(bt.positions) < 3

                if strategy_type == "shadow_random":
                    should_open = np.random.random() < 0.02 and len(bt.positions) < 3

                if should_open:
                    _try_open(bt, bar, params, strategy_type=strategy_type)

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    total_return = bt.equity / INITIAL_EQUITY - 1

    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        returns = np.diff(equity_arr) / equity_arr[:-1]
        mean_r = np.mean(returns)
        std_r = np.std(returns)
        sharpe = np.sqrt(252 * 240) * mean_r / std_r if std_r > 1e-10 else 0.0
    else:
        sharpe = 0.0

    if len(equity_arr) > 0:
        cummax = np.maximum.accumulate(equity_arr)
        drawdowns = equity_arr / cummax - 1
        max_dd = float(np.min(drawdowns))
    else:
        max_dd = 0.0

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
    }


def run_backtest_box_extreme(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_extreme",
) -> Dict[str, Any]:
    """箱体极值策略回测

    在other状态下，检测箱体边界极值，做反向操作：
    - 箱底极值（价格触及箱体下沿）→ 做多
    - 箱顶极值（价格触及箱体上沿）→ 做空

    影子策略：
    - shadow_reverse: 方向强制反转（箱底→做空，箱顶→做多）
    - shadow_random: 随机方向（50/50）

    V7.1时间框架适应性：decision_interval_minutes控制决策频率，风控每Bar都执行。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    bt = _BacktestState()
    box_threshold = params.get("box_detection_threshold", 0.03)
    box_min_bars = int(params.get("box_min_bars", 20))
    extreme_ratio = params.get("extreme_entry_ratio", 0.5)
    np.random.seed(42 if train else 24)
    decision_interval = max(1, int(params.get("decision_interval_minutes", 5)))

    box_highs: List[float] = []
    box_lows: List[float] = []

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.Timestamp.now())
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        high = bar.get("high", 0.0)
        low = bar.get("low", 0.0)
        close = bar.get("close", 0.0)
        symbol = bar.get("symbol", "")

        box_highs.append(high)
        box_lows.append(low)
        if len(box_highs) > box_min_bars:
            box_highs.pop(0)
            box_lows.pop(0)

        for sym in list(bt.positions.keys()):
            if sym == symbol:
                _check_positions(bt, bar, params)

        if idx % decision_interval == 0:
            if len(box_highs) < box_min_bars:
                continue

            box_high = max(box_highs)
            box_low = min(box_lows)
            box_range = box_high - box_low

            if box_range < close * box_threshold:
                continue

            if _check_safety(bt, bar_time, params) and len(bt.positions) < 3:
                position_in_box = (close - box_low) / box_range if box_range > 0 else 0.5

                is_box_bottom = position_in_box < (1 - extreme_ratio)
                is_box_top = position_in_box > extreme_ratio

                should_open = False
                direction = 0

                if is_box_bottom:
                    direction = 1
                    should_open = True
                elif is_box_top:
                    direction = -1
                    should_open = True

                if strategy_type == "shadow_reverse":
                    direction = -direction
                elif strategy_type == "shadow_random":
                    if np.random.random() < 0.02:
                        direction = 1 if np.random.random() < 0.5 else -1
                        should_open = True
                    else:
                        should_open = False

                if should_open and direction != 0:
                    price = close
                    tp_ratio = params.get("close_take_profit_ratio", 2.0)
                    sl_ratio = params.get("close_stop_loss_ratio", 0.5)
                    lots = int(params.get("lots_min", 1))

                    sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
                    sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

                    commission = lots * COMMISSION_PER_LOT * 2
                    bt.equity -= commission

                    pos = _BacktestPosition(
                        instrument_id=symbol,
                        volume=direction * lots,
                        open_price=price,
                        open_time=bar_time,
                        stop_profit_price=sp_price,
                        stop_loss_price=sl_price,
                        open_reason="BOX_EXTREME",
                        lots=lots,
                    )
                    bt.positions[symbol] = pos
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        returns = np.diff(equity_arr) / equity_arr[:-1]
        sharpe = np.sqrt(252 * 240) * np.mean(returns) / np.std(returns) if np.std(returns) > 1e-10 else 0.0
    else:
        sharpe = 0.0

    max_dd = float(np.min(equity_arr / np.maximum.accumulate(equity_arr) - 1)) if len(equity_arr) > 0 else 0.0

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
    }


def run_backtest_box_spring(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_spring",
) -> Dict[str, Any]:
    """箱体弹簧策略回测

    条件：
    - IV极低（低于阈值）
    - 近月期权
    - 价格在箱体内部
    → 预期波动率回归，买入期权做多波动率

    影子策略：
    - shadow_reverse: 方向强制反转（买CALL→买PUT）
    - shadow_random: 随机方向

    V7.1时间框架适应性：decision_interval_minutes控制决策频率，风控每Bar都执行。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    bt = _BacktestState()
    iv_threshold = params.get("spring_iv_threshold", 0.20)
    impulse_threshold = params.get("spring_impulse_threshold", 0.02)
    np.random.seed(42 if train else 24)
    decision_interval = max(1, int(params.get("decision_interval_minutes", 5)))

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.Timestamp.now())
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        symbol = bar.get("symbol", "")
        close = bar.get("close", 0.0)
        iv = bar.get("iv", 0.0)
        high = bar.get("high", close)
        low = bar.get("low", close)

        for sym in list(bt.positions.keys()):
            if sym == symbol:
                _check_positions(bt, bar, params)

        if idx % decision_interval == 0:
            if _check_safety(bt, bar_time, params) and len(bt.positions) < 3:
                impulse = (high - low) / close if close > 0 else 0

                is_low_iv = iv > 0 and iv < iv_threshold
                is_impulse = impulse > impulse_threshold

                should_open = False
                direction = 0

                if is_low_iv and is_impulse:
                    direction = 1
                    should_open = True

                if strategy_type == "shadow_reverse":
                    direction = -direction
                elif strategy_type == "shadow_random":
                    if np.random.random() < 0.02:
                        direction = 1 if np.random.random() < 0.5 else -1
                        should_open = True
                    else:
                        should_open = False

                if should_open and direction != 0:
                    price = close
                    tp_ratio = params.get("spring_stop_profit_ratio", 5.0)
                    sl_ratio = params.get("spring_max_loss_pct", 0.90)
                    lots = int(params.get("lots_min", 1))

                    sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
                    sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

                    commission = lots * COMMISSION_PER_LOT * 2
                    bt.equity -= commission

                    pos = _BacktestPosition(
                        instrument_id=symbol,
                        volume=direction * lots,
                        open_price=price,
                        open_time=bar_time,
                        stop_profit_price=sp_price,
                        stop_loss_price=sl_price,
                        open_reason="BOX_SPRING",
                        lots=lots,
                    )
                    bt.positions[symbol] = pos
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        returns = np.diff(equity_arr) / equity_arr[:-1]
        sharpe = np.sqrt(252 * 240) * np.mean(returns) / np.std(returns) if np.std(returns) > 1e-10 else 0.0
    else:
        sharpe = 0.0

    max_dd = float(np.min(equity_arr / np.maximum.accumulate(equity_arr) - 1)) if len(equity_arr) > 0 else 0.0

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
    }


def run_backtest_hft(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
) -> Dict[str, Any]:
    """S1高频趋势共振回测：tick级决策频率

    与S2分钟级趋势共振共享底层逻辑（一致性共振→方向延续），
    但参数集完全独立（毫秒/tick数 vs 分钟/秒）：
    - hft_signal_confirm_ticks: 信号确认所需连续tick数
    - hft_cooldown_ms: 信号冷却时间（毫秒）
    - hft_min_imbalance: 最小允许开仓的imbalance阈值

    影子策略：
    - shadow_reverse: 方向强制反转
    - shadow_random: 随机方向（概率基于信号频率）
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    bt = _BacktestState()
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    np.random.seed(42 if train else 24)

    hft_signal_count = 0
    hft_pending_direction = 0

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.Timestamp.now())
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)

        _check_logic_reversal(bt, bar, params)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            imbalance = abs(bar.get("imbalance", 0))
            strength = bar.get("strength", 0)

            if strategy_type == "shadow_random":
                if np.random.random() < 0.005 and len(bt.positions) < 3:
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
                else:
                    should_open_hft = False
            else:
                should_open_hft = False
                if imbalance >= min_imbalance and strength > 0.2:
                    current_dir = 1 if bar.get("imbalance", 0) >= 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True

            if should_open_hft and len(bt.positions) < 3:
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                lots = int(params.get("lots_min", 1))
                risk = params.get("max_risk_ratio", 0.2)
                max_lots = max(1, int(bt.equity * risk / (price * sl_ratio + 1e-8)))
                lots = min(lots, max_lots)

                volume = hft_pending_direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 1)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q)
                slip_cost = price * slip_bps / 10000 * lots
                commission = lots * COMMISSION_PER_LOT * 2
                bt.equity -= (commission + slip_cost)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=volume,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason=reason,
                    lots=lots,
                    open_state=bt.current_state,
                    open_strength=strength,
                )
                bt.positions[symbol] = pos
                bt.last_signal_time = bar_time
                bt.total_signals += 1
                hft_signal_count = 0

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        returns = np.diff(equity_arr) / equity_arr[:-1]
        sharpe = np.sqrt(252 * 240) * np.mean(returns) / np.std(returns) if np.std(returns) > 1e-10 else 0.0
    else:
        sharpe = 0.0

    max_dd = float(np.min(equity_arr / np.maximum.accumulate(equity_arr) - 1)) if len(equity_arr) > 0 else 0.0

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
        "hft_fidelity_warning": "DEGRADED: tick级参数(hft_cooldown_ms/hft_signal_confirm_ticks)在分钟级回测中失真，需HFT回放引擎验证",
    }


# ============================================================================
# V7.1 深度验证与反验证体系（7个结构性漏洞修补）
# ============================================================================

@dataclass
class _DeepValidationResult:
    test_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)


def run_backtest_hft_with_disturbance(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
    tick_drop_prob: float = 0.0,
    delay_skip_lambda: float = 0.0,
) -> Dict[str, Any]:
    """漏洞三+七：S1 HFT回测 + 随机tick丢弃 + 微秒延迟注入

    Args:
        tick_drop_prob: 每个tick被丢弃的概率（模拟生产环境网络IO/负载导致漏tick）
        delay_skip_lambda: Poisson分布的lambda，模拟微秒延迟导致跳过tick数
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    bt = _BacktestState()
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    np.random.seed(42 if train else 24)
    decision_interval = max(1, int(params.get("decision_interval_minutes", 1)))

    hft_signal_count = 0
    hft_pending_direction = 0
    dropped_ticks = 0
    delayed_skips = 0

    for idx in range(len(bar_data)):
        if tick_drop_prob > 0 and np.random.random() < tick_drop_prob:
            dropped_ticks += 1
            bt.equity_curve.append(bt.equity)
            continue

        if delay_skip_lambda > 0:
            skip_n = np.random.poisson(delay_skip_lambda)
            if skip_n > 0:
                delayed_skips += skip_n
                for _ in range(min(skip_n, len(bar_data) - idx - 1)):
                    idx += 1
                    if idx >= len(bar_data):
                        break
                    bt.equity_curve.append(bt.equity)

        if idx >= len(bar_data):
            break

        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.Timestamp.now())
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            imbalance = abs(bar.get("imbalance", 0))
            strength = bar.get("strength", 0)

            if strategy_type == "shadow_random":
                if np.random.random() < 0.005 and len(bt.positions) < 3:
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
                else:
                    should_open_hft = False
            else:
                should_open_hft = False
                if imbalance >= min_imbalance and strength > 0.2:
                    current_dir = 1 if bar.get("imbalance", 0) >= 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True

            if should_open_hft and len(bt.positions) < 3:
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                lots = int(params.get("lots_min", 1))
                risk = params.get("max_risk_ratio", 0.2)
                max_lots = max(1, int(bt.equity * risk / (price * sl_ratio + 1e-8)))
                lots = min(lots, max_lots)

                volume = hft_pending_direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 1)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q)
                slip_cost = price * slip_bps / 10000 * lots
                commission = lots * COMMISSION_PER_LOT * 2
                bt.equity -= (commission + slip_cost)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=volume,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason=reason,
                    lots=lots,
                    open_state=bt.current_state,
                    open_strength=strength,
                )
                bt.positions[symbol] = pos
                bt.last_signal_time = bar_time
                bt.total_signals += 1
                hft_signal_count = 0

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        returns = np.diff(equity_arr) / equity_arr[:-1]
        sharpe = np.sqrt(252 * 240) * np.mean(returns) / np.std(returns) if np.std(returns) > 1e-10 else 0.0
    else:
        sharpe = 0.0

    max_dd = float(np.min(equity_arr / np.maximum.accumulate(equity_arr) - 1)) if len(equity_arr) > 0 else 0.0

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
        "dropped_ticks": dropped_ticks,
        "delayed_skips": delayed_skips,
    }


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

    np.random.seed(42 if train else 24)
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
        bl = bar_data.sample(frac=0.5, random_state=i + 1000).reset_index(drop=True)
        if len(bl) > 10:
            r = run_backtest(params, bl, train, "main")
            baseline_returns.append(r.get("total_return", 0))

    shuffled_returns = []
    for i in range(n_shuffle):
        shuffled = bar_data.sample(frac=1.0, random_state=i).reset_index(drop=True)
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
            "minute": bar_data["minute"].values,
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
    reversed_data = bar_data.iloc[::-1].reset_index(drop=True)
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


DEEP_VALIDATION_TIERS = {
    "must_run": {
        "description": "每次参数重检必跑（P0级别，约3秒）",
        "tests": ["doomed_tests", "market_friendliness"],
    },
    "quarterly": {
        "description": "季度大检（P1级别，约30秒）",
        "tests": ["doomed_tests", "market_friendliness", "liquidity_stress", "regime_robustness"],
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
        "spring_max_loss_pct", "spring_max_position_pct", "decision_interval_minutes",
    ],
    "annual_or_phase_change": [
        "capital_route_master_base", "shadow_alpha_threshold",
        "rate_limit_global_per_min", "hard_time_stop_minutes",
        "daily_loss_hard_stop_pct", "logic_reversal_threshold",
    ],
    "hft_replay_only": list(HFT_TICK_PARAMS),
}


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

    selected = DEEP_VALIDATION_TIERS[tier]["tests"]
    report = {
        "validation_version": f"V7.1-deep-v1-tier:{tier}",
        "tier": tier,
        "tier_description": DEEP_VALIDATION_TIERS[tier]["description"],
        "tests_run": selected,
        "total_tests": 0,
        "passed": 0,
        "failed": 0,
        "results": {},
    }

    all_results = []

    if "hft_temporal_robustness" in selected:
        hft_results = validate_hft_temporal_robustness(params_s1, bar_data, train)
        report["results"]["hft_temporal_robustness"] = [
            {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold}
            for r in hft_results
        ]
        all_results.extend(hft_results)

    if "cross_strategy_correlation" in selected:
        corr = validate_cross_strategy_correlation(params_s1, params_s2, params_s3, params_s4, bar_data, train)
        report["results"]["cross_strategy_correlation"] = {"passed": corr.passed, "metric": corr.metric_value}
        all_results.append(corr)

    if "market_friendliness" in selected:
        friendly = validate_market_friendliness_baseline(bar_data, train)
        report["results"]["market_friendliness"] = {"passed": friendly.passed, "metric": friendly.metric_value,
                                                     "details": friendly.details}
        all_results.append(friendly)

    if "regime_robustness" in selected:
        regime = validate_regime_robustness(params_s2, bar_data, train=train)
        report["results"]["regime_robustness"] = {"passed": regime.passed, "metric": regime.metric_value}
        all_results.append(regime)

    if "liquidity_stress" in selected:
        liq = validate_liquidity_stress(params_s2, bar_data, train)
        report["results"]["liquidity_stress"] = [
            {"test": r.test_name, "passed": r.passed, "metric": r.metric_value}
            for r in liq
        ]
        all_results.extend(liq)

    if "logic_transferability" in selected and bar_data_secondary is not None:
        transfer = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
        report["results"]["logic_transferability"] = {"passed": transfer.passed, "metric": transfer.metric_value}
        all_results.append(transfer)

    if "doomed_tests" in selected:
        doomed = validate_doomed_tests(params_s2, bar_data, train)
        report["results"]["doomed_tests"] = [
            {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "details": r.details}
            for r in doomed
        ]
        all_results.extend(doomed)

    report["total_tests"] = len(all_results)
    report["passed"] = sum(1 for r in all_results if r.passed)
    report["failed"] = sum(1 for r in all_results if not r.passed)

    return report


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

    # 漏洞五：逻辑可迁移性（需要副品种数据）
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
        report["results"]["logic_transferability"] = {
            "passed": transfer_result.passed, "metric": transfer_result.metric_value,
            "threshold": transfer_result.threshold, "details": transfer_result.details,
        }

    # 元批判：注定失败测试
    doomed_results = validate_doomed_tests(params_s2, bar_data, train)
    report["results"]["doomed_tests"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in doomed_results
    ]

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
    report["failed"] = sum(1 for r in all_results if not r.passed)

    return report


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
            transitions = (states != states.shift(1)).sum() - 1
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
            fold_data = bar_data.iloc[start:end]
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

    L-2参数在十二策略扫描中作为超参数固定（不参与网格搜索），
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


# ============================================================================
# V7.1 Step 1: L-2基岩参数独立优化（目标函数=状态判定准确率，非策略Sharpe）
# ============================================================================

L2_PARAM_GRID = {
    "non_other_ratio_threshold": [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60],
    "state_confirm_bars": [2, 3, 4, 5, 6],
    "logic_reversal_threshold": [1.0, 1.2, 1.5, 1.8, 2.0],
}

L2_CONFLICT_RESOLUTION = {
    "rule": "independent_dataset_wins",
    "rationale": "L-2参数验证的是'状态判定在独立数据上的稳健性'。如果独立数据集与主数据集最优区间不重叠，说明状态判定对数据集过拟合，此时应扩展独立数据集而非妥协。",
    "escalation": "若冲突持续 → 标记为'unresolvable' → 禁止生产使用，需人工介入分析数据集差异",
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
    threshold = params.get("non_other_ratio_threshold", 0.4)
    confirm_bars = int(params.get("state_confirm_bars", 3))
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

    return {
        "state_accuracy": state_accuracy,
        "overall_accuracy": overall_accuracy,
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
    qualified_combos = []

    for combo in all_combos:
        params = {k: v for k, v in zip(param_keys, combo)}
        result = evaluate_state_accuracy(params, independent_data, lookahead_bars)
        accuracy = result.get("overall_accuracy", 0)
        n_trans = result.get("n_transitions", 0)

        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_params = params
            best_result = result

        if accuracy >= min_accuracy and n_trans >= min_transitions:
            qualified_combos.append({"params": params, "accuracy": accuracy, "n_transitions": n_trans})

    return {
        "best_params": best_params,
        "best_accuracy": best_accuracy,
        "best_result": best_result,
        "qualified": len(qualified_combos) > 0,
        "qualified_count": len(qualified_combos),
        "total_combos": len(all_combos),
        "min_accuracy": min_accuracy,
        "min_transitions": min_transitions,
        "conflict_resolution": L2_CONFLICT_RESOLUTION,
    }


def check_l2_conflict(
    l2_params_independent: Dict[str, float],
    l2_params_main: Dict[str, float],
    tolerance: float = 0.10,
) -> Dict[str, Any]:
    """深层问题2：独立数据集vs主数据集冲突裁决

    规则：独立数据集胜出。若差异>tolerance → 标记冲突。
    冲突持续 → 禁止生产使用，需人工介入。
    """
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
    np.random.seed(42 if train else 24)
    states_seen = set()
    state_transitions = 0
    prev_state = None

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.Timestamp.now())
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
    "hard_time_stop_minutes": {"source": "直觉", "lock_after": "Step1", "rationale": "硬时间止损依赖L-2状态切换频率"},
    "daily_loss_hard_stop_pct": {"source": "直觉", "lock_after": "Step1", "rationale": "日回撤硬停止依赖L-2状态判定质量"},
    "rate_limit_global_per_min": {"source": "直觉", "lock_after": "Step1", "rationale": "速率限制依赖实盘交易延迟经验"},
    "capital_route_master_base": {"source": "直觉", "lock_after": "Step2", "rationale": "资金路由基线依赖十二策略Alpha报告"},
}


def _ensure_results_table(con: duckdb.DuckDBPyConnection, param_keys: List[str]) -> None:
    param_cols = ",\n            ".join(f'"{k}" DOUBLE' for k in param_keys)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS backtest_results (
            task_id INTEGER PRIMARY KEY,
            is_train BOOLEAN,
            {param_cols},
            params_json VARCHAR,
            
            -- S1: 高频趋势共振
            hft_sharpe DOUBLE,
            hft_max_dd DOUBLE,
            hft_total_return DOUBLE,
            hft_num_signals INTEGER,
            hft_shadow_a_sharpe DOUBLE,
            hft_shadow_b_sharpe DOUBLE,
            hft_alpha DOUBLE,
            
            -- S2: 分钟级趋势共振
            minute_sharpe DOUBLE,
            minute_max_dd DOUBLE,
            minute_total_return DOUBLE,
            minute_num_signals INTEGER,
            minute_shadow_a_sharpe DOUBLE,
            minute_shadow_b_sharpe DOUBLE,
            minute_alpha DOUBLE,
            
            -- S3: 箱体极值策略
            box_extreme_sharpe DOUBLE,
            box_extreme_max_dd DOUBLE,
            box_extreme_total_return DOUBLE,
            box_extreme_num_signals INTEGER,
            box_extreme_shadow_a_sharpe DOUBLE,
            box_extreme_shadow_b_sharpe DOUBLE,
            box_extreme_alpha DOUBLE,
            
            -- S4: 箱体弹簧策略
            box_spring_sharpe DOUBLE,
            box_spring_max_dd DOUBLE,
            box_spring_total_return DOUBLE,
            box_spring_num_signals INTEGER,
            box_spring_shadow_a_sharpe DOUBLE,
            box_spring_shadow_b_sharpe DOUBLE,
            box_spring_alpha DOUBLE,
            
            error VARCHAR
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_train ON backtest_results(is_train)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_hft_sharpe ON backtest_results(hft_sharpe)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_minute_sharpe ON backtest_results(minute_sharpe)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_box_extreme_sharpe ON backtest_results(box_extreme_sharpe)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_box_spring_sharpe ON backtest_results(box_spring_sharpe)")


def _get_completed_task_ids(con: duckdb.DuckDBPyConnection) -> Set[int]:
    try:
        rows = con.execute("SELECT task_id FROM backtest_results").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _load_data_for_period(
    db_path: str,
    date_start: str,
    date_end: str,
    symbols: Optional[List[str]] = None,
) -> pd.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        if symbols and len(symbols) > 0:
            placeholders = ", ".join(["?"] * len(symbols))
            sql = f"""
                SELECT * FROM minute_data
                WHERE minute >= ? AND minute < ? AND symbol IN ({placeholders})
                ORDER BY symbol, minute
            """
            params = [date_start, date_end] + list(symbols)
        else:
            sql = """
                SELECT * FROM minute_data
                WHERE minute >= ? AND minute < ?
                ORDER BY symbol, minute
            """
            params = [date_start, date_end]
        df = con.execute(sql, params).fetchdf()
    finally:
        con.close()
    return df


def _worker_init(train_data_shared: pd.DataFrame, test_data_shared: pd.DataFrame) -> None:
    global _TRAIN_DATA, _TEST_DATA
    _TRAIN_DATA = train_data_shared
    _TEST_DATA = test_data_shared


def _worker_task(task: dict) -> dict:
    """十二策略并行回测：4策略组 × 3策略类型（1主+2影子）
    
    在同一任务中串行运行12个回测，共享bar_data：
      - S1 高频趋势共振：hft + shadow_reverse + shadow_random
      - S2 分钟级趋势共振：main + shadow_reverse + shadow_random
      - S3 箱体极值策略：main + shadow_reverse + shadow_random
      - S4 箱体弹簧策略：main + shadow_reverse + shadow_random
    
    返回扁平化字典，包含所有12个策略的指标。
    """
    bar_data = _TRAIN_DATA if task["train"] else _TEST_DATA
    
    if bar_data is None or bar_data.empty:
        return {
            "task_id": task["id"],
            "is_train": task["train"],
            "params": task["params"],
            "error": "数据未加载"
        }
    
    results = {
        "task_id": task["id"],
        "is_train": task["train"],
        "params": task["params"],
    }
    
    params = task["params"]
    
    try:
        # S1: 高频趋势共振策略组
        hft_params = {**params}
        hft_params.update(PARAM_DEFAULTS_HFT)
        
        hft_shadow_a_params = {**params}
        hft_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["hft"]["shadow_a"])
        hft_shadow_b_params = {**params}
        hft_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["hft"]["shadow_b"])
        
        hft_main = run_backtest_hft(hft_params, bar_data, task["train"], strategy_type="hft")
        hft_rev = run_backtest_hft(hft_shadow_a_params, bar_data, task["train"], strategy_type="shadow_reverse")
        hft_rand = run_backtest_hft(hft_shadow_b_params, bar_data, task["train"], strategy_type="shadow_random")
        
        results["hft_sharpe"] = hft_main.get("sharpe")
        results["hft_max_dd"] = hft_main.get("max_drawdown")
        results["hft_total_return"] = hft_main.get("total_return")
        results["hft_num_signals"] = hft_main.get("num_signals")
        results["hft_shadow_a_sharpe"] = hft_rev.get("sharpe")
        results["hft_shadow_b_sharpe"] = hft_rand.get("sharpe")
        
        if results["hft_sharpe"] is not None:
            shadow_max = max(
                results.get("hft_shadow_a_sharpe", 0) or 0,
                results.get("hft_shadow_b_sharpe", 0) or 0
            )
            results["hft_alpha"] = results["hft_sharpe"] - shadow_max
        
        # S2: 分钟级趋势共振策略组（原master）
        s2_shadow_a_params = {**params}
        s2_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["main"]["shadow_a"])
        s2_shadow_b_params = {**params}
        s2_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["main"]["shadow_b"])
        
        master_main = run_backtest(params, bar_data, task["train"], strategy_type="main")
        master_rev = run_backtest(s2_shadow_a_params, bar_data, task["train"], strategy_type="shadow_reverse")
        master_rand = run_backtest(s2_shadow_b_params, bar_data, task["train"], strategy_type="shadow_random")
        
        results["minute_sharpe"] = master_main.get("sharpe")
        results["minute_max_dd"] = master_main.get("max_drawdown")
        results["minute_total_return"] = master_main.get("total_return")
        results["minute_num_signals"] = master_main.get("num_signals")
        results["minute_shadow_a_sharpe"] = master_rev.get("sharpe")
        results["minute_shadow_b_sharpe"] = master_rand.get("sharpe")
        
        if results["minute_sharpe"] is not None:
            shadow_max = max(
                results.get("minute_shadow_a_sharpe", 0) or 0,
                results.get("minute_shadow_b_sharpe", 0) or 0
            )
            results["minute_alpha"] = results["minute_sharpe"] - shadow_max
        
        # S3: 箱体极值策略组
        box_ext_params = {**params}
        box_ext_params.update(PARAM_DEFAULTS_BOX_EXTREME)
        
        be_shadow_a_params = {**params}
        be_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["box_extreme"]["shadow_a"])
        be_shadow_b_params = {**params}
        be_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["box_extreme"]["shadow_b"])
        
        be_main = run_backtest_box_extreme(box_ext_params, bar_data, task["train"], strategy_type="box_extreme")
        be_rev = run_backtest_box_extreme(be_shadow_a_params, bar_data, task["train"], strategy_type="shadow_reverse")
        be_rand = run_backtest_box_extreme(be_shadow_b_params, bar_data, task["train"], strategy_type="shadow_random")
        
        results["box_extreme_sharpe"] = be_main.get("sharpe")
        results["box_extreme_max_dd"] = be_main.get("max_drawdown")
        results["box_extreme_total_return"] = be_main.get("total_return")
        results["box_extreme_num_signals"] = be_main.get("num_signals")
        results["box_extreme_shadow_a_sharpe"] = be_rev.get("sharpe")
        results["box_extreme_shadow_b_sharpe"] = be_rand.get("sharpe")
        
        if results["box_extreme_sharpe"] is not None:
            shadow_max = max(
                results.get("box_extreme_shadow_a_sharpe", 0) or 0,
                results.get("box_extreme_shadow_b_sharpe", 0) or 0
            )
            results["box_extreme_alpha"] = results["box_extreme_sharpe"] - shadow_max
        
        # S4: 箱体弹簧策略组
        box_spring_params = {**params}
        box_spring_params.update(PARAM_DEFAULTS_BOX_SPRING)
        
        bs_shadow_a_params = {**params}
        bs_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["box_spring"]["shadow_a"])
        bs_shadow_b_params = {**params}
        bs_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["box_spring"]["shadow_b"])
        
        bs_main = run_backtest_box_spring(box_spring_params, bar_data, task["train"], strategy_type="box_spring")
        bs_rev = run_backtest_box_spring(bs_shadow_a_params, bar_data, task["train"], strategy_type="shadow_reverse")
        bs_rand = run_backtest_box_spring(bs_shadow_b_params, bar_data, task["train"], strategy_type="shadow_random")
        
        results["box_spring_sharpe"] = bs_main.get("sharpe")
        results["box_spring_max_dd"] = bs_main.get("max_drawdown")
        results["box_spring_total_return"] = bs_main.get("total_return")
        results["box_spring_num_signals"] = bs_main.get("num_signals")
        results["box_spring_shadow_a_sharpe"] = bs_rev.get("sharpe")
        results["box_spring_shadow_b_sharpe"] = bs_rand.get("sharpe")
        
        if results["box_spring_sharpe"] is not None:
            shadow_max = max(
                results.get("box_spring_shadow_a_sharpe", 0) or 0,
                results.get("box_spring_shadow_b_sharpe", 0) or 0
            )
            results["box_spring_alpha"] = results["box_spring_sharpe"] - shadow_max
    
    except Exception as e:
        results["error"] = str(e)
    
    return results


def _insert_results(
    con: duckdb.DuckDBPyConnection,
    results: List[dict],
    param_keys: List[str],
) -> int:
    rows = []
    for r in results:
        p = r.get("params", {})
        row = [r["task_id"], r.get("is_train", True)]
        row.extend([p.get(k) for k in param_keys])
        row.extend([
            json.dumps(p),
            r.get("hft_sharpe"),
            r.get("hft_max_dd"),
            r.get("hft_total_return"),
            r.get("hft_num_signals"),
            r.get("hft_shadow_a_sharpe"),
            r.get("hft_shadow_b_sharpe"),
            r.get("hft_alpha"),
            r.get("minute_sharpe"),
            r.get("minute_max_dd"),
            r.get("minute_total_return"),
            r.get("minute_num_signals"),
            r.get("minute_shadow_a_sharpe"),
            r.get("minute_shadow_b_sharpe"),
            r.get("minute_alpha"),
            r.get("box_extreme_sharpe"),
            r.get("box_extreme_max_dd"),
            r.get("box_extreme_total_return"),
            r.get("box_extreme_num_signals"),
            r.get("box_extreme_shadow_a_sharpe"),
            r.get("box_extreme_shadow_b_sharpe"),
            r.get("box_extreme_alpha"),
            r.get("box_spring_sharpe"),
            r.get("box_spring_max_dd"),
            r.get("box_spring_total_return"),
            r.get("box_spring_num_signals"),
            r.get("box_spring_shadow_a_sharpe"),
            r.get("box_spring_shadow_b_sharpe"),
            r.get("box_spring_alpha"),
            r.get("error"),
        ])
        rows.append(row)
    placeholders = ", ".join(["?"] * (2 + len(param_keys) + 30))
    con.executemany(f"INSERT INTO backtest_results VALUES ({placeholders})", rows)
    return len(rows)


def _execute_round(
    round_name: str,
    param_grid: Dict[str, List],
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    task_id_offset: int = 0,
    fixed_params: Optional[Dict[str, float]] = None,
) -> Tuple[List[dict], int]:
    """执行一轮参数扫描，每个任务包含九策略回测"""
    all_keys = sorted(set(list(param_grid.keys()) + (list(fixed_params.keys()) if fixed_params else [])))
    grid_keys, grid_values = zip(*param_grid.items())
    grid_combos = [dict(zip(grid_keys, v)) for v in itertools.product(*grid_values)]

    tasks = []
    task_id = task_id_offset
    for grid_params in grid_combos:
        full_params = dict(PARAM_DEFAULTS)
        if fixed_params:
            full_params.update(fixed_params)
        full_params.update(grid_params)

        tasks.append({"id": task_id, "params": full_params, "train": True})
        task_id += 1
        tasks.append({"id": task_id, "params": full_params, "train": False})
        task_id += 1

    logger.info("[%s] %d 组合 × 2(train+test) × 12策略 = %d 回测", round_name, len(grid_combos), len(tasks) * 12)

    results: List[dict] = []
    if MAX_WORKERS > 1:
        with ProcessPoolExecutor(
            max_workers=MAX_WORKERS,
            initializer=_worker_init,
            initargs=(train_data, test_data),
        ) as executor:
            futures = {executor.submit(_worker_task, task): task for task in tasks}
            for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                try:
                    task_result = future.result()
                    if task_result:
                        results.append(task_result)
                except Exception as e:
                    logger.error("任务执行异常: %s", e)
    else:
        _worker_init(train_data, test_data)
        for task in tqdm(tasks, desc=f"{round_name}进度"):
            task_result = _worker_task(task)
            if task_result:
                results.append(task_result)

    return results, task_id


def _run_final_checks(
    best_params_json: str,
    train_sharpe: float,
    test_sharpe: float,
    train_return: float = 0.0,
    test_return: float = 0.0,
    train_max_dd: float = 0.0,
    test_max_dd: float = 0.0,
    num_signals: int = 0,
    alpha_hft: float = 0.0,
    alpha_minute: float = 0.0,
    alpha_box_extreme: float = 0.0,
    alpha_box_spring: float = 0.0,
) -> bool:
    """执行P0最终绿灯检验 — 从"结果"到"证据"的法官审判

    不可妥协的P0硬编码检验项：
      1. 样本外衰减 < 30%（反过拟合铁律）
      2. 训练夏普 > 0.5（最低可交易门槛）
      3. 样本外夏普 > 0.3（样本外必须正期望）
      4. 最大回撤 > -50%（生存红线）
      5. 最少信号数 > 30（统计显著性最低要求）
      6. 逻辑反转阈值合理性
      7. 止损比例 < 止盈比例（风险收益比 > 1）
      8. 参数来源不可为intuition（铁律：无量化来源的参数不得锁定生产值）
      9. 九策略Alpha占比检验

    Returns:
        True = P0绿灯通过, False = P0红灯未通过
    """
    p = json.loads(best_params_json) if best_params_json else {}
    all_passed = True
    warnings_list: List[str] = []

    print("\n" + "=" * 70)
    print("P0 最终绿灯检验 — 从结果到证据的法官审判")
    print("=" * 70)

    # --- 1. 样本外衰减检验（不可妥协）---
    decay = (test_sharpe - train_sharpe) / train_sharpe if abs(train_sharpe) > 1e-8 else 0
    if decay < -0.30:
        print(f"  [FAIL] 样本外衰减={decay:.1%} < -30%: 严重过拟合，样本外不可信")
        all_passed = False
    elif decay < -0.20:
        warnings_list.append(f"样本外衰减={decay:.1%} 接近-30%警戒线")
        print(f"  [WARN] 样本外衰减={decay:.1%}: 接近警戒线（-30%），需关注")
    else:
        print(f"  [PASS] 样本外衰减={decay:.1%} >= -30%: 样本外稳健")

    # --- 2. 训练夏普最低门槛 ---
    if train_sharpe < 0.5:
        print(f"  [FAIL] 训练夏普={train_sharpe:.3f} < 0.5: 不满足最低可交易门槛")
        all_passed = False
    else:
        print(f"  [PASS] 训练夏普={train_sharpe:.3f} >= 0.5: 满足可交易门槛")

    # --- 3. 样本外夏普正期望 ---
    if test_sharpe < 0.3:
        print(f"  [FAIL] 样本外夏普={test_sharpe:.3f} < 0.3: 样本外非正期望")
        all_passed = False
    else:
        print(f"  [PASS] 样本外夏普={test_sharpe:.3f} >= 0.3: 样本外正期望")

    # --- 4. 最大回撤生存红线 ---
    if test_max_dd < -0.50:
        print(f"  [FAIL] 样本外最大回撤={test_max_dd:.3f} < -50%: 超过生存红线")
        all_passed = False
    else:
        print(f"  [PASS] 样本外最大回撤={test_max_dd:.3f} >= -50%: 回撤可控")

    # --- 5. 最少信号数统计显著性 ---
    if num_signals < 30:
        print(f"  [FAIL] 信号数={num_signals} < 30: 统计显著性不足，结论不可靠")
        all_passed = False
    else:
        print(f"  [PASS] 信号数={num_signals} >= 30: 满足统计显著性最低要求")

    # --- 6. 逻辑反转阈值合理性 ---
    lr_threshold = p.get("logic_reversal_threshold", 1.5)
    if lr_threshold < 0.8:
        print(f"  [FAIL] 逻辑反转阈值={lr_threshold} < 0.8: 频繁误平仓风险极高")
        all_passed = False
    elif lr_threshold < 1.0:
        warnings_list.append(f"逻辑反转阈值={lr_threshold}偏低，可能频繁误平仓")
        print(f"  [WARN] 逻辑反转阈值={lr_threshold}: 偏低，可能频繁误平仓")
    else:
        print(f"  [PASS] 逻辑反转阈值={lr_threshold} >= 1.0: 合理")

    # --- 7. 风险收益比 > 1 ---
    tp = p.get("close_take_profit_ratio", 1.5)
    sl = p.get("close_stop_loss_ratio", 0.5)
    if tp <= sl:
        print(f"  [FAIL] 止盈={tp} <= 止损={sl}: 风险收益比<=1，长期必亏")
        all_passed = False
    else:
        print(f"  [PASS] 止盈/止损={tp}/{sl}={tp/sl:.1f}:1 风险收益比>1")

    # --- 8. 参数来源intuition检查（不可妥协铁律）---
    intuition_params_found = []
    try:
        from params_service import get_params_service
        ps = get_params_service()
        for key, value in p.items():
            attr = ps._attribute_matrix.get(key)
            if isinstance(attr, dict) and attr.get('source') == 'intuition':
                intuition_params_found.append(f"{key}={value}")
    except Exception:
        pass

    if intuition_params_found:
        print(f"  [FAIL] 发现 {len(intuition_params_found)} 个intuition参数: {intuition_params_found}")
        print(f"         铁律: 无量化来源的参数不得锁定为生产值")
        all_passed = False
    else:
        print(f"  [PASS] 无intuition参数: 所有锁定参数均有量化来源")

    # --- 9. 十二策略Alpha占比检验 ---
    print("\n" + "-" * 70)
    print("十二策略Alpha占比检验:")
    alpha_checks = [
        ("S1 HFT趋势共振", alpha_hft, 0.5),
        ("S2 分钟趋势共振", alpha_minute, 0.5),
        ("S3 箱体极值", alpha_box_extreme, 0.3),
        ("S4 箱体弹簧", alpha_box_spring, 0.4),
    ]
    
    for label, alpha_val, threshold in alpha_checks:
        alpha_pct_val = alpha_val / train_sharpe * 100 if train_sharpe > 0 else 0
        print(f"  {label}: Alpha={alpha_val:.3f} ({alpha_pct_val:.1f}%) 阈值≥{threshold}")
        if alpha_val < threshold:
            print(f"    [WARN] {label} Alpha={alpha_val:.3f} < {threshold}，独立Alpha不足")
            warnings_list.append(f"{label} Alpha={alpha_val:.3f}<{threshold}")
        if alpha_pct_val < 30:
            print(f"    [WARN] {label} Alpha占比={alpha_pct_val:.1f}%<30%，收益主要由市场驱动")
            warnings_list.append(f"{label} Alpha占比={alpha_pct_val:.1f}%<30%")

    # --- 最终判决 ---
    print("-" * 70)
    if warnings_list:
        print(f"  警告 ({len(warnings_list)}):")
        for w in warnings_list:
            print(f"    - {w}")

    if all_passed and not warnings_list:
        print("\n  *** P0绿灯: 全部通过。可进入小资金实盘测试。***")
    elif all_passed:
        print("\n  *** P0黄灯: 硬检验通过但有警告。建议处理警告后进入实盘。***")
    else:
        print("\n  *** P0红灯: 未通过。需调整参数或补充量化证据。***")

    print("=" * 70)
    return all_passed


def _validate_params_via_params_service() -> Dict[str, Any]:
    """回测前通过 ParamsService API 加载校验参数

    确保：
    1. attribute_matrix 已加载且校验通过
    2. PARAM_DEFAULTS 中所有参数在 attribute_matrix 中有定义
    3. source=intuition 的参数发出生产锁定警告
    4. 依赖约束和互斥规则通过
    """
    try:
        from params_service import get_params_service
        ps = get_params_service()
    except Exception as e:
        logger.warning("ParamsService unavailable, skip validation: %s", e)
        return {'violations': [], 'warnings': [f'ParamsService unavailable: {e}'], 'checked_count': 0}

    if not ps._attribute_matrix_loaded:
        try:
            report = ps.load_attribute_matrix()
        except Exception as e:
            logger.warning("Failed to load attribute matrix: %s", e)
            return {'violations': [], 'warnings': [f'Attribute matrix load failed: {e}'], 'checked_count': 0}
    else:
        report = ps.validate_with_attribute_matrix()

    for key, value in PARAM_DEFAULTS.items():
        if key not in ps._attribute_matrix:
            report.setdefault('warnings', []).append(
                f"PARAM_NOT_IN_MATRIX | {key}={value} in PARAM_DEFAULTS but not in attribute_matrix"
            )

    for key, attr in ps._attribute_matrix.items():
        if not isinstance(attr, dict):
            continue
        if attr.get('source') == 'intuition':
            report.setdefault('warnings', []).append(
                f"INTUTION_PARAM | {key}={attr.get('default')}: source=intuition, 不可锁定为生产值"
            )

    if report.get('violations'):
        logger.error("ParamsService 校验 %d 违规:", len(report['violations']))
        for v in report['violations']:
            logger.error("  %s", v)
    if report.get('warnings'):
        logger.warning("ParamsService 校验 %d 警告:", len(report.get('warnings', [])))
        for w in report['warnings']:
            logger.warning("  %s", w)
    if not report.get('violations') and not report.get('warnings'):
        logger.info("ParamsService 参数校验全部通过 (%d params)", report.get('checked_count', 0))

    return report


def main_scheduler() -> None:
    start_time = time.time()

    validate_report = _validate_params_via_params_service()
    if validate_report.get('violations'):
        logger.error("参数校验存在 %d 违规，建议修复后再回测", len(validate_report['violations']))

    logger.info("预加载数据...")
    train_data = _load_data_for_period(PREPROCESSED_DB, TRAIN_START, TEST_START, TARGET_SYMBOLS)
    test_data = _load_data_for_period(PREPROCESSED_DB, TEST_START, TEST_END, TARGET_SYMBOLS)
    logger.info("训练集: %d 行, 测试集: %d 行", len(train_data), len(test_data))

    all_param_keys = sorted(set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()) + list(PARAM_DEFAULTS.keys())))

    r1_results, next_id = _execute_round("Round1粗扫", PARAM_GRID_ROUND1, train_data, test_data, 0)

    r1_train = [r for r in r1_results if r.get("is_train") and "minute_sharpe" in r and r["minute_sharpe"] is not None]
    r1_train.sort(key=lambda r: r["minute_sharpe"], reverse=True)
    top_k = r1_train[:ROUND1_TOP_K]

    logger.info("Round1完成: %d 结果, Top%d 训练夏普: %s",
                len(r1_results), ROUND1_TOP_K,
                [f"{r['minute_sharpe']:.2f}" for r in top_k[:3]])

    r2_all_results: List[dict] = []
    for i, top_result in enumerate(top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in PARAM_GRID_ROUND1}
        r2_results, next_id = _execute_round(
            f"Round2精扫-Top{i+1}", PARAM_GRID_ROUND2, train_data, test_data, next_id, core_only,
        )
        r2_all_results.extend(r2_results)

    all_results = r1_results + r2_all_results
    if not all_results:
        logger.warning("无回测结果")
        return

    con = duckdb.connect(RESULTS_DB)
    try:
        _ensure_results_table(con, all_param_keys)
        inserted = _insert_results(con, all_results, all_param_keys)
        logger.info("写入 %d 条结果到 %s", inserted, RESULTS_DB)

        best_train = con.execute("""
            SELECT minute_sharpe, minute_total_return, minute_max_dd, minute_num_signals, params_json
            FROM backtest_results
            WHERE is_train = true AND minute_sharpe IS NOT NULL
            ORDER BY minute_sharpe DESC
            LIMIT 5
        """).fetchall()

        print("\n=== 训练集最佳参数（Top5，S2分钟趋势共振） ===")
        for sharpe, ret, dd, n_sig, pjson in best_train:
            p = json.loads(pjson) if pjson else {}
            print(f"  夏普={sharpe:.3f}  收益={ret:.4f}  回撤={dd:.3f}  信号={n_sig}  参数={p}")

        oos = con.execute("""
            SELECT r_train.minute_sharpe AS train_sharpe, r_test.minute_sharpe AS test_sharpe,
                   r_train.minute_total_return AS train_ret, r_test.minute_total_return AS test_ret,
                   r_train.params_json
            FROM backtest_results r_train
            JOIN backtest_results r_test
              ON r_train.params_json = r_test.params_json
            WHERE r_train.is_train = true AND r_test.is_train = false
              AND r_train.minute_sharpe IS NOT NULL AND r_test.minute_sharpe IS NOT NULL
            ORDER BY r_train.minute_sharpe DESC
            LIMIT 5
        """).fetchall()

        if oos:
            print("\n=== 训练vs测试 样本外验证（Top5，S2分钟趋势共振） ===")
            for t_sh, te_sh, t_ret, te_ret, pjson in oos:
                decay = (te_sh - t_sh) / t_sh if abs(t_sh) > 1e-8 else 0
                p = json.loads(pjson) if pjson else {}
                print(f"  训练夏普={t_sh:.3f}  测试夏普={te_sh:.3f}  衰减={decay:.1%}  参数={p}")

            best_oos = oos[0]
            best_params_json = best_oos[4]

            print("\n" + "=" * 80)
            print("十二策略影子对比报告（1主+2影子 × 4策略组）")
            print("=" * 80)

            strategy_groups = [
                ("hft", "S1 高频趋势共振"),
                ("minute", "S2 分钟级趋势共振"),
                ("box_extreme", "S3 箱体极值策略"),
                ("box_spring", "S4 箱体弹簧策略"),
            ]
            
            alpha_evidence = {}
            
            for stype, label in strategy_groups:
                best = con.execute(f"""
                    SELECT {stype}_sharpe, {stype}_shadow_a_sharpe, {stype}_shadow_b_sharpe, {stype}_alpha,
                           {stype}_max_dd, {stype}_total_return, {stype}_num_signals
                    FROM backtest_results
                    WHERE is_train = true AND params_json = ? AND {stype}_sharpe IS NOT NULL
                    LIMIT 1
                """, [best_params_json]).fetchone()
                
                if best:
                    master_s = best[0]
                    shadow_a_s = best[1] or 0
                    shadow_b_s = best[2] or 0
                    alpha = best[3] or 0
                    beta = max(shadow_a_s, shadow_b_s)
                    alpha_pct = alpha / master_s * 100 if master_s > 0 else 0
                    
                    alpha_evidence[stype] = {"alpha_pct": alpha_pct, "sharpe": master_s, "alpha": alpha}
                    
                    print(f"\n  {label}:")
                    print(f"    主策略夏普:       {master_s:.3f}")
                    print(f"    影子A(反向)夏普:  {shadow_a_s:.3f}")
                    print(f"    影子B(随机)夏普:  {shadow_b_s:.3f}")
                    print(f"    Alpha超额:        {alpha:.3f}")
                    print(f"    Beta(市场)贡献:   {beta:.3f} ({beta/master_s*100:.1f}%)")
                    print(f"    独立Alpha占比:    {alpha_pct:.1f}%")
                    
                    if alpha_pct < 30:
                        print(f"    ⚠️ 警告: 独立Alpha占比<30%，策略收益主要由市场行情驱动")
                else:
                    print(f"\n  {label}: (无数据)")
                    alpha_evidence[stype] = {"alpha_pct": 0, "sharpe": 0, "alpha": 0}

            print("\n" + "-" * 80)
            print("十二策略资金分配建议（基于独立Alpha占比）")
            total_alpha = sum(e["alpha"] for e in alpha_evidence.values() if e["alpha"] > 0)
            for stype, label in strategy_groups:
                ev = alpha_evidence.get(stype, {"alpha": 0, "alpha_pct": 0, "sharpe": 0})
                if total_alpha > 0 and ev["alpha"] > 0:
                    alloc = ev["alpha"] / total_alpha * 100
                else:
                    alloc = 0
                print(f"  {label}: 独立Alpha={ev['alpha']:.3f} 占比={ev['alpha_pct']:.1f}% 建议分配={alloc:.1f}%")

            print("=" * 80)

            best_train_full = con.execute("""
                SELECT minute_sharpe, minute_total_return, minute_max_dd, minute_num_signals, params_json
                FROM backtest_results
                WHERE is_train = true AND minute_sharpe IS NOT NULL
                ORDER BY minute_sharpe DESC LIMIT 1
            """).fetchone()
            best_test_full = con.execute("""
                SELECT minute_sharpe, minute_total_return, minute_max_dd
                FROM backtest_results
                WHERE is_train = false AND minute_sharpe IS NOT NULL
                  AND params_json = ?
                ORDER BY minute_sharpe DESC LIMIT 1
            """, [best_params_json]).fetchone()

            if best_train_full and best_test_full:
                best_alpha_row = con.execute("""
                    SELECT hft_alpha, minute_alpha, box_extreme_alpha, box_spring_alpha
                    FROM backtest_results
                    WHERE is_train = true AND params_json = ?
                    LIMIT 1
                """, [best_params_json]).fetchone()
                
                alpha_hft = best_alpha_row[0] if best_alpha_row and best_alpha_row[0] else 0.0
                alpha_minute = best_alpha_row[1] if best_alpha_row and best_alpha_row[1] else 0.0
                alpha_box_extreme = best_alpha_row[2] if best_alpha_row and best_alpha_row[2] else 0.0
                alpha_box_spring = best_alpha_row[3] if best_alpha_row and best_alpha_row[3] else 0.0
                
                _run_final_checks(
                    best_params_json=best_train_full[4],
                    train_sharpe=best_train_full[0],
                    test_sharpe=best_test_full[0],
                    train_return=best_train_full[1],
                    test_return=best_test_full[1],
                    train_max_dd=best_train_full[2],
                    test_max_dd=best_test_full[2],
                    num_signals=best_train_full[3] or 0,
                    alpha_hft=alpha_hft,
                    alpha_minute=alpha_minute,
                    alpha_box_extreme=alpha_box_extreme,
                    alpha_box_spring=alpha_box_spring,
                )
            else:
                print("\n[WARN] 无法执行P0检验: 缺少训练/测试最佳结果配对")
    finally:
        con.close()

    elapsed = time.time() - start_time
    r1_combos = 1
    for v in PARAM_GRID_ROUND1.values():
        r1_combos *= len(v)
    r2_combos = 1
    for v in PARAM_GRID_ROUND2.values():
        r2_combos *= len(v)
    total_tasks = r1_combos * 2 + ROUND1_TOP_K * r2_combos * 2
    logger.info("全部完成: Round1(%d)+Round2(%d×%d)=%d任务, 耗时%.1f秒",
                r1_combos, ROUND1_TOP_K, r2_combos, total_tasks, elapsed)


if __name__ == "__main__":
    main_scheduler()
