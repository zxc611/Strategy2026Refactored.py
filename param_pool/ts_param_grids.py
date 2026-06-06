#!/usr/bin/env python3
"""ts_param_grids — 参数网格定义子模块 (R27-CP-08-FIX: 从task_scheduler.py拆分)

包含:
  - RISK_FREE_RATE: 无风险利率
  - BACKTEST_THRESHOLDS: 回测阈值常量
  - MULTISCALE_BAR_LENGTHS: 多粒度Bar长度列表
  - BAR_INTERVAL_GRID: K线间隔策略适配集
  - KLINE_LENGTH_PARAM_GRID: K线长度参数网格(22键)
  - _SUBPROCESS_NEEDED_COLS: 子进程最小列集合
  - Re-export: P0_IRON_RULES, REASON_MULTIPLIERS, detect_rollover_gaps, compute_rollover_cost
"""
from __future__ import annotations

# R27-CP-01-FIX: 常量已提取到共享模块，此处re-export保持向后兼容
from ali2026v3_trading.shared_trading_constants import (  # noqa: F401
    P0_IRON_RULES,
    REASON_MULTIPLIERS,
    detect_rollover_gaps,
    compute_rollover_cost,
)

RISK_FREE_RATE = 0.02

# R10-P2-06修复: BACKTEST_THRESHOLDS唯一权威定义源
BACKTEST_THRESHOLDS = {
    "logic_reversal_min_wrong_pct": 0.3,
    "correct_resonance_min_strength": 0.3,
    "circuit_breaker_daily_dd": 0.03,
    "main_open_min_strength": 0.3,
    "shadow_random_open_prob": 0.02,
    "hft_shadow_random_open_prob": 0.005,
    "hft_open_min_strength": 0.2,
    "mm_rebalance_imbalance": 0.3,
    "hft_fidelity_sharpe_ratio": 0.5,
    "liquidity_stress_max_dd": 0.3,
    "gbm_max_return": 0.05,
    "reverse_max_return": 0.0,
    "ci_width_eliminate": 2.0,
    "ci_width_reduce": 1.0,
    "ci_width_flag": 0.5,
    "kl_q1_sharpe_degradation": 0.5,
    "kl_q1_min_sharpe": 0.01,
    "kl_q3_max_trade_diff": 0.5,
    "min_oos_retention": 0.50,
    "alpha_threshold_hft": 0.5,
    "alpha_threshold_minute": 0.5,
    "alpha_threshold_box_extreme": 0.3,
    "alpha_threshold_box_spring": 0.4,
    "alpha_threshold_arbitrage": 0.3,
    "alpha_threshold_market_making": 0.2,
    "alpha_pct_threshold": 30,
}


# ======================================================================
# K线长度回测基础设施 — 多粒度Bar加载 + HFT tick插值回放
# ======================================================================

MULTISCALE_BAR_LENGTHS = [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440]


# P-18修复注释: BAR_INTERVAL_GRID策略适配集已扩展至6策略(原手册仅4策略)，S5/S6为后续新增
BAR_INTERVAL_GRID = {
    "high_freq": [1],
    "resonance": [1, 2, 3, 5, 10, 15, 30],
    "box": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "spring": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "arbitrage": [1, 2, 3, 5],
    "market_making": [1, 2, 3, 5, 10, 15],
}

# C-27/P1-40修复注释: KLINE_LENGTH_PARAM_GRID精确22键(原V7手册13维)
# 因S1-S6策略参数扩展新增9键: adx_period/box_min_bars/state_confirm_bars/
# bar_interval_sec_production/min_tick_volume_threshold/max_intra_bar_ticks/
# box_breakout_confirm_bars/spring_charge_confirm_bars/spring_release_confirm_bars/
# hft_cooldown_ticks/trend_score_ema_alpha/hmm_train_min_ticks/kline_snr_window
# 精确维度: bar_interval_minutes(11) + trend_period_short(5) + trend_period_medium(5) + trend_period_long(5)
# + adx_period(5) + box_lookback_bars(6) + box_min_bars(6) + state_confirm_bars(3)
# + hft_signal_confirm_ticks(5) + hft_ticks_per_bar(7) + vol_lookback(6) + iv_lookback_bars(6)
# + bar_interval_sec_production(5) + min_tick_volume_threshold(5) + max_intra_bar_ticks(6)
# + box_breakout_confirm_bars(4) + spring_charge_confirm_bars(5) + spring_release_confirm_bars(4)
# + hft_cooldown_ticks(5) + trend_score_ema_alpha(5) + hmm_train_min_ticks(5) + kline_snr_window(4) = 22键
KLINE_LENGTH_PARAM_GRID = {
    "bar_interval_minutes": [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440],
    "trend_period_short": [2, 3, 5, 8, 13],
    "trend_period_medium": [10, 15, 20, 30, 45],
    "trend_period_long": [30, 40, 60, 90, 120],
    "adx_period": [7, 10, 14, 20, 28],
    "box_lookback_bars": [20, 30, 60, 90, 120, 180],
    "box_min_bars": [5, 10, 15, 20, 30, 45],
    "state_confirm_bars": [3, 5, 8],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    "hft_signal_confirm_ticks": [2, 3, 5, 8, 13],
    "hft_ticks_per_bar": [2, 3, 5, 8, 10, 15, 20],
    "vol_lookback": [20, 50, 100, 150, 200, 300],
    "iv_lookback_bars": [30, 60, 90, 120, 180, 240],
    "bar_interval_sec_production": [60, 120, 180, 300, 600],
    "min_tick_volume_threshold": [0, 10, 50, 100, 500],
    "max_intra_bar_ticks": [3, 5, 10, 15, 20, 30],
    "box_breakout_confirm_bars": [1, 2, 3, 5],
    "spring_charge_confirm_bars": [2, 3, 5, 8, 13],
    "spring_release_confirm_bars": [1, 2, 3, 5],
    "hft_cooldown_ticks": [1, 3, 5, 10, 20],
    "trend_score_ema_alpha": [0.05, 0.1, 0.15, 0.2, 0.3],
    "hmm_train_min_ticks": [50, 100, 200, 500, 1000],
    "kline_snr_window": [10, 20, 50, 100],
}


# R10-P0-17修复: 子进程所需的最小列集合，避免传递完整DataFrame导致内存膨胀
_SUBPROCESS_NEEDED_COLS = [
    "minute", "symbol", "close", "open", "high", "low", "volume",
    "strength", "imbalance", "bid_ask_spread", "_spread_quality",
    "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
    "days_to_expiry", "expiry_date", "timestamp", "datetime",
    "iv", "tick_size_bps", "avg_trade_size",
]
