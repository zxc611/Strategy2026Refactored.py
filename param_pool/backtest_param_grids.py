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

import copy
import itertools
import math
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

PULLBACK_DEFAULTS = {
    "pullback_enabled": False,
    "pullback_wait_bars": 5,
    "pullback_retrace_pct": 0.15,
    "pullback_iv_min_percentile": 20.0,
    "pullback_iv_max_percentile": 80.0,
    "pullback_ref_mode": "peak",
    "pullback_atr_wait_multiplier": 0.0,
    "pullback_retrace_pct_call": 0.38,  # P2-4修复: Call回撤默认值，与shared_utils.py对齐
    "pullback_retrace_pct_put": 0.42,  # P2-4修复: Put回撤默认值，与shared_utils.py对齐
    "pullback_theta_decay_accel": 0.0,
    "pullback_min_retrace_abs": 0.0,
    "pullback_max_valid_bars": 24,
}

PULLBACK_GRID = {
    # P2-R8-10修复: 与enhanced_phase_scan.py PULLBACK_FULL_GRID对齐
    "pullback_enabled": [True, False],
    "pullback_wait_bars": [2, 4, 6, 8, 10],
    "pullback_retrace_pct": [0.15, 0.30, 0.45, 0.60, 0.75, 0.90],
    "pullback_ref_mode": ["peak", "atr"],
    "pullback_atr_wait_multiplier": [0.0, 1.0, 2.0, 3.5, 5.0],
    "pullback_theta_decay_accel": [0.0, 0.3, 0.5, 0.8, 1.0],
    "pullback_min_retrace_abs": [0.0, 0.3, 0.5, 0.8, 1.0, 1.5],
    "pullback_max_valid_bars": [8, 16, 24, 32, 40, 50],
    "pullback_iv_min_percentile": [5.0, 10.0, 20.0, 30.0, 40.0],
    "pullback_iv_max_percentile": [60.0, 70.0, 80.0, 90.0, 95.0],
    # Call/Put特定回撤比例(仅task_scheduler使用，enhanced_phase_scan不含)
    "pullback_retrace_pct_call": [0.1, 0.15, 0.2, 0.3, 0.38],
    "pullback_retrace_pct_put": [0.1, 0.15, 0.2, 0.3, 0.42],
}

# ── 11维度统一风险评分参数 ──────────────────────────────
# 三层架构：核心信号层(0.45) + 市场状态层(0.30) + 组合风控层(0.25)
# 权重总和=1.0，可通过params覆盖(RiskService读取decision.score.{dim}_weight)
# P1-8修复: 从risk_service.RiskService动态导入并转换键名，保持单一数据源


def _get_risk_dimension_defaults():
    from ali2026v3_trading.param_pool.backtest_runner_base import _build_risk_dimension_defaults
    return _build_risk_dimension_defaults()

try:
    RISK_DIMENSION_DEFAULTS = _get_risk_dimension_defaults()
except Exception:
    RISK_DIMENSION_DEFAULTS = {}
RISK_DIMENSION_DEFAULTS.setdefault("decision.score.threshold_high", 0.70)
RISK_DIMENSION_DEFAULTS.setdefault("decision.score.threshold_low", 0.50)

RISK_DIMENSION_GRID = {
    # 核心信号层权重扫描
    "decision.score.state_strength_weight": [0.10, 0.15, 0.20],
    "decision.score.order_flow_weight": [0.05, 0.10, 0.15],
    "decision.score.cycle_resonance_weight": [0.05, 0.10, 0.15],
    "decision.score.tri_validation_weight": [0.05, 0.10, 0.15],
    # 市场状态层权重扫描
    "decision.score.life_expectancy_weight": [0.05, 0.10, 0.15],
    "decision.score.phase_quality_weight": [0.04, 0.08, 0.12],
    "decision.score.greeks_usage_weight": [0.03, 0.07, 0.10],
    "decision.score.asymmetric_drawdown_weight": [0.03, 0.05, 0.08],
    # 组合风控层权重扫描
    "decision.score.consecutive_loss_weight": [0.03, 0.07, 0.10],
    "decision.score.alpha_decay_weight": [0.05, 0.10, 0.15],
    "decision.score.cross_correlation_weight": [0.04, 0.08, 0.12],
    # 决策阈值扫描
    "decision.score.threshold_high": [0.60, 0.70, 0.80],
    "decision.score.threshold_low": [0.40, 0.50, 0.60],  # R13-三对齐修复: 中值0.50与signal_service对齐
    # P1-GR-002修复: 希腊约束阈值纳入扫描网格
    "max_net_delta_pct": [0.10, 0.20, 0.30, 0.40, 0.50],
    "max_net_gamma_pct": [0.03, 0.05, 0.08, 0.12, 0.20],
    "max_net_vega_bps": [0.005, 0.01, 0.02, 0.03, 0.05],
}

# 行情寿命估计器参数
LIFE_ESTIMATOR_DEFAULTS = {
    "life_min_sample_count": 30,           # 最小样本数(Level3降级阈值)
    "life_shrinkage_prior_strength": 1.0,  # 贝叶斯收缩先验强度
    "life_decay_r_squared_threshold": 0.3, # 衰减曲线R²阈值
}

LIFE_ESTIMATOR_GRID = {
    "life_min_sample_count": [20, 30, 50, 80],
    "life_shrinkage_prior_strength": [0.5, 1.0, 2.0],
    "life_decay_r_squared_threshold": [0.2, 0.3, 0.5],
}

# ── 期权盈亏比核心网格 — 顶级基金验证水准 ──
# 期权权利金盈亏比从0.5:1到5:1全覆盖，覆盖实盘常见状态
OPTION_TAKE_PROFIT_GRID = [0.5, 0.75, 1.0, 1.25, 1.5, 1.8, 2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0, 4.5, 5.0]  # R24-P1-DF-07修复: 追加1.8使默认值在网格中
OPTION_STOP_LOSS_GRID = [0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
# 动量策略盈亏比网格（偏右侧，止盈高止损低）
MOMENTUM_TAKE_PROFIT_GRID = [1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
MOMENTUM_STOP_LOSS_GRID = [0.2, 0.25, 0.3, 0.4, 0.5, 0.6]
# 均值回归策略盈亏比网格（偏左侧，止盈低止损高）
REVERSION_TAKE_PROFIT_GRID = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
REVERSION_STOP_LOSS_GRID = [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
# 套利策略盈亏比网格（窄幅，快速止盈止损）
ARB_TAKE_PROFIT_GRID = [0.5, 0.75, 1.0, 1.25, 1.5]
ARB_STOP_LOSS_GRID = [0.2, 0.25, 0.3, 0.35, 0.4, 0.5]
# 做市策略盈亏比网格（极窄幅，高频小利）
MM_TAKE_PROFIT_GRID = [0.4, 0.5, 0.6, 0.75, 0.8, 1.0, 1.2]
MM_STOP_LOSS_GRID = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
# 期权弹簧止盈比网格（高盈亏比，权利金杠杆特性）
SPRING_STOP_PROFIT_GRID = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0]
SPRING_MAX_LOSS_GRID = [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.98]

# ── 策略分层时间参数 ──────────────────────────────────────
# 核心原则：高频策略时间参数从毫秒起步，秒级策略从秒起步，分钟策略从分钟起步
# 用"分钟"统一度量亚秒级策略 = F1赛车油门刻度设成"每小时公里数"——丧失控制精度

# S1 HFT专用（毫秒级）— 订单簿半衰期约50-200ms
HFT_TIME_PARAMS = {
    "hft_hard_time_stop_ms": (100, 5000),       # 100ms ~ 5s，防止微观结构突变后持仓过久
    "hft_signal_confirm_ticks": (3, 15),         # tick数确认
    "hft_cooldown_ms": (10, 500),                # 10ms ~ 500ms 信号冷却
}
HFT_TIME_PARAM_GRID = {
    "hft_hard_time_stop_ms": [100, 200, 500, 1000, 2000, 5000],
    "hft_signal_confirm_ticks": [3, 5, 8, 10, 13, 15],
    "hft_cooldown_ms": [10, 30, 50, 100, 200, 500],
}
HFT_TIME_DEFAULTS = {
    "hft_hard_time_stop_ms": 1000,               # 默认1秒
    "hft_signal_confirm_ticks": 5,
    "hft_cooldown_ms": 100,
    # P1-R11-14修复: HFT策略时间基准确认(秒)，替代Bar计数确认
    # 当bar_period<=0.1(亚秒Bar)时，使用此参数计算confirm_bars
    "hft_state_confirm_seconds": 5.0,
}

# S4 弹簧专用（秒级）— Gamma峰值持续时间约10-60s
SPRING_TIME_PARAMS = {
    "spring_hard_time_stop_sec": (1, 120),       # 1s ~ 2min，防止Gamma脉冲衰减后权利金损耗
    "spring_confirm_ticks": (5, 50),             # tick数确认
    "spring_iv_pulse_window_sec": (5, 60),       # IV脉冲观察窗口
}
SPRING_TIME_PARAM_GRID = {
    "spring_hard_time_stop_sec": [1, 5, 10, 30, 60, 90, 120],
    "spring_confirm_ticks": [5, 10, 15, 20, 30, 50],
    "spring_iv_pulse_window_sec": [5, 10, 20, 30, 45, 60],
}
SPRING_TIME_DEFAULTS = {
    "spring_hard_time_stop_sec": 30,              # 默认30秒
    "spring_confirm_ticks": 10,
    "spring_iv_pulse_window_sec": 20,
}

# S2 共振专用（分钟级）— 分钟级趋势持续性约5-30分钟
RESONANCE_TIME_PARAMS = {
    "resonance_hard_time_stop_min": (1, 15),     # 1min ~ 15min，防止趋势反转后方向性暴露
    "state_confirm_bars": (2, 8),                # 2 ~ 8 Bar确认
}
RESONANCE_TIME_PARAM_GRID = {
    "resonance_hard_time_stop_min": [1, 3, 5, 8, 10, 15],
    "state_confirm_bars": [2, 3, 4, 5, 6, 8],
}
RESONANCE_TIME_DEFAULTS = {
    "resonance_hard_time_stop_min": 5,            # 默认5分钟
    "state_confirm_bars": 5,  # R24-P1-DF-01修复: 统一默认值为5
}

# S3 箱体专用（分钟级）— 箱体形成周期约30-120分钟
BOX_TIME_PARAMS = {
    "box_hard_time_stop_min": (15, 60),          # 15min ~ 1h，防止假突破后长期横盘磨损
    "box_confirm_bars": (5, 20),                 # 5 ~ 20 Bar确认
}
BOX_TIME_PARAM_GRID = {
    "box_hard_time_stop_min": [15, 20, 30, 45, 60],
    "box_confirm_bars": [5, 8, 10, 15, 20],
}
BOX_TIME_DEFAULTS = {
    "box_hard_time_stop_min": 30,                 # 默认30分钟
    "box_confirm_bars": 10,
}

PARAM_GRID_ROUND1 = {
    "close_take_profit_ratio": OPTION_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": OPTION_STOP_LOSS_GRID,
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.80],  # R27-P2-FP-06修复: 追加0.80使config_params全局默认值0.8在扫描网格中
    "lots_min": [1, 2, 3, 5],
    "signal_cooldown_sec": [0.0, 30.0, 60.0, 90.0, 120.0, 180.0],
    "non_other_ratio_threshold": [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    # 决策频率由 K线周期 × state_confirm_bars 自然决定
}

PARAM_DEFAULTS = {  # R21-MEM-P2-05修复: 模块级大字典，含多个PARAM_DEFAULTS_*变体共~15个字典常驻内存；可合并为分层结构或懒加载
    "close_take_profit_ratio": 1.8,  # R17-P0-CFG-04修复: 1.5→1.8与CENTRALIZED_DEFAULTS对齐
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    "max_risk_ratio": 0.8,  # R17重审计修复: 0.3→0.8与config_params全局默认值对齐
    "max_risk_per_trade": 0.05,
    "max_open_positions": 3,
    "lots_min": 3,
    "max_signals_per_window": 5,
    "signal_cooldown_sec": 60.0,
    "non_other_ratio_threshold": 0.65,
    "state_confirm_bars": 5,  # R24-P1-DF-01修复: 统一默认值为5
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.95,
    "spring_max_position_pct": 0.015,
    "capital_route_master_base": 0.60,
    "shadow_alpha_threshold": 0.1,
    "rate_limit_global_per_min": 60,
    "daily_loss_hard_stop_pct": 0.05,
    "logic_reversal_threshold": 1.5,
    # P1-R11-05: 订单执行延迟配置(毫秒)，回测中模拟从信号生成到订单成交的延迟。
    # 默认50ms保证非零延迟，避免回测中零延迟导致的成交优化偏差。
    "execution_delay_ms": 50,
    # P1-R11-04: 平仓信号延迟执行Bar数，模拟实盘中信号生成到实际成交的延迟。
    # 当close_profit_delay_bars>0时，平仓信号延迟N个Bar才执行，模拟实盘事件驱动延迟。
    # 默认0表示立即执行(向后兼容)，建议回测中设1-2以增加真实性。
    "close_profit_delay_bars": 0,
    # P1-R11-01: 回测滑点保守溢价(bps)，弥补历史spread与实时spread的系统性差异
    "backtest_slippage_premium_bps": 0.5,
    # CS-01修复: 统一滑点模型参数
    "expiry_slippage_mult_1d": 20.0,     # 到期前1天滑点倍增
    "expiry_slippage_mult_2d": 10.0,     # 到期前2天滑点倍增
    "expiry_slippage_mult_3d": 5.0,      # 到期前3天滑点倍增
    "expiry_slippage_mult_5d": 3.0,      # 到期前5天滑点倍增
    "expiry_slippage_mult_7d": 2.0,      # 到期前7天滑点倍增
    "expiry_slippage_mult_10d": 1.5,     # 到期前10天滑点倍增
    "expiry_slippage_mult_14d": 1.2,     # 到期前14天滑点倍增
    "base_slippage_bps": 3.0,            # 基础滑点(bps)
    # S1 HFT: hft_hard_time_stop_ms (毫秒级)
    # S2 共振: resonance_hard_time_stop_min (分钟级)
    # S3 箱体: box_hard_time_stop_min (分钟级)
    # S4 弹簧: spring_hard_time_stop_sec (秒级)
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **HFT_TIME_DEFAULTS,
    **SPRING_TIME_DEFAULTS,
    **RESONANCE_TIME_DEFAULTS,
    **BOX_TIME_DEFAULTS,
    **PULLBACK_DEFAULTS,
    **RISK_DIMENSION_DEFAULTS,
    **LIFE_ESTIMATOR_DEFAULTS,
    "skip_rollover_days": 3,
    "backtest_max_sub_order_lots": 5,
    "backtest_fidelity_mode": "standard",
    "execution_model": "standard",
    "enable_intra_bar_stop_loss": False,
    "enable_gap_handling": False,
    "open_execution_delay_bars": 0,
    "close_profit_delay_bars": 0,
    "stop_loss_no_delay": True,
    "default_order_type": "taker",
    "enable_mtm_equity": False,
    "use_option_bs_pricing": False,
    "enable_partial_fill": False,
    "max_participation_rate": 1.0,
    "enable_market_impact": False,
    "market_impact_eta": 0.1,
    "market_impact_gamma": 0.05,
    "exchange_code": "DEFAULT",
    "min_fill_lots": 1,
}

PARAM_GRID_ROUND2 = {
    "max_signals_per_window": [2, 3, 4, 5, 6, 8, 10],
    "state_confirm_bars": [3, 4, 5, 6, 7, 8],
    "spring_stop_profit_ratio": SPRING_STOP_PROFIT_GRID,
    "spring_max_loss_pct": SPRING_MAX_LOSS_GRID,
    "spring_max_position_pct": [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025, 0.030],
    "capital_route_master_base": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
    "capital_route_s5_arbitrage": [0.05, 0.08, 0.10, 0.12, 0.15, 0.20],
    "capital_route_s6_market_making": [0.05, 0.08, 0.10, 0.12, 0.15, 0.20],
    "shadow_alpha_threshold": [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
    "rate_limit_global_per_min": [20, 30, 45, 60, 90, 120, 150],
    "resonance_hard_time_stop_min": RESONANCE_TIME_PARAM_GRID["resonance_hard_time_stop_min"],
    "daily_loss_hard_stop_pct": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10],
    "logic_reversal_threshold": [1.0, 1.2, 1.5, 1.8, 2.0, 2.5],
    # CS-01修复: 滑点参数扫描网格
    "expiry_slippage_mult_3d": [2.0, 5.0, 10.0, 20.0],  # 关键参数：3天到期倍增
    "expiry_slippage_mult_7d": [1.5, 2.0, 3.0, 5.0],    # 7天到期倍增
    "base_slippage_bps": [1.0, 2.0, 3.0, 5.0],          # 基础滑点
    "backtest_slippage_premium_bps": [0.0, 0.5, 1.0],    # 回测保守溢价
    **PULLBACK_GRID,
    **LIFE_ESTIMATOR_GRID,
}

# Round3: 风险权重独立扫描 — 三阶段独立扫描架构(市场参数→辅助交易参数→风险权重)
# Round3固定Round1+Round2最优参数,仅扫描D1-D11决策权重和阈值
PARAM_GRID_ROUND3 = {
    **RISK_DIMENSION_GRID,
}

# Round4: 评分系数独立扫描 — CascadeJudge+StrategyJudgment评分权重
# Round4固定Round1+Round2+Round3最优参数,仅扫描评判评分系数
SCORING_COEFFICIENT_GRID = {
    # CascadeJudge评分权重(profit_ratio+sortino+calmar+sharpe归一化=1.0)
    "scoring_profit_ratio_weight": [0.40, 0.50, 0.60, 0.70],
    "scoring_sortino_weight": [0.20, 0.30, 0.40],
    "scoring_calmar_weight": [0.15, 0.25, 0.30, 0.35],
    "scoring_sharpe_weight": [0.15, 0.25, 0.30, 0.35],
}

PARAM_GRID_ROUND4 = {
    **SCORING_COEFFICIENT_GRID,
}

PARAM_GRID_BOX_EXTREME = {
    "box_detection_threshold": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
    "box_min_bars": [10, 15, 20, 25, 30, 45],
    "extreme_entry_ratio": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
    "close_take_profit_ratio": REVERSION_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": REVERSION_STOP_LOSS_GRID,
    "box_hard_time_stop_min": BOX_TIME_PARAM_GRID["box_hard_time_stop_min"],
    "box_confirm_bars": BOX_TIME_PARAM_GRID["box_confirm_bars"],
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **PULLBACK_GRID,
}

PARAM_DEFAULTS_BOX_EXTREME = {
    "box_detection_threshold": 0.03,
    "box_min_bars": 20,
    "extreme_entry_ratio": 0.5,
    "close_take_profit_ratio": 2.0,
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    "lots_min": 1,
    "max_risk_ratio": 0.2,
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **BOX_TIME_DEFAULTS,
    **PULLBACK_DEFAULTS,
}

PARAM_GRID_BOX_SPRING = {
    "spring_iv_threshold": [0.10, 0.15, 0.20, 0.25, 0.30, 0.35],
    "spring_maturity_days": [3, 5, 7, 10, 14, 21, 30],
    "spring_impulse_threshold": [0.005, 0.01, 0.02, 0.03, 0.04, 0.05],
    "spring_stop_profit_ratio": SPRING_STOP_PROFIT_GRID,
    "spring_max_loss_pct": SPRING_MAX_LOSS_GRID,
    "spring_max_position_pct": [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025, 0.030],
    "close_take_profit_ratio": REVERSION_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": REVERSION_STOP_LOSS_GRID,
    "spring_hard_time_stop_sec": SPRING_TIME_PARAM_GRID["spring_hard_time_stop_sec"],
    "spring_confirm_ticks": SPRING_TIME_PARAM_GRID["spring_confirm_ticks"],
    "spring_iv_pulse_window_sec": SPRING_TIME_PARAM_GRID["spring_iv_pulse_window_sec"],
    "max_risk_ratio": [0.008, 0.010, 0.012, 0.015, 0.020],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **PULLBACK_GRID,
}

PARAM_DEFAULTS_BOX_SPRING = {
    "spring_iv_threshold": 0.20,
    "spring_maturity_days": 14,
    "spring_impulse_threshold": 0.02,
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.90,
    "spring_max_position_pct": 0.015,
    "lots_min": 1,
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    "close_take_profit_ratio": 5.0,
    "close_stop_loss_ratio": 0.95,
    "max_risk_ratio": 0.015,
    **SPRING_TIME_DEFAULTS,
    **PULLBACK_DEFAULTS,
}

PARAM_GRID_HFT = {
    "hft_signal_confirm_ticks": [2, 3, 5, 8, 13],
    "hft_cooldown_ms": [30.0, 50.0, 100.0, 150.0, 200.0, 300.0],
    "hft_min_imbalance": [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40],
    "close_take_profit_ratio": MOMENTUM_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": MOMENTUM_STOP_LOSS_GRID,
    "hft_hard_time_stop_ms": HFT_TIME_PARAM_GRID["hft_hard_time_stop_ms"],
    "daily_loss_hard_stop_pct": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08],
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30],
    "lots_min": [1, 2],
    "non_other_ratio_threshold": [0.3, 0.4, 0.5, 0.6],
}

HFT_TICK_PARAMS = {"hft_signal_confirm_ticks", "hft_cooldown_ms", "hft_min_imbalance", "hft_hard_time_stop_ms"}

PARAM_DEFAULTS_HFT = {
    "hft_signal_confirm_ticks": 5,
    "hft_cooldown_ms": 100.0,
    "hft_min_imbalance": 0.25,
    "close_take_profit_ratio": 1.0,  # HFT策略专用: 高频交易使用1.0(快速止盈)，非通用默认值
    "close_stop_loss_ratio": 0.3,
    "lots_min": 1,
    "max_risk_ratio": 0.2,
    "non_other_ratio_threshold": 0.4,
    "daily_loss_hard_stop_pct": 0.03,
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **HFT_TIME_DEFAULTS,
    # R30-P0-06修复: PARAM_DEFAULTS_HFT缺失**PULLBACK_DEFAULTS展开
    # 导致S1 HFT策略缺少pullback_iv_min_percentile等4个回退参数
    **PULLBACK_DEFAULTS,
}

PARAM_DEFAULTS_ARBITRAGE = {
    **PARAM_DEFAULTS_HFT,
    "arb_deviation_threshold_bps": 50.0,
    "arb_reversion_target_bps": 30.0,
    "arb_min_confidence": 0.6,
    "arb_max_hold_minutes": 15.0,
    "close_take_profit_ratio": 1.0,
    "close_stop_loss_ratio": 0.3,
    **PULLBACK_DEFAULTS,
}

PARAM_DEFAULTS_MARKET_MAKING = {
    **PARAM_DEFAULTS_HFT,
    "mm_ioc_signal_threshold": 0.8,
    "mm_offset_min_ticks": 0,
    "mm_offset_max_ticks": 3,
    "mm_spread_target_bps": 5.0,
    "mm_max_inventory_lots": 5,
    "mm_rebalance_threshold": 3,
    "close_take_profit_ratio": 0.8,
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    **PULLBACK_DEFAULTS,
}

PARAM_GRID_ARBITRAGE = {
    "arb_deviation_threshold_bps": [20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0],
    "arb_reversion_target_bps": [15.0, 20.0, 25.0, 30.0, 35.0, 40.0],
    "arb_min_confidence": [0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8],
    "arb_max_hold_minutes": [5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0],
    "close_take_profit_ratio": ARB_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": ARB_STOP_LOSS_GRID,
    "hft_hard_time_stop_ms": HFT_TIME_PARAM_GRID["hft_hard_time_stop_ms"],
    **PULLBACK_GRID,
}

PARAM_GRID_MARKET_MAKING = {
    "mm_spread_target_bps": [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0],
    "mm_max_inventory_lots": [2, 3, 4, 5, 6, 8, 10],
    "mm_rebalance_threshold": [2, 3, 4, 5, 6, 8],
    "mm_ioc_signal_threshold": [0.6, 0.7, 0.75, 0.8, 0.85, 0.9],
    "mm_offset_max_ticks": [1, 2, 3, 4, 5, 6],
    "close_take_profit_ratio": MM_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": MM_STOP_LOSS_GRID,
    "hft_hard_time_stop_ms": HFT_TIME_PARAM_GRID["hft_hard_time_stop_ms"],
    **PULLBACK_GRID,
}

PARAM_DEFAULTS_SHADOW_A = {
    **PARAM_DEFAULTS,
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.6,
    "max_risk_ratio": 0.15,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
    "spring_hard_time_stop_sec": 24,
    "resonance_hard_time_stop_min": 4,
    "box_hard_time_stop_min": 24,
}

PARAM_DEFAULTS_SHADOW_B = {
    **PARAM_DEFAULTS,
    "close_take_profit_ratio": 1.1,
    "close_stop_loss_ratio": 0.7,
    "max_risk_ratio": 0.1,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
    "spring_hard_time_stop_sec": 18,
    "resonance_hard_time_stop_min": 3,
    "box_hard_time_stop_min": 18,
}

PARAM_DEFAULTS_HFT_SHADOW_A = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.25,
    "max_risk_ratio": 0.12,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_HFT_SHADOW_B = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 1.0,
    "close_stop_loss_ratio": 0.2,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
}

PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A = {
    **PARAM_DEFAULTS_BOX_EXTREME,
    "close_take_profit_ratio": 1.6,
    "close_stop_loss_ratio": 0.6,
    "max_risk_ratio": 0.12,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "box_hard_time_stop_min": 24,
}

PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B = {
    **PARAM_DEFAULTS_BOX_EXTREME,
    "close_take_profit_ratio": 1.3,
    "close_stop_loss_ratio": 0.7,
    "max_risk_ratio": 0.08,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "box_hard_time_stop_min": 18,
}

PARAM_DEFAULTS_BOX_SPRING_SHADOW_A = {
    **PARAM_DEFAULTS_BOX_SPRING,
    "spring_stop_profit_ratio": 4.0,
    "spring_max_loss_pct": 0.85,
    "spring_max_position_pct": 0.010,
    "close_take_profit_ratio": 4.0,
    "close_stop_loss_ratio": 0.75,
    "max_risk_ratio": 0.012,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "spring_hard_time_stop_sec": 24,
}

PARAM_DEFAULTS_BOX_SPRING_SHADOW_B = {
    **PARAM_DEFAULTS_BOX_SPRING,
    "spring_stop_profit_ratio": 3.0,
    "spring_max_loss_pct": 0.80,
    "spring_max_position_pct": 0.008,
    "close_take_profit_ratio": 3.0,
    "close_stop_loss_ratio": 0.70,
    "max_risk_ratio": 0.010,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "spring_hard_time_stop_sec": 18,
}

PARAM_DEFAULTS_ARBITRAGE_SHADOW_A = {
    **PARAM_DEFAULTS_ARBITRAGE,
    "arb_deviation_threshold_bps": 60.0,
    "arb_reversion_target_bps": 35.0,
    "arb_min_confidence": 0.5,
    "close_take_profit_ratio": 0.8,
    "close_stop_loss_ratio": 0.4,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_ARBITRAGE_SHADOW_B = {
    **PARAM_DEFAULTS_ARBITRAGE,
    "arb_deviation_threshold_bps": 55.0,
    "arb_reversion_target_bps": 32.0,
    "arb_min_confidence": 0.55,
    "close_take_profit_ratio": 0.9,
    "close_stop_loss_ratio": 0.35,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
}

PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A = {
    **PARAM_DEFAULTS_MARKET_MAKING,
    "mm_ioc_signal_threshold": 0.7,
    "mm_offset_max_ticks": 4,
    "mm_spread_target_bps": 6.0,
    "mm_max_inventory_lots": 4,
    "close_take_profit_ratio": 0.7,
    "close_stop_loss_ratio": 0.6,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B = {
    **PARAM_DEFAULTS_MARKET_MAKING,
    "mm_ioc_signal_threshold": 0.75,
    "mm_offset_max_ticks": 5,
    "mm_spread_target_bps": 7.0,
    "mm_max_inventory_lots": 3,
    "close_take_profit_ratio": 0.6,
    "close_stop_loss_ratio": 0.55,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
}

# R27-CP-01-FIX: REASON_MULTIPLIERS 已移至 shared_trading_constants.py，此处从共享模块导入
from ali2026v3_trading.shared_trading_constants import REASON_MULTIPLIERS  # noqa: F401





try:
    from ali2026v3_trading.param_pool.backtest_runner_base import _sync_reason_multipliers_with_position_service
    _sync_reason_multipliers_with_position_service()
except Exception:
    pass


_REASON_TIME_STOP_SOURCE = {
    "CORRECT_RESONANCE": ("resonance_hard_time_stop_min", "min", 5.0),
    "CORRECT_DIVERGENCE": ("resonance_hard_time_stop_min", "min", 5.0),
    "INCORRECT_REVERSAL": ("resonance_hard_time_stop_min", "min", 5.0),
    "OTHER_SCALP": ("box_hard_time_stop_min", "min", 30.0),
    "ARBITRAGE": ("resonance_hard_time_stop_min", "min", 5.0),
    "MARKET_MAKING": ("hft_hard_time_stop_ms", "ms", 1000.0),
    "MANUAL": ("resonance_hard_time_stop_min", "min", 5.0),
}

# R27-CP-01-FIX: P0_IRON_RULES 已移至 shared_trading_constants.py，此处从共享模块导入
from ali2026v3_trading.shared_trading_constants import P0_IRON_RULES  # noqa: F401

# R10-P2-06修复: BACKTEST_THRESHOLDS唯一权威定义在task_scheduler.py
try:
    from ali2026v3_trading.param_pool.task_scheduler import BACKTEST_THRESHOLDS
except ImportError:
    try:
        from param_pool.task_scheduler import BACKTEST_THRESHOLDS
    except ImportError:
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
        }

_BACKTEST_THRESHOLD_LINK_MAP = {
    "main_open_min_strength": "decision_threshold_low",
    "correct_resonance_min_strength": "decision_threshold_high",
    "circuit_breaker_daily_dd": "daily_loss_hard_stop_pct",
}

_P0_IRON_RULE_LINK_MAP = {
    "max_oos_decay": "p0_max_oos_decay",
    "oos_decay_warn": "p0_oos_decay_warn",
    "min_train_sharpe": "p0_min_train_sharpe",
    "min_test_sharpe": "p0_min_test_sharpe",
    "min_lr_threshold": "p0_min_lr_threshold",
    "lr_threshold_warn": "p0_lr_threshold_warn",
}





try:
    from ali2026v3_trading.param_pool.backtest_runner_base import _sync_thresholds_from_runtime_config
    _sync_thresholds_from_runtime_config()
except Exception:
    pass

SHADOW_PARAM_MAP = {
    "main": None,
    "shadow_reverse": "shadow_a",
    "shadow_random": "shadow_b",
}

# P0-R11-07修复: 使用deepcopy创建独立副本，防止修改影子参数污染原始默认值
STRATEGY_SHADOW_DEFAULTS = {
    "hft":             {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_HFT_SHADOW_A),          "shadow_b": copy.deepcopy(PARAM_DEFAULTS_HFT_SHADOW_B)},
    "main":            {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_SHADOW_A),              "shadow_b": copy.deepcopy(PARAM_DEFAULTS_SHADOW_B)},
    "box_extreme":     {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A),  "shadow_b": copy.deepcopy(PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B)},
    "box_spring":      {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_BOX_SPRING_SHADOW_A),   "shadow_b": copy.deepcopy(PARAM_DEFAULTS_BOX_SPRING_SHADOW_B)},
    "arbitrage":       {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_ARBITRAGE_SHADOW_A),    "shadow_b": copy.deepcopy(PARAM_DEFAULTS_ARBITRAGE_SHADOW_B)},
    "market_making":   {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A),"shadow_b": copy.deepcopy(PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B)},
}

ROUND1_TOP_K = 10

PARAM_GRID = PARAM_GRID_ROUND1

# 扫描轮次与排序指标的强绑定关系：扫描某类参数时按其影响指标选优。
SCAN_TARGET_SORT_METRIC = {
    "round1": "minute_sharpe",
    "round2": "minute_sharpe",
    "round3": "risk_adjusted_score",
    "round4": "cascade_final_score",
}

# R24-P1-DF-05修复: 参数级排序指标映射，覆盖轮次级默认排序
PARAM_SORT_METRIC_OVERRIDE = {
    'max_risk_ratio': 'risk_adjusted_score',  # 风控参数按风控指标排序
    'close_stop_loss_ratio': 'stop_loss_hit_rate',  # 止损参数按止损指标排序
    'close_take_profit_ratio': 'profit_loss_ratio',  # 止盈参数按盈亏比排序
    # P0-R9-验收标准7修复: 补齐PARAM_GRID_ROUND1中3个缺失排序映射的参数
    'lots_min': 'total_return',  # 仓位参数按总收益排序
    'signal_cooldown_sec': 'win_rate',  # 冷却参数按胜率排序（signal_quality不存在，使用win_rate替代）
    'non_other_ratio_threshold': 'sharpe',  # 状态阈值按夏普比率排序（state_accuracy不存在，使用sharpe替代）
}

OBJECTIVE_FUNCTIONS = {
    'sharpe': lambda r: r.get('sharpe', 0.0),
    'profit_factor': lambda r: r.get('profit_factor', 0.0),
    'win_loss_ratio': lambda r: r.get('win_loss_ratio', 0.0),  # R6-D-16修复: 键名从avg_win_loss_ratio改为win_loss_ratio
    'plr_composite': lambda r: (
        r.get('profit_factor', 0.0) * 0.4
        + r.get('win_loss_ratio', 0.0) * 0.3  # R6-D-16修复: 键名从avg_win_loss_ratio改为win_loss_ratio
        + r.get('sharpe', 0.0) * 0.1
        + r.get('total_return', 0.0) * 0.2
    ),
    'return_per_dd': lambda r: (
        r.get('total_return', 0.0) / abs(r.get('max_drawdown', -0.01))
        if abs(r.get('max_drawdown', -0.01)) > 1e-10 else 0.0
    ),
}

DEFAULT_OBJECTIVE = 'sharpe'





# ======================================================================
# 周期共振参数网格回测 — CRParams全参数扫描
# ======================================================================

CR_PARAM_GRID = {
    'hmm_entropy_window': [10, 20, 30],
    'phase_transition_threshold': [0.2, 0.3, 0.4],
    'chaos_entropy_threshold': [0.6, 0.7, 0.8],
    'trend_weight_short': [0.1, 0.2, 0.3],
    'trend_weight_medium': [0.3, 0.5, 0.6],
    'imbalance_coeff': [0.1, 0.3, 0.5],
    'consistency_sign_weight': [0.3, 0.5, 0.7],
    'consistency_mag_weight': [0.3, 0.5, 0.7],
    'hmm_stability_coeff': [0.3, 0.5, 0.7],
    'release_strength_threshold': [0.4, 0.5, 0.6],
    'release_bias_threshold': [0.2, 0.3, 0.4],
    'exhaust_strength_threshold': [0.1, 0.2, 0.3],
    'exhaust_highvol_threshold': [0.3, 0.4, 0.5],
    'secondary_chaos_entropy': [0.3, 0.4, 0.5],
    'strength_trend_release_threshold': [0.03, 0.05, 0.08],
    'hf_co_size': [0.8, 1.0, 1.2],
    'hf_co_sl': [1.0, 1.2, 1.5],
    'hf_co_hold': [180, 300, 420],
    'hf_counter_size': [0.2, 0.4, 0.6],
    'hf_counter_sl': [0.3, 0.4, 0.5],
    'hf_counter_hold': [15, 30, 60],
    'hf_entropy_penalty_coeff': [0.3, 0.5, 0.7],
    'hf_chaos_size': [0.1, 0.2, 0.3],
    'hf_chaos_sl': [0.2, 0.3, 0.4],
    'hf_chaos_hold': [10, 15, 20],
    'hf_size_mult_max': [1.5, 2.0, 2.5],
    'hf_size_mult_min': [0.05, 0.1, 0.15],
    'res_full_strength': [0.6, 0.7, 0.8],
    'res_half_strength': [0.3, 0.4, 0.5],
    'res_sl_base': [0.6, 0.8, 1.0],
    'res_sl_strength_coeff': [0.2, 0.4, 0.6],
    'res_chaos_size': [0.2, 0.3, 0.4],
    'res_low_size': [0.2, 0.3, 0.4],
    'res_release_full_size': [0.8, 1.0, 1.2],
    'res_half_size': [0.4, 0.6, 0.8],
    'res_release_hold': [400, 600, 800],
    'res_default_hold': [180, 240, 300],
    'res_overnight_strength': [0.5, 0.6, 0.7],
    'res_min_size': [0.1, 0.2, 0.3],
    'box_low_vol_size': [0.8, 1.0, 1.2],
    'box_low_vol_sl': [0.6, 0.8, 1.0],
    'box_low_vol_hold': [1200, 1800, 2400],
    'box_high_vol_release_size': [0.6, 0.8, 1.0],
    'box_high_vol_release_sl': [1.0, 1.5, 2.0],
    'box_high_vol_release_hold': [400, 600, 800],
    'box_normal_size': [0.3, 0.5, 0.7],
    'box_normal_sl': [0.8, 1.0, 1.2],
    'box_normal_hold': [600, 900, 1200],
    'box_default_size': [0.2, 0.3, 0.4],
    'box_default_sl': [1.0, 1.2, 1.4],
    'box_default_hold': [200, 300, 400],
    'box_bias_threshold': [0.2, 0.3, 0.4],
    'box_bias_up_mult': [1.0, 1.1, 1.2],
    'box_bias_down_mult': [0.8, 0.9, 1.0],
    'sp_charge_size': [0.4, 0.6, 0.8],
    'sp_charge_sl': [1.0, 1.5, 2.0],
    'sp_charge_hold': [3600, 7200, 10800],
    'sp_bias_threshold': [0.4, 0.6, 0.8],
    'sp_entropy_penalty_coeff': [0.2, 0.4, 0.6],
    'sp_release_size': [0.8, 1.0, 1.2],
    'sp_release_sl': [0.6, 0.8, 1.0],
    'sp_release_hold': [1200, 1800, 2400],
    'sp_default_size': [0.2, 0.3, 0.4],
    'sp_default_sl': [0.8, 1.0, 1.2],
    'sp_default_hold': [2400, 3600, 4800],
    'sp_bias_boost_mult': [1.0, 1.2, 1.4],
    'max_directional_exposure': [1.0, 1.5, 2.0],
    'chaos_max_total_size': [0.3, 0.4, 0.5],
    'cb_entropy_threshold': [0.8, 0.9, 0.95],
    'cb_sustained_minutes': [10, 15, 20],
    'cb_drawdown_pct': [2.0, 3.0, 4.0],
    'spring_bias_threshold': [0.4, 0.6, 0.8],
    'spring_asymmetric_low': [0.5, 0.7, 0.9],
    'spring_asymmetric_high': [1.2, 1.5, 2.0],
    'hft_default_floor': [0.3, 0.4, 0.5],
    'hft_resonance_floor': [0.5, 0.7, 0.9],
    'hft_floor_strength': [0.3, 0.5, 0.7],
    'trend_direction_window': [50, 100, 150],
    'strength_history_window': [50, 100, 150],
}

