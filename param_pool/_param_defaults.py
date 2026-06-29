# MODULE_ID: M1-148
from __future__ import annotations
"""叶子模块：参数网格纯字面量常量（无param_pool内部依赖）

提取自 backtest_param_grids.py，打破 backtest_param_grids ↔ backtest_runner_utils 循环依赖。
所有常量均为纯字面量，不依赖param_pool内任何其他模块。
"""
import copy
from typing import Any, Dict, List, Optional, Tuple

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
    "pullback_wait_bars": [2, 4, 5, 6, 8, 10],
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


RISK_DIMENSION_DEFAULTS = {
    "decision.score.threshold_high": 0.70,
    "decision.score.threshold_low": 0.50,
}

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
REVERSION_STOP_LOSS_GRID = [0.3, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
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
    # P0-裂缝5修复: 展期参数加入扫描网格，使参数池可优化展期检测敏感度
    "rollover_skip_days": [1, 2, 3, 5],
    "rollover_gap_threshold": [0.01, 0.02, 0.03, 0.05],
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
    "rollover_skip_days": 3,          # P0-裂缝5修复: 与backtest_runner_utils.py参数名对齐
    "rollover_gap_threshold": 0.02,   # P0-裂缝5修复: 展期跳空阈值默认2%
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
    "close_take_profit_ratio": 0.8,
    "close_stop_loss_ratio": 0.25,
    "max_risk_ratio": 0.12,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_HFT_SHADOW_B = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 0.6,
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
    "max_risk_ratio": 0.15,
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
    "max_risk_ratio": 0.10,
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

# R27-CP-01-FIX: REASON_MULTIPLIERS 已移至 shared_trading_constants.py
# NOTE: _sync_reason_multipliers_with_position_service() side effect moved back to backtest_param_grids.py


_REASON_TIME_STOP_SOURCE = {
    "CORRECT_RESONANCE": ("resonance_hard_time_stop_min", "min", 5.0),
    "CORRECT_DIVERGENCE": ("divergence_hard_time_stop_min", "min", 60.0),
    "INCORRECT_REVERSAL": ("resonance_hard_time_stop_min", "min", 5.0),
    "OTHER_SCALP": ("box_hard_time_stop_min", "min", 30.0),
    "ARBITRAGE": ("resonance_hard_time_stop_min", "min", 5.0),
    "MARKET_MAKING": ("hft_hard_time_stop_ms", "ms", 1000.0),
    "MANUAL": ("resonance_hard_time_stop_min", "min", 5.0),
}

SHADOW_PARAM_MAP = {
    "main": None,
    "shadow_reverse": "shadow_a",
    "shadow_random": "shadow_b",
}

# ── S7 背离反转策略参数 ──────────────────────────────────────
DIVERGENCE_DEFAULTS = {
    "divergence_lookback": 20,
    "divergence_atm_threshold": 0.03,
    "divergence_w_future": 0.35,
    "divergence_w_option_coll": 0.35,
    "divergence_consistency_boost": 1.5,
    "divergence_min_ratio": 0.6,
    "divergence_trend_significance": 1e-6,
    "divergence_div_strength_clip": 1.0,
    "divergence_signal_threshold": 0.15,
    "divergence_take_profit_ratio": 1.8,
    "divergence_stop_loss_ratio": 0.3,
    "divergence_max_risk_ratio": 0.5,
    "divergence_hard_time_stop_min": 60.0,
    "divergence_cooldown_bars": 10,
    "divergence_position_scale": 0.3,
    "divergence_moneyness_depth": 0.06,
}

PARAM_GRID_DIVERGENCE = {
    "divergence_lookback": [10, 15, 20, 25, 30],
    "divergence_atm_threshold": [0.02, 0.03, 0.04, 0.05],
    "divergence_w_future": [0.25, 0.30, 0.35, 0.40, 0.45],
    "divergence_w_option_coll": [0.25, 0.30, 0.35, 0.40, 0.45],
    "divergence_consistency_boost": [1.2, 1.3, 1.5, 1.7, 2.0],
    "divergence_min_ratio": [0.4, 0.5, 0.6, 0.7, 0.8],
    "divergence_trend_significance": [1e-7, 1e-6, 1e-5],
    "divergence_div_strength_clip": [0.8, 0.9, 1.0],
    "divergence_signal_threshold": [0.10, 0.15, 0.20, 0.25, 0.30],
    "divergence_take_profit_ratio": [1.0, 1.2, 1.5, 1.8, 2.0, 2.5, 3.0],
    "divergence_stop_loss_ratio": [0.2, 0.3, 0.4, 0.5, 0.6],
    "divergence_max_risk_ratio": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8],
    "divergence_hard_time_stop_min": [30, 45, 60, 90, 120],
    "divergence_cooldown_bars": [5, 8, 10, 15, 20, 30],
    "divergence_position_scale": [0.1, 0.2, 0.3, 0.4, 0.5],
    "divergence_moneyness_depth": [0.04, 0.05, 0.06, 0.08, 0.10],
}

PARAM_DEFAULTS_DIVERGENCE = {
    **PARAM_DEFAULTS,
    **DIVERGENCE_DEFAULTS,
}

PARAM_DEFAULTS_DIVERGENCE_SHADOW_A = {
    **PARAM_DEFAULTS_DIVERGENCE,
    "divergence_lookback": 16,  # lookback * 0.8
    "divergence_consistency_boost": 1.35,  # consistency_boost * 0.9
    "divergence_take_profit_ratio": 1.2,  # vs 1.8 master
    "divergence_stop_loss_ratio": 0.6,    # vs 0.3 master
    "divergence_max_risk_ratio": 0.15,    # vs 0.5 master
    "divergence_hard_time_stop_min": 48.0,  # 60*0.8
    "divergence_position_scale": 0.24,      # 0.3*0.8
    "divergence_cooldown_bars": 12,         # 10*1.2
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.6,
    "max_risk_ratio": 0.15,
}

PARAM_DEFAULTS_DIVERGENCE_SHADOW_B = {
    **PARAM_DEFAULTS_DIVERGENCE,
    "divergence_lookback": 12,  # lookback * 0.6
    "divergence_consistency_boost": 1.2,  # consistency_boost * 0.8
    "divergence_take_profit_ratio": 1.1,
    "divergence_stop_loss_ratio": 0.7,
    "divergence_max_risk_ratio": 0.10,
    "divergence_hard_time_stop_min": 36.0,  # 60*0.6
    "divergence_position_scale": 0.18,      # 0.3*0.6
    "divergence_cooldown_bars": 15,         # 10*1.5
    "close_take_profit_ratio": 1.1,
    "close_stop_loss_ratio": 0.7,
    "max_risk_ratio": 0.1,
}

# P0-R11-07修复: 使用deepcopy创建独立副本，防止修改影子参数污染原始默认值
STRATEGY_SHADOW_DEFAULTS = {
    "hft":             {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_HFT_SHADOW_A),          "shadow_b": copy.deepcopy(PARAM_DEFAULTS_HFT_SHADOW_B)},
    "main":            {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_SHADOW_A),              "shadow_b": copy.deepcopy(PARAM_DEFAULTS_SHADOW_B)},
    "box_extreme":     {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A),  "shadow_b": copy.deepcopy(PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B)},
    "box_spring":      {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_BOX_SPRING_SHADOW_A),   "shadow_b": copy.deepcopy(PARAM_DEFAULTS_BOX_SPRING_SHADOW_B)},
    "arbitrage":       {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_ARBITRAGE_SHADOW_A),    "shadow_b": copy.deepcopy(PARAM_DEFAULTS_ARBITRAGE_SHADOW_B)},
    "market_making":   {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A),"shadow_b": copy.deepcopy(PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B)},
    "divergence":      {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_DIVERGENCE_SHADOW_A),   "shadow_b": copy.deepcopy(PARAM_DEFAULTS_DIVERGENCE_SHADOW_B)},
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
    'signal_cooldown_sec': 'win_rate',  # 冷却参数按胜率排序（signal_quality不存在，使用win_rate替代）'
    'non_other_ratio_threshold': 'sharpe',  # 状态阈值按夏普比率排序（state_accuracy不存在，使用sharpe替代）'
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


MULTISCALE_BAR_LENGTHS = [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440]

BAR_INTERVAL_GRID = {
    "high_freq": [1],
    "resonance": [1, 2, 3, 5, 10, 15, 30],
    "box": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "spring": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "arbitrage": [1, 2, 3, 5],
    "market_making": [1, 2, 3, 5, 10, 15],
}

KLINE_LENGTH_PARAM_GRID = {
    "bar_interval_minutes": [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440],
    "trend_period_short": [2, 3, 5, 8, 13],
    "trend_period_medium": [10, 15, 20, 30, 45],
    "trend_period_long": [30, 40, 60, 90, 120],
    "adx_period": [7, 10, 14, 20, 28],
    "box_lookback_bars": [20, 30, 60, 90, 120, 180],
    "box_min_bars": [5, 10, 15, 20, 30, 45],
    "state_confirm_bars": [3, 5, 8],
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
    "scale_periods_with_bar_interval": [True, False],
}

_SUBPROCESS_NEEDED_COLS = [
    "minute", "symbol", "close", "open", "high", "low", "volume",
    "strength", "imbalance", "bid_ask_spread", "_spread_quality",
    "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
    "days_to_expiry", "expiry_date", "timestamp", "datetime",
    "iv", "tick_size_bps", "avg_trade_size",
]

PULLBACK_FULL_GRID = {
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
}

STRATEGY_PARAM_GRID = {
    "close_take_profit_ratio": [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
    "close_stop_loss_ratio": [0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95],
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 0.70, 0.80, 0.85, 0.90],
    "lots_min": [1, 2, 3, 4, 5],
    "signal_cooldown_sec": [0.0, 30.0, 60.0, 90.0, 120.0, 180.0],
    "non_other_ratio_threshold": [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65],
}

AUX_PARAM_GRID = {
    "max_signals_per_window": [2, 3, 4, 5, 6, 8, 10],
    "state_confirm_bars": [1, 2, 3, 4, 5, 6, 7, 8],
    "alpha_window_days": [3, 5, 7, 10, 14, 21, 30],
    "spring_stop_profit_ratio": [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0],
    "spring_max_loss_pct": [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.98],
    "spring_max_position_pct": [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025, 0.030],
    "capital_route_master_base": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
    "shadow_alpha_threshold": [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
    "rate_limit_global_per_min": [20, 30, 45, 60, 90, 120, 150],
    "hft_hard_time_stop_ms": [100, 200, 500, 1000, 2000, 5000],
    "spring_hard_time_stop_sec": [1, 5, 10, 30, 60, 90, 120],
    "resonance_hard_time_stop_min": [1, 3, 5, 8, 10, 15],
    "box_hard_time_stop_min": [15, 20, 30, 45, 60],
    "daily_loss_hard_stop_pct": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10],
    "logic_reversal_threshold": [1.0, 1.2, 1.5, 1.8, 2.0, 2.5],
}

FIXED_PARAMS = {"max_risk_per_trade": 0.05, "max_open_positions": 3, "logic_reversal_threshold": 1.5}

AUX_DEFAULTS = {
    "max_signals_per_window": 5, "state_confirm_bars": 5, "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.95, "spring_max_position_pct": 0.015, "capital_route_master_base": 0.60,
    "shadow_alpha_threshold": 0.1, "rate_limit_global_per_min": 60, "hft_hard_time_stop_ms": 1000,
    "spring_hard_time_stop_sec": 30, "resonance_hard_time_stop_min": 5, "box_hard_time_stop_min": 30,
    "daily_loss_hard_stop_pct": 0.05,
}

PULLBACK_DEFAULTS_DISABLED = {
    "pullback_enabled": False, "pullback_wait_bars": 5, "pullback_retrace_pct": 0.15,
    "pullback_iv_min_percentile": 20.0, "pullback_iv_max_percentile": 80.0, "pullback_ref_mode": "peak",
    "pullback_atr_wait_multiplier": 0.0, "pullback_retrace_pct_call": 0.38, "pullback_retrace_pct_put": 0.42,
    "pullback_theta_decay_accel": 0.0, "pullback_min_retrace_abs": 0.0, "pullback_max_valid_bars": 24,
}

TRAIN_START = "2023-01-01"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"


# =========================================================================
# 三层期权五态排序参数（最终落地方案第七章 7.3 参数校准优先级）
# =========================================================================

# Phase 1: 核心9参数（4 Tier阈值 + 5个月份权重）
THREE_LAYER_CORE_DEFAULTS = {
    "tl_tier1_wilson_threshold": 0.50,        # 范围 (0.50, 0.70)
    "tl_tier2_coverage_threshold": 0.40,      # 范围 (0.40, 0.70)
    "tl_tier2_correct_up_threshold": 0.45,    # 范围 (0.45, 0.60)
    "tl_tier3_correct_up_threshold": 0.35,    # 范围 (0.35, 0.50)
    "tl_month_weight_1": 0.35,
    "tl_month_weight_2": 0.25,
    "tl_month_weight_3": 0.20,
    "tl_month_weight_4": 0.12,
    "tl_month_weight_5": 0.08,
}

THREE_LAYER_CORE_GRID = {
    "tl_tier1_wilson_threshold": [0.50, 0.55, 0.60, 0.65, 0.70],
    "tl_tier2_coverage_threshold": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70],
    "tl_tier2_correct_up_threshold": [0.45, 0.50, 0.55, 0.60],
    "tl_tier3_correct_up_threshold": [0.35, 0.40, 0.45, 0.50],
    "tl_month_weight_1": [0.25, 0.30, 0.35, 0.40, 0.45],
    "tl_month_weight_2": [0.20, 0.25, 0.30, 0.35],
    "tl_month_weight_3": [0.15, 0.20, 0.25, 0.30],
    "tl_month_weight_4": [0.08, 0.10, 0.12, 0.15],
    "tl_month_weight_5": [0.05, 0.08, 0.10, 0.12],
}

# Phase 2: 扩展参数（Greeks修正）
THREE_LAYER_EXTENSION_DEFAULTS = {
    "tl_delta_min": 0.15,                     # 范围 (0.10, 0.25)
    "tl_delta_max": 0.85,                     # 范围 (0.75, 0.90)
    "tl_gamma_boost_factor": 1.2,             # 范围 (1.0, 1.5)
    "tl_vega_penalty_factor": 0.8,            # 范围 (0.6, 0.9)
    "tl_theta_penalty_max": 0.15,             # 范围 (0.05, 0.25)
    "tl_gamma_max_front_month": 0.08,
    "tl_theta_max_front_month_pct": 0.015,
}

THREE_LAYER_EXTENSION_GRID = {
    "tl_delta_min": [0.10, 0.15, 0.20, 0.25],
    "tl_delta_max": [0.75, 0.80, 0.85, 0.90],
    "tl_gamma_boost_factor": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
    "tl_vega_penalty_factor": [0.6, 0.7, 0.8, 0.9],
    "tl_theta_penalty_max": [0.05, 0.10, 0.15, 0.20, 0.25],
    "tl_gamma_max_front_month": [0.05, 0.06, 0.08, 0.10],
    "tl_theta_max_front_month_pct": [0.010, 0.015, 0.020, 0.025],
}

# Phase 3: 全局参数（Layer 2/3）
THREE_LAYER_GLOBAL_DEFAULTS = {
    "tl_cluster_dispersion_threshold": 0.15,  # 范围 (0.10, 0.25)
    "tl_correlation_threshold": 0.80,        # 范围 (0.70, 0.90)
    "tl_layer2_weight_score1": 0.70,         # 范围 (0.60, 0.80)
    "tl_layer3_weight_score2": 0.65,          # 范围 (0.55, 0.75)
    "tl_market_accuracy_floor_percentile": 20,  # 范围 (10, 30)
    "tl_resonance_veto_threshold": 0.80,
    "tl_cluster_confidence_threshold": 0.60,
    "tl_max_single_product_pct": 0.15,
    "tl_max_single_cluster_pct": 0.40,
    "tl_circuit_breaker_consecutive_min": 5,
    "tl_circuit_breaker_position_mult": 0.5,
}

THREE_LAYER_GLOBAL_GRID = {
    "tl_cluster_dispersion_threshold": [0.10, 0.15, 0.20, 0.25],
    "tl_correlation_threshold": [0.70, 0.75, 0.80, 0.85, 0.90],
    "tl_layer2_weight_score1": [0.60, 0.65, 0.70, 0.75, 0.80],
    "tl_layer3_weight_score2": [0.55, 0.60, 0.65, 0.70, 0.75],
    "tl_market_accuracy_floor_percentile": [10, 15, 20, 25, 30],
    "tl_resonance_veto_threshold": [0.70, 0.75, 0.80, 0.85, 0.90],
    "tl_cluster_confidence_threshold": [0.50, 0.55, 0.60, 0.65, 0.70],
    "tl_max_single_product_pct": [0.10, 0.12, 0.15, 0.18, 0.20],
    "tl_max_single_cluster_pct": [0.30, 0.35, 0.40, 0.45, 0.50],
    "tl_circuit_breaker_consecutive_min": [3, 5, 7, 10],
    "tl_circuit_breaker_position_mult": [0.3, 0.4, 0.5, 0.6, 0.7],
}

# 三层排序完整默认值（合并所有Phase）
# v2.0重构：串行三层→并列信号源A/B/C
THREE_LAYER_PARAM_DEFAULTS = {
    **THREE_LAYER_CORE_DEFAULTS,
    **THREE_LAYER_EXTENSION_DEFAULTS,
    **THREE_LAYER_GLOBAL_DEFAULTS,
    "tl_signal_source": "C",             # A=单品种月份, B=联动品种簇, C=全域品种, auto=自动选择
    "tl_layer1_sort_mode": "lexicographic",  # lexicographic | weighted
    "tl_layer2_mode": "veto",          # veto | weighted
    "tl_enable_resonance_weighting": False,
    "tl_enable_resonance_veto": False,
    "tl_enable_global_percentile": True,
    "tl_hard_filter_enabled": False,
    "tl_pure_mode": False,
    "tl_market_floor_mode": "percentile",  # percentile | fixed
    "tl_ab_test_enabled": False,
}

# 三层排序完整网格（合并所有Phase）
THREE_LAYER_PARAM_GRID = {
    **THREE_LAYER_CORE_GRID,
    **THREE_LAYER_EXTENSION_GRID,
    **THREE_LAYER_GLOBAL_GRID,
    "tl_signal_source": ["A", "B", "C", "auto"],
    "tl_layer1_sort_mode": ["lexicographic", "weighted"],
    "tl_layer2_mode": ["veto", "weighted"],
    "tl_enable_resonance_weighting": [True, False],
    "tl_enable_resonance_veto": [True, False],
    "tl_enable_global_percentile": [True, False],
    "tl_hard_filter_enabled": [True, False],
    "tl_pure_mode": [True, False],
    "tl_market_floor_mode": ["percentile", "fixed"],
    "tl_ab_test_enabled": [True, False],
}