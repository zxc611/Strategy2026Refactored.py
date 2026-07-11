# MODULE_ID: M1-149
from __future__ import annotations
"""叶子模块：回测数据类型（无param_pool内部依赖）

提取自 backtest_state.py，打破 backtest_state ↔ backtest_fidelity 等循环依赖。
所有类型均为纯数据类，不依赖param_pool内任何其他模块。
"""
import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from collections import deque

import pandas as pd


_DEFAULT_INITIAL_EQUITY = 1_000_000.0


@dataclass
class _PendingOrder:
    """BF-02+03: 统一执行时序模型 - 延迟订单"""
    symbol: str
    order_type: str
    volume: int
    lots: int
    reason: str
    signal_bar_idx: int
    signal_price: float
    tp_ratio: float
    sl_ratio: float
    params_snapshot: Dict[str, Any]
    created_at_bar: int
    fee_type: str = "taker"
    retry_count: int = 0


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
    profit_history: list = field(default_factory=lambda: [])
    _PROFIT_HISTORY_MAXLEN = 2000
    last_check_time: Optional[pd.Timestamp] = None
    instrument_type: str = "future"
    option_premium: float = 0.0
    margin_requirement: float = 0.0
    strike_price: float = 0.0
    remaining_days: int = 0
    option_type: str = "call"
    open_iv: float = 0.0
    open_theo_price: float = 0.0
    fill_ratio: float = 1.0


@dataclass(slots=True)
class _ClosedTrade:
    pnl: float
    pnl_pct: float
    close_reason: str
    hold_minutes: float
    open_reason: str = ""
    premium_pnl: float = 0.0
    delta_pnl: float = 0.0
    stage1_passed: bool = False


@dataclass
class _BacktestState:
    """回测状态 — INITIAL_EQUITY改为参数注入，消除对backtest_config的依赖"""
    equity: float = _DEFAULT_INITIAL_EQUITY
    initial_equity: float = _DEFAULT_INITIAL_EQUITY
    peak_equity: float = _DEFAULT_INITIAL_EQUITY
    positions: Dict[str, _BacktestPosition] = field(default_factory=dict)
    equity_curve: Deque[float] = field(default_factory=lambda: deque(maxlen=None))
    daily_returns: Deque[float] = field(default_factory=lambda: deque(maxlen=None))
    current_state: str = "other"
    state_confirm_count: int = 0
    pending_state: Optional[str] = None
    last_state_check_time: Optional[pd.Timestamp] = None
    last_signal_time: Optional[pd.Timestamp] = None
    circuit_breaker_until: Optional[pd.Timestamp] = None
    circuit_breaker_events: List[Dict] = field(default_factory=list)
    daily_loss: float = 0.0
    daily_start_equity: float = _DEFAULT_INITIAL_EQUITY
    consecutive_loss_pause_until: Optional[pd.Timestamp] = None
    consecutive_loss_streak: int = 0
    consecutive_losses: int = 0
    total_signals: int = 0
    total_trades: int = 0
    prev_date: Optional[str] = None
    recent_pnls: List[float] = field(default_factory=list)
    closed_trades: List[_ClosedTrade] = field(default_factory=list)
    _kahan_c: float = 0.0
    snapshot_collector: Any = None
    pending_orders: List = field(default_factory=list)
    mtm_equity: float = 0.0
    mtm_equity_curve: list = field(default_factory=list)
    mtm_peak_equity: float = 0.0
    mtm_max_drawdown: float = 0.0
    bar_idx: int = 0


# ======================================================================
# 常量（提取自 validation_deep_orchestrator, validation_l2_hyperparams, backtest_state）
# ======================================================================
"""叶子模块：回测验证常量（无param_pool内部依赖）

提取自 validation_deep_orchestrator.py, validation_l2_hyperparams.py,
backtest_state.py，打破 backtest_param_grids → backtest_config 等循环依赖。
所有常量均为纯字面量，不依赖param_pool内任何其他模块。
"""


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


PARAM_TIERS = {
    "must_calibrate_every_run": [
        "close_take_profit_ratio", "close_stop_loss_ratio", "max_risk_ratio",
        "lots_min", "signal_cooldown_sec", "non_other_ratio_threshold",
    ],
    "quarterly_review": [
        "max_signals_per_window", "state_confirm_bars", "spring_stop_profit_ratio",
        "spring_max_loss_pct", "spring_max_position_pct",
    ],
    "annual_or_phase_change": [
        "capital_route_master_base", "shadow_alpha_threshold",
        "rate_limit_global_per_min",
        "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
        "resonance_hard_time_stop_min", "box_hard_time_stop_min",
        "daily_loss_hard_stop_pct", "logic_reversal_threshold",
    ],
    "hft_replay_only": [
        "hft_signal_confirm_ticks", "hft_cooldown_ms",
        "hft_min_imbalance", "hft_hard_time_stop_ms",
    ],
}


# ======================================================================
# 纯函数（提取自 backtest_runner_utils, backtest_fidelity）
# ======================================================================
"""叶子模块：回测纯函数（无param_pool内部依赖）'
提取自 backtest_runner_utils.py, backtest_fidelity.py，
打破 backtest_runner_base ↔ backtest_runner_utils 等循环依赖。'
所有函数仅依赖标准库/第三方库/infra，不依赖param_pool内任何模块。
"""
import random
from typing import Dict

import numpy as np
import pandas as pd


def _sync_random_seed(seed: int) -> "np.random.Generator":
    """C-38修复: 使用局部Generator替代全局seed"""
    random.seed(seed)
    return np.random.default_rng(seed)


def _compute_market_impact_v2(
    fill_lots: int,
    bar: pd.Series,
    price: float,
    params: Dict[str, float],
) -> float:
    """BF-06: Almgren-Chriss冲击成本（二级模型，默认关闭）'
    当enable_market_impact=False时返回0.0(向后兼容)
    冲击成本 = eta * sigma * |X|/V + gamma * sigma * (X/V)^2
    """
    if not params.get("enable_market_impact", False):
        return 0.0
    if fill_lots <= 0 or price <= 0:
        return 0.0
    eta = params.get("market_impact_eta", 0.1)
    gamma = params.get("market_impact_gamma", 0.05)
    bar_volume = bar.get("volume", 1.0)
    if bar_volume <= 0:
        bar_volume = 1.0
    participation = abs(fill_lots) / bar_volume
    bar_range = bar.get("high", price) - bar.get("low", price)
    sigma = bar_range / price if price > 0 else 0.01
    impact = eta * sigma * participation + gamma * sigma * participation ** 2
    return price * impact


def _compute_dynamic_slippage_bps(
    price: float,
    bid_ask_spread: float,
    base_slippage_bps: float = 3.0,
    spread_quality: int = 1,
    bar: pd.Series = None,
    params: Dict[str, float] = None,
    liquidity_tier: str = "future_main",
    instrument_type: str = "ETF",
) -> float:
    """动态滑点模型 — 委托到infra.shared_utils.compute_slippage_bps统一实现

    改造说明：原版通过延迟import获取SLIPPAGE_BPS/_infer_liquidity_tier/_infer_instrument_type，
    现改为参数注入，消除对backtest_runner_base和backtest_state的运行时循环依赖。
    调用方需传入base_slippage_bps/liquidity_tier/instrument_type。
    """
    from ali2026v3_trading.infra.shared_utils import compute_slippage_bps as _unified_slippage

    _premium_bps = 0.0
    if params is not None:
        _premium_bps = float(params.get("backtest_slippage_premium_bps", 0.5))

    days_to_expiry = 999
    if bar is not None:
        _dte = bar.get("days_to_expiry", None)
        if _dte is not None:
            try:
                days_to_expiry = int(_dte)
            except (TypeError, ValueError):
                pass

    return _unified_slippage(
        price=price,
        bid_ask_spread=bid_ask_spread,
        base_slippage_bps=base_slippage_bps,
        spread_quality=spread_quality,
        days_to_expiry=days_to_expiry,
        instrument_type=instrument_type,
        liquidity_tier=liquidity_tier,
        backtest_premium_bps=_premium_bps,
    )

# ── Backtest Param Grids (merged from backtest_param_grids.py) ──

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
    P1修复：添加enable_queue_simulation配置参数预留，占位实现现simulate_limit_order_queue()
  - BF-P1-03: 市价单始终以收盘价成交（假设：无滑点模拟）
    P1修复：添加market_order_slippage_bps和market_order_price_mode配置参数预留，
           占位实现现simulate_market_order_slippage()支持weighted/open/high/low/close成交价
  - BF-P1-04: 未考虑资金占用和机会成本（假设：无限资金）
  - BF-P1-05: 期权定价使用BS模型而非市场实际报价（假设：BS模型有效）
  - BF-P1-06: 未模拟保证金追保和强平（假设：保证金充足）
  - BF-P1-07: ETF/期货/期权使用相同撮合假设（假设：品种无差异）
    P1修复：添加instrument_slippage_multiplier配置参数预留，按instrument_type区分滑点，
           占位实现现get_instrument_type_slippage()
  - BF-P1-08: 未模拟交易所限速和拒单（假设：无限制）
  - BF-P1-09: 无撤单模拟（假设：撤单立即成功）
    P1修复：添加enable_cancel_simulation配置参数预留，占位实现现simulate_order_cancel()
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



from ali2026v3_trading.param_pool._param_defaults import (  # noqa: F401
    ARB_STOP_LOSS_GRID,
    ARB_TAKE_PROFIT_GRID,
    BOX_TIME_DEFAULTS,
    BOX_TIME_PARAMS,
    BOX_TIME_PARAM_GRID,
    CR_PARAM_GRID,
    DEFAULT_OBJECTIVE,
    HFT_TICK_PARAMS,
    HFT_TIME_DEFAULTS,
    HFT_TIME_PARAMS,
    HFT_TIME_PARAM_GRID,
    LIFE_ESTIMATOR_DEFAULTS,
    LIFE_ESTIMATOR_GRID,
    MM_STOP_LOSS_GRID,
    MM_TAKE_PROFIT_GRID,
    MOMENTUM_STOP_LOSS_GRID,
    MOMENTUM_TAKE_PROFIT_GRID,
    OBJECTIVE_FUNCTIONS,
    OPTION_STOP_LOSS_GRID,
    OPTION_TAKE_PROFIT_GRID,
    PARAM_DEFAULTS,
    PARAM_DEFAULTS_ARBITRAGE,
    PARAM_DEFAULTS_ARBITRAGE_SHADOW_A,
    PARAM_DEFAULTS_ARBITRAGE_SHADOW_B,
    PARAM_DEFAULTS_BOX_EXTREME,
    PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A,
    PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B,
    PARAM_DEFAULTS_BOX_SPRING,
    PARAM_DEFAULTS_BOX_SPRING_SHADOW_A,
    PARAM_DEFAULTS_BOX_SPRING_SHADOW_B,
    PARAM_DEFAULTS_HFT,
    PARAM_DEFAULTS_HFT_SHADOW_A,
    PARAM_DEFAULTS_HFT_SHADOW_B,
    PARAM_DEFAULTS_MARKET_MAKING,
    PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A,
    PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B,
    PARAM_DEFAULTS_SHADOW_A,
    PARAM_DEFAULTS_SHADOW_B,
    PARAM_GRID,
    PARAM_GRID_ARBITRAGE,
    PARAM_GRID_BOX_EXTREME,
    PARAM_GRID_BOX_SPRING,
    PARAM_GRID_HFT,
    PARAM_GRID_MARKET_MAKING,
    PARAM_GRID_ROUND1,
    PARAM_GRID_ROUND2,
    PARAM_GRID_ROUND3,
    PARAM_GRID_ROUND4,
    PARAM_SORT_METRIC_OVERRIDE,
    PULLBACK_DEFAULTS,
    PULLBACK_GRID,
    RESONANCE_TIME_DEFAULTS,
    RESONANCE_TIME_PARAMS,
    RESONANCE_TIME_PARAM_GRID,
    REVERSION_STOP_LOSS_GRID,
    REVERSION_TAKE_PROFIT_GRID,
    RISK_DIMENSION_DEFAULTS,
    RISK_DIMENSION_GRID,
    ROUND1_TOP_K,
    SCAN_TARGET_SORT_METRIC,
    SCORING_COEFFICIENT_GRID,
    SHADOW_PARAM_MAP,
    SPRING_MAX_LOSS_GRID,
    SPRING_STOP_PROFIT_GRID,
    SPRING_TIME_DEFAULTS,
    SPRING_TIME_PARAMS,
    SPRING_TIME_PARAM_GRID,
    STRATEGY_SHADOW_DEFAULTS,
    THREE_LAYER_CORE_DEFAULTS,
    THREE_LAYER_CORE_GRID,
    THREE_LAYER_EXTENSION_DEFAULTS,
    THREE_LAYER_EXTENSION_GRID,
    THREE_LAYER_GLOBAL_DEFAULTS,
    THREE_LAYER_GLOBAL_GRID,
    THREE_LAYER_PARAM_DEFAULTS,
    THREE_LAYER_PARAM_GRID,
)

try:
    from ali2026v3_trading.param_pool.backtest_runner_base import _sync_reason_multipliers_with_position_service
    _sync_reason_multipliers_with_position_service()
except (ImportError, AttributeError):
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
from ali2026v3_trading.infra.commission_utils import P0_IRON_RULES  # noqa: F401

# R10-P2-06修复: BACKTEST_THRESHOLDS唯一权威定义在task_scheduler.py
_BACKTEST_THRESHOLDS_DEFAULT = {
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

_BACKTEST_THRESHOLDS_LOADED = False
BACKTEST_THRESHOLDS = dict(_BACKTEST_THRESHOLDS_DEFAULT)


def _ensure_backtest_thresholds():
    global BACKTEST_THRESHOLDS, _BACKTEST_THRESHOLDS_LOADED
    if _BACKTEST_THRESHOLDS_LOADED:
        return BACKTEST_THRESHOLDS
    _BACKTEST_THRESHOLDS_LOADED = True
    try:
        from ali2026v3_trading.param_pool.backtest_config import BACKTEST_THRESHOLDS as _bt
        BACKTEST_THRESHOLDS = _bt
    except ImportError:
        pass
    return BACKTEST_THRESHOLDS

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
except (ImportError, AttributeError):
    pass

from ali2026v3_trading.param_pool._param_defaults import SHADOW_PARAM_MAP, STRATEGY_SHADOW_DEFAULTS  # 五唯一性修复：统一从 _param_defaults 导入（STRATEGY_SHADOW_DEFAULTS含divergence项，修复此前缺失BUG）

ROUND1_TOP_K = 10

PARAM_GRID = PARAM_GRID_ROUND1
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
    # P0-裂缝5修复: 展期参数排序映射
    'rollover_skip_days': 'sharpe',  # 展期跳过天数按夏普排序（展期剔除后夏普差异<10%为通过标准）'
    'rollover_gap_threshold': 'sharpe',  # 展期跳空阈值按夏普排序
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