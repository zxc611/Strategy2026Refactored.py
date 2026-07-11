# [M3-11] 市场快照采集器
# MODULE_ID: M3-621
"""
市场快照采集器 (Market Snapshot Collector) — 生产就绪版

覆盖21策略(7主策略×3变体: master/shadow_a/shadow_b)的完整方法和状态，
在以下关键时刻捕获完整市场状态快照：
  1. 所有信号发生点（开单策略触发信号时）
  2. 所有开仓、持仓检查、平仓时刻
  3. 周线/月线级别高低点前后5天
  4. 极值区域进入时
  5. 周期共振相位转换时
  6. 弹簧策略状态转换时
  7. 生态系统策略切换时
  8. 安全元层触发时

快照内容（策略池监测的所有信息）：
  - 增强Bar + 周期共振四变量 + 风险曲面调节
  - 七策略各自状态(SignalService/BoxDetector/SpringState/Ecosystem/ArbitrageDetector/MarketMakerDefense/DivergenceReversal)
  - 21策略独立StrategyStateSnapshot(7主策略×master/shadow_a/shadow_b)
  - 五态分布 + 订单流指标 + Greeks
  - HMM状态/后验 + 影子策略Alpha指标(7组×3变体Sharpe)
  - 跨策略Greeks敞口聚合 + 安全元层状态
  - 组合级PnL/回撤/权益曲线

存储：DuckDB列式存储
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from ali2026v3_trading.infra.shared_utils import ToDictMixin  # R3-4修复
from ali2026v3_trading.infra._helpers import get_logger  # R9-5

import numpy as np

logger = get_logger(__name__)  # R9-5

# ── 21策略标识体系：7主策略 × 3变体(master/shadow_a/shadow_b) ──
SEVEN_STRATEGY_KEYS = ["high_freq", "resonance", "box", "spring", "arbitrage", "market_making", "divergence"]
SIX_STRATEGY_KEYS = SEVEN_STRATEGY_KEYS  # 向后兼容别名
THREE_VARIANTS = ["master", "shadow_a", "shadow_b"]

ALL_21_STRATEGY_IDS = [
    f"{sk}_{variant}" for sk in SEVEN_STRATEGY_KEYS for variant in THREE_VARIANTS
]
ALL_18_STRATEGY_IDS = ALL_21_STRATEGY_IDS  # 向后兼容别名

STRATEGY_ID_TO_TYPE = {sid: sid.rsplit("_", 1)[0] if "_" in sid else sid for sid in ALL_21_STRATEGY_IDS}
_STRATEGY_KEY_MAP = {
    "high_freq": "high_freq",
    "resonance": "resonance",
    "box": "box",
    "spring": "spring",
    "arbitrage": "arbitrage",
    "market_making": "market_making",
    "divergence": "divergence",
}

def _parse_strategy_id(strategy_id: str) -> Tuple[str, str]:
    """解析策略ID → (策略类型, 变体名)"""
    for sk in SIX_STRATEGY_KEYS:
        for variant in THREE_VARIANTS:
            if strategy_id == f"{sk}_{variant}":
                return sk, variant
    return strategy_id, "master"


class SnapshotTrigger(Enum):
    SIGNAL_GENERATED = "信号生成"
    ORDER_OPENED = "开仓"
    POSITION_HELD = "持仓检查"
    ORDER_CLOSED = "平仓"
    WEEKLY_HIGH = "周线高点"
    WEEKLY_LOW = "周线低点"
    MONTHLY_HIGH = "月线高点"
    MONTHLY_LOW = "月线低点"
    EXTREME_REGION = "极值区域"
    TURNING_POINT = "转折点"
    PHASE_TRANSITION = "相位转换"
    SPRING_STATE_CHANGE = "弹簧状态转换"
    ECOSYSTEM_SWITCH = "生态系统策略切换"
    SAFETY_META_TRIGGER = "安全元层触发"
    CIRCUIT_BREAKER = "熔断触发"
    EV_BOTTOMLINE_BREACH = "期望值底线突破"
    DIVERGENCE_REVERSAL = "背离反转信号"


@dataclass(slots=True)
class StrategyStateSnapshot(ToDictMixin):
    strategy_id: str
    strategy_type: str
    signal_strength: float = 0.0
    signal_direction: str = ""
    position_size: float = 0.0
    position_direction: str = ""
    position_pnl: float = 0.0
    position_unrealized_pnl: float = 0.0
    position_hold_seconds: float = 0.0
    expected_value: float = 0.0
    sharpe: float = 0.0
    risk_surface_size_mult: float = 1.0
    risk_surface_sl_mult: float = 1.0
    risk_surface_max_hold: float = 300.0
    paused: bool = False
    frozen: bool = False
    greeks_delta: float = 0.0
    greeks_gamma: float = 0.0
    greeks_vega: float = 0.0
    greeks_theta: float = 0.0
    greeks_iv: float = 0.0
    open_reason: str = ""
    capital_allocation: float = 0.0
    state: str = "inactive"
    params_locked: bool = False
    last_direction: str = ""



@dataclass(slots=True)
class HFTSpecificState(ToDictMixin):
    signal_confirm_ticks: int = 0
    cooldown_remaining_ms: float = 0.0
    current_imbalance: float = 0.0
    microstructure_signal: str = ""
    tick_fidelity_score: float = 0.0



@dataclass(slots=True)
class ResonanceSpecificState(ToDictMixin):
    hmm_state: str = "NORMAL"
    hmm_posterior: Tuple[float, float, float] = (0.33, 0.34, 0.33)
    trend_scores: Tuple[float, float, float] = (0.0, 0.0, 0.0)
    trend_directions: Tuple[float, float, float] = (0.0, 0.0, 0.0)
    current_market_state: str = "other"
    state_confirm_count: int = 0
    circuit_breaker_active: bool = False



@dataclass(slots=True)
class BoxSpecificState(ToDictMixin):
    box_top: float = 0.0
    box_bottom: float = 0.0
    box_width_pct: float = 0.0
    box_confidence: float = 0.0
    touch_count: int = 0
    extreme_type: str = ""
    extreme_confidence: float = 0.0
    iv_percentile: float = 0.0
    iv_filter_passed: bool = False
    flow_exhaustion: bool = False
    trade_direction: str = ""



@dataclass(slots=True)
class SpringSpecificState(ToDictMixin):
    spring_state: str = "DORMANT"
    iv_percentile: float = 0.0
    premium_cost_pct: float = 0.0
    gamma_exposure: float = 0.0
    box_id: str = ""
    direction: str = ""
    active_positions: int = 0
    max_positions: int = 3
    pnl_ratio: float = 0.0
    should_take_profit: bool = False
    should_accept_loss: bool = False
    prevent_trend_conversion: bool = True



@dataclass(slots=True)
class ArbitrageSpecificState(ToDictMixin):
    deviation_bps: float = 0.0
    fair_value_shift_bps: float = 0.0
    confidence: float = 0.0
    direction: int = 0
    is_opportunity: bool = False
    ttl_remaining_seconds: float = 0.0
    total_opportunities: int = 0
    total_checks: int = 0



@dataclass(slots=True)
class MarketMakingSpecificState(ToDictMixin):
    current_inventory: int = 0
    max_inventory: int = 5
    bid_price: float = 0.0
    ask_price: float = 0.0
    spread_bps: float = 0.0
    fill_count: int = 0
    ioc_count: int = 0
    hidden_count: int = 0
    rebalance_needed: bool = False



@dataclass(slots=True)
class DivergenceSpecificState(ToDictMixin):
    """背离反转策略状态快照 — 三层背离检测 + 五级期权分类 + 综合信号"""
    # 期权价值状态 (0=DITM, 1=ITM, 2=ATM, 3=OTM, 4=DOTM)
    option_moneyness_state: int = 2
    # L1: 跨期期货背离强度 [-1, 1]
    div_future_cross_term: float = 0.0
    # L2: 远月期权权利金集体背离 [-1, 1]
    div_option_premium_coll: float = 0.0
    # L3: 当月期权近实值权利金背离 [-1, 1]
    div_option_near_itm: float = 0.0
    # 综合背离反转信号 [-1, 1]
    div_reversal_signal: float = 0.0
    # 信号方向: "bearish" / "bullish" / ""
    signal_direction: str = ""
    # 信号强度: |div_reversal_signal|
    signal_strength: float = 0.0
    # 三层一致性: L1/L2/L3是否同向
    three_layer_consistent: bool = False
    # 当前lookback窗口内新高标记
    current_month_new_high: bool = False
    # 当前lookback窗口内新低标记
    current_month_new_low: bool = False



@dataclass(slots=True)
class EcosystemState(ToDictMixin):
    active_strategy: str = ""
    master_state: str = "inactive"
    reverse_state: str = "inactive"
    other_state: str = "inactive"
    spring_state: str = "inactive"
    arbitrage_state: str = "inactive"
    market_making_state: str = "inactive"
    divergence_state: str = "inactive"
    master_capital: float = 0.60
    reverse_capital: float = 0.25
    other_capital: float = 0.15
    spring_capital: float = 0.15
    arbitrage_capital: float = 0.10
    market_making_capital: float = 0.10
    divergence_capital: float = 0.10
    mutual_exclusion_ok: bool = True
    absolute_ev_ok: bool = True
    master_ev: float = 0.0
    reverse_ev: float = 0.0
    other_ev: float = 0.0
    spring_ev: float = 0.0
    arbitrage_ev: float = 0.0
    market_making_ev: float = 0.0
    divergence_ev: float = 0.0



@dataclass(slots=True)
class ShadowAlphaState(ToDictMixin):
    """影子策略Alpha指标 — 覆盖7策略组×3变体(master/shadow_a/shadow_b)"""
    # S1 高频
    s1_master_sharpe: float = 0.0
    s1_shadow_a_sharpe: float = 0.0
    s1_shadow_b_sharpe: float = 0.0
    # S2 共振
    s2_master_sharpe: float = 0.0
    s2_shadow_a_sharpe: float = 0.0
    s2_shadow_b_sharpe: float = 0.0
    # S3 箱体极值
    s3_master_sharpe: float = 0.0
    s3_shadow_a_sharpe: float = 0.0
    s3_shadow_b_sharpe: float = 0.0
    # S4 弹簧
    s4_master_sharpe: float = 0.0
    s4_shadow_a_sharpe: float = 0.0
    s4_shadow_b_sharpe: float = 0.0
    # S5 套利
    s5_master_sharpe: float = 0.0
    s5_shadow_a_sharpe: float = 0.0
    s5_shadow_b_sharpe: float = 0.0
    # S6 做市
    s6_master_sharpe: float = 0.0
    s6_shadow_a_sharpe: float = 0.0
    s6_shadow_b_sharpe: float = 0.0
    # S7 背离反转
    s7_master_sharpe: float = 0.0
    s7_shadow_a_sharpe: float = 0.0
    s7_shadow_b_sharpe: float = 0.0
    # 聚合指标（向后兼容）
    master_sharpe: float = 0.0
    shadow_a_sharpe: float = 0.0
    shadow_b_sharpe: float = 0.0
    alpha_ratio: float = 0.0
    alpha_ratio_decline_pct: float = 0.0
    degradation_active: bool = False
    absolute_ev_breached: bool = False



@dataclass(slots=True)
class SafetyMetaState(ToDictMixin):
    circuit_breaker_active: bool = False
    hard_stop_triggered: bool = False
    new_open_blocked: bool = False
    trading_paused: bool = False
    daily_drawdown_pct: float = 0.0
    max_drawdown_pct: float = 0.0



@dataclass(slots=True)
class CrossStrategyGreeks(ToDictMixin):
    net_delta: float = 0.0
    gross_delta: float = 0.0
    net_vega: float = 0.0
    gross_vega: float = 0.0
    net_gamma: float = 0.0
    gross_gamma: float = 0.0
    total_futures_lots: int = 0
    total_option_lots: int = 0
    risk_guard_level: str = "PASS"



@dataclass(slots=True)
class LifeExpectancySnapshot(ToDictMixin):
    """行情寿命快照 — 捕获当前HMM状态的寿命期望"""
    hmm_state: str = ""
    duration_p25: float = 0.0       # 持续时间p25(分钟)
    duration_p50: float = 0.0       # 持续时间p50(分钟)
    duration_p75: float = 0.0       # 持续时间p75(分钟)
    duration_p99: float = 0.0       # 持续时间p99(分钟)
    magnitude_p50: float = 0.0      # 价格变化幅度p50(%)
    magnitude_p75: float = 0.0      # 价格变化幅度p75(%)
    sample_count: int = 0           # 样本数
    decay_r_squared: float = 0.0    # 衰减曲线R²
    degradation_level: int = 0      # 降级等级(0-3)
    is_valid: bool = False          # 寿命数据是否有效



@dataclass(slots=True)
class CyclePredictionSnapshot(ToDictMixin):
    """周期预测快照 — 捕获多周期趋势和HMM后验"""
    hmm_posterior: Tuple[float, float, float] = (0.33, 0.34, 0.33)  # HMM后验(LOW_VOL, NORMAL, HIGH_VOL)
    trend_scores_short: float = 0.0    # 短周期趋势评分[-1,1]
    trend_scores_medium: float = 0.0   # 中周期趋势评分[-1,1]
    trend_scores_long: float = 0.0     # 长周期趋势评分[-1,1]
    trend_directions_short: float = 0.0  # 短周期方向(1=up,-1=down,0=flat)
    trend_directions_medium: float = 0.0 # 中周期方向
    trend_directions_long: float = 0.0   # 长周期方向
    phase_remaining_estimate: float = 0.0  # 当前相位预计剩余(分钟)
    resonance_strength: float = 0.0    # D4 周期共振强度[0,1]
    phase_quality: float = 0.5         # D5 相位质量评分[0,1]
    state_entropy: float = 0.5         # 状态熵[0,1]



@dataclass(slots=True)
class RiskDimensionScores(ToDictMixin):
    """11维度统一风险评分快照 — 三层架构"""
    # 核心信号层(0.45)
    d1_state_strength: float = 0.5      # 五态→三态路由状态强度
    d2_order_flow: float = 0.5          # 订单流方向一致性
    d4_cycle_resonance: float = 0.5     # 周期共振强度
    d9_tri_validation: float = 0.5      # 三角验证(Sortino/Calmar/Sharpe)
    # 市场状态层(0.30)
    d3_life_expectancy: float = 0.5     # 行情寿命置信度
    d5_phase_quality: float = 0.5       # 周期相位质量
    d6_greeks_usage: float = 0.5        # Greeks使用率
    d8_asymmetric_drawdown: float = 0.5 # 非对称回撤
    # 组合风控层(0.25)
    d7_consecutive_loss: float = 1.0    # 连亏状态(默认无亏损=1.0)
    d10_alpha_decay: float = 0.5        # Alpha衰减
    d11_cross_correlation: float = 0.5  # 跨策略相关性
    # 综合
    composite_score: float = 0.5        # 11维度加权综合评分



@dataclass(slots=True)
class MarketSnapshot:
    snapshot_id: str
    timestamp: np.datetime64
    symbol: str
    trigger: SnapshotTrigger
    trigger_detail: str = ""

    bar_open: float = 0.0
    bar_high: float = 0.0
    bar_low: float = 0.0
    bar_close: float = 0.0
    bar_vwap: float = 0.0
    bar_volume: int = 0
    bar_atr14: float = 0.0
    extreme_region: str = "NORMAL"
    ma_alignment: str = "过渡态"
    price_vs_mas: Dict[str, float] = field(default_factory=dict)
    price_deviation_sigma: Dict[str, float] = field(default_factory=dict)
    ma_curvatures: Dict[str, float] = field(default_factory=dict)

    cr_directional_bias: float = 0.0
    cr_resonance_strength: float = 0.0
    cr_phase: str = ""
    cr_state_entropy: float = 0.0
    cr_hmm_state: str = ""

    # V5预计算：KL-RPD四维向量
    kl_rpd_k: float = 0.0
    kl_rpd_l: float = 0.5
    kl_rpd_r: float = 0.0
    kl_rpd_d: float = 0.0

    # V5预计算：信号向量
    signal_s1: float = 0.0
    signal_s2: float = 0.0
    signal_s3: float = 0.0
    signal_s4: float = 0.0
    signal_s5: float = 0.0
    signal_s6: float = 0.0

    # V5预计算：衰减信号
    signal_s1_decayed: float = 0.0
    signal_s2_decayed: float = 0.0
    signal_s3_decayed: float = 0.0
    signal_s4_decayed: float = 0.0

    # V5预计算：联动信号
    linkage_s1: float = 0.0
    linkage_s2: float = 0.0
    linkage_s3: float = 0.0
    linkage_s4: float = 0.0

    # V5预计算：L0状态诊断
    l0_raw_state: int = 2
    l0_smoothed_state: int = 2
    l0_state_entropy: float = 0.5

    # V5预计算：HMM状态
    hmm_state_v5: int = 1
    hmm_posterior_low: float = 0.2
    hmm_posterior_normal: float = 0.6
    hmm_posterior_high: float = 0.2

    # V5预计算：趋势评分
    trend_score_short: float = 0.0
    trend_score_medium: float = 0.0
    trend_score_long: float = 0.0
    trend_direction_short: int = 0
    trend_direction_medium: int = 0
    trend_direction_long: int = 0

    # V5预计算：回撤指标
    pullback_pct_peak: float = 0.0
    pullback_pct_entry: float = 0.0
    pullback_pct_ma: float = 0.0
    pullback_pct_atr: float = 0.0

    # V5预计算：超买超卖
    obos_rsi: float = 50.0
    obos_stoch_k: float = 50.0
    obos_cci: float = 0.0
    obos_williams_r: float = -50.0
    obos_signal: float = 0.0

    # V5预计算：TVF六维向量
    tvf_trend: float = 0.0
    tvf_volatility: float = 0.5
    tvf_flow: float = 0.0
    tvf_risk: float = 0.5
    tvf_pullback: float = 0.5
    tvf_entropy: float = 0.5

    # V5预计算：仓位决策
    kelly_fraction: float = 0.1
    position_suggestion: float = 0.0

    # V5预计算：背离反转
    option_moneyness_state: int = 2
    div_future_cross_term: float = 0.0
    div_option_premium_coll: float = 0.0
    div_option_near_itm: float = 0.0
    div_reversal_signal: float = 0.0

    risk_surface_size_mult: float = 1.0
    risk_surface_sl_mult: float = 1.0
    risk_surface_max_hold: float = 300.0
    risk_surface_allow_overnight: bool = False

    five_state_correct_rise: float = 0.0
    five_state_correct_fall: float = 0.0
    five_state_wrong_rise: float = 0.0
    five_state_wrong_fall: float = 0.0
    five_state_other: float = 0.0
    five_state_strength: float = 0.0

    order_flow_imbalance: float = 0.0
    order_flow_consistency: float = 0.0

    strategy_states: List[StrategyStateSnapshot] = field(default_factory=list)
    hft_state: HFTSpecificState = field(default_factory=HFTSpecificState)
    resonance_state: ResonanceSpecificState = field(default_factory=ResonanceSpecificState)
    box_state: BoxSpecificState = field(default_factory=BoxSpecificState)
    spring_state: SpringSpecificState = field(default_factory=SpringSpecificState)
    arbitrage_state: ArbitrageSpecificState = field(default_factory=ArbitrageSpecificState)
    market_making_state: MarketMakingSpecificState = field(default_factory=MarketMakingSpecificState)
    divergence_state: DivergenceSpecificState = field(default_factory=DivergenceSpecificState)
    ecosystem_state: EcosystemState = field(default_factory=EcosystemState)
    shadow_alpha: ShadowAlphaState = field(default_factory=ShadowAlphaState)
    safety_meta: SafetyMetaState = field(default_factory=SafetyMetaState)
    cross_greeks: CrossStrategyGreeks = field(default_factory=CrossStrategyGreeks)

    # 行情寿命快照
    life_expectancy: LifeExpectancySnapshot = field(default_factory=LifeExpectancySnapshot)
    # 周期预测快照
    cycle_prediction: CyclePredictionSnapshot = field(default_factory=CyclePredictionSnapshot)
    # 11维度统一风险评分快照
    risk_dimensions: RiskDimensionScores = field(default_factory=RiskDimensionScores)

    total_portfolio_pnl: float = 0.0
    total_portfolio_position: float = 0.0
    daily_drawdown_pct: float = 0.0
    peak_equity: float = 0.0
    current_equity: float = 0.0

    is_extreme_bar: bool = False

    # FIX-SNAPSHOT-ORDER-INFO: 开仓/平仓订单上下文(position_command_service传入)
    # 原capture()不接受order_info导致TypeError被静默吞掉，快照从未采集
    order_info: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['trigger'] = self.trigger.value
        d['timestamp'] = str(self.timestamp)
        return d

    def to_flat_dict(self) -> Dict[str, Any]:
        d = self.to_dict()
        for ss in d.pop('strategy_states', []):
            prefix = f"strategy_{ss['strategy_id']}_"
            for k, v in ss.items():
                if k == 'strategy_id':
                    continue
                d[f"{prefix}{k}"] = v
        for sub_key in ['hft_state', 'resonance_state', 'box_state', 'spring_state',
                        'arbitrage_state', 'market_making_state', 'divergence_state',
                        'ecosystem_state', 'shadow_alpha', 'safety_meta', 'cross_greeks',
                        'life_expectancy', 'cycle_prediction', 'risk_dimensions']:
            sub = d.pop(sub_key, {})
            for k, v in sub.items():
                d[f"{sub_key}_{k}"] = v
        return d


class MarketSnapshotCollector:
    """
    市场快照采集器 — 覆盖七大策略完整状态
    """

    def __init__(
        self,
        symbol: str,
        max_in_memory: int = 100_000,
        weekly_extreme_window: int = 5,
        monthly_extreme_window: int = 5,
        bars_per_day: int = 240,
    ):
        self._symbol = symbol
        self._max_in_memory = max_in_memory
        self._weekly_window = weekly_extreme_window
        self._monthly_window = monthly_extreme_window
        self._bars_per_day = bars_per_day

        self._snapshots: deque = deque(maxlen=max_in_memory)
        self._weekly_monthly_snapshots: deque = deque(maxlen=max_in_memory)
        self._snapshot_count = 0

        self._weekly_highs: deque = deque(maxlen=10)
        self._weekly_lows: deque = deque(maxlen=10)
        self._monthly_highs: deque = deque(maxlen=10)
        self._monthly_lows: deque = deque(maxlen=10)

        self._current_week_high: float = -np.inf
        self._current_week_low: float = np.inf
        self._current_week_ts: Optional[np.datetime64] = None
        self._current_month_high: float = -np.inf
        self._current_month_low: float = np.inf
        self._current_month_ts: Optional[np.datetime64] = None

        max_window = max(self._weekly_window, self._monthly_window)
        bar_buffer_size = (max_window + 2) * bars_per_day
        self._bar_buffer: deque = deque(maxlen=bar_buffer_size)
        self._current_bar = None
        self._current_bar_kwargs: Dict[str, Any] = {}

        # 行情寿命估计器（由外部注入）'
        self._life_estimator: Any = None

    def _generate_id(self) -> str:
        self._snapshot_count += 1
        return f"snap_{self._snapshot_count:08d}"

    def capture(
        self,
        timestamp: np.datetime64,
        trigger: SnapshotTrigger,
        trigger_detail: str = "",
        bar: Optional[Any] = None,
        cr_output: Optional[Any] = None,
        risk_surface: Optional[Any] = None,
        strategy_states: Optional[List[StrategyStateSnapshot]] = None,
        hft_state: Optional[HFTSpecificState] = None,
        resonance_state: Optional[ResonanceSpecificState] = None,
        box_state: Optional[BoxSpecificState] = None,
        spring_state: Optional[SpringSpecificState] = None,
        arbitrage_state: Optional[ArbitrageSpecificState] = None,
        market_making_state: Optional[MarketMakingSpecificState] = None,
        divergence_state: Optional[DivergenceSpecificState] = None,
        ecosystem_state: Optional[EcosystemState] = None,
        shadow_alpha: Optional[ShadowAlphaState] = None,
        safety_meta: Optional[SafetyMetaState] = None,
        cross_greeks: Optional[CrossStrategyGreeks] = None,
        five_state: Optional[Dict[str, float]] = None,
        order_flow: Optional[Dict[str, float]] = None,
        portfolio_info: Optional[Dict[str, float]] = None,
        life_expectancy: Optional[LifeExpectancySnapshot] = None,
        cycle_prediction: Optional[CyclePredictionSnapshot] = None,
        risk_dimensions: Optional[RiskDimensionScores] = None,
        order_info: Optional[Dict[str, Any]] = None,
    ) -> MarketSnapshot:
        snap = MarketSnapshot(
            snapshot_id=self._generate_id(),
            timestamp=timestamp,
            symbol=self._symbol,
            trigger=trigger,
            trigger_detail=trigger_detail,
        )

        if bar is not None:
            if hasattr(bar, 'open'):
                snap.bar_open = bar.open
                snap.bar_high = bar.high
                snap.bar_low = bar.low
                snap.bar_close = bar.close
                snap.bar_vwap = bar.vwap
                snap.bar_volume = bar.volume
                snap.bar_atr14 = getattr(bar, 'atr14', 0.0)
                if hasattr(bar, 'extreme_region') and hasattr(bar.extreme_region, 'value'):
                    snap.extreme_region = bar.extreme_region.value
                if hasattr(bar, 'ma_alignment') and hasattr(bar.ma_alignment, 'value'):
                    snap.ma_alignment = bar.ma_alignment.value
                snap.price_vs_mas = getattr(bar, 'price_vs_mas', {})
                snap.price_deviation_sigma = getattr(bar, 'price_ma_deviation_sigma', {})
                snap.ma_curvatures = getattr(bar, 'ma_curvatures', {})
                self._fill_v5_columns_from_obj(snap, bar)
            elif isinstance(bar, dict):
                snap.bar_open = bar.get('open', 0)
                snap.bar_high = bar.get('high', 0)
                snap.bar_low = bar.get('low', 0)
                snap.bar_close = bar.get('close', 0)
                snap.bar_vwap = bar.get('vwap', bar.get('close', 0))
                snap.bar_volume = bar.get('volume', 0)
                snap.bar_atr14 = bar.get('atr14', 0)
                self._fill_v5_columns_from_dict(snap, bar)

        if cr_output is not None:
            if hasattr(cr_output, 'directional_bias'):
                snap.cr_directional_bias = cr_output.directional_bias
                snap.cr_resonance_strength = cr_output.resonance_strength
                snap.cr_state_entropy = cr_output.state_entropy
                snap.cr_hmm_state = getattr(cr_output, 'hmm_state', '')
                if hasattr(cr_output, 'phase') and hasattr(cr_output.phase, 'value'):
                    snap.cr_phase = cr_output.phase.value

        if risk_surface is not None:
            if hasattr(risk_surface, 'size_multiplier'):
                snap.risk_surface_size_mult = risk_surface.size_multiplier
                snap.risk_surface_sl_mult = risk_surface.stop_loss_multiplier
                snap.risk_surface_max_hold = risk_surface.max_hold_seconds
                snap.risk_surface_allow_overnight = getattr(risk_surface, 'allow_overnight', False)

        if strategy_states is not None:
            snap.strategy_states = strategy_states
        if hft_state is not None:
            snap.hft_state = hft_state
        if resonance_state is not None:
            snap.resonance_state = resonance_state
        if box_state is not None:
            snap.box_state = box_state
        if spring_state is not None:
            snap.spring_state = spring_state
        if arbitrage_state is not None:
            snap.arbitrage_state = arbitrage_state
        if market_making_state is not None:
            snap.market_making_state = market_making_state
        if divergence_state is not None:
            snap.divergence_state = divergence_state
        if ecosystem_state is not None:
            snap.ecosystem_state = ecosystem_state
        if shadow_alpha is not None:
            snap.shadow_alpha = shadow_alpha
        if safety_meta is not None:
            snap.safety_meta = safety_meta
        if cross_greeks is not None:
            snap.cross_greeks = cross_greeks

        # 行情寿命快照填充
        if life_expectancy is not None:
            snap.life_expectancy = life_expectancy
        else:
            # 自动从cr_output的hmm_state推断寿命（如果寿命估计器可用）'
            self._auto_fill_life_expectancy(snap)

        # 周期预测快照填充
        if cycle_prediction is not None:
            snap.cycle_prediction = cycle_prediction
        else:
            # 自动从cr_output推断周期预测
            self._auto_fill_cycle_prediction(snap)

        # 11维度风险评分快照填充
        if risk_dimensions is not None:
            snap.risk_dimensions = risk_dimensions
        else:
            # 从已有快照字段自动填充
            self._auto_fill_risk_dimensions(snap)

        if five_state is not None:
            snap.five_state_correct_rise = five_state.get('correct_rise_pct', 0)
            snap.five_state_correct_fall = five_state.get('correct_fall_pct', 0)
            snap.five_state_wrong_rise = five_state.get('wrong_rise_pct', 0)
            snap.five_state_wrong_fall = five_state.get('wrong_fall_pct', 0)
            snap.five_state_other = five_state.get('other_pct', 0)
            snap.five_state_strength = five_state.get('strength', 0)

        if order_flow is not None:
            snap.order_flow_imbalance = order_flow.get('imbalance', 0)
            snap.order_flow_consistency = order_flow.get('consistency', 0)

        if portfolio_info is not None:
            snap.total_portfolio_pnl = portfolio_info.get('total_pnl', 0)
            snap.total_portfolio_position = portfolio_info.get('total_position', 0)
            snap.daily_drawdown_pct = portfolio_info.get('daily_drawdown_pct', 0)
            snap.peak_equity = portfolio_info.get('peak_equity', 0)
            snap.current_equity = portfolio_info.get('current_equity', 0)

        # FIX-SNAPSHOT-ORDER-INFO: 开仓/平仓订单上下文填充
        if order_info is not None:
            snap.order_info = order_info

        self._snapshots.append(snap)
        self._maybe_auto_export()
        return snap

    _V5_DICT_COLUMNS: List[str] = [
        "kl_rpd_k", "kl_rpd_l", "kl_rpd_r", "kl_rpd_d",
        "signal_s1", "signal_s2", "signal_s3", "signal_s4", "signal_s5", "signal_s6",
        "signal_s1_decayed", "signal_s2_decayed", "signal_s3_decayed", "signal_s4_decayed",
        "linkage_s1", "linkage_s2", "linkage_s3", "linkage_s4",
        "l0_raw_state", "l0_smoothed_state", "l0_state_entropy",
        "hmm_state", "hmm_posterior_low", "hmm_posterior_normal", "hmm_posterior_high",
        "trend_score_short", "trend_score_medium", "trend_score_long",
        "trend_direction_short", "trend_direction_medium", "trend_direction_long",
        "pullback_pct_peak", "pullback_pct_entry", "pullback_pct_ma", "pullback_pct_atr",
        "obos_rsi", "obos_stoch_k", "obos_cci", "obos_williams_r", "obos_signal",
        "tvf_trend", "tvf_volatility", "tvf_flow", "tvf_risk", "tvf_pullback", "tvf_entropy",
        "kelly_fraction", "position_suggestion",
        "option_moneyness_state", "div_future_cross_term", "div_option_premium_coll",
        "div_option_near_itm", "div_reversal_signal",
    ]

    def _fill_v5_columns_from_obj(self, snap: MarketSnapshot, bar: Any) -> None:
        for col in self._V5_DICT_COLUMNS:
            val = getattr(bar, col, None)
            if val is not None:
                if col == "hmm_state":
                    snap.hmm_state_v5 = int(val)
                else:
                    setattr(snap, col, val)

    def _fill_v5_columns_from_dict(self, snap: MarketSnapshot, bar: dict) -> None:
        for col in self._V5_DICT_COLUMNS:
            val = bar.get(col)
            if val is not None:
                if col == "hmm_state":
                    snap.hmm_state_v5 = int(val)
                else:
                    setattr(snap, col, val)

    def _auto_fill_life_expectancy(self, snap: MarketSnapshot) -> None:
        """自动从寿命估计器填充行情寿命快照"""
        hmm_state = snap.cr_hmm_state
        if not hmm_state:
            return
        try:
            if not hasattr(self, '_life_estimator') or self._life_estimator is None:
                return
            life = self._life_estimator.get_life_expectancy(hmm_state)
            if life is not None and life.is_valid():
                snap.life_expectancy = LifeExpectancySnapshot(
                    hmm_state=hmm_state,
                    duration_p25=life.duration.get('p25', 0),
                    duration_p50=life.duration.get('p50', 0),
                    duration_p75=life.duration.get('p75', 0),
                    duration_p99=life.duration.get('p99', 0),
                    magnitude_p50=life.magnitude.get('p50', 0),
                    magnitude_p75=life.magnitude.get('p75', 0),
                    sample_count=life.sample_count,
                    decay_r_squared=life.decay_r_squared,
                    degradation_level=life.degradation_level,
                    is_valid=True,
                )
        except (ValueError, KeyError, TypeError, AttributeError) as _life_err:  # [R27-AUDIT] P2修复: 添加日志
            logging.debug("[MarketSnapshotCollector] life snapshot auto-fill failed: %s", _life_err)

    def _auto_fill_cycle_prediction(self, snap: MarketSnapshot) -> None:
        """自动从cr_output推断周期预测快照"""
        # 从resonance_state获取HMM后验和趋势信息
        if snap.resonance_state.hmm_posterior != (0.33, 0.34, 0.33):
            snap.cycle_prediction.hmm_posterior = snap.resonance_state.hmm_posterior
        if snap.resonance_state.trend_scores != (0.0, 0.0, 0.0):
            snap.cycle_prediction.trend_scores_short = snap.resonance_state.trend_scores[0]
            snap.cycle_prediction.trend_scores_medium = snap.resonance_state.trend_scores[1]
            snap.cycle_prediction.trend_scores_long = snap.resonance_state.trend_scores[2]
        if snap.resonance_state.trend_directions != (0.0, 0.0, 0.0):
            snap.cycle_prediction.trend_directions_short = snap.resonance_state.trend_directions[0]
            snap.cycle_prediction.trend_directions_medium = snap.resonance_state.trend_directions[1]
            snap.cycle_prediction.trend_directions_long = snap.resonance_state.trend_directions[2]
        # 从寿命p75估算相位剩余时间
        if snap.life_expectancy.is_valid and snap.life_expectancy.duration_p75 > 0:
            snap.cycle_prediction.phase_remaining_estimate = snap.life_expectancy.duration_p75
        # 从cr共振字段填充
        if snap.cr_resonance_strength > 0:
            snap.cycle_prediction.resonance_strength = snap.cr_resonance_strength
        if snap.cr_state_entropy != 0.5:
            snap.cycle_prediction.state_entropy = snap.cr_state_entropy

    def _auto_fill_risk_dimensions(self, snap: MarketSnapshot) -> None:
        """从快照已有字段自动填充11维度风险评分"""
        rd = snap.risk_dimensions
        # D1 状态强度 — 从五态强度推断
        rd.d1_state_strength = snap.five_state_strength if snap.five_state_strength > 0 else 0.5
        # D2 订单流
        rd.d2_order_flow = (snap.order_flow_consistency + 1.0) / 2.0
        # D3 行情寿命
        rd.d3_life_expectancy = 1.0 if snap.life_expectancy.is_valid and snap.life_expectancy.degradation_level == 0 else (
            0.7 if snap.life_expectancy.degradation_level == 1 else (
            0.4 if snap.life_expectancy.degradation_level == 2 else (
            0.2 if snap.life_expectancy.degradation_level == 3 else 0.5)))
        # D4 周期共振
        rd.d4_cycle_resonance = snap.cr_resonance_strength if snap.cr_resonance_strength > 0 else 0.5
        # D5 相位质量
        phase_map = {'RELEASE': 1.0, 'CHARGE': 0.7, 'EXHAUST': 0.4, 'CHAOS': 0.2}
        rd.d5_phase_quality = phase_map.get(snap.cr_phase, 0.5)
        # D6 Greeks使用率 — 从cross_greeks推断
        rd.d6_greeks_usage = 0.5  # 需要dashboard数据，默认中性
        # D7 连亏 — 默认无亏损
        rd.d7_consecutive_loss = 1.0
        # D8 非对称回撤
        rd.d8_asymmetric_drawdown = 0.5
        # D9 三角验证 — 需要外部传入
        rd.d9_tri_validation = 0.5
        # D10 Alpha衰减 — 从shadow_alpha推断
        if hasattr(snap.shadow_alpha, 'alpha_ratio') and snap.shadow_alpha.alpha_ratio != 0:
            ar = snap.shadow_alpha.alpha_ratio
            rd.d10_alpha_decay = 1.0 if ar > 1.0 else (0.7 if ar > 0.5 else (0.4 if ar > 0 else 0.1))
        # D11 跨策略相关性
        rd.d11_cross_correlation = 0.5

        # 背离反转信号增强: 当三层背离一致时, 降低d1状态强度置信度(趋势可能反转)
        if snap.divergence_state.three_layer_consistent:
            div_signal = abs(snap.divergence_state.div_reversal_signal)
            if div_signal > 0.3:
                rd.d1_state_strength = max(0.2, rd.d1_state_strength - div_signal * 0.3)

    def set_life_estimator(self, estimator: Any) -> None:
        """注入行情寿命估计器（由task_scheduler或策略引擎在回测/实盘启动时调用）"""
        self._life_estimator = estimator

    def capture_signal_point(self, timestamp, **kwargs) -> MarketSnapshot:
        return self.capture(timestamp, SnapshotTrigger.SIGNAL_GENERATED, **kwargs)

    def capture_order_event(self, timestamp, event_type: SnapshotTrigger, **kwargs) -> MarketSnapshot:
        return self.capture(timestamp, event_type, **kwargs)

    def capture_safety_meta_trigger(self, timestamp, detail: str = "", **kwargs) -> MarketSnapshot:
        return self.capture(timestamp, SnapshotTrigger.SAFETY_META_TRIGGER, trigger_detail=detail, **kwargs)

    def capture_ecosystem_switch(self, timestamp, detail: str = "", **kwargs) -> MarketSnapshot:
        return self.capture(timestamp, SnapshotTrigger.ECOSYSTEM_SWITCH, trigger_detail=detail, **kwargs)

    def capture_spring_state_change(self, timestamp, detail: str = "", **kwargs) -> MarketSnapshot:
        return self.capture(timestamp, SnapshotTrigger.SPRING_STATE_CHANGE, trigger_detail=detail, **kwargs)

    def capture_phase_transition(self, timestamp, detail: str = "", **kwargs) -> MarketSnapshot:
        return self.capture(timestamp, SnapshotTrigger.PHASE_TRANSITION, trigger_detail=detail, **kwargs)

    def capture_divergence_signal(self, timestamp, detail: str = "", **kwargs) -> MarketSnapshot:
        """捕获背离反转信号时刻的快照"""
        return self.capture(timestamp, SnapshotTrigger.DIVERGENCE_REVERSAL, trigger_detail=detail, **kwargs)

    def process_bar_for_weekly_monthly(self, bar, **kwargs) -> List[MarketSnapshot]:
        ts = bar.timestamp if hasattr(bar, 'timestamp') else (bar.get('timestamp') if hasattr(bar, 'get') else None)
        high = bar.high if hasattr(bar, 'high') else (bar.get('high', 0) if hasattr(bar, 'get') else 0)
        low = bar.low if hasattr(bar, 'low') else (bar.get('low', 0) if hasattr(bar, 'get') else 0)

        self._bar_buffer.append(bar)
        self._current_bar = bar
        self._current_bar_kwargs = kwargs

        new_snapshots = []

        if isinstance(ts, np.datetime64):
            ts_dt = ts.astype('datetime64[D]')
        else:
            ts_dt = np.datetime64(ts, 'D')

        from datetime import datetime as _dt
        try:
            ts_py = ts_dt.astype(_dt)
            weekday = ts_py.weekday()
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"周期计算异常(ts={ts_dt}): {e}, 回退weekday=0")
            weekday = 0
        week_key = ts_dt - np.timedelta64(weekday, 'D')
        month_key = ts_dt.astype('datetime64[M]')

        week_ended = self._current_week_ts is not None and week_key != self._current_week_ts
        month_ended = self._current_month_ts is not None and month_key != self._current_month_ts

        if week_ended:
            if self._current_week_high > -np.inf:
                extreme_info = {'ts': self._current_week_ts, 'value': self._current_week_high, 'type': 'weekly_high'}
                self._weekly_highs.append(extreme_info)
                window_snaps = self._capture_window_snapshots(
                    extreme_info, SnapshotTrigger.WEEKLY_HIGH, self._weekly_window, **kwargs
                )
                new_snapshots.extend(window_snaps)
            if self._current_week_low < np.inf:
                extreme_info = {'ts': self._current_week_ts, 'value': self._current_week_low, 'type': 'weekly_low'}
                self._weekly_lows.append(extreme_info)
                window_snaps = self._capture_window_snapshots(
                    extreme_info, SnapshotTrigger.WEEKLY_LOW, self._weekly_window, **kwargs
                )
                new_snapshots.extend(window_snaps)
            self._current_week_high = high
            self._current_week_low = low
        else:
            self._current_week_high = max(self._current_week_high, high)
            self._current_week_low = min(self._current_week_low, low)
        self._current_week_ts = week_key

        if month_ended:
            if self._current_month_high > -np.inf:
                extreme_info = {'ts': self._current_month_ts, 'value': self._current_month_high, 'type': 'monthly_high'}
                self._monthly_highs.append(extreme_info)
                window_snaps = self._capture_window_snapshots(
                    extreme_info, SnapshotTrigger.MONTHLY_HIGH, self._monthly_window, **kwargs
                )
                new_snapshots.extend(window_snaps)
            if self._current_month_low < np.inf:
                extreme_info = {'ts': self._current_month_ts, 'value': self._current_month_low, 'type': 'monthly_low'}
                self._monthly_lows.append(extreme_info)
                window_snaps = self._capture_window_snapshots(
                    extreme_info, SnapshotTrigger.MONTHLY_LOW, self._monthly_window, **kwargs
                )
                new_snapshots.extend(window_snaps)
            self._current_month_high = high
            self._current_month_low = low
        else:
            self._current_month_high = max(self._current_month_high, high)
            self._current_month_low = min(self._current_month_low, low)
        self._current_month_ts = month_key

        return new_snapshots

    def _capture_window_snapshots(
        self,
        extreme_info: Dict[str, Any],
        trigger: SnapshotTrigger,
        window_days: int,
        **kwargs,
    ) -> List[MarketSnapshot]:
        extreme_ts = extreme_info['ts']
        extreme_value = extreme_info['value']
        extreme_type = extreme_info['type']

        if not self._bar_buffer:
            return []

        if isinstance(extreme_ts, np.datetime64):
            extreme_ts_day = extreme_ts.astype('datetime64[D]')
        else:
            extreme_ts_day = np.datetime64(extreme_ts, 'D')

        window_start = extreme_ts_day - np.timedelta64(window_days, 'D')
        window_end = extreme_ts_day + np.timedelta64(window_days, 'D')

        captured = []
        for bar in self._bar_buffer:
            bar_ts = bar.timestamp if hasattr(bar, 'timestamp') else bar.get('timestamp')
            if bar_ts is None:
                continue
            if isinstance(bar_ts, np.datetime64):
                bar_ts_day = bar_ts.astype('datetime64[D]')
            else:
                bar_ts_day = np.datetime64(bar_ts, 'D')

            if window_start <= bar_ts_day <= window_end:
                days_from_extreme = int((bar_ts_day - extreme_ts_day) / np.timedelta64(1, 'D'))
                if days_from_extreme < 0:
                    day_label = f"前{abs(days_from_extreme)}天"
                elif days_from_extreme > 0:
                    day_label = f"后{days_from_extreme}天"
                else:
                    day_label = "当天"

                bar_high = bar.high if hasattr(bar, 'high') else bar.get('high', 0)
                is_extreme = abs(bar_high - extreme_value) < abs(extreme_value) * 1e-6

                detail = f"{extreme_type}={extreme_value:.2f}, 窗口={day_label}"
                snap = self.capture(
                    timestamp=bar_ts,
                    trigger=trigger,
                    trigger_detail=detail,
                    bar=bar,
                    **{k: v for k, v in kwargs.items() if k != 'bar'},
                )
                snap.is_extreme_bar = is_extreme
                self._weekly_monthly_snapshots.append(snap)
                captured.append(snap)

        logger.info(
            f"[周月窗口] {extreme_type}={extreme_value:.2f}, "
            f"窗口={window_days}天, 捕获={len(captured)}条快照"
        )
        return captured

    def get_all_snapshots(self) -> List[MarketSnapshot]:
        return list(self._snapshots)

    def get_weekly_monthly_snapshots(self) -> List[MarketSnapshot]:
        return list(self._weekly_monthly_snapshots)

    def get_weekly_monthly_snapshot_windows(self, snapshot_id: str = "") -> List[MarketSnapshot]:
        if snapshot_id:
            return [s for s in self._weekly_monthly_snapshots if s.snapshot_id == snapshot_id]
        return list(self._weekly_monthly_snapshots)

    def get_snapshots_by_trigger(self, trigger: SnapshotTrigger) -> List[MarketSnapshot]:
        return [s for s in self._snapshots if s.trigger == trigger]

    def get_snapshots_in_range(self, start_ts, end_ts) -> List[MarketSnapshot]:
        return [s for s in self._snapshots if start_ts <= s.timestamp <= end_ts]

    def get_statistics(self) -> Dict[str, Any]:
        trigger_counts = {}
        for s in self._snapshots:
            key = s.trigger.value
            trigger_counts[key] = trigger_counts.get(key, 0) + 1
        wm_trigger_counts = {}
        for s in self._weekly_monthly_snapshots:
            key = s.trigger.value
            wm_trigger_counts[key] = wm_trigger_counts.get(key, 0) + 1
        return {
            "symbol": self._symbol,
            "total_snapshots": len(self._snapshots),
            "trigger_counts": trigger_counts,
            "weekly_monthly_snapshot_count": len(self._weekly_monthly_snapshots),
            "weekly_monthly_trigger_counts": wm_trigger_counts,
            "weekly_extremes_found": len(self._weekly_highs) + len(self._weekly_lows),
            "monthly_extremes_found": len(self._monthly_highs) + len(self._monthly_lows),
            "bar_buffer_size": len(self._bar_buffer),
        }

    def export_to_duckdb(self, db_path: str, table_name: str = "market_snapshots") -> None:
        try:
            from ali2026v3_trading.data.data_access import get_data_access
            from ali2026v3_trading.data.db_adapter import connect
        except ImportError:
            logger.error("导出DuckDB需要duckdb库，请执行: pip install duckdb")
            return
        try:
            import pandas as pd
        except ImportError:
            logger.error("导出DuckDB需要pandas库，请执行: pip install pandas")
            return
        if not self._snapshots:
            return
        flat_dicts = [snap.to_flat_dict() for snap in self._snapshots]
        df = pd.DataFrame(flat_dicts)
        con = connect(db_path)
        try:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            logger.info(f"已导出{len(df)}条快照到 {db_path}.{table_name}")
        finally:
            con.close()

    def export_to_parquet(self, file_path: str) -> None:
        try:
            import pandas as pd
        except ImportError:
            logger.error("导出Parquet需要pandas库，请执行: pip install pandas")
            return
        if not self._snapshots:
            return
        flat_dicts = [snap.to_flat_dict() for snap in self._snapshots]
        df = pd.DataFrame(flat_dicts)
        from ali2026v3_trading.infra.serialization_utils import safe_dataframe_to_parquet
        safe_dataframe_to_parquet(df, file_path, preserve_index=False)

    _auto_export_interval_sec = 300
    _auto_export_counter = 0
    _auto_export_threshold = 100

    def _maybe_auto_export(self) -> None:
        self._auto_export_counter += 1
        if self._auto_export_counter < self._auto_export_threshold:
            return
        self._auto_export_counter = 0
        try:
            from ali2026v3_trading.config._params_canary_env import DEFAULT_LOG_DIR
            import os
            db_path = os.path.join(DEFAULT_LOG_DIR, 'market_snapshots_auto.duckdb')
            self.export_to_duckdb(db_path)
            logger.info("[AutoExport] 已自动导出快照到 %s", db_path)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.debug("[AutoExport] 自动导出跳过: %s", e)
