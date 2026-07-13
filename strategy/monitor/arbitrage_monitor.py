# MODULE_ID: S5-ARB-MONITOR-V1
"""S5套利策略实盘模拟监控模块 V1（顶级基金统计套利标准）

核心升级（对比旧版）
-------------------
1. 真实配对交易支持：leg_a/leg_b 双价格序列，而非单合约 deviation。
2. 动态 Hedge Ratio：滚动 OLS + Kalman Filter 双模式，旧版固定返回 1.0。
3. 严格协整检验：基于 Engle-Granger 两步法的简化 ADF 检验，旧版仅用方差比代理。
4. 交易成本模型：手续费、滑点、冲击成本显式建模，信号阈值经成本调整。
5. 风控硬约束：最大敞口、半衰期限制、结构性断点止损、Z-score 止损。
6. 多时间框架：支持 tick/1min/5min 多周期残差融合。

约束
----
- 不参与资金分配，不调用 OrderService.send_order，不调用 route_capital。
- 仅生成模拟信号 + JSON 快照，供其他策略参考方向。
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

from ali2026v3_trading.infra._helpers import get_logger
from ali2026v3_trading.infra.shared_utils import (
    CHINA_TZ as _CHINA_TZ,
    generate_prefixed_id,
)
from ali2026v3_trading.infra.resilience import safe_divide

__all__ = ['ArbitrageMonitor', 'SimulatedArbitrageSignal', 'get_arbitrage_monitor']

logger = get_logger(__name__)


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class SimulatedArbitrageSignal:
    """模拟套利信号（实盘模拟，不下单）。"""

    signal_id: str
    instrument_id: str          # 主合约/组合标识
    leg_a: str = ''             # 配对腿A
    leg_b: str = ''             # 配对腿B
    direction: str = 'BUY'      # 'BUY' / 'SELL'
    signal_type: str = 'OPEN_LONG'
    deviation_bps: float = 0.0
    confidence: float = 0.0
    entry_price: float = 0.0
    spread_convergence_rate: float = 0.0
    quality_score: float = 0.0
    timestamp: float = 0.0
    simulated: bool = True
    capital_allocation: float = 0.0
    hft_consumed: bool = False
    source: str = 's5_arbitrage_monitor_v1'

    # 顶级基金统计套利字段
    z_score: float = 0.0
    half_life_sec: float = 0.0
    hedge_ratio: float = 1.0
    log_spread: float = 0.0
    cointegration_pvalue: float = 1.0
    structural_break_flag: bool = False
    expected_profit_per_unit: float = 0.0   # 成本调整后预期盈利
    transaction_cost_bps: float = 0.0       # 双边交易成本
    # FIX-20260713-S5-MULTISRC: 多源数据采集字段
    order_book_imbalance_a: float = 0.0     # leg_a 订单簿不平衡 [-1,1]
    order_book_imbalance_b: float = 0.0     # leg_b 订单簿不平衡 [-1,1]
    open_interest_a: float = 0.0            # leg_a 持仓量
    open_interest_b: float = 0.0            # leg_b 持仓量
    oi_trend_a: str = "stable"              # leg_a 持仓量趋势
    oi_trend_b: str = "stable"              # leg_b 持仓量趋势
    vwap_a: float = 0.0                     # leg_a VWAP
    vwap_b: float = 0.0                     # leg_b VWAP
    data_source_mask: int = 0               # 活跃数据源掩码

    # FIX-20260713-S5-V2ABSORB: V2 顶级基金组件字段
    market_regime: str = "TRENDING"           # HMM市场状态
    regime_confidence: float = 0.0            # 状态置信度
    multi_tf_zscore: Dict[str, float] = field(default_factory=dict)  # 多时间框架Z-score
    fused_zscore: float = 0.0                 # 融合Z-score
    adaptive_z_open: float = 0.0              # 自适应开仓阈值
    adaptive_z_close: float = 0.0             # 自适应平仓阈值
    kelly_fraction: float = 0.0               # Kelly分数(信息性)
    optimal_position_units: float = 0.0       # Kelly建议仓位(信息性)
    cv_stability_score: float = 0.5           # 交叉验证稳定性
    signal_age_sec: float = 0.0               # 信号年龄
    decayed_confidence: float = 0.0           # 衰减后置信度
    market_impact_bps: float = 0.0            # 市场冲击成本
    queue_position_prob: float = 0.85         # 队列位置概率
    total_execution_cost_bps: float = 0.0     # 总执行成本
    net_expected_profit_bps: float = 0.0      # 净预期盈利

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ArbitragePairConfig:
    """套利对配置。"""

    leg_a: str
    leg_b: str
    hedge_ratio: float = 1.0
    beta_lookback: int = 60
    z_open: float = 2.0
    z_close: float = 0.5
    z_stop: float = 3.5
    max_half_life_sec: float = 300.0
    min_half_life_sec: float = 5.0
    coint_pvalue_threshold: float = 0.05
    transaction_cost_bps: float = 2.0       # 双边总成本（手续费+滑点）
    min_expected_profit_bps: float = 1.0    # 成本调整后最小盈利阈值
    max_position_units: float = 10.0
    # FIX-20260713-S5-V2ABSORB: V2 配置字段
    min_cv_stability: float = 0.5             # 最小交叉验证稳定性
    impact_coefficient: float = 0.1           # 市场冲击系数


# ============================================================================
# 数学工具
# ============================================================================

class _KalmanHedgeRatio:
    """简化 Kalman Filter 估计动态 hedge ratio。

    状态空间
    --------
    观测方程:  y_t = β_t * x_t + ε_t
    状态方程:  β_t = β_{t-1} + η_t

    使用固定观测方差和过程方差，适合无外部依赖场景。
    """

    def __init__(self, trans_var: float = 1e-5, obs_var: float = 1e-3) -> None:
        self.beta: float = 1.0
        self.P: float = 1.0
        self.trans_var = trans_var
        self.obs_var = obs_var

    def update(self, x: float, y: float) -> float:
        if x == 0:
            return self.beta
        # 预测
        P_pred = self.P + self.trans_var
        # 更新
        K = P_pred * x / (x * x * P_pred + self.obs_var)
        self.beta = self.beta + K * (y - self.beta * x)
        self.P = (1 - K * x) * P_pred
        return self.beta


def _simple_adf_pvalue(residuals: List[float]) -> float:
    """简化 ADF 检验：回归 Δe_t = α + ρ*e_{t-1} + ε_t，返回 ρ 对应的近似 p-value。

    没有 statsmodels，使用 t-stat 代理：
        t = ρ_hat / SE(ρ_hat)
    平稳序列 ρ<0 且 |t| 大；随机游走 ρ≈0。
    """
    n = len(residuals)
    if n < 10:
        return 1.0
    # 构造 Δe_t 和 e_{t-1}
    y = [residuals[i] - residuals[i - 1] for i in range(1, n)]
    x = [residuals[i - 1] for i in range(1, n)]
    n2 = len(y)
    mean_x = sum(x) / n2
    mean_y = sum(y) / n2
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n2))
    den = sum((x[i] - mean_x) ** 2 for i in range(n2))
    if den <= 0:
        return 1.0
    rho_hat = num / den
    # 残差标准差
    a = mean_y - rho_hat * mean_x
    sse = sum((y[i] - a - rho_hat * x[i]) ** 2 for i in range(n2))
    se = math.sqrt(sse / max(n2 - 2, 1)) / math.sqrt(den)
    if se <= 0:
        return 1.0
    t_stat = rho_hat / se
    # ρ_hat 越负（均值回归越强），p-value 越小
    # t_stat 越小（越负），越平稳
    p_value = max(0.001, min(1.0, math.exp(t_stat + 0.5)))
    return p_value


def _rolling_ols_beta(x: List[float], y: List[float]) -> float:
    """滚动 OLS: y = βx，返回 β。"""
    n = len(x)
    if n < 5:
        return 1.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    if den <= 0:
        return 1.0
    return num / den


# ============================================================================
# FIX-20260713-S5-V2ABSORB: V2 顶级基金组件（批判式吸收自 arbitrage_monitor_v2）
# ============================================================================

class _MarketRegimeClassifier:
    """简化 HMM 市场状态分类器。
    3个隐状态: TRENDING(协整稳定), MEAN_REVERTING(强均值回归), REGIME_BREAK(结构断点)
    对标: Renaissance Technologies HMM-based regime detection。
    """

    def __init__(self) -> None:
        self.trans_probs = [
            [0.90, 0.08, 0.02],
            [0.15, 0.80, 0.05],
            [0.05, 0.10, 0.85],
        ]
        self.state_probs = [0.7, 0.2, 0.1]
        self._z_history: Deque[float] = deque(maxlen=60)

    def update(self, z_score: float, half_life_sec: float,
               pvalue: float, is_break: bool) -> Tuple[str, float]:
        self._z_history.append(z_score)
        if is_break:
            obs_probs = [0.05, 0.10, 0.85]
        elif pvalue < 0.05 and 0 < half_life_sec < 600:
            if abs(z_score) > 3.0:
                obs_probs = [0.15, 0.80, 0.05]
            else:
                obs_probs = [0.85, 0.10, 0.05]
        else:
            obs_probs = [0.20, 0.30, 0.50]
        new_probs = [0.0, 0.0, 0.0]
        for j in range(3):
            pred = sum(self.state_probs[i] * self.trans_probs[i][j] for i in range(3))
            new_probs[j] = pred * obs_probs[j]
        total = sum(new_probs)
        if total > 0:
            new_probs = [p / total for p in new_probs]
        self.state_probs = new_probs
        max_idx = max(range(3), key=lambda i: self.state_probs[i])
        regimes = ['TRENDING', 'MEAN_REVERTING', 'REGIME_BREAK']
        return regimes[max_idx], self.state_probs[max_idx]

    def get_state_probs(self) -> Dict[str, float]:
        regimes = ['TRENDING', 'MEAN_REVERTING', 'REGIME_BREAK']
        return {regimes[i]: self.state_probs[i] for i in range(3)}


class _KellyPositionSizer:
    """OU过程驱动的Kelly Criterion仓位计算。
    f* = (μ - r) / σ², 对于OU过程: μ = κ * |Z| * σ
    对标: Renaissance Technologies systematic Kelly sizing。
    注意: S5 capital_allocation=0.0, Kelly仅作为信号质量指标, 非实际仓位。
    """

    def __init__(self, max_fraction: float = 0.25) -> None:
        self.max_fraction = max_fraction

    def compute(self, z_score: float, half_life_sec: float,
                spread_volatility: float, max_units: float) -> float:
        if half_life_sec <= 0 or spread_volatility <= 0:
            return 0.0
        kappa = math.log(2) / half_life_sec if half_life_sec > 0 else 0.0
        expected_return = kappa * abs(z_score) * spread_volatility
        risk = spread_volatility ** 2
        if risk <= 0:
            return 0.0
        kelly_fraction = expected_return / risk
        kelly_fraction = max(0.0, min(self.max_fraction, kelly_fraction))
        optimal_units = kelly_fraction * max_units
        return max(0.0, min(max_units, optimal_units))


class _CVStabilityScorer:
    """样本内/外交叉验证稳定性评分。
    对标: Two Sigma walk-forward cross-validation。
    """

    def __init__(self, train_ratio: float = 0.7) -> None:
        self.train_ratio = train_ratio
        self._cv_scores: Dict[str, float] = {}

    def score(self, pair_id: str, log_spread_history: List[float]) -> float:
        n = len(log_spread_history)
        if n < 30:
            self._cv_scores[pair_id] = 0.5
            return 0.5
        split = int(n * self.train_ratio)
        train = log_spread_history[:split]
        test = log_spread_history[split:]
        train_mean = sum(train) / len(train)
        train_std = math.sqrt(sum((x - train_mean) ** 2 for x in train) / len(train))
        test_mean = sum(test) / len(test)
        if train_std <= 0:
            self._cv_scores[pair_id] = 0.5
            return 0.5
        deviation = abs(test_mean - train_mean) / train_std
        stability = max(0.0, min(1.0, 1.0 - deviation / 3.0))
        self._cv_scores[pair_id] = stability
        return stability

    def get_score(self, pair_id: str) -> float:
        return self._cv_scores.get(pair_id, 0.5)


def _square_root_impact(volume: float, daily_volume: float, volatility: float,
                        coefficient: float = 0.1) -> float:
    """平方根市场冲击模型。Impact = coefficient * σ * sqrt(Q / ADV)
    对标: Almgren-Chriss (2005)。仅作信息性成本指标。"""
    if daily_volume <= 0 or volume <= 0:
        return 0.0
    return coefficient * volatility * math.sqrt(volume / daily_volume)


# ============================================================================
# ArbitrageMonitor V1
# ============================================================================

class ArbitrageMonitor:
    """S5套利策略实盘模拟监控模块 V1。"""

    STRATEGY_ID = 's5_arbitrage_v1'
    STRATEGY_TYPE = 'arbitrage'
    CAPITAL_ALLOCATION = 0.0
    SNAPSHOT_SUBDIR = os.path.join('logs', 'arbitrage_monitor')
    CONVERGENCE_WINDOW_SEC = 60.0
    MAX_HISTORY_SIGNALS = 500
    # FIX-20260713-S5-V2ABSORB: V2 类常量
    TF_WEIGHTS = {'tick': 0.50, 'm1': 0.30, 'm5': 0.20}
    TF_WINDOWS = {'tick': 60, 'm1': 20, 'm5': 10}
    SIGNAL_DECAY_HALF_LIFE_SEC = 30.0

    def __init__(self, base_dir: Optional[str] = None) -> None:
        self._base_dir = base_dir or self._detect_base_dir()
        self._snapshot_dir = os.path.join(self._base_dir, self.SNAPSHOT_SUBDIR)
        os.makedirs(self._snapshot_dir, exist_ok=True)

        self._lock = threading.RLock()

        # 配对配置与状态
        self._pairs: Dict[str, ArbitragePairConfig] = {}
        self._price_history: Dict[str, List[Tuple[float, float]]] = {}  # instr -> [(ts, price)]
        self._kalman: Dict[str, _KalmanHedgeRatio] = {}
        self._hedge_ratios: Dict[str, float] = {}

        # 残差/对数价差历史
        self._log_spread_history: Dict[str, List[float]] = {}
        self._cointegration_pvalues: Dict[str, float] = {}
        self._half_life_sec: Dict[str, float] = {}
        self._last_z_score: Dict[str, float] = {}
        self._structural_break_count: int = 0

        # FIX-20260713-S5-MULTISRC: 多源数据采集状态（不限于期权五态/HFT管道）
        # 每腿的订单簿/持仓量/VWAP 状态
        self._leg_bid_volume: Dict[str, float] = {}       # instr -> bid_volume1
        self._leg_ask_volume: Dict[str, float] = {}       # instr -> ask_volume1
        self._leg_ob_imbalance: Dict[str, float] = {}     # instr -> 订单簿不平衡
        self._leg_open_interest: Dict[str, float] = {}    # instr -> 持仓量
        self._leg_prev_oi: Dict[str, float] = {}          # instr -> 上次持仓量
        self._leg_oi_trend: Dict[str, str] = {}           # instr -> OI趋势
        self._leg_vwap: Dict[str, float] = {}             # instr -> VWAP
        self._leg_cum_turnover: Dict[str, float] = {}     # instr -> 累计成交额
        self._leg_cum_volume: Dict[str, float] = {}       # instr -> 累计成交量
        self._leg_trade_direction: Dict[str, int] = {}    # instr -> 真实成交方向
        self._data_source_mask: int = 0

        # 兼容旧版单合约模式
        self._last_hft_signal: Optional[Dict[str, Any]] = None
        self._last_hft_signal_ts: float = 0.0
        # FIX-20260713-S5-V1-CRIT: 记录配对信号生成时间，使信号衰减真正生效
        self._last_pair_signal_ts: float = 0.0
        self._deviation_history: Dict[str, List[Dict[str, Any]]] = {}
        self._simulated_signals: List[SimulatedArbitrageSignal] = []

        # FIX-20260713-S5-V2ABSORB: V2 顶级基金组件
        self._regime_classifiers: Dict[str, _MarketRegimeClassifier] = {}
        self._kelly_sizer = _KellyPositionSizer()
        self._cv_scorer = _CVStabilityScorer()
        self._tf_log_spread: Dict[str, Dict[str, Deque[float]]] = {}  # pair_id → {tf: deque}
        self._tf_z_scores: Dict[str, Dict[str, float]] = {}
        self._daily_volume_estimates: Dict[str, float] = {}

        self._diag_stats: Dict[str, int] = {
            'layer1_hft_signal_total': 0,
            'layer1_hft_signal_passed': 0,
            'layer2_main_capture_total': 0,
            'layer2_main_capture_passed': 0,
            'layer3_standby_block_total': 0,
            'layer3_standby_block_blocked': 0,
            'simulated_signals_generated': 0,
            'snapshots_saved': 0,
            'cointegration_tests_run': 0,
            'cointegration_passed': 0,
            'z_score_signals_generated': 0,
            'structural_breaks_detected': 0,
            'half_life_estimations': 0,
            'hedge_ratio_updates': 0,
            'cost_filtered_signals': 0,
            # FIX-20260713-S5-V2ABSORB: V2 统计计数器
            'regime_classifications': 0,
            'cv_stability_scores': 0,
            'kelly_sized_signals': 0,
            'multi_tf_fusions': 0,
        }

        logger.info("[S5-ARB-V1] 初始化完成 base_dir=%s", self._base_dir)

    @staticmethod
    def _detect_base_dir() -> str:
        try:
            current = os.path.dirname(os.path.abspath(__file__))
        except (ValueError, OSError):
            return os.getcwd()
        for _ in range(6):
            pkg_init = os.path.join(current, 'ali2026v3_trading', '__init__.py')
            if os.path.isfile(pkg_init):
                return current
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
        return os.getcwd()

    @staticmethod
    def _now_iso() -> str:
        try:
            return datetime.now(_CHINA_TZ).isoformat(timespec='milliseconds')
        except (ValueError, OSError):
            return datetime.now().isoformat(timespec='milliseconds')

    # ------------------------------------------------------------------
    # 配对注册与价格更新
    # ------------------------------------------------------------------

    def register_pair(self, config: ArbitragePairConfig) -> None:
        """注册一个套利对。"""
        pair_id = f"{config.leg_a}:{config.leg_b}"
        with self._lock:
            self._pairs[pair_id] = config
            self._kalman[pair_id] = _KalmanHedgeRatio()
            self._hedge_ratios[pair_id] = config.hedge_ratio
            self._price_history[config.leg_a] = []
            self._price_history[config.leg_b] = []
            self._log_spread_history[pair_id] = []
            self._cointegration_pvalues[pair_id] = 1.0
            self._half_life_sec[pair_id] = 0.0
            self._last_z_score[pair_id] = 0.0
            # FIX-20260713-S5-V2ABSORB: V2 组件初始化
            self._regime_classifiers[pair_id] = _MarketRegimeClassifier()
            self._tf_log_spread[pair_id] = {'tick': deque(maxlen=60), 'm1': deque(maxlen=20), 'm5': deque(maxlen=10)}
            self._tf_z_scores[pair_id] = {'tick': 0.0, 'm1': 0.0, 'm5': 0.0}
        logger.info("[S5-ARB-V1] 注册配对 %s β=%.4f", pair_id, config.hedge_ratio)

    def on_price_update(self, instrument_id: str, price: float,
                        timestamp: Optional[float] = None,
                        tick_data: Optional[Dict[str, Any]] = None) -> None:
        """更新单腿价格，驱动配对统计套利计算。

        FIX-20260713-S5-MULTISRC: 支持传入完整 tick 字典以提取多源数据。
        当 tick_data 非 None 时，提取 bid_volume1/ask_volume1/volume/turnover/
        open_interest/direction 全部可用字段，不限于价格。
        """
        if price <= 0 or not instrument_id:
            return
        ts = timestamp or time.time()
        with self._lock:
            hist = self._price_history.setdefault(instrument_id, [])
            hist.append((ts, price))
            if len(hist) > 500:
                hist.pop(0)

            # FIX-20260713-S5-MULTISRC: 从完整 tick 提取多源数据
            if tick_data is not None and isinstance(tick_data, dict):
                self._extract_multisrc_fields(instrument_id, tick_data)
                self._data_source_mask |= 0x01

        # 检查该腿属于哪个配对
        with self._lock:
            for pair_id, cfg in self._pairs.items():
                if instrument_id in (cfg.leg_a, cfg.leg_b):
                    self._update_pair(pair_id)

    def _extract_multisrc_fields(self, instrument_id: str, tick_data: Dict[str, Any]) -> None:
        """从 tick 字典提取全部多源字段。调用方需持锁。"""
        try:
            # 订单簿不平衡
            bid_vol = float(tick_data.get("bid_volume1", tick_data.get("bid_volume", 0.0)) or 0.0)
            ask_vol = float(tick_data.get("ask_volume1", tick_data.get("ask_volume", 0.0)) or 0.0)
            if bid_vol > 0 or ask_vol > 0:
                self._leg_bid_volume[instrument_id] = bid_vol
                self._leg_ask_volume[instrument_id] = ask_vol
                total = bid_vol + ask_vol
                if total > 0:
                    self._leg_ob_imbalance[instrument_id] = (bid_vol - ask_vol) / total

            # VWAP 与日成交量估计（用于市场冲击模型）
            turnover = float(tick_data.get("turnover", 0.0) or 0.0)
            volume = float(tick_data.get("volume", 0.0) or 0.0)
            if turnover > 0 and volume > 0:
                self._leg_cum_turnover[instrument_id] = turnover
                self._leg_cum_volume[instrument_id] = volume
                if volume > 0:
                    self._leg_vwap[instrument_id] = turnover / volume
                # FIX-20260713-S5-V1: 用累计成交量作为日成交量估计，替代固定默认值
                self._daily_volume_estimates[instrument_id] = volume

            # 持仓量趋势
            oi = float(tick_data.get("open_interest", 0.0) or 0.0)
            if oi > 0:
                if self._leg_open_interest.get(instrument_id, 0) > 0:
                    self._leg_prev_oi[instrument_id] = self._leg_open_interest[instrument_id]
                self._leg_open_interest[instrument_id] = oi
                prev = self._leg_prev_oi.get(instrument_id, 0)
                if prev > 0:
                    change = oi - prev
                    if change > 0:
                        self._leg_oi_trend[instrument_id] = "rising"
                    elif change < 0:
                        self._leg_oi_trend[instrument_id] = "falling"
                    else:
                        self._leg_oi_trend[instrument_id] = "stable"

            # 真实成交方向
            raw_dir = tick_data.get("direction", tick_data.get("trade_direction", 0))
            if raw_dir is not None:
                try:
                    self._leg_trade_direction[instrument_id] = int(float(raw_dir))
                except (ValueError, TypeError):
                    pass
        except (ValueError, TypeError) as e:
            logger.debug("[S5-ARB-V1] _extract_multisrc_fields 异常: %s", e)

    # ------------------------------------------------------------------
    # FIX-20260713-S5-MULTISRC: 多源数据采集接口（不限于 HFT tick 管道）
    # ------------------------------------------------------------------

    def on_tick(self, tick_data: Dict[str, Any]) -> None:
        """tick 回调 — 提取全部可用字段，驱动配对计算。

        顶级基金统计套利不限于单一价格管道，而是从 tick 中提取
        盘口量、成交量、成交额、持仓量、成交方向等全部可用信息。
        """
        if not tick_data or not isinstance(tick_data, dict):
            return
        instrument_id = str(tick_data.get('instrument_id', ''))
        price = float(tick_data.get('last_price', 0.0) or 0.0)
        if not instrument_id or price <= 0:
            return
        self.on_price_update(instrument_id, price, tick_data=tick_data)

    def on_order_book(self, instrument_id: str, levels: List[Dict[str, Any]]) -> None:
        """多级订单簿数据接入（单腿）。

        顶级基金统计套利监控每腿的订单簿深度，用于：
        - 订单簿不平衡作为信号确认（两腿不平衡同向 → 信号更强）
        - 流动性评估（流动性差 → 交易成本调高）

        Args:
            instrument_id: 合约 ID
            levels: [{'level': 1, 'bid_price': ..., 'bid_volume': ..., 'ask_price': ..., 'ask_volume': ...}]
        """
        if not instrument_id or not levels or not isinstance(levels, list):
            return
        try:
            with self._lock:
                total_bid_vol = 0.0
                total_ask_vol = 0.0
                for lv in levels:
                    if not isinstance(lv, dict):
                        continue
                    bv = float(lv.get('bid_volume', 0.0) or 0.0)
                    av = float(lv.get('ask_volume', 0.0) or 0.0)
                    total_bid_vol += bv
                    total_ask_vol += av
                if total_bid_vol + total_ask_vol > 0:
                    self._leg_ob_imbalance[instrument_id] = (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol)
                self._data_source_mask |= 0x02
        except (ValueError, TypeError) as e:
            logger.debug("[S5-ARB-V1] on_order_book 异常: %s", e)

    def on_trade(self, instrument_id: str, trade: Dict[str, Any]) -> None:
        """单笔成交流数据接入（单腿）。

        顶级基金统计套利直接消费成交流以获取真实的成交方向和成交量，
        而非从 tick 价格推断。

        Args:
            instrument_id: 合约 ID
            trade: {'price': ..., 'volume': ..., 'direction': +1/-1/0}
        """
        if not instrument_id or not trade or not isinstance(trade, dict):
            return
        try:
            direction = int(trade.get('direction', 0) or 0)
            self._leg_trade_direction[instrument_id] = direction
            self._data_source_mask |= 0x04
        except (ValueError, TypeError) as e:
            logger.debug("[S5-ARB-V1] on_trade 异常: %s", e)

    def on_open_interest(self, instrument_id: str, oi: float, prev: float = 0.0) -> None:
        """持仓量数据接入（单腿）。

        顶级基金统计套利监控持仓量变化以判断资金流向：
        - OI 上升 + 价差扩大 → 趋势性套利机会
        - OI 下降 + 价差收窄 → 平仓信号

        Args:
            instrument_id: 合约 ID
            oi: 当前持仓量
            prev: 上一时刻持仓量（0 表示自动从上次记录推断）
        """
        if not instrument_id or oi <= 0:
            return
        with self._lock:
            if self._leg_open_interest.get(instrument_id, 0) > 0:
                self._leg_prev_oi[instrument_id] = self._leg_open_interest[instrument_id]
            self._leg_open_interest[instrument_id] = oi
            if prev > 0:
                self._leg_prev_oi[instrument_id] = prev
            p = self._leg_prev_oi.get(instrument_id, 0)
            if p > 0:
                change = oi - p
                if change > 0:
                    self._leg_oi_trend[instrument_id] = "rising"
                elif change < 0:
                    self._leg_oi_trend[instrument_id] = "falling"
                else:
                    self._leg_oi_trend[instrument_id] = "stable"
            self._data_source_mask |= 0x08

    def _update_pair(self, pair_id: str) -> None:
        """更新配对统计量：hedge ratio、log spread、协整、半衰期。"""
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return
        hist_a = self._price_history.get(cfg.leg_a, [])
        hist_b = self._price_history.get(cfg.leg_b, [])
        if len(hist_a) < cfg.beta_lookback or len(hist_b) < cfg.beta_lookback:
            return

        # 时间对齐：取最近 N 个共同时刻
        ts_set_a = {h[0] for h in hist_a[-cfg.beta_lookback:]}
        ts_set_b = {h[0] for h in hist_b[-cfg.beta_lookback:]}
        common_ts = sorted(ts_set_a & ts_set_b)
        if len(common_ts) < 10:
            return
        price_map_a = {h[0]: h[1] for h in hist_a}
        price_map_b = {h[0]: h[1] for h in hist_b}
        x = [price_map_a[ts] for ts in common_ts]
        y = [price_map_b[ts] for ts in common_ts]

        # Hedge ratio: OLS + Kalman 融合
        beta_ols = _rolling_ols_beta(x, y)
        beta_kalman = self._kalman[pair_id].update(x[-1], y[-1])
        # 权重：OLS 0.6 + Kalman 0.4
        beta = 0.6 * beta_ols + 0.4 * beta_kalman
        self._hedge_ratios[pair_id] = beta
        self._diag_stats['hedge_ratio_updates'] += 1

        # 对数价差
        log_spread = math.log(y[-1]) - beta * math.log(x[-1])
        ls_hist = self._log_spread_history.setdefault(pair_id, [])
        ls_hist.append(log_spread)
        if len(ls_hist) > 500:
            ls_hist.pop(0)

        # FIX-20260713-S5-V2ABSORB: 更新多时间框架历史（tick/m1/m5）
        # 监控模式下无真实分钟线，用同一 log_spread 按不同窗口平滑（tick=高频，m1=中频，m5=低频）
        tf_hist = self._tf_log_spread.get(pair_id, {})
        if 'tick' in tf_hist:
            tf_hist['tick'].append(log_spread)
        if 'm1' in tf_hist:
            tf_hist['m1'].append(log_spread)
        if 'm5' in tf_hist:
            tf_hist['m5'].append(log_spread)

        # 协整与半衰期
        pvalue = _simple_adf_pvalue(ls_hist)
        self._cointegration_pvalues[pair_id] = pvalue
        if pvalue < cfg.coint_pvalue_threshold:
            self._diag_stats['cointegration_passed'] += 1

        hl = self._estimate_half_life(pair_id)
        self._half_life_sec[pair_id] = hl

    # ------------------------------------------------------------------
    # 统计套利计算
    # ------------------------------------------------------------------

    def compute_zscore(self, pair_id: str) -> float:
        """计算配对残差 Z-score。"""
        with self._lock:
            history = self._log_spread_history.get(pair_id, [])
            cfg = self._pairs.get(pair_id)
            min_samples = cfg.beta_lookback if cfg else 30
            if len(history) < min_samples:
                return 0.0
            current = history[-1]
            window = history[-min_samples:]
            mean = sum(window) / len(window)
            var = sum((x - mean) ** 2 for x in window) / len(window)
            std = math.sqrt(var) if var > 0 else 0.0
            if std <= 0:
                return 0.0
            return (current - mean) / std

    def _estimate_half_life(self, pair_id: str) -> float:
        """OU 过程半衰期估计。"""
        history = self._log_spread_history.get(pair_id, [])
        cfg = self._pairs.get(pair_id)
        min_samples = cfg.beta_lookback if cfg else 30
        if len(history) < min_samples:
            return 0.0
        s_prev = history[:-1]
        ds = [history[i + 1] - history[i] for i in range(len(history) - 1)]
        n = len(ds)
        mean_s = sum(s_prev) / n
        mean_ds = sum(ds) / n
        num = sum((s_prev[i] - mean_s) * (ds[i] - mean_ds) for i in range(n))
        den = sum((s_prev[i] - mean_s) ** 2 for i in range(n))
        if den <= 0:
            return 0.0
        kappa = -num / den
        self._diag_stats['half_life_estimations'] += 1
        if kappa <= 0:
            return float('inf')
        return math.log(2) / kappa

    def test_cointegration(self, pair_id: str) -> float:
        """返回协整 p-value。"""
        with self._lock:
            return self._cointegration_pvalues.get(pair_id, 1.0)

    def check_structural_break(self, pair_id: str, current_z: float) -> bool:
        """结构性断点检测。"""
        with self._lock:
            last_z = self._last_z_score.get(pair_id, 0.0)
            is_break = False
            threshold = 4.0
            if abs(current_z) > threshold:
                is_break = True
            elif last_z != 0.0 and (current_z * last_z < 0) and \
                    (abs(current_z) + abs(last_z)) > threshold:
                is_break = True
            if is_break:
                self._structural_break_count += 1
                self._diag_stats['structural_breaks_detected'] += 1
                logger.warning("[S5-ARB-V1] 结构性断点 pair=%s Z=%.4f last_Z=%.4f",
                               pair_id, current_z, last_z)
            self._last_z_score[pair_id] = current_z
            return is_break

    # ------------------------------------------------------------------
    # 信号生成（顶级基金标准）
    # ------------------------------------------------------------------

    # FIX-20260713-S5-V2ABSORB: V2 顶级基金组件方法

    def _compute_multi_tf_zscore(self, pair_id: str) -> Dict[str, float]:
        """多时间框架Z-score融合。对标 Two Sigma。"""
        z_scores = {}
        with self._lock:
            tf_hist = self._tf_log_spread.get(pair_id, {})
            for tf in self.TF_WINDOWS:
                history = list(tf_hist.get(tf, []))
                if len(history) < 5:
                    z_scores[tf] = 0.0
                    continue
                current = history[-1]
                mean = sum(history) / len(history)
                var = sum((x - mean) ** 2 for x in history) / len(history)
                std = math.sqrt(var) if var > 0 else 0.0
                z_scores[tf] = (current - mean) / std if std > 0 else 0.0
            self._tf_z_scores[pair_id] = z_scores
        fused = 0.0
        total_weight = 0.0
        for tf, weight in self.TF_WEIGHTS.items():
            if abs(z_scores.get(tf, 0.0)) > 0:
                fused += z_scores[tf] * weight
                total_weight += weight
        fused = fused / total_weight if total_weight > 0 else 0.0
        self._diag_stats['multi_tf_fusions'] += 1
        return {'tick': z_scores.get('tick', 0.0), 'm1': z_scores.get('m1', 0.0),
                'm5': z_scores.get('m5', 0.0), 'fused': fused}

    def _compute_adaptive_thresholds(self, pair_id: str) -> Tuple[float, float]:
        """波动率自适应阈值。高波动→扩大阈值(减少假信号)。对标 Citadel。"""
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return 2.0, 0.5
        with self._lock:
            history = self._log_spread_history.get(pair_id, [])
        if len(history) < 30:
            return cfg.z_open, cfg.z_close
        mean = sum(history[-30:]) / 30
        std = math.sqrt(sum((x - mean) ** 2 for x in history[-30:]) / 30)
        baseline_std = 0.01
        vol_ratio = std / baseline_std if baseline_std > 0 else 1.0
        scale = max(0.7, min(1.5, vol_ratio))
        return cfg.z_open * scale, cfg.z_close * scale

    def _compute_spread_volatility(self, pair_id: str) -> float:
        """计算价差波动率。"""
        with self._lock:
            history = self._log_spread_history.get(pair_id, [])
        if len(history) < 10:
            return 0.01
        window = history[-30:] if len(history) >= 30 else history
        mean = sum(window) / len(window)
        var = sum((x - mean) ** 2 for x in window) / len(window)
        return math.sqrt(var) if var > 0 else 0.01

    def _compute_execution_cost(self, pair_id: str, direction: str) -> float:
        """计算执行成本(含市场冲击)。对标 Jane Street。"""
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return 2.0
        spread_vol = self._compute_spread_volatility(pair_id)
        daily_vol = self._daily_volume_estimates.get(cfg.leg_b, 10000.0)
        # FIX-20260713-S5-V1: 使用 cfg.max_position_units 作为下单量，替代硬编码 10.0
        trade_volume = max(1.0, cfg.max_position_units)
        impact = _square_root_impact(trade_volume, daily_vol, spread_vol, cfg.impact_coefficient)
        return cfg.transaction_cost_bps + impact * 10000.0

    def evaluate_pair_signal(self, pair_id: str) -> Optional[SimulatedArbitrageSignal]:
        """基于统计套利模型生成信号（含成本调整）。"""
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return None
        with self._lock:
            z = self.compute_zscore(pair_id)
            pvalue = self._cointegration_pvalues.get(pair_id, 1.0)
            hl = self._half_life_sec.get(pair_id, 0.0)
            beta = self._hedge_ratios.get(pair_id, cfg.hedge_ratio)
            break_flag = self.check_structural_break(pair_id, z)

            is_cointegrated = pvalue < cfg.coint_pvalue_threshold
            hl_ok = cfg.min_half_life_sec < hl <= cfg.max_half_life_sec

            # FIX-20260713-S5-V2ABSORB: V2 顶级基金组件集成
            multi_tf = self._compute_multi_tf_zscore(pair_id)
            fused_z = multi_tf.get('fused', 0.0)
            regime_classifier = self._regime_classifiers.get(pair_id)
            if regime_classifier is not None:
                regime, regime_conf = regime_classifier.update(z, hl, pvalue, break_flag)
                self._diag_stats['regime_classifications'] += 1
            else:
                regime, regime_conf = 'TRENDING', 0.0
            adaptive_open, adaptive_close = self._compute_adaptive_thresholds(pair_id)
            ls_hist = self._log_spread_history.get(pair_id, [])
            cv_stability = self._cv_scorer.score(pair_id, ls_hist)
            self._diag_stats['cv_stability_scores'] += 1
            regime_ok = regime != 'REGIME_BREAK'
            cv_ok = cv_stability >= cfg.min_cv_stability

            action = 'HOLD'
            direction = ''
            confidence = 0.0
            signal_type = 'HOLD'
            expected_profit_bps = 0.0  # FIX-20260713-S5-V1: 初始化为0，避免平仓/止损分支引用未定义变量
            # FIX-20260713-S5-V1-CRIT: 多时间框架融合必须参与开仓决策，不能仅作为信号字段
            signal_z = max(abs(z), abs(fused_z))

            # 止损优先
            if abs(z) > cfg.z_stop or break_flag:
                action = 'STOP_LOSS'
                direction = 'SELL' if z > 0 else 'BUY'
                signal_type = 'CLOSE_LONG' if z > 0 else 'CLOSE_SHORT'
                confidence = min(abs(z) / 5.0, 1.0)
            # 开仓：协整 + 半衰期 + Z阈值(单tick+多TF融合) + 成本调整后盈利 + V2组件确认
            elif signal_z > adaptive_open and is_cointegrated and hl_ok and cv_ok and regime_ok:
                expected_profit_bps = signal_z * 0.5 - cfg.transaction_cost_bps
                # FIX-20260713-S5-MULTISRC: 多源数据信号确认
                # 两腿订单簿不平衡同向 → 信号增强（市场结构性偏向一致）
                ob_a = self._leg_ob_imbalance.get(cfg.leg_a, 0.0)
                ob_b = self._leg_ob_imbalance.get(cfg.leg_b, 0.0)
                ob_confirm = (abs(ob_a - ob_b) < 0.3) and (abs(ob_a) > 0.1 or abs(ob_b) > 0.1)
                if ob_confirm:
                    expected_profit_bps += 0.5  # 订单簿确认 → 盈利预期 +0.5 bps
                if expected_profit_bps >= cfg.min_expected_profit_bps:
                    action = 'OPEN'
                    direction = 'SELL' if z > 0 else 'BUY'
                    signal_type = 'OPEN_SHORT' if z > 0 else 'OPEN_LONG'
                    confidence = min(abs(z) / 4.0, 1.0)
                    if ob_confirm:
                        confidence = min(confidence + 0.1, 1.0)  # 订单簿确认 → 置信度 +10%
                else:
                    self._diag_stats['cost_filtered_signals'] += 1
            # 平仓
            elif abs(z) < adaptive_close:
                action = 'CLOSE'
                direction = 'SELL' if z > 0 else 'BUY'
                signal_type = 'CLOSE_LONG' if z > 0 else 'CLOSE_SHORT'
                confidence = 0.5 + (adaptive_close - abs(z)) / adaptive_close * 0.5 if adaptive_close > 0 else 0.5

            if action == 'HOLD':
                return None

            self._diag_stats['z_score_signals_generated'] += 1

            # FIX-20260713-S5-V2ABSORB: V2 仓位/成本/衰减计算（信息性）
            spread_vol = self._compute_spread_volatility(pair_id)
            kelly_units = self._kelly_sizer.compute(z, hl if hl != float('inf') else 0.0,
                                                    spread_vol, cfg.max_position_units)
            kelly_fraction = kelly_units / cfg.max_position_units if cfg.max_position_units > 0 else 0.0
            self._diag_stats['kelly_sized_signals'] += 1
            total_exec_cost = self._compute_execution_cost(pair_id, direction)
            market_impact_bps = max(0.0, total_exec_cost - cfg.transaction_cost_bps)
            base_profit = expected_profit_bps if action == 'OPEN' else 0.0
            net_profit_bps = base_profit - market_impact_bps
            # FIX-20260713-S5-V1-CRIT: 信号衰减必须基于上次配对信号时间，不能恒为0
            now_ts = time.time()
            signal_age = now_ts - self._last_pair_signal_ts if self._last_pair_signal_ts > 0 else 0.0
            decayed_conf = confidence * (0.5 ** (signal_age / self.SIGNAL_DECAY_HALF_LIFE_SEC))

            # 获取价格用于 entry_price
            hist_a = self._price_history.get(cfg.leg_a, [])
            hist_b = self._price_history.get(cfg.leg_b, [])
            price_a = hist_a[-1][1] if hist_a else 0.0
            price_b = hist_b[-1][1] if hist_b else 0.0
            entry_price = price_b if direction == 'BUY' else price_a

            signal = SimulatedArbitrageSignal(
                signal_id=generate_prefixed_id('S5ARB', 12),
                instrument_id=pair_id,
                leg_a=cfg.leg_a,
                leg_b=cfg.leg_b,
                direction=direction,
                signal_type=signal_type,
                deviation_bps=round(abs(z) * 10.0, 2),  # Z-score 映射为 bps
                confidence=round(confidence, 4),
                entry_price=round(entry_price, 6),
                spread_convergence_rate=0.0,
                quality_score=round(self._compute_quality_score(pair_id, z, hl, pvalue), 4),
                timestamp=time.time(),
                simulated=True,
                capital_allocation=self.CAPITAL_ALLOCATION,
                hft_consumed=False,
                source='s5_arbitrage_monitor_v1',
                z_score=round(z, 6),
                half_life_sec=round(hl, 4) if hl != float('inf') else -1.0,
                hedge_ratio=round(beta, 6),
                log_spread=round(math.log(price_b) - beta * math.log(price_a), 6) if price_a > 0 and price_b > 0 else 0.0,
                cointegration_pvalue=round(pvalue, 6),
                structural_break_flag=break_flag,
                expected_profit_per_unit=round(expected_profit_bps if action == 'OPEN' else 0.0, 4),
                transaction_cost_bps=round(cfg.transaction_cost_bps, 4),
                # FIX-20260713-S5-MULTISRC: 多源数据采集字段
                order_book_imbalance_a=round(self._leg_ob_imbalance.get(cfg.leg_a, 0.0), 6),
                order_book_imbalance_b=round(self._leg_ob_imbalance.get(cfg.leg_b, 0.0), 6),
                open_interest_a=self._leg_open_interest.get(cfg.leg_a, 0.0),
                open_interest_b=self._leg_open_interest.get(cfg.leg_b, 0.0),
                oi_trend_a=self._leg_oi_trend.get(cfg.leg_a, 'stable'),
                oi_trend_b=self._leg_oi_trend.get(cfg.leg_b, 'stable'),
                vwap_a=self._leg_vwap.get(cfg.leg_a, 0.0),
                vwap_b=self._leg_vwap.get(cfg.leg_b, 0.0),
                data_source_mask=self._data_source_mask,
                # FIX-20260713-S5-V2ABSORB: V2 顶级基金组件字段
                market_regime=regime,
                regime_confidence=round(regime_conf, 4),
                multi_tf_zscore={k: round(v, 6) for k, v in multi_tf.items()},
                fused_zscore=round(fused_z, 6),
                adaptive_z_open=round(adaptive_open, 4),
                adaptive_z_close=round(adaptive_close, 4),
                kelly_fraction=round(kelly_fraction, 6),
                optimal_position_units=round(kelly_units, 4),
                cv_stability_score=round(cv_stability, 4),
                signal_age_sec=round(signal_age, 4),
                decayed_confidence=round(decayed_conf, 4),
                market_impact_bps=round(market_impact_bps, 4),
                queue_position_prob=0.85,
                total_execution_cost_bps=round(total_exec_cost, 4),
                net_expected_profit_bps=round(net_profit_bps, 4),
            )
            self._simulated_signals.append(signal)
            if len(self._simulated_signals) > self.MAX_HISTORY_SIGNALS:
                self._simulated_signals = self._simulated_signals[-self.MAX_HISTORY_SIGNALS:]
            self._diag_stats['simulated_signals_generated'] += 1
            self.save_snapshot(signal)
            # FIX-20260713-S5-V1-CRIT: 更新配对信号时间戳，供下次衰减计算使用
            self._last_pair_signal_ts = now_ts
            logger.info("[S5-ARB-V1] 配对信号 pair=%s action=%s dir=%s Z=%.4f p=%.4f hl=%.2f β=%.4f",
                        pair_id, action, direction, z, pvalue, hl, beta)
            return signal

    def _compute_quality_score(self, pair_id: str, z: float, hl: float, pvalue: float) -> float:
        """信号质量评分：协整性、半衰期、Z-score 幅度。"""
        cfg = self._pairs.get(pair_id)
        max_hl = cfg.max_half_life_sec if cfg else 300.0
        min_hl = cfg.min_half_life_sec if cfg else 5.0
        coint_score = 1.0 if pvalue < 0.05 else max(0.0, 1.0 - pvalue * 10)
        hl_score = 0.0
        if min_hl < hl <= max_hl:
            hl_score = 1.0 - (hl - min_hl) / (max_hl - min_hl)
            hl_score = max(0.0, min(1.0, hl_score))
        z_score = min(abs(z) / 4.0, 1.0)
        return coint_score * 0.4 + hl_score * 0.3 + z_score * 0.3

    # ------------------------------------------------------------------
    # 兼容旧版：单合约 deviation 模式
    # ------------------------------------------------------------------

    def on_arbitrage_signal(self, signal: Dict[str, Any]) -> None:
        """兼容旧版 HFT 套利信号回调。"""
        if not signal or not isinstance(signal, dict):
            return
        with self._lock:
            self._last_hft_signal = dict(signal)
            self._last_hft_signal_ts = time.time()
            instrument_id = str(signal.get('instrument_id', ''))
            deviation = float(signal.get('deviation_bps', 0.0))
            entry_price = float(signal.get('entry_price', 0.0))
            if instrument_id:
                self._append_deviation_sample(instrument_id, deviation, entry_price)
        logger.info("[S5-ARB-V1] 接收HFT套利信号 instr=%s dev=%.1fbps",
                    signal.get('instrument_id', ''), signal.get('deviation_bps', 0.0))

    def _append_deviation_sample(self, instrument_id: str, deviation: float, price: float) -> None:
        if not instrument_id:
            return
        samples = self._deviation_history.setdefault(instrument_id, [])
        samples.append({'ts': time.time(), 'deviation': float(deviation), 'price': float(price)})
        cutoff = time.time() - self.CONVERGENCE_WINDOW_SEC
        fresh = [s for s in samples if s['ts'] >= cutoff]
        if len(fresh) > 300:
            fresh = fresh[-300:]
        self._deviation_history[instrument_id] = fresh

    def generate_simulated_signal(self, action: str = 'OPEN') -> Optional[SimulatedArbitrageSignal]:
        """兼容旧版模拟信号生成（基于最近 HFT 信号）。"""
        # 优先尝试新版配对信号
        for pair_id in list(self._pairs.keys()):
            sig = self.evaluate_pair_signal(pair_id)
            if sig is not None:
                return sig
        # 回退旧版单合约模式
        with self._lock:
            sig = self._last_hft_signal
            if sig is None:
                return None
            instrument_id = str(sig.get('instrument_id', ''))
            direction = str(sig.get('direction', 'BUY')).upper()
            if direction not in ('BUY', 'SELL'):
                direction = 'BUY'
            if action.upper() == 'CLOSE':
                signal_type = 'CLOSE_LONG' if direction == 'BUY' else 'CLOSE_SHORT'
                sim_direction = 'SELL' if direction == 'BUY' else 'BUY'
            else:
                signal_type = 'OPEN_LONG' if direction == 'BUY' else 'OPEN_SHORT'
                sim_direction = direction
            conv = self.track_spread_convergence(instrument_id)
            quality = 0.5 if conv.get('trend') == 'converging' else 0.3
            signal = SimulatedArbitrageSignal(
                signal_id=generate_prefixed_id('S5ARB', 12),
                instrument_id=instrument_id,
                direction=sim_direction,
                signal_type=signal_type,
                deviation_bps=float(sig.get('deviation_bps', 0.0)),
                confidence=float(sig.get('confidence', 0.0)),
                entry_price=float(sig.get('entry_price', 0.0)),
                spread_convergence_rate=float(conv.get('convergence_rate', 0.0)),
                quality_score=round(quality, 4),
                timestamp=time.time(),
                simulated=True,
                capital_allocation=self.CAPITAL_ALLOCATION,
                hft_consumed=bool(sig.get('hft_consumed', False)),
                source='s5_arbitrage_monitor_v1_fallback',
            )
            self._simulated_signals.append(signal)
            if len(self._simulated_signals) > self.MAX_HISTORY_SIGNALS:
                self._simulated_signals = self._simulated_signals[-self.MAX_HISTORY_SIGNALS:]
            self._diag_stats['simulated_signals_generated'] += 1
            self.save_snapshot(signal)
            return signal

    def track_spread_convergence(self, instrument_id: str) -> Dict[str, Any]:
        """兼容旧版单合约收敛跟踪。"""
        with self._lock:
            result = {
                'instrument_id': instrument_id,
                'convergence_rate': 0.0,
                'trend': 'unknown',
                'sample_count': 0,
                'time_span_sec': 0.0,
                'start_deviation': 0.0,
                'end_deviation': 0.0,
            }
            samples = self._deviation_history.get(instrument_id, [])
            if len(samples) < 2:
                result['trend'] = 'insufficient_samples'
                return result
            now = time.time()
            cutoff = now - self.CONVERGENCE_WINDOW_SEC
            fresh = [s for s in samples if s['ts'] >= cutoff]
            if len(fresh) < 2:
                result['trend'] = 'insufficient_samples'
                return result
            start, end = fresh[0], fresh[-1]
            span = end['ts'] - start['ts']
            rate = safe_divide(start['deviation'] - end['deviation'], span, default=0.0)
            result.update({
                'convergence_rate': rate,
                'trend': 'converging' if rate > 0 else ('diverging' if rate < 0 else 'flat'),
                'sample_count': len(fresh),
                'time_span_sec': span,
                'start_deviation': start['deviation'],
                'end_deviation': end['deviation'],
            })
            return result

    def evaluate_arbitrage_quality(self, instrument_id: str, volume: float = 0.0) -> Dict[str, Any]:
        """兼容旧版质量评估。"""
        conv = self.track_spread_convergence(instrument_id)
        conv_rate = float(conv.get('convergence_rate', 0.0))
        trend = conv.get('trend', 'unknown')
        conv_component = min(abs(conv_rate) / 10.0, 1.0)
        if trend == 'converging':
            conv_component = min(conv_component * 1.2, 1.0)
        elif trend == 'diverging':
            conv_component *= 0.5
        samples = self._deviation_history.get(instrument_id, [])
        deviations = [float(s['deviation']) for s in samples]
        std = (sum((d - sum(deviations) / len(deviations)) ** 2 for d in deviations) / len(deviations)) ** 0.5 if deviations else 0.0
        vol_component = 1.0 - min(std / 50.0, 1.0)
        volume_component = min(float(volume) / 1000.0, 1.0)
        quality = conv_component * 0.4 + vol_component * 0.3 + volume_component * 0.3
        quality = max(0.0, min(1.0, quality))
        return {
            'instrument_id': instrument_id,
            'quality_score': round(quality, 4),
            'convergence_rate': round(conv_rate, 4),
            'trend': trend,
            'volatility': round(std, 4),
            'volume': float(volume),
            'components': {
                'convergence': round(conv_component, 4),
                'volatility': round(vol_component, 4),
                'volume': round(volume_component, 4),
            },
        }

    # ------------------------------------------------------------------
    # 3层诊断（兼容旧版接口）
    # ------------------------------------------------------------------

    def diagnose_layer1_hft_signal(self) -> Dict[str, Any]:
        self._diag_stats['layer1_hft_signal_total'] += 1
        with self._lock:
            sig = self._last_hft_signal
            result = {
                'layer': 1, 'passed': False, 'instrument_id': '',
                'deviation_bps': 0.0, 'confidence': 0.0, 'reason': '', 'timestamp': time.time(),
            }
            if sig is None:
                result['reason'] = 'no_hft_arbitrage_signal_received'
                return result
            elapsed = time.time() - self._last_hft_signal_ts
            if elapsed > self.CONVERGENCE_WINDOW_SEC:
                result['reason'] = f'hft_signal_expired(elapsed={elapsed:.1f}s)'
                return result
            confidence = float(sig.get('confidence', 0.0))
            deviation = float(sig.get('deviation_bps', 0.0))
            instrument_id = str(sig.get('instrument_id', ''))
            result.update({'instrument_id': instrument_id, 'deviation_bps': deviation, 'confidence': confidence})
            if confidence < 0.6:
                result['reason'] = f'confidence_below_threshold({confidence:.2f}<0.60)'
                return result
            result['passed'] = True
            result['reason'] = 'hft_signal_valid'
            self._diag_stats['layer1_hft_signal_passed'] += 1
            return result

    def diagnose_layer2_main_capture(self) -> Dict[str, Any]:
        self._diag_stats['layer2_main_capture_total'] += 1
        with self._lock:
            sig = self._last_hft_signal
            result = {
                'layer': 2, 'passed': False, 'captured': False,
                'hft_consumed': False, 'confidence': 0.0, 'deviation_bps': 0.0,
                'reason': '', 'timestamp': time.time(),
            }
            if sig is None:
                result['reason'] = 'no_hft_signal_to_check'
                return result
            hft_consumed = bool(sig.get('hft_consumed', False))
            confidence = float(sig.get('confidence', 0.0))
            deviation = abs(float(sig.get('deviation_bps', 0.0)))
            captured = (confidence >= 0.8) and (deviation > 100.0) and (not hft_consumed)
            result.update({'hft_consumed': hft_consumed, 'confidence': confidence, 'deviation_bps': deviation, 'captured': captured})
            if captured:
                result['passed'] = True
                result['reason'] = 'main_cycle_captured_arbitrage'
                self._diag_stats['layer2_main_capture_passed'] += 1
            else:
                if hft_consumed:
                    result['reason'] = 'hft_already_consumed_signal'
                elif confidence < 0.8:
                    result['reason'] = f'confidence_below_main_threshold({confidence:.2f}<0.80)'
                elif deviation <= 100.0:
                    result['reason'] = f'deviation_below_main_threshold({deviation:.1f}<=100bps)'
                else:
                    result['reason'] = 'unknown_not_captured'
            return result

    def diagnose_layer3_standby_block(self, slot_state: Optional[str] = None) -> Dict[str, Any]:
        self._diag_stats['layer3_standby_block_total'] += 1
        state = str(slot_state or 'active').lower()
        result = {
            'layer': 3, 'passed': True, 'blocked': False,
            'slot_state': state, 'reason': '', 'timestamp': time.time(),
        }
        if state == 'standby':
            result.update({'blocked': True, 'passed': False, 'reason': 'slot_in_standby_blocks_open'})
            self._diag_stats['layer3_standby_block_blocked'] += 1
        elif state in ('inactive', 'retired', 'handover'):
            result.update({'blocked': True, 'passed': False, 'reason': f'slot_state_{state}_blocks_open'})
            self._diag_stats['layer3_standby_block_blocked'] += 1
        else:
            result['reason'] = 'slot_allows_open'
        return result

    # ------------------------------------------------------------------
    # 快照与报告
    # ------------------------------------------------------------------

    def save_snapshot(self, signal: SimulatedArbitrageSignal) -> str:
        with self._lock:
            safe_ts = datetime.now(_CHINA_TZ).strftime('%Y%m%d_%H%M%S_%f')
            filename = f"S5ARB_{signal.signal_id}_{safe_ts}.json"
            filepath = os.path.join(self._snapshot_dir, filename)
            snapshot = {
                'signal': signal.to_dict(),
                'snapshot_time_iso': self._now_iso(),
                'snapshot_time_ts': time.time(),
                'monitor_stats': dict(self._diag_stats),
                'strategy_meta': {
                    'strategy_id': self.STRATEGY_ID,
                    'strategy_type': self.STRATEGY_TYPE,
                    'capital_allocation': self.CAPITAL_ALLOCATION,
                    'simulated': True,
                    'note': 'S5套利策略V1实盘模拟，不参与资金分配，不实际下单',
                },
            }
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
                self._diag_stats['snapshots_saved'] += 1
            except (OSError, IOError, ValueError, TypeError) as e:
                logger.warning("[S5-ARB-V1] 快照保存失败 file=%s error=%s", filepath, e)
                raise
            assert os.path.exists(filepath), f"快照文件保存后不存在: {filepath}"
            return filepath

    def get_monitoring_report(self) -> Dict[str, Any]:
        with self._lock:
            layer1 = self.diagnose_layer1_hft_signal()
            layer2 = self.diagnose_layer2_main_capture()
            layer3 = self.diagnose_layer3_standby_block()
            pair_states = []
            for pair_id in self._pairs:
                pair_states.append({
                    'pair_id': pair_id,
                    'z_score': round(self.compute_zscore(pair_id), 4),
                    'hedge_ratio': round(self._hedge_ratios.get(pair_id, 1.0), 4),
                    'half_life_sec': round(self._half_life_sec.get(pair_id, 0.0), 4),
                    'cointegration_pvalue': round(self._cointegration_pvalues.get(pair_id, 1.0), 6),
                    'structural_break_count': self._structural_break_count,
                })
            # FIX-20260713-S5-MULTISRC: 多源数据采集状态聚合
            leg_states = {}
            for instr in set(list(self._leg_ob_imbalance.keys()) +
                             list(self._leg_open_interest.keys()) +
                             list(self._leg_vwap.keys())):
                leg_states[instr] = {
                    'order_book_imbalance': round(self._leg_ob_imbalance.get(instr, 0.0), 6),
                    'bid_volume1': self._leg_bid_volume.get(instr, 0.0),
                    'ask_volume1': self._leg_ask_volume.get(instr, 0.0),
                    'open_interest': self._leg_open_interest.get(instr, 0.0),
                    'oi_trend': self._leg_oi_trend.get(instr, 'stable'),
                    'vwap': round(self._leg_vwap.get(instr, 0.0), 6),
                    'trade_direction': self._leg_trade_direction.get(instr, 0),
                }
            return {
                'strategy_id': self.STRATEGY_ID,
                'strategy_type': self.STRATEGY_TYPE,
                'capital_allocation': self.CAPITAL_ALLOCATION,
                'report_time_iso': self._now_iso(),
                'report_time_ts': time.time(),
                'diagnostics': {'layer1_hft_signal': layer1, 'layer2_main_capture': layer2, 'layer3_standby_block': layer3},
                'pair_states': pair_states,
                'recent_simulated_signals': [s.to_dict() for s in self._simulated_signals[-10:]],
                'stats': dict(self._diag_stats),
                # FIX-20260713-S5-MULTISRC: 多源数据采集状态（不限于期权五态/HFT管道）
                'data_source_mask': self._data_source_mask,
                'data_sources_active': {
                    'tick': bool(self._data_source_mask & 0x01),
                    'order_book': bool(self._data_source_mask & 0x02),
                    'trade': bool(self._data_source_mask & 0x04),
                    'open_interest': bool(self._data_source_mask & 0x08),
                    'bar': bool(self._data_source_mask & 0x10),
                },
                'leg_multisrc_state': leg_states,
                # FIX-20260713-S5-V2ABSORB: V2 顶级基金组件状态
                'v2_regime_states': {pid: rc.get_state_probs() for pid, rc in self._regime_classifiers.items()},
                'v2_cv_scores': {pid: self._cv_scorer.get_score(pid) for pid in self._pairs},
                'v2_tf_z_scores': dict(self._tf_z_scores),
            }

    def get_last_simulated_signal(self) -> Optional[SimulatedArbitrageSignal]:
        with self._lock:
            return self._simulated_signals[-1] if self._simulated_signals else None

    def get_stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._diag_stats)

    @classmethod
    def get_instance(cls) -> 'ArbitrageMonitor':
        return get_arbitrage_monitor()


# ============================================================================
# 模块级单例
# ============================================================================

_monitor_instance: Optional[ArbitrageMonitor] = None
_monitor_lock = threading.Lock()


def get_arbitrage_monitor() -> ArbitrageMonitor:
    global _monitor_instance
    with _monitor_lock:
        if _monitor_instance is None:
            _monitor_instance = ArbitrageMonitor()
        return _monitor_instance
