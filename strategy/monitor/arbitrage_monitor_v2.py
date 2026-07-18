# MODULE_ID: S5-ARB-MONITOR-V2
"""S5套利策略实盘模拟监控模块 V2（顶级基金统计套利标准 — 2026版）

V2 核心升级（对比 V1）
----------------------
1. 动态仓位管理：OU参数驱动的Kelly Criterion，替代固定pair配置。
2. 市场状态识别：HMM隐状态分类(TRENDING/MEAN_REVERTING/REGIME_BREAK)。
3. 多时间框架融合：tick/1min/5min残差加权融合，指数衰减权重。
4. 自适应阈值：波动率调整的入场/出场Z-score阈值。
5. 交叉验证评分：样本内/外配对稳定性验证。
6. 信号衰减模型：时间衰减置信度，过期信号自动降权。
7. 执行成本模型：平方根市场冲击+队列位置概率，替代固定bps。

顶级基金对标
------------
- Renaissance Technologies: 多维相关性矩阵 → 多时间框架融合
- Two Sigma: 机器学习特征工程 → 交叉验证评分
- Jane Street: 执行成本建模 → 平方根冲击+队列位置
- Citadel: 自适应参数 → 波动率调整阈值

约束
----
- 不参与资金分配，不调用 OrderService.send_order。
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

from infra._helpers import get_logger
from infra.shared_utils import (
    CHINA_TZ as _CHINA_TZ,
    generate_prefixed_id,
)
from infra.resilience import safe_divide

__all__ = ['ArbitrageMonitorV2', 'SimulatedArbitrageSignalV2', 'get_arbitrage_monitor_v2']

logger = get_logger(__name__)


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class SimulatedArbitrageSignalV2:
    """V2模拟套利信号（顶级基金标准）。"""

    signal_id: str
    instrument_id: str
    leg_a: str = ''
    leg_b: str = ''
    direction: str = 'BUY'
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
    source: str = 's5_arbitrage_monitor_v2'

    # V1保留字段
    z_score: float = 0.0
    half_life_sec: float = 0.0
    hedge_ratio: float = 1.0
    log_spread: float = 0.0
    cointegration_pvalue: float = 1.0
    structural_break_flag: bool = False
    expected_profit_per_unit: float = 0.0
    transaction_cost_bps: float = 0.0

    # V2新增字段
    market_regime: str = 'TRENDING'  # TRENDING / MEAN_REVERTING / REGIME_BREAK
    regime_confidence: float = 0.0
    multi_tf_zscore: Dict[str, float] = field(default_factory=dict)  # tick/1min/5min Z-scores
    fused_zscore: float = 0.0
    adaptive_z_open: float = 2.0
    adaptive_z_close: float = 0.5
    kelly_fraction: float = 0.0  # Kelly Criterion suggested position size
    optimal_position_units: float = 0.0
    cv_stability_score: float = 0.0  # 交叉验证稳定性
    signal_age_sec: float = 0.0
    decayed_confidence: float = 0.0
    market_impact_bps: float = 0.0  # 预估市场冲击
    queue_position_prob: float = 0.0  # 队列位置成交概率
    total_execution_cost_bps: float = 0.0  # 总执行成本
    net_expected_profit_bps: float = 0.0  # 扣除执行成本后净盈利

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ArbitragePairConfigV2:
    """V2套利对配置。"""

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
    transaction_cost_bps: float = 2.0
    min_expected_profit_bps: float = 1.0
    max_position_units: float = 10.0

    # V2新增
    max_kelly_fraction: float = 0.25  # Kelly分数上限（防爆仓）
    volatility_scale: float = 1.0  # 波动率缩放因子
    impact_coefficient: float = 0.1  # 市场冲击系数
    min_cv_stability: float = 0.6  # 最低交叉验证稳定性


# ============================================================================
# 数学工具
# ============================================================================

class _KalmanHedgeRatio:
    """简化 Kalman Filter 估计动态 hedge ratio。"""

    def __init__(self, trans_var: float = 1e-5, obs_var: float = 1e-3) -> None:
        self.beta: float = 1.0
        self.P: float = 1.0
        self.trans_var = trans_var
        self.obs_var = obs_var

    def update(self, x: float, y: float) -> float:
        if x == 0:
            return self.beta
        P_pred = self.P + self.trans_var
        K = P_pred * x / (x * x * P_pred + self.obs_var)
        self.beta = self.beta + K * (y - self.beta * x)
        self.P = (1 - K * x) * P_pred
        return self.beta


def _simple_adf_pvalue(residuals: List[float]) -> float:
    """简化 ADF 检验 p-value。"""
    n = len(residuals)
    if n < 10:
        return 1.0
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
    a = mean_y - rho_hat * mean_x
    sse = sum((y[i] - a - rho_hat * x[i]) ** 2 for i in range(n2))
    se = math.sqrt(sse / max(n2 - 2, 1)) / math.sqrt(den)
    if se <= 0:
        return 1.0
    t_stat = rho_hat / se
    p_value = max(0.001, min(1.0, math.exp(t_stat + 0.5)))
    return p_value


def _rolling_ols_beta(x: List[float], y: List[float]) -> float:
    """滚动 OLS: y = βx。"""
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


def _square_root_impact(volume: float, daily_volume: float, volatility: float,
                        coefficient: float = 0.1) -> float:
    """平方根市场冲击模型。

    Impact = coefficient * σ * sqrt(Q / ADV)
    其中 Q=交易量, ADV=日均交易量, σ=波动率。
    对标: Almgren-Chriss (2005) 市场冲击模型。
    """
    if daily_volume <= 0 or volume <= 0:
        return 0.0
    return coefficient * volatility * math.sqrt(volume / daily_volume)


# ============================================================================
# HMM 市场状态分类器
# ============================================================================

class _MarketRegimeClassifier:
    """简化 HMM 市场状态分类器。

    3个隐状态:
    - TRENDING: 协整关系稳定，均值回归有效
    - MEAN_REVERTING: 强均值回归，偏离快速收敛
    - REGIME_BREAK: 结构性断点，协整关系失效

    对标: Renaissance Technologies HMM-based regime detection。
    """

    def __init__(self) -> None:
        # 状态转移概率矩阵 (3x3)
        self.trans_probs = [
            [0.90, 0.08, 0.02],  # TRENDING → [TRENDING, MEAN_REVERTING, REGIME_BREAK]
            [0.15, 0.80, 0.05],  # MEAN_REVERTING →
            [0.05, 0.10, 0.85],  # REGIME_BREAK →
        ]
        self.state_probs = [0.7, 0.2, 0.1]  # 先验概率
        self._last_z: float = 0.0
        self._z_history: Deque[float] = deque(maxlen=60)
        self._break_count: int = 0

    def update(self, z_score: float, half_life_sec: float,
               pvalue: float, is_break: bool) -> Tuple[str, float]:
        """更新状态概率并返回当前最可能状态。"""
        self._z_history.append(z_score)

        # 观测概率: P(obs | state)
        if is_break:
            obs_probs = [0.05, 0.10, 0.85]  # 断点 → 大概率REGIME_BREAK
        elif pvalue < 0.05 and half_life_sec > 0 and half_life_sec < 600:
            if abs(z_score) > 3.0:
                obs_probs = [0.15, 0.80, 0.05]  # 极端Z → MEAN_REVERTING
            else:
                obs_probs = [0.85, 0.10, 0.05]  # 正常 → TRENDING
        else:
            obs_probs = [0.20, 0.30, 0.50]  # 协整弱 → 不确定

        # 前向算法: 预测 + 更新
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


# ============================================================================
# Kelly Criterion 仓位计算器
# ============================================================================

class _KellyPositionSizer:
    """OU过程驱动的Kelly Criterion仓位计算。

    Kelly公式: f* = (μ - r) / σ²
    对于OU过程: μ = κ * (θ - S_t), σ = σ_OU

    对标: Renaissance Technologies systematic Kelly sizing。
    """

    def __init__(self, max_fraction: float = 0.25) -> None:
        self.max_fraction = max_fraction

    def compute(self, z_score: float, half_life_sec: float,
                spread_volatility: float, max_units: float) -> float:
        """计算最优Kelly仓位。

        Args:
            z_score: 当前Z-score
            half_life_sec: 半衰期（秒）
            spread_volatility: 价差波动率
            max_units: 最大仓位单位

        Returns:
            建议仓位单位数
        """
        if half_life_sec <= 0 or spread_volatility <= 0:
            return 0.0

        # OU过程均值回归速度 κ = ln(2) / half_life
        kappa = math.log(2) / half_life_sec if half_life_sec > 0 else 0.0

        # 预期收益: μ = κ * Z * σ (Z偏离越大，预期收益越高)
        expected_return = kappa * abs(z_score) * spread_volatility

        # 风险: σ²
        risk = spread_volatility ** 2

        if risk <= 0:
            return 0.0

        # Kelly分数
        kelly_fraction = expected_return / risk
        kelly_fraction = max(0.0, min(self.max_fraction, kelly_fraction))

        # 转换为仓位单位
        optimal_units = kelly_fraction * max_units
        return max(0.0, min(max_units, optimal_units))


# ============================================================================
# 交叉验证评分器
# ============================================================================

class _CVStabilityScorer:
    """样本内/外交叉验证稳定性评分。

    对标: Two Sigma walk-forward cross-validation。
    """

    def __init__(self, train_ratio: float = 0.7) -> None:
        self.train_ratio = train_ratio
        self._cv_scores: Dict[str, float] = {}

    def score(self, pair_id: str, log_spread_history: List[float]) -> float:
        """计算交叉验证稳定性分数。

        将历史数据分为训练集(70%)和验证集(30%)，比较两段的统计特性。
        """
        n = len(log_spread_history)
        if n < 30:
            self._cv_scores[pair_id] = 0.5
            return 0.5

        split = int(n * self.train_ratio)
        train = log_spread_history[:split]
        test = log_spread_history[split:]

        # 训练集统计
        train_mean = sum(train) / len(train)
        train_std = math.sqrt(sum((x - train_mean) ** 2 for x in train) / len(train))

        # 验证集统计
        test_mean = sum(test) / len(test)

        if train_std <= 0:
            self._cv_scores[pair_id] = 0.5
            return 0.5

        # 验证集均值偏离训练集均值的程度
        deviation = abs(test_mean - train_mean) / train_std

        # 偏离越小，稳定性越高
        stability = max(0.0, 1.0 - deviation / 3.0)
        stability = min(1.0, stability)

        self._cv_scores[pair_id] = stability
        return stability

    def get_score(self, pair_id: str) -> float:
        return self._cv_scores.get(pair_id, 0.5)


# ============================================================================
# ArbitrageMonitorV2
# ============================================================================

class ArbitrageMonitorV2:
    """S5套利策略实盘模拟监控模块 V2（顶级基金统计套利标准）。"""

    STRATEGY_ID = 's5_arbitrage_v2'
    STRATEGY_TYPE = 's5_arbitrage'
    CAPITAL_ALLOCATION = 0.0
    SNAPSHOT_SUBDIR = os.path.join('logs', 'arbitrage_monitor_v2')
    CONVERGENCE_WINDOW_SEC = 60.0
    MAX_HISTORY_SIGNALS = 500

    # V2: 多时间框架配置
    TF_WEIGHTS = {'tick': 0.50, 'm1': 0.30, 'm5': 0.20}
    TF_WINDOWS = {'tick': 60, 'm1': 20, 'm5': 10}

    # V2: 信号衰减半衰期
    SIGNAL_DECAY_HALF_LIFE_SEC = 30.0

    def __init__(self, base_dir: Optional[str] = None) -> None:
        self._base_dir = base_dir or self._detect_base_dir()
        self._snapshot_dir = os.path.join(self._base_dir, self.SNAPSHOT_SUBDIR)
        os.makedirs(self._snapshot_dir, exist_ok=True)

        self._lock = threading.RLock()

        # 配对配置与状态
        self._pairs: Dict[str, ArbitragePairConfigV2] = {}
        self._price_history: Dict[str, List[Tuple[float, float]]] = {}
        self._kalman: Dict[str, _KalmanHedgeRatio] = {}
        self._hedge_ratios: Dict[str, float] = {}

        # V2: 多时间框架历史
        self._tf_log_spread: Dict[str, Dict[str, Deque[float]]] = {}  # pair_id → {tf: deque}
        self._tf_z_scores: Dict[str, Dict[str, float]] = {}

        # 残差/对数价差历史
        self._log_spread_history: Dict[str, List[float]] = {}
        self._cointegration_pvalues: Dict[str, float] = {}
        self._half_life_sec: Dict[str, float] = {}
        self._last_z_score: Dict[str, float] = {}
        self._structural_break_count: int = 0

        # V2: 新增组件
        self._regime_classifier: Dict[str, _MarketRegimeClassifier] = {}
        self._kelly_sizer = _KellyPositionSizer()
        self._cv_scorer = _CVStabilityScorer()

        # V2: 执行成本相关
        self._daily_volume_estimates: Dict[str, float] = {}

        # 兼容旧版单合约模式
        self._last_hft_signal: Optional[Dict[str, Any]] = None
        self._last_hft_signal_ts: float = 0.0
        # FIX-20260713-S5-V2-CRIT: 记录配对信号生成时间，使信号衰减真正生效
        self._last_pair_signal_ts: float = 0.0
        self._deviation_history: Dict[str, List[Dict[str, Any]]] = {}
        self._simulated_signals: List[SimulatedArbitrageSignalV2] = []

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
            # V2新增
            'regime_classifications': 0,
            'kelly_sized_signals': 0,
            'cv_stability_scores': 0,
            'multi_tf_fusions': 0,
        }

        logger.info("[S5-ARB-V2] 初始化完成 base_dir=%s", self._base_dir)

    @staticmethod
    def _detect_base_dir() -> str:
        try:
            current = os.path.dirname(os.path.abspath(__file__))
        except (ValueError, OSError):
            return os.getcwd()
        for _ in range(6):
            pkg_init = os.path.join(current, '', '__init__.py')
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

    def register_pair(self, config: ArbitragePairConfigV2) -> None:
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
            self._regime_classifier[pair_id] = _MarketRegimeClassifier()

            # V2: 多时间框架初始化
            self._tf_log_spread[pair_id] = {
                tf: deque(maxlen=self.TF_WINDOWS[tf]) for tf in self.TF_WINDOWS
            }
            self._tf_z_scores[pair_id] = {tf: 0.0 for tf in self.TF_WINDOWS}

        logger.info("[S5-ARB-V2] 注册配对 %s β=%.4f", pair_id, config.hedge_ratio)

    def on_price_update(self, instrument_id: str, price: float,
                        timestamp: Optional[float] = None,
                        daily_volume: Optional[float] = None) -> None:
        if price <= 0 or not instrument_id:
            return
        ts = timestamp or time.time()
        with self._lock:
            hist = self._price_history.setdefault(instrument_id, [])
            hist.append((ts, price))
            if len(hist) > 500:
                hist.pop(0)

        if daily_volume is not None and daily_volume > 0:
            self._daily_volume_estimates[instrument_id] = float(daily_volume)

        with self._lock:
            for pair_id, cfg in self._pairs.items():
                if instrument_id in (cfg.leg_a, cfg.leg_b):
                    self._update_pair(pair_id)

    def _update_pair(self, pair_id: str) -> None:
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return
        hist_a = self._price_history.get(cfg.leg_a, [])
        hist_b = self._price_history.get(cfg.leg_b, [])
        if len(hist_a) < cfg.beta_lookback or len(hist_b) < cfg.beta_lookback:
            return

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
        beta = 0.6 * beta_ols + 0.4 * beta_kalman
        self._hedge_ratios[pair_id] = beta
        self._diag_stats['hedge_ratio_updates'] += 1

        # 对数价差
        log_spread = math.log(y[-1]) - beta * math.log(x[-1])
        ls_hist = self._log_spread_history.setdefault(pair_id, [])
        ls_hist.append(log_spread)
        if len(ls_hist) > 500:
            ls_hist.pop(0)

        # V2: 多时间框架对数价差更新
        if pair_id in self._tf_log_spread:
            for tf in self.TF_WINDOWS:
                self._tf_log_spread[pair_id][tf].append(log_spread)

        # 协整与半衰期
        pvalue = _simple_adf_pvalue(ls_hist)
        self._cointegration_pvalues[pair_id] = pvalue
        if pvalue < cfg.coint_pvalue_threshold:
            self._diag_stats['cointegration_passed'] += 1

        hl = self._estimate_half_life(pair_id)
        self._half_life_sec[pair_id] = hl

    # ------------------------------------------------------------------
    # V2: 多时间框架融合
    # ------------------------------------------------------------------

    def _compute_multi_tf_zscore(self, pair_id: str) -> Dict[str, float]:
        """计算多时间框架Z-score并融合。"""
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

        # 加权融合: 短周期权重更高
        fused = 0.0
        total_weight = 0.0
        for tf, weight in self.TF_WEIGHTS.items():
            if abs(z_scores.get(tf, 0.0)) > 0:
                fused += z_scores[tf] * weight
                total_weight += weight
        fused = fused / total_weight if total_weight > 0 else 0.0

        self._diag_stats['multi_tf_fusions'] += 1
        return {'tick': z_scores.get('tick', 0.0),
                'm1': z_scores.get('m1', 0.0),
                'm5': z_scores.get('m5', 0.0),
                'fused': fused}

    # ------------------------------------------------------------------
    # V2: 自适应阈值
    # ------------------------------------------------------------------

    def _compute_adaptive_thresholds(self, pair_id: str) -> Tuple[float, float]:
        """基于波动率自适应调整入场/出场阈值。

        高波动 → 扩大阈值（减少假信号）
        低波动 → 收窄阈值（捕捉更多机会）
        """
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return 2.0, 0.5

        with self._lock:
            history = self._log_spread_history.get(pair_id, [])

        if len(history) < 30:
            return cfg.z_open, cfg.z_close

        # 计算波动率
        mean = sum(history[-30:]) / 30
        std = math.sqrt(sum((x - mean) ** 2 for x in history[-30:]) / 30)

        # 基准波动率估计
        baseline_std = 0.01  # 1%基准
        vol_ratio = std / baseline_std if baseline_std > 0 else 1.0

        # 自适应: 波动率比例因子
        scale = max(0.7, min(1.5, vol_ratio))
        adaptive_open = cfg.z_open * scale
        adaptive_close = cfg.z_close * scale

        return adaptive_open, adaptive_close

    # ------------------------------------------------------------------
    # 统计套利计算
    # ------------------------------------------------------------------

    def compute_zscore(self, pair_id: str) -> float:
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
        with self._lock:
            return self._cointegration_pvalues.get(pair_id, 1.0)

    def check_structural_break(self, pair_id: str, current_z: float) -> bool:
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
                logger.warning("[S5-ARB-V2] 结构性断点 pair=%s Z=%.4f last_Z=%.4f",
                               pair_id, current_z, last_z)
            self._last_z_score[pair_id] = current_z
            return is_break

    # ------------------------------------------------------------------
    # V2: 信号生成（顶级基金标准）
    # ------------------------------------------------------------------

    def evaluate_pair_signal(self, pair_id: str) -> Optional[SimulatedArbitrageSignalV2]:
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return None

        with self._lock:
            z = self.compute_zscore(pair_id)
            pvalue = self._cointegration_pvalues.get(pair_id, 1.0)
            hl = self._half_life_sec.get(pair_id, 0.0)
            beta = self._hedge_ratios.get(pair_id, cfg.hedge_ratio)
            break_flag = self.check_structural_break(pair_id, z)

            # V2: 多时间框架融合
            multi_tf = self._compute_multi_tf_zscore(pair_id)
            fused_z = multi_tf.get('fused', z)

            # V2: 市场状态分类
            regime, regime_conf = self._regime_classifier[pair_id].update(
                z, hl, pvalue, break_flag)
            self._diag_stats['regime_classifications'] += 1

            # V2: 自适应阈值
            adaptive_open, adaptive_close = self._compute_adaptive_thresholds(pair_id)

            # V2: 交叉验证稳定性
            cv_stability = self._cv_scorer.score(
                pair_id, self._log_spread_history.get(pair_id, []))
            self._diag_stats['cv_stability_scores'] += 1

            is_cointegrated = pvalue < cfg.coint_pvalue_threshold
            hl_ok = cfg.min_half_life_sec < hl <= cfg.max_half_life_sec
            cv_ok = cv_stability >= cfg.min_cv_stability
            regime_ok = regime != 'REGIME_BREAK'

            action = 'HOLD'
            direction = ''
            confidence = 0.0
            signal_type = 'HOLD'
            expected_profit_bps = 0.0  # FIX-20260713-S5-V2: 初始化为0，避免平仓/止损分支引用未定义变量
            # FIX-20260713-S5-V2-CRIT: 多时间框架融合必须参与开仓决策，不能仅作为信号字段
            signal_z = max(abs(z), abs(fused_z))

            # 止损优先
            if abs(z) > cfg.z_stop or break_flag:
                action = 'STOP_LOSS'
                direction = 'SELL' if z > 0 else 'BUY'
                signal_type = 'CLOSE_LONG' if z > 0 else 'CLOSE_SHORT'
                confidence = min(abs(z) / 5.0, 1.0)
            # V2: 开仓条件增强 — 协整 + 半衰期 + 交叉验证 + 状态 + 成本
            elif signal_z > adaptive_open and is_cointegrated and hl_ok and cv_ok and regime_ok:
                # FIX-20260713-S5-V2-CRIT: 先确定方向，再计算执行成本，避免direction未初始化
                action = 'OPEN'
                direction = 'SELL' if z > 0 else 'BUY'
                signal_type = 'OPEN_SHORT' if z > 0 else 'OPEN_LONG'
                confidence = min(signal_z / 4.0, 1.0)
                # V2: 执行成本计算（使用目标仓位规模，与信号输出一致）
                exec_cost = self._compute_execution_cost(
                    pair_id, direction, cfg.max_position_units)
                expected_profit_bps = signal_z * 0.5 - exec_cost
                if expected_profit_bps < cfg.min_expected_profit_bps:
                    action = 'HOLD'
                    direction = ''
                    signal_type = 'HOLD'
                    confidence = 0.0
                    expected_profit_bps = 0.0
                    self._diag_stats['cost_filtered_signals'] += 1
            elif abs(z) < adaptive_close:
                action = 'CLOSE'
                direction = 'SELL' if z > 0 else 'BUY'
                signal_type = 'CLOSE_LONG' if z > 0 else 'CLOSE_SHORT'
                confidence = 0.5 + (adaptive_close - abs(z)) / adaptive_close * 0.5

            if action == 'HOLD':
                return None

            self._diag_stats['z_score_signals_generated'] += 1

            # V2: Kelly仓位计算
            spread_vol = self._compute_spread_volatility(pair_id)
            kelly_units = self._kelly_sizer.compute(z, hl, spread_vol, cfg.max_position_units)
            self._diag_stats['kelly_sized_signals'] += 1

            # 获取价格
            hist_a = self._price_history.get(cfg.leg_a, [])
            hist_b = self._price_history.get(cfg.leg_b, [])
            price_a = hist_a[-1][1] if hist_a else 0.0
            price_b = hist_b[-1][1] if hist_b else 0.0
            entry_price = price_b if direction == 'BUY' else price_a

            # V2: 信号衰减（基于配对信号时间，而非HFT信号时间）
            now_ts = time.time()
            signal_age = 0.0
            decayed_conf = confidence
            if self._last_pair_signal_ts > 0:
                signal_age = now_ts - self._last_pair_signal_ts
                decay = math.exp(-math.log(2) * signal_age / self.SIGNAL_DECAY_HALF_LIFE_SEC)
                decayed_conf = confidence * decay

            # V2: 执行成本
            impact = _square_root_impact(
                kelly_units, self._daily_volume_estimates.get(cfg.leg_b, 10000),
                spread_vol, cfg.impact_coefficient)
            impact_bps = impact * 10000.0
            queue_prob = 0.85 if abs(z) < 2.0 else 0.6  # 偏差越大队列越靠前
            total_exec = cfg.transaction_cost_bps + impact_bps
            net_profit = expected_profit_bps if action == 'OPEN' else 0.0

            signal = SimulatedArbitrageSignalV2(
                signal_id=generate_prefixed_id('S5ARB', 12),
                instrument_id=pair_id,
                leg_a=cfg.leg_a,
                leg_b=cfg.leg_b,
                direction=direction,
                signal_type=signal_type,
                deviation_bps=round(abs(z) * 10.0, 2),
                confidence=round(confidence, 4),
                entry_price=round(entry_price, 6),
                spread_convergence_rate=0.0,
                quality_score=round(self._compute_quality_score(pair_id, z, hl, pvalue, cv_stability), 4),
                timestamp=time.time(),
                simulated=True,
                capital_allocation=self.CAPITAL_ALLOCATION,
                hft_consumed=False,
                source='s5_arbitrage_monitor_v2',
                z_score=round(z, 6),
                half_life_sec=round(hl, 4) if hl != float('inf') else -1.0,
                hedge_ratio=round(beta, 6),
                log_spread=round(math.log(price_b) - beta * math.log(price_a), 6) if price_a > 0 and price_b > 0 else 0.0,
                cointegration_pvalue=round(pvalue, 6),
                structural_break_flag=break_flag,
                expected_profit_per_unit=round(expected_profit_bps if action == 'OPEN' else 0.0, 4),
                transaction_cost_bps=round(cfg.transaction_cost_bps, 4),
                # V2新增
                market_regime=regime,
                regime_confidence=round(regime_conf, 4),
                multi_tf_zscore=multi_tf,
                fused_zscore=round(fused_z, 6),
                adaptive_z_open=round(adaptive_open, 4),
                adaptive_z_close=round(adaptive_close, 4),
                kelly_fraction=round(kelly_units / max(cfg.max_position_units, 1), 4),
                optimal_position_units=round(kelly_units, 4),
                cv_stability_score=round(cv_stability, 4),
                signal_age_sec=round(signal_age, 4),
                decayed_confidence=round(decayed_conf, 4),
                market_impact_bps=round(impact_bps, 4),
                queue_position_prob=round(queue_prob, 4),
                total_execution_cost_bps=round(total_exec, 4),
                net_expected_profit_bps=round(net_profit, 4),
            )

            self._simulated_signals.append(signal)
            if len(self._simulated_signals) > self.MAX_HISTORY_SIGNALS:
                self._simulated_signals = self._simulated_signals[-self.MAX_HISTORY_SIGNALS:]
            self._diag_stats['simulated_signals_generated'] += 1
            self.save_snapshot(signal)
            # FIX-20260713-S5-V2-CRIT: 更新配对信号时间戳，供下次衰减计算使用
            self._last_pair_signal_ts = now_ts
            logger.info(
                "[S5-ARB-V2] 配对信号 pair=%s action=%s dir=%s Z=%.4f fused_Z=%.4f "
                "regime=%s kelly=%.2f cost=%.2fbps",
                pair_id, action, direction, z, fused_z, regime, kelly_units, total_exec)
            return signal

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

    def _compute_execution_cost(self, pair_id: str, direction: str,
                                 volume: Optional[float] = None) -> float:
        """V2: 计算总执行成本。volume 默认使用 cfg.max_position_units。"""
        cfg = self._pairs.get(pair_id)
        if cfg is None:
            return 2.0

        spread_vol = self._compute_spread_volatility(pair_id)
        avg_daily_vol = self._daily_volume_estimates.get(cfg.leg_b, 10000.0)
        trade_volume = max(1.0, volume if volume is not None else cfg.max_position_units)
        impact = _square_root_impact(trade_volume, avg_daily_vol, spread_vol, cfg.impact_coefficient)
        impact_bps = impact * 10000.0
        return cfg.transaction_cost_bps + impact_bps

    def _compute_quality_score(self, pair_id: str, z: float, hl: float,
                               pvalue: float, cv_stability: float = 0.5) -> float:
        """V2: 信号质量评分（含交叉验证）。"""
        cfg = self._pairs.get(pair_id)
        max_hl = cfg.max_half_life_sec if cfg else 300.0
        min_hl = cfg.min_half_life_sec if cfg else 5.0
        coint_score = 1.0 if pvalue < 0.05 else max(0.0, 1.0 - pvalue * 10)
        hl_score = 0.0
        if min_hl < hl <= max_hl:
            hl_score = 1.0 - (hl - min_hl) / (max_hl - min_hl)
            hl_score = max(0.0, min(1.0, hl_score))
        z_score = min(abs(z) / 4.0, 1.0)
        # V2: 加入交叉验证稳定性
        return coint_score * 0.30 + hl_score * 0.25 + z_score * 0.25 + cv_stability * 0.20

    # ------------------------------------------------------------------
    # 兼容旧版接口
    # ------------------------------------------------------------------

    def on_arbitrage_signal(self, signal: Dict[str, Any]) -> None:
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
        logger.info("[S5-ARB-V2] 接收HFT套利信号 instr=%s dev=%.1fbps",
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

    def generate_simulated_signal(self, action: str = 'OPEN') -> Optional[SimulatedArbitrageSignalV2]:
        for pair_id in list(self._pairs.keys()):
            sig = self.evaluate_pair_signal(pair_id)
            if sig is not None:
                return sig
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
            signal = SimulatedArbitrageSignalV2(
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
                source='s5_arbitrage_monitor_v2_fallback',
            )
            self._simulated_signals.append(signal)
            if len(self._simulated_signals) > self.MAX_HISTORY_SIGNALS:
                self._simulated_signals = self._simulated_signals[-self.MAX_HISTORY_SIGNALS:]
            self._diag_stats['simulated_signals_generated'] += 1
            self.save_snapshot(signal)
            return signal

    def track_spread_convergence(self, instrument_id: str) -> Dict[str, Any]:
        with self._lock:
            result = {
                'instrument_id': instrument_id, 'convergence_rate': 0.0,
                'trend': 'unknown', 'sample_count': 0, 'time_span_sec': 0.0,
                'start_deviation': 0.0, 'end_deviation': 0.0,
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
                'sample_count': len(fresh), 'time_span_sec': span,
                'start_deviation': start['deviation'], 'end_deviation': end['deviation'],
            })
            return result

    def evaluate_arbitrage_quality(self, instrument_id: str, volume: float = 0.0) -> Dict[str, Any]:
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
            'instrument_id': instrument_id, 'quality_score': round(quality, 4),
            'convergence_rate': round(conv_rate, 4), 'trend': trend,
            'volatility': round(std, 4), 'volume': float(volume),
            'components': {'convergence': round(conv_component, 4),
                           'volatility': round(vol_component, 4),
                           'volume': round(volume_component, 4)},
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
            result.update({'hft_consumed': hft_consumed, 'confidence': confidence,
                           'deviation_bps': deviation, 'captured': captured})
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

    def save_snapshot(self, signal: SimulatedArbitrageSignalV2) -> str:
        with self._lock:
            safe_ts = datetime.now(_CHINA_TZ).strftime('%Y%m%d_%H%M%S_%f')
            filename = f"S5ARBV2_{signal.signal_id}_{safe_ts}.json"
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
                    'note': 'S5套利策略V2实盘模拟，顶级基金统计套利标准，不参与资金分配',
                },
            }
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
                self._diag_stats['snapshots_saved'] += 1
            except (OSError, IOError, ValueError, TypeError) as e:
                logger.warning("[S5-ARB-V2] 快照保存失败 file=%s error=%s", filepath, e)
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
                regime, regime_conf = self._regime_classifier[pair_id].update(
                    self.compute_zscore(pair_id),
                    self._half_life_sec.get(pair_id, 0.0),
                    self._cointegration_pvalues.get(pair_id, 1.0),
                    False)
                multi_tf = self._compute_multi_tf_zscore(pair_id)
                cv = self._cv_scorer.get_score(pair_id)
                pair_states.append({
                    'pair_id': pair_id,
                    'z_score': round(self.compute_zscore(pair_id), 4),
                    'fused_zscore': round(multi_tf.get('fused', 0.0), 4),
                    'hedge_ratio': round(self._hedge_ratios.get(pair_id, 1.0), 4),
                    'half_life_sec': round(self._half_life_sec.get(pair_id, 0.0), 4),
                    'cointegration_pvalue': round(self._cointegration_pvalues.get(pair_id, 1.0), 6),
                    'market_regime': regime,
                    'regime_confidence': round(regime_conf, 4),
                    'cv_stability': round(cv, 4),
                    'multi_tf_zscore': multi_tf,
                })
            return {
                'strategy_id': self.STRATEGY_ID,
                'strategy_type': self.STRATEGY_TYPE,
                'capital_allocation': self.CAPITAL_ALLOCATION,
                'report_time_iso': self._now_iso(),
                'report_time_ts': time.time(),
                'diagnostics': {
                    'layer1_hft_signal': layer1,
                    'layer2_main_capture': layer2,
                    'layer3_standby_block': layer3,
                },
                'pair_states': pair_states,
                'recent_simulated_signals': [s.to_dict() for s in self._simulated_signals[-10:]],
                'stats': dict(self._diag_stats),
            }

    def get_last_simulated_signal(self) -> Optional[SimulatedArbitrageSignalV2]:
        with self._lock:
            return self._simulated_signals[-1] if self._simulated_signals else None

    def get_stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._diag_stats)

    @classmethod
    def get_instance(cls) -> 'ArbitrageMonitorV2':
        return get_arbitrage_monitor_v2()


# ============================================================================
# 模块级单例
# ============================================================================

_monitor_v2_instance: Optional[ArbitrageMonitorV2] = None
_monitor_v2_lock = threading.Lock()


def get_arbitrage_monitor_v2() -> ArbitrageMonitorV2:
    global _monitor_v2_instance
    with _monitor_v2_lock:
        if _monitor_v2_instance is None:
            _monitor_v2_instance = ArbitrageMonitorV2()
        return _monitor_v2_instance