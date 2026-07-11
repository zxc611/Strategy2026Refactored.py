# MODULE_ID: M1-056-SA
"""
signal_source_a.py — v2.8 期权五态 AlphaEngine 与多粒度视图层排序系统

依据文档：三层期权五态排序方案_最终落地方案V2_20260624.md
  §15 (v2.5 Final): AlphaEngine + ClusterView + GlobalView 重命名, 等权默认,
                    Rank 标准化 20日, 多候选列表含 score_delta + rank_confidence,
                    SORTER_CONFIG 配置接口 + V7 反馈槽
  §16 (v2.6): ResonanceEngine 可插拔共振模块（默认关闭）
  §17 (v2.7 Final): D_term_structure 合并 D1+D2, D_momentum 当日, D_type_balance 新增,
                    方向一致性检查, 冷启动回退, resonance_weight=0.5 等权
  §21 (v2.8 Final): 四象限正交分解 D/Q 替代 net_score, confidence 修正公式,
                    entropy/concentration/direction_bias 新增, output_mode 参数化

架构关系（v2.5 定稿）：
  AlphaEngine（核心打分引擎，原信号源A IntraProductSorter）
    ├──→ ClusterView（簇内排名视图层，原信号源B InterProductClusterSorter）
    └──→ GlobalView（全局排名视图层，原信号源C GlobalSorter）

向后兼容：
  - 旧类名 IntraProductSorter / InterProductClusterSorter / GlobalSorter 保留为别名
  - 旧字段 best_month / best_score / candidates 保留（与新字段并存）
  - v2.8: net_score 标记 @deprecated，保留一版本后移除（v2.9）
"""
from __future__ import annotations

import math
import time
import logging
import threading
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.config.tvf_param_loader import (
    MONTH_WEIGHTS_5,
    ASYMMETRIC_DECAY,
    TIER1_WILSON_THRESHOLD,
    TIER2_COVERAGE_THRESHOLD,
    TIER2_CORRECT_UP_THRESHOLD,
    TIER3_CORRECT_UP_THRESHOLD,
    DELTA_MIN,
    DELTA_MAX,
    GAMMA_MAX_FRONT_MONTH,
    THETA_MAX_FRONT_MONTH_PCT,
    FRESHNESS_WEIGHTS,
    RESONANCE_VETO_THRESHOLD,
    RESONANCE_MODE,
    LAYER2_WEIGHT_SCORE1,
    SORTER_CONFIG,
)

logger = logging.getLogger(__name__)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


# ============================================================================
# v2.7 §18.4: 日频方向冲突品种占比监控（边缘风险预埋）
# ============================================================================

class _DirectionConflictMonitor:
    """
     lightweight 日频监控器：统计 resonance_enabled=True 时方向冲突品种占比。

    触发阈值 > 20% 时记录 WARNING，供 V7 紧急审查（不阻塞交易）。
    该监控为 Phase 0 预埋日志层，共振模块默认关闭时不会产生有效样本。
    """

    _WARNING_THRESHOLD = 0.20

    def __init__(self):
        self._lock = threading.Lock()
        self._date: Optional[str] = None
        self._total = 0
        self._conflicts = 0

    def record(self, result: Dict[str, Any]) -> None:
        if not isinstance(result, dict):
            return
        if not result.get('resonance_enabled'):
            return

        today = datetime.now().strftime('%Y-%m-%d')

        with self._lock:
            if self._date is None:
                self._date = today
            elif self._date != today:
                self._flush(self._date)
                self._date = today
                self._total = 0
                self._conflicts = 0

            self._total += 1
            if result.get('direction_conflict') is True:
                self._conflicts += 1

    def _flush(self, date: str) -> None:
        if self._total > 0:
            freq = self._conflicts / self._total
            logger.info(
                f"[direction_conflict_frequency] date={date} "
                f"frequency={freq:.4f} conflicts={self._conflicts} total={self._total}"
            )
            if freq > self._WARNING_THRESHOLD:
                logger.warning(
                    f"[direction_conflict_frequency] {date} 冲突占比 {freq:.2%} > "
                    f"{self._WARNING_THRESHOLD:.0%}，触发 V7 紧急审查阈值"
                )
        else:
            logger.info(f"[direction_conflict_frequency] date={date} no resonance-enabled products ranked")

    def force_flush(self) -> None:
        with self._lock:
            if self._date:
                self._flush(self._date)


# Module-level singleton monitor
_DIRECTION_CONFLICT_MONITOR = _DirectionConflictMonitor()


# ============================================================================
# v2.5 §15.1: Rank 标准化引擎（20日滚动窗口 + Gaussian Copula）
# ============================================================================

class RankNormalizer:
    """
    v2.5: Rank 标准化引擎。

    核心流程：
    1. 维护每个品种的 product_score 滚动历史（默认 20 日窗口）
    2. 计算当前得分在历史中的 Percentile Rank
    3. 通过 Gaussian Copula（inverse normal CDF）转换为 z-score
    4. 截断到 [-3, 3]

    输出：
    - product_score_ts: 时序标准化得分（z-score, [-3,3]）
    - historical_percentile: 百分位 [0, 1]
    """

    def __init__(
        self,
        window_days: int = 20,
        clip_range: Tuple[float, float] = (-3.0, 3.0),
    ):
        self._window_days = max(5, int(window_days))
        self._clip_range = clip_range
        self._history: Dict[str, deque] = {}

        # 延迟初始化 NormalDist（Python 3.8+）
        self._nd = None
        try:
            from statistics import NormalDist
            self._nd = NormalDist()
        except ImportError:
            logger.warning("[RankNormalizer] statistics.NormalDist not available (Python < 3.8), z-score disabled")

    def update_and_normalize(
        self,
        product_id: str,
        score: float,
    ) -> Tuple[float, Optional[float]]:
        """
        更新历史并返回 (product_score_ts, historical_percentile)。

        Args:
            product_id: 品种ID
            score: 当前原始 product_score

        Returns:
            (z_score_clipped, percentile)
            若历史不足 5 个数据点，z_score = 原始 score，percentile = None
        """
        score = _safe_float(score)
        if product_id not in self._history:
            self._history[product_id] = deque(maxlen=self._window_days)

        history = self._history[product_id]

        # 历史不足时，不做标准化
        if len(history) < 5:
            history.append(score)
            return score, None

        # 计算 Percentile Rank
        count_below = sum(1 for h in history if h < score)
        percentile = count_below / len(history)

        # 高斯 Copula 转换
        z_score = score  # 默认退化为原始值
        if self._nd is not None:
            # 裁剪 percentile 避免 inv_cdf 返回 ±inf
            p_clipped = max(0.001, min(0.999, percentile))
            try:
                z_score = self._nd.inv_cdf(p_clipped)
            except (ValueError, OverflowError):
                z_score = score

        # 截断
        clip_lo, clip_hi = self._clip_range
        z_score = max(clip_lo, min(clip_hi, z_score))

        # 更新历史
        history.append(score)

        return round(z_score, 6), round(percentile, 4)

    def get_history(self, product_id: str) -> List[float]:
        """获取品种的历史得分序列（用于 rank_confidence 计算）"""
        return list(self._history.get(product_id, []))


# ============================================================================
# 非对称状态时效衰减引擎（方案文档3.3第1步，v2.0不变）
# ============================================================================

class AsymmetricDecay:
    """非对称状态时效衰减引擎（方案文档3.3第1步）"""

    def __init__(self, sector: str = 'default'):
        self._half_lives = ASYMMETRIC_DECAY.get(sector, ASYMMETRIC_DECAY['default'])

    def decay_counts(
        self,
        counts: Dict[str, int],
        last_update: Dict[str, float],
        now: Optional[float] = None,
    ) -> Dict[str, float]:
        now = now or time.time()
        norm_counts = {k.lower(): v for k, v in counts.items()}
        norm_update = {k.lower(): v for k, v in last_update.items()}
        decayed = {}
        for state in ('cr', 'cf', 'wr', 'wf', 'other'):
            raw = norm_counts.get(state, 0)
            ts = norm_update.get(state, now)
            half_life = self._half_lives.get(state, 60)
            if half_life <= 0 or raw <= 0:
                decayed[state] = float(raw)
                continue
            elapsed = max(0.0, now - ts)
            decayed[state] = raw * (0.5 ** (elapsed / half_life))
        return decayed


class GreeksHardFilter:
    """Greeks硬过滤（方案文档3.4，默认关闭，参数化）"""

    def __init__(self, enabled: bool = False):
        self.enabled = enabled

    def filter(
        self,
        delta: float,
        gamma: float,
        theta: float,
        option_price: float,
        month_type: str,
    ) -> bool:
        if not self.enabled:
            return True
        delta = _safe_float(delta)
        gamma = _safe_float(gamma)
        theta = _safe_float(theta)
        option_price = _safe_float(option_price)
        if abs(delta) < DELTA_MIN or abs(delta) > DELTA_MAX:
            return False
        if month_type == 'front_month' and gamma > GAMMA_MAX_FRONT_MONTH:
            return False
        if month_type == 'front_month' and option_price > 0:
            if abs(theta) / option_price > THETA_MAX_FRONT_MONTH_PCT:
                return False
        return True

    def filter_batch(self, options: List[Dict]) -> List[Dict]:
        return [
            opt for opt in (options or [])
            if self.filter(
                opt.get('delta', 0.5),
                opt.get('gamma', 0.03),
                opt.get('theta', -0.5),
                opt.get('option_price', 100.0),
                opt.get('month_type', 'quarter_month'),
            )
        ]


# ============================================================================
# v2.5: rank_confidence 计算函数
# ============================================================================

def _candidate_primary_score(cand: Dict) -> float:
    """v2.8.1: 提取候选的主信号得分（scheme 感知）。

    优先级：primary_score > net_score（向后兼容）。
    primary_score 由 AlphaEngine.rank() 按 scoring_scheme 填充：
      scheme_1 → net_score, scheme_2/3/all → D
    """
    return _safe_float(cand.get('primary_score', cand.get('net_score', 0.0)))


def compute_rank_confidence_default(
    month_candidates: List[Dict],
    product_history: Optional[List[float]] = None,
) -> List[float]:
    """
    v2.5 §15.1 拷问三: rank_confidence 默认计算。

    默认实现：候选月份主信号（primary_score）相对于该品种 20 日滚动历史得分的 Percentile Rank。
    取值 [0, 1]，0.5 表示中位数，0.9 表示历史前 10% 强度。

    v2.8.1: 使用 primary_score（scheme 感知）替代硬编码 net_score。
    历史不足时退化为相邻 gap 的相对大小（_fallback_gap_confidence）。
    """
    if product_history is not None and len(product_history) >= 10:
        confidences = []
        for cand in month_candidates:
            score = _candidate_primary_score(cand)
            rank = sum(1 for h in product_history if h < score) / len(product_history)
            confidences.append(max(0.0, min(1.0, rank)))
        return confidences

    return _fallback_gap_confidence(month_candidates)


def _fallback_gap_confidence(month_candidates: List[Dict]) -> List[float]:
    """
    v2.5: 历史数据不足时的退避实现：基于相邻候选 score gap 的相对大小。
    gap 越大，confidence 越高（排名越稳定）。

    v2.8.1: 使用 primary_score（scheme 感知）替代硬编码 net_score。
    """
    n = len(month_candidates)
    if n == 0:
        return []
    confidences = []
    for i in range(n):
        if i == n - 1:
            # 末位候选：与倒数第二的 gap
            if i > 0:
                gap = abs(
                    _candidate_primary_score(month_candidates[i])
                    - _candidate_primary_score(month_candidates[i - 1])
                )
            else:
                gap = 0.0
        else:
            gap = _safe_float(month_candidates[i].get('score_delta_to_next', 0.0))
        # gap > 0.10 → confidence → 1.0; gap < 0.01 → confidence → 0.0
        confidences.append(max(0.0, min(1.0, gap * 10)))
    return confidences


def compute_score_delta_to_next(month_candidates: List[Dict]) -> List[Optional[float]]:
    """计算每个候选与下一候选的主信号差距（末位为 None）

    v2.8.1: 使用 primary_score（scheme 感知）替代硬编码 net_score。
    """
    n = len(month_candidates)
    if n == 0:
        return []
    deltas = []
    for i in range(n):
        if i == n - 1:
            deltas.append(None)
        else:
            curr = _candidate_primary_score(month_candidates[i])
            nxt = _candidate_primary_score(month_candidates[i + 1])
            deltas.append(round(curr - nxt, 6))
    return deltas


# ============================================================================
# v2.8 §21: 四象限正交分解 D/Q（替代 net_score 作为核心方向信号）
# ============================================================================

def compute_orthogonal_decomposition(counts: Dict[str, float]) -> Dict[str, Any]:
    """
    v2.8 §21.1 评审二: 四象限正交分解（核心升级）。

    将 CR/CF/WR/WF 分解为:
    - D (方向强度): 列边际 (看涨 - 看跌) = (CR + WR - CF - WF) / signal_total
    - Q (信号质量): 行边际对角优势 = (CR + WF - CF - WR) / signal_total

    D 与 Q 在 2×2 列联表中严格正交 (系数向量点积 = 0)。
    替代 v2.7 的 net_score = (CR + CF - WR - WF) / Total。

    Args:
        counts: 五态计数字典 {'CR': n, 'CF': n, 'WR': n, 'WF': n, 'Other': n}
                 值可以是 int 或 float（衰减后）

    Returns:
        包含 D/Q/direction/quality_level 等字段的字典
    """
    total = sum(counts.values()) or 1
    cr = counts.get('CR', 0) or 0
    cf = counts.get('CF', 0) or 0
    wr = counts.get('WR', 0) or 0
    wf = counts.get('WF', 0) or 0

    signal_total = cr + cf + wr + wf
    if signal_total == 0:
        return {
            'D': 0.0,
            'Q': 0.0,
            'direction': 'neutral',
            'quality_level': 'low',
            'direction_strength': 0.0,
            'signal_quality': 0.0,
            'participation': 0.0,
        }

    # 方向强度 D：列边际（看涨 - 看跌）
    D = (cr + wr - cf - wf) / signal_total

    # 信号质量 Q：对角优势（CR+WF vs CF+WR）
    Q = (cr + wf - cf - wr) / signal_total

    # 方向判定（基于 D）
    direction = 'long' if D > 0.1 else 'short' if D < -0.1 else 'neutral'

    # 质量判定（基于 Q）
    quality_level = 'high' if Q > 0.3 else 'medium' if Q > 0 else 'low'

    return {
        'D': round(D, 4),
        'Q': round(Q, 4),
        'direction': direction,
        'quality_level': quality_level,
        'direction_strength': round(abs(D), 4),
        'signal_quality': round(Q, 4),
        'participation': round(signal_total / total, 4),
    }


def compute_confidence_v28(counts: Dict[str, float]) -> float:
    """
    v2.8 §21.1 评审一: 修正后的置信度公式。

    核心思想：置信度 = 市场参与度 × 方向清晰度
    - 参与度 = (CR + CF + WR + WF) / Total
    - 清晰度 = |正确 - 错误| / (正确 + 错误), [0, 1]
    - 置信度 = 参与度 × 清晰度, [0, 1]

    修正了用户原方案量纲不一致的问题：
    原 confidence = 1 - (Other/Total)×2 - |CR+CF-WR-WF|/Total 存在反直觉行为。
    """
    total = sum(counts.values()) or 1
    cr = counts.get('CR', 0) or 0
    cf = counts.get('CF', 0) or 0
    wr = counts.get('WR', 0) or 0
    wf = counts.get('WF', 0) or 0

    # 市场参与度：非 Other 状态占比
    participation = (cr + cf + wr + wf) / total

    # 方向清晰度：正确状态与错误状态的比率
    correct = cr + cf
    wrong = wr + wf
    if correct + wrong == 0:
        clarity = 0.0
    else:
        clarity = abs(correct - wrong) / (correct + wrong)

    # 置信度 = 参与度 × 清晰度
    confidence = participation * clarity

    return round(confidence, 4)


def compute_entropy_features(counts: Dict[str, float]) -> Dict[str, float]:
    """
    v2.8 §21.1 评审三: 信息熵特征（修正版）。

    保留熵和集中度（信息论标准做法），删除偏度（术语冲突 + 数学错误），
    新增 direction_bias 替代原 skewness。

    - entropy: 香农熵（bit）
    - concentration: 1 - entropy/log2(5), [0, 1]
    - direction_bias: (CR+WR-CF-WF)/Total = D × participation, [-1, 1]
    """
    total = sum(counts.values()) or 1

    # 香农熵
    entropy = 0.0
    for state in ['CR', 'CF', 'WR', 'WF', 'Other']:
        p = (counts.get(state, 0) or 0) / total
        if p > 0:
            entropy -= p * math.log2(p)

    max_entropy = math.log2(5)
    concentration = 1 - entropy / max_entropy if max_entropy > 0 else 0.0  # [0, 1]

    # 方向性偏差（替代原 skewness）
    cr = counts.get('CR', 0) or 0
    cf = counts.get('CF', 0) or 0
    wr = counts.get('WR', 0) or 0
    wf = counts.get('WF', 0) or 0
    direction_bias = (cr + wr - cf - wf) / total  # [-1, 1]

    return {
        'entropy': round(entropy, 4),
        'max_entropy': round(max_entropy, 4),
        'concentration': round(concentration, 4),
        'direction_bias': round(direction_bias, 4),
    }


# ============================================================================
# v2.5 AlphaEngine: 核心打分引擎（原信号源A IntraProductSorter）
# ============================================================================

class AlphaEngine:
    """v2.8: 核心打分引擎（原信号源A IntraProductSorter）

    核心流程（v2.8 定稿）：
    1. 非对称状态时效衰减
    2. 等权/加权月份汇总（默认等权，消除静态先验）
    3. v2.8: 四象限正交分解 D/Q 替代 net_score
    4. Rank 标准化（20日窗口，Gaussian Copula，截断[-3,3]）
    5. 多候选列表输出（含 score_delta_to_next + rank_confidence）

    v2.6/v2.7 扩展：
    - 当 SORTER_CONFIG['resonance_enabled']=True 时，集成 ResonanceEngine
    - 输出追加 resonance_score, final_score, direction_conflict 等字段
    - 配置关闭时完全退化为 v2.5

    v2.8 扩展（§21）：
    - scoring_scheme 参数化: scheme_1(实盘默认)/scheme_2/scheme_3/all
    - 方案一(实盘默认): product_score 基于 net_score + confidence 修正版
    - 方案二: product_score 基于 D/Q 正交分解
    - 方案三: product_score 基于 D, 辅助 entropy/concentration/direction_bias
    - all: 全字段输出(research 模式)
    - net_score 标记 @deprecated，保留一版本后移除（v2.9）

    向后兼容：
    - 旧字段 best_month, best_score, candidates 保留
    - best_month = net_score 最高的月份
    - best_score = product_score（品种汇总得分）
    """

    MAX_MONTHS = 5

    def __init__(
        self,
        pure_mode: bool = False,
        hard_filter_enabled: bool = False,
        sector: str = 'default',
        sorter_config: Optional[Dict] = None,
    ):
        self.pure_mode = pure_mode
        self._decay = AsymmetricDecay(sector)
        self._hard_filter = GreeksHardFilter(enabled=hard_filter_enabled)

        # v2.5: 加载 SORTER_CONFIG
        self._config = sorter_config if sorter_config is not None else SORTER_CONFIG
        self._rank_window_days = int(self._config.get('rank_window_days', 20))
        self._enable_rank_normalize = bool(self._config.get('enable_rank_normalize', True))
        self._rank_clip = tuple(self._config.get('rank_normalize_clip', (-3.0, 3.0)))
        self._month_weights_cfg = self._config.get('month_weights', None)
        self._output_candidates_max = int(self._config.get('output_candidates_max', 5))
        self._rank_confidence_method = self._config.get('rank_confidence_method', 'percentile')

        # v2.8: output_mode 参数化（research=全字段 / production=瘦身字段）
        self._output_mode = self._config.get('output_mode', 'research')
        self._production_fields = self._config.get('production_fields', None)

        # v2.8 §21: 方案开关机制
        # scheme_1(实盘默认): product_score=net_score, 主信号=confidence
        # scheme_2: product_score=D, 主信号=D/Q
        # scheme_3: product_score=D, 主信号=entropy/concentration/direction_bias
        # all: 全字段输出
        self._scoring_scheme = self._config.get('scoring_scheme', 'scheme_1')
        if self._scoring_scheme not in ('scheme_1', 'scheme_2', 'scheme_3', 'all'):
            logger.warning(
                f"[AlphaEngine] Invalid scoring_scheme='{self._scoring_scheme}', "
                f"falling back to 'scheme_1'"
            )
            self._scoring_scheme = 'scheme_1'

        # v2.5: Rank 标准化引擎
        self._rank_normalizer = RankNormalizer(
            window_days=self._rank_window_days,
            clip_range=self._rank_clip,
        ) if self._enable_rank_normalize else None

        # v2.6/v2.7: ResonanceEngine（延迟导入，避免循环依赖）
        self._resonance_engine = None
        if bool(self._config.get('resonance_enabled', False)):
            try:
                from ali2026v3_trading.data.three_layer_sort.resonance_engine import ResonanceEngine
                self._resonance_engine = ResonanceEngine(config=self._config)
            except ImportError as e:
                logger.warning(f"[AlphaEngine] ResonanceEngine import failed, resonance disabled: {e}")
                self._resonance_engine = None

    @staticmethod
    def resolve_month_weights(n_months: int) -> Tuple[float, ...]:
        """v2.0 向后兼容: 静态月份权重（仅当配置指定加权时使用）"""
        if n_months <= 0:
            return ()
        if n_months > AlphaEngine.MAX_MONTHS:
            return MONTH_WEIGHTS_5
        raw = MONTH_WEIGHTS_5[:n_months]
        s = sum(raw)
        return tuple(w / s for w in raw) if s > 0 else tuple(1.0 / n_months for _ in range(n_months))

    def _resolve_weights(self, n_months: int) -> Tuple[float, ...]:
        """
        v2.5: 月份权重解析。
        - 默认等权（month_weights=None）
        - 配置传入权重向量时按长度归一化
        """
        if self._month_weights_cfg is None:
            # v2.5 默认：等权
            return tuple(1.0 / n_months for _ in range(n_months))
        # 配置指定权重
        cfg = self._month_weights_cfg
        if isinstance(cfg, (list, tuple)):
            raw = list(cfg)[:n_months]
            if len(raw) < n_months:
                raw.extend([0.0] * (n_months - len(raw)))
            s = sum(raw)
            if s > 0:
                return tuple(w / s for w in raw)
        # 回退到等权
        return tuple(1.0 / n_months for _ in range(n_months))

    @staticmethod
    def wilson_lower_bound(pos: float, total: float, z: float = 1.96) -> float:
        if total <= 0:
            return 0.0
        p = pos / total
        n = total
        denom = 1.0 + z * z / n
        center = p + z * z / (2 * n)
        spread = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
        return max(0.0, (center - spread) / denom)

    @staticmethod
    def determine_tier(
        coverage: float,
        wilson: float,
        correct_up_pct: float,
        noise_ratio: float = 0.0,
    ) -> int:
        # v2.8: 从 SORTER_CONFIG 读取 Tier 阈值（参数化），回退到模块常量
        _tier1_wilson = SORTER_CONFIG.get('tier1_wilson_threshold', TIER1_WILSON_THRESHOLD)
        _tier2_coverage = SORTER_CONFIG.get('tier2_coverage_threshold', TIER2_COVERAGE_THRESHOLD)
        _tier2_correct_up = SORTER_CONFIG.get('tier2_correct_up_threshold', TIER2_CORRECT_UP_THRESHOLD)
        _tier3_correct_up = SORTER_CONFIG.get('tier3_correct_up_threshold', TIER3_CORRECT_UP_THRESHOLD)

        if correct_up_pct > 0:
            if coverage >= 0.8 and wilson >= _tier1_wilson:
                return 1
            if coverage >= _tier2_coverage and correct_up_pct >= _tier2_correct_up:
                return 2
            if correct_up_pct >= _tier3_correct_up:
                return 3
            return 4
        else:
            if coverage == 0.0:
                return 3
            return 4

    def rank(
        self,
        product_id: str,
        month_data_list: List[Dict],
        call_counts: Optional[Dict] = None,
        put_counts: Optional[Dict] = None,
        current_price: Optional[float] = None,
        prev_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        v2.8 核心打分流程。

        Args:
            product_id: 品种ID
            month_data_list: 月份五态数据列表
            call_counts: Call 五态汇总计数（v2.7 共振用）
            put_counts: Put 五态汇总计数（v2.7 共振用）
            current_price: 标的当前价格（v2.7 共振用）
            prev_price: 标的前一日价格（v2.7 共振用）

        Returns:
            v2.8 完整输出格式（含 v2.8 D/Q + v2.5 多候选 + v2.7 共振字段）
        """
        now = time.time()
        n_months = min(len(month_data_list), self.MAX_MONTHS)

        if n_months == 0:
            return self._empty_result(product_id, now)

        weights = self._resolve_weights(n_months)
        candidates = []
        total_cr_eff = 0.0
        total_rise_eff = 0.0
        weighted_correct_up = 0.0
        weighted_total = 0.0
        active_months = 0

        # v2.8: 品种级五态汇总（用于 D/Q/confidence/entropy 计算）
        agg_counts = {'CR': 0.0, 'CF': 0.0, 'WR': 0.0, 'WF': 0.0, 'Other': 0.0}
        # v2.8: 月份加权 D/Q/net_score 汇总
        weighted_D_sum = 0.0
        weighted_Q_sum = 0.0
        weighted_net_score_sum = 0.0

        for i in range(n_months):
            mdata = month_data_list[i]
            w = weights[i]
            counts = mdata.get('counts', {})
            last_update = mdata.get('last_update', {})

            if self.pure_mode:
                decayed = {k.lower(): float(v) for k, v in counts.items()}
            else:
                decayed = self._decay.decay_counts(counts, last_update, now)

            cr_eff = decayed.get('cr', 0.0)
            cf_eff = decayed.get('cf', 0.0)
            wr_eff = decayed.get('wr', 0.0)
            wf_eff = decayed.get('wf', 0.0)
            other_eff = decayed.get('other', 0.0)
            total_eff = cr_eff + cf_eff + wr_eff + wf_eff + other_eff

            # v2.7 兼容: net_score 仍保留 (@deprecated)
            net_score = (cr_eff + cf_eff - wr_eff - wf_eff) / total_eff if total_eff > 0 else 0.0
            weighted_score = net_score * w
            weighted_contrib = net_score * w  # v2.5: 该月份对 product_score 的贡献

            # v2.8: 月份级 D/Q 正交分解
            month_dq = compute_orthogonal_decomposition({
                'CR': cr_eff, 'CF': cf_eff, 'WR': wr_eff, 'WF': wf_eff, 'Other': other_eff,
            })
            weighted_D_sum += month_dq['D'] * w
            weighted_Q_sum += month_dq['Q'] * w
            weighted_net_score_sum += net_score * w

            # v2.8: 品种级五态汇总
            agg_counts['CR'] += cr_eff
            agg_counts['CF'] += cf_eff
            agg_counts['WR'] += wr_eff
            agg_counts['WF'] += wf_eff
            agg_counts['Other'] += other_eff

            correct_up = (cr_eff + cf_eff) / (cr_eff + cf_eff + wr_eff + wf_eff) if (cr_eff + cf_eff + wr_eff + wf_eff) > 0 else 0.0
            weighted_correct_up += w * (cr_eff + cf_eff)
            weighted_total += w * (cr_eff + cf_eff + wr_eff + wf_eff)

            total_cr_eff += cr_eff
            total_rise_eff += cr_eff + wr_eff

            if (cr_eff + cf_eff + wr_eff + wf_eff) > 0:
                active_months += 1

            month = mdata.get('month', f'm{i+1}')
            # v2.8.1 fix: primary_score 按 scoring_scheme 选择主信号
            # scheme_1: primary_score = net_score (实盘兼容 v2.7)
            # scheme_2/3/all: primary_score = D (v2.8 正交分解方向强度)
            # 下游排序/置信度/score_delta 统一使用 primary_score，消除 scheme 不一致
            primary_score = net_score if self._scoring_scheme == 'scheme_1' else month_dq['D']
            candidates.append({
                'month': month,
                'net_score': round(net_score, 6),  # @deprecated v2.8, 保留向后兼容
                'D': month_dq['D'],                 # v2.8: 方向强度
                'Q': month_dq['Q'],                 # v2.8: 信号质量
                'primary_score': round(primary_score, 6),  # v2.8.1: scheme 感知主信号
                'weight': round(w, 6),
                'weighted_contrib': round(weighted_contrib, 6),
                'weighted_score': round(weighted_score, 6),  # 向后兼容
                'score': round(weighted_score, 6),  # 向后兼容
                'correct_up_pct': correct_up,
                'cr_eff': cr_eff,
                'cf_eff': cf_eff,
                'wr_eff': wr_eff,
                'wf_eff': wf_eff,
            })

        # v2.8.1 fix: month_candidates 按 primary_score 降序排列（scheme 感知）
        # scheme_1: 按 net_score 降序（v2.7 兼容）
        # scheme_2/3/all: 按 D 降序（v2.8 正交分解方向强度）
        # 向后兼容：primary_score 缺失时回退到 net_score
        month_candidates = sorted(
            candidates,
            key=lambda x: -_safe_float(x.get('primary_score', x.get('net_score', 0.0))),
        )

        # v2.5: 计算 score_delta_to_next
        deltas = compute_score_delta_to_next(month_candidates)
        for i, delta in enumerate(deltas):
            month_candidates[i]['score_delta_to_next'] = delta

        # v2.5: 计算 rank_confidence
        product_history = self._rank_normalizer.get_history(product_id) if self._rank_normalizer else None
        if self._rank_confidence_method == 'percentile':
            confidences = compute_rank_confidence_default(month_candidates, product_history)
        else:
            confidences = _fallback_gap_confidence(month_candidates)
        for i, conf in enumerate(confidences):
            month_candidates[i]['rank_confidence'] = round(conf, 6)

        # v2.8: 品种级 D/Q 正交分解（基于汇总五态）
        product_dq = compute_orthogonal_decomposition(agg_counts)

        # v2.8: confidence = participation × clarity
        product_confidence = compute_confidence_v28(agg_counts)

        # v2.8: entropy features (entropy, concentration, direction_bias)
        product_entropy = compute_entropy_features(agg_counts)

        # v2.8: product_score 根据 scoring_scheme 选择基础
        # scheme_1(实盘默认): product_score = weighted net_score (v2.7 兼容)
        # scheme_2/scheme_3/all: product_score = weighted D (v2.8 正交分解)
        if self._scoring_scheme == 'scheme_1':
            product_score = weighted_net_score_sum
        else:
            product_score = weighted_D_sum

        # v2.5: Rank 标准化 → product_score_ts
        if self._rank_normalizer:
            product_score_ts, historical_percentile = self._rank_normalizer.update_and_normalize(
                product_id, product_score,
            )
        else:
            product_score_ts = product_score
            historical_percentile = None

        # 统计指标
        correct_up_pct = weighted_correct_up / weighted_total if weighted_total > 0 else 0.0
        wilson = self.wilson_lower_bound(total_cr_eff, total_rise_eff)
        coverage = active_months / self.MAX_MONTHS if self.MAX_MONTHS > 0 else 0.0
        tier = self.determine_tier(coverage, wilson, correct_up_pct)

        # v2.5: best_month = net_score 最高的月份（向后兼容）
        best = month_candidates[0] if month_candidates else {}

        # 限制输出候选数量
        output_candidates = month_candidates[:self._output_candidates_max]

        # v2.6/v2.7: ResonanceEngine 集成
        resonance_fields = {
            'resonance_enabled': False,
            'resonance_score': None,
            'resonance_direction': None,
            'resonance_active_dims': None,
            'resonance_fallback_applied': None,
            'direction_conflict': None,
            'conflict_reason': '',
            'final_score': product_score_ts,  # v2.5 退化：final = product_score_ts
            'historical_percentile': historical_percentile,
            'product_score_cs': None,  # 截面标准化由 GlobalView 填充
        }

        if self._resonance_engine is not None and self._resonance_engine.enabled:
            resonance_result = self._resonance_engine.compute(
                month_data_list=month_candidates,
                call_counts=call_counts,
                put_counts=put_counts,
                current_price=current_price,
                prev_price=prev_price,
                product_score_ts=product_score_ts,
            )
            resonance_fields.update(resonance_result)
            # v2.7 fix: AlphaEngine 的 historical_percentile 基于真实滚动历史 Percentile Rank，
            # 必须保留，禁止被 ResonanceEngine 的 CDF 估算覆盖。
            resonance_fields['historical_percentile'] = historical_percentile

        result = {
            'signal_source': 'A',
            'product_id': product_id,
            # === v2.8 方案标识 ===
            'scoring_scheme': self._scoring_scheme,
            # === v2.8 核心字段（四象限正交分解，方案二主信号）===
            'D': product_dq['D'],                          # 方向强度 [-1, 1]
            'Q': product_dq['Q'],                          # 信号质量 [-1, 1]
            'direction': product_dq['direction'],           # long / short / neutral
            'quality_level': product_dq['quality_level'],   # high / medium / low
            'direction_strength': product_dq['direction_strength'],  # |D| [0, 1]
            'signal_quality': product_dq['signal_quality'],  # Q（保留符号）
            # === v2.8 辅助字段（方案一修正 + 方案三保留）===
            'confidence': product_confidence,               # 参与度 × 清晰度 [0, 1]（方案一主信号）
            'concentration': product_entropy['concentration'],  # 集中度 [0, 1]（方案三主信号）
            'direction_bias': product_entropy['direction_bias'],  # 方向性偏差 [-1, 1]（方案三主信号）
            'entropy': product_entropy['entropy'],          # 原始熵值（诊断用）
            'participation': product_dq['participation'],   # 市场参与度
            # === v2.7 兼容字段（方案一主信号）===
            'net_score': round(weighted_net_score_sum, 6),  # @deprecated v2.8, 方案一仍使用
            # === v2.5 新字段 ===
            'product_score': round(product_score, 6),       # v2.8: = weighted D (替代原 net_score 汇总)
            'product_score_ts': round(product_score_ts, 6),
            'month_candidates': output_candidates,
            # === v2.6/v2.7 共振字段 ===
            **resonance_fields,
            # === 向后兼容字段 ===
            'best_month': best.get('month'),
            'best_score': round(product_score, 6),  # v2.8: best_score = product_score (based on D)
            'correct_up_pct': correct_up_pct,
            'tier': tier,
            'month_count': n_months,
            'candidates': output_candidates,  # 向后兼容: 同 month_candidates
            'wilson': wilson,
            'coverage': coverage,
            'timestamp': now,
        }

        # v2.7 §18.4: 预埋方向冲突日频监控
        _DIRECTION_CONFLICT_MONITOR.record(result)

        # v2.8: output_mode 参数化（research=全字段 / production=瘦身字段）
        result = self._apply_output_mode(result)

        return result

    def _empty_result(self, product_id: str, now: float) -> Dict[str, Any]:
        """空结果（无月份数据时）"""
        result = {
            'signal_source': 'A',
            'product_id': product_id,
            # v2.8 方案标识
            'scoring_scheme': self._scoring_scheme,
            # v2.8 核心字段
            'D': 0.0,
            'Q': 0.0,
            'direction': 'neutral',
            'quality_level': 'low',
            'direction_strength': 0.0,
            'signal_quality': 0.0,
            'confidence': 0.0,
            'concentration': 0.0,
            'direction_bias': 0.0,
            'entropy': 0.0,
            'participation': 0.0,
            'net_score': 0.0,  # @deprecated v2.8
            # v2.5 字段
            'product_score': 0.0,
            'product_score_ts': 0.0,
            'month_candidates': [],
            # v2.6/v2.7 共振字段
            'resonance_enabled': bool(self._resonance_engine and self._resonance_engine.enabled),
            'resonance_score': None,
            'resonance_direction': None,
            'resonance_active_dims': None,
            'resonance_fallback_applied': None,
            'direction_conflict': None,
            'conflict_reason': '',
            'final_score': 0.0,
            'historical_percentile': None,
            'product_score_cs': None,
            # 向后兼容
            'best_month': None,
            'best_score': 0.0,
            'correct_up_pct': 0.0,
            'tier': 4,
            'month_count': 0,
            'candidates': [],
            'wilson': 0.0,
            'coverage': 0.0,
            'timestamp': now,
        }
        _DIRECTION_CONFLICT_MONITOR.record(result)
        return self._apply_output_mode(result)

    # v2.8: 各方案对应的 production 输出字段
    _SCHEME_PRODUCTION_FIELDS = {
        'scheme_1': [
            'product_id',
            'net_score',          # 方案一核心信号（@deprecated 但方案一仍使用）
            'confidence',         # 方案一修正版置信度
            'product_score',      # 基于 net_score
            'product_score_ts',
            'final_score',
            'timestamp',
        ],
        'scheme_2': [
            'product_id',
            'D',                  # 方案二核心信号：方向强度
            'Q',                  # 方案二核心信号：信号质量
            'direction',          # 方向判定
            'quality_level',      # 质量分级
            'product_score',      # 基于 D
            'product_score_ts',
            'final_score',
            'timestamp',
        ],
        'scheme_3': [
            'product_id',
            'concentration',      # 方案三核心信号：集中度
            'direction_bias',     # 方案三核心信号：方向偏差
            'entropy',            # 方案三辅助信号：熵
            'product_score',      # 基于 D
            'product_score_ts',
            'final_score',
            'timestamp',
        ],
        'all': None,  # all 模式输出全部字段
    }

    def _apply_output_mode(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        v2.8 §21.3: output_mode + scoring_scheme 参数化。

        - 'research': 全字段输出（默认），用于回测和特征分析
        - 'production': 仅输出 scoring_scheme 对应的字段子集，用于实盘

        注意：month_candidates 和 candidates 列表始终保留（下游消费必需）。
        scoring_scheme 为 'all' 时等价于 research 模式（全字段）。
        """
        # research 模式或 scheme='all' 时全字段输出
        if self._output_mode != 'production':
            return result
        if self._scoring_scheme == 'all':
            return result

        # 确定字段集：优先使用配置的 production_fields，否则用方案默认
        fields_to_keep = self._production_fields
        if fields_to_keep is None:
            fields_to_keep = self._SCHEME_PRODUCTION_FIELDS.get(
                self._scoring_scheme, self._SCHEME_PRODUCTION_FIELDS['scheme_1']
            )

        if not fields_to_keep:
            return result

        # 始终保留列表字段（下游消费必需）
        fields_to_keep = set(fields_to_keep)
        fields_to_keep.update(['month_candidates', 'candidates'])

        return {k: v for k, v in result.items() if k in fields_to_keep}


# 向后兼容别名
IntraProductSorter = AlphaEngine


# ============================================================================
# v2.5 ClusterView: 簇内排名视图层（原信号源B InterProductClusterSorter）
# 非独立信号源，仅做 AlphaEngine 输出的预聚合
# ============================================================================

class ClusterView:
    """v2.5: 簇内排名视图层（原信号源B InterProductClusterSorter）

    非独立信号源，仅做 AlphaEngine 输出的预聚合。
    核心流程（方案文档4.3）：
    1. 簇平均正确率
    2. 各品种共振度
    3. 共振加权得分（可选，参数化）
    4. 排序输出
    """

    def __init__(
        self,
        enable_resonance_weighting: bool = False,
        enable_resonance_veto: bool = False,
        resonance_veto_threshold: float = RESONANCE_VETO_THRESHOLD,
    ):
        self.enable_resonance_weighting = enable_resonance_weighting
        self.enable_resonance_veto = enable_resonance_veto
        self.resonance_veto_threshold = resonance_veto_threshold

    def rank(
        self,
        cluster_id: str,
        product_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """方案文档4.3核心流程"""
        now = time.time()

        if not product_results:
            return {
                'signal_source': 'B',
                'cluster_id': cluster_id,
                'group_accuracy': 0.0,
                'sorted_products': [],
                'timestamp': now,
            }

        products = []
        for pr in product_results:
            cup = pr.get('correct_up_pct', 0.0)
            # v2.5: 优先使用 product_score / final_score，向后兼容 best_score
            score = pr.get('final_score', pr.get('product_score', pr.get('best_score', 0.0)))
            products.append({
                'product_id': pr.get('product_id', ''),
                'product_score': pr.get('product_score', score),
                'final_score': pr.get('final_score', score),
                'best_score': score,  # 向后兼容
                'correct_up_pct': cup,
                'tier': pr.get('tier', 4),
                'best_month': pr.get('best_month'),
            })

        group_accuracy = sum(p['correct_up_pct'] for p in products) / len(products) if products else 0.0

        for p in products:
            p['resonance'] = p['correct_up_pct'] / group_accuracy if group_accuracy > 0 else 1.0

            if self.enable_resonance_weighting:
                p['score_b'] = LAYER2_WEIGHT_SCORE1 * p['best_score'] + (1.0 - LAYER2_WEIGHT_SCORE1) * (p['resonance'] - 1.0)
            else:
                p['score_b'] = p['best_score']

            p['vetoed'] = False
            if self.enable_resonance_veto and p['resonance'] < self.resonance_veto_threshold:
                p['vetoed'] = True

        products.sort(key=lambda x: -x['score_b'])

        return {
            'signal_source': 'B',
            'cluster_id': cluster_id,
            'group_accuracy': group_accuracy,
            'sorted_products': products,
            'timestamp': now,
        }


# 向后兼容别名
InterProductClusterSorter = ClusterView


# ============================================================================
# v2.5 GlobalView: 全局排名视图层（原信号源C GlobalSorter）
# 非独立信号源，仅做 AlphaEngine 输出的预聚合
# ============================================================================

class GlobalView:
    """v2.5: 全局排名视图层（原信号源C GlobalSorter）

    非独立信号源，仅做 AlphaEngine 输出的预聚合。
    核心流程（方案文档5.2）：
    1. 收集所有品种的 AlphaEngine 输出
    2. 全局排序（按 product_score / final_score 降序）
    3. v2.5 新增：截面标准化 product_score_cs（z-score 跨品种）
    4. 可选：历史分位数参考
    """

    def __init__(self, enable_percentile: bool = True):
        self.enable_percentile = enable_percentile

    def rank(
        self,
        all_product_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """方案文档5.2核心流程 + v2.5 截面标准化"""
        now = time.time()

        if not all_product_results:
            return {
                'signal_source': 'C',
                'total_products': 0,
                'sorted_products': [],
                'timestamp': now,
            }

        products = []
        scores = []
        for pr in all_product_results:
            # v2.5: 优先使用 final_score（共振启用时），其次 product_score_ts，向后兼容 best_score
            score = pr.get('final_score', pr.get('product_score_ts', pr.get('product_score', pr.get('best_score', 0.0))))
            scores.append(score)
            products.append({
                'product_id': pr.get('product_id', ''),
                'product_score': pr.get('product_score', score),
                'product_score_ts': pr.get('product_score_ts', score),
                'final_score': pr.get('final_score', score),
                'best_score': score,  # 向后兼容
                'correct_up_pct': pr.get('correct_up_pct', 0.0),
                'tier': pr.get('tier', 4),
                'best_month': pr.get('best_month'),
            })

        # v2.5: 截面标准化 product_score_cs（z-score 跨品种）
        n = len(scores)
        if n > 1:
            mean_score = sum(scores) / n
            var_score = sum((s - mean_score) ** 2 for s in scores) / n
            std_score = var_score ** 0.5 if var_score > 0 else 1.0
        else:
            mean_score = scores[0] if scores else 0.0
            std_score = 1.0

        for p in products:
            raw_score = p['best_score']
            if std_score > 0:
                p['product_score_cs'] = round((raw_score - mean_score) / std_score, 6)
            else:
                p['product_score_cs'] = 0.0

        # 全局排序（按 best_score 降序，即 final_score 或 product_score_ts）
        products.sort(key=lambda x: -x['best_score'])

        total = len(products)
        for i, p in enumerate(products):
            p['global_rank'] = i + 1
            if self.enable_percentile and total > 0:
                p['global_percentile'] = (total - i) / total
            else:
                p['global_percentile'] = None

        return {
            'signal_source': 'C',
            'total_products': total,
            'sorted_products': products,
            'timestamp': now,
        }


# 向后兼容别名
GlobalSorter = GlobalView
