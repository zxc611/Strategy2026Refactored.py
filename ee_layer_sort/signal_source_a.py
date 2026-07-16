# MODULE_ID: M1-056-SA
"""
signal_source_a.py — 三层期权五态排序信号源（A+B+C合并）

依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md

合并说明 (2026-06-30):
- 信号源A：IntraProductSorter（单品种月份排序）
- 信号源B：InterProductClusterSorter（联动品种簇排序）← 合入自 signal_source_b.py
- 信号源C：GlobalSorter（全域品种排序）← 合入自 signal_source_c.py
"""
from __future__ import annotations

import math
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

from config.tvf_param_loader import (
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
)

logger = logging.getLogger(__name__)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


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


class IntraProductSorter:
    """信号源A：单品种月份排序

    核心流程（方案文档3.3）：
    1. 非对称状态时效衰减
    2. 月份加权
    3. 方向净得分
    4. 加权得分排序
    """

    MAX_MONTHS = 5

    def __init__(
        self,
        pure_mode: bool = False,
        hard_filter_enabled: bool = False,
        sector: str = 'default',
    ):
        self.pure_mode = pure_mode
        self._decay = AsymmetricDecay(sector)
        self._hard_filter = GreeksHardFilter(enabled=hard_filter_enabled)

    @staticmethod
    def resolve_month_weights(n_months: int) -> Tuple[float, ...]:
        if n_months <= 0:
            return ()
        if n_months > IntraProductSorter.MAX_MONTHS:
            return MONTH_WEIGHTS_5
        raw = MONTH_WEIGHTS_5[:n_months]
        s = sum(raw)
        return tuple(w / s for w in raw) if s > 0 else tuple(1.0 / n_months for _ in range(n_months))

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
        # FIX-R5: 统一与width_cache_query_mixin.determine_tier的阈值一致
        # 原差异：Tier 1 coverage用TIER2_COVERAGE_THRESHOLD(0.40)而非0.8；缺correct_up_pct>0守卫
        if correct_up_pct > 0:
            if coverage >= 0.8 and wilson >= TIER1_WILSON_THRESHOLD:
                return 1
            if coverage >= TIER2_COVERAGE_THRESHOLD and correct_up_pct >= TIER2_CORRECT_UP_THRESHOLD:
                return 2
            if correct_up_pct >= TIER3_CORRECT_UP_THRESHOLD:
                return 3
            return 4
        else:
            # FIX-R4: 无分类数据时降级为Tier 3（与query_mixin一致）
            if coverage == 0.0:
                return 3
            return 4

    def rank(self, product_id: str, month_data_list: List[Dict]) -> Dict[str, Any]:
        """方案文档3.3核心流程"""
        now = time.time()
        n_months = min(len(month_data_list), self.MAX_MONTHS)
        if n_months == 0:
            return {
                'signal_source': 'A',
                'product_id': product_id,
                'best_month': None,
                'best_score': 0.0,
                'correct_up_pct': 0.0,
                'tier': 4,
                'month_count': 0,
                'candidates': [],
                'timestamp': now,
            }

        weights = self.resolve_month_weights(n_months)
        candidates = []
        total_cr_eff = 0.0
        total_rise_eff = 0.0
        weighted_correct_up = 0.0
        weighted_total = 0.0
        active_months = 0

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

            net_score = (cr_eff + cf_eff - wr_eff - wf_eff) / total_eff if total_eff > 0 else 0.0
            weighted_score = net_score * w

            correct_up = (cr_eff + cf_eff) / (cr_eff + cf_eff + wr_eff + wf_eff) if (cr_eff + cf_eff + wr_eff + wf_eff) > 0 else 0.0
            weighted_correct_up += w * (cr_eff + cf_eff)
            weighted_total += w * (cr_eff + cf_eff + wr_eff + wf_eff)

            total_cr_eff += cr_eff
            total_rise_eff += cr_eff + wr_eff

            if (cr_eff + cf_eff + wr_eff + wf_eff) > 0:
                active_months += 1

            month = mdata.get('month', f'm{i+1}')
            candidates.append({
                'month': month,
                'score': weighted_score,
                'correct_up_pct': correct_up,
                'net_score': net_score,
                'cr_eff': cr_eff,
                'cf_eff': cf_eff,
                'wr_eff': wr_eff,
                'wf_eff': wf_eff,
            })

        candidates.sort(key=lambda x: -x['score'])

        correct_up_pct = weighted_correct_up / weighted_total if weighted_total > 0 else 0.0
        wilson = self.wilson_lower_bound(total_cr_eff, total_rise_eff)
        coverage = active_months / self.MAX_MONTHS if self.MAX_MONTHS > 0 else 0.0
        tier = self.determine_tier(coverage, wilson, correct_up_pct)

        best = candidates[0] if candidates else {}

        return {
            'signal_source': 'A',
            'product_id': product_id,
            'best_month': best.get('month'),
            'best_score': best.get('score', 0.0),
            'correct_up_pct': correct_up_pct,
            'tier': tier,
            'month_count': n_months,
            'candidates': candidates,
            'wilson': wilson,
            'coverage': coverage,
            'timestamp': now,
        }


# ============================================================================
# 信号源B：联动品种簇排序（合入自 signal_source_b.py）
# ============================================================================

from config.tvf_param_loader import (
    RESONANCE_VETO_THRESHOLD,
    RESONANCE_MODE,
    LAYER2_WEIGHT_SCORE1,
)


class InterProductClusterSorter:
    """信号源B：联动品种簇排序

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
            score = pr.get('best_score', 0.0)
            products.append({
                'product_id': pr.get('product_id', ''),
                'best_score': score,
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


# ============================================================================
# 信号源C：全域品种排序（合入自 signal_source_c.py）
# ============================================================================


class GlobalSorter:
    """信号源C：全域品种排序

    核心流程（方案文档5.2）：
    1. 收集所有品种的信号源A输出
    2. 全局排序（直接按 best_score 降序）
    3. 可选：历史分位数参考
    """

    def __init__(self, enable_percentile: bool = True):
        self.enable_percentile = enable_percentile

    def rank(
        self,
        all_product_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """方案文档5.2核心流程"""
        now = time.time()

        if not all_product_results:
            return {
                'signal_source': 'C',
                'total_products': 0,
                'sorted_products': [],
                'timestamp': now,
            }

        products = []
        for pr in all_product_results:
            products.append({
                'product_id': pr.get('product_id', ''),
                'best_score': pr.get('best_score', 0.0),
                'correct_up_pct': pr.get('correct_up_pct', 0.0),
                'tier': pr.get('tier', 4),
                'best_month': pr.get('best_month'),
            })

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