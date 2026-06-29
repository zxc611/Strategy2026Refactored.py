# MODULE_ID: M1-056-SB
"""
signal_source_b.py — 信号源B：联动品种簇排序（InterProductClusterSorter）

依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md 第四章

职责：在品种簇内部比较各品种的信号源A输出，识别共振，输出簇内排序
不做：强制过滤品种、仓位分配
"""
from __future__ import annotations

import time
import logging
from typing import Any, Dict, List, Optional

from ali2026v3_trading.config.final_three_layer_config import (
    RESONANCE_VETO_THRESHOLD,
    RESONANCE_MODE,
    LAYER2_WEIGHT_SCORE1,
)

logger = logging.getLogger(__name__)


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