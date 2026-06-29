# MODULE_ID: M1-056-SC
"""
signal_source_c.py — 信号源C：全域品种排序（GlobalSorter）

依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md 第五章

职责：对所有品种的信号源A输出做全局比较，输出跨品种排名
不做：仓位分配、风险预算、熔断判断
"""
from __future__ import annotations

import time
import logging
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


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