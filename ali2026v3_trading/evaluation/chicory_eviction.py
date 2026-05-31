from __future__ import annotations
from typing import Any, Dict, List, Optional
import math


class ChicoryEvictionPolicy:
    W0_BASE = 0.60
    W1_DECAY = 0.25
    W2_VIOLATION = 0.15
    HALF_LIFE_DAYS = 30

    def __init__(self, w0: float = 0.60, w1: float = 0.25, w2: float = 0.15):
        self.w0 = w0
        self.w1 = w1
        self.w2 = w2

    def eviction_score(self, strategy_score: float, age_days: int, violation_count: int = 0) -> float:
        # R21-ALIGN: 与生产系统quant_core.py的R21-MATH-P2-04修复对齐
        # age_days正常为正整数，exp参数为负不会溢出；但若age_days异常为负则需保护
        _exp_arg = max(-500.0, min(500.0, -age_days / self.HALF_LIFE_DAYS))
        base_score = strategy_score
        decay_score = math.exp(_exp_arg)
        violation_score = max(0.0, 1.0 - violation_count * 0.1)
        return self.w0 * base_score + self.w1 * decay_score + self.w2 * violation_score

    def should_evict(self, strategy_score: float, age_days: int, violation_count: int = 0, threshold: float = 0.3) -> bool:
        return self.eviction_score(strategy_score, age_days, violation_count) < threshold

    def evaluate(self, strategy_id: str, overall: float, dimensions: Dict[str, Any]) -> Dict[str, Any]:
        age_days = dimensions.get("age_days", 30) if isinstance(dimensions, dict) else 30
        violation_count = dimensions.get("violation_count", 0) if isinstance(dimensions, dict) else 0
        score = self.eviction_score(overall, age_days, violation_count)
        should_evict = score < 0.3
        return {"should_evict": should_evict, "eviction_score": score, "strategy_id": strategy_id, "reason": f"eviction_score={score:.4f}<0.3" if should_evict else ""}
