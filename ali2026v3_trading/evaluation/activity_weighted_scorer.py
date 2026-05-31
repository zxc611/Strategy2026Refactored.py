from __future__ import annotations
from typing import Any, Dict, List, Optional


class ActivityWeightedScorer:
    def __init__(self, recency_weight: float = 0.7, volume_weight: float = 0.3):
        self._recency_weight = recency_weight
        self._volume_weight = volume_weight

    def calculate(self, strategy_id: str, overall: float, dimensions: Dict[str, Any]) -> float:
        activity_count = dimensions.get("activity_count", 1) if isinstance(dimensions, dict) else 1
        recent_days = dimensions.get("recent_active_days", 30) if isinstance(dimensions, dict) else 30
        recency_factor = min(1.0, recent_days / 30.0)
        volume_factor = min(1.0, activity_count / max(100, activity_count))
        weighted_score = overall * (self._recency_weight * recency_factor + self._volume_weight * volume_factor)
        return weighted_score
