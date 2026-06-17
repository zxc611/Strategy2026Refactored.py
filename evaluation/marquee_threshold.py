# MODULE_ID: M1-063
from __future__ import annotations
from typing import Any, Dict, List, Optional


class MarqueeThreshold:
    PHASE1_THRESHOLD = 0.80
    PHASE2_THRESHOLD = 0.70
    PHASE3_THRESHOLD = 0.60
    PHASE1_MIN_SAMPLES = 60
    PHASE2_MIN_SAMPLES = 120

    def __init__(self, phase1_threshold: float = 0.80, phase2_threshold: float = 0.70, phase3_threshold: float = 0.60):
        self.phase1_threshold = phase1_threshold
        self.phase2_threshold = phase2_threshold
        self.phase3_threshold = phase3_threshold

    def get_phase(self, n_samples: int) -> str:
        if n_samples < self.PHASE1_MIN_SAMPLES:
            return "strict"
        elif n_samples < self.PHASE2_MIN_SAMPLES:
            return "moderate"
        return "relaxed"

    def get_threshold(self, n_samples: int) -> float:
        phase = self.get_phase(n_samples)
        if phase == "strict":
            return self.phase1_threshold
        elif phase == "moderate":
            return self.phase2_threshold
        return self.phase3_threshold

    def is_marquee(self, score: float, n_samples: int) -> bool:
        return score >= self.get_threshold(n_samples)

    def check(self, strategy_id: str, overall: float, dimensions: Dict[str, Any]) -> Dict[str, Any]:
        n_samples = dimensions.get("n_samples", 30) if isinstance(dimensions, dict) else 30
        threshold = self.get_threshold(n_samples)
        passed = overall >= threshold
        failed_dims = []
        if isinstance(dimensions, dict):
            for dim_name, dim_score in dimensions.items():
                if isinstance(dim_score, (int, float)) and dim_score < threshold * 0.8:
                    failed_dims.append(dim_name)
        return {"passed": passed, "threshold": threshold, "score": overall, "strategy_id": strategy_id, "failed_dims": failed_dims}
