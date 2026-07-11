# MODULE_ID: M1-065
from __future__ import annotations
from typing import Any, Dict, List, Optional
import math


class StateEDensityDecayTracker:
    def __init__(self, half_life_days: float = 30.0, min_density_threshold: float = 0.05):
        self._half_life_days = half_life_days
        self._min_density_threshold = min_density_threshold
        self._density_history: List[Dict[str, Any]] = []

    def track(self, strategy_id_or_count, diagnosis_report_or_total=None, day: int = 0) -> Dict[str, Any]:
        if isinstance(strategy_id_or_count, str):
            strategy_id = strategy_id_or_count
            if isinstance(diagnosis_report_or_total, dict):
                state_e_count = diagnosis_report_or_total.get("state_e_count", 0)
                total_count = diagnosis_report_or_total.get("total_state_count", 1)
            else:
                state_e_count = 0
                total_count = max(1, diagnosis_report_or_total or 1)
            density = state_e_count / total_count if total_count > 0 else 0.0
            record = {"day": day, "strategy_id": strategy_id, "state_e_count": state_e_count, "total_state_count": total_count, "density": density}
        else:
            state_e_count = strategy_id_or_count
            total_count = diagnosis_report_or_total if diagnosis_report_or_total is not None else 1
            density = state_e_count / total_count if total_count > 0 else 0.0
            record = {"day": day, "state_e_count": state_e_count, "total_state_count": total_count, "density": density}
        self._density_history.append(record)
        if len(self._density_history) >= 2:
            recent = self._density_history[-1]["density"]
            older = self._density_history[0]["density"]
            if older > 0:
                decay_rate = (older - recent) / older
                record["decay_detected"] = decay_rate > 0.5
                record["decay_rate"] = decay_rate
            else:
                record["decay_detected"] = False
                record["decay_rate"] = 0.0
        else:
            record["decay_detected"] = False
            record["decay_rate"] = 0.0
        return record

    def detect_decay(self) -> Dict[str, Any]:
        if len(self._density_history) < 2:
            return {"decaying": False, "decay_rate": 0.0}
        recent = self._density_history[-1]["density"]
        older = self._density_history[0]["density"]
        if older <= 0:
            return {"decaying": False, "decay_rate": 0.0}
        decay_rate = (older - recent) / older
        return {"decaying": decay_rate > 0.5, "decay_rate": decay_rate, "recent_density": recent, "older_density": older}
