# MODULE_ID: M1-064
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional


def _get_base_class():
    from governance.governance_engine import ParameterDriftDetector as _PDD
    return _PDD


class ParameterDriftDetector:
    def detect(self, strategy_id: str, parameter_stability_result: Optional[Dict] = None) -> Dict[str, Any]:
        param_history = []
        if parameter_stability_result and isinstance(parameter_stability_result, dict):
            param_history = parameter_stability_result.get("param_history", [])
            if not param_history and "snapshots" in parameter_stability_result:
                param_history = parameter_stability_result["snapshots"]
        if not param_history:
            try:
                from governance.governance_engine import get_governance_engine
                _engine = get_governance_engine()
                param_history = _engine._param_snapshot_history
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
        if param_history:
            return self.detect_drift(param_history)
        return {"drift_detected": False, "drifted_params": [], "max_drift": 0.0, "strategy_id": strategy_id}

    def detect_drift(self, param_history):
        _PDD = _get_base_class()
        return _PDD().detect_drift(param_history)
