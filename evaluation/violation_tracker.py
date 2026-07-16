# MODULE_ID: M1-066
from __future__ import annotations
from typing import Any, Dict, List, Optional


def _get_base_class():
    from governance.governance_engine import StrategyViolationTracker as _SVT
    return _SVT


class StrategyViolationTracker:
    def track(self, strategy_id: str, verdict: str, blockers: List[str]) -> List[str]:
        violations = []
        if verdict in ("FAIL", "REJECT") or blockers:
            violations.append(f"strategy={strategy_id} verdict={verdict} blockers={len(blockers)}")
        if blockers:
            for b in blockers:
                violations.append(f"strategy={strategy_id} blocker={b}")
        return violations

    def __getattr__(self, name):
        _SVT = _get_base_class()
        return getattr(_SVT(), name)
