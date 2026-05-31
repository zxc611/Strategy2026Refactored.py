from __future__ import annotations
from typing import Any, Dict, List, Optional
from ali2026v3_trading.governance_engine import StrategyViolationTracker as _SVT


class StrategyViolationTracker(_SVT):
    def track(self, strategy_id: str, verdict: str, blockers: List[str]) -> List[str]:
        violations = []
        if verdict in ("FAIL", "REJECT") or blockers:
            violations.append(f"strategy={strategy_id} verdict={verdict} blockers={len(blockers)}")
        if blockers:
            for b in blockers:
                violations.append(f"strategy={strategy_id} blocker={b}")
        return violations
