# MODULE_ID: M1-067
"""governance/子系统(P2-S1)"""
from __future__ import annotations

__all__ = [
    "GovernanceEngine",
    "get_governance_engine",
    "ComplianceEngine",
    "create_default_compliance_engine",
    "check_compliance_via_engine",
    "MultiStateSwitchBacktestScenario",
    "StateEDensityDecayTracker",
]


def __getattr__(name: str):
    if name in ("GovernanceEngine", "get_governance_engine", "MultiStateSwitchBacktestScenario", "StateEDensityDecayTracker"):
        from .governance_engine import GovernanceEngine, get_governance_engine, MultiStateSwitchBacktestScenario, StateEDensityDecayTracker
        return locals()[name]
    if name in ("ComplianceEngine", "create_default_compliance_engine"):
        from .regulatory_compliance import ComplianceEngine, create_default_compliance_engine
        return locals()[name]
    if name == "check_compliance_via_engine":
        from .compliance_checker import ComplianceChecker
        return ComplianceChecker.check_compliance_via_engine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
