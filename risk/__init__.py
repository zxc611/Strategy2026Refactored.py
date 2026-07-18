# MODULE_ID: M1-176
"""risk/子系统(P2-S1)"""
from __future__ import annotations

__all__ = [
    "RiskService",
    "get_risk_service",
    "reset_risk_service",
    "RiskDashboardService",
    "get_risk_dashboard_service",
    "RiskCheckService",
    "create_default_risk_check_engine",
]


def __getattr__(name: str):
    if name in ("RiskService", "get_risk_service", "reset_risk_service", "RiskDashboardService", "get_risk_dashboard_service"):
        from .risk_service import RiskService, get_risk_service, reset_risk_service, RiskDashboardService, get_risk_dashboard_service
        return locals()[name]
    if name == "RiskCheckService":
        from .risk_check_service import RiskCheckService
        return RiskCheckService
    if name == "create_default_risk_check_engine":
        from .risk_check_engine import create_default_risk_check_engine
        return create_default_risk_check_engine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
