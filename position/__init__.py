# MODULE_ID: M1-200
"""position/子系统(P2-S1)"""
from __future__ import annotations

__all__ = [
    "PositionService",
    "get_position_service",
    "reset_position_service",
    "PositionPnlService",
    "RiskService",
    "PositionLimitConfig",
    "TWO_STAGE_STOP_CONFIG",
]


def __getattr__(name: str):
    if name in ("PositionService", "get_position_service", "reset_position_service", "PositionLimitConfig"):
        from .position_service import PositionService, get_position_service, reset_position_service, PositionLimitConfig
        return locals()[name]
    if name == "PositionPnlService":
        from .position_pnl_service import PositionPnlService
        return PositionPnlService
    if name == "TWO_STAGE_STOP_CONFIG":
        from .position_pnl_service import PositionPnlService
        return PositionPnlService.TWO_STAGE_STOP_CONFIG
    if name == "RiskService":
        return None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
