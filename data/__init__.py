# MODULE_ID: M1-019
"""data/子系统(P2-S1)"""
from __future__ import annotations

__all__ = [
    "DataService",
    "get_data_service",
    "reset_data_service",
    "RiskStateAccess",
    "DefaultDataAccess",
    "params_service",
    "set_params_service",
]


def __getattr__(name: str):
    if name == "DataService":
        from .data_service import DataService
        return DataService
    if name == "get_data_service":
        from .data_service import get_data_service
        return get_data_service
    if name == "reset_data_service":
        from .data_service import reset_data_service
        return reset_data_service
    if name == "RiskStateAccess":
        from .data_access import RiskStateAccess
        return RiskStateAccess
    if name == "DefaultDataAccess":
        from .data_access import DefaultDataAccess
        return DefaultDataAccess
    if name == "params_service":
        from config.params_service import get_params_service
        return get_params_service()
    if name == "set_params_service":
        from config.params_service import set_params_service
        return set_params_service
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
