"""order/子系统(P1-R1)"""
from __future__ import annotations

__all__ = [
    "BaseService",
    "OrderService",
    "get_order_service",
    "reset_order_service",
    "MarketMakerDefenseEngine",
    "MicrostructureConfig",
    "NetworkRetryManager",
]


def __getattr__(name: str):
    if name == "BaseService":
        from .order_base import BaseService
        return BaseService
    if name == "OrderService":
        from .order_service import OrderService
        return OrderService
    if name == "get_order_service":
        from .order_base import get_order_service
        return get_order_service
    if name == "reset_order_service":
        from .order_base import reset_order_service
        return reset_order_service
    if name == "MarketMakerDefenseEngine":
        from .order_flow_bridge import MarketMakerDefenseEngine
        return MarketMakerDefenseEngine
    if name == "MicrostructureConfig":
        from .order_flow_analyzer import MicrostructureConfig
        return MicrostructureConfig
    if name == "NetworkRetryManager":
        from .order_persistence import NetworkRetryManager
        return NetworkRetryManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
