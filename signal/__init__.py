"""signal/子系统(P2-S1)

五唯一性修复：包级 __init__.py 不需要独立 MODULE_ID，已删除原 M1-195（与 adv_validation_misc.py 重复）。
"""
from __future__ import annotations

__all__ = [
    "SignalService",
    "get_signal_service",
    "ModeEngine",
    "SignalState",
    "transition_signal_state",
    "SignalHistoryService",
    "get_state",
]


def __getattr__(name: str):
    if name == "SignalService":
        from .signal_service import SignalService
        return SignalService
    if name == "get_signal_service":
        from .signal_service import get_signal_service
        return get_signal_service
    if name == "SignalState":
        from .signal_service import SignalState
        return SignalState
    if name == "SignalHistoryService":
        from .signal_history_service import SignalHistoryService
        return SignalHistoryService
    if name == "ModeEngine":
        from ali2026v3_trading.governance.mode_engine import ModeEngine
        return ModeEngine
    if name == "transition_signal_state":
        from .signal_filter_chain import SignalFilterChain
        return SignalFilterChain.transition_signal_state
    if name == "get_state":
        from .signal_timing_filter import KalmanSignalFilter
        return KalmanSignalFilter.get_state
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
