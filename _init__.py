"""signal/子系统(P2-S1)

五唯一性修复：包级 __init__.py 不需要独立 MODULE_ID，已删除原 M1-195（与 adv_validation_misc.py 重复）。

模块合并记录(2026-07-11):
    cooldown_manager.py + signal_timing_filter.py + signal_filter_chain.py + signal_history_service.py
    → signal_components.py（4合1）
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
    "CooldownManager",
    "SignalFilterChain",
    "KalmanFilter1D",
    "EMASignalFilter",
    "SignalTimingFilter",
    "AdaptiveSignalThreshold",
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
        from .signal_components import SignalHistoryService
        return SignalHistoryService
    if name == "ModeEngine":
        from ali2026v3_trading.governance.mode_engine import ModeEngine
        return ModeEngine
    if name == "transition_signal_state":
        from .signal_components import SignalFilterChain
        return SignalFilterChain.transition_signal_state
    if name == "get_state":
        from .signal_components import KalmanFilter1D
        return KalmanFilter1D.get_state
    if name == "CooldownManager":
        from .signal_components import CooldownManager
        return CooldownManager
    if name == "SignalFilterChain":
        from .signal_components import SignalFilterChain
        return SignalFilterChain
    if name == "KalmanFilter1D":
        from .signal_components import KalmanFilter1D
        return KalmanFilter1D
    if name == "EMASignalFilter":
        from .signal_components import EMASignalFilter
        return EMASignalFilter
    if name == "SignalTimingFilter":
        from .signal_components import SignalTimingFilter
        return SignalTimingFilter
    if name == "AdaptiveSignalThreshold":
        from .signal_components import AdaptiveSignalThreshold
        return AdaptiveSignalThreshold
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
