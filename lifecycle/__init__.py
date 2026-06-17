"""lifecycle/子系统(P1-R2)"""
from __future__ import annotations

__all__ = [
    "save_state",
    "LifecycleManager",
    "VALID_STATE_TRANSITIONS",
    "state",
    "get_state",
    "StrategyState",
    "LifecycleStateMachine",
]


def __getattr__(name: str):
    if name == "LifecycleManager":
        from .lifecycle_manager import LifecycleManager
        return LifecycleManager
    if name in ("StrategyState", "VALID_STATE_TRANSITIONS"):
        from .lifecycle_state_machine import StrategyState, VALID_STATE_TRANSITIONS
        return locals()[name]
    if name == "LifecycleStateMachine":
        from .lifecycle_state_machine import LifecycleStateMachine
        return LifecycleStateMachine
    if name == "save_state":
        from .lifecycle_callbacks import LifecycleCallbacks
        return LifecycleCallbacks.save_state
    if name == "state":
        return None
    if name == "get_state":
        return None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
