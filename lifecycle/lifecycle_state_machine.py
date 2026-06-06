"""
生命周期状态机 — 从strategy_lifecycle_mixin.py拆分
职责: 五态状态转移、合法转换校验
"""
from __future__ import annotations

import logging
from typing import Optional, Set, Dict, Any

_INITIALIZED = "INITIALIZED"
_RUNNING = "RUNNING"
_PAUSED = "PAUSED"
_STOPPED = "STOPPED"
_DESTROYED = "DESTROYED"

_ALL_STATES = frozenset({_INITIALIZED, _RUNNING, _PAUSED, _STOPPED, _DESTROYED})

_LEGAL_TRANSITIONS: Dict[str, Set[str]] = {
    _INITIALIZED: {_RUNNING, _DESTROYED},
    _RUNNING: {_PAUSED, _STOPPED, _DESTROYED},
    _PAUSED: {_RUNNING, _STOPPED, _DESTROYED},
    _STOPPED: {_INITIALIZED, _DESTROYED},
    _DESTROYED: set(),
}


class LifecycleStateMachine:
    def __init__(self, initial_state: str = _INITIALIZED):
        if initial_state not in _ALL_STATES:
            raise ValueError(f"非法初始状态: {initial_state}")
        self._state = initial_state

    @property
    def state(self) -> str:
        return self._state

    def can_transition_to(self, target: str) -> bool:
        return target in _LEGAL_TRANSITIONS.get(self._state, set())

    def transition_to(self, target: str) -> bool:
        if not self.can_transition_to(target):
            logging.warning("[LifecycleStateMachine] 非法状态转换: %s → %s", self._state, target)
            return False
        self._state = target
        return True

    @property
    def is_running(self) -> bool:
        return self._state == _RUNNING

    @property
    def is_paused(self) -> bool:
        return self._state == _PAUSED

    @property
    def is_destroyed(self) -> bool:
        return self._state == _DESTROYED

    @property
    def is_stopped(self) -> bool:
        return self._state == _STOPPED