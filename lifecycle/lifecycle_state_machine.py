# MODULE_ID: M1-128
"""lifecycle_state_machine/统一状态模块 — 合并自lifecycle_state + lifecycle_state_machine"""
from __future__ import annotations

import logging
from enum import Enum
from typing import Dict, List, Optional, Set, Any

from ali2026v3_trading.infra.state_machine import BaseStateMachine


# ──────────────────────────────────────────────────────────────
# Section 1: 状态定义 (原 lifecycle_state.py)
# ──────────────────────────────────────────────────────────────

class StrategyState(Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    PARALLEL_RUNNING = "parallel_running"
    DEGRADED = "degraded"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"
    DEGRADED_STOP = "degraded_stop"
    DESTROYED = "destroyed"


def _state_key(state) -> str:
    if state is None:
        return ""
    value = getattr(state, 'value', None)
    if value is not None:
        return str(value)
    name = getattr(state, 'name', None)
    if name is not None:
        return str(name)
    return str(state).rsplit('.', 1)[-1]


def _state_is(state, *targets) -> bool:
    state_key = _state_key(state)
    return any(state_key == _state_key(target) for target in targets)


VALID_STATE_TRANSITIONS: Dict[StrategyState, List[StrategyState]] = {
    StrategyState.INITIALIZING: [StrategyState.RUNNING, StrategyState.ERROR, StrategyState.DEGRADED],
    StrategyState.RUNNING: [StrategyState.PAUSED, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED, StrategyState.PARALLEL_RUNNING],
    StrategyState.PARALLEL_RUNNING: [StrategyState.RUNNING, StrategyState.PAUSED, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED],
    StrategyState.PAUSED: [StrategyState.RUNNING, StrategyState.STOPPED, StrategyState.ERROR],
    StrategyState.DEGRADED: [StrategyState.RUNNING, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED_STOP],
    StrategyState.ERROR: [StrategyState.INITIALIZING, StrategyState.STOPPED],
    StrategyState.DEGRADED_STOP: [StrategyState.STOPPED, StrategyState.INITIALIZING],
    StrategyState.STOPPED: [StrategyState.INITIALIZING, StrategyState.DESTROYED],
    StrategyState.DESTROYED: [],
}


# ──────────────────────────────────────────────────────────────
# Section 2: 状态机 (原 lifecycle_state_machine.py)
# ──────────────────────────────────────────────────────────────

_INITIALIZED = StrategyState.INITIALIZING.value
_RUNNING = StrategyState.RUNNING.value
_PAUSED = StrategyState.PAUSED.value
_STOPPED = StrategyState.STOPPED.value
_DESTROYED = StrategyState.DESTROYED.value

_ALL_STATES = frozenset({_INITIALIZED, _RUNNING, _PAUSED, _STOPPED, _DESTROYED})


class LifecycleStateMachine(BaseStateMachine):
    """P1-56修复: 继承 BaseStateMachine 抽象基类"""

    _LEGAL_TRANSITIONS: Dict[str, Set[str]] = {
        _INITIALIZED: {_RUNNING, _DESTROYED},
        _RUNNING: {_PAUSED, _STOPPED, _DESTROYED},
        _PAUSED: {_RUNNING, _STOPPED, _DESTROYED},
        _STOPPED: {_INITIALIZED, _DESTROYED},
        _DESTROYED: set(),
    }

    def __init__(self, initial_state: str = _INITIALIZED):
        if initial_state not in _ALL_STATES:
            raise ValueError(f"非法初始状态: {initial_state}")
        self._state = initial_state

    @property
    def state(self) -> str:
        return self._state

    def can_transition(self, target: str) -> bool:
        return target in self._LEGAL_TRANSITIONS.get(self._state, set())

    # 向后兼容别名
    def can_transition_to(self, target: str) -> bool:
        return self.can_transition(target)

    def transition_to(self, target: str) -> None:
        if not self.can_transition(target):
            logging.warning("[LifecycleStateMachine] 非法状态转换: %s → %s", self._state, target)
            return
        self._state = target

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
