"""
lifecycle_state.py - LifecycleState
Phase 2 (CC-P1-04+CC-P1-05): 从strategy_lifecycle_mixin.py提取的状态定义

职责：
- StrategyState枚举定义
- 状态比较工具函数 (_state_key, _state_is)
- 合法状态转换表 (VALID_STATE_TRANSITIONS)
"""
from __future__ import annotations

from enum import Enum
from typing import Dict, List


class StrategyState(Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    PARALLEL_RUNNING = "parallel_running"
    DEGRADED = "degraded"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"
    DEGRADED_STOP = "degraded_stop"


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
    StrategyState.STOPPED: [StrategyState.INITIALIZING],
}