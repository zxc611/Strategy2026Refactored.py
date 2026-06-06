"""
lifecycle_transition.py - LifecycleTransition
Phase 2 (CC-P1-04+CC-P1-05): 从strategy_lifecycle_mixin.py提取的状态转换逻辑

职责：
- 线程安全的状态转换 (transition_to)
- 状态转换合法性校验
- 转换事件通知 (通过EventBus)
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Optional

from ali2026v3_trading.lifecycle.lifecycle_state import StrategyState, _state_key, _state_is, VALID_STATE_TRANSITIONS


class LifecycleTransition:
    """生命周期状态转换 — 独立可测试组件"""

    def __init__(self, event_bus=None):
        self._event_bus = event_bus
        self._transition_callbacks: List[Callable] = []

    def transition_to(self, current_state: StrategyState, new_state: StrategyState,
                       state_lock: threading.RLock = None) -> tuple:
        """线程安全的状态转换

        Returns:
            tuple: (success: bool, old_state: StrategyState, new_state: StrategyState)
        """
        if not _state_is(current_state, new_state):
            valid_targets = VALID_STATE_TRANSITIONS.get(current_state, [])
            if not any(_state_is(new_state, t) for t in valid_targets):
                logging.warning(
                    "[LifecycleTransition] Invalid state transition: %s -> %s, allowed: %s",
                    _state_key(current_state), _state_key(new_state),
                    [_state_key(s) for s in valid_targets]
                )
                return (False, current_state, current_state)

        old_state = current_state

        for cb in self._transition_callbacks:
            try:
                cb(old_state, new_state)
            except Exception as e:
                logging.debug("[LifecycleTransition] callback error: %s", e)

        if self._event_bus is not None:
            try:
                self._event_bus.publish('lifecycle.state_changed', {
                    'old_state': _state_key(old_state),
                    'new_state': _state_key(new_state),
                })
            except Exception:
                pass

        return (True, old_state, new_state)

    def add_transition_callback(self, callback: Callable) -> None:
        self._transition_callbacks.append(callback)

    def is_valid_transition(self, from_state: StrategyState, to_state: StrategyState) -> bool:
        if _state_is(from_state, to_state):
            return True
        valid_targets = VALID_STATE_TRANSITIONS.get(from_state, [])
        return any(_state_is(to_state, t) for t in valid_targets)