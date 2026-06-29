# [M1-75] 共享提供者
"""Shared Provider Interfaces — Phase 1 architectural refactoring.

Defines explicit interfaces for the 6 cross-shared self._ attributes
currently implicitly shared between Mixin classes via self:

  self._state, self._is_running, self._is_paused,
  self._lock, self._stats, self._safety_meta_layer

These protocols allow Mixin→composition migration (Phase 2) by providing
explicit dependency injection interfaces. During Phase 1, existing code
continues to work via self attributes; these interfaces are the target
for Phase 2 composition refactoring.
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from ali2026v3_trading.infra.shared_utils import CHINA_TZ  # 五唯一性修复：统一 CHINA_TZ


@runtime_checkable
class StateProvider(Protocol):
    """Interface for strategy state access."""

    @property
    def state(self) -> Any:
        ...

    @property
    def is_running(self) -> bool:
        ...

    @property
    def is_paused(self) -> bool:
        ...


@runtime_checkable
class LockProvider(Protocol):
    """Interface for thread lock access."""

    @property
    def lock(self) -> threading.RLock:
        ...


@runtime_checkable
class StatsProvider(Protocol):
    """Interface for statistics access."""

    @property
    def stats(self) -> Dict[str, Any]:
        ...

    def record_tick(self) -> None:
        ...

    def record_trade(self) -> None:
        ...

    def record_signal(self) -> None:
        ...

    def record_error(self, error_message: str) -> None:
        ...


@runtime_checkable
class SafetyProvider(Protocol):
    """Interface for safety meta layer access."""

    @property
    def safety_meta_layer(self) -> Any:
        ...

    def check_safety(self, signal: Dict[str, Any]) -> Any:
        ...


class DefaultStateProvider:
    """Default implementation of StateProvider for standalone use."""

    def __init__(self, initial_state: Any = None):
        self._state = initial_state
        self._is_running = False
        self._is_paused = False

    @property
    def state(self) -> Any:
        return self._state

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_paused(self) -> bool:
        return self._is_paused


class DefaultLockProvider:
    """Default implementation of LockProvider for standalone use."""

    def __init__(self):
        self._lock = threading.RLock()

    @property
    def lock(self) -> threading.RLock:
        return self._lock


class DefaultStatsProvider:
    """Default implementation of StatsProvider for standalone use."""

    def __init__(self):
        self._stats: Dict[str, Any] = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': '',
        }
        self._lock = threading.RLock()

    @property
    def stats(self) -> Dict[str, Any]:
        return self._stats

    def record_tick(self) -> None:
        with self._lock:
            self._stats['total_ticks'] += 1

    def record_trade(self) -> None:
        with self._lock:
            self._stats['total_trades'] += 1

    def record_signal(self) -> None:
        with self._lock:
            self._stats['total_signals'] += 1

    def record_error(self, error_message: str) -> None:
        with self._lock:
            self._stats['errors_count'] += 1
            self._stats['last_error_time'] = datetime.now(CHINA_TZ)
            self._stats['last_error_message'] = error_message


class DefaultSafetyProvider:
    """Default implementation of SafetyProvider (no-op)."""

    @property
    def safety_meta_layer(self) -> Any:
        return None

    def check_safety(self, signal: Dict[str, Any]) -> Any:
        return None
