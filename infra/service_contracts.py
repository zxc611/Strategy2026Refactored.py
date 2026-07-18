"""
infra/service_contracts.py — 服务协议/提供者/状态/系统契约 合并模块

合并自:
  - service_contracts.py (ServiceProtocol协议定义)
  - shared_providers.py (共享提供者接口)
  - state_machine.py (状态机抽象基类)
  - state_store.py (状态存储 → 已去重至 registry_service.py)
  - contracts_service.py (系统契约 RuntimeSystem/UnifiedErrorCode/HealthStatusLevel等)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

from infra.shared_utils import CHINA_TZ


# ============================================================
# Section 1: 服务协议 (原 service_contracts.py)
# ============================================================

@runtime_checkable
class ServiceProtocol(Protocol):
    def health_check(self) -> Dict[str, Any]:
        ...

    def get_service_name(self) -> str:
        ...


# ============================================================
# Section 2: 共享提供者接口 (原 shared_providers.py)
# ============================================================

import threading as _threading


@runtime_checkable
class StateProvider(Protocol):
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
    @property
    def lock(self) -> _threading.RLock:
        ...


@runtime_checkable
class StatsProvider(Protocol):
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
    @property
    def safety_meta_layer(self) -> Any:
        ...

    def check_safety(self, signal: Dict[str, Any]) -> Any:
        ...


class DefaultStateProvider:
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
    def __init__(self):
        self._lock = _threading.RLock()

    @property
    def lock(self) -> _threading.RLock:
        return self._lock


class DefaultStatsProvider:
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
        self._lock = _threading.RLock()

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
    @property
    def safety_meta_layer(self) -> Any:
        return None

    def check_safety(self, signal: Dict[str, Any]) -> Any:
        return None


# ============================================================
# Section 3: 状态机抽象基类 (原 state_machine.py)
# ============================================================

class BaseStateMachine(ABC):
    _LEGAL_TRANSITIONS: Dict[str, set] = {}

    @property
    @abstractmethod
    def state(self) -> str:
        ...

    @abstractmethod
    def can_transition(self, target_state: str) -> bool:
        ...

    @abstractmethod
    def transition_to(self, target_state: str) -> None:
        ...


# ============================================================
# Section 4: 状态存储 (re-export from registry_service)
# ============================================================

from infra.registry_service import (  # 去重：StateStore规范版本位于 registry_service.py
    StateSnapshot, StateStore, get_state_store, reset_state_store,
)
from infra.metrics_registry import PhaseFeatureFlag  # 去重：PhaseFeatureFlag规范版本位于 metrics_registry.py


# ============================================================
# Section 5: 系统契约 (原 contracts_service.py)
# ============================================================

class RuntimeSystem(str, Enum):
    PRODUCTION = 'production'
    BACKTEST = 'backtest'
    JUDGMENT = 'judgment'


class UnifiedErrorCode(str, Enum):
    MODE_FILTER_EXCEPTION = 'SIG-ERR-001'
    DECISION_FILTER_EXCEPTION = 'SIG-ERR-002'
    HFT_FILTER_EXCEPTION = 'SIG-ERR-003'
    EVENT_PUBLISH_EXCEPTION = 'SIG-ERR-004'
    COMPENSATION_PUBLISH_EXCEPTION = 'SIG-ERR-005'


class UnifiedDataSourceAdapter(Protocol):
    def get_latest_tick(self, instrument_id: str) -> Dict[str, Any]:
        ...


class HealthStatusLevel(str, Enum):
    OK = 'OK'
    WARNING = 'WARNING'
    CRITICAL = 'CRITICAL'


def normalize_health_status(raw: Dict[str, Any], component_name: str = '') -> Dict[str, Any]:
    _status_raw = raw.get('status') or raw.get('overall_status') or raw.get('health_status') or raw.get('health', 'UNKNOWN')
    if isinstance(_status_raw, str):
        _s = _status_raw.upper()
        if _s in ('OK', 'HEALTHY', 'NORMAL'):
            _status = HealthStatusLevel.OK
        elif _s in ('WARNING', 'DEGRADED'):
            _status = HealthStatusLevel.WARNING
        elif _s in ('CRITICAL', 'UNHEALTHY', 'ERROR'):
            _status = HealthStatusLevel.CRITICAL
        else:
            _status = HealthStatusLevel.WARNING
    else:
        _status = HealthStatusLevel.WARNING
    return {
        'component': raw.get('component', component_name),
        'status': _status.value,
        'timestamp': raw.get('timestamp', time.time()),
        'details': {k: v for k, v in raw.items() if k not in ('component', 'status', 'overall_status', 'health_status', 'health', 'timestamp')},
    }


_UNIFIED_GREEKS_CALCULATOR: Any = None


@dataclass(slots=True)
class SignalDiagnosisRecord:
    signal_id: str
    sequence_no: int
    stage: str
    risk_blocked: bool
    mode_reason: str
    runtime_system: str
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            'signal_id': self.signal_id,
            'sequence_no': self.sequence_no,
            'stage': self.stage,
            'risk_blocked': self.risk_blocked,
            'mode_reason': self.mode_reason,
            'runtime_system': self.runtime_system,
            'timestamp': self.timestamp,
        }


def resolve_runtime_system(env: Optional[str] = None) -> RuntimeSystem:
    value = (env or os.environ.get('TRADING_ENV') or os.environ.get('INFINITRADER_ENV') or 'production').strip().lower()
    if value in ('backtest', 'bt', 'sim'):
        return RuntimeSystem.BACKTEST
    if value in ('judgment', 'judge', 'eval'):
        return RuntimeSystem.JUDGMENT
    return RuntimeSystem.PRODUCTION


def build_unified_log(
    runtime_system: RuntimeSystem,
    component: str,
    event: str,
    error_code: Optional[UnifiedErrorCode] = None,
    **fields: Any,
) -> str:
    from infra.serialization_utils import json_dumps
    payload = {
        'runtime_system': runtime_system.value,
        'component': component,
        'event': event,
        'error_code': error_code.value if error_code else '',
        'timestamp': time.time(),
    }
    payload.update(fields)
    return json_dumps(payload, sort_keys=True)


def get_unified_greeks_calculator() -> Any:
    global _UNIFIED_GREEKS_CALCULATOR
    if _UNIFIED_GREEKS_CALCULATOR is None:
        from governance.greeks_calculator import GreeksCalculator
        _UNIFIED_GREEKS_CALCULATOR = GreeksCalculator()
    return _UNIFIED_GREEKS_CALCULATOR
