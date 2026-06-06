"""Cross-system contracts for production/backtest/judgment alignment."""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Protocol

from ali2026v3_trading.serialization_utils import json_dumps


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
        from ali2026v3_trading.greeks_calculator import GreeksCalculator
        _UNIFIED_GREEKS_CALCULATOR = GreeksCalculator()
    return _UNIFIED_GREEKS_CALCULATOR
