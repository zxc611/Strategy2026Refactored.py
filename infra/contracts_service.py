# [M1-71] 服务契约
# MODULE_ID: M1-086

"""contracts_service.py - 契约与特性标志服务合并

合并发service_contracts.py + system_contracts.py + phase_feature_flag.py (2026-06-12)

"""

from __future__ import annotations



import json

import logging

import os

import time

import threading

from dataclasses import dataclass

from enum import Enum

from typing import Any, Dict, Optional, Protocol, runtime_checkable



from ali2026v3_trading.infra.serialization_utils import json_dumps





# ============================================================

# Section 1: ServiceProtocol (from service_contracts.py)

# ============================================================



@runtime_checkable

class ServiceProtocol(Protocol):

    """服务协议 _所有服务类应满足此接口"""



    def health_check(self) -> Dict[str, Any]:

        """返回服务健康状态字典"""

        ...



    def get_service_name(self) -> str:

        """返回服务名称"""

        ...





# ============================================================

# Section 2: System contracts (from system_contracts.py)

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

        from ali2026v3_trading.governance.greeks_calculator import GreeksCalculator

        _UNIFIED_GREEKS_CALCULATOR = GreeksCalculator()

    return _UNIFIED_GREEKS_CALCULATOR





# ============================================================

# Section 3: PhaseFeatureFlag (from phase_feature_flag.py)

# ============================================================



class PhaseFeatureFlag:

    """重构Phase的Feature Flag控制"""



    _flags: Dict[str, bool] = {

        'USE_PIPELINED_SEND_ORDER': True,

        'USE_FILTER_CHAIN_SIGNAL': True,

        'USE_BUILDER_INIT': True,

        'USE_HEALTH_AGGREGATOR': True,

        'USE_PARAM_TABLE_PROVIDER': True,

        'USE_RISK_CHECK_ENGINE': True,

        'USE_VALIDATION_REGISTRY': True,

    }

    _lock = threading.RLock()

    _change_log: list = []



    @classmethod

    def is_enabled(cls, flag_name: str) -> bool:

        with cls._lock:

            return cls._flags.get(flag_name, False)



    @classmethod

    def enable(cls, flag_name: str) -> None:

        with cls._lock:

            old = cls._flags.get(flag_name, False)

            cls._flags[flag_name] = True

            cls._change_log.append((flag_name, old, True))

            logging.info("[FeatureFlag] %s: %s -> True", flag_name, old)



    @classmethod

    def disable(cls, flag_name: str) -> None:

        with cls._lock:

            old = cls._flags.get(flag_name, False)

            cls._flags[flag_name] = False

            cls._change_log.append((flag_name, old, False))

            logging.info("[FeatureFlag] %s: %s -> False", flag_name, old)



    @classmethod

    def disable_all(cls) -> None:

        with cls._lock:

            for k in cls._flags:

                cls._flags[k] = False

            logging.critical("[FeatureFlag] 全局紧急回撤 所有Feature Flag已关闭")



    @classmethod

    def get_all(cls) -> Dict[str, bool]:

        with cls._lock:

            return dict(cls._flags)



    @classmethod

    def get_change_log(cls) -> list:

        with cls._lock:

            return list(cls._change_log)





__all__ = [

    'ServiceProtocol',

    'RuntimeSystem', 'UnifiedErrorCode', 'UnifiedDataSourceAdapter',

    'HealthStatusLevel', 'normalize_health_status',

    'SignalDiagnosisRecord', 'resolve_runtime_system',

    'build_unified_log', 'get_unified_greeks_calculator',

    'PhaseFeatureFlag',

]

