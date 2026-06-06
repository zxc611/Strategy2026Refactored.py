"""
Phase3-Sprint7: resilience_config.py — 从config_params.py提取的容错/校验相关代码
包含: check_multi_level_cache_consistency, check_indicator_freshness, check_risk_data_freshness,
      _verify_default_param_table_integrity, validate_params, validate_config_schema,
      validate_ui_params, _auto_recovery_decision_tree, check_config_hot_reload
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional


def check_multi_level_cache_consistency() -> bool:
    try:
        from ali2026v3_trading.config_params import _param_table_cache, DEFAULT_PARAM_TABLE, _param_table_lock
        with _param_table_lock:
            if _param_table_cache is None:
                return True
            for k in DEFAULT_PARAM_TABLE:
                if k in _param_table_cache and k not in ('api_key', 'infini_api_key', 'access_key', 'access_secret', '_sensitive_fields'):
                    if _param_table_cache[k] != DEFAULT_PARAM_TABLE[k]:
                        return False
            return True
    except ImportError:
        return True


def check_indicator_freshness(max_age_seconds: float = 300.0) -> bool:
    try:
        from ali2026v3_trading.config_params import _indicator_timestamps
        import time
        now = time.time()
        return all((now - ts) < max_age_seconds for ts in _indicator_timestamps.values())
    except ImportError:
        return True


def check_risk_data_freshness(max_age_seconds: float = 60.0) -> bool:
    try:
        from ali2026v3_trading.config_params import _risk_data_timestamp
        import time
        return (time.time() - _risk_data_timestamp) < max_age_seconds
    except ImportError:
        return True


def _verify_default_param_table_integrity() -> bool:
    try:
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        required_keys = ['max_kline', 'kline_style', 'exchange']
        return all(k in DEFAULT_PARAM_TABLE for k in required_keys)
    except ImportError:
        return False


def validate_params(params: Dict[str, Any], strict: bool = False) -> Dict[str, Any]:
    errors = []
    if not isinstance(params, dict):
        return {'valid': False, 'errors': ['params must be a dict']}
    if 'max_risk_ratio' in params:
        v = params['max_risk_ratio']
        if not isinstance(v, (int, float)) or v < 0 or v > 1:
            errors.append(f'max_risk_ratio={v} not in [0,1]')
    if 'close_stop_loss_ratio' in params:
        v = params['close_stop_loss_ratio']
        if not isinstance(v, (int, float)) or v < 0 or v > 1:
            errors.append(f'close_stop_loss_ratio={v} not in [0,1]')
    if 'signal_cooldown_sec' in params:
        v = params['signal_cooldown_sec']
        if not isinstance(v, (int, float)) or v < 0:
            errors.append(f'signal_cooldown_sec={v} must be non-negative')
    return {'valid': len(errors) == 0, 'errors': errors}


def validate_config_schema(config: Dict[str, Any]) -> Dict[str, Any]:
    errors = []
    if not isinstance(config, dict):
        return {'valid': False, 'errors': ['config must be a dict']}
    return {'valid': len(errors) == 0, 'errors': errors}


def validate_ui_params(params: Dict[str, Any]) -> Dict[str, Any]:
    errors = []
    if not isinstance(params, dict):
        return {'valid': False, 'errors': ['params must be a dict']}
    return {'valid': len(errors) == 0, 'errors': errors}


def _auto_recovery_decision_tree(error_type: str, error_count: int, elapsed_sec: float) -> str:
    if error_count <= 2 and elapsed_sec < 30:
        return 'retry'
    elif error_count <= 5 and elapsed_sec < 120:
        if error_type in ('db_lock', 'network'):
            return 'circuit_break'
        return 'degrade'
    elif error_count <= 10 and elapsed_sec < 300:
        if error_type == 'oom':
            return 'restart'
        return 'degrade'
    else:
        return 'halt'


def check_config_hot_reload(updates: Dict[str, Any]) -> Dict[str, Any]:
    _UNSUPPORTED_KEYS = frozenset([
        'tvf_enabled', 'tvf_sigmoid_scale', 'mode_engine_version',
        'max_risk_ratio', 'close_stop_loss_ratio', 'signal_cooldown_sec',
    ])
    unsupported = set(updates.keys()) & _UNSUPPORTED_KEYS
    return {
        'can_hot_reload': len(unsupported) == 0,
        'unsupported_keys': sorted(unsupported),
        'requires_restart': len(unsupported) > 0,
    }


import os
from ali2026v3_trading.shared_utils import safe_int

CHECKPOINT_INTERVAL_SECONDS = safe_int(os.getenv('CHECKPOINT_INTERVAL_SECONDS', '300'))

RESILIENCE_REGISTRY = {
    'circuit_breaker': {'enabled': True, 'failure_threshold': 5, 'recovery_timeout': 60},
    'retry_with_backoff': {'enabled': True, 'max_retries': 3, 'base_delay': 1.0, 'max_delay': 30.0},
    'bulkhead': {'enabled': True, 'max_concurrent': 10, 'max_queue': 100},
    'timeout': {'enabled': True, 'default_timeout': 30.0},
    'fallback': {'enabled': True, 'fallback_value': None},
}

WATCHDOG_TIMEOUT_ORDER_SEC = float(os.getenv('WATCHDOG_TIMEOUT_ORDER_SEC', '30.0'))
WATCHDOG_TIMEOUT_POSITION_SEC = float(os.getenv('WATCHDOG_TIMEOUT_POSITION_SEC', '30.0'))
WATCHDOG_TIMEOUT_RISK_SEC = float(os.getenv('WATCHDOG_TIMEOUT_RISK_SEC', '30.0'))
WATCHDOG_TIMEOUT_SIGNAL_SEC = float(os.getenv('WATCHDOG_TIMEOUT_SIGNAL_SEC', '60.0'))
WATCHDOG_TIMEOUT_TICK_SEC = float(os.getenv('WATCHDOG_TIMEOUT_TICK_SEC', '10.0'))

DEGRADATION_FEATURES = [
    'greeks_calculation', 'alpha_monitoring', 'shadow_strategy',
    'hft_signal_filter', 'cycle_resonance', 'parameter_optimization',
]

SLA_CONFIG = {
    'max_recovery_time_seconds': 300,
    'max_data_staleness_seconds': 60,
    'min_tick_throughput_per_sec': 10,
    'max_order_latency_ms': 500,
    'max_position_reconciliation_diff': 0,
    'availability_target_pct': 99.9,
}

ALARM_LEVELS = {
    'P0_CRITICAL': {'description': '系统不可用，需立即介入', 'notify': ['phone', 'sms'], 'escalation_sec': 60},
    'P1_HIGH': {'description': '功能严重降级，需30分钟内介入', 'notify': ['sms', 'email'], 'escalation_sec': 300},
    'P2_MEDIUM': {'description': '性能劣化，需当日处理', 'notify': ['email'], 'escalation_sec': 3600},
    'P3_LOW': {'description': '潜在风险，记录跟踪', 'notify': ['log'], 'escalation_sec': 86400},
}

from enum import Enum

class ISOLATION_LEVELS(Enum):
    NONE = "none"
    PROCESS = "process"
    THREAD = "thread"
    SERVICE = "service"
    CONTAINER = "container"

CAPACITY_LIMITS = {
    'max_instruments': 500, 'max_open_positions': 50, 'max_orders_per_sec': 20,
    'max_memory_mb': 4096, 'max_cpu_pct': 80, 'max_disk_gb': 50, 'max_connections': 100,
}

COLD_START_TIMEOUT = safe_int(os.getenv('COLD_START_TIMEOUT', '120'))
COLD_START_PHASES = {
    'config_load': 5, 'db_init': 15, 'history_load': 30, 'instrument_subscribe': 20,
    'model_init': 10, 'shadow_engine_init': 10, 'health_check': 5, 'warmup_ticks': 25,
}