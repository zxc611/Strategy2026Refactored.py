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