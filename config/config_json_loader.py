"""
Phase3-Sprint8: config_json_loader.py — 从config_params.py提取的JSON加载/热重载逻辑
包含: _resolve_params_json_path, _resolve_runtime_env_name, _apply_env_overrides,
      load_default_params_from_json, check_config_hot_reload
"""
from __future__ import annotations

import errno
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

_PARAMS_JSON_PATH: Optional[str] = None
_PARAMS_JSON_CACHE: Optional[Dict[str, Any]] = None
_PARAMS_JSON_CACHE_TIMESTAMP: float = 0.0
PARAMS_JSON_CACHE_TTL: float = 60.0
_PARAMS_JSON_LOCK = threading.Lock()

_config_file_watchdog_last_mtime: float = 0.0
_config_file_watchdog_last_check: float = 0.0
_CONFIG_WATCHDOG_INTERVAL: float = 60.0


def _resolve_params_json_path() -> str:
    global _PARAMS_JSON_PATH
    if _PARAMS_JSON_PATH and os.path.isfile(_PARAMS_JSON_PATH):
        return _PARAMS_JSON_PATH
    runtime_env = _resolve_runtime_env_name()
    env_possible = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', f'params_default.{runtime_env}.json'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', f'params_default.{runtime_env}.json'),
    ]
    possible = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'params_default.json'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'params_default.json'),
    ]
    for p in env_possible:
        if os.path.isfile(p):
            _PARAMS_JSON_PATH = p
            logging.info("[config_json_loader] 检测到环境参数分支: env=%s path=%s", runtime_env, p)
            return p
    for p in possible:
        if os.path.isfile(p):
            _PARAMS_JSON_PATH = p
            return p
    _PARAMS_JSON_PATH = possible[0]
    return _PARAMS_JSON_PATH


def _resolve_runtime_env_name() -> str:
    env = os.environ.get('TRADING_ENV') or os.environ.get('INFINITRADER_ENV') or 'default'
    env = str(env).strip().lower()
    return env if env else 'default'


def _apply_env_overrides(params: Dict[str, Any]) -> Dict[str, Any]:
    env = _resolve_runtime_env_name()
    if env == 'production':
        pass
    elif env == 'testing':
        params['test_mode'] = True
        logging.info("[CFG-01] TRADING_ENV=testing, override test_mode=True")
    elif env == 'development':
        params['test_mode'] = True
        params['log_level'] = 'DEBUG'
        logging.info("[CFG-01] TRADING_ENV=development, override test_mode=True + log_level=DEBUG")
    return params


def load_default_params_from_json(json_path: Optional[str] = None) -> Dict[str, Any]:
    global _PARAMS_JSON_CACHE, _PARAMS_JSON_CACHE_TIMESTAMP
    from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE
    from ali2026v3_trading.infra.security_config import _validate_path_safety
    with _PARAMS_JSON_LOCK:
        now = time.time()
        if _PARAMS_JSON_CACHE is not None and (now - _PARAMS_JSON_CACHE_TIMESTAMP < PARAMS_JSON_CACHE_TTL):
            return _PARAMS_JSON_CACHE
    path = json_path or _resolve_params_json_path()
    path = _validate_path_safety(path)
    if not os.path.isfile(path):
        _marker_file = os.path.join(os.path.dirname(path), '.params_json_deployed')
        if os.path.isfile(_marker_file):
            logging.error("[R24-P2-DF-02] 参数JSON文件丢失(曾部署): %s，回退到DEFAULT_PARAM_TABLE", path)
        else:
            logging.info("[R24-P2-DF-02] 参数JSON首次部署(文件不存在): %s，使用DEFAULT_PARAM_TABLE", path)
            try:
                with open(_marker_file, 'w', encoding='utf-8') as _mf:
                    _mf.write(str(time.time()))
            except Exception:
                pass
        try:
            from ali2026v3_trading.governance.param_table_provider import get_param_table_provider
            base = get_param_table_provider().get_params('_default')
            return _apply_env_overrides(dict(base) if base else dict(DEFAULT_PARAM_TABLE))
        except Exception:
            return _apply_env_overrides(dict(DEFAULT_PARAM_TABLE))
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.warning("[config_json_loader] 参数JSON解析失败: %s，回退到DEFAULT_PARAM_TABLE", e)
        return _apply_env_overrides(dict(DEFAULT_PARAM_TABLE))
    params_raw = data.get('params', {})
    result = {}
    for key, meta in params_raw.items():
        if isinstance(meta, dict):
            result[key] = meta.get('default')
        else:
            result[key] = meta
    for key, value in DEFAULT_PARAM_TABLE.items():
        if key not in result:
            result[key] = value
    with _PARAMS_JSON_LOCK:
        _PARAMS_JSON_CACHE = result
        _PARAMS_JSON_CACHE_TIMESTAMP = now
    logging.info("[config_json_loader] 从JSON加载参数完成: %s，共 %d 个参数", path, len(result))
    return _apply_env_overrides(result)


def check_config_hot_reload() -> bool:
    global _config_file_watchdog_last_mtime, _config_file_watchdog_last_check
    from ali2026v3_trading.config.config_params import get_cached_params, reset_param_cache, _notify_param_change
    now = time.time()
    if now - _config_file_watchdog_last_check < _CONFIG_WATCHDOG_INTERVAL:
        return False
    _config_file_watchdog_last_check = now
    path = _resolve_params_json_path()
    if not os.path.isfile(path):
        return False
    try:
        current_mtime = os.path.getmtime(path)
        if current_mtime > _config_file_watchdog_last_mtime:
            try:
                with open(path, 'r', encoding='utf-8') as _cfg_f:
                    _raw_config = json.load(_cfg_f)
                _format_ver = _raw_config.get('format_version', None)
                if _format_ver is not None and _format_ver != '1.0':
                    logging.warning("[P1-FIX] Config hot-reload: format_version mismatch: %s (expected 1.0)", _format_ver)
                    _config_file_watchdog_last_mtime = current_mtime
                    return False
            except (json.JSONDecodeError, IOError) as _cfg_err:
                logging.warning("[P1-FIX] Config hot-reload: failed to read config: %s", _cfg_err)
                _config_file_watchdog_last_mtime = current_mtime
                return False
            _config_file_watchdog_last_mtime = current_mtime
            reset_param_cache()
            logging.info("[R4-D-10] 配置热更新 %s 已变更，缓存已刷新", path)
            try:
                from ali2026v3_trading.serialization_utils import json_dumps, json_loads
                _reloaded_params = get_cached_params()
                _serialized = json_dumps(_reloaded_params)
                _deserialized = json_loads(_serialized)
                if _reloaded_params != _deserialized:
                    logging.warning(
                        "SER-P1-14: 热重载后序列化格式不一致: 原始keys=%d, 反序列化keys=%d",
                        len(_reloaded_params) if isinstance(_reloaded_params, dict) else 0,
                        len(_deserialized) if isinstance(_deserialized, dict) else 0,
                    )
            except Exception as _ser_err:
                logging.warning("SER-P1-14: 热重载后序列化格式验证失败: %s", _ser_err)
            _notify_param_change([], 'hot_reload')
            return True
    except OSError as _os_err:
        if getattr(_os_err, 'errno', None) == errno.ENOSPC:
            logging.critical("[P1-16] 磁盘空间不足，配置热重载失败: %s", _os_err)
        else:
            logging.debug("[P1-16] 配置热重载OSError(非ENOSPC): %s", _os_err)
    return False