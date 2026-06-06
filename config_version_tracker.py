"""
Phase3-Sprint8: config_version_tracker.py — 从config_params.py提取的参数版本追踪逻辑
包含: _record_param_version_snapshot, get_param_version, list_param_version_history, rollback_param_version
"""
from __future__ import annotations

import copy
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

from ali2026v3_trading.serialization_utils import json_dumps
from ali2026v3_trading.shared_utils import safe_int

_param_version_counter: int = 0
_param_version_hash: str = ""
_param_version_history: List[Dict[str, Any]] = []
_PARAM_VERSION_HISTORY_MAX = 50


def _record_param_version_snapshot(source: str, params: Optional[Dict[str, Any]] = None) -> None:
    global _param_version_history
    if params is None:
        from ali2026v3_trading.config_params import get_cached_params
        params = get_cached_params()
    _param_version_history.append({
        'version': _param_version_counter,
        'hash': _param_version_hash,
        'timestamp': time.time(),
        'source': source,
        'param_count': len(params),
        'params': copy.deepcopy(params),
    })
    if len(_param_version_history) > _PARAM_VERSION_HISTORY_MAX:
        _param_version_history = _param_version_history[-_PARAM_VERSION_HISTORY_MAX:]


def get_param_version() -> Dict[str, Any]:
    global _param_version_counter, _param_version_hash
    from ali2026v3_trading.config_params import get_cached_params
    params = get_cached_params()
    content = json_dumps(params)
    new_hash = hashlib.md5(content.encode()).hexdigest()[:12]
    if new_hash != _param_version_hash:
        _param_version_counter += 1
        _param_version_hash = new_hash
        logging.info("[R4-D-12] 参数版本更新: v%d (hash=%s)", _param_version_counter, new_hash)
        _record_param_version_snapshot('get_param_version', params)
    return {
        "version": _param_version_counter,
        "hash": _param_version_hash,
        "param_count": len(params),
        "timestamp": time.time(),
    }


def list_param_version_history(limit: int = 20) -> List[Dict[str, Any]]:
    safe_limit = max(1, int(limit))
    rows = _param_version_history[-safe_limit:]
    return [
        {
            'version': row.get('version', 0),
            'hash': row.get('hash', ''),
            'timestamp': row.get('timestamp', 0.0),
            'source': row.get('source', 'unknown'),
            'param_count': row.get('param_count', 0),
        }
        for row in rows
    ]


def rollback_param_version(version: int) -> bool:
    global _param_version_counter, _param_version_hash
    from ali2026v3_trading.config_params import (
        _param_table_cache, _param_table_cache_timestamp,
        _param_table_lock, _notify_param_change,
    )
    with _param_table_lock:
        target = None
        for row in reversed(_param_version_history):
            if safe_int(row.get('version', -1)) == safe_int(version):
                target = row
                break
        if target is None:
            return False
        from ali2026v3_trading.config_params import _param_table_cache as _cache_ref
        import ali2026v3_trading.config_params as _cp
        _cp._param_table_cache = copy.deepcopy(target.get('params', {}))
        _cp._param_table_cache_timestamp = time.time()
        _param_version_counter = safe_int(target.get('version', _param_version_counter))
        _param_version_hash = str(target.get('hash', _param_version_hash))
    _notify_param_change([], 'rollback_param_version')
    logging.warning("[config_version_tracker] 参数回滚完成: version=%s hash=%s", _param_version_counter, _param_version_hash)
    return True