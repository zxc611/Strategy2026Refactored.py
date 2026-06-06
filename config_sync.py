"""
Phase3-Sprint8: config_sync.py — 从config_params.py提取的配置同步/校验逻辑
包含: _sync_defaults_from_attribute_matrix, verify_param_pool_sync
"""
from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Dict

from ali2026v3_trading.serialization_utils import yaml_safe_load


def _sync_defaults_from_attribute_matrix() -> None:
    from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE, _DEFAULT_PARAM_TABLE_LOCK
    matrix_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'param_pool',
        'parameter_attribute_matrix.yaml',
    )
    if not os.path.isfile(matrix_path):
        return
    try:
        import yaml
    except Exception:
        logging.info("[config_sync] attribute_matrix同步跳过: PyYAML不可用")
        return
    try:
        with open(matrix_path, 'r', encoding='utf-8') as f:
            matrix = yaml_safe_load(f) or {}
    except (OSError, ValueError, yaml.YAMLError) as e:
        logging.warning("[config_sync] 读取attribute_matrix失败: %s", e)
        return
    if not isinstance(matrix, dict):
        return
    synced = []
    with _DEFAULT_PARAM_TABLE_LOCK:
        for key, meta in matrix.items():
            if key not in DEFAULT_PARAM_TABLE:
                continue
            if not isinstance(meta, dict) or 'default' not in meta:
                continue
            new_val = meta.get('default')
            old_val = DEFAULT_PARAM_TABLE.get(key)
            if isinstance(old_val, bool) and isinstance(new_val, bool):
                if old_val != new_val:
                    DEFAULT_PARAM_TABLE[key] = new_val
                    synced.append(key)
            elif isinstance(old_val, int) and isinstance(new_val, (int, float)):
                cast_val = int(new_val)
                if old_val != cast_val:
                    DEFAULT_PARAM_TABLE[key] = cast_val
                    synced.append(key)
            elif isinstance(old_val, float) and isinstance(new_val, (int, float)):
                cast_val = float(new_val)
                if old_val != cast_val:
                    DEFAULT_PARAM_TABLE[key] = cast_val
                    synced.append(key)
            elif isinstance(old_val, str) and isinstance(new_val, str):
                if old_val != new_val:
                    DEFAULT_PARAM_TABLE[key] = new_val
                    synced.append(key)
    if synced:
        logging.info("[config_sync] attribute_matrix默认值已同步到DEFAULT_PARAM_TABLE: %s", ','.join(sorted(synced)))


def verify_param_pool_sync():
    """NEW-R6-10修复: 验证param_pool/与参数池/目录关键文件同步性"""
    _base = os.path.dirname(os.path.abspath(__file__))
    _pp1 = os.path.join(_base, 'param_pool')
    _pp2 = os.path.join(_base, 'param_pool')
    if not os.path.isdir(_pp1) or not os.path.isdir(_pp2):
        return True
    _key_files = ['task_scheduler.py', 'parameter_attribute_matrix.yaml',
                  'enhanced_phase_scan.py', 'optuna_multiobjective_search.py',
                  'preprocess_ticks.py', 'l2_optimizer.py']
    _mismatches = []
    for _fname in _key_files:
        _f1 = os.path.join(_pp1, _fname)
        _f2 = os.path.join(_pp2, _fname)
        if os.path.isfile(_f1) and os.path.isfile(_f2):
            with open(_f1, 'rb') as _fh1, open(_f2, 'rb') as _fh2:
                _h1 = hashlib.md5(_fh1.read()).hexdigest()
                _h2 = hashlib.md5(_fh2.read()).hexdigest()
                if _h1 != _h2:
                    _mismatches.append(_fname)
    if _mismatches:
        logging.critical("[SYNC-CHECK] param_pool/与参数池/目录文件不一致: %s", _mismatches)
        return False
    return True