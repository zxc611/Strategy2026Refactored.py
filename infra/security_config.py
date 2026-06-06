"""
Phase3-Sprint7: security_config.py — 从config_params.py提取的安全相关代码
包含: _SecureCredential, reveal_credential, _sanitize_for_return, get_sensitive_credential,
      _validate_path_safety, _check_config_file_permissions, _filter_sensitive_keys,
      check_key_lifecycle, SecurityEventResponder, get_security_responder
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from ali2026v3_trading.infra.shared_utils import CHINA_TZ
except ImportError:
    CHINA_TZ = None

SENSITIVE_KEYS = frozenset(['api_key', 'infini_api_key', 'access_key', 'access_secret'])


class _SecureCredential:
    __slots__ = ('_obfuscated', '_seed')

    def __init__(self, value: str):
        self._seed = os.urandom(8)
        self._obfuscated = self._obfuscate(value)

    def _obfuscate(self, value: str) -> bytes:
        if not value:
            return b''
        data = value.encode('utf-8')
        key = self._seed * ((len(data) // len(self._seed)) + 1)
        return bytes(a ^ b for a, b in zip(data, key[:len(data)]))

    def reveal(self) -> str:
        if not self._obfuscated:
            return ''
        key = self._seed * ((len(self._obfuscated) // len(self._seed)) + 1)
        data = bytes(a ^ b for a, b in zip(self._obfuscated, key[:len(self._obfuscated)]))
        return data.decode('utf-8')

    def __repr__(self) -> str:
        return '***REDACTED***'

    def __str__(self) -> str:
        return '***REDACTED***'


def reveal_credential(cred) -> str:
    if isinstance(cred, _SecureCredential):
        return cred.reveal()
    return str(cred) if cred else ''


def _sanitize_for_return(params: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = {}
    for k, v in params.items():
        if k in SENSITIVE_KEYS:
            sanitized[k] = '***REDACTED***'
        else:
            sanitized[k] = v
    return sanitized


_TRUSTED_CREDENTIAL_CALLERS = frozenset(['order_service', 'risk_service', 'strategy_core_service'])


def get_sensitive_credential(key_name: str, caller_id: str = '') -> str:
    if caller_id not in _TRUSTED_CREDENTIAL_CALLERS:
        logging.error("[security_config] 敏感凭据访问被拒绝: key=%s caller=%s", key_name, caller_id)
        return ''
    try:
        from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE, _param_table_cache, _param_table_lock
        with _param_table_lock:
            val = _param_table_cache.get(key_name, DEFAULT_PARAM_TABLE.get(key_name, ''))
    except ImportError:
        return ''
    if isinstance(val, _SecureCredential):
        return val.reveal()
    return str(val) if val else ''


_ALLOWED_BASE_DIRS = [
    os.path.dirname(os.path.abspath(__file__)),
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
]


def _validate_path_safety(file_path: str, allowed_base_dirs: Optional[List[str]] = None) -> str:
    resolved = os.path.realpath(os.path.abspath(file_path))
    bases = allowed_base_dirs or _ALLOWED_BASE_DIRS
    if not any(resolved.startswith(base) for base in bases):
        raise ValueError(
            f"Path traversal detected: '{file_path}' resolves to '{resolved}' "
            f"which is outside allowed directories"
        )
    return resolved


def _check_config_file_permissions(file_path: str) -> None:
    if not os.path.isfile(file_path):
        return
    try:
        import stat
        file_stat = os.stat(file_path)
        if os.name != 'nt':
            mode = file_stat.st_mode
            if mode & stat.S_IROTH:
                logging.warning(
                    "[R13-P1-SEC-05] 配置文件 '%s' 对所有用户可读(mode=%o)存在"
                    "建议限制权限: chmod 600 %s",
                    file_path, stat.S_IMODE(mode), file_path,
                )
        else:
            try:
                import ctypes
                attrs = ctypes.windll.kernel32.GetFileAttributesW(file_path)
                if attrs != 0xFFFFFFFF and not (attrs & 0x1):
                    logging.info(
                        "[R13-P1-SEC-05] 配置文件 '%s' 在Windows上非只读"
                        "建议设置适当的NTFS权限限制访问",
                        file_path,
                    )
            except Exception:
                pass
    except Exception as e:
        logging.debug("[R13-P1-SEC-05] 权限检查跳过 %s", e)


def _filter_sensitive_keys(keys: List[str]) -> List[str]:
    return [k for k in keys if k not in SENSITIVE_KEYS]


def check_key_lifecycle() -> Dict[str, Any]:
    try:
        from ali2026v3_trading.config.config_params import get_cached_params
        params = get_cached_params()
    except ImportError:
        params = {}
    rotation_days = params.get('key_rotation_interval_days', 90)
    max_age_hours = params.get('key_max_age_hours', 2160)
    last_rotation = params.get('key_last_rotation_time')
    result = {
        'rotation_days': rotation_days,
        'max_age_hours': max_age_hours,
        'needs_rotation': False,
        'days_since_rotation': None,
    }
    if last_rotation:
        try:
            last_dt = datetime.fromisoformat(last_rotation) if isinstance(last_rotation, str) else last_rotation
            days_since = (datetime.now(CHINA_TZ) - last_dt).days
            result['days_since_rotation'] = days_since
            result['needs_rotation'] = days_since >= rotation_days
            if result['needs_rotation']:
                logging.warning(
                    "R15-P1-SEC-09: API密钥已超过轮换周期 days_since=%d rotation_days=%d",
                    days_since, rotation_days
                )
        except Exception as e:
            logging.debug("R15-P1-SEC-09: key_last_rotation_time解析失败: %s", e)
            result['needs_rotation'] = True
    else:
        result['needs_rotation'] = True
        logging.info("R15-P1-SEC-09: 密钥从未轮换，建议立即轮换")
    return result


class SecurityEventResponder:

    def __init__(self):
        self._blocked_sources: set = set()
        self._suspicious_counts: Dict[str, int] = {}
        self._lock = threading.Lock()

    def report_suspicious(self, source: str, reason: str) -> bool:
        with self._lock:
            self._suspicious_counts[source] = self._suspicious_counts.get(source, 0) + 1
            if self._suspicious_counts[source] >= 3:
                self._blocked_sources.add(source)
                logging.critical(
                    "R15-P1-SEC-12: 安全自动阻断! source=%s reason=%s count=%d",
                    source, reason, self._suspicious_counts[source]
                )
                return True
            logging.warning(
                "R15-P1-SEC-12: 可疑活动记录 source=%s reason=%s count=%d/3",
                source, reason, self._suspicious_counts[source]
            )
            return False

    def is_blocked(self, source: str) -> bool:
        with self._lock:
            return source in self._blocked_sources

    def unblock(self, source: str) -> None:
        with self._lock:
            self._blocked_sources.discard(source)
            self._suspicious_counts.pop(source, None)


_security_responder: Optional[SecurityEventResponder] = None
_security_responder_lock = threading.Lock()


def get_security_responder() -> SecurityEventResponder:
    global _security_responder
    with _security_responder_lock:
        if _security_responder is None:
            _security_responder = SecurityEventResponder()
        return _security_responder