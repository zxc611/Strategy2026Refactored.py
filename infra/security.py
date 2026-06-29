"""security/统一安全模块 — 合并自2个子模块"""
from __future__ import annotations

import logging
import os
import re
import threading
import traceback
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

try:
    from ali2026v3_trading.infra.shared_utils import CHINA_TZ
except ImportError:
    CHINA_TZ = None

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5

import sys as _sys_mlock
import ctypes as _ctypes_mlock


# ============================================================
# Section 1: 安全配置 (原 security_config.py)
# ============================================================

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
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] security Windows file attributes check suppressed: %s", _r3_err)
                pass
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        logging.debug("[R13-P1-SEC-05] 权限检查跳过 %s", e)


def _filter_sensitive_keys(keys: List[str]) -> List[str]:
    return [k for k in keys if k not in SENSITIVE_KEYS]


def check_key_lifecycle() -> Dict[str, Any]:
    try:
        from ali2026v3_trading.config.config_service import get_cached_params
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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


# ============================================================
# Section 2: 安全加固 (原 security_hardening.py)
# ============================================================

class SecurityProfile(Enum):
    DEV = "development"
    STAGING = "staging"
    PRODUCTION = "production"


# R15-P2-SEC-08修复: 环境安全配置差异化
ENV_SECURITY_PROFILES = {
    SecurityProfile.DEV: {
        'log_level': logging.DEBUG,
        'expose_stack_traces': True,
        'expose_internal_paths': True,
        'allow_pickle': True,
        'max_stack_trace_lines': 50,
        'sanitize_errors': False,
    },
    SecurityProfile.STAGING: {
        'log_level': logging.INFO,
        'expose_stack_traces': True,
        'expose_internal_paths': False,
        'allow_pickle': False,
        'max_stack_trace_lines': 10,
        'sanitize_errors': True,
    },
    SecurityProfile.PRODUCTION: {
        'log_level': logging.WARNING,
        'expose_stack_traces': False,
        'expose_internal_paths': False,
        'allow_pickle': False,
        'max_stack_trace_lines': 0,
        'sanitize_errors': True,
    },
}


def _get_current_security_profile() -> SecurityProfile:
    """从环境变量获取当前安全配置"""
    env = os.getenv('ALI2026_SECURITY_PROFILE', 'development').lower()
    for profile in SecurityProfile:
        if profile.value == env:
            return profile
    return SecurityProfile.DEV


def _get_security_config() -> Dict[str, Any]:
    """获取当前环境的安全配置"""
    profile = _get_current_security_profile()
    return ENV_SECURITY_PROFILES[profile]


# R15-P2-SEC-02修复: 错误消息消毒，脱敏内部路径
_INTERNAL_PATH_PATTERN = re.compile(
    r'(?:[A-Z]:\\|/home/|/Users/|/root/|/opt/|/var/|/tmp/)[\w./\\-]+'
)



def apply_security_profile(profile: SecurityProfile) -> Dict[str, Any]:
    """SEC-P2-01/03修复: 应用指定安全配置profile

    根据profile设置日志级别、堆栈暴露策略等安全参数。
    返回应用的安全配置字典。
    """
    config = ENV_SECURITY_PROFILES.get(profile, ENV_SECURITY_PROFILES[SecurityProfile.DEV])
    log_level = config.get('log_level', logging.WARNING)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    logger.info("[SEC-P2-03] 安全配置已应用: profile=%s, log_level=%s", profile.name, logging.getLevelName(log_level))
    return config

def SANITIZE_ERROR_MSG(error_msg: str) -> str:
    """消毒错误消息: 替换内部路径为<PATH_REDACTED>"""
    config = _get_security_config()
    if not config.get('sanitize_errors', True):
        return error_msg
    return _INTERNAL_PATH_PATTERN.sub('<PATH_REDACTED>', error_msg)


# R15-P2-SEC-04修复: 异常堆栈截断，PRODUCTION时隐藏代码结构
def _truncate_stack_trace(exc: Optional[Exception] = None, max_lines: Optional[int] = None) -> str:
    """截断异常堆栈输出

    Args:
        exc: 异常对象，None时使用当前异常
        max_lines: 最大行数，None时从安全配置获取

    Returns:
        截断后的堆栈字符串
    """
    config = _get_security_config()
    if max_lines is None:
        max_lines = config.get('max_stack_trace_lines', 0)
    if not config.get('expose_stack_traces', True):
        return f"<STACK_TRACE_REDACTED: {type(exc).__name__ if exc else 'Exception'}>"
    tb_str = traceback.format_exc() if exc is None else ''.join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)
    )
    if max_lines > 0:
        lines = tb_str.strip().split('\n')
        if len(lines) > max_lines:
            return '\n'.join(lines[:max_lines]) + f'\n... [{len(lines) - max_lines} more lines truncated]'
    return tb_str


# R15-P2-SEC-07修复: pickle安全检查
_UNPICKLE_ALLOWED = False  # 默认禁止unpickle不可信数据

def _safe_unpickle(data: bytes, allow_untrusted: bool = False) -> Any:
    """安全unpickle: 默认禁止unpickle不可信数据源

    Args:
        data: pickle字节流
        allow_untrusted: 是否允许不可信数据unpickle(仅DEV环境)

    Raises:
        RuntimeError: 禁止unpickle不可信数据时
    """
    if not allow_untrusted:
        config = _get_security_config()
        if not config.get('allow_pickle', False):
            raise RuntimeError(
                "R15-P2-SEC-07: unpickle不可信数据被禁止。"
                "设置环境变量 ALI2026_SECURITY_PROFILE=development 或 allow_untrusted=True 绕过(仅限开发环境)"
            )
    import pickle
    return pickle.loads(data)  # R21-MEM-P2-07修复: pickle.loads从bytes直接反序列化，已是最低开销路径


# R15-P2-SEC-01确认: 路径拼接realpath消毒已在SEC-P1-02修复
# R15-P2-SEC-05确认: requirements-freeze.txt已存在，第三方库版本已锁定
# R15-P2-SEC-06修复: .bandit配置文件(见项目根目录.bandit文件)


# ============================================================================
# R16-P1-SEC-06修复: mlock实现防止core dump泄露密钥
# ============================================================================

def enable_mlock_if_linux() -> bool:
    """Linux环境下调用mlockall防止内存被swap到磁盘，避免core dump泄露密钥

    Returns:
        bool: True表示成功启用mlock，False表示不支持或失败
    """
    if _sys_mlock.platform != 'linux':
        logger.debug("[R16-P1-SEC-06] mlock仅支持Linux，当前平台: %s", _sys_mlock.platform)
        return False
    try:
        libc = _ctypes_mlock.CDLL('libc.so.6', use_errno=True)
    except OSError:
        try:
            libc = _ctypes_mlock.CDLL('libc.musl.so.1', use_errno=True)
        except OSError:
            libc = None
    if libc is None:
        logger.warning("[R16-P1-SEC-06] mlock: 未找到libc (glibc/musl), 跳过mlockall")
        return False
    try:
        MCL_CURRENT = 1
        MCL_FUTURE = 2
        result = libc.mlockall(MCL_CURRENT | MCL_FUTURE)
        if result == 0:
            logger.info("[R16-P1-SEC-06] mlockall()成功，内存已锁定，防止core dump泄露")
            return True
        else:
            errno = _ctypes_mlock.get_errno()
            logger.warning("[R16-P1-SEC-06] mlockall()失败，errno=%d (可能需要CAP_IPC_LOCK权限)", errno)
            return False
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        logger.warning("[R16-P1-SEC-06] mlock启用失败: %s", e)
        return False


def disable_core_dump() -> bool:
    """禁用core dump（Linux/Unix）

    Returns:
        bool: True表示成功禁用
    """
    if _sys_mlock.platform not in ('linux', 'darwin'):
        return False
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        logger.info("[R16-P1-SEC-06] core dump已禁用")
        return True
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        logger.warning("[R16-P1-SEC-06] 禁用core dump失败: %s", e)
        return False
