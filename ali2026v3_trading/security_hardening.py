"""
R15-P2-SEC-03修复: 安全加固框架模块

提供统一安全基础设施:
- 错误消息消毒(脱敏内部路径)
- 异常堆栈截断(PRODUCTION模式下隐藏代码结构)
- pickle安全检查(禁止unpickle不可信数据)
- 环境安全配置差异化(DEV/STAGING/PRODUCTION)
"""
from __future__ import annotations

import logging
import os
import re
import traceback
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


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
import sys as _sys_mlock
import ctypes as _ctypes_mlock

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
    except Exception as e:
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
    except Exception as e:
        logger.warning("[R16-P1-SEC-06] 禁用core dump失败: %s", e)
        return False
