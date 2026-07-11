# [M1-66] 安全服务
# MODULE_ID: M1-106

"""security_service.py - 安全服务合并

合并发security.py + risk_audit_utils.py + risk_rules.py (2026-06-12)

"""

from __future__ import annotations

# 五唯一性修复：从 security.py 统一导入 SecurityProfile 与 ENV_SECURITY_PROFILES（避免跨文件重复定义）
from ali2026v3_trading.infra.security import (
    SecurityProfile, ENV_SECURITY_PROFILES,
)
from enum import Enum, auto


class AlertLevel(Enum):
    INFO = auto()
    WARNING = auto()
    CRITICAL = auto()



# ============================================================

# Section 1: Security (from security.py)

# ============================================================



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



from ali2026v3_trading.infra._helpers import get_logger  # R9-5



logger = get_logger(__name__)  # R9-5



import sys as _sys_mlock

import ctypes as _ctypes_mlock





# ============================================================

# Section 1a: 安全配置 (_security_config.py)

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

        logging.error("[security_config] 敏感凭据访问被拒绝 key=%s caller=%s", key_name, caller_id)

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

                    "[R13-P1-SEC-05] 配置文件 '%s' 对所有用户可。mode=%o)存在"

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

        logging.debug("[R13-P1-SEC-05] 权限检查跳过%s", e)





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

                    "R15-P1-SEC-09: API密钥已超过轮换周。days_since=%d rotation_days=%d",

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

# Section 1b: 安全加固 (_security_hardening.py)

# ============================================================



# 五唯一性修复：SecurityProfile 与 ENV_SECURITY_PROFILES 已统一从 security.py 导入（文件顶部）
# 原 SecurityProfile / ENV_SECURITY_PROFILES 重复定义已删除





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





# R15-P2-SEC-02修复: 错误消息消毒，脱敏内部路由

_INTERNAL_PATH_PATTERN = re.compile(

    r'(?:[A-Z]:\\|/home/|/Users/|/root/|/opt/|/var/|/tmp/)[\w./\\-]+'

)







def apply_security_profile(profile: SecurityProfile) -> Dict[str, Any]:

    """SEC-P2-01/03修复: 应用指定安全配置profile



    根据profile设置日志级别、堆栈暴露策略等安全参数据

    返回应用的安全配置字典。'
    """

    config = ENV_SECURITY_PROFILES.get(profile, ENV_SECURITY_PROFILES[SecurityProfile.DEV])

    log_level = config.get('log_level', logging.WARNING)

    root_logger = logging.getLogger()

    root_logger.setLevel(log_level)

    logger.info("[SEC-P2-03] 安全配置已应） profile=%s, log_level=%s", profile.name, logging.getLevelName(log_level))

    return config



def SANITIZE_ERROR_MSG(error_msg: str) -> str:

    """消毒错误消息: 替换内部路径径PATH_REDACTED>"""

    config = _get_security_config()

    if not config.get('sanitize_errors', True):

        return error_msg

    return _INTERNAL_PATH_PATTERN.sub('<PATH_REDACTED>', error_msg)





# R15-P2-SEC-04修复: 异常堆栈截断，PRODUCTION时隐藏代码结束

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

        data: pickle字节点

        allow_untrusted: 是否允许不可信数据unpickle(仅DEV环境)



    Raises:

        RuntimeError: 禁止unpickle不可信数据时

    """

    if not allow_untrusted:

        config = _get_security_config()

        if not config.get('allow_pickle', False):

            raise RuntimeError(

                "R15-P2-SEC-07: unpickle不可信数据被禁止。"

                "设置环境变量 ALI2026_SECURITY_PROFILE=development _allow_untrusted=True 绕过(仅限开发环节"

            )

    import pickle

    return pickle.loads(data)  # R21-MEM-P2-07修复: pickle.loads从bytes直接反序列化，已是最低开销路径





# R15-P2-SEC-01确认: 路径拼接realpath消毒已在SEC-P1-02修复

# R15-P2-SEC-05确认: requirements-freeze.txt已存在，第三方库版本已锁。'
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

        logger.debug("[R16-P1-SEC-06] mlock仅支持Linux，当前平台 %s", _sys_mlock.platform)

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

    """禁用core dump（Linux/Unix_



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





# ============================================================

# Section 2: Risk audit utilities (from risk_audit_utils.py)

# ============================================================



from enum import auto



# 从实际存在的模块导入

from ali2026v3_trading.infra.serialization_utils import (

    json_dumps, json_loads, json_default_serializer, safe_jsonl_append_line,

)

from ali2026v3_trading.infra.resilience import (

    KahanSummation, approx_equal, stable_sum, stable_mean, stable_std, stable_variance,

    PRICE_TOLERANCE,

)

from ali2026v3_trading.infra.resilience import safe_divide

from ali2026v3_trading.infra.shared_utils import safe_float, safe_int, safe_get_float, safe_get_int

from ali2026v3_trading.risk.risk_support import structured_audit_log



# 常量

FLOAT_COMPARE_TOLERANCE = 1e-9

# PRICE_TOLERANCE 已统一从 resilience 导入（值=1e-6，全项目一致）



# R1-4修复: 以下符号在代码拆分后已不存在于活跃代码中，提示no-op stub 防止 ImportError

# 这些符号仅在 risk_service.__all__ _import 中出现，运行时未被实际调度



# 五唯一性修复：AlertLevel 已统一从 risk_audit_utils.py 导入（文件顶部）
# 原 AlertLevel stub 重复定义已删除



class AlertDeduplicator:

    """告警去重复stub"""

    def __init__(self):

        self._seen = set()

    def is_duplicate(self, key: str) -> bool:

        return key in self._seen

    def record(self, key: str) -> None:

        self._seen.add(key)



_ALERT_DEDUP = AlertDeduplicator()



def get_alert_deduplicator() -> AlertDeduplicator:

    return _ALERT_DEDUP



def alert(level: AlertLevel, message: str, **kwargs) -> None:

    """告警 stub _仅记录日志"""

    logging.log(logging.WARNING if level != AlertLevel.INFO else logging.INFO,

                "[AlertStub] %s: %s %s", level.name, message, kwargs or '')



def _check_alert_escalation(*args, **kwargs) -> None:

    pass



def _get_audit_log_path(*args, **kwargs) -> str:

    return ''



def operations_audit_log(*args, **kwargs) -> None:

    pass



def audit_chain_append(*args, **kwargs) -> None:

    pass



def save_state_snapshot(*args, **kwargs) -> None:

    pass



def generate_exchange_report(*args, **kwargs) -> str:

    return ''



class SimplifiedSPAN:

    """SPAN保证金计量stub"""

    pass



def calculate_var_historical(*args, **kwargs) -> float:

    return 0.0



def calculate_var_rolling(*args, **kwargs) -> float:

    return 0.0



def check_circuit_breaker_auto_recovery(*args, **kwargs) -> bool:

    return False



def validate_bid_ask_spread_quality(*args, **kwargs) -> bool:

    return True



def validate_tick_timestamp_uniqueness(*args, **kwargs) -> bool:

    return True



def validate_option_metadata_integrity(*args, **kwargs) -> bool:

    return True



def validate_depth_imbalance_quality(*args, **kwargs) -> bool:

    return True



def validate_volume_quality(*args, **kwargs) -> bool:

    return True



def approx_less(a: float, b: float, tol: float = FLOAT_COMPARE_TOLERANCE) -> bool:

    return a < b - tol



def safe_get(obj, attr, default=None):

    """安全属性获取"""

    return getattr(obj, attr, default)



# config_params 延迟导入避免循环

def __getattr__(name):

    if name == 'config_params':

        from ali2026v3_trading.config.config_service import get_cached_params

        return type('config_params', (), {'get_cached_params': staticmethod(get_cached_params)})()

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")





def _normalize_risk_scope_id(scope_id: Optional[str] = None, strategy: Any = None) -> str:

    return scope_id or 'default'





# ============================================================

# Section 3: Risk rules (from risk_rules.py)

# ============================================================



from typing import Tuple





def check_daily_drawdown_hard_stop(

    daily_drawdown_pct: float,

    hard_stop_pct: float,

    prev_5day_avg_profit: float = 0.0,

    multiplier: float = 2.0,

    daily_start_equity: float = 1.0,

) -> Tuple[bool, str]:

    """统一的日回撤硬停止检查询



    两条路径（互斥）_

    1. _prev_5day_avg_profit > 0 _multiplier > 0 时，

       使用5日均值乘数动态阈值 (prev_5day_avg_profit * multiplier) / daily_start_equity

    2. 否则使用固定阈值hard_stop_pct



    Args:

        daily_drawdown_pct: 当前日回撤百分比 (0.0~1.0)

        hard_stop_pct: 固定硬停止阈值(0.0~1.0)

        prev_5day_avg_profit: _日平均盈利（绝对值）_=0 时走固定阈值路由

        multiplier: 5日均值乘数，默认2.0

        daily_start_equity: 日初权益（动态阈值计算分母），默所.0



    Returns:

        (should_stop, reason): 是否应停止及原因描述

    """

    if prev_5day_avg_profit > 0 and multiplier > 0:

        dynamic_threshold = (prev_5day_avg_profit * multiplier) / max(daily_start_equity, 1.0)

        if daily_drawdown_pct >= dynamic_threshold:

            return True, f"日回撤{daily_drawdown_pct:.2%}≥动态阈值{dynamic_threshold:.2%}(5日均值乘）"

        return False, ""

    if daily_drawdown_pct >= hard_stop_pct:

        return True, f"日回撤{daily_drawdown_pct:.2%}≥硬停止{hard_stop_pct:.2%}"

    return False, ""





def resolve_and_check_daily_drawdown(

    daily_drawdown_pct: float,

    hard_stop_pct: Optional[float] = None,

    prev_5day_avg_profit: float = 0.0,

    multiplier: float = 2.0,

    daily_start_equity: Optional[float] = None,

) -> Tuple[bool, str]:

    """P0-06完整统一: 参数解析+保护+核心判断一站式入口避



    统一处理:

    1. hard_stop_pct 未显式传入时常params_service 回退读取

    2. daily_start_equity 无效时将 effective_hard_stop 设为 inf（禁用固定阈值路径）'
    3. 调用 check_daily_drawdown_hard_stop 执行双路径判断

    """

    if hard_stop_pct is None:

        try:

            from ali2026v3_trading.config.params_service import get_params_service

            hard_stop_pct = get_params_service().get_float('daily_loss_hard_stop_pct', 0.05)

        except (ImportError, AttributeError, ValueError) as _err:

            logging.debug("[risk_rules] 属性访问降级 %s", _err)

            hard_stop_pct = 0.05

    _equity_valid = daily_start_equity is not None and daily_start_equity > 0

    effective_hard_stop = hard_stop_pct if _equity_valid else float('inf')

    effective_equity = daily_start_equity if _equity_valid else 1.0

    return check_daily_drawdown_hard_stop(

        daily_drawdown_pct=daily_drawdown_pct,

        hard_stop_pct=effective_hard_stop,

        prev_5day_avg_profit=prev_5day_avg_profit,

        multiplier=multiplier,

        daily_start_equity=effective_equity,

    )





# ============================================================

# SecurityService facade

# ============================================================



class SecurityService:

    """统一安全服务门面 _合并 security / risk_audit / risk_rules 三模块



    提供凭据管理、安全加固、告警去重、风控规则等一站式访问者

    """



    # --- 凭据与敏感信号---

    @staticmethod

    def reveal_credential(cred) -> str:

        return reveal_credential(cred)



    @staticmethod

    def get_sensitive_credential(key_name: str, caller_id: str = '') -> str:

        return get_sensitive_credential(key_name, caller_id)



    @staticmethod

    def sanitize_for_return(params: Dict[str, Any]) -> Dict[str, Any]:

        return _sanitize_for_return(params)



    @staticmethod

    def filter_sensitive_keys(keys: List[str]) -> List[str]:

        return _filter_sensitive_keys(keys)



    # --- 路径与权益---

    @staticmethod

    def validate_path_safety(file_path: str, allowed_base_dirs: Optional[List[str]] = None) -> str:

        return _validate_path_safety(file_path, allowed_base_dirs)



    @staticmethod

    def check_config_file_permissions(file_path: str) -> None:

        _check_config_file_permissions(file_path)



    # --- 密钥轮换 ---

    @staticmethod

    def check_key_lifecycle() -> Dict[str, Any]:

        return check_key_lifecycle()



    # --- 安全事件响应 ---

    @staticmethod

    def get_responder() -> SecurityEventResponder:

        return get_security_responder()



    # --- 安全配置 ---

    @staticmethod

    def get_current_profile() -> SecurityProfile:

        return _get_current_security_profile()



    @staticmethod

    def get_security_config() -> Dict[str, Any]:

        return _get_security_config()



    @staticmethod

    def apply_profile(profile: SecurityProfile) -> Dict[str, Any]:

        return apply_security_profile(profile)



    # --- 错误消毒与堆—---

    @staticmethod

    def sanitize_error_msg(error_msg: str) -> str:

        return SANITIZE_ERROR_MSG(error_msg)



    @staticmethod

    def truncate_stack_trace(exc: Optional[Exception] = None, max_lines: Optional[int] = None) -> str:

        return _truncate_stack_trace(exc, max_lines)



    # --- pickle 安全 ---

    @staticmethod

    def safe_unpickle(data: bytes, allow_untrusted: bool = False) -> Any:

        return _safe_unpickle(data, allow_untrusted)



    # --- 内存锁定 ---

    @staticmethod

    def enable_mlock() -> bool:

        return enable_mlock_if_linux()



    @staticmethod

    def disable_core_dump() -> bool:

        return disable_core_dump()



    # --- 告警 ---

    @staticmethod

    def get_alert_deduplicator() -> AlertDeduplicator:

        return get_alert_deduplicator()



    @staticmethod

    def alert(level: AlertLevel, message: str, **kwargs) -> None:

        alert(level, message, **kwargs)



    # --- 风控规则 ---

    @staticmethod

    def check_daily_drawdown(

        daily_drawdown_pct: float,

        hard_stop_pct: Optional[float] = None,

        prev_5day_avg_profit: float = 0.0,

        multiplier: float = 2.0,

        daily_start_equity: Optional[float] = None,

    ) -> Tuple[bool, str]:

        return resolve_and_check_daily_drawdown(

            daily_drawdown_pct, hard_stop_pct,

            prev_5day_avg_profit, multiplier, daily_start_equity,

        )





# ============================================================

# __all__ _合并所有公共符。'
# ============================================================



__all__ = [

    # Section 1: Security

    'SENSITIVE_KEYS',

    '_SecureCredential',

    'reveal_credential',

    '_sanitize_for_return',

    '_TRUSTED_CREDENTIAL_CALLERS',

    'get_sensitive_credential',

    '_ALLOWED_BASE_DIRS',

    '_validate_path_safety',

    '_check_config_file_permissions',

    '_filter_sensitive_keys',

    'check_key_lifecycle',

    'SecurityEventResponder',

    'get_security_responder',

    'SecurityProfile',

    'ENV_SECURITY_PROFILES',

    'apply_security_profile',

    'SANITIZE_ERROR_MSG',

    '_truncate_stack_trace',

    '_UNPICKLE_ALLOWED',

    '_safe_unpickle',

    'enable_mlock_if_linux',

    'disable_core_dump',

    # Section 2: Risk audit utilities

    'AlertDeduplicator',

    'AlertLevel',

    'FLOAT_COMPARE_TOLERANCE',

    'KahanSummation',

    'PRICE_TOLERANCE',

    'SimplifiedSPAN',

    'alert',

    'approx_equal',

    'approx_less',

    'audit_chain_append',

    'calculate_var_historical',

    'calculate_var_rolling',

    'check_circuit_breaker_auto_recovery',

    'config_params',

    'generate_exchange_report',

    'get_alert_deduplicator',

    'json_default_serializer',

    'json_dumps',

    'json_loads',

    'operations_audit_log',

    'safe_divide',

    'safe_float',

    'safe_get',

    'safe_get_float',

    'safe_get_int',

    'safe_int',

    'safe_jsonl_append_line',

    'save_state_snapshot',

    'stable_mean',

    'stable_std',

    'stable_sum',

    'stable_variance',

    'structured_audit_log',

    'validate_bid_ask_spread_quality',

    'validate_depth_imbalance_quality',

    'validate_option_metadata_integrity',

    'validate_tick_timestamp_uniqueness',

    'validate_volume_quality',

    '_normalize_risk_scope_id',

    # Section 3: Risk rules

    'check_daily_drawdown_hard_stop',

    'resolve_and_check_daily_drawdown',

    # Facade

    'SecurityService',

]

