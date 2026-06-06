"""shared_utils_contracts — 契约编程相关 (R27-CP-04-FIX: 从shared_utils拆分)

前置条件验证、后置条件验证、超时控制、初始化阶段装饰器
"""

import logging
import threading
from typing import Any

from ali2026v3_trading.infra.shared_utils_infra import InitPhase

__all__ = [
    'requires_phase', 'require_precondition', 'ensure_postcondition',
    'PreconditionError', 'PostconditionError', 'OperationTimeoutError',
    'with_timeout',
]


class PreconditionError(Exception):
    """OPS-P1-06修复: 前置条件验证失败异常"""
    pass


class PostconditionError(Exception):
    """OPS-P1-07修复: 后置条件验证失败异常"""
    pass


class OperationTimeoutError(Exception):
    """OPS-P1-08修复: 操作超时异常"""
    pass


def require_precondition(condition: bool, message: str = "") -> None:
    """OPS-P1-06修复: 前置条件验证

    在执行关键操作前验证前置条件，不满足时抛出PreconditionError。

    Args:
        condition: 前置条件
        message: 失败时的错误信息

    Raises:
        PreconditionError: 前置条件不满足
    """
    if not condition:
        raise PreconditionError(f"前置条件验证失败: {message}")


def ensure_postcondition(condition: bool, message: str = "") -> None:
    """OPS-P1-07修复: 后置条件验证

    在关键操作执行后验证后置条件，不满足时抛出PostconditionError。

    Args:
        condition: 后置条件
        message: 失败时的错误信息

    Raises:
        PostconditionError: 后置条件不满足
    """
    if not condition:
        raise PostconditionError(f"后置条件验证失败: {message}")


def with_timeout(func, timeout_sec: float = 30.0, logger=None):
    """OPS-P1-08修复: 带超时控制的操作执行

    使用线程执行函数，超时后抛出OperationTimeoutError。

    Args:
        func: 要执行的函数（无参数）
        timeout_sec: 超时秒数
        logger: 可选的logger实例

    Returns:
        func()的返回值

    Raises:
        OperationTimeoutError: 操作超时
    """
    import concurrent.futures
    result_holder = [None]
    exception_holder = [None]

    def _target():
        try:
            result_holder[0] = func()
        except Exception as e:
            exception_holder[0] = e

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_target)
        try:
            future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            if logger:
                logger.warning("OPS-P1-08: 操作超时(%.1fs): %s",
                               timeout_sec, getattr(func, '__name__', 'func'))
            raise OperationTimeoutError(
                f"操作超时({timeout_sec:.1fs}): {getattr(func, '__name__', 'func')}"
            )

    if exception_holder[0] is not None:
        raise exception_holder[0]
    return result_holder[0]


def requires_phase(required: InitPhase):
    def decorator(method):
        def wrapper(self, *args, **kwargs):
            if hasattr(self, '_init_state'):
                self._init_state.check_phase(required)
            return method(self, *args, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        wrapper.__wrapped__ = method
        return wrapper
    return decorator
