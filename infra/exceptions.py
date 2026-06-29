# [M1-73] 异常定义
# MODULE_ID: M1-090

"""R10-P2-01: 全系统自定义异常类层。

+ R3-1: 异常分层策略 (从exception_layers.py迁入)

"""



__all__ = [

    'TradingSystemError', 'RiskCheckError', 'OrderExecutionError',

    'StateTransitionError', 'ConfigurationError', 'DataIntegrityError',

    'ParameterValidationError', 'FutureLeakException',

    # From exception_layers.py

    'L0_FATAL_EXCEPTIONS', 'L1_SEVERE_EXCEPTIONS', 'L2_DEGRADABLE_EXCEPTIONS',

    'L0_FATAL_MSG', 'L1_SEVERE_MSG', 'L2_DEGRADABLE_MSG',

    'FatalGateError', 'SevereCheckError',

    'raise_fatal', 'mark_severe', 'degrade',

]





class TradingSystemError(Exception):

    """交易系统基础异常"""

    pass





class RiskCheckError(TradingSystemError):

    """风控检查异常"""

    pass





class OrderExecutionError(TradingSystemError):

    """订单执行异常"""

    pass





class StateTransitionError(TradingSystemError):

    """状态转换异常"""

    pass





class ConfigurationError(TradingSystemError):

    """配置异常"""

    pass





class DataIntegrityError(TradingSystemError):

    """数据完整性异常"""

    pass





class ParameterValidationError(TradingSystemError):

    """参数验证异常"""

    pass





class FutureLeakException(Exception):

    """未来数据泄露异常 - 回测模式下访问超出当前事件时间的数据时抛出后



    统一异常类，替代原有界FutureLeakException（param_pool）和

    FutureDataLeakException（strategy）两个语义相同但类名不同的异常流



    Attributes:

        access_time: 访问时间

        poison_time: 数据时间（被访问的未来时间）'
        stack_trace: 调用栈信号

    """

    def __init__(self, access_time=None, poison_time=None, stack_trace=None):

        self.access_time = access_time

        self.poison_time = poison_time

        self.stack_trace = stack_trace

        if access_time is not None and poison_time is not None:

            super().__init__(

                f"Future leak: accessed {poison_time} at {access_time}"

            )

        else:

            super().__init__(str(access_time) if access_time else "Future data leak detected")





# ============================================================

# 异常分层策略 (_exception_layers.py 迁入)

# ============================================================



# L0 致命: 核心门控硬阻断

L0_FATAL_EXCEPTIONS = (ImportError, AttributeError, TypeError)

L0_FATAL_MSG = "[FATAL] 核心门控失败(硬阻断): {}"





class FatalGateError(RuntimeError):

    """L0级致命错误- 核心门控失败时抛出，替代静默吞没"""

    pass





# L1 严重: 评判/验证组件标记无效

L1_SEVERE_EXCEPTIONS = (ValueError, KeyError, RuntimeError)

L1_SEVERE_MSG = "[SEVERE] 评判/验证组件异常(标记无效): {}"





class SevereCheckError(RuntimeError):

    """L1级严重错误- 评判/验证组件失败时标记结果无界"""

    pass





# L2 可降级 辅助指标降级到默认证

L2_DEGRADABLE_EXCEPTIONS = (ValueError, KeyError, TypeError, AttributeError, ImportError)

L2_DEGRADABLE_MSG = "[DEGRADE] 辅助指标降级: {}"





def raise_fatal(component: str, original_error: Exception) -> None:

    """L0_ 核心门控失败硬阻塞"""

    raise FatalGateError(f"[FATAL] {component}核心门控失败: {original_error}") from original_error





def mark_severe(component: str, original_error: Exception, warnings_list: list = None) -> str:

    """L1_ 评判/验证组件失败标记无效"""

    msg = f"[SEVERE] {component}执行异常(标记无效): {original_error}"

    if warnings_list is not None:

        warnings_list.append(msg)

    return msg





def degrade(component: str, original_error: Exception, logger=None) -> str:

    """L2_ 辅助指标降级到默认证"""

    msg = f"[DEGRADE] {component}降级: {original_error}"

    if logger is not None:

        logger.warning(msg)

    return msg

