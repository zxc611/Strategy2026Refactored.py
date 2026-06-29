"""R3-1: 异常分层策略模块

定义L0-L3四级异常分层策略，用于治理全系统352处`except Exception`软集成问题。

分层策略:
    L0 致命: 核心门控失败必须硬阻断，不允许静默吞没
        - 适用: CascadeJudge、风控检查、交易执行
        - 捕获: (ImportError, AttributeError, TypeError)
        - 处理: raise RuntimeError 硬阻断

    L1 严重: 评判/验证组件失败必须标记结果无效
        - 适用: 评判组件、验证器、checker
        - 捕获: (ValueError, KeyError, RuntimeError)
        - 处理: 记录error + 标记结果无效

    L2 可降级: 辅助指标失败可降级到默认值
        - 适用: 辅助指标、非关键评分、监控采集
        - 捕获: (ValueError, KeyError, TypeError, AttributeError, ImportError)
        - 处理: 记录warning + fallback默认值

    L3 可忽略: 非功能性采集失败可跳过
        - 适用: 日志、遥测、非功能性采集
        - 捕获: Exception (保留宽捕获)
        - 处理: 记录debug + 跳过
"""

__all__ = [
    'L0_FATAL_EXCEPTIONS', 'L1_SEVERE_EXCEPTIONS', 'L2_DEGRADABLE_EXCEPTIONS',
    'L0_FATAL_MSG', 'L1_SEVERE_MSG', 'L2_DEGRADABLE_MSG',
    'FatalGateError', 'SevereCheckError',
]

# ===== L0 致命: 核心门控硬阻断 =====
L0_FATAL_EXCEPTIONS = (ImportError, AttributeError, TypeError)
L0_FATAL_MSG = "[FATAL] 核心门控失败(硬阻断): {}"


class FatalGateError(RuntimeError):
    """L0级致命错误 - 核心门控失败时抛出，替代静默吞没"""
    pass


# ===== L1 严重: 评判/验证组件标记无效 =====
L1_SEVERE_EXCEPTIONS = (ValueError, KeyError, RuntimeError)
L1_SEVERE_MSG = "[SEVERE] 评判/验证组件异常(标记无效): {}"


class SevereCheckError(RuntimeError):
    """L1级严重错误 - 评判/验证组件失败时标记结果无效"""
    pass


# ===== L2 可降级: 辅助指标降级到默认值 =====
L2_DEGRADABLE_EXCEPTIONS = (ValueError, KeyError, TypeError, AttributeError, ImportError)
L2_DEGRADABLE_MSG = "[DEGRADE] 辅助指标降级: {}"


# ===== 辅助函数 =====

def raise_fatal(component: str, original_error: Exception) -> None:
    """L0级: 核心门控失败硬阻断

    Args:
        component: 组件名称，如"CascadeJudge"、"RiskService"
        original_error: 原始异常

    Raises:
        FatalGateError: 始终抛出
    """
    raise FatalGateError(f"[FATAL] {component}核心门控失败: {original_error}") from original_error


def mark_severe(component: str, original_error: Exception, warnings_list: list = None) -> str:
    """L1级: 评判/验证组件失败标记无效

    Args:
        component: 组件名称
        original_error: 原始异常
        warnings_list: 可选的warnings列表，会自动追加

    Returns:
        格式化的警告消息
    """
    msg = f"[SEVERE] {component}执行异常(标记无效): {original_error}"
    if warnings_list is not None:
        warnings_list.append(msg)
    return msg


def degrade(component: str, original_error: Exception, logger=None) -> str:
    """L2级: 辅助指标降级到默认值

    Args:
        component: 组件名称
        original_error: 原始异常
        logger: 可选的logger，会自动记录warning

    Returns:
        格式化的降级消息
    """
    msg = f"[DEGRADE] {component}降级: {original_error}"
    if logger is not None:
        logger.warning(msg)
    return msg
