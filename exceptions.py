"""R10-P2-01: 全系统自定义异常类层次"""

__all__ = [
    'TradingSystemError', 'RiskCheckError', 'OrderExecutionError',
    'StateTransitionError', 'ConfigurationError', 'DataIntegrityError',
    'ParameterValidationError',
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
