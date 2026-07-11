# [M1-46] 订单执行器 - Mixin组合门面

from ali2026v3_trading.order.order_executor_validation import _OrderExecutorBase, OrderContext
from ali2026v3_trading.order.order_executor_platform import _OrderExecutorPlatformMethods


class OrderExecutor(_OrderExecutorPlatformMethods, _OrderExecutorBase):
    """订单执行器 - Mixin组合: 平台交互 + 基础校验"""
    pass


__all__ = ['OrderExecutor', 'OrderContext']
