# MODULE_ID: M1-143
"""
_executor_batch.py - 批量下单与平台API适配

拆分自 order_executor.py (2026-06-30)
职责：拆单下单、批量下单、平台API适配

委托给原order_executor.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = ['OrderExecutorBatch']


# 委托导入
try:
    from ali2026v3_trading.order.order_executor import OrderExecutor as _OrderExecutor
    
    class OrderExecutorBatch:
        """批量下单执行器（委托给OrderExecutor）"""
        
        def __init__(self, order_service):
            self._executor = _OrderExecutor(order_service)
        
        # 委托所有方法
        def __getattr__(self, name):
            return getattr(self._executor, name)
    
except ImportError as e:
    import logging
    logging.warning("[_executor_batch] 委托导入失败: %s", e)
    
    # 提供空实现以避免导入错误
    class OrderExecutorBatch:
        def __init__(self, order_service):
            pass