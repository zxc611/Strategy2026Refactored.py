# MODULE_ID: M1-141
"""
_bridge_service.py - 桥接服务核心

拆分自 order_flow_bridge.py (2026-06-30)
职责：tick/depth数据输入，查询接口封装

委托给原order_flow_bridge.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = ['OrderFlowBridge', 'get_order_flow_bridge']


# 委托导入
try:
    from ali2026v3_trading.order.order_flow_bridge import (
        OrderFlowBridge as _OrderFlowBridge,
    )
    
    OrderFlowBridge = _OrderFlowBridge
    
    # 单例获取函数
    _instance = None
    _lock = None
    
    def get_order_flow_bridge(config=None):
        """获取OrderFlowBridge单例"""
        global _instance, _lock
        if _lock is None:
            import threading
            _lock = threading.Lock()
        
        with _lock:
            if _instance is None:
                _instance = OrderFlowBridge(config)
            return _instance
    
except ImportError as e:
    import logging
    logging.warning("[_bridge_service] 委托导入失败: %s", e)
    
    # 提供空实现以避免导入错误
    class OrderFlowBridge:
        def __init__(self, config=None):
            pass
    
    def get_order_flow_bridge(config=None):
        return OrderFlowBridge(config)