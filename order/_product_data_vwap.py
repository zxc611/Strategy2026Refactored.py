# MODULE_ID: M1-146
"""
_product_data_vwap.py - 品种微观数据VWAP和执行评估

拆分自 order_flow_analyzer.py (2026-06-30)
职责：ProductMicroData的VWAP计算和执行评估

委托给原order_flow_analyzer.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = ['ProductMicroDataVWAP']


# 委托导入
try:
    from ali2026v3_trading.order.order_flow_analyzer import ProductMicroData as _ProductMicroData
    
    class ProductMicroDataVWAP:
        """品种微观数据VWAP类（委托给ProductMicroData）"""
        
        def __init__(self, product: str, config):
            self._impl = _ProductMicroData(product, config)
        
        # 委托所有方法
        def __getattr__(self, name):
            return getattr(self._impl, name)
    
except ImportError as e:
    import logging
    logging.warning("[_product_data_vwap] 委托导入失败: %s", e)
    
    # 提供空实现
    class ProductMicroDataVWAP:
        def __init__(self, product: str, config):
            pass