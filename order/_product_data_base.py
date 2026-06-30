# MODULE_ID: M1-144
"""
_product_data_base.py - 品种微观数据基类

拆分自 order_flow_analyzer.py (2026-06-30)
职责：ProductMicroData基础数据结构和更新方法

委托给原order_flow_analyzer.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = ['ProductMicroDataBase']


# 委托导入
try:
    from ali2026v3_trading.order.order_flow_analyzer import ProductMicroData as _ProductMicroData
    
    class ProductMicroDataBase:
        """品种微观数据基类（委托给ProductMicroData）"""
        
        def __init__(self, product: str, config):
            self._impl = _ProductMicroData(product, config)
        
        # 委托所有方法
        def __getattr__(self, name):
            return getattr(self._impl, name)
    
except ImportError as e:
    import logging
    logging.warning("[_product_data_base] 委托导入失败: %s", e)
    
    # 提供空实现
    class ProductMicroDataBase:
        def __init__(self, product: str, config):
            pass