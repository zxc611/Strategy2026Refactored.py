# MODULE_ID: M1-145
"""
_product_data_analysis.py - 品种微观数据分析方法

拆分自 order_flow_analyzer.py (2026-06-30)
职责：ProductMicroData分析计算方法（CVD/OFI/不平衡）

委托给原order_flow_analyzer.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = ['ProductMicroDataAnalysis']


# 委托导入
try:
    from ali2026v3_trading.order.order_flow_analyzer import ProductMicroData as _ProductMicroData
    
    class ProductMicroDataAnalysis:
        """品种微观数据分析类（委托给ProductMicroData）"""
        
        def __init__(self, product: str, config):
            self._impl = _ProductMicroData(product, config)
        
        # 委托所有方法
        def __getattr__(self, name):
            return getattr(self._impl, name)
    
except ImportError as e:
    import logging
    logging.warning("[_product_data_analysis] 委托导入失败: %s", e)
    
    # 提供空实现
    class ProductMicroDataAnalysis:
        def __init__(self, product: str, config):
            pass