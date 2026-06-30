# MODULE_ID: M1-139
"""
_microstructure_core.py - 核心微观结构分析

拆分自 order_flow_analyzer.py (2026-06-30)
职责：MicrostructureConfig/FootprintBar配置类 + MicrostructureAnalyzer分析器

注意：ProductMicroData已拆分为3个子模块（M1-144/145/146）
"""

from __future__ import annotations

__all__ = [
    'MicrostructureConfig',
    'FootprintBar',
    'MicrostructureAnalyzer',
]


# 委托导入
try:
    from ali2026v3_trading.order.order_flow_analyzer import (
        MicrostructureConfig as _MicrostructureConfig,
        FootprintBar as _FootprintBar,
        ProductMicroData as _ProductMicroData,
        MicrostructureAnalyzer as _MicrostructureAnalyzer,
    )
    
    MicrostructureConfig = _MicrostructureConfig
    FootprintBar = _FootprintBar
    ProductMicroData = _ProductMicroData
    MicrostructureAnalyzer = _MicrostructureAnalyzer
    
except ImportError as e:
    import logging
    logging.warning("[_microstructure_core] 委托导入失败: %s", e)
    
    # 提供空实现以避免导入错误
    from dataclasses import dataclass, field
    from typing import Any, Dict, Optional
    
    @dataclass
    class MicrostructureConfig:
        large_order_threshold: int = 50
        max_history_size: int = 10000
        price_precision: int = 2
        footprint_maxlen: int = 1000
        anchor_vwap_maxsize: int = 100
        cvd_lookback_seconds: int = 600
        imbalance_depth_levels: int = 5
        vwap_lookback_seconds: int = 60
    
    @dataclass
    class FootprintBar:
        timestamp: float = 0.0
        open: float = 0.0
        high: float = 0.0
        low: float = 0.0
        close: float = 0.0
        volume: float = 0.0
        buy_volume: float = 0.0
        sell_volume: float = 0.0
        imbalance: float = 0.0
    
    class ProductMicroData:
        def __init__(self, config=None):
            pass
    
    class MicrostructureAnalyzer:
        def __init__(self, config=None):
            pass