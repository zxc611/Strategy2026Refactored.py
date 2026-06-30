# MODULE_ID: M1-140
"""
_arbitrage_defense.py - 套利检测与防御引擎

拆分自 order_flow_bridge.py (2026-06-30)
职责：微观套利检测、跨合约套利、扫单检测、做市商防御

委托给原order_flow_bridge.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = [
    'ArbitrageOpportunity',
    'MicrostructureArbitrageDetector',
    'CrossContractArbitrageDetector',
    'OrderDefenseType',
    'SweepDetector',
    'DefensiveOrder',
    'MarketMakerDefenseEngine',
]


# 委托导入
try:
    from ali2026v3_trading.order.order_flow_bridge import (
        ArbitrageOpportunity as _ArbitrageOpportunity,
        MicrostructureArbitrageDetector as _MicrostructureArbitrageDetector,
        CrossContractArbitrageDetector as _CrossContractArbitrageDetector,
        OrderDefenseType as _OrderDefenseType,
        SweepDetector as _SweepDetector,
        DefensiveOrder as _DefensiveOrder,
        MarketMakerDefenseEngine as _MarketMakerDefenseEngine,
    )
    
    ArbitrageOpportunity = _ArbitrageOpportunity
    MicrostructureArbitrageDetector = _MicrostructureArbitrageDetector
    CrossContractArbitrageDetector = _CrossContractArbitrageDetector
    OrderDefenseType = _OrderDefenseType
    SweepDetector = _SweepDetector
    DefensiveOrder = _DefensiveOrder
    MarketMakerDefenseEngine = _MarketMakerDefenseEngine
    
except ImportError as e:
    import logging
    logging.warning("[_arbitrage_defense] 委托导入失败: %s", e)
    
    # 提供空实现以避免导入错误
    from dataclasses import dataclass, field
    from enum import Enum, auto
    from typing import Any, Dict, List, Optional
    
    @dataclass
    class ArbitrageOpportunity:
        instrument_id: str = ''
        fair_value: float = 0.0
        market_price: float = 0.0
        spread: float = 0.0
        direction: str = ''
        confidence: float = 0.0
        timestamp: float = 0.0
        expiry_seconds: float = 5.0
    
    class MicrostructureArbitrageDetector:
        def __init__(self):
            pass
    
    class CrossContractArbitrageDetector:
        def __init__(self):
            pass
    
    class OrderDefenseType(Enum):
        IOC = auto()
        HIDDEN_WITH_OFFSET = auto()
        STANDARD = auto()
        POST_ONLY = auto()
    
    class SweepDetector:
        def __init__(self):
            pass
    
    @dataclass
    class DefensiveOrder:
        instrument_id: str = ''
        price: float = 0.0
        volume: int = 0
        direction: str = ''
        defense_type: Any = None
        parent_order_id: str = ''
        created_at: float = 0.0
        expires_at: float = 0.0
        metadata: Dict[str, Any] = field(default_factory=dict)
    
    class MarketMakerDefenseEngine:
        def __init__(self):
            pass