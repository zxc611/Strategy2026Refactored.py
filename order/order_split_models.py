"""
order_split_models.py - 拆单策略模型
R27: 从order_service.py提取的拆单策略相关类

职责：
- 拆单策略常量 (OrderSplitStrategy)
- 拆单结果数据类 (SplitOrderResult)
- 智能拆单器 (SmartOrderSplitter)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class OrderSplitStrategy:
    VOLUME_BASED = 'volume'
    LOB_DEPTH = 'lob_depth'
    ICEBERG = 'iceberg'
    TWAP = 'twap'


@dataclass(slots=True)
class SplitOrderResult:
    success: bool
    order_ids: List[str]
    split_count: int = 0
    total_volume: float = 0.0
    error_message: str = ''

    @staticmethod
    def ok(order_ids: List[str], split_count: int, total_volume: float) -> 'SplitOrderResult':
        return SplitOrderResult(success=True, order_ids=order_ids, split_count=split_count, total_volume=total_volume)

    @staticmethod
    def fail(error_message: str) -> 'SplitOrderResult':
        return SplitOrderResult(success=False, order_ids=[], error_message=error_message)


class SmartOrderSplitter:
    def __init__(self, order_service: Optional['OrderService'] = None, strategy: str = OrderSplitStrategy.LOB_DEPTH, split_threshold: int = 5, max_depth_levels: int = 5, aggressive_signal_threshold: float = 0.8, passive_signal_threshold: float = 0.6):
        self._order_service = order_service
        self._strategy = strategy
        self._split_threshold = split_threshold
        self._max_depth_levels = max_depth_levels
        self._aggressive_signal_threshold = aggressive_signal_threshold
        self._passive_signal_threshold = passive_signal_threshold

    def split_and_send(
        self,
        instrument_id: str,
        volume: float,
        price: float,
        direction: str = 'BUY',
        action: str = 'OPEN',
        exchange: str = '',
        signal_strength: float = 1.0,
        bids: Optional[List] = None,
        asks: Optional[List] = None,
        signal_id: str = '',
        open_reason: str = '',
    ) -> SplitOrderResult:
        if self._order_service is None:
            return SplitOrderResult.fail('order_service not set')
        try:
            order_ids = self._order_service.send_order_split(
                instrument_id=instrument_id,
                volume=volume,
                price=price,
                direction=direction,
                action=action,
                exchange=exchange,
                signal_strength=signal_strength,
                bids=bids,
                asks=asks,
                signal_id=signal_id,
                open_reason=open_reason,
            )
            if order_ids:
                return SplitOrderResult.ok(order_ids, split_count=len(order_ids), total_volume=volume)
            return SplitOrderResult.fail('send_order_split returned empty')
        except Exception as e:
            logging.error("[SmartOrderSplitter] split_and_send异常: %s", e)
            return SplitOrderResult.fail(str(e))
