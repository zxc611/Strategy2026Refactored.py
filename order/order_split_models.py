# [M1-51] �𵥲���ģ��
# MODULE_ID: M1-142
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
order_split_models.py - 拆单策略模型
R27: 从order_service.py提取的拆单策略相关类

职责�?
- 拆单策略常量 (OrderSplitStrategy)
- 拆单结果数据�?(SplitOrderResult)
- 智能拆单�?(SmartOrderSplitter)
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
    AGGRESSIVE = 'aggressive'
    PASSIVE = 'passive'
    ADAPTIVE = 'adaptive'


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


@dataclass
class PlanOrderSplitResult:
    """plan_order_split()的返回类�?""
    child_orders: List[Dict[str, Any]]
    total_volume: float
    strategy_used: str


# [P2-04] 权威拆单实现，所有拆单逻辑应委托此�?
class SmartOrderSplitter:
    def __init__(self, order_service: Optional['OrderService'] = None, strategy: str = OrderSplitStrategy.LOB_DEPTH, split_threshold: int = 5, max_depth_levels: int = 5, aggressive_signal_threshold: float = 0.8, passive_signal_threshold: float = 0.6):
        self._order_service = order_service
        self._strategy = strategy
        self._split_threshold = split_threshold
        self._max_depth_levels = max_depth_levels
        self._aggressive_signal_threshold = aggressive_signal_threshold
        self._passive_signal_threshold = passive_signal_threshold

    def plan_order_split(
        self,
        instrument_id: str,
        volume: float,
        direction: str = 'BUY',
        signal_strength: float = 1.0,
        bids: Optional[List] = None,
        asks: Optional[List] = None,
        strategy: Optional[str] = None,
    ) -> PlanOrderSplitResult:
        """根据信号强度和盘口深度规划拆单方�?""
        _strategy = strategy or self._strategy
        child_orders = []

        if _strategy == OrderSplitStrategy.AGGRESSIVE:
            # 激进策略：吃掉盘口深度，按深度层级分配
            book = asks if direction == 'BUY' else bids
            if book:
                remaining = volume
                for i, (price, qty) in enumerate(book[:self._max_depth_levels]):
                    alloc = min(remaining, qty)
                    child_orders.append({
                        'instrument_id': instrument_id,
                        'direction': direction,
                        'volume': alloc,
                        'price': price,
                        'level': i,
                    })
                    remaining -= alloc
                    if remaining <= 0:
                        break
                if remaining > 0:
                    child_orders.append({
                        'instrument_id': instrument_id,
                        'direction': direction,
                        'volume': remaining,
                        'price': book[-1][0] if book else 0.0,
                        'level': len(child_orders),
                    })
            else:
                child_orders.append({
                    'instrument_id': instrument_id,
                    'direction': direction,
                    'volume': volume,
                    'price': 0.0,
                    'level': 0,
                })
        elif _strategy == OrderSplitStrategy.PASSIVE:
            # 被动策略：挂在最优价
            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0
            price = best_bid if direction == 'BUY' else best_ask
            child_orders.append({
                'instrument_id': instrument_id,
                'direction': direction,
                'volume': volume,
                'price': price,
                'level': 0,
            })
        elif _strategy == OrderSplitStrategy.ADAPTIVE:
            # 自适应策略：根据信号强度选择激�?被动混合
            if signal_strength >= self._aggressive_signal_threshold:
                return self.plan_order_split(instrument_id, volume, direction, signal_strength, bids, asks, OrderSplitStrategy.AGGRESSIVE)
            elif signal_strength <= self._passive_signal_threshold:
                return self.plan_order_split(instrument_id, volume, direction, signal_strength, bids, asks, OrderSplitStrategy.PASSIVE)
            else:
                # 混合：部分激进部分被�?
                aggressive_ratio = (signal_strength - self._passive_signal_threshold) / max(self._aggressive_signal_threshold - self._passive_signal_threshold, 0.01)
                aggressive_vol = volume * aggressive_ratio
                passive_vol = volume - aggressive_vol
                if aggressive_vol > 0:
                    agg_result = self.plan_order_split(instrument_id, aggressive_vol, direction, signal_strength, bids, asks, OrderSplitStrategy.AGGRESSIVE)
                    child_orders.extend(agg_result.child_orders)
                if passive_vol > 0:
                    pas_result = self.plan_order_split(instrument_id, passive_vol, direction, signal_strength, bids, asks, OrderSplitStrategy.PASSIVE)
                    child_orders.extend(pas_result.child_orders)
        else:
            # 默认：简单均�?
            num_splits = max(1, min(self._split_threshold, int(volume)))
            per_split = volume / num_splits
            for i in range(num_splits):
                child_orders.append({
                    'instrument_id': instrument_id,
                    'direction': direction,
                    'volume': per_split,
                    'price': 0.0,
                    'level': i,
                })

        return PlanOrderSplitResult(
            child_orders=child_orders,
            total_volume=volume,
            strategy_used=_strategy,
        )

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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[SmartOrderSplitter] split_and_send异常: %s", e)
            return SplitOrderResult.fail(str(e))
