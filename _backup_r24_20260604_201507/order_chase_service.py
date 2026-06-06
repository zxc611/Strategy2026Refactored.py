"""
order_chase_service.py - OrderChaseService
Phase 2 (CC-P1-01): 从OrderService提取的撤单/追单职责域

职责：
- 撤单 (cancel_order, cancel_all_pending)
- 紧急平仓 (emergency_close_all_positions)
- 追单 (_chase_reorder)
- 拆单 (_plan_volume_split)

Provider接口：通过provider（OrderService实例）获取共享属性
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.shared_utils import CHINA_TZ, TradeAction, TradeDirection


class OrderChaseService:
    """撤单/追单服务 — 从OrderService提取的独立可测试组件"""

    def __init__(self, provider=None):
        self._provider = provider

    def cancel_order(self, order_id: str) -> bool:
        svc = self._provider
        try:
            with svc._lock:
                order = svc._orders_by_id.get(order_id)
                if not order:
                    logging.warning("[OrderChaseService] Order not found: %s", order_id)
                    return False
                if order['status'] in ('FILLED', 'CANCELLED', 'FAILED'):
                    return False
                platform_id = order.get('platform_order_id', order_id)
            if svc._platform_cancel_order and callable(svc._platform_cancel_order):
                try:
                    svc._invoke_platform_cancel_with_timeout(platform_id)
                    logging.info("[OrderChaseService] 平台撤单成功: %s", order_id)
                    with svc._lock:
                        svc._cancel_count_window.append(time.time())
                except Exception as e:
                    logging.error("[OrderChaseService] 平台撤单失败: %s %s", order_id, e)
                    return False
            with svc._lock:
                order['status'] = 'CANCELLED'
                order['updated_at'] = datetime.now(CHINA_TZ)
                svc._stats['cancelled_orders'] += 1
            return True
        except Exception as e:
            logging.error("[OrderChaseService] Cancel order error: %s", e)
            return False

    def cancel_all_pending(self) -> int:
        svc = self._provider
        cancelled = 0
        try:
            with svc._lock:
                pending_ids = [
                    oid for oid, o in svc._orders_by_id.items()
                    if o.get('status') in ('PENDING', 'SUBMITTED', 'PARTIAL_FILL')
                ]
            for oid in pending_ids:
                if self.cancel_order(oid):
                    cancelled += 1
                else:
                    logging.warning("[R27-P0-RC-04] cancel_all_pending: 撤单失败 %s", oid)
            if cancelled > 0:
                logging.info("[R27-P0-RC-04] cancel_all_pending: 撤销%d/%d笔挂单", cancelled, len(pending_ids))
        except Exception as e:
            logging.error("[R27-P0-RC-04] cancel_all_pending异常: %s", e)
        return cancelled

    def emergency_close_all_positions(self, caller_id: str = "unknown") -> Dict[str, Any]:
        svc = self._provider
        result: Dict[str, Any] = {'cancelled': [], 'closed': [], 'errors': []}
        try:
            with svc._lock:
                pending_order_ids = [
                    oid for oid, o in svc._orders_by_id.items()
                    if o.get('status') in ('PENDING', 'SUBMITTED', 'PARTIAL_FILL')
                ]
            for oid in pending_order_ids:
                if self.cancel_order(oid):
                    result['cancelled'].append(oid)
                else:
                    result['errors'].append(f"cancel_failed:{oid}")
            with svc._lock:
                open_positions = {
                    oid: dict(o) for oid, o in svc._orders_by_id.items()
                    if o.get('status') == 'FILLED' and o.get('action') == TradeAction.OPEN
                }
            for oid, pos in open_positions.items():
                instrument_id = pos.get('instrument_id', '')
                direction = TradeDirection.SELL if pos.get('direction') == TradeDirection.BUY else TradeDirection.BUY
                volume = pos.get('volume', 0)
                price = pos.get('price', 0)
                if volume > 0 and price > 0:
                    close_result = svc.send_order(
                        instrument_id=instrument_id,
                        exchange=pos.get('exchange', ''),
                        direction=direction,
                        price=price,
                        volume=volume,
                        action='CLOSE',
                        signal_id=f'emergency_{caller_id}',
                        open_reason=f'emergency_close_by_{caller_id}',
                    )
                    if close_result.ok:
                        result['closed'].append(oid)
                    else:
                        result['errors'].append(f"close_failed:{oid}:{close_result.error_code}")
            logging.critical("[R22-P0-EC-01] emergency_close_all_positions: caller=%s cancelled=%d closed=%d errors=%d",
                             caller_id, len(result['cancelled']), len(result['closed']), len(result['errors']))
        except Exception as e:
            logging.error("[R22-P0-EC-01] emergency_close_all_positions error: %s", e)
            result['errors'].append(str(e))
        return result

    def _chase_reorder(self, original_order: Dict, retry_count: int) -> None:
        svc = self._provider
        _orig_id = original_order.get('order_id', '')
        with svc._lock:
            _existing = svc._orders_by_id.get(_orig_id)
            if _existing and _existing.get('status') in ('SUBMITTED', 'ACCEPTED', 'PARTIAL_FILLED'):
                logging.info("[R23-ID-03-FIX] chase重试跳过: 订单%s仍处于活跃状态%s",
                           _orig_id, _existing.get('status'))
                return
        _filled_volume = original_order.get('filled_volume', 0)
        _original_volume = original_order.get('volume', 0)
        if _filled_volume > 0:
            _remaining_volume = _original_volume - _filled_volume
            if _remaining_volume <= 0:
                logging.info("[P1-R9-26] 原订单已完全成交, 无需追单: order=%s filled=%d",
                           original_order.get('order_id', ''), _filled_volume)
                return
            logging.info("[P1-R9-26] 原订单部分成交, 追单剩余量: order=%s original=%d filled=%d remaining=%d",
                        original_order.get('order_id', ''), _original_volume, _filled_volume, _remaining_volume)
        else:
            _remaining_volume = _original_volume
        _order_id = original_order.get('order_id', '')
        _backoff_delay = min(svc._retry_backoff_base * (2 ** (retry_count - 1)), svc._retry_backoff_max)
        _last_retry = svc._last_retry_time.get(_order_id, 0.0)
        _elapsed_since_last = time.time() - _last_retry
        if _elapsed_since_last < _backoff_delay:
            logging.debug("[ID-P1-08-FIX] 重试退避中: order=%s retry=%d wait=%.1fs", _order_id, retry_count, _backoff_delay - _elapsed_since_last)
            return
        svc._last_retry_time[_order_id] = time.time()
        instrument_id = original_order['instrument_id']
        tick_size = svc._get_tick_size(instrument_id)
        chase_ticks = min(retry_count, 3)
        price_offset = tick_size * chase_ticks
        max_chase_ticks = 5
        if chase_ticks > max_chase_ticks:
            logging.warning("[OrderChaseService._chase_reorder] Chase ticks %d exceeds max %d, capping", chase_ticks, max_chase_ticks)
            chase_ticks = max_chase_ticks
            price_offset = tick_size * chase_ticks
        if original_order['direction'] == TradeDirection.BUY:
            new_price = original_order['price'] + price_offset
        else:
            new_price = original_order['price'] - price_offset
        new_order_id = svc.send_order(
            instrument_id=instrument_id,
            volume=_remaining_volume,
            price=new_price,
            direction=original_order['direction'],
            action=original_order['action'],
            exchange=original_order.get('exchange', ''),
            is_chase=True,
        )
        if new_order_id and new_order_id.order_id:
            _oid = new_order_id.order_id
            with svc._lock:
                svc._orders_by_id[_oid]['retry_count'] = retry_count
                svc._orders_by_id[_oid]['original_order_id'] = original_order['order_id']
                chase_prefixed_id = svc._generate_chase_order_id(original_order['order_id'])
                svc._orders_by_id[_oid]['chase_order_id'] = chase_prefixed_id
            logging.info("[OrderChaseService] 追单: %s -> %s retry=%d price=%.2f->%.2f",
                         original_order['order_id'], _oid, retry_count, original_order['price'], new_price)
        else:
            logging.warning("[OrderChaseService] 追单失败: %s retry=%d error=%s", instrument_id, retry_count, new_order_id.error_code if new_order_id else 'unknown')

    def _plan_volume_split(self, volume, price, direction, bids, asks, signal_strength):
        svc = self._provider
        split_threshold = getattr(svc, '_split_volume_threshold', 5)
        if volume <= split_threshold or not (bids or asks):
            return None
        import math
        n_splits = min(max(2, math.ceil(volume / split_threshold)), 10)
        single_vol = volume / n_splits
        splits = []
        for i in range(n_splits):
            splits.append({'volume': single_vol, 'price': price, 'index': i})
        return splits