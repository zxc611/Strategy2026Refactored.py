"""
订单服务模块 - CQRS 架构 Command 层
来源：09_executor.py + 10_gate.py (部分)
功能：订单执行 + 平台认证 + 限流控制 + 撤单追单
"""
from __future__ import annotations

import re
import threading
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from collections import deque

from ali2026v3_trading.event_bus import RateLimiter


class BaseService:
    def __init__(self, event_bus=None):
        self.event_bus = event_bus


class PlatformAuthenticator:
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._lock = threading.Lock()

    def get_token(self) -> Optional[str]:
        with self._lock:
            if self._token and time.time() < self._token_expiry:
                return self._token
            return None

    def set_token(self, token: str, expires_in: float = 3600.0) -> None:
        with self._lock:
            self._token = token
            self._token_expiry = time.time() + expires_in


_order_service_instance: Optional['OrderService'] = None
_order_service_lock = threading.Lock()


def get_order_service() -> 'OrderService':
    global _order_service_instance
    if _order_service_instance is None:
        with _order_service_lock:
            if _order_service_instance is None:
                _order_service_instance = OrderService()
    return _order_service_instance


class OrderService(BaseService):
    def __init__(self, event_bus=None):
        super().__init__(event_bus)
        self.authenticator = PlatformAuthenticator()
        rate_per_min = self._get_rate_limit()
        self.rate_limiter = RateLimiter(rate=max(rate_per_min / 60.0, 1.0))
        self._orders_by_id: Dict[str, Dict] = {}
        self._recent_orders_by_instrument: Dict[str, List[str]] = {}
        self._chase_tasks: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self._platform_insert_order = None
        self._platform_cancel_order = None
        self._order_timeout_seconds = 5.0
        self._max_chase_retries = 3
        self._stats = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'cancelled_orders': 0,
            'chase_tasks_active': 0,
        }
        # 定期清理已完成订单防止内存泄漏
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5分钟

    def bind_platform_apis(self, insert_order_func, cancel_order_func):
        self._platform_insert_order = insert_order_func
        self._platform_cancel_order = cancel_order_func
        logging.info("[OrderService] 平台下单/撤单API已绑定")

    _PLATFORM_ATTR_MAP = {
        'order_id': 'OrderRef',
        'status': 'OrderStatus',
        'filled_volume': 'VolumeTraded',
    }

    @staticmethod
    def _get_platform_attr(obj: Any, attr_new: str, attr_old: str, default: Any = None) -> Any:
        val = getattr(obj, attr_new, None)
        if val is not None:
            return val
        val = getattr(obj, attr_old, None)
        if val is not None:
            return val
        return default

    def _normalize_platform_result(self, result: Any) -> Dict[str, Any]:
        normalized = {}
        for new_attr, old_attr in self._PLATFORM_ATTR_MAP.items():
            normalized[new_attr] = self._get_platform_attr(result, new_attr, old_attr)
        return normalized

    def send_order(
        self,
        instrument_id: str,
        volume: float,
        price: float,
        direction: str = 'BUY',
        action: str = 'OPEN',
        exchange: str = '',
        priority: str = 'NORMAL',
    ) -> Optional[str]:
        # ✅ #87修复：紧急平仓(action='CLOSE')跳过限流和重复检查
        if action != 'CLOSE':
            if not self.rate_limiter.acquire():
                logging.warning("[OrderService] Order rate limited: %s", instrument_id)
                return None
            order_key = f"{instrument_id}_{direction}_{action}_{volume}_{round(price, 4)}"
            if self._is_duplicate_order(instrument_id, order_key):
                logging.warning("[OrderService] Duplicate order: %s", order_key)
                return None
        try:
            order_id = self._generate_order_id()
            order = {
                'order_id': order_id,
                'instrument_id': instrument_id,
                'exchange': exchange,
                'volume': volume,
                'price': price,
                'direction': direction,
                'action': action,
                'status': 'SUBMITTED',
                'filled_volume': 0,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
            }
            with self._lock:
                self._orders_by_id[order_id] = order
                self._stats['total_orders'] += 1
            if self._platform_insert_order and callable(self._platform_insert_order):
                try:
                    from ali2026v3_trading.config_service import resolve_product_exchange
                    if not exchange:
                        exchange = resolve_product_exchange(instrument_id)
                    result = self._platform_insert_order(
                        exchange=exchange,
                        instrument_id=instrument_id,
                        volume=int(volume),
                        price=price,
                        direction=direction,
                        action=action,
                    )
                    platform_order_id = self._normalize_platform_result(result).get('order_id')
                    if platform_order_id:
                        with self._lock:
                            order['platform_order_id'] = platform_order_id
                    logging.info("[OrderService] 平台下单成功: %s %s %s %d@%.2f", order_id, instrument_id, direction, int(volume), price)
                except Exception as e:
                    logging.error("[OrderService] 平台下单失败: %s %s", instrument_id, e)
                    with self._lock:
                        order['status'] = 'FAILED'
                        self._stats['failed_orders'] += 1
                    return None
            else:
                logging.info("[OrderService] 模拟下单: %s %s %s %d@%.2f", order_id, instrument_id, direction, int(volume), price)
            return order_id
        except Exception as e:
            logging.error("[OrderService] Send order error: %s", e)
            with self._lock:
                self._stats['failed_orders'] += 1
            return None

    def execute_by_ranking(self, targets: List[Dict[str, Any]]) -> List[str]:
        if not targets:
            return []
        results = []
        for target in targets:
            instrument_id = target.get('instrument_id', '')
            volume = target.get('lots', 1)
            price = target.get('price', 0)
            if not instrument_id or price <= 0:
                continue
            order_id = self.send_order(
                instrument_id=instrument_id,
                volume=volume,
                price=price,
                direction='BUY',
                action='OPEN',
            )
            if order_id:
                results.append(order_id)
            else:
                logging.warning("[OrderService] execute_by_ranking 下单失败: %s", instrument_id)
        return results

    def cancel_order(self, order_id: str) -> bool:
        try:
            with self._lock:
                order = self._orders_by_id.get(order_id)
                if not order:
                    logging.warning("[OrderService] Order not found: %s", order_id)
                    return False
                if order['status'] in ('FILLED', 'CANCELLED', 'FAILED'):
                    return False
                order['status'] = 'CANCELLED'
                order['updated_at'] = datetime.now()
                self._stats['cancelled_orders'] += 1
                platform_id = order.get('platform_order_id', order_id)
            if self._platform_cancel_order and callable(self._platform_cancel_order):
                try:
                    self._platform_cancel_order(platform_id)
                    logging.info("[OrderService] 平台撤单成功: %s", order_id)
                except Exception as e:
                    logging.error("[OrderService] 平台撤单失败: %s %s", order_id, e)
                    return False
            return True
        except Exception as e:
            logging.error("[OrderService] Cancel order error: %s", e)
            return False

    def on_trade_update(self, trade_data: Any) -> None:
        try:
            normalized = self._normalize_platform_result(trade_data)
            oid = normalized.get('order_id', '')
            sts = normalized.get('status', '')
            filled = normalized.get('filled_volume', 0)
            with self._lock:
                order = self._orders_by_id.get(oid)
                if not order:
                    for o in self._orders_by_id.values():
                        if o.get('platform_order_id') == oid:
                            order = o
                            break
                if order:
                    order['status'] = sts
                    order['filled_volume'] = filled
                    order['updated_at'] = datetime.now()
                    if sts in ('FILLED', 'ALL_FILLED', '全成'):
                        self._stats['successful_orders'] += 1
        except Exception as e:
            logging.error("[OrderService] on_trade_update error: %s", e)

    def check_pending_orders(self) -> None:
        now = datetime.now()
        with self._lock:
            for order_id, order in list(self._orders_by_id.items()):
                if order['status'] != 'SUBMITTED':
                    continue
                elapsed = (now - order['created_at']).total_seconds()
                if elapsed > self._order_timeout_seconds:
                    self.cancel_order(order_id)
                    retry_count = order.get('retry_count', 0)
                    if retry_count < self._max_chase_retries:
                        self._chase_reorder(order, retry_count + 1)

    def _get_tick_size(self, instrument_id: str) -> float:
        """获取合约最小变动价位
        
        注意：instrument_id 已在入口处标准化，此处直接提取品种代码
        """
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            # 直接使用正则提取品种代码（与 analytics_service 保持一致）
            product_match = re.match(r'^([A-Za-z]+)', instrument_id)
            product = product_match.group(1) if product_match else instrument_id
            product_cache = params_svc.get_product_cache(product)
            if product_cache:
                tick_size = float(product_cache.get('tick_size', 0))
                if tick_size > 0:
                    return tick_size
        except Exception:
            pass
        return 1.0

    def _chase_reorder(self, original_order: Dict, retry_count: int) -> None:
        instrument_id = original_order['instrument_id']
        tick_size = self._get_tick_size(instrument_id)
        new_price = original_order['price'] + tick_size * (1 if original_order['direction'] == 'BUY' else -1)
        new_order_id = self.send_order(
            instrument_id=instrument_id,
            volume=original_order['volume'],
            price=new_price,
            direction=original_order['direction'],
            action=original_order['action'],
            exchange=original_order.get('exchange', ''),
        )
        if new_order_id:
            with self._lock:
                self._orders_by_id[new_order_id]['retry_count'] = retry_count
                self._orders_by_id[new_order_id]['original_order_id'] = original_order['order_id']
            logging.info("[OrderService] 追单: %s -> %s retry=%d price=%.2f->%.2f",
                         original_order['order_id'], new_order_id, retry_count, original_order['price'], new_price)
        else:
            logging.warning("[OrderService] 追单失败: %s retry=%d", instrument_id, retry_count)

    def get_order(self, order_id: str) -> Optional[Dict]:
        with self._lock:
            return self._orders_by_id.get(order_id)

    def get_orders_by_instrument(self, instrument_id: str) -> List[Dict]:
        with self._lock:
            return [o for o in self._orders_by_id.values() if o['instrument_id'] == instrument_id]

    # ✅ ID唯一：get_stats统一接口，返回值含service_name="OrderService"
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            # 定期清理已完成订单（每5分钟）
            now = time.time()
            if now - self._last_cleanup > self._cleanup_interval:
                self._last_cleanup = now
                to_remove = [oid for oid, o in self._orders_by_id.items() 
                            if o['status'] in ('FILLED', 'CANCELLED', 'FAILED') 
                            and (datetime.now() - o['updated_at']).total_seconds() > 3600]
                for oid in to_remove:
                    del self._orders_by_id[oid]
                if to_remove:
                    logging.info(f"[OrderService] 清理{len(to_remove)}个已完成订单")
            return {
                'service_name': 'OrderService',  # ✅ ID唯一：统一标识服务来源
                **self._stats,
                'active_orders': sum(1 for o in self._orders_by_id.values() if o['status'] == 'SUBMITTED'),
                'total_tracked': len(self._orders_by_id),
                'chase_tasks': len(self._chase_tasks),
            }

    def _is_duplicate_order(self, instrument_id: str, order_key: str) -> bool:
        with self._lock:
            recent = self._recent_orders_by_instrument.get(instrument_id, [])
            for order in recent[-10:]:
                if order == order_key:
                    return True
            recent.append(order_key)
            if len(recent) > 20:
                self._recent_orders_by_instrument[instrument_id] = recent[-20:]
        return False

    def _generate_order_id(self) -> str:
        import uuid
        timestamp = int(time.time() * 1000)
        unique_suffix = uuid.uuid4().hex[:8]
        return f"ORD_{timestamp}_{unique_suffix}"

    def _get_rate_limit(self) -> int:
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            rate = params_svc.get_int('rate_limit_global_per_min', 60)
            if rate > 0:
                return rate
        except Exception:
            pass
        return 60


__all__ = ['OrderService', 'RateLimiter', 'PlatformAuthenticator', 'get_order_service']
