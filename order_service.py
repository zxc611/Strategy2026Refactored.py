"""
订单服务模块 - CQRS 架构 Command 层
来源：09_executor.py + 10_gate.py (部分)
功能：订单执行 + 平台认证 + 限流控制 + 撤单追单 + V7增强(开仓理由/异常处理/持久化/自成交检测)

优化 v2.0 (2026-05-12):
- ✅ 集成HFT增强: 智能订单拆分(基于订单簿深度)
- ✅ 集成HFT增强: 做市商扫单防御(IOC订单/隐藏挂单)
"""
from __future__ import annotations

import json
import os
import random
import threading
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

from ali2026v3_trading.event_bus import RateLimiter

from dataclasses import dataclass, field
from enum import Enum, auto

_OPEN_REASON_CODES = frozenset({
    'CORRECT_RESONANCE', 'CORRECT_DIVERGENCE',
    'INCORRECT_REVERSAL', 'OTHER_SCALP', 'MANUAL',
    'BOX_SPRING',
})


class BaseService:
    def __init__(self, event_bus=None):
        self.event_bus = event_bus


class PlatformAuthenticator:
    DEFAULT_TOKEN_EXPIRY_SECONDS = 3600.0
    
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._lock = threading.Lock()

    def get_token(self) -> Optional[str]:
        with self._lock:
            if self._token and time.time() < self._token_expiry:
                return self._token
            return None

    def set_token(self, token: str, expires_in: float = None) -> None:
        if expires_in is None:
            expires_in = self.DEFAULT_TOKEN_EXPIRY_SECONDS
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
    DEFAULT_ORDER_TIMEOUT_SECONDS = 5.0
    MAX_CHASE_RETRIES = 3
    MAX_SEND_RETRIES = 3
    CLEANUP_INTERVAL_SECONDS = 300
    DEFAULT_CONTRACT_MULTIPLIER = 100
    
    def __init__(self, event_bus=None):
        super().__init__(event_bus)
        self.authenticator = PlatformAuthenticator()
        rate_per_min = self._get_rate_limit()
        self.rate_limiter = RateLimiter(rate=max(rate_per_min / 60.0, 1.0))
        self._orders_by_id: Dict[str, Dict] = {}
        self._recent_orders_by_instrument: Dict[str, List[str]] = {}
        self._chase_tasks: Dict[str, Dict] = {}
        self._platform_id_to_order_id: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._platform_insert_order = None
        self._platform_cancel_order = None
        self._platform_insert_order_params = set()
        self._order_timeout_seconds = self.DEFAULT_ORDER_TIMEOUT_SECONDS
        self._max_chase_retries = self.MAX_CHASE_RETRIES
        self._max_send_retries = self.MAX_SEND_RETRIES
        self._stats = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'cancelled_orders': 0,
            'chase_tasks_active': 0,
            'self_trade_blocks': 0,
            'retry_successes': 0,
            'partial_fills': 0,
            'insufficient_fund_orders': 0,
        }
        self._last_cleanup = time.time()
        self._cleanup_interval = self.CLEANUP_INTERVAL_SECONDS

        self._trade_log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs'
        )
        self._trade_log_path: Optional[str] = None
        self._trade_log_lock = threading.Lock()
        self._trade_log_initialized = False

        self._insufficient_fund_signals: List[Dict[str, Any]] = []
        self._pending_order_attempts: Dict[str, Dict[str, Any]] = {}
        self._virtual_positions: Dict[str, Dict[str, Any]] = {}
        self._virtual_position_pnl_history: List[Dict[str, Any]] = []
        self._max_insufficient_fund_signals = 1000
        self._max_pnl_history = 10000

        self._hft_order_splitter = None
        self._hft_defense_engine = None

    def bind_platform_apis(self, insert_order_func, cancel_order_func):
        self._platform_insert_order = insert_order_func
        self._platform_cancel_order = cancel_order_func
        self._platform_insert_order_params = set()
        if insert_order_func and callable(insert_order_func):
            try:
                import inspect
                sig = inspect.signature(insert_order_func)
                has_var_keyword = any(
                    p.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
                    for p in sig.parameters.values()
                )
                if has_var_keyword:
                    self._platform_insert_order_params = set()
                    logging.info("[OrderService] 平台下单API含*args/**kwargs，跳过参数过滤: %s", list(sig.parameters.keys()))
                else:
                    self._platform_insert_order_params = set(sig.parameters.keys())
                    logging.info("[OrderService] 平台下单API参数签名: %s", list(sig.parameters.keys()))
            except Exception as e:
                logging.warning("[OrderService] 无法检测平台API签名: %s", e)
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
        is_chase: bool = False,
        open_reason: str = '',
    ) -> Optional[str]:
        if action != 'CLOSE' and not is_chase:
            if not self.rate_limiter.acquire():
                logging.warning("[OrderService] Order rate limited: %s", instrument_id)
                return None
            order_key = f"{instrument_id}_{exchange}_{direction}_{action}_{volume}_{round(price, 4)}"
            if self._is_duplicate_order(instrument_id, order_key):
                logging.warning("[OrderService] Duplicate order: %s", order_key)
                return None

        if action == 'OPEN' and direction in ('BUY', 'SELL'):
            if self._check_self_trade(instrument_id, direction):
                logging.warning("[OrderService] Self-trade detected: %s %s, blocking", instrument_id, direction)
                with self._lock:
                    self._stats['self_trade_blocks'] += 1
                return None

            risk_level = self._check_cross_strategy_risk()
            if risk_level in ('BLOCK', 'CIRCUIT_BREAK'):
                logging.warning("[OrderService] 跨策略风控阻断: level=%s instrument=%s dir=%s", risk_level, instrument_id, direction)
                with self._lock:
                    self._stats['risk_guard_blocks'] = self._stats.get('risk_guard_blocks', 0) + 1
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
                'open_reason': open_reason if open_reason in _OPEN_REASON_CODES else '',
                'status': 'SUBMITTED',
                'filled_volume': 0,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
            }
            with self._lock:
                self._orders_by_id[order_id] = order
                self._stats['total_orders'] += 1
                self._pending_order_attempts[order_id] = {
                    'timestamp': datetime.now().isoformat(),
                    'instrument_id': instrument_id,
                    'price': price,
                    'volume': volume,
                    'direction': direction,
                    'action': action,
                    'open_reason': order.get('open_reason', ''),
                }

            send_result = self._execute_send_order(order, retry_count=0)
            if send_result is None and not is_chase:
                send_result = self._retry_send_order(order)

            if send_result is not None:
                self._persist_trade_log(order, 'OPEN')
                return send_result
            return None
        except Exception as e:
            logging.error("[OrderService] Send order error: %s", e)
            with self._lock:
                self._stats['failed_orders'] += 1
            return None

    def _execute_send_order(self, order: Dict, retry_count: int = 0) -> Optional[str]:
        """执行下单（平台调用），返回order_id或None"""
        instrument_id = order['instrument_id']
        exchange = order['exchange']
        volume = order['volume']
        price = order['price']
        direction = order['direction']
        action = order['action']
        order_id = order['order_id']

        if self._platform_insert_order and callable(self._platform_insert_order):
            try:
                from ali2026v3_trading.config_service import resolve_product_exchange
                if not exchange:
                    exchange = resolve_product_exchange(instrument_id)
                all_params = {
                    'exchange': exchange,
                    'instrument_id': instrument_id,
                    'volume': int(volume),
                    'price': price,
                    'direction': direction,
                    'action': action,
                }
                _PARAM_NAME_MAP = {
                    'direction': 'order_direction',
                    'action': 'order_type',
                }
                if self._platform_insert_order_params:
                    mapped_params = {}
                    for k, v in all_params.items():
                        target_key = _PARAM_NAME_MAP.get(k, k)
                        if target_key in self._platform_insert_order_params:
                            mapped_params[target_key] = v
                    filtered_params = mapped_params
                else:
                    filtered_params = all_params
                result = self._platform_insert_order(**filtered_params)
                platform_order_id = self._normalize_platform_result(result).get('order_id')
                if platform_order_id:
                    with self._lock:
                        order['platform_order_id'] = platform_order_id
                        self._platform_id_to_order_id[str(platform_order_id)] = order_id
                logging.info("[OrderService] 平台下单成功: %s %s %s %d@%.2f reason=%s", order_id, instrument_id, direction, int(volume), price, order.get('open_reason', ''))
                return order_id
            except Exception as e:
                error_msg = str(e)
                logging.error("[OrderService] 平台下单失败(retry=%d): %s %s", retry_count, instrument_id, e)
                if retry_count == 0:
                    with self._lock:
                        order['status'] = 'FAILED'
                        self._stats['failed_orders'] += 1
                        if '资金不足' in error_msg or 'insufficient' in error_msg.lower():
                            self._record_insufficient_fund(order_id, error_msg)
                return None
        else:
            logging.info("[OrderService] 模拟下单: %s %s %s %d@%.2f reason=%s", order_id, instrument_id, direction, int(volume), price, order.get('open_reason', ''))
            return order_id

    def _retry_send_order(self, order: Dict) -> Optional[str]:
        """V7新增：指数退避重试，最多3次，间隔2^n秒"""
        for retry in range(1, self._max_send_retries + 1):
            delay = 2 ** retry
            logging.info("[OrderService] Retry %d/%d after %.0fs: %s", retry, self._max_send_retries, delay, order['instrument_id'])
            time.sleep(delay)
            result = self._execute_send_order(order, retry_count=retry)
            if result is not None:
                with self._lock:
                    self._stats['retry_successes'] += 1
                    if order['order_id'] in self._orders_by_id:
                        self._orders_by_id[order['order_id']]['status'] = 'SUBMITTED'
                return result
        logging.error("[OrderService] All %d retries failed: %s", self._max_send_retries, order['instrument_id'])
        return None

    def _check_cross_strategy_risk(self) -> str:
        try:
            from ali2026v3_trading.position_service import (
                aggregate_greeks_exposure,
                get_cross_strategy_risk_guard,
                get_position_service,
            )
            guard = get_cross_strategy_risk_guard()
            pos_svc = get_position_service()
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            level, reason, detail = guard.check(exposure)
            if level in (guard.BLOCK, guard.CIRCUIT_BREAK):
                return level
            return 'PASS'
        except Exception as e:
            logging.warning("[OrderService._check_cross_strategy_risk] Error: %s, fail-safe阻断", e)
            return 'BLOCK'

    @staticmethod
    def _check_self_trade_core(order_side: str, existing_orders: List[Dict]) -> bool:
        for order in existing_orders:
            if order.get('status') not in ('SUBMITTED', 'PENDING'):
                continue
            if order_side == 'BUY' and order.get('direction') == 'SELL' and order.get('action') == 'OPEN':
                return True
            if order_side == 'SELL' and order.get('direction') == 'BUY' and order.get('action') == 'OPEN':
                return True
        return False

    def _check_self_trade(self, instrument_id: str, direction: str) -> bool:
        """V7新增：自成交检测 — 同合约存在反向挂单则告警并禁止"""
        with self._lock:
            matching_orders = [
                o for o in self._orders_by_id.values()
                if o['instrument_id'] == instrument_id
            ]
            return self._check_self_trade_core(direction, matching_orders)

    def execute_by_ranking(self, targets: List[Dict[str, Any]], direction: str = 'BUY', action: str = 'OPEN') -> List[str]:
        if not targets:
            return []
        results = []
        for target in targets:
            instrument_id = target.get('instrument_id', '')
            volume = target.get('lots', 1)
            price = target.get('price', 0)
            target_direction = target.get('direction', direction)
            target_action = target.get('action', action)
            open_reason = target.get('open_reason', '')
            if not instrument_id or price <= 0:
                continue
            tick_size = self._get_tick_size(instrument_id)
            if target_direction == 'BUY':
                price = price + tick_size
            elif target_direction == 'SELL':
                price = max(0.01, price - tick_size)
            order_id = self.send_order(
                instrument_id=instrument_id,
                volume=volume,
                price=price,
                direction=target_direction,
                action=target_action,
                open_reason=open_reason,
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
                platform_id = order.get('platform_order_id', order_id)
            if self._platform_cancel_order and callable(self._platform_cancel_order):
                try:
                    self._platform_cancel_order(platform_id)
                    logging.info("[OrderService] 平台撤单成功: %s", order_id)
                except Exception as e:
                    logging.error("[OrderService] 平台撤单失败: %s %s", order_id, e)
                    return False
            with self._lock:
                order['status'] = 'CANCELLED'
                order['updated_at'] = datetime.now()
                self._stats['cancelled_orders'] += 1
            return True
        except Exception as e:
            logging.error("[OrderService] Cancel order error: %s", e)
            return False

    def cancel_all_pending(self) -> int:
        """V7新增：撤销所有未成交订单（断路器触发时调用）"""
        cancelled_count = 0
        with self._lock:
            pending_ids = [
                oid for oid, o in self._orders_by_id.items()
                if o['status'] in ('SUBMITTED', 'PENDING')
            ]
        for oid in pending_ids:
            if self.cancel_order(oid):
                cancelled_count += 1
        if cancelled_count > 0:
            logging.info("[OrderService] cancel_all_pending: %d orders cancelled", cancelled_count)
        return cancelled_count

    def on_trade_update(self, trade_data: Any) -> None:
        try:
            normalized = self._normalize_platform_result(trade_data)
            oid = normalized.get('order_id', '')
            sts = normalized.get('status', '')
            filled = normalized.get('filled_volume', 0)
            with self._lock:
                order = self._orders_by_id.get(oid)
                if not order:
                    mapped_id = self._platform_id_to_order_id.get(oid)
                    if mapped_id:
                        order = self._orders_by_id.get(mapped_id)
                    if not order:
                        for o in self._orders_by_id.values():
                            if o.get('platform_order_id') == oid:
                                order = o
                                break
                if order:
                    prev_filled = order.get('filled_volume', 0)
                    order['status'] = sts
                    order['filled_volume'] = filled
                    order['updated_at'] = datetime.now()

                    if filled > 0 and filled < order.get('volume', 0) and sts not in ('FILLED', 'ALL_FILLED', '全成'):
                        self._stats['partial_fills'] += 1
                        logging.info("[OrderService] Partial fill: %s filled=%d/%d", oid, filled, order.get('volume', 0))

                    if sts in ('FILLED', 'ALL_FILLED', '全成'):
                        self._stats['successful_orders'] += 1
                        self._persist_trade_log(order, 'FILLED')

                    if sts in ('CANCELLED',) and filled > 0 and filled < order.get('volume', 0):
                        unfilled = order.get('volume', 0) - filled
                        logging.warning("[OrderService] Partial fill cancelled: %s filled=%d unfilled=%d", oid, filled, unfilled)
                        self._persist_trade_log(order, 'PARTIAL_CANCEL', extra={'unfilled': unfilled})
        except Exception as e:
            logging.error("[OrderService] on_trade_update error: %s", e)

    def check_pending_orders(self) -> None:
        now = datetime.now()
        timeout_orders = []
        with self._lock:
            expired_order_ids = []
            for order_id, order in list(self._orders_by_id.items()):
                if order['status'] not in ('SUBMITTED', 'PENDING'):
                    pid = order.get('platform_order_id')
                    if pid and str(pid) in self._platform_id_to_order_id:
                        del self._platform_id_to_order_id[str(pid)]
                    if order['status'] in ('FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', '全成'):
                        elapsed = (now - order.get('updated_at', order['created_at'])).total_seconds()
                        if elapsed > 300:
                            expired_order_ids.append(order_id)
                    continue
                elapsed = (now - order['created_at']).total_seconds()
                if elapsed > self._order_timeout_seconds:
                    timeout_orders.append((order_id, dict(order)))
            for oid in expired_order_ids:
                del self._orders_by_id[oid]
        for order_id, order_snapshot in timeout_orders:
            with self._lock:
                current_order = self._orders_by_id.get(order_id)
                if not current_order or current_order['status'] not in ('SUBMITTED', 'PENDING'):
                    continue
            self.cancel_order(order_id)
            retry_count = current_order.get('retry_count', 0) if current_order else order_snapshot.get('retry_count', 0)
            if retry_count < self._max_chase_retries:
                self._chase_reorder(current_order or order_snapshot, retry_count + 1)

    def _get_tick_size(self, instrument_id: str) -> float:
        """获取合约最小变动价位
        
        注意：instrument_id 已在入口处标准化，此处直接提取品种代码
        """
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            # ✅ 使用 params_service 元数据获取品种，而非正则
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            product = meta.get('product') if meta else instrument_id
            product_cache = params_svc.get_product_cache(product)
            if product_cache:
                tick_size = float(product_cache.get('tick_size', 0))
                if tick_size > 0:
                    return tick_size
        except Exception as e:
            # ✅ P1修复：添加异常日志，便于诊断
            logging.debug(f"[OrderService._get_tick_size] Failed to get tick_size from ParamsService: {e}")
        # ✅ P1修复：使用品种配置的默认值，而非硬编码1.0
        default_tick_sizes = {
            'IF': 0.2,  # 沪深300股指期货
            'IC': 0.2,  # 中证500股指期货
            'IH': 0.2,  # 上证50股指期货
            'IM': 0.2,  # 中证1000股指期货
        }
        for prefix, tick in default_tick_sizes.items():
            if instrument_id.startswith(prefix):
                return tick
        return 1.0  # 最终降级

    def _chase_reorder(self, original_order: Dict, retry_count: int) -> None:
        instrument_id = original_order['instrument_id']
        tick_size = self._get_tick_size(instrument_id)
        chase_ticks = min(retry_count, 3)
        price_offset = tick_size * chase_ticks
        
        if original_order['direction'] == 'BUY':
            new_price = original_order['price'] + price_offset
        else:
            new_price = original_order['price'] - price_offset
        new_order_id = self.send_order(
            instrument_id=instrument_id,
            volume=original_order['volume'],
            price=new_price,
            direction=original_order['direction'],
            action=original_order['action'],
            exchange=original_order.get('exchange', ''),
            is_chase=True,
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
        # ✅ P2修复：将订单清理逻辑移到独立方法，避免隐藏副作用
        self._cleanup_orders()
        
        with self._lock:
            return {
                'service_name': 'OrderService',  # ✅ ID唯一：统一标识服务来源
                **self._stats,
                'active_orders': sum(1 for o in self._orders_by_id.values() if o['status'] == 'SUBMITTED'),
                'total_tracked': len(self._orders_by_id),
                'chase_tasks': len(self._chase_tasks),
            }
    
    def _cleanup_orders(self) -> None:
        """✅ P2修复：独立的订单清理方法，消除get_stats的隐藏副作用"""
        now = time.time()
        if now - self._last_cleanup <= self._cleanup_interval:
            return
        
        with self._lock:
            self._last_cleanup = now
            to_remove = [oid for oid, o in self._orders_by_id.items() 
                        if o['status'] in ('FILLED', 'CANCELLED', 'FAILED') 
                        and (datetime.now() - o['updated_at']).total_seconds() > 3600]
            for oid in to_remove:
                del self._orders_by_id[oid]
            if to_remove:
                logging.info(f"[OrderService] 清理{len(to_remove)}个已完成订单")

    def _is_duplicate_order(self, instrument_id: str, order_key: str) -> bool:
        with self._lock:
            if instrument_id not in self._recent_orders_by_instrument:
                self._recent_orders_by_instrument[instrument_id] = []
            recent = self._recent_orders_by_instrument[instrument_id]
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
        except Exception as e:
            logging.debug(f"[OrderService._get_rate_limit] Failed to get rate limit from ParamsService: {e}")
        return 60

    def _ensure_trade_log(self) -> None:
        """V7新增：初始化交易日志文件(JSON lines格式)"""
        if self._trade_log_initialized:
            return
        with self._trade_log_lock:
            if self._trade_log_initialized:
                return
            try:
                os.makedirs(self._trade_log_dir, exist_ok=True)
                today = datetime.now().strftime("%Y%m%d")
                self._trade_log_path = os.path.join(
                    self._trade_log_dir, f"trade_log_{today}.jsonl"
                )
                self._trade_log_initialized = True
                logging.info("[OrderService] Trade log initialized: %s", self._trade_log_path)
            except Exception as e:
                logging.warning("[OrderService] Failed to init trade log: %s", e)

    def _persist_trade_log(self, order: Dict, event_type: str, extra: Optional[Dict] = None) -> None:
        """V7新增：信号/订单持久化到JSON lines日志

        开仓信号、订单状态变化、平仓结果写入JSON lines格式日志。
        平仓时补充close_reason和pnl字段。
        """
        self._ensure_trade_log()
        if not self._trade_log_path:
            return

        try:
            entry = {
                "timestamp": datetime.now().isoformat(),
                "signal_id": order.get('signal_id', ''),
                "state": order.get('state', ''),
                "decision_score": order.get('decision_score'),
                "open_reason": order.get('open_reason', ''),
                "order_id": order.get('order_id', ''),
                "action": event_type,
                "instrument_id": order.get('instrument_id', ''),
                "price": order.get('price', 0),
                "quantity": order.get('volume', 0),
                "direction": order.get('direction', ''),
                "filled_volume": order.get('filled_volume', 0),
                "close_reason": order.get('close_reason'),
                "pnl": order.get('pnl'),
            }
            if extra:
                entry.update(extra)

            with self._trade_log_lock:
                with open(self._trade_log_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(entry, ensure_ascii=False, default=str) + '\n')
        except Exception as e:
            logging.debug("[OrderService._persist_trade_log] Write error: %s", e)

    def persist_close_event(self, order_id: str, close_reason: str, pnl: float) -> None:
        """V7新增：平仓事件持久化，补充close_reason和pnl"""
        with self._lock:
            order = self._orders_by_id.get(order_id)
            if order:
                order['close_reason'] = close_reason
                order['pnl'] = pnl
                self._persist_trade_log(order, 'CLOSE', extra={'close_reason': close_reason, 'pnl': pnl})

    def _record_insufficient_fund(self, order_id: str, error_msg: str) -> None:
        import re
        with self._lock:
            self._stats['insufficient_fund_orders'] += 1
            attempt = self._pending_order_attempts.pop(order_id, {})
            if not attempt:
                attempt = {
                    'timestamp': datetime.now().isoformat(),
                    'instrument_id': '',
                    'price': 0,
                    'volume': 0,
                    'direction': '',
                    'action': '',
                    'open_reason': '',
                }
            missing_match = re.search(r'缺少资金\[([\d.]+)\]', error_msg)
            missing_fund = float(missing_match.group(1)) if missing_match else 0.0
            instrument_id = attempt.get('instrument_id', '')
            option_type = "UNKNOWN"
            strike_price = 0.0
            opt_match = re.search(r'([CP])-(\d+)', instrument_id)
            if opt_match:
                option_type = "PUT" if opt_match.group(1) == 'P' else "CALL"
                strike_price = float(opt_match.group(2))
            signal_record = {
                'timestamp': attempt.get('timestamp', datetime.now().isoformat()),
                'order_id': order_id,
                'instrument_id': instrument_id,
                'option_type': option_type,
                'strike_price': strike_price,
                'price': attempt.get('price', 0),
                'volume': attempt.get('volume', 0),
                'direction': attempt.get('direction', ''),
                'action': attempt.get('action', ''),
                'open_reason': attempt.get('open_reason', ''),
                'missing_fund': missing_fund,
                'error_msg': error_msg,
                'signal_type': f"OPEN_{option_type}" if attempt.get('action') == 'OPEN' else f"CLOSE_{option_type}",
                'theoretical_pnl': 0.0,
            }
            self._insufficient_fund_signals.append(signal_record)
            if len(self._insufficient_fund_signals) > self._max_insufficient_fund_signals:
                self._insufficient_fund_signals = self._insufficient_fund_signals[-self._max_insufficient_fund_signals:]
            self._persist_trade_log(
                {'order_id': order_id, **attempt},
                'INSUFFICIENT_FUND',
                extra={'missing_fund': missing_fund, 'error_msg': error_msg, 'signal_type': signal_record['signal_type']}
            )
            if attempt.get('action') == 'OPEN':
                self._create_virtual_position(signal_record)
            logging.warning(
                "[OrderService] 资金不足信号: %s %s price=%.2f missing=%.2f type=%s",
                order_id, instrument_id, attempt.get('price', 0), missing_fund, signal_record['signal_type']
            )

    def record_insufficient_fund_from_log(self, instrument_id: str, price: float,
                                           volume: int, direction: str, open_close: str,
                                           missing_fund: float, strategy_id: str = '',
                                           investor_id: str = '') -> None:
        import re
        with self._lock:
            self._stats['insufficient_fund_orders'] += 1
            option_type = "UNKNOWN"
            strike_price = 0.0
            opt_match = re.search(r'([CP])-(\d+)', instrument_id)
            if opt_match:
                option_type = "PUT" if opt_match.group(1) == 'P' else "CALL"
                strike_price = float(opt_match.group(2))
            signal_record = {
                'timestamp': datetime.now().isoformat(),
                'order_id': '',
                'instrument_id': instrument_id,
                'option_type': option_type,
                'strike_price': strike_price,
                'price': price,
                'volume': volume,
                'direction': direction,
                'open_close': open_close,
                'open_reason': '',
                'missing_fund': missing_fund,
                'strategy_id': strategy_id,
                'investor_id': investor_id,
                'signal_type': f"OPEN_{option_type}" if open_close == '开仓' else f"CLOSE_{option_type}",
                'theoretical_pnl': 0.0,
            }
            self._insufficient_fund_signals.append(signal_record)
            if len(self._insufficient_fund_signals) > self._max_insufficient_fund_signals:
                self._insufficient_fund_signals = self._insufficient_fund_signals[-self._max_insufficient_fund_signals:]
            if open_close == '开仓':
                self._create_virtual_position(signal_record)
            logging.warning(
                "[OrderService] 资金不足信号(外部): %s %s price=%.2f missing=%.2f type=%s",
                instrument_id, direction, price, missing_fund, signal_record['signal_type']
            )

    def _create_virtual_position(self, signal_record: Dict[str, Any]) -> None:
        instrument_id = signal_record.get('instrument_id', '')
        if not instrument_id:
            return
        direction = signal_record.get('direction', '')
        is_long = direction in ('买', 'BUY')
        vpid = f"VP_{instrument_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{id(signal_record)}"
        virtual_pos = {
            'virtual_position_id': vpid,
            'instrument_id': instrument_id,
            'option_type': signal_record.get('option_type', 'UNKNOWN'),
            'strike_price': signal_record.get('strike_price', 0),
            'open_price': signal_record.get('price', 0),
            'open_time': signal_record.get('timestamp', datetime.now().isoformat()),
            'open_date': datetime.now().strftime("%Y-%m-%d"),
            'volume': signal_record.get('volume', 0),
            'direction': 'long' if is_long else 'short',
            'open_reason': signal_record.get('open_reason', ''),
            'signal_type': signal_record.get('signal_type', ''),
            'missing_fund': signal_record.get('missing_fund', 0),
            'mark_price': signal_record.get('price', 0),
            'mark_date': datetime.now().strftime("%Y-%m-%d"),
            'daily_pnl': 0.0,
            'accumulated_pnl': 0.0,
            'status': 'OPEN',
            'close_price': 0.0,
            'close_time': None,
            'close_reason': '',
            'realized_pnl': 0.0,
            'pnl_history': [],
        }
        self._virtual_positions[vpid] = virtual_pos
        logging.info(
            "[OrderService] 创建虚拟仓位: %s %s %s price=%.2f vol=%d missing=%.2f",
            vpid, instrument_id, virtual_pos['direction'],
            virtual_pos['open_price'], virtual_pos['volume'], virtual_pos['missing_fund']
        )

    def _calc_pnl(self, direction: str, open_price: float, close_price: float, volume: int, instrument_id: str = '') -> float:
        contract_size = self.DEFAULT_CONTRACT_MULTIPLIER
        if instrument_id:
            try:
                from ali2026v3_trading.params_service import get_params_service
                params_svc = get_params_service()
                meta = params_svc.get_instrument_meta_by_id(instrument_id)
                if meta:
                    product = meta.get('product', '')
                    product_cache = params_svc.get_product_cache(product)
                    if product_cache:
                        contract_size = float(product_cache.get('contract_size', 100))
            except Exception:
                pass
        if direction in ('long', '买', 'BUY'):
            return (close_price - open_price) * volume * contract_size
        else:
            return (open_price - close_price) * volume * contract_size

    def mark_virtual_positions_eod(self, close_prices: Optional[Dict[str, float]] = None) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            for vpid, vp in list(self._virtual_positions.items()):
                if vp['status'] != 'OPEN':
                    continue
                if vp.get('mark_date') == today:
                    continue
                close_price = 0.0
                if close_prices and vp['instrument_id'] in close_prices:
                    close_price = close_prices[vp['instrument_id']]
                if close_price <= 0:
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and hasattr(ds, 'realtime_cache'):
                            tick = ds.realtime_cache._latest_ticks.get(vp['instrument_id'])
                            if tick:
                                close_price = tick.get('price', tick.get('last_price', 0.0))
                    except Exception:
                        pass
                if close_price <= 0:
                    close_price = vp.get('mark_price', vp['open_price'])
                daily_pnl = self._calc_pnl(vp['direction'], vp.get('mark_price', vp['open_price']), close_price, vp['volume'])
                vp['mark_price'] = close_price
                vp['mark_date'] = today
                vp['daily_pnl'] = daily_pnl
                vp['accumulated_pnl'] += daily_pnl
                pnl_entry = {
                    'date': today,
                    'mark_price': close_price,
                    'daily_pnl': daily_pnl,
                    'accumulated_pnl': vp['accumulated_pnl'],
                }
                vp['pnl_history'].append(pnl_entry)
                self._virtual_position_pnl_history.append({
                    'virtual_position_id': vpid,
                    'instrument_id': vp['instrument_id'],
                    **pnl_entry,
                })
                if len(self._virtual_position_pnl_history) > self._max_pnl_history:
                    self._virtual_position_pnl_history = self._virtual_position_pnl_history[-self._max_pnl_history:]
                logging.info(
                    "[OrderService] 虚拟仓位隔夜标记: %s %s mark=%.2f daily_pnl=%.2f accum_pnl=%.2f",
                    vpid, vp['instrument_id'], close_price, daily_pnl, vp['accumulated_pnl']
                )

    def on_close_signal(self, instrument_id: str, signal_type: str, close_price: float) -> Optional[Dict[str, Any]]:
        is_close_long = signal_type in ('CLOSE_LONG', 'SELL', '卖', '平多')
        is_close_short = signal_type in ('CLOSE_SHORT', 'BUY', '买', '平空')
        if not (is_close_long or is_close_short):
            return None
        with self._lock:
            matched_vpid = None
            for vpid, vp in self._virtual_positions.items():
                if vp['status'] != 'OPEN':
                    continue
                if vp['instrument_id'] != instrument_id:
                    continue
                if is_close_long and vp['direction'] == 'long':
                    matched_vpid = vpid
                    break
                if is_close_short and vp['direction'] == 'short':
                    matched_vpid = vpid
                    break
            if not matched_vpid:
                return None
            vp = self._virtual_positions[matched_vpid]
            if close_price <= 0:
                try:
                    from ali2026v3_trading.data_service import get_data_service
                    ds = get_data_service()
                    if ds and hasattr(ds, 'realtime_cache'):
                        tick = ds.realtime_cache._latest_ticks.get(instrument_id)
                        if tick:
                            if is_close_long:
                                close_price = tick.get('bid_price', tick.get('price', 0.0))
                            else:
                                close_price = tick.get('ask_price', tick.get('price', 0.0))
                except Exception:
                    pass
            if close_price <= 0:
                close_price = vp.get('mark_price', vp['open_price'])
            daily_pnl = self._calc_pnl(vp['direction'], vp.get('mark_price', vp['open_price']), close_price, vp['volume'])
            realized_pnl = vp['accumulated_pnl'] + daily_pnl
            vp['mark_price'] = close_price
            vp['mark_date'] = datetime.now().strftime("%Y-%m-%d")
            vp['daily_pnl'] = daily_pnl
            vp['accumulated_pnl'] = realized_pnl
            vp['close_price'] = close_price
            vp['close_time'] = datetime.now().isoformat()
            vp['close_reason'] = f"策略平仓信号: {signal_type}"
            vp['realized_pnl'] = realized_pnl
            vp['status'] = 'CLOSED'
            pnl_entry = {
                'date': datetime.now().strftime("%Y-%m-%d"),
                'mark_price': close_price,
                'daily_pnl': daily_pnl,
                'accumulated_pnl': realized_pnl,
                'event': 'CLOSE_SIGNAL',
                'close_reason': vp['close_reason'],
            }
            vp['pnl_history'].append(pnl_entry)
            for signal in self._insufficient_fund_signals:
                if signal.get('instrument_id') == instrument_id and (
                    (signal.get('direction') in ('买', 'BUY') and is_close_long)
                    or (signal.get('direction') in ('卖', 'SELL') and is_close_short)
                ):
                    signal['theoretical_pnl'] = realized_pnl
                    signal['status'] = 'CLOSED_BY_SIGNAL'
            self._persist_trade_log(
                {'order_id': matched_vpid, 'instrument_id': instrument_id, 'price': close_price,
                 'volume': vp['volume'], 'direction': vp['direction']},
                'VIRTUAL_CLOSE',
                extra={
                    'virtual_position_id': matched_vpid,
                    'open_price': vp['open_price'],
                    'close_price': close_price,
                    'realized_pnl': realized_pnl,
                    'close_reason': vp['close_reason'],
                    'hold_days': len(vp['pnl_history']),
                }
            )
            logging.info(
                "[OrderService] 虚拟仓位平仓结算: %s %s open=%.2f close=%.2f realized_pnl=%.2f reason=%s",
                matched_vpid, instrument_id, vp['open_price'], close_price, realized_pnl, vp['close_reason']
            )
            return dict(vp)

    def get_virtual_positions(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            positions = list(self._virtual_positions.values())
            if status:
                positions = [vp for vp in positions if vp.get('status') == status]
            return positions

    def get_virtual_position_summary(self) -> Dict[str, Any]:
        with self._lock:
            open_positions = [vp for vp in self._virtual_positions.values() if vp['status'] == 'OPEN']
            closed_positions = [vp for vp in self._virtual_positions.values() if vp['status'] == 'CLOSED']
            total_open_pnl = sum(vp.get('accumulated_pnl', 0) for vp in open_positions)
            total_realized_pnl = sum(vp.get('realized_pnl', 0) for vp in closed_positions)
            return {
                'total_virtual': len(self._virtual_positions),
                'open_count': len(open_positions),
                'closed_count': len(closed_positions),
                'total_open_pnl': total_open_pnl,
                'total_realized_pnl': total_realized_pnl,
                'total_pnl': total_open_pnl + total_realized_pnl,
            }

    def generate_daily_signal_report(self, date: Optional[str] = None) -> str:
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            date_signals = [
                s for s in self._insufficient_fund_signals
                if s.get('timestamp', '').startswith(date.replace('-', ''))
                or s.get('timestamp', '').startswith(date)
            ]
        total_missing = sum(s.get('missing_fund', 0) for s in date_signals)
        total_pnl = sum(s.get('theoretical_pnl', 0) for s in date_signals)
        with self._lock:
            open_vps = [vp for vp in self._virtual_positions.values() if vp['status'] == 'OPEN']
            closed_today = [vp for vp in self._virtual_positions.values()
                           if vp['status'] == 'CLOSED' and vp.get('close_time', '')[:10] == date]
        total_open_pnl = sum(vp.get('accumulated_pnl', 0) for vp in open_vps)
        total_realized_pnl = sum(vp.get('realized_pnl', 0) for vp in closed_today)
        lines = [
            "=" * 80,
            f"当日信号明细报告 - {date}",
            "=" * 80,
            f"资金不足信号数: {len(date_signals)}",
            f"缺少资金总额: {total_missing:.2f}",
            f"理论盈亏合计: {total_pnl:.2f}",
            f"虚拟仓位(持仓中): {len(open_vps)} 浮动盈亏: {total_open_pnl:.2f}",
            f"虚拟仓位(今日平仓): {len(closed_today)} 实现盈亏: {total_realized_pnl:.2f}",
            "-" * 80,
        ]
        for i, s in enumerate(date_signals, 1):
            lines.extend([
                f"【信号 {i}】",
                f"  时间: {s.get('timestamp', '')}",
                f"  合约: {s.get('instrument_id', '')}",
                f"  期权类型: {s.get('option_type', '')}",
                f"  行权价: {s.get('strike_price', 0)}",
                f"  方向: {s.get('direction', '')} {s.get('open_close', s.get('action', ''))}",
                f"  价格: {s.get('price', 0)}",
                f"  数量: {s.get('volume', 0)}",
                f"  缺少资金: {s.get('missing_fund', 0):.2f}",
                f"  信号类型: {s.get('signal_type', '')}",
                f"  开仓理由: {s.get('open_reason', '')}",
                f"  理论盈亏: {s.get('theoretical_pnl', 0):.2f}",
            ])
        if open_vps:
            lines.extend(["", "-" * 80, "虚拟持仓明细:", "-" * 80])
            for i, vp in enumerate(open_vps, 1):
                lines.extend([
                    f"【虚拟仓位 {i}】",
                    f"  合约: {vp['instrument_id']}  方向: {vp['direction']}",
                    f"  开仓价: {vp['open_price']:.2f}  标记价: {vp.get('mark_price', 0):.2f}",
                    f"  数量: {vp['volume']}  累计盈亏: {vp.get('accumulated_pnl', 0):.2f}",
                    f"  持仓天数: {len(vp.get('pnl_history', []))}",
                ])
        if closed_today:
            lines.extend(["", "-" * 80, "今日平仓虚拟仓位:", "-" * 80])
            for i, vp in enumerate(closed_today, 1):
                lines.extend([
                    f"【已平仓 {i}】",
                    f"  合约: {vp['instrument_id']}  方向: {vp['direction']}",
                    f"  开仓价: {vp['open_price']:.2f}  平仓价: {vp.get('close_price', 0):.2f}",
                    f"  实现盈亏: {vp.get('realized_pnl', 0):.2f}  平仓原因: {vp.get('close_reason', '')}",
                ])
        lines.extend([
            "=" * 80,
            f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
        ])
        report = "\n".join(lines)
        report_file = os.path.join(self._trade_log_dir, f"daily_signal_report_{date}.txt")
        try:
            os.makedirs(self._trade_log_dir, exist_ok=True)
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            json_file = os.path.join(self._trade_log_dir, f"daily_signal_report_{date}.json")
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'date': date,
                    'signals': date_signals,
                    'total_missing': total_missing,
                    'total_pnl': total_pnl,
                    'virtual_positions_open': [{'id': k, **{kk: vv for kk, vv in v.items() if kk != 'pnl_history'}}
                                               for k, v in self._virtual_positions.items() if v['status'] == 'OPEN'],
                    'virtual_positions_closed_today': [{'id': k, **v} for k, v in self._virtual_positions.items()
                                                       if v['status'] == 'CLOSED' and v.get('close_time', '')[:10] == date],
                }, f, ensure_ascii=False, indent=2, default=str)
            logging.info("[OrderService] 日报已生成: %s", report_file)
        except Exception as e:
            logging.warning("[OrderService] 生成日报失败: %s", e)
        return report

    def get_insufficient_fund_signals(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if date:
                return [
                    s for s in self._insufficient_fund_signals
                    if s.get('timestamp', '').startswith(date.replace('-', ''))
                    or s.get('timestamp', '').startswith(date)
                ]
            return list(self._insufficient_fund_signals)

    def enable_hft_enhancements(self) -> None:
        self._hft_order_splitter = SmartOrderSplitter()
        logging.info("[OrderService] HFT增强已启用(智能订单拆分)")

    def send_order_split(
        self,
        instrument_id: str,
        volume: float,
        price: float,
        direction: str = 'BUY',
        action: str = 'OPEN',
        exchange: str = '',
        signal_strength: float = 0.0,
        bids: Optional[List[Tuple[float, int]]] = None,
        asks: Optional[List[Tuple[float, int]]] = None,
        open_reason: str = '',
    ) -> List[str]:
        if not self._hft_order_splitter:
            order_id = self.send_order(instrument_id, volume, price, direction, action, exchange, open_reason=open_reason)
            return [order_id] if order_id else []

        split_result = self._hft_order_splitter.plan_order_split(
            instrument_id=instrument_id,
            volume=int(volume),
            direction=direction,
            signal_strength=signal_strength,
            bids=bids,
            asks=asks,
            strategy=OrderSplitStrategy.ADAPTIVE,
        )

        order_ids = []
        for child in split_result.child_orders:
            child_price = child.get('price', price)
            if child_price <= 0:
                child_price = price
            oid = self.send_order(
                instrument_id=child.get('instrument_id', instrument_id),
                volume=child.get('volume', 1),
                price=child_price,
                direction=child.get('direction', direction),
                action=action,
                exchange=exchange,
                open_reason=open_reason,
            )
            if oid:
                order_ids.append(oid)

        logging.info("[OrderService] HFT拆单: %s vol=%d -> %d子单 strategy=%s",
                     instrument_id, int(volume), len(order_ids), split_result.strategy_used.name)
        return order_ids

    def send_defensive_order(
        self,
        instrument_id: str,
        volume: float,
        price: float,
        direction: str = 'BUY',
        action: str = 'CLOSE',
        exchange: str = '',
        signal_strength: float = 0.0,
        is_stop_order: bool = False,
    ) -> List[str]:
        if not self._hft_defense_engine:
            try:
                from ali2026v3_trading.order_flow_bridge import MarketMakerDefenseEngine
                self._hft_defense_engine = MarketMakerDefenseEngine()
            except Exception as e:
                logging.warning("[OrderService] HFT防御引擎初始化失败，回退标准下单: %s", e)
                order_id = self.send_order(instrument_id, volume, price, direction, action, exchange)
                return [order_id] if order_id else []

        tick_size = self._get_tick_size(instrument_id)
        defensive_orders = self._hft_defense_engine.create_defensive_order(
            instrument_id=instrument_id,
            direction=direction,
            volume=int(volume),
            price=price,
            signal_strength=signal_strength,
            tick_size=tick_size,
            is_stop_order=is_stop_order,
        )

        order_ids = []
        for def_order in defensive_orders:
            oid = self.send_order(
                instrument_id=def_order.instrument_id,
                volume=def_order.volume,
                price=def_order.price,
                direction=def_order.direction,
                action=action,
                exchange=exchange,
            )
            if oid:
                order_ids.append(oid)

        logging.info("[OrderService] HFT防御单: %s vol=%d -> %d子单 type=%s",
                     instrument_id, int(volume), len(order_ids),
                     defensive_orders[0].defense_type.name if defensive_orders else 'N/A')
        return order_ids


__all__ = ['OrderService', 'RateLimiter', 'PlatformAuthenticator', 'get_order_service',
           'SmartOrderSplitter', 'OrderSplitStrategy', 'SplitOrderResult']


class OrderSplitStrategy(Enum):
    AGGRESSIVE = auto()
    PASSIVE = auto()
    ADAPTIVE = auto()


@dataclass
class SplitOrderResult:
    parent_order_id: str
    child_orders: List[Dict[str, Any]]
    total_volume: int
    filled_volume: int
    strategy_used: OrderSplitStrategy
    estimated_slippage_bps: float
    created_at: float = field(default_factory=time.time)


class SmartOrderSplitter:
    def __init__(self, max_depth_levels: int = 5,
                 aggressive_signal_threshold: float = 0.8,
                 passive_signal_threshold: float = 0.6,
                 max_slippage_ticks: int = 3,
                 high_plr_threshold: float = 3.0,
                 low_plr_threshold: float = 1.5):
        self._max_depth_levels = max_depth_levels
        self._aggressive_threshold = aggressive_signal_threshold
        self._passive_threshold = passive_signal_threshold
        self._max_slippage_ticks = max_slippage_ticks
        self._high_plr_threshold = high_plr_threshold
        self._low_plr_threshold = low_plr_threshold
        self._lock = threading.Lock()
        self._stats = {
            'total_splits': 0,
            'aggressive_orders': 0,
            'passive_orders': 0,
            'adaptive_orders': 0,
            'avg_slippage_bps': 0.0,
            'plr_adjusted_splits': 0,
        }

    def plan_order_split(
        self,
        instrument_id: str,
        volume: int,
        direction: str,
        signal_strength: float,
        bids: Optional[List[Tuple[float, int]]] = None,
        asks: Optional[List[Tuple[float, int]]] = None,
        strategy: OrderSplitStrategy = OrderSplitStrategy.ADAPTIVE,
        estimated_plr: float = 0.0,
    ) -> SplitOrderResult:
        self._stats['total_splits'] += 1
        if estimated_plr > 0:
            if estimated_plr >= self._high_plr_threshold:
                strategy = OrderSplitStrategy.AGGRESSIVE
                self._stats['plr_adjusted_splits'] += 1
            elif estimated_plr < self._low_plr_threshold:
                strategy = OrderSplitStrategy.PASSIVE
                self._stats['plr_adjusted_splits'] += 1
        effective_strategy = self._resolve_strategy(strategy, signal_strength)
        if effective_strategy == OrderSplitStrategy.AGGRESSIVE:
            child_orders = self._plan_aggressive_split(
                instrument_id, volume, direction, asks if direction == 'BUY' else bids)
            self._stats['aggressive_orders'] += 1
        elif effective_strategy == OrderSplitStrategy.PASSIVE:
            child_orders = self._plan_passive_split(
                instrument_id, volume, direction, bids, asks)
            self._stats['passive_orders'] += 1
        else:
            child_orders = self._plan_adaptive_split(
                instrument_id, volume, direction, signal_strength, bids, asks)
            self._stats['adaptive_orders'] += 1
        slippage = self._estimate_slippage(child_orders, bids, asks, direction)
        result = SplitOrderResult(
            parent_order_id=f"SPLIT_{instrument_id}_{int(time.time()*1000)}_{id(self)}",
            child_orders=child_orders,
            total_volume=volume,
            filled_volume=sum(o.get('volume', 0) for o in child_orders),
            strategy_used=effective_strategy,
            estimated_slippage_bps=slippage,
        )
        logging.info("[SmartOrderSplitter] %s %s vol=%d strategy=%s children=%d slippage=%.1fbps",
                     instrument_id, direction, volume, effective_strategy.name, len(child_orders), slippage)
        return result

    def _resolve_strategy(self, strategy: OrderSplitStrategy, signal_strength: float) -> OrderSplitStrategy:
        if strategy != OrderSplitStrategy.ADAPTIVE:
            return strategy
        if signal_strength >= self._aggressive_threshold:
            return OrderSplitStrategy.AGGRESSIVE
        if signal_strength >= self._passive_threshold:
            return OrderSplitStrategy.ADAPTIVE
        return OrderSplitStrategy.PASSIVE

    def _plan_aggressive_split(self, instrument_id: str, volume: int, direction: str,
                                opposite_book: Optional[List[Tuple[float, int]]]) -> List[Dict[str, Any]]:
        if not opposite_book:
            return [{'instrument_id': instrument_id, 'volume': volume,
                     'direction': direction, 'price_offset_ticks': 1, 'order_type': 'market'}]
        remaining = volume
        child_orders = []
        for i, (price, avail_vol) in enumerate(opposite_book[:self._max_depth_levels]):
            if remaining <= 0:
                break
            take_vol = min(remaining, avail_vol)
            child_orders.append({
                'instrument_id': instrument_id, 'volume': take_vol, 'direction': direction,
                'price': price, 'price_offset_ticks': i, 'order_type': 'aggressive_take', 'depth_level': i,
            })
            remaining -= take_vol
        if remaining > 0:
            child_orders.append({
                'instrument_id': instrument_id, 'volume': remaining, 'direction': direction,
                'price_offset_ticks': len(child_orders), 'order_type': 'market',
            })
        return child_orders

    def _plan_passive_split(self, instrument_id: str, volume: int, direction: str,
                             bids: Optional[List[Tuple[float, int]]], asks: Optional[List[Tuple[float, int]]]) -> List[Dict[str, Any]]:
        target_price = 0.0
        if direction == 'BUY' and bids:
            target_price = bids[0][0]
        elif direction == 'SELL' and asks:
            target_price = asks[0][0]
        return [{'instrument_id': instrument_id, 'volume': volume, 'direction': direction,
                 'price': target_price, 'price_offset_ticks': 0, 'order_type': 'passive_limit', 'time_in_force': 'GTC'}]

    def _plan_adaptive_split(self, instrument_id: str, volume: int, direction: str,
                              signal_strength: float, bids: Optional[List[Tuple[float, int]]],
                              asks: Optional[List[Tuple[float, int]]]) -> List[Dict[str, Any]]:
        ratio = min(1.0, (signal_strength - self._passive_threshold) /
                    max(0.01, self._aggressive_threshold - self._passive_threshold))
        aggressive_vol = int(volume * ratio)
        passive_vol = volume - aggressive_vol
        orders = []
        if aggressive_vol > 0:
            opposite_book = asks if direction == 'BUY' else bids
            orders.extend(self._plan_aggressive_split(instrument_id, aggressive_vol, direction, opposite_book))
        if passive_vol > 0:
            orders.extend(self._plan_passive_split(instrument_id, passive_vol, direction, bids, asks))
        return orders

    def _estimate_slippage(self, child_orders: List[Dict], bids, asks, direction: str) -> float:
        if not bids or not asks:
            return 0.0
        best_bid, best_ask = bids[0][0], asks[0][0]
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return 0.0
        total_slip = sum(o.get('price_offset_ticks', 0) for o in child_orders)
        avg_slip = total_slip / max(1, len(child_orders))
        tick_size = best_ask - best_bid
        if tick_size <= 0:
            tick_size = 0.2
        return (avg_slip * tick_size / mid) * 10000

    def get_stats(self) -> Dict[str, Any]:
        return {'service_name': 'SmartOrderSplitter', **self._stats}


class IcebergOrderSplitter:
    """冰山订单拆分：将大单拆为随机小量隐藏真实意图

    原理：每次只展示display_volume量，成交后自动补充下一笔，
    随机化display_volume防止被识别为冰山模式。
    """

    def __init__(self, avg_display_volume: int = 5,
                 randomize_factor: float = 0.5):
        self._avg_display = avg_display_volume
        self._randomize_factor = randomize_factor
        self._stats = {'total_splits': 0, 'total_volume': 0}

    def split(self, total_volume: int) -> List[int]:
        if total_volume <= 0:
            return []
        parts = []
        remaining = total_volume
        while remaining > 0:
            rand_mult = 1.0 + (random.random() * 2 - 1) * self._randomize_factor
            display = max(1, int(self._avg_display * rand_mult))
            take = min(remaining, display)
            parts.append(take)
            remaining -= take
        random.shuffle(parts)
        self._stats['total_splits'] += 1
        self._stats['total_volume'] += total_volume
        return parts


class TWAPSplitter:
    """TWAP时间加权拆分：将订单均匀分布在时间窗口内

    原理：在time_window_seconds内均匀分N笔发出，每笔volume/N，
    降低对市场的瞬时冲击。
    """

    def __init__(self, num_slices: int = 10,
                 time_window_seconds: float = 60.0,
                 randomize_timing: bool = True):
        self._num_slices = num_slices
        self._time_window = time_window_seconds
        self._randomize_timing = randomize_timing
        self._stats = {'total_twaps': 0}

    def split(self, total_volume: int) -> List[Dict[str, Any]]:
        if total_volume <= 0 or self._num_slices <= 0:
            return []
        slice_vol = total_volume // self._num_slices
        remainder = total_volume % self._num_slices
        slices = []
        for i in range(self._num_slices):
            vol = slice_vol + (1 if i < remainder else 0)
            if vol <= 0:
                continue
            base_delay = (i / self._num_slices) * self._time_window
            if self._randomize_timing:
                jitter = (random.random() - 0.5) * (self._time_window / self._num_slices * 0.3)
            else:
                jitter = 0.0
            slices.append({
                'volume': vol,
                'delay_seconds': max(0.0, base_delay + jitter),
                'slice_index': i,
            })
        self._stats['total_twaps'] += 1
        return slices
