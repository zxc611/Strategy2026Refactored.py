# [M1-47] ����״̬������
# MODULE_ID: M1-143
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
订单状态管理器 �?从order_service.py拆分
职责: 订单状态追踪、待处理订单扫描、超时检�?
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional
from collections import deque

from ali2026v3_trading.infra.shared_utils import CHINA_TZ


class OrderStateManager:
    def __init__(self, max_pending: int = 1000):
        self._pending_orders: Dict[str, Dict[str, Any]] = {}
        self._order_timestamps: Dict[str, float] = {}
        self._max_pending = max_pending
        self._timeout_sec = 30.0
        # 线程安全：调度器线程与主线程并发访问保护
        self._lock = threading.Lock()

    def add_pending(self, order_id: str, order_info: Dict[str, Any]) -> None:
        with self._lock:
            self._pending_orders[order_id] = order_info
            self._order_timestamps[order_id] = time.time()

    def remove_pending(self, order_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            self._order_timestamps.pop(order_id, None)
            return self._pending_orders.pop(order_id, None)

    def get_pending(self, order_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._pending_orders.get(order_id)

    def scan_timeouts(self, timeout_sec: Optional[float] = None) -> List[str]:
        _timeout = timeout_sec if timeout_sec is not None else self._timeout_sec
        _now = time.time()
        with self._lock:
            timed_out = [oid for oid, ts in self._order_timestamps.items() if _now - ts > _timeout]
        return timed_out

    def check_pending_orders(self) -> List[str]:
        return self.scan_timeouts()

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_orders)

    def on_trade_update(self, svc, trade_data: Any) -> None:
        from datetime import datetime
        from ali2026v3_trading.order.order_service import _validate_order_status_transition
        try:
            normalized = svc._normalize_platform_result(trade_data)
            oid = normalized.get('order_id', '')
            sts = normalized.get('status', '')
            filled = normalized.get('filled_volume', 0)
            with svc._lock:
                order = svc._orders_by_id.get(oid)
                if not order:
                    mapped_id = svc._platform_id_to_order_id.get(oid)
                    if mapped_id:
                        order = svc._orders_by_id.get(mapped_id)
                    if not order:
                        for o in svc._orders_by_id.values():
                            if o.get('platform_order_id') == oid:
                                order = o
                                break
                if order:
                    old_sts = order.get('status', '')
                    if not _validate_order_status_transition(old_sts, sts):
                        logging.warning(
                            "[R14-P0-BIZ-08] 非法订单状态转�? order_id=%s %s->%s, 拒绝更新",
                            oid, old_sts, sts,
                        )
                        return
                    order['status'] = sts
                    order['filled_volume'] = filled
                    order['updated_at'] = datetime.now(CHINA_TZ)
                    svc._order_state_audit_log.append({
                        'timestamp': time.time(), 'order_id': oid,
                        'old_status': order.get('status', ''), 'new_status': sts,
                    })
                    if len(svc._order_state_audit_log) > 1000:
                        svc._order_state_audit_log = svc._order_state_audit_log[-500:]
                    svc._order_last_update_time[oid] = time.time()
                    if sts in ('PARTIAL_FILLED', '部分成交') and filled >= order.get('volume', 0) and order.get('volume', 0) > 0:
                        order['status'] = 'FILLED'
                        logging.info("[R23-SM-P1-01-FIX] PARTIAL_FILLED→FILLED自动转移: order_id=%s filled=%d volume=%d", oid, filled, order.get('volume', 0))
                    elif sts in ('PARTIAL_FILLED', '部分成交') and 0 < filled < order.get('volume', 0):
                        _partial_ratio = filled / order.get('volume', 0)
                        logging.warning("[BIZ-P1-10] 订单部分成交: order_id=%s filled=%d/%d ratio=%.1f%%", oid, filled, order.get('volume', 0), _partial_ratio * 100)
                        try:
                            from ali2026v3_trading.infra.event_bus import get_global_event_bus
                            _bus = get_global_event_bus()
                            if _bus is not None:
                                _bus.publish('order.partial_filled', {'order_id': oid, 'filled_volume': filled, 'total_volume': order.get('volume', 0), 'partial_ratio': _partial_ratio}, async_mode=True)
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[R3-L2] suppressed exception", exc_info=True)
                            pass
                            pass
                    if sts in ('FILLED', 'ALL_FILLED', '全成') or order['status'] == 'FILLED':
                        svc._stats['successful_orders'] += 1
                        svc._release_margin_reservation(oid)
                        self.remove_pending(oid)
                    if sts in ('CANCELLED', 'FAILED'):
                        svc._release_margin_reservation(oid)
                        self.remove_pending(oid)
                    if sts == 'FAILED':
                        _retry_count = order.get('retry_count', 0)
                        _max_retries = 3
                        if _retry_count < _max_retries:
                            logging.info("[P1-R9-27] 订单FAILED, 触发自动重试: order=%s retry=%d/%d", oid, _retry_count + 1, _max_retries)
                            svc._chase_reorder(order, _retry_count + 1)
                        else:
                            logging.warning("[P1-R9-27] 订单FAILED且已达最大重试次�? order=%s retries=%d", oid, _retry_count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[R16-P2-8.5] on_trade_update异常: %s", e, exc_info=True)

    def scan_order_timeouts(self, svc) -> int:
        _before = self.pending_count
        self.check_pending_orders_full(svc)
        _after = self.pending_count
        _processed = _before - _after
        if _processed > 0:
            logging.info("[R23-SM-04-FIX] scan_order_timeouts: 处理�?d个超时订�?, _processed)
        return max(_processed, 0)

    def check_pending_orders_full(self, svc) -> None:
        from datetime import datetime
        now = datetime.now(CHINA_TZ)
        timeout_orders = []
        with svc._lock:
            expired_order_ids = []
            for order_id, order in list(svc._orders_by_id.items()):
                if order['status'] not in ('SUBMITTED', 'PENDING'):
                    self.remove_pending(order_id)
                    pid = order.get('platform_order_id')
                    if pid and str(pid) in svc._platform_id_to_order_id:
                        del svc._platform_id_to_order_id[str(pid)]
                    if order['status'] in ('FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', '全成', 'ORPHANED'):
                        elapsed = (now - order.get('updated_at', order['created_at'])).total_seconds()
                        if elapsed > 300:
                            expired_order_ids.append(order_id)
                    continue
                if not self.get_pending(order_id):
                    self.add_pending(order_id, order)
                elapsed = (now - order['created_at']).total_seconds()
                _order_action = order.get('action', '')
                _order_op_type = 'close' if _order_action in ('CLOSE', 'close', 'SELL', 'sell') else 'open'
                _order_timeout = svc._operation_timeouts.get(_order_op_type, svc._operation_timeouts['default'])
                if elapsed > _order_timeout:
                    timeout_orders.append((order_id, dict(order)))
            for oid in expired_order_ids:
                _expired_order = svc._orders_by_id.get(oid, {})
                _expired_key = f"{_expired_order.get('instrument_id', '')}_{_expired_order.get('direction', '')}_{_expired_order.get('action', '')}_{_expired_order.get('volume', '')}_{round(_expired_order.get('price', 0), 4)}"
                svc._order_idempotent_set.discard(_expired_key)
                del svc._orders_by_id[oid]
        for order_id, order_snapshot in timeout_orders:
            with svc._lock:
                current_order = svc._orders_by_id.get(order_id)
                if not current_order or current_order['status'] not in ('SUBMITTED', 'PENDING'):
                    self.remove_pending(order_id)
                    continue
                current_order['status'] = 'TIMEOUT'
                current_order['updated_at'] = datetime.now(CHINA_TZ)
                logging.info("[R23-SM-04-FIX] 订单超时标记TIMEOUT: order_id=%s elapsed=%.1fs", order_id, (datetime.now(CHINA_TZ) - current_order['created_at']).total_seconds())
            self.remove_pending(order_id)
            _cancel_ok = svc.cancel_order(order_id)
            if not _cancel_ok:
                logging.error("[R27-P0-FIX] 撤单失败,跳过追单避免重复持仓: order_id=%s", order_id)
                continue
            retry_count = current_order.get('retry_count', 0) if current_order else order_snapshot.get('retry_count', 0)
            if retry_count < svc._max_chase_retries:
                svc._chase_reorder(current_order or order_snapshot, retry_count + 1)

    def _cleanup_orders(self, svc) -> None:
        from datetime import datetime
        now = time.time()
        if now - svc._last_cleanup <= svc._cleanup_interval:
            return
        with svc._lock:
            svc._last_cleanup = now
            to_remove = [oid for oid, o in svc._orders_by_id.items()
                        if o['status'] in ('FILLED', 'CANCELLED', 'FAILED', 'ORPHANED')
                        and (datetime.now(CHINA_TZ) - o['updated_at']).total_seconds() > 3600]
            for oid in to_remove:
                _order = svc._orders_by_id.get(oid, {})
                svc._history_audit.append({
                    'order_id': oid, 'instrument_id': _order.get('instrument_id', ''),
                    'direction': _order.get('direction', ''), 'signal_id': _order.get('signal_id', ''),
                    'status': _order.get('status', ''), 'cleanup_time': time.time(),
                })
                del svc._orders_by_id[oid]
            for oid in to_remove:
                svc._wal_delete(oid)
            if to_remove:
                logging.info("[OrderService] 清理%d个已完成订单", len(to_remove))