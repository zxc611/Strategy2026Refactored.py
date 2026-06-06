"""
订单状态管理器 — 从order_service.py拆分
职责: 订单状态追踪、待处理订单扫描、超时检测
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
from collections import deque


class OrderStateManager:
    def __init__(self, max_pending: int = 1000):
        self._pending_orders: Dict[str, Dict[str, Any]] = {}
        self._order_timestamps: Dict[str, float] = {}
        self._max_pending = max_pending
        self._timeout_sec = 30.0

    def add_pending(self, order_id: str, order_info: Dict[str, Any]) -> None:
        self._pending_orders[order_id] = order_info
        self._order_timestamps[order_id] = time.time()

    def remove_pending(self, order_id: str) -> Optional[Dict[str, Any]]:
        self._order_timestamps.pop(order_id, None)
        return self._pending_orders.pop(order_id, None)

    def get_pending(self, order_id: str) -> Optional[Dict[str, Any]]:
        return self._pending_orders.get(order_id)

    def scan_timeouts(self, timeout_sec: Optional[float] = None) -> List[str]:
        _timeout = timeout_sec or self._timeout_sec
        _now = time.time()
        timed_out = []
        for oid, ts in list(self._order_timestamps.items()):
            if _now - ts > _timeout:
                timed_out.append(oid)
        return timed_out

    def check_pending_orders(self) -> List[str]:
        return self.scan_timeouts()

    @property
    def pending_count(self) -> int:
        return len(self._pending_orders)