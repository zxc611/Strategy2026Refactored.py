import logging
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class OrderRecord:
    order_id: str
    instrument_id: str
    direction: str
    price: float
    volume: int
    timestamp: float
    status: str = "pending"


class SelfTradeDetector:
    def __init__(self):
        self._pending_orders: Dict[str, List[OrderRecord]] = defaultdict(list)
        self._self_trade_history: List[Dict[str, Any]] = []

    def add_order(self, order: OrderRecord) -> None:
        self._pending_orders[order.instrument_id].append(order)

    def check_self_trade(self, new_order: OrderRecord) -> Tuple[bool, Optional[str]]:
        instrument = new_order.instrument_id
        opposite_dir = "sell" if new_order.direction == "buy" else "buy"
        
        for existing in self._pending_orders[instrument]:
            if existing.direction == opposite_dir:
                price_match = (
                    (new_order.direction == "buy" and new_order.price >= existing.price) or
                    (new_order.direction == "sell" and new_order.price <= existing.price)
                )
                if price_match:
                    alert_msg = (
                        f"[SELF-TRADE-ALERT] instrument={instrument} "
                        f"new_order={new_order.order_id}({new_order.direction}@{new_order.price}) "
                        f"vs existing={existing.order_id}({existing.direction}@{existing.price})"
                    )
                    logger.error(alert_msg)
                    self._self_trade_history.append({
                        "timestamp": time.time(),
                        "instrument_id": instrument,
                        "new_order_id": new_order.order_id,
                        "existing_order_id": existing.order_id,
                        "alert": alert_msg,
                    })
                    return True, alert_msg
        return False, None

    def remove_order(self, order_id: str) -> None:
        for instrument, orders in self._pending_orders.items():
            self._pending_orders[instrument] = [o for o in orders if o.order_id != order_id]

    def get_self_trade_history(self) -> List[Dict[str, Any]]:
        return list(self._self_trade_history)


class NetworkRetryManager:
    def __init__(self, max_retries: int = 3, base_interval_sec: float = 2.0):
        self._max_retries = max_retries
        self._base_interval = base_interval_sec
        self._retry_counts: Dict[str, int] = defaultdict(int)
        self._last_retry_time: Dict[str, float] = {}

    def get_retry_interval(self, operation_id: str) -> float:
        count = self._retry_counts.get(operation_id, 0)
        return self._base_interval * (2 ** count)

    def should_retry(self, operation_id: str) -> bool:
        return self._retry_counts.get(operation_id, 0) < self._max_retries

    def record_retry(self, operation_id: str) -> None:
        self._retry_counts[operation_id] += 1
        self._last_retry_time[operation_id] = time.time()

    def reset_retry(self, operation_id: str) -> None:
        self._retry_counts[operation_id] = 0
        self._last_retry_time.pop(operation_id, None)

    def execute_with_retry(self, operation_id: str, func, *args, **kwargs) -> Any:
        while self.should_retry(operation_id):
            try:
                result = func(*args, **kwargs)
                self.reset_retry(operation_id)
                return result
            except Exception as e:
                self.record_retry(operation_id)
                interval = self.get_retry_interval(operation_id)
                logger.warning(
                    "[NetworkRetry] operation=%s retry=%d/%d interval=%.1fs error=%s",
                    operation_id, self._retry_counts[operation_id], self._max_retries, interval, e,
                )
                if not self.should_retry(operation_id):
                    raise
                time.sleep(interval)
        return None


class PartialFillHandler:
    def __init__(self, timeout_sec: float = 300.0):
        self._timeout_sec = timeout_sec
        self._partial_fills: Dict[str, Dict[str, Any]] = {}

    def record_partial_fill(self, order_id: str, instrument_id: str,
                            filled_volume: int, total_volume: int,
                            timestamp: float = None) -> None:
        if timestamp is None:
            timestamp = time.time()
        remaining = total_volume - filled_volume
        self._partial_fills[order_id] = {
            "instrument_id": instrument_id,
            "filled_volume": filled_volume,
            "total_volume": total_volume,
            "remaining_volume": remaining,
            "timestamp": timestamp,
            "status": "partial" if remaining > 0 else "filled",
        }
        if remaining > 0:
            logger.info(
                "[PartialFill] order=%s instrument=%s filled=%d/%d remaining=%d",
                order_id, instrument_id, filled_volume, total_volume, remaining,
            )

    def check_and_cancel_remaining(self, order_id: str,
                                    cancel_func = None) -> Dict[str, Any]:
        record = self._partial_fills.get(order_id)
        if record is None:
            return {"cancelled": False, "reason": "order_not_found"}
        
        elapsed = time.time() - record["timestamp"]
        if elapsed > self._timeout_sec and record["remaining_volume"] > 0:
            if cancel_func is not None:
                try:
                    cancel_func(order_id)
                except Exception as e:
                    logger.error("[PartialFill] cancel failed: %s", e)
            record["status"] = "cancelled"
            logger.warning(
                "[PartialFill] order=%s timeout=%.1fs > %.1fs remaining=%d cancelled",
                order_id, elapsed, self._timeout_sec, record["remaining_volume"],
            )
            return {
                "cancelled": True,
                "reason": "timeout",
                "remaining_volume": record["remaining_volume"],
                "elapsed_sec": elapsed,
            }
        return {"cancelled": False, "reason": "not_timeout_or_filled"}

    def get_pending_partial_fills(self) -> Dict[str, Dict[str, Any]]:
        return {
            oid: rec for oid, rec in self._partial_fills.items()
            if rec["status"] == "partial"
        }
