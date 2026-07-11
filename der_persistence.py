# [M1-45] _____־û_

# MODULE_ID: M1-138

"""

订单持久化模块块WAL写前日志 + JSONL状态持久化 + 自成交检查+ 网络重试 + 部分成交

从order_service.py拆分，委托到order_wal_state_service.py

"""

from __future__ import annotations



import logging

import json

import os

import time

from typing import Any, Dict, List, Optional, Set, Tuple, Callable

from dataclasses import dataclass, field

from collections import defaultdict



from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads

from ali2026v3_trading.infra.shared_utils import atomic_replace_file, sanitize_filename

from ali2026v3_trading.infra._helpers import get_logger  # R9-5



logger = get_logger(__name__)  # R9-5





@dataclass(slots=True)

class OrderRecord:

    order_id: str

    instrument_id: str

    direction: str

    price: float

    volume: int

    timestamp: float

    status: str = "pending"

    action: str = "OPEN"  # FIX-R29: OPEN/CLOSE，自成交检测需区分





class SelfTradeDetector:

    def __init__(self):

        self._pending_orders: Dict[str, List[OrderRecord]] = defaultdict(list)

        self._self_trade_history: List[Dict[str, Any]] = []



    def add_order(self, order: OrderRecord) -> None:

        self._pending_orders[order.instrument_id].append(order)



    def check_self_trade(self, new_order: OrderRecord) -> Tuple[bool, Optional[str]]:

        # FIX-R29: CLOSE订单(平仓)与OPEN订单(开仓)不构成自交易，跳过检测
        if getattr(new_order, 'action', 'OPEN') == 'CLOSE':
            return False, None

        instrument = new_order.instrument_id

        opposite_dir = "sell" if new_order.direction == "buy" else "buy"

        for existing in self._pending_orders[instrument]:

            # FIX-R29: 已有订单是CLOSE(平仓)的，不构成自交易
            if getattr(existing, 'action', 'OPEN') == 'CLOSE':
                continue

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





# P2-12修复: NetworkRetryManager简化为BoundedRetry的薄包装，消除重复重试逻辑

class NetworkRetryManager:

    """网络重试管理器——委托到infra.resilience_retry.BoundedRetry统一实现"""



    def __init__(self, max_retries: int = 3, base_interval_sec: float = 2.0):

        self._max_retries = max_retries

        self._base_interval = base_interval_sec

        # P2-12修复: 移除独立的指retry_counts/_last_retry_time跟踪

        # 重试逻辑完全委托到BoundedRetry



    def execute_with_retry(self, operation_id: str, func, *args, **kwargs) -> Any:

        from ali2026v3_trading.infra.resilience import BoundedRetry as _BoundedRetry

        br = _BoundedRetry(

            max_retries=self._max_retries,

            base_delay=self._base_interval,

            max_delay=self._base_interval * (2 ** self._max_retries),

            on_exhausted="raise",

        )

        return br.execute(func, *args, **kwargs)



    # P2-12修复: 以下方法保留为兼容接口，内部委托到BoundedRetry

    def get_retry_interval(self, operation_id: str) -> float:

        return self._base_interval  # BoundedRetry内部管理指数退避



    def should_retry(self, operation_id: str) -> bool:

        return True  # BoundedRetry内部管理重试次数



    def record_retry(self, operation_id: str) -> None:

        pass  # BoundedRetry内部管理重试记录



    def reset_retry(self, operation_id: str) -> None:

        pass  # BoundedRetry内部管理重试状态





class PartialFillHandler:

    def __init__(self, timeout_sec: float = 300.0):

        self._timeout_sec = timeout_sec

        self._partial_fills: Dict[str, Dict[str, Any]] = {}



    def record_partial_fill(self, order_id: str, instrument_id: str,

                            traded_volume: int, total_volume: int,

                            timestamp: float = None) -> None:

        if timestamp is None:

            timestamp = time.time()

        remaining = total_volume - traded_volume

        self._partial_fills[order_id] = {

            "instrument_id": instrument_id,

            "traded_volume": traded_volume,

            "total_volume": total_volume,

            "remaining_volume": remaining,

            "timestamp": timestamp,

            "status": "partial" if remaining > 0 else "filled",

        }

        if remaining > 0:

            logger.info(

                "[PartialFill] order=%s instrument=%s traded=%d/%d remaining=%d",

                order_id, instrument_id, traded_volume, total_volume, remaining,

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

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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





class OrderPersistenceService:

    _ORDER_STATE_MAX_BYTES = 50 * 1024 * 1024

    _ORDER_STATE_BACKUP_COUNT = 3



    def __init__(self, wal_dir: str = "", idempotent_file: str = "", state_file: str = ""):

        self._wal_dir = wal_dir

        self._idempotent_state_file = idempotent_file

        self._order_state_file = state_file

        self._order_idempotent_set: Set[str] = set()

        self._self_trade_detector = SelfTradeDetector()

        self._network_retry = NetworkRetryManager()

        self._partial_fill_handler = PartialFillHandler()



    def ensure_wal_dir(self) -> None:

        if self._wal_dir:

            os.makedirs(self._wal_dir, exist_ok=True)



    def wal_path(self, order_id: str) -> str:

        safe_id = sanitize_filename(order_id)  # R2-3修复: 使用统一sanitize_filename

        return os.path.join(self._wal_dir, f"{safe_id}.wal")



    def wal_read(self, order_id: str) -> Optional[Dict]:

        try:

            path = self.wal_path(order_id)

            if os.path.exists(path):

                with open(path, 'r', encoding='utf-8') as f:

                    return json_loads(f.read())  # R3-2修复: 使用统一json_loads

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.warning("[OrderPersistence] WAL读取失败: order=%s err=%s", order_id, e)

        return None



    def wal_delete(self, order_id: str) -> None:

        try:

            path = self.wal_path(order_id)

            if os.path.exists(path):

                os.remove(path)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.warning("[OrderPersistence] WAL删除失败: order=%s err=%s", order_id, e)



    # P2-01修复: 委托到infra/serialization_utils.py的公共函数

    def rotate_jsonl_if_needed(self, filepath: str) -> None:

        from ali2026v3_trading.infra.serialization_utils import rotate_jsonl_if_needed as _rotate

        _rotate(filepath, self._ORDER_STATE_MAX_BYTES, self._ORDER_STATE_BACKUP_COUNT)



    def recover_order_state(self) -> Dict[str, Dict]:

        recovered = {}

        try:

            if not self._order_state_file or not os.path.exists(self._order_state_file):

                return recovered

            with open(self._order_state_file, 'r', encoding='utf-8') as f:

                for line in f:

                    line = line.strip()

                    if not line:

                        continue

                    try:

                        record = json_loads(line)

                        order_id = record.get('order_id', '')

                        if order_id:

                            recovered[order_id] = record

                    except (json.JSONDecodeError, KeyError):

                        continue

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.warning("[OrderPersistence] 订单状态恢复失败 %s", e)

        return recovered



    def recover_idempotent_state(self) -> Set[str]:

        try:

            if not self._idempotent_state_file or not os.path.exists(self._idempotent_state_file):

                return self._order_idempotent_set

            with open(self._idempotent_state_file, 'r', encoding='utf-8') as f:

                for line in f:

                    line = line.strip()

                    if not line:

                        continue

                    try:

                        record = json_loads(line)

                        key = record.get('key', '')

                        if key:

                            self._order_idempotent_set.add(key)

                    except (json.JSONDecodeError, KeyError):

                        continue

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.warning("[OrderPersistence] 幂等状态恢复失败 %s", e)

        return self._order_idempotent_set



    def recover_orphaned_orders(self, orders_by_id: Dict[str, Dict]) -> int:

        orphaned_count = 0

        try:

            if not self._wal_dir or not os.path.isdir(self._wal_dir):

                return 0

            for fname in os.listdir(self._wal_dir):

                if not fname.endswith('.wal'):

                    continue

                fpath = os.path.join(self._wal_dir, fname)

                try:

                    with open(fpath, 'r', encoding='utf-8') as f:

                        entry = json_loads(f.read())  # R3-2修复

                    if entry.get('state') == 'PENDING':

                        order_id = entry.get('order_id', '')

                        order = orders_by_id.get(order_id)

                        if order and order.get('status') in ('SUBMITTED', 'PENDING'):

                            order['status'] = 'ORPHANED'

                            orphaned_count += 1

                            try:

                                _owal_path = self.wal_path(order_id)

                                _owal_res = atomic_replace_file(_owal_path, json_dumps({'order_id': order_id, 'state': 'ORPHANED', 'timestamp': time.time()}))

                                if not _owal_res['success']:

                                    raise RuntimeError(_owal_res.get('error', 'atomic_replace_file failed'))

                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _owal_err:

                                logger.error("[OrderPersistence] WAL写入失败: order=%s state=ORPHANED err=%s", order_id, _owal_err)

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    logger.warning("[OrderPersistence] WAL文件恢复异常: %s err=%s", fname, e)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.warning("[OrderPersistence] 孤儿订单恢复过程异常: %s", e)

        return orphaned_count



    @property

    def self_trade_detector(self) -> SelfTradeDetector:

        return self._self_trade_detector



    @property

    def network_retry(self) -> NetworkRetryManager:

        return self._network_retry



    @property

    def partial_fill_handler(self) -> PartialFillHandler:

        return self._partial_fill_handler

