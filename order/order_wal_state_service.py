# [M1-49] WALдǰ__־

# MODULE_ID: M1-145

"""

order_wal_state_service.py - OrderWALStateService

R26: 从order_service.py提取的WAL写前日志+状态持久化方法



职责任

- WAL写前日志 (_ensure_wal_dir, _wal_path, _wal_write, _wal_read, _wal_delete, _recover_orphaned_orders)

- 状态持久化 (_persist_idempotent_key, _recover_idempotent_state, _rotate_jsonl_if_needed, _append_order_state, _recover_order_state)

- 补偿事务 (_execute_with_compensation_v2)

"""

from __future__ import annotations



import json

import logging

import os

import time

from datetime import datetime

from typing import Any, Callable, Dict, List, Optional



from infra.serialization_utils import json_dumps, json_loads

from infra.serialization_utils import safe_jsonl_append_line

from infra.resilience import is_disk_full_error

from infra.shared_utils import atomic_replace_file, sanitize_filename



from infra.shared_utils import CHINA_TZ





class OrderWALStateService:

    _ORDER_STATE_MAX_BYTES = 50 * 1024 * 1024

    _ORDER_STATE_BACKUP_COUNT = 3



    def __init__(self, provider):

        self._provider = provider

        self._persistence = None

        try:

            from order.order_persistence import OrderPersistenceService

            wal_dir = getattr(provider, '_wal_dir', 'orders_wal')

            idempotent_file = getattr(provider, '_idempotent_state_file', 'idempotent_state.jsonl')

            state_file = getattr(provider, '_order_state_file', 'order_state.jsonl')

            self._persistence = OrderPersistenceService(wal_dir, idempotent_file, state_file)

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] suppressed exception", exc_info=True)

            pass

            pass



    def _ensure_wal_dir(self) -> None:

        try:

            os.makedirs(self._provider._wal_dir, exist_ok=True)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[OrderService] R25-TO-03-FIX: WAL目录创建失败: %s", e)



    def _wal_path(self, order_id: str) -> str:

        safe_id = sanitize_filename(order_id)  # R2-3修复: 使用统一sanitize_filename

        return os.path.join(self._provider._wal_dir, f"{safe_id}.wal")



    def _wal_write(self, order_id: str, state: str, order: Dict) -> None:

        try:

            entry = {

                'order_id': order_id,

                'state': state,

                'instrument_id': order.get('instrument_id', ''),

                'direction': order.get('direction', ''),

                'volume': order.get('volume', 0),

                'price': order.get('price', 0),

                # FIX-R32-ACTION-PERSIST: 必须保存action字段，否则重启后订单丢失action，
                # 导致on_trade无法判断开仓/平仓，_reset_closing_flag_on_order_failure无法识别CLOSE订单，
                # 自成交检测_has_close检查失败无法回退到_reduce_position
                'action': order.get('action', ''),

                'timestamp': time.time(),

                'datetime': datetime.now(CHINA_TZ).isoformat(),

            }

            _wal_path = self._wal_path(order_id)

            # P2-22修复: 使用 atomic_replace_file 替代内联 os.replace

            _result = atomic_replace_file(_wal_path, json_dumps(entry))

            if not _result['success']:

                raise RuntimeError(_result.get('error', 'atomic_replace_file failed'))

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[OrderService] R25-TO-03-FIX: WAL写入失败: order=%s state=%s err=%s", order_id, state, e)

            if is_disk_full_error(e):

                logging.critical("[R33-P1-16] WAL写入失败: 磁盘满ENOSPC)! 订单持久化不可靠! err=%s", e)

                if not hasattr(self._provider, '_disk_full_mode'):

                    self._provider._disk_full_mode = True

                    logging.critical("[R33-P1-16] 进入磁盘满降级模式，后续订单仅内存暂停")
            if not hasattr(self._provider, '_wal_write_fail_count'):

                self._provider._wal_write_fail_count = 0

            self._provider._wal_write_fail_count += 1

            if self._provider._wal_write_fail_count >= 10:

                logging.critical("[R31-P1-10] WAL连续写入失败%d次，订单持久化不可靠!", self._provider._wal_write_fail_count)



    def _wal_read(self, order_id: str) -> Optional[Dict]:

        try:

            path = self._wal_path(order_id)

            if os.path.exists(path):

                with open(path, 'r', encoding='utf-8') as f:

                    return json_loads(f.read())  # R3-2修复: 使用统一json_loads

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[OrderService] R25-TO-03-FIX: WAL读取失败: order=%s err=%s", order_id, e)

        return None



    def _wal_delete(self, order_id: str) -> None:

        try:

            path = self._wal_path(order_id)

            if os.path.exists(path):

                os.remove(path)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[OrderService] R25-TO-03-FIX: WAL删除失败: order=%s err=%s", order_id, e)



    def _recover_orphaned_orders(self) -> None:

        try:

            if not os.path.isdir(self._provider._wal_dir):

                return

            orphaned_count = 0

            for fname in os.listdir(self._provider._wal_dir):

                if not fname.endswith('.wal'):

                    continue

                fpath = os.path.join(self._provider._wal_dir, fname)

                try:

                    with open(fpath, 'r', encoding='utf-8') as f:

                        entry = json_loads(f.read())  # R3-2修复

                    if entry.get('state') == 'PENDING':

                        order_id = entry.get('order_id', '')

                        with self._provider._lock:

                            order = self._provider._orders_by_id.get(order_id)

                            if order and order.get('status') in ('SUBMITTED', 'PENDING'):

                                order['status'] = 'ORPHANED'

                                order['updated_at'] = datetime.now(CHINA_TZ)

                                orphaned_count += 1

                                logging.warning(

                                    "[OrderService] R25-TO-03-FIX: 孤儿订单恢复: order_id=%s instrument=%s "

                                    "状态从SUBMITTED/PENDING标记为ORPHANED",

                                    order_id, entry.get('instrument_id', ''),

                                )

                        self._wal_write(order_id, 'ORPHANED', {'order_id': order_id, 'instrument_id': entry.get('instrument_id', '')})

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    logging.warning("[OrderService] R25-TO-03-FIX: WAL文件恢复异常: %s err=%s", fname, e)

            if orphaned_count > 0:

                logging.info("[OrderService] R25-TO-03-FIX: 启动时恢复d个孤儿订单", orphaned_count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[OrderService] R25-TO-03-FIX: 孤儿订单恢复过程异常: %s", e)



    def _persist_idempotent_key(self, key: str) -> None:

        try:

            with self._provider._idempotent_lock:

                with open(self._provider._idempotent_state_file, 'a', encoding='utf-8') as f:

                    safe_jsonl_append_line(f, {'key': key, 'ts': time.time()})

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[R16-P0-RES-02] 幂等键持久化失败: %s", e)



    def _recover_idempotent_state(self) -> None:

        try:

            if not os.path.exists(self._provider._idempotent_state_file):

                return

            recovered = 0

            with open(self._provider._idempotent_state_file, 'r', encoding='utf-8') as f:

                for line in f:

                    line = line.strip()

                    if not line:

                        continue

                    try:

                        record = json_loads(line)

                        key = record.get('key', '')

                        if key:

                            self._provider._order_idempotent_set.add(key)

                            recovered += 1

                    except (json.JSONDecodeError, KeyError):

                        continue

            if recovered > 0:

                logging.info("[R16-P0-RES-02] 幂等去重集合已恢复 %d条记录", recovered)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[R16-P0-RES-02] 幂等状态恢复失败 %s", e)



    # P2-01修复: 委托到infra/serialization_utils.py的公共函数

    def _rotate_jsonl_if_needed(self, filepath: str) -> None:

        from infra.serialization_utils import rotate_jsonl_if_needed as _rotate

        _rotate(filepath, self._ORDER_STATE_MAX_BYTES, self._ORDER_STATE_BACKUP_COUNT)



    def _append_order_state(self, order_id: str, state: str, order: Dict) -> None:

        try:

            record = {

                'order_id': order_id,

                'state': state,

                'instrument_id': order.get('instrument_id', ''),

                'direction': order.get('direction', ''),

                'volume': order.get('volume', 0),

                'price': order.get('price', 0),

                # FIX-R32-ACTION-PERSIST: 必须保存action字段，否则重启后订单丢失action
                'action': order.get('action', ''),

                # FIX-R37-PID-PERSIST: 持久化platform_order_id，重启后可重建platform_id→internal_id映射，
                # 避免on_order/on_trade回调时因映射丢失而退化为instrument模糊匹配(导致错单)
                'platform_order_id': order.get('platform_order_id', ''),

                'ts': time.time(),

            }

            with self._provider._order_state_lock:

                self._rotate_jsonl_if_needed(self._provider._order_state_file)

                with open(self._provider._order_state_file, 'a', encoding='utf-8') as f:

                    safe_jsonl_append_line(f, record)

            self._provider._append_state_fail_count = 0

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._provider._append_state_fail_count = getattr(self._provider, '_append_state_fail_count', 0) + 1

            logging.error("[R33-P1-10] 订单状态追加写失败(连续%d次): order=%s state=%s err=%s",

                          self._provider._append_state_fail_count, order_id, state, e)

            if is_disk_full_error(e):

                logging.critical("[R33-P1-16] 订单状态追加写失败: 磁盘满ENOSPC)! err=%s", e)

                if not hasattr(self._provider, '_disk_full_mode'):

                    self._provider._disk_full_mode = True

                    logging.critical("[R33-P1-16] 进入磁盘满降级模式，后续订单仅内存暂停")
            _threshold = getattr(self._provider, '_append_state_fail_critical_threshold', 10)

            if self._provider._append_state_fail_count >= _threshold:

                logging.critical(
                    "[R33-P1-10] CRITICAL: 订单状态追加写已连续失败%d，阈值%d，" "WAL写入链路可能已损坏，订单状态持久化丢失风险，", self._provider._append_state_fail_count, _threshold)


    def _recover_order_state(self) -> None:

        try:

            if not os.path.exists(self._provider._order_state_file):

                return

            recovered = 0

            with open(self._provider._order_state_file, 'r', encoding='utf-8') as f:

                for line in f:

                    line = line.strip()

                    if not line:

                        continue

                    try:

                        record = json_loads(line)

                        order_id = record.get('order_id', '')

                        state = record.get('state', '')

                        instrument_id = record.get('instrument_id', '')

                        if order_id and state:

                            with self._provider._lock:

                                if order_id not in self._provider._orders_by_id:

                                    self._provider._orders_by_id[order_id] = {

                                        'order_id': order_id,

                                        'instrument_id': instrument_id,

                                        'direction': record.get('direction', ''),

                                        'volume': record.get('volume', 0),

                                        'price': record.get('price', 0),

                                        'status': state,

                                        # FIX-R32-ACTION-PERSIST: 恢复action字段，否则重启后订单丢失action
                                        'action': record.get('action', ''),

                                        # FIX-R37-PID-PERSIST: 恢复platform_order_id字段
                                        'platform_order_id': record.get('platform_order_id', ''),

                                    }

                                # FIX-R37-PID-PERSIST: 重建 platform_id→internal_id 映射
                                # 重启后_platform_id_to_order_id为空，此处从恢复的订单中重建映射，
                                # 避免on_order/on_trade回调时退化为instrument模糊匹配(导致错单)
                                _pid = record.get('platform_order_id', '') or self._provider._orders_by_id[order_id].get('platform_order_id', '')
                                if _pid and str(_pid) != order_id and not str(_pid).startswith('ORD_'):
                                    self._provider._platform_id_to_order_id[str(_pid)] = order_id

                            recovered += 1

                    except (json.JSONDecodeError, KeyError):

                        continue

            if recovered > 0:

                _pid_count = len(self._provider._platform_id_to_order_id)

                logging.info("[R16-P0-RES-05] 订单状态已从JSONL恢复: %d条记录, platform_id映射重建: %d条", recovered, _pid_count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[R16-P0-RES-05] 订单状态恢复失败 %s", e)



    def _execute_with_compensation_v2(

        self,

        steps: List[Dict[str, Any]],

        result_ids: List[str],

        compensate_fn: Optional[Callable] = None,

    ) -> List[str]:

        executed_ids: List[str] = []

        for i, step_params in enumerate(steps):

            result = self._provider.send_order(**step_params)

            if hasattr(result, 'order_id') and result.order_id:

                executed_ids.append(result.order_id)

                result_ids.append(result.order_id)

            else:

                logging.error("[R16-P0-RES-11] 补偿事务时d步失败，开始逆序撤单", i + 1)

                for oid in reversed(executed_ids):

                    try:

                        if compensate_fn:

                            compensate_fn(oid)

                        else:

                            with self._provider._lock:

                                order = self._provider._orders_by_id.get(oid)

                                if order:

                                    order['status'] = 'COMPENSATED'

                                    self._append_order_state(oid, 'COMPENSATED', order)

                            logging.info("[R16-P0-RES-11] 补偿撤单: %s", oid)

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as ce:

                        logging.error("[R16-P0-RES-11] 补偿撤单失败: %s err=%s", oid, ce)

                return executed_ids

        return executed_ids



    def remove_order_and_idempotent_key(self, provider, order_id: str, order: Dict) -> None:

        # FIX-R28: CLOSE订单的idempotent_key含signal_id，需同步构造以正确移除
        _action = order.get('action', '')
        if _action in ('CLOSE', 'close'):
            _idempotent_key = f"{order.get('instrument_id', '')}_{order.get('direction', '')}_{_action}_{order.get('volume', '')}_{round(order.get('price', 0), 4)}_{order.get('signal_id', '')}"
        else:
            _idempotent_key = f"{order.get('instrument_id', '')}_{order.get('direction', '')}_{_action}_{order.get('volume', '')}_{round(order.get('price', 0), 4)}"

        provider._order_idempotent_set.discard(_idempotent_key)

        provider._orders_by_id.pop(order_id, None)

