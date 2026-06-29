# MODULE_ID: M1-041
"""
storage_async_writer_mixin.py — 异步写入器服务
包含: StorageAsyncWriterService

重构说明 (2026-06-11):
- _StorageAsyncWriterMixin → StorageAsyncWriterService（服务提取，消灭Mixin）
"""

from ali2026v3_trading.infra.shared_utils import InitPhase, requires_phase, ShardRouter, extract_product_code
from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
import logging
import os
import threading
import time
import json
import queue
import sys
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)


class StorageAsyncWriterService:

    def __init__(self, lock=None, data_service=None, params_service=None,
                 maintenance_service=None):
        self._lock = lock or threading.Lock()
        self._data_service = data_service
        self._params_service = params_service
        self._maintenance_service = maintenance_service

    @staticmethod
    def _pc_to_shard_idx(inst_id: str, shard_mask: int) -> int:
        _pc_raw = extract_product_code(inst_id)
        pc = _pc_raw.lower() if _pc_raw else ''
        return ShardRouter._deterministic_hash(pc) & shard_mask if pc else 0

    def _get_shard_index_for_task(self, func_name: str, args: tuple) -> int:
        _SHARD_MASK = self._TICK_SHARD_COUNT - 1
        if func_name.startswith('_save_tick') and args:
            internal_id = args[0] if args else 0
            if isinstance(internal_id, (int, float)):
                info = self._get_info_by_id(int(internal_id))
                if info:
                    shard_key = info.get('shard_key', -1)
                    if isinstance(shard_key, int) and shard_key >= 0:
                        return shard_key & _SHARD_MASK
                    inst_id = info.get('instrument_id', '')
                    if inst_id:
                        return self._pc_to_shard_idx(inst_id, _SHARD_MASK)
            return 0
        return 0

    def _start_async_writer(self):
        _SHARD_WRITER_ASSIGN = [
            [0, 6, 12], [1, 7, 13], [2, 8, 14],
            [3, 9, 15], [4, 10], [5, 11],
        ]

        if self._pending_on_stop_data:
            recovered = 0
            for task in self._pending_on_stop_data:
                try:
                    func_name = task[0]
                    args = task[1] if len(task) > 1 else ()
                    if func_name.startswith('_save_kline'):
                        target_queue = self._kline_queue
                    elif func_name.startswith(('_save_signal', '_save_underlying', '_save_kv',
                                               '_save_depth_batch', '_save_option_snapshot_batch')):
                        target_queue = self._maintenance_queue
                    else:
                        si = self._get_shard_index_for_task(func_name, args)
                        target_queue = self._tick_shard_queues[si]
                    target_queue.put_nowait(task)
                    recovered += 1
                except queue.Full:
                    logging.warning("[AsyncWriter] 恢复停机数据时队列满，丢弃: %s", task[0] if task else '?')
                    break
            self._pending_on_stop_data.clear()
            self._spill_wal_clear()
            logging.info("[AsyncWriter] 恢复停机数据 %d 条", recovered)

        thread_configs = []
        for wi in range(self._TICK_WRITER_COUNT):
            shard_ids = _SHARD_WRITER_ASSIGN[wi]
            t = threading.Thread(
                target=self._async_shard_writer_loop,
                args=(shard_ids, 200, f"TickWriter-{wi}", wi),
                name=f"TickWriter-{wi}",
                daemon=False
            )
            thread_configs.append((f"TickWriter-{wi}", t, False))
            self._tick_shard_writers.append(t)
            logging.info("[AsyncWriter] TickWriter-%d 创建(shards=%s)", wi, shard_ids)

        self._kline_writer_thread = threading.Thread(
            target=self._async_writer_loop,
            args=(self._kline_queue, 200, "KlineWriter"),
            name="KlineWriter",
            daemon=False
        )
        thread_configs.append(("KlineWriter", self._kline_writer_thread, False))

        self._maintenance_writer_thread = threading.Thread(
            target=self._async_writer_loop,
            args=(self._maintenance_queue, 100, "MaintenanceWriter"),
            name="MaintenanceWriter",
            daemon=False
        )
        thread_configs.append(("MaintenanceWriter", self._maintenance_writer_thread, False))

        import atexit
        atexit.register(self._shutdown_impl, flush=True)

        self._thread_mgr.start_all(thread_configs)
        logging.info("[AsyncWriter] 16+6+1+1 Shard写入线程已启动(daemon=False)")
        logging.info("[ShardRouter] 通道绑定算法=md5取模 shard_count=%d", self._TICK_SHARD_COUNT)

    def _stop_async_writer(self):
        self._stop_event.set()

        def _drain_queue(q, label):
            saved = 0
            while not q.empty():
                try:
                    task = q.get_nowait()
                    with self._pending_data_lock:
                        if len(self._pending_on_stop_data) < self._spill_wal_max_entries:
                            self._pending_on_stop_data.append(task)
                            self._spill_wal_append(task)
                            saved += 1
                        else:
                            logging.warning("[AsyncWriter] _pending_on_stop_data已达上限%d, 丢弃", self._spill_wal_max_entries)
                            with self._queue_stats_lock:
                                self._queue_stats['drops_count'] += 1
                except queue.Empty:
                    break
            if saved:
                logging.info("[AsyncWriter] %s 停止时保存 %d 条到内存+WAL", label, saved)
            return saved

        total_saved = 0

        for wi, t in enumerate(self._tick_shard_writers):
            if t and t.is_alive():
                t.join(timeout=10.0)
                if t.is_alive():
                    logging.warning("[AsyncWriter] TickWriter-%d 10秒未退出", wi)
                else:
                    logging.info("[AsyncWriter] TickWriter-%d 已停止", wi)

        for si in range(self._TICK_SHARD_COUNT):
            total_saved += _drain_queue(self._tick_shard_queues[si], f"TickShard-{si}")

        if self._kline_writer_thread and self._kline_writer_thread.is_alive():
            self._kline_writer_thread.join(timeout=10.0)
            if self._kline_writer_thread.is_alive():
                logging.warning("[AsyncWriter] KlineWriter 10秒未退出")
            else:
                logging.info("[AsyncWriter] KlineWriter 已停止")
        total_saved += _drain_queue(self._kline_queue, "Kline")

        if self._maintenance_writer_thread and self._maintenance_writer_thread.is_alive():
            self._maintenance_writer_thread.join(timeout=10.0)
            if self._maintenance_writer_thread.is_alive():
                logging.warning("[AsyncWriter] MaintenanceWriter 10秒未退出")
            else:
                logging.info("[AsyncWriter] MaintenanceWriter 已停止")
        total_saved += _drain_queue(self._maintenance_queue, "Maintenance")

        if total_saved:
            logging.info("[AsyncWriter] 停止时共保存 %d 条任务到内存", total_saved)

    def _async_shard_writer_loop(self, shard_indices: List[int], batch_tasks: int, name: str, writer_idx: int = 0):  # [R22-P2-TS17]
        batch = []
        idx = 0
        while not self._stop_event.is_set():
            try:
                for _ in range(len(shard_indices)):
                    si = shard_indices[idx % len(shard_indices)]
                    idx += 1
                    try:
                        item = self._tick_shard_queues[si].get_nowait()
                        batch.append(item)
                    except queue.Empty:
                        pass

                if len(batch) >= batch_tasks or (batch and all(
                    self._tick_shard_queues[si].empty() for si in shard_indices
                )):
                    written_count = self._flush_batch_to_db(batch, writer_idx=writer_idx)
                    if written_count is None:
                        time.sleep(min(max(self.retry_delay, 0.1), 1.0))
                        continue
                    with self._queue_stats_lock:
                        self._queue_stats['total_written'] += written_count
                    batch.clear()
                    self._batch_retry_count = 0
                else:
                    time.sleep(0.001)

            except Exception as e:
                logging.error("[AsyncWriter][%s] 写入异常：%s", name, e, exc_info=True)
                self._batch_retry_count = getattr(self, '_batch_retry_count', 0) + 1
                if self._batch_retry_count > 3:
                    lost_count = len(batch)
                    logging.critical("[DATA_LOSS][%s] batch重试超限，丢弃%d条数据", name, lost_count)
                    batch.clear()
                    self._batch_retry_count = 0

        remaining = list(batch)
        for si in shard_indices:
            while not self._tick_shard_queues[si].empty():
                try:
                    remaining.append(self._tick_shard_queues[si].get_nowait())
                except queue.Empty:
                    break
        if remaining:
            with self._pending_data_lock:
                self._pending_on_stop_data.extend(remaining)
            logging.info("[AsyncWriter][%s] 停止时保存 %d 条余下数据到内存", name, len(remaining))

    def _async_writer_loop(self, task_queue, batch_tasks, name):
        batch = []
        while not self._stop_event.is_set():
            try:
                try:
                    item = task_queue.get(timeout=0.1)
                    batch.append(item)
                except queue.Empty:
                    pass

                if len(batch) >= batch_tasks or (batch and task_queue.empty()):
                    written_count = self._flush_batch_to_db(batch)
                    if written_count is None:
                        time.sleep(min(max(self.retry_delay, 0.1), 1.0))
                        continue
                    with self._queue_stats_lock:
                        self._queue_stats['total_written'] += written_count
                    batch.clear()
                    self._batch_retry_count = 0

            except Exception as e:
                logging.error("[AsyncWriter][%s] 写入异常：%s", name, e, exc_info=True)
                self._batch_retry_count = getattr(self, '_batch_retry_count', 0) + 1
                if self._batch_retry_count > 3:
                    lost_count = len(batch)
                    logging.critical("[DATA_LOSS][%s] batch重试超限，丢弃%d条数据", name, lost_count)
                    batch.clear()
                    self._batch_retry_count = 0

        remaining = list(batch)
        while not task_queue.empty():
            try:
                remaining.append(task_queue.get_nowait())
            except queue.Empty:
                break
        if remaining:
            with self._pending_data_lock:
                self._pending_on_stop_data.extend(remaining)
            logging.info("[AsyncWriter][%s] 停止时保存 %d 条余下数据到内存", name, len(remaining))

    def _enqueue_write(self, func_name: str, *args, **kwargs) -> bool:
        if self._stop_event.is_set():
            logging.warning("写入线程已停止，拒绝新任务：%s", func_name)
            return False
        with self._queue_stats_lock:
            self._queue_stats['total_received'] += 1

        task = (func_name, args, kwargs)

        if func_name.startswith('_save_kline'):
            target_queue = self._kline_queue
            channel = 'Kline'
            si = -1
        elif func_name.startswith(('_save_signal', '_save_underlying', '_save_kv',
                                   '_save_depth_batch', '_save_option_snapshot_batch')):
            target_queue = self._maintenance_queue
            channel = 'Maintenance'
            si = -1
        else:
            si = self._get_shard_index_for_task(func_name, args)
            target_queue = self._tick_shard_queues[si]
            channel = f'TickShard-{si}'

        try:
            try:
                target_queue.put(task, block=True, timeout=0.5)

                if func_name == '_save_tick_impl' and args:
                    internal_id = args[0] if args else None
                    if internal_id:
                        _inst_cache = self._params_service.get_all_instrument_cache()
                        for inst_id, meta in _inst_cache.items():
                            if meta.get('internal_id') == internal_id:
                                DiagnosisProbeManager.on_storage_enqueue(inst_id, True, shard_idx=si)
                                break

                queue_size = target_queue.qsize()
                max_size = target_queue.maxsize
                fill_rate = queue_size / max_size * 100

                with self._queue_stats_lock:
                    if queue_size > self._queue_stats['max_queue_size_seen']:
                        self._queue_stats['max_queue_size_seen'] = queue_size

                if fill_rate > 80:
                    logging.warning("队列使用率 %.1f%% (%d/%d) 通道=%s 方法=%s",
                                    fill_rate, queue_size, max_size,
                                    channel, func_name)

                if fill_rate < 50:
                    with self._pending_data_lock:
                        has_pending = bool(self._pending_on_stop_data)
                    if has_pending:
                        self._try_replay_pending_data(target_queue)

                return True
            except queue.Full:
                with self._pending_data_lock:
                    if len(self._pending_on_stop_data) < self._spill_wal_max_entries:
                        self._pending_on_stop_data.append(task)
                        self._spill_wal_append(task)
                    else:
                        logging.warning("[SPILL] _pending_on_stop_data已达上限%d, 丢弃", self._spill_wal_max_entries)
                with self._queue_stats_lock:
                    self._queue_stats['drops_count'] += 1
                    self._queue_stats['spill_count'] = self._queue_stats.get('spill_count', 0) + 1
                logging.warning("[SPILL] 队列满(反压0.5s后仍满) 已暂存到。pending_on_stop_data+WAL 累计spill=%d 通道=%s 方法=%s",
                                self._queue_stats['spill_count'], channel, func_name)
                return True
        except Exception as e:
            logging.error("[AsyncWriter] 入队失败：%s", e)
            return False

    def _try_replay_pending_data(self, target_queue) -> int:
        with self._pending_data_lock:
            if not self._pending_on_stop_data:
                self._spill_wal_clear()
                return 0
            replayed = 0
            remaining = []
            for task in self._pending_on_stop_data:
                try:
                    target_queue.put_nowait(task)
                    replayed += 1
                except queue.Full:
                    remaining.append(task)
            if replayed > 0:
                self._pending_on_stop_data[:] = remaining
                with self._queue_stats_lock:
                    self._queue_stats['replay_count'] = self._queue_stats.get('replay_count', 0) + replayed
                if not remaining:
                    self._spill_wal_clear()
                else:
                    self._spill_wal_rewrite()
                logging.info("[REPLAY] 从。pending_on_stop_data恢复 %d 条数据, 剩余 %d 条", replayed, len(remaining))
            return replayed

    def _flush_batch_to_db(self, batch: List[Tuple[str, Any, Any]], writer_idx: int = -1) -> Optional[int]:
        if not batch:
            return 0

        max_sub_batch = self.DEFAULT_MAX_SUB_BATCH
        if len(batch) <= max_sub_batch:
            return self._flush_sub_batch(batch, writer_idx=writer_idx)
        executed_total = 0
        is_kline = any(fn.startswith('_save_kline') for fn, _, _ in batch)
        for i in range(0, len(batch), max_sub_batch):
            sub = batch[i:i + max_sub_batch]
            count = self._flush_sub_batch(sub, writer_idx=writer_idx)
            if count is not None:
                executed_total += count
            if is_kline and i + max_sub_batch < len(batch):
                time.sleep(0.01)
        return executed_total

    def _flush_sub_batch(self, batch: List[Tuple[str, Any, Any]], writer_idx: int = -1) -> Optional[int]:
        if not batch:
            return 0

        is_kline_batch = any(fn.startswith('_save_kline') for fn, _, _ in batch)
        is_maintenance_batch = any(fn.startswith(('_save_signal', '_save_underlying', '_save_kv',
                                                     '_save_depth_batch', '_save_option_snapshot_batch')) for fn, _, _ in batch)

        if is_kline_batch:
            write_lock = self._db_kline_write_lock
        elif is_maintenance_batch:
            write_lock = self._db_maintenance_write_lock
        elif 0 <= writer_idx < len(self._db_tick_write_locks):
            write_lock = self._db_tick_write_locks[writer_idx]
        else:
            write_lock = self._db_tick_write_locks[0]

        with write_lock:
            # P0-R11-20: 获取跨进程文件锁 — DuckDB单进程写入限制
            if not self._acquire_db_file_lock():
                logging.error("P0-R11-20: [AsyncWriter] 无法获取DB文件锁, 跳过本批次 %d 条写入", len(batch))
                return 0
            try:
                merged_tasks = self._data_service.merge_tick_task_batch(batch, info_callback=self._get_info_by_id)
                if not merged_tasks:
                    logging.warning("[AsyncWriter] merge返回空结果，原batch有%d条", len(batch))
                    return 0
                executed_count = 0
                for func_name, args, kwargs in merged_tasks:
                    if hasattr(self, func_name):
                        method = getattr(self, func_name)
                        method(*args, **kwargs)
                        executed_count += 1
                    else:
                        logging.debug("[AsyncWriter] 未找到写入方法：%s", func_name)

                return executed_count
            except Exception as e:
                logging.error("[AsyncWriter] 数据库错误：%s", e, exc_info=True)
                with self._pending_data_lock:
                    for task in batch:
                        try:
                            if len(self._pending_on_stop_data) < self._spill_wal_max_entries:
                                self._pending_on_stop_data.append(task)
                                self._spill_wal_append(task)
                            else:
                                with self._queue_stats_lock:
                                    self._queue_stats['drops_count'] += 1
                        except Exception as _spill_err:
                            logging.warning("[SPILL] 暂存数据到。pending_on_stop_data失败: %s", _spill_err)
                            break
                logging.critical("[DATA_LOSS] _flush_batch_to_db异常，%d条数据已暂存到。pending_on_stop_data", len(batch))
                return len(batch)
            finally:
                # P0-R11-20: 释放跨进程文件锁
                self._release_db_file_lock()

    def _wait_for_queue_capacity(self, max_fill_rate: float = 60.0, timeout_sec: float = 30.0, source: str = 'runtime') -> bool:
        deadline = time.time() + max(0.1, float(timeout_sec or 0.1))
        warned = False
        while not self._stop_event.is_set():
            tick_total = sum(q.qsize() for q in self._tick_shard_queues)
            tick_max_total = sum(q.maxsize for q in self._tick_shard_queues)
            kline_size = self._kline_queue.qsize()
            kline_max = self._kline_queue.maxsize
            total_size = tick_total + kline_size
            total_max = tick_max_total + kline_max
            fill_rate = total_size / total_max * 100 if total_max > 0 else 0.0

            if fill_rate <= max_fill_rate:
                return True

            if time.time() >= deadline:
                logging.warning(
                    "[Storage] 队列长时间高水位，继续入队：source=%s, fill_rate=%.1f%%, queue_size=%d",
                    source, fill_rate, total_size,
                )
                return False

            if not warned:
                logging.info(
                    "[Storage] 等待队列回落后继续入队：source=%s, fill_rate=%.1f%%, queue_size=%d",
                    source, fill_rate, total_size,
                )
                warned = True

            time.sleep(0.1)

        return False

    def get_queue_stats(self) -> Dict[str, int]:
        with self._queue_stats_lock:
            stats = self._queue_stats.copy()

        tick_total = 0
        tick_max_total = 0
        shard_stats = {}
        for si in range(self._TICK_SHARD_COUNT):
            q = self._tick_shard_queues[si]
            sz = q.qsize() if q else 0
            mx = q.maxsize if q else 1
            tick_total += sz
            tick_max_total += mx
            shard_stats[f'tick_shard_{si}_size'] = sz
            shard_stats[f'tick_shard_{si}_fill'] = round(sz / mx * 100, 1) if mx > 0 else 0.0

        kline_size = self._kline_queue.qsize() if self._kline_queue else 0
        kline_max = self._kline_queue.maxsize if self._kline_queue else 1
        maint_size = self._maintenance_queue.qsize() if self._maintenance_queue else 0
        maint_max = self._maintenance_queue.maxsize if self._maintenance_queue else 1

        stats.update(shard_stats)
        stats['tick_queue_total'] = tick_total
        stats['tick_queue_max'] = tick_max_total
        stats['tick_fill_rate'] = round(tick_total / tick_max_total * 100, 1) if tick_max_total > 0 else 0.0
        stats['kline_queue_size'] = kline_size
        stats['kline_fill_rate'] = round(kline_size / kline_max * 100, 1) if kline_max > 0 else 0.0
        stats['maintenance_queue_size'] = maint_size
        stats['maintenance_fill_rate'] = round(maint_size / maint_max * 100, 1) if maint_max > 0 else 0.0
        stats['current_queue_size'] = tick_total + kline_size + maint_size
        stats['max_queue_size'] = tick_max_total + kline_max + maint_max
        stats['fill_rate'] = round(stats['current_queue_size'] / stats['max_queue_size'] * 100, 1) if stats['max_queue_size'] > 0 else 0.0
        stats['shard_binding_audit'] = self._shard_router.get_routing_audit_line()
        stats['shard_member_count'] = {f'shard_{k}_members': len(v) for k, v in self._shard_router.get_shard_members().items() if v}
        with self._pending_data_lock:
            stats['pending_on_stop_data_size'] = len(self._pending_on_stop_data)
        with self._queue_stats_lock:
            stats['spill_count'] = self._queue_stats.get('spill_count', 0)
            stats['replay_count'] = self._queue_stats.get('replay_count', 0)

        return stats


_StorageAsyncWriterMixin = StorageAsyncWriterService

