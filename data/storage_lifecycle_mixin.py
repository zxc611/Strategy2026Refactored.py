# MODULE_ID: M1-045
"""
storage_lifecycle_mixin.py — 生命周期服务
包含: StorageLifecycleService

重构说明 (2026-06-11):
- _StorageLifecycleMixin → StorageLifecycleService（服务提取，消灭Mixin）
- 构造函数显式接收依赖
"""

from infra.shared_utils import InitPhase, requires_phase, CHINA_TZ
import logging
import os
import re
import threading
import time
import queue
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StorageLifecycleService:

    def __init__(self, lock=None, params_service=None, data_service=None,
                 maintenance_service=None, platform_subscribe=None):
        self._lock = lock or threading.Lock()
        self._params_service = params_service
        self._data_service = data_service
        self._maintenance_service = maintenance_service
        self._platform_subscribe = platform_subscribe

    def subscribe(self, instrument_id: str, data_type: str = 'tick') -> bool:
        if self._platform_subscribe and callable(self._platform_subscribe):
            try:
                self._platform_subscribe(instrument_id, data_type)
                return True
            except Exception as e:
                logging.warning("[Storage.subscribe] 失败 %s: %s", instrument_id, e)
                return False
        return False

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
    def unsubscribe(self, instrument_id: str, data_type: str = 'tick') -> bool:
        if self._platform_unsubscribe and callable(self._platform_unsubscribe):
            try:
                self._platform_unsubscribe(instrument_id, data_type)
                return True
            except Exception as e:
                logging.warning("[Storage.unsubscribe] 失败 %s: %s", instrument_id, e)
                return False
        return False

    def bind_platform_subscribe_api(self, subscribe_func, unsubscribe_func):
        self._platform_subscribe = subscribe_func
        self._platform_unsubscribe = unsubscribe_func
        logging.info("[Storage] 平台订阅API已绑定")

    def _shutdown_impl(self, flush: bool = True) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True

        logging.info("开始关闭...")

        if hasattr(self, '_thread_mgr') and self._thread_mgr:
            stop_result = self._thread_mgr.stop_all(self._stop_event, timeout=30.0)
            logging.info("[Shutdown] 线程停止结果: %s", stop_result)
        else:
            self._stop_event.set()
            for wi, thread in enumerate(self._tick_shard_writers):
                if thread:
                    thread.join(timeout=30.0)
                    if thread.is_alive():
                        logging.warning("TickWriter-%d 未能在规定时间内停止", wi)
            for name, thread in [("KlineWriter", self._kline_writer_thread),
                                  ("MaintenanceWriter", self._maintenance_writer_thread)]:
                if thread:
                    thread.join(timeout=30.0)
                    if thread.is_alive():
                        logging.warning("%s 未能在规定时间内停止", name)

        if flush:
            # P0-R11-20: flush阶段获取跨进程文件锁 — drain写入直接调用DB
            _shutdown_locked = self._acquire_db_file_lock()
            if not _shutdown_locked:
                logging.error("P0-R11-20: [Shutdown] 无法获取DB文件锁, flush阶段将跳过直接写入")
            try:
                drained = 0
                deadline = time.time() + 30.0
                all_queues = [(q, f'TickShard-{si}') for si, q in enumerate(self._tick_shard_queues)]
                all_queues.append((self._kline_queue, 'Kline'))
                all_queues.append((self._maintenance_queue, 'Maintenance'))
                if _shutdown_locked:
                    for q, qname in all_queues:
                        while not q.empty() and time.time() < deadline:
                            try:
                                task = q.get_nowait()
                                func_name, args, kwargs = task
                                if hasattr(self, func_name):
                                    getattr(self, func_name)(*args, **kwargs)
                                    drained += 1
                            except (queue.Empty, Exception) as e:
                                if not isinstance(e, queue.Empty):
                                    logging.warning("[Shutdown] drain失败: %s", e)
                                break
                    if drained > 0:
                        logging.info("[Shutdown] drain写入 %d 条剩余数据到DB", drained)

                    if self._pending_on_stop_data:
                        pending_count = len(self._pending_on_stop_data)
                        logging.info("[Shutdown] 处理 %d 条待恢复数据", pending_count)
                        _max_retries = 3
                        _retry_delay = 0.1
                        failed_tasks = []
                        for task in self._pending_on_stop_data[:]:
                            func_name, args, kwargs = task
                            success = False
                            for _attempt in range(_max_retries):
                                try:
                                    if hasattr(self, func_name):
                                        getattr(self, func_name)(*args, **kwargs)
                                        drained += 1
                                        success = True
                                        break
                                except Exception as e:
                                    if _attempt < _max_retries - 1:
                                        logging.debug("[Shutdown] 恢复数据重试 %d/%d: %s", _attempt + 1, _max_retries, e)
                                        time.sleep(_retry_delay)
                                    else:
                                        logging.warning("[Shutdown] 恢复数据失败(已重试%d次): %s", _max_retries, e)
                            if not success:
                                failed_tasks.append(task)
                        if failed_tasks:
                            logging.error("[Shutdown] %d 条数据恢复失败,已写入WAL待下次启动恢复", len(failed_tasks))
                            self._spill_wal_append_batch(failed_tasks)
                        self._pending_on_stop_data.clear()
                else:
                    # 锁获取失败: 将所有队列数据保存到 pending_on_stop_data
                    skipped = 0
                    for q, qname in all_queues:
                        while not q.empty():
                            try:
                                task = q.get_nowait()
                                with self._pending_data_lock:
                                    if len(self._pending_on_stop_data) < self._spill_wal_max_entries:
                                        self._pending_on_stop_data.append(task)
                                        self._spill_wal_append(task)
                                        skipped += 1
                            except queue.Empty:
                                break
                    if skipped:
                        logging.warning("P0-R11-20: [Shutdown] 锁获取失败, %d 条数据已暂存到。pending_on_stop_data", skipped)
            finally:
                if _shutdown_locked:
                    self._release_db_file_lock()

        if not flush:
            for q in [self._maintenance_queue, self._kline_queue] + list(self._tick_shard_queues):
                while not q.empty():
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        break
            self._pending_on_stop_data.clear()

        remaining = sum(q.qsize() for q in self._tick_shard_queues)
        remaining += self._kline_queue.qsize() + self._maintenance_queue.qsize()
        if remaining > 0:
            logging.error("关闭时仍有 %d 个任务未完成，可能丢失数据", remaining)

        with self._queue_stats_lock:
            stats = self._queue_stats.copy()
        logging.info("队列统计: 接收=%d 写入=%d 丢弃=%d 峰值=%d",
                     stats['total_received'], stats['total_written'],
                     stats['drops_count'], stats['max_queue_size_seen'])

        if flush and self._aggregators:
            self._save_aggregator_states()

        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2)

        # P0-R11-20: 关闭跨进程文件锁句柄
        self._close_db_file_lock()

        logging.info("关闭完成")

    def close_connection(self):
        pass

    def _start_cleanup_thread(self):
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return
        self._cleanup_thread = threading.Thread(target=self._auto_cleanup_loop, daemon=True)
        self._cleanup_thread.start()
        logging.info("[AutoCleanup] 自动清理线程已启动，间隔：%d秒", self._cleanup_interval)

    def _auto_cleanup_loop(self):
        while not self._stop_event.is_set():
            try:
                self._stop_event.wait(self._cleanup_interval)
                if self._stop_event.is_set():
                    break

                for table, days in self._cleanup_config.items():
                    try:
                        deleted = self.cleanup_old_data(table, days)
                        logging.info("[AutoCleanup] %s 表清理完成，删除 %d 条数据（保留 %d 天）", table, deleted, days)
                    except Exception as e:
                        logging.error("[AutoCleanup] %s 表清理失败：%s", table, e)
            except Exception as e:
                logging.error("[AutoCleanup] 清理循环异常：%s", e, exc_info=True)

    def cleanup_old_data(self, table: str, days: int, condition: str = "") -> int:
        if days <= 0:
            return 0

        # P2-R11-05: tick_partition使用DROP TABLE方式清理日期分区表
        if table == 'tick_partition':
            return self._cleanup_tick_partitions(days)

        allowed_tables = {
            'tick', 'kline', 'depth_market', 'underlying_snapshot',
            'option_snapshot', 'strategy_signals', 'external_klines'
        }
        if table not in allowed_tables:
            logging.error("无效的表名（不在白名单中）：%s", table)
            raise ValueError(f"表名 '{table}' 不在允许列表中")

        cutoff = time.time() - days * 86400

        sql = f"DELETE FROM {table} WHERE ts < ?"
        params = [cutoff]

        if condition:
            match = re.match(r"^\s+AND\s+(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?$", condition, re.IGNORECASE)
            if match:
                col_name = match.group(1)
                col_value = match.group(2)
                allowed_cols = {'symbol', 'instrument_id', 'strategy_id', 'trade_date', 'ts'}
                if col_name not in allowed_cols:
                    raise ValueError(f"列名 '{col_name}' 不在允许列表中")
                sql += f" AND {col_name} = ?"
                params.append(col_value)
            else:
                logging.error("无效的 condition 格式，只支持 ' AND column=value' 格式：%s", condition)
                raise ValueError("condition 格式错误")
        table_check = self._data_service.query(
            "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = ?",
            (table,),
        ).to_pylist()
        if not table_check or table_check[0]['cnt'] == 0:
            logging.warning("清理跳过：表 %s 不存在", table)
            return 0

        try:
            # P0-R11-20: 获取跨进程文件锁 — DELETE操作也需要排他锁
            if not self._acquire_db_file_lock():
                logging.error("P0-R11-20: [cleanup_old_data] 无法获取DB文件锁, 跳过表 %s 清理", table)
                return 0
            try:
                count_sql = f"SELECT COUNT(*) as cnt FROM {table} WHERE ts < ?"
                count_params = list(params)
                if condition:
                    match2 = re.match(r"^\s+AND\s+(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?$", condition, re.IGNORECASE)
                    if match2:
                        count_sql += f" AND {match2.group(1)} = ?"
                        count_params.append(match2.group(2))
                count_result = self._data_service.query(count_sql, tuple(count_params))
                deleted = count_result.to_pylist()[0]['cnt'] if count_result else 0

                self._data_service.query("BEGIN")
                self._data_service.query(sql, tuple(params))
                self._data_service.query("COMMIT")
                logging.info("从表 %s 删除了 %d 条过期数据（截止 %.3f）", table, deleted, cutoff)
                return deleted
            finally:
                # P0-R11-20: 释放跨进程文件锁
                self._release_db_file_lock()
        except Exception as e:
            try:
                self._data_service.query("ROLLBACK")
            except Exception as _rb_err:
                logging.debug("[DB] ROLLBACK失败(可能连接已断): %s", _rb_err)
            logging.error("清理数据失败：%s", e)
            return 0

    def close(self):
        self._shutdown_impl(flush=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _cache_to_params_service(self, instrument_id: str, info: Dict[str, Any]) -> Dict[str, Any]:
        return self._params_service.cache_instrument_info(instrument_id, info)

    def _cache_alias_instrument_mapping(
        self,
        alias_instrument_id: str,
        canonical_info: Dict[str, Any],
    ) -> int:
        canonical_internal_id = self._get_info_internal_id(canonical_info)
        if canonical_internal_id is None:
            raise ValueError(f"canonical instrument has no internal_id: {canonical_info}")

        alias_info = dict(canonical_info)
        alias_info['instrument_id'] = alias_instrument_id
        alias_info['internal_id'] = canonical_internal_id
        alias_info['canonical_instrument_id'] = canonical_info.get('instrument_id')

        try:
            self._cache_to_params_service(alias_instrument_id, alias_info)
        except RuntimeError as exc:
            if '重复 internal_id' not in str(exc):
                raise
        return canonical_internal_id

    def _preload_column_cache(self):
        common_tables = [
            'futures_instruments', 'option_instruments',
            'future_products', 'option_products',
            'app_kv_store'
        ]
        for table in common_tables:
            try:
                rows = self._data_service.query(f"DESCRIBE {table}").to_pylist()
                columns = [row['column_name'] for row in rows]
                with self._column_cache_lock:
                    self._column_cache[table] = columns
            except Exception as e:
                logging.debug("预加载表 %s 列名失败：%s", table, e)

    def _get_table_columns(self, table_name: str) -> List[str]:
        with self._column_cache_lock:
            if table_name in self._column_cache:
                return self._column_cache[table_name]
        try:
            rows = self._data_service.query(f"DESCRIBE {table_name}").to_pylist()
            columns = [row['column_name'] for row in rows]
            with self._column_cache_lock:
                self._column_cache[table_name] = columns
            return columns
        except Exception as e:
            logging.error("获取表 %s 列名失败：%s", table_name, e)
            return []


_StorageLifecycleMixin = StorageLifecycleService
