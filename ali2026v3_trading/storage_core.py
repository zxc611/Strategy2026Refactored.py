"""
storage_core.py — 存储核心模块
包含: _get_default_db_path, _StorageCoreMixin

架构：Mixin模式
  shared_utils.py   → InitPhase + InitStateMachine + requires_phase + ThreadLifecycleManager
  storage_core.py   → _get_default_db_path + _StorageCoreMixin
  storage_query.py  → _StorageQueryMixin (READ + HELPER + INSTRUMENT + SCHEMA)
  __init__.py       → InstrumentDataManager = _StorageCoreMixin + _StorageQueryMixin + 单例
"""

from ali2026v3_trading.shared_utils import InitPhase, InitializationError, InitStateMachine, requires_phase, ThreadLifecycleManager
from ali2026v3_trading.data_service import get_data_service
from ali2026v3_trading.query_service import _KlineAggregator, StorageMaintenanceService
from ali2026v3_trading.subscription_manager import SubscriptionConfig, SubscriptionManager
from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
import logging
import os
import re
import threading
import time
import json
import queue
import math
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _get_default_db_path() -> str:
    try:
        from ali2026v3_trading.config_service import get_default_db_path

        db_path = get_default_db_path()
        if db_path:
            return db_path
    except Exception as exc:
        logging.warning("[storage] Failed to resolve db path from config_service: %s", exc)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, 'trading_data.duckdb')


class _StorageCoreMixin:

    __version__ = "2026-03-26-FIXED-dbpath-explicit"

    _TABLE_NAME_PATTERN = re.compile(r'^[A-Za-z0-9_]+$')

    SUPPORTED_PERIODS = ['1min', '5min', '15min', '30min', '60min']

    def __init__(self, db_path: Optional[str] = None, max_retries: int = 3, retry_delay: float = 0.1,
                 async_queue_size: int = 500000, batch_size: int = 5000,
                 drop_on_full: bool = True, max_connections: int = 20,
                 cleanup_interval: Optional[int] = 3600,
                 cleanup_config: Optional[Dict[str, int]] = None):
        """初始化数据库连接，创建元数据表，启用WAL模式

        三阶段初始化（P2-2 初始化阶段化）：
        - Phase1: 纯内存初始化，零副作用
        - Phase2: DB连接+schema迁移+缓存加载，失败通过_emergency_cleanup回滚
        - Phase3: 线程启动进入运行时，Writer失败回滚，Cleanup可降级
        各阶段独立，失败可回滚到上阶段结束状态。
        """
        self._init_state = InitStateMachine()
        self._phase1_memory_init(db_path, max_retries, retry_delay, batch_size, drop_on_full)
        self._phase2_db_init(max_retries, retry_delay, cleanup_interval, cleanup_config)
        self._phase3_runtime_init()

    def _phase1_memory_init(self, db_path, max_retries, retry_delay, batch_size, drop_on_full):
        """Phase1: 纯内存初始化——零副作用，永不失败"""
        self.db_path = db_path or _get_default_db_path()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        self._writer_batch_tasks = max(5, min(20, int(batch_size or 1)))
        self.drop_on_full = drop_on_full

        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self._TICK_SHARD_COUNT = 16
        self._TICK_WRITER_COUNT = 6
        from ali2026v3_trading.shared_utils import get_shard_router
        self._shard_router = get_shard_router(shard_count=self._TICK_SHARD_COUNT)
        _shard_cap = max(10000, 10000000 // self._TICK_SHARD_COUNT)
        self._tick_shard_queues = [queue.Queue(maxsize=_shard_cap) for _ in range(self._TICK_SHARD_COUNT)]
        self._tick_shard_writers: list = []
        self._kline_queue = queue.Queue(maxsize=100000)
        self._kline_writer_thread = None
        self._maintenance_queue = queue.Queue(maxsize=50000)
        self._maintenance_writer_thread = None
        self._stop_event = threading.Event()
        self._pending_on_stop_data: list = []
        # 锁获取顺序(LOCK_ORDER): 编号小的先获取,禁止反向嵌套
        #   L1 _pending_data_lock    — 保护 _pending_on_stop_data
        #   L2 _queue_stats_lock     — 保护 _queue_stats (允许L1→L2嵌套,禁止L2→L1)
        #   L3 _spill_wal_lock       — 保护 WAL文件读写 (独立,无嵌套)
        #   L4 _column_cache_lock    — 保护 _column_cache (独立,无嵌套)
        #   L5 _lock (RLock)         — 保护公共API入口 (独立,无嵌套)
        #   L6 _ext_kline_lock       — 保护外部K线写入 (独立,无嵌套)
        #   L7 _agg_lock             — 保护K线聚合 (独立,无嵌套)
        #   L8 _db_tick_write_locks  — 保护Tick分片DB写入 (独立,无嵌套)
        #   L9 _db_kline_write_lock  — 保护K线DB写入 (独立,无嵌套)
        #   L10 _db_maintenance_write_lock — 保护维护DB写入 (独立,无嵌套)
        self._pending_data_lock = threading.Lock()
        self._spill_wal_path = os.path.join(db_dir, '_spill_wal.jsonl')
        self._spill_wal_lock = threading.Lock()
        self._spill_wal_max_entries = 100000
        self._restore_spill_wal()

        self._queue_stats = {
            'total_received': 0, 'total_written': 0,
            'drops_count': 0, 'max_queue_size_seen': 0,
        }
        self._queue_stats_lock = threading.Lock()

        self._column_cache: Dict[str, List[str]] = {}
        self._column_cache_lock = threading.Lock()

        self._lock = threading.RLock()
        self._ext_kline_lock = threading.Lock()
        self._agg_lock = threading.Lock()
        self._db_tick_write_locks = [threading.Lock() for _ in range(self._TICK_WRITER_COUNT)]
        self._db_kline_write_lock = threading.Lock()
        self._db_maintenance_write_lock = threading.Lock()
        self._runtime_missing_warned = set()
        self._platform_subscribe = None
        self._platform_unsubscribe = None

        self._last_ext_kline: Dict[Tuple[str, str], float] = {}
        self._ext_kline_load_in_progress = False
        self._aggregators: Dict[Tuple[str, str], '_KlineAggregator'] = {}
        self._init_state.advance(InitPhase.MEMORY_ALLOC)

    def _phase2_db_init(self, max_retries, retry_delay, cleanup_interval, cleanup_config):
        """Phase2: DB连接+schema迁移+缓存加载——失败通过_emergency_cleanup回滚"""
        self._data_service = get_data_service()
        self._init_state.advance(InitPhase.DB_CONNECT)

        self._assigned_ids = set()

        from ali2026v3_trading.params_service import get_params_service
        self._params_service = get_params_service()
        try:
            self._params_service.init_instrument_cache()
        except Exception as e:
            logging.warning(f"[Storage] ParamsService缓存初始化失败: {e}")

        self.subscription_manager = SubscriptionManager(
            self,
            SubscriptionConfig(max_retries=max_retries, retry_base_delay=retry_delay),
        )

        self._maintenance_service = StorageMaintenanceService(self)
        self._init_state.advance(InitPhase.EXTERNAL_SERVICES)

        self._cleanup_interval = cleanup_interval
        self._cleanup_config = cleanup_config or {}
        self._cleanup_thread = None
        self._closed = False

        try:
            self._migrate_legacy_schema()
            self._create_indexes()
            self._init_kv_store()
            self._init_state.advance(InitPhase.DB_SCHEMA)
            self._maintenance_service.run_startup_checks()
            self._load_caches()
            self._restore_aggregator_states()
            self._init_state.advance(InitPhase.DB_CACHES)
        except Exception as init_err:
            logging.critical("[Storage] 初始化数据库阶段失败: %s", init_err, exc_info=True)
            self._emergency_cleanup()
            raise

    def _phase3_runtime_init(self):
        """Phase3: 线程启动进入运行时——Writer失败回滚，Cleanup降级运行"""
        self._thread_mgr = ThreadLifecycleManager('InstrumentDataManager')
        try:
            self._start_async_writer()
            self._init_state.advance(InitPhase.WRITERS_START)
        except Exception as writer_err:
            logging.critical("[Storage] 启动写入线程失败: %s", writer_err, exc_info=True)
            self._emergency_cleanup()
            raise

        if self._cleanup_interval:
            try:
                self._start_cleanup_thread()
            except Exception as cleanup_err:
                logging.warning("[Storage] 启动清理线程失败(降级运行): %s", cleanup_err)
        self._init_state.advance(InitPhase.CLEANUP_START)
        self._preload_column_cache()
        self._init_state.advance(InitPhase.READY)

    def _emergency_cleanup(self) -> None:
        try:
            self._stop_event.set()
            self._closed = True
            if hasattr(self, '_tick_shard_writers'):
                for t in self._tick_shard_writers:
                    if t and t.is_alive():
                        t.join(timeout=5.0)
            if hasattr(self, '_kline_writer_thread') and self._kline_writer_thread and self._kline_writer_thread.is_alive():
                self._kline_writer_thread.join(timeout=5.0)
            if hasattr(self, '_maintenance_writer_thread') and self._maintenance_writer_thread and self._maintenance_writer_thread.is_alive():
                self._maintenance_writer_thread.join(timeout=5.0)
            if hasattr(self, '_cleanup_thread') and self._cleanup_thread and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=2.0)
        except Exception as e:
            logging.warning("[Storage] 紧急清理线程停止失败: %s", e)
        try:
            if hasattr(self, '_data_service') and self._data_service:
                self._data_service.close()
                logging.info("[Storage] 紧急清理: DB连接已关闭")
        except Exception as e:
            logging.warning("[Storage] 紧急清理DB关闭失败: %s", e)

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        return self._init_state.wait_until_ready(timeout=timeout)

    def is_ready(self) -> bool:
        return self._init_state.is_ready()

    @property
    def init_phase(self) -> InitPhase:
        return self._init_state.phase

    @property
    def _instrument_cache(self):
        """代理到ParamsService的instrument_id映射（通过公共接口）"""
        return self._params_service.get_all_instrument_cache()

    @property
    def _id_cache(self):
        """代理到ParamsService的internal_id映射（通过公共接口）"""
        return self._params_service.get_all_instrument_cache()

    @property
    def _product_cache(self):
        """代理到ParamsService的product缓存"""
        return self._params_service._product_cache

    @staticmethod
    def _pc_to_shard_idx(inst_id: str, shard_mask: int) -> int:
        from ali2026v3_trading.shared_utils import ShardRouter, extract_product_code
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

    def _async_shard_writer_loop(self, shard_indices: list, batch_tasks: int, name: str, writer_idx: int = 0):
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
                logging.warning("[SPILL] 队列满(反压0.5s后仍满) 已暂存到_pending_on_stop_data+WAL 累计spill=%d 通道=%s 方法=%s",
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
                logging.info("[REPLAY] 从_pending_on_stop_data恢复 %d 条数据, 剩余 %d 条", replayed, len(remaining))
            return replayed

    def _flush_batch_to_db(self, batch: List[Tuple[str, Any, Any]], writer_idx: int = -1) -> Optional[int]:
        if not batch:
            return 0

        max_sub_batch = 50
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
                            logging.warning("[SPILL] 暂存数据到_pending_on_stop_data失败: %s", _spill_err)
                            break
                logging.critical("[DATA_LOSS] _flush_batch_to_db异常，%d条数据已暂存到_pending_on_stop_data", len(batch))
                return len(batch)

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

    def _restore_spill_wal(self) -> None:
        if not os.path.exists(self._spill_wal_path):
            return
        restored = 0
        try:
            with open(self._spill_wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        task = json.loads(line)
                        if isinstance(task, list) and len(task) >= 2:
                            self._pending_on_stop_data.append(task)
                            restored += 1
                    except (json.JSONDecodeError, ValueError):
                        continue
            if restored > 0:
                logging.info("[SpillWAL] 从WAL恢复 %d 条spill数据: %s (WAL保留至replay成功后清除)", restored, self._spill_wal_path)
        except Exception as e:
            logging.warning("[SpillWAL] WAL恢复失败: %s", e)

    def _spill_wal_append(self, task) -> None:
        try:
            wal_entry = [task[0] if len(task) > 0 else '']
            args_data = task[1] if len(task) > 1 else None
            try:
                json.dumps(args_data, default=str)
                wal_entry.append(args_data)
            except (TypeError, ValueError):
                wal_entry.append(str(args_data))
            kwargs_data = task[2] if len(task) > 2 else {}
            try:
                json.dumps(kwargs_data, default=str)
                wal_entry.append(kwargs_data)
            except (TypeError, ValueError):
                wal_entry.append(str(kwargs_data))
            with self._spill_wal_lock:
                with open(self._spill_wal_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(wal_entry, ensure_ascii=False, default=str) + '\n')
        except Exception as e:
            logging.debug("[SpillWAL] WAL追加失败(非致命): %s", e)

    def _spill_wal_append_batch(self, tasks: list) -> None:
        if not tasks:
            return
        try:
            with self._spill_wal_lock:
                with open(self._spill_wal_path, 'a', encoding='utf-8') as f:
                    for task in tasks:
                        try:
                            wal_entry = [task[0] if len(task) > 0 else '']
                            args_data = task[1] if len(task) > 1 else None
                            try:
                                json.dumps(args_data, default=str)
                                wal_entry.append(args_data)
                            except (TypeError, ValueError):
                                wal_entry.append(str(args_data))
                            kwargs_data = task[2] if len(task) > 2 else {}
                            try:
                                json.dumps(kwargs_data, default=str)
                                wal_entry.append(kwargs_data)
                            except (TypeError, ValueError):
                                wal_entry.append(str(kwargs_data))
                            f.write(json.dumps(wal_entry, ensure_ascii=False, default=str) + '\n')
                        except Exception as _batch_wal_err:
                            logging.debug("[SpillWAL] 批量WAL单条写入失败(跳过): %s", _batch_wal_err)
                            continue
        except Exception as e:
            logging.debug("[SpillWAL] 批量WAL写入失败(非致命): %s", e)

    def _spill_wal_clear(self) -> None:
        try:
            with self._spill_wal_lock:
                if os.path.exists(self._spill_wal_path):
                    os.remove(self._spill_wal_path)
        except Exception as e:
            logging.debug("[SpillWAL] WAL清除失败(非致命): %s", e)

    def _spill_wal_rewrite(self) -> None:
        try:
            with self._spill_wal_lock:
                tmp_path = self._spill_wal_path + '.tmp'
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    for task in self._pending_on_stop_data:
                        try:
                            wal_entry = [task[0] if len(task) > 0 else '']
                            args_data = task[1] if len(task) > 1 else None
                            try:
                                json.dumps(args_data, default=str)
                                wal_entry.append(args_data)
                            except (TypeError, ValueError):
                                wal_entry.append(str(args_data))
                            kwargs_data = task[2] if len(task) > 2 else {}
                            try:
                                json.dumps(kwargs_data, default=str)
                                wal_entry.append(kwargs_data)
                            except (TypeError, ValueError):
                                wal_entry.append(str(kwargs_data))
                            f.write(json.dumps(wal_entry, ensure_ascii=False, default=str) + '\n')
                        except Exception as _wal_err:
                            logging.debug("[SpillWAL] 单条WAL写入失败(跳过): %s", _wal_err)
                            continue
                os.replace(tmp_path, self._spill_wal_path)
        except Exception as e:
            logging.debug("[SpillWAL] WAL重写失败(非致命): %s", e)

    @requires_phase(InitPhase.READY)
    def drain_all_queues(self, timeout_per_queue: float = 5.0) -> Dict[str, int]:
        result = {}
        total = 0
        for si in range(self._TICK_SHARD_COUNT):
            q = self._tick_shard_queues[si]
            drained = 0
            deadline = time.monotonic() + timeout_per_queue
            while not q.empty() and time.monotonic() < deadline:
                try:
                    task = q.get_nowait()
                    self._flush_batch_to_db([task], writer_idx=si % self._TICK_WRITER_COUNT)
                    drained += 1
                except (queue.Empty, Exception):
                    break
            result[f'tick_shard_{si}'] = drained
            total += drained
        kline_drained = 0
        deadline = time.monotonic() + timeout_per_queue
        while not self._kline_queue.empty() and time.monotonic() < deadline:
            try:
                task = self._kline_queue.get_nowait()
                self._flush_batch_to_db([task])
                kline_drained += 1
            except (queue.Empty, Exception):
                break
        result['kline'] = kline_drained
        total += kline_drained
        maint_drained = 0
        deadline = time.monotonic() + timeout_per_queue
        while not self._maintenance_queue.empty() and time.monotonic() < deadline:
            try:
                task = self._maintenance_queue.get_nowait()
                self._flush_batch_to_db([task])
                maint_drained += 1
            except (queue.Empty, Exception):
                break
        result['maintenance'] = maint_drained
        total += maint_drained
        if total > 0:
            logging.info("[drain_all_queues] 暂停时drain: %s (共%d条)", result, total)
        return result

    def _enqueue_kline_chunks_by_info(self, info: Dict[str, Any], kline_data: List[Dict], period: str = '1min') -> bool:
        if not kline_data:
            return True
        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type', 'future')
        if internal_id is None:
            return False
        chunk_size = self.batch_size
        for start in range(0, len(kline_data), chunk_size):
            chunk = kline_data[start:start + chunk_size]
            if not self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, period):
                return False
        return True

    def _enqueue_tick_chunks_by_info(self, info: Dict[str, Any], tick_data: List[Dict]) -> bool:
        if not tick_data:
            return True
        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type')
        if internal_id is None or instrument_type not in ('future', 'option'):
            return False
        chunk_size = self.batch_size
        for start in range(0, len(tick_data), chunk_size):
            chunk = tick_data[start:start + chunk_size]
            if not self._enqueue_write('_save_tick_impl', internal_id, instrument_type, chunk):
                return False
        return True

    def _check_info_or_skip(self, internal_id: int, caller: str, extra: str = '') -> Optional[Dict]:
        """检查info是否存在，不存在则记录跳过日志并返回None"""
        info = self._get_info_by_id(internal_id)
        if not info:
            with self._queue_stats_lock:
                self._queue_stats['save_skip_count'] = self._queue_stats.get('save_skip_count', 0) + 1
            warn_key = (f'{caller}_skip', internal_id)
            with self._lock:
                if warn_key not in self._runtime_missing_warned:
                    self._runtime_missing_warned.add(warn_key)
                    logging.warning("[DATA_LOSS] %s: internal_id %d not found, skipping %s", caller, internal_id, extra)
            return None
        return info

    def _save_kline_impl(self, internal_id: int, instrument_type: str, kline_data: List[Dict], period: str = '1min') -> None:
        kline_rows = kline_data
        if not kline_rows:
            return

        info = self._check_info_or_skip(internal_id, '_save_kline_impl', f'{len(kline_rows)} klines(period={period})')
        if not info:
            return

        normalized_klines = []
        for row in kline_rows:
            ts = self._to_timestamp(row.get('ts') or row.get('timestamp'))
            if ts is None:
                continue
            normalized_klines.append({
                'internal_id': internal_id,
                'instrument_type': instrument_type,
                'period': period,
                'timestamp': datetime.fromtimestamp(ts),
                'open': row.get('open', 0.0),
                'high': row.get('high', 0.0),
                'low': row.get('low', 0.0),
                'close': row.get('close', 0.0),
                'volume': row.get('volume', 0),
                'open_interest': row.get('open_interest', 0),
                'trade_date': datetime.fromtimestamp(ts).date(),
            })

        if normalized_klines:
            self._data_service.batch_insert_klines(normalized_klines)

    def _save_tick_impl(self, internal_id: int, instrument_type: str, tick_data) -> None:
        info = self._check_info_or_skip(internal_id, '_save_tick_impl', 'tick')
        if not info:
            return

        instrument_id = info.get('instrument_id')
        if not instrument_id:
            logging.warning("[DATA_LOSS] _save_tick_impl: internal_id %d missing instrument_id, skipping", internal_id)
            return

        self._data_service.batch_insert_ticks(tick_data, instrument_id)

    @requires_phase(InitPhase.READY)
    def process_tick(self, tick: Dict[str, Any]) -> None:
        if not self._validate_tick(tick):
            return

        tick = self._normalize_tick_fields(tick)

        tick_ts = self._to_timestamp(tick.get('ts'))
        if tick_ts is None:
            logging.error("process_tick 时间戳转换失败：%s", tick.get('timestamp') or tick.get('ts'))
            return
        tick['timestamp'] = tick_ts
        instrument = tick.get('instrument_id')
        price = tick.get('last_price')

        normalized_id = str(instrument or '').strip()
        info = self._params_service.get_instrument_meta_by_id(normalized_id)
        if info is None:
            info = self._get_instrument_info(normalized_id)
        if info is None:
            warn_key = ('process_tick', normalized_id)
            with self._lock:
                if warn_key not in self._runtime_missing_warned:
                    self._runtime_missing_warned.add(warn_key)
                    logging.info("[process_tick] 合约未预注册，尝试运行时注册：%s", normalized_id)
            try:
                self.register_instrument(normalized_id)
                info = self._params_service.get_instrument_meta_by_id(normalized_id)
                if info is None:
                    info = self._get_instrument_info(normalized_id)
            except Exception as e:
                logging.critical("[process_tick] 运行时注册失败 %s: %s，数据将被丢弃", normalized_id, e)
            if info is None:
                logging.critical("[process_tick] 合约 %s 不在任何注册路径中，Tick数据已丢弃", normalized_id)
                return

        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type')
        if internal_id is None or instrument_type not in ('future', 'option'):
            logging.critical("[process_tick] internal_id=None 或类型无效(%s)，Tick数据已丢弃：%s",
                           instrument_type, normalized_id)
            return

        if not self._enqueue_write('_save_tick_impl', internal_id, instrument_type, [tick]):
            logging.critical("[process_tick] Tick 入队失败，已跳过：%s", instrument)
            return

        current_time = tick_ts
        for period in self.SUPPORTED_PERIODS:
            key = (instrument, period)
            with self._agg_lock:
                agg = self._aggregators.get(key)
                if agg is None:
                    agg = _KlineAggregator(instrument, period, logging.getLogger(__name__))
                    self._aggregators[key] = agg

            completed_kline = agg.update(tick_ts, price,
                                         volume=tick.get('volume', 0),
                                         amount=tick.get('amount', 0.0),
                                         open_interest=tick.get('open_interest'))

            if completed_kline:
                if self._is_ext_kline_missing(instrument, period, current_time):
                    self.save_external_kline(completed_kline)
                else:
                    completed_ts = completed_kline.get('ts') or completed_kline.get('timestamp')
                    logging.debug("外部 K 线正常，丢弃合成的 K 线：%s %s %.3f",
                                  instrument, period, completed_ts)

    @requires_phase(InitPhase.READY)
    def save_external_kline(self, kline_data: Dict[str, Any]) -> None:
        if not self._validate_kline(kline_data):
            return
        kline_data = kline_data.copy()
        ts = self._to_timestamp(kline_data.get('ts') or kline_data.get('timestamp'))
        if ts is None:
            logging.error("save_external_kline 时间戳转换失败：%s", kline_data.get('ts') or kline_data.get('timestamp'))
            return
        kline_data['ts'] = ts
        instrument_id = kline_data.get('instrument_id')
        period = kline_data.get('period') or '1min'

        normalized_id = str(instrument_id or '').strip()
        info = self._params_service.get_instrument_meta_by_id(normalized_id)
        if info is None:
            info = self._get_instrument_info(normalized_id)
        if info is None:
            warn_key = ('save_external_kline', normalized_id)
            with self._lock:
                if warn_key not in self._runtime_missing_warned:
                    self._runtime_missing_warned.add(warn_key)
                    logging.warning("[save_external_kline] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
            return

        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type', 'future')
        if internal_id is None:
            return

        if not self._enqueue_write('_save_kline_impl', internal_id, instrument_type, [kline_data], period):
            logging.warning("[save_external_kline] K线入队失败，已跳过：%s %s", instrument_id, period)
            return

        with self._ext_kline_lock:
            self._last_ext_kline[(instrument_id, period)] = ts

    @requires_phase(InitPhase.READY)
    def batch_write_kline(self, instrument_id: str, kline_data: List[Dict], period: str = '1min') -> None:
        if not kline_data:
            return
        internal_id = self.register_instrument(instrument_id)
        info = self._get_info_by_id(internal_id)
        if not info:
            return
        instrument_type = info.get('type', 'future')

        chunk_size = self.batch_size
        for i in range(0, len(kline_data), chunk_size):
            chunk = kline_data[i:i+chunk_size]
            self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, period)

    @requires_phase(InitPhase.READY)
    def batch_write_tick(self, instrument_id: str, tick_data: List[Dict]) -> None:
        if not tick_data:
            return
        internal_id = self.register_instrument(instrument_id)
        info = self._get_info_by_id(internal_id)
        if not info:
            return
        instrument_type = info.get('type', 'future')

        chunk_size = self.batch_size
        for i in range(0, len(tick_data), chunk_size):
            chunk = tick_data[i:i+chunk_size]
            self._enqueue_write('_save_tick_impl', internal_id, instrument_type, chunk)

    @requires_phase(InitPhase.READY)
    def write_kline_to_table(self, table_name: str, kline_data: List[Dict]) -> None:
        if not kline_data:
            return

        self._validate_table_name(table_name)

        for k in kline_data:
            self._data_service.query(f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, open, high, low, close, volume, open_interest)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [k.get('ts') or k.get('timestamp'),
                   k.get('open', 0.0),
                   k.get('high', 0.0),
                   k.get('low', 0.0),
                   k.get('close', 0.0),
                   k.get('volume', 0),
                   k.get('open_interest', 0)], raise_on_error=True)
        logging.debug(f"写入 {len(kline_data)} 条 K 线到 {table_name}")

    @requires_phase(InitPhase.READY)
    def write_tick_to_table(self, table_name: str, tick_data: List[Dict]) -> None:
        if not tick_data:
            return

        self._validate_table_name(table_name)

        for t in tick_data:
            self._data_service.query(f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [t.get('ts') or t.get('timestamp'),
                   t.get('last_price', 0.0),
                   t.get('volume', 0),
                   t.get('open_interest', 0),
                   t.get('bid_price1'),
                   t.get('ask_price1')], raise_on_error=True)
        logging.debug(f"写入 {len(tick_data)} 条 Tick 到 {table_name}")

    def save_tick(self, tick: Dict[str, Any]) -> None:
        logging.debug("[DEPRECATED] save_tick已废弃，请使用process_tick（含K线聚合+完整验证+shard路由）")
        self.process_tick(tick)

    def _save_depth_batch_impl(self, depth_list: List[Dict[str, Any]]):
        grouped: Dict[str, List[tuple]] = {}
        for d in depth_list:
            try:
                internal_id = self.register_instrument(d['instrument_id'])
            except ValueError as e:
                logging.debug("_save_depth_batch_impl register failed: %s", e)
                continue

            info = self._get_info_by_id(internal_id)
            if not info:
                continue

            table_name = info.get('tick_table', 'ticks_raw')
            self._validate_table_name(table_name)

            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            values = [d.get('ts') or d.get('timestamp'),
                      d.get('last_price', 0.0),
                      d.get('volume', 0),
                      d.get('open_interest', 0)]

            for i in range(1, 6):
                bid_key = f'bid_price{i}'
                ask_key = f'ask_price{i}'
                if bid_key in d:
                    fields.append(bid_key)
                    values.append(d[bid_key])
                if ask_key in d:
                    fields.append(ask_key)
                    values.append(d[ask_key])

            key = (table_name, tuple(fields))
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tuple(values))

        _BATCH_INSERT_LIMIT = 100
        for (table_name, fields), rows in grouped.items():
            if not rows:
                continue
            field_str = ', '.join(fields)
            placeholder_str = ', '.join(['?'] * len(fields))
            update_fields = ', '.join([f"{f} = excluded.{f}" for f in fields[1:]])
            for batch_start in range(0, len(rows), _BATCH_INSERT_LIMIT):
                batch = rows[batch_start:batch_start + _BATCH_INSERT_LIMIT]
                flat_values = [v for row in batch for v in row]
                multi_placeholder = ', '.join([f'({placeholder_str})'] * len(batch))
                try:
                    self._data_service.query(f"""
                        INSERT INTO {table_name}
                        ({field_str})
                        VALUES {multi_placeholder}
                        ON CONFLICT(timestamp) DO UPDATE SET
                        {update_fields}
                    """, flat_values, raise_on_error=True)
                except Exception as e:
                    logging.warning("_save_depth_batch_impl batch insert failed, fallback to row-by-row: %s", e)
                    for row in batch:
                        try:
                            self._data_service.query(f"""
                                INSERT INTO {table_name}
                                ({field_str})
                                VALUES ({placeholder_str})
                                ON CONFLICT(timestamp) DO UPDATE SET
                                {update_fields}
                            """, list(row), raise_on_error=True)
                        except Exception as _row_err:
                            logging.debug("[DB] 单行写入失败(已降级): %s", _row_err)

    @requires_phase(InitPhase.READY)
    def save_depth_batch(self, depth_list: List[Dict[str, Any]]) -> None:
        if not isinstance(depth_list, list):
            logging.error("save_depth_batch 参数不是列表：%s", type(depth_list))
            return
        if not depth_list:
            return

        valid_list = []
        for d in depth_list:
            if not isinstance(d, dict):
                continue
            required = ['ts', 'instrument_id', 'last_price']
            if any(field not in d for field in required):
                continue
            price = d.get('last_price')
            if price is not None and (not isinstance(price, (int, float)) or price <= 0):
                continue
            d_copy = d.copy()
            ts = self._to_timestamp(d_copy.get('ts'))
            if ts is None:
                continue
            d_copy['ts'] = ts
            valid_list.append(d_copy)

        if valid_list:
            self._enqueue_write('_save_depth_batch_impl', valid_list)

    def _save_signal_impl(self, signal: Dict[str, Any]):
        try:
            internal_id = self.register_instrument(signal['instrument_id'])
        except ValueError as e:
            logging.debug("_save_signal_impl register failed: %s", e)
            return

        info = self._get_info_by_id(internal_id)
        if not info:
            logging.error("_save_signal_impl 找不到合约信息：%s", signal['instrument_id'])
            return

        key = f"signal:{signal['ts']}:{signal['instrument_id']}:{signal['strategy_name']}"
        self._save_kv_impl(key, json.dumps(signal, ensure_ascii=False, default=self._json_default))
        strategy_key = f"strategy_signals:{signal.get('strategy_name', 'unknown')}:{signal['ts']}"
        self._save_kv_impl(strategy_key, json.dumps(signal, ensure_ascii=False, default=self._json_default))

    @requires_phase(InitPhase.READY)
    def save_signal(self, signal: Dict[str, Any]) -> None:
        if not self._validate_signal(signal):
            return
        signal = signal.copy()
        ts = self._to_timestamp(signal.get('ts'))
        if ts is None:
            logging.error("save_signal 时间戳转换失败：%s", signal.get('ts'))
            return
        signal['ts'] = ts
        self._enqueue_write('_save_signal_impl', signal)

    def _save_underlying_snapshot_impl(self, data: Dict[str, Any]):
        key = f"underlying_snapshot:{data.get('underlying')}:{data.get('expiration')}:{data.get('ts')}"
        self._save_kv_impl(key, json.dumps(data, ensure_ascii=False, default=self._json_default))
        logging.debug("标的物快照已保存：%s %s", data.get('underlying'), data.get('expiration'))

    @requires_phase(InitPhase.READY)
    def save_underlying_snapshot(self, data: Dict[str, Any]) -> None:
        if not self._validate_underlying(data):
            return
        data = data.copy()
        ts = self._to_timestamp(data.get('ts'))
        if ts is None:
            logging.error("save_underlying_snapshot 时间戳转换失败：%s", data.get('ts'))
            return
        data['ts'] = ts
        self._enqueue_write('_save_underlying_snapshot_impl', data)

    def _save_option_snapshot_batch_impl(self, data_list: List[Dict[str, Any]]):
        grouped: Dict[str, List[tuple]] = {}
        for d in data_list:
            try:
                internal_id = self.register_instrument(d['instrument_id'])
            except ValueError as e:
                logging.debug("_save_option_snapshot_batch_impl register failed: %s", e)
                continue

            info = self._get_info_by_id(internal_id)
            if not info:
                continue

            table_name = info.get('tick_table', 'ticks_raw')
            self._validate_table_name(table_name)

            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            values = [d.get('ts') or d.get('timestamp'),
                      d.get('last_price', 0.0),
                      d.get('volume', 0),
                      d.get('open_interest', 0)]

            if 'bid_price1' in d:
                fields.append('bid_price1')
                values.append(d['bid_price1'])
            if 'ask_price1' in d:
                fields.append('ask_price1')
                values.append(d['ask_price1'])

            option_fields = ['implied_volatility', 'delta', 'gamma', 'theta', 'vega']
            for field in option_fields:
                if field in d:
                    fields.append(field)
                    values.append(d[field])

            key = (table_name, tuple(fields))
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tuple(values))

        _BATCH_INSERT_LIMIT = 100
        for (table_name, fields), rows in grouped.items():
            if not rows:
                continue
            field_str = ', '.join(fields)
            placeholder_str = ', '.join(['?'] * len(fields))
            update_fields = ', '.join([f"{f} = excluded.{f}" for f in fields[1:]])
            for batch_start in range(0, len(rows), _BATCH_INSERT_LIMIT):
                batch = rows[batch_start:batch_start + _BATCH_INSERT_LIMIT]
                flat_values = [v for row in batch for v in row]
                multi_placeholder = ', '.join([f'({placeholder_str})'] * len(batch))
                try:
                    self._data_service.query(f"""
                        INSERT INTO {table_name}
                        ({field_str})
                        VALUES {multi_placeholder}
                        ON CONFLICT(timestamp) DO UPDATE SET
                        {update_fields}
                    """, flat_values, raise_on_error=True)
                except Exception as e:
                    logging.warning("_save_option_snapshot_batch_impl batch insert failed, fallback to row-by-row: %s", e)
                    for row in batch:
                        try:
                            self._data_service.query(f"""
                                INSERT INTO {table_name}
                                ({field_str})
                                VALUES ({placeholder_str})
                                ON CONFLICT(timestamp) DO UPDATE SET
                                {update_fields}
                            """, list(row), raise_on_error=True)
                        except Exception as _row_err:
                            logging.debug("[DB] 期权快照单行写入失败(已降级): %s", _row_err)

    @requires_phase(InitPhase.READY)
    def save_option_snapshot_batch(self, data_list: List[Dict[str, Any]]) -> None:
        if not isinstance(data_list, list):
            logging.error("save_option_snapshot_batch 参数不是列表：%s", type(data_list))
            return
        if not data_list:
            return

        valid_list = []
        for d in data_list:
            if not isinstance(d, dict):
                continue
            required = ['ts', 'instrument_id', 'last_price']
            if any(field not in d for field in required):
                continue
            strike = d.get('strike')
            if strike is not None and (not isinstance(strike, (int, float)) or strike <= 0):
                continue
            d_copy = d.copy()
            ts = self._to_timestamp(d_copy.get('ts'))
            if ts is None:
                continue
            d_copy['ts'] = ts
            valid_list.append(d_copy)

        if valid_list:
            self._enqueue_write('_save_option_snapshot_batch_impl', valid_list)

    def _save_kv_impl(self, key: str, payload: str):
        try:
            self._data_service.query(
                """
                INSERT OR REPLACE INTO app_kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                [key, payload, datetime.now().isoformat()],
                raise_on_error=True,
            )
        except Exception as e:
            logging.error(f"_save_kv_impl 保存失败 key={key}: {e}")
            raise

    def save(self, key: str, data: Any, async_mode: bool = True) -> bool:
        if not key or not isinstance(key, str):
            logging.error("save 失败：key 无效：%s", key)
            return False

        try:
            payload = json.dumps(data, ensure_ascii=False, default=self._json_default)

            if async_mode:
                return self._enqueue_write('_save_kv_impl', key, payload)
            else:
                self._save_kv_impl(key, payload)
                return True
        except Exception as e:
            logging.error(f"save 异常：{e}")
            return False

    def _save_aggregator_states(self):
        """✅ BP-17：持久化K线聚合器状态到KV store"""
        try:
            states = {}
            with self._agg_lock:
                for (instrument, period), agg in self._aggregators.items():
                    state = agg.to_state_dict()
                    if state:
                        states[f"{instrument}:{period}"] = state
            if states:
                self.save('kline_aggregator_states', states, async_mode=False)
                logging.info("[BP-17] 已持久化 %d 个K线聚合器状态", len(states))
        except Exception as e:
            logging.warning("[BP-17] K线聚合器状态持久化失败: %s", e)

    def _restore_aggregator_states(self):
        """✅ BP-17：从KV store恢复K线聚合器状态"""
        try:
            states = self.load('kline_aggregator_states')
            if not states:
                return
            restored = 0
            with self._agg_lock:
                for key, state in states.items():
                    instrument = state.get('instrument_id', '')
                    period = state.get('period', '')
                    if not instrument or not period:
                        continue
                    agg_key = (instrument, period)
                    if agg_key not in self._aggregators:
                        self._aggregators[agg_key] = _KlineAggregator.from_state_dict(state)
                        restored += 1
            if restored > 0:
                logging.info("[BP-17] 已恢复 %d 个K线聚合器状态", restored)
        except Exception as e:
            logging.warning("[BP-17] K线聚合器状态恢复失败: %s", e)

    def _update_external_kline_timestamp(self, instrument_id: str, period: str, ts: float) -> None:
        key = (instrument_id, period)
        with self._ext_kline_lock:
            self._last_ext_kline[key] = ts

    def _is_ext_kline_missing(self, instrument: str, period: str, current_time: float) -> bool:
        with self._ext_kline_lock:
            if self._ext_kline_load_in_progress:
                return False
        key = (instrument, period)
        with self._ext_kline_lock:
            last_time = self._last_ext_kline.get(key)

        if last_time is None:
            return True

        try:
            minutes = int(period.replace('min', ''))
        except (ValueError, AttributeError) as e:
            logging.error("无效周期格式：%s, 错误：%s", period, e)
            return True

        timeout = minutes * 60 * 2.0
        return (current_time - last_time) > timeout

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
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
            drained = 0
            deadline = time.time() + 30.0
            all_queues = [(q, f'TickShard-{si}') for si, q in enumerate(self._tick_shard_queues)]
            all_queues.append((self._kline_queue, 'Kline'))
            all_queues.append((self._maintenance_queue, 'Maintenance'))
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
            self._data_service.query("BEGIN")
            self._data_service.query(sql, tuple(params))
            self._data_service.query("COMMIT")
            deleted = 0
            logging.info("从表 %s 删除了 %d 条过期数据（截止 %.3f）", table, deleted, cutoff)
            return deleted
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
