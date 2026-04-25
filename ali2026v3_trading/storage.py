"""
instrument_data_manager.py
期货/期权数据管理核心模块
设计理念：原样订阅 -> 内部ID映射 -> 原样保存
所有操作基于合约内部ID，避免运行时正则解析。
支持期货和期权的K线/Tick数据管理，提供订阅、写入、查询、清理等全生命周期管理。
"""

from ali2026v3_trading.data_service import get_data_service
from ali2026v3_trading.query_service import _KlineAggregator, StorageMaintenanceService
from ali2026v3_trading.subscription_manager import SubscriptionConfig, SubscriptionManager, inst_get
from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
import logging
import os
import re
import threading
import time
import json
import queue
import math
import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Optional, Any, Tuple, Callable
from datetime import datetime, time as dt_time


# 配置日志
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



class InstrumentDataManager:
    
    # ✅ VERSION MARKER - Helps identify if platform is using cached bytecode
    __version__ = "2026-03-26-FIXED-dbpath-explicit"
    
    # 安全表名正则：仅允许字母、数字、下划线，防止 SQL 注入
    _TABLE_NAME_PATTERN = re.compile(r'^[A-Za-z0-9_]+$')
    
    # 支持的 K 线周期（单位：分钟），可根据需要扩展
    SUPPORTED_PERIODS = ['1min', '5min', '15min', '30min', '60min']

    def __init__(self, db_path: Optional[str] = None, max_retries: int = 3, retry_delay: float = 0.1,
                 async_queue_size: int = 500000, batch_size: int = 5000,
                 drop_on_full: bool = True, max_connections: int = 20,
                 cleanup_interval: Optional[int] = 3600,
                 cleanup_config: Optional[Dict[str, int]] = None):
        """
        初始化数据库连接，创建元数据表，启用 WAL 模式
        :param db_path: SQLite 数据库文件路径，未提供时自动使用默认路径
        :param max_retries: 数据库操作最大重试次数（处理锁超时）
        :param retry_delay: 重试间隔（秒）
        :param async_queue_size: 异步写入队列最大长度（默认50万，应对高峰流量）
        :param batch_size: 批量写入分块大小
        :param drop_on_full: 队列满时是否丢弃新数据（True）或阻塞等待（False）
        :param cleanup_interval: 自动清理间隔（秒），None 表示不启动自动清理
        :param cleanup_config: 清理配置，如 {'tick': 30, 'kline': 90} 表示保留天数
        """
        self.db_path = db_path or _get_default_db_path()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        self._writer_batch_tasks = max(5, min(20, int(batch_size or 1)))
        self.drop_on_full = drop_on_full

        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        self._data_service = get_data_service()
        
        # 异步写入队列和线程（双通道）
        # Tick通道：高优先级，实时数据
        self._tick_queue = queue.Queue(maxsize=async_queue_size // 2)
        self._tick_writer_thread = None
        # K线通道：低优先级，批量数据
        self._kline_queue = queue.Queue(maxsize=async_queue_size)
        self._kline_writer_thread = None
        # 停止信号（全局）
        self._stop_event = threading.Event()
        self._pending_on_stop_data: list = []  # 停止时保存的余下数据
        
        # 队列监控统计
        self._queue_stats = {
            'total_received': 0,
            'total_written': 0,
            'drops_count': 0,
            'max_queue_size_seen': 0,
        }
        self._queue_stats_lock = threading.Lock()
        
        # 列名缓存（避免重复获取）
        self._column_cache: Dict[str, List[str]] = {}
        self._column_cache_lock = threading.Lock()
        
        # 传递渠道唯一修复：本地缓存代理到params_service，不再独立维护
        # _instrument_cache/_id_cache/_product_cache均通过property委托给params_service
        self._lock = threading.RLock()       # 保证线程安全
        self._ext_kline_lock = threading.Lock()  # 外部 K 线时间戳锁
        self._agg_lock = threading.Lock()        # K 线聚合器锁
        self._runtime_missing_warned = set()
        self._platform_subscribe = None
        self._platform_unsubscribe = None
        
        
        # 外部 K 线最近写入时间记录：{(instrument_id, period): last_timestamp}
        self._last_ext_kline: Dict[Tuple[str, str], float] = {}
        
        # K 线聚合器字典：{(instrument_id, period): _KlineAggregator}
        self._aggregators: Dict[Tuple[str, str], '_KlineAggregator'] = {}
        
        # 统一订阅管理器（新增）
        self.subscription_manager = SubscriptionManager(
            self,
            SubscriptionConfig(max_retries=max_retries, retry_base_delay=retry_delay),
        )
        
        # v1吸收：ParamsService双向缓存同步
        # ✅ 传递渠道唯一：统一使用get_params_service()工厂函数获取单例，禁止直接实例化
        from ali2026v3_trading.params_service import get_params_service
        self._params_service = get_params_service()
        try:
            self._params_service.init_instrument_cache()
        except Exception as e:
            logging.warning(f"[Storage] ParamsService缓存初始化失败: {e}")
        self._maintenance_service = StorageMaintenanceService(self)
        
        # v1吸收：已分配ID集合（用于_load_caches中加载已有internal_id）
        self._assigned_ids = set()
        
        # 自动清理配置
        self._cleanup_interval = cleanup_interval
        self._cleanup_config = cleanup_config or {}
        self._cleanup_thread = None
        self._closed = False
        
        # 初始化（元数据表和品种配置已由 DataService 和 StrategyCoreService.on_init() 完成）
        self._migrate_legacy_schema()
        self._create_indexes()
        self._init_kv_store()
        self._maintenance_service.run_startup_checks()
        self._load_caches()
        self._start_async_writer()
        if self._cleanup_interval:
            self._start_cleanup_thread()
        
        # 列名缓存预加载（可选优化）
        self._preload_column_cache()
    
    # 传递渠道唯一：_instrument_cache/_id_cache/_product_cache代理到params_service
    @property
    def _instrument_cache(self):
        """代理到 ParamsService 的 instrument_id -> InstrumentMeta 映射"""
        # 构建兼容的字典接口
        cache = {}
        for inst_id, internal_id in self._params_service._instrument_id_to_internal_id.items():
            meta = self._params_service._instrument_meta_by_id.get(internal_id)
            if meta:
                cache[inst_id] = meta
        return cache
    
    @property
    def _id_cache(self):
        """代理到 ParamsService 的 internal_id -> InstrumentMeta 映射"""
        return self._params_service._instrument_meta_by_id

    @property
    def _product_cache(self):
        """代理到 ParamsService 的 product 缓存"""
        return self._params_service._product_cache


    def _validate_table_name(self, table_name: str):
        if not self._TABLE_NAME_PATTERN.match(table_name):
            raise ValueError(f"非法表名：{table_name}")
    
    def subscribe(self, instrument_id: str, data_type: str = 'tick') -> bool:
        if self._platform_subscribe and callable(self._platform_subscribe):
            try:
                self._platform_subscribe(instrument_id, data_type)
                return True
            except Exception as e:
                logging.warning("[Storage.subscribe] 失败 %s: %s", instrument_id, e)
                return False
        return False
    
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
        
    # ========== 异步写入队列核心 ==========
    def _start_async_writer(self):
        self._tick_writer_thread = threading.Thread(
            target=self._async_writer_loop,
            args=(self._tick_queue, 5, "TickWriter"),
            name="TickWriter",
            daemon=True
        )
        self._tick_writer_thread.start()
        
        self._kline_writer_thread = threading.Thread(
            target=self._async_writer_loop,
            args=(self._kline_queue, 20, "KlineWriter"),
            name="KlineWriter",
            daemon=True
        )
        self._kline_writer_thread.start()
        logging.info("[AsyncWriter] 双通道后台写入线程已启动 (Tick+Kline)")
        
    def _stop_async_writer(self):
        self._stop_event.set()
        
        # K线通道：立即停止，保存余下任务到内存
        saved_kline = 0
        if self._kline_writer_thread and self._kline_writer_thread.is_alive():
            while not self._kline_queue.empty():
                try:
                    task = self._kline_queue.get_nowait()
                    self._pending_on_stop_data.append(task)
                    saved_kline += 1
                except queue.Empty:
                    break
            self._kline_writer_thread.join(timeout=3.0)
            if self._kline_writer_thread.is_alive():
                logging.warning("[AsyncWriter] KlineWriter 3秒未退出，强制放弃")
            else:
                logging.info("[AsyncWriter] KlineWriter 已停止")
        
        # Tick通道：排空后退出
        saved_tick = 0
        if self._tick_writer_thread and self._tick_writer_thread.is_alive():
            while not self._tick_queue.empty():
                try:
                    task = self._tick_queue.get_nowait()
                    self._pending_on_stop_data.append(task)
                    saved_tick += 1
                except queue.Empty:
                    break
            self._tick_writer_thread.join(timeout=3.0)
            if self._tick_writer_thread.is_alive():
                logging.warning("[AsyncWriter] TickWriter 3秒未退出，强制放弃")
            else:
                logging.info("[AsyncWriter] TickWriter 已停止")
        
        total = saved_kline + saved_tick
        if total:
            logging.info("[AsyncWriter] 停止时共保存 %d 条任务到内存 (Kline=%d, Tick=%d)", total, saved_kline, saved_tick)
        
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
                        
            except Exception as e:
                logging.error("[AsyncWriter][%s] 写入异常：%s", name, e, exc_info=True)
                if batch:
                    batch.clear()
        
        # 停止信号已收到：保存 batch 和队列余下数据到内存
        remaining = list(batch)
        while not task_queue.empty():
            try:
                remaining.append(task_queue.get_nowait())
            except queue.Empty:
                break
        if remaining:
            self._pending_on_stop_data.extend(remaining)
            logging.info("[AsyncWriter][%s] 停止时保存 %d 条余下数据到内存", name, len(remaining))
        
    def _enqueue_write(self, func_name: str, *args, **kwargs) -> bool:
        if self._stop_event.is_set():
            logging.warning("写入线程已停止，拒绝新任务：%s", func_name)
            return False
        
        # ✅ 更新统计
        with self._queue_stats_lock:
            self._queue_stats['total_received'] += 1
        
        task = (func_name, args, kwargs)
        
        # 双通道分发
        is_tick = func_name.startswith('_save_tick') or func_name.startswith('_save_depth') or func_name.startswith('_save_option') or func_name.startswith('_save_underlying')
        target_queue = self._tick_queue if is_tick else self._kline_queue
        
        try:
            if self.drop_on_full:
                try:
                    target_queue.put_nowait(task)
                    
                    # ✅ 环节6: Storage入队探针（仅监控合约）
                    if func_name == '_save_tick_impl' and args:
                        internal_id = args[0] if args else None
                        if internal_id:
                            # ✅ 传递渠道唯一：遍历 ParamsService 缓存
                            for inst_id, meta in self._params_service._instrument_meta_by_id.items():
                                if meta.get('internal_id') == internal_id:
                                    DiagnosisProbeManager.on_storage_enqueue(inst_id, True)
                                    break
                    
                    # ✅ 监控队列使用率
                    queue_size = target_queue.qsize()
                    max_size = target_queue.maxsize
                    fill_rate = queue_size / max_size * 100
                    
                    with self._queue_stats_lock:
                        if queue_size > self._queue_stats['max_queue_size_seen']:
                            self._queue_stats['max_queue_size_seen'] = queue_size
                    
                    if fill_rate > 80:
                        logging.warning(
                            f"⚠️ 队列使用率超过 80%: {fill_rate:.1f}% ({queue_size}/{max_size}) - "
                            f"通道：{'Tick' if is_tick else 'Kline'}，方法：{func_name}"
                        )
                    elif fill_rate > 50:
                        if queue_size % 100 == 0:
                            logging.info(f"📊 队列使用率：{fill_rate:.1f}% ({queue_size}/{max_size})")
                    
                    return True
                except queue.Full:
                    with self._queue_stats_lock:
                        self._queue_stats['drops_count'] += 1
                    logging.critical("🚨 [DATA_LOSS] 写入队列已满，数据被丢弃！累计丢弃=%d, 通道=%s, 方法=%s",
                                     self._queue_stats['drops_count'], 'Tick' if is_tick else 'Kline', func_name)
                    return False
            else:
                try:
                    target_queue.put(task, block=True, timeout=1)
                    return True
                except queue.Full:
                    with self._queue_stats_lock:
                        self._queue_stats['drops_count'] += 1
                    logging.critical("🚨 [DATA_LOSS] 写入队列已满，等待超时！累计丢弃=%d, 通道=%s, 方法=%s",
                                     self._queue_stats['drops_count'], 'Tick' if is_tick else 'Kline', func_name)
                    return False
        except Exception as e:
            logging.error("[AsyncWriter] 入队失败：%s", e)
            return False
        
    def _flush_batch_to_db(self, batch: List[Tuple[str, Any, Any]]) -> Optional[int]:
        if not batch:
            return 0

        try:
            merged_tasks = self._data_service.merge_tick_task_batch(batch, info_callback=self._get_info_by_id)
            executed_count = 0
            for func_name, args, kwargs in merged_tasks:
                if hasattr(self, func_name):
                    method = getattr(self, func_name)
                    method(*args, **kwargs)
                    executed_count += 1
                else:
                    logging.warning("[AsyncWriter] 未找到写入方法：%s", func_name)

            return executed_count
        except Exception as e:
            logging.error("[AsyncWriter] 数据库错误：%s", e, exc_info=True)
            return None

    def _wait_for_queue_capacity(self, max_fill_rate: float = 60.0, timeout_sec: float = 30.0, source: str = 'runtime') -> bool:
        deadline = time.time() + max(0.1, float(timeout_sec or 0.1))
        warned = False
        while not self._stop_event.is_set():
            stats = self.get_queue_stats()
            if float(stats.get('fill_rate', 0.0) or 0.0) <= max_fill_rate:
                return True

            if time.time() >= deadline:
                logging.warning(
                    "[Storage] 队列长时间高水位，继续入队：source=%s, fill_rate=%.1f%%, queue_size=%d",
                    source,
                    float(stats.get('fill_rate', 0.0) or 0.0),
                    int(stats.get('current_queue_size', 0) or 0),
                )
                return False

            if not warned:
                logging.info(
                    "[Storage] 等待队列回落后继续入队：source=%s, fill_rate=%.1f%%, queue_size=%d",
                    source,
                    float(stats.get('fill_rate', 0.0) or 0.0),
                    int(stats.get('current_queue_size', 0) or 0),
                )
                warned = True

            time.sleep(0.1)

        return False
    def get_queue_stats(self) -> Dict[str, int]:
        with self._queue_stats_lock:
            stats = self._queue_stats.copy()
        
        tick_size = self._tick_queue.qsize() if self._tick_queue else 0
        kline_size = self._kline_queue.qsize() if self._kline_queue else 0
        tick_max = self._tick_queue.maxsize if self._tick_queue else 1
        kline_max = self._kline_queue.maxsize if self._kline_queue else 1
        
        stats['tick_queue_size'] = tick_size
        stats['kline_queue_size'] = kline_size
        stats['current_queue_size'] = tick_size + kline_size
        stats['max_queue_size'] = tick_max + kline_max
        stats['tick_fill_rate'] = tick_size / tick_max * 100 if tick_max > 0 else 0.0
        stats['kline_fill_rate'] = kline_size / kline_max * 100 if kline_max > 0 else 0.0
        stats['fill_rate'] = stats['current_queue_size'] / stats['max_queue_size'] * 100 if stats['max_queue_size'] > 0 else 0.0
        
        return stats
    
    def _shutdown_impl(self, flush: bool = True) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True

        logging.info("开始关闭...")
        self._stop_event.set()

        if not flush:
            for q in [self._kline_queue, self._tick_queue]:
                while not q.empty():
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        break

        for name, thread in [("TickWriter", self._tick_writer_thread), ("KlineWriter", self._kline_writer_thread)]:
            if thread:
                timeout = 30.0
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logging.warning("%s 未能在规定时间内停止", name)
        
        remaining_tick = self._tick_queue.qsize() if self._tick_queue else 0
        remaining_kline = self._kline_queue.qsize() if self._kline_queue else 0
        remaining = remaining_tick + remaining_kline
        if remaining > 0:
            logging.error("关闭时仍有 %d 个任务未完成 (Tick=%d, Kline=%d)，可能丢失数据", remaining, remaining_tick, remaining_kline)
        
        # ✅ 输出最终统计
        with self._queue_stats_lock:
            stats = self._queue_stats.copy()
        
        logging.info(
            f"📊 队列统计汇总：接收={stats['total_received']:,}, "
            f"写入={stats['total_written']:,}, "
            f"丢弃={stats['drops_count']:,}, "
            f"峰值使用={stats['max_queue_size_seen']:,}"
        )

        # 停止清理线程
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2)

        logging.info("关闭完成")

    def _execute_with_retry(self, func, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

    def _migrate_legacy_schema(self):
        migrated = False
        try:
            rows = self._data_service.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'futures_instruments'").to_pylist()
            futures_columns = [row['column_name'] for row in rows]

            if 'internal_id' not in futures_columns and 'id' in futures_columns:
                logging.info("正在迁移 futures_instruments 表：添加 internal_id 列并回填旧 id...")
                self._data_service.query("ALTER TABLE futures_instruments ADD COLUMN internal_id INTEGER", raise_on_error=True)
                self._data_service.query("UPDATE futures_instruments SET internal_id=id WHERE internal_id IS NULL", raise_on_error=True)
                migrated = True



            rows = self._data_service.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'option_instruments'").to_pylist()
            option_columns = [row['column_name'] for row in rows]

            if 'internal_id' not in option_columns and 'id' in option_columns:
                logging.info("正在迁移 option_instruments 表：添加 internal_id 列并回填旧 id...")
                self._data_service.query("ALTER TABLE option_instruments ADD COLUMN internal_id INTEGER", raise_on_error=True)
                self._data_service.query("UPDATE option_instruments SET internal_id=id WHERE internal_id IS NULL", raise_on_error=True)
                migrated = True

            # ✅ Group A收口：不再添加underlying_instrument_id列，只认underlying_future_id主键
            # if 'underlying_instrument_id' not in option_columns:
            #     logging.info("正在迁移 option_instruments 表：添加 underlying_instrument_id 列...")
            #     self._data_service.query("ALTER TABLE option_instruments ADD COLUMN underlying_instrument_id TEXT", raise_on_error=True)
            #     migrated = True

            if 'underlying_future_id' not in option_columns:
                logging.info("正在迁移 option_instruments 表：添加 underlying_future_id 列...")
                self._data_service.query("ALTER TABLE option_instruments ADD COLUMN underlying_future_id INTEGER", raise_on_error=True)
                migrated = True


            # ✅ Group A收口：已移除underlying_future字段，不再需要基于字符串的迁移
            # 历史数据应该已经迁移完成，新数据直接使用underlying_future_id
            # 注释掉基于字符串的迁移逻辑，因为underlying_future字段已不存在
            # self._data_service.query("""
            #     UPDATE option_instruments oi
            #     SET underlying_future_id = fi.internal_id
            #     FROM futures_instruments fi
            #     WHERE oi.underlying_future_id IS NULL
            #       AND fi.instrument_id = oi.underlying_future
            # """, raise_on_error=True)

            # ✅ subscriptions 表已废弃，相关迁移代码已移除

            self._data_service.query("""
                UPDATE option_instruments
                SET underlying_product = product
                WHERE underlying_product != product
                  AND EXISTS (
                      SELECT 1
                      FROM option_products op
                      WHERE op.product = option_instruments.product
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM option_products op
                      WHERE op.product = option_instruments.underlying_product
                  )
            """, raise_on_error=True)
            fix_rows = self._data_service.query("""SELECT COUNT(*) as cnt FROM option_instruments
                WHERE underlying_product != product
                  AND EXISTS (SELECT 1 FROM option_products op WHERE op.product = option_instruments.product)
                  AND NOT EXISTS (SELECT 1 FROM option_products op WHERE op.product = option_instruments.underlying_product)
            """).to_pylist()
            fix_count = fix_rows[0]['cnt'] if fix_rows else 0
            if fix_count > 0:
                migrated = True
                logging.info(
                    "正在修复 option_instruments.underlying_product 历史错误值：%d 条",
                    fix_count,
                )

            if migrated:
                logging.info("✅ 数据库表结构迁移完成")
            else:
                logging.debug("数据库表结构已是最新版本，无需迁移")

        except Exception as e:
            logging.error(f"表结构迁移失败：{e}")
            raise

    def _create_indexes(self):
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_future_product ON futures_instruments(product)", raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_future_yym ON futures_instruments(year_month)", raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_option_product ON option_instruments(product)", raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_option_underlying_id ON option_instruments(underlying_future_id)", raise_on_error=True)
        logging.info("索引创建/检查完成")

    def _load_caches(self):
        """
        加载缓存：委托给 ParamsService 统一管理
        
        ✅ 传递渠道唯一：缓存由 params_service 统一管理，Storage 不再维护独立缓存
        """
        # 传递渠道唯一：调用 ParamsService 统一加载缓存
        try:
            self._params_service.load_caches_from_db(self._data_service)
            logging.info(
                f"[Storage] ParamsService 缓存加载完成: "
                f"{len(self._params_service._instrument_id_to_internal_id)} 个合约, "
                f"{len(self._params_service._product_cache)} 个品种"
            )
        except Exception as e:
            logging.error(f"[Storage] ParamsService 缓存加载失败: {e}")
            raise
        
        # v1吸收：加载已有 internal_id 到 _assigned_ids
        try:
            for internal_id in self._params_service._instrument_meta_by_id.keys():
                self._assigned_ids.add(internal_id)
            if self._assigned_ids:
                logging.info(f"[Storage] 已加载 {len(self._assigned_ids)} 个已分配 ID")
        except Exception as e:
            logging.warning(f"[Storage] 加载 assigned_ids 失败: {e}")

    # ========== 合约解析（简单直接） ==========
    # ✅ 方法唯一：_parse_future委托给SubscriptionManager.parse_future
    def _parse_future(self, instrument_id: str) -> Dict[str, Any]:
        return SubscriptionManager.parse_future(instrument_id)

    def _parse_option_with_dash(self, instrument_id: str) -> Dict[str, Any]:
        return SubscriptionManager.parse_option(instrument_id)


    # ========== v1吸收：运行时注册辅助 + 入队辅助 + 通用查询 + 连接管理 ==========

    def _cache_to_params_service(self, instrument_id: str, info: Dict[str, Any]) -> Dict[str, Any]:
        return self._params_service.cache_instrument_info(instrument_id, info)

    @staticmethod
    def _make_instrument_info(
        internal_id: int,
        instrument_id: str,
        instrument_type: str,
        **extra: Any,
    ) -> Dict[str, Any]:
        info = {
            'internal_id': internal_id,
            # ✅ Group A收口：删除id别名，新代码必须使用internal_id
            'instrument_id': instrument_id,
            'type': instrument_type,
        }
        info.update(extra)
        return info
    
    @staticmethod
    def _get_info_internal_id(info: Optional[Dict[str, Any]]) -> Optional[int]:
        if not info:
            return None
        # ID直通：只使用internal_id，不再回退id字段
        internal_id = info.get('internal_id')
        return int(internal_id) if internal_id is not None else None

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

        # ✅ 传递渠道唯一：通过 ParamsService 统一缓存，不直接写入代理属性
        try:
            self._cache_to_params_service(alias_instrument_id, alias_info)
        except RuntimeError as exc:
            if '重复 internal_id' not in str(exc):
                raise
            # 重复ID错误，忽略
        return canonical_internal_id

    @staticmethod
    def _extract_table_suffix_id(table_name: Optional[str], expected_prefix: str) -> Optional[int]:
        if not table_name or not table_name.startswith(expected_prefix):
            return None
        suffix = table_name[len(expected_prefix):]
        if not suffix.isdigit():
            return None
        value = int(suffix)
        return value if value > 0 else None

    def _warn_runtime_unregistered(self, instrument_id: str, source: str) -> None:
        warn_key = (source, instrument_id)
        with self._lock:
            if warn_key in self._runtime_missing_warned:
                return
            self._runtime_missing_warned.add(warn_key)
        logging.warning(
            "[%s] 合约未预注册，跳过运行时自动注册/建表：%s",
            source, instrument_id,
        )

    def _get_registered_internal_id(self, instrument_id: str, source: str = 'runtime') -> Optional[int]:
        info = self._get_registered_instrument_info(instrument_id, source=source)
        if info:
            return self._get_info_internal_id(info)
        return None

    def _get_registered_instrument_info(self, instrument_id: str, source: str = 'runtime') -> Optional[dict]:
        normalized_id = str(instrument_id or '').strip()
        if not normalized_id:
            return None

        # 先查ParamsService缓存（v1双向同步优势）
        info = self._params_service.get_instrument_meta_by_id(normalized_id)
        if info is None:
            info = self._get_instrument_info(normalized_id)

        if info:
            return info

        # ✅ Group A收口：不再使用大小写不敏感查找和紧凑格式回退，强制使用规范化合约ID
        self._warn_runtime_unregistered(normalized_id, source)
        return None

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

    def _update_external_kline_timestamp(self, instrument_id: str, period: str, ts: float) -> None:
        key = (instrument_id, period)
        with self._ext_kline_lock:
            self._last_ext_kline[key] = ts

    def _query_rows(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        result = self._data_service.query(sql, params, arrow=False)
        if hasattr(result, 'to_dict'):
            return result.to_dict('records')
        return []

    @property
    def connection(self):
        return self._data_service

    def close_connection(self):
        pass

    # ========== V3原有：交易所推断 ==========

    def infer_exchange_from_id(self, instrument_id: str) -> str:
        normalized_id = str(instrument_id or '').strip()
        # CFFEX
        if normalized_id.startswith(('IF', 'IH', 'IC', 'IM', 'IO', 'HO', 'MO')):
            return 'CFFEX'
        # GFEX (must check before SHFE because SI/LC overlap with SHFE prefixes)
        elif normalized_id.startswith(('LC', 'SI')):
            return 'GFEX'
        # SHFE (SI removed - now matched by GFEX above)
        elif normalized_id.startswith(('CU', 'AL', 'ZN', 'RB', 'AU', 'AG', 'NI', 'SN', 'PB', 'SS', 'WR', 'RU', 'NR', 'BC', 'LU')):
            return 'SHFE'
        # DCE
        elif normalized_id.startswith(('M', 'Y', 'A', 'JM', 'I', 'C', 'CS', 'JD', 'L', 'V', 'PP', 'EG', 'PG', 'J', 'P')):
            return 'DCE'
        # CZCE
        elif normalized_id.startswith(('CF', 'SR', 'MA', 'TA', 'RM', 'OI', 'SA', 'PF', 'FG', 'AP', 'CJ', 'SF', 'SM', 'UR')):
            return 'CZCE'
        # INE
        elif normalized_id.startswith(('SC',)):
            return 'INE'
        else:
            # 默认返回 CFFEX（中金所）
            return 'CFFEX'

    def _q(self, sql: str, params: list = None) -> list:
        result = self._data_service.query(sql, params or [])
        if result is None:
            return []
        if hasattr(result, 'to_pylist'):
            return result.to_pylist()
        if hasattr(result, 'to_dict'):
            return result.to_dict('records')
        if hasattr(result, '__iter__'):
            return list(result)
        return []
    
    # ========== 合约信息查询（缓存优先） ==========
    def _get_instrument_info(self, instrument_id: str) -> Optional[dict]:
        # ✅ 传递渠道唯一：通过 ParamsService 获取
        info = self._params_service.get_instrument_meta_by_id(instrument_id)
        if info:
            return info
        
        # 缓存未命中，从数据库查询
        rows = self._q("SELECT internal_id, instrument_id FROM futures_instruments WHERE instrument_id=?", [instrument_id])
        row = rows[0] if rows else None
        if row:
            info = self._make_instrument_info(
                row['internal_id'],
                row['instrument_id'],
                'future',
            )
            # ✅ 传递渠道唯一：通过 ParamsService 缓存
            self._cache_to_params_service(instrument_id, info)
            return info
        
        rows = self._q("SELECT internal_id, instrument_id FROM option_instruments WHERE instrument_id=?", [instrument_id])
        row = rows[0] if rows else None
        if row:
            info = self._make_instrument_info(
                row['internal_id'],
                row['instrument_id'],
                'option',
            )
            # ✅ 传递渠道唯一：通过 ParamsService 缓存
            self._cache_to_params_service(instrument_id, info)
            return info
        return None

    def _get_info_by_id(self, internal_id: int) -> Optional[dict]:
        # ✅ 传递渠道唯一：通过 ParamsService 获取
        info = self._params_service._instrument_meta_by_id.get(internal_id)
        if info:
            return info
        
        # 缓存未命中，从数据库查询
        rows = self._q("SELECT internal_id, instrument_id, 'future' as type FROM futures_instruments WHERE internal_id=?", [internal_id])
        row = rows[0] if rows else None
        if row:
            info = self._make_instrument_info(
                row['internal_id'],
                row['instrument_id'],
                row['type'],
            )
            # ✅ 传递渠道唯一：通过 ParamsService 缓存
            instrument_id = row['instrument_id']
            self._cache_to_params_service(instrument_id, info)
            return info
        
        rows = self._q("SELECT internal_id, instrument_id, 'option' as type FROM option_instruments WHERE internal_id=?", [internal_id])
        row = rows[0] if rows else None
        if row:
            info = self._make_instrument_info(
                row['internal_id'],
                row['instrument_id'],
                row['type'],
            )
            # ✅ 传递渠道唯一：通过 ParamsService 缓存
            instrument_id = row['instrument_id']
            self._cache_to_params_service(instrument_id, info)
            return info
        return None

    def get_registered_instrument_ids(self, instrument_ids: Optional[List[str]] = None) -> List[str]:
        if not instrument_ids:
            # ✅ 传递渠道唯一：通过 ParamsService 获取所有合约ID
            return list(self._params_service._instrument_id_to_internal_id.keys())

        registered_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids:
            normalized_id = str(instrument_id or '').strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)

            # ✅ 传递渠道唯一：通过 ParamsService 检查是否存在
            if self._params_service.get_instrument_meta_by_id(normalized_id):
                registered_ids.append(normalized_id)

        return registered_ids

    def ensure_registered_instruments(self, instrument_ids: List[str]) -> Dict[str, int]:
        from ali2026v3_trading.query_service import QueryService

        return QueryService(self).ensure_registered_instruments(instrument_ids)

    # ========== 辅助方法 ==========

    
    def _get_next_id(self) -> int:
        """从全局序列获取下一个 internal_id。
        
        委托 StorageMaintenanceService.reserve_next_global_id()。
        """
        if self._maintenance_service is not None:
            return self._maintenance_service.reserve_next_global_id()

        raise RuntimeError(
            "[Storage] _maintenance_service is None, cannot reserve global ID. "
            "StorageMaintenanceService must be initialized before registering instruments."
        )

    def _create_future_tables_by_id(self, instrument_id: int):
        # ⚠️ DEPRECATED: 已迁移到统一表 klines_raw
        # 此方法不再使用，保留仅为向后兼容
        logging.warning(
            f"_create_future_tables_by_id is deprecated (instrument_id={instrument_id}). "
            "K-line data now uses unified table 'klines_raw'."
        )
        # 不再创建分表

    def _create_option_tables_by_id(self, instrument_id: int):
        # ⚠️ DEPRECATED: 已迁移到统一表 klines_raw
        # 此方法不再使用，保留仅为向后兼容
        logging.warning(
            f"_create_option_tables_by_id is deprecated (instrument_id={instrument_id}). "
            "K-line data now uses unified table 'klines_raw'."
        )
        # 不再创建分表

    # ========== 核心：合约注册 ==========
    def register_instrument(self, instrument_id: str, exchange: str = "AUTO",
                            expire_date: Optional[str] = None,
                            listing_date: Optional[str] = None) -> int:
        normalized_instrument_id = str(instrument_id or '').strip()
        if not normalized_instrument_id:
            raise ValueError("instrument_id 不能为空")

        if str(exchange or 'AUTO').strip() == 'AUTO':
            exchange = self.infer_exchange_from_id(normalized_instrument_id)

        # ✅ 传递渠道唯一：检查 ParamsService 缓存
        existing_meta = self._params_service.get_instrument_meta_by_id(normalized_instrument_id)
        if existing_meta:
            return existing_meta.get('internal_id')

        # 查询数据库
        info = self._get_instrument_info(normalized_instrument_id)
        if info:
            return self._get_info_internal_id(info)

        # 注册新合约（加锁保证唯一性）
        with self._lock:
            # ✅ 传递渠道唯一：再次检查 ParamsService 缓存（避免重复插入）
            existing_meta = self._params_service.get_instrument_meta_by_id(normalized_instrument_id)
            if existing_meta:
                return existing_meta.get('internal_id')
            normalized_upper = str(normalized_instrument_id).strip().upper()
            rows = self._data_service.query("SELECT product, exchange, format_template FROM future_products WHERE is_active=1").to_pylist()
            for prod in rows:
                product_key = str(prod['product'] or '').strip()
                if normalized_upper.startswith(product_key.upper()):
                    try:
                        parsed = self._parse_future(normalized_instrument_id)
                        # 生成新ID
                        new_id = self._get_next_id()
                        kline_table = f"kline_future_{new_id}"
                        tick_table = f"tick_future_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        self._data_service.query("""
                            INSERT INTO futures_instruments
                            (internal_id, exchange, instrument_id, product, year_month, format,
                             expire_date, listing_date, kline_table, tick_table)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [new_id, exchange, normalized_instrument_id, parsed['product'], parsed['year_month'],
                              prod['format_template'], expire_date, listing_date, kline_table, tick_table], raise_on_error=True)
                        self._create_future_tables_by_id(new_id)
                        # 更新缓存
                        info = self._make_instrument_info(
                            new_id,
                            normalized_instrument_id,
                            'future',
                            product=parsed['product'],
                            year_month=parsed['year_month'],
                            kline_table=kline_table,
                            tick_table=tick_table,
                        )
                        # ✅ 传递渠道唯一：只通过 ParamsService 缓存，不直接写入代理属性
                        self._assigned_ids.add(new_id)  # v1吸收：跟踪已分配ID
                        self._cache_to_params_service(normalized_instrument_id, info)  # v1吸收：双向缓存同步
                        logging.info(f"注册期货合约：{normalized_instrument_id} -> ID={new_id}")
                        
                        # ✅ 环节2-3: 注册和ID转换探针
                        # DiagnosisProbeManager already imported at module level
                        DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'future')
                        
                        return new_id
                    except ValueError:
                        continue

            # 尝试注册为期权
            rows = self._data_service.query("SELECT product, exchange, underlying_product, format_template FROM option_products WHERE is_active=1").to_pylist()
            for prod in rows:
                product_key = str(prod['product'] or '').strip()
                if normalized_upper.startswith(product_key.upper()):
                    try:
                        parsed = self._parse_option_with_dash(normalized_instrument_id)
                        # 查找标的期货 ID（如果没有，先注册期货）
                        new_future_id = None
                        rows = self._data_service.query("SELECT internal_id, instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                       [prod['underlying_product'], parsed['year_month']]).to_pylist()
                        row = rows[0] if rows else None
                        if not row:
                            # ✅ ID直通原则：标的期货未注册，报错并要求上游先注册
                            expected_future_id = f"{prod['underlying_product']}{parsed['year_month']}"
                            logging.error(
                                f"[Storage] 标的期货未注册，无法注册期权 {normalized_instrument_id}。"
                                f"请先注册期货合约: {expected_future_id}"
                            )
                            return -1  # 注册失败
                        
                        # 使用数据库中已存在的原始instrument_id（ID直通）
                        new_future_id = row['internal_id']
                        logging.info("标的期货已存在：ID=%d", new_future_id)
                        new_id = self._get_next_id()
                        kline_table = f"kline_option_{new_id}"
                        tick_table = f"tick_option_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        self._data_service.query("""
                            INSERT INTO option_instruments
                                                        (internal_id, exchange, instrument_id, product, underlying_future_id, underlying_product,
                            year_month, option_type, strike_price, format,
                            expire_date, listing_date, kline_table, tick_table)
                                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [new_id, exchange, normalized_instrument_id, parsed['product'], new_future_id, prod['product'], parsed['year_month'],
                              parsed['option_type'], parsed['strike_price'], prod['format_template'],
                              expire_date, listing_date, kline_table, tick_table], raise_on_error=True)
                        self._create_option_tables_by_id(new_id)
                        # 更新缓存
                        info = self._make_instrument_info(
                            new_id,
                            normalized_instrument_id,
                            'option',
                            product=parsed['product'],
                            year_month=parsed['year_month'],
                            option_type=parsed['option_type'],
                            strike_price=parsed['strike_price'],
                            underlying_future_id=new_future_id,
                            kline_table=kline_table,
                            tick_table=tick_table,
                        )
                        # ✅ 传递渠道唯一：只通过 ParamsService 缓存，不直接写入代理属性
                        self._assigned_ids.add(new_id)  # v1吸收：跟踪已分配ID
                        self._cache_to_params_service(normalized_instrument_id, info)  # v1吸收：双向缓存同步
                        logging.info(f"注册期权合约：{normalized_instrument_id} -> ID={new_id}")
                        
                        # ✅ 环节2-3: 注册和ID转换探针
                        # DiagnosisProbeManager already imported at module level
                        DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'option')
                        
                        return new_id
                    except ValueError:
                        # 期权解析失败，尝试注册为期权标的期货（如 ho2605 -> IH2605）
                        try:
                            # 方法唯一修复：统一使用_parse_future，不再内联正则
                            future_parsed = self._parse_future(normalized_instrument_id)
                            underlying_prod = str(prod.get('underlying_product') or '').strip()
                            if underlying_prod and future_parsed.get('year_month'):
                                # ✅ ID直通：从数据库查询已注册的期货合约，不自行构造
                                rows = self._data_service.query(
                                    "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                    [underlying_prod, future_parsed['year_month']]
                                ).to_pylist()
                                if rows:
                                    future_instrument_id = rows[0]['instrument_id']  # 使用数据库中的原始值
                                    # 检查是否已注册
                                    existing = self._get_instrument_info(future_instrument_id)
                                    if existing:
                                            self._cache_alias_instrument_mapping(normalized_instrument_id, existing)
                                            canonical_internal_id = self._get_info_internal_id(existing)
                                            logging.info("复用期权标的期货主记录：%s -> ID=%d (主记录=%s)", normalized_instrument_id, canonical_internal_id, future_instrument_id)
                                            # DiagnosisProbeManager already imported at module level
                                            DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', canonical_internal_id, 'future')
                                            return canonical_internal_id
                        except (ValueError, Exception) as _e:
                            logging.debug("[Storage] 期权标的期货回退注册失败: %s", _e)
                        continue

            raise ValueError(f"无法识别合约品种：{normalized_instrument_id}")

    # ========== 订阅管理 ==========


    # ========== 异步队列实现方法 ==========
    def _save_kline_impl(self, internal_id: int, instrument_type: str, kline_data: List[Dict], period: str = '1min') -> None:
        kline_rows = kline_data
        if not kline_rows:
            return

        normalized_klines = []
        for row in kline_rows:
            ts = self._to_timestamp(row.get('ts') or row.get('timestamp'))
            if ts is None:
                continue
            normalized_klines.append({
                'internal_id': internal_id,
                'instrument_type': instrument_type,
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
        info = self._get_info_by_id(internal_id)
        if not info:
            logging.debug("_save_tick_impl: internal_id %d not found, skipping", internal_id)
            return

        instrument_id = info.get('instrument_id')
        if not instrument_id:
            logging.debug("_save_tick_impl: internal_id %d missing instrument_id, skipping", internal_id)
            return

        self._data_service.batch_insert_ticks(tick_data, instrument_id)

    # ========== 数据查询 ==========
    # ========== 关联查询 ==========
    def get_option_chain_by_future_id(self, future_id: int) -> Dict:
        info = self._get_info_by_id(future_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货ID: {future_id}")
        future_instrument = info.get('instrument_id')
        # 从缓存或数据库获取 instrument_id
        if not future_instrument:
            rows = self._data_service.query("SELECT instrument_id FROM futures_instruments WHERE internal_id=?", [future_id]).to_pylist()
            row = rows[0] if rows else None
            if row:
                future_instrument = row['instrument_id']
        if not future_instrument:
            raise ValueError(f"未找到期货合约ID: {future_id}")

        options = self._data_service.query("""
            SELECT internal_id, instrument_id, option_type, strike_price
            FROM option_instruments
            WHERE underlying_future_id=?
            ORDER BY strike_price, option_type
        """, [future_id]).to_pylist()

        normalized_options = []
        for row in options:
            option_internal_id = int(row['internal_id'])
            normalized_options.append({
                'internal_id': option_internal_id,
                'instrument_id': row['instrument_id'],
                'option_type': row['option_type'],
                'strike_price': row['strike_price'],
            })

        return {
            'future': {'internal_id': future_id, 'instrument_id': future_instrument},
            'options': normalized_options
        }

    # ========== 清理 ==========
    def delete_instrument(self, instrument_id: str) -> None:
        info = self._get_instrument_info(instrument_id)
        if not info:
            logging.warning(f"合约不存在，无法删除：{instrument_id}")
            return

        internal_id = self._get_info_internal_id(info)

        if info['type'] == 'future':
            rows = self._data_service.query(
                "SELECT instrument_id FROM option_instruments WHERE underlying_future_id=?",
                [internal_id],
            ).to_pylist()
            dependent_options = [row['instrument_id'] for row in rows]
            for option_instrument_id in dependent_options:
                self.delete_instrument(option_instrument_id)

        # 删除合约专属数据表（kline_table/tick_table可能存在于info中）
        kline_table = info.get('kline_table')
        tick_table = info.get('tick_table')
        if kline_table:
            self._validate_table_name(kline_table)
            self._data_service.query(f"DROP TABLE IF EXISTS {kline_table}", raise_on_error=True)
        if tick_table:
            self._validate_table_name(tick_table)
            self._data_service.query(f"DROP TABLE IF EXISTS {tick_table}", raise_on_error=True)
        delete_internal_id = int(internal_id)
        if info['type'] == 'future':
            self._data_service.query("DELETE FROM futures_instruments WHERE instrument_id=?", [instrument_id], raise_on_error=True)
        else:
            self._data_service.query("DELETE FROM option_instruments WHERE instrument_id=?", [instrument_id], raise_on_error=True)
        # subscriptions 表已废弃，DELETE 操作已移除
        # ✅ 传递渠道唯一：缓存由 ParamsService 管理，不支持单个删除（下次加载时自动同步）
        # if instrument_id in self._instrument_cache:
        #     del self._instrument_cache[instrument_id]
        # if internal_id in self._id_cache:
        #     del self._id_cache[internal_id]
        logging.info(f"已删除合约 {instrument_id} (ID={internal_id})")

    # ========== 时间戳转换工具 ==========
    @staticmethod
    def _to_timestamp(ts) -> Optional[float]:
        if ts is None:
            return None
        if isinstance(ts, datetime):
            result = ts.timestamp()
        elif isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts)
                result = dt.timestamp()
            except ValueError:
                try:
                    result = float(ts)
                except (ValueError, TypeError) as e:
                    logging.warning(f"[_to_timestamp] Failed to convert timestamp '{ts}': {e}")
                    return None
        elif isinstance(ts, (int, float)):
            result = float(ts)
        else:
            logging.warning(f"[_to_timestamp] Unsupported timestamp type: {type(ts)}")
            return None
        
        # 检查 NaN 或 Inf
        import math
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    
    # ========== 数据验证 ==========
    def _validate_tick(self, tick: Dict[str, Any]) -> bool:
        if not isinstance(tick, dict):
            logging.error("save_tick 参数不是字典：%s", type(tick))
            return False
        # ✅ 支持 PythonGO 和 INFINIGO 两种字段名格式
        instrument_id = tick.get('instrument_id') or tick.get('InstrumentID')
        if not instrument_id:
            logging.error("save_tick 缺少合约标识字段 (instrument_id 或 InstrumentID)")
            return False
        last_price = tick.get('last_price') or tick.get('LastPrice')
        if last_price is None:
            logging.error("save_tick 缺少价格字段 (last_price 或 LastPrice)")
            return False
        # ✅ 支持 ts、timestamp 或 UpdateTime 字段
        ts_value = tick.get('ts') or tick.get('timestamp') or tick.get('UpdateTime')
        if ts_value is None:
            logging.error("save_tick 缺少时间戳字段 (ts、timestamp 或 UpdateTime)")
            return False
        if not isinstance(last_price, (int, float)) or last_price <= 0:
            logging.error("save_tick 价格无效：%s", last_price)
            return False
        volume = tick.get('volume', tick.get('Volume', 0))
        if volume is not None and (not isinstance(volume, (int, float)) or volume < 0):
            logging.error("save_tick 成交量无效：%s", volume)
            return False
        return True
    
    def _validate_kline(self, kline: Dict[str, Any]) -> bool:
        if not isinstance(kline, dict):
            logging.error("save_external_kline 参数不是字典：%s", type(kline))
            return False
        required = ['instrument_id', 'period', 'open', 'high', 'low', 'close']
        for field in required:
            if field not in kline:
                logging.error("save_external_kline 缺少必要字段：%s", field)
                return False
        # ✅ 支持 ts 或 timestamp 字段
        if 'ts' not in kline and 'timestamp' not in kline:
            logging.error("save_external_kline 缺少时间戳字段 (ts 或 timestamp)")
            return False
        for field in ['open', 'high', 'low', 'close']:
            val = kline.get(field)
            if val is not None and (not isinstance(val, (int, float)) or val < 0):
                logging.error("save_external_kline %s 无效：%s", field, val)
                return False
        return True
    
    # ========== 智能 Tick 处理 ==========
    # 方法唯一修复：统一字段映射表，_normalize_tick_fields只做一次遍历
    _TICK_FIELD_ALIASES = {
        'InstrumentID': 'instrument_id',
        'LastPrice': 'last_price',
        'Volume': 'volume',
        'Timestamp': 'ts',
        'timestamp': 'ts',
        'UpdateTime': 'ts',
        'DateTime': 'ts',
        'datetime': 'ts',
        'BidPrice1': 'bid_price1',
        'AskPrice1': 'ask_price1',
        'OpenInterest': 'open_interest',
        'Amount': 'amount',
        'Turnover': 'turnover',
        'ExchangeID': 'exchange',
    }

    def _normalize_tick_fields(self, tick: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化Tick字段名：统一为 PythonGO 格式
        使用_TICK_FIELD_ALIASES映射表，单次遍历完成所有字段转换
        """
        normalized = tick.copy()
        for src, dst in self._TICK_FIELD_ALIASES.items():
            if dst not in normalized and src in normalized:
                normalized[dst] = normalized[src]
        return normalized
    
    def process_tick(self, tick: Dict[str, Any]) -> None:
        if not self._validate_tick(tick):
            return
        
        # ✅ 标准化字段名
        tick = self._normalize_tick_fields(tick)
        
        tick_ts = self._to_timestamp(tick.get('ts'))
        if tick_ts is None:
            logging.error("process_tick 时间戳转换失败：%s", tick.get('timestamp') or tick.get('ts'))
            return
        tick['timestamp'] = tick_ts
        instrument = tick.get('instrument_id')
        price = tick.get('last_price')

        normalized_id = str(instrument or '').strip()
        # ✅ 传递渠道唯一：通过 ParamsService 获取
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
                # ✅ 传递渠道唯一：通过 ParamsService 获取
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
    
    def _is_ext_kline_missing(self, instrument: str, period: str, current_time: float) -> bool:
        key = (instrument, period)
        with self._ext_kline_lock:
            last_time = self._last_ext_kline.get(key)
        
        if last_time is None:
            return True
        
        # 解析周期分钟数
        try:
            minutes = int(period.replace('min', ''))
        except (ValueError, AttributeError) as e:
            logging.error("无效周期格式：%s, 错误：%s", period, e)
            return True
        
        timeout = minutes * 60 * 2.0  # 秒，timeout factor=2.0
        return (current_time - last_time) > timeout
    
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
        # ✅ 传递渠道唯一：通过 ParamsService 获取
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

    # 方法唯一修复：K线提供者探测链，按优先级有序
    _KLINE_PROVIDER_PROBES = [
        ('get_kline_data', lambda p: p),
        ('get_kline', lambda p: p),
    ]

    @staticmethod
    def _resolve_kline_provider(provider: Any) -> Tuple[Optional[Any], str]:
        if provider is None:
            return None, 'none'
        for method_name, resolver in InstrumentDataManager._KLINE_PROVIDER_PROBES:
            target = resolver(provider)
            if callable(getattr(target, method_name.rsplit('.', 1)[-1], None)):
                return target, method_name
        return None, 'unavailable'

    @staticmethod
    def _estimate_kline_count(history_minutes: int, kline_style: str) -> int:
        style = str(kline_style or 'M1').upper()
        if style.startswith('M'):
            try:
                period_minutes = max(1, int(style[1:]))
            except ValueError:
                period_minutes = 1
        else:
            period_minutes = 1

        return max(1, int(history_minutes / period_minutes))

    @staticmethod
    def _normalize_kline_period(kline_style: str) -> str:
        style = str(kline_style or 'M1').upper()
        if style.startswith('M'):
            suffix = style[1:] or '1'
            return f"{suffix}min"
        return style.lower()

    def _fetch_historical_kline_data(
        self,
        provider: Any,
        exchange: str,
        instrument_id: str,
        kline_style: str,
        history_minutes: int,
        start_time: datetime,
        end_time: datetime,
    ) -> Any:
        resolved_provider, provider_type = self._resolve_kline_provider(provider)
        if resolved_provider is None:
            raise RuntimeError('no historical kline provider available')

        provider_class = type(resolved_provider).__name__
        provider_module = getattr(type(resolved_provider), '__module__', '')

        if provider_type in ('get_kline_data', 'market_center.get_kline_data'):
            get_kline_data = getattr(resolved_provider, 'get_kline_data')
            count = self._estimate_kline_count(history_minutes, kline_style)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

            def _log_probe_once(stage: str, call_name: str) -> None:
                probe_key = (stage, provider_type, provider_module, provider_class, call_name)
                attr_name = '_hkl_provider_preprobe_emitted' if stage == 'before' else '_hkl_provider_probe_emitted'
                with self._lock:
                    emitted = getattr(self, attr_name, set())
                    if probe_key in emitted:
                        return
                    emitted.add(probe_key)
                    setattr(self, attr_name, emitted)
                if stage == 'before':
                    logging.info(
                        "[HKLProbeBefore] provider_type=%s provider_class=%s provider_module=%s call=%s exchange=%s instrument=%s style=%s count=%s start_time=%s end_time=%s",
                        provider_type,
                        provider_class,
                        provider_module,
                        call_name,
                        exchange,
                        instrument_id,
                        kline_style,
                        count,
                        start_time,
                        end_time,
                    )
                else:
                    logging.info(
                        "[HKLProbe] provider_type=%s provider_class=%s provider_module=%s call=%s exchange=%s instrument=%s style=%s count=%s",
                        provider_type,
                        provider_class,
                        provider_module,
                        call_name,
                        exchange,
                        instrument_id,
                        kline_style,
                        count,
                    )

            def _try_calls(call_specs: List[Tuple[str, Callable[[], Any]]], allow_date_error: bool = False) -> Any:
                last_exc = None
                for call_name, call in call_specs:
                    _log_probe_once('before', call_name)
                    try:
                        result = call()
                        _log_probe_once('after', call_name)
                        return result
                    except (TypeError, AttributeError) as exc:
                        last_exc = exc
                        continue
                    except Exception as exc:
                        last_exc = exc
                        if allow_date_error and ('Parsing Date Err' in str(exc) or 'Date Err' in str(exc)):
                            logging.debug("[Storage] get_kline_data 时间参数调用失败，跳过 %s: %s", call_name, exc)
                            continue
                        logging.debug("[Storage] get_kline_data 调用失败，跳过 %s: %s", call_name, exc)
                        continue
                if last_exc is not None:
                    logging.debug(
                        "[Storage] provider=%s instrument=%s 未匹配到可用历史K线调用签名，最后错误：%s",
                        provider_type,
                        instrument_id,
                        last_exc,
                    )
                return None

            # 方法唯一修复：统一签名探测表，数据驱动替代10个lambda
            kline_call_specs = [
                ('kw_style_count', False, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, count=count)),
                ('pos_style_count', False, lambda: get_kline_data(exchange, instrument_id, kline_style, count)),
                ('kw_period_count', False, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, period=kline_style, count=count)),
                ('kw_style_tstr', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, start_time=start_time_str, end_time=end_time_str)),
                ('kw_style_dt', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, start_time=start_time, end_time=end_time)),
                ('kw_period_tstr', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, period=kline_style, start_time=start_time_str, end_time=end_time_str)),
            ]
            for name, allow_date_error, call_fn in kline_call_specs:
                result = _try_calls([(name, call_fn)], allow_date_error=allow_date_error)
                if result is not None:
                    return result

            raise RuntimeError(f'get_kline_data provider has no compatible signature for {instrument_id}')

        get_kline = getattr(resolved_provider, 'get_kline')
        count = self._estimate_kline_count(history_minutes, kline_style)
        try:
            return get_kline(
                exchange=exchange,
                instrument_id=instrument_id,
                style=kline_style,
                count=count,
            )
        except TypeError:
            return get_kline(exchange, instrument_id, kline_style, count)

    def load_historical_klines(self, instruments: List[str], history_minutes: int = 1440,
                              kline_style: str = 'M1', market_center: Any = None,
                              batch_size: Optional[int] = None,
                              inter_batch_delay_sec: float = 0.0,
                              request_delay_sec: float = 0.0,
                              progress_callback: Optional[Callable[[Dict[str, int]], None]] = None) -> Dict[str, int]:
        resolved_provider, provider_type = self._resolve_kline_provider(market_center)
        if not resolved_provider:
            logging.warning("[Storage] 历史K线提供者不可用，无法加载历史K线")
            return {
                'success': 0,
                'failed': len(instruments),
                'total_klines': 0,
                'fetched_klines': 0,
                'enqueued_klines': 0,
                'persisted_klines': 0,
                'queue_received_delta': 0,
                'queue_written_delta': 0,
            }

        from datetime import timedelta
        import time as time_module

        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=history_minutes)

        logging.info(
            f"[Storage] 开始为 {len(instruments)} 个合约加载历史K线: "
            f"{start_time} -> {end_time}, 周期={kline_style}, provider={provider_type}"
        )

        success_count = 0
        failed_count = 0
        total_klines = 0
        fetched_klines = 0
        normalized_period = self._normalize_kline_period(kline_style)

        effective_batch_size = max(1, int(batch_size or len(instruments) or 1))
        total_batches = math.ceil(len(instruments) / effective_batch_size) if instruments else 0

        for batch_index, start in enumerate(range(0, len(instruments), effective_batch_size), start=1):
            batch_instruments = instruments[start:start + effective_batch_size]
            batch_enqueued_klines = 0

            logging.info(
                "[Storage] 历史K线批次 %d/%d 开始: batch_size=%d, request_delay=%.3fs, inter_batch_delay=%.3fs",
                batch_index,
                total_batches,
                len(batch_instruments),
                request_delay_sec,
                inter_batch_delay_sec,
            )

            for instrument_index, instrument_str in enumerate(batch_instruments, start=1):
                try:
                    if '.' in instrument_str:
                        exchange, instrument_id = instrument_str.split('.', 1)
                    else:
                        exchange = None
                        instrument_id = instrument_str

                    if exchange:
                        logging.info(f"[Storage] 加载 {exchange}.{instrument_id} 历史 K 线")
                    else:
                        logging.info(f"[Storage] 加载 {instrument_id} 历史 K 线")

                    normalized_id = str(instrument_id or '').strip()
                    # ✅ 传递渠道唯一：通过 ParamsService 获取
                    info = self._params_service.get_instrument_meta_by_id(normalized_id)
                    if info is None:
                        info = self._get_instrument_info(normalized_id)
                    if info is None:
                        warn_key = ('load_historical_klines', normalized_id)
                        with self._lock:
                            if warn_key not in self._runtime_missing_warned:
                                self._runtime_missing_warned.add(warn_key)
                                logging.warning("[load_historical_klines] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
                        failed_count += 1
                        continue

                    if not exchange:
                        exchange = self.infer_exchange_from_id(instrument_id)

                    kline_data = self._fetch_historical_kline_data(
                        resolved_provider,
                        exchange,
                        instrument_id,
                        kline_style,
                        history_minutes,
                        start_time,
                        end_time,
                    )

                    if kline_data and len(kline_data) > 0:
                        fetched_klines += len(kline_data)
                        normalized_klines = []
                        for kline in kline_data:
                            try:
                                ts = self._to_timestamp(
                                    getattr(kline, 'timestamp', getattr(kline, 'ts', time_module.time()))
                                )
                                if ts is None:
                                    raise ValueError('invalid historical kline timestamp')

                                normalized_klines.append({
                                    'ts': ts,
                                    'instrument_id': instrument_id,
                                    'exchange': exchange,
                                    'open': getattr(kline, 'open', getattr(kline, 'Open', 0.0)),
                                    'high': getattr(kline, 'high', getattr(kline, 'High', 0.0)),
                                    'low': getattr(kline, 'low', getattr(kline, 'Low', 0.0)),
                                    'close': getattr(kline, 'close', getattr(kline, 'Close', 0.0)),
                                    'volume': getattr(kline, 'volume', getattr(kline, 'Volume', 0)),
                                    'open_interest': getattr(kline, 'open_interest', getattr(kline, 'OpenInterest', 0)),
                                    'period': normalized_period,
                                })
                            except Exception as e:
                                logging.warning(f"[Storage] 保存K线失败 {instrument_id}: {e}")

                        if not normalized_klines:
                            failed_count += 1
                            continue

                        self._wait_for_queue_capacity(
                            max_fill_rate=60.0,
                            timeout_sec=max(5.0, inter_batch_delay_sec * 20 if inter_batch_delay_sec > 0 else 5.0),
                            source='load_historical_klines',
                        )

                        internal_id = self._get_info_internal_id(info)
                        instrument_type = info.get('type', 'future')
                        enqueue_failed = False
                        for chunk_start in range(0, len(normalized_klines), self.batch_size):
                            chunk = normalized_klines[chunk_start:chunk_start + self.batch_size]
                            if not self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, normalized_period):
                                enqueue_failed = True
                                break

                        if enqueue_failed:
                            logging.warning(f"[Storage] ⚠️ {instrument_id}: 历史K线入队失败")
                            failed_count += 1
                            continue

                        with self._ext_kline_lock:
                            self._last_ext_kline[(instrument_id, normalized_period)] = normalized_klines[-1]['ts']

                        saved_count = len(normalized_klines)
                        success_count += 1
                        total_klines += saved_count
                        batch_enqueued_klines += saved_count
                        logging.info(f"[Storage] ✅ {instrument_id}: 已批量入队 {saved_count} 条K线")
                    else:
                        logging.warning(f"[Storage] ⚠️ {instrument_id}: 无历史K线数据")
                        failed_count += 1

                except Exception as e:
                    logging.error(f"[Storage] 加载历史K线失败 {instrument_str}: {e}")
                    failed_count += 1

                if request_delay_sec > 0 and instrument_index < len(batch_instruments):
                    time_module.sleep(request_delay_sec)

            logging.info(
                "[Storage] 历史K线批次 %d/%d 完成: success=%d, failed=%d, fetched_klines=%d, enqueued_klines=%d",
                batch_index,
                total_batches,
                success_count,
                failed_count,
                fetched_klines,
                batch_enqueued_klines,
            )

            if progress_callback:
                try:
                    progress_callback({
                        'success': success_count,
                        'failed': failed_count,
                        'total_klines': total_klines,
                        'fetched_klines': fetched_klines,
                        'enqueued_klines': batch_enqueued_klines,
                        'batch_index': batch_index,
                        'total_batches': total_batches,
                    })
                except Exception as callback_exc:
                    logging.debug(f"[Storage] 历史K线进度回调失败: {callback_exc}")

            if inter_batch_delay_sec > 0 and batch_index < total_batches:
                time_module.sleep(inter_batch_delay_sec)

        queue_stats_after = self.get_queue_stats()
        queue_received_delta = queue_stats_after.get('total_received', 0)
        queue_written_delta = queue_stats_after.get('total_written', 0)
        drops_delta = queue_stats_after.get('drops_count', 0)
        actual_enqueued = max(0, total_klines - drops_delta)
        logging.info(
            f"[Storage] 历史K线加载完成: 成功={success_count}, 失败={failed_count}, "
            f"抓取K线={fetched_klines} 条, 入队K线={actual_enqueued} 条(原始={total_klines}, 丢弃={drops_delta}), "
            f"落盘K线={queue_written_delta} 条"
        )

        return {
            'success': success_count,
            'failed': failed_count,
            'total_klines': total_klines,
            'fetched_klines': fetched_klines,
            'enqueued_klines': actual_enqueued,
            'persisted_klines': queue_written_delta,
            'queue_received_delta': queue_received_delta,
            'queue_written_delta': queue_written_delta,
        }
    
    # ========== 通用查询辅助方法 ==========
    
    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """
        根据期货合约代码获取期权链（委托给QueryService）。
        
        Args:
            future_instrument_id: 期货合约代码
            
        Returns:
            Dict: 期权链信息
        """
        # ✅ 统一委托给QueryService.get_option_chain_for_future
        from ali2026v3_trading.query_service import get_query_service
        qs = get_query_service()
        return qs.get_option_chain_for_future(future_instrument_id)
    
    # ========== 批量操作 ==========
    def batch_add_future_instruments(self, instruments: List[Dict]) -> None:
        try:
            for inst in instruments:
                exchange = inst.get('exchange', 'AUTO')
                instrument_id = inst.get('instrument_id')
                if not instrument_id:
                    continue
                try:
                    self.register_instrument(instrument_id, exchange,
                                           expire_date=inst.get('expire_date'),
                                           listing_date=inst.get('listing_date'))
                except Exception as e:
                    logging.warning("批量导入期货失败 %s: %s", instrument_id, e)
            logging.info("批量导入 %d 个期货合约", len(instruments))
        except Exception as e:
            logging.error("批量导入期货失败：%s", e)
            raise
    
    def batch_add_option_instruments(self, instruments: List[Dict]) -> None:
        try:
            for inst in instruments:
                exchange = inst.get('exchange', 'AUTO')
                instrument_id = inst.get('instrument_id')
                if not instrument_id:
                    continue
                try:
                    self.register_instrument(instrument_id, exchange,
                                           expire_date=inst.get('expire_date'),
                                           listing_date=inst.get('listing_date'))
                except Exception as e:
                    logging.warning("批量导入期权失败 %s: %s", instrument_id, e)
            logging.info("批量导入 %d 个期权合约", len(instruments))
        except Exception as e:
            logging.error("批量导入期权失败：%s", e)
            raise
    
    def batch_write_kline(self, instrument_id: str, kline_data: List[Dict], period: str = '1min') -> None:
        if not kline_data:
            return
        internal_id = self.register_instrument(instrument_id)
        info = self._get_info_by_id(internal_id)
        if not info:
            return
        instrument_type = info.get('type', 'future')
        
        # 分批入队，避免单次数据过大
        chunk_size = self.batch_size
        for i in range(0, len(kline_data), chunk_size):
            chunk = kline_data[i:i+chunk_size]
            self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, period)
    
    def batch_write_tick(self, instrument_id: str, tick_data: List[Dict]) -> None:
        if not tick_data:
            return
        internal_id = self.register_instrument(instrument_id)
        info = self._get_info_by_id(internal_id)
        if not info:
            return
        instrument_type = info.get('type', 'future')
        
        # 分批入队
        chunk_size = self.batch_size
        for i in range(0, len(tick_data), chunk_size):
            chunk = tick_data[i:i+chunk_size]
            self._enqueue_write('_save_tick_impl', internal_id, instrument_type, chunk)
    
    # ========== 高级查询 ==========
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
    
    
    # ========== 数据导出 ==========

    
    # ========== 底层写入方法（storage.py 兼容） ==========
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
    
    # ========== Query Methods (storage.py compatible) ==========
    def query_kline(self, instrument_id: str, start_time: str, end_time: str,
                    limit: Optional[int] = None) -> List[Dict]:
        internal_id = self.register_instrument(instrument_id)
        return self.query_kline_by_id(internal_id, start_time, end_time, limit)
    
    def query_tick(self, instrument_id: str, start_time: str, end_time: str,
                   limit: Optional[int] = None) -> List[Dict]:
        internal_id = self.register_instrument(instrument_id)
        return self.query_tick_by_id(internal_id, start_time, end_time, limit)
    
    def get_option_chain(self, ts: float, underlying: str, expiration: str) -> List[Dict[str, Any]]:
        # ✅ ID直通：从数据库查询已注册的期货合约，不自行构造
        try:
            rows = self._data_service.query(
                "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                [underlying, expiration]
            ).to_pylist()
            
            if not rows:
                logging.warning(f"[Storage] 期货合约未注册: {underlying}{expiration}，无法查询期权链")
                return []
            
            future_instrument_id = rows[0]['instrument_id']  # 使用数据库中的原始值
            chain = self.get_option_chain_for_future(future_instrument_id)
            return chain.get('options', [])
        except Exception as e:
            logging.error("get_option_chain failed: %s", e)
            return []
    
    def get_latest_underlying(self, underlying: str, expiration: str) -> Optional[Dict[str, Any]]:
        # This would require actual underlying_snapshot table implementation
        # For now, return None or can be extended
        logging.debug("get_latest_underlying called: %s %s", underlying, expiration)
        return None
    
    # ========== 自动清理线程 ==========
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
    
    def save_tick(self, tick: Dict[str, Any]) -> None:
        if not self._validate_tick(tick):
            return
        
        tick = self._normalize_tick_fields(tick)
        
        ts = self._to_timestamp(tick.get('ts'))
        if ts is None:
            logging.error("save_tick 时间戳转换失败：%s", tick.get('ts'))
            return
        
        try:
            internal_id = self.register_instrument(tick['instrument_id'])
            info = self._get_info_by_id(internal_id)
            if not info:
                return
            instrument_type = info.get('type', 'future')
            instrument_id = info.get('instrument_id')
            
            arrow_batch = self._data_service.build_tick_arrow_batch([tick], instrument_id)
            if arrow_batch is not None:
                self._enqueue_write('_save_tick_impl', internal_id, instrument_type, arrow_batch)
        except ValueError as e:
            logging.debug("save_tick register failed: %s", e)
    
    def _save_depth_batch_impl(self, depth_list: List[Dict[str, Any]]):
        for d in depth_list:
            try:
                internal_id = self.register_instrument(d['instrument_id'])
            except ValueError as e:
                # 合约无法识别时静默跳过，避免阻塞队列
                logging.debug("_save_depth_batch_impl register failed: %s", e)
                continue
            
            info = self._get_info_by_id(internal_id)
            if not info:
                continue
            
            # 使用统一的 ticks_raw 表（不再使用合约专属 tick_table）
            table_name = info.get('tick_table', 'ticks_raw')
            self._validate_table_name(table_name)
            
            # 构建字段列表（根据实际数据动态调整）- ✅ 全部使用 .get()
            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            placeholders = ['?', '?', '?', '?']
            values = [d.get('ts') or d.get('timestamp'), 
                      d.get('last_price', 0.0), 
                      d.get('volume', 0), 
                      d.get('open_interest', 0)]
            
            # 添加 bid/ask 价格
            for i in range(1, 6):  # 5 档行情
                bid_key = f'bid_price{i}'
                ask_key = f'ask_price{i}'
                if bid_key in d:
                    fields.append(bid_key)
                    placeholders.append('?')
                    values.append(d[bid_key])
                if ask_key in d:
                    fields.append(ask_key)
                    placeholders.append('?')
                    values.append(d[ask_key])
            
            field_str = ', '.join(fields)
            placeholder_str = ', '.join(placeholders)
            update_fields = ', '.join([f"{f} = excluded.{f}" for f in fields[1:]])
            
            self._data_service.query(f"""
                INSERT INTO {table_name}
                ({field_str})
                VALUES ({placeholder_str})
                ON CONFLICT(timestamp) DO UPDATE SET
                {update_fields}
            """, values, raise_on_error=True)
    
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
    
    def _validate_signal(self, signal: Dict[str, Any]) -> bool:
        if not isinstance(signal, dict):
            logging.error("save_signal 参数不是字典：%s", type(signal))
            return False
        required = ['ts', 'instrument_id', 'signal_type', 'price', 'score', 'strategy_name']
        for field in required:
            if field not in signal:
                logging.error("save_signal 缺少必要字段：%s", field)
                return False
        price = signal.get('price')
        if price is not None and (not isinstance(price, (int, float)) or price <= 0):
            logging.error("save_signal 价格无效：%s", price)
            return False
        score = signal.get('score')
        if score is not None and (not isinstance(score, (int, float)) or score < 0 or score > 1):
            logging.error("save_signal 评分无效（应为 0-1）: %s", score)
            return False
        return True
    
    def _save_signal_impl(self, signal: Dict[str, Any]):
        try:
            internal_id = self.register_instrument(signal['instrument_id'])
        except ValueError as e:
            # 合约无法识别时静默跳过
            logging.debug("_save_signal_impl register failed: %s", e)
            return
        
        info = self._get_info_by_id(internal_id)
        if not info:
            logging.error("_save_signal_impl 找不到合约信息：%s", signal['instrument_id'])
            return
        
        # 简化实现，保存到 kv_store
        key = f"signal:{signal['ts']}:{signal['instrument_id']}:{signal['strategy_name']}"
        self.save(key, signal)
    
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
        logging.debug("保存标的物快照：%s %s", data.get('underlying'), data.get('expiration'))
    
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
    
    def _validate_underlying(self, data: Dict[str, Any]) -> bool:
        if not isinstance(data, dict):
            logging.error("save_underlying_snapshot 参数不是字典：%s", type(data))
            return False
        required = ['ts', 'underlying', 'expiration']
        for field in required:
            if field not in data:
                logging.error("save_underlying_snapshot 缺少必要字段：%s", field)
                return False
        last_price = data.get('last_price')
        if last_price is not None and (not isinstance(last_price, (int, float)) or last_price < 0):
            logging.error("save_underlying_snapshot 价格无效：%s", last_price)
            return False
        return True
    
    def _save_option_snapshot_batch_impl(self, data_list: List[Dict[str, Any]]):
        for d in data_list:
            try:
                internal_id = self.register_instrument(d['instrument_id'])
            except ValueError as e:
                # 合约无法识别时静默跳过
                logging.debug("_save_option_snapshot_batch_impl register failed: %s", e)
                continue
            
            info = self._get_info_by_id(internal_id)
            if not info:
                continue
            
            # 使用统一的 ticks_raw 表（不再使用合约专属 tick_table）
            table_name = info.get('tick_table', 'ticks_raw')
            self._validate_table_name(table_name)
            
            # 期权 Tick 包含希腊字母 - ✅ 全部使用 .get()
            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            values = [d.get('ts') or d.get('timestamp'), 
                      d.get('last_price', 0.0), 
                      d.get('volume', 0), 
                      d.get('open_interest', 0)]
            
            # 添加 bid/ask
            if 'bid_price1' in d:
                fields.append('bid_price1')
                values.append(d['bid_price1'])
            if 'ask_price1' in d:
                fields.append('ask_price1')
                values.append(d['ask_price1'])
            
            # 添加期权特有字段
            option_fields = ['implied_volatility', 'delta', 'gamma', 'theta', 'vega']
            for field in option_fields:
                if field in d:
                    fields.append(field)
                    values.append(d[field])
            
            field_str = ', '.join(fields)
            placeholder_str = ', '.join(['?'] * len(fields))
            update_fields = ', '.join([f"{f} = excluded.{f}" for f in fields[1:]])
            
            self._data_service.query(f"""
                INSERT INTO {table_name}
                ({field_str})
                VALUES ({placeholder_str})
                ON CONFLICT(timestamp) DO UPDATE SET
                {update_fields}
            """, values, raise_on_error=True)
    
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
    
    # ========== 数据源加载（完整版） ==========
    @staticmethod
    def load_all_instruments(strategy_instance=None, params=None, logger=None):
        import logging as stdlib_logging
        
        if logger is None:
            logger = stdlib_logging.getLogger(__name__)
        
        futures_list = []
        options_dict = {}
        
        try:
            if strategy_instance is not None:
                try:
                    instruments = getattr(strategy_instance, 'subscribed_instruments', {})
                    for inst_id, inst_info in instruments.items():
                        try:
                            product = inst_get(inst_info, 'product', 'Product', 'underlying_product')
                            instrument_id = inst_get(inst_info, 'instrument_id', 'InstrumentID', '_instrument_id')
                            
                            if not instrument_id:
                                continue
                            
                            is_option = False
                            try:
                                SubscriptionManager.parse_option(instrument_id)
                                is_option = True
                            except (ValueError, Exception):
                                pass
                            
                            if is_option:
                                underlying_future_id = inst_get(inst_info, 'underlying_future_id')
                                if underlying_future_id:
                                    try:
                                        from ali2026v3_trading.data_service import get_data_service
                                        ds = get_data_service()
                                        future_rows = ds.query(
                                            "SELECT instrument_id FROM futures_instruments WHERE internal_id = ?",
                                            [underlying_future_id]
                                        ).to_pylist()
                                        if future_rows and len(future_rows) > 0:
                                            future_instrument_id = future_rows[0]['instrument_id']
                                        else:
                                            continue
                                    except Exception:
                                        continue
                                    
                                    if future_instrument_id not in options_dict:
                                        options_dict[future_instrument_id] = []
                                    options_dict[future_instrument_id].append({
                                        'instrument_id': instrument_id,
                                        'product': product,
                                        'underlying_future_id': underlying_future_id,
                                        'exchange': inst_get(inst_info, 'exchange', 'ExchangeID'),
                                        'year_month': inst_get(inst_info, 'year_month', 'DeliveryMonth'),
                                        'strike_price': inst_get(inst_info, 'strike_price', 'StrikePrice'),
                                        'option_type': inst_get(inst_info, 'option_type', 'OptionsType')
                                    })
                            else:
                                futures_list.append({
                                    'instrument_id': instrument_id,
                                    'product': product,
                                    'exchange': inst_get(inst_info, 'exchange', 'ExchangeID'),
                                    'year_month': inst_get(inst_info, 'year_month', 'DeliveryMonth')
                                })
                        except Exception as e:
                            logger.debug("跳过合约处理失败：%s", e)
                except Exception as e:
                    logger.warning("从策略实例获取合约失败：%s", e)
            
            if not futures_list and not options_dict and params is not None:
                try:
                    configured_instruments = getattr(params, 'instrument_ids', [])
                    for inst_id in configured_instruments:
                        try:
                            if isinstance(inst_id, str):
                                is_option = False
                                opt_parsed = None
                                try:
                                    opt_parsed = SubscriptionManager.parse_option(inst_id)
                                    is_option = True
                                except (ValueError, Exception):
                                    pass
                                
                                if is_option and opt_parsed:
                                    underlying = opt_parsed.get('underlying', '')
                                    if not underlying:
                                        underlying = SubscriptionManager.parse_future(inst_id).get('product', inst_id)
                                    if underlying not in options_dict:
                                        options_dict[underlying] = []
                                    options_dict[underlying].append({
                                        'instrument_id': inst_id,
                                        'underlying': underlying
                                    })
                                else:
                                    futures_list.append({
                                        'instrument_id': inst_id
                                    })
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug("从 params 获取合约失败：%s", e)
            
            logger.info("加载完成：%d 个期货，%d 个品种期权", len(futures_list), len(options_dict))
            return futures_list, options_dict
            
        except Exception as e:
            logger.error("load_all_instruments 异常：%s", e)
            return [], {}

    # ========== 事务管理 ==========
    def cleanup_old_data(self, table: str, days: int, condition: str = "") -> int:
        if days <= 0:
            return 0
            
        # 白名单验证表名，防止 SQL 注入
        allowed_tables = {
            'tick', 'kline', 'depth_market', 'underlying_snapshot',
            'option_snapshot', 'strategy_signals', 'external_klines'
        }
        if table not in allowed_tables:
            logging.error("无效的表名（不在白名单中）：%s", table)
            raise ValueError(f"表名 '{table}' 不在允许列表中")
            
        # 计算截止时间戳（秒）
        cutoff = time.time() - days * 86400
            
        # 参数化查询，防止 SQL 注入
        sql = f"DELETE FROM {table} WHERE ts < ?"
        params = [cutoff]
            
        # 如果 condition 存在，只允许简单的 AND 条件且必须参数化
        if condition:
            # 简单验证：只允许 AND column=value 格式
            match = re.match(r"^\s+AND\s+(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?$", condition, re.IGNORECASE)
            if match:
                col_name = match.group(1)
                col_value = match.group(2)
                sql += f" AND {col_name} = ?"
                params.append(col_value)
            else:
                logging.error("无效的 condition 格式，只支持 ' AND column=value' 格式：%s", condition)
                raise ValueError("condition 格式错误")
        # 检查表是否存在
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
            except Exception:
                pass
            logging.error("清理数据失败：%s", e)
            return 0

    # ========== 通用状态持久化（KV 存储） ==========
    def _init_kv_store(self):
        self._data_service.query("""
            CREATE TABLE IF NOT EXISTS app_kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """, raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_app_kv_updated_at ON app_kv_store(updated_at DESC)", raise_on_error=True)
        logging.debug("KV 存储表初始化完成")

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

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
                # ✅ 使用异步队列（默认，高性能）
                return self._enqueue_write('_save_kv_impl', key, payload)
            else:
                self._save_kv_impl(key, payload)
                return True
        except Exception as e:
            logging.error(f"save 异常：{e}")
            return False

    def load(self, key: str) -> Optional[Any]:
        if not key or not isinstance(key, str):
            logging.error("load 失败：key 无效：%s", key)
            return None

        try:
            rows = self._data_service.query("SELECT value FROM app_kv_store WHERE key = ?", [key]).to_pylist()
            row = rows[0] if rows else None
            if not row:
                return None
            return json.loads(row['value'])
        except Exception as e:
            logging.error("load 失败 key=%s: %s", key, e, exc_info=True)
            return None

    @contextmanager
    def transaction(self):
        try:
            yield
        except Exception as e:
            raise

    # ========== 资源管理 ==========
    def close(self):
        self._shutdown_impl(flush=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# ========== 全局 InstrumentDataManager 单例 ==========
_instrument_data_manager_instance: Optional[InstrumentDataManager] = None
_instrument_data_manager_lock = threading.Lock()


def get_instrument_data_manager() -> InstrumentDataManager:
    global _instrument_data_manager_instance
    
    with _instrument_data_manager_lock:
        if _instrument_data_manager_instance is None:
            # 使用默认配置创建全局单例
            from ali2026v3_trading.storage import _get_default_db_path
            db_path = _get_default_db_path()
            
            # 创建全局单例（使用默认配置：max_connections=20, async_queue_size=500000）
            _instrument_data_manager_instance = InstrumentDataManager(
                db_path=db_path,
                max_retries=3,
                retry_delay=0.1,
                async_queue_size=500000,
                batch_size=5000,
                drop_on_full=True,
                cleanup_interval=3600,
                cleanup_config={'tick': 30, 'kline': 90}
            )
            
            logging.info(f"[Storage] Global InstrumentDataManager singleton initialized (db={db_path})")
        
        return _instrument_data_manager_instance