"""
instrument_data_manager.py
期货/期权数据管理核心模块
设计理念：原样订阅 -> 内部ID映射 -> 原样保存
所有操作基于合约内部ID，避免运行时正则解析。
支持期货和期权的K线/Tick数据分月存储，提供订阅、写入、查询、清理等全生命周期管理。
"""

import sqlite3
import logging
import os
import re
import threading
import time
import json
import queue
import weakref
import math
from typing import List, Dict, Optional, Any, Tuple
from contextlib import contextmanager
from datetime import datetime, time as dt_time
from functools import wraps

# ========== 重试机制装饰器 ==========
def retry_on_sqlite_error(max_retries=3, base_delay=0.1, backoff=2):
    """
    装饰器：在 SQLite 操作失败时进行指数退避重试。
    仅重试可恢复的错误（如数据库锁定、磁盘 I/O 错误等），其他错误直接抛出。
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.Error as e:
                    # 判断是否可重试（OperationalError 中的锁定/超时等）
                    if isinstance(e, sqlite3.OperationalError) and 'locked' in str(e).lower():
                        last_exception = e
                        delay = base_delay * (backoff ** attempt)
                        time.sleep(delay)
                        continue
                    else:
                        # 不可重试的错误直接抛出
                        raise
                except Exception as e:
                    # 非 SQLite 错误直接抛出，记录详细日志
                    logging.error(
                        f"[retry_on_sqlite_error] Non-SQLite error during database operation: {e}",
                        exc_info=True
                    )
                    raise
            # 重试耗尽，抛出最后一个异常
            raise last_exception
        return wrapper
    return decorator


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _get_default_db_path() -> str:
    """Resolve the default database path using config_service first, then a local fallback."""
    try:
        from ali2026v3_trading.config_service import get_default_db_path

        db_path = get_default_db_path()
        if db_path:
            return db_path
    except Exception as exc:
        logging.warning("[storage] Failed to resolve db path from config_service: %s", exc)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, 'trading_data.db')


class InstrumentDataManager:
    """期货/期权数据管理核心模块"""
    
    # ✅ VERSION MARKER - Helps identify if platform is using cached bytecode
    __version__ = "2026-03-26-FIXED-dbpath-explicit"
    
    # 安全表名正则：仅允许字母、数字、下划线，防止 SQL 注入
    _TABLE_NAME_PATTERN = re.compile(r'^[A-Za-z0-9_]+$')
    
    # 支持的 K 线周期（单位：分钟），可根据需要扩展
    SUPPORTED_PERIODS = ['1min', '5min', '15min', '30min', '60min']

    def __init__(self, db_path: Optional[str] = None, max_retries: int = 3, retry_delay: float = 0.1,
                 async_queue_size: int = 100000, batch_size: int = 5000,
                 drop_on_full: bool = True, max_connections: int = 20,
                 cleanup_interval: Optional[int] = 3600,
                 cleanup_config: Optional[Dict[str, int]] = None):
        """
        初始化数据库连接，创建元数据表，启用 WAL 模式
        :param db_path: SQLite 数据库文件路径，未提供时自动使用默认路径
        :param max_retries: 数据库操作最大重试次数（处理锁超时）
        :param retry_delay: 重试间隔（秒）
        :param async_queue_size: 异步写入队列最大长度
        :param batch_size: 批量写入分块大小
        :param drop_on_full: 队列满时是否丢弃新数据（True）或阻塞等待（False）
        :param max_connections: 最大数据库连接数（连接池使用）
        :param cleanup_interval: 自动清理间隔（秒），None 表示不启动自动清理
        :param cleanup_config: 清理配置，如 {'tick': 30, 'kline': 90} 表示保留天数
        """
        self.db_path = db_path or _get_default_db_path()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        self.drop_on_full = drop_on_full

        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        # 连接池（优先使用连接池而非单连接）
        self._conn_pool = _ConnectionPool(self.db_path, max_connections)
        self.conn = None  # 向后兼容，实际使用_conn_pool
        
        # 异步写入队列和线程
        self._write_queue = queue.Queue(maxsize=async_queue_size)
        self._stop_event = threading.Event()
        self._writer_thread = None
        
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
        
        self._instrument_cache = {}          # instrument_id -> info
        self._id_cache = {}                  # internal_id -> info
        self._product_cache = {}             # product -> product_info
        self._lock = threading.RLock()       # 保证线程安全
        self._ext_kline_lock = threading.Lock()  # 外部 K 线时间戳锁
        self._agg_lock = threading.Lock()        # K 线聚合器锁
        self._column_cache: Dict[str, List[str]] = {}  # 列名缓存
        self._column_cache_lock = threading.Lock()
        
        # 线程本地存储（用于存储当前线程持有的连接）
        self._thread_local = threading.local()
        
        # 外部 K 线最近写入时间记录：{(instrument_id, period): last_timestamp}
        self._last_ext_kline: Dict[Tuple[str, str], float] = {}
        
        # K 线聚合器字典：{(instrument_id, period): _KlineAggregator}
        self._aggregators: Dict[Tuple[str, str], '_KlineAggregator'] = {}
        
        # 统一订阅管理器（新增）
        self.subscription_manager = SubscriptionManager(self, max_retries=max_retries)
        
        # 自动清理配置
        self._cleanup_interval = cleanup_interval
        self._cleanup_config = cleanup_config or {}
        self._cleanup_thread = None
        self._closed = False
        
        # 初始化
        self._init_connection()
        self._init_kv_store()
        # 归还初始连接
        self._conn_pool.release_connection(self.conn)
        self._start_async_writer()
        if self._cleanup_interval:
            self._start_cleanup_thread()
        
        # 列名缓存预加载（可选优化）
        self._preload_column_cache()
    
    @property
    def connection(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接（从连接池获取）。"""
        if not hasattr(self._thread_local, "connection"):
            conn = self._conn_pool.get_connection()
            self._thread_local.connection = conn
        return self._thread_local.connection
    
    def close_connection(self) -> None:
        """释放当前线程持有的连接，归还给连接池。"""
        if hasattr(self._thread_local, "connection"):
            conn = self._thread_local.connection
            self._conn_pool.release_connection(conn)
            del self._thread_local.connection

    def _init_connection(self):
        """建立连接，启用外键约束和 WAL 模式，创建元数据表"""
        # 使用连接池获取一个初始连接（用于初始化）
        self.conn = self._conn_pool.get_connection()
        self._create_metadata_tables()
        self._migrate_legacy_schema()  # ✅ 添加表迁移
        self._create_indexes()
        self._load_caches()
        # 注意：不在此处归还连接，因为 __init__ 中还需要调用 _init_kv_store
        # 连接将在 __init__ 结束时归还

    def _validate_table_name(self, table_name: str):
        """验证表名安全，防止 SQL 注入"""
        if not self._TABLE_NAME_PATTERN.match(table_name):
            raise ValueError(f"非法表名：{table_name}")
        
    # ========== 异步写入队列核心 ==========
    def _start_async_writer(self):
        """启动后台写入线程。"""
        self._writer_thread = threading.Thread(target=self._async_writer_loop, daemon=True)
        self._writer_thread.start()
        logging.info("[AsyncWriter] 后台写入线程已启动")
        
    def _stop_async_writer(self):
        """停止后台写入线程。"""
        if self._writer_thread and self._writer_thread.is_alive():
            self._stop_event.set()
            self._writer_thread.join(timeout=5.0)
            logging.info("[AsyncWriter] 后台写入线程已停止")
        
    def _async_writer_loop(self):
        """后台写入循环，批量处理队列中的写入任务。"""
        batch = []
        while not self._stop_event.is_set():
            try:
                # 等待新任务或超时
                try:
                    item = self._write_queue.get(timeout=1.0)
                    batch.append(item)
                except queue.Empty:
                    pass
                    
                # 如果批次满了或者有新数据且队列为空，执行写入
                if len(batch) >= self.batch_size or (batch and self._write_queue.empty()):
                    written_count = self._flush_batch_to_db(batch)
                    with self._queue_stats_lock:
                        self._queue_stats['total_written'] += written_count
                    batch.clear()
                        
            except Exception as e:
                logging.error("[AsyncWriter] 写入异常：%s", e, exc_info=True)
                if batch:
                    batch.clear()
            
        # 退出前清空剩余数据
        if batch:
            try:
                written_count = self._flush_batch_to_db(batch)
                with self._queue_stats_lock:
                    self._queue_stats['total_written'] += written_count
            except Exception as e:
                logging.error("[AsyncWriter] 最终刷新失败：%s", e)
        
    def _enqueue_write(self, func_name: str, *args, **kwargs) -> bool:
        """
        将写入任务加入异步队列。
        :param func_name: 要调用的方法名（如 '_save_tick_impl'）
        :param args: 位置参数
        :param kwargs: 关键字参数
        :return: 是否成功入队
        """
        if self._stop_event.is_set():
            logging.warning("写入线程已停止，拒绝新任务：%s", func_name)
            return False
        
        # ✅ 更新统计
        with self._queue_stats_lock:
            self._queue_stats['total_received'] += 1
        
        task = (func_name, args, kwargs)
            
        try:
            if self.drop_on_full:
                # 队列满时丢弃最新数据
                try:
                    self._write_queue.put_nowait(task)
                    
                    # ✅ 监控队列使用率
                    queue_size = self._write_queue.qsize()
                    max_size = self._write_queue.maxsize
                    fill_rate = queue_size / max_size * 100
                    
                    # 更新最大使用量记录
                    with self._queue_stats_lock:
                        if queue_size > self._queue_stats['max_queue_size_seen']:
                            self._queue_stats['max_queue_size_seen'] = queue_size
                    
                    # 预警机制
                    if fill_rate > 80:
                        logging.warning(
                            f"⚠️ 队列使用率超过 80%: {fill_rate:.1f}% ({queue_size}/{max_size}) - "
                            f"方法：{func_name}"
                        )
                    elif fill_rate > 50:
                        # 每 100 条记录一次 50% 使用率
                        if queue_size % 100 == 0:
                            logging.info(f"📊 队列使用率：{fill_rate:.1f}% ({queue_size}/{max_size})")
                    
                    return True
                except queue.Full:
                    with self._queue_stats_lock:
                        self._queue_stats['drops_count'] += 1
                    logging.error("写入队列已满，数据被丢弃：%s", func_name)
                    return False
            else:
                # 阻塞等待
                try:
                    self._write_queue.put(task, block=True, timeout=1)
                    return True
                except queue.Full:
                    logging.error("写入队列已满，等待超时，数据可能丢失：%s", func_name)
                    return False
        except Exception as e:
            logging.error("[AsyncWriter] 入队失败：%s", e)
            return False
        
    def _flush_batch_to_db(self, batch: List[Tuple[str, Any, Any]]) -> int:
        """批量刷新到数据库，返回成功处理的任务数。"""
        if not batch:
            return 0
            
        conn = None
        executed_count = 0
        try:
            conn = self._conn_pool.get_connection()
            cursor = conn.cursor()
                
            for func_name, args, kwargs in batch:
                # 根据函数名调用对应的实现
                if hasattr(self, func_name):
                    method = getattr(self, func_name)
                    method(cursor, *args, **kwargs)
                    executed_count += 1
                else:
                    logging.warning("[AsyncWriter] 未找到写入方法：%s", func_name)
                
            conn.commit()
            return executed_count
        except sqlite3.Error as e:
            logging.error("[AsyncWriter] 批量写入失败：%s", e)
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                self._conn_pool.release_connection(conn)
        
    def get_queue_stats(self) -> Dict[str, int]:
        """获取队列统计信息。"""
        with self._queue_stats_lock:
            stats = self._queue_stats.copy()
        
        # 添加当前队列大小
        stats['current_queue_size'] = self._write_queue.qsize()
        stats['max_queue_size'] = self._write_queue.maxsize
        
        if stats['max_queue_size'] > 0:
            stats['fill_rate'] = stats['current_queue_size'] / stats['max_queue_size'] * 100
        else:
            stats['fill_rate'] = 0.0
        
        return stats
    
    def _shutdown_impl(self, flush: bool = True) -> None:
        """
        优雅关闭：停止后台线程，可选择等待队列清空。
        Args:
            flush: True 则等待所有待处理任务完成；False 则丢弃队列中剩余任务。
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True

        logging.info("开始关闭...")
        self._stop_event.set()

        if not flush:
            # 清空队列
            while not self._write_queue.empty():
                try:
                    self._write_queue.get_nowait()
                    self._write_queue.task_done()
                except queue.Empty:
                    break

        if self._writer_thread:
            # 等待写入线程完成，最多 30 秒
            timeout = 30.0
            self._writer_thread.join(timeout=timeout)
            
            if self._writer_thread.is_alive():
                remaining_tasks = self._write_queue.qsize()
                if remaining_tasks > 0:
                    logging.error("关闭时仍有 %d 个任务未完成，可能丢失数据", remaining_tasks)
                else:
                    logging.warning("写入线程未能在规定时间内停止")
        
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

        # 关闭所有连接
        self._conn_pool.close_all()
        logging.info("关闭完成")

    def _execute_with_retry(self, func, *args, **kwargs):
        """带重试的数据库执行，处理锁超时"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

    def _create_metadata_tables(self):
        cursor = self.conn.cursor()
    
        # 期货品种元数据表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS future_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product TEXT NOT NULL UNIQUE,
                exchange TEXT NOT NULL,
                format_template TEXT NOT NULL,
                tick_size REAL NOT NULL DEFAULT 0.2,
                contract_size REAL NOT NULL,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        """)
    
        # 期权品种元数据表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS option_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product TEXT NOT NULL UNIQUE,
                exchange TEXT NOT NULL,
                underlying_product TEXT NOT NULL,
                format_template TEXT NOT NULL,
                tick_size REAL NOT NULL DEFAULT 0.2,
                contract_size REAL NOT NULL,
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (underlying_product) REFERENCES future_products(product)
            )
        """)
    
        # 期货合约表（核心：内部 ID 映射）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS futures_instruments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                instrument_id TEXT NOT NULL UNIQUE,
                product TEXT NOT NULL,
                year_month TEXT NOT NULL,
                format TEXT NOT NULL,
                expire_date DATETIME,
                listing_date DATETIME,
                kline_table TEXT NOT NULL,
                tick_table TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (product) REFERENCES future_products(product)
            )
        """)
    
        # 期权合约表（核心：内部 ID 映射）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS option_instruments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                instrument_id TEXT NOT NULL UNIQUE,
                product TEXT NOT NULL,
                underlying_future TEXT NOT NULL,
                underlying_product TEXT NOT NULL,
                year_month TEXT NOT NULL,
                option_type TEXT NOT NULL,
                strike_price REAL NOT NULL,
                format TEXT NOT NULL,
                expire_date DATETIME,
                listing_date DATETIME,
                kline_table TEXT NOT NULL,
                tick_table TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (underlying_future) REFERENCES futures_instruments(instrument_id),
                FOREIGN KEY (underlying_product) REFERENCES option_products(product)
            )
        """)
    
        # 订阅管理表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_id INTEGER NOT NULL,
                data_type TEXT NOT NULL,
                kline_period TEXT,
                subscribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        """)
    
        self.conn.commit()
        logging.info("元数据表创建/检查完成")
        
    def _migrate_legacy_schema(self):
        """迁移旧版数据库表结构，添加缺失的列"""
        cursor = self.conn.cursor()
        migrated = False
            
        try:
            # 检查 futures_instruments 表
            cursor.execute("PRAGMA table_info(futures_instruments)")
            futures_columns = [row[1] for row in cursor.fetchall()]  # 只取列名
                
            if 'kline_table' not in futures_columns:
                logging.info("正在迁移 futures_instruments 表：添加 kline_table 列...")
                cursor.execute(
                    "ALTER TABLE futures_instruments ADD COLUMN kline_table TEXT"
                )
                migrated = True
            else:
                logging.debug("futures_instruments.kline_table 已存在")
            
            if 'tick_table' not in futures_columns:
                logging.info("正在迁移 futures_instruments 表：添加 tick_table 列...")
                cursor.execute(
                    "ALTER TABLE futures_instruments ADD COLUMN tick_table TEXT"
                )
                migrated = True
            else:
                logging.debug("futures_instruments.tick_table 已存在")
                
            # 检查 option_instruments 表
            cursor.execute("PRAGMA table_info(option_instruments)")
            option_columns = [row[1] for row in cursor.fetchall()]  # 只取列名
                
            if 'kline_table' not in option_columns:
                logging.info("正在迁移 option_instruments 表：添加 kline_table 列...")
                cursor.execute(
                    "ALTER TABLE option_instruments ADD COLUMN kline_table TEXT"
                )
                migrated = True
            else:
                logging.debug("option_instruments.kline_table 已存在")
            
            if 'tick_table' not in option_columns:
                logging.info("正在迁移 option_instruments 表：添加 tick_table 列...")
                cursor.execute(
                    "ALTER TABLE option_instruments ADD COLUMN tick_table TEXT"
                )
                migrated = True
            else:
                logging.debug("option_instruments.tick_table 已存在")

            cursor.execute("""
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
            """)
            if cursor.rowcount > 0:
                migrated = True
                logging.info(
                    "正在修复 option_instruments.underlying_product 历史错误值：%d 条",
                    cursor.rowcount,
                )
                
            if migrated:
                self.conn.commit()
                logging.info("✅ 数据库表结构迁移完成")
            else:
                logging.debug("数据库表结构已是最新版本，无需迁移")
                    
        except Exception as e:
            logging.error(f"表结构迁移失败：{e}")
            self.conn.rollback()
            raise

    def _create_indexes(self):
        cursor = self.conn.cursor()
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_future_product ON futures_instruments(product)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_future_yym ON futures_instruments(year_month)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_option_product ON option_instruments(product)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_option_underlying ON option_instruments(underlying_future)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sub_instrument ON subscriptions(instrument_id)")
        self.conn.commit()
        logging.info("索引创建/检查完成")

    def _load_caches(self):
        """加载所有合约映射到内存缓存"""
        cursor = self.conn.cursor()
        # 加载期货
        cursor.execute("SELECT id, instrument_id, product, year_month, kline_table, tick_table FROM futures_instruments WHERE is_active=1")
        for row in cursor.fetchall():
            info = {
                'id': row['id'],
                'type': 'future',
                'product': row['product'],
                'year_month': row['year_month'],
                'kline_table': row['kline_table'],
                'tick_table': row['tick_table']
            }
            self._instrument_cache[row['instrument_id']] = info
            self._id_cache[row['id']] = info
        # 加载期权
        cursor.execute("SELECT id, instrument_id, product, year_month, option_type, strike_price, kline_table, tick_table FROM option_instruments WHERE is_active=1")
        for row in cursor.fetchall():
            info = {
                'id': row['id'],
                'type': 'option',
                'product': row['product'],
                'year_month': row['year_month'],
                'option_type': row['option_type'],
                'strike_price': row['strike_price'],
                'kline_table': row['kline_table'],
                'tick_table': row['tick_table']
            }
            self._instrument_cache[row['instrument_id']] = info
            self._id_cache[row['id']] = info
        # 加载品种信息
        cursor.execute("SELECT product, exchange, format_template, tick_size, contract_size FROM future_products WHERE is_active=1")
        for row in cursor.fetchall():
            self._product_cache[row['product']] = {'type': 'future', **dict(row)}
        cursor.execute("SELECT product, exchange, underlying_product, format_template, tick_size, contract_size FROM option_products WHERE is_active=1")
        for row in cursor.fetchall():
            self._product_cache[row['product']] = {'type': 'option', **dict(row)}
        logging.info(f"加载缓存：{len(self._instrument_cache)}个活跃合约，{len(self._product_cache)}个活跃品种")

    # ========== 合约解析（简单直接） ==========
    def _parse_future(self, instrument_id: str) -> Dict[str, Any]:
        """解析期货合约"""
        match = re.match(r'^([A-Za-z]+)(\d{4})$', instrument_id)
        if not match:
            raise ValueError(f"无法解析期货：{instrument_id}")
        return {
            'product': match.group(1),
            'year_month': match.group(2)
        }

    def _parse_option_with_dash(self, instrument_id: str) -> Dict[str, Any]:
        """解析带连字符的期权"""
        match = re.match(r'^([A-Za-z]+)(\d{4})-([CP])-(\d+(?:\.\d+)?)$', instrument_id)
        if not match:
            raise ValueError(f"无法解析期权：{instrument_id}")
        return {
            'product': match.group(1),
            'year_month': match.group(2),
            'option_type': match.group(3),
            'strike_price': float(match.group(4))
        }

    def classify_instruments(self, instrument_ids: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
        """
        分类合约 ID 列表为期货和期权
        
        Args:
            instrument_ids: 合约 ID 列表
            
        Returns:
            Tuple[List[str], Dict[str, List[str]]]: (futures_list, options_dict)
            - futures_list: 期货合约列表
            - options_dict: 期权合约字典 {underlying: [option_ids]}
        """
        futures_list = []
        options_dict = {}
        
        for inst_id in instrument_ids:
            try:
                # 使用精确正则解析判断是期货还是期权
                parsed = self._parse_option_with_dash(inst_id)
                # 是期权
                underlying = inst_id.split('-')[0]
                if underlying not in options_dict:
                    options_dict[underlying] = []
                options_dict[underlying].append(inst_id)
            except ValueError:
                # 不是标准期权格式，归类为期货
                futures_list.append(inst_id)
        
        return futures_list, options_dict
        
    def infer_exchange_from_id(self, instrument_id: str) -> str:
        """
        从合约 ID 推断交易所（基于品种前缀）。
            
        Args:
            instrument_id: 合约 ID（如 'IF2603', 'CU2406'）
                
        Returns:
            str: 交易所代码（如 'CFFEX', 'SHFE'）
        """
        # CFFEX
        if instrument_id.startswith(('IF', 'IH', 'IC', 'IM', 'IO', 'HO', 'MO')):
            return 'CFFEX'
        # SHFE
        elif instrument_id.startswith(('CU', 'AL', 'ZN', 'RB', 'AU', 'AG', 'NI', 'SN', 'PB', 'SS', 'WR', 'SI', 'RU', 'NR', 'BC', 'LU', 'SC')):
            return 'SHFE'
        # DCE
        elif instrument_id.startswith(('M', 'Y', 'A', 'JM', 'I', 'C', 'CS', 'JD', 'L', 'V', 'PP', 'EG', 'PG', 'J', 'P')):
            return 'DCE'
        # CZCE
        elif instrument_id.startswith(('CF', 'SR', 'MA', 'TA', 'RM', 'OI', 'SA', 'PF', 'EB', 'LC')):
            return 'CZCE'
        # INE
        elif instrument_id.startswith(('SC',)):
            return 'INE'
        # GFEX
        elif instrument_id.startswith(('LC',)):
            return 'GFEX'
        else:
            # 默认返回 CFFEX（中金所）
            return 'CFFEX'
    
    # ========== 合约信息查询（缓存优先） ==========
    def _get_instrument_info(self, instrument_id: str) -> Optional[dict]:
        """获取合约信息（优先缓存，否则查询数据库）"""
        if instrument_id in self._instrument_cache:
            return self._instrument_cache[instrument_id]
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, kline_table, tick_table FROM futures_instruments WHERE instrument_id=?", (instrument_id,))
        row = cursor.fetchone()
        if row:
            info = {
                'id': row['id'],
                'type': 'future',
                'kline_table': row['kline_table'],
                'tick_table': row['tick_table']
            }
            self._instrument_cache[instrument_id] = info
            self._id_cache[row['id']] = info
            return info
        cursor.execute("SELECT id, kline_table, tick_table FROM option_instruments WHERE instrument_id=?", (instrument_id,))
        row = cursor.fetchone()
        if row:
            info = {
                'id': row['id'],
                'type': 'option',
                'kline_table': row['kline_table'],
                'tick_table': row['tick_table']
            }
            self._instrument_cache[instrument_id] = info
            self._id_cache[row['id']] = info
            return info
        return None

    def _get_info_by_id(self, internal_id: int) -> Optional[dict]:
        """根据内部ID获取合约信息（优先缓存）"""
        if internal_id in self._id_cache:
            return self._id_cache[internal_id]
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, 'future' as type, kline_table, tick_table FROM futures_instruments WHERE id=?", (internal_id,))
        row = cursor.fetchone()
        if row:
            info = dict(row)
            self._id_cache[internal_id] = info
            return info
        cursor.execute("SELECT id, 'option' as type, kline_table, tick_table FROM option_instruments WHERE id=?", (internal_id,))
        row = cursor.fetchone()
        if row:
            info = dict(row)
            self._id_cache[internal_id] = info
            return info
        return None

    # ========== 辅助方法 ==========
    def get_active_instruments_by_product(self, product: str) -> List[str]:
        """
        获取指定品种的所有活跃合约
        
        Args:
            product: 品种代码（如 'IF', 'IO'）
            
        Returns:
            List[str]: 合约 ID 列表
        """
        cursor = self.conn.cursor()
        instrument_ids = []
        
        # 查询期货
        cursor.execute("""
            SELECT instrument_id FROM futures_instruments 
            WHERE product=? AND is_active=1
            ORDER BY instrument_id
        """, (product,))
        for row in cursor.fetchall():
            instrument_ids.append(row['instrument_id'])
        
        # 查询期权
        cursor.execute("""
            SELECT instrument_id FROM option_instruments 
            WHERE product=? AND is_active=1
            ORDER BY instrument_id
        """, (product,))
        for row in cursor.fetchall():
            instrument_ids.append(row['instrument_id'])
        
        return instrument_ids
    
    def get_active_instruments_by_products(self, products: List[str]) -> List[str]:
        """
        获取多个品种的所有活跃合约
        
        Args:
            products: 品种代码列表（如 ['IF', 'IH', 'IC']）
            
        Returns:
            List[str]: 合约 ID 列表
        """
        all_instrument_ids = []
        for product in products:
            instrument_ids = self.get_active_instruments_by_product(product.strip())
            all_instrument_ids.extend(instrument_ids)
        return all_instrument_ids
    
    def get_current_month_contracts(self, product: str) -> List[str]:
        """
        获取指定品种的当月合约
        
        Args:
            product: 品种代码
            
        Returns:
            List[str]: 当月合约 ID 列表
        """
        from datetime import datetime
        current_month = datetime.now().strftime('%y%m')
        
        all_instruments = self.get_active_instruments_by_product(product)
        current_month_contracts = []
        
        for inst_id in all_instruments:
            # 提取年月部分（如 IF2603 -> 2603）
            match = re.search(r'(\d{4})', inst_id)
            if match and match.group(1) == current_month:
                current_month_contracts.append(inst_id)
        
        return current_month_contracts
    
    def get_next_month_contracts(self, product: str) -> List[str]:
        """
        获取指定品种的下月合约
        
        Args:
            product: 品种代码
            
        Returns:
            List[str]: 下月合约 ID 列表
        """
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        next_month = (datetime.now() + relativedelta(months=1)).strftime('%y%m')
        
        all_instruments = self.get_active_instruments_by_product(product)
        next_month_contracts = []
        
        for inst_id in all_instruments:
            # 提取年月部分（如 IF2603 -> 2603）
            match = re.search(r'(\d{4})', inst_id)
            if match and match.group(1) == next_month:
                next_month_contracts.append(inst_id)
        
        return next_month_contracts
    
    def _get_next_id(self, table_prefix: str) -> int:
        """获取下一个可用的内部ID（线程安全）"""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table_prefix}_instruments")
        return cursor.fetchone()[0]

    def _create_future_tables_by_id(self, instrument_id: int):
        """根据内部ID创建期货数据表"""
        cursor = self.conn.cursor()
        kline_table = f"kline_future_{instrument_id}"
        tick_table = f"tick_future_{instrument_id}"
        self._validate_table_name(kline_table)
        self._validate_table_name(tick_table)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {kline_table} (
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL,
                PRIMARY KEY (timestamp)
            ) WITHOUT ROWID
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {tick_table} (
                timestamp DATETIME NOT NULL,
                last_price REAL NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL,
                bid_price1 REAL,
                ask_price1 REAL,
                PRIMARY KEY (timestamp)
            ) WITHOUT ROWID
        """)
        self.conn.commit()
        logging.debug(f"创建期货表：{kline_table}, {tick_table}")

    def _create_option_tables_by_id(self, instrument_id: int):
        """根据内部ID创建期权数据表"""
        cursor = self.conn.cursor()
        kline_table = f"kline_option_{instrument_id}"
        tick_table = f"tick_option_{instrument_id}"
        self._validate_table_name(kline_table)
        self._validate_table_name(tick_table)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {kline_table} (
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL,
                PRIMARY KEY (timestamp)
            ) WITHOUT ROWID
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {tick_table} (
                timestamp DATETIME NOT NULL,
                last_price REAL NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL,
                bid_price1 REAL,
                ask_price1 REAL,
                implied_volatility REAL,
                delta REAL,
                gamma REAL,
                theta REAL,
                vega REAL,
                PRIMARY KEY (timestamp)
            ) WITHOUT ROWID
        """)
        self.conn.commit()
        logging.debug(f"创建期权表：{kline_table}, {tick_table}")

    # ========== 核心：合约注册 ==========
    def register_instrument(self, instrument_id: str, exchange: str = "AUTO",
                            expire_date: Optional[str] = None,
                            listing_date: Optional[str] = None) -> int:
        """
        注册合约（期货或期权），返回内部ID
        如果合约已存在，直接返回ID
        """
        # 检查缓存
        if instrument_id in self._instrument_cache:
            return self._instrument_cache[instrument_id]['id']

        # 查询数据库
        info = self._get_instrument_info(instrument_id)
        if info:
            return info['id']

        # 注册新合约（加锁保证唯一性）
        with self._lock:
            # 再次检查（避免重复插入）
            if instrument_id in self._instrument_cache:
                return self._instrument_cache[instrument_id]['id']
            cursor = self.conn.cursor()

            # 尝试注册为期货
            cursor.execute("SELECT product, exchange, format_template FROM future_products WHERE is_active=1")
            for prod in cursor.fetchall():
                if instrument_id.startswith(prod['product']):
                    try:
                        parsed = self._parse_future(instrument_id)
                        # 生成新ID
                        new_id = self._get_next_id('futures')
                        kline_table = f"kline_future_{new_id}"
                        tick_table = f"tick_future_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        cursor.execute("""
                            INSERT INTO futures_instruments
                            (exchange, instrument_id, product, year_month, format,
                             expire_date, listing_date, kline_table, tick_table)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (exchange, instrument_id, parsed['product'], parsed['year_month'],
                              prod['format_template'], expire_date, listing_date, kline_table, tick_table))
                        self.conn.commit()
                        self._create_future_tables_by_id(new_id)
                        # 更新缓存
                        info = {
                            'id': new_id,
                            'type': 'future',
                            'product': parsed['product'],
                            'year_month': parsed['year_month'],
                            'kline_table': kline_table,
                            'tick_table': tick_table
                        }
                        self._instrument_cache[instrument_id] = info
                        self._id_cache[new_id] = info
                        logging.info(f"注册期货合约：{instrument_id} -> ID={new_id}")
                        return new_id
                    except ValueError:
                        continue

            # 尝试注册为期权
            cursor.execute("SELECT product, exchange, underlying_product, format_template FROM option_products WHERE is_active=1")
            for prod in cursor.fetchall():
                if instrument_id.startswith(prod['product']):
                    try:
                        parsed = self._parse_option_with_dash(instrument_id)
                        # 查找标的期货 ID（如果没有，先注册期货）
                        underlying_future = None
                        cursor.execute("SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                       (prod['underlying_product'], parsed['year_month']))
                        row = cursor.fetchone()
                        if row:
                            underlying_future = row[0]
                        else:
                            # 自动注册标的期货
                            future_instrument_id = f"{prod['underlying_product']}{parsed['year_month']}"
                            logging.info("期权注册：标的期货不存在，先注册 %s", future_instrument_id)
                                                    
                            # ✅ 检查是否已存在 (避免并发插入)
                            cursor.execute("SELECT id, instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                           (prod['underlying_product'], parsed['year_month']))
                            row = cursor.fetchone()
                                                    
                            if row:
                                # 已存在，直接使用
                                new_future_id = row[0]
                                underlying_future = row[1]
                                logging.info("标的期货已存在：%s -> ID=%d", underlying_future, new_future_id)
                            else:
                                # 不存在，插入新记录
                                cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM futures_instruments")
                                new_future_id = cursor.fetchone()[0]
                                kline_future_table = f"kline_future_{new_future_id}"
                                tick_future_table = f"tick_future_{new_future_id}"
                                self._validate_table_name(kline_future_table)
                                self._validate_table_name(tick_future_table)
                                                        
                                cursor.execute("""
                                    INSERT INTO futures_instruments
                                    (exchange, instrument_id, product, year_month, format, kline_table, tick_table)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (exchange, future_instrument_id, prod['underlying_product'], parsed['year_month'],
                                      prod['format_template'], kline_future_table, tick_future_table))
                                                        
                                # 创建数据表（使用同一个连接）
                                self._validate_table_name(kline_future_table)
                                self._validate_table_name(tick_future_table)
                                cursor.execute(f"""
                                    CREATE TABLE IF NOT EXISTS {kline_future_table} (
                                        timestamp DATETIME NOT NULL,
                                        open REAL NOT NULL,
                                        high REAL NOT NULL,
                                        low REAL NOT NULL,
                                        close REAL NOT NULL,
                                        volume INTEGER NOT NULL,
                                        open_interest INTEGER NOT NULL,
                                        PRIMARY KEY (timestamp)
                                    ) WITHOUT ROWID
                                """)
                                cursor.execute(f"""
                                    CREATE TABLE IF NOT EXISTS {tick_future_table} (
                                        timestamp DATETIME NOT NULL,
                                        last_price REAL NOT NULL,
                                        volume INTEGER NOT NULL,
                                        open_interest INTEGER NOT NULL,
                                        bid_price1 REAL,
                                        ask_price1 REAL,
                                        PRIMARY KEY (timestamp)
                                    ) WITHOUT ROWID
                                """)
                                                        
                                underlying_future = future_instrument_id
                                logging.info("自动注册标的期货：%s -> ID=%d", future_instrument_id, new_future_id)
                        new_id = self._get_next_id('option')
                        kline_table = f"kline_option_{new_id}"
                        tick_table = f"tick_option_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        cursor.execute("""
                            INSERT INTO option_instruments
                            (exchange, instrument_id, product, underlying_future, underlying_product,
                             year_month, option_type, strike_price, format,
                             expire_date, listing_date, kline_table, tick_table)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (exchange, instrument_id, parsed['product'], underlying_future,
                              prod['product'], parsed['year_month'],
                              parsed['option_type'], parsed['strike_price'], prod['format_template'],
                              expire_date, listing_date, kline_table, tick_table))
                        self.conn.commit()
                        self._create_option_tables_by_id(new_id)
                        # 更新缓存
                        info = {
                            'id': new_id,
                            'type': 'option',
                            'product': parsed['product'],
                            'year_month': parsed['year_month'],
                            'option_type': parsed['option_type'],
                            'strike_price': parsed['strike_price'],
                            'kline_table': kline_table,
                            'tick_table': tick_table
                        }
                        self._instrument_cache[instrument_id] = info
                        self._id_cache[new_id] = info
                        logging.info(f"注册期权合约：{instrument_id} -> ID={new_id}")
                        return new_id
                    except ValueError:
                        continue

            raise ValueError(f"无法识别合约品种：{instrument_id}")

    # ========== 订阅管理 ==========
    def subscribe(self, instrument_id: str, data_type: str, kline_period: Optional[str] = None) -> int:
        """
        订阅合约数据，返回内部ID
        :param instrument_id: 原始合约代码
        :param data_type: 'kline' 或 'tick'
        :param kline_period: 仅当 data_type='kline'时，指定周期如 '1min'
        """
        try:
            internal_id = self.register_instrument(instrument_id)
        except ValueError as e:
            logging.error("subscribe failed for %s: %s", instrument_id, e)
            return None
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO subscriptions
            (instrument_id, data_type, kline_period)
            VALUES (?, ?, ?)
        """, (internal_id, data_type, kline_period))
        self.conn.commit()
        logging.info(f"订阅成功：{instrument_id} (ID={internal_id})")
        return internal_id

    def unsubscribe(self, instrument_id: str, data_type: Optional[str] = None):
        """取消订阅"""
        try:
            internal_id = self.register_instrument(instrument_id)
        except ValueError as e:
            logging.error("unsubscribe failed for %s: %s", instrument_id, e)
            return
        cursor = self.conn.cursor()
        if data_type:
            cursor.execute("UPDATE subscriptions SET is_active=0 WHERE instrument_id=? AND data_type=?", (internal_id, data_type))
        else:
            cursor.execute("UPDATE subscriptions SET is_active=0 WHERE instrument_id=?", (internal_id,))
        self.conn.commit()
        logging.info(f"取消订阅：{instrument_id} (ID={internal_id})")

    # ========== 数据写入 ==========
    def write_kline_by_id(self, instrument_id: int, kline_data: List[Dict]) -> None:
        """通过内部 ID 写入 K 线数据"""
        if not kline_data:
            return
        info = self._get_info_by_id(instrument_id)
        if not info:
            # 合约不存在时静默跳过
            logging.debug("write_kline_by_id: instrument_id %d not found", instrument_id)
            return
        table_name = info['kline_table']
        self._validate_table_name(table_name)

        cursor = self.conn.cursor()
        values = [(k.get('ts') or k.get('timestamp'), 
                   k.get('open', 0.0), 
                   k.get('high', 0.0), 
                   k.get('low', 0.0),
                   k.get('close', 0.0), 
                   k.get('volume', 0), 
                   k.get('open_interest', 0)) for k in kline_data]
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name}
            (timestamp, open, high, low, close, volume, open_interest)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)
        self.conn.commit()
        logging.debug(f"写入 {len(kline_data)} 条K线到 {table_name}")

    def write_tick_by_id(self, instrument_id: int, tick_data: List[Dict]) -> None:
        """通过内部 ID 写入 Tick 数据（自动判断期货/期权）"""
        if not tick_data:
            return
        info = self._get_info_by_id(instrument_id)
        if not info:
            # 合约不存在时静默跳过
            logging.debug("write_tick_by_id: instrument_id %d not found", instrument_id)
            return
        table_name = info['tick_table']
        self._validate_table_name(table_name)
    
        cursor = self.conn.cursor()
        if info['type'] == 'future':
            values = [(t.get('ts') or t.get('timestamp'), 
                       t.get('last_price', 0.0), 
                       t.get('volume', 0), 
                       t.get('open_interest', 0),
                       t.get('bid_price1'), 
                       t.get('ask_price1')) for t in tick_data]
            sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
                VALUES (?, ?, ?, ?, ?, ?)
            """
        else:
            values = [(t.get('ts') or t.get('timestamp'), 
                       t.get('last_price', 0.0), 
                       t.get('volume', 0), 
                       t.get('open_interest', 0),
                       t.get('bid_price1'), 
                       t.get('ask_price1'),
                       t.get('implied_volatility'), 
                       t.get('delta'), 
                       t.get('gamma'),
                       t.get('theta'), 
                       t.get('vega')) for t in tick_data]
            sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1,
                 implied_volatility, delta, gamma, theta, vega)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        cursor.executemany(sql, values)
        self.conn.commit()
        logging.debug(f"写入 {len(tick_data)} 条 Tick 到 {table_name}")
    
    # ========== 异步队列实现方法 ==========
    def _save_kline_impl(self, cursor, instrument_id: int, kline_data: List[Dict], period: str = '1min') -> None:
        """K 线数据保存实现（供异步队列使用）"""
        if not kline_data:
            return
        info = self._get_info_by_id(instrument_id)
        if not info:
            # 合约信息不存在时静默跳过
            logging.debug("_save_kline_impl: instrument_id %d not found, skipping", instrument_id)
            return
        table_name = info['kline_table']
        self._validate_table_name(table_name)
    
        values = [(k.get('ts') or k.get('timestamp'), 
                   k.get('open', 0.0), 
                   k.get('high', 0.0), 
                   k.get('low', 0.0),
                   k.get('close', 0.0), 
                   k.get('volume', 0), 
                   k.get('open_interest', 0)) for k in kline_data]
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name}
            (timestamp, open, high, low, close, volume, open_interest)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)
    
    def _save_tick_impl(self, cursor, instrument_id: int, tick_data: List[Dict]) -> None:
        """Tick 数据保存实现（供异步队列使用）"""
        if not tick_data:
            return
        info = self._get_info_by_id(instrument_id)
        if not info:
            # 合约信息不存在时静默跳过，避免抛出异常阻塞队列
            logging.debug("_save_tick_impl: instrument_id %d not found, skipping", instrument_id)
            return
        table_name = info['tick_table']
        self._validate_table_name(table_name)
    
        if info['type'] == 'future':
            values = [(t.get('ts') or t.get('timestamp'), 
                       t.get('last_price', 0.0), 
                       t.get('volume', 0), 
                       t.get('open_interest', 0),
                       t.get('bid_price1'), 
                       t.get('ask_price1')) for t in tick_data]
            sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
                VALUES (?, ?, ?, ?, ?, ?)
            """
        else:
            values = [(t.get('ts') or t.get('timestamp'), 
                       t.get('last_price', 0.0), 
                       t.get('volume', 0), 
                       t.get('open_interest', 0),
                       t.get('bid_price1'), 
                       t.get('ask_price1'),
                       t.get('implied_volatility'), 
                       t.get('delta'), 
                       t.get('gamma'),
                       t.get('theta'), 
                       t.get('vega')) for t in tick_data]
            sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1,
                 implied_volatility, delta, gamma, theta, vega)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        cursor.executemany(sql, values)

    # ========== 数据查询 ==========
    def query_kline_by_id(self, instrument_id: int, start_time: str, end_time: str,
                          limit: Optional[int] = None) -> List[Dict]:
        """查询 K 线数据"""
        info = self._get_info_by_id(instrument_id)
        if not info:
            logging.debug("query_kline_by_id: instrument_id %d not found", instrument_id)
            return []
        table_name = info['kline_table']
        self._validate_table_name(table_name)

        cursor = self.conn.cursor()
        sql = f"""
            SELECT timestamp, open, high, low, close, volume, open_interest
            FROM {table_name}
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """
        if limit is not None:
            sql += f" LIMIT {limit}"
        cursor.execute(sql, (start_time, end_time))
        return [dict(row) for row in cursor.fetchall()]

    def query_tick_by_id(self, instrument_id: int, start_time: str, end_time: str,
                         limit: Optional[int] = None) -> List[Dict]:
        """查询 Tick 数据"""
        info = self._get_info_by_id(instrument_id)
        if not info:
            logging.debug("query_tick_by_id: instrument_id %d not found", instrument_id)
            return []
        table_name = info['tick_table']
        self._validate_table_name(table_name)

        cursor = self.conn.cursor()
        sql = f"SELECT * FROM {table_name} WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp"
        if limit is not None:
            sql += f" LIMIT {limit}"
        cursor.execute(sql, (start_time, end_time))
        return [dict(row) for row in cursor.fetchall()]

    # ========== 关联查询 ==========
    def get_option_chain_by_future_id(self, future_id: int) -> Dict:
        """根据期货内部ID获取期权链"""
        info = self._get_info_by_id(future_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货ID: {future_id}")
        future_instrument = None
        # 从缓存或数据库获取 instrument_id
        for k, v in self._instrument_cache.items():
            if v['id'] == future_id:
                future_instrument = k
                break
        if not future_instrument:
            cursor = self.conn.cursor()
            cursor.execute("SELECT instrument_id FROM futures_instruments WHERE id=?", (future_id,))
            row = cursor.fetchone()
            if row:
                future_instrument = row[0]
        if not future_instrument:
            raise ValueError(f"未找到期货合约ID: {future_id}")

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, instrument_id, option_type, strike_price
            FROM option_instruments
            WHERE underlying_future=?
            ORDER BY strike_price, option_type
        """, (future_instrument,))
        options = [dict(row) for row in cursor.fetchall()]

        return {
            'future': {'id': future_id, 'instrument_id': future_instrument},
            'options': options
        }

    # ========== 清理 ==========
    def delete_instrument(self, instrument_id: str) -> None:
        """删除合约及其数据表"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            logging.warning(f"合约不存在，无法删除：{instrument_id}")
            return

        if info['type'] == 'future':
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT instrument_id FROM option_instruments WHERE underlying_future=?",
                (instrument_id,),
            )
            dependent_options = [row[0] for row in cursor.fetchall()]
            for option_instrument_id in dependent_options:
                self.delete_instrument(option_instrument_id)

        internal_id = info['id']
        cursor = self.conn.cursor()
        # 删除数据表
        for table in [info['kline_table'], info['tick_table']]:
            self._validate_table_name(table)
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        # 删除元数据记录
        if info['type'] == 'future':
            cursor.execute("DELETE FROM futures_instruments WHERE id=?", (internal_id,))
        else:
            cursor.execute("DELETE FROM option_instruments WHERE id=?", (internal_id,))
        # 删除订阅记录
        cursor.execute("DELETE FROM subscriptions WHERE instrument_id=?", (internal_id,))
        self.conn.commit()
        # 清理缓存
        if instrument_id in self._instrument_cache:
            del self._instrument_cache[instrument_id]
        if internal_id in self._id_cache:
            del self._id_cache[internal_id]
        logging.info(f"已删除合约 {instrument_id} (ID={internal_id})")

    # ========== 时间戳转换工具 ==========
    @staticmethod
    def _to_timestamp(ts) -> Optional[float]:
        """
        将输入转换为 Unix 时间戳（浮点数，秒）。
        支持：datetime 对象、ISO 格式字符串、数字。
        转换失败或结果为 NaN/Inf 时返回 None。
        """
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
        """单条 tick 数据校验。"""
        if not isinstance(tick, dict):
            logging.error("save_tick 参数不是字典：%s", type(tick))
            return False
        required = ['instrument_id', 'last_price']
        for field in required:
            if field not in tick:
                logging.error("save_tick 缺少必要字段：%s", field)
                return False
        # ✅ 支持 ts 或 timestamp 字段
        if 'ts' not in tick and 'timestamp' not in tick:
            logging.error("save_tick 缺少时间戳字段 (ts 或 timestamp)")
            return False
        price = tick.get('last_price')
        if price is not None and (not isinstance(price, (int, float)) or price <= 0):
            logging.error("save_tick 价格无效：%s", price)
            return False
        volume = tick.get('volume', 0)
        if volume is not None and (not isinstance(volume, (int, float)) or volume < 0):
            logging.error("save_tick 成交量无效：%s", volume)
            return False
        return True
    
    def _validate_kline(self, kline: Dict[str, Any]) -> bool:
        """K 线数据校验。"""
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
    def process_tick(self, tick: Dict[str, Any]) -> None:
        """
        处理一条 tick 数据：
        1. 保存 tick 到数据库；
        2. 对于每个支持的周期，更新对应聚合器；
        3. 如果外部 K 线缺失，则将聚合器产生的 K 线保存。
        """
        # 校验
        if not self._validate_tick(tick):
            return
        
        # 转换时间戳
        tick = tick.copy()
        tick_ts = self._to_timestamp(tick.get('timestamp') or tick.get('ts'))
        if tick_ts is None:
            logging.error("process_tick 时间戳转换失败：%s", tick.get('timestamp') or tick.get('ts'))
            return
        tick['timestamp'] = tick_ts  # ✅ 统一使用 timestamp
        instrument = tick.get('instrument_id')
        price = tick.get('last_price')
        
        # 1. 保存 tick（添加异常保护）
        try:
            internal_id = self.register_instrument(instrument)
            tick_data = [tick]
            self.write_tick_by_id(internal_id, tick_data)
        except ValueError as e:
            logging.debug("process_tick register failed for %s: %s", instrument, e)
            return
        
        # 2. 对每个支持的周期进行处理
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
                    # 外部 K 线缺失，保存合成的 K 线
                    self.save_external_kline(completed_kline)
                else:
                    logging.debug("外部 K 线正常，丢弃合成的 K 线：%s %s %.3f",
                                  instrument, period, completed_kline['ts'])
    
    def _is_ext_kline_missing(self, instrument: str, period: str, current_time: float) -> bool:
        """判断外部 K 线是否缺失。"""
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
        """
        保存来自外部源的 K 线数据，并更新外部 K 线最近到达时间。
        """
        if not self._validate_kline(kline_data):
            return
        kline_data = kline_data.copy()
        ts = self._to_timestamp(kline_data.get('ts') or kline_data.get('timestamp'))
        if ts is None:
            logging.error("save_external_kline 时间戳转换失败：%s", kline_data.get('ts') or kline_data.get('timestamp'))
            return
        kline_data['ts'] = ts
        
        # 写入数据库（添加异常保护）
        try:
            internal_id = self.register_instrument(kline_data.get('instrument_id'))
            self.write_kline_by_id(internal_id, [kline_data])
        except ValueError as e:
            # 合约无法识别时静默跳过
            logging.debug("save_external_kline register failed: %s", e)
        
        # 更新时间戳 - ✅ 使用 .get() 安全访问
        key = (kline_data.get('instrument_id'), kline_data.get('period'))
        with self._ext_kline_lock:
            self._last_ext_kline[key] = ts
    
    def load_historical_klines(self, instruments: List[str], history_minutes: int = 1440, 
                              kline_style: str = 'M1', market_center: Any = None) -> Dict[str, int]:
        """加载多个合约的历史K线数据
        
        Args:
            instruments: 合约列表，格式为 ["EXCHANGE.INSTRUMENT_ID", ...]
            history_minutes: 历史数据分钟数，默认1440(24小时)
            kline_style: K线周期，默认M1
            market_center: MarketCenter实例，用于获取历史数据
            
        Returns:
            统计结果字典: {'success': 成功数量, 'failed': 失败数量, 'total_klines': 总K线数}
        """
        if not market_center:
            logging.warning("[Storage] market_center 未提供，无法加载历史K线")
            return {'success': 0, 'failed': len(instruments), 'total_klines': 0}
        
        from datetime import datetime, timedelta
        import time as time_module
        
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=history_minutes)
        
        logging.info(f"[Storage] 开始为 {len(instruments)} 个合约加载历史K线: {start_time} -> {end_time}, 周期={kline_style}")
        
        success_count = 0
        failed_count = 0
        total_klines = 0
        
        for instrument_str in instruments:
            try:
                # 解析合约字符串 (格式: EXCHANGE.INSTRUMENT_ID)
                if '.' not in instrument_str:
                    logging.warning(f"[Storage] 跳过无效合约格式: {instrument_str}")
                    failed_count += 1
                    continue
                
                exchange, instrument_id = instrument_str.split('.', 1)
                
                logging.info(f"[Storage] 加载 {exchange}.{instrument_id} 历史K线")
                
                # 获取历史K线数据
                kline_data = market_center.get_kline_data(
                    exchange=exchange,
                    instrument_id=instrument_id,
                    style=kline_style,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if kline_data and len(kline_data) > 0:
                    # 保存历史K线到storage
                    saved_count = 0
                    for kline in kline_data:
                        try:
                            kline_dict = {
                                'timestamp': getattr(kline, 'timestamp', getattr(kline, 'ts', time_module.time())),  # ✅ 统一使用 timestamp
                                'instrument_id': instrument_id,
                                'exchange': exchange,
                                'open': getattr(kline, 'open', getattr(kline, 'Open', 0.0)),
                                'high': getattr(kline, 'high', getattr(kline, 'High', 0.0)),
                                'low': getattr(kline, 'low', getattr(kline, 'Low', 0.0)),
                                'close': getattr(kline, 'close', getattr(kline, 'Close', 0.0)),
                                'volume': getattr(kline, 'volume', getattr(kline, 'Volume', 0)),
                                'open_interest': getattr(kline, 'open_interest', getattr(kline, 'OpenInterest', 0)),
                                'period': kline_style.lower().replace('m', 'min')  # M1 -> 1min
                            }
                            self.save_external_kline(kline_dict)
                            saved_count += 1
                        except Exception as e:
                            logging.warning(f"[Storage] 保存K线失败 {instrument_id}: {e}")
                    
                    success_count += 1
                    total_klines += saved_count
                    logging.info(f"[Storage] ✅ {instrument_id}: 加载并保存 {saved_count} 条K线")
                else:
                    logging.warning(f"[Storage] ⚠️ {instrument_id}: 无历史K线数据")
                    failed_count += 1
                    
            except Exception as e:
                logging.error(f"[Storage] 加载历史K线失败 {instrument_str}: {e}")
                failed_count += 1
        
        # 汇总结果
        logging.info(
            f"[Storage] 历史K线加载完成: "
            f"成功={success_count}, 失败={failed_count}, "
            f"总计K线={total_klines} 条"
        )
        
        return {
            'success': success_count,
            'failed': failed_count,
            'total_klines': total_klines
        }
    
    # ========== 通用查询辅助方法 ==========
    def _get_column_names(self, table_name: str) -> List[str]:
        """获取表名列（带缓存）。"""
        with self._column_cache_lock:
            if table_name in self._column_cache:
                return self._column_cache[table_name]
        
        # 查询一次获取列名
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        col_names = [desc[0] for desc in cursor.description]
        
        # 缓存
        with self._column_cache_lock:
            self._column_cache[table_name] = col_names
        
        return col_names
    
    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """根据期货合约代码获取期权链（基于 instrument_id 而非 internal_id）。"""
        info = self._get_instrument_info(future_instrument_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货合约：{future_instrument_id}")
        
        future_id = info['id']
        return self.get_option_chain_by_future_id(future_id)
    
    # ========== 批量操作 ==========
    def batch_add_future_instruments(self, instruments: List[Dict]) -> None:
        """批量导入期货合约（带事务）"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN")
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
            self.conn.commit()
            logging.info("批量导入 %d 个期货合约", len(instruments))
        except Exception as e:
            self.conn.rollback()
            logging.error("批量导入期货失败：%s", e)
            raise
    
    def batch_add_option_instruments(self, instruments: List[Dict]) -> None:
        """批量导入期权合约（带事务）"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN")
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
            self.conn.commit()
            logging.info("批量导入 %d 个期权合约", len(instruments))
        except Exception as e:
            self.conn.rollback()
            logging.error("批量导入期权失败：%s", e)
            raise
    
    def batch_write_kline(self, instrument_id: str, kline_data: List[Dict], period: str = '1min') -> None:
        """批量写入 K 线数据（真正使用异步队列）"""
        if not kline_data:
            return
        internal_id = self.register_instrument(instrument_id)
        
        # 分批入队，避免单次数据过大
        chunk_size = self.batch_size
        for i in range(0, len(kline_data), chunk_size):
            chunk = kline_data[i:i+chunk_size]
            # ✅ 使用异步队列
            self._enqueue_write('_save_kline_impl', internal_id, chunk, period)
    
    def batch_write_tick(self, instrument_id: str, tick_data: List[Dict]) -> None:
        """批量写入 Tick 数据（真正使用异步队列）"""
        if not tick_data:
            return
        internal_id = self.register_instrument(instrument_id)
        
        # 分批入队
        chunk_size = self.batch_size
        for i in range(0, len(tick_data), chunk_size):
            chunk = tick_data[i:i+chunk_size]
            # ✅ 使用异步队列
            self._enqueue_write('_save_tick_impl', internal_id, chunk)
    
    # ========== 高级查询 ==========
    def get_latest_kline(self, instrument_id: str, limit: int = 100) -> List[Dict]:
        """获取最新 N 条 K 线"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return []
        
        table_name = info['kline_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {table_name}
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,))
        
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        result = [dict(zip(col_names, row)) for row in rows]
        # 按时间正序返回
        return list(reversed(result))
    
    def _preload_column_cache(self):
        """预加载常用表的列名到缓存，加速后续写入。"""
        common_tables = [
            'futures_instruments', 'option_instruments',
            'future_products', 'option_products',
            'kv_store'
        ]
        
        cursor = self.conn.cursor()
        for table in common_tables:
            try:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in cursor.fetchall()]
                with self._column_cache_lock:
                    self._column_cache[table] = columns
            except Exception as e:
                logging.debug("预加载表 %s 列名失败：%s", table, e)
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """获取表的列名（带缓存）。"""
        # 先查缓存
        with self._column_cache_lock:
            if table_name in self._column_cache:
                return self._column_cache[table_name]
        
        # 缓存未命中，查询数据库
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            
            # 写入缓存
            with self._column_cache_lock:
                self._column_cache[table_name] = columns
            
            return columns
        except Exception as e:
            logging.error("获取表 %s 列名失败：%s", table_name, e)
            return []
    
    def get_latest_tick(self, instrument_id: str, limit: int = 100) -> List[Dict]:
        """获取最新 N 条 Tick"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return []
        
        table_name = info['tick_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {table_name}
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,))
        
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        result = [dict(zip(col_names, row)) for row in rows]
        return list(reversed(result))
    
    def get_kline_count(self, instrument_id: str) -> int:
        """获取 K 线总条数"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return 0
        
        table_name = info['kline_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    
    def get_tick_count(self, instrument_id: str) -> int:
        """获取 Tick 总条数"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return 0
        
        table_name = info['tick_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    
    def get_kline_range(self, instrument_id: str) -> Tuple[Optional[str], Optional[str]]:
        """获取 K 线数据的时间范围 (min, max)"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return (None, None)
        
        table_name = info['kline_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}")
        row = cursor.fetchone()
        return (row[0], row[1]) if row else (None, None)
    
    def get_tick_range(self, instrument_id: str) -> Tuple[Optional[str], Optional[str]]:
        """获取 Tick 数据的时间范围 (min, max)"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return (None, None)
        
        table_name = info['tick_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}")
        row = cursor.fetchone()
        return (row[0], row[1]) if row else (None, None)
    
    # ========== 统计与分析 ==========
    def get_instrument_summary(self, instrument_id: str) -> Dict[str, Any]:
        """获取合约数据摘要统计"""
        info = self._get_instrument_info(instrument_id)
        if not info:
            return {}
        
        kline_count = self.get_kline_count(instrument_id)
        tick_count = self.get_tick_count(instrument_id)
        kline_range = self.get_kline_range(instrument_id)
        tick_range = self.get_tick_range(instrument_id)
        
        return {
            'instrument_id': instrument_id,
            'internal_id': info['id'],
            'type': info['type'],
            'product': info.get('product'),
            'year_month': info.get('year_month'),
            'kline_count': kline_count,
            'tick_count': tick_count,
            'kline_first': kline_range[0],
            'kline_last': kline_range[1],
            'tick_first': tick_range[0],
            'tick_last': tick_range[1]
        }
    
    def get_all_instruments_summary(self) -> List[Dict[str, Any]]:
        """获取所有合约的摘要统计"""
        summaries = []
        for instrument_id in list(self._instrument_cache.keys()):
            summary = self.get_instrument_summary(instrument_id)
            if summary:
                summaries.append(summary)
        return summaries
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """获取数据库存储统计"""
        cursor = self.conn.cursor()
        
        # 获取所有表的大小
        cursor.execute("""
            SELECT name, 
                   pgsize_as_used - pgsize_unused as size_bytes
            FROM dbstat 
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY size_bytes DESC
        """)
        
        tables = []
        total_size = 0
        for row in cursor.fetchall():
            tables.append({
                'name': row[0],
                'size_bytes': row[1] or 0
            })
            total_size += (row[1] or 0)
        
        return {
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'tables': tables,
            'num_futures': len([k for k, v in self._instrument_cache.items() if v.get('type') == 'future']),
            'num_options': len([k for k, v in self._instrument_cache.items() if v.get('type') == 'option'])
        }
    
    # ========== 数据导出 ==========
    def export_kline_to_csv(self, instrument_id: str, output_file: str,
                           start_time: Optional[str] = None,
                           end_time: Optional[str] = None) -> int:
        """导出 K 线数据到 CSV 文件"""
        import csv
        
        info = self._get_instrument_info(instrument_id)
        if not info:
            logging.error("导出失败：合约不存在 %s", instrument_id)
            return 0
        
        table_name = info['kline_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        
        # 构建查询
        where_clause = ""
        params = []
        if start_time:
            where_clause += " WHERE timestamp >= ?"
            params.append(start_time)
        if end_time:
            if where_clause:
                where_clause += " AND timestamp <= ?"
            else:
                where_clause = " WHERE timestamp <= ?"
            params.append(end_time)
        
        sql = f"SELECT * FROM {table_name}{where_clause} ORDER BY timestamp"
        cursor.execute(sql, params)
        
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        
        # 写入 CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=col_names)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(zip(col_names, row)))
        
        logging.info("导出 K 线 %d 条到 %s", len(rows), output_file)
        return len(rows)
    
    def export_tick_to_csv(self, instrument_id: str, output_file: str,
                          start_time: Optional[str] = None,
                          end_time: Optional[str] = None) -> int:
        """导出 Tick 数据到 CSV 文件"""
        import csv
        
        info = self._get_instrument_info(instrument_id)
        if not info:
            logging.error("导出失败：合约不存在 %s", instrument_id)
            return 0
        
        table_name = info['tick_table']
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        
        # 构建查询
        where_clause = ""
        params = []
        if start_time:
            where_clause += " WHERE timestamp >= ?"
            params.append(start_time)
        if end_time:
            if where_clause:
                where_clause += " AND timestamp <= ?"
            else:
                where_clause = " WHERE timestamp <= ?"
            params.append(end_time)
        
        sql = f"SELECT * FROM {table_name}{where_clause} ORDER BY timestamp"
        cursor.execute(sql, params)
        
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        
        # 写入 CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=col_names)
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(zip(col_names, row)))
        
        logging.info("导出 Tick %d 条到 %s", len(rows), output_file)
        return len(rows)
    
    # ========== 底层写入方法（storage.py 兼容） ==========
    def write_kline_to_table(self, table_name: str, kline_data: List[Dict]) -> None:
        """
        直接写入 K 线数据到指定表（不经过 instrument_id 映射）
        用于 storage.py 兼容模式
        """
        if not kline_data:
            return
        
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        values = [(k.get('ts') or k.get('timestamp'), 
                   k.get('open', 0.0), 
                   k.get('high', 0.0), 
                   k.get('low', 0.0),
                   k.get('close', 0.0), 
                   k.get('volume', 0), 
                   k.get('open_interest', 0)) for k in kline_data]
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name}
            (timestamp, open, high, low, close, volume, open_interest)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)
        self.conn.commit()
        logging.debug(f"写入 {len(kline_data)} 条 K 线到 {table_name}")
    
    def write_tick_to_table(self, table_name: str, tick_data: List[Dict]) -> None:
        """
        直接写入 Tick 数据到指定表（不经过 instrument_id 映射）
        用于 storage.py 兼容模式
        """
        if not tick_data:
            return
        
        self._validate_table_name(table_name)
        
        cursor = self.conn.cursor()
        # 简化版，假设是期货 Tick - ✅ 全部改为 .get() 安全访问
        values = [(t.get('ts') or t.get('timestamp'), 
                   t.get('last_price', 0.0), 
                   t.get('volume', 0), 
                   t.get('open_interest', 0),
                   t.get('bid_price1'), 
                   t.get('ask_price1')) for t in tick_data]
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name}
            (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
            VALUES (?, ?, ?, ?, ?, ?)
        """, values)
        self.conn.commit()
        logging.debug(f"写入 {len(tick_data)} 条 Tick 到 {table_name}")
    
    # ========== Query Methods (storage.py compatible) ==========
    def query_kline(self, instrument_id: str, start_time: str, end_time: str,
                    limit: Optional[int] = None) -> List[Dict]:
        """
        Query K-line data by instrument_id (alias for query_kline_by_id)
        Compatible with storage.py interface
        """
        internal_id = self.register_instrument(instrument_id)
        return self.query_kline_by_id(internal_id, start_time, end_time, limit)
    
    def query_tick(self, instrument_id: str, start_time: str, end_time: str,
                   limit: Optional[int] = None) -> List[Dict]:
        """
        Query tick data by instrument_id (alias for query_tick_by_id)
        Compatible with storage.py interface
        """
        internal_id = self.register_instrument(instrument_id)
        return self.query_tick_by_id(internal_id, start_time, end_time, limit)
    
    def get_option_chain(self, ts: float, underlying: str, expiration: str) -> List[Dict[str, Any]]:
        """
        Get option chain at specific timestamp (simplified implementation)
        Compatible with storage.py interface
        """
        # Simplified: return all options for the underlying future
        try:
            future_instrument_id = f"{underlying}{expiration}"
            chain = self.get_option_chain_for_future(future_instrument_id)
            return chain.get('options', [])
        except Exception as e:
            logging.error("get_option_chain failed: %s", e)
            return []
    
    def get_latest_underlying(self, underlying: str, expiration: str) -> Optional[Dict[str, Any]]:
        """
        Get latest underlying snapshot (simplified implementation)
        Compatible with storage.py interface
        """
        # This would require actual underlying_snapshot table implementation
        # For now, return None or can be extended
        logging.debug("get_latest_underlying called: %s %s", underlying, expiration)
        return None
    
    # ========== 自动清理线程 ==========
    def _start_cleanup_thread(self):
        """启动自动清理线程。"""
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return
        self._cleanup_thread = threading.Thread(target=self._auto_cleanup_loop, daemon=True)
        self._cleanup_thread.start()
        logging.info("[AutoCleanup] 自动清理线程已启动，间隔：%d秒", self._cleanup_interval)
    
    def _auto_cleanup_loop(self):
        """自动清理循环。"""
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
        """保存原始 Tick 数据（异步，批量版本）。"""
        if not self._validate_tick(tick):
            return
        tick = tick.copy()
        ts = self._to_timestamp(tick.get('ts'))
        if ts is None:
            logging.error("save_tick 时间戳转换失败：%s", tick.get('ts'))
            return
        tick['ts'] = ts
        
        # 获取合约 ID 并包装成列表，使用批量版本
        try:
            internal_id = self.register_instrument(tick['instrument_id'])
            # 包装成列表，调用批量版本
            self._enqueue_write('_save_tick_impl', internal_id, [tick])
        except ValueError as e:
            # 合约无法识别时静默失败
            logging.debug("save_tick register failed: %s", e)
    
    def _save_depth_batch_impl(self, cursor, depth_list: List[Dict[str, Any]]):
        """批量保存深度行情（同步实现）。"""
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
            
            table_name = info['tick_table']
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
            
            cursor.execute(f"""
                INSERT INTO {table_name}
                ({field_str})
                VALUES ({placeholder_str})
                ON CONFLICT(timestamp) DO UPDATE SET
                {update_fields}
            """, tuple(values))
    
    def save_depth_batch(self, depth_list: List[Dict[str, Any]]) -> None:
        """批量保存深度行情（异步）。"""
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
    
    def _save_signal_impl(self, cursor, signal: Dict[str, Any]):
        """保存策略信号（同步实现）。"""
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
        """保存策略信号（异步）。"""
        if not self._validate_signal(signal):
            return
        signal = signal.copy()
        ts = self._to_timestamp(signal.get('ts'))
        if ts is None:
            logging.error("save_signal 时间戳转换失败：%s", signal.get('ts'))
            return
        signal['ts'] = ts
        self._enqueue_write('_save_signal_impl', signal)
    
    def _save_underlying_snapshot_impl(self, cursor, data: Dict[str, Any]):
        """保存标的物快照（同步实现）。"""
        # 简化实现，实际应保存到专门的 underlying_snapshot 表
        # 这里仅做日志记录
        logging.debug("保存标的物快照：%s %s", data.get('underlying'), data.get('expiration'))
    
    def save_underlying_snapshot(self, data: Dict[str, Any]) -> None:
        """保存标的物快照（异步）。"""
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
        """标的物数据校验。"""
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
    
    def _save_option_snapshot_batch_impl(self, cursor, data_list: List[Dict[str, Any]]):
        """批量保存期权快照（同步实现）。"""
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
            
            table_name = info['tick_table']
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
            
            cursor.execute(f"""
                INSERT INTO {table_name}
                ({field_str})
                VALUES ({placeholder_str})
                ON CONFLICT(timestamp) DO UPDATE SET
                {update_fields}
            """, tuple(values))
    
    def save_option_snapshot_batch(self, data_list: List[Dict[str, Any]]) -> None:
        """批量保存期权快照（异步）。"""
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
        """
        加载所有期货和期权合约信息
        
        Args:
            strategy_instance: Strategy2026 实例（可选）
            params: 策略参数对象（可选）
            logger: 日志记录器（可选）
            
        Returns:
            Tuple[List[Dict], Dict[str, List[Dict]]]: (futures_list, options_dict)
            - futures_list: 期货合约列表
            - options_dict: 期权合约字典 {underlying_symbol: [option_contracts]}
        """
        import logging as stdlib_logging
        
        # 使用传入的 logger 或创建临时 logger
        if logger is None:
            logger = stdlib_logging.getLogger(__name__)
        
        futures_list = []
        options_dict = {}
        
        try:
            # 尝试从策略实例获取合约信息
            if strategy_instance is not None:
                try:
                    # 获取所有订阅的合约
                    instruments = getattr(strategy_instance, 'subscribed_instruments', {})
                    for inst_id, inst_info in instruments.items():
                        try:
                            product = _inst_get(inst_info, 'product', 'Product', 'underlying_product')
                            instrument_id = _inst_get(inst_info, 'instrument_id', 'InstrumentID', '_instrument_id')
                            
                            if not instrument_id:
                                continue
                            
                            # 使用精确正则解析判断是期货还是期权
                            is_option = False
                            try:
                                parsed = self._parse_option_with_dash(instrument_id)
                                is_option = True
                            except ValueError:
                                # 不是标准期权格式，可能是期货
                                pass
                            
                            # 判断是期货还是期权
                            if is_option:
                                # 期权
                                underlying = _inst_get(inst_info, 'underlying_future', 'UnderlyingInstr', 'underlying')
                                if underlying:
                                    if underlying not in options_dict:
                                        options_dict[underlying] = []
                                    options_dict[underlying].append({
                                        'instrument_id': instrument_id,
                                        'product': product,
                                        'underlying': underlying,
                                        'exchange': _inst_get(inst_info, 'exchange', 'ExchangeID'),
                                        'year_month': _inst_get(inst_info, 'year_month', 'DeliveryMonth'),
                                        'strike_price': _inst_get(inst_info, 'strike_price', 'StrikePrice'),
                                        'option_type': _inst_get(inst_info, 'option_type', 'OptionsType')
                                    })
                            else:
                                # 期货
                                futures_list.append({
                                    'instrument_id': instrument_id,
                                    'product': product,
                                    'exchange': _inst_get(inst_info, 'exchange', 'ExchangeID'),
                                    'year_month': _inst_get(inst_info, 'year_month', 'DeliveryMonth')
                                })
                        except Exception as e:
                            logger.debug("跳过合约处理失败：%s", e)
                except Exception as e:
                    logger.warning("从策略实例获取合约失败：%s", e)
            
            # 如果没有从策略实例获取到数据，尝试从 params 获取
            if not futures_list and not options_dict and params is not None:
                try:
                    # 获取配置的合约列表
                    configured_instruments = getattr(params, 'instrument_ids', [])
                    for inst_id in configured_instruments:
                        try:
                            if isinstance(inst_id, str):
                                # 使用精确正则解析判断是期货还是期权
                                is_option = False
                                try:
                                    parsed = self._parse_option_with_dash(inst_id)
                                    is_option = True
                                except ValueError:
                                    # 不是标准期权格式，可能是期货
                                    pass
                                
                                if is_option:
                                    # 期权（简化解析）
                                    parts = inst_id.split('-')
                                    underlying = parts[0] if parts else inst_id
                                    if underlying not in options_dict:
                                        options_dict[underlying] = []
                                    options_dict[underlying].append({
                                        'instrument_id': inst_id,
                                        'underlying': underlying
                                    })
                                else:
                                    # 期货
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
        """
        删除指定表中早于 days 天的数据。
        Args:
            table: 表名，如 'tick', 'kline'（白名单验证）。
            days: 保留天数。
            condition: 额外的 WHERE 条件（参数化查询）。
        Returns:
            删除的行数。
        """
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

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        if cursor.fetchone() is None:
            logging.warning("清理跳过：表 %s 不存在", table)
            return 0

        try:
            cursor.execute("BEGIN")
            cursor.execute(sql, tuple(params))
            deleted = cursor.rowcount
            self.conn.commit()
            logging.info("从表 %s 删除了 %d 条过期数据（截止 %.3f）", table, deleted, cutoff)
            return deleted
        except sqlite3.Error as e:
            logging.error("清理数据失败：%s", e)
            self.conn.rollback()
            return 0

    # ========== 通用状态持久化（KV 存储） ==========
    def _init_kv_store(self):
        """初始化键值存储表"""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app_kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_kv_updated_at ON app_kv_store(updated_at DESC)")
        self.conn.commit()
        logging.debug("KV 存储表初始化完成")

    @staticmethod
    def _json_default(value: Any) -> Any:
        """将不可直接 JSON 序列化的对象转换为可序列化格式。"""
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _save_kv_impl(self, cursor, key: str, payload: str):
        """异步保存键值对数据的实现。"""
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO app_kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                (key, payload, datetime.now().isoformat()),
            )
        except Exception as e:
            logging.error(f"_save_kv_impl 保存失败 key={key}: {e}")
            raise  # 重新抛出异常，触发重试机制
    
    def save(self, key: str, data: Any, async_mode: bool = True) -> bool:
        """
        保存任意结构化数据到键值存储（默认异步，可选同步）。
        
        :param key: 键名（字符串）
        :param data: 任意可 JSON 序列化的数据
        :param async_mode: 是否使用异步写入，默认 True（高性能）
        :return: 是否保存成功
        """
        if not key or not isinstance(key, str):
            logging.error("save 失败：key 无效：%s", key)
            return False

        try:
            payload = json.dumps(data, ensure_ascii=False, default=self._json_default)
            
            if async_mode:
                # ✅ 使用异步队列（默认，高性能）
                return self._enqueue_write('_save_kv_impl', key, payload)
            else:
                # ❌ 同步写入（兼容旧代码，不推荐）
                cursor = self.conn.cursor()
                cursor.execute("BEGIN")
                self._save_kv_impl(cursor, key, payload)
                self.conn.commit()
                return True
        except Exception as e:
            logging.error(f"save 异常：{e}")
            if not async_mode:
                # 同步模式下尝试回滚
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            return False

    def load(self, key: str) -> Optional[Any]:
        """
        按 key 同步加载数据，不存在则返回 None。
        :param key: 键名
        :return: 解析后的数据或 None
        """
        if not key or not isinstance(key, str):
            logging.error("load 失败：key 无效：%s", key)
            return None

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT value FROM app_kv_store WHERE key = ?", (key,))
            row = cursor.fetchone()
            if not row:
                return None
            return json.loads(row[0])
        except Exception as e:
            logging.error("load 失败 key=%s: %s", key, e, exc_info=True)
            return None

    # ========== 事务管理 ==========
    @contextmanager
    def transaction(self):
        """上下文管理器形式的事务（自动重试）"""
        def _execute():
            self.conn.execute("BEGIN")
        self._execute_with_retry(_execute)
        try:
            yield
            self._execute_with_retry(self.conn.commit)
        except Exception as e:
            self._execute_with_retry(self.conn.rollback)
            raise

    # ========== 资源管理 ==========
    def close(self):
        """关闭资源（连接池、异步线程、清理线程）。"""
        self._shutdown_impl(flush=True)
    
    def shutdown(self, flush: bool = True):
        """
        关闭服务（storage.py 兼容接口）
        :param flush: 是否刷新队列中的数据
        """
        self._shutdown_impl(flush=flush)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ========== T 型图订阅管理器 ==========
# ========== 统一订阅管理器 ==========
class SubscriptionManager:
    """
    统一订阅管理器，负责所有合约（期货 + 期权）的订阅管理。
    支持：握手协议、窗口化节流、分批订阅、自动重连。
    
    设计目标：
    1. 防止平台限流（分批 + 节流）
    2. 统一订阅接口（替代 strategy_core_service 的直接调用）
    3. 支持期权 T 型图批量订阅
    4. 握手协议认证
    """
    
    def __init__(self, data_manager: InstrumentDataManager, max_retries: int = 3):
        self.data_manager = data_manager
        self.max_retries = max_retries
        self._subscriptions: Dict[str, Dict] = {}  # {underlying: subscription_info}
        self._lock = threading.Lock()
        self._handshake_timeout = 10.0  # 秒
        self._throttle_window = 1.0     # 秒
        self._batch_size = 10           # 每批最多订阅的合约数
        self._batch_delay = 0.5         # 批次间延迟（秒）
        
        # 防无限重试保护
        self._retry_cooldowns: Dict[str, float] = {}  # {instrument_id: cooldown_until_timestamp}
        self._max_cooldown = 300.0  # 最大冷却时间 5 分钟
        self._permanent_failures: set = set()  # 永久失败的合约 ID
    
    def subscribe_all_instruments(self, futures_list: List[str], 
                                   options_dict: Dict[str, List[str]]) -> bool:
        """
        全量订阅所有合约（期货 + 期权），采用分批 + 节流策略。
        
        Args:
            futures_list: 期货合约列表
            options_dict: 期权合约字典 {underlying: [option_ids]}
            
        Returns:
            bool: 是否全部订阅成功
        """
        total_count = len(futures_list) + sum(len(opts) for opts in options_dict.values())
        logging.info("[SubscriptionManager] 开始全量订阅：%d 个合约", total_count)
        
        success_count = 0
        failed_count = 0
        
        # 第 1 步：先订阅期货（快速）
        if futures_list:
            logging.info("[SubscriptionManager] 第 1 批：订阅 %d 个期货合约", len(futures_list))
            for i, future_id in enumerate(futures_list):
                if self._subscribe_single(future_id, 'tick'):
                    success_count += 1
                else:
                    failed_count += 1
                
                # 节流控制
                if (i + 1) % self._batch_size == 0:
                    time.sleep(self._batch_delay)
        
        # 第 2 步：按 T 型图分批订阅期权（带握手协议）
        if options_dict:
            logging.info("[SubscriptionManager] 第 2 批：订阅 %d 个标的的期权", len(options_dict))
            for underlying, option_ids in options_dict.items():
                # 提取到期年月
                if option_ids:
                    # 从期权 ID 中提取 expiration (如 IO2406-C-4000 -> 2406)
                    try:
                        expiration = option_ids[0].split('-')[0][-4:]  # 取最后 4 位年月
                        
                        # 执行 T 型图握手协议
                        if not self._handshake(underlying, expiration):
                            logging.error("[SubscriptionManager] 握手失败：%s_%s", underlying, expiration)
                            failed_count += len(option_ids)
                            continue
                        
                        # 批量订阅该标的的所有期权
                        batch_success, batch_failed = self._subscribe_option_chain(
                            underlying, expiration, option_ids
                        )
                        success_count += batch_success
                        failed_count += batch_failed
                        
                        # 批次间延迟，防止限流
                        time.sleep(self._batch_delay)
                        
                    except Exception as e:
                        logging.warning("[SubscriptionManager] 期权订阅失败 %s: %s", underlying, e)
                        failed_count += len(option_ids)
        
        logging.info(
            "[SubscriptionManager] 全量订阅完成：成功=%d, 失败=%d, 总计=%d",
            success_count, failed_count, total_count
        )
        return failed_count == 0
    
    def _subscribe_single(self, instrument_id: str, data_type: str = 'tick') -> bool:
        """
        订阅单个合约（带重试 + 防无限重试保护）。
        
        Args:
            instrument_id: 合约 ID
            data_type: 数据类型 ('tick', 'kline', etc.)
            
        Returns:
            bool: 是否订阅成功
        """
        # 检查是否为永久失败合约
        if instrument_id in self._permanent_failures:
            logging.debug("[SubscriptionManager] 跳过永久失败合约：%s", instrument_id)
            return False
        
        # 检查是否在冷却期
        now = time.time()
        if instrument_id in self._retry_cooldowns:
            cooldown_until = self._retry_cooldowns[instrument_id]
            if now < cooldown_until:
                remaining = cooldown_until - now
                logging.debug(
                    "[SubscriptionManager] 冷却期中，跳过 %s (剩余 %.1f 秒)",
                    instrument_id, remaining
                )
                return False
            else:
                # 冷却期结束，移除记录
                del self._retry_cooldowns[instrument_id]
        
        # 执行重试逻辑
        consecutive_failures = 0
        for attempt in range(self.max_retries):
            try:
                internal_id = self.data_manager.subscribe(instrument_id, data_type)
                
                # 检查返回值，None 表示失败
                if internal_id is None:
                    raise ValueError(f"subscribe returned None for {instrument_id}")
                
                logging.debug("[SubscriptionManager] 订阅成功：%s -> ID=%d", instrument_id, internal_id)
                
                # 成功后清除冷却记录
                if instrument_id in self._retry_cooldowns:
                    del self._retry_cooldowns[instrument_id]
                
                return True
                
            except Exception as e:
                consecutive_failures += 1
                logging.warning(
                    "[SubscriptionManager] 订阅失败 %s (尝试 %d/%d): %s",
                    instrument_id, attempt + 1, self.max_retries, e
                )
                
                # 如果达到最大重试次数
                if attempt >= self.max_retries - 1:
                    # 计算指数退避时间：base_delay * 2^(consecutive_failures-1), 上限为_max_cooldown
                    base_delay = 1.0
                    exponential_delay = min(base_delay * (2 ** (consecutive_failures - 1)), self._max_cooldown)
                    
                    # 设置冷却期
                    self._retry_cooldowns[instrument_id] = now + exponential_delay
                    
                    logging.warning(
                        "[SubscriptionManager] 合约 %s 连续失败 %d 次，进入冷却期 %.1f 秒",
                        instrument_id, consecutive_failures, exponential_delay
                    )
                    
                    # 如果连续失败超过阈值，标记为永久失败
                    if consecutive_failures >= 5:
                        self._permanent_failures.add(instrument_id)
                        logging.error(
                            "[SubscriptionManager] 合约 %s 连续失败 %d 次，标记为永久失败",
                            instrument_id, consecutive_failures
                        )
                    
                    return False
                
                # 重试前递增延迟
                time.sleep(0.1 * (attempt + 1))
        
        return False
    
    def _subscribe_option_chain(self, underlying: str, expiration: str,
                                 option_ids: List[str]) -> Tuple[int, int]:
        """
        批量订阅某一期权链的所有合约。
        
        Args:
            underlying: 标的代码
            expiration: 到期年月
            option_ids: 期权 ID 列表
            
        Returns:
            Tuple[int, int]: (成功数，失败数)
        """
        success_count = 0
        failed_count = 0
        
        for i, option_id in enumerate(option_ids):
            if self._subscribe_single(option_id, 'tick'):
                success_count += 1
            else:
                failed_count += 1
            
            # 期权内部也要节流
            if (i + 1) % self._batch_size == 0:
                time.sleep(self._batch_delay)
        
        # 记录订阅信息
        key = f"{underlying}_{expiration}"
        self._subscriptions[key] = {
            'underlying': underlying,
            'expiration': expiration,
            'option_ids': option_ids,
            'success_count': success_count,
            'failed_count': failed_count,
            'created_at': time.time()
        }
        
        logging.debug(
            "[SubscriptionManager] 期权链订阅完成：%s_%s (成功=%d, 失败=%d)",
            underlying, expiration, success_count, failed_count
        )
        return success_count, failed_count
    
    def _handshake(self, underlying: str, expiration: str) -> bool:
        """
        握手协议（简化实现）。
        实际应调用交易所 API 进行认证和限流控制。
        
        Args:
            underlying: 标的代码
            expiration: 到期年月
            
        Returns:
            bool: 是否握手成功
        """
        logging.debug("[SubscriptionManager] T 型图握手：%s %s", underlying, expiration)
        # 简化实现：直接返回成功
        # TODO: 实际应调用 platform.handshake() 或类似接口
        return True
    
    def unsubscribe_all(self) -> bool:
        """
        取消所有订阅。
        
        Returns:
            bool: 是否全部取消成功
        """
        with self._lock:
            success_count = 0
            failed_count = 0
            
            for key, sub_info in list(self._subscriptions.items()):
                option_ids = sub_info.get('option_ids', [])
                for option_id in option_ids:
                    try:
                        self.data_manager.unsubscribe(option_id, 'tick')
                        success_count += 1
                    except Exception as e:
                        failed_count += 1
                        logging.warning("[SubscriptionManager] 取消订阅失败 %s: %s", option_id, e)
                
                del self._subscriptions[key]
            
            logging.info(
                "[SubscriptionManager] 取消订阅完成：成功=%d, 失败=%d",
                success_count, failed_count
            )
            return failed_count == 0
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息。"""
        with self._lock:
            total_options = sum(
                len(sub_info.get('option_ids', []))
                for sub_info in self._subscriptions.values()
            )
            return {
                'total_subscriptions': len(self._subscriptions),
                'total_option_contracts': total_options,
                'underlyings': list(self._subscriptions.keys())
            }


# ============================================================================
# 连接池管理（内部使用）
# ============================================================================
class _ConnectionPool(object):
    """简单连接池，支持 LRU 淘汰空闲连接，线程安全。"""
    def __init__(self, db_path: str, max_connections: int = 20):
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections: List[sqlite3.Connection] = []          # 所有连接
        self._in_use: Dict[sqlite3.Connection, bool] = {}        # True 表示被占用
        self._last_used: Dict[sqlite3.Connection, float] = {}    # 最后使用时间戳
        self._owner_thread_ids: Dict[sqlite3.Connection, int] = {}
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._finalizers: Dict[sqlite3.Connection, Any] = {}      # 用于移除已销毁的连接

    def _create_connection(self) -> sqlite3.Connection:
        """创建新连接并设置 WAL 模式。"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _register_finalizer(self, conn: sqlite3.Connection):
        """注册 finalizer，当连接被垃圾回收时从池中移除。"""
        def _finalize():
            with self._lock:
                if conn in self._connections:
                    self._connections.remove(conn)
                    self._in_use.pop(conn, None)
                    self._last_used.pop(conn, None)
                    self._owner_thread_ids.pop(conn, None)
                    # 通知等待者，可能有线程在等待
                    self._cond.notify_all()

        try:
            self._finalizers[conn] = weakref.finalize(conn, _finalize)
        except TypeError:
            # 不支持弱引用时，直接注册清理函数
            self._finalizers[id(conn)] = _finalize

    def _reclaim_dead_thread_connections_unlocked(self) -> None:
        """回收被已退出线程持有但未归还的连接。"""
        active_thread_ids = {thread.ident for thread in threading.enumerate() if thread.ident is not None}
        reclaimed = 0

        for conn in list(self._connections):
            if not self._in_use.get(conn, False):
                continue

            owner_thread_id = self._owner_thread_ids.get(conn)
            if owner_thread_id is None:
                continue

            if owner_thread_id in active_thread_ids:
                continue

            self._in_use[conn] = False
            self._last_used[conn] = time.time()
            self._owner_thread_ids.pop(conn, None)
            reclaimed += 1

        if reclaimed:
            logging.warning(f"[_ConnectionPool] Reclaimed {reclaimed} leaked connection(s) from dead threads")
            self._cond.notify_all()

    def get_connection(self, timeout_sec: float = 30.0) -> sqlite3.Connection:
        """
        获取一个连接。如果池中有空闲连接，返回最久未使用的；
        否则如果总连接数未达上限，创建新连接；
        否则阻塞直到有空闲连接（带超时机制）。
        """
        with self._lock:
            timeout = max(0.01, float(timeout_sec or 30.0))
            deadline = time.time() + timeout
            current_thread_id = threading.get_ident()
            
            while True:
                self._reclaim_dead_thread_connections_unlocked()

                # 查找空闲连接
                idle_conns = [c for c in self._connections if not self._in_use.get(c, False)]
                if idle_conns:
                    # 选择最久未使用的
                    idle_conns.sort(key=lambda c: self._last_used.get(c, 0))
                    conn = idle_conns[0]
                    self._in_use[conn] = True
                    self._last_used[conn] = time.time()
                    self._owner_thread_ids[conn] = current_thread_id
                    return conn

                # 无空闲连接，如果未达上限，创建新连接
                if len(self._connections) < self.max_connections:
                    conn = self._create_connection()
                    self._connections.append(conn)
                    self._in_use[conn] = True
                    self._last_used[conn] = time.time()
                    self._owner_thread_ids[conn] = current_thread_id
                    self._register_finalizer(conn)
                    return conn

                # 池满且无空闲，等待（带超时）
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(f"等待数据库连接超时 ({timeout}秒)")
                self._cond.wait(timeout=min(remaining, 1.0))

    def release_connection(self, conn: sqlite3.Connection):
        """释放连接，供其他线程使用。"""
        with self._lock:
            if conn in self._connections:
                self._in_use[conn] = False
                self._last_used[conn] = time.time()
                self._owner_thread_ids.pop(conn, None)
                self._cond.notify_all()

    def close_all(self):
        """关闭所有连接并清空池。"""
        with self._lock:
            for conn in self._connections[:]:
                try:
                    conn.close()
                except Exception as e:
                    logging.warning(f"[_ConnectionPool] Failed to close connection: {e}")
            self._connections.clear()
            self._in_use.clear()
            self._last_used.clear()
            self._owner_thread_ids.clear()
            self._finalizers.clear()
            self._cond.notify_all()


# ========== 辅助函数 ==========
def _inst_get(inst: Any, *keys: str, default: Any = '') -> Any:
    """兼容 dict / object 两种返回结构读取字段。"""
    if isinstance(inst, dict):
        for k in keys:
            if k in inst and inst.get(k) not in (None, ''):
                return inst.get(k)
        return default
    
    for k in keys:
        try:
            val = getattr(inst, k, None)
        except Exception:
            val = None
        if val not in (None, ''):
            return val
    return default


# ========== K 线聚合器（内部类） ==========
class _KlineAggregator(object):
    """单个合约单个周期的 K 线聚合器，线程安全。使用时间戳（浮点数）作为时间。"""
    
    def __init__(self, instrument_id: str, period: str, logger: logging.Logger):
        self.instrument_id = instrument_id
        self.period = period
        self.logger = logger
        
        # 解析周期分钟数
        self.minutes = self._parse_period(period)
        self.period_seconds = self.minutes * 60
        
        self.lock = threading.Lock()
        self.reset()
    
    def _parse_period(self, period: str) -> int:
        try:
            return int(period.replace('min', ''))
        except (ValueError, AttributeError) as e:
            raise ValueError(f"无效的 K 线周期格式：{period}, 错误：{e}")
    
    def reset(self) -> None:
        self.current_start: Optional[float] = None
        self.open: Optional[float] = None
        self.high: float = -float('inf')
        self.low: float = float('inf')
        self.close: Optional[float] = None
        self.volume: int = 0
        self.amount: float = 0.0
        self.open_interest: Optional[float] = None
        self.last_tick_time: Optional[float] = None
    
    def _get_period_start(self, ts: float) -> float:
        """返回 ts 所属周期的起始时间戳。"""
        return (ts // self.period_seconds) * self.period_seconds
    
    def update(self, ts: float, price: float,
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        用一条 tick 更新 K 线。如果当前 K 线完成，返回完成的 K 线数据，否则返回 None。
        """
        with self.lock:
            period_start = self._get_period_start(ts)
            
            if self.current_start is None:
                # 新周期开始
                self.current_start = period_start
                self.open = price
                self.high = price
                self.low = price
                self.close = price
                self.volume = volume
                self.amount = amount
                self.open_interest = open_interest
                self.last_tick_time = ts
                return None
            
            if period_start == self.current_start:
                # 当前周期内
                self.high = max(self.high, price)
                self.low = min(self.low, price)
                self.close = price
                self.volume += volume
                self.amount += amount
                if open_interest is not None:
                    self.open_interest = open_interest
                self.last_tick_time = ts
                return None
            
            elif period_start > self.current_start:
                # 新周期，当前 K 线完成
                completed = {
                    'ts': self.current_start,
                    'instrument_id': self.instrument_id,
                    'period': self.period,
                    'open': self.open,
                    'high': self.high if self.high != -float('inf') else None,
                    'low': self.low if self.low != float('inf') else None,
                    'close': self.close,
                    'volume': self.volume,
                    'amount': self.amount,
                    'open_interest': self.open_interest
                }
                
                # 重置为新周期
                self.current_start = period_start
                self.open = price
                self.high = price
                self.low = price
                self.close = price
                self.volume = volume
                self.amount = amount
                self.open_interest = open_interest
                self.last_tick_time = ts
                
                return completed
            
            else:
                # 历史 tick（乱序），忽略
                self.logger.warning("收到历史 tick: %.3f, 当前周期：%.3f", ts, self.current_start)
                return None


if __name__ == "__main__":
    # 生产环境示例代码已移除 - 测试请使用独立的 test_*.py 脚本
    print("Storage module loaded. Use test scripts for verification.")


# ============================================================================
# 市场时间服务（从 market_data_service.py 移入）
# ============================================================================

class MarketTimeService:
    """市场时间服务 - 负责市场开盘状态检测和交易时间配置"""
    
    def __init__(self):
        """初始化市场时间服务"""
        self._lock = threading.RLock()
        self._cache: Dict[str, Tuple[float, bool]] = {}  # 缓存 {exchange: (timestamp, is_open)}
        
        # 交易所交易时段配置（支持跨午夜夜盘）
        self._exchange_trading_sessions: Dict[str, List[Tuple[dt_time, dt_time]]] = {
            # 中金所：无夜盘
            'CFFEX': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 0), dt_time(15, 0)),
            ],
            # 上期所/大商所/能源中心/广期所：按宽窗口覆盖常见夜盘
            'SHFE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            'DCE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            'INE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            'GFEX': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            # 郑商所：夜盘通常较短，给到 23:30 的保护窗口
            'CZCE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(23, 30)),
            ],
        }
        
        # 初始化时检查一次开盘状态
        for exchange in self._exchange_trading_sessions:
            self._check_market_status(exchange)

    @staticmethod
    def _time_in_session(current_time: dt_time, session_start: dt_time, session_end: dt_time) -> bool:
        """判断当前时间是否落在某个交易时段内（支持跨午夜时段）。"""
        if session_start <= session_end:
            return session_start <= current_time < session_end
        # 跨午夜，如 21:00-02:30
        return current_time >= session_start or current_time < session_end
    
    def _check_market_status(self, exchange: str, current_time: Optional[datetime] = None) -> bool:
        """
        执行市场状态检查
        
        Args:
            exchange: 交易所代码
            current_time: 当前时间，默认为 None（使用当前时间）
            
        Returns:
            bool: 市场是否开盘
        """
        now = current_time or datetime.now()
        
        exchange = (exchange or 'CFFEX').upper()
        sessions = self._exchange_trading_sessions.get(exchange, self._exchange_trading_sessions['CFFEX'])
        current_time_obj = now.time()
        weekday = now.weekday()  # Mon=0 ... Sun=6

        # 周六全日关闭
        if weekday == 5:
            result = False
        # 周日仅夜盘窗口可能开（中金所无夜盘）
        elif weekday == 6:
            result = any(
                self._time_in_session(current_time_obj, start, end)
                for start, end in sessions
                if start > end
            )
        else:
            # 周五 21:00 后通常不开夜盘，直接关（中金所本来也无夜盘）
            if weekday == 4 and current_time_obj >= dt_time(21, 0):
                result = False
            else:
                result = any(
                    self._time_in_session(current_time_obj, start, end)
                    for start, end in sessions
                )
        
        # 更新缓存
        current_timestamp = now.timestamp()
        with self._lock:
            self._cache[exchange] = (current_timestamp, result)
        
        logging.debug(f"[MarketTimeService._check_market_status] Checked {exchange}: {result}")
        return result
    
    def is_market_open(self, exchange: str = 'CFFEX', current_time: Optional[datetime] = None) -> bool:
        """
        检查市场是否开盘
        
        只在以下情况进行实际检查：
        1. 服务重启时（缓存为空）
        2. 到达开盘时间时
        3. 到达收盘时间时
        
        Args:
            exchange: 交易所代码
            current_time: 当前时间，默认为 None（使用当前时间）
            
        Returns:
            bool: 市场是否开盘
        """
        try:
            now = current_time or datetime.now()
            current_timestamp = now.timestamp()
            current_time_obj = now.time()

            # 传入显式时间时直接实算，避免缓存污染测试/回放判断。
            if current_time is not None:
                return self._check_market_status(exchange, now)

            # 检查缓存是否需要更新（5 秒内不重复查）
            with self._lock:
                if exchange in self._cache:
                    cached_timestamp, cached_result = self._cache[exchange]
                    if current_timestamp - cached_timestamp < 5:
                        return cached_result

            # 执行实际检查
            return self._check_market_status(exchange, now)
        except Exception as e:
            logging.error(f"[MarketTimeService.is_market_open] Error checking market status for {exchange}: {e}")
            return False


# 全局市场时间服务实例
_market_time_service_instance = None
_market_time_service_lock = threading.Lock()

def get_market_time_service() -> MarketTimeService:
    """获取市场时间服务实例（单例模式）"""
    global _market_time_service_instance
    with _market_time_service_lock:
        if _market_time_service_instance is None:
            _market_time_service_instance = MarketTimeService()
        return _market_time_service_instance


def is_market_open(exchange: str = 'CFFEX', current_time: Optional[datetime] = None) -> bool:
    """检查市场是否开盘（便捷函数）"""
    service = get_market_time_service()
    return service.is_market_open(exchange, current_time)


# ========== 全局 InstrumentDataManager 单例 ==========
_instrument_data_manager_instance: Optional[InstrumentDataManager] = None
_instrument_data_manager_lock = threading.Lock()


def get_instrument_data_manager() -> InstrumentDataManager:
    """
    获取全局 InstrumentDataManager 单例（共享连接池）
    
    Returns:
        InstrumentDataManager: 全局单例实例
    """
    global _instrument_data_manager_instance
    
    with _instrument_data_manager_lock:
        if _instrument_data_manager_instance is None:
            # 使用默认配置创建全局单例
            from ali2026v3_trading.storage import _get_default_db_path
            db_path = _get_default_db_path()
            
            # 创建全局单例（使用默认配置：max_connections=20, async_queue_size=100000）
            _instrument_data_manager_instance = InstrumentDataManager(
                db_path=db_path,
                max_retries=3,
                retry_delay=0.1,
                async_queue_size=100000,
                batch_size=5000,
                drop_on_full=True,
                max_connections=20,
                cleanup_interval=3600,
                cleanup_config={'tick': 30, 'kline': 90}
            )
            
            logging.info(f"[Storage] Global InstrumentDataManager singleton initialized (db={db_path})")
        
        return _instrument_data_manager_instance