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
from datetime import datetime
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
        
        # 自动清理配置
        self._cleanup_interval = cleanup_interval
        self._cleanup_config = cleanup_config or {}
        self._cleanup_thread = None
        
        # 初始化
        self._init_connection()
        self._init_kv_store()
        # 归还初始连接
        self._conn_pool.release_connection(self.conn)
        self._start_async_writer()
        
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
        
        # 启动自动清理线程
        if self._cleanup_interval:
            self._start_cleanup_thread()

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
                    self._flush_batch_to_db(batch)
                    with self._queue_stats_lock:
                        self._queue_stats['total_written'] += len(batch)
                    batch.clear()
                        
            except Exception as e:
                logging.error("[AsyncWriter] 写入异常：%s", e, exc_info=True)
                if batch:
                    batch.clear()
            
        # 退出前清空剩余数据
        if batch:
            try:
                self._flush_batch_to_db(batch)
                with self._queue_stats_lock:
                    self._queue_stats['total_written'] += len(batch)
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
        
    def _flush_batch_to_db(self, batch: List[Tuple[str, Any, Any]]):
        """批量刷新到数据库。"""
        if not batch:
            return
            
        conn = None
        try:
            conn = self._conn_pool.get_connection()
            cursor = conn.cursor()
                
            for func_name, args, kwargs in batch:
                # 根据函数名调用对应的实现
                if hasattr(self, func_name):
                    method = getattr(self, func_name)
                    method(cursor, *args, **kwargs)
                
            conn.commit()
        except sqlite3.Error as e:
            logging.error("[AsyncWriter] 批量写入失败：%s", e)
            if conn:
                conn.rollback()
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
    
    def shutdown(self, flush: bool = True) -> None:
        """
        优雅关闭：停止后台线程，可选择等待队列清空。
        Args:
            flush: True 则等待所有待处理任务完成；False 则丢弃队列中剩余任务。
        """
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
                            # 使用当前连接直接插入，避免递归导致的事务冲突
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
                            
                            self.conn.commit()
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
                              prod['underlying_product'], parsed['year_month'],
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
        :param kline_period: 仅当data_type='kline'时，指定周期如 '1min'
        """
        internal_id = self.register_instrument(instrument_id)
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
        internal_id = self.register_instrument(instrument_id)
        cursor = self.conn.cursor()
        if data_type:
            cursor.execute("UPDATE subscriptions SET is_active=0 WHERE instrument_id=? AND data_type=?", (internal_id, data_type))
        else:
            cursor.execute("UPDATE subscriptions SET is_active=0 WHERE instrument_id=?", (internal_id,))
        self.conn.commit()
        logging.info(f"取消订阅：{instrument_id} (ID={internal_id})")

    # ========== 数据写入 ==========
    def write_kline_by_id(self, instrument_id: int, kline_data: List[Dict]) -> None:
        """通过内部ID写入K线数据"""
        if not kline_data:
            return
        info = self._get_info_by_id(instrument_id)
        if not info:
            raise ValueError(f"未找到合约ID: {instrument_id}")
        table_name = info['kline_table']
        self._validate_table_name(table_name)

        cursor = self.conn.cursor()
        values = [(k['timestamp'], k['open'], k['high'], k['low'],
                   k['close'], k['volume'], k['open_interest']) for k in kline_data]
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name}
            (timestamp, open, high, low, close, volume, open_interest)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)
        self.conn.commit()
        logging.debug(f"写入 {len(kline_data)} 条K线到 {table_name}")

    def write_tick_by_id(self, instrument_id: int, tick_data: List[Dict]) -> None:
        """通过内部ID写入Tick数据（自动判断期货/期权）"""
        if not tick_data:
            return
        info = self._get_info_by_id(instrument_id)
        if not info:
            raise ValueError(f"未找到合约ID: {instrument_id}")
        table_name = info['tick_table']
        self._validate_table_name(table_name)

        cursor = self.conn.cursor()
        if info['type'] == 'future':
            values = [(t['timestamp'], t['last_price'], t['volume'], t['open_interest'],
                       t.get('bid_price1'), t.get('ask_price1')) for t in tick_data]
            sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
                VALUES (?, ?, ?, ?, ?, ?)
            """
        else:
            values = [(t['timestamp'], t['last_price'], t['volume'], t['open_interest'],
                       t.get('bid_price1'), t.get('ask_price1'),
                       t.get('implied_volatility'), t.get('delta'), t.get('gamma'),
                       t.get('theta'), t.get('vega')) for t in tick_data]
            sql = f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1,
                 implied_volatility, delta, gamma, theta, vega)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        cursor.executemany(sql, values)
        self.conn.commit()
        logging.debug(f"写入 {len(tick_data)} 条Tick到 {table_name}")

    # ========== 数据查询 ==========
    def query_kline_by_id(self, instrument_id: int, start_time: str, end_time: str,
                          limit: Optional[int] = None) -> List[Dict]:
        """查询K线数据"""
        info = self._get_info_by_id(instrument_id)
        if not info:
            raise ValueError(f"未找到合约ID: {instrument_id}")
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
        """查询Tick数据"""
        info = self._get_info_by_id(instrument_id)
        if not info:
            raise ValueError(f"未找到合约ID: {instrument_id}")
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
        required = ['ts', 'instrument_id', 'last_price']
        for field in required:
            if field not in tick:
                logging.error("save_tick 缺少必要字段：%s", field)
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
        required = ['ts', 'instrument_id', 'period', 'open', 'high', 'low', 'close']
        for field in required:
            if field not in kline:
                logging.error("save_external_kline 缺少必要字段：%s", field)
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
        tick_ts = self._to_timestamp(tick.get('ts'))
        if tick_ts is None:
            logging.error("process_tick 时间戳转换失败：%s", tick.get('ts'))
            return
        tick['ts'] = tick_ts
        instrument = tick.get('instrument_id')
        price = tick.get('last_price')
        
        # 1. 保存 tick
        internal_id = self.register_instrument(instrument)
        tick_data = [tick]
        self.write_tick_by_id(internal_id, tick_data)
        
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
        ts = self._to_timestamp(kline_data.get('ts'))
        if ts is None:
            logging.error("save_external_kline 时间戳转换失败：%s", kline_data.get('ts'))
            return
        kline_data['ts'] = ts
        
        # 写入数据库
        internal_id = self.register_instrument(kline_data['instrument_id'])
        self.write_kline_by_id(internal_id, [kline_data])
        
        # 更新时间戳
        key = (kline_data['instrument_id'], kline_data['period'])
        with self._ext_kline_lock:
            self._last_ext_kline[key] = ts
    
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
        """批量写入 K 线数据（优化版，直接入队）"""
        if not kline_data:
            return
        internal_id = self.register_instrument(instrument_id)
        # 分批入队，避免单次数据过大
        chunk_size = self.batch_size
        for i in range(0, len(kline_data), chunk_size):
            chunk = kline_data[i:i+chunk_size]
            self.write_kline_by_id(internal_id, chunk)
    
    def batch_write_tick(self, instrument_id: str, tick_data: List[Dict]) -> None:
        """批量写入 Tick 数据（优化版，直接入队）"""
        if not tick_data:
            return
        internal_id = self.register_instrument(instrument_id)
        # 分批入队
        chunk_size = self.batch_size
        for i in range(0, len(tick_data), chunk_size):
            chunk = tick_data[i:i+chunk_size]
            self.write_tick_by_id(internal_id, chunk)
    
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
        values = [(k['timestamp'], k['open'], k['high'], k['low'],
                   k['close'], k['volume'], k['open_interest']) for k in kline_data]
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
        # 简化版，假设是期货 Tick
        values = [(t['timestamp'], t['last_price'], t['volume'], t['open_interest'],
                   t.get('bid_price1'), t.get('ask_price1')) for t in tick_data]
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
    
    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """根据期货合约代码获取期权链（基于 instrument_id 而非 internal_id）。"""
        info = self._get_instrument_info(future_instrument_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货合约：{future_instrument_id}")
        
        future_id = info['id']
        return self.get_option_chain_by_future_id(future_id)
    
    # ========== 自动清理线程 ==========
    def _start_cleanup_thread(self):
        """启动自动清理线程。"""
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
    
    # ========== 通用数据存储（异步） ==========
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
    
    def _save_tick_impl(self, cursor, tick: Dict[str, Any]):
        """保存 Tick 数据的底层实现（同步）。"""
        internal_id = self.register_instrument(tick['instrument_id'])
        info = self._get_info_by_id(internal_id)
        if not info:
            logging.error("_save_tick_impl 未找到合约：%s", tick['instrument_id'])
            return
        
        table_name = info['tick_table']
        self._validate_table_name(table_name)
        
        values = (tick['ts'], tick['last_price'], tick.get('volume', 0),
                  tick.get('open_interest'), tick.get('bid_price1'), tick.get('ask_price1'))
        cursor.execute(f"""
            INSERT OR REPLACE INTO {table_name}
            (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
            VALUES (?, ?, ?, ?, ?, ?)
        """, values)
    
    def save_tick(self, tick: Dict[str, Any]) -> None:
        """保存原始 Tick 数据（异步）。"""
        if not self._validate_tick(tick):
            return
        tick = tick.copy()
        ts = self._to_timestamp(tick.get('ts'))
        if ts is None:
            logging.error("save_tick 时间戳转换失败：%s", tick.get('ts'))
            return
        tick['ts'] = ts
        self._enqueue_write('_save_tick_impl', tick)
    
    def _save_depth_batch_impl(self, cursor, depth_list: List[Dict[str, Any]]):
        """批量保存深度行情（同步实现）。"""
        for d in depth_list:
            internal_id = self.register_instrument(d['instrument_id'])
            info = self._get_info_by_id(internal_id)
            if not info:
                continue
            
            table_name = info['tick_table']
            self._validate_table_name(table_name)
            
            # 构建字段列表（根据实际数据动态调整）
            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            placeholders = ['?', '?', '?', '?']
            values = [d['ts'], d['last_price'], d.get('volume', 0), d.get('open_interest')]
            
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
        # 简化实现，实际应保存到专门的 signal 表
        logging.debug("保存信号：%s", signal.get('signal_type'))
    
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
    
    def _save_signal_impl(self, cursor, signal: Dict[str, Any]):
        """保存信号（同步实现）。"""
        internal_id = self.register_instrument(signal['instrument_id'])
        info = self._get_info_by_id(internal_id)
        if not info:
            logging.error("_save_signal_impl 找不到合约信息：%s", signal['instrument_id'])
            return
        
        # 简化实现，保存到 kv_store
        key = f"signal:{signal['ts']}:{signal['instrument_id']}:{signal['strategy_name']}"
        self.save(key, signal)
    
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
            internal_id = self.register_instrument(d['instrument_id'])
            info = self._get_info_by_id(internal_id)
            if not info:
                continue
            
            table_name = info['tick_table']
            self._validate_table_name(table_name)
            
            # 期权 Tick 包含希腊字母
            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            values = [d['ts'], d['last_price'], d.get('volume', 0), d.get('open_interest')]
            
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
                            
                            # 判断是期货还是期权
                            if '-' in instrument_id and ('C' in instrument_id or 'P' in instrument_id):
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
                                if '-' in inst_id and ('C' in inst_id or 'P' in inst_id):
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

    def save(self, key: str, data: Any) -> bool:
        """
        同步保存任意结构化数据到键值存储。
        :param key: 键名（字符串）
        :param data: 任意可 JSON 序列化的数据
        :return: 是否保存成功
        """
        if not key or not isinstance(key, str):
            logging.error("save 失败：key 无效：%s", key)
            return False

        try:
            payload = json.dumps(data, ensure_ascii=False, default=self._json_default)
            cursor = self.conn.cursor()
            cursor.execute("BEGIN")
            cursor.execute(
                """
                INSERT OR REPLACE INTO app_kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                (key, payload, time.time())
            )
            self.conn.commit()
            return True
        except Exception as e:
            logging.error("save 失败 key=%s: %s", key, e, exc_info=True)
            self.conn.rollback()
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
        # 停止异步写入线程
        self._stop_async_writer()
        
        # 停止自动清理线程
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._stop_event.set()
            self._cleanup_thread.join(timeout=5.0)
        
        # 关闭连接池
        self._conn_pool.close_all()
        logging.info("数据库资源已关闭")
    
    def shutdown(self, flush: bool = True):
        """
        关闭服务（storage.py 兼容接口）
        :param flush: 是否刷新队列中的数据
        """
        if flush:
            # 等待队列清空
            time.sleep(0.5)
        self.close()
        logging.info("服务已关闭")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ========== T 型图订阅管理器 ==========
class TTypeSubscriptionManager:
    """
    T 型图订阅管理器，负责期权链的订阅管理。
    支持：握手协议、窗口化节流、自动重连。
    """
    
    def __init__(self, data_manager: InstrumentDataManager, max_retries: int = 3):
        self.data_manager = data_manager
        self.max_retries = max_retries
        self._subscriptions: Dict[str, Dict] = {}  # {underlying: subscription_info}
        self._lock = threading.Lock()
        self._handshake_timeout = 10.0  # 秒
        self._throttle_window = 1.0     # 秒
    
    def subscribe_option_chain(self, underlying: str, expiration: str,
                               strikes: List[float], option_types: List[str]) -> bool:
        """
        订阅整个期权链。
        :param underlying: 标的期货代码
        :param expiration: 到期年月 (YYYYMM)
        :param strikes: 行权价列表
        :param option_types: 期权类型列表 (['C', 'P'])
        :return: 是否订阅成功
        """
        with self._lock:
            key = f"{underlying}_{expiration}"
            if key in self._subscriptions:
                logging.info("重复订阅，忽略：%s", key)
                return True
            
            # 执行握手协议（简化版）
            if not self._handshake(underlying, expiration):
                logging.error("握手失败：%s", key)
                return False
            
            # 注册所有期权合约
            subscribed = []
            for strike in strikes:
                for opt_type in option_types:
                    instrument_id = f"{underlying}{expiration}-{opt_type}-{int(strike)}"
                    try:
                        internal_id = self.data_manager.subscribe(instrument_id, 'tick')
                        subscribed.append(instrument_id)
                        logging.debug("订阅期权：%s -> ID=%d", instrument_id, internal_id)
                    except Exception as e:
                        logging.warning("订阅失败 %s: %s", instrument_id, e)
            
            self._subscriptions[key] = {
                'underlying': underlying,
                'expiration': expiration,
                'strikes': strikes,
                'option_types': option_types,
                'subscribed': subscribed,
                'created_at': time.time()
            }
            
            logging.info("期权链订阅完成：%s (%d 个合约)", key, len(subscribed))
            return True
    
    def _handshake(self, underlying: str, expiration: str) -> bool:
        """
        握手协议（简化实现）。
        实际应调用交易所 API 进行认证和限流控制。
        """
        logging.debug("T 型图握手：%s %s", underlying, expiration)
        # 简化实现：直接返回成功
        return True
    
    def unsubscribe_option_chain(self, underlying: str, expiration: str) -> bool:
        """取消订阅期权链。"""
        with self._lock:
            key = f"{underlying}_{expiration}"
            if key not in self._subscriptions:
                logging.warning("未找到订阅：%s", key)
                return False
            
            sub = self._subscriptions[key]
            for instrument_id in sub['subscribed']:
                try:
                    self.data_manager.unsubscribe(instrument_id, 'tick')
                except Exception as e:
                    logging.warning("取消订阅失败 %s: %s", instrument_id, e)
            
            del self._subscriptions[key]
            logging.info("取消订阅：%s", key)
            return True
    
    def get_subscription_info(self, underlying: str, expiration: str) -> Optional[Dict]:
        """获取订阅信息。"""
        with self._lock:
            key = f"{underlying}_{expiration}"
            return self._subscriptions.get(key)
    
    def list_all_subscriptions(self) -> List[Dict]:
        """列出所有订阅。"""
        with self._lock:
            return list(self._subscriptions.values())


# ========== 使用示例 ==========
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


# ========== 使用示例 ==========
if __name__ == "__main__":
    # 初始化（带异步队列和自动清理）
    cleanup_config = {'tick': 30, 'kline': 90}  # Tick 保留 30 天，K 线保留 90 天
    with InstrumentDataManager("market_data.db", async_queue_size=100000,
                               batch_size=5000, cleanup_interval=3600,
                               cleanup_config=cleanup_config) as mgr:
        
        # 初始化品种（模拟，实际应从配置加载）
        cursor = mgr.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO future_products (product, exchange, format_template, tick_size, contract_size) VALUES (?, ?, ?, ?, ?)",
                       ("IF", "CFFEX", "YYYYMM", 0.2, 300.0))
        cursor.execute("INSERT OR IGNORE INTO option_products (product, exchange, underlying_product, format_template, tick_size, contract_size) VALUES (?, ?, ?, ?, ?, ?)",
                       ("IO", "CFFEX", "IF", "YYYYMM-C-XXXX", 0.2, 300.0))
        mgr.conn.commit()

        # ===== 测试基础功能 =====
        print("\n=== 测试基础订阅和数据写入 ===")
        future_id = mgr.subscribe("IF2603", "kline", "1min")
        option_id = mgr.subscribe("IO2603-C-4500", "tick")

        # 写入 K 线数据
        kline_data = [
            {"timestamp": "2026-03-01 09:30:00", "open": 4500.0, "high": 4520.0, "low": 4490.0,
             "close": 4510.0, "volume": 10000, "open_interest": 50000}
        ]
        mgr.write_kline_by_id(future_id, kline_data)

        # 写入 Tick 数据（期权）
        tick_data = [
            {"timestamp": "2026-03-01 09:30:00", "last_price": 4510.0, "volume": 100,
             "open_interest": 50000, "bid_price1": 4505.0, "ask_price1": 4515.0,
             "implied_volatility": 0.25, "delta": 0.5, "gamma": 0.02, "theta": -0.1, "vega": 0.3}
        ]
        mgr.write_tick_by_id(option_id, tick_data)

        # 查询
        klines = mgr.query_kline_by_id(future_id, "2026-03-01 00:00:00", "2026-03-02 00:00:00")
        print("K 线结果:", klines)
        ticks = mgr.query_tick_by_id(option_id, "2026-03-01 00:00:00", "2026-03-02 00:00:00")
        print("Tick 结果:", ticks)

        # 获取期权链
        chain = mgr.get_option_chain_by_future_id(future_id)
        print("期权链:", chain)
        
        # ===== 测试 KV 存储 =====
        print("\n=== 测试 KV 存储 ===")
        test_data = {
            'counter': 100,
            'items': ['a', 'b', 'c'],
            'nested': {'x': 1, 'y': 2},
            'timestamp': datetime.now()
        }
        success = mgr.save('test_state', test_data)
        print(f"KV 保存：{'成功' if success else '失败'}")
        loaded = mgr.load('test_state')
        print(f"KV 加载：{loaded}")
        
        # ===== 测试智能 Tick 处理 =====
        print("\n=== 测试智能 Tick 处理 ===")
        sample_tick = {
            'ts': time.time(),
            'instrument_id': 'IF2603',
            'last_price': 4520.0,
            'volume': 150,
            'open_interest': 51000
        }
        mgr.process_tick(sample_tick)
        print("智能 Tick 处理完成")
        
        # ===== 测试外部 K 线保存 =====
        print("\n=== 测试外部 K 线保存 ===")
        external_kline = {
            'ts': time.time(),
            'instrument_id': 'IF2603',
            'period': '1min',
            'open': 4520.0,
            'high': 4530.0,
            'low': 4515.0,
            'close': 4525.0,
            'volume': 200
        }
        mgr.save_external_kline(external_kline)
        print("外部 K 线保存完成")
        
        # ===== 测试异步队列统计 =====
        print("\n=== 队列统计 ===")
        stats = mgr.get_queue_stats()
        print(f"队列统计：{stats}")
        
        # ===== 测试 T 型图订阅管理器 =====
        print("\n=== 测试 T 型图订阅管理器 ===")
        ttype_mgr = TTypeSubscriptionManager(mgr)
        success = ttype_mgr.subscribe_option_chain(
            underlying='IF',
            expiration='2603',
            strikes=[4400, 4500, 4600],
            option_types=['C', 'P']
        )
        print(f"T 型图订阅：{'成功' if success else '失败'}")
        subscriptions = ttype_mgr.list_all_subscriptions()
        print(f"当前订阅数：{len(subscriptions)}")
        
        print("\n所有测试完成!")
        
        # ===== 测试新增功能 =====
        print("\n=== 测试 KV 存储 ===")
        # 保存状态
        test_data = {
            'counter': 100,
            'items': ['a', 'b', 'c'],
            'nested': {'x': 1, 'y': 2},
            'timestamp': datetime.now()
        }
        success = mgr.save('test_state', test_data)
        print(f"KV 保存：{'成功' if success else '失败'}")
        
        # 加载状态
        loaded = mgr.load('test_state')
        print(f"KV 加载：{loaded}")
        
        # 测试数据清理
        print("\n=== 测试数据清理 ===")
        try:
            deleted = mgr.cleanup_old_data('tick', days=30)
            print(f"清理 Tick 数据：{deleted} 条")
        except Exception as e:
            print(f"清理失败：{e}")
        
        print("\n所有测试完成!")