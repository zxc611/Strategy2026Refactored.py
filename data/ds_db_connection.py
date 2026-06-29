# MODULE_ID: M1-024
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""ds_db_connection.py - DuckDB连接池管理Mixin

从data_service.py拆分出的连接管理职责，包括：
- 连接配置
- 连接池（按需获取+用完归还模式）
- 连接健康检查与恢复
- 连接关闭
- 性能监控线程

架构变更（v2）：
- 旧模式：线程本地连接（_thread_local.conn），每个线程持有连接永不释放
- 新模式：连接池（Queue），按需借出+用完归还，连接跨线程复用
- 根因：旧模式下30+线程同时运行时连接池耗尽，高频回收导致性能雪崩
"""
try:
    import duckdb
except ImportError:
    duckdb = None
import logging
import threading
import os
import tempfile
import time
import queue
try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


class _TimedDuckDBConnection:
    """带超时保护的DuckDB连接代理

    使用持久化线程池执行查询，超时后标记连接不健康。
    所有数据库操作方法均有超时保护。
    """

    _QUERY_TIMEOUT_SEC = 30.0

    def __init__(self, conn: duckdb.DuckDBPyConnection, timeout_sec: float = 30.0):
        self._conn = conn
        self._QUERY_TIMEOUT_SEC = timeout_sec
        self._unhealthy = False
        self._last_used_time = time.time()
        self._in_use = False
        import concurrent.futures as _cf
        self._executor = _cf.ThreadPoolExecutor(max_workers=1)

    def _execute_with_timeout(self, fn, *args, **kwargs):
        if self._unhealthy:
            raise ConnectionError("连接已标记为不健康(前次查询超时)，请重新获取连接")
        import concurrent.futures
        try:
            self._last_used_time = time.time()
            _future = self._executor.submit(fn, *args, **kwargs)
            return _future.result(timeout=self._QUERY_TIMEOUT_SEC)
        except concurrent.futures.TimeoutError:
            self._unhealthy = True
            logger.error("[R5-E-10] 查询超时(%.1fs)，连接标记为不健康", self._QUERY_TIMEOUT_SEC)
            raise TimeoutError(f"查询超时({self._QUERY_TIMEOUT_SEC}s)，连接已不健康")

    def execute(self, query, parameters=None):
        if parameters is not None:
            return self._execute_with_timeout(self._conn.execute, query, parameters)
        return self._execute_with_timeout(self._conn.execute, query)

    def fetchall(self):
        return self._execute_with_timeout(self._conn.fetchall)

    def fetchone(self):
        return self._execute_with_timeout(self._conn.fetchone)

    def fetchdf(self):
        return self._execute_with_timeout(self._conn.fetchdf)

    def executemany(self, query, parameters=None):
        if parameters is not None:
            return self._execute_with_timeout(self._conn.executemany, query, parameters)
        return self._execute_with_timeout(self._conn.executemany, query)

    def register(self, view_name, python_object):
        return self._execute_with_timeout(self._conn.register, view_name, python_object)

    def unregister(self, view_name):
        try:
            return self._execute_with_timeout(self._conn.unregister, view_name)
        except Exception:
            pass

    def close(self):
        try:
            self._executor.shutdown(wait=False)
        except RuntimeError:
            pass
        except Exception:
            pass
        finally:
            self._conn.close()

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._conn, name)


class DBConnectionMixin:
    """DuckDB连接池管理Mixin - 由DataService组合使用

    v2架构：按需获取+用完归还模式
    - _pool: queue.Queue 存放空闲连接
    - _all_connections: list 追踪所有已创建连接（用于close_all）
    - _pool_size: 当前存活连接数（含借出和空闲）
    - _pool_lock: 保护 _pool_size 和 _all_connections
    - _MAX_POOL_SIZE: 连接池上限
    """

    _unsupported_params_warned = set()

    _CONNECTION_PARAMS_VERSION = "1.0"
    _CONNECTION_PARAMS_HISTORY: list = []
    _CLASS_LOCK = threading.Lock()

    _DB_CONNECT_TIMEOUT_SEC = 10.0

    _SHARED_MEMORY_FALLBACK_CONN = None
    _SHARED_MEMORY_FALLBACK_LOCK = threading.Lock()

    _MAX_POOL_SIZE = 50
    _MAX_IDLE_SECONDS = 120
    # FIX: 连接池满警告限流，避免高并发下每秒打印数十条警告刷屏
    _pool_full_warn_interval = 30.0  # 同一警告最多每30秒打印一次
    _pool_full_warn_last_ts = 0.0

    def _ensure_pool_initialized(self):
        if not hasattr(self, '_pool'):
            self._pool = queue.Queue(maxsize=self._MAX_POOL_SIZE)
            self._all_connections = []
            self._pool_lock = threading.Lock()
            self._pool_size = 0
            self._connection_generation = 0
            self._stop_monitor = threading.Event()
            self._integrity_checked = False
            self._pool_size_initialized = False
            self._pool_full_warn_last_ts = 0.0

    def _connect_with_timeout(self, db_file: str):
        import concurrent.futures as _cf
        _executor = _cf.ThreadPoolExecutor(max_workers=1)
        try:
            _future = _executor.submit(duckdb.connect, db_file, False)
            raw_conn = _future.result(timeout=self._DB_CONNECT_TIMEOUT_SEC)
            conn = _TimedDuckDBConnection(raw_conn, timeout_sec=30.0)
            return conn
        except _cf.TimeoutError:
            _future.cancel()
            logger.error("[R27-P0-DR-11] 数据库连接超时(%.1fs): %s", self._DB_CONNECT_TIMEOUT_SEC, db_file)
            raise ConnectionError(f"DB连接超时({self._DB_CONNECT_TIMEOUT_SEC}s)")
        finally:
            _executor.shutdown(wait=False)

    def _configure_connection(self, conn: duckdb.DuckDBPyConnection):
        mem_limit = self._get_duckdb_memory_limit()
        try:
            conn.execute(f"SET max_memory = '{mem_limit}'")
        except Exception as e:
            logger.warning(f"Failed to set max_memory: {e}")
        try:
            conn.execute(f"SET threads = {self.DUCKDB_THREADS}")
        except Exception as e:
            logger.warning(f"Failed to set threads: {e}")
        temp_dir = os.path.join(tempfile.gettempdir(), 'duckdb_tmp')
        os.makedirs(temp_dir, exist_ok=True)
        try:
            conn.execute(f"SET temp_directory = '{temp_dir}'")
        except Exception as e:
            logger.warning(f"Failed to set temp_directory (may already be set): {e}")
        try:
            conn.execute("SET max_temp_directory_size='100GB'")
        except Exception as e:
            logger.warning(f"Failed to set max_temp_directory_size: {e}")
        self._safe_set_param(conn, "enable_dictionary_compression", "true")
        try:
            conn.execute("SET preserve_insertion_order=false")
        except Exception as e:
            logger.warning(f"Failed to set preserve_insertion_order: {e}")
        self._safe_set_param(conn, "zstd_compression_level", "3")
        try:
            conn.execute("SET enable_progress_bar=false")
        except Exception as e:
            logger.warning(f"Failed to set enable_progress_bar: {e}")
        try:
            conn.execute("SET enable_http_metadata_cache=true")
        except Exception as e:
            logger.warning(f"Failed to set enable_http_metadata_cache: {e}")
        self._safe_set_param(conn, "catalog_error_max_scans", "0")

    def _safe_set_param(self, conn, param_name, param_value):
        try:
            conn.execute(f"SET {param_name}={param_value}")
        except Exception as e:
            with DBConnectionMixin._CLASS_LOCK:
                if param_name not in DBConnectionMixin._unsupported_params_warned:
                    DBConnectionMixin._unsupported_params_warned.add(param_name)
                    logger.debug(f"DuckDB param '{param_name}' not supported: {e}")

    def _get_connection_params_version(self) -> str:
        return self._CONNECTION_PARAMS_VERSION

    def _migrate_connection_params(self, conn, from_version: str, to_version: str) -> None:
        if from_version == to_version:
            return
        migrations_applied = []
        try:
            if from_version < "1.0" <= to_version:
                self._safe_set_param(conn, "enable_http_metadata_cache", "true")
                self._safe_set_param(conn, "catalog_error_max_scans", "0")
                migrations_applied.append("0.9->1.0")
            if migrations_applied:
                logger.info("UPG-P1-14: 连接参数迁移 %s -> %s, %s", from_version, to_version, ', '.join(migrations_applied))
                with DBConnectionMixin._CLASS_LOCK:
                    DBConnectionMixin._CONNECTION_PARAMS_HISTORY.append({
                        'from': from_version, 'to': to_version,
                        'migrations': migrations_applied, 'timestamp': time.time(),
                    })
        except Exception as e:
            logger.warning("UPG-P1-14: 连接参数迁移失败: %s", e)

    def _configure_connection_with_version(self, conn) -> None:
        self._configure_connection(conn)
        stored_version = getattr(self, '_stored_connection_params_version', self._CONNECTION_PARAMS_VERSION)
        if stored_version != self._CONNECTION_PARAMS_VERSION:
            self._migrate_connection_params(conn, stored_version, self._CONNECTION_PARAMS_VERSION)
            self._stored_connection_params_version = self._CONNECTION_PARAMS_VERSION

    def _get_duckdb_memory_limit(self) -> str:
        return self.DUCKDB_MAX_MEMORY if self.DUCKDB_MAX_MEMORY else '4GB'

    def _create_new_connection(self):
        try:
            conn = self._connect_with_timeout(self.DB_FILE)
        except (ConnectionError, Exception) as _conn_err:
            logger.warning("首次连接失败: %s, 尝试指数退避重连", _conn_err)
            conn = self._reconnect_with_backoff(max_retries=3)
        self._configure_connection(conn)
        if not getattr(self, '_integrity_checked', False):
            try:
                _ic_result = conn.execute("PRAGMA integrity_check").fetchone()
                if _ic_result and _ic_result[0] != 'ok':
                    logger.warning("DuckDB integrity_check: %s", _ic_result[0])
                else:
                    logger.info("DuckDB integrity_check passed")
            except Exception as _ice:
                logger.debug("integrity_check skipped: %s", _ice)
            self._integrity_checked = True
        return conn

    def _get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        self._ensure_pool_initialized()

        if not getattr(self, '_pool_size_initialized', False):
            try:
                from ali2026v3_trading.config.config_service import get_config_service
                _cs = get_config_service()
                _cfg_pool_size = getattr(getattr(_cs, 'database_config', None), 'connection_pool_size', None)
                if _cfg_pool_size is not None and isinstance(_cfg_pool_size, int) and _cfg_pool_size > 0:
                    self._MAX_POOL_SIZE = _cfg_pool_size
                    self._pool.maxsize = _cfg_pool_size
                    logger.info("从DatabaseConfig读取connection_pool_size=%d", _cfg_pool_size)
            except Exception:
                pass
            self._pool_size_initialized = True

        if read_only:
            logger.warning("[DataService] read_only=True requested but DuckDB requires single-mode; using read-write")

        if DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN is not None:
            return DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN

        try:
            conn = self._pool.get_nowait()
            if getattr(conn, '_unhealthy', False):
                self._destroy_connection(conn)
            else:
                conn._last_used_time = time.time()
                conn._in_use = True
                return conn
        except queue.Empty:
            pass

        with self._pool_lock:
            if self._pool_size < self._MAX_POOL_SIZE:
                self._pool_size += 1
            else:
                # FIX: 限流警告，避免高并发下刷屏（每_pool_full_warn_interval秒最多打印一次）
                _now = time.time()
                if _now - getattr(self, '_pool_full_warn_last_ts', 0.0) >= self._pool_full_warn_interval:
                    self._pool_full_warn_last_ts = _now
                    logger.warning("连接池已满(%d>=%d)，等待归还", self._pool_size, self._MAX_POOL_SIZE)

        if self._pool_size <= self._MAX_POOL_SIZE:
            try:
                conn = self._create_new_connection()
                with self._pool_lock:
                    self._all_connections.append(conn)
                conn._in_use = True
                return conn
            except Exception:
                with self._pool_lock:
                    self._pool_size = max(0, self._pool_size - 1)
                raise
        else:
            try:
                conn = self._pool.get(timeout=30.0)
                if getattr(conn, '_unhealthy', False):
                    self._destroy_connection(conn)
                    conn = self._create_new_connection()
                    with self._pool_lock:
                        self._all_connections.append(conn)
                conn._last_used_time = time.time()
                conn._in_use = True
                return conn
            except queue.Empty:
                logger.error("连接池耗尽(等待超时30s)，降级到内存数据库")
                return self._try_fallback_to_memory_db()

    def _get_read_connection(self) -> duckdb.DuckDBPyConnection:
        return self._get_connection(read_only=False)

    def set_max_pool_size(self, new_size: int) -> None:
        if new_size > 0:
            self._MAX_POOL_SIZE = new_size
            if hasattr(self, '_pool'):
                self._pool.maxsize = new_size
            logging.info("连接池上限调整为: %d", new_size)

    def release_connection(self):
        self._ensure_pool_initialized()
        conn = getattr(self._thread_local, 'conn', None)
        if conn is not None:
            self._return_connection(conn)
            self._thread_local.conn = None
            self._thread_local.conn_healthy = False

    def _return_connection(self, conn):
        if conn is None or conn is DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN:
            return
        conn._in_use = False
        conn._last_used_time = time.time()
        if getattr(conn, '_unhealthy', False):
            self._destroy_connection(conn)
            return
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            self._destroy_connection(conn)

    def _destroy_connection(self, conn):
        try:
            conn.close()
        except Exception:
            pass
        with self._pool_lock:
            if conn in self._all_connections:
                self._all_connections.remove(conn)
            self._pool_size = max(0, self._pool_size - 1)

    def _try_reclaim_connections(self) -> int:
        self._ensure_pool_initialized()
        reclaimed = 0
        now = time.time()
        with self._pool_lock:
            _to_remove = []
            for conn in list(self._all_connections):
                if conn is DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN:
                    continue
                if getattr(conn, '_unhealthy', False):
                    _to_remove.append(conn)
                    continue
                if not getattr(conn, '_in_use', True):
                    _idle_time = now - getattr(conn, '_last_used_time', now)
                    if _idle_time > self._MAX_IDLE_SECONDS:
                        _to_remove.append(conn)
            for conn in _to_remove:
                try:
                    conn.close()
                except Exception:
                    pass
                if conn in self._all_connections:
                    self._all_connections.remove(conn)
                self._pool_size = max(0, self._pool_size - 1)
                try:
                    self._pool.get_nowait()
                except queue.Empty:
                    pass
                reclaimed += 1
        if reclaimed > 0:
            logger.info("回收了%d个空闲/不健康连接", reclaimed)
        return reclaimed

    def _mark_connection_unhealthy(self):
        conn = getattr(self._thread_local, 'conn', None)
        if conn is not None:
            conn._unhealthy = True
        self._thread_local.conn_healthy = False

    @staticmethod
    def _is_fatal_database_error(e: Exception) -> bool:
        error_str = str(e)
        return 'FATAL' in error_str or 'database has been invalidated' in error_str

    def _try_fallback_to_memory_db(self) -> duckdb.DuckDBPyConnection:
        with DBConnectionMixin._SHARED_MEMORY_FALLBACK_LOCK:
            if DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN is not None:
                return DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN
            try:
                conn = duckdb.connect(':memory:')
                self._init_memory_fallback_schema(conn)
                DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN = conn
                logger.warning("[R30-P0-08] 降级到内存数据库(schema已初始化)，数据不会持久化！")
                return conn
            except Exception as mem_err:
                logger.critical("[R30-P0-08] 内存数据库创建也失败: %s", mem_err)
                raise

    def _init_memory_fallback_schema(self, conn) -> None:
        # 五唯一性修复：schema 定义见 ds_schema_manager.py，此处内存回退 schema 必须与规范保持一致
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS instrument_id_seq START 1")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS future_products (
                    product VARCHAR PRIMARY KEY, exchange VARCHAR, format_template VARCHAR,
                    tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS option_products (
                    product VARCHAR PRIMARY KEY, exchange VARCHAR, underlying_product VARCHAR,
                    format_template VARCHAR, tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS futures_instruments (
                    internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE, product VARCHAR,
                    exchange VARCHAR, year_month VARCHAR, is_active BOOLEAN, format VARCHAR,
                    expire_date VARCHAR, listing_date VARCHAR, kline_table VARCHAR,
                    tick_table VARCHAR, product_code VARCHAR, shard_key BIGINT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS option_instruments (
                    internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE, product VARCHAR,
                    exchange VARCHAR, underlying_future_id INTEGER, underlying_product VARCHAR,
                    year_month VARCHAR, option_type VARCHAR, strike_price DOUBLE, is_active BOOLEAN,
                    format VARCHAR, expire_date VARCHAR, listing_date VARCHAR, kline_table VARCHAR,
                    tick_table VARCHAR, product_code VARCHAR, shard_key BIGINT
                )
            """)
            try:
                conn.execute("DROP TABLE IF EXISTS ticks_raw")
            except Exception:
                pass
            conn.execute("""
                CREATE TABLE ticks_raw (
                    timestamp TIMESTAMP, instrument_id VARCHAR, exchange VARCHAR,
                    last_price DOUBLE, volume BIGINT, open_interest DOUBLE, turnover DOUBLE,
                    bid_price DOUBLE, ask_price DOUBLE, bid_volume BIGINT, ask_volume BIGINT,
                    bid_price2 DOUBLE, ask_price2 DOUBLE, bid_volume2 BIGINT, ask_volume2 BIGINT,
                    bid_price3 DOUBLE, ask_price3 DOUBLE, bid_volume3 BIGINT, ask_volume3 BIGINT,
                    bid_price4 DOUBLE, ask_price4 DOUBLE, bid_volume4 BIGINT, ask_volume4 BIGINT,
                    bid_price5 DOUBLE, ask_price5 DOUBLE, bid_volume5 BIGINT, ask_volume5 BIGINT,
                    date DATE, option_type VARCHAR, strike_price DOUBLE,
                    is_otm BOOLEAN, sync_status VARCHAR, future_sync_status VARCHAR,
                    is_same_rise BOOLEAN, is_same_fall BOOLEAN, is_diff_sync BOOLEAN,
                    spread_quality INTEGER, days_to_expiry INTEGER, implied_volatility DOUBLE
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS option_sync_otm_stats (
                    month VARCHAR, underlying_symbol VARCHAR, option_type VARCHAR,
                    correct_rise_otm_count BIGINT, wrong_rise_otm_count BIGINT,
                    correct_fall_otm_count BIGINT, wrong_fall_otm_count BIGINT,
                    other_otm_count BIGINT, total_otm_count BIGINT, total_samples BIGINT,
                    calculated_at TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW latest_prices AS
                SELECT instrument_id, last_price, timestamp FROM (
                    SELECT instrument_id, last_price, timestamp,
                    ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY timestamp DESC) AS rn
                    FROM ticks_raw
                    WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
                ) WHERE rn = 1
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW daily_aggregates AS
                SELECT date, COUNT(*) AS tick_count, AVG(last_price) AS avg_price,
                MAX(last_price) AS max_price, MIN(last_price) AS min_price, SUM(volume) AS total_volume
                FROM ticks_raw WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
                GROUP BY date ORDER BY date
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW symbol_daily_aggregates AS
                SELECT instrument_id, date, COUNT(*) AS tick_count,
                FIRST(last_price ORDER BY timestamp) AS open_price,
                LAST(last_price ORDER BY timestamp) AS close_price,
                MAX(last_price) AS high_price, MIN(last_price) AS low_price,
                SUM(volume) AS total_volume
                FROM ticks_raw WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
                GROUP BY instrument_id, date ORDER BY instrument_id, date
            """)
            logger.info("内存降级连接schema初始化完成（7张表+3视图）")
        except Exception as schema_err:
            logger.error("内存降级连接schema初始化失败: %s", schema_err)
            raise

    def _handle_fatal_db_error(self, error: Exception) -> None:
        conn = getattr(self._thread_local, 'conn', None)
        if conn is not None:
            conn._unhealthy = True
            self._return_connection(conn)
            self._thread_local.conn = None
        self._thread_local.conn_healthy = False
        if self._is_fatal_database_error(error):
            logger.critical("[R30-P0-08] FATAL数据库错误: %s，尝试降级到内存数据库", error)
            try:
                fallback_conn = self._try_fallback_to_memory_db()
                self._thread_local.conn = fallback_conn
                self._thread_local.conn_healthy = True
                logger.info("[R30-P0-08] 已降级到内存数据库，系统继续运行（数据不持久化）")
            except Exception as fallback_err:
                logger.critical("[R30-P0-08] 降级失败: %s，数据写入永久中断", fallback_err)

    def get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        self._ensure_pool_initialized()
        conn = self._get_connection(read_only)
        self._thread_local.conn = conn
        self._thread_local.conn_healthy = not getattr(conn, '_unhealthy', False)
        self._thread_local.conn_generation = getattr(self, '_connection_generation', 0)
        return conn

    def _reconnect_with_backoff(self, max_retries: int = 3) -> duckdb.DuckDBPyConnection:
        if DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN is not None:
            return DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN
        for attempt in range(max_retries):
            try:
                conn = self._connect_with_timeout(self.DB_FILE)
                return conn
            except (ConnectionError, Exception) as _conn_err:
                _delay = min(1.0 * (2 ** attempt), 10.0)
                logger.warning("重连第%d次失败: %s, %.1fs后重试", attempt + 1, _conn_err, _delay)
                time.sleep(_delay)
        logger.error("%d次重连全部失败，降级到内存数据库", max_retries)
        return self._try_fallback_to_memory_db()

    def close(self):
        conn = getattr(self._thread_local, 'conn', None)
        if conn is not None and conn is not DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN:
            self._return_connection(conn)
        self._thread_local.conn = None
        self._thread_local.conn_healthy = False

    def close_all(self):
        self._ensure_pool_initialized()
        self._stop_monitor.set()
        closed_count = 0
        with self._pool_lock:
            connections_to_close = list(self._all_connections)
            self._all_connections.clear()
            self._pool_size = 0
        while True:
            try:
                self._pool.get_nowait()
            except queue.Empty:
                break
        for conn in connections_to_close:
            if conn is DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN:
                continue
            try:
                conn.close()
                closed_count += 1
            except Exception as e:
                logger.debug(f"Failed to close connection: {e}")
        self._connection_generation = getattr(self, '_connection_generation', 0) + 1
        self._thread_local.conn = None
        self._thread_local.conn_healthy = False
        logger.info(f"DataService closed all {closed_count} connections and invalidated generation to {getattr(self, '_connection_generation', 0)}.")

    def _start_performance_monitor(self):
        def monitor():
            while not self._stop_monitor.is_set():
                if self._stop_monitor.wait(timeout=300):
                    break
                try:
                    mem_info = psutil.virtual_memory()
                    logger.debug(f"[DataService] System Memory: {mem_info.percent}% used, Available: {mem_info.available / (1024**3):.1f}GB")
                except Exception as e:
                    logger.warning(f"[DataService] Memory monitoring error: {e}")
        threading.Thread(target=monitor, daemon=True, name="DuckDB-PerfMonitor").start()
