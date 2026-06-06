"""ds_db_connection.py - DuckDB连接池管理Mixin

从data_service.py拆分出的连接管理职责，包括：
- 连接配置
- 线程本地连接获取
- 连接健康检查与恢复
- 连接关闭
- 性能监控线程
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
try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


class _DuckDBConnectionContextManager:
    """R5-E-09修复: DuckDB连接的context manager包装器

    将DuckDB连接包装为支持with语句的资源管理器，
    确保异常时连接被正确释放，消除裸连接泄露风险。
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, owner=None):
        self._conn = conn
        self._owner = owner

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and self._owner is not None and hasattr(self._owner, '_thread_local'):
            try:
                if hasattr(self._owner._thread_local, 'conn_healthy'):
                    self._owner._thread_local.conn_healthy = False
            except AttributeError:
                pass  # [R26-FIX] thread_local无conn_healthy属性，无需设置
            except Exception as e:
                logging.debug("[R26-FIX] conn_healthy标志设置失败(非关键): %s", e)
        try:
            self._conn.close()
        except Exception as e:
            logger.debug("R5-E-09: context manager关闭连接时异常(可忽略): %s", e)
        return False


class _TimedDuckDBConnection:
    """R5-E-10修复: 带超时保护的DuckDB连接代理

    使用持久化线程池执行查询，超时后标记连接不健康。
    所有数据库操作方法均有超时保护。
    """

    _QUERY_TIMEOUT_SEC = 30.0

    def __init__(self, conn: duckdb.DuckDBPyConnection, timeout_sec: float = 30.0):
        self._conn = conn
        self._QUERY_TIMEOUT_SEC = timeout_sec
        self._unhealthy = False
        self._last_used_time = time.time()
        import concurrent.futures as _cf
        self._executor = _cf.ThreadPoolExecutor(max_workers=1)

    def _execute_with_timeout(self, fn, *args, **kwargs):
        if self._unhealthy:
            raise ConnectionError("R5-E-10: 连接已标记为不健康(前次查询超时)，请重新获取连接")
        import concurrent.futures
        try:
            self._last_used_time = time.time()
            _future = self._executor.submit(fn, *args, **kwargs)
            return _future.result(timeout=self._QUERY_TIMEOUT_SEC)
        except concurrent.futures.TimeoutError:
            self._unhealthy = True
            logger.error("[R5-E-10] 查询超时(%.1fs)，连接标记为不健康", self._QUERY_TIMEOUT_SEC)
            raise TimeoutError(f"R5-E-10: 查询超时({self._QUERY_TIMEOUT_SEC}s)，连接已不健康")

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
        """R17-P1-PERF-04修复: 批量执行SQL（带超时保护）"""
        if parameters is not None:
            return self._execute_with_timeout(self._conn.executemany, query, parameters)
        return self._execute_with_timeout(self._conn.executemany, query)

    def close(self):
        try:
            self._executor.shutdown(wait=False)
        except RuntimeError as e:
            logging.debug("[R26-FIX] executor已关闭(cannot schedule new futures): %s", e)
        except Exception as e:
            logging.warning("[R26-FIX] executor.shutdown异常: %s", e)
        finally:
            self._conn.close()

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._conn, name)


class DBConnectionMixin:
    """DuckDB连接池管理Mixin - 由DataService组合使用"""

    _unsupported_params_warned = set()

    # UPG-P1-14修复: 连接参数版本化
    _CONNECTION_PARAMS_VERSION = "1.0"
    _CONNECTION_PARAMS_HISTORY: list = []  # 记录参数变更历史
    _CLASS_LOCK = threading.Lock()  # 线程安全：保护可变类级别对象

    # R27-P0-DR-11修复: 数据库连接超时保护方法
    _DB_CONNECT_TIMEOUT_SEC = 10.0

    def _connect_with_timeout(self, db_file: str):
        """R27-P0-DR-11修复: 带超时保护的数据库连接，防止无限阻塞

        R5-E-10修复: 返回_TimedDuckDBConnection代理，为查询增加30秒超时保护
        """
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
            raise ConnectionError(f"R27-P0-DR-11: DB连接超时({self._DB_CONNECT_TIMEOUT_SEC}s)")
        finally:
            _executor.shutdown(wait=False)

    def _configure_connection(self, conn: duckdb.DuckDBPyConnection):
        """配置 DuckDB 连接：最大化性能配置（内存、并行度、压缩、WAL）"""
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
                    logger.debug(f"DuckDB param '{param_name}' not supported in this version, skipping: {e}")

    # UPG-P1-14修复: 连接参数版本化和迁移
    def _get_connection_params_version(self) -> str:
        """UPG-P1-14修复: 获取当前连接参数版本"""
        return self._CONNECTION_PARAMS_VERSION

    def _migrate_connection_params(self, conn: duckdb.DuckDBPyConnection,
                                    from_version: str, to_version: str) -> None:
        """UPG-P1-14修复: 连接参数迁移

        当连接参数版本变更时，对已有连接执行参数迁移。
        当前迁移规则：
        - 0.9 → 1.0: 启用enable_http_metadata_cache, catalog_error_max_scans

        Args:
            conn: DuckDB连接
            from_version: 源版本
            to_version: 目标版本
        """
        if from_version == to_version:
            return

        migrations_applied = []
        try:
            # 迁移规则: 0.9 → 1.0
            if from_version < "1.0" <= to_version:
                self._safe_set_param(conn, "enable_http_metadata_cache", "true")
                self._safe_set_param(conn, "catalog_error_max_scans", "0")
                migrations_applied.append("0.9→1.0")

            if migrations_applied:
                logger.info(
                    "UPG-P1-14: 连接参数迁移 %s → %s, 已应用: %s",
                    from_version, to_version, ', '.join(migrations_applied),
                )
                with DBConnectionMixin._CLASS_LOCK:
                    DBConnectionMixin._CONNECTION_PARAMS_HISTORY.append({
                        'from': from_version,
                        'to': to_version,
                        'migrations': migrations_applied,
                        'timestamp': time.time(),
                    })
        except Exception as e:
            logger.warning("UPG-P1-14: 连接参数迁移失败 %s → %s: %s", from_version, to_version, e)

    def _configure_connection_with_version(self, conn: duckdb.DuckDBPyConnection) -> None:
        """UPG-P1-14修复: 带版本追踪的连接配置

        先执行标准配置，再检查是否需要参数迁移。
        """
        self._configure_connection(conn)
        # 检查是否需要迁移（从存储的版本到当前版本）
        stored_version = getattr(self, '_stored_connection_params_version', self._CONNECTION_PARAMS_VERSION)
        if stored_version != self._CONNECTION_PARAMS_VERSION:
            self._migrate_connection_params(conn, stored_version, self._CONNECTION_PARAMS_VERSION)
            self._stored_connection_params_version = self._CONNECTION_PARAMS_VERSION

    def _get_duckdb_memory_limit(self) -> str:
        """P1 修复：返回静态配置的内存限制，杜绝动态计算的不确定性"""
        return self.DUCKDB_MAX_MEMORY if self.DUCKDB_MAX_MEMORY else '4GB'

    # R15-P1-PERF-16修复: DuckDB已有线程本地连接池(_thread_local)，单例模式避免重复创建
    # R25-BV-02-FIX: 连接池大小限制和耗尽fallback
    # R31-P1-17修复: 从DatabaseConfig读取connection_pool_size，与配置系统对齐
    _MAX_POOL_SIZE = 5  # R33-P1-17修复: 默认值与DatabaseConfig.connection_pool_size对齐

    def _get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        # R31-P1-17修复: 首次调用时尝试从DatabaseConfig读取connection_pool_size
        if not getattr(self, '_pool_size_initialized', False):
            try:
                from ali2026v3_trading.config_service import get_config_service
                _cs = get_config_service()
                _cfg_pool_size = getattr(getattr(_cs, 'database_config', None), 'connection_pool_size', None)
                if _cfg_pool_size is not None and isinstance(_cfg_pool_size, int) and _cfg_pool_size > 0:
                    self._MAX_POOL_SIZE = _cfg_pool_size
                    logger.info("[R31-P1-17] 从DatabaseConfig读取connection_pool_size=%d", _cfg_pool_size)
            except ImportError as e:
                logging.debug("[R26-FIX] config_service模块不可用，使用默认pool_size=%d: %s", self._MAX_POOL_SIZE, e)
            except AttributeError as e:
                logging.debug("[R26-FIX] DatabaseConfig无connection_pool_size属性，使用默认值: %s", e)
            except Exception as e:
                logging.warning("[R26-FIX] connection_pool_size读取异常，使用默认值: %s", e)
            self._pool_size_initialized = True
        if read_only:
            logger.warning("[DataService] read_only=True requested but DuckDB requires single-mode; using read-write")
        with self._all_connections_lock:
            _current_pool_size = len(self._all_connections)
        if _current_pool_size >= self._MAX_POOL_SIZE:
            logger.warning("[R25-BV-02-FIX] 连接池已满(%d>=%d), 尝试回收过期连接", _current_pool_size, self._MAX_POOL_SIZE)
            _reclaimed = self._try_reclaim_connections()
            if _reclaimed == 0:
                logger.error("[R27-BV-02-FIX] 连接池耗尽且无法回收, 降级到内存数据库")
                return self._try_fallback_to_memory_db()
        conn_attr = 'conn'
        current_gen = getattr(self, '_connection_generation', 0)
        conn_gen = getattr(self._thread_local, 'conn_generation', -1)
        if not hasattr(self._thread_local, conn_attr) or getattr(self._thread_local, conn_attr) is None:
            # R27-P0-DR-11修复: 数据库连接添加超时保护，防止无限阻塞
            # R31-P0-01修复: 连接失败时使用_reconnect_with_backoff重试而非直接抛异常
            try:
                conn = self._connect_with_timeout(self.DB_FILE)
            except (ConnectionError, Exception) as _conn_err:
                logger.warning("[R31-P0-01] 首次连接失败: %s, 尝试指数退避重连", _conn_err)
                conn = self._reconnect_with_backoff(max_retries=3)
            self._configure_connection(conn)
            setattr(self._thread_local, conn_attr, conn)
            self._thread_local.conn_healthy = True
            self._thread_local.conn_generation = current_gen
            with self._all_connections_lock:
                self._all_connections.append(conn)
            # R15-P1-DATA-05修复: 首次连接后执行integrity_check
            if not getattr(self, '_integrity_checked', False):
                try:
                    _ic_result = conn.execute("PRAGMA integrity_check").fetchone()
                    if _ic_result and _ic_result[0] != 'ok':
                        logger.warning("R15-P1-DATA-05: DuckDB integrity_check: %s", _ic_result[0])
                    else:
                        logger.info("R15-P1-DATA-05: DuckDB integrity_check passed")
                except Exception as _ice:
                    logger.debug("R15-P1-DATA-05: integrity_check skipped: %s", _ice)
                self._integrity_checked = True
        elif conn_gen != current_gen or not getattr(self._thread_local, 'conn_healthy', True):
            reason = "generation mismatch (close_all invalidated)" if conn_gen != current_gen else "unhealthy"
            logger.warning("[DataService] Connection invalidated (%s), recreating...", reason)
            try:
                old_conn = getattr(self._thread_local, conn_attr)
                with self._all_connections_lock:
                    if old_conn in self._all_connections:
                        self._all_connections.remove(old_conn)
                old_conn.close()
            except Exception as e:
                logger.warning(f"Failed to close invalidated connection: {e}")
            # R31-P0-01修复: 连接重建失败时使用_reconnect_with_backoff重试
            try:
                conn = self._connect_with_timeout(self.DB_FILE)  # R27-P0-DR-11修复: 超时保护
            except (ConnectionError, Exception) as _reconn_err:
                logger.warning("[R31-P0-01] 连接重建失败: %s, 尝试指数退避重连", _reconn_err)
                conn = self._reconnect_with_backoff(max_retries=3)
            self._configure_connection(conn)
            setattr(self._thread_local, conn_attr, conn)
            self._thread_local.conn_healthy = True
            self._thread_local.conn_generation = current_gen
            with self._all_connections_lock:
                self._all_connections.append(conn)
            logger.info("[DataService] Connection recovered successfully")
        return getattr(self._thread_local, conn_attr)

    def _get_read_connection(self) -> duckdb.DuckDBPyConnection:
        return self._get_connection(read_only=False)

    def set_max_pool_size(self, new_size: int) -> None:
        """R33-P1-17修复: 动态调整连接池上限"""
        if new_size > 0:
            self._MAX_POOL_SIZE = new_size
            logging.info("[R33-P1-17] 连接池上限调整为: %d", new_size)

    _MAX_IDLE_SECONDS = 600

    def _try_reclaim_connections(self) -> int:
        """R25-BV-02-FIX: 尝试回收不健康或过期的连接，返回回收数量
        R26-P2-DB-01修复: 增加基于空闲时间的连接回收，防止长期空闲连接资源泄漏
        """
        reclaimed = 0
        now = time.time()
        with self._all_connections_lock:
            _to_remove = []
            for conn in self._all_connections:
                try:
                    _idle_time = now - getattr(conn, '_last_used_time', now) if hasattr(conn, '_last_used_time') else 0
                    if _idle_time > self._MAX_IDLE_SECONDS:
                        _to_remove.append(conn)
                        continue
                    conn.execute("SELECT 1").fetchone()
                except Exception:
                    _to_remove.append(conn)
            for conn in _to_remove:
                try:
                    conn.close()
                except Exception as e:
                    logging.debug("[R26-FIX] conn.close()失败(连接可能已关闭): %s", e)
                finally:
                    self._all_connections.remove(conn)
                reclaimed += 1
        if reclaimed > 0:
            logger.info("[R25-BV-02-FIX] 回收了%d个过期/空闲连接", reclaimed)
        return reclaimed

    def _mark_connection_unhealthy(self):
        """标记当前线程连接为不健康（P1 FATAL error 恢复机制）"""
        self._thread_local.conn_healthy = False

    @staticmethod
    def _is_fatal_database_error(e: Exception) -> bool:
        """检测是否为 DuckDB FATAL 数据库错误（导致连接不可恢复的致命错误）"""
        error_str = str(e)
        return 'FATAL' in error_str or 'database has been invalidated' in error_str

    # R30-P0-08修复: 数据库文件损坏降级路径
    def _try_fallback_to_memory_db(self) -> duckdb.DuckDBPyConnection:
        """R30-P0-08修复: DuckDB文件损坏时降级到内存数据库

        R5-E-09修复: _DuckDBConnectionContextManager可用于with语句管理降级连接生命周期
        R5-E-10修复: 使用_TimedDuckDBConnection添加查询超时保护

        Returns:
            内存数据库连接(带超时保护代理)
        """
        try:
            raw_conn = duckdb.connect(':memory:')
            conn = _TimedDuckDBConnection(raw_conn, timeout_sec=30.0)
            # R5-E-09修复: _DuckDBConnectionContextManager可用于with语句管理降级连接生命周期
            # 调用方需with语句时: with _DuckDBConnectionContextManager(conn, self) as c:
            logger.warning("[R30-P0-08] 降级到内存数据库(带超时保护)，数据不会持久化！请尽快修复主数据库文件。")
            return conn
        except Exception as mem_err:
            logger.critical("[R30-P0-08] 内存数据库创建也失败: %s", mem_err)
            raise

    def _handle_fatal_db_error(self, error: Exception) -> None:
        """R30-P0-08修复: 处理FATAL数据库错误，尝试降级恢复

        Args:
            error: 原始异常
        """
        self._thread_local.conn_healthy = False
        if self._is_fatal_database_error(error):
            logger.critical("[R30-P0-08] FATAL数据库错误: %s，尝试降级到内存数据库", error)
            try:
                fallback_conn = self._try_fallback_to_memory_db()
                self._configure_connection(fallback_conn)
                setattr(self._thread_local, 'conn', fallback_conn)
                self._thread_local.conn_healthy = True
                with self._all_connections_lock:
                    self._all_connections.append(fallback_conn)
                logger.info("[R30-P0-08] 已降级到内存数据库，系统继续运行（数据不持久化）")
            except Exception as fallback_err:
                logger.critical("[R30-P0-08] 降级失败: %s，数据写入永久中断", fallback_err)

    def get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        return self._get_connection(read_only)

    # R15-P1-RES-03修复: 连接失败后自动重连(最多3次)
    # R21-MEM-P2-14修复: 本方法使用for循环实现重试，非递归调用，无栈溢出风险。
    # 若未来改为递归重连实现，需在入口检查sys.getrecursionlimit()确保递归深度不超过限制。
    def _reconnect_with_backoff(self, max_retries: int = 3) -> duckdb.DuckDBPyConnection:
        for attempt in range(max_retries):
            try:
                self._thread_local.conn = None
                self._thread_local.conn_healthy = False
                return self._get_connection()
            except Exception as e:
                _delay = min(1.0 * (2 ** attempt), 10.0)
                logger.warning("R15-P1-RES-03: 重连第%d次失败: %s, %.1fs后重试", attempt + 1, e, _delay)
                time.sleep(_delay)
        raise ConnectionError(f"R15-P1-RES-03: {max_retries}次重连全部失败")

    def close(self):
        if hasattr(self._thread_local, 'conn'):
            try:
                self._thread_local.conn.close()
            except Exception as e:
                logger.warning(f"Failed to close thread-local connection: {e}")
            self._thread_local.conn = None

    def close_all(self):
        """P1 Bug #41修复：关闭所有线程的连接，并标记为无效"""
        self._stop_monitor.set()
        closed_count = 0
        with self._all_connections_lock:
            connections_to_close = list(self._all_connections)
            self._all_connections.clear()
        for conn in connections_to_close:
            try:
                conn.close()
                closed_count += 1
            except Exception as e:
                logger.warning(f"Failed to close connection: {e}")
        self._connection_generation = getattr(self, '_connection_generation', 0) + 1
        self._thread_local.conn = None
        self._thread_local.conn_healthy = False
        logger.info(f"DataService closed all {closed_count} connections and invalidated generation to {getattr(self, '_connection_generation', 0)}.")

    def _start_performance_monitor(self):
        """P1 修复：简化监控逻辑，实现长期稳定的后台内存监控"""
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
