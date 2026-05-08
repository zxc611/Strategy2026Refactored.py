"""ds_db_connection.py - DuckDB连接池管理Mixin

从data_service.py拆分出的连接管理职责，包括：
- 连接配置
- 线程本地连接获取
- 连接健康检查与恢复
- 连接关闭
- 性能监控线程
"""
import duckdb
import logging
import threading
import os
import tempfile
import psutil

logger = logging.getLogger(__name__)


class DBConnectionMixin:
    """DuckDB连接池管理Mixin - 由DataService组合使用"""

    _unsupported_params_warned = set()

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
            if param_name not in DBConnectionMixin._unsupported_params_warned:
                DBConnectionMixin._unsupported_params_warned.add(param_name)
                logger.debug(f"DuckDB param '{param_name}' not supported in this version, skipping: {e}")

    def _get_duckdb_memory_limit(self) -> str:
        """P1 修复：返回静态配置的内存限制，杜绝动态计算的不确定性"""
        return self.DUCKDB_MAX_MEMORY if self.DUCKDB_MAX_MEMORY else '4GB'

    def _get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        # DuckDB不允许同时使用read_only=True和read_only=False打开同一个数据库
        # 忽略read_only参数，所有连接都使用读写模式
        conn_attr = 'conn'
        if not hasattr(self._thread_local, conn_attr) or getattr(self._thread_local, conn_attr) is None:
            conn = duckdb.connect(self.DB_FILE, read_only=False)
            self._configure_connection(conn)
            setattr(self._thread_local, conn_attr, conn)
            self._thread_local.conn_healthy = True
            with self._all_connections_lock:
                self._all_connections.append(conn)
        elif not getattr(self._thread_local, 'conn_healthy', True):
            logger.warning("[DataService] Connection marked unhealthy, recreating...")
            try:
                old_conn = getattr(self._thread_local, conn_attr)
                with self._all_connections_lock:
                    if old_conn in self._all_connections:
                        self._all_connections.remove(old_conn)
                old_conn.close()
            except Exception as e:
                logger.warning(f"Failed to close unhealthy connection: {e}")
            conn = duckdb.connect(self.DB_FILE, read_only=False)
            self._configure_connection(conn)
            setattr(self._thread_local, conn_attr, conn)
            self._thread_local.conn_healthy = True
            with self._all_connections_lock:
                self._all_connections.append(conn)
            logger.info("[DataService] Connection recovered successfully")
        return getattr(self._thread_local, conn_attr)

    def _get_read_connection(self) -> duckdb.DuckDBPyConnection:
        # DuckDB不允许同时使用read_only=True和read_only=False打开同一个数据库
        # 所有连接都使用读写模式
        return self._get_connection(read_only=False)

    def _mark_connection_unhealthy(self):
        """标记当前线程连接为不健康（P1 FATAL error 恢复机制）"""
        self._thread_local.conn_healthy = False

    @staticmethod
    def _is_fatal_database_error(e: Exception) -> bool:
        """检测是否为 DuckDB FATAL 数据库错误（导致连接不可恢复的致命错误）"""
        error_str = str(e)
        return 'FATAL' in error_str or 'database has been invalidated' in error_str

    def close(self):
        if hasattr(self._thread_local, 'conn'):
            try:
                self._thread_local.conn.close()
            except Exception as e:
                logger.warning(f"Failed to close thread-local connection: {e}")
            self._thread_local.conn = None

    def close_all(self):
        """P1 Bug #41修复：关闭所有线程的连接，而不仅仅是当前线程"""
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

        self.close()
        logger.info(f"DataService closed all {closed_count} connections.")

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
