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

FIX-20260702:
- 所有 DuckDB 连接统一走 db_adapter，避免同库多入口配置不一致。
- 支持 DUCKDB_POOL_DISABLED=1 单连接模式，降低 PythonGO 嵌入式环境下
  多连接/多线程触发 _duckdb.cp312-win_amd64.pyd 原生崩溃的风险。
"""
import logging
import threading
import os
import tempfile
import time
import queue
from typing import Dict as _Dict
try:
    import psutil
except ImportError:
    psutil = None

from ali2026v3_trading.data.db_adapter import connect as _db_connect
from ali2026v3_trading.data.db_adapter import connect_in_memory as _db_connect_in_memory
from ali2026v3_trading.data.db_adapter import get_duckdb_connection_type

logger = logging.getLogger(__name__)


_DuckDBPyConnection = get_duckdb_connection_type()


def _is_embedded_python() -> bool:
    """检测是否运行在 PythonGO 嵌入式环境中。

    判断依据：
    1. 进程名为 InfiniTrader（实盘平台宿主进程）
    2. sys.executable 不包含 'python'（嵌入式 Python 解释器的可执行文件名通常不是 python）
    3. 存在 INFINITRADER_INSTANCE_ID 环境变量（实盘平台设置）

    FIX-20260702-HARDEN: 在嵌入式环境中自动启用安全模式（单连接+同步+threads=1）
    """
    import sys
    try:
        proc_name = os.path.basename(getattr(os, 'get_executable', lambda: sys.executable)()).lower()
    except Exception:
        proc_name = os.path.basename(sys.executable).lower()

    if 'infinitrader' in proc_name:
        return True

    exe_lower = os.path.basename(sys.executable).lower()
    if 'python' not in exe_lower and exe_lower.endswith('.exe'):
        return True

    if os.getenv('INFINITRADER_INSTANCE_ID'):
        return True

    return False


def _duckdb_preflight_check() -> bool:
    """DuckDB 安全预检：在子进程中测试 DuckDB 是否能正常工作。

    在 PythonGO 嵌入式环境中，DuckDB 原生扩展可能与宿主进程的
    C++ 运行时不兼容。此函数通过子进程测试来验证兼容性。

    Returns:
        True: DuckDB 可安全使用
        False: DuckDB 在当前环境下不可用，应完全禁用

    FIX-20260708-DUCKDB-PREFLIGHT: 嵌入式环境下跳过子进程预检，
    原因：sys.executable=InfiniTrader.exe，不是Python解释器，
    subprocess.run([InfiniTrader.exe, '-c', ...]) 会超时(10s)阻塞启动。
    嵌入式环境改为进程内直接测试 import duckdb。
    """
    import subprocess
    import sys

    # FIX-20260708-DUCKDB-PREFLIGHT: 嵌入式环境跳过子进程预检
    if _is_embedded_python():
        try:
            import duckdb
            c = duckdb.connect(':memory:')
            c.execute('SELECT 1').fetchone()
            c.close()
            logger.info("[FIX-20260708] DuckDB 进程内预检通过(嵌入式环境跳过子进程)")
            return True
        except Exception as e:
            logger.error("[FIX-20260708] DuckDB 进程内预检失败(嵌入式环境): %s", e)
            return False

    # 非嵌入式环境：原有子进程预检逻辑
    check_script = (
        "import duckdb; "
        "c = duckdb.connect(':memory:'); "
        "c.execute('SELECT 1').fetchone(); "
        "c.close(); "
        "print('OK')"
    )
    try:
        result = subprocess.run(
            [sys.executable, '-c', check_script],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and ('OK' in result.stdout or not result.stderr):
            return True
        else:
            logger.error(
                "[FIX-20260702] DuckDB 预检失败！returncode=%d, stdout=%s, stderr=%s",
                result.returncode, result.stdout[:200], result.stderr[:500]
            )
            return False
    except Exception as e:
        logger.error("[FIX-20260702] DuckDB 预检异常: %s", e, exc_info=True)
        return False


class _TimedDuckDBConnection:
    """带超时保护的DuckDB连接代理

    使用持久化线程池执行查询，超时后标记连接不健康。
    所有数据库操作方法均有超时保护。

    FIX-20260702-HARDEN:
    - 当 DUCKDB_POOL_DISABLED=1 (单连接模式) 时，查询改为同步执行，
      避免 ThreadPoolExecutor 线程跳转触发 _duckdb.cp312 原生崩溃
      (异常码 0xc0000409，偏移 0x186f565)。
    - 增加 owner_thread 跟踪，跨线程访问时记录 warning，便于定位
      嵌入式环境下的线程/DuckDB ABI 不兼容问题。
    """

    _QUERY_TIMEOUT_SEC = 30.0
    # FIX-20260702-HARDEN: 嵌入式环境或 DUCKDB_POOL_DISABLED=1 时同步执行查询，
    # 不创建 ThreadPoolExecutor，避免线程跳转触发 _duckdb.cp312 原生崩溃(0xc0000409)。
    # 注意：此值在类定义时计算，依赖 _is_embedded_python() 的检测结果。
    _SYNC_EXEC = (
        os.getenv('DUCKDB_POOL_DISABLED', '0').lower() in ('1', 'true', 'yes')
        or _is_embedded_python()
    )

    def __init__(self, conn: "_DuckDBPyConnection", timeout_sec: float = 30.0):
        self._conn = conn
        self._QUERY_TIMEOUT_SEC = timeout_sec
        self._unhealthy = False
        self._last_used_time = time.time()
        self._in_use = False
        # FIX-20260702-HARDEN: 记录创建线程，检测跨线程访问
        self._owner_thread = threading.get_ident()
        self._cross_thread_warned = False
        if not _TimedDuckDBConnection._SYNC_EXEC:
            import concurrent.futures as _cf
            self._executor = _cf.ThreadPoolExecutor(max_workers=1)
        else:
            self._executor = None

    def _execute_with_timeout(self, fn, *args, **kwargs):
        if self._unhealthy:
            raise ConnectionError("连接已标记为不健康(前次查询超时)，请重新获取连接")
        self._last_used_time = time.time()
        # FIX-20260702-HARDEN: 跨线程访问检测（仅警告，不阻断）
        _current_thread = threading.get_ident()
        if _current_thread != self._owner_thread and not self._cross_thread_warned:
            self._cross_thread_warned = True
            logger.info(
                "[FIX-20260702] DuckDB连接跨线程访问: owner=%s current=%s "
                "(单连接模式下建议同线程访问以降低原生崩溃风险)",
                self._owner_thread, _current_thread,
            )
        # FIX-20260702-HARDEN: 单连接模式下同步执行，避免 ThreadPoolExecutor 线程跳转
        if self._executor is None:
            _single_mode = getattr(DBConnectionMixin, '_POOL_DISABLED', False)
            _query_lock = DBConnectionMixin._SINGLE_QUERY_LOCK if _single_mode else None
            if _query_lock is not None:
                _query_lock.acquire()
            try:
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    _msg = str(exc).lower()
                    _is_catalog_err = 'catalog error' in _msg or 'does not exist' in _msg
                    _is_fatal_err = ('hang' in _msg or 'timeout' in _msg or 'deadlock' in _msg) and not _is_catalog_err
                    if _is_fatal_err:
                        self._unhealthy = True
                        logger.error("[FIX-20260702] 连接标记为不健康(超时/死锁): %s", exc)
                    elif not _is_catalog_err:
                        logger.debug("[FIX-20260702] 同步执行异常(非致命，不标记不健康): %s", exc)
                    raise
            finally:
                if _query_lock is not None:
                    _query_lock.release()
        # 多连接模式：保留 ThreadPoolExecutor 超时保护
        import concurrent.futures
        try:
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
        # FIX-20260702-HARDEN: 单连接模式下 _executor 为 None，跳过 shutdown
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False)
            except RuntimeError:
                pass
            except Exception:
                pass
        try:
            self._conn.close()
        except Exception as _close_err:
            logger.debug("[FIX-20260702] DuckDB连接关闭异常(已忽略): %s", _close_err)

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

    # FIX-20260702-HARDEN: 自动检测 PythonGO 嵌入式环境，启用安全模式
    # 根因：DuckDB 内部 C++ 线程池(SET threads=N)在 PythonGO 嵌入式进程中
    # 与宿主 InfiniTrader C++ 运行时冲突，触发 _duckdb.cp312 原生崩溃(0xc0000409)。
    # 安全模式：单连接 + 同步执行 + threads=1，彻底消除多线程冲突。
    _EMBEDDED_MODE = (
        os.getenv('DUCKDB_POOL_DISABLED', '').lower() in ('1', 'true', 'yes')
        or _is_embedded_python()
    )
    _POOL_DISABLED = _EMBEDDED_MODE or os.getenv('DUCKDB_POOL_DISABLED', '0').lower() in ('1', 'true', 'yes')
    _EMBEDDED_THREADS = 1 if _EMBEDDED_MODE else None  # None = use default
    _SINGLE_CONN: "_TimedDuckDBConnection" = None
    _SINGLE_CONN_LOCK = threading.Lock()
    _SINGLE_QUERY_LOCK = threading.Lock()
    _THREAD_LOCAL_CONNS: "_Dict[int, _TimedDuckDBConnection]" = {}
    _THREAD_LOCAL_CONNS_LOCK = threading.Lock()
    _PREFLIGHT_PASSED = False

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
        # FIX-20260702: 单连接模式下避免创建临时 ThreadPoolExecutor 线程，
        # 降低 PythonGO 嵌入式环境中线程/DuckDB 原生崩溃风险。
        if DBConnectionMixin._POOL_DISABLED:
            try:
                raw_conn = _db_connect(db_file, False)
                return _TimedDuckDBConnection(raw_conn, timeout_sec=30.0)
            except Exception as e:
                logger.error("[R27-P0-DR-11] 数据库连接失败: %s", e)
                raise ConnectionError(f"DB连接失败: {e}")

        import concurrent.futures as _cf
        _executor = _cf.ThreadPoolExecutor(max_workers=1)
        try:
            # FIX-20260702: 统一走 db_adapter.connect，确保 read_only 等配置一致。
            _future = _executor.submit(_db_connect, db_file, False)
            raw_conn = _future.result(timeout=self._DB_CONNECT_TIMEOUT_SEC)
            conn = _TimedDuckDBConnection(raw_conn, timeout_sec=30.0)
            return conn
        except _cf.TimeoutError:
            _future.cancel()
            logger.error("[R27-P0-DR-11] 数据库连接超时(%.1fs): %s", self._DB_CONNECT_TIMEOUT_SEC, db_file)
            raise ConnectionError(f"DB连接超时({self._DB_CONNECT_TIMEOUT_SEC}s)")
        finally:
            _executor.shutdown(wait=False)

    def _configure_connection(self, conn: "_DuckDBPyConnection"):
        mem_limit = self._get_duckdb_memory_limit()
        try:
            conn.execute(f"SET max_memory = '{mem_limit}'")
        except Exception as e:
            logger.warning(f"Failed to set max_memory: {e}")
        # FIX-20260702-HARDEN: 嵌入式环境下强制 threads=1，
        # 避免 DuckDB 内部 C++ 线程池与 PythonGO 宿主进程冲突
        _threads = DBConnectionMixin._EMBEDDED_THREADS or self.DUCKDB_THREADS
        if DBConnectionMixin._EMBEDDED_MODE and _threads != 1:
            logger.info(
                "[FIX-20260702] 嵌入式环境: DuckDB threads 从 %d 降为 1 "
                "(避免内部C++线程池触发原生崩溃 0xc0000409)",
                self.DUCKDB_THREADS,
            )
            _threads = 1
        try:
            conn.execute(f"SET threads = {_threads}")
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
        # FIX-20260702-HARDEN: 嵌入式环境下首次连接前做安全预检
        if DBConnectionMixin._EMBEDDED_MODE and not DBConnectionMixin._PREFLIGHT_PASSED:
            logger.info("[FIX-20260702] 嵌入式环境检测到，执行 DuckDB 安全预检...")
            passed = _duckdb_preflight_check()
            DBConnectionMixin._PREFLIGHT_PASSED = True  # 只检一次
            if not passed:
                logger.error(
                    "[FIX-20260702] DuckDB 安全预检失败！嵌入式环境可能不兼容 DuckDB 原生扩展。"
                    "回退到内存模式以避免原生崩溃(0xc0000409)。"
                )
                self.DB_FILE = ':memory:'
                self._USE_MEMORY_MODE = True

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
        # FIX-20260703: 确保核心 schema(ticks_raw + latest_prices)存在于每个新连接上
        # 根因: 单连接模式下连接被标记不健康后销毁重建，新连接上表/视图可能不存在
        _schema_hook = getattr(self, '_ensure_core_schema_on_connect', None)
        if _schema_hook is not None:
            try:
                _schema_hook(conn)
            except Exception as _schema_err:
                logger.warning("[FIX-20260703] 新连接 schema 初始化失败: %s", _schema_err)
        return conn

    def _get_connection(self, read_only: bool = False) -> "_DuckDBPyConnection":
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

        # FIX-20260702: 单连接模式，避免 PythonGO 嵌入式环境下多连接崩溃。
        # FIX-20260704-P0-3: 线程局部连接，避免跨线程使用同一DuckDB连接导致原生崩溃(0xc0000409)
        # 根因: owner=9180 current=19192 跨线程调用DuckDB C API，单连接锁只防并发不防跨线程
        if DBConnectionMixin._POOL_DISABLED:
            _current_tid = threading.get_ident()
            with DBConnectionMixin._THREAD_LOCAL_CONNS_LOCK:
                _tl_conn = DBConnectionMixin._THREAD_LOCAL_CONNS.get(_current_tid)
                if _tl_conn is not None and not getattr(_tl_conn, '_unhealthy', False):
                    _tl_conn._last_used_time = time.time()
                    _tl_conn._in_use = True
                    return _tl_conn
            with DBConnectionMixin._SINGLE_CONN_LOCK:
                if DBConnectionMixin._SINGLE_CONN is None or getattr(DBConnectionMixin._SINGLE_CONN, '_unhealthy', False):
                    DBConnectionMixin._SINGLE_CONN = self._create_new_connection()
                    logger.info("[FIX-20260702] DUCKDB_POOL_DISABLED=1，使用单连接模式")
                DBConnectionMixin._SINGLE_CONN._last_used_time = time.time()
                DBConnectionMixin._SINGLE_CONN._in_use = True
            _owner_tid = DBConnectionMixin._SINGLE_CONN._owner_thread
            if _current_tid != _owner_tid:
                with DBConnectionMixin._THREAD_LOCAL_CONNS_LOCK:
                    _tl_conn = DBConnectionMixin._THREAD_LOCAL_CONNS.get(_current_tid)
                    if _tl_conn is None or getattr(_tl_conn, '_unhealthy', False):
                        if _tl_conn is not None:
                            try:
                                _tl_conn.close()
                            except Exception:
                                pass
                        _tl_conn = self._create_new_connection()
                        _tl_conn._owner_thread = _current_tid
                        DBConnectionMixin._THREAD_LOCAL_CONNS[_current_tid] = _tl_conn
                        logger.debug("[FIX-20260704-P0-3] 为线程%d创建独立DuckDB连接(owner=%d), 避免跨线程原生崩溃",
                                    _current_tid, _owner_tid)
                    _tl_conn._last_used_time = time.time()
                    _tl_conn._in_use = True
                    return _tl_conn
            return DBConnectionMixin._SINGLE_CONN

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

    def _get_read_connection(self) -> "_DuckDBPyConnection":
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
        # FIX-20260706: 线程局部连接归还——标记空闲，不健康则销毁重建
        _is_tl_conn = False
        with DBConnectionMixin._THREAD_LOCAL_CONNS_LOCK:
            _is_tl_conn = conn in DBConnectionMixin._THREAD_LOCAL_CONNS.values()
        if _is_tl_conn:
            conn._in_use = False
            conn._last_used_time = time.time()
            if getattr(conn, '_unhealthy', False):
                self._destroy_connection(conn)
            return
        # FIX-20260702: 单连接模式下不归还连接池，仅标记为空闲。
        if conn is DBConnectionMixin._SINGLE_CONN:
            conn._in_use = False
            conn._last_used_time = time.time()
            if getattr(conn, '_unhealthy', False):
                self._destroy_connection(conn)
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
        # FIX-20260706: 从线程局部连接表中移除已销毁连接，防止下次获取到已关闭连接
        with DBConnectionMixin._THREAD_LOCAL_CONNS_LOCK:
            _dead_tids = [tid for tid, c in DBConnectionMixin._THREAD_LOCAL_CONNS.items() if c is conn]
            for _tid in _dead_tids:
                del DBConnectionMixin._THREAD_LOCAL_CONNS[_tid]
        # FIX-20260702: 单连接模式被销毁时清空静态引用，便于下次重建。
        if conn is DBConnectionMixin._SINGLE_CONN:
            with DBConnectionMixin._SINGLE_CONN_LOCK:
                DBConnectionMixin._SINGLE_CONN = None
            return
        with self._pool_lock:
            if conn in self._all_connections:
                self._all_connections.remove(conn)
            self._pool_size = max(0, self._pool_size - 1)

    def _try_reclaim_connections(self) -> int:
        self._ensure_pool_initialized()
        # FIX-20260702: 单连接模式不回收。
        if DBConnectionMixin._POOL_DISABLED:
            return 0
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

    def _try_fallback_to_memory_db(self) -> "_DuckDBPyConnection":
        with DBConnectionMixin._SHARED_MEMORY_FALLBACK_LOCK:
            if DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN is not None:
                return DBConnectionMixin._SHARED_MEMORY_FALLBACK_CONN
            try:
                # FIX-20260702: 统一走 db_adapter.connect_in_memory。
                conn = _db_connect_in_memory()
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

    def get_connection(self, read_only: bool = False) -> "_DuckDBPyConnection":
        self._ensure_pool_initialized()
        conn = self._get_connection(read_only)
        self._thread_local.conn = conn
        self._thread_local.conn_healthy = not getattr(conn, '_unhealthy', False)
        self._thread_local.conn_generation = getattr(self, '_connection_generation', 0)
        return conn

    def _reconnect_with_backoff(self, max_retries: int = 3) -> "_DuckDBPyConnection":
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
