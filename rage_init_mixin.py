# MODULE_ID: M1-044
"""
storage_init_mixin.py — 存储核心初始化服务
包含: _get_default_db_path, StorageInitService

重构说明 (2026-06-11):
- _StorageInitMixin → StorageInitService（服务提取，消灭Mixin）
- 构造函数显式接收依赖
"""

from ali2026v3_trading.infra.shared_utils import InitPhase, InitStateMachine, requires_phase, ThreadLifecycleManager, CHINA_TZ, ShardRouter, extract_product_code
from ali2026v3_trading.data.query_service import _KlineAggregator, StorageMaintenanceService
from ali2026v3_trading.infra.subscription_service import SubscriptionConfig, SubscriptionManager
from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
import logging
import os
import re
import threading
import time
import json
import queue
import sys
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

# P0-R11-20: 跨进程文件锁 — DuckDB只支持单进程写入，多进程同时写入会导致数据损坏
# 使用 msvcrt(Windows) / fcntl(Linux) 实现OS级文件锁，配合 threading.Lock() 作为进程内伴生锁
if sys.platform == 'win32':
    import msvcrt as _filelock_module
    _FILELOCK_AVAILABLE = True
else:
    try:
        import fcntl as _filelock_module
        _FILELOCK_AVAILABLE = True
    except ImportError:
        _FILELOCK_AVAILABLE = False
        logging.getLogger(__name__).warning("P0-R11-20: fcntl不可用，跨进程文件锁已禁用")

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads

# R27-P1修复: 导入有界重试和容错工具
from ali2026v3_trading.infra.resilience import (
    BoundedRetry, ExponentialBackoff, Watchdog, HeartbeatMonitor,
    SlowQueryDetector, DataStalenessDetector, ResourceLeakDetector,
    safe_callback_wrapper, get_process_health,
)


logger = logging.getLogger(__name__)


def _get_default_db_path() -> str:
    try:
        from ali2026v3_trading.config.config_service import get_default_db_path

        db_path = get_default_db_path()
        if db_path:
            # R15-P1-SEC-02修复: 路径消毒+越权检查
            _resolved = os.path.realpath(db_path)
            _proj_root = os.path.realpath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            if not _resolved.startswith(_proj_root):
                logging.warning("R15-P1-SEC-02: db_path越权,回退默认: %s", db_path)
                db_path = None
            else:
                return db_path
    except Exception as exc:
        logging.warning("[storage] Failed to resolve db path from config_service: %s", exc)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, 'trading_data.duckdb')


class StorageInitService:

    __version__ = "2026-03-26-FIXED-dbpath-explicit"

    _TABLE_NAME_PATTERN = re.compile(r'^[A-Za-z0-9_]+$')

    SUPPORTED_PERIODS = ['1min', '5min', '15min', '30min', '60min']


    DEFAULT_TICK_SHARD_COUNT = 16
    DEFAULT_TICK_WRITER_COUNT = 6
    DEFAULT_MAX_SUB_BATCH = 50
    DEFAULT_SHARD_CAP_MIN = 10000
    DEFAULT_SHARD_CAP_DIVISOR = 10000000
    DEFAULT_KLINE_MISSING_TIMEOUT_MULTIPLIER = 2.0

    # P0-R11-20: 跨进程文件锁超时(秒) — 超过此时间未能获取锁则跳过写入
    DB_LOCK_TIMEOUT = 30.0

    def __init__(self, db_path: Optional[str] = None, max_retries: int = 3, retry_delay: float = 0.1,
                 async_queue_size: int = 500000, batch_size: int = 5000,
                 drop_on_full: bool = True, max_connections: int = 20,
                 cleanup_interval: Optional[int] = 3600,
                 cleanup_config: Optional[Dict[str, int]] = None,
                 kline_missing_timeout_multiplier: float = 2.0):
        """初始化数据库连接，创建元数据表，启用WAL模式

        三阶段初始化（P2-2 初始化阶段化）：
        - Phase1: 纯内存初始化，零副作用
        - Phase2: DB连接+schema迁移+缓存加载，失败通过滤emergency_cleanup回滚
        - Phase3: 线程启动进入运行时，Writer失败回滚，Cleanup可降级
        各阶段独立，失败可回滚到上阶段结束状态。'
        """
        self._init_state = InitStateMachine()
        self._kline_missing_timeout_multiplier = kline_missing_timeout_multiplier

        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            _params = get_cached_params()
            if 'kline_missing_timeout_multiplier' in _params:
                self._kline_missing_timeout_multiplier = _params['kline_missing_timeout_multiplier']
        except Exception:
            pass
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

        self._TICK_SHARD_COUNT = self.DEFAULT_TICK_SHARD_COUNT
        self._TICK_WRITER_COUNT = self.DEFAULT_TICK_WRITER_COUNT
        from ali2026v3_trading.infra.shared_utils import get_shard_router
        self._shard_router = get_shard_router(shard_count=self._TICK_SHARD_COUNT)
        _shard_cap = max(self.DEFAULT_SHARD_CAP_MIN, self.DEFAULT_SHARD_CAP_DIVISOR // self._TICK_SHARD_COUNT)
        self._tick_shard_queues = [queue.Queue(maxsize=_shard_cap) for _ in range(self._TICK_SHARD_COUNT)]
        self._tick_shard_writers: list = []
        self._kline_queue = queue.Queue(maxsize=100000)
        self._kline_writer_thread = None
        self._maintenance_queue = queue.Queue(maxsize=50000)
        # R21-NET-P1-06修复: 消息队列优先级标记 — 当前使用普通Queue无优先级区分
        # 优先级规划: P0=风控/撤单(最高), P1=下单/成交回报, P2=Tick/K线数据, P3=诊断/日志(最低)
        # 后续升级方案: 将queue.Queue替换为PriorityQueue，消息体增加priority字段
        self._queue_priority_levels = {'risk_cancel': 0, 'order': 1, 'tick': 2, 'kline': 2, 'diagnosis': 3}
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
        # P0-R11-20: L0 _db_file_lock  — 跨进程文件锁(intra-process伴生锁), 最先获取, 最外层的写保护
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
        # P0-R11-20: 跨进程文件锁初始化 — 打开 {db_path}.lock 文件并获取OS文件锁fd
        self._db_file_lock_fd = None
        self._db_file_lock = threading.Lock()  # intra-process伴生锁
        if _FILELOCK_AVAILABLE:
            _lock_path = self.db_path + '.lock'
            _lock_fd = None
            try:
                _lock_fd = open(_lock_path, 'w', encoding='utf-8')
                self._db_file_lock_fd = _lock_fd
                _lock_fd = None  # NEW-P1-02修复: 所有权转移至self._db_file_lock_fd，finally不再关闭
                logger.info("P0-R11-20: 跨进程文件锁已初始化 lock_path=%s", _lock_path)
            except Exception as _flock_err:
                logger.warning("P0-R11-20: 无法打开锁文件 %s: %s", self.db_path + '.lock', _flock_err)
                self._db_file_lock_fd = None
            finally:
                # NEW-P1-02修复: 若open()成功但后续异常导致。lock_fd未转移，确保fd被关闭
                if _lock_fd is not None:
                    try:
                        _lock_fd.close()
                    except Exception:
                        pass
        self._runtime_missing_warned = set()
        self._platform_subscribe = None
        self._platform_unsubscribe = None

        self._last_ext_kline: Dict[Tuple[str, str], float] = {}
        self._ext_kline_load_in_progress = False
        self._aggregators: Dict[Tuple[str, str], '_KlineAggregator'] = {}
        self._init_state.advance(InitPhase.MEMORY_ALLOC)

    # ── P0-R11-20: 跨进程文件锁 acquire/release ──────────────────────────
    def _acquire_db_file_lock(self, timeout: Optional[float] = None) -> bool:
        """获取跨进程文件锁（intra-process伴生锁 + OS文件锁）'
        先获取 threading.Lock() 确保进程内串行，
        再通过 msvcrt.locking / fcntl.flock 获取OS级文件锁防止多进程并发写入。'
        P0-R11-20: DuckDB单进程写入限制，多进程并发写会导致数据损坏。
        """
        if timeout is None:
            timeout = self.DB_LOCK_TIMEOUT

        # Step 1: 获取 intra-process 伴生锁（进程内串行化）'
        acquired_internal = self._db_file_lock.acquire(timeout=min(timeout, 5.0))
        if not acquired_internal:
            logger.error("P0-R11-20: 无法获取intra-process伴生锁(timeout=%.1fs), 跳过写入", min(timeout, 5.0))
            return False

        # Step 2: 如果文件锁fd未初始化, 降级为仅intra-process锁
        if self._db_file_lock_fd is None:
            return True

        # Step 3: 获取OS级文件锁（带超时重试）
        deadline = time.time() + timeout
        while True:
            try:
                if sys.platform == 'win32':
                    # Windows: msvcrt.locking — 锁定整个文件
                    _filelock_module.locking(self._db_file_lock_fd.fileno(),
                                             _filelock_module.LK_LOCK, 1)
                else:
                    # Linux/macOS: fcntl.flock — 排他锁, 非阻塞
                    _filelock_module.flock(self._db_file_lock_fd.fileno(),
                                           _filelock_module.LOCK_EX | _filelock_module.LOCK_NB)
                return True
            except (IOError, OSError) as _flock_err:
                remaining = deadline - time.time()
                if remaining <= 0:
                    logger.error(
                        "P0-R11-20: 获取OS文件锁超时(timeout=%.1fs), 跳过写入: %s",
                        timeout, _flock_err,
                    )
                    # 释放 intra-process 锁
                    self._db_file_lock.release()
                    return False
                # 指数退避重试: 10ms, 20ms, 40ms, ... max 500ms
                wait = min(0.01 * (2 ** min(6, int((timeout - remaining) / 0.1))), 0.5)
                time.sleep(wait)

    def _release_db_file_lock(self) -> None:
        """释放跨进程文件锁（OS文件锁 → intra-process伴生锁）'
        必须与 _acquire_db_file_lock 成对调用。'
        """
        if self._db_file_lock_fd is not None:
            try:
                if sys.platform == 'win32':
                    _filelock_module.locking(self._db_file_lock_fd.fileno(),
                                             _filelock_module.LK_UNLCK, 1)
                else:
                    _filelock_module.flock(self._db_file_lock_fd.fileno(),
                                           _filelock_module.LOCK_UN)
            except (IOError, OSError) as _flock_err:
                logger.warning("P0-R11-20: 释放OS文件锁异常(可能已被释放): %s", _flock_err)

        # 释放 intra-process 伴生锁
        try:
            self._db_file_lock.release()
        except RuntimeError:
            # 锁可能已被释放（例如 _acquire_db_file_lock 失败时已释放）'
            pass

    def _close_db_file_lock(self) -> None:
        """关闭锁文件句柄（shutdown时调用）"""
        if self._db_file_lock_fd is not None:
            try:
                self._db_file_lock_fd.close()
            except Exception as _close_err:
                logger.warning("P0-R11-20: 关闭锁文件失败: %s", _close_err)
            finally:
                self._db_file_lock_fd = None
    # ────────────────────────────────────────────────────────────────────

    def _phase2_db_init(self, max_retries, retry_delay, cleanup_interval, cleanup_config):
        """Phase2: DB连接+schema迁移+缓存加载——失败通过滤emergency_cleanup回滚"""
        from ali2026v3_trading.data.data_service import get_data_service
        self._data_service = get_data_service()
        self._init_state.advance(InitPhase.DB_CONNECT)

        self._assigned_ids = set()

        from ali2026v3_trading.config.params_service import get_params_service
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
        # P0-R11-20: 关闭锁文件句柄
        try:
            self._close_db_file_lock()
        except Exception as _lock_close_err:
            logging.warning("[Storage] 紧急清理关闭文件锁失败: %s", _lock_close_err)

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
        instrument_cache = self._params_service.get_all_instrument_cache()
        return {info['internal_id']: info for info in instrument_cache.values() if 'internal_id' in info}

    @property
    def _product_cache(self):
        """代理到ParamsService的product缓存"""
        return self._params_service._product_cache


_StorageInitMixin = StorageInitService
