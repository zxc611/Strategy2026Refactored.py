# MODULE_ID: M1-021
"""
data_service.py - 顶级性能数据服务 Facade (v3.3 拆分重构)

模块拆分：
- ds_realtime_cache.py: RealTimeCache缓存管理
- ds_db_connection.py: DuckDB连接池管理 (DBConnectionMixin)
- ds_query_cache.py: 查询缓存+查询接口 (QueryCacheMixin)
- ds_option_sync.py: 期权同步状态计算 (OptionSyncMixin)
- ds_data_writer.py: 数据写入+upsert (DataWriterMixin)
- ds_schema_manager.py: 数据库Schema初始化与迁移 (SchemaManagerMixin)

DataService = DBConnectionMixin + QueryCacheMixin + OptionSyncMixin + DataWriterMixin + SchemaManagerMixin
所有公共API保持100%向后兼容。
"""

# 1. 顶层imports (保留原有)
try:
    from ali2026v3_trading.data.data_access import get_data_access
    _data_access = get_data_access()
    from ali2026v3_trading.data.db_adapter import get_duckdb_module
    duckdb = get_duckdb_module()
except ImportError:
    duckdb = None

try:
    import pyarrow as pa
except ImportError:
    pa = None
import pandas as pd
from collections import OrderedDict, deque
import collections
import logging, os, threading, atexit, hashlib, time, tempfile
try:
    import psutil
except ImportError:
    psutil = None
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timezone

# 2. 从子模块导入
from ali2026v3_trading.data.ds_realtime_cache import (
    RealTimeCache, get_realtime_cache,
    _INTRADAY_MODE, _INTRADAY_MAX_TICKS_PER_SYMBOL, _INTRADAY_MAX_TOTAL_TICKS, _INTRADAY_FULL_CAPTURE,
    _HAS_REALTIME_CACHE, _resolve_flush_windows, _resolve_duckdb_file, _resolve_parquet_path,
    _get_data_paths_config, _get_default_data_dir, _parse_flush_windows_str
)
from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
from ali2026v3_trading.data.ds_query_cache import QueryCacheMixin
from ali2026v3_trading.data.ds_option_sync import OptionSyncMixin
from ali2026v3_trading.data.ds_data_writer import DataWriterMixin
from ali2026v3_trading.data.ds_schema_manager import SchemaManagerMixin
from ali2026v3_trading.infra.shared_utils import CHINA_TZ

__all__ = [
    'DataService', 'get_data_service', 'query', 'get_latest_price',
    'get_time_range', 'get_daily_aggregates', 'get_symbol_daily_ohlc',
    'batch_get_latest_prices', 'explain', 'refresh_data', 'batch_insert_ticks',
    'incremental_load', 'clear_cache',
    'DB_FILE', 'PARQUET_PATH', 'DUCKDB_MAX_MEMORY', 'DUCKDB_THREADS',
    'PREAGGREGATE_DAILY', 'PREAGGREGATE_SYMBOL_DAILY', 'QUERY_CACHE_SIZE',
    'QUERY_CACHE_TTL',
]

# 3. 全局配置变量（保留，因为外部可能直接引用）'
logger = logging.getLogger(__name__)

DB_FILE = _resolve_duckdb_file()
PARQUET_PATH = _resolve_parquet_path()

if psutil is not None:
    _total_mem = psutil.virtual_memory().total
else:
    _total_mem = 8 * (1024 ** 3)
DUCKDB_MAX_MEMORY = os.getenv('DUCKDB_MAX_MEMORY', f'{int(_total_mem * 0.75 / (1024**3))}GB')
DUCKDB_THREADS = int(os.getenv('DUCKDB_THREADS', str(os.cpu_count() or 4)))
PREAGGREGATE_DAILY = os.getenv('PREAGGREGATE_DAILY', 'true').lower() == 'true'
PREAGGREGATE_SYMBOL_DAILY = os.getenv('PREAGGREGATE_SYMBOL_DAILY', 'true').lower() == 'true'
QUERY_CACHE_SIZE = int(os.getenv('QUERY_CACHE_SIZE', '128'))
QUERY_CACHE_TTL = int(os.getenv('QUERY_CACHE_TTL', '60'))


# 4. DataService类 - 组合所有Mixin
class DataService(DBConnectionMixin, QueryCacheMixin, OptionSyncMixin, DataWriterMixin, SchemaManagerMixin):
    """顶级性能数据服务（v3.3 拆分重构Facade）

    组合:
    - DBConnectionMixin: DuckDB连接池管理
    - QueryCacheMixin: 查询缓存+查询接口
    - OptionSyncMixin: 期权同步状态计算
    - DataWriterMixin: 数据写入+upsert
    - SchemaManagerMixin: 数据库Schema初始化与迁移
    """

    _lock = threading.RLock()
    _tick_sync_lock = threading.Lock()
    _thread_local = threading.local()
    _table_initialized = False
    _stop_monitor = threading.Event()
    _subscribe_fn = None
    _unsubscribe_fn = None
    _subscribe_api_bound = False

    def __init__(self):
        """DataService 初始化 — 仅由 get_data_service() 工厂函数调用"""
        super().__init__()  # API-P1-15修复: 调用所有Mixin的指。init__
        self.DB_FILE = DB_FILE
        self.PARQUET_PATH = PARQUET_PATH
        self.DUCKDB_MAX_MEMORY = DUCKDB_MAX_MEMORY
        self.DUCKDB_THREADS = DUCKDB_THREADS
        self.PREAGGREGATE_DAILY = PREAGGREGATE_DAILY
        self.PREAGGREGATE_SYMBOL_DAILY = PREAGGREGATE_SYMBOL_DAILY
        self.QUERY_CACHE_SIZE = QUERY_CACHE_SIZE
        self.QUERY_CACHE_TTL = QUERY_CACHE_TTL
        self._query_cache = OrderedDict()
        self._miss_log_tracker = {}
        self._all_connections = []
        self._all_connections_lock = threading.RLock()
        self._cache_lock = threading.RLock()
        # 历史K线加载支持属性（load_historical_klines_with_stop所需）
        self._ext_kline_lock = threading.Lock()
        self._last_ext_kline = {}
        self._runtime_missing_warned = set()
        self._kline_batch_size = 500
        self._subscription_manager = None
        # FIX: strategy_historical.py使用storage._stop_event.wait()，缺少此属性导致AttributeError
        self._stop_event = threading.Event()
        self._initialize()

    def check_data_source_ready(self) -> Tuple[bool, str]:
        """R23-IN-06-FIX: 数据源就绪检查
        
        检查DuckDB连接状态，确保数据管道可用。
        RealTimeCache为None不视为致命问题（策略可基于DuckDB运行）。
        Returns:
            (is_ready, message) 元组
        """
        try:
            conn = self._get_connection()
            if conn is None:
                logger.warning("[R23-IN-06-FIX] DuckDB连接获取返回None")
                return False, "DuckDB连接未建立"
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                self._return_connection(conn)
        except Exception as e:
            logger.warning("[R23-IN-06-FIX] DuckDB连接异常: %s", e)
            return False, f"DuckDB连接异常: {e}"
        
        if not hasattr(self, 'realtime_cache') or self.realtime_cache is None:
            logger.info("[R23-IN-06-FIX] RealTimeCache未初始化（非致命，策略可基于DuckDB运行）")
        
        return True, "OK"

    def _try_fallback_to_cache_only(self) -> bool:
        """R27-P0-DR-06修复: 主数据源不可用时降级到纯缓存模式
        仅使用RealTimeCache中的最新数据，不依赖DuckDB。
        策略基于缓存中的最后tick运行，精度降低但不中断。'
        """
        if hasattr(self, 'realtime_cache') and self.realtime_cache is not None:
            _cache_size = 0
            try:
                if hasattr(self.realtime_cache, '_cache'):
                    _cache_size = len(self.realtime_cache._cache)
                elif hasattr(self.realtime_cache, 'size'):
                    _cache_size = self.realtime_cache.size()
            except Exception:
                pass
            if _cache_size > 0:
                logger.info("[R27-P0-DR-06] 纯缓存降级: 缓存中有%d条记录", _cache_size)
                return True
        return False

    @classmethod
    def bind_subscribe_api(cls, subscribe_fn, unsubscribe_fn):
        cls._subscribe_fn = subscribe_fn
        cls._unsubscribe_fn = unsubscribe_fn
        cls._subscribe_api_bound = subscribe_fn is not None
        logger.info("[DataService] subscribe API bound: subscribe=%s, unsubscribe=%s", subscribe_fn, unsubscribe_fn)

    def subscribe(self, instrument_id, callback=None):
        _fn = DataService._subscribe_fn
        if _fn is None:
            logger.warning("[DataService] subscribe called but no subscribe_fn bound")
            # FIX-M9-02: 返回 False 而非 None, 使调用方能区分"未绑定"与"成功"
            # 同时计入失败计数器, 避免 success=16288/failed=0 的虚假统计
            DataService._subscribe_fail_count = getattr(DataService, '_subscribe_fail_count', 0) + 1
            return False
        try:
            if callback is not None:
                _fn(instrument_id, callback)
            else:
                _fn(instrument_id)
            # FIX-M9-02: 返回 True 表示订阅调用成功 (不等于平台 ack)
            return True
        except Exception as e:
            # FIX-M9-02: 不再静默吞噬异常返回 None, 而是:
            #   1) 计入失败计数器 (供 subscribe_all_instruments 校验)
            #   2) 重新抛出, 让 _subscribe_single_with_retry 走重试队列
            #   原实现 return None 会让 _do_subscribe 误判为"成功但无返回值",
            #   导致 success_count 被无条件自增, 虚假的成功率掩盖真实失败
            DataService._subscribe_fail_count = getattr(DataService, '_subscribe_fail_count', 0) + 1
            logger.warning("[DataService] subscribe(%s) failed: %s", instrument_id, e)
            raise

    @classmethod
    def get_subscribe_fail_count(cls) -> int:
        """返回 subscribe 失败次数 (FIX-M9-02 新增, 供诊断使用)"""
        return getattr(cls, '_subscribe_fail_count', 0)

    @classmethod
    def reset_subscribe_fail_count(cls) -> None:
        """重置失败计数器 (FIX-M9-02 新增)"""
        cls._subscribe_fail_count = 0

    def request_realtime(self, instrument_id, fields=None):
        _fn = DataService._subscribe_fn
        if _fn is None:
            logger.warning("[DataService] request_realtime called but no subscribe_fn bound")
            return False
        try:
            return _fn(instrument_id)
        except Exception as e:
            logger.warning("[DataService] request_realtime(%s) failed: %s", instrument_id, e)
            return False

    @property
    def subscription_manager(self):
        _sm = self.__dict__.get('_subscription_manager')
        if _sm is not None:
            return _sm
        with self._cache_lock:
            _sm = self.__dict__.get('_subscription_manager')
            if _sm is not None:
                return _sm
            try:
                from ali2026v3_trading.infra.subscription_service import SubscriptionManager
                _sm = SubscriptionManager(data_manager=self)
                self._subscription_manager = _sm
                logger.info("[DataService] SubscriptionManager lazy-created (data_manager=self)")
            except Exception as e:
                logger.warning("[DataService] Failed to create SubscriptionManager: %s", e)
                _sm = None
            return _sm

    def bind_data_manager(self, data_manager):
        _sm = self.__dict__.get('_subscription_manager')
        if _sm is None:
            return None
        bind_method = getattr(_sm, 'bind_data_manager', None)
        if callable(bind_method):
            bind_method(data_manager)
        else:
            _sm.data_manager = data_manager
            if hasattr(_sm, '_core_service'):
                _sm._core_service.data_manager = data_manager
        logger.info("[DataService] SubscriptionManager data_manager rebound")
        return _sm

    @classmethod
    def is_subscribe_api_bound(cls) -> bool:
        return bool(cls._subscribe_api_bound and cls._subscribe_fn is not None)

    def ensure_registered_instruments(self, instrument_ids):
        registered = 0
        skipped = 0
        errors = 0
        for inst_id in instrument_ids:
            try:
                result = self.register_instrument(inst_id)
                if result:
                    registered += 1
                else:
                    skipped += 1
            except Exception:
                errors += 1
        return {'registered': registered, 'skipped': skipped, 'errors': errors, 'total': len(instrument_ids), 'configured_count': len(instrument_ids), 'new_count': registered, 'missing_count': 0, 'registered_count': registered, 'created_count': registered, 'failed_count': errors}

    def get_registered_instrument_ids(self):
        try:
            conn = self._get_connection()
            if conn is None:
                return []
            try:
                rows = conn.execute("SELECT instrument_id FROM instruments_registry").fetchall()
                return [r[0] for r in rows]
            finally:
                self._return_connection(conn)
        except Exception:
            return []

    def register_instrument(self, instrument_id, **kwargs):
        try:
            conn = self._get_connection()
            if conn is None:
                return False
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS instruments_registry (
                        instrument_id VARCHAR PRIMARY KEY,
                        product VARCHAR,
                        exchange VARCHAR,
                        year_month VARCHAR,
                        internal_id INTEGER,
                        option_type VARCHAR,
                        strike_price DOUBLE,
                        underlying_future_id INTEGER,
                        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                existing = conn.execute(
                    "SELECT instrument_id FROM instruments_registry WHERE instrument_id = ?",
                    [instrument_id]
                ).fetchone()
                if existing:
                    return True
                from ali2026v3_trading.infra.subscription_service import SubscriptionManager
                is_opt = SubscriptionManager.is_option(instrument_id)
                if is_opt:
                    parsed = SubscriptionManager.parse_option(instrument_id)
                    internal_id = kwargs.get('internal_id')
                    underlying_future_id = kwargs.get('underlying_future_id')
                    if internal_id is None or underlying_future_id is None:
                        try:
                            oi_row = conn.execute(
                                "SELECT internal_id, underlying_future_id FROM option_instruments WHERE instrument_id = ?",
                                [instrument_id]
                            ).fetchone()
                            if oi_row:
                                if internal_id is None:
                                    internal_id = oi_row[0]
                                if underlying_future_id is None:
                                    underlying_future_id = oi_row[1]
                        except Exception:
                            pass
                    conn.execute(
                        "INSERT INTO instruments_registry (instrument_id, product, exchange, year_month, internal_id, option_type, strike_price, underlying_future_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [instrument_id, parsed.get('product', ''), kwargs.get('exchange', ''), parsed.get('year_month', ''), internal_id, parsed.get('option_type', ''), parsed.get('strike_price', 0.0), underlying_future_id]
                    )
                else:
                    parsed = SubscriptionManager.parse_future(instrument_id)
                    internal_id = kwargs.get('internal_id')
                    if internal_id is None:
                        try:
                            fi_row = conn.execute(
                                "SELECT internal_id FROM futures_instruments WHERE instrument_id = ?",
                                [instrument_id]
                            ).fetchone()
                            if fi_row:
                                internal_id = fi_row[0]
                        except Exception:
                            pass
                    conn.execute(
                        "INSERT INTO instruments_registry (instrument_id, product, exchange, year_month, internal_id) VALUES (?, ?, ?, ?, ?)",
                        [instrument_id, parsed.get('product', ''), kwargs.get('exchange', ''), parsed.get('year_month', ''), internal_id]
                    )
                return True
            finally:
                self._return_connection(conn)
        except Exception as e:
            logger.debug("[DataService] register_instrument(%s) failed: %s", instrument_id, e)
            return False

    def _get_instrument_info(self, instrument_id: str):
        try:
            conn = self._get_connection()
            if conn is None:
                return None
            try:
                try:
                    row = conn.execute(
                        "SELECT instrument_id, product, exchange, year_month, internal_id, option_type, strike_price, underlying_future_id FROM instruments_registry WHERE instrument_id = ?",
                        [instrument_id]
                    ).fetchone()
                    if row and row[4] is not None:
                        info = {
                            'instrument_id': row[0],
                            'product': row[1],
                            'exchange': row[2],
                            'year_month': row[3],
                            'internal_id': row[4],
                        }
                        if row[5]:
                            info['type'] = 'option'
                            info['option_type'] = row[5]
                            info['strike_price'] = row[6]
                            info['underlying_future_id'] = row[7]
                        else:
                            info['type'] = 'future'
                        return info
                except Exception:
                    pass
                try:
                    row = conn.execute(
                        "SELECT internal_id, instrument_id, product, exchange, year_month FROM futures_instruments WHERE instrument_id = ?",
                        [instrument_id]
                    ).fetchone()
                    if row:
                        return {
                            'internal_id': row[0],
                            'instrument_id': row[1],
                            'type': 'future',
                            'product': row[2],
                            'exchange': row[3],
                            'year_month': row[4],
                            'product_code': (row[2] or '').lower(),
                            'shard_key': row[0],
                        }
                except Exception:
                    pass
                try:
                    row = conn.execute(
                        "SELECT internal_id, instrument_id, product, exchange, underlying_future_id, underlying_product, year_month, option_type, strike_price FROM option_instruments WHERE instrument_id = ?",
                        [instrument_id]
                    ).fetchone()
                    if row:
                        return {
                            'internal_id': row[0],
                            'instrument_id': row[1],
                            'type': 'option',
                            'product': row[2],
                            'exchange': row[3],
                            'underlying_future_id': row[4],
                            'underlying_product': row[5],
                            'year_month': row[6],
                            'option_type': row[7],
                            'strike_price': row[8],
                            'product_code': (row[2] or '').lower(),
                            'shard_key': row[0],
                        }
                except Exception:
                    pass
                return None
            finally:
                self._return_connection(conn)
        except Exception as e:
            logger.debug("[DataService] _get_instrument_info(%s) failed: %s", instrument_id, e)
            return None


    def bind_data_manager(self, data_manager):
        """绑定外部DataManager到SubscriptionManager（当data_manager不是DataService自身时使用）"""
        _sm = self.__dict__.get('_subscription_manager')
        if _sm is None:
            return None
        bind_method = getattr(_sm, 'bind_data_manager', None)
        if callable(bind_method):
            bind_method(data_manager)
        else:
            _sm.data_manager = data_manager
            if hasattr(_sm, '_core_service'):
                _sm._core_service.data_manager = data_manager
        logger.info("[DataService] SubscriptionManager data_manager rebound")
        return _sm

    def process_tick(self, tick_data: dict) -> int:
        if not tick_data or not isinstance(tick_data, dict):
            return 0
        try:
            return self.batch_insert_ticks([tick_data], use_arrow=True)
        except Exception as e:
            logger.debug("[DataService.process_tick] batch_insert_ticks failed: %s", e)
            return 0

    def _initialize(self):
        logger.info("Initializing DataService (v3.3-facade)... DB_FILE=%s", self.DB_FILE)

        # FIX-20260702-HARDEN: 嵌入式环境自动检测 + 安全模式日志
        try:
            from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
            if DBConnectionMixin._EMBEDDED_MODE:
                logger.info(
                    "[FIX-20260702-HARDEN] PythonGO 嵌入式环境已检测！自动启用安全模式: "
                    "单连接 + 同步执行 + threads=1 "
                    "(消除 DuckDB 内部 C++ 线程池与宿主进程的冲突)"
                )
            elif not DBConnectionMixin._POOL_DISABLED:
                logger.info(
                    "[FIX-20260702] DuckDB连接池模式(多连接+ThreadPoolExecutor)已启用。"
                    "实盘PythonGO嵌入式环境如遇 _duckdb.cp312 原生崩溃(0xc0000409)，"
                    "请设置环境变量 DUCKDB_POOL_DISABLED=1 切换到安全模式。"
                )
            else:
                logger.info("[FIX-20260702] DuckDB单连接同步执行模式已启用(DUCKDB_POOL_DISABLED=1)")
        except (ImportError, AttributeError):
            pass

        try:
            if DB_FILE:
                os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
            if PARQUET_PATH:
                os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
        except Exception as e:
            logger.warning(f"[DataService] Failed to create data directories: {e}")

        conn = self._get_connection()
        try:
            # FIX-20260702: 单连接模式下避免重复获取连接导致 _in_use 状态混乱，
            # 统一使用同一个连接贯穿初始化流程。
            if not self._load_or_create_table(conn):
                self._create_empty_table(conn)

            self._ensure_ticks_raw_schema(conn)
            self._create_indexes_and_views(conn)

            self._create_metadata_tables(conn)
        finally:
            self._return_connection(conn)

        self._params_service = None
        self._products_loaded = False

        # 初始化RealTimeCache
        if _HAS_REALTIME_CACHE and get_realtime_cache:
            try:
                self.realtime_cache = get_realtime_cache(max_recent_ticks=100)
                self._preload_realtime_cache()
                logger.info("[DataService] RealTimeCache integration enabled and preloaded.")
            except Exception as e:
                logger.warning(f"[DataService] Failed to initialize RealTimeCache: {e}")
                self.realtime_cache = None
        else:
            self.realtime_cache = None
            logger.warning("[DataService] RealTimeCache not available")

        # 启动性能监控
        self._start_performance_monitor()

        # 注册atexit
        atexit.register(self.close_all)
        logger.info("DataService ready.")

    @property
    def params_service(self):
        """惰性获取 ParamsService（避免循环依赖）'
        注意：必须在 ensure_products_with_retry 之后访问，否则缓存可能为空
        """
        if self._params_service is None:
            if not self._products_loaded:
                logger.warning(
                    "[DataService] params_service 在 ensure_products_with_retry 之前被访问！"
                    "这可能导致 ParamsService 缓存为空。"
                    "请确保先调用 mark_products_loaded()。"
                )

            try:
                from ali2026v3_trading.config.params_service import get_params_service
                self._params_service = get_params_service()
                logger.info(f"[DataService] ParamsService 惰性加载成功, id={id(self._params_service)}")
                if (
                    self.realtime_cache
                    and self._products_loaded
                    and getattr(self.realtime_cache, '_params_service', None) is None
                ):
                    self.realtime_cache.set_params_service(self._params_service)
            except Exception as e:
                logger.warning(f"[DataService] Failed to get ParamsService: {e}")
        return self._params_service

    def mark_products_loaded(self):
        """标记品种配置已加载完成（由 StrategyCoreService.on_init() 调用）"""
        self._products_loaded = True
        logger.info("[DataService] 品种配置已加载，params_service 现在可以安全访问")
        if self.realtime_cache and getattr(self.realtime_cache, '_params_service', None) is None:
            _ = self.params_service

    def _preload_realtime_cache(self):
        """从持久化存储预加载最新价格，避免启动初期的降级查询开销"""
        if not self.realtime_cache:
            return
        try:
            result = self.query("SELECT instrument_id, last_price, timestamp FROM latest_prices", arrow=False)
            result_list = list(result.itertuples()) if hasattr(result, 'itertuples') else (list(result) if hasattr(result, '__iter__') else [])
            row_count = len(result_list)

            if row_count > 0:
                for row in result_list:
                    self.realtime_cache.update_tick(
                        symbol=row.instrument_id,
                        price=float(row.last_price),
                        timestamp=row.timestamp if hasattr(row, 'timestamp') else datetime.now(CHINA_TZ),
                        volume=0,
                        bid_price=0.0,
                        ask_price=0.0
                    )
                logger.info(f"[DataService] Preloaded {row_count} prices into RealTimeCache.")
        except Exception as e:
            logger.warning(f"[DataService] Failed to preload RealTimeCache: {e}")


# ============================================================================
# 全局单例与便捷函数
# ============================================================================

_data_service_instance: Optional[DataService] = None
_data_service_lock = threading.Lock()


def reset_data_service() -> None:
    """R27-CP-03-FIX: 重置DataService单例，用于测试隔离"""
    global _data_service_instance
    with _data_service_lock:
        _data_service_instance = None


def get_data_service() -> DataService:
    """获取 DataService 单例（唯一入口）'
    设计原则：
    - 工厂函数是获取 DataService 实例的唯一合法途径
    - 初始化成功后才存储实例，失败则不留下半成品
    - 双重检查锁定保证线程安全
    """
    global _data_service_instance
    if _data_service_instance is not None:
        return _data_service_instance
    with _data_service_lock:
        if _data_service_instance is not None:
            return _data_service_instance
        instance = DataService()
        _data_service_instance = instance
        return _data_service_instance


def get_existing_data_service() -> Optional[DataService]:
    """Return the existing DataService singleton without creating it."""
    return _data_service_instance


def query(sql: str, params: Optional[List] = None) -> pa.Table:
    """便捷函数：执行SQL查询（统一入口，通过get_data_service()）"""
    return get_data_service().query(sql, params)

def get_latest_price(symbol: str) -> Optional[float]:
    """便捷函数：获取最新价格（统一入口，通过get_data_service()）"""
    return get_data_service().get_latest_price(symbol)

def get_time_range(instrument_id: str, start: datetime, end: datetime) -> pa.Table:
    return get_data_service().get_time_range(instrument_id, start, end)

def get_daily_aggregates(start_date: date, end_date: date) -> pa.Table:
    return get_data_service().get_daily_aggregates(start_date, end_date)

def get_symbol_daily_ohlc(symbol: str, start_date: date, end_date: date) -> pa.Table:
    return get_data_service().get_symbol_daily_ohlc(symbol, start_date, end_date)

def batch_get_latest_prices(symbols: List[str]) -> pa.Table:
    return get_data_service().batch_get_latest_prices(symbols)

def explain(sql: str) -> str:
    return get_data_service().explain(sql)

def refresh_data() -> bool:
    return get_data_service().refresh_data()

def batch_insert_ticks(ticks_data: List[Dict[str, Any]], use_arrow: bool = True) -> int:
    """批量插入 Tick 数据（便捷函数）"""
    return get_data_service().batch_insert_ticks(ticks_data, use_arrow)

def incremental_load(new_parquet_path: str) -> Dict[str, Any]:
    """增量加载新数据（便捷函数）"""
    return get_data_service().incremental_load(new_parquet_path)

def clear_cache():
    get_data_service().clear_cache()


# ============================================================================
# 示例
# ============================================================================
if __name__ == '__main__':
    print(explain("SELECT * FROM ticks_raw LIMIT 10"))
    price = get_latest_price('IF2605')
    print(f"Latest price: {price}")
