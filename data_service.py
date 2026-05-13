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
import duckdb
import pyarrow as pa
import pandas as pd
from collections import OrderedDict, deque
import collections
import logging, os, threading, atexit, psutil, hashlib, time, tempfile
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timezone

# 2. 从子模块导入
from ali2026v3_trading.ds_realtime_cache import (
    RealTimeCache, get_realtime_cache,
    _INTRADAY_MODE, _INTRADAY_MAX_TICKS_PER_SYMBOL, _INTRADAY_MAX_TOTAL_TICKS, _INTRADAY_FULL_CAPTURE,
    _HAS_REALTIME_CACHE, _resolve_flush_windows, _resolve_duckdb_file, _resolve_parquet_path,
    _get_data_paths_config, _get_default_data_dir, _parse_flush_windows_str
)
from ali2026v3_trading.ds_db_connection import DBConnectionMixin
from ali2026v3_trading.ds_query_cache import QueryCacheMixin
from ali2026v3_trading.ds_option_sync import OptionSyncMixin
from ali2026v3_trading.ds_data_writer import DataWriterMixin
from ali2026v3_trading.ds_schema_manager import SchemaManagerMixin

# 3. 全局配置变量（保留，因为外部可能直接引用）
logger = logging.getLogger(__name__)

DB_FILE = _resolve_duckdb_file()
PARQUET_PATH = _resolve_parquet_path()

import psutil
_total_mem = psutil.virtual_memory().total
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

    def __init__(self):
        """DataService 初始化 — 仅由 get_data_service() 工厂函数调用"""
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
        self._initialize()

    def _initialize(self):
        """初始化 DataService"""
        logger.info("Initializing DataService (v3.3-facade)...")
        
        # 创建数据目录
        try:
            if DB_FILE:
                os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
            if PARQUET_PATH:
                os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
        except Exception as e:
            logger.warning(f"[DataService] Failed to create data directories: {e}")

        conn = self._get_connection()

        if not self._load_or_create_table():
            self._create_empty_table(conn)

        self._ensure_ticks_raw_schema(conn)
        self._create_indexes_and_views()

        self._create_metadata_tables()

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
        """惰性获取 ParamsService（避免循环依赖）

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
                from ali2026v3_trading.params_service import get_params_service
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
                        timestamp=row.timestamp if hasattr(row, 'timestamp') else datetime.now(),
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


def get_data_service() -> DataService:
    """获取 DataService 单例（唯一入口）

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
