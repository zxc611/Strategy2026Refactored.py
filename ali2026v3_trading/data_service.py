"""
data_service.py - 顶级性能数据服务（最终修复版 v3.2）

核心特性：
- 持久化 DuckDB 数据库（重启无需重载）
- 列式存储 + 向量化执行
- 显式索引（instrument_id, timestamp）
- 预聚合视图（每日全局、每合约 OHLC）
- 最新价格视图（窗口函数实现）
- Arrow 零拷贝返回
- 自适应内存/并行度
- 线程安全连接（每线程独立连接）
- 查询缓存（LRU + TTL，参数规范化）
- 明确的时区处理：表中 timestamp 必须为 UTC naive，
  查询时自动将带时区的 datetime 转换为 UTC naive 并发出警告。

重要：表中 timestamp 列必须存储 UTC 时间（无时区）。
"""

import duckdb
import pyarrow as pa
import pandas as pd
from collections import OrderedDict
import logging, os, threading, atexit, psutil, hashlib, time, tempfile
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timezone

# ✅ 新增：导入实时缓存
try:
    # ✅ 传递渠道唯一：导入工厂函数，禁止直接实例化RealTimeCache
    from ali2026v3_trading.realtime_cache import get_realtime_cache
    _HAS_REALTIME_CACHE = True
except ImportError as e:
    logging.warning(f"[DataService] Failed to import RealTimeCache: {e}")
    _HAS_REALTIME_CACHE = False
    get_realtime_cache = None

logger = logging.getLogger(__name__)

# ============================================================================
# 环境配置
# ============================================================================

# P1 修复：强制使用磁盘持久化文件，消除 :memory: 模式下的全量数据双份拷贝
# P1 DuckDB核查报告修复：数据库路径迁移到D盘，避免C盘Windows权限冲突和防病毒扫描
DB_FILE = os.getenv('DUCKDB_FILE', 'D:\\ali2026_data\\ticks.duckdb')
PARQUET_PATH = os.getenv('TICK_DATA_PATH', 'D:\\ali2026_data\\ticks.parquet')
# P1 修复：严格限制内存上限，禁止自动计算，确保资源占用可控
DUCKDB_MAX_MEMORY = os.getenv('DUCKDB_MAX_MEMORY', '4GB')   
DUCKDB_THREADS = int(os.getenv('DUCKDB_THREADS', str(os.cpu_count() or 4)))
PREAGGREGATE_DAILY = os.getenv('PREAGGREGATE_DAILY', 'true').lower() == 'true'
PREAGGREGATE_SYMBOL_DAILY = os.getenv('PREAGGREGATE_SYMBOL_DAILY', 'true').lower() == 'true'
QUERY_CACHE_SIZE = int(os.getenv('QUERY_CACHE_SIZE', '128'))
QUERY_CACHE_TTL = int(os.getenv('QUERY_CACHE_TTL', '60'))


class DataService:
    """单例数据服务 - 顶级性能版（线程安全）"""

    _instance: Optional['DataService'] = None
    _lock = threading.RLock()
    _tick_sync_lock = threading.Lock()
    _thread_local = threading.local()
    _table_initialized = False
    _stop_monitor = threading.Event()

    def __new__(cls):
        # P0 Bug #7修复：双重检查锁完整实现，外层也加锁保护
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        logger.info("Initializing DataService (v3.2)...")
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)

        # P0 Bug #8修复：将类变量改为实例变量，避免共享可变对象
        self._query_cache = OrderedDict()
        self._miss_log_tracker = {}
        self._all_connections = []  # P1 Bug #41修复：跟踪所有连接
        self._cache_lock = threading.RLock()

        conn = self._get_connection()
        # Note: _get_connection already calls _configure_connection, no need to call again

        if not self._load_or_create_table():
            self._create_empty_table(conn)

        self._ensure_ticks_raw_schema(conn)

        self._create_indexes_and_views()
        
        # ✅ 关键修复：在 DataService 初始化时创建元数据表（解决品种配置加载依赖问题）
        # 这样 ensure_products_with_retry 可以在 Storage 初始化之前工作
        self._create_metadata_tables()
        
        # ✅ 循环依赖修复：延迟初始化 ParamsService，避免与 RealTimeCache 形成循环
        # ⚠️  重要：ParamsService 必须在 ensure_products_with_retry 之后才能访问
        # 否则会导致缓存加载时品种数据尚未写入数据库
        self._params_service = None
        self._products_loaded = False  # 标记品种配置是否已加载
        
        # 初始化 realtime_cache
        # ✅ 传递渠道唯一：使用get_realtime_cache()工厂函数获取单例
        if _HAS_REALTIME_CACHE and get_realtime_cache:
            try:
                self.realtime_cache = get_realtime_cache(max_recent_ticks=100)
                # ✅ 循环依赖修复：不立即设置 params_service，改为惰性设置
                # ParamsService 将在首次使用时通过属性访问器获取
                self._preload_realtime_cache()
                logger.info("[DataService] RealTimeCache integration enabled and preloaded.")
            except Exception as e:
                logger.warning(f"[DataService] Failed to initialize RealTimeCache: {e}")
                self.realtime_cache = None
        else:
            self.realtime_cache = None
            logger.warning("[DataService] RealTimeCache not available")
        
        atexit.register(self.close_all)
        self._start_performance_monitor()
        logger.info("DataService ready.")

    @property
    def params_service(self):
        """
        惰性获取 ParamsService（避免循环依赖）
        
        ⚠️  重要：必须在 ensure_products_with_retry 之后访问，否则缓存可能为空
        
        Returns:
            ParamsService or None: ParamsService 单例，如果获取失败则返回 None
        """
        if self._params_service is None:
            # ✅ 检查品种配置是否已加载
            if not self._products_loaded:
                logger.warning(
                    "[DataService] ⚠️  params_service 在 ensure_products_with_retry 之前被访问！"
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
        """
        标记品种配置已加载完成（由 StrategyCoreService.on_init() 调用）
        
        这会允许后续安全地访问 params_service 属性。
        """
        self._products_loaded = True
        logger.info("[DataService] ✅ 品种配置已加载，params_service 现在可以安全访问")
        if self.realtime_cache and getattr(self.realtime_cache, '_params_service', None) is None:
            _ = self.params_service

    def _configure_connection(self, conn: duckdb.DuckDBPyConnection):
        """配置 DuckDB 连接：内存、并行度、压缩（P1 修复：强化磁盘优先策略）"""
        mem_limit = self._get_duckdb_memory_limit()
        try:
            conn.execute(f"SET max_memory = '{mem_limit}'")
        except Exception as e:
            logger.warning(f"Failed to set max_memory: {e}")
        try:
            conn.execute(f"SET threads = {DUCKDB_THREADS}")
        except Exception as e:
            logger.warning(f"Failed to set threads: {e}")
        temp_dir = os.path.join(tempfile.gettempdir(), 'duckdb_tmp')
        os.makedirs(temp_dir, exist_ok=True)
        try:
            conn.execute(f"SET temp_directory = '{temp_dir}'")
        except Exception as e:
            # DuckDB does not allow switching temp_directory after it has been used
            logger.warning(f"Failed to set temp_directory (may already be set): {e}")
        
        # P1 Bug #55修复：设置临时目录大小限制，避免OOM错误
        try:
            conn.execute("SET max_temp_directory_size='50GB'")
        except Exception as e:
            logger.warning(f"Failed to set max_temp_directory_size: {e}")
        
        # P1 修复：启用字典压缩与 ZSTD，提升磁盘 I/O 效率
        try: conn.execute("SET enable_dictionary_compression=true")
        except Exception: pass
        try:
            conn.execute("SET preserve_insertion_order=false")
        except Exception: pass
        try: conn.execute("SET zstd_compression_level=3")
        except Exception: pass

    def _preload_realtime_cache(self):
        """P1 修复：从持久化存储预加载最新价格，避免启动初期的降级查询开销"""
        if not self.realtime_cache:
            return
        try:
            result = self.query("SELECT instrument_id, last_price, timestamp FROM latest_prices", arrow=False)
            # P2 Bug #104修复：正确处理DataFrame/Arrow结果
            result_list = list(result.itertuples()) if hasattr(result, 'itertuples') else (list(result) if hasattr(result, '__iter__') else [])
            row_count = len(result_list)
            
            if row_count > 0:
                for row in result_list:
                    # P1 修复：统一使用与 on_tick 一致的 6 参数签名
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

    def _get_duckdb_memory_limit(self) -> str:
        """P1 修复：返回静态配置的内存限制，杜绝动态计算的不确定性"""
        return DUCKDB_MAX_MEMORY if DUCKDB_MAX_MEMORY else '4GB'

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        if not hasattr(self._thread_local, 'conn'):
            conn = duckdb.connect(DB_FILE)
            self._configure_connection(conn)
            self._thread_local.conn = conn
            self._thread_local.conn_healthy = True
            # P1 Bug #40修复：跟踪所有连接，便于优雅关闭
            self._all_connections.append(conn)
        elif not getattr(self._thread_local, 'conn_healthy', True):
            logger.warning("[DataService] Connection marked unhealthy, recreating...")
            try:
                old_conn = self._thread_local.conn
                if old_conn in self._all_connections:
                    self._all_connections.remove(old_conn)
                old_conn.close()
            except Exception:
                pass
            conn = duckdb.connect(DB_FILE)
            self._configure_connection(conn)
            self._thread_local.conn = conn
            self._thread_local.conn_healthy = True
            self._all_connections.append(conn)
            logger.info("[DataService] Connection recovered successfully")
        return self._thread_local.conn

    def _mark_connection_unhealthy(self):
        """标记当前线程连接为不健康（P1 FATAL error 恢复机制）"""
        self._thread_local.conn_healthy = False

    @staticmethod
    def _is_fatal_database_error(e: Exception) -> bool:
        """检测是否为 DuckDB FATAL 数据库错误（导致连接不可恢复的致命错误）"""
        error_str = str(e)
        return 'FATAL' in error_str or 'database has been invalidated' in error_str

    def _load_or_create_table(self) -> bool:
        """加载 Parquet 数据到表。返回 True 表示成功加载/已存在，False 表示文件不存在需创建空表。"""
        conn = self._get_connection()
        # 检查表是否已存在（持久化模式）
        res = conn.execute("SELECT table_name FROM duckdb_tables WHERE table_name='ticks_raw'").fetchone()
        if res:
            logger.info("Table 'ticks_raw' already exists.")
            self._table_initialized = True
            return True

        if not os.path.exists(PARQUET_PATH):
            logger.warning(f"Parquet file not found: {PARQUET_PATH}")
            return False

        logger.info(f"Loading Parquet from {PARQUET_PATH}...")
        # 注意：假设 Parquet 文件中列名与目标表一致，且 timestamp 为 UTC naive
        try:
            conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE temp_ticks AS
                SELECT *, CAST(timestamp AS DATE) AS date
                FROM read_parquet('{PARQUET_PATH}', union_by_name=false)
            """)
        except Exception as e:
            logger.error(f"Failed to read Parquet: {e}. Please ensure column names match.")
            raise

        # 按时间排序创建最终表（有助于范围查询）
        conn.execute("""
            CREATE TABLE ticks_raw AS
            SELECT * FROM temp_ticks
            ORDER BY timestamp, instrument_id
        """)
        conn.execute("DROP TABLE temp_ticks")
        self._table_initialized = True
        logger.info("Table created and loaded.")
        return True

    def _create_empty_table(self, conn):
        conn.execute("""
            CREATE TABLE ticks_raw (
                timestamp TIMESTAMP,
                instrument_id VARCHAR,
                last_price DOUBLE,
                volume BIGINT,
                open_interest DOUBLE,
                bid_price DOUBLE,
                ask_price DOUBLE,
                date DATE,
                -- P2 优化：预存期权关键信息，避免运行时重复解析与类型转换
                option_type VARCHAR,               -- 期权类型 (CALL/PUT)
                strike_price DOUBLE,               -- 行权价 (如 3850.0)
                -- ✅ 新增：期权状态计算列（2026-04-03）
                is_otm BOOLEAN,              -- 是否虚值（仅期权）
                sync_status VARCHAR,         -- 五类状态：'correct_rise'/'wrong_rise'/'correct_fall'/'wrong_fall'/'other'
                -- ✅ 新增：期货同步状态计算列（2026-04-03）
                future_sync_status VARCHAR,  -- 期货同步状态：'same_rise'/'same_fall'/'diff'
                is_same_rise BOOLEAN,        -- 同涨（与主力合约相比）
                is_same_fall BOOLEAN,        -- 同跌（与主力合约相比）
                is_diff_sync BOOLEAN         -- 不同步（与主力合约相比）
            )
        """)
        logger.info("Empty table created with option and future status columns.")

        conn.execute("""
            CREATE TABLE future_products (
                product VARCHAR PRIMARY KEY,
                exchange VARCHAR,
                format_template VARCHAR,
                tick_size DOUBLE,
                contract_size DOUBLE,
                is_active BOOLEAN
            )
        """)

        conn.execute("""
            CREATE TABLE option_products (
                product VARCHAR PRIMARY KEY,
                exchange VARCHAR,
                underlying_product VARCHAR,
                format_template VARCHAR,
                tick_size DOUBLE,
                contract_size DOUBLE,
                is_active BOOLEAN
            )
        """)

        conn.execute("""
            CREATE TABLE futures_instruments (
                internal_id BIGINT PRIMARY KEY,
                instrument_id VARCHAR UNIQUE,
                product VARCHAR,
                exchange VARCHAR,
                year_month VARCHAR,
                is_active BOOLEAN
            )
        """)

        conn.execute("""
            CREATE TABLE option_instruments (
                internal_id BIGINT PRIMARY KEY,
                instrument_id VARCHAR UNIQUE,
                product VARCHAR,
                exchange VARCHAR,
                underlying_future_id INTEGER,  -- ✅ ID 直通：引用 futures_instruments.internal_id
                underlying_product VARCHAR,
                year_month VARCHAR,
                option_type VARCHAR,
                strike_price DOUBLE,
                is_active BOOLEAN
            )
        """)

        logger.info("Created product and instrument tables.")

    def _create_metadata_tables(self):
        """
        创建元数据表（future_products, option_products, futures_instruments, option_instruments）
        
        ✅ P2 Bug #103修复：使用CREATE TABLE IF NOT EXISTS，避免每次DROP所有表导致数据丢失
        
        目的：解决品种配置加载依赖问题 - ensure_products_with_retry 需要在 Storage 初始化之前工作。
        """
        import os
        
        conn = self._get_connection()
        
        # P2 Bug #103修复：不再DROP表，改为IF NOT EXISTS创建，保护已有数据
        logger.info("[DataService] 检查并创建元数据表（如不存在）")
        
        # ✅ 创建全局统一的 instrument_id 序列（ID直通原则：全局唯一标识）
        conn.execute("CREATE SEQUENCE IF NOT EXISTS instrument_id_seq START 1")
        
        # 期货品种元数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS future_products (
                product VARCHAR PRIMARY KEY,
                exchange VARCHAR,
                format_template VARCHAR,
                tick_size DOUBLE,
                contract_size DOUBLE,
                is_active BOOLEAN
            )
        """)
    
        # 期权品种元数据表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS option_products (
                product VARCHAR PRIMARY KEY,
                exchange VARCHAR,
                underlying_product VARCHAR,
                format_template VARCHAR,
                tick_size DOUBLE,
                contract_size DOUBLE,
                is_active BOOLEAN
            )
        """)
        
        # 期货合约表（internal_id 由全局序列统一分配）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS futures_instruments (
                internal_id BIGINT PRIMARY KEY,
                instrument_id VARCHAR UNIQUE,
                product VARCHAR,
                exchange VARCHAR,
                year_month VARCHAR,
                is_active BOOLEAN,
                format VARCHAR,
                expire_date VARCHAR,
                listing_date VARCHAR,
                kline_table VARCHAR,
                tick_table VARCHAR
            )
        """)
        
        # 期权合约表（遵循 ID 直通原则：使用 underlying_future_id 引用期货合约，internal_id 由全局序列统一分配）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS option_instruments (
                internal_id BIGINT PRIMARY KEY,
                instrument_id VARCHAR UNIQUE,
                product VARCHAR,
                exchange VARCHAR,
                underlying_future_id INTEGER,
                underlying_product VARCHAR,
                year_month VARCHAR,
                option_type VARCHAR,
                strike_price DOUBLE,
                is_active BOOLEAN,
                format VARCHAR,
                expire_date VARCHAR,
                listing_date VARCHAR,
                kline_table VARCHAR,
                tick_table VARCHAR
            )
        """)
        
        # 为已有数据库补齐新增列（format, expire_date, listing_date, kline_table, tick_table）
        for table_name in ('futures_instruments', 'option_instruments'):
            existing_cols = {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
            for col_name, col_type in [('format', 'VARCHAR'), ('expire_date', 'VARCHAR'),
                                        ('listing_date', 'VARCHAR'), ('kline_table', 'VARCHAR'),
                                        ('tick_table', 'VARCHAR')]:
                if col_name not in existing_cols:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
                    logger.info("[DataService] Added column %s to %s", col_name, table_name)

        logger.info("[DataService] 元数据表检查/创建完成")
        
        # ✅ 创建 K 线数据表（与 Storage._save_kline_impl 传入的字段匹配）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS klines_raw (
                internal_id BIGINT,                -- 合约内部 ID（与元数据表一致）
                instrument_type VARCHAR,             -- 合约类型: 'future' 或 'option'
                timestamp TIMESTAMP,                 -- K 线时间戳
                open DOUBLE,                         -- 开盘价
                high DOUBLE,                         -- 最高价
                low DOUBLE,                          -- 最低价
                close DOUBLE,                        -- 收盘价
                volume BIGINT,                       -- 成交量
                open_interest DOUBLE,                -- 持仓量
                trade_date DATE                      -- 交易日期
            )
        """)
        logger.info("K-line table created/verified: klines_raw")
        
        logger.info("[DataService] ✅ 元数据表已创建/验证完成")

    def _ensure_ticks_raw_schema(self, conn: duckdb.DuckDBPyConnection):
        """兼容旧版 DuckDB：为 ticks_raw 自动补齐新版本依赖的列。"""
        existing_columns = {
            row[0]  # ID pass-through
            for row in conn.execute("DESCRIBE ticks_raw").fetchall()
        }
        required_columns = {
            'option_type': 'VARCHAR',
            'strike_price': 'DOUBLE',
            'is_otm': 'BOOLEAN',
            'sync_status': 'VARCHAR',
            'future_sync_status': 'VARCHAR',
            'is_same_rise': 'BOOLEAN',
            'is_same_fall': 'BOOLEAN',
            'is_diff_sync': 'BOOLEAN',
        }

        added_columns = []
        for column_name, column_type in required_columns.items():
            if column_name in existing_columns:
                continue
            conn.execute(f"ALTER TABLE ticks_raw ADD COLUMN {column_name} {column_type}")
            added_columns.append(column_name)

        if added_columns:
            logger.warning(
                "[DataService] Migrated legacy ticks_raw schema, added columns: %s",
                ', '.join(added_columns),
            )

        self._backfill_option_metadata(conn)

    def _backfill_option_metadata(self, conn: duckdb.DuckDBPyConnection):
        """为旧库中的期权 Tick 回填 option_type/strike_price。
        
        修复标准：统一委托SubscriptionManager.parse_option，删除内联正则
        注意：由于SQL中无法直接调用Python函数，这里保留正则但标注为与parse_option语义同步
        """
        # 注意：DuckDB SQL正则必须与 SubscriptionManager.parse_option() 保持一致
        # parse_option支持两种格式：连字符(SHFE2401-C-5000)和紧凑(SHFE2401C5000)
        conn.execute("""
            UPDATE ticks_raw
            SET
                option_type = COALESCE(
                    option_type,
                    CASE
                        -- 连字符格式：SHFE2401-C-5000
                        WHEN regexp_matches(instrument_id, '^[A-Za-z]+\\d{4}-[CP]-\\d+(?:\\.\\d+)?$')
                            THEN UPPER(regexp_extract(instrument_id, '^[A-Za-z]+\\d{4}-([CP])-', 1))
                        -- 紧凑格式：SHFE2401C5000
                        WHEN regexp_matches(instrument_id, '^[A-Za-z]+\\d{3,4}[CP]\\d+$')
                            THEN UPPER(regexp_extract(instrument_id, '^[A-Za-z]+\\d{3,4}([CP])', 1))
                        ELSE option_type
                    END
                ),
                strike_price = COALESCE(
                    strike_price,
                    CASE
                        -- 连字符格式：SHFE2401-C-5000
                        WHEN regexp_matches(instrument_id, '^[A-Za-z]+\\d{4}-[CP]-\\d+(?:\\.\\d+)?$')
                            THEN CAST(regexp_extract(instrument_id, '^[A-Za-z]+\\d{4}-[CP]-(\\d+(?:\\.\\d+)?)$', 1) AS DOUBLE)
                        -- 紧凑格式：SHFE2401C5000
                        WHEN regexp_matches(instrument_id, '^[A-Za-z]+\\d{3,4}[CP]\\d+$')
                            THEN CAST(regexp_extract(instrument_id, '^[A-Za-z]+\\d{3,4}[CP](\\d+)$', 1) AS DOUBLE)
                        ELSE strike_price
                    END
                )
            WHERE option_type IS NULL OR strike_price IS NULL
        """)

    def _create_indexes_and_views(self):
        conn = self._get_connection()
        # 索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_instrument ON ticks_raw (instrument_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON ticks_raw (timestamp)")
        
        # ✅ 优化 1: 复合索引（instrument_id + timestamp DESC）
        # 适用场景：查询某合约的历史数据（带时间排序）
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_instrument_time 
            ON ticks_raw (instrument_id, timestamp DESC)
        """)
        
        # ✅ 优化 1: 覆盖索引（将常用字段包含在索引中）
        # 适用场景：只需要价格数据的查询（避免回表）
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_covering_price 
            ON ticks_raw (instrument_id, timestamp, last_price, volume, open_interest)
        """)
        
        # ✅ 优化 1: 复合索引（将常用字段包含在索引中）
        # 适用场景：按日期分组查询
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date_instrument 
            ON ticks_raw (date, instrument_id)
        """)

        # 每日全局聚合视图
        if PREAGGREGATE_DAILY:
            conn.execute("""
                CREATE OR REPLACE VIEW daily_aggregates AS
                SELECT
                    date,
                    COUNT(*) AS tick_count,
                    AVG(last_price) AS avg_price,
                    MAX(last_price) AS max_price,
                    MIN(last_price) AS min_price,
                    SUM(volume) AS total_volume
                FROM ticks_raw
                GROUP BY date
                ORDER BY date
            """)
            logger.info("Created view: daily_aggregates")

        # 每合约每日 OHLC
        if PREAGGREGATE_SYMBOL_DAILY:
            conn.execute("""
                CREATE OR REPLACE VIEW symbol_daily_aggregates AS
                SELECT
                    instrument_id,
                    date,
                    COUNT(*) AS tick_count,
                    FIRST(last_price ORDER BY timestamp) AS open_price,
                    LAST(last_price ORDER BY timestamp) AS close_price,
                    MAX(last_price) AS high_price,
                    MIN(last_price) AS low_price,
                    SUM(volume) AS total_volume
                FROM ticks_raw
                GROUP BY instrument_id, date
                ORDER BY instrument_id, date
            """)
            logger.info("Created view: symbol_daily_aggregates")

        # 最新价格视图（使用窗口函数，兼容 DuckDB）
        conn.execute("""
            CREATE OR REPLACE VIEW latest_prices AS
            SELECT instrument_id, last_price, timestamp
            FROM (
                SELECT
                    instrument_id,
                    last_price,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY timestamp DESC) AS rn
                FROM ticks_raw
            ) WHERE rn = 1
        """)
        logger.info("Created view: latest_prices")
        
        # ✅ 新增：期权同步虚值统计物化视图（DuckDB 兼容实现）
        # 使用物理表 + 刷新机制替代 MATERIALIZED VIEW，保持相同功能
        logger.info("Creating materialized view: option_sync_otm_stats (physical table implementation)")
        
        # Step 1: 创建物理表存储结果
        conn.execute("DROP TABLE IF EXISTS option_sync_otm_stats")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS option_sync_otm_stats (
                month VARCHAR,
                underlying_symbol VARCHAR,
                option_type VARCHAR,
                correct_rise_otm_count BIGINT,  -- 正确上涨虚值数量
                wrong_rise_otm_count BIGINT,    -- 错误上涨虚值数量
                correct_fall_otm_count BIGINT,  -- 正确下跌虚值数量
                wrong_fall_otm_count BIGINT,    -- 错误下跌虚值数量
                other_otm_count BIGINT,         -- 其它状态虚值数量
                total_otm_count BIGINT,
                total_samples BIGINT,
                calculated_at TIMESTAMP
            )
        """)
        logger.info("Created physical table: option_sync_otm_stats")
        
        # Step 2: 创建索引加速查询
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_option_stats_month ON option_sync_otm_stats(month)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_option_stats_symbol ON option_sync_otm_stats(underlying_symbol)")
            logger.info("Created indexes on option_sync_otm_stats")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
        
        # Step 3: 初始数据填充
        self._refresh_option_sync_stats()
        logger.info("Initial data loaded into option_sync_otm_stats")
        
        # ✅ 新增：为期权合约初始化状态列（批量更新历史数据）
        self._update_option_status_columns(conn)
        
        # ✅ 优化 2: 收集统计信息（帮助查询优化器）
        try:
            conn.execute("ANALYZE ticks_raw")
            logger.info("Statistics collected for query optimizer")
        except Exception as e:
            logger.warning(f"Failed to collect statistics: {e}")

    # ======================== 辅助函数 ========================

    def _refresh_option_sync_stats(self):
        """
        ✅ 新增：刷新期权同步虚值统计物化视图数据
        
        功能说明：
        1. 清空旧数据
        2. 重新计算所有期权的同步状态和虚值数量
        3. 按月聚合统计结果
        
        调用时机：
        - 初始化时填充数据
        - 定期刷新（如每日收盘后）
        - 手动触发刷新
        """
        logger.info("Refreshing option_sync_otm_stats data...")
        conn = self._get_connection()
        
        try:
            # Step 1: 清空旧数据
            conn.execute("DELETE FROM option_sync_otm_stats")
            
            # Step 2: 插入新计算的聚合数据
            # 使用 ASOF JOIN 替代 LEFT JOIN LATERAL：O(N log M) vs O(N*M)
            conn.execute("""
                INSERT INTO option_sync_otm_stats (
                    month, underlying_symbol, option_type,
                    correct_rise_otm_count, wrong_rise_otm_count, 
                    correct_fall_otm_count, wrong_fall_otm_count, other_otm_count,
                    total_otm_count, total_samples, calculated_at
                )
                WITH option_base AS (
                    SELECT
                        t.instrument_id,
                        t.timestamp,
                        t.last_price AS option_price,
                        oi.underlying_future_id,
                        fi.product AS underlying_symbol,
                        COALESCE(t.option_type, oi.option_type) AS option_type,
                        COALESCE(t.strike_price, oi.strike_price) AS strike_price,
                        f.last_price AS underlying_price
                    FROM ticks_raw t
                    INNER JOIN option_instruments oi ON oi.instrument_id = t.instrument_id
                    INNER JOIN futures_instruments fi ON fi.internal_id = oi.underlying_future_id
                    ASOF LEFT JOIN ticks_raw f
                        ON f.instrument_id = fi.instrument_id
                        AND t.timestamp >= f.timestamp
                    WHERE oi.underlying_future_id IS NOT NULL
                ),
                option_with_lag AS (
                    SELECT
                        *,
                        LAG(underlying_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_underlying_price,
                        LAG(option_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_option_price
                    FROM option_base
                    WHERE underlying_price IS NOT NULL
                ),
                option_calculated AS (
                    SELECT
                        *,
                        CASE 
                            WHEN option_type = 'CALL' AND underlying_price < strike_price THEN TRUE
                            WHEN option_type = 'PUT' AND underlying_price > strike_price THEN TRUE
                            ELSE FALSE
                        END AS is_otm,
                        CASE 
                            WHEN underlying_price IS NULL OR prev_underlying_price IS NULL OR prev_option_price IS NULL THEN 'other'
                            WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                            WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                            WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                            WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                            WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                            WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                            WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                            WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                            ELSE 'other'
                        END AS sync_status
                    FROM option_with_lag
                    WHERE prev_underlying_price IS NOT NULL AND prev_option_price IS NOT NULL
                )
                SELECT
                    strftime(timestamp, '%Y%m') AS month,
                    underlying_symbol,
                    option_type,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'correct_rise') AS correct_rise_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'wrong_rise') AS wrong_rise_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'correct_fall') AS correct_fall_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'wrong_fall') AS wrong_fall_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'other') AS other_otm_count,
                    COUNT(*) FILTER (WHERE is_otm) AS total_otm_count,
                    COUNT(*) AS total_samples,
                    MAX(timestamp) AS calculated_at
                FROM option_calculated
                GROUP BY strftime(timestamp, '%Y%m'), underlying_symbol, option_type
                ORDER BY month, underlying_symbol, option_type
            """)
            
            logger.info(f"Refreshed option_sync_otm_stats successfully")
            
        except Exception as e:
            logger.error(f"Failed to refresh option_sync_otm_stats: {e}")
            raise

    def _update_option_status_columns(self, conn):
        """
        ✅ 新增：为期货和期权合约批量计算并更新同步状态列
        
        核心逻辑：
        1. 识别期权合约（instrument_id 包含 -C- 或 -P-）→ 计算与期货的同步
        2. 识别期货合约（非期权）→ 计算与主力合约的同步
        3. 批量更新 ticks_raw 表
        
        优化：使用 ASOF JOIN 替代 LEFT JOIN LATERAL，将 O(N*M) 降为 O(N log M)。
        """
        logger.info("Updating option and future status columns...")
        
        try:
            # ========== 第一部分：期权同步状态计算 ==========
            # 使用 ASOF JOIN 替代 LEFT JOIN LATERAL：O(N log M) vs O(N*M)
            conn.execute("""
                CREATE OR REPLACE TEMP VIEW option_status_calc AS
                WITH option_base AS (
                    SELECT
                        t.rowid AS row_id,
                        t.instrument_id,
                        t.timestamp,
                        t.last_price AS option_price,
                        oi.underlying_future_id,
                        fi.product AS underlying_symbol,
                        COALESCE(t.option_type, oi.option_type) AS option_type,
                        COALESCE(t.strike_price, oi.strike_price) AS strike_price,
                        f.last_price AS underlying_price
                    FROM ticks_raw t
                    INNER JOIN option_instruments oi ON oi.instrument_id = t.instrument_id
                    INNER JOIN futures_instruments fi ON fi.internal_id = oi.underlying_future_id
                    ASOF LEFT JOIN ticks_raw f
                        ON f.instrument_id = fi.instrument_id
                        AND t.timestamp >= f.timestamp
                    WHERE oi.underlying_future_id IS NOT NULL
                ),
                option_with_lag AS (
                    SELECT
                        row_id,
                        *,
                        LAG(underlying_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_underlying_price,
                        LAG(option_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_option_price
                    FROM option_base
                    WHERE underlying_price IS NOT NULL
                )
                SELECT
                    row_id,
                    CASE 
                        WHEN option_type = 'CALL' AND underlying_price < strike_price THEN TRUE
                        WHEN option_type = 'PUT' AND underlying_price > strike_price THEN TRUE
                        ELSE FALSE
                    END AS is_otm,
                    CASE 
                        WHEN underlying_price IS NULL OR prev_underlying_price IS NULL OR prev_option_price IS NULL THEN 'other'
                        WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                        WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                        WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                        WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                        WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                        WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                        WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                        WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                        ELSE 'other'
                    END AS sync_status,
                    NULL AS future_sync_status,
                    NULL AS is_same_rise,
                    NULL AS is_same_fall,
                    NULL AS is_diff_sync
                FROM option_with_lag
                WHERE prev_underlying_price IS NOT NULL AND prev_option_price IS NOT NULL
            """)
            
            # 执行期权批量更新
            conn.execute("""
                UPDATE ticks_raw
                SET 
                    is_otm = calc.is_otm,
                    sync_status = calc.sync_status,
                    future_sync_status = calc.future_sync_status,
                    is_same_rise = calc.is_same_rise,
                    is_same_fall = calc.is_same_fall,
                    is_diff_sync = calc.is_diff_sync
                FROM option_status_calc calc
                WHERE ticks_raw.rowid = calc.row_id
            """)
            
            logger.info("Option status columns updated.")
            
            # ========== 第二部分：期货同步状态计算 ==========
            # 使用 ASOF JOIN 替代 LEFT JOIN LATERAL：O(N log M) vs O(N*M)
            conn.execute("""
                CREATE OR REPLACE TEMP VIEW future_status_calc AS
                WITH future_base AS (
                    SELECT
                        t.rowid AS row_id,
                        t.instrument_id,
                        t.timestamp,
                        t.last_price AS future_price,
                        fi.product AS underlying_symbol,
                        ref.last_price AS main_future_price,
                        ref.volume AS main_volume
                    FROM ticks_raw t
                    JOIN futures_instruments fi ON t.instrument_id = fi.instrument_id
                    ASOF LEFT JOIN (
                        SELECT ref.instrument_id, ref.timestamp, ref.last_price, ref.volume
                        FROM ticks_raw ref
                        JOIN futures_instruments fi2 ON ref.instrument_id = fi2.instrument_id
                    ) ref
                        ON t.timestamp >= ref.timestamp
                        AND fi.product = (SELECT fi2.product FROM futures_instruments fi2 WHERE fi2.instrument_id = ref.instrument_id)
                ),
                future_with_lag AS (
                    SELECT
                        row_id,
                        *,
                        LAG(future_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_future_price,
                        LAG(main_future_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_main_future_price
                    FROM future_base
                    WHERE main_future_price IS NOT NULL
                )
                SELECT
                    row_id,
                    CASE 
                        WHEN future_price > prev_future_price 
                             AND main_future_price > prev_main_future_price
                            THEN 'same_rise'
                        WHEN future_price < prev_future_price 
                             AND main_future_price < prev_main_future_price
                            THEN 'same_fall'
                        ELSE 'diff'
                    END AS future_sync_status,
                    CASE 
                        WHEN future_price > prev_future_price 
                             AND main_future_price > prev_main_future_price
                            THEN TRUE ELSE FALSE 
                    END AS is_same_rise,
                    CASE 
                        WHEN future_price < prev_future_price 
                             AND main_future_price < prev_main_future_price
                            THEN TRUE ELSE FALSE 
                    END AS is_same_fall,
                    CASE 
                        WHEN NOT (
                            (future_price > prev_future_price AND main_future_price > prev_main_future_price)
                            OR (future_price < prev_future_price AND main_future_price < prev_main_future_price)
                        )
                        THEN TRUE ELSE FALSE 
                    END AS is_diff_sync,
                    NULL AS is_otm,
                    NULL AS sync_status
                FROM future_with_lag
                WHERE prev_future_price IS NOT NULL AND prev_main_future_price IS NOT NULL
            """)
            
            # 执行期货批量更新
            conn.execute("""
                UPDATE ticks_raw
                SET 
                    is_otm = calc.is_otm,
                    sync_status = calc.sync_status,
                    future_sync_status = calc.future_sync_status,
                    is_same_rise = calc.is_same_rise,
                    is_same_fall = calc.is_same_fall,
                    is_diff_sync = calc.is_diff_sync
                FROM future_status_calc calc
                WHERE ticks_raw.rowid = calc.row_id
            """)
            
            logger.info("Future status columns updated.")
            logger.info("All status columns updated successfully.")
            
        except Exception as e:
            logger.warning(f"Failed to update status columns: {e}")
    
    def on_tick(self, symbol: str, price: float, timestamp: datetime, 
                volume: int = 0, bid_price: float = 0.0, ask_price: float = 0.0):
        """接收 tick 时同步更新内存缓存（非持久化路径）
        
        注意：此方法仅更新 RealTimeCache 内存缓存和清理查询缓存，
        不写入 ticks_raw 表。数据持久化应通过 storage.process_tick() 完成。
        不应将此方法作为数据持久化路径使用。
        """
        if self.realtime_cache:
            self.realtime_cache.update_tick(symbol, price, timestamp, volume, bid_price, ask_price)
        
        # P1 Bug #44修复：精确匹配symbol，避免子串匹配误删其他合约缓存
        with self._cache_lock:
            keys_to_delete = [k for k in self._query_cache.keys() if k == symbol or (isinstance(k, str) and k.startswith(symbol + '.'))]
            for k in keys_to_delete:
                del self._query_cache[k]

    @staticmethod
    def _normalize_datetime_for_cache(dt: Optional[datetime]) -> Optional[str]:
        """将 datetime 规范化为 UTC naive 字符串（无微秒），用于缓存键。"""
        if dt is None:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        dt = dt.replace(microsecond=0)
        return dt.isoformat()

    @staticmethod
    def _to_utc_naive(dt: datetime) -> datetime:
        """将可能有时区的 datetime 转换为 UTC naive，并发出警告。"""
        if dt.tzinfo is not None:
            logger.warning(f"Converting timezone-aware datetime {dt} to UTC naive. "
                           "Ensure your table's timestamp column is UTC.")
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    # ======================== 公共查询接口 ========================

    def query(self, sql: str, params: Optional[List] = None, arrow: bool = True, raise_on_error: bool = False, use_cache: bool = True):
        """
        执行 SQL 查询，默认返回 Arrow Table。
        
        Args:
            raise_on_error: 是否抛出异常（False 时返回空表）
            use_cache: 是否使用缓存（默认True）
        """
        if use_cache and QUERY_CACHE_SIZE > 0:
            return self._query_with_cache(sql, params)
        
        conn = self._get_connection()
        try:
            if params:
                rel = conn.execute(sql, params)
            else:
                rel = conn.execute(sql)
            result = rel.arrow() if arrow else rel.df()
            # 如果返回的是 RecordBatchReader，转换为 Table
            if hasattr(result, 'read_all'):
                result = result.read_all()
            return result
        except Exception as e:
            logger.error(f"Query failed: {e}\nSQL: {sql}")
            if raise_on_error:
                raise
            return pa.table({}) if arrow else pd.DataFrame()

    def _query_with_cache(self, sql: str, params: Optional[List] = None) -> pa.Table:
        """内部缓存查询方法，由query()调用"""
        # 规范化参数（将 datetime 转为字符串）
        norm_params = []
        if params:
            for p in params:
                if isinstance(p, datetime):
                    norm_params.append(self._normalize_datetime_for_cache(p))
                else:
                    norm_params.append(p)
        key = hashlib.md5((sql + repr(norm_params)).encode()).hexdigest()
        with self._cache_lock:
            if key in self._query_cache:
                result, expire = self._query_cache[key]
                if time.time() < expire:
                    return result
                else:
                    del self._query_cache[key]
        result = self.query(sql, params, use_cache=False)
        with self._cache_lock:
            self._query_cache[key] = (result, time.time() + QUERY_CACHE_TTL)
            self._query_cache.move_to_end(key)  # ✅ 移到末尾（最近使用）
            if len(self._query_cache) > QUERY_CACHE_SIZE:
                self._query_cache.popitem(last=False)  # ✅ 删除最久未使用的（LRU）
        return result

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """获取合约最新价格 - RealTimeCache为唯一实时价格源，DuckDB仅历史回补"""
        # ✅ 优先从内存缓存获取（极快且实时）- 唯一权威源
        if self.realtime_cache:
            price = self.realtime_cache.get_latest_price(symbol)
            if price is not None:
                return price
        
        # P1 修复：降级查询不使用缓存，避免 TTL 导致的数据陈旧
        # P2 Bug #7修复：明确指定 arrow=False 确保返回 pandas DataFrame
        sql = "SELECT last_price FROM latest_prices WHERE instrument_id = ?"
        result_df = self.query(sql, [symbol], raise_on_error=False, arrow=False, use_cache=False)
        
        if hasattr(result_df, 'empty'):
            return float(result_df['last_price'].iloc[0]) if not result_df.empty else None
        elif hasattr(result_df, 'num_rows'):
            return result_df['last_price'][0].as_py() if result_df.num_rows > 0 else None
        return None

    def batch_get_latest_prices(self, symbols: List[str]) -> pa.Table:
        """批量获取合约最新价格 - RealTimeCache为唯一实时价格源，DuckDB仅历史回补
        
        Args:
            symbols: 合约代码列表
            
        Returns:
            pa.Table: 包含 instrument_id, last_price, timestamp 的结果表
        """
        if not symbols:
            return pa.table({})
        
        # ✅ 优先从内存缓存获取（极快）- 唯一权威源
        if self.realtime_cache:
            cached_results = []
            missing_symbols = []
            for s in symbols:
                price = self.realtime_cache.get_latest_price(s)
                if price is not None:
                    cached_results.append({'instrument_id': s, 'last_price': price, 'timestamp': datetime.now()})
                else:
                    missing_symbols.append(s)
            
            # 如果全部命中缓存，直接返回
            if not missing_symbols:
                return pa.Table.from_pylist(cached_results)
            
            # 部分命中，继续查询缺失的
            if cached_results and missing_symbols:
                logger.debug(f"[DataService] Partial cache hit: {len(cached_results)}/{len(symbols)}")
                symbols = missing_symbols

        # 降级到 DuckDB 批量查询（不使用缓存保证实时性）
        placeholders = ','.join(['?' for _ in symbols])
        sql = f"""
            SELECT instrument_id, last_price, timestamp
            FROM latest_prices
            WHERE instrument_id IN ({placeholders})
        """
        result = self.query(sql, symbols, use_cache=False)
        
        # 合并缓存结果和数据库结果
        if cached_results and hasattr(result, 'num_rows') and result.num_rows > 0:
            # 将 Arrow Table 转换为列表并合并
            db_results = result.to_pylist()
            return pa.Table.from_pylist(cached_results + db_results)
        
        return result

    def get_time_range(self, instrument_id: str, start: datetime, end: datetime,
                       columns: Optional[List[str]] = None) -> pa.Table:
        """
        时间范围查询。
        注意：表中 timestamp 必须为 UTC naive。传入的 start/end 若带时区，会强制转换为 UTC naive 并发出警告。
        """
        start_utc = self._to_utc_naive(start)
        end_utc = self._to_utc_naive(end)
        if columns is None:
            columns = ['timestamp', 'last_price', 'volume']
        cols = ', '.join(columns)
        sql = f"""
            SELECT {cols}
            FROM ticks_raw
            WHERE instrument_id = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """
        return self.query(sql, [instrument_id, start_utc, end_utc])

    def get_daily_aggregates(self, start_date: date, end_date: date) -> pa.Table:
        sql = "SELECT * FROM daily_aggregates WHERE date BETWEEN ? AND ? ORDER BY date"
        return self.query(sql, [start_date, end_date])

    def get_symbol_daily_ohlc(self, symbol: str, start_date: date, end_date: date) -> pa.Table:
        sql = """
            SELECT * FROM symbol_daily_aggregates
            WHERE instrument_id = ? AND date BETWEEN ? AND ?
            ORDER BY date
        """
        return self.query(sql, [symbol, start_date, end_date])

    # ======================== K 线查询接口 ========================

    def get_kline_range(
        self,
        instrument_id: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None
    ) -> pa.Table:
        """
        获取 K 线时间范围数据（基于 ticks_raw 表）。
        
        Args:
            instrument_id: 合约代码
            start: 开始时间（UTC naive 或带时区，会自动转换）
            end: 结束时间（UTC naive 或带时区，会自动转换）
            limit: 可选的结果数量限制
            
        Returns:
            pa.Table: K 线数据，包含 timestamp, open, high, low, close, volume, open_interest
        """
        start_utc = self._to_utc_naive(start)
        end_utc = self._to_utc_naive(end)
        
        sql = """
            SELECT timestamp, open, high, low, close, volume, open_interest
            FROM ticks_raw
            WHERE instrument_id = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """
        params = [instrument_id, start_utc, end_utc]
        
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        
        return self.query(sql, params)

    def get_latest_klines(
        self,
        instrument_id: str,
        limit: int = 100
    ) -> pa.Table:
        """
        获取最新 N 条 K 线数据（基于 ticks_raw 表）。
        
        Args:
            instrument_id: 合约代码
            limit: 返回数量限制（默认 100）
            
        Returns:
            pa.Table: K 线数据，按时间正序排列
        """
        sql = """
            SELECT timestamp, open, high, low, close, volume, open_interest
            FROM ticks_raw
            WHERE instrument_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        result = self.query(sql, [instrument_id, limit])
        
        # 如果是 Arrow Table，使用 to_pandas() 反转后再转回 Arrow
        if hasattr(result, 'to_pandas'):
            df = result.to_pandas()
            df = df.iloc[::-1].reset_index(drop=True)  # 反转为正序
            return pa.Table.from_pandas(df)
        
        return result

    def get_kline_count(self, instrument_id: str) -> int:
        """
        获取合约的 K 线总条数。
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            int: K 线总条数
        """
        sql = "SELECT COUNT(*) as cnt FROM ticks_raw WHERE instrument_id = ?"
        result_df = self.query(sql, [instrument_id])
        if hasattr(result_df, 'empty'):
            return int(result_df['cnt'].iloc[0]) if not result_df.empty else 0
        elif hasattr(result_df, 'num_rows'):
            return result_df['cnt'][0].as_py() if result_df.num_rows > 0 else 0
        return 0

    def get_kline_stats(self, instrument_id: str) -> Dict[str, Any]:
        """
        获取合约 K 线统计数据。
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            Dict[str, Any]: 统计信息 {count, first_time, last_time}
        """
        sql = """
            SELECT 
                COUNT(*) as count,
                MIN(timestamp) as first_time,
                MAX(timestamp) as last_time
            FROM ticks_raw
            WHERE instrument_id = ?
        """
        result_df = self.query(sql, [instrument_id])
        # 如果是 DataFrame，检查是否为空
        if hasattr(result_df, 'empty'):
            if result_df.empty:
                return {}
            return {
                'count': int(result_df['count'].iloc[0]),
                'first_time': result_df['first_time'].iloc[0],
                'last_time': result_df['last_time'].iloc[0]
            }
        # 如果是 Arrow Table
        elif hasattr(result_df, 'num_rows'):
            if result_df.num_rows == 0:
                return {}
            return {
                'count': result_df['count'][0].as_py(),
                'first_time': result_df['first_time'][0].as_py(),
                'last_time': result_df['last_time'][0].as_py()
            }
        return {}

    def _enrich_tick_option_metadata(self, ticks_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """复用统一解析器，为批量 Tick 插入补齐期权元数据。
        
        修复标准：统一为ParamsService缓存+parse_option降级，删除DB查询
        """
        try:
            from ali2026v3_trading.subscription_manager import SubscriptionManager
        except Exception:
            SubscriptionManager = None

        enriched_ticks: List[Dict[str, Any]] = []
        for tick in ticks_data or []:
            tick_row = dict(tick or {})
            tick_row.setdefault('option_type', None)
            tick_row.setdefault('strike_price', None)

            instrument_id = str(tick_row.get('instrument_id') or '').strip()
            enriched = False
            
            # ✅ 第一优先级：ParamsService缓存（唯一权威源）
            if instrument_id and self.params_service:
                try:
                    info = self.params_service.get_instrument_meta_by_id(instrument_id)
                    if info and info.get('type') == 'option':
                        raw_opt = info.get('option_type', '')
                        raw_opt_upper = raw_opt.upper() if raw_opt else ''
                        tick_row['option_type'] = 'CALL' if raw_opt_upper in ('C', 'CALL') else 'PUT' if raw_opt_upper in ('P', 'PUT') else None
                        tick_row['strike_price'] = float(info.get('strike_price') or 0.0)
                        enriched = True
                except Exception:
                    pass

            # ✅ 第二优先级：SubscriptionManager.parse_option降级解析
            if not enriched and instrument_id and SubscriptionManager:
                try:
                    parsed = SubscriptionManager.parse_option(instrument_id)
                    if not tick_row.get('option_type'):
                        tick_row['option_type'] = 'CALL' if parsed.get('option_type') == 'C' else 'PUT'
                    if not tick_row.get('strike_price'):
                        tick_row['strike_price'] = float(parsed.get('strike_price') or 0.0)
                except Exception:
                    pass

            enriched_ticks.append(tick_row)

        return enriched_ticks

    def _ensure_arrow_tick_columns(self, arrow_table: pa.Table) -> pa.Table:
        """确保Arrow表包含数据库表的所有列，缺失的列用NULL填充。"""
        schema = self._build_tick_arrow_schema()
        existing_columns = set(arrow_table.schema.names)
        new_columns = []
        new_schema = []
        
        for field in schema:
            col_name = field.name
            if col_name in existing_columns:
                new_columns.append(arrow_table.column(col_name))
                new_schema.append((col_name, arrow_table.schema.field(col_name).type))
            else:
                null_array = pa.nulls(arrow_table.num_rows, type=field.type)
                new_columns.append(null_array)
                new_schema.append((col_name, field.type))
        
        return pa.Table.from_arrays(new_columns, schema=pa.schema(new_schema))

    def _build_tick_arrow_schema(self) -> pa.Schema:
        """构建ticks_raw表的显式schema，避免PyArrow类型推断错误。"""
        return pa.schema([
            ('timestamp', pa.timestamp('us')),
            ('instrument_id', pa.string()),
            ('last_price', pa.float64()),
            ('volume', pa.int64()),
            ('open_interest', pa.float64()),
            ('bid_price', pa.float64()),
            ('ask_price', pa.float64()),
            ('date', pa.date32()),
            ('option_type', pa.string()),
            ('strike_price', pa.float64()),
            ('is_otm', pa.bool_()),
            ('sync_status', pa.string()),
            ('future_sync_status', pa.string()),
            ('is_same_rise', pa.bool_()),
            ('is_same_fall', pa.bool_()),
            ('is_diff_sync', pa.bool_()),
        ])

    def _get_ticks_raw_column_names(self) -> List[str]:
        """获取ticks_raw表的所有列名（按顺序）。"""
        return [field.name for field in self._build_tick_arrow_schema()]

    def build_tick_arrow_batch(self, tick_rows: List[Dict], instrument_id: str) -> Optional[pa.Table]:
        """构建完整的 Arrow 表，包含数据库表的所有列。"""
        enriched = self._enrich_tick_option_metadata(tick_rows)
        
        normalized_ticks = []
        for row in enriched:
            ts_str = row.get('ts') or row.get('timestamp')
            if ts_str is None:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_str).split('.')[0]).timestamp()
                dt = datetime.fromtimestamp(ts)
                normalized_ticks.append({
                    'timestamp': dt,
                    'instrument_id': instrument_id,
                    'last_price': row.get('last_price', 0.0),
                    'volume': row.get('volume', 0),
                    'open_interest': float(row.get('open_interest', 0) or 0),
                    'bid_price': row.get('bid_price1'),
                    'ask_price': row.get('ask_price1'),
                    'date': dt.date(),
                    'option_type': row.get('option_type'),
                    'strike_price': row.get('strike_price'),
                    'is_otm': None,
                    'sync_status': None,
                    'future_sync_status': None,
                    'is_same_rise': None,
                    'is_same_fall': None,
                    'is_diff_sync': None,
                })
            except Exception:
                continue
        
        if not normalized_ticks:
            return None
        
        return pa.Table.from_pylist(normalized_ticks, schema=self._build_tick_arrow_schema())

    def merge_arrow_tick_tables(self, tables: List[pa.Table]) -> Optional[pa.Table]:
        if not tables:
            return None
        if len(tables) == 1:
            return self._ensure_arrow_tick_columns(tables[0])
        merged = pa.concat_tables(tables)
        return self._ensure_arrow_tick_columns(merged)

    def merge_tick_task_batch(self, batch, info_callback=None) -> List[Tuple[str, Any, Any]]:
        merged = []
        arrow_batches: Dict[Tuple[int, str], List[pa.Table]] = {}
        
        for func_name, args, kwargs in batch:
            if func_name == '_save_tick_impl' and len(args) >= 3:
                internal_id = args[0]
                instrument_type = args[1]
                tick_data = args[2]
                
                key = (internal_id, instrument_type)
                if key not in arrow_batches:
                    arrow_batches[key] = []
                
                if isinstance(tick_data, pa.Table):
                    arrow_batches[key].append(tick_data)
                elif isinstance(tick_data, list) and tick_data:
                    if info_callback:
                        info = info_callback(internal_id)
                        instrument_id = info.get('instrument_id', '') if info else ''
                    else:
                        instrument_id = ''
                    
                    if instrument_id:
                        arrow_table = self.build_tick_arrow_batch(tick_data, instrument_id)
                        if arrow_table is not None:
                            arrow_batches[key].append(arrow_table)
            else:
                merged.append((func_name, args, kwargs))
        
        for (internal_id, instrument_type), tables in arrow_batches.items():
            if tables:
                merged_table = self.merge_arrow_tick_tables(tables)
                if merged_table is not None:
                    merged.append(('_save_tick_impl', (internal_id, instrument_type, merged_table), {}))
        
        return merged

    # ======================== 优化 3: 批量插入接口 ========================
    
    def batch_insert_ticks(self, ticks_data, instrument_id: str = None, use_arrow: bool = True) -> int:
        """
        批量插入 Tick 数据（事务级别优化，统一接口）
        
        Args:
            ticks_data: Tick 数据，可以是：
                - pa.Table: PyArrow表（方案3优化：零拷贝路径）
                - List[Dict]: dict列表（向后兼容，自动转为Arrow）
            instrument_id: 合约ID（当ticks_data为dict列表时需要）
            use_arrow: 是否使用 Arrow 批量插入（默认 True，自动转换）
        
        Returns:
            int: 成功插入的记录数
        """
        conn = self._get_connection()
        
        try:
            if isinstance(ticks_data, pa.Table):
                arrow_table = self._ensure_arrow_tick_columns(ticks_data)
                row_count = arrow_table.num_rows
            elif use_arrow and isinstance(ticks_data, list) and len(ticks_data) > 0:
                if instrument_id:
                    arrow_table = self.build_tick_arrow_batch(ticks_data, instrument_id)
                    if arrow_table is None:
                        return 0
                    arrow_table = self._ensure_arrow_tick_columns(arrow_table)
                else:
                    arrow_table = pa.Table.from_pylist(self._enrich_tick_option_metadata(ticks_data))
                    arrow_table = self._ensure_arrow_tick_columns(arrow_table)
                row_count = arrow_table.num_rows
            else:
                return 0
            
            # P1 Bug #42修复：检查事务状态，避免嵌套事务异常
            in_transaction = False
            try:
                conn.execute("BEGIN")
            except Exception:
                in_transaction = True
            
            try:
                conn.register('temp_ticks', arrow_table)
                columns = self._get_ticks_raw_column_names()
                columns_str = ', '.join(columns)
                conn.execute(f"""
                    INSERT INTO ticks_raw ({columns_str})
                    SELECT {columns_str}
                    FROM temp_ticks
                """)
                conn.unregister('temp_ticks')
                if not in_transaction:
                    conn.execute("COMMIT")
            except Exception as e:
                if not in_transaction:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                logger.error(f"Arrow batch insert failed, rolled back: {e}")
                if self._is_fatal_database_error(e):
                    self._mark_connection_unhealthy()
                raise
            
            logger.info(f"Batch inserted {row_count} ticks via Arrow")
            return row_count
        
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise

        return 0

    def upsert_future_instrument(self, instrument_id: str, product: str, exchange: str,
                                 year_month: str, is_active: bool = True) -> int:
        """
        插入或更新期货合约，同时分配 internal_id。

        Args:
            instrument_id: 合约代码，如 'IF2605'
            product: 品种代码，如 'IF'
            exchange: 交易所代码
            year_month: 年月，如 '2605'
            is_active: 是否活跃

        Returns:
            int: internal_id
        """
        conn = self._get_connection()
        try:
            existing = conn.execute("""
                SELECT internal_id FROM futures_instruments WHERE instrument_id = ?
            """, [instrument_id]).fetchone()
            if existing:
                return existing[0]

            next_id = self._get_next_instrument_id()
            conn.execute("""
                INSERT INTO futures_instruments (internal_id, instrument_id, product, exchange, year_month, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [next_id, instrument_id, product, exchange, year_month, is_active])
            logger.info(f"Upserted future instrument: {instrument_id} with internal_id={next_id}")
            return next_id
        except Exception as e:
            logger.error(f"Failed to upsert future instrument {instrument_id}: {e}")
            raise

    def upsert_option_instrument(self, instrument_id: str, product: str, exchange: str,
                                 underlying_future_id: Optional[int], underlying_product: str,
                                 year_month: str, option_type: str, strike_price: float,
                                 is_active: bool = True) -> int:
        """
        插入或更新期权合约，同时分配 internal_id。

        Args:
            instrument_id: 合约代码，如 'HO2605-C-2800'
            product: 品种代码，如 'HO'
            exchange: 交易所代码
            underlying_future_id: 标的期货的 internal_id（由调用方通过映射提供，符合ID直通原则）
            underlying_product: 标的期货品种，如 'IH'
            year_month: 年月，如 '2605'
            option_type: 'C' 或 'P'
            strike_price: 行权价
            is_active: 是否活跃

        Returns:
            int: internal_id
        """
        conn = self._get_connection()
        try:
            existing = conn.execute("""
                SELECT internal_id, underlying_future_id, underlying_product, year_month, exchange,
                       option_type, strike_price, is_active
                FROM option_instruments WHERE instrument_id = ?
            """, [instrument_id]).fetchone()
            if existing:
                updates = []
                params = []

                # 配置文件是 ID 直通的源头，若当前计算出的 underlying_future_id 与库内不一致，则同步修正
                if underlying_future_id is not None and existing[1] != underlying_future_id:
                    updates.append("underlying_future_id = ?")
                    params.append(underlying_future_id)

                if underlying_product and existing[2] != underlying_product:
                    updates.append("underlying_product = ?")
                    params.append(underlying_product)

                if year_month and existing[3] != year_month:
                    updates.append("year_month = ?")
                    params.append(year_month)

                if exchange and existing[4] != exchange:
                    updates.append("exchange = ?")
                    params.append(exchange)

                if option_type and existing[5] != option_type:
                    updates.append("option_type = ?")
                    params.append(option_type)

                if strike_price and float(existing[6] or 0.0) != float(strike_price):
                    updates.append("strike_price = ?")
                    params.append(strike_price)

                if bool(existing[7]) != bool(is_active):
                    updates.append("is_active = ?")
                    params.append(is_active)

                if updates:
                    params.append(instrument_id)
                    conn.execute("""
                        UPDATE option_instruments
                        SET {updates_sql}
                        WHERE instrument_id = ?
                    """.replace("{updates_sql}", ", ".join(updates)), params)
                    logger.info(
                        "Updated option instrument from config: %s, underlying_future_id=%s",
                        instrument_id,
                        underlying_future_id,
                    )
                return existing[0]

            next_id = self._get_next_instrument_id()
            conn.execute("""
                INSERT INTO option_instruments (internal_id, instrument_id, product, exchange,
                    underlying_future_id, underlying_product, year_month, option_type, strike_price, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [next_id, instrument_id, product, exchange, underlying_future_id,
                  underlying_product, year_month, option_type, strike_price, is_active])
            logger.info(f"Upserted option instrument: {instrument_id} with internal_id={next_id}, underlying_future_id={underlying_future_id}")
            return next_id
        except Exception as e:
            logger.error(f"Failed to upsert option instrument {instrument_id}: {e}")
            raise

    def _get_next_instrument_id(self) -> int:
        """从全局序列获取下一个 internal_id
        
        ✅ P1-3根因修复 + ID直通原则：使用统一的全局序列，确保所有合约ID来源一致
        """
        conn = self._get_connection()
        # 使用全局统一的 instrument_id_seq 序列
        result = conn.execute("SELECT nextval('instrument_id_seq')").fetchone()
        return result[0]

    def batch_insert_klines(self, klines_data: List[Dict[str, Any]]) -> int:
        """
        批量插入 K 线数据
        
        Args:
            klines_data: K 线数据列表，每个元素为 dict，包含 internal_id, instrument_type, timestamp, open, high, low, close, volume, open_interest, trade_date
        
        Returns:
            int: 成功插入的记录数
        """
        if not klines_data:
            return 0
        
        conn = self._get_connection()
        
        try:
            # 使用事务批量插入
            conn.execute("BEGIN")
            try:
                for kline in klines_data:
                    conn.execute("""
                        INSERT INTO klines_raw (internal_id, instrument_type, timestamp, open, high, low, close, volume, open_interest, trade_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        kline.get('internal_id'),
                        kline.get('instrument_type'),
                        kline.get('timestamp'),
                        kline.get('open'),
                        kline.get('high'),
                        kline.get('low'),
                        kline.get('close'),
                        kline.get('volume', 0),
                        kline.get('open_interest', 0),
                        kline.get('trade_date')
                    ])
                
                conn.execute("COMMIT")
                logger.info(f"Batch inserted {len(klines_data)} klines")
                return len(klines_data)
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                logger.error(f"Kline batch insert failed, rolled back: {e}")
                if self._is_fatal_database_error(e):
                    self._mark_connection_unhealthy()
                raise
        except Exception as e:
            logger.error(f"Batch insert klines failed: {e}")
            raise

    def explain(self, sql: str) -> str:
        """P1 Bug #43修复：返回查询执行计划，仅允许SELECT语句防止SQL注入"""
        # 安全检查：只允许SELECT语句
        sql_stripped = sql.strip().upper()
        if not sql_stripped.startswith('SELECT'):
            raise ValueError("explain() only accepts SELECT statements for security reasons")
        
        conn = self._get_connection()
        rows = conn.execute(f"EXPLAIN {sql}").fetchall()
        return "\n".join(str(row[0]) for row in rows)

    def refresh_data(self) -> bool:
        """重新加载 Parquet 数据（会删除现有表）。"""
        with self._lock:
            logger.info("Refreshing data...")
            conn = self._get_connection()
            try:
                conn.execute("DROP TABLE IF EXISTS ticks_raw")
                self._load_or_create_table()
                self._create_indexes_and_views()
                self.clear_cache()
                return True
            except Exception as e:
                logger.error(f"Refresh failed: {e}", exc_info=True)
                return False

    def upsert_future_product(self, product: str, exchange: str, format_template: str,
                              tick_size: float = 0.2, contract_size: float = 1.0,
                              is_active: bool = True) -> bool:
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO future_products (product, exchange, format_template, tick_size, contract_size, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (product) DO UPDATE SET
                exchange = excluded.exchange,
                format_template = excluded.format_template,
                tick_size = excluded.tick_size,
                contract_size = excluded.contract_size,
                is_active = excluded.is_active
            """, [product, exchange, format_template, tick_size, contract_size, is_active])
            logger.info(f"Upserted future product: {product}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert future product {product}: {e}")
            return False

    def upsert_option_product(self, product: str, exchange: str, underlying_product: str,
                              format_template: str, tick_size: float = 0.2,
                              contract_size: float = 1.0, is_active: bool = True) -> bool:
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO option_products (product, exchange, underlying_product, format_template, tick_size, contract_size, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (product) DO UPDATE SET
                exchange = excluded.exchange,
                underlying_product = excluded.underlying_product,
                format_template = excluded.format_template,
                tick_size = excluded.tick_size,
                contract_size = excluded.contract_size,
                is_active = excluded.is_active
            """, [product, exchange, underlying_product, format_template, tick_size, contract_size, is_active])
            logger.info(f"Upserted option product: {product}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert option product {product}: {e}")
            return False

    # ======================== ✅ 优化 4: 增量数据加载 ========================
    
    def incremental_load(self, new_parquet_path: str) -> Dict[str, Any]:
        """
        增量加载新数据（只追加，不覆盖）
        
        Args:
            new_parquet_path: 新 Parquet 文件路径
        
        Returns:
            Dict: 统计信息 {
                'loaded_count': int,      # 加载的记录数
                'skipped_count': int,     # 跳过的旧记录数
                'max_timestamp': datetime  # 当前最大时间戳
            }
        
        Example:
            >>> result = ds.incremental_load('data/new_ticks.parquet')
            >>> print(f"Loaded {result['loaded_count']} new ticks")
        """
        conn = self._get_connection()
        
        try:
            # 1. 获取当前最大时间戳
            max_ts_result = conn.execute("""
                SELECT MAX(timestamp) as max_ts FROM ticks_raw
            """).fetchone()
            
            max_ts = max_ts_result[0] if max_ts_result and max_ts_result[0] else None
            
            if not max_ts:
                # 首次全量加载
                logger.info("No existing data, performing full load...")
                self._load_or_create_table()
                return {
                    'loaded_count': 0, 
                    'skipped_count': 0, 
                    'max_timestamp': None
                }
            
            # 2. 读取Parquet数据并过滤
            logger.info(f"Incremental load from {max_ts}...")
            
            # 读取Parquet为Arrow表并过滤
            new_data = conn.execute("""
                SELECT * FROM read_parquet(?)
                WHERE timestamp > ?
                ORDER BY timestamp, instrument_id
            """, [new_parquet_path, max_ts]).fetch_arrow_table()
            
            if new_data.num_rows == 0:
                logger.info("No new data to load")
                return {
                    'loaded_count': 0, 
                    'skipped_count': 0, 
                    'max_timestamp': max_ts
                }
            
            # 3. 通过batch_insert_ticks统一插入
            new_count = new_data.num_rows
            dict_list = new_data.to_pylist()
            self.batch_insert_ticks(dict_list, use_arrow=True)
            
            # 5. 更新最大时间戳
            new_max_ts = conn.execute("SELECT MAX(timestamp) FROM ticks_raw").fetchone()[0]
            
            logger.info(f"Incremental load completed: {new_count} ticks, max_ts={new_max_ts}")
            
            return {
                'loaded_count': new_count,
                'skipped_count': 0,  # 跳过的旧数据量
                'max_timestamp': new_max_ts
            }
        
        except Exception as e:
            logger.error(f"Incremental load failed: {e}", exc_info=True)
            raise

    # ✅ ID唯一：clear_cache统一接口，服务=DataService
    def clear_cache(self):
        with self._cache_lock:
            self._query_cache.clear()
        logger.info("Query cache cleared.")
        logging.info('[DataService] Cache cleared')

    def close(self):
        if hasattr(self._thread_local, 'conn'):
            try:
                self._thread_local.conn.close()
            except:
                pass
            self._thread_local.conn = None

    def close_all(self):
        """P1 Bug #41修复：关闭所有线程的连接，而不仅仅是当前线程"""
        self._stop_monitor.set()
        
        # 关闭所有跟踪的连接
        closed_count = 0
        for conn in self._all_connections:
            try:
                conn.close()
                closed_count += 1
            except Exception as e:
                logger.warning(f"Failed to close connection: {e}")
        
        # 清空连接列表
        self._all_connections.clear()
        
        # 关闭当前线程连接
        self.close()
        logger.info(f"DataService closed all {closed_count} connections.")

    def _start_performance_monitor(self):
        """P1 修复：简化监控逻辑，实现长期稳定的后台内存监控"""
        def monitor():
            while not self._stop_monitor.is_set():
                # P1 修复：使用 wait 替代 sleep，支持即时唤醒并简化循环
                if self._stop_monitor.wait(timeout=300):  # 每 5 分钟检查一次
                    break
                try:
                    mem_info = psutil.virtual_memory()
                    logger.debug(f"[DataService] System Memory: {mem_info.percent}% used, Available: {mem_info.available / (1024**3):.1f}GB")
                except Exception as e:
                    logger.warning(f"[DataService] Memory monitoring error: {e}")
        threading.Thread(target=monitor, daemon=True, name="DuckDB-PerfMonitor").start()

    def sync_tick_tables_to_ticks_raw(self, storage_db_path: str = None) -> int:
        """
        ✅ 新增：将 tick_option_* 和 tick_future_* 分表数据同步到 ticks_raw 统一表
        
        Args:
            storage_db_path: 可选，storage使用的数据库路径。如果不提供，使用默认路径。
        
        Returns:
            int: 同步的记录数
        """
        if not self._tick_sync_lock.acquire(blocking=False):
            logger.info("[SyncTicks] Skip new run because another sync is already in progress")
            return 0

        started_at = time.perf_counter()
        external_conn = None
        logger.info("[SyncTicks] sync_tick_tables_to_ticks_raw called")

        try:
            # 如果提供了storage_db_path，使用该路径；否则使用默认的data/ticks.duckdb
            if storage_db_path:
                import duckdb
                external_conn = duckdb.connect(storage_db_path)
                self._configure_connection(external_conn)
                conn = external_conn
                logger.info(f"[SyncTicks] Using storage database: {storage_db_path}")
            else:
                conn = self._get_connection()
                logger.info("[SyncTicks] Using default DataService connection")

            # 检查 ticks_raw 是否存在，不存在则创建
            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name='ticks_raw'").fetchall()
            if not tables:
                logger.info("[SyncTicks] Creating ticks_raw table...")
                conn.execute("""
                    CREATE TABLE ticks_raw (
                        timestamp TIMESTAMP,
                        instrument_id VARCHAR,
                        last_price DOUBLE,
                        volume BIGINT,
                        open_interest DOUBLE,
                        bid_price DOUBLE,
                        ask_price DOUBLE,
                        date DATE,
                        option_type VARCHAR,
                        strike_price DOUBLE,
                        is_otm BOOLEAN,
                        sync_status VARCHAR,
                        future_sync_status VARCHAR,
                        is_same_rise BOOLEAN,
                        is_same_fall BOOLEAN,
                        is_diff_sync BOOLEAN
                    )
                """)
                logger.info("[SyncTicks] ticks_raw table created")

            # 获取所有期权分表
            option_tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE 'tick_option_%'
            """).fetchall()
            logger.info(f"[SyncTicks] Found {len(option_tables)} option tick tables")

            # 获取所有期货分表
            future_tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE 'tick_future_%'
            """).fetchall()
            logger.info(f"[SyncTicks] Found {len(future_tables)} future tick tables")

            total_synced = 0

            # 同步期权数据
            if option_tables:
                logger.info(f"[SyncTicks] Syncing {len(option_tables)} option tables...")
                for table in option_tables:
                    table_name = table[0]
                    try:
                        option_data = conn.execute(f"""
                            SELECT 
                                t.timestamp, oi.instrument_id, t.last_price, t.volume, t.open_interest,
                                t.bid_price1 as bid_price, t.ask_price1 as ask_price,
                                CAST(t.timestamp AS DATE) as date,
                                oi.option_type, oi.strike_price,
                                NULL as is_otm, NULL as sync_status, 
                                NULL as future_sync_status, NULL as is_same_rise,
                                NULL as is_same_fall, NULL as is_diff_sync
                            FROM {table_name} t
                            JOIN option_instruments oi ON oi.tick_table = '{table_name}'
                        """).fetch_arrow_table()
                        
                        if option_data.num_rows > 0:
                            dict_list = option_data.to_pylist()
                            self.batch_insert_ticks(dict_list, use_arrow=True)
                            total_synced += option_data.num_rows
                    except Exception as e:
                        logger.error(f"[SyncTicks] Failed to sync {table_name}: {e}")

            # 同步期货数据
            if future_tables:
                logger.info(f"[SyncTicks] Syncing {len(future_tables)} future tables...")
                for table in future_tables:
                    table_name = table[0]
                    try:
                        # 查询期货分表数据并关联元数据
                        future_data = conn.execute(f"""
                            SELECT 
                                t.timestamp, fi.instrument_id, t.last_price, t.volume, t.open_interest,
                                t.bid_price1 as bid_price, t.ask_price1 as ask_price,
                                CAST(t.timestamp AS DATE) as date,
                                NULL as option_type, NULL as strike_price,
                                NULL as is_otm, NULL as sync_status, 
                                NULL as future_sync_status, NULL as is_same_rise,
                                NULL as is_same_fall, NULL as is_diff_sync
                            FROM {table_name} t
                            JOIN futures_instruments fi ON fi.tick_table = '{table_name}'
                        """).fetch_arrow_table()
                        
                        if future_data.num_rows > 0:
                            dict_list = future_data.to_pylist()
                            self.batch_insert_ticks(dict_list, use_arrow=True)
                            total_synced += future_data.num_rows
                    except Exception as e:
                        logger.error(f"[SyncTicks] Failed to sync {table_name}: {e}")

            logger.info(
                "[SyncTicks] ✅ Synced %s records to ticks_raw in %.3fs",
                f"{total_synced:,}",
                time.perf_counter() - started_at,
            )
            return total_synced
        finally:
            if external_conn is not None:
                external_conn.close()
            self._tick_sync_lock.release()


# ============================================================================
# 全局单例与便捷函数
# ============================================================================

# P2 Bug #106修复：模块级锁在import时立即创建，避免延迟创建的竞态条件
_data_service_instance: Optional[DataService] = None
_data_service_lock = threading.Lock()


def get_data_service() -> DataService:
    """获取 DataService 单例，避免多个实例竞争初始化 ParamsService"""
    global _data_service_instance
    import logging
    
    # P2 Bug #106修复：使用模块级锁（已在import时创建），消除竞态条件
    with _data_service_lock:
        if _data_service_instance is None:
            _data_service_instance = DataService()
            # 诊断日志：确认实际加载的模块路径
            import ali2026v3_trading.params_service as ps_module
            ps_module_path = ps_module.__file__
            logging.info(f"[DataService] ParamsService module path: {ps_module_path}")
            logging.info(f"[DataService] _params_service_instance id: {id(ps_module._params_service_instance)}")
            if ps_module._params_service_instance is not None:
                logging.info(f"[DataService] _params_service_instance._instrument_cache size: {len(ps_module._params_service_instance._instrument_cache)}")
            else:
                logging.info("[DataService] _params_service_instance is None")
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

# ✅ 优化 3 & 4: 批量插入和增量加载便捷函数
def batch_insert_ticks(ticks_data: List[Dict[str, Any]], use_arrow: bool = True) -> int:
    """批量插入 Tick 数据（便捷函数）"""
    return get_data_service().batch_insert_ticks(ticks_data, use_arrow)

def incremental_load(new_parquet_path: str) -> Dict[str, Any]:
    """增量加载新数据（便捷函数）"""
    return get_data_service().incremental_load(new_parquet_path)

# ✅ ID唯一：模块级clear_cache委托给DataService实例方法，唯一实现
def clear_cache():
    data_service.clear_cache()


# ============================================================================
# 示例
# ============================================================================
if __name__ == '__main__':
    print(explain("SELECT * FROM ticks_raw LIMIT 10"))
    price = get_latest_price('IF2605')
    print(f"Latest price: {price}")