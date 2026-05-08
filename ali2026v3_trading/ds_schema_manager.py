"""ds_schema_manager.py - 数据库Schema初始化与迁移Mixin

从data_service.py拆分出的Schema管理职责，包括：
- _load_or_create_table: 加载Parquet或创建空表
- _create_empty_table: 创建空ticks_raw表和元数据表
- _create_metadata_tables: 创建/升级元数据表
- _ensure_ticks_raw_schema: 兼容旧版Schema补齐列
- _backfill_shard_key: 回填shard_key
- _cleanup_legacy_tables: 清理遗留分表
- _backfill_option_metadata: 回填期权元数据
- _execute_backfill_batch: 批量回填UPDATE
- _create_indexes_and_views: 创建索引和视图
"""
import duckdb
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class SchemaManagerMixin:
    """数据库Schema初始化与迁移Mixin - 由DataService组合使用"""

    def _load_or_create_table(self) -> bool:
        """加载 Parquet 数据到表。返回 True 表示成功加载/已存在，False 表示文件不存在需创建空表。"""
        conn = self._get_connection()
        res = conn.execute("SELECT table_name FROM duckdb_tables WHERE table_name='ticks_raw'").fetchone()
        if res:
            logger.info("Table 'ticks_raw' already exists.")
            self._table_initialized = True
            return True

        if not os.path.exists(self.PARQUET_PATH):
            logger.warning(f"Parquet file not found: {self.PARQUET_PATH}")
            return False

        logger.info(f"Loading Parquet from {self.PARQUET_PATH}...")
        try:
            conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE temp_ticks AS
                SELECT *, CAST(timestamp AS DATE) AS date
                FROM read_parquet('{self.PARQUET_PATH}', union_by_name=false)
            """)
        except Exception as e:
            logger.error(f"Failed to read Parquet: {e}. Please ensure column names match.")
            raise

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
                underlying_future_id INTEGER,
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

        使用CREATE TABLE IF NOT EXISTS，避免每次DROP所有表导致数据丢失

        目的：解决品种配置加载依赖问题 - ensure_products_with_retry 需要在 Storage 初始化之前工作。
        """
        import os

        conn = self._get_connection()

        logger.info("[DataService] 检查并创建元数据表（如不存在）")

        conn.execute("CREATE SEQUENCE IF NOT EXISTS instrument_id_seq START 1")

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

        for table_name in ('futures_instruments', 'option_instruments'):
            existing_cols = {row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}
            for col_name, col_type in [('format', 'VARCHAR'), ('expire_date', 'VARCHAR'),
                                        ('listing_date', 'VARCHAR'), ('kline_table', 'VARCHAR'),
                                        ('tick_table', 'VARCHAR'),
                                        ('product_code', 'VARCHAR'), ('shard_key', 'BIGINT')]:
                if col_name not in existing_cols:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
                    logger.info("[DataService] Added column %s to %s", col_name, table_name)

        self._backfill_shard_key(conn)

        self._cleanup_legacy_tables(conn)

        logger.info("[DataService] 元数据表检查/创建完成")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS klines_raw (
                internal_id BIGINT,
                instrument_type VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                open_interest DOUBLE,
                trade_date DATE
            )
        """)
        logger.info("K-line table created/verified: klines_raw")

        logger.info("[DataService] 元数据表已创建/验证完成")

    def _ensure_ticks_raw_schema(self, conn: duckdb.DuckDBPyConnection):
        """兼容旧版 DuckDB：为 ticks_raw 自动补齐新版本依赖的列。"""
        existing_columns = {
            row[0]
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

    def _backfill_shard_key(self, conn: duckdb.DuckDBPyConnection):
        from ali2026v3_trading.shared_utils import ShardRouter
        backfilled = 0
        for table_name in ('futures_instruments', 'option_instruments'):
            try:
                rows = conn.execute(
                    f"SELECT internal_id, product FROM {table_name} WHERE shard_key IS NULL"
                ).fetchall()
                if not rows:
                    continue
                for row in rows:
                    internal_id = row[0]
                    product = row[1]
                    if not product:
                        continue
                    product_code = product.lower()
                    shard_key = ShardRouter._deterministic_hash(product_code)
                    conn.execute(
                        f"UPDATE {table_name} SET product_code = ?, shard_key = ? WHERE internal_id = ?",
                        [product_code, shard_key, internal_id],
                    )
                    backfilled += 1
            except Exception as e:
                logger.warning("[DataService] _backfill_shard_key %s failed: %s", table_name, e)
        if backfilled > 0:
            logger.info("[DataService] _backfill_shard_key: backfilled %d rows", backfilled)

    def _cleanup_legacy_tables(self, conn: duckdb.DuckDBPyConnection):
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS app_kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)
        except Exception:
            pass

        try:
            rows = conn.execute(
                "SELECT value FROM app_kv_store WHERE key = 'schema_version_shard_key'"
            ).fetchall()
            if rows:
                return
        except Exception:
            pass

        dropped = 0
        try:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tick_future_%' OR table_name LIKE 'tick_option_%' OR table_name LIKE 'kline_future_%' OR table_name LIKE 'kline_option_%'"
            ).fetchall()
            for (table_name,) in tables:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    dropped += 1
                    logger.info("[DataService] Dropped legacy table: %s", table_name)
                except Exception as e:
                    logger.warning("[DataService] Failed to drop %s: %s", table_name, e)
        except Exception as e:
            logger.warning("[DataService] _cleanup_legacy_tables scan failed: %s", e)

        try:
            conn.execute(
                "INSERT OR REPLACE INTO app_kv_store (key, value, updated_at) VALUES (?, ?, ?)",
                ['schema_version_shard_key', '1', datetime.now().isoformat()],
            )
        except Exception:
            pass

        if dropped > 0:
            logger.info("[DataService] _cleanup_legacy_tables: dropped %d legacy per-instrument tables", dropped)

    def _backfill_option_metadata(self, conn: duckdb.DuckDBPyConnection):
        """为旧库中的期权 Tick 回填 option_type/strike_price。

        按"入口解析一次"原则，删除SQL内联正则，
        改为Python层委托SubscriptionManager.parse_option解析，原样ID直通。
        """
        from ali2026v3_trading.subscription_manager import SubscriptionManager

        result = conn.execute(
            "SELECT instrument_id FROM ticks_raw WHERE option_type IS NULL OR strike_price IS NULL"
        ).fetchall()

        if not result:
            return

        batch = []
        for row in result:
            instrument_id = row[0]
            try:
                parsed = SubscriptionManager.parse_option(instrument_id)
                batch.append((
                    parsed['option_type'],
                    parsed['strike_price'],
                    instrument_id,
                ))
            except (ValueError, KeyError):
                continue

            if len(batch) >= 1000:
                self._execute_backfill_batch(conn, batch)
                batch = []

        if batch:
            self._execute_backfill_batch(conn, batch)

    def _execute_backfill_batch(self, conn, batch):
        """批量执行回填UPDATE"""
        for option_type, strike_price, instrument_id in batch:
            conn.execute(
                "UPDATE ticks_raw SET option_type = ?, strike_price = ? WHERE instrument_id = ?",
                [option_type, strike_price, instrument_id],
            )

    def _create_indexes_and_views(self):
        conn = self._get_connection()
        conn.execute("CREATE INDEX IF NOT EXISTS idx_instrument ON ticks_raw (instrument_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON ticks_raw (timestamp)")

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_instrument_time
            ON ticks_raw (instrument_id, timestamp DESC)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_covering_price
            ON ticks_raw (instrument_id, timestamp, last_price, volume, open_interest)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date_instrument
            ON ticks_raw (date, instrument_id)
        """)

        if self.PREAGGREGATE_DAILY:
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

        if self.PREAGGREGATE_SYMBOL_DAILY:
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

        logger.info("Creating materialized view: option_sync_otm_stats (physical table implementation)")

        conn.execute("DROP TABLE IF EXISTS option_sync_otm_stats")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS option_sync_otm_stats (
                month VARCHAR,
                underlying_symbol VARCHAR,
                option_type VARCHAR,
                correct_rise_otm_count BIGINT,
                wrong_rise_otm_count BIGINT,
                correct_fall_otm_count BIGINT,
                wrong_fall_otm_count BIGINT,
                other_otm_count BIGINT,
                total_otm_count BIGINT,
                total_samples BIGINT,
                calculated_at TIMESTAMP
            )
        """)
        logger.info("Created physical table: option_sync_otm_stats")

        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_option_stats_month ON option_sync_otm_stats(month)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_option_stats_symbol ON option_sync_otm_stats(underlying_symbol)")
            logger.info("Created indexes on option_sync_otm_stats")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")

        self._refresh_option_sync_stats()
        logger.info("Initial data loaded into option_sync_otm_stats")

        self._update_option_status_columns(conn)

        try:
            conn.execute("ANALYZE ticks_raw")
            logger.info("Statistics collected for query optimizer")
        except Exception as e:
            logger.warning(f"Failed to collect statistics: {e}")
