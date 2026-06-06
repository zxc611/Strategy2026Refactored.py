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
try:
    import duckdb
except ImportError:
    duckdb = None
import logging
import os
from datetime import datetime
from typing import Any, Dict
from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.shared_utils import sanitize_sql_identifier, sanitize_sql_value

logger = logging.getLogger(__name__)


class SchemaManagerMixin:
    """数据库Schema初始化与迁移Mixin - 由DataService组合使用"""

    SCHEMA_VERSION = '2.0'
    DATA_SCHEMA_VERSION = '1.0'
    CHECKPOINT_SCHEMA_VERSION = '1.0'

    def get_schema_version(self) -> str:
        return self.SCHEMA_VERSION

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
                FROM read_parquet({sanitize_sql_value(self.PARQUET_PATH)}, union_by_name=false)
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
                open_interest DOUBLE,  -- P1-8: 预留BI查询字段，生产SELECT引用存在
                bid_price DOUBLE,  -- P1-8: 预留BI查询字段，仅测试SELECT引用
                ask_price DOUBLE,  -- P1-8: 预留BI查询字段，仅测试SELECT引用
                date DATE,
                option_type VARCHAR,
                strike_price DOUBLE,
                is_otm BOOLEAN,  -- P1-8修复: OTM标记，已有SELECT消费(risk_compute_service.compute_otm_sync_exposure)
                sync_status VARCHAR,
                future_sync_status VARCHAR,  -- P1-8修复: 期货同步状态，已有SELECT消费(risk_compute_service.compute_otm_sync_exposure)
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
            existing_cols = {row[0] for row in conn.execute(f"DESCRIBE {sanitize_sql_identifier(table_name)}").fetchall()}
            for col_name, col_type in [('format', 'VARCHAR'), ('expire_date', 'VARCHAR'),
                                        ('listing_date', 'VARCHAR'), ('kline_table', 'VARCHAR'),
                                        ('tick_table', 'VARCHAR'),
                                        ('product_code', 'VARCHAR'), ('shard_key', 'BIGINT')]:
                if col_name not in existing_cols:
                    conn.execute(f"ALTER TABLE {sanitize_sql_identifier(table_name)} ADD COLUMN {sanitize_sql_identifier(col_name)} {col_type}")
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

    def verify_referential_integrity(self, conn: duckdb.DuckDBPyConnection = None) -> Dict[str, Any]:
        """R26-P0-DI-07修复: 引用完整性验证——检查订单/信号/持仓记录是否引用了不存在的合约/策略

        Returns:
            Dict: {is_valid: bool, violations: [{table, column, orphan_count, sample_ids}]}
        """
        _conn = conn or getattr(self, '_conn', None)
        if _conn is None:
            _conn = self._get_connection()
        result = {'is_valid': True, 'violations': []}
        try:
            integrity_checks = [
                {
                    'child_table': 'klines_raw',
                    'child_column': 'internal_id',
                    'parent_table': 'option_instruments',
                    'parent_column': 'internal_id',
                    'description': 'klines_raw引用不存在的instrument'
                },
                {
                    'child_table': 'klines_raw',
                    'child_column': 'internal_id',
                    'parent_table': 'futures_instruments',
                    'parent_column': 'internal_id',
                    'description': 'klines_raw引用不存在的future_instrument'
                },
            ]
            _ALLOWED_TABLES = frozenset(['klines_raw', 'option_instruments', 'futures_instruments',
                                          'ticks_raw', 'signals', 'orders', 'positions'])
            _ALLOWED_COLS = frozenset(['internal_id', 'instrument_id', 'strategy_id', 'signal_id', 'order_id'])
            for check in integrity_checks:
                try:
                    child_tbl = check['child_table']
                    child_col = check['child_column']
                    parent_tbl = check['parent_table']
                    parent_col = check['parent_column']
                    # R27-P2-HZ-03: 标识符白名单校验(防止SQL注入, 虽值来自hardcoded字典)
                    for _tbl in (child_tbl, parent_tbl):
                        if _tbl not in _ALLOWED_TABLES:
                            raise ValueError(f"verify_referential_integrity: 不允许的表名 '{_tbl}'")
                    for _col in (child_col, parent_col):
                        if _col not in _ALLOWED_COLS:
                            raise ValueError(f"verify_referential_integrity: 不允许的列名 '{_col}'")
                    sql = f"""SELECT c.{child_col}, COUNT(*) as cnt
                             FROM {child_tbl} c
                             LEFT JOIN {parent_tbl} p ON c.{child_col} = p.{parent_col}
                             WHERE p.{parent_col} IS NULL
                             GROUP BY c.{child_col}
                             LIMIT 10"""
                    orphan_rows = _conn.execute(sql).fetchall()
                    if orphan_rows:
                        orphan_count = sum(r[1] for r in orphan_rows)
                        sample_ids = [r[0] for r in orphan_rows[:5]]
                        result['violations'].append({
                            'table': child_tbl,
                            'column': child_col,
                            'orphan_count': orphan_count,
                            'sample_ids': sample_ids,
                            'description': check['description']
                        })
                        result['is_valid'] = False
                except Exception as e:
                    logger.warning("[R26-P0-DI-07] 完整性检查跳过(%s→%s): %s", child_tbl, parent_tbl, e)
            if not result['is_valid']:
                total_orphans = sum(v['orphan_count'] for v in result['violations'])
                logger.warning("[R26-P0-DI-07] 引用完整性违反: %d组孤立记录, 共%d条",
                              len(result['violations']), total_orphans)
        except Exception as e:
            logger.error("[R26-P0-DI-07] 引用完整性验证异常: %s", e)
            result['is_valid'] = False
        return result

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
            conn.execute(f"ALTER TABLE ticks_raw ADD COLUMN {sanitize_sql_identifier(column_name)} {column_type}")
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
                    f"SELECT internal_id, product FROM {sanitize_sql_identifier(table_name)} WHERE shard_key IS NULL"
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

        renamed = 0
        dropped = 0
        try:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tick_future_%' OR table_name LIKE 'tick_option_%' OR table_name LIKE 'kline_future_%' OR table_name LIKE 'kline_option_%'"
            ).fetchall()
            legacy_tables_to_drop = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '_legacy_%'"
            ).fetchall()
            for (table_name,) in legacy_tables_to_drop:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {sanitize_sql_identifier(table_name)}")
                    dropped += 1
                    logger.info("[DataService] Dropped previously renamed legacy table: %s", table_name)
                except Exception as e:
                    logger.warning("[DataService] Failed to drop %s: %s", table_name, e)
            for (table_name,) in tables:
                try:
                    renamed_name = f"_legacy_{table_name}"
                    conn.execute(f"ALTER TABLE {sanitize_sql_identifier(table_name)} RENAME TO {sanitize_sql_identifier(renamed_name)}")
                    renamed += 1
                    logger.info("[DataService] Renamed legacy table: %s -> %s (pending next-startup drop)", table_name, renamed_name)
                except Exception as e:
                    logger.warning("[DataService] Failed to rename %s: %s", table_name, e)
        except Exception as e:
            logger.warning("[DataService] _cleanup_legacy_tables scan failed: %s", e)

        try:
            conn.execute(
                "INSERT OR REPLACE INTO app_kv_store (key, value, updated_at) VALUES (?, ?, ?)",
                ['schema_version_shard_key', '1', datetime.now(CHINA_TZ).isoformat()],
            )
        except Exception:
            pass

        if renamed > 0 or dropped > 0:
            logger.info("[DataService] _cleanup_legacy_tables: renamed %d, dropped %d legacy per-instrument tables", renamed, dropped)

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

        # P1-10修复: 移除idx_instrument_time，idx_covering_price已覆盖(instrument_id, timestamp)前缀
        # conn.execute("""
        #     CREATE INDEX IF NOT EXISTS idx_instrument_time
        #     ON ticks_raw (instrument_id, timestamp DESC)
        # """)

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
        # P1-9修复: OTM同步统计表，已有SELECT消费(cascade_judge.get_otm_sync_penalty)
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
