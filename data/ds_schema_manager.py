# MODULE_ID: M1-028
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
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.shared_utils import sanitize_sql_identifier, sanitize_sql_value

logger = logging.getLogger(__name__)

def _get_duckdb():
    """延迟导入DuckDB，避免模块级加载触发崩溃"""
    try:
        import duckdb
        return duckdb
    except ImportError:
        return None


class SchemaManagerMixin:
    """数据库Schema初始化与迁移Mixin - 由DataService组合使用"""

    SCHEMA_VERSION = '2.0'
    DATA_SCHEMA_VERSION = '1.0'
    CHECKPOINT_SCHEMA_VERSION = '1.0'

    def get_schema_version(self) -> str:
        return self.SCHEMA_VERSION

    def _load_or_create_table(self, conn=None) -> bool:
        _conn_provided = conn is not None
        if conn is None:
            conn = self._get_connection()
        try:
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

            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS ticks_raw AS
                    SELECT * FROM temp_ticks
                    ORDER BY timestamp, instrument_id
                """)
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info("Table 'ticks_raw' already exists, skipping creation.")
                else:
                    raise
            try:
                conn.execute("DROP TABLE temp_ticks")
            except Exception:
                pass
            self._table_initialized = True
            logger.info("Table created and loaded.")
            return True
        finally:
            if not _conn_provided:
                self._return_connection(conn)

    def _create_empty_table(self, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ticks_raw (
                timestamp TIMESTAMP,
                instrument_id VARCHAR,
                exchange VARCHAR,
                last_price DOUBLE,
                volume BIGINT,
                open_interest DOUBLE,
                turnover DOUBLE,
                bid_price DOUBLE,
                ask_price DOUBLE,
                bid_volume BIGINT,
                ask_volume BIGINT,
                bid_price2 DOUBLE,
                ask_price2 DOUBLE,
                bid_volume2 BIGINT,
                ask_volume2 BIGINT,
                bid_price3 DOUBLE,
                ask_price3 DOUBLE,
                bid_volume3 BIGINT,
                ask_volume3 BIGINT,
                bid_price4 DOUBLE,
                ask_price4 DOUBLE,
                bid_volume4 BIGINT,
                ask_volume4 BIGINT,
                bid_price5 DOUBLE,
                ask_price5 DOUBLE,
                bid_volume5 BIGINT,
                ask_volume5 BIGINT,
                date DATE,
                option_type VARCHAR,
                strike_price DOUBLE,
                is_otm BOOLEAN,
                sync_status VARCHAR,
                future_sync_status VARCHAR,
                is_same_rise BOOLEAN,
                is_same_fall BOOLEAN,
                is_diff_sync BOOLEAN,
                spread_quality INTEGER,
                days_to_expiry INTEGER,
                implied_volatility DOUBLE
            )
        """)
        # 已有表补齐 bid_volume/ask_volume 列
        try:
            conn.execute("ALTER TABLE ticks_raw ADD COLUMN IF NOT EXISTS bid_volume BIGINT")
            conn.execute("ALTER TABLE ticks_raw ADD COLUMN IF NOT EXISTS ask_volume BIGINT")
        except Exception:
            pass
        # 创建唯一索引以支持 ON CONFLICT (instrument_id, timestamp) DO NOTHING 去重
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_raw_inst_ts
            ON ticks_raw(instrument_id, timestamp)
        """)
        logger.debug("Empty table created with option and future status columns.")

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
            CREATE TABLE IF NOT EXISTS futures_instruments (
                internal_id BIGINT PRIMARY KEY,
                instrument_id VARCHAR UNIQUE,
                product VARCHAR,
                exchange VARCHAR,
                year_month VARCHAR,
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
                is_active BOOLEAN
            )
        """)

        logger.debug("Created product and instrument tables.")

    def _ensure_core_schema_on_connect(self, conn):
        """FIX-20260703: 确保每个新连接上 ticks_raw 表和 latest_prices 视图存在。

        根因: 单连接模式下，当连接因 changes() 错误或其他原因被标记不健康后，
        _get_connection 会销毁旧连接并创建新连接。如果 DB_FILE 是 ':memory:' 或
        表创建未持久化，新连接上将不存在 ticks_raw 和 latest_prices，导致：
        1. 所有 tick 写入失败 ("Table with name ticks_raw does not exist")
        2. 所有价格查询失败 ("Table with name latest_prices does not exist")
        3. 平仓下单无法获取价格 → platform_rejected → 策略卡住

        此方法在 _create_new_connection 中调用，确保核心 schema 始终存在。
        幂等: CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW。
        """
        try:
            _exists = conn.execute(
                "SELECT table_name FROM duckdb_tables WHERE table_name='ticks_raw'"
            ).fetchone()
            if not _exists:
                self._create_empty_table(conn)
                logger.debug("[FIX-20260703] ticks_raw 表在新连接上创建成功")
        except Exception as e:
            logger.warning("[FIX-20260703] 检查/创建 ticks_raw 表失败: %s", e)

        try:
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
                    WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
                ) WHERE rn = 1
            """)
            logger.debug("[FIX-20260703] latest_prices 视图在新连接上已确保存在")
        except Exception as e:
            logger.warning("[FIX-20260703] 创建 latest_prices 视图失败: %s", e)

        # FIX-20260706-KLINES-NEWCONN: 新连接上确保klines_raw表存在
        # 根因: _ensure_core_schema_on_connect仅创建ticks_raw+latest_prices，不创建klines_raw
        # 当20合约诊断线程创建独立DuckDB连接时，klines_raw不存在导致refresh_chain_coverage_audit WARNING
        # FIX-20260706-KLINES-SCHEMA( Fix 60): 修正schema缺失trade_date/open_interest列
        #   9:58 LIVE日志Binder Error "Referenced table 'k' not found"根因:
        #   旧Fix58创建的klines_raw缺少trade_date和open_interest列,
        #   ensure_config_coverage_for_today的INSERT引用trade_date时DuckDB binder报错
        #   (binder在解析NOT EXISTS子查询引用不存在列时报"Referenced table k not found")
        #   schema必须与ds_schema_manager.py:_create_metadata_tables中klines_raw定义一致
        try:
            _klines_exists = conn.execute(
                "SELECT table_name FROM duckdb_tables WHERE table_name='klines_raw'"
            ).fetchone()
            if not _klines_exists:
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
                        trade_date DATE,
                        period VARCHAR DEFAULT 'M1'
                    )
                """)
                logger.debug("[FIX-20260706] klines_raw 表在新连接上创建成功(schema含trade_date/open_interest)")
            else:
                # Fix 60: 修复旧Fix58创建的缺列klines_raw (CREATE TABLE IF NOT EXISTS不会修改已有表)
                # 9:58 LIVE日志Binder Error根因: 旧Fix58创建的klines_raw缺trade_date/open_interest
                _existing_cols = {r[1] for r in conn.execute("PRAGMA table_info('klines_raw')").fetchall()}
                for _col, _type in (('open_interest', 'DOUBLE'), ('trade_date', 'DATE')):
                    if _col not in _existing_cols:
                        try:
                            conn.execute(f"ALTER TABLE klines_raw ADD COLUMN {_col} {_type}")
                            logger.info("[FIX-20260706-KLINES-SCHEMA] klines_raw补充列: %s %s", _col, _type)
                        except Exception as _col_err:
                            logger.debug("[FIX-20260706-KLINES-SCHEMA] klines_raw补充列%s失败(可能已存在): %s", _col, _col_err)
        except Exception as e:
            logger.warning("[FIX-20260706] 检查/创建 klines_raw 表失败: %s", e)

        # FIX-20260707-METADATA: 新连接上确保元数据表存在
        # 根因: _ensure_core_schema_on_connect仅创建ticks_raw+latest_prices+klines_raw，
        # 当连接因不健康被销毁重建后，instruments_registry/futures_instruments/option_instruments
        # 表不存在→_get_instrument_info返回None→K线加载_get_instrument_info失败→success=0
        # 同时导致health_monitor/subscription_service/ds_data_writer查询Catalog Error
        _metadata_tables = {
            'instruments_registry': """
                CREATE TABLE IF NOT EXISTS instruments_registry (
                    instrument_id VARCHAR PRIMARY KEY,
                    product VARCHAR, exchange VARCHAR, year_month VARCHAR,
                    internal_id INTEGER, option_type VARCHAR, strike_price DOUBLE,
                    underlying_future_id INTEGER, registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
            'future_products': """
                CREATE TABLE IF NOT EXISTS future_products (
                    product VARCHAR PRIMARY KEY, exchange VARCHAR, format_template VARCHAR,
                    tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN
                )""",
            'option_products': """
                CREATE TABLE IF NOT EXISTS option_products (
                    product VARCHAR PRIMARY KEY, exchange VARCHAR, underlying_product VARCHAR,
                    format_template VARCHAR, tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN
                )""",
            'futures_instruments': """
                CREATE TABLE IF NOT EXISTS futures_instruments (
                    internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE,
                    product VARCHAR, exchange VARCHAR, year_month VARCHAR, is_active BOOLEAN
                )""",
            'option_instruments': """
                CREATE TABLE IF NOT EXISTS option_instruments (
                    internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE,
                    product VARCHAR, exchange VARCHAR, underlying_future_id INTEGER,
                    underlying_product VARCHAR, year_month VARCHAR, option_type VARCHAR,
                    strike_price DOUBLE, is_active BOOLEAN, format VARCHAR,
                    expire_date VARCHAR, listing_date VARCHAR,
                    kline_table VARCHAR, tick_table VARCHAR
                )""",
        }
        for _tname, _ddl in _metadata_tables.items():
            try:
                _exists = conn.execute(
                    "SELECT table_name FROM duckdb_tables WHERE table_name=?", [_tname]
                ).fetchone()
                if not _exists:
                    conn.execute(_ddl)
                    logger.debug("[FIX-20260707] %s 表在新连接上创建成功", _tname)
            except Exception as e:
                logger.warning("[FIX-20260707] 检查/创建 %s 表失败: %s", _tname, e)

    def _create_metadata_tables(self, conn=None):
        import os

        _conn_provided = conn is not None
        if conn is None:
            conn = self._get_connection()
        logger.info("[DataService] 检查并创建元数据表（如不存在）")

        conn.execute("CREATE SEQUENCE IF NOT EXISTS instrument_id_seq START 1")

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
            CREATE TABLE IF NOT EXISTS futures_instruments (
                internal_id BIGINT PRIMARY KEY,
                instrument_id VARCHAR UNIQUE,
                product VARCHAR,
                exchange VARCHAR,
                year_month VARCHAR,
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
                trade_date DATE,
                period VARCHAR DEFAULT 'M1'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS chain_coverage_audit (
                audit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                trade_date DATE,
                config_count BIGINT,
                subscription_confirmed_count BIGINT,
                tick_return_count BIGINT,
                tick_buffer_count BIGINT,
                ticks_raw_count BIGINT,
                klines_raw_count BIGINT,
                active_tick_count BIGINT,
                simulated_coverage_tick_count BIGINT,
                simulated_coverage_kline_count BIGINT,
                notes VARCHAR
            )
        """)
        # 已有表补齐 period 列（ALTER TABLE ADD COLUMN IF NOT EXISTS）
        try:
            conn.execute("ALTER TABLE klines_raw ADD COLUMN IF NOT EXISTS period VARCHAR DEFAULT 'M1'")
        except Exception:
            pass
        # 创建唯一索引以支持 ON CONFLICT (internal_id, timestamp, period) DO NOTHING 去重
        try:
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_klines_raw_inst_ts_period
                ON klines_raw(internal_id, timestamp, period)
            """)
        except Exception as _dup_err:
            logger.warning("CREATE UNIQUE INDEX on klines_raw failed (%s), deduplicating...", _dup_err)
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            conn.execute("""
                DELETE FROM klines_raw
                WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM klines_raw
                    GROUP BY internal_id, timestamp, period
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_klines_raw_inst_ts_period
                ON klines_raw(internal_id, timestamp, period)
            """)
            logger.info("Deduplicated klines_raw and created UNIQUE INDEX idx_klines_raw_inst_ts_period")
        logger.info("K-line table created/verified: klines_raw")

        logger.info("[DataService] 元数据表已创建/验证完成")

        # v2.8 §21.3: 排序特征宽表（Feature Table）
        # 回测模式 (mode='research') 写入全字段；生产模式 (mode='production') 仅写入瘦身字段
        # 铁律一：存储介质分离——计算一次，存储全量
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sorter_features (
                    product_id VARCHAR NOT NULL,
                    timestamp DOUBLE NOT NULL,
                    trade_date DATE,
                    -- v2.8 核心字段（四象限正交分解）
                    D DOUBLE,
                    Q DOUBLE,
                    direction VARCHAR,
                    quality_level VARCHAR,
                    direction_strength DOUBLE,
                    signal_quality DOUBLE,
                    -- v2.8 辅助字段（方案一修正 + 方案三保留）
                    confidence DOUBLE,
                    concentration DOUBLE,
                    direction_bias DOUBLE,
                    entropy DOUBLE,
                    participation DOUBLE,
                    -- v2.5 保留字段
                    product_score DOUBLE,
                    product_score_ts DOUBLE,
                    correct_up_pct DOUBLE,
                    tier INTEGER,
                    wilson DOUBLE,
                    coverage DOUBLE,
                    month_count INTEGER,
                    -- v2.7 共振字段
                    resonance_enabled BOOLEAN DEFAULT FALSE,
                    resonance_score DOUBLE,
                    resonance_direction INTEGER,
                    final_score DOUBLE,
                    historical_percentile DOUBLE,
                    product_score_cs DOUBLE,
                    PRIMARY KEY (product_id, timestamp)
                )
            """)
            try:
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sorter_features_date
                    ON sorter_features(trade_date)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sorter_features_direction
                    ON sorter_features(direction)
                """)
            except Exception as idx_err:
                logger.debug("[v2.8] sorter_features index creation note: %s", idx_err)
            logger.info("[v2.8] sorter_features 特征宽表已创建/验证")
        except Exception as e:
            logger.warning("[v2.8] sorter_features 特征宽表创建失败: %s", e)

        if not _conn_provided:
            self._return_connection(conn)

    def verify_referential_integrity(self, conn: duckdb.DuckDBPyConnection = None) -> Dict[str, Any]:
        _conn_provided = conn is not None
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
        finally:
            if not _conn_provided:
                self._return_connection(_conn)
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
            'exchange': 'VARCHAR',
            'turnover': 'DOUBLE',
            'bid_volume': 'BIGINT',
            'ask_volume': 'BIGINT',
            'bid_price2': 'DOUBLE',
            'ask_price2': 'DOUBLE',
            'bid_volume2': 'BIGINT',
            'ask_volume2': 'BIGINT',
            'bid_price3': 'DOUBLE',
            'ask_price3': 'DOUBLE',
            'bid_volume3': 'BIGINT',
            'ask_volume3': 'BIGINT',
            'bid_price4': 'DOUBLE',
            'ask_price4': 'DOUBLE',
            'bid_volume4': 'BIGINT',
            'ask_volume4': 'BIGINT',
            'bid_price5': 'DOUBLE',
            'ask_price5': 'DOUBLE',
            'bid_volume5': 'BIGINT',
            'ask_volume5': 'BIGINT',
            'spread_quality': 'INTEGER',
            'days_to_expiry': 'INTEGER',
            'implied_volatility': 'DOUBLE',
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
        from ali2026v3_trading.infra.shared_utils import ShardRouter
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
        改为Python层委托SubscriptionManager.parse_option解析，原样ID直通。'
        """
        from ali2026v3_trading.infra.subscription_service import SubscriptionManager

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

    def _create_indexes_and_views(self, conn=None):
        _conn_provided = conn is not None
        if conn is None:
            conn = self._get_connection()

        try:
            conn.execute("ALTER TABLE ticks_raw ADD COLUMN IF NOT EXISTS bid_volume BIGINT")
            conn.execute("ALTER TABLE ticks_raw ADD COLUMN IF NOT EXISTS ask_volume BIGINT")
        except Exception:
            pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_instrument ON ticks_raw (instrument_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON ticks_raw (timestamp)")

        # 创建唯一索引以支持 ON CONFLICT (instrument_id, timestamp) DO NOTHING 去重
        # 必须在 _create_indexes_and_views 中创建（而非仅在 _create_empty_table），
        # 因为已存在的表不会触发 _create_empty_table，但 _initialize 总会调用本方法
        try:
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_raw_inst_ts
                ON ticks_raw(instrument_id, timestamp)
            """)
        except Exception as _dup_err:
            # 已有重复数据时，先去重再创建唯一索引
            logger.warning("CREATE UNIQUE INDEX failed (%s), deduplicating ticks_raw...", _dup_err)
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            conn.execute("""
                DELETE FROM ticks_raw
                WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM ticks_raw
                    GROUP BY instrument_id, timestamp
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_raw_inst_ts
                ON ticks_raw(instrument_id, timestamp)
            """)
            logger.info("Deduplicated ticks_raw and created UNIQUE INDEX idx_ticks_raw_inst_ts")

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
                WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
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
                WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
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
                WHERE COALESCE(sync_status, '') <> 'simulated_coverage'
            ) WHERE rn = 1
        """)
        logger.info("Created view: latest_prices")

        logger.info("Creating materialized view: option_sync_otm_stats (physical table implementation)")
        # P1-9修复: OTM同步统计表，已有SELECT消费(cascade_judge.get_otm_sync_penalty)
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

        if os.environ.get('ALI2026_REFRESH_OPTION_SYNC_ON_INIT') == '1':
            self._refresh_option_sync_stats()
            logger.info("Initial data loaded into option_sync_otm_stats")

        if os.environ.get('ALI2026_UPDATE_TICK_STATUS_ON_INIT') == '1':
            self._update_option_status_columns(conn)

        try:
            conn.execute("ANALYZE ticks_raw")
            logger.info("Statistics collected for query optimizer")
        except Exception as e:
            logger.warning(f"Failed to collect statistics: {e}")
        finally:
            if not _conn_provided:
                self._return_connection(conn)
