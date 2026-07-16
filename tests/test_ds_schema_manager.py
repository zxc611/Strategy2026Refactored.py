# MODULE_ID: M2-337
"""测试 data/ds_schema_manager.py — Schema迁移"""
from unittest.mock import MagicMock

import pytest


# ============================================================
# Schema版本
# ============================================================

class TestSchemaVersion:
    """Schema版本号"""

    def test_schema_version_is_2_0(self):
        """Schema版本为2.0"""
        from data.ds_schema_manager import SchemaManagerMixin
        mixin = SchemaManagerMixin()
        assert mixin.get_schema_version() == '2.0'

    def test_data_schema_version(self):
        """DATA_SCHEMA_VERSION为1.0"""
        from data.ds_schema_manager import SchemaManagerMixin
        assert SchemaManagerMixin.DATA_SCHEMA_VERSION == '1.0'

    def test_checkpoint_schema_version(self):
        """CHECKPOINT_SCHEMA_VERSION为1.0"""
        from data.ds_schema_manager import SchemaManagerMixin
        assert SchemaManagerMixin.CHECKPOINT_SCHEMA_VERSION == '1.0'


# ============================================================
# _ensure_ticks_raw_schema
# ============================================================

class TestEnsureTicksRawSchema:
    """_ensure_ticks_raw_schema: Schema补齐"""

    def test_adds_missing_columns(self):
        """补齐缺失列"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE ticks_raw (
                timestamp TIMESTAMP, instrument_id VARCHAR, last_price DOUBLE,
                volume BIGINT, date DATE
            )
        """)
        conn.execute("INSERT INTO ticks_raw VALUES (CURRENT_TIMESTAMP, 'test', 100.0, 1, CURRENT_DATE)")

        mixin = SchemaManagerMixin()
        mixin._backfill_option_metadata = MagicMock()
        mixin._ensure_ticks_raw_schema(conn)

        columns = {row[0] for row in conn.execute("DESCRIBE ticks_raw").fetchall()}
        expected_cols = {'option_type', 'strike_price', 'is_otm', 'sync_status',
                         'future_sync_status', 'is_same_rise', 'is_same_fall', 'is_diff_sync'}
        for col in expected_cols:
            assert col in columns, f"列{col}未添加"
        conn.close()

    def test_idempotent(self):
        """幂等: 多次调用不出错"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE ticks_raw (
                timestamp TIMESTAMP, instrument_id VARCHAR, last_price DOUBLE,
                volume BIGINT, date DATE
            )
        """)

        mixin = SchemaManagerMixin()
        mixin._backfill_option_metadata = MagicMock()
        mixin._ensure_ticks_raw_schema(conn)
        mixin._ensure_ticks_raw_schema(conn)  # 第二次

        columns = {row[0] for row in conn.execute("DESCRIBE ticks_raw").fetchall()}
        assert 'is_otm' in columns
        conn.close()

    def test_no_duplicate_columns(self):
        """不会创建重复列"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE ticks_raw (
                timestamp TIMESTAMP, instrument_id VARCHAR, last_price DOUBLE,
                volume BIGINT, date DATE, is_otm BOOLEAN
            )
        """)

        mixin = SchemaManagerMixin()
        mixin._backfill_option_metadata = MagicMock()
        mixin._ensure_ticks_raw_schema(conn)

        columns = {row[0] for row in conn.execute("DESCRIBE ticks_raw").fetchall()}
        # is_otm只出现一次
        col_list = [row[0] for row in conn.execute("DESCRIBE ticks_raw").fetchall()]
        assert col_list.count('is_otm') == 1
        conn.close()

    def test_preserves_existing_data(self):
        """迁移保留已有数据"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE ticks_raw (
                timestamp TIMESTAMP, instrument_id VARCHAR, last_price DOUBLE,
                volume BIGINT, date DATE
            )
        """)
        conn.execute("INSERT INTO ticks_raw VALUES (CURRENT_TIMESTAMP, 'test', 100.0, 1, CURRENT_DATE)")

        mixin = SchemaManagerMixin()
        mixin._backfill_option_metadata = MagicMock()
        mixin._ensure_ticks_raw_schema(conn)

        count = conn.execute("SELECT COUNT(*) FROM ticks_raw").fetchone()[0]
        assert count == 1
        conn.close()


# ============================================================
# _create_empty_table
# ============================================================

class TestCreateEmptyTable:
    """_create_empty_table: 创建空表"""

    def test_creates_ticks_raw(self):
        """创建ticks_raw表"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        mixin = SchemaManagerMixin()
        mixin._create_empty_table(conn)

        tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables").fetchall()}
        assert 'ticks_raw' in tables
        conn.close()

    def test_creates_metadata_tables(self):
        """创建元数据表"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        mixin = SchemaManagerMixin()
        mixin._create_empty_table(conn)

        tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables").fetchall()}
        assert 'future_products' in tables
        assert 'option_products' in tables
        assert 'futures_instruments' in tables
        assert 'option_instruments' in tables
        conn.close()

    def test_ticks_raw_has_all_columns(self):
        """ticks_raw包含所有必要列"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        mixin = SchemaManagerMixin()
        mixin._create_empty_table(conn)

        columns = {row[0] for row in conn.execute("DESCRIBE ticks_raw").fetchall()}
        required = {'timestamp', 'instrument_id', 'last_price', 'volume', 'date',
                    'option_type', 'strike_price', 'is_otm', 'sync_status',
                    'future_sync_status', 'is_same_rise', 'is_same_fall', 'is_diff_sync'}
        for col in required:
            assert col in columns, f"列{col}缺失"
        conn.close()


# ============================================================
# _execute_backfill_batch
# ============================================================

class TestExecuteBackfillBatch:
    """_execute_backfill_batch: 批量回填"""

    def test_backfill_updates_rows(self):
        """回填更新行"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE ticks_raw (
                instrument_id VARCHAR, option_type VARCHAR, strike_price DOUBLE
            )
        """)
        conn.execute("INSERT INTO ticks_raw VALUES ('test_C_2700', NULL, NULL)")

        mixin = SchemaManagerMixin()
        batch = [('CALL', 2700.0, 'test_C_2700')]
        mixin._execute_backfill_batch(conn, batch)

        row = conn.execute("SELECT option_type, strike_price FROM ticks_raw WHERE instrument_id = 'test_C_2700'").fetchone()
        assert row[0] == 'CALL'
        assert row[1] == 2700.0
        conn.close()


# ============================================================
# verify_referential_integrity
# ============================================================

class TestVerifyReferentialIntegrity:
    """verify_referential_integrity: 引用完整性验证"""

    def test_returns_valid_structure(self):
        """返回合法结构"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from data.ds_schema_manager import SchemaManagerMixin

        conn = duckdb.connect(':memory:')
        # 创建必要表
        conn.execute("CREATE TABLE option_instruments (internal_id BIGINT PRIMARY KEY)")
        conn.execute("CREATE TABLE futures_instruments (internal_id BIGINT PRIMARY KEY)")
        conn.execute("CREATE TABLE klines_raw (internal_id BIGINT)")

        mixin = SchemaManagerMixin()
        result = mixin.verify_referential_integrity(conn)
        assert 'is_valid' in result
        assert 'violations' in result
        conn.close()
