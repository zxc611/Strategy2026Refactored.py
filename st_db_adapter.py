# MODULE_ID: M2-332
"""测试: data/db_adapter.py"""
import pytest
from ali2026v3_trading.data.db_adapter import (
    connect, connect_in_memory, execute, fetchone, fetchall, fetchdf,
    register_df, close, table_exists, get_duckdb_module, connect_duckdb,
    ConnectionConfig, get_connection_pool, release_connection,
    get_duckdb_connection_type, execute_parquet_query, manage_schema,
    get_db_adapter,
)


class TestConnectionConfig:
    def test_defaults(self):
        cfg = ConnectionConfig()
        assert cfg.db_path == ":memory:"
        assert cfg.read_only is False
        assert cfg.max_memory_mb == 512
        assert cfg.threads == 1
        assert cfg.extensions == []

    def test_custom(self):
        cfg = ConnectionConfig(db_path="/tmp/test.duckdb", read_only=True, threads=4)
        assert cfg.db_path == "/tmp/test.duckdb"
        assert cfg.read_only is True
        assert cfg.threads == 4

    def test_extensions_post_init(self):
        cfg = ConnectionConfig()
        assert isinstance(cfg.extensions, list)
        assert len(cfg.extensions) == 0


class TestDbAdapterFunctions:
    def test_connect_callable(self):
        assert callable(connect)

    def test_connect_in_memory_callable(self):
        assert callable(connect_in_memory)

    def test_execute_callable(self):
        assert callable(execute)

    def test_fetchone_callable(self):
        assert callable(fetchone)

    def test_fetchall_callable(self):
        assert callable(fetchall)

    def test_fetchdf_callable(self):
        assert callable(fetchdf)

    def test_register_df_callable(self):
        assert callable(register_df)

    def test_close_callable(self):
        assert callable(close)

    def test_table_exists_callable(self):
        assert callable(table_exists)

    def test_get_duckdb_module_callable(self):
        assert callable(get_duckdb_module)

    def test_connect_duckdb_callable(self):
        assert callable(connect_duckdb)

    def test_get_duckdb_connection_type_callable(self):
        assert callable(get_duckdb_connection_type)

    def test_execute_parquet_query_callable(self):
        assert callable(execute_parquet_query)

    def test_manage_schema_callable(self):
        assert callable(manage_schema)

    def test_get_db_adapter_callable(self):
        assert callable(get_db_adapter)

    def test_get_connection_pool_callable(self):
        assert callable(get_connection_pool)

    def test_release_connection_callable(self):
        assert callable(release_connection)


class TestDbAdapterInMemory:
    def test_connect_in_memory_and_query(self):
        conn = connect_in_memory()
        assert conn is not None
        result = execute(conn, "SELECT 1 AS val")
        row = result.fetchone()
        assert row[0] == 1
        close(conn)

    def test_fetchone(self):
        conn = connect_in_memory()
        row = fetchone(conn, "SELECT 42 AS answer")
        assert row[0] == 42
        close(conn)

    def test_fetchall(self):
        conn = connect_in_memory()
        rows = fetchall(conn, "SELECT * FROM range(3)")
        assert len(rows) == 3
        close(conn)

    def test_table_exists_false(self):
        conn = connect_in_memory()
        assert table_exists(conn, "nonexistent_table_xyz") is False
        close(conn)

    def test_manage_schema_create_and_check(self):
        conn = connect_in_memory()
        manage_schema(conn, "test_tbl", {"id": "INTEGER", "name": "VARCHAR"}, if_not_exists=True)
        assert table_exists(conn, "test_tbl") is True
        close(conn)

    def test_manage_schema_idempotent(self):
        conn = connect_in_memory()
        manage_schema(conn, "test_tbl2", {"id": "INTEGER"}, if_not_exists=True)
        manage_schema(conn, "test_tbl2", {"id": "INTEGER"}, if_not_exists=True)
        assert table_exists(conn, "test_tbl2") is True
        close(conn)

    def test_get_duckdb_module(self):
        mod = get_duckdb_module()
        assert mod is not None
        assert hasattr(mod, 'connect')

    def test_get_duckdb_connection_type(self):
        t = get_duckdb_connection_type()
        assert t is not None

    def test_connect_duckdb_alias(self):
        conn = connect_duckdb(":memory:")
        assert conn is not None
        close(conn)

    def test_connection_pool_reuse(self):
        cfg = ConnectionConfig()
        c1 = get_connection_pool(config=cfg, pool_key="test_reuse")
        c2 = get_connection_pool(config=cfg, pool_key="test_reuse")
        assert c1 is c2
        release_connection(pool_key="test_reuse")

    def test_release_connection(self):
        cfg = ConnectionConfig()
        get_connection_pool(config=cfg, pool_key="test_release")
        release_connection(pool_key="test_release")

    def test_get_db_adapter(self):
        cfg = ConnectionConfig()
        conn = get_db_adapter(config=cfg, pool_key="test_adapter")
        assert conn is not None
        release_connection(pool_key="test_adapter")

    def test_fetchdf(self):
        conn = connect_in_memory()
        df = fetchdf(conn, "SELECT 1 AS a, 2 AS b")
        assert df is not None
        assert len(df) == 1
        close(conn)

    def test_execute_with_params(self):
        conn = connect_in_memory()
        result = execute(conn, "SELECT ?", [42])
        row = result.fetchone()
        assert row[0] == 42
        close(conn)

    def test_register_df(self):
        import pandas as pd
        conn = connect_in_memory()
        df = pd.DataFrame({"x": [1, 2, 3]})
        register_df(conn, "my_df", df)
        rows = fetchall(conn, "SELECT * FROM my_df")
        assert len(rows) == 3
        close(conn)