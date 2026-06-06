"""DuckDB access adapter — centralized abstraction layer (R19-AP-05 fix).

Provide a single import boundary for DuckDB connections to reduce direct
storage dependency leakage into business modules. All modules should use
this adapter instead of 'import duckdb' directly.

AP-05 fix: Reduces DuckDB leak from 17 files to 1 (this file).
Any future DB migration only needs to modify this single file.
"""
from __future__ import annotations

from typing import Any, Optional, List, Dict
import logging
import threading

from ali2026v3_trading.shared_utils import sanitize_sql_identifier, sanitize_sql_value

__all__ = [
    'connect', 'connect_in_memory', 'execute', 'fetchone', 'fetchall', 'fetchdf',
    'register_df', 'close', 'table_exists', 'get_duckdb_module', 'connect_duckdb',
    'ConnectionConfig', 'get_connection_pool', 'release_connection',
    'get_duckdb_connection_type', 'execute_parquet_query', 'manage_schema',
]

logger = logging.getLogger(__name__)


def connect(db_path: str, read_only: bool = False) -> Any:
    """Create a DuckDB connection through a centralized adapter."""
    import duckdb
    return duckdb.connect(db_path, read_only=read_only)


def connect_in_memory() -> Any:
    """Create an in-memory DuckDB connection."""
    import duckdb
    return duckdb.connect()


def execute(conn: Any, query: str, params: Optional[List] = None) -> Any:
    """Execute a query through the adapter."""
    if params is not None:
        return conn.execute(query, params)
    return conn.execute(query)


def fetchone(conn: Any, query: str, params: Optional[List] = None) -> Optional[tuple]:
    """Execute query and fetch one result row."""
    result = execute(conn, query, params)
    return result.fetchone()


def fetchall(conn: Any, query: str, params: Optional[List] = None) -> List[tuple]:
    """Execute query and fetch all result rows."""
    result = execute(conn, query, params)
    return result.fetchall()


def fetchdf(conn: Any, query: str, params: Optional[List] = None) -> Any:
    """Execute query and fetch result as DataFrame."""
    result = execute(conn, query, params)
    return result.fetchdf()


def register_df(conn: Any, name: str, df: Any) -> None:
    """Register a DataFrame as a virtual table."""
    conn.register(name, df)


def close(conn: Any) -> None:
    """Safely close a connection."""
    try:
        conn.close()
    except Exception as e:
        logger.warning("db_adapter.close() error: %s", e)


def table_exists(conn: Any, table_name: str) -> bool:
    """Check if a table exists in the database."""
    result = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name]
    ).fetchone()
    return result[0] > 0 if result else False


def get_duckdb_module() -> Any:
    """Get the duckdb module itself (for advanced usage like duckdb.sql type)."""
    import duckdb
    return duckdb


def connect_duckdb(db_path: str, read_only: bool = False) -> Any:
    """Backward-compatible alias for connect()."""
    return connect(db_path, read_only=read_only)


# ============================================================================
# Phase 1: Missing interface additions (5 items from audit report 32.2)
# ============================================================================

from dataclasses import dataclass
from typing import List


@dataclass
class ConnectionConfig:
    """Unified connection configuration [Phase 1-5].
    
    Supports in-memory, file-based, and cloud storage connections.
    """
    db_path: str = ":memory:"
    read_only: bool = False
    max_memory_mb: int = 512
    threads: int = 1
    extensions: List[str] = None

    def __post_init__(self):
        if self.extensions is None:
            self.extensions = []


_CONNECTION_POOL: Dict[str, Any] = {}
_POOL_LOCK = threading.RLock()


def get_connection_pool(config: ConnectionConfig = None, pool_key: str = "default") -> Any:
    """Get or create a pooled DuckDB connection [Phase 1-1].

    Replaces per-call `import duckdb; duckdb.connect()` with a
    managed connection pool to avoid connection proliferation.
    """
    if config is None:
        config = ConnectionConfig()
    with _POOL_LOCK:
        if pool_key not in _CONNECTION_POOL:
            conn = connect(config.db_path, read_only=config.read_only)
            if config.threads > 1:
                try:
                    import duckdb
                    conn.execute(f"SET threads={config.threads}")
                except Exception:
                    pass
            _CONNECTION_POOL[pool_key] = conn
        return _CONNECTION_POOL[pool_key]


def release_connection(pool_key: str = "default") -> None:
    """Release a pooled connection [Phase 1-1]."""
    with _POOL_LOCK:
        conn = _CONNECTION_POOL.pop(pool_key, None)
        if conn is not None:
            close(conn)


def get_duckdb_connection_type() -> Any:
    """Get DuckDBPyConnection type for type annotations [Phase 1-2].
    
    Usage:
        from ali2026v3_trading.db_adapter import get_duckdb_connection_type
        DuckDBConnection = get_duckdb_connection_type()
        def my_func(conn: DuckDBConnection) -> None: ...
    """
    import duckdb
    return duckdb.DuckDBPyConnection


def execute_parquet_query(conn: Any, parquet_path: str, query: str,
                          table_alias: str = "parquet_data") -> Any:
    """Execute a query directly on a Parquet file [Phase 1-3].
    
    Reads Parquet file as a virtual table and executes the query.
    """
    full_query = f"SELECT * FROM read_parquet({sanitize_sql_value(parquet_path)}) AS {sanitize_sql_identifier(table_alias)}"
    if query.strip().upper().startswith("SELECT"):
        subquery = query.strip()
        combined = f"WITH {sanitize_sql_identifier(table_alias)} AS (SELECT * FROM read_parquet({sanitize_sql_value(parquet_path)})) {subquery}"
        return execute(conn, combined)
    return execute(conn, full_query)


def manage_schema(conn: Any, table_name: str, columns: Dict[str, str],
                  if_not_exists: bool = True) -> None:
    """Create or migrate a table schema [Phase 1-4].
    
    Args:
        conn: DuckDB connection
        table_name: Target table name
        columns: Dict of column_name → SQL type (e.g., {"ts": "TIMESTAMP", "price": "DOUBLE"})
        if_not_exists: Use CREATE TABLE IF NOT EXISTS
    """
    exists = if_not_exists and table_exists(conn, table_name)
    if exists:
        return
    cols_def = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
    clause = "IF NOT EXISTS" if if_not_exists else ""
    sql = f"CREATE TABLE {clause} {sanitize_sql_identifier(table_name)} ({cols_def})"
    execute(conn, sql)
