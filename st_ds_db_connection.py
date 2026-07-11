# MODULE_ID: M2-335
"""测试 data/ds_db_connection.py — 连接池超时 + 降级"""
import time
from unittest.mock import MagicMock, patch

import pytest


# ============================================================
# _TimedDuckDBConnection
# ============================================================

class TestTimedDuckDBConnection:
    """_TimedDuckDBConnection: 超时保护"""

    def test_successful_query(self):
        """正常查询通过"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_conn.execute = MagicMock(return_value=mock_result)
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        result = timed.execute("SELECT 1")
        assert result == mock_result

    def test_execute_with_parameters(self):
        """带参数查询"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_conn.execute = MagicMock(return_value=mock_result)
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        result = timed.execute("SELECT ?", [42])
        assert result == mock_result
        mock_conn.execute.assert_called_with("SELECT ?", [42])

    def test_timeout_marks_unhealthy(self):
        """超时后标记连接不健康"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_conn.execute = MagicMock(side_effect=lambda *a, **k: time.sleep(5))
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=0.1)
        with pytest.raises(TimeoutError):
            timed.execute("SELECT 1")
        assert timed._unhealthy is True

    def test_unhealthy_connection_raises(self):
        """不健康连接直接抛ConnectionError"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        timed._unhealthy = True
        with pytest.raises(ConnectionError, match="连接已标记为不健康"):
            timed.execute("SELECT 1")

    def test_unhealthy_fetchall_raises(self):
        """不健康连接fetchall抛ConnectionError"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        timed._unhealthy = True
        with pytest.raises(ConnectionError):
            timed.fetchall()

    def test_unhealthy_fetchone_raises(self):
        """不健康连接fetchone抛ConnectionError"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        timed._unhealthy = True
        with pytest.raises(ConnectionError):
            timed.fetchone()

    def test_fetchall_delegates(self):
        """fetchall委托给底层连接"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_conn.fetchall = MagicMock(return_value=[(1, 2)])
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        assert timed.fetchall() == [(1, 2)]

    def test_fetchone_delegates(self):
        """fetchone委托给底层连接"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_conn.fetchone = MagicMock(return_value=(1,))
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        assert timed.fetchone() == (1,)

    def test_executemany_delegates(self):
        """executemany委托给底层连接"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_conn.executemany = MagicMock(return_value=None)
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        timed.executemany("INSERT INTO t VALUES (?)", [[1], [2]])
        mock_conn.executemany.assert_called()

    def test_close_shuts_down_executor(self):
        """close关闭executor"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        timed.close()
        mock_conn.close.assert_called_once()

    def test_getattr_delegates_to_conn(self):
        """__getattr__委托给底层连接"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_conn.custom_method = MagicMock(return_value="custom")
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        assert timed.custom_method() == "custom"

    def test_getattr_private_raises(self):
        """__getattr__私有属性抛AttributeError"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        with pytest.raises(AttributeError):
            _ = timed._private_attr

    def test_last_used_time_updated(self):
        """查询更新新last_used_time"""
        from ali2026v3_trading.data.ds_db_connection import _TimedDuckDBConnection
        mock_conn = MagicMock()
        mock_conn.execute = MagicMock(return_value=None)
        timed = _TimedDuckDBConnection(mock_conn, timeout_sec=30.0)
        before = timed._last_used_time
        time.sleep(0.01)
        timed.execute("SELECT 1")
        assert timed._last_used_time > before


# ============================================================
# _DuckDBConnectionContextManager
# ============================================================

class TestDuckDBConnectionContextManager:
    """_DuckDBConnectionContextManager: 上下文管理器"""

    def test_enter_returns_conn(self):
        """__enter__返回连接"""
        from ali2026v3_trading.data.ds_db_connection import _DuckDBConnectionContextManager
        mock_conn = MagicMock()
        cm = _DuckDBConnectionContextManager(mock_conn)
        assert cm.__enter__() is mock_conn

    def test_exit_closes_conn(self):
        """__exit__关闭连接"""
        from ali2026v3_trading.data.ds_db_connection import _DuckDBConnectionContextManager
        mock_conn = MagicMock()
        cm = _DuckDBConnectionContextManager(mock_conn)
        cm.__exit__(None, None, None)
        mock_conn.close.assert_called_once()

    def test_exit_with_exception_marks_unhealthy(self):
        """异常时标记不健康"""
        from ali2026v3_trading.data.ds_db_connection import _DuckDBConnectionContextManager
        mock_conn = MagicMock()
        mock_owner = MagicMock()
        mock_owner._thread_local = MagicMock()
        mock_owner._thread_local.conn_healthy = True
        cm = _DuckDBConnectionContextManager(mock_conn, owner=mock_owner)
        cm.__exit__(ValueError, ValueError("test"), None)
        assert mock_owner._thread_local.conn_healthy is False


# ============================================================
# DBConnectionMixin
# ============================================================

class TestDBConnectionMixin:
    """DBConnectionMixin: 连接池管理"""

    def test_is_fatal_database_error_fatal(self):
        """FATAL错误检测"""
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        assert DBConnectionMixin._is_fatal_database_error(Exception("FATAL: database error")) is True

    def test_is_fatal_database_error_invalidated(self):
        """数据库失效检测"""
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        assert DBConnectionMixin._is_fatal_database_error(Exception("database has been invalidated")) is True

    def test_is_fatal_database_error_normal(self):
        """普通错误不是FATAL"""
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        assert DBConnectionMixin._is_fatal_database_error(Exception("normal error")) is False

    def test_try_fallback_to_memory_db(self):
        """降级到内存数据库"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        mixin = DBConnectionMixin()
        conn = mixin._try_fallback_to_memory_db()
        assert conn is not None
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1
        conn.close()

    def test_connect_with_timeout_returns_timed_conn(self):
        """_connect_with_timeout返回滚TimedDuckDBConnection"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin, _TimedDuckDBConnection
        mixin = DBConnectionMixin()
        conn = mixin._connect_with_timeout(':memory:')
        assert isinstance(conn, _TimedDuckDBConnection)
        conn.close()

    def test_migrate_connection_params_same_version(self):
        """同版本不迁移"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        mixin = DBConnectionMixin()
        conn = duckdb.connect(':memory:')
        # 同版本不应报错
        mixin._migrate_connection_params(conn, '1.0', '1.0')
        conn.close()

    def test_migrate_connection_params_upgrade(self):
        """版本升级迁移"""
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        mixin = DBConnectionMixin()
        conn = duckdb.connect(':memory:')
        # 从0.9迁移到1.0
        mixin._migrate_connection_params(conn, '0.9', '1.0')
        conn.close()

    def test_set_max_pool_size(self):
        """动态调整连接池上限"""
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        mixin = DBConnectionMixin()
        mixin.set_max_pool_size(10)
        assert mixin._MAX_POOL_SIZE == 10

    def test_set_max_pool_size_invalid(self):
        """无效大小不调整"""
        from ali2026v3_trading.data.ds_db_connection import DBConnectionMixin
        mixin = DBConnectionMixin()
        original = mixin._MAX_POOL_SIZE
        mixin.set_max_pool_size(0)
        assert mixin._MAX_POOL_SIZE == original
        mixin.set_max_pool_size(-1)
        assert mixin._MAX_POOL_SIZE == original
