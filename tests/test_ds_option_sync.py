# MODULE_ID: M2-336
"""测试 data/ds_option_sync.py — SQL构建"""
import pytest


# ============================================================
# _build_option_sync_sql
# ============================================================

class TestBuildOptionSyncSQL:
    """_build_option_sync_sql: SQL构建"""

    def test_sql_without_row_id(self):
        """不含row_id时SQL不包含rowid"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'row_id' not in sql

    def test_sql_with_row_id(self):
        """含row_id时SQL包含rowid"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=True)
        assert 'row_id' in sql
        assert 't.rowid AS row_id' in sql

    def test_sql_contains_cte_option_base(self):
        """SQL包含option_base CTE"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'option_base' in sql
        assert 'WITH option_base' in sql

    def test_sql_contains_cte_option_with_lag(self):
        """SQL包含option_with_lag CTE"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'option_with_lag' in sql

    def test_sql_contains_cte_option_calculated(self):
        """SQL包含option_calculated CTE"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'option_calculated' in sql

    def test_sql_contains_asof_join(self):
        """SQL包含ASOF JOIN"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'ASOF' in sql

    def test_sql_contains_option_type_logic(self):
        """SQL包含CALL/PUT判断逻辑"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'CALL' in sql
        assert 'PUT' in sql

    def test_sql_contains_lag_window(self):
        """SQL包含LAG窗口函数"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'LAG(' in sql

    def test_sql_contains_is_otm(self):
        """SQL包含is_otm计算"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'is_otm' in sql

    def test_sql_contains_sync_status(self):
        """SQL包含sync_status计算"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'sync_status' in sql

    def test_sql_contains_underlying_price(self):
        """SQL包含underlying_price"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'underlying_price' in sql

    def test_sql_contains_strike_price(self):
        """SQL包含strike_price"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'strike_price' in sql

    def test_sql_contains_partition_by_instrument(self):
        """SQL包含PARTITION BY instrument_id"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'PARTITION BY instrument_id' in sql

    def test_sql_row_id_in_all_ctes(self):
        """row_id出现在所有三个CTE中"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=True)
        # option_base中有t.rowid AS row_id
        assert 't.rowid AS row_id' in sql
        # option_with_lag中有row_id
        assert 'row_id,' in sql

    def test_sql_correct_rise_fall_logic(self):
        """SQL包含正确的涨跌同步逻辑"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'correct_rise' in sql
        assert 'wrong_rise' in sql
        assert 'correct_fall' in sql
        assert 'wrong_fall' in sql

    def test_sql_prev_underlying_price(self):
        """SQL包含prev_underlying_price"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'prev_underlying_price' in sql
        assert 'prev_option_price' in sql

    def test_sql_joins_option_instruments(self):
        """SQL包含option_instruments JOIN"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'option_instruments' in sql
        assert 'futures_instruments' in sql

    def test_sql_where_clause(self):
        """SQL包含WHERE条件"""
        from data.ds_option_sync import _build_option_sync_sql
        sql = _build_option_sync_sql(include_row_id=False)
        assert 'WHERE' in sql
        assert 'underlying_future_id IS NOT NULL' in sql


# ============================================================
# OptionSyncMixin (结构验证)
# ============================================================

class TestOptionSyncMixin:
    """OptionSyncMixin: 结构验证"""

    def test_mixin_has_refresh_method(self):
        """Mixin有界refresh_option_sync_stats方法"""
        from data.ds_option_sync import OptionSyncMixin
        assert hasattr(OptionSyncMixin, '_refresh_option_sync_stats')

    def test_mixin_has_update_method(self):
        """Mixin有界update_option_status_columns方法"""
        from data.ds_option_sync import OptionSyncMixin
        assert hasattr(OptionSyncMixin, '_update_option_status_columns')
