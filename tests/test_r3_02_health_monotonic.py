# MODULE_ID: M2-525
"""test_r3_02_health_monotonic.py — F-03/F-04 重复函数定义修复验证

迁移后验证:
1. _check_backtest_health 权威源已从 backtest_loop_core 迁移到 backtest_runner_validation
2. backtest_loop_core 作为 shim re-export，与权威源是同一对象
3. _check_bar_data_monotonic 从 backtest_runner 可导入，且与权威源(backtest_runner_validation)是同一对象
4. _check_bar_data_monotonic 运行时行为正确
"""
import os
import sys

import pandas as pd
import pytest

# 确保项目根目录在 sys.path 中
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestCheckBacktestHealthImport:
    """F-03: _check_backtest_health 去重验证（权威源已迁移到 validation）"""

    def test_importable_from_validation(self):
        """从 backtest_runner_validation 可导入 _check_backtest_health"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        assert callable(_check_backtest_health)

    def test_importable_from_runner_base(self):
        """从 backtest_runner_base 可导入 _check_backtest_health"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_backtest_health
        assert callable(_check_backtest_health)

    def test_same_object_via_runner_base(self):
        """validation 中的 _check_backtest_health 与 runner_base re-export 是同一对象"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health as val_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_backtest_health as base_fn
        assert val_fn is base_fn, (
            f"_check_backtest_health 不是同一对象: validation={val_fn}, runner_base={base_fn}"
        )

    def test_defined_in_validation(self):
        """_check_backtest_health 定义在 backtest_runner_validation.py (权威源)"""
        import ali2026v3_trading.param_pool.backtest.backtest_runner_validation as mod
        fn = mod._check_backtest_health
        assert 'backtest_runner_validation' in fn.__code__.co_filename, (
            f"_check_backtest_health 定义在 {fn.__code__.co_filename}，应在 backtest_runner_validation"
        )


class TestCheckBarDataMonotonicImport:
    """F-04: _check_bar_data_monotonic 去重验证"""

    def test_importable_from_runner(self):
        """从 backtest_runner 可导入 _check_bar_data_monotonic"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_bar_data_monotonic
        assert callable(_check_bar_data_monotonic)

    def test_importable_from_authoritative(self):
        """从权威源 backtest_runner_validation 可导入 _check_bar_data_monotonic"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        assert callable(_check_bar_data_monotonic)

    def test_same_object_as_authoritative(self):
        """runner 中的 _check_bar_data_monotonic 与权威源是同一对象"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_bar_data_monotonic as runner_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic as auth_fn
        assert runner_fn is auth_fn, (
            f"_check_bar_data_monotonic 不是同一对象: runner={runner_fn}, validation={auth_fn}"
        )

    def test_no_local_definition_in_runner_base(self):
        """backtest_runner_base 中不再有 _check_bar_data_monotonic 的本地定义"""
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as mod
        fn = mod._check_bar_data_monotonic
        assert 'backtest_runner_validation' in fn.__code__.co_filename, (
            f"_check_bar_data_monotonic 仍定义在 {fn.__code__.co_filename}，应来自 backtest_runner_validation"
        )


class TestCheckBarDataMonotonicBehavior:
    """_check_bar_data_monotonic 运行时行为验证"""

    def test_monotonic_increasing_no_exception(self):
        """传入单调递增的 DataFrame 不抛异常"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        df = pd.DataFrame({
            'minute': pd.date_range('2025-01-01 09:00', periods=100, freq='1min'),
            'close': range(100),
        })
        _check_bar_data_monotonic(df)  # 不应抛异常

    def test_none_input_no_exception(self):
        """传入 None 不抛异常"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        _check_bar_data_monotonic(None)

    def test_empty_dataframe_no_exception(self):
        """传入空 DataFrame 不抛异常"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        _check_bar_data_monotonic(pd.DataFrame())

    def test_no_time_column_no_exception(self):
        """传入无时间列的 DataFrame 不抛异常"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        df = pd.DataFrame({'close': [1, 2, 3]})
        _check_bar_data_monotonic(df)

    def test_timestamp_column_supported(self):
        """支持 timestamp 列名"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        df = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01 09:00', periods=10, freq='1min'),
            'close': range(10),
        })
        _check_bar_data_monotonic(df)  # 不应抛异常

    def test_datetime_column_supported(self):
        """支持 datetime 列名"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        df = pd.DataFrame({
            'datetime': pd.date_range('2025-01-01 09:00', periods=10, freq='1min'),
            'close': range(10),
        })
        _check_bar_data_monotonic(df)  # 不应抛异常

    def test_symbol_grouping_supported(self):
        """支持按 symbol 分组检查"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_bar_data_monotonic
        df = pd.DataFrame({
            'minute': list(pd.date_range('2025-01-01 09:00', periods=5, freq='1min')) * 2,
            'symbol': ['A'] * 5 + ['B'] * 5,
            'close': list(range(5)) * 2,
        })
        _check_bar_data_monotonic(df)  # 不应抛异常
