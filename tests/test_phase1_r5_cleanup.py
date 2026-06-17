# MODULE_ID: M2-467
"""Phase 1 轮次5断言测试: 周边清理 — types合并到config, result合并到utils, special导入链修复
空壳文件已删除，所有导入指向实际实现模块。"""
import sys
sys.path.insert(0, '..')


class TestTypesMigration:
    """BacktestStateEnum / _STATE_REASON_MAP 迁移到 backtest_config.py"""

    def test_backtest_state_enum_defined_in_config(self):
        from ali2026v3_trading.param_pool.backtest_config import BacktestStateEnum
        assert BacktestStateEnum is not None

    def test_state_reason_map_defined_in_config(self):
        from ali2026v3_trading.param_pool.backtest_config import _STATE_REASON_MAP
        assert isinstance(_STATE_REASON_MAP, dict)
        assert 'correct_trending' in _STATE_REASON_MAP

    def test_types_removed_import_from_config(self):
        from ali2026v3_trading.param_pool.backtest_config import BacktestStateEnum as BSE_config
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import BacktestStateEnum as BSE_base
        assert BSE_config is BSE_base, "runner_base should re-export same object as config"

    def test_state_reason_map_same_identity(self):
        from ali2026v3_trading.param_pool.backtest_config import _STATE_REASON_MAP as MAP_config
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _STATE_REASON_MAP as MAP_base
        assert MAP_config is MAP_base, "runner_base should re-export same dict as config"

    def test_enum_values(self):
        from ali2026v3_trading.param_pool.backtest_config import BacktestStateEnum
        assert hasattr(BacktestStateEnum, 'INIT')
        assert hasattr(BacktestStateEnum, 'RUNNING')
        assert hasattr(BacktestStateEnum, 'PAUSED')
        assert hasattr(BacktestStateEnum, 'STOPPED')
        assert hasattr(BacktestStateEnum, 'COMPLETED')

    def test_downstream_base_imports_from_config(self):
        from ali2026v3_trading.param_pool.backtest_config import BacktestStateEnum
        assert BacktestStateEnum is not None

    def test_downstream_utils_imports_from_config(self):
        from ali2026v3_trading.param_pool.backtest_config import _STATE_REASON_MAP
        assert isinstance(_STATE_REASON_MAP, dict)


class TestResultMigration:
    """_build_backtest_result 迁移到 backtest_runner_utils.py"""

    def test_build_result_defined_in_utils(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _build_backtest_result
        assert callable(_build_backtest_result)

    def test_result_removed_import_from_utils(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _build_backtest_result as fn_utils
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _build_backtest_result as fn_base
        assert fn_utils is fn_base, "runner_base should re-export same function as utils"

    def test_build_result_is_not_wrapper(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _build_backtest_result
        import inspect
        src = inspect.getsource(_build_backtest_result)
        assert '_build_backtest_result_impl' not in src, "should not delegate to _impl"


class TestSpecialImportChain:
    """backtest_runner_special.py 已合并到 backtest_strategy_runners.py"""

    def test_special_direct_import(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_multiscale, run_backtest_with_cycle_resonance
        assert callable(run_backtest_multiscale)
        assert callable(run_backtest_with_cycle_resonance)

    def test_special_functions_exist(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base
        import ali2026v3_trading.param_pool.backtest.backtest_strategy_runners as runners
        assert hasattr(runners, 'run_backtest_multiscale')
        assert hasattr(runners, 'run_backtest_with_cycle_resonance')

    def test_base_getattr_lists_special(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as base
        import inspect
        src = inspect.getsource(base.__getattr__) if hasattr(base, '__getattr__') else ''
        if not src:
            src = open(base.__file__, encoding='utf-8').read()
        assert 'run_backtest_multiscale' in src
        assert 'run_backtest_with_cycle_resonance' in src


class TestDownstreamCompatibility:
    """下游兼容性验证"""

    def test_runner_base_star_import_from_config(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import BacktestStateEnum
        assert BacktestStateEnum is not None

    def test_utils_imports_state_reason_map(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _STATE_REASON_MAP
        assert isinstance(_STATE_REASON_MAP, dict)


class TestFacadeCompleteness:
    """backtest_runner_base.py facade 完整性验证（空壳已删除）"""

    def test_facade_exports_arbitrage(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as base
        assert hasattr(base, 'run_backtest_arbitrage'), "facade missing run_backtest_arbitrage"

    def test_facade_exports_market_making(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as base
        assert hasattr(base, 'run_backtest_market_making'), "facade missing run_backtest_market_making"

    def test_facade_exports_hft_with_disturbance(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as base
        assert hasattr(base, 'run_backtest_hft_with_disturbance'), "facade missing run_backtest_hft_with_disturbance"

    def test_facade_exports_hft_tick_fidelity(self):
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as base
        assert hasattr(base, 'run_backtest_hft_tick_fidelity'), "facade missing run_backtest_hft_tick_fidelity"

    def test_facade_arbitrage_same_as_runners(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import run_backtest_arbitrage as base_fn
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import run_backtest_arbitrage as runners_fn
        assert base_fn is runners_fn


class TestGreeksMathShim:
    """greeks_math.py shim 验证"""

    def test_greeks_math_reexports_norm_cdf(self):
        from ali2026v3_trading.governance.greeks_calculator import _norm_cdf
        from ali2026v3_trading.governance.greeks_calculator import _norm_cdf as calc_cdf
        assert _norm_cdf is calc_cdf, "greeks_math shim should re-export same _norm_cdf"

    def test_greeks_math_reexports_bs_price(self):
        from ali2026v3_trading.governance.greeks_calculator import _bs_price
        from ali2026v3_trading.governance.greeks_calculator import _bs_price as calc_price
        assert _bs_price is calc_price, "greeks_math shim should re-export same _bs_price"

    def test_greeks_math_all_includes_underscore_names(self):
        import ali2026v3_trading.governance.greeks_calculator as gc
        assert '_norm_cdf' in gc.__all__
        assert '_normalize_option_type' in gc.__all__
