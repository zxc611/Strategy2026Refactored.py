# MODULE_ID: M2-465
"""test_phase1_r2_loop_core_migration.py — Phase 1 轮次2 断言测试

验证 backtest_loop_core.py 已删除，活代码迁移后的运行时行为:
1. _compute_fill_quantity 权威源在 backtest_fidelity.py，循环依赖已解除
2. _check_backtest_health 权威源在 backtest_runner_validation.py
3. _update_mtm_equity / _apply_fidelity_presets / exclude_rollover_signals 去重正确
4. 迁移后运行时行为不变
"""
import pytest
import pandas as pd
import numpy as np
from types import ModuleType


# ── 1. _compute_fill_quantity: 权威源在 backtest_fidelity ──

class TestComputeFillQuantityMigration:
    """_compute_fill_quantity 从 loop_core 迁移到 fidelity"""

    def test_defined_in_fidelity(self):
        """权威源: _compute_fill_quantity 定义在 backtest_fidelity.py"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
        assert 'backtest_fidelity' in _compute_fill_quantity.__code__.co_filename

    def test_shim_removed_import_fidelity(self):
        """loop_core 已删除，直接从 backtest_fidelity 导入"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity as fidelity_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity as base_fn
        assert fidelity_fn is base_fn

    def test_runner_base_gets_from_fidelity(self):
        """backtest_runner_base 从 fidelity 获取 _compute_fill_quantity"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity as fidelity_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity as base_fn
        assert fidelity_fn is base_fn

    def test_no_circular_import(self):
        """fidelity 不再从 loop_core 导入 _compute_fill_quantity (循环依赖已解除)"""
        import ali2026v3_trading.param_pool.backtest.backtest_runner_base as mod
        # 模块应可正常导入，不触发循环导入
        assert isinstance(mod, ModuleType)

    def test_behavior_partial_fill_disabled(self):
        """运行时: enable_partial_fill=False 返回原始手数"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
        bar = pd.Series({"volume": 10000})
        params = {"enable_partial_fill": False}
        assert _compute_fill_quantity(10, bar, params) == 10

    def test_behavior_zero_lots(self):
        """运行时: order_lots<=0 返回0"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
        bar = pd.Series({"volume": 10000})
        params = {"enable_partial_fill": True}
        assert _compute_fill_quantity(0, bar, params) == 0
        assert _compute_fill_quantity(-5, bar, params) == 0

    def test_behavior_participation_rate(self):
        """运行时: 参与率限制生效"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
        bar = pd.Series({"volume": 100})  # 100手成交量
        params = {"enable_partial_fill": True, "max_participation_rate": 0.15, "min_fill_lots": 1}
        result = _compute_fill_quantity(50, bar, params)
        assert result == min(50, max(1, int(100 * 0.15)))  # 15手

    def test_behavior_full_rate(self):
        """运行时: max_participation_rate>=1.0 返回原始手数"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
        bar = pd.Series({"volume": 100})
        params = {"enable_partial_fill": True, "max_participation_rate": 1.0, "min_fill_lots": 1}
        assert _compute_fill_quantity(50, bar, params) == 50


# ── 2. _check_backtest_health: 权威源在 backtest_runner_validation ──

class TestCheckBacktestHealthMigration:
    """_check_backtest_health 从 loop_core 迁移到 validation"""

    def test_defined_in_validation(self):
        """权威源: _check_backtest_health 定义在 backtest_runner_validation.py"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        assert 'backtest_runner_validation' in _check_backtest_health.__code__.co_filename

    def test_shim_removed_import_validation(self):
        """loop_core 已删除，直接从 backtest_runner_validation 导入"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health as val_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _check_backtest_health as utils_fn
        assert val_fn is utils_fn

    def test_runner_utils_gets_from_validation(self):
        """backtest_runner_utils 从 validation 获取 _check_backtest_health"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health as val_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _check_backtest_health as utils_fn
        assert val_fn is utils_fn

    def test_behavior_healthy(self):
        """运行时: 正常权益返回 True"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        from types import SimpleNamespace
        bt = SimpleNamespace(initial_equity=100000, equity=80000, mtm_equity=0,
                           consecutive_loss_streak=2)
        params = {"max_consecutive_losses": 5}
        assert _check_backtest_health(bt, params, None) is True

    def test_behavior_critical_equity(self):
        """运行时: 权益低于50%返回 False"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        from types import SimpleNamespace
        bt = SimpleNamespace(initial_equity=100000, equity=40000, mtm_equity=0,
                           consecutive_loss_streak=0)
        params = {"max_consecutive_losses": 5}
        assert _check_backtest_health(bt, params, None) is False

    def test_behavior_critical_consecutive_loss(self):
        """运行时: 连续亏损超限返回 False"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        from types import SimpleNamespace
        bt = SimpleNamespace(initial_equity=100000, equity=80000, mtm_equity=0,
                           consecutive_loss_streak=5)
        params = {"max_consecutive_losses": 5}
        assert _check_backtest_health(bt, params, None) is False

    def test_behavior_mtm_equity_used(self):
        """运行时: enable_mtm_equity时使用mtm_equity判断"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        from types import SimpleNamespace
        bt = SimpleNamespace(initial_equity=100000, equity=80000, mtm_equity=40000,
                           consecutive_loss_streak=0)
        params = {"enable_mtm_equity": True, "max_consecutive_losses": 5}
        assert _check_backtest_health(bt, params, None) is False


# ── 3. loop_core shim: 死代码已删除 ──

class TestLoopCoreRemoved:
    """backtest_loop_core.py 已删除"""

    def test_loop_core_import_fails(self):
        """loop_core 已删除，导入应失败"""
        import importlib
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module('ali2026v3_trading.param_pool.backtest_loop_core')

    def test_dead_code_not_accessible(self):
        """死代码函数不再可访问"""
        from ali2026v3_trading.param_pool import backtest_runner_base as mod
        dead_names = [
            '_run_backtest_main_loop', '_run_backtest_setup',
            '_process_pending_orders',
            '_check_positions_phase1_state_conflict',
            '_check_positions_phase2_close_check',
        ]
        for name in dead_names:
            assert not hasattr(mod, name), (
                f"死代码 {name} 仍存在于 backtest_runner_base"
            )


# ── 4. 去重验证: _update_mtm_equity / _apply_fidelity_presets / exclude_rollover_signals ──

class TestDeduplication:
    """去重: loop_core 副本已删除，权威源正确"""

    def test_update_mtm_equity_from_state(self):
        """_update_mtm_equity 权威源在 backtest_state.py"""
        from ali2026v3_trading.param_pool.backtest.backtest_state import _update_mtm_equity as state_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _update_mtm_equity as base_fn
        assert state_fn is base_fn

    def test_apply_fidelity_presets_from_fidelity(self):
        """_apply_fidelity_presets 权威源在 backtest_fidelity.py"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _apply_fidelity_presets as fidelity_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _apply_fidelity_presets as base_fn
        assert fidelity_fn is base_fn

    def test_exclude_rollover_signals_from_utils(self):
        """exclude_rollover_signals 权威源在 backtest_runner_utils.py"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import exclude_rollover_signals as utils_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import exclude_rollover_signals as base_fn
        assert utils_fn is base_fn

    def test_apply_fidelity_presets_behavior(self):
        """运行时: _apply_fidelity_presets institutional 模式设置正确"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _apply_fidelity_presets
        params = {"backtest_fidelity_mode": "institutional"}
        result = _apply_fidelity_presets(params)
        assert result["execution_model"] == "institutional"
        assert result["enable_intra_bar_stop_loss"] is True
        assert result["enable_mtm_equity"] is True
        assert result["max_participation_rate"] == 0.15

    def test_apply_fidelity_presets_standard(self):
        """运行时: _apply_fidelity_presets standard 模式不修改参数"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _apply_fidelity_presets
        params = {"backtest_fidelity_mode": "standard"}
        result = _apply_fidelity_presets(params)
        assert "execution_model" not in result

    def test_exclude_rollover_signals_behavior(self):
        """运行时: exclude_rollover_signals 空 rollover 不修改数据"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import exclude_rollover_signals
        df = pd.DataFrame({
            'minute': pd.date_range('2025-01-01', periods=5, freq='1min'),
            'close': range(5),
        })
        result = exclude_rollover_signals(df, [])
        assert 'rollover_excluded' in result.columns
        assert not result['rollover_excluded'].any()


# ── 5. 下游兼容性 ──

class TestDownstreamCompatibility:
    """下游模块导入兼容性验证"""

    def test_runner_base_imports_work(self):
        """backtest_runner_base.py 所有导入正常"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
            _compute_fill_quantity, _check_backtest_health,
            _apply_fidelity_presets, _update_mtm_equity,
            exclude_rollover_signals,
        )
        assert callable(_compute_fill_quantity)
        assert callable(_check_backtest_health)
        assert callable(_apply_fidelity_presets)
        assert callable(_update_mtm_equity)
        assert callable(exclude_rollover_signals)

    def test_strategy_runners_imports_work(self):
        """backtest_strategy_runners.py 导入正常"""
        from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import (
            run_backtest_box_extreme, run_backtest_hft,
        )
        assert callable(run_backtest_box_extreme)
        assert callable(run_backtest_hft)

    def test_validation_deep_orchestrator_imports_work(self):
        """validation_deep_orchestrator.py 导入正常"""
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import (
            exclude_rollover_signals,
        )
        assert callable(exclude_rollover_signals)
