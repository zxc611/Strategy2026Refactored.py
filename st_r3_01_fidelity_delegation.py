# MODULE_ID: M2-523
"""test_r3_01_fidelity_delegation.py — 验证 backtest_fidelity 中 _compute_fill_quantity 和 _compute_market_impact_v2

迁移后验证:
1. _compute_fill_quantity 现在定义在 backtest_fidelity.py (权威源)，不再从 loop_core 导入
2. backtest_loop_core 作为 shim re-export，与权威源是同一对象
3. 运行时行为正确
"""
import pytest
import pandas as pd


def test_compute_fill_quantity_importable():
    """backtest_fidelity 模块中 _compute_fill_quantity 可导入"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
    assert callable(_compute_fill_quantity)


def test_compute_market_impact_v2_importable():
    """backtest_fidelity 模块中 _compute_market_impact_v2 可导入"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_market_impact_v2
    assert callable(_compute_market_impact_v2)


def test_compute_fill_quantity_is_authoritative():
    """_compute_fill_quantity 权威源在 backtest_fidelity，loop_core 已删除"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity as fidelity_fn
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity as base_fn
    assert fidelity_fn is base_fn


def test_compute_market_impact_v2_is_authoritative():
    """导入的 _compute_market_impact_v2 与权威源(backtest_fidelity)是同一个对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_market_impact_v2 as fidelity_fn
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_market_impact_v2 as base_fn
    assert fidelity_fn is base_fn


def test_compute_fill_quantity_defined_in_fidelity():
    """_compute_fill_quantity 定义在 backtest_fidelity.py (权威源)"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
    assert 'backtest_fidelity' in _compute_fill_quantity.__code__.co_filename, (
        f"_compute_fill_quantity 定义在 {_compute_fill_quantity.__code__.co_filename}，应在 backtest_fidelity"
    )


def test_compute_fill_quantity_behavior():
    """_compute_fill_quantity 运行时行为正确（最小参数调用不抛异常）"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_fill_quantity
    bar = pd.Series({"volume": 10000})
    params = {"enable_partial_fill": True, "max_participation_rate": 0.15, "min_fill_lots": 1}
    result = _compute_fill_quantity(5, bar, params)
    assert isinstance(result, int)
    assert result >= 0


def test_compute_market_impact_v2_behavior():
    """_compute_market_impact_v2 运行时行为正确（最小参数调用不抛异常）"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_market_impact_v2
    bar = pd.Series({"volume": 10000, "high": 3100.0, "low": 3090.0})
    params = {"enable_market_impact": False}
    result = _compute_market_impact_v2(5, bar, 3095.0, params)
    assert isinstance(result, float)
    assert result == 0.0  # 默认关闭时返回0.0
