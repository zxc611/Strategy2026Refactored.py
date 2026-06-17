# MODULE_ID: M2-506
"""R1-3: 验证 backtest_runner_hft.py 使用统一止盈止损函数"""
import pytest


def test_hft_uses_unified_stop_loss():
    """backtest_runner_hft 应导入 should_trigger_stop_loss/should_trigger_take_profit"""
    import ali2026v3_trading.param_pool.backtest.backtest_strategy_runners as hft_mod
    assert hasattr(hft_mod, 'should_trigger_stop_loss'), \
        "backtest_runner_hft 应导入 should_trigger_stop_loss"
    assert hasattr(hft_mod, 'should_trigger_take_profit'), \
        "backtest_runner_hft 应导入 should_trigger_take_profit"


def test_no_bare_comparison_in_hft_stop_logic():
    """backtest_runner_hft.py 中不应有裸 <= pos.stop_loss_price 或 >= pos.stop_profit_price"""
    import inspect
    import ali2026v3_trading.param_pool.backtest.backtest_strategy_runners as hft_mod
    source = inspect.getsource(hft_mod)
    # 搜索裸比较模式：bar_price <= pos.stop_loss_price 或 bar_price >= pos.stop_profit_price
    assert 'bar_price <= pos.stop_loss_price' not in source, \
        "R1-3: backtest_runner_hft.py 仍有裸 bar_price <= pos.stop_loss_price 比较"
    assert 'bar_price >= pos.stop_profit_price' not in source, \
        "R1-3: backtest_runner_hft.py 仍有裸 bar_price >= pos.stop_profit_price 比较"
    assert 'bar_price >= pos.stop_loss_price' not in source, \
        "R1-3: backtest_runner_hft.py 仍有裸 bar_price >= pos.stop_loss_price 比较"
    assert 'bar_price <= pos.stop_profit_price' not in source, \
        "R1-3: backtest_runner_hft.py 仍有裸 bar_price <= pos.stop_profit_price 比较"


def test_unified_stop_functions_behavior():
    """should_trigger_stop_loss/should_trigger_take_profit 行为正确"""
    from ali2026v3_trading.infra.resilience import (
        should_trigger_stop_loss, should_trigger_take_profit
    )
    # 多头止损：价格低于止损价触发
    assert should_trigger_stop_loss(99.0, 100.0, is_long=True) == True
    # 多头止损：价格高于止损价不触发
    assert should_trigger_stop_loss(101.0, 100.0, is_long=True) == False
    # 空头止损：价格高于止损价触发
    assert should_trigger_stop_loss(101.0, 100.0, is_long=False) == True
    # 多头止盈：价格高于止盈价触发
    assert should_trigger_take_profit(110.0, 100.0, is_long=True) == True
    # 空头止盈：价格低于止盈价触发
    assert should_trigger_take_profit(90.0, 100.0, is_long=False) == True
