# MODULE_ID: M2-528
"""R3-03: 验证4组重复函数修复后，各模块可导入且与权威源是同一对象，运行时可调用。
空壳文件已删除，所有导入指向实际实现模块。"""
import pytest
import pandas as pd
import numpy as np


# ── F-05: _check_intra_bar_stop_loss ──────────────────────────────────

def test_check_intra_bar_stop_loss_importable_from_state():
    """backtest_state 模块中 _check_intra_bar_stop_loss 可导入"""
    from ali2026v3_trading.param_pool.backtest.backtest_state import _check_intra_bar_stop_loss
    assert callable(_check_intra_bar_stop_loss)


def test_check_intra_bar_stop_loss_is_authoritative():
    """导入的 _check_intra_bar_stop_loss 与权威源(backtest_state)是同一个对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_intra_bar_stop_loss as base_fn
    from ali2026v3_trading.param_pool.backtest.backtest_state import _check_intra_bar_stop_loss as state_fn
    assert base_fn is state_fn


def test_check_intra_bar_stop_loss_callable():
    """_check_intra_bar_stop_loss 运行时可调用（最小参数）"""
    from ali2026v3_trading.param_pool.backtest.backtest_state import _check_intra_bar_stop_loss
    result = _check_intra_bar_stop_loss(None, pd.Series(), None, {})
    assert result == (False, "", 0.0)


# ── F-06: _check_two_stage_stop ──────────────────────────────────────

def test_check_two_stage_stop_importable_from_state():
    """backtest_state 模块中 _check_two_stage_stop 可导入"""
    from ali2026v3_trading.param_pool.backtest.backtest_state import _check_two_stage_stop
    assert callable(_check_two_stage_stop)


def test_check_two_stage_stop_is_authoritative():
    """导入的 _check_two_stage_stop 与权威源(backtest_state)是同一个对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_two_stage_stop as base_fn
    from ali2026v3_trading.param_pool.backtest.backtest_state import _check_two_stage_stop as state_fn
    assert base_fn is state_fn


# ── F-07: _check_state_transition ────────────────────────────────────

def test_check_state_transition_importable_from_runner_base():
    """backtest_runner_base 模块中 _check_state_transition 可导入"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_state_transition
    assert callable(_check_state_transition)


def test_check_state_transition_is_authoritative():
    """导入的 _check_state_transition 与权威源(_leaf_runner_helpers)是同一个对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_state_transition as base_fn
    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_state_transition as state_fn
    assert base_fn is state_fn


def test_check_state_transition_callable():
    """_check_state_transition 运行时可调用（最小参数）"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_state_transition
    from ali2026v3_trading.param_pool.backtest.backtest_state import _BacktestState

    bt = _BacktestState()
    bar = pd.Series({
        "correct_rise_pct": 0.5,
        "correct_fall_pct": 0.1,
        "wrong_rise_pct": 0.2,
        "wrong_fall_pct": 0.1,
    })
    params = {"state_confirm_bars": 1, "bar_period": 1.0, "non_other_ratio_threshold": 0.65}
    result = _check_state_transition(bt, bar, params)
    assert isinstance(result, str)


# ── F-08: _check_logic_reversal ──────────────────────────────────────

def test_check_logic_reversal_importable_from_runner_base():
    """backtest_runner_base 模块中 _check_logic_reversal 可导入"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_logic_reversal
    assert callable(_check_logic_reversal)


def test_check_logic_reversal_is_authoritative():
    """导入的 _check_logic_reversal 与权威源(_leaf_runner_helpers)是同一个对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _check_logic_reversal as base_fn
    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_logic_reversal as state_fn
    assert base_fn is state_fn
