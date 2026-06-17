# MODULE_ID: M2-304
"""Tests for ali2026v3_trading.param_pool.backtest._backtest_fidelity."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np


def test_simulate_limit_order_queue():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    bar = pd.Series({"volume": 1000, "high": 105.0, "low": 95.0})
    result = btf._simulate_limit_order_queue(
        order_price=100.0, current_price=100.0, bar=bar, order_lots=1
    )
    assert isinstance(result, dict)
    assert "filled" in result
    assert "fill_price" in result
    assert "fill_lots" in result


def test_simulate_market_order_slippage():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    bar = pd.Series({"close": 100.0, "open": 99.0, "high": 101.0, "low": 98.0})
    result = btf._simulate_market_order_slippage(bar, slippage_bps=5.0, direction=1)
    assert isinstance(result, float)
    assert result > 0


def test_get_instrument_type_slippage():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    result = btf._get_instrument_type_slippage("510050.SH")
    assert isinstance(result, float)
    result2 = btf._get_instrument_type_slippage("IO2506-C-4000")
    assert isinstance(result2, float)


def test_simulate_order_cancel():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    result = btf._simulate_order_cancel("order_123", enable_cancel=False)
    assert isinstance(result, dict)
    assert result["success"] is True


def test_compute_fill_quantity():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    bar = pd.Series({"volume": 1000})
    params = {"enable_partial_fill": True, "max_participation_rate": 0.1, "min_fill_lots": 1}
    result = btf._compute_fill_quantity(10, bar, params)
    assert isinstance(result, int)
    assert result >= 0


def test_apply_fidelity_presets():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    params = {"backtest_fidelity_mode": "institutional"}
    result = btf._apply_fidelity_presets(params)
    assert isinstance(result, dict)
    assert result.get("execution_model") == "institutional"


def test_get_expiry_slippage_multiplier():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    bar = pd.Series({"days_to_expiry": 5})
    result = btf._get_expiry_slippage_multiplier(bar, {})
    assert isinstance(result, float)
    assert result >= 1.0


def test_run_backtest_fidelity_analysis():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    if not hasattr(btf, "run_backtest_fidelity_analysis"):
        pytest.skip("run_backtest_fidelity_analysis not found in _backtest_fidelity module")
    class MockStrategy:
        pass
    params = {"position_scale": 1.0, "execution_delay_ms": 0}
    result = btf.run_backtest_fidelity_analysis(MockStrategy(), params, np.array([]), [])
    assert isinstance(result, dict)


def test_simulate_latency():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    if not hasattr(btf, "_simulate_latency"):
        pytest.skip("_simulate_latency not found in _backtest_fidelity module")
    result = btf._simulate_latency(100.0, 5.0)
    assert isinstance(result, (float, int))


def test_simulate_slippage():
    from ali2026v3_trading.param_pool.backtest import _backtest_fidelity as btf
    if not hasattr(btf, "_simulate_slippage"):
        pytest.skip("_simulate_slippage not found in _backtest_fidelity module")
    result = btf._simulate_slippage(100.0, 2.0, 1)
    assert isinstance(result, (float, int))
