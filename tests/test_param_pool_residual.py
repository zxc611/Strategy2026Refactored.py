# MODULE_ID: M2-457
"""param_pool/ 未覆盖模块测试"""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from unittest.mock import patch
import pandas as pd
import numpy as np


class TestAdvValidationMiscShim:
    def test_import_success(self):
        import ali2026v3_trading.param_pool.adv_validation_misc as mod
        assert mod is not None
        assert mod._TARGET == "ali2026v3_trading.param_pool.validation.adv_validation_misc"

    def test_getattr_redirects(self):
        from ali2026v3_trading.param_pool.adv_validation_misc import AdvancedValidation
        assert AdvancedValidation is not None

    def test_getattr_missing_raises(self):
        import ali2026v3_trading.param_pool.adv_validation_misc as mod
        with pytest.raises(AttributeError):
            _ = mod.ThisAttributeDoesNotExist

    def test_dir_redirects(self):
        import ali2026v3_trading.param_pool.adv_validation_misc as mod
        d = mod.__dir__()
        assert isinstance(d, list)
        assert "AdvancedValidation" in d


class TestBacktestFidelity:
    @pytest.fixture(scope="class")
    def mod(self):
        from ali2026v3_trading.param_pool.backtest import _backtest_fidelity
        return _backtest_fidelity

    def test_simulate_limit_order_queue_disabled(self, mod):
        bar = pd.Series({"volume": 1000, "high": 101, "low": 99})
        result = mod._simulate_limit_order_queue(
            order_price=100, current_price=100, bar=bar,
            order_lots=5, enable_queue=False
        )
        assert result["filled"] is True
        assert result["fill_price"] == 100
        assert result["fill_lots"] == 5
        assert result["queue_time"] == 0

    def test_simulate_limit_order_queue_enabled(self, mod):
        bar = pd.Series({"volume": 10000, "high": 105, "low": 95})
        result = mod._simulate_limit_order_queue(
            order_price=100, current_price=100, bar=bar,
            order_lots=5, enable_queue=True, timeout_seconds=300
        )
        assert isinstance(result["filled"], bool)
        assert "fill_price" in result
        assert "fill_lots" in result
        assert "queue_time" in result
        assert "queue_position" in result

    def test_simulate_limit_order_queue_none_bar(self, mod):
        result = mod._simulate_limit_order_queue(
            order_price=100, current_price=100, bar=None,
            order_lots=5, enable_queue=False
        )
        assert result["filled"] is True

    def test_simulate_market_order_slippage_close(self, mod):
        bar = pd.Series({"close": 100.0, "open": 99, "high": 101, "low": 98})
        price = mod._simulate_market_order_slippage(bar, price_mode="close", direction=1)
        assert price > 100.0

    def test_simulate_market_order_slippage_weighted(self, mod):
        bar = pd.Series({"close": 100.0, "open": 99, "high": 101, "low": 98})
        price = mod._simulate_market_order_slippage(bar, price_mode="weighted", direction=-1)
        expected_base = (99 + 101 + 98 + 100) / 4.0
        assert price < expected_base

    def test_simulate_market_order_slippage_random(self, mod):
        bar = pd.Series({"close": 100.0, "high": 101, "low": 99})
        price = mod._simulate_market_order_slippage(bar, price_mode="random", direction=1)
        assert price >= 99.0
        assert price <= 101.0 + (101.0 * 5.0 / 10000.0)

    def test_get_instrument_type_slippage_etf(self, mod):
        s = mod._get_instrument_type_slippage("510050.SH", base_slippage_bps=5.0)
        assert s == 5.0

    def test_get_instrument_type_slippage_option_etf(self, mod):
        s = mod._get_instrument_type_slippage("50ETF-C-2500", base_slippage_bps=5.0)
        assert s == 7.5

    def test_get_instrument_type_slippage_future(self, mod):
        s = mod._get_instrument_type_slippage("M2501", base_slippage_bps=5.0)
        assert s == 12.5
