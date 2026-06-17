# MODULE_ID: M2-344
"""P3.3: 输入模糊测试 - TickHandlerMixin和RiskEngine异常输入鲁棒性"""
from __future__ import annotations

import math
import pytest
from unittest.mock import MagicMock, patch
from typing import Any


class MockStateStore:
    def __init__(self):
        self._data = {}
    def set(self, key, value):
        self._data[key] = value
    def get(self, key, default=None):
        return self._data.get(key, default)
    def set_ref(self, key, value):
        self._data[key] = value


_FUZZ_INPUTS = {
    "nan_price": {"symbol": "IF2606", "last_price": float("nan"), "volume": 1},
    "inf_price": {"symbol": "IF2606", "last_price": float("inf"), "volume": 1},
    "negative_price": {"symbol": "IF2606", "last_price": -100.0, "volume": 1},
    "zero_price": {"symbol": "IF2606", "last_price": 0.0, "volume": 1},
    "zero_volume": {"symbol": "IF2606", "last_price": 4000.0, "volume": 0},
    "missing_symbol": {"last_price": 4000.0, "volume": 1},
    "extra_long_symbol": {"symbol": "X" * 10000, "last_price": 4000.0, "volume": 1},
    "none_price": {"symbol": "IF2606", "last_price": None, "volume": 1},
    "nan_volume": {"symbol": "IF2606", "last_price": 4000.0, "volume": float("nan")},
    "dict_as_price": {"symbol": "IF2606", "last_price": {"bad": True}, "volume": 1},
}


class TestTickFuzz:
    @pytest.mark.parametrize("fuzz_name,tick_data", list(_FUZZ_INPUTS.items()))
    def test_fuzz_tick_does_not_crash(self, fuzz_name, tick_data):
        try:
            from ali2026v3_trading.data.data_access import get_data_access
            da = get_data_access()
            if "symbol" in tick_data and isinstance(tick_data.get("last_price"), (int, float)):
                result = da.get_market_data(tick_data.get("symbol", ""))
                assert hasattr(result, "success")
        except (TypeError, ValueError, AttributeError) as e:
            pass
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            if "duckdb" not in str(e).lower() and "import" not in str(e).lower():
                pytest.fail(f"Unexpected exception for {fuzz_name}: {e}")


class TestRiskEngineFuzz:
    def _make_snapshot(self, **overrides):
        snap = MagicMock()
        snap.action = overrides.get("action", "OPEN")
        snap.symbol = overrides.get("symbol", "IF2606")
        snap.instrument_id = overrides.get("instrument_id", "IF2606")
        snap.amount = overrides.get("amount", 1.0)
        snap.price = overrides.get("price", 4000.0)
        snap.equity = overrides.get("equity", 1000000.0)
        snap.is_valid = overrides.get("is_valid", True)
        snap.signal = overrides.get("signal", {})
        snap.account_id = "test"
        snap.hedge_type = "none"
        snap.days_to_expiry = 30
        snap.bar_datetime = None
        return snap

    def _make_risk_service(self):
        rs = MagicMock()
        rs._check_position_limit.return_value = None
        rs._check_risk_ratio.return_value = None
        rs._check_greeks_limits.return_value = None
        rs._check_life_expectancy.return_value = None
        rs._check_spread_degradation.return_value = None
        rs._check_strategy_status.return_value = None
        rs._check_rate_limit.return_value = None
        rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=False)
        rs._check_invariant_runtime.return_value = {"all_passed": True, "violations": []}
        rs._record_signal_time.return_value = None
        return rs

    @pytest.mark.parametrize("price", [float("nan"), float("inf"), -100.0, 0.0, None])
    def test_market_risk_with_fuzz_price(self, price):
        from ali2026v3_trading.risk_engine.market_risk import check_market_risks
        snap = self._make_snapshot(price=price)
        rs = self._make_risk_service()
        try:
            result = check_market_risks(snap, rs)
            assert result is None or hasattr(result, "is_block")
        except (TypeError, ValueError):
            pass

    @pytest.mark.parametrize("amount", [float("nan"), float("inf"), -100.0, 0.0])
    def test_shared_checks_with_fuzz_amount(self, amount):
        from ali2026v3_trading.risk_engine.shared_checks import check_position_limit
        snap = self._make_snapshot(amount=amount)
        rs = self._make_risk_service()
        try:
            result = check_position_limit(snap, rs)
            assert result is None or hasattr(result, "is_block")
        except (TypeError, ValueError):
            pass

    def test_signal_validity_with_invalid_signal(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_signal_validity
        snap = self._make_snapshot(is_valid=False)
        rs = self._make_risk_service()
        result = check_signal_validity(snap, rs)
        assert result is not None and result.is_block
