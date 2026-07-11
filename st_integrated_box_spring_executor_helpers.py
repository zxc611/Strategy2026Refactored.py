# MODULE_ID: M2-357
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestComputeHedgeRatio:
    def test_default_returns_1(self):
        from ali2026v3_trading.strategy.box_spring_executor import _compute_hedge_ratio
        result = _compute_hedge_ratio({"signal_id": "test"})
        assert result == 1.0

    def test_empty_signal(self):
        from ali2026v3_trading.strategy.box_spring_executor import _compute_hedge_ratio
        result = _compute_hedge_ratio({})
        assert result == 1.0


class TestCheckCrossStrategyRisk:
    def test_default_returns_true(self):
        from ali2026v3_trading.strategy.box_spring_executor import _check_cross_strategy_risk
        result = _check_cross_strategy_risk({"signal_id": "test"})
        assert result is True

    def test_module_function_style(self):
        from ali2026v3_trading.strategy.box_spring_executor import _check_cross_strategy_risk
        result = _check_cross_strategy_risk(self_or_signal={"signal_id": "test"})
        assert result is True

    def test_instance_method_style(self):
        from ali2026v3_trading.strategy.box_spring_executor import _check_cross_strategy_risk
        result = _check_cross_strategy_risk(self_or_signal={"signal_id": "test"})
        assert result is True


class TestRecordSpringTrade:
    def test_module_function_style(self):
        from ali2026v3_trading.strategy.box_spring_executor import _record_spring_trade
        result = _record_spring_trade({"signal_id": "sig_001"})
        assert result["recorded"] is True
        assert result["signal_id"] == "sig_001"

    def test_instance_method_style(self):
        from ali2026v3_trading.strategy.box_spring_executor import _record_spring_trade
        result = _record_spring_trade(self_or_signal=None, signal={"signal_id": "sig_002"})
        assert result["recorded"] is True
        assert result["signal_id"] == "sig_002"

    def test_missing_signal_id(self):
        from ali2026v3_trading.strategy.box_spring_executor import _record_spring_trade
        result = _record_spring_trade({})
        assert result["recorded"] is True
        assert result["signal_id"] == "unknown"


class TestFindStraddlePair:
    def test_module_function_style(self):
        from ali2026v3_trading.strategy.box_spring_executor import _find_straddle_pair
        result = _find_straddle_pair({"instrument_id": "au2506", "premium": 5.0, "option_type": "CALL"})
        assert result == ("au2506", 5.0, "CALL")

    def test_instance_method_style(self):
        from ali2026v3_trading.strategy.box_spring_executor import _find_straddle_pair
        result = _find_straddle_pair(self_or_signal=None, signal={"instrument_id": "rb2510", "premium": 3.0, "option_type": "PUT"})
        assert result == ("rb2510", 3.0, "PUT")

    def test_missing_fields(self):
        from ali2026v3_trading.strategy.box_spring_executor import _find_straddle_pair
        result = _find_straddle_pair({})
        assert result == ("", 0.0, "CALL")