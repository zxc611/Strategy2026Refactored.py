# MODULE_ID: M2-362
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestMarketRiskFunctions:
    def test_check_risk_ratio(self):
        from ali2026v3_trading.risk_engine.market_risk import check_risk_ratio
        assert callable(check_risk_ratio)

    def test_check_greeks_limits(self):
        from ali2026v3_trading.risk_engine.market_risk import check_greeks_limits
        assert callable(check_greeks_limits)

    def test_check_life_expectancy(self):
        from ali2026v3_trading.risk_engine.market_risk import check_life_expectancy
        assert callable(check_life_expectancy)

    def test_check_spread_degradation(self):
        from ali2026v3_trading.risk_engine.market_risk import check_spread_degradation
        assert callable(check_spread_degradation)

    def test_check_exchange_status(self):
        from ali2026v3_trading.risk_engine.market_risk import check_exchange_status
        assert callable(check_exchange_status)

    def test_check_price_limit(self):
        from ali2026v3_trading.risk_engine.market_risk import check_price_limit
        assert callable(check_price_limit)

    def test_check_expiry_risk(self):
        from ali2026v3_trading.risk_engine.market_risk import check_expiry_risk
        assert callable(check_expiry_risk)

    def test_check_auction_session(self):
        from ali2026v3_trading.risk_engine.market_risk import check_auction_session
        assert callable(check_auction_session)

    def test_check_market_risks(self):
        from ali2026v3_trading.risk_engine.market_risk import check_market_risks
        assert callable(check_market_risks)

    def test_all_functions_have_uniform_signature(self):
        from ali2026v3_trading.risk_engine import market_risk
        import inspect
        fn_names = [
            'check_risk_ratio', 'check_greeks_limits', 'check_life_expectancy',
            'check_spread_degradation', 'check_exchange_status', 'check_price_limit',
            'check_expiry_risk', 'check_auction_session', 'check_market_risks',
        ]
        for name in fn_names:
            fn = getattr(market_risk, name)
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert 'snapshot' in params, f"{name} missing 'snapshot' param"
            assert 'risk_service' in params, f"{name} missing 'risk_service' param"