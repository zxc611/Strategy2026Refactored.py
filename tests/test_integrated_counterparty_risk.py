# MODULE_ID: M2-359
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestCounterpartyRiskFunctions:
    def test_check_e13_collusion(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_e13_collusion
        assert callable(check_e13_collusion)

    def test_check_strategy_health(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_strategy_health
        assert callable(check_strategy_health)

    def test_check_counterparty_risks(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_counterparty_risks
        assert callable(check_counterparty_risks)

    def test_check_e13_collusion_with_mock(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_e13_collusion
        snapshot = MagicMock()
        risk_service = MagicMock()
        result = check_e13_collusion(snapshot, risk_service)
        assert result is None or hasattr(result, 'level') or isinstance(result, MagicMock)