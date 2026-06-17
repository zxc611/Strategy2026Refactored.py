# MODULE_ID: M2-365
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestOperationalRiskFunctions:
    def test_check_strategy_status(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_strategy_status
        assert callable(check_strategy_status)

    def test_check_rate_limit(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_rate_limit
        assert callable(check_rate_limit)

    def test_check_consecutive_loss_protection(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_consecutive_loss_protection
        assert callable(check_consecutive_loss_protection)

    def test_check_invariant_runtime(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_invariant_runtime
        assert callable(check_invariant_runtime)

    def test_check_signal_validity(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_signal_validity
        assert callable(check_signal_validity)

    def test_check_single_trade_risk(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_single_trade_risk
        assert callable(check_single_trade_risk)

    def test_check_sharpe_iron_rule(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_sharpe_iron_rule
        assert callable(check_sharpe_iron_rule)

    def test_check_e7_residual_block(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_e7_residual_block
        assert callable(check_e7_residual_block)

    def test_check_capital_sufficiency_in_trade(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_capital_sufficiency_in_trade
        assert callable(check_capital_sufficiency_in_trade)

    def test_check_shadow_ev(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_shadow_ev
        assert callable(check_shadow_ev)

    def test_check_operational_risks(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_operational_risks
        assert callable(check_operational_risks)

    def test_all_functions_have_uniform_signature(self):
        from ali2026v3_trading.risk_engine import operational_risk
        import inspect
        fn_names = [
            'check_strategy_status', 'check_rate_limit', 'check_consecutive_loss_protection',
            'check_invariant_runtime', 'check_signal_validity', 'check_single_trade_risk',
            'check_sharpe_iron_rule', 'check_e7_residual_block', 'check_capital_sufficiency_in_trade',
            'check_shadow_ev', 'check_operational_risks',
        ]
        for name in fn_names:
            fn = getattr(operational_risk, name)
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert 'snapshot' in params, f"{name} missing 'snapshot' param"
            assert 'risk_service' in params, f"{name} missing 'risk_service' param"