# MODULE_ID: M2-381
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestSharedChecksFunctions:
    def test_check_position_limit(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_position_limit
        assert callable(check_position_limit)

    def test_check_governance_violations(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_governance_violations
        assert callable(check_governance_violations)

    def test_check_margin_sufficiency(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_margin_sufficiency
        assert callable(check_margin_sufficiency)

    def test_check_cross_instrument_limit(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_cross_instrument_limit
        assert callable(check_cross_instrument_limit)

    def test_update_margin_ratio_override(self):
        from ali2026v3_trading.risk_engine.shared_checks import update_margin_ratio_override
        assert callable(update_margin_ratio_override)

    def test_check_regulatory_risks(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_regulatory_risks
        assert callable(check_regulatory_risks)

    def test_all_functions_have_uniform_signature(self):
        from ali2026v3_trading.risk_engine import shared_checks
        import inspect
        fn_names = [
            'check_position_limit', 'check_governance_violations', 'check_margin_sufficiency',
            'check_cross_instrument_limit', 'update_margin_ratio_override', 'check_regulatory_risks',
        ]
        for name in fn_names:
            fn = getattr(shared_checks, name)
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())
            assert 'snapshot' in params, f"{name} missing 'snapshot' param"
            assert 'risk_service' in params, f"{name} missing 'risk_service' param"