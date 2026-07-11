# MODULE_ID: M2-377
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestRegulatoryRiskReexport:
    def test_import(self):
        from ali2026v3_trading.risk_engine import regulatory_risk
        assert regulatory_risk is not None

    def test_check_position_limit(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_position_limit
        assert callable(check_position_limit)

    def test_check_governance_violations(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_governance_violations
        assert callable(check_governance_violations)

    def test_check_margin_sufficiency(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_margin_sufficiency
        assert callable(check_margin_sufficiency)

    def test_check_cross_instrument_limit(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_cross_instrument_limit
        assert callable(check_cross_instrument_limit)

    def test_update_margin_ratio_override(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import update_margin_ratio_override
        assert callable(update_margin_ratio_override)

    def test_check_regulatory_risks(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_regulatory_risks
        assert callable(check_regulatory_risks)