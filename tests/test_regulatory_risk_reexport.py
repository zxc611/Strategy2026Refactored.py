# MODULE_ID: M2-553
"""P3.6: regulatory_risk re-export验证"""
from __future__ import annotations

import pytest


class TestRegulatoryRiskReExport:
    def test_check_position_limit_reexported(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_position_limit
        assert callable(check_position_limit)

    def test_check_governance_violations_reexported(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_governance_violations
        assert callable(check_governance_violations)

    def test_check_margin_sufficiency_reexported(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_margin_sufficiency
        assert callable(check_margin_sufficiency)

    def test_check_cross_instrument_limit_reexported(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_cross_instrument_limit
        assert callable(check_cross_instrument_limit)

    def test_check_regulatory_risks_reexported(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import check_regulatory_risks
        assert callable(check_regulatory_risks)

    def test_update_margin_ratio_override_reexported(self):
        from ali2026v3_trading.risk_engine.regulatory_risk import update_margin_ratio_override
        assert callable(update_margin_ratio_override)

    def test_reexported_functions_match_shared_checks(self):
        import ali2026v3_trading.risk_engine.regulatory_risk as reg_mod
        import ali2026v3_trading.risk_engine.shared_checks as sc_mod
        names = [
            "check_position_limit",
            "check_governance_violations",
            "check_margin_sufficiency",
            "check_cross_instrument_limit",
            "update_margin_ratio_override",
            "check_regulatory_risks",
        ]
        for name in names:
            reg_fn = getattr(reg_mod, name)
            sc_fn = getattr(sc_mod, name)
            assert reg_fn is sc_fn, f"{name} re-export is not same object as shared_checks"
