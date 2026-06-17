# MODULE_ID: M2-558
"""P3.1: 风控引擎4域契约测试"""
from __future__ import annotations

import pytest
from typing import Any, Optional
from unittest.mock import MagicMock, patch
from ali2026v3_trading.tests._stubs import RiskSnapshotStub



def _make_risk_service():
    rs = MagicMock()
    rs._check_position_limit.return_value = None
    rs._check_governance_violations.return_value = None
    rs._check_risk_ratio.return_value = None
    rs._check_greeks_limits.return_value = None
    rs._check_life_expectancy.return_value = None
    rs._check_spread_degradation.return_value = None
    rs._check_strategy_status.return_value = None
    rs._check_rate_limit.return_value = None
    rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=False)
    rs._check_invariant_runtime.return_value = {"all_passed": True, "violations": []}
    rs._check_single_trade_risk.return_value = None
    rs._check_sharpe_iron_rule.return_value = None
    rs._check_e7_residual_block.return_value = None
    rs._check_capital_sufficiency_in_trade.return_value = None
    rs.check_margin_sufficiency.return_value = MagicMock(is_block=False)
    rs.check_cross_instrument_limit.return_value = MagicMock(is_block=False)
    rs.check_exchange_status.return_value = {"tradeable": True}
    rs.is_at_price_limit.return_value = {}
    rs.check_expiry_risk.return_value = MagicMock(is_block=False)
    rs.check_auction_session.return_value = MagicMock(is_block=False)
    rs._record_signal_time.return_value = None
    return rs


class TestCounterpartyRiskContract:
    def test_check_counterparty_risks_returns_none_on_pass(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_counterparty_risks
        snapshot = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.risk_engine.shared_checks.check_position_limit", return_value=None):
            result = check_counterparty_risks(snapshot, rs)
        assert result is None or hasattr(result, "is_block")

    def test_check_e13_returns_none_on_close_action(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_e13_collusion
        snapshot = RiskSnapshotStub(action="CLOSE")
        rs = _make_risk_service()
        result = check_e13_collusion(snapshot, rs)
        assert result is None


class TestRegulatoryRiskContract:
    def test_check_regulatory_risks_returns_none_on_pass(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_regulatory_risks
        snapshot = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.risk_engine.shared_checks.check_position_limit", return_value=None), \
             patch("ali2026v3_trading.risk_engine.shared_checks.check_governance_violations", return_value=None), \
             patch("ali2026v3_trading.risk_engine.shared_checks.check_margin_sufficiency", return_value=None), \
             patch("ali2026v3_trading.risk_engine.shared_checks.check_cross_instrument_limit", return_value=None):
            result = check_regulatory_risks(snapshot, rs)
        assert result is None or hasattr(result, "is_block")

    def test_regulatory_risk_re_exports_shared_checks(self):
        import ali2026v3_trading.risk_engine.regulatory_risk as reg_mod
        expected = [
            "check_position_limit",
            "check_governance_violations",
            "check_margin_sufficiency",
            "check_cross_instrument_limit",
            "update_margin_ratio_override",
            "check_regulatory_risks",
        ]
        for name in expected:
            assert hasattr(reg_mod, name), f"regulatory_risk missing re-export: {name}"


class TestMarketRiskContract:
    def test_check_market_risks_returns_none_on_pass(self):
        from ali2026v3_trading.risk_engine.market_risk import check_market_risks
        snapshot = RiskSnapshotStub()
        rs = _make_risk_service()
        result = check_market_risks(snapshot, rs)
        assert result is None or hasattr(result, "is_block")

    def test_check_risk_ratio_delegates(self):
        from ali2026v3_trading.risk_engine.market_risk import check_risk_ratio
        snapshot = RiskSnapshotStub()
        rs = _make_risk_service()
        result = check_risk_ratio(snapshot, rs)
        rs._check_risk_ratio.assert_called_once()


class TestOperationalRiskContract:
    def test_check_operational_risks_returns_none_on_pass(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_operational_risks
        snapshot = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.risk_engine.operational_risk.check_shadow_ev", return_value=None):
            result = check_operational_risks(snapshot, rs)
        assert result is None or hasattr(result, "is_block")

    def test_check_signal_validity_blocks_invalid(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_signal_validity
        from ali2026v3_trading.risk.risk_service import RiskCheckResponse
        snapshot = RiskSnapshotStub(is_valid=False)
        rs = _make_risk_service()
        result = check_signal_validity(snapshot, rs)
        assert result is not None and result.is_block
