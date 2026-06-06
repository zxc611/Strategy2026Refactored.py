"""P3.6: shared_checks 5个共享函数完整单测"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock


class RiskSnapshotStub:
    def __init__(self, action="OPEN", symbol="IF2606", amount=1.0, price=4000.0, equity=1000000.0):
        self.action = action
        self.symbol = symbol
        self.instrument_id = symbol
        self.amount = amount
        self.price = price
        self.equity = equity
        self.account_id = "test"
        self.hedge_type = "none"
        self.signal = {}


def _make_risk_service():
    rs = MagicMock()
    rs._check_position_limit.return_value = None
    rs._check_governance_violations.return_value = None
    rs.check_margin_sufficiency.return_value = MagicMock(is_block=False)
    rs.check_cross_instrument_limit.return_value = MagicMock(is_block=False)
    rs.update_margin_ratio.return_value = None
    rs._record_signal_time.return_value = None
    return rs


class TestCheckPositionLimit:
    def test_delegates_to_risk_service(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_position_limit
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        check_position_limit(snap, rs)
        rs._check_position_limit.assert_called_once()

    def test_returns_none_when_pass(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_position_limit
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        rs._check_position_limit.return_value = None
        result = check_position_limit(snap, rs)
        assert result is None


class TestCheckGovernanceViolations:
    def test_delegates_to_risk_service(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_governance_violations
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        check_governance_violations(snap, rs)
        rs._check_governance_violations.assert_called_once_with(snap.signal)


class TestCheckMarginSufficiency:
    def test_returns_none_when_sufficient(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_margin_sufficiency
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        result = check_margin_sufficiency(snap, rs)
        assert result is None

    def test_returns_none_when_zero_equity(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_margin_sufficiency
        snap = RiskSnapshotStub(equity=0.0)
        rs = _make_risk_service()
        result = check_margin_sufficiency(snap, rs)
        assert result is None

    def test_returns_none_when_zero_volume(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_margin_sufficiency
        snap = RiskSnapshotStub(amount=0.0)
        rs = _make_risk_service()
        result = check_margin_sufficiency(snap, rs)
        assert result is None


class TestCheckCrossInstrumentLimit:
    def test_returns_none_when_pass(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_cross_instrument_limit
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        result = check_cross_instrument_limit(snap, rs)
        assert result is None


class TestCheckRegulatoryRisks:
    def test_returns_none_when_all_pass(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_regulatory_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        result = check_regulatory_risks(snap, rs)
        assert result is None

    def test_position_limit_blocks_first(self):
        from ali2026v3_trading.risk_engine.shared_checks import check_regulatory_risks
        from ali2026v3_trading.risk_service import RiskCheckResponse, RiskLevel
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        rs._check_position_limit.return_value = RiskCheckResponse.block_result(
            "position_limit", "exceeded", RiskLevel.HIGH
        )
        result = check_regulatory_risks(snap, rs)
        assert result is not None and result.is_block
