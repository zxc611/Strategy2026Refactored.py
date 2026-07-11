# MODULE_ID: M2-410
"""P3.6: market_risk领域单测"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from ali2026v3_trading.tests._stubs import RiskSnapshotStub



def _make_risk_service():
    rs = MagicMock()
    rs._check_risk_ratio.return_value = None
    rs._check_greeks_limits.return_value = None
    rs._check_life_expectancy.return_value = None
    rs._check_spread_degradation.return_value = None
    rs.check_exchange_status.return_value = {"tradeable": True}
    rs.is_at_price_limit.return_value = {}
    rs.check_expiry_risk.return_value = MagicMock(is_block=False)
    rs.check_auction_session.return_value = MagicMock(is_block=False)
    return rs


class TestCheckMarketRisks:
    def test_returns_none_when_all_pass(self):
        from ali2026v3_trading.risk_engine.market_risk import check_market_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        result = check_market_risks(snap, rs)
        assert result is None

    def test_risk_ratio_blocks(self):
        from ali2026v3_trading.risk_engine.market_risk import check_market_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        rs._check_risk_ratio.return_value = MagicMock(is_block=True)
        result = check_market_risks(snap, rs)
        assert result is not None and result.is_block

    def test_greeks_limits_blocks(self):
        from ali2026v3_trading.risk_engine.market_risk import check_market_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        rs._check_greeks_limits.return_value = MagicMock(is_block=True)
        result = check_market_risks(snap, rs)
        assert result is not None and result.is_block

    def test_exchange_not_tradeable_blocks(self):
        from ali2026v3_trading.risk_engine.market_risk import check_exchange_status
        from ali2026v3_trading.risk.risk_service import RiskLevel
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        rs.check_exchange_status.return_value = {"tradeable": False, "reason": "closed"}
        result = check_exchange_status(snap, rs)
        assert result is not None and result.is_block
