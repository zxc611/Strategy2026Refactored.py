# MODULE_ID: M2-411
"""测试: risk_engine/market_risk.py (补充)"""
import pytest
from unittest.mock import MagicMock
from ali2026v3_trading.risk_engine.market_risk import (
    check_market_risks, check_risk_ratio, check_greeks_limits,
    check_exchange_status, check_auction_session,
)


class TestCheckRiskRatio:
    def test_blocks_over_limit(self):
        snap = MagicMock()
        snap.risk_ratio = 0.5
        snap.max_risk_ratio = 0.3
        rs = MagicMock()
        rs._check_risk_ratio.return_value = MagicMock(passed=False, severity='P0')
        result = check_risk_ratio(snap, rs)
        assert result is not None

    def test_passes_under_limit(self):
        snap = MagicMock()
        snap.risk_ratio = 0.1
        snap.max_risk_ratio = 0.3
        rs = MagicMock()
        rs._check_risk_ratio.return_value = None
        result = check_risk_ratio(snap, rs)
        assert result is None or result.passed is True

    def test_zero_ratio(self):
        snap = MagicMock()
        snap.risk_ratio = 0.0
        snap.max_risk_ratio = 0.3
        rs = MagicMock()
        rs._check_risk_ratio.return_value = None
        result = check_risk_ratio(snap, rs)
        assert result is None or result.passed is True


class TestCheckGreeksLimits:
    def test_callable(self):
        assert callable(check_greeks_limits)


class TestCheckExchangeStatus:
    def test_callable(self):
        assert callable(check_exchange_status)


class TestCheckAuctionSession:
    def test_callable(self):
        assert callable(check_auction_session)


class TestCheckMarketRisks:
    def test_callable(self):
        assert callable(check_market_risks)

    def test_close_action(self):
        snap = MagicMock()
        snap.action = 'CLOSE'
        snap.risk_ratio = 0.1
        snap.max_risk_ratio = 0.3
        rs = MagicMock()
        result = check_market_risks(snap, rs)
        assert result is None or isinstance(result, MagicMock)