# MODULE_ID: M2-423
"""P3.6: operational_risk领域单测"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.tests._stubs import RiskSnapshotStub



def _make_risk_service():
    rs = MagicMock()
    rs._check_strategy_status.return_value = None
    rs._check_rate_limit.return_value = None
    rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=False)
    rs._check_invariant_runtime.return_value = {"all_passed": True, "violations": []}
    rs._check_single_trade_risk.return_value = None
    rs._check_sharpe_iron_rule.return_value = None
    rs._check_e7_residual_block.return_value = None
    rs._check_capital_sufficiency_in_trade.return_value = None
    rs._fire_alert.return_value = None
    return rs


class TestCheckOperationalRisks:
    def test_returns_none_when_all_pass(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_operational_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.risk_engine.operational_risk.check_shadow_ev", return_value=None):
            result = check_operational_risks(snap, rs)
        assert result is None

    def test_signal_validity_blocks_invalid(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_signal_validity
        snap = RiskSnapshotStub(is_valid=False)
        rs = _make_risk_service()
        result = check_signal_validity(snap, rs)
        assert result is not None and result.is_block

    def test_consecutive_loss_can_block(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_consecutive_loss_protection
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        rs._check_consecutive_loss_protection.return_value = MagicMock(is_block=True)
        result = check_consecutive_loss_protection(snap, rs)
        assert result is not None and result.is_block

    def test_strategy_status_blocks(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_strategy_status
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        block = MagicMock(is_block=True)
        rs._check_strategy_status.return_value = block
        result = check_strategy_status(snap, rs)
        assert result is not None and result.is_block

    def test_rate_limit_blocks(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_rate_limit
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        block = MagicMock(is_block=True)
        rs._check_rate_limit.return_value = block
        result = check_rate_limit(snap, rs)
        assert result is not None and result.is_block

    def test_shadow_ev_critical_blocks(self):
        from ali2026v3_trading.risk_engine.operational_risk import check_shadow_ev
        from ali2026v3_trading.risk.risk_service import RiskLevel
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine") as mock_sse:
            sse = MagicMock()
            sse.is_absolute_ev_paused.return_value = True
            mock_sse.return_value = sse
            result = check_shadow_ev(snap, rs)
        assert result is not None and result.is_block
