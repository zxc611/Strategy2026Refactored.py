# MODULE_ID: M2-316
"""P3.6: counterparty_risk领域单测"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.tests._stubs import RiskSnapshotStub



def _make_risk_service():
    rs = MagicMock()
    rs._check_position_limit.return_value = None
    return rs


class TestCheckCounterpartyRisks:
    def test_returns_none_when_all_pass(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_counterparty_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.risk_engine.counterparty_risk.check_position_limit", return_value=None), \
             patch("ali2026v3_trading.risk_engine.counterparty_risk.check_e13_collusion", return_value=None), \
             patch("ali2026v3_trading.risk_engine.counterparty_risk.check_strategy_health", return_value=None):
            result = check_counterparty_risks(snap, rs)
        assert result is None

    def test_position_limit_blocks(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_counterparty_risks
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        block = MagicMock(is_block=True)
        with patch("ali2026v3_trading.risk_engine.counterparty_risk.check_position_limit", return_value=block):
            result = check_counterparty_risks(snap, rs)
        assert result is not None and result.is_block

    def test_e13_on_close_returns_none(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_e13_collusion
        snap = RiskSnapshotStub(action="CLOSE")
        rs = _make_risk_service()
        result = check_e13_collusion(snap, rs)
        assert result is None

    def test_strategy_health_degraded_not_block(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import check_strategy_health
        snap = RiskSnapshotStub()
        rs = _make_risk_service()
        with patch("ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem") as mock_eco, \
             patch("ali2026v3_trading.infra.event_bus.get_global_event_bus", return_value=None):
            eco = MagicMock()
            eco.get_health_status.return_value = {"status": "DEGRADED"}
            mock_eco.return_value = eco
            result = check_strategy_health(snap, rs)
        assert result is None
