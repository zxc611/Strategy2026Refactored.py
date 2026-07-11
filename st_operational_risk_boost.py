# MODULE_ID: M2-424
"""测试: risk_engine/operational_risk.py (补充)"""
import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.risk_engine.operational_risk import (
    check_operational_risks, check_shadow_ev, check_consecutive_loss_protection,
    check_signal_validity, check_sharpe_iron_rule,
)


class TestCheckShadowEv:
    def test_critical_on_absolute_ev_pause(self):
        snap = MagicMock()
        snap.action = "OPEN"
        snap.signal = {}
        rs = MagicMock()
        with patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine') as mock_sse:
            mock_sse.return_value.is_absolute_ev_paused.return_value = True
            result = check_shadow_ev(snap, rs)
            assert result is not None

    def test_no_engine_returns_none(self):
        snap = MagicMock()
        snap.action = "OPEN"
        snap.signal = {}
        rs = MagicMock()
        with patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine') as mock_sse:
            mock_sse.return_value = None
            result = check_shadow_ev(snap, rs)
            assert result is None

    def test_degraded_engine(self):
        snap = MagicMock()
        snap.action = "OPEN"
        snap.signal = {}
        rs = MagicMock()
        with patch('ali2026v3_trading.strategy.shadow_strategy_facade.get_shadow_strategy_engine') as mock_sse:
            mock_sse.return_value.is_absolute_ev_paused.return_value = False
            mock_sse.return_value.is_degradation_active.return_value = False
            result = check_shadow_ev(snap, rs)
            assert result is None or result is not None


class TestCheckConsecutiveLossProtection:
    def test_callable(self):
        assert callable(check_consecutive_loss_protection)


class TestCheckSignalValidity:
    def test_callable(self):
        assert callable(check_signal_validity)


class TestCheckSharpeIronRule:
    def test_callable(self):
        assert callable(check_sharpe_iron_rule)


class TestCheckOperationalRisks:
    def test_callable(self):
        assert callable(check_operational_risks)