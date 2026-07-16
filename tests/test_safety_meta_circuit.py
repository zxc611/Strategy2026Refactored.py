# MODULE_ID: M2-565
"""测试: risk/safety_meta_circuit.py"""
import time
import pytest
from unittest.mock import MagicMock
from risk.safety_meta_circuit import CircuitBreakerService


class TestCircuitBreakerServiceDeep:
    def _make(self):
        return CircuitBreakerService(MagicMock(), MagicMock())

    def test_is_trading_paused_no_pause(self):
        svc = self._make()
        svc._trading_paused_until = 0
        paused, reason = svc.is_trading_paused()
        assert paused is False

    def test_is_trading_paused_during_pause(self):
        svc = self._make()
        from risk.safety_meta_circuit import CircuitBreakerHalfOpen
        svc._risk_cb_half_open = CircuitBreakerHalfOpen()
        svc._risk_cb_half_open.force_open(open_duration_sec=3600)
        svc._pause_reason = "test_circuit"
        paused, reason = svc.is_trading_paused()
        assert paused is True
        assert "test_circuit" in reason

    def test_is_trading_paused_expired(self):
        svc = self._make()
        svc._trading_paused_until = time.time() - 1
        paused, reason = svc.is_trading_paused()
        assert paused is False

    def test_check_circuit_breaker_callable(self):
        svc = self._make()
        assert callable(svc.check_circuit_breaker)

    def test_check_algo_circuit_breaker_callable(self):
        svc = self._make()
        assert callable(svc.check_algo_circuit_breaker)

    def test_verify_market_safety_before_resume_callable(self):
        svc = self._make()
        assert callable(svc.verify_market_safety_before_resume)

    def test_creation_sets_defaults(self):
        svc = self._make()
        assert hasattr(svc, '_pause_reason')
        assert hasattr(svc, '_risk_cb_half_open')
        assert hasattr(svc, '_algo_paused_until')