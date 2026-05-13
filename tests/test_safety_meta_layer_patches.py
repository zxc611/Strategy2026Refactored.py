import time
import threading
import pytest
from unittest.mock import patch, MagicMock

from risk_service import SafetyMetaLayer, get_safety_meta_layer, RiskService, RiskCheckResponse


class TestDailyHardStopTrigger:
    def test_hard_stop_not_triggered_initially(self):
        s = SafetyMetaLayer()
        assert not s.is_hard_stop_triggered()
        assert not s.is_new_open_blocked()

    def test_hard_stop_triggered_on_drawdown_exceed(self):
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._daily_peak_equity = 10000.0
        s._current_date = "2026-05-10"
        for _ in range(12):
            s._equity_series.append(10000.0)
            s._equity_timestamps.append(time.time())
        s._equity_series.append(9750.0)
        s._equity_timestamps.append(time.time())
        s._daily_peak_equity = 10000.0
        s._daily_drawdown = (10000.0 - 9750.0) / 10000.0
        current_loss = 10000.0 - 9750.0
        assert current_loss >= 100.0 * 2.0
        s._check_daily_drawdown()
        assert s.is_hard_stop_triggered()
        assert s.is_new_open_blocked()

    def test_hard_stop_triggered_on_5pct_drawdown(self):
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 0
        s._daily_start_equity = 10000.0
        s._daily_peak_equity = 10000.0
        s._current_date = "2026-05-10"
        s._daily_drawdown = 0.05
        for _ in range(12):
            s._equity_series.append(9500.0)
            s._equity_timestamps.append(time.time())
        s._check_daily_drawdown()
        assert s.is_hard_stop_triggered()

    def test_hard_stop_no_retrigger(self):
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._stats["daily_drawdown_triggers"] = 1
        old_triggers = s._stats["daily_drawdown_triggers"]
        s._check_daily_drawdown()
        assert s._stats["daily_drawdown_triggers"] == old_triggers


class TestConfirmDailyResume:
    def test_resume_clears_hard_stop(self):
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_new_open_blocked = True
        result = s.confirm_daily_resume()
        assert result is True
        assert not s.is_hard_stop_triggered()
        assert not s.is_new_open_blocked()

    def test_resume_noop_when_not_triggered(self):
        s = SafetyMetaLayer()
        result = s.confirm_daily_resume()
        assert result is False
        assert not s.is_hard_stop_triggered()

    def test_resume_clears_drawdown(self):
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_new_open_blocked = True
        s._daily_drawdown = 0.06
        s.confirm_daily_resume()
        assert s._daily_drawdown == 0.0


class TestNewDayDoesNotAutoReset:
    def test_new_day_keeps_hard_stop(self):
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_new_open_blocked = True
        s._current_date = "2026-05-09"
        s.on_equity_update(10000.0)
        assert s.is_hard_stop_triggered()


class TestCheckSafetyMetaLayerIntegration:
    def _make_risk_service(self):
        return RiskService(params={})

    def test_hard_stop_blocks_all_trades(self):
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = True
        safety._daily_new_open_blocked = True
        open_signal = {"action": "OPEN", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(open_signal)
        assert result.is_block

    def test_hard_stop_blocks_close_too(self):
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = True
        close_signal = {"action": "CLOSE", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(close_signal)
        assert result.is_block

    def test_new_open_blocked_still_allows_close(self):
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = False
        safety._daily_new_open_blocked = True
        close_signal = {"action": "CLOSE", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(close_signal)
        assert not result.is_block

    def test_new_open_blocked_blocks_open(self):
        svc = self._make_risk_service()
        safety = get_safety_meta_layer(svc.params)
        safety._daily_hard_stop_triggered = False
        safety._daily_new_open_blocked = True
        open_signal = {"action": "OPEN", "symbol": "test", "amount": 1}
        result = svc._check_safety_meta_layer(open_signal)
        assert result.is_block


class TestCircuitBreakerCalmPeriod:
    def test_calm_period_rejects_retrigger(self):
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-05-10"
        now = time.time()
        s._circuit_breaker_calm_until = now + 600
        for i in range(12):
            s._equity_series.append(10000.0 if i < 10 else 9500.0)
            s._equity_timestamps.append(now - (12 - i) * 10)
        old_triggers = s._stats["circuit_breaker_triggers"]
        s._check_circuit_breaker(now)
        assert s._stats["circuit_breaker_calm_rejects"] >= 1
        assert s._stats["circuit_breaker_triggers"] == old_triggers

    def test_after_calm_period_can_trigger(self):
        s = SafetyMetaLayer()
        s._prev_5day_avg_profit = 100.0
        s._daily_start_equity = 10000.0
        s._current_date = "2026-05-10"
        now = time.time()
        s._circuit_breaker_calm_until = now - 1
        for i in range(12):
            val = 10000.0 if i < 10 else 9500.0
            s._equity_series.append(val)
            s._equity_timestamps.append(now - (12 - i) * 10)
        s._check_circuit_breaker(now)
        assert s._stats["circuit_breaker_triggers"] >= 1


class TestGetStats:
    def test_stats_includes_hard_stop(self):
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        stats = s.get_stats()
        assert "hard_stop_triggered" in stats
        assert stats["hard_stop_triggered"] is True

    def test_stats_initial_values(self):
        s = SafetyMetaLayer()
        stats = s.get_stats()
        assert stats["hard_stop_triggered"] is False
        assert stats["daily_hard_stop_triggers"] == 0


class TestHardStopSkipsFurtherChecks:
    def test_on_equity_update_skips_checks_when_hard_stop(self):
        s = SafetyMetaLayer()
        s._daily_hard_stop_triggered = True
        s._daily_start_equity = 10000.0
        s._current_date = "2026-05-10"
        old_cb_triggers = s._stats["circuit_breaker_triggers"]
        old_dd_triggers = s._stats["daily_drawdown_triggers"]
        for i in range(15):
            s.on_equity_update(9500.0)
        assert s._stats["circuit_breaker_triggers"] == old_cb_triggers
        assert s._stats["daily_drawdown_triggers"] == old_dd_triggers
