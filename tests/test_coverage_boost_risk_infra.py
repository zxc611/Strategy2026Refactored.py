# MODULE_ID: M2-323
"""
补充测试：risk + infra 行为测试（补充覆盖）
覆盖模块: safety_meta_equity, safety_meta_circuit, risk_circuit_breaker,
          risk_rules, trading_utils, risk_audit_utils
"""
import time
import threading
from unittest.mock import MagicMock, patch

import pytest

from ali2026v3_trading.risk.safety_meta_equity import DrawdownMonitorService
from ali2026v3_trading.risk.safety_meta_circuit import CircuitBreakerService
from ali2026v3_trading.infra.trading_utils import (
    classify_deviation, compute_commission, ThreadSafeCounter, ThreadSafeDict,
    Signal,
)
from ali2026v3_trading.infra.risk_audit_utils import structured_audit_log


class TestDrawdownMonitorService:
    def _make_params(self):
        return MagicMock()

    def _make_owner(self):
        return MagicMock()

    def _make_stats(self):
        return {
            'total_equity_updates': 0,
            'negative_equity_triggers': 0,
            'daily_drawdown_triggers': 0,
            'daily_hard_stop_triggers': 0,
        }

    def test_negative_equity_triggers_hard_stop(self):
        params = self._make_params()
        owner = self._make_owner()
        dms = DrawdownMonitorService(params, owner)
        stats = self._make_stats()
        cb_service = MagicMock()
        dms.on_equity_update(-100.0, stats, cb_service)
        assert dms._daily_hard_stop_triggered is True
        assert dms._daily_new_open_blocked is True
        assert stats['negative_equity_triggers'] == 1

    def test_positive_equity_updates_metrics(self):
        params = self._make_params()
        owner = self._make_owner()
        dms = DrawdownMonitorService(params, owner)
        stats = self._make_stats()
        cb_service = MagicMock()
        dms.on_equity_update(100000.0, stats, cb_service)
        assert stats['total_equity_updates'] == 1
        assert dms._daily_start_equity == 100000.0

    def test_set_prev_5day_avg_profit(self):
        params = self._make_params()
        owner = self._make_owner()
        dms = DrawdownMonitorService(params, owner)
        dms.set_prev_5day_avg_profit(500.0)
        assert dms._prev_5day_avg_profit == 500.0

    def test_get_state_snapshot(self):
        params = self._make_params()
        owner = self._make_owner()
        dms = DrawdownMonitorService(params, owner)
        snapshot = dms.get_state_snapshot()
        assert 'daily_start_equity' in snapshot
        assert 'daily_hard_stop_triggered' in snapshot
        assert 'daily_new_open_blocked' in snapshot

    def test_daily_drawdown_triggers_hard_stop(self):
        params = {'daily_loss_hard_stop_pct': 0.05, 'daily_drawdown_multiplier': 2.0}
        owner = self._make_owner()
        dms = DrawdownMonitorService(params, owner)
        stats = self._make_stats()
        cb_service = MagicMock()
        dms.on_equity_update(100000.0, stats, cb_service)
        dms._daily_drawdown = 0.99
        dms._check_daily_drawdown(stats, save_callback=None)
        assert dms._daily_hard_stop_triggered is True


class TestCircuitBreakerService:
    def test_creation(self):
        cbs = CircuitBreakerService(MagicMock(), MagicMock())
        assert cbs is not None


class TestClassifyDeviation:
    def test_match(self):
        import time as _t
        s1 = Signal(direction='BUY', price=100.0, timestamp=_t.time())
        s2 = Signal(direction='BUY', price=100.0, timestamp=_t.time())
        result = classify_deviation(s1, s2)
        assert result.category == 'MATCH'

    def test_critical_deviation(self):
        import time as _t
        s1 = Signal(direction='BUY', price=100.0, timestamp=_t.time())
        s2 = Signal(direction='SELL', price=100.0, timestamp=_t.time())
        result = classify_deviation(s1, s2)
        assert result.category == 'CRITICAL_DEVIATION'


class TestComputeCommission:
    def test_basic_call(self):
        result = compute_commission('50ETF_OPTION', 1, trade_value=10000.0)
        assert isinstance(result, (int, float))
        assert result >= 0


class TestThreadSafeCounter:
    def test_add_and_get(self):
        counter = ThreadSafeCounter()
        counter.add(5)
        counter.add(3)
        assert counter.get() == 8

    def test_concurrent_adds(self):
        counter = ThreadSafeCounter()
        threads = []
        for _ in range(10):
            t = threading.Thread(target=lambda: counter.add(1))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        assert counter.get() == 10


class TestThreadSafeDict:
    def test_set_and_get(self):
        d = ThreadSafeDict()
        d.set('key', 'value')
        assert d.get('key') == 'value'

    def test_get_default(self):
        d = ThreadSafeDict()
        assert d.get('missing', 'default') == 'default'


class TestStructuredAuditLog:
    def test_callable(self):
        assert callable(structured_audit_log)