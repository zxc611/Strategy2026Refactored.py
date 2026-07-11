# MODULE_ID: M2-373
import sys
import os
import pytest
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestExchangeTime:
    def test_init(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        assert et is not None

    def test_now_cst(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime()
        now = et.now_cst()
        assert now is not None

    def test_get_trade_date_daytime(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 10, 30, tzinfo=timezone(timedelta(hours=8)))
        assert et.get_trade_date(dt) == '2026-06-15'

    def test_get_trade_date_nighttime(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 21, 30, tzinfo=timezone(timedelta(hours=8)))
        assert et.get_trade_date(dt) == '2026-06-16'

    def test_is_trading_hours_day(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 10, 0, tzinfo=timezone(timedelta(hours=8)))
        assert et.is_trading_hours(dt) is True

    def test_is_trading_hours_night(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 21, 0, tzinfo=timezone(timedelta(hours=8)))
        assert et.is_trading_hours(dt) is True

    def test_is_trading_hours_closed(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 16, 0, tzinfo=timezone(timedelta(hours=8)))
        assert et.is_trading_hours(dt) is False

    def test_to_epoch_ms(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime()
        dt = datetime(2026, 6, 15, 10, 0, tzinfo=timezone(timedelta(hours=8)))
        ms = et.to_epoch_ms(dt)
        assert isinstance(ms, int)
        assert ms > 0

    def test_from_epoch_ms_with_seconds(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime()
        dt = datetime(2026, 6, 15, 10, 0, tzinfo=timezone(timedelta(hours=8)))
        epoch_sec = int(dt.timestamp())
        restored = et.from_epoch_ms(epoch_sec)
        assert isinstance(restored, datetime)

    def test_from_epoch_ms_with_millis(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime()
        result = et.from_epoch_ms(1700000000000)
        assert isinstance(result, datetime)


class TestTickAggregator:
    def test_init(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=60, volume_mode='tick')
        assert ta is not None

    def test_update_tick_returns_none_or_bar(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=300, volume_mode='tick')
        result = ta.update_tick(500.0, 1)
        assert result is None or isinstance(result, dict)

    def test_get_current_bar(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=300, volume_mode='tick')
        ta.update_tick(500.0, 1)
        bar = ta.get_current_bar()
        assert bar is not None
        assert 'open' in bar
        assert 'close' in bar

    def test_get_bar_history(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=300, volume_mode='tick')
        ta.update_tick(500.0, 1)
        history = ta.get_bar_history()
        assert isinstance(history, list)

    def test_volume_mode_delta(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=300, volume_mode='delta')
        ta.update_tick(500.0, 100)
        bar = ta.get_current_bar()
        assert bar is not None

    def test_invalid_price_rejected(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=300, volume_mode='tick')
        result = ta.update_tick(-1.0, 1)
        assert result is None
        result = ta.update_tick(0.0, 1)
        assert result is None


class TestAtomicSystemState:
    def test_init(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        assert state is not None

    def test_capture(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        v = state.capture(strategy='running', risk='ok')
        assert isinstance(v, int)
        assert v == 1

    def test_get_snapshot(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        state.capture(strategy='running')
        snap = state.get_snapshot()
        assert isinstance(snap, dict)
        assert 'version' in snap
        assert snap['strategy'] == 'running'

    def test_version_property(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        assert state.version == 0
        state.capture()
        assert state.version == 1
        state.capture()
        assert state.version == 2

    def test_age_ms_property(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        assert state.age_ms == float('inf')
        state.capture()
        assert state.age_ms >= 0


class TestSystemHealthMonitor:
    def test_init(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        assert monitor is not None

    def test_record_latency(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        monitor.record_latency('trend_scorer', 100.0)
        monitor.record_latency('trend_scorer', 200.0)

    def test_record_error(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        monitor.record_error('trend_scorer', 'test_error')

    def test_get_health_report(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        monitor.record_latency('trend_scorer', 100.0)
        monitor.record_error('trend_scorer', 'test_error')
        report = monitor.get_health_report()
        assert isinstance(report, dict)
        assert 'uptime_sec' in report
        assert 'modules' in report
        assert 'trend_scorer' in report['modules']

    def test_health_report_percentiles(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        for i in range(20):
            monitor.record_latency('hmm', float(i * 10))
        report = monitor.get_health_report()
        stats = report['modules']['hmm']
        assert 'p50_latency_us' in stats
        assert 'p95_latency_us' in stats
        assert 'p99_latency_us' in stats