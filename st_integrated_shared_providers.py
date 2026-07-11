# MODULE_ID: M2-382
import sys
import os
import pytest
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStateProvider:
    def test_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import StateProvider
        assert hasattr(StateProvider, 'state')
        assert hasattr(StateProvider, 'is_running')
        assert hasattr(StateProvider, 'is_paused')

    def test_runtime_checkable(self):
        from ali2026v3_trading.infra.service_contracts import StateProvider
        class MyState:
            @property
            def state(self): return None
            @property
            def is_running(self): return True
            @property
            def is_paused(self): return False
        assert isinstance(MyState(), StateProvider)


class TestLockProvider:
    def test_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import LockProvider
        assert hasattr(LockProvider, 'lock')

    def test_runtime_checkable(self):
        from ali2026v3_trading.infra.service_contracts import LockProvider
        class MyLock:
            @property
            def lock(self): return threading.RLock()
        assert isinstance(MyLock(), LockProvider)


class TestStatsProvider:
    def test_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import StatsProvider
        assert hasattr(StatsProvider, 'stats')
        assert hasattr(StatsProvider, 'record_tick')
        assert hasattr(StatsProvider, 'record_trade')
        assert hasattr(StatsProvider, 'record_signal')
        assert hasattr(StatsProvider, 'record_error')

    def test_runtime_checkable(self):
        from ali2026v3_trading.infra.service_contracts import StatsProvider
        class MyStats:
            @property
            def stats(self): return {}
            def record_tick(self): pass
            def record_trade(self): pass
            def record_signal(self): pass
            def record_error(self, msg): pass
        assert isinstance(MyStats(), StatsProvider)


class TestSafetyProvider:
    def test_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import SafetyProvider
        assert hasattr(SafetyProvider, 'safety_meta_layer')
        assert hasattr(SafetyProvider, 'check_safety')

    def test_runtime_checkable(self):
        from ali2026v3_trading.infra.service_contracts import SafetyProvider
        class MySafety:
            @property
            def safety_meta_layer(self): return None
            def check_safety(self, signal): return None
        assert isinstance(MySafety(), SafetyProvider)


class TestDefaultStateProvider:
    def test_init_default(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStateProvider
        provider = DefaultStateProvider()
        assert provider.state is None
        assert provider.is_running is False
        assert provider.is_paused is False

    def test_init_with_state(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStateProvider
        provider = DefaultStateProvider(initial_state="running")
        assert provider.state == "running"

    def test_satisfies_protocol(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStateProvider, StateProvider
        provider = DefaultStateProvider()
        assert isinstance(provider, StateProvider)


class TestDefaultLockProvider:
    def test_init(self):
        from ali2026v3_trading.infra.service_contracts import DefaultLockProvider
        provider = DefaultLockProvider()
        assert provider.lock is not None
        assert isinstance(provider.lock, type(threading.RLock()))

    def test_satisfies_protocol(self):
        from ali2026v3_trading.infra.service_contracts import DefaultLockProvider, LockProvider
        provider = DefaultLockProvider()
        assert isinstance(provider, LockProvider)


class TestDefaultStatsProvider:
    def test_init(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider
        provider = DefaultStatsProvider()
        assert isinstance(provider.stats, dict)

    def test_record_tick(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider
        provider = DefaultStatsProvider()
        provider.record_tick()
        assert provider.stats['total_ticks'] == 1

    def test_record_trade(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider
        provider = DefaultStatsProvider()
        provider.record_trade()
        assert provider.stats['total_trades'] == 1

    def test_record_signal(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider
        provider = DefaultStatsProvider()
        provider.record_signal()
        assert provider.stats['total_signals'] == 1

    def test_record_error(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider
        provider = DefaultStatsProvider()
        provider.record_error("test error")
        assert provider.stats['errors_count'] == 1
        assert provider.stats['last_error_message'] == "test error"

    def test_satisfies_protocol(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider, StatsProvider
        provider = DefaultStatsProvider()
        assert isinstance(provider, StatsProvider)


class TestDefaultSafetyProvider:
    def test_safety_meta_layer(self):
        from ali2026v3_trading.infra.service_contracts import DefaultSafetyProvider
        provider = DefaultSafetyProvider()
        assert provider.safety_meta_layer is None

    def test_check_safety(self):
        from ali2026v3_trading.infra.service_contracts import DefaultSafetyProvider
        provider = DefaultSafetyProvider()
        result = provider.check_safety({"signal": "test"})
        assert result is None

    def test_satisfies_protocol(self):
        from ali2026v3_trading.infra.service_contracts import DefaultSafetyProvider, SafetyProvider
        provider = DefaultSafetyProvider()
        assert isinstance(provider, SafetyProvider)