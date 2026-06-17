# MODULE_ID: M2-322
"""
补充测试：lifecycle 模块行为测试
覆盖模块: lifecycle_state_machine, lifecycle_manager, lifecycle_monitor, lifecycle_platform
"""
import threading
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ali2026v3_trading.lifecycle.lifecycle_state_machine import (
    StrategyState, LifecycleStateMachine, VALID_STATE_TRANSITIONS,
    _state_key, _state_is,
)
from ali2026v3_trading.lifecycle.lifecycle_manager import LifecycleManager
from ali2026v3_trading.lifecycle.lifecycle_monitor import LifecycleMonitor
from ali2026v3_trading.lifecycle.lifecycle_platform import LifecyclePlatform


class TestLifecycleStateMachine:
    def test_valid_transition_running_to_paused(self):
        sm = LifecycleStateMachine("running")
        assert sm.can_transition("paused") is True
        sm.transition_to("paused")
        assert sm.state == "paused"
        assert sm.is_paused is True

    def test_invalid_transition_destroyed_to_running(self):
        sm = LifecycleStateMachine("destroyed")
        assert sm.can_transition("running") is False
        sm.transition_to("running")
        assert sm.state == "destroyed"

    def test_valid_transition_initializing_to_running(self):
        sm = LifecycleStateMachine("initializing")
        sm.transition_to("running")
        assert sm.state == "running"
        assert sm.is_running is True

    def test_invalid_initial_state_raises(self):
        with pytest.raises(ValueError, match="非法初始状态"):
            LifecycleStateMachine("invalid_state")

    def test_can_transition_to_alias(self):
        sm = LifecycleStateMachine("running")
        assert sm.can_transition_to("paused") == sm.can_transition("paused")

    def test_is_stopped_property(self):
        sm = LifecycleStateMachine("stopped")
        assert sm.is_stopped is True
        assert sm.is_running is False

    def test_is_destroyed_property(self):
        sm = LifecycleStateMachine("destroyed")
        assert sm.is_destroyed is True

    def test_chain_transitions(self):
        sm = LifecycleStateMachine("initializing")
        sm.transition_to("running")
        sm.transition_to("paused")
        sm.transition_to("running")
        sm.transition_to("stopped")
        assert sm.state == "stopped"

    def test_state_key_function(self):
        assert _state_key(StrategyState.RUNNING) == "running"
        assert _state_key(None) == ""

    def test_state_is_function(self):
        assert _state_is(StrategyState.RUNNING, StrategyState.RUNNING) is True
        assert _state_is(StrategyState.RUNNING, StrategyState.PAUSED) is False
        assert _state_is(StrategyState.RUNNING, StrategyState.RUNNING, StrategyState.PAUSED) is True


class TestLifecycleManager:
    def test_valid_transition_returns_true(self):
        lm = LifecycleManager()
        result = lm.transition_to(StrategyState.RUNNING)
        assert result is True
        assert lm.state == StrategyState.RUNNING
        assert lm.is_running is True

    def test_invalid_transition_returns_false(self):
        lm = LifecycleManager()
        result = lm.transition_to(StrategyState.DESTROYED)
        assert result is False
        assert lm.state == StrategyState.INITIALIZING

    def test_transition_to_degraded_sets_not_running(self):
        lm = LifecycleManager()
        lm.transition_to(StrategyState.RUNNING)
        lm.transition_to(StrategyState.DEGRADED)
        assert lm.is_running is False
        assert lm.state == StrategyState.DEGRADED

    def test_get_state(self):
        lm = LifecycleManager()
        assert lm.get_state() == StrategyState.INITIALIZING

    def test_is_running_check(self):
        lm = LifecycleManager()
        lm.transition_to(StrategyState.RUNNING)
        assert lm.is_running_check() is True
        lm.is_paused = True
        assert lm.is_running_check() is False

    def test_is_paused_check(self):
        lm = LifecycleManager()
        assert lm.is_paused_check() is False
        lm.is_paused = True
        assert lm.is_paused_check() is True

    def test_state_property_setter(self):
        lm = LifecycleManager()
        lm.state = StrategyState.RUNNING
        assert lm.state == StrategyState.RUNNING

    def test_destroyed_property(self):
        lm = LifecycleManager()
        lm.destroyed = True
        assert lm.destroyed is True

    def test_initialized_property(self):
        lm = LifecycleManager()
        lm.initialized = True
        assert lm.initialized is True

    def test_lock_property(self):
        lm = LifecycleManager()
        assert hasattr(lm.lock, 'acquire')
        assert hasattr(lm.lock, 'release')
        assert lm.lock.__class__.__name__ == 'RLock'

    def test_transition_to_stopped_clears_running(self):
        lm = LifecycleManager()
        lm.transition_to(StrategyState.RUNNING)
        lm.transition_to(StrategyState.STOPPED)
        assert lm.is_running is False


class TestLifecycleMonitor:
    def _make_provider(self):
        p = MagicMock()
        p._state = StrategyState.RUNNING
        p._is_running = True
        p._is_paused = False
        p._is_trading = True
        p._stats = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': '',
        }
        p._lock = threading.Lock()
        p._e2e_counters = {
            'configured_instruments': 0,
            'first_tick_received': 0,
            'preregistered_instruments': 0,
            'kline_persisted_count': 0,
        }
        p._e2e_shard_enqueued = {}
        p._e2e_shard_persisted = {}
        p.strategy_id = 'test_strategy'
        return p

    def test_get_state(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        assert m.get_state() == StrategyState.RUNNING

    def test_is_running(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        assert m.is_running() is True
        p._is_paused = True
        assert m.is_running() is False

    def test_record_tick(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        m.record_tick()
        m.record_tick()
        assert p._stats['total_ticks'] == 2

    def test_record_trade(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        m.record_trade()
        assert p._stats['total_trades'] == 1

    def test_record_signal(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        m.record_signal()
        assert p._stats['total_signals'] == 1

    def test_record_error(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        m.record_error("test error")
        assert p._stats['errors_count'] == 1
        assert p._stats['last_error_message'] == "test error"

    def test_health_check_error_state(self):
        p = self._make_provider()
        p._state = StrategyState.ERROR
        m = LifecycleMonitor(p)
        result = m.health_check()
        assert result['status'] == 'unhealthy'
        assert any('ERROR' in i for i in result['issues'])

    def test_health_check_degraded_state(self):
        p = self._make_provider()
        p._state = StrategyState.DEGRADED
        m = LifecycleMonitor(p)
        result = m.health_check()
        assert result['status'] == 'degraded'

    def test_health_check_healthy(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        result = m.health_check()
        assert result['status'] == 'healthy'

    def test_get_stats(self):
        p = self._make_provider()
        m = LifecycleMonitor(p)
        stats = m.get_stats()
        assert 'uptime_seconds' in stats
        assert 'ticks_per_second' in stats
        assert 'state' in stats
        assert stats['total_ticks'] == 0

    def test_should_probe_t_type_future(self):
        assert LifecycleMonitor._should_probe_t_type_future('AL') is True
        assert LifecycleMonitor._should_probe_t_type_future('IH') is True
        assert LifecycleMonitor._should_probe_t_type_future('IF') is False


class TestLifecyclePlatform:
    def _make_provider(self):
        return MagicMock()

    def test_bind_platform_apis_sets_ready(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        lp.bind_platform_apis({
            'subscribe': lambda x: None,
            'unsubscribe': lambda x: None,
            'get_kline': lambda: None,
        })
        assert lp.api_ready is True
        assert lp.kline_ready is True

    def test_bind_platform_apis_missing_kline(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        lp.bind_platform_apis({
            'subscribe': lambda x: None,
            'unsubscribe': lambda x: None,
        })
        assert lp.api_ready is True
        assert lp.kline_ready is False

    def test_subscribe_instrument(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        sub_fn = MagicMock()
        lp.bind_platform_apis({'subscribe': sub_fn, 'unsubscribe': MagicMock()})
        lp.subscribe_instrument('AL2401')
        assert 'AL2401' in lp.subscribed_instruments
        sub_fn.assert_called_once_with('AL2401')

    def test_subscribe_duplicate_ignored(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        sub_fn = MagicMock()
        lp.bind_platform_apis({'subscribe': sub_fn, 'unsubscribe': MagicMock()})
        lp.subscribe_instrument('AL2401')
        lp.subscribe_instrument('AL2401')
        assert sub_fn.call_count == 1

    def test_unsubscribe_instrument(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        unsub_fn = MagicMock()
        lp.bind_platform_apis({'subscribe': MagicMock(), 'unsubscribe': unsub_fn})
        lp.subscribe_instrument('AL2401')
        lp.unsubscribe_instrument('AL2401')
        assert 'AL2401' not in lp.subscribed_instruments
        unsub_fn.assert_called_once_with('AL2401')

    def test_mark_degraded(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        assert lp.is_degraded is False
        lp.mark_degraded("API不可用")
        assert lp.is_degraded is True
        assert "API不可用" in lp.degraded_reasons

    def test_clear_degraded(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        lp.mark_degraded("test")
        lp.clear_degraded()
        assert lp.is_degraded is False
        assert len(lp.degraded_reasons) == 0

    def test_check_api_health(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        lp.bind_platform_apis({
            'subscribe': lambda x: None,
            'unsubscribe': lambda x: None,
            'get_kline': lambda: None,
            'get_instrument': MagicMock(),
            'insert_order': MagicMock(),
            'cancel_order': MagicMock(),
        })
        result = lp.check_api_health()
        assert result['subscribe'] is True
        assert result['get_kline'] is True
        assert result['insert_order'] is True

    def test_get_platform_api(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        lp.bind_platform_apis({'test_api': 42})
        assert lp.get_platform_api('test_api') == 42
        assert lp.get_platform_api('nonexistent') is None

    def test_unsubscribe_all(self):
        p = self._make_provider()
        lp = LifecyclePlatform(p)
        unsub_fn = MagicMock()
        lp.bind_platform_apis({'subscribe': MagicMock(), 'unsubscribe': unsub_fn})
        lp.subscribe_instrument('AL2401')
        lp.subscribe_instrument('IF2401')
        lp.unsubscribe_all()
        assert len(lp.subscribed_instruments) == 0