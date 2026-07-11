# MODULE_ID: M2-326
"""
补充测试：strategy + order + risk 行为测试
覆盖模块: types, strategy_ecosystem/_models, order_platform_auth,
          safety_meta_audit, safety_meta_equity, strategy_monitoring_layer
"""
import os
import tempfile
import time
import threading
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ali2026v3_trading.strategy.instrument_service import StrategyParams
from ali2026v3_trading.strategy.strategy_ecosystem._models import (
    SlotState, StrategySlot, CapitalRoute, EcosystemTradeRecord,
    VALID_STRATEGY_TRANSITIONS, _require_interface, _require_arg_interface,
)
from ali2026v3_trading.order.order_platform_auth import PlatformAuthenticator
from ali2026v3_trading.risk.risk_support import CircuitBreakerStateStore
from ali2026v3_trading.strategy.strategy_monitoring_layer import StrategyMonitoringLayer


class TestStrategyParams:
    def test_attribute_access(self):
        sp = StrategyParams({'alpha': 0.5, 'beta': 1.2})
        assert sp.alpha == 0.5
        assert sp.beta == 1.2

    def test_missing_attribute_raises(self):
        sp = StrategyParams({'alpha': 0.5})
        with pytest.raises(AttributeError, match="no attribute"):
            sp.nonexistent

    def test_setattr_updates_data(self):
        sp = StrategyParams({'alpha': 0.5})
        sp.alpha = 0.8
        assert sp.alpha == 0.8

    def test_as_dict_returns_copy(self):
        sp = StrategyParams({'alpha': 0.5})
        d = sp.as_dict()
        d['alpha'] = 999
        assert sp.alpha == 0.5

    def test_get_method(self):
        sp = StrategyParams({'alpha': 0.5})
        assert sp.get('alpha') == 0.5
        assert sp.get('missing', 'default') == 'default'

    def test_repr(self):
        sp = StrategyParams({'alpha': 0.5})
        r = repr(sp)
        assert 'StrategyParams' in r
        assert 'alpha' in r

    def test_dir_returns_keys(self):
        sp = StrategyParams({'alpha': 0.5, 'beta': 1.2})
        assert sorted(dir(sp)) == ['alpha', 'beta']


class TestStrategySlot:
    def test_default_state_is_inactive(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft')
        assert slot.state == SlotState.INACTIVE

    def test_validate_valid_transition(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft', state=SlotState.INACTIVE)
        assert slot.validate_state_transition(SlotState.STANDBY) is True

    def test_validate_invalid_transition(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft', state=SlotState.RETIRED)
        assert slot.validate_state_transition(SlotState.ACTIVE) is False

    def test_validate_same_state(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft', state=SlotState.ACTIVE)
        assert slot.validate_state_transition(SlotState.ACTIVE) is True

    def test_record_sub_strategy_trade(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft')
        slot.record_sub_strategy_trade('s1_hft', 100.0)
        slot.record_sub_strategy_trade('s1_hft', -50.0)
        stats = slot._sub_strategy_stats['s1_hft']
        assert stats['trade_count'] == 2
        assert stats['pnl'] == 50.0
        assert stats['wins'] == 1

    def test_get_sub_strategy_ev(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft')
        slot.record_sub_strategy_trade('s1_hft', 100.0)
        slot.record_sub_strategy_trade('s1_hft', 200.0)
        ev = slot.get_sub_strategy_ev('s1_hft')
        assert ev == 150.0

    def test_get_sub_strategy_ev_no_trades(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft')
        assert slot.get_sub_strategy_ev('s1_hft') == 0.0

    def test_to_dict(self):
        slot = StrategySlot(strategy_id='s1', strategy_type='hft')
        d = slot.to_dict()
        assert d['strategy_id'] == 's1'


class TestCapitalRoute:
    def test_default_values(self):
        cr = CapitalRoute()
        assert cr.master_base == 0.60
        assert cr.total_base > 0

    def test_freeze_prevents_setattr(self):
        cr = CapitalRoute()
        cr.freeze()
        with pytest.raises(RuntimeError, match="已冻结"):
            cr.master_base = 0.99

    def test_unfreeze_allows_setattr(self):
        cr = CapitalRoute()
        cr.freeze()
        cr.unfreeze()
        cr.master_base = 0.99
        assert cr.master_base == 0.99

    def test_update_from_dict(self):
        cr = CapitalRoute()
        cr.freeze()
        cr.update_from_dict({'master_base': 0.50, 'reverse_base': 0.30})
        assert cr.master_base == 0.50
        assert cr.reverse_base == 0.30

    def test_total_base(self):
        cr = CapitalRoute(master_base=0.5, reverse_base=0.3, other_base=0.2)
        assert abs(cr.total_base - 1.0) < 0.001

    def test_to_dict_excludes_frozen(self):
        cr = CapitalRoute()
        d = cr.to_dict()
        assert '_frozen' not in d


class TestEcosystemTradeRecord:
    def test_creation(self):
        rec = EcosystemTradeRecord(trade_id='t1', strategy_id='s1', timestamp='2026-01-01')
        assert rec.trade_id == 't1'
        assert rec.is_open is True

    def test_to_dict(self):
        rec = EcosystemTradeRecord(trade_id='t1', strategy_id='s1', timestamp='2026-01-01')
        d = rec.to_dict()
        assert d['trade_id'] == 't1'


class TestRequireInterface:
    def test_missing_attrs_raises_type_error(self):
        @_require_interface(('foo', 'bar'))
        def method(self):
            return True

        obj = MagicMock(spec=[])
        del obj.foo
        del obj.bar
        with pytest.raises(TypeError, match="缺少接口属性"):
            method(obj)

    def test_has_attrs_passes(self):
        @_require_interface(('foo',))
        def method(self):
            return True

        obj = MagicMock()
        obj.foo = 42
        assert method(obj) is True


class TestPlatformAuthenticator:
    def test_set_and_get_token(self):
        auth = PlatformAuthenticator()
        auth.set_token("test_token", expires_in=3600)
        assert auth.get_token() == "test_token"

    def test_expired_token_returns_none(self):
        auth = PlatformAuthenticator()
        auth.set_token("test_token", expires_in=-1)
        assert auth.get_token() is None

    def test_no_token_returns_none(self):
        auth = PlatformAuthenticator()
        assert auth.get_token() is None

    def test_validate_nonce_first_use(self):
        auth = PlatformAuthenticator()
        assert auth.validate_nonce("nonce_1") is True

    def test_validate_nonce_replay_blocked(self):
        auth = PlatformAuthenticator()
        auth.validate_nonce("nonce_1")
        assert auth.validate_nonce("nonce_1") is False

    def test_validate_nonce_expired_timestamp(self):
        auth = PlatformAuthenticator()
        assert auth.validate_nonce("nonce_2", timestamp=time.time() - 600) is False

    def test_generate_signed_request(self):
        auth = PlatformAuthenticator()
        auth.set_token("test_token")
        result = auth.generate_signed_request({'action': 'buy'})
        assert '_nonce' in result
        assert '_timestamp' in result
        assert '_signature' in result
        assert result['action'] == 'buy'


class TestCircuitBreakerStateStore:
    def test_save_and_load_same_day(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CircuitBreakerStateStore()
            store._circuit_breaker_state_path = os.path.join(tmpdir, ".circuit_breaker_state.json")
            state_data = {
                "daily_hard_stop_triggered": True,
                "daily_new_open_blocked": True,
                "daily_start_equity": 100000.0,
                "daily_peak_equity": 105000.0,
                "daily_drawdown": 0.03,
                "prev_5day_avg_profit": 500.0,
                "current_date": time.strftime("%Y-%m-%d"),
            }
            store.save_state(
                trading_paused_until=0, pause_reason="",
                circuit_breaker_calm_until=0, circuit_breaker_shadow_mode=False,
                circuit_breaker_shadow_until=0, circuit_breaker_activated_at=0,
                state_data=state_data,
            )
            loaded = store.load_state()
            assert loaded.get("daily_hard_stop_triggered") is True
            assert loaded.get("daily_new_open_blocked") is True
            assert loaded.get("daily_start_equity") == 100000.0

    def test_load_cross_day_resets_new_open(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CircuitBreakerStateStore()
            store._circuit_breaker_state_path = os.path.join(tmpdir, ".circuit_breaker_state.json")
            yesterday = "2020-01-01"
            state_data = {
                "daily_hard_stop_triggered": True,
                "daily_new_open_blocked": True,
                "daily_start_equity": 100000.0,
                "daily_peak_equity": 105000.0,
                "daily_drawdown": 0.03,
                "prev_5day_avg_profit": 500.0,
                "current_date": yesterday,
            }
            store.save_state(
                trading_paused_until=0, pause_reason="",
                circuit_breaker_calm_until=0, circuit_breaker_shadow_mode=False,
                circuit_breaker_shadow_until=0, circuit_breaker_activated_at=0,
                state_data=state_data,
            )
            loaded = store.load_state()
            assert loaded.get("daily_hard_stop_triggered") is True
            assert loaded.get("daily_new_open_blocked") is False

    def test_load_nonexistent_file(self):
        store = CircuitBreakerStateStore()
        store._circuit_breaker_state_path = "/nonexistent/path/.state.json"
        result = store.load_state()
        assert result == {}


class TestStrategyMonitoringLayer:
    def test_confirm_daily_resume_rejects_auto_caller(self):
        provider = MagicMock()
        sml = StrategyMonitoringLayer(provider)
        for auto_id in ['timer', 'scheduler', 'auto', 'cron', 'periodic', 'heartbeat']:
            assert sml.confirm_daily_resume(caller_id=auto_id) is False

    def test_confirm_daily_resume_allows_manual(self):
        provider = MagicMock()
        sml = StrategyMonitoringLayer(provider)
        with patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', side_effect=ImportError):
            result = sml.confirm_daily_resume(caller_id='human_operator')
            assert result is False

    def test_is_hard_stop_triggered_no_safety(self):
        provider = MagicMock()
        sml = StrategyMonitoringLayer(provider)
        with patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', side_effect=ImportError):
            assert sml.is_hard_stop_triggered() is False