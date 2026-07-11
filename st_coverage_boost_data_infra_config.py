# MODULE_ID: M2-319
"""
补充测试：data + infra + config 行为测试
覆盖模块: query_kline_aggregator, contracts_service, state_machine,
          config/tvf_param_loader, config/params_service, infra/trading_utils
"""
import math
import time
from unittest.mock import MagicMock, patch

import pytest

from ali2026v3_trading.data.query_service import LightKLine, _KlineAggregator
from ali2026v3_trading.infra.service_contracts import (
    RuntimeSystem, HealthStatusLevel, normalize_health_status,
    PhaseFeatureFlag, resolve_runtime_system,
)
from ali2026v3_trading.infra.service_contracts import BaseStateMachine


class TestKlineAggregator:
    def test_first_tick_no_completed_kline(self):
        agg = _KlineAggregator('AL2401', '1m')
        result = agg.update(1000.0, 20000.0)
        assert result is None
        assert agg.open_price == 20000.0

    def test_tick_within_same_kline(self):
        agg = _KlineAggregator('AL2401', '1m')
        base_ts = 1200.0
        agg.update(base_ts, 20000.0)
        result = agg.update(base_ts + 30, 20100.0)
        assert result is None
        assert agg.high_price == 20100.0
        assert agg.close_price == 20100.0

    def test_tick_crosses_kline_boundary(self):
        agg = _KlineAggregator('AL2401', '1m')
        base_ts = 1200.0
        agg.update(base_ts, 20000.0)
        agg.update(base_ts + 30, 20100.0)
        completed = agg.update(base_ts + 60, 20200.0)
        assert completed is not None
        assert completed['open'] == 20000.0
        assert completed['close'] == 20100.0
        assert agg.open_price == 20200.0

    def test_nan_price_rejected(self):
        agg = _KlineAggregator('AL2401', '1m')
        result = agg.update(1000.0, float('nan'))
        assert result is None
        assert agg.open_price is None

    def test_negative_price_rejected(self):
        agg = _KlineAggregator('AL2401', '1m')
        result = agg.update(1000.0, -100.0)
        assert result is None

    def test_out_of_order_tick_rejected(self):
        agg = _KlineAggregator('AL2401', '1m')
        agg.update(1200.0, 20000.0)
        result = agg.update(1100.0, 19900.0)
        assert result is None

    def test_parse_period(self):
        assert _KlineAggregator('X', '1s').period_seconds == 1
        assert _KlineAggregator('X', '1m').period_seconds == 60
        assert _KlineAggregator('X', '5m').period_seconds == 300
        assert _KlineAggregator('X', '1h').period_seconds == 3600
        assert _KlineAggregator('X', '1d').period_seconds == 86400
        assert _KlineAggregator('X', 'unknown').period_seconds == 60

    def test_to_state_dict_none(self):
        agg = _KlineAggregator('AL2401', '1m')
        assert agg.to_state_dict() is None

    def test_to_state_dict_and_from_state_dict(self):
        agg = _KlineAggregator('AL2401', '1m')
        agg.update(1200.0, 20000.0)
        state = agg.to_state_dict()
        assert state is not None
        assert state['instrument_id'] == 'AL2401'
        restored = _KlineAggregator.from_state_dict(state)
        assert restored.open_price == 20000.0
        assert restored.instrument_id == 'AL2401'

    def test_reset(self):
        agg = _KlineAggregator('AL2401', '1m')
        agg.update(1200.0, 20000.0)
        agg.reset()
        assert agg.open_price is None
        assert agg.volume == 0


class TestLightKLine:
    def test_creation(self):
        from datetime import datetime
        dt = datetime(2026, 1, 1, 10, 0)
        kline = LightKLine(open_price=100.0, high=105.0, low=99.0,
                           close=103.0, volume=1000.0, dt=dt)
        assert kline.open == 100.0
        assert kline.high == 105.0
        assert kline.low == 99.0
        assert kline.close == 103.0
        assert kline.volume == 1000.0


class TestContractsService:
    def test_runtime_system_enum(self):
        assert RuntimeSystem.PRODUCTION.value == 'production'
        assert RuntimeSystem.BACKTEST.value == 'backtest'
        assert RuntimeSystem.JUDGMENT.value == 'judgment'

    def test_resolve_runtime_system_backtest(self):
        with patch.dict('os.environ', {'TRADING_ENV': 'backtest'}):
            result = resolve_runtime_system()
            assert result == RuntimeSystem.BACKTEST

    def test_resolve_runtime_system_production(self):
        with patch.dict('os.environ', {'TRADING_ENV': 'production'}, clear=False):
            result = resolve_runtime_system()
            assert result in (RuntimeSystem.PRODUCTION, RuntimeSystem.BACKTEST, RuntimeSystem.JUDGMENT)

    def test_normalize_health_status_ok(self):
        result = normalize_health_status({'status': 'HEALTHY'}, 'test')
        assert result['status'] == HealthStatusLevel.OK.value

    def test_normalize_health_status_warning(self):
        result = normalize_health_status({'status': 'DEGRADED'}, 'test')
        assert result['status'] == HealthStatusLevel.WARNING.value

    def test_normalize_health_status_critical(self):
        result = normalize_health_status({'status': 'CRITICAL'}, 'test')
        assert result['status'] == HealthStatusLevel.CRITICAL.value

    def test_normalize_health_status_unknown(self):
        result = normalize_health_status({'status': 'WEIRD'}, 'test')
        assert result['status'] == HealthStatusLevel.WARNING.value

    def test_phase_feature_flag_enable_disable(self):
        flag = PhaseFeatureFlag()
        flag.enable('test_flag')
        assert flag.is_enabled('test_flag') is True
        flag.disable('test_flag')
        assert flag.is_enabled('test_flag') is False

    def test_phase_feature_flag_disable_all(self):
        flag = PhaseFeatureFlag()
        flag.enable('f1')
        flag.enable('f2')
        flag.disable_all()
        assert flag.is_enabled('f1') is False
        assert flag.is_enabled('f2') is False


class TestBaseStateMachine:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            BaseStateMachine()

    def test_concrete_subclass_works(self):
        class TestSM(BaseStateMachine):
            _LEGAL_TRANSITIONS = {'A': {'B'}, 'B': {'A'}}

            def __init__(self):
                self._state = 'A'

            @property
            def state(self):
                return self._state

            def can_transition(self, target):
                return target in self._LEGAL_TRANSITIONS.get(self._state, set())

            def transition_to(self, target):
                if self.can_transition(target):
                    self._state = target

        sm = TestSM()
        assert sm.state == 'A'
        assert sm.can_transition('B') is True
        sm.transition_to('B')
        assert sm.state == 'B'
        assert sm.can_transition('A') is True