# MODULE_ID: M2-320
"""
深度行为测试：shadow_strategy_core, strategy_ecosystem/services, tick_hft
覆盖模块: shadow_strategy_core.py, strategy_ecosystem/services.py, tick_hft.py
"""
import time
import threading
from unittest.mock import MagicMock, patch
from dataclasses import asdict

import pytest

from ali2026v3_trading.strategy.shadow_strategy_core import (
    ShadowTradeRecord, AlphaMetrics, ShadowParamsSnapshot,
    ShadowStrategyCoreService, ConfigError,
)
from ali2026v3_trading.strategy.strategy_ecosystem._models import (
    SlotState, StrategySlot, CapitalRoute, VALID_STRATEGY_TRANSITIONS,
)
from ali2026v3_trading.strategy.tick_hft import (
    DynamicPursuitEngine, PursuitPosition, PyramidAddPositionEngine,
    check_hard_time_stop_for_position,
)


# ═══════════════════════════════════════════════════════════════
# shadow_strategy_core.py 深度测试
# ═══════════════════════════════════════════════════════════════

class TestShadowTradeRecord:
    def test_creation_defaults(self):
        rec = ShadowTradeRecord(trade_id='t1', shadow_type='shadow_a', timestamp='2026-01-01')
        assert rec.trade_id == 't1'
        assert rec.shadow_type == 'shadow_a'
        assert rec.is_open is True
        assert rec.pnl == 0.0
        assert rec.net_pnl == 0.0

    def test_to_dict(self):
        rec = ShadowTradeRecord(trade_id='t1', shadow_type='shadow_a', timestamp='2026-01-01',
                                instrument_id='AL2401', direction='BUY', price=20000.0)
        d = rec.to_dict()
        assert d['trade_id'] == 't1'
        assert d['instrument_id'] == 'AL2401'
        assert d['price'] == 20000.0

    def test_with_pnl(self):
        rec = ShadowTradeRecord(trade_id='t2', shadow_type='shadow_b', timestamp='2026-01-01',
                                pnl=500.0, commission=15.0, net_pnl=485.0)
        assert rec.pnl == 500.0
        assert rec.commission == 15.0
        assert rec.net_pnl == 485.0


class TestAlphaMetrics:
    def test_creation_defaults(self):
        m = AlphaMetrics(timestamp='2026-01-01')
        assert m.s1_master_sharpe == 0.0
        assert m.alpha_ratio == 0.0
        assert m.degradation_triggered is False
        assert m.master_sharpe_eliminate is False

    def test_to_dict(self):
        m = AlphaMetrics(timestamp='2026-01-01', s1_master_sharpe=2.5, alpha_ratio=0.3)
        d = m.to_dict()
        assert d['s1_master_sharpe'] == 2.5
        assert d['alpha_ratio'] == 0.3

    def test_all_strategy_groups_present(self):
        m = AlphaMetrics(timestamp='2026-01-01')
        for g in range(1, 7):
            assert hasattr(m, f's{g}_master_sharpe')
            assert hasattr(m, f's{g}_shadow_a_sharpe')
            assert hasattr(m, f's{g}_shadow_b_sharpe')
            assert hasattr(m, f's{g}_alpha')


class TestShadowParamsSnapshot:
    def test_creation(self):
        sp = ShadowParamsSnapshot(shadow_type='shadow_a', locked_at='2026-01-01T00:00:00',
                                  param_set={'key': 'value'}, param_hash='abc123')
        assert sp.shadow_type == 'shadow_a'
        assert sp.param_set == {'key': 'value'}
        assert sp.param_hash == 'abc123'

    def test_to_dict(self):
        sp = ShadowParamsSnapshot(shadow_type='shadow_b', locked_at='2026-01-01',
                                  param_set={'a': 1}, param_yaml_path='/path/to/yaml')
        d = sp.to_dict()
        assert d['shadow_type'] == 'shadow_b'
        assert d['param_yaml_path'] == '/path/to/yaml'


class TestShadowStrategyCoreServiceDataClasses:
    def test_strategy_groups_constant(self):
        expected = ['s1_hft', 's2_resonance', 's3_box', 's4_spring', 's5_arbitrage', 's6_market_making']
        assert ShadowStrategyCoreService.STRATEGY_GROUPS == expected

    def test_class_constants(self):
        assert ShadowStrategyCoreService.ALPHA_DECLINE_THRESHOLD_PCT == 20.0
        assert ShadowStrategyCoreService.CONSECUTIVE_DECLINE_LIMIT == 2
        assert ShadowStrategyCoreService.ABSOLUTE_EV_FLOOR == -0.5
        assert ShadowStrategyCoreService.DEFAULT_COMMISSION_RATE == 0.00003

    def test_validate_shadow_param_independence_passes(self):
        svc = MagicMock(spec=ShadowStrategyCoreService)
        svc._lock = threading.RLock()
        shadow_a = {"close_take_profit_ratio": 0.5, "close_stop_loss_ratio": 0.1, "max_risk_ratio": 0.02, "max_hold_minutes": 30}
        shadow_b = {"close_take_profit_ratio": 1.5, "close_stop_loss_ratio": 0.3, "max_risk_ratio": 0.05, "max_hold_minutes": 60}
        result = ShadowStrategyCoreService._validate_shadow_param_independence(svc, shadow_a, shadow_b)
        assert result is True

    def test_validate_shadow_param_independence_fails_similar(self):
        svc = MagicMock(spec=ShadowStrategyCoreService)
        svc._lock = threading.RLock()
        shadow_a = {"close_take_profit_ratio": 0.5, "close_stop_loss_ratio": 0.1}
        shadow_b = {"close_take_profit_ratio": 0.51, "close_stop_loss_ratio": 0.101}
        result = ShadowStrategyCoreService._validate_shadow_param_independence(svc, shadow_a, shadow_b)
        assert result is False

    def test_validate_shadow_param_independence_no_comparable(self):
        svc = MagicMock(spec=ShadowStrategyCoreService)
        svc._lock = threading.RLock()
        result = ShadowStrategyCoreService._validate_shadow_param_independence(svc, {}, {})
        assert result is True

    def test_mask_sensitive_value(self):
        result = ShadowStrategyCoreService._mask_sensitive_value(None, 123456)
        assert '12' in result
        assert '**' in result

    def test_estimate_commission(self):
        svc = MagicMock(spec=ShadowStrategyCoreService)
        svc.DEFAULT_COMMISSION_RATE = 0.00003
        result = ShadowStrategyCoreService._estimate_commission(svc, 20000.0, 1)
        assert result >= 0

    def test_generate_trade_id(self):
        svc = MagicMock(spec=ShadowStrategyCoreService)
        svc._lock = threading.RLock()
        svc._trade_id_counter = 0
        tid = ShadowStrategyCoreService._generate_trade_id(svc, 'shadow_a')
        assert tid.startswith('SHADOW-shadow_a-')
        assert svc._trade_id_counter == 1

    def test_compute_param_hash(self):
        h1 = ShadowStrategyCoreService._compute_param_hash(None, {'a': 1, 'b': 2})
        h2 = ShadowStrategyCoreService._compute_param_hash(None, {'b': 2, 'a': 1})
        h3 = ShadowStrategyCoreService._compute_param_hash(None, {'a': 1, 'b': 3})
        assert h1 == h2
        assert h1 != h3


# ═══════════════════════════════════════════════════════════════
# strategy_ecosystem/services.py 深度测试
# ═══════════════════════════════════════════════════════════════

class TestOpsServiceRegressionCheck:
    def _make_ecosystem(self):
        eco = MagicMock()
        eco._lock = threading.RLock()
        eco._master = StrategySlot(strategy_id='master', strategy_type='hft', state=SlotState.ACTIVE,
                                   capital_allocation=0.5)
        eco._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal', state=SlotState.ACTIVE,
                                    capital_allocation=0.2)
        eco._other = StrategySlot(strategy_id='other', strategy_type='other', state=SlotState.STANDBY,
                                  capital_allocation=0.1)
        eco._spring = StrategySlot(strategy_id='spring', strategy_type='spring', state=SlotState.STANDBY,
                                   capital_allocation=0.1)
        eco._arbitrage = StrategySlot(strategy_id='arbitrage', strategy_type='arbitrage', state=SlotState.STANDBY,
                                      capital_allocation=0.05)
        eco._market_making = StrategySlot(strategy_id='market_making', strategy_type='mm', state=SlotState.STANDBY,
                                          capital_allocation=0.05)
        return eco

    def test_regression_check_passes_with_valid_state(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService
        eco = self._make_ecosystem()
        ops = OpsService()
        ops.__dict__.update(eco.__dict__)
        result = ops.regression_check()
        assert isinstance(result, dict)
        assert 'passed' in result
        assert 'violations' in result
        assert 'checked_invariants' in result

    def test_regression_check_fails_with_zero_allocation(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService
        eco = self._make_ecosystem()
        for slot in [eco._master, eco._reverse, eco._other, eco._spring, eco._arbitrage, eco._market_making]:
            slot.capital_allocation = 0.0
        ops = OpsService()
        ops.__dict__.update(eco.__dict__)
        result = ops.regression_check()
        assert result['passed'] is False

    def test_emergency_degrade_reduces_active(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService
        eco = self._make_ecosystem()
        ops = OpsService()
        ops.__dict__.update(eco.__dict__)
        ops._get_slot = lambda sid: {
            'master': eco._master, 'reverse': eco._reverse, 'other': eco._other,
            'spring': eco._spring, 'arbitrage': eco._arbitrage, 'market_making': eco._market_making,
        }.get(sid)
        ops.switch_active_strategy = MagicMock()
        result = ops.emergency_degrade(target_active_count=1, caller_id='test')
        assert isinstance(result, dict)
        assert 'degraded_strategies' in result


class TestGovernanceService:
    def _make_ecosystem(self):
        eco = MagicMock()
        eco._lock = threading.RLock()
        eco._grayscale_config = None
        eco._governance_degraded = False
        eco._governance_degradation_reasons = []
        eco._wf_elimination_checker = None
        eco._e7_checker = None
        eco._e8e9e10_checker = None
        eco._e11_checker = None
        eco._master = StrategySlot(strategy_id='master', strategy_type='hft', state=SlotState.ACTIVE)
        eco._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal', state=SlotState.ACTIVE)
        eco._other = StrategySlot(strategy_id='other', strategy_type='other', state=SlotState.STANDBY)
        eco._spring = StrategySlot(strategy_id='spring', strategy_type='spring', state=SlotState.STANDBY)
        eco.freeze_strategy_slot = MagicMock()
        return eco

    def test_configure_grayscale(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService
        eco = self._make_ecosystem()
        gov = GovernanceService()
        gov.__dict__.update(eco.__dict__)
        gov._get_slot = lambda sid: {
            'master': eco._master, 'reverse': eco._reverse, 'other': eco._other,
            'spring': eco._spring, 'arbitrage': eco._arbitrage, 'market_making': eco._market_making,
        }.get(sid)
        result = gov.configure_grayscale({'strategy_id': 'master', 'traffic_pct': 20, 'duration_sec': 1800})
        assert result['success'] is True

    def test_get_grayscale_status_inactive(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService
        eco = self._make_ecosystem()
        gov = GovernanceService()
        gov.__dict__.update(eco.__dict__)
        result = gov.get_grayscale_status()
        assert result['active'] is False

    def test_is_governance_degraded_default(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService
        eco = self._make_ecosystem()
        gov = GovernanceService()
        gov.__dict__.update(eco.__dict__)
        assert gov.is_governance_degraded() is False

    def test_run_governance_checks_no_checkers(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService
        eco = self._make_ecosystem()
        gov = GovernanceService()
        gov.__dict__.update(eco.__dict__)
        result = gov.run_governance_checks()
        assert result['degraded'] is False

    def test_resume_strategy_not_paused(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService
        eco = self._make_ecosystem()
        gov = GovernanceService()
        gov.__dict__.update(eco.__dict__)
        gov._get_slot = lambda sid: eco._master if sid == 'master' else None
        result = gov.resume_strategy('master')
        assert result is False


class TestTradingEVService:
    def test_get_slot_mapping(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService
        svc = TradingEVService()
        master = StrategySlot(strategy_id='master', strategy_type='hft')
        svc._master = master
        svc._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal')
        svc._other = StrategySlot(strategy_id='other', strategy_type='other')
        svc._spring = StrategySlot(strategy_id='spring', strategy_type='spring')
        svc._arbitrage = StrategySlot(strategy_id='arbitrage', strategy_type='arbitrage')
        svc._market_making = StrategySlot(strategy_id='market_making', strategy_type='mm')
        assert svc._get_slot('master') is master
        assert svc._get_slot('nonexistent') is None

    def test_is_same_delta_direction_same(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService
        svc = TradingEVService()
        svc._master = StrategySlot(strategy_id='master', strategy_type='hft')
        svc._master.last_direction = 'BUY'
        svc._master.last_open_reason = ''
        svc._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal')
        assert svc._is_same_delta_direction('reverse', 'BUY', 'master') is True

    def test_is_same_delta_direction_hedge(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService
        svc = TradingEVService()
        svc._master = StrategySlot(strategy_id='master', strategy_type='hft')
        svc._master.last_direction = 'SELL'
        svc._master.last_open_reason = 'DELTA_HEDGE position'
        svc._reverse = StrategySlot(strategy_id='reverse', strategy_type='reversal')
        assert svc._is_same_delta_direction('reverse', 'BUY', 'master') is False


# ═══════════════════════════════════════════════════════════════
# tick_hft.py 深度测试
# ═══════════════════════════════════════════════════════════════

class TestDynamicPursuitEngine:
    def test_evaluate_surge_new_position(self):
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('AL2401', current_strength=0.8, prev_strength=0.3,
                                       current_price=20000.0, direction='BUY')
        assert result is not None
        assert result['action'] == 'OPEN_POSITION'
        assert result['instrument_id'] == 'AL2401'
        assert result['direction'] == 'BUY'

    def test_evaluate_surge_below_threshold(self):
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('AL2401', current_strength=0.5, prev_strength=0.4,
                                       current_price=20000.0, direction='BUY')
        assert result is None

    def test_evaluate_surge_add_position(self):
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_add_positions=3, max_total_position_pct=1.0)
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY', account_equity=1000000.0)
        engine.confirm_position_on_platform('AL2401', 'order_1')
        result = engine.evaluate_surge('AL2401', 1.2, 0.8, 20100.0, 'BUY', account_equity=1000000.0)
        assert result is not None
        assert result['action'] == 'ADD_POSITION'

    def test_evaluate_surge_invalid_direction(self):
        engine = DynamicPursuitEngine()
        result = engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'INVALID')
        assert result is None

    def test_evaluate_surge_zero_price(self):
        engine = DynamicPursuitEngine()
        result = engine.evaluate_surge('AL2401', 0.8, 0.3, 0.0, 'BUY')
        assert result is None

    def test_evaluate_surge_opposite_direction_rejected(self):
        engine = DynamicPursuitEngine(surge_threshold=0.3, max_total_position_pct=1.0)
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY', account_equity=1000000.0)
        engine.confirm_position_on_platform('AL2401', 'order_1')
        result = engine.evaluate_surge('AL2401', 1.2, 0.8, 20100.0, 'SELL', account_equity=1000000.0)
        assert result is None

    def test_update_trailing_stop_buy_improves(self):
        engine = DynamicPursuitEngine()
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY')
        engine.confirm_position_on_platform('AL2401', 'order_1')
        new_stop = engine.update_trailing_stop('AL2401', 20500.0, 'BUY')
        assert new_stop is not None
        assert new_stop > 20000.0

    def test_update_trailing_stop_no_position(self):
        engine = DynamicPursuitEngine()
        result = engine.update_trailing_stop('AL2401', 20500.0, 'BUY')
        assert result is None

    def test_check_exit_take_profit_buy(self):
        engine = DynamicPursuitEngine(tight_stop_loss_pct=0.15)
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY')
        engine.confirm_position_on_platform('AL2401', 'order_1')
        result = engine.check_exit('AL2401', 25000.0)
        assert result is not None
        assert result['action'] == 'CLOSE_ALL'
        assert result['reason'] == 'pursuit_take_profit'

    def test_check_exit_stop_loss_buy(self):
        engine = DynamicPursuitEngine(tight_stop_loss_pct=0.15)
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY')
        engine.confirm_position_on_platform('AL2401', 'order_1')
        result = engine.check_exit('AL2401', 16000.0)
        assert result is not None
        assert result['reason'] == 'pursuit_stop_loss'

    def test_check_exit_no_position(self):
        engine = DynamicPursuitEngine()
        result = engine.check_exit('AL2401', 20000.0)
        assert result is None

    def test_confirm_position_on_platform(self):
        engine = DynamicPursuitEngine()
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY')
        assert engine.confirm_position_on_platform('AL2401', 'order_123') is True
        pos = engine._positions['AL2401']
        assert pos.platform_confirmed is True
        assert 'order_123' in pos.platform_order_ids

    def test_add_platform_order_id(self):
        engine = DynamicPursuitEngine()
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY')
        assert engine.add_platform_order_id('AL2401', 'order_456') is True

    def test_get_stats(self):
        engine = DynamicPursuitEngine()
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY')
        stats = engine.get_stats()
        assert stats['service_name'] == 'DynamicPursuitEngine'
        assert stats['total_pursuit_entries'] == 1
        assert stats['surge_detected'] == 1

    def test_max_add_positions_limit(self):
        engine = DynamicPursuitEngine(surge_threshold=0.1, max_add_positions=1, max_total_position_pct=1.0)
        engine.evaluate_surge('AL2401', 0.8, 0.3, 20000.0, 'BUY', account_equity=1000000.0)
        engine.confirm_position_on_platform('AL2401', 'o1')
        engine.evaluate_surge('AL2401', 1.2, 0.8, 20100.0, 'BUY', account_equity=1000000.0)
        result = engine.evaluate_surge('AL2401', 1.5, 1.2, 20200.0, 'BUY', account_equity=1000000.0)
        assert result is None

    def test_sell_direction_surge(self):
        engine = DynamicPursuitEngine(surge_threshold=0.3)
        result = engine.evaluate_surge('IF2401', 0.8, 0.3, 4000.0, 'SELL')
        assert result is not None
        assert result['action'] == 'OPEN_POSITION'
        assert result['direction'] == 'SELL'

    def test_sell_direction_exit_take_profit(self):
        engine = DynamicPursuitEngine(tight_stop_loss_pct=0.15)
        engine.evaluate_surge('IF2401', 0.8, 0.3, 4000.0, 'SELL')
        engine.confirm_position_on_platform('IF2401', 'o1')
        result = engine.check_exit('IF2401', 3500.0)
        assert result is not None
        assert result['reason'] == 'pursuit_take_profit'


class TestPursuitPosition:
    def test_creation(self):
        pos = PursuitPosition(
            position_id='P1', instrument_id='AL2401', direction='BUY',
            entries=[{'price': 20000.0, 'volume': 1}],
            total_volume=1, weighted_avg_price=20000.0,
            current_stop_profit=20100.0, current_stop_loss=19700.0,
            peak_strength=0.8,
        )
        assert pos.is_open is True
        assert pos.platform_confirmed is False
        assert pos.platform_order_ids == []


class TestPyramidAddPositionEngine:
    def test_calc_add_volume_level0(self):
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('AL2401', base_volume=10, current_level=0)
        assert vol == 10

    def test_calc_add_volume_level1(self):
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('AL2401', base_volume=10, current_level=1)
        assert vol == 5

    def test_calc_add_volume_level2(self):
        engine = PyramidAddPositionEngine(max_levels=4, pyramid_ratio=0.5)
        vol = engine.calc_add_volume('AL2401', base_volume=10, current_level=2)
        assert vol == 2

    def test_calc_add_volume_exceeds_max_levels(self):
        engine = PyramidAddPositionEngine(max_levels=4)
        vol = engine.calc_add_volume('AL2401', base_volume=10, current_level=4)
        assert vol == 0

    def test_calc_add_volume_plr_blocked(self):
        engine = PyramidAddPositionEngine(min_plr_for_add=1.5)
        vol = engine.calc_add_volume('AL2401', base_volume=10, current_level=0, current_plr=1.0)
        assert vol == 0

    def test_calc_add_volume_atr_adaptive(self):
        engine = PyramidAddPositionEngine(atr_adaptive=True, atr_reference=0.02)
        vol = engine.calc_add_volume('AL2401', base_volume=10, current_level=0, current_atr=0.01)
        assert vol >= 1

    def test_get_stats(self):
        engine = PyramidAddPositionEngine()
        engine.calc_add_volume('AL2401', 10, 0)
        stats = engine.get_stats()
        assert stats['service_name'] == 'PyramidAddPositionEngine'
        assert stats['total_adds'] == 1


class TestCheckHardTimeStopForPosition:
    def test_no_safety_meta_layer(self):
        risk_svc = MagicMock()
        risk_svc._safety_meta_layer = None
        result = check_hard_time_stop_for_position(risk_svc, 'p1', time.time(), 0.0)
        assert result is None

    def test_with_safety_meta_layer(self):
        risk_svc = MagicMock()
        risk_svc._safety_meta_layer = MagicMock()
        risk_svc._safety_meta_layer.check_position_hard_time_stop.return_value = 'hard_time_stop'
        result = check_hard_time_stop_for_position(risk_svc, 'p1', time.time(), 0.0)
        assert result == 'hard_time_stop'