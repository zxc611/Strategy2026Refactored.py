# MODULE_ID: M2-422
import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.governance.mode_exit_rules import (
    ExitRuleEngine, DrawdownManager, DefensiveDrawdownChecker,
)
from ali2026v3_trading.governance.mode_config import TakeProfitMethod, StopLossMethod, DrawdownAction


class TestExitRuleEngine:
    def test_init(self):
        engine = ExitRuleEngine(TakeProfitMethod.TIERED, StopLossMethod.FIXED)
        assert engine.take_profit_method == TakeProfitMethod.TIERED
        assert engine.stop_loss_method == StopLossMethod.FIXED

    def test_compute_take_profit_tiered_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.TIERED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(100.0, 5.0, 1.0, direction='BUY')
        assert len(levels) == 3
        assert levels[0]['price'] == 105.0
        assert levels[1]['price'] == 110.0
        assert levels[2]['price'] is None

    def test_compute_take_profit_tiered_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.TIERED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(100.0, 5.0, 1.0, direction='SELL')
        assert len(levels) == 3
        assert levels[0]['price'] == 95.0
        assert levels[1]['price'] == 90.0

    def test_compute_take_profit_trailing_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.TRAILING, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(100.0, 5.0, 1.0, direction='BUY')
        assert len(levels) == 1
        assert levels[0]['activation'] == 105.0

    def test_compute_take_profit_trailing_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.TRAILING, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(100.0, 5.0, 1.0, direction='SELL')
        assert len(levels) == 1
        assert levels[0]['activation'] == 95.0

    def test_compute_take_profit_fixed_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(100.0, 5.0, 1.0, direction='BUY')
        assert len(levels) == 1
        assert abs(levels[0]['price'] - 110.0) < 1e-9

    def test_compute_take_profit_fixed_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.FIXED)
        levels = engine.compute_take_profit_levels(100.0, 5.0, 1.0, direction='SELL')
        assert len(levels) == 1
        assert levels[0]['price'] == 90.0

    def test_compute_stop_loss_fixed_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.FIXED)
        result = engine.compute_stop_loss(100.0, 3.0, direction='BUY')
        assert result['method'] == 'fixed'
        assert result['price'] == 97.0

    def test_compute_stop_loss_fixed_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.FIXED)
        result = engine.compute_stop_loss(100.0, 3.0, direction='SELL')
        assert result['method'] == 'fixed'
        assert result['price'] == 103.0

    def test_compute_stop_loss_volatility_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.VOLATILITY)
        result = engine.compute_stop_loss(100.0, 2.0, volatility_20d=0.03, direction='BUY')
        assert result['method'] == 'volatility'
        assert result['price'] < 100.0

    def test_compute_stop_loss_volatility_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.VOLATILITY)
        result = engine.compute_stop_loss(100.0, 2.0, volatility_20d=0.03, direction='SELL')
        assert result['method'] == 'volatility'
        assert result['price'] > 100.0

    def test_compute_stop_loss_time_decay_buy(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.TIME_DECAY)
        result = engine.compute_stop_loss(100.0, 2.5, direction='BUY')
        assert result['method'] == 'time_decay'
        assert result['price'] == 97.5

    def test_compute_stop_loss_time_decay_sell(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.TIME_DECAY)
        result = engine.compute_stop_loss(100.0, 2.5, direction='SELL')
        assert result['method'] == 'time_decay'
        assert result['price'] == 102.5

    def test_compute_stop_loss_volatility_zero(self):
        engine = ExitRuleEngine(TakeProfitMethod.FIXED, StopLossMethod.VOLATILITY)
        result = engine.compute_stop_loss(100.0, 2.0, volatility_20d=0.0, direction='BUY')
        assert result['method'] == 'fixed'


class TestDrawdownManager:
    def test_init_defaults(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        assert dm.action == DrawdownAction.REDUCE_SIZE
        assert dm.recovery_target == 1.0
        assert dm.var_upgrade_active is False
        assert dm.atr_upgrade_active is False

    def test_set_daily_hard_stop(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        dm.set_daily_hard_stop(True)
        assert dm.should_reduce_size() is True
        assert dm.should_halt_new() is True
        assert dm.should_full_stop() is True

    def test_update_drawdown(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(-0.03)
        assert dm.should_reduce_size() is True

    def test_should_reduce_size_no_drawdown(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        assert dm.should_reduce_size() is False

    def test_should_reduce_size_with_drawdown(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(-0.01)
        assert dm.should_reduce_size() is True

    def test_should_halt_new_halt_action(self):
        dm = DrawdownManager(DrawdownAction.HALT_NEW)
        dm.update_drawdown(-0.01)
        assert dm.should_halt_new() is True

    def test_should_halt_new_reduce_action(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(-0.01)
        assert dm.should_halt_new() is False

    def test_should_full_stop_action(self):
        dm = DrawdownManager(DrawdownAction.FULL_STOP)
        dm.update_drawdown(-0.06)
        assert dm.should_full_stop() is True

    def test_should_full_stop_insufficient(self):
        dm = DrawdownManager(DrawdownAction.FULL_STOP)
        dm.update_drawdown(-0.04)
        assert dm.should_full_stop() is False

    def test_var_upgrade_activates(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        returns = [-0.05] * 25
        var = dm.update_var_baseline(returns, confidence=0.95)
        assert var > 0
        assert dm.var_upgrade_active is True
        assert dm.last_var_value > 0

    def test_var_upgrade_not_activated(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE, var_upgrade_threshold=0.5)
        returns = [-0.001] * 25
        var = dm.update_var_baseline(returns, confidence=0.95)
        assert dm.var_upgrade_active is False

    def test_var_upgrade_short_returns(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        var = dm.update_var_baseline([-0.01] * 5, confidence=0.95)
        assert var == 0.0

    def test_atr_upgrade_activates(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        dm.update_atr_baseline(0.03)
        assert dm.atr_upgrade_active is True
        assert dm.last_atr_value == 0.03

    def test_atr_upgrade_not_activated(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE, atr_upgrade_threshold=0.05)
        dm.update_atr_baseline(0.01)
        assert dm.atr_upgrade_active is False

    def test_var_upgrade_early_reduce(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        returns = [-0.05] * 25
        dm.update_var_baseline(returns, confidence=0.95)
        dm.update_drawdown(-0.001)
        assert dm.should_reduce_size() is True

    def test_var_upgrade_early_halt(self):
        dm = DrawdownManager(DrawdownAction.HALT_NEW)
        returns = [-0.05] * 25
        dm.update_var_baseline(returns, confidence=0.95)
        dm.update_drawdown(-0.001)
        assert dm.should_halt_new() is True

    def test_var_upgrade_tightened_full_stop(self):
        dm = DrawdownManager(DrawdownAction.FULL_STOP)
        returns = [-0.05] * 25
        dm.update_var_baseline(returns, confidence=0.95)
        dm.update_drawdown(-0.025)
        assert dm.should_full_stop() is True

    def test_var_upgrade_full_stop_not_triggered(self):
        dm = DrawdownManager(DrawdownAction.FULL_STOP)
        returns = [-0.05] * 25
        dm.update_var_baseline(returns, confidence=0.95)
        dm.update_drawdown(-0.01)
        assert dm.should_full_stop() is False

    def test_is_recovered_no_drawdown(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        assert dm.is_recovered(100.0, 100.0) is True

    def test_is_recovered_success(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE, recovery_target=1.0)
        dm.update_drawdown(-0.05)
        assert dm.is_recovered(105.0, 100.0) is True

    def test_is_recovered_failure(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE, recovery_target=1.0)
        dm.update_drawdown(-0.05)
        assert dm.is_recovered(99.98, 100.0) is False

    def test_drawdown_low_tracking(self):
        dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
        dm.update_drawdown(-0.03)
        dm.update_drawdown(-0.05)
        dm.update_drawdown(-0.02)
        assert dm.is_recovered(100.0, 100.0) is False


class TestDefensiveDrawdownChecker:
    def test_no_decay(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(1.5, 2.0, 1.5, 2.0)
        assert result == 1.0

    def test_sortino_decay(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(0.3, 2.0, 1.5, 2.0)
        assert result < 1.0

    def test_calmar_decay(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(1.5, 0.3, 1.5, 2.0)
        assert result < 1.0

    def test_zero_entry_values(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(1.5, 2.0, 0.0, 2.0)
        assert result == 1.0

    def test_negative_entry_values(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(1.5, 2.0, -1.0, 2.0)
        assert result == 1.0

    def test_custom_threshold(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(0.6, 2.0, 1.5, 2.0, decay_threshold=0.3)
        assert result == 1.0

    def test_below_threshold(self):
        checker = DefensiveDrawdownChecker()
        result = checker.check(0.2, 2.0, 1.5, 2.0, decay_threshold=0.5)
        assert result < 0.5