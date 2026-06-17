# MODULE_ID: M2-563
"""risk/signal/governance/param_pool 0%模块覆盖率测试"""
import pytest, sys, os
import numpy as np
from unittest.mock import MagicMock
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestRiskPriorityMatrix:
    def test_init(self):
        from ali2026v3_trading.risk.risk_priority_matrix import RiskPriorityMatrix
        rpm = RiskPriorityMatrix()
        assert rpm is not None

    def test_add_risk(self):
        from ali2026v3_trading.risk.risk_priority_matrix import RiskPriorityMatrix, RiskItem
        rpm = RiskPriorityMatrix()
        item = RiskItem(name='test_risk', probability=3, impact=5, detectability=2)
        rpm.add_risk(item)
        risks = rpm.get_sorted_risks()
        assert len(risks) > 0

    def test_get_critical_risks(self):
        from ali2026v3_trading.risk.risk_priority_matrix import RiskPriorityMatrix, RiskItem
        rpm = RiskPriorityMatrix()
        item = RiskItem(name='critical_risk', probability=5, impact=5, detectability=1)
        rpm.add_risk(item)
        critical = rpm.get_critical_risks()
        assert isinstance(critical, list)

    def test_get_risk_by_name(self):
        from ali2026v3_trading.risk.risk_priority_matrix import RiskPriorityMatrix, RiskItem
        rpm = RiskPriorityMatrix()
        item = RiskItem(name='findable_risk', probability=2, impact=3, detectability=4)
        rpm.add_risk(item)
        risk = rpm.get_risk_by_name('findable_risk')
        assert risk is not None

    def test_to_report(self):
        from ali2026v3_trading.risk.risk_priority_matrix import RiskPriorityMatrix
        rpm = RiskPriorityMatrix()
        report = rpm.to_report()
        assert report is not None

    def test_risk_item_rpn(self):
        from ali2026v3_trading.risk.risk_priority_matrix import RiskItem
        item = RiskItem(name='test', probability=3, impact=5, detectability=2)
        assert item.rpn == 30


class TestSafetyMetaLayer:
    def test_init(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        assert sml is not None

    def test_set_daily_start_equity(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        sml.set_daily_start_equity(100000.0)

    def test_check_daily_drawdown(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        sml.set_daily_start_equity(100000.0)
        result = sml.check_daily_drawdown(current_equity=95000.0)
        assert isinstance(result, bool)

    def test_is_hard_stop_triggered(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        result = sml.is_hard_stop_triggered()
        assert isinstance(result, bool)

    def test_confirm_daily_resume(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        result = sml.confirm_daily_resume(caller_id='test')
        assert isinstance(result, bool)

    def test_activate_circuit_breaker(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        sml.activate_circuit_breaker(pause_sec=10.0)

    def test_is_circuit_breaker_active(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        sml = SafetyMetaLayer()
        result = sml.is_circuit_breaker_active()
        assert isinstance(result, bool)


class TestSignalGenerator:
    def test_init(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator, SignalContext
        svc = type('MockSvc', (), {})()
        sg = SignalGenerator(signal_service=svc)
        assert sg is not None

    def test_generate_signal(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator, SignalContext
        svc = MagicMock()
        svc._stats = {'filtered_signals': 0}
        svc._plr_filter_enabled = False
        svc._min_estimated_plr = 0
        svc._default_cooldown_seconds = 60
        svc._is_in_cooldown.return_value = False
        svc._decision_score_filter_enabled = False
        svc._filter_by_hft_enabled = False
        svc._adaptive_filter_enabled = False
        sg = SignalGenerator(signal_service=svc)
        ctx = SignalContext(instrument_id='AU2506', signal_type='BUY', price=500.0, volume=1, signal_strength=1.0)
        result = sg.generate_signal(ctx)
        assert result is not None


class TestGovernanceValidationRegistry:
    def test_init(self):
        from ali2026v3_trading.governance.validation_registry import ValidationRegistry
        vr = ValidationRegistry()
        assert vr is not None

    def test_register_and_run(self):
        from ali2026v3_trading.governance.validation_registry import (
            ValidationRegistry, SharpeRatioValidator, MaxDrawdownValidator,
            WinRateValidator, ProfitLossRatioValidator, MinTradesValidator,
            BacktestResult,
        )
        vr = ValidationRegistry()
        vr.register('sharpe', SharpeRatioValidator())
        vr.register('drawdown', MaxDrawdownValidator())
        vr.register('win_rate', WinRateValidator())
        vr.register('plr', ProfitLossRatioValidator())
        vr.register('min_trades', MinTradesValidator())
        result = BacktestResult(
            strategy_name='test', total_return=0.2, sharpe_ratio=2.0,
            max_drawdown=0.1, win_rate=0.55, profit_loss_ratio=2.0, total_trades=100,
        )
        report = vr.run_all(result)
        assert report is not None

    def test_unregister(self):
        from ali2026v3_trading.governance.validation_registry import ValidationRegistry, SharpeRatioValidator
        vr = ValidationRegistry()
        vr.register('sharpe', SharpeRatioValidator())
        vr.unregister('sharpe')

    def test_run_one(self):
        from ali2026v3_trading.governance.validation_registry import ValidationRegistry, SharpeRatioValidator, BacktestResult
        vr = ValidationRegistry()
        vr.register('sharpe', SharpeRatioValidator())
        result = BacktestResult(sharpe_ratio=2.0)
        vr_result = vr.run_one('sharpe', result)
        assert vr_result is not None

    def test_create_default(self):
        from ali2026v3_trading.governance.validation_registry import create_default_validation_registry
        vr = create_default_validation_registry()
        assert vr is not None


class TestParamPoolShims:
    def test_leaf_param_defaults_importable(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PULLBACK_DEFAULTS
        assert isinstance(PULLBACK_DEFAULTS, dict)

    def test_leaf_core_importable(self):
        from ali2026v3_trading.param_pool._param_grids import DEEP_VALIDATION_TIERS
        assert DEEP_VALIDATION_TIERS is not None

    def test_leaf_runner_helpers_importable(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_backtest_health
        assert _check_backtest_health is not None

    def test_backtest_param_grids_importable(self):
        from ali2026v3_trading.param_pool._param_grids import BACKTEST_THRESHOLDS
        assert BACKTEST_THRESHOLDS is not None

    def test_task_scheduler_importable(self):
        from ali2026v3_trading.param_pool.task_scheduler import DEEP_VALIDATION_TIERS
        assert DEEP_VALIDATION_TIERS is not None

    def test_backtest_runner_importable(self):
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import run_backtest
        assert run_backtest is not None


class TestBoxSpringExecutorHelpers:
    def test_compute_hedge_ratio(self):
        from ali2026v3_trading.strategy.box_spring_executor_helpers import _compute_hedge_ratio
        signal = {'direction': 'BUY', 'volume': 10, 'price': 500.0}
        result = _compute_hedge_ratio(signal)
        assert isinstance(result, float)

    def test_check_cross_strategy_risk(self):
        from ali2026v3_trading.strategy.box_spring_executor_helpers import _check_cross_strategy_risk
        signal1 = {'direction': 'BUY', 'volume': 10}
        signal2 = {'direction': 'SELL', 'volume': 5}
        result = _check_cross_strategy_risk(signal1, signal2)
        assert result is not None
