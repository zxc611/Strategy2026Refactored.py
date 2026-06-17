# MODULE_ID: M2-305
"""strategy_judgment/backtest_integration_hooks.py 覆盖率测试"""
import pytest, sys, os
import numpy as np
from unittest.mock import patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.strategy_judgment.backtest_integration_hooks import (
    BacktestIntegrationHooks, HookConfig,
)


class TestHookConfig:
    def test_defaults(self):
        cfg = HookConfig()
        assert cfg is not None


class TestBacktestIntegrationHooks:
    def test_init(self):
        hooks = BacktestIntegrationHooks()
        assert hooks is not None

    def test_on_tick(self):
        hooks = BacktestIntegrationHooks()
        ts = np.datetime64('2025-01-02T10:00:00')
        hooks.on_tick(timestamp=ts, last_price=500.0, volume=100)

    def test_on_bar(self):
        hooks = BacktestIntegrationHooks()
        bar = type('EnhancedBar', (), {'symbol': 'AU2506', 'close': 500.0, 'volume': 100, 'extreme_region': None, 'timestamp': np.datetime64('2025-01-02T10:00:00'), 'high': 505.0, 'low': 495.0})()
        hooks.on_bar(bar=bar)

    def test_on_signal_generated(self):
        hooks = BacktestIntegrationHooks()
        with patch.object(hooks._snapshot_collector, 'capture_signal_point', return_value=None):
            hooks.on_signal_generated(timestamp=None, signal_info={'direction': 'BUY'})

    def test_on_order_opened(self):
        hooks = BacktestIntegrationHooks()
        with patch.object(hooks._snapshot_collector, 'capture_order_event', return_value=None):
            hooks.on_order_opened(timestamp=None, order_info={'order_id': 'O001'})

    def test_on_order_closed(self):
        hooks = BacktestIntegrationHooks()
        with patch.object(hooks._snapshot_collector, 'capture_order_event', return_value=None):
            hooks.on_order_closed(timestamp=None, order_info={'order_id': 'O001', 'pnl': 100.0})

    def test_on_safety_meta_trigger(self):
        hooks = BacktestIntegrationHooks()
        hooks.on_safety_meta_trigger(timestamp=None, detail='daily_drawdown')

    def test_on_ecosystem_switch(self):
        hooks = BacktestIntegrationHooks()
        hooks.on_ecosystem_switch(timestamp=None, detail='debug_to_trade')

    def test_on_spring_state_change(self):
        hooks = BacktestIntegrationHooks()
        hooks.on_spring_state_change(timestamp=None, detail='idle_to_active')

    def test_on_phase_transition(self):
        hooks = BacktestIntegrationHooks()
        hooks.on_phase_transition(timestamp=None, detail='L1_to_L2')

    def test_update_state(self):
        hooks = BacktestIntegrationHooks()
        hooks.update_state(strategy_states={'box': 'active'})

    def test_build_18_strategy_states(self):
        hooks = BacktestIntegrationHooks()
        states = hooks.build_18_strategy_states()
        assert isinstance(states, list)

    def test_on_backtest_finish(self):
        hooks = BacktestIntegrationHooks()
        with patch('ali2026v3_trading.strategy_judgment.strategy_judgment_facade.run_ecosystem_integrations', return_value=0.5):
            result = hooks.on_backtest_finish(symbol='AU2506', backtest_period='2025-01')
        assert isinstance(result, dict)
