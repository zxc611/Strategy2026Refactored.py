# MODULE_ID: M2-380
import sys
import os
import pytest
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestSafetyMetaLayer:
    def test_init_default(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer()
        assert layer is not None
        assert not layer.is_hard_stop_triggered()

    def test_daily_drawdown_not_triggered(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        assert not layer.check_daily_drawdown(96000.0)

    def test_daily_drawdown_triggered(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        assert layer.check_daily_drawdown(94000.0)
        assert layer.is_hard_stop_triggered()

    def test_confirm_daily_resume_human(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        layer.check_daily_drawdown(94000.0)
        assert layer.confirm_daily_resume("trader")
        assert not layer.is_hard_stop_triggered()

    def test_confirm_daily_resume_auto_rejected(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        layer.check_daily_drawdown(94000.0)
        assert not layer.confirm_daily_resume("timer")
        assert layer.is_hard_stop_triggered()

    def test_confirm_daily_resume_scheduler_rejected(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        layer.check_daily_drawdown(94000.0)
        assert not layer.confirm_daily_resume("scheduler")
        assert layer.is_hard_stop_triggered()

    def test_circuit_breaker_activate_and_expire(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer()
        layer.activate_circuit_breaker(pause_sec=0.01)
        assert layer.is_circuit_breaker_active()
        time.sleep(0.02)
        assert not layer.is_circuit_breaker_active()

    def test_circuit_breaker_not_active_by_default(self):
        from ali2026v3_trading.risk.safety_meta_layer import SafetyMetaLayer
        layer = SafetyMetaLayer()
        assert not layer.is_circuit_breaker_active()