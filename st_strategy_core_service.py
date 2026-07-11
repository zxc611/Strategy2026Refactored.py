# MODULE_ID: M2-590
"""
test_strategy_core_service.py - StrategyCoreService核心服务测试

验证内容：
1. StrategyCoreService实例化
2. 核心方法存在性
3. 健康状态接口
4. 策略状态管理
"""
import unittest
import time
from typing import Dict, Any


class TestStrategyCoreServiceInstantiation(unittest.TestCase):
    def test_default_instantiation(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService()
        self.assertIsNotNone(svc)
        self.assertIsNotNone(svc.strategy_id)

    def test_custom_strategy_id(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService(strategy_id="test_strategy_001")
        self.assertEqual(svc.strategy_id, "test_strategy_001")

    def test_initial_state_is_initializing(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        from ali2026v3_trading.strategy.strategy_core_service import StrategyState
        svc = StrategyCoreService(strategy_id="test_state_init")
        self.assertEqual(svc._state, StrategyState.INITIALIZING)

    def test_not_running_initially(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService(strategy_id="test_not_running")
        self.assertFalse(svc._is_running)

    def test_not_destroyed_initially(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService(strategy_id="test_not_destroyed")
        self.assertFalse(svc._destroyed)


class TestStrategyCoreServiceHasRequiredMethods(unittest.TestCase):
    def setUp(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        self.svc = StrategyCoreService(strategy_id="test_methods")

    def test_has_get_health_status(self):
        self.assertTrue(hasattr(self.svc, 'get_health_status'))
        self.assertTrue(callable(self.svc.get_health_status))

    def test_has_execute_option_trading_cycle(self):
        self.assertTrue(hasattr(self.svc, 'execute_option_trading_cycle'))
        self.assertTrue(callable(self.svc.execute_option_trading_cycle))

    def test_has_check_position_risk(self):
        self.assertTrue(hasattr(self.svc, 'check_position_risk'))
        self.assertTrue(callable(self.svc.check_position_risk))

    def test_has_on_order(self):
        self.assertTrue(hasattr(self.svc, 'on_order'))
        self.assertTrue(callable(self.svc.on_order))

    def test_has_on_trade(self):
        self.assertTrue(hasattr(self.svc, 'on_trade'))
        self.assertTrue(callable(self.svc.on_trade))

    def test_has_recover_from_checkpoint(self):
        self.assertTrue(hasattr(self.svc, 'recover_from_checkpoint'))
        self.assertTrue(callable(self.svc.recover_from_checkpoint))

    def test_has_save_checkpoint(self):
        self.assertTrue(hasattr(self.svc, 'save_checkpoint'))
        self.assertTrue(callable(self.svc.save_checkpoint))


class TestStrategyCoreServiceHealthStatus(unittest.TestCase):
    def test_health_status_returns_dict(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService(strategy_id="test_health")
        health = svc.get_health_status()
        self.assertIsInstance(health, dict)

    def test_health_status_has_component_key(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService(strategy_id="test_health_component")
        health = svc.get_health_status()
        # health_status返回的dict包含各组件检查结果（key以。check_开头）'
        self.assertTrue(any(k.startswith('_check_') for k in health), "health dict should contain component check keys")

    def test_health_status_has_status_key(self):
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        svc = StrategyCoreService(strategy_id="test_health_status_key")
        health = svc.get_health_status()
        self.assertIn('overall_status', health)


if __name__ == '__main__':
    unittest.main()
