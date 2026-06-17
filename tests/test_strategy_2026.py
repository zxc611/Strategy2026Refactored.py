# MODULE_ID: M2-586
"""
test_strategy_2026.py - Strategy2026核心逻辑测试

验证内容：
1. Strategy2026实例化
2. 继承关系正确（BaseStrategy, UIMixin）
3. 核心属性和方法存在性
4. strategy_core关联
5. 日志方法
"""
import unittest
import logging
from typing import Any


class TestStrategy2026Instantiation(unittest.TestCase):
    def test_basic_instantiation(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026()
        self.assertIsNotNone(s)

    def test_has_strategy_id(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_2026_001")
        self.assertEqual(s.strategy_id, "test_2026_001")

    def test_has_strategy_core(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_core_ref")
        self.assertIsNotNone(s.strategy_core)

    def test_strategy_core_is_correct_type(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026, StrategyCoreService
        s = Strategy2026(strategy_id="test_core_type")
        self.assertIsInstance(s.strategy_core, StrategyCoreService)

    def test_auto_trading_disabled_initially(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_auto_trading")
        self.assertFalse(s.auto_trading_enabled)


class TestStrategy2026Inheritance(unittest.TestCase):
    def test_inherits_base_strategy(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        from ali2026v3_trading.strategy.strategy_2026 import BaseStrategy
        self.assertTrue(issubclass(Strategy2026, BaseStrategy))

    def test_inherits_ui_mixin(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        from ali2026v3_trading.config.ui_service import UIMixin
        self.assertTrue(issubclass(Strategy2026, UIMixin))


class TestStrategy2026RequiredMethods(unittest.TestCase):
    def setUp(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        self.s = Strategy2026(strategy_id="test_methods_2026")

    def test_has_log_warning(self):
        self.assertTrue(hasattr(self.s, '_log_warning'))
        self.assertTrue(callable(self.s._log_warning))

    def test_has_log_info(self):
        self.assertTrue(hasattr(self.s, '_log_info'))
        self.assertTrue(callable(self.s._log_info))

    def test_has_log_error(self):
        self.assertTrue(hasattr(self.s, '_log_error'))
        self.assertTrue(callable(self.s._log_error))

    def test_has_ensure_runtime_config_loaded(self):
        self.assertTrue(hasattr(self.s, '_ensure_runtime_config_loaded'))
        self.assertTrue(callable(self.s._ensure_runtime_config_loaded))

    def test_has_cancel_all_timers(self):
        self.assertTrue(hasattr(self.s, '_cancel_all_timers'))
        self.assertTrue(callable(self.s._cancel_all_timers))


class TestStrategy2026Attributes(unittest.TestCase):
    def test_has_config(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_config")
        self.assertTrue(hasattr(s, 'config'))
        self.assertIsInstance(s.config, dict)

    def test_has_params(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_params")
        self.assertTrue(hasattr(s, 'params'))

    def test_is_real_strategy_flag(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_real_flag")
        self.assertTrue(s._is_real_strategy)

    def test_stop_requested_initially_false(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        s = Strategy2026(strategy_id="test_stop_flag")
        self.assertFalse(s._stop_requested)


class TestStrategy2026LogMethods(unittest.TestCase):
    def setUp(self):
        from ali2026v3_trading.strategy.strategy_core_service import Strategy2026
        self.s = Strategy2026(strategy_id="test_log_methods")

    def test_log_warning_does_not_raise(self):
        try:
            self.s._log_warning("test warning message")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            self.fail(f"_log_warning raised: {e}")

    def test_log_info_does_not_raise(self):
        try:
            self.s._log_info("test info message")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            self.fail(f"_log_info raised: {e}")

    def test_log_error_does_not_raise(self):
        try:
            self.s._log_error("test error message")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            self.fail(f"_log_error raised: {e}")


if __name__ == '__main__':
    unittest.main()
