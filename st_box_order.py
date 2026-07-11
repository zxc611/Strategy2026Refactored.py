# MODULE_ID: M2-307
"""
test_box_order.py - 箱形策略下单测试脚本

测试链路：
  BoxDetector.update_bar() → detect_box() 箱体识别
    → classify_extreme_state() 极值子状态判断
    → check_iv_filter() IV过滤
    → check_order_flow_exhaustion() 订单流衰竭确认
    → determine_trade_direction() 方向确定
    → StrategyEcosystem.process_other_strategy_signal() 信号生成
    → SignalService.generate_signal() 信号发布
    → OrderService.send_order() 实际下单

验证目标：
  1. 震荡市箱体正确识别（低ADX + 价格收敛）
  2. 箱底极值 + 看跌共振 → 做多信号 → BUY下单
  3. 箱顶极值 + 看涨共振 → 做空信号 → SELL下单
  4. IV高位过滤通过 → 允许下单
  5. 订单流衰竭确认 → 增强信号置信度
  6. 策略生态系统互斥规则正确执行
  7. 信号→下单全链路通畅，必须发出下单信号
"""
import math
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.strategy.box_detector import (
    BoxDetector,
    BoxProfile,
    ExtremeState,
    BoxStrategyParams,
    get_box_detector,
)
from ali2026v3_trading.strategy.strategy_ecosystem import (
    StrategyEcosystem,
    StrategySlot,
    CapitalRoute,
    EcosystemTradeRecord,
    get_strategy_ecosystem,
)
from ali2026v3_trading.signal.signal_service import SignalService
from ali2026v3_trading.order.order_service import OrderService, get_order_service


def _reset_order_service_singleton():
    from ali2026v3_trading.order.order_base import reset_order_service
    reset_order_service()


def _reset_ecosystem_singleton():
    import ali2026v3_trading.strategy.strategy_ecosystem as mod
    mod._ecosystem = None


def _reset_box_detector_singleton():
    import ali2026v3_trading.strategy.box_detector as mod
    mod._box_detector = None


def _reset_global_singletons():
    _reset_order_service_singleton()
    _reset_ecosystem_singleton()
    _reset_box_detector_singleton()
    from ali2026v3_trading.position.position_service import reset_position_service
    reset_position_service()
    import ali2026v3_trading.position.position_greeks as pg_mod
    pg_mod._cross_strategy_risk_guard = None
    from ali2026v3_trading.governance.mode_engine import ModeEngine
    ModeEngine.reset_instance()


def _build_ranging_market(bd: BoxDetector, n_bars: int = 40,
                          box_top: float = 4020.0,
                          box_bottom: float = 3980.0):
    prices = [
        3995, 4010, 3990, 4015, 3985, 4018, 3988, 4012, 3992, 4016,
        3986, 4019, 3988, 4014, 3989, 4017, 3987, 4013, 3993, 4011,
        3990, 4018, 3985, 4016, 3988, 4015, 3992, 4019, 3986, 4017,
        3991, 4014, 3989, 4012, 3993, 4010, 3987, 4016, 3985, 4018,
    ]
    for p in prices[:n_bars]:
        high = min(p + 3, box_top + 2)
        low = max(p - 3, box_bottom - 2)
        bd.update_bar(high, low, p, 1000)


def _build_iv_history(bd: BoxDetector, n: int = 100, base_iv: float = 0.20):
    for i in range(n):
        iv = base_iv + 0.02 * (i % 10) / 10.0
        bd.update_iv(iv)


class TestBoxDetectorAndOrder(unittest.TestCase):
    """箱形策略：箱体检测 → 极值判断 → 下单"""

    def setUp(self):
        _reset_order_service_singleton()
        self.bd = BoxDetector(
            lookback_bars=60,
            min_box_bars=10,
            adx_threshold=35.0,
        )
        self.order_svc = get_order_service()
        self.placed_orders = []
        self.order_svc.bind_platform_apis(
            insert_order_func=self._mock_insert_order,
            cancel_order_func=lambda x: None,
        )

    def _mock_insert_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        mock_result = MagicMock()
        mock_result.OrderRef = f"MOCK_{int(time.time()*1000)}"
        mock_result.order_id = f"MOCK_{int(time.time()*1000)}"
        mock_result.OrderStatus = 'SUBMITTED'
        mock_result.VolumeTraded = 0
        return mock_result

    def tearDown(self):
        _reset_order_service_singleton()

    def test_ranging_market_box_detected(self):
        _build_ranging_market(self.bd, 40)
        profile = self.bd.detect_box()
        self.assertIsInstance(profile, BoxProfile)
        self.assertTrue(profile.is_box, "Ranging market must produce a valid box")
        self.assertTrue(profile.is_valid)
        self.assertGreater(profile.upper, profile.lower)
        self.assertGreater(profile.bounce_count, 0)

    def test_trending_market_no_box(self):
        for i in range(30):
            price = 3800 + i * 20
            self.bd.update_bar(price + 5, price - 5, price, 1000)
        profile = self.bd.detect_box()
        self.assertFalse(profile.is_box)


class TestBoxExtremeStateAndOrder(unittest.TestCase):
    """箱形策略：极值判断 → 方向确定 → 下单（必须发出下单信号）"""

    def setUp(self):
        _reset_global_singletons()
        self.bd = BoxDetector(
            lookback_bars=60,
            min_box_bars=10,
            adx_threshold=35.0,
        )
        _build_ranging_market(self.bd, 40)
        self.bd.detect_box()
        _build_iv_history(self.bd, 100, 0.20)
        self.order_svc = get_order_service()
        self.placed_orders = []
        self.order_svc.bind_platform_apis(
            insert_order_func=self._mock_insert_order,
            cancel_order_func=lambda x: None,
        )
        self.signal_svc = SignalService()
        self.signal_svc._default_cooldown_seconds = 0.0
        self.signal_svc._adaptive_threshold = None

    def _mock_insert_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        mock_result = MagicMock()
        mock_result.OrderRef = f"MOCK_{int(time.time()*1000)}"
        mock_result.order_id = f"MOCK_{int(time.time()*1000)}"
        mock_result.OrderStatus = 'SUBMITTED'
        mock_result.VolumeTraded = 0
        return mock_result

    def tearDown(self):
        _reset_global_singletons()

    def test_bottom_extreme_long_order(self):
        box = self.bd.get_current_box()
        self.assertIsNotNone(box, "Box must be detected for extreme state test")
        self.assertTrue(box.is_valid, "Box must be valid for extreme state test")
        extreme = self.bd.classify_extreme_state(
            current_price=box.lower,
            resonance_direction='fall',
            resonance_strength=0.9,
            current_iv=0.28,
            flow_imbalance=0.05,
            cvd_slope=0.001,
        )
        self.assertTrue(extreme.tradeable, "Bottom extreme must be tradeable")
        self.assertTrue(extreme.is_bottom_extreme)
        trade_dir = self.bd.determine_trade_direction(extreme)
        self.assertEqual(trade_dir, 'long')
        signal = self.signal_svc.generate_signal(
            instrument_id='IF2606',
            signal_type='BUY',
            price=box.lower,
            volume=1,
            reason='BOX_BOTTOM_EXTREME',
        )
        self.assertIsNotNone(signal)
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=box.lower,
            direction='BUY',
            action='OPEN',
            open_reason='OTHER_SCALP',
        )
        self.assertIsNotNone(order_id, "Order ID must be returned for bottom extreme BUY")
        self.assertGreater(len(self.placed_orders), 0)
        self.assertEqual(self.placed_orders[-1]['direction'], 'BUY')

    def test_top_extreme_short_order(self):
        box = self.bd.get_current_box()
        self.assertIsNotNone(box, "Box must be detected for extreme state test")
        self.assertTrue(box.is_valid, "Box must be valid for extreme state test")
        extreme = self.bd.classify_extreme_state(
            current_price=box.upper,
            resonance_direction='rise',
            resonance_strength=0.9,
            current_iv=0.28,
            flow_imbalance=0.05,
            cvd_slope=0.001,
        )
        self.assertTrue(extreme.tradeable, "Top extreme must be tradeable")
        self.assertTrue(extreme.is_top_extreme)
        trade_dir = self.bd.determine_trade_direction(extreme)
        self.assertEqual(trade_dir, 'short')
        signal = self.signal_svc.generate_signal(
            instrument_id='IF2606',
            signal_type='SELL',
            price=box.upper,
            volume=1,
            reason='BOX_TOP_EXTREME',
        )
        self.assertIsNotNone(signal)
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=box.upper,
            direction='SELL',
            action='OPEN',
            open_reason='OTHER_SCALP',
        )
        self.assertIsNotNone(order_id, "Order ID must be returned for top extreme SELL")
        self.assertGreater(len(self.placed_orders), 0)
        self.assertEqual(self.placed_orders[-1]['direction'], 'SELL')


class TestBoxIVFilterAndOrder(unittest.TestCase):
    """箱形策略：IV过滤 → 下单"""

    def setUp(self):
        self.bd = BoxDetector()
        _build_iv_history(self.bd, 100, 0.20)

    def test_high_iv_passes_filter(self):
        passed = self.bd.check_iv_filter(0.28)
        self.assertTrue(passed)

    def test_low_iv_fails_filter(self):
        passed = self.bd.check_iv_filter(0.08)
        self.assertFalse(passed)

    def test_zero_iv_fails(self):
        passed = self.bd.check_iv_filter(0.0)
        self.assertFalse(passed)


class TestBoxOrderFlowExhaustion(unittest.TestCase):
    """箱形策略：订单流衰竭确认"""

    def setUp(self):
        self.bd = BoxDetector()

    def test_exhausted_imbalance(self):
        result = self.bd.check_order_flow_exhaustion(
            flow_imbalance=0.05, cvd_slope=0.001,
        )
        self.assertTrue(result)

    def test_exhausted_cvd_stalling(self):
        result = self.bd.check_order_flow_exhaustion(
            flow_imbalance=0.8, cvd_slope=0.001,
        )
        self.assertTrue(result)

    def test_active_flow_not_exhausted(self):
        result = self.bd.check_order_flow_exhaustion(
            flow_imbalance=0.8, cvd_slope=0.5,
        )
        self.assertFalse(result)


class TestBoxEcosystemOrder(unittest.TestCase):
    """箱形策略：策略生态系统 → 下单（必须发出下单信号）"""

    def setUp(self):
        _reset_order_service_singleton()
        _reset_ecosystem_singleton()
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)
        self.bd = self.eco.box_detector
        _build_ranging_market(self.bd, 40)
        self.bd.detect_box()
        _build_iv_history(self.bd, 100, 0.20)
        self.order_svc = get_order_service()
        self.placed_orders = []
        self.order_svc.bind_platform_apis(
            insert_order_func=self._mock_insert_order,
            cancel_order_func=lambda x: None,
        )

    def _mock_insert_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        mock_result = MagicMock()
        mock_result.OrderRef = f"MOCK_{int(time.time()*1000)}"
        mock_result.order_id = f"MOCK_{int(time.time()*1000)}"
        mock_result.OrderStatus = 'SUBMITTED'
        mock_result.VolumeTraded = 0
        return mock_result

    def tearDown(self):
        _reset_order_service_singleton()
        _reset_ecosystem_singleton()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_other_strategy_signal_bottom_extreme(self):
        self.eco.switch_active_strategy('other')
        box = self.bd.get_current_box()
        self.assertIsNotNone(box, "Box must be detected in ecosystem test")
        self.assertTrue(box.is_valid, "Box must be valid in ecosystem test")
        result = self.eco.process_other_strategy_signal(
            current_price=box.lower,
            resonance_direction='fall',
            resonance_strength=0.9,
            current_iv=0.28,
            flow_imbalance=0.05,
            cvd_slope=0.001,
        )
        self.assertEqual(result['action'], 'open', "Bottom extreme must produce open action")
        self.assertEqual(result['direction'], 'long')
        self.assertEqual(result['open_reason'], 'OTHER_SCALP')
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=box.lower,
            direction='BUY',
            action='OPEN',
            open_reason='OTHER_SCALP',
        )
        self.assertIsNotNone(order_id, "Order must be placed for bottom extreme")
        self.assertGreater(len(self.placed_orders), 0)

    def test_other_strategy_signal_top_extreme(self):
        self.eco.switch_active_strategy('other')
        box = self.bd.get_current_box()
        self.assertIsNotNone(box, "Box must be detected in ecosystem test")
        self.assertTrue(box.is_valid, "Box must be valid in ecosystem test")
        result = self.eco.process_other_strategy_signal(
            current_price=box.upper,
            resonance_direction='rise',
            resonance_strength=0.9,
            current_iv=0.28,
            flow_imbalance=0.05,
            cvd_slope=0.001,
        )
        self.assertEqual(result['action'], 'open', "Top extreme must produce open action")
        self.assertEqual(result['direction'], 'short')
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=box.upper,
            direction='SELL',
            action='OPEN',
            open_reason='OTHER_SCALP',
        )
        self.assertIsNotNone(order_id, "Order must be placed for top extreme")
        self.assertGreater(len(self.placed_orders), 0)

    def test_mutual_exclusion_blocks_master_in_other_state(self):
        self.eco.switch_active_strategy('other')
        allowed, reason = self.eco.check_mutual_exclusion('master', 'long')
        self.assertFalse(allowed)

    def test_other_strategy_allowed_in_other_state(self):
        self.eco.switch_active_strategy('other')
        allowed, reason = self.eco.check_mutual_exclusion('other', 'long')
        self.assertTrue(allowed)


class TestBoxCapitalRoutingAndOrder(unittest.TestCase):
    """箱形策略：资金路由 → 下单"""

    def setUp(self):
        _reset_order_service_singleton()
        _reset_ecosystem_singleton()
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)
        self.order_svc = get_order_service()
        self.placed_orders = []
        self.order_svc.bind_platform_apis(
            insert_order_func=self._mock_insert_order,
            cancel_order_func=lambda x: None,
        )

    def _mock_insert_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        mock_result = MagicMock()
        mock_result.OrderRef = f"MOCK_{int(time.time()*1000)}"
        mock_result.order_id = f"MOCK_{int(time.time()*1000)}"
        mock_result.OrderStatus = 'SUBMITTED'
        mock_result.VolumeTraded = 0
        return mock_result

    def tearDown(self):
        _reset_order_service_singleton()
        _reset_ecosystem_singleton()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_other_state_boosts_other_allocation(self):
        allocs = self.eco.route_capital('other')
        self.assertGreater(allocs['other'], 0.1)
        self.assertAlmostEqual(sum(allocs.values()), 1.0, places=5)

    def test_box_order_with_position_scale(self):
        params = BoxStrategyParams(position_scale=0.3)
        bd = BoxDetector(params=params, lookback_bars=60, min_box_bars=10, adx_threshold=35.0)
        _build_ranging_market(bd, 40)
        bd.detect_box()
        box = bd.get_current_box()
        self.assertIsNotNone(box, "Box must be detected with position scale test")
        self.assertTrue(box.is_valid, "Box must be valid with position scale test")
        volume = max(1, int(params.position_scale * 10))
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=volume,
            price=box.lower,
            direction='BUY',
            action='OPEN',
            open_reason='OTHER_SCALP',
        )
        self.assertIsNotNone(order_id, "Order must be placed with position scale")
        self.assertGreater(len(self.placed_orders), 0)


class TestBoxClosePositionOrder(unittest.TestCase):
    """箱形策略：平仓下单"""

    def setUp(self):
        _reset_order_service_singleton()
        self.order_svc = get_order_service()
        self.placed_orders = []
        self.order_svc.bind_platform_apis(
            insert_order_func=self._mock_insert_order,
            cancel_order_func=lambda x: None,
        )

    def _mock_insert_order(self, **kwargs):
        self.placed_orders.append(kwargs)
        mock_result = MagicMock()
        mock_result.OrderRef = f"MOCK_{int(time.time()*1000)}"
        mock_result.order_id = f"MOCK_{int(time.time()*1000)}"
        mock_result.OrderStatus = 'SUBMITTED'
        mock_result.VolumeTraded = 0
        return mock_result

    def tearDown(self):
        _reset_order_service_singleton()

    def test_box_close_long_order(self):
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4020.0,
            direction='SELL',
            action='CLOSE',
        )
        self.assertIsNotNone(order_id)
        self.assertEqual(self.placed_orders[-1]['direction'], 'SELL')

    def test_box_close_short_order(self):
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=3980.0,
            direction='BUY',
            action='CLOSE',
        )
        self.assertIsNotNone(order_id)
        self.assertEqual(self.placed_orders[-1]['direction'], 'BUY')


class TestBoxHealthAndStats(unittest.TestCase):
    """箱形策略：健康检查与统计"""

    def setUp(self):
        self.bd = BoxDetector(lookback_bars=60, min_box_bars=10, adx_threshold=35.0)
        _build_ranging_market(self.bd, 40)

    def test_box_detector_health(self):
        health = self.bd.get_health_status()
        self.assertEqual(health['component'], 'box_detector')
        self.assertIn('status', health)

    def test_box_detector_stats(self):
        stats = self.bd.get_stats()
        self.assertIn('bars_processed', stats)
        self.assertIn('boxes_detected', stats)
        self.assertGreaterEqual(stats['bars_processed'], 40)


if __name__ == '__main__':
    unittest.main()
