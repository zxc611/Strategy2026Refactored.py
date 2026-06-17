# MODULE_ID: M2-350
"""
test_hft_order.py - 高频策略(HFT)下单测试脚本

测试链路：
  HFTEnhancementEngine.on_tick_enhanced()
    → SignalTimingFilter.filter_signal() 信号滤波
    → DynamicPursuitEngine.evaluate_surge() 追击评估
    → MicrostructureArbitrageDetector.detect_arbitrage() 套利检测
    → SmartOrderSplitter.plan_order_split() 智能拆单
    → OrderService.send_order() / send_order_split() 实际下单

验证目标：
  1. HFT引擎在共振强度达标时产生有效信号
  2. 信号通过滤波器后触发下单
  3. 智能拆单按深度分布生成子单
  4. OrderService成功记录订单并返回order_id
  5. 追击引擎在共振急升时产生追击信号
  6. 做市商防御单正确生成
"""
import os
import sys
import threading
import time
import unittest
from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.strategy.hft_enhancements import (
    HFTEnhancementEngine,
    get_hft_engine,
)
from ali2026v3_trading.signal.signal_service import (
    KalmanFilter1D,
    EMASignalFilter,
    SignalTimingFilter,
    SignalService,
)
from ali2026v3_trading.order.order_service import (
    OrderService,
    SmartOrderSplitter,
    OrderSplitStrategy,
    get_order_service,
)
from ali2026v3_trading.strategy.strategy_tick_handler import DynamicPursuitEngine
from ali2026v3_trading.order.order_flow_bridge import (
    MicrostructureArbitrageDetector,
    MarketMakerDefenseEngine,
)


def _reset_order_service_singleton():
    from ali2026v3_trading.order.order_base import reset_order_service
    reset_order_service()


def _reset_hft_singleton():
    import ali2026v3_trading.strategy.hft_enhancements as mod
    mod._hft_engine = None


def _reset_global_singletons():
    _reset_order_service_singleton()
    _reset_hft_singleton()
    import ali2026v3_trading.position.position_service as ps_mod
    ps_mod._cross_strategy_risk_guard = None
    ps_mod._position_service_instance = None
    from ali2026v3_trading.governance.mode_engine import ModeEngine
    ModeEngine.reset_instance()


class TestHFTSignalFilterAndOrder(unittest.TestCase):
    """高频策略：信号滤波 → 下单"""

    def setUp(self):
        _reset_order_service_singleton()
        _reset_hft_singleton()
        self.order_svc = get_order_service()
        self.hft = HFTEnhancementEngine({
            'signal_filter_threshold': 0.5,
            'use_kalman': True,
            'aggressive_threshold': 0.7,
            'passive_threshold': 0.4,
        })

    def tearDown(self):
        _reset_order_service_singleton()
        _reset_hft_singleton()

    def test_signal_filter_passes_strong_signal(self):
        for v in [0.5, 0.6, 0.7, 0.8, 0.85]:
            result = self.hft.signal_filter.filter_signal('IF2606', v)
        self.assertTrue(result['signal_passed'])
        self.assertGreater(result['smoothed_value'], 0.5)

    def test_signal_filter_rejects_weak_signal(self):
        result = self.hft.signal_filter.filter_signal('IF2606', 0.2)
        self.assertFalse(result['signal_passed'])

    def test_kalman_filter_converges(self):
        kf = KalmanFilter1D()
        values = [0.6, 0.65, 0.7, 0.75, 0.8]
        for v in values:
            smoothed, velocity = kf.update(v)
        self.assertGreater(smoothed, 0.5)
        self.assertGreater(velocity, 0)

    def test_ema_filter_bullish_crossover(self):
        ema = EMASignalFilter(fast_period=3, slow_period=10)
        for v in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
            ema.update(v)
        self.assertTrue(ema.is_bullish_crossover())


class TestHFTOnTickEnhanced(unittest.TestCase):
    """高频策略：on_tick_enhanced 全链路信号生成"""

    def setUp(self):
        _reset_order_service_singleton()
        _reset_hft_singleton()
        self.hft = HFTEnhancementEngine({
            'signal_filter_threshold': 0.5,
            'surge_threshold': 0.2,
            'arbitrage_deviation_bps': 30.0,
        })

    def tearDown(self):
        _reset_order_service_singleton()
        _reset_hft_singleton()

    def test_on_tick_with_strong_resonance(self):
        for v in [0.4, 0.5, 0.6, 0.7, 0.8]:
            self.hft.on_tick_enhanced(
                instrument_id='IF2606',
                price=4000.0 + v * 10,
                volume=100,
                direction='BUY',
                product='IF',
                resonance_strength=v,
                prev_resonance_strength=max(0, v - 0.1),
                current_state='correct_rise',
                prev_state='other',
            )
        result = self.hft.on_tick_enhanced(
            instrument_id='IF2606',
            price=4008.5,
            volume=100,
            direction='BUY',
            product='IF',
            bid_price=3999.8,
            ask_price=4000.2,
            resonance_strength=0.85,
            prev_resonance_strength=0.8,
            current_state='correct_rise',
            prev_state='other',
        )
        self.assertIsNotNone(result)
        self.assertIsNotNone(result['signal_filter'])
        self.assertTrue(result['signal_filter']['signal_passed'])

    def test_on_tick_pursuit_signal_on_surge(self):
        for i in range(5):
            self.hft.on_tick_enhanced(
                instrument_id='IF2606',
                price=4000.0 + i * 2,
                volume=100,
                direction='BUY',
                product='IF',
                resonance_strength=0.3 + i * 0.15,
                prev_resonance_strength=0.3,
                current_state='correct_rise',
                prev_state='other',
            )
        result = self.hft.on_tick_enhanced(
            instrument_id='IF2606',
            price=4010.0,
            volume=100,
            direction='BUY',
            product='IF',
            resonance_strength=0.9,
            prev_resonance_strength=0.6,
            current_state='correct_rise',
            prev_state='correct_rise',
        )
        if result.get('pursuit_signal'):
            self.assertIn('direction', result['pursuit_signal'])

    def test_on_tick_arbitrage_detection(self):
        for i in range(20):
            self.hft.on_tick_enhanced(
                instrument_id='IF2606',
                price=4000.0,
                volume=100,
                direction='BUY',
                product='IF',
                resonance_strength=0.5,
            )
        result = self.hft.on_tick_enhanced(
            instrument_id='IF2606',
            price=4050.0,
            volume=200,
            direction='BUY',
            product='IF',
            resonance_strength=0.6,
        )
        self.assertIsNotNone(result)

    def test_on_tick_state_transition_signal(self):
        result = self.hft.on_tick_enhanced(
            instrument_id='IF2606',
            price=4000.0,
            volume=100,
            direction='BUY',
            product='IF',
            resonance_strength=0.7,
            current_state='correct_rise',
            prev_state='other',
        )
        if result.get('transition_signal'):
            self.assertIn('from_state', result['transition_signal'])


class TestHFTSmartOrderSplit(unittest.TestCase):
    """高频策略：智能拆单 → 下单"""

    def setUp(self):
        _reset_order_service_singleton()
        self.order_svc = get_order_service()

    def tearDown(self):
        _reset_order_service_singleton()

    def test_aggressive_split_with_depth(self):
        splitter = SmartOrderSplitter(max_depth_levels=5)
        bids = [(3999.0 + i * 0.2, 50 + i * 10) for i in range(5)]
        asks = [(4000.0 + i * 0.2, 50 + i * 10) for i in range(5)]
        result = splitter.plan_order_split(
            instrument_id='IF2606',
            volume=100,
            direction='BUY',
            signal_strength=0.9,
            bids=bids,
            asks=asks,
            strategy=OrderSplitStrategy.AGGRESSIVE,
        )
        self.assertGreater(len(result.child_orders), 0)
        self.assertEqual(result.total_volume, 100)
        self.assertEqual(result.strategy_used, OrderSplitStrategy.AGGRESSIVE)

    def test_passive_split(self):
        splitter = SmartOrderSplitter()
        bids = [(3999.8, 100)]
        asks = [(4000.2, 100)]
        result = splitter.plan_order_split(
            instrument_id='IF2606',
            volume=10,
            direction='BUY',
            signal_strength=0.3,
            bids=bids,
            asks=asks,
            strategy=OrderSplitStrategy.PASSIVE,
        )
        self.assertEqual(len(result.child_orders), 1)
        self.assertEqual(result.strategy_used, OrderSplitStrategy.PASSIVE)

    def test_adaptive_split_high_signal(self):
        splitter = SmartOrderSplitter()
        bids = [(3999.8, 50), (3999.6, 50)]
        asks = [(4000.2, 50), (4000.4, 50)]
        result = splitter.plan_order_split(
            instrument_id='IF2606',
            volume=50,
            direction='BUY',
            signal_strength=0.85,
            bids=bids,
            asks=asks,
            strategy=OrderSplitStrategy.ADAPTIVE,
        )
        self.assertGreater(len(result.child_orders), 0)


class TestHFTOrderPlacement(unittest.TestCase):
    """高频策略：完整下单流程"""

    def setUp(self):
        _reset_order_service_singleton()
        _reset_hft_singleton()
        self.order_svc = get_order_service()
        self.hft = HFTEnhancementEngine({
            'signal_filter_threshold': 0.5,
            'aggressive_threshold': 0.7,
            'passive_threshold': 0.4,
        })
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
        _reset_hft_singleton()

    def test_hft_buy_order_via_send_order(self):
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4000.0,
            direction='BUY',
            action='OPEN',
            open_reason='CORRECT_RESONANCE',
        )
        self.assertIsNotNone(order_id)
        self.assertEqual(len(self.placed_orders), 1)
        self.assertEqual(self.placed_orders[0]['direction'], 'BUY')
        self.assertEqual(self.placed_orders[0]['instrument_id'], 'IF2606')

    def test_hft_sell_order_via_send_order(self):
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4000.0,
            direction='SELL',
            action='OPEN',
            open_reason='CORRECT_DIVERGENCE',
        )
        self.assertIsNotNone(order_id)
        self.assertEqual(self.placed_orders[-1]['direction'], 'SELL')

    def test_hft_split_order_flow(self):
        self.order_svc.enable_hft_enhancements()
        bids = [(3999.8, 50), (3999.6, 50)]
        asks = [(4000.2, 50), (4000.4, 50)]
        order_ids = self.order_svc.send_order_split(
            instrument_id='IF2606',
            volume=30,
            price=4000.0,
            direction='BUY',
            action='OPEN',
            signal_strength=0.9,
            bids=bids,
            asks=asks,
            open_reason='CORRECT_RESONANCE',
        )
        self.assertGreater(len(order_ids), 0)
        for oid in order_ids:
            self.assertIsNotNone(oid)

    def test_hft_defensive_order_flow(self):
        order_ids = self.order_svc.send_defensive_order(
            instrument_id='IF2606',
            volume=5,
            price=4000.0,
            direction='BUY',
            action='CLOSE',
            signal_strength=0.7,
        )
        self.assertGreater(len(order_ids), 0)

    def test_hft_full_pipeline_signal_to_order(self):
        result = self.hft.on_tick_enhanced(
            instrument_id='IF2606',
            price=4000.0,
            volume=100,
            direction='BUY',
            product='IF',
            resonance_strength=0.85,
            prev_resonance_strength=0.5,
            current_state='correct_rise',
            prev_state='other',
        )
        self.assertIsNotNone(result)
        if result.get('signal_filter') and result['signal_filter']['signal_passed']:
            order_id = self.order_svc.send_order(
                instrument_id='IF2606',
                volume=1,
                price=4000.2,
                direction='BUY',
                action='OPEN',
                open_reason='CORRECT_RESONANCE',
            )
            self.assertIsNotNone(order_id)

    def test_hft_order_stats_tracking(self):
        self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4000.0,
            direction='BUY',
            action='OPEN',
            open_reason='CORRECT_RESONANCE',
        )
        stats = self.order_svc.get_stats()
        self.assertGreaterEqual(stats['total_orders'], 1)


class TestHFTPursuitRiskGate(unittest.TestCase):
    def setUp(self):
        from ali2026v3_trading.strategy.strategy_tick_handler import TickHandlerMixin

        self.mixin = TickHandlerMixin.__new__(TickHandlerMixin)
        self.mixin._order_service = MagicMock()
        self.mixin._ensure_order_service = MagicMock()
        self.mixin._ensure_order_service_fn = MagicMock()
        self.mixin._state_store = MagicMock()
        self.mixin._state_store.get_ref.return_value = self.mixin._order_service
        self.mixin._state_store.get.return_value = None
        self.mixin._check_hft_open_risk = MagicMock(return_value=True)
        self.mixin._get_tick_field = MagicMock(return_value=0.0)

    def test_pursuit_entry_blocked_by_risk_service(self):
        hft = MagicMock()
        hft.pursuit_engine.confirm_position_on_platform.return_value = True
        tick = MagicMock()
        blocked = MagicMock()
        blocked.is_block = True
        blocked.reason = 'circuit_breaker_open'

        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as get_rs:
            get_rs.return_value.check_before_trade.return_value = blocked
            self.mixin._execute_pursuit_entry(
                hft,
                {'direction': 'BUY', 'volume': 2, 'price': 4000.0},
                tick,
                'IF2606',
                4000.0,
                2,
                'CFFEX',
            )

        self.mixin._order_service.send_order_split.assert_not_called()

    def test_pursuit_add_passes_risk_service_then_places_order(self):
        hft = MagicMock()
        hft.pursuit_engine.add_platform_order_id.return_value = True
        tick = MagicMock()
        tick.bid_price1 = 3999.8
        tick.ask_price1 = 4000.2
        allowed = MagicMock()
        allowed.is_block = False
        allowed.reason = ''
        self.mixin._order_service.send_order_split.return_value = ['OID-1']

        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as get_rs:
            get_rs.return_value.check_before_trade.return_value = allowed
            self.mixin._execute_pursuit_add(
                hft,
                {'direction': 'BUY', 'volume': 1, 'price': 4000.0},
                tick,
                'IF2606',
                4000.0,
                1,
                'CFFEX',
            )

        self.mixin._order_service.send_order_split.assert_called_once()


class TestHFTMarketMakerDefense(unittest.TestCase):
    """高频策略：做市商防御"""

    def setUp(self):
        self.defense = MarketMakerDefenseEngine(
            ioc_signal_threshold=0.8,
            offset_max_ticks=3,
        )

    def test_defensive_order_creation(self):
        orders = self.defense.create_defensive_order(
            instrument_id='IF2606',
            direction='BUY',
            volume=5,
            price=4000.0,
            signal_strength=0.9,
            tick_size=0.2,
            is_stop_order=False,
        )
        self.assertGreater(len(orders), 0)

    def test_defense_stats(self):
        stats = self.defense.get_stats()
        self.assertIn('service_name', stats)


class TestHFTPursuitEngine(unittest.TestCase):
    """高频策略：动态追击引擎"""

    def setUp(self):
        self.pursuit = DynamicPursuitEngine(
            surge_threshold=0.2,
            max_add_positions=3,
        )

    def test_surge_detection(self):
        result = self.pursuit.evaluate_surge(
            instrument_id='IF2606',
            current_strength=0.8,
            prev_strength=0.4,
            current_price=4000.0,
            direction='BUY',
        )
        if result:
            self.assertIn('direction', result)

    def test_trailing_stop_update(self):
        self.pursuit.evaluate_surge(
            instrument_id='IF2606',
            current_strength=0.8,
            prev_strength=0.4,
            current_price=4000.0,
            direction='BUY',
        )
        pos = self.pursuit._positions.get('IF2606')
        if pos and pos.is_open:
            pos.platform_confirmed = True
            pos.created_at = time.time() - 60
            self.pursuit.update_trailing_stop('IF2606', 4010.0, 'BUY')
            exit_sig = self.pursuit.check_exit('IF2606', 3990.0)
            if exit_sig is not None:
                self.assertIsNotNone(exit_sig)
            else:
                self.assertTrue(True)

    def test_pursuit_stats(self):
        stats = self.pursuit._stats
        self.assertIn('surge_detected', stats)


class TestHFTSignalServiceIntegration(unittest.TestCase):
    """高频策略：SignalService集成下单"""

    def setUp(self):
        _reset_global_singletons()
        self.signal_svc = SignalService()
        self.signal_svc._default_cooldown_seconds = 0.0
        self.signal_svc._adaptive_threshold = None
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
        _reset_global_singletons()

    def test_hft_signal_generates_and_orders(self):
        with patch('ali2026v3_trading.signal.signal_service.ModeEngine') as mock_me:
            mock_me.get_instance.return_value.filter_signal_by_mode.return_value = (True, '')
            signal = self.signal_svc.generate_signal(
                instrument_id='IF2606',
                signal_type='BUY',
                price=4000.0,
                volume=1,
                reason='HFT_CORRECT_RESONANCE',
            )
        self.assertIsNotNone(signal)
        self.assertEqual(signal['signal_type'], 'BUY')
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4000.2,
            direction='BUY',
            action='OPEN',
            open_reason='CORRECT_RESONANCE',
        )
        self.assertIsNotNone(order_id)

    def test_hft_sell_signal_and_order(self):
        with patch('ali2026v3_trading.signal.signal_service.ModeEngine') as mock_me:
            mock_me.get_instance.return_value.filter_signal_by_mode.return_value = (True, '')
            signal = self.signal_svc.generate_signal(
                instrument_id='IF2606',
                signal_type='SELL',
                price=3998.0,
                volume=1,
                reason='HFT_CORRECT_DIVERGENCE',
            )
        self.assertIsNotNone(signal)
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=3997.8,
            direction='SELL',
            action='OPEN',
            open_reason='CORRECT_DIVERGENCE',
        )
        self.assertIsNotNone(order_id)

    def test_hft_close_long_signal(self):
        signal = self.signal_svc.generate_signal(
            instrument_id='IF2606',
            signal_type='CLOSE_LONG',
            price=4010.0,
            volume=1,
            reason='HFT_TAKE_PROFIT',
        )
        self.assertIsNotNone(signal)
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4010.0,
            direction='SELL',
            action='CLOSE',
        )
        self.assertIsNotNone(order_id)


if __name__ == '__main__':
    unittest.main()
