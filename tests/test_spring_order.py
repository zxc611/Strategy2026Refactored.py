"""
test_spring_order.py - 弹簧策略下单测试脚本

测试链路：
  BoxSpringStrategy.update_box() 箱体识别
    → update_iv() IV百分位监控
    → detect_spring() 弹簧识别（四条件同时满足）
    → check_trigger() 入场信号触发
    → execute_spring_entry() 下单执行
    → on_premium_update() 平仓纪律
    → OrderService.send_order() 实际下单

弹簧四条件：
  1. 箱体结构已识别（价格在箱顶箱底之间震荡）
  2. 近月期权（2-5天到期），权利金极便宜
  3. IV百分位处于近期极低水平（下分位数）
  4. 期货价格接近该期权行权价（Gamma最敏感位置）

额外约束：
  - detect_spring要求价格在箱体0.3-0.7位置（中间区域）
  - BUY_CALL: price_pos 0.3-0.4
  - BUY_STRADDLE: price_pos 0.4-0.6
  - BUY_PUT: price_pos 0.6-0.7

验证目标：
  1. 箱体正确建立和维持
  2. IV极低时弹簧条件满足，必须发出信号
  3. 订单流异动触发入场信号，必须触发
  4. BUY_CALL/BUY_PUT/BUY_STRADDLE方向正确
  5. 止盈/止损/时间过期平仓逻辑正确
  6. 铁律：绝不平移为趋势策略
  7. 期望值模型正确计算
  8. 全链路必须发出下单信号
"""
import math
import os
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.box_spring_strategy import (
    BoxSpringStrategy,
    BoxRange,
    SpringSignal,
    SpringPosition,
    SpringState,
    get_box_spring_strategy,
)
from ali2026v3_trading.order_service import OrderService, get_order_service


def _reset_order_service_singleton():
    import ali2026v3_trading.order_service as mod
    mod._order_service_instance = None


def _reset_box_spring_singleton():
    import ali2026v3_trading.box_spring_strategy as mod
    mod._box_spring_instance = None


def _reset_global_singletons():
    _reset_order_service_singleton()
    _reset_box_spring_singleton()
    import ali2026v3_trading.position_service as ps_mod
    ps_mod._cross_strategy_risk_guard = None
    ps_mod._position_service_instance = None
    from ali2026v3_trading.mode_engine import ModeEngine
    ModeEngine.reset_instance()


BOX_TOP = 4050.0
BOX_BOTTOM = 3950.0
BOX_MID = (BOX_TOP + BOX_BOTTOM) / 2.0


def _build_box(strategy: BoxSpringStrategy, instrument_id: str = 'IF2606',
               box_top: float = BOX_TOP, box_bottom: float = BOX_BOTTOM,
               n_touches: int = 5):
    for i in range(n_touches):
        high = box_top if i % 2 == 0 else box_top - 5
        low = box_bottom if i % 2 == 1 else box_bottom + 5
        close = (box_top + box_bottom) / 2.0
        strategy.update_box(instrument_id, high, low, close)
    return strategy.get_active_box(instrument_id)


def _build_iv_history(strategy: BoxSpringStrategy, option_id: str = 'IF2606-C-4000',
                      n: int = 120, base_iv: float = 0.20):
    for i in range(n):
        iv = base_iv + 0.005 * math.sin(i * 0.1)
        strategy.update_iv(option_id, iv)


def _setup_spring_env(strategy: BoxSpringStrategy, option_id: str = 'IF2606-C-4000'):
    _build_box(strategy, 'IF2606', BOX_TOP, BOX_BOTTOM, n_touches=5)
    _build_iv_history(strategy, option_id, 120, 0.20)
    strategy.update_iv(option_id, 0.08)


class TestBoxRangeAndOrder(unittest.TestCase):
    """弹簧策略：箱体建立 → 下单前置条件"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({
            'min_box_touches': 3,
            'max_box_width_pct': 0.04,
        })

    def test_box_creation(self):
        box = self.strategy.update_box('IF2606', BOX_TOP, BOX_BOTTOM, BOX_MID)
        self.assertIsNotNone(box)
        self.assertEqual(box.instrument_id, 'IF2606')

    def test_box_contains_price(self):
        box = self.strategy.update_box('IF2606', BOX_TOP, BOX_BOTTOM, BOX_MID)
        self.assertTrue(box.contains_price(BOX_MID))
        self.assertTrue(box.contains_price(3985.0))
        self.assertFalse(box.contains_price(3900.0))

    def test_box_price_position(self):
        box = self.strategy.update_box('IF2606', BOX_TOP, BOX_BOTTOM, BOX_MID)
        pos = box.price_position(BOX_MID)
        self.assertAlmostEqual(pos, 0.5, places=1)

    def test_box_invalidated_on_break(self):
        for i in range(5):
            self.strategy.update_box('IF2606', BOX_TOP, BOX_BOTTOM, BOX_MID)
        result = self.strategy.update_box('IF2606', 4100.0, BOX_BOTTOM, 4080.0)
        self.assertIsNone(result)

    def test_active_box_requires_min_touches(self):
        box = self.strategy.update_box('IF2606', BOX_TOP, BOX_BOTTOM, BOX_MID)
        self.assertIsNone(self.strategy.get_active_box('IF2606'))
        for i in range(4):
            self.strategy.update_box('IF2606', BOX_TOP, BOX_BOTTOM, BOX_MID)
        active = self.strategy.get_active_box('IF2606')
        self.assertIsNotNone(active)


class TestSpringIVPercentile(unittest.TestCase):
    """弹簧策略：IV百分位监控"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({'iv_lookback_bars': 120})

    def test_iv_percentile_low(self):
        option_id = 'IF2606-C-4000'
        for i in range(100):
            self.strategy.update_iv(option_id, 0.20 + 0.01 * (i % 10))
        pct = self.strategy.update_iv(option_id, 0.12)
        self.assertLess(pct, 20.0)

    def test_iv_percentile_high(self):
        option_id = 'IF2606-C-4000'
        for i in range(100):
            self.strategy.update_iv(option_id, 0.10 + 0.01 * (i % 10))
        pct = self.strategy.update_iv(option_id, 0.30)
        self.assertGreater(pct, 80.0)

    def test_iv_percentile_insufficient_data(self):
        pct = self.strategy.update_iv('IF2606-C-4000', 0.15)
        self.assertEqual(pct, 50.0)


class TestSpringDetectionAndOrder(unittest.TestCase):
    """弹簧策略：弹簧识别 → 下单（必须发出信号）"""

    def setUp(self):
        _reset_global_singletons()
        self.strategy = BoxSpringStrategy({
            'min_box_touches': 3,
            'max_box_width_pct': 0.04,
            'iv_low_percentile': 5.0,
            'iv_very_low_percentile': 2.0,
            'min_days_to_expiry': 2,
            'max_days_to_expiry': 5,
            'max_premium_cost_pct': 0.015,
            'spring_cooldown_sec': 0,
        })

    def tearDown(self):
        _reset_global_singletons()

    def test_detect_spring_all_conditions_met(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Spring signal must be generated when all conditions met")
        self.assertEqual(signal.spring_state, SpringState.COMPRESSED)
        self.assertIn(signal.direction, ('BUY_CALL', 'BUY_PUT', 'BUY_STRADDLE'))

    def test_detect_spring_buy_call_direction(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=3985.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Spring signal must be generated for BUY_CALL")
        self.assertEqual(signal.direction, 'BUY_CALL')

    def test_detect_spring_buy_put_direction(self):
        _setup_spring_env(self.strategy, 'IF2606-P-4000')
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4015.0,
            option_instrument_id='IF2606-P-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Spring signal must be generated for BUY_PUT")
        self.assertEqual(signal.direction, 'BUY_PUT')

    def test_detect_spring_buy_straddle_direction(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Spring signal must be generated for BUY_STRADDLE")
        self.assertEqual(signal.direction, 'BUY_STRADDLE')

    def test_detect_spring_no_box_returns_none(self):
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
        )
        self.assertIsNone(signal)

    def test_detect_spring_iv_not_low_returns_none(self):
        _build_box(self.strategy, 'IF2606', BOX_TOP, BOX_BOTTOM, n_touches=5)
        option_id = 'IF2606-C-4000'
        for i in range(120):
            self.strategy.update_iv(option_id, 0.20)
        self.strategy.update_iv(option_id, 0.30)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id=option_id,
            strike_price=4000.0,
            iv=0.30,
            premium_price=50.0,
            days_to_expiry=3,
        )
        self.assertIsNone(signal)

    def test_detect_spring_wrong_expiry_returns_none(self):
        _build_box(self.strategy, 'IF2606', BOX_TOP, BOX_BOTTOM, n_touches=5)
        option_id = 'IF2606-C-4000'
        for i in range(120):
            self.strategy.update_iv(option_id, 0.12)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id=option_id,
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=10,
        )
        self.assertIsNone(signal)

    def test_detect_spring_strike_too_far_returns_none(self):
        _build_box(self.strategy, 'IF2606', BOX_TOP, BOX_BOTTOM, n_touches=5)
        option_id = 'IF2606-C-4100'
        for i in range(120):
            self.strategy.update_iv(option_id, 0.12)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id=option_id,
            strike_price=4100.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
        )
        self.assertIsNone(signal)

    def test_detect_spring_price_outside_range_returns_none(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=3960.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNone(signal, "Price at position 0.1 must be rejected (< 0.3)")


class TestSpringTriggerAndOrder(unittest.TestCase):
    """弹簧策略：入场触发 → 下单执行（必须触发信号）"""

    def setUp(self):
        _reset_global_singletons()
        self.strategy = BoxSpringStrategy({
            'min_box_touches': 3,
            'max_box_width_pct': 0.04,
            'iv_low_percentile': 5.0,
            'spring_cooldown_sec': 0,
            'max_spring_positions': 3,
            'stop_profit_ratio': 5.0,
            'max_loss_pct': 0.95,
        })

    def tearDown(self):
        _reset_global_singletons()

    def test_trigger_with_order_flow_imbalance(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Signal must be generated before trigger test")
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=0.5)
        self.assertIsNotNone(triggered, "Trigger must fire with order flow imbalance=0.5")
        self.assertEqual(triggered.spring_state, SpringState.TRIGGERED)

    def test_trigger_with_option_chain_activity(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Signal must be generated before trigger test")
        triggered = self.strategy.check_trigger('IF2606', option_chain_activity=2.0)
        self.assertIsNotNone(triggered, "Trigger must fire with option chain activity=2.0")
        self.assertEqual(triggered.spring_state, SpringState.TRIGGERED)

    def test_no_trigger_without_catalyst(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "Signal must be generated before trigger test")
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=0.1, option_chain_activity=0.5)
        self.assertIsNone(triggered)


class TestSpringEntryOrder(unittest.TestCase):
    """弹簧策略：下单执行（必须发出下单信号）"""

    def setUp(self):
        _reset_global_singletons()
        self.strategy = BoxSpringStrategy({
            'min_box_touches': 3,
            'max_box_width_pct': 0.04,
            'iv_low_percentile': 5.0,
            'spring_cooldown_sec': 0,
            'max_spring_positions': 3,
            'stop_profit_ratio': 5.0,
            'max_loss_pct': 0.95,
        })
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

    def test_buy_call_entry_order(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=3985.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "BUY_CALL signal must be generated")
        self.assertEqual(signal.direction, 'BUY_CALL')
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=0.5)
        self.assertIsNotNone(triggered, "BUY_CALL trigger must fire")
        order_id = self.strategy.execute_spring_entry(triggered)
        self.assertIsNotNone(order_id, "BUY_CALL order must be placed")
        self.assertGreater(len(self.placed_orders), 0)

    def test_buy_put_entry_order(self):
        _setup_spring_env(self.strategy, 'IF2606-P-4000')
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4015.0,
            option_instrument_id='IF2606-P-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "BUY_PUT signal must be generated")
        self.assertEqual(signal.direction, 'BUY_PUT')
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=-0.5)
        self.assertIsNotNone(triggered, "BUY_PUT trigger must fire")
        order_id = self.strategy.execute_spring_entry(triggered)
        self.assertIsNotNone(order_id, "BUY_PUT order must be placed")

    def test_buy_straddle_entry_order(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "BUY_STRADDLE signal must be generated")
        self.assertEqual(signal.direction, 'BUY_STRADDLE')
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=0.5)
        self.assertIsNotNone(triggered, "BUY_STRADDLE trigger must fire")
        with patch.object(self.strategy, '_find_straddle_pair',
                          return_value=('IF2606-P-4000', 48.0, 'CALL')), \
             patch.object(self.order_svc.rate_limiter, 'acquire', return_value=True):
            order_id = self.strategy.execute_spring_entry(triggered)
        self.assertIsNotNone(order_id, "BUY_STRADDLE order must be placed")
        self.assertGreater(len(self.placed_orders), 0)

    def test_max_positions_limit(self):
        _setup_spring_env(self.strategy)
        for j in range(4):
            sig = SpringSignal(
                signal_id=f'SPRING_TEST_{j}',
                instrument_id='IF2606',
                option_instrument_id=f'IF2606-C-400{j}',
                spring_state=SpringState.TRIGGERED,
                iv_percentile=3.0,
                premium_cost_pct=0.01,
                gamma_exposure=0.5,
                box_id='BOX_TEST',
                direction='BUY_CALL',
                strike_price=4000.0,
                current_price=4000.0,
                premium_price=50.0,
            )
            self.strategy.execute_spring_entry(sig)
        stats = self.strategy.get_stats()
        self.assertLessEqual(stats['positions_opened'], 3)


class TestSpringClosePosition(unittest.TestCase):
    """弹簧策略：平仓纪律"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({
            'stop_profit_ratio': 5.0,
            'max_loss_pct': 0.95,
            'max_spring_hold_minutes': 120,
        })

    def test_take_profit_close(self):
        pos = SpringPosition(
            position_id='SP_TEST_1',
            signal_id='SIG_TEST_1',
            instrument_id='IF2606',
            option_instrument_id='IF2606-C-4000',
            direction='BUY_CALL',
            entry_premium=50.0,
            current_premium=300.0,
            entry_time=datetime.now() - timedelta(minutes=10),
            stop_profit_ratio=5.0,
            max_loss_pct=0.95,
            box_id='BOX_TEST',
        )
        self.assertTrue(pos.should_take_profit)

    def test_stop_loss_close(self):
        pos = SpringPosition(
            position_id='SP_TEST_2',
            signal_id='SIG_TEST_2',
            instrument_id='IF2606',
            option_instrument_id='IF2606-C-4000',
            direction='BUY_CALL',
            entry_premium=50.0,
            current_premium=1.0,
            entry_time=datetime.now() - timedelta(minutes=10),
            stop_profit_ratio=5.0,
            max_loss_pct=0.95,
            box_id='BOX_TEST',
        )
        self.assertTrue(pos.should_accept_loss)

    def test_pnl_ratio_calculation(self):
        pos = SpringPosition(
            position_id='SP_TEST_3',
            signal_id='SIG_TEST_3',
            instrument_id='IF2606',
            option_instrument_id='IF2606-C-4000',
            direction='BUY_CALL',
            entry_premium=50.0,
            current_premium=100.0,
            entry_time=datetime.now(),
            stop_profit_ratio=5.0,
            max_loss_pct=0.95,
            box_id='BOX_TEST',
        )
        self.assertAlmostEqual(pos.pnl_ratio, 2.0)


class TestSpringIronRule(unittest.TestCase):
    """弹簧策略：铁律 — 绝不平移为趋势策略"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({})

    def test_prevent_trend_conversion(self):
        pos = SpringPosition(
            position_id='SP_IRON_1',
            signal_id='SIG_IRON_1',
            instrument_id='IF2606',
            option_instrument_id='IF2606-C-4000',
            direction='BUY_CALL',
            entry_premium=50.0,
            current_premium=100.0,
            entry_time=datetime.now(),
            stop_profit_ratio=5.0,
            max_loss_pct=0.95,
            box_id='BOX_TEST',
        )
        self.strategy._positions['SP_IRON_1'] = pos
        allowed = self.strategy.prevent_trend_conversion(
            'IF2606-C-4000', 'OPEN', 'CORRECT_RESONANCE',
        )
        self.assertFalse(allowed)

    def test_allow_non_spring_instrument(self):
        allowed = self.strategy.prevent_trend_conversion(
            'IF2606', 'OPEN', 'CORRECT_RESONANCE',
        )
        self.assertTrue(allowed)

    def test_is_spring_position(self):
        pos = SpringPosition(
            position_id='SP_CHECK_1',
            signal_id='SIG_CHECK_1',
            instrument_id='IF2606',
            option_instrument_id='IF2606-C-4000',
            direction='BUY_CALL',
            entry_premium=50.0,
            current_premium=100.0,
            entry_time=datetime.now(),
            stop_profit_ratio=5.0,
            max_loss_pct=0.95,
            box_id='BOX_TEST',
        )
        self.strategy._positions['SP_CHECK_1'] = pos
        self.assertTrue(self.strategy.is_spring_position('IF2606-C-4000'))
        self.assertFalse(self.strategy.is_spring_position('IF2607-C-4100'))


class TestSpringExpectedValue(unittest.TestCase):
    """弹簧策略：期望值模型"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({})

    def test_no_trades_zero_ev(self):
        ev = self.strategy.get_expected_value()
        self.assertEqual(ev['total_trades'], 0)
        self.assertFalse(ev['is_positive_ev'])

    def test_positive_ev_with_wins(self):
        for pnl_ratio in [5.0, 6.0, 4.0]:
            pos = SpringPosition(
                position_id=f'SP_EV_{pnl_ratio}',
                signal_id=f'SIG_EV_{pnl_ratio}',
                instrument_id='IF2606',
                option_instrument_id='IF2606-C-4000',
                direction='BUY_CALL',
                entry_premium=50.0,
                current_premium=50.0 * pnl_ratio,
                entry_time=datetime.now(),
                stop_profit_ratio=5.0,
                max_loss_pct=0.95,
                box_id='BOX_TEST',
            )
            pos.is_open = False
            self.strategy._positions[f'SP_EV_{pnl_ratio}'] = pos
        self.strategy._stats['win_count'] = 3
        ev = self.strategy.get_expected_value()
        self.assertTrue(ev['is_positive_ev'])


class TestSpringDirectionInference(unittest.TestCase):
    """弹簧策略：方向推断"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({})

    def test_low_position_buy_call(self):
        box = BoxRange(
            box_id='B1', instrument_id='IF2606',
            box_top=BOX_TOP, box_bottom=BOX_BOTTOM,
            box_width_pct=0.025, confirmed_at=datetime.now(),
            touch_count=5,
        )
        direction = self.strategy._infer_direction(box, 3985.0, 0.35)
        self.assertEqual(direction, 'BUY_CALL')

    def test_high_position_buy_put(self):
        box = BoxRange(
            box_id='B2', instrument_id='IF2606',
            box_top=BOX_TOP, box_bottom=BOX_BOTTOM,
            box_width_pct=0.025, confirmed_at=datetime.now(),
            touch_count=5,
        )
        direction = self.strategy._infer_direction(box, 4015.0, 0.65)
        self.assertEqual(direction, 'BUY_PUT')

    def test_mid_position_buy_straddle(self):
        box = BoxRange(
            box_id='B3', instrument_id='IF2606',
            box_top=BOX_TOP, box_bottom=BOX_BOTTOM,
            box_width_pct=0.025, confirmed_at=datetime.now(),
            touch_count=5,
        )
        direction = self.strategy._infer_direction(box, 4000.0, 0.5)
        self.assertEqual(direction, 'BUY_STRADDLE')


class TestSpringFullPipelineOrder(unittest.TestCase):
    """弹簧策略：完整下单流程（detect → trigger → execute，必须发出下单信号）"""

    def setUp(self):
        _reset_global_singletons()
        self.strategy = BoxSpringStrategy({
            'min_box_touches': 3,
            'max_box_width_pct': 0.04,
            'iv_low_percentile': 5.0,
            'spring_cooldown_sec': 0,
            'max_spring_positions': 3,
            'stop_profit_ratio': 5.0,
            'max_loss_pct': 0.95,
        })
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

    def test_full_pipeline_buy_call(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=3985.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "BUY_CALL signal must be generated in full pipeline")
        self.assertEqual(signal.direction, 'BUY_CALL')
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=0.5)
        self.assertIsNotNone(triggered, "BUY_CALL trigger must fire in full pipeline")
        order_id = self.strategy.execute_spring_entry(triggered)
        self.assertIsNotNone(order_id, "BUY_CALL order must be placed in full pipeline")
        stats = self.strategy.get_stats()
        self.assertGreaterEqual(stats['positions_opened'], 1)

    def test_full_pipeline_buy_put(self):
        _setup_spring_env(self.strategy, 'IF2606-P-4000')
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4015.0,
            option_instrument_id='IF2606-P-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "BUY_PUT signal must be generated in full pipeline")
        self.assertEqual(signal.direction, 'BUY_PUT')
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=-0.5)
        self.assertIsNotNone(triggered, "BUY_PUT trigger must fire in full pipeline")
        order_id = self.strategy.execute_spring_entry(triggered)
        self.assertIsNotNone(order_id, "BUY_PUT order must be placed in full pipeline")

    def test_full_pipeline_buy_straddle(self):
        _setup_spring_env(self.strategy)
        signal = self.strategy.detect_spring(
            instrument_id='IF2606',
            future_price=4000.0,
            option_instrument_id='IF2606-C-4000',
            strike_price=4000.0,
            iv=0.08,
            premium_price=50.0,
            days_to_expiry=3,
            account_equity=1000000.0,
        )
        self.assertIsNotNone(signal, "BUY_STRADDLE signal must be generated in full pipeline")
        self.assertEqual(signal.direction, 'BUY_STRADDLE')
        triggered = self.strategy.check_trigger('IF2606', order_flow_imbalance=0.5)
        self.assertIsNotNone(triggered, "BUY_STRADDLE trigger must fire in full pipeline")
        with patch.object(self.strategy, '_find_straddle_pair',
                          return_value=('IF2606-P-4000', 48.0, 'CALL')), \
             patch.object(self.order_svc.rate_limiter, 'acquire', return_value=True):
            order_id = self.strategy.execute_spring_entry(triggered)
        self.assertIsNotNone(order_id, "BUY_STRADDLE order must be placed in full pipeline")


class TestSpringHealthAndStats(unittest.TestCase):
    """弹簧策略：健康检查与统计"""

    def setUp(self):
        self.strategy = BoxSpringStrategy({})

    def test_health_status_initial(self):
        stats = self.strategy.get_stats()
        self.assertEqual(stats['active_positions'], 0)

    def test_stats_structure(self):
        stats = self.strategy.get_stats()
        self.assertEqual(stats['service_name'], 'BoxSpringStrategy')
        self.assertIn('total_signals', stats)
        self.assertIn('positions_opened', stats)
        self.assertIn('expected_value', stats)


class TestSpringSingletonFactory(unittest.TestCase):
    """弹簧策略：单例工厂"""

    def test_singleton_creation(self):
        _reset_box_spring_singleton()
        s1 = get_box_spring_strategy()
        s2 = get_box_spring_strategy()
        self.assertIs(s1, s2)
        _reset_box_spring_singleton()


if __name__ == '__main__':
    unittest.main()
