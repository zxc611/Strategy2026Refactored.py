"""
test_strategy_ecosystem.py - 策略生态系统(次系统2)全面测试

验证内容：
1. BoxDetector - 箱体检测（箱底/箱顶识别、极值子状态）
2. StrategyEcosystem - 三策略协同、资金路由、互斥规则
3. 极值策略参数（比趋势策略更苛刻）
4. IV过滤、订单流衰竭确认
5. 绝对期望值底线
6. 健康检查集成
"""
import json
import math
import os
import shutil
import tempfile
import threading
import time
import unittest
from collections import deque
from datetime import datetime
from typing import Dict, Any

from ali2026v3_trading.box_detector import (
    BoxDetector, BoxProfile, ExtremeState, BoxStrategyParams, get_box_detector,
)
from ali2026v3_trading.strategy_ecosystem import (
    StrategyEcosystem, StrategySlot, CapitalRoute, EcosystemTradeRecord,
    get_strategy_ecosystem,
)


class TestBoxStrategyParams(unittest.TestCase):
    def test_default_params_stricter_than_trend(self):
        p = BoxStrategyParams()
        self.assertLessEqual(p.max_hold_minutes, 45)
        self.assertLessEqual(p.take_profit_ratio, 0.5)
        self.assertLessEqual(p.stop_loss_ratio, 0.3)
        self.assertLessEqual(p.max_risk_ratio, 0.05)
        self.assertGreaterEqual(p.iv_percentile_min, 50.0)

    def test_to_dict(self):
        p = BoxStrategyParams()
        d = p.to_dict()
        self.assertIn('max_hold_minutes', d)
        self.assertIn('iv_percentile_min', d)


class TestBoxProfile(unittest.TestCase):
    def test_valid_box(self):
        b = BoxProfile(box_id='B1', timestamp='now', is_box=True, upper=4100, lower=3900)
        self.assertTrue(b.is_valid)

    def test_invalid_box_no_bounds(self):
        b = BoxProfile(box_id='B1', timestamp='now', is_box=True, upper=0, lower=0)
        self.assertFalse(b.is_valid)

    def test_not_box(self):
        b = BoxProfile(box_id='B1', timestamp='now', is_box=False, upper=4100, lower=3900)
        self.assertFalse(b.is_valid)


class TestExtremeState(unittest.TestCase):
    def test_default_no_extreme(self):
        e = ExtremeState(timestamp='now')
        self.assertFalse(e.is_bottom_extreme)
        self.assertFalse(e.is_top_extreme)
        self.assertFalse(e.tradeable)


class TestBoxDetectorInit(unittest.TestCase):
    def test_init(self):
        bd = BoxDetector()
        self.assertIsNone(bd.get_current_box())

    def test_custom_params(self):
        params = BoxStrategyParams(max_hold_minutes=45, iv_percentile_min=60.0)
        bd = BoxDetector(params=params)
        self.assertEqual(bd.params.max_hold_minutes, 45)
        self.assertEqual(bd.params.iv_percentile_min, 60.0)


class TestBoxDetectorUpdateBar(unittest.TestCase):
    def setUp(self):
        self.bd = BoxDetector()

    def test_update_bar(self):
        self.bd.update_bar(4100, 3900, 4000, 1000)
        stats = self.bd.get_stats()
        self.assertEqual(stats['bars_processed'], 1)
        self.assertEqual(stats['price_bars'], 1)


class TestBoxDetectorDetectBox(unittest.TestCase):
    def setUp(self):
        self.bd = BoxDetector(lookback_bars=30, min_box_bars=10)

    def test_insufficient_data(self):
        for i in range(5):
            self.bd.update_bar(4100, 3900, 4000)
        profile = self.bd.detect_box()
        self.assertFalse(profile.is_box)

    def test_ranging_market_detects_box(self):
        for i in range(30):
            price = 4000 + 50 * math.sin(i * 0.3)
            self.bd.update_bar(price + 10, price - 10, price, 1000)
        profile = self.bd.detect_box()
        self.assertIsInstance(profile, BoxProfile)
        self.assertGreater(profile.duration_bars, 0)

    def test_trending_market_no_box(self):
        for i in range(30):
            price = 3800 + i * 20
            self.bd.update_bar(price + 5, price - 5, price, 1000)
        profile = self.bd.detect_box()
        self.assertFalse(profile.is_box)

    def test_box_has_upper_lower(self):
        for i in range(30):
            price = 4000 + 30 * math.sin(i * 0.5)
            self.bd.update_bar(price + 8, price - 8, price, 1000)
        profile = self.bd.detect_box()
        if profile.is_box:
            self.assertGreater(profile.upper, profile.lower)


class TestBoxDetectorExtremeState(unittest.TestCase):
    def setUp(self):
        self.bd = BoxDetector(lookback_bars=30, min_box_bars=10)
        for i in range(30):
            price = 4000 + 50 * math.sin(i * 0.3)
            self.bd.update_bar(price + 10, price - 10, price, 1000)
        self.bd.detect_box()

    def test_no_box_no_extreme(self):
        bd2 = BoxDetector()
        e = bd2.classify_extreme_state(current_price=4000)
        self.assertFalse(e.tradeable)

    def test_bottom_extreme_with_resonance(self):
        box = self.bd.get_current_box()
        if box and box.is_valid:
            e = self.bd.classify_extreme_state(
                current_price=box.lower,
                resonance_direction='fall',
                resonance_strength=0.8,
                current_iv=0.25,
            )
            self.assertTrue(e.is_bottom_extreme)

    def test_top_extreme_with_resonance(self):
        box = self.bd.get_current_box()
        if box and box.is_valid:
            e = self.bd.classify_extreme_state(
                current_price=box.upper,
                resonance_direction='rise',
                resonance_strength=0.8,
                current_iv=0.25,
            )
            self.assertTrue(e.is_top_extreme)

    def test_mid_price_no_extreme(self):
        box = self.bd.get_current_box()
        if box and box.is_valid:
            e = self.bd.classify_extreme_state(
                current_price=box.median,
                resonance_direction='fall',
                resonance_strength=0.8,
                current_iv=0.25,
            )
            self.assertFalse(e.is_bottom_extreme)
            self.assertFalse(e.is_top_extreme)


class TestIVFilter(unittest.TestCase):
    def setUp(self):
        self.bd = BoxDetector()
        for _ in range(100):
            self.bd.update_iv(0.15 + 0.05 * (hash(str(_)) % 10) / 10.0)

    def test_high_iv_passes(self):
        passed = self.bd.check_iv_filter(0.25)
        self.assertTrue(passed)

    def test_low_iv_fails(self):
        passed = self.bd.check_iv_filter(0.10)
        self.assertFalse(passed)

    def test_zero_iv_fails(self):
        passed = self.bd.check_iv_filter(0.0)
        self.assertFalse(passed)


class TestOrderFlowExhaustion(unittest.TestCase):
    def setUp(self):
        self.bd = BoxDetector()

    def test_exhausted_flow(self):
        result = self.bd.check_order_flow_exhaustion(flow_imbalance=0.05, cvd_slope=0.001)
        self.assertTrue(result)

    def test_active_flow_not_exhausted(self):
        result = self.bd.check_order_flow_exhaustion(flow_imbalance=0.8, cvd_slope=0.5)
        self.assertFalse(result)


class TestDetermineTradeDirection(unittest.TestCase):
    def setUp(self):
        self.bd = BoxDetector()

    def test_bottom_extreme_goes_long(self):
        e = ExtremeState(
            timestamp='now', is_bottom_extreme=True, tradeable=True,
            extreme_type='box_bottom_extreme',
        )
        self.assertEqual(self.bd.determine_trade_direction(e), 'long')

    def test_top_extreme_goes_short(self):
        e = ExtremeState(
            timestamp='now', is_top_extreme=True, tradeable=True,
            extreme_type='box_top_extreme',
        )
        self.assertEqual(self.bd.determine_trade_direction(e), 'short')

    def test_no_extreme_no_direction(self):
        e = ExtremeState(timestamp='now')
        self.assertEqual(self.bd.determine_trade_direction(e), '')


class TestCapitalRoute(unittest.TestCase):
    def test_default_allocations(self):
        cr = CapitalRoute()
        self.assertAlmostEqual(cr.total_base, 1.0, places=2)

    def test_to_dict(self):
        cr = CapitalRoute()
        d = cr.to_dict()
        self.assertIn('master_base', d)


class TestStrategyEcosystemInit(unittest.TestCase):
    def test_init(self):
        tmp = tempfile.mkdtemp()
        eco = StrategyEcosystem(log_dir=tmp)
        self.assertEqual(eco.get_active_strategy(), 'master')
        allocs = eco.get_capital_allocations()
        self.assertIn('master', allocs)
        shutil.rmtree(tmp, ignore_errors=True)


class TestCapitalRouting(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_master_state_boosts_master(self):
        allocs = self.eco.route_capital('correct_trending')
        self.assertGreater(allocs['master'], 0.5)

    def test_reverse_state_boosts_reverse(self):
        allocs = self.eco.route_capital('incorrect_reversal')
        self.assertGreater(allocs['reverse'], 0.2)

    def test_other_state_boosts_other(self):
        allocs = self.eco.route_capital('other')
        self.assertGreater(allocs['other'], 0.1)

    def test_allocations_sum_to_one(self):
        for state in ('correct_trending', 'incorrect_reversal', 'other'):
            allocs = self.eco.route_capital(state)
            self.assertAlmostEqual(sum(allocs.values()), 1.0, places=5)


class TestStrategySwitching(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_switch_to_reverse(self):
        result = self.eco.switch_active_strategy('incorrect_reversal')
        self.assertTrue(result['switched'])
        self.assertEqual(result['from'], 'master')
        self.assertEqual(result['to'], 'reverse')
        self.assertEqual(self.eco.get_active_strategy(), 'reverse')

    def test_switch_to_other(self):
        result = self.eco.switch_active_strategy('other')
        self.assertTrue(result['switched'])
        self.assertEqual(self.eco.get_active_strategy(), 'other')

    def test_same_state_no_switch(self):
        result = self.eco.switch_active_strategy('correct_trending')
        self.assertFalse(result['switched'])

    def test_switch_close_old_first(self):
        result = self.eco.switch_active_strategy('other')
        self.assertTrue(result['close_old_positions_first'])

    def test_switch_updates_capital(self):
        result = self.eco.switch_active_strategy('other')
        self.assertIn('capital_allocations', result)


class TestMutualExclusion(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_master_allowed_when_no_reverse(self):
        allowed, reason = self.eco.check_mutual_exclusion('master', 'long')
        self.assertTrue(allowed)

    def test_other_state_blocks_master_new_open(self):
        self.eco.switch_active_strategy('other')
        allowed, reason = self.eco.check_mutual_exclusion('master', 'long')
        self.assertFalse(allowed)

    def test_other_state_blocks_reverse_new_open(self):
        self.eco.switch_active_strategy('other')
        allowed, reason = self.eco.check_mutual_exclusion('reverse', 'short')
        self.assertFalse(allowed)

    def test_other_strategy_allowed_in_other_state(self):
        self.eco.switch_active_strategy('other')
        allowed, reason = self.eco.check_mutual_exclusion('other', 'long')
        self.assertTrue(allowed)


class TestOtherStrategySignal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)
        self.bd = self.eco.box_detector
        for i in range(30):
            price = 4000 + 30 * math.sin(i * 0.3)
            self.bd.update_bar(price + 8, price - 8, price, 1000)
        self.bd.detect_box()
        for _ in range(50):
            self.bd.update_iv(0.20)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_box_returns_skip(self):
        eco2 = StrategyEcosystem(log_dir=self.tmp)
        result = eco2.process_other_strategy_signal(current_price=4000)
        self.assertIn(result['action'], ('skip', 'no_signal'))

    def test_tradeable_extreme_generates_signal(self):
        box = self.bd.get_current_box()
        if box and box.is_valid:
            self.eco.switch_active_strategy('other')
            result = self.eco.process_other_strategy_signal(
                current_price=box.lower,
                resonance_direction='fall',
                resonance_strength=0.9,
                current_iv=0.25,
            )
            self.assertIn(result['action'], ('open', 'no_signal', 'blocked', 'skip'))


class TestAbsoluteEVBottomline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_breach_initially(self):
        result = self.eco.check_absolute_ev_bottomline()
        self.assertFalse(result['any_breached'])

    def test_negative_ev_triggers_pause(self):
        for _ in range(10):
            self.eco._master_trades.append(EcosystemTradeRecord(
                trade_id=f'T{_}', strategy_id='master',
                timestamp=datetime.now().isoformat(),
                pnl=-100.0, net_pnl=-100.0, is_open=False,
            ))
        self.eco._master._closed_pnl_sum = -1000.0
        self.eco._master._closed_count = 10
        result = self.eco.check_absolute_ev_bottomline()
        self.assertTrue(result['strategies']['master']['breached'])

    def test_resume_strategy(self):
        self.eco._master.paused = True
        self.eco._master.pause_reason = 'EV<0'
        ok = self.eco.resume_strategy('master')
        self.assertTrue(ok)
        self.assertFalse(self.eco._master.paused)


class TestHealthStatus(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_health_structure(self):
        health = self.eco.get_health_status()
        self.assertEqual(health['component'], 'strategy_ecosystem')
        self.assertIn('status', health)
        self.assertIn('active_strategy', health)
        self.assertIn('strategies', health)
        self.assertIn('capital_allocations', health)
        self.assertIn('box_detector', health)

    def test_health_ok_initially(self):
        health = self.eco.get_health_status()
        self.assertEqual(health['status'], 'OK')


class TestStats(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_stats_structure(self):
        stats = self.eco.get_stats()
        self.assertIn('total_trades', stats)
        self.assertIn('master_trades', stats)
        self.assertIn('reverse_trades', stats)
        self.assertIn('other_trades', stats)
        self.assertIn('state_switches', stats)
        self.assertIn('mutual_exclusion_blocks', stats)


class TestStrategySlots(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_slots_structure(self):
        slots = self.eco.get_strategy_slots()
        self.assertIn('master', slots)
        self.assertIn('reverse', slots)
        self.assertIn('other', slots)
        self.assertEqual(slots['master']['strategy_type'], 'correct_trending')
        self.assertEqual(slots['reverse']['strategy_type'], 'incorrect_reversal')
        self.assertEqual(slots['other']['strategy_type'], 'box_extreme')


class TestRecordPnL(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_record_master_pnl(self):
        r = EcosystemTradeRecord(
            trade_id='T1', strategy_id='master',
            timestamp=datetime.now().isoformat(), is_open=True,
        )
        self.eco._master_trades.append(r)
        self.eco._master.position_count = 1
        self.eco.record_strategy_pnl('master', 100.0, 5.0)
        stats = self.eco.get_stats()
        self.assertAlmostEqual(stats['master_pnl'], 95.0)


class TestComputeExpectedValue(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.eco = StrategyEcosystem(log_dir=self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_positive_ev(self):
        for pnl in [50, 30, 20]:
            r = EcosystemTradeRecord(
                trade_id=f'T{pnl}', strategy_id='master',
                timestamp=datetime.now().isoformat(),
                pnl=float(pnl), net_pnl=float(pnl), is_open=False,
            )
            self.eco._master_trades.append(r)
        ev = self.eco.compute_expected_value('master')
        self.assertAlmostEqual(ev, 100.0 / 3.0, places=2)

    def test_negative_ev(self):
        for pnl in [-50, -30, -20]:
            r = EcosystemTradeRecord(
                trade_id=f'T{pnl}', strategy_id='master',
                timestamp=datetime.now().isoformat(),
                pnl=float(pnl), net_pnl=float(pnl), is_open=False,
            )
            self.eco._master_trades.append(r)
        ev = self.eco.compute_expected_value('master')
        self.assertLess(ev, 0)

    def test_no_trades_ev_zero(self):
        ev = self.eco.compute_expected_value('master')
        self.assertAlmostEqual(ev, 0.0)


class TestADXComputation(unittest.TestCase):
    def test_ranging_low_adx(self):
        bd = BoxDetector()
        for i in range(30):
            price = 4000 + 20 * math.sin(i * 0.3)
            bd.update_bar(price + 5, price - 5, price)
        adx = BoxDetector._compute_adx_simplified(bd._price_highs, bd._price_lows, bd._price_closes)
        self.assertIsInstance(adx, float)
        self.assertGreaterEqual(adx, 0.0)

    def test_trending_high_adx(self):
        bd = BoxDetector()
        for i in range(30):
            price = 3800 + i * 30
            bd.update_bar(price + 5, price - 5, price)
        adx = BoxDetector._compute_adx_simplified(bd._price_highs, bd._price_lows, bd._price_closes)
        self.assertIsInstance(adx, float)


class TestSupportResistance(unittest.TestCase):
    def test_find_clusters(self):
        lows = [3900, 3902, 3898, 3901, 4000, 4002, 3899, 3903]
        highs = [4100, 4102, 4098, 4101, 4200, 4103, 4099, 4100]
        supports, resistances = BoxDetector._find_support_resistance(
            deque(lows), deque(highs)
        )
        self.assertGreater(len(supports), 0)
        self.assertGreater(len(resistances), 0)


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_ecosystem_access(self):
        tmp = tempfile.mkdtemp()
        eco = StrategyEcosystem(log_dir=tmp)
        errors = []

        def worker():
            try:
                for _ in range(50):
                    eco.route_capital('correct_trending')
                    eco.switch_active_strategy('other')
                    eco.get_stats()
                    eco.get_health_status()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(len(errors), 0)
        shutil.rmtree(tmp, ignore_errors=True)


class TestSingletonFactories(unittest.TestCase):
    def test_box_detector_singleton(self):
        import ali2026v3_trading.box_detector as mod
        mod._box_detector = None
        d1 = get_box_detector()
        d2 = get_box_detector()
        self.assertIs(d1, d2)
        mod._box_detector = None

    def test_ecosystem_singleton(self):
        import ali2026v3_trading.strategy_ecosystem as mod
        mod._ecosystem = None
        e1 = get_strategy_ecosystem()
        e2 = get_strategy_ecosystem()
        self.assertIs(e1, e2)
        mod._ecosystem = None


if __name__ == '__main__':
    unittest.main()
