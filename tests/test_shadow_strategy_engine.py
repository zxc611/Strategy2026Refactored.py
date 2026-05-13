"""
test_shadow_strategy_engine.py - 影子策略监控引擎全面测试

验证V7次系统1功能完整性：
1. 影子A(反向逻辑)信号处理
2. 影子B(随机动作)信号处理
3. Alpha比率计算与衰减监控
4. 绝对期望值底线检查
5. 独立参数锁定
6. 交易独立日志
7. 每日/周汇总
8. 健康检查集成
9. 降级与暂停机制
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
from unittest.mock import patch, MagicMock

from ali2026v3_trading.shadow_strategy_engine import (
    ShadowStrategyEngine,
    ShadowTradeRecord,
    AlphaMetrics,
    ShadowParamsSnapshot,
    get_shadow_strategy_engine,
)


class TestShadowTradeRecord(unittest.TestCase):
    def test_create_and_to_dict(self):
        r = ShadowTradeRecord(
            trade_id="SHADOW-A-001",
            shadow_type="shadow_a",
            timestamp="2026-05-10T14:35:00",
            instrument_id="IO2506-C-4000",
            direction="short",
            price=2.35,
            quantity=10,
            open_reason="SHADOW_A_REVERSAL",
        )
        d = r.to_dict()
        self.assertEqual(d['trade_id'], "SHADOW-A-001")
        self.assertEqual(d['shadow_type'], "shadow_a")
        self.assertEqual(d['direction'], "short")
        self.assertTrue(d['is_open'])

    def test_default_values(self):
        r = ShadowTradeRecord(trade_id="T1", shadow_type="shadow_b", timestamp="now")
        self.assertEqual(r.pnl, 0.0)
        self.assertEqual(r.commission, 0.0)
        self.assertEqual(r.net_pnl, 0.0)
        self.assertTrue(r.is_open)


class TestAlphaMetrics(unittest.TestCase):
    def test_create_and_to_dict(self):
        m = AlphaMetrics(
            timestamp="2026-05-10T14:35:00",
            master_sharpe=1.5,
            shadow_a_sharpe=0.3,
            shadow_b_sharpe=0.1,
            alpha_ratio=1.2,
        )
        d = m.to_dict()
        self.assertAlmostEqual(d['alpha_ratio'], 1.2)
        self.assertFalse(d['degradation_triggered'])
        self.assertFalse(d['absolute_ev_breached'])


class TestShadowParamsSnapshot(unittest.TestCase):
    def test_create_and_to_dict(self):
        s = ShadowParamsSnapshot(
            shadow_type="shadow_a",
            locked_at="2026-05-10T14:35:00",
            param_set={"close_take_profit_ratio": 1.3},
            param_hash="abc123",
        )
        d = s.to_dict()
        self.assertEqual(d['shadow_type'], "shadow_a")
        self.assertIn('close_take_profit_ratio', d['param_set'])


class TestShadowStrategyEngineInit(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_init_creates_log_dir(self):
        log_dir = os.path.join(self.tmp_dir, 'shadow_logs')
        engine = ShadowStrategyEngine(log_dir=log_dir)
        self.assertTrue(os.path.isdir(log_dir))

    def test_params_locked_on_init(self):
        engine = ShadowStrategyEngine(log_dir=self.tmp_dir)
        self.assertTrue(engine.are_params_locked())

    def test_shadow_a_params_loaded(self):
        engine = ShadowStrategyEngine(log_dir=self.tmp_dir)
        params = engine.get_shadow_a_params()
        self.assertIn('close_take_profit_ratio', params)
        self.assertAlmostEqual(params.get('close_take_profit_ratio', 0), 1.3, places=1)

    def test_shadow_b_params_loaded(self):
        engine = ShadowStrategyEngine(log_dir=self.tmp_dir)
        params = engine.get_shadow_b_params()
        self.assertIn('close_take_profit_ratio', params)

    def test_params_snapshot_structure(self):
        engine = ShadowStrategyEngine(log_dir=self.tmp_dir)
        snapshot = engine.get_params_snapshot()
        self.assertIn('shadow_a', snapshot)
        self.assertIn('shadow_b', snapshot)
        self.assertTrue(snapshot['params_locked'])
        self.assertIn('param_hash', snapshot['shadow_a'])
        self.assertNotEqual(snapshot['shadow_a']['param_hash'], '')
        self.assertNotEqual(snapshot['shadow_b']['param_hash'], '')

    def test_param_hash_deterministic(self):
        engine = ShadowStrategyEngine(log_dir=self.tmp_dir)
        s1 = engine.get_params_snapshot()
        engine2 = ShadowStrategyEngine(log_dir=self.tmp_dir)
        s2 = engine2.get_params_snapshot()
        self.assertEqual(s1['shadow_a']['param_hash'], s2['shadow_a']['param_hash'])


class TestShadowASignal(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_reverses_long_to_short(self):
        record = self.engine.process_shadow_a_signal(
            market_state='correct_trending',
            instrument_id='IO2506-C-4000',
            signal_direction='long',
            price=2.35,
            quantity=10,
        )
        self.assertEqual(record.direction, 'short')
        self.assertEqual(record.shadow_type, 'shadow_a')
        self.assertEqual(record.open_reason, 'SHADOW_A_REVERSAL')

    def test_reverses_short_to_long(self):
        record = self.engine.process_shadow_a_signal(
            market_state='correct_trending',
            signal_direction='short',
        )
        self.assertEqual(record.direction, 'long')

    def test_reverses_buy_to_sell(self):
        record = self.engine.process_shadow_a_signal(
            market_state='correct_trending',
            signal_direction='buy',
        )
        self.assertEqual(record.direction, 'sell')

    def test_unknown_direction_kept(self):
        record = self.engine.process_shadow_a_signal(
            market_state='correct_trending',
            signal_direction='unknown_dir',
        )
        self.assertEqual(record.direction, 'unknown_dir')

    def test_trade_record_stored(self):
        self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        stats = self.engine.get_stats()
        self.assertEqual(stats['shadow_a_trades'], 1)

    def test_trade_id_unique(self):
        r1 = self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        r2 = self.engine.process_shadow_a_signal(market_state='test', signal_direction='short')
        self.assertNotEqual(r1.trade_id, r2.trade_id)


class TestShadowBSignal(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_random_direction_is_long_or_short(self):
        for _ in range(100):
            record = self.engine.process_shadow_b_signal(
                market_state='correct_trending',
                signal_direction='long',
            )
            self.assertIn(record.direction, ('long', 'short'))

    def test_shadow_b_type(self):
        record = self.engine.process_shadow_b_signal(market_state='test')
        self.assertEqual(record.shadow_type, 'shadow_b')
        self.assertEqual(record.open_reason, 'SHADOW_B_RANDOM')

    def test_randomness_distribution(self):
        longs = 0
        shorts = 0
        for _ in range(1000):
            record = self.engine.process_shadow_b_signal(market_state='test')
            if record.direction == 'long':
                longs += 1
            else:
                shorts += 1
        self.assertGreater(longs, 300)
        self.assertGreater(shorts, 300)

    def test_trade_record_stored(self):
        self.engine.process_shadow_b_signal(market_state='test')
        stats = self.engine.get_stats()
        self.assertEqual(stats['shadow_b_trades'], 1)


class TestMasterTradeRecording(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_record_master_trade(self):
        record = self.engine.record_master_trade(
            instrument_id='IO2506-C-4000',
            direction='long',
            price=2.35,
            quantity=10,
            pnl=150.0,
            open_reason='CORRECT_RESONANCE',
        )
        self.assertEqual(record.shadow_type, 'master')
        self.assertAlmostEqual(record.pnl, 150.0)

    def test_pnl_accumulation(self):
        self.engine.record_master_trade(pnl=100.0)
        self.engine.record_master_trade(pnl=-50.0)
        self.engine.record_master_trade(pnl=200.0)
        stats = self.engine.get_stats()
        self.assertAlmostEqual(stats['master_pnl_sum'], 250.0)


class TestShadowPnlRecording(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_record_shadow_a_pnl(self):
        r = self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        self.engine.record_shadow_pnl(
            shadow_type='shadow_a',
            trade_id=r.trade_id,
            pnl=100.0,
            close_reason='TAKE_PROFIT',
            commission=5.0,
        )
        stats = self.engine.get_stats()
        self.assertAlmostEqual(stats['shadow_a_pnl_sum'], 95.0)

    def test_record_shadow_b_pnl(self):
        r = self.engine.process_shadow_b_signal(market_state='test')
        self.engine.record_shadow_pnl(
            shadow_type='shadow_b',
            trade_id=r.trade_id,
            pnl=-50.0,
            close_reason='STOP_LOSS',
            commission=5.0,
        )
        stats = self.engine.get_stats()
        self.assertAlmostEqual(stats['shadow_b_pnl_sum'], -55.0)


class TestAlphaMetricsComputation(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_compute_with_no_data(self):
        metrics = self.engine.compute_alpha_metrics()
        self.assertAlmostEqual(metrics.alpha_ratio, 0.0)
        self.assertFalse(metrics.degradation_triggered)
        self.assertFalse(metrics.absolute_ev_breached)

    def test_compute_with_equity_curves(self):
        for i in range(20):
            self.engine.update_equity_curves(
                master_equity=100000 + i * 100,
                shadow_a_equity=100000 + i * 20,
                shadow_b_equity=100000 + i * 5,
            )
        metrics = self.engine.compute_alpha_metrics()
        self.assertGreater(metrics.master_sharpe, 0.0)
        self.assertNotEqual(metrics.alpha_ratio, 0.0)

    def test_alpha_decline_detection(self):
        for i in range(20):
            self.engine.update_equity_curves(
                master_equity=100000 + i * 100,
                shadow_a_equity=100000,
                shadow_b_equity=100000,
            )
        self.engine.compute_alpha_metrics()

        for i in range(20):
            self.engine.update_equity_curves(
                master_equity=100000 + 1900 - i * 50,
                shadow_a_equity=100000 + i * 100,
                shadow_b_equity=100000,
            )
        metrics = self.engine.compute_alpha_metrics()
        self.assertIsInstance(metrics.consecutive_decline_windows, int)

    def test_absolute_ev_breach(self):
        for _ in range(10):
            self.engine.record_master_trade(pnl=-100.0)
        for _ in range(20):
            self.engine.update_equity_curves(
                master_equity=100000,
                shadow_a_equity=100000,
                shadow_b_equity=100000,
            )
        metrics = self.engine.compute_alpha_metrics()
        self.assertLess(metrics.master_expected_value, 0.0)


class TestDegradationAndPause(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_initial_no_degradation(self):
        self.assertFalse(self.engine.is_degradation_active())

    def test_initial_no_pause(self):
        self.assertFalse(self.engine.is_absolute_ev_paused())

    def test_clear_degradation(self):
        self.engine._degradation_active = True
        self.assertTrue(self.engine.is_degradation_active())
        self.engine.clear_degradation()
        self.assertFalse(self.engine.is_degradation_active())

    def test_clear_absolute_ev_pause(self):
        self.engine._absolute_ev_pause = True
        self.assertTrue(self.engine.is_absolute_ev_paused())
        self.engine.clear_absolute_ev_pause()
        self.assertFalse(self.engine.is_absolute_ev_paused())


class TestProcessSignal(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_process_signal_returns_both(self):
        result = self.engine.process_signal(
            market_state='correct_trending',
            instrument_id='IO2506-C-4000',
            signal_direction='long',
            price=2.35,
            quantity=10,
        )
        self.assertIn('shadow_a', result)
        self.assertIn('shadow_b', result)
        self.assertEqual(result['shadow_a'].shadow_type, 'shadow_a')
        self.assertEqual(result['shadow_b'].shadow_type, 'shadow_b')
        self.assertEqual(result['shadow_a'].direction, 'short')
        self.assertIn(result['shadow_b'].direction, ('long', 'short'))


class TestParamIndependence(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_shadow_a_params_are_independent_copy(self):
        params_a = self.engine.get_shadow_a_params()
        params_a['close_take_profit_ratio'] = 999.0
        params_a_again = self.engine.get_shadow_a_params()
        self.assertNotAlmostEqual(params_a_again['close_take_profit_ratio'], 999.0)

    def test_params_do_not_change_after_relock(self):
        original_a = self.engine.get_shadow_a_params()
        original_hash = self.engine.get_params_snapshot()['shadow_a']['param_hash']
        self.engine.relock_params()
        new_hash = self.engine.get_params_snapshot()['shadow_a']['param_hash']
        self.assertEqual(original_hash, new_hash)


class TestTradeLog(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_trade_log_file_created(self):
        self.engine.process_shadow_a_signal(
            market_state='test',
            signal_direction='long',
        )
        self.engine._async_flush_log_queue()
        import time as _time
        _time.sleep(0.3)
        log_files = [f for f in os.listdir(self.tmp_dir) if f.startswith('shadow_') and f.endswith('.jsonl')]
        self.assertGreater(len(log_files), 0)

    def test_trade_log_is_valid_jsonl(self):
        self.engine.process_shadow_a_signal(
            market_state='test',
            signal_direction='long',
            instrument_id='IO2506-C-4000',
        )
        self.engine._async_flush_log_queue()
        import time as _time
        _time.sleep(0.3)
        log_files = [f for f in os.listdir(self.tmp_dir) if f.startswith('shadow_shadow_a') and f.endswith('.jsonl')]
        self.assertGreater(len(log_files), 0)
        with open(os.path.join(self.tmp_dir, log_files[0]), 'r') as f:
            line = f.readline().strip()
            data = json.loads(line)
            self.assertIn('trade_id', data)
            self.assertEqual(data['shadow_type'], 'shadow_a')


class TestDailySummary(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_daily_summary_structure(self):
        self.engine.record_master_trade(pnl=100.0)
        self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        self.engine.process_shadow_b_signal(market_state='test')
        summary = self.engine.generate_daily_summary()
        self.assertIn('summary_date', summary)
        self.assertIn('alpha_metrics', summary)
        self.assertIn('shadow_a_stats', summary)
        self.assertIn('shadow_b_stats', summary)
        self.assertIn('master_stats', summary)
        self.assertTrue(summary['params_locked'])

    def test_daily_summary_file_written(self):
        self.engine.generate_daily_summary()
        summary_files = [f for f in os.listdir(self.tmp_dir) if f.startswith('summary_') and f.endswith('.json')]
        self.assertGreater(len(summary_files), 0)


class TestWeeklySummary(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_weekly_summary_structure(self):
        for i in range(5):
            self.engine.update_equity_curves(
                master_equity=100000 + i * 100,
                shadow_a_equity=100000,
                shadow_b_equity=100000,
            )
            self.engine.compute_alpha_metrics()
        summary = self.engine.generate_weekly_summary()
        self.assertIn('week_start', summary)
        self.assertIn('alpha_ratio_stats', summary)


class TestHealthStatus(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_health_status_structure(self):
        health = self.engine.get_health_status()
        self.assertEqual(health['component'], 'shadow_strategy_engine')
        self.assertEqual(health['status'], 'OK')
        self.assertTrue(health['params_locked'])
        self.assertFalse(health['degradation_active'])
        self.assertFalse(health['absolute_ev_pause'])

    def test_health_status_critical_on_ev_breach(self):
        self.engine._absolute_ev_pause = True
        health = self.engine.get_health_status()
        self.assertEqual(health['status'], 'CRITICAL')


class TestStats(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_stats_structure(self):
        stats = self.engine.get_stats()
        self.assertIn('shadow_a_trades', stats)
        self.assertIn('shadow_b_trades', stats)
        self.assertIn('master_trades', stats)
        self.assertIn('alpha_calculations', stats)
        self.assertIn('degradation_events', stats)
        self.assertIn('absolute_ev_breaches', stats)

    def test_stats_increment(self):
        self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        self.engine.process_shadow_b_signal(market_state='test')
        self.engine.record_master_trade(pnl=50.0)
        stats = self.engine.get_stats()
        self.assertEqual(stats['shadow_a_trades'], 1)
        self.assertEqual(stats['shadow_b_trades'], 1)
        self.assertEqual(stats['master_trades'], 1)


class TestRecentTrades(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_get_recent_trades_all(self):
        self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        self.engine.process_shadow_b_signal(market_state='test')
        self.engine.record_master_trade(pnl=50.0)
        trades = self.engine.get_recent_trades()
        self.assertEqual(len(trades), 3)

    def test_get_recent_trades_by_type(self):
        self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        self.engine.process_shadow_b_signal(market_state='test')
        trades_a = self.engine.get_recent_trades(shadow_type='shadow_a')
        self.assertEqual(len(trades_a), 1)
        self.assertEqual(trades_a[0]['shadow_type'], 'shadow_a')

    def test_get_recent_trades_limit(self):
        for _ in range(10):
            self.engine.process_shadow_a_signal(market_state='test', signal_direction='long')
        trades = self.engine.get_recent_trades(limit=5)
        self.assertEqual(len(trades), 5)


class TestEquityCurveAndSharpe(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_update_equity_curves(self):
        self.engine.update_equity_curves(100000, 100000, 100000)
        self.engine.update_equity_curves(100100, 100050, 100025)
        self.assertEqual(len(self.engine._master_equity_curve), 2)

    def test_sharpe_computation(self):
        returns = [0.01, 0.02, -0.005, 0.015, 0.008]
        sharpe = ShadowStrategyEngine._compute_sharpe(returns)
        self.assertIsInstance(sharpe, float)
        self.assertGreater(sharpe, 0.0)

    def test_sharpe_zero_returns(self):
        sharpe = ShadowStrategyEngine._compute_sharpe([])
        self.assertAlmostEqual(sharpe, 0.0)

    def test_sharpe_single_return(self):
        sharpe = ShadowStrategyEngine._compute_sharpe([0.01])
        self.assertAlmostEqual(sharpe, 0.0)

    def test_max_drawdown(self):
        curve = [100, 110, 105, 115, 100, 120]
        dd = ShadowStrategyEngine._compute_max_drawdown(curve)
        self.assertGreater(dd, 0.0)
        self.assertLessEqual(dd, 1.0)

    def test_max_drawdown_monotonic_up(self):
        curve = [100, 110, 120, 130]
        dd = ShadowStrategyEngine._compute_max_drawdown(curve)
        self.assertAlmostEqual(dd, 0.0)

    def test_equity_to_returns(self):
        curve = [100, 110, 105]
        returns = ShadowStrategyEngine._equity_to_returns(curve)
        self.assertAlmostEqual(returns[0], 0.1)
        self.assertAlmostEqual(returns[1], -5.0 / 110.0)

    def test_expected_value(self):
        r1 = ShadowTradeRecord(trade_id="T1", shadow_type="shadow_a", timestamp="now", is_open=False, net_pnl=100.0)
        r2 = ShadowTradeRecord(trade_id="T2", shadow_type="shadow_a", timestamp="now", is_open=False, net_pnl=-50.0)
        r3 = ShadowTradeRecord(trade_id="T3", shadow_type="shadow_a", timestamp="now", is_open=True, net_pnl=200.0)
        trades = deque([r1, r2, r3])
        ev = ShadowStrategyEngine._compute_expected_value(trades)
        self.assertAlmostEqual(ev, 25.0)


class TestSingletonFactory(unittest.TestCase):
    def test_get_shadow_strategy_engine_singleton(self):
        import ali2026v3_trading.shadow_strategy_engine as mod
        mod._shadow_engine = None
        e1 = get_shadow_strategy_engine()
        e2 = get_shadow_strategy_engine()
        self.assertIs(e1, e2)
        mod._shadow_engine = None


class TestThreadSafety(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.engine = ShadowStrategyEngine(log_dir=self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_concurrent_signal_processing(self):
        errors = []
        def worker():
            try:
                for _ in range(50):
                    self.engine.process_signal(
                        market_state='test',
                        signal_direction='long',
                        price=2.35,
                        quantity=10,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        stats = self.engine.get_stats()
        self.assertEqual(stats['shadow_a_trades'], 200)
        self.assertEqual(stats['shadow_b_trades'], 200)


if __name__ == '__main__':
    unittest.main()
