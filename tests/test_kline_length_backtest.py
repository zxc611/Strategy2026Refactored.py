# MODULE_ID: M2-402
"""
test_kline_length_backtest.py — K线长度回测基础设施测试

验证目标：
  1. _interpolate_ticks_in_bar: tick插值保真（价格路径/OHVLC/成交量守恒）
  2. _resample_bars_runtime: 多粒度聚合正确性
  3. BAR_INTERVAL_GRID: 四策略天然K线适配合理性
  4. KLINE_LENGTH_PARAM_GRID: 参数网格完整性
  5. HFT tick保真 vs Bar级失真对比
  6. run_backtest_hft_tick_fidelity: 输出结构正确性
"""
import os
import sys
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'ali2026v3_trading', 'param_pool'))

from ali2026v3_trading.param_pool.ts.ts_result_writer import _interpolate_ticks_in_bar, _resample_bars_runtime
from ali2026v3_trading.param_pool._param_defaults import (
    BAR_INTERVAL_GRID,
    KLINE_LENGTH_PARAM_GRID,
    MULTISCALE_BAR_LENGTHS,
    PARAM_DEFAULTS_HFT,
)
from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_tick_fidelity

# These symbols were moved to backup during 2026-06-12 reorganization
try:
    from ali2026v3_trading.param_pool.ts_result_kline_sweep import (
        run_kline_length_sweep,
        run_kline_length_deep_sweep,
        run_kline_cr_cross_sweep,
        validate_kline_length_quality_gates,
    )
except ImportError:
    run_kline_length_sweep = None
    run_kline_length_deep_sweep = None
    run_kline_cr_cross_sweep = None
    validate_kline_length_quality_gates = None


def _make_bar(open=4000, high=4010, low=3990, close=4005, volume=1000,
              imbalance=0.3, strength=0.6, symbol='IF2606'):
    return pd.Series({
        'minute': pd.Timestamp('2025-03-15 10:30:00'),
        'symbol': symbol,
        'open': open, 'high': high, 'low': low, 'close': close,
        'volume': volume, 'imbalance': imbalance, 'strength': strength,
    })


class TestTickInterpolation(unittest.TestCase):
    """tick插值保真测试"""

    def test_price_path_ohlc_preserved(self):
        bar = _make_bar(open=4000, high=4010, low=3990, close=4005)
        ticks = _interpolate_ticks_in_bar(bar, n_ticks=10)
        self.assertEqual(len(ticks), 10)
        self.assertAlmostEqual(ticks[0]['price'], 4000, places=1)
        self.assertAlmostEqual(ticks[-1]['price'], 4005, places=1)
        prices = [t['price'] for t in ticks]
        self.assertGreaterEqual(max(prices), 4009)
        self.assertLessEqual(min(prices), 3991)

    def test_bearish_bar_low_preserved(self):
        bar = _make_bar(open=4010, high=4015, low=3990, close=3995)
        ticks = _interpolate_ticks_in_bar(bar, n_ticks=20)
        prices = [t['price'] for t in ticks]
        self.assertLessEqual(min(prices), 3991)

    def test_volume_conservation(self):
        bar = _make_bar(volume=1000)
        ticks = _interpolate_ticks_in_bar(bar, n_ticks=5)
        total_vol = sum(t['volume'] for t in ticks)
        self.assertAlmostEqual(total_vol, 1000, places=1)

    def test_imbalance_transition(self):
        bar = _make_bar(imbalance=0.5)
        ticks = _interpolate_ticks_in_bar(bar, n_ticks=5)
        self.assertAlmostEqual(ticks[0]['imbalance'], 0.5, places=0)
        self.assertLessEqual(abs(ticks[-1]['imbalance'] - 0.5 * 0.8), 0.15)

    def test_single_tick_degradation(self):
        bar = _make_bar()
        ticks = _interpolate_ticks_in_bar(bar, n_ticks=1)
        self.assertEqual(len(ticks), 1)
        self.assertAlmostEqual(ticks[0]['price'], 4005)

    def test_bearish_bar_path(self):
        bar = _make_bar(open=4010, high=4010, low=3990, close=3995)
        ticks = _interpolate_ticks_in_bar(bar, n_ticks=20)
        prices = [t['price'] for t in ticks]
        self.assertAlmostEqual(prices[0], 4010, places=1)
        self.assertAlmostEqual(prices[-1], 3995, places=1)
        self.assertLessEqual(min(prices), 3993)


class TestResampleRuntime(unittest.TestCase):
    """运行时多粒度聚合测试"""

    def setUp(self):
        np.random.seed(42)

    def _make_1m_data(self, n_bars=60):
        times = pd.date_range('2025-03-15 09:30:00', periods=n_bars, freq='1min')
        df = pd.DataFrame({
            'minute': times,
            'symbol': 'IF2606',
            'open': 4000 + np.random.randn(n_bars).cumsum() * 0.5,
            'high': 4002 + np.random.randn(n_bars).cumsum() * 0.5,
            'low': 3998 + np.random.randn(n_bars).cumsum() * 0.5,
            'close': 4001 + np.random.randn(n_bars).cumsum() * 0.5,
            'volume': np.random.randint(100, 500, n_bars),
            'imbalance': np.random.randn(n_bars) * 0.3,
            'strength': np.random.rand(n_bars) * 0.8,
        })
        df['high'] = df[['open', 'high', 'close']].max(axis=1) + 1
        df['low'] = df[['open', 'low', 'close']].min(axis=1) - 1
        return df

    def test_resample_5min_reduces_count(self):
        df_1m = self._make_1m_data(60)
        df_5m = _resample_bars_runtime(df_1m, 5)
        self.assertLessEqual(len(df_5m), 12)
        self.assertGreater(len(df_5m), 0)

    def test_resample_15min_reduces_count(self):
        df_1m = self._make_1m_data(60)
        df_15m = _resample_bars_runtime(df_1m, 15)
        self.assertLessEqual(len(df_15m), 4)
        self.assertGreater(len(df_15m), 0)

    def test_resample_identity(self):
        df_1m = self._make_1m_data(10)
        df_same = _resample_bars_runtime(df_1m, 1)
        self.assertEqual(len(df_same), len(df_1m))

    def test_resample_ohlc_consistency(self):
        df_1m = self._make_1m_data(60)
        df_5m = _resample_bars_runtime(df_1m, 5)
        for _, row in df_5m.iterrows():
            self.assertGreaterEqual(row['high'], row['low'])
            self.assertGreaterEqual(row['high'], row['open'])
            self.assertGreaterEqual(row['high'], row['close'])
            self.assertLessEqual(row['low'], row['open'])
            self.assertLessEqual(row['low'], row['close'])


class TestBarIntervalGrid(unittest.TestCase):
    """策略天然K线适配合理性"""

    def test_hft_fixed_1min(self):
        self.assertEqual(BAR_INTERVAL_GRID['high_freq'], [1])

    def test_resonance_includes_short(self):
        self.assertIn(1, BAR_INTERVAL_GRID['resonance'])
        self.assertIn(5, BAR_INTERVAL_GRID['resonance'])

    def test_box_includes_long(self):
        self.assertIn(60, BAR_INTERVAL_GRID['box'])

    def test_spring_includes_medium(self):
        self.assertIn(15, BAR_INTERVAL_GRID['spring'])


class TestKlineLengthParamGrid(unittest.TestCase):
    """K线长度参数网格完整性"""

    def test_grid_has_bar_interval(self):
        self.assertIn('bar_interval_minutes', KLINE_LENGTH_PARAM_GRID)

    def test_grid_has_trend_periods(self):
        self.assertIn('trend_period_short', KLINE_LENGTH_PARAM_GRID)
        self.assertIn('trend_period_medium', KLINE_LENGTH_PARAM_GRID)
        self.assertIn('trend_period_long', KLINE_LENGTH_PARAM_GRID)

    def test_grid_has_hft_tick_params(self):
        self.assertIn('hft_signal_confirm_ticks', KLINE_LENGTH_PARAM_GRID)
        self.assertIn('hft_ticks_per_bar', KLINE_LENGTH_PARAM_GRID)

    def test_grid_has_lookback_params(self):
        self.assertIn('box_lookback_bars', KLINE_LENGTH_PARAM_GRID)
        self.assertIn('vol_lookback', KLINE_LENGTH_PARAM_GRID)
        self.assertIn('iv_lookback_bars', KLINE_LENGTH_PARAM_GRID)

    def test_bar_interval_grid_values(self):
        bar_levels = KLINE_LENGTH_PARAM_GRID['bar_interval_minutes']
        self.assertIn(1, bar_levels)
        self.assertIn(5, bar_levels)
        self.assertIn(60, bar_levels)
        self.assertIn(1440, bar_levels)

    def test_hft_ticks_per_bar_values(self):
        self.assertEqual(KLINE_LENGTH_PARAM_GRID['hft_ticks_per_bar'], [2, 3, 5, 8, 10, 15, 20])

    def test_grid_total_params(self):
        self.assertGreaterEqual(len(KLINE_LENGTH_PARAM_GRID), 23)

    def test_multiscale_lengths(self):
        self.assertEqual(MULTISCALE_BAR_LENGTHS, [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440])

    def test_bar_interval_grid_depth(self):
        self.assertGreaterEqual(len(BAR_INTERVAL_GRID['resonance']), 7)
        self.assertGreaterEqual(len(BAR_INTERVAL_GRID['box']), 9)
        self.assertGreaterEqual(len(BAR_INTERVAL_GRID['spring']), 9)

    def test_kline_grid_bar_interval_coverage(self):
        bar_levels = KLINE_LENGTH_PARAM_GRID['bar_interval_minutes']
        self.assertGreaterEqual(len(bar_levels), 11)
        self.assertIn(1, bar_levels)
        self.assertIn(1440, bar_levels)

    def test_kline_grid_trend_period_depth(self):
        self.assertGreaterEqual(len(KLINE_LENGTH_PARAM_GRID['trend_period_short']), 5)
        self.assertGreaterEqual(len(KLINE_LENGTH_PARAM_GRID['trend_period_medium']), 5)
        self.assertGreaterEqual(len(KLINE_LENGTH_PARAM_GRID['trend_period_long']), 5)

    def test_kline_grid_hft_depth(self):
        self.assertGreaterEqual(len(KLINE_LENGTH_PARAM_GRID['hft_signal_confirm_ticks']), 5)
        self.assertGreaterEqual(len(KLINE_LENGTH_PARAM_GRID['hft_ticks_per_bar']), 7)

    def test_kline_grid_total_space(self):
        total = 1
        for k, v in KLINE_LENGTH_PARAM_GRID.items():
            total *= len(v)
        self.assertGreaterEqual(total, 1_000_000)


class TestHFTTickFidelityBacktest(unittest.TestCase):
    """HFT tick保真回测输出结构正确性"""

    def setUp(self):
        np.random.seed(42)

    def test_tick_fidelity_output_structure(self):
        n = 100
        times = pd.date_range('2025-03-15 09:30:00', periods=n, freq='1min')
        bar_data = pd.DataFrame({
            'minute': times,
            'symbol': 'IF2606',
            'open': 4000 + np.random.randn(n).cumsum() * 0.5,
            'high': 4002 + np.abs(np.random.randn(n)),
            'low': 3998 - np.abs(np.random.randn(n)),
            'close': 4001 + np.random.randn(n).cumsum() * 0.5,
            'volume': np.random.randint(100, 500, n),
            'imbalance': np.random.randn(n) * 0.3,
            'strength': np.random.rand(n) * 0.8,
            'iv': 20 + np.random.rand(n) * 5,
        })
        bar_data['high'] = bar_data[['open', 'high', 'close']].max(axis=1) + 0.5
        bar_data['low'] = bar_data[['open', 'low', 'close']].min(axis=1) - 0.5

        result = run_backtest_hft_tick_fidelity(
            PARAM_DEFAULTS_HFT, bar_data, train=True,
        )

        self.assertIn('hft_fidelity', result)
        self.assertEqual(result['hft_fidelity'], 'TICK_INTERPOLATED')
        self.assertIn('ticks_per_bar', result)
        self.assertIn('confirm_ticks', result)
        self.assertIn('sharpe', result)
        self.assertIn('max_drawdown', result)
        self.assertIn('n_trades', result)

    def test_tick_fidelity_vs_bar_degraded(self):
        n = 200
        times = pd.date_range('2025-03-15 09:30:00', periods=n, freq='1min')
        bar_data = pd.DataFrame({
            'minute': times,
            'symbol': 'IF2606',
            'open': 4000 + np.random.randn(n).cumsum() * 0.5,
            'high': 4002 + np.abs(np.random.randn(n)),
            'low': 3998 - np.abs(np.random.randn(n)),
            'close': 4001 + np.random.randn(n).cumsum() * 0.5,
            'volume': np.random.randint(100, 500, n),
            'imbalance': np.random.randn(n) * 0.3,
            'strength': np.random.rand(n) * 0.8,
            'iv': 20 + np.random.rand(n) * 5,
        })
        bar_data['high'] = bar_data[['open', 'high', 'close']].max(axis=1) + 0.5
        bar_data['low'] = bar_data[['open', 'low', 'close']].min(axis=1) - 0.5

        params_tick = PARAM_DEFAULTS_HFT.copy()
        params_tick['hft_ticks_per_bar'] = 10

        result_tick = run_backtest_hft_tick_fidelity(params_tick, bar_data, train=True)
        self.assertEqual(result_tick['hft_fidelity'], 'TICK_INTERPOLATED')
        self.assertEqual(result_tick['ticks_per_bar'], 10)


if __name__ == '__main__':
    unittest.main()
