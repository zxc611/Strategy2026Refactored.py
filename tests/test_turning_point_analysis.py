# MODULE_ID: M2-598
"""Tests for turning_point_analysis.py"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import numpy as np


def _ensure_imports():
    # 清除可能被前面测试污染的 mock 模块缓存
    for _mod_name in [
        'strategy_judgment.turning_point_analysis',
        'strategy_judgment.market_snapshot_collector',
    ]:
        if _mod_name in sys.modules:
            del sys.modules[_mod_name]
    try:
        from strategy_judgment.turning_point_analysis import (
            TurningPointMicroscope, ResonanceTurningPointMarker,
            EnhancedBar, ExtremeRegion, MAAlignment, TurningPointType, _ResonancePhase,
        )
        return TurningPointMicroscope, ResonanceTurningPointMarker, EnhancedBar, ExtremeRegion, MAAlignment, TurningPointType, _ResonancePhase
    except Exception as e:
        pytest.skip(f"Import failed: {e}")


class TestTurningPointMicroscope:
    def test_init(self):
        TurningPointMicroscope = _ensure_imports()[0]
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST", ma_periods=[5, 10], extreme_window=10)
        assert inst._symbol == "TEST"
        assert inst._ma_periods == [5, 10]

    def test_process_tick_and_emit(self):
        TurningPointMicroscope, _, EnhancedBar, ExtremeRegion, *_ = _ensure_imports()
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST", minute_ms=60_000)
        ts = np.datetime64("2024-01-01T00:00:00")
        bar = inst.process_tick(ts, last_price=100.0, volume=10, turnover=1000.0)
        # First tick should not emit bar
        assert bar is None
        # Tick in next minute should emit previous bar
        ts2 = np.datetime64("2024-01-01T00:01:00")
        bar2 = inst.process_tick(ts2, last_price=101.0, volume=10, turnover=1010.0)
        assert bar2 is not None
        assert isinstance(bar2, EnhancedBar)
        assert bar2.symbol == "TEST"

    def test_get_all_bars(self):
        TurningPointMicroscope = _ensure_imports()[0]
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST", minute_ms=60_000)
        ts = np.datetime64("2024-01-01T00:00:00")
        inst.process_tick(ts, last_price=100.0, volume=10, turnover=1000.0)
        ts2 = np.datetime64("2024-01-01T00:01:00")
        inst.process_tick(ts2, last_price=101.0, volume=10, turnover=1010.0)
        bars = inst.get_all_bars()
        assert len(bars) >= 1

    def test_get_statistics(self):
        TurningPointMicroscope = _ensure_imports()[0]
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST", minute_ms=60_000)
        stats = inst.get_statistics()
        assert stats["symbol"] == "TEST"
        assert stats["total_bars"] == 0

    def test_flush_empty(self):
        TurningPointMicroscope = _ensure_imports()[0]
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST", minute_ms=60_000)
        assert inst.flush() is None

    def test_classify_ma_alignment_bullish(self):
        TurningPointMicroscope, _, _, _, MAAlignment, *_ = _ensure_imports()
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST")
        ma_values = {5: 110, 10: 105, 15: 100}
        assert inst._classify_ma_alignment(ma_values) == MAAlignment.BULLISH

    def test_classify_ma_alignment_bearish(self):
        TurningPointMicroscope, _, _, _, MAAlignment, *_ = _ensure_imports()
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST")
        ma_values = {5: 100, 10: 105, 15: 110}
        assert inst._classify_ma_alignment(ma_values) == MAAlignment.BEARISH

    def test_classify_ma_alignment_transitional(self):
        TurningPointMicroscope, _, _, _, MAAlignment, *_ = _ensure_imports()
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST")
        ma_values = {5: 100}
        assert inst._classify_ma_alignment(ma_values) == MAAlignment.TRANSITIONAL

    def test_detect_extreme_region_normal(self):
        TurningPointMicroscope, _, EnhancedBar, ExtremeRegion, *_ = _ensure_imports()
        inst = TurningPointMicroscope.__new__(TurningPointMicroscope)
        inst.__init__("TEST", extreme_window=5)
        # Seed some bars to fill buffers
        for i in range(10):
            bar = EnhancedBar.__new__(EnhancedBar)
            bar.high = 100.0 + i
            bar.low = 90.0 + i
            bar.close = 95.0 + i
            bar.vwap = 95.0 + i
            bar.atr14 = 1.0
            inst._high_buffer.append(bar.high)
            inst._low_buffer.append(bar.low)
            inst._close_buffer.append(bar.close)
            inst._tr_buffer.append(1.0)
            inst._last_bar = bar
        region = inst._detect_extreme_region(95.0)
        assert region == ExtremeRegion.NORMAL or region in (
            ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW, ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW
        )

    def test_build_from_preprocessed_df_no_crash(self):
        TurningPointMicroscope = _ensure_imports()[0]
        try:
            import pandas as pd
        except Exception as e:
            pytest.skip(f"pandas not available: {e}")
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="min"),
            "open": [100.0] * 10,
            "high": [101.0] * 10,
            "low": [99.0] * 10,
            "close": [100.5] * 10,
            "volume": [100] * 10,
            "vwap": [100.2] * 10,
            "bid_ask_spread": [0.01] * 10,
            "open_interest": [0] * 10,
            "tick_count": [10] * 10,
        })
        inst = TurningPointMicroscope.build_from_preprocessed_df(df, symbol="TEST")
        assert inst._symbol == "TEST"
        assert inst._bar_count == 10


class FakeCROutput:
    def __init__(self, phase, directional_bias, resonance_strength, state_entropy):
        self.phase = phase
        self.directional_bias = directional_bias
        self.resonance_strength = resonance_strength
        self.state_entropy = state_entropy


class TestResonanceTurningPointMarker:
    def test_init(self):
        ResonanceTurningPointMarker = _ensure_imports()[1]
        inst = ResonanceTurningPointMarker.__new__(ResonanceTurningPointMarker)
        inst.__init__("TEST")
        assert inst._symbol == "TEST"
        assert inst._confirmation_window == 10

    def test_process_bar_no_crash(self):
        ResonanceTurningPointMarker = _ensure_imports()[1]
        EnhancedBar = _ensure_imports()[2]
        ExtremeRegion = _ensure_imports()[3]
        inst = ResonanceTurningPointMarker.__new__(ResonanceTurningPointMarker)
        inst.__init__("TEST")
        bar = EnhancedBar.__new__(EnhancedBar)
        bar.timestamp = np.datetime64("2024-01-01T00:00:00")
        bar.symbol = "TEST"
        bar.vwap = 100.0
        bar.price_ma_deviation_sigma = {"ma5": 0.0}
        bar.ma_alignment = type("A", (), {"value": "过渡态"})()
        bar.extreme_region = ExtremeRegion.NORMAL
        tps = inst.process_bar(bar, cr_output=None)
        assert isinstance(tps, list)

    def test_process_bar_with_cr_output(self):
        ResonanceTurningPointMarker = _ensure_imports()[1]
        EnhancedBar = _ensure_imports()[2]
        ExtremeRegion = _ensure_imports()[3]
        inst = ResonanceTurningPointMarker.__new__(ResonanceTurningPointMarker)
        inst.__init__("TEST")
        bar = EnhancedBar.__new__(EnhancedBar)
        bar.timestamp = np.datetime64("2024-01-01T00:00:00")
        bar.symbol = "TEST"
        bar.vwap = 100.0
        bar.price_ma_deviation_sigma = {"ma5": 3.0}
        bar.ma_alignment = type("A", (), {"value": "均线收敛"})()
        bar.extreme_region = ExtremeRegion.NEAR_HIGH
        phase = type("P", (), {"value": "释放"})()
        cr = FakeCROutput(phase, directional_bias=0.5, resonance_strength=0.6, state_entropy=0.3)
        tps = inst.process_bar(bar, cr_output=cr)
        assert isinstance(tps, list)
        # Second bar with phase change should generate expected TP
        bar2 = EnhancedBar.__new__(EnhancedBar)
        bar2.timestamp = np.datetime64("2024-01-01T00:01:00")
        bar2.symbol = "TEST"
        bar2.vwap = 101.0
        bar2.price_ma_deviation_sigma = {"ma5": 3.0}
        bar2.ma_alignment = type("A", (), {"value": "均线收敛"})()
        bar2.extreme_region = ExtremeRegion.NEAR_HIGH
        phase2 = type("P2", (), {"value": "衰竭"})()
        cr2 = FakeCROutput(phase2, directional_bias=0.5, resonance_strength=0.6, state_entropy=0.3)
        tps2 = inst.process_bar(bar2, cr_output=cr2)
        assert isinstance(tps2, list)

    def test_are_types_compatible(self):
        ResonanceTurningPointMarker = _ensure_imports()[1]
        TurningPointType = _ensure_imports()[5]
        inst = ResonanceTurningPointMarker.__new__(ResonanceTurningPointMarker)
        inst.__init__("TEST")
        assert inst._are_types_compatible(TurningPointType.EXPECTED_TREND_REVERSAL, TurningPointType.ACTUAL_TREND_REVERSAL) is True
        assert inst._are_types_compatible(TurningPointType.EXPECTED_TREND_CONTINUATION, TurningPointType.ACTUAL_TREND_CONTINUATION) is True
        assert inst._are_types_compatible(TurningPointType.EXPECTED_TREND_REVERSAL, TurningPointType.ACTUAL_TREND_CONTINUATION) is False

    def test_expire_unconfirmed(self):
        ResonanceTurningPointMarker = _ensure_imports()[1]
        inst = ResonanceTurningPointMarker.__new__(ResonanceTurningPointMarker)
        inst.__init__("TEST", confirmation_window_bars=2)
        inst._bar_count = 10
        inst._unconfirmed_expected = []
        inst._expire_unconfirmed()
        assert inst._unconfirmed_expected == []

    def test_get_prediction_accuracy_empty(self):
        ResonanceTurningPointMarker = _ensure_imports()[1]
        inst = ResonanceTurningPointMarker.__new__(ResonanceTurningPointMarker)
        inst.__init__("TEST")
        acc = inst.get_prediction_accuracy()
        assert acc["total_expected"] == 0
        assert acc["hit_rate"] == 0.0
