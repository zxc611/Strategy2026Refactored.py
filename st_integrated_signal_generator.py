# MODULE_ID: M2-383
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestSignalContext:
    def test_creation(self):
        from ali2026v3_trading.signal.signal_generator import SignalContext
        ctx = SignalContext(instrument_id="au2506", signal_type="LONG", price=500.0)
        assert ctx.instrument_id == "au2506"
        assert ctx.signal_type == "LONG"
        assert ctx.price == 500.0
        assert not ctx.rejected

    def test_default_values(self):
        from ali2026v3_trading.signal.signal_generator import SignalContext
        ctx = SignalContext()
        assert ctx.instrument_id == ''
        assert ctx.price == 0.0
        assert ctx.rejected is False
        assert ctx.reject_reason == ''

    def test_with_all_fields(self):
        from ali2026v3_trading.signal.signal_generator import SignalContext
        ctx = SignalContext(
            instrument_id="au2506",
            signal_type="LONG",
            price=500.0,
            volume=10.0,
            reason="test",
            priority=1,
            signal_strength=0.8,
            days_to_expiry=30,
        )
        assert ctx.volume == 10.0
        assert ctx.priority == 1
        assert ctx.signal_strength == 0.8
        assert ctx.days_to_expiry == 30


class TestSignalGenerator:
    def test_import(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator
        assert SignalGenerator is not None

    def test_init(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator
        svc = MagicMock()
        svc._stats = {'filtered_signals': 0}
        gen = SignalGenerator(svc)
        assert gen is not None

    def test_filter_chain_defined(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator
        assert hasattr(SignalGenerator, '_FILTER_CHAIN')
        assert len(SignalGenerator._FILTER_CHAIN) == 7

    def test_generate_signal(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator, SignalContext
        from unittest.mock import MagicMock
        svc = MagicMock()
        svc._stats = {'filtered_signals': 0}
        svc._plr_filter_enabled = False
        svc._min_estimated_plr = 0
        svc._default_cooldown_seconds = 0
        svc._is_in_cooldown = MagicMock(return_value=False)
        svc._make_cooldown_key = MagicMock(return_value="key")
        svc._collect_decision_dimensions = MagicMock(return_value={})
        svc._decision_score_filter_enabled = False
        svc._hft_time_filter_enabled = False
        svc._adaptive_filter_enabled = False
        gen = SignalGenerator(svc)
        ctx = SignalContext(instrument_id="au2506", signal_type="LONG", price=500.0, signal_strength=0.5)
        result = gen.generate_signal(ctx)
        assert isinstance(result, SignalContext)