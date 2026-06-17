# MODULE_ID: M2-355
"""测试: risk_engine/input_builder.py"""
import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.risk_engine.input_builder import build_snapshot
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot


class TestBuildSnapshot:
    def test_builds_from_risk_service_attrs(self):
        rs = MagicMock()
        rs.params = MagicMock()
        rs.strategy = MagicMock()
        rs.position_manager = MagicMock()
        signal = {'instrument_id': 'AL2401', 'direction': 'BUY', 'volume': 1}
        with patch('ali2026v3_trading.risk_engine.input_builder.RiskSnapshot') as mock_snap:
            mock_snap.from_signal.return_value = MagicMock()
            result = build_snapshot(signal, rs)
            mock_snap.from_signal.assert_called_once()
            assert result is not None

    def test_missing_attrs_returns_none_attrs(self):
        rs = MagicMock(spec=[])
        signal = {'instrument_id': 'AL2401'}
        with patch('ali2026v3_trading.risk_engine.input_builder.RiskSnapshot') as mock_snap:
            mock_snap.from_signal.return_value = MagicMock()
            result = build_snapshot(signal, rs)
            call_kwargs = mock_snap.from_signal.call_args[1]
            assert call_kwargs['params'] is None
            assert call_kwargs['strategy'] is None
            assert call_kwargs['position_manager'] is None

    def test_safety_meta_layer_import_failure(self):
        rs = MagicMock()
        rs.params = MagicMock()
        signal = {'instrument_id': 'AL2401'}
        with patch('ali2026v3_trading.risk_engine.input_builder.RiskSnapshot') as mock_snap:
            mock_snap.from_signal.return_value = MagicMock()
            with patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', side_effect=ImportError):
                result = build_snapshot(signal, rs)
                call_kwargs = mock_snap.from_signal.call_args[1]
                assert call_kwargs['safety_meta_layer'] is None

    def test_signal_passed_through(self):
        rs = MagicMock()
        signal = {'instrument_id': 'IF2401', 'direction': 'SELL', 'volume': 2}
        with patch('ali2026v3_trading.risk_engine.input_builder.RiskSnapshot') as mock_snap:
            mock_snap.from_signal.return_value = MagicMock()
            build_snapshot(signal, rs)
            call_kwargs = mock_snap.from_signal.call_args[1]
            assert call_kwargs['signal'] is signal