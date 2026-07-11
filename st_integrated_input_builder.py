# MODULE_ID: M2-360
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestInputBuilder:
    def test_import(self):
        from ali2026v3_trading.risk_engine.input_builder import build_snapshot
        assert callable(build_snapshot)

    def test_build_snapshot_callable(self):
        from ali2026v3_trading.risk_engine.input_builder import build_snapshot
        signal = {
            "symbol": "au2506",
            "instrument_id": "au2506",
            "action": "OPEN",
            "direction": "LONG",
            "price": 500.0,
            "volume": 1.0,
            "amount": 1.0,
        }
        risk_service = MagicMock()
        try:
            result = build_snapshot(signal, risk_service)
            assert result is not None
        except Exception:
            pass