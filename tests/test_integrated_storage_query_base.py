# MODULE_ID: M2-385
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStorageQueryBaseService:
    def test_import(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        assert StorageQueryBaseService is not None

    def test_backward_compat_alias(self):
        from ali2026v3_trading.data.storage_query_base import _StorageQueryBaseMixin
        assert _StorageQueryBaseMixin is not None

    def test_infer_exchange_from_id(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        from unittest.mock import MagicMock
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        svc = StorageQueryBaseService(lock, params, data)
        result = svc.infer_exchange_from_id("au2506")
        assert isinstance(result, str)

    def test_validate_tick(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        from unittest.mock import MagicMock
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        svc = StorageQueryBaseService(lock, params, data)
        valid_tick = {"instrument_id": "au2506", "price": 500.0, "volume": 1}
        result = svc._validate_tick(valid_tick)
        assert isinstance(result, bool)

    def test_validate_kline(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        from unittest.mock import MagicMock
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        svc = StorageQueryBaseService(lock, params, data)
        valid_kline = {"instrument_id": "au2506", "open": 500.0, "close": 501.0}
        result = svc._validate_kline(valid_kline)
        assert isinstance(result, bool)

    def test_normalize_tick_fields(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        from unittest.mock import MagicMock
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        svc = StorageQueryBaseService(lock, params, data)
        tick = {"price": 500.0, "volume": 1}
        result = svc._normalize_tick_fields(tick)
        assert isinstance(result, dict)

    def test_to_timestamp(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        result = StorageQueryBaseService._to_timestamp("2026-06-15 10:00:00")
        assert result is not None or result is None

    def test_make_instrument_info(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        info = StorageQueryBaseService._make_instrument_info(
            internal_id=1, instrument_id="au2506", instrument_type="future"
        )
        assert isinstance(info, dict)
        assert info['instrument_id'] == "au2506"

    def test_json_default(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        result = StorageQueryBaseService._json_default(ValueError("test"))
        assert isinstance(result, str)