# MODULE_ID: M2-387
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStorageInstrumentService:
    def test_import(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert StorageInstrumentService is not None

    def test_backward_compat_alias(self):
        from ali2026v3_trading.data.storage_query_instrument import _StorageQueryInstrumentMixin
        assert _StorageQueryInstrumentMixin is not None

    def test_init_with_base_service(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        base = StorageQueryBaseService(lock, params, data)
        svc = StorageInstrumentService(base)
        assert svc is not None

    def test_has_register_instrument(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'register_instrument')

    def test_has_query_kline(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'query_kline')

    def test_has_query_tick(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'query_tick')

    def test_has_get_option_chain(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'get_option_chain')

    def test_has_ensure_registered_instruments(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'ensure_registered_instruments')

    def test_has_delete_instrument(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'delete_instrument')

    def test_has_batch_add_future_instruments(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'batch_add_future_instruments')

    def test_has_batch_add_option_instruments(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert hasattr(StorageInstrumentService, 'batch_add_option_instruments')