# MODULE_ID: M2-386
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStorageHistoryService:
    def test_import(self):
        from ali2026v3_trading.data.storage_query_history import StorageHistoryService
        assert StorageHistoryService is not None

    def test_backward_compat_alias(self):
        from ali2026v3_trading.data.storage_query_history import _StorageQueryHistoryMixin
        assert _StorageQueryHistoryMixin is not None

    def test_init_with_base_service(self):
        from ali2026v3_trading.data.storage_query_history import StorageHistoryService
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        base = StorageQueryBaseService(lock, params, data)
        svc = StorageHistoryService(base)
        assert svc is not None

    def test_has_load_historical_klines(self):
        from ali2026v3_trading.data.storage_query_history import StorageHistoryService
        assert hasattr(StorageHistoryService, 'load_historical_klines')