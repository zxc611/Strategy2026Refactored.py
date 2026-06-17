# MODULE_ID: M2-388
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStorageWalService:
    def test_import(self):
        from ali2026v3_trading.data.storage_wal_mixin import StorageWalService
        assert StorageWalService is not None

    def test_backward_compat_alias(self):
        from ali2026v3_trading.data.storage_wal_mixin import _StorageWalMixin
        assert _StorageWalMixin is not None

    def test_init_with_facade(self):
        from ali2026v3_trading.data.storage_wal_mixin import StorageWalService
        facade = MagicMock()
        svc = StorageWalService(facade=facade)
        assert svc is not None

    def test_init_without_facade(self):
        from ali2026v3_trading.data.storage_wal_mixin import StorageWalService
        svc = StorageWalService()
        assert svc is not None

    def test_has_spill_wal_methods(self):
        from ali2026v3_trading.data.storage_wal_mixin import StorageWalService
        assert hasattr(StorageWalService, '_spill_wal_append')
        assert hasattr(StorageWalService, '_spill_wal_append_batch')
        assert hasattr(StorageWalService, '_spill_wal_clear')