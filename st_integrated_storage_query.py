# MODULE_ID: M2-384
import sys
import os
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestStorageQuery:
    def test_import(self):
        from ali2026v3_trading.data.storage_core import StorageQuery
        assert StorageQuery is not None

    def test_backward_compat_alias(self):
        from ali2026v3_trading.data.storage_core import _StorageQueryMixin
        assert _StorageQueryMixin is not None

    def test_facade_composition(self):
        from ali2026v3_trading.data.storage_core import StorageQuery
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        sq = StorageQuery(lock, params, data)
        assert hasattr(sq, '_base_service')
        assert hasattr(sq, '_instrument_service')
        assert hasattr(sq, '_history_service')

    def test_getattr_delegation(self):
        from ali2026v3_trading.data.storage_core import StorageQuery
        lock = MagicMock()
        params = MagicMock()
        data = MagicMock()
        sq = StorageQuery(lock, params, data)
        with pytest.raises(AttributeError):
            sq.nonexistent_method

    def test_validate_tick_bound(self):
        from ali2026v3_trading.data.storage_core import StorageQuery
        assert callable(StorageQuery._validate_tick)

    def test_validate_kline_bound(self):
        from ali2026v3_trading.data.storage_core import StorageQuery
        assert callable(StorageQuery._validate_kline)