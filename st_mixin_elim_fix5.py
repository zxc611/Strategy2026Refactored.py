# MODULE_ID: M2-420
import sys
import os
import inspect
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.storage_query_history import StorageHistoryService, _StorageQueryHistoryMixin
from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService, _StorageQueryBaseMixin
from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService, _StorageQueryInstrumentMixin
from ali2026v3_trading.data.storage_core import StorageQuery, _StorageQueryMixin


def test_history_service_exists():
    assert StorageHistoryService is not None
    assert _StorageQueryHistoryMixin is StorageHistoryService


def test_constructor_explicit_deps():
    sig = inspect.signature(StorageHistoryService.__init__)
    params = list(sig.parameters.keys())
    assert 'base_service' in params, f"__init__ should have base_service param, got {params}"


def test_key_methods_preserved():
    methods = [
        '_fetch_historical_kline_data', 'load_historical_klines',
        '_migrate_legacy_schema', '_create_indexes',
    ]
    for m in methods:
        assert hasattr(StorageHistoryService, m), f"StorageHistoryService missing method {m}"


def test_storage_query_no_mixin_inheritance():
    bases = StorageQuery.__bases__
    assert _StorageQueryBaseMixin not in bases, f"StorageQuery should NOT inherit _StorageQueryBaseMixin"
    assert _StorageQueryInstrumentMixin not in bases, f"StorageQuery should NOT inherit _StorageQueryInstrumentMixin"
    assert _StorageQueryHistoryMixin not in bases, f"StorageQuery should NOT inherit _StorageQueryHistoryMixin"


def test_storage_query_facade_delegates():
    lock = threading.Lock()
    class FakePS:
        pass
    class FakeDS:
        pass
    sq = StorageQuery(lock, FakePS(), FakeDS())
    assert hasattr(sq, 'infer_exchange_from_id')
    assert hasattr(sq, 'register_instrument')
    assert hasattr(sq, 'load_historical_klines')


if __name__ == '__main__':
    test_history_service_exists()
    test_constructor_explicit_deps()
    test_key_methods_preserved()
    test_storage_query_no_mixin_inheritance()
    test_storage_query_facade_delegates()
    print("ALL 5 ASSERTIONS PASSED for Fix 5")