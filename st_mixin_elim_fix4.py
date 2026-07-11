# MODULE_ID: M2-419
import sys
import os
import inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService, _StorageQueryInstrumentMixin
from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService


def test_instrument_service_exists():
    assert StorageInstrumentService is not None
    assert _StorageQueryInstrumentMixin is StorageInstrumentService


def test_constructor_explicit_deps():
    sig = inspect.signature(StorageInstrumentService.__init__)
    params = list(sig.parameters.keys())
    assert 'base_service' in params, f"__init__ should have base_service param, got {params}"


def test_key_methods_preserved():
    methods = [
        'get_option_chain_by_future_id', 'get_option_chain_for_future',
        'query_kline', 'query_tick', 'get_option_chain',
        'get_latest_underlying', 'load_all_instruments',
        'ensure_registered_instruments', 'register_instrument',
        'delete_instrument', 'batch_add_future_instruments',
        'batch_add_option_instruments',
    ]
    for m in methods:
        assert hasattr(StorageInstrumentService, m), f"StorageInstrumentService missing method {m}"


def test_delegates_to_base():
    import threading
    lock = threading.Lock()
    class FakePS:
        pass
    class FakeDS:
        pass
    base = StorageQueryBaseService(lock, FakePS(), FakeDS())
    svc = StorageInstrumentService(base)
    assert svc._lock is lock
    assert svc._params_service is base._params_service


def test_no_mixin_in_name():
    assert 'Mixin' not in StorageInstrumentService.__name__


if __name__ == '__main__':
    test_instrument_service_exists()
    test_constructor_explicit_deps()
    test_key_methods_preserved()
    test_delegates_to_base()
    test_no_mixin_in_name()
    print("ALL 5 ASSERTIONS PASSED for Fix 4")