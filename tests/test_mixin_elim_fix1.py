# MODULE_ID: M2-416
import sys
import os
import inspect
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.query_instrument_service import InstrumentQueryService, _QueryInstrumentMixin
from ali2026v3_trading.data.query_service import QueryService


def test_instrument_query_service_exists():
    assert InstrumentQueryService is not None
    assert _QueryInstrumentMixin is InstrumentQueryService


def test_constructor_explicit_deps():
    sig = inspect.signature(InstrumentQueryService.__init__)
    params = list(sig.parameters.keys())
    assert 'storage' in params, f"__init__ should have storage param, got {params}"
    assert 'params_service' in params, f"__init__ should have params_service param, got {params}"


def test_key_methods_preserved():
    methods = [
        'classify_instruments', 'infer_exchange_from_id',
        'get_registered_instrument_ids', 'ensure_registered_instruments',
        'load_and_preregister_instruments', 'get_active_instruments_by_product',
        'get_option_chain_for_future',
    ]
    for m in methods:
        assert hasattr(InstrumentQueryService, m), f"InstrumentQueryService missing method {m}"


def test_query_service_no_mixin_inheritance():
    bases = QueryService.__bases__
    assert _QueryInstrumentMixin not in bases, f"QueryService should NOT inherit _QueryInstrumentMixin, bases={bases}"


def test_query_service_delegates_to_instrument_service():
    qs = QueryService.__new__(QueryService)
    qs._storage = None
    qs._futures_file_path = ""
    qs._internal_id_to_instrument_idx = {}
    qs._idx_built = False
    qs._idx_lock = threading.Lock()
    qs._instrument_service = InstrumentQueryService(None)
    qs._export_service = None
    assert hasattr(qs, 'classify_instruments')


if __name__ == '__main__':
    test_instrument_query_service_exists()
    test_constructor_explicit_deps()
    test_key_methods_preserved()
    test_query_service_no_mixin_inheritance()
    test_query_service_delegates_to_instrument_service()
    print("ALL 5 ASSERTIONS PASSED for Fix 1")