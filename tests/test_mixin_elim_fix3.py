# MODULE_ID: M2-418
import sys
import os
import inspect
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService, _StorageQueryBaseMixin


def test_base_service_exists():
    assert StorageQueryBaseService is not None
    assert _StorageQueryBaseMixin is StorageQueryBaseService


def test_constructor_explicit_deps():
    sig = inspect.signature(StorageQueryBaseService.__init__)
    params = list(sig.parameters.keys())
    assert 'lock' in params, f"__init__ should have lock param, got {params}"
    assert 'params_service' in params, f"__init__ should have params_service param, got {params}"
    assert 'data_service' in params, f"__init__ should have data_service param, got {params}"
    assert 'maintenance_service' in params, f"__init__ should have maintenance_service param, got {params}"


def test_key_methods_preserved():
    methods = [
        '_ensure_exchange_map_loaded', '_validate_table_name',
        '_execute_with_retry', '_parse_future', '_parse_option_with_dash',
        '_make_instrument_info', '_get_info_internal_id',
        'infer_exchange_from_id', '_validate_tick', '_validate_kline',
        '_get_instrument_info', '_get_info_by_id',
        'get_registered_instrument_ids', 'load',
        '_cache_to_params_service', '_cache_alias_instrument_mapping',
    ]
    for m in methods:
        assert hasattr(StorageQueryBaseService, m), f"StorageQueryBaseService missing method {m}"


def test_no_mixin_in_name():
    assert 'Mixin' not in StorageQueryBaseService.__name__


def test_cache_to_params_service_delegates():
    svc = StorageQueryBaseService.__new__(StorageQueryBaseService)
    called = {}
    class FakePS:
        def cache_instrument_info(self, iid, info):
            called['iid'] = iid
            called['info'] = info
            return info
    svc._params_service = FakePS()
    svc._cache_to_params_service('IF2603', {'internal_id': 1, 'instrument_id': 'IF2603'})
    assert called['iid'] == 'IF2603'


if __name__ == '__main__':
    test_base_service_exists()
    test_constructor_explicit_deps()
    test_key_methods_preserved()
    test_no_mixin_in_name()
    test_cache_to_params_service_delegates()
    print("ALL 5 ASSERTIONS PASSED for Fix 3")