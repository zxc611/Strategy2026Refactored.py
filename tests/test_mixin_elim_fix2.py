# MODULE_ID: M2-417
import sys
import os
import inspect
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.query_data_export import DataExportService, _QueryDataExportMixin
from ali2026v3_trading.data.query_service import QueryService


def test_data_export_service_exists():
    assert DataExportService is not None
    assert _QueryDataExportMixin is DataExportService


def test_constructor_explicit_deps():
    sig = inspect.signature(DataExportService.__init__)
    params = list(sig.parameters.keys())
    assert 'storage' in params, f"__init__ should have storage param, got {params}"
    assert 'params_service' in params, f"__init__ should have params_service param, got {params}"


def test_key_methods_preserved():
    methods = [
        'get_kline_count', 'get_tick_count', 'get_kline_range',
        'get_tick_range', 'get_instrument_summary', 'get_all_instruments_summary',
        'get_storage_stats', 'export_kline_to_csv', 'export_tick_to_csv',
        '_diagnose_contract',
    ]
    for m in methods:
        assert hasattr(DataExportService, m), f"DataExportService missing method {m}"


def test_query_service_no_export_mixin_inheritance():
    bases = QueryService.__bases__
    assert _QueryDataExportMixin not in bases, f"QueryService should NOT inherit _QueryDataExportMixin, bases={bases}"


def test_query_service_delegates_to_export_service():
    qs = QueryService.__new__(QueryService)
    qs._storage = None
    qs._futures_file_path = ""
    qs._internal_id_to_instrument_idx = {}
    qs._idx_built = False
    qs._idx_lock = threading.Lock()
    qs._instrument_service = None
    qs._export_service = DataExportService(None)
    assert hasattr(qs, 'get_kline_count')


if __name__ == '__main__':
    test_data_export_service_exists()
    test_constructor_explicit_deps()
    test_key_methods_preserved()
    test_query_service_no_export_mixin_inheritance()
    test_query_service_delegates_to_export_service()
    print("ALL 5 ASSERTIONS PASSED for Fix 2")