# MODULE_ID: M2-331
"""Tests for data.data_service module."""
import sys
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


def _ensure_imports():
    for _mod_name in [
        'data.data_service',
        'data.ds_realtime_cache',
    ]:
        if _mod_name in sys.modules:
            del sys.modules[_mod_name]
    from data import data_service as ds
    return ds


class TestDataServiceSingleton:
    def test_reset_and_get(self):
        ds = _ensure_imports()
        ds.reset_data_service()
        assert ds._data_service_instance is None
        with patch.object(ds.DataService, '_initialize', lambda self: None):
            svc = ds.get_data_service()
            assert svc is not None
            assert ds.get_data_service() is svc
        ds.reset_data_service()
        assert ds._data_service_instance is None


class TestDataServiceMethods:
    def _make_svc(self):
        ds = _ensure_imports()
        with patch.object(ds.DataService, '_initialize', lambda self: None):
            svc = ds.DataService()
        svc.realtime_cache = None
        svc._params_service = None
        svc._products_loaded = False
        return svc

    def test_bind_subscribe_api(self):
        ds = _ensure_imports()
        sub = MagicMock()
        unsub = MagicMock()
        ds.DataService.bind_subscribe_api(sub, unsub)
        assert ds.DataService._subscribe_api_bound is True
        ds.DataService.bind_subscribe_api(None, None)
        assert ds.DataService._subscribe_api_bound is False

    def test_subscribe_no_fn(self):
        svc = self._make_svc()
        ds = _ensure_imports()
        ds.DataService._subscribe_fn = None
        assert svc.subscribe("IF2606") is None

    def test_subscribe_with_fn(self):
        svc = self._make_svc()
        ds = _ensure_imports()
        ds.DataService._subscribe_fn = MagicMock(return_value="ok")
        assert svc.subscribe("IF2606") == "ok"
        ds.DataService._subscribe_fn = None

    def test_request_realtime_no_fn(self):
        svc = self._make_svc()
        ds = _ensure_imports()
        ds.DataService._subscribe_fn = None
        assert svc.request_realtime("IF2606") is False

    def test_request_realtime_with_fn(self):
        svc = self._make_svc()
        ds = _ensure_imports()
        ds.DataService._subscribe_fn = MagicMock(return_value=True)
        assert svc.request_realtime("IF2606") is True
        ds.DataService._subscribe_fn = None

    def test_subscription_manager_lazy_create(self):
        svc = self._make_svc()
        sm = svc.subscription_manager
        assert sm is not None
        assert sm.data_manager is svc
        assert svc.subscription_manager is sm

    def test_bind_data_manager_replays_to_subscription_manager(self):
        svc = self._make_svc()
        mock_sm = svc.subscription_manager
        mock_sm.bind_data_manager = MagicMock()
        svc.bind_data_manager(svc)
        mock_sm.bind_data_manager.assert_called_once_with(svc)

    def test_check_data_source_ready_no_conn(self):
        svc = self._make_svc()
        svc._get_connection = MagicMock(return_value=None)
        ready, msg = svc.check_data_source_ready()
        assert ready is False
        assert "DuckDB" in msg or "数据源" in msg

    def test_check_data_source_ready_with_conn(self):
        svc = self._make_svc()
        mock_conn = MagicMock()
        svc._get_connection = MagicMock(return_value=mock_conn)
        svc._return_connection = MagicMock()
        svc.realtime_cache = MagicMock()
        ready, msg = svc.check_data_source_ready()
        assert ready is True

    def test_mark_products_loaded(self):
        svc = self._make_svc()
        svc.mark_products_loaded()
        assert svc._products_loaded is True

    def test_params_service_lazy(self):
        svc = self._make_svc()
        svc._products_loaded = True
        with patch("config.params_service.get_params_service") as mock_get:
            mock_ps = MagicMock()
            mock_get.return_value = mock_ps
            ps = svc.params_service
            assert ps is mock_ps


class TestConvenienceFunctions:
    def test_query(self):
        ds = _ensure_imports()
        with patch.object(ds, "get_data_service") as mock_get:
            mock_svc = MagicMock()
            mock_get.return_value = mock_svc
            ds.query("SELECT 1")
            mock_svc.query.assert_called_once()

    def test_get_latest_price(self):
        ds = _ensure_imports()
        with patch.object(ds, "get_data_service") as mock_get:
            mock_svc = MagicMock()
            mock_svc.get_latest_price.return_value = 3.14
            mock_get.return_value = mock_svc
            assert ds.get_latest_price("IF") == 3.14

    def test_refresh_data(self):
        ds = _ensure_imports()
        with patch.object(ds, "get_data_service") as mock_get:
            mock_svc = MagicMock()
            mock_svc.refresh_data.return_value = True
            mock_get.return_value = mock_svc
            assert ds.refresh_data() is True

    def test_batch_insert_ticks(self):
        ds = _ensure_imports()
        with patch.object(ds, "get_data_service") as mock_get:
            mock_svc = MagicMock()
            mock_svc.batch_insert_ticks.return_value = 5
            mock_get.return_value = mock_svc
            assert ds.batch_insert_ticks([{"a": 1}]) == 5

    def test_clear_cache(self):
        ds = _ensure_imports()
        with patch.object(ds, "get_data_service") as mock_get:
            mock_svc = MagicMock()
            mock_get.return_value = mock_svc
            ds.clear_cache()
            mock_svc.clear_cache.assert_called_once()


class TestModuleGlobals:
    def test_globals_exist(self):
        ds = _ensure_imports()
        assert hasattr(ds, "DB_FILE")
        assert hasattr(ds, "PARQUET_PATH")
        assert hasattr(ds, "DUCKDB_MAX_MEMORY")
        assert hasattr(ds, "DUCKDB_THREADS")
        assert hasattr(ds, "QUERY_CACHE_SIZE")
        assert hasattr(ds, "QUERY_CACHE_TTL")
