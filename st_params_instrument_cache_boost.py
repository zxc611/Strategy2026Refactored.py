# MODULE_ID: M2-461
import pytest
import threading
from unittest.mock import MagicMock, patch
from ali2026v3_trading.config._params_instrument_cache import InstrumentCacheService


class TestInstrumentCacheServiceInit:
    def test_init_no_params_service(self):
        svc = InstrumentCacheService()
        assert svc._params_service is None

    def test_init_with_params_service(self):
        mock_ps = MagicMock()
        svc = InstrumentCacheService(params_service=mock_ps)
        assert svc._params_service is mock_ps

    def test_getattr_delegation(self):
        mock_ps = MagicMock()
        mock_ps.some_method.return_value = "ok"
        svc = InstrumentCacheService(params_service=mock_ps)
        assert svc.some_method() == "ok"

    def test_getattr_no_params_service(self):
        svc = InstrumentCacheService()
        with pytest.raises(AttributeError):
            svc.nonexistent


class TestInitInstrumentCache:
    def test_init_cache(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        assert svc._instrument_id_to_internal_id == {}
        assert svc._instrument_meta_by_id == {}
        assert svc._product_cache == {}
        assert svc._column_cache == {}


class TestNormalizeIdField:
    def test_removes_id_field(self):
        svc = InstrumentCacheService()
        info = {'id': 123, 'internal_id': 456, 'name': 'test'}
        result = svc._normalize_id_field(info)
        assert 'id' not in result
        assert result['internal_id'] == 456

    def test_no_id_field(self):
        svc = InstrumentCacheService()
        info = {'internal_id': 456, 'name': 'test'}
        result = svc._normalize_id_field(info)
        assert result == info


class TestCacheInstrumentInfo:
    def _make_svc(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {}
        svc._instrument_meta_by_id = {}
        return svc

    def test_cache_new_instrument(self):
        svc = self._make_svc()
        info = {'internal_id': 1, 'product': 'IF'}
        result = svc.cache_instrument_info('IF2606', info)
        assert result['instrument_id'] == 'IF2606'
        assert svc._instrument_id_to_internal_id['IF2606'] == 1

    def test_cache_no_internal_id(self):
        svc = self._make_svc()
        info = {'product': 'IF'}
        result = svc.cache_instrument_info('IF2606', info)
        assert result['instrument_id'] == 'IF2606'

    def test_cache_conflict_internal_id(self):
        svc = self._make_svc()
        info1 = {'internal_id': 1, 'product': 'IF'}
        svc.cache_instrument_info('IF2606', info1)
        info2 = {'internal_id': 1, 'product': 'IH'}
        with pytest.raises(RuntimeError, match="重复 internal_id"):
            svc.cache_instrument_info('IH2606', info2)

    def test_cache_same_instrument_update(self):
        svc = self._make_svc()
        info1 = {'internal_id': 1, 'product': 'IF'}
        svc.cache_instrument_info('IF2606', info1)
        info2 = {'internal_id': 1, 'product': 'IF', 'extra': 'val'}
        result = svc.cache_instrument_info('IF2606', info2)
        assert result['extra'] == 'val'


class TestGetInternalId:
    def _make_svc_with_cache(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {'IF2606': 1}
        svc._instrument_meta_by_id = {}
        return svc

    def test_existing_id(self):
        svc = self._make_svc_with_cache()
        result = svc.get_internal_id('IF2606')
        assert result == 1

    def test_nonexistent_id(self):
        svc = self._make_svc_with_cache()
        with patch.object(svc, '_lazy_load_from_db'):
            result = svc.get_internal_id('IH2606')
            assert result is None


class TestGetInstrumentMeta:
    def _make_svc_with_cache(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {}
        svc._instrument_meta_by_id = {1: {'internal_id': 1, 'product': 'IF'}}
        return svc

    def test_existing_meta(self):
        svc = self._make_svc_with_cache()
        result = svc.get_instrument_meta(1)
        assert result is not None
        assert result['product'] == 'IF'

    def test_nonexistent_meta(self):
        svc = self._make_svc_with_cache()
        with patch.object(svc, '_lazy_load_from_db'):
            result = svc.get_instrument_meta(999)
            assert result is None


class TestGetInstrumentMetaById:
    def test_existing(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {'IF2606': 1}
        svc._instrument_meta_by_id = {1: {'internal_id': 1, 'product': 'IF'}}
        result = svc.get_instrument_meta_by_id('IF2606')
        assert result is not None
        assert result['product'] == 'IF'

    def test_nonexistent(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {}
        svc._instrument_meta_by_id = {}
        with patch.object(svc, '_lazy_load_from_db'):
            result = svc.get_instrument_meta_by_id('IH2606')
            assert result is None


class TestValidateContractsLoaded:
    def _make_svc(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {'IF2606': 1}
        svc._instrument_meta_by_id = {}
        return svc

    def test_all_loaded(self):
        svc = self._make_svc()
        result = svc.validate_contracts_loaded(['IF2606'])
        assert result == []

    def test_missing_contract(self):
        svc = self._make_svc()
        with pytest.raises(RuntimeError, match="failed to load"):
            svc.validate_contracts_loaded(['IF2606', 'IH2606'])


class TestGetIdCacheDeprecated:
    def test_deprecated_warning(self):
        import warnings
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {}
        svc._instrument_meta_by_id = {1: {'product': 'IF'}}
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            svc.get_id_cache(1)
            assert any(issubclass(x.category, DeprecationWarning) for x in w)


class TestGetProductCache:
    def test_existing(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._product_cache = {'IF': {'multiplier': 300}}
        result = svc.get_product_cache('IF')
        assert result is not None

    def test_nonexistent(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._product_cache = {}
        result = svc.get_product_cache('IH')
        assert result is None


class TestGetAllInstrumentCache:
    def test_returns_copy(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {'IF2606': 1}
        svc._instrument_meta_by_id = {1: {'internal_id': 1, 'product': 'IF'}}
        result = svc.get_all_instrument_cache()
        assert 'IF2606' in result
        assert result['IF2606']['product'] == 'IF'


class TestGetAllInstrumentIds:
    def test_returns_ids(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {'IF2606': 1, 'IH2606': 2}
        result = svc.get_all_instrument_ids()
        assert 'IF2606' in result
        assert 'IH2606' in result


class TestCacheColumnNames:
    def test_cache_and_get(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc.cache_column_names('futures_instruments', ['internal_id', 'instrument_id', 'product'])
        result = svc.get_cached_column_names('futures_instruments')
        assert result == ['internal_id', 'instrument_id', 'product']

    def test_nonexistent_table(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        result = svc.get_cached_column_names('nonexistent')
        assert result is None


class TestCacheInstrumentList:
    def test_cache_to_dict(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        params = {}
        svc.cache_instrument_list(params, ['IF2606'], {'IH': ['IH2606']})
        assert params['future_instruments'] == ['IF2606']
        assert params['option_instruments'] == {'IH': ['IH2606']}

    def test_cache_to_object(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        params = MagicMock()
        svc.cache_instrument_list(params, ['IF2606'], {})
        params.__setattr__  # just verify no crash


class TestClearInstrumentCache:
    def _make_svc_with_data(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {'IF2606': 1}
        svc._instrument_meta_by_id = {1: {'internal_id': 1, 'product': 'IF'}}
        svc._product_cache = {'IF': {'multiplier': 300}}
        return svc

    def test_clear_rebuild_failure_keeps_old(self):
        svc = self._make_svc_with_data()
        with patch.object(svc, '_rebuild_instrument_cache', side_effect=RuntimeError("fail")):
            svc.clear_instrument_cache()
        assert 'IF2606' in svc._instrument_id_to_internal_id


class TestLoadInstrumentList:
    def test_load_from_param_cache_dict(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {}
        svc._instrument_meta_by_id = {}
        params = {
            'future_instruments': ['IF2606'],
            'option_instruments': {'IH': ['IH2606']},
            'futures_metadata': {'IF2606': {'internal_id': 1, 'product': 'IF'}},
            'options_metadata': {},
        }
        with patch.object(svc, 'cache_instrument_info', return_value={'instrument_id': 'IF2606', 'internal_id': 1}):
            result = svc.load_instrument_list(params, source='param_cache')
            assert result is not None
            assert 'IF2606' in result['futures_list']

    def test_load_empty_raises(self):
        svc = InstrumentCacheService()
        svc.init_instrument_cache()
        svc._lock = threading.Lock()
        svc._instrument_id_to_internal_id = {}
        svc._instrument_meta_by_id = {}
        params = {'future_instruments': [], 'option_instruments': {}}
        with pytest.raises((ValueError, RuntimeError)):
            svc.load_instrument_list(params, source='param_cache')