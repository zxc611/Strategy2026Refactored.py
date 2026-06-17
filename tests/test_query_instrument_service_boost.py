# MODULE_ID: M2-472
import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.data.query_instrument_service import InstrumentQueryService


class TestInstrumentQueryServiceInit:
    def test_init(self):
        storage = MagicMock()
        svc = InstrumentQueryService(storage)
        assert svc._storage is storage

    def test_get_params_service_explicit(self):
        ps = MagicMock()
        storage = MagicMock()
        svc = InstrumentQueryService(storage, params_service=ps)
        assert svc._get_params_service() is ps

    def test_get_info_internal_id(self):
        storage = MagicMock()
        svc = InstrumentQueryService(storage)
        assert svc._get_info_internal_id({'internal_id': 42}) == 42
        assert svc._get_info_internal_id({'internal_id': '7'}) == 7
        assert svc._get_info_internal_id({}) is None
        assert svc._get_info_internal_id(None) is None


class TestNormalizeInstruments:
    def test_basic(self):
        result = InstrumentQueryService._normalize_instruments(['IF2606', 'IH2606'])
        assert result == ['IF2606', 'IH2606']

    def test_dedup(self):
        result = InstrumentQueryService._normalize_instruments(['IF2606', 'IF2606'])
        assert result == ['IF2606']

    def test_strip_exchange_prefix(self):
        result = InstrumentQueryService._normalize_instruments(['CFFEX.IF2606'])
        assert result == ['IF2606']

    def test_empty_and_none(self):
        result = InstrumentQueryService._normalize_instruments([None, '', 'IF2606'])
        assert result == ['IF2606']


class TestNormalizeOptionsDict:
    def test_basic(self):
        result = InstrumentQueryService._normalize_options_dict({'IH': ['IH2606-C-3500']})
        assert 'IH' in result

    def test_strip_prefix(self):
        result = InstrumentQueryService._normalize_options_dict({'CFFEX.IH': ['CFFEX.IH2606-C-3500']})
        assert 'IH' in result

    def test_empty(self):
        result = InstrumentQueryService._normalize_options_dict({})
        assert result == {}

    def test_none_values(self):
        result = InstrumentQueryService._normalize_options_dict({'IH': None})
        assert result == {}


class TestCountOptionContracts:
    def test_count(self):
        result = InstrumentQueryService._count_option_contracts(
            {'IH': ['IH2606-C-3500', 'IH2606-P-3500']}
        )
        assert result == 2

    def test_empty(self):
        assert InstrumentQueryService._count_option_contracts({}) == 0
        assert InstrumentQueryService._count_option_contracts(None) == 0


class TestInferExchangeFromId:
    def test_empty_string(self):
        storage = MagicMock()
        svc = InstrumentQueryService(storage)
        assert svc.infer_exchange_from_id('') == 'UNKNOWN'

    def test_none(self):
        storage = MagicMock()
        svc = InstrumentQueryService(storage)
        assert svc.infer_exchange_from_id(None) == 'UNKNOWN'

    def test_from_params_service_cache(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_instrument_meta_by_id.return_value = {'exchange': 'CFFEX'}
        svc = InstrumentQueryService(storage, params_service=ps)
        result = svc.infer_exchange_from_id('IF2606')
        assert result == 'CFFEX'

    def test_auto_exchange_resolves(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_instrument_meta_by_id.return_value = {'exchange': 'AUTO'}
        svc = InstrumentQueryService(storage, params_service=ps)
        with patch('ali2026v3_trading.config.config_service.resolve_product_exchange', return_value='CFFEX'):
            result = svc.infer_exchange_from_id('IF2606')
            assert result == 'CFFEX'


class TestGetRegisteredInstrumentIds:
    def test_no_filter(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_all_instrument_ids.return_value = ['IF2606', 'IH2606']
        svc = InstrumentQueryService(storage, params_service=ps)
        result = svc.get_registered_instrument_ids()
        assert len(result) == 2

    def test_with_filter(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_instrument_meta_by_id.side_effect = lambda x: {'product': 'IF'} if x == 'IF2606' else None
        svc = InstrumentQueryService(storage, params_service=ps)
        result = svc.get_registered_instrument_ids(['IF2606', 'IH2606'])
        assert 'IF2606' in result

    def test_empty_list(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_all_instrument_ids.return_value = []
        svc = InstrumentQueryService(storage, params_service=ps)
        result = svc.get_registered_instrument_ids()
        assert result == []


class TestEnsureRegisteredInstruments:
    def test_all_registered(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_instrument_meta_by_id.return_value = {'internal_id': 1}
        svc = InstrumentQueryService(storage, params_service=ps)
        result = svc.ensure_registered_instruments(['IF2606'])
        assert result['registered_count'] == 1
        assert result['missing_count'] == 0

    def test_missing_instruments(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_instrument_meta_by_id.return_value = None
        svc = InstrumentQueryService(storage, params_service=ps)
        with patch.object(svc, 'infer_exchange_from_id', return_value='CFFEX'):
            storage.register_instrument.return_value = 1
            result = svc.ensure_registered_instruments(['IF2606'])
            assert result['missing_count'] == 1

    def test_register_failure(self):
        storage = MagicMock()
        ps = MagicMock()
        ps.get_instrument_meta_by_id.return_value = None
        svc = InstrumentQueryService(storage, params_service=ps)
        with patch.object(svc, 'infer_exchange_from_id', return_value='CFFEX'):
            storage.register_instrument.side_effect = ValueError("fail")
            result = svc.ensure_registered_instruments(['IF2606'])
            assert result['failed_count'] == 1


class TestDeriveUnderlyingFutures:
    def test_derive(self):
        with patch('ali2026v3_trading.infra.subscription_manager.SubscriptionManager') as mock_sm:
            mock_sm.parse_option.return_value = {'product': 'IO', 'year_month': '2606'}
            result = InstrumentQueryService._derive_underlying_futures({'IO': ['IO2606-C-3500']})
            assert isinstance(result, list)

    def test_empty(self):
        result = InstrumentQueryService._derive_underlying_futures({})
        assert result == []