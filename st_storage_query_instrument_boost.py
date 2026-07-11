# MODULE_ID: M2-585
import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService


class TestStorageInstrumentServiceInit:
    def test_init(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        assert svc._base is base

    def test_getattr_delegated(self):
        base = MagicMock()
        base._lock = MagicMock()
        svc = StorageInstrumentService(base)
        assert svc._lock is base._lock

    def test_getattr_non_delegated(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        with pytest.raises(AttributeError):
            svc.nonexistent_attr


class TestGetOptionChainByFutureId:
    def test_invalid_future_id(self):
        base = MagicMock()
        base._get_info_by_id.return_value = None
        svc = StorageInstrumentService(base)
        with pytest.raises(ValueError, match="无效的期货ID"):
            svc.get_option_chain_by_future_id(1)

    def test_not_future_type(self):
        base = MagicMock()
        base._get_info_by_id.return_value = {'type': 'option'}
        svc = StorageInstrumentService(base)
        with pytest.raises(ValueError, match="无效的期货ID"):
            svc.get_option_chain_by_future_id(1)

    def test_valid_future_with_options(self):
        base = MagicMock()
        base._get_info_by_id.return_value = {
            'type': 'future', 'instrument_id': 'IF2606'
        }
        base._data_service.query.return_value.to_pylist.return_value = [
            {'internal_id': 10, 'instrument_id': 'IO2606-C-3500', 'option_type': 'C', 'strike_price': 3500.0},
        ]
        svc = StorageInstrumentService(base)
        result = svc.get_option_chain_by_future_id(1)
        assert 'future' in result
        assert 'options' in result
        assert len(result['options']) == 1

    def test_future_no_instrument_id_queries_db(self):
        base = MagicMock()
        base._get_info_by_id.return_value = {'type': 'future', 'instrument_id': None}
        base._data_service.query.return_value.to_pylist.return_value = [
            {'instrument_id': 'IF2606'},
        ]
        svc = StorageInstrumentService(base)
        with patch.object(svc, '_data_service') as mock_ds:
            mock_ds.query.return_value.to_pylist.return_value = [
                {'internal_id': 10, 'instrument_id': 'IO2606-C-3500', 'option_type': 'C', 'strike_price': 3500.0},
            ]
            result = svc.get_option_chain_by_future_id(1)
            assert result is not None


class TestGetLatestUnderlying:
    def test_returns_none(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        result = svc.get_latest_underlying('IF', '2606')
        assert result is None


class TestLoadAllInstruments:
    def test_no_strategy_no_params(self):
        result = StorageInstrumentService.load_all_instruments()
        assert result == ([], {})

    def test_with_strategy_instance(self):
        mock_strategy = MagicMock()
        mock_strategy.subscribed_instruments = {}
        result = StorageInstrumentService.load_all_instruments(strategy_instance=mock_strategy)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_with_params(self):
        mock_params = MagicMock()
        mock_params.instrument_ids = ['IF2606']
        with patch('ali2026v3_trading.data.storage_query_instrument.SubscriptionManager') as mock_sm:
            mock_sm.parse_option.side_effect = Exception("not option")
            result = StorageInstrumentService.load_all_instruments(params=mock_params)
            assert isinstance(result, tuple)


class TestRegisterInstrument:
    def test_empty_instrument_id(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        with pytest.raises(ValueError, match="instrument_id 不能为空"):
            svc.register_instrument('')

    def test_already_in_params_cache(self):
        base = MagicMock()
        base._params_service.get_instrument_meta_by_id.return_value = {'internal_id': 42}
        svc = StorageInstrumentService(base)
        result = svc.register_instrument('IF2606')
        assert result == 42


class TestDeleteInstrument:
    def test_not_found(self):
        base = MagicMock()
        base._get_instrument_info.return_value = None
        svc = StorageInstrumentService(base)
        svc.delete_instrument('IF2606')

    def test_delete_future(self):
        base = MagicMock()
        base._get_instrument_info.return_value = {
            'type': 'future', 'internal_id': 1, 'kline_table': 'kline_future_1', 'tick_table': 'tick_future_1'
        }
        base._get_info_internal_id.return_value = 1
        base._data_service.query.return_value.to_pylist.return_value = []
        svc = StorageInstrumentService(base)
        svc.delete_instrument('IF2606')


class TestBatchAddFutureInstruments:
    def test_batch_add(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        with patch.object(svc, 'register_instrument', return_value=1):
            svc.batch_add_future_instruments([
                {'instrument_id': 'IF2606', 'exchange': 'CFFEX'},
                {'instrument_id': 'IH2606', 'exchange': 'CFFEX'},
            ])

    def test_batch_add_empty(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        svc.batch_add_future_instruments([])

    def test_batch_add_missing_id(self):
        base = MagicMock()
        svc = StorageInstrumentService(base)
        with patch.object(svc, 'register_instrument', return_value=1):
            svc.batch_add_future_instruments([{'exchange': 'CFFEX'}])