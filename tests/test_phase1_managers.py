# MODULE_ID: M2-464
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.strategy.instrument_service import InstrumentManager
from ali2026v3_trading.data.historical_data_manager import HistoricalDataManager


class TestInstrumentManager:
    def test_extract_contract_year_month_with_mock(self):
        mgr = InstrumentManager()
        mock_ps = MagicMock()
        mock_ps.get_instrument_meta_by_id.return_value = {'year_month': '2501'}
        with patch('ali2026v3_trading.config.params_service.get_params_service', return_value=mock_ps):
            result = mgr.extract_contract_year_month('IF2501')
            assert result == '2501'

    def test_extract_contract_year_month_empty(self):
        mgr = InstrumentManager()
        mock_ps = MagicMock()
        mock_ps.get_instrument_meta_by_id.return_value = None
        with patch('ali2026v3_trading.config.params_service.get_params_service', return_value=mock_ps), \
             patch('ali2026v3_trading.infra.subscription_manager.SubscriptionManager.is_option', return_value=False):
            result = mgr.extract_contract_year_month('')
            assert result is None

    def test_extract_contract_year_month_none(self):
        mgr = InstrumentManager()
        mock_ps = MagicMock()
        mock_ps.get_instrument_meta_by_id.return_value = None
        with patch('ali2026v3_trading.config.params_service.get_params_service', return_value=mock_ps), \
             patch('ali2026v3_trading.infra.subscription_manager.SubscriptionManager.is_option', return_value=False):
            result = mgr.extract_contract_year_month(None)
            assert result is None

    def test_count_option_contracts_empty(self):
        mgr = InstrumentManager()
        assert mgr.count_option_contracts({}) == 0

    def test_count_option_contracts_with_data(self):
        mgr = InstrumentManager()
        data = {'IO': ['IO2501C4000', 'IO2501P4000'], 'MO': ['MO2501C3000']}
        assert mgr.count_option_contracts(data) == 3

    def test_normalize_cached_futures_empty(self):
        mgr = InstrumentManager()
        result = mgr.normalize_cached_futures([])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_normalize_cached_options_empty(self):
        mgr = InstrumentManager()
        result = mgr.normalize_cached_options({})
        assert isinstance(result, dict)
        assert len(result) == 0


class TestHistoricalDataManager:
    def test_init(self):
        mgr = HistoricalDataManager()
        assert mgr._historical_load_in_progress is False
        assert mgr._historical_load_started is False
        assert mgr._historical_kline_result is None

    def test_init_historical(self):
        mgr = HistoricalDataManager()
        mgr._historical_load_in_progress = True
        mgr.init_historical()
        assert mgr._historical_load_in_progress is False

    def test_filter_historical_month_scope_empty(self):
        mock_store = MagicMock()
        mock_store.get.return_value = None
        mock_group = MagicMock()
        mock_registry = MagicMock()
        mock_registry.invoke.return_value = None
        mock_group.get_registry.return_value = mock_registry
        mgr = HistoricalDataManager(state_store=mock_store, callback_group=mock_group)
        result = mgr.filter_historical_month_scope([])
        assert isinstance(result, tuple)
        assert len(result) == 3
        assert isinstance(result[0], list)

    def test_filter_historical_month_scope_filters_old(self):
        mock_store = MagicMock()
        mock_store.get.return_value = '2601'
        mock_group = MagicMock()
        mock_registry = MagicMock()
        year_month_map = {'old_contract': '2501', 'new_contract': '2606'}
        mock_registry.invoke.side_effect = lambda *a, **kw: [year_month_map.get(a[0], '2606')] if a else [None]
        mock_group.get_registry.return_value = mock_registry
        mgr = HistoricalDataManager(state_store=mock_store, callback_group=mock_group)
        result = mgr.filter_historical_month_scope(['old_contract', 'new_contract'])
        filtered, removed, min_ym = result
        assert removed == 1
        assert 'new_contract' in filtered

    def test_reset_historical_state_for_restart(self):
        mgr = HistoricalDataManager()
        mgr._historical_stop_flag = True
        mgr._historical_load_started = True
        mgr.reset_historical_state_for_restart()
        assert mgr._historical_stop_flag is False
        assert mgr._historical_load_started is False
