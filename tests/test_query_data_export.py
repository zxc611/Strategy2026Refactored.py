# MODULE_ID: M2-471
"""测试: data/query_data_export.py"""
import pytest
from unittest.mock import MagicMock, patch
from data.query_data_export import DataExportService


class TestDataExportService:
    def test_creation(self):
        svc = DataExportService(MagicMock())
        assert svc is not None

    def test_get_instrument_summary_callable(self):
        assert callable(DataExportService.get_instrument_summary)

    def test_diagnose_contract_callable(self):
        assert callable(DataExportService._diagnose_contract)

    def test_get_instrument_summary_no_data(self):
        svc = DataExportService(MagicMock())
        result = svc.get_instrument_summary('NONEXISTENT')
        assert isinstance(result, dict)

    def test_diagnose_contract_no_data(self):
        svc = DataExportService(MagicMock())
        result = svc._diagnose_contract('NONEXISTENT', is_future=True)
        assert isinstance(result, dict)

    def test_export_kline_to_csv_callable(self):
        svc = DataExportService(MagicMock())
        assert callable(getattr(svc, 'export_kline_to_csv', None))

    def test_export_tick_to_csv_callable(self):
        svc = DataExportService(MagicMock())
        assert callable(getattr(svc, 'export_tick_to_csv', None))