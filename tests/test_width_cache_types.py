# MODULE_ID: M2-607
"""测试: data/width_cache_types.py"""
import pytest
from ali2026v3_trading.data.width_cache_types import (
    SortEntry, _NoOpDiagnosisProbeManager,
    MAX_OPTION_CACHE_SIZE, MAX_INSTRUMENT_ID_MAP_SIZE, MAX_FUTURE_CACHE_SIZE,
    MAX_STATUS_COUNTS_SIZE, MAX_SORT_BUCKETS_FUTURES,
)


class TestSortEntry:
    def test_create_call_option(self):
        e = SortEntry.create(
            internal_id=1, instrument_id='AL2401C20000', option_type='CALL',
            strike_price=20000.0, volume=100, price=50.0,
            sync_flag=True, m_price=19500.0, month='2401', product='AL',
        )
        assert e.sync_flag is True
        assert e.option_type == 'CALL'

    def test_create_put_option(self):
        e = SortEntry.create(
            internal_id=2, instrument_id='AL2401P20000', option_type='PUT',
            strike_price=20000.0, volume=50, price=30.0,
            sync_flag=False, m_price=20500.0, month='2401', product='AL',
        )
        assert e.sync_flag is False

    def test_call_zero_m_price(self):
        e = SortEntry.create(
            internal_id=3, instrument_id='IF2401C4000', option_type='CALL',
            strike_price=4000.0, volume=10, price=100.0,
            sync_flag=True, m_price=0.0, month='2401', product='IF',
        )
        assert e is not None

    def test_put_zero_m_price(self):
        e = SortEntry.create(
            internal_id=4, instrument_id='IF2401P4000', option_type='PUT',
            strike_price=4000.0, volume=10, price=100.0,
            sync_flag=False, m_price=0.0, month='2401', product='IF',
        )
        assert e is not None

    def test_sort_sync_flag_priority(self):
        e_sync = SortEntry.create(1, 'A', 'CALL', 100.0, 10, 5.0, True, 100.0, '2401', 'AL')
        e_nosync = SortEntry.create(2, 'B', 'CALL', 100.0, 10, 5.0, False, 100.0, '2401', 'AL')
        assert e_sync < e_nosync

    def test_sort_volume_priority(self):
        e_high = SortEntry.create(1, 'A', 'CALL', 100.0, 200, 5.0, True, 100.0, '2401', 'AL')
        e_low = SortEntry.create(2, 'B', 'CALL', 100.0, 50, 5.0, True, 100.0, '2401', 'AL')
        assert e_high < e_low

    def test_sort_strike_distance_priority(self):
        e_near = SortEntry.create(1, 'A', 'CALL', 105.0, 10, 5.0, True, 100.0, '2401', 'AL')
        e_far = SortEntry.create(2, 'B', 'CALL', 120.0, 10, 5.0, True, 100.0, '2401', 'AL')
        assert e_near < e_far

    def test_zero_volume_default(self):
        e = SortEntry.create(1, 'A', 'CALL', 100.0, 0, 5.0, True, 100.0, '2401', 'AL')
        assert e is not None

    def test_attributes(self):
        e = SortEntry.create(1, 'AL2401C20000', 'CALL', 20000.0, 100, 50.0, True, 19500.0, '2401', 'AL')
        assert e.internal_id == 1
        assert e.instrument_id == 'AL2401C20000'
        assert e.strike_price == 20000.0
        assert e.volume == 100
        assert e.price == 50.0
        assert e.month == '2401'
        assert e.product == 'AL'


class TestNoOpDiagnosisProbeManager:
    def test_startup_step_context_manager(self):
        mgr = _NoOpDiagnosisProbeManager()
        with mgr.startup_step("test_step"):
            pass

    def test_mark_startup_event(self):
        mgr = _NoOpDiagnosisProbeManager()
        mgr.mark_startup_event("test_event", detail="test")

    def test_static_methods(self):
        assert callable(_NoOpDiagnosisProbeManager.startup_step)
        assert callable(_NoOpDiagnosisProbeManager.mark_startup_event)


class TestModuleConstants:
    def test_max_option_cache_size(self):
        assert MAX_OPTION_CACHE_SIZE > 0

    def test_max_instrument_id_map_size(self):
        assert MAX_INSTRUMENT_ID_MAP_SIZE > 0

    def test_max_future_cache_size(self):
        assert MAX_FUTURE_CACHE_SIZE > 0

    def test_max_status_counts_size(self):
        assert MAX_STATUS_COUNTS_SIZE > 0

    def test_max_sort_buckets_futures(self):
        assert MAX_SORT_BUCKETS_FUTURES > 0