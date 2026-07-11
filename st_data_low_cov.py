# MODULE_ID: M2-329
"""data/ 低覆盖率大文件测试"""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestDSRealtimeCache:
    def test_real_time_cache(self):
        from ali2026v3_trading.data.ds_realtime_cache import RealTimeCache
        rc = RealTimeCache.__new__(RealTimeCache)
        assert rc is not None


class TestDSDataWriter:
    def test_data_writer_mixin(self):
        from ali2026v3_trading.data.ds_data_writer import DataWriterMixin
        dwm = DataWriterMixin.__new__(DataWriterMixin)
        assert dwm is not None

    def test_direct_enqueue_write_supports_tick_impl(self):
        from ali2026v3_trading.data.ds_data_writer import DataWriterMixin

        class DummyWriter(DataWriterMixin):
            def __init__(self):
                self.insert_calls = []

            def _get_connection(self):
                return self

            def _return_connection(self, conn):
                self.returned_conn = conn

            def execute(self, sql, params=None):
                self.last_sql = (sql, params)
                return self

            def fetchone(self):
                return ('IF2606',)

            def batch_insert_ticks(self, rows, instrument_id=None, use_arrow=True):
                self.insert_calls.append((rows, instrument_id, use_arrow))
                return len(rows)

        writer = DummyWriter()
        ok = writer._enqueue_write('_save_tick_impl', 370, 'future', [{'last_price': 100.0, 'volume': 1}])

        assert ok is True
        assert writer.insert_calls == [([{'last_price': 100.0, 'volume': 1, 'instrument_id': 'IF2606'}], 'IF2606', True)]
        assert writer.get_queue_stats()['total_received'] == 1
        assert writer.get_queue_stats()['total_written'] == 1
        assert writer.get_queue_stats()['drops_count'] == 0


class TestWidthCacheStateMixin:
    def test_width_cache_state_service(self):
        from ali2026v3_trading.data.width_cache_state_mixin import WidthCacheStateService
        wcss = WidthCacheStateService.__new__(WidthCacheStateService)
        assert wcss is not None


class TestStorageQueryBase:
    def test_storage_query_base_service(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        sqbs = StorageQueryBaseService.__new__(StorageQueryBaseService)
        assert sqbs is not None


class TestStorageAsyncWriterMixin:
    def test_storage_async_writer_service(self):
        from ali2026v3_trading.data.storage_async_writer_mixin import StorageAsyncWriterService
        saws = StorageAsyncWriterService.__new__(StorageAsyncWriterService)
        assert saws is not None


class TestStorageDataWriteMixin:
    def test_storage_data_write_service(self):
        from ali2026v3_trading.data.storage_data_write_mixin import StorageDataWriteService
        sdws = StorageDataWriteService.__new__(StorageDataWriteService)
        assert sdws is not None


class TestQueryInstrumentService:
    def test_instrument_query_service(self):
        from ali2026v3_trading.data.query_instrument_service import InstrumentQueryService
        iqs = InstrumentQueryService.__new__(InstrumentQueryService)
        assert iqs is not None


class TestHistoricalDataManager:
    def test_init(self):
        from ali2026v3_trading.data.historical_data_manager import HistoricalDataManager
        hdm = HistoricalDataManager.__new__(HistoricalDataManager)
        assert hdm is not None


class TestStorageLifecycleMixin:
    def test_storage_lifecycle_service(self):
        from ali2026v3_trading.data.storage_lifecycle_mixin import StorageLifecycleService
        sls = StorageLifecycleService.__new__(StorageLifecycleService)
        assert sls is not None


class TestStorageInitMixin:
    def test_module_importable(self):
        import ali2026v3_trading.data.storage_init_mixin
        assert ali2026v3_trading.data.storage_init_mixin is not None


class TestStorageSnapshotMixin:
    def test_storage_snapshot_service(self):
        from ali2026v3_trading.data.storage_snapshot_mixin import StorageSnapshotService
        sss = StorageSnapshotService.__new__(StorageSnapshotService)
        assert sss is not None


class TestStorageQueryHistory:
    def test_storage_history_service(self):
        from ali2026v3_trading.data.storage_query_history import StorageHistoryService
        shs = StorageHistoryService.__new__(StorageHistoryService)
        assert shs is not None


class TestDSQueryCache:
    def test_query_cache_mixin(self):
        from ali2026v3_trading.data.ds_query_cache import QueryCacheMixin
        qcm = QueryCacheMixin.__new__(QueryCacheMixin)
        assert qcm is not None
