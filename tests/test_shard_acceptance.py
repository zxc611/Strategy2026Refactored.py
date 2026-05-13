import unittest
import threading
import time
from unittest.mock import MagicMock, patch


class TestShardRouter(unittest.TestCase):
    def test_deterministic_routing(self):
        from ali2026v3_trading.shared_utils import ShardRouter
        r1 = ShardRouter(shard_count=16)
        r2 = ShardRouter(shard_count=16)
        for inst_id in ['IF2506', 'al2605', 'm2605', 'IC2506', 'rb2510']:
            self.assertEqual(r1.route(inst_id), r2.route(inst_id),
                             f"路由不一致: {inst_id}")

    def test_same_product_same_shard(self):
        from ali2026v3_trading.shared_utils import ShardRouter
        router = ShardRouter(shard_count=16)
        self.assertEqual(router.route('IF2506'), router.route('IF2509'))
        self.assertEqual(router.route('al2605'), router.route('al2609'))
        self.assertEqual(router.route('m2605'), router.route('m2609'))

    def test_binding_map_audit(self):
        from ali2026v3_trading.shared_utils import ShardRouter
        router = ShardRouter(shard_count=16)
        router.route('IF2506')
        router.route('al2605')
        bm = router.get_binding_map()
        self.assertIn('IF', bm)
        self.assertIn('AL', bm)
        audit = router.get_routing_audit_line()
        self.assertIn('Shard-', audit)

    def test_consistent_hash_migration(self):
        from ali2026v3_trading.shared_utils import ShardRouter
        router = ShardRouter(shard_count=16)
        for inst_id in ['IF2506', 'al2605', 'm2605', 'IC2506', 'rb2510', 'cu2607']:
            router.route(inst_id)
        migration = router.reconfigure(8)
        self.assertIsInstance(migration, dict)


class TestShardRoutingConsistency(unittest.TestCase):
    def test_extract_product_code(self):
        from ali2026v3_trading.shared_utils import extract_product_code
        self.assertEqual(extract_product_code('IF2506'), 'IF')
        self.assertEqual(extract_product_code('al2605'), 'AL')
        self.assertEqual(extract_product_code('IO2506-C-4000'), 'IO')
        self.assertEqual(extract_product_code('m2605'), 'M')
        self.assertEqual(extract_product_code(''), '')

    def test_two_layer_same_shard(self):
        from ali2026v3_trading.shared_utils import ShardRouter
        router = ShardRouter(shard_count=16)
        test_ids = ['IF2506', 'al2605', 'm2605', 'IC2506', 'rb2510',
                     'IO2506-C-4000', 'AG2606', 'cu2607', 'T2506', 'au2606']
        for inst_id in test_ids:
            idx1 = router.route(inst_id)
            idx2 = router.route(inst_id)
            self.assertEqual(idx1, idx2, f"同实例两次路由不一致: {inst_id}")


class TestDegradedDispatch(unittest.TestCase):
    def test_degraded_method_exists(self):
        from ali2026v3_trading.strategy_tick_handler import TickHandlerMixin
        self.assertTrue(hasattr(TickHandlerMixin, '_dispatch_tick_degraded'))

    def test_degraded_does_not_drop_tick(self):
        from ali2026v3_trading.strategy_tick_handler import TickHandlerMixin
        mixin = TickHandlerMixin.__new__(TickHandlerMixin)
        mixin._TICK_SHARD_COUNT = 16
        from ali2026v3_trading.shared_utils import ShardRouter
        mixin._shard_router = ShardRouter(shard_count=16)
        mixin._shard_buffers = {}
        mixin._shard_locks = {}
        mixin._shard_buffers_lock = threading.Lock()
        mixin._degraded_tick_count = 0
        tick = {'instrument_id': 'IF2506', 'last_price': 4000.0, 'ts': time.time()}
        mixin._dispatch_tick_degraded(tick, 'IF2506', 4000.0, 100, 'CFFEX')
        self.assertEqual(mixin._degraded_tick_count, 1)
        found = False
        for buf in mixin._shard_buffers.values():
            if tick in buf:
                found = True
        self.assertTrue(found, "降级分发后tick应在shard_buffer中")


class TestHistoricalMaxWorkers(unittest.TestCase):
    def test_max_workers_wired(self):
        from ali2026v3_trading.strategy_historical import load_historical_klines_with_stop
        import inspect
        sig = inspect.signature(load_historical_klines_with_stop)
        self.assertIn('max_workers', sig.parameters)


class TestConfigShardParams(unittest.TestCase):
    def test_shard_config_exists(self):
        from ali2026v3_trading.config_service import DEFAULT_PARAM_TABLE
        self.assertIn('tick_shard_count', DEFAULT_PARAM_TABLE)
        self.assertIn('tick_writer_count', DEFAULT_PARAM_TABLE)
        self.assertIn('tick_shard_queue_capacity', DEFAULT_PARAM_TABLE)
        self.assertIn('spill_enabled', DEFAULT_PARAM_TABLE)
        self.assertEqual(DEFAULT_PARAM_TABLE['tick_shard_count'], 16)
        self.assertEqual(DEFAULT_PARAM_TABLE['tick_writer_count'], 6)


class TestDiagnosisShardDimension(unittest.TestCase):
    def test_on_storage_enqueue_accepts_shard_idx(self):
        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
        import inspect
        sig = inspect.signature(DiagnosisProbeManager.on_storage_enqueue)
        self.assertIn('shard_idx', sig.parameters)

    def test_shard_enqueue_counts_exists(self):
        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
        self.assertTrue(hasattr(DiagnosisProbeManager, '_shard_enqueue_counts'))


if __name__ == '__main__':
    unittest.main()
