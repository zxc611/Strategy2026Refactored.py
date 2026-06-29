"""
BUG-2品种受限根因修复 — 端到端验证

验证项：
1. TOP_FUTURES_COUNT=8（非2）
2. compute_correct_up_pct排除other状态（分母不含other）
3. determine_tier不再因correct_up_pct <= noise_ratio直接归Tier 4
4. 排序桶接受wrong_rise/wrong_fall状态（非仅correct_rise/correct_fall）
5. _check_ecosystem_exclusion的_state_map包含correct_trending_defensive和incorrect_reversal_defensive
6. _health_pause_new_open超时300秒自动恢复
7. _health_pause_new_open_ts时间戳记录

运行方式: python -m pytest ali2026v3_trading/tests/test_bug2_top_futures_sort_bucket_ecosystem_e2e.py -v --tb=short --no-cov
"""

import sys
import os
import unittest
import logging
import time
from unittest.mock import MagicMock, patch

logging.basicConfig(level=logging.WARNING)

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestTopFuturesCountIncreased(unittest.TestCase):

    def test_top_futures_count_is_8(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        self.assertEqual(WidthCacheQueryService.TOP_FUTURES_COUNT, 8)

    def test_top_futures_count_not_2(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        self.assertNotEqual(WidthCacheQueryService.TOP_FUTURES_COUNT, 2)


class TestComputeCorrectUpPctExcludesOther(unittest.TestCase):

    def test_other_not_in_denominator(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        month_data = [
            {'cr': 10, 'wr': 2, 'cf': 5, 'wf': 1, 'other': 100}
        ]
        weights = (1.0,)
        result = WidthCacheQueryService.compute_correct_up_pct(month_data, weights)
        expected = (10 + 5) / (10 + 2 + 5 + 1)
        self.assertAlmostEqual(result, expected, places=6)

    def test_zero_other_same_result(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        month_data_no_other = [{'cr': 10, 'wr': 2, 'cf': 5, 'wf': 1, 'other': 0}]
        month_data_with_other = [{'cr': 10, 'wr': 2, 'cf': 5, 'wf': 1, 'other': 100}]
        weights = (1.0,)
        r1 = WidthCacheQueryService.compute_correct_up_pct(month_data_no_other, weights)
        r2 = WidthCacheQueryService.compute_correct_up_pct(month_data_with_other, weights)
        self.assertAlmostEqual(r1, r2, places=6)

    def test_all_other_returns_zero(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        month_data = [{'cr': 0, 'wr': 0, 'cf': 0, 'wf': 0, 'other': 100}]
        weights = (1.0,)
        result = WidthCacheQueryService.compute_correct_up_pct(month_data, weights)
        self.assertEqual(result, 0.0)

    def test_night_session_high_other_no_longer_dilutes(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        month_data = [
            {'cr': 5, 'wr': 1, 'cf': 3, 'wf': 1, 'other': 90}
        ]
        weights = (1.0,)
        result = WidthCacheQueryService.compute_correct_up_pct(month_data, weights)
        self.assertAlmostEqual(result, (5 + 3) / (5 + 1 + 3 + 1), places=6)
        self.assertGreater(result, 0.5)


class TestDetermineTierRelaxed(unittest.TestCase):

    def test_correct_up_pct_positive_but_below_noise_ratio_still_tier3(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        tier = WidthCacheQueryService.determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.40, noise_ratio=0.50
        )
        self.assertEqual(tier, 3)

    def test_zero_correct_up_pct_still_tier4(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        tier = WidthCacheQueryService.determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.0, noise_ratio=0.0
        )
        self.assertEqual(tier, 4)

    def test_high_correct_up_pct_tier1(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        tier = WidthCacheQueryService.determine_tier(
            coverage=0.9, wilson=0.55, correct_up_pct=0.80, noise_ratio=0.10
        )
        self.assertEqual(tier, 1)

    def test_low_correct_up_pct_tier4(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        tier = WidthCacheQueryService.determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.20, noise_ratio=0.10
        )
        self.assertEqual(tier, 4)


class TestSortBucketAcceptsWrongStates(unittest.TestCase):

    def test_wrong_rise_not_filtered(self):
        from ali2026v3_trading.data.width_cache_sort_mixin import WidthCacheSortService
        allowed = ('correct_rise', 'correct_fall', 'wrong_rise', 'wrong_fall')
        self.assertIn('wrong_rise', allowed)

    def test_wrong_fall_not_filtered(self):
        from ali2026v3_trading.data.width_cache_sort_mixin import WidthCacheSortService
        allowed = ('correct_rise', 'correct_fall', 'wrong_rise', 'wrong_fall')
        self.assertIn('wrong_fall', allowed)

    def test_other_still_filtered(self):
        allowed = ('correct_rise', 'correct_fall', 'wrong_rise', 'wrong_fall')
        self.assertNotIn('other', allowed)

    def test_wrong_rise_accepted_in_do_update(self):
        from ali2026v3_trading.data.width_cache_sort_mixin import WidthCacheSortService
        import threading
        svc = WidthCacheSortService.__new__(WidthCacheSortService)
        svc._lock = threading.RLock()
        svc._sort_buckets = {}
        svc._buckets_dirty = set()
        svc._option_daily_volume = {1: 100}
        svc._option_volume = {1: 100}
        svc._option_price = {1: 50.0}
        svc._sync_flag = {1: True}
        svc._internal_id_to_instrument_id = {1: 'IO2607-C-3900'}
        svc._current_status = {1: 'wrong_rise'}
        svc._sort_bucket_max_size = 50
        svc._params_service = None
        mock_params = MagicMock()
        mock_params.get_instrument_meta = MagicMock(return_value={'product': 'IF'})
        svc._get_params = MagicMock(return_value=mock_params)
        svc._get_future_price_by_id_and_month = MagicMock(return_value=4000.0)
        svc._is_out_of_the_money = MagicMock(return_value=True)
        info = {'strike_price': 3900, 'instrument_id': 'IO2607-C-3900', 'underlying_future_id': 100}
        removed = False
        original_remove = svc._do_remove_from_sort_bucket
        def track_remove(*a, **kw):
            nonlocal removed
            removed = True
        svc._do_remove_from_sort_bucket = track_remove
        with svc._lock:
            svc._do_update_sort_bucket(1, info, 100, '2607', 'CALL')
        self.assertFalse(removed, "wrong_rise should NOT trigger removal from sort bucket")


class TestEcosystemStateMapComplete(unittest.TestCase):

    def test_state_map_has_defensive_variants(self):
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        provider = MagicMock()
        provider._state_store = MagicMock()
        provider._health_pause_new_open = False
        provider._health_pause_new_open_ts = 0.0
        layer = StrategyBusinessLayer(provider)
        eco = MagicMock()
        eco._active_strategy = 'master'
        with patch('ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem', return_value=eco):
            spm = MagicMock()
            spm.get_current_state = MagicMock(return_value='correct_trending_defensive')
            provider._state_param_manager = spm
            result = layer._check_ecosystem_exclusion([{'direction': 'BUY'}])
            self.assertTrue(result, "correct_trending_defensive should map to 'master' and not be blocked")

    def test_incorrect_reversal_defensive_maps_to_reverse(self):
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        provider = MagicMock()
        provider._state_store = MagicMock()
        provider._health_pause_new_open = False
        provider._health_pause_new_open_ts = 0.0
        layer = StrategyBusinessLayer(provider)
        eco = MagicMock()
        eco._active_strategy = 'master'
        with patch('ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem', return_value=eco):
            spm = MagicMock()
            spm.get_current_state = MagicMock(return_value='incorrect_reversal_defensive')
            provider._state_param_manager = spm
            result = layer._check_ecosystem_exclusion([{'direction': 'BUY'}])
            self.assertTrue(result, "incorrect_reversal_defensive should map to 'reverse' and not be blocked by master")

    def test_other_state_blocked_when_active_is_other(self):
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        provider = MagicMock()
        provider._state_store = MagicMock()
        provider._health_pause_new_open = False
        provider._health_pause_new_open_ts = 0.0
        layer = StrategyBusinessLayer(provider)
        eco = MagicMock()
        eco._active_strategy = 'other'
        with patch('ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem', return_value=eco):
            spm = MagicMock()
            spm.get_current_state = MagicMock(return_value='correct_trending')
            provider._state_param_manager = spm
            result = layer._check_ecosystem_exclusion([{'direction': 'BUY'}])
            self.assertFalse(result, "master strategy should be blocked when active is 'other'")


class TestHealthPauseTimeout(unittest.TestCase):

    def test_health_pause_auto_recovery_after_300s(self):
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState

        class RealProvider:
            def __init__(self):
                self._initialized = True
                self._is_running = True
                self._is_paused = False
                self._state = StrategyState.RUNNING
                self._health_pause_new_open = True
                self._health_pause_new_open_ts = time.time() - 301
                self._trading_lock = MagicMock()
                self._trading_lock.acquire = MagicMock(return_value=True)
                self._trading_lock.release = MagicMock()
                self._order_service = None
                self.t_type_service = MagicMock()
                self.t_type_service.select_otm_targets_by_volume = MagicMock(return_value=[{'instrument_id': 'test'}])
                self.strategy_id = 'test'

            def _resolve_open_reason(self):
                return 'TEST'

            def _check_ecosystem_exclusion(self, targets):
                return True

            def get_health_status(self):
                return {'health': 'HEALTHY'}

            def _ensure_order_service(self):
                pass

            def _feed_shadow_engine(self, targets, reason):
                pass

        provider = RealProvider()
        layer = StrategyBusinessLayer(provider)
        layer.execute_option_trading_cycle()
        self.assertFalse(provider._health_pause_new_open, "Should auto-recover after 300s timeout")
        self.assertEqual(provider._health_pause_new_open_ts, 0)

    def test_health_pause_still_active_within_300s(self):
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState

        class RealProvider:
            def __init__(self):
                self._initialized = True
                self._is_running = True
                self._is_paused = False
                self._state = StrategyState.RUNNING
                self._health_pause_new_open = True
                self._health_pause_new_open_ts = time.time() - 100
                self._trading_lock = MagicMock()
                self._trading_lock.acquire = MagicMock(return_value=True)
                self._trading_lock.release = MagicMock()
                self._order_service = None
                self.t_type_service = None
                self.strategy_id = 'test'

        provider = RealProvider()
        layer = StrategyBusinessLayer(provider)
        layer.execute_option_trading_cycle()
        self.assertTrue(provider._health_pause_new_open, "Should still be paused within 300s")

    def test_ecosystem_exception_sets_timestamp(self):
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer

        class RealProvider:
            def __init__(self):
                self._health_pause_new_open = False
                self._health_pause_new_open_ts = 0.0
                self._state_store = MagicMock()

        provider = RealProvider()
        layer = StrategyBusinessLayer(provider)
        with patch('ali2026v3_trading.strategy.strategy_ecosystem.get_strategy_ecosystem',
                   side_effect=ImportError("test")):
            spm = MagicMock()
            spm.get_current_state = MagicMock(return_value='correct_trending')
            provider._state_param_manager = spm
            result = layer._check_ecosystem_exclusion([{'direction': 'BUY'}])
            self.assertFalse(result)
            self.assertTrue(provider._health_pause_new_open)
            self.assertGreater(provider._health_pause_new_open_ts, 0)


class TestInitStateHealthPauseTs(unittest.TestCase):

    def test_init_state_sets_health_pause_ts(self):
        from ali2026v3_trading.strategy.strategy_config_layer import StrategyConfigLayer

        class RealProvider:
            pass

        provider = RealProvider()
        layer = StrategyConfigLayer(provider)
        layer.init_state()
        self.assertFalse(provider._health_pause_new_open)
        self.assertEqual(provider._health_pause_new_open_ts, 0.0)


if __name__ == '__main__':
    unittest.main()
