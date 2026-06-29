# MODULE_ID: M1-056-TL-TEST
"""
test_signal_sources_abc.py — 并列信号源A/B/C测试

依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md
"""
import time
import pytest
from unittest.mock import MagicMock, patch

from ali2026v3_trading.data.three_layer_sort import (
    IntraProductSorter,
    AsymmetricDecay,
    GreeksHardFilter,
    InterProductClusterSorter,
    GlobalSorter,
)


class TestAsymmetricDecay:
    def test_decay_reduces_stale_counts(self):
        decay = AsymmetricDecay('financial')
        counts = {'CR': 100, 'CF': 100, 'WR': 50, 'WF': 50, 'Other': 10}
        now = time.time()
        last_update = {s: now - 600 for s in counts}
        decayed = decay.decay_counts(counts, last_update, now)
        assert decayed['cr'] < 100
        assert decayed['wr'] < 50

    def test_fresh_counts_no_decay(self):
        decay = AsymmetricDecay('financial')
        counts = {'CR': 100, 'CF': 80, 'WR': 30, 'WF': 20, 'Other': 10}
        now = time.time()
        last_update = {s: now for s in counts}
        decayed = decay.decay_counts(counts, last_update, now)
        assert abs(decayed['cr'] - 100.0) < 0.01

    def test_asymmetric_cr_slower_than_wr(self):
        decay = AsymmetricDecay('financial')
        counts = {'CR': 100, 'CF': 100, 'WR': 100, 'WF': 100, 'Other': 100}
        now = time.time()
        last_update = {s: now - 300 for s in counts}
        decayed = decay.decay_counts(counts, last_update, now)
        assert decayed['cr'] > decayed['wr']
        assert decayed['cf'] > decayed['wf']

    def test_default_sector(self):
        decay = AsymmetricDecay('unknown_sector')
        counts = {'CR': 100, 'CF': 100, 'WR': 100, 'WF': 100, 'Other': 100}
        now = time.time()
        last_update = {s: now for s in counts}
        decayed = decay.decay_counts(counts, last_update, now)
        assert decayed['cr'] == 100.0


class TestGreeksHardFilter:
    def test_disabled_passes_all(self):
        f = GreeksHardFilter(enabled=False)
        assert f.filter(0.01, 0.5, -0.5, 10.0, 'front_month') is True

    def test_enabled_rejects_extreme_delta(self):
        f = GreeksHardFilter(enabled=True)
        assert f.filter(0.05, 0.03, -0.5, 100.0, 'front_month') is False
        assert f.filter(0.90, 0.03, -0.5, 100.0, 'front_month') is False

    def test_enabled_accepts_normal(self):
        f = GreeksHardFilter(enabled=True)
        assert f.filter(0.45, 0.03, -0.5, 100.0, 'front_month') is True

    def test_batch_filter(self):
        f = GreeksHardFilter(enabled=True)
        opts = [
            {'delta': 0.45, 'gamma': 0.03, 'theta': -0.5, 'option_price': 100.0, 'month_type': 'quarter_month'},
            {'delta': 0.05, 'gamma': 0.03, 'theta': -0.5, 'option_price': 100.0, 'month_type': 'quarter_month'},
        ]
        result = f.filter_batch(opts)
        assert len(result) == 1


class TestIntraProductSorter:
    def _make_month_data(self, months=None):
        if months is None:
            months = [
                {'CR': 120, 'CF': 80, 'WR': 30, 'WF': 20, 'Other': 10},
                {'CR': 100, 'CF': 60, 'WR': 40, 'WF': 30, 'Other': 20},
            ]
        now = time.time()
        result = []
        for i, counts in enumerate(months):
            result.append({
                'month': f'260{i+7}',
                'counts': counts,
                'last_update': {s: now for s in counts},
                'greeks': {'delta': 0.5, 'gamma': 0.03, 'theta': -0.5, 'vega': 0.05},
                'option_price': 100.0,
                'month_type': 'front_month' if i == 0 else 'next_month',
                'liquidity': 0.8,
            })
        return result

    def test_rank_returns_signal_source_a(self):
        sorter = IntraProductSorter()
        result = sorter.rank('IF', self._make_month_data())
        assert result['signal_source'] == 'A'
        assert result['product_id'] == 'IF'
        assert result['best_month'] is not None
        assert result['tier'] in (1, 2, 3, 4)
        assert result['month_count'] == 2
        assert len(result['candidates']) == 2

    def test_rank_empty_data(self):
        sorter = IntraProductSorter()
        result = sorter.rank('IF', [])
        assert result['signal_source'] == 'A'
        assert result['best_month'] is None
        assert result['tier'] == 4

    def test_pure_mode_no_decay(self):
        sorter = IntraProductSorter(pure_mode=True)
        data = self._make_month_data()
        result = sorter.rank('IF', data)
        assert result['signal_source'] == 'A'

    def test_correct_up_pct_positive(self):
        sorter = IntraProductSorter()
        data = self._make_month_data([
            {'CR': 120, 'CF': 80, 'WR': 30, 'WF': 20, 'Other': 10},
        ])
        result = sorter.rank('IF', data)
        assert result['correct_up_pct'] > 0.5

    def test_tier4_for_weak_signal(self):
        sorter = IntraProductSorter()
        data = self._make_month_data([
            {'CR': 10, 'CF': 10, 'WR': 80, 'WF': 80, 'Other': 20},
        ])
        result = sorter.rank('IF', data)
        assert result['tier'] == 4

    def test_candidates_sorted_by_score_desc(self):
        sorter = IntraProductSorter()
        data = self._make_month_data([
            {'CR': 120, 'CF': 80, 'WR': 30, 'WF': 20, 'Other': 10},
            {'CR': 50, 'CF': 30, 'WR': 80, 'WF': 70, 'Other': 20},
        ])
        result = sorter.rank('IF', data)
        scores = [c['score'] for c in result['candidates']]
        assert scores == sorted(scores, reverse=True)


class TestInterProductClusterSorter:
    def _make_product_results(self):
        return [
            {'product_id': 'IF', 'best_score': 0.245, 'correct_up_pct': 0.72, 'tier': 1, 'best_month': '2607'},
            {'product_id': 'IH', 'best_score': 0.180, 'correct_up_pct': 0.65, 'tier': 2, 'best_month': '2607'},
            {'product_id': 'IC', 'best_score': 0.150, 'correct_up_pct': 0.55, 'tier': 2, 'best_month': '2608'},
        ]

    def test_rank_returns_signal_source_b(self):
        sorter = InterProductClusterSorter()
        result = sorter.rank('index_futures', self._make_product_results())
        assert result['signal_source'] == 'B'
        assert result['cluster_id'] == 'index_futures'
        assert len(result['sorted_products']) == 3
        assert result['group_accuracy'] > 0

    def test_rank_empty(self):
        sorter = InterProductClusterSorter()
        result = sorter.rank('test', [])
        assert result['sorted_products'] == []

    def test_resonance_calculated(self):
        sorter = InterProductClusterSorter()
        result = sorter.rank('test', self._make_product_results())
        for p in result['sorted_products']:
            assert 'resonance' in p

    def test_veto_marks_low_resonance(self):
        results = [
            {'product_id': 'A', 'best_score': 0.3, 'correct_up_pct': 0.8, 'tier': 1},
            {'product_id': 'B', 'best_score': 0.1, 'correct_up_pct': 0.2, 'tier': 3},
        ]
        sorter = InterProductClusterSorter(enable_resonance_veto=True, resonance_veto_threshold=0.80)
        result = sorter.rank('test', results)
        vetoed = [p for p in result['sorted_products'] if p.get('vetoed')]
        assert len(vetoed) >= 1

    def test_sorted_by_score_b_desc(self):
        sorter = InterProductClusterSorter()
        result = sorter.rank('test', self._make_product_results())
        scores = [p['score_b'] for p in result['sorted_products']]
        assert scores == sorted(scores, reverse=True)


class TestGlobalSorter:
    def _make_all_results(self):
        return [
            {'product_id': 'IF', 'best_score': 0.32, 'correct_up_pct': 0.72, 'tier': 1, 'best_month': '2607'},
            {'product_id': 'AU', 'best_score': 0.28, 'correct_up_pct': 0.65, 'tier': 1, 'best_month': '2608'},
            {'product_id': 'CF', 'best_score': 0.15, 'correct_up_pct': 0.45, 'tier': 3, 'best_month': '2609'},
        ]

    def test_rank_returns_signal_source_c(self):
        sorter = GlobalSorter()
        result = sorter.rank(self._make_all_results())
        assert result['signal_source'] == 'C'
        assert result['total_products'] == 3
        assert len(result['sorted_products']) == 3

    def test_rank_empty(self):
        sorter = GlobalSorter()
        result = sorter.rank([])
        assert result['total_products'] == 0

    def test_global_rank_assigned(self):
        sorter = GlobalSorter()
        result = sorter.rank(self._make_all_results())
        ranks = [p['global_rank'] for p in result['sorted_products']]
        assert ranks == [1, 2, 3]

    def test_percentile_calculated(self):
        sorter = GlobalSorter(enable_percentile=True)
        result = sorter.rank(self._make_all_results())
        for p in result['sorted_products']:
            assert p['global_percentile'] is not None
            assert 0 < p['global_percentile'] <= 1.0

    def test_sorted_by_best_score_desc(self):
        sorter = GlobalSorter()
        result = sorter.rank(self._make_all_results())
        scores = [p['best_score'] for p in result['sorted_products']]
        assert scores == sorted(scores, reverse=True)

    def test_percentile_disabled(self):
        sorter = GlobalSorter(enable_percentile=False)
        result = sorter.rank(self._make_all_results())
        for p in result['sorted_products']:
            assert p['global_percentile'] is None


class TestThreeSourcesIndependent:
    """方案文档1.1核心原则：三个信号源并列独立"""

    def test_a_b_c_independent(self):
        month_data = [
            {'month': '2607', 'counts': {'CR': 120, 'CF': 80, 'WR': 30, 'WF': 20, 'Other': 10},
             'last_update': {s: time.time() for s in ('CR', 'CF', 'WR', 'WF', 'Other')},
             'greeks': {'delta': 0.5, 'gamma': 0.03, 'theta': -0.5, 'vega': 0.05},
             'option_price': 100.0, 'month_type': 'front_month', 'liquidity': 0.8},
        ]
        product_results = [
            {'product_id': 'IF', 'best_score': 0.245, 'correct_up_pct': 0.72, 'tier': 1, 'best_month': '2607'},
        ]

        result_a = IntraProductSorter().rank('IF', month_data)
        result_b = InterProductClusterSorter().rank('index', product_results)
        result_c = GlobalSorter().rank(product_results)

        assert result_a['signal_source'] == 'A'
        assert result_b['signal_source'] == 'B'
        assert result_c['signal_source'] == 'C'

    def test_a_output_feeds_b_and_c(self):
        sorter_a = IntraProductSorter()
        result_a = sorter_a.rank('IF', [
            {'month': '2607', 'counts': {'CR': 100, 'CF': 80, 'WR': 20, 'WF': 10, 'Other': 5},
             'last_update': {s: time.time() for s in ('CR', 'CF', 'WR', 'WF', 'Other')},
             'greeks': {'delta': 0.5, 'gamma': 0.03, 'theta': -0.5, 'vega': 0.05},
             'option_price': 100.0, 'month_type': 'front_month', 'liquidity': 0.8},
        ])

        result_b = InterProductClusterSorter().rank('test', [result_a])
        result_c = GlobalSorter().rank([result_a])

        assert len(result_b['sorted_products']) == 1
        assert len(result_c['sorted_products']) == 1
        assert result_b['sorted_products'][0]['product_id'] == 'IF'
        assert result_c['sorted_products'][0]['product_id'] == 'IF'


# ══════════════════════════════════════════════════════════════════
# 集成测试：signal_source 参数传递链路
# ══════════════════════════════════════════════════════════════════

class TestSignalSourceParameterPassing:
    """验证 signal_source 参数从 strategy_business_layer → t_type_service → select_otm_targets_signal_sources 的完整传递"""

    def test_business_layer_passes_signal_source_a(self):
        from unittest.mock import MagicMock, patch
        provider = MagicMock()
        provider._is_running = True
        provider._is_paused = False
        provider._state = MagicMock()
        provider._state.value = 'RUNNING'
        provider._trading_lock = MagicMock()
        provider._trading_lock.acquire.return_value = True
        provider._signal_source = 'A'
        provider._resolve_open_reason.return_value = 'CORRECT_RESONANCE'
        provider._check_ecosystem_exclusion.return_value = True

        t_type = MagicMock()
        t_type.select_otm_targets_signal_sources.return_value = [
            {'instrument_id': 'IO2607-C-4200', 'lots': 2, 'open_reason': 'CORRECT_RESONANCE',
             'signal_source': 'A', 'tier': 1, 'direction': 'BUY'}
        ]
        provider.t_type_service = t_type

        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        biz = StrategyBusinessLayer.__new__(StrategyBusinessLayer)
        biz._provider = provider

        with patch.object(biz, 'ensure_order_service'), \
             patch.object(biz, 'ensure_position_service'):
            try:
                biz._trading_cycle()
            except Exception:
                pass

        t_type.select_otm_targets_signal_sources.assert_called_once_with(signal_source='A')

    def test_business_layer_passes_signal_source_b(self):
        from unittest.mock import MagicMock, patch
        provider = MagicMock()
        provider._is_running = True
        provider._is_paused = False
        provider._state = MagicMock()
        provider._state.value = 'RUNNING'
        provider._trading_lock = MagicMock()
        provider._trading_lock.acquire.return_value = True
        provider._signal_source = 'B'
        provider._resolve_open_reason.return_value = 'CORRECT_RESONANCE'
        provider._check_ecosystem_exclusion.return_value = True

        t_type = MagicMock()
        t_type.select_otm_targets_signal_sources.return_value = [
            {'instrument_id': 'IO2607-C-4200', 'lots': 2, 'open_reason': 'CORRECT_RESONANCE',
             'signal_source': 'B', 'tier': 2, 'direction': 'BUY'}
        ]
        provider.t_type_service = t_type

        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        biz = StrategyBusinessLayer.__new__(StrategyBusinessLayer)
        biz._provider = provider

        with patch.object(biz, 'ensure_order_service'), \
             patch.object(biz, 'ensure_position_service'):
            try:
                biz._trading_cycle()
            except Exception:
                pass

        t_type.select_otm_targets_signal_sources.assert_called_once_with(signal_source='B')

    def test_business_layer_passes_signal_source_c(self):
        from unittest.mock import MagicMock, patch
        provider = MagicMock()
        provider._is_running = True
        provider._is_paused = False
        provider._state = MagicMock()
        provider._state.value = 'RUNNING'
        provider._trading_lock = MagicMock()
        provider._trading_lock.acquire.return_value = True
        provider._signal_source = 'C'
        provider._resolve_open_reason.return_value = 'CORRECT_RESONANCE'
        provider._check_ecosystem_exclusion.return_value = True

        t_type = MagicMock()
        t_type.select_otm_targets_signal_sources.return_value = [
            {'instrument_id': 'IO2607-C-4200', 'lots': 2, 'open_reason': 'CORRECT_RESONANCE',
             'signal_source': 'C', 'tier': 1, 'direction': 'BUY'}
        ]
        provider.t_type_service = t_type

        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        biz = StrategyBusinessLayer.__new__(StrategyBusinessLayer)
        biz._provider = provider

        with patch.object(biz, 'ensure_order_service'), \
             patch.object(biz, 'ensure_position_service'):
            try:
                biz._trading_cycle()
            except Exception:
                pass

        t_type.select_otm_targets_signal_sources.assert_called_once_with(signal_source='C')

    def test_business_layer_legacy_falls_back_to_by_volume(self):
        from unittest.mock import MagicMock, patch
        provider = MagicMock()
        provider._is_running = True
        provider._is_paused = False
        provider._state = MagicMock()
        provider._state.value = 'RUNNING'
        provider._trading_lock = MagicMock()
        provider._trading_lock.acquire.return_value = True
        provider._signal_source = 'legacy'
        provider._resolve_open_reason.return_value = 'CORRECT_RESONANCE'
        provider._check_ecosystem_exclusion.return_value = True

        t_type = MagicMock()
        t_type.select_otm_targets_by_volume.return_value = [
            {'instrument_id': 'IO2607-C-4200', 'lots': 2, 'open_reason': 'CORRECT_RESONANCE'}
        ]
        provider.t_type_service = t_type

        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        biz = StrategyBusinessLayer.__new__(StrategyBusinessLayer)
        biz._provider = provider

        with patch.object(biz, 'ensure_order_service'), \
             patch.object(biz, 'ensure_position_service'):
            try:
                biz._trading_cycle()
            except Exception:
                pass

        t_type.select_otm_targets_by_volume.assert_called_once()
        t_type.select_otm_targets_signal_sources.assert_not_called()


class TestSelectOtmTargetsSignalSourceRouting:
    """验证 select_otm_targets_signal_sources 内部根据 signal_source 参数选择A/B/C输出"""

    def _build_mock_cache(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        import threading

        cache = WidthCacheQueryService()
        cache._lock = threading.RLock()
        cache._status_counts = {
            1: {
                '2607': {
                    'CALL': {'correct_rise': 120, 'wrong_rise': 30, 'correct_fall': 80, 'wrong_fall': 20, 'other': 10},
                    'PUT': {'correct_rise': 50, 'wrong_rise': 50, 'correct_fall': 50, 'wrong_fall': 50, 'other': 10},
                },
            },
        }
        cache._future_initialized = {1: True}
        cache._future_rising = {1: True}
        cache._sort_buckets = {
            1: {
                '2607': [
                    {'instrument_id': 'IO2607-C-4200', 'volume': 800, 'delta': 0.45},
                ],
            },
        }
        cache._is_main_month = {1: {'2607': True}}
        cache._sorter_a = None
        cache._sorter_b = None
        cache._sorter_c = None

        mock_params = MagicMock()
        mock_params.get_instrument_meta.return_value = {'product': 'IF', 'exchange': 'CFFEX'}
        cache._get_params = MagicMock(return_value=mock_params)
        cache._get_scoring_months = MagicMock(return_value=['2607'])
        cache._backfill_is_main_month = MagicMock()
        cache.select_from_sort_bucket = MagicMock(return_value=[
            {'instrument_id': 'IO2607-C-4200', 'volume': 800, 'delta': 0.45}
        ])

        return cache

    @patch('ali2026v3_trading.position.position_service.get_position_service', side_effect=ImportError)
    def test_signal_source_c_uses_global_sorter(self, mock_pos):
        cache = self._build_mock_cache()
        targets = cache.select_otm_targets_signal_sources(signal_source='C')
        assert len(targets) >= 1
        assert targets[0]['signal_source'] == 'C'

    @patch('ali2026v3_trading.position.position_service.get_position_service', side_effect=ImportError)
    def test_signal_source_b_uses_cluster_sorter(self, mock_pos):
        cache = self._build_mock_cache()
        targets = cache.select_otm_targets_signal_sources(signal_source='B')
        assert len(targets) >= 1
        assert targets[0]['signal_source'] == 'B'

    @patch('ali2026v3_trading.position.position_service.get_position_service', side_effect=ImportError)
    def test_signal_source_a_uses_intra_sorter(self, mock_pos):
        cache = self._build_mock_cache()
        targets = cache.select_otm_targets_signal_sources(signal_source='A')
        assert len(targets) >= 1
        assert targets[0]['signal_source'] == 'A'

    @patch('ali2026v3_trading.position.position_service.get_position_service', side_effect=ImportError)
    def test_default_signal_source_is_c(self, mock_pos):
        cache = self._build_mock_cache()
        targets = cache.select_otm_targets_signal_sources()
        assert len(targets) >= 1
        assert targets[0]['signal_source'] == 'C'

    @patch('ali2026v3_trading.position.position_service.get_position_service', side_effect=ImportError)
    def test_auto_falls_back_to_c(self, mock_pos):
        cache = self._build_mock_cache()
        targets = cache.select_otm_targets_signal_sources(signal_source='auto')
        assert len(targets) >= 1
        assert targets[0]['signal_source'] == 'C'

    @patch('ali2026v3_trading.position.position_service.get_position_service', side_effect=ImportError)
    def test_no_data_returns_empty(self, mock_pos):
        cache = self._build_mock_cache()
        cache._status_counts = {}
        cache._future_initialized = {}
        for src in ('A', 'B', 'C'):
            targets = cache.select_otm_targets_signal_sources(signal_source=src)
            assert targets == []


class TestSignalSourceInitFromParamPool:
    """验证 StrategyBusinessLayer.__init__ 从参数池读取 tl_signal_source"""

    def test_reads_tl_signal_source_from_param_pool(self):
        from unittest.mock import MagicMock, patch
        mock_pp = MagicMock()
        mock_pp.get_params.return_value = {'tl_signal_source': 'A'}

        provider = MagicMock()
        with patch('ali2026v3_trading.strategy.strategy_business_layer.get_param_provider', return_value=mock_pp):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            biz = StrategyBusinessLayer(provider)

        assert provider._signal_source == 'A'

    def test_falls_back_to_config_when_param_pool_missing(self):
        from unittest.mock import MagicMock, patch
        mock_pp = MagicMock()
        mock_pp.get_params.return_value = {}

        provider = MagicMock()
        with patch('ali2026v3_trading.strategy.strategy_business_layer.get_param_provider', return_value=mock_pp):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            biz = StrategyBusinessLayer(provider)

        assert provider._signal_source == 'C'

    def test_falls_back_to_config_on_exception(self):
        from unittest.mock import MagicMock, patch

        provider = MagicMock()
        with patch('ali2026v3_trading.strategy.strategy_business_layer.get_param_provider', side_effect=Exception("no pool")):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            biz = StrategyBusinessLayer(provider)

        assert provider._signal_source == 'C'

    def test_invalid_value_falls_back_to_config(self):
        from unittest.mock import MagicMock, patch
        mock_pp = MagicMock()
        mock_pp.get_params.return_value = {'tl_signal_source': 'INVALID'}

        provider = MagicMock()
        with patch('ali2026v3_trading.strategy.strategy_business_layer.get_param_provider', return_value=mock_pp):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            biz = StrategyBusinessLayer(provider)

        assert provider._signal_source == 'C'