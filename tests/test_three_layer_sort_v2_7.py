# MODULE_ID: M2-555
"""
test_three_layer_sort_v2_7.py — v2.5-v2.8 落地方案验证

验证范围（依据 docs/audit/三层期权五态排序方案_最终落地方案V2_20260624.md）：
  §15 (v2.5): AlphaEngine + ClusterView + GlobalView, 等权默认, 20日Rank,
              多候选 gap/confidence, SORTER_CONFIG, V7反馈槽
  §16 (v2.6): ResonanceEngine 可插拔，默认关闭
  §17 (v2.7): D_term_structure / D_momentum(period=1) / D_type_balance,
              方向一致性检查, 冷启动回退, resonance_weight=0.5
  §18.4:      direction_conflict_frequency 日频监控预埋
  §21 (v2.8): scoring_scheme 方案开关, D/Q 正交分解, confidence 修正,
              entropy/concentration/direction_bias, output_mode 参数化

验证结果：pytest 执行，全部断言 PASS 为 v2.8 落地合格标准。
"""
import sys
import os
import math
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest

from ali2026v3_trading.data.three_layer_sort import (
    AlphaEngine, ClusterView, GlobalView,
    RankNormalizer,
    compute_rank_confidence_default, compute_score_delta_to_next,
    compute_orthogonal_decomposition, compute_confidence_v28, compute_entropy_features,
    ResonanceEngine,
    compute_term_structure_direction, compute_type_resonance_direction,
    compute_momentum_direction, compute_resonance_with_fallback, compute_final_score,
)
from ali2026v3_trading.config.tvf_param_loader import SORTER_CONFIG


# =====================================================================
# Helpers
# =====================================================================

def _make_month_data(month: str, cr: float, cf: float, wr: float, wf: float, other: float = 0.0):
    return {
        'month': month,
        'counts': {'CR': cr, 'CF': cf, 'WR': wr, 'WF': wf, 'Other': other},
        'last_update': {s: time.time() for s in ('CR', 'CF', 'WR', 'WF', 'Other')},
    }


def _make_product_history(engine: AlphaEngine, product_id: str, scores: list):
    """Inject fake history into RankNormalizer (for testing percentile)."""
    from collections import deque
    engine._rank_normalizer._history[product_id] = deque(scores, maxlen=engine._rank_window_days)


# =====================================================================
# v2.5 §15: AlphaEngine 核心打分引擎
# =====================================================================

class TestAlphaEngineV25:
    """v2.5 核心排序功能验证。"""

    def test_default_sorter_config_values(self):
        """SORTER_CONFIG 默认参数与 v2.5/v2.7 文档一致。"""
        assert SORTER_CONFIG['rank_window_days'] == 20
        assert SORTER_CONFIG['month_weights'] is None
        assert SORTER_CONFIG['enable_rank_normalize'] is True
        assert SORTER_CONFIG['rank_normalize_clip'] == (-3.0, 3.0)
        assert SORTER_CONFIG['output_candidates_max'] == 5
        assert SORTER_CONFIG['rank_confidence_method'] == 'percentile'

    def test_alpha_engine_default_resonance_disabled(self):
        """默认构造的 AlphaEngine 共振模块必须关闭（安全闸门）。"""
        engine = AlphaEngine()
        assert engine._resonance_engine is None or not engine._resonance_engine.enabled
        assert not (engine._resonance_engine and engine._resonance_engine.enabled)

    def test_equal_weights_default(self):
        """默认 month_weights=None 时按月份数量等权。"""
        engine = AlphaEngine()
        weights = engine._resolve_weights(3)
        assert weights == pytest.approx((1 / 3, 1 / 3, 1 / 3))
        weights5 = engine._resolve_weights(5)
        assert weights5 == pytest.approx((0.2, 0.2, 0.2, 0.2, 0.2))

    def test_custom_weights_normalized(self):
        """自定义权重向量按长度归一化。"""
        cfg = dict(SORTER_CONFIG)
        cfg['month_weights'] = [0.4, 0.3, 0.2]
        engine = AlphaEngine(sorter_config=cfg)
        weights = engine._resolve_weights(3)
        assert sum(weights) == pytest.approx(1.0)
        assert weights[0] == pytest.approx(0.4 / 0.9, abs=1e-6)

    def test_rank_normalizer_window_20_days(self):
        """RankNormalizer 默认窗口为 20 日。"""
        engine = AlphaEngine()
        assert engine._rank_window_days == 20
        assert engine._rank_normalizer._window_days == 20

    def test_output_contains_v25_fields(self):
        """AlphaEngine 输出包含 v2.5 强制字段。"""
        engine = AlphaEngine()
        data = [
            _make_month_data('2507', 80, 10, 5, 5),
            _make_month_data('2508', 70, 15, 10, 5),
        ]
        result = engine.rank('IF', data)
        assert result['signal_source'] == 'A'
        assert result['product_id'] == 'IF'
        assert 'product_score' in result
        assert 'product_score_ts' in result
        assert 'month_candidates' in result
        assert isinstance(result['month_candidates'], list)
        assert len(result['month_candidates']) == 2

    def test_month_candidates_sorted_by_net_score_desc(self):
        """月份候选按 net_score 降序排列。"""
        engine = AlphaEngine(pure_mode=True)
        data = [
            _make_month_data('2508', 30, 10, 10, 10),  # net_score = (40-20)/60=0.333
            _make_month_data('2507', 80, 10, 5, 5),    # net_score = (90-10)/100=0.8
        ]
        result = engine.rank('IF', data)
        cands = result['month_candidates']
        assert cands[0]['month'] == '2507'
        assert cands[1]['month'] == '2508'
        assert cands[0]['net_score'] > cands[1]['net_score']

    def test_score_delta_to_next_field(self):
        """月份候选包含 score_delta_to_next（末位 None）。"""
        engine = AlphaEngine(pure_mode=True)
        data = [
            _make_month_data('2507', 80, 10, 5, 5),
            _make_month_data('2508', 60, 10, 10, 10),
            _make_month_data('2509', 40, 10, 15, 15),
        ]
        result = engine.rank('IF', data)
        cands = result['month_candidates']
        assert len(cands) == 3
        assert cands[0]['score_delta_to_next'] is not None
        assert cands[2]['score_delta_to_next'] is None

    def test_rank_confidence_percentile_default(self):
        """rank_confidence 默认 percentile 方法取值 [0,1]。"""
        engine = AlphaEngine(pure_mode=True)
        _make_product_history(engine, 'IF', [0.1] * 5 + [0.2] * 5 + [0.3] * 5 + [0.4] * 5)
        data = [
            _make_month_data('2507', 80, 10, 5, 5),   # net_score ~ 0.8
            _make_month_data('2508', 30, 10, 10, 10),  # net_score ~ 0.167
        ]
        result = engine.rank('IF', data)
        for cand in result['month_candidates']:
            assert 'rank_confidence' in cand
            assert 0.0 <= cand['rank_confidence'] <= 1.0

    def test_historical_percentile_present_when_history_enough(self):
        """历史足够时 historical_percentile 不为 None。"""
        engine = AlphaEngine(pure_mode=True)
        _make_product_history(engine, 'IF', [0.1] * 10)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        assert result['historical_percentile'] is not None
        assert 0.0 <= result['historical_percentile'] <= 1.0

    def test_backward_compatible_aliases(self):
        """旧类名 IntraProductSorter 等仍可用。"""
        from ali2026v3_trading.data.three_layer_sort import IntraProductSorter, InterProductClusterSorter, GlobalSorter
        assert IntraProductSorter is AlphaEngine
        assert InterProductClusterSorter is ClusterView
        assert GlobalSorter is GlobalView


# =====================================================================
# v2.5 §15: ClusterView / GlobalView 视图层
# =====================================================================

class TestClusterViewGlobalViewV25:
    """v2.5 视图层验证。"""

    def test_clusterview_output_format(self):
        """ClusterView 输出包含 sorted_products 与 group_accuracy。"""
        cv = ClusterView()
        alpha_results = [
            {'product_id': 'IF', 'product_score': 0.5, 'product_score_ts': 0.6, 'correct_up_pct': 0.7, 'tier': 1},
            {'product_id': 'IC', 'product_score': 0.3, 'product_score_ts': 0.2, 'correct_up_pct': 0.6, 'tier': 2},
        ]
        result = cv.rank('financial', alpha_results)
        assert result['signal_source'] == 'B'
        assert result['cluster_id'] == 'financial'
        assert 'group_accuracy' in result
        assert len(result['sorted_products']) == 2

    def test_globalview_output_format(self):
        """GlobalView 输出包含 sorted_products 与 total_products，并填充 product_score_cs。"""
        gv = GlobalView()
        alpha_results = [
            {'product_id': 'IF', 'product_score': 0.5, 'product_score_ts': 0.6, 'correct_up_pct': 0.7, 'tier': 1},
            {'product_id': 'IC', 'product_score': 0.3, 'product_score_ts': 0.2, 'correct_up_pct': 0.6, 'tier': 2},
        ]
        result = gv.rank(alpha_results)
        assert result['signal_source'] == 'C'
        assert result['total_products'] == 2
        assert len(result['sorted_products']) == 2
        for p in result['sorted_products']:
            assert 'product_score_cs' in p

    def test_globalview_sorts_by_final_score_when_resonance_enabled(self):
        """共振启用时 GlobalView 按 final_score 排序。"""
        gv = GlobalView()
        alpha_results = [
            {'product_id': 'IF', 'product_score_ts': 0.6, 'final_score': 0.8},
            {'product_id': 'IC', 'product_score_ts': 0.7, 'final_score': 0.5},
        ]
        result = gv.rank(alpha_results)
        sorted_ids = [p['product_id'] for p in result['sorted_products']]
        assert sorted_ids[0] == 'IF'


# =====================================================================
# v2.7 §17: ResonanceEngine 共振模块
# =====================================================================

class TestResonanceEngineV27:
    """v2.7 共振模块验证。"""

    def test_default_disabled(self):
        """ResonanceEngine 默认关闭。"""
        engine = ResonanceEngine(config=SORTER_CONFIG)
        assert not engine.enabled

    def test_dimensions_default(self):
        """默认维度为 D_term_structure + D_momentum + D_type_balance。"""
        cfg = dict(SORTER_CONFIG)
        cfg['resonance_enabled'] = True
        engine = ResonanceEngine(config=cfg)
        assert engine._dimensions_list == ['D_term_structure', 'D_momentum', 'D_type_balance']

    def test_resonance_weight_default_0_5(self):
        """v2.7 默认共振权重为 0.5（等权）。"""
        assert SORTER_CONFIG['resonance_weight'] == 0.5
        cfg = dict(SORTER_CONFIG)
        cfg['resonance_enabled'] = True
        engine = ResonanceEngine(config=cfg)
        assert engine._resonance_weight == 0.5

    def test_term_structure_direction_same_sign(self):
        """D_term_structure: 近月/远月同向时返回 ±1。"""
        months = [
            {'net_score': 0.5},
            {'net_score': 0.3},
            {'net_score': 0.2},
        ]
        assert compute_term_structure_direction(months) == 1
        months_neg = [
            {'net_score': -0.5},
            {'net_score': -0.3},
        ]
        assert compute_term_structure_direction(months_neg) == -1

    def test_term_structure_direction_opposite_sign(self):
        """D_term_structure: 近月/远月反向时返回 0。"""
        months = [
            {'net_score': 0.5},
            {'net_score': -0.3},
        ]
        assert compute_term_structure_direction(months) == 0

    def test_momentum_direction_period_1(self):
        """D_momentum: period=1 使用当日收益率。"""
        assert compute_momentum_direction(105, 100, period=1) == 1
        assert compute_momentum_direction(95, 100, period=1) == -1
        assert compute_momentum_direction(100, 100, period=1) == 0

    def test_type_resonance_direction_bull(self):
        """D_type_balance: Call 多头 + Put 空头 → +1。"""
        data = {
            'call_counts': {'CR': 60, 'CF': 10, 'WR': 10, 'WF': 10, 'Other': 10},
            'put_counts': {'CR': 60, 'CF': 10, 'WR': 10, 'WF': 10, 'Other': 10},
        }
        # Call long pct = 70/100 = 0.7; Put short pct = 70/100 = 0.7 -> bullish
        assert compute_type_resonance_direction(data, threshold=0.5) == 1

    def test_type_resonance_direction_bear(self):
        """D_type_balance: Call 空头 + Put 多头 → -1。"""
        data = {
            'call_counts': {'CR': 10, 'CF': 10, 'WR': 60, 'WF': 10, 'Other': 10},
            'put_counts': {'CR': 10, 'CF': 60, 'WR': 10, 'WF': 10, 'Other': 10},
        }
        # Call short pct = 70/100; Put long pct = 70/100 -> bearish
        assert compute_type_resonance_direction(data, threshold=0.5) == -1

    def test_cold_start_fallback_redistributes_weights(self):
        """冷启动回退：缺失维度权重重新分配。"""
        dimensions = {'D_term_structure': 1, 'D_momentum': 0, 'D_type_balance': -1}
        weights = {'D_term_structure': 1.0, 'D_momentum': 1.0, 'D_type_balance': 1.0}
        result = compute_resonance_with_fallback(dimensions, weights)
        assert result['fallback_applied'] is True
        assert result['active_count'] == 2
        assert result['resonance_score'] == 0.0  # +1 and -1 balance out

    def test_compute_final_score_direction_conflict(self):
        """方向冲突：异号时 final_score=0 并标记 conflict。"""
        result = compute_final_score(0.5, -0.8, 0.5)
        assert result['direction_conflict'] is True
        assert result['final_score'] == 0.0
        assert 'product_score_ts' in result
        assert 'resonance_score' in result

    def test_compute_final_score_same_direction(self):
        """方向一致时按权重融合。"""
        result = compute_final_score(0.5, 0.8, 0.5)
        assert result['direction_conflict'] is False
        assert result['final_score'] == pytest.approx(0.65, abs=1e-6)

    def test_resonance_output_format_v27(self):
        """ResonanceEngine 输出包含 v2.7 定稿字段。"""
        cfg = dict(SORTER_CONFIG)
        cfg['resonance_enabled'] = True
        engine = ResonanceEngine(config=cfg)
        months = [
            {'net_score': 0.5},
            {'net_score': 0.3},
        ]
        call_counts = {'CR': 60, 'CF': 10, 'WR': 10, 'WF': 10, 'Other': 10}
        put_counts = {'CR': 60, 'CF': 10, 'WR': 10, 'WF': 10, 'Other': 10}
        result = engine.compute(
            month_data_list=months,
            call_counts=call_counts,
            put_counts=put_counts,
            current_price=105,
            prev_price=100,
            product_score_ts=0.6,
        )
        assert 'resonance_score' in result
        assert 'resonance_direction' in result
        assert 'resonance_active_dims' in result
        assert 'resonance_fallback_applied' in result
        assert 'direction_conflict' in result
        assert 'final_score' in result

    def test_alpha_engine_preserves_historical_percentile_with_resonance(self):
        """关键修复验证：共振启用时 AlphaEngine 的 historical_percentile 不被覆盖。"""
        cfg = dict(SORTER_CONFIG)
        cfg['resonance_enabled'] = True
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        _make_product_history(engine, 'IF', [0.1] * 10)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data, current_price=105, prev_price=100)
        assert result['resonance_enabled'] is True
        # historical_percentile 应来自 RankNormalizer，不为 None
        assert result['historical_percentile'] is not None
        # 且不应等于 ResonanceEngine 用 CDF 估算的值（应基于真实历史）
        # 这里 0.8 的历史百分位应为 1.0（所有历史都低于 0.8）
        assert result['historical_percentile'] == 1.0


# =====================================================================
# v2.7 §18.4: 方向冲突日频监控
# =====================================================================

class TestDirectionConflictMonitor:
    """v2.7 边缘风险监控预埋验证。"""

    def test_monitor_counts_conflicts(self):
        """监控器正确统计方向冲突品种占比。"""
        from ali2026v3_trading.data.three_layer_sort.signal_source_a import _DirectionConflictMonitor
        monitor = _DirectionConflictMonitor()
        monitor.record({'resonance_enabled': True, 'direction_conflict': True})
        monitor.record({'resonance_enabled': True, 'direction_conflict': False})
        monitor.record({'resonance_enabled': True, 'direction_conflict': False})
        assert monitor._total == 3
        assert monitor._conflicts == 1

    def test_monitor_ignores_disabled_resonance(self):
        """共振关闭时不统计。"""
        from ali2026v3_trading.data.three_layer_sort.signal_source_a import _DirectionConflictMonitor
        monitor = _DirectionConflictMonitor()
        monitor.record({'resonance_enabled': False, 'direction_conflict': True})
        assert monitor._total == 0

    def test_monitor_warning_threshold(self, caplog):
        """单日冲突占比 > 20% 时记录 WARNING（日末 flush 触发）。"""
        import logging
        from ali2026v3_trading.data.three_layer_sort.signal_source_a import _DirectionConflictMonitor
        monitor = _DirectionConflictMonitor()
        with caplog.at_level(logging.WARNING):
            for _ in range(10):
                monitor.record({'resonance_enabled': True, 'direction_conflict': True})
            # 模拟日末触发 flush
            monitor.force_flush()
        assert any('20%' in rec.message for rec in caplog.records)


# =====================================================================
# v2.8 §21: 方案开关 + D/Q + confidence + entropy + output_mode
# =====================================================================

class TestScoringSchemeV28:
    """v2.8 方案开关机制验证。"""

    def test_default_scoring_scheme_is_scheme_1(self):
        """实盘默认 scoring_scheme=scheme_1。"""
        assert SORTER_CONFIG['scoring_scheme'] == 'scheme_1'

    def test_scheme_1_product_score_based_on_net_score(self):
        """方案一: product_score = weighted net_score (v2.7 兼容)。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'scheme_1'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        # scheme_1: product_score 应等于 net_score 的加权（单月份就是 net_score 本身）
        # net_score = (80+10-5-5)/100 = 0.8
        assert result['scoring_scheme'] == 'scheme_1'
        assert result['product_score'] == pytest.approx(0.8, abs=0.01)

    def test_scheme_2_product_score_based_on_D(self):
        """方案二: product_score = weighted D (正交分解)。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'scheme_2'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        # D = (CR+WR-CF-WF)/signal_total = (80+5-10-5)/100 = 0.7
        assert result['scoring_scheme'] == 'scheme_2'
        assert result['product_score'] == pytest.approx(0.7, abs=0.01)

    def test_scheme_3_product_score_based_on_D(self):
        """方案三: product_score = weighted D (同方案二基础)。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'scheme_3'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        assert result['scoring_scheme'] == 'scheme_3'
        assert result['product_score'] == pytest.approx(0.7, abs=0.01)

    def test_scheme_1_production_outputs_net_score_and_confidence(self):
        """方案一 production 模式: 输出 net_score + confidence + 基础字段。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'scheme_1'
        cfg['output_mode'] = 'production'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        assert 'net_score' in result
        assert 'confidence' in result
        assert 'product_score' in result
        assert 'timestamp' in result

    def test_scheme_2_production_outputs_D_and_Q(self):
        """方案二 production 模式: 输出 D + Q + direction + 基础字段。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'scheme_2'
        cfg['output_mode'] = 'production'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        assert 'D' in result
        assert 'Q' in result
        assert 'direction' in result
        assert 'quality_level' in result

    def test_scheme_3_production_outputs_entropy_features(self):
        """方案三 production 模式: 输出 concentration + direction_bias + entropy + 基础字段。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'scheme_3'
        cfg['output_mode'] = 'production'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        assert 'concentration' in result
        assert 'direction_bias' in result
        assert 'entropy' in result

    def test_scheme_all_research_outputs_all_fields(self):
        """all + research 模式: 全字段输出。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'all'
        cfg['output_mode'] = 'research'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        # 全字段都存在
        for field in ['D', 'Q', 'confidence', 'concentration', 'direction_bias',
                      'entropy', 'net_score', 'product_score', 'direction', 'quality_level']:
            assert field in result, f"Missing field '{field}' in all+research mode"

    def test_invalid_scoring_scheme_falls_back_to_scheme_1(self):
        """无效 scoring_scheme 回退到 scheme_1。"""
        cfg = dict(SORTER_CONFIG)
        cfg['scoring_scheme'] = 'invalid_scheme'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        assert engine._scoring_scheme == 'scheme_1'

    def test_scheme_1_and_scheme_2_product_score_differ(self):
        """方案一和方案二的 product_score 值不同（net_score vs D）。"""
        cfg1 = dict(SORTER_CONFIG)
        cfg1['scoring_scheme'] = 'scheme_1'
        engine1 = AlphaEngine(pure_mode=True, sorter_config=cfg1)

        cfg2 = dict(SORTER_CONFIG)
        cfg2['scoring_scheme'] = 'scheme_2'
        engine2 = AlphaEngine(pure_mode=True, sorter_config=cfg2)

        data = [_make_month_data('2507', 80, 10, 5, 5)]
        r1 = engine1.rank('IF', data)
        r2 = engine2.rank('IF', data)
        # net_score = (80+10-5-5)/100 = 0.8
        # D = (80+5-10-5)/100 = 0.7
        assert r1['product_score'] != r2['product_score']


class TestOrthogonalDecompositionV28:
    """v2.8 D/Q 正交分解验证。"""

    def test_D_Q_orthogonality(self):
        """D/Q 系数向量正交性验证: (1,1,-1,-1)·(1,-1,-1,1) = 0。"""
        # 当 CR=CF=WR=WF 时，D=Q=0
        dq = compute_orthogonal_decomposition({'CR': 25, 'CF': 25, 'WR': 25, 'WF': 25, 'Other': 0})
        assert dq['D'] == 0.0
        assert dq['Q'] == 0.0

    def test_bullish_direction(self):
        """看涨方向: CR + WR > CF + WF → D > 0。"""
        dq = compute_orthogonal_decomposition({'CR': 70, 'CF': 5, 'WR': 5, 'WF': 20, 'Other': 0})
        assert dq['D'] > 0
        assert dq['direction'] == 'long'

    def test_bearish_direction(self):
        """看跌方向: CR + WR < CF + WF → D < 0。"""
        dq = compute_orthogonal_decomposition({'CR': 5, 'CF': 70, 'WR': 20, 'WF': 5, 'Other': 0})
        assert dq['D'] < 0
        assert dq['direction'] == 'short'

    def test_signal_total_zero_returns_neutral(self):
        """signal_total=0 时返回 neutral/low。"""
        dq = compute_orthogonal_decomposition({'CR': 0, 'CF': 0, 'WR': 0, 'WF': 0, 'Other': 100})
        assert dq['D'] == 0.0
        assert dq['Q'] == 0.0
        assert dq['direction'] == 'neutral'
        assert dq['quality_level'] == 'low'

    def test_quality_level_high(self):
        """Q > 0.3 → quality_level = high。"""
        dq = compute_orthogonal_decomposition({'CR': 80, 'CF': 5, 'WR': 5, 'WF': 10, 'Other': 0})
        assert dq['Q'] > 0.3
        assert dq['quality_level'] == 'high'


class TestConfidenceV28:
    """v2.8 confidence 修正公式验证。"""

    def test_balanced_direction_zero_confidence(self):
        """方向完全均衡时 confidence=0（修正原方案 0.80 的错误）。"""
        conf = compute_confidence_v28({'CR': 45, 'CF': 0, 'WR': 45, 'WF': 0, 'Other': 10})
        assert conf == 0.0

    def test_clear_direction_high_confidence(self):
        """方向明确时 confidence > 0。"""
        conf = compute_confidence_v28({'CR': 70, 'CF': 0, 'WR': 5, 'WF': 0, 'Other': 25})
        assert conf > 0.5

    def test_low_participation_reduces_confidence(self):
        """参与度低时 confidence 低。"""
        conf = compute_confidence_v28({'CR': 10, 'CF': 0, 'WR': 1, 'WF': 0, 'Other': 89})
        assert conf < 0.2


class TestEntropyFeaturesV28:
    """v2.8 entropy/concentration/direction_bias 验证。"""

    def test_concentration_range(self):
        """concentration 范围 [0, 1]。"""
        ent = compute_entropy_features({'CR': 70, 'CF': 5, 'WR': 5, 'WF': 20, 'Other': 0})
        assert 0 <= ent['concentration'] <= 1

    def test_direction_bias_range(self):
        """direction_bias 范围 [-1, 1]。"""
        ent = compute_entropy_features({'CR': 70, 'CF': 5, 'WR': 5, 'WF': 20, 'Other': 0})
        assert -1 <= ent['direction_bias'] <= 1

    def test_direction_bias_equals_D_times_participation(self):
        """direction_bias = D × participation。"""
        counts = {'CR': 70, 'CF': 5, 'WR': 5, 'WF': 20, 'Other': 0}
        dq = compute_orthogonal_decomposition(counts)
        ent = compute_entropy_features(counts)
        assert ent['direction_bias'] == pytest.approx(dq['D'] * dq['participation'], abs=0.001)

    def test_uniform_distribution_max_entropy(self):
        """均匀分布时 entropy 接近 max_entropy。"""
        ent = compute_entropy_features({'CR': 20, 'CF': 20, 'WR': 20, 'WF': 20, 'Other': 20})
        assert ent['concentration'] == pytest.approx(0.0, abs=0.01)


class TestOutputModeV28:
    """v2.8 output_mode 参数化验证。"""

    def test_research_mode_all_fields(self):
        """research 模式输出全字段。"""
        cfg = dict(SORTER_CONFIG)
        cfg['output_mode'] = 'research'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        assert 'D' in result
        assert 'Q' in result
        assert 'confidence' in result
        assert 'concentration' in result

    def test_production_mode_scheme_1_slim_fields(self):
        """production + scheme_1: 瘦身输出。"""
        cfg = dict(SORTER_CONFIG)
        cfg['output_mode'] = 'production'
        cfg['scoring_scheme'] = 'scheme_1'
        engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        # 方案一核心字段保留
        assert 'net_score' in result
        assert 'confidence' in result
        assert 'product_id' in result


# =====================================================================
# v2.8 §21: WidthCacheQueryService.configure_sorter 回测三方案切换
# =====================================================================

class TestConfigureSorterV28:
    """v2.8 configure_sorter 回测方案切换验证。"""

    def test_configure_sorter_accepts_override(self):
        """configure_sorter 接受配置覆盖。"""
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        svc = WidthCacheQueryService()
        assert svc._sorter_config_override is None
        svc.configure_sorter({'scoring_scheme': 'scheme_2', 'output_mode': 'research'})
        assert svc._sorter_config_override is not None
        assert svc._sorter_config_override['scoring_scheme'] == 'scheme_2'

    def test_configure_sorter_none_clears_override(self):
        """configure_sorter(None) 清除覆盖。"""
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        svc = WidthCacheQueryService()
        svc.configure_sorter({'scoring_scheme': 'scheme_3'})
        assert svc._sorter_config_override is not None
        svc.configure_sorter(None)
        assert svc._sorter_config_override is None

    def test_configure_sorter_resets_lazy_sorters(self):
        """configure_sorter 清除已实例化的排序器，强制重建。"""
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        svc = WidthCacheQueryService()
        # 模拟已实例化
        svc._sorter_a = object()  # placeholder
        svc._sorter_b = object()
        svc._sorter_c = object()
        svc.configure_sorter({'scoring_scheme': 'scheme_2'})
        assert svc._sorter_a is None
        assert svc._sorter_b is None
        assert svc._sorter_c is None

    def test_build_sorter_config_override_priority(self):
        """_build_sorter_config: 外部覆盖优先于 params_service。"""
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        svc = WidthCacheQueryService()
        svc.configure_sorter({'scoring_scheme': 'scheme_3', 'output_mode': 'production'})
        cfg = svc._build_sorter_config()
        assert cfg is not None
        assert cfg['scoring_scheme'] == 'scheme_3'
        assert cfg['output_mode'] == 'production'

    def test_build_sorter_config_default_returns_none(self):
        """_build_sorter_config: 无覆盖且params_service无配置时返回None（用默认）。"""
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        svc = WidthCacheQueryService()
        # 无 facade，无 params_service 配置 → 应返回 None 或空配置
        # 注意：实际环境 params_service 可能存在但无 sorter.* 键，应返回 None
        cfg = svc._build_sorter_config()
        # 在测试环境（无 params_service 配置 sorter.*），应返回 None
        assert cfg is None

    def test_three_schemes_switchable_via_configure_sorter(self):
        """回测三方案切换端到端：scheme_1/2/3 均可通过 configure_sorter 切换。"""
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        from ali2026v3_trading.data.three_layer_sort import AlphaEngine

        data = [_make_month_data('2507', 80, 10, 5, 5)]
        results = {}
        for scheme in ('scheme_1', 'scheme_2', 'scheme_3', 'all'):
            svc = WidthCacheQueryService()
            svc.configure_sorter({'scoring_scheme': scheme, 'output_mode': 'research'})
            cfg = svc._build_sorter_config()
            engine = AlphaEngine(pure_mode=True, sorter_config=cfg)
            result = engine.rank('IF', data)
            results[scheme] = result
            assert result['scoring_scheme'] == scheme

        # scheme_1: product_score = net_score = 0.8
        assert results['scheme_1']['product_score'] == pytest.approx(0.8, abs=0.01)
        # scheme_2/3: product_score = D = 0.7
        assert results['scheme_2']['product_score'] == pytest.approx(0.7, abs=0.01)
        assert results['scheme_3']['product_score'] == pytest.approx(0.7, abs=0.01)
        # all: 全字段输出
        for field in ['D', 'Q', 'confidence', 'concentration', 'direction_bias',
                      'entropy', 'net_score']:
            assert field in results['all']


# =====================================================================
# v2.8.1 fix: primary_score 统一排序键（消除 scheme 不一致）
# =====================================================================

class TestPrimaryScoreSchemeAwareV281:
    """v2.8.1 修复验证：primary_score 字段统一月份候选排序键。

    修复背景（§22.6 已知隐患 #1/#3）：
      - 旧实现 month_candidates 硬编码按 net_score 排序，scheme_2/3 下
        product_score 基础是 D，但月份排序键仍是 net_score，语义不一致
      - 旧实现 compute_score_delta_to_next / rank_confidence / fallback_gap
        均硬编码 net_score
      - 旧实现 resonance_engine.D_term_structure 使用 net_score，scheme_2/3 下
        与 product_score 基础不一致

    修复方案：新增 primary_score 字段（scheme 感知），下游统一使用。
    """

    def test_primary_score_field_present_in_month_candidates(self):
        """month_candidates 每个候选包含 primary_score 字段。"""
        engine = AlphaEngine(pure_mode=True)
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        for cand in result['month_candidates']:
            assert 'primary_score' in cand
            assert isinstance(cand['primary_score'], float)

    def test_scheme_1_primary_score_equals_net_score(self):
        """scheme_1: primary_score = net_score（实盘兼容 v2.7）。"""
        engine = AlphaEngine(pure_mode=True, sorter_config={'scoring_scheme': 'scheme_1'})
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        cand = result['month_candidates'][0]
        assert cand['primary_score'] == pytest.approx(cand['net_score'], abs=1e-6)

    def test_scheme_2_primary_score_equals_D(self):
        """scheme_2: primary_score = D（v2.8 正交分解方向强度）。"""
        engine = AlphaEngine(pure_mode=True, sorter_config={'scoring_scheme': 'scheme_2'})
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        cand = result['month_candidates'][0]
        assert cand['primary_score'] == pytest.approx(cand['D'], abs=1e-6)

    def test_scheme_3_primary_score_equals_D(self):
        """scheme_3: primary_score = D。"""
        engine = AlphaEngine(pure_mode=True, sorter_config={'scoring_scheme': 'scheme_3'})
        data = [_make_month_data('2507', 80, 10, 5, 5)]
        result = engine.rank('IF', data)
        cand = result['month_candidates'][0]
        assert cand['primary_score'] == pytest.approx(cand['D'], abs=1e-6)

    def test_scheme_2_sorts_by_D_not_net_score(self):
        """scheme_2: month_candidates 按 D 降序，非按 net_score。

        构造 net_score 与 D 排序相反的数据，验证 scheme_2 下月份顺序与 D 一致。
        Month A: CR=80, CF=20, WR=5, WF=5, Other=0
          net_score = (80+20-5-5)/110 ≈ 0.818
          D = (80+5-20-5)/110 ≈ 0.545
        Month B: CR=50, CF=10, WR=5, WF=5, Other=30
          net_score = (50+10-5-5)/100 = 0.500
          D = (50+5-10-5)/70 ≈ 0.571
        → net_score(A) > net_score(B), 但 D(A) < D(B)
        → scheme_1 排序: A, B; scheme_2 排序: B, A
        """
        data = [
            _make_month_data('A', 80, 20, 5, 5, other=0),
            _make_month_data('B', 50, 10, 5, 5, other=30),
        ]
        # scheme_1: 按 net_score 降序 → A(0.818) 先于 B(0.500)
        engine1 = AlphaEngine(pure_mode=True, sorter_config={'scoring_scheme': 'scheme_1'})
        r1 = engine1.rank('IF', data)
        assert r1['month_candidates'][0]['month'] == 'A'
        assert r1['month_candidates'][1]['month'] == 'B'
        # scheme_2: 按 D 降序 → B(0.571) 先于 A(0.545)
        engine2 = AlphaEngine(pure_mode=True, sorter_config={'scoring_scheme': 'scheme_2'})
        r2 = engine2.rank('IF', data)
        assert r2['month_candidates'][0]['month'] == 'B'
        assert r2['month_candidates'][1]['month'] == 'A'

    def test_score_delta_uses_primary_score(self):
        """compute_score_delta_to_next 使用 primary_score（向后兼容 net_score）。"""
        # 有 primary_score 的候选
        cands_with_ps = [
            {'month': 'm1', 'primary_score': 0.8, 'net_score': 0.7, 'D': 0.8},
            {'month': 'm2', 'primary_score': 0.5, 'net_score': 0.4, 'D': 0.5},
        ]
        deltas = compute_score_delta_to_next(cands_with_ps)
        assert deltas[0] == pytest.approx(0.3, abs=1e-6)  # 0.8 - 0.5
        assert deltas[1] is None

    def test_score_delta_backward_compat_net_score(self):
        """compute_score_delta_to_next 向后兼容：无 primary_score 时用 net_score。"""
        cands_no_ps = [
            {'month': 'm1', 'net_score': 0.8},
            {'month': 'm2', 'net_score': 0.5},
        ]
        deltas = compute_score_delta_to_next(cands_no_ps)
        assert deltas[0] == pytest.approx(0.3, abs=1e-6)
        assert deltas[1] is None

    def test_rank_confidence_uses_primary_score(self):
        """compute_rank_confidence_default 使用 primary_score（向后兼容）。"""
        # 有 primary_score 的候选
        cands = [
            {'month': 'm1', 'primary_score': 0.9, 'net_score': 0.5},
            {'month': 'm2', 'primary_score': 0.1, 'net_score': 0.8},
        ]
        history = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.15]
        confs = compute_rank_confidence_default(cands, history)
        # primary_score=0.9 → 大部分历史值 < 0.9 → 高 percentile
        # primary_score=0.1 → 大部分历史值 > 0.1 → 低 percentile
        assert confs[0] > confs[1]
        assert 0.0 <= confs[0] <= 1.0
        assert 0.0 <= confs[1] <= 1.0


class TestTermStructureDirectionDFieldV281:
    """v2.8.1 修复验证：D_term_structure 优先使用 D 字段。

    修复背景（§22.6 已知隐患 #2）：
      - 旧实现 compute_term_structure_direction 硬编码使用 net_score
      - scheme_2/3 下 product_score 基于 D，但期限结构方向仍用 net_score，语义不一致
    修复方案：优先 D，回退 net_score（向后兼容直接调用方）。
    """

    def test_term_structure_uses_D_when_present_same_sign(self):
        """有 D 字段且同向时，基于 D 返回方向。"""
        data = [
            {'month': 'm1', 'D': 0.5, 'net_score': -0.3},  # D>0, net_score<0
            {'month': 'm2', 'D': 0.3, 'net_score': -0.1},  # D>0, net_score<0
        ]
        # D 同为正 → 返回 +1（即使 net_score 为负）
        assert compute_term_structure_direction(data) == 1

    def test_term_structure_uses_D_when_present_opposite_sign(self):
        """有 D 字段且反向时，基于 D 返回 0。"""
        data = [
            {'month': 'm1', 'D': 0.5, 'net_score': 0.3},   # D>0
            {'month': 'm2', 'D': -0.3, 'net_score': -0.5},  # D<0
        ]
        # D 反向 → 返回 0（即使 net_score 同向为正）
        assert compute_term_structure_direction(data) == 0

    def test_term_structure_backward_compat_net_score_only(self):
        """无 D 字段时回退到 net_score（向后兼容直接调用方）。"""
        data = [
            {'month': 'm1', 'net_score': 0.5},
            {'month': 'm2', 'net_score': 0.3},
        ]
        # 无 D → 用 net_score，同向为正 → +1
        assert compute_term_structure_direction(data) == 1

    def test_term_structure_D_takes_precedence_over_net_score(self):
        """D 字段优先于 net_score（当两者方向冲突时以 D 为准）。"""
        data = [
            {'month': 'm1', 'D': -0.5, 'net_score': 0.5},  # D<0, net_score>0
            {'month': 'm2', 'D': -0.3, 'net_score': 0.3},  # D<0, net_score>0
        ]
        # D 同为负 → 返回 -1（即使 net_score 同向为正）
        assert compute_term_structure_direction(data) == -1


class TestWidthCacheDFieldPropagationV281:
    """v2.8.1 修复验证：width_cache_query_mixin 传播 D / primary_score 字段。

    通过完整调用 select_otm_targets_signal_sources 端到端验证字段传播。
    """

    def test_entry_includes_D_and_primary_score_fields(self):
        """select_otm_targets_signal_sources 输出的 target 包含 D / primary_score 字段。"""
        import threading
        from unittest.mock import MagicMock, patch
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService

        svc = WidthCacheQueryService()
        fid = 10001

        # 最小状态：锁（RLock 可重入，支持 select_from_sort_bucket 在持锁时回调）
        svc._lock = threading.RLock()
        svc._query_count = 0
        svc._ensure_signal_sorters = MagicMock()
        svc._backfill_is_main_month = MagicMock()
        svc._sorter_config_override = None

        # _get_params mock
        params_mock = MagicMock()
        params_mock.get_instrument_meta.return_value = {'product': 'IF', 'year_month': '2507'}
        svc._get_params = MagicMock(return_value=params_mock)

        # _get_scoring_months mock：直接返回我们准备的月份
        svc._get_scoring_months = MagicMock(return_value=['2507'])

        # 状态计数：最小数据让循环进入
        svc._status_counts = {
            fid: {
                '2507': {
                    'CALL': {'correct_rise': 80, 'wrong_rise': 5,
                             'correct_fall': 20, 'wrong_fall': 5, 'other': 0}
                }
            }
        }
        svc._future_initialized = {fid: True}
        svc._future_rising = {fid: True}
        svc._sort_buckets = {fid: {'2507': {'CALL': []}}}
        svc._buckets_dirty = set()

        # select_from_sort_bucket mock：返回带基础字段的 entry
        base_entry = {
            'instrument_id': 'IF2507C4000',
            'internal_id': 1001,
            'option_type': 'CALL',
            'strike_price': 4000.0,
            'volume': 100.0,
            'price': 50.0,
            'month': '2507',
            'product': 'IF',
            'sync_flag': True,
        }
        svc.select_from_sort_bucket = MagicMock(return_value=[base_entry])

        # sorter_a mock：返回带 month_candidates（含 v2.8.1 字段）
        svc._sorter_a = MagicMock()
        svc._sorter_a.rank.return_value = {
            'signal_source': 'A',
            'product_id': 'IF',
            'product_score': 0.8,
            'product_score_ts': 0.8,
            'final_score': 0.8,
            'correct_up_pct': 0.8,
            'tier': 1,
            'future_internal_id': fid,
            'month_candidates': [
                {
                    'month': '2507',
                    'net_score': 0.8,
                    'D': 0.7,
                    'primary_score': 0.7,
                    'rank_confidence': 0.9,
                    'score_delta_to_next': 0.2,
                }
            ],
        }
        svc._sorter_b = MagicMock()
        svc._sorter_b.rank.return_value = {'sorted_products': []}
        svc._sorter_c = MagicMock()
        svc._sorter_c.rank.return_value = {
            'sorted_products': [
                {'product_id': 'IF', 'tier': 1, 'global_rank': 1,
                 'correct_up_pct': 0.8, 'global_percentile': 1.0}
            ]
        }

        # position_service mock：无持仓
        with patch('ali2026v3_trading.position.position_service.get_position_service',
                   return_value=None):
            targets = svc.select_otm_targets_signal_sources(signal_source='C')

        assert len(targets) == 1
        t = targets[0]
        # v2.8.1: 验证 D / primary_score 字段已传播
        assert 'D' in t
        assert t['D'] == pytest.approx(0.7, abs=1e-6)
        assert 'primary_score' in t
        assert t['primary_score'] == pytest.approx(0.7, abs=1e-6)
        # 向后兼容：net_score 仍保留
        assert 'net_score' in t
        assert t['net_score'] == pytest.approx(0.8, abs=1e-6)
        # rank_confidence / score_delta_to_next 也已传播
        assert t['rank_confidence'] == pytest.approx(0.9, abs=1e-6)
        assert t['score_delta_to_next'] == pytest.approx(0.2, abs=1e-6)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
