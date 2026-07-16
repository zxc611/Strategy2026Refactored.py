# MODULE_ID: M2-327
"""
V7实盘前边缘裂缝验证测试 (裂缝14-30)

覆盖裂缝清单中第14到30个问题的验证测试。
"""
import pytest
import math
import numpy as np

from risk.crack_validation import (
    validate_circuit_breaker_halts,
    validate_shadow_b_stability,
    apply_tick_rounding,
    apply_lot_rounding,
    validate_tick_rounding_impact,
    compute_cascade_slippage,
    validate_cascade_slippage_impact,
    compute_discrete_equity,
    validate_capital_discretization_gap,
    validate_shadow_order_isolation,
    StateConfirmWindow,
    validate_state_window_boundary,
    check_safety_meta_layer,
    validate_circuit_breaker_vs_time_stop,
    resample_indicator_ffill,
    validate_indicator_multiscale_consistency,
    compute_sharpe_ci,
    validate_alpha_confidence_overlap,
    validate_logic_reversal_timestamp_causality,
    compute_kl_divergence,
    validate_shadow_param_orthogonality,
    validate_attribution_residual_significance,
    CASCADE_SLIPPAGE_MULTIPLIER_DEFAULT,
)


# =====================================================================
# 裂缝14: 期权熔断停牌
# =====================================================================

class TestCrack14CircuitBreaker:
    def test_survival_ok(self):
        result = validate_circuit_breaker_halts(100000, 99000)
        assert result['passed'] and result['survival_ratio'] == 0.99

    def test_survival_fail(self):
        result = validate_circuit_breaker_halts(100000, 98000, survival_threshold=0.99)
        assert not result['passed']

    def test_zero_equity(self):
        result = validate_circuit_breaker_halts(0, 0)
        assert not result['passed']


# =====================================================================
# 裂缝15: 影子B随机路径稳定性
# =====================================================================

class TestCrack15ShadowBStability:
    def test_stable_ci(self):
        sharpes = [1.0 + 0.01 * i for i in range(100)]
        result = validate_shadow_b_stability(sharpes)
        assert result['passed'] and result['ci_width'] < 1.0

    def test_unstable_ci(self):
        sharpes = list(np.random.normal(0, 2, 200))
        result = validate_shadow_b_stability(sharpes, ci_width_threshold=1.0)
        assert result['recommendation'] in ('use_mean', 'use_max_ok')

    def test_too_few_samples(self):
        result = validate_shadow_b_stability([1.0])
        assert not result['passed']

    def test_ci_width_zero_variance(self):
        result = validate_shadow_b_stability([1.5] * 50)
        assert result['ci_width'] < 0.01


# =====================================================================
# 裂缝16: 最小变动价位与手数取整
# =====================================================================

class TestCrack16TickRounding:
    def test_round_nearest(self):
        assert abs(apply_tick_rounding(100.03, 0.05) - 100.05) < 1e-9

    def test_round_up(self):
        assert abs(apply_tick_rounding(100.01, 0.05, 'up') - 100.05) < 1e-9

    def test_round_down(self):
        assert apply_tick_rounding(100.04, 0.05, 'down') == 100.0

    def test_lot_floor(self):
        assert apply_lot_rounding(1.7) == 1

    def test_lot_ceil(self):
        assert apply_lot_rounding(1.3, 'ceil') == 2

    def test_impact_ok(self):
        result = validate_tick_rounding_impact(1.0, 0.8, min_ratio=0.7)
        assert result['passed']

    def test_impact_fail(self):
        result = validate_tick_rounding_impact(1.0, 0.5, min_ratio=0.7)
        assert not result['passed']


# =====================================================================
# 裂缝19: 连续滑点放大效应
# =====================================================================

class TestCrack19CascadeSlippage:
    def test_no_volume_impact(self):
        slip = compute_cascade_slippage(1.0, 0, 1000)
        assert abs(slip - 1.0 * CASCADE_SLIPPAGE_MULTIPLIER_DEFAULT) < 1e-6

    def test_with_volume_impact(self):
        import math
        slip = compute_cascade_slippage(1.0, 500, 1000)
        expected = 1.0 * (1 + math.sqrt(0.5)) * 1.5
        assert abs(slip - expected) < 1e-6

    def test_large_close_volume(self):
        import math
        slip = compute_cascade_slippage(1.0, 2000, 1000)
        expected = min(1.0 * (1 + math.sqrt(2.0)) * 1.5, 50.0)
        assert abs(slip - expected) < 1e-6

    def test_impact_ok(self):
        result = validate_cascade_slippage_impact(1.0, 0.8, max_decay_pct=0.25)
        assert result['passed']

    def test_impact_fail(self):
        result = validate_cascade_slippage_impact(1.0, 0.5, max_decay_pct=0.25)
        assert not result['passed']


# =====================================================================
# 裂缝20: 资金曲线离散化
# =====================================================================

class TestCrack20DiscreteEquity:
    def test_no_loss(self):
        curve = compute_discrete_equity(100000, [0, 0], [1000, 1000])
        assert curve == [100000, 101000, 102000]

    def test_full_premium_loss(self):
        curve = compute_discrete_equity(100000, [5000, 5000], [0, 0])
        assert curve == [100000, 95000, 90000]

    def test_zero_equity_floor(self):
        curve = compute_discrete_equity(1000, [2000], [0])
        assert curve[-1] == 0

    def test_gap_ok(self):
        result = validate_capital_discretization_gap(1.5, 1.3, max_diff=0.3)
        assert result['passed']

    def test_gap_fail(self):
        result = validate_capital_discretization_gap(1.5, 0.8, max_diff=0.3)
        assert not result['passed']


# =====================================================================
# 裂缝23: 影子订单隔离
# =====================================================================

class TestCrack23ShadowOrderIsolation:
    def test_isolated(self):
        result = validate_shadow_order_isolation(
            main_cash_before=100000,
            main_cash_after=100000,
            main_positions_before={},
            main_positions_after={},
        )
        assert result['passed']

    def test_not_isolated_cash(self):
        result = validate_shadow_order_isolation(
            main_cash_before=100000,
            main_cash_after=99000,
            main_positions_before={},
            main_positions_after={},
        )
        assert not result['passed']

    def test_not_isolated_positions(self):
        result = validate_shadow_order_isolation(
            main_cash_before=100000,
            main_cash_after=100000,
            main_positions_before={},
            main_positions_after={'IF2606': 1},
        )
        assert not result['passed']


# =====================================================================
# 裂缝24: 状态确认窗口边界语义
# =====================================================================

class TestCrack24StateConfirmWindow:
    def test_three_consecutive_confirm(self):
        w = StateConfirmWindow(confirm_bars=3)
        assert w.feed('A') == (False, None)
        assert w.feed('A') == (False, None)
        switched, to = w.feed('A')
        assert switched and to == 'A'

    def test_interrupt_resets(self):
        w = StateConfirmWindow(confirm_bars=3)
        w.feed('A')
        w.feed('A')
        switched, to = w.feed('B')
        assert not switched

    def test_five_patterns(self):
        result = validate_state_window_boundary()
        assert result['passed']
        assert all(d[1] for d in result['details'])

    def test_fast_alternation_no_confirm(self):
        w = StateConfirmWindow(confirm_bars=3)
        for s in ['A', 'B', 'A', 'B', 'A', 'B']:
            switched, _ = w.feed(s)
            assert not switched


# =====================================================================
# 裂缝25: 断路器vs时间止损优先级
# =====================================================================

class TestCrack25CircuitBreakerVsTimeStop:
    def test_normal_can_open_and_close(self):
        can_open, can_close = check_safety_meta_layer(False, False)
        assert can_open and can_close

    def test_circuit_breaker_can_close_only(self):
        can_open, can_close = check_safety_meta_layer(True, False)
        assert not can_open and can_close

    def test_time_stop_can_close_only(self):
        can_open, can_close = check_safety_meta_layer(False, True)
        assert not can_open and can_close

    def test_both_can_close_only(self):
        can_open, can_close = check_safety_meta_layer(True, True)
        assert not can_open and can_close

    def test_all_four_combinations(self):
        result = validate_circuit_breaker_vs_time_stop()
        assert result['passed']


# =====================================================================
# 裂缝26: 多粒度指标一致性
# =====================================================================

class TestCrack26MultiscaleIndicator:
    def test_ffill_resample(self):
        ind = np.arange(20, dtype=float)
        result = resample_indicator_ffill(ind, 5)
        assert len(result) == 4
        assert result[0] == 0.0
        assert result[1] == 5.0

    def test_consistency_high_corr(self, seeded_random):
        np.random.seed(seeded_random)
        native = np.random.randn(100).cumsum()
        resampled = native + np.random.randn(100) * 0.01
        result = validate_indicator_multiscale_consistency(native, resampled)
        assert result['passed'] and result['corr'] > 0.95

    def test_consistency_low_corr(self, seeded_random):
        np.random.seed(seeded_random)
        native = np.random.randn(100).cumsum()
        resampled = np.random.randn(100).cumsum()
        result = validate_indicator_multiscale_consistency(native, resampled, min_corr=0.95)
        assert not result['passed']

    def test_constant_series(self):
        result = validate_indicator_multiscale_consistency(
            np.ones(10), np.ones(10)
        )
        assert result['passed']


# =====================================================================
# 裂缝27: 主-影子Sharpe置信区间重叠
# =====================================================================

class TestCrack27SharpeCIOverlap:
    def test_no_overlap(self):
        result = validate_alpha_confidence_overlap(
            main_sharpe=2.0, main_n=1000,
            shadow_sharpe=0.5, shadow_n=1000,
        )
        assert result['passed'] and result['alpha_reliable']

    def test_high_overlap(self):
        result = validate_alpha_confidence_overlap(
            main_sharpe=1.0, main_n=10,
            shadow_sharpe=0.9, shadow_n=10,
        )
        assert not result['passed']
        assert result['recommendation'] == 'fallback_to_base_weights'

    def test_ci_calculation(self):
        low, high = compute_sharpe_ci(1.0, 100)
        se = 1.96 / 10.0
        assert abs(low - (1.0 - se)) < 1e-6
        assert abs(high - (1.0 + se)) < 1e-6


# =====================================================================
# 裂缝28: 逻辑反转wrong_pct未来函数
# =====================================================================

class TestCrack28LogicReversalNoFuture:
    def test_no_future_violation(self):
        result = validate_logic_reversal_timestamp_causality(
            ['10:01', '10:02', '10:03'],
            ['10:00', '10:01', '10:02'],
        )
        assert result['n_violations'] == 0
        assert result['needs_bar_level_audit']

    def test_future_violation(self):
        result = validate_logic_reversal_timestamp_causality(
            ['10:00', '10:01'],
            ['10:01', '10:02'],
        )
        assert result['n_violations'] == 2

    def test_bar_level_no_future(self):
        result = validate_logic_reversal_timestamp_causality(
            ['10:02', '10:03'],
            ['10:01', '10:02'],
            wrong_pct_bar_timestamps=['10:02', '10:03'],
        )
        assert result['passed'] and result['n_bar_violations'] == 0

    def test_bar_level_future_violation(self):
        result = validate_logic_reversal_timestamp_causality(
            ['10:02', '10:03'],
            ['10:01', '10:02'],
            wrong_pct_bar_timestamps=['10:03', '10:04'],
        )
        assert not result['passed'] and result['n_bar_violations'] == 2

    def test_missing_bar_timestamps_needs_audit(self):
        result = validate_logic_reversal_timestamp_causality(
            ['10:01', '10:02'],
            ['10:00', '10:01'],
        )
        assert result['needs_bar_level_audit'] and not result['passed']


# =====================================================================
# 裂缝29: 影子参数空间独立性(KL散度)
# =====================================================================

class TestCrack29ShadowParamKL:
    def test_identical_distributions(self):
        p = np.array([10, 20, 30, 20, 10], dtype=float)
        kl = compute_kl_divergence(p, p)
        assert kl < 0.01

    def test_different_distributions(self):
        p = np.array([50, 10, 10, 10, 10], dtype=float)
        q = np.array([10, 10, 10, 10, 50], dtype=float)
        kl = compute_kl_divergence(p, q)
        assert kl > 0.5

    def test_orthogonality_pass(self):
        main_params = np.random.normal(1.0, 0.5, 1000)
        shadow_params = np.random.normal(3.0, 0.5, 1000)
        result = validate_shadow_param_orthogonality(main_params, shadow_params, min_kl=0.1)
        assert result['passed']

    def test_orthogonality_fail(self):
        params = np.random.normal(1.0, 0.5, 1000)
        result = validate_shadow_param_orthogonality(params, params + 0.001, min_kl=0.5)
        assert not result['passed']

    def test_empty_vector(self):
        result = validate_shadow_param_orthogonality(np.array([]), np.array([1.0]))
        assert not result['passed']


# =====================================================================
# 裂缝30: 归因残差统计显著性
# =====================================================================

class TestCrack30AttributionResidual:
    def test_no_significant_residual(self, seeded_random):
        np.random.seed(seeded_random)
        residuals = np.random.normal(0, 0.01, 100)
        pnls = np.random.normal(1, 0.5, 100)
        result = validate_attribution_residual_significance(residuals, pnls)
        assert result['passed'] and not result['e7_should_trigger']

    def test_significant_large_residual(self, seeded_random):
        np.random.seed(seeded_random)
        residuals = np.random.normal(0.5, 0.1, 100)
        pnls = np.random.normal(1, 0.5, 100)
        result = validate_attribution_residual_significance(residuals, pnls)
        assert result['e7_should_trigger']

    def test_small_sample_skip(self):
        result = validate_attribution_residual_significance(
            np.array([0.1]), np.array([1.0])
        )
        assert result['passed'] and not result['e7_should_trigger']

    def test_zero_pnl(self):
        residuals = np.array([0.1, 0.2, 0.1])
        pnls = np.array([0.0, 0.0, 0.0])
        result = validate_attribution_residual_significance(residuals, pnls)
        assert result['passed']
