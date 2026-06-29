# MODULE_ID: M2-426
"""
Test option sort alignment against design report.

Validates the core calculation functions for option state sorting
by importing the actual production implementations from
width_cache_query_mixin.py.
"""

import pytest
from typing import Dict, List, Tuple

from ali2026v3_trading.data.width_cache import WidthStrengthCache
from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
# 五唯一性修复：常量已统一从 final_three_layer_config 导入到 width_cache_query_mixin 模块级
# 此处直接从规范源导入，避免依赖已被删除的类级常量
from ali2026v3_trading.config.final_three_layer_config import (
    MONTH_WEIGHTS_5, TIER1_WILSON_THRESHOLD,
    TIER2_COVERAGE_THRESHOLD, TIER2_CORRECT_UP_THRESHOLD,
    TIER3_CORRECT_UP_THRESHOLD,
)

# Re-export production constants for test readability
MAX_MONTHS_FOR_SCORING = WidthCacheQueryService.MAX_MONTHS_FOR_SCORING
TIER1_LOTS = WidthCacheQueryService.TIER1_LOTS
TIER2_LOTS = WidthCacheQueryService.TIER2_LOTS

# Re-export production static methods as module-level functions
wilson_lower_bound = WidthCacheQueryService.wilson_lower_bound
resolve_month_weights = WidthCacheQueryService.resolve_month_weights
compute_correct_up_pct = WidthCacheQueryService.compute_correct_up_pct
compute_noise_ratio = WidthCacheQueryService.compute_noise_ratio
compute_coverage = WidthCacheQueryService.compute_coverage
compute_th = WidthCacheQueryService.compute_th
compute_ra = WidthCacheQueryService.compute_ra
determine_tier = WidthCacheQueryService.determine_tier


def sort_key(tier: int, correct_up_pct: float, wilson: float) -> Tuple:
    """Sort key (design report Section 5).

    Sort by (tier ascending, correct_up_pct descending, wilson descending).
    """
    return (tier, -correct_up_pct, -wilson)


def allocate_lots(tier: int) -> int:
    """Allocate lots (design report Section 5, Phase 2 Step 4).

    Tier 1 -> 2 lots, Tier 2+ -> 1 lot.
    """
    return TIER1_LOTS if tier == 1 else TIER2_LOTS


# =====================================================================
# Test: MONTH_WEIGHTS_5 normalization (Section 4.3)
# =====================================================================

class TestMonthWeightsNormalization:
    """Design report Section 4.3: adaptive month weight normalization."""

    def test_base_weights_sum_to_one(self):
        """The 5-month base weights must sum to exactly 1.0."""
        assert sum(MONTH_WEIGHTS_5) == pytest.approx(1.0, abs=1e-10), \
            f"MONTH_WEIGHTS_5 sum = {sum(MONTH_WEIGHTS_5)}, expected 1.0"

    def test_base_weights_exact_values(self):
        """When 5 months: weights should be (0.35, 0.25, 0.20, 0.12, 0.08)."""
        weights = resolve_month_weights(5)
        assert weights == pytest.approx(
            (0.35, 0.25, 0.20, 0.12, 0.08), abs=1e-10
        )

    def test_three_months_truncate_and_normalize(self):
        """When 3 months: truncate to (0.35, 0.25, 0.20) then normalize.

        Raw sum = 0.80; normalized = (0.4375, 0.3125, 0.2500).
        """
        weights = resolve_month_weights(3)
        assert weights == pytest.approx(
            (0.4375, 0.3125, 0.2500), abs=1e-4
        ), f"3-month weights = {weights}"

    def test_two_months_truncate_and_normalize(self):
        """When 2 months: truncate to (0.35, 0.25) then normalize.

        Raw sum = 0.60; normalized = (0.5833..., 0.4167...).
        """
        weights = resolve_month_weights(2)
        assert weights[0] == pytest.approx(0.5833, abs=1e-3), \
            f"2-month weight[0] = {weights[0]}"
        assert weights[1] == pytest.approx(0.4167, abs=1e-3), \
            f"2-month weight[1] = {weights[1]}"

    def test_one_month_truncate_and_normalize(self):
        """When 1 month: truncate to (0.35) then normalize -> (1.0)."""
        weights = resolve_month_weights(1)
        assert weights == (pytest.approx(1.0, abs=1e-10),)

    def test_zero_months_returns_empty(self):
        """When 0 months: return empty tuple."""
        weights = resolve_month_weights(0)
        assert weights == ()

    def test_six_months_truncate_to_five(self):
        """When >5 months: return MONTH_WEIGHTS_5 (5 elements), caller truncates.

        BUG-12 fix: design report 4.3 says "取前5个月份参与评分，超出的月份不参与计算".
        Previously returned equal weights for all n months, which diluted
        near-month signals. Now returns the 5-element MONTH_WEIGHTS_5 tuple.
        """
        weights = resolve_month_weights(6)
        assert len(weights) == 5, \
            f"6-month should return 5 weights (truncate), got {len(weights)}"
        assert weights == pytest.approx(MONTH_WEIGHTS_5, abs=1e-10)

    @pytest.mark.parametrize("n_months", [1, 2, 3, 4, 5])
    def test_weights_always_sum_to_one(self, n_months):
        """Sum of weights must always equal 1.0 for n_months <= 5.

        Note: when n_months > 5, resolve_month_weights returns 5 elements
        (caller is responsible for truncating months to 5). The sum is still
        1.0 but the length is 5, not n_months.
        """
        weights = resolve_month_weights(n_months)
        if n_months == 0:
            return
        assert sum(weights) == pytest.approx(1.0, abs=1e-10), \
            f"Sum of {n_months}-month weights = {sum(weights)}"


# =====================================================================
# Test: Wilson lower bound (Section 8)
# =====================================================================

class TestWilsonLowerBound:
    """Design report Section 8: Wilson score interval lower bound."""

    def test_small_sample_penalty(self):
        """Wilson(2, 3, z=1.96): small sample should pull estimate down.

        With p_hat=2/3 and n=3, the Wilson lower bound applies a strong
        small-sample penalty, producing a value well below the naive 0.667.
        """
        result = wilson_lower_bound(2, 3, z=1.96)
        # The standard Wilson formula gives ~0.208 for these inputs.
        # Key property: result << p_hat due to small sample penalty.
        assert 0.15 < result < 0.30, \
            f"Wilson(2,3) = {result:.4f}, expected small-sample penalty range [0.15, 0.30]"
        assert result < 2 / 3, \
            "Wilson lower bound must be below the point estimate for small n"

    def test_medium_sample(self):
        """Wilson(100, 150, z=1.96): moderate sample, estimate closer to p_hat."""
        result = wilson_lower_bound(100, 150, z=1.96)
        # Standard formula gives ~0.588. Still below p_hat=0.667 but closer.
        assert 0.55 < result < 0.65, \
            f"Wilson(100,150) = {result:.4f}, expected range [0.55, 0.65]"

    def test_large_sample_converges(self):
        """Wilson(1000, 1500, z=1.96): large sample, close to p_hat."""
        result = wilson_lower_bound(1000, 1500, z=1.96)
        # Standard formula gives ~0.642. Very close to p_hat=0.667.
        assert 0.60 < result < 0.68, \
            f"Wilson(1000,1500) = {result:.4f}, expected range [0.60, 0.68]"
        # Larger sample should give higher Wilson than smaller sample at same p_hat
        result_medium = wilson_lower_bound(100, 150, z=1.96)
        assert result > result_medium, \
            "Wilson should increase with sample size at constant p_hat"

    def test_zero_total_returns_zero(self):
        """Wilson(0, 0) should return 0.0 (no data)."""
        assert wilson_lower_bound(0, 0) == 0.0

    def test_zero_pos_returns_zero_or_near_zero(self):
        """Wilson(0, 10): zero successes should give lower bound of 0 or near 0."""
        result = wilson_lower_bound(0, 10, z=1.96)
        assert result == pytest.approx(0.0, abs=0.05), \
            f"Wilson(0,10) = {result:.4f}, expected near 0.0"

    def test_perfect_score(self):
        """Wilson(100, 100, z=1.96): 100% success rate, lower bound still < 1.0."""
        result = wilson_lower_bound(100, 100, z=1.96)
        assert 0.9 < result < 1.0, \
            f"Wilson(100,100) = {result:.4f}, expected high but < 1.0"

    def test_wilson_monotonic_with_sample_size(self):
        """At fixed p_hat, Wilson lower bound should increase with n."""
        p_hat = 2 / 3
        results = [
            wilson_lower_bound(int(p_hat * n), n)
            for n in [10, 50, 100, 500, 1000]
        ]
        for i in range(len(results) - 1):
            assert results[i] < results[i + 1], \
                f"Wilson not monotonic: n={[10,50,100,500,1000][i]} -> {results[i]:.4f}, " \
                f"n={[10,50,100,500,1000][i+1]} -> {results[i+1]:.4f}"

    def test_wilson_bounded_below_zero(self):
        """Wilson lower bound should never be negative (max(0, ...) guard)."""
        result = wilson_lower_bound(1, 1000, z=1.96)
        assert result >= 0.0, f"Wilson should be >= 0, got {result}"


# =====================================================================
# Test: Tier determination (Section 2, defect 2 fix)
# =====================================================================

class TestTierDetermination:
    """Design report Section 2, defect 2 fix: tier determination logic."""

    def test_tier1_coverage_and_wilson(self):
        """coverage >= 0.8 AND wilson >= TIER1_WILSON_THRESHOLD (0.50) -> Tier 1."""
        tier = determine_tier(
            coverage=1.0, wilson=0.7, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier == 1, f"Expected Tier 1, got Tier {tier}"

    def test_tier1_exact_wilson_threshold(self):
        """wilson >= TIER1_WILSON_THRESHOLD (0.50) qualifies for Tier 1."""
        tier_exact = determine_tier(
            coverage=1.0, wilson=TIER1_WILSON_THRESHOLD, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier_exact == 1, \
            "wilson == TIER1_WILSON_THRESHOLD should qualify for Tier 1 (>=)"

        tier_above = determine_tier(
            coverage=1.0, wilson=TIER1_WILSON_THRESHOLD + 0.01, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier_above == 1, \
            "wilson > TIER1_WILSON_THRESHOLD should qualify for Tier 1"

    def test_tier2_coverage_and_correct_up(self):
        """coverage >= TIER2_COVERAGE_THRESHOLD (0.40) AND correct_up_pct >= TIER2_CORRECT_UP_THRESHOLD (0.45) -> Tier 2."""
        tier = determine_tier(
            coverage=0.5, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier == 2, f"Expected Tier 2, got Tier {tier}"

    def test_tier2_exact_coverage_threshold(self):
        """coverage must be >= TIER2_COVERAGE_THRESHOLD (0.40) for Tier 2."""
        tier_below = determine_tier(
            coverage=TIER2_COVERAGE_THRESHOLD - 0.01, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier_below != 2, \
            "coverage < TIER2_COVERAGE_THRESHOLD should NOT qualify for Tier 2"

        tier_at = determine_tier(
            coverage=TIER2_COVERAGE_THRESHOLD, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier_at == 2, \
            "coverage == TIER2_COVERAGE_THRESHOLD should qualify for Tier 2 (>=)"

    def test_tier2_exact_correct_up_threshold(self):
        """correct_up_pct >= TIER2_CORRECT_UP_THRESHOLD (0.45) qualifies for Tier 2."""
        tier_at = determine_tier(
            coverage=0.6, wilson=0.3, correct_up_pct=TIER2_CORRECT_UP_THRESHOLD, noise_ratio=0.2
        )
        assert tier_at == 2, \
            "correct_up_pct == TIER2_CORRECT_UP_THRESHOLD should qualify for Tier 2 (>=)"

        tier_above = determine_tier(
            coverage=0.6, wilson=0.3, correct_up_pct=TIER2_CORRECT_UP_THRESHOLD + 0.01, noise_ratio=0.2
        )
        assert tier_above == 2, \
            "correct_up_pct > TIER2_CORRECT_UP_THRESHOLD should qualify for Tier 2"

    def test_tier3_correct_up_only(self):
        """correct_up_pct >= TIER3_CORRECT_UP_THRESHOLD (0.35) -> Tier 3 (when Tier 1/2 conditions not met."""
        tier = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.45, noise_ratio=0.3
        )
        assert tier == 3, f"Expected Tier 3, got Tier {tier}"

    def test_tier3_exact_threshold(self):
        """correct_up_pct >= TIER3_CORRECT_UP_THRESHOLD (0.35) qualifies for Tier 3."""
        tier_at = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=TIER3_CORRECT_UP_THRESHOLD, noise_ratio=0.3
        )
        assert tier_at == 3, \
            "correct_up_pct == TIER3_CORRECT_UP_THRESHOLD should qualify for Tier 3 (>=)"

        tier_above = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=TIER3_CORRECT_UP_THRESHOLD + 0.01, noise_ratio=0.3
        )
        assert tier_above == 3, \
            "correct_up_pct > TIER3_CORRECT_UP_THRESHOLD should qualify for Tier 3"

    def test_tier4_low_correct_up(self):
        """correct_up_pct < TIER3_CORRECT_UP_THRESHOLD (0.35) -> Tier 4."""
        tier = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.34, noise_ratio=0.3
        )
        assert tier == 4, f"Expected Tier 4, got Tier {tier}"

    def test_tier4_noise_filter_zero_correct_up(self):
        """correct_up_pct <= 0 -> Tier 4 (noise filter)."""
        tier = determine_tier(
            coverage=1.0, wilson=0.8, correct_up_pct=0.0, noise_ratio=0.1
        )
        assert tier == 4, \
            "correct_up_pct = 0 should be filtered to Tier 4"

    def test_tier4_noise_filter_below_noise_ratio(self):
        """correct_up_pct < noise_ratio -> Tier 4 (noise filter)."""
        tier = determine_tier(
            coverage=1.0, wilson=0.8, correct_up_pct=0.3, noise_ratio=0.5
        )
        assert tier == 4, \
            "correct_up_pct < noise_ratio should be filtered to Tier 4"

    def test_tier4_noise_filter_equal_noise_ratio(self):
        """correct_up_pct == noise_ratio should pass the filter (>=)."""
        tier = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.45, noise_ratio=0.45
        )
        assert tier == 3, \
            "correct_up_pct == noise_ratio should pass filter (>=), giving Tier 3"

    def test_tier1_takes_priority_over_tier2(self):
        """When both Tier 1 and Tier 2 conditions met, Tier 1 wins."""
        tier = determine_tier(
            coverage=1.0, wilson=0.7, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier == 1, "Tier 1 should take priority when all conditions met"

    def test_tier2_takes_priority_over_tier3(self):
        """When both Tier 2 and Tier 3 conditions met, Tier 2 wins."""
        tier = determine_tier(
            coverage=0.6, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier == 2, "Tier 2 should take priority over Tier 3"


# =====================================================================
# Test: Sort key (Section 5)
# =====================================================================

class TestSortKey:
    """Design report Section 5: sort by (tier asc, correct_up_pct desc, wilson desc)."""

    def test_lower_tier_sorts_first(self):
        """Lower tier should sort before higher tier."""
        key_a = sort_key(tier=1, correct_up_pct=0.5, wilson=0.5)
        key_b = sort_key(tier=2, correct_up_pct=0.9, wilson=0.9)
        assert key_a < key_b, "Tier 1 should sort before Tier 2"

    def test_same_tier_higher_correct_up_sorts_first(self):
        """Within same tier, higher correct_up_pct sorts first."""
        key_a = sort_key(tier=2, correct_up_pct=0.7, wilson=0.5)
        key_b = sort_key(tier=2, correct_up_pct=0.5, wilson=0.5)
        assert key_a < key_b, "Higher correct_up_pct should sort first"

    def test_same_tier_same_correct_up_higher_wilson_sorts_first(self):
        """Within same tier and correct_up_pct, higher wilson sorts first."""
        key_a = sort_key(tier=2, correct_up_pct=0.6, wilson=0.7)
        key_b = sort_key(tier=2, correct_up_pct=0.6, wilson=0.5)
        assert key_a < key_b, "Higher wilson should sort first"

    def test_sort_order_matches_design_report(self):
        """Full sort example: verify ordering of multiple products."""
        products = [
            {'tier': 2, 'correct_up_pct': 0.6, 'wilson': 0.5},
            {'tier': 1, 'correct_up_pct': 0.8, 'wilson': 0.7},
            {'tier': 1, 'correct_up_pct': 0.8, 'wilson': 0.6},
            {'tier': 2, 'correct_up_pct': 0.7, 'wilson': 0.4},
            {'tier': 3, 'correct_up_pct': 0.45, 'wilson': 0.3},
        ]
        sorted_products = sorted(
            products,
            key=lambda x: sort_key(x['tier'], x['correct_up_pct'], x['wilson'])
        )
        expected_order = [
            (1, 0.8, 0.7),  # Tier 1, highest wilson
            (1, 0.8, 0.6),  # Tier 1, lower wilson
            (2, 0.7, 0.4),  # Tier 2, higher correct_up
            (2, 0.6, 0.5),  # Tier 2, lower correct_up
            (3, 0.45, 0.3), # Tier 3
        ]
        actual_order = [
            (p['tier'], p['correct_up_pct'], p['wilson'])
            for p in sorted_products
        ]
        assert actual_order == expected_order, \
            f"Sort order mismatch: {actual_order}"


# =====================================================================
# Test: Lots allocation (Section 5, Phase 2 Step 4)
# =====================================================================

class TestLotsAllocation:
    """Design report Section 5, Phase 2 Step 4: lots allocation."""

    def test_tier1_gets_2_lots(self):
        assert allocate_lots(1) == 2, "Tier 1 should get 2 lots"

    def test_tier2_gets_1_lot(self):
        assert allocate_lots(2) == 1, "Tier 2 should get 1 lot"

    def test_tier3_gets_1_lot(self):
        """Tier 3 also gets TIER2_LOTS (1 lot) per codebase implementation."""
        assert allocate_lots(3) == 1, "Tier 3 should get 1 lot"

    def test_tier4_gets_1_lot(self):
        """Tier 4 also gets TIER2_LOTS (1 lot) per codebase implementation."""
        assert allocate_lots(4) == 1, "Tier 4 should get 1 lot"


# =====================================================================
# Test: correct_up_pct calculation (Section 2, defect 1 fix)
# =====================================================================

class TestCorrectUpPct:
    """Design report Section 3.4: correct_up_pct = Σ[w_i × (cr_i + cf_i)] / Σ[w_i × total_i]."""

    def test_single_month_full_correct(self):
        """100% correct in a single month."""
        weights = resolve_month_weights(1)
        data = [{'cr': 100, 'wr': 0}]
        result = compute_correct_up_pct(data, weights)
        assert result == pytest.approx(1.0, abs=1e-10)

    def test_single_month_no_correct(self):
        """0% correct in a single month."""
        weights = resolve_month_weights(1)
        data = [{'cr': 0, 'wr': 100}]
        result = compute_correct_up_pct(data, weights)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_single_month_half_correct(self):
        """50% correct in a single month."""
        weights = resolve_month_weights(1)
        data = [{'cr': 50, 'wr': 50}]
        result = compute_correct_up_pct(data, weights)
        assert result == pytest.approx(0.5, abs=1e-10)

    def test_weighted_average_three_months(self):
        """Weighted average across 3 months with different ratios."""
        weights = resolve_month_weights(3)  # (0.4375, 0.3125, 0.25)
        data = [
            {'cr': 80, 'wr': 20},  # 0.80
            {'cr': 90, 'wr': 10},  # 0.90
            {'cr': 70, 'wr': 30},  # 0.70
        ]
        result = compute_correct_up_pct(data, weights)
        expected = 0.4375 * 0.80 + 0.3125 * 0.90 + 0.25 * 0.70
        assert result == pytest.approx(expected, abs=1e-10), \
            f"Expected {expected}, got {result}"

    def test_zero_rise_month_skipped(self):
        """Month with total=0 should not contribute to correct_up_pct."""
        weights = resolve_month_weights(2)
        data = [
            {'cr': 60, 'wr': 40},  # total=100, cr+cf=60
            {'cr': 0, 'wr': 0},    # total=0, contributes nothing
        ]
        result = compute_correct_up_pct(data, weights)
        # Only month 1 has non-zero total: (cr+cf)/total = 60/100 = 0.60
        expected = 0.60
        assert result == pytest.approx(expected, abs=1e-4)

    def test_result_bounded_zero_to_one(self):
        """correct_up_pct must be in [0, 1]."""
        weights = resolve_month_weights(3)
        data = [
            {'cr': 80, 'wr': 20},
            {'cr': 90, 'wr': 10},
            {'cr': 70, 'wr': 30},
        ]
        result = compute_correct_up_pct(data, weights)
        assert 0.0 <= result <= 1.0, f"correct_up_pct = {result} out of [0, 1]"

    def test_result_bounded_edge_all_zero(self):
        """All months with zero rise -> correct_up_pct = 0.0."""
        weights = resolve_month_weights(2)
        data = [
            {'cr': 0, 'wr': 0},
            {'cr': 0, 'wr': 0},
        ]
        result = compute_correct_up_pct(data, weights)
        assert result == 0.0


# =====================================================================
# Test: noise_ratio calculation (Section 2, defect 3 fix)
# =====================================================================

class TestNoiseRatio:
    """Design report Section 2, defect 3 fix: noise_ratio = sum(w_i * (WR_i+WF_i)/Total_i)."""

    def test_no_noise(self):
        """Zero wrong predictions -> noise_ratio = 0."""
        weights = resolve_month_weights(1)
        data = [{'wr': 0, 'wf': 0, 'total': 100}]
        result = compute_noise_ratio(data, weights)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_all_noise(self):
        """All volume is wrong -> noise_ratio = 1.0."""
        weights = resolve_month_weights(1)
        data = [{'wr': 50, 'wf': 50, 'total': 100}]
        result = compute_noise_ratio(data, weights)
        assert result == pytest.approx(1.0, abs=1e-10)

    def test_weighted_noise_two_months(self):
        """Weighted noise across 2 months."""
        weights = resolve_month_weights(2)  # (7/12, 5/12)
        data = [
            {'wr': 20, 'wf': 10, 'total': 100},  # noise = 30/100 = 0.30
            {'wr': 40, 'wf': 10, 'total': 100},  # noise = 50/100 = 0.50
        ]
        result = compute_noise_ratio(data, weights)
        expected = (7 / 12) * 0.30 + (5 / 12) * 0.50
        assert result == pytest.approx(expected, abs=1e-4)

    def test_zero_total_month_skipped(self):
        """Month with total=0 should not contribute to noise_ratio."""
        weights = resolve_month_weights(2)
        data = [
            {'wr': 30, 'wf': 10, 'total': 100},  # noise = 0.40
            {'wr': 0, 'wf': 0, 'total': 0},       # skipped
        ]
        result = compute_noise_ratio(data, weights)
        expected = (7 / 12) * 0.40
        assert result == pytest.approx(expected, abs=1e-4)


# =====================================================================
# Test: Numerical example (Section 7)
# =====================================================================

class TestNumericalExample:
    """Design report Section 7: full numerical example with Product A and B.

    Note: expected values reflect the spec-aligned formulas (P2-01/02/03 fixes):
    correct_up_pct uses (cr+cf)/total, coverage is count-based (n/5).

    Product A (3 months): correct_up_pct ~ 0.807, noise_ratio ~ 0.192,
                           coverage = 0.6 (3/5), Tier 2
    Product B (2 months): correct_up_pct ~ 0.492, noise_ratio ~ 0.428,
                           coverage = 0.4 (2/5), Tier 2
    """

    @pytest.fixture
    def product_a_data(self):
        """Construct month data for Product A that yields the design report values.

        Using uniform per-month ratios to get clean numbers:
          Each month: CR=807, WR=193, CF=808, WF=192, Total=2000
          rise = 1000, CR/rise = 0.807, (WR+WF)/Total = 385/2000 = 0.1925
        """
        month_data = [
            {'cr': 807, 'wr': 193, 'cf': 808, 'wf': 192, 'total': 2000},
            {'cr': 807, 'wr': 193, 'cf': 808, 'wf': 192, 'total': 2000},
            {'cr': 807, 'wr': 193, 'cf': 808, 'wf': 192, 'total': 2000},
        ]
        weights = resolve_month_weights(3)
        return month_data, weights

    @pytest.fixture
    def product_b_data(self):
        """Construct month data for Product B that yields the design report values.

        Month 1 (w=7/12): CR=561, WR=439, CF=100, WF=275, Total=1375
          rise=1000, CR/rise = 0.561, (WR+WF)/Total = 714/1375 ~ 0.5193
        Month 2 (w=5/12): CR=0, WR=30, CF=70, WF=0, Total=100
          rise=30, CR/rise = 0, (WR+WF)/Total = 30/100 = 0.30
        """
        month_data = [
            {'cr': 561, 'wr': 439, 'cf': 100, 'wf': 275, 'total': 1375},
            {'cr': 0, 'wr': 30, 'cf': 70, 'wf': 0, 'total': 100},
        ]
        weights = resolve_month_weights(2)
        return month_data, weights

    def test_product_a_correct_up_pct(self, product_a_data):
        """Product A: correct_up_pct ~ 0.807."""
        month_data, weights = product_a_data
        result = compute_correct_up_pct(month_data, weights)
        # Each month: CR/rise = 807/1000 = 0.807, weights sum to 1.0
        assert result == pytest.approx(0.807, abs=0.002), \
            f"Product A correct_up_pct = {result:.4f}, expected ~0.807"

    def test_product_a_noise_ratio(self, product_a_data):
        """Product A: noise_ratio ~ 0.192."""
        month_data, weights = product_a_data
        result = compute_noise_ratio(month_data, weights)
        # Each month: (WR+WF)/Total = 385/2000 = 0.1925, weights sum to 1.0
        assert result == pytest.approx(0.192, abs=0.005), \
            f"Product A noise_ratio = {result:.4f}, expected ~0.192"

    def test_product_a_coverage(self, product_a_data):
        """Product A: coverage = 0.6 (3 active months / 5)."""
        month_data, weights = product_a_data
        result = compute_coverage(month_data, weights)
        assert result == pytest.approx(0.6, abs=1e-10), \
            f"Product A coverage = {result:.4f}, expected 0.6 (3/5)"

    def test_product_a_wilson(self, product_a_data):
        """Product A: Wilson should be well above 0.6 threshold."""
        month_data, weights = product_a_data
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise)
        assert wilson > 0.6, \
            f"Product A wilson = {wilson:.4f}, expected > 0.6 for Tier 1"

    def test_product_a_tier(self, product_a_data):
        """Product A: Tier 2 (coverage=0.6 < 0.8, so not Tier 1; meets Tier 2)."""
        month_data, weights = product_a_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise)

        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        assert tier == 2, \
            f"Product A tier = {tier}, expected 2 " \
            f"(coverage={coverage:.3f}, wilson={wilson:.4f}, " \
            f"cup={correct_up_pct:.4f}, noise={noise_ratio:.4f})"

    def test_product_b_correct_up_pct(self, product_b_data):
        """Product B: correct_up_pct ~ 0.492 (cr+cf in numerator)."""
        month_data, weights = product_b_data
        result = compute_correct_up_pct(month_data, weights)
        # Σ[w_i*(cr+cf)] / Σ[w_i*total]
        # = [(7/12)*661 + (5/12)*70] / [(7/12)*1375 + (5/12)*100]
        # = 414.75 / 843.75 ~ 0.4916
        assert result == pytest.approx(0.492, abs=0.005), \
            f"Product B correct_up_pct = {result:.4f}, expected ~0.492"

    def test_product_b_noise_ratio(self, product_b_data):
        """Product B: noise_ratio ~ 0.428."""
        month_data, weights = product_b_data
        result = compute_noise_ratio(month_data, weights)
        # (7/12)*(714/1375) + (5/12)*(30/100) = 0.3029 + 0.125 = 0.4279
        assert result == pytest.approx(0.428, abs=0.005), \
            f"Product B noise_ratio = {result:.4f}, expected ~0.428"

    def test_product_b_coverage(self, product_b_data):
        """Product B: coverage = 0.4 (2 active months / 5)."""
        month_data, weights = product_b_data
        result = compute_coverage(month_data, weights)
        # Both months have non-zero totals -> 2/5 = 0.4
        assert result == pytest.approx(0.4, abs=0.01), \
            f"Product B coverage = {result:.4f}, expected 0.4 (2/5)"

    def test_product_b_tier(self, product_b_data):
        """Product B: Tier 2 (cup > noise, coverage >= 0.4, cup >= 0.45)."""
        month_data, weights = product_b_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise) if total_rise > 0 else 0.0

        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        assert tier == 2, \
            f"Product B tier = {tier}, expected 2 " \
            f"(cup={correct_up_pct:.4f}, noise={noise_ratio:.4f}, coverage={coverage:.3f})"

    def test_product_a_sorts_before_product_b(self, product_a_data, product_b_data):
        """Product A (higher correct_up_pct) should sort before Product B."""
        a_data, a_weights = product_a_data
        b_data, b_weights = product_b_data

        a_cup = compute_correct_up_pct(a_data, a_weights)
        a_nr = compute_noise_ratio(a_data, a_weights)
        a_cov = compute_coverage(a_data, a_weights)
        a_cr = sum(m['cr'] for m in a_data)
        a_rise = sum(m['cr'] + m['wr'] for m in a_data)
        a_wilson = wilson_lower_bound(a_cr, a_rise)
        a_tier = determine_tier(a_cov, a_wilson, a_cup, a_nr)

        b_cup = compute_correct_up_pct(b_data, b_weights)
        b_nr = compute_noise_ratio(b_data, b_weights)
        b_cov = compute_coverage(b_data, b_weights)
        b_cr = sum(m['cr'] for m in b_data)
        b_rise = sum(m['cr'] + m['wr'] for m in b_data)
        b_wilson = wilson_lower_bound(b_cr, b_rise) if b_rise > 0 else 0.0
        b_tier = determine_tier(b_cov, b_wilson, b_cup, b_nr)

        key_a = sort_key(a_tier, a_cup, a_wilson)
        key_b = sort_key(b_tier, b_cup, b_wilson)
        assert key_a < key_b, \
            f"Product A (tier={a_tier}) should sort before Product B (tier={b_tier})"

    def test_product_a_lots(self, product_a_data):
        """Product A (Tier 2) should get 1 lot."""
        month_data, weights = product_a_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise)
        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        lots = allocate_lots(tier)
        assert lots == 1, f"Product A should get 1 lot (tier={tier})"

    def test_product_b_lots(self, product_b_data):
        """Product B (Tier 2) should get 1 lot."""
        month_data, weights = product_b_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise) if total_rise > 0 else 0.0
        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        lots = allocate_lots(tier)
        assert lots == 1, f"Product B should get 1 lot (tier={tier})"


# =====================================================================
# Test: Regression guards
# =====================================================================

class TestRegressionGuards:
    """Cross-cutting regression tests for common failure modes."""

    def test_correct_up_pct_always_bounded(self):
        """correct_up_pct must always be in [0, 1] regardless of input."""
        weights = resolve_month_weights(5)
        test_cases = [
            [{'cr': 0, 'wr': 0}] * 5,
            [{'cr': 1000, 'wr': 0}] * 5,
            [{'cr': 0, 'wr': 1000}] * 5,
            [{'cr': 1, 'wr': 999}] * 5,
        ]
        for data in test_cases:
            result = compute_correct_up_pct(data, weights)
            assert 0.0 <= result <= 1.0, \
                f"correct_up_pct = {result} out of [0, 1] for data={data}"

    def test_noise_ratio_always_bounded(self):
        """noise_ratio must always be in [0, 1] (since WR+WF <= Total)."""
        weights = resolve_month_weights(3)
        data = [
            {'wr': 30, 'wf': 20, 'total': 100},
            {'wr': 10, 'wf': 5, 'total': 50},
            {'wr': 0, 'wf': 0, 'total': 200},
        ]
        result = compute_noise_ratio(data, weights)
        assert 0.0 <= result <= 1.0, \
            f"noise_ratio = {result} out of [0, 1]"

    def test_coverage_always_bounded(self):
        """coverage must always be in [0, 1]."""
        weights = resolve_month_weights(3)
        data = [
            {'cr': 100, 'wr': 0},
            {'cr': 0, 'wr': 100},
            {'cr': 50, 'wr': 50},
        ]
        result = compute_coverage(data, weights)
        assert 0.0 <= result <= 1.0, \
            f"coverage = {result} out of [0, 1]"

    def test_wilson_with_total_cr_exceeding_rise(self):
        """Edge case: if somehow total_cr > total_rise, Wilson should still be valid.

        In practice this shouldn't happen (CR <= rise), but the formula
        should not crash or return NaN.
        """
        result = wilson_lower_bound(100, 100)
        assert isinstance(result, float)
        assert result >= 0.0

    def test_tier_determination_with_extreme_values(self):
        """Tier determination should handle extreme but valid inputs."""
        # Very high correct_up_pct but zero coverage
        tier = determine_tier(
            coverage=0.0, wilson=0.0, correct_up_pct=0.99, noise_ratio=0.01
        )
        assert tier == 3, \
            "High correct_up_pct with zero coverage should be Tier 3 (cup > TIER3_CORRECT_UP_THRESHOLD)"

        # coverage = 1.0 but wilson just below threshold
        tier = determine_tier(
            coverage=1.0, wilson=TIER1_WILSON_THRESHOLD - 0.01, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier == 2, \
            "coverage=1.0 but wilson < TIER1_WILSON_THRESHOLD should fall to Tier 2 if conditions met"

    def test_month_weights_consistency_with_codebase(self):
        """Verify our pure function matches the codebase's class constants."""
        assert MONTH_WEIGHTS_5 == (0.35, 0.25, 0.20, 0.12, 0.08)
        assert MAX_MONTHS_FOR_SCORING == 5
        # 五唯一性修复：阈值已统一从 final_three_layer_config 导入（规范值 0.50/0.40/0.45/0.35）
        assert TIER1_WILSON_THRESHOLD == 0.50
        assert TIER2_COVERAGE_THRESHOLD == 0.40
        assert TIER2_CORRECT_UP_THRESHOLD == 0.45
        assert TIER3_CORRECT_UP_THRESHOLD == 0.35
        assert TIER1_LOTS == 2
        assert TIER2_LOTS == 1

    def test_wilson_formula_matches_codebase(self):
        """Verify Wilson formula implementation matches width_cache._wilson_lower_bound.

        The codebase implementation:
            p = pos / total
            n = total
            denom = 1.0 + z*z / n
            center = p + z*z / (2*n)
            spread = z * sqrt(p*(1-p)/n + z*z/(4*n*n))
            return max(0.0, (center - spread) / denom)
        """
        # Test a few known values to ensure formula parity
        assert wilson_lower_bound(0, 0) == 0.0
        assert wilson_lower_bound(50, 100) > 0.3
        assert wilson_lower_bound(50, 100) < 0.6
        # Symmetry: Wilson(50, 100) should be around 0.40-0.50
        result = wilson_lower_bound(50, 100, z=1.96)
        assert 0.40 < result < 0.50, \
            f"Wilson(50,100) = {result:.4f}, expected ~0.40-0.50"

    def test_get_scoring_months_falls_back_to_config_mapping(self):
        """Scoring months should prefer the runtime month window when ParamsService has no loaded params."""

        class _EmptyParams:
            def get_instrument_meta(self, _fid):
                return {'product': 'IF', 'year_month': '2606'}

            def get(self, key, default=None):
                return default

        service = WidthCacheQueryService()
        service._get_params = lambda: _EmptyParams()

        months = service._get_scoring_months(1, ['2602', '2606', '2609', '2612'])

        assert months == ['2609', '2612']

    def test_register_option_preserves_runtime_state_and_migrates_status_bucket(self):
        """Re-registering a live option must move its current state to the new future/month bucket."""

        class _FakeParams:
            def get_instrument_meta(self, fid):
                mapping = {
                    101: {'product': 'FG', 'year_month': '2607', 'exchange': 'CZCE'},
                    102: {'product': 'FG', 'year_month': '2608', 'exchange': 'CZCE'},
                }
                return mapping.get(fid)

            def get(self, key, default=None):
                return default

        cache = WidthStrengthCache(params_service=_FakeParams())

        cache.register_option(
            instrument_id='FG607C2000',
            underlying_product='FG',
            month='2607',
            strike_price=2000,
            option_type='CALL',
            initial_price=12,
            underlying_future_id=101,
            internal_id=9001,
        )

        assert cache._status_counts[101]['2607']['CALL']['other'] == 1

        cache.register_option(
            instrument_id='FG608C2000',
            underlying_product='FG',
            month='2608',
            strike_price=2000,
            option_type='CALL',
            initial_price=0,
            underlying_future_id=102,
            internal_id=9001,
        )

        assert cache._months[102] == ['2608']
        assert cache._status_counts[101]['2607']['CALL']['other'] == 0
        assert cache._status_counts[102]['2608']['CALL']['other'] == 1
        assert cache._current_status[9001] == 'other'


# =====================================================================
# Test: compute_th (Section 3.2 diagnostic indicator)
# =====================================================================

class TestComputeTh:
    """Design report Section 3.2: th = Σ[w_i × CR_i / (CR_i + CF_i)]."""

    def test_single_month_full_health(self):
        """100% correct fall -> th = 1.0."""
        weights = resolve_month_weights(1)
        data = [{'cr': 100, 'cf': 0}]
        result = compute_th(data, weights)
        assert result == pytest.approx(1.0, abs=1e-10)

    def test_single_month_zero_health(self):
        """0% correct fall -> th = 0.0."""
        weights = resolve_month_weights(1)
        data = [{'cr': 0, 'cf': 100}]
        result = compute_th(data, weights)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_single_month_half(self):
        """50% correct fall -> th = 0.5."""
        weights = resolve_month_weights(1)
        data = [{'cr': 50, 'cf': 50}]
        result = compute_th(data, weights)
        assert result == pytest.approx(0.5, abs=1e-10)

    def test_zero_sync_total_skipped(self):
        """Month with cr=0 and cf=0 should not contribute."""
        weights = resolve_month_weights(2)
        data = [
            {'cr': 80, 'cf': 20},  # 0.80
            {'cr': 0, 'cf': 0},    # skipped
        ]
        result = compute_th(data, weights)
        expected = (7 / 12) * 0.80
        assert result == pytest.approx(expected, abs=1e-4)

    def test_th_bounded_zero_to_one(self):
        """th must be in [0, 1]."""
        weights = resolve_month_weights(3)
        data = [
            {'cr': 80, 'cf': 20},
            {'cr': 30, 'cf': 70},
            {'cr': 50, 'cf': 50},
        ]
        result = compute_th(data, weights)
        assert 0.0 <= result <= 1.0


# =====================================================================
# Test: compute_ra (Section 3.2 diagnostic indicator)
# =====================================================================

class TestComputeRa:
    """Design report Section 3.2: ra = Σ[w_i × (1 - WR_i / (CR_i + WR_i))]."""

    def test_single_month_no_divergence(self):
        """WR=0 -> ra = 1.0 (no divergence)."""
        weights = resolve_month_weights(1)
        data = [{'cr': 100, 'wr': 0}]
        result = compute_ra(data, weights)
        assert result == pytest.approx(1.0, abs=1e-10)

    def test_single_month_full_divergence(self):
        """CR=0, WR>0 -> ra = 0.0 (full divergence)."""
        weights = resolve_month_weights(1)
        data = [{'cr': 0, 'wr': 100}]
        result = compute_ra(data, weights)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_single_month_half(self):
        """CR=WR -> ra = 0.5."""
        weights = resolve_month_weights(1)
        data = [{'cr': 50, 'wr': 50}]
        result = compute_ra(data, weights)
        assert result == pytest.approx(0.5, abs=1e-10)

    def test_zero_rise_skipped(self):
        """Month with rise=0 should not contribute to ra."""
        weights = resolve_month_weights(2)
        data = [
            {'cr': 80, 'wr': 20},  # ra contribution = 0.80
            {'cr': 0, 'wr': 0},    # skipped
        ]
        result = compute_ra(data, weights)
        expected = (7 / 12) * 0.80
        assert result == pytest.approx(expected, abs=1e-4)

    def test_ra_bounded_zero_to_one(self):
        """ra must be in [0, 1]."""
        weights = resolve_month_weights(3)
        data = [
            {'cr': 80, 'wr': 20},
            {'cr': 30, 'wr': 70},
            {'cr': 50, 'wr': 50},
        ]
        result = compute_ra(data, weights)
        assert 0.0 <= result <= 1.0


# =====================================================================
# Test: Tier 4 filtering (BUG-9 fix)
# =====================================================================

class TestTier4Filtering:
    """BUG-9 fix: Tier 4 futures must be filtered out (design report: 不交易)."""

    def test_tier4_is_not_tradable(self):
        """Tier 4 indicates 'do not trade' per design report Section 2."""
        # Product B from design report example: correct_up_pct < noise_ratio
        tier = determine_tier(
            coverage=0.6, wilson=0.514,
            correct_up_pct=0.327, noise_ratio=0.428
        )
        assert tier == 4, "Product B should be Tier 4"

    def test_tier4_filtered_from_results(self):
        """When all futures are Tier 4, select_otm_targets_by_volume returns []."""
        # This is validated end-to-end in runtime_verify_option_sort.py
        # Here we verify the determine_tier logic that drives the filter
        tier = determine_tier(
            coverage=0.3, wilson=0.3,
            correct_up_pct=0.2, noise_ratio=0.5
        )
        assert tier == 4
        # Per BUG-9 fix, tier==4 futures are skipped in select_otm_targets_by_volume

    def test_tier1_is_tradable(self):
        """Tier 1 futures pass the filter."""
        tier = determine_tier(
            coverage=1.0, wilson=0.787,
            correct_up_pct=0.807, noise_ratio=0.192
        )
        assert tier == 1
        # Per BUG-9 fix, only tier != 4 futures are added to future_scores


# =====================================================================
# Test: resolve_month_weights truncation (BUG-12 fix)
# =====================================================================

class TestMonthWeightsTruncation:
    """BUG-12 fix: n_months > 5 returns MONTH_WEIGHTS_5 (5 elements)."""

    def test_six_months_returns_five_weights(self):
        weights = resolve_month_weights(6)
        assert len(weights) == 5

    def test_eight_months_returns_five_weights(self):
        weights = resolve_month_weights(8)
        assert len(weights) == 5

    def test_six_months_matches_month_weights_5(self):
        weights = resolve_month_weights(6)
        assert weights == pytest.approx(MONTH_WEIGHTS_5, abs=1e-10)

    def test_five_months_unchanged(self):
        """n_months=5 should still return normalized MONTH_WEIGHTS_5."""
        weights = resolve_month_weights(5)
        assert weights == pytest.approx(MONTH_WEIGHTS_5, abs=1e-10)

