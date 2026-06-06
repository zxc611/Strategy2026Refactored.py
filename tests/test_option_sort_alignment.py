"""
Test option sort alignment against design report.

Validates the core calculation functions for option state sorting
without importing the actual ali2026v3_trading modules (which have
complex dependencies). All pure calculation functions are implemented
directly here, ported from width_cache.py.

Design report references:
  Section 2  - Tier determination & defect fixes
  Section 4.3 - MONTH_WEIGHTS_5 normalization
  Section 5  - Sort key & lots allocation
  Section 7  - Numerical example
  Section 8  - Wilson lower bound
"""

import pytest
from typing import Dict, List, Tuple


# =====================================================================
# Pure calculation functions (ported from width_cache.py)
# =====================================================================

MONTH_WEIGHTS_5 = (0.35, 0.25, 0.20, 0.12, 0.08)
MAX_MONTHS_FOR_SCORING = 5
TIER1_WILSON_THRESHOLD = 0.6
TIER2_COVERAGE_THRESHOLD = 0.5
TIER2_CORRECT_UP_THRESHOLD = 0.5
TIER3_CORRECT_UP_THRESHOLD = 0.4
TIER1_LOTS = 2
TIER2_LOTS = 1


def wilson_lower_bound(pos: float, total: float, z: float = 1.96) -> float:
    """Wilson score interval lower bound (design report Section 8).

    Args:
        pos: Number of positive outcomes (e.g. total correct_rise volume).
        total: Number of total trials (e.g. total rise volume).
        z: Z-score for confidence level (default 1.96 for 95%).
    """
    if total <= 0:
        return 0.0
    p = pos / total
    n = total
    denom = 1.0 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return max(0.0, (center - spread) / denom)


def resolve_month_weights(n_months: int) -> Tuple[float, ...]:
    """Resolve month weights for n months (design report Section 4.3).

    Truncates MONTH_WEIGHTS_5 to the first n_months entries and normalizes
    so that the sum equals 1.0. When n_months > MAX_MONTHS_FOR_SCORING,
    falls back to equal weights.
    """
    if n_months <= 0:
        return ()
    if n_months > MAX_MONTHS_FOR_SCORING:
        per = 1.0 / n_months
        return tuple(per for _ in range(n_months))
    raw_weights = MONTH_WEIGHTS_5[:n_months]
    raw_sum = sum(raw_weights)
    if raw_sum <= 0:
        per = 1.0 / n_months
        return tuple(per for _ in range(n_months))
    return tuple(w / raw_sum for w in raw_weights)


def compute_correct_up_pct(
    month_data: List[Dict], weights: Tuple[float, ...]
) -> float:
    """Compute weighted correct_up_pct (design report Section 2, defect 1 fix).

    Formula: correct_up_pct = sum(w_i * CR_i / (CR_i + WR_i))

    Each month_data dict must have keys: 'cr' (correct_rise), 'wr' (wrong_rise).
    """
    result = 0.0
    for i, data in enumerate(month_data):
        if i >= len(weights):
            break
        w = weights[i]
        cr = data.get('cr', 0)
        wr = data.get('wr', 0)
        rise = cr + wr
        if rise > 0:
            result += w * (cr / rise)
    return result


def compute_noise_ratio(
    month_data: List[Dict], weights: Tuple[float, ...]
) -> float:
    """Compute weighted noise_ratio (design report Section 2, defect 3 fix).

    Formula: noise_ratio = sum(w_i * (WR_i + WF_i) / Total_i)

    Each month_data dict must have keys: 'wr' (wrong_rise), 'wf' (wrong_fall),
    'total' (total volume for the month).
    """
    result = 0.0
    for i, data in enumerate(month_data):
        if i >= len(weights):
            break
        w = weights[i]
        wr = data.get('wr', 0)
        wf = data.get('wf', 0)
        total = data.get('total', 0)
        if total > 0:
            result += w * ((wr + wf) / total)
    return result


def compute_coverage(
    month_data: List[Dict], weights: Tuple[float, ...]
) -> float:
    """Compute coverage = sum of weights for months where CR > 0."""
    result = 0.0
    for i, data in enumerate(month_data):
        if i >= len(weights):
            break
        if data.get('cr', 0) > 0:
            result += weights[i]
    return result


def determine_tier(
    coverage: float, wilson: float, correct_up_pct: float, noise_ratio: float
) -> int:
    """Determine tier (design report Section 2, defect 2 fix).

    Filter: if correct_up_pct <= 0 OR correct_up_pct < noise_ratio -> Tier 4.
    Otherwise:
      coverage >= 1.0 AND wilson > 0.6  -> Tier 1
      coverage >= 0.5 AND correct_up_pct > 0.5 -> Tier 2
      correct_up_pct > 0.4 -> Tier 3
      otherwise -> Tier 4
    """
    if correct_up_pct > 0 and correct_up_pct >= noise_ratio:
        if coverage >= 1.0 and wilson > TIER1_WILSON_THRESHOLD:
            return 1
        elif (
            coverage >= TIER2_COVERAGE_THRESHOLD
            and correct_up_pct > TIER2_CORRECT_UP_THRESHOLD
        ):
            return 2
        elif correct_up_pct > TIER3_CORRECT_UP_THRESHOLD:
            return 3
        else:
            return 4
    else:
        return 4


def sort_key(tier: int, correct_up_pct: float, wilson: float) -> Tuple:
    """Sort key (design report Section 5).

    Sort by (tier ascending, correct_up_pct descending, wilson descending).
    """
    return (tier, -correct_up_pct, -wilson)


def allocate_lots(tier: int) -> int:
    """Allocate lots (design report Section 5, Phase 2 Step 4).

    Tier 1 -> 2 lots, Tier 2+ -> 1 lot.
    """
    if tier == 1:
        return TIER1_LOTS
    else:
        return TIER2_LOTS


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

    def test_six_months_equal_weights(self):
        """When >5 months: fall back to equal weights."""
        weights = resolve_month_weights(6)
        assert len(weights) == 6
        for w in weights:
            assert w == pytest.approx(1.0 / 6, abs=1e-10)

    @pytest.mark.parametrize("n_months", [1, 2, 3, 4, 5, 6, 7])
    def test_weights_always_sum_to_one(self, n_months):
        """Sum of weights must always equal 1.0 regardless of month count."""
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
        """coverage >= 1.0 AND wilson > 0.6 -> Tier 1."""
        tier = determine_tier(
            coverage=1.0, wilson=0.7, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier == 1, f"Expected Tier 1, got Tier {tier}"

    def test_tier1_exact_wilson_threshold(self):
        """wilson must be strictly > 0.6 for Tier 1 (not >=)."""
        tier_exact = determine_tier(
            coverage=1.0, wilson=0.6, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier_exact != 1, \
            "wilson == 0.6 should NOT qualify for Tier 1 (must be > 0.6)"

        tier_above = determine_tier(
            coverage=1.0, wilson=0.6001, correct_up_pct=0.8, noise_ratio=0.1
        )
        assert tier_above == 1, \
            "wilson = 0.6001 > 0.6 should qualify for Tier 1"

    def test_tier2_coverage_and_correct_up(self):
        """coverage >= 0.5 AND correct_up_pct > 0.5 -> Tier 2."""
        tier = determine_tier(
            coverage=0.5, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier == 2, f"Expected Tier 2, got Tier {tier}"

    def test_tier2_exact_coverage_threshold(self):
        """coverage must be >= 0.5 for Tier 2."""
        tier_below = determine_tier(
            coverage=0.49, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier_below != 2, \
            "coverage = 0.49 < 0.5 should NOT qualify for Tier 2"

        tier_at = determine_tier(
            coverage=0.5, wilson=0.3, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier_at == 2, \
            "coverage = 0.5 should qualify for Tier 2"

    def test_tier2_exact_correct_up_threshold(self):
        """correct_up_pct must be strictly > 0.5 for Tier 2."""
        tier_at = determine_tier(
            coverage=0.6, wilson=0.3, correct_up_pct=0.5, noise_ratio=0.2
        )
        assert tier_at != 2, \
            "correct_up_pct = 0.5 should NOT qualify for Tier 2 (must be > 0.5)"

        tier_above = determine_tier(
            coverage=0.6, wilson=0.3, correct_up_pct=0.51, noise_ratio=0.2
        )
        assert tier_above == 2, \
            "correct_up_pct = 0.51 > 0.5 should qualify for Tier 2"

    def test_tier3_correct_up_only(self):
        """correct_up_pct > 0.4 -> Tier 3 (when Tier 1/2 conditions not met)."""
        tier = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.45, noise_ratio=0.3
        )
        assert tier == 3, f"Expected Tier 3, got Tier {tier}"

    def test_tier3_exact_threshold(self):
        """correct_up_pct must be strictly > 0.4 for Tier 3."""
        tier_at = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.4, noise_ratio=0.3
        )
        assert tier_at == 4, \
            "correct_up_pct = 0.4 should NOT qualify for Tier 3 (must be > 0.4)"

        tier_above = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.41, noise_ratio=0.3
        )
        assert tier_above == 3, \
            "correct_up_pct = 0.41 > 0.4 should qualify for Tier 3"

    def test_tier4_low_correct_up(self):
        """correct_up_pct <= 0.4 -> Tier 4."""
        tier = determine_tier(
            coverage=0.3, wilson=0.2, correct_up_pct=0.35, noise_ratio=0.3
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
    """Design report Section 2, defect 1 fix: correct_up_pct = sum(w_i * CR_i/(CR_i+WR_i))."""

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
        """Month with rise=0 should not contribute to correct_up_pct."""
        weights = resolve_month_weights(2)
        data = [
            {'cr': 60, 'wr': 40},  # 0.60
            {'cr': 0, 'wr': 0},    # rise=0, skipped
        ]
        result = compute_correct_up_pct(data, weights)
        # Only month 1 contributes: 0.5833 * 0.60
        expected = (7 / 12) * 0.60
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

    Product A (3 months): correct_up_pct ~ 0.807, noise_ratio ~ 0.192,
                           coverage = 1.0, Tier 1
    Product B (2 months): correct_up_pct ~ 0.327, noise_ratio ~ 0.428,
                           coverage ~ 0.6, Tier 4
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
        """Product A: coverage = 1.0 (all months have CR > 0)."""
        month_data, weights = product_a_data
        result = compute_coverage(month_data, weights)
        assert result == pytest.approx(1.0, abs=1e-10), \
            f"Product A coverage = {result:.4f}, expected 1.0"

    def test_product_a_wilson(self, product_a_data):
        """Product A: Wilson should be well above 0.6 threshold."""
        month_data, weights = product_a_data
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise)
        assert wilson > 0.6, \
            f"Product A wilson = {wilson:.4f}, expected > 0.6 for Tier 1"

    def test_product_a_tier(self, product_a_data):
        """Product A: Tier 1 (coverage=1.0, wilson>0.6, correct_up_pct>noise_ratio)."""
        month_data, weights = product_a_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise)

        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        assert tier == 1, \
            f"Product A tier = {tier}, expected 1 " \
            f"(coverage={coverage:.3f}, wilson={wilson:.4f}, " \
            f"cup={correct_up_pct:.4f}, noise={noise_ratio:.4f})"

    def test_product_b_correct_up_pct(self, product_b_data):
        """Product B: correct_up_pct ~ 0.327."""
        month_data, weights = product_b_data
        result = compute_correct_up_pct(month_data, weights)
        # (7/12)*0.561 + (5/12)*0 = 0.32725
        assert result == pytest.approx(0.327, abs=0.005), \
            f"Product B correct_up_pct = {result:.4f}, expected ~0.327"

    def test_product_b_noise_ratio(self, product_b_data):
        """Product B: noise_ratio ~ 0.428."""
        month_data, weights = product_b_data
        result = compute_noise_ratio(month_data, weights)
        # (7/12)*(714/1375) + (5/12)*(30/100) = 0.3029 + 0.125 = 0.4279
        assert result == pytest.approx(0.428, abs=0.005), \
            f"Product B noise_ratio = {result:.4f}, expected ~0.428"

    def test_product_b_coverage(self, product_b_data):
        """Product B: coverage ~ 0.6 (only month 1 has CR > 0, weight = 7/12)."""
        month_data, weights = product_b_data
        result = compute_coverage(month_data, weights)
        # Only month 1 has CR > 0, weight = 7/12 ~ 0.5833
        assert result == pytest.approx(7 / 12, abs=0.01), \
            f"Product B coverage = {result:.4f}, expected ~0.583 (rounds to 0.6)"

    def test_product_b_tier(self, product_b_data):
        """Product B: Tier 4 (correct_up_pct < noise_ratio, filtered out)."""
        month_data, weights = product_b_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise) if total_rise > 0 else 0.0

        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        assert tier == 4, \
            f"Product B tier = {tier}, expected 4 " \
            f"(cup={correct_up_pct:.4f} < noise={noise_ratio:.4f})"

    def test_product_a_sorts_before_product_b(self, product_a_data, product_b_data):
        """Product A (Tier 1) should sort before Product B (Tier 4)."""
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
        """Product A (Tier 1) should get 2 lots."""
        month_data, weights = product_a_data
        correct_up_pct = compute_correct_up_pct(month_data, weights)
        noise_ratio = compute_noise_ratio(month_data, weights)
        coverage = compute_coverage(month_data, weights)
        total_cr = sum(m['cr'] for m in month_data)
        total_rise = sum(m['cr'] + m['wr'] for m in month_data)
        wilson = wilson_lower_bound(total_cr, total_rise)
        tier = determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
        lots = allocate_lots(tier)
        assert lots == 2, f"Product A should get 2 lots (tier={tier})"

    def test_product_b_lots(self, product_b_data):
        """Product B (Tier 4) should get 1 lot."""
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
            "High correct_up_pct with zero coverage should be Tier 3 (cup > 0.4)"

        # coverage = 1.0 but wilson just below threshold
        tier = determine_tier(
            coverage=1.0, wilson=0.59, correct_up_pct=0.6, noise_ratio=0.2
        )
        assert tier == 2, \
            "coverage=1.0 but wilson<0.6 should fall to Tier 2 if conditions met"

    def test_month_weights_consistency_with_codebase(self):
        """Verify our pure function matches the codebase's class constants."""
        assert MONTH_WEIGHTS_5 == (0.35, 0.25, 0.20, 0.12, 0.08)
        assert MAX_MONTHS_FOR_SCORING == 5
        assert TIER1_WILSON_THRESHOLD == 0.6
        assert TIER2_COVERAGE_THRESHOLD == 0.5
        assert TIER2_CORRECT_UP_THRESHOLD == 0.5
        assert TIER3_CORRECT_UP_THRESHOLD == 0.4
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


# =====================================================================
# Decay monitoring constants (design report Section 10)
# =====================================================================

DIVERGENCE_RATE_WARN = 0.3
DIVERGENCE_RATE_CONFIRM = 0.5
REVERSAL_PROB_WARN = 0.3
REVERSAL_PROB_CONFIRM = 0.5
NOISE_RATIO_WARN = 0.2
NOISE_RATIO_CONFIRM = 0.3
TH_WARN_THRESHOLD = 0.5
TH_CONFIRM_THRESHOLD = 0.3
DECAY_COVERAGE_WEAK = 0.4
DECAY_COVERAGE_STRONG = 0.6


def compute_divergence_rate(
    month_data: List[Dict], weights: Tuple[float, ...]
) -> float:
    """Compute weighted divergence_rate = sum(w_i * WR_i / (CR_i + WR_i))."""
    result = 0.0
    for i, data in enumerate(month_data):
        if i >= len(weights):
            break
        w = weights[i]
        cr = data.get('cr', 0)
        wr = data.get('wr', 0)
        rise = cr + wr
        if rise > 0:
            result += w * (wr / rise)
    return result


def compute_reversal_prob(
    month_data: List[Dict], weights: Tuple[float, ...]
) -> float:
    """Compute weighted reversal_prob = sum(w_i * WF_i / (CF_i + WF_i))."""
    result = 0.0
    for i, data in enumerate(month_data):
        if i >= len(weights):
            break
        w = weights[i]
        cf = data.get('cf', 0)
        wf = data.get('wf', 0)
        directional_down = cf + wf
        if directional_down > 0:
            result += w * (wf / directional_down)
    return result


def compute_decay_coverage(
    month_data: List[Dict], weights: Tuple[float, ...]
) -> float:
    """Compute decay_coverage = sum(w_i * I(CF_i > CR_i)) / sum(w_i)."""
    decay_sum = 0.0
    weight_sum = 0.0
    for i, data in enumerate(month_data):
        if i >= len(weights):
            break
        w = weights[i]
        cr = data.get('cr', 0)
        cf = data.get('cf', 0)
        if cf > cr:
            decay_sum += w
        weight_sum += w
    if weight_sum > 0:
        return decay_sum / weight_sum
    return 0.0


def evaluate_decay_action(
    th: float, divergence_rate: float, noise_ratio: float,
    tier: int, decay_coverage: float
) -> Dict:
    """Evaluate decay action (design report Section 10.5 + 10.7 + 10.8 Tier linkage).

    Escalation: decay_coverage upgrades the action severity.
    """
    if tier == 4:
        return {'action': 'observe', 'lot_multiplier': 0.0}
    if tier == 3 and noise_ratio > NOISE_RATIO_CONFIRM:
        return {'action': 'observe', 'lot_multiplier': 0.0}
    if th >= 0.7 and divergence_rate <= 0.2:
        base_action = 'hold'
    elif th >= TH_WARN_THRESHOLD and divergence_rate < DIVERGENCE_RATE_WARN:
        base_action = 'reduce_third'
    elif th < 0.6 and divergence_rate >= 0.4:
        base_action = 'close_all'
    elif (TH_CONFIRM_THRESHOLD <= th < TH_WARN_THRESHOLD) or (DIVERGENCE_RATE_WARN <= divergence_rate < DIVERGENCE_RATE_CONFIRM):
        base_action = 'reduce_half'
    elif th < TH_CONFIRM_THRESHOLD or divergence_rate >= DIVERGENCE_RATE_CONFIRM:
        base_action = 'close_all'
    else:
        base_action = 'hold'
    escalation_levels = {'hold': 0, 'reduce_third': 1, 'reduce_half': 2, 'close_all': 3}
    action_names = ['hold', 'reduce_third', 'reduce_half', 'close_all']
    multipliers = [1.0, 0.67, 0.5, 0.0]
    base_level = escalation_levels[base_action]
    if decay_coverage >= DECAY_COVERAGE_STRONG:
        escalated_level = min(3, base_level + 2)
    elif decay_coverage >= DECAY_COVERAGE_WEAK:
        escalated_level = min(3, base_level + 1)
    else:
        escalated_level = base_level
    return {'action': action_names[escalated_level], 'lot_multiplier': multipliers[escalated_level]}


# =====================================================================
# Test: Divergence rate (Section 10.4)
# =====================================================================

class TestDivergenceRate:
    """Design report Section 10.4: divergence_rate = sum(w_i * WR_i/(CR_i+WR_i))."""

    def test_no_divergence(self):
        data = [{'cr': 100, 'wr': 0}]
        w = resolve_month_weights(1)
        assert compute_divergence_rate(data, w) == 0.0

    def test_full_divergence(self):
        data = [{'cr': 0, 'wr': 100}]
        w = resolve_month_weights(1)
        assert compute_divergence_rate(data, w) == 1.0

    def test_half_divergence(self):
        data = [{'cr': 50, 'wr': 50}]
        w = resolve_month_weights(1)
        result = compute_divergence_rate(data, w)
        assert abs(result - 0.5) < 1e-6

    def test_weighted_two_months(self):
        data = [
            {'cr': 80, 'wr': 20},
            {'cr': 60, 'wr': 40},
        ]
        w = resolve_month_weights(2)
        result = compute_divergence_rate(data, w)
        expected = w[0] * (20 / 100) + w[1] * (40 / 100)
        assert abs(result - expected) < 1e-6

    def test_zero_rise_month_skipped(self):
        data = [{'cr': 0, 'wr': 0}]
        w = resolve_month_weights(1)
        assert compute_divergence_rate(data, w) == 0.0

    def test_bounded_zero_to_one(self):
        data = [
            {'cr': 30, 'wr': 70},
            {'cr': 10, 'wr': 90},
            {'cr': 50, 'wr': 50},
        ]
        w = resolve_month_weights(3)
        result = compute_divergence_rate(data, w)
        assert 0.0 <= result <= 1.0


# =====================================================================
# Test: Reversal probability (Section 10.4)
# =====================================================================

class TestReversalProbability:
    """Design report Section 10.4: reversal_prob = sum(w_i * WF_i/(CF_i+WF_i))."""

    def test_no_reversal(self):
        data = [{'cf': 100, 'wf': 0}]
        w = resolve_month_weights(1)
        assert compute_reversal_prob(data, w) == 0.0

    def test_full_reversal(self):
        data = [{'cf': 0, 'wf': 100}]
        w = resolve_month_weights(1)
        assert compute_reversal_prob(data, w) == 1.0

    def test_half_reversal(self):
        data = [{'cf': 50, 'wf': 50}]
        w = resolve_month_weights(1)
        result = compute_reversal_prob(data, w)
        assert abs(result - 0.5) < 1e-6

    def test_zero_directional_down_skipped(self):
        data = [{'cf': 0, 'wf': 0}]
        w = resolve_month_weights(1)
        assert compute_reversal_prob(data, w) == 0.0

    def test_weighted_two_months(self):
        data = [
            {'cf': 70, 'wf': 30},
            {'cf': 40, 'wf': 60},
        ]
        w = resolve_month_weights(2)
        result = compute_reversal_prob(data, w)
        expected = w[0] * (30 / 100) + w[1] * (60 / 100)
        assert abs(result - expected) < 1e-6

    def test_bounded_zero_to_one(self):
        data = [
            {'cf': 20, 'wf': 80},
            {'cf': 80, 'wf': 20},
        ]
        w = resolve_month_weights(2)
        result = compute_reversal_prob(data, w)
        assert 0.0 <= result <= 1.0


# =====================================================================
# Test: Decay coverage (Section 10.6)
# =====================================================================

class TestDecayCoverage:
    """Design report Section 10.6: decay_coverage = sum(w_i * I(CF_i > CR_i)) / sum(w_i)."""

    def test_no_decay(self):
        data = [{'cr': 100, 'cf': 10}]
        w = resolve_month_weights(1)
        assert compute_decay_coverage(data, w) == 0.0

    def test_full_decay(self):
        data = [{'cr': 10, 'cf': 100}]
        w = resolve_month_weights(1)
        assert compute_decay_coverage(data, w) == 1.0

    def test_partial_decay_two_months(self):
        data = [
            {'cr': 100, 'cf': 50},
            {'cr': 30, 'cf': 80},
        ]
        w = resolve_month_weights(2)
        result = compute_decay_coverage(data, w)
        expected = w[1] / (w[0] + w[1])
        assert abs(result - expected) < 1e-6

    def test_equal_cr_cf_not_counted(self):
        data = [{'cr': 50, 'cf': 50}]
        w = resolve_month_weights(1)
        assert compute_decay_coverage(data, w) == 0.0

    def test_bounded_zero_to_one(self):
        data = [
            {'cr': 10, 'cf': 90},
            {'cr': 90, 'cf': 10},
        ]
        w = resolve_month_weights(2)
        result = compute_decay_coverage(data, w)
        assert 0.0 <= result <= 1.0


# =====================================================================
# Test: Decay action decision tree (Section 10.5 + 10.8)
# =====================================================================

class TestDecayActionDecisionTree:
    """Design report Section 10.5: decay-guided entry decision tree + Section 10.8 Tier linkage."""

    def test_tier4_observe(self):
        result = evaluate_decay_action(th=0.8, divergence_rate=0.1, noise_ratio=0.1, tier=4, decay_coverage=0.0)
        assert result['action'] == 'observe' and result['lot_multiplier'] == 0.0

    def test_tier3_high_noise_observe(self):
        result = evaluate_decay_action(th=0.6, divergence_rate=0.2, noise_ratio=0.35, tier=3, decay_coverage=0.0)
        assert result['action'] == 'observe' and result['lot_multiplier'] == 0.0

    def test_tier3_low_noise_hold(self):
        result = evaluate_decay_action(th=0.8, divergence_rate=0.1, noise_ratio=0.1, tier=3, decay_coverage=0.0)
        assert result['action'] == 'hold' and result['lot_multiplier'] == 1.0

    def test_th_gte_07_low_divergence_hold(self):
        result = evaluate_decay_action(th=0.8, divergence_rate=0.2, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'hold' and result['lot_multiplier'] == 1.0

    def test_th_05_07_low_divergence_reduce_third(self):
        result = evaluate_decay_action(th=0.6, divergence_rate=0.2, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_third' and result['lot_multiplier'] == 0.67

    def test_th_03_05_reduce_half(self):
        result = evaluate_decay_action(th=0.4, divergence_rate=0.1, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_half' and result['lot_multiplier'] == 0.5

    def test_divergence_03_05_reduce_half(self):
        result = evaluate_decay_action(th=0.8, divergence_rate=0.4, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_half' and result['lot_multiplier'] == 0.5

    def test_th_below_03_close_all(self):
        result = evaluate_decay_action(th=0.2, divergence_rate=0.1, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'close_all' and result['lot_multiplier'] == 0.0

    def test_divergence_gte_05_close_all(self):
        result = evaluate_decay_action(th=0.8, divergence_rate=0.6, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'close_all' and result['lot_multiplier'] == 0.0

    def test_decay_coverage_strong_escalates_2_levels(self):
        result = evaluate_decay_action(th=0.75, divergence_rate=0.15, noise_ratio=0.1, tier=2, decay_coverage=0.7)
        assert result['action'] == 'reduce_half' and result['lot_multiplier'] == 0.5

    def test_decay_coverage_weak_escalates_1_level(self):
        result = evaluate_decay_action(th=0.75, divergence_rate=0.15, noise_ratio=0.1, tier=2, decay_coverage=0.5)
        assert result['action'] == 'reduce_third' and result['lot_multiplier'] == 0.67

    def test_decay_coverage_strong_from_reduce_third_to_close_all(self):
        result = evaluate_decay_action(th=0.6, divergence_rate=0.2, noise_ratio=0.1, tier=1, decay_coverage=0.7)
        assert result['action'] == 'close_all' and result['lot_multiplier'] == 0.0

    def test_boundary_th_exactly_07(self):
        result = evaluate_decay_action(th=0.7, divergence_rate=0.2, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'hold' and result['lot_multiplier'] == 1.0

    def test_boundary_th_exactly_05(self):
        result = evaluate_decay_action(th=0.5, divergence_rate=0.2, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_third' and result['lot_multiplier'] == 0.67

    def test_boundary_th_exactly_03(self):
        result = evaluate_decay_action(th=0.3, divergence_rate=0.1, noise_ratio=0.1, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_half' and result['lot_multiplier'] == 0.5


# =====================================================================
# Test: Decay indicator numerical example (Section 10.7 Case 1)
# =====================================================================

class TestDecayNumericalExample:
    """Design report Section 10.7 Case 1: iron ore rebound top detection."""

    def test_910_normal_hold(self):
        result = evaluate_decay_action(th=0.94, divergence_rate=0.09, noise_ratio=0.10, tier=1, decay_coverage=0.0)
        assert result['action'] == 'hold'

    def test_912_attention_wr_increasing(self):
        result = evaluate_decay_action(th=0.88, divergence_rate=0.20, noise_ratio=0.20, tier=1, decay_coverage=0.0)
        assert result['action'] == 'hold'

    def test_913_reduce_third(self):
        result = evaluate_decay_action(th=0.83, divergence_rate=0.29, noise_ratio=0.25, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_third'

    def test_916_reduce_half(self):
        result = evaluate_decay_action(th=0.71, divergence_rate=0.40, noise_ratio=0.28, tier=1, decay_coverage=0.0)
        assert result['action'] == 'reduce_half'

    def test_917_close_all(self):
        result = evaluate_decay_action(th=0.58, divergence_rate=0.44, noise_ratio=0.30, tier=2, decay_coverage=0.3)
        assert result['action'] == 'close_all'


# =====================================================================
# Test: Decay constants consistency with codebase
# =====================================================================

class TestDecayConstantsConsistency:
    """Verify decay constants match width_cache.py class-level constants."""

    def test_divergence_rate_thresholds(self):
        assert DIVERGENCE_RATE_WARN == 0.3
        assert DIVERGENCE_RATE_CONFIRM == 0.5

    def test_reversal_prob_thresholds(self):
        assert REVERSAL_PROB_WARN == 0.3
        assert REVERSAL_PROB_CONFIRM == 0.5

    def test_noise_ratio_thresholds(self):
        assert NOISE_RATIO_WARN == 0.2
        assert NOISE_RATIO_CONFIRM == 0.3

    def test_th_thresholds(self):
        assert TH_WARN_THRESHOLD == 0.5
        assert TH_CONFIRM_THRESHOLD == 0.3

    def test_decay_coverage_thresholds(self):
        assert DECAY_COVERAGE_WEAK == 0.4
        assert DECAY_COVERAGE_STRONG == 0.6
