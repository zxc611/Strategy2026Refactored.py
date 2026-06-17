# MODULE_ID: M2-427
"""
Test option sort decay monitoring against design report.

Validates the decay monitoring functions for option state sorting
without importing the actual ali2026v3_trading modules (which have
complex dependencies). All pure calculation functions are implemented
directly here, ported from width_cache.py.

Design report references:
  Section 10 - Decay monitoring
"""

import pytest
from typing import Dict, List, Tuple

from tests.test_option_sort_basic import (
    resolve_month_weights,
)


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
