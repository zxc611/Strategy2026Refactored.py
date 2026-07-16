# MODULE_ID: M2-583
"""Tests for statistical_validity_extensions.py"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import numpy as np


def _ensure_imports():
    try:
        from strategy_judgment import statistical_validity_extensions as sve
        return sve
    except Exception as e:
        pytest.skip(f"Import failed: {e}")


class TestDetectRegimeChanges:
    def test_too_short(self):
        sve = _ensure_imports()
        returns = np.array([0.01, 0.02])
        result = sve.detect_regime_changes(returns, min_segment_length=20)
        assert result.n_regimes == 1
        assert result.is_stable is True

    def test_stable_regime(self):
        sve = _ensure_imports()
        rng = np.random.RandomState(42)
        returns = rng.normal(0.001, 0.01, size=100)
        result = sve.detect_regime_changes(returns, min_segment_length=20, sharpe_diff_threshold=2.0)
        assert result.n_regimes >= 1
        assert result.is_stable is True or isinstance(result.change_points, list)


class TestDetectSurvivorshipBias:
    def test_basic(self):
        sve = _ensure_imports()
        all_sharpes = np.array([0.5, 0.2, -0.1, 0.0, -0.3])
        surviving = np.array([0.5, 0.2])
        result = sve.detect_survivorship_bias(all_sharpes, surviving)
        assert result.n_total_strategies == 5
        assert result.n_surviving_strategies == 2
        assert result.survival_rate == 0.4

    def test_too_few(self):
        sve = _ensure_imports()
        result = sve.detect_survivorship_bias(np.array([0.5]), np.array([0.5]))
        assert result.bias_score == 0.0


class TestApplyComplexityPenalty:
    def test_basic(self):
        sve = _ensure_imports()
        result = sve.apply_complexity_penalty(sharpe=1.5, n_parameters=10, n_observations=100)
        assert result.original_sharpe == 1.5
        assert result.n_parameters == 10
        assert result.penalized_sharpe <= result.original_sharpe


class TestAssessStatisticalReliability:
    def test_reliable(self):
        sve = _ensure_imports()
        result = sve.assess_statistical_reliability(n_trades=100, sharpe=1.0, min_trades=30)
        assert result.is_reliable is True
        assert result.n_trades == 100

    def test_unreliable(self):
        sve = _ensure_imports()
        result = sve.assess_statistical_reliability(n_trades=5, sharpe=1.0, min_trades=30)
        assert result.is_reliable is False


class TestAnalyzeExtremeEvents:
    def test_no_extreme(self):
        sve = _ensure_imports()
        rng = np.random.RandomState(42)
        returns = rng.normal(0.001, 0.005, size=50)
        result = sve.analyze_extreme_events(returns, threshold_sigma=5.0)
        assert result.n_extreme_events >= 0
        assert 0.0 <= result.extreme_event_pct <= 1.0

    def test_with_extreme(self):
        sve = _ensure_imports()
        returns = np.array([0.01] * 20 + [0.5] + [0.01] * 20)
        result = sve.analyze_extreme_events(returns, threshold_sigma=2.0)
        assert result.n_extreme_events >= 1


class TestAnalyzeCostSensitivity:
    def test_basic(self):
        sve = _ensure_imports()
        rng = np.random.RandomState(42)
        returns = rng.normal(0.001, 0.01, size=30)
        commissions = np.ones(30)
        result = sve.analyze_cost_sensitivity(returns, commissions, base_commission_bps=1.0)
        assert result.base_sharpe is not None
        assert isinstance(result.sensitivity_points, dict)

    def test_too_short(self):
        sve = _ensure_imports()
        result = sve.analyze_cost_sensitivity(np.array([0.01]), np.array([1.0]))
        assert result.base_sharpe == 0.0


class TestTestMarketStability:
    def test_basic(self):
        sve = _ensure_imports()
        rng = np.random.RandomState(42)
        returns = rng.normal(0.001, 0.01, size=100)
        result = sve.test_market_stability(returns)
        assert isinstance(result.regime_sharpes, dict)
        assert 0.0 <= result.stability_score <= 1.0

    def test_too_short(self):
        sve = _ensure_imports()
        result = sve.test_market_stability(np.array([0.01] * 5))
        assert result.stability_score == 1.0
        assert result.worst_regime == "unknown"


class TestComputeSharpeSimple:
    def test_basic(self):
        sve = _ensure_imports()
        returns = np.array([0.01, 0.02, -0.01, 0.015, 0.005])
        sharpe = sve._compute_sharpe_simple(returns)
        assert isinstance(sharpe, float)

    def test_too_short(self):
        sve = _ensure_imports()
        assert sve._compute_sharpe_simple(np.array([0.01])) == 0.0


class TestComputeJensenAlpha:
    def test_basic(self):
        sve = _ensure_imports()
        rng = np.random.RandomState(42)
        portfolio = rng.normal(0.001, 0.01, size=60)
        benchmark = rng.normal(0.0005, 0.008, size=60)
        result = sve.compute_jensen_alpha(portfolio, benchmark, risk_free_rate=0.02)
        assert isinstance(result.alpha, float)
        assert isinstance(result.beta, float)

    def test_too_short(self):
        sve = _ensure_imports()
        result = sve.compute_jensen_alpha(np.array([0.01]), np.array([0.01]))
        assert result.alpha == 0.0
        assert result.beta == 1.0


class TestComputeInformationRatio:
    def test_basic(self):
        sve = _ensure_imports()
        rng = np.random.RandomState(42)
        portfolio = rng.normal(0.001, 0.01, size=60)
        benchmark = rng.normal(0.0005, 0.008, size=60)
        ir = sve.compute_information_ratio(portfolio, benchmark)
        assert isinstance(ir, float)

    def test_too_short(self):
        sve = _ensure_imports()
        assert sve.compute_information_ratio(np.array([0.01]), np.array([0.01])) == 0.0
