# MODULE_ID: M2-300
"""Tests for evaluation.activity_weighted_scorer module."""
import pytest
from evaluation.activity_weighted_scorer import ActivityWeightedScorer


class TestActivityWeightedScorer:
    def test_init_defaults(self):
        scorer = ActivityWeightedScorer()
        assert scorer._recency_weight == 0.7
        assert scorer._volume_weight == 0.3

    def test_init_custom(self):
        scorer = ActivityWeightedScorer(recency_weight=0.5, volume_weight=0.5)
        assert scorer._recency_weight == 0.5
        assert scorer._volume_weight == 0.5

    def test_calculate_basic(self):
        scorer = ActivityWeightedScorer()
        score = scorer.calculate("s1", overall=0.8, dimensions={"activity_count": 50, "recent_active_days": 15})
        assert isinstance(score, float)
        assert score > 0

    def test_calculate_no_dimensions(self):
        scorer = ActivityWeightedScorer()
        score = scorer.calculate("s1", overall=0.8, dimensions=None)
        assert isinstance(score, float)

    def test_calculate_high_activity(self):
        scorer = ActivityWeightedScorer()
        score = scorer.calculate("s1", overall=1.0, dimensions={"activity_count": 200, "recent_active_days": 30})
        assert score >= 1.0 * (0.7 * 1.0 + 0.3 * 1.0)

    def test_calculate_zero_recent_days(self):
        scorer = ActivityWeightedScorer()
        score = scorer.calculate("s1", overall=0.5, dimensions={"activity_count": 1, "recent_active_days": 0})
        recency_factor = 0.0
        volume_factor = min(1.0, 1 / max(100, 1))
        expected = 0.5 * (0.7 * recency_factor + 0.3 * volume_factor)
        assert score == pytest.approx(expected, rel=1e-6)
