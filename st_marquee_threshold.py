# MODULE_ID: M2-412
"""Tests for evaluation.marquee_threshold module."""
import pytest
from ali2026v3_trading.evaluation.marquee_threshold import MarqueeThreshold


class TestMarqueeThreshold:
    def test_defaults(self):
        mt = MarqueeThreshold()
        assert mt.phase1_threshold == 0.80
        assert mt.phase2_threshold == 0.70
        assert mt.phase3_threshold == 0.60

    def test_custom_init(self):
        mt = MarqueeThreshold(phase1_threshold=0.9, phase2_threshold=0.8, phase3_threshold=0.7)
        assert mt.phase1_threshold == 0.9

    def test_get_phase_strict(self):
        mt = MarqueeThreshold()
        assert mt.get_phase(30) == "strict"
        assert mt.get_phase(0) == "strict"

    def test_get_phase_moderate(self):
        mt = MarqueeThreshold()
        assert mt.get_phase(60) == "moderate"
        assert mt.get_phase(119) == "moderate"

    def test_get_phase_relaxed(self):
        mt = MarqueeThreshold()
        assert mt.get_phase(120) == "relaxed"
        assert mt.get_phase(500) == "relaxed"

    def test_get_threshold(self):
        mt = MarqueeThreshold()
        assert mt.get_threshold(30) == 0.80
        assert mt.get_threshold(60) == 0.70
        assert mt.get_threshold(120) == 0.60

    def test_is_marquee_true(self):
        mt = MarqueeThreshold()
        assert mt.is_marquee(0.85, 30) is True
        assert mt.is_marquee(0.75, 100) is True
        assert mt.is_marquee(0.65, 200) is True

    def test_is_marquee_false(self):
        mt = MarqueeThreshold()
        assert mt.is_marquee(0.79, 30) is False
        assert mt.is_marquee(0.69, 100) is False
        assert mt.is_marquee(0.59, 200) is False

    def test_check_passed(self):
        mt = MarqueeThreshold()
        result = mt.check("s1", overall=0.85, dimensions={"n_samples": 30, "dim_a": 0.9})
        assert result["passed"] is True
        assert result["threshold"] == 0.80
        assert result["failed_dims"] == []

    def test_check_failed(self):
        mt = MarqueeThreshold()
        result = mt.check("s1", overall=0.75, dimensions={"n_samples": 30, "dim_a": 0.5})
        assert result["passed"] is False
        assert "dim_a" in result["failed_dims"]

    def test_check_no_dimensions(self):
        mt = MarqueeThreshold()
        result = mt.check("s1", overall=0.85, dimensions=None)
        assert result["passed"] is True
        assert result["failed_dims"] == []
