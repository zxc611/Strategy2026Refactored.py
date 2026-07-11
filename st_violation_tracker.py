# MODULE_ID: M2-606
"""Tests for evaluation.violation_tracker module."""
import pytest
from unittest.mock import patch, MagicMock
from ali2026v3_trading.evaluation.violation_tracker import StrategyViolationTracker


class TestStrategyViolationTracker:
    def test_track_no_violation(self):
        vt = StrategyViolationTracker()
        result = vt.track("s1", verdict="PASS", blockers=[])
        assert result == []

    def test_track_fail_verdict(self):
        vt = StrategyViolationTracker()
        result = vt.track("s1", verdict="FAIL", blockers=[])
        assert len(result) == 1
        assert "s1" in result[0]
        assert "FAIL" in result[0]

    def test_track_with_blockers(self):
        vt = StrategyViolationTracker()
        result = vt.track("s1", verdict="PASS", blockers=["b1", "b2"])
        assert len(result) == 3
        assert any("blocker=b1" in r for r in result)
        assert any("blocker=b2" in r for r in result)

    def test_track_fail_and_blockers(self):
        vt = StrategyViolationTracker()
        result = vt.track("s1", verdict="REJECT", blockers=["b1"])
        assert len(result) == 2

    def test_getattr_delegation(self):
        vt = StrategyViolationTracker()
        with patch("ali2026v3_trading.evaluation.violation_tracker._get_base_class") as mock_get:
            mock_cls = MagicMock()
            mock_instance = MagicMock()
            mock_instance.some_method.return_value = 42
            mock_cls.return_value = mock_instance
            mock_get.return_value = mock_cls
            assert vt.some_method() == 42
