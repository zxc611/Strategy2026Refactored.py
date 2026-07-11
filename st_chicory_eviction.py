# MODULE_ID: M2-311
"""Tests for evaluation.chicory_eviction module."""
import os
import pytest
from ali2026v3_trading.evaluation.chicory_eviction import (
    EvictionCode,
    EVICTION_REVIVAL_POLICY,
    COOLING_PERIOD_DAYS,
    EvictionDecision,
    ChicoryEvictionPolicy,
)


class TestEvictionCode:
    def test_enum_values(self):
        assert EvictionCode.CHICORY_LOW_SCORE.value == "chicory_low_score"
        assert EvictionCode.CHICORY_AGE_DECAY.value == "chicory_age_decay"


class TestRevivalPolicy:
    def test_policy_structure(self):
        assert EvictionCode.CHICORY_LOW_SCORE in EVICTION_REVIVAL_POLICY
        assert EVICTION_REVIVAL_POLICY[EvictionCode.CHICORY_LOW_SCORE]["revivable"] is False
        assert EVICTION_REVIVAL_POLICY[EvictionCode.CHICORY_AGE_DECAY]["revivable"] is True


class TestEvictionDecision:
    def test_defaults(self):
        d = EvictionDecision()
        assert d.should_evict is False
        assert d.cooling_period_days == COOLING_PERIOD_DAYS


class TestChicoryEvictionPolicy:
    def test_init(self):
        policy = ChicoryEvictionPolicy(eviction_threshold=0.25)
        assert policy._eviction_threshold == 0.25

    def test_eviction_score_high_base(self):
        policy = ChicoryEvictionPolicy()
        score = policy.eviction_score(base_score=1.0, age_days=0, violation_count=0)
        assert score > 0.8

    def test_eviction_score_low_base_old_age(self):
        policy = ChicoryEvictionPolicy()
        score = policy.eviction_score(base_score=0.1, age_days=365, violation_count=0)
        assert score < 0.5

    def test_eviction_score_with_violations(self):
        policy = ChicoryEvictionPolicy()
        score0 = policy.eviction_score(base_score=0.5, age_days=30, violation_count=0)
        score5 = policy.eviction_score(base_score=0.5, age_days=30, violation_count=5)
        assert score5 < score0

    def test_eviction_score_clipped(self):
        policy = ChicoryEvictionPolicy()
        score = policy.eviction_score(base_score=10.0, age_days=0, violation_count=0)
        assert 0.0 <= score <= 1.0

    def test_determine_code_violation(self):
        policy = ChicoryEvictionPolicy()
        code = policy._determine_eviction_code(0.1, 0.1, 10, 3)
        assert code == EvictionCode.CHICORY_VIOLATION

    def test_determine_code_low_score(self):
        policy = ChicoryEvictionPolicy()
        code = policy._determine_eviction_code(0.1, 0.1, 10, 0)
        assert code == EvictionCode.CHICORY_LOW_SCORE

    def test_determine_code_age_decay(self):
        policy = ChicoryEvictionPolicy()
        code = policy._determine_eviction_code(0.1, 0.4, 100, 0)
        assert code == EvictionCode.CHICORY_AGE_DECAY

    def test_determine_code_composite(self):
        policy = ChicoryEvictionPolicy()
        code = policy._determine_eviction_code(0.5, 0.5, 10, 0)
        assert code == EvictionCode.CHICORY_COMPOSITE

    def test_evaluate_should_evict(self):
        policy = ChicoryEvictionPolicy()
        result = policy.evaluate("s1", overall_score=0.01, dimensions={"base_score": 0.01, "age_days": 365, "violation_count": 5})
        assert result["should_evict"] is True
        assert result["eviction_score"] < policy._eviction_threshold
        assert "revivable" in result

    def test_evaluate_should_not_evict(self):
        policy = ChicoryEvictionPolicy()
        result = policy.evaluate("s1", overall_score=0.9, dimensions={"base_score": 0.9, "age_days": 10, "violation_count": 0})
        assert result["should_evict"] is False
        assert result["cooling_period_days"] == 0

    def test_audit_log(self, tmp_path):
        policy = ChicoryEvictionPolicy()
        log_path = tmp_path / "audit.jsonl"
        policy.set_audit_log_path(str(log_path))
        result = policy.evaluate("s1", overall_score=0.01, dimensions={"base_score": 0.01, "age_days": 365, "violation_count": 5})
        assert result["should_evict"] is True
        assert log_path.exists()
        content = log_path.read_text(encoding='utf-8')
        assert "s1" in content

    def test_audit_log_no_path(self):
        policy = ChicoryEvictionPolicy()
        result = policy.evaluate("s1", overall_score=0.01, dimensions={"base_score": 0.01, "age_days": 365, "violation_count": 5})
        assert result["should_evict"] is True
