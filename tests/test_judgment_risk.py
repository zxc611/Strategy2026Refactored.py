# MODULE_ID: M2-397
"""Tests for judgment_risk.py"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


def _ensure_imports():
    try:
        from ali2026v3_trading.strategy_judgment.judgment_risk import RiskJudger
        from ali2026v3_trading.strategy_judgment.judgment_types import (
            _JudgmentDimension, DIM_RISK_BUDGET_COMPLIANCE, DIM_EXTREME_SURVIVAL, DIM_DISPLAY_NAMES, SCORING_COEFFICIENTS,
        )
        return RiskJudger, _JudgmentDimension, DIM_RISK_BUDGET_COMPLIANCE, DIM_EXTREME_SURVIVAL, DIM_DISPLAY_NAMES, SCORING_COEFFICIENTS
    except Exception as e:
        pytest.skip(f"Import failed: {e}")


class FakeReport:
    def __init__(self):
        self.extreme_point_count = 50
        self.overall_score = type("OS", (), {"confidence": 0.85})()
        self.dimensions = []


class FakeDim:
    def __init__(self, dimension, score):
        self.dimension = dimension
        self.score = score


class TestRiskJudger:
    def test_init_default(self):
        cls = _ensure_imports()[0]
        inst = cls.__new__(cls)
        inst.__init__()
        assert inst.SCORING_COEFFICIENTS is not None
        assert inst._min_samples == 30

    def test_judge_statistical_significance_none(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_statistical_significance(None, threshold=0.70, weight=0.06)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == 0.0
        assert result.passed is False

    def test_judge_statistical_significance_pass(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        report = FakeReport()
        result = inst.judge_statistical_significance(report, threshold=0.70, weight=0.06)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == pytest.approx(0.85, abs=1e-6)
        assert result.passed is True

    def test_judge_risk_budget_compliance_none(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_risk_budget_compliance(None, threshold=0.70, weight=0.11)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == 0.0
        assert result.is_blocker is True

    def test_judge_risk_budget_compliance_with_dims(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        report = FakeReport()
        report.dimensions = [
            FakeDim("仓位", 0.8),
            FakeDim("Greeks暴露", 0.6),
        ]
        result = inst.judge_risk_budget_compliance(report, threshold=0.70, weight=0.11)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == pytest.approx(0.7, abs=1e-6)

    def test_judge_extreme_survival_none(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_extreme_survival(None, threshold=0.60, weight=0.08)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == 0.0
        assert result.passed is False
        assert result.is_blocker is True

    def test_judge_extreme_survival_survived(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_extreme_survival(
            {"survived": True, "max_drawdown_pct": 5.0, "recovery_hours": 2.0},
            threshold=0.60, weight=0.08,
        )
        assert isinstance(result, _JudgmentDimension)
        assert result.passed is True
        assert result.is_blocker is True

    def test_judge_drawdown_recovery_none(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_drawdown_recovery(None, threshold=0.50, weight=0.05)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == 0.0
        assert result.passed is False

    def test_judge_drawdown_recovery_with_data(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_drawdown_recovery(
            {"max_recovery_hours": 12.0, "mean_recovery_hours": 6.0, "recovery_count": 5, "no_recovery_count": 1, "max_drawdown_pct": 10.0},
            threshold=0.50, weight=0.05,
        )
        assert isinstance(result, _JudgmentDimension)
        assert 0.0 <= result.score <= 1.0

    def test_judge_realtime_risk_score_none(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_realtime_risk_score(None, threshold=0.50, weight=0.10)
        assert isinstance(result, _JudgmentDimension)
        assert result.score == 0.0
        assert result.passed is False

    def test_judge_realtime_risk_score_composite(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_realtime_risk_score(
            {"composite_score": 0.85},
            threshold=0.50, weight=0.10,
        )
        assert isinstance(result, _JudgmentDimension)
        assert result.score == pytest.approx(0.85, abs=1e-6)
        assert result.passed is True

    def test_judge_realtime_risk_score_from_dims(self):
        cls, _JudgmentDimension = _ensure_imports()[:2]
        inst = cls.__new__(cls)
        inst.__init__()
        result = inst.judge_realtime_risk_score(
            {"d1_state_strength": 0.8, "d2_order_flow": 0.8, "d3_life_expectancy": 0.8,
             "d4_cycle_resonance": 0.8, "d5_phase_quality": 0.8, "d6_greeks_usage": 0.8,
             "d7_consecutive_loss": 1.0, "d8_asymmetric_drawdown": 0.8, "d9_tri_validation": 0.8,
             "d10_alpha_decay": 0.8, "d11_cross_correlation": 0.8},
            threshold=0.50, weight=0.10,
        )
        assert isinstance(result, _JudgmentDimension)
        assert 0.0 <= result.score <= 1.0
