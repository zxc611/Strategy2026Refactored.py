# MODULE_ID: M2-400
"""strategy_judgment/_judgment_services.py 覆盖率测试"""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.strategy_judgment._judgment_services import (
    VerdictService, ScoringHelper, run_deep_validations,
)


class TestVerdictService:
    def test_init(self):
        svc = VerdictService()
        assert svc is not None

    def test_determine_verdict(self):
        from unittest.mock import MagicMock
        svc = VerdictService()
        report = MagicMock()
        report.extreme_point_count = 100
        result = svc.determine_verdict(overall=0.75, dimensions=[], report=report)
        assert result is not None

    def test_determine_verdict_low(self):
        from unittest.mock import MagicMock
        svc = VerdictService()
        report = MagicMock()
        report.extreme_point_count = 100
        result = svc.determine_verdict(overall=0.2, dimensions=[], report=report)
        assert result is not None

    def test_generate_recommendations(self):
        svc = VerdictService()
        result = svc.generate_recommendations(verdict='conditional_pass', dimensions={}, blockers=[], conditions=[])
        assert result is not None


class TestScoringHelper:
    def test_chicory_eviction_score(self):
        helper = ScoringHelper()
        score = helper.chicory_eviction_score(strategy_score=0.8, age_days=30, violation_count=0)
        assert isinstance(score, float)

    def test_activity_weighted_score(self):
        helper = ScoringHelper()
        result = helper.activity_weighted_score()
        assert isinstance(result, (float, dict))


class TestRunDeepValidations:
    def test_basic_call(self):
        result = run_deep_validations(
            scoring_coefficients={},
            capital_scale='medium',
            strategy_id='test',
            strategy_type='box',
            symbol='AU2506',
            backtest_period='2025-01',
            diagnosis_report={},
            resonance_accuracy=0.8,
            snapshot_statistics={},
            extreme_survival_result={},
            cross_instrument_results={},
            profitability_metrics={},
            parameter_stability_result={},
            return_source_diversification={},
            drawdown_recovery_result={},
        )
        assert result is not None
