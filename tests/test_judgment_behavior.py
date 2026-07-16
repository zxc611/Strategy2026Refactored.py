# MODULE_ID: M2-392
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from unittest.mock import MagicMock

# Mock modules that may cause circular import issues before importing target modules
_mock_tp = MagicMock()
_mock_tp.ResonanceTurningPointMarker = MagicMock
sys.modules['strategy_judgment.turning_point_analysis'] = _mock_tp

_mock_msc = MagicMock()
_mock_msc.MarketSnapshotCollector = MagicMock
_mock_msc.MarketSnapshot = MagicMock
_mock_msc.SnapshotTrigger = MagicMock
sys.modules['strategy_judgment.market_snapshot_collector'] = _mock_msc

import infra._helpers as _logging_utils
_logging_utils.get_logger = MagicMock(return_value=MagicMock())

from strategy_judgment.judgment_types import (
    _JudgmentDimension, SCORING_COEFFICIENTS,
)
from strategy_judgment.strategy_behavior_diagnosis import DiagnosisSeverity
from strategy_judgment.judgment_behavior import BehaviorJudger


def _make_instance():
    obj = BehaviorJudger.__new__(BehaviorJudger)
    obj.SCORING_COEFFICIENTS = SCORING_COEFFICIENTS
    obj._min_instruments = 3
    return obj


def test_import():
    assert BehaviorJudger is not None


def test_judge_behavior_consistency_none():
    obj = _make_instance()
    dim = obj.judge_behavior_consistency(None, threshold=0.7, weight=0.18)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert dim.is_blocker is True
    assert "无诊断报告" in dim.detail


def test_judge_behavior_consistency_with_report():
    obj = _make_instance()
    report = MagicMock()
    bcs = MagicMock()
    bcs.overall_score = 0.85
    bcs.signal_decay_consistency = 0.8
    bcs.position_smoothness = 0.75
    bcs.greeks_rationality = 0.7
    bcs.pnl_path_quality = 0.9
    report.overall_score = bcs
    dim = obj.judge_behavior_consistency(report, threshold=0.7, weight=0.18)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.85
    assert dim.passed is True
    assert dim.weight == 0.18
    assert dim.is_blocker is True


def test_judge_process_explainability_none():
    obj = _make_instance()
    dim = obj.judge_process_explainability(None, None, threshold=0.65, weight=0.06)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert dim.is_blocker is False


def test_judge_process_explainability_with_coverage():
    obj = _make_instance()
    coverage = {
        "total_decisions": 100,
        "explained_decisions": 80,
        "avg_chain_length": 3.0,
        "unique_rules_triggered": 12,
    }
    dim = obj.judge_process_explainability(None, coverage, threshold=0.65, weight=0.06)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score > 0.0
    assert dim.passed is True


def test_judge_process_explainability_zero_decisions():
    obj = _make_instance()
    coverage = {"total_decisions": 0}
    dim = obj.judge_process_explainability(None, coverage, threshold=0.65, weight=0.06)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False


def test_judge_process_explainability_with_report():
    obj = _make_instance()
    report = MagicMock()
    mock_dim = MagicMock()
    mock_dim.severity = DiagnosisSeverity.HEALTHY
    report.dimensions = [mock_dim, mock_dim, mock_dim]
    dim = obj.judge_process_explainability(report, None, threshold=0.65, weight=0.06)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 1.0
    assert dim.passed is True


def test_judge_cross_instrument_consistency_empty():
    obj = _make_instance()
    dim = obj.judge_cross_instrument_consistency({}, threshold=0.60, weight=0.04)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.passed is False
    assert dim.score == 0.0


def test_judge_cross_instrument_consistency_with_results():
    obj = _make_instance()

    def _make_report(overall):
        r = MagicMock()
        bcs = MagicMock()
        bcs.overall_score = overall
        bcs.confidence = 0.5
        r.overall_score = bcs
        trend_dim = MagicMock()
        trend_dim.name = "趋势捕捉能力"
        trend_dim.score = 0.5
        r.dimensions = [trend_dim]
        return r

    results = {
        "IF_1": _make_report(0.85),
        "IF_2": _make_report(0.75),
        "IF_3": _make_report(0.65),
        "IF_4": _make_report(0.55),
    }
    dim = obj.judge_cross_instrument_consistency(results, threshold=0.60, weight=0.04)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score >= 0.0
    assert dim.weight == 0.04
    assert "跨" in dim.detail
