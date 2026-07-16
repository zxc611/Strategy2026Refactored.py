# MODULE_ID: M2-393
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
from strategy_judgment.judgment_calibration import CalibrationJudger


def _make_instance():
    obj = CalibrationJudger.__new__(CalibrationJudger)
    obj.SCORING_COEFFICIENTS = SCORING_COEFFICIENTS
    return obj


def test_import():
    assert CalibrationJudger is not None


def test_judge_prediction_calibration_none():
    obj = _make_instance()
    dim = obj.judge_prediction_calibration(None, threshold=0.50, weight=0.05)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert "无共振预测数据" in dim.detail


def test_judge_prediction_calibration_with_accuracy():
    obj = _make_instance()
    accuracy = {"hit_rate": 0.7, "direction_accuracy": 0.6}
    dim = obj.judge_prediction_calibration(accuracy, threshold=0.50, weight=0.05)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score > 0.0
    assert dim.passed is True
    assert "命中率" in dim.detail


def test_judge_parameter_stability_none():
    obj = _make_instance()
    dim = obj.judge_parameter_stability(None, threshold=0.50, weight=0.05)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert "未执行参数稳定性检验" in dim.detail


def test_judge_parameter_stability_with_result():
    obj = _make_instance()
    result = {
        "plateau_score": 0.8,
        "return_skewness": 0.2,
        "return_kurtosis_raw": 3.5,
        "max_param_sensitivity": 0.3,
    }
    dim = obj.judge_parameter_stability(result, threshold=0.50, weight=0.05)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score > 0.0
    assert "高原度" in dim.detail


def test_judge_return_source_diversification_none():
    obj = _make_instance()
    dim = obj.judge_return_source_diversification(None, threshold=0.50, weight=0.06)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert "未提供收益来源分解数据" in dim.detail


def test_judge_return_source_diversification_with_result():
    obj = _make_instance()
    result = {
        "signal_source_weights": {"trend": 0.5, "mean_reversion": 0.3, "breakout": 0.2},
        "market_state_weights": {"trending": 0.6, "ranging": 0.4},
        "time_concentration": 0.3,
    }
    dim = obj.judge_return_source_diversification(result, threshold=0.50, weight=0.06)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score > 0.0
    assert "信号源分散" in dim.detail


def test_judge_cross_strategy_correlation_none():
    obj = _make_instance()
    dim = obj.judge_cross_strategy_correlation(None, threshold=0.70, weight=0.07)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert "未提供策略间相关性数据" in dim.detail


def test_judge_cross_strategy_correlation_with_result():
    obj = _make_instance()
    result = {
        "pairwise_correlations": {"s1_s2": 0.2, "s1_s3": 0.3},
        "crisis_correlations": {"s1_s2": 0.4},
        "max_normal_corr": 0.3,
        "max_crisis_corr": 0.4,
    }
    dim = obj.judge_cross_strategy_correlation(result, threshold=0.70, weight=0.07)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score > 0.0
    assert "最大相关" in dim.detail


def test_judge_cross_strategy_correlation_empty_pairwise():
    obj = _make_instance()
    result = {
        "pairwise_correlations": {},
        "crisis_correlations": {},
        "max_normal_corr": 0.0,
        "max_crisis_corr": 0.0,
    }
    dim = obj.judge_cross_strategy_correlation(result, threshold=0.70, weight=0.07)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert "无策略间相关性数据" in dim.detail
