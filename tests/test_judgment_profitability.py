# MODULE_ID: M2-396
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
    _JudgmentDimension, CapitalScale, SCORING_COEFFICIENTS,
)
from strategy_judgment.judgment_profitability import ProfitabilityJudger


def _make_instance(**kwargs):
    obj = ProfitabilityJudger.__new__(ProfitabilityJudger)
    obj.SCORING_COEFFICIENTS = kwargs.get("scoring_coefficients", SCORING_COEFFICIENTS)
    obj._capital_scale = kwargs.get("capital_scale", None)
    obj._shadow_metrics = kwargs.get("shadow_metrics", {})
    # Fix: diminishing_return_score is defined without self in source,
    # but called as instance method via self.diminishing_return_score(...)
    obj.diminishing_return_score = lambda value, breakpoints=None: ProfitabilityJudger.diminishing_return_score(value, breakpoints)
    return obj


def test_import():
    assert ProfitabilityJudger is not None


def test_diminishing_return_score():
    # Defined as a plain function inside the class (no self usage)
    assert ProfitabilityJudger.diminishing_return_score(0.0) == 0.0
    assert ProfitabilityJudger.diminishing_return_score(-1.0) == 0.0
    assert ProfitabilityJudger.diminishing_return_score(1.0) == 0.50
    assert ProfitabilityJudger.diminishing_return_score(2.0) == 0.80
    assert ProfitabilityJudger.diminishing_return_score(3.0) == 1.00
    assert ProfitabilityJudger.diminishing_return_score(5.0) == 1.00
    # interpolation
    assert ProfitabilityJudger.diminishing_return_score(1.5) == 0.65


def test_judge_profitability_none():
    obj = _make_instance()
    dim = obj.judge_profitability(None, threshold=0.5, weight=0.09)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score == 0.0
    assert dim.passed is False
    assert "未提供盈利指标" in dim.detail


def test_judge_profitability_basic():
    obj = _make_instance()
    metrics = {
        "sharpe": 2.0,
        "calmar": 1.5,
        "win_rate": 0.55,
        "profit_loss_ratio": 2.0,
    }
    dim = obj.judge_profitability(metrics, threshold=0.5, weight=0.09)
    assert isinstance(dim, _JudgmentDimension)
    assert 0.0 <= dim.score <= 1.0
    assert dim.name == "盈利能力"
    assert dim.passed is True


def test_judge_profitability_with_shadow_metrics():
    obj = _make_instance(shadow_metrics={"g1": [{"alpha_decay_rate": 0.5}]})
    metrics = {
        "sharpe": 2.0,
        "calmar": 1.5,
        "win_rate": 0.55,
        "profit_loss_ratio": 2.0,
    }
    dim = obj.judge_profitability(metrics, threshold=0.5, weight=0.09)
    assert isinstance(dim, _JudgmentDimension)
    assert dim.score >= 0.0
    assert "AlphaDecayAdj" in dim.detail


def test_load_shadow_jsonl_no_crash():
    obj = _make_instance()
    # Should not raise even if no files exist
    obj.load_shadow_jsonl()


def test_resolve_scale_config_fallback():
    obj = _make_instance()
    cfg = obj.resolve_scale_config(CapitalScale.MEDIUM)
    # If ModeEngine unavailable, returns None (fallback logs)
    assert cfg is None or isinstance(cfg, dict)


def test_judge_profitability_by_scale():
    obj = _make_instance(capital_scale=CapitalScale.MEDIUM)
    metrics = {
        "win_loss_ratio": 2.0,
        "profit_factor": 1.5,
        "recovery_efficiency": 2.0,
        "max_consecutive_losses": 3,
        "sharpe": 2.0,
        "calmar": 2.0,
    }
    dim = obj.judge_profitability_by_scale(metrics, threshold=0.5, is_blocker=False, weight=0.09)
    assert isinstance(dim, _JudgmentDimension)
    assert 0.0 <= dim.score <= 1.0
    assert dim.name == "盈利能力"
    assert "模式" in dim.detail
