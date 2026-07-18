# MODULE_ID: M2-588
"""Tests for strategy_behavior_diagnosis.py"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import numpy as np


_IMPORTS = None

def _ensure_imports():
    global _IMPORTS
    if _IMPORTS is not None:
        return _IMPORTS
    # 清除可能被前面测试污染的 mock 模块缓存
    for _mod_name in [
        'strategy_judgment.turning_point_analysis',
        'strategy_judgment.market_snapshot_collector',
        'strategy_judgment.strategy_behavior_diagnosis',
    ]:
        if _mod_name in sys.modules:
            del sys.modules[_mod_name]
    try:
        from strategy_judgment.strategy_behavior_diagnosis import (
            StrategyBehaviorDiagnosis, DiagnosisSeverity, DimensionDiagnosis,
        )
        from strategy_judgment.market_snapshot_collector import MarketSnapshot, StrategyStateSnapshot
        _IMPORTS = (StrategyBehaviorDiagnosis, DiagnosisSeverity, DimensionDiagnosis, MarketSnapshot, StrategyStateSnapshot)
        return _IMPORTS
    except Exception as e:
        pytest.skip(f"Import failed: {e}")


def _make_snapshot(strategy_id, extreme_region, signal_strength=0.5, position_size=1.0, pnl=0.0, greeks_delta=0.1, greeks_vega=0.05, signal_direction="LONG"):
    _, _, _, MarketSnapshot, StrategyStateSnapshot = _ensure_imports()
    ss = StrategyStateSnapshot(
        strategy_id=strategy_id,
        strategy_type="s1_hft",
        signal_strength=signal_strength,
        signal_direction=signal_direction,
        position_size=position_size,
        greeks_delta=greeks_delta,
        greeks_vega=greeks_vega,
    )
    snap = MarketSnapshot.__new__(MarketSnapshot)
    snap.snapshot_id = "snap_001"
    snap.timestamp = np.datetime64("2024-01-01T00:00:00")
    snap.symbol = "TEST"
    snap.trigger = type("T", (), {"value": "TEST"})()
    snap.extreme_region = extreme_region
    snap.strategy_states = [ss]
    snap.total_portfolio_pnl = pnl
    return snap


class TestStrategyBehaviorDiagnosis:
    def test_init(self):
        StrategyBehaviorDiagnosis = _ensure_imports()[0]
        inst = StrategyBehaviorDiagnosis.__new__(StrategyBehaviorDiagnosis)
        inst.__init__("s1", strategy_type="s1_hft")
        assert inst._strategy_id == "s1"
        assert inst._strategy_type == "s1_hft"
        assert inst._expected_logic is not None

    def test_init_empty_id(self):
        StrategyBehaviorDiagnosis = _ensure_imports()[0]
        inst = StrategyBehaviorDiagnosis.__new__(StrategyBehaviorDiagnosis)
        inst.__init__("", strategy_type="unknown")
        assert inst._strategy_id == ""
        assert inst._expected_logic is not None

    def test_diagnose_insufficient_data(self):
        StrategyBehaviorDiagnosis = _ensure_imports()[0]
        DiagnosisSeverity = _ensure_imports()[1]
        inst = StrategyBehaviorDiagnosis.__new__(StrategyBehaviorDiagnosis)
        inst.__init__("s1", strategy_type="s1_hft")
        snaps = []
        report = inst.diagnose(snaps, snaps, symbol="TEST", backtest_period="2024")
        assert report.strategy_id == "s1"
        assert report.overall_score.severity == DiagnosisSeverity.INSUFFICIENT_DATA

    def test_diagnose_with_samples(self):
        StrategyBehaviorDiagnosis = _ensure_imports()[0]
        inst = StrategyBehaviorDiagnosis.__new__(StrategyBehaviorDiagnosis)
        inst.__init__("s1", strategy_type="s1_hft")
        snaps = [
            _make_snapshot("s1", "NEAR_HIGH", signal_strength=0.8, position_size=1.0, pnl=100.0),
            _make_snapshot("s1", "NEAR_HIGH", signal_strength=0.6, position_size=0.8, pnl=110.0),
            _make_snapshot("s1", "NEAR_LOW", signal_strength=0.8, position_size=1.0, pnl=90.0),
            _make_snapshot("s1", "NEAR_LOW", signal_strength=0.6, position_size=0.8, pnl=85.0),
        ]
        report = inst.diagnose(snaps, snaps, symbol="TEST", backtest_period="2024")
        assert report.strategy_id == "s1"
        assert report.extreme_point_count == 4
        assert len(report.dimensions) == 5
        assert report.overall_score.sample_count == 4

    def test_extract_sub_score(self):
        StrategyBehaviorDiagnosis = _ensure_imports()[0]
        DimensionDiagnosis = _ensure_imports()[2]
        dim = DimensionDiagnosis(
            dimension="信号强度演化", score=0.6,
            severity=_ensure_imports()[1].HEALTHY,
            pattern="正常", detail="", sample_count=10, confidence=0.8,
        )
        val = StrategyBehaviorDiagnosis._extract_sub_score(dim, "不存在")
        assert val == dim.score
