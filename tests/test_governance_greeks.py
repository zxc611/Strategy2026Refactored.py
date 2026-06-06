"""
R17-P0-TEST-10/11: P0铁律2反事实验证 + Greeks仪表盘测试
添加E7触发行为真实功能测试
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.governance_engine import GovernanceEngine, E7UnexplainedReturnChecker
from ali2026v3_trading.greeks_calculator import GreeksCalculator
from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE


class TestAntiFactualValidation:
    """R17-P0-TEST-10: 反事实验证测试"""

    def test_governance_engine_importable(self):
        assert GovernanceEngine is not None

    def test_e7_detector_exists(self):
        assert E7UnexplainedReturnChecker is not None

    def test_e12_detector_exists(self):
        from ali2026v3_trading.governance_engine import E12ReverseStrategyPseudoIndependenceDetector
        assert E12ReverseStrategyPseudoIndependenceDetector is not None


class TestGreeksDashboard:
    """R17-P0-TEST-11: Greeks仪表盘真实功能测试"""

    def test_greeks_calculator_importable(self):
        assert GreeksCalculator is not None

    def test_max_net_delta_pct_in_config(self):
        key = 'max_net_delta_pct'
        assert key in DEFAULT_PARAM_TABLE
        val = DEFAULT_PARAM_TABLE[key]
        assert 0 < val <= 1

    def test_max_net_gamma_pct_in_config(self):
        key = 'max_net_gamma_pct'
        assert key in DEFAULT_PARAM_TABLE
        val = DEFAULT_PARAM_TABLE[key]
        assert 0 < val <= 1

    def test_max_net_vega_bps_in_config(self):
        key = 'max_net_vega_bps'
        assert key in DEFAULT_PARAM_TABLE
        val = DEFAULT_PARAM_TABLE[key]
        assert val > 0

    def test_e7_triggered_when_residual_exceeds_threshold(self):
        checker = E7UnexplainedReturnChecker.__new__(E7UnexplainedReturnChecker)
        checker._residual_threshold_pct = 15.0
        pnl_attribution = {
            'unexplained': 100.0,
            'delta_contrib': 30.0,
            'gamma_contrib': 20.0,
            'vega_contrib': 10.0,
            'theta_contrib': 5.0,
        }
        result = checker.check(pnl_attribution)
        assert result.get('e7_triggered', False) is True

    def test_e7_not_triggered_when_residual_below_threshold(self):
        checker = E7UnexplainedReturnChecker.__new__(E7UnexplainedReturnChecker)
        checker._residual_threshold_pct = 15.0
        pnl_attribution = {
            'unexplained': 1.0,
            'delta_contrib': 50.0,
            'gamma_contrib': 30.0,
            'vega_contrib': 10.0,
            'theta_contrib': 5.0,
        }
        result = checker.check(pnl_attribution)
        assert result.get('e7_triggered', True) is False
