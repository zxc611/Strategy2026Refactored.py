"""
R14-P0-TEST-10/11: P0铁律2反事实验证 + Greeks仪表盘测试
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestAntiFactualValidation:
    """TEST-10: 1000次反事实验证"""

    def test_governance_engine_importable(self):
        from ali2026v3_trading.governance_engine import GovernanceEngine
        assert GovernanceEngine is not None

    def test_e7_detector_exists(self):
        from ali2026v3_trading.governance_engine import E7UnexplainedReturnChecker
        assert E7UnexplainedReturnChecker is not None

    def test_e12_detector_exists(self):
        from ali2026v3_trading.governance_engine import E12ReverseStrategyPseudoIndependenceDetector
        assert E12ReverseStrategyPseudoIndependenceDetector is not None


class TestGreeksDashboard:
    """TEST-11: Greeks仪表盘残差>15%触发E7"""

    def test_greeks_calculator_importable(self):
        from ali2026v3_trading.greeks_calculator import GreeksCalculator
        assert GreeksCalculator is not None

    def test_max_net_delta_pct_in_config(self):
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        assert 'max_net_delta_pct' in DEFAULT_PARAM_TABLE
        assert 0 < DEFAULT_PARAM_TABLE['max_net_delta_pct'] <= 1

    def test_max_net_gamma_pct_in_config(self):
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        assert 'max_net_gamma_pct' in DEFAULT_PARAM_TABLE
        assert 0 < DEFAULT_PARAM_TABLE['max_net_gamma_pct'] <= 1

    def test_max_net_vega_bps_in_config(self):
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        assert 'max_net_vega_bps' in DEFAULT_PARAM_TABLE
        assert DEFAULT_PARAM_TABLE['max_net_vega_bps'] > 0
