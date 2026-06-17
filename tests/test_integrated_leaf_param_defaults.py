# MODULE_ID: M2-361
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestLeafParamDefaults:
    def test_import(self):
        from ali2026v3_trading.param_pool import _leaf_param_defaults
        assert _leaf_param_defaults is not None

    def test_pullback_defaults(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PULLBACK_DEFAULTS
        assert isinstance(PULLBACK_DEFAULTS, dict)
        assert len(PULLBACK_DEFAULTS) > 0

    def test_pullback_grid(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PULLBACK_GRID
        assert isinstance(PULLBACK_GRID, dict)
        assert len(PULLBACK_GRID) > 0

    def test_risk_dimension_defaults(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import RISK_DIMENSION_DEFAULTS
        assert isinstance(RISK_DIMENSION_DEFAULTS, dict)

    def test_risk_dimension_grid(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import RISK_DIMENSION_GRID
        assert isinstance(RISK_DIMENSION_GRID, dict)

    def test_param_defaults(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PARAM_DEFAULTS
        assert isinstance(PARAM_DEFAULTS, dict)
        assert len(PARAM_DEFAULTS) > 0

    def test_param_grid_round1(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PARAM_GRID_ROUND1
        assert isinstance(PARAM_GRID_ROUND1, dict)

    def test_param_grid_round2(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PARAM_GRID_ROUND2
        assert isinstance(PARAM_GRID_ROUND2, dict)

    def test_round1_top_k(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import ROUND1_TOP_K
        assert isinstance(ROUND1_TOP_K, int)
        assert ROUND1_TOP_K > 0

    def test_param_grid_alias(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import PARAM_GRID
        assert PARAM_GRID is not None

    def test_default_objective(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import DEFAULT_OBJECTIVE
        assert isinstance(DEFAULT_OBJECTIVE, str)

    def test_objective_functions(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import OBJECTIVE_FUNCTIONS
        assert isinstance(OBJECTIVE_FUNCTIONS, dict)

    def test_hft_time_params(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import HFT_TIME_PARAMS
        assert isinstance(HFT_TIME_PARAMS, dict)

    def test_hft_time_param_grid(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import HFT_TIME_PARAM_GRID
        assert isinstance(HFT_TIME_PARAM_GRID, dict)

    def test_shadow_param_map(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import SHADOW_PARAM_MAP
        assert isinstance(SHADOW_PARAM_MAP, dict)

    def test_strategy_shadow_defaults(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import STRATEGY_SHADOW_DEFAULTS
        assert isinstance(STRATEGY_SHADOW_DEFAULTS, dict)

    def test_cr_param_grid(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import CR_PARAM_GRID
        assert isinstance(CR_PARAM_GRID, dict)

    def test_option_take_profit_grid(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import OPTION_TAKE_PROFIT_GRID
        assert isinstance(OPTION_TAKE_PROFIT_GRID, list)

    def test_option_stop_loss_grid(self):
        from ali2026v3_trading.param_pool._leaf_param_defaults import OPTION_STOP_LOSS_GRID
        assert isinstance(OPTION_STOP_LOSS_GRID, list)