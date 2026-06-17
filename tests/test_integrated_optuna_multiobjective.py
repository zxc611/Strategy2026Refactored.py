# MODULE_ID: M2-367
import sys
import os
import pytest
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestLatinHypercubeSample:
    def test_shape(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import latin_hypercube_sample
        result = latin_hypercube_sample(n_dims=3, n_samples=5)
        assert result.shape == (5, 3)

    def test_values_in_unit_range(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import latin_hypercube_sample
        result = latin_hypercube_sample(n_dims=3, n_samples=10, seed=42)
        assert np.all(result >= 0.0)
        assert np.all(result <= 1.0)

    def test_reproducible_with_seed(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import latin_hypercube_sample
        r1 = latin_hypercube_sample(n_dims=3, n_samples=5, seed=42)
        r2 = latin_hypercube_sample(n_dims=3, n_samples=5, seed=42)
        np.testing.assert_array_equal(r1, r2)


class TestLhsToParamGrid:
    def test_returns_list(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import lhs_to_param_grid
        lhs = np.random.rand(3, 2)
        param_ranges = {'p1': (0.0, 1.0), 'p2': (10.0, 100.0)}
        result = lhs_to_param_grid(lhs, param_ranges)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_with_param_ranges(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import lhs_to_param_grid
        lhs = np.random.rand(3, 2)
        param_ranges = {'p1': (0.0, 1.0), 'p2': (10.0, 100.0)}
        result = lhs_to_param_grid(lhs, param_ranges)
        assert isinstance(result, list)
        assert len(result) == 3


class TestSigmoidScore:
    def test_returns_float(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import sigmoid_score
        result = sigmoid_score(test_sharpe=0.5, profit_loss_ratio=1.5, test_max_drawdown=0.1, win_rate=0.6)
        assert isinstance(result, float)

    def test_in_range(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import sigmoid_score
        for sharpe in [-1.0, 0.0, 0.5, 1.0, 2.0]:
            result = sigmoid_score(test_sharpe=sharpe, profit_loss_ratio=1.0, test_max_drawdown=0.1, win_rate=0.5)
            assert 0.0 <= result <= 1.0


class TestEnrichBacktestResult:
    def test_callable(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import enrich_backtest_result
        assert callable(enrich_backtest_result)


class TestGenerateExpandedSearchSpaces:
    def test_callable(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import generate_expanded_search_spaces
        assert callable(generate_expanded_search_spaces)

    def test_returns_dict(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import generate_expanded_search_spaces
        result = generate_expanded_search_spaces()
        assert isinstance(result, dict)