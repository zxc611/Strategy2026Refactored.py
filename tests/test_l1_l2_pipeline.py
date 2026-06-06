import pytest
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.param_pool.l1_quantification.triple_truth_anchor import TripleTruthAnchor, TruthAnchorResult
from ali2026v3_trading.param_pool.l1_quantification.bayesian_shrinkage_life_estimator import (
    BayesianShrinkageLifeEstimator, LifeExpectancy, ShrinkageResult,
)
from ali2026v3_trading.param_pool.l1_quantification.soft_constrained_optimizer import (
    SoftConstrainedOptimizer, OptimizationResult,
)
from ali2026v3_trading.param_pool.l1_quantification.performance_tier_manager import (
    PerformanceTierManager, PerformanceTier, TierConfig,
)
from ali2026v3_trading.param_pool.l1_quantification.duckdb_tick_storage import DuckDBTickStorage
from ali2026v3_trading.param_pool.l1_quantification.external_validation_pipeline import (
    ExternalValidationPipeline, ValidationStatus, QuarterlyReport,
)


class TestTripleTruthAnchor:
    def test_voting_weights_sum_to_one(self):
        anchor = TripleTruthAnchor()
        assert abs(sum(anchor.VOTING_WEIGHTS.values()) - 1.0) < 1e-10

    def test_no_future_leak_default(self):
        anchor = TripleTruthAnchor()
        assert anchor.verify_no_future_leak()

    def test_train_validate_separation(self):
        dates = pd.date_range('2024-01-01', periods=200, freq='D')
        data = pd.DataFrame({
            'close': 4000 + np.cumsum(np.random.randn(200) * 10),
            'hmm_state': np.random.choice(
                ['trend_up', 'trend_down', 'range_bound', 'volatile', 'quiet'],
                200,
            ),
        }, index=dates)
        anchor = TripleTruthAnchor(lookahead_minutes=5)
        result = anchor.train_and_validate(
            data, train_end_date='2024-06-01', state_column='hmm_state',
        )
        assert result.future_leak_risk == 'NONE'
        assert 0.0 <= result.algorithm_accuracy <= 1.0
        assert 0.0 <= result.agreement_rate_algo_expost <= 1.0
        assert result.train_end_date == '2024-06-01'

    def test_generate_no_future_labels(self):
        anchor = TripleTruthAnchor()
        dates = pd.date_range('2024-01-01', periods=50, freq='D')
        data = pd.DataFrame({
            'close': 4000 + np.cumsum(np.random.randn(50) * 5),
            'hmm_state': ['trend_up'] * 50,
        }, index=dates)
        labels = anchor._generate_no_future_labels(data, 'hmm_state')
        assert len(labels) == 50


class TestBayesianShrinkageLifeEstimator:
    def test_four_degradation_levels(self):
        estimator = BayesianShrinkageLifeEstimator()
        assert estimator.MIN_SAMPLES_FULL == 200
        assert estimator.MIN_SAMPLES_SHRINK == 50
        assert estimator.MIN_SAMPLES_MERGE == 10
        assert estimator.MIN_R_SQUARED == 0.7

    def test_level0_full_distribution(self):
        estimator = BayesianShrinkageLifeEstimator()
        life = LifeExpectancy(
            state='trend_up',
            duration={'p25': 5, 'p50': 15, 'p75': 45, 'p99': 180},
            magnitude={'p25': 0.005, 'p50': 0.02, 'p75': 0.05, 'p99': 0.15},
            sample_count=300,
            decay_r_squared=0.85,
        )
        estimator._life_dict['trend_up'] = life
        estimator._compute_global_prior()
        result = estimator._apply_degradation(life)
        assert result.degradation_level == 0
        assert result.shrinkage_factor == 1.0

    def test_level1_bayesian_shrinkage(self):
        estimator = BayesianShrinkageLifeEstimator()
        life = LifeExpectancy(
            state='volatile',
            duration={'p25': 2, 'p50': 8, 'p75': 20, 'p99': 60},
            magnitude={'p25': 0.01, 'p50': 0.04, 'p75': 0.08, 'p99': 0.20},
            sample_count=80,
            decay_r_squared=0.75,
        )
        estimator._life_dict['volatile'] = life
        estimator._compute_global_prior()
        estimator._compute_category_priors()
        result = estimator._apply_degradation(life)
        assert result.degradation_level in (1, 2)

    def test_level3_global_prior(self):
        estimator = BayesianShrinkageLifeEstimator()
        life = LifeExpectancy(
            state='rare_state',
            duration={'p25': 1, 'p50': 2, 'p75': 5, 'p99': 10},
            magnitude={'p25': 0.001, 'p50': 0.005, 'p75': 0.01, 'p99': 0.02},
            sample_count=3,
            decay_r_squared=0.3,
        )
        estimator._life_dict['trend_up'] = LifeExpectancy(
            state='trend_up',
            duration={'p25': 5, 'p50': 15, 'p75': 45, 'p99': 180},
            magnitude={'p25': 0.005, 'p50': 0.02, 'p75': 0.05, 'p99': 0.15},
            sample_count=500, decay_r_squared=0.9,
        )
        estimator._life_dict['rare_state'] = life
        estimator._compute_global_prior()
        result = estimator._apply_degradation(life)
        assert result.degradation_level == 3

    def test_penalty_calibration(self):
        estimator = BayesianShrinkageLifeEstimator()
        estimator._life_dict['trend_up'] = LifeExpectancy(
            state='trend_up',
            duration={'p25': 5, 'p50': 15, 'p75': 45, 'p99': 180},
            magnitude={'p25': 0.005, 'p50': 0.02, 'p75': 0.05, 'p99': 0.15},
            sample_count=500, decay_r_squared=0.9,
        )
        estimator._compute_global_prior()
        result = estimator.calibrate_penalty('trend_up', n_simulations=1000)
        assert 'base_scale' in result
        assert 'coefficients' in result
        assert result['coefficients']['pullback_soft'] < result['coefficients']['take_profit_hard']


class TestSoftConstrainedOptimizer:
    def test_no_penalty_no_constraints(self):
        optimizer = SoftConstrainedOptimizer()
        result = optimizer.optimize(
            objective_fn=lambda p: -sum((p[k] - 1.0) ** 2 for k in p),
            param_space={'x': [0.0, 0.5, 1.0, 1.5, 2.0], 'y': [0.0, 0.5, 1.0, 1.5, 2.0]},
            n_trials=50,
        )
        assert result.constraints_satisfied is True

    def test_exploration_mode_halves_penalty(self):
        normal = SoftConstrainedOptimizer(exploration_mode=False)
        explore = SoftConstrainedOptimizer(exploration_mode=True)
        assert explore._exploration_mode is True
        assert explore.EXPLORATION_PENALTY_MULTIPLIER == 0.5

    def test_oos_constraint_check(self):
        optimizer = SoftConstrainedOptimizer(oos_min_ratio=0.90)
        assert optimizer._check_oos_constraint(2.0, 1.9) is True
        assert optimizer._check_oos_constraint(2.0, 1.5) is False

    def test_calibrate_coefficients(self):
        optimizer = SoftConstrainedOptimizer()
        coeffs = optimizer.calibrate_coefficients(sharpe_std=0.5)
        assert coeffs['pullback_soft'] < coeffs['take_profit_hard']
        assert coeffs['holding_time_soft'] < coeffs['pullback_soft']


class TestPerformanceTierManager:
    def test_three_tiers_exist(self):
        assert len(PerformanceTier) == 3

    def test_tier3_default(self):
        mgr = PerformanceTierManager(
            data_availability='unknown', compute_capacity='unknown',
        )
        assert mgr.get_current_tier() == PerformanceTier.TIER_3_FAST

    def test_tier1_full_resources(self):
        mgr = PerformanceTierManager(
            data_availability='tick_ready', compute_capacity='high',
        )
        assert mgr.get_current_tier() == PerformanceTier.TIER_1_FULL

    def test_upgrade_on_sharpe(self):
        mgr = PerformanceTierManager(
            data_availability='tick_ready', compute_capacity='high',
        )
        mgr._current_tier = PerformanceTier.TIER_3_FAST
        result = mgr.attempt_upgrade(achieved_sharpe=1.2)
        assert result == PerformanceTier.TIER_2_CORE

    def test_upgrade_fails_low_sharpe(self):
        mgr = PerformanceTierManager()
        result = mgr.attempt_upgrade(achieved_sharpe=0.5)
        assert result is None

    def test_force_downgrade(self):
        mgr = PerformanceTierManager(
            data_availability='tick_ready', compute_capacity='high',
        )
        mgr._current_tier = PerformanceTier.TIER_2_CORE
        new = mgr.force_downgrade("数据损坏")
        assert new == PerformanceTier.TIER_3_FAST

    def test_tier_config_summary(self):
        mgr = PerformanceTierManager()
        summary = mgr.get_tier_summary()
        assert 'current_tier' in summary
        assert summary['min_sharpe'] == 1.0

    def test_assess_environment(self):
        mgr = PerformanceTierManager()
        result = mgr.assess_environment(
            data_rows=2_000_000_000, available_memory_gb=64, gpu_available=True,
        )
        assert result['data_availability'] == 'tick_ready'
        assert result['compute_capacity'] == 'high'


class TestDuckDBTickStorage:
    def test_create_and_close(self, tmp_path):
        db_path = str(tmp_path / 'test_tick.duckdb')
        storage = DuckDBTickStorage(db_path)
        stats = storage.get_stats()
        assert stats.total_rows == 0
        storage.close()

    def test_import_and_query(self, tmp_path):
        db_path = str(tmp_path / 'test_tick.duckdb')
        with DuckDBTickStorage(db_path) as storage:
            n = 100
            df = pd.DataFrame({
                'symbol': ['IF2605'] * n,
                'timestamp': pd.date_range('2024-01-01', periods=n, freq='min'),
                'price': 4000.0 + np.random.randn(n),
                'volume': np.random.randint(1, 100, n),
                'bid': 3999.0 + np.random.randn(n),
                'ask': 4001.0 + np.random.randn(n),
                'iv': np.full(n, 0.20),
                'delta': np.full(n, 0.50),
                'gamma': np.full(n, 0.002),
                'theta': np.full(n, -0.05),
                'vega': np.full(n, 5.0),
                'strike': np.full(n, 4000.0),
                'expiry': [pd.Timestamp('2024-03-15')] * n,
                'option_type': ['C'] * n,
                'underlying': ['IF2605'] * n,
            })
            inserted = storage.bulk_import_dataframe(df)
            assert inserted == n

            result = storage.query_ticks_by_date_range(symbol='IF2605')
            assert len(result) == n

            stats = storage.get_stats()
            assert stats.total_rows == n
            assert stats.total_symbols >= 1


class TestExternalValidationPipeline:
    def test_four_default_sources(self):
        pipeline = ExternalValidationPipeline()
        assert len(pipeline._sources) == 4

    def test_mock_validation_pass(self):
        pipeline = ExternalValidationPipeline()
        mock_fns = pipeline.generate_mock_external_data(
            {'iv_median': 0.20, 'sharpe': 1.5, 'delta': 0.5, 'state_accuracy': 0.7},
            noise_level=0.01,
        )
        report = pipeline.validate_quarter(
            'Q1-2026',
            {'iv_median': 0.20, 'sharpe': 1.5, 'delta': 0.5, 'state_accuracy': 0.7},
            external_fetch_functions=mock_fns,
        )
        assert report.overall_status in (
            ValidationStatus.PASS, ValidationStatus.WARNING,
        )

    def test_consecutive_fail_counting(self):
        pipeline = ExternalValidationPipeline(consecutive_fail_limit=2)
        pipeline._report_history.append(QuarterlyReport(
            quarter='Q3-2025', results=[], overall_status=ValidationStatus.FAIL,
            max_deviation=0.15, action_required='',
        ))
        pipeline._report_history.append(QuarterlyReport(
            quarter='Q4-2025', results=[], overall_status=ValidationStatus.FAIL,
            max_deviation=0.12, action_required='',
        ))
        assert pipeline._count_consecutive_fails() == 2

    def test_drift_detection(self):
        pipeline = ExternalValidationPipeline()
        internal = pd.Series([0.20, 0.21, 0.19, 0.22, 0.20])
        external = pd.Series([0.20, 0.21, 0.19, 0.22, 0.20])
        result = pipeline.check_drift(internal, external)
        assert result['drift_detected'] is False

    def test_drift_detected(self):
        pipeline = ExternalValidationPipeline()
        internal = pd.Series([0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65])
        external = pd.Series([0.20, 0.21, 0.19, 0.22, 0.20, 0.21, 0.19, 0.22, 0.20, 0.21])
        result = pipeline.check_drift(internal, external)
        assert result['mean_deviation'] > 0.05 or result['correlation'] < 0.9
