"""
V7.1 task_scheduler 核心功能单元测试
覆盖：常量完整性、Alpha CI、L-2冲突裁决、验证分级、参数分级、HFT保真度、
      两阶段止损(P1-1)、影子参数独立性(P0-Q1)
"""
import pytest
import sys
import os
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.param_pool.task_scheduler import (
    DEEP_VALIDATION_TIERS,
    PARAM_TIERS,
    HFT_TICK_PARAMS,
    L2_HYPERPARAMS,
    L2_PARAM_GRID,
    L2_CONFLICT_RESOLUTION,
    PARAM_SOURCE_ANNOTATION,
    compute_alpha_confidence_interval,
    check_l2_conflict,
    run_deep_validation_tiered,
    validate_doomed_tests,
    validate_market_friendliness_baseline,
    _check_two_stage_stop,
    _BacktestPosition,
    _BacktestState,
    _try_open,
    validate_shadow_param_independence,
    REASON_MULTIPLIERS,
    STRATEGY_SHADOW_DEFAULTS,
    PARAM_DEFAULTS,
    PARAM_DEFAULTS_HFT,
    PARAM_DEFAULTS_BOX_EXTREME,
    PARAM_DEFAULTS_BOX_SPRING,
    PARAM_DEFAULTS_ARBITRAGE,
    PARAM_DEFAULTS_MARKET_MAKING,
    PARAM_DEFAULTS_SHADOW_A,
    PARAM_DEFAULTS_SHADOW_B,
    PARAM_DEFAULTS_HFT_SHADOW_A,
    PARAM_DEFAULTS_HFT_SHADOW_B,
    PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A,
    PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B,
    PARAM_DEFAULTS_BOX_SPRING_SHADOW_A,
    PARAM_DEFAULTS_BOX_SPRING_SHADOW_B,
    PARAM_DEFAULTS_ARBITRAGE_SHADOW_A,
    PARAM_DEFAULTS_ARBITRAGE_SHADOW_B,
    PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A,
    PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B,
    SCAN_TARGET_SORT_METRIC,
    validate_default_values_in_grids,
    _select_top_k_train,
)


class TestDeepValidationTiers:
    def test_three_tiers_exist(self):
        assert set(DEEP_VALIDATION_TIERS.keys()) == {'must_run', 'quarterly', 'annual'}

    def test_must_run_is_minimal(self):
        must = DEEP_VALIDATION_TIERS['must_run']['tests']
        assert 'doomed_tests' in must
        assert 'market_friendliness' in must
        assert len(must) == 7

    def test_quarterly_superset_must_run(self):
        must = set(DEEP_VALIDATION_TIERS['must_run']['tests'])
        quarterly = set(DEEP_VALIDATION_TIERS['quarterly']['tests'])
        assert must.issubset(quarterly)

    def test_annual_is_full_set(self):
        annual = DEEP_VALIDATION_TIERS['annual']['tests']
        all_known = {
            'doomed_tests', 'market_friendliness', 'liquidity_stress',
            'regime_robustness', 'hft_temporal_robustness',
            'cross_strategy_correlation', 'logic_transferability'
        }
        assert set(annual) == all_known


class TestParamTiers:
    def test_four_tiers_exist(self):
        assert set(PARAM_TIERS.keys()) == {
            'must_calibrate_every_run', 'quarterly_review',
            'annual_or_phase_change', 'hft_replay_only'
        }

    def test_hft_replay_only_contains_tick_params(self):
        hft = PARAM_TIERS['hft_replay_only']
        assert 'hft_signal_confirm_ticks' in hft
        assert 'hft_cooldown_ms' in hft
        assert 'hft_min_imbalance' in hft

    def test_l2_params_in_correct_tiers(self):
        must = PARAM_TIERS['must_calibrate_every_run']
        quarterly = PARAM_TIERS['quarterly_review']
        annual = PARAM_TIERS['annual_or_phase_change']
        assert 'non_other_ratio_threshold' in must
        assert 'state_confirm_bars' in quarterly
        assert 'logic_reversal_threshold' in annual


class TestHftTickParams:
    def test_three_tick_params(self):
        assert HFT_TICK_PARAMS == {'hft_signal_confirm_ticks', 'hft_cooldown_ms', 'hft_min_imbalance', 'hft_hard_time_stop_ms'}


class TestL2Hyperparams:
    def test_count(self):
        assert len(L2_HYPERPARAMS) == 3

    def test_names(self):
        names = {p['name'] if isinstance(p, dict) else p for p in L2_HYPERPARAMS}
        expected = {'non_other_ratio_threshold', 'state_confirm_bars', 'logic_reversal_threshold'}
        assert names == expected

    def test_grid_sizes(self):
        assert len(L2_PARAM_GRID['non_other_ratio_threshold']) == 13
        assert len(L2_PARAM_GRID['state_confirm_bars']) == 5
        assert len(L2_PARAM_GRID['logic_reversal_threshold']) == 5
        total = 13 * 5 * 5
        assert total == 325


class TestAlphaConfidenceInterval:
    def test_reliable_sharpe(self):
        result = compute_alpha_confidence_interval(
            strategy_return=0.20, strategy_sharpe=2.5, n_signals=500, confidence=0.95
        )
        assert result['sharpe_ci_lower'] < 2.5 < result['sharpe_ci_upper']
        assert result['action'] in ('reliable', 'flag')

    def test_low_signal_flag(self):
        result = compute_alpha_confidence_interval(
            strategy_return=0.05, strategy_sharpe=0.8, n_signals=20, confidence=0.95
        )
        assert result['action'] in ('flag', 'reduce_weight', 'eliminate')

    def test_ci_width_shrinks_with_more_signals(self):
        r1 = compute_alpha_confidence_interval(0.15, 1.5, 50, 0.95)
        r2 = compute_alpha_confidence_interval(0.15, 1.5, 500, 0.95)
        assert r2['ci_width'] < r1['ci_width']

    def test_ci_contains_sharpe(self):
        result = compute_alpha_confidence_interval(0.15, 1.5, 100, 0.95)
        assert result['sharpe_ci_lower'] <= 1.5 <= result['sharpe_ci_upper']


class TestL2ConflictResolution:
    def test_conflict_detected(self):
        result = check_l2_conflict(
            l2_params_independent={'non_other_ratio_threshold': 0.5, 'state_confirm_bars': 5, 'logic_reversal_threshold': 2.0},
            l2_params_main={'non_other_ratio_threshold': 0.3, 'state_confirm_bars': 2, 'logic_reversal_threshold': 1.0},
            tolerance=0.1
        )
        assert result['any_conflict'] is True
        assert result['action'] == 'expand_independent_data_or_manual_review'

    def test_no_conflict(self):
        result = check_l2_conflict(
            l2_params_independent={'non_other_ratio_threshold': 0.4, 'state_confirm_bars': 4, 'logic_reversal_threshold': 1.5},
            l2_params_main={'non_other_ratio_threshold': 0.39, 'state_confirm_bars': 4, 'logic_reversal_threshold': 1.48},
            tolerance=0.1
        )
        assert result['any_conflict'] is False
        assert result['action'] == 'proceed_to_step2'

    def test_resolution_rule(self):
        assert L2_CONFLICT_RESOLUTION['rule'] == 'independent_dataset_wins'


class TestParamSourceAnnotation:
    def test_count(self):
        assert len(PARAM_SOURCE_ANNOTATION) == 16

    def test_critical_params_annotated(self):
        names = set(PARAM_SOURCE_ANNOTATION)
        assert 'non_other_ratio_threshold' in names
        assert 'state_confirm_bars' in names
        assert 'logic_reversal_threshold' in names
        assert 'shadow_alpha_threshold' in names


class TestTwoStageStop:
    """P1-Q1: 两阶段硬时间止损逻辑测试（P1-R8-10修复: 32用例）"""

    def _make_pos(self, open_price=4000.0, volume=1, open_time=None):
        if open_time is None:
            open_time = pd.Timestamp("2026-05-10 09:30:00")
        return _BacktestPosition(
            instrument_id="IF2605",
            volume=volume,
            open_price=open_price,
            open_time=open_time,
            stop_profit_price=open_price * 1.015,
            stop_loss_price=open_price * 0.995,
            open_reason="CORRECT_RESONANCE",
        )

    # --- Stage1 测试 (16用例) ---

    def test_stage1_not_triggered_before_min_minutes(self):
        pos = self._make_pos()
        bar_time = pd.Timestamp("2026-05-10 10:30:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed
        assert result is False

    def test_stage1_triggered_after_min_minutes_with_profit(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_not_triggered_profit_below_threshold(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.001
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 1.001
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed

    def test_stage1_triggered_exact_min_minutes(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_not_triggered_at_zero_minutes(self):
        pos = self._make_pos()
        bar_time = pd.Timestamp("2026-05-10 09:31:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed

    def test_stage1_short_volume_position(self):
        pos = self._make_pos(volume=-1)
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 0.997
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_long_zero_profit(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.0
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed

    def test_stage1_negative_profit(self):
        pos = self._make_pos()
        pos.max_float_profit = -0.005
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 0.995
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed

    def test_stage1_custom_min_minutes_30(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 10:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 30.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_custom_min_minutes_120_not_reached(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 10:30:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 120.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed

    def test_stage1_exact_profit_threshold(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.002
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 1.002
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_once_passed_stays_passed(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed
        bar_time2 = pd.Timestamp("2026-05-10 13:00:00")
        price2 = pos.open_price * 0.99
        result2 = _check_two_stage_stop(pos, price2, bar_time2, params)
        assert pos.stage1_passed

    def test_stage1_high_profit_threshold_not_met(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.01}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed

    def test_stage1_very_small_min_minutes(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 09:31:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 1.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_zero_min_minutes_with_profit(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 09:30:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 0.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_stage1_different_open_times(self):
        pos = self._make_pos(open_time=pd.Timestamp("2026-05-10 14:00:00"))
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 15:30:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    # --- Stage2 测试 (16用例) ---

    def test_stage2_slope_negative_triggers_stop(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.005, 0.004, 0.003, 0.002, 0.001, 0.0, -0.001, -0.002, -0.003, -0.004]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 0.996
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_slope_positive_no_stop(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.010
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_not_checked_before_stage1(self):
        pos = self._make_pos()
        pos.profit_history = [0.005, 0.004, 0.003, 0.002, 0.001, 0.0, -0.001, -0.002, -0.003, -0.004]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 0.996
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert not pos.stage1_passed
        assert result is False

    def test_stage2_flat_slope_no_stop(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.003] * 10
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_insufficient_history(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.005, 0.004, 0.003]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_custom_slope_threshold(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.005, 0.0045, 0.004, 0.0035, 0.003, 0.0025, 0.002, 0.0015, 0.001, 0.0005]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.001
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": -0.0001}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_short_window(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.003, 0.001]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.001
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 2, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_oscillating_profit_no_stop(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.003, 0.001, 0.004, 0.002, 0.005, 0.003, 0.004, 0.002, 0.003, 0.003]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_sharp_decline_triggers(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.010, 0.010, 0.010, 0.010, 0.010, 0.010, 0.010, 0.010, 0.005, -0.010]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 0.990
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_gradual_decline_triggers(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.010, 0.009, 0.008, 0.007, 0.006, 0.005, 0.004, 0.003, 0.002, 0.001]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.001
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_short_position_decline(self):
        pos = self._make_pos(volume=-1)
        pos.stage1_passed = True
        pos.profit_history = [0.005, 0.004, 0.003, 0.002, 0.001, 0.0, -0.001, -0.002, -0.003, -0.004]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.004
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_zero_slope_threshold_exact(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.003, 0.003, 0.003, 0.003, 0.003, 0.003, 0.003, 0.003, 0.003, 0.003]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_single_bar_window(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.003]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.003
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 1, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_large_window_larger_than_history(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.005, 0.004, 0.003, 0.002, 0.001]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.001
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 20, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_stage2_v_shape_recovery_no_stop(self):
        pos = self._make_pos()
        pos.stage1_passed = True
        pos.profit_history = [0.010, 0.005, 0.002, 0.001, 0.001, 0.002, 0.004, 0.006, 0.008, 0.010]
        bar_time = pd.Timestamp("2026-05-10 14:00:00")
        price = pos.open_price * 1.010
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002,
                  "stage2_slope_window": 10, "stage2_slope_threshold": 0.0}
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False


class TestBacktestTryOpenRiskGate:
    def _make_bar(self, **overrides):
        base = {
            'symbol': 'IF2606',
            'minute': pd.Timestamp('2026-05-10 09:35:00'),
            'close': 4000.0,
            'strength': 0.9,
            'imbalance': 1,
            '_spread_quality': 1,
            '_option_metadata_quality': 1,
            'bid_ask_spread': 0.2,
        }
        base.update(overrides)
        return pd.Series(base)

    def test_try_open_fail_safe_blocks_when_risk_service_raises(self):
        bt = _BacktestState()
        params = {
            'close_take_profit_ratio': 1.8,  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
            'close_stop_loss_ratio': 0.3,  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
            'max_risk_ratio': 0.8,  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
            'lots_min': 3,  # R24-P1-DF-08修复: 5→3与生产代码CENTRALIZED_DEFAULTS对齐
            'signal_cooldown_sec': 60.0,  # R24-P0-DF-02修复: 0→60.0与生产代码CENTRALIZED_DEFAULTS对齐
            'max_signals_per_window': 5,
            'margin_ratio': 0.1,
        }

        with patch('ali2026v3_trading.risk_service.get_risk_service', side_effect=RuntimeError('boom')):
            _try_open(bt, self._make_bar(), params)

        assert bt.positions == {}
        assert bt.total_signals == 0

    def test_try_open_passes_complete_signal_to_risk_service(self):
        bt = _BacktestState()
        params = {
            'close_take_profit_ratio': 1.8,  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
            'close_stop_loss_ratio': 0.3,  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
            'max_risk_ratio': 0.8,  # R24修复: 断言值与生产代码DEFAULT_PARAM_TABLE对齐
            'lots_min': 3,  # R24-P1-DF-08修复: 5→3与生产代码CENTRALIZED_DEFAULTS对齐
            'signal_cooldown_sec': 60.0,  # R24-P0-DF-02修复: 0→60.0与生产代码CENTRALIZED_DEFAULTS对齐
            'max_signals_per_window': 5,
            'margin_ratio': 0.1,
        }
        risk_service = MagicMock()
        risk_service.check_before_trade.return_value = MagicMock(is_block=False, reason='')
        risk_service.check_regulatory_compliance.return_value = {'compliant': True}
        risk_service.check_capital_sufficiency.return_value = {'sufficient': True}
        risk_service.check_exchange_status.return_value = {'status': 'OPEN'}

        with patch('ali2026v3_trading.risk_service.get_risk_service', return_value=risk_service):
            _try_open(bt, self._make_bar(), params)

        signal = risk_service.check_before_trade.call_args[0][0]
        assert signal['action'] == 'OPEN'
        assert signal['direction'] == 'BUY'
        assert signal['price'] == 4000.0
        assert signal['volume'] >= 1
        assert signal['amount'] > 0
        assert bt.total_signals == 1
        assert 'IF2606' in bt.positions

    def test_stage2_slope_negative_triggers_close(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.005
        pos.stage1_passed = True
        for i in range(10):
            pos.profit_history.append(0.005 - i * 0.0005)
        bar_time = pd.Timestamp("2026-05-10 11:10:00")
        price = pos.open_price * 1.001
        params = {
            "stage1_min_minutes": 90.0,
            "stage1_profit_threshold": 0.002,
            "stage2_slope_window": 10,
            "stage2_slope_threshold": 0.0,
        }
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is True

    def test_stage2_slope_positive_does_not_trigger(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.005
        pos.stage1_passed = True
        for i in range(10):
            pos.profit_history.append(0.003 + i * 0.0002)
        bar_time = pd.Timestamp("2026-05-10 11:10:00")
        price = pos.open_price * 1.005
        params = {
            "stage1_min_minutes": 90.0,
            "stage1_profit_threshold": 0.002,
            "stage2_slope_window": 10,
            "stage2_slope_threshold": 0.0,
        }
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False

    def test_short_position_stage1(self):
        pos = self._make_pos(volume=-1)
        pos.max_float_profit = 0.003
        bar_time = pd.Timestamp("2026-05-10 11:00:00")
        price = pos.open_price * 0.997
        params = {"stage1_min_minutes": 90.0, "stage1_profit_threshold": 0.002}
        _check_two_stage_stop(pos, price, bar_time, params)
        assert pos.stage1_passed

    def test_insufficient_history_no_trigger(self):
        pos = self._make_pos()
        pos.max_float_profit = 0.005
        pos.stage1_passed = True
        pos.profit_history = [0.005]
        bar_time = pd.Timestamp("2026-05-10 11:01:00")
        price = pos.open_price * 1.005
        params = {
            "stage1_min_minutes": 90.0,
            "stage1_profit_threshold": 0.002,
            "stage2_slope_window": 10,
            "stage2_slope_threshold": 0.0,
        }
        result = _check_two_stage_stop(pos, price, bar_time, params)
        assert result is False


class TestShadowParamIndependence:
    """P0-Q1: 影子策略参数独立性验证"""

    def test_all_groups_pass_threshold(self):
        results = validate_shadow_param_independence(threshold=0.20)
        for label, diff in results.items():
            assert diff >= 0.20, f"{label} 差异度 {diff:.4f} < 20%阈值"

    def test_shadow_defaults_has_six_groups(self):
        assert set(STRATEGY_SHADOW_DEFAULTS.keys()) == {
            'hft', 'main', 'box_extreme', 'box_spring', 'arbitrage', 'market_making'
        }

    def test_each_group_has_shadow_a_and_b(self):
        for group_name, shadow_map in STRATEGY_SHADOW_DEFAULTS.items():
            assert 'shadow_a' in shadow_map, f"{group_name} 缺少 shadow_a"
            assert 'shadow_b' in shadow_map, f"{group_name} 缺少 shadow_b"


class TestReasonMultipliersCompleteness:
    """P1-2: REASON_MULTIPLIERS完整性验证"""

    def test_all_reasons_have_three_multipliers(self):
        for reason, mults in REASON_MULTIPLIERS.items():
            assert 'tp_mult' in mults, f"{reason} 缺少 tp_mult"
            assert 'sl_mult' in mults, f"{reason} 缺少 sl_mult"
            assert 'time_mult' in mults, f"{reason} 缺少 time_mult"

    def test_manual_reason_is_identity(self):
        manual = REASON_MULTIPLIERS.get("MANUAL", {})
        assert manual.get("tp_mult") == 1.0
        assert manual.get("sl_mult") == 1.0
        assert manual.get("time_mult") == 1.0

    def test_at_least_four_reasons(self):
        assert len(REASON_MULTIPLIERS) >= 4


class TestParamDefaults:
    """P1-4: PARAM_DEFAULTS默认值断言（6主策略+6影子A+6影子B=18实例）"""

    _ALL_MASTER_DEFAULTS = {
        'main': PARAM_DEFAULTS,
        'hft': PARAM_DEFAULTS_HFT,
        'box_extreme': PARAM_DEFAULTS_BOX_EXTREME,
        'box_spring': PARAM_DEFAULTS_BOX_SPRING,
        'arbitrage': PARAM_DEFAULTS_ARBITRAGE,
        'market_making': PARAM_DEFAULTS_MARKET_MAKING,
    }

    _ALL_SHADOW_A_DEFAULTS = {
        'main': PARAM_DEFAULTS_SHADOW_A,
        'hft': PARAM_DEFAULTS_HFT_SHADOW_A,
        'box_extreme': PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A,
        'box_spring': PARAM_DEFAULTS_BOX_SPRING_SHADOW_A,
        'arbitrage': PARAM_DEFAULTS_ARBITRAGE_SHADOW_A,
        'market_making': PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A,
    }

    _ALL_SHADOW_B_DEFAULTS = {
        'main': PARAM_DEFAULTS_SHADOW_B,
        'hft': PARAM_DEFAULTS_HFT_SHADOW_B,
        'box_extreme': PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B,
        'box_spring': PARAM_DEFAULTS_BOX_SPRING_SHADOW_B,
        'arbitrage': PARAM_DEFAULTS_ARBITRAGE_SHADOW_B,
        'market_making': PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B,
    }

    def test_six_master_defaults_exist(self):
        assert len(self._ALL_MASTER_DEFAULTS) == 6

    def test_six_shadow_a_defaults_exist(self):
        assert len(self._ALL_SHADOW_A_DEFAULTS) == 6

    def test_six_shadow_b_defaults_exist(self):
        assert len(self._ALL_SHADOW_B_DEFAULTS) == 6

    def test_total_eighteen_instances(self):
        total = len(self._ALL_MASTER_DEFAULTS) + len(self._ALL_SHADOW_A_DEFAULTS) + len(self._ALL_SHADOW_B_DEFAULTS)
        assert total == 18

    def test_main_defaults_non_other_ratio(self):
        assert PARAM_DEFAULTS['non_other_ratio_threshold'] == 0.65

    def test_hft_defaults_non_other_ratio(self):
        assert PARAM_DEFAULTS_HFT['non_other_ratio_threshold'] == 0.4

    def test_all_defaults_have_close_take_profit(self):
        for name, defaults in self._ALL_MASTER_DEFAULTS.items():
            assert 'close_take_profit_ratio' in defaults, f"{name} 缺少 close_take_profit_ratio"
            assert 'close_stop_loss_ratio' in defaults, f"{name} 缺少 close_stop_loss_ratio"

    def test_shadow_a_degraded_tp(self):
        assert PARAM_DEFAULTS_SHADOW_A['close_take_profit_ratio'] < PARAM_DEFAULTS['close_take_profit_ratio']
        assert PARAM_DEFAULTS_HFT_SHADOW_A['close_take_profit_ratio'] < PARAM_DEFAULTS_HFT['close_take_profit_ratio']

    def test_shadow_b_degraded_tp(self):
        assert PARAM_DEFAULTS_SHADOW_B['close_take_profit_ratio'] < PARAM_DEFAULTS['close_take_profit_ratio']
        assert PARAM_DEFAULTS_HFT_SHADOW_B['close_take_profit_ratio'] < PARAM_DEFAULTS_HFT['close_take_profit_ratio']

    def test_shadow_a_lower_risk_than_master(self):
        assert PARAM_DEFAULTS_SHADOW_A['max_risk_ratio'] < PARAM_DEFAULTS['max_risk_ratio']

    def test_shadow_b_lower_risk_than_master(self):
        assert PARAM_DEFAULTS_SHADOW_B['max_risk_ratio'] < PARAM_DEFAULTS['max_risk_ratio']

    def test_arbitrage_has_specific_keys(self):
        assert 'arb_deviation_threshold_bps' in PARAM_DEFAULTS_ARBITRAGE
        assert 'arb_reversion_target_bps' in PARAM_DEFAULTS_ARBITRAGE
        assert 'arb_min_confidence' in PARAM_DEFAULTS_ARBITRAGE
        assert 'arb_max_hold_minutes' in PARAM_DEFAULTS_ARBITRAGE

    def test_market_making_has_specific_keys(self):
        assert 'mm_ioc_signal_threshold' in PARAM_DEFAULTS_MARKET_MAKING
        assert 'mm_spread_target_bps' in PARAM_DEFAULTS_MARKET_MAKING
        assert 'mm_max_inventory_lots' in PARAM_DEFAULTS_MARKET_MAKING
        assert 'mm_rebalance_threshold' in PARAM_DEFAULTS_MARKET_MAKING


class TestEighteenStrategyCoverage:
    """P1-5: 18策略全覆盖断言 — 评判系统ALL_18_STRATEGY_IDS与参数池STRATEGY_SHADOW_DEFAULTS对齐"""

    @pytest.fixture(autouse=True)
    def _import_judgment(self):
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import (
            ALL_18_STRATEGY_IDS,
            SIX_STRATEGY_KEYS,
            THREE_VARIANTS,
        )
        self.all_18_ids = ALL_18_STRATEGY_IDS
        self.six_keys = SIX_STRATEGY_KEYS
        self.three_variants = THREE_VARIANTS

    def test_eighteen_ids_count(self):
        assert len(self.all_18_ids) == 18

    def test_six_strategy_keys(self):
        assert len(self.six_keys) == 6
        expected = {"high_freq", "resonance", "box", "spring", "arbitrage", "market_making"}
        assert set(self.six_keys) == expected

    def test_three_variants(self):
        assert set(self.three_variants) == {"master", "shadow_a", "shadow_b"}

    def test_each_strategy_has_master_and_shadows(self):
        for sk in self.six_keys:
            assert f"{sk}_master" in self.all_18_ids, f"{sk} 缺少 master"
            assert f"{sk}_shadow_a" in self.all_18_ids, f"{sk} 缺少 shadow_a"
            assert f"{sk}_shadow_b" in self.all_18_ids, f"{sk} 缺少 shadow_b"

    def test_shadow_defaults_cover_all_six_groups(self):
        assert set(STRATEGY_SHADOW_DEFAULTS.keys()) == {
            'hft', 'main', 'box_extreme', 'box_spring', 'arbitrage', 'market_making'
        }

    def test_judgment_ids_align_with_param_pool_groups(self):
        judgment_to_param = {
            "high_freq": "hft",
            "resonance": "main",
            "box": "box_extreme",
            "spring": "box_spring",
            "arbitrage": "arbitrage",
            "market_making": "market_making",
        }
        for jk, pk in judgment_to_param.items():
            assert pk in STRATEGY_SHADOW_DEFAULTS, f"评判键{jk}→参数池键{pk}不存在"
            shadow_map = STRATEGY_SHADOW_DEFAULTS[pk]
            assert 'shadow_a' in shadow_map, f"{pk}缺少shadow_a"
            assert 'shadow_b' in shadow_map, f"{pk}缺少shadow_b"

    def test_no_duplicate_ids(self):
        assert len(self.all_18_ids) == len(set(self.all_18_ids))


class TestGridDefaultAndSortAlignment:
    """验收标准6/7: 默认值落在网格中 + 扫描目标与排序指标一致"""

    def test_defaults_exist_in_all_scan_grids(self):
        violations = validate_default_values_in_grids()
        assert violations == [], f"发现默认值不在扫描网格中的项: {violations[:5]}"

    def test_round_sort_metric_mapping(self):
        assert SCAN_TARGET_SORT_METRIC["round1"] == "minute_sharpe"
        assert SCAN_TARGET_SORT_METRIC["round2"] == "minute_sharpe"
        assert SCAN_TARGET_SORT_METRIC["round3"] == "risk_adjusted_score"
        assert SCAN_TARGET_SORT_METRIC["round4"] == "cascade_final_score"

    def test_select_top_k_prefers_target_metric(self):
        rows = [
            {"is_train": True, "minute_sharpe": 3.0, "risk_adjusted_score": 0.4, "params": {"id": "a"}},
            {"is_train": True, "minute_sharpe": 2.0, "risk_adjusted_score": 0.9, "params": {"id": "b"}},
            {"is_train": True, "minute_sharpe": 1.5, "risk_adjusted_score": 0.7, "params": {"id": "c"}},
        ]
        top = _select_top_k_train(rows, "risk_adjusted_score", 2)
        picked = [x["params"]["id"] for x in top]
        assert picked == ["b", "c"]
