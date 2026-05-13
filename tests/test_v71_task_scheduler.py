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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.参数池.task_scheduler import (
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
    validate_shadow_param_independence,
    REASON_MULTIPLIERS,
    STRATEGY_SHADOW_DEFAULTS,
)


class TestDeepValidationTiers:
    def test_three_tiers_exist(self):
        assert set(DEEP_VALIDATION_TIERS.keys()) == {'must_run', 'quarterly', 'annual'}

    def test_must_run_is_minimal(self):
        must = DEEP_VALIDATION_TIERS['must_run']['tests']
        assert 'doomed_tests' in must
        assert 'market_friendliness' in must
        assert len(must) <= 3

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
        assert HFT_TICK_PARAMS == {'hft_signal_confirm_ticks', 'hft_cooldown_ms', 'hft_min_imbalance'}


class TestL2Hyperparams:
    def test_count(self):
        assert len(L2_HYPERPARAMS) == 3

    def test_names(self):
        names = {p['name'] if isinstance(p, dict) else p for p in L2_HYPERPARAMS}
        expected = {'non_other_ratio_threshold', 'state_confirm_bars', 'logic_reversal_threshold'}
        assert names == expected

    def test_grid_sizes(self):
        assert len(L2_PARAM_GRID['non_other_ratio_threshold']) == 8
        assert len(L2_PARAM_GRID['state_confirm_bars']) == 5
        assert len(L2_PARAM_GRID['logic_reversal_threshold']) == 5
        total = 8 * 5 * 5
        assert total == 200


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
        assert len(PARAM_SOURCE_ANNOTATION) == 13

    def test_critical_params_annotated(self):
        names = set(PARAM_SOURCE_ANNOTATION)
        assert 'non_other_ratio_threshold' in names
        assert 'state_confirm_bars' in names
        assert 'logic_reversal_threshold' in names
        assert 'shadow_alpha_threshold' in names


class TestTwoStageStop:
    """P1-Q1: 两阶段硬时间止损逻辑测试（4用例）"""

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

    def test_shadow_defaults_has_four_groups(self):
        assert set(STRATEGY_SHADOW_DEFAULTS.keys()) == {
            'hft', 'main', 'box_extreme', 'box_spring'
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
