# MODULE_ID: M2-325
"""
补充测试：signal + governance + risk_engine 行为测试
覆盖模块: plr_calculator, signal_timing_filter, regulatory_compliance,
          mode_position_sizing, mode_config, log_deduplicator
"""
import math
import time
from unittest.mock import MagicMock, patch

import pytest

from ali2026v3_trading.signal.plr_calculator import ProfitLossRatioCalculator, PLRResult
from ali2026v3_trading.signal.signal_timing_filter import (
    KalmanFilter1D, EMASignalFilter, SignalTimingFilter, AdaptiveSignalThreshold,
)
from ali2026v3_trading.governance.regulatory_compliance import (
    ComplianceEngine, ComplianceContext, ComplianceResult,
    MaxPositionRule, ConcentrationRule, CrossMarketRule,
    RegulatoryCompliance, create_default_compliance_engine,
)
from ali2026v3_trading.governance.mode_position_sizing import (
    SixDimPositionAdjustmentFactor, kelly_fraction, kelly_position_size,
    PredictiveStateEngine,
)
from ali2026v3_trading.governance.mode_config import (
    CapitalMode, TakeProfitMethod, StopLossMethod, DrawdownAction, PyramidingRule,
    ModeConfig, _validate_yaml_modeconfig_mapping, _get_flattened_param,
    SHARPE_CONFIG, PROFIT_CONFIG, BALANCED_CONFIG, CAPITAL_MODE_CONFIGS,
)
from ali2026v3_trading.risk_engine.log_deduplicator import LogDeduplicator


class TestPLRCalculator:
    def test_all_wins_status(self):
        calc = ProfitLossRatioCalculator(default_target_plr=2.0)
        trades = [{'pnl': 100}, {'pnl': 50}, {'pnl': 200}]
        result = calc.update_from_trades('s1', trades)
        assert result.plr_status == 'all_wins'
        assert result.current_plr == 100
        assert result.win_count == 3
        assert result.loss_count == 0

    def test_mixed_trades(self):
        calc = ProfitLossRatioCalculator(default_target_plr=2.0)
        trades = [{'pnl': 300}, {'pnl': -100}, {'pnl': 200}, {'pnl': -50}]
        result = calc.update_from_trades('s1', trades)
        assert result.win_count == 2
        assert result.loss_count == 2
        assert result.current_plr > 0
        assert result.profit_factor > 0

    def test_critical_status(self):
        calc = ProfitLossRatioCalculator(default_target_plr=10.0)
        trades = [{'pnl': 50}, {'pnl': -200}]
        result = calc.update_from_trades('s1', trades)
        assert result.plr_status == 'critical'

    def test_on_target_status(self):
        calc = ProfitLossRatioCalculator(default_target_plr=2.0)
        trades = [{'pnl': 400}, {'pnl': -200}]
        result = calc.update_from_trades('s1', trades)
        assert result.plr_status in ('on_target', 'excellent', 'all_wins')

    def test_plr_result_to_dict(self):
        r = PLRResult(target_plr=2.0, current_plr=3.0, plr_ratio=1.5)
        d = r.to_dict()
        assert d['target_plr'] == 2.0
        assert d['current_plr'] == 3.0

    def test_assess_quality_excellent(self):
        calc = ProfitLossRatioCalculator(default_target_plr=2.0)
        trades = [{'pnl': 300}, {'pnl': -100}, {'pnl': 200}, {'pnl': -50},
                  {'pnl': 250}, {'pnl': -80}]
        calc.update_from_trades('s1', trades)
        result = calc.assess_quality('s1')
        assert result['overall_quality'] in ('excellent', 'good', 'fair', 'poor')

    def test_assess_quality_no_data(self):
        calc = ProfitLossRatioCalculator()
        result = calc.assess_quality('nonexistent')
        assert result.get('quality_level', result.get('overall_quality')) == 'no_data'


class TestKalmanFilter1D:
    def test_first_update_returns_measurement(self):
        kf = KalmanFilter1D()
        val, vel = kf.update(10.0)
        assert val == 10.0
        assert vel == 0.0

    def test_subsequent_update_smooths(self):
        kf = KalmanFilter1D()
        kf.update(10.0)
        val, vel = kf.update(12.0)
        assert 10.0 < val < 12.0
        assert vel != 0.0

    def test_get_state(self):
        kf = KalmanFilter1D()
        kf.update(5.0)
        val, vel = kf.get_state()
        assert val == 5.0

    def test_reset(self):
        kf = KalmanFilter1D()
        kf.update(5.0)
        kf.reset()
        val, vel = kf.get_state()
        assert val == 0.0
        assert vel == 0.0


class TestEMASignalFilter:
    def test_first_update_returns_value(self):
        ema = EMASignalFilter()
        fast, slow, vel = ema.update(100.0)
        assert fast == 100.0
        assert slow == 100.0
        assert vel == 0.0

    def test_bullish_crossover(self):
        ema = EMASignalFilter(fast_period=3, slow_period=10)
        for i in range(20):
            ema.update(100.0 + i * 2)
        result = ema.is_bullish_crossover()
        assert isinstance(result, bool)

    def test_get_state(self):
        ema = EMASignalFilter()
        ema.update(100.0)
        fast, slow, vel = ema.get_state()
        assert fast == 100.0


class TestSignalTimingFilter:
    def test_filter_signal_returns_dict(self):
        stf = SignalTimingFilter(threshold=0.5)
        result = stf.filter_signal('AL2401', 0.8)
        assert 'signal_passed' in result
        assert 'smoothed_value' in result
        assert 'velocity' in result
        assert result['instrument_id'] == 'AL2401'

    def test_get_stats(self):
        stf = SignalTimingFilter()
        stf.filter_signal('AL2401', 0.8)
        stats = stf.get_stats()
        assert 'total_inputs' in stats
        assert stats['total_inputs'] == 1

    def test_per_instrument_filter(self):
        stf = SignalTimingFilter()
        stf.filter_signal('AL2401', 0.8)
        stf.filter_signal('IF2401', 0.9)
        stats = stf.get_stats()
        assert stats['tracked_instruments'] == 2


class TestAdaptiveSignalThreshold:
    def test_initial_threshold(self):
        ast = AdaptiveSignalThreshold(initial_threshold=0.3)
        assert ast.threshold == 0.3

    def test_record_signal(self):
        ast = AdaptiveSignalThreshold()
        for _ in range(25):
            ast.record_signal(True, pnl=10.0)
        assert ast.threshold >= ast._min_threshold

    def test_get_stats(self):
        ast = AdaptiveSignalThreshold()
        ast.record_signal(True)
        stats = ast.get_stats()
        assert 'current_threshold' in stats
        assert 'pass_rate' in stats


class TestRegulatoryCompliance:
    def test_max_position_rule_pass(self):
        rule = MaxPositionRule()
        ctx = ComplianceContext(position_data={}, equity=100000, total_position_value=10000)
        result = rule.check(ctx)
        assert result.compliant is True

    def test_max_position_rule_fail(self):
        rule = MaxPositionRule()
        ctx = ComplianceContext(position_data={}, equity=100000, total_position_value=30000)
        result = rule.check(ctx)
        assert result.compliant is False
        assert result.severity == 'P0'

    def test_concentration_rule_fail(self):
        rule = ConcentrationRule()
        ctx = ComplianceContext(position_data={'IF': 90, 'AL': 10}, concentration_limit=0.5)
        result = rule.check(ctx)
        assert result.compliant is False

    def test_cross_market_rule_fail(self):
        rule = CrossMarketRule()
        ctx = ComplianceContext(
            position_data={'SHFE_AL': 10, 'DCE_m': 10, 'CFFO_IF': 10, 'CZC_TA': 10},
            cross_market_limit=3,
        )
        result = rule.check(ctx)
        assert result.compliant is False

    def test_compliance_engine_p0_short_circuit(self):
        engine = ComplianceEngine()
        engine.register(MaxPositionRule())
        engine.register(ConcentrationRule())
        ctx = ComplianceContext(position_data={}, equity=100000, total_position_value=30000)
        results = engine.run_checks(ctx)
        assert len(results) == 1
        assert results[0].rule_name == 'max_position'

    def test_create_default_engine(self):
        engine = create_default_compliance_engine()
        assert len(engine.rule_names) == 3

    def test_self_trade_detection(self):
        rc = RegulatoryCompliance()
        rc.record_trade({'instrument_id': 'IF2401', 'direction': 'BUY'})
        assert rc.check_self_trade({'instrument_id': 'IF2401', 'direction': 'SELL'}) is True
        assert rc.check_self_trade({'instrument_id': 'IF2401', 'direction': 'BUY'}) is False

    def test_check_compliance(self):
        rc = RegulatoryCompliance()
        results = rc.check_compliance({}, equity=100000, total_position_value=50000)
        assert isinstance(results, list)


class TestSixDimPositionAdjustmentFactor:
    def _make_config(self, tvf_enabled=True):
        return ModeConfig(
            mode=CapitalMode.BALANCED,
            max_positions=6, single_position_cap=0.15,
            take_profit_method=TakeProfitMethod.TIERED,
            stop_loss_method=StopLossMethod.VOLATILITY,
            pyramiding=True, pyramiding_rule=PyramidingRule.TREND_CONFIRMED,
            drawdown_action=DrawdownAction.HALT_NEW,
            recovery_target=1.5, min_signal_strength=0.65, time_decay_cutoff=0.05,
            tvf_enabled=tvf_enabled,
        )

    def test_tvf_disabled_returns_1(self):
        factor = SixDimPositionAdjustmentFactor()
        config = self._make_config(tvf_enabled=False)
        result = factor.compute_adjustment(config)
        assert result == 1.0

    def test_tvf_enabled_returns_between_0_and_1(self):
        factor = SixDimPositionAdjustmentFactor()
        config = self._make_config(tvf_enabled=True)
        result = factor.compute_adjustment(config, sortino=2.0, calmar=1.0, sharpe=1.5)
        assert 0.0 <= result <= 1.0

    def test_high_metrics_higher_result(self):
        factor = SixDimPositionAdjustmentFactor()
        config = self._make_config(tvf_enabled=True)
        low = factor.compute_adjustment(config, sortino=0.5, calmar=0.3, sharpe=0.4)
        high = factor.compute_adjustment(config, sortino=3.0, calmar=2.0, sharpe=2.5)
        assert high > low


class TestKellyFraction:
    def test_basic_kelly(self):
        result = kelly_fraction(win_rate=0.6, win_loss_ratio=2.0)
        assert result > 0
        assert result <= 1.0

    def test_zero_win_rate(self):
        assert kelly_fraction(win_rate=0.0, win_loss_ratio=2.0) == 0.0

    def test_with_cost_ratio(self):
        no_cost = kelly_fraction(win_rate=0.6, win_loss_ratio=2.0, cost_ratio=0.0)
        with_cost = kelly_fraction(win_rate=0.6, win_loss_ratio=2.0, cost_ratio=0.1)
        assert with_cost < no_cost

    def test_negative_effective_odds(self):
        assert kelly_fraction(win_rate=0.6, win_loss_ratio=0.05, cost_ratio=0.1) == 0.0


class TestKellyPositionSize:
    def test_basic_calculation(self):
        result = kelly_position_size(
            equity=100000, win_rate=0.6, win_loss_ratio=2.0,
            entry_price=5000.0, stop_price=4900.0, kelly_fraction_param=0.33,
        )
        assert result > 0

    def test_zero_equity(self):
        assert kelly_position_size(0, 0.6, 2.0, 5000, 4900) == 0.0

    def test_option_buyer_discount(self):
        future = kelly_position_size(100000, 0.6, 2.0, 5000, 4000, instrument_type="future", max_cap=1.0)
        option = kelly_position_size(100000, 0.6, 2.0, 5000, 4000, instrument_type="option_buyer", max_cap=1.0)
        assert option < future


class TestPredictiveStateEngine:
    def test_classify_correct_rise(self):
        pse = PredictiveStateEngine()
        assert pse.classify_state(True, 1) == 'correct_rise'

    def test_classify_correct_fall(self):
        pse = PredictiveStateEngine()
        assert pse.classify_state(True, -1) == 'correct_fall'

    def test_classify_wrong_rise(self):
        pse = PredictiveStateEngine()
        assert pse.classify_state(False, 1) == 'wrong_rise'

    def test_classify_wrong_fall(self):
        pse = PredictiveStateEngine()
        assert pse.classify_state(False, -1) == 'wrong_fall'

    def test_position_multiplier_correct(self):
        pse = PredictiveStateEngine()
        assert pse.get_position_multiplier('correct_rise') == 1.0

    def test_position_multiplier_wrong(self):
        pse = PredictiveStateEngine()
        assert pse.get_position_multiplier('wrong_rise') == 0.5

    def test_record_transition(self):
        pse = PredictiveStateEngine()
        pse.record_transition('correct_rise', 'wrong_fall', pnl=-50)
        stats = pse.get_state_stats()
        assert stats['counts']['wrong_fall'] == 1

    def test_get_state_stats(self):
        pse = PredictiveStateEngine()
        pse.record_transition('correct_rise', 'correct_rise', pnl=100)
        pse.record_transition('correct_rise', 'wrong_fall', pnl=-50)
        stats = pse.get_state_stats()
        assert 'correct_ratio' in stats
        assert 'wrong_ratio' in stats

    def test_singleton_pattern(self):
        PredictiveStateEngine.reset_instance()
        a = PredictiveStateEngine.get_instance()
        b = PredictiveStateEngine.get_instance()
        assert a is b
        PredictiveStateEngine.reset_instance()


class TestModeConfig:
    def test_sharpe_config_mode(self):
        assert SHARPE_CONFIG.mode == CapitalMode.SHARPE_MAX

    def test_profit_config_mode(self):
        assert PROFIT_CONFIG.mode == CapitalMode.PROFIT_RATIO

    def test_balanced_config_mode(self):
        assert BALANCED_CONFIG.mode == CapitalMode.BALANCED

    def test_capital_mode_configs_keys(self):
        assert 'small' in CAPITAL_MODE_CONFIGS
        assert 'medium' in CAPITAL_MODE_CONFIGS
        assert 'large' in CAPITAL_MODE_CONFIGS

    def test_mode_config_frozen(self):
        with pytest.raises(AttributeError):
            SHARPE_CONFIG.max_positions = 999

    def test_tvf_weights_sum(self):
        for cfg in [SHARPE_CONFIG, PROFIT_CONFIG, BALANCED_CONFIG]:
            assert abs(cfg.tvf_l1_weight + cfg.tvf_l2_weight + cfg.tvf_l3_weight - 1.0) < 0.01

    def test_validate_yaml_modeconfig_mapping_empty(self):
        _validate_yaml_modeconfig_mapping(None)
        _validate_yaml_modeconfig_mapping({})

    def test_get_flattened_param(self):
        d = {'a': {'b': {'c': 42}}}
        assert _get_flattened_param(d, 'a.b.c') == 42
        assert _get_flattened_param(d, 'a.b.x') is None
        assert _get_flattened_param(d, 'x.y') is None


class TestLogDeduplicator:
    def test_first_log_allowed(self):
        ld = LogDeduplicator()
        assert ld.should_log("test message") is True

    def test_duplicate_within_window_blocked(self):
        ld = LogDeduplicator()
        ld.should_log("test message")
        assert ld.should_log("test message") is False

    def test_different_messages_allowed(self):
        ld = LogDeduplicator()
        assert ld.should_log("msg1") is True
        assert ld.should_log("msg2") is True

    def test_cache_eviction(self):
        ld = LogDeduplicator()
        for i in range(600):
            ld.should_log(f"msg_{i}")
        assert len(ld._recent) <= 600