"""End-to-end integration test: Step1 → Step1.5 → Deep Validation → Step2 smoke"""
import sys
import os
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.param_pool.task_scheduler import (
    optimize_l2_params_step1,
    run_step2_smoke_test,
    run_deep_validation_tiered,
    compute_alpha_confidence_interval,
    check_l2_conflict,
    DEEP_VALIDATION_TIERS,
    L2_PARAM_GRID,
)


def _make_synthetic_bar_data(n=3000, seed=42):
    np.random.seed(seed)
    dates = pd.date_range('2025-01-01 09:30', periods=n, freq='1min')
    close = 4000.0 + np.cumsum(np.random.randn(n) * 2)
    high = close + np.abs(np.random.randn(n) * 1.5)
    low = close - np.abs(np.random.randn(n) * 1.5)
    open_ = close + np.random.randn(n) * 0.5
    volume = np.random.randint(100, 1000, n).astype(float)

    correct_rise_pct = np.zeros(n)
    correct_fall_pct = np.zeros(n)
    wrong_rise_pct = np.zeros(n)
    wrong_fall_pct = np.zeros(n)

    for i in range(n):
        seg = i // 500
        if seg % 3 == 0:
            correct_rise_pct[i] = np.random.uniform(0.3, 0.5)
            correct_fall_pct[i] = np.random.uniform(0.1, 0.2)
            wrong_rise_pct[i] = np.random.uniform(0.05, 0.15)
            wrong_fall_pct[i] = np.random.uniform(0.05, 0.1)
        elif seg % 3 == 1:
            correct_rise_pct[i] = np.random.uniform(0.05, 0.1)
            correct_fall_pct[i] = np.random.uniform(0.05, 0.15)
            wrong_rise_pct[i] = np.random.uniform(0.3, 0.5)
            wrong_fall_pct[i] = np.random.uniform(0.1, 0.2)
        else:
            correct_rise_pct[i] = np.random.uniform(0.05, 0.15)
            correct_fall_pct[i] = np.random.uniform(0.05, 0.15)
            wrong_rise_pct[i] = np.random.uniform(0.05, 0.15)
            wrong_fall_pct[i] = np.random.uniform(0.05, 0.15)

    return pd.DataFrame({
        'datetime': dates, 'minute': dates, 'open': open_, 'high': high,
        'low': low, 'close': close, 'volume': volume,
        'correct_rise_pct': correct_rise_pct, 'correct_fall_pct': correct_fall_pct,
        'wrong_rise_pct': wrong_rise_pct, 'wrong_fall_pct': wrong_fall_pct,
        'symbol': 'IF2605'
    })


def test_full_pipeline():
    bar_data = _make_synthetic_bar_data()

    # Step1: L-2 parameter optimization
    print("=" * 80)
    print("Step1: L-2 Parameter Optimization")
    print("=" * 80)
    opt = optimize_l2_params_step1(bar_data, lookahead_bars=10, min_accuracy=0.3, min_transitions=50)
    print(f"  best_params: {opt['best_params']}")
    print(f"  best_accuracy: {opt['best_accuracy']:.4f}")
    print(f"  qualified: {opt['qualified']}")
    print(f"  qualified_count: {opt['qualified_count']}/{opt['total_combos']}")
    assert opt['qualified'], "Step1 should find qualified L-2 params"
    l2_best = opt['best_params']

    # Step1.5: Smoke test
    print("\n" + "=" * 80)
    print("Step1.5: Smoke Test")
    print("=" * 80)
    smoke = run_step2_smoke_test(l2_best, bar_data, train=True, min_state_transitions=3)
    print(f"  passed: {smoke['passed']}")
    print(f"  states_seen: {smoke['states_seen']}")
    print(f"  state_transitions: {smoke['state_transitions']}")
    assert smoke['passed'], "Step1.5 smoke test should pass with synthetic data"

    # Alpha CI validation
    print("\n" + "=" * 80)
    print("Alpha Confidence Interval Validation")
    print("=" * 80)
    for name, sharpe, n_sig in [('S1_HFT', 2.0, 200), ('S2_Box', 1.5, 100), ('S3_Ext', 0.8, 50), ('S4_Spr', 1.2, 30)]:
        ci = compute_alpha_confidence_interval(0.15, sharpe, n_sig, 0.95)
        print(f"  {name}: Sharpe={sharpe}, n={n_sig} → CI=[{ci['sharpe_ci_lower']:.3f}, {ci['sharpe_ci_upper']:.3f}], action={ci['action']}")

    # L-2 Conflict resolution
    print("\n" + "=" * 80)
    print("L-2 Conflict Resolution")
    print("=" * 80)
    no_conflict = check_l2_conflict(l2_best, l2_best, tolerance=0.1)
    print(f"  Same params: any_conflict={no_conflict['any_conflict']}, action={no_conflict['action']}")
    assert not no_conflict['any_conflict'], "Same params should have no conflict"

    # Deep validation must_run tier
    print("\n" + "=" * 80)
    print("Deep Validation: must_run tier")
    print("=" * 80)
    params = l2_best.copy()
    params.update({'close_take_profit_ratio': 3.0, 'close_stop_loss_ratio': 1.5})
    val = run_deep_validation_tiered('must_run', params, params, params, params, bar_data, train=True)
    print(f"  tier: must_run")
    print(f"  tests_run: {val.get('tests_run', [])}")
    for test_name, test_result in val.get('results', {}).items():
        status = test_result.get('passed', 'N/A') if isinstance(test_result, dict) else 'N/A'
        print(f"    {test_name}: passed={status}")

    print("\n" + "=" * 80)
    print("ALL PIPELINE STAGES COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    test_full_pipeline()


class TestTradingCoreChainE2E:
    """R17-P0-TEST-E2E: 交易核心链路E2E测试 — 信号→风控→下单→持仓"""

    def test_signal_service_generates_and_rejects_empty(self):
        from ali2026v3_trading.signal_service import SignalService
        svc = SignalService.__new__(SignalService)
        svc._cooldown_times = {}
        svc._cooldown_durations = {}
        svc._signal_dedup_cache = {}
        svc._signal_history = []
        svc._min_estimated_plr = 0.0
        svc._mode_engine = None
        svc._hft_filter = None
        svc._adaptive_threshold = None
        result = svc.generate_signal('', 'BUY', 100.0, 1)
        assert result is None

    def test_risk_check_response_block_and_pass(self):
        from ali2026v3_trading.risk_service import RiskCheckResponse, RiskLevel
        block_resp = RiskCheckResponse.block_result('TEST', 'test block', RiskLevel.HIGH)
        assert block_resp.is_block is True
        assert block_resp.is_pass is False
        pass_resp = RiskCheckResponse.pass_result('test pass')
        assert pass_resp.is_pass is True
        assert pass_resp.is_block is False

    def test_order_result_ok_and_fail(self):
        from ali2026v3_trading.order_service import OrderResult
        ok = OrderResult.ok('ORD_001')
        assert ok.success is True
        assert bool(ok) is True
        fail = OrderResult.fail('ERR_TIMEOUT', 'order timed out')
        assert fail.success is False
        assert bool(fail) is False

    def test_position_tp_sl_ratios_consistent_chain(self):
        from ali2026v3_trading.position_service import PositionService
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        svc = PositionService.__new__(PositionService)
        svc._state_param_manager = None
        tp, sl = svc._get_tp_sl_ratios_by_reason('CORRECT_RESONANCE')
        assert tp > 0 and sl > 0
        assert tp > sl
        assert DEFAULT_PARAM_TABLE['close_take_profit_ratio'] > 0
        assert DEFAULT_PARAM_TABLE['close_stop_loss_ratio'] > 0

    def test_e7_checker_integrated_with_greeks_config(self):
        from ali2026v3_trading.governance_engine import E7UnexplainedReturnChecker
        from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
        assert 'max_net_delta_pct' in DEFAULT_PARAM_TABLE
        assert 'max_net_gamma_pct' in DEFAULT_PARAM_TABLE
        assert 'max_net_vega_bps' in DEFAULT_PARAM_TABLE
        checker = E7UnexplainedReturnChecker.__new__(E7UnexplainedReturnChecker)
        checker._residual_threshold_pct = 15.0
        pnl_attribution = {
            'unexplained': 50.0,
            'delta_contrib': 30.0,
            'gamma_contrib': 10.0,
            'vega_contrib': 5.0,
            'theta_contrib': 5.0,
        }
        result = checker.check(pnl_attribution)
        assert 'e7_triggered' in result
        assert 'residual_pct' in result
