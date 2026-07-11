# MODULE_ID: M2-434
"""P0-20断言测试: compute_cascade_slippage使用sqrt冲击模型，与回测版对齐"""
import sys
sys.path.insert(0, '..')

import math
from ali2026v3_trading.risk.crack_validation import compute_cascade_slippage


def test_cascade_uses_sqrt_not_linear():
    base = 10.0
    close_vol = 100.0
    avg_vol = 1000.0
    multiplier = 1.5
    participation = close_vol / avg_vol
    expected_impact = 1.0 + math.sqrt(participation)
    expected = min(base * expected_impact * multiplier, 50.0)
    result = compute_cascade_slippage(base, close_vol, avg_vol, cascade_multiplier=multiplier)
    assert abs(result - expected) < 1e-10, f"expected {expected}, got {result}"


def test_cascade_sqrt_vs_linear_differs():
    base = 10.0
    close_vol = 100.0
    avg_vol = 1000.0
    multiplier = 1.5
    participation = close_vol / avg_vol
    sqrt_result = compute_cascade_slippage(base, close_vol, avg_vol, cascade_multiplier=multiplier)
    linear_impact = 1.0 + participation
    linear_result = min(base * linear_impact * multiplier, 50.0)
    assert sqrt_result != linear_result, "sqrt and linear should differ for participation>0"


def test_cascade_zero_volume():
    result = compute_cascade_slippage(10.0, 0.0, 1000.0)
    assert result == 10.0 * 1.0 * 1.5, f"expected {10.0*1.5}, got {result}"


def test_cascade_zero_avg_volume():
    result = compute_cascade_slippage(10.0, 100.0, 0.0)
    assert result == 10.0 * 1.0 * 1.5, f"expected {10.0*1.5}, got {result}"


def test_cascade_respects_cap():
    result = compute_cascade_slippage(100.0, 500.0, 100.0, cascade_cap_bps=50.0)
    assert result == 50.0, f"expected cap 50.0, got {result}"


def test_cascade_backtest_alignment():
    from ali2026v3_trading.param_pool.backtest.backtest_state import _compute_cascade_slippage_bps, CASCADE_SLIPPAGE_MULTIPLIER, CASCADE_SLIPPAGE_CAP_BPS
    base = 10.0
    close_vol = 50.0
    avg_vol = 500.0
    crack_result = compute_cascade_slippage(base, close_vol, avg_vol, cascade_multiplier=CASCADE_SLIPPAGE_MULTIPLIER, cascade_cap_bps=CASCADE_SLIPPAGE_CAP_BPS)
    bt_result = _compute_cascade_slippage_bps(base_slippage_bps=base, close_volume=close_vol, avg_volume=avg_vol, is_state_switch=True)
    assert abs(crack_result - bt_result) < 1e-10, f"crack={crack_result}, bt={bt_result}"


if __name__ == '__main__':
    import traceback
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            try:
                fn()
                print(f'  PASS: {name}')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                failed += 1
                print(f'  FAIL: {name}')
                traceback.print_exc()
    total = len([n for n in globals() if n.startswith("test_")])
    print(f'\n{total - failed}/{total} passed')
    sys.exit(failed)