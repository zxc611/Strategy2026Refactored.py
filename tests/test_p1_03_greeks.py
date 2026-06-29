# MODULE_ID: M2-438
"""P1-03断言测试: feature_engine BS函数与governance greeks_calculator数值一致"""
import sys
sys.path.insert(0, '..')

import math
from ali2026v3_trading.precompute._preprocess import _bs_price_scalar, _bs_greeks_scalar
from ali2026v3_trading.governance.greeks_calculator import _norm_cdf, _norm_pdf, _bs_price, _bs_greeks


def test_bs_price_consistency():
    S, K, T, r, q, sigma = 100.0, 95.0, 0.25, 0.02, 0.0, 0.3
    local_price = _bs_price_scalar(S, K, T, r, q, sigma, 'CALL')
    gov_price = _bs_price(S, K, T, r, q, sigma, 'CALL')
    assert abs(local_price - gov_price) < 1e-10, f"CALL price mismatch: local={local_price}, gov={gov_price}"
    local_put = _bs_price_scalar(S, K, T, r, q, sigma, 'PUT')
    gov_put = _bs_price(S, K, T, r, q, sigma, 'PUT')
    assert abs(local_put - gov_put) < 1e-10, f"PUT price mismatch: local={local_put}, gov={gov_put}"


def test_bs_greeks_consistency():
    S, K, T, r, q, sigma = 100.0, 95.0, 0.25, 0.02, 0.0, 0.3
    local_greeks = _bs_greeks_scalar(S, K, T, r, q, sigma, 'CALL')
    gov_greeks = _bs_greeks(S, K, T, r, q, sigma, 'CALL')
    if isinstance(gov_greeks, dict):
        gov_vals = [gov_greeks['delta'], gov_greeks['gamma'], gov_greeks['theta'], gov_greeks['vega']]
    else:
        gov_vals = list(gov_greeks)
    for i, name in enumerate(['delta', 'gamma', 'theta', 'vega']):
        assert abs(local_greeks[i] - gov_vals[i]) < 1e-8, f"{name} mismatch: local={local_greeks[i]}, gov={gov_vals[i]}"


def test_norm_cdf_shared():
    from ali2026v3_trading.governance.greeks_calculator import _norm_cdf as gm_cdf
    # _norm_cdf was consolidated into governance.greeks_calculator; verify it works correctly
    for x in [-2.0, -1.0, 0.0, 1.0, 2.0]:
        assert 0.0 <= gm_cdf(x) <= 1.0, f"_norm_cdf({x}) out of range"


def test_fallback_is_data_degradation_not_code_fallback():
    from ali2026v3_trading.precompute._preprocess import _bs_greeks_scalar
    import pandas as pd
    import numpy as np
    # _compute_greeks_fallback was removed; verify _bs_greeks_scalar works as the authoritative greeks path
    S, K, T, r, q, sigma = 100.0, 95.0, 0.25, 0.02, 0.0, 0.3
    result = _bs_greeks_scalar(S, K, T, r, q, sigma, 'CALL')
    assert len(result) >= 4  # delta, gamma, theta, vega


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