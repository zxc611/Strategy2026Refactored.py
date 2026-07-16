# MODULE_ID: M2-HYP
"""
Hypothesis属性测试：对核心纯函数进行基于属性的随机测试

覆盖模块：
- risk/crack_validation.py: tick/lot取整、级联滑点、离散资金、KL散度、正态CDF、Sharpe CI
- infra/resilience.py: EWMA、safe_divide、safe_float_to_int、deterministic_round、权重归一化
- infra/commission_utils.py: 简单佣金估算
- risk/crack_validation.py: 断路器/时间止损优先级
"""
import math

import numpy as np
import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st


# ============================================================
# 策略定义
# ============================================================

finite_float = st.floats(min_value=-1e8, max_value=1e8, allow_nan=False, allow_infinity=False)
positive_float = st.floats(min_value=1e-9, max_value=1e8, allow_nan=False, allow_infinity=False)
nonneg_float = st.floats(min_value=0.0, max_value=1e8, allow_nan=False, allow_infinity=False)
small_float = st.floats(min_value=-1e4, max_value=1e4, allow_nan=False, allow_infinity=False)
alpha_float = st.floats(min_value=-0.5, max_value=1.5, allow_nan=False, allow_infinity=False)
direction_str = st.sampled_from(['nearest', 'up', 'down'])
lot_direction_str = st.sampled_from(['floor', 'ceil'])


# ============================================================
# apply_tick_rounding
# ============================================================

class TestApplyTickRounding:
    """apply_tick_rounding 属性测试"""

    @given(price=small_float, tick_size=positive_float, direction=direction_str)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_idempotent(self, price, tick_size, direction):
        from risk.crack_validation import apply_tick_rounding
        r1 = apply_tick_rounding(price, tick_size, direction)
        r2 = apply_tick_rounding(r1, tick_size, direction)
        assert math.isclose(r1, r2, rel_tol=1e-6, abs_tol=tick_size)

    @given(price=small_float, tick_size=positive_float)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_up_gte_price(self, price, tick_size):
        from risk.crack_validation import apply_tick_rounding
        result = apply_tick_rounding(price, tick_size, 'up')
        assert result >= price - tick_size * 1e-9

    @given(price=small_float, tick_size=positive_float)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_down_lte_price(self, price, tick_size):
        from risk.crack_validation import apply_tick_rounding
        result = apply_tick_rounding(price, tick_size, 'down')
        assert result <= price + tick_size * 1e-9

    @given(price=small_float, tick_size=positive_float, direction=direction_str)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_tick_size_multiple(self, price, tick_size, direction):
        from risk.crack_validation import apply_tick_rounding
        result = apply_tick_rounding(price, tick_size, direction)
        remainder = result / tick_size
        assert math.isclose(remainder, round(remainder), rel_tol=1e-6, abs_tol=1e-9)


# ============================================================
# apply_lot_rounding
# ============================================================

class TestApplyLotRounding:
    """apply_lot_rounding 属性测试"""

    @given(lots=st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_floor_nonneg(self, lots):
        from risk.crack_validation import apply_lot_rounding
        result = apply_lot_rounding(lots, 'floor')
        assert result >= 0

    @given(lots=st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_ceil_at_least_one(self, lots):
        assume(lots > 0)
        from risk.crack_validation import apply_lot_rounding
        result = apply_lot_rounding(lots, 'ceil')
        assert result >= 1

    @given(lots=st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_floor_le_ceil(self, lots):
        from risk.crack_validation import apply_lot_rounding
        f = apply_lot_rounding(lots, 'floor')
        c = apply_lot_rounding(lots, 'ceil')
        assert f <= c


# ============================================================
# compute_cascade_slippage
# ============================================================

class TestComputeCascadeSlippage:
    """compute_cascade_slippage 属性测试"""

    @given(base=nonneg_float, close_vol=nonneg_float, avg_vol=nonneg_float,
           mult=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
           cap=st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_result_bounded(self, base, close_vol, avg_vol, mult, cap):
        from risk.crack_validation import compute_cascade_slippage
        result = compute_cascade_slippage(base, close_vol, avg_vol, mult, cap)
        assert 0.0 <= result <= cap

    @given(base=nonneg_float, avg_vol=positive_float,
           mult=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
           cap=st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_monotonic_in_close_vol(self, base, avg_vol, mult, cap):
        from risk.crack_validation import compute_cascade_slippage
        r1 = compute_cascade_slippage(base, 100.0, avg_vol, mult, cap)
        r2 = compute_cascade_slippage(base, 10000.0, avg_vol, mult, cap)
        assert r2 >= r1 - 1e-9


# ============================================================
# compute_discrete_equity
# ============================================================

class TestComputeDiscreteEquity:
    """compute_discrete_equity 属性测试"""

    @given(capital=nonneg_float,
           losses=st.lists(nonneg_float, min_size=0, max_size=20),
           gains=st.lists(nonneg_float, min_size=0, max_size=20))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_length_and_first(self, capital, losses, gains):
        from risk.crack_validation import compute_discrete_equity
        result = compute_discrete_equity(capital, losses, gains)
        n = min(len(losses), len(gains))
        assert len(result) == 1 + n
        assert math.isclose(result[0], capital, rel_tol=1e-9)

    @given(capital=nonneg_float,
           losses=st.lists(nonneg_float, min_size=0, max_size=20),
           gains=st.lists(nonneg_float, min_size=0, max_size=20))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_nonneg(self, capital, losses, gains):
        from risk.crack_validation import compute_discrete_equity
        result = compute_discrete_equity(capital, losses, gains)
        assert all(e >= -1e-9 for e in result)


# ============================================================
# compute_kl_divergence
# ============================================================

class TestComputeKLDivergence:
    """compute_kl_divergence 属性测试"""

    @given(p=st.lists(st.floats(min_value=0.01, max_value=100.0, allow_nan=False), min_size=1, max_size=10))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_nonneg_same_length(self, p):
        from risk.crack_validation import compute_kl_divergence
        q = [v * 2.0 + 0.1 for v in p]
        result = compute_kl_divergence(np.array(p), np.array(q))
        assert result >= -1e-6

    @given(p=st.lists(st.floats(min_value=0.01, max_value=100.0, allow_nan=False), min_size=1, max_size=10))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_self_divergence_zero(self, p):
        from risk.crack_validation import compute_kl_divergence
        arr = np.array(p)
        result = compute_kl_divergence(arr, arr)
        assert math.isclose(result, 0.0, abs_tol=1e-6)


# ============================================================
# _normal_cdf
# ============================================================

class TestNormalCDF:
    """_normal_cdf 属性测试"""

    @given(x=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_range_0_1(self, x):
        from risk.crack_validation import _normal_cdf
        result = _normal_cdf(x)
        assert 0.0 <= result <= 1.0

    @given(x=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_symmetry(self, x):
        from risk.crack_validation import _normal_cdf
        assert math.isclose(_normal_cdf(x), 1.0 - _normal_cdf(-x), rel_tol=1e-6, abs_tol=1e-9)

    def test_zero_is_half(self):
        from risk.crack_validation import _normal_cdf
        assert math.isclose(_normal_cdf(0.0), 0.5, abs_tol=1e-10)


# ============================================================
# compute_sharpe_ci
# ============================================================

class TestComputeSharpeCI:
    """compute_sharpe_ci 属性测试"""

    @given(sharpe=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False),
           n=st.integers(min_value=1, max_value=10000),
           z=st.floats(min_value=0.1, max_value=5.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_ci_contains_sharpe(self, sharpe, n, z):
        from risk.crack_validation import compute_sharpe_ci
        low, high = compute_sharpe_ci(sharpe, n, z)
        assert low <= sharpe + 1e-9
        assert high >= sharpe - 1e-9

    @given(sharpe=st.floats(min_value=-10.0, max_value=10.0, allow_nan=False),
           n=st.integers(min_value=1, max_value=10000),
           z=st.floats(min_value=0.1, max_value=5.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_symmetric(self, sharpe, n, z):
        from risk.crack_validation import compute_sharpe_ci
        low, high = compute_sharpe_ci(sharpe, n, z)
        assert math.isclose(sharpe - low, high - sharpe, rel_tol=1e-9)


# ============================================================
# stable_ewma
# ============================================================

class TestStableEWMA:
    """stable_ewma 属性测试"""

    @given(c=finite_float, n=finite_float)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_alpha_zero_returns_current(self, c, n):
        from infra.resilience import stable_ewma
        result = stable_ewma(c, n, 0.0)
        assert math.isclose(result, c, rel_tol=1e-3, abs_tol=abs(n - c) * 1e-10 + 1e-6)

    @given(c=finite_float, n=finite_float)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_alpha_one_returns_new(self, c, n):
        from infra.resilience import stable_ewma
        result = stable_ewma(c, n, 1.0)
        assert math.isclose(result, n, rel_tol=1e-6, abs_tol=1e-6)

    @given(c=finite_float, n=finite_float)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_alpha_half_is_average(self, c, n):
        from infra.resilience import stable_ewma
        result = stable_ewma(c, n, 0.5)
        expected = (c + n) / 2.0
        assert math.isclose(result, expected, rel_tol=1e-6, abs_tol=1e-6)


# ============================================================
# safe_divide
# ============================================================

class TestSafeDivide:
    """safe_divide 属性测试"""

    @given(numerator=finite_float, default=finite_float)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_zero_denom_returns_default(self, numerator, default):
        from infra.resilience import safe_divide
        result = safe_divide(numerator, 0.0, default)
        assert result == default

    @given(numerator=finite_float, denominator=st.floats(min_value=1.0, max_value=1e8, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_large_denom_equals_division(self, numerator, denominator):
        from infra.resilience import safe_divide
        result = safe_divide(numerator, denominator)
        expected = numerator / denominator
        assert math.isclose(result, expected, rel_tol=1e-9)


# ============================================================
# safe_float_to_int
# ============================================================

class TestSafeFloatToInt:
    """safe_float_to_int 属性测试"""

    @given(n=st.integers(min_value=-1000000, max_value=1000000))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_integer_input_unchanged(self, n):
        from infra.resilience import safe_float_to_int
        result = safe_float_to_int(float(n))
        assert result == n

    def test_half_rounds_up(self):
        from infra.resilience import safe_float_to_int
        assert safe_float_to_int(0.5) == 1

    def test_neg_half_rounds_to_zero(self):
        from infra.resilience import safe_float_to_int
        assert safe_float_to_int(-0.5) == 0


# ============================================================
# deterministic_round
# ============================================================

class TestDeterministicRound:
    """deterministic_round 属性测试"""

    @given(value=small_float, ndigits=st.integers(min_value=-4, max_value=4))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_deterministic(self, value, ndigits):
        from infra.resilience import deterministic_round
        r1 = deterministic_round(value, ndigits)
        r2 = deterministic_round(value, ndigits)
        assert r1 == r2

    def test_half_rounds_up(self):
        from infra.resilience import deterministic_round
        assert deterministic_round(0.5) == 1.0
        assert deterministic_round(1.5) == 2.0


# ============================================================
# safe_normalize_weights
# ============================================================

class TestSafeNormalizeWeights:
    """safe_normalize_weights 属性测试"""

    @given(weights=st.dictionaries(
        st.text(min_size=1, max_size=5, alphabet='abc'),
        st.floats(min_value=0.01, max_value=100.0, allow_nan=False, allow_infinity=False),
        min_size=1, max_size=8))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_sums_to_one(self, weights):
        from infra.resilience import safe_normalize_weights
        result = safe_normalize_weights(weights)
        assert math.isclose(sum(result.values()), 1.0, rel_tol=1e-6)

    @given(weights=st.dictionaries(
        st.text(min_size=1, max_size=5, alphabet='abc'),
        st.floats(min_value=0.01, max_value=100.0, allow_nan=False, allow_infinity=False),
        min_size=1, max_size=8))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_keys_preserved(self, weights):
        from infra.resilience import safe_normalize_weights
        result = safe_normalize_weights(weights)
        assert result.keys() == weights.keys()


# ============================================================
# estimate_commission_simple
# ============================================================

class TestEstimateCommissionSimple:
    """estimate_commission_simple 属性测试"""

    @given(price=finite_float, quantity=st.integers(min_value=-10000, max_value=10000),
           rate=st.floats(min_value=0.0, max_value=0.01, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_nonneg(self, price, quantity, rate):
        from infra.commission_utils import estimate_commission_simple
        result = estimate_commission_simple(price, quantity, rate)
        assert result >= -1e-9

    @given(price=positive_float, quantity=st.integers(min_value=1, max_value=10000))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_zero_rate_is_zero(self, price, quantity):
        from infra.commission_utils import estimate_commission_simple
        result = estimate_commission_simple(price, quantity, 0.0)
        assert result == 0.0

    @given(price=positive_float, quantity=st.integers(min_value=1, max_value=10000),
           rate=st.floats(min_value=1e-6, max_value=0.01, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_linear_in_quantity(self, price, quantity, rate):
        assume(quantity > 0 and quantity * 2 <= 10000)
        from infra.commission_utils import estimate_commission_simple
        r1 = estimate_commission_simple(price, quantity, rate)
        r2 = estimate_commission_simple(price, quantity * 2, rate)
        assert math.isclose(r2, 2 * r1, rel_tol=1e-9)


# ============================================================
# check_safety_meta_layer
# ============================================================

class TestCheckSafetyMetaLayer:
    """check_safety_meta_layer 属性测试"""

    @given(cb=st.booleans(), ts=st.booleans())
    @settings(max_examples=50)
    def test_can_close_always_true(self, cb, ts):
        from risk.crack_validation import check_safety_meta_layer
        _, can_close = check_safety_meta_layer(cb, ts)
        assert can_close is True

    @given(cb=st.booleans(), ts=st.booleans())
    @settings(max_examples=50)
    def test_any_trigger_blocks_open(self, cb, ts):
        from risk.crack_validation import check_safety_meta_layer
        can_open, _ = check_safety_meta_layer(cb, ts)
        if cb or ts:
            assert can_open is False
        else:
            assert can_open is True