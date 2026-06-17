# MODULE_ID: M2-533
"""R4-01: commission / sharpe / multiscale 命名统一断言测试

验证 F-11 / F-12 / F-13 三组修复:
1. _compute_commission 从 backtest_state 可导入，与 commission_utils.compute_commission 是同一对象
2. statistical_validity_extensions 不再有 _compute_sharpe_simple 本地定义（检查源码）
3. validate_multiscale_indicator_consistency 从 validation_hmm_state 可导入
4. compute_commission 运行时可调用（用最小参数）
5. compute_sharpe_stable 运行时可调用
"""
from __future__ import annotations

import inspect
import textwrap


def test_f11_compute_commission_same_object():
    """F-11: backtest_state._compute_commission 与 commission_utils.compute_commission 是同一对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_state import _compute_commission
    from ali2026v3_trading.infra.commission_utils import compute_commission

    assert _compute_commission is compute_commission, (
        "_compute_commission 应该是 commission_utils.compute_commission 的别名（同一对象）"
    )


def test_f12_no_local_sharpe_definition():
    """F-12: statistical_validity_extensions 不再有 _compute_sharpe_simple 本地定义（检查源码）"""
    from ali2026v3_trading.strategy_judgment import statistical_validity_extensions as mod

    source = inspect.getsource(mod)
    # 不应包含独立的 def _compute_sharpe_simple ... 函数体（含 mean_r / std_r 计算逻辑）
    # 委托版只包含 compute_sharpe_stable 调用
    assert "mean_r = np.mean(r) - rf_period" not in source, (
        "_compute_sharpe_simple 不应包含本地 mean_r/std_r 计算逻辑，应委托 compute_sharpe_stable"
    )
    assert "std_r = np.std(r, ddof=1)" not in source, (
        "_compute_sharpe_simple 不应包含本地 std_r 计算，应委托 compute_sharpe_stable"
    )


def test_f13_multiscale_importable():
    """F-13: validate_multiscale_indicator_consistency 从 validation_deep_orchestrator 可导入"""
    from ali2026v3_trading.param_pool.validation.validation_deep_orchestrator import (
        validate_multiscale_indicator_consistency,
    )
    from ali2026v3_trading.risk.crack_validation import (
        validate_indicator_multiscale_consistency,
    )

    assert validate_multiscale_indicator_consistency is validate_indicator_multiscale_consistency, (
        "validate_multiscale_indicator_consistency 应该是 crack_validation.validate_indicator_multiscale_consistency 的别名"
    )


def test_compute_commission_callable():
    """F-11 补充: compute_commission 运行时可调用（用最小参数）"""
    from ali2026v3_trading.infra.commission_utils import compute_commission

    result = compute_commission(symbol="IO2506", lots=1, is_open=True)
    assert isinstance(result, float), f"compute_commission 应返回 float，实际返回 {type(result)}"
    assert result >= 0, f"手续费应非负，实际 {result}"


def test_compute_sharpe_stable_callable():
    """F-12 补充: compute_sharpe_stable 运行时可调用"""
    from ali2026v3_trading.infra.resilience import compute_sharpe_stable

    result = compute_sharpe_stable([0.01, -0.005, 0.02, 0.003, -0.001])
    assert isinstance(result, float), f"compute_sharpe_stable 应返回 float，实际返回 {type(result)}"
