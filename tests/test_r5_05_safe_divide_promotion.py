# MODULE_ID: M2-545
"""R5-5断言测试: P1-32 safe_divide推广3处残留

验证3个文件使用safe_divide替代手动除法保护:
1. order_flow_analyzer.py:541 — epsilon除法→safe_divide(min_denominator=1e-6)
2. governance_engine.py:415 — 条件除法→safe_divide(min_denominator=1e-10)
3. backtest_state.py:680 — 条件除法→safe_divide
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


def test_order_flow_analyzer_no_manual_epsilon_division():
    """order_flow_analyzer.py不应有手动epsilon除法保护"""
    src = _read('order/order_flow_analyzer.py')
    assert 'abs(start_cvd) + 1e-6' not in src, "order_flow_analyzer.py不应有手动epsilon除法"


def test_order_flow_analyzer_uses_safe_divide_cvd():
    """order_flow_analyzer.py的CVD计算应使用safe_divide"""
    src = _read('order/order_flow_analyzer.py')
    assert 'safe_divide(end_cvd - start_cvd, abs(start_cvd)' in src, "CVD计算应使用safe_divide"


def test_governance_engine_no_conditional_division():
    """governance_engine.py不应有条件除法residual / denominator"""
    src = _read('governance/governance_engine.py')
    assert 'residual / denominator * 100' not in src, "governance_engine.py不应有residual / denominator * 100"


def test_governance_engine_uses_safe_divide_residual():
    """governance_engine.py的残差百分比应使用safe_divide"""
    src = _read('governance/governance_engine.py')
    assert 'safe_divide(residual, denominator' in src, "残差百分比应使用safe_divide"


def test_backtest_state_uses_safe_divide():
    """backtest_state.py应导入并使用safe_divide"""
    src = _read('param_pool/backtest/backtest_state.py')
    assert 'from ali2026v3_trading.infra.resilience import safe_divide' in src, "backtest_state.py应导入safe_divide"
    assert 'safe_divide(numerator, denominator' in src, "backtest_state.py应使用safe_divide"


def test_safe_divide_semantic_equivalence_epsilon():
    """验证safe_divide(min_denominator=1e-6)与原epsilon逻辑语义等价（正常情况）

    行为变更说明：当start_cvd=0时，原逻辑产生极大值(end/1e-6)，
    safe_divide返回0.0更安全，这是有意的改进。
    """
    from ali2026v3_trading.infra.resilience import safe_divide

    # 正常情况应一致
    start, end = 100.0, 110.0
    original = (end - start) / (abs(start) + 1e-6)
    new = safe_divide(end - start, abs(start), default=0.0, min_denominator=1e-6)
    assert abs(original - new) < 1e-8, f"正常情况不一致: {original} != {new}"

    # start_cvd=0时，safe_divide返回0（更安全的行为）
    start, end = 0.0, 10.0
    new = safe_divide(end - start, abs(start), default=0.0, min_denominator=1e-6)
    assert new == 0.0, f"start_cvd=0时应返回0: {new}"


def test_safe_divide_semantic_equivalence_governance():
    """验证safe_divide(min_denominator=1e-10)与原条件除法语义等价"""
    from ali2026v3_trading.infra.resilience import safe_divide

    # 原逻辑: abs(residual / denominator * 100) if abs(denominator) > 1e-10 else 0.0
    # 新逻辑: abs(safe_divide(residual, denominator, default=0.0, min_denominator=1e-10)) * 100
    residual, denominator = 5.0, 100.0
    original = abs(residual / denominator * 100) if abs(denominator) > 1e-10 else 0.0
    new = abs(safe_divide(residual, denominator, default=0.0, min_denominator=1e-10)) * 100
    assert abs(original - new) < 1e-10, f"残差百分比不一致: {original} != {new}"

    # denominator接近0时
    residual, denominator = 5.0, 1e-12
    original = abs(residual / denominator * 100) if abs(denominator) > 1e-10 else 0.0
    new = abs(safe_divide(residual, denominator, default=0.0, min_denominator=1e-10)) * 100
    assert new == 0.0, f"接近0时应返回0: {new}"
    assert original == 0.0, f"原逻辑也应返回0: {original}"
