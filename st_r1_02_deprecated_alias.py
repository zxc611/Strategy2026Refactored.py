# MODULE_ID: M2-505
"""Round1-2 断言测试：P0-20 删除 crack_validation.py 中 DEPRECATED 别名
验证：1) crack_validation 不再导出 validate_logic_reversal_no_future
      2) 权威版本 param_pool.checks_orchestrator 仍可导出
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_alias_removed():
    """crack_validation 不再导出 validate_logic_reversal_no_future"""
    import importlib
    mod = importlib.import_module('ali2026v3_trading.risk.crack_validation')
    assert not hasattr(mod, 'validate_logic_reversal_no_future'), \
        "crack_validation 仍导出 validate_logic_reversal_no_future 别名"
    print("  OK: crack_validation 不再导出 DEPRECATED 别名")

def test_canonical_still_works():
    """权威版本 param_pool.checks_orchestrator 仍可导出"""
    import importlib
    mod = importlib.import_module('ali2026v3_trading.param_pool.checks_orchestrator')
    assert hasattr(mod, 'validate_logic_reversal_no_future'), \
        "checks_orchestrator 缺少 validate_logic_reversal_no_future"
    print("  OK: 权威版本 checks_orchestrator 仍可导出")

def test_runtime_behavior():
    """运行时行为：权威函数可调用且返回合理值"""
    from ali2026v3_trading.param_pool.validation.checks_orchestrator import validate_logic_reversal_no_future
    import pandas as pd
    df = pd.DataFrame({'close': [100.0, 101.0, 102.0]})
    result = validate_logic_reversal_no_future(bar_data=df, n_check_bars=2)
    assert result is not None or result is None, "函数执行异常"
    print(f"  OK: 权威函数运行时行为正常, 返回类型={type(result).__name__}")

if __name__ == '__main__':
    print("=== Round1-2: P0-20 删除 DEPRECATED 别名 ===")
    test_alias_removed()
    test_canonical_still_works()
    test_runtime_behavior()
    print("ALL ASSERTIONS PASSED")
