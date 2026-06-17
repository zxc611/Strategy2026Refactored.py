#!/usr/bin/env python3
# MODULE_ID: M2-394
"""评判关键组件失败阻断测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_block_policy_exists():
    from ali2026v3_trading.strategy_judgment._judgment_services import ComponentFailurePolicy
    assert hasattr(ComponentFailurePolicy, 'BLOCK')

def test_governance_is_block():
    from ali2026v3_trading.strategy_judgment._judgment_services import CRITICAL_COMPONENTS, ComponentFailurePolicy
    assert CRITICAL_COMPONENTS.get("E-04_governance") == ComponentFailurePolicy.BLOCK

def test_violation_tracker_is_block():
    from ali2026v3_trading.strategy_judgment._judgment_services import CRITICAL_COMPONENTS, ComponentFailurePolicy
    assert CRITICAL_COMPONENTS.get("E-11_violation_tracker") == ComponentFailurePolicy.BLOCK

def test_block_returns_false():
    """BLOCK策略的组件失败应返回False(阻断)"""
    from ali2026v3_trading.strategy_judgment._judgment_services import _handle_component_failure
    result = _handle_component_failure("E-04_governance", RuntimeError("test"))
    assert result == False, f"BLOCK should return False, got {result}"

def test_degrade_returns_true():
    """DEGRADE策略的组件失败应返回True(降级继续)"""
    from ali2026v3_trading.strategy_judgment._judgment_services import _handle_component_failure
    result = _handle_component_failure("E-05_parameter_drift", RuntimeError("test"))
    assert result == True, f"DEGRADE should return True, got {result}"

def test_was_blocked_flag():
    """验证was_blocked()函数存在"""
    from ali2026v3_trading.strategy_judgment._judgment_services import was_blocked
    assert callable(was_blocked)

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            try:
                fn()
                print(f'[PASS] {name}')
            except Exception as e:
                print(f'[FAIL] {name}: {e}')
