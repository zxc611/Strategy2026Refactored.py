#!/usr/bin/env python3
# MODULE_ID: M2-398
"""评判评分辅助函数测试 — 验证且拒绝模式"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_component_failure_policy_enum():
    from ali2026v3_trading.strategy_judgment._judgment_services import ComponentFailurePolicy
    assert ComponentFailurePolicy.BLOCK.value == "block"
    assert ComponentFailurePolicy.DEGRADE.value == "degrade"
    assert ComponentFailurePolicy.WARN.value == "warn"

def test_critical_components_block():
    from ali2026v3_trading.strategy_judgment._judgment_services import CRITICAL_COMPONENTS, ComponentFailurePolicy
    assert CRITICAL_COMPONENTS["E-04_governance"] == ComponentFailurePolicy.BLOCK
    assert CRITICAL_COMPONENTS["E-11_violation_tracker"] == ComponentFailurePolicy.BLOCK

def test_critical_components_degrade():
    from ali2026v3_trading.strategy_judgment._judgment_services import CRITICAL_COMPONENTS, ComponentFailurePolicy
    assert CRITICAL_COMPONENTS["E-05_parameter_drift"] == ComponentFailurePolicy.DEGRADE

def test_handle_component_failure_block():
    from ali2026v3_trading.strategy_judgment._judgment_services import _handle_component_failure
    assert _handle_component_failure("E-04_governance", Exception("test")) == False

def test_handle_component_failure_warn():
    from ali2026v3_trading.strategy_judgment._judgment_services import _handle_component_failure
    assert _handle_component_failure("E-13_state_density_decay", Exception("test")) == True

def test_handle_component_failure_unknown():
    from ali2026v3_trading.strategy_judgment._judgment_services import _handle_component_failure
    assert _handle_component_failure("UNKNOWN", Exception("test")) == True

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            try:
                fn()
                print(f'[PASS] {name}')
            except Exception as e:
                print(f'[FAIL] {name}: {e}')
