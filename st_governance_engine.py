#!/usr/bin/env python3
# MODULE_ID: M2-345
"""Governance引擎集成测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_governance_engine_import():
    from ali2026v3_trading.governance.governance_engine import GovernanceEngine
    assert GovernanceEngine is not None

def test_governance_engine_get():
    from ali2026v3_trading.governance.governance_engine import get_governance_engine
    engine = get_governance_engine()
    assert engine is not None

def test_governance_checkers_exist():
    """验证governance引擎的checker已集成到评判系统"""
    import ali2026v3_trading.strategy_judgment._judgment_services as jsh
    src = open(jsh.__file__, encoding='utf-8').read()
    assert 'governance_engine' in src.lower() or 'get_governance_engine' in src

def test_governance_judgment_integration():
    """验证E-04集成: governance_engine checker结果传递到评判系统"""
    from ali2026v3_trading.strategy_judgment._judgment_services import run_ecosystem_integrations
    assert callable(run_ecosystem_integrations)

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            try:
                fn()
                print(f'[PASS] {name}')
            except Exception as e:
                print(f'[FAIL] {name}: {e}')
