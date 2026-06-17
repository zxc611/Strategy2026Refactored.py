# MODULE_ID: M2-445
"""P1-28断言测试: Sharpe计算统一 — 无硬编码0.02/252.0在Sharpe相关代码"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
_SKIP_DIRS = ('_backup_', 'archive', '__pycache__', '.git', 'tests')


def test_no_hardcoded_risk_free_in_sharpe():
    violations = []
    for rel in ('param_pool/validation/adv_validation_misc.py', 'strategy/shadow_strategy_pnl.py', 'strategy_judgment/statistical_validity_extensions.py', 'param_pool/validation/statistical_validation.py'):
        with open(os.path.join(_ROOT, *rel.split('/')), encoding='utf-8') as f:
            src = f.read()
        if re.search(r'risk_free_rate\s*=\s*0\.02', src):
            violations.append(f"{rel}: still has risk_free_rate=0.02")
        if re.search(r'annualize_factor\s*=\s*252\.0', src):
            violations.append(f"{rel}: still has annualize_factor=252.0")
    assert len(violations) == 0, f"Found hardcoded values: {violations}"


def test_compute_sharpe_stable_is_authority():
    from ali2026v3_trading.infra.resilience import compute_sharpe_stable
    result = compute_sharpe_stable([0.01, 0.02, -0.01, 0.03], risk_free_rate=0.0, annualize_factor=252.0)
    assert isinstance(result, float)


def test_adv_validation_uses_constants():
    with open(os.path.join(_ROOT, 'param_pool', 'validation', 'adv_validation_misc.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DEFAULT_RISK_FREE_RATE' in src
    assert 'ANNUALIZE_FACTOR_DAILY' in src


def test_shadow_pnl_uses_constants():
    with open(os.path.join(_ROOT, 'strategy', 'shadow_strategy_pnl.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DEFAULT_RISK_FREE_RATE' in src
    assert 'ANNUALIZE_FACTOR_DAILY' in src


def test_walkforward_uses_constants():
    # adv_validation_stats.py is now a shim; check the real source
    with open(os.path.join(_ROOT, 'param_pool', 'validation', 'statistical_validation.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DEFAULT_RISK_FREE_RATE' in src


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