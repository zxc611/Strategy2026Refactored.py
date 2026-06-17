# MODULE_ID: M2-442
"""P1-15断言测试: RISK_FREE_RATE硬编码清理 — 主代码无硬编码0.02"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def _find_py_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ('_backup_r19_20260603_133221', '_backup_split_20260609_210047', '_backup_split_20260609_210112', '_backup_split_20260610_073126', '__pycache__', '.git', 'tests')]
        for fn in filenames:
            if fn.endswith('.py'):
                yield os.path.join(dirpath, fn)


def test_no_hardcoded_risk_free_rate_in_main_code():
    pattern = re.compile(r'risk_free_rate\s*=\s*0\.\d')
    violations = []
    for fpath in _find_py_files(_ROOT):
        with open(fpath, encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if pattern.search(line):
                    rel = os.path.relpath(fpath, _ROOT)
                    violations.append(f"{rel}:{i}: {line.strip()}")
    assert len(violations) == 0, f"Found {len(violations)} hardcoded risk_free_rate:\n" + "\n".join(violations)


def test_default_risk_free_rate_exists_in_shared_utils():
    from ali2026v3_trading.infra.shared_utils import DEFAULT_RISK_FREE_RATE
    assert DEFAULT_RISK_FREE_RATE == 0.02


def test_greeks_calculator_uses_constant():
    with open(os.path.join(_ROOT, 'governance', 'greeks_calculator.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'self._risk_free_rate = 0.02' not in src, "greeks_calculator still has hardcoded 0.02"
    assert 'DEFAULT_RISK_FREE_RATE' in src, "greeks_calculator does not import DEFAULT_RISK_FREE_RATE"


def test_shadow_pnl_metrics_uses_constant():
    # P1-15修复: 文件已合并到shadow_strategy_pnl.py
    _path = os.path.join(_ROOT, 'strategy', 'shadow_strategy_pnl.py')
    with open(_path, encoding='utf-8') as f:
        src = f.read()
    assert 'risk_free_rate=0.02' not in src, "shadow_strategy_pnl_metrics still has hardcoded 0.02"
    assert 'DEFAULT_RISK_FREE_RATE' in src, "shadow_strategy_pnl_metrics does not import DEFAULT_RISK_FREE_RATE"


def test_adv_validation_uses_constant():
    with open(os.path.join(_ROOT, 'param_pool', 'validation', 'adv_validation_misc.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'risk_free_rate=0.02' not in src, "adv_validation_misc still has hardcoded 0.02"
    assert 'DEFAULT_RISK_FREE_RATE' in src, "adv_validation_misc does not import DEFAULT_RISK_FREE_RATE"


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