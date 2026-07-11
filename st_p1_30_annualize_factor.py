# MODULE_ID: M2-447
"""P1-30断言测试: _get_annualize_factor统一到shared_utils"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def test_get_annualize_factor_in_shared_utils():
    from ali2026v3_trading.infra.shared_utils import get_annualize_factor, ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
    assert get_annualize_factor('box_extreme') == ANNUALIZE_FACTOR_DAILY
    assert get_annualize_factor('default') == ANNUALIZE_FACTOR_MINUTE
    assert get_annualize_factor('default', ticks_per_bar=5) == ANNUALIZE_FACTOR_MINUTE * 5


def test_no_local_get_annualize_factor_definition():
    pattern = re.compile(r'^def _get_annualize_factor\(')
    violations = []
    skip = ('_backup_', 'archive', '__pycache__', '.git', 'tests')
    for dirpath, dirnames, filenames in os.walk(_ROOT):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) for s in skip)]
        for fn in filenames:
            if fn.endswith('.py'):
                fpath = os.path.join(dirpath, fn)
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if pattern.match(line.strip()):
                            rel = os.path.relpath(fpath, _ROOT)
                            violations.append(f"{rel}:{i}")
    assert len(violations) == 0, f"Found local _get_annualize_factor definitions: {violations}"


def test_cascade_judge_imports_from_shared():
    with open(os.path.join(_ROOT, 'evaluation', 'cascade_judge.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'get_annualize_factor' in src, "cascade_judge does not import get_annualize_factor"


def test_backtest_runner_utils_imports_from_shared():
    with open(os.path.join(_ROOT, 'param_pool', 'backtest', 'backtest_runner_utils.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'get_annualize_factor' in src, "backtest_runner_utils does not import get_annualize_factor"


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