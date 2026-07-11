# MODULE_ID: M2-449
"""P1-34断言测试: 硬编码金融常量统一"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
_SKIP_DIRS = ('_backup_', 'archive', '__pycache__', '.git', 'tests')


def test_no_hardcoded_365_in_main_code():
    pattern = re.compile(r'\b365\.0\b')
    violations = []
    for dirpath, dirnames, filenames in os.walk(_ROOT):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) for s in _SKIP_DIRS)]
        for fn in filenames:
            if fn.endswith('.py'):
                fpath = os.path.join(dirpath, fn)
                rel = os.path.relpath(fpath, _ROOT)
                if 'shared_utils.py' in rel:
                    continue
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if pattern.search(line):
                            violations.append(f"{rel}:{i}")
    assert len(violations) == 0, f"Found hardcoded 365.0: {violations}"


def test_days_per_year_calendar_exists():
    from ali2026v3_trading.infra.shared_utils import DAYS_PER_YEAR_CALENDAR
    assert DAYS_PER_YEAR_CALENDAR == 365.0


def test_greeks_uses_constant():
    with open(os.path.join(_ROOT, 'governance', 'greeks_calculator.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DAYS_PER_YEAR_CALENDAR' in src


def test_greeks_calculator_uses_constant():
    with open(os.path.join(_ROOT, 'governance', 'greeks_calculator.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DAYS_PER_YEAR_CALENDAR' in src


def test_backtest_pricing_uses_constant():
    with open(os.path.join(_ROOT, 'param_pool', 'backtest', 'backtest_config.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DAYS_PER_YEAR_CALENDAR' in src


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