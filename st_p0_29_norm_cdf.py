# MODULE_ID: M2-436
"""P0-29断言测试: _norm_cdf/_norm_pdf统一到greeks_calculator（原greeks_math已合并）"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
_SKIP_DIRS = ('_backup_', 'archive', '__pycache__', '.git', 'tests')


def _find_py_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) for s in _SKIP_DIRS)]
        for fn in filenames:
            if fn.endswith('.py'):
                yield os.path.join(dirpath, fn)


def test_no_local_norm_cdf_definition():
    pattern = re.compile(r'^def _norm_cdf\(')
    violations = []
    for fpath in _find_py_files(_ROOT):
        with open(fpath, encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if pattern.match(line.strip()):
                    rel = os.path.relpath(fpath, _ROOT)
                    violations.append(f"{rel}:{i}")
    assert len(violations) == 1, f"Expected 1 _norm_cdf definition (greeks_calculator), found {len(violations)}: {violations}"
    assert 'greeks_calculator' in violations[0], f"Definition not in greeks_calculator: {violations[0]}"


def test_no_local_norm_pdf_definition():
    pattern = re.compile(r'^def _norm_pdf\(')
    violations = []
    for fpath in _find_py_files(_ROOT):
        with open(fpath, encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if pattern.match(line.strip()):
                    rel = os.path.relpath(fpath, _ROOT)
                    violations.append(f"{rel}:{i}")
    assert len(violations) == 1, f"Expected 1 _norm_pdf definition (greeks_calculator), found {len(violations)}: {violations}"


def test_norm_cdf_precision():
    from ali2026v3_trading.governance.greeks_calculator import _norm_cdf
    assert abs(_norm_cdf(0.0) - 0.5) < 1e-15
    assert abs(_norm_cdf(1.96) - 0.975) < 1e-3
    assert abs(_norm_cdf(-1.96) - 0.025) < 1e-3


def test_feature_engine_imports_from_greeks_calculator():
    import ali2026v3_trading.param_pool._preprocess as mod
    with open(mod.__file__, encoding='utf-8') as f:
        src = f.read()
    assert 'greeks_calculator' in src, "_preprocess does not import from greeks_calculator"


def test_backtest_pricing_uses_greeks_calculator():
    with open(os.path.join(_ROOT, 'param_pool', 'backtest', 'backtest_config.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'greeks_calculator' in src, "backtest_pricing does not import from greeks_calculator"
    assert 'def _norm_cdf' not in src, "backtest_pricing still has local _norm_cdf definition"


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