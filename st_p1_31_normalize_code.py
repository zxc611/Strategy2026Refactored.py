# MODULE_ID: M2-448
"""P1-31断言测试: _normalize_code统一到infra/_helpers"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
_SKIP_DIRS = ('_backup_', 'archive', '__pycache__', '.git', 'tests')


def test_normalize_code_single_definition():
    pattern = re.compile(r'^def _normalize_code\(')
    definitions = []
    for dirpath, dirnames, filenames in os.walk(_ROOT):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) for s in _SKIP_DIRS) and d != 'param_pool_backup_20260612']
        for fn in filenames:
            if fn.endswith('.py'):
                fpath = os.path.join(dirpath, fn)
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if pattern.match(line.strip()):
                            rel = os.path.relpath(fpath, _ROOT)
                            definitions.append(f"{rel}:{i}")
    assert len(definitions) == 1, f"Expected 1 _normalize_code definition, found {len(definitions)}: {definitions}"
    assert '_helpers' in definitions[0] or 'storage_service' in definitions[0], f"Definition not in _helpers or storage_service: {definitions[0]}"


def test_query_kline_imports_from_helpers():
    with open(os.path.join(_ROOT, 'data', 'query_kline_aggregator.py'), encoding='utf-8') as f:
        src = f.read()
    assert '_helpers' in src, "query_kline_aggregator does not import from _helpers"


def test_normalize_code_behavior():
    from ali2026v3_trading.infra._helpers import _normalize_code
    assert _normalize_code('  IF  ') == 'IF'
    assert _normalize_code('') == ''
    assert _normalize_code('cu') == 'cu'


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