# MODULE_ID: M2-443
"""P1-21断言测试: deprecated装饰器无重复定义 — 仅resilience_retry.py有权威定义"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def _find_py_files(root):
    skip_dirs = ('_backup_r19_20260603_133221', '_backup_split_20260609_210047', '_backup_split_20260609_210112', '_backup_split_20260610_073126', '__pycache__', '.git', 'tests')
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for fn in filenames:
            if fn.endswith('.py'):
                yield os.path.join(dirpath, fn)


def test_deprecated_single_definition():
    pattern = re.compile(r'^def deprecated\(')
    definitions = []
    for fpath in _find_py_files(_ROOT):
        with open(fpath, encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if pattern.match(line.strip()):
                    rel = os.path.relpath(fpath, _ROOT)
                    definitions.append(rel)
    assert len(definitions) == 1, f"Expected 1 deprecated definition, found {len(definitions)}: {definitions}"
    assert 'resilience_retry.py' in definitions[0] or 'resilience.py' in definitions[0], f"Definition not in resilience_retry.py or resilience.py: {definitions[0]}"


def test_shared_utils_reexports_deprecated():
    with open(os.path.join(_ROOT, 'infra', 'shared_utils.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'deprecated' in src, "shared_utils does not re-export deprecated"


def test_deprecated_importable_from_shared_utils():
    from ali2026v3_trading.infra.shared_utils import deprecated
    assert callable(deprecated), "deprecated from shared_utils is not callable"


def test_deprecated_importable_from_resilience_retry():
    from ali2026v3_trading.infra.resilience import deprecated
    assert callable(deprecated), "deprecated from resilience_retry is not callable"


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