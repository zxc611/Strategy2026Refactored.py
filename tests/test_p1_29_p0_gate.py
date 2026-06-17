# MODULE_ID: M2-446
"""P1-29断言测试: p0_gate_check统一到phase_scan_core"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def test_p0_gate_check_single_definition():
    pattern = re.compile(r'^def p0_gate_check\(')
    definitions = []
    skip = ('_backup_', 'archive', '__pycache__', '.git', 'tests', 'param_pool_backup_20260612', 'param_pool_backup_slim_20260613')
    for dirpath, dirnames, filenames in os.walk(_ROOT):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) or d == s for s in skip)]
        for fn in filenames:
            if fn.endswith('.py'):
                fpath = os.path.join(dirpath, fn)
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if pattern.match(line.strip()):
                            rel = os.path.relpath(fpath, _ROOT)
                            definitions.append(f"{rel}:{i}")
    assert len(definitions) == 1, f"Expected 1 p0_gate_check definition, found {len(definitions)}: {definitions}"
    assert 'phase_scan' in definitions[0], f"Definition not in phase_scan: {definitions[0]}"


def test_enhanced_gates_imports_from_core():
    # enhanced_phase_scan_gates.py was merged into phase_scan.py
    with open(os.path.join(_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'def p0_gate_check(' in src, "phase_scan.py should have p0_gate_check definition"


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