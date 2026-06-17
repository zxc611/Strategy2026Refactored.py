# MODULE_ID: M2-437
"""P0-30断言测试: CONTRACT_MULTIPLIER_MAP统一到shared_trading_constants"""
import sys
sys.path.insert(0, '..')

import os

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def test_contract_multiplier_map_in_shared_trading_constants():
    from ali2026v3_trading.infra.shared_trading_constants import CONTRACT_MULTIPLIER_MAP
    assert 'IF' in CONTRACT_MULTIPLIER_MAP
    assert CONTRACT_MULTIPLIER_MAP['IF'] == 300.0


def test_judgment_types_imports_from_shared():
    with open(os.path.join(_ROOT, 'strategy_judgment', 'judgment_types.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'from ali2026v3_trading.infra.shared_trading_constants import CONTRACT_MULTIPLIER_MAP' in src
    assert "'IF': 300.0" not in src, "judgment_types still has inline CONTRACT_MULTIPLIER_MAP"


def test_contract_multiplier_map_single_source():
    import re
    pattern = re.compile(r"CONTRACT_MULTIPLIER_MAP\s*[:=]\s*(?:Dict\[.*?\]\s*=)?\s*\{")
    definitions = []
    skip = ('_backup_', 'archive', '__pycache__', '.git', 'tests')
    for dirpath, dirnames, filenames in os.walk(_ROOT):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) for s in skip)]
        for fn in filenames:
            if fn.endswith('.py'):
                fpath = os.path.join(dirpath, fn)
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if pattern.search(line):
                            rel = os.path.relpath(fpath, _ROOT)
                            definitions.append(f"{rel}:{i}")
    assert len(definitions) == 1, f"Expected 1 CONTRACT_MULTIPLIER_MAP definition, found {len(definitions)}: {definitions}"


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