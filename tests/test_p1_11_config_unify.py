# MODULE_ID: M2-441
"""P1-11断言测试: get_cached_params统一从config_service门面导入"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
_SKIP_DIRS = ('_backup_', 'archive', '__pycache__', '.git', 'tests', 'config')


def test_no_direct_config_params_get_cached_import():
    pattern = re.compile(r'from\s+ali2026v3_trading\.config\.config_params\s+import\s+.*get_cached_params')
    violations = []
    for dirpath, dirnames, filenames in os.walk(_ROOT):
        dirnames[:] = [d for d in dirnames if not any(d.startswith(s) for s in _SKIP_DIRS)]
        for fn in filenames:
            if fn.endswith('.py'):
                fpath = os.path.join(dirpath, fn)
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if pattern.search(line):
                            rel = os.path.relpath(fpath, _ROOT)
                            violations.append(f"{rel}:{i}")
    assert len(violations) == 0, f"Found {len(violations)} direct config_params imports: {violations}"


def test_config_service_exports_get_cached_params():
    from ali2026v3_trading.config.config_service import get_cached_params
    assert callable(get_cached_params)


def test_config_service_and_config_params_return_same_object():
    from ali2026v3_trading.config.config_service import get_cached_params as svc_fn
    from ali2026v3_trading.config.config_params import get_cached_params as params_fn
    assert svc_fn is params_fn, "config_service and config_params return different get_cached_params"


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