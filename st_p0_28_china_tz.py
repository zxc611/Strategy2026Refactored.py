# MODULE_ID: M2-435
"""P0-28断言测试: _CHINA_TZ本地定义统一到shared_utils"""
import sys
sys.path.insert(0, '..')

import os
import re

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))

_SKIP_DIRS = ('_backup_r19_20260603_133221', '_backup_split_20260609_210047', '_backup_split_20260609_210112', '_backup_split_20260610_073126', '_backup_config_split_20260607_205348', '_backup_g2b_mixin_20260607_122111', '_backup_p1_20260531_154535', '_backup_phase1_20260531_164321', '_backup_phase2_20260531', '_backup_phase2_4_20260603_193528', '_backup_r20_20260604_121840', '_backup_r22_20260603_210608', '_backup_r23_20260604_151129', '_backup_r24_20260604_201507', 'archive', '__pycache__', '.git', 'tests')


def _find_py_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for fn in filenames:
            if fn.endswith('.py'):
                yield os.path.join(dirpath, fn)


def test_no_local_china_tz_definition():
    pattern = re.compile(r'_CHINA_TZ\s*=\s*timezone\(timedelta\(hours=8\)\)')
    violations = []
    for fpath in _find_py_files(_ROOT):
        with open(fpath, encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if pattern.search(line):
                    rel = os.path.relpath(fpath, _ROOT)
                    violations.append(f"{rel}:{i}")
    assert len(violations) == 0, f"Found {len(violations)} local _CHINA_TZ definitions: {violations}"


def test_data_modules_import_from_shared_utils():
    for rel in ('data/data_access.py', 'data/ds_query_cache.py', 'data/quant_services.py', 'data/query_instrument_service.py', 'data/query_kline_aggregator.py'):
        with open(os.path.join(_ROOT, *rel.split('/')), encoding='utf-8') as f:
            src = f.read()
        assert 'CHINA_TZ as _CHINA_TZ' in src, f"{rel} does not import CHINA_TZ from shared_utils"


def test_shared_utils_exports_china_tz():
    from ali2026v3_trading.infra.shared_utils import CHINA_TZ
    from datetime import timezone, timedelta
    assert CHINA_TZ == timezone(timedelta(hours=8))


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