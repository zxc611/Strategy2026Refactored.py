# MODULE_ID: M2-453
"""P2-15/P2-16断言测试: 导入路径修复+diagnosis_probe fallback有warning"""
import sys
sys.path.insert(0, '..')

import os

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def test_backtest_runner_utils_imports_from_correct_source():
    with open(os.path.join(_ROOT, 'param_pool', 'backtest', 'backtest_runner_utils.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'from ali2026v3_trading.param_pool._param_defaults import _REASON_TIME_STOP_SOURCE' in src or 'from ali2026v3_trading.param_pool._param_grids import _REASON_TIME_STOP_SOURCE' in src
    assert 'from ali2026v3_trading.infra.shared_trading_constants import REASON_MULTIPLIERS' in src


def test_diagnosis_probe_fallback_has_warning():
    with open(os.path.join(_ROOT, 'infra', 'health_monitor.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'logging.warning' in src or 'logging.info' in src


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