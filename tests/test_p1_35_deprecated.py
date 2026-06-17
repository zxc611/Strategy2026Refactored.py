# MODULE_ID: M2-450
"""P1-35断言测试: DEPRECATED代码已正确标注DeprecationWarning"""
import sys
sys.path.insert(0, '..')

import os

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def test_strategy_ui_has_deprecation():
    with open(os.path.join(_ROOT, 'config', 'ui_service.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DeprecationWarning' in src or 'DEPRECATED' in src


def test_plr_calculator_deprecated_methods_have_warning():
    with open(os.path.join(_ROOT, 'signal', 'plr_calculator.py'), encoding='utf-8') as f:
        src = f.read()
    for method in ('estimate_plr', 'get_plr_status', 'set_target_plr', 'get_all_stats'):
        assert f'def {method}' in src
    assert src.count('DeprecationWarning') >= 4


def test_crack_validation_module_deprecated():
    with open(os.path.join(_ROOT, 'risk', 'crack_validation.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'DEPRECATED' in src


def test_migrate_method_deprecated():
    with open(os.path.join(_ROOT, 'data', 'query_instrument_service.py'), encoding='utf-8') as f:
        src = f.read()
    assert '_migrate_instrument_ids_to_global_namespace' in src
    assert 'deprecated' in src.lower()


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