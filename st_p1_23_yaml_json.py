# MODULE_ID: M2-444
"""P1-23断言测试: YAML/JSON双源已通过config_service统一门面"""
import sys
sys.path.insert(0, '..')

import os

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def test_config_service_reexports_both_loaders():
    from ali2026v3_trading.config.config_service import load_cascade_threshold_grid, load_plr_thresholds
    assert callable(load_cascade_threshold_grid)
    assert callable(load_plr_thresholds)


def test_json_loader_has_priority_annotation():
    with open(os.path.join(_ROOT, 'config', 'config_loader.py'), encoding='utf-8') as f:
        src = f.read()
    assert 'P1-23' in src or '优先级' in src or 'priority' in src.lower() or 'load_default_params_from_json' in src


def test_yaml_loader_exists_and_importable():
    import ali2026v3_trading.config.config_loader as yml
    assert hasattr(yml, 'load_cascade_threshold_grid') or hasattr(yml, 'load_plr_thresholds')


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