# MODULE_ID: M2-439
"""P1-05断言测试: EventBus统一 — 源码级验证参数变更通知走EventBus单通道"""
import sys
sys.path.insert(0, '..')

import os

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))


def _read(rel_path):
    with open(os.path.join(_ROOT, *rel_path.split('/')), encoding='utf-8') as f:
        return f.read()


def test_strategy_config_no_local_subscribers():
    src = _read('strategy/strategy_config_layer.py')
    assert '_param_change_subscribers' not in src, "StrategyConfig still has local _param_change_subscribers"


def test_config_params_no_local_subscribers():
    src = _read('config/config_params.py')
    assert '_PARAM_CHANGE_SUBSCRIBERS' not in src, "config_params still has _PARAM_CHANGE_SUBSCRIBERS"
    assert '_param_change_subscribers' not in src, "config_params still has _param_change_subscribers"


def test_params_core_no_local_callbacks():
    src = _read('config/_params_core.py')

    assert 'self._param_change_callbacks = []' not in src, "ParamsCore still has _param_change_callbacks as local list"
    assert 'self._param_change_callbacks = {}' not in src, "ParamsCore still has _param_change_callbacks as local dict"


def test_callback_registry_delegates_to_eventbus():
    src = _read('infra/registry_service.py')
    assert 'EventBus' in src or 'event_bus' in src, "callback_registry does not reference EventBus"


def test_strategy_scheduler_no_param_change_subscribers():
    src = _read('strategy/strategy_scheduler.py')
    assert '_param_change' not in src, "StrategyScheduler has _param_change subscriber list"


def test_state_param_core_uses_eventbus():
    src = _read('config/state_param.py')
    assert 'EventBus' in src or 'event_bus' in src, "state_param does not reference EventBus"


def test_health_check_service_uses_eventbus():
    src = _read('infra/health_monitor.py')
    assert 'EventBus' in src or 'event_bus' in src, "health_monitor does not reference EventBus"


def test_state_store_uses_eventbus():
    src = _read('infra/registry_service.py')
    assert 'EventBus' in src or 'event_bus' in src, "state_store does not reference EventBus"


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
