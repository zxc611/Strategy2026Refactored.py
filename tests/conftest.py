"""
pytest conftest: 全局单例重置，解决测试间状态污染
"""
import pytest
import sys


def _reset_all_singletons():
    """强制重置所有ali2026v3_trading模块的全局单例"""
    modules_to_reset = [
        k for k in sys.modules
        if k.startswith('ali2026v3_trading') and k != 'ali2026v3_trading.tests.conftest'
    ]
    singleton_attrs = [
        '_global_instance', '_instance', '_global_signal_service',
        '_global_risk_service', '_global_position_service',
        '_global_order_service', '_global_event_bus',
        '_singleton', '_instance_lock', '_init_lock',
    ]
    for mod_name in modules_to_reset:
        mod = sys.modules.get(mod_name)
        if mod is None:
            continue
        for attr in singleton_attrs:
            if hasattr(mod, attr):
                val = getattr(mod, attr)
                if val is not None and not isinstance(val, type) and not callable(val):
                    try:
                        setattr(mod, attr, None)
                    except Exception:
                        pass


@pytest.fixture(autouse=True)
def _isolate_global_state():
    _reset_all_singletons()
    # AP-03: SingletonRegistry全局重置
    try:
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        SingletonRegistry.reset_all()
    except Exception:
        pass
    yield
    _reset_all_singletons()
    try:
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        SingletonRegistry.reset_all()
    except Exception:
        pass
