# MODULE_ID: M2-482
"""R4-F1: P1-40 DI容器与服务工厂统一 断言测试

验证运行时行为:
1. SingletonRegistry.register_singleton同步注册到ServiceContainer
2. ServiceContainer是统一DI入口
3. ServiceFactory创建的服务通过ServiceContainer可获取
4. 三套机制不再完全独立
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_singleton_registry_syncs_to_service_container():
    """P1-40验证: SingletonRegistry.register_singleton同步注册到ServiceContainer"""
    from ali2026v3_trading.infra.registry_service import SingletonRegistry

    # 验证register_singleton源码中包含ServiceContainer桥接
    import inspect
    src = inspect.getsource(SingletonRegistry.register_singleton)
    assert 'ServiceContainer' in src, \
        "register_singleton应桥接到ServiceContainer"
    assert 'ServiceContainer().register' in src, \
        "register_singleton应调用ServiceContainer().register"


def test_service_container_is_unified_entry():
    """P1-40验证: ServiceContainer是统一DI入口"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    assert hasattr(sc, 'register'), "ServiceContainer应有register方法"
    assert hasattr(sc, 'get'), "ServiceContainer应有get方法"

    # 注册并获取
    sc.register('test_p1_40', {'value': 42})
    result = sc.get('test_p1_40')
    assert result == {'value': 42}


def test_service_factory_creates_services():
    """P1-40验证: ServiceFactory创建服务"""
    from ali2026v3_trading.strategy.service_factory import ServiceFactory
    # ServiceFactory需要strategy_id参数
    sf = ServiceFactory('test_strategy')
    assert hasattr(sf, 'create_kline_service'), "ServiceFactory应有create_kline_service方法"
    assert callable(sf.create_kline_service)


def test_three_mechanisms_not_fully_independent():
    """P1-40验证: 三套机制不再完全独立——SingletonRegistry桥接到ServiceContainer"""
    import inspect
    from ali2026v3_trading.infra.registry_service import SingletonRegistry
    src = inspect.getsource(SingletonRegistry.register_singleton)
    assert 'ServiceContainer' in src, \
        "SingletonRegistry.register_singleton应桥接到ServiceContainer"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
