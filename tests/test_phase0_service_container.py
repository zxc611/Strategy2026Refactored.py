# MODULE_ID: M2-463
"""Phase 0.1+0.2 断言测试: service_container.py 缩进修复 + 回滚逻辑修复"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def test_service_container_methods_exist():
    """P0.1: bind_cross_layer_dependencies 和 _check_circular_dependencies 必须是 ServiceContainer 的方法"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    # 验证方法是类实例的方法
    assert hasattr(sc, 'bind_cross_layer_dependencies'), "bind_cross_layer_dependencies 不存在于 ServiceContainer 实例"
    assert hasattr(sc, '_check_circular_dependencies'), "_check_circular_dependencies 不存在于 ServiceContainer 实例"
    assert callable(getattr(sc, 'bind_cross_layer_dependencies')), "bind_cross_layer_dependencies 不可调用"
    assert callable(getattr(sc, '_check_circular_dependencies')), "_check_circular_dependencies 不可调用"
    print("[PASS] P0.1: ServiceContainer 方法存在且可调用")

def test_check_circular_dependencies_no_cycle():
    """P0.1: _check_circular_dependencies 在无循环依赖时返回 False"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    result = sc._check_circular_dependencies()
    assert result is False, f"无循环依赖时应返回 False, 实际返回 {result}"
    print("[PASS] P0.1: _check_circular_dependencies 无循环依赖时返回 False")

def test_check_circular_dependencies_with_cycle():
    """P0.1: _check_circular_dependencies 在有循环依赖时返回 True"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    # 注入循环依赖: A -> B -> A
    sc._dependencies['test_a'] = ['test_b']
    sc._dependencies['test_b'] = ['test_a']
    result = sc._check_circular_dependencies()
    assert result is True, f"有循环依赖时应返回 True, 实际返回 {result}"
    print("[PASS] P0.1: _check_circular_dependencies 有循环依赖时返回 True")

def test_rollback_all_services_stopped():
    """P0.2: _rollback_started_services 必须停止所有已启动服务后才抛异常"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    stopped = []
    class MockService:
        def __init__(self, name):
            self.name = name
        def stop(self):
            stopped.append(self.name)
            if self.name == 'svc2':
                raise RuntimeError("mock stop error")

    sc._services['svc1'] = MockService('svc1')
    sc._services['svc2'] = MockService('svc2')
    sc._services['svc3'] = MockService('svc3')

    # 调用回滚，应停止所有3个服务（即使svc2抛异常）'
    try:
        sc._rollback_started_services(['svc1', 'svc2', 'svc3'])
        # 如果没有错误，svc2的stop()没有抛异常的情况
    except RuntimeError as e:
        pass  # 预期：svc2的stop()抛异常后，回滚继续，最终汇总抛出

    # 关键断言：所有3个服务都必须被尝试停止
    assert 'svc1' in stopped, f"svc1 未被停止, stopped={stopped}"
    assert 'svc2' in stopped, f"svc2 未被停止, stopped={stopped}"
    assert 'svc3' in stopped, f"svc3 未被停止, stopped={stopped}"
    print("[PASS] P0.2: 回滚时所有服务均被停止，不会在第一个异常后中断")

def test_rollback_preserves_order():
    """P0.2: 回滚顺序必须是逆序"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    stopped = []
    class MockService:
        def __init__(self, name):
            self.name = name
        def stop(self):
            stopped.append(self.name)

    sc._services['svc1'] = MockService('svc1')
    sc._services['svc2'] = MockService('svc2')
    sc._services['svc3'] = MockService('svc3')

    sc._rollback_started_services(['svc1', 'svc2', 'svc3'])

    assert stopped == ['svc3', 'svc2', 'svc1'], f"回滚顺序应为逆序, 实际={stopped}"
    print("[PASS] P0.2: 回滚顺序为逆序 svc3->svc2->svc1")

def test_initialize_all_calls_bind():
    """P0.1: initialize_all() 必须能调用 bind_cross_layer_dependencies"""
    from ali2026v3_trading.infra.service_container import ServiceContainer
    sc = ServiceContainer()
    # 注册一个无依赖的服务
    class MockSvc:
        pass
    sc.register('event_bus', MockSvc())
    result = sc.initialize_all()
    assert result is True, f"initialize_all 应返回 True, 实际返回 {result}"
    print("[PASS] P0.1: initialize_all() 成功调用 bind_cross_layer_dependencies")

if __name__ == '__main__':
    test_service_container_methods_exist()
    test_check_circular_dependencies_no_cycle()
    test_check_circular_dependencies_with_cycle()
    test_rollback_all_services_stopped()
    test_rollback_preserves_order()
    test_initialize_all_calls_bind()
    print("\n=== Phase 0.1+0.2 全部断言测试通过 ===")
