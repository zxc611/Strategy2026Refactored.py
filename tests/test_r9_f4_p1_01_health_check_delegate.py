# MODULE_ID: M2-551
"""Round9-F4 断言测试：P1-01 HealthCheckAPI委托HealthCheckAggregator
验证运行时行为：
1) HealthCheckAPI有界delegate_to_aggregator方法
2) 委托失败时回退到原有逻辑
3) get_health_status始终返回有效结果
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_has_delegate_method():
    """HealthCheckAPI应有界delegate_to_aggregator方法"""
    from ali2026v3_trading.infra.health_monitor import HealthCheckAPI
    api = HealthCheckAPI()
    assert hasattr(api, '_delegate_to_aggregator'), \
        "HealthCheckAPI缺少。delegate_to_aggregator方法"
    print("  OK: HealthCheckAPI有界delegate_to_aggregator方法")


def test_fallback_when_aggregator_unavailable():
    """聚合器不可用时，回退到原有逻辑仍返回有效结果"""
    from ali2026v3_trading.infra.health_monitor import HealthCheckAPI
    api = HealthCheckAPI()
    # 无ServiceContainer时，_delegate_to_aggregator返回None
    result = api._delegate_to_aggregator()
    # 聚合器不可用时返回None（触发回退）'
    assert result is None, "无ServiceContainer时应返回None触发回退"
    # get_health_status仍能正常工作
    status = api.get_health_status()
    assert isinstance(status, dict), "get_health_status应返回dict"
    assert 'overall_status' in status, "结果应包含overall_status"
    print(f"  OK: 回退逻辑正常工作, overall_status={status['overall_status']}")


def test_get_health_status_always_returns_valid():
    """get_health_status始终返回有效结果"""
    from ali2026v3_trading.infra.health_monitor import HealthCheckAPI
    api = HealthCheckAPI()
    status = api.get_health_status()
    assert isinstance(status, dict), "应返回dict"
    assert 'overall_status' in status, "应包含overall_status"
    assert 'timestamp' in status, "应包含timestamp"
    # 不应有delegated_to字段（因为聚合器不可用，走了回退逻辑）'
    assert status.get('delegated_to') is None or 'delegated_to' not in status, \
        "回退逻辑不应设置delegated_to"
    print("  OK: get_health_status始终返回有效结果")


if __name__ == '__main__':
    print("=== Round9-F4: P1-01 HealthCheckAPI委托HealthCheckAggregator ===")
    test_has_delegate_method()
    test_fallback_when_aggregator_unavailable()
    test_get_health_status_always_returns_valid()
    print("ALL ASSERTIONS PASSED")
