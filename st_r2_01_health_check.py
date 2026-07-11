# MODULE_ID: M2-513
"""test_r2_01_health_check.py — P1-01 健康检查重叠修复验证

验证:
1. HealthCheckAggregator 的3个方法内部存在委托调用（通过检查源码）
2. HealthCheckAggregator 可以实例化并调用 aggregate() 不抛异常
"""
import inspect
import os
import sys

import pytest

# 确保项目根目录在 sys.path 中
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestHealthCheckDelegation:
    """验证 HealthCheckAggregator 三个方法的委托调用存在"""

    @pytest.fixture(autouse=True)
    def _load_module(self):
        from ali2026v3_trading.infra.health_monitor import HealthCheckAggregator
        self.HealthCheckAggregator = HealthCheckAggregator

    def test_check_heartbeat_status_delegates_to_health_monitor(self):
        """_check_heartbeat_status 应委托 HealthMonitor.heartbeat()"""
        source = inspect.getsource(self.HealthCheckAggregator._check_heartbeat_status)
        assert 'get_health_monitor' in source, "_check_heartbeat_status 未包含 get_health_monitor 委托"
        assert '_hm.heartbeat()' in source, "_check_heartbeat_status 未调用 _hm.heartbeat()"
        assert 'delegated_to' in source, "_check_heartbeat_status 未设置 delegated_to 标记"

    def test_check_safety_meta_layer_delegates_to_health_check_api(self):
        """_check_safety_meta_layer 应委托 HealthCheckAPI._get_circuit_breaker_status()"""
        source = inspect.getsource(self.HealthCheckAggregator._check_safety_meta_layer)
        assert 'HealthCheckAPI' in source, "_check_safety_meta_layer 未包含 HealthCheckAPI 委托"
        assert '_get_circuit_breaker_status' in source, "_check_safety_meta_layer 未调用 _get_circuit_breaker_status()"
        assert 'delegated_to' in source, "_check_safety_meta_layer 未设置 delegated_to 标记"

    def test_check_strategy_ecosystem_delegates_to_lifecycle_monitor(self):
        """_check_strategy_ecosystem 应委托 LifecycleMonitor.health_check()"""
        source = inspect.getsource(self.HealthCheckAggregator._check_strategy_ecosystem)
        assert '_lc_monitor' in source, "_check_strategy_ecosystem 未包含 _lc_monitor 委托"
        assert '_monitor.health_check()' in source, "_check_strategy_ecosystem 未调用 _monitor.health_check()"
        assert 'delegated_to' in source, "_check_strategy_ecosystem 未设置 delegated_to 标记"


class TestHealthCheckAggregatorInstantiation:
    """验证 HealthCheckAggregator 可以实例化并调用 aggregate() 不抛异常"""

    def test_instantiate_and_aggregate_no_exception(self):
        """使用最小 mock 对象实例化并调用 aggregate()"""
        from ali2026v3_trading.infra.health_monitor import HealthCheckAggregator

        class MinimalSvc:
            _platform_api_ready = False
            _last_heartbeat_ts = 0.0
            _orders_by_id = {}
            _shadow_engine = None
            _strategy_ecosystem = None
            _lifecycle_svc = None
            _health_check_api = None

        svc = MinimalSvc()
        aggregator = HealthCheckAggregator(svc)
        result = aggregator.aggregate()

        assert isinstance(result, dict), "aggregate() 应返回 dict"
        assert 'overall_status' in result, "aggregate() 结果应包含 overall_status"
        # 验证6个检查项都存在
        for check_name in HealthCheckAggregator._CHECKS:
            assert check_name in result, f"aggregate() 结果缺少检查项 {check_name}"

    def test_aggregate_returns_valid_statuses(self):
        """aggregate() 返回的每个检查项都应包含 status 字段"""
        from ali2026v3_trading.infra.health_monitor import HealthCheckAggregator

        class MinimalSvc:
            _platform_api_ready = True
            _last_heartbeat_ts = 0.0
            _orders_by_id = {}
            _shadow_engine = None
            _strategy_ecosystem = None
            _lifecycle_svc = None
            _health_check_api = None

        svc = MinimalSvc()
        aggregator = HealthCheckAggregator(svc)
        result = aggregator.aggregate()

        for check_name in HealthCheckAggregator._CHECKS:
            check_result = result[check_name]
            assert isinstance(check_result, dict), f"{check_name} 结果应为 dict"
            assert 'status' in check_result, f"{check_name} 结果缺少 status 字段"
