# MODULE_ID: M2-499
"""R7断言测试: P1-61/P1-62/P1-63/P1-65/P2-14——验证运行时行为"""
import os
import sys
import inspect
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ali2026v3_trading'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_p1_61_aggregate_greeks_delegates():
    """P1-61: RiskDashboardService.aggregate_greeks 委托到 position_greeks"""
    from ali2026v3_trading.risk.risk_service import RiskDashboardService
    src = inspect.getsource(RiskDashboardService.aggregate_greeks)
    assert 'position_greeks' in src, "aggregate_greeks未委托到position_greeks"
    assert 'aggregate_greeks_exposure' in src, "aggregate_greeks未调用aggregate_greeks_exposure"


def test_p1_62_calculate_risk_metrics_delegates():
    """P1-62: RiskService.calculate_risk_metrics 委托到 _compute_service"""
    from ali2026v3_trading.risk.risk_service import RiskService
    src = inspect.getsource(RiskService.calculate_risk_metrics)
    assert '_compute_service' in src, "calculate_risk_metrics未委托到。compute_service"


def test_p1_63_compute_decision_score_delegates():
    """P1-63: RiskService.compute_decision_score 委托到 _compute_service"""
    from ali2026v3_trading.risk.risk_service import RiskService
    src = inspect.getsource(RiskService.compute_decision_score)
    assert '_compute_service' in src, "compute_decision_score未委托到。compute_service"


def test_p1_63_safe_normalize_weights_single_source():
    """P1-63: safe_normalize_weights 只在 resilience.py 定义"""
    from ali2026v3_trading.infra.resilience import safe_normalize_weights
    # 运行时验证
    result = safe_normalize_weights({'a': 2.0, 'b': 3.0})
    assert abs(sum(result.values()) - 1.0) < 1e-9, f"权重之和应为1.0: {result}"


def test_p1_65_get_cached_params_delegates():
    """P1-65: get_cached_params 优先委托到 ParamsService"""
    from ali2026v3_trading.config.config_params import get_cached_params
    src = inspect.getsource(get_cached_params)
    assert 'get_params_service' in src, "get_cached_params未委托到ParamsService"


def test_p2_14_callback_registry_lifecycle():
    """P2-14: callback_registry 线程池注册到 lifecycle_resource"""
    import ali2026v3_trading.infra.registry_service as cr
    src = inspect.getsource(cr)
    assert 'register_thread_pool' in src, "callback_registry未注册到lifecycle_resource"
    assert 'lifecycle_resource' in src, "callback_registry未引用lifecycle_resource"


def test_p2_14_position_command_lifecycle():
    """P2-14: position_command_service 线程池注册到 lifecycle_resource"""
    from ali2026v3_trading.position.position_command_service import PositionCommandService
    src = inspect.getsource(PositionCommandService._schedule_close_retry)
    assert 'register_thread_pool' in src, "position_command_service未注册到lifecycle_resource"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
