# MODULE_ID: M2-495
"""P1-57断言测试: max_risk_ratio等配置项重复获取与校验统一——验证运行时行为"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ali2026v3_trading'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_validate_param_range_exists():
    """validate_param_range 函数存在于 config_params.py"""
    from ali2026v3_trading.config.config_params import validate_param_range
    assert callable(validate_param_range)


def test_validate_param_range_max_risk_ratio():
    """validate_param_range 对 max_risk_ratio 的校验行为正确"""
    from ali2026v3_trading.config.config_params import validate_param_range
    # 有效值
    assert validate_param_range('max_risk_ratio', 0.5) is True
    assert validate_param_range('max_risk_ratio', 1.0) is True
    assert validate_param_range('max_risk_ratio', 0.01) is True
    # 无效值
    assert validate_param_range('max_risk_ratio', 0.0) is False  # min_exclusive
    assert validate_param_range('max_risk_ratio', 1.5) is False
    assert validate_param_range('max_risk_ratio', -0.1) is False


def test_validate_param_range_close_stop_loss():
    """validate_param_range 对 close_stop_loss_ratio 的校验行为正确"""
    from ali2026v3_trading.config.config_params import validate_param_range
    assert validate_param_range('close_stop_loss_ratio', 0.3) is True
    assert validate_param_range('close_stop_loss_ratio', 0.0) is False  # min_exclusive
    assert validate_param_range('close_stop_loss_ratio', 1.5) is False


def test_resilience_delegates_to_validate_param_range():
    """validate_resilience_params 委托到 validate_param_range"""
    import inspect
    from ali2026v3_trading.infra.resilience import validate_resilience_params
    src = inspect.getsource(validate_resilience_params)
    assert 'validate_param_range' in src, "validate_resilience_params未委托到validate_param_range"


def test_risk_config_provider_uses_get_param_default():
    """risk_config_provider._get_max_risk_ratio 使用 get_param_default"""
    import inspect
    from ali2026v3_trading.risk.risk_config_provider import RiskConfigProvider
    src = inspect.getsource(RiskConfigProvider._get_max_risk_ratio)
    assert 'get_param_default' in src, "_get_max_risk_ratio未使用get_param_default"


def test_config_resolver_uses_validate_param_range():
    """config_resolver 约束C2/C3 使用 validate_param_range"""
    import inspect
    from ali2026v3_trading.config.config_loader import validate_params_with_metadata
    src = inspect.getsource(validate_params_with_metadata)
    assert 'validate_param_range' in src, "config_resolver未使用validate_param_range"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
