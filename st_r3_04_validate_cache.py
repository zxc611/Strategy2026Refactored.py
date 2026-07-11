# MODULE_ID: M2-530
"""R3-04: 验证 F-09/F-10/C-01 修复后的断言测试
1. validate_shadow_param_independence 从两个文件导入是同一对象
2. validate_resilience_params 可从 resilience_config 导入
3. validate_params 仍可从 config_params 导入
4. CACHE_TTL 从 strategy_config 导入与 config_params 是同一值
5. validate_resilience_params 运行时可调用
"""
import pytest


def test_validate_shadow_param_independence_same_object():
    """F-09: validate_shadow_param_independence 从两个文件导入是同一对象"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import validate_shadow_param_independence as utils_fn
    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence as validation_fn
    assert utils_fn is validation_fn, (
        "backtest_runner_utils.validate_shadow_param_independence 应与 "
        "backtest_runner_validation.validate_shadow_param_independence 是同一对象"
    )


def test_validate_resilience_params_importable():
    """F-10: validate_resilience_params 可从 resilience_config 导入"""
    from ali2026v3_trading.infra.resilience import validate_resilience_params
    assert callable(validate_resilience_params)


def test_validate_params_from_config_params():
    """F-10: validate_params 仍可从 config_params 导入"""
    from ali2026v3_trading.config.config_params import validate_params
    assert callable(validate_params)


def test_cache_ttl_same_value():
    """C-01: CACHE_TTL 从 strategy_config 导入与 config_params 是同一值"""
    from ali2026v3_trading.strategy.strategy_config_layer import CACHE_TTL as sc_ttl
    from ali2026v3_trading.config.config_params import CACHE_TTL as cp_ttl
    assert sc_ttl == cp_ttl, f"strategy_config.CACHE_TTL={sc_ttl} != config_params.CACHE_TTL={cp_ttl}"


def test_cache_ttl_is_same_object():
    """C-01: CACHE_TTL 从 strategy_config 导入与 config_params 是同一对象"""
    from ali2026v3_trading.strategy.strategy_config_layer import CACHE_TTL as sc_ttl
    from ali2026v3_trading.config.config_params import CACHE_TTL as cp_ttl
    assert sc_ttl is cp_ttl, "strategy_config.CACHE_TTL 应与 config_params.CACHE_TTL 是同一对象"


def test_validate_resilience_params_callable():
    """F-10: validate_resilience_params 运行时可调用"""
    from ali2026v3_trading.infra.resilience import validate_resilience_params
    result = validate_resilience_params({'max_risk_ratio': 0.5})
    assert isinstance(result, dict)
    assert 'valid' in result
    assert result['valid'] is True


def test_validate_resilience_params_rejects_invalid():
    """F-10: validate_resilience_params 拒绝无效参数"""
    from ali2026v3_trading.infra.resilience import validate_resilience_params
    result = validate_resilience_params({'max_risk_ratio': 1.5})
    assert isinstance(result, dict)
    assert result['valid'] is False
