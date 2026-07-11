# MODULE_ID: M2-432
"""升A路径T2.2: 过拟合参数集集成测试

验收标准: 故意构造过拟合参数集，验证扫描器能正确拒绝
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestOverfitRejection:
    """过拟合参数拒绝测试"""

    def test_validate_parameter_set_rejects_overfit(self):
        """_validate_parameter_set应拒绝过拟合参数集"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import _validate_parameter_set

        # 构造过拟合回测结果：equity_curve仅上涨（完美曲线=过拟合信号）'
        overfit_result = {
            'sharpe': 5.0,  # 异常高Sharpe
            'equity_curve': [1000000 + i * 1000 for i in range(100)],  # 完美线性上涨
            '_random_sharpes': [0.1, -0.2, 0.3, 0.05, -0.1, 0.2, -0.3, 0.15, -0.05, 0.08],
        }
        overfit_params = {'param1': 1.0, 'param2': 2.0}

        # 过拟合参数应被拒绝（抛出ValueError）
        with pytest.raises(ValueError, match="验证失败|偏差检测失败"):
            _validate_parameter_set(overfit_params, overfit_result)

    def test_validate_parameter_set_accepts_robust(self):
        """_validate_parameter_set应接受稳健参数集"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import _validate_parameter_set

        # 构造稳健回测结果：有波动但整体正向
        import random
        random.seed(42)
        robust_curve = [1000000]
        for i in range(99):
            change = random.gauss(50, 500)
            robust_curve.append(robust_curve[-1] + change)

        robust_result = {
            'sharpe': 1.5,
            'equity_curve': robust_curve,
            '_random_sharpes': [1.2, 1.4, 1.3, 1.1, 1.5, 1.6, 1.2, 1.3, 1.4, 1.1],
        }
        robust_params = {'param1': 0.5, 'param2': 1.0}

        # 稳健参数不应被拒绝（不抛出异常）'
        _validate_parameter_set(robust_params, robust_result)  # 不应抛出异常

    def test_validate_parameter_set_handles_missing_data(self):
        """_validate_parameter_set应处理缺失数据（不崩溃）"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import _validate_parameter_set

        # 缺失equity_curve和。random_sharpes
        minimal_result = {
            'sharpe': 1.0,
        }
        minimal_params = {'param1': 0.5}

        # 缺失数据不应崩溃（跳过验证）'
        _validate_parameter_set(minimal_params, minimal_result)  # 不应抛出异常


class TestConfigServiceGetParam:
    """ConfigService.get_param 统一参数接口测试"""

    def test_get_param_reads_from_yaml(self):
        """get_param从params.yaml读取参数"""
        from ali2026v3_trading.config.config_service import get_param, ConfigService
        ConfigService.reset_cache()
        value = get_param("shadow_strategy.absolute_ev_floor")
        assert value is not None, "shadow_strategy.absolute_ev_floor should exist in params.yaml"
        assert isinstance(value, (int, float)), f"Expected numeric value, got {type(value)}"

    def test_get_param_dot_notation(self):
        """get_param支持点分隔键"""
        from ali2026v3_trading.config.config_service import get_param, ConfigService
        ConfigService.reset_cache()
        value = get_param("parameter_attributes.shadow_absolute_ev_floor.default")
        assert value is not None, "parameter_attributes.shadow_absolute_ev_floor.default should exist"
        assert value == -0.5, f"Expected -0.5, got {value}"

    def test_get_param_default_value(self):
        """get_param键不存在时返回默认值"""
        from ali2026v3_trading.config.config_service import get_param, ConfigService
        ConfigService.reset_cache()
        value = get_param("nonexistent.key.path", default="fallback")
        assert value == "fallback", f"Expected 'fallback', got {value}"

    def test_get_param_caches_yaml(self):
        """get_param缓存YAML数据"""
        from ali2026v3_trading.config.config_service import get_param, ConfigService
        ConfigService.reset_cache()
        get_param("shadow_strategy.absolute_ev_floor")  # First call loads
        assert ConfigService._params_cache is not None, "Cache should be populated after first call"
        get_param("risk.max_risk_ratio")  # Second call uses cache
        # No error = cache working


class TestNullValidator:
    """NullValidator Null Object模式测试"""

    def test_null_validator_is_falsy(self):
        """NullValidator在布尔上下文中为False"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        nv = NullValidator("TestValidator")
        assert not nv, "NullValidator should be falsy"

    def test_null_validator_has_is_null_flag(self):
        """NullValidator有界is_null标识"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        nv = NullValidator("TestValidator")
        assert nv._is_null is True, "NullValidator._is_null should be True"

    def test_null_validator_method_returns_empty(self):
        """NullValidator方法调用返回空字典"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        nv = NullValidator("TestValidator")
        result = nv.some_method()
        assert result == {}, f"Expected empty dict, got {result}"

    def test_null_validator_repr(self):
        """NullValidator有可读的repr"""
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        nv = NullValidator("TestValidator")
        assert "NullValidator" in repr(nv)
        assert "TestValidator" in repr(nv)
