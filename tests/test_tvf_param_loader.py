# MODULE_ID: M2-599
"""测试 config/tvf_param_loader.py — TVFParamLoader单例 + 参数校验"""
import os
import sys
import tempfile
import threading

import pytest

_project_root = os.path.join(os.path.dirname(__file__), '..', '..')
_project_root = os.path.abspath(_project_root)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def _reset_singleton():
    """重置TVFParamLoader单例"""
    from config.tvf_param_loader import TVFParamLoader
    TVFParamLoader.reset()
    try:
        from infra.registry_service import SingletonRegistry
        SingletonRegistry.reset_all()
    except Exception:
        pass


# ============================================================
# 单例模式
# ============================================================

class TestTVFParamLoaderSingleton:
    """TVFParamLoader 单例模式"""

    def setup_method(self):
        _reset_singleton()

    def teardown_method(self):
        _reset_singleton()

    def test_get_instance_returns_same_object(self):
        """get_instance两次返回同一对象"""
        from config.tvf_param_loader import TVFParamLoader
        a = TVFParamLoader.get_instance()
        b = TVFParamLoader.get_instance()
        assert a is b

    def test_constructor_returns_same_object(self):
        """构造函数两次返回同一对象"""
        from config.tvf_param_loader import TVFParamLoader
        a = TVFParamLoader()
        b = TVFParamLoader()
        assert a is b

    def test_constructor_equals_get_instance(self):
        """构造函数与get_instance返回同一对象"""
        from config.tvf_param_loader import TVFParamLoader
        a = TVFParamLoader()
        b = TVFParamLoader.get_instance()
        assert a is b

    def test_reset_produces_new_instance(self):
        """reset后新实例与旧实例不同"""
        from config.tvf_param_loader import TVFParamLoader
        old = TVFParamLoader.get_instance()
        TVFParamLoader.reset()
        new = TVFParamLoader.get_instance()
        assert old is not new

    def test_reset_clears_loaded_flag(self):
        """reset清除。loaded标志"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        loader.load('/nonexistent.yaml')
        assert loader._loaded is True
        TVFParamLoader.reset()
        # reset后新实例的指loaded应为False
        new_loader = TVFParamLoader.get_instance()
        assert new_loader._loaded is False

    def test_reset_clears_params(self):
        """reset清除。params"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        loader.load('/nonexistent.yaml')
        assert loader._params != {}
        TVFParamLoader.reset()
        new_loader = TVFParamLoader.get_instance()
        assert new_loader._params == {}

    def test_thread_safety_of_singleton(self):
        """多线程并发获取单例"""
        from config.tvf_param_loader import TVFParamLoader
        instances = []
        barrier = threading.Barrier(10)

        def get_inst():
            barrier.wait()
            instances.append(TVFParamLoader.get_instance())

        threads = [threading.Thread(target=get_inst) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 所有实例应该是同一个
        assert all(inst is instances[0] for inst in instances)


# ============================================================
# 默认参数
# ============================================================

class TestTVFDefaultParams:
    """TVFParamLoader 默认参数结构"""

    def setup_method(self):
        _reset_singleton()

    def teardown_method(self):
        _reset_singleton()

    def test_default_params_has_layer_weights(self):
        """默认参数包含layer_weights"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert 'layer_weights' in params
        assert 'l1_risk_return' in params['layer_weights']
        assert 'l2_market_micro' in params['layer_weights']
        assert 'l3_option_greeks' in params['layer_weights']

    def test_default_params_has_l1_tri_validation(self):
        """默认参数包含l1_tri_validation"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert 'l1_tri_validation' in params
        l1 = params['l1_tri_validation']
        assert 'inner_weights' in l1
        assert 'sortino_tri_scale' in l1
        assert 'calmar_tri_scale' in l1
        assert 'sharpe_tri_scale' in l1

    def test_default_params_has_l2_order_flow(self):
        """默认参数包含l2_order_flow"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert 'l2_order_flow' in params
        l2 = params['l2_order_flow']
        assert 'inner_weights' in l2
        assert 'ofi_scale' in l2
        assert 'smf_scale' in l2

    def test_default_params_has_l3_greeks(self):
        """默认参数包含l3_greeks"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert 'l3_greeks' in params
        l3 = params['l3_greeks']
        assert 'inner_weights' in l3
        assert 'gamma_scale' in l3
        assert 'theta_scale' in l3
        assert 'vega_scale' in l3

    def test_default_params_tvf_enabled_true(self):
        """默认参数tvf_enabled为True"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert params.get('tvf_enabled') is True

    def test_layer_weights_sum_to_one(self):
        """层间权重和为1.0"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        lw = params['layer_weights']
        total = lw['l1_risk_return'] + lw['l2_market_micro'] + lw['l3_option_greeks']
        assert abs(total - 1.0) < 1e-6

    def test_l1_inner_weights_sum_to_one(self):
        """L1层内权重和为1.0"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert abs(sum(params['l1_tri_validation']['inner_weights'].values()) - 1.0) < 1e-6

    def test_l2_inner_weights_sum_to_one(self):
        """L2层内权重和为1.0"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert abs(sum(params['l2_order_flow']['inner_weights'].values()) - 1.0) < 1e-6

    def test_l3_inner_weights_sum_to_one(self):
        """L3层内权重和为1.0"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        assert abs(sum(params['l3_greeks']['inner_weights'].values()) - 1.0) < 1e-6

    def test_all_scales_positive(self):
        """所有scale值为正数"""
        from config.tvf_param_loader import TVFParamLoader
        params = TVFParamLoader._default_params()
        scales = [
            params['l1_tri_validation']['sortino_tri_scale'],
            params['l1_tri_validation']['calmar_tri_scale'],
            params['l1_tri_validation']['sharpe_tri_scale'],
            params['l2_order_flow']['ofi_scale'],
            params['l2_order_flow']['smf_scale'],
            params['l3_greeks']['gamma_scale'],
            params['l3_greeks']['theta_scale'],
            params['l3_greeks']['vega_scale'],
        ]
        for s in scales:
            assert s > 0, f"scale值{s}应为正数"


# ============================================================
# 参数校验
# ============================================================

class TestTVFParamValidation:
    """TVFParamLoader 参数校验"""

    def setup_method(self):
        _reset_singleton()

    def teardown_method(self):
        _reset_singleton()

    def _make_valid_params(self):
        """构造合法参数"""
        from config.tvf_param_loader import TVFParamLoader
        return TVFParamLoader._default_params()

    def test_valid_params_pass_validation(self):
        """合法参数通过校验"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        loader._params = self._make_valid_params()
        # 不应抛异常
        loader._validate_params()

    def test_reject_layer_weights_not_sum_to_one(self):
        """层间权重和不为1.0时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['layer_weights']['l1_risk_return'] = 0.5
        params['layer_weights']['l2_market_micro'] = 0.5
        params['layer_weights']['l3_option_greeks'] = 0.5
        loader._params = params
        with pytest.raises(ValueError, match='层间权重和不等于1.0'):
            loader._validate_params()

    def test_reject_l1_inner_weights_not_sum_to_one(self):
        """L1层内权重和不为1.0时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['l1_tri_validation']['inner_weights'] = {'sortino': 0.5, 'calmar': 0.5, 'sharpe': 0.5}
        loader._params = params
        with pytest.raises(ValueError, match='L1层内权重和不等于1.0'):
            loader._validate_params()

    def test_reject_l2_inner_weights_not_sum_to_one(self):
        """L2层内权重和不为1.0时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['l2_order_flow']['inner_weights'] = {'ofi': 0.5, 'cvd_divergence': 0.5, 'smart_money_flow': 0.5}
        loader._params = params
        with pytest.raises(ValueError, match='L2层内权重和不等于1.0'):
            loader._validate_params()

    def test_reject_l3_inner_weights_not_sum_to_one(self):
        """L3层内权重和不为1.0时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['l3_greeks']['inner_weights'] = {'delta': 0.5, 'gamma': 0.5, 'theta': 0.5, 'vega': 0.5}
        loader._params = params
        with pytest.raises(ValueError, match='L3层内权重和不等于1.0'):
            loader._validate_params()

    def test_reject_negative_sortino_scale(self):
        """sortino_tri_scale为负数时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['l1_tri_validation']['sortino_tri_scale'] = -1.0
        loader._params = params
        with pytest.raises(ValueError, match='sortino_tri_scale必须为正数'):
            loader._validate_params()

    def test_reject_zero_ofi_scale(self):
        """ofi_scale为0时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['l2_order_flow']['ofi_scale'] = 0
        loader._params = params
        with pytest.raises(ValueError, match='ofi_scale必须为正数'):
            loader._validate_params()

    def test_reject_negative_vega_scale(self):
        """vega_scale为负数时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        params['l3_greeks']['vega_scale'] = -0.01
        loader._params = params
        with pytest.raises(ValueError, match='vega_scale必须为正数'):
            loader._validate_params()

    def test_reject_missing_layer_weights_key(self):
        """缺少layer_weights嵌套key时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        del params['layer_weights']
        loader._params = params
        with pytest.raises(ValueError, match='缺少必要嵌套key'):
            loader._validate_params()

    def test_reject_missing_l1_key(self):
        """缺少l1_tri_validation嵌套key时拒绝"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = self._make_valid_params()
        del params['l1_tri_validation']
        loader._params = params
        with pytest.raises(ValueError, match='缺少必要嵌套key'):
            loader._validate_params()


# ============================================================
# 加载/重载
# ============================================================

class TestTVFParamLoading:
    """TVFParamLoader 加载与重载"""

    def setup_method(self):
        _reset_singleton()

    def teardown_method(self):
        _reset_singleton()

    def test_load_missing_file_uses_defaults(self):
        """加载不存在的文件时使用默认参数"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        params = loader.load('/nonexistent/path/tvf_params.yaml')
        assert 'layer_weights' in params
        assert loader._loaded is True

    def test_load_sets_loaded_flag(self):
        """load设置。loaded标志"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        assert loader._loaded is False
        loader.load('/nonexistent.yaml')
        assert loader._loaded is True

    def test_load_valid_yaml_file(self):
        """加载合法YAML文件"""
        import yaml
        from config.tvf_param_loader import TVFParamLoader
        valid_params = TVFParamLoader._default_params()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            yaml.dump({'tvf_params': valid_params}, f)
            tmp_path = f.name
        try:
            loader = TVFParamLoader.get_instance()
            params = loader.load(tmp_path)
            assert 'layer_weights' in params
            assert loader._loaded is True
        finally:
            os.unlink(tmp_path)

    def test_reload_reloads_params(self):
        """reload重新加载参数"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        loader.load('/nonexistent1.yaml')
        assert loader._loaded is True
        params2 = loader.reload('/nonexistent2.yaml')
        assert loader._loaded is True
        assert isinstance(params2, dict)

    def test_get_params_auto_loads(self):
        """get_params在未加载时自动触发load"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        assert loader._loaded is False
        params = loader.get_params()
        assert loader._loaded is True
        assert isinstance(params, dict)
        assert 'layer_weights' in params

    def test_get_params_returns_current_params(self):
        """get_params返回当前已加载参数"""
        from config.tvf_param_loader import TVFParamLoader
        loader = TVFParamLoader.get_instance()
        loader.load('/nonexistent.yaml')
        params = loader.get_params()
        assert params is loader._params


# ============================================================
# 便捷函数
# ============================================================

class TestTVFConvenienceFunctions:
    """TVFParamLoader 便捷函数"""

    def setup_method(self):
        _reset_singleton()

    def teardown_method(self):
        _reset_singleton()

    def test_get_tvf_param_loader_returns_instance(self):
        """get_tvf_param_loader返回TVFParamLoader实例"""
        from config.tvf_param_loader import get_tvf_param_loader, TVFParamLoader
        loader = get_tvf_param_loader()
        assert isinstance(loader, TVFParamLoader)

    def test_load_tvf_params_returns_dict(self):
        """load_tvf_params返回参数字典"""
        from config.tvf_param_loader import load_tvf_params
        params = load_tvf_params('/nonexistent.yaml')
        assert isinstance(params, dict)
        assert 'layer_weights' in params
