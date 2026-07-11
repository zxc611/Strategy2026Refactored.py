# MODULE_ID: M2-479
"""R3-F3: P1-45 配置热更新函数三重定义 断言测试

验证运行时行为:
1. resilience.check_hot_reload_compatibility存在且可调用
2. resilience.check_config_hot_reload已标记为deprecated
3. config_loader.check_config_hot_reload保持为权威热更新入口
4. config_service.reload_config保持为手动重载入口
5. 三个函数语义不同，不再混淆
"""
import os
import sys
import warnings
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_resilience_has_check_hot_reload_compatibility():
    """P1-45验证: resilience.check_hot_reload_compatibility存在且可调用"""
    from ali2026v3_trading.infra.resilience import check_hot_reload_compatibility
    assert callable(check_hot_reload_compatibility)
    result = check_hot_reload_compatibility({'max_risk_ratio': 0.5})
    assert result['can_hot_reload'] is False
    assert 'max_risk_ratio' in result['unsupported_keys']
    assert result['requires_restart'] is True


def test_resilience_old_name_deprecated():
    """P1-45验证: resilience.check_config_hot_reload已标记为deprecated"""
    from ali2026v3_trading.infra.resilience import check_config_hot_reload
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = check_config_hot_reload({'some_key': 1})
        assert len(w) >= 1, "应产生DeprecationWarning"
        assert issubclass(w[0].category, DeprecationWarning)
    assert result['can_hot_reload'] is True


def test_config_loader_check_config_hot_reload_is_file_watchdog():
    """P1-45验证: config_loader.check_config_hot_reload是文件watchdog入口"""
    from ali2026v3_trading.config.config_loader import check_config_hot_reload
    assert callable(check_config_hot_reload)
    # 验证签名：无参数（文件watchdog模式）'
    import inspect
    sig = inspect.signature(check_config_hot_reload)
    assert len(sig.parameters) == 0, \
        f"config_loader.check_config_hot_reload应无参数，实际有{len(sig.parameters)}个"


def test_config_service_reload_config_is_manual_reload():
    """P1-45验证: config_service.reload_config是手动重载入口"""
    from ali2026v3_trading.config.config_service import reload_config
    assert callable(reload_config)


def test_three_functions_have_different_signatures():
    """P1-45验证: 三个函数签名不同，不再混淆"""
    import inspect
    from ali2026v3_trading.infra.resilience import check_hot_reload_compatibility
    from ali2026v3_trading.config.config_loader import check_config_hot_reload as loader_fn
    from ali2026v3_trading.config.config_service import reload_config

    sig_compat = inspect.signature(check_hot_reload_compatibility)
    sig_loader = inspect.signature(loader_fn)
    sig_reload = inspect.signature(reload_config)

    # resilience版本接受Dict参数
    assert 'updates' in sig_compat.parameters
    # config_loader版本无参数
    assert len(sig_loader.parameters) == 0
    # config_service版本无参数
    assert len(sig_reload.parameters) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
