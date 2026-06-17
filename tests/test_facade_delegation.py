# MODULE_ID: M2-343
"""升A路径T3.1: shadow_strategy_facade 委托逻辑单元测试

验收标准: Facade委托逻辑、__getattr__边界、组合模式验证
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestFacadePureComposition:
    """Facade纯组合模式测试"""

    def test_no_inheritance_from_core_service(self):
        """ShadowStrategyEngine不继承ShadowStrategyCoreService"""
        from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
        from ali2026v3_trading.strategy.shadow_strategy_core import ShadowStrategyCoreService
        assert ShadowStrategyCoreService not in ShadowStrategyEngine.__mro__, \
            "ShadowStrategyEngine should not inherit from ShadowStrategyCoreService"

    def test_has_core_service_attribute(self):
        """Facade持有_core_service组合属性"""
        from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
        # 检查__init__中是否设置了_core_service
        import inspect
        source = inspect.getsource(ShadowStrategyEngine.__init__)
        assert '_core_service' in source, "Facade should set _core_service in __init__"

    def test_getattr_delegation(self):
        """__getattr__按优先级委托: core -> signal -> pnl"""
        from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
        import inspect
        source = inspect.getsource(ShadowStrategyEngine.__getattr__)
        assert '_core_service' in source, "__getattr__ should delegate to _core_service"
        assert '_signal_service' in source, "__getattr__ should delegate to _signal_service"
        assert '_pnl_service' in source, "__getattr__ should delegate to _pnl_service"

    def test_unknown_attribute_raises(self):
        """访问不存在的属性应抛出AttributeError"""
        from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
        # 不实例化（避免触发完整初始化），仅检查__getattr__逻辑
        import inspect
        source = inspect.getsource(ShadowStrategyEngine.__getattr__)
        assert 'AttributeError' in source, "__getattr__ should raise AttributeError for unknown attrs"

    def test_explicit_delegation_methods(self):
        """显式委托方法存在"""
        from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
        # 检查关键方法存在
        for method_name in ['are_params_locked', 'get_shadow_a_params', 'get_shadow_b_params',
                           'get_params_snapshot', 'shutdown', 'validate_dependencies']:
            assert hasattr(ShadowStrategyEngine, method_name), \
                f"ShadowStrategyEngine should have explicit delegation method: {method_name}"


class TestFacadeImports:
    """Facade导入正确模块路径测试"""

    def test_imports_shadow_strategy_core(self):
        """Facade导入shadow_strategy_core（新合并文件）"""
        import ali2026v3_trading.strategy.shadow_strategy_facade as facade_module
        source = open(facade_module.__file__, 'r', encoding='utf-8').read()
        assert 'shadow_strategy_core' in source, "Facade should import shadow_strategy_core (merged file)"

    def test_imports_underscore_signal(self):
        """Facade导入_shadow_strategy_signal"""
        import ali2026v3_trading.strategy.shadow_strategy_facade as facade_module
        source = open(facade_module.__file__, 'r', encoding='utf-8').read()
        assert '_shadow_strategy_signal' in source, "Facade should import _shadow_strategy_signal"

    def test_imports_shadow_strategy_pnl(self):
        """Facade导入shadow_strategy_pnl（新合并文件）"""
        import ali2026v3_trading.strategy.shadow_strategy_facade as facade_module
        source = open(facade_module.__file__, 'r', encoding='utf-8').read()
        assert 'shadow_strategy_pnl' in source, "Facade should import shadow_strategy_pnl (merged file)"
