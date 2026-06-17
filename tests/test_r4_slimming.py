# MODULE_ID: M2-540
"""R4轮端到端断言验证测试 — P2工程瘦身+死代码清理"""
import os
import sys
import pytest

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestR4_1_ToolScriptsMoved:
    """R4-1: strategy_judgment/ 工具脚本迁出"""

    def test_split_v2_moved(self):
        """_split_v2.py已从strategy_judgment/迁出"""
        assert not os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_split_v2.py')), "R4-1失败: _split_v2.py仍在strategy_judgment/"

    def test_split_content_moved(self):
        """_split_content.py已从strategy_judgment/迁出"""
        assert not os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_split_content.py')), "R4-1失败: _split_content.py仍在strategy_judgment/"

    def test_do_split_moved(self):
        """_do_split.py已从strategy_judgment/迁出"""
        assert not os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_do_split.py')), "R4-1失败: _do_split.py仍在strategy_judgment/"

    def test_gen_rest_moved(self):
        """_gen_rest.py已从strategy_judgment/迁出"""
        assert not os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_gen_rest.py')), "R4-1失败: _gen_rest.py仍在strategy_judgment/"

    def test_gen_all_moved(self):
        """_gen_all.py已从strategy_judgment/迁出"""
        assert not os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_gen_all.py')), "R4-1失败: _gen_all.py仍在strategy_judgment/"

    def test_scripts_in_new_location(self):
        """5个脚本已迁至scripts/judgment_codegen/"""
        scripts_dir = os.path.join(_PROJECT_ROOT, 'scripts', 'judgment_codegen')
        assert os.path.exists(os.path.join(scripts_dir, 'split_v2.py')), "R4-1失败: split_v2.py不在scripts/judgment_codegen/"
        assert os.path.exists(os.path.join(scripts_dir, 'split_content.py')), "R4-1失败: split_content.py不在scripts/judgment_codegen/"
        assert os.path.exists(os.path.join(scripts_dir, 'do_split.py')), "R4-1失败: do_split.py不在scripts/judgment_codegen/"
        assert os.path.exists(os.path.join(scripts_dir, 'gen_rest.py')), "R4-1失败: gen_rest.py不在scripts/judgment_codegen/"
        assert os.path.exists(os.path.join(scripts_dir, 'gen_all.py')), "R4-1失败: gen_all.py不在scripts/judgment_codegen/"


class TestR4_2_RiskDashboardServiceIntegrated:
    """R4-2: RiskDashboardService已从死代码变为被集成（T1.2验收）"""

    def test_class_exists_in_risk_service(self):
        """RiskDashboardService类存在于risk_service.py中"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'class RiskDashboardService' in src, "R4-2失败: RiskDashboardService类不存在"

    def test_get_risk_dashboard_exists(self):
        """get_risk_dashboard_service()函数存在于risk_service.py中"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'def get_risk_dashboard_service' in src, "R4-2失败: get_risk_dashboard_service()不存在"

    def test_risk_service_calls_dashboard(self):
        """RiskService.__init__中调用get_risk_dashboard_service()"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'get_risk_dashboard_service()' in src, "R4-2失败: RiskService未调用get_risk_dashboard_service()"

    def test_has_get_dashboard_service_method(self):
        """RiskService有get_dashboard_service()方法暴露dashboard"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'def get_dashboard_service' in src, "R4-2失败: RiskService缺少get_dashboard_service()方法"

    def test_in_risk_init_all(self):
        """RiskDashboardService在risk/__init__.py的__all__中"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', '__init__.py'), encoding='utf-8').read()
        assert 'RiskDashboardService' in src, "R4-2失败: RiskDashboardService不在risk/__init__.py"


class TestR4_4_ShadowModulePrivatePrefix:
    """R4-4: ShadowStrategyCore模块重构 — 新合并文件为真相源，shim标记DEPRECATED"""

    def test_core_merged_file_exists(self):
        """shadow_strategy_core.py 新合并文件存在（真相源）"""
        assert os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_core.py')), "R4-4失败: shadow_strategy_core.py 新合并文件不存在"
        # shim文件应标记DEPRECATED
        shim = os.path.join(_PROJECT_ROOT, 'strategy', '_shadow_strategy_core.py')
        if os.path.exists(shim):
            assert 'DEPRECATED' in open(shim, encoding='utf-8').read()[:200], "R4-4失败: _shadow_strategy_core.py shim未标记DEPRECATED"

    def test_signal_has_underscore_prefix(self):
        """shadow_strategy_signal.py已重命名为_shadow_strategy_signal.py"""
        assert os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy', '_shadow_strategy_signal.py')), "R4-4失败: _shadow_strategy_signal.py不存在"

    def test_pnl_merged_file_exists(self):
        """shadow_strategy_pnl.py 新合并文件存在（真相源）"""
        assert os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_pnl.py')), "R4-4失败: shadow_strategy_pnl.py 新合并文件不存在"
        # shim文件应标记DEPRECATED
        shim = os.path.join(_PROJECT_ROOT, 'strategy', '_shadow_strategy_pnl.py')
        if os.path.exists(shim):
            assert 'DEPRECATED' in open(shim, encoding='utf-8').read()[:200], "R4-4失败: _shadow_strategy_pnl.py shim未标记DEPRECATED"

    def test_facade_imports_correct_modules(self):
        """facade中的import路径正确（新合并文件无下划线前缀，signal保持下划线前缀）"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'from ali2026v3_trading.strategy.shadow_strategy_core import' in src, "R4-4失败: facade未导入shadow_strategy_core(新合并文件)"
        assert 'from ali2026v3_trading.strategy._shadow_strategy_signal import' in src, "R4-4失败: facade未更新_signal导入路径"
        assert 'from ali2026v3_trading.strategy.shadow_strategy_pnl import' in src, "R4-4失败: facade未导入shadow_strategy_pnl(新合并文件)"

    def test_facade_still_public(self):
        """facade自身仍为公共模块（无_前缀）"""
        assert os.path.exists(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py')), "R4-4失败: shadow_strategy_facade.py不应重命名"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])