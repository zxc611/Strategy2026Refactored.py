# MODULE_ID: M2-522
"""R2轮端到端断言验证测试 — P1架构对齐修复"""
import ast
import os
import sys
import pytest

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestR2_1_FacadePureComposition:
    """R2-1: ShadowStrategyFacade改为纯组合（消除继承）"""

    def test_no_inheritance(self):
        """ShadowStrategyEngine不再继承ShadowStrategyCoreService"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'class ShadowStrategyEngine(ShadowStrategyCoreService)' not in src, "R2-1失败: 仍使用继承"

    def test_pure_composition_class(self):
        """ShadowStrategyEngine使用纯组合class定义"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'class ShadowStrategyEngine:' in src, "R2-1失败: 未使用纯组合class定义"

    def test_core_service_composition(self):
        """通过组合持有_core_service"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'self._core_service = ShadowStrategyCoreService' in src, "R2-1失败: 未通过组合持有_core_service"

    def test_explicit_delegation_validate(self):
        """显式委托validate_dependencies方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'def validate_dependencies(self)' in src, "R2-1失败: 缺少validate_dependencies显式委托"

    def test_explicit_delegation_shutdown(self):
        """显式委托shutdown方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'def shutdown(self' in src, "R2-1失败: 缺少shutdown显式委托"

    def test_explicit_delegation_get_params(self):
        """显式委托get_shadow_a_params/get_shadow_b_params方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'def get_shadow_a_params(self)' in src, "R2-1失败: 缺少get_shadow_a_params显式委托"
        assert 'def get_shadow_b_params(self)' in src, "R2-1失败: 缺少get_shadow_b_params显式委托"

    def test_explicit_delegation_relock(self):
        """显式委托relock_params方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'def relock_params(self' in src, "R2-1失败: 缺少relock_params显式委托"

    def test_no_mixin_import(self):
        """不再导入Mixin类"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_facade.py'), encoding='utf-8').read()
        assert 'ShadowStrategyCoreMixin' not in src, "R2-1失败: 仍导入CoreMixin"
        assert 'ShadowStrategySignalMixin' not in src, "R2-1失败: 仍导入SignalMixin"
        assert 'ShadowStrategyPnLMixin' not in src, "R2-1失败: 仍导入PnLMixin"


class TestR2_2_CascadeJudgeNamingAlignment:
    """R2-2: CascadeJudge架构命名与手册V7.5对齐"""

    def test_l1_rule_engine_method(self):
        """CascadeJudge有gate_l1_rule_engine方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'evaluation', 'cascade_judge.py'), encoding='utf-8').read()
        assert 'def gate_l1_rule_engine(self' in src, "R2-2失败: 缺少gate_l1_rule_engine方法"

    def test_l2_ml_model_method(self):
        """CascadeJudge有gate_l2_ml_model方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'evaluation', 'cascade_judge.py'), encoding='utf-8').read()
        assert 'def gate_l2_ml_model(self' in src, "R2-2失败: 缺少gate_l2_ml_model方法"

    def test_l3_shadow_stealth_method(self):
        """CascadeJudge有gate_l3_shadow_stealth方法"""
        src = open(os.path.join(_PROJECT_ROOT, 'evaluation', 'cascade_judge.py'), encoding='utf-8').read()
        assert 'def gate_l3_shadow_stealth(self' in src, "R2-2失败: 缺少gate_l3_shadow_stealth方法"

    def test_docstring_has_v75_mapping(self):
        """文档注释包含V7.5映射关系"""
        src = open(os.path.join(_PROJECT_ROOT, 'evaluation', 'cascade_judge.py'), encoding='utf-8').read()
        assert 'L1 规则引擎' in src, "R2-2失败: 文档缺少L1规则引擎映射"
        assert 'L2 ML模型' in src, "R2-2失败: 文档缺少L2 ML模型映射"
        assert 'L3 影子隐形' in src, "R2-2失败: 文档缺少L3影子隐形映射"

    def test_l1_docstring(self):
        """gate_l1_rule_engine有L1规则引擎docstring"""
        src = open(os.path.join(_PROJECT_ROOT, 'evaluation', 'cascade_judge.py'), encoding='utf-8').read()
        assert 'L1规则引擎' in src, "R2-2失败: gate_l1_rule_engine缺少L1规则引擎docstring"


class TestR2_3_RiskDelegationUnification:
    """R2-3: 风控委托链统一（消除全局单例绕路）"""

    def test_check_regulatory_uses_check_service(self):
        """check_regulatory_compliance走_check_service而非全局单例"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'self._check_service.check_regulatory_compliance' in src, "R2-3失败: check_regulatory_compliance未走_check_service"

    def test_check_capital_uses_check_service(self):
        """check_capital_sufficiency走_check_service而非全局单例"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'self._check_service.check_capital_sufficiency' in src, "R2-3失败: check_capital_sufficiency未走_check_service"

    def test_check_exchange_uses_check_service(self):
        """check_exchange_status走_check_service而非全局单例"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        assert 'self._check_service.check_exchange_status' in src, "R2-3失败: check_exchange_status未走_check_service"

    def test_no_get_safety_meta_layer_in_three_methods(self):
        """三个方法不再直接调用get_safety_meta_layer()"""
        src = open(os.path.join(_PROJECT_ROOT, 'risk', 'risk_service.py'), encoding='utf-8').read()
        # 在check_regulatory_compliance/check_capital_sufficiency/check_exchange_status方法体中
        # 不应出现 get_safety_meta_layer()
        # 简化检查：确认没有 _layer = get_safety_meta_layer() 模式
        lines = src.split('\n')
        in_target_method = False
        for line in lines:
            if 'def check_regulatory_compliance' in line or 'def check_capital_sufficiency' in line or 'def check_exchange_status' in line:
                in_target_method = True
            elif in_target_method and line.strip().startswith('def ') and 'check_' not in line:
                in_target_method = False
            if in_target_method and 'get_safety_meta_layer()' in line:
                pytest.fail(f"R2-3失败: 三方法中仍有get_safety_meta_layer()调用: {line.strip()}")


class TestR2_4_ParamTripleSourceAudit:
    """R2-4: 参数三源一致性（YAML为唯一真相源）"""

    def test_yaml_is_single_source(self):
        """_load_absolute_ev_floor强制YAML为唯一源"""
        # _shadow_strategy_core.py was merged into shadow_strategy_core.py
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_core.py'), encoding='utf-8').read()
        assert 'YAML为唯一参数真相源' in src, "R2-4失败: _load_absolute_ev_floor未声明YAML为唯一源"

    def test_no_hardcoded_fallback(self):
        """禁止硬编码fallback"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'shadow_strategy_core.py'), encoding='utf-8').read()
        # 在_load_absolute_ev_floor中不应有 self._absolute_ev_floor = self.ABSOLUTE_EV_FLOOR 的fallback
        lines = src.split('\n')
        in_load_method = False
        for i, line in enumerate(lines, 1):
            if '_load_absolute_ev_floor' in line and 'def ' in line:
                in_load_method = True
            elif in_load_method and line.strip().startswith('def ') and '_load' not in line:
                in_load_method = False
            if in_load_method and 'self.ABSOLUTE_EV_FLOOR' in line and 'raise' not in line:
                pytest.fail(f"R2-4失败: _load_absolute_ev_floor仍有硬编码fallback: 行{i}: {line.strip()}")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])