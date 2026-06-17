# MODULE_ID: M2-536
"""R4-2 断言测试: P1-32 safe_divide推广 — 替换内联除法保护

验证项:
1. safe_divide存在于resilience_numeric并可导入
2. _factory.py的_precompute_division使用safe_divide
3. order_flow_analyzer.py使用safe_divide替代内联保护
4. 两个文件不再有内联denominator==0检查
"""
import sys
import os
import re
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestSafeDividePromotion:
    """R4-2: safe_divide推广断言测试"""

    def test_safe_divide_importable(self):
        """验证safe_divide可从resilience_numeric导入"""
        from ali2026v3_trading.infra.resilience import safe_divide
        assert callable(safe_divide)

    def test_safe_divide_behavior(self):
        """验证safe_divide运行时行为正确"""
        from ali2026v3_trading.infra.resilience import safe_divide
        assert safe_divide(10.0, 2.0) == 5.0
        assert safe_divide(10.0, 0.0, default=0.0) == 0.0
        assert safe_divide(10.0, 0.0, default=-1.0) == -1.0

    def test_factory_uses_safe_divide(self):
        """验证strategy_ecosystem/__init__.py的_precompute_division使用safe_divide"""
        path = os.path.join(_project_root, "strategy", "strategy_ecosystem", "__init__.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'from ali2026v3_trading.infra.resilience import safe_divide' in source
        assert 'safe_divide(numerator, denominator' in source

    def test_factory_no_inline_denominator_check(self):
        """验证strategy_ecosystem/__init__.py不再有内联denominator==0.0检查（在_precompute_division内）"""
        path = os.path.join(_project_root, "strategy", "strategy_ecosystem", "__init__.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        # _precompute_division函数内不应有"if denominator == 0.0"
        func_match = re.search(
            r'def _precompute_division.*?(?=\ndef |\Z)', source, re.DOTALL
        )
        if func_match:
            func_body = func_match.group()
            assert 'denominator == 0.0' not in func_body, \
                "_precompute_division不应有内联denominator==0.0检查"

    def test_order_flow_analyzer_uses_safe_divide(self):
        """验证order_flow_analyzer.py导入并使用safe_divide"""
        path = os.path.join(_project_root, "order", "order_flow_analyzer.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'from ali2026v3_trading.infra.resilience import safe_divide' in source
        assert 'safe_divide(numerator, denominator' in source

    def test_order_flow_analyzer_no_inline_denominator_check(self):
        """验证order_flow_analyzer.py不再有内联if denominator == 0: return 0.0"""
        path = os.path.join(_project_root, "order", "order_flow_analyzer.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        # 不应有"if denominator == 0:"后跟"return 0.0"的模式
        pattern = r'if\s+denominator\s*==\s*0\s*:\s*\n\s*return\s+0\.0'
        matches = re.findall(pattern, source)
        assert len(matches) == 0, \
            f"order_flow_analyzer.py仍有{len(matches)}处内联denominator==0检查"
