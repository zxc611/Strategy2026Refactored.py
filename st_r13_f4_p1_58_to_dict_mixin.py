# MODULE_ID: M2-490
"""R5-F4: P1-58 to_dict方法重复实现统一 断言测试

验证运行时行为:
1. ToDictMixin在shared_utils.py中定义
2. ToDictMixin.to_dict()运行时行为正确
3. ToDictMixin正确处理嵌套对象
4. ToDictMixin跳过私有属性
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_to_dict_mixin_in_shared_utils():
    """P1-58验证: ToDictMixin在shared_utils.py中定义"""
    from ali2026v3_trading.infra.shared_utils import ToDictMixin
    assert callable(ToDictMixin)


def test_to_dict_mixin_basic_behavior():
    """P1-58验证: ToDictMixin.to_dict()运行时行为正确"""
    from ali2026v3_trading.infra.shared_utils import ToDictMixin

    class TestObj(ToDictMixin):
        def __init__(self):
            self.name = "test"
            self.value = 42
            self._private = "hidden"

    obj = TestObj()
    result = obj.to_dict()
    assert result == {'name': 'test', 'value': 42}, \
        f"to_dict应返回{{'name': 'test', 'value': 42}}，实际为{result}"
    assert '_private' not in result, "to_dict应跳过私有属性"


def test_to_dict_mixin_nested_objects():
    """P1-58验证: ToDictMixin正确处理嵌套对象"""
    from ali2026v3_trading.infra.shared_utils import ToDictMixin

    class Inner(ToDictMixin):
        def __init__(self):
            self.x = 1

    class Outer(ToDictMixin):
        def __init__(self):
            self.inner = Inner()
            self.items = [Inner(), Inner()]

    obj = Outer()
    result = obj.to_dict()
    assert 'inner' in result
    assert result['inner'] == {'x': 1}
    assert len(result['items']) == 2
    assert result['items'][0] == {'x': 1}


def test_to_dict_mixin_skips_private_attrs():
    """P1-58验证: ToDictMixin跳过私有属性"""
    from ali2026v3_trading.infra.shared_utils import ToDictMixin

    class TestObj(ToDictMixin):
        def __init__(self):
            self.public = "visible"
            self._private = "hidden"
            self.__dunder = "also_hidden"

    obj = TestObj()
    result = obj.to_dict()
    assert 'public' in result
    assert '_private' not in result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
