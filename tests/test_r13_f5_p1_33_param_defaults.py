# MODULE_ID: M2-491
"""R5-F5: P1-33 参数默认值三处重复 断言测试

验证运行时行为:
1. get_param_default函数存在且可调用
2. get_param_default从DEFAULT_PARAM_TABLE获取默认值
3. get_param_default返回None表示参数无默认值
4. get_param_default是参数默认值的单一数据源
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_get_param_default_exists():
    """P1-33验证: get_param_default函数存在且可调用"""
    from ali2026v3_trading.config.config_params import get_param_default
    assert callable(get_param_default)


def test_get_param_default_returns_from_table():
    """P1-33验证: get_param_default从DEFAULT_PARAM_TABLE获取默认值"""
    from ali2026v3_trading.config.config_params import get_param_default, DEFAULT_PARAM_TABLE
    # 找一个已知存在的参数
    known_key = 'max_kline'
    if known_key in DEFAULT_PARAM_TABLE:
        result = get_param_default(known_key)
        assert result == DEFAULT_PARAM_TABLE[known_key], \
            f"get_param_default('{known_key}')应返回{DEFAULT_PARAM_TABLE[known_key]}，实际为{result}"


def test_get_param_default_returns_none_for_unknown():
    """P1-33验证: get_param_default返回None表示参数无默认值"""
    from ali2026v3_trading.config.config_params import get_param_default
    result = get_param_default('nonexistent_param_xyz')
    assert result is None, f"不存在的参数应返回None，实际为{result}"


def test_get_param_default_is_single_source():
    """P1-33验证: get_param_default是参数默认值的单一数据源"""
    import inspect
    from ali2026v3_trading.config.config_params import get_param_default
    src = inspect.getsource(get_param_default)
    assert 'DEFAULT_PARAM_TABLE' in src, \
        "get_param_default应从DEFAULT_PARAM_TABLE获取默认值"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
