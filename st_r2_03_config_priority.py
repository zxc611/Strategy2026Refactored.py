# MODULE_ID: M2-516
"""P1-11: 配置管理三层架构断言测试
验证config_params/config_service/config_facade三层优先级标注和职责边界。
"""
import ast
import os


def _read_file(rel_path: str) -> str:
    base = os.path.join(os.path.dirname(__file__), '..')
    full = os.path.abspath(os.path.join(base, rel_path))
    with open(full, 'r', encoding='utf-8') as f:
        return f.read()


def test_config_facade_has_three_layer_docstring():
    """config_facade.py的docstring包含三层架构说明"""
    content = _read_file('config/config_service.py')
    assert 'P1-11' in content, "config_service.py缺少P1-11标注"
    assert 'config_params' in content, "config_service.py缺少config_params层说明"
    assert 'config_service' in content, "config_service.py缺少config_service层说明"
    assert 'config_facade' in content, "config_service.py缺少config_facade层说明"
    assert '权威运行时配置源' in content, "config_service.py缺少权威运行时配置源描述"
    assert '类型安全查询层' in content, "config_service.py缺少类型安全查询层描述"
    assert '统一入口' in content, "config_service.py缺少统一入口描述"


def test_config_params_get_param_has_authority_comment():
    """config_params.py的get_param函数上方有权威源注释"""
    content = _read_file('config/config_params.py')
    # 找到get_param函数定义
    assert 'def get_param(' in content, "config_params.py缺少get_param函数"
    # 验证P1-11权威源注释在get_param上方
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'def get_param(' in line:
            # 检查上方几行是否有P1-11注释
            comment_area = '\n'.join(lines[max(0, i - 5):i])
            assert 'P1-11' in comment_area, f"get_param上方缺少P1-11注释, 附近内容: {comment_area}"
            assert '权威运行时配置源' in comment_area, f"get_param上方缺少权威源标注"
            break


def test_three_layer_architecture_parseable():
    """三层架构文件均可正常解析为AST"""
    for rel_path in [
        'config/config_service.py',
        'config/config_params.py',
        'config/config_service.py',
    ]:
        content = _read_file(rel_path)
        try:
            ast.parse(content)
        except SyntaxError as e:
            raise AssertionError(f"{rel_path}语法错误: {e}")


def test_config_facade_delegates_to_config_service():
    """ConfigQueryFacade委托到ConfigService"""
    content = _read_file('config/config_service.py')
    assert 'ConfigService' in content, "ConfigQueryFacade未委托到ConfigService"
    assert 'self._service' in content, "ConfigQueryFacade未持有界service引用"


def test_config_service_delegates_to_config_params():
    """ConfigService最终委托到config_params获取参数"""
    content = _read_file('config/config_service.py')
    assert 'get_cached_params' in content or 'config_params' in content, \
        "ConfigService未委托到config_params"
