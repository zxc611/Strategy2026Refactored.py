# MODULE_ID: M2-521
"""P1-23 + P2-12: YAML/JSON双源 + 重试模式统一断言测试
验证JSON权威源标注和重试模式权威实现标注存在。
"""
import ast
import os


def _read_file(rel_path: str) -> str:
    base = os.path.join(os.path.dirname(__file__), '..')
    full = os.path.abspath(os.path.join(base, rel_path))
    with open(full, 'r', encoding='utf-8') as f:
        return f.read()


# ========== P1-23: YAML/JSON双源 ==========

def test_json_loader_has_authority_source_comment():
    """config_loader.py的load_default_params_from_json上方有权威源注释"""
    content = _read_file('config/config_loader.py')
    assert 'P1-23' in content, "config_loader.py缺少P1-23标注"
    assert '权威配置源' in content or '权威持久化源' in content, \
        "config_loader.py缺少权威源标注"
    # 确认注释在load_default_params_from_json上方
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'def load_default_params_from_json' in line:
            comment_area = '\n'.join(lines[max(0, i - 5):i])
            assert 'P1-23' in comment_area, \
                "load_default_params_from_json上方缺少P1-23注释"
            break


def test_json_loader_mentions_yaml_relationship():
    """config_loader.py标注了YAML与JSON的关系"""
    content = _read_file('config/config_loader.py')
    assert 'YAML' in content or 'cascade_threshold_grid' in content, \
        "config_loader.py未标注YAML关系"


def test_json_loader_mentions_priority():
    """config_loader.py标注了配置加载优先级"""
    content = _read_file('config/config_loader.py')
    assert '优先级' in content or 'priority' in content.lower(), \
        "config_loader.py未标注配置加载优先级"


# ========== P2-12: 重试模式统一 ==========

def test_resilience_retry_has_authority_comment():
    """resilience.py有权威重试实现注释"""
    content = _read_file('infra/resilience.py')
    assert 'P2-12' in content, "resilience.py缺少P2-12标注"
    assert '权威重试实现' in content, "resilience.py缺少权威重试实现标注"


def test_resilience_retry_lists_non_authority_implementations():
    """resilience.py列出了已知非权威重试实现"""
    content = _read_file('infra/resilience.py')
    assert 'NetworkRetryManager' in content, \
        "resilience.py未列出NetworkRetryManager为非权威实现"
    assert '_execute_with_retry_and_timeout' in content, \
        "resilience.py未列出后execute_with_retry_and_timeout为非权威实现"


def test_shared_utils_retry_has_delegation_todo():
    """shared_utils.py的retry_with_limit已委托到resilience.BoundedRetry"""
    content = _read_file('infra/shared_utils.py')
    assert 'BoundedRetry' in content, \
        "shared_utils.py的retry_with_limit未委托到BoundedRetry"
    assert 'resilience' in content, \
        "shared_utils.py的retry_with_limit未指向resilience"


def test_order_persistence_retry_has_delegation_todo():
    """order_persistence.py的NetworkRetryManager已使用BoundedRetry"""
    content = _read_file('order/order_persistence.py')
    assert 'BoundedRetry' in content, \
        "order_persistence.py的NetworkRetryManager未使用BoundedRetry"
    assert 'resilience' in content, \
        "order_persistence.py未指向resilience"


def test_ops_framework_retry_has_delegation_todo():
    """ops_service.py的指execute_with_retry_and_timeout已使用BoundedRetry"""
    content = _read_file('infra/ops_service.py')
    assert 'BoundedRetry' in content, \
        "ops_service.py的指execute_with_retry_and_timeout未使用BoundedRetry"
    assert 'resilience' in content, \
        "ops_service.py未指向resilience"


def test_position_command_retry_has_delegation_todo():
    """position_command_service.py的指schedule_close_retry已使用BoundedRetry"""
    content = _read_file('position/position_command_service.py')
    assert 'BoundedRetry' in content, \
        "position_command_service.py的指schedule_close_retry未使用BoundedRetry"
    assert 'resilience' in content, \
        "position_command_service.py未指向resilience"


def test_all_modified_files_parseable():
    """所有修改的文件均可正常解析为AST"""
    for rel_path in [
        'config/config_loader.py',
        'infra/resilience.py',
        'infra/shared_utils.py',
        'order/order_persistence.py',
        'infra/_ops_framework.py',
        'position/position_command_service.py',
    ]:
        content = _read_file(rel_path)
        try:
            ast.parse(content)
        except SyntaxError as e:
            raise AssertionError(f"{rel_path}语法错误: {e}")
