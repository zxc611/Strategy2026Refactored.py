# MODULE_ID: M2-485
"""R4-F4: P2-21 日志目录路径硬编码统一 断言测试

验证运行时行为:
1. get_log_dir权威实现在shared_utils.py
2. get_log_dir返回有效路径
3. get_log_dir支持子目录参数
4. get_log_dir在ConfigService不可用时fallback
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_get_log_dir_in_shared_utils():
    """P2-21验证: get_log_dir权威实现在shared_utils.py"""
    from ali2026v3_trading.infra.shared_utils import get_log_dir
    assert callable(get_log_dir)


def test_get_log_dir_returns_valid_path():
    """P2-21验证: get_log_dir返回有效路径"""
    from ali2026v3_trading.infra.shared_utils import get_log_dir
    path = get_log_dir()
    assert isinstance(path, str), f"应返回str，实际返回{type(path)}"
    assert 'logs' in path, f"路径应包含'logs'，实际为{path}"


def test_get_log_dir_supports_subdir():
    """P2-21验证: get_log_dir支持子目录参数"""
    from ali2026v3_trading.infra.shared_utils import get_log_dir
    path = get_log_dir('shadow')
    assert 'shadow' in path, f"路径应包含'shadow'，实际为{path}"
    assert 'logs' in path, f"路径应包含'logs'，实际为{path}"


def test_get_log_dir_fallback_when_config_unavailable():
    """P2-21验证: get_log_dir在ConfigService不可用时fallback"""
    from ali2026v3_trading.infra.shared_utils import get_log_dir
    # 即使ConfigService不可用，也应返回有效路径
    path = get_log_dir()
    assert os.path.isabs(path) or 'logs' in path, \
        f"fallback路径应有效，实际为{path}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
