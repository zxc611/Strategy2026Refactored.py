# MODULE_ID: M2-514
"""R2-2: 验证 _disk_monitor.py 使用 atomic_replace_file 替代内联 os.replace"""
import inspect
import pytest


def test_disk_monitor_uses_atomic_replace():
    """DiskSpaceMonitor._save_history 应使用 atomic_replace_file"""
    from ali2026v3_trading.infra._helpers import DiskSpaceMonitor
    source = inspect.getsource(DiskSpaceMonitor._save_history)
    # 检查有 atomic_replace_file 调用（非注释行）
    code_lines = [l for l in source.splitlines() if not l.strip().startswith('#')]
    code_text = '\n'.join(code_lines)
    assert 'atomic_replace_file' in code_text, \
        "_save_history 应调用 atomic_replace_file"
    # 检查没有 os.replace 作为语句调用（排除注释中的提及）'
    import re
    os_replace_calls = re.findall(r'(?<![#\w])os\.replace\s*\(', code_text)
    assert len(os_replace_calls) == 0, \
        f"_save_history 不应有 os.replace() 调用，发现 {len(os_replace_calls)} 处"


def test_disk_monitor_imports_atomic_replace():
    """_helpers (merged from _disk_monitor) 应从 shared_utils 导入 atomic_replace_file"""
    import ali2026v3_trading.infra._helpers as mod
    assert hasattr(mod, 'DiskSpaceMonitor'), \
        "_helpers 模块应包含 DiskSpaceMonitor"


def test_no_manual_tmp_file_in_save():
    """_save_history 不应手动创建 .tmp 临时文件"""
    from ali2026v3_trading.infra._helpers import DiskSpaceMonitor
    source = inspect.getsource(DiskSpaceMonitor._save_history)
    assert '.tmp' not in source, \
        "_save_history 不应手动创建 .tmp 临时文件（atomic_replace_file 内部管理）"
