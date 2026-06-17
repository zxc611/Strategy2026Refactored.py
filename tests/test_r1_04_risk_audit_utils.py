# MODULE_ID: M2-508
"""R1-4: 验证 infra/risk_audit_utils.py 断裂重导出已修复"""
import pytest


def test_no_archive_imports_in_production_code():
    """生产代码中不应有任何 from ali2026v3_trading.archive 导入"""
    import subprocess
    import os
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    result = subprocess.run(
        ['python', '-c', '''
import subprocess, os
r = subprocess.run(
    ["python", "-m", "ripgrep", "--no-filename", "-n",
     "from ali2026v3_trading.archive.", "ali2026v3_trading",
     "--glob", "!_backup_*", "--glob", "!*.md"],
    capture_output=True, text=True, cwd=os.getcwd())
print(r.stdout.strip())
'''],
        capture_output=True, text=True, cwd=root
    )
    # 简单方案：直接用 Grep 工具逻辑，这里用 python grep 替代
    import glob
    archive_refs = []
    for pyfile in glob.glob(os.path.join(root, 'ali2026v3_trading', '**', '*.py'), recursive=True):
        if '_backup_' in pyfile or 'archive' in pyfile or 'tests' in pyfile:
            continue
        try:
            with open(pyfile, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f, 1):
                    if 'from ali2026v3_trading.archive.' in line:
                        rel = os.path.relpath(pyfile, root)
                        archive_refs.append(f"{rel}:{i}")
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            continue

    assert len(archive_refs) == 0, \
        f"发现 {len(archive_refs)} 处 archive 导入残留: {archive_refs}"


def test_risk_audit_utils_importable():
    """risk_audit_utils 所有 __all__ 符号应可导入"""
    from ali2026v3_trading.infra import risk_audit_utils
    for name in risk_audit_utils.__all__:
        assert hasattr(risk_audit_utils, name), \
            f"risk_audit_utils 缺少符号: {name}"


def test_structured_audit_log_from_risk_audit_utils():
    """structured_audit_log 应从 risk_audit_utils 可用"""
    from ali2026v3_trading.infra.risk_audit_utils import structured_audit_log
    assert callable(structured_audit_log), \
        "structured_audit_log 应为可调用对象"


def test_alert_level_stub_works():
    """AlertLevel stub 应可正常使用"""
    from ali2026v3_trading.infra.risk_audit_utils import AlertLevel, alert
    assert AlertLevel.INFO is not None
    assert AlertLevel.WARNING is not None
    # alert 不应抛异常
    alert(AlertLevel.INFO, "test")
