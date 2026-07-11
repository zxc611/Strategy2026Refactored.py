# MODULE_ID: M2-524
"""R3-1: 验证 yaml.safe_load 3处残留已替换为 yaml_safe_load"""
import glob
import os
import pytest


def test_no_raw_yaml_safe_load_in_production():
    """生产代码中不应有直接使用 yaml.safe_load 的调用（serialization_utils内部除外）"""
    root = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'ali2026v3_trading')
    violations = []
    for pyfile in glob.glob(os.path.join(root, '**', '*.py'), recursive=True):
        if '_backup_' in pyfile or 'param_pool_backup' in pyfile or 'serialization_utils.py' in pyfile or 'tests' in pyfile:
            continue
        with open(pyfile, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                # 检测 yaml.safe_load( 或 _yaml.safe_load( 调用
                stripped = line.strip()
                if stripped.startswith('#'):
                    continue
                if 'yaml.safe_load(' in line or '_yaml.safe_load(' in line:
                    rel = os.path.relpath(pyfile, root)
                    violations.append(f"{rel}:{i}")
    assert len(violations) == 0, \
        f"发现 {len(violations)} 处 yaml.safe_load 残留: {violations}"


def test_yaml_safe_load_importable():
    """yaml_safe_load 应从 serialization_utils 可导入"""
    from ali2026v3_trading.infra.serialization_utils import yaml_safe_load
    assert callable(yaml_safe_load)


def test_enhanced_phase_scan_uses_yaml_safe_load():
    """phase_scan.py (merged from enhanced_phase_scan_gates) 应使用 yaml_safe_load"""
    import ali2026v3_trading.param_pool.optimization.phase_scan as mod
    source = open(mod.__file__, 'r', encoding='utf-8').read()
    assert 'yaml_safe_load' in source, "phase_scan.py 应使用 yaml_safe_load"
