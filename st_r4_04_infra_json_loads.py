# MODULE_ID: M2-538
"""R4-4 断言测试: P1-35 json.load→json_loads在infra层3文件

验证项:
1. _disk_monitor.py不再有json.load(f)
2. event_bus.py不再有json.load(f)
3. _backup_restore.py不再有json.load(f)
4. 三个文件都导入了json_loads
"""
import sys
import os
import re
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestInfraJsonLoads:
    """R4-4: infra层json.load→json_loads断言测试"""

    FILES = [
        ('infra/_disk_monitor.py', '_disk_monitor'),
        ('infra/event_bus.py', 'event_bus'),
        ('infra/_backup_restore.py', '_backup_restore'),
    ]

    def test_disk_monitor_no_json_load(self):
        """验证验disk_monitor.py不再有json.load(f)"""
        path = os.path.join(_project_root, "infra", "storage_service.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json.load(' not in source, "_disk_monitor.py不应有json.load()"

    def test_disk_monitor_imports_json_loads(self):
        """验证验disk_monitor.py导入了json_loads"""
        path = os.path.join(_project_root, "infra", "storage_service.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json_loads' in source, "_disk_monitor.py应导入json_loads"

    def test_event_bus_no_json_load(self):
        """验证event_bus.py不再有json.load(f)"""
        path = os.path.join(_project_root, "infra", "event_bus.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json.load(' not in source, "event_bus.py不应有json.load()"

    def test_event_bus_imports_json_loads(self):
        """验证event_bus.py导入了json_loads"""
        path = os.path.join(_project_root, "infra", "event_bus.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json_loads' in source, "event_bus.py应导入json_loads"

    def test_backup_restore_no_json_load(self):
        """验证验backup_restore.py不再有json.load(f)"""
        path = os.path.join(_project_root, "infra", "storage_service.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json.load(' not in source, "_backup_restore.py不应有json.load()"

    def test_backup_restore_imports_json_loads(self):
        """验证验backup_restore.py导入了json_loads"""
        path = os.path.join(_project_root, "infra", "storage_service.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json_loads' in source, "_backup_restore.py应导入json_loads"
