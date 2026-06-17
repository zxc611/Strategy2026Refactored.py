# MODULE_ID: M2-539
"""R4-5 断言测试: P1-35 json.dump→json_dumps+文件写入在3文件

验证项:
1. phase_scan_core.py不再有json.dump(
2. shadow_strategy_pnl_metrics.py不再有json.dump(
3. signal_history_service.py不再有json.dump(
4. 三个文件都使用json_dumps
"""
import sys
import os
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestJsonDumpUnified:
    """R4-5: json.dump→json_dumps+文件写入断言测试"""

    def test_phase_scan_core_no_json_dump(self):
        """验证phase_scan.py不再有json.dump("""
        path = os.path.join(_project_root, "param_pool", "optimization", "phase_scan.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        # 排除注释行
        code_lines = [l for l in source.split('\n')
                      if not l.strip().startswith('#') and 'json.dump(' in l]
        assert len(code_lines) == 0, \
            f"phase_scan.py仍有json.dump(): {code_lines}"

    def test_phase_scan_core_uses_json_dumps(self):
        """验证phase_scan.py使用json_dumps"""
        path = os.path.join(_project_root, "param_pool", "optimization", "phase_scan.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json_dumps' in source, "phase_scan.py应使用json_dumps"

    def test_shadow_pnl_metrics_no_json_dump(self):
        """验证shadow_strategy_pnl_metrics.py不再有json.dump("""
        path = os.path.join(_project_root, "strategy", "shadow_strategy_pnl.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        code_lines = [l for l in source.split('\n')
                      if not l.strip().startswith('#') and 'json.dump(' in l]
        assert len(code_lines) == 0, \
            f"shadow_strategy_pnl_metrics.py仍有json.dump(): {code_lines}"

    def test_shadow_pnl_metrics_uses_json_dumps(self):
        """验证shadow_strategy_pnl_metrics.py使用json_dumps"""
        path = os.path.join(_project_root, "strategy", "shadow_strategy_pnl.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json_dumps' in source, "shadow_strategy_pnl_metrics.py应使用json_dumps"

    def test_signal_history_no_json_dump(self):
        """验证signal_history_service.py不再有json.dump("""
        path = os.path.join(_project_root, "signal", "signal_history_service.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        code_lines = [l for l in source.split('\n')
                      if not l.strip().startswith('#') and 'json.dump(' in l]
        assert len(code_lines) == 0, \
            f"signal_history_service.py仍有json.dump(): {code_lines}"

    def test_signal_history_uses_json_dumps(self):
        """验证signal_history_service.py使用json_dumps"""
        path = os.path.join(_project_root, "signal", "signal_history_service.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'json_dumps' in source, "signal_history_service.py应使用json_dumps"
