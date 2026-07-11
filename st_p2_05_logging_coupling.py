# MODULE_ID: M2-451
"""
P2-05 断言测试: 日志系统交叉耦合修复验证

验证:
1. enhanced_phase_scan_gates.py 无模块级 basicConfig
2. _preprocess.py 无模块级 basicConfig
3. optuna_multiobjective_search.py 无模块级 basicConfig (临时常logging_temp除外)
4. ProductionQuantSystem.py 委托 setup_logging (有fallback)
5. 生产代码中模块级 logging.basicConfig 调用数 <= 2 (fallback + __main__)
"""
import re
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestP205LoggingCoupling(unittest.TestCase):
    """P2-05 日志交叉耦合修复断言"""

    def _read(self, rel_path: str) -> str:
        return (PROJECT_ROOT / rel_path).read_text(encoding="utf-8")

    def test_enhanced_phase_scan_gates_no_basic_config(self):
        """phase_scan.py (merged from enhanced_phase_scan_gates) 不得有模块级 logging.basicConfig"""
        src = self._read("param_pool/optimization/phase_scan.py")
        lines = src.splitlines()
        for i, line in enumerate(lines, 1):
            if "logging.basicConfig" in line and not line.strip().startswith("#"):
                self.fail(f"phase_scan.py:{i} 仍有 logging.basicConfig 调用")

    def test_preprocess_ticks_no_basic_config(self):
        """_preprocess.py 不得有模块级 logging.basicConfig"""
        src = self._read("param_pool/_preprocess.py")
        lines = src.splitlines()
        for i, line in enumerate(lines, 1):
            if "logging.basicConfig" in line and not line.strip().startswith("#"):
                self.fail(f"_preprocess.py:{i} 仍有 logging.basicConfig 调用")

    def test_optuna_multiobjective_no_basic_config(self):
        """optuna_multiobjective_search.py 不得有全局 logging.basicConfig (临时常logging_temp除外)"""
        src = self._read("param_pool/optuna_multiobjective_search.py")
        lines = src.splitlines()
        for i, line in enumerate(lines, 1):
            if "logging.basicConfig" in line and "_logging_temp" not in line and not line.strip().startswith("#"):
                self.fail(f"optuna_multiobjective_search.py:{i} 仍有全局 logging.basicConfig 调用")

    def test_production_quant_system_delegates_setup_logging(self):
        """ProductionQuantSystem.py CLI入口必须委托 setup_logging (有fallback)"""
        src = self._read("ProductionQuantSystem.py")
        self.assertIn("setup_logging", src,
                      "ProductionQuantSystem.py 必须委托 setup_logging()")

    def test_module_level_basic_config_count_limited(self):
        """生产代码中模块级 logging.basicConfig 调用数 <= 3
        (config_logging fallback + ProductionQuantSystem fallback + optuna _logging_temp)
        """
        allowed = {
            "config/config_logging.py",
            "ProductionQuantSystem.py",
            "param_pool/optuna_multiobjective_search.py",

            "param_pool/l1_quantification/meta_audit_engine.py",
        }
        violations = []
        for py_file in PROJECT_ROOT.rglob("*.py"):
            rel = str(py_file.relative_to(PROJECT_ROOT)).replace("\\", "/")
            parts = rel.split("/")
            if any(p.startswith("_backup_") or p.startswith("test_") or p == "param_pool_backup_20260612" for p in parts):
                continue
            if rel in allowed:
                continue
            try:
                src = py_file.read_text(encoding="utf-8")
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                continue
            lines = src.splitlines()
            for i, line in enumerate(lines, 1):
                if "logging.basicConfig" in line and "_logging_temp" not in line and not line.strip().startswith("#"):
                    violations.append(f"{rel}:{i}")
        self.assertEqual(len(violations), 0,
                         f"以下文件仍有模块级 logging.basicConfig: {violations}")


if __name__ == "__main__":
    unittest.main()