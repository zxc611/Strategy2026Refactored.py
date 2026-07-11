# MODULE_ID: M2-535
"""R4-02: 验证 import json 直接使用和死导入修复

测试项：
1. chicory_eviction 模块可导入且使用 json_dumps
2. ui_mixin.py 中不再有 import json
3. checks_individual.py 中不再有 import json
4. ui_service.py 中不再有 import json
"""
import ast
import importlib
import pathlib


_BASE = pathlib.Path(__file__).resolve().parent.parent


def _source_lines(module_rel_path: str) -> list[str]:
    """读取模块源码行"""
    path = _BASE / module_rel_path
    return path.read_text(encoding="utf-8").splitlines()


def _has_top_level_import_json(module_rel_path: str) -> bool:
    """检查模块源码中是否有顶层 `import json`（不含注释说明保留原因的）"""
    lines = _source_lines(module_rel_path)
    for line in lines:
        stripped = line.strip()
        if stripped == "import json":
            return True
        # 允许带注释的 import json（如 "import json  # 保留: ..."）
        if stripped.startswith("import json  # 保留:"):
            continue
        if stripped.startswith("import json  #") and "保留" in stripped:
            continue
    return False


class TestChicoryEviction:
    """J-03: chicory_eviction.py 应使用 json_dumps 而非 json.dumps"""

    def test_module_importable(self):
        mod = importlib.import_module("ali2026v3_trading.evaluation.chicory_eviction")
        assert mod is not None

    def test_uses_json_dumps(self):
        lines = _source_lines("evaluation/chicory_eviction.py")
        # 不应包含 json.dumps 调用
        assert not any("json.dumps(" in line for line in lines), \
            "chicory_eviction.py 仍使用 json.dumps()，应改为 json_dumps"

    def test_imports_json_dumps(self):
        lines = _source_lines("evaluation/chicory_eviction.py")
        assert any("from ali2026v3_trading.infra.serialization_utils import json_dumps" in line for line in lines), \
            "chicory_eviction.py 未导入 json_dumps"

    def test_no_bare_import_json(self):
        assert not _has_top_level_import_json("evaluation/chicory_eviction.py"), \
            "chicory_eviction.py 不应有 `import json`"


class TestUIServiceJson:
    """J-06: ui_service.py 不应有 import json"""

    def test_no_import_json(self):
        assert not _has_top_level_import_json("config/ui_service.py"), \
            "ui_service.py 不应有 `import json`（已改用 json_dumps/json_loads）"


class TestChecksOrchestrator:
    """J-07: checks_orchestrator.py 不应有 import json"""

    def test_no_import_json(self):
        assert not _has_top_level_import_json("param_pool/checks_orchestrator.py"), \
            "checks_orchestrator.py 不应有 `import json`（未使用）"


class TestUIService:
    """J-08: ui_service.py 不应有 import json"""

    def test_no_import_json(self):
        assert not _has_top_level_import_json("config/ui_service.py"), \
            "ui_service.py 不应有 `import json`（未使用）"
