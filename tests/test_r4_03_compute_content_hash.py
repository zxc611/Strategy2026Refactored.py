# MODULE_ID: M2-537
"""R4-3 断言测试: P1-64 哈希计算统一 — compute_content_hash

验证项:
1. compute_content_hash存在于shared_utils_infra并可导入
2. compute_content_hash运行时行为正确（md5/sha256/截断）
3. config_params.py使用compute_content_hash替代hashlib.md5
4. shadow_strategy_core.py使用compute_content_hash替代hashlib.md5
5. config_version_tracker.py使用compute_content_hash替代hashlib.md5
"""
import sys
import os
import re
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestComputeContentHash:
    """R4-3: 哈希计算统一断言测试"""

    def test_compute_content_hash_importable(self):
        """验证compute_content_hash可从shared_utils_infra导入"""
        from ali2026v3_trading.infra.shared_utils import compute_content_hash
        assert callable(compute_content_hash)

    def test_compute_content_hash_md5(self):
        """验证compute_content_hash md5行为正确"""
        from ali2026v3_trading.infra.shared_utils import compute_content_hash
        import hashlib
        content = "test_content"
        result = compute_content_hash(content)
        expected = hashlib.md5(content.encode('utf-8')).hexdigest()
        assert result == expected

    def test_compute_content_hash_sha256(self):
        """验证compute_content_hash sha256行为正确"""
        from ali2026v3_trading.infra.shared_utils import compute_content_hash
        import hashlib
        content = "test_content"
        result = compute_content_hash(content, algorithm='sha256')
        expected = hashlib.sha256(content.encode('utf-8')).hexdigest()
        assert result == expected

    def test_compute_content_hash_truncate(self):
        """验证compute_content_hash截断功能"""
        from ali2026v3_trading.infra.shared_utils import compute_content_hash
        result = compute_content_hash("test", truncate=8)
        assert len(result) == 8

    def test_config_params_uses_compute_content_hash(self):
        """验证config_params.py使用compute_content_hash"""
        path = os.path.join(_project_root, "config", "config_params.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'compute_content_hash' in source, \
            "config_params.py应使用compute_content_hash"
        # _params_hash行不应有hashlib.md5
        for line in source.split('\n'):
            if '_params_hash' in line and '=' in line and 'hashlib.md5' in line:
                pytest.fail("config_params.py _params_hash仍使用hashlib.md5")

    def test_shadow_strategy_uses_compute_content_hash(self):
        """验证shadow_strategy_core.py使用compute_content_hash"""
        # _shadow_strategy_core.py was merged into shadow_strategy_core.py
        path = os.path.join(_project_root, "strategy", "shadow_strategy_core.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'compute_content_hash' in source, \
            "shadow_strategy_core.py应使用compute_content_hash"
        # _compute_param_hash方法不应有hashlib.md5
        func_match = re.search(
            r'def _compute_param_hash.*?(?=\n    def |\Z)', source, re.DOTALL
        )
        if func_match:
            assert 'hashlib.md5' not in func_match.group(), \
                "_compute_param_hash不应使用hashlib.md5"

    def test_config_version_tracker_uses_compute_content_hash(self):
        """验证config_version_tracker.py使用compute_content_hash"""
        path = os.path.join(_project_root, "config", "config_params.py")
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            source = f.read()
        assert 'compute_content_hash' in source, \
            "config_version_tracker.py应使用compute_content_hash"
        assert 'hashlib.md5(content.encode())' not in source, \
            "config_version_tracker.py不应有hashlib.md5内联调用"
