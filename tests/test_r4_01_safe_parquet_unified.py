# MODULE_ID: M2-534
"""R4-1 断言测试: P2-29 .to_parquet()→safe_dataframe_to_parquet统一

验证项:
1. _preprocess.py不再直接调用.to_parquet()
2. bayesian_shrinkage_life_estimator.py不再直接调用.to_parquet()
3. 两个文件都导入了safe_dataframe_to_parquet
4. 活跃生产代码中不应有裸.to_parquet()调用（排除serialization_utils自身和tests）
"""
import sys
import os
import re
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestSafeParquetUnified:
    """R4-1: .to_parquet()→safe_dataframe_to_parquet统一断言测试"""

    def test__preprocess_uses_safe_parquet(self):
        """验证_preprocess.py导入了safe_dataframe_to_parquet"""
        path = os.path.join(_project_root, "param_pool", "_preprocess.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'safe_dataframe_to_parquet' in source, \
            "_preprocess.py应导入safe_dataframe_to_parquet"

    def test__preprocess_no_raw_to_parquet(self):
        """验证_preprocess.py不再直接调用.to_parquet()"""
        path = os.path.join(_project_root, "param_pool", "_preprocess.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        # 排除注释行
        code_lines = [l for l in source.split('\n')
                      if not l.strip().startswith('#') and '.to_parquet(' in l]
        assert len(code_lines) == 0, \
            f"_preprocess.py仍有.to_parquet()调用: {code_lines}"

    def test_quantification_core_uses_safe_parquet(self):
        """验证_quantification_core.py导入了safe_dataframe_to_parquet"""
        path = os.path.join(_project_root, "param_pool", "l1_quantification",
                            "_quantification_core.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        assert 'safe_dataframe_to_parquet' in source, \
            "_quantification_core.py应导入safe_dataframe_to_parquet"

    def test_quantification_core_no_raw_to_parquet(self):
        """验证_quantification_core.py不再直接调用.to_parquet()"""
        path = os.path.join(_project_root, "param_pool", "l1_quantification",
                            "_quantification_core.py")
        with open(path, 'r', encoding='utf-8') as f:
            source = f.read()
        code_lines = [l for l in source.split('\n')
                      if not l.strip().startswith('#') and '.to_parquet(' in l]
        assert len(code_lines) == 0, \
            f"_quantification_core.py仍有.to_parquet()调用: {code_lines}"

    def test_no_raw_to_parquet_in_production(self):
        """验证活跃生产代码中不应有裸.to_parquet()调用（排除serialization_utils和tests/backup）"""
        exclude_dirs = {'tests', '_backup', 'archive', '__pycache__', '.pytest_cache'}
        violations = []
        for root, dirs, files in os.walk(_project_root):
            dirs[:] = [d for d in dirs if d not in exclude_dirs and not d.startswith('_backup')]
            for fname in files:
                if not fname.endswith('.py'):
                    continue
                fpath = os.path.join(root, fname)
                rel = os.path.relpath(fpath, _project_root)
                # 排除serialization_utils自身（它定义了safe_dataframe_to_parquet）
                if 'serialization_utils' in rel:
                    continue
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        source = f.read()
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    continue
                for i, line in enumerate(source.split('\n'), 1):
                    stripped = line.strip()
                    if stripped.startswith('#'):
                        continue
                    if '.to_parquet(' in line:
                        violations.append(f"{rel}:{i}")
        assert len(violations) == 0, \
            f"生产代码中仍有{len(violations)}处.to_parquet()调用: {violations}"
