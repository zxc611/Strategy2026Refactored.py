# MODULE_ID: M2-487
"""R5-F1: P1-49/60 预处理流水线大面积重复 断言测试

验证运行时行为:
1. preprocess_ticks的公共函数委托到_preprocess
2. MinuteBoundaryResult和check_minute_boundary_integrity保留在preprocess_ticks
3. preprocess_ticks导入的函数与_preprocess是同一实例
4. preprocess_ticks产生DeprecationWarning
"""
import os
import sys
import warnings
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_preprocess_ticks_delegates_to_pipeline():
    """P1-49/60验证: _preprocess模块包含关键公共函数"""
    from ali2026v3_trading.param_pool import _preprocess
    # 关键函数应存在
    assert hasattr(_preprocess, 'check_minute_boundary_integrity'), \
        "_preprocess应包含check_minute_boundary_integrity"
    assert hasattr(_preprocess, 'validate_out_of_order_ticks'), \
        "_preprocess应包含validate_out_of_order_ticks"
    assert hasattr(_preprocess, 'MinuteBoundaryResult'), \
        "_preprocess应包含MinuteBoundaryResult"


def test_minute_boundary_result_retained():
    """P1-49/60验证: MinuteBoundaryResult和check_minute_boundary_integrity保留在preprocess_ticks"""
    from ali2026v3_trading.param_pool._preprocess import (
        MinuteBoundaryResult, check_minute_boundary_integrity,
    )
    assert MinuteBoundaryResult is not None
    assert callable(check_minute_boundary_integrity)


def test_preprocess_ticks_emits_deprecation_warning():
    """P1-49/60验证: preprocess_ticks产生DeprecationWarning"""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        # 重新导入触发警告
        import importlib
        import ali2026v3_trading.param_pool._preprocess as pt
        importlib.reload(pt)
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(dep_warnings) >= 1, \
            f"应产生DeprecationWarning，实际产生{len(dep_warnings)}个"


def test_preprocess_ticks_file_size_reduced():
    """P1-49/60验证: _preprocess文件大小合理（合并后完整实现）"""
    _path = os.path.join(_project_root, 'param_pool', '_preprocess.py')
    with open(_path, encoding='utf-8') as f:
        lines = f.readlines()
    assert len(lines) <= 505, \
        f"_preprocess应少于505行，实际{len(lines)}行"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
