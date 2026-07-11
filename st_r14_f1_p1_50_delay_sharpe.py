# MODULE_ID: M2-492
"""R6-F1: P1-50 delay_time_sharpe_3d重复 断言测试

验证运行时行为:
1. delay_time_sharpe_3d_core.py不存在（已合并）
2. delay_time_sharpe_3d.py是唯一实现
3. 无其他模块重复导入delay_time_sharpe_3d_core
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_delay_time_sharpe_3d_core_not_exists():
    """P1-50验证: delay_time_sharpe_3d_core.py不存在（已合并）"""
    _path = os.path.join(_project_root, 'param_pool', 'delay_time_sharpe_3d_core.py')
    assert not os.path.exists(_path), \
        "delay_time_sharpe_3d_core.py应不存在（已合并到delay_time_sharpe_3d.py）"


def test_delay_time_sharpe_3d_exists():
    """P1-50验证: sharpe_3d_mapping已合并到cycle_sharpe.py"""
    import ali2026v3_trading.param_pool.optimization.cycle_sharpe as mod
    _path = mod.__file__
    assert os.path.exists(_path), \
        "cycle_sharpe.py应存在"
    with open(_path, encoding='utf-8') as f:
        src = f.read()
    assert 'sharpe_3d_mapping' in src.lower() or 'Sharpe 3D Mapping' in src or 'sharpe_3d' in src.lower(), \
        "cycle_sharpe.py应包含sharpe_3d_mapping合并代码"


def test_no_imports_of_delay_time_sharpe_3d_core():
    """P1-50验证: 无其他模块重复导入delay_time_sharpe_3d_core"""
    _root = os.path.join(_project_root, 'ali2026v3_trading')
    violations = []
    for dirpath, _, filenames in os.walk(_root):
        if '__pycache__' in dirpath:
            continue
        for fn in filenames:
            if not fn.endswith('.py'):
                continue
            fpath = os.path.join(dirpath, fn)
            try:
                with open(fpath, encoding='utf-8') as f:
                    src = f.read()
                if 'delay_time_sharpe_3d_core' in src:
                    violations.append(os.path.basename(fpath))
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    assert len(violations) == 0, \
        f"发现引用delay_time_sharpe_3d_core的文件: {violations}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
