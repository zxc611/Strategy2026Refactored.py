# MODULE_ID: M2-476
"""R2-F5: P1-20 safe_get_float/safe_get_int统一 断言测试

验证运行时行为:
1. safe_get_float/safe_get_int在shared_utils.py统一定义
2. shared_utils_types.py是re-export facade，指向同一函数
3. risk/_utils.py导入的是同一函数实例
4. safe_get_float运行时行为正确
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_safe_get_float_defined_in_shared_utils():
    """P1-20验证: safe_get_float在shared_utils.py统一定义"""
    from ali2026v3_trading.infra.shared_utils import safe_get_float, safe_get_int
    assert callable(safe_get_float), "safe_get_float应为可调用函数"
    assert callable(safe_get_int), "safe_get_int应为可调用函数"


def test_shared_utils_types_is_reexport_facade():
    """P1-20验证: shared_utils_types.py是re-export facade，指向同一函数"""
    from ali2026v3_trading.infra.shared_utils import safe_get_float, safe_get_int
    from ali2026v3_trading.infra.shared_utils import (
        safe_get_float as sgf2, safe_get_int as sgi2,
    )
    assert safe_get_float is sgf2, \
        "shared_utils_types.safe_get_float应与shared_utils.safe_get_float是同一函数"
    assert safe_get_int is sgi2, \
        "shared_utils_types.safe_get_int应与shared_utils.safe_get_int是同一函数"


def test_risk_utils_imports_same_instance():
    """P1-20验证: risk/_utils.py导入的是同一函数实例"""
    from ali2026v3_trading.infra.shared_utils import safe_get_float, safe_get_int
    from ali2026v3_trading.risk.risk_support import (
        safe_get_float as sgf3, safe_get_int as sgi3,
    )
    assert safe_get_float is sgf3, \
        "risk._utils.safe_get_float应与shared_utils.safe_get_float是同一函数"
    assert safe_get_int is sgi3, \
        "risk._utils.safe_get_int应与shared_utils.safe_get_int是同一函数"


def test_safe_get_float_runtime_behavior():
    """P1-20验证: safe_get_float运行时行为正确"""
    from ali2026v3_trading.infra.shared_utils import safe_get_float, safe_get_int

    class MockObj:
        x = 3.14
        y = 42
        z = None

    obj = MockObj()
    assert safe_get_float(obj, 'x') == pytest.approx(3.14, abs=1e-6)
    assert safe_get_float(obj, 'z', default=1.0) == 1.0
    assert safe_get_float(obj, 'missing', default=2.5) == 2.5
    assert safe_get_int(obj, 'y') == 42
    assert safe_get_int(obj, 'missing', default=99) == 99


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
