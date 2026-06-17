# MODULE_ID: M2-477
"""R3-F1: P0-28/P1-51 _CHINA_TZ时区常量统一 断言测试

验证运行时行为:
1. CHINA_TZ权威定义在shared_utils.py
2. config_dataclasses._CHINA_TZ指向同一实例
3. _helpers._CHINA_TZ指向同一实例
4. 所有CHINA_TZ实例的utcoffset一致(+8h)
5. 没有独立的CHINA_TZ定义
"""
import os
import sys
import pytest
from datetime import timezone, timedelta

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_china_tz_authoritative_in_shared_utils():
    """P0-28验证: CHINA_TZ权威定义在shared_utils.py"""
    from ali2026v3_trading.infra.shared_utils import CHINA_TZ
    assert CHINA_TZ is not None
    assert CHINA_TZ.utcoffset(None) == timedelta(hours=8)


def test_config_dataclasses_china_tz_same_instance():
    """P0-28验证: config_dataclasses._CHINA_TZ指向同一实例"""
    from ali2026v3_trading.infra.shared_utils import CHINA_TZ
    from ali2026v3_trading.config.config_dataclasses import _CHINA_TZ
    assert _CHINA_TZ is CHINA_TZ


def test_helpers_china_tz_same_instance():
    """P0-28验证: _helpers._CHINA_TZ指向同一实例"""
    from ali2026v3_trading.infra.shared_utils import CHINA_TZ
    from ali2026v3_trading.infra._helpers import _CHINA_TZ
    assert _CHINA_TZ is CHINA_TZ


def test_no_independent_china_tz_definitions():
    """P0-28验证: 没有独立的CHINA_TZ赋值定义（除shared_utils.py和security.py fallback外）"""
    import ast
    _root = os.path.join(_project_root, 'ali2026v3_trading')
    independent_defs = []
    for dirpath, _, filenames in os.walk(_root):
        if 'tests' in dirpath or '__pycache__' in dirpath:
            continue
        for fn in filenames:
            if not fn.endswith('.py'):
                continue
            fpath = os.path.join(dirpath, fn)
            if 'shared_utils.py' in fpath or 'security.py' in fpath:
                continue
            try:
                with open(fpath, encoding='utf-8') as f:
                    src = f.read()
                tree = ast.parse(src)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Assign):
                        for target in node.targets:
                            if isinstance(target, ast.Name) and target.id in ('CHINA_TZ', '_CHINA_TZ'):
                                independent_defs.append(f'{os.path.basename(fpath)}:{node.lineno}')
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    assert len(independent_defs) == 0, \
        f"发现独立CHINA_TZ定义: {independent_defs}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
