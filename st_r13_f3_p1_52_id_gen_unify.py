# MODULE_ID: M2-489
"""R5-F3: P1-52 ID生成逻辑分散统一 断言测试

验证运行时行为:
1. generate_prefixed_id权威实现在shared_utils.py
2. generate_prefixed_id运行时行为正确
3. 生产代码不再直接使用uuid4().hex[:N]
4. generate_prefixed_id生成的ID格式正确
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_generate_prefixed_id_in_shared_utils():
    """P1-52验证: generate_prefixed_id权威实现在shared_utils.py"""
    from ali2026v3_trading.infra.shared_utils import generate_prefixed_id
    assert callable(generate_prefixed_id)


def test_generate_prefixed_id_runtime_behavior():
    """P1-52验证: generate_prefixed_id运行时行为正确"""
    from ali2026v3_trading.infra.shared_utils import generate_prefixed_id
    id1 = generate_prefixed_id('SIG', 12)
    assert id1.startswith('SIG_'), f"ID应以'SIG_'开头，实际为{id1}"
    assert len(id1) == 16, f"ID长度应为16(SIG_+12hex)，实际为{len(id1)}"

    id2 = generate_prefixed_id('', 8)
    assert len(id2) == 8, f"无前缀ID长度应为8(8hex)，实际为{len(id2)}"

    id3 = generate_prefixed_id('ORD')
    assert id3.startswith('ORD_'), f"ID应以'ORD_'开头，实际为{id3}"


def test_no_direct_uuid4_hex_in_production():
    """P1-52验证: 生产代码不再直接使用uuid4().hex[:N]"""
    _root = os.path.join(_project_root, 'ali2026v3_trading')
    violations = []
    skip_dirs = {'tests', '__pycache__', 'param_pool'}
    for dirpath, _, filenames in os.walk(_root):
        if any(d in dirpath for d in skip_dirs):
            continue
        for fn in filenames:
            if not fn.endswith('.py'):
                continue
            fpath = os.path.join(dirpath, fn)
            if 'shared_utils.py' in fpath:
                continue  # 权威定义
            try:
                with open(fpath, encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if 'uuid4().hex[:' in line and 'generate_prefixed_id' not in line:
                            violations.append(f'{os.path.basename(fpath)}:{i}')
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    assert len(violations) == 0, \
        f"发现直接uuid4().hex[:N]调用: {violations}"


def test_generate_prefixed_id_uniqueness():
    """P1-52验证: generate_prefixed_id生成唯一ID"""
    from ali2026v3_trading.infra.shared_utils import generate_prefixed_id
    ids = {generate_prefixed_id('T', 8) for _ in range(100)}
    assert len(ids) == 100, "100次调用应生成100个唯一ID"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
