# MODULE_ID: M2-486
"""R4-F5: P2-19 文件路径安全处理重复统一 断言测试

验证运行时行为:
1. sanitize_filename权威实现在shared_utils.py
2. 其他模块无内联replace('/','_')重复实现
3. sanitize_filename运行时行为正确
4. sanitize_filename可从shared_utils导入
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_sanitize_filename_in_shared_utils():
    """P2-19验证: sanitize_filename权威实现在shared_utils.py"""
    from ali2026v3_trading.infra.shared_utils import sanitize_filename
    assert callable(sanitize_filename)


def test_sanitize_filename_runtime_behavior():
    """P2-19验证: sanitize_filename运行时行为正确"""
    from ali2026v3_trading.infra.shared_utils import sanitize_filename
    assert sanitize_filename('path/to/file') == 'path_to_file'
    assert sanitize_filename('path\\to\\file') == 'path_to_file'
    assert sanitize_filename('normal') == 'normal'
    assert sanitize_filename('a/b\\c') == 'a_b_c'


def test_no_inline_replace_in_production_code():
    """P2-19验证: 生产代码无内联replace('/','_')重复实现"""
    _root = os.path.join(_project_root, 'ali2026v3_trading')
    violations = []
    for dirpath, _, filenames in os.walk(_root):
        if 'tests' in dirpath or '__pycache__' in dirpath:
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
                        if ".replace('/', '_')" in line or '.replace("/", "_")' in line:
                            violations.append(f'{os.path.basename(fpath)}:{i}')
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    assert len(violations) == 0, \
        f"发现内联replace('/','_'): {violations}"


def test_sanitize_filename_importable():
    """P2-19验证: sanitize_filename可从shared_utils导入"""
    from ali2026v3_trading.infra.shared_utils import sanitize_filename
    result = sanitize_filename('test/path')
    assert isinstance(result, str)
    assert '/' not in result
    assert '\\' not in result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
