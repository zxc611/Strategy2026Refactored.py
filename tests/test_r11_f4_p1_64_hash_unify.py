# MODULE_ID: M2-480
"""R3-F4: P1-64 哈希计算逻辑分散统一 断言测试

验证运行时行为:
1. compute_content_hash权威实现在shared_utils.py
2. _params_core.py使用compute_content_hash而非直接hashlib
3. _backup_restore.py流式sha256是合理使用（不替换）
4. 非流式hashlib调用应委托compute_content_hash
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_compute_content_hash_authoritative():
    """P1-64验证: compute_content_hash权威实现在shared_utils.py"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    assert callable(compute_content_hash)
    result = compute_content_hash("test", algorithm='md5')
    assert isinstance(result, str) and len(result) > 0
    result_sha = compute_content_hash("test", algorithm='sha256')
    assert isinstance(result_sha, str) and len(result_sha) > 0


def test_params_core_uses_compute_content_hash():
    """P1-64验证: _params_core.py使用compute_content_hash而非直接hashlib"""
    _path = os.path.join(_project_root, 'config', '_params_core.py')
    with open(_path, encoding='utf-8') as f:
        src = f.read()
    # 不应有直接的hashlib.md5调用（除了import语句）'
    lines = src.split('\n')
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if 'hashlib.md5(' in stripped and 'import' not in stripped:
            pytest.fail(f"_params_core.py行{i}仍有直接hashlib.md5调用: {stripped}")


def test_backup_restore_streaming_sha256_is_reasonable():
    """P1-64验证: _backup_restore.py流式sha256是合理使用"""
    # _backup_restore.py was merged into storage_service.py
    _path = os.path.join(_project_root, 'infra', 'storage_service.py')
    if not os.path.exists(_path):
        pytest.skip("storage_service.py不存在")
    with open(_path, encoding='utf-8') as f:
        src = f.read()
    # 流式sha256（分块读取文件）是合理使用，不应替换
    assert 'hashlib.sha256()' in src, \
        "storage_service.py应有流式hashlib.sha256()（文件校验合理使用）"


def test_compute_content_hash_runtime_behavior():
    """P1-64验证: compute_content_hash运行时行为正确"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    import hashlib

    # MD5
    content = "hello world"
    expected_md5 = hashlib.md5(content.encode()).hexdigest()
    actual_md5 = compute_content_hash(content, algorithm='md5')
    assert actual_md5 == expected_md5, \
        f"MD5不匹配: 期望{expected_md5}，实际{actual_md5}"

    # SHA256
    expected_sha = hashlib.sha256(content.encode()).hexdigest()
    actual_sha = compute_content_hash(content, algorithm='sha256')
    assert actual_sha == expected_sha, \
        f"SHA256不匹配: 期望{expected_sha}，实际{actual_sha}"

    # Truncate
    truncated = compute_content_hash(content, algorithm='sha256', truncate=16)
    assert len(truncated) == 16, \
        f"截断后长度应为16，实际{len(truncated)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
