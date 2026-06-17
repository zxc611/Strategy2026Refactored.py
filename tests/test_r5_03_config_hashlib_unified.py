# MODULE_ID: M2-543
"""R5-3断言测试: P1-64 hashlib config层→compute_content_hash统一

验证2个config文件不再有直接hashlib字符串哈希调用:
1. _params_canary_env.py — hashlib.md5(instrument_id.encode()) → compute_content_hash
2. config_logging.py — hashlib.sha256(raw.encode('utf-8')) → compute_content_hash(algorithm='sha256')

注: _params_core.py和config_sync.py使用二进制文件内容哈希，不适合compute_content_hash替换
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


def test_canary_env_no_hashlib_md5_string():
    """_params_canary_env.py不应有hashlib.md5(instrument_id.encode())"""
    src = _read('config/_params_canary_env.py')
    assert 'hashlib.md5(instrument_id' not in src, "_params_canary_env.py不应有hashlib.md5(instrument_id.encode())"


def test_canary_env_has_compute_content_hash():
    """_params_canary_env.py应使用compute_content_hash"""
    src = _read('config/_params_canary_env.py')
    assert 'compute_content_hash' in src, "_params_canary_env.py应使用compute_content_hash"


def test_canary_env_hash_equivalence():
    """验证compute_content_hash与原hashlib.md5结果一致"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    import hashlib
    test_str = "IF2401"
    original = hashlib.md5(test_str.encode()).hexdigest()
    unified = compute_content_hash(test_str)
    assert original == unified, f"哈希结果不一致: {original} != {unified}"


def test_config_logging_no_hashlib_sha256_string():
    """config_logging.py不应有hashlib.sha256(raw.encode"""
    src = _read('config/config_logging.py')
    assert 'hashlib.sha256(raw.encode' not in src, "config_logging.py不应有hashlib.sha256(raw.encode('utf-8'))"


def test_config_logging_has_compute_content_hash():
    """config_logging.py应使用compute_content_hash"""
    src = _read('config/config_logging.py')
    assert 'compute_content_hash' in src, "config_logging.py应使用compute_content_hash"


def test_config_logging_sha256_equivalence():
    """验证compute_content_hash(algorithm='sha256')与原hashlib.sha256结果一致"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    import hashlib
    test_str = "0"*64 + "2026-01-01T00:00:00" + "CONFIG_CHANGE" + '{"key":"val"}'
    original = hashlib.sha256(test_str.encode('utf-8')).hexdigest()
    unified = compute_content_hash(test_str, algorithm='sha256')
    assert original == unified, f"SHA256哈希结果不一致: {original} != {unified}"


def test_config_logging_no_local_hashlib_import():
    """config_logging.py不应有局部import hashlib"""
    src = _read('config/config_logging.py')
    # 检查函数内的局部import hashlib
    lines = src.split('\n')
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == 'import hashlib':
            # 允许模块级import（但不应有）
            assert False, f"config_logging.py第{i+1}行仍有'import hashlib'"
