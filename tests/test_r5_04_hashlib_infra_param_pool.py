# MODULE_ID: M2-544
"""R5-4断言测试: P1-64 hashlib infra+param_pool层→compute_content_hash统一

验证meta_audit_passport.py不再有直接hashlib.sha256字符串哈希调用:
1. _generate_id: hashlib.sha256(content.encode()).hexdigest()[:16] → compute_content_hash(content, truncate=16, algorithm='sha256')
2. is_certification_valid: hashlib.sha256(self.engine_code.encode()).hexdigest() → compute_content_hash(self.engine_code, algorithm='sha256')
3. audit: hashlib.sha256(self.engine_code.encode()).hexdigest() → compute_content_hash
4. certify: hashlib.sha256(self.engine_code.encode()).hexdigest() → compute_content_hash
5. inject_passport: hashlib.sha256(json.dumps(...).encode()).hexdigest()[:16] → compute_content_hash(..., truncate=16, algorithm='sha256')

注: _backup_restore.py使用流式二进制哈希，不适合compute_content_hash替换
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


def test_meta_audit_passport_no_hashlib_sha256_string():
    """meta_audit_passport.py不应有hashlib.sha256(xxx.encode())"""
    src = _read('param_pool/l1_quantification/meta_audit_passport.py')
    # 检查字符串哈希模式（排除import行和注释）
    for line in src.split('\n'):
        stripped = line.strip()
        if 'hashlib.sha256(' in stripped and 'encode()' in stripped:
            assert False, f"meta_audit_passport.py仍有hashlib.sha256(xxx.encode()): {stripped}"


def test_meta_audit_passport_has_compute_content_hash():
    """meta_audit_passport.py应使用compute_content_hash"""
    src = _read('param_pool/l1_quantification/meta_audit_passport.py')
    assert 'compute_content_hash' in src, "meta_audit_passport.py应使用compute_content_hash"


def test_compute_content_hash_sha256_truncate():
    """验证compute_content_hash(algorithm='sha256', truncate=16)与原结果一致"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    import hashlib
    test_str = "engine_code_123"
    original = hashlib.sha256(test_str.encode()).hexdigest()[:16]
    unified = compute_content_hash(test_str, truncate=16, algorithm='sha256')
    assert original == unified, f"SHA256截断哈希不一致: {original} != {unified}"


def test_compute_content_hash_sha256_full():
    """验证compute_content_hash(algorithm='sha256')与原结果一致"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    import hashlib
    test_str = "engine_code_456"
    original = hashlib.sha256(test_str.encode()).hexdigest()
    unified = compute_content_hash(test_str, algorithm='sha256')
    assert original == unified, f"SHA256完整哈希不一致: {original} != {unified}"


def test_compute_content_hash_with_json_dumps():
    """验证compute_content_hash(json.dumps(...))与原模式一致"""
    from ali2026v3_trading.infra.shared_utils import compute_content_hash
    import hashlib
    import json
    config = {"key": "value", "num": 42}
    json_str = json.dumps(config, sort_keys=True)
    original = hashlib.sha256(json_str.encode()).hexdigest()[:16]
    unified = compute_content_hash(json_str, truncate=16, algorithm='sha256')
    assert original == unified, f"JSON哈希不一致: {original} != {unified}"


def test_backup_restore_keeps_hashlib():
    """_backup_restore.py仍可使用hashlib（流式二进制哈希）"""
    src = _read('infra/storage_service.py')
    # 流式哈希模式应保留
    assert 'hashlib.sha256()' in src, "_backup_restore.py应有流式hashlib.sha256()"
