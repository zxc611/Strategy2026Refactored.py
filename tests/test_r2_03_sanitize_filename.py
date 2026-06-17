# MODULE_ID: M2-517
"""R2-3: 验证 sanitize_filename 统一提取，消除内联重复"""
import pytest


def test_sanitize_filename_behavior():
    """sanitize_filename 应正确替换路径分隔符"""
    from ali2026v3_trading.infra.shared_utils import sanitize_filename
    assert sanitize_filename('a/b\\c') == 'a_b_c'
    assert sanitize_filename('normal') == 'normal'
    assert sanitize_filename('../etc/passwd') == '.._etc_passwd'
    assert sanitize_filename('C:\\Users\\x') == 'C:_Users_x'


def test_order_persistence_uses_sanitize_filename():
    """order_persistence.wal_path 应使用 sanitize_filename"""
    import inspect
    from ali2026v3_trading.order.order_persistence import OrderPersistenceService
    source = inspect.getsource(OrderPersistenceService.wal_path)
    assert 'sanitize_filename' in source, \
        "OrderPersistenceService.wal_path 应使用 sanitize_filename"
    assert ".replace('/', '_').replace('\\\\', '_')" not in source, \
        "OrderPersistenceService.wal_path 不应有内联路径安全处理"


def test_order_wal_state_uses_sanitize_filename():
    """order_wal_state_service._wal_path 应使用 sanitize_filename"""
    import inspect
    from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
    source = inspect.getsource(OrderWALStateService._wal_path)
    assert 'sanitize_filename' in source, \
        "OrderWALStateService._wal_path 应使用 sanitize_filename"
    assert ".replace('/', '_').replace('\\\\', '_')" not in source, \
        "OrderWALStateService._wal_path 不应有内联路径安全处理"


def test_no_inline_path_sanitize_in_order_modules():
    """order模块中不应有内联的 replace('/','_').replace('\\\\','_') 模式"""
    import glob
    import os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.dirname(root)  # up to ali2026v3_trading
    inline_refs = []
    for pyfile in glob.glob(os.path.join(root, 'order', '*.py'), recursive=True):
        if '_backup_' in pyfile:
            continue
        with open(pyfile, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                if ".replace('/', '_')" in line or ".replace('\\\\', '_')" in line:
                    inline_refs.append(f"{os.path.basename(pyfile)}:{i}")
    assert len(inline_refs) == 0, \
        f"order模块仍有 {len(inline_refs)} 处内联路径安全处理: {inline_refs}"
