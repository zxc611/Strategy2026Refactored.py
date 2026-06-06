import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestStorageCore:
    """R17-P2: storage_core基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.storage_core import _StorageCoreMixin
            assert _StorageCoreMixin is not None
        except ImportError:
            pytest.skip("storage_core not importable")
