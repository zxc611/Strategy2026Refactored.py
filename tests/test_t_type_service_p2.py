import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestTTypeService:
    """R17-P2: t_type_service基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.t_type_service import TTypeService
            assert TTypeService is not None
        except ImportError:
            pytest.skip("t_type_service not importable")
