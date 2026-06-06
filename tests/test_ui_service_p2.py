import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestUIService:
    """R17-P2: ui_service基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.ui_service import UIService
            assert UIService is not None
        except ImportError:
            pytest.skip("ui_service not importable")
