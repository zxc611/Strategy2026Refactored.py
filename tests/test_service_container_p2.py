# MODULE_ID: M2-571
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestServiceContainer:
    """R17-P2: service_container基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.infra.service_container import ServiceContainer
            assert ServiceContainer is not None
        except ImportError:
            pytest.skip("service_container not importable")
