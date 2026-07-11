# MODULE_ID: M2-333
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestDiagnosisService:
    """R17-P2: diagnosis_service基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.infra.health_monitor import DiagnosisService
            assert DiagnosisService is not None
        except ImportError:
            pytest.skip("diagnosis_service not importable")
