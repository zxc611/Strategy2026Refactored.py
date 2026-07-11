# MODULE_ID: M2-595
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestTTypeService:
    """R17-P2: t_type_service基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.data.t_type_service import TTypeService
            assert TTypeService is not None
        except ImportError:
            pytest.skip("t_type_service not importable")

    def test_width_cache_uses_data_service_params_service(self, monkeypatch):
        from ali2026v3_trading.data import t_type_service as module

        captured = {}
        fake_params_service = object()

        class _FakeStep:
            def __enter__(self):
                return None

            def __exit__(self, exc_type, exc, tb):
                return False

        class _FakeDiagnosisProbeManager:
            @staticmethod
            def startup_step(_name):
                return _FakeStep()

        class _FakeWidthStrengthCache:
            def __init__(self, tracked_option_tick_ids=None, params_service=None, strategy_id=None):
                captured['params_service'] = params_service
                captured['strategy_id'] = strategy_id

        class _FakeDataService:
            @property
            def params_service(self):
                return fake_params_service

        monkeypatch.setattr(module, 'WidthStrengthCache', _FakeWidthStrengthCache)
        monkeypatch.setattr(module, '_HAS_DATA_SERVICE', True)
        monkeypatch.setattr(module, '_NoOpDiagnosisProbeManager', _FakeDiagnosisProbeManager)

        import ali2026v3_trading.data.data_service as ds_module
        monkeypatch.setattr(ds_module, 'get_data_service', lambda: _FakeDataService())

        svc = module.TTypeService()

        assert svc is not None
        assert captured['strategy_id'] == 'global'
        assert captured['params_service'] is fake_params_service
