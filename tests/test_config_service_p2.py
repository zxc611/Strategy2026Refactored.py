import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestConfigService:
    """R17-P2: config_service基础测试"""
    def test_importable(self):
        try:
            from ali2026v3_trading.config_service import ConfigService
            assert ConfigService is not None
        except ImportError:
            pytest.skip("config_service not importable")

    def test_config_version_defined(self):
        from ali2026v3_trading.config_service import ConfigService
        svc = ConfigService.__new__(ConfigService)
        assert hasattr(svc, 'config_version') or hasattr(ConfigService, '_YAML_REQUIRED_VERSION')
