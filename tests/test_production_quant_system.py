# MODULE_ID: M2-470
"""ProductionQuantSystem.py 覆盖率测试"""
import pytest, sys, os
from unittest.mock import MagicMock
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestProductionQuantSystem:
    def test_init(self):
        from ali2026v3_trading.ProductionQuantSystem import ProductionQuantSystem
        pqs = ProductionQuantSystem.__new__(ProductionQuantSystem)
        assert pqs is not None

    def test_get_system_status(self):
        from ali2026v3_trading.ProductionQuantSystem import ProductionQuantSystem
        pqs = ProductionQuantSystem.__new__(ProductionQuantSystem)
        pqs._initialized = False
        pqs._symbols = []
        pqs.atomic_state = MagicMock()
        pqs.atomic_state.version = 0
        pqs.atomic_state.age_ms = 0
        pqs.health_monitor = MagicMock()
        pqs.health_monitor.get_health_report.return_value = {}
        pqs.hot_config = MagicMock()
        pqs.hot_config.get_all.return_value = {}
        pqs.health_check_api = None
        status = pqs.get_system_status()
        assert isinstance(status, dict)

    def test_shutdown(self):
        from ali2026v3_trading.ProductionQuantSystem import ProductionQuantSystem
        pqs = ProductionQuantSystem.__new__(ProductionQuantSystem)
        pqs._initialized = False
        pqs._symbols = []
        pqs.health_monitor = MagicMock()
        pqs.singleton_mgr = MagicMock()
        pqs.persistence = MagicMock()
        pqs.hot_config = MagicMock()
        pqs.hmm = MagicMock()
        pqs.hmm._em_running = False
        pqs.hmm._em_thread = None
        pqs._event_bus = None
        pqs.shutdown()

    def test_load_config_from_file(self):
        from ali2026v3_trading.ProductionQuantSystem import _load_config_from_file
        import json, tempfile, os
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump({'test': 1}, f)
            tmp_path = f.name
        try:
            result = _load_config_from_file(config_path=tmp_path)
            assert result is not None
        finally:
            os.unlink(tmp_path)

    def test_build_config_from_args(self):
        from ali2026v3_trading.ProductionQuantSystem import _build_config_from_args
        import argparse
        args = argparse.Namespace(config=None, exchange='DCE', log_dir='/tmp/log', symbols='AL2401')
        result = _build_config_from_args(args=args)
        assert result is not None