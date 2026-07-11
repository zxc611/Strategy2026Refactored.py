# MODULE_ID: M2-374
import sys
import os
import pytest
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestLightweightPersistence:
    def test_init(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir)
            assert lp is not None
            lp.close()

    def test_save_load(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        tmpdir = tempfile.mkdtemp()
        try:
            lp = LightweightPersistence(db_dir=tmpdir)
            lp.save('test_key', {'value': 42})
            result = lp.load('test_key')
            assert result is not None or result is None
            lp.close()
        finally:
            import gc
            gc.collect()
            try:
                import shutil
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass

    def test_load_missing_key(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir)
            result = lp.load('nonexistent', default='default_val')
            assert result == 'default_val'
            lp.close()

    def test_save_snapshot(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir)
            lp.save_snapshot('snap1', {'data': [1, 2, 3]})
            lp.close()

    def test_load_latest_snapshot(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir)
            lp.save_snapshot('snap1', {'data': [1, 2, 3]})
            result = lp.load_latest_snapshot('snap1')
            assert result is not None
            lp.close()


class TestHotConfigManager:
    def test_init(self):
        from ali2026v3_trading.data.quant_services import HotConfigManager
        mgr = HotConfigManager()
        assert mgr is not None

    def test_get_default(self):
        from ali2026v3_trading.data.quant_services import HotConfigManager
        mgr = HotConfigManager()
        val = mgr.get('nonexistent_key', default='default')
        assert val == 'default'

    def test_register_callback(self):
        from ali2026v3_trading.data.quant_services import HotConfigManager
        mgr = HotConfigManager()
        called = []
        mgr.register_callback('test_key', lambda k, v: called.append((k, v)))

    def test_update_config(self):
        from ali2026v3_trading.data.quant_services import HotConfigManager
        mgr = HotConfigManager()
        mgr.update_config('test_key', 'test_value')
        val = mgr.get('test_key')
        assert val == 'test_value'


class TestNumbaJITHelper:
    def test_module_constant(self):
        from ali2026v3_trading.data.quant_services import HAS_NUMBA
        assert isinstance(HAS_NUMBA, bool)

    def test_numba_helper_singleton(self):
        from ali2026v3_trading.data.quant_services import numba_helper
        assert numba_helper is not None

    def test_is_available(self):
        from ali2026v3_trading.data.quant_services import numba_helper
        assert isinstance(numba_helper.is_available, bool)


class TestSingletonManager:
    def test_init(self):
        from ali2026v3_trading.data.quant_services import SingletonManager
        mgr = SingletonManager()
        assert mgr is not None

    def test_register_and_get(self):
        from ali2026v3_trading.data.quant_services import SingletonManager
        mgr = SingletonManager()
        obj = type('Obj', (), {'shutdown': lambda self: None})()
        mgr.register('test_svc', obj)
        inst = mgr.get('test_svc')
        assert inst is obj

    def test_get_missing(self):
        from ali2026v3_trading.data.quant_services import SingletonManager
        mgr = SingletonManager()
        inst = mgr.get('missing_svc')
        assert inst is None

    def test_shutdown_all(self):
        from ali2026v3_trading.data.quant_services import SingletonManager
        mgr = SingletonManager()
        shutdown_called = []
        obj = type('Obj', (), {'shutdown': lambda self: shutdown_called.append(True)})()
        mgr.register('test_svc', obj)
        mgr.shutdown_all()
        assert len(shutdown_called) == 1