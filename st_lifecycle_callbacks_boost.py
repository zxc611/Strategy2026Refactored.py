# MODULE_ID: M2-405
import pytest
import sys
import threading
import types
from unittest.mock import MagicMock, patch
from ali2026v3_trading.lifecycle.lifecycle_callbacks import LifecycleCallbacks
from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState


@pytest.fixture(autouse=True)
def mock_diagnosis_module():
    if 'ali2026v3_trading.infra.diagnosis' not in sys.modules:
        mock_mod = types.ModuleType('ali2026v3_trading.infra.diagnosis')
        mock_mod.DiagnosisProbeManager = MagicMock()
        mock_mod.DiagnosisProbeManager.start_contract_watch = MagicMock()
        mock_mod.DiagnosisProbeManager.stop_contract_watch = MagicMock()
        mock_mod.reset_diagnosis_grace_period = MagicMock()
        sys.modules['ali2026v3_trading.infra.diagnosis'] = mock_mod
    yield


class TestLifecycleCallbacksInit:
    def test_init(self):
        provider = MagicMock()
        lc = LifecycleCallbacks(provider)
        assert lc.p is provider


class TestOnStart:
    def test_not_initialized(self):
        provider = MagicMock()
        provider._initialized = False
        lc = LifecycleCallbacks(provider)
        result = lc.on_start()
        assert result is False

    def test_start_success(self):
        provider = MagicMock()
        provider._initialized = True
        provider._lock = threading.Lock()
        provider._state = StrategyState.INITIALIZING
        provider._is_paused = False
        provider._is_running = False
        provider._lifecycle_mgr = MagicMock()
        provider._init_instruments_result = {
            'futures_list': ['IF2606'],
            'options_dict': {},
            'subscribed_instruments': ['IF2606'],
        }
        provider._subscribed_instruments = ['IF2606']
        provider._e2e_counters = {}
        provider._safety_meta_layer = None
        provider._lifecycle_platform = MagicMock()
        provider._lifecycle_resource = MagicMock()
        provider._stats = {}
        provider._scheduler_manager = MagicMock()
        provider._platform_subscribe_thread = None
        provider._platform_subscribe_stop = threading.Event()
        provider._platform_subscribe_completed = threading.Event()
        provider._count_option_contracts = MagicMock(return_value=0)

        with patch('ali2026v3_trading.lifecycle.lifecycle_callbacks.get_param_value', return_value=False):
            lc = LifecycleCallbacks(provider)
            result = lc.on_start()
            assert result is True


class TestOnStop:
    def test_already_stopped(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.STOPPED
        provider._lifecycle_mgr = MagicMock()
        lc = LifecycleCallbacks(provider)
        result = lc.on_stop()
        assert result is True

    def test_stop_success(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.RUNNING
        provider._is_running = True
        provider._is_paused = False
        provider._lifecycle_mgr = MagicMock()
        provider._scheduler_manager = MagicMock()
        provider._platform_subscribe_thread = None
        provider._platform_subscribe_stop = threading.Event()
        provider._lifecycle_platform = MagicMock()
        provider._lifecycle_resource = MagicMock()
        provider._stats = {}
        lc = LifecycleCallbacks(provider)
        result = lc.on_stop()
        assert result is True


class TestOnDestroy:
    def test_destroy(self):
        provider = MagicMock()
        lc = LifecycleCallbacks(provider)
        lc.on_destroy()


class TestStartStopAliases:
    def test_start_calls_on_start(self):
        provider = MagicMock()
        provider._initialized = False
        lc = LifecycleCallbacks(provider)
        assert lc.start() is False

    def test_stop_calls_on_stop(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.STOPPED
        provider._lifecycle_mgr = MagicMock()
        lc = LifecycleCallbacks(provider)
        assert lc.stop() is True


class TestPause:
    def test_pause_not_running(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.STOPPED
        lc = LifecycleCallbacks(provider)
        result = lc.pause()
        assert result is False

    def test_pause_success(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.RUNNING
        provider._is_paused = False
        provider._lifecycle_mgr = MagicMock()
        lc = LifecycleCallbacks(provider)
        result = lc.pause()
        assert result is True
        assert provider._is_paused is True


class TestResume:
    def test_resume_not_paused(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.RUNNING
        lc = LifecycleCallbacks(provider)
        result = lc.resume()
        assert result is False

    def test_resume_success(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._state = StrategyState.PAUSED
        provider._is_paused = True
        provider._is_running = False
        provider._lifecycle_mgr = MagicMock()
        lc = LifecycleCallbacks(provider)
        result = lc.resume()
        assert result is True
        assert provider._is_running is True


class TestDestroy:
    def test_already_destroyed(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._destroyed = True
        provider._lifecycle_mgr = MagicMock()
        lc = LifecycleCallbacks(provider)
        result = lc.destroy()
        assert result is True

    def test_destroy_success(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._destroyed = False
        provider._state = StrategyState.RUNNING
        provider._is_running = True
        provider._is_paused = False
        provider._lifecycle_mgr = MagicMock()
        provider._scheduler_manager = MagicMock()
        provider._platform_subscribe_thread = None
        provider._platform_subscribe_stop = threading.Event()
        provider._lifecycle_platform = MagicMock()
        provider._lifecycle_resource = MagicMock()
        provider._stats = {}
        provider._lsm_instance = MagicMock()
        lc = LifecycleCallbacks(provider)
        result = lc.destroy()
        assert result is True
        assert provider._destroyed is True