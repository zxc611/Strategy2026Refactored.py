# MODULE_ID: M2-407
import pytest
import threading
from unittest.mock import MagicMock, patch
from ali2026v3_trading.lifecycle.lifecycle_init import LifecycleInit
from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState


class TestLifecycleInitInit:
    def test_init(self):
        provider = MagicMock()
        li = LifecycleInit(provider)
        assert li.p is provider


class TestOnInit:
    def test_already_initialized(self):
        provider = MagicMock()
        provider._initialized = True
        provider._lock = threading.Lock()
        li = LifecycleInit(provider)
        result = li.on_init()
        assert result is True

    def test_wrong_state(self):
        provider = MagicMock()
        provider._initialized = False
        provider._lock = threading.Lock()
        provider._state = StrategyState.STOPPED
        li = LifecycleInit(provider)
        result = li.on_init()
        assert result is False

    def test_init_success(self):
        provider = MagicMock()
        provider._initialized = False
        provider._lock = threading.Lock()
        provider._state = StrategyState.INITIALIZING
        provider._lifecycle_mgr = MagicMock()
        provider._stats = {'errors_count': 0}
        provider._analytics_warmup_done = False
        provider._analytics_warmup_thread = None

        with patch('ali2026v3_trading.config.config_service.ensure_products_with_retry') as mock_ensure, \
             patch('ali2026v3_trading.data.data_service.get_data_service') as mock_ds, \
             patch('ali2026v3_trading.data.query_service.QueryService') as mock_qs:
            mock_ensure.return_value = {'future_added': 0, 'future_existing': 5, 'option_added': 0, 'option_existing': 3}
            mock_qs_inst = MagicMock()
            mock_qs_inst._count_option_contracts.return_value = 0
            mock_qs_inst.load_and_preregister_instruments.return_value = {
                'futures_list': ['IF2606'],
                'options_dict': {},
                'subscribed_instruments': ['IF2606'],
                'futures_metadata': {},
                'options_metadata': {},
            }
            mock_qs.return_value = mock_qs_inst
            provider.storage = MagicMock()
            provider.params = {}

            li = LifecycleInit(provider)
            result = li.on_init()
            assert result is True
            assert provider._initialized is True

    def test_init_failure(self):
        provider = MagicMock()
        provider._initialized = False
        provider._lock = threading.Lock()
        provider._state = StrategyState.INITIALIZING
        provider._lifecycle_mgr = MagicMock()
        provider._stats = {'errors_count': 0}

        with patch('ali2026v3_trading.config.config_service.ensure_products_with_retry', side_effect=RuntimeError("fail")), \
             patch('ali2026v3_trading.data.data_service.get_data_service'):
            li = LifecycleInit(provider)
            result = li.on_init()
            assert result is False


class TestInitialize:
    def test_delegates_to_on_init(self):
        provider = MagicMock()
        provider._initialized = True
        provider._lock = threading.Lock()
        li = LifecycleInit(provider)
        result = li.initialize(params={'key': 'val'})
        assert result is True


class TestPrepareRestart:
    def test_destroyed(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._destroyed = True
        li = LifecycleInit(provider)
        result = li.prepare_restart()
        assert result is False

    def test_not_stopped(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._destroyed = False
        provider._state = StrategyState.RUNNING
        li = LifecycleInit(provider)
        result = li.prepare_restart()
        assert result is True

    def test_restart_from_stopped(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._destroyed = False
        provider._state = StrategyState.STOPPED
        provider._is_running = True
        provider._is_paused = True
        provider._initialized = True
        provider._lifecycle_mgr = MagicMock()
        provider._analytics_warmup_done = True
        provider._analytics_warmup_thread = None
        provider._platform_subscribe_thread = None
        provider._platform_subscribe_stop = threading.Event()
        li = LifecycleInit(provider)
        result = li.prepare_restart()
        assert result is True
        assert provider._initialized is False


class TestBuildInstrumentGroups:
    def test_no_storage(self):
        provider = MagicMock()
        li = LifecycleInit(provider)
        result = li._build_instrument_groups(None)
        assert result == ([], {})

    def test_with_storage(self):
        provider = MagicMock()
        li = LifecycleInit(provider)
        mock_storage = MagicMock()
        mock_storage.get_registered_instrument_ids.return_value = ['IF2606']
        with patch('ali2026v3_trading.infra.subscription_service.SubscriptionManager') as mock_sm:
            mock_sm.is_option.return_value = False
            result = li._build_instrument_groups(mock_storage)
            assert 'IF2606' in result[0]


class TestInitAnalyticsServices:
    def test_init_analytics(self):
        provider = MagicMock()
        provider.storage = MagicMock()
        provider._future_ids = set()
        provider._option_ids = set()
        provider.t_type_service = MagicMock()
        li = LifecycleInit(provider)
        with patch.object(li, '_build_instrument_groups', return_value=(['IF2606'], {})), \
             patch.object(li, '_init_t_type_service_and_preload'), \
             patch.object(li, '_register_analytics_jobs'):
            li._init_analytics_services(MagicMock())
            assert provider.analytics_service is None

    def test_init_analytics_error(self):
        provider = MagicMock()
        provider.storage = MagicMock()
        li = LifecycleInit(provider)
        with patch.object(li, '_build_instrument_groups', side_effect=RuntimeError("fail")):
            li._init_analytics_services(MagicMock())
            assert provider.analytics_service is None


class TestInitTTypeServiceAndPreload:
    def test_fallback_to_data_service_subscription_manager(self):
        provider = MagicMock()
        provider.storage = MagicMock()
        provider.storage.subscription_manager = None
        provider._init_instruments_result = {
            'futures_metadata': {'IF2606': {'internal_id': 1, 'year_month': '2606'}},
            'options_metadata': {},
            'futures_list': ['IF2606'],
            'options_dict': {},
        }
        provider._extract_contract_year_month = MagicMock(return_value='2606')
        li = LifecycleInit(provider)

        t_type_service = MagicMock()
        t_type_service._width_cache = MagicMock()
        sm = MagicMock()
        ds = MagicMock()
        ds.subscription_manager = sm

        with patch('ali2026v3_trading.data.t_type_service.get_t_type_service', return_value=t_type_service):
            with patch('ali2026v3_trading.data.data_service.get_data_service', return_value=ds):
                with patch('ali2026v3_trading.data.data_service.get_latest_price', return_value=100.0):
                    li._init_t_type_service_and_preload(provider.storage)

        sm.set_t_type_service.assert_called_once_with(t_type_service)


class TestEnsureAnalyticsReady:
    def test_already_done(self):
        provider = MagicMock()
        provider._analytics_warmup_done = True
        li = LifecycleInit(provider)
        assert li._ensure_analytics_ready() is True

    def test_warmup_thread(self):
        provider = MagicMock()
        provider._analytics_warmup_done = False
        mock_thread = MagicMock()
        provider._analytics_warmup_thread = mock_thread
        li = LifecycleInit(provider)
        result = li._ensure_analytics_ready(timeout=1.0)
        mock_thread.join.assert_called_once()


class TestWarmStorageAsync:
    def test_with_storage(self):
        provider = MagicMock()
        provider.storage = MagicMock()
        li = LifecycleInit(provider)
        li._warm_storage_async()

    def test_no_storage(self):
        provider = MagicMock()
        provider.storage = None
        li = LifecycleInit(provider)
        li._warm_storage_async()


class TestFetchChinaHolidays:
    def test_builtin_holidays(self):
        import urllib.request
        with patch.object(urllib.request, 'urlopen', side_effect=RuntimeError("no network")):
            result = LifecycleInit._fetch_china_holidays(2026)
            assert len(result) > 0

    def test_default_year(self):
        import urllib.request
        with patch.object(urllib.request, 'urlopen', side_effect=RuntimeError("no network")):
            result = LifecycleInit._fetch_china_holidays()
            assert isinstance(result, list)

class TestInitLogging:
    def test_init_logging_no_file(self):
        provider = MagicMock()
        li = LifecycleInit(provider)
        li._init_logging({'log_level': 'DEBUG'})


class TestInitScheduler:
    def test_init_scheduler(self):
        provider = MagicMock()
        li = LifecycleInit(provider)
        li._init_scheduler()
        provider._scheduler_manager.initialize.assert_called_once()


class TestStopScheduler:
    def test_stop_scheduler(self):
        provider = MagicMock()
        li = LifecycleInit(provider)
        with patch('threading.enumerate', return_value=[]):
            li._stop_scheduler()
        provider._scheduler_manager.stop_strategy_jobs.assert_called_once()


class TestAddJobs:
    def test_add_option_status_diagnosis_job(self):
        provider = MagicMock()
        provider.t_type_service = MagicMock()
        li = LifecycleInit(provider)
        li._add_option_status_diagnosis_job()
        provider._scheduler_manager.register_option_diagnosis_task.assert_called_once()

    def test_add_tick_sync_job(self):
        provider = MagicMock()
        provider._data_service = MagicMock()
        li = LifecycleInit(provider)
        li._add_tick_sync_job()
        provider._scheduler_manager.register_cache_flush_task.assert_called_once()

    def test_add_14_contracts_diagnosis_job(self):
        provider = MagicMock()
        provider.storage = MagicMock()
        provider.query_service = MagicMock()
        li = LifecycleInit(provider)
        li._add_14_contracts_diagnosis_job()
        provider._scheduler_manager.register_14_contracts_diagnosis_task.assert_called_once()

    def test_add_trading_jobs(self):
        provider = MagicMock()
        provider._lifecycle_run_id = 'run_001'
        li = LifecycleInit(provider)
        li._add_trading_jobs()
        provider._scheduler_manager.register_trading_jobs.assert_called_once()