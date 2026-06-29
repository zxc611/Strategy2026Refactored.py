# MODULE_ID: M2-404
import pytest
import threading
from unittest.mock import MagicMock, patch, PropertyMock
from ali2026v3_trading.lifecycle.lifecycle_bind import LifecycleBind
from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState


class TestLifecycleBindInit:
    def test_init(self):
        provider = MagicMock()
        lb = LifecycleBind(provider)
        assert lb.p is provider


class TestExtractRuntimeMarketCenter:
    def test_from_strategy_market_center(self):
        strategy = MagicMock()
        strategy.market_center = "mc_obj"
        result = LifecycleBind._extract_runtime_market_center(strategy)
        assert result == "mc_obj"

    def test_from_strategy_infini(self):
        strategy = MagicMock(spec=[])
        del strategy.market_center
        strategy.infini = MagicMock()
        strategy.infini.market_center = "mc_from_infini"
        result = LifecycleBind._extract_runtime_market_center(strategy)
        assert result == "mc_from_infini"

    def test_none_strategy(self):
        result = LifecycleBind._extract_runtime_market_center(None)
        assert result is None

    def test_no_market_center(self):
        strategy = MagicMock(spec=[])
        del strategy.market_center
        strategy.infini = None
        result = LifecycleBind._extract_runtime_market_center(strategy)
        assert result is None


class TestBindPlatformApis:
    def test_no_lock_logs_error(self):
        provider = MagicMock()
        del provider._lock
        lb = LifecycleBind(provider)
        lb.bind_platform_apis(MagicMock())

    def test_with_subscribe(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._runtime_market_center = None
        provider._fallback_market_center = None
        provider._api_ready = False
        provider._kline_ready = False
        provider._lifecycle_platform = MagicMock()

        strategy = MagicMock()
        strategy.sub_market_data = MagicMock(return_value=None)
        strategy.unsub_market_data = MagicMock(return_value=None)

        lb = LifecycleBind(provider)
        provider._do_bind_platform_apis = lb._do_bind_platform_apis

        with patch('ali2026v3_trading.config.config_service.resolve_product_exchange', return_value='CFFEX'):
            with patch('ali2026v3_trading.config.params_service._read_param', return_value=None):
                lb.bind_platform_apis(strategy)

    def test_without_subscribe(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._runtime_market_center = None
        provider._fallback_market_center = None
        provider._api_ready = False
        provider._kline_ready = False
        provider._lifecycle_platform = MagicMock()

        strategy = MagicMock(spec=[])
        del strategy.sub_market_data
        del strategy.unsub_market_data

        lb = LifecycleBind(provider)
        provider._do_bind_platform_apis = lb._do_bind_platform_apis

        with patch('ali2026v3_trading.config.config_service.resolve_product_exchange', return_value='CFFEX'):
            with patch('ali2026v3_trading.config.params_service._read_param', return_value=None):
                lb.bind_platform_apis(strategy)

    def test_binds_subscription_manager_to_data_service(self):
        provider = MagicMock()
        provider._lock = threading.Lock()
        provider._runtime_market_center = None
        provider._fallback_market_center = None
        provider._api_ready = False
        provider._kline_ready = False
        provider._lifecycle_platform = MagicMock()
        provider._platform_insert_order = None
        provider._ensure_order_service = MagicMock()
        provider._inject_runtime_context = MagicMock()
        provider._extract_runtime_market_center = MagicMock(return_value=None)
        provider._get_fallback_market_center = MagicMock(return_value=None)
        provider.transition_to = MagicMock()
        provider.storage = MagicMock()
        provider.storage.subscription_manager = MagicMock()

        strategy = MagicMock()
        strategy.sub_market_data = MagicMock(return_value=True)
        strategy.unsub_market_data = MagicMock(return_value=True)

        lb = LifecycleBind(provider)
        provider._do_bind_platform_apis = lb._do_bind_platform_apis
        data_service = MagicMock()
        with patch('ali2026v3_trading.config.config_service.resolve_product_exchange', return_value='CFFEX'):
            with patch('ali2026v3_trading.config.params_service._read_param', return_value=None):
                with patch('ali2026v3_trading.data.data_service.get_data_service', return_value=data_service):
                    lb.bind_platform_apis(strategy)

        provider.storage.subscription_manager.bind_data_manager.assert_called_once_with(data_service)
        data_service.bind_data_manager.assert_called_once_with(data_service)


class TestInjectRuntimeContext:
    def test_inject_dict_params(self):
        provider = MagicMock()
        provider._runtime_market_center = None
        provider._fallback_market_center = None
        provider.params = {'key': 'val'}
        lb = LifecycleBind(provider)
        lb._inject_runtime_context(MagicMock())
        assert 'strategy' in provider.params

    def test_inject_no_params(self):
        provider = MagicMock()
        provider.params = None
        lb = LifecycleBind(provider)
        lb._inject_runtime_context(MagicMock())

    def test_inject_none_strategy(self):
        provider = MagicMock()
        provider.params = {'key': 'val'}
        lb = LifecycleBind(provider)
        lb._inject_runtime_context(None)


class TestGetFallbackMarketCenter:
    def test_existing_fallback(self):
        provider = MagicMock()
        provider._fallback_market_center = "existing_mc"
        lb = LifecycleBind(provider)
        result = lb._get_fallback_market_center()
        assert result == "existing_mc"

    def test_no_pythongo(self):
        provider = MagicMock()
        provider._fallback_market_center = None
        lb = LifecycleBind(provider)
        with patch.dict('sys.modules', {'pythongo.core': None}):
            result = lb._get_fallback_market_center()
            assert result is None


class TestStartPlatformSubscribeAsync:
    def test_empty_list(self):
        provider = MagicMock()
        provider._platform_subscribe_lock = threading.Lock()
        lb = LifecycleBind(provider)
        lb._start_platform_subscribe_async([])

    def test_none_list(self):
        provider = MagicMock()
        provider._platform_subscribe_lock = threading.Lock()
        lb = LifecycleBind(provider)
        lb._start_platform_subscribe_async(None)

    def test_already_running(self):
        provider = MagicMock()
        provider._platform_subscribe_lock = threading.Lock()
        provider._platform_subscribe_thread = MagicMock()
        provider._platform_subscribe_thread.is_alive.return_value = True
        provider._platform_subscribe_stop = threading.Event()
        lb = LifecycleBind(provider)
        lb._start_platform_subscribe_async(['IF2606'])


class TestUnsubscribeAllInstruments:
    def test_unsubscribe(self):
        provider = MagicMock()
        provider._platform_subscribe_stop = threading.Event()
        provider.unsubscribe = MagicMock()
        provider._subscribed_instruments = ['IF2606', 'IH2606']
        lb = LifecycleBind(provider)
        lb._unsubscribe_all_instruments()

    def test_unsubscribe_not_callable(self):
        provider = MagicMock()
        provider._platform_subscribe_stop = threading.Event()
        provider.unsubscribe = "not_callable"
        lb = LifecycleBind(provider)
        lb._unsubscribe_all_instruments()

    def test_unsubscribe_empty_list(self):
        provider = MagicMock()
        provider._platform_subscribe_stop = threading.Event()
        provider.unsubscribe = MagicMock()
        provider._subscribed_instruments = []
        lb = LifecycleBind(provider)
        lb._unsubscribe_all_instruments()


class TestStartHistoricalKlineLoadAsync:
    def test_starts_thread(self):
        provider = MagicMock()
        provider.strategy_id = "test_strategy"
        provider._start_historical_kline_load = MagicMock()
        lb = LifecycleBind(provider)
        lb._start_historical_kline_load_async()