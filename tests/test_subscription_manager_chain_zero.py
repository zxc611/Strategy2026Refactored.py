import inspect


def test_data_service_subscription_manager_runtime_contracts():
    from ali2026v3_trading.data.data_service import DataService

    prop_source = inspect.getsource(DataService.subscription_manager.fget)
    bind_source = inspect.getsource(DataService.bind_data_manager)

    assert "__dict__.get('_subscription_manager')" in prop_source
    assert "SubscriptionManager(data_manager=self)" in prop_source
    assert "bind_method = getattr(_sm, 'bind_data_manager', None)" in bind_source
    assert "return _sm" in bind_source


def test_lifecycle_callbacks_has_data_service_fallback():
    from ali2026v3_trading.lifecycle.lifecycle_callbacks import LifecycleCallbacks

    source = inspect.getsource(LifecycleCallbacks.on_start)
    assert "DataService fallback for subscription_manager failed" in source
    assert "get_data_service" in source
    assert "subscribe_all_instruments" in source


def test_strategy_historical_has_data_service_fallback():
    from ali2026v3_trading.strategy import strategy_historical as sh

    source = inspect.getsource(sh.HistoricalKlineMixin._load_historical_klines_once)
    assert "get_data_service" in source
    assert "record_kline_received" in source


def test_health_monitor_has_data_service_fallback():
    from ali2026v3_trading.infra import health_monitor as hm

    source = inspect.getsource(hm)
    assert "get_data_service" in source
    assert "data_service subscription_manager fallback failed" in source


def test_tick_processing_auxiliary_paths_have_data_service_fallback():
    from ali2026v3_trading.strategy.tick_processing_service import process_tick_core, output_periodic_summary

    tick_source = inspect.getsource(process_tick_core)
    summary_source = inspect.getsource(output_periodic_summary)

    assert "record_tick_received" in tick_source
    assert "get_data_service" in tick_source
    assert "log_subscription_success_summary" in summary_source
    assert "get_data_service" in summary_source