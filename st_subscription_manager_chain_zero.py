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


def test_subscription_tick_gap_audit_uses_runtime_duckdb_path():
    from ali2026v3_trading.infra.subscription_service import SubscriptionCoreService

    source = inspect.getsource(SubscriptionCoreService.audit_subscription_tick_gap)

    assert "_resolve_duckdb_file" in source
    assert "dirname(_os_duckdb.dirname(_os_duckdb.abspath(__file__)))" not in source


def test_subscription_tick_gap_audit_declares_local_call_denominator():
    from ali2026v3_trading.infra.subscription_service import SubscriptionCoreService

    audit_source = inspect.getsource(SubscriptionCoreService.audit_subscription_tick_gap)
    subscribe_source = inspect.getsource(SubscriptionCoreService.subscribe_all_instruments)

    assert "local_subscription_calls" in audit_source
    assert "platform_ack_observed=false" in audit_source
    assert "bulk_subscription_completed" in subscribe_source
    assert "audit_subscription_tick_gap" in subscribe_source


def test_subscription_tick_gap_audit_counts_received_ticks_from_local_calls():
    from ali2026v3_trading.infra.subscription_service import SubscriptionCoreService

    svc = SubscriptionCoreService()
    svc.record_subscription(["IF2606", "ag2609C8000"])
    svc._tick_received_contracts.add("IF2606")

    result = svc.audit_subscription_tick_gap(reason="unit_test")

    assert result["local_subscription_calls"] == 2
    assert result["received_tick"] == 1
    assert result["gap"] == 1


def test_lifecycle_start_does_not_label_second_registration_as_platform_subscribe():
    from ali2026v3_trading.lifecycle.lifecycle_callbacks import LifecycleCallbacks

    source = inspect.getsource(LifecycleCallbacks.on_start)

    assert "平台订阅已在后台启动" not in source
    assert "跳过二次后台平台订阅入口" in source
    assert "local_subscription_calls" in source


def test_health_monitor_uses_runtime_duckdb_path(monkeypatch, tmp_path):
    import duckdb
    from datetime import date
    from ali2026v3_trading.data import ds_realtime_cache
    from ali2026v3_trading.infra import health_monitor as hm

    db_path = tmp_path / "runtime_strategy.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE instruments_registry(instrument_id VARCHAR)")
    con.execute("CREATE TABLE futures_instruments(instrument_id VARCHAR)")
    con.execute("CREATE TABLE option_instruments(instrument_id VARCHAR)")
    con.execute("CREATE TABLE ticks_raw(instrument_id VARCHAR, date DATE, sync_status VARCHAR)")
    con.execute("CREATE TABLE klines_raw(internal_id BIGINT, trade_date DATE)")
    con.execute("""
        CREATE TABLE chain_coverage_audit(
            audit_time TIMESTAMP,
            config_count BIGINT,
            subscription_confirmed_count BIGINT,
            tick_return_count BIGINT,
            tick_buffer_count BIGINT,
            ticks_raw_count BIGINT,
            klines_raw_count BIGINT,
            simulated_coverage_tick_count BIGINT,
            simulated_coverage_kline_count BIGINT
        )
    """)
    con.execute("INSERT INTO instruments_registry VALUES ('a2609'), ('a2609C3000')")
    con.execute("INSERT INTO futures_instruments VALUES ('a2609')")
    con.execute("INSERT INTO option_instruments VALUES ('a2609C3000')")
    con.execute("INSERT INTO ticks_raw VALUES ('a2609', DATE '2026-06-30', ''), ('a2609C3000', DATE '2026-06-30', 'simulated_coverage')")
    con.execute("INSERT INTO klines_raw VALUES (1, DATE '2026-06-30'), (2, DATE '2026-06-30')")
    con.execute("INSERT INTO chain_coverage_audit VALUES (TIMESTAMP '2026-06-30 21:12:00', 2, 2, 1, 1, 2, 2, 1, 0)")
    con.execute("INSERT INTO chain_coverage_audit VALUES (TIMESTAMP '2026-07-01 01:00:00', 99, 99, 99, 99, 99, 99, 99, 99)")
    con.close()

    monkeypatch.setattr(ds_realtime_cache, "_resolve_duckdb_file", lambda: str(db_path))
    monkeypatch.setattr(hm, "_current_trade_date_for_coverage", lambda: date(2026, 6, 30))

    counts = hm._load_full_chain_entry_counts()

    assert hm._resolve_diagnosis_duckdb_file() == str(db_path)
    assert counts["registry_count"] == 2
    assert counts["ticks_raw_today_count"] == 2
    assert counts["real_tick_today_count"] == 1
    assert counts["audit_rows"] == 1
    assert counts["audit_config_count"] == 2
    assert counts["audit_tick_return_count"] == 1


def test_health_monitor_has_no_local_duckdb_fallback():
    from ali2026v3_trading.infra import health_monitor as hm

    source = inspect.getsource(hm._resolve_diagnosis_duckdb_file)

    assert "_resolve_duckdb_file" in source
    assert "strategy.duckdb" not in source
    assert "os.path.join" not in source


def test_health_monitor_prefers_audit_trade_date(monkeypatch, tmp_path):
    import duckdb
    from datetime import date
    from ali2026v3_trading.data import ds_realtime_cache
    from ali2026v3_trading.infra import health_monitor as hm

    db_path = tmp_path / "runtime_strategy.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE instruments_registry(instrument_id VARCHAR)")
    con.execute("CREATE TABLE futures_instruments(instrument_id VARCHAR)")
    con.execute("CREATE TABLE option_instruments(instrument_id VARCHAR)")
    con.execute("CREATE TABLE ticks_raw(instrument_id VARCHAR, date DATE, sync_status VARCHAR)")
    con.execute("CREATE TABLE klines_raw(internal_id BIGINT, trade_date DATE)")
    con.execute("""
        CREATE TABLE chain_coverage_audit(
            audit_time TIMESTAMP,
            trade_date DATE,
            config_count BIGINT,
            subscription_confirmed_count BIGINT,
            tick_return_count BIGINT,
            tick_buffer_count BIGINT,
            ticks_raw_count BIGINT,
            klines_raw_count BIGINT,
            simulated_coverage_tick_count BIGINT,
            simulated_coverage_kline_count BIGINT
        )
    """)
    con.execute("INSERT INTO chain_coverage_audit VALUES (TIMESTAMP '2026-07-01 00:30:00', DATE '2026-06-30', 2, 2, 1, 1, 2, 2, 1, 0)")
    con.execute("INSERT INTO chain_coverage_audit VALUES (TIMESTAMP '2026-07-01 01:00:00', DATE '2026-07-01', 99, 99, 99, 99, 99, 99, 99, 99)")
    con.close()

    monkeypatch.setattr(ds_realtime_cache, "_resolve_duckdb_file", lambda: str(db_path))
    monkeypatch.setattr(hm, "_current_trade_date_for_coverage", lambda: date(2026, 6, 30))

    counts = hm._load_full_chain_entry_counts()

    assert counts["audit_rows"] == 1
    assert counts["audit_config_count"] == 2
    assert counts["audit_tick_return_count"] == 1


def test_tick_processing_auxiliary_paths_have_data_service_fallback():
    from ali2026v3_trading.strategy.tick_processing_service import process_tick_core, output_periodic_summary

    tick_source = inspect.getsource(process_tick_core)
    summary_source = inspect.getsource(output_periodic_summary)

    assert "record_tick_received" in tick_source
    assert "get_data_service" in tick_source
    assert "log_subscription_success_summary" in summary_source
    assert "get_data_service" in summary_source