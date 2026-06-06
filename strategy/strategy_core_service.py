"""
策略核心服务模块 - Facade (v3.3 瘦身版)
Facade: StrategyCoreService = _LifecycleMixin(composition) + _InstrumentHelperMixin
         + HistoricalKlineMixin + TickHandlerMixin
         + StrategyConfigLayer + StrategyBusinessLayer + StrategyMonitoringLayer
分拆历史: v3.0~v3.2见git log; v3.3(2026-06-05) Facade瘦身809行→≤200行
"""
from __future__ import annotations
import threading, logging, time
from typing import Any, Dict, List, Optional, Callable, Tuple

from ali2026v3_trading.strategy.strategy_lifecycle_mixin import StrategyState, _LifecycleMixin
from ali2026v3_trading.strategy.strategy_instrument_mixin import _InstrumentHelperMixin
from ali2026v3_trading.strategy_historical import HistoricalKlineMixin
from ali2026v3_trading.strategy.strategy_tick_handler import TickHandlerMixin
from ali2026v3_trading.strategy.strategy_config_layer import StrategyConfigLayer
from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
from ali2026v3_trading.strategy.strategy_monitoring_layer import StrategyMonitoringLayer
from ali2026v3_trading.strategy_recovery_mixin import StrategyRecoveryMixin
from ali2026v3_trading.strategy_checkpoint_mixin import StrategyCheckpointMixin

logger = logging.getLogger(__name__)
try:
    from ali2026v3_trading.param_pool.delay_time_sharpe_3d import LiveStrategySelector
except ImportError:
    LiveStrategySelector = None

_L = _LifecycleMixin

class StrategyCoreService(_InstrumentHelperMixin, HistoricalKlineMixin, TickHandlerMixin):
    """策略核心服务 - Facade: 委托到三层子模块 + Mixin组合"""
    MAX_PROCESSED_TRADE_IDS = 10000
    _order_service_init_lock = threading.Lock()

    def __init__(self, strategy_id: str = None, event_bus: Optional[Any] = None):
        self.strategy_id = strategy_id or f"strategy_{int(time.time())}"
        self._init_core_deps(event_bus)
        self._init_attributes(); self._check_startup_dependencies(); self._sync_state_to_store()

    def _init_core_deps(self, event_bus):
        try:
            from ali2026v3_trading.infra.phase_feature_flag import PhaseFeatureFlag
            if PhaseFeatureFlag.is_enabled('USE_BUILDER_INIT'):
                from ali2026v3_trading.strategy.strategy_core_service_builder import StrategyCoreServiceBuilder
                _b = StrategyCoreServiceBuilder()
                _b.set_config(getattr(self, '_params', {})); _b.set_event_bus(event_bus)
                _b.build_state_store().build_managers().build_thread_pools().build_platform_apis().build_monitoring()
                _b.apply_to(self)
                self._callback_group = self._try_import('ali2026v3_trading.infra.callback_registry', 'get_callback_group')
                self._data_access = self._try_import('ali2026v3_trading.data.data_access', 'get_data_access')
                self._strategy_executor = self._create_executor(); return
        except Exception: pass
        self._state_store = self._try_import('ali2026v3_trading.infra.state_store', 'get_state_store')
        self._callback_group = self._try_import('ali2026v3_trading.infra.callback_registry', 'get_callback_group')
        self._data_access = self._try_import('ali2026v3_trading.data.data_access', 'get_data_access')
        self._strategy_executor = self._create_executor()

    @staticmethod
    def _try_import(module_path, func_name):
        try: return getattr(__import__(module_path, fromlist=[func_name]), func_name)()
        except Exception: return None

    def _create_executor(self):
        try:
            import concurrent.futures as _cf
            return _cf.ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"strat_{self.strategy_id[:12]}")
        except Exception as _e:
            logging.warning("[R22-P1-NEW] 策略线程池创建失败: %s", _e); return None

    def _init_attributes(self):
        self._config_layer.init_state(); self._config_layer.init_locks()
        self._config_layer.init_services(event_bus=getattr(self, '_event_bus_param', None))

    def _check_startup_dependencies(self):
        for attr in ('_state_store', '_callback_group', '_data_access'):
            if not hasattr(self, attr) or getattr(self, attr) is None:
                logging.error("[STARTUP-DEP] R17-P1-CFG-P1-05: 关键依赖缺失: %s", attr)
        self._config_layer.check_startup_dependencies()

    def _coordinated_shutdown(self): return self._config_layer.coordinated_shutdown()
    def _atexit_cleanup_executor(self):
        for _a in ('_executor', '_log_writer_executor', '_analysis_executor', '_strategy_executor'):
            _ex = getattr(self, _a, None)
            if _ex and hasattr(_ex, 'shutdown'):
                try: _ex.shutdown(wait=False)
                except Exception: pass

    def _register_callbacks(self):
        if self._callback_group is None: return
        for name in ('_extract_contract_year_month', '_extract_runtime_market_center', '_get_fallback_market_center', '_publish_event'):
            fn = getattr(self, name, None)
            if callable(fn): self._callback_group.get_registry(name).register(fn)

    _PHASE2_GETATTR_MAP = {'_state': '_lifecycle_mgr', '_is_running': '_lifecycle_mgr', '_is_paused': '_lifecycle_mgr',
        '_destroyed': '_lifecycle_mgr', '_initialized': '_lifecycle_mgr', '_lock': '_lifecycle_mgr',
        '_stats': '_lifecycle_mgr', '_safety_meta_layer': '_lifecycle_mgr'}

    def __getattr__(self, name: str):
        mgr_name = self._PHASE2_GETATTR_MAP.get(name)
        if mgr_name is not None:
            mgr = object.__getattribute__(self, mgr_name)
            _m = {'_state': 'state', '_is_running': 'is_running', '_is_paused': 'is_paused', '_destroyed': 'destroyed', '_initialized': 'initialized'}
            if name in _m: return getattr(mgr, _m[name])
            if name == '_lock': return getattr(mgr, 'lock', object.__getattribute__(self, '_state_lock'))
            if name == '_stats': return getattr(mgr, 'stats', None)
            if name == '_safety_meta_layer': return getattr(mgr, 'safety_meta_layer', None)
        if name in type(self)._LIFECYCLE_DELEGATE:
            _m = getattr(_L, name)
            if isinstance(_m, staticmethod): return _m.__func__
            return lambda *a, **kw: _m(self, *a, **kw)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    @property
    def storage(self): return _L.storage.fget(self)
    def transition_to(self, new_state): return _L.transition_to(self, new_state)
    def bind_platform_apis(self, strategy_obj): return _L.bind_platform_apis(self, strategy_obj)
    def save_state(self): return _L.save_state(self)
    def on_init(self, *a, **kw): return _L.on_init(self, *a, **kw)
    def on_start(self): return _L.on_start(self)
    def on_stop(self): return _L.on_stop(self)
    def on_destroy(self): return _L.on_destroy(self)
    def initialize(self, params=None): return _L.initialize(self, params)
    def start(self): return _L.start(self)
    def pause(self): return _L.pause(self)
    def resume(self): return _L.resume(self)
    def stop(self): return _L.stop(self)
    def destroy(self): return _L.destroy(self)
    def get_state(self): return _L.get_state(self)
    def is_running(self): return _L.is_running(self)
    def is_paused(self): return _L.is_paused(self)
    def is_trading(self): return _L.is_trading(self)
    def get_uptime(self): return _L.get_uptime(self)
    def health_check(self): return _L.health_check(self)
    def enter_parallel_running(self, shadow_strategy=None, comparison_callback=None, duration_sec=3600.0): return _L.enter_parallel_running(self, shadow_strategy, comparison_callback, duration_sec)
    def exit_parallel_running(self, promote_new=False): return _L.exit_parallel_running(self, promote_new)
    def compare_parallel_results(self, old_result, new_result): return _L.compare_parallel_results(self, old_result, new_result)
    def get_parallel_running_status(self): return _L.get_parallel_running_status(self)
    def record_parallel_result(self, key, result): return _L.record_parallel_result(self, key, result)
    def get_parallel_results(self): return _L.get_parallel_results(self)
    def prepare_restart(self): return _L.prepare_restart(self)
    def record_tick(self): return _L.record_tick(self)
    def record_trade(self): return _L.record_trade(self)
    def record_signal(self): return _L.record_signal(self)
    def record_error(self, error_message): return _L.record_error(self, error_message)
    def get_stats(self): return _L.get_stats(self)
    @staticmethod
    def _extract_runtime_market_center(strategy_obj): return _L._extract_runtime_market_center(strategy_obj)
    @staticmethod
    def _fetch_china_holidays(year=None): return _L._fetch_china_holidays(year)
    @staticmethod
    def _should_probe_t_type_future(product): return _L._should_probe_t_type_future(product)
    def _init_logging(self, params=None): _L._init_logging(self, params); self._config_layer.init_logging(params)
    def _init_scheduler(self): _L._init_scheduler(self); self._config_layer.init_scheduler()
    def _stop_scheduler(self): _L._stop_scheduler(self); self._config_layer.stop_scheduler()
    _LIFECYCLE_DELEGATE = frozenset({'_start_platform_subscribe_async', '_platform_subscribe_worker',
        '_do_bind_platform_apis', '_inject_runtime_context', '_get_fallback_market_center',
        '_init_analytics_services', '_build_instrument_groups', '_resolve_option_underlying_id',
        '_init_t_type_service_and_preload', '_register_analytics_jobs', '_start_analytics_warmup_async',
        '_ensure_analytics_ready', '_warm_storage_async', '_start_historical_kline_load_async',
        '_unsubscribe_all_instruments', '_shutdown_runtime_services', '_log_resource_ownership_table',
        '_log_t_type_future_probe', '_add_option_status_diagnosis_job', '_add_tick_sync_job',
        '_add_14_contracts_diagnosis_job', '_add_trading_jobs', '_ensure_check_pending_orders_job', '_publish_event'})


    def _auto_recovery_flow(self): return StrategyRecoveryMixin._auto_recovery_flow(self)
    def _watchdog_restart(self, max_restarts=3, cooldown_sec=60): return StrategyRecoveryMixin._watchdog_restart(self, max_restarts, cooldown_sec)
    def recover_from_checkpoint(self): return StrategyRecoveryMixin.recover_from_checkpoint(self)
    def save_checkpoint(self, reason='manual'): return StrategyCheckpointMixin.save_checkpoint(self, reason)
    def _on_system_event_checkpoint(self, event): return StrategyCheckpointMixin._on_system_event_checkpoint(self, event)
    def _sync_state_to_store(self): return StrategyCheckpointMixin._sync_state_to_store(self)

    def _ensure_order_service(self): return self._business_layer.ensure_order_service()
    def _ensure_position_service(self): return self._business_layer.ensure_position_service()
    def _ensure_hft_engine(self): return self._business_layer.ensure_hft_engine()
    def _ensure_snapshot_collector(self, symbol=""): return self._business_layer.ensure_snapshot_collector(symbol=symbol)
    def get_snapshot_collector(self): return self._business_layer.get_snapshot_collector()
    def on_order(self, order_data, *a, **kw): return self._business_layer.on_order(order_data, *a, **kw)
    def on_trade(self, trade_data, *a, **kw): return self._business_layer.on_trade(trade_data, *a, **kw)
    def _update_position_from_trade(self, trade_data): return self._business_layer.update_position_from_trade(trade_data)
    def execute_option_trading_cycle(self): return self._business_layer.execute_option_trading_cycle()
    def _check_ecosystem_exclusion(self, targets): return self._business_layer._check_ecosystem_exclusion(targets)
    def _feed_shadow_engine(self, targets, open_reason): return self._business_layer._feed_shadow_engine(targets, open_reason)

    def _resolve_open_reason(self): return self._config_layer.resolve_open_reason()
    def check_position_risk(self): return self._monitoring_layer.check_position_risk()
    def get_health_status(self): return self._monitoring_layer.get_health_status()
    def confirm_daily_resume(self, caller_id="unknown"): return self._monitoring_layer.confirm_daily_resume(caller_id=caller_id)
    def is_hard_stop_triggered(self): return self._monitoring_layer.is_hard_stop_triggered()
    def emergency_stop(self, caller_id="unknown", reason=""): return self._monitoring_layer.emergency_stop(caller_id=caller_id, reason=reason)
    def run_regression_check(self, test_cases=None, test_func=None): return self._monitoring_layer.run_regression_check(test_cases=test_cases, test_func=test_func)

# re-export保持向后兼容
from ali2026v3_trading.strategy.strategy_2026 import StrategyParams, Strategy2026
