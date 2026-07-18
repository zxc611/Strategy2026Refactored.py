# MODULE_ID: M1-267
"""策略核心服务 - 纯Facade (G2b Mixin消灭)
所有逻辑委托到6个独立服务 + 3个Layer子模块，零Mixin继承。服务通过ServiceFactory注入，确保策略级隔离(PA-03)。
+ ServiceFactory (从service_factory.py迁入)
"""
from __future__ import annotations
import threading, logging, time, os
from typing import Any, Dict, List, Optional, Callable, Tuple

from strategy.instrument_service import KlineDataService
from infra.exceptions import FutureLeakException
from strategy.tick_processing_service import TickProcessingService, MarketEvent, TickEvent, BarCompletedEvent
from strategy.persistence_service import RecoveryService
from strategy.persistence_service import CheckpointService
from strategy.lifecycle_service import LifecycleService
from strategy.instrument_service import InstrumentService
from strategy.strategy_config_layer import StrategyConfigLayer
from strategy.strategy_business_layer import StrategyBusinessLayer
from strategy.strategy_monitoring_layer import StrategyMonitoringLayer
from lifecycle.lifecycle_state_machine import StrategyState


# ============================================================
# ServiceFactory (从 service_factory.py 迁入)
# 职责: 策略级服务工厂 — 确保每个策略获得独立的服务实例（PA-03隔离）
# ============================================================

class ServiceFactory:
    """策略级服务工厂 — 确保每个策略获得独立的服务实例（PA-03隔离）"""

    def __init__(
        self,
        strategy_id: str,
        state_store: Optional[Any] = None,
        callback_group: Optional[Any] = None,
        is_shadow: bool = False,
        is_backtest: bool = False,
    ):
        self._strategy_id = str(strategy_id)
        self._state_store = state_store
        self._callback_group = callback_group
        self._is_shadow = is_shadow
        self._is_backtest = is_backtest
        self._namespace = f"{strategy_id}_shadow" if is_shadow else strategy_id
        # Lazy-created service instances
        self._kline_service = None
        self._tick_service = None
        self._recovery_service = None
        self._checkpoint_service = None
        self._lifecycle_service = None
        self._instrument_service = None

    @property
    def namespace(self) -> str:
        return self._namespace

    def create_kline_service(self) -> Any:
        """创建KlineDataService实例"""
        from strategy.instrument_service import KlineDataService
        svc = KlineDataService(
            state_store=self._state_store,
            callback_group=self._callback_group,
            strategy_id=self._strategy_id,
            is_backtest=self._is_backtest,
        )
        self._kline_service = svc
        return svc

    def create_tick_processing_service(self, **fns) -> Any:
        """创建TickProcessingService实例"""
        from strategy.tick_processing_service import TickProcessingService
        svc = TickProcessingService(
            state_store=self._state_store,
            callback_group=self._callback_group,
            strategy_id=self._strategy_id,
            is_backtest=self._is_backtest,
            **fns,
        )
        self._tick_service = svc
        return svc

    def create_recovery_service(self, **fns) -> RecoveryService:
        """创建RecoveryService实例"""
        svc = RecoveryService(
            strategy_id=self._strategy_id,
            state_store=self._state_store,
            **fns,
        )
        self._recovery_service = svc
        return svc

    def create_checkpoint_service(self, **fns) -> CheckpointService:
        """创建CheckpointService实例"""
        svc = CheckpointService(
            strategy_id=self._strategy_id,
            state_store=self._state_store,
            **fns,
        )
        self._checkpoint_service = svc
        return svc

    def create_lifecycle_service(self, provider=None) -> Any:
        """创建LifecycleService实例"""
        from strategy.lifecycle_service import LifecycleService
        svc = LifecycleService(provider=provider, strategy_id=self._strategy_id)
        self._lifecycle_service = svc
        return svc

    def create_instrument_service(self) -> Any:
        """创建InstrumentService实例"""
        from strategy.instrument_service import InstrumentService
        svc = InstrumentService()
        self._instrument_service = svc
        return svc

    # -- Convenience getters with lazy init --

    @property
    def kline_service(self) -> Any:
        if self._kline_service is None:
            self._kline_service = self.create_kline_service()
        return self._kline_service

    @property
    def tick_processing_service(self) -> Any:
        if self._tick_service is None:
            self._tick_service = self.create_tick_processing_service()
        return self._tick_service

    @property
    def recovery_service(self) -> RecoveryService:
        if self._recovery_service is None:
            self._recovery_service = self.create_recovery_service()
        return self._recovery_service

    @property
    def checkpoint_service(self) -> CheckpointService:
        if self._checkpoint_service is None:
            self._checkpoint_service = self.create_checkpoint_service()
        return self._checkpoint_service

class StrategyCoreService:
    """纯Facade: 零Mixin继承，所有逻辑委托到服务实例"""
    MAX_PROCESSED_TRADE_IDS = 10000; _order_service_init_lock = threading.Lock()

    def __init__(self, strategy_id: str = None, event_bus: Optional[Any] = None):
        self.strategy_id = strategy_id or f"strategy_{int(time.time())}"
        self._heartbeat_interval_sec = float(os.environ.get('HEARTBEAT_INTERVAL_SEC', '30.0'))
        self._heartbeat_max_failures = int(os.environ.get('HEARTBEAT_MAX_FAILURES', '3'))
        self._init_core_deps(event_bus); self._init_attributes(); self._check_startup_dependencies(); self._sync_state_to_store()

    def _init_core_deps(self, event_bus):
        try:
            from infra.metrics_registry import PhaseFeatureFlag
            if PhaseFeatureFlag.is_enabled('USE_BUILDER_INIT'):
                from strategy.strategy_core_service import StrategyCoreServiceBuilder
                _b = StrategyCoreServiceBuilder()
                _b.set_config(getattr(self, '_params', {})); _b.set_event_bus(event_bus)
                _b.build_state_store().build_managers().build_thread_pools().build_platform_apis().build_monitoring()
                _b.apply_to(self); self._init_services(event_bus); return
        except (ImportError, AttributeError, TypeError) as _builder_err:
            logging.debug("[StrategyCoreService] Builder初始化失败，回退手动导入: %s", _builder_err)
        self._state_store = self._try_import('infra.service_contracts', 'get_state_store')
        # FIX-R7: callback_registry模块已重构至registry_service，更新引用路径
        self._callback_group = self._try_import('infra.registry_service', 'get_callback_group')
        self._data_access = self._try_import('data.data_access', 'get_data_access')
        self._strategy_executor = self._create_executor(); self._init_services(event_bus)

    def _init_services(self, event_bus):
        self._service_factory = ServiceFactory(strategy_id=self.strategy_id, state_store=self._state_store, callback_group=self._callback_group)
        self._lifecycle_svc = self._service_factory.create_lifecycle_service(provider=self)
        self._instrument_svc = self._service_factory.create_instrument_service()
        self._kline_svc = self._service_factory.create_kline_service()
        _eos, _eps = lambda: getattr(self, '_ensure_order_service', lambda: None)(), lambda: getattr(self, '_ensure_position_service', lambda: None)()
        self._tick_svc = self._service_factory.create_tick_processing_service(
            ensure_order_service_fn=getattr(self, '_ensure_order_service', None),
            ensure_position_service_fn=getattr(self, '_ensure_position_service', None),
            ensure_hft_engine_fn=getattr(self, '_ensure_hft_engine', None))
        _g = lambda k, d=None: lambda: getattr(self, k, d)
        self._recovery_svc = self._service_factory.create_recovery_service(
            ensure_order_service_fn=_eos, ensure_position_service_fn=_eps,
            get_order_service_fn=_g('_order_service'), get_position_service_fn=_g('_position_service'),
            get_signal_service_fn=_g('_signal_service'), get_stats_fn=lambda: getattr(self, '_stats', {}),
            get_state_fn=_g('_state'), set_state_fn=lambda s: setattr(self, '_state', s))
        self._checkpoint_svc = self._service_factory.create_checkpoint_service(
            ensure_order_service_fn=_eos, get_order_service_fn=_g('_order_service'),
            get_position_service_fn=_g('_position_service'),
            get_signal_service_fn=_g('_signal_service'), get_stats_fn=lambda: getattr(self, '_stats', {}),
            get_state_fn=_g('_state'), get_attr_fn=lambda k: getattr(self, k, None),
            set_attr_fn=lambda k, v: setattr(self, k, v), has_attr_fn=lambda k: hasattr(self, k),
            recover_from_checkpoint_fn=self.recover_from_checkpoint)
        for _la, _lc in (('_config_layer', StrategyConfigLayer), ('_business_layer', StrategyBusinessLayer), ('_monitoring_layer', StrategyMonitoringLayer)):
            if not hasattr(self, _la) or getattr(self, _la) is None: setattr(self, _la, _lc(provider=self))

    # P1-27[v6] SingletonRegistry cache for _try_import to prevent duplicate instance creation.
    class _SingletonRegistry:
        _cache = {}  # (module_path, func_name) -> instance
        @classmethod
        def get(cls, module_path, func_name):
            key = (module_path, func_name)
            if key not in cls._cache:
                try:
                    obj = getattr(__import__(module_path, fromlist=[func_name]), func_name)()
                    cls._cache[key] = obj
                except (ValueError, KeyError, TypeError, AttributeError, ImportError, ModuleNotFoundError) as _r3_err:
                    cls._cache[key] = None
            return cls._cache[key]

    @staticmethod
    def _try_import(module_path, func_name):
        return StrategyCoreService._SingletonRegistry.get(module_path, func_name)

    def _create_executor(self):
        try:
            import concurrent.futures as _cf
            _sid_str = str(self.strategy_id)
            return _cf.ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"strat_{_sid_str[:12]}")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _e:
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
                except (RuntimeError, AttributeError): pass
        _tp = getattr(self, '_thread_pools', None)
        if isinstance(_tp, dict):
            for _name, _pool in list(_tp.items()):
                if _pool and hasattr(_pool, 'shutdown'):
                    try: _pool.shutdown(wait=False)
                    except (RuntimeError, AttributeError): pass
    def _register_callbacks(self):
        if self._callback_group is None: return
        for name in ('_extract_contract_year_month', '_extract_runtime_market_center', '_get_fallback_market_center', '_publish_event'):
            fn = getattr(self, name, None)
            if callable(fn): self._callback_group.get_registry(name).register(fn)

    _PHASE2_GETATTR_MAP = {'_state':'_lifecycle_mgr','_is_running':'_lifecycle_mgr','_is_paused':'_lifecycle_mgr','_is_trading':'_lifecycle_mgr','_destroyed':'_lifecycle_mgr','_initialized':'_lifecycle_mgr','_lock':'_lifecycle_mgr','_stats':'_lifecycle_mgr','_safety_meta_layer':'_lifecycle_mgr'}
    _PHASE2_SETATTR_MAP = {'_state':('state',None),'_is_running':('is_running',None),'_is_paused':('is_paused',None),'_is_trading':('_is_trading',None)}
    def __setattr__(self, name: str, value):
        if name in type(self)._PHASE2_SETATTR_MAP:
            _mgr_attr, _ = type(self)._PHASE2_SETATTR_MAP[name]
            try:
                _mgr = object.__getattribute__(self, '_lifecycle_mgr')
                setattr(_mgr, _mgr_attr, value)
                return
            except (AttributeError, KeyError):
                pass
        object.__setattr__(self, name, value)
    def __getattr__(self, name: str):
        mgr_name = self._PHASE2_GETATTR_MAP.get(name)
        if mgr_name is not None:
            mgr = object.__getattribute__(self, mgr_name)
            _m = {'_state':'state','_is_running':'is_running','_is_paused':'is_paused','_is_trading':'_is_trading','_destroyed':'destroyed','_initialized':'initialized'}
            if name in _m: return getattr(mgr, _m[name])
            if name == '_lock': return getattr(mgr, 'lock', object.__getattribute__(self, '_state_lock'))
            if name == '_stats': return getattr(mgr, 'stats', None)
            if name == '_safety_meta_layer': return getattr(mgr, 'safety_meta_layer', None)
        if name in type(self)._LIFECYCLE_DELEGATE:
            return getattr(object.__getattribute__(self, '_lifecycle_svc'), name)
        for _sa, _ma in (('_kline_svc','_historical_mgr'),('_instrument_svc','_instrument_mgr')):
            _svc = object.__getattribute__(self, _sa) if _sa in self.__dict__ else None
            if _svc is not None:
                _mgr = getattr(_svc, _ma, None)
                if _mgr is not None and hasattr(_mgr, name): return getattr(_mgr, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    @property
    def storage(self): return self._lifecycle_svc.storage
    def transition_to(self, ns): return self._lifecycle_svc.transition_to(ns)
    def bind_platform_apis(self, so): return self._lifecycle_svc.bind_platform_apis(so)
    def save_state(self): return self._lifecycle_svc.save_state()
    def on_init(self, *a, **kw): return self._lifecycle_svc.on_init(*a, **kw)
    def on_start(self): return self._lifecycle_svc.on_start()
    def on_stop(self): return self._lifecycle_svc.on_stop()
    def on_destroy(self): return self._lifecycle_svc.on_destroy()
    def initialize(self, p=None): return self._lifecycle_svc.initialize(p)
    def start(self): return self._lifecycle_svc.start()
    def pause(self): return self._lifecycle_svc.pause()
    def resume(self): return self._lifecycle_svc.resume()
    def stop(self): return self._lifecycle_svc.stop()
    def destroy(self): return self._lifecycle_svc.destroy()
    def get_state(self): return self._lifecycle_svc.get_state()
    def is_running(self): return self._lifecycle_svc.is_running()
    def is_paused(self): return self._lifecycle_svc.is_paused()
    def is_trading(self): return self._lifecycle_svc.is_trading()
    def health_check(self): return self._lifecycle_svc.health_check()
    @staticmethod
    def _count_option_contracts(options_dict):
        return sum(len(v) for v in (options_dict or {}).values())
    def get_stats(self): return self._lifecycle_svc.get_stats()
    def enter_parallel_running(self, ss=None, cc=None, dur=3600.0): return self._lifecycle_svc.enter_parallel_running(ss, cc, dur)
    def exit_parallel_running(self, prom=False): return self._lifecycle_svc.exit_parallel_running(prom)
    def record_error(self, msg): return self._lifecycle_svc.record_error(msg)
    @staticmethod
    def _extract_runtime_market_center(so): return LifecycleService._extract_runtime_market_center(so)
    @staticmethod
    def _fetch_china_holidays(y=None): return LifecycleService._fetch_china_holidays(y)
    @staticmethod
    def _should_probe_t_type_future(p): return LifecycleService._should_probe_t_type_future(p)
    def _init_logging(self, p=None): self._lifecycle_svc._init_logging(p); self._config_layer.init_logging(p)
    def _init_scheduler(self): self._lifecycle_svc._init_scheduler(); self._config_layer.init_scheduler()
    def _stop_scheduler(self): self._lifecycle_svc._stop_scheduler(); self._config_layer.stop_scheduler()
    _LIFECYCLE_DELEGATE = frozenset({'_start_platform_subscribe_async','_platform_subscribe_worker','_do_bind_platform_apis','_inject_runtime_context','_get_fallback_market_center','_init_analytics_services','_build_instrument_groups','_resolve_option_underlying_id','_init_t_type_service_and_preload','_register_analytics_jobs','_start_analytics_warmup_async','_ensure_analytics_ready','_warm_storage_async','_start_historical_kline_load_async','_start_historical_kline_load','_shutdown_historical_services','_unsubscribe_all_instruments','_shutdown_runtime_services','_log_resource_ownership_table','_log_t_type_future_probe','_add_option_status_diagnosis_job','_add_tick_sync_job','_add_14_contracts_diagnosis_job','_add_trading_jobs','_ensure_check_pending_orders_job','_publish_event','compare_parallel_results','get_parallel_running_status','record_parallel_result','get_parallel_results','prepare_restart','record_tick','record_trade','record_signal','get_uptime','_init_lifecycle_submodules','_init_logging','_init_scheduler','_extract_contract_year_month','_reset_historical_state_for_restart','_lifecycle_platform','_lifecycle_state_machine','_lifecycle_resource','_lifecycle_parallel','_lc_transition','_lc_init','_lc_bind','_lc_callbacks','_lc_monitor','_lc_parallel_ops','_cancel_all_timers'})
    def _auto_recovery_flow(self): return self._recovery_svc.auto_recovery_flow()
    def _watchdog_restart(self, mr=3, cs=60): return self._recovery_svc.watchdog_restart(mr, cs)
    def recover_from_checkpoint(self): return self._recovery_svc.recover_from_checkpoint()
    def save_checkpoint(self, reason='manual'): return self._checkpoint_svc.save_checkpoint()
    def _on_system_event_checkpoint(self, ev): return self._checkpoint_svc.on_system_event_checkpoint(ev)
    def _sync_state_to_store(self): return self._checkpoint_svc.sync_state_to_store()
    def _ensure_order_service(self): return self._business_layer.ensure_order_service()
    def _ensure_position_service(self): return self._business_layer.ensure_position_service()
    def _ensure_hft_engine(self): return self._business_layer.ensure_hft_engine()
    def _ensure_snapshot_collector(self, s=""): return self._business_layer.ensure_snapshot_collector(symbol=s)
    def get_snapshot_collector(self): return self._business_layer.get_snapshot_collector()
    def on_order(self, od, *a, **kw): return self._business_layer.on_order(od, *a, **kw)
    def on_trade(self, td, *a, **kw): return self._business_layer.on_trade(td, *a, **kw)
    def _update_position_from_trade(self, td): return self._business_layer.update_position_from_trade(td)
    def execute_option_trading_cycle(self): return self._business_layer.execute_option_trading_cycle()
    def _check_ecosystem_exclusion(self, t): return self._business_layer._check_ecosystem_exclusion(t)
    def _feed_shadow_engine(self, t, r): return self._business_layer._feed_shadow_engine(t, r)
    def _resolve_open_reason(self): return self._config_layer.resolve_open_reason()
    def check_position_risk(self): return self._monitoring_layer.check_position_risk()
    def get_health_status(self): return self._monitoring_layer.get_health_status()
    def confirm_daily_resume(self, ci="unknown"): return self._monitoring_layer.confirm_daily_resume(caller_id=ci)
    def is_hard_stop_triggered(self): return self._monitoring_layer.is_hard_stop_triggered()
    def emergency_stop(self, ci="unknown", r=""): return self._monitoring_layer.emergency_stop(caller_id=ci, reason=r)
    def on_tick(self, t): return self._tick_svc.on_tick(t)
    def on_market_event(self, e): return self._tick_svc.on_market_event(e)
    @property
    def _tick_service(self): return self._tick_svc
    def _init_historical_kline_mixin(self): self._kline_svc.init_historical(); self._historical_mgr = self._kline_svc.historical_mgr
    def _init_tick_handler_mixin(self): self._tick_svc.init_tick_handler()
    def _get_checkpoint_service(self): return self._checkpoint_svc
    def run_regression_check(self, tc=None, tf=None): return self._monitoring_layer.run_regression_check(test_cases=tc, test_func=tf)

# Merged from strategy_core_service_builder.py on 2026-06-12
class StrategyCoreServiceBuilder:
    """渐进式构建StrategyCoreService

    原StrategyCoreService.__init__ 339行(圈复杂度>40)拆分为:
    - build_state_store: 状态存储初始化
    - build_managers: 管理器初始化(合约/持仓/风控/诊断)
    - build_thread_pools: 线程池初始化
    - build_platform_apis: 平台API绑定
    - build_monitoring: 监控/健康检查初始化
    - apply_to: 将构建结果注入到已有StrategyCoreService实例(非递归)
    """

    def __init__(self):
        self._built_state_store: Any = None
        self._built_managers: Dict[str, Any] = {}
        self._built_thread_pools: Dict[str, Any] = {}
        self._built_platform_apis: Dict[str, Any] = {}
        self._built_monitoring: Dict[str, Any] = {}
        self._config: Dict[str, Any] = {}
        self._event_bus: Any = None

    def set_config(self, config: Dict[str, Any]) -> 'StrategyCoreServiceBuilder':
        self._config = config
        return self

    def set_event_bus(self, event_bus: Any) -> 'StrategyCoreServiceBuilder':
        self._event_bus = event_bus
        return self

    def build_state_store(self) -> 'StrategyCoreServiceBuilder':
        try:
            from infra.service_contracts import StateStore
            self._built_state_store = StateStore()
        except ImportError:
            self._built_state_store = None
            logging.debug("[Builder] StateStore不可用,使用dict替代")
        return self

    def build_managers(self) -> 'StrategyCoreServiceBuilder':
        self._built_managers = {}
        try:
            from strategy.instrument_service import InstrumentManager
            self._built_managers['instrument'] = InstrumentManager()
        except ImportError:
            pass
        try:
            from data.historical_data_manager import HistoricalDataManager
            self._built_managers['historical'] = HistoricalDataManager()
        except ImportError:
            pass
        try:
            from infra.health_monitor import DiagnosisService
            self._built_managers['diagnosis'] = DiagnosisService()
        except ImportError:
            pass
        return self

    def build_thread_pools(self) -> 'StrategyCoreServiceBuilder':
        from concurrent.futures import ThreadPoolExecutor
        self._built_thread_pools = {
            'subscription': ThreadPoolExecutor(max_workers=4, thread_name_prefix='sub'),
            'diagnosis': ThreadPoolExecutor(max_workers=2, thread_name_prefix='diag'),
            'heartbeat': ThreadPoolExecutor(max_workers=1, thread_name_prefix='hb'),
        }
        return self

    def build_platform_apis(self) -> 'StrategyCoreServiceBuilder':
        self._built_platform_apis = {
            'insert_order': None,
            'cancel_order': None,
            'query_order': None,
        }
        return self

    def build_monitoring(self) -> 'StrategyCoreServiceBuilder':
        self._built_monitoring = {
            'health_check_interval': self._config.get('health_check_interval', 30.0),
            'performance_monitor': None,
            'health_aggregator': None,
        }
        try:
            from infra.metrics_registry import PerformanceMonitor
            self._built_monitoring['performance_monitor'] = PerformanceMonitor()
        except ImportError:
            pass
        return self

    def apply_to(self, instance: Any) -> Any:
        """将构建结果注入到已有StrategyCoreService实例(非递归)"""
        if self._built_state_store is not None:
            instance._state_store = self._built_state_store
        if self._built_managers:
            instance._managers = self._built_managers
        if self._built_thread_pools:
            instance._thread_pools = self._built_thread_pools
        if self._built_platform_apis:
            instance._platform_apis = self._built_platform_apis
        if self._built_monitoring:
            instance._monitoring = self._built_monitoring
        return instance

    def build(self) -> Any:
        """创建新StrategyCoreService实例并注入构建结果(非递归: event_bus直传)"""
        instance = StrategyCoreService.__new__(StrategyCoreService)
        instance.strategy_id = f"strategy_{int(time.time())}"
        instance._event_bus = self._event_bus
        self.apply_to(instance)
        return instance

# 打破循环依赖: StrategyParams 从独立模块导入，不再从 strategy_2026 导入
from strategy.instrument_service import StrategyParams
# Re-export Strategy2026 for backward compatibility
from strategy.strategy_2026 import Strategy2026
