"""strategy_lifecycle_mixin.py — StrategyState + _LifecycleMixin (Facade)
方法体迁移至子模块: lifecycle_init/bind/callbacks/monitor/parallel_ops/transition"""
import logging
from typing import Any,Callable,Dict,List,Optional,Tuple
from ali2026v3_trading.lifecycle_state_machine import LifecycleStateMachine;from ali2026v3_trading.lifecycle_platform import LifecyclePlatform;from ali2026v3_trading.lifecycle_resource import LifecycleResource;from ali2026v3_trading.lifecycle_parallel import LifecycleParallel;from ali2026v3_trading.lifecycle_transition import LifecycleTransition;from ali2026v3_trading.lifecycle_init import LifecycleInit;from ali2026v3_trading.lifecycle_bind import LifecycleBind;from ali2026v3_trading.lifecycle_callbacks import LifecycleCallbacks;from ali2026v3_trading.lifecycle_monitor import LifecycleMonitor;from ali2026v3_trading.lifecycle_parallel_ops import LifecycleParallelOps
from ali2026v3_trading.lifecycle_state import StrategyState,_state_key,_state_is,VALID_STATE_TRANSITIONS
__all__=['StrategyState','_state_key','_state_is','VALID_STATE_TRANSITIONS','LifecycleStateMachine','LifecyclePlatform','LifecycleResource','LifecycleParallel']
try:
    from ali2026v3_trading import get_instrument_data_manager
except ImportError:
    get_instrument_data_manager=None
def _lazy(cls,np=True):
    @property
    def p(self):
        a=f'_lazy_{cls.__name__}'
        if not hasattr(self,a)or getattr(self,a)is None:object.__setattr__(self,a,cls(provider=self)if np else cls())
        return getattr(self,a)
    return p
class _LifecycleMixin:
    OPTION_TO_FUTURE_MAP={'MO':'IM','IO':'IF','HO':'IH'};VALID_STATE_TRANSITIONS=VALID_STATE_TRANSITIONS
    _STRATEGY_STATE_TO_LSM_MAP={StrategyState.INITIALIZING:"INITIALIZED",StrategyState.RUNNING:"RUNNING",StrategyState.PARALLEL_RUNNING:"RUNNING",StrategyState.PAUSED:"PAUSED",StrategyState.STOPPED:"STOPPED",StrategyState.DEGRADED:"PAUSED",StrategyState.DEGRADED_STOP:"STOPPED",StrategyState.ERROR:"STOPPED"}
    _lifecycle_state_machine=_lazy(LifecycleStateMachine,False);_lifecycle_platform=_lazy(LifecyclePlatform);_lifecycle_resource=_lazy(LifecycleResource);_lifecycle_parallel=_lazy(LifecycleParallel);_lc_transition=_lazy(LifecycleTransition);_lc_init=_lazy(LifecycleInit);_lc_bind=_lazy(LifecycleBind);_lc_callbacks=_lazy(LifecycleCallbacks);_lc_monitor=_lazy(LifecycleMonitor);_lc_parallel_ops=_lazy(LifecycleParallelOps)
    @property
    def storage(self):
        if self._storage is None:
            with self._storage_lock:
                if self._storage is None:
                    try:
                        if get_instrument_data_manager is None:raise ImportError("not available")
                        self._storage=get_instrument_data_manager()
                    except Exception as e:logging.warning("[Storage] Init failed: %s",e);return None
        return self._storage
    def _init_lifecycle_submodules(self):
        for a in['_lifecycle_state_machine','_lifecycle_platform','_lifecycle_resource','_lifecycle_parallel','_lc_transition','_lc_init','_lc_bind','_lc_callbacks','_lc_monitor','_lc_parallel_ops']:getattr(self,a)
        logging.info("[LifecycleSubmodules] SM=%s Plat=%s Res=%s Par=%s",self._lazy_LifecycleStateMachine.state,len(self._lazy_LifecyclePlatform.subscribed_instruments),len(self._lazy_LifecycleResource.owned_resource_names),self._lazy_LifecycleParallel.is_parallel_running)
    def transition_to(self,new_state):return self._lc_transition.transition_to(new_state)
    def bind_platform_apis(self,s):return self._lc_bind.bind_platform_apis(s)
    def _do_bind_platform_apis(self,s):return self._lc_bind._do_bind_platform_apis(s)
    @staticmethod
    def _extract_runtime_market_center(s):return LifecycleBind._extract_runtime_market_center(s)
    def _inject_runtime_context(self,s):return self._lc_bind._inject_runtime_context(s)
    def _get_fallback_market_center(self):return self._lc_bind._get_fallback_market_center()
    def _start_platform_subscribe_async(self,i):return self._lc_bind._start_platform_subscribe_async(i)
    def _platform_subscribe_worker(self,i):return self._lc_bind._platform_subscribe_worker(i)
    def _unsubscribe_all_instruments(self):return self._lc_bind._unsubscribe_all_instruments()
    def _start_historical_kline_load_async(self):return self._lc_bind._start_historical_kline_load_async()
    def on_init(self,*a,**kw):return self._lc_init.on_init(*a,**kw)
    def initialize(self,params=None):return self._lc_init.initialize(params)
    def prepare_restart(self):return self._lc_init.prepare_restart()
    def _init_analytics_services(self,p):return self._lc_init._init_analytics_services(p)
    def _build_instrument_groups(self,s):return self._lc_init._build_instrument_groups(s)
    def _resolve_option_underlying_id(self,i,s):return self._lc_init._resolve_option_underlying_id(i,s)
    def _init_t_type_service_and_preload(self,s):return self._lc_init._init_t_type_service_and_preload(s)
    def _register_analytics_jobs(self):return self._lc_init._register_analytics_jobs()
    def _start_analytics_warmup_async(self,p):return self._lc_init._start_analytics_warmup_async(p)
    def _ensure_analytics_ready(self,timeout=30.0):return self._lc_init._ensure_analytics_ready(timeout)
    def _warm_storage_async(self):return self._lc_init._warm_storage_async()
    @staticmethod
    def _fetch_china_holidays(year=None):return LifecycleInit._fetch_china_holidays(year)
    def _init_logging(self,p):return self._lc_init._init_logging(p)
    def _init_scheduler(self):return self._lc_init._init_scheduler()
    def _stop_scheduler(self):return self._lc_init._stop_scheduler()
    def _add_option_status_diagnosis_job(self):return self._lc_init._add_option_status_diagnosis_job()
    def _add_tick_sync_job(self):return self._lc_init._add_tick_sync_job()
    def _add_14_contracts_diagnosis_job(self):return self._lc_init._add_14_contracts_diagnosis_job()
    def _add_trading_jobs(self):return self._lc_init._add_trading_jobs()
    def _ensure_check_pending_orders_job(self):return self._lc_init._ensure_check_pending_orders_job()
    def on_start(self):return self._lc_callbacks.on_start()
    def on_stop(self):return self._lc_callbacks.on_stop()
    def on_destroy(self):return self._lc_callbacks.on_destroy()
    def start(self):return self._lc_callbacks.start()
    def stop(self):return self._lc_callbacks.stop()
    def pause(self):return self._lc_callbacks.pause()
    def resume(self):return self._lc_callbacks.resume()
    def destroy(self):return self._lc_callbacks.destroy()
    def save_state(self):return self._lc_callbacks.save_state()
    def _shutdown_runtime_services(self):return self._lc_callbacks._shutdown_runtime_services()
    def _log_resource_ownership_table(self,phase='unknown'):return self._lc_callbacks._log_resource_ownership_table(phase)
    def _publish_event(self,e,d):return self._lc_callbacks._publish_event(e,d)
    def get_state(self):return self._lc_monitor.get_state()
    def is_running(self):return self._lc_monitor.is_running()
    def is_paused(self):return self._lc_monitor.is_paused()
    def is_trading(self):return self._lc_monitor.is_trading()
    def get_uptime(self):return self._lc_monitor.get_uptime()
    def record_tick(self):return self._lc_monitor.record_tick()
    def record_trade(self):return self._lc_monitor.record_trade()
    def record_signal(self):return self._lc_monitor.record_signal()
    def record_error(self,m):return self._lc_monitor.record_error(m)
    def get_stats(self):return self._lc_monitor.get_stats()
    def health_check(self):return self._lc_monitor.health_check()
    @staticmethod
    def _should_probe_t_type_future(p):return LifecycleMonitor._should_probe_t_type_future(p)
    def _log_t_type_future_probe(self,ph,i,pr,m,lp):return self._lc_monitor._log_t_type_future_probe(ph,i,pr,m,lp)
    def enter_parallel_running(self,shadow=None,cb=None,dur=3600.0):return self._lc_parallel_ops.enter_parallel_running(shadow,cb,dur)
    def exit_parallel_running(self,promote=False):return self._lc_parallel_ops.exit_parallel_running(promote)
    def compare_parallel_results(self,o,n):return self._lc_parallel_ops.compare_parallel_results(o,n)
    def record_parallel_result(self,k,r):return self._lc_parallel_ops.record_parallel_result(k,r)
    def get_parallel_results(self):return self._lc_parallel_ops.get_parallel_results()
    def get_parallel_running_status(self):return self._lc_parallel_ops.get_parallel_running_status()
