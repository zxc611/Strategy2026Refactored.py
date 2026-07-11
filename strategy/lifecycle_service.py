"""lifecycle_service.py — LifecycleService 独立生命周期服务
替代 _LifecycleMixin，以组合替代继承 (G2b Step 4)

设计原则:
- LifecycleService 是独立服务对象，不依赖 Mixin 继承链
- 通过 self._provider 访问策略实例的共享状态 (与 StrategyBusinessLayer 等一致)
- 子模块延迟初始化模式与 _LifecycleMixin 保持一致
- 所有公开方法签名与 _LifecycleMixin 完全兼容，确保平滑迁移
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from ali2026v3_trading.lifecycle.lifecycle_state_machine import LifecycleStateMachine
from ali2026v3_trading.lifecycle.lifecycle_platform import LifecyclePlatform
from ali2026v3_trading.lifecycle.lifecycle_resource import LifecycleResource
from ali2026v3_trading.lifecycle.lifecycle_parallel import LifecycleParallel
from ali2026v3_trading.lifecycle.lifecycle_transition import LifecycleTransition
from ali2026v3_trading.lifecycle.lifecycle_init import LifecycleInit
from ali2026v3_trading.infra.commission_utils import OPTION_TO_FUTURE_MAP  # 五唯一性修复
from ali2026v3_trading.lifecycle.lifecycle_bind import LifecycleBind
from ali2026v3_trading.lifecycle.lifecycle_callbacks import LifecycleCallbacks
from ali2026v3_trading.lifecycle.lifecycle_monitor import LifecycleMonitor
from ali2026v3_trading.lifecycle.lifecycle_parallel import LifecycleParallelOps
from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState, _state_key, _state_is, VALID_STATE_TRANSITIONS

try:
    from ali2026v3_trading.data.data_service import get_data_service as _get_data_service
except ImportError:
    _get_data_service = None

# Backward-compatible module symbol used by legacy callers and tests.
get_instrument_data_manager = _get_data_service


class LifecycleService:
    """独立生命周期服务 — 替代 _LifecycleMixin，以组合替代继承

    .. deprecated::
        直接使用 LifecycleService；作为 _LifecycleMixin 的 Shim 导入时会触发 DeprecationWarning。
    """

    def __init_subclass__(cls, **kwargs):
        import warnings
        warnings.warn(
            "_LifecycleMixin is deprecated; use LifecycleService instead",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init_subclass__(**kwargs)

    OPTION_TO_FUTURE_MAP = OPTION_TO_FUTURE_MAP  # 五唯一性修复：从 shared_trading_constants 导入
    VALID_STATE_TRANSITIONS = VALID_STATE_TRANSITIONS

    _STRATEGY_STATE_TO_LSM_MAP = {
        StrategyState.INITIALIZING: "INITIALIZED",
        StrategyState.RUNNING: "RUNNING",
        StrategyState.PARALLEL_RUNNING: "RUNNING",
        StrategyState.PAUSED: "PAUSED",
        StrategyState.STOPPED: "STOPPED",
        StrategyState.DEGRADED: "PAUSED",
        StrategyState.DEGRADED_STOP: "STOPPED",
        StrategyState.ERROR: "STOPPED",
    }

    def __init__(self, provider, strategy_id: str = ''):
        """
        Args:
            provider: 策略实例引用（用于访问 _state, _lock, _storage 等共享状态）
                      后续可替换为显式依赖注入
            strategy_id: 策略ID
        """
        self._provider = provider
        self._strategy_id = strategy_id
        # Lazy sub-module instances (same pattern as _LifecycleMixin)
        self._lazy_LifecycleStateMachine = None
        self._lazy_LifecyclePlatform = None
        self._lazy_LifecycleResource = None
        self._lazy_LifecycleParallel = None
        self._lazy_LifecycleTransition = None
        self._lazy_LifecycleInit = None
        self._lazy_LifecycleBind = None
        self._lazy_LifecycleCallbacks = None
        self._lazy_LifecycleMonitor = None
        self._lazy_LifecycleParallelOps = None

    # ========== Lazy sub-module properties ==========

    @property
    def _lifecycle_state_machine(self) -> LifecycleStateMachine:
        if self._lazy_LifecycleStateMachine is None:
            self._lazy_LifecycleStateMachine = LifecycleStateMachine()
        return self._lazy_LifecycleStateMachine

    @property
    def _lifecycle_platform(self) -> LifecyclePlatform:
        if self._lazy_LifecyclePlatform is None:
            self._lazy_LifecyclePlatform = LifecyclePlatform(provider=self._provider)
        return self._lazy_LifecyclePlatform

    @property
    def _lifecycle_resource(self) -> LifecycleResource:
        if self._lazy_LifecycleResource is None:
            self._lazy_LifecycleResource = LifecycleResource(provider=self._provider)
        return self._lazy_LifecycleResource

    @property
    def _lifecycle_parallel(self) -> LifecycleParallel:
        if self._lazy_LifecycleParallel is None:
            self._lazy_LifecycleParallel = LifecycleParallel(provider=self._provider)
        return self._lazy_LifecycleParallel

    @property
    def _lc_transition(self) -> LifecycleTransition:
        if self._lazy_LifecycleTransition is None:
            event_bus = getattr(self._provider, '_event_bus_param', None)
            self._lazy_LifecycleTransition = LifecycleTransition(event_bus=event_bus)
        return self._lazy_LifecycleTransition

    @property
    def _lc_init(self) -> LifecycleInit:
        if self._lazy_LifecycleInit is None:
            self._lazy_LifecycleInit = LifecycleInit(provider=self._provider)
        return self._lazy_LifecycleInit

    @property
    def _lc_bind(self) -> LifecycleBind:
        if self._lazy_LifecycleBind is None:
            self._lazy_LifecycleBind = LifecycleBind(provider=self._provider)
        return self._lazy_LifecycleBind

    @property
    def _lc_callbacks(self) -> LifecycleCallbacks:
        if self._lazy_LifecycleCallbacks is None:
            self._lazy_LifecycleCallbacks = LifecycleCallbacks(provider=self._provider)
        return self._lazy_LifecycleCallbacks

    @property
    def _lc_monitor(self) -> LifecycleMonitor:
        if self._lazy_LifecycleMonitor is None:
            self._lazy_LifecycleMonitor = LifecycleMonitor(provider=self._provider)
        return self._lazy_LifecycleMonitor

    @property
    def _lc_parallel_ops(self) -> LifecycleParallelOps:
        if self._lazy_LifecycleParallelOps is None:
            self._lazy_LifecycleParallelOps = LifecycleParallelOps(provider=self._provider)
        return self._lazy_LifecycleParallelOps

    # ========== Storage (lazy init) ==========

    @property
    def storage(self):
        p = self._provider
        if p._storage is None:
            with p._storage_lock:
                if p._storage is None:
                    try:
                        if get_instrument_data_manager is not None:
                            p._storage = get_instrument_data_manager()
                        else:
                            return None
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                        logging.warning("[Storage] Init failed: %s", e)
                        return None
        return p._storage

    def _extract_contract_year_month(self, instrument_id):
        try:
            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
            parsed = SubscriptionManager.parse_future(instrument_id)
            return parsed.get('year_month', '')
        except Exception:
            return ''

    def _reset_historical_state_for_restart(self):
        p = self._provider
        _hm = getattr(p, '_historical_mgr', None)
        if _hm is not None and hasattr(_hm, 'reset_for_restart'):
            _hm.reset_for_restart()

    # ========== Sub-module initialization ==========

    def _init_lifecycle_submodules(self):
        """强制初始化所有子模块"""
        for attr in ('_lifecycle_state_machine', '_lifecycle_platform', '_lifecycle_resource',
                      '_lifecycle_parallel', '_lc_transition', '_lc_init', '_lc_bind',
                      '_lc_callbacks', '_lc_monitor', '_lc_parallel_ops'):
            getattr(self, attr)
        logging.info(
            "[LifecycleSubmodules] SM=%s Plat=%s Res=%s Par=%s",
            self._lazy_LifecycleStateMachine.state,
            len(self._lazy_LifecyclePlatform.subscribed_instruments),
            len(self._lazy_LifecycleResource.owned_resource_names),
            self._lazy_LifecycleParallel.is_parallel_running,
        )

    # ========== Transition ==========

    def transition_to(self, new_state):
        p = self._provider
        current_state = getattr(p, '_state', None)
        # FIX-20260711-PAUSE-GUARD: 当_is_paused=True时，仅允许resume()路径转换到RUNNING
        # 防止异步线程(subscribe-retry/exit_parallel_running等)覆盖用户暂停指令
        _new_key = _state_key(new_state)
        if getattr(p, '_is_paused', False) and _new_key in ('running', 'parallel_running'):
            if not getattr(p, '_resume_in_progress', False):
                logging.warning(
                    "[LifecycleService.transition_to] PAUSE-GUARD: 拒绝 %s→%s (策略已暂停，非resume路径)",
                    current_state, new_state,
                )
                return False
        try:
            result = self._lc_transition.transition_to(current_state, new_state)
        except TypeError:
            result = self._lc_transition.transition_to(new_state)
        success = result[0] if isinstance(result, tuple) else bool(result)
        if success:
            p._state = new_state
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.state = new_state
            # FIX-20260709-PAUSE-ROOT-V2: 四元状态原子同步
            # 根因: transition_to()只更新_state和_is_running，不更新_is_paused/_is_trading
            # 导致任何通过transition_to()转换状态的代码路径(如DEGRADED→RUNNING恢复)都可能出现四元状态不一致
            # 修复: 根据目标状态自动同步四元变量，作为PAUSE-SAFE规范的安全兜底
            _new_key = _state_key(new_state)
            if _new_key == 'running' or _new_key == 'parallel_running':
                _new_running, _new_paused, _new_trading = True, False, True
            elif _new_key == 'paused':
                _new_running, _new_paused, _new_trading = False, True, False
            elif _new_key in ('stopped', 'degraded_stop'):
                _new_running, _new_paused, _new_trading = False, True, False
            elif _new_key == 'degraded':
                _new_running, _new_paused, _new_trading = True, False, True
            elif _new_key == 'error':
                _new_running, _new_paused, _new_trading = False, False, False
            elif _new_key == 'initializing':
                _new_running, _new_paused, _new_trading = False, False, True
            else:
                _new_running, _new_paused, _new_trading = None, None, None
            if _new_running is not None:
                p._is_running = _new_running
                p._is_paused = _new_paused
                p._is_trading = _new_trading
                if _lm is not None:
                    _lm.is_running = _new_running
                    _lm.is_paused = _new_paused
            # FIX: 同步 _state 到 _state_store，否则状态报告(tick_processing_service.output_periodic_summary)
            # 从 _state_store 读取 _state 时恒显示 initializing，导致"长期处于初始化状态"假象
            _ss = getattr(p, '_state_store', None)
            if _ss is not None:
                try:
                    _ss.set('_state', new_state)
                    if _new_running is not None:
                        _ss.set('_is_running', _new_running)
                        _ss.set('_is_paused', _new_paused)
                        _ss.set('_is_trading', _new_trading)
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass
            logging.info(
                "[LifecycleService.transition_to] %s -> %s (4-state synced: running=%s paused=%s trading=%s)",
                current_state, new_state,
                _new_running, _new_paused, _new_trading,
            )
        return result

    # ========== Bind ==========

    def bind_platform_apis(self, strategy_obj):
        return self._lc_bind.bind_platform_apis(strategy_obj)

    def _do_bind_platform_apis(self, strategy_obj):
        return self._lc_bind._do_bind_platform_apis(strategy_obj)

    @staticmethod
    def _extract_runtime_market_center(strategy_obj):
        return LifecycleBind._extract_runtime_market_center(strategy_obj)

    def _inject_runtime_context(self, strategy_obj):
        return self._lc_bind._inject_runtime_context(strategy_obj)

    def _get_fallback_market_center(self):
        return self._lc_bind._get_fallback_market_center()

    def _start_platform_subscribe_async(self, instruments):
        return self._lc_bind._start_platform_subscribe_async(instruments)

    def _platform_subscribe_worker(self, instruments):
        return self._lc_bind._platform_subscribe_worker(instruments)

    def _unsubscribe_all_instruments(self):
        return self._lc_bind._unsubscribe_all_instruments()

    def _start_historical_kline_load_async(self):
        return self._lc_bind._start_historical_kline_load_async()

    def _start_historical_kline_load(self, blocking: bool = False):
        return self._lc_bind._start_historical_kline_load(blocking=blocking)

    def _shutdown_historical_services(self):
        return self._lc_bind._shutdown_historical_services()

    # ========== Init ==========

    def on_init(self, *a, **kw):
        return self._lc_init.on_init(*a, **kw)

    def initialize(self, params=None):
        return self._lc_init.initialize(params)

    def prepare_restart(self):
        return self._lc_init.prepare_restart()

    def _init_analytics_services(self, params):
        return self._lc_init._init_analytics_services(params)

    def _build_instrument_groups(self, strategy_obj):
        return self._lc_init._build_instrument_groups(strategy_obj)

    def _resolve_option_underlying_id(self, instrument_id, strategy_obj):
        return self._lc_init._resolve_option_underlying_id(instrument_id, strategy_obj)

    def _init_t_type_service_and_preload(self, strategy_obj):
        return self._lc_init._init_t_type_service_and_preload(strategy_obj)

    def _register_analytics_jobs(self):
        return self._lc_init._register_analytics_jobs()

    def _start_analytics_warmup_async(self, params):
        return self._lc_init._start_analytics_warmup_async(params)

    def _ensure_analytics_ready(self, timeout=30.0):
        return self._lc_init._ensure_analytics_ready(timeout)

    def _warm_storage_async(self):
        return self._lc_init._warm_storage_async()

    @staticmethod
    def _fetch_china_holidays(year=None):
        return LifecycleInit._fetch_china_holidays(year)

    def _init_logging(self, params):
        return self._lc_init._init_logging(params)

    def _init_scheduler(self):
        return self._lc_init._init_scheduler()

    def _stop_scheduler(self):
        return self._lc_init._stop_scheduler()

    def _add_option_status_diagnosis_job(self):
        return self._lc_init._add_option_status_diagnosis_job()

    def _add_tick_sync_job(self):
        return self._lc_init._add_tick_sync_job()

    def _add_14_contracts_diagnosis_job(self):
        return self._lc_init._add_14_contracts_diagnosis_job()

    def _add_trading_jobs(self):
        return self._lc_init._add_trading_jobs()

    def _ensure_check_pending_orders_job(self):
        return self._lc_init._ensure_check_pending_orders_job()

    # ========== Callbacks ==========

    def on_start(self):
        return self._lc_callbacks.on_start()

    def on_stop(self):
        return self._lc_callbacks.on_stop()

    def on_destroy(self):
        return self._lc_callbacks.on_destroy()

    def start(self):
        return self._lc_callbacks.start()

    def stop(self):
        return self._lc_callbacks.stop()

    def pause(self):
        return self._lc_callbacks.pause()

    def resume(self):
        return self._lc_callbacks.resume()

    def destroy(self):
        return self._lc_callbacks.destroy()

    def save_state(self):
        return self._lc_callbacks.save_state()

    def _shutdown_runtime_services(self):
        return self._lc_callbacks._shutdown_runtime_services()

    def _cancel_all_timers(self):
        p = self._provider
        _warm_timer = getattr(p, '_storage_warm_timer', None)
        if _warm_timer is not None:
            _warm_timer.cancel()
            try:
                p._storage_warm_timer = None
            except (AttributeError,):
                pass
        _warm_thread = getattr(p, '_storage_warm_thread', None)
        if _warm_thread is not None and _warm_thread.is_alive():
            _warm_thread.join(timeout=5.0)
            try:
                p._storage_warm_thread = None
            except (AttributeError,):
                pass
        _sub_thread = getattr(p, '_platform_subscribe_thread', None)
        if _sub_thread is not None and _sub_thread.is_alive():
            _stop_event = getattr(p, '_platform_subscribe_stop', None)
            if _stop_event is not None:
                _stop_event.set()
            _sub_thread.join(timeout=5.0)
            try:
                p._platform_subscribe_thread = None
            except (AttributeError,):
                pass

    def _log_resource_ownership_table(self, phase='unknown'):
        return self._lc_callbacks._log_resource_ownership_table(phase)

    def _publish_event(self, event, data):
        return self._lc_callbacks._publish_event(event, data)

    # ========== Monitor ==========

    def get_state(self):
        return self._lc_monitor.get_state()

    def is_running(self):
        return self._lc_monitor.is_running()

    def is_paused(self):
        return self._lc_monitor.is_paused()

    def is_trading(self):
        return self._lc_monitor.is_trading()

    def get_uptime(self):
        return self._lc_monitor.get_uptime()

    def record_tick(self):
        return self._lc_monitor.record_tick()

    def record_trade(self):
        return self._lc_monitor.record_trade()

    def record_signal(self):
        return self._lc_monitor.record_signal()

    def record_error(self, msg):
        return self._lc_monitor.record_error(msg)

    def get_stats(self):
        return self._lc_monitor.get_stats()

    def health_check(self):
        return self._lc_monitor.health_check()

    @staticmethod
    def _should_probe_t_type_future(product):
        return LifecycleMonitor._should_probe_t_type_future(product)

    def _log_t_type_future_probe(self, phase, instrument_id, product, market_center, log_prefix):
        return self._lc_monitor._log_t_type_future_probe(phase, instrument_id, product, market_center, log_prefix)

    # ========== Parallel Ops ==========

    def enter_parallel_running(self, shadow=None, cb=None, dur=3600.0):
        return self._lc_parallel_ops.enter_parallel_running(shadow, cb, dur)

    def exit_parallel_running(self, promote=False):
        return self._lc_parallel_ops.exit_parallel_running(promote)

    def compare_parallel_results(self, old_result, new_result):
        return self._lc_parallel_ops.compare_parallel_results(old_result, new_result)

    def record_parallel_result(self, key, result):
        return self._lc_parallel_ops.record_parallel_result(key, result)

    def get_parallel_results(self):
        return self._lc_parallel_ops.get_parallel_results()

    def get_parallel_running_status(self):
        return self._lc_parallel_ops.get_parallel_running_status()


# Backward-compatible export used by ecosystem bootstrap and legacy tests.
StrategyLifecycleService = LifecycleService

__all__ = ["LifecycleService", "StrategyLifecycleService", "get_instrument_data_manager"]
