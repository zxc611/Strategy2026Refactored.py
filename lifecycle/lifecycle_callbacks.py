"""lifecycle_callbacks.py — 生命周期回调逻辑（从strategy_lifecycle_mixin.py拆分）
职责: on_start, on_stop, on_destroy, start/stop/pause/resume/destroy, save_state, _shutdown_runtime_services,
      _log_resource_ownership_table, _publish_event
"""
from __future__ import annotations

import time
import threading
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.params_service import get_param_value
from ali2026v3_trading.lifecycle.lifecycle_state import StrategyState, _state_is


class LifecycleCallbacks:
    def __init__(self, provider):
        self.p = provider

    def on_start(self) -> bool:
        p = self.p
        if not p._initialized:
            logging.error("[StrategyCoreService.on_start] R24-P1-CF-02: 初始化未完成(_initialized=False)，拒绝启动")
            return False
        root_logger = logging.getLogger()
        logging.info(
            f"[StrategyCoreService.on_start] 日志配置诊断: "
            f"Level={logging.getLevelName(root_logger.level)}, Handlers={len(root_logger.handlers)}"
        )
        logging.info("[StrategyCoreService.on_start] ========== START ==========")
        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            if hasattr(p, 't_type_service') and p.t_type_service:
                wc = getattr(p.t_type_service, '_width_cache', None)
                if wc:
                    spm.bind_width_cache(wc)
            p._state_param_manager = spm
            logging.info("[StrategyCoreService.on_start] StateParamManager initialized, state=%s", spm.get_current_state())
            try:
                from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
                eco = get_strategy_ecosystem()
                spm.register_on_state_switch(eco.on_state_switched)
                logging.info("[StrategyCoreService.on_start] SPM-Ecosystem联动已绑定")
            except Exception as eco_e:
                logging.warning("[StrategyCoreService.on_start] Ecosystem联动绑定失败: %s", eco_e)
        except Exception as spm_e:
            logging.warning("[StrategyCoreService.on_start] StateParamManager init failed: %s", spm_e)
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            _sid = str(getattr(p, 'strategy_id', '') or 'global')
            p._safety_meta_layer = get_safety_meta_layer(params=ps, strategy_id=_sid)
            logging.info("[StrategyCoreService.on_start] SafetyMetaLayer initialized")
        except Exception as safety_e:
            logging.warning("[StrategyCoreService.on_start] SafetyMetaLayer init failed: %s", safety_e)
        try:
            params = getattr(p, 'params', None) or {}
            if params.get('debug_mode', False):
                logging.getLogger().setLevel(logging.DEBUG)
                logging.info("[P2-R8-06] debug_mode激活: 日志级别设为DEBUG")
                if p._safety_meta_layer:
                    p._safety_meta_layer._circuit_breaker_calm_until = float('inf')
                    logging.info("[P2-R8-06] debug_mode: 断路器冷却期设为无限")
            if params.get('stress_test_mode', False):
                logging.critical("[P2-R8-06] stress_test_mode激活: 启用极端场景测试配置")
                if p._safety_meta_layer:
                    p._safety_meta_layer.DEFAULT_MAX_DRAWDOWN = 0.02
                    _stress_sigma = float(getattr(p, '_params', {}).get("stress_test_anomaly_threshold", 1.5)) if hasattr(p, '_params') and p._params else 1.5
                    p._safety_meta_layer.ANOMALY_THRESHOLD_MULTIPLIER = _stress_sigma
                    logging.info("[P2-R8-06] stress_test_mode: 回撤阈值2%%/断路器%.1f sigma", _stress_sigma)
        except Exception as mode_e:
            logging.debug("[P2-R8-06] debug/stress mode处理异常: %s", mode_e)
        try:
            from ali2026v3_trading.data.data_service import get_data_service
            ds = get_data_service()
            logging.info(f"[StrategyCoreService.on_start] DataService预热完成: {ds is not None}")
        except Exception as ds_e:
            logging.warning(f"[StrategyCoreService.on_start] DataService预热失败: {ds_e}")
        with p._lock:
            if p._state not in (StrategyState.INITIALIZING, StrategyState.RUNNING, StrategyState.PAUSED, StrategyState.DEGRADED):
                logging.warning(f"[StrategyCoreService.on_start] Cannot start in state: {p._state}")
                return False
            if p._is_paused:
                p._is_paused = False
                _lm = getattr(p, '_lifecycle_mgr', None)
                if _lm is not None:
                    _lm.is_paused = False
            p._is_running = True
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.is_running = True
            p.transition_to(StrategyState.RUNNING)
            logging.info(f"[StrategyCoreService.on_start] Started: {p.strategy_id}")
            params = None
            if hasattr(p, '_runtime_strategy_host') and p._runtime_strategy_host:
                params = getattr(p._runtime_strategy_host, 'params', None)
            if params is None:
                logging.warning("[Subscribe] 无法获取 params 对象，跳过订阅")
                return True
            selected_futures_list = p._init_instruments_result['futures_list']
            selected_options_dict = p._init_instruments_result['options_dict']
            p._subscribed_instruments = p._init_instruments_result['subscribed_instruments']
            logging.info(
                f"[Subscribe] 使用on_init结果: "
                f"{len(selected_futures_list)} 期货, "
                f"{p._count_option_contracts(selected_options_dict)} 期权, "
                f"共 {len(p._subscribed_instruments)} 个合约"
            )
            if selected_futures_list or selected_options_dict:
                try:
                    from ali2026v3_trading.infra.diagnosis_service import DiagnosisProbeManager
                    DiagnosisProbeManager.start_contract_watch(p._subscribed_instruments)
                except Exception as contract_watch_e:
                    logging.warning("[ContractWatch] 启动失败: %s", contract_watch_e)
                p._e2e_counters['configured_instruments'] = len(p._subscribed_instruments)
                try:
                    _ = p.storage
                    logging.info(f"[Subscribe] storage 已就绪: {p.storage is not None}")
                except Exception as storage_e:
                    logging.error(f"[Subscribe] storage 初始化失败: {storage_e}")
                if p.storage and hasattr(p.storage, 'subscription_manager'):
                    db_count = p.storage.subscription_manager.subscribe_all_instruments(
                        selected_futures_list, selected_options_dict,
                    )
                    logging.info(f"[Subscribe] 数据库登记完成：{db_count} 个合约")
                    p._e2e_counters['preregistered_instruments'] = db_count
                    if callable(p.subscribe):
                        p._start_platform_subscribe_async(p._subscribed_instruments)
                        logging.info("[Subscribe] 平台订阅已在后台启动")
                        p._e2e_counters['platform_subscribe_called'] = len(p._subscribed_instruments)
                    else:
                        logging.warning("[Subscribe] self.subscribe 不可调用，平台API未就绪，安排延迟重试")
                        p.transition_to(StrategyState.DEGRADED)
                        import weakref as _weakref
                        _self_ref = _weakref.ref(p)
                        def _retry_platform_subscribe():
                            for attempt in range(1, 4):
                                time.sleep(5.0 * attempt)
                                _self = _self_ref()
                                if _self is None:
                                    logging.debug("[Subscribe] 策略已销毁(weakref)，终止重试")
                                    return
                                if getattr(_self, '_destroyed', False):
                                    logging.debug("[Subscribe] 策略已销毁，终止重试")
                                    return
                                if not getattr(_self, '_api_ready', False):
                                    _host = getattr(_self, '_runtime_strategy_host', None)
                                    _bind_fn = getattr(_self, 'bind_platform_apis', None)
                                    if _host and callable(_bind_fn):
                                        try:
                                            _bind_fn(_host)
                                            logging.info("[Subscribe] 延迟重试bind_platform_apis成功（第%d次）", attempt)
                                        except Exception as bind_e:
                                            logging.warning("[Subscribe] 延迟重试bind_platform_apis失败: %s", bind_e)
                                _subscribe_fn = getattr(_self, 'subscribe', None)
                                if callable(_subscribe_fn):
                                    _instruments = getattr(_self, '_subscribed_instruments', [])
                                    _start_fn = getattr(_self, '_start_platform_subscribe_async', None)
                                    if callable(_start_fn) and _instruments:
                                        _start_fn(_instruments)
                                    _e2e = getattr(_self, '_e2e_counters', None)
                                    if _e2e is not None and _instruments:
                                        _e2e['platform_subscribe_called'] = len(_instruments)
                                    logging.info("[Subscribe] 延迟重试平台订阅成功（第%d次）", attempt)
                                    _cur_state = getattr(_self, '_state', None)
                                    if _state_is(_cur_state, StrategyState.DEGRADED):
                                        _transition_fn = getattr(_self, 'transition_to', None)
                                        if callable(_transition_fn):
                                            _transition_fn(StrategyState.RUNNING)
                                        _is_running_attr = getattr(_self, '_is_running', None)
                                        if _is_running_attr is not None or hasattr(_self, '_is_running'):
                                            _self._is_running = True
                                            logging.info("[R23-SM-01-FIX] DEGRADED->RUNNING: _is_running同步为True")
                                    return
                                logging.warning("[Subscribe] 第%d次重试失败，API仍未就绪", attempt)
                            logging.error("[Subscribe] 平台API经3次重试始终未就绪，策略保持DEGRADED状态运行")
                        threading.Thread(
                            target=_retry_platform_subscribe,
                            name=f"subscribe-retry[strategy:{p.strategy_id}]",
                            daemon=True
                        ).start()
                    logging.info(f"[SyncTicks] 跳过初始全量同步，依赖定时任务增量同步")
                else:
                    logging.warning("[Subscribe] 无 subscription_manager")
            else:
                logging.warning("[Subscribe] 无合约可订阅")
            auto_load = bool(get_param_value(params, 'auto_load_history', False))
            if auto_load:
                logging.info("[StrategyCoreService.on_start] 历史K线加载启动（异步，不阻塞）...")
                p._start_historical_kline_load_async()
            else:
                logging.info("[StrategyCoreService.on_start] auto_load_history=False，跳过历史K线加载")
            p._publish_event('StrategyStarted', {'strategy_id': p.strategy_id})
            p._log_resource_ownership_table(phase='start')
            return True

    def on_stop(self) -> bool:
        p = self.p
        run_id = getattr(p, '_lifecycle_run_id', 'N/A')
        with p._lock:
            if p._state == StrategyState.STOPPED:
                logging.debug(
                    f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Already stopped"
                )
                return True
            logging.info(
                f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Stopping"
            )
            p._is_running = False
            p._is_paused = True
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.is_running = False
                _lm.is_paused = True
        jobs_zero = True
        try:
            if hasattr(p._scheduler_manager, 'pause_scheduler'):
                p._scheduler_manager.pause_scheduler()
            if hasattr(p._scheduler_manager, 'remove_jobs_by_owner'):
                removed = p._scheduler_manager.remove_jobs_by_owner(p.strategy_id)
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Removed {removed} strategy jobs"
                )
            if hasattr(p._scheduler_manager, 'wait_for_jobs_zero'):
                jobs_zero = p._scheduler_manager.wait_for_jobs_zero(timeout=10.0)
                if not jobs_zero:
                    logging.warning(
                        f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                        f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                        f"Jobs not zero after 10s, entering DEGRADED_STOP"
                    )
        except Exception as e:
            logging.error(
                f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Phase 2 error: {e}"
            )
            jobs_zero = False
        try:
            from ali2026v3_trading.infra.diagnosis_service import DiagnosisProbeManager, reset_diagnosis_grace_period
            DiagnosisProbeManager.stop_contract_watch(reason='strategy_stop')
            reset_diagnosis_grace_period()
        except Exception as contract_watch_e:
            logging.warning(
                f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                f"[run_id={run_id}] contract_watch stop error: {contract_watch_e}"
            )
        try:
            _sub_thread = getattr(p, '_platform_subscribe_thread', None)
            if _sub_thread is not None and _sub_thread.is_alive():
                _stop_event = getattr(p, '_platform_subscribe_stop', None)
                if _stop_event is not None:
                    _stop_event.set()
                _sub_thread.join(timeout=5.0)
                p._platform_subscribe_thread = None
                logging.debug("[R22-RES-03-修复] _platform_subscribe_thread已清理")
        except Exception as _cancel_err:
            logging.warning("[R22-RES-03] _platform_subscribe_thread清理失败: %s", _cancel_err)
        try:
            if hasattr(p, '_cancel_all_timers'):
                p._cancel_all_timers()
                logging.debug("[R22-RES-03-修复] _cancel_all_timers()已调用")
        except Exception as _timer_err:
            logging.warning("[R22-RES-03] _cancel_all_timers()调用失败: %s", _timer_err)
        try:
            p._stop_scheduler()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}][run_id={run_id}] _stop_scheduler error: {e}")
        try:
            p._unsubscribe_all_instruments()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}][run_id={run_id}] _unsubscribe error: {e}")
        try:
            p._shutdown_runtime_services()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}][run_id={run_id}] _shutdown_runtime error: {e}")
        if hasattr(p, '_flush_tick_buffer'):
            try:
                p._flush_tick_buffer()
            except Exception as e:
                logging.error(f"[on_stop][strategy_id={p.strategy_id}][run_id={run_id}] Failed to flush tick buffer: {e}")
        with p._lock:
            if jobs_zero:
                p.transition_to(StrategyState.STOPPED)
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Stopped"
                )
            else:
                p.transition_to(StrategyState.DEGRADED_STOP)
                logging.warning(
                    f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] DEGRADED_STOP (jobs not zero)"
                )
        try:
            p._publish_event('StrategyStopped', {'strategy_id': p.strategy_id, 'state': p._state.value})
        except Exception as e:
            logging.error(f"[on_stop][strategy_id={p.strategy_id}][run_id={run_id}] Failed to publish event: {e}")
        p._log_resource_ownership_table(phase='stop')
        try:
            p._lifecycle_platform.unsubscribe_all()
        except Exception as _lp_err:
            logging.debug("[LifecyclePlatform] unsubscribe_all 委托失败: %s", _lp_err)
        try:
            p._lifecycle_resource.cleanup_all(level='normal')
        except Exception as _lr_err:
            logging.debug("[LifecycleResource] cleanup_all 委托失败: %s", _lr_err)
        try:
            from ali2026v3_trading.risk.risk_service import generate_exchange_report
            generate_exchange_report([], output_path='logs/exchange_report.csv')
        except Exception:
            pass
        try:
            p.save_state()
        except Exception as e:
            logging.warning(f"[on_stop] save_state failed: {e}")
        return True

    def on_destroy(self) -> None:
        p = self.p
        try:
            p.destroy()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_destroy] Error: {e}", exc_info=True)

    def start(self) -> bool:
        return self.on_start()

    def stop(self) -> bool:
        return self.on_stop()

    def pause(self) -> bool:
        p = self.p
        with p._lock:
            if p._state != StrategyState.RUNNING:
                logging.warning(f"[StrategyCoreService] Cannot pause in state: {p._state}")
                return False
            p._is_paused = True
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.is_paused = True
            p.transition_to(StrategyState.PAUSED)
            logging.info(f"[StrategyCoreService] Paused: {p.strategy_id}")
            p._publish_event('StrategyPaused', {'strategy_id': p.strategy_id})
        tick_handler = getattr(p, '_tick_handler', None)
        if tick_handler and hasattr(tick_handler, '_flush_tick_buffer'):
            try:
                tick_handler._flush_tick_buffer()
                logging.info("[StrategyCoreService] pause: shard buffer已flush")
            except Exception as e:
                logging.warning("[StrategyCoreService] pause: shard buffer flush失败: %s", e)
        storage = getattr(p, 'storage', None)
        if storage and hasattr(storage, 'drain_all_queues'):
            try:
                drain_result = storage.drain_all_queues(timeout_per_queue=2.0)
                total_drained = sum(drain_result.values()) if drain_result else 0
                if total_drained > 0:
                    logging.info("[StrategyCoreService] pause: drain完成 %s", drain_result)
            except Exception as e:
                logging.warning("[StrategyCoreService] pause: drain失败: %s", e)
        return True

    def resume(self) -> bool:
        p = self.p
        with p._lock:
            if p._state != StrategyState.PAUSED:
                logging.warning(f"[StrategyCoreService] Cannot resume in state: {p._state}")
                return False
            p._is_paused = False
            p._is_running = True
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.is_paused = False
                _lm.is_running = True
            p.transition_to(StrategyState.RUNNING)
            logging.info(f"[StrategyCoreService] Resumed: {p.strategy_id} [R23-SM-01-FIX] _is_running同步为True")
            p._publish_event('StrategyResumed', {'strategy_id': p.strategy_id})
            return True

    def destroy(self) -> bool:
        p = self.p
        run_id = getattr(p, '_lifecycle_run_id', 'N/A')
        with p._lock:
            if p._destroyed:
                logging.info(
                    f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Already destroyed"
                )
                return True
            try:
                logging.info(
                    f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Destroying"
                )
                p.on_stop()
                try:
                    p._shutdown_runtime_services()
                except Exception as e:
                    logging.warning(f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}][run_id={run_id}] _shutdown_runtime_services error: {e}")
                p._scheduler = None
                p._event_bus = None
                p._destroyed = True
                try:
                    if hasattr(p, '_lsm_instance') and p._lsm_instance is not None:
                        p._lsm_instance.transition_to("DESTROYED")
                except Exception as _lsm_err:
                    logging.debug("[LifecycleStateMachine] DESTROYED 委托失败: %s", _lsm_err)
                try:
                    p._lifecycle_resource.cleanup_all(level='final')
                except Exception as _lr_err:
                    logging.debug("[LifecycleResource] cleanup_all(destroy) 委托失败: %s", _lr_err)
                p._stats = {
                    'start_time': None, 'total_ticks': 0, 'total_trades': 0, 'total_signals': 0,
                    'errors_count': 0, 'last_error_time': None, 'last_error_message': None,
                    'tick_by_type': {'future': 0, 'option': 0}, 'tick_by_exchange': {}, 'tick_by_instrument': {},
                    'kline_stats': {'total_requested': 0, 'success': 0, 'failed': 0, 'total_klines_loaded': 0, 'by_instrument': {}}
                }
                logging.info(
                    f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Destroyed"
                )
                p._publish_event('StrategyDestroyed', {'strategy_id': p.strategy_id})
                return True
            except Exception as e:
                logging.error(
                    f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Failed: {e}"
                )
                p._stats['errors_count'] += 1
                p._stats['last_error_time'] = datetime.now(CHINA_TZ)
                p._stats['last_error_message'] = str(e)
                return False

    def save_state(self) -> bool:
        p = self.p
        try:
            if not hasattr(p, '_storage') or not p._storage or not hasattr(p._storage, 'save'):
                logging.error("[save_state] Storage not available or missing save method")
                return False
            state_data = {
                'strategy_id': p.strategy_id,
                'state': p._state.value,
                'stats': p._stats,
                'saved_at': datetime.now(CHINA_TZ).isoformat()
            }
            save_result = p._storage.save(f'strategy_state_{p.strategy_id}', state_data)
            if not save_result:
                raise RuntimeError("Storage save returned False")
            loaded_data = p._storage.load(f'strategy_state_{p.strategy_id}')
            if not loaded_data:
                raise RuntimeError("Data verification failed: cannot load saved state")
            if loaded_data.get('strategy_id') != p.strategy_id:
                raise RuntimeError("Data verification failed: strategy_id mismatch")
            logging.info("[save_state] State saved and verified")
            return True
        except Exception as e:
            logging.error(f"[save_state] Failed: {e}", exc_info=True)
            return False

    def _shutdown_runtime_services(self) -> None:
        p = self.p
        p._shutdown_historical_services()
        if p._storage is not None and hasattr(p._storage, '_stop_async_writer'):
            try:
                p._storage._stop_async_writer()
            except Exception as e:
                logging.warning(f"[StrategyCoreService] Storage async writer stop error: {e}")
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            _rs = get_risk_service()
            if _rs is not None and hasattr(_rs, 'stop'):
                _rs.stop()
        except Exception as e:
            logging.warning("[StrategyCoreService] R24-P1-CF-05: RiskService stop error: %s", e)

    def _log_resource_ownership_table(self, phase: str = 'unknown') -> None:
        p = self.p
        import threading as _threading
        _sid = getattr(p, 'strategy_id', None)
        if _sid is None:
            logging.warning("[P1-R11-17] strategy_id未提供，使用'unknown'作为默认值。")
            _sid = 'unknown'
        strategy_id = _sid
        run_id = getattr(p, '_lifecycle_run_id', 'N/A')
        ALLOWED_PREFIXES = (
            'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
            'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
            'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
            'TTypeService-Preload[shared-service]', 'onStop-worker',
        )
        threads = _threading.enumerate()
        strategy_threads = []
        shared_threads = []
        system_threads = []
        for t in threads:
            name = t.name or ''
            if '[shared-service]' in name:
                shared_threads.append(name)
            elif any(name.startswith(p_) for p_ in ALLOWED_PREFIXES):
                system_threads.append(name)
            elif 'strategy' in name.lower() or strategy_id in name:
                strategy_threads.append(name)
            elif name and not name.startswith('Main'):
                system_threads.append(name)
        logging.info(
            f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
            f"[source_type=resource-ownership] "
            f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
            f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
        )
        if shared_threads:
            for name in shared_threads:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] Thread alive: {name} (expected: continues after strategy stop)"
                )
        if strategy_threads:
            for name in strategy_threads:
                logging.warning(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] LEAKED thread: {name} (expected: should be gone after strategy stop)"
                )
                for t in _threading.enumerate():
                    if t.name == name and t.is_alive():
                        if hasattr(t, '_stop_requested'):
                            t._stop_requested = True
                        if not t.daemon:
                            t.daemon = True
                        logging.warning(
                            "[R33-P2-3] 已为僵尸线程 %s 设置中断标记(_stop_requested=True)和daemon=True", name,
                        )
                        break
        else:
            if phase == 'stop':
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] No strategy-instance threads leaked"
                )
        scheduler_mgr = getattr(p, '_scheduler_manager', None)
        if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
            try:
                remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
                if remaining_jobs:
                    job_ids = [j['job_id'] for j in remaining_jobs]
                    logging.warning(
                        f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=strategy-job] LEAKED scheduler jobs: {job_ids}"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=strategy-job] No strategy-instance scheduler jobs leaked"
                        )
            except Exception as e:
                logging.debug(f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}] Scheduler diagnosis error: {e}")
        storage = getattr(p, '_storage', None)
        if storage and hasattr(storage, 'get_queue_stats'):
            try:
                qstats = storage.get_queue_stats()
                qsize = qstats.get('current_queue_size', 0)
                if qsize > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] Storage queue backlog: {qsize} tasks"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=shared-queue-drain] Storage queue empty"
                        )
            except Exception as e:
                logging.debug(f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}] Storage queue diagnosis error: {e}")
        event_bus = getattr(p, '_event_bus', None)
        if event_bus and hasattr(event_bus, '_pending_events'):
            try:
                pending = getattr(event_bus, '_pending_events', 0)
                if pending > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] EventBus pending callbacks: {pending}"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=event-tail] EventBus pending callbacks empty"
                        )
            except Exception as e:
                logging.debug(f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}] EventBus diagnosis error: {e}")

    def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        p = self.p
        run_id = getattr(p, '_lifecycle_run_id', 'N/A')
        if p._event_bus:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'strategy_id': p.strategy_id,
                    'run_id': run_id,
                    'source_type': 'event-tail',
                    **data
                })()
                p._event_bus.publish(event, async_mode=True)
            except Exception as e:
                logging.debug(
                    f"[StrategyCoreService][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=event-tail] "
                    f"Failed to publish {event_type}: {e}"
                )