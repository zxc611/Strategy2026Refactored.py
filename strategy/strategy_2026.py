"""Strategy2026 主策略类 — 从strategy_core_service.py拆分"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

from ali2026v3_trading.strategy.strategy_lifecycle_mixin import StrategyState, _state_is

try:
    from pythongo.base import BaseStrategy
except ImportError:
    import logging as _import_logging
    _import_logging.warning("[R16-P0-DEP-01] pythongo.base.BaseStrategy不可用，使用兜底基类")
    class BaseStrategy:
        """pythongo缺失时的兜底策略基类"""
        pass
from ali2026v3_trading.ui_service import UIMixin


class Strategy2026(BaseStrategy, UIMixin):
    """策略 2026 主类 - 直接继承平台基类和 UI 混合类

    [P1-2归属] UIMixin被此类继承，属于UI层，在UI重构阶段(Phase 3+)处理
    | StrategyCoreService不继承UIMixin(仅继承4核心Mixin: Lifecycle/Instrument/Historical/Tick)
    """

    def __init__(self, *args, **kwargs):
        BaseStrategy.__init__(self)
        # R13-P2-API-08修复: 调用UIMixin.__init__确保Mixin正确初始化
        UIMixin.__init__(self, *args, **kwargs)

        self._is_real_strategy = True

        self._strategy = None
        self._init_pending = False
        self._init_error = None

        self._lifecycle_run_id = None
        self._start_executed = False
        self._stop_executed = False
        self._lifecycle_lock = threading.Lock()

        runtime_config = kwargs.get('config', {}) or {}
        self._runtime_config = dict(runtime_config)
        self._runtime_strategy_ref = kwargs.get('strategy')
        self._runtime_market_center_ref = kwargs.get('market_center')
        self._config_loaded = False

        logging.info(
            "[Strategy2026.__init__] type=%s, MRO=%s, kwargs_keys=%s",
            type(self).__name__,
            [c.__name__ for c in type(self).__mro__],
            list(kwargs.keys()),
        )

        bootstrap_config = dict(runtime_config)
        if self._runtime_strategy_ref is not None:
            bootstrap_config['strategy'] = self._runtime_strategy_ref
        else:
            bootstrap_config['strategy'] = self
        if self._runtime_market_center_ref is not None:
            bootstrap_config['market_center'] = self._runtime_market_center_ref

        self.config = bootstrap_config

        from ali2026v3_trading.strategy.strategy_core_service import StrategyParams, StrategyCoreService
        self.params = StrategyParams(bootstrap_config)

        self.strategy_id = kwargs.get('strategy_id', f"strategy_{int(time.time())}")
        self.strategy_core = StrategyCoreService(self.strategy_id)

        try:
            import t_type_bootstrap as ttb
            if hasattr(ttb, 'register_strategy_cache'):
                ttb.register_strategy_cache(self)
                logging.info("[Strategy2026] Registered to t_type_bootstrap via register_strategy_cache()")
        except Exception as e:
            logging.warning("[Strategy2026] Failed to register to t_type_bootstrap: %s", e)

        self.auto_trading_enabled = False
        self._callbacks_enabled = True
        self._stop_requested = False
        self._storage_warm_timer: Optional[threading.Timer] = None

        from ali2026v3_trading.infra.scheduler_service import get_market_time_service
        self.market_time_service = get_market_time_service()

        logging.debug("[Strategy2026] 已初始化，运行时配置延迟加载")
        logging.info("[Strategy2026] UI 功能已集成，继承自 UIMixin")

    def _log_warning(self, message: str) -> None:
        logging.warning("[Strategy2026] %s", message)

    def _log_info(self, message: str) -> None:
        logging.info("[Strategy2026] %s", message)

    def _log_error(self, message: str) -> None:
        logging.error("[Strategy2026] %s", message)

    def _log_tick_summary(self, tick: Any) -> None:
        instrument_id = getattr(tick, 'instrument_id', '') or getattr(tick, 'InstrumentID', '')
        last_price = getattr(tick, 'last_price', 0.0) or getattr(tick, 'LastPrice', 0.0)
        volume = getattr(tick, 'volume', 0) or getattr(tick, 'Volume', 0)
        exchange = getattr(tick, 'exchange', '') or getattr(tick, 'ExchangeID', '')
        logging.debug("[Strategy2026.onTick] %s %s price=%.2f vol=%d", exchange, instrument_id, last_price, volume)

    def _ensure_runtime_config_loaded(self) -> None:
        if self._config_loaded:
            return

        from ali2026v3_trading.config.config_service import get_cached_params

        started_at = time.perf_counter()
        loaded_config = get_cached_params()
        logging.info("[Strategy2026] 从缓存参数表加载了 %d 个参数", len(loaded_config))

        merged = dict(loaded_config)
        for key, value in self._runtime_config.items():
            if value is None:
                continue
            if isinstance(value, str) and value == "":
                continue
            if isinstance(value, (list, dict)) and len(value) == 0:
                continue
            merged[key] = value

        merged['strategy'] = self._runtime_strategy_ref or self
        runtime_market_center = self._runtime_market_center_ref or getattr(self, 'market_center', None)
        if runtime_market_center is not None:
            merged['market_center'] = runtime_market_center

        self.config = merged
        from ali2026v3_trading.strategy.strategy_core_service import StrategyParams
        self.params = StrategyParams(merged)
        self._config_loaded = True

        logging.info("[Strategy2026] params.strategy = %s", getattr(self.params, 'strategy', None))
        logging.info("[Strategy2026] 运行时配置就绪，耗时 %.3fs", time.perf_counter() - started_at)

    def _schedule_storage_warmup(self, delay_seconds: float = 0.25) -> None:
        try:
            existing_timer = getattr(self, '_storage_warm_timer', None)
            if existing_timer and existing_timer.is_alive():
                return

            def _warmup() -> None:
                try:
                    self.strategy_core._warm_storage_async()
                except Exception as exc:
                    logging.warning("[Strategy2026] storage 延迟预热启动失败: %s", exc)

            timer = threading.Timer(delay_seconds, _warmup)
            timer.daemon = True
            self._storage_warm_timer = timer
            timer.start()
            logging.info("[Strategy2026] 已计划在 %.2fs 后启动 storage 后台预热", delay_seconds)
        except Exception as exc:
            logging.warning("[Strategy2026] 安排 storage 预热失败: %s", exc)

    # R21-CC-P1-04修复: Timer取消机制 — stop时取消所有已注册的Timer
    def _cancel_all_timers(self) -> None:
        """取消所有已注册的threading.Timer，防止stop后回调仍触发"""
        _warm_timer = getattr(self, '_storage_warm_timer', None)
        if _warm_timer is not None:
            _warm_timer.cancel()
            self._storage_warm_timer = None
        # R22-RES-03修复: 线程生命周期管理 — stop时join清理
        _warm_thread = getattr(self, '_storage_warm_thread', None)
        if _warm_thread is not None and _warm_thread.is_alive():
            _warm_thread.join(timeout=5.0)
            self._storage_warm_thread = None
        _sub_thread = getattr(self, '_platform_subscribe_thread', None)
        if _sub_thread is not None and _sub_thread.is_alive():
            self._platform_subscribe_stop.set()
            _sub_thread.join(timeout=5.0)
            self._platform_subscribe_thread = None

    def is_market_open(self, exchange: str = 'AUTO') -> bool:
        from ali2026v3_trading.infra.scheduler_service import is_market_open as _is_market_open
        from ali2026v3_trading.config.config_params import get_param

        try:
            exchange = exchange or 'AUTO'
            if exchange != 'AUTO':
                return _is_market_open(exchange)

            exchanges_raw = getattr(self.params, 'exchanges', '') if hasattr(self, 'params') else ''
            if isinstance(exchanges_raw, (list, tuple, set)):
                exchanges = [str(e).strip() for e in exchanges_raw if str(e).strip()]
            else:
                exchanges = [e.strip() for e in str(exchanges_raw).split(',') if e.strip()]

            if not exchanges:
                default_exchanges_raw = get_param('exchanges', 'CFFEX,SHFE,DCE,CZCE,INE,GFEX')
                exchanges = [e.strip() for e in str(default_exchanges_raw).split(',') if e.strip()]

            return any(_is_market_open(exch) for exch in exchanges)
        except Exception:
            return any(_is_market_open(exch) for exch in ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX'])

    def onStart(self):
        with self._lifecycle_lock:
            if self._start_executed:
                logging.warning(
                    f"[Strategy2026.onStart] DUPLICATE ENTRY DETECTED! "
                    f"run_id={self._lifecycle_run_id}, skipping duplicate execution"
                )
                return None

            import uuid
            self._lifecycle_run_id = str(uuid.uuid4())[:8]
            self._start_executed = True

        logging.info("[Strategy2026.onStart] New run_id=%s", self._lifecycle_run_id)
        self._callbacks_enabled = True
        self._stop_requested = False
        logging.info("[Strategy2026] 平台启动回调")

        _init_steps = {}
        _CRITICAL_STEPS = ('config_load', 'api_bind', 'core_init', 'core_start')
        _step_timings = {}
        _overall_start = time.time()

        _init_steps['config_load'], _step_timings['config_load'] = self._onStart_step_config_load()
        _init_steps['api_bind'], _step_timings['api_bind'] = self._onStart_step_api_bind()
        self._onStart_step_status_set(_init_steps)
        _init_steps['core_init'], _step_timings['core_init'] = self._onStart_step_core_init()
        self._onStart_step_auto_recovery(_init_steps)
        self._onStart_step_service_container(_init_steps)
        self._onStart_step_performance_monitor(_init_steps)
        self._onStart_step_market_check(_init_steps)
        self._onStart_step_ui_start(_init_steps)
        _init_steps['core_start'], _step_timings['core_start'] = self._onStart_step_core_start()
        self._onStart_finalize(_init_steps, _CRITICAL_STEPS, _step_timings, _overall_start)
        return None

    def _onStart_step_config_load(self):
        _step_start = time.time()
        try:
            self._ensure_runtime_config_loaded()
            try:
                _ds = getattr(self.strategy_core, 'storage', None)
                if _ds is not None and hasattr(_ds, 'check_data_source_ready'):
                    _ds_ready, _ds_msg = _ds.check_data_source_ready()
                    if not _ds_ready:
                        logging.warning("[R23-IN-06-FIX] 数据源未就绪: %s, 策略降级为DEGRADED", _ds_msg)
                        self.strategy_core.transition_to(StrategyState.DEGRADED)
            except Exception as _ds_err:
                logging.warning("[R23-IN-06-FIX] 数据源就绪检查异常: %s", _ds_err)
            return True, time.time() - _step_start
        except Exception as e:
            logging.error("[Strategy2026.onStart] CRITICAL: 加载运行时配置错误：%s", e)
            logging.exception("[Strategy2026.onStart] 加载运行时配置堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=config_load error=%s run_id=%s", e, self._lifecycle_run_id)
            return False, time.time() - _step_start

    def _onStart_step_api_bind(self):
        _step_start = time.time()
        try:
            if not getattr(self.strategy_core, '_api_ready', False):
                self.strategy_core.bind_platform_apis(self)
                logging.info("[Strategy2026.onStart] API未就绪，已补调bind_platform_apis")
            else:
                logging.info("[Strategy2026.onStart] API已就绪，跳过bind_platform_apis")
            return True, time.time() - _step_start
        except Exception as e:
            logging.error("[Strategy2026.onStart] CRITICAL: bind_platform_apis() 错误：%s", e)
            logging.exception("[Strategy2026.onStart] bind_platform_apis() 堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=api_bind error=%s run_id=%s", e, self._lifecycle_run_id)
            return False, time.time() - _step_start

    def _onStart_step_status_set(self, _init_steps):
        try:
            self.trading = True
            self.update_status_bar()
            self.output("策略启动")
            _init_steps['status_set'] = True
        except Exception as e:
            _init_steps['status_set'] = False
            logging.error("[Strategy2026.onStart] 状态设置错误：%s", e)

    def _onStart_step_core_init(self):
        _step_start = time.time()
        try:
            logging.info("[Strategy2026] 检查 strategy_core 状态：has on_init=%s", hasattr(self.strategy_core, 'on_init'))
            if hasattr(self.strategy_core, 'on_init'):
                current_state = getattr(self.strategy_core, '_state', None)
                is_running = getattr(self.strategy_core, '_is_running', False)
                is_paused = getattr(self.strategy_core, '_is_paused', False)
                logging.info("[Strategy2026] strategy_core 状态：state=%s, _is_running=%s, _is_paused=%s", current_state, is_running, is_paused)
                if _state_is(current_state, StrategyState.STOPPED) and hasattr(self.strategy_core, 'prepare_restart'):
                    restart_ready = self.strategy_core.prepare_restart()
                    logging.info("[Strategy2026] strategy_core STOPPED -> prepare_restart=%s", restart_ready)
                    current_state = getattr(self.strategy_core, '_state', None)
                if _state_is(current_state, StrategyState.ERROR):
                    logging.error("[Strategy2026] strategy_core 处于ERROR状态，策略终止运行")
                    self.strategy_core._is_running = False
                    raise RuntimeError("合约加载/预注册失败，策略不可恢复启动。")
                if _state_is(current_state, StrategyState.INITIALIZING):
                    logging.info("[Strategy2026] 正在调用 strategy_core.initialize()...")
                    init_result = self.strategy_core.initialize(params=self.config)
                    logging.info("[Strategy2026] strategy_core 初始化结果：%s", init_result)
                    is_running = getattr(self.strategy_core, '_is_running', False)
                    is_paused = getattr(self.strategy_core, '_is_paused', False)
                    logging.info("[Strategy2026] 初始化后状态：_is_running=%s, _is_paused=%s", is_running, is_paused)
                else:
                    logging.info("[Strategy2026] 无需调用 on_init，当前状态为：%s", current_state)
            return True, time.time() - _step_start
        except Exception as e:
            logging.error("[Strategy2026.onStart] CRITICAL: strategy_core 初始化阶段错误：%s", e)
            logging.exception("[Strategy2026.onStart] strategy_core 初始化阶段堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=core_init error=%s run_id=%s", e, self._lifecycle_run_id)
            return False, time.time() - _step_start

    def _onStart_step_auto_recovery(self, _init_steps):
        try:
            if hasattr(self.strategy_core, '_auto_recovery_flow'):
                recovery_ok = self.strategy_core._auto_recovery_flow()
                _init_steps['auto_recovery'] = recovery_ok
                if not recovery_ok:
                    logging.critical("[Strategy2026.onStart] DR-P1-04: 自动化恢复失败，进入SAFE_MODE")
            else:
                _init_steps['auto_recovery'] = 'skipped'
        except Exception as e:
            _init_steps['auto_recovery'] = False
            logging.error("[Strategy2026.onStart] DR-P1-04: 自动化恢复异常: %s", e)

    def _onStart_step_service_container(self, _init_steps):
        try:
            from ali2026v3_trading.infra.service_container import ServiceContainer
            _service_container = ServiceContainer()
            _sc_ok = _service_container.create_and_register_all_services()
            if _sc_ok:
                logging.info("[Strategy2026] ServiceContainer所有服务注册并初始化成功")
            else:
                logging.warning("[Strategy2026] ServiceContainer初始化返回False，部分服务可能未就绪")
            _init_steps['service_container_init'] = True
        except Exception as e:
            _init_steps['service_container_init'] = False
            logging.warning("[Strategy2026.onStart] ServiceContainer初始化跳过（非关键）: %s", e)

    def _onStart_step_performance_monitor(self, _init_steps):
        try:
            from ali2026v3_trading.infra.performance_monitor import PathCounter
            _perf_enabled = getattr(self, 'config', None)
            if _perf_enabled is not None:
                _perf_enabled = getattr(_perf_enabled, 'performance', None)
                if _perf_enabled is not None:
                    _perf_enabled = getattr(_perf_enabled, 'enable_performance_monitoring', True)
            if _perf_enabled:
                PathCounter.enable()
                logging.info("[Strategy2026] 性能监控已启用（P0-21集成）")
            else:
                PathCounter.disable()
                logging.info("[Strategy2026] 性能监控已禁用")
            _init_steps['performance_monitor_init'] = True
        except Exception as e:
            _init_steps['performance_monitor_init'] = False
            logging.warning("[R22-EP-P1-17] 性能监控初始化跳过: %s", e)

    def _onStart_step_market_check(self, _init_steps):
        try:
            market_open = self.is_market_open()
            logging.info("[Strategy2026] 市场开盘状态：%s", market_open)
            _init_steps['market_check'] = True
        except Exception as e:
            _init_steps['market_check'] = False
            logging.error("[Strategy2026.onStart] 市场状态检查错误：%s", e)

    def _onStart_step_ui_start(self, _init_steps):
        try:
            logging.info("[Strategy2026] 正在启动 UI 界面...")
            self._start_output_mode_ui()
            logging.info("[Strategy2026] UI 界面启动成功")
            _init_steps['ui_start'] = True
        except Exception as e:
            _init_steps['ui_start'] = False
            logging.error("[Strategy2026.onStart] 启动 UI 错误：%s", e)

    def _onStart_step_core_start(self):
        _step_start = time.time()
        try:
            if hasattr(self.strategy_core, 'start'):
                result = self.strategy_core.start()
                logging.info("[Strategy2026] 策略核心启动结果：%s", result)
            else:
                logging.warning("[Strategy2026.onStart] strategy_core.start 不可用，跳过")
            return True, time.time() - _step_start
        except Exception as e:
            logging.error("[Strategy2026.onStart] CRITICAL: strategy_core.start() 错误：%s", e)
            logging.exception("[Strategy2026.onStart] strategy_core.start() 堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=core_start error=%s run_id=%s", e, self._lifecycle_run_id)
            return False, time.time() - _step_start

    def _onStart_finalize(self, _init_steps, _CRITICAL_STEPS, _step_timings, _overall_start):
        _failed_critical = [s for s in _CRITICAL_STEPS if not _init_steps.get(s, False)]
        _failed_all = [s for s, ok in _init_steps.items() if not ok]
        if _failed_critical:
            logging.error("[Strategy2026.onStart] CRITICAL STEPS FAILED: %s, all results: %s.", _failed_critical, _init_steps)
            if hasattr(self.strategy_core, 'transition_to'):
                try:
                    self.strategy_core.transition_to(StrategyState.DEGRADED)
                except Exception:
                    pass
        elif _failed_all:
            logging.warning("[Strategy2026.onStart] Non-critical steps failed: %s, all results: %s.", _failed_all, _init_steps)
        else:
            logging.info("[Strategy2026.onStart] All steps succeeded: %s", _init_steps)
        _overall_elapsed = time.time() - _overall_start
        logging.info("[R23-P2-05-FIX] onStart分段计时汇总: timings=%s overall=%.3fs", _step_timings, _overall_elapsed)

    def onInit(self):
        try:
            logging.info("[Strategy2026] 平台初始化回调")
            self._ensure_runtime_config_loaded()

            self.strategy_core.bind_platform_apis(self)
            logging.info("[Strategy2026.onInit] 已刷新平台 API 到 strategy_core")

            self._init_pending = True
            self._init_error = None

            if hasattr(self.strategy_core, 'initialize'):
                result = self.strategy_core.initialize(params=self.config)
                logging.info("[Strategy2026.onInit] strategy_core 初始化结果：%s", result)

            self._init_pending = False

            super().on_init()
            self._schedule_storage_warmup()

            return None
        except Exception as e:
            logging.error("[Strategy2026.onInit] 错误：%s", e)
            logging.info("[PROBE_PARAMS_MAP] exchange=%s", getattr(self.params_map, 'exchange', 'N/A'))
            logging.info("[PROBE_PARAMS_MAP] instrument_id=%s", getattr(self.params_map, 'instrument_id', 'N/A'))
            logging.info("[PROBE_PARAMS_MAP] exchange_list=%s", self.exchange_list[:10] if self.exchange_list else 'empty')
            logging.info("[PROBE_PARAMS_MAP] instrument_list=%s", self.instrument_list[:10] if self.instrument_list else 'empty')

            logging.exception("[Strategy2026.onInit] 堆栈:")
            self._init_error = e
            self._init_pending = False
            return None

    def onStop(self):
        with self._lifecycle_lock:
            if self._stop_executed:
                logging.warning(
                    "[Strategy2026.onStop] DUPLICATE ENTRY DETECTED! "
                    "run_id=%s, skipping duplicate execution",
                    self._lifecycle_run_id,
                )
                return None

            self._stop_executed = True

        logging.info("[Strategy2026.onStop] Stopped (run_id=%s)", self._lifecycle_run_id)

        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onStop] enter")

        try:
            logging.info("[Strategy2026] 平台停止回调")
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'stop'):
                result = self.strategy_core.stop()
                logging.info("[Strategy2026.onStop] strategy_core 停止结果：%s", result)
                # R23-IN-07-FIX: onStop清理所有实例属性，防止热重启惰性引用滞留
                for _attr in ('_shadow_engine', '_signal_service', '_strategy_ecosystem', '_hft_engine', '_snapshot_collector'):
                    if hasattr(self.strategy_core, _attr):
                        setattr(self.strategy_core, _attr, None)
                logging.info("[R23-IN-07-FIX] onStop: 已清理5个实例属性(_shadow_engine/_signal_service/_strategy_ecosystem/_hft_engine/_snapshot_collector)")
            else:
                logging.warning("[Strategy2026.onStop] strategy_core.stop 不可用，跳过")
        except Exception as e:
            logging.error("[Strategy2026.onStop] strategy_core.stop failed: %s", e)
            logging.exception("[Strategy2026.onStop] strategy_core.on_stop stack:")

        try:
            from pythongo import utils as _utils
        except ImportError:
            _utils = None
            logging.warning("[DEP-04] pythongo.utils not available, using None fallback")
        self.trading = False
        if _utils is not None:
            try:
                _utils.Scheduler("PythonGO").stop()
            except Exception as e:
                logging.error("[Strategy2026.onStop] Scheduler stop failed: %s", e)
        self.save_instance_file()
        logging.info("[Strategy2026.onStop] BaseStrategy non-INFINIGO steps completed")

        return None

    def internal_pause_strategy(self) -> bool:
        result = False
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'pause'):
                result = bool(self.strategy_core.pause())
        except Exception as e:
            logging.error("[Strategy2026.internal_pause_strategy] 错误：%s", e)
            logging.exception("[Strategy2026.internal_pause_strategy] 堆栈:")
            result = False

        return result

    def internal_resume_strategy(self) -> bool:
        result = False
        try:
            current_state = getattr(self.strategy_core, '_state', None)
            if _state_is(current_state, StrategyState.PAUSED) and hasattr(self.strategy_core, 'resume'):
                result = bool(self.strategy_core.resume())
            elif _state_is(current_state, StrategyState.STOPPED):
                logging.info("[Strategy2026.internal_resume_strategy] 从STOPPED状态恢复，重置启动标志后调用onStart()")
                with self._lifecycle_lock:
                    self._start_executed = False
                    self._stop_executed = False
                self.onStart()
                result = bool(_state_is(getattr(self.strategy_core, '_state', None), StrategyState.RUNNING))
            else:
                logging.warning("[Strategy2026.internal_resume_strategy] Cannot resume in state: %s", current_state)
        except Exception as e:
            logging.error("[Strategy2026.internal_resume_strategy] 错误：%s", e)
            logging.exception("[Strategy2026.internal_resume_strategy] 堆栈:")
            result = False

        return result

    def onDestroy(self):
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onDestroy] enter")

        if not getattr(self, '_stop_executed', False):
            logging.info("[Strategy2026.onDestroy] onStop not executed, performing full cleanup")
            self._stop_executed = True
        else:
            logging.info("[Strategy2026.onDestroy] onStop already executed, performing remaining cleanup")

        try:
            self._destroy_output_mode_ui()
        except Exception as e:
            logging.warning("[Strategy2026.onDestroy] destroy UI failed: %s", e)

        try:
            from ali2026v3_trading.param_pool.task_scheduler import cleanup_global_data
            cleanup_global_data()
            logging.info("[R22-P0-MEM-01] onDestroy: task_scheduler全局DataFrame已释放")
        except ImportError as _ie:
            logging.warning("[R22-P0-MEM-01] onDestroy: param_pool.task_scheduler导入失败, 全局DataFrame可能未释放: %s", _ie)
        except Exception as e:
            logging.warning("[R22-P0-MEM-01] onDestroy: cleanup_global_data failed: %s", e)

    def onTick(self, tick):
        """平台 Tick 数据回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        # R15-P0-RES-01修复: DEGRADED状态下阻断tick回调，避免降级后仍处理行情
        try:
            core_state = getattr(self.strategy_core, '_state', None) if hasattr(self, 'strategy_core') else None
            if _state_is(core_state, StrategyState.DEGRADED):
                return None
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        try:
            super().on_tick(tick)
        except Exception as e:
            logging.error("[Strategy2026.onTick] super().on_tick() 错误：%s", e)

        try:
            self._log_tick_summary(tick)

            # R30-ARCH-02修复: 桥接PQS量化分析到策略决策链路
            # R32-ARCH-02-v2修复: 添加tick节流，避免高频tick下PQS计算成为性能瓶颈
            try:
                _now_pqs = time.time()
                _pqs_tick_count = getattr(self, '_pqs_tick_count', 0) + 1
                self._pqs_tick_count = _pqs_tick_count
                _pqs_last_time = getattr(self, '_pqs_last_update_time', 0.0)
                _pqs_throttle_sec = 5.0
                _pqs_throttle_ticks = 10
                if (_now_pqs - _pqs_last_time) >= _pqs_throttle_sec or _pqs_tick_count >= _pqs_throttle_ticks:
                    from ali2026v3_trading.ProductionQuantSystem import get_production_quant_system
                    _pqs = get_production_quant_system()
                    if _pqs is not None and _pqs._initialized:
                        _sym = getattr(tick, 'instrument_id', '') or getattr(tick, 'symbol', '')
                        _high = getattr(tick, 'high_price', 0) or getattr(tick, 'high', 0)
                        _low = getattr(tick, 'low_price', 0) or getattr(tick, 'low', 0)
                        _close = getattr(tick, 'last_price', 0) or getattr(tick, 'close', 0)
                        _ret = getattr(tick, 'return_value', 0)
                        if _close > 0:
                            _result = _pqs.update_tick(_sym, _high, _low, _close, _ret)
                            _pqs.publish_analysis_to_strategy(_result)
                        self._pqs_last_update_time = _now_pqs
                        self._pqs_tick_count = 0
            except Exception as _pqs_err:
                logging.debug("[R22-P1-NEW] PQS量化分析tick更新失败(策略缺少量化输入): %s", _pqs_err)

            if hasattr(self.strategy_core, 'on_tick'):
                self.strategy_core.on_tick(tick)
        except Exception as e:
            logging.error("[Strategy2026.onTick] strategy_core.on_tick() 错误：%s", e)

        return None

    def onOrder(self, order):
        """平台订单回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_order(order)
        except Exception as e:
            logging.error("[Strategy2026.onOrder] super().on_order() 错误：%s", e)

        try:
            if hasattr(self.strategy_core, 'on_order'):
                self.strategy_core.on_order(order)
        except Exception as e:
            logging.error("[Strategy2026.onOrder] strategy_core.on_order() 错误：%s", e)

        return None

    def onTrade(self, trade):
        """平台成交回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_trade(trade)
        except Exception as e:
            logging.error("[Strategy2026.onTrade] super().on_trade() 错误：%s", e)

        try:
            if hasattr(self.strategy_core, 'on_trade'):
                self.strategy_core.on_trade(trade)
        except Exception as e:
            logging.error("[Strategy2026.onTrade] strategy_core.on_trade() 错误：%s", e)

        return None

    on_init = onInit
    on_start = onStart
    on_stop = onStop
    on_destroy = onDestroy
    on_tick = onTick
    on_order = onOrder
    on_trade = onTrade

    @property
    def my_is_running(self) -> bool:
        return getattr(self.strategy_core, '_is_running', False)

    @property
    def my_is_paused(self) -> bool:
        return getattr(self.strategy_core, '_is_paused', False)

    @property
    def my_trading(self) -> bool:
        if hasattr(self.strategy_core, '_is_trading'):
            return getattr(self.strategy_core, '_is_trading', True)
        return getattr(self.strategy_core, '_is_running', False) and not getattr(self.strategy_core, '_is_paused', False)

    @my_trading.setter
    def my_trading(self, value: bool) -> None:
        if hasattr(self.strategy_core, '_is_trading'):
            self.strategy_core._is_trading = bool(value)


class StrategyParams:
    """策略参数容器 - 替代 type('Params', (), dict)() 匿名类

    提供可调试的 __repr__ 和 __dir__，方便排查参数问题。
    从strategy_core_service.py迁移至此。
    """

    __slots__ = ('_data',)

    def __init__(self, data: Dict[str, Any]):
        object.__setattr__(self, '_data', dict(data))

    def __getattr__(self, name: str):
        try:
            return object.__getattribute__(self, '_data')[name]
        except KeyError:
            raise AttributeError(
                f"'StrategyParams' object has no attribute '{name}'. "
                f"Available: {sorted(object.__getattribute__(self, '_data').keys())}"
            )

    def __setattr__(self, name: str, value):
        if name == '_data':
            object.__setattr__(self, name, value)
        else:
            object.__getattribute__(self, '_data')[name] = value

    def __repr__(self) -> str:
        data = object.__getattribute__(self, '_data')
        items = {k: v for k, v in data.items() if not k.startswith('_')}
        return f"StrategyParams({items})"

    def __dir__(self):
        data = object.__getattribute__(self, '_data')
        return sorted(data.keys())

    def as_dict(self) -> Dict[str, Any]:
        return dict(object.__getattribute__(self, '_data'))

    def get(self, key: str, default=None):
        return object.__getattribute__(self, '_data').get(key, default)
