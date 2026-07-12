"""Strategy2026 主策略类 — 从strategy_core_service.py拆分"""
from __future__ import annotations

import logging
import os
import importlib
_stdlib_signal = importlib.import_module('signal')
import sys
import threading
import time
from typing import Any, Dict, Optional

from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState, _state_is
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id

try:
    from pythongo.ui import BaseStrategy
except ImportError:
    try:
        from pythongo.base import BaseStrategy
    except ImportError:
        logging.warning("[R16-P0-DEP-01] pythongo BaseStrategy不可用，使用兜底基类")
        class BaseStrategy:
            pass

try:
    from ali2026v3_trading.config.ui_service import UIMixin
except ImportError:
    class UIMixin:
        def __init__(self, *args, **kwargs):
            try:
                super().__init__(*args, **kwargs)
            except TypeError:
                pass


class Strategy2026(BaseStrategy, UIMixin):
    """策略 2026 主类"""

    def __init__(self, *args, **kwargs):
        # 尽早初始化日志，确保后续所有日志能正常输出
        try:
            from ali2026v3_trading.config.config_logging import setup_logging
            setup_logging()
        except Exception as _log_err:
            import logging as _logging_fallback
            _logging_fallback.basicConfig(level=_logging_fallback.INFO)
            _logging_fallback.warning("[Strategy2026.__init__] setup_logging失败，使用fallback: %s", _log_err)
        
        BaseStrategy.__init__(self)
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
        _sid = kwargs.get('strategy_id')
        logging.info("[Strategy2026.__init__] strategy_id from kwargs: %s (type=%s)", _sid, type(_sid).__name__)
        # FIX-20260706-STRATEGY-ID-TYPE: strategy_id必须为int
        # 根因: INFINIGO.updateState(StrategyID=...) 要求int，传str触发TypeError
        # health_monitor.py:2766 的 strategy_id in name 已改为 str(strategy_id) in name
        if isinstance(_sid, int):
            self.strategy_id = _sid
        elif isinstance(_sid, str) and _sid.isdigit():
            self.strategy_id = int(_sid)
        else:
            self.strategy_id = int(time.time())
        logging.info("[Strategy2026.__init__] strategy_id final: %s (type=%s)", self.strategy_id, type(self.strategy_id).__name__)
        self.strategy_core = StrategyCoreService(self.strategy_id)
        self.auto_trading_enabled = False
        self._callbacks_enabled = True
        self._stop_requested = False
        self._storage_warm_timer = None
        from ali2026v3_trading.infra.scheduler_service import get_market_time_service
        self.market_time_service = get_market_time_service()
        _orig_excepthook = sys.excepthook
        def _crash_excepthook(exc_type, exc_value, exc_tb):
            logging.critical("[Strategy2026] UNCAUGHT EXCEPTION: %s: %s", exc_type.__name__, exc_value)
            _orig_excepthook(exc_type, exc_value, exc_tb)
        sys.excepthook = _crash_excepthook
        _orig_thread_hook = threading.excepthook
        def _crash_thread_hook(args):
            logging.critical("[Strategy2026] UNCAUGHT THREAD: %s: %s", args.exc_type.__name__, args.exc_value)
            _orig_thread_hook(args)
        threading.excepthook = _crash_thread_hook
        def _signal_handler(sig, frame):
            logging.critical("[Strategy2026] SIGNAL RECEIVED: sig=%s", sig)
        try:
            _stdlib_signal.signal(_stdlib_signal.SIGTERM, _signal_handler)
        except (ValueError, OSError):
            pass
        try:
            import t_type_bootstrap as ttb
            ttb._strategy_cache_instance = self
        except Exception:
            pass

    def _log_warning(self, message: str) -> None:
        logging.warning("[Strategy2026] %s", message)

    def _log_info(self, message: str) -> None:
        logging.info("[Strategy2026] %s", message)

    def _log_error(self, message: str) -> None:
        logging.error("[Strategy2026] %s", message)

    def _log_tick_summary(self, tick: Any) -> None:
        instrument_id = getattr(tick, 'instrument_id', '')
        last_price = getattr(tick, 'last_price', 0.0)
        volume = getattr(tick, 'volume', 0)
        exchange = getattr(tick, 'exchange', '')
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
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
                    logging.warning("[Strategy2026] storage 延迟预热启动失败: %s", exc)

            timer = threading.Timer(delay_seconds, _warmup)
            timer.daemon = True
            self._storage_warm_timer = timer
            timer.start()
            logging.info("[Strategy2026] 已计划在 %.2fs 后启动 storage 后台预热", delay_seconds)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
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
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return any(_is_market_open(exch) for exch in ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX'])

    def on_start(self):
        with self._lifecycle_lock:
            if self._start_executed:
                logging.warning(
                    "[Strategy2026.onStart] DUPLICATE ENTRY DETECTED! "
                    "run_id=%s, resetting flags and re-executing full start",
                    self._lifecycle_run_id,
                )
                self._start_executed = False
                self._stop_executed = False

            self._lifecycle_run_id = generate_prefixed_id("", 8)
            self._start_executed = True

        super().on_start()

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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _ds_err:
                logging.warning("[R23-IN-06-FIX] 数据源就绪检查异常: %s", _ds_err)
            return True, time.time() - _step_start
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
                logging.info("[Strategy2026.onStart] API已就绪，跳过bind_platform_apis，但仍需注入runtime_context")
                try:
                    self.strategy_core._inject_runtime_context(self)
                except Exception as inject_err:
                    logging.error("[Strategy2026.onStart] _inject_runtime_context failed: %s", inject_err)
            self._ensure_storage_injected()
            return True, time.time() - _step_start
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onStart] CRITICAL: bind_platform_apis() 错误：%s", e)
            logging.exception("[Strategy2026.onStart] bind_platform_apis() 堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=api_bind error=%s run_id=%s", e, self._lifecycle_run_id)
            return False, time.time() - _step_start

    def _ensure_storage_injected(self) -> None:
        _state_store = getattr(self.strategy_core, '_state_store', None)
        if _state_store is None:
            logging.warning("[Strategy2026] _state_store为None，无法注入storage")
            return
        _existing = _state_store.get_ref('storage') if hasattr(_state_store, 'get_ref') else None
        if _existing is not None:
            logging.info("[Strategy2026] storage已注入state_store，无需重复注入 (type=%s)", type(_existing).__name__)
            return
        try:
            from ali2026v3_trading.data.data_service import get_data_service
            _ds = get_data_service()
            if _ds is not None:
                _state_store.set_ref('storage', _ds)
                logging.info("[Strategy2026] storage注入state_store成功 (type=%s)", type(_ds).__name__)
            else:
                logging.error("[Strategy2026] get_data_service()返回None，storage注入失败")
        except Exception as e:
            logging.error("[Strategy2026] storage注入失败: %s", e)

    def _onStart_step_status_set(self, _init_steps):
        try:
            self.trading = True
            logging.info("[Strategy2026] _onStart_step_status_set: about to call update_status_bar()")
            self.update_status_bar()
            logging.info("[Strategy2026] _onStart_step_status_set: update_status_bar() completed")
            self.output("策略启动")
            _init_steps['status_set'] = True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            _init_steps['status_set'] = False
            logging.error("[Strategy2026.onStart] 状态设置错误：%s", e)

    def _onStart_step_core_init(self):
        _step_start = time.time()
        try:
            # FIX-20260706-STATE-FORCE: on_start被调用意味着on_init已完成，直接强制RUNNING
            # 根因: getattr(self.strategy_core, '_initialized', False)因StrategyCoreService的__getattr__
            #       委托机制阻塞，导致策略停留INITIALIZING状态，tick处理被跳过
            # 证据: 7月6日8:57/9:12/9:27/9:58 LIVE日志均在L350后阻塞，_already_initialized日志从未出现
            # 修复: 使用object.__getattribute__直接访问_lifecycle_mgr，绕过__getattr__，强制设置RUNNING
            #
            # ⚠️ FIX-20260707-PAUSE-SAFE (重要! 修改此处时必须遵守以下规则):
            #    1. 必须使用 transition_to(RUNNING) 而非 _state = StrategyState.RUNNING 直接赋值
            #       原因: 直接赋值绕过状态转换合法性检查，导致三元状态(_state/_is_paused/_is_running)不同步，
            #             pause()方法只接受特定状态的暂停请求，状态不一致时暂停功能失效
            #    2. 如果transition_to失败(非法转换)，fallback到带锁的直接赋值+同步_is_paused=False
            #    3. 禁止在p._lock外修改_state/_is_paused/_is_running三个变量中的任意一个
            try:
                from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState as _SS, _state_is as _si
                _mgr = object.__getattribute__(self.strategy_core, '_lifecycle_mgr')
                _current = getattr(self.strategy_core, '_state', None)
                # 优先使用正规状态转换路径（会同步三元状态）
                if hasattr(self.strategy_core, 'transition_to'):
                    with self.strategy_core._lock:
                        if _si(_current, (_SS.INITIALIZING, _SS.DEGRADED)):
                            self.strategy_core.transition_to(_SS.RUNNING)
                        elif not _si(_current, _SS.RUNNING):
                            # fallback: 非 INITIALIZING/DEGRADED/RUNNING 时尝试直接设置
                            _mgr._state = _SS.RUNNING
                            _mgr._is_running = True
                            _mgr.is_paused = False  # 同步is_paused!
                    logging.info("[Strategy2026] FIX-20260706-STATE-FORCE: transition_to(Running) 成功")
                else:
                    # 无transition_to时的旧路径（保持兼容但同步三元状态）
                    with self.strategy_core._lock:
                        _mgr._state = _SS.RUNNING
                        _mgr._is_running = True
                        _mgr.is_paused = False  # ⚠️ 关键：必须同步is_paused，否则pause()可能失效
                    logging.info("[Strategy2026] FIX-20260706-STATE-FORCE: 强制RUNNING + is_paused=False同步")
            except (ValueError, KeyError, TypeError, AttributeError) as _force_err:
                logging.warning("[Strategy2026] FIX-20260706-STATE-FORCE: 强制设置失败(非致命): %s", _force_err)
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
                    # FIX-20260706-SKIP-INIT: 跳过initialize()调用，避免__getattr__阻塞
                    # 根因: getattr(self.strategy_core, '_initialized', False)因__getattr__委托机制阻塞
                    # 修复: 使用object.__getattribute__直接访问_lifecycle_mgr._initialized，绕过__getattr__
                    # 安全性: on_start被调用意味着on_init已完成（L8541日志证实），_initialized必为True
                    try:
                        _already_initialized = object.__getattribute__(self.strategy_core, '_lifecycle_mgr')._initialized
                    except (AttributeError, Exception) as _init_err:
                        logging.warning("[Strategy2026] FIX-20260706-SKIP-INIT: 直接访问_lifecycle_mgr._initialized失败，默认True: %s", _init_err)
                        _already_initialized = True
                    logging.info("[Strategy2026] _already_initialized=%s, about to decide init path", _already_initialized)
                    if _already_initialized:
                        logging.info("[Strategy2026] strategy_core已初始化(_initialized=True)，跳过二次initialize()避免锁争用")
                    else:
                        logging.info("[Strategy2026] 正在调用 strategy_core.initialize()...")
                        init_result = self.strategy_core.initialize(self.config)
                        logging.info("[Strategy2026] strategy_core 初始化结果：%s", init_result)
                    is_running = getattr(self.strategy_core, '_is_running', False)
                    is_paused = getattr(self.strategy_core, '_is_paused', False)
                    logging.info("[Strategy2026] 初始化后状态：_is_running=%s, _is_paused=%s", is_running, is_paused)
                    # FIX-20260704-STATE-RUNNING: initialize()后若_is_running仍为False，手动设置为True
                    if not is_running:
                        try:
                            self.strategy_core._is_running = True
                            _new_state = getattr(self.strategy_core, '_state', None)
                            if _new_state is not None and hasattr(self.strategy_core, '_state'):
                                self.strategy_core._state = StrategyState.RUNNING
                            logging.info("[Strategy2026] FIX-20260704-STATE-RUNNING: _is_running已手动设置为True, _state=%s", getattr(self.strategy_core, '_state', None))
                        except (ValueError, KeyError, TypeError, AttributeError) as _state_err:
                            logging.warning("[Strategy2026] FIX-20260704-STATE-RUNNING: 设置_is_running=True失败(非致命): %s", _state_err)
                else:
                    logging.info("[Strategy2026] 无需调用 on_init，当前状态为：%s", current_state)
            # [FIX-20260710-HARD-STOP-RESET-V3] 每日开盘自动重置跨日保留的hard_stop
            # 根因V1: set_daily_start_equity()作用于risk_support.SafetyMetaLayer，但实际阻断交易的是
            #          risk_circuit_breaker.SafetyMetaLayer._drawdown_monitor_svc._daily_hard_stop_triggered
            # 根因V2: V2只重置strategy-level实例，但交易逻辑使用global实例(首次初始化完成时加载hard_stop=True)
            # 修复V3: 同时重置strategy-level和global实例，确保所有SafetyMetaLayer实例hard_stop=False
            try:
                from ali2026v3_trading.risk.risk_circuit_breaker import get_safety_meta_layer as _get_sml
                from ali2026v3_trading.infra.shared_utils import CHINA_TZ
                from datetime import datetime as _dt_hs
                _now_hs = _dt_hs.now(CHINA_TZ)
                _market_open_today = _now_hs.replace(hour=9, minute=0, second=0, microsecond=0)
                if _now_hs >= _market_open_today:
                    _sid = str(getattr(self, 'strategy_id', '') or 'global')
                    _reset_count = 0
                    # 重置所有SafetyMetaLayer实例: strategy-level + global
                    for _sml_label, _sml_inst in [
                        ('strategy', _get_sml(None, strategy_id=_sid)),
                        ('global', _get_sml(None)),
                    ]:
                        if _sml_inst is not None and _sml_inst.is_hard_stop_triggered():
                            _ddm = getattr(_sml_inst, '_drawdown_monitor_svc', None)
                            if _ddm is not None:
                                _ddm._daily_hard_stop_triggered = False
                                _ddm._daily_new_open_blocked = False
                                _ddm._daily_start_equity = 0.0
                                _reset_count += 1
                                logging.info("[FIX-0710-HARD-STOP-RESET-V3] 重置%s实例hard_stop=False: "
                                             "strategy_id=%s time=%s", _sml_label, _sid, _now_hs.strftime('%H:%M:%S'))
                            else:
                                logging.warning("[FIX-0710-HARD-STOP-RESET-V3] %s实例_drawdown_monitor_svc为None", _sml_label)
                    if _reset_count == 0:
                        logging.info("[FIX-0710-HARD-STOP-RESET-V3] 无需重置(所有实例hard_stop已为False)")
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _hs_err:
                logging.warning("[FIX-0710-HARD-STOP-RESET-V3] hard_stop重置跳过(非致命): %s", _hs_err)
            return True, time.time() - _step_start
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            _init_steps['service_container_init'] = False
            logging.warning("[Strategy2026.onStart] ServiceContainer初始化跳过（非关键）: %s", e)

    def _onStart_step_performance_monitor(self, _init_steps):
        try:
            from ali2026v3_trading.infra.metrics_registry import PathCounter
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            _init_steps['performance_monitor_init'] = False
            logging.warning("[R22-EP-P1-17] 性能监控初始化跳过: %s", e)

    def _onStart_step_market_check(self, _init_steps):
        try:
            market_open = self.is_market_open()
            logging.info("[Strategy2026] 市场开盘状态：%s", market_open)
            _init_steps['market_check'] = True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            _init_steps['market_check'] = False
            logging.error("[Strategy2026.onStart] 市场状态检查错误：%s", e)

    def _onStart_step_ui_start(self, _init_steps):
        try:
            logging.info("[Strategy2026] 正在启动 UI 界面...")
            self._start_output_mode_ui()
            logging.info("[Strategy2026] UI 界面启动成功")
            _init_steps['ui_start'] = True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
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
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] transition_to DEGRADED failed: %s", _r3_err)
                    pass
        elif _failed_all:
            logging.warning("[Strategy2026.onStart] Non-critical steps failed: %s, all results: %s.", _failed_all, _init_steps)
        else:
            logging.info("[Strategy2026.onStart] All steps succeeded: %s", _init_steps)
        _overall_elapsed = time.time() - _overall_start
        logging.info("[R23-P2-05-FIX] onStart分段计时汇总: timings=%s overall=%.3fs", _step_timings, _overall_elapsed)

    def on_init(self):
        # 平台规范：所有回调必须 super()，且应在最前面调用
        super().on_init()
        try:
            logging.info("[Strategy2026] 平台初始化回调")
            self._ensure_runtime_config_loaded()

            # 先初始化params，确保bind_platform_apis时params已就绪
            if hasattr(self.strategy_core, 'initialize'):
                result = self.strategy_core.initialize(self.config)
                logging.info("[Strategy2026.onInit] strategy_core 初始化结果：%s", result)

            self.strategy_core.bind_platform_apis(self)
            logging.info("[Strategy2026.onInit] 已刷新平台 API 到 strategy_core")

            self._init_pending = True
            self._init_error = False

            self._init_pending = False

            self._schedule_storage_warmup()

            self.output("策略初始化完毕，等待平台启动")

            # FIX-20260710-AUTO-START: 初始化完成后自动进入执行状态
            # 根因: InfiniTrader C++宿主要求用户手动点击"执行"才调用on_start()，
            #        Python代码层面无自动启动逻辑，导致策略长时间停留在INITIALIZING状态
            # 修复: 检查auto_start_after_init配置，若为True则在on_init末尾自动调用on_start()
            # 全链条: on_init() → auto_start_after_init=True → on_start() → RUNNING
            try:
                from ali2026v3_trading.config.params_service import get_param_value
                _auto_start = bool(get_param_value(getattr(self, 'config', None), 'auto_start_after_init', True))
            except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                _auto_start = True
            if _auto_start:
                logging.info("[Strategy2026.on_init] auto_start_after_init=True, 自动调用 on_start()")
                try:
                    self.on_start()
                    logging.info("[Strategy2026.on_init] 自动启动成功, state=%s", getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _as_e:
                    logging.error("[Strategy2026.on_init] 自动启动失败: %s", _as_e, exc_info=True)
            else:
                logging.info("[Strategy2026.on_init] auto_start_after_init=False, 等待手动启动")

            return None
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onInit] 错误：%s", e)
            logging.exception("[Strategy2026.onInit] 堆栈:")
            self._init_error = e
            self._init_pending = False
            return None


    def on_stop(self):
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error("[Strategy2026.onStop] Scheduler stop failed: %s", e)
        self.save_instance_file()
        logging.info("[Strategy2026.onStop] BaseStrategy non-INFINIGO steps completed")

        with self._lifecycle_lock:
            self._start_executed = False
            self._stop_executed = False
        logging.info("[Strategy2026.onStop] 生命周期标志已重置，允许下次on_start正常执行")

        # 平台规范：所有回调必须 super()
        super().on_stop()
        return None


    def internal_pause_strategy(self) -> bool:
        # FIX-20260709-PAUSE-ROOT-V3: 添加诊断日志
        # 这是UI暂停按钮实际调用的方法（通过_call_method_by_priority）
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] internal_pause_strategy ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s",
            getattr(self, 'strategy_id', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_running', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
        )
        result = False
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'pause'):
                result = bool(self.strategy_core.pause())
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.internal_pause_strategy] 错误：%s", e)
            logging.exception("[Strategy2026.internal_pause_strategy] 堆栈:")
            result = False
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] internal_pause_strategy EXIT: result=%s state=%s _is_paused=%s",
            result,
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
        )
        return result

    # FIX-20260709-PAUSE-ROOT-V3: 添加pause/on_pause方法，接收平台UI暂停回调
    # 根因: strategy_2026.py无pause方法，平台UI暂停按钮调用BaseStrategy.pause()→空操作
    #       导致LifecycleCallbacks.pause()从未被调用，四元状态从未同步到PAUSED
    # 全链条: platform.pause() → strategy_2026.pause() → strategy_core.pause() → _lifecycle_svc.pause() → LifecycleCallbacks.pause()
    def pause(self) -> bool:
        import traceback as _tb_pause
        _caller_stack_pause = ''.join(_tb_pause.format_stack()[-4:]).strip()[:300]
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] pause ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s _is_trading=%s\n"
            "caller_stack:\n%s",
            getattr(self, 'strategy_id', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_running', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_trading', 'N/A'),
            _caller_stack_pause,
        )
        result = self.internal_pause_strategy()
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] pause EXIT: result=%s state=%s _is_paused=%s",
            result,
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
        )
        return result

    # FIX-20260709-PAUSE-ROOT-V3: on_pause别名，兼容平台不同回调命名
    def on_pause(self) -> bool:
        logging.info("[Strategy2026.on_pause] 委托到 pause()")
        return self.pause()

    # FIX-20260710-RESUME: 补全resume方法，接收平台C++宿主恢复回调
    # 根因: strategy_2026.py无resume方法，平台恢复时调用BaseStrategy.resume()→空操作
    #       导致LifecycleCallbacks.resume()从未被调用，四元状态从未从PAUSED恢复到RUNNING
    # 全链条: platform.resume() → strategy_2026.resume() → strategy_core.resume() → _lifecycle_svc.resume() → LifecycleCallbacks.resume()
    def resume(self) -> bool:
        import traceback as _tb_resume
        _caller_stack_resume = ''.join(_tb_resume.format_stack()[-4:]).strip()[:300]
        logging.critical(
            "[FIX-20260710-RESUME-DIAG] resume ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s _is_trading=%s\n"
            "caller_stack:\n%s",
            getattr(self, 'strategy_id', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_running', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_trading', 'N/A'),
            _caller_stack_resume,
        )
        result = self.internal_resume_strategy()
        logging.critical(
            "[FIX-20260710-RESUME-DIAG] resume EXIT: result=%s state=%s _is_running=%s _is_paused=%s",
            result,
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_running', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
        )
        return result

    # FIX-20260710-RESUME: on_resume别名，兼容平台不同回调命名
    def on_resume(self) -> bool:
        logging.info("[Strategy2026.on_resume] 委托到 resume()")
        return self.resume()

    # FIX-20260709-PAUSE-ROOT-V3: on_destroy别名(蛇形命名)，兼容平台不同回调命名
    def on_destroy(self):
        logging.info("[Strategy2026.on_destroy] 委托到 onDestroy()")
        self.onDestroy()

    def internal_resume_strategy(self) -> bool:
        # FIX-20260709-PAUSE-ROOT-V3: 添加诊断日志
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] internal_resume_strategy ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s",
            getattr(self, 'strategy_id', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_running', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
        )
        result = False
        try:
            current_state = getattr(self.strategy_core, '_state', None)
            if _state_is(current_state, StrategyState.PAUSED) and hasattr(self.strategy_core, 'resume'):
                result = bool(self.strategy_core.resume())
            elif _state_is(current_state, StrategyState.STOPPED):
                logging.info("[Strategy2026.internal_resume_strategy] 从STOPPED状态恢复，重置启动标志后调用on_start()")
                with self._lifecycle_lock:
                    self._start_executed = False
                    self._stop_executed = False
                self.on_start()
                result = bool(_state_is(getattr(self.strategy_core, '_state', None), StrategyState.RUNNING))
            else:
                logging.warning("[Strategy2026.internal_resume_strategy] Cannot resume in state: %s", current_state)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.internal_resume_strategy] 错误：%s", e)
            logging.exception("[Strategy2026.internal_resume_strategy] 堆栈:")
            result = False
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] internal_resume_strategy EXIT: result=%s state=%s _is_paused=%s",
            result,
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
        )
        return result

    def onDestroy(self):
        # FIX-20260709-PAUSE-ROOT-V3: 暂停/删除彻底修复
        # 根因: onDestroy不调用strategy_core.on_destroy()，导致LifecycleCallbacks.on_destroy()从未被调用
        #       四元状态从未同步到STOPPED/destroyed，平台UI显示"删除"但策略实际仍在运行
        import traceback as _tb_destroy
        _caller_stack_destroy = ''.join(_tb_destroy.format_stack()[-4:]).strip()[:300]
        logging.critical(
            "[FIX-20260709-PAUSE-DIAG] onDestroy ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s\n"
            "caller_stack:\n%s",
            getattr(self, 'strategy_id', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_state', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_running', 'N/A'),
            getattr(getattr(self, 'strategy_core', None), '_is_paused', 'N/A'),
            _caller_stack_destroy,
        )
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onDestroy] enter")

        if not getattr(self, '_stop_executed', False):
            logging.info("[Strategy2026.onDestroy] onStop not executed, performing full cleanup")
            self._stop_executed = True
        else:
            logging.info("[Strategy2026.onDestroy] onStop already executed, performing remaining cleanup")

        # FIX-20260709-PAUSE-ROOT-V3: 调用strategy_core.on_destroy()触发LifecycleCallbacks.on_destroy()
        # 全链条: strategy_2026.onDestroy() → strategy_core.on_destroy() → _lifecycle_svc.on_destroy() → LifecycleCallbacks.on_destroy()
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'on_destroy'):
                logging.info("[Strategy2026.onDestroy] 调用 strategy_core.on_destroy()")
                self.strategy_core.on_destroy()
                logging.info("[Strategy2026.onDestroy] strategy_core.on_destroy() 完成")
            elif hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'destroy'):
                logging.info("[Strategy2026.onDestroy] 调用 strategy_core.destroy() (fallback)")
                self.strategy_core.destroy()
                logging.info("[Strategy2026.onDestroy] strategy_core.destroy() 完成")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onDestroy] strategy_core.on_destroy failed: %s", e, exc_info=True)

        try:
            self._destroy_output_mode_ui()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[Strategy2026.onDestroy] destroy UI failed: %s", e)

        try:
            from ali2026v3_trading.param_pool.task_scheduler import cleanup_global_data
            cleanup_global_data()
            logging.info("[R22-P0-MEM-01] onDestroy: task_scheduler全局DataFrame已释放")
        except ImportError as _ie:
            logging.warning("[R22-P0-MEM-01] onDestroy: param_pool.task_scheduler导入失败, 全局DataFrame可能未释放: %s", _ie)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[R22-P0-MEM-01] onDestroy: cleanup_global_data failed: %s", e)

    def onTick(self, tick):
        """平台 Tick 数据回调（必须返回 None）"""
        # 平台规范：所有回调必须 super()，且应在最前面调用
        super().on_tick(tick)

        # [OPTCOV_PROBE] 期权coverage诊断 - onTick绝对入口（任何过滤前）
        try:
            from ali2026v3_trading.infra.health_monitor import record_option_entry_probe
            record_option_entry_probe(getattr(tick, 'instrument_id', ''), getattr(tick, 'last_price', 0.0))
        except Exception:
            pass

        # 数据保存必须无条件执行——DEGRADED状态只跳过策略决策，不跳过数据落库
        try:
            if hasattr(self.strategy_core, 'on_tick'):
                self.strategy_core.on_tick(tick)
        except Exception as e:
            _tick_err_count = getattr(self, '_tick_err_count', 0) + 1
            self._tick_err_count = _tick_err_count
            if _tick_err_count <= 3 or _tick_err_count % 1000 == 0:
                logging.error("[Strategy2026.onTick] strategy_core.on_tick() 数据保存错误(%d次): %s", _tick_err_count, e)

        if not getattr(self, '_callbacks_enabled', True):
            return None

        # FIX-20260711-PAUSE-ACTION: 暂停状态下跳过策略决策（数据保存已在上方无条件执行）
        # 根因: onTick不检查_is_paused，暂停后PQS/信号生成等策略决策仍继续执行
        _is_paused = False
        try:
            _is_paused = getattr(self.strategy_core, '_is_paused', False) if hasattr(self, 'strategy_core') else False
            if _is_paused:
                _paused_tick_count = getattr(self, '_paused_tick_count', 0) + 1
                self._paused_tick_count = _paused_tick_count
                if _paused_tick_count <= 3 or _paused_tick_count % 1000 == 1:
                    logging.info("[Strategy2026.onTick] PAUSED状态：策略决策已跳过，数据保存继续。累计跳过%d个tick的决策", _paused_tick_count)
                return None
        except (ValueError, KeyError, TypeError, AttributeError):
            pass

        _first_tick = not getattr(self, '_tick_received', False)
        if _first_tick:
            self._tick_received = True
            instrument_id = getattr(tick, 'instrument_id', '')
            last_price = getattr(tick, 'last_price', 0.0)
            logging.info("[Strategy2026.onTick] FIRST TICK RECEIVED: instrument=%s price=%.2f — 平台数据流入确认", instrument_id, last_price)

        # R15-P0-RES-01修复: DEGRADED状态下阻断策略决策，但数据保存必须继续
        _is_degraded = False
        try:
            core_state = getattr(self.strategy_core, '_state', None) if hasattr(self, 'strategy_core') else None
            if _state_is(core_state, StrategyState.DEGRADED):
                _is_degraded = True
                _degraded_tick_count = getattr(self, '_degraded_tick_count', 0) + 1
                self._degraded_tick_count = _degraded_tick_count
                if _degraded_tick_count % 1000 == 1:
                    logging.warning("[Strategy2026.onTick] DEGRADED状态：策略决策已跳过，数据保存继续。累计跳过%d个tick的决策", _degraded_tick_count)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        # DEGRADED状态下跳过后续策略决策逻辑（数据保存已在上方无条件执行）
        if _is_degraded:
            return None

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
                    # FIX-20260709-PQS-INIT: 首次使用时自动初始化PQS，否则_initialized永远为False
                    # 导致crm.update()从未被调用→incorrect_reversal=0→spring/box/resonance未运行
                    if _pqs is not None and not _pqs._initialized:
                        _pqs.initialize()
                        logging.info("[PQS] 首次tick触发自动初始化: initialized=%s", _pqs._initialized)
                    # FIX-20260709-PQS-VERIFY: 全链条验证 — 增加调用入口日志
                    _pqs_call_count = getattr(self, '_pqs_call_count', 0) + 1
                    self._pqs_call_count = _pqs_call_count
                    if _pqs_call_count <= 2 or _pqs_call_count % 100 == 0:
                        logging.info(
                            "[PQS-VERIFY] tick节流触发PQS更新: call_count=%d initialized=%s sym=%s close=%s",
                            _pqs_call_count, _pqs._initialized if _pqs else None,
                            getattr(tick, 'instrument_id', ''), getattr(tick, 'last_price', 0),
                        )
                    if _pqs is not None and _pqs._initialized:
                        _sym = getattr(tick, 'instrument_id', '')
                        _high = getattr(tick, 'high_price', 0)
                        _low = getattr(tick, 'low_price', 0)
                        _close = getattr(tick, 'last_price', 0)
                        _ret = 0.0
                        if _close > 0:
                            _result = _pqs.update_tick(_sym, _high, _low, _close, _ret)
                            _pqs.publish_analysis_to_strategy(_result)
                            # FIX-20260709-PQS-VERIFY: 全链条验证 — 验证crm._last_output已更新
                            _crm = getattr(_pqs, '_crm', None)
                            _crm_last = getattr(_crm, '_last_output', None)
                            if _pqs_call_count <= 2 or _pqs_call_count % 100 == 0:
                                logging.info(
                                    "[PQS-VERIFY] update_tick完成: crm._last_output=%s type=%s",
                                    'None' if _crm_last is None else 'SET',
                                    type(_crm_last).__name__ if _crm_last is not None else 'N/A',
                                )
                        self._pqs_last_update_time = _now_pqs
                        self._pqs_tick_count = 0
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _pqs_err:
                # FIX-20260709-PQS-VERIFY: 全链条验证修复
                # 原因: logging.debug在INFO级别被过滤，PQS异常被静默吞没
                # 导致 FIX-20260709-PQS-INIT 修复看似已落地，实则从未在运行时被验证
                # 修复: 升级为warning级别，并增加首次异常堆栈输出便于定位
                _pqs_err_count = getattr(self, '_pqs_err_count', 0) + 1
                self._pqs_err_count = _pqs_err_count
                if _pqs_err_count <= 3 or _pqs_err_count % 100 == 0:
                    logging.warning(
                        "[R22-P1-NEW] PQS量化分析tick更新失败(第%d次, 策略缺少量化输入): %s",
                        _pqs_err_count, _pqs_err, exc_info=True,
                    )
                # FIX-20260709-PQS-VERIFY: 即便失败也记录到_state_store便于诊断
                try:
                    _ss = getattr(self, '_state_store', None) or getattr(self.strategy_core, '_state_store', None) if hasattr(self, 'strategy_core') else None
                    if _ss is not None:
                        _ss.set('_pqs_last_error', str(_pqs_err))
                        _ss.set('_pqs_error_count', _pqs_err_count)
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass

                # FIX-20260712-P0: PQS失败时的CRM降级更新 — 确保_last_output不为None
                # 根因: PQS.update_tick()失败时crm.update()从未被调用，_last_output永远为None，
                # get_risk_surface()返回保守默认max_hold=1800s，与high_freq的1min hold_override冲突。
                # 修复: PQS失败时用默认参数直接调用crm.update()，确保至少有基础风险曲面。
                try:
                    import importlib as _imp_fb
                    _crm_mod = _imp_fb.import_module('ali2026v3_trading.param_pool.optimization.cycle_sharpe')
                    _crm_inst = _crm_mod.get_cycle_resonance_module()
                    if getattr(_crm_inst, '_last_output', None) is None:
                        _crm_inst.update(
                            hmm_state='NORMAL',
                            hmm_posterior=(0.33, 0.34, 0.33),
                            trend_scores=(0.0, 0.0, 0.0),
                            trend_directions=(0.0, 0.0, 0.0),
                            strength=0.5,
                            imbalance=0.0,
                        )
                        _fb_count = getattr(self, '_crm_fallback_count', 0) + 1
                        self._crm_fallback_count = _fb_count
                        if _fb_count <= 3 or _fb_count % 100 == 0:
                            logging.warning(
                                "[FIX-20260712-P0] CRM降级更新(PQS失败fallback): count=%d, "
                                "crm._last_output已设置为NORMAL默认值", _fb_count,
                            )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _crm_fb_err:
                    _crm_fb_count = getattr(self, '_crm_fb_err_count', 0) + 1
                    self._crm_fb_err_count = _crm_fb_count
                    if _crm_fb_count <= 3 or _crm_fb_count % 100 == 0:
                        logging.warning(
                            "[FIX-20260712-P0] CRM降级更新也失败: %s (count=%d)", _crm_fb_err, _crm_fb_count,
                        )


        except Exception as e:
            _tick_err_count = getattr(self, '_tick_err_count', 0) + 1
            self._tick_err_count = _tick_err_count
            if _tick_err_count <= 3 or _tick_err_count % 1000 == 0:
                logging.error("[Strategy2026.onTick] strategy_core.on_tick() 错误(%d次): %s", _tick_err_count, e)

        return None

    def onOrder(self, order):
        """平台订单回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_order(order)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onOrder] super().on_order() 错误：%s", e)

        try:
            if hasattr(self.strategy_core, 'on_order'):
                self.strategy_core.on_order(order)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onOrder] strategy_core.on_order() 错误：%s", e)

        return None

    def onTrade(self, trade):
        """平台成交回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_trade(trade)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onTrade] super().on_trade() 错误：%s", e)

        try:
            if hasattr(self.strategy_core, 'on_trade'):
                self.strategy_core.on_trade(trade)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Strategy2026.onTrade] strategy_core.on_trade() 错误：%s", e)

        return None

    onInit = on_init
    onStart = on_start
    onStop = on_stop
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

