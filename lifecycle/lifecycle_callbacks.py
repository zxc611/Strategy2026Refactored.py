# MODULE_ID: M1-121
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

from infra.shared_utils import CHINA_TZ
from config.params_service import get_param_value
from lifecycle.lifecycle_state_machine import StrategyState, _state_is


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
            from config.state_param import get_state_param_manager
            spm = get_state_param_manager()
            if hasattr(p, 't_type_service') and p.t_type_service:
                wc = getattr(p.t_type_service, '_width_cache', None)
                if wc:
                    spm.bind_width_cache(wc)
                    logging.info("[on_start] bind_width_cache 成功, wc type=%s", type(wc).__name__)
                else:
                    logging.error("[on_start] bind_width_cache 失败! _width_cache=None, 五态分类将无法更新")
            p._state_param_manager = spm
            # FIX-56 RC-13: SPM注册到_state_store，确保tick_hft.py可通过get_ref访问
            # 根因: V3报告FIX-12声称已落地但实际未落地，set_ref从未被调用
            #       → tick_hft.py:154 get_ref('_state_param_manager')返回None
            #       → resonance_strength=0, SPM五态分类系统失效
            # 修复: 补全set_ref注册，同时保留属性赋值作为兼容性fallback
            try:
                _ss_spm = getattr(p, '_state_store', None)
                if _ss_spm is not None:
                    _ss_spm.set_ref('_state_param_manager', spm)
                    logging.info("[FIX-56] SPM已注册到_state_store (set_ref成功)")
            except Exception as _spm_ref_err:
                logging.warning("[on_start] SPM set_ref注册失败(非阻断): %s", _spm_ref_err)
            logging.info("[StrategyCoreService.on_start] StateParamManager initialized, state=%s", spm.get_current_state())
            try:
                from strategy.strategy_ecosystem import get_strategy_ecosystem
                eco = get_strategy_ecosystem()
                spm.register_on_state_switch(eco.on_state_switched)
                logging.info("[StrategyCoreService.on_start] SPM-Ecosystem联动已绑定")
            except Exception as eco_e:
                logging.warning("[StrategyCoreService.on_start] Ecosystem联动绑定失败: %s", eco_e)
        except Exception as spm_e:
            logging.warning("[StrategyCoreService.on_start] StateParamManager init failed: %s", spm_e)
        try:
            from risk.risk_service import get_safety_meta_layer
            from config.params_service import get_params_service
            ps = get_params_service()
            _sid = str(getattr(p, 'strategy_id', '') or 'global')
            p._safety_meta_layer = get_safety_meta_layer(params=ps, strategy_id=_sid)
            logging.info("[StrategyCoreService.on_start] SafetyMetaLayer initialized")
        except Exception as safety_e:
            logging.warning("[StrategyCoreService.on_start] SafetyMetaLayer init failed: %s", safety_e)
        try:
            _bl = getattr(p, '_business_layer', None)
            _ps = getattr(p, '_position_service', None)
            if _ps is None and _bl is not None:
                _ps = getattr(_bl, '_position_service', None)
            if _ps is None and _bl is not None:
                try:
                    # FIX-20260704-PS-INIT: ensure_position_service()返回None(副作用设置)，
                    # 旧代码直接赋值导致_ps永远为None→SnapshotCollector注入被跳过
                    _bl.ensure_position_service()
                    _ps = getattr(_bl, '_position_service', None) or getattr(p, '_position_service', None)
                except Exception as _pass_err_0:
                    logging.debug("[on_start] 异常(非阻断): %s", _pass_err_0)
            if _bl is None:
                logging.info("[StrategyCoreService.on_start] SnapshotCollector注入跳过: _business_layer=None")
            elif _ps is None:
                logging.info("[StrategyCoreService.on_start] SnapshotCollector注入跳过: _position_service=None")
            else:
                _sc = _bl.get_snapshot_collector()
                if _sc is None:
                    logging.info("[StrategyCoreService.on_start] SnapshotCollector注入跳过: snapshot_collector=None")
                else:
                    _ps.set_snapshot_collector(_sc)
                    logging.info("[StrategyCoreService.on_start] SnapshotCollector注入PositionService完成")
        except Exception as _sc_e:
            logging.debug("[StrategyCoreService.on_start] SnapshotCollector注入跳过: %s", _sc_e)
        try:
            params = getattr(p, 'params', None) or {}
            if params.get('debug_mode', False):
                logging.getLogger().setLevel(logging.DEBUG)
                logging.info("[P2-R8-06] debug_mode激活: 日志级别设为DEBUG")
                if p._safety_meta_layer:
                    # [P0-29修复] 通过 _circuit_breaker_svc._calm_period_duration 设置无限冷静期
                    p._safety_meta_layer._circuit_breaker_svc._calm_period_duration = float('inf')
                    logging.info("[P2-R8-06] debug_mode: 断路器冷却期设为无限")
            if params.get('stress_test_mode', False):
                logging.critical("[P2-R8-06] stress_test_mode激活: 启用极端场景测试配置")
                if p._safety_meta_layer:
                    p._safety_meta_layer.DEFAULT_MAX_DRAWDOWN = 0.02
                    _stress_sigma = float(getattr(p, '_params', {}).get("stress_test_anomaly_threshold", 1.5)) if hasattr(p, '_params') and p._params else 1.5
                    p._safety_meta_layer.ANOMALY_THRESHOLD_MULTIPLIER = _stress_sigma
                    logging.info("[P2-R8-06] stress_test_mode: 回撤阈值2%%/断路器%.1f sigma", _stress_sigma)
            # [DRY-RUN m3-07] 启动assertion校验dry_run_mode状态与互斥规则
            _dry_run = bool(params.get('dry_run_mode', False))
            if _dry_run:
                # 互斥性校验: dry_run与stress_test不可同时开启
                assert not params.get('stress_test_mode', False), \
                    "[STARTUP-ASSERT] dry_run_mode 与 stress_test_mode 不可同时开启(违反mutual_exclusion规则)"
                # 打印dry_run横幅提醒
                logging.warning("=" * 60)
                logging.warning("[DRY-RUN] 策略以模拟模式启动，不会实际下单！")
                logging.warning("[DRY-RUN] 订单将被拦截并记录，虚拟回调将注入")
                logging.warning("[DRY-RUN] CascadeJudge/CyclicDependencyGuard已跳过")
                logging.warning("[DRY-RUN] 切回实盘: 将 params.yaml:dry_run_mode 设为 false 并重启")
                logging.warning("=" * 60)
                # 标记到策略对象，便于运行时检查
                try:
                    setattr(p, '_dry_run_active', True)
                    # FIX-GG-REAL: 传播_dry_run_active到PositionService和SignalService
                    # 根因: position_command_service.py L479从PositionService实例读取_dry_run_active
                    #       但原代码只SET到StrategyCoreService实例(p)，PositionService是独立实例
                    #       导致FIX-A(dry_run保证金旁路)的第二路径不触发
                    # 修复(v2): _business_layer为None时也要尝试从p直接读取，避免传播块被整体跳过
                    _bl = getattr(p, '_business_layer', None)
                    _ps = getattr(_bl, '_position_service', None) if _bl is not None else None
                    if _ps is None:
                        _ps = getattr(p, '_position_service', None)
                    if _ps is not None:
                        setattr(_ps, '_dry_run_active', True)
                        logging.info("[FIX-GG-REAL] 已传播_dry_run_active=True到PositionService")
                    _ss = getattr(_bl, '_signal_service', None) if _bl is not None else None
                    if _ss is None:
                        _ss = getattr(p, '_signal_service', None)
                    if _ss is not None:
                        setattr(_ss, '_dry_run_active', True)
                        logging.info("[FIX-GG-REAL] 已传播_dry_run_active=True到SignalService")
                except Exception as _pass_err_1:
                    logging.debug("[on_start] 异常(非阻断): %s", _pass_err_1)
                # [FIX-20260708-DRY-RUN-V2] 同步到OrderService._dry_run_mode
                try:
                    from order.order_base import get_order_service
                    _osvc = get_order_service()
                    if _osvc is not None:
                        _osvc._dry_run_mode = True
                        logging.info("[DRY-RUN] 已同步_dry_run_mode=True到OrderService")
                except Exception as _pass_err_2:
                    logging.debug("[on_start] 异常(非阻断): %s", _pass_err_2)
        except Exception as mode_e:
            logging.debug("[P2-R8-06] debug/stress mode处理异常: %s", mode_e)
        try:
            from data.data_service import get_data_service
            ds = get_data_service()
            # FIX-20260702-HARDEN: 记录嵌入式环境安全模式状态
            try:
                from data.ds_db_connection import DBConnectionMixin, _TimedDuckDBConnection
                if DBConnectionMixin._EMBEDDED_MODE:
                    logging.info(
                        "[FIX-20260702-HARDEN] DataService 预热完成(嵌入式安全模式: "
                        "pool_disabled=%s, threads=%s, sync_exec=%s)",
                        DBConnectionMixin._POOL_DISABLED,
                        DBConnectionMixin._EMBEDDED_THREADS,
                        _TimedDuckDBConnection._SYNC_EXEC,
                    )
            except Exception as _pass_err_3:
                logging.debug("[on_start] 异常(非阻断): %s", _pass_err_3)
            logging.info(f"[StrategyCoreService.on_start] DataService预热完成: {ds is not None}")
        except Exception as ds_e:
            logging.warning(f"[StrategyCoreService.on_start] DataService预热失败: {ds_e}")
        # FIX-20260709-PAUSE-DIAG: 入口诊断日志，追踪平台回调
        import traceback as _tb_start
        try:
            _caller_stack_start = ''.join(_tb_start.format_stack()[-3:]).strip()
        except Exception as _fs_err:
            _caller_stack_start = ""
            logging.debug("[on_start] format_stack失败(非阻断): %s", _fs_err)
        try:  # FIX-56: 保护入口诊断日志的属性访问，防止极端场景下跳过整个on_start
            logging.critical(
                "[FIX-20260709-PAUSE-DIAG] on_start ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s _is_trading=%s\n"
                "caller_stack:\n%s",
                getattr(p, 'strategy_id', 'N/A'), getattr(p, '_state', 'N/A'),
                getattr(p, '_is_running', 'N/A'), getattr(p, '_is_paused', 'N/A'),
                getattr(p, '_is_trading', 'N/A'), _caller_stack_start,
            )
        except Exception as _diag_err:
            logging.debug("[on_start] 入口诊断日志失败(非阻断): %s", _diag_err)
        with p._lock:
            # FIX-20260713: 添加STOPPED到允许状态，支持从STOPPED状态重新启动
            # FIX-20260714-DEGRADED-STOP: 添加DEGRADED_STOP到允许状态，支持从降级停止恢复启动
            if p._state not in (StrategyState.INITIALIZING, StrategyState.RUNNING, StrategyState.PAUSED, StrategyState.DEGRADED, StrategyState.STOPPED, StrategyState.DEGRADED_STOP):
                logging.warning(f"[StrategyCoreService.on_start] Cannot start in state: {p._state}")
                return False
            # FIX-20260713: STOPPED状态先转换到INITIALIZING
            # FIX-20260714-DEGRADED-STOP: DEGRADED_STOP也先转换到INITIALIZING（合法转换，lifecycle_state_machine.py:55）
            if p._state in (StrategyState.STOPPED, StrategyState.DEGRADED_STOP):
                logging.info(f"[FIX-20260714] on_start: {p._state}→INITIALIZING 转换")
                _lm = getattr(p, '_lifecycle_mgr', None)
                if _lm is not None:
                    _lm.state = StrategyState.INITIALIZING
                p._state = StrategyState.INITIALIZING
            # FIX-20260709-PAUSE-ROOT-V2: 四元状态原子同步
            # 原代码遗漏 _is_trading=True，导致从PAUSED状态恢复后无法交易
            p._is_paused = False
            p._is_running = True
            p._is_trading = True
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.is_paused = False
                _lm.is_running = True
            _ss = getattr(p, '_state_store', None)
            if _ss is not None:
                try:
                    _ss.set('_is_running', True)
                    _ss.set('_is_paused', False)
                    _ss.set('_is_trading', True)
                except Exception as _pass_err_4:
                    logging.debug("[on_start] 异常(非阻断): %s", _pass_err_4)
            p.transition_to(StrategyState.RUNNING)
            logging.info(f"[StrategyCoreService.on_start] Started: {p.strategy_id}")
            # FIX-20260714-STOP-EVENTS: 创建线程停止事件（原代码遗漏，导致on_stop无法通知线程退出）
            # 根因: _subscribe_retry_stop/_deferred_subscribe_stop/_historical_kline_stop/_bulk_subscribe_stop
            #       从未被创建，on_stop()中set()的是None，线程无法被通知退出
            # 修复: 在on_start()创建所有线程之前，统一创建4个stop event
            import threading as _threading_stop
            if not hasattr(p, '_bulk_subscribe_stop') or p._bulk_subscribe_stop is None:
                p._bulk_subscribe_stop = _threading_stop.Event()
            if not hasattr(p, '_subscribe_retry_stop') or p._subscribe_retry_stop is None:
                p._subscribe_retry_stop = _threading_stop.Event()
            if not hasattr(p, '_deferred_subscribe_stop') or p._deferred_subscribe_stop is None:
                p._deferred_subscribe_stop = _threading_stop.Event()
            if not hasattr(p, '_historical_kline_stop') or p._historical_kline_stop is None:
                p._historical_kline_stop = _threading_stop.Event()
            logging.info("[on_start] 4个线程stop event已创建: bulk/retry/deferred/kline")
            params = None
            if hasattr(p, '_runtime_strategy_host') and p._runtime_strategy_host:
                params = getattr(p._runtime_strategy_host, 'params', None)
            if params is None:
                logging.warning("[Subscribe] 无法获取 params 对象，跳过订阅")
                return True
            selected_futures_list = p._init_instruments_result.get('futures_list', [])
            selected_options_dict = p._init_instruments_result.get('options_dict', {})
            p._subscribed_instruments = p._init_instruments_result.get('subscribed_instruments', [])

            import os as _os_validate
            _validate_limit = int(_os_validate.environ.get('SUBSCRIBE_VALIDATE_MODE', '0') or '0')
            if _validate_limit > 0:
                logging.warning(
                    "[Subscribe] SUBSCRIBE_VALIDATE_MODE=%d: 仅订阅前 %d 个合约验证启动",
                    _validate_limit, _validate_limit,
                )
                selected_futures_list = selected_futures_list[:_validate_limit]
                _limited_opts = {}
                _opt_count = 0
                for _uk, _uopts in selected_options_dict.items():
                    if _opt_count >= _validate_limit:
                        break
                    _remaining = _validate_limit - _opt_count
                    _take = _uopts[:_remaining]
                    _limited_opts[_uk] = _take
                    _opt_count += len(_take)
                selected_options_dict = _limited_opts
                _limited_sub = list(selected_futures_list)
                for _ov in selected_options_dict.values():
                    _limited_sub.extend(_ov or [])
                p._subscribed_instruments = _limited_sub
            _ss = getattr(p, '_state_store', None)
            if _ss is not None:
                try:
                    _ss.set('_subscribed_instruments', p._subscribed_instruments)
                except Exception as _pass_err_5:
                    logging.debug("[on_start] 异常(非阻断): %s", _pass_err_5)
            logging.info(
                f"[Subscribe] 使用on_init结果: "
                f"{len(selected_futures_list)} 期货, "
                f"{p._count_option_contracts(selected_options_dict)} 期权, "
                f"共 {len(p._subscribed_instruments)} 个合约"
            )
            if selected_futures_list or selected_options_dict:
                try:
                    from infra.health_monitor import DiagnosisProbeManager
                    DiagnosisProbeManager.start_contract_watch(p._subscribed_instruments)
                except Exception as contract_watch_e:
                    logging.warning("[ContractWatch] 启动失败: %s", contract_watch_e)
                p._e2e_counters['configured_instruments'] = len(p._subscribed_instruments)
                try:
                    _storage = getattr(p, 'storage', None)
                    logging.info(f"[Subscribe] storage 已就绪: {_storage is not None}")
                except Exception as storage_e:
                    logging.error(f"[Subscribe] storage 初始化失败: {storage_e}")
                    _storage = None
                _sm = None
                if _storage is not None:
                    _sm = getattr(_storage, 'subscription_manager', None)
                    if callable(_sm):
                        _sm = _sm()
                if _sm is None:
                    try:
                        from data.data_service import get_data_service
                        _ds = get_data_service()
                        if _ds is not None:
                            _sm = getattr(_ds, 'subscription_manager', None)
                    except Exception as sm_fb_err:
                        logging.warning("[Subscribe] DataService fallback for subscription_manager failed: %s", sm_fb_err)
                if _sm is not None and hasattr(_sm, 'subscribe_all_instruments'):
                    # FIX-20260703-LOCK: subscribe_all_instruments移出同步阻塞路径
                    # 原同步调用在p._lock内执行16000+合约数据库登记(10+秒)导致on_start卡住
                    _db_subscribe_args = (selected_futures_list, selected_options_dict)
                    def _async_db_subscribe(_sm_ref=_sm, _args=_db_subscribe_args, _p_ref=p):
                        try:
                            # FIX-20260714-R4/R9: 启动阶段daemon线程支持pause/stop退出
                            if getattr(_p_ref, '_bulk_subscribe_stop', None) is not None and _p_ref._bulk_subscribe_stop.is_set():
                                logging.info("[Subscribe] db-subscribe线程收到停止事件，取消数据库登记")
                                return
                            _cnt = _sm_ref.subscribe_all_instruments(*_args)
                            logging.info(f"[Subscribe] 数据库登记完成：{_cnt} 个合约")
                            _p_ref._e2e_counters['preregistered_instruments'] = _cnt
                            # FIX-20260707-SUB-RET: 检查C++订阅失败数并重试
                            try:
                                from data.data_service import DataService
                                _fail_cnt = DataService.get_subscribe_fail_count()
                                _total = len(_args[0]) + sum(len(v) for v in _args[1].values()) if _args[1] else len(_args[0])
                                logging.info(
                                    "[Subscribe] C++订阅结果: success=%d failed=%d total=%d (失败率=%.1f%%)",
                                    _cnt, _fail_cnt, _total, (_fail_cnt * 100.0 / _total) if _total > 0 else 0,
                                )
                            except Exception as _fc_err:
                                logging.debug("[Subscribe] 获取失败计数异常: %s", _fc_err)
                            # 重试失败订阅: 排空pending队列
                            _retry_max = 3
                            for _retry_idx in range(1, _retry_max + 1):
                                # FIX-20260714-R4/R9: 重试期间检查停止事件
                                if getattr(_p_ref, '_bulk_subscribe_stop', None) is not None and _p_ref._bulk_subscribe_stop.is_set():
                                    logging.info("[Subscribe] db-subscribe重试阶段收到停止事件，终止")
                                    return
                                try:
                                    _pending = list(getattr(_sm_ref, '_pending_subscriptions', []) or [])
                                    if not _pending:
                                        break
                                    _sleep_sec = _retry_idx * 2.0
                                    logging.info(
                                        "[Subscribe] 重试#%d: %d个失败订阅 (delay=%ds)",
                                        _retry_idx, len(_pending), _sleep_sec,
                                    )
                                    # FIX-20260714-R9-INTERRUPT: 可中断sleep
                                    if _p_ref._bulk_subscribe_stop.wait(_sleep_sec):
                                        logging.info("[Subscribe] db-subscribe重试sleep期间收到停止事件，终止")
                                        return
                                    _sm_ref._pending_subscriptions = []
                                    _retry_ok = 0
                                    _retry_fail = 0
                                    for _task in _pending:
                                        try:
                                            _sm_ref._do_subscribe(_task.get('instrument_id'), _task.get('data_type', 'tick'))
                                            _retry_ok += 1
                                        except Exception as _re:
                                            _retry_fail += 1
                                            if _retry_fail <= 5:
                                                logging.debug("[Subscribe] 重试失败: %s - %s", _task.get('instrument_id'), _re)
                                    logging.info(
                                        "[Subscribe] 重试#%d完成: ok=%d fail=%d remaining_pending=%d",
                                        _retry_idx, _retry_ok, _retry_fail, len(getattr(_sm_ref, '_pending_subscriptions', []) or []),
                                    )
                                except Exception as _retry_err:
                                    logging.warning("[Subscribe] 重试#%d异常: %s", _retry_idx, _retry_err)
                                    break
                            # 诊断C++ sub_market_data返回值 (采样)
                            try:
                                from infra.health_monitor import DiagnosisProbeManager
                                DiagnosisProbeManager.diagnose_subscribe_api_return(_p_ref)
                            except Exception as _diag_err:
                                logging.debug("[Subscribe] diagnose_subscribe_api_return跳过: %s", _diag_err)
                            # FIX-20260707-WATCHDOG-RESUB: tick watchdog 重订阅
                            # 根因: C++ sub_market_data 返回 None 无法检测失败,
                            # 约36%期权在订阅后仍无tick, 需要通过tick到达情况反向检测并重订阅
                            try:
                                _wd_result = _sm_ref.resubscribe_no_tick_instruments(
                                    delay_sec=120.0, max_rounds=3,
                                    batch_size=500, batch_pause_sec=0.3,
                                )
                                logging.info(
                                    "[Subscribe] WatchdogResub结果: total=%d no_tick=%d resubscribed=%d tick_received=%d coverage=%.1f%% rounds=%d",
                                    _wd_result.get('total_subscribed', 0),
                                    _wd_result.get('no_tick_count', 0),
                                    _wd_result.get('resubscribed', 0),
                                    _wd_result.get('tick_received_after', 0),
                                    (_wd_result.get('tick_received_after', 0) * 100.0 / max(_wd_result.get('total_subscribed', 1), 1)),
                                    _wd_result.get('rounds_executed', 0),
                                )
                            except Exception as _wd_err:
                                logging.warning("[Subscribe] WatchdogResub异常: %s", _wd_err)
                        except Exception as _e:
                            logging.error(f"[Subscribe] 数据库登记失败: {_e}", exc_info=True)
                    _bulk_thread = threading.Thread(
                        target=_async_db_subscribe,
                        name=f"db-subscribe[{p.strategy_id}]",
                        daemon=True,
                    )
                    # FIX-20260703-CRASH: 存储批量订阅线程引用，供 _start_historical_kline_load_async 等待
                    # 根因：sub_market_data(订阅)和get_kline_data(K线)并发调用触发StrategyLib.dll STATUS_STACK_BUFFER_OVERRUN(0xc0000409)
                    p._bulk_subscribe_thread = _bulk_thread
                    _bulk_thread.start()
                    logging.info("[Subscribe] 数据库登记已异步启动，不阻塞on_start")
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
                            # FIX-20260714-R4/R9: 重试线程响应pause/stop退出事件
                            _stop_evt = getattr(p, '_subscribe_retry_stop', None)
                            if _stop_evt is not None and _stop_evt.is_set():
                                logging.info("[Subscribe] subscribe-retry线程收到停止事件，终止")
                                return
                            # FIX-20260714-R9-INTERRUPT: 可中断sleep，避免stop期间阻塞
                            if _stop_evt is not None:
                                if _stop_evt.wait(5.0 * attempt):
                                    logging.info("[Subscribe] subscribe-retry线程在sleep期间收到停止事件，终止")
                                    return
                            else:
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
                                # FIX-20260707-PAUSE-SAFE: 异步重试线程恢复RUNNING前必须检查暂停状态
                                # 根因: 用户在DEGRADED期间点击暂停→_is_paused=True/_state=PAUSED，
                                #       但异步线程不检查_is_paused直接设_state=RUNNING→暂停被无声覆盖
                                if getattr(_self, '_is_paused', False):
                                    logging.info("[Subscribe] 策略已暂停，跳过DEGRADED→RUNNING恢复")
                                    return
                                if _state_is(_cur_state, StrategyState.DEGRADED):
                                    _transition_fn = getattr(_self, 'transition_to', None)
                                    if callable(_transition_fn):
                                        _transition_fn(StrategyState.RUNNING)
                                    # FIX-20260711-PAUSE-RACE: transition_to后二次检查_is_paused，
                                    # 防止用户在_is_paused检查与transition_to之间的竞态窗口内暂停
                                    if getattr(_self, '_is_paused', False):
                                        logging.warning("[Subscribe] DEGRADED→RUNNING期间策略被暂停，回退状态")
                                        if callable(_transition_fn):
                                            _transition_fn(StrategyState.PAUSED)
                                        return
                                    # FIX-20260708-PAUSE-ROOT: 同步_is_trading=True，确保恢复后可交易
                                    _lock = getattr(_self, '_lock', None)
                                    if _lock is not None:
                                        with _lock:
                                            _self._is_running = True
                                            _self._is_trading = True
                                    else:
                                        _self._is_running = True
                                        _self._is_trading = True
                                    logging.info("[R23-SM-01-FIX] DEGRADED->RUNNING: _is_running同步为True")
                                return
                            logging.warning("[Subscribe] 第%d次重试失败，API仍未就绪", attempt)
                        logging.error("[Subscribe] 平台API经3次重试始终未就绪，策略保持DEGRADED状态运行")
                    # FIX-20260714-R17: 保存subscribe-retry线程引用，供on_stop停止
                    _retry_thread = threading.Thread(
                        target=_retry_platform_subscribe,
                        name=f"subscribe-retry[strategy:{p.strategy_id}]",
                        daemon=True
                    )
                    p._subscribe_retry_thread = _retry_thread
                    _retry_thread.start()
                logging.info(f"[SyncTicks] 跳过初始全量同步，依赖定时任务增量同步")
                if _sm is None:
                    logging.warning("[Subscribe] 无 subscription_manager")
                _deferred = p._init_instruments_result.get('deferred_instruments', []) if p._init_instruments_result else []
                _deferred_opts = p._init_instruments_result.get('deferred_options', {}) if p._init_instruments_result else {}
                _tts_deferred_chk = getattr(p, '_deferred_t_type_options', None)
                if _deferred or _deferred_opts or _tts_deferred_chk:
                    _deferred_count = len(_deferred) + sum(len(v) for v in _deferred_opts.values())
                    _tts_deferred_count = sum(len(v) for v in _tts_deferred_chk['options_dict'].values()) if _tts_deferred_chk and _tts_deferred_chk.get('options_dict') else 0
                    logging.info("[Subscribe] 启动延迟合约异步加载: %d 个延迟合约 + %d 个延迟期权t_type 将在5秒后加载", _deferred_count, _tts_deferred_count)
                    def _load_deferred_instruments():
                        # FIX-20260714-R9-INTERRUPT: 可中断sleep，前5秒内即可响应停止事件
                        _stop_evt = getattr(p, '_deferred_subscribe_stop', None)
                        if _stop_evt is not None:
                            if _stop_evt.wait(5.0):
                                logging.info("[Subscribe] deferred-subscribe线程在延迟期间收到停止事件，终止")
                                return
                        else:
                            time.sleep(5.0)
                        # FIX-20260714-R4/R9: 延迟加载线程响应pause/stop退出事件
                        if _stop_evt is not None and _stop_evt.is_set():
                            logging.info("[Subscribe] deferred-subscribe线程收到停止事件，终止")
                            return
                        try:
                            from infra.subscription_service import SubscriptionManager
                            _deferred_futures = [x for x in _deferred if not SubscriptionManager.is_option(x)]
                            _deferred_option_ids = [x for x in _deferred if SubscriptionManager.is_option(x)]
                            # [FIX-20260703-DLL] 延迟合约(on_init分区时已超出DLL 16287上限)禁止重新订阅
                            # 重新订阅/t_type注册会导致 STATUS_STACK_BUFFER_OVERRUN 崩溃
                            _dll_skip_total = len(_deferred_futures) + len(_deferred_option_ids)
                            if _dll_skip_total > 0:
                                logging.warning(
                                    "[DLL-Safety] 跳过 %d 个延迟合约的平台订阅+t_type注册(超出DLL上限16287): "
                                    "期货=%d, 期权=%d",
                                    _dll_skip_total, len(_deferred_futures), len(_deferred_option_ids),
                                )
                            _tts = getattr(p, 't_type_service', None)
                            if _tts is not None:
                                # [DLL安全] 仅处理 _deferred_t_type_options(在限额内，由warmup延迟至此)
                                # 不处理 _deferred_futures/_deferred_option_ids(已超限额)
                                _tts_deferred = getattr(p, '_deferred_t_type_options', None)
                                if _tts_deferred and _tts_deferred.get('options_dict'):
                                    _tts_opt_total = sum(len(v) for v in _tts_deferred['options_dict'].values())
                                    logging.info("[TTS-Deferred] 期权t_type分批注册开始: %d 个", _tts_opt_total)
                                    _tts_opt_dict = _tts_deferred['options_dict']
                                    _tts_opt_meta = _tts_deferred['options_metadata']
                                    _tts_reg = 0
                                    _tts_proc = 0
                                    _TTS_BATCH = 1000
                                    for _product, _opt_ids in _tts_opt_dict.items():
                                        for _oid in (_opt_ids or []):
                                            _tts_proc += 1
                                            try:
                                                # FIX-20260704-TTS-DEFERRED-HANG: TTS延迟注册线程必须零DB访问
                                                # 根因: 本线程与_bulk_subscribe_thread并发写入/读取DuckDB单连接(无锁)会挂起
                                                # 期权元数据在on_init阶段已由options_metadata预计算完整，直接取用即可
                                                _meta = _tts_opt_meta.get(_oid, {})
                                                _iid = _meta.get('internal_id')
                                                _ufid = _meta.get('underlying_future_id')
                                                _op = _meta.get('product')
                                                _mo = _meta.get('year_month')
                                                _ot = _meta.get('option_type')
                                                _sk = _meta.get('strike_price')
                                                if _iid is None or _ufid is None or not _op or not _mo or not _ot or _sk is None or _sk <= 0:
                                                    continue
                                                # FIX-20260704-TTS-DEFERRED-HANG: 不再调用 get_latest_price(_oid)
                                                # initial_price仅为WidthStrengthCache种子值，0.0安全：on_option_tick会以真实tick更新
                                                _price = 0.0
                                                _tts.register_option_contract(
                                                    instrument_id=_oid,
                                                    underlying_product=_op,
                                                    month=_mo,
                                                    strike_price=float(_sk),
                                                    option_type=_ot,
                                                    initial_price=float(_price),
                                                    underlying_future_id=int(_ufid),
                                                    internal_id=int(_iid),
                                                )
                                                _tts_reg += 1
                                            except Exception as _oe:
                                                logging.warning("[TTS-Deferred] 注册期权 %s 失败: %s", _oid, _oe)
                                            if _tts_proc % _TTS_BATCH == 0:
                                                logging.info("[TTS-Deferred] 期权注册进度: %d/%d (成功=%d)", _tts_proc, _tts_opt_total, _tts_reg)
                                                time.sleep(0.2)
                                    logging.info("[TTS-Deferred] 期权t_type注册完成: %d/%d", _tts_reg, _tts_opt_total)
                                    p._deferred_t_type_options = None
                            else:
                                logging.warning("[Subscribe] t_type_service不可用，跳过延迟合约t_type注册")
                            logging.info("[Subscribe] 延迟合约加载完成: %d 个合约(DLL安全跳过%d个超限合约)",
                                         _deferred_count, _dll_skip_total)
                            # [FIX-20260703-DLL] 不再将超限延迟合约加入_subscribed_instruments
                            # 这些合约无法被DLL处理，加入后会触发平台层错误
                        except Exception as _d_err:
                            logging.error("[Subscribe] 延迟合约加载失败: %s", _d_err)
                    # FIX-20260714-R17: 保存deferred-subscribe线程引用，供on_stop停止
                    _deferred_thread = threading.Thread(
                        target=_load_deferred_instruments,
                        name=f"deferred-subscribe[strategy:{p.strategy_id}]",
                        daemon=True,
                    )
                    p._deferred_subscribe_thread = _deferred_thread
                    _deferred_thread.start()
            else:
                logging.warning("[Subscribe] 无合约可订阅")
            auto_load = bool(get_param_value(params, 'auto_load_history', True))
            if auto_load:
                logging.info("[StrategyCoreService.on_start] 历史K线加载启动（异步，不阻塞）...")
                p._start_historical_kline_load_async()
            else:
                logging.info("[StrategyCoreService.on_start] auto_load_history=False，跳过历史K线加载")
            # FIX-20260706-OPTION-DIAGNOSIS: 重启时重新注册期权5态诊断任务
            # 根因: 重启跳过on_init→_start_analytics_warmup_async→_register_analytics_jobs
            # 导致期权5态诊断任务未注册，日志中无期权5态统计输出
            try:
                if hasattr(p, '_add_option_status_diagnosis_job'):
                    p._add_option_status_diagnosis_job()
                    logging.info("[StrategyCoreService.on_start] 期权5态诊断任务已重新注册（重启场景）")
            except Exception as _diag_err:
                logging.warning("[StrategyCoreService.on_start] 期权5态诊断任务注册失败: %s", _diag_err)
            p._publish_event('StrategyStarted', {'strategy_id': p.strategy_id})
            p._log_resource_ownership_table(phase='start')
            return True

    def on_stop(self) -> bool:
        p = self.p
        run_id = getattr(p, '_lifecycle_run_id', 'N/A')
        # FIX-20260709-PAUSE-DIAG: 入口诊断日志，追踪平台回调
        import traceback as _tb
        try:
            _caller_stack = ''.join(_tb.format_stack()[-3:]).strip()
        except Exception as _fs_err:
            _caller_stack = ""
            logging.debug("[on_stop] format_stack失败(非阻断): %s", _fs_err)
        try:  # FIX-56: 保护入口诊断日志的属性访问，防止极端场景下跳过整个on_stop
            logging.critical(
                "[FIX-20260709-PAUSE-DIAG] on_stop ENTER: strategy_id=%s run_id=%s state=%s _is_running=%s _is_paused=%s _is_trading=%s\n"
                "caller_stack:\n%s",
                getattr(p, 'strategy_id', 'N/A'), run_id, getattr(p, '_state', 'N/A'),
                getattr(p, '_is_running', 'N/A'), getattr(p, '_is_paused', 'N/A'),
                getattr(p, '_is_trading', 'N/A'), _caller_stack,
            )
        except Exception as _diag_err:
            logging.debug("[on_stop] 入口诊断日志失败(非阻断): %s", _diag_err)
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
            # FIX-20260709-PAUSE-ROOT-V2: 四元状态原子同步
            # 原代码遗漏 _is_trading=False，违反PAUSE-SAFE四元同步规范
            # FIX-20260714-R16: _is_paused应为False（STOPPED状态不是暂停状态）
            # 根因R16: on_stop()设置_is_paused=True，但transition_to(STOPPED)会覆盖为False
            #         竞态窗口内其他线程读取_is_paused得到错误值
            # FIX-42: 保护with p._lock内的状态赋值和tick_svc调用
            try:
                p._is_running = False
                p._is_paused = False
                p._is_trading = False
            except Exception as _pass_err_6:
                logging.debug("[on_stop] 异常(非阻断): %s", _pass_err_6)
            _tick_svc = getattr(p, '_tick_svc', None)
            if _tick_svc is not None:
                try:
                    if hasattr(_tick_svc, 'on_stop'):
                        _tick_svc.on_stop()
                    else:
                        _tick_svc._flush_stop_requested = True
                except Exception as _tick_stop_err:
                    logging.warning("[on_stop] _tick_svc.on_stop()失败(非阻断): %s", _tick_stop_err)
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                try:
                    _lm.is_running = False
                    _lm.is_paused = False
                except Exception as _pass_err_7:
                    logging.debug("[on_stop] 异常(非阻断): %s", _pass_err_7)
            _ss = getattr(p, '_state_store', None)
            if _ss is not None:
                try:
                    _ss.set('_is_running', False)
                    _ss.set('_is_paused', False)
                    _ss.set('_is_trading', False)
                except Exception as _pass_err_8:  # FIX-40: 扩展异常类型，防止OSError等跳过on_stop后续清理
                    logging.debug("[on_stop] 异常(非阻断): %s", _pass_err_8)
        jobs_zero = True
        try:
            # FIX-20260714-SCHED-SAFE: pause_scheduler添加SchedulerNotRunningError保护
            # 根因: on_start失败时APScheduler未启动，on_stop调用pause_scheduler()崩溃
            if hasattr(p._scheduler_manager, 'pause_scheduler'):
                try:
                    p._scheduler_manager.pause_scheduler()
                except Exception as _pause_err:
                    logging.warning(f"[on_stop] pause_scheduler失败(非致命，调度器可能未启动): {_pause_err}")
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
            from infra.health_monitor import DiagnosisProbeManager, reset_diagnosis_grace_period
            DiagnosisProbeManager.stop_contract_watch(reason='strategy_stop')
            reset_diagnosis_grace_period()
        except Exception as contract_watch_e:
            logging.warning(
                f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                f"[run_id={run_id}] contract_watch stop error: {contract_watch_e}"
            )
        # FIX-20260716-THREAD-V2: 统一由_cancel_all_timers停止所有托管线程
        # 根因: 原on_stop中有3处重复停止逻辑（_platform_subscribe清理+降级循环+_cancel_all_timers调用），
        #       违反方法唯一原则，且降级循环与_cancel_all_timers功能完全重复。
        # 修复: 仅保留_cancel_all_timers调用（位于lifecycle_service.py），统一停止
        #       _storage_warm/_platform_subscribe/W7/W8/W9/W11全部线程。
        try:
            if hasattr(p, '_cancel_all_timers'):
                p._cancel_all_timers()
                logging.info("[on_stop] _cancel_all_timers()已统一停止所有托管线程")
        except Exception as _timer_err:
            logging.warning("[on_stop] _cancel_all_timers()调用失败: %s", _timer_err)
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
                try:  # FIX-40: 保护transition_to，防止异常跳过save_state/shutdown_thread_pools
                    p.transition_to(StrategyState.STOPPED)
                except Exception as _ts_err:
                    logging.warning("[on_stop] transition_to(STOPPED)失败(非阻断): %s", _ts_err)
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Stopped"
                )
            else:
                try:  # FIX-40: 保护transition_to
                    p.transition_to(StrategyState.DEGRADED_STOP)
                except Exception as _ts_err:
                    logging.warning("[on_stop] transition_to(DEGRADED_STOP)失败(非阻断): %s", _ts_err)
                logging.warning(
                    f"[StrategyCoreService.on_stop][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] DEGRADED_STOP (jobs not zero)"
                )
        try:
            p._publish_event('StrategyStopped', {'strategy_id': p.strategy_id, 'state': p._state.value})
        except Exception as e:
            logging.error(f"[on_stop][strategy_id={p.strategy_id}][run_id={run_id}] Failed to publish event: {e}")
        try:  # FIX-40: 保护_log_resource_ownership_table，防止异常跳过后续清理
            p._log_resource_ownership_table(phase='stop')
        except Exception as _rot_err:
            logging.warning("[on_stop] _log_resource_ownership_table失败(非阻断): %s", _rot_err)
        try:
            p._lifecycle_platform.unsubscribe_all()
        except Exception as _lp_err:
            logging.debug("[LifecyclePlatform] unsubscribe_all 委托失败: %s", _lp_err)
        try:
            p._lifecycle_resource.cleanup_all(level='normal')
        except Exception as _lr_err:
            logging.debug("[LifecycleResource] cleanup_all 委托失败: %s", _lr_err)
        try:
            from risk.risk_service import generate_exchange_report
            generate_exchange_report([], output_path='logs/exchange_report.csv')
        except Exception as _r3_err:  # FIX-40: 扩展异常类型
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            p.save_state()
        except Exception as e:
            logging.warning(f"[on_stop] save_state failed: {e}")
        # FIX-20260716-THREAD-V2: on_stop时关闭线程池（防止ThreadPoolExecutor泄漏）
        # 根因: on_stop()只停止daemon线程，不关闭ThreadPoolExecutor，导致线程池工作线程泄漏
        #       （07-15日志证据: UNCAUGHT THREAD TclError: application has been destroyed）。
        # 修复: 调用shutdown_thread_pools(wait=True)阻塞关闭线程池。
        try:
            _lr = getattr(p, '_lifecycle_resource', None)
            if _lr is not None and hasattr(_lr, 'shutdown_thread_pools'):
                _lr.shutdown_thread_pools(wait=True, timeout=5.0)
                logging.info("[on_stop] shutdown_thread_pools(wait=True)已调用")
        except Exception as e:
            logging.warning("[on_stop] shutdown_thread_pools失败: %s", e)
        # FIX-38 RC-48: on_stop时停止SubscriptionWALService的4个共享服务线程
        # 根因: SubscriptionWALService有stop_background_threads()方法但从未在on_stop中被调用
        #       →SubAsyncWriter/SubRetry/SubCleanup/TickWatchdog 4个线程在策略停止后仍运行
        #       (之前误判为"死代码"是因为Grep工具在subscription_service.py上失效)
        # 修复: 通过subscription_manager.stop_background_threads()或_wal_service.close()停止
        try:
            _ds = getattr(p, '_data_service', None) or getattr(p, 'storage', None)
            if _ds is not None:
                _sm = getattr(_ds, 'subscription_manager', None)
                if _sm is not None:
                    if hasattr(_sm, 'stop_background_threads'):
                        _sm.stop_background_threads(join_timeout=2.0)
                        logging.info("[FIX-38] on_stop: SubscriptionWALService 4个共享线程已停止")
                    elif hasattr(_sm, 'close'):
                        _sm.close(silent=True)
                        logging.info("[FIX-38] on_stop: SubscriptionWALService已关闭")
        except Exception as _sub_err:
            logging.warning("[FIX-38] on_stop: SubscriptionWALService停止失败(非阻断): %s", _sub_err)
        return True

    def on_destroy(self) -> None:
        p = self.p
        # FIX-20260709-PAUSE-DIAG: 入口诊断日志，追踪平台回调
        import traceback as _tb_destroy
        try:
            _caller_stack_destroy = ''.join(_tb_destroy.format_stack()[-3:]).strip()
        except Exception as _fs_err:
            _caller_stack_destroy = ""
            logging.debug("[on_destroy] format_stack失败(非阻断): %s", _fs_err)
        try:  # FIX-56: 保护入口诊断日志的属性访问，防止极端场景下跳过整个on_destroy
            logging.critical(
                "[FIX-20260709-PAUSE-DIAG] on_destroy ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s\n"
                "caller_stack:\n%s",
                getattr(p, 'strategy_id', 'N/A'), getattr(p, '_state', 'N/A'),
                getattr(p, '_is_running', 'N/A'), getattr(p, '_is_paused', 'N/A'),
                _caller_stack_destroy,
            )
        except Exception as _diag_err:
            logging.debug("[on_destroy] 入口诊断日志失败(非阻断): %s", _diag_err)
        try:
            p.destroy()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_destroy] Error: {e}", exc_info=True)
            # FIX-54: 即使destroy()失败也必须设置_destroyed=True
            try:
                p._destroyed = True
            except Exception as _dest_err:
                logging.warning("[on_destroy] _destroyed=True设置失败(非阻断): %s", _dest_err)

    def start(self) -> bool:
        return self.on_start()

    def stop(self) -> bool:
        return self.on_stop()

    def pause(self) -> bool:
        p = self.p
        # FIX-20260709-PAUSE-DIAG: 入口诊断日志，追踪平台/UI回调
        import traceback as _tb_pause
        try:
            _caller_stack_pause = ''.join(_tb_pause.format_stack()[-4:]).strip()
        except Exception as _fs_err:
            _caller_stack_pause = ""
            logging.debug("[pause] format_stack失败(非阻断): %s", _fs_err)
        try:  # FIX-56: 保护入口诊断日志的属性访问，防止极端场景下跳过整个pause
            logging.critical(
                "[FIX-20260709-PAUSE-DIAG] pause ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s _is_trading=%s\n"
                "caller_stack:\n%s",
                getattr(p, 'strategy_id', 'N/A'), getattr(p, '_state', 'N/A'),
                getattr(p, '_is_running', 'N/A'), getattr(p, '_is_paused', 'N/A'),
                getattr(p, '_is_trading', 'N/A'), _caller_stack_pause,
            )
        except Exception as _diag_err:
            logging.debug("[pause] 入口诊断日志失败(非阻断): %s", _diag_err)
        with p._lock:
            # FIX-20260707-PAUSE: 扩展暂停可接受状态
            # 根因: 原只接受RUNNING状态，DEGRADED(订阅重试中)或PARALLEL_RUNNING时用户点暂停返回False
            # 导致"暂停/删除时好时坏": 策略在RUNNING时可暂停，在DEGRADED时不可暂停
            # 修复: 允许从RUNNING/DEGRADED/PARALLEL_RUNNING三种运行态暂停
            _pauseable_states = (StrategyState.RUNNING, StrategyState.DEGRADED, StrategyState.PARALLEL_RUNNING)
            if p._state not in _pauseable_states:
                logging.warning(f"[StrategyCoreService] Cannot pause in state: {p._state}")
                return False
            # ⚠️ FIX-20260707-PAUSE-SAFE: 此方法修改了四元状态(_state/_is_paused/_is_running/_is_trading)，
            #    所有修改都在with p._lock内，确保原子性。任何其他代码若要修改这四个变量中的任意一个，
            #    也必须获取p._lock，否则会导致四元状态不同步→暂停/恢复功能失效。
            # FIX-20260708-PAUSE-ROOT: 补全_is_running=False和_is_trading=False，
            #    原代码只设_is_paused=True，导致暂停后_is_running仍为True，状态不一致
            # FIX-48: 保护四元状态赋值
            try:
                p._is_paused = True
                p._is_running = False
                p._is_trading = False
            except Exception as _pass_err_9:
                logging.debug("[pause] 异常(非阻断): %s", _pass_err_9)
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                try:
                    _lm.is_paused = True
                    _lm.is_running = False
                except Exception as _pass_err_10:
                    logging.debug("[pause] 异常(非阻断): %s", _pass_err_10)
            # FIX-46: 保护transition_to和_publish_event
            try:
                p.transition_to(StrategyState.PAUSED)
            except Exception as _pause_ts_err:
                logging.warning("[pause] transition_to(PAUSED)失败(非阻断): %s", _pause_ts_err)
            _ss = getattr(p, '_state_store', None)
            if _ss is not None:
                try:
                    _ss.set('_is_paused', True)
                except Exception as _pass_err_11:  # FIX-40: 扩展异常类型
                    logging.debug("[pause] 异常(非阻断): %s", _pass_err_11)
            logging.info(f"[StrategyCoreService] Paused: {p.strategy_id}")
            try:
                p._publish_event('StrategyPaused', {'strategy_id': p.strategy_id})
            except Exception as _pe_err:
                logging.warning("[pause] _publish_event失败(非阻断): %s", _pe_err)
        # FIX-20260711-PAUSE-ACTION: 暂停时必须逐个关闭正在运行的工作
        # 根因: pause()只设状态变量但不停止任何运行中的工作(APScheduler/onTick策略决策)
        # 导致暂停后定时任务继续触发、交易周期继续执行、tick策略决策继续运行
        # 修复: 冻结APScheduler(核心)，onTick层通过_is_paused检查跳过策略决策
        # 注意: 不调用_cancel_all_timers()——那是stop/destroy的职责，暂停不应销毁线程
        try:
            if hasattr(p, '_scheduler_manager') and hasattr(p._scheduler_manager, 'pause_scheduler'):
                p._scheduler_manager.pause_scheduler()
                logging.info("[StrategyCoreService] pause: APScheduler已冻结")
        except Exception as e:
            logging.warning("[StrategyCoreService] pause: pause_scheduler失败: %s", e)
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
        # FIX-20260714-PAUSE-STOP: 补全暂停时漏停的工作
        # 根因R4: pause()漏停W6(DiagnosisProbeManager)/W10(_platform_subscribe_thread)/W14(TickBufferFlushFallback)
        # 修复: 暂停时停止这3个工作（冻结后台活动，不销毁线程）
        # W6: 停止合约监控（on_start使用DiagnosisProbeManager.start_contract_watch类方法启动）
        try:
            _diag_mgr = getattr(p, '_diagnosis_probe_manager', None)
            if _diag_mgr is not None and hasattr(_diag_mgr, 'stop_contract_watch'):
                _diag_mgr.stop_contract_watch()
                logging.info("[StrategyCoreService] pause: DiagnosisProbeManager(_diagnosis_probe_manager)合约监控已停止")
            else:
                from infra.health_monitor import DiagnosisProbeManager
                DiagnosisProbeManager.stop_contract_watch()
                logging.info("[StrategyCoreService] pause: DiagnosisProbeManager类方法合约监控已停止")
        except Exception as e:
            logging.warning("[StrategyCoreService] pause: stop_contract_watch失败: %s", e)
        # FIX-20260714-R4/R9: 暂停时若W7/W8/W9仍在运行，设置退出标志让其自行退出
        for _thread_attr, _stop_attr, _desc in [
            ('_bulk_subscribe_thread', '_bulk_subscribe_stop', 'db-subscribe'),
            ('_subscribe_retry_thread', '_subscribe_retry_stop', 'subscribe-retry'),
            ('_deferred_subscribe_thread', '_deferred_subscribe_stop', 'deferred-subscribe'),
        ]:
            try:
                _stop_evt = getattr(p, _stop_attr, None)
                if _stop_evt is not None:
                    _stop_evt.set()
                    logging.info("[StrategyCoreService] pause: %s停止事件已设置", _desc)
                _t = getattr(p, _thread_attr, None)
                if _t is not None and _t.is_alive():
                    _t.join(timeout=1.0)
                    if _t.is_alive():
                        logging.warning("[StrategyCoreService] pause: %s线程仍存活（daemon，将在后台自行退出）", _desc)
            except Exception as _pause_thread_err:  # FIX-40: 扩展异常类型
                logging.warning("[StrategyCoreService] pause: %s线程停止失败: %s", _desc, _pause_thread_err)
        # W10: 停止平台订阅线程（设置stop标志，让其自行退出）
        try:
            _stop_event = getattr(p, '_platform_subscribe_stop', None)
            if _stop_event is not None:
                _stop_event.set()
                logging.info("[StrategyCoreService] pause: _platform_subscribe_stop已设置")
        except Exception as e:  # FIX-40: 扩展异常类型
            logging.warning("[StrategyCoreService] pause: _platform_subscribe_stop设置失败: %s", e)
        # W14: 停止TickBufferFlushFallback线程
        try:
            _tick_svc = getattr(p, '_tick_svc', None)
            if _tick_svc is not None and hasattr(_tick_svc, 'on_stop'):
                _tick_svc.on_stop()
                logging.info("[StrategyCoreService] pause: TickBufferFlushFallback已停止")
        except Exception as e:
            logging.warning("[StrategyCoreService] pause: _tick_svc.on_stop失败: %s", e)
        # FIX-37 RC-47: pause()不应关闭线程池
        # 根因: pause()调用shutdown_thread_pools(wait=False)关闭4个已注册线程池(subscription/diagnosis/heartbeat/close_retry)，
        #       但resume()不重建它们→恢复后策略无法提交任务到已关闭的ThreadPoolExecutor→RuntimeError
        # 修复: 暂停时不关闭线程池，仅通过_is_paused状态标志阻止新任务提交。on_stop()会正确关闭线程池。
        return True

    def resume(self) -> bool:
        p = self.p
        # FIX-20260713-RESUME-ROBUST: 入口日志+支持STOPPED状态恢复
        # 根因1: 原仅接受PAUSED状态，STOPPED状态调用resume静默返回False
        # 根因2: _resume_in_progress不在try/finally中，异常时标志残留导致PAUSE-GUARD永久放行
        try:  # FIX-56: 保护入口诊断日志的属性访问，防止极端场景下跳过整个resume
            logging.critical(
                "[FIX-20260713-RESUME] resume ENTER: strategy_id=%s state=%s _is_running=%s _is_paused=%s",
                getattr(p, 'strategy_id', 'N/A'), getattr(p, '_state', 'N/A'),
                getattr(p, '_is_running', 'N/A'), getattr(p, '_is_paused', 'N/A'),
            )
        except Exception as _diag_err:
            logging.debug("[resume] 入口诊断日志失败(非阻断): %s", _diag_err)
        with p._lock:
            # FIX-20260713: 允许从PAUSED和STOPPED状态恢复
            if p._state == StrategyState.PAUSED:
                pass  # 正常恢复路径
            elif p._state == StrategyState.STOPPED:
                # STOPPED → INITIALIZING → RUNNING 两步转换
                logging.info("[FIX-20260713-RESUME] 从STOPPED状态恢复，先转换到INITIALIZING")
                # FIX-48: 保护_lm.state和p._state赋值
                try:
                    _lm = getattr(p, '_lifecycle_mgr', None)
                    if _lm is not None:
                        _lm.state = StrategyState.INITIALIZING
                    p._state = StrategyState.INITIALIZING
                except Exception as _stopped_ts_err:
                    logging.warning("[resume] STOPPED→INITIALIZING状态转换失败(非阻断): %s", _stopped_ts_err)
            else:
                logging.warning(f"[StrategyCoreService] Cannot resume in state: {p._state}")
                return False
            # FIX-20260711-PAUSE-GUARD: 设置_resume_in_progress标志，允许transition_to(RUNNING)通过暂停保护
            # FIX-20260713: 使用try/finally确保标志一定被清除
            p._resume_in_progress = True
            try:
                p._is_paused = False
                p._is_running = True
                # FIX-20260708-PAUSE-ROOT: 补全_is_trading=True，原代码遗漏导致恢复后无法交易
                p._is_trading = True
                _lm = getattr(p, '_lifecycle_mgr', None)
                if _lm is not None:
                    _lm.is_paused = False
                    _lm.is_running = True
                # FIX-47: 保护transition_to(RUNNING)和_publish_event
                try:
                    p.transition_to(StrategyState.RUNNING)
                except Exception as _resume_ts_err:
                    logging.warning("[resume] transition_to(RUNNING)失败(非阻断): %s", _resume_ts_err)
                _ss = getattr(p, '_state_store', None)
                if _ss is not None:
                    try:
                        _ss.set('_is_running', True)
                        _ss.set('_is_paused', False)
                        _ss.set('_is_trading', True)
                    except Exception as _pass_err_12:  # FIX-40: 扩展异常类型
                        logging.debug("[resume] 异常(非阻断): %s", _pass_err_12)
                logging.info(f"[StrategyCoreService] Resumed: {p.strategy_id} [R23-SM-01-FIX] _is_running同步为True")
                try:
                    p._publish_event('StrategyResumed', {'strategy_id': p.strategy_id})
                except Exception as _re_err:
                    logging.warning("[resume] _publish_event失败(非阻断): %s", _re_err)
            finally:
                # FIX-20260713: 确保标志一定被清除，防止PAUSE-GUARD永久放行
                p._resume_in_progress = False
        # FIX-20260711-PAUSE-ACTION: 恢复时必须逐个恢复暂停时关闭的工作
        # 对称于pause()中暂停scheduler和取消timer的操作
        try:
            if hasattr(p, '_scheduler_manager') and hasattr(p._scheduler_manager, 'resume_scheduler'):
                p._scheduler_manager.resume_scheduler()
                logging.info("[StrategyCoreService] resume: APScheduler已恢复")
        except Exception as e:
            logging.warning("[StrategyCoreService] resume: resume_scheduler失败: %s", e)
        # FIX-20260711-PAUSE-ACTION: 恢复时检查并重新注册交易定时任务
        # APScheduler的pause()不会删除job，但为安全起见检查job数量
        try:
            _job_count = 0
            if hasattr(p, '_scheduler_manager') and hasattr(p._scheduler_manager, 'ensure_trading_jobs'):
                _job_count = p._scheduler_manager.ensure_trading_jobs()
            if _job_count == 0 and hasattr(p, '_add_trading_jobs'):
                p._add_trading_jobs()
                logging.info("[StrategyCoreService] resume: 交易定时任务已重新注册")
        except Exception as e:
            logging.warning("[StrategyCoreService] resume: ensure_trading_jobs失败: %s", e)
        # FIX-20260714-RESUME-RESTART: 恢复pause()中停止的W6/W10/W14
        # 根因: pause()停止了W6(DiagnosisProbeManager)/W10(_platform_subscribe_thread)/W14(TickBufferFlushFallback)
        #       但resume()未重启它们→恢复后无平台订阅/无tick缓冲刷新/无合约监控
        # 修复: resume()中重启这3个工作
        # W10: 重置stop event + 重新启动平台订阅线程（需传instrument_ids参数）
        try:
            _stop_event = getattr(p, '_platform_subscribe_stop', None)
            if _stop_event is not None:
                _stop_event.clear()  # 重置stop event
            _subscribed = getattr(p, '_subscribed_instruments', [])
            if hasattr(p, '_start_platform_subscribe_async') and _subscribed:
                p._start_platform_subscribe_async(_subscribed)
                logging.info("[StrategyCoreService] resume: 平台订阅线程已重启(instruments=%d)", len(_subscribed))
        except Exception as e:
            logging.warning("[StrategyCoreService] resume: 平台订阅重启失败: %s", e)
        # W14: 重置_flush_stop_requested + 重启TickBufferFlushFallback线程
        try:
            _tick_svc = getattr(p, '_tick_svc', None)
            if _tick_svc is not None:
                _tick_svc._flush_stop_requested = False  # 重置停止标志
                # 检查fallback线程是否存活，不存活则重启
                _fallback_thread = getattr(_tick_svc, '_flush_fallback_thread', None)
                if _fallback_thread is None or not _fallback_thread.is_alive():
                    import threading as _threading_resume
                    _flush_interval = float(getattr(_tick_svc, '_tick_buffer_flush_interval', 1.0))
                    def _periodic_flush_fallback():
                        while not getattr(_tick_svc, '_flush_stop_requested', False):
                            try:
                                import time as _time_flush
                                _time_flush.sleep(_flush_interval)
                                if getattr(_tick_svc, '_flush_stop_requested', False):
                                    break
                                if hasattr(_tick_svc, '_shard_buffers') and _tick_svc._shard_buffers:
                                    from strategy.tick_processing_service import flush_tick_buffer
                                    flush_tick_buffer(_tick_svc)
                            except Exception as _flush_err:
                                import logging as _logging_flush
                                _logging_flush.debug("[RESUME] periodic_flush_fallback error: %s", _flush_err)
                    _new_thread = _threading_resume.Thread(target=_periodic_flush_fallback, daemon=True, name='TickBufferFlushFallback[resume]')
                    _new_thread.start()
                    _tick_svc._flush_fallback_thread = _new_thread
                    logging.info("[StrategyCoreService] resume: TickBufferFlushFallback线程已重启")
                else:
                    logging.info("[StrategyCoreService] resume: TickBufferFlushFallback线程仍存活，仅重置标志")
        except Exception as e:  # FIX-40: 扩展异常类型
            logging.warning("[StrategyCoreService] resume: TickBufferFlushFallback重启失败: %s", e)
        # W6: 重启合约监控
        try:
            from infra.health_monitor import DiagnosisProbeManager
            _host = getattr(p, '_runtime_strategy_host', None)
            DiagnosisProbeManager.start_contract_watch(_host)
            logging.info("[StrategyCoreService] resume: DiagnosisProbeManager合约监控已重启")
        except Exception as e:  # FIX-40: 扩展异常类型
            logging.warning("[StrategyCoreService] resume: 合约监控重启失败: %s", e)
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
                # FIX-C (2026-07-18): EventBus 显式 shutdown 防止线程池泄漏
                # 根因: destroy() 仅解除引用 p._event_bus = None，未调用 shutdown()
                #       导致 SP-01 EventBus 线程池工作线程泄漏
                # 修复: 显式调用 shutdown() 后再解除引用，与 strategy_2026.onDestroy 的
                #       _coordinated_shutdown 配合，确保所有线程池资源释放
                try:
                    _eb = getattr(p, '_event_bus', None)
                    if _eb is not None and hasattr(_eb, 'shutdown'):
                        _eb.shutdown(wait=False)
                        logging.info(f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}][run_id={run_id}] EventBus.shutdown()已完成")
                except Exception as _eb_err:
                    logging.warning(f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}][run_id={run_id}] EventBus.shutdown失败(非阻断): {_eb_err}")
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
                try:
                    p._publish_event('StrategyDestroyed', {'strategy_id': p.strategy_id})
                except Exception as _pe_err:  # FIX-50: 保护_publish_event
                    logging.warning("[destroy] _publish_event失败(非阻断): %s", _pe_err)
                return True
            except Exception as e:
                logging.error(
                    f"[StrategyCoreService.destroy][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] Failed: {e}"
                )
                # FIX-44: 即使destroy失败也必须设置_destroyed=True和执行final cleanup
                try:
                    p._destroyed = True
                except Exception as _pass_err_13:
                    logging.debug("[destroy] 异常(非阻断): %s", _pass_err_13)
                try:
                    if hasattr(p, '_lifecycle_resource') and p._lifecycle_resource is not None:
                        p._lifecycle_resource.cleanup_all(level='final')
                except Exception as _final_err:
                    logging.warning("[destroy] cleanup_all(final)失败(非阻断): %s", _final_err)
                p._stats['errors_count'] += 1
                p._stats['last_error_time'] = datetime.now(CHINA_TZ)
                p._stats['last_error_message'] = str(e)
                return False

    def save_state(self) -> bool:
        p = self.p
        try:
            if not hasattr(p, '_storage') or not p._storage or not hasattr(p._storage, 'save'):
                logging.debug("[save_state] Storage not available or missing save method")
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
        # FIX-43: 保护_shutdown_historical_services()
        try:
            p._shutdown_historical_services()
        except Exception as _hs_err:
            logging.warning(f"[StrategyCoreService] _shutdown_historical_services error: {_hs_err}")
        if p._storage is not None and hasattr(p._storage, '_stop_async_writer'):
            try:
                p._storage._stop_async_writer()
            except Exception as e:
                logging.warning(f"[StrategyCoreService] Storage async writer stop error: {e}")
        try:
            from risk.risk_service import get_risk_service
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
            try:
                from config.config_service import get_cached_params
                _cp = get_cached_params() or {}
                _rs = _cp.get('strategy')
                _sid = getattr(_rs, 'strategy_id', None) if _rs is not None else None
            except Exception:
                pass
        if _sid is None:
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
            elif 'strategy' in name.lower() or str(strategy_id) in name:
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
            _leaked_log_ts = getattr(self, '_leaked_thread_log_ts', {})
            _now = __import__('time').time()
            for name in strategy_threads:
                _last_ts = _leaked_log_ts.get(name, 0.0)
                if _now - _last_ts > 300.0:
                    _leaked_log_ts[name] = _now
                    logging.debug(
                        f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=resource-ownership] LEAKED thread: {name} (expected: should be gone after strategy stop)"
                    )
                for t in _threading.enumerate():
                    if t.name == name and t.is_alive():
                        if hasattr(t, '_stop_requested'):
                            t._stop_requested = True
                        if not t.daemon:
                            t.daemon = True
                        if _now - _last_ts > 300.0:
                            logging.debug(
                                "[R33-P2-3] 已为僵尸线程 %s 设置中断标记(_stop_requested=True)和daemon=True", name,
                            )
                        break
            self._leaked_thread_log_ts = _leaked_log_ts
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
                    logging.debug(
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