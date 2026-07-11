# MODULE_ID: M1-120
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""lifecycle_bind.py — 平台API绑定/订阅逻辑（从strategy_lifecycle_mixin.py拆分）
职责: bind_platform_apis, 平台订阅/退订, 市场中心提取, 运行时上下文注入, 历史K线异步加载
"""
from __future__ import annotations

import threading
import logging
from typing import Any, Dict, List, Optional


from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState


class LifecycleBind:
    def __init__(self, provider):
        self.p = provider

    def bind_platform_apis(self, strategy_obj: Any) -> None:
        p = self.p
        if not hasattr(p, '_lock'):
            logging.error("[StrategyCoreService] bind_platform_apis: _lock未初始化")
            return
        with p._lock:
            p._do_bind_platform_apis(strategy_obj)

    def _do_bind_platform_apis(self, strategy_obj: Any) -> None:
        p = self.p
        p._runtime_strategy_host = strategy_obj
        from ali2026v3_trading.config.config_service import resolve_product_exchange
        sub = getattr(strategy_obj, 'sub_market_data', None)
        unsub = getattr(strategy_obj, 'unsub_market_data', None)
        if callable(sub):
            _sub_call_counter = [0]
            _sub_seen = set()
            _sub_skip_counter = [0]
            _sub_rate_lock = threading.Lock()
            _sub_last_call_at = [0.0]
            _sub_min_interval_sec = 0.0
            _STACK_PRESSURE_BATCH = 500
            _STACK_PRESSURE_PAUSE_SEC = 0.3
            # FIX-M4: _subscribe 包装器接受 data_type 但不传递 → 'kline_1min' 订阅退化为 tick 订阅
            # 修复:
            #   1) 探测平台 sub() 是否接受 data_type 参数(签名长度>=3)
            #   2) 接受则透传, 不接受则按 data_type 走 fallback (直接调用 sub(exchange, instrument_id))
            #   3) 始终记录 data_type 到日志, 便于追踪订阅类型丢失问题
            import inspect as _inspect_m4
            try:
                _sub_sig = _inspect_m4.signature(sub)
                _sub_accepts_data_type = len(_sub_sig.parameters) >= 3
            except (ValueError, TypeError):
                _sub_accepts_data_type = False
            import re as _re_opt
            _option_pattern = _re_opt.compile(r'[CP]\d{4,5}$')
            def _is_option_id(inst_id: str) -> bool:
                return bool(_option_pattern.search(inst_id))
            def _subscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
                dedupe_key = (str(instrument_id).strip(), str(data_type or 'tick').strip())
                with _sub_rate_lock:
                    if dedupe_key in _sub_seen:
                        _sub_skip_counter[0] += 1
                        if _sub_skip_counter[0] <= 20 or _sub_skip_counter[0] % 1000 == 0:
                            logging.warning(
                                "[PROBE_SUB_DEDUP_SKIP] #%d exchange=%s instrument_id=%s data_type=%s reason=duplicate_local_subscribe_call",
                                _sub_skip_counter[0], exchange, instrument_id, data_type,
                            )
                        return
                    _sub_seen.add(dedupe_key)
                    import time as _sub_time
                    elapsed = _sub_time.monotonic() - _sub_last_call_at[0]
                    if _sub_last_call_at[0] > 0 and elapsed < _sub_min_interval_sec:
                        _sub_time.sleep(_sub_min_interval_sec - elapsed)
                    _sub_call_counter[0] += 1
                    _sub_last_call_at[0] = _sub_time.monotonic()
                    _call_no = _sub_call_counter[0]
                    if _call_no % _STACK_PRESSURE_BATCH == 0:
                        _sub_time.sleep(_STACK_PRESSURE_PAUSE_SEC)
                _is_opt = _is_option_id(instrument_id)
                if _call_no <= 20 or (_is_opt and _call_no % 1000 == 0):
                    logging.info(f"[PROBE_SUB] #{_call_no} exchange={exchange} instrument_id={instrument_id} data_type={data_type} is_option={_is_opt} platform_accepts_data_type={_sub_accepts_data_type}")
                # FIX-M4: 透传 data_type (如果平台支持), 否则退化调用并显式 warning
                try:
                    if _sub_accepts_data_type:
                        try:
                            sub(exchange, instrument_id, data_type)
                            return
                        except TypeError:
                            # 平台签名探测失败, 退化为 2-arg 调用
                            pass
                    if data_type != 'tick':
                        logging.warning(
                            "[FIX-M4] platform sub() 不支持 data_type 参数, 订阅 %s 退化为 tick 订阅: inst=%s data_type=%s",
                            instrument_id, instrument_id, data_type,
                        )
                    sub(exchange, instrument_id)
                except Exception:
                    with _sub_rate_lock:
                        _sub_seen.discard(dedupe_key)
                    raise
            p.subscribe = _subscribe
        else:
            p.subscribe = None
        if callable(unsub):
            # FIX-M4: 同步处理 unsub 的 data_type
            import inspect as _inspect_m4_un
            try:
                _unsub_sig = _inspect_m4_un.signature(unsub)
                _unsub_accepts_data_type = len(_unsub_sig.parameters) >= 3
            except (ValueError, TypeError):
                _unsub_accepts_data_type = False
            def _unsubscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
                if _unsub_accepts_data_type:
                    try:
                        unsub(exchange, instrument_id, data_type)
                        return
                    except TypeError:
                        pass
                unsub(exchange, instrument_id)
            p.unsubscribe = _unsubscribe
        else:
            p.unsubscribe = None
        p.get_instrument = getattr(strategy_obj, 'get_instrument', None)
        from ali2026v3_trading.config.params_service import _read_param
        p._platform_insert_order = _read_param(strategy_obj, 'insert_order') or _read_param(strategy_obj, 'make_order_req')
        p._platform_cancel_order = _read_param(strategy_obj, 'cancel_order')
        p._platform_get_position = getattr(strategy_obj, 'get_position', None)
        p._platform_get_orders = getattr(strategy_obj, 'get_orders', None)
        p._runtime_market_center = p._extract_runtime_market_center(strategy_obj) or p._get_fallback_market_center()
        p.get_kline = None
        if p._runtime_market_center and callable(getattr(p._runtime_market_center, 'get_kline_data', None)):
            _raw_get_kline_data = p._runtime_market_center.get_kline_data

            def _compat_get_kline_data(exchange, instrument_id=None, instrument=None, style="M1", count=-1440, start_time=None, end_time=None, **kwargs):
                inst = instrument_id or instrument
                try:
                    return _raw_get_kline_data(exchange=exchange, instrument_id=inst, style=style, count=count, start_time=start_time, end_time=end_time, **kwargs)
                except (TypeError, Exception) as _e:
                    if 'Date Err' in str(_e) or 'Parsing' in str(_e) or isinstance(_e, TypeError):
                        return _raw_get_kline_data(exchange=exchange, instrument=inst, style=style, count=count)
                    raise

            p.get_kline = _compat_get_kline_data
        p._inject_runtime_context(strategy_obj)
        p._api_ready = callable(p.subscribe) and callable(p.unsubscribe)
        p._kline_ready = callable(p.get_kline)
        _data_service = None
        try:
            from ali2026v3_trading.data.data_service import get_data_service
            _data_service = get_data_service()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[bind_platform_apis] get_data_service failed: %s", e)
        if _data_service is not None and hasattr(p, '_state_store') and p._state_store is not None:
            p._state_store.set_ref('storage', _data_service)
            logging.info("[bind_platform_apis] storage injected into state_store")
        _storage = getattr(p, 'storage', None)
        if _storage is not None and hasattr(_storage, 'bind_platform_subscribe_api'):
            _storage.bind_platform_subscribe_api(p.subscribe, p.unsubscribe)
        if _storage is not None and hasattr(_storage, 'subscription_manager'):
            _sm = _storage.subscription_manager
            if _sm is not None:
                _bind_target = _data_service or _storage
                _bind_method = getattr(_sm, 'bind_data_manager', None)
                if callable(_bind_method):
                    _bind_method(_bind_target)
                elif hasattr(_sm, 'data_manager'):
                    _sm.data_manager = _bind_target
                    if hasattr(_sm, '_core_service'):
                        _sm._core_service.data_manager = _bind_target
                logging.info("[bind_platform_apis] SubscriptionManager.data_manager bound to %s", type(_bind_target).__name__)
        try:
            from ali2026v3_trading.data.data_service import DataService
            DataService.bind_subscribe_api(p.subscribe, p.unsubscribe)
            if _data_service is not None:
                _data_service.bind_data_manager(_data_service)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[StrategyLifecycleMixin] DataService.bind_subscribe_api failed: %s", e)
        if p._platform_insert_order:
            p._ensure_order_service()
            if p._order_service:
                p._order_service.bind_platform_apis(p._platform_insert_order, p._platform_cancel_order)
                p._ensure_check_pending_orders_job()
        if not p._api_ready:
            logging.warning(
                "[bind_platform_apis] 平台API未完全就绪: "
                f"subscribe={callable(p.subscribe)}, unsubscribe={callable(p.unsubscribe)}, 策略将以DEGRADED状态运行"
            )
            p.transition_to(StrategyState.DEGRADED)
        if not p._kline_ready:
            logging.warning(
                "[bind_platform_apis] 历史K线API不可用: "
                f"get_kline={callable(p.get_kline)}, 历史K线加载将跳过"
            )
        try:
            _apis_to_bind = {}
            if callable(getattr(p, 'subscribe', None)):
                _apis_to_bind['subscribe'] = p.subscribe
            if callable(getattr(p, 'unsubscribe', None)):
                _apis_to_bind['unsubscribe'] = p.unsubscribe
            if callable(getattr(p, 'get_kline', None)):
                _apis_to_bind['get_kline'] = p.get_kline
            if getattr(p, 'get_instrument', None):
                _apis_to_bind['get_instrument'] = p.get_instrument
            if _apis_to_bind:
                p._lifecycle_platform.bind_platform_apis(_apis_to_bind)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lp_err:
            logging.debug("[LifecyclePlatform] bind_platform_apis 委托失败: %s", _lp_err)

    @staticmethod
    def _extract_runtime_market_center(strategy_obj: Any) -> Any:
        if not strategy_obj:
            return None
        mc = getattr(strategy_obj, 'market_center', None)
        if mc:
            return mc
        infini = getattr(strategy_obj, 'infini', None)
        if infini:
            return getattr(infini, 'market_center', None)
        return None

    def _inject_runtime_context(self, strategy_obj: Any) -> None:
        p = self.p
        logging.info(f"[LifecycleBind._inject_runtime_context] called with strategy_obj={type(strategy_obj).__name__ if strategy_obj else 'None'}")
        if not strategy_obj:
            logging.warning("[LifecycleBind._inject_runtime_context] strategy_obj is None, returning")
            return
        params = getattr(p, 'params', None)
        if not params:
            logging.warning("[LifecycleBind._inject_runtime_context] params is None, returning")
            return
        mc = p._runtime_market_center
        if mc is None and getattr(p, '_fallback_market_center', None) is not None:
            mc = p._fallback_market_center
        try:
            if isinstance(params, dict):
                params['strategy'] = strategy_obj
                if mc:
                    params['market_center'] = mc
            else:
                setattr(params, 'strategy', strategy_obj)
                if mc:
                    setattr(params, 'market_center', mc)
            if isinstance(params, dict):
                params['strategy_instance'] = params['strategy']
            else:
                setattr(params, 'strategy_instance', params.strategy)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[Context] Inject failed: %s", e)
        try:
            from ali2026v3_trading.config.config_service import update_cached_params
            update_cached_params({'strategy': strategy_obj}, caller_id='_inject_runtime_context')
            logging.info(f"[LifecycleBind._inject_runtime_context] update_cached_params called with strategy={type(strategy_obj).__name__}")
        except Exception as e:
            logging.error("[Context] update_cached_params failed: %s", e)
        try:
            from ali2026v3_trading.infra.health_monitor import set_runtime_strategy_ref
            set_runtime_strategy_ref(strategy_obj)
            logging.info(f"[LifecycleBind._inject_runtime_context] set_runtime_strategy_ref called with strategy={type(strategy_obj).__name__}")
        except Exception as e:
            logging.error("[Context] set_runtime_strategy_ref failed: %s", e)

    def _get_fallback_market_center(self) -> Any:
        p = self.p
        if p._fallback_market_center:
            return p._fallback_market_center
        try:
            from pythongo.core import MarketCenter
        except ImportError:
            logging.warning("[DEP-04] pythongo.core.MarketCenter not available, using None fallback")
            return None
        # 平台规范：MarketCenter是K线数据的标准入口，必须创建实例
        try:
            mc = MarketCenter()
            p._fallback_market_center = mc
            logging.info("[Fallback] MarketCenter created successfully")
            return mc
        except Exception as e:
            logging.warning(f"[Fallback] MarketCenter creation failed: {e}, using None fallback")
            return None

    def _start_platform_subscribe_async(self, instrument_ids: List[str]) -> None:
        p = self.p
        targets = [str(x).strip() for x in (instrument_ids or []) if str(x).strip()]
        if not targets:
            return
        try:
            _lp_diag = p._lifecycle_platform
            logging.debug("[Subscribe] R-04 DIAG: p._lifecycle_platform accessible, type=%s, subscribed_count=%d", type(_lp_diag).__name__, len(_lp_diag.subscribed_instruments))
        except Exception as _diag_err:
            logging.error("[Subscribe] R-04 DIAG: p._lifecycle_platform NOT accessible! err_type=%s err=%s p_type=%s", type(_diag_err).__name__, _diag_err, type(p).__name__, exc_info=True)
        with p._platform_subscribe_lock:
            if p._platform_subscribe_thread and p._platform_subscribe_thread.is_alive():
                return
            p._platform_subscribe_stop.clear()
            thread = threading.Thread(
                target=p._platform_subscribe_worker,
                args=(targets,),
                name=f"PlatformSubscribe[strategy:{p.strategy_id}]",
                daemon=True
            )
            p._platform_subscribe_thread = thread
            thread.start()

    def _platform_subscribe_worker(self, instrument_ids: List[str]) -> None:
        import time as _sub_time
        p = self.p
        logging.debug("[Subscribe] R-04 DIAG: worker started, p_type=%s, p_has_lifecycle_svc=%s, p_has_lifecycle_platform=%s",
                     type(p).__name__,
                     hasattr(p, '_lifecycle_svc'),
                     hasattr(p, '_lifecycle_platform'))
        try:
            _lp_pre = p._lifecycle_platform
            logging.debug("[Subscribe] R-04 DIAG: p._lifecycle_platform pre-check OK, type=%s", type(_lp_pre).__name__)
        except Exception as _pre_err:
            logging.error("[Subscribe] R-04 DIAG: p._lifecycle_platform pre-check FAILED! err_type=%s err=%s", type(_pre_err).__name__, _pre_err, exc_info=True)
        registered = failed = 0
        total = len(instrument_ids)
        _batch_start = _sub_time.monotonic()
        _first_error_logged = False
        _BATCH_SIZE = 500
        _BATCH_PAUSE_SEC = 0.5
        _batch_num = 0
        for batch_start_idx in range(0, total, _BATCH_SIZE):
            if p._platform_subscribe_stop.is_set():
                break
            _batch_end_idx = min(batch_start_idx + _BATCH_SIZE, total)
            _batch_num += 1
            for i in range(batch_start_idx + 1, _batch_end_idx + 1):
                inst = instrument_ids[i - 1]
                if p._platform_subscribe_stop.is_set():
                    break
                try:
                    _lp = p._lifecycle_platform
                except Exception as _lp_attr_err:
                    failed += 1
                    if not _first_error_logged:
                        _first_error_logged = True
                        logging.error("[Subscribe] R-04 DIAG: p._lifecycle_platform attr access failed #%d inst=%s err_type=%s err=%s p_type=%s p_dict_keys=%s", failed, inst, type(_lp_attr_err).__name__, _lp_attr_err, type(p).__name__, list(p.__dict__.keys())[:20] if hasattr(p, "__dict__") else "N/A", exc_info=True)
                    elif failed <= 5 or failed % 1000 == 0:
                        logging.warning("[Subscribe] p._lifecycle_platform attr access failed #%d %s: %s", failed, inst, _lp_attr_err)
                    continue
                try:
                    _lp.subscribe_instrument(inst)
                    registered += 1
                except Exception as _lp_err:
                    failed += 1
                    if not _first_error_logged:
                        _first_error_logged = True
                        logging.error("[Subscribe] R-04 DIAG: subscribe_instrument failed #%d inst=%s err_type=%s err=%s _lp_type=%s", failed, inst, type(_lp_err).__name__, _lp_err, type(_lp).__name__, exc_info=True)
                    elif failed <= 5 or failed % 1000 == 0:
                        logging.warning("[Subscribe] lifecycle_platform.register failed #%d %s: %s", failed, inst, _lp_err)
            logging.info(f"[Subscribe] Batch {_batch_num}: instruments {batch_start_idx+1}-{_batch_end_idx}/{total}, registered={registered}, failed={failed}")
            if _batch_end_idx < total:
                _sub_time.sleep(_BATCH_PAUSE_SEC)
        _elapsed = _sub_time.monotonic() - _batch_start
        logging.info(f"[Subscribe] Done: registered={registered}, failed={failed}, total={total}, batches={_batch_num}, elapsed={_elapsed:.1f}s")
        p._platform_subscribe_completed.set()
        if failed > 0 and failed == total:
            logging.error("[Subscribe] 全部lifecycle登记失败(failed=%d)，但lifecycle登记仅为内部记录，不触发DEGRADED(平台订阅已在数据库登记阶段完成)", failed)
            logging.error("[Subscribe] R-04 ROOT CAUSE: p._lifecycle_platform属性访问或subscribe_instrument调用异常，详见上方R-04 DIAG日志")

    def _unsubscribe_all_instruments(self) -> None:
        p = self.p
        try:
            p._platform_subscribe_stop.set()
            if not callable(p.unsubscribe):
                return
            subscribed = list(getattr(p, '_subscribed_instruments', []) or [])
            if not subscribed:
                return
            success_count = 0
            failed_count = 0
            for inst in subscribed:
                try:
                    p.unsubscribe(inst)
                    success_count += 1
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    failed_count += 1
            logging.info(
                f"[Unsubscribe] Summary: total={len(subscribed)}, success={success_count}, failed={failed_count}"
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[StrategyCoreService._unsubscribe_all_instruments] Error: {e}", exc_info=True)

    def _start_historical_kline_load_async(self) -> None:
        p = self.p
        def _kline_worker():
            try:
                # FIX-20260703-CRASH: 等待批量订阅完成后再加载历史K线，避免并发C++ API调用导致StrategyLib.dll崩溃
                # 根因：sub_market_data(订阅线程)和get_kline_data(K线线程max_workers=4)并发调用
                #       触发STATUS_STACK_BUFFER_OVERRUN(0xc0000409) at offset 0xfa9db
                _bulk_thread = getattr(p, '_bulk_subscribe_thread', None)
                if _bulk_thread is not None and _bulk_thread.is_alive():
                    logging.info("[KlineLoadAsync] 等待批量订阅线程完成，避免并发C++ API调用导致DLL崩溃...")
                    _bulk_thread.join(timeout=120.0)
                    if _bulk_thread.is_alive():
                        logging.warning("[KlineLoadAsync] 批量订阅线程join超时(120s)，继续启动K线加载（风险：可能触发DLL崩溃）")
                    else:
                        logging.info("[KlineLoadAsync] 批量订阅线程已完成，开始加载历史K线")
                logging.info("[KlineLoadAsync] 后台历史K线加载开始...")
                p._start_historical_kline_load()
                logging.info("[KlineLoadAsync] 后台历史K线加载完成")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error(f"[KlineLoadAsync] 后台历史K线加载失败: {e}", exc_info=True)
        threading.Thread(
            target=_kline_worker,
            name=f"kline-load-async[strategy:{p.strategy_id}]",
            daemon=True
        ).start()
        logging.info("[KlineLoadAsync] 历史K线加载已调度到后台线程，onStart 不再阻塞")

    def _start_historical_kline_load(self, blocking: bool = False) -> None:
        p = self.p
        try:
            _kline_svc = getattr(p, '_kline_svc', None)
            if _kline_svc is not None and hasattr(_kline_svc, 'start_historical_kline_load'):
                _kline_svc.start_historical_kline_load(blocking=blocking)
            else:
                logging.info("[KlineLoad] _kline_svc not available, skipping historical kline load")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[KlineLoad] 历史K线加载失败: {e}", exc_info=True)

    def _shutdown_historical_services(self) -> None:
        p = self.p
        try:
            _kline_svc = getattr(p, '_kline_svc', None)
            if _kline_svc is not None and hasattr(_kline_svc, 'shutdown_historical_services'):
                _kline_svc.shutdown_historical_services()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[KlineShutdown] 历史K线服务关闭异常: %s", e)