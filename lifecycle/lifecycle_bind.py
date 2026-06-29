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
            def _subscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
                _sub_call_counter[0] += 1
                suffix = instrument_id[6:] if len(instrument_id) > 6 else ''
                if _sub_call_counter[0] <= 10 or (exchange == 'SHFE' and ('C' in suffix or 'P' in suffix)):
                    logging.info(f"[PROBE_SUB] #{_sub_call_counter[0]} exchange={exchange} instrument_id={instrument_id}")
                sub(exchange, instrument_id)
            p.subscribe = _subscribe
        else:
            p.subscribe = None
        if callable(unsub):
            def _unsubscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
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
            p.get_kline = p._runtime_market_center.get_kline_data
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
        if not strategy_obj:
            return
        params = getattr(p, 'params', None)
        if not params:
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
        except Exception as e:
            logging.error("[Context] update_cached_params failed: %s", e)
        try:
            from ali2026v3_trading.infra.health_monitor import set_runtime_strategy_ref
            set_runtime_strategy_ref(strategy_obj)
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
        p = self.p
        success = failed = 0
        total = len(instrument_ids)
        subscribe_fn = None
        if callable(getattr(p, 'subscribe', None)):
            subscribe_fn = p.subscribe
        else:
            logging.error("[Subscribe] self.subscribe不可用，无法订阅")
        for i, inst in enumerate(instrument_ids, 1):
            if p._platform_subscribe_stop.is_set():
                break
            try:
                if subscribe_fn:
                    subscribe_fn(inst)
                    success += 1
                else:
                    failed += 1
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                failed += 1
                logging.warning(f"[Subscribe] Failed {inst}: {e}")
            if i % 500 == 0 or i == total:
                logging.info(f"[Subscribe] Progress {i}/{total}, ok={success}, fail={failed}")
        logging.info(f"[Subscribe] Done: ok={success}, fail={failed}, total={total}")
        p._platform_subscribe_completed.set()
        try:
            for inst in instrument_ids:
                p._lifecycle_platform.subscribe_instrument(inst)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lp_err:
            logging.debug("[LifecyclePlatform] subscribe_instrument 委托失败: %s", _lp_err)
        if failed > 0 and failed == total:
            logging.error("[Subscribe] 全部订阅失败，策略进入DEGRADED状态")
            p.transition_to(StrategyState.DEGRADED)

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