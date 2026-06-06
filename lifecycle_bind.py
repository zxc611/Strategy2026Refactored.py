"""lifecycle_bind.py — 平台API绑定/订阅逻辑（从strategy_lifecycle_mixin.py拆分）
职责: bind_platform_apis, 平台订阅/退订, 市场中心提取, 运行时上下文注入, 历史K线异步加载
"""
from __future__ import annotations

import threading
import logging
from typing import Any, Dict, List, Optional

from ali2026v3_trading.params_service import _read_param
from ali2026v3_trading.lifecycle_state import StrategyState


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
        from ali2026v3_trading.config_service import resolve_product_exchange
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
        p._platform_insert_order = _read_param(strategy_obj, 'insert_order') or _read_param(strategy_obj, 'send_order')
        p._platform_cancel_order = _read_param(strategy_obj, 'cancel_order') or _read_param(strategy_obj, 'cancel_order_ref')
        p._platform_get_position = getattr(strategy_obj, 'get_position', None)
        p._platform_get_orders = getattr(strategy_obj, 'get_orders', None)
        p._runtime_market_center = p._extract_runtime_market_center(strategy_obj) or p._get_fallback_market_center()
        p.get_kline = None
        if p._runtime_market_center and callable(getattr(p._runtime_market_center, 'get_kline_data', None)):
            p.get_kline = p._runtime_market_center.get_kline_data
        p._inject_runtime_context(strategy_obj)
        p._api_ready = callable(p.subscribe) and callable(p.unsubscribe)
        p._kline_ready = callable(p.get_kline)
        if hasattr(p, 'storage') and p.storage is not None:
            p.storage.bind_platform_subscribe_api(p.subscribe, p.unsubscribe)
        try:
            from ali2026v3_trading.data_service import DataService
            DataService.bind_subscribe_api(p.subscribe, p.unsubscribe)
        except Exception as e:
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
        except Exception as _lp_err:
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
        mc = p._runtime_market_center or p._get_fallback_market_center()
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
        except Exception as e:
            logging.warning(f"[Context] Inject failed: {e}")

    def _get_fallback_market_center(self) -> Any:
        p = self.p
        if p._fallback_market_center:
            return p._fallback_market_center
        try:
            from pythongo.core import MarketCenter
        except ImportError:
            MarketCenter = None
            logging.warning("[DEP-04] pythongo.core.MarketCenter not available, using None fallback")
        if MarketCenter is None:
            return None
        try:
            p._fallback_market_center = MarketCenter()
            return p._fallback_market_center
        except Exception as e:
            logging.warning(f"[Fallback] Create failed: {e}")
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
            except Exception as e:
                failed += 1
                logging.warning(f"[Subscribe] Failed {inst}: {e}")
            if i % 500 == 0 or i == total:
                logging.info(f"[Subscribe] Progress {i}/{total}, ok={success}, fail={failed}")
        logging.info(f"[Subscribe] Done: ok={success}, fail={failed}, total={total}")
        p._platform_subscribe_completed.set()
        try:
            for inst in instrument_ids:
                p._lifecycle_platform.subscribe_instrument(inst)
        except Exception as _lp_err:
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
                except Exception:
                    failed_count += 1
            logging.info(
                f"[Unsubscribe] Summary: total={len(subscribed)}, success={success_count}, failed={failed_count}"
            )
        except Exception as e:
            logging.error(f"[StrategyCoreService._unsubscribe_all_instruments] Error: {e}", exc_info=True)

    def _start_historical_kline_load_async(self) -> None:
        p = self.p
        def _kline_worker():
            try:
                logging.info("[KlineLoadAsync] 后台历史K线加载开始...")
                p._start_historical_kline_load()
                logging.info("[KlineLoadAsync] 后台历史K线加载完成")
            except Exception as e:
                logging.error(f"[KlineLoadAsync] 后台历史K线加载失败: {e}", exc_info=True)
        threading.Thread(
            target=_kline_worker,
            name=f"kline-load-async[strategy:{p.strategy_id}]",
            daemon=True
        ).start()
        logging.info("[KlineLoadAsync] 历史K线加载已调度到后台线程，onStart 不再阻塞")