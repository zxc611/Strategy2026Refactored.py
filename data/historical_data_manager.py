# MODULE_ID: M1-029
import logging
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.shared_utils import safe_int, safe_float, CHINA_TZ
from ali2026v3_trading.config.params_service import get_param_value as _get_param_value


class HistoricalDataManager:
    DEFAULT_RETRY_DELAYS = [10.0, 30.0, 60.0]
    DEFAULT_THREAD_JOIN_TIMEOUT = 300.0
    MAX_RETRIES = 3

    def __init__(self, state_store=None, callback_group=None):
        self._state_store = state_store
        self._callback_group = callback_group
        self._historical_load_in_progress = False
        self._historical_kline_result = None
        self._historical_kline_progress = None
        self._historical_load_started = False
        self._historical_loader_thread = None
        self._historical_loader_lock = threading.Lock()
        self._background_threads_lock = threading.Lock()
        self._background_threads = []
        self._historical_load_retry_count = 0
        self._historical_load_max_retries = self.MAX_RETRIES
        self._historical_provider_retry_delays = list(self.DEFAULT_RETRY_DELAYS)
        self._hkl_diag_emitted = False
        self._historical_stop_flag = False

    def _invoke_callback(self, name: str, *args, **kwargs):
        if self._callback_group is not None:
            registry = self._callback_group.get_registry(name)
            results = registry.invoke(*args, **kwargs)
            return results[0] if results else None
        return None

    def init_historical(self):
        self._historical_load_in_progress = False
        self._historical_kline_result = None
        self._historical_kline_progress = None
        self._historical_load_started = False
        self._historical_loader_thread = None
        self._historical_loader_lock = threading.Lock()
        self._background_threads_lock = threading.Lock()
        self._background_threads = []
        self._historical_load_retry_count = 0
        self._historical_load_max_retries = self.MAX_RETRIES
        self._historical_provider_retry_delays = list(self.DEFAULT_RETRY_DELAYS)
        self._hkl_diag_emitted = False
        self._historical_stop_flag = False

    def filter_historical_month_scope(self, instrument_ids: List[str]) -> Tuple[List[str], int, str]:
        params = self._state_store.get('params')
        min_year_month = str(
            _get_param_value(params, 'history_min_year_month', datetime.now(CHINA_TZ).strftime('%y%m'))
            or datetime.now(CHINA_TZ).strftime('%y%m')
        ).strip()

        if not re.fullmatch(r'\d{3,4}', min_year_month):
            min_year_month = datetime.now(CHINA_TZ).strftime('%y%m')

        filtered: List[str] = []
        removed_count = 0
        for instrument_id in instrument_ids:
            year_month = self._invoke_callback('_extract_contract_year_month', instrument_id)
            if year_month and year_month < min_year_month:
                removed_count += 1
                continue
            filtered.append(instrument_id)

        return filtered, removed_count, min_year_month

    def build_historical_instruments(self) -> List[str]:
        strategy_id = self._state_store.get('strategy_id')
        params = self._state_store.get('params')
        subscribed = list(self._state_store.get('_subscribed_instruments') or [])

        if not subscribed:
            return []

        subscribed, removed_by_month, min_year_month = self.filter_historical_month_scope(subscribed)
        if removed_by_month > 0:
            logging.warning(f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                            f"[source_type=historical-loader] Filtered by month: min={min_year_month}, removed={removed_by_month}")

        include_options = bool(_get_param_value(params, 'load_history_options', True))
        from ali2026v3_trading.infra.subscription_manager import SubscriptionManager
        future_count = 0
        option_count = 0
        instruments: List[str] = []

        for instrument_id in subscribed:
            is_option = SubscriptionManager.is_option(instrument_id)
            if is_option:
                option_count += 1
            else:
                future_count += 1

            if not include_options and is_option:
                continue
            instruments.append(instrument_id)

        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Historical instrument set: total_subscribed=%d, futures=%d, options=%d, include_options=%s, selected=%d",
            strategy_id,
            len(subscribed),
            future_count,
            option_count,
            include_options,
            len(instruments),
        )

        return instruments

    def resolve_historical_provider(self) -> Tuple[Any, str]:
        params = self._state_store.get('params')

        runtime_market_center = _get_param_value(params, 'market_center')
        if runtime_market_center is not None:
            return runtime_market_center, 'params'

        runtime_strategy = _get_param_value(params, 'strategy')
        if runtime_strategy is not None:
            strategy_mc = getattr(runtime_strategy, 'market_center', None)
            if strategy_mc is not None:
                return strategy_mc, 'strategy'
            if callable(getattr(runtime_strategy, 'get_kline_data', None)) or callable(getattr(runtime_strategy, 'get_kline', None)):
                return runtime_strategy, 'strategy'

        cached_mc = self._state_store.get('_runtime_market_center')
        if cached_mc is not None:
            return cached_mc, 'runtime'

        runtime_host = self._state_store.get('_runtime_strategy_host')
        if runtime_host is not None:
            host_mc = self._invoke_callback('_extract_runtime_market_center', runtime_host)
            if host_mc is not None:
                self._state_store.set('_runtime_market_center', host_mc)
                return host_mc, 'runtime_host'
            if callable(getattr(runtime_host, 'get_kline_data', None)) or callable(getattr(runtime_host, 'get_kline', None)):
                return runtime_host, 'runtime_host'

        get_kline_fn = self._state_store.get('get_kline')
        if callable(get_kline_fn):
            class _Provider:
                def __init__(self, fn): self.get_kline = fn
            return _Provider(get_kline_fn), 'get_kline'

        fallback = self._invoke_callback('_get_fallback_market_center')
        if fallback is not None:
            self._state_store.set('_runtime_market_center', fallback)
            return fallback, 'fallback'

        return None, 'unavailable'

    def load_historical_klines_once(self, instruments: List[str], provider: Any, provider_source: str) -> None:
        strategy_id = self._state_store.get('strategy_id')
        params = self._state_store.get('params')
        storage = self._state_store.get('storage')
        stats = self._state_store.get('_stats')
        e2e_counters = self._state_store.get('_e2e_counters')
        lock = self._state_store.get('_lock')

        history_minutes = int(_get_param_value(params, 'history_minutes', 1440) or 1440)
        kline_style = str(_get_param_value(params, 'kline_style', 'M1') or 'M1')
        configured_batch_size = max(1, int(_get_param_value(params, 'history_load_batch_size', 200) or 200))
        max_batch_size = max(1, int(_get_param_value(params, 'history_load_max_batch_size', 50) or 50))
        batch_size = min(configured_batch_size, max_batch_size)
        max_workers = max(1, int(_get_param_value(params, 'history_load_max_workers', 4) or 4))
        batch_delay = max(0.0, safe_float(_get_param_value(params, 'history_load_batch_delay_sec', 0.2)))
        request_delay = max(
            0.0,
            float(
                _get_param_value(
                    params,
                    'history_load_request_delay_sec',
                    max(0.1, min(batch_delay, 0.15)),
                )
                or 0.0
            ),
        )

        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Start: instruments=%d, history_minutes=%d, provider=%s, configured_batch_size=%d, effective_batch_size=%d, batch_delay=%.3fs, request_delay=%.3fs",
            strategy_id,
            len(instruments),
            history_minutes,
            provider_source,
            configured_batch_size,
            batch_size,
            batch_delay,
            request_delay,
        )

        mgr_self = self

        def _on_progress(progress: Dict[str, Any]) -> None:
            mgr_self._historical_kline_progress = dict(progress or {})

        from ali2026v3_trading.strategy.strategy_historical import load_historical_klines_with_stop
        result = load_historical_klines_with_stop(
            storage,
            instruments=instruments,
            history_minutes=history_minutes,
            kline_style=kline_style,
            market_center=provider,
            batch_size=batch_size,
            inter_batch_delay_sec=batch_delay,
            request_delay_sec=request_delay,
            max_workers=max_workers,
            progress_callback=_on_progress,
            stop_check=lambda: bool(
                mgr_self._historical_stop_flag
                or self._state_store.get('_destroyed', False)
                or self._state_store.get('_stop_requested', False)
                or not self._state_store.get('_is_running', False)
            ),
        )

        with self._historical_loader_lock:
            self._historical_kline_result = dict(result or {})
            self._historical_kline_progress = dict(result or {})
        enqueued = safe_int(result.get('enqueued_klines', 0) or result.get('total_klines', 0))
        persisted = safe_int(result.get('persisted_klines', 0))

        kline_instruments = result.get('kline_instruments', []) if result else []
        if kline_instruments:
            if storage is not None and hasattr(storage, 'subscription_manager'):
                sm = storage.subscription_manager
                if sm and hasattr(sm, 'record_kline_received'):
                    for inst_id in kline_instruments:
                        sm.record_kline_received(inst_id)

        with self._historical_loader_lock:
            stats['total_klines'] = stats.get('total_klines', 0) + enqueued
            stats['total_enqueued_klines'] = stats.get('total_enqueued_klines', 0) + enqueued
            stats['total_persisted_klines'] = stats.get('total_persisted_klines', 0) + persisted
        if e2e_counters is not None and persisted > 0:
            with lock:
                e2e_counters['kline_persisted_count'] += persisted
        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Done: success=%d, failed=%d, enqueued=%d, persisted=%d",
            strategy_id,
            result.get('success', 0),
            result.get('failed', 0),
            enqueued,
            persisted,
        )

        self.notify_historical_kline_loaded(result or {})

    def notify_historical_kline_loaded(self, result: Dict[str, Any]) -> None:
        strategy_id = self._state_store.get('strategy_id')
        success = result.get('success', 0)
        failed = result.get('failed', 0)
        total_klines = result.get('enqueued_klines', 0) or result.get('total_klines', 0)
        persisted = result.get('persisted_klines', 0)
        kline_instruments = result.get('kline_instruments', [])

        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Historical kline load complete notification: success=%d, failed=%d, klines=%d, persisted=%d, instruments=%d",
            strategy_id, success, failed, total_klines, persisted,
            len(kline_instruments) if kline_instruments else 0,
        )

        try:
            self._invoke_callback('_publish_event', 'HistoricalKlineLoaded', {
                'success': success,
                'failed': failed,
                'total_klines': total_klines,
                'persisted_klines': persisted,
                'kline_instruments_count': len(kline_instruments) if kline_instruments else 0,
            })
        except Exception as e:
            logging.debug(
                "[HKL][strategy_id=%s] Failed to publish HistoricalKlineLoaded event: %s",
                strategy_id, e,
            )

    def start_historical_kline_load(self, blocking: bool = False) -> None:
        strategy_id = self._state_store.get('strategy_id')
        params = self._state_store.get('params')
        storage = self._state_store.get('storage')

        # 平台规范：auto_load_history默认值为True（与config_params.py和lifecycle_callbacks.py一致）
        auto_load = bool(_get_param_value(params, 'auto_load_history', True))
        if not auto_load:
            logging.info(f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                         f"[source_type=historical-loader] auto_load_history=False, skip")
            return

        instruments = self.build_historical_instruments()
        if not instruments:
            logging.info(f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                         f"[source_type=historical-loader] No subscribed instruments, skip")
            return

        provider, provider_source = self.resolve_historical_provider()
        if provider is None:
            with self._historical_loader_lock:
                if self._historical_load_retry_count < self._historical_load_max_retries:
                    delay = self._historical_provider_retry_delays[
                        min(self._historical_load_retry_count, len(self._historical_provider_retry_delays) - 1)
                    ]
                    self._historical_load_retry_count += 1
                    retry_remaining = self._historical_load_max_retries - self._historical_load_retry_count
                    logging.warning(
                        f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                        f"[source_type=historical-loader] Provider not ready, scheduling retry "
                        f"{self._historical_load_retry_count}/{self._historical_load_max_retries} "
                        f"in {delay:.0f}s (remaining={retry_remaining})"
                    )
                    mgr_self = self

                    def _retry_after_delay():
                        time.sleep(delay)
                        try:
                            mgr_self.start_historical_kline_load()
                        except Exception as e:
                            logging.error(f"[HKL] Provider retry failed: {e}", exc_info=True)
                    t = threading.Thread(target=_retry_after_delay, name=f"hkl-retry-{strategy_id}", daemon=True)
                    with self._background_threads_lock:
                        self._background_threads.append(t)
                    t.start()
                else:
                    logging.error(
                        f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                        f"[source_type=historical-loader] Provider not ready after {self._historical_load_max_retries} retries, giving up"
                    )
            return

        with self._historical_loader_lock:
            if self._historical_load_started and self._historical_kline_result is not None:
                result = self._historical_kline_result
                if result.get('success', 0) > 0 or result.get('total_klines', 0) > 0:
                    return
            self._historical_load_started = True
            self._historical_load_in_progress = True
            self._historical_kline_result = None
            self._historical_kline_progress = None
            self._historical_stop_flag = False

        mgr_self = self

        def _runner() -> None:
            _instruments = instruments
            try:
                if storage is not None:
                    with storage._ext_kline_lock:
                        storage._ext_kline_load_in_progress = True
                logging.info(
                    "[HKL][strategy_id=%s] 预注册已由on_init保证完成，直接加载历史K线: %d 个合约",
                    strategy_id, len(_instruments)
                )

                mgr_self.load_historical_klines_once(_instruments, provider, provider_source)
            except Exception as e:
                logging.error(
                    f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                    f"[source_type=historical-loader] Load failed: {e}", exc_info=True
                )
                with mgr_self._historical_loader_lock:
                    if mgr_self._historical_load_retry_count < mgr_self._historical_load_max_retries:
                        mgr_self._historical_load_started = False
                        logging.info(
                            "[HKL][strategy_id=%s] Load failed, will allow retry (%d/%d)",
                            strategy_id, mgr_self._historical_load_retry_count, mgr_self._historical_load_max_retries
                        )
            finally:
                with mgr_self._historical_loader_lock:
                    mgr_self._historical_load_in_progress = False
                    mgr_self._historical_kline_progress = None
                if storage is not None:
                    with storage._ext_kline_lock:
                        storage._ext_kline_load_in_progress = False
                    try:
                        storage.close_connection()
                    except Exception as e:
                        logging.debug(f"[{strategy_id}] Failed to close connection: {e}")

        thread = threading.Thread(target=_runner, name=f"hkl-{strategy_id}", daemon=True)
        self._historical_loader_thread = thread
        with self._background_threads_lock:
            self._background_threads.append(thread)
        thread.start()
        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Loader thread started (blocking=%s)",
            strategy_id, blocking,
        )

        if blocking:
            logging.info(
                "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
                "Blocking until load completes (max 300s)...",
                strategy_id,
            )
            thread.join(timeout=self.DEFAULT_THREAD_JOIN_TIMEOUT)
            if thread.is_alive():
                logging.warning(
                    "[HKL][strategy_id=%s] Historical K-line load still running after %.1fs, "
                    "proceeding non-blocking (load continues in background)",
                    strategy_id, self.DEFAULT_THREAD_JOIN_TIMEOUT,
                )
            else:
                logging.info(
                    "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
                    "Load completed, resuming initialization.",
                    strategy_id,
                )

    def shutdown_historical_services(self) -> None:
        strategy_id = self._state_store.get('strategy_id')
        with self._historical_loader_lock:
            self._historical_stop_flag = True
            self._historical_load_started = False
            loader_thread = self._historical_loader_thread
            self._historical_loader_thread = None

        if loader_thread and loader_thread.is_alive():
            logging.info(
                f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                f"[source_type=historical-loader] Waiting for loader thread to exit (max 10s)"
            )
            deadline = time.time() + 10.0
            while loader_thread.is_alive() and time.time() < deadline:
                loader_thread.join(timeout=0.5)

            if loader_thread.is_alive():
                logging.warning(
                    f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                    f"[source_type=historical-loader] Loader thread still alive after 10s (daemon=True, will exit with process)"
                )
            else:
                logging.info(
                    f"[HKL][strategy_id={strategy_id}][owner_scope=strategy-instance]"
                    f"[source_type=historical-loader] Loader thread exited cleanly"
                )

        with self._background_threads_lock:
            threads_to_join = list(self._background_threads)
            self._background_threads.clear()

        alive_threads = []
        for t in threads_to_join:
            if t.is_alive():
                t.join(timeout=3.0)
                if t.is_alive():
                    alive_threads.append(t.name)

        if alive_threads:
            logging.info(
                f"[HKL][strategy_id={strategy_id}] {len(alive_threads)} daemon threads still alive "
                f"after join timeout (will exit with process): {alive_threads[:5]}"
            )

    def reset_historical_state_for_restart(self) -> None:
        with self._historical_loader_lock:
            self._historical_stop_flag = False
            self._historical_load_started = False
            self._historical_loader_thread = None
            self._historical_load_retry_count = 0

    def emit_historical_kline_diagnostic_on_first_tick(self) -> None:
        strategy_id = self._state_store.get('strategy_id')
        stats = self._state_store.get('_stats')
        state = self._state_store.get('_state')
        is_running = self._state_store.get('_is_running')
        is_paused = self._state_store.get('_is_paused')
        subscribed_instruments = self._state_store.get('_subscribed_instruments')

        if not self._hkl_diag_emitted:
            self._hkl_diag_emitted = True
            historical_load_started = self._historical_load_started

            historical_result = self._historical_kline_result or {}
            historical_progress = self._historical_kline_progress or {}
            current_total_klines = safe_int(stats.get('total_klines', 0))
            if self._historical_load_in_progress:
                current_total_klines += safe_int(historical_progress.get('enqueued_klines', 0) or historical_progress.get('total_klines', 0))

            if self._historical_load_in_progress:
                kline_summary_line = f"K 线数据：总计 {current_total_klines:,} 条（历史加载进行中）"
            else:
                kline_summary_line = f"K 线数据：总计 {current_total_klines:,} 条"

            historical_detail_line = ""
            progress_source = historical_progress if self._historical_load_in_progress and historical_progress else historical_result
            if progress_source:
                historical_detail_line = (
                    f"历史K线加载{'进度' if self._historical_load_in_progress else '结果'}：抓取 {safe_int(progress_source.get('fetched_klines', 0)):,} 条，"
                    f"入队 {safe_int(progress_source.get('enqueued_klines', 0) or progress_source.get('total_klines', 0)):,} 条，"
                    f"成功 {safe_int(progress_source.get('success', 0)):,}，"
                    f"失败 {safe_int(progress_source.get('failed', 0)):,}"
                )
                if self._historical_load_in_progress and progress_source.get('batch_index') and progress_source.get('total_batches'):
                    historical_detail_line += f"，批次 {int(progress_source.get('batch_index', 0))}/{int(progress_source.get('total_batches', 0))}"

            logging.info(
                "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
                "First tick diagnostics: "
                "state=%s, is_running=%s, is_paused=%s, "
                "historical_started=%s, "
                "subscribed_count=%d, "
                "has_market_data_service=False",
                strategy_id,
                state, is_running, is_paused,
                historical_load_started,
                len(subscribed_instruments) if subscribed_instruments else 0,
            )

    def check_and_start_historical_load_on_tick(self) -> None:
        strategy_id = self._state_store.get('strategy_id')
        with self._historical_loader_lock:
            if self._historical_load_started and self._historical_kline_result is not None:
                return
            if self._historical_load_in_progress:
                return

            if self._historical_load_started:
                if self._historical_kline_result is not None:
                    result = self._historical_kline_result
                    if result.get('success', 0) > 0 or result.get('total_klines', 0) > 0:
                        return
                if self._historical_load_retry_count >= self._historical_load_max_retries:
                    return
                if self._historical_load_in_progress:
                    return
                logging.info(
                    "[HKL][strategy_id=%s] Previous load failed, allowing tick-triggered retry (%d/%d)",
                    strategy_id, self._historical_load_retry_count, self._historical_load_max_retries
                )
                self._historical_load_started = False
            self._historical_load_started = True

        mgr_self = self

        def _async_kline_load():
            try:
                mgr_self.start_historical_kline_load()
            except Exception as e:
                logging.error(
                    "[HistoricalLoadTick][strategy_id=%s][owner_scope=strategy-instance]"
                    "[source_type=historical-fallback] 异步K线加载失败: %s",
                    strategy_id, e, exc_info=True
                )
        t = threading.Thread(
            target=_async_kline_load,
            name=f"kline-load-tick-fallback-{strategy_id}",
            daemon=True
        )
        with self._background_threads_lock:
            self._background_threads.append(t)
        t.start()

    def get_historical_kline_summary_lines(self) -> Tuple[str, str]:
        stats = self._state_store.get('_stats')
        historical_result = self._historical_kline_result or {}
        if self._historical_load_in_progress:
            kline_summary_line = "K 线数据：历史加载进行中，完成后更新累计条数"
        else:
            kline_summary_line = f"K 线数据：总计 {stats['total_klines']:,} 条"

        historical_detail_line = ""
        if historical_result:
            historical_detail_line = (
                f"历史K线加载结果：抓取 {safe_int(historical_result.get('fetched_klines', 0)):,} 条，"
                f"入队 {safe_int(historical_result.get('enqueued_klines', 0) or historical_result.get('total_klines', 0)):,} 条，"
                f"成功 {safe_int(historical_result.get('success', 0)):,}，"
                f"失败 {safe_int(historical_result.get('failed', 0)):,}"
            )

        return kline_summary_line, historical_detail_line
