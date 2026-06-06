"""
历史K线加载Mixin模块

[ACTIVE] 此模块通过 HistoricalKlineMixin 被 StrategyCoreService 继承使用，是当前主链路的组成部分。
    StrategyCoreService(HistoricalKlineMixin, TickHandlerMixin) 是唯一权威链路。

职责：
- 管理历史K线加载的启动、执行和进度跟踪
- 提供合约过滤和提供者解析功能
- 维护历史加载状态和统计信息

设计原则：
- 单一职责：仅处理历史K线相关逻辑
- 无状态依赖：通过self访问所需属性
- 接口稳定：保持与StrategyCoreService的兼容性
"""

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.shared_utils import safe_int, safe_float, CHINA_TZ


def load_historical_klines_with_stop(
    storage,
    instruments: List[str],
    history_minutes: int = 1440,
    kline_style: str = 'M1',
    market_center: Any = None,
    batch_size: Optional[int] = None,
    inter_batch_delay_sec: float = 0.0,
    request_delay_sec: float = 0.0,
    max_workers: int = 4,
    progress_callback=None,
    stop_check=None,
) -> Dict[str, int]:
    """在历史加载 mixin 内实现可取消加载，避免继续扩张 storage.py。"""
    resolved_provider, provider_type = storage._resolve_kline_provider(market_center)
    if not resolved_provider:
        logging.debug("[Storage] 历史K线提供者不可用，无法加载历史K线")
        return {
            'success': 0,
            'failed': len(instruments),
            'total_klines': 0,
            'fetched_klines': 0,
            'enqueued_klines': 0,
            'persisted_klines': 0,
            'queue_received_delta': 0,
            'queue_written_delta': 0,
        }

    def _should_stop() -> bool:
        try:
            return bool(stop_check and stop_check())
        except Exception as exc:
            logging.debug("[Storage] 历史K线 stop_check 失败: %s", exc)
            return False

    end_time = datetime.now(CHINA_TZ)
    start_time = end_time - timedelta(minutes=history_minutes)

    logging.info(
        "[Storage] 开始为 %d 个合约加载历史K线: %s -> %s, 周期=%s, provider=%s",
        len(instruments), start_time, end_time, kline_style, provider_type,
    )

    success_count = 0
    failed_count = 0
    total_klines = 0
    fetched_klines = 0
    kline_success_instruments = []  # 分子：平台返回K线的合约列表
    normalized_period = storage._normalize_kline_period(kline_style)
    effective_batch_size = max(1, int(batch_size or len(instruments) or 1))
    total_batches = (len(instruments) + effective_batch_size - 1) // effective_batch_size if instruments else 0

    for batch_index, start in enumerate(range(0, len(instruments), effective_batch_size), start=1):
        if _should_stop():
            logging.info("[Storage] 历史K线加载收到停止信号，批次开始前退出")
            break

        batch_instruments = instruments[start:start + effective_batch_size]
        batch_enqueued_klines = 0
        logging.info(
            "[Storage] 历史K线批次 %d/%d 开始: batch_size=%d, max_workers=%d, request_delay=%.3fs, inter_batch_delay=%.3fs",
            batch_index, total_batches, len(batch_instruments), max_workers, request_delay_sec, inter_batch_delay_sec,
        )

        # 线程安全计数器和结果收集
        _counter_lock = threading.Lock()
        _stopped = [False]  # 使用列表包装以便在嵌套函数中修改

        def _load_single(instrument_str: str) -> Optional[Dict[str, Any]]:
            """单个合约的历史K线加载（线程安全）"""
            if _stopped[0] or _should_stop():
                return None
            try:
                if '.' in instrument_str:
                    exchange, instrument_id = instrument_str.split('.', 1)
                else:
                    exchange = None
                    instrument_id = instrument_str

                normalized_id = str(instrument_id or '').strip()
                info = storage._get_instrument_info(normalized_id)
                if info is None:
                    warn_key = ('load_historical_klines', normalized_id)
                    with storage._lock:
                        if warn_key not in storage._runtime_missing_warned:
                            storage._runtime_missing_warned.add(warn_key)
                            logging.warning("[load_historical_klines] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
                    return {'failed': True}

                if not exchange:
                    exchange = storage.infer_exchange_from_id(instrument_id)

                kline_data = storage._fetch_historical_kline_data(
                    resolved_provider, exchange, instrument_id, kline_style, history_minutes, start_time, end_time,
                )

                if _stopped[0] or _should_stop():
                    return None

                if kline_data and len(kline_data) > 0:
                    normalized_klines = []
                    for kline in kline_data:
                        try:
                            ts = storage._to_timestamp(getattr(kline, 'timestamp', getattr(kline, 'ts', time.time())))
                            if ts is None:
                                raise ValueError('invalid historical kline timestamp')
                            normalized_klines.append({
                                'ts': ts,
                                'instrument_id': instrument_id,
                                'exchange': exchange,
                                'open': getattr(kline, 'open', getattr(kline, 'Open', 0.0)),
                                'high': getattr(kline, 'high', getattr(kline, 'High', 0.0)),
                                'low': getattr(kline, 'low', getattr(kline, 'Low', 0.0)),
                                'close': getattr(kline, 'close', getattr(kline, 'Close', 0.0)),
                                'volume': getattr(kline, 'volume', getattr(kline, 'Volume', 0)),
                                'open_interest': getattr(kline, 'open_interest', getattr(kline, 'OpenInterest', 0)),
                                'period': normalized_period,
                            })
                        except Exception as exc:
                            logging.warning(f"[Storage] 保存K线失败 {instrument_id}: {exc}")

                    if not normalized_klines:
                        return {'failed': True, 'instrument_id': instrument_id}

                    storage._wait_for_queue_capacity(
                        max_fill_rate=60.0,
                        timeout_sec=max(5.0, inter_batch_delay_sec * 20 if inter_batch_delay_sec > 0 else 5.0),
                        source='load_historical_klines',
                    )

                    internal_id = storage._get_info_internal_id(info)
                    instrument_type = info.get('type', 'future')
                    enqueue_failed = False
                    for chunk_start in range(0, len(normalized_klines), storage.batch_size):
                        if _stopped[0] or _should_stop():
                            enqueue_failed = True
                            break
                        chunk = normalized_klines[chunk_start:chunk_start + storage.batch_size]
                        if not storage._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, normalized_period):
                            enqueue_failed = True
                            break

                    if enqueue_failed:
                        logging.warning(f"[Storage] {instrument_id}: 历史K线入队失败")
                        return {'failed': True, 'instrument_id': instrument_id}

                    with storage._ext_kline_lock:
                        storage._last_ext_kline[(instrument_id, normalized_period)] = normalized_klines[-1]['ts']

                    saved_count = len(normalized_klines)
                    logging.info(f"[Storage] ✅ {instrument_id}: 已批量入队 {saved_count} 条K线")
                    return {
                        'success': True,
                        'instrument_id': instrument_id,
                        'fetched': len(kline_data),
                        'enqueued': saved_count,
                    }
                else:
                    logging.info(f"[Storage] {instrument_id}: 无历史K线数据")
                    return {'failed': True, 'instrument_id': instrument_id, 'reason': 'no_data'}
            except Exception as exc:
                logging.error(f"[Storage] 加载历史K线失败 {instrument_str}: {exc}")
                return {'failed': True, 'instrument_id': instrument_str, 'error': str(exc)}

        # 使用线程池并行加载批次内合约
        if max_workers > 1:
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"hkl-batch{batch_index}") as executor:
                futures = {executor.submit(_load_single, inst): inst for inst in batch_instruments}
                for future in as_completed(futures):
                    if _should_stop():
                        _stopped[0] = True
                        break
                    result = future.result()
                    if result is None:
                        continue
                    if result.get('success'):
                        with _counter_lock:
                            success_count += 1
                            fetched_klines += result['fetched']
                            total_klines += result['enqueued']
                            batch_enqueued_klines += result['enqueued']
                            kline_success_instruments.append(result['instrument_id'])
                    elif result.get('failed'):
                        with _counter_lock:
                            failed_count += 1
        else:
            # 单线程模式（max_workers=1），保持原有串行逻辑
            for instrument_str in batch_instruments:
                if _should_stop():
                    break
                result = _load_single(instrument_str)
                if result is None:
                    continue
                if result.get('success'):
                    success_count += 1
                    fetched_klines += result['fetched']
                    total_klines += result['enqueued']
                    batch_enqueued_klines += result['enqueued']
                    kline_success_instruments.append(result['instrument_id'])
                elif result.get('failed'):
                    failed_count += 1
                # request_delay 仅在串行模式下生效
                if request_delay_sec > 0:
                    if _should_stop() or storage._stop_event.wait(request_delay_sec):
                        logging.info("[Storage] 历史K线加载在请求间隔中断")
                        break

        logging.info(
            "[Storage] 历史K线批次 %d/%d 完成: success=%d, failed=%d, fetched_klines=%d, enqueued_klines=%d",
            batch_index, total_batches, success_count, failed_count, fetched_klines, batch_enqueued_klines,
        )

        if progress_callback:
            try:
                progress_callback({
                    'success': success_count,
                    'failed': failed_count,
                    'total_klines': total_klines,
                    'fetched_klines': fetched_klines,
                    'enqueued_klines': batch_enqueued_klines,
                    'batch_index': batch_index,
                    'total_batches': total_batches,
                })
            except Exception as exc:
                logging.debug(f"[Storage] 历史K线进度回调失败: {exc}")

        if inter_batch_delay_sec > 0 and batch_index < total_batches:
            if _should_stop() or storage._stop_event.wait(inter_batch_delay_sec):
                logging.info("[Storage] 历史K线加载在批次间隔中断")
                break

    queue_stats_after = storage.get_queue_stats()
    queue_received_delta = queue_stats_after.get('total_received', 0)
    queue_written_delta = queue_stats_after.get('total_written', 0)
    drops_delta = queue_stats_after.get('drops_count', 0)
    actual_enqueued = max(0, total_klines - drops_delta)
    logging.info(f"[Storage] 历史K线加载完成: 成功={success_count}, 失败={failed_count}, " f"抓取K线={fetched_klines} 条, 入队K线={actual_enqueued} 条(原始={total_klines}, 丢弃={drops_delta}), " f"落盘K线={queue_written_delta} 条")

    return {
        'success': success_count,
        'failed': failed_count,
        'total_klines': total_klines,
        'fetched_klines': fetched_klines,
        'enqueued_klines': actual_enqueued,
        'persisted_klines': queue_written_delta,
        'queue_received_delta': queue_received_delta,
        'queue_written_delta': queue_written_delta,
        'kline_instruments': kline_success_instruments,  # 分子：平台返回K线的合约列表
    }


from ali2026v3_trading.params_service import get_param_value as _get_param_value

_HISTORICAL_MGR_OWN_ATTRS = frozenset([
    '_historical_load_in_progress', '_historical_kline_result',
    '_historical_kline_progress', '_historical_load_started',
    '_historical_loader_thread', '_historical_loader_lock',
    '_background_threads_lock', '_background_threads',
    '_historical_load_retry_count', '_historical_load_max_retries',
    '_historical_provider_retry_delays', '_hkl_diag_emitted',
    '_historical_stop_flag',
])


class HistoricalKlineMixin:
    """历史K线加载Mixin
    
    提供完整的历史K线加载功能，包括：
    - 合约范围过滤
    - 数据提供者解析
    - 异步加载管理
    - 进度跟踪和诊断
    """
    
    DEFAULT_RETRY_DELAYS = [10.0, 30.0, 60.0]
    DEFAULT_THREAD_JOIN_TIMEOUT = 300.0
    MAX_RETRIES = 3

    def __getattr__(self, name):
        if name in _HISTORICAL_MGR_OWN_ATTRS:
            mgr = self.__dict__.get('_historical_mgr')
            if mgr is not None:
                return getattr(mgr, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    
    def _init_historical_kline_mixin(self) -> None:
        if not hasattr(self, '_historical_mgr'):
            from ali2026v3_trading.historical_data_manager import HistoricalDataManager
            from ali2026v3_trading.state_store import get_state_store
            from ali2026v3_trading.callback_registry import get_callback_group
            state_store = get_state_store()
            callback_group = get_callback_group()
            self._historical_mgr = HistoricalDataManager(
                state_store=state_store,
                callback_group=callback_group,
            )
        self._historical_mgr.init_historical()

    def _filter_historical_month_scope(self, instrument_ids: List[str]) -> Tuple[List[str], int, str]:
        return self._historical_mgr.filter_historical_month_scope(instrument_ids)

    def _build_historical_instruments(self) -> List[str]:
        return self._historical_mgr.build_historical_instruments()

    def _resolve_historical_provider(self) -> Tuple[Any, str]:
        return self._historical_mgr.resolve_historical_provider()

    def _load_historical_klines_once(self, instruments: List[str], provider: Any, provider_source: str) -> None:
        return self._historical_mgr.load_historical_klines_once(instruments, provider, provider_source)

    def _notify_historical_kline_loaded(self, result: Dict[str, Any]) -> None:
        return self._historical_mgr.notify_historical_kline_loaded(result)

    def _start_historical_kline_load(self, blocking: bool = False) -> None:
        return self._historical_mgr.start_historical_kline_load(blocking=blocking)

    def _shutdown_historical_services(self) -> None:
        return self._historical_mgr.shutdown_historical_services()

    def _reset_historical_state_for_restart(self) -> None:
        return self._historical_mgr.reset_historical_state_for_restart()

    def _emit_historical_kline_diagnostic_on_first_tick(self) -> None:
        return self._historical_mgr.emit_historical_kline_diagnostic_on_first_tick()

    def _check_and_start_historical_load_on_tick(self) -> None:
        return self._historical_mgr.check_and_start_historical_load_on_tick()

    def _get_historical_kline_summary_lines(self) -> Tuple[str, str]:
        return self._historical_mgr.get_historical_kline_summary_lines()
