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
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.config_service import resolve_product_exchange
from ali2026v3_trading.analytics_service import safe_int, safe_float


def load_historical_klines_with_stop(
    storage,
    instruments: List[str],
    history_minutes: int = 1440,
    kline_style: str = 'M1',
    market_center: Any = None,
    batch_size: Optional[int] = None,
    inter_batch_delay_sec: float = 0.0,
    request_delay_sec: float = 0.0,
    progress_callback=None,
    stop_check=None,
) -> Dict[str, int]:
    """在历史加载 mixin 内实现可取消加载，避免继续扩张 storage.py。"""
    resolved_provider, provider_type = storage._resolve_kline_provider(market_center)
    if not resolved_provider:
        logging.warning("[Storage] 历史K线提供者不可用，无法加载历史K线")
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

    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=history_minutes)

    logging.info(
        "[Storage] 开始为 %d 个合约加载历史K线: %s -> %s, 周期=%s, provider=%s",
        len(instruments), start_time, end_time, kline_style, provider_type,
    )

    success_count = 0
    failed_count = 0
    total_klines = 0
    fetched_klines = 0
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
            "[Storage] 历史K线批次 %d/%d 开始: batch_size=%d, request_delay=%.3fs, inter_batch_delay=%.3fs",
            batch_index, total_batches, len(batch_instruments), request_delay_sec, inter_batch_delay_sec,
        )

        for instrument_index, instrument_str in enumerate(batch_instruments, start=1):
            if _should_stop():
                logging.info("[Storage] 历史K线加载收到停止信号，合约循环退出")
                break
            try:
                if '.' in instrument_str:
                    exchange, instrument_id = instrument_str.split('.', 1)
                else:
                    exchange = None
                    instrument_id = instrument_str

                if exchange:
                    logging.info(f"[Storage] 加载 {exchange}.{instrument_id} 历史 K 线")
                else:
                    logging.info(f"[Storage] 加载 {instrument_id} 历史 K 线")

                normalized_id = str(instrument_id or '').strip()
                info = storage._get_instrument_info(normalized_id)
                if info is None:
                    warn_key = ('load_historical_klines', normalized_id)
                    with storage._lock:
                        if warn_key not in storage._runtime_missing_warned:
                            storage._runtime_missing_warned.add(warn_key)
                            logging.warning("[load_historical_klines] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
                    failed_count += 1
                    continue

                if not exchange:
                    exchange = storage.infer_exchange_from_id(instrument_id)

                kline_data = storage._fetch_historical_kline_data(
                    resolved_provider, exchange, instrument_id, kline_style, history_minutes, start_time, end_time,
                )

                if _should_stop():
                    logging.info("[Storage] 历史K线加载在API调用后收到停止信号，跳过当前合约")
                    break

                if kline_data and len(kline_data) > 0:
                    fetched_klines += len(kline_data)
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
                        failed_count += 1
                        continue

                    storage._wait_for_queue_capacity(
                        max_fill_rate=60.0,
                        timeout_sec=max(5.0, inter_batch_delay_sec * 20 if inter_batch_delay_sec > 0 else 5.0),
                        source='load_historical_klines',
                    )

                    internal_id = storage._get_info_internal_id(info)
                    instrument_type = info.get('type', 'future')
                    enqueue_failed = False
                    for chunk_start in range(0, len(normalized_klines), storage.batch_size):
                        if _should_stop():
                            logging.info("[Storage] 历史K线加载在入队阶段收到停止信号")
                            enqueue_failed = True
                            break
                        chunk = normalized_klines[chunk_start:chunk_start + storage.batch_size]
                        if not storage._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, normalized_period):
                            enqueue_failed = True
                            break

                    if enqueue_failed:
                        logging.warning(f"[Storage] ⚠️ {instrument_id}: 历史K线入队失败")
                        failed_count += 1
                        continue

                    with storage._ext_kline_lock:
                        storage._last_ext_kline[(instrument_id, normalized_period)] = normalized_klines[-1]['ts']

                    saved_count = len(normalized_klines)
                    success_count += 1
                    total_klines += saved_count
                    batch_enqueued_klines += saved_count
                    logging.info(f"[Storage] ✅ {instrument_id}: 已批量入队 {saved_count} 条K线")
                else:
                    logging.warning(f"[Storage] ⚠️ {instrument_id}: 无历史K线数据")
                    failed_count += 1
            except Exception as exc:
                logging.error(f"[Storage] 加载历史K线失败 {instrument_str}: {exc}")
                failed_count += 1

            if request_delay_sec > 0 and instrument_index < len(batch_instruments):
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
    }


# 从 params_service 导入统一的参数读取函数（解决 strategy_core_service ↔ strategy_historical 循环导入）
from ali2026v3_trading.params_service import get_param_value as _get_param_value


class HistoricalKlineMixin:
    """历史K线加载Mixin
    
    提供完整的历史K线加载功能，包括：
    - 合约范围过滤
    - 数据提供者解析
    - 异步加载管理
    - 进度跟踪和诊断
    """
    
    def _init_historical_kline_mixin(self) -> None:
        """初始化历史K线Mixin的状态
        
        注意：此方法应在StrategyCoreService.__init__中调用
        """
        # 历史加载状态
        self._historical_load_in_progress = False
        self._historical_kline_result = None
        self._historical_kline_progress = None
        self._historical_load_started = False
        self._historical_loader_thread: Optional[threading.Thread] = None
        self._historical_loader_lock = threading.Lock()
        self._background_threads: List[threading.Thread] = []
        
        # 诊断标志
        self._hkl_diag_emitted = False
    
    # ========== 合约过滤 ==========
    
    def _filter_historical_month_scope(self, instrument_ids: List[str]) -> Tuple[List[str], int, str]:
        """历史加载仅保留不早于最小年月门槛的合约，降低无效请求。
        
        Args:
            instrument_ids: 待过滤的合约ID列表
            
        Returns:
            Tuple[List[str], int, str]: (过滤后的合约列表, 移除数量, 最小年月)
        """
        min_year_month = str(
            _get_param_value(self.params, 'history_min_year_month', datetime.now().strftime('%y%m'))
            or datetime.now().strftime('%y%m')
        ).strip()

        # 仅接受三四位年月，配置无效时回退当月。
        if not re.fullmatch(r'\d{3,4}', min_year_month):
            min_year_month = datetime.now().strftime('%y%m')

        filtered: List[str] = []
        removed_count = 0
        for instrument_id in instrument_ids:
            year_month = self._extract_contract_year_month(instrument_id)
            if year_month and year_month < min_year_month:
                removed_count += 1
                continue
            filtered.append(instrument_id)

        return filtered, removed_count, min_year_month
    
    # ========== 合约构建 ==========
    
    def _build_historical_instruments(self) -> List[str]:
        """构建历史K线合约列表
        
        Returns:
            List[str]: 带交易所前缀的合约代码列表
        """
        subscribed = list(getattr(self, '_subscribed_instruments', []) or [])
        if not subscribed:
            return []

        subscribed, removed_by_month, min_year_month = self._filter_historical_month_scope(subscribed)
        if removed_by_month > 0:
            logging.warning(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] Filtered by month: min={min_year_month}, removed={removed_by_month}")

        include_options = bool(_get_param_value(self.params, 'load_history_options', True))
        instruments: List[str] = []
        seen: set = set()
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        future_count = 0
        option_count = 0

        for instrument_id in subscribed:
            # 统一使用 SubscriptionManager.is_option 判断（单一数据源）
            is_option = SubscriptionManager.is_option(instrument_id)
            if is_option:
                option_count += 1
            else:
                future_count += 1

            if not include_options and is_option:
                continue
            exchange = resolve_product_exchange(instrument_id)
            instrument_code = f"{exchange}.{instrument_id}"
            if instrument_code not in seen:
                seen.add(instrument_code)
                instruments.append(instrument_code)

        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Historical instrument set: total_subscribed=%d, futures=%d, options=%d, include_options=%s, selected=%d",
            self.strategy_id,
            len(subscribed),
            future_count,
            option_count,
            include_options,
            len(instruments),
        )

        return instruments
    
    # ========== 提供者解析 ==========
    
    def _resolve_historical_provider(self) -> tuple:
        """解析历史数据提供者
        
        按优先级尝试获取市场中心或策略实例：
        1. params.market_center
        2. strategy.market_center
        3. runtime_market_center
        4. runtime_host提取
        5. get_kline方法包装
        6. fallback方案
        
        Returns:
            tuple: (provider, provider_source) 提供者和来源标识
        """
        params = getattr(self, 'params', None)
        
        # 1. 优先使用params.market_center
        runtime_market_center = _get_param_value(params, 'market_center')
        if runtime_market_center is not None:
            return runtime_market_center, 'params'
        
        # 2. 尝试strategy.market_center
        runtime_strategy = _get_param_value(params, 'strategy')
        if runtime_strategy is not None:
            strategy_mc = getattr(runtime_strategy, 'market_center', None)
            if strategy_mc is not None:
                return strategy_mc, 'strategy'
            if callable(getattr(runtime_strategy, 'get_kline_data', None)) or callable(getattr(runtime_strategy, 'get_kline', None)):
                return runtime_strategy, 'strategy'
        
        # 3. 使用运行时市场中心
        if hasattr(self, '_runtime_market_center') and self._runtime_market_center is not None:
            return self._runtime_market_center, 'runtime'
        
        # 4. 尝试从runtime_host提取
        if hasattr(self, '_runtime_strategy_host'):
            runtime_host = self._runtime_strategy_host
            if runtime_host is not None:
                host_mc = self._extract_runtime_market_center(runtime_host)
                if host_mc is not None:
                    self._runtime_market_center = host_mc
                    return host_mc, 'runtime_host'
                if callable(getattr(runtime_host, 'get_kline_data', None)) or callable(getattr(runtime_host, 'get_kline', None)):
                    return runtime_host, 'runtime_host'
        
        # 5. 使用get_kline方法
        if callable(getattr(self, 'get_kline', None)):
            class _Provider:
                def __init__(self, fn): self.get_kline = fn
            return _Provider(self.get_kline), 'get_kline'
        
        # 6. 备用方案
        fallback = self._get_fallback_market_center()
        if fallback is not None:
            if hasattr(self, '_runtime_market_center'):
                self._runtime_market_center = fallback
            return fallback, 'fallback'
        
        return None, 'unavailable'
    
    # ========== 核心加载逻辑 ==========
    
    def _load_historical_klines_once(self, instruments: List[str], provider: Any, provider_source: str) -> None:
        """加载历史K线（一次性执行）
        
        Args:
            instruments: 合约列表（带交易所前缀）
            provider: 数据提供者
            provider_source: 提供者来源标识
        """
        history_minutes = int(_get_param_value(self.params, 'history_minutes', 1440) or 1440)
        kline_style = str(_get_param_value(self.params, 'kline_style', 'M1') or 'M1')
        configured_batch_size = max(1, int(_get_param_value(self.params, 'history_load_batch_size', 200) or 200))
        max_batch_size = max(1, int(_get_param_value(self.params, 'history_load_max_batch_size', 50) or 50))
        batch_size = min(configured_batch_size, max_batch_size)
        batch_delay = max(0.0, safe_float(_get_param_value(self.params, 'history_load_batch_delay_sec', 0.2)))
        request_delay = max(
            0.0,
            float(
                _get_param_value(
                    self.params,
                    'history_load_request_delay_sec',
                    max(0.03, min(batch_delay, 0.1)),
                )
                or 0.0
            ),
        )

        logging.info(
            "[HKL][strategy_id=%s][owner_scope=strategy-instance][source_type=historical-loader] "
            "Start: instruments=%d, history_minutes=%d, provider=%s, configured_batch_size=%d, effective_batch_size=%d, batch_delay=%.3fs, request_delay=%.3fs",
            self.strategy_id,
            len(instruments),
            history_minutes,
            provider_source,
            configured_batch_size,
            batch_size,
            batch_delay,
            request_delay,
        )

        def _on_progress(progress: Dict[str, Any]) -> None:
            self._historical_kline_progress = dict(progress or {})

        result = load_historical_klines_with_stop(
            self.storage,
            instruments=instruments,
            history_minutes=history_minutes,
            kline_style=kline_style,
            market_center=provider,
            batch_size=batch_size,
            inter_batch_delay_sec=batch_delay,
            request_delay_sec=request_delay,
            progress_callback=_on_progress,
            stop_check=lambda: bool(
                getattr(self, '_historical_stop_flag', False)
                or getattr(self, '_destroyed', False)
                or getattr(self, '_stop_requested', False)
                or not getattr(self, '_is_running', False)
            ),
        )
        
        self._historical_kline_result = dict(result or {})
        self._historical_kline_progress = dict(result or {})
        # P1修复：消费enqueued_klines/persisted_klines，兼容旧字段total_klines
        enqueued = safe_int(result.get('enqueued_klines', 0) or result.get('total_klines', 0))
        persisted = safe_int(result.get('persisted_klines', 0))
        with self._historical_loader_lock:
            self._stats['total_klines'] = self._stats.get('total_klines', 0) + enqueued
            self._stats['total_enqueued_klines'] = self._stats.get('total_enqueued_klines', 0) + enqueued
            self._stats['total_persisted_klines'] = self._stats.get('total_persisted_klines', 0) + persisted
        logging.info(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] " f"Done: success={result.get('success', 0)}, failed={result.get('failed', 0)}, " f"enqueued={enqueued}, persisted={persisted}")
    
    # ========== 启动控制 ==========
    
    def _start_historical_kline_load(self) -> None:
        """启动历史K线加载
        
        检查配置和条件，决定是否启动加载任务。
        支持同步和异步两种模式。
        """
        auto_load = bool(_get_param_value(self.params, 'auto_load_history', False))
        if not auto_load:
            logging.info(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] auto_load_history=False, skip")
            return

        instruments = self._build_historical_instruments()
        if not instruments:
            logging.info(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] No subscribed instruments, skip")
            return

        provider, provider_source = self._resolve_historical_provider()
        if provider is None:
            logging.warning(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] Provider not ready, will retry later")
            return

        with self._historical_loader_lock:
            if self._historical_load_started and self._historical_kline_result is not None:
                return
            # P1 Bug #28修复：所有状态修改都在锁内，保证一致性
            self._historical_load_started = True
            self._historical_load_in_progress = True
            self._historical_kline_result = None
            self._historical_kline_progress = None
            self._historical_stop_flag = False  # P1 Bug #29修复：添加停止标志

        def _runner() -> None:
            try:
                # 等待合约注册（最多30秒）
                max_wait, waited = 30, 0
                while waited < max_wait:
                    # P1 Bug #29修复：检查停止标志，提前退出
                    with self._historical_loader_lock:
                        if self._historical_stop_flag:
                            logging.info(
                                "[HKL][strategy_id=%s][owner_scope=strategy-instance]"
                                "[source_type=historical-loader] Stopped by stop flag during registration wait",
                                self.strategy_id
                            )
                            return
                    
                    if hasattr(self, 'storage') and self.storage:
                        try:
                            registered = len(self.storage.get_registered_instrument_ids() or [])
                        except Exception:
                            registered = 0
                    else:
                        registered = 0
                    
                    if registered >= len(instruments):
                        break
                    
                    if waited % 5 == 0:
                        logging.info(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] " f"Waiting for registration... {registered}/{len(instruments)} ({waited}s)")
                    
                    time.sleep(1)
                    waited += 1
                
                if waited >= max_wait:
                    logging.warning(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] " f"Registration timeout ({waited}s), force start")
                
                self._load_historical_klines_once(instruments, provider, provider_source)
            except Exception as e:
                logging.error(
                    f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]"
                    f"[source_type=historical-loader] Load failed: {e}", exc_info=True
                )
            finally:
                # P1 Bug #28修复：状态修改在锁内
                with self._historical_loader_lock:
                    self._historical_load_in_progress = False
                    self._historical_kline_progress = None
                if hasattr(self, 'storage') and self.storage:
                    try:
                        self.storage.close_connection()
                    except Exception:
                        pass

        if bool(_get_param_value(self.params, 'async_history_load', True)):
            thread = threading.Thread(target=_runner, name=f"hkl-{self.strategy_id}", daemon=True)
            self._historical_loader_thread = thread
            thread.start()
            logging.info(f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]" f"[source_type=historical-loader] Async loader started")
        else:
            self._historical_loader_thread = None
            _runner()
    
    # ========== 状态管理 ==========
    
    def _shutdown_historical_services(self) -> None:
        with self._historical_loader_lock:
            self._historical_stop_flag = True
            self._historical_load_started = False
            loader_thread = self._historical_loader_thread
            self._historical_loader_thread = None

        if loader_thread and loader_thread.is_alive():
            logging.info(
                f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]"
                f"[source_type=historical-loader] Waiting for loader thread to exit (max 5s)"
            )
            deadline = time.time() + 5.0
            while loader_thread.is_alive() and time.time() < deadline:
                loader_thread.join(timeout=0.2)

            if loader_thread.is_alive():
                logging.warning(
                    f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]"
                    f"[source_type=historical-loader] ⚠️ Loader thread still alive after 5s"
                )
            else:
                logging.info(
                    f"[HKL][strategy_id={self.strategy_id}][owner_scope=strategy-instance]"
                    f"[source_type=historical-loader] ✅ Loader thread exited"
                )

        # join所有后台线程
        for t in self._background_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        self._background_threads.clear()
    
    def _reset_historical_state_for_restart(self) -> None:
        """重置历史K线状态以支持重启
        
        在prepare_restart时调用，允许重新触发历史加载。
        """
        with self._historical_loader_lock:
            self._historical_stop_flag = False
            self._historical_load_started = False
            self._historical_loader_thread = None
    
    # ========== Tick入口诊断 ==========
    
    def _emit_historical_kline_diagnostic_on_first_tick(self) -> None:
        """在第一个Tick时输出历史K线加载诊断信息
        
        仅在首次Tick时执行一次，用于验证加载状态。
        """
        if not self._hkl_diag_emitted:
            self._hkl_diag_emitted = True
            historical_load_started = getattr(self, '_historical_load_started', False)
            
            # 获取 K 线统计
            historical_result = self._historical_kline_result or {}
            historical_progress = self._historical_kline_progress or {}
            current_total_klines = safe_int(self._stats.get('total_klines', 0))
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
                self.strategy_id,
                self._state, self._is_running, self._is_paused,
                historical_load_started,
                len(self._subscribed_instruments),
            )
    
    def _check_and_start_historical_load_on_tick(self) -> None:
        """在Tick回调中检查并启动历史K线加载
        
        P1-2修复：如果尚未启动，异步触发加载流程，避免在tick回调线程中同步执行耗时K线加载。
        正常情况下 on_start 已通过 _start_historical_kline_load_async 启动了后台加载，
        此方法仅作为兜底：如果后台加载失败或尚未完成，在tick回调中异步补启。
        """
        with self._historical_loader_lock:
            if self._historical_load_started:
                return
            self._historical_load_started = True

        # 异步启动，不在tick回调线程中同步执行
        import threading
        def _async_kline_load():
            try:
                self._start_historical_kline_load()
            except Exception as e:
                import logging
                logging.error(
                    "[HistoricalLoadTick][strategy_id=%s][owner_scope=strategy-instance]"
                    "[source_type=historical-fallback] 异步K线加载失败: %s",
                    self.strategy_id, e, exc_info=True
                )
        t = threading.Thread(
            target=_async_kline_load,
            name=f"kline-load-tick-fallback-{self.strategy_id}",
            daemon=True
        )
        self._background_threads.append(t)
        t.start()
    
    # ========== 周期性汇总 ==========
    
    def _get_historical_kline_summary_lines(self) -> Tuple[str, str]:
        """获取历史K线汇总行（用于周期性状态输出）
        
        Returns:
            Tuple[str, str]: (K线摘要行, 历史加载详情行)
        """
        historical_result = self._historical_kline_result or {}
        if self._historical_load_in_progress:
            kline_summary_line = "K 线数据：历史加载进行中，完成后更新累计条数"
        else:
            kline_summary_line = f"K 线数据：总计 {self._stats['total_klines']:,} 条"

        historical_detail_line = ""
        if historical_result:
            historical_detail_line = (
                f"历史K线加载结果：抓取 {safe_int(historical_result.get('fetched_klines', 0)):,} 条，"
                f"入队 {safe_int(historical_result.get('enqueued_klines', 0) or historical_result.get('total_klines', 0)):,} 条，"
                f"成功 {safe_int(historical_result.get('success', 0)):,}，"
                f"失败 {safe_int(historical_result.get('failed', 0)):,}"
            )
        
        return kline_summary_line, historical_detail_line
