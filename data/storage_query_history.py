# MODULE_ID: M1-048
"""
storage_query_history.py — StorageHistoryService
历史K线加载 + Schema迁移

重构说明 (2026-06-11):
- _StorageQueryHistoryMixin → StorageHistoryService（服务提取+Facade组合，消灭Mixin）
- 构造函数显式接收 base_service (StorageQueryBaseService)，组合替代继承
"""

import logging
import math
import time
from typing import List, Dict, Optional, Any, Tuple, Callable
from datetime import datetime, timedelta

from infra.shared_utils import InitPhase, requires_phase, CHINA_TZ


class StorageHistoryService:

    def __init__(self, base_service):
        self._base = base_service

    def __getattr__(self, name):
        delegated = [
            '_lock', '_params_service', '_data_service', '_maintenance_service',
            '_runtime_missing_warned', '_assigned_ids', 'max_retries', 'retry_delay',
            'batch_size', '_TABLE_NAME_PATTERN',
            '_ensure_exchange_map_loaded', '_validate_table_name',
            '_execute_with_retry', '_parse_future', '_parse_option_with_dash',
            '_make_instrument_info', '_get_info_internal_id',
            'infer_exchange_from_id', '_validate_tick', '_validate_kline',
            '_get_instrument_info', '_get_info_by_id',
            'get_registered_instrument_ids', 'load',
            '_cache_to_params_service', '_cache_alias_instrument_mapping',
            '_warn_runtime_unregistered', '_get_registered_instrument_info',
            '_get_registered_internal_id', '_query_rows', 'connection', '_q',
            '_to_timestamp', '_normalize_tick_fields', '_validate_signal',
            '_validate_underlying', '_json_default', '_load_caches',
            '_init_kv_store', '_load_exchange_map_from_config',
            '_resolve_kline_provider', '_estimate_kline_count',
            '_normalize_kline_period', '_get_next_id',
            '_create_future_tables_by_id', '_create_option_tables_by_id',
            '_extract_table_suffix_id',
            '_enqueue_write', '_wait_for_queue_capacity', 'get_queue_stats',
            '_ext_kline_lock', '_last_ext_kline',
        ]
        if name in delegated:
            return getattr(self._base, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _fetch_historical_kline_data(
        self,
        provider: Any,
        exchange: str,
        instrument_id: str,
        kline_style: str,
        history_minutes: int,
        start_time: datetime,
        end_time: datetime,
    ) -> Any:
        resolved_provider, provider_type = self._resolve_kline_provider(provider)
        if resolved_provider is None:
            raise RuntimeError('no historical kline provider available')

        provider_class = type(resolved_provider).__name__
        provider_module = getattr(type(resolved_provider), '__module__', '')

        if provider_type in ('get_kline_data', 'market_center.get_kline_data'):
            get_kline_data = getattr(resolved_provider, 'get_kline_data')
            count = self._estimate_kline_count(history_minutes, kline_style)  # 负值: PythonGO要求count<0表示从最新往回取

            start_time_naive = start_time.replace(tzinfo=None) if hasattr(start_time, 'tzinfo') and start_time.tzinfo else start_time
            end_time_naive = end_time.replace(tzinfo=None) if hasattr(end_time, 'tzinfo') and end_time.tzinfo else end_time
            start_time_str = start_time_naive.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = end_time_naive.strftime('%Y-%m-%d %H:%M:%S')


            def _log_probe_once(stage: str, call_name: str) -> None:
                probe_key = (stage, provider_type, provider_module, provider_class, call_name)
                attr_name = '_hkl_provider_preprobe_emitted' if stage == 'before' else '_hkl_provider_probe_emitted'
                with self._lock:
                    emitted = getattr(self, attr_name, set())
                    if probe_key in emitted:
                        return
                    emitted.add(probe_key)
                    setattr(self, attr_name, emitted)
                if stage == 'before':
                    logging.info(
                        "[HKLProbeBefore] provider_type=%s provider_class=%s provider_module=%s call=%s exchange=%s instrument=%s style=%s count=%s start_time=%s end_time=%s",
                        provider_type,
                        provider_class,
                        provider_module,
                        call_name,
                        exchange,
                        instrument_id,
                        kline_style,
                        count,
                        start_time,
                        end_time,
                    )
                else:
                    logging.info(
                        "[HKLProbe] provider_type=%s provider_class=%s provider_module=%s call=%s exchange=%s instrument=%s style=%s count=%s",
                        provider_type,
                        provider_class,
                        provider_module,
                        call_name,
                        exchange,
                        instrument_id,
                        kline_style,
                        count,
                    )

            def _try_calls(call_specs: List[Tuple[str, Callable[[], Any]]], allow_date_error: bool = False) -> Any:
                last_exc = None
                for call_name, call in call_specs:
                    _log_probe_once('before', call_name)
                    try:
                        result = call()
                        if isinstance(result, (list, tuple)) and len(result) == 0:
                            logging.debug("[Storage] get_kline_data %s 返回空列表，跳过", call_name)
                            continue
                        _log_probe_once('after', call_name)
                        return result
                    except (TypeError, AttributeError) as exc:
                        last_exc = exc
                        continue
                    except Exception as exc:
                        last_exc = exc
                        if allow_date_error and ('Parsing Date Err' in str(exc) or 'Date Err' in str(exc)):
                            logging.debug("[Storage] get_kline_data 时间参数调用失败，跳过 %s: %s", call_name, exc)
                            continue
                        logging.debug("[Storage] get_kline_data 调用失败，跳过 %s: %s", call_name, exc)
                        continue
                if last_exc is not None:
                    logging.debug(
                        "[Storage] provider=%s instrument=%s 未匹配到可用历史K线调用签名，最后错误：%s",
                        provider_type,
                        instrument_id,
                        last_exc,
                    )
                return None

            kline_call_specs = [
                ('kw_style_count', False, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, count=count)),
                ('kw_instrument_count', False, lambda: get_kline_data(exchange=exchange, instrument=instrument_id, style=kline_style, count=count)),
                ('pos_style_count', False, lambda: get_kline_data(exchange, instrument_id, kline_style, count)),
                ('kw_period_count', False, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, period=kline_style, count=count)),
                ('kw_style_tstr', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, start_time=start_time_str, end_time=end_time_str)),
                ('kw_period_tstr', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, period=kline_style, start_time=start_time_str, end_time=end_time_str)),
            ]

            cached_sig = getattr(self.__class__, '_kline_sig_cache', {}).get(provider_type)
            if cached_sig:
                for name, allow_date_error, call_fn in kline_call_specs:
                    if name == cached_sig:
                        result = _try_calls([(name, call_fn)], allow_date_error=allow_date_error)
                        if result is not None:
                            return result
                if not hasattr(self.__class__, '_kline_sig_cache'):
                    self.__class__._kline_sig_cache = {}
                self.__class__._kline_sig_cache.pop(provider_type, None)

            for name, allow_date_error, call_fn in kline_call_specs:
                result = _try_calls([(name, call_fn)], allow_date_error=allow_date_error)
                if result is not None:
                    logging.debug("[Storage] get_kline_data matched signature: %s", name)
                    if not hasattr(self.__class__, '_kline_sig_cache'):
                        self.__class__._kline_sig_cache = {}
                    self.__class__._kline_sig_cache[provider_type] = name
                    return result

            if not hasattr(self.__class__, '_kline_sig_no_match_logged'):
                self.__class__._kline_sig_no_match_logged = set()
            if provider_type not in self.__class__._kline_sig_no_match_logged:
                self.__class__._kline_sig_no_match_logged.add(provider_type)
                logging.warning(
                    "[Storage] get_kline_data provider=%s has no compatible signature for %s, "
                    "subsequent errors will be suppressed",
                    provider_type, instrument_id,
                )
            return None

        get_kline = getattr(resolved_provider, 'get_kline')
        count = self._estimate_kline_count(history_minutes, kline_style)  # 负值: PythonGO要求count<0表示从最新往回取
        try:
            return get_kline(
                exchange=exchange,
                instrument_id=instrument_id,
                style=kline_style,
                count=count,
            )
        except TypeError:
            return get_kline(exchange, instrument_id, kline_style, count)

    @requires_phase(InitPhase.DB_SCHEMA)
    def load_historical_klines(self, instruments: List[str], history_minutes: int = 1440,
                              kline_style: str = 'M1', market_center: Any = None,
                              batch_size: Optional[int] = None,
                              inter_batch_delay_sec: float = 0.0,
                              request_delay_sec: float = 0.0,
                              progress_callback: Optional[Callable[[Dict[str, int]], None]] = None) -> Dict[str, int]:
        resolved_provider, provider_type = self._resolve_kline_provider(market_center)
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

        import time as time_module

        end_time = datetime.now(CHINA_TZ)
        start_time = end_time - timedelta(minutes=history_minutes)

        logging.info(
            f"[Storage] 开始为 {len(instruments)} 个合约加载历史K线: "
            f"{start_time} -> {end_time}, 周期={kline_style}, provider={provider_type}"
        )

        success_count = 0
        failed_count = 0
        total_klines = 0
        fetched_klines = 0
        normalized_period = self._normalize_kline_period(kline_style)

        effective_batch_size = max(1, int(batch_size or len(instruments) or 1))
        total_batches = math.ceil(len(instruments) / effective_batch_size) if instruments else 0

        for batch_index, start in enumerate(range(0, len(instruments), effective_batch_size), start=1):
            batch_instruments = instruments[start:start + effective_batch_size]
            batch_enqueued_klines = 0

            logging.info(
                "[Storage] 历史K线批次 %d/%d 开始: batch_size=%d, request_delay=%.3fs, inter_batch_delay=%.3fs",
                batch_index,
                total_batches,
                len(batch_instruments),
                request_delay_sec,
                inter_batch_delay_sec,
            )

            for instrument_index, instrument_str in enumerate(batch_instruments, start=1):
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
                    info = self._params_service.get_instrument_meta_by_id(normalized_id)
                    if info is None:
                        info = self._get_instrument_info(normalized_id)
                    if info is None:
                        warn_key = ('load_historical_klines', normalized_id)
                        with self._lock:
                            if warn_key not in self._runtime_missing_warned:
                                self._runtime_missing_warned.add(warn_key)
                                logging.debug("[load_historical_klines] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
                        failed_count += 1
                        continue

                    if not exchange:
                        exchange = self.infer_exchange_from_id(instrument_id)

                    kline_data = self._fetch_historical_kline_data(
                        resolved_provider,
                        exchange,
                        instrument_id,
                        kline_style,
                        history_minutes,
                        start_time,
                        end_time,
                    )

                    if kline_data and len(kline_data) > 0:
                        fetched_klines += len(kline_data)
                        normalized_klines = []
                        for kline in kline_data:
                            try:
                                ts = self._to_timestamp(
                                    getattr(kline, 'datetime', None)
                                )
                                if ts is None:
                                    raise ValueError('invalid historical kline timestamp')

                                normalized_klines.append({
                                    'ts': ts,
                                    'instrument_id': instrument_id,
                                    'exchange': exchange,
                                    'open': getattr(kline, 'open', 0.0),
                                    'high': getattr(kline, 'high', 0.0),
                                    'low': getattr(kline, 'low', 0.0),
                                    'close': getattr(kline, 'close', 0.0),
                                    'volume': getattr(kline, 'volume', 0),
                                    'open_interest': getattr(kline, 'open_interest', 0),
                                    'period': normalized_period,
                                })
                            except Exception as e:
                                logging.warning(f"[Storage] 保存K线失败 {instrument_id}: {e}")

                        if not normalized_klines:
                            failed_count += 1
                            continue

                        self._wait_for_queue_capacity(
                            max_fill_rate=60.0,
                            timeout_sec=max(5.0, inter_batch_delay_sec * 20 if inter_batch_delay_sec > 0 else 5.0),
                            source='load_historical_klines',
                        )

                        internal_id = self._get_info_internal_id(info)
                        instrument_type = info.get('type', 'future')
                        enqueue_failed = False
                        for chunk_start in range(0, len(normalized_klines), self.batch_size):
                            chunk = normalized_klines[chunk_start:chunk_start + self.batch_size]
                            if not self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, normalized_period):
                                enqueue_failed = True
                                break

                        if enqueue_failed:
                            logging.warning(f"[Storage] {instrument_id}: 历史K线入队失败")
                            failed_count += 1
                            continue

                        with self._ext_kline_lock:
                            self._last_ext_kline[(instrument_id, normalized_period)] = normalized_klines[-1]['ts']

                        saved_count = len(normalized_klines)
                        success_count += 1
                        total_klines += saved_count
                        batch_enqueued_klines += saved_count
                        logging.info(f"[Storage] ✅ {instrument_id}: 已批量入队 {saved_count} 条K线")
                    else:
                        logging.info(f"[Storage] {instrument_id}: 无历史K线数据")
                        failed_count += 1

                except Exception as e:
                    logging.error(f"[Storage] 加载历史K线失败 {instrument_str}: {e}")
                    failed_count += 1

                if request_delay_sec > 0 and instrument_index < len(batch_instruments):
                    time_module.sleep(request_delay_sec)

            logging.info(
                "[Storage] 历史K线批次 %d/%d 完成: success=%d, failed=%d, fetched_klines=%d, enqueued_klines=%d",
                batch_index,
                total_batches,
                success_count,
                failed_count,
                fetched_klines,
                batch_enqueued_klines,
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
                except Exception as callback_exc:
                    logging.debug(f"[Storage] 历史K线进度回调失败: {callback_exc}")

            if inter_batch_delay_sec > 0 and batch_index < total_batches:
                time_module.sleep(inter_batch_delay_sec)

        queue_stats_after = self.get_queue_stats()
        queue_received_delta = queue_stats_after.get('total_received', 0)
        queue_written_delta = queue_stats_after.get('total_written', 0)
        drops_delta = queue_stats_after.get('drops_count', 0)
        actual_enqueued = max(0, total_klines - drops_delta)
        logging.info(
            f"[Storage] 历史K线加载完成: 成功={success_count}, 失败={failed_count}, "
            f"抓取K线={fetched_klines} 条, 入队K线={actual_enqueued} 条(原始={total_klines}, 丢弃={drops_delta}), "
            f"落盘K线={queue_written_delta} 条"
        )

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

    def _migrate_legacy_schema(self):
        migrated = False
        try:
            rows = self._data_service.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'futures_instruments'").to_pylist()
            futures_columns = [row['column_name'] for row in rows]

            if 'internal_id' not in futures_columns and 'id' in futures_columns:
                logging.info("正在迁移 futures_instruments 表：添加 internal_id 列并回填旧 id...")
                self._data_service.query("ALTER TABLE futures_instruments ADD COLUMN internal_id INTEGER", raise_on_error=True)
                self._data_service.query("UPDATE futures_instruments SET internal_id=id WHERE internal_id IS NULL", raise_on_error=True)
                migrated = True


            rows = self._data_service.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'option_instruments'").to_pylist()
            option_columns = [row['column_name'] for row in rows]

            if 'internal_id' not in option_columns and 'id' in option_columns:
                logging.info("正在迁移 option_instruments 表：添加 internal_id 列并回填旧 id...")
                self._data_service.query("ALTER TABLE option_instruments ADD COLUMN internal_id INTEGER", raise_on_error=True)
                self._data_service.query("UPDATE option_instruments SET internal_id=id WHERE internal_id IS NULL", raise_on_error=True)
                migrated = True

            if 'underlying_future_id' not in option_columns:
                logging.info("正在迁移 option_instruments 表：添加 underlying_future_id 列...")
                self._data_service.query("ALTER TABLE option_instruments ADD COLUMN underlying_future_id INTEGER", raise_on_error=True)
                migrated = True


            self._data_service.query("""
                UPDATE option_instruments
                SET underlying_product = product
                WHERE underlying_product != product
                  AND EXISTS (
                      SELECT 1
                      FROM option_products op
                      WHERE op.product = option_instruments.product
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM option_products op
                      WHERE op.product = option_instruments.underlying_product
                  )
            """, raise_on_error=True)
            fix_rows = self._data_service.query("""SELECT COUNT(*) as cnt FROM option_instruments
                WHERE underlying_product != product
                  AND EXISTS (SELECT 1 FROM option_products op WHERE op.product = option_instruments.product)
                  AND NOT EXISTS (SELECT 1 FROM option_products op WHERE op.product = option_instruments.underlying_product)
            """).to_pylist()
            fix_count = fix_rows[0]['cnt'] if fix_rows else 0
            if fix_count > 0:
                migrated = True
                logging.info(
                    "正在修复 option_instruments.underlying_product 历史错误值：%d 条",
                    fix_count,
                )

            if migrated:
                logging.info("✅ 数据库表结构迁移完成")
            else:
                logging.debug("数据库表结构已是最新版本，无需迁移")

        except Exception as e:
            logging.error(f"表结构迁移失败：{e}")
            raise

    def _create_indexes(self):
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_future_product ON futures_instruments(product)", raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_future_yym ON futures_instruments(year_month)", raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_option_product ON option_instruments(product)", raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_option_underlying_id ON option_instruments(underlying_future_id)", raise_on_error=True)
        logging.info("索引创建/检查完成")


_StorageQueryHistoryMixin = StorageHistoryService
