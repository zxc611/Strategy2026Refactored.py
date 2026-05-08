"""
storage_query.py — _StorageQueryMixin
READ + HELPER + INSTRUMENT + SCHEMA 方法组
"""

from ali2026v3_trading.subscription_manager import SubscriptionManager, inst_get
from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
import logging
import os
import re
import time
import json
import math
import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Optional, Any, Tuple, Callable
from datetime import datetime, time as dt_time

from ali2026v3_trading.shared_utils import InitPhase, requires_phase


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class _StorageQueryMixin:

    _exchange_map_loaded = False

    def _ensure_exchange_map_loaded(self):
        if not _StorageQueryMixin._exchange_map_loaded:
            _StorageQueryMixin._exchange_map_loaded = True
            self._load_exchange_map_from_config()

    def _validate_table_name(self, table_name: str):
        if not self._TABLE_NAME_PATTERN.match(table_name):
            raise ValueError(f"非法表名：{table_name}")

    def _execute_with_retry(self, func, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

    def _parse_future(self, instrument_id: str) -> Dict[str, Any]:
        return SubscriptionManager.parse_future(instrument_id)

    def _parse_option_with_dash(self, instrument_id: str) -> Dict[str, Any]:
        return SubscriptionManager.parse_option(instrument_id)

    @staticmethod
    def _make_instrument_info(
        internal_id: int,
        instrument_id: str,
        instrument_type: str,
        **extra: Any,
    ) -> Dict[str, Any]:
        from ali2026v3_trading.shared_utils import extract_product_code, ShardRouter
        _pc_raw = extract_product_code(instrument_id)
        product_code = _pc_raw.lower() if _pc_raw else ''
        shard_key = ShardRouter._deterministic_hash(product_code) if product_code else 0
        info = {
            'internal_id': internal_id,
            'instrument_id': instrument_id,
            'type': instrument_type,
            'product_code': product_code,
            'shard_key': shard_key,
        }
        info.update(extra)
        return info

    @staticmethod
    def _get_info_internal_id(info: Optional[Dict[str, Any]]) -> Optional[int]:
        if not info:
            return None
        internal_id = info.get('internal_id')
        return int(internal_id) if internal_id is not None else None

    @staticmethod
    def _extract_table_suffix_id(table_name: Optional[str], expected_prefix: str) -> Optional[int]:
        if not table_name or not table_name.startswith(expected_prefix):
            return None
        suffix = table_name[len(expected_prefix):]
        if not suffix.isdigit():
            return None
        value = int(suffix)
        return value if value > 0 else None

    def _warn_runtime_unregistered(self, instrument_id: str, source: str) -> None:
        warn_key = (source, instrument_id)
        with self._lock:
            if warn_key in self._runtime_missing_warned:
                return
            self._runtime_missing_warned.add(warn_key)
        logging.warning(
            "[%s] 合约未预注册，跳过运行时自动注册/建表：%s",
            source, instrument_id,
        )

    _EXCHANGE_MAP = {
        'CFFEX': ('IF', 'IH', 'IC', 'IM', 'IO', 'HO', 'MO'),
        'GFEX': ('LC', 'SI'),
        'SHFE': ('CU', 'AL', 'ZN', 'RB', 'AU', 'AG', 'NI', 'SN', 'PB', 'SS', 'WR', 'RU', 'NR', 'BC', 'LU'),
        'DCE': ('M', 'Y', 'A', 'JM', 'I', 'C', 'CS', 'JD', 'L', 'V', 'PP', 'EG', 'PG', 'J', 'P'),
        'CZCE': ('CF', 'SR', 'MA', 'TA', 'RM', 'OI', 'SA', 'PF', 'FG', 'AP', 'CJ', 'SF', 'SM', 'UR'),
        'INE': ('SC',),
    }

    @classmethod
    def _load_exchange_map_from_config(cls):
        import os
        import json
        try:
            env_map = os.getenv('ALI2026_EXCHANGE_MAP', '').strip()
            if env_map:
                custom = json.loads(env_map)
                if isinstance(custom, dict) and custom:
                    cls._EXCHANGE_MAP = custom
                    logging.info("[StorageQuery] Loaded exchange map from env: %d exchanges", len(custom))
                    return
        except Exception as e:
            logging.debug("[StorageQuery] Failed to load exchange map from env: %s", e)

    def infer_exchange_from_id(self, instrument_id: str) -> str:
        self._ensure_exchange_map_loaded()
        normalized_id = str(instrument_id or '').strip()
        upper_id = normalized_id.upper()
        for exchange, prefixes in self._EXCHANGE_MAP.items():
            for prefix in prefixes:
                if upper_id.startswith(prefix.upper()):
                    return exchange
        logging.debug("[StorageQuery] infer_exchange_from_id: unknown prefix for %s, defaulting to CFFEX", instrument_id)
        return 'CFFEX'

    @staticmethod
    def _to_timestamp(ts) -> Optional[float]:
        if ts is None:
            return None
        if isinstance(ts, datetime):
            result = ts.timestamp()
        elif isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts)
                result = dt.timestamp()
            except ValueError:
                try:
                    result = float(ts)
                except (ValueError, TypeError) as e:
                    logging.warning(f"[_to_timestamp] Failed to convert timestamp '{ts}': {e}")
                    return None
        elif isinstance(ts, (int, float)):
            result = float(ts)
        else:
            logging.warning(f"[_to_timestamp] Unsupported timestamp type: {type(ts)}")
            return None

        import math
        if math.isnan(result) or math.isinf(result):
            return None
        return result

    def _validate_tick(self, tick: Dict[str, Any]) -> bool:
        if not isinstance(tick, dict):
            logging.error("save_tick 参数不是字典：%s", type(tick))
            return False
        instrument_id = tick.get('instrument_id') or tick.get('InstrumentID')
        if not instrument_id:
            logging.error("save_tick 缺少合约标识字段 (instrument_id 或 InstrumentID)")
            return False
        last_price = tick.get('last_price')
        if last_price is None:
            last_price = tick.get('LastPrice')
        if last_price is None:
            logging.error("save_tick 缺少价格字段 (last_price 或 LastPrice)")
            return False
        ts_value = tick.get('ts') or tick.get('timestamp') or tick.get('UpdateTime')
        if ts_value is None:
            logging.error("save_tick 缺少时间戳字段 (ts、timestamp 或 UpdateTime)")
            return False
        if not isinstance(last_price, (int, float)) or last_price < 0:
            logging.warning("save_tick 价格无效（结算tick可能为0）：%s", last_price)
            return True
        volume = tick.get('volume', tick.get('Volume', 0))
        if volume is not None and (not isinstance(volume, (int, float)) or volume < 0):
            logging.error("save_tick 成交量无效：%s", volume)
            return False
        return True

    def _validate_kline(self, kline: Dict[str, Any]) -> bool:
        if not isinstance(kline, dict):
            logging.error("save_external_kline 参数不是字典：%s", type(kline))
            return False
        required = ['instrument_id', 'period', 'open', 'high', 'low', 'close']
        for field in required:
            if field not in kline:
                logging.error("save_external_kline 缺少必要字段：%s", field)
                return False
        if 'ts' not in kline and 'timestamp' not in kline:
            logging.error("save_external_kline 缺少时间戳字段 (ts 或 timestamp)")
            return False
        for field in ['open', 'high', 'low', 'close']:
            val = kline.get(field)
            if val is not None and (not isinstance(val, (int, float)) or val < 0):
                logging.error("save_external_kline %s 无效：%s", field, val)
                return False
        return True

    _TICK_FIELD_ALIASES = {
        'InstrumentID': 'instrument_id',
        'LastPrice': 'last_price',
        'Volume': 'volume',
        'Timestamp': 'ts',
        'timestamp': 'ts',
        'UpdateTime': 'ts',
        'DateTime': 'ts',
        'datetime': 'ts',
        'BidPrice1': 'bid_price1',
        'AskPrice1': 'ask_price1',
        'OpenInterest': 'open_interest',
        'Amount': 'amount',
        'Turnover': 'turnover',
        'ExchangeID': 'exchange',
    }

    def _normalize_tick_fields(self, tick: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化Tick字段名：统一为 PythonGO 格式
        使用_TICK_FIELD_ALIASES映射表，单次遍历完成所有字段转换
        """
        normalized = tick.copy()
        for src, dst in self._TICK_FIELD_ALIASES.items():
            if dst not in normalized and src in normalized:
                normalized[dst] = normalized[src]
        return normalized

    # 方法唯一修复：K线提供者探测链，按优先级有序
    _KLINE_PROVIDER_PROBES = [
        ('get_kline_data', lambda p: p),
        ('get_kline', lambda p: p),
    ]

    @staticmethod
    def _resolve_kline_provider(provider: Any) -> Tuple[Optional[Any], str]:
        if provider is None:
            return None, 'none'
        for method_name, resolver in _StorageQueryMixin._KLINE_PROVIDER_PROBES:
            target = resolver(provider)
            if callable(getattr(target, method_name.rsplit('.', 1)[-1], None)):
                return target, method_name
        return None, 'unavailable'

    @staticmethod
    def _estimate_kline_count(history_minutes: int, kline_style: str) -> int:
        style = str(kline_style or 'M1').upper()
        if style.startswith('M'):
            try:
                period_minutes = max(1, int(style[1:]))
            except ValueError:
                period_minutes = 1
        else:
            period_minutes = 1

        return -max(1, int(history_minutes / period_minutes))

    @staticmethod
    def _normalize_kline_period(kline_style: str) -> str:
        style = str(kline_style or 'M1').upper()
        if style.startswith('M'):
            suffix = style[1:] or '1'
            return f"{suffix}min"
        return style.lower()

    def _validate_signal(self, signal: Dict[str, Any]) -> bool:
        if not isinstance(signal, dict):
            logging.error("save_signal 参数不是字典：%s", type(signal))
            return False
        required = ['ts', 'instrument_id', 'signal_type', 'price', 'score', 'strategy_name']
        for field in required:
            if field not in signal:
                logging.error("save_signal 缺少必要字段：%s", field)
                return False
        price = signal.get('price')
        if price is not None and (not isinstance(price, (int, float)) or price <= 0):
            logging.error("save_signal 价格无效：%s", price)
            return False
        score = signal.get('score')
        if score is not None and (not isinstance(score, (int, float)) or score < 0 or score > 1):
            logging.error("save_signal 评分无效（应为 0-1）: %s", score)
            return False
        return True

    def _validate_underlying(self, data: Dict[str, Any]) -> bool:
        if not isinstance(data, dict):
            logging.error("save_underlying_snapshot 参数不是字典：%s", type(data))
            return False
        required = ['ts', 'underlying', 'expiration']
        for field in required:
            if field not in data:
                logging.error("save_underlying_snapshot 缺少必要字段：%s", field)
                return False
        last_price = data.get('last_price')
        if last_price is not None and (not isinstance(last_price, (int, float)) or last_price < 0):
            logging.error("save_underlying_snapshot 价格无效：%s", last_price)
            return False
        return True

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _get_registered_internal_id(self, instrument_id: str, source: str = 'runtime') -> Optional[int]:
        info = self._get_registered_instrument_info(instrument_id, source=source)
        if info:
            return self._get_info_internal_id(info)
        return None

    def _get_registered_instrument_info(self, instrument_id: str, source: str = 'runtime') -> Optional[dict]:
        normalized_id = str(instrument_id or '').strip()
        if not normalized_id:
            return None

        info = self._params_service.get_instrument_meta_by_id(normalized_id)
        if info is None:
            info = self._get_instrument_info(normalized_id)

        if info:
            return info

        self._warn_runtime_unregistered(normalized_id, source)
        return None

    def _query_rows(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        result = self._data_service.query(sql, params, arrow=False)
        if hasattr(result, 'to_dict'):
            return result.to_dict('records')
        return []

    @property
    def connection(self):
        return self._data_service

    def _q(self, sql: str, params: list = None) -> list:
        result = self._data_service.query(sql, params or [])
        if result is None:
            return []
        if hasattr(result, 'to_pylist'):
            return result.to_pylist()
        if hasattr(result, 'to_dict'):
            return result.to_dict('records')
        if hasattr(result, '__iter__'):
            return list(result)
        return []

    def _get_instrument_info(self, instrument_id: str) -> Optional[dict]:
        with self._lock:
            info = self._params_service.get_instrument_meta_by_id(instrument_id)
            if info:
                return info

            rows = self._q("SELECT internal_id, instrument_id, product_code, shard_key FROM futures_instruments WHERE instrument_id=?", [instrument_id])
            if rows:
                r = rows[0]
                info = self._make_instrument_info(r['internal_id'], r['instrument_id'], 'future')
                if r.get('shard_key') is not None:
                    info['shard_key'] = r['shard_key']
                    info['product_code'] = r.get('product_code') or info.get('product_code', '')
                self._cache_to_params_service(instrument_id, info)
                return info

            rows = self._q("SELECT internal_id, instrument_id, product_code, shard_key FROM option_instruments WHERE instrument_id=?", [instrument_id])
            if rows:
                r = rows[0]
                info = self._make_instrument_info(r['internal_id'], r['instrument_id'], 'option')
                if r.get('shard_key') is not None:
                    info['shard_key'] = r['shard_key']
                    info['product_code'] = r.get('product_code') or info.get('product_code', '')
                self._cache_to_params_service(instrument_id, info)
                return info
            return None

    def _get_info_by_id(self, internal_id: int) -> Optional[dict]:
        with self._lock:
            info = self._params_service.get_instrument_meta_by_id(str(internal_id))
            if info:
                return info

            rows = self._q("SELECT internal_id, instrument_id, product_code, shard_key FROM futures_instruments WHERE internal_id=?", [internal_id])
            row = rows[0] if rows else None
            if row:
                info = self._make_instrument_info(
                    row['internal_id'],
                    row['instrument_id'],
                    'future',
                )
                if row.get('shard_key') is not None:
                    info['shard_key'] = row['shard_key']
                    info['product_code'] = row.get('product_code') or info.get('product_code', '')
                instrument_id = row['instrument_id']
                self._cache_to_params_service(instrument_id, info)
                return info

            rows = self._q("SELECT internal_id, instrument_id, product_code, shard_key FROM option_instruments WHERE internal_id=?", [internal_id])
            row = rows[0] if rows else None
            if row:
                info = self._make_instrument_info(
                    row['internal_id'],
                    row['instrument_id'],
                    'option',
                )
                if row.get('shard_key') is not None:
                    info['shard_key'] = row['shard_key']
                    info['product_code'] = row.get('product_code') or info.get('product_code', '')
                instrument_id = row['instrument_id']
                self._cache_to_params_service(instrument_id, info)
                return info
            return None

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
    def get_registered_instrument_ids(self, instrument_ids: Optional[List[str]] = None) -> List[str]:
        if not instrument_ids:
            return list(self._params_service._instrument_id_to_internal_id.keys())

        registered_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids:
            normalized_id = str(instrument_id or '').strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)

            if self._params_service.get_instrument_meta_by_id(normalized_id):
                registered_ids.append(normalized_id)

        return registered_ids

    def get_option_chain_by_future_id(self, future_id: int) -> Dict:
        info = self._get_info_by_id(future_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货ID: {future_id}")
        future_instrument = info.get('instrument_id')
        if not future_instrument:
            rows = self._data_service.query("SELECT instrument_id FROM futures_instruments WHERE internal_id=?", [future_id]).to_pylist()
            row = rows[0] if rows else None
            if row:
                future_instrument = row['instrument_id']
        if not future_instrument:
            raise ValueError(f"未找到期货合约ID: {future_id}")

        options = self._data_service.query("""
            SELECT internal_id, instrument_id, option_type, strike_price
            FROM option_instruments
            WHERE underlying_future_id=?
            ORDER BY strike_price, option_type
        """, [future_id]).to_pylist()

        normalized_options = []
        for row in options:
            option_internal_id = int(row['internal_id'])
            normalized_options.append({
                'internal_id': option_internal_id,
                'instrument_id': row['instrument_id'],
                'option_type': row['option_type'],
                'strike_price': row['strike_price'],
            })

        return {
            'future': {'internal_id': future_id, 'instrument_id': future_instrument},
            'options': normalized_options
        }

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
            count = self._estimate_kline_count(history_minutes, kline_style)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

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
                ('pos_style_count', False, lambda: get_kline_data(exchange, instrument_id, kline_style, count)),
                ('kw_period_count', False, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, period=kline_style, count=count)),
                ('kw_style_tstr', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, start_time=start_time_str, end_time=end_time_str)),
                ('kw_style_dt', True, lambda: get_kline_data(exchange=exchange, instrument_id=instrument_id, style=kline_style, start_time=start_time, end_time=end_time)),
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
        count = self._estimate_kline_count(history_minutes, kline_style)
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

        from datetime import timedelta
        import time as time_module

        end_time = datetime.now()
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
                                logging.warning("[load_historical_klines] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
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
                                    getattr(kline, 'timestamp', getattr(kline, 'ts', time_module.time()))
                                )
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

    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """
        根据期货合约代码获取期权链（委托给QueryService）。

        Args:
            future_instrument_id: 期货合约代码

        Returns:
            Dict: 期权链信息
        """
        from ali2026v3_trading.query_service import QueryService
        qs = QueryService(self)
        return qs.get_option_chain_for_future(future_instrument_id)

    @requires_phase(InitPhase.DB_SCHEMA)
    def query_kline(self, instrument_id: str, start_time: str, end_time: str,
                    limit: Optional[int] = None) -> List[Dict]:
        internal_id = self.register_instrument(instrument_id)
        return self.query_kline_by_id(internal_id, start_time, end_time, limit)

    @requires_phase(InitPhase.DB_SCHEMA)
    def query_tick(self, instrument_id: str, start_time: str, end_time: str,
                   limit: Optional[int] = None) -> List[Dict]:
        internal_id = self.register_instrument(instrument_id)
        return self.query_tick_by_id(internal_id, start_time, end_time, limit)

    @requires_phase(InitPhase.DB_SCHEMA)
    def get_option_chain(self, ts: float, underlying: str, expiration: str) -> List[Dict[str, Any]]:
        try:
            rows = self._data_service.query(
                "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                [underlying, expiration]
            ).to_pylist()

            if not rows:
                logging.warning(f"[Storage] 期货合约未注册: {underlying}{expiration}，无法查询期权链")
                return []

            future_instrument_id = rows[0]['instrument_id']
            chain = self.get_option_chain_for_future(future_instrument_id)
            return chain.get('options', [])
        except Exception as e:
            logging.error("get_option_chain failed: %s", e)
            return []

    def get_latest_underlying(self, underlying: str, expiration: str) -> Optional[Dict[str, Any]]:
        logging.debug("get_latest_underlying called: %s %s", underlying, expiration)
        return None

    @staticmethod
    def load_all_instruments(strategy_instance=None, params=None, logger=None):
        import logging as stdlib_logging

        if logger is None:
            logger = stdlib_logging.getLogger(__name__)

        futures_list = []
        options_dict = {}

        try:
            if strategy_instance is not None:
                try:
                    instruments = getattr(strategy_instance, 'subscribed_instruments', {})
                    for inst_id, inst_info in instruments.items():
                        try:
                            product = inst_get(inst_info, 'product', 'Product', 'underlying_product')
                            instrument_id = inst_get(inst_info, 'instrument_id', 'InstrumentID', '_instrument_id')

                            if not instrument_id:
                                continue

                            is_option = False
                            try:
                                SubscriptionManager.parse_option(instrument_id)
                                is_option = True
                            except Exception as _parse_err:
                                logging.debug("[Storage] 合约解析失败(已降级): %s", _parse_err)

                            if is_option:
                                underlying_future_id = inst_get(inst_info, 'underlying_future_id')
                                if underlying_future_id:
                                    try:
                                        from ali2026v3_trading.data_service import get_data_service
                                        ds = get_data_service()
                                        future_rows = ds.query(
                                            "SELECT instrument_id FROM futures_instruments WHERE internal_id = ?",
                                            [underlying_future_id]
                                        ).to_pylist()
                                        if future_rows and len(future_rows) > 0:
                                            future_instrument_id = future_rows[0]['instrument_id']
                                        else:
                                            continue
                                    except Exception:
                                        continue

                                    if future_instrument_id not in options_dict:
                                        options_dict[future_instrument_id] = []
                                    options_dict[future_instrument_id].append({
                                        'instrument_id': instrument_id,
                                        'product': product,
                                        'underlying_future_id': underlying_future_id,
                                        'exchange': inst_get(inst_info, 'exchange', 'ExchangeID'),
                                        'year_month': inst_get(inst_info, 'year_month', 'DeliveryMonth'),
                                        'strike_price': inst_get(inst_info, 'strike_price', 'StrikePrice'),
                                        'option_type': inst_get(inst_info, 'option_type', 'OptionsType')
                                    })
                            else:
                                futures_list.append({
                                    'instrument_id': instrument_id,
                                    'product': product,
                                    'exchange': inst_get(inst_info, 'exchange', 'ExchangeID'),
                                    'year_month': inst_get(inst_info, 'year_month', 'DeliveryMonth')
                                })
                        except Exception as e:
                            logger.debug("跳过合约处理失败：%s", e)
                except Exception as e:
                    logger.warning("从策略实例获取合约失败：%s", e)

            if not futures_list and not options_dict and params is not None:
                try:
                    configured_instruments = getattr(params, 'instrument_ids', [])
                    for inst_id in configured_instruments:
                        try:
                            if isinstance(inst_id, str):
                                is_option = False
                                opt_parsed = None
                                try:
                                    opt_parsed = SubscriptionManager.parse_option(inst_id)
                                    is_option = True
                                except Exception as _parse_err:
                                    logging.debug("[Storage] 合约解析失败(已降级): %s", _parse_err)

                                if is_option and opt_parsed:
                                    underlying = opt_parsed.get('underlying', '')
                                    if not underlying:
                                        underlying = SubscriptionManager.parse_future(inst_id).get('product', inst_id)
                                    if underlying not in options_dict:
                                        options_dict[underlying] = []
                                    options_dict[underlying].append({
                                        'instrument_id': inst_id,
                                        'underlying': underlying
                                    })
                                else:
                                    futures_list.append({
                                        'instrument_id': inst_id
                                    })
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug("从 params 获取合约失败：%s", e)

            logger.info("加载完成：%d 个期货，%d 个品种期权", len(futures_list), len(options_dict))
            return futures_list, options_dict

        except Exception as e:
            logger.error("load_all_instruments 异常：%s", e)
            return [], {}

    def load(self, key: str) -> Optional[Any]:
        if not key or not isinstance(key, str):
            logging.error("load 失败：key 无效：%s", key)
            return None

        try:
            rows = self._data_service.query("SELECT value FROM app_kv_store WHERE key = ?", [key]).to_pylist()
            row = rows[0] if rows else None
            if not row:
                return None
            return json.loads(row['value'])
        except Exception as e:
            logging.error("load 失败 key=%s: %s", key, e, exc_info=True)
            return None

    @contextmanager
    def transaction(self):
        try:
            yield
        except Exception as e:
            raise

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
    def ensure_registered_instruments(self, instrument_ids: List[str]) -> Dict[str, int]:
        from ali2026v3_trading.query_service import QueryService

        return QueryService(self).ensure_registered_instruments(instrument_ids)

    def _get_next_id(self) -> int:
        """从全局序列获取下一个internal_id"""
        if self._maintenance_service is not None:
            return self._maintenance_service.reserve_next_global_id()

        raise RuntimeError("[Storage] _maintenance_service is None, cannot reserve global ID")

    def _create_future_tables_by_id(self, instrument_id: int):
        logging.debug("_create_future_tables_by_id is deprecated, using unified klines_raw")

    def _create_option_tables_by_id(self, instrument_id: int):
        logging.debug("_create_option_tables_by_id is deprecated, using unified klines_raw")

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
    def register_instrument(self, instrument_id: str, exchange: str = "AUTO",
                            expire_date: Optional[str] = None,
                            listing_date: Optional[str] = None) -> int:
        normalized_instrument_id = str(instrument_id or '').strip()
        if not normalized_instrument_id:
            raise ValueError("instrument_id 不能为空")

        if str(exchange or 'AUTO').strip() == 'AUTO':
            exchange = self.infer_exchange_from_id(normalized_instrument_id)

        existing_meta = self._params_service.get_instrument_meta_by_id(normalized_instrument_id)
        if existing_meta:
            return existing_meta.get('internal_id')

        # 查询数据库
        info = self._get_instrument_info(normalized_instrument_id)
        if info:
            return self._get_info_internal_id(info)

        # 注册新合约（加锁保证唯一性）
        with self._lock:
            from ali2026v3_trading.shared_utils import ShardRouter
            existing_meta = self._params_service.get_instrument_meta_by_id(normalized_instrument_id)
            if existing_meta:
                return existing_meta.get('internal_id')
            normalized_upper = str(normalized_instrument_id).strip().upper()
            rows = self._data_service.query("SELECT product, exchange, format_template FROM future_products WHERE is_active=1").to_pylist()
            for prod in rows:
                product_key = str(prod['product'] or '').strip()
                if normalized_upper.startswith(product_key.upper()):
                    try:
                        parsed = self._parse_future(normalized_instrument_id)
                        # 生成新ID
                        new_id = self._get_next_id()
                        kline_table = f"kline_future_{new_id}"
                        tick_table = f"tick_future_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        self._data_service.query("""
                            INSERT INTO futures_instruments
                            (internal_id, exchange, instrument_id, product, year_month, format,
                             expire_date, listing_date, kline_table, tick_table, product_code, shard_key)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [new_id, exchange, normalized_instrument_id, parsed['product'], parsed['year_month'],
                              prod['format_template'], expire_date, listing_date, kline_table, tick_table,
                              parsed['product'].lower(), ShardRouter._deterministic_hash(parsed['product'].lower())], raise_on_error=True)
                        self._create_future_tables_by_id(new_id)
                        # 更新缓存
                        info = self._make_instrument_info(
                            new_id,
                            normalized_instrument_id,
                            'future',
                            product=parsed['product'],
                            year_month=parsed['year_month'],
                            kline_table=kline_table,
                            tick_table=tick_table,
                        )
                        self._assigned_ids.add(new_id)  # v1吸收：跟踪已分配ID
                        self._cache_to_params_service(normalized_instrument_id, info)  # v1吸收：双向缓存同步
                        logging.info(f"注册期货合约：{normalized_instrument_id} -> ID={new_id}")
                        
                        # DiagnosisProbeManager already imported at module level
                        DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'future')
                        
                        return new_id
                    except ValueError:
                        continue

            # 尝试注册为期权
            rows = self._data_service.query("SELECT product, exchange, underlying_product, format_template FROM option_products WHERE is_active=1").to_pylist()
            for prod in rows:
                product_key = str(prod['product'] or '').strip()
                if normalized_upper.startswith(product_key.upper()):
                    try:
                        parsed = self._parse_option_with_dash(normalized_instrument_id)
                        # 查找标的期货 ID（如果没有，先注册期货）
                        new_future_id = None
                        rows = self._data_service.query("SELECT internal_id, instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                       [prod['underlying_product'], parsed['year_month']]).to_pylist()
                        row = rows[0] if rows else None
                        if not row:
                            expected_future_id = f"{prod['underlying_product']}{parsed['year_month']}"
                            logging.error(
                                f"[Storage] 标的期货未注册，无法注册期权 {normalized_instrument_id}。"
                                f"请先注册期货合约: {expected_future_id}"
                            )
                            return -1  # 注册失败
                        
                        # 使用数据库中已存在的原始instrument_id（ID直通）
                        new_future_id = row['internal_id']
                        logging.info("标的期货已存在：ID=%d", new_future_id)
                        new_id = self._get_next_id()
                        kline_table = f"kline_option_{new_id}"
                        tick_table = f"tick_option_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        opt_product_code = parsed['product'].lower()
                        opt_shard_key = ShardRouter._deterministic_hash(opt_product_code)
                        self._data_service.query("""
                            INSERT INTO option_instruments
                            (internal_id, exchange, instrument_id, product, underlying_future_id, underlying_product,
                            year_month, option_type, strike_price, format,
                            expire_date, listing_date, kline_table, tick_table, product_code, shard_key)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [new_id, exchange, normalized_instrument_id, parsed['product'], new_future_id, prod['product'], parsed['year_month'],
                              parsed['option_type'], parsed['strike_price'], prod['format_template'],
                              expire_date, listing_date, kline_table, tick_table,
                              opt_product_code, opt_shard_key], raise_on_error=True)
                        self._create_option_tables_by_id(new_id)
                        # 更新缓存
                        info = self._make_instrument_info(
                            new_id,
                            normalized_instrument_id,
                            'option',
                            product=parsed['product'],
                            year_month=parsed['year_month'],
                            option_type=parsed['option_type'],
                            strike_price=parsed['strike_price'],
                            underlying_future_id=new_future_id,
                            kline_table=kline_table,
                            tick_table=tick_table,
                        )
                        self._assigned_ids.add(new_id)  # v1吸收：跟踪已分配ID
                        self._cache_to_params_service(normalized_instrument_id, info)  # v1吸收：双向缓存同步
                        logging.info(f"注册期权合约：{normalized_instrument_id} -> ID={new_id}")
                        
                        # DiagnosisProbeManager already imported at module level
                        DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'option')
                        
                        return new_id
                    except ValueError:
                        # 期权解析失败，尝试注册为期权标的期货（如 ho2605 -> IH2605）
                        try:
                            # 方法唯一修复：统一使用_parse_future，不再内联正则
                            future_parsed = self._parse_future(normalized_instrument_id)
                            underlying_prod = str(prod.get('underlying_product') or '').strip()
                            if underlying_prod and future_parsed.get('year_month'):
                                rows = self._data_service.query(
                                    "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                    [underlying_prod, future_parsed['year_month']]
                                ).to_pylist()
                                if rows:
                                    future_instrument_id = rows[0]['instrument_id']  # 使用数据库中的原始值
                                    # 检查是否已注册
                                    existing = self._get_instrument_info(future_instrument_id)
                                    if existing:
                                            self._cache_alias_instrument_mapping(normalized_instrument_id, existing)
                                            canonical_internal_id = self._get_info_internal_id(existing)
                                            logging.info("复用期权标的期货主记录：%s -> ID=%d (主记录=%s)", normalized_instrument_id, canonical_internal_id, future_instrument_id)
                                            # DiagnosisProbeManager already imported at module level
                                            DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', canonical_internal_id, 'future')
                                            return canonical_internal_id
                        except Exception as _e:
                            logging.debug("[Storage] 期权标的期货回退注册失败: %s", _e)
                        continue

            raise ValueError(f"无法识别合约品种：{normalized_instrument_id}")


    @requires_phase(InitPhase.DB_SCHEMA)
    def delete_instrument(self, instrument_id: str) -> None:
        info = self._get_instrument_info(instrument_id)
        if not info:
            logging.warning(f"合约不存在，无法删除：{instrument_id}")
            return

        internal_id = self._get_info_internal_id(info)

        if info['type'] == 'future':
            rows = self._data_service.query(
                "SELECT instrument_id FROM option_instruments WHERE underlying_future_id=?",
                [internal_id],
            ).to_pylist()
            dependent_options = [row['instrument_id'] for row in rows]
            for option_instrument_id in dependent_options:
                self.delete_instrument(option_instrument_id)

        kline_table = info.get('kline_table')
        tick_table = info.get('tick_table')
        if kline_table:
            self._validate_table_name(kline_table)
            self._data_service.query(f"DROP TABLE IF EXISTS {kline_table}", raise_on_error=True)
        if tick_table:
            self._validate_table_name(tick_table)
            self._data_service.query(f"DROP TABLE IF EXISTS {tick_table}", raise_on_error=True)
        delete_internal_id = int(internal_id)
        if info['type'] == 'future':
            self._data_service.query("DELETE FROM futures_instruments WHERE instrument_id=?", [instrument_id], raise_on_error=True)
        else:
            self._data_service.query("DELETE FROM option_instruments WHERE instrument_id=?", [instrument_id], raise_on_error=True)
        logging.info(f"已删除合约 {instrument_id} (ID={internal_id})")

    def batch_add_future_instruments(self, instruments: List[Dict]) -> None:
        try:
            for inst in instruments:
                exchange = inst.get('exchange', 'AUTO')
                instrument_id = inst.get('instrument_id')
                if not instrument_id:
                    continue
                try:
                    self.register_instrument(instrument_id, exchange,
                                           expire_date=inst.get('expire_date'),
                                           listing_date=inst.get('listing_date'))
                except Exception as e:
                    logging.warning("批量导入期货失败 %s: %s", instrument_id, e)
            logging.info("批量导入 %d 个期货合约", len(instruments))
        except Exception as e:
            logging.error("批量导入期货失败：%s", e)
            raise

    def batch_add_option_instruments(self, instruments: List[Dict]) -> None:
        try:
            for inst in instruments:
                exchange = inst.get('exchange', 'AUTO')
                instrument_id = inst.get('instrument_id')
                if not instrument_id:
                    continue
                try:
                    self.register_instrument(instrument_id, exchange,
                                           expire_date=inst.get('expire_date'),
                                           listing_date=inst.get('listing_date'))
                except Exception as e:
                    logging.warning("批量导入期权失败 %s: %s", instrument_id, e)
            logging.info("批量导入 %d 个期权合约", len(instruments))
        except Exception as e:
            logging.error("批量导入期权失败：%s", e)
            raise

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

    def _load_caches(self):
        """加载缓存：委托给ParamsService统一管理"""
        try:
            self._params_service.load_caches_from_db(self._data_service)
            logging.info("[Storage] ParamsService缓存加载完成: %d个合约, %d个品种",
                         len(self._params_service._instrument_id_to_internal_id),
                         len(self._params_service._product_cache))
        except Exception as e:
            logging.error("[Storage] ParamsService缓存加载失败: %s", e)
            raise

        try:
            for internal_id in self._params_service._instrument_meta_by_id.keys():
                self._assigned_ids.add(internal_id)
            if self._assigned_ids:
                logging.info(f"[Storage] 已加载 {len(self._assigned_ids)} 个已分配 ID")
        except Exception as e:
            logging.warning(f"[Storage] 加载 assigned_ids 失败: {e}")

    def _init_kv_store(self):
        self._data_service.query("""
            CREATE TABLE IF NOT EXISTS app_kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """, raise_on_error=True)
        self._data_service.query("CREATE INDEX IF NOT EXISTS idx_app_kv_updated_at ON app_kv_store(updated_at DESC)", raise_on_error=True)
        logging.debug("KV 存储表初始化完成")
