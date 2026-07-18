# MODULE_ID: M1-047
"""
storage_query_base.py — StorageQueryBaseService
基础工具方法 + 数据验证方法 + 交易所映射

重构说明 (2026-06-11):
- _StorageQueryBaseMixin → StorageQueryBaseService（服务提取+Facade组合，消灭Mixin）
- 构造函数显式接收 lock / params_service / data_service / maintenance_service
"""

from infra.subscription_service import SubscriptionManager
from config.config_params import DATA_CLEANING_RULES
import logging
import os
import json
import time
from contextlib import contextmanager
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

from infra.shared_utils import InitPhase, requires_phase, CHINA_TZ


class StorageQueryBaseService:

    _exchange_map_loaded = False

    def __init__(self, lock, params_service, data_service, maintenance_service=None):
        self._lock = lock
        self._params_service = params_service
        self._data_service = data_service
        self._maintenance_service = maintenance_service
        self._runtime_missing_warned = set()
        self._assigned_ids = set()
        self.max_retries = 3
        self.retry_delay = 0.1
        self.batch_size = 1000
        self._TABLE_NAME_PATTERN = __import__('re').compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    def _ensure_exchange_map_loaded(self):
        if not _StorageQueryBaseMixin._exchange_map_loaded:
            _StorageQueryBaseMixin._exchange_map_loaded = True
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
        from infra.shared_utils import extract_product_code, ShardRouter
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
        logging.debug(
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
        _rules = DATA_CLEANING_RULES
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
        if _rules.get('remove_zero_price', True) and (not isinstance(last_price, (int, float)) or last_price <= 0):
            logging.warning("save_tick 价格无效(<=0)：%s", last_price)
            return False
        bid_price1 = tick.get('bid_price1') or tick.get('BidPrice1')
        ask_price1 = tick.get('ask_price1') or tick.get('AskPrice1')
        # FIX-20260704-DBL-MAX: 过滤C++ DBL_MAX(1.79e308)哨兵值
        import math
        if isinstance(bid_price1, float) and (not math.isfinite(bid_price1) or bid_price1 > 1e300):
            bid_price1 = None
        if isinstance(ask_price1, float) and (not math.isfinite(ask_price1) or ask_price1 > 1e300):
            ask_price1 = None
        if bid_price1 is not None and ask_price1 is not None:
            if isinstance(bid_price1, (int, float)) and isinstance(ask_price1, (int, float)):
                if bid_price1 > 0 and ask_price1 > 0 and bid_price1 >= ask_price1:
                    _max_spread_pct = _rules.get('max_spread_pct', 5.0)
                    _spread_pct = abs(bid_price1 - ask_price1) / ask_price1 * 100.0
                    if _spread_pct > _max_spread_pct:
                        logging.warning("save_tick 盘口价差异常: spread_pct=%.2f%% > max=%.2f%%, bid=%.4f, ask=%.4f, instrument=%s",
                                        _spread_pct, _max_spread_pct, bid_price1, ask_price1, instrument_id)
                    logging.warning("save_tick 盘口交叉异常: bid=%.4f >= ask=%.4f instrument=%s",
                                    bid_price1, ask_price1, instrument_id)
                    return False
        volume = tick.get('volume', tick.get('Volume', 0))
        if _rules.get('remove_negative_volume', True) and volume is not None and (not isinstance(volume, (int, float)) or volume < 0):
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
        使用例TICK_FIELD_ALIASES映射表，单次遍历完成所有字段转换
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
        for method_name, resolver in _StorageQueryBaseMixin._KLINE_PROVIDER_PROBES:
            target = resolver(provider)
            if callable(getattr(target, method_name.rsplit('.', 1)[-1], None)):
                return target, method_name
        return None, 'unavailable'

    @staticmethod
    def _estimate_kline_count(history_minutes: int, kline_style: str) -> int:
        """估算历史K线拉取条数，返回负值。

        PythonGO平台 MarketCenter.get_kline_data() 的 count 参数语义：
        - count > 0: 从最早数据开始取N条（正向）
        - count < 0: 从最新数据往回取N条（反向，即"最近N条"）
        历史K线拉取场景需要"最近N条"，因此 count 必须为负值。
        例如 count=-1440 表示从当前往回拉取1440条M1 K线。
        这是正确的设计，非bug。参见PythonGO说明文档 get_kline_data(count=-10) 示例。
        """
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

    def load(self, key: str) -> Optional[Any]:
        if not key or not isinstance(key, str):
            logging.error("load 失败：key 无效：%s", key)
            return None

        try:
            rows = self._data_service.query("SELECT value FROM app_kv_store WHERE key = ?", [key]).to_pylist()
            row = rows[0] if rows else None
            if not row:
                return None
            loaded = json.loads(row['value'])
            # SER-04修复: 兼容带版本号和不带版本号的payload
            if isinstance(loaded, dict) and '__kv_version__' in loaded:
                return loaded.get('data')
            return loaded
        except Exception as e:
            logging.error("load 失败 key=%s: %s", key, e, exc_info=True)
            return None

    @contextmanager
    def transaction(self):
        try:
            yield
        except Exception as e:
            raise

    def _get_next_id(self) -> int:
        """从全局序列获取下一个internal_id"""
        if self._maintenance_service is not None:
            return self._maintenance_service.reserve_next_global_id()

        raise RuntimeError("[Storage] _maintenance_service is None, cannot reserve global ID")

    def _create_future_tables_by_id(self, instrument_id: int):
        logging.warning("[DEPRECATED] _create_future_tables_by_id is deprecated, using unified klines_raw")  # R16-P1-DOC-P1-10修复: debug→warning确保废弃标记可见

    def _create_option_tables_by_id(self, instrument_id: int):
        logging.warning("[DEPRECATED] _create_option_tables_by_id is deprecated, using unified klines_raw")  # R16-P1-DOC-P1-10修复: debug→warning确保废弃标记可见

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

    def _cache_to_params_service(self, instrument_id: str, info: Dict[str, Any]) -> Dict[str, Any]:
        return self._params_service.cache_instrument_info(instrument_id, info)

    def _cache_alias_instrument_mapping(
        self,
        alias_instrument_id: str,
        canonical_info: Dict[str, Any],
    ) -> int:
        canonical_internal_id = self._get_info_internal_id(canonical_info)
        if canonical_internal_id is None:
            raise ValueError(f"canonical instrument has no internal_id: {canonical_info}")
        alias_info = dict(canonical_info)
        alias_info['instrument_id'] = alias_instrument_id
        alias_info['internal_id'] = canonical_internal_id
        alias_info['canonical_instrument_id'] = canonical_info.get('instrument_id')
        try:
            self._cache_to_params_service(alias_instrument_id, alias_info)
        except RuntimeError as exc:
            if '重复 internal_id' not in str(exc):
                raise
        return canonical_internal_id


_StorageQueryBaseMixin = StorageQueryBaseService
