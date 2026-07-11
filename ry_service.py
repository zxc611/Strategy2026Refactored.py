# MODULE_ID: M1-040
"""
query_service.py - 数据查询服务 (DuckDB 版本)

合并说明 (2026-06-30):
- 原 query_kline_aggregator.py: LightKLine + _KlineAggregator + 辅助函数 ← 合入
- query_instrument_service.py: _QueryInstrumentMixin (合约注册/预注册/分类)
- query_data_export.py: _QueryDataExportMixin (数据查询/导出)
- 本文件: QueryService + K线聚合 + 辅助函数 + 合约工具
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ
from ali2026v3_trading.infra.maintenance_service import _normalize_code

try:
    from ali2026v3_trading.data.data_service import DataService, get_data_service
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning("[QueryService] Failed to import DataService: %s", e)
    _HAS_DATA_SERVICE = False
    DataService = None
    get_data_service = None

try:
    from ali2026v3_trading.config.config_params import DATA_EXPORT_FORMAT
except ImportError:
    DATA_EXPORT_FORMAT = 'parquet'


# ============================================================================
# K线聚合辅助函数（合入自 query_kline_aggregator.py）
# ============================================================================

def _result_to_pylist(result: Any) -> List[Dict[str, Any]]:
    if result is None:
        return []
    if hasattr(result, 'to_pylist'):
        return result.to_pylist()
    if hasattr(result, 'read_all'):
        materialized = result.read_all()
        if hasattr(materialized, 'to_pylist'):
            return materialized.to_pylist()
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return []


def _table_exists(ds: Any, table_name: str) -> bool:
    rows = _result_to_pylist(ds.query(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ))
    return bool(rows and int(rows[0].get('cnt', 0) or 0) > 0)


class LightKLine:
    """
    轻量级 K 线对象，使用 __slots__ 优化内存

    适用场景：
    - 内存中临时存储大量K线数据
    - 需要比dict更高效的属性访问
    - 与pythongo.KLineData兼容的简化结构

    注意：生产环境建议直接使用 pyarrow.Table 或 pythongo.KLineData
    """
    __slots__ = ('open', 'high', 'low', 'close', 'volume', 'datetime',
                 'exchange', 'instrument_id', 'style', 'is_placeholder', 'source')

    def __init__(self, open_price: float, high: float, low: float,
                 close: float, volume: float, dt: datetime):
        self.open = float(open_price)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)
        self.volume = float(volume)
        self.datetime = dt
        self.exchange = ""
        self.instrument_id = ""
        self.style = "M1"
        self.is_placeholder = False
        self.source = ""


class _KlineAggregator:
    """K 线聚合器 - 从 Tick 数据合成 K 线（流式处理，内存高效）"""

    def __init__(self, instrument_id: str, period: str, logger: Optional[logging.Logger] = None):
        self.instrument_id = instrument_id
        self.period = period
        self.logger = logger or logging.getLogger(__name__)

        self._lock = threading.Lock()

        self.open_price: Optional[float] = None
        self.high_price: Optional[float] = None
        self.low_price: Optional[float] = None
        self.close_price: Optional[float] = None
        self.volume: int = 0
        self.amount: float = 0.0
        self.open_interest: Optional[int] = None

        self.kline_start_time: Optional[float] = None

        self._last_tick_ts: Optional[float] = None

        self.period_seconds = self._parse_period(period)

    def _parse_period(self, period: str) -> int:
        period_map = {
            '1s': 1, '1m': 60, '1min': 60, '5m': 300, '5min': 300,
            '15m': 900, '15min': 900, '30m': 1800, '30min': 1800,
            '60m': 3600, '60min': 3600, '1h': 3600, '2h': 7200,
            '4h': 14400, '1d': 86400,
        }
        return period_map.get(period, 60)

    def update(self, tick_ts: float, price: float,
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[int] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._update_impl(tick_ts, price, volume, amount, open_interest)

    def _update_impl(self, tick_ts: float, price: float,
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[int] = None) -> Optional[Dict[str, Any]]:
        import math as _kline_math
        # FIX-M8-2: 原代码price<=0直接拒绝，但aggregate_kline_only已尝试用bid/ask中间价替代
        # 当bid/ask也无效时price仍为<=0，此处再次拒绝导致双重过滤
        # 修复: 仅拒绝NaN/Inf和负价格，允许price=0通过(深度虚值期权快照last_price=0是合法数据)
        # price=0的K线虽然OHLC均为0，但能保证K线时间轴连续性，便于后续分析
        if isinstance(price, float) and (_kline_math.isnan(price) or _kline_math.isinf(price) or price < 0):
            self.logger.warning("[CC-05] K线聚合拒绝NaN/Inf/负价格: instrument_id=%s price=%s", self.instrument_id, price)
            return None
        if self._last_tick_ts is not None and tick_ts < self._last_tick_ts:
            self.logger.warning(
                "Tick乱序检测: instrument_id=%s, "
                "当前tick_ts=%s < 上一个tick_ts=%s, 丢弃乱序tick不聚合",
                self.instrument_id, tick_ts, self._last_tick_ts
            )
            return None
        self._last_tick_ts = tick_ts

        if self.kline_start_time is None:
            self.kline_start_time = (tick_ts // self.period_seconds) * self.period_seconds
            self.open_price = self.high_price = self.low_price = self.close_price = price
            self.volume = volume
            self.amount = amount
            self.open_interest = open_interest
            return None

        current_kline_end = self.kline_start_time + self.period_seconds
        if tick_ts >= current_kline_end:
            completed_kline = self._get_current_kline()
            self.kline_start_time = (tick_ts // self.period_seconds) * self.period_seconds
            self.open_price = self.high_price = self.low_price = self.close_price = price
            self.volume = volume
            self.amount = amount
            self.open_interest = open_interest
            return completed_kline

        self.close_price = price
        if price > self.high_price:
            self.high_price = price
        if price < self.low_price:
            self.low_price = price
        self.volume += volume
        self.amount += amount
        if open_interest is not None:
            self.open_interest = open_interest

        return None

    def _get_current_kline(self) -> Dict[str, Any]:
        return {
            'instrument_id': self.instrument_id,
            'period': self.period,
            'ts': self.kline_start_time,
            'timestamp': self.kline_start_time,
            'open': self.open_price,
            'high': self.high_price,
            'low': self.low_price,
            'close': self.close_price,
            'volume': self.volume,
            'amount': self.amount,
            'open_interest': self.open_interest,
        }

    def flush(self) -> Optional[Dict[str, Any]]:
        # FIX-M8-3: 新增flush方法，强制返回当前未完成的K线
        # 解决稀疏tick场景下K线永不完成的问题(736 ticks / 16288 instruments = ~0.045 tick/instrument)
        # 由flush_incomplete_klines定期调用，确保至少有一根K线落库
        with self._lock:
            if self.kline_start_time is None or self.open_price is None:
                return None
            kline = self._get_current_kline()
            # 重置聚合器，开始新的K线周期
            self.open_price = self.high_price = self.low_price = self.close_price = None
            self.volume = 0
            self.amount = 0.0
            self.open_interest = None
            self.kline_start_time = None
            return kline

    def reset(self):
        self.open_price = self.high_price = self.low_price = self.close_price = None
        self.volume = 0
        self.amount = 0.0
        self.open_interest = None
        self.kline_start_time = None

    def to_state_dict(self) -> Dict[str, Any]:
        if self.kline_start_time is None:
            return None
        return {
            'instrument_id': self.instrument_id,
            'period': self.period,
            'open_price': self.open_price,
            'high_price': self.high_price,
            'low_price': self.low_price,
            'close_price': self.close_price,
            'volume': self.volume,
            'amount': self.amount,
            'open_interest': self.open_interest,
            'kline_start_time': self.kline_start_time,
        }

    @classmethod
    def from_state_dict(cls, state: Dict[str, Any]) -> '_KlineAggregator':
        agg = cls(state.get('instrument_id', ''), state.get('period', ''), logging.getLogger(__name__))
        agg.open_price = state.get('open_price')
        agg.high_price = state.get('high_price')
        agg.low_price = state.get('low_price')
        agg.close_price = state.get('close_price')
        agg.volume = state.get('volume', 0)
        agg.amount = state.get('amount', 0.0)
        agg.open_interest = state.get('open_interest')
        agg.kline_start_time = state.get('kline_start_time')
        return agg


# ============================================================================
# QueryService — 数据查询服务（Facade组合）
# ============================================================================

from ali2026v3_trading.data.query_instrument_service import (
    InstrumentQueryService,
    _QueryInstrumentMixin,
)

from ali2026v3_trading.data.query_data_export import (
    DataExportService,
    _QueryDataExportMixin,
)


class QueryService:
    """
    数据查询服务（Facade组合，消灭Mixin继承）

    职责：
    1. 合约信息查询（缓存优先）
    2. K 线/Tick 数据查询
    3. 期权链关联查询
    4. 数据统计与摘要
    5. 数据导出到 CSV
    """

    def __init__(self, storage_instance):
        self._storage = storage_instance
        self._futures_file_path = ""
        self._internal_id_to_instrument_idx: Dict[int, str] = {}
        self._idx_built = False
        self._idx_lock = threading.Lock()
        self._instrument_service = InstrumentQueryService(storage_instance)
        self._export_service = DataExportService(storage_instance)

    def _get_params_service(self):
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            return get_params_service()
        except Exception:
            logging.warning("[R22-EP-P1] QueryService exception swallowed")
            return None

    def _get_cached_instruments(self) -> Dict[str, Dict[str, Any]]:
        ps = self._get_params_service()
        return ps.get_all_instrument_cache() if ps else {}

    def _ensure_internal_id_index(self) -> Dict[int, str]:
        with self._idx_lock:
            if self._idx_built:
                return self._internal_id_to_instrument_idx

            instruments = self._get_cached_instruments()
            self._internal_id_to_instrument_idx.clear()
            for instrument_id, info in instruments.items():
                internal_id = self._get_info_internal_id(info)
                if internal_id is not None:
                    self._internal_id_to_instrument_idx[internal_id] = instrument_id
            self._idx_built = True
            return self._internal_id_to_instrument_idx

    def _get_info_internal_id(self, info: Optional[Dict[str, Any]]) -> Optional[int]:
        return self._storage._get_info_internal_id(info)

    def __getattr__(self, name):
        if hasattr(self._instrument_service, name):
            return getattr(self._instrument_service, name)
        if hasattr(self._export_service, name):
            return getattr(self._export_service, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


from ali2026v3_trading.infra.maintenance_service import StorageMaintenanceService


# ============================================================================
# 合约辅助工具函数
# ============================================================================

def resolve_subscribe_flag(params: Any, flag_name: str, legacy_flag: str, default: bool) -> bool:
    try:
        value = getattr(params, flag_name, None)
        if value is not None:
            return bool(value)
        value = getattr(params, legacy_flag, default)
        return bool(value)
    except Exception:
        return default


def is_symbol_current_or_next(symbol: str, params: Any) -> bool:
    try:
        month_mapping = getattr(params, 'month_mapping', {})
        if not month_mapping:
            return True

        from ali2026v3_trading.config.params_service import get_params_service
        ps = get_params_service()
        meta = ps.get_instrument_meta_by_id(symbol)
        if not meta:
            return True

        product = meta.get('product', '').upper()
        if product in month_mapping:
            specified_months = month_mapping[product]
            if isinstance(specified_months, (list, tuple)) and len(specified_months) >= 2:
                return symbol in [specified_months[0], specified_months[1]]
        return True
    except Exception:
        return True


def is_real_month_contract(instrument_id: str) -> bool:
    return True


_query_service_instance: Optional['QueryService'] = None

def get_query_service(storage_instance=None) -> 'QueryService':
    global _query_service_instance
    if _query_service_instance is None:
        if storage_instance is None:
            try:
                from ali2026v3_trading.data.data_manager import InstrumentDataManager
                storage_instance = InstrumentDataManager()
            except Exception:
                return None
        _query_service_instance = QueryService(storage_instance)
    return _query_service_instance


__all__ = [
    'LightKLine',
    '_KlineAggregator',
    'QueryService',
    'StorageMaintenanceService',
    'resolve_subscribe_flag',
    'is_symbol_current_or_next',
    'is_real_month_contract',
    'get_query_service',
]
