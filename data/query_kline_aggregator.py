# MODULE_ID: M1-039
"""
query_kline_aggregator.py - K线聚合器与轻量K线数据结构

从 query_service.py 拆分出的K线聚合相关功能

核心功能:
1. LightKLine 轻量K线数据结构
2. _KlineAggregator K线聚合器（从Tick合成K线）
3. 辅助函数: _normalize_code, _result_to_pylist, _table_exists
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


# ============================================================================
# 轻量 K 线数据结构（从 market_data_service.py 迁移）'
# ============================================================================

class LightKLine:
    """
    轻量级 K 线对象，使用例例slots__优化内存

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

        # ✅ BP-17修复：加锁防竞态（on_tick线程与drain线程可并发调用update）
        self._lock = threading.Lock()

        # 当前 K 线状态
        self.open_price: Optional[float] = None
        self.high_price: Optional[float] = None
        self.low_price: Optional[float] = None
        self.close_price: Optional[float] = None
        self.volume: int = 0
        self.amount: float = 0.0
        self.open_interest: Optional[int] = None

        # 当前 K 线开始时间
        self.kline_start_time: Optional[float] = None

        # R15-P0-DATA-04修复: tick乱序检测，记录上一个tick时间戳
        self._last_tick_ts: Optional[float] = None

        # 周期转换为秒
        self.period_seconds = self._parse_period(period)

    def _parse_period(self, period: str) -> int:
        """将周期字符串转换为秒数"""
        period_map = {
            '1s': 1,
            '1m': 60,
            '1min': 60,
            '5m': 300,
            '5min': 300,
            '15m': 900,
            '15min': 900,
            '30m': 1800,
            '30min': 1800,
            '60m': 3600,
            '60min': 3600,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '1d': 86400,
        }
        return period_map.get(period, 60)

    def update(self, tick_ts: float, price: float,
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """更新 Tick 数据，返回完成的 K 线（如果有）"""
        # ✅ BP-17修复：加锁防竞态
        with self._lock:
            return self._update_impl(tick_ts, price, volume, amount, open_interest)

    def _update_impl(self, tick_ts: float, price: float,
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """update的锁内实现

        R15-P0-DATA-04修复: tick乱序通过时间戳比较自动检测，无需外部传入标记
        """
        import math as _kline_math
        if isinstance(price, float) and (_kline_math.isnan(price) or _kline_math.isinf(price) or price <= 0):
            self.logger.warning("[CC-05] K线聚合拒绝NaN/Inf/负价格: instrument_id=%s price=%s", self.instrument_id, price)
            return None
        # R16-P0-DATA-04修复: tick乱序时不聚合到当前K线，避免Bar数据失真
        if self._last_tick_ts is not None and tick_ts < self._last_tick_ts:
            self.logger.warning(
                "Tick乱序检测: instrument_id=%s, "
                "当前tick_ts=%s < 上一个tick_ts=%s, 丢弃乱序tick不聚合",
                self.instrument_id, tick_ts, self._last_tick_ts
            )
            return None
        self._last_tick_ts = tick_ts

        if self.kline_start_time is None:
            # 初始化第一根 K 线
            self.kline_start_time = (tick_ts // self.period_seconds) * self.period_seconds
            self.open_price = self.high_price = self.low_price = self.close_price = price
            self.volume = volume
            self.amount = amount
            self.open_interest = open_interest
            return None

        # 检查是否应该开启新 K 线
        current_kline_end = self.kline_start_time + self.period_seconds
        if tick_ts >= current_kline_end:
            completed_kline = self._get_current_kline()
            # 开启新 K 线
            self.kline_start_time = (tick_ts // self.period_seconds) * self.period_seconds
            self.open_price = self.high_price = self.low_price = self.close_price = price
            self.volume = volume
            self.amount = amount
            self.open_interest = open_interest
            return completed_kline

        # 更新当前 K 线
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
        """获取当前完成的 K 线数据"""
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

    def reset(self):
        """重置聚合器状态"""
        self.open_price = self.high_price = self.low_price = self.close_price = None
        self.volume = 0
        self.amount = 0.0
        self.open_interest = None
        self.kline_start_time = None

    def to_state_dict(self) -> Dict[str, Any]:
        """✅ BP-17：导出聚合器状态用于持久化"""
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
        """✅ BP-17：从持久化状态恢复聚合器"""
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
