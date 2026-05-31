"""
quant_platform.py - 量化系统平台层

包含：
- ExchangeTime: 交易所时间标准化（时区+精度+夜盘跨天）
- TickAggregator: Tick→Bar聚合（累计量转逐笔+夜盘跨天处理）
- AtomicSystemState: 全局事务快照（原子一致性）
- SystemHealthMonitor: 健康检查+日志限流+P50/P95/P99分位数
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

try:
    from .quant_infra import rate_limit_log
except ImportError:
    from quant_infra import rate_limit_log


class ExchangeTime:
    """交易所时间标准化，处理CST时区和夜盘跨天trade_date。"""

    _CST = None  # [R22-TIME-P1-01] 延迟初始化为CHINA_TZ
    _NIGHT_START = 21
    _DAY_START = 9

    __slots__ = ('_exchange', '_night_end_hour')

    # [R16-P2-1.3修复] 夜盘结束时间默认0点(凌晨)，23点仍属夜盘交易时段
    def __init__(self, exchange: str = 'DCE', night_end_hour: int = 0):
        self._exchange = exchange
        self._night_end_hour = night_end_hour
        if ExchangeTime._CST is None:  # [R22-TIME-P1-01] 延迟初始化
            from ali2026v3_trading.shared_utils import CHINA_TZ
            ExchangeTime._CST = CHINA_TZ

    def now_cst(self) -> datetime:
        return datetime.now(self._CST)

    def get_trade_date(self, dt: Optional[datetime] = None) -> str:
        if dt is None:
            dt = self.now_cst()
        hour = dt.hour
        if hour >= self._NIGHT_START:
            return (dt + timedelta(days=1)).strftime('%Y-%m-%d')
        return dt.strftime('%Y-%m-%d')

    def is_trading_hours(self, dt: Optional[datetime] = None) -> bool:
        if dt is None:
            dt = self.now_cst()
        h = dt.hour
        m = dt.minute
        if self._DAY_START <= h < 15:
            return True
        if h == 15 and m == 0:
            return True
        if h >= self._NIGHT_START or (self._night_end_hour < self._DAY_START and h < self._night_end_hour):
            return True
        return False

    def to_epoch_ms(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    def from_epoch_ms(self, epoch_ms: int) -> datetime:
        # R23-P2-05修复: 参数校验，自动检测毫秒/秒格式
        if epoch_ms > 1e12:
            epoch_ms = epoch_ms / 1000.0
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=self._CST)


class TickAggregator:
    """
    Tick→Bar聚合器。

    volume_mode:
    - 'delta': 传入CTP累计成交量，自动计算差值（默认，适配CTP推送）
    - 'tick':  传入逐笔成交量，直接使用
    """

    __slots__ = ('_lock', '_interval_sec', '_vol_threshold', '_exchange_time',
                 '_current_bar', '_bar_history', '_max_history',
                 '_last_cum_volume', '_volume_mode')

    def __init__(self, interval_sec: int = 300, vol_threshold: int = 0,
                 exchange_time: Optional[ExchangeTime] = None,
                 max_history: int = 500,
                 volume_mode: str = 'delta'):
        self._lock = threading.RLock()
        self._interval_sec = interval_sec
        self._vol_threshold = vol_threshold
        self._exchange_time = exchange_time or ExchangeTime()
        self._current_bar: Optional[Dict[str, Any]] = None
        self._bar_history: List[Dict[str, Any]] = []
        self._max_history = max_history
        self._last_cum_volume: Optional[int] = None
        self._volume_mode = volume_mode

    def update_tick(self, price: float, volume: int,
                    timestamp_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        # R24-P1-IV-01修复: NaN/Inf价格直接丢弃
        import math
        if isinstance(price, float) and (math.isnan(price) or math.isinf(price)):
            return None
        if price <= 0:
            return None

        with self._lock:
            tick_vol = volume
            if self._volume_mode == 'delta':
                if self._last_cum_volume is not None:
                    tick_vol = max(0, volume - self._last_cum_volume)
                else:
                    tick_vol = 0
                self._last_cum_volume = volume
            # R16-P1-1.1修复: delta模式首tick volume=0保护
            if tick_vol == 0 and self._current_bar is None:
                tick_vol = max(1, getattr(volume, 'last_volume', 1) if not isinstance(volume, int) else 1)

            now_ms = timestamp_ms or int(time.time() * 1000)
            dt = self._exchange_time.from_epoch_ms(now_ms)
            trade_date = self._exchange_time.get_trade_date(dt)
            bar_start = self._bar_start_time(dt)

            completed_bar = None
            if self._current_bar is not None:
                if self._should_close_bar(bar_start, tick_vol):
                    completed_bar = self._current_bar
                    self._bar_history.append(completed_bar)
                    if len(self._bar_history) > self._max_history:
                        self._bar_history = self._bar_history[-self._max_history:]
                    self._current_bar = None
                    self._last_cum_volume = None

            if self._current_bar is None:
                self._current_bar = {
                    'open': price, 'high': price, 'low': price, 'close': price,
                    'volume': tick_vol, 'vwap': price,
                    'trade_date': trade_date,
                    'bar_start_ms': now_ms,
                    'bar_start': bar_start.strftime('%Y-%m-%d %H:%M:%S'),
                    'tick_count': 1,
                }
            else:
                self._current_bar['high'] = max(self._current_bar['high'], price)
                self._current_bar['low'] = min(self._current_bar['low'], price)
                self._current_bar['close'] = price
                old_vol = self._current_bar['volume']
                new_vol = old_vol + tick_vol
                self._current_bar['vwap'] = (
                    (self._current_bar['vwap'] * old_vol + price * tick_vol) / new_vol
                    if new_vol > 0 else price
                )
                self._current_bar['volume'] = new_vol
                self._current_bar['tick_count'] += 1

            return completed_bar

    def _should_close_bar(self, bar_start: datetime, new_vol: int) -> bool:
        if self._current_bar is None:
            return False
        # [R16-P2-1.2修复] 使用时间戳数值比较替代strftime字符串比对，避免格式差异导致误判
        cur_start_ms = self._current_bar.get('bar_start_ms', 0)
        _bar_ts = int(bar_start.timestamp() * 1000) if bar_start is not None else 0
        if cur_start_ms != _bar_ts:
            return True
        if self._vol_threshold > 0 and self._current_bar['volume'] >= self._vol_threshold:
            return True
        return False

    def _bar_start_time(self, dt: datetime) -> datetime:
        total_sec = dt.hour * 3600 + dt.minute * 60 + dt.second
        night_start = self._exchange_time._NIGHT_START
        day_start = self._exchange_time._DAY_START
        night_end = self._exchange_time._night_end_hour

        if dt.hour >= night_start or dt.hour < night_end:
            if dt.hour >= night_start:
                elapsed = total_sec - night_start * 3600
            else:
                elapsed = total_sec + (24 - night_start) * 3600
            bar_sec = elapsed // self._interval_sec
            result_elapsed = bar_sec * self._interval_sec
            result_hour = night_start + result_elapsed // 3600
            result_minute = (result_elapsed % 3600) // 60
            if result_hour >= 24:
                result_hour -= 24
            return dt.replace(hour=result_hour, minute=result_minute, second=0, microsecond=0)

        bar_sec = (total_sec - day_start * 3600) // self._interval_sec
        return dt.replace(
            hour=day_start + bar_sec * self._interval_sec // 3600,
            minute=(bar_sec * self._interval_sec % 3600) // 60,
            second=0, microsecond=0
        )

    def get_current_bar(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return dict(self._current_bar) if self._current_bar else None

    def get_bar_history(self, n: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._bar_history[-n:])


class AtomicSystemState:
    """全局事务快照，保证跨模块状态一致性。"""

    __slots__ = ('_lock', '_snapshot', '_version', '_last_update_ms')

    def __init__(self):
        self._lock = threading.RLock()
        self._snapshot: Dict[str, Any] = {}
        self._version = 0
        self._last_update_ms = 0

    def capture(self, **module_states: Any) -> int:
        with self._lock:
            self._version += 1
            self._snapshot = {
                'version': self._version,
                'timestamp_ms': int(time.time() * 1000),
                **module_states,
            }
            self._last_update_ms = self._snapshot['timestamp_ms']
            return self._version

    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._snapshot)

    @property
    def version(self) -> int:
        return self._version

    @property
    def age_ms(self) -> float:
        if self._last_update_ms == 0:
            return float('inf')
        return (time.time() * 1000) - self._last_update_ms


class SystemHealthMonitor:
    """健康检查+延迟追踪+P50/P95/P99分位数。"""

    __slots__ = ('_lock', '_latency_thresholds', '_error_counts',
                 '_module_stats', '_start_time')

    def __init__(self, latency_thresholds: Optional[Dict[str, float]] = None):
        self._lock = threading.RLock()
        self._latency_thresholds = latency_thresholds or {
            'trend_scorer': 200.0, 'iv_pca': 500.0,
            'hmm': 100.0, 'vol_filter': 30.0,
            'coint_scanner': 2000.0, 'survival': 100000.0,
        }
        self._error_counts: Dict[str, int] = {}
        self._module_stats: Dict[str, Dict[str, Any]] = {}
        self._start_time = time.monotonic()

    def record_latency(self, module: str, latency_us: float) -> None:
        threshold = self._latency_thresholds.get(module, 1000.0)
        with self._lock:
            if module not in self._module_stats:
                self._module_stats[module] = {
                    'count': 0, 'total_us': 0.0,
                    'max_us': 0.0, 'over_threshold': 0,
                    'latencies': [],
                }
            stats = self._module_stats[module]
            stats['count'] += 1
            stats['total_us'] += latency_us
            stats['max_us'] = max(stats['max_us'], latency_us)
            if latency_us > threshold:
                stats['over_threshold'] += 1
                rate_limit_log(
                    logging.getLogger(), logging.WARNING,
                    f"[HealthMonitor] {module} latency {latency_us:.0f}us > threshold {threshold:.0f}us",
                    f"latency_{module}", min_interval=60.0,
                )
            latencies = stats['latencies']
            latencies.append(latency_us)
            if len(latencies) > 200:
                stats['latencies'] = latencies[-200:]

    def record_error(self, module: str, error: str) -> None:
        with self._lock:
            key = f"error_{module}"
            self._error_counts[key] = self._error_counts.get(key, 0) + 1
            count = self._error_counts[key]
            if count >= 10:
                rate_limit_log(
                    logging.getLogger(), logging.ERROR,
                    f"[HealthMonitor] {module} has {count} errors, latest: {error}",
                    f"error_{module}", min_interval=300.0,
                )

    @staticmethod
    def _percentile(sorted_arr: list, p: float) -> float:
        if not sorted_arr:
            return 0.0
        idx = min(int(len(sorted_arr) * p / 100.0), len(sorted_arr) - 1)
        return sorted_arr[idx]

    def get_health_report(self) -> Dict[str, Any]:
        with self._lock:
            uptime_sec = time.monotonic() - self._start_time
            report = {'uptime_sec': uptime_sec, 'modules': {}}
            for module, stats in self._module_stats.items():
                avg_us = stats['total_us'] / stats['count'] if stats['count'] > 0 else 0
                latencies = sorted(stats.get('latencies', []))
                report['modules'][module] = {
                    'avg_latency_us': avg_us,
                    'max_latency_us': stats['max_us'],
                    'p50_latency_us': self._percentile(latencies, 50.0),
                    'p95_latency_us': self._percentile(latencies, 95.0),
                    'p99_latency_us': self._percentile(latencies, 99.0),
                    'call_count': stats['count'],
                    'over_threshold_count': stats['over_threshold'],
                    'error_count': self._error_counts.get(f"error_{module}", 0),
                }
            return report
