# MODULE_ID: M1-032
"""
quant_infra.py - 量化系统基础设施（合并版）

合并说明 (2026-06-30):
- 原 quant_infra.py: rate_limit_log + NumpyRingBuffer
- 原 quant_platform.py: ExchangeTime + TickAggregator + AtomicSystemState + SystemHealthMonitor ← 合入
- 原 quant_trend_scorer.py: MultiPeriodTrendScorer ← 合入
- 原 quant_volatility.py: IVSurfacePCA + VolatilityRegimeFilter ← 合入
"""
from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


_last_log: Dict[str, float] = {}
_last_log_lock = threading.Lock()


def rate_limit_log(logger: logging.Logger, level: int, msg: str,
                   key: str, min_interval: float = 60.0) -> None:
    now = time.monotonic()
    with _last_log_lock:
        last = _last_log.get(key, 0.0)
        if now - last >= min_interval:
            _last_log[key] = now
            logger.log(level, msg)


class NumpyRingBuffer:
    """
    numpy环形缓冲区，替代collections.deque。

    解决问题：deque迭代期间非原子，多线程可能读到半更新状态。
    方案：numpy固定数组+原子head指针，snapshot读取保证一致性。
    """

    __slots__ = ('_lock', '_buffer', '_capacity', '_head', '_count', '_dtype')

    def __init__(self, capacity: int, dtype: np.dtype = np.float64):
        self._lock = threading.RLock()
        self._capacity = capacity
        self._buffer = np.zeros(capacity, dtype=dtype)
        self._head = 0
        self._count = 0
        self._dtype = dtype

    def append(self, value: float) -> None:
        with self._lock:
            self._buffer[self._head] = value
            self._head = (self._head + 1) % self._capacity
            if self._count < self._capacity:
                self._count += 1

    def snapshot(self) -> np.ndarray:
        with self._lock:
            if self._count < self._capacity:
                return self._buffer[:self._count].copy()
            return np.concatenate([
                self._buffer[self._head:],
                self._buffer[:self._head],
            ]).copy()

    @property
    def count(self) -> int:
        return self._count

    def __len__(self) -> int:
        return self._count

    def sum(self) -> float:
        with self._lock:
            return float(np.sum(self._buffer[:self._count]))

    def mean(self) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            return float(np.mean(self._buffer[:self._count]))

    def std(self) -> float:
        with self._lock:
            if self._count < 2:
                return 0.0
            return float(np.std(self._buffer[:self._count]))

    def last(self) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            idx = (self._head - 1) % self._capacity
            return float(self._buffer[idx])

    def sorted_values(self) -> np.ndarray:
        with self._lock:
            return np.sort(self._buffer[:self._count])

    def percentile(self, p: float) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            sorted_arr = np.sort(self._buffer[:self._count])
            idx = min(int(self._count * p / 100.0), self._count - 1)
            return float(sorted_arr[idx])

    def to_list(self) -> List[float]:
        return self.snapshot().tolist()


# ============================================================================
# 量化平台层（合入自 quant_platform.py）
# ============================================================================

class ExchangeTime:
    """交易所时间标准化，处理CST时区和夜盘跨天trade_date。"""

    _CST = None
    _NIGHT_START = 21
    _DAY_START = 9

    __slots__ = ('_exchange', '_night_end_hour')

    def __init__(self, exchange: str = 'DCE', night_end_hour: int = 0):
        self._exchange = exchange
        self._night_end_hour = night_end_hour
        if ExchangeTime._CST is None:
            from ali2026v3_trading.infra.shared_utils import CHINA_TZ
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
        import math as _tick_math
        if not isinstance(price, (int, float)) or not _tick_math.isfinite(price) or price <= 0:
            return None

        with self._lock:
            tick_vol = volume
            if self._volume_mode == 'delta':
                if self._last_cum_volume is not None:
                    tick_vol = max(0, volume - self._last_cum_volume)
                else:
                    tick_vol = 0
                self._last_cum_volume = volume
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
        with self._lock:
            return self._version

    @property
    def age_ms(self) -> float:
        with self._lock:
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


# ============================================================================
# 趋势评分器（合入自 quant_trend_scorer.py）
# ============================================================================

class MultiPeriodTrendScorer:
    """
    多周期趋势评分器。

    P0修复：Wilder初始化使用前adx_period个值的简单平均，
    而非第一个TR直接赋值（导致ATR/DM初始值偏差巨大）。
    """

    __slots__ = (
        '_lock', '_periods', '_weights', '_wilder_alpha', '_ema_alphas', '_adx_period',
        '_atr_buffers', '_plus_dm_buffers', '_minus_dm_buffers',
        '_dx_buffers', '_adx_buffers', '_ema_buffers',
        '_vol_buffers', '_dynamic_periods',
        '_prices', '_highs', '_lows',
        '_initialized', '_tick_count',
        '_init_tr_accum', '_init_plus_dm_accum', '_init_minus_dm_accum', '_init_count',
    )

    def __init__(self, periods: Tuple[int, ...] = (5, 20, 60),
                 weights: Tuple[float, ...] = (0.2, 0.5, 0.3),
                 adx_period: int = 14):
        self._lock = threading.RLock()
        self._periods = periods
        self._weights = np.array(weights, dtype=np.float64)
        self._weights /= self._weights.sum()
        self._wilder_alpha = 1.0 / adx_period
        self._adx_period = adx_period
        self._ema_alphas = [2.0 / (p + 1) for p in periods]

        n = len(periods)
        self._atr_buffers = [0.0] * n
        self._plus_dm_buffers = [0.0] * n
        self._minus_dm_buffers = [0.0] * n
        self._dx_buffers = [0.0] * n
        self._adx_buffers = [0.0] * n
        self._ema_buffers = [0.0] * n
        self._vol_buffers = [NumpyRingBuffer(20) for _ in range(n)]
        self._dynamic_periods = list(periods)

        self._prices = NumpyRingBuffer(3)
        self._highs = NumpyRingBuffer(3)
        self._lows = NumpyRingBuffer(3)
        self._initialized = False
        self._tick_count = 0
        self._init_tr_accum = [0.0] * n
        self._init_plus_dm_accum = [0.0] * n
        self._init_minus_dm_accum = [0.0] * n
        self._init_count = 0

    def update(self, high: float, low: float, close: float) -> Dict[str, Any]:
        import math as _ts_math
        if (close <= 0 or high < low
            or (isinstance(close, float) and (_ts_math.isnan(close) or _ts_math.isinf(close)))
            or (isinstance(high, float) and (_ts_math.isnan(high) or _ts_math.isinf(high)))
            or (isinstance(low, float) and (_ts_math.isnan(low) or _ts_math.isinf(low)))):
            return self._empty_result()
        with self._lock:
            self._prices.append(close)
            self._highs.append(high)
            self._lows.append(low)
            self._tick_count += 1
            if len(self._prices) < 3:
                return self._empty_result()
            return self._compute()

    def _compute(self) -> Dict[str, Any]:
        prices_snap = self._prices.snapshot()
        highs_snap = self._highs.snapshot()
        lows_snap = self._lows.snapshot()

        prev_high, prev_low = highs_snap[-2], lows_snap[-2]
        curr_high, curr_low = highs_snap[-1], lows_snap[-1]
        curr_close, prev_close = prices_snap[-1], prices_snap[-2]

        tr = max(curr_high - curr_low, abs(curr_high - prev_close), abs(curr_low - prev_close))
        up_move = curr_high - prev_high
        down_move = prev_low - curr_low
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0

        scores = [0.0] * len(self._periods)
        adx_values = [0.0] * len(self._periods)
        ema_values = [0.0] * len(self._periods)

        for i in range(len(self._periods)):
            if not self._initialized:
                self._init_tr_accum[i] += tr
                self._init_plus_dm_accum[i] += plus_dm
                self._init_minus_dm_accum[i] += minus_dm
                self._ema_buffers[i] = curr_close
                continue

            a = self._wilder_alpha
            self._atr_buffers[i] = a * tr + (1 - a) * self._atr_buffers[i]
            self._plus_dm_buffers[i] = a * plus_dm + (1 - a) * self._plus_dm_buffers[i]
            self._minus_dm_buffers[i] = a * minus_dm + (1 - a) * self._minus_dm_buffers[i]

            atr = self._atr_buffers[i]
            if atr > 0:
                plus_di = 100.0 * self._plus_dm_buffers[i] / atr
                minus_di = 100.0 * self._minus_dm_buffers[i] / atr
                di_sum = plus_di + minus_di
                dx = 100.0 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0.0
            else:
                plus_di = minus_di = dx = 0.0

            self._dx_buffers[i] = dx
            self._adx_buffers[i] = a * dx + (1 - a) * self._adx_buffers[i]

            momentum = (curr_close - prev_close) / prev_close if prev_close > 1e-10 else 0.0
            adx_enhanced = min(self._adx_buffers[i] * (1.0 + 0.3 * abs(momentum) * 100.0), 100.0)

            self._vol_buffers[i].append(tr)
            vbuf = self._vol_buffers[i]
            cv = vbuf.std() / vbuf.mean() if len(vbuf) > 2 and vbuf.mean() > 0 else 1.0
            # FIX-20260710-NAN: 防止NaN/Inf导致int()转换失败 (15360 warnings根因)
            if cv != cv or cv == float('inf') or cv == float('-inf'):
                cv = 1.0
            cv = min(cv, 3.0)

            dynamic_period = max(3, int(self._periods[i] / (1.0 + cv)))
            self._dynamic_periods[i] = dynamic_period
            dynamic_alpha = 2.0 / (dynamic_period + 1)
            self._ema_buffers[i] = dynamic_alpha * curr_close + (1 - dynamic_alpha) * self._ema_buffers[i]

            trend_dir = 1.0 if curr_close > self._ema_buffers[i] else -1.0
            scores[i] = trend_dir * adx_enhanced / 100.0
            adx_values[i] = adx_enhanced
            ema_values[i] = self._ema_buffers[i]

        if not self._initialized and self._tick_count >= 3:
            self._init_count += 1
            if self._init_count >= self._adx_period:
                for i in range(len(self._periods)):
                    cnt = max(self._init_count, 1)
                    self._atr_buffers[i] = self._init_tr_accum[i] / cnt
                    self._plus_dm_buffers[i] = self._init_plus_dm_accum[i] / cnt
                    self._minus_dm_buffers[i] = self._init_minus_dm_accum[i] / cnt
                    dx_sum = self._plus_dm_buffers[i] + self._minus_dm_buffers[i]
                    if dx_sum > 0 and self._atr_buffers[i] > 0:
                        plus_di = 100.0 * self._plus_dm_buffers[i] / self._atr_buffers[i]
                        minus_di = 100.0 * self._minus_dm_buffers[i] / self._atr_buffers[i]
                        self._dx_buffers[i] = 100.0 * abs(plus_di - minus_di) / (plus_di + minus_di)
                    self._adx_buffers[i] = self._dx_buffers[i]
                self._initialized = True

        composite = max(-1.0, min(1.0, sum(w * s for w, s in zip(self._weights, scores))))
        return {
            'composite_score': composite,
            'trend_direction': 'UP' if composite > 0.05 else ('DOWN' if composite < -0.05 else 'FLAT'),
            'strength': abs(composite),
            'period_scores': scores,
            'adx_values': adx_values,
            'ema_values': ema_values,
            'dynamic_periods': list(self._dynamic_periods),
            'ema_warmup': not self._initialized,
        }

    def _empty_result(self) -> Dict[str, Any]:
        n = len(self._periods)
        return {
            'composite_score': 0.0, 'trend_direction': 'FLAT', 'strength': 0.0,
            'period_scores': [0.0] * n, 'adx_values': [0.0] * n,
            'ema_values': [0.0] * n, 'dynamic_periods': list(self._periods),
        }


# ============================================================================
# 波动率分析（合入自 quant_volatility.py）
# ============================================================================

try:
    from scipy.linalg import eigh as _scipy_eigh
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
    _scipy_eigh = None


class IVSurfacePCA:
    """
    IV曲面PCA，Ledoit-Wolf收缩+EWMA均值替代简单均值。
    """

    __slots__ = (
        '_lock', '_window', '_shrinkage', '_var_explained_threshold',
        '_max_components', '_data_buffer', '_strike_labels', '_strike_labels_hash',
        '_components', '_explained_ratio', '_mean_iv', '_ewma_alpha',
        '_projection', '_is_fitted', '_update_counter', '_refit_interval',
    )

    def __init__(self, window: int = 120, shrinkage: float = 0.5,
                 var_explained_threshold: float = 0.90, max_components: int = 3,
                 refit_interval: int = 10, ewma_alpha: float = 0.06):
        self._lock = threading.RLock()
        self._window = window
        self._shrinkage = shrinkage
        self._var_explained_threshold = var_explained_threshold
        self._max_components = max_components
        self._refit_interval = refit_interval
        self._ewma_alpha = ewma_alpha
        self._data_buffer: deque = deque(maxlen=window)
        self._strike_labels: List[str] = []
        self._strike_labels_hash: int = 0
        self._components: Optional[np.ndarray] = None
        self._explained_ratio: Optional[np.ndarray] = None
        self._mean_iv: Optional[np.ndarray] = None
        self._projection: Optional[np.ndarray] = None
        self._is_fitted = False
        self._update_counter = 0

    def update(self, iv_surface: Dict[str, float]) -> Dict[str, Any]:
        if not iv_surface:
            return self._empty_result()
        with self._lock:
            new_hash = hash(frozenset(iv_surface.keys()))
            if not self._strike_labels:
                self._strike_labels = sorted(iv_surface.keys())
                self._strike_labels_hash = new_hash
            elif new_hash != self._strike_labels_hash:
                self._strike_labels = sorted(iv_surface.keys())
                self._strike_labels_hash = new_hash
                self._is_fitted = False

            vec = np.array([iv_surface.get(k, 0.0) for k in self._strike_labels], dtype=np.float64)
            if len(vec) != len(self._strike_labels) or np.any(vec <= 0):
                return self._empty_result()

            self._data_buffer.append(vec)
            self._update_counter += 1
            if len(self._data_buffer) < 10:
                return self._empty_result()
            if not self._is_fitted or self._update_counter >= self._refit_interval:
                self._fit()
                self._update_counter = 0

            if self._components is not None and self._mean_iv is not None:
                self._mean_iv = self._ewma_alpha * vec + (1 - self._ewma_alpha) * self._mean_iv
                self._projection = (vec - self._mean_iv) @ self._components

            return self._build_result()

    def _fit(self) -> None:
        data = np.array(self._data_buffer, dtype=np.float64)
        if data.shape[0] < 5:
            return
        self._mean_iv = np.mean(data, axis=0)
        centered = data - self._mean_iv
        n, p = centered.shape
        sample_cov = (centered.T @ centered) / max(n - 1, 1)
        shrunk_cov = (1 - self._shrinkage) * sample_cov + self._shrinkage * np.diag(np.diag(sample_cov))
        try:
            if _HAS_SCIPY and _scipy_eigh is not None:
                eigenvalues, eigenvectors = _scipy_eigh(shrunk_cov)
            else:
                eigenvalues, eigenvectors = np.linalg.eigh(shrunk_cov)
        except np.linalg.LinAlgError:
            return
        assert np.all(np.isreal(eigenvalues)), "[R21-MATH-P2-06修复] eigh返回复数特征值，数值异常"
        eigenvalues = np.real(eigenvalues)
        _min_eig = np.min(eigenvalues)
        if _min_eig <= 0:
            _reg = abs(_min_eig) + 1e-8
            shrunk_cov += _reg * np.eye(p)
            try:
                if _HAS_SCIPY and _scipy_eigh is not None:
                    eigenvalues, eigenvectors = _scipy_eigh(shrunk_cov)
                else:
                    eigenvalues, eigenvectors = np.linalg.eigh(shrunk_cov)
            except np.linalg.LinAlgError:
                return
            eigenvalues = np.real(eigenvalues)
        idx = np.argsort(eigenvalues)[::-1]
        eigenvalues, eigenvectors = eigenvalues[idx], eigenvectors[:, idx]
        pos = eigenvalues > 1e-10
        eigenvalues, eigenvectors = eigenvalues[pos], eigenvectors[:, pos]
        if len(eigenvalues) == 0:
            return
        total_var = np.sum(eigenvalues)
        cum_var = np.cumsum(eigenvalues) / total_var
        nc = min(int(np.searchsorted(cum_var, self._var_explained_threshold) + 1),
                 self._max_components, len(eigenvalues))
        nc = max(nc, 1)
        self._components = eigenvectors[:, :nc]
        self._explained_ratio = eigenvalues[:nc] / total_var
        self._is_fitted = True

    def _build_result(self) -> Dict[str, Any]:
        if self._components is None:
            return self._empty_result()
        nc = self._components.shape[1]
        labels = ['Level', 'Slope', 'Curvature', 'PC4', 'PC5'][:nc]
        proj = self._projection.tolist() if self._projection is not None else [0.0] * nc
        return {
            'n_components': nc, 'component_labels': labels,
            'explained_variance_ratio': self._explained_ratio.tolist(),
            'total_explained': float(np.sum(self._explained_ratio)),
            'projections': proj,
            'mean_iv': self._mean_iv.tolist() if self._mean_iv is not None else [],
            'is_fitted': True,
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            'n_components': 0, 'component_labels': [],
            'explained_variance_ratio': [], 'total_explained': 0.0,
            'projections': [], 'mean_iv': [], 'is_fitted': False,
        }


class VolatilityRegimeFilter:
    """波动率环境过滤，EWMA RV+百分位自适应阈值+最小持仓tick。"""

    __slots__ = (
        '_lock', '_lookback', '_low_pct', '_high_pct', '_min_hold_ticks',
        '_rv_buffer', '_current_rv', '_current_regime', '_regime_hold_count',
        '_p25', '_p50', '_p75', '_update_count', '_sort_interval',
        '_ewma_rv', '_ewma_rv_corrected', '_ewma_alpha', '_ewma_step',
    )

    def __init__(self, lookback: int = 100, low_percentile: float = 25.0,
                 high_percentile: float = 75.0, min_hold_ticks: int = 20,
                 ewma_alpha: float = 0.06):
        self._lock = threading.RLock()
        self._lookback = lookback
        self._low_pct = low_percentile / 100.0
        self._high_pct = high_percentile / 100.0
        self._min_hold_ticks = min_hold_ticks
        self._rv_buffer = NumpyRingBuffer(lookback)
        self._current_rv = 0.0
        self._current_regime = 1
        self._regime_hold_count = 0
        self._p25 = self._p50 = self._p75 = 0.0
        self._update_count = 0
        self._sort_interval = 20
        self._ewma_rv = 0.0
        self._ewma_rv_corrected = 0.0
        self._ewma_alpha = ewma_alpha
        self._ewma_step = 0

    def update(self, return_value: float) -> Dict[str, Any]:
        with self._lock:
            self._rv_buffer.append(return_value ** 2)
            self._update_count += 1
            if len(self._rv_buffer) < 5:
                return self._empty_result()
            _buf_sum = self._rv_buffer.sum()
            _buf_len = len(self._rv_buffer)
            self._current_rv = math.sqrt(_buf_sum / _buf_len) if _buf_len > 0 and _buf_sum >= 0 else 0.0
            _prev_ewma = self._ewma_rv
            a = self._ewma_alpha
            self._ewma_step += 1
            _bias_correction = 1.0 - (1.0 - a) ** self._ewma_step if self._ewma_step > 0 else 1.0
            self._ewma_rv = a * self._current_rv + (1 - a) * self._ewma_rv if self._ewma_rv > 0 and self._current_rv > 0 else (self._current_rv if self._ewma_rv <= 0 else self._ewma_rv)
            self._ewma_rv_corrected = self._ewma_rv / _bias_correction if _bias_correction > 1e-10 else self._ewma_rv
            if _prev_ewma > 1e-10 and self._ewma_rv > 1e-10:
                _rv_ratio = self._ewma_rv / _prev_ewma
                if _rv_ratio > 3.0 or _rv_ratio < 0.33:
                    logging.warning("[R24-P2-IV-10] 波动率异常跳变: prev=%.6f curr=%.6f ratio=%.2f",
                                  _prev_ewma, self._ewma_rv, _rv_ratio)
            if self._update_count % self._sort_interval == 0:
                sorted_rv = self._rv_buffer.sorted_values()
                n = len(sorted_rv)
                if n > 0:
                    self._p25 = math.sqrt(sorted_rv[int(n * self._low_pct)])
                    self._p50 = math.sqrt(sorted_rv[int(n * 0.5)])
                    self._p75 = math.sqrt(sorted_rv[int(n * self._high_pct)])
            _ewma_for_regime = self._ewma_rv_corrected if hasattr(self, '_ewma_rv_corrected') else self._ewma_rv
            new_regime = 0 if _ewma_for_regime <= self._p25 else (2 if _ewma_for_regime >= self._p75 else 1)
            if new_regime != self._current_regime:
                if self._regime_hold_count >= self._min_hold_ticks:
                    self._current_regime = new_regime
                    self._regime_hold_count = 0
            else:
                self._regime_hold_count += 1
            return self._build_result()

    def _build_result(self) -> Dict[str, Any]:
        _ewma_vol = self._ewma_rv_corrected if hasattr(self, '_ewma_rv_corrected') else self._ewma_rv
        return {
            'regime': self._current_regime,
            'regime_label': ['LOW', 'NORMAL', 'HIGH'][self._current_regime],
            'realized_vol': self._current_rv, 'ewma_vol': _ewma_vol,
            'low_threshold': self._p25, 'high_threshold': self._p75,
            'p25': self._p25, 'p50': self._p50, 'p75': self._p75,
            'hold_count': self._regime_hold_count,
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            'regime': 1, 'regime_label': 'NORMAL',
            'realized_vol': 0.0, 'ewma_vol': 0.0,
            'low_threshold': 0.0, 'high_threshold': 0.0,
            'p25': 0.0, 'p50': 0.0, 'p75': 0.0, 'hold_count': 0,
        }
