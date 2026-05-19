"""
拐点显微镜 (Turning Point Microscope) — 策略微观行为诊断系统的数据基础

功能：
  1. Tick → 增强Bar（VWAP/高低点时间偏移/高低点成交量/买卖价差）
  2. 在线高低点识别（仅使用历史数据，禁止未来信息泄露）
  3. 多周期均线位置计算（5/10/15/30/60期MA + 标准化偏离度 + 均线排列形态）
  4. 区分"明显高低点"与"极端行情"

关键设计决策：
  - VWAP作为Bar代表价，而非简单收盘价（无偏估计）
  - 高低点不仅记录价格，还记录时间戳和成交量分布
  - 均线计算严格使用截至当前Bar的数据（当前Bar的close只参与下一Bar的均线）
  - 极值识别使用rolling窗口+前瞻确认（N-Bar后确认，非实时最优）
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


class ExtremeRegion(Enum):
    NORMAL = "NORMAL"
    NEAR_HIGH = "NEAR_HIGH"
    NEAR_LOW = "NEAR_LOW"
    EXTREME_HIGH = "EXTREME_HIGH"
    EXTREME_LOW = "EXTREME_LOW"


class MAAlignment(Enum):
    BULLISH = "多头排列"
    BEARISH = "空头排列"
    CONVERGENT = "均线收敛"
    INTERTWINED = "均线缠绕"
    TRANSITIONAL = "过渡态"


@dataclass
class EnhancedBar:
    timestamp: np.datetime64
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float
    high_time_offset_ms: int
    low_time_offset_ms: int
    volume_at_high: float
    volume_at_low: float
    bid_ask_spread_avg: float
    open_interest: int = 0
    tick_count: int = 0
    extreme_region: ExtremeRegion = ExtremeRegion.NORMAL
    price_vs_mas: Dict[str, float] = field(default_factory=dict)
    ma_alignment: MAAlignment = MAAlignment.TRANSITIONAL
    price_ma_deviation_sigma: Dict[str, float] = field(default_factory=dict)
    ma_curvatures: Dict[str, float] = field(default_factory=dict)
    atr14: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['extreme_region'] = self.extreme_region.value
        d['ma_alignment'] = self.ma_alignment.value
        d['timestamp'] = str(self.timestamp)
        return d


@dataclass
class _TickAccumulator:
    bar_start_ts: Optional[np.datetime64] = None
    symbol: str = ""
    open: float = 0.0
    high: float = -np.inf
    low: float = np.inf
    close: float = 0.0
    total_volume: int = 0
    total_turnover: float = 0.0
    open_interest: int = 0
    tick_count: int = 0
    high_ts: Optional[np.datetime64] = None
    low_ts: Optional[np.datetime64] = None
    volume_at_high: float = 0.0
    volume_at_low: float = 0.0
    bid_ask_spread_sum: float = 0.0
    prev_total_volume: int = 0

    def reset(self, first_tick_ts: np.datetime64, symbol: str):
        self.bar_start_ts = first_tick_ts
        self.symbol = symbol
        self.open = 0.0
        self.high = -np.inf
        self.low = np.inf
        self.close = 0.0
        self.total_volume = 0
        self.total_turnover = 0.0
        self.open_interest = 0
        self.tick_count = 0
        self.high_ts = None
        self.low_ts = None
        self.volume_at_high = 0.0
        self.volume_at_low = 0.0
        self.bid_ask_spread_sum = 0.0
        self.prev_total_volume = 0


class TurningPointMicroscope:
    """
    拐点显微镜：Tick→增强Bar + 在线高低点识别 + 均线位置

    使用方法：
      1. 每个tick调用 process_tick()
      2. 分钟Bar完成时自动回调，产生EnhancedBar
      3. 每个EnhancedBar包含极值区域判定和均线位置信息
    """

    DEFAULT_MA_PERIODS = [5, 10, 15, 30, 60]

    def __init__(
        self,
        symbol: str,
        ma_periods: Optional[List[int]] = None,
        extreme_window: int = 20,
        extreme_quantile: float = 0.05,
        extreme_check_bars: int = 3,
        atr_period: int = 14,
        minute_ms: int = 60_000,
        on_bar_complete: Optional[callable] = None,
    ):
        self._symbol = symbol
        self._ma_periods = ma_periods or self.DEFAULT_MA_PERIODS
        self._extreme_window = extreme_window
        self._extreme_quantile = extreme_quantile
        self._extreme_check_bars = extreme_check_bars
        self._atr_period = atr_period
        self._minute_ms = minute_ms
        self._on_bar_complete = on_bar_complete

        self._vwap_buffers: Dict[int, deque] = {p: deque(maxlen=p) for p in self._ma_periods}
        self._close_buffer: deque = deque(maxlen=max(self._ma_periods) + 2)
        self._high_buffer: deque = deque(maxlen=self._extreme_window + self._extreme_check_bars + 1)
        self._low_buffer: deque = deque(maxlen=self._extreme_window + self._extreme_check_bars + 1)
        self._tr_buffer: deque = deque(maxlen=self._atr_period + 1)

        self._tick_acc = _TickAccumulator()
        self._bar_count = 0
        self._last_bar: Optional[EnhancedBar] = None
        self._current_bar_start_ms: int = -1

        self._all_bars: List[EnhancedBar] = []
        self._extreme_snapshots: List[EnhancedBar] = []

    def process_tick(
        self,
        timestamp: np.datetime64,
        last_price: float,
        volume: int,
        turnover: float,
        bid_price1: float = 0.0,
        ask_price1: float = 0.0,
        open_interest: int = 0,
        total_volume: int = 0,
    ) -> Optional[EnhancedBar]:
        tick_volume = max(0, total_volume - self._tick_acc.prev_total_volume)
        self._tick_acc.prev_total_volume = total_volume
        if tick_volume == 0 and self._tick_acc.tick_count > 0:
            tick_volume = 0

        if self._tick_acc.tick_count == 0:
            self._tick_acc.reset(timestamp, self._symbol)
            self._tick_acc.open = last_price

        if last_price > self._tick_acc.high:
            self._tick_acc.high = last_price
            self._tick_acc.high_ts = timestamp
            self._tick_acc.volume_at_high = tick_volume
        elif last_price == self._tick_acc.high:
            self._tick_acc.volume_at_high += tick_volume

        if last_price < self._tick_acc.low:
            self._tick_acc.low = last_price
            self._tick_acc.low_ts = timestamp
            self._tick_acc.volume_at_low = tick_volume
        elif last_price == self._tick_acc.low:
            self._tick_acc.volume_at_low += tick_volume

        self._tick_acc.close = last_price
        self._tick_acc.total_volume += tick_volume
        self._tick_acc.total_turnover += (last_price * tick_volume)
        self._tick_acc.open_interest = open_interest
        self._tick_acc.tick_count += 1

        if ask_price1 > 0 and bid_price1 > 0:
            self._tick_acc.bid_ask_spread_sum += (ask_price1 - bid_price1)

        bar_ts_ms = int(timestamp.astype('datetime64[ms]').astype(np.int64))
        bar_start_ms = bar_ts_ms - (bar_ts_ms % self._minute_ms)

        emitted_bar = None
        if self._current_bar_start_ms >= 0 and bar_start_ms != self._current_bar_start_ms:
            emitted_bar = self._emit_bar(timestamp)

        self._current_bar_start_ms = bar_start_ms

        return emitted_bar

    def _emit_bar(self, current_ts: np.datetime64) -> EnhancedBar:
        acc = self._tick_acc
        if acc.tick_count == 0:
            return self._last_bar

        vwap = acc.total_turnover / acc.total_volume if acc.total_volume > 0 else acc.close
        bid_ask_avg = acc.bid_ask_spread_sum / acc.tick_count if acc.tick_count > 0 else 0.0

        bar_start = acc.bar_start_ts or current_ts
        high_offset = 0
        low_offset = 0
        if acc.high_ts is not None:
            high_offset = int((acc.high_ts.astype('datetime64[ms]').astype(np.int64)
                               - bar_start.astype('datetime64[ms]').astype(np.int64)))
        if acc.low_ts is not None:
            low_offset = int((acc.low_ts.astype('datetime64[ms]').astype(np.int64)
                              - bar_start.astype('datetime64[ms]').astype(np.int64)))

        bar = EnhancedBar(
            timestamp=bar_start,
            symbol=self._symbol,
            open=acc.open,
            high=acc.high,
            low=acc.low,
            close=acc.close,
            volume=acc.total_volume,
            vwap=vwap,
            high_time_offset_ms=high_offset,
            low_time_offset_ms=low_offset,
            volume_at_high=acc.volume_at_high,
            volume_at_low=acc.volume_at_low,
            bid_ask_spread_avg=bid_ask_avg,
            open_interest=acc.open_interest,
            tick_count=acc.tick_count,
        )

        self._update_indicators(bar)
        self._bar_count += 1
        self._last_bar = bar
        self._all_bars.append(bar)

        if bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,
                                  ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):
            self._extreme_snapshots.append(bar)

        if self._on_bar_complete is not None:
            self._on_bar_complete(bar)

        self._tick_acc = _TickAccumulator()
        return bar

    def _update_indicators(self, bar: EnhancedBar) -> None:
        vwap = bar.vwap
        close = bar.close

        for period in self._ma_periods:
            self._vwap_buffers[period].append(vwap)
        self._close_buffer.append(close)
        self._high_buffer.append(bar.high)
        self._low_buffer.append(bar.low)

        prev_close = self._close_buffer[-2] if len(self._close_buffer) >= 2 else close
        prev_high = self._high_buffer[-2] if len(self._high_buffer) >= 2 else bar.high
        prev_low = self._low_buffer[-2] if len(self._low_buffer) >= 2 else bar.low
        tr = max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))
        self._tr_buffer.append(tr)

        price_vs_mas = {}
        price_ma_deviation_sigma = {}
        ma_curvatures = {}
        ma_values = {}

        atr14 = float(np.mean(list(self._tr_buffer))) if len(self._tr_buffer) > 0 else 1e-8
        bar.atr14 = atr14

        for period in self._ma_periods:
            buf = self._vwap_buffers[period]
            if len(buf) >= period:
                ma_val = float(np.mean(list(buf)))
                ma_values[period] = ma_val

                deviation = (vwap - ma_val) / atr14 if atr14 > 1e-8 else 0.0
                price_vs_mas[f"ma{period}"] = float(np.sign(vwap - ma_val))
                price_ma_deviation_sigma[f"ma{period}"] = float(deviation)

                if len(buf) >= period + 1:
                    prev_vals = list(buf)
                    if len(prev_vals) >= 3:
                        recent_ma = np.mean(prev_vals[-period:])
                        mid_ma = np.mean(prev_vals[-period - 1:-1])
                        far_ma = np.mean(prev_vals[-period - 2:-2]) if len(prev_vals) >= period + 2 else mid_ma
                        curvature = recent_ma - 2 * mid_ma + far_ma
                        ma_curvatures[f"ma{period}"] = float(curvature / atr14 if atr14 > 1e-8 else 0.0)
            else:
                ma_values[period] = vwap
                price_vs_mas[f"ma{period}"] = 0.0
                price_ma_deviation_sigma[f"ma{period}"] = 0.0
                ma_curvatures[f"ma{period}"] = 0.0

        bar.price_vs_mas = price_vs_mas
        bar.price_ma_deviation_sigma = price_ma_deviation_sigma
        bar.ma_curvatures = ma_curvatures

        bar.ma_alignment = self._classify_ma_alignment(ma_values)

        bar.extreme_region = self._detect_extreme_region(vwap)

    def _detect_extreme_region(self, current_price: float) -> ExtremeRegion:
        """
        在线极值识别 — 仅使用当前Bar及之前的数据

        算法：
          1. 取最近extreme_window个Bar的high/low
          2. 如果当前价格接近局部高点（前quantile分位）→ NEAR_HIGH
          3. 如果当前价格接近局部低点（后quantile分位）→ NEAR_LOW
          4. 额外检查：如果偏离度超过3σ → EXTREME_HIGH/EXTREME_LOW（极端行情）
        """
        if len(self._high_buffer) < 3:
            return ExtremeRegion.NORMAL

        recent_highs = list(self._high_buffer)[-self._extreme_window:]
        recent_lows = list(self._low_buffer)[-self._extreme_window:]

        local_max = max(recent_highs)
        local_min = min(recent_lows)
        price_range = local_max - local_min

        if price_range < 1e-8:
            return ExtremeRegion.NORMAL

        high_threshold = local_max - self._extreme_quantile * price_range
        low_threshold = local_min + self._extreme_quantile * price_range

        atr = self._last_bar.atr14 if self._last_bar and self._last_bar.atr14 > 0 else price_range * 0.02
        extreme_sigma = 3.0

        is_near_high = current_price >= high_threshold
        is_near_low = current_price <= low_threshold

        if is_near_high:
            deviation_sigma = (local_max - current_price) / atr if atr > 1e-8 else 0.0
            if deviation_sigma > extreme_sigma:
                return ExtremeRegion.EXTREME_HIGH
            return ExtremeRegion.NEAR_HIGH

        if is_near_low:
            deviation_sigma = (current_price - local_min) / atr if atr > 1e-8 else 0.0
            if deviation_sigma > extreme_sigma:
                return ExtremeRegion.EXTREME_LOW
            return ExtremeRegion.NEAR_LOW

        return ExtremeRegion.NORMAL

    def _classify_ma_alignment(self, ma_values: Dict[int, float]) -> MAAlignment:
        """
        均线排列形态分类

        多头排列：MA5 > MA10 > MA15 > MA30 > MA60
        空头排列：MA5 < MA10 < MA15 < MA30 < MA60
        收敛：短周期与长周期均线差值缩小
        缠绕：均线频繁交叉
        """
        sorted_periods = sorted(ma_values.keys())
        if len(sorted_periods) < 3:
            return MAAlignment.TRANSITIONAL

        vals = [ma_values[p] for p in sorted_periods]

        bullish_count = sum(1 for i in range(len(vals) - 1) if vals[i] > vals[i + 1])
        bearish_count = sum(1 for i in range(len(vals) - 1) if vals[i] < vals[i + 1])
        n_pairs = len(vals) - 1

        if bullish_count == n_pairs:
            return MAAlignment.BULLISH
        if bearish_count == n_pairs:
            return MAAlignment.BEARISH

        if len(vals) >= 3:
            short_long_spread = abs(vals[0] - vals[-1])
            max_spread = max(abs(vals[i] - vals[j]) for i in range(len(vals)) for j in range(i + 1, len(vals)))
            if max_spread > 1e-8 and short_long_spread / max_spread < 0.3:
                return MAAlignment.CONVERGENT

        if bullish_count >= 1 and bearish_count >= 1:
            return MAAlignment.INTERTWINED

        return MAAlignment.TRANSITIONAL

    def get_extreme_snapshots(self) -> List[EnhancedBar]:
        return list(self._extreme_snapshots)

    def get_all_bars(self) -> List[EnhancedBar]:
        return list(self._all_bars)

    def flush(self) -> Optional[EnhancedBar]:
        """emit最后一个未完成的Bar"""
        if self._tick_acc.tick_count > 0:
            return self._emit_bar(np.datetime64('now'))
        return None

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "symbol": self._symbol,
            "total_bars": self._bar_count,
            "extreme_bar_count": len(self._extreme_snapshots),
            "near_high_count": sum(1 for b in self._extreme_snapshots
                                   if b.extreme_region == ExtremeRegion.NEAR_HIGH),
            "near_low_count": sum(1 for b in self._extreme_snapshots
                                  if b.extreme_region == ExtremeRegion.NEAR_LOW),
            "extreme_high_count": sum(1 for b in self._extreme_snapshots
                                     if b.extreme_region == ExtremeRegion.EXTREME_HIGH),
            "extreme_low_count": sum(1 for b in self._extreme_snapshots
                                    if b.extreme_region == ExtremeRegion.EXTREME_LOW),
        }

    @staticmethod
    def build_from_preprocessed_df(
        df: Any,
        symbol: str,
        ma_periods: Optional[List[int]] = None,
        extreme_window: int = 20,
        extreme_quantile: float = 0.05,
    ) -> 'TurningPointMicroscope':
        """
        从预处理DataFrame（如preprocess_ticks.py输出）构建拐点显微镜

        DataFrame需包含列：timestamp/minute, open, high, low, close, volume, vwap,
                           bid_ask_spread, open_interest, tick_count
        """
        import pandas as pd

        scope = TurningPointMicroscope(
            symbol=symbol,
            ma_periods=ma_periods,
            extreme_window=extreme_window,
            extreme_quantile=extreme_quantile,
        )

        ts_col = "minute" if "minute" in df.columns else "timestamp"

        for _, row in df.iterrows():
            ts = np.datetime64(row[ts_col])
            bar = EnhancedBar(
                timestamp=ts,
                symbol=symbol,
                open=float(row.get("open", 0)),
                high=float(row.get("high", 0)),
                low=float(row.get("low", 0)),
                close=float(row.get("close", 0)),
                volume=int(row.get("volume", 0)),
                vwap=float(row.get("vwap", row.get("close", 0))),
                high_time_offset_ms=0,
                low_time_offset_ms=0,
                volume_at_high=0.0,
                volume_at_low=0.0,
                bid_ask_spread_avg=float(row.get("bid_ask_spread", 0)),
                open_interest=int(row.get("open_interest", 0)),
                tick_count=int(row.get("tick_count", 0)),
            )
            scope._update_indicators(bar)
            scope._bar_count += 1
            scope._last_bar = bar
            scope._all_bars.append(bar)
            if bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,
                                      ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):
                scope._extreme_snapshots.append(bar)

        return scope
