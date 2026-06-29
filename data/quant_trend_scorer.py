# MODULE_ID: M1-035
"""
quant_trend_scorer.py - 多周期趋势评分模块

包含:
  MultiPeriodTrendScorer - 多周期趋势评分（Wilder ADX+动态EMA）   <100μs
"""
from __future__ import annotations

import math
import threading
from typing import Any, Dict, List, Tuple

import numpy as np

try:
    from .quant_infra import NumpyRingBuffer
except ImportError:
    from quant_infra import NumpyRingBuffer


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
        # R24-P1-IV-02修复: close/high/low价格过滤增加NaN/Inf检查
        import math
        if (close <= 0 or high < low
            or (isinstance(close, float) and (math.isnan(close) or math.isinf(close)))
            or (isinstance(high, float) and (math.isnan(high) or math.isinf(high)))
            or (isinstance(low, float) and (math.isnan(low) or math.isinf(low)))):
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

            momentum = (curr_close - prev_close) / prev_close if prev_close > 1e-10 else 0.0  # R24-P2-IV-02修复: 使用1e-10替代0防止极端低价合约产生巨大中间值
            adx_enhanced = min(self._adx_buffers[i] * (1.0 + 0.3 * abs(momentum) * 100.0), 100.0)

            self._vol_buffers[i].append(tr)
            vbuf = self._vol_buffers[i]
            cv = vbuf.std() / vbuf.mean() if len(vbuf) > 2 and vbuf.mean() > 0 else 1.0
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