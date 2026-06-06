"""
信号时序滤波器 — 从signal_service.py拆分
包含: KalmanFilter1D, EMASignalFilter, SignalTimingFilter, AdaptiveSignalThreshold
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional, Tuple
from collections import deque


class KalmanFilter1D:
    def __init__(self, process_variance: float = 1e-4, measurement_variance: float = 1e-2):
        self._x = 0.0
        self._p = 1.0
        self._q = process_variance
        self._r = measurement_variance
        self._velocity = 0.0
        self._prev_x = 0.0
        self._initialized = False
        self._lock = threading.Lock()

    def update(self, measurement: float) -> Tuple[float, float]:
        with self._lock:
            if not self._initialized:
                self._x = measurement
                self._prev_x = measurement
                self._initialized = True
                return self._x, 0.0
            self._p += self._q
            k = self._p / (self._p + self._r)
            self._prev_x = self._x
            self._x = self._x + k * (measurement - self._x)
            self._p = (1 - k) * self._p
            self._velocity = self._x - self._prev_x
            return self._x, self._velocity

    def get_state(self) -> Tuple[float, float]:
        with self._lock:
            return self._x, self._velocity

    def reset(self) -> None:
        with self._lock:
            self._x = 0.0
            self._p = 1.0
            self._velocity = 0.0
            self._prev_x = 0.0
            self._initialized = False


class EMASignalFilter:
    def __init__(self, fast_period: int = 5, slow_period: int = 20):
        self._fast_alpha = 2.0 / (fast_period + 1)
        self._slow_alpha = 2.0 / (slow_period + 1)
        self._fast_ema: Optional[float] = None
        self._slow_ema: Optional[float] = None
        self._velocity = 0.0
        self._prev_fast: Optional[float] = None
        self._lock = threading.Lock()

    def update(self, value: float) -> Tuple[float, float, float]:
        with self._lock:
            if self._fast_ema is None:
                self._fast_ema = value
                self._slow_ema = value
                self._prev_fast = value
                return self._fast_ema, self._slow_ema, 0.0
            self._prev_fast = self._fast_ema
            self._fast_ema = self._fast_alpha * value + (1 - self._fast_alpha) * self._fast_ema
            self._slow_ema = self._slow_alpha * value + (1 - self._slow_alpha) * self._slow_ema
            self._velocity = self._fast_ema - self._prev_fast
            return self._fast_ema, self._slow_ema, self._velocity

    def is_bullish_crossover(self) -> bool:
        with self._lock:
            if self._fast_ema is None or self._slow_ema is None:
                return False
            return self._fast_ema > self._slow_ema and self._velocity > 0

    def get_state(self) -> Tuple[float, float, float]:
        with self._lock:
            return self._fast_ema or 0.0, self._slow_ema or 0.0, self._velocity


class SignalTimingFilter:
    def __init__(self, threshold: float = 0.6, use_kalman: bool = True,
                 kalman_process_var: float = 1e-4, kalman_measure_var: float = 1e-2,
                 ema_fast_period: int = 5, ema_slow_period: int = 20):
        self._threshold = threshold
        self._use_kalman = use_kalman
        self._kalman = KalmanFilter1D(kalman_process_var, kalman_measure_var)
        self._ema = EMASignalFilter(ema_fast_period, ema_slow_period)
        self._filters: Dict[str, KalmanFilter1D] = {}
        self._ema_filters: Dict[str, EMASignalFilter] = {}
        self._lock = threading.Lock()
        self._stats = {'total_inputs': 0, 'filtered_noise': 0, 'passed_signals': 0}

    def filter_signal(self, instrument_id: str, raw_strength: float) -> Dict[str, Any]:
        self._stats['total_inputs'] += 1
        with self._lock:
            if instrument_id not in self._filters:
                self._filters[instrument_id] = KalmanFilter1D()
                self._ema_filters[instrument_id] = EMASignalFilter()
            kf = self._filters[instrument_id]
            ema_f = self._ema_filters[instrument_id]
        smoothed, velocity = kf.update(raw_strength)
        fast_ema, slow_ema, ema_vel = ema_f.update(raw_strength)
        if self._use_kalman:
            effective_value, effective_velocity = smoothed, velocity
        else:
            effective_value, effective_velocity = fast_ema, ema_vel
        threshold_crossed = effective_value >= self._threshold
        velocity_positive = effective_velocity > 0
        ema_confirmed = ema_f.is_bullish_crossover()
        passed = threshold_crossed and velocity_positive
        if not passed and raw_strength >= self._threshold:
            self._stats['filtered_noise'] += 1
        if passed:
            self._stats['passed_signals'] += 1
        return {
            'instrument_id': instrument_id, 'raw_strength': raw_strength,
            'smoothed_value': effective_value, 'velocity': effective_velocity,
            'threshold_crossed': threshold_crossed, 'velocity_positive': velocity_positive,
            'ema_confirmed': ema_confirmed, 'signal_passed': passed,
            'fast_ema': fast_ema, 'slow_ema': slow_ema,
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            'service_name': 'SignalTimingFilter', **self._stats,
            'filter_ratio': self._stats['filtered_noise'] / max(1, self._stats['total_inputs']),
            'pass_ratio': self._stats['passed_signals'] / max(1, self._stats['total_inputs']),
            'tracked_instruments': len(self._filters),
        }


class AdaptiveSignalThreshold:
    def __init__(self, initial_threshold: float = 0.3,
                 min_threshold: float = 0.15, max_threshold: float = 0.6,
                 adaptation_rate: float = 0.05, target_pass_rate: float = 0.4):
        self._threshold = initial_threshold
        self._min_threshold = min_threshold
        self._max_threshold = max_threshold
        self._adaptation_rate = adaptation_rate
        self._target_pass_rate = target_pass_rate
        self._recent_passes: deque = deque(maxlen=100)
        self._recent_pnls: deque = deque(maxlen=50)

    @property
    def threshold(self) -> float:
        return self._threshold

    def record_signal(self, passed: bool, pnl: float = 0.0) -> None:
        self._recent_passes.append(passed)
        if passed:
            self._recent_pnls.append(pnl)
        if len(self._recent_passes) >= 20:
            self._adapt()

    def _adapt(self) -> None:
        pass_rate = sum(1 for p in self._recent_passes if p) / len(self._recent_passes)
        adjustment = (pass_rate - self._target_pass_rate) * self._adaptation_rate
        if len(self._recent_pnls) >= 10:
            avg_pnl = sum(self._recent_pnls) / len(self._recent_pnls)
            if avg_pnl < 0:
                adjustment += self._adaptation_rate * 0.5
        self._threshold = max(self._min_threshold, min(self._max_threshold, self._threshold + adjustment))

    def get_stats(self) -> Dict[str, Any]:
        pass_rate = sum(1 for p in self._recent_passes if p) / max(1, len(self._recent_passes))
        return {
            'service_name': 'AdaptiveSignalThreshold',
            'current_threshold': round(self._threshold, 4),
            'pass_rate': round(pass_rate, 4),
            'target_pass_rate': self._target_pass_rate,
        }