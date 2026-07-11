# MODULE_ID: M1-236+M1-237+M1-238+M1-242
"""signal_components.py - 信号组件集合（合并自4个小碎模块）

合并来源:
    - cooldown_manager.py (M1-236): CooldownManager
    - signal_filter_chain.py (M1-238): SignalFilterChain
    - signal_history_service.py (M1-240): SignalHistoryService
    - signal_timing_filter.py (M1-242): KalmanFilter1D, EMASignalFilter, SignalTimingFilter, AdaptiveSignalThreshold

合并时间: 2026-07-11 | 合并原因: 4个模块均<500行，职责同属信号支撑组件
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import warnings
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# 原 cooldown_manager.py (M1-236)
# ============================================================================

class CooldownManager:
    """冷却管理 — 从SignalService提取的独立可测试组件"""

    def __init__(self):
        self._cooldown_times: Dict[str, float] = {}
        self._cooldown_durations: Dict[str, float] = {}
        self._lock = threading.Lock()

    def make_cooldown_key(self, instrument_id: str, signal_type: str = '') -> str:
        if signal_type:
            return f"{instrument_id}_{signal_type}"
        return f"{instrument_id}_default"

    def is_in_cooldown(self, cooldown_key: str, cooldown_seconds: float) -> bool:
        last_signal_time = self._cooldown_times.get(cooldown_key, 0)
        effective_cooldown = self._cooldown_durations.get(cooldown_key, cooldown_seconds)
        elapsed = time.time() - last_signal_time
        return elapsed < effective_cooldown

    def set_cooldown(self, instrument_id: str, cooldown_seconds: float,
                      signal_type: str = '') -> None:
        cooldown_key = self.make_cooldown_key(instrument_id, signal_type)
        with self._lock:
            self._cooldown_times[cooldown_key] = time.time()
            self._cooldown_durations[cooldown_key] = cooldown_seconds
            logging.info("[CooldownManager] Cooldown set for %s: %ss", cooldown_key, cooldown_seconds)

    def clear_cooldown(self, instrument_id: str) -> None:
        with self._lock:
            if instrument_id in self._cooldown_times:
                del self._cooldown_times[instrument_id]
                self._cooldown_durations.pop(instrument_id, None)
                logging.debug("[CooldownManager] Cooldown cleared for %s", instrument_id)

    @property
    def cooldown_times(self) -> Dict[str, float]:
        return self._cooldown_times

    @property
    def cooldown_durations(self) -> Dict[str, float]:
        return self._cooldown_durations

    def cleanup_expired(self, default_cooldown_seconds: float,
                        cleanup_interval: float,
                        last_cleanup: float) -> int:
        now = time.time()
        if now - last_cleanup <= cleanup_interval:
            return 0
        expired = [k for k, v in self._cooldown_times.items()
                   if now - v > default_cooldown_seconds]
        with self._lock:
            for k in expired:
                del self._cooldown_times[k]
                self._cooldown_durations.pop(k, None)
        if expired:
            logging.debug("[SignalService] 清理%d个过期冷却条目", len(expired))
        return len(expired)

    def count_active_cooldowns(self, default_cooldown_seconds: float) -> int:
        now = time.time()
        return len([k for k, v in self._cooldown_times.items()
                    if now - v < default_cooldown_seconds])


# ============================================================================
# 原 signal_timing_filter.py (M1-242)
# ============================================================================

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


# ============================================================================
# 原 signal_filter_chain.py (M1-238)
# ============================================================================

class SignalFilterChain:
    """信号过滤链 — 从SignalService提取的独立可测试组件"""

    def __init__(self, signal_service=None):
        self._signal_service = signal_service
        self._hft_signal_filter = None
        self._hft_filter_enabled = False
        self._min_estimated_plr = 2.0
        self._plr_filter_enabled = False

    def enable_hft_filter(self, threshold: float = 0.6, use_kalman: bool = True) -> None:
        if self._signal_service is not None:
            self._hft_signal_filter = SignalTimingFilter(threshold=threshold, use_kalman=use_kalman)
        self._hft_filter_enabled = True
        logging.info("[SignalFilterChain] HFT信号时序滤波器已启用 threshold=%.2f", threshold)

    def filter_with_hft(self, instrument_id: str, resonance_strength: float) -> Optional[Dict[str, Any]]:
        if not self._hft_filter_enabled or not self._hft_signal_filter:
            return None
        return self._hft_signal_filter.filter_signal(instrument_id, resonance_strength)

    def enable_plr_filter(self, min_estimated_plr: float = 2.0) -> None:
        self._min_estimated_plr = min_estimated_plr
        self._plr_filter_enabled = True
        logging.info("[SignalFilterChain] PLR过滤已启用 min_estimated_plr=%.2f", min_estimated_plr)

    def disable_plr_filter(self) -> None:
        self._plr_filter_enabled = False
        logging.info("[SignalFilterChain] PLR过滤已禁用")

    @property
    def plr_filter_enabled(self) -> bool:
        return self._plr_filter_enabled

    @property
    def min_estimated_plr(self) -> float:
        return self._min_estimated_plr

    def transition_signal_state(self, signal_id: str, new_state: str,
                                 signal_states: Dict[str, str],
                                 valid_transitions: Dict[str, List[str]]) -> bool:
        current = signal_states.get(signal_id, 'GENERATED')
        valid_targets = valid_transitions.get(current, [])
        if new_state not in valid_targets:
            logging.warning("[R23-SM-02-FIX] 非法信号状态转移: %s -> %s, signal_id=%s", current, new_state, signal_id)
            return False
        signal_states[signal_id] = new_state
        return True

    def expire_stale_signals(self, signal_history: List[Dict],
                              signal_states: Dict[str, str],
                              signal_max_age_sec: float,
                              lock: threading.Lock = None) -> int:
        if signal_max_age_sec <= 0:
            return 0
        _now = time.time()
        if hasattr(self, '_last_expire_check_time') and (_now - self._last_expire_check_time) < 30.0:
            return 0
        self._last_expire_check_time = _now
        _expired_count = 0
        expired_states = ('COMPLETED', 'EXPIRED', 'REJECTED')
        if lock:
            with lock:
                for sig in signal_history:
                    sig_id = sig.get('signal_id', '')
                    sig_state = signal_states.get(sig_id, 'GENERATED')
                    if sig_state in expired_states:
                        continue
                    sig_time = sig.get('timestamp', 0)
                    if isinstance(sig_time, (int, float)) and (_now - sig_time) > signal_max_age_sec:
                        signal_states[sig_id] = 'EXPIRED'
                        _expired_count += 1
        else:
            for sig in signal_history:
                sig_id = sig.get('signal_id', '')
                sig_state = signal_states.get(sig_id, 'GENERATED')
                if sig_state in expired_states:
                    continue
                sig_time = sig.get('timestamp', 0)
                if isinstance(sig_time, (int, float)) and (_now - sig_time) > signal_max_age_sec:
                    signal_states[sig_id] = 'EXPIRED'
                    _expired_count += 1
        if _expired_count > 0:
            logging.info("[R23-FR-05-FIX] 过期信号清理: %d个信号已标记EXPIRED, max_age=%ds", _expired_count, int(signal_max_age_sec))
        return _expired_count


# ============================================================================
# 原 signal_history_service.py (M1-240)
# ============================================================================

class SignalHistoryService:
    def __init__(self, max_history: int = 10000):
        self._history: deque = deque(maxlen=max_history)
        self._signal_counts: Dict[str, int] = {}
        self._rejected_counts: Dict[str, int] = {}
        self._daily_report_generated: Dict[str, bool] = {}
        self._log_dir: str = ''

    def set_log_dir(self, log_dir: str) -> None:
        self._log_dir = log_dir

    def record_signal(self, signal: Dict[str, Any]) -> None:
        signal['_record_time'] = time.time()
        self._history.append(signal)
        reason = signal.get('reason', 'unknown')
        self._signal_counts[reason] = self._signal_counts.get(reason, 0) + 1

    def record_rejection(self, reason: str) -> None:
        self._rejected_counts[reason] = self._rejected_counts.get(reason, 0) + 1

    def get_recent(self, n: int = 100) -> List[Dict[str, Any]]:
        return list(self._history)[-n:]

    def get_statistics(self) -> Dict[str, Any]:
        return {
            'total_signals': len(self._history),
            'signal_counts': dict(self._signal_counts),
            'rejected_counts': dict(self._rejected_counts),
        }

    def clear(self) -> None:
        self._history.clear()
        self._signal_counts.clear()
        self._rejected_counts.clear()

    def generate_daily_signal_report(self, date: Optional[str] = None,
                                     market_close_hour: int = 15,
                                     market_close_minute_start: int = 15,
                                     market_close_minute_end: int = 20) -> Optional[str]:
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        if date is None:
            date = datetime.now(CHINA_TZ).strftime("%Y-%m-%d")
        if self._daily_report_generated.get(date):
            return None
        date_signals = [
            s for s in list(self._history)
            if isinstance(s.get('generated_at'), datetime) and s['generated_at'].strftime("%Y-%m-%d") == date
        ]
        if not date_signals:
            return None
        buy_signals = [s for s in date_signals if s.get('signal_type') in ('BUY',)]
        sell_signals = [s for s in date_signals if s.get('signal_type') in ('SELL',)]
        close_long = [s for s in date_signals if s.get('signal_type') == 'CLOSE_LONG']
        close_short = [s for s in date_signals if s.get('signal_type') == 'CLOSE_SHORT']
        lines = [
            "=" * 80,
            f"当日信号明细报告 - {date}",
            "=" * 80,
            f"总信号数: {len(date_signals)}",
            f"  买入信号: {len(buy_signals)}",
            f"  卖出信号: {len(sell_signals)}",
            f"  平多信号: {len(close_long)}",
            f"  平空信号: {len(close_short)}",
            "-" * 80,
            "信号明细:",
        ]
        for i, s in enumerate(date_signals, 1):
            ts = s['generated_at'].strftime('%H:%M:%S') if isinstance(s.get('generated_at'), datetime) else str(s.get('generated_at', ''))
            lines.extend([
                f"【信号 {i}】",
                f"  时间: {ts}",
                f"  合约: {s.get('instrument_id', '')}",
                f"  类型: {s.get('signal_type', '')}",
                f"  价格: {s.get('price', 0)}",
                f"  数量: {s.get('volume', 0)}",
                f"  原因: {s.get('reason', '')}",
                f"  信号ID: {s.get('signal_id', '')}",
            ])
        lines.extend([
            "=" * 80,
            f"报告生成时间: {datetime.now(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
        ])
        report = "\n".join(lines)
        try:
            os.makedirs(self._log_dir, exist_ok=True)
            report_file = os.path.join(self._log_dir, f"signal_daily_report_{date}.txt")
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            json_file = os.path.join(self._log_dir, f"signal_daily_report_{date}.json")
            serializable_signals = []
            for s in date_signals:
                entry = dict(s)
                if isinstance(entry.get('generated_at'), datetime):
                    entry['generated_at'] = entry['generated_at'].isoformat()
                serializable_signals.append(entry)
            report_data = {
                'date': date,
                'total': len(date_signals),
                'buy': len(buy_signals),
                'sell': len(sell_signals),
                'close_long': len(close_long),
                'close_short': len(close_short),
                'signals': serializable_signals,
            }
            with open(json_file, 'w', encoding='utf-8') as f:
                try:
                    from ali2026v3_trading.infra.serialization_utils import json_dumps
                    f.write(json_dumps(report_data, indent=2))
                except ImportError:
                    def _fallback_default(obj):
                        import math as _math
                        import datetime as _dt
                        import decimal as _decimal
                        if isinstance(obj, float):
                            if _math.isnan(obj):
                                return {"__special__": "NaN"}
                            if _math.isinf(obj):
                                return {"__special__": "Infinity" if obj > 0 else "-Infinity"}
                        if isinstance(obj, (_dt.datetime, _dt.date)):
                            return obj.isoformat()
                        if isinstance(obj, _decimal.Decimal):
                            return {"__decimal__": str(obj)}
                        return str(obj)
                    f.write(json_dumps(report_data, indent=2, default=_fallback_default, ensure_ascii=False))
            self._daily_report_generated[date] = True
            logging.info("[SignalService] 日信号报告已生成: %s", report_file)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SignalService] 生成日信号报告失败: %s", e)
        return report

    def check_market_close_and_report(self,
                                       market_close_hour: int = 15,
                                       market_close_minute_start: int = 15,
                                       market_close_minute_end: int = 20) -> Optional[str]:
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        now = datetime.now(CHINA_TZ)
        current_time = now.time()
        from datetime import time as dt_time
        close_time = dt_time(market_close_hour, market_close_minute_start)
        check_end = dt_time(market_close_hour, market_close_minute_end)
        if close_time <= current_time <= check_end:
            today = now.strftime("%Y-%m-%d")
            return self.generate_daily_signal_report(today,
                                                      market_close_hour=market_close_hour,
                                                      market_close_minute_start=market_close_minute_start,
                                                      market_close_minute_end=market_close_minute_end)
        return None