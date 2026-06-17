# MODULE_ID: M1-247
"""
box_spring_detector.py - 箱体波动率脉冲策略（弹簧策略）- 检测Mixin

包含箱体识别、IV监控、弹簧信号识别、扫描等检测相关方法。
从BoxSpringStrategy类中提取，作为Mixin使用。
"""
from __future__ import annotations

import logging
import math
import threading
import time
from bisect import bisect_left, insort
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.logging_utils import get_logger
from ali2026v3_trading.infra.shared_utils import CHINA_TZ, TRADING_DAYS_PER_YEAR_CHINA
from ali2026v3_trading.infra.resilience import safe_float_to_int

# box_spring_scanner模块可能不存在，使用延迟导入
try:
    from ali2026v3_trading.strategy.box_spring_scanner import (
        scan_springs,
        _scan_from_sort_buckets,
        _scan_from_option_info,
        _evaluate_candidate,
    )
except ImportError:
    scan_springs = None
    _scan_from_sort_buckets = None
    _scan_from_option_info = None
    _evaluate_candidate = None

logger = get_logger(__name__)

# Merged from _box_spring_types.py on 2026-06-12

# _INTERNAL: internal module, not part of public API

class SpringState(Enum):
    DORMANT = auto()
    COMPRESSED = auto()
    TRIGGERED = auto()
    ACTIVE = auto()
    EXPIRED = auto()


@dataclass(slots=True)
class BoxRange:
    box_id: str
    instrument_id: str
    box_top: float
    box_bottom: float
    box_width_pct: float
    confirmed_at: datetime
    touch_count: int = 0
    last_touch_time: Optional[datetime] = None
    is_active: bool = True

    def contains_price(self, price: float) -> bool:
        if self.box_bottom <= 0 or self.box_top <= 0:
            return False
        _eps = max(abs(self.box_bottom), abs(self.box_top)) * 1e-9
        return price >= self.box_bottom - _eps and price <= self.box_top + _eps

    def price_position(self, price: float) -> float:
        if self.box_top <= self.box_bottom:
            return 0.5
        return (price - self.box_bottom) / (self.box_top - self.box_bottom)


@dataclass(slots=True)
class SpringSignal:
    signal_id: str
    instrument_id: str
    option_instrument_id: str
    spring_state: SpringState
    iv_percentile: float
    premium_cost_pct: float
    gamma_exposure: float
    box_id: str
    direction: str
    strike_price: float
    current_price: float
    premium_price: float
    lots: int = 1
    open_reason: str = ''
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_consumed: bool = False
    account_equity: float = 100000.0


@dataclass(slots=True)
class SpringPosition:
    position_id: str
    signal_id: str
    instrument_id: str
    option_instrument_id: str
    direction: str
    entry_premium: float
    current_premium: float
    entry_time: datetime
    stop_profit_ratio: float
    max_loss_pct: float
    box_id: str
    is_open: bool = True
    peak_premium: float = 0.0
    peak_time: Optional[datetime] = None
    paired_instrument_id: str = ''
    paired_current_premium: float = 0.0
    lots: int = 1

    @property
    def pnl_ratio(self) -> float:
        if self.entry_premium <= 0:
            return 0.0
        return min(self.current_premium / self.entry_premium, 10.0)

    @property
    def should_take_profit(self) -> bool:
        return self.pnl_ratio >= self.stop_profit_ratio

    @property
    def should_accept_loss(self) -> bool:
        if self.entry_premium <= 0:
            return True
        loss_pct = 1.0 - (self.current_premium / self.entry_premium)
        return loss_pct >= self.max_loss_pct

    def adjust_tp_sl_by_plr(self, estimated_plr: float, min_take_profit_ratio: float = 1.0,
                            min_max_loss_pct: float = 0.15) -> None:
        if estimated_plr > 0:
            self.stop_profit_ratio = max(min(estimated_plr, 8.0), min_take_profit_ratio)
            self.max_loss_pct = max(min(1.0 / estimated_plr, 0.95), min_max_loss_pct)


@dataclass(slots=True)
class PendingPullback:
    signal: SpringSignal
    signal_bar_index: int
    peak_premium: float
    peak_bar_index: int
    bars_elapsed: int = 0
    is_expired: bool = False

    def check_retrace(self, current_premium: float,
                      pullback_retrace_pct: float,
                      pullback_wait_bars: int) -> bool:
        if self.is_expired:
            return False
        self.bars_elapsed += 1
        if self.bars_elapsed > pullback_wait_bars:
            self.is_expired = True
            return False
        if current_premium > self.peak_premium:
            self.peak_premium = current_premium
            self.peak_bar_index += self.bars_elapsed
        if self.peak_premium <= 0:
            return False
        retrace_pct = (self.peak_premium - current_premium) / self.peak_premium
        return retrace_pct >= pullback_retrace_pct


_box_spring_instance: Optional['BoxSpringStrategy'] = None
_box_spring_lock = threading.Lock()


def get_box_spring_strategy(params: Optional[Dict] = None) -> 'BoxSpringStrategy':
    global _box_spring_instance
    if _box_spring_instance is None:
        with _box_spring_lock:
            if _box_spring_instance is None:
                if params is None:
                    try:
                        from ali2026v3_trading.config.config_service import get_cached_params
                        all_params = get_cached_params()
                        spring_keys = [
                            'box_breakout_tolerance', 'spring_price_pos_min', 'spring_price_pos_max',
                            'strike_distance_threshold', 'direction_buy_call_threshold',
                            'direction_buy_put_threshold', 'iv_low_percentile', 'iv_very_low_percentile',
                            'min_box_touches', 'max_box_width_pct', 'min_days_to_expiry',
                            'max_days_to_expiry', 'max_premium_cost_pct', 'stop_profit_ratio',
                            'max_loss_pct', 'max_position_pct', 'dynamic_tp_sl_enabled',
                            'min_estimated_plr', 'spring_cooldown_sec', 'max_spring_positions',
                            'pullback_enabled', 'pullback_wait_bars', 'pullback_retrace_pct',
                            'pullback_iv_min_percentile', 'pullback_iv_max_percentile',
                        ]
                        params = {k: all_params[k] for k in spring_keys if k in all_params}
                    except (KeyError, TypeError) as e:
                        logging.debug("[BoxSpringStrategy] param extraction failed: %s", e)
                        params = {}
                from ali2026v3_trading.strategy.box_spring_strategy_impl import BoxSpringStrategy
                _box_spring_instance = BoxSpringStrategy(params or {})
    return _box_spring_instance


class BoxSpringDetectorService:

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        from collections import deque
        _p = params or {}
        self._lock = threading.RLock()
        self._boxes: Dict[str, BoxRange] = {}
        self._signals: Dict[str, SpringSignal] = {}
        self._iv_history: Dict[str, deque] = {}
        self._iv_window = _p.get('iv_lookback_bars', 120)
        self._min_box_touches = _p.get('min_box_touches', 3)
        self._max_box_width_pct = _p.get('max_box_width_pct', 0.04)
        self._box_breakout_tolerance = _p.get('box_breakout_tolerance', 0.005)
        self._iv_low_percentile = _p.get('iv_low_percentile', 5.0)
        self._iv_very_low_percentile = _p.get('iv_very_low_percentile', 2.0)
        self._min_days_to_expiry = _p.get('min_days_to_expiry', 2)
        self._max_days_to_expiry = _p.get('max_days_to_expiry', 5)
        self._max_premium_cost_pct = _p.get('max_premium_cost_pct', 0.015)
        self._spring_price_pos_min = _p.get('spring_price_pos_min', 0.3)
        self._spring_price_pos_max = _p.get('spring_price_pos_max', 0.7)
        self._strike_distance_threshold = _p.get('strike_distance_threshold', 0.02)
        self._direction_buy_call_threshold = _p.get('direction_buy_call_threshold', 0.45)
        self._direction_buy_put_threshold = _p.get('direction_buy_put_threshold', 0.55)
        self._cooldown_sec = _p.get('spring_cooldown_sec', 300)
        self._last_signal_time: Dict[str, float] = {}
        self._current_bar_time: Optional[float] = None
        self._stats = {
            'total_signals': 0,
            'compressed_detected': 0,
            'triggers_fired': 0,
        }
        self._pullback_enabled = _p.get('pullback_enabled', False)
        self._pullback_wait_bars = _p.get('pullback_wait_bars', 5)
        self._pullback_retrace_pct = _p.get('pullback_retrace_pct', 0.15)
        self._pullback_iv_min_percentile = _p.get('pullback_iv_min_percentile', 20.0)
        self._pullback_iv_max_percentile = _p.get('pullback_iv_max_percentile', 80.0)
        self._pending_pullbacks: Dict[str, PendingPullback] = {}
        self._pullback_stats = {
            'signals_deferred': 0,
            'retrace_entries': 0,
            'retrace_timeouts': 0,
            'retrace_iv_rejected': 0,
        }
        from ali2026v3_trading.infra.shared_utils import PullbackManager
        self._pullback_mgr = PullbackManager({
            'pullback_enabled': self._pullback_enabled,
            'pullback_wait_bars': self._pullback_wait_bars,
            'pullback_retrace_pct': self._pullback_retrace_pct,
            'pullback_iv_min_percentile': self._pullback_iv_min_percentile,
            'pullback_iv_max_percentile': self._pullback_iv_max_percentile,
        })
        try:
            self._box_detector = get_box_detector()
        except (ImportError, RuntimeError):
            self._box_detector = BoxDetector()

    def _get_now(self) -> datetime:
        if self._current_bar_time is not None:
            return datetime.fromtimestamp(self._current_bar_time, tz=CHINA_TZ)
        return datetime.now(CHINA_TZ)

    def _invalidate_box(self, instrument_id: str, reason: str) -> None:
        with self._lock:
            box = self._boxes.get(instrument_id)
            if box:
                box.is_active = False
                logging.debug("[BoxSpringDetector] box invalidated: %s reason=%s", instrument_id, reason)

    # ========================================================================
    # 箱体识别
    # ========================================================================

    def update_box(self, instrument_id: str, high: float, low: float,
                   close: float, timestamp: Optional[datetime] = None) -> Optional[BoxRange]:
        # R24-P1-IV-02修复: close/high/low价格过滤增加NaN/Inf检查
        # R23-P2-04修复: 扩展类型检查覆盖int和numpy类型
        def _is_finite_number(x):
            try:
                return math.isfinite(float(x))
            except (TypeError, ValueError):
                return False

        if (high <= 0 or low <= 0 or close <= 0
            or not _is_finite_number(high)
            or not _is_finite_number(low)
            or not _is_finite_number(close)):
            return None

        ts = timestamp or self._get_now()

        self._box_detector.update_bar(high, low, close, timestamp=ts.isoformat())

        box_profile = self._box_detector.detect_box()

        if box_profile is None or not box_profile.is_valid:
            if not box_profile or not box_profile.is_box:
                return self._update_box_fallback(instrument_id, high, low, close, ts)
            return None

        box_id = box_profile.box_id
        box_top = box_profile.upper
        box_bottom = box_profile.lower
        width_pct = box_profile.width_pct / 100.0

        if width_pct > self._max_box_width_pct:
            self._invalidate_box(instrument_id, 'width_exceeded')
            return None

        box = self._boxes.get(instrument_id)
        if box and box.is_active:
            if close > box.box_top * (1.0 + self._box_breakout_tolerance) or close < box.box_bottom * (1.0 - self._box_breakout_tolerance):
                self._invalidate_box(instrument_id, 'price_broken')
                return None

            if high >= box.box_top * 0.998 or low <= box.box_bottom * 1.002:
                box.touch_count += 1
                box.last_touch_time = ts

            return box

        new_box = BoxRange(
            box_id=box_id,
            instrument_id=instrument_id,
            box_top=box_top,
            box_bottom=box_bottom,
            box_width_pct=width_pct,
            confirmed_at=ts,
            touch_count=box_profile.bounce_count or 1,
            last_touch_time=ts,
        )
        with self._lock:
            self._boxes[instrument_id] = new_box
        return new_box

    def _update_box_fallback(self, instrument_id: str, high: float, low: float,
                              close: float, ts: datetime) -> Optional[BoxRange]:
        width_pct = (high - low) / close if close > 0 else 1.0

        if width_pct > self._max_box_width_pct:
            self._invalidate_box(instrument_id, 'width_exceeded')
            return None

        box = self._boxes.get(instrument_id)
        if box and box.is_active:
            if close > box.box_top * (1.0 + self._box_breakout_tolerance) or close < box.box_bottom * (1.0 - self._box_breakout_tolerance):
                self._invalidate_box(instrument_id, 'price_broken')
                return None

            if high >= box.box_top * 0.998 or low <= box.box_bottom * 1.002:
                box.touch_count += 1
                box.last_touch_time = ts

            return box

        # R27-P2-FP-13修复: int()截断→safe_float_to_int()
        box_id = f"BOX_{instrument_id}_{safe_float_to_int(ts.timestamp())}"
        new_box = BoxRange(
            box_id=box_id,
            instrument_id=instrument_id,
            box_top=high,
            box_bottom=low,
            box_width_pct=width_pct,
            confirmed_at=ts,
            touch_count=1,
            last_touch_time=ts,
        )
        with self._lock:
            self._boxes[instrument_id] = new_box
        return new_box

    def get_active_box(self, instrument_id: str) -> Optional[BoxRange]:
        box = self._boxes.get(instrument_id)
        if box and box.is_active and box.touch_count >= self._min_box_touches:
            return box
        return None

    def _invalidate_box(self, instrument_id: str, reason: str):
        with self._lock:
            box = self._boxes.get(instrument_id)
            if box:
                box.is_active = False
                logging.info("[BoxSpring] Box invalidated: %s reason=%s", instrument_id, reason)

    # ========================================================================
    # IV百分位监控
    # ========================================================================

    def update_iv(self, instrument_id: str, iv: float) -> float:
        if iv <= 0:
            return 50.0

        self._box_detector.update_iv(iv)

        with self._lock:
            if instrument_id not in self._iv_history:
                self._iv_history[instrument_id] = deque(maxlen=self._iv_window)
            self._iv_history[instrument_id].append(iv)

            history = self._iv_history[instrument_id]
            if len(history) < 5:
                return 50.0

            sorted_ivs = sorted(history)
            return BoxDetector.compute_iv_percentile(iv, sorted_ivs)

    def get_iv_percentile(self, instrument_id: str) -> float:
        with self._lock:
            history = self._iv_history.get(instrument_id)
            if not history or len(history) < 5:
                return 50.0
            current_iv = history[-1]
            sorted_ivs = sorted(history)
            return BoxDetector.compute_iv_percentile(current_iv, sorted_ivs)

    # ========================================================================
    # 弹簧识别：四条件同时满足
    # ========================================================================

    def detect_spring(self, instrument_id: str, future_price: float,
                      option_instrument_id: str, strike_price: float,
                      iv: float, premium_price: float, days_to_expiry: int,
                      account_equity: float = 100000.0) -> Optional[SpringSignal]:
        box = self.get_active_box(instrument_id)
        if not box:
            return None

        if not box.contains_price(future_price):
            return None

        if days_to_expiry < self._min_days_to_expiry or days_to_expiry > self._max_days_to_expiry:
            return None

        iv_pct = self.update_iv(option_instrument_id, iv)
        if iv_pct > self._iv_low_percentile:
            return None

        is_very_compressed = iv_pct <= self._iv_very_low_percentile

        price_pos = box.price_position(future_price)
        if price_pos < self._spring_price_pos_min or price_pos > self._spring_price_pos_max:
            return None

        strike_distance_pct = abs(future_price - strike_price) / future_price if future_price > 0 else 1.0
        if strike_distance_pct > self._strike_distance_threshold:
            return None

        premium_cost_pct = premium_price / account_equity if account_equity > 0 else 1.0
        if premium_cost_pct > self._max_premium_cost_pct:
            return None

        now = time.time()
        last_time = self._last_signal_time.get(instrument_id, 0)
        if now - last_time < self._cooldown_sec:
            return None

        direction = self._infer_direction(box, future_price, price_pos)

        gamma_exposure = self._estimate_gamma_exposure(
            future_price, strike_price, iv, days_to_expiry, premium_price
        )

        # R27-P2-FP-14修复: int()截断→safe_float_to_int()
        # R24-P1-TR-14修复: 信号ID命名空间统一为SIG_
        signal_id = f"SIG_{instrument_id}_{safe_float_to_int(now*1000)}"
        initial_state = SpringState.DORMANT
        signal = SpringSignal(
            signal_id=signal_id,
            instrument_id=instrument_id,
            option_instrument_id=option_instrument_id,
            spring_state=initial_state,
            iv_percentile=iv_pct,
            premium_cost_pct=premium_cost_pct,
            gamma_exposure=gamma_exposure,
            box_id=box.box_id,
            direction=direction,
            strike_price=strike_price,
            current_price=future_price,
            premium_price=premium_price,
        )

        if is_very_compressed:
            signal.spring_state = SpringState.COMPRESSED
            with self._lock:
                self._stats['compressed_detected'] += 1
                self._last_signal_time[instrument_id] = now
            logging.info(
                "[BoxSpring] COMPRESSED(very_low_iv): %s IV_pct=%.1f%% premium=%.4f gamma_exp=%.4f dir=%s DTE=%d",
                option_instrument_id, iv_pct, premium_price, gamma_exposure, direction, days_to_expiry
            )
        else:
            signal.spring_state = SpringState.DORMANT
            with self._lock:
                self._last_signal_time[instrument_id] = now
            logging.info(
                "[BoxSpring] DORMANT(iv_low_but_not_very): %s IV_pct=%.1f%% premium=%.4f dir=%s DTE=%d",
                option_instrument_id, iv_pct, premium_price, direction, days_to_expiry
            )

        with self._lock:
            self._signals[signal_id] = signal

        return signal

    def _infer_direction(self, box: BoxRange, price: float, price_pos: float) -> str:
        if price_pos < self._direction_buy_call_threshold:
            return 'BUY_CALL'
        elif price_pos > self._direction_buy_put_threshold:
            return 'BUY_PUT'
        else:
            return 'BUY_STRADDLE'

    def _estimate_gamma_exposure(self, S: float, K: float, sigma: float,
                                  T_days: int, premium: float) -> float:
        if S <= 0 or K <= 0 or sigma <= 0 or T_days <= 0 or premium <= 0:
            return 0.0
        try:
            T = T_days / TRADING_DAYS_PER_YEAR_CHINA
            d1 = (math.log(S / K) + (0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
            gamma = math.exp(-0.5 * d1 * d1) / (S * sigma * math.sqrt(T) * math.sqrt(2 * math.pi))
            return gamma * S * S / premium if premium > 0 else 0.0
        except (ValueError, ZeroDivisionError, OverflowError) as e:
            logging.debug("[BoxSpringStrategy] gamma calc failed: %s", e)
            return 0.0

    # ========================================================================
    # 入场信号：箱内脉冲触发
    # ========================================================================

    def estimate_plr_before_entry(self, instrument_id: str) -> float:
        with self._lock:
            matched_signal = None
            for sig in self._signals.values():
                if sig.instrument_id == instrument_id and not sig.is_consumed:
                    matched_signal = sig
                    break
            if matched_signal is None:
                return 0.0
            box = self._boxes.get(instrument_id)
            if box is None or not box.is_active:
                return 0.0
            box_upper = getattr(box, 'upper', getattr(box, 'box_top', 0.0))
            box_lower = getattr(box, 'lower', getattr(box, 'box_bottom', 0.0))
            box_height = box_upper - box_lower
        if box_height < 1e-10 or matched_signal.premium_price < 1e-10:
            return 0.0
        return BoxDetector.estimate_plr(box_height, matched_signal.premium_price)

    def check_trigger(self, instrument_id: str, order_flow_imbalance: float = 0.0,
                      option_chain_activity: float = 0.0) -> Optional[SpringSignal]:
        with self._lock:
            triggerable = [
                s for s in self._signals.values()
                if s.instrument_id == instrument_id
                and s.spring_state in (SpringState.COMPRESSED, SpringState.DORMANT)
                and not s.is_consumed
            ]

        if not triggerable:
            return None

        triggered = False
        trigger_reason = ''

        if abs(order_flow_imbalance) > 0.3:
            triggered = True
            trigger_reason = f'order_flow={order_flow_imbalance:.2f}'

        if option_chain_activity > 1.5:
            triggered = True
            trigger_reason = f'chain_activity={option_chain_activity:.2f}'

        if not triggered:
            return None

        signal = triggerable[0]
        signal.spring_state = SpringState.TRIGGERED

        with self._lock:
            self._stats['triggers_fired'] += 1
            self._stats['total_signals'] += 1

        logging.info(
            "[BoxSpring] TRIGGERED: %s reason=%s IV_pct=%.1f%% dir=%s",
            signal.option_instrument_id, trigger_reason, signal.iv_percentile, signal.direction
        )

        return signal

    # ========================================================================
    # 回撤开仓（Pullback Entry）
    # ========================================================================

    def check_pullback_entry(self, signal: SpringSignal,
                             current_premium: float = 0.0,
                             iv_percentile: float = 50.0,
                             bar_index: int = 0) -> Optional[SpringSignal]:
        if not self._pullback_enabled:
            return signal
        deferred = self._pullback_mgr.should_defer(
            signal_id=signal.signal_id,
            strategy_type='spring',
            instrument_id=signal.option_instrument_id,
            direction=signal.direction,
            current_price=current_premium or signal.premium_price,
            iv_percentile=iv_percentile,
        )
        if deferred:
            with self._lock:
                if signal.signal_id not in self._pending_pullbacks:
                    self._pending_pullbacks[signal.signal_id] = PendingPullback(
                        signal=signal,
                        signal_bar_index=bar_index,
                        peak_premium=current_premium or signal.premium_price,
                        peak_bar_index=bar_index,
                    )
                    self._pullback_stats['signals_deferred'] += 1
                    logging.info(
                        "[BoxSpring] PendingPullback created: %s peak_prem=%.4f bar=%d",
                        signal.signal_id, current_premium or signal.premium_price, bar_index,
                    )
            return None
        return signal

    def _process_pending_pullbacks(self, current_premiums: Dict[str, float]) -> List[SpringSignal]:
        if not self._pullback_enabled:
            return []
        ready_signals: List[SpringSignal] = []
        with self._lock:
            expired_ids = []
            for sig_id, pp in self._pending_pullbacks.items():
                opt_id = pp.signal.option_instrument_id
                current_prem = current_premiums.get(opt_id, 0.0)
                if current_prem <= 0:
                    continue
                retrace_ok = pp.check_retrace(
                    current_prem,
                    self._pullback_retrace_pct,
                    self._pullback_wait_bars,
                )
                if retrace_ok:
                    pp.signal.spring_state = SpringState.TRIGGERED
                    ready_signals.append(pp.signal)
                    self._pullback_stats['retrace_entries'] += 1
                    logging.info(
                        "[BoxSpring] PendingPullback retrace entry: %s peak=%.4f current=%.4f",
                        sig_id, pp.peak_premium, current_prem,
                    )
                elif pp.is_expired:
                    expired_ids.append(sig_id)
                    pp.signal.spring_state = SpringState.EXPIRED
                    self._pullback_stats['retrace_timeouts'] += 1
                    logging.info(
                        "[BoxSpring] PendingPullback expired: %s bars=%d",
                        sig_id, pp.bars_elapsed,
                    )
            for sig_id in expired_ids:
                del self._pending_pullbacks[sig_id]
        ready = self._pullback_mgr.process_bar(current_premiums)
        for p in ready:
            self._pullback_stats['retrace_entries'] = self._pullback_mgr._stats['retrace_entries']
            self._pullback_stats['retrace_timeouts'] = self._pullback_mgr._stats['retrace_timeouts']
        return ready_signals

    def get_pullback_stats(self) -> Dict[str, Any]:
        return self._pullback_mgr.get_stats()

    scan_springs = scan_springs
    _scan_from_sort_buckets = _scan_from_sort_buckets
    _scan_from_option_info = _scan_from_option_info
    _evaluate_candidate = _evaluate_candidate

BoxSpringDetectorMixin = BoxSpringDetectorService


# Merged from box_detector.py on 2026-06-12

@dataclass(slots=True)
class BoxProfile:
    """箱体轮廓"""
    box_id: str
    timestamp: str
    is_box: bool = False
    box_type: str = ''
    upper: float = 0.0
    lower: float = 0.0
    median: float = 0.0
    width_pct: float = 0.0
    confidence: float = 0.0
    confidence_source: str = 'box'  # P1-29修复: 区分box/extreme来源
    duration_bars: int = 0
    bounce_count: int = 0
    adx: float = 0.0

    @property
    def is_valid(self) -> bool:
        return self.is_box and self.upper > self.lower > 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ExtremeState:
    """极值子状态"""
    timestamp: str
    extreme_type: str = ''
    is_bottom_extreme: bool = False
    is_top_extreme: bool = False
    price_position_pct: float = 0.0
    resonance_direction: str = ''
    resonance_strength: float = 0.0
    iv_percentile: float = 0.0
    iv_filter_passed: bool = False
    flow_exhaustion_detected: bool = False
    confidence: float = 0.0
    confidence_source: str = 'extreme'  # P1-29修复: 区分box/extreme来源
    tradeable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BoxStrategyParams:
    """箱体策略参数（比趋势策略更苛刻）"""
    max_hold_minutes: int = 30
    take_profit_ratio: float = 0.4
    stop_loss_ratio: float = 0.3
    max_risk_ratio: float = 0.05
    iv_percentile_min: float = 50.0
    signal_cooldown_sec: float = 60.0  # R27-P0-CD-06修复: 120.0→60.0，与config_params全局默认值对齐
    position_scale: float = 0.3
    lots_min: int = 1
    option_buy_lots_max: int = 10
    min_extreme_confidence: float = 0.6
    min_bounce_count: int = 2
    box_width_max_pct: float = 5.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BoxDetector:
    """箱体检测与极值判断引擎

    职责：
    1. 基于价格序列识别箱体（箱底/箱顶）
    2. 在箱体边界判断极值子状态
    3. IV过滤：仅在IV高位时允许箱体买方操作
    4. 订单流衰竭确认：大单停手、小单反向
    5. 生成箱体交易信号
    """

    ADX_DEFAULT_VALUE = 50.0
    ADX_MULTIPLIER = 100.0
    BOX_HISTORY_MAXLEN = 100
    FLOW_IMBALANCE_THRESHOLD = 0.2
    CVD_SLOPE_THRESHOLD = 0.01
    WIDTH_SCORE_WEIGHT = 0.25
    ADX_SCORE_WEIGHT = 0.25
    BOUNCE_SCORE_WEIGHT = 0.30
    PLR_SCORE_WEIGHT = 0.20
    PRICE_SCORE_WEIGHT = 0.25
    RESONANCE_SCORE_WEIGHT = 0.30
    IV_SCORE_WEIGHT = 0.25
    FLOW_SCORE_WEIGHT = 0.20
    BOTTOM_THRESHOLD_RATIO = 0.15
    TOP_THRESHOLD_RATIO = 0.15

    def __init__(
        self,
        params: Optional[BoxStrategyParams] = None,
        lookback_bars: int = 60,
        min_box_bars: int = 20,
        adx_period: int = 14,
        adx_threshold: float = 25.0,
        bounce_tolerance_pct: float = 0.1,
        iv_history_maxlen: int = 1000,
        box_gain_ratio: float = 0.5,
        plr_normalization_base: float = 3.0,
    ):
        self._lock = threading.RLock()
        self._params = params or BoxStrategyParams()
        self._lookback_bars = lookback_bars
        self._min_box_bars = min_box_bars
        self._adx_period = adx_period
        self._adx_threshold = adx_threshold
        self._bounce_tolerance_pct = bounce_tolerance_pct
        self._box_gain_ratio = box_gain_ratio
        self._plr_normalization_base = plr_normalization_base

        self._price_highs: deque = deque(maxlen=lookback_bars)
        self._price_lows: deque = deque(maxlen=lookback_bars)
        self._price_closes: deque = deque(maxlen=lookback_bars)
        self._volumes: deque = deque(maxlen=lookback_bars)
        self._timestamps: deque = deque(maxlen=lookback_bars)

        self._current_box: Optional[BoxProfile] = None
        self._box_history: deque = deque(maxlen=self.BOX_HISTORY_MAXLEN)
        self._extreme_state: Optional[ExtremeState] = None

        self._iv_history: deque = deque(maxlen=iv_history_maxlen)
        self._iv_sorted: List[float] = []
        self._bounce_at_upper: int = 0
        self._bounce_at_lower: int = 0

        self._stats = {
            'bars_processed': 0,
            'boxes_detected': 0,
            'bottom_extremes': 0,
            'top_extremes': 0,
            'iv_filtered': 0,
            'flow_exhaustion_confirmed': 0,
            'tradeable_signals': 0,
        }

        self._box_id_counter: int = 0
        logger.info("[BoxDetector] 初始化完成, lookback=%d, min_bars=%d",
                     lookback_bars, min_box_bars)

    @property
    def params(self) -> BoxStrategyParams:
        return self._params

    def update_bar(
        self,
        high: float,
        low: float,
        close: float,
        volume: float = 0.0,
        timestamp: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._price_highs.append(high)
            self._price_lows.append(low)
            self._price_closes.append(close)
            self._volumes.append(volume)
            self._timestamps.append(timestamp or datetime.now(CHINA_TZ).isoformat())
            self._stats['bars_processed'] += 1

    def update_iv(self, iv: float) -> None:
        with self._lock:
            if iv is None or iv <= 0:
                return
            if len(self._iv_history) == self._iv_history.maxlen:
                evicted = self._iv_history[0]
                self._iv_history.append(iv)
                try:
                    self._iv_sorted.remove(evicted)
                except ValueError:
                    pass
                insort(self._iv_sorted, iv)
            else:
                self._iv_history.append(iv)
                insort(self._iv_sorted, iv)

    @staticmethod
    def _compute_adx_simplified(highs, lows, closes, period: int = 14) -> float:
        if len(closes) < period + 1:
            return BoxDetector.ADX_DEFAULT_VALUE
        plus_dm_list = []
        minus_dm_list = []
        tr_list = []
        for i in range(1, min(len(closes), period + 1)):
            idx = len(closes) - 1 - i
            if idx < 0:
                break
            h_curr = highs[-(i)]
            l_curr = lows[-(i)]
            h_prev = highs[-(i + 1)]
            l_prev = lows[-(i + 1)]
            c_prev = closes[-(i + 1)]

            up_move = h_curr - h_prev
            down_move = l_prev - l_curr

            plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
            minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0

            tr1 = h_curr - l_curr
            tr2 = abs(h_curr - c_prev)
            tr3 = abs(l_curr - c_prev)
            tr = max(tr1, tr2, tr3)

            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)
            tr_list.append(tr)

        if not tr_list or sum(tr_list) < 1e-10:
            return BoxDetector.ADX_DEFAULT_VALUE

        avg_plus_dm = sum(plus_dm_list) / len(plus_dm_list)
        avg_minus_dm = sum(minus_dm_list) / len(minus_dm_list)
        avg_tr = sum(tr_list) / len(tr_list)

        if avg_tr < 1e-10:
            return BoxDetector.ADX_DEFAULT_VALUE

        plus_di = BoxDetector.ADX_MULTIPLIER * avg_plus_dm / avg_tr
        minus_di = BoxDetector.ADX_MULTIPLIER * avg_minus_dm / avg_tr

        di_sum = plus_di + minus_di
        if di_sum < 1e-10:
            return 0.0

        dx = BoxDetector.ADX_MULTIPLIER * abs(plus_di - minus_di) / di_sum
        return dx

    @staticmethod
    def _find_support_resistance(
        lows,
        highs,
        n_clusters: int = 2,
        tolerance_pct: float = 0.3,
    ) -> Tuple[List[float], List[float]]:
        if len(lows) < 5 or len(highs) < 5:
            return [], []

        low_list = sorted(lows)
        high_list = sorted(highs)

        def cluster(prices: List[float]) -> List[Tuple[float, int]]:
            if not prices:
                return []
            clusters = []
            current_center = prices[0]
            current_count = 1
            current_sum = prices[0]
            for p in prices[1:]:
                if abs(p - current_center) / max(abs(current_center), 1e-10) < tolerance_pct / 100.0:
                    current_count += 1
                    current_sum += p
                    current_center = current_sum / current_count
                else:
                    clusters.append((current_center, current_count))
                    current_center = p
                    current_count = 1
                    current_sum = p
            clusters.append((current_center, current_count))
            return clusters

        low_clusters = sorted(cluster(low_list), key=lambda x: x[1], reverse=True)
        high_clusters = sorted(cluster(high_list), key=lambda x: x[1], reverse=True)

        supports = [c[0] for c in low_clusters[:n_clusters]]
        resistances = [c[0] for c in high_clusters[:n_clusters]]

        return supports, resistances

    def detect_box(self) -> BoxProfile:
        with self._lock:
            now_str = datetime.now(CHINA_TZ).isoformat()
            self._box_id_counter += 1
            box_id = f"BOX-{self._box_id_counter:06d}"

            if len(self._price_closes) < self._min_box_bars:
                return BoxProfile(box_id=box_id, timestamp=now_str)

            closes = list(self._price_closes)
            highs = list(self._price_highs)
            lows = list(self._price_lows)

            recent_high = max(highs[-self._min_box_bars:])
            recent_low = min(lows[-self._min_box_bars:])
            recent_close = closes[-1]

            if recent_close < 1e-10:
                return BoxProfile(box_id=box_id, timestamp=now_str)

            width_pct = (recent_high - recent_low) / recent_close * 100.0

            adx = self._compute_adx_simplified(highs, lows, closes, period=self._adx_period)

            is_box = width_pct <= self._params.box_width_max_pct and adx < self._adx_threshold

            supports, resistances = self._find_support_resistance(lows, highs)

            if supports and resistances:
                box_lower = supports[0]
                box_upper = resistances[0]
            else:
                box_lower = recent_low
                box_upper = recent_high

            if box_upper <= box_lower:
                box_upper = recent_high
                box_lower = recent_low

            median = (box_upper + box_lower) / 2.0
            confidence = 0.0
            bounce_count = 0

            if is_box:
                tolerance = (box_upper - box_lower) * self._bounce_tolerance_pct
                for low in lows[-self._min_box_bars:]:
                    if abs(low - box_lower) <= tolerance:
                        bounce_count += 1
                for high in highs[-self._min_box_bars:]:
                    if abs(high - box_upper) <= tolerance:
                        bounce_count += 1

                width_score = max(0.0, 1.0 - width_pct / self._params.box_width_max_pct)
                adx_score = max(0.0, 1.0 - adx / self._adx_threshold)
                bounce_score = min(1.0, bounce_count / (self._params.min_bounce_count * 2))

                plr_score = 0.0
                box_height = box_upper - box_lower
                if box_height > 1e-10:
                    mid_price = (box_upper + box_lower) / 2.0
                    potential_loss = abs(mid_price - box_lower) if abs(mid_price - box_lower) > 1e-10 else 1e-10
                    potential_plr = BoxDetector.estimate_plr(box_height, potential_loss, self._box_gain_ratio)
                    plr_score = min(1.0, potential_plr / self._plr_normalization_base)

                confidence = self.WIDTH_SCORE_WEIGHT * width_score + self.ADX_SCORE_WEIGHT * adx_score + self.BOUNCE_SCORE_WEIGHT * bounce_score + self.PLR_SCORE_WEIGHT * plr_score

                is_box = bounce_count >= self._params.min_bounce_count

            profile = BoxProfile(
                box_id=box_id,
                timestamp=now_str,
                is_box=is_box,
                box_type='range' if is_box else '',
                upper=box_upper,
                lower=box_lower,
                median=median,
                width_pct=width_pct,
                confidence=confidence,
                duration_bars=len(closes),
                bounce_count=bounce_count,
                adx=adx,
            )

            if is_box:
                self._current_box = profile
                self._box_history.append(profile)
                self._stats['boxes_detected'] += 1
                self._bounce_at_lower = 0
                self._bounce_at_upper = 0

            return profile

    def get_current_box(self) -> Optional[BoxProfile]:
        with self._lock:
            return self._current_box

    def classify_extreme_state(
        self,
        current_price: float,
        resonance_direction: str = '',
        resonance_strength: float = 0.0,
        current_iv: float = 0.0,
        flow_imbalance: float = 0.0,
        cvd_slope: float = 0.0,
    ) -> ExtremeState:
        with self._lock:
            now_str = datetime.now(CHINA_TZ).isoformat()

            if self._current_box is None or not self._current_box.is_valid:
                return ExtremeState(timestamp=now_str)

            box = self._current_box
            box_range = box.upper - box.lower

            if box_range < 1e-10:
                return ExtremeState(timestamp=now_str)

            price_position_pct = (current_price - box.lower) / box_range * 100.0

            is_bottom = current_price <= box.lower + box_range * self.BOTTOM_THRESHOLD_RATIO
            is_top = current_price >= box.upper - box_range * self.TOP_THRESHOLD_RATIO

            is_bottom_extreme = False
            is_top_extreme = False
            extreme_type = ''

            if is_bottom and resonance_direction in ('fall', 'correct_fall', 'wrong_fall'):
                is_bottom_extreme = True
                extreme_type = 'box_bottom_extreme'
                self._stats['bottom_extremes'] += 1
            elif is_top and resonance_direction in ('rise', 'correct_rise', 'wrong_rise'):
                is_top_extreme = True
                extreme_type = 'box_top_extreme'
                self._stats['top_extremes'] += 1

            iv_percentile = self._compute_iv_percentile(current_iv)
            iv_filter_passed = iv_percentile >= self._params.iv_percentile_min if current_iv > 0 else False
            if not iv_filter_passed and current_iv > 0:
                self._stats['iv_filtered'] += 1

            imbalance_exhausted = abs(flow_imbalance) < self.FLOW_IMBALANCE_THRESHOLD
            cvd_stalling = abs(cvd_slope) < self.CVD_SLOPE_THRESHOLD
            flow_exhaustion = imbalance_exhausted or cvd_stalling
            if flow_exhaustion:
                self._stats['flow_exhaustion_confirmed'] += 1

            confidence = 0.0
            if extreme_type:
                price_score = 1.0 - min(price_position_pct, 100.0 - price_position_pct) / 50.0
                resonance_score = min(resonance_strength, 1.0)
                iv_score = iv_percentile / 100.0 if iv_filter_passed else 0.0
                flow_score = 1.0 if flow_exhaustion else 0.3

                confidence = (
                    self.PRICE_SCORE_WEIGHT * max(0.0, price_score) +
                    self.RESONANCE_SCORE_WEIGHT * resonance_score +
                    self.IV_SCORE_WEIGHT * iv_score +
                    self.FLOW_SCORE_WEIGHT * flow_score
                )

            tradeable = (
                extreme_type != '' and
                confidence >= self._params.min_extreme_confidence and
                iv_filter_passed and
                (is_bottom_extreme or is_top_extreme)
            )

            if tradeable:
                self._stats['tradeable_signals'] += 1

            state = ExtremeState(
                timestamp=now_str,
                extreme_type=extreme_type,
                is_bottom_extreme=is_bottom_extreme,
                is_top_extreme=is_top_extreme,
                price_position_pct=price_position_pct,
                resonance_direction=resonance_direction,
                resonance_strength=resonance_strength,
                iv_percentile=iv_percentile,
                iv_filter_passed=iv_filter_passed,
                flow_exhaustion_detected=flow_exhaustion,
                confidence=confidence,
                tradeable=tradeable,
            )

            self._extreme_state = state
            return state

    def check_iv_filter(self, current_iv: float) -> bool:
        with self._lock:
            if current_iv <= 0:
                return False
            percentile = self._compute_iv_percentile(current_iv)
            passed = percentile >= self._params.iv_percentile_min
            if not passed and current_iv > 0:
                self._stats['iv_filtered'] += 1
            return passed

    @staticmethod
    def compute_iv_percentile(iv_value: float, iv_sorted_list: List[float]) -> float:
        if not iv_sorted_list or iv_value <= 0:
            return 0.0
        count_below = bisect_left(iv_sorted_list, iv_value)
        return count_below / len(iv_sorted_list) * 100.0

    @staticmethod
    def estimate_plr(box_height: float, avg_loss: float, box_gain_ratio: float = 0.5) -> float:
        if box_height < 1e-10 or avg_loss < 1e-10:
            return 0.0
        potential_gain = box_height * box_gain_ratio
        return potential_gain / avg_loss

    def _compute_iv_percentile(self, current_iv: float) -> float:
        return BoxDetector.compute_iv_percentile(current_iv, self._iv_sorted)

    def check_order_flow_exhaustion(
        self,
        flow_imbalance: float,
        cvd_slope: float,
    ) -> bool:
        with self._lock:
            imbalance_exhausted = abs(flow_imbalance) < self.FLOW_IMBALANCE_THRESHOLD
            cvd_stalling = abs(cvd_slope) < self.CVD_SLOPE_THRESHOLD

            exhaustion = imbalance_exhausted or cvd_stalling

            if exhaustion:
                self._stats['flow_exhaustion_confirmed'] += 1

            return exhaustion

    def determine_trade_direction(self, extreme_state: ExtremeState) -> str:
        if extreme_state.is_bottom_extreme and extreme_state.tradeable:
            return 'long'
        elif extreme_state.is_top_extreme and extreme_state.tradeable:
            return 'short'
        return ''

    def get_extreme_state(self) -> Optional[ExtremeState]:
        with self._lock:
            return self._extreme_state

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'component': 'box_detector',
                'status': 'OK',
                'current_box': self._current_box.to_dict() if self._current_box else None,
                'extreme_state': self._extreme_state.to_dict() if self._extreme_state else None,
                'bars_processed': self._stats['bars_processed'],
                'boxes_detected': self._stats['boxes_detected'],
                'tradeable_signals': self._stats['tradeable_signals'],
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['current_box_valid'] = self._current_box.is_valid if self._current_box else False
            stats['iv_history_size'] = len(self._iv_history)
            stats['price_bars'] = len(self._price_closes)
            stats['adx_period'] = self._adx_period
            stats['adx_threshold'] = self._adx_threshold
            stats['bounce_tolerance_pct'] = self._bounce_tolerance_pct
            return stats

    def get_box_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return [b.to_dict() for b in list(self._box_history)[-limit:]]

    def estimate_potential_plr(self, current_price: float, direction: str) -> float:
        with self._lock:
            if not self._current_box or self._current_box.lower is None or self._current_box.upper is None:
                return 0.0
            box_bottom = self._current_box.lower
            box_top = self._current_box.upper
            box_height = box_top - box_bottom
            if box_height < 1e-10:
                return 0.0
            if direction == 'long':
                risk = current_price - box_bottom
                reward = box_top - current_price
            else:
                risk = box_top - current_price
                reward = current_price - box_bottom
            if risk < 1e-10:
                return 0.0
            return reward / risk


_box_detector: Optional[BoxDetector] = None
_box_detector_lock = threading.Lock()


def get_box_detector(**kwargs) -> BoxDetector:
    global _box_detector
    with _box_detector_lock:
        if _box_detector is None:
            if not kwargs:
                try:
                    from ali2026v3_trading.config.config_service import get_cached_params
                    all_params = get_cached_params()
                    detector_keys = ['box_gain_ratio', 'plr_normalization_base']
                    for k in detector_keys:
                        if k in all_params and k not in kwargs:
                            kwargs[k] = all_params[k]
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
            _box_detector = BoxDetector(**kwargs)
        return _box_detector


__all__ = [
    'BoxSpringDetectorService',
    'BoxSpringDetectorMixin',
    # from _box_spring_types.py
    'SpringState',
    'BoxRange',
    'SpringSignal',
    'SpringPosition',
    'PendingPullback',
    'get_box_spring_strategy',
    # from box_detector.py
    'BoxDetector',
    'BoxProfile',
    'ExtremeState',
    'BoxStrategyParams',
    'get_box_detector',
]
