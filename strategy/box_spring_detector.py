"""
box_spring_detector.py - 箱体波动率脉冲策略（弹簧策略）- 检测Mixin

包含箱体识别、IV监控、弹簧信号识别、扫描等检测相关方法。
从BoxSpringStrategy类中提取，作为Mixin使用。
"""
from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.shared_utils import CHINA_TZ, TRADING_DAYS_PER_YEAR_CHINA
from ali2026v3_trading.infra.resilience import safe_float_to_int
from ali2026v3_trading.strategy._box_spring_types import (
    SpringState, BoxRange, SpringSignal, PendingPullback,
)



class BoxSpringDetectorService:

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        import threading
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
            from ali2026v3_trading.strategy.box_detector import get_box_detector
            self._box_detector = get_box_detector()
        except (ImportError, RuntimeError):
            from ali2026v3_trading.strategy.box_detector import BoxDetector
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
        import math
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
            from ali2026v3_trading.strategy.box_detector import BoxDetector
            return BoxDetector.compute_iv_percentile(iv, sorted_ivs)

    def get_iv_percentile(self, instrument_id: str) -> float:
        with self._lock:
            history = self._iv_history.get(instrument_id)
            if not history or len(history) < 5:
                return 50.0
            current_iv = history[-1]
            sorted_ivs = sorted(history)
            from ali2026v3_trading.strategy.box_detector import BoxDetector
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
            import math
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
        from ali2026v3_trading.strategy.box_detector import BoxDetector
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

    # ========================================================================
    # 扫描方法
    # ========================================================================

    def scan_springs(self, instruments_data: Dict) -> List[SpringSignal]:
        """扫描所有标的的弹簧信号。

        Args:
            instruments_data: 标的数据字典，格式为
                {instrument_id: {future_price, option_instrument_id, strike_price,
                                 iv, premium_price, days_to_expiry, account_equity}}

        Returns:
            检测到的 SpringSignal 列表
        """
        results: List[SpringSignal] = []
        if not instruments_data:
            return results
        for instrument_id, data in instruments_data.items():
            if not isinstance(data, dict):
                continue
            signal = self.detect_spring(
                instrument_id=instrument_id,
                future_price=data.get('future_price', 0.0),
                option_instrument_id=data.get('option_instrument_id', ''),
                strike_price=data.get('strike_price', 0.0),
                iv=data.get('iv', 0.0),
                premium_price=data.get('premium_price', 0.0),
                days_to_expiry=data.get('days_to_expiry', 0),
                account_equity=data.get('account_equity', 100000.0),
            )
            if signal is not None:
                results.append(signal)
        return results

    def _scan_from_sort_buckets(self, buckets: Dict) -> List[SpringSignal]:
        """从排序后的IV分桶数据中扫描弹簧信号。

        Args:
            buckets: IV分桶字典，格式为
                {bucket_key: [{instrument_id, future_price, option_instrument_id,
                               strike_price, iv, premium_price, days_to_expiry, ...}]}

        Returns:
            检测到的 SpringSignal 列表
        """
        results: List[SpringSignal] = []
        if not buckets:
            return results
        for bucket_key, items in buckets.items():
            if not isinstance(items, (list, tuple)):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                instrument_id = item.get('instrument_id', '')
                if not instrument_id:
                    continue
                signal = self.detect_spring(
                    instrument_id=instrument_id,
                    future_price=item.get('future_price', 0.0),
                    option_instrument_id=item.get('option_instrument_id', ''),
                    strike_price=item.get('strike_price', 0.0),
                    iv=item.get('iv', 0.0),
                    premium_price=item.get('premium_price', 0.0),
                    days_to_expiry=item.get('days_to_expiry', 0),
                    account_equity=item.get('account_equity', 100000.0),
                )
                if signal is not None:
                    results.append(signal)
        return results

    def _scan_from_option_info(self, option_info: Dict) -> List[SpringSignal]:
        """从期权链信息中扫描弹簧信号。

        Args:
            option_info: 期权信息字典，格式为
                {option_instrument_id: {instrument_id, future_price, strike_price,
                                        iv, premium_price, days_to_expiry, ...}}

        Returns:
            检测到的 SpringSignal 列表
        """
        results: List[SpringSignal] = []
        if not option_info:
            return results
        for option_instrument_id, info in option_info.items():
            if not isinstance(info, dict):
                continue
            instrument_id = info.get('instrument_id', '')
            if not instrument_id:
                continue
            signal = self.detect_spring(
                instrument_id=instrument_id,
                future_price=info.get('future_price', 0.0),
                option_instrument_id=option_instrument_id,
                strike_price=info.get('strike_price', 0.0),
                iv=info.get('iv', 0.0),
                premium_price=info.get('premium_price', 0.0),
                days_to_expiry=info.get('days_to_expiry', 0),
                account_equity=info.get('account_equity', 100000.0),
            )
            if signal is not None:
                results.append(signal)
        return results

    def _evaluate_candidate(self, instrument_id: str, option_data: Dict) -> Optional[SpringSignal]:
        """评估单个候选标的是否产生弹簧信号。

        Args:
            instrument_id: 标的ID
            option_data: 期权数据字典，包含 future_price, option_instrument_id,
                         strike_price, iv, premium_price, days_to_expiry, account_equity

        Returns:
            检测到的 SpringSignal 或 None
        """
        if not option_data or not isinstance(option_data, dict):
            return None
        return self.detect_spring(
            instrument_id=instrument_id,
            future_price=option_data.get('future_price', 0.0),
            option_instrument_id=option_data.get('option_instrument_id', ''),
            strike_price=option_data.get('strike_price', 0.0),
            iv=option_data.get('iv', 0.0),
            premium_price=option_data.get('premium_price', 0.0),
            days_to_expiry=option_data.get('days_to_expiry', 0),
            account_equity=option_data.get('account_equity', 100000.0),
        )

BoxSpringDetectorMixin = BoxSpringDetectorService
