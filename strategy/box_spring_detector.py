"""
box_spring_detector.py - 箱体波动率脉冲策略（弹簧策略）- 检测与类型定义

合并自:
- box_spring_detector.py (检测Mixin)
- _box_spring_types.py (类型定义)
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from infra.shared_utils import CHINA_TZ, TRADING_DAYS_PER_YEAR_CHINA
from infra.resilience import safe_float_to_int


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
                        from config.config_service import get_cached_params
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
                from strategy.box_spring_strategy_impl import BoxSpringStrategy
                _box_spring_instance = BoxSpringStrategy(params or {})
    return _box_spring_instance



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
        # FIX-OO5 (S4-Spring-ROOT): 放宽IV百分位阈值
        # 根因: iv_low_percentile=5.0过严，update_iv历史≥5后真实百分位远>5.0→
        #       detect_spring L417 iv_pct>5.0→return None→S4零信号(1659次拦截/97.4%)
        # 修复: 5.0→20.0，允许IV百分位前20%的候选通过(仍保持低IV偏好)
        self._iv_low_percentile = _p.get('iv_low_percentile', 20.0)
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
        from infra.shared_utils import PullbackManager
        self._pullback_mgr = PullbackManager({
            'pullback_enabled': self._pullback_enabled,
            'pullback_wait_bars': self._pullback_wait_bars,
            'pullback_retrace_pct': self._pullback_retrace_pct,
            'pullback_iv_min_percentile': self._pullback_iv_min_percentile,
            'pullback_iv_max_percentile': self._pullback_iv_max_percentile,
        })
        try:
            from strategy.box_detector import get_box_detector
            self._box_detector = get_box_detector()
        except (ImportError, RuntimeError):
            from strategy.box_detector import BoxDetector
            self._box_detector = BoxDetector()

    def _get_now(self) -> datetime:
        if self._current_bar_time is not None:
            return datetime.fromtimestamp(self._current_bar_time, tz=CHINA_TZ)
        return datetime.now(CHINA_TZ)

    # [FIX-20260712-AUDIT-P1] 删除重复的_invalidate_box定义，保留L361的info级别版本
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
            # FIX-S3S4-11: 原逻辑仅当is_box=False时才走fallback，但当targets数据无high/low字段
            # 导致high=low=price时，BoxDetector识别出零宽度箱体(is_box=True, upper==lower)，
            # 其is_valid=False(upper>lower不成立)。原L274条件not is_box为False→L276返回None，
            # 永远不走fallback→BoxRange从不创建→detect_spring L403返回None→S4信号链路完全断裂。
            # 修复：只要box_profile无效(None/is_box=False/is_valid=False)，统一走fallback路径。
            return self._update_box_fallback(instrument_id, high, low, close, ts)

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
            touch_count=self._min_box_touches,  # FIX-S3S4-4: fallback箱体初始touch_count设为min_box_touches，避免get_active_box因touch_count<3返回None
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
                return 5.0  # FIX-S3S4-3: IV历史不足时返回5.0(极低分位)而非50.0(中位)，避免iv_pct>iv_low_percentile(5.0)过滤掉所有信号

            sorted_ivs = sorted(history)
            from strategy.box_detector import BoxDetector
            return BoxDetector.compute_iv_percentile(iv, sorted_ivs)

    def get_iv_percentile(self, instrument_id: str) -> float:
        with self._lock:
            history = self._iv_history.get(instrument_id)
            if not history or len(history) < 5:
                return 50.0
            current_iv = history[-1]
            sorted_ivs = sorted(history)
            from strategy.box_detector import BoxDetector
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
            open_reason='BOX_SPRING',  # [FIX-20260712-S4-P0] 必须设置，否则prevent_trend_conversion自锁
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

    # [FIX-20260712-S4] 弹簧强度评分模块 — 上策H-Rev
    # 原理: 弹簧突破的强度取决于IV压缩程度、价格位置、箱体触底次数、gamma暴露
    # 评分 < 阈值的信号应被过滤，避免低质量弹簧信号驱动交易
    def compute_spring_strength(self, iv_percentile: float, price_pos: float,
                                 gamma_exposure: float, box: Optional[BoxRange]) -> float:
        """计算弹簧强度评分

        Args:
            iv_percentile: IV百分位（越低=压缩越深=弹簧越强）
            price_pos: 价格在箱体中的位置 0-1（0.5=中间=最强）
            gamma_exposure: gamma暴露值
            box: 当前箱体

        Returns:
            float: 弹簧强度评分 0.0-1.0（>0.5为有效信号）
        """
        # 维度1: IV压缩分（IV越低，弹簧蓄能越强）— 权重0.35
        # iv_percentile 0-5% → score 1.0; 5-20% → score 0.5; >20% → score 0.0
        if iv_percentile <= self._iv_very_low_percentile:
            iv_score = 1.0
        elif iv_percentile <= self._iv_low_percentile:
            iv_score = 0.5
        else:
            iv_score = 0.0

        # 维度2: 价格位置分（越接近箱体中央=0.5，弹簧越平衡）— 权重0.25
        # price_pos 0.5 → score 1.0; 偏离0.5 → 线性递减
        pos_score = 1.0 - abs(price_pos - 0.5) * 2.0
        pos_score = max(0.0, min(1.0, pos_score))

        # 维度3: 箱体触底次数分（更多触底=箱体越可靠）— 权重0.20
        # 需要从box对象获取触底次数
        touch_count = 0
        if box is not None:
            touch_count = getattr(box, 'touch_count', 0) or getattr(box, 'bounces', 0)
        touch_score = min(1.0, touch_count / max(1, self._min_box_touches))

        # 维度4: Gamma暴露分（gamma越高=弹性越大）— 权重0.20
        # gamma_exposure 归一化到 0-1（假设 0-0.1 范围）
        gamma_score = min(1.0, abs(gamma_exposure) / 0.1) if gamma_exposure != 0 else 0.0

        # 加权综合
        strength = (
            0.35 * iv_score +
            0.25 * pos_score +
            0.20 * touch_score +
            0.20 * gamma_score
        )

        return max(0.0, min(1.0, strength))

    # 弹簧强度阈值
    # FIX-OO5b (S4-Spring-ROOT): 降低弹簧强度阈值
    # 根因: 零宽箱体+fallback数据下strength极易<0.45→continue跳过→S4零信号
    # 修复: 0.45→0.25，允许中等强度弹簧信号通过
    SPRING_STRENGTH_THRESHOLD = 0.25  # 低于此值的信号被过滤

    def detect_spring_strength(self, signal: 'SpringSignal', box: Optional['BoxRange']) -> float:
        """从SpringSignal提取参数计算弹簧强度（供BoxSpringStrategy委托调用）"""
        if signal is None:
            return 0.0
        # 从信号对象提取参数
        iv_pct = getattr(signal, 'iv_percentile', 50.0)
        gamma_exp = getattr(signal, 'gamma_exposure', 0.0)
        current_price = getattr(signal, 'current_price', 0.0)
        # 计算价格位置
        price_pos = 0.5
        if box is not None and hasattr(box, 'price_position'):
            try:
                price_pos = box.price_position(current_price)
            except (ValueError, TypeError):
                pass
        return self.compute_spring_strength(iv_pct, price_pos, gamma_exp, box)

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
        from strategy.box_detector import BoxDetector
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

        # FIX-S3S4-9: 当订单流和期权链数据均不可用(=0.0)时，默认触发
        # 根因: targets数据中无order_flow_imbalance/option_chain_activity字段，fallback=0.0导致触发条件永远不满足
        if not triggered and order_flow_imbalance == 0.0 and option_chain_activity == 0.0:
            triggered = True
            trigger_reason = 'no_flow_data_default'

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
            strategy_type='s4_spring',
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
