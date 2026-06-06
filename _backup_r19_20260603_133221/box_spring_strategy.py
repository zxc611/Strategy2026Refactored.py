"""
box_spring_strategy.py - 箱体波动率脉冲策略（弹簧策略）

策略本质：交易波动率的脉冲，而非价格的方向
利润来源：波动率从极低回归正常 + Gamma在行权价附近的非线性收益

识别条件（弹簧被压紧）：
1. 箱体结构已识别（价格在箱顶箱底之间震荡）
2. 近月期权（2-5天到期），权利金极便宜
3. IV百分位处于近期极低水平（下分位数）
4. 期货价格接近该期权行权价（Gamma最敏感位置）

入场信号（箱内脉冲）：
- 弹簧压紧后，任何波动率回归迹象即可入场
- 订单流出现异动，或全链期权从死寂状态略有反应

平仓纪律（铁律）：
- 止盈：弹簧松开即走，盈亏比达1.5:1或2:1时立刻平仓
- 止损：接受归零（成本极低，占总资金0.5%-1.5%）
- 铁律：绝不平移为趋势策略

期望值模型：
- 胜率 ~20%，盈亏比 ~5:1
- 期望值 = 20%*5 - 80%*1 = +0.2 > 0（正期望）

作者：AI代码助手
版本：v1.0
日期：2026-05-10
"""
from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone  # ENV-P1-01修复: 导入timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple
from ali2026v3_trading.resilience_utils import deterministic_round, safe_float_to_int
from ali2026v3_trading.shared_utils import CHINA_TZ, TRADING_DAYS_PER_YEAR_CHINA


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
        """NP-P1-11修复: 使用容差比较替代浮点<=，避免边界精度问题"""
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
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))  # ENV-P1-01修复: UTC时区
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
        return min(self.current_premium / self.entry_premium, 10.0)  # NP-P2-11: pnl_ratio上界clip

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
                        from ali2026v3_trading.config_params import get_cached_params
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
                _box_spring_instance = BoxSpringStrategy(params or {})
    return _box_spring_instance


class BoxSpringStrategy:
    OPEN_REASON = 'BOX_SPRING'

    def __init__(self, params: Dict[str, Any]):
        # R13-P2-API-10修复: 外部dict参数验证
        if not isinstance(params, dict):
            logging.warning("[BoxSpringStrategy] params不是dict类型: %s，使用空dict", type(params))
            params = {}
        # R15-P2-API-10修复: 关键参数schema校验与类型修正
        _param_schema = {
            'iv_lookback_bars': int, 'min_box_touches': int, 'max_box_width_pct': float,
            'iv_low_percentile': float, 'iv_very_low_percentile': float,
            'spring_threshold': float, 'dynamic_tp_sl': bool,
        }
        for _pk, _pt in _param_schema.items():
            if _pk in params and not isinstance(params[_pk], _pt):
                try:
                    params[_pk] = _pt(params[_pk])
                except (ValueError, TypeError):
                    logging.warning("[BoxSpringStrategy] R15-P2-API-10: 参数%s类型修正失败, 期望%s", _pk, _pt.__name__)
        self.params = params
        self._lock = threading.RLock()

        self._boxes: Dict[str, BoxRange] = {}
        self._iv_history: Dict[str, deque] = {}
        self._iv_window = params.get('iv_lookback_bars', 120)

        try:
            from ali2026v3_trading.box_detector import get_box_detector
            self._box_detector = get_box_detector()
        except (ImportError, RuntimeError) as e:
            logging.debug("[BoxSpringStrategy] get_box_detector failed: %s", e)
            from ali2026v3_trading.box_detector import BoxDetector
            self._box_detector = BoxDetector()

        self._signals: Dict[str, SpringSignal] = {}
        self._positions: Dict[str, SpringPosition] = {}

        self._min_box_touches = params.get('min_box_touches', 3)
        self._max_box_width_pct = params.get('max_box_width_pct', 0.04)
        self._iv_low_percentile = params.get('iv_low_percentile', 5.0)
        self._iv_very_low_percentile = params.get('iv_very_low_percentile', 2.0)
        self._min_days_to_expiry = params.get('min_days_to_expiry', 2)
        self._max_days_to_expiry = params.get('max_days_to_expiry', 5)
        self._max_premium_cost_pct = params.get('max_premium_cost_pct', 0.015)
        self._stop_profit_ratio = params.get('stop_profit_ratio', 5.0)
        self._max_loss_pct = params.get('max_loss_pct', 0.95)
        self._max_position_pct = params.get('max_position_pct', 0.015)
        self._dynamic_tp_sl_enabled = params.get('dynamic_tp_sl_enabled', True)  # R14-P1-DEAD-06修复: 默认启用动态止盈止损
        self._min_estimated_plr = params.get('min_estimated_plr', 0.0)
        self._capital_scale: str = params.get('capital_scale', 'medium')
        self._cooldown_sec = params.get('spring_cooldown_sec', 300)
        self._max_active_positions = params.get('max_spring_positions', None)
        if self._max_active_positions is None:  # P1-4修复: 优先从统一参数max_open_positions读取
            try:
                from ali2026v3_trading.config_params import get_param
                self._max_active_positions = int(get_param('max_open_positions', get_param('max_position_count', 3)))
            except Exception:
                self._max_active_positions = 3

        self._pullback_enabled = params.get('pullback_enabled', False)
        self._pullback_wait_bars = params.get('pullback_wait_bars', 5)
        self._pullback_retrace_pct = params.get('pullback_retrace_pct', 0.15)
        self._pullback_iv_min_percentile = params.get('pullback_iv_min_percentile', 20.0)
        self._pullback_iv_max_percentile = params.get('pullback_iv_max_percentile', 80.0)
        self._pending_pullbacks: Dict[str, PendingPullback] = {}
        self._pullback_bar_counter: int = 0
        self._pullback_stats = {
            'signals_deferred': 0,
            'retrace_entries': 0,
            'retrace_timeouts': 0,
            'retrace_iv_rejected': 0,
        }

        self._box_breakout_tolerance = params.get('box_breakout_tolerance', 0.005)
        self._spring_price_pos_min = params.get('spring_price_pos_min', 0.3)
        self._spring_price_pos_max = params.get('spring_price_pos_max', 0.7)
        self._strike_distance_threshold = params.get('strike_distance_threshold', 0.02)
        self._direction_buy_call_threshold = params.get('direction_buy_call_threshold', 0.45)
        self._direction_buy_put_threshold = params.get('direction_buy_put_threshold', 0.55)
        # R24-P2-CF-02修复 + R26-P2修复: assert改为运行时检查，防止-O优化跳过
        if not (self._direction_buy_call_threshold < self._direction_buy_put_threshold):
            raise ValueError(
                f"direction_buy_call_threshold({self._direction_buy_call_threshold}) must be < "
                f"direction_buy_put_threshold({self._direction_buy_put_threshold})"
            )
        self._max_risk_ratio = params.get('max_risk_ratio', 0.8)  # R24-P0-DF-01修复: max_risk_ratio回退值0.3→0.8，与系统默认值对齐
        self._option_multiplier = params.get('option_multiplier', 10000)
        self._fallback_strike_search = params.get('fallback_strike_search', True)

        from ali2026v3_trading.shared_utils import PullbackManager
        self._pullback_tick_tracking: bool = params.get('pullback_tick_tracking', True)
        self._pullback_mgr = PullbackManager({
            'pullback_enabled': self._pullback_enabled,
            'pullback_wait_bars': self._pullback_wait_bars,
            'pullback_retrace_pct': self._pullback_retrace_pct,
            'pullback_iv_min_percentile': self._pullback_iv_min_percentile,
            'pullback_iv_max_percentile': self._pullback_iv_max_percentile,
            'pullback_tick_tracking': self._pullback_tick_tracking,
        })

        self._current_bar_time: Optional[float] = None
        self._last_signal_time: Dict[str, float] = {}

        self._stats = {
            'total_signals': 0,
            'compressed_detected': 0,
            'triggers_fired': 0,
            'positions_opened': 0,
            'positions_closed_tp': 0,
            'positions_closed_sl': 0,
            'positions_closed_expired': 0,
            'total_pnl': 0.0,
            'win_count': 0,
            'loss_count': 0,
        }

    def set_capital_scale(self, capital_scale: str) -> None:
        with self._lock:
            old_scale = self._capital_scale
            self._capital_scale = capital_scale
            logging.info(
                '[BoxSpringStrategy] capital_scale changed: %s -> %s',
                old_scale, capital_scale,
            )

    def get_capital_scale(self) -> str:
        with self._lock:
            return self._capital_scale

    def _get_now(self) -> datetime:
        if self._current_bar_time is not None:
            return datetime.fromtimestamp(self._current_bar_time, tz=CHINA_TZ)
        return datetime.now(CHINA_TZ)

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
            from ali2026v3_trading.box_detector import BoxDetector
            return BoxDetector.compute_iv_percentile(iv, sorted_ivs)

    def get_iv_percentile(self, instrument_id: str) -> float:
        with self._lock:
            history = self._iv_history.get(instrument_id)
            if not history or len(history) < 5:
                return 50.0
            current_iv = history[-1]
            sorted_ivs = sorted(history)
            from ali2026v3_trading.box_detector import BoxDetector
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
        from ali2026v3_trading.box_detector import BoxDetector
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
    # 资金自动推算手数 + 同月低权利金行权价降级选择
    # ========================================================================

    def compute_equity_based_lots(
        self,
        premium_price: float,
        account_equity: float,
        max_loss_pct: Optional[float] = None,
        instrument_id: str = '',  # R13-P2-BIZ-01修复: 新增instrument_id参数用于查找已有持仓
    ) -> int:
        """
        根据实时资金和权利金自动推算下单手数。
        公式: lots = floor(account_equity * max_risk_ratio / (premium_price * 10000))
        其中 premium_price * 10000 为单手权利金成本(期权1手=10000张)。
        至少1手，受max_position_pct约束。

        R13-P2-BIZ-01修复: 计算lots时应减去该合约已有的持仓手数，
        避免在已有持仓的情况下重复计算导致超仓。
        """
        if premium_price <= 0 or account_equity <= 0:
            return 1
        max_loss = max_loss_pct if max_loss_pct is not None else self._max_loss_pct
        risk_budget = account_equity * self._max_risk_ratio
        cost_per_lot = premium_price * self._option_multiplier
        # R27-P2-FP-17修复: int()截断→safe_float_to_int()
        lots = max(1, safe_float_to_int(risk_budget / cost_per_lot))
        max_lots_by_position_pct = max(1, safe_float_to_int(account_equity * self._max_position_pct / cost_per_lot))
        lots = min(lots, max_lots_by_position_pct)

        # R13-P2-BIZ-01修复: 减去该合约已有的持仓手数
        if instrument_id and hasattr(self, '_positions') and self._positions:
            existing_lots = 0
            for pos in self._positions.values():
                pos_inst = getattr(pos, 'instrument_id', '') if hasattr(pos, 'instrument_id') else ''
                if pos_inst == instrument_id:
                    pos_lots = abs(getattr(pos, 'lots', 0) or getattr(pos, 'volume', 0) or 0)
                    existing_lots += pos_lots
            lots = max(0, lots - existing_lots)
            if lots <= 0:
                logging.info("[BoxSpring] R13-P2-BIZ-01: instrument=%s 已有持仓%d手, 无需额外开仓", instrument_id, existing_lots)
                return 0

        return lots

    def find_cheaper_strike_same_month(
        self,
        instrument_id: str,
        current_premium: float,
        account_equity: float,
        direction: str,
    ) -> Optional[Dict[str, Any]]:
        """
        资金不足时，在同月期权中选择权利金较小的行权价品种。
        按权利金升序排列同月同类型期权，返回能买1手的最低权利金候选。
        """
        if not self._fallback_strike_search:
            return None
        try:
            from ali2026v3_trading.width_cache import get_width_cache
            cache = get_width_cache()
            if cache is None:
                return None
        except (ImportError, RuntimeError):
            return None

        current_info = None
        for iid, info in cache._option_info.items():
            if iid == instrument_id or info.get('instrument_id', '') == instrument_id:
                current_info = info
                break
        if current_info is None:
            return None

        current_month = current_info.get('month', '')
        opt_type = current_info.get('option_type', 'CALL')
        underlying_fid = current_info.get('underlying_future_id', '')

        candidates = []
        for iid, info in cache._option_info.items():
            if info.get('underlying_future_id', '') != underlying_fid:
                continue
            if info.get('option_type', 'CALL') != opt_type:
                continue
            if info.get('month', '') != current_month:
                continue
            premium = info.get('last_price', 0.0)
            if premium <= 0 or premium >= current_premium:
                continue
            strike = info.get('strike_price', 0.0)
            if strike <= 0:
                continue
            cost_per_lot = premium * self._option_multiplier
            if cost_per_lot > account_equity * self._max_risk_ratio:
                continue
            distance = abs(info.get('underlying_price', 0) - strike) / max(info.get('underlying_price', 1), 1)
            if distance > self._strike_distance_threshold * 2:
                continue
            candidates.append({
                'instrument_id': info.get('instrument_id', iid),
                'strike_price': strike,
                'premium_price': premium,
                'distance': distance,
            })

        if not candidates:
            return None

        candidates.sort(key=lambda x: x['premium_price'])
        best = candidates[0]
        logging.info(
            "[BoxSpring] FALLBACK_STRIKE: %s premium=%.4f->%.4f strike=%.1f->%.1f month=%s",
            direction, current_premium, best['premium_price'],
            current_info.get('strike_price', 0), best['strike_price'], current_month,
        )
        return best

    # ========================================================================
    # 下单执行
    # ========================================================================

    def execute_spring_entry(self, signal: SpringSignal) -> Optional[str]:
        with self._lock:
            active_count = sum(1 for p in self._positions.values() if p.is_open)
            if active_count >= self._max_active_positions:
                logging.debug("[BoxSpring] Max positions reached: %d", active_count)
                return None

            if not self._check_cross_strategy_risk(signal):
                logging.warning("[BoxSpring] 跨策略风控阻断, 跳过Spring入场")
                return None

            # ✅ P1-10修复: 铁律检查——防止趋势转换
            if not self.prevent_trend_conversion(
                signal.instrument_id, 'OPEN', signal.open_reason
            ):
                logging.warning("[BoxSpring] 铁律阻断: 趋势转换被阻止 %s", signal.instrument_id)
                return None

            estimated_plr = 0.0
            if self._min_estimated_plr > 0 or self._dynamic_tp_sl_enabled:
                estimated_plr = self.estimate_plr_before_entry(signal.instrument_id)
                if self._min_estimated_plr > 0 and estimated_plr < self._min_estimated_plr:
                    logging.debug("[BoxSpring] PLR过滤: estimated_plr=%.2f < min=%.2f", estimated_plr, self._min_estimated_plr)
                    return None

            try:
                from ali2026v3_trading.order_service import get_order_service
                osvc = get_order_service()
                if not osvc:
                    return None

                self._record_spring_trade(signal)

                if signal.direction == 'BUY_STRADDLE':
                    return self._execute_straddle_entry(signal)

                action_map = {
                    'BUY_CALL': ('BUY', 'OPEN'),
                    'BUY_PUT': ('BUY', 'OPEN'),
                }
                direction, action = action_map.get(signal.direction, ('BUY', 'OPEN'))

                equity_lots = self.compute_equity_based_lots(
                    signal.premium_price, signal.account_equity if hasattr(signal, 'account_equity') else 100000.0,
                )
                actual_lots = min(signal.lots, equity_lots) if signal.lots > 0 else equity_lots

                if actual_lots <= 0:
                    cheaper = self.find_cheaper_strike_same_month(
                        signal.option_instrument_id, signal.premium_price,
                        signal.account_equity if hasattr(signal, 'account_equity') else 100000.0,
                        signal.direction,
                    )
                    if cheaper is None:
                        logging.warning("[BoxSpring] 资金不足且无更低权利金候选, 跳过: %s", signal.option_instrument_id)
                        return None
                    signal.option_instrument_id = cheaper['instrument_id']
                    signal.strike_price = cheaper['strike_price']
                    signal.premium_price = cheaper['premium_price']
                    actual_lots = self.compute_equity_based_lots(
                        cheaper['premium_price'],
                        signal.account_equity if hasattr(signal, 'account_equity') else 100000.0,
                    )
                    logging.info("[BoxSpring] 降级选择低权利金行权价: %s lots=%d", cheaper['instrument_id'], actual_lots)

                signal.lots = actual_lots

                order_id = osvc.send_order(
                    instrument_id=signal.option_instrument_id,
                    volume=signal.lots,
                    price=signal.premium_price,
                    direction=direction,
                    action=action,
                    open_reason=self.OPEN_REASON,
                    signal_id=getattr(signal, 'signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
                )

                if order_id:
                    # R27-P2-FP-15修复: int()截断→safe_float_to_int()
                    pos_id = f"SIG_POS_{signal.option_instrument_id}_{safe_float_to_int(time.time()*1000)}"
                    position = SpringPosition(
                        position_id=pos_id,
                        signal_id=signal.signal_id,
                        instrument_id=signal.instrument_id,
                        option_instrument_id=signal.option_instrument_id,
                        direction=signal.direction,
                        entry_premium=signal.premium_price,
                        current_premium=signal.premium_price,
                        entry_time=self._get_now(),
                        stop_profit_ratio=self._stop_profit_ratio,
                        max_loss_pct=self._max_loss_pct,
                        box_id=signal.box_id,
                        lots=signal.lots,
                    )
                    self._positions[pos_id] = position
                    if self._dynamic_tp_sl_enabled and estimated_plr > 0:
                        position.adjust_tp_sl_by_plr(estimated_plr)
                    signal.spring_state = SpringState.ACTIVE
                    signal.is_consumed = True
                    self._stats['positions_opened'] += 1

                    logging.info(
                        "[BoxSpring] ENTRY: %s dir=%s premium=%.4f stop_profit=%.1fx max_loss=%.0f%%",
                        signal.option_instrument_id, signal.direction, signal.premium_price,
                        self._stop_profit_ratio, self._max_loss_pct * 100
                    )
                    return order_id

            except Exception as e:
                logging.error("[BoxSpring] Entry error: %s", e)

        return None

    def _execute_straddle_entry(self, signal: SpringSignal) -> Optional[str]:
        paired_instrument_id, paired_premium, signal_opt_type = self._find_straddle_pair(signal)

        if not paired_instrument_id or not signal_opt_type:
            logging.warning("[BoxSpring] STRADDLE: no pair found for %s (opt_type=%s)",
                            signal.option_instrument_id, signal_opt_type)
            return None

        is_call = signal_opt_type == 'CALL'
        call_instrument = signal.option_instrument_id if is_call else paired_instrument_id
        put_instrument = paired_instrument_id if is_call else signal.option_instrument_id
        call_premium = signal.premium_price if is_call else paired_premium
        put_premium = paired_premium if is_call else signal.premium_price

        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if not osvc:
                return None
        except Exception as e:
            logging.error("[BoxSpring] STRADDLE: get_order_service failed: %s", e)
            return None

        call_order_id = osvc.send_order(
            instrument_id=call_instrument,
            volume=signal.lots,
            price=call_premium,
            direction='BUY',
            action='OPEN',
            open_reason=self.OPEN_REASON,
            signal_id=getattr(signal, 'signal_id', ''),
        )

        if not call_order_id:
            logging.warning("[BoxSpring] STRADDLE: Call order failed for %s, aborting straddle",
                            call_instrument)
            return None

        put_order_id = osvc.send_order(
            instrument_id=put_instrument,
            volume=signal.lots,
            price=put_premium,
            direction='BUY',
            action='OPEN',
            open_reason=self.OPEN_REASON,
            signal_id=getattr(signal, 'signal_id', ''),
        )

        if not put_order_id:
            logging.warning("[BoxSpring] STRADDLE: Put order failed for %s, closing Call leg to avoid single-leg risk",
                            put_instrument)
            try:
                osvc.send_order(
                    instrument_id=call_instrument,
                    volume=signal.lots,
                    price=call_premium,
                    direction='SELL',
                    action='CLOSE',
                    open_reason=self.OPEN_REASON,
                    signal_id=getattr(signal, 'signal_id', ''),
                )
            except (ImportError, AttributeError, RuntimeError) as e:
                logging.error("[BoxSpring] STRADDLE: failed to close Call leg after Put failure: %s", e)
            return None

        total_entry_premium = call_premium + put_premium

        # R27-P2-FP-16修复: int()截断→safe_float_to_int()
        pos_id = f"SIG_POS_STRADDLE_{signal.instrument_id}_{safe_float_to_int(time.time()*1000)}"
        position = SpringPosition(
            position_id=pos_id,
            signal_id=signal.signal_id,
            instrument_id=signal.instrument_id,
            option_instrument_id=call_instrument,
            direction='BUY_STRADDLE',
            entry_premium=total_entry_premium,
            current_premium=call_premium,
            entry_time=self._get_now(),
            stop_profit_ratio=self._stop_profit_ratio,
            max_loss_pct=self._max_loss_pct,
            box_id=signal.box_id,
            paired_instrument_id=put_instrument,
            paired_current_premium=put_premium,
        )
        with self._lock:
            self._positions[pos_id] = position
            estimated_plr = self.estimate_plr_before_entry(signal.instrument_id) if hasattr(self, 'estimate_plr_before_entry') else 0.0
            if self._dynamic_tp_sl_enabled and estimated_plr > 0:
                position.adjust_tp_sl_by_plr(estimated_plr)
            signal.is_consumed = True
            self._stats['positions_opened'] += 1

        logging.info(
            "[BoxSpring] STRADDLE ENTRY: call=%s(oid=%s) put=%s(oid=%s) "
            "call_prem=%.4f put_prem=%.4f total=%.4f stop_profit=%.1fx max_loss=%.0f%%",
            call_instrument, call_order_id, put_instrument, put_order_id,
            call_premium, put_premium, total_entry_premium,
            self._stop_profit_ratio, self._max_loss_pct * 100
        )

        return call_order_id

    def _find_straddle_pair(self, signal: SpringSignal) -> Tuple[str, float, str]:
        try:
            from ali2026v3_trading.t_type_service import get_t_type_service
            t_type = get_t_type_service()
            try:
                if not t_type or not t_type._width_cache:
                    return '', 0.0, ''
                cache = t_type._width_cache
            except AttributeError as e:
                logging.warning(
                    "[BoxSpring] _find_straddle_pair: failed to access t_type._width_cache "
                    "(internal API may have changed): %s", e
                )
                return '', 0.0, ''

            strike = signal.strike_price

            signal_opt_type = ''
            try:
                with cache._lock:
                    for iid, info in cache._option_info.items():
                        if info.get('instrument_id') == signal.option_instrument_id:
                            signal_opt_type = info.get('option_type', '')
                            break
            except AttributeError as e:
                logging.warning(
                    "[BoxSpring] _find_straddle_pair: failed to access cache._lock or cache._option_info "
                    "(internal API may have changed): %s", e
                )
                return '', 0.0, ''

            if not signal_opt_type:
                return '', 0.0, ''

            target_type = 'PUT' if signal_opt_type == 'CALL' else 'CALL'

            try:
                with cache._lock:
                    for iid, info in cache._option_info.items():
                        if info.get('underlying_future_id') != signal.instrument_id:
                            continue
                        if info.get('strike_price') != strike:
                            continue
                        if info.get('option_type') != target_type:
                            continue
                        inst_id = info.get('instrument_id', '')
                        if not inst_id or inst_id == signal.option_instrument_id:
                            continue

                        premium = 0.0
                        try:
                            from ali2026v3_trading.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                premium = ds.realtime_cache.get_latest_price(inst_id) or 0.0
                        except Exception as _ds_err:
                            logging.debug("[R22-EP-04] data_service获取premium失败: %s", _ds_err)
                        if premium <= 0:
                            premium = signal.premium_price

                        return inst_id, premium, signal_opt_type
            except AttributeError as e:
                logging.warning(
                    "[BoxSpring] _find_straddle_pair: failed to access cache._lock or cache._option_info "
                    "(internal API may have changed): %s", e
                )
                return '', 0.0, ''
        except Exception as e:
            logging.warning("[BoxSpring] _find_straddle_pair error: %s", e)

        return '', 0.0, ''

    # ========================================================================
    # 平仓纪律：弹簧松开即走 / 接受归零
    # ========================================================================

    def on_premium_update(self, option_instrument_id: str, current_premium: float) -> Optional[Dict[str, Any]]:
        if current_premium <= 0:
            return None

        with self._lock:
            open_positions = [
                p for p in self._positions.values()
                if (p.option_instrument_id == option_instrument_id or
                    p.paired_instrument_id == option_instrument_id) and p.is_open
            ]

        if not open_positions:
            return None

        pos = open_positions[0]

        if pos.direction == 'BUY_STRADDLE' and pos.paired_instrument_id:
            if option_instrument_id == pos.paired_instrument_id:
                pos.paired_current_premium = current_premium
            else:
                pos.current_premium = current_premium
            total_premium = pos.current_premium + pos.paired_current_premium
            if total_premium > pos.peak_premium:
                pos.peak_premium = total_premium
                pos.peak_time = self._get_now()

            close_action = self._evaluate_close_straddle(pos)
            if close_action:
                pos.current_premium = total_premium
                return self._execute_close(pos, close_action)
            return None

        pos.current_premium = current_premium

        if current_premium > pos.peak_premium:
            pos.peak_premium = current_premium
            pos.peak_time = self._get_now()

        close_action = self._evaluate_close(pos)
        if close_action:
            return self._execute_close(pos, close_action)

        return None

    def _evaluate_close_straddle(self, pos: SpringPosition) -> Optional[str]:
        total_premium = pos.current_premium + pos.paired_current_premium
        if pos.entry_premium <= 0:
            return 'STOP_LOSS'
        pnl_ratio = total_premium / pos.entry_premium
        pnl_ratio = max(min(pnl_ratio, 100.0), -100.0)  # NP-P2-11: pnl_ratio溢出clip
        if pnl_ratio >= pos.stop_profit_ratio:
            return 'TAKE_PROFIT'
        loss_pct = 1.0 - (total_premium / pos.entry_premium)
        if loss_pct >= pos.max_loss_pct:
            return 'STOP_LOSS'
        hold_minutes = (self._get_now() - pos.entry_time).total_seconds() / 60.0
        max_hold = self.params.get('max_spring_hold_minutes', 120)
        if hold_minutes > max_hold:
            return 'TIME_EXPIRE'
        box = self._boxes.get(pos.instrument_id)
        if box and not box.is_active:
            return 'BOX_BROKEN'
        return None

    def _evaluate_close(self, pos: SpringPosition) -> Optional[str]:
        if pos.should_take_profit:
            return 'TAKE_PROFIT'

        if pos.should_accept_loss:
            return 'STOP_LOSS'

        hold_minutes = (self._get_now() - pos.entry_time).total_seconds() / 60.0
        max_hold = self.params.get('max_spring_hold_minutes', 120)
        if hold_minutes > max_hold:
            return 'TIME_EXPIRE'

        box = self._boxes.get(pos.instrument_id)
        if box and not box.is_active:
            return 'BOX_BROKEN'

        return None

    def _execute_close(self, pos: SpringPosition, reason: str) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                _CLOSE_DIRECTION_MAP = {
                    'BUY_CALL': 'SELL', 'BUY_PUT': 'SELL', 'BUY_STRADDLE': 'SELL',
                    'SELL_CALL': 'BUY', 'SELL_PUT': 'BUY',
                }
                close_direction = _CLOSE_DIRECTION_MAP.get(pos.direction, 'SELL')
                close_lots = pos.lots if hasattr(pos, 'lots') and pos.lots > 0 else 1
                osvc.send_order(
                    instrument_id=pos.option_instrument_id,
                    volume=close_lots,
                    price=pos.current_premium,
                    direction=close_direction,
                    action='CLOSE',
                    signal_id=getattr(pos, 'signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
                )
                if pos.direction == 'BUY_STRADDLE' and pos.paired_instrument_id:
                    paired_close_dir = _CLOSE_DIRECTION_MAP.get(pos.direction, 'SELL')
                    osvc.send_order(
                        instrument_id=pos.paired_instrument_id,
                        volume=close_lots,
                        price=pos.paired_current_premium,
                        direction=paired_close_dir,
                        action='CLOSE',
                        signal_id=getattr(pos, 'signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
                    )
                osvc.persist_close_event(
                    order_id=pos.position_id,
                    close_reason=f'SIG_{reason}',
                    pnl=(pos.current_premium + pos.paired_current_premium) - pos.entry_premium if pos.direction == 'BUY_STRADDLE' else pos.current_premium - pos.entry_premium,
                )
        except Exception as e:
            logging.error("[BoxSpring] Close error: %s", e)

        pos.is_open = False
        if reason == 'TIME_EXPIRE':
            sig = self._signals.get(pos.signal_id)
            if sig:
                sig.spring_state = SpringState.EXPIRED
        pnl = (pos.current_premium + pos.paired_current_premium) - pos.entry_premium if pos.direction == 'BUY_STRADDLE' else pos.current_premium - pos.entry_premium

        with self._lock:
            self._stats['total_pnl'] += pnl
            if reason == 'TAKE_PROFIT':
                self._stats['positions_closed_tp'] += 1
                self._stats['win_count'] += 1
            elif reason == 'STOP_LOSS':
                self._stats['positions_closed_sl'] += 1
                self._stats['loss_count'] += 1
            else:
                self._stats['positions_closed_expired'] += 1
                self._stats['loss_count'] += 1

        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            rs.record_trade_result('spring', pnl, self._capital_scale)
        except Exception as e:
            logging.debug("[BoxSpring] record_trade_result failed: %s", e)

        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            _close_lots = pos.lots if hasattr(pos, 'lots') and pos.lots > 0 else 1
            _est_commission = _close_lots * 3.0
            _est_slippage = abs(pnl) * 3.0 / 10000 if pnl != 0 else 0.0
            eco.record_strategy_pnl('spring', pnl, commission=_est_commission, slippage=_est_slippage)
            eco.update_plr_stats('spring')
        except Exception as e:
            logging.debug("[BoxSpring] record_strategy_pnl/update_plr_stats failed: %s", e)

        logging.info(
            "[BoxSpring] CLOSE: %s reason=%s entry=%.4f exit=%.4f pnl=%.4f ratio=%.2f%%",
            pos.option_instrument_id, reason, pos.entry_premium, pos.current_premium,
            pnl, pos.pnl_ratio * 100  # NP-P2-02: pnl_ratio显示加*100和%%
        )

        return {
            'position_id': pos.position_id,
            'reason': reason,
            'entry_premium': pos.entry_premium,
            'exit_premium': pos.current_premium,
            'pnl': pnl,
            'pnl_ratio': pos.pnl_ratio,
            'peak_premium': pos.peak_premium,
        }

    # ========================================================================
    # R13-P0-BIZ-06修复: 对冲比率计算机制
    # ========================================================================

    def _compute_hedge_ratio(self, instrument_id: str = '') -> float:
        """计算delta中性对冲比率

        使用greeks_calculator获取当前持仓的net_delta，
        计算需要多少期货手数来对冲至delta中性。

        Returns:
            float: 对冲比率（需要买入/卖出的期货手数，正=买入，负=卖出）
        """
        try:
            from ali2026v3_trading.position_service import (
                aggregate_greeks_exposure,
                get_position_service,
            )
            pos_svc = get_position_service()
            if pos_svc is None:
                return 0.0
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            net_delta = exposure.net_delta
            if abs(net_delta) < 1e-6:
                return 0.0
            # 期货delta=1/手，对冲手数 = -net_delta
            hedge_lots = -net_delta
            logging.info(
                "[BoxSpring] R13-P0-BIZ-06: 对冲比率计算 net_delta=%.4f hedge_lots=%.2f",
                net_delta, hedge_lots,
            )
            return hedge_lots
        except Exception as e:
            logging.debug("[BoxSpring] _compute_hedge_ratio failed: %s", e)
            return 0.0

    # ========================================================================
    # 铁律检查：绝不平移为趋势策略
    # ========================================================================

    def is_spring_position(self, instrument_id: str) -> bool:
        with self._lock:
            for pos in self._positions.values():
                if (pos.option_instrument_id == instrument_id or
                    pos.paired_instrument_id == instrument_id) and pos.is_open:
                    return True
        return False

    def prevent_trend_conversion(self, instrument_id: str, proposed_action: str,
                                  proposed_reason: str) -> bool:
        if self.is_spring_position(instrument_id):
            if proposed_action == 'OPEN' and proposed_reason != self.OPEN_REASON:
                logging.warning(
                    "[BoxSpring] IRON_RULE: Blocking trend conversion for %s "
                    "(spring position exists, proposed_reason=%s)",
                    instrument_id, proposed_reason
                )
                return False
        return True

    # ========================================================================
    # 期望值计算与统计
    # ========================================================================

    def get_expected_value(self) -> Dict[str, Any]:
        with self._lock:
            total = self._stats['win_count'] + self._stats['loss_count']
            if total == 0:
                return {
                    'total_trades': 0,
                    'win_rate': 0.0,
                    'avg_win_ratio': 0.0,
                    'avg_loss_ratio': 0.0,
                    'expected_value': 0.0,
                    'is_positive_ev': False,
                }

            win_rate = self._stats['win_count'] / total

            win_pnls = []
            loss_pnls = []
            for pos in self._positions.values():
                if not pos.is_open:
                    if pos.pnl_ratio > 1.0:
                        win_pnls.append(pos.pnl_ratio - 1.0)
                    else:
                        loss_pnls.append(1.0 - pos.pnl_ratio)

            avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
            avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 1.0

            ev = win_rate * avg_win - (1 - win_rate) * avg_loss

            # R27-P2-FP-09修复: round()→deterministic_round()
            return {
                'total_trades': total,
                'win_rate': deterministic_round(win_rate, 4),
                'avg_win_ratio': deterministic_round(avg_win, 4),
                'avg_loss_ratio': deterministic_round(avg_loss, 4),
                'expected_value': deterministic_round(ev, 4),
                'is_positive_ev': ev > 0,
                'total_pnl': deterministic_round(self._stats['total_pnl'], 4),
            }

    def _check_cross_strategy_risk(self, signal: SpringSignal) -> bool:
        try:
            from ali2026v3_trading.position_service import (
                aggregate_greeks_exposure,
                get_cross_strategy_risk_guard,
                get_position_service,
            )
            guard = get_cross_strategy_risk_guard()
            pos_svc = get_position_service()
            if pos_svc is None:
                logging.warning("[BoxSpring._check_cross_strategy_risk] PositionService unavailable, fail-safe阻断")
                return False
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            level, reason, detail = guard.check(exposure)
            if level in (guard.BLOCK, guard.CIRCUIT_BREAK):
                return False
            return True
        except Exception as e:
            import logging
            logging.warning("[BoxSpring._check_cross_strategy_risk] Error: %s, fail-safe阻断", e)
            return False

    def _record_spring_trade(self, signal: SpringSignal):
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            _spring_pnl = getattr(signal, 'pnl', 0.0) if hasattr(signal, 'pnl') else 0.0
            eco.record_spring_trade(signal.direction, pnl=_spring_pnl)
        except Exception as e:
            logging.warning("[BoxSpring] _record_spring_trade failed: %s", e)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            ev = self.get_expected_value()
            active = sum(1 for p in self._positions.values() if p.is_open)
            return {
                'service_name': 'BoxSpringStrategy',
                **self._stats,
                **self._pullback_mgr._stats,
                'pending_pullbacks': len(self._pullback_mgr._pending),
                'active_positions': active,
                'active_boxes': sum(1 for b in self._boxes.values() if b.is_active),
                'expected_value': ev,
            }

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            ev = self.get_expected_value()
            active = sum(1 for p in self._positions.values() if p.is_open)
            boxes = sum(1 for b in self._boxes.values() if b.is_active)

            status = 'OK'
            if ev['total_trades'] >= 20 and not ev['is_positive_ev']:
                status = 'DEGRADED'
            if active > self._max_active_positions:
                status = 'WARNING'

            return {
                'status': status,
                'active_positions': active,
                'active_boxes': boxes,
                'expected_value': ev['expected_value'],
                'is_positive_ev': ev['is_positive_ev'],
                'total_pnl': ev['total_pnl'],
            }

    # ========================================================================
    # Tick驱动入口
    # ========================================================================

    def on_tick(self, instrument_id: str, price: float, high: float = 0.0,
                low: float = 0.0, volume: int = 0, timestamp: Optional[datetime] = None) -> None:
        if price <= 0:
            return

        _tick_ts = getattr(timestamp, 'timestamp', None) if timestamp is not None else None
        if _tick_ts is not None:
            if isinstance(_tick_ts, (int, float)):
                self._current_bar_time = float(_tick_ts)
            elif hasattr(_tick_ts, 'timestamp'):
                self._current_bar_time = _tick_ts.timestamp()
        elif timestamp is not None:
            self._current_bar_time = timestamp.timestamp() if hasattr(timestamp, 'timestamp') else None

        if high > 0 and low > 0:
            self.update_box(instrument_id, high, low, price, timestamp)

        # R14-P0-BIZ-06修复: 在tick处理中调用对冲比率计算，辅助交易决策
        try:
            hedge_ratio = self._compute_hedge_ratio(instrument_id)
            if abs(hedge_ratio) > 0.01:
                logging.info(
                    "[BoxSpring] R14-P0-BIZ-06: 对冲比率非零 hedge_ratio=%.4f instrument=%s",
                    hedge_ratio, instrument_id,
                )
                self._last_hedge_ratio = hedge_ratio
        except Exception as _hr_err:
            _hedge_fail_count = getattr(self, '_hedge_ratio_fail_count', 0) + 1
            self._hedge_ratio_fail_count = _hedge_fail_count
            logging.warning("[BoxSpring] BIZ-P1-06: 对冲比率计算失败(count=%d): %s", _hedge_fail_count, _hr_err)
            if _hedge_fail_count >= 5:
                logging.critical("[BoxSpring] BIZ-P1-06: 对冲连续%d次失败，暂停新开仓", _hedge_fail_count)
                self._hedge_ratio_fail_paused = True
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus
                    _bus = get_global_event_bus()
                    if _bus is not None:
                        _bus.publish('hedge.failure_critical', {'fail_count': _hedge_fail_count, 'action': 'pause_new_open'}, async_mode=True)
                except Exception:
                    pass

        with self._lock:
            open_positions = [
                p for p in self._positions.values()
                if p.instrument_id == instrument_id and p.is_open
            ]

        for pos in open_positions:
            try:
                from ali2026v3_trading.greeks_calculator import GreeksCalculator
                calc = self._get_greeks_calculator()
                if calc:
                    greeks = calc.get_greeks(pos.option_instrument_id)
                    if greeks:
                        iv = greeks.get('iv', 0.0)
                        if iv > 0:
                            self.update_iv(pos.option_instrument_id, iv)
                        gamma = greeks.get('gamma', 0.0)
                        theta = greeks.get('theta', 0.0)
                        vega = greeks.get('vega', 0.0)
                        if abs(gamma) > 0 or abs(theta) > 0 or abs(vega) > 0:
                            logging.debug(
                                "[BoxSpring] Greeks update: %s gamma=%.6f theta=%.6f vega=%.6f",
                                pos.option_instrument_id, gamma, theta, vega,
                            )

                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            opt_price = ds.realtime_cache.get_latest_price(pos.option_instrument_id)
                            if opt_price and opt_price > 0:
                                self.on_premium_update(pos.option_instrument_id, opt_price)
            except Exception as _price_err:
                logging.debug("[R22-EP-04b] premium更新失败: %s", _price_err)

    def _get_greeks_calculator(self):
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            if rs:
                return rs._get_greeks_calculator()
        except Exception as _greeks_err:
            logging.debug("[R22-EP-04c] greeks_calculator获取失败: %s", _greeks_err)
        return None

    # ========================================================================
    # 扫描：从全量期权中寻找弹簧机会
    # ========================================================================

    def scan_springs(self) -> List[SpringSignal]:
        results = []
        try:
            results = self._scan_from_sort_buckets()
            if not results:
                results = self._scan_from_option_info()
        except Exception as e:
            logging.warning("[BoxSpring] Scan error: %s", e)
        return results

    def _scan_from_sort_buckets(self) -> List[SpringSignal]:
        results = []
        try:
            from ali2026v3_trading.t_type_service import get_t_type_service
            t_type = get_t_type_service()
            if not t_type or not t_type._width_cache:
                return results

            cache = t_type._width_cache
            with cache._lock:
                for future_id, future_rising in cache._future_rising.items():
                    box = self.get_active_box(str(future_id))
                    if not box:
                        continue

                    future_price = 0.0
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            future_price = ds.realtime_cache.get_latest_price(str(future_id)) or 0.0
                    except Exception as _fp_err:
                        logging.debug("[R22-EP-04d] future_price获取失败: %s", _fp_err)
                        continue

                    if future_price <= 0:
                        continue

                    all_months = sorted(cache._sort_buckets.get(future_id, {}).keys())
                    if not all_months:
                        continue

                    nearest_month = all_months[0]

                    for opt_type in ('CALL', 'PUT'):
                        candidates = cache.select_from_sort_bucket(future_id, nearest_month, opt_type, top_n=5)
                        for c in candidates:
                            signal = self._evaluate_candidate(c, future_id, future_price, cache)
                            if signal:
                                results.append(signal)

                    if not results:
                        for month in all_months[1:2]:
                            for opt_type in ('CALL', 'PUT'):
                                candidates = cache.select_from_sort_bucket(future_id, month, opt_type, top_n=3)
                                for c in candidates:
                                    signal = self._evaluate_candidate(c, future_id, future_price, cache)
                                    if signal:
                                        results.append(signal)

        except Exception as e:
            logging.warning("[BoxSpring] _scan_from_sort_buckets error: %s", e)

        return results

    def _scan_from_option_info(self) -> List[SpringSignal]:
        results = []
        try:
            from ali2026v3_trading.t_type_service import get_t_type_service
            t_type = get_t_type_service()
            if not t_type or not t_type._width_cache:
                return results

            cache = t_type._width_cache
            with cache._lock:
                for future_id, future_rising in cache._future_rising.items():
                    box = self.get_active_box(str(future_id))
                    if not box:
                        continue

                    future_price = 0.0
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            future_price = ds.realtime_cache.get_latest_price(str(future_id)) or 0.0
                    except Exception as _fp2_err:
                        logging.debug("[R22-EP-04e] future_price获取失败: %s", _fp2_err)
                        continue

                    if future_price <= 0:
                        continue

                    nearest_otm_by_type: Dict[str, List[Dict]] = {'CALL': [], 'PUT': []}

                    for iid, info in cache._option_info.items():
                        underlying_fid = info.get('underlying_future_id')
                        if underlying_fid != future_id:
                            continue

                        strike = info.get('strike_price', 0)
                        opt_type = info.get('option_type', 'CALL')
                        inst_id = info.get('instrument_id', '')
                        if not inst_id or strike <= 0:
                            continue

                        if opt_type == 'CALL' and future_price >= strike:
                            continue
                        if opt_type == 'PUT' and future_price <= strike:
                            continue

                        distance = abs(future_price - strike) / future_price if future_price > 0 else 1.0
                        if distance > self._strike_distance_threshold:
                            continue

                        expiry_str = info.get('expiry_date', '')
                        days_to_expiry = 999
                        month = info.get('month', '')
                        if expiry_str:
                            try:
                                expiry = datetime.strptime(str(expiry_str), '%Y%m%d')
                                days_to_expiry = (expiry - self._get_now()).days
                            except Exception as _expiry_err:
                                logging.debug("[R22-EP-04f] expiry_date解析失败: %s", _expiry_err)

                        if days_to_expiry < self._min_days_to_expiry or days_to_expiry > self._max_days_to_expiry:
                            continue

                        nearest_otm_by_type[opt_type].append({
                            'instrument_id': inst_id,
                            'internal_id': iid,
                            'strike_price': strike,
                            'distance': distance,
                            'days_to_expiry': days_to_expiry,
                            'month': month,
                        })

                    for opt_type, candidates in nearest_otm_by_type.items():
                        candidates.sort(key=lambda x: x['distance'])
                        for c in candidates[:3]:
                            inst_id = c['instrument_id']
                            strike = c['strike_price']

                            iv = 0.0
                            premium = 0.0
                            try:
                                calc = self._get_greeks_calculator()
                                if calc:
                                    greeks = calc.get_greeks(inst_id)
                                    iv = greeks.get('iv', 0) if greeks else 0
                                    premium = greeks.get('price', 0) if greeks else 0
                            except Exception as _greeks2_err:
                                logging.debug("[R22-EP-04g] greeks获取失败: %s", _greeks2_err)

                            if premium <= 0:
                                try:
                                    from ali2026v3_trading.data_service import get_data_service
                                    ds = get_data_service()
                                    if ds and ds.realtime_cache:
                                        opt_price = ds.realtime_cache.get_latest_price(inst_id)
                                        if opt_price and opt_price > 0:
                                            premium = opt_price
                                except Exception as _opt_err:
                                    logging.debug("[R22-EP-04h] opt_price获取失败: %s", _opt_err)

                            if premium <= 0:
                                continue

                            signal = self.detect_spring(
                                instrument_id=str(future_id),
                                future_price=future_price,
                                option_instrument_id=inst_id,
                                strike_price=strike,
                                iv=iv,
                                premium_price=premium,
                                days_to_expiry=c['days_to_expiry'],
                            )
                            if signal:
                                results.append(signal)

        except Exception as e:
            logging.warning("[BoxSpring] _scan_from_option_info error: %s", e)

        return results

    def _evaluate_candidate(self, candidate: Dict, future_id: int,
                             future_price: float, cache: Any) -> Optional[SpringSignal]:
        inst_id = candidate.get('instrument_id', '')
        strike = candidate.get('strike_price', 0)
        if not inst_id or strike <= 0:
            return None

        iv = 0.0
        premium = 0.0
        try:
            calc = self._get_greeks_calculator()
            if calc:
                greeks = calc.get_greeks(inst_id)
                iv = greeks.get('iv', 0) if greeks else 0
                premium = greeks.get('price', 0) if greeks else 0
        except Exception as _greeks3_err:
            logging.debug("[R22-EP-04i] greeks获取失败: %s", _greeks3_err)

        if iv <= 0 or premium <= 0:
            try:
                from ali2026v3_trading.data_service import get_data_service
                ds = get_data_service()
                if ds and ds.realtime_cache:
                    opt_price = ds.realtime_cache.get_latest_price(inst_id)
                    if opt_price and opt_price > 0:
                        premium = opt_price
            except Exception as _opt2_err:
                logging.debug("[R22-EP-04j] opt_price获取失败: %s", _opt2_err)

        if premium <= 0:
            return None

        days_to_expiry = 3
        try:
            info = cache._option_info.get(candidate.get('internal_id', 0), {})
            expiry_str = info.get('expiry_date', '')
            if expiry_str:
                expiry = datetime.strptime(str(expiry_str), '%Y%m%d')
                days_to_expiry = (expiry - self._get_now()).days
        except Exception as _exp2_err:
            logging.debug("[R22-EP-04k] expiry解析失败: %s", _exp2_err)

        return self.detect_spring(
            instrument_id=str(future_id),
            future_price=future_price,
            option_instrument_id=inst_id,
            strike_price=strike,
            iv=iv,
            premium_price=premium,
            days_to_expiry=days_to_expiry,
        )


__all__ = [
    'BoxSpringStrategy',
    'BoxRange',
    'SpringSignal',
    'SpringPosition',
    'SpringState',
    'PendingPullback',
    'get_box_spring_strategy',
]
