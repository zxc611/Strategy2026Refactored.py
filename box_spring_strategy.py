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
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple


class SpringState(Enum):
    DORMANT = auto()
    COMPRESSED = auto()
    TRIGGERED = auto()
    ACTIVE = auto()
    EXPIRED = auto()


@dataclass
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
        return self.box_bottom <= price <= self.box_top

    def price_position(self, price: float) -> float:
        if self.box_top <= self.box_bottom:
            return 0.5
        return (price - self.box_bottom) / (self.box_top - self.box_bottom)


@dataclass
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
    created_at: datetime = field(default_factory=datetime.now)
    is_consumed: bool = False


@dataclass
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

    @property
    def pnl_ratio(self) -> float:
        if self.entry_premium <= 0:
            return 0.0
        return self.current_premium / self.entry_premium

    @property
    def should_take_profit(self) -> bool:
        return self.pnl_ratio >= self.stop_profit_ratio

    @property
    def should_accept_loss(self) -> bool:
        if self.entry_premium <= 0:
            return True
        loss_pct = 1.0 - (self.current_premium / self.entry_premium)
        return loss_pct >= self.max_loss_pct


_box_spring_instance: Optional['BoxSpringStrategy'] = None
_box_spring_lock = threading.Lock()


def get_box_spring_strategy(params: Optional[Dict] = None) -> 'BoxSpringStrategy':
    global _box_spring_instance
    if _box_spring_instance is None:
        with _box_spring_lock:
            if _box_spring_instance is None:
                _box_spring_instance = BoxSpringStrategy(params or {})
    return _box_spring_instance


class BoxSpringStrategy:
    OPEN_REASON = 'BOX_SPRING'

    def __init__(self, params: Dict[str, Any]):
        self.params = params
        self._lock = threading.RLock()

        self._boxes: Dict[str, BoxRange] = {}
        self._iv_history: Dict[str, deque] = {}
        self._iv_window = params.get('iv_lookback_bars', 120)

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
        self._max_loss_pct = params.get('max_loss_loss_pct', 0.95)
        self._max_position_pct = params.get('max_position_pct', 0.015)
        self._cooldown_sec = params.get('spring_cooldown_sec', 300)
        self._max_active_positions = params.get('max_spring_positions', 3)

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

    # ========================================================================
    # 箱体识别
    # ========================================================================

    def update_box(self, instrument_id: str, high: float, low: float,
                   close: float, timestamp: Optional[datetime] = None) -> Optional[BoxRange]:
        if high <= 0 or low <= 0 or close <= 0:
            return None

        ts = timestamp or datetime.now()
        width_pct = (high - low) / close if close > 0 else 1.0

        if width_pct > self._max_box_width_pct:
            self._invalidate_box(instrument_id, 'width_exceeded')
            return None

        box = self._boxes.get(instrument_id)
        if box and box.is_active:
            if close > box.box_top * 1.005 or close < box.box_bottom * 0.995:
                self._invalidate_box(instrument_id, 'price_broken')
                return None

            if high >= box.box_top * 0.998 or low <= box.box_bottom * 1.002:
                box.touch_count += 1
                box.last_touch_time = ts

            return box

        box_id = f"BOX_{instrument_id}_{int(ts.timestamp())}"
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

        with self._lock:
            if instrument_id not in self._iv_history:
                self._iv_history[instrument_id] = deque(maxlen=self._iv_window)
            self._iv_history[instrument_id].append(iv)

            history = self._iv_history[instrument_id]
            if len(history) < 5:
                return 50.0

            sorted_ivs = sorted(history)
            rank = 0
            for v in sorted_ivs:
                if v < iv:
                    rank += 1
                else:
                    break
            percentile = (rank / len(sorted_ivs)) * 100.0
            return percentile

    def get_iv_percentile(self, instrument_id: str) -> float:
        with self._lock:
            history = self._iv_history.get(instrument_id)
            if not history or len(history) < 5:
                return 50.0
            current_iv = history[-1]
            sorted_ivs = sorted(history)
            rank = sum(1 for v in sorted_ivs if v < current_iv)
            return (rank / len(sorted_ivs)) * 100.0

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

        price_pos = box.price_position(future_price)
        if price_pos < 0.3 or price_pos > 0.7:
            return None

        strike_distance_pct = abs(future_price - strike_price) / future_price if future_price > 0 else 1.0
        if strike_distance_pct > 0.02:
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

        signal_id = f"SPRING_{instrument_id}_{int(now*1000)}"
        signal = SpringSignal(
            signal_id=signal_id,
            instrument_id=instrument_id,
            option_instrument_id=option_instrument_id,
            spring_state=SpringState.COMPRESSED,
            iv_percentile=iv_pct,
            premium_cost_pct=premium_cost_pct,
            gamma_exposure=gamma_exposure,
            box_id=box.box_id,
            direction=direction,
            strike_price=strike_price,
            current_price=future_price,
            premium_price=premium_price,
        )

        with self._lock:
            self._signals[signal_id] = signal
            self._stats['compressed_detected'] += 1
            self._last_signal_time[instrument_id] = now

        logging.info(
            "[BoxSpring] COMPRESSED: %s IV_pct=%.1f%% premium=%.4f gamma_exp=%.4f dir=%s DTE=%d",
            option_instrument_id, iv_pct, premium_price, gamma_exposure, direction, days_to_expiry
        )

        return signal

    def _infer_direction(self, box: BoxRange, price: float, price_pos: float) -> str:
        if price_pos < 0.45:
            return 'BUY_CALL'
        elif price_pos > 0.55:
            return 'BUY_PUT'
        else:
            return 'BUY_STRADDLE'

    def _estimate_gamma_exposure(self, S: float, K: float, sigma: float,
                                  T_days: int, premium: float) -> float:
        if S <= 0 or K <= 0 or sigma <= 0 or T_days <= 0 or premium <= 0:
            return 0.0
        try:
            import math
            T = T_days / 365.0
            d1 = (math.log(S / K) + (0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
            gamma = math.exp(-0.5 * d1 * d1) / (S * sigma * math.sqrt(T) * math.sqrt(2 * math.pi))
            return gamma * S * S / premium if premium > 0 else 0.0
        except Exception:
            return 0.0

    # ========================================================================
    # 入场信号：箱内脉冲触发
    # ========================================================================

    def check_trigger(self, instrument_id: str, order_flow_imbalance: float = 0.0,
                      option_chain_activity: float = 0.0) -> Optional[SpringSignal]:
        with self._lock:
            compressed = [
                s for s in self._signals.values()
                if s.instrument_id == instrument_id
                and s.spring_state == SpringState.COMPRESSED
                and not s.is_consumed
            ]

        if not compressed:
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

        signal = compressed[0]
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
    # 下单执行
    # ========================================================================

    def execute_spring_entry(self, signal: SpringSignal) -> Optional[str]:
        active_count = sum(1 for p in self._positions.values() if p.is_open)
        if active_count >= self._max_active_positions:
            logging.debug("[BoxSpring] Max positions reached: %d", active_count)
            return None

        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if not osvc:
                return None

            action_map = {
                'BUY_CALL': ('BUY', 'OPEN'),
                'BUY_PUT': ('SELL', 'OPEN'),
                'BUY_STRADDLE': ('BUY', 'OPEN'),
            }
            direction, action = action_map.get(signal.direction, ('BUY', 'OPEN'))

            order_id = osvc.send_order(
                instrument_id=signal.option_instrument_id,
                volume=1,
                price=signal.premium_price,
                direction=direction,
                action=action,
                open_reason=self.OPEN_REASON,
            )

            if order_id:
                pos_id = f"SPRING_POS_{signal.option_instrument_id}_{int(time.time()*1000)}"
                position = SpringPosition(
                    position_id=pos_id,
                    signal_id=signal.signal_id,
                    instrument_id=signal.instrument_id,
                    option_instrument_id=signal.option_instrument_id,
                    direction=signal.direction,
                    entry_premium=signal.premium_price,
                    current_premium=signal.premium_price,
                    entry_time=datetime.now(),
                    stop_profit_ratio=self._stop_profit_ratio,
                    max_loss_pct=self._max_loss_pct,
                    box_id=signal.box_id,
                )
                with self._lock:
                    self._positions[pos_id] = position
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

    # ========================================================================
    # 平仓纪律：弹簧松开即走 / 接受归零
    # ========================================================================

    def on_premium_update(self, option_instrument_id: str, current_premium: float) -> Optional[Dict[str, Any]]:
        if current_premium <= 0:
            return None

        with self._lock:
            open_positions = [
                p for p in self._positions.values()
                if p.option_instrument_id == option_instrument_id and p.is_open
            ]

        if not open_positions:
            return None

        pos = open_positions[0]
        pos.current_premium = current_premium

        if current_premium > pos.peak_premium:
            pos.peak_premium = current_premium
            pos.peak_time = datetime.now()

        close_action = self._evaluate_close(pos)
        if close_action:
            return self._execute_close(pos, close_action)

        return None

    def _evaluate_close(self, pos: SpringPosition) -> Optional[str]:
        if pos.should_take_profit:
            return 'TAKE_PROFIT'

        if pos.should_accept_loss:
            return 'STOP_LOSS'

        hold_minutes = (datetime.now() - pos.entry_time).total_seconds() / 60.0
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
                close_direction = 'SELL' if pos.direction in ('BUY_CALL', 'BUY_STRADDLE') else 'BUY'
                osvc.send_order(
                    instrument_id=pos.option_instrument_id,
                    volume=1,
                    price=pos.current_premium,
                    direction=close_direction,
                    action='CLOSE',
                )
                osvc.persist_close_event(
                    order_id=pos.position_id,
                    close_reason=f'SPRING_{reason}',
                    pnl=pos.current_premium - pos.entry_premium,
                )
        except Exception as e:
            logging.error("[BoxSpring] Close error: %s", e)

        pos.is_open = False
        pnl = pos.current_premium - pos.entry_premium

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

        logging.info(
            "[BoxSpring] CLOSE: %s reason=%s entry=%.4f exit=%.4f pnl=%.4f ratio=%.2f",
            pos.option_instrument_id, reason, pos.entry_premium, pos.current_premium,
            pnl, pos.pnl_ratio
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
    # 铁律检查：绝不平移为趋势策略
    # ========================================================================

    def is_spring_position(self, instrument_id: str) -> bool:
        with self._lock:
            for pos in self._positions.values():
                if pos.option_instrument_id == instrument_id and pos.is_open:
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

            return {
                'total_trades': total,
                'win_rate': round(win_rate, 4),
                'avg_win_ratio': round(avg_win, 4),
                'avg_loss_ratio': round(avg_loss, 4),
                'expected_value': round(ev, 4),
                'is_positive_ev': ev > 0,
                'total_pnl': round(self._stats['total_pnl'], 4),
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            ev = self.get_expected_value()
            active = sum(1 for p in self._positions.values() if p.is_open)
            return {
                'service_name': 'BoxSpringStrategy',
                **self._stats,
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

        if high > 0 and low > 0:
            self.update_box(instrument_id, high, low, price, timestamp)

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

                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            opt_price = ds.realtime_cache.get_latest_price(pos.option_instrument_id)
                            if opt_price and opt_price > 0:
                                self.on_premium_update(pos.option_instrument_id, opt_price)
            except Exception:
                pass

    def _get_greeks_calculator(self):
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            if rs:
                return rs._get_greeks_calculator()
        except Exception:
            pass
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
                    except Exception:
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
                    except Exception:
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
                        if distance > 0.03:
                            continue

                        expiry_str = info.get('expiry_date', '')
                        days_to_expiry = 999
                        month = info.get('month', '')
                        if expiry_str:
                            try:
                                expiry = datetime.strptime(str(expiry_str), '%Y%m%d')
                                days_to_expiry = (expiry - datetime.now()).days
                            except Exception:
                                pass

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
                            except Exception:
                                pass

                            if premium <= 0:
                                try:
                                    from ali2026v3_trading.data_service import get_data_service
                                    ds = get_data_service()
                                    if ds and ds.realtime_cache:
                                        opt_price = ds.realtime_cache.get_latest_price(inst_id)
                                        if opt_price and opt_price > 0:
                                            premium = opt_price
                                except Exception:
                                    pass

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
        except Exception:
            pass

        if iv <= 0 or premium <= 0:
            try:
                from ali2026v3_trading.data_service import get_data_service
                ds = get_data_service()
                if ds and ds.realtime_cache:
                    opt_price = ds.realtime_cache.get_latest_price(inst_id)
                    if opt_price and opt_price > 0:
                        premium = opt_price
            except Exception:
                pass

        if premium <= 0:
            return None

        days_to_expiry = 3
        try:
            info = cache._option_info.get(candidate.get('internal_id', 0), {})
            expiry_str = info.get('expiry_date', '')
            if expiry_str:
                expiry = datetime.strptime(str(expiry_str), '%Y%m%d')
                days_to_expiry = (expiry - datetime.now()).days
        except Exception:
            pass

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
    'get_box_spring_strategy',
]
