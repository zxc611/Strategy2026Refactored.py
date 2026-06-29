# _INTERNAL: internal module, not part of public API
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple


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