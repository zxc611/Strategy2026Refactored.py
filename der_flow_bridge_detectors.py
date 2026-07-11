



__all__ = [

    'OrderFlowBridge',

    'get_order_flow_bridge',

    'MicrostructureArbitrageDetector',

    'ArbitrageOpportunity',

    'MarketMakerDefenseEngine',

    'OrderDefenseType',

    'DefensiveOrder',

]





@dataclass(slots=True)

# [M1-43-02] 订单流桥接-检测器

import time
import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple


class ArbitrageOpportunity:

    opportunity_id: str

    instrument_id: str

    product: str

    deviation_type: str

    deviation_bps: float

    expected_reversion_bps: float

    confidence: float

    direction: str

    entry_price: float

    fair_value: float

    detected_at: float = field(default_factory=time.time)

    is_expired: bool = False





class MicrostructureArbitrageDetector:

    def __init__(self, deviation_threshold_bps: float = 50.0, reversion_target_bps: float = 30.0,

                 lookback_seconds: int = 60, max_opportunities: int = 100,

                 opportunity_ttl_seconds: float = 30.0):

        self._deviation_threshold = deviation_threshold_bps

        self._reversion_target = reversion_target_bps

        self._lookback = lookback_seconds

        self._max_opportunities = max_opportunities

        self._ttl = opportunity_ttl_seconds

        self._price_history: Dict[str, deque] = {}

        self._resonance_state: Dict[str, float] = {}

        self._opportunities: Dict[str, ArbitrageOpportunity] = {}

        self._lock = threading.RLock()

        self._stats = {

            'total_checks': 0, 'deviations_detected': 0,

            'opportunities_created': 0, 'opportunities_expired': 0,

        }



    def update_price(self, instrument_id: str, price: float, product: str = '') -> None:

        with self._lock:

            key = instrument_id

            if key not in self._price_history:

                self._price_history[key] = deque(maxlen=1000)

            self._price_history[key].append((time.time(), price, product))



    def update_resonance_state(self, product: str, resonance_strength: float) -> None:

        with self._lock:

            self._resonance_state[product] = resonance_strength



    def detect_arbitrage(self, instrument_id: str, current_price: float, product: str = '',

                         fair_value: Optional[float] = None) -> Optional[ArbitrageOpportunity]:

        with self._lock:

            self._stats['total_checks'] += 1

            self._expire_opportunities()

            if fair_value is None:

                fair_value = self._estimate_fair_value(instrument_id)

                if fair_value is None or fair_value <= 0:

                    return None

            if current_price <= 0 or fair_value <= 0:

                return None

            deviation_bps = abs(current_price - fair_value) / fair_value * 10000

            if deviation_bps < self._deviation_threshold:

                return None

            resonance = self._resonance_state.get(product, 0.0)

            if abs(resonance) > 0.5:

                return None

            self._stats['deviations_detected'] += 1

            direction = 'BUY' if current_price < fair_value else 'SELL'

            confidence = min(1.0, deviation_bps / 200.0)

            reversion_target = min(deviation_bps * 0.6, self._reversion_target)

            opp = ArbitrageOpportunity(

                opportunity_id=f"ARB_{instrument_id}_{int(time.time()*1000)}",

                instrument_id=instrument_id, product=product, deviation_type='price_dislocation',

                deviation_bps=deviation_bps, expected_reversion_bps=reversion_target,

                confidence=confidence, direction=direction,

                entry_price=current_price, fair_value=fair_value,

            )

            if len(self._opportunities) >= self._max_opportunities:

                oldest_key = min(self._opportunities, key=lambda k: self._opportunities[k].detected_at)

                del self._opportunities[oldest_key]

            self._opportunities[opp.opportunity_id] = opp

            self._stats['opportunities_created'] += 1

            logging.info("[ArbitrageDetector] %s deviation=%.1fbps direction=%s confidence=%.2f",

                         instrument_id, deviation_bps, direction, confidence)

            return opp



    def _estimate_fair_value(self, instrument_id: str) -> Optional[float]:

        history = self._price_history.get(instrument_id)

        if not history or len(history) < 5:

            return None

        now = time.time()

        cutoff = now - self._lookback

        recent = [(ts, p) for ts, p, _ in history if ts >= cutoff]

        if len(recent) < 3:

            return None

        return sum(p for _, p in recent) / len(recent)



    def _expire_opportunities(self) -> None:

        now = time.time()

        expired_keys = [k for k, opp in self._opportunities.items() if now - opp.detected_at > self._ttl]

        for key in expired_keys:

            del self._opportunities[key]

            self._stats['opportunities_expired'] += 1



    def get_active_opportunities(self, product: str = '') -> List[ArbitrageOpportunity]:

        with self._lock:

            self._expire_opportunities()

            result = [opp for opp in self._opportunities.values() if not opp.is_expired]

            if product:

                result = [opp for opp in result if opp.product == product]

            return sorted(result, key=lambda x: x.deviation_bps, reverse=True)



    def get_stats(self) -> Dict[str, Any]:

        with self._lock:

            return {

                'service_name': 'MicrostructureArbitrageDetector', **self._stats,

                'active_opportunities': len(self._opportunities),

                'tracked_instruments': len(self._price_history),

            }





class CrossContractArbitrageDetector:

    """跨合约套利检测：同标的不同行权价/到期日期权间的相对偏差



    原理：同标的期权应满足无套利约束（如call spread单调性）_

    违反时即存在套利机会。订单簿失衡可辅助确认方向。'
    """



    def __init__(self, max_deviation_bps: float = 30.0):

        self._max_deviation_bps = max_deviation_bps

        self._stats = {'total_checks': 0, 'violations': 0}



    def check_call_spread_monotonicity(

        self, calls: Dict[str, float]

    ) -> List[Dict[str, Any]]:

        """检查Call Spread单调性：K1<K2则C(K1)>=C(K2)



        calls: {strike: price} 按行权价排列的Call价格

        违反时C(K1)<C(K2)，可买入C(K1)卖出C(K2)

        """

        violations = []

        strikes = sorted(calls.keys(), key=lambda x: float(x))

        self._stats['total_checks'] += 1

        for i in range(len(strikes) - 1):

            k1, k2 = strikes[i], strikes[i + 1]

            c1, c2 = calls[k1], calls[k2]

            if c1 < c2:

                dev_bps = (c2 - c1) / max(c1, 0.001) * 10000

                if dev_bps > self._max_deviation_bps:

                    violations.append({

                        'type': 'call_spread_violation',

                        'buy_strike': k1, 'buy_price': c1,

                        'sell_strike': k2, 'sell_price': c2,

                        'deviation_bps': round(dev_bps, 2),

                    })

                    self._stats['violations'] += 1

        return violations



    def get_stats(self) -> Dict[str, Any]:

        return {'service_name': 'CrossContractArbitrageDetector', **self._stats}





class OrderDefenseType(Enum):

    IOC = auto()

    HIDDEN_WITH_OFFSET = auto()

    STANDARD = auto()

    POST_ONLY = auto()





# R35-P2-6标注: SweepDetector已实例化(OrderFlowBridge.__init__),

# on_fill()通过OrderFlowBridge.on_fill_event()间接调用例

# 待集成到风控检查链表目前无主动定时调度度

class SweepDetector:

    """扫单行为识别器：检测做市商短时间内多档扫单



    原理：如果在detect_window_ms内，同一方向连续成交

    超过sweep_depth_levels个档位，判定为扫单行为，

    触发防御（暂停该方向挂单、切换IOC模式）_

    """



    def __init__(self, detect_window_ms: float = 500.0,

                 sweep_depth_levels: int = 3,

                 cooldown_ms: float = 2000.0):

        self._window_ms = detect_window_ms

        self._sweep_depth = sweep_depth_levels

        self._cooldown_ms = cooldown_ms

        self._recent_fills: Deque[Dict] = deque(maxlen=50)

        self._sweep_until: float = 0.0

        self._stats = {'total_checks': 0, 'sweeps_detected': 0}



    def on_fill(self, direction: str, price_level: int,

                timestamp_ms: float = 0.0) -> Dict[str, Any]:

        if timestamp_ms <= 0:

            timestamp_ms = time.time() * 1000

        self._recent_fills.append({

            'direction': direction, 'level': price_level,

            'ts': timestamp_ms,

        })

        self._stats['total_checks'] += 1

        return self._detect(timestamp_ms)



    def _detect(self, now_ms: float) -> Dict[str, Any]:

        if now_ms < self._sweep_until:

            return {'sweep_detected': True, 'in_cooldown': True}

        recent = [f for f in self._recent_fills

                  if now_ms - f['ts'] <= self._window_ms]

        for direction in ('BUY', 'SELL'):

            dir_fills = [f for f in recent if f['direction'] == direction]

            if len(dir_fills) >= 2:

                levels = set(f['level'] for f in dir_fills)

                if len(levels) >= self._sweep_depth:

                    self._sweep_until = now_ms + self._cooldown_ms

                    self._stats['sweeps_detected'] += 1

                    return {

                        'sweep_detected': True, 'in_cooldown': False,

                        'direction': direction, 'depth': len(levels),

                    }

        return {'sweep_detected': False, 'in_cooldown': False}



    def get_stats(self) -> Dict[str, Any]:

        return {'service_name': 'SweepDetector', **self._stats}





@dataclass(slots=True)

class DefensiveOrder:

    order_id: str

    instrument_id: str

    direction: str

    volume: int

    price: float

    defense_type: OrderDefenseType

    random_offset_ticks: int

    original_price: float

    is_ioc: bool

    created_at: float = field(default_factory=time.time)





class MarketMakerDefenseEngine:

    def __init__(self, ioc_signal_threshold: float = 0.8, offset_min_ticks: int = 0,

                 offset_max_ticks: int = 3, split_large_orders: bool = True,

                 max_single_order_volume: int = 5, random_seed: Optional[int] = None):

        self._ioc_threshold = ioc_signal_threshold

        self._offset_min = offset_min_ticks

        self._offset_max = offset_max_ticks

        self._split_large = split_large_orders

        self._max_single_vol = max_single_order_volume

        self._rng = random.Random(random_seed)

        self._defensive_orders: Dict[str, DefensiveOrder] = {}

        self._lock = threading.RLock()

        self._stats = {

            'total_orders': 0, 'ioc_orders': 0, 'hidden_orders': 0,

            'standard_orders': 0, 'split_orders': 0, 'avg_offset_ticks': 0.0,

        }



    def create_defensive_order(self, instrument_id: str, direction: str, volume: int,

                                price: float, signal_strength: float = 0.0,

                                tick_size: float = 0.2, is_stop_order: bool = False) -> List[DefensiveOrder]:

        self._stats['total_orders'] += 1

        defense_type = self._select_defense_type(signal_strength, is_stop_order)

        if defense_type == OrderDefenseType.IOC:

            return self._create_ioc_orders(instrument_id, direction, volume, price, tick_size)

        elif defense_type == OrderDefenseType.HIDDEN_WITH_OFFSET:

            return self._create_hidden_orders(instrument_id, direction, volume, price, tick_size)

        return self._create_standard_orders(instrument_id, direction, volume, price)



    def _select_defense_type(self, signal_strength: float, is_stop_order: bool) -> OrderDefenseType:

        if signal_strength >= self._ioc_threshold:

            self._stats['ioc_orders'] += 1

            return OrderDefenseType.IOC

        if is_stop_order:

            self._stats['hidden_orders'] += 1

            return OrderDefenseType.HIDDEN_WITH_OFFSET

        self._stats['standard_orders'] += 1

        return OrderDefenseType.STANDARD



    def _create_ioc_orders(self, instrument_id: str, direction: str, volume: int,

                            price: float, tick_size: float) -> List[DefensiveOrder]:

        sub_volumes = self._split_volume(volume) if self._split_large and volume > self._max_single_vol else [volume]

        if self._split_large and volume > self._max_single_vol:

            self._stats['split_orders'] += 1

        orders = []

        for vol in sub_volumes:

            offset = self._rng.randint(self._offset_min, min(1, self._offset_max))

            adjusted_price = self._apply_offset(price, direction, offset, tick_size)

            order = DefensiveOrder(

                order_id=f"DEF_IOC_{instrument_id}_{int(time.time()*1000)}_{vol}",

                instrument_id=instrument_id, direction=direction, volume=vol,

                price=adjusted_price, defense_type=OrderDefenseType.IOC,

                random_offset_ticks=offset, original_price=price, is_ioc=True,

            )

            orders.append(order)

            self._register_order(order)

        return orders



    def _create_hidden_orders(self, instrument_id: str, direction: str, volume: int,

                               price: float, tick_size: float) -> List[DefensiveOrder]:

        sub_volumes = self._split_volume(volume) if self._split_large and volume > self._max_single_vol else [volume]

        if self._split_large and volume > self._max_single_vol:

            self._stats['split_orders'] += 1

        orders = []

        for vol in sub_volumes:

            offset = self._rng.randint(self._offset_min, self._offset_max)

            adjusted_price = self._apply_offset(price, direction, offset, tick_size)

            order = DefensiveOrder(

                order_id=f"DEF_HID_{instrument_id}_{int(time.time()*1000)}_{vol}",

                instrument_id=instrument_id, direction=direction, volume=vol,

                price=adjusted_price, defense_type=OrderDefenseType.HIDDEN_WITH_OFFSET,

                random_offset_ticks=offset, original_price=price, is_ioc=False,

            )

            orders.append(order)

            self._register_order(order)

        return orders



    def _create_standard_orders(self, instrument_id: str, direction: str, volume: int,

                                 price: float) -> List[DefensiveOrder]:

        order = DefensiveOrder(

            order_id=f"DEF_STD_{instrument_id}_{int(time.time()*1000)}",

            instrument_id=instrument_id, direction=direction, volume=volume,

            price=price, defense_type=OrderDefenseType.STANDARD,

            random_offset_ticks=0, original_price=price, is_ioc=False,

        )

        self._register_order(order)

        return [order]



    def _apply_offset(self, price: float, direction: str, offset_ticks: int, tick_size: float) -> float:

        if offset_ticks == 0:

            return price

        offset_amount = offset_ticks * tick_size

        return price - offset_amount if direction == 'BUY' else price + offset_amount



    def _split_volume(self, volume: int) -> List[int]:

        if volume <= self._max_single_vol:

            return [volume]

        parts = []

        remaining = volume

        while remaining > 0:

            part = min(remaining, self._rng.randint(1, self._max_single_vol))

            parts.append(part)

            remaining -= part

        self._rng.shuffle(parts)

        return parts



    def _register_order(self, order: DefensiveOrder) -> None:

        with self._lock:

            self._defensive_orders[order.order_id] = order



    def remove_order(self, order_id: str) -> None:

        with self._lock:

            self._defensive_orders.pop(order_id, None)



    def get_stats(self) -> Dict[str, Any]:

        with self._lock:

            self._cleanup_expired_orders()

            offsets = [o.random_offset_ticks for o in self._defensive_orders.values()]

            return {

                'service_name': 'MarketMakerDefenseEngine', **self._stats,

                'active_defensive_orders': len(self._defensive_orders),

                'avg_offset_ticks': sum(offsets) / max(1, len(offsets)) if offsets else 0.0,

            }



    def _cleanup_expired_orders(self, ttl_seconds: float = 300.0) -> None:

        now = time.time()

        expired = [oid for oid, o in self._defensive_orders.items()

                   if now - o.created_at > ttl_seconds]

        for oid in expired:

            del self._defensive_orders[oid]

