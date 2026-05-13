import time
import logging
import math
import random
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.order_flow_analyzer import (
    MicrostructureAnalyzer,
    MicrostructureConfig,
    VolumeWeightedOrderFlow,
)


class OrderFlowBridge:
    """订单流实时桥接服务

    将 MicrostructureAnalyzer 封装为策略可用的实时服务：
    1. 从tick热路径接收成交数据（on_tick_feed）
    2. 从tick热路径接收订单簿快照（on_depth_feed）
    3. 提供实时订单流方向一致性查询（get_flow_consistency）
    4. 提供综合评估查询（get_composite_assessment）
    5. 缓存计算结果，避免热路径重复计算
    """

    def __init__(self, config: MicrostructureConfig = None):
        self._config = config or MicrostructureConfig()
        self._analyzer = MicrostructureAnalyzer(self._config)
        self._lock = threading.RLock()

        self._product_cache: Dict[str, str] = {}
        self._prev_volume: Dict[str, int] = {}
        self._prev_timestamp: Dict[str, float] = {}

        self._flow_cache: Dict[str, Dict[str, Any]] = {}
        self._flow_cache_ttl = 5.0
        self._flow_cache_timestamp: float = 0.0

        self._hft_volume_flow: Optional[VolumeWeightedOrderFlow] = None
        self._hft_arbitrage: Optional[MicrostructureArbitrageDetector] = None
        try:
            self._hft_volume_flow = VolumeWeightedOrderFlow()
            self._hft_arbitrage = MicrostructureArbitrageDetector()
            logging.info("[OrderFlowBridge] HFT增强已集成(成交量加权+微观套利)")
        except Exception as e:
            logging.warning("[OrderFlowBridge] HFT增强集成失败: %s", e)

        self._stats = {
            'ticks_fed': 0,
            'depth_updates': 0,
            'direction_buy': 0,
            'direction_sell': 0,
            'cache_hits': 0,
            'cache_misses': 0,
        }

    def on_tick_feed(self, instrument_id: str, price: float, volume: int,
                     exchange: str = '', bid_price: float = 0.0,
                     ask_price: float = 0.0) -> None:
        product = self._extract_product(instrument_id)
        if not product:
            return

        now = time.time()
        cache_key = f"{product}"

        prev_vol = self._prev_volume.get(cache_key, 0)
        prev_ts = self._prev_timestamp.get(cache_key, 0.0)

        delta_vol = volume - prev_vol
        if delta_vol < 0:
            delta_vol = volume

        if delta_vol > 0 and prev_ts > 0:
            direction = self._infer_direction(price, bid_price, ask_price)
            self._analyzer.on_trade(
                instrument_id, price, delta_vol, direction,
                timestamp=now, product=product
            )
            self._stats['ticks_fed'] += 1
            if direction == 'buy':
                self._stats['direction_buy'] += 1
            else:
                self._stats['direction_sell'] += 1

            if self._hft_volume_flow:
                try:
                    self._hft_volume_flow.on_trade(product, price, delta_vol, direction)
                except Exception as e:
                    logging.debug("[OrderFlowBridge] HFT on_trade error: %s", e)

            if self._hft_arbitrage:
                try:
                    self._hft_arbitrage.update_price(instrument_id, price, product)
                except Exception as e:
                    logging.debug("[OrderFlowBridge] HFT update_price error: %s", e)

        self._prev_volume[cache_key] = volume
        self._prev_timestamp[cache_key] = now

        if bid_price > 0 and ask_price > 0:
            self._feed_depth_from_tick(product, bid_price, ask_price, now)

    def on_depth_feed(self, instrument_id: str,
                      bids: List[tuple], asks: List[tuple],
                      product: str = None) -> None:
        if product is None:
            product = self._extract_product(instrument_id)
        if not product:
            return

        self._analyzer.on_depth(
            instrument_id, bids, asks,
            timestamp=time.time(), product=product
        )
        self._stats['depth_updates'] += 1

    def get_flow_consistency(self, product: str = None) -> float:
        if product is None:
            return self._get_aggregate_flow_consistency()

        now = time.time()
        cache_key = f"fc_{product}"
        cached = self._flow_cache.get(cache_key)
        if cached and (now - cached.get('ts', 0)) < self._flow_cache_ttl:
            self._stats['cache_hits'] += 1
            return cached.get('value', 0.0)

        self._stats['cache_misses'] += 1

        ofi = self._analyzer.get_ofi(product, lookback_seconds=60)
        imbalance = self._analyzer.get_instant_imbalance(product)

        consistency = ofi * 0.6 + imbalance * 0.4
        consistency = max(-1.0, min(1.0, consistency))

        self._flow_cache[cache_key] = {'value': consistency, 'ts': now}
        return consistency

    def _get_aggregate_flow_consistency(self) -> float:
        now = time.time()
        cached = self._flow_cache.get('_aggregate_fc')
        if cached and (now - cached.get('ts', 0)) < self._flow_cache_ttl:
            self._stats['cache_hits'] += 1
            return cached.get('value', 0.0)

        self._stats['cache_misses'] += 1

        products = self._analyzer.get_products()
        if not products:
            return 0.0

        total_consistency = 0.0
        count = 0
        for prod in products:
            ofi = self._analyzer.get_ofi(prod, lookback_seconds=60)
            if ofi != 0.0:
                total_consistency += ofi
                count += 1

        if count == 0:
            return 0.0

        result = max(-1.0, min(1.0, total_consistency / count))
        self._flow_cache['_aggregate_fc'] = {'value': result, 'ts': now}
        return result

    def get_composite_assessment(self, product: str,
                                  lookback_seconds: int = 300) -> Dict[str, Any]:
        return self._analyzer.get_composite_assessment(product, lookback_seconds)

    def get_ofi(self, product: str, lookback_seconds: int = 60) -> float:
        return self._analyzer.get_ofi(product, lookback_seconds)

    def get_instant_imbalance(self, product: str) -> float:
        return self._analyzer.get_instant_imbalance(product)

    def get_cvd(self, product: str) -> float:
        return self._analyzer.get_cvd(product)

    def get_cvd_divergence(self, product: str) -> Optional[Dict]:
        return self._analyzer.get_cvd_divergence(product)

    def get_products(self) -> List[str]:
        return self._analyzer.get_products()

    def _infer_direction(self, price: float, bid_price: float, ask_price: float) -> str:
        if bid_price <= 0 or ask_price <= 0:
            return 'buy'

        mid = (bid_price + ask_price) / 2.0
        spread = ask_price - bid_price
        if spread <= 0:
            return 'buy'

        if price >= ask_price:
            return 'buy'
        elif price <= bid_price:
            return 'sell'

        if price > mid:
            return 'buy'
        return 'sell'

    def _feed_depth_from_tick(self, product: str, bid_price: float,
                               ask_price: float, timestamp: float) -> None:
        bid_vol = 100
        ask_vol = 100
        bids = [(bid_price, bid_vol)]
        asks = [(ask_price, ask_vol)]
        self._analyzer.on_depth(
            product, bids, asks,
            timestamp=timestamp, product=product
        )

    def _extract_product(self, instrument_id: str) -> str:
        if instrument_id in self._product_cache:
            return self._product_cache[instrument_id]

        try:
            from ali2026v3_trading.shared_utils import extract_product_code
            product = extract_product_code(instrument_id)
        except Exception:
            product = ''

        if product:
            self._product_cache[instrument_id] = product
        return product

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self._stats)
        stats['products_tracked'] = len(self._analyzer.get_products())
        stats['product_cache_size'] = len(self._product_cache)
        stats['flow_cache_size'] = len(self._flow_cache)
        return stats

    def get_volume_weighted_imbalance(self, product: str) -> float:
        if self._hft_volume_flow:
            try:
                return self._hft_volume_flow.calc_volume_weighted_imbalance(product)
            except Exception as e:
                logging.debug("[OrderFlowBridge] calc_volume_weighted_imbalance error: %s", e)
        return 0.0

    def get_smart_money_flow(self, product: str) -> Dict[str, Any]:
        if self._hft_volume_flow:
            try:
                return self._hft_volume_flow.calc_smart_money_flow(product)
            except Exception as e:
                logging.debug("[OrderFlowBridge] calc_smart_money_flow error: %s", e)
        return {'smart_buy': 0.0, 'smart_sell': 0.0, 'net_flow': 0.0, 'signal': 'neutral'}

    def detect_arbitrage(self, instrument_id: str, current_price: float,
                         product: str = '', fair_value: Optional[float] = None) -> Optional[Dict[str, Any]]:
        if self._hft_arbitrage:
            try:
                opp = self._hft_arbitrage.detect_arbitrage(instrument_id, current_price, product, fair_value)
                if opp:
                    return {
                        'opportunity_id': opp.opportunity_id,
                        'direction': opp.direction,
                        'deviation_bps': opp.deviation_bps,
                        'confidence': opp.confidence,
                        'entry_price': opp.entry_price,
                        'fair_value': opp.fair_value,
                    }
            except Exception as e:
                logging.debug("[OrderFlowBridge] detect_arbitrage error: %s", e)
        return None


_order_flow_bridge: Optional[OrderFlowBridge] = None
_order_flow_bridge_lock = threading.Lock()


def get_order_flow_bridge(config: MicrostructureConfig = None) -> OrderFlowBridge:
    global _order_flow_bridge
    if _order_flow_bridge is None:
        with _order_flow_bridge_lock:
            if _order_flow_bridge is None:
                _order_flow_bridge = OrderFlowBridge(config=config)
                logging.info("[OrderFlowBridge] Initialized, ready for tick feed")
    return _order_flow_bridge


__all__ = [
    'OrderFlowBridge',
    'get_order_flow_bridge',
    'MicrostructureArbitrageDetector',
    'ArbitrageOpportunity',
    'MarketMakerDefenseEngine',
    'OrderDefenseType',
    'DefensiveOrder',
]


@dataclass
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
        self._stats['total_checks'] += 1
        with self._lock:
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
    """跨合约套利检测：同标的不同行权价/到期日期权间的相对偏离

    原理：同标的期权应满足无套利约束（如call spread单调性）。
    违反时即存在套利机会。订单簿失衡可辅助确认方向。
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


class SweepDetector:
    """扫单行为识别器：检测做市商短时间内多档扫单

    原理：如果在detect_window_ms内，同一方向连续成交
    超过sweep_depth_levels个档位，判定为扫单行为，
    触发防御（暂停该方向挂单、切换IOC模式）。
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


@dataclass
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
