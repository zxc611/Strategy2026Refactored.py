# [M1-43] 订单流实时桥接服务

# MODULE_ID: M1-136

import time
import logging
import threading
from typing import Any, Dict, List, Optional

from order.order_flow_analyzer import (
    MicrostructureAnalyzer,
    MicrostructureConfig,
    VolumeWeightedOrderFlow,
    LiquidityConsumptionTracker,
)

from order.order_flow_bridge_detectors import (
    MicrostructureArbitrageDetector,
    CrossContractArbitrageDetector,
    SweepDetector,
)


class OrderFlowBridge:
    """订单流实时桥接服务

    MicrostructureAnalyzer 封装为策略可用的实时服务
    1. 从tick热路径接收成交数据（on_tick_feed）
    2. 从tick热路径接收订单簿快照（on_depth_feed）
    3. 提供实时订单流方向一致性查询（get_flow_consistency）
    4. 提供综合评估查询（get_composite_assessment）
    5. 缓存计算结果，避免热路径重复计算
    """

    DEFAULT_DEPTH_VOLUME = 50

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
            logging.info("[OrderFlowBridge] HFT增强已集合成交量加载微观套利)")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[OrderFlowBridge] HFT增强集成失败: %s", e)

        self._stats = {
            'ticks_fed': 0,
            'depth_updates': 0,
            'direction_buy': 0,
            'direction_sell': 0,
            'cache_hits': 0,
            'cache_misses': 0,
        }

        # MS-02: 深度质量统计（REAL vs INFERRED）
        self._depth_quality = {'real': 0, 'inferred': 0}
        self._last_inferred_warning = 0.0

        # R33-P2-6修复: 集成三个微结构检测器到OrderFlowBridge
        self._liquidity_tracker = LiquidityConsumptionTracker()
        self._arbitrage_detector = CrossContractArbitrageDetector()
        self._sweep_detector = SweepDetector()

    def on_tick_feed(self, instrument_id: str, price: float, volume: int,
                     exchange: str = '', bid_price: float = 0.0,
                     ask_price: float = 0.0, mid_price: float = 0.0,
                     l2_bids: List[tuple] = None, l2_asks: List[tuple] = None) -> None:
        product = self._extract_product(instrument_id)
        if not product:
            return

        # MS-02: 保存mid_price用于后续分析
        if mid_price > 0:
            self._last_mid_price = mid_price

        # MS-07: 真实L2多档深度优先
        if l2_bids and l2_asks:
            self.on_depth_feed(instrument_id, l2_bids, l2_asks, product=product)

        now = time.time()
        cache_key = instrument_id

        with self._lock:
            prev_vol = self._prev_volume.get(cache_key, 0)
            prev_ts = self._prev_timestamp.get(cache_key, 0.0)

        delta_vol = volume - prev_vol
        if delta_vol < 0:
            delta_vol = volume

        direction = ''
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
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug("[OrderFlowBridge] HFT on_trade error: %s", e)

            if self._hft_arbitrage:
                try:
                    self._hft_arbitrage.update_price(instrument_id, price, product)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug("[OrderFlowBridge] HFT update_price error: %s", e)

        with self._lock:
            self._prev_volume[cache_key] = volume
            self._prev_timestamp[cache_key] = now

        # R33-P2-6修复: 喂入流动性追踪器
        if delta_vol > 0:
            _depth_before = 0.0
            if l2_bids and l2_asks:
                _depth_before = float(sum(v for _, v in l2_bids[:1]) + sum(v for _, v in l2_asks[:1]))
            self._liquidity_tracker.on_trade(volume=float(delta_vol), depth_before=_depth_before)

        if bid_price > 0 and ask_price > 0:
            self._feed_depth_from_tick(product, bid_price, ask_price, now,
                                       delta_vol=delta_vol, direction=direction)

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
        self._depth_quality['real'] += 1

    def get_flow_consistency(self, product: str = None) -> float:
        if product is None:
            return self._get_aggregate_flow_consistency()

        now = time.time()
        self._cleanup_flow_cache(now)
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

    def _cleanup_flow_cache(self, now: float) -> None:
        if len(self._flow_cache) > 20:
            expired_keys = [k for k, v in self._flow_cache.items()
                           if now - v.get('ts', 0) > self._flow_cache_ttl]
            for k in expired_keys:
                del self._flow_cache[k]

    def _get_aggregate_flow_consistency(self) -> float:
        now = time.time()
        self._cleanup_flow_cache(now)
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
            return 'unknown'

        mid = (bid_price + ask_price) / 2.0
        spread = ask_price - bid_price
        if spread <= 0:
            return 'unknown'

        if price >= ask_price:
            return 'buy'
        elif price <= bid_price:
            return 'sell'

        if price > mid:
            return 'buy'
        if price < mid:
            return 'sell'
        return 'unknown'

    def _feed_depth_from_tick(self, product: str, bid_price: float,
                               ask_price: float, timestamp: float,
                               delta_vol: int = 0, direction: str = '') -> None:
        # MS-02: 标记INFERRED深度（从tick推断，非真实L2数据）
        depth_vol = max(delta_vol, self.DEFAULT_DEPTH_VOLUME) if delta_vol > 0 else self.DEFAULT_DEPTH_VOLUME
        if direction == 'buy':
            ask_vol = depth_vol
            bid_vol = max(depth_vol // 3, 1)
        elif direction == 'sell':
            bid_vol = depth_vol
            ask_vol = max(depth_vol // 3, 1)
        else:
            bid_vol = depth_vol
            ask_vol = depth_vol
        bids = [(bid_price, bid_vol)]
        asks = [(ask_price, ask_vol)]
        self._analyzer.on_depth(
            product, bids, asks,
            timestamp=timestamp, product=product
        )
        self._depth_quality['inferred'] += 1

        if timestamp - self._last_inferred_warning >= 300.0:
            logging.debug(
                "[MS-02] 使用推断深度(INFERRED) - 非真实L2数据: real=%d inferred=%d",
                self._depth_quality['real'], self._depth_quality['inferred'],
            )
            self._last_inferred_warning = timestamp

    def _extract_product(self, instrument_id: str) -> str:
        if instrument_id in self._product_cache:
            return self._product_cache[instrument_id]

        try:
            from infra.shared_utils import extract_product_code
            product = extract_product_code(instrument_id)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            product = ''

        if product:
            self._product_cache[instrument_id] = product
        return product

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self._stats)
        stats['products_tracked'] = len(self._analyzer.get_products())
        stats['product_cache_size'] = len(self._product_cache)
        stats['flow_cache_size'] = len(self._flow_cache)
        stats['depth_quality'] = dict(self._depth_quality)
        return stats

    def get_volume_weighted_imbalance(self, product: str) -> float:
        if self._hft_volume_flow:
            try:
                return self._hft_volume_flow.calc_volume_weighted_imbalance(product)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug("[OrderFlowBridge] calc_volume_weighted_imbalance error: %s", e)
        return 0.0

    def get_smart_money_flow(self, product: str) -> Dict[str, Any]:
        if self._hft_volume_flow:
            try:
                return self._hft_volume_flow.calc_smart_money_flow(product)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug("[OrderFlowBridge] detect_arbitrage error: %s", e)
        return None

    def get_liquidity_consumption(self) -> Dict[str, Any]:
        """R33-P2-6: 获取流动性消耗统计"""
        return self._liquidity_tracker.get_stats()

    def check_call_spread(self, calls: Dict[str, float]) -> List[Dict[str, Any]]:
        """R33-P2-6: 检查看涨期权价差单调度"""
        return self._arbitrage_detector.check_call_spread_monotonicity(calls)

    def get_arbitrage_stats(self) -> Dict[str, Any]:
        """R33-P2-6: 获取套利检测统计"""
        return self._arbitrage_detector.get_stats()

    def on_fill_event(self, direction: str, price_level: int, timestamp_ms: float = 0.0) -> Dict[str, Any]:
        """R33-P2-6: 成交事件推送到扫单检测器"""
        return self._sweep_detector.on_fill(direction, price_level, timestamp_ms)

    def get_sweep_stats(self) -> Dict[str, Any]:
        """R33-P2-6: 获取扫单检测统计"""
        return self._sweep_detector.get_stats()


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
