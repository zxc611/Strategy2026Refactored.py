# [M1-44] ΢_ṹ______

# MODULE_ID: M1-135

import math

import time

import threading

import logging

from collections import deque, defaultdict

from typing import Dict, List, Optional, Tuple, Any, Callable

from dataclasses import dataclass, field



from ali2026v3_trading.infra.shared_utils import RingBuffer

from ali2026v3_trading.infra.resilience import safe_divide  # R4-2: 统一安全除法

from ali2026v3_trading.infra._helpers import get_logger  # R9-5



__all__ = ['MicrostructureAnalyzer', 'MicrostructureConfig', 'VolumeWeightedOrderFlow',

           'ProductMicroData', 'FootprintBar']



# R35-P1-6标注: 原analyze()方法已在重构中拆分删除，不再存在。'
# 旧接口analyze()已废弃，新接口为:

#   - MicrostructureAnalyzer.get_composite_assessment() (综合评估)

#   - VolumeWeightedOrderFlow.calc_volume_weighted_imbalance() (成交量加权失败

#   - VolumeWeightedOrderFlow.calc_smart_money_flow() (聪明钱流水

# 调用例 OrderFlowBridge -> MicrostructureAnalyzer.get_composite_assessment()

#         OrderFlowBridge -> VolumeWeightedOrderFlow (通过滤hft_volume_flow成员)



logger = get_logger(__name__)  # R9-5





@dataclass(slots=True)

class MicrostructureConfig:

    """微结构分析器配置"""

    large_order_threshold: int = 50

    max_history_size: int = 10000

    price_precision: int = 2

    footprint_maxlen: int = 1000

    anchor_vwap_maxsize: int = 100

    cvd_lookback_seconds: int = 600

    imbalance_depth_levels: int = 5

    vwap_lookback_seconds: int = 60

    

    IMBALANCE_SCORE_MULTIPLIER = 1.5

    OFI_SHORT_TERM_WEIGHT = 0.6

    OFI_LONG_TERM_WEIGHT = 0.4

    DEVIATION_THRESHOLD = 0.3

    SLIPPAGE_THRESHOLD_BPS = 0.5

    SCORE_INCREMENT = 0.5

    BASE_CONFIDENCE = 0.5

    MODERATE_CONFIDENCE = 0.6

    CONFIDENCE_ADJUSTMENT = 0.3

    LARGE_ORDER_RATIO_THRESHOLD = 0.3

    VOLUME_NORMALIZATION_FACTOR = 0.5

    SMART_MONEY_WEIGHT = 3.0

    LARGE_ORDER_WEIGHT = 2.0



# 延迟导入 data_service，避免循环依赖

def _get_data_service():

    """获取DataService单例（延迟加载）"""

    try:

        from ali2026v3_trading.data.data_service import get_data_service

        return get_data_service()

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

        logger.warning(f"DataService not available: {e}")

        return None





@dataclass(slots=True)

class FootprintBar:

    """Footprint K线数据结束"""

    timestamp: float

    open: float

    high: float

    low: float

    close: float

    volume: float

    buy_volume: float          # 主动买入口

    sell_volume: float         # 主动卖出量

    price_volume: Dict[int, float]   # 价格->成交量映射（用于分布布

    tick_count: int





class ProductMicroData:

    """单个品种的微观数据维度"""

    

    def __init__(self, product: str, config: MicrostructureConfig):

        self.product = product

        self.config = config

        self.large_threshold = config.large_order_threshold

        self.max_history_size = config.max_history_size

        self.price_precision = config.price_precision



        # 线程池

        self._lock = threading.RLock()



        # 成交记录 (timestamp, price, volume, direction)

        self._trades = RingBuffer(maxlen=self.max_history_size)

        # 订单簿快照

        self._bids: List[Tuple[float, int]] = []

        self._asks: List[Tuple[float, int]] = []

        self._depth_timestamp = 0.0



        # CVD累计

        self.cvd = 0.0

        self._price_history = RingBuffer(maxlen=self.max_history_size)   # (timestamp, price)

        self._cvd_history = RingBuffer(maxlen=self.max_history_size)     # (timestamp, cvd)



        # Footprint数据: 时间窗口 -> 当前K线对手

        self._footprint_windows = {

            '1min': 60,

            '5min': 300,

            '15min': 900,

            '30min': 1800,

            '1hour': 3600,

        }

        self._current_footprint: Dict[str, FootprintBar] = {}

        self._footprint_bars: Dict[str, RingBuffer] = {}

        for period in self._footprint_windows:

            self._footprint_bars[period] = RingBuffer(maxlen=config.footprint_maxlen)



        # 锚定VWAP缓存 (限制大小防止内存泄漏)

        self._anchor_vwap: Dict[float, Tuple[float, float]] = {}  # anchor_time -> (cum_price*vol, cum_vol)

        self._anchor_vwap_order: List[float] = []  # 记录锚点添加顺序，用于LRU

        self._anchor_vwap_maxsize = config.anchor_vwap_maxsize



    # ========================================================================

    # 数据更新

    # ========================================================================

    def add_trade(self, price: float, volume: float, direction: str, timestamp: float):

        with self._lock:

            # 存储成交

            self._trades.append((timestamp, price, volume, direction))



            # 更新CVD

            delta = volume if direction == 'buy' else -volume

            self.cvd += delta

            self._price_history.append((timestamp, price))

            self._cvd_history.append((timestamp, self.cvd))



            # 更新Footprint K_

            self._update_footprint(timestamp, price, volume, direction)



            # 更新锚定VWAP（所有锚定点都需要累加）'
            for anchor_time in list(self._anchor_vwap.keys()):

                cum_pv, cum_vol = self._anchor_vwap[anchor_time]

                self._anchor_vwap[anchor_time] = (cum_pv + price * volume, cum_vol + volume)



    def update_depth(self, bids: List[Tuple[float, int]], asks: List[Tuple[float, int]], timestamp: float):

        with self._lock:

            self._bids = bids.copy()

            self._asks = asks.copy()

            self._depth_timestamp = timestamp



    # ========================================================================

    # Footprint K_

    # ========================================================================

    def _update_footprint(self, ts: float, price: float, volume: float, direction: str):

        for win_name, win_sec in self._footprint_windows.items():

            bar_start = int(ts // win_sec) * win_sec

            current = self._current_footprint.get(win_name)



            if current is None or current.timestamp != bar_start:

                # 完成上一个K线，保存

                if current is not None:

                    self._footprint_bars[win_name].append(current)

                # 创建新K_

                current = FootprintBar(

                    timestamp=bar_start,

                    open=price,

                    high=price,

                    low=price,

                    close=price,

                    volume=0.0,

                    buy_volume=0.0,

                    sell_volume=0.0,

                    price_volume={},

                    tick_count=0

                )

                self._current_footprint[win_name] = current



            # 更新当前K_

            current.high = max(current.high, price)

            current.low = min(current.low, price)

            current.close = price

            current.volume += volume

            current.tick_count += 1

            if direction == 'buy':

                current.buy_volume += volume

            else:

                current.sell_volume += volume



            # 更新价格分布（根据品种精度动态取整）'
            # P2 Bug #79修复：使用整数key避免float精度问题

            # NP-P2-09: price极大时price_key溢出保护

            if abs(price) > 1e8:

                price = round(price, self.price_precision)

            price_key = int(round(price * (10 ** self.price_precision)))

            current.price_volume[price_key] = current.price_volume.get(price_key, 0) + volume



    def get_footprint_bars(self, period: str) -> List[FootprintBar]:

        with self._lock:

            if period not in self._footprint_bars:

                return []

            return self._footprint_bars[period].to_list()



    # ========================================================================

    # 辅助函数：线性回归斜率（增量优化版）

    # ========================================================================

    @staticmethod

    def _linear_trend_slope(points: List[Tuple[float, float]]) -> float:

        """

        计算一组点 (x, y) 的线性回归斜率

        使用归一化时间戳 + 批量均值法，O(n)复杂。'
        """

        n = len(points)

        if n < 2:

            return 0.0

        

        # 归一化时间戳，避免大数运行

        x_base = points[0][0]

        x = [p[0] - x_base for p in points]

        y = [p[1] for p in points]

        

        mean_x = sum(x) / n

        mean_y = sum(y) / n

        

        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))

        denominator = sum((x[i] - mean_x) ** 2 for i in range(n))



        return safe_divide(numerator, denominator, default=0.0)  # R4-2: 使用统一safe_divide



    # ========================================================================

    # CVD背离检测（使用线性回归趋势）'
    # ========================================================================

    def check_cvd_divergence(self, lookback_seconds: int = None) -> Optional[Dict]:

        """检测最近一段时间内价格与CVD的背离（基于线性回归趋势）"""

        if lookback_seconds is None:

            lookback_seconds = self.config.cvd_lookback_seconds

            

        with self._lock:

            if len(self._price_history) < 10 or len(self._cvd_history) < 10:

                return None



            now = time.time()

            cutoff = now - lookback_seconds

            prices = [(ts, p) for ts, p in self._price_history if ts >= cutoff]

            cvd_vals = [(ts, c) for ts, c in self._cvd_history if ts >= cutoff]



            if len(prices) < 5 or len(cvd_vals) < 5:

                return None



            try:

                # 使用线性回归计算趋势斜率

                price_slope = self._linear_trend_slope(prices)

                cvd_slope = self._linear_trend_slope(cvd_vals)

                

                # 判断背离：价格和CVD趋势相反（符号相反）

                if price_slope > 0 and cvd_slope < 0:  # 价格上涨，CVD下降 -> 看跌背离

                    return {

                        'divergence': 'bearish',

                        'price_trend': price_slope,

                        'cvd_trend': cvd_slope,

                        'timestamp': now

                    }

                elif price_slope < 0 and cvd_slope > 0:  # 价格下跌，CVD上升 -> 看涨背离

                    return {

                        'divergence': 'bullish',

                        'price_trend': price_slope,

                        'cvd_trend': cvd_slope,

                        'timestamp': now

                    }

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logger.warning(f"CVD divergence calculation error: {e}")

                

            return None



    # ========================================================================

    # 即时订单流不平衡（订单簿加权益

    # ========================================================================

    def calc_instant_imbalance(self, depth_levels: int = None) -> float:

        if depth_levels is None:

            depth_levels = self.config.imbalance_depth_levels

            

        with self._lock:

            # MS-P2修复: 深度快照过期检测——若深度数据超过60秒未更新新

            # 说明订单簿数据已失效，返回指数衰减后的指

            now = time.time()

            depth_age = now - self._depth_timestamp

            if depth_age > 60.0:

                decay = math.exp(-depth_age / 60.0)  # _0秒衰减到1/e_7%

                logger.warning(f"Depth snapshot stale ({depth_age:.0f}s old), returning decayed imbalance")

                # 如果深度数据存在，计算实际值后衰减；否则返。'
                if not self._bids or not self._asks:

                    return 0.0

                raw_imb = self._calc_raw_imbalance(bid_levels=min(len(self._bids), depth_levels or self.config.imbalance_depth_levels),

                                                    ask_levels=min(len(self._asks), depth_levels or self.config.imbalance_depth_levels))

                return raw_imb * decay

            

            if not self._bids or not self._asks:

                return 0.0

            

            bid_levels = min(len(self._bids), depth_levels or self.config.imbalance_depth_levels)

            ask_levels = min(len(self._asks), depth_levels or self.config.imbalance_depth_levels)

            

            if bid_levels == 0 or ask_levels == 0:

                return 0.0

                

            return self._calc_raw_imbalance(bid_levels, ask_levels)



    def _calc_raw_imbalance(self, bid_levels: int, ask_levels: int) -> float:

        """MS-P2: 提取为独立方法，供calc_instant_imbalance正常路径和过期衰减路径共享"""

        bid_weighted = 0.0

        ask_weighted = 0.0

        

        for i in range(bid_levels):

            _, vol = self._bids[i]

            bid_weighted += vol * (1.0 / (i + 1))

            

        for i in range(ask_levels):

            _, vol = self._asks[i]

            ask_weighted += vol * (1.0 / (i + 1))

            

        total = bid_weighted + ask_weighted

        if total == 0:

            return 0.0

        return (bid_weighted - ask_weighted) / total



    # ========================================================================

    # 订单流失衡指标(OFI - Order Flow Imbalance)

    # ========================================================================

    def calc_ofi(self, lookback_seconds: int = 60) -> float:

        with self._lock:

            buy_vol = 0.0

            sell_vol = 0.0

            now = time.time()

            cutoff = now - lookback_seconds



            for ts, _price, volume, direction in self._trades:

                if ts >= cutoff:

                    if direction == 'buy':

                        buy_vol += volume

                    else:

                        sell_vol += volume



            total = buy_vol + sell_vol

            if total == 0:

                return 0.0

            return (buy_vol - sell_vol) / total



    def calc_multi_timeframe_ofi(self, timeframes: List[int] = None) -> Dict[int, float]:

        if timeframes is None:

            timeframes = [10, 30, 60, 300]

        return {tf: self.calc_ofi(lookback_seconds=tf) for tf in timeframes}



    # ========================================================================

    # 锚定VWAP（带LRU淘汰。'
    # ========================================================================

    def get_anchor_vwap(self, anchor_time: float, initial_price: float = None,

                        initial_volume: float = None) -> float:

        with self._lock:

            # 如果锚点不存在，创建新锚。

            if anchor_time not in self._anchor_vwap:

                # LRU淘汰：如果超过最大数量，删除最旧的锚点

                if len(self._anchor_vwap) >= self._anchor_vwap_maxsize:

                    oldest = self._anchor_vwap_order.pop(0)

                    del self._anchor_vwap[oldest]

                

                if initial_price is not None and initial_volume is not None:

                    self._anchor_vwap[anchor_time] = (initial_price * initial_volume, initial_volume)

                else:

                    self._anchor_vwap[anchor_time] = (0.0, 0.0)

                self._anchor_vwap_order.append(anchor_time)

            

            cum_pv, cum_vol = self._anchor_vwap[anchor_time]

            if cum_vol == 0:

                return 0.0

            return cum_pv / cum_vol



    # ========================================================================

    # 最优执行评估（增加除零保护。

    def evaluate_execution(self, order_price: float, order_volume: float, side: str) -> Dict:

        with self._lock:

            if not self._bids or not self._asks:

                return {'error': 'no_depth'}

                

            try:

                best_bid = self._bids[0][0] if self._bids else 0.0

                best_ask = self._asks[0][0] if self._asks else float('inf')

                

                MIN_PRICE_THRESHOLD = 1e-6  # NP-P2-26: 极小值保存

                if best_bid == 0.0 or best_ask == float('inf') or best_bid < MIN_PRICE_THRESHOLD or best_ask < MIN_PRICE_THRESHOLD:

                    return {'error': 'invalid_depth'}

                    

                mid_price = (best_bid + best_ask) / 2



                # 计算最小0秒的VWAP（作为基准）'
                now = time.time()

                cutoff = now - self.config.vwap_lookback_seconds

                cum_pv = 0.0

                cum_vol = 0.0

                trades_count = 0

                

                for ts, p, v, _ in self._trades:

                    if ts >= cutoff:

                        cum_pv += p * v

                        cum_vol += v

                        trades_count += 1

                        

                # 修复：添加除零保存

                vwap_60 = cum_pv / cum_vol if cum_vol > 0 else mid_price



                # 理想价格：买方用ask，卖方用bid

                if side == 'buy':

                    ideal_price = best_ask

                else:

                    ideal_price = best_bid



                # 计算滑点

                if side == 'buy':

                    slippage = order_price - ideal_price

                else:

                    slippage = ideal_price - order_price

                    

                # 修复：添加除零保存

                slippage_bps = (slippage / ideal_price) * 10000 if ideal_price > 0 else 0



                # 与VWAP比较（增加除零保护）'
                if side == 'buy':

                    vwap_slippage = order_price - vwap_60

                else:

                    vwap_slippage = vwap_60 - order_price

                    

                # 修复：添加除零保存

                vwap_bps = (vwap_slippage / vwap_60) * 10000 if vwap_60 > 0 else 0



                # 效率评级

                if abs(slippage_bps) <= MicrostructureConfig.SLIPPAGE_THRESHOLD_BPS:

                    efficiency = 'excellent'

                elif abs(slippage_bps) <= 2:

                    efficiency = 'good'

                elif abs(slippage_bps) <= 10:

                    efficiency = 'acceptable'

                else:

                    efficiency = 'poor'



                return {

                    'order_price': order_price,

                    'mid_price': mid_price,

                    'vwap_60': vwap_60,

                    'ideal_price': ideal_price,

                    'slippage_bps': round(slippage_bps, 2),

                    'vwap_slippage_bps': round(vwap_bps, 2),

                    'efficiency': efficiency,

                    'timestamp': now,

                    'recent_trades': trades_count

                }

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logger.error(f"Execution evaluation error: {e}")

                return {'error': f'calculation_error: {str(e)}'}



    # ========================================================================

    # 综合评估（修复CVD变化率除零）'
    # ========================================================================

    def get_composite_assessment(self, lookback_seconds: int = 300,

                                 footprint_period: str = '5min',

                                 price_func: Optional[Callable] = None) -> Dict:

        """

        综合评估订单流信号，输出交易建议

        price_func: 可选，用于获取当前价格的函数（如果未提供则内部获取消

        """

        with self._lock:

            score = 0.0

            reasons = []

            details = {}



            try:

                # 1. CVD 背离（_分）'
                cvd_div = self.check_cvd_divergence(lookback_seconds)

                if cvd_div:

                    details['cvd_divergence'] = cvd_div

                    if cvd_div['divergence'] == 'bullish':

                        score += 2.0

                        reasons.append("CVD bullish divergence")

                    elif cvd_div['divergence'] == 'bearish':

                        score -= 2.0

                        reasons.append("CVD bearish divergence")



                # 2. 即时订单流不平衡。1~1，映射到 -1.5 ~ 1.5_

                imb = self.calc_instant_imbalance(depth_levels=5)

                details['instant_imbalance'] = imb

                imb_score = imb * MicrostructureConfig.IMBALANCE_SCORE_MULTIPLIER

                score += imb_score

                reasons.append(f"order_book imbalance={imb:.2f}")



                # 3. 锚定VWAP偏离（锚定最小分钟频

                anchor_time = time.time() - 300

                vwap = self.get_anchor_vwap(anchor_time)

                

                # 获取当前价格

                current_price = 0.0

                if price_func:

                    current_price = price_func(self)

                else:

                    if self._bids and self._asks:

                        current_price = (self._bids[0][0] + self._asks[0][0]) / 2

                    elif len(self._trades) > 0:

                        current_price = self._trades[-1][1]

                        

                if vwap > 0 and current_price > 0:

                    deviation_pct = (current_price - vwap) / vwap * 100

                    details['vwap_deviation_pct'] = deviation_pct

                    if deviation_pct > MicrostructureConfig.DEVIATION_THRESHOLD:

                        score += 1.0

                        reasons.append(f"price above VWAP by {deviation_pct:.2f}%")

                    elif deviation_pct < -MicrostructureConfig.DEVIATION_THRESHOLD:

                        score -= 1.0

                        reasons.append(f"price below VWAP by {abs(deviation_pct):.2f}%")

                    else:

                        reasons.append(f"price near VWAP ({deviation_pct:.2f}%)")



                # 4. Footprint 支撑/阻力识别（最小根K线内成交量密集区。'
                footprint_bars = self.get_footprint_bars(footprint_period)

                if footprint_bars and current_price > 0:

                    vol_profile = {}

                    for bar in footprint_bars[-5:]:

                        for price_int, vol in bar.price_volume.items():

                            vol_profile[price_int] = vol_profile.get(price_int, 0) + vol

                    if vol_profile:

                        poc_price = max(vol_profile, key=vol_profile.get)

                        details['footprint_poc'] = poc_price

                        details['footprint_volume'] = vol_profile[poc_price]

                        if current_price > poc_price:

                            score += MicrostructureConfig.SCORE_INCREMENT

                            reasons.append(f"price above footprint POC={poc_price}")

                        elif current_price < poc_price:

                            score -= MicrostructureConfig.SCORE_INCREMENT

                            reasons.append(f"price below footprint POC={poc_price}")



                # 5. 价格动量（基于CVD变化率，增加epsilon防除零）'
                if len(self._cvd_history) >= 10:

                    recent_data = [(t, c) for t, c in self._cvd_history][-10:]

                    if len(recent_data) >= 2:

                        start_cvd = recent_data[0][1]

                        end_cvd = recent_data[-1][1]

                        # R5-5: 使用safe_divide替代手动epsilon除法保护

                        cvd_change = safe_divide(end_cvd - start_cvd, abs(start_cvd), default=0.0, min_denominator=1e-6)

                        cvd_change = max(-1, min(1, cvd_change))

                        score += cvd_change * 1.0

                        reasons.append(f"CVD change rate={cvd_change:.2f}")



                # 6. 订单流失衡指标OFI（_.5分，基于逐笔成交买方/卖方力量对比。'
                ofi_10s = self.calc_ofi(lookback_seconds=10)

                ofi_60s = self.calc_ofi(lookback_seconds=60)

                details['ofi_10s'] = ofi_10s

                details['ofi_60s'] = ofi_60s

                ofi_weighted = ofi_10s * MicrostructureConfig.OFI_SHORT_TERM_WEIGHT + ofi_60s * MicrostructureConfig.OFI_LONG_TERM_WEIGHT

                ofi_score = ofi_weighted * MicrostructureConfig.IMBALANCE_SCORE_MULTIPLIER

                score += ofi_score

                reasons.append(f"OFI(10s)={ofi_10s:.2f}, OFI(60s)={ofi_60s:.2f}")



                # 限制分数范围 -10..10

                final_score = max(-10.0, min(10.0, score))



                # 信号判定

                if final_score >= 5:

                    signal = "strong_buy"

                    confidence = min(1.0, (final_score - 5) / 5 + MicrostructureConfig.BASE_CONFIDENCE)

                elif final_score >= 2:

                    signal = "buy"

                    confidence = MicrostructureConfig.MODERATE_CONFIDENCE + (final_score - 2) / 3 * MicrostructureConfig.CONFIDENCE_ADJUSTMENT

                elif final_score <= -5:

                    signal = "strong_sell"

                    confidence = min(1.0, (-final_score - 5) / 5 + MicrostructureConfig.BASE_CONFIDENCE)

                elif final_score <= -2:

                    signal = "sell"

                    confidence = MicrostructureConfig.MODERATE_CONFIDENCE + (-final_score - 2) / 3 * MicrostructureConfig.CONFIDENCE_ADJUSTMENT

                else:

                    signal = "neutral"

                    confidence = MicrostructureConfig.BASE_CONFIDENCE



                return {

                    'product': self.product,

                    'score': round(final_score, 2),

                    'signal': signal,

                    'confidence': round(confidence, 2),

                    'details': details,

                    'reason': '; '.join(reasons),

                    'timestamp': time.time()

                }

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logger.error(f"Composite assessment error: {e}")

                return {

                    'product': self.product,

                    'score': 0.0,

                    'signal': 'neutral',

                    'confidence': 0.0,

                    'details': {},

                    'reason': f'calculation_error: {str(e)}',

                    'timestamp': time.time()

                }



    # ========================================================================

    # 快照导出

    # ========================================================================

    def get_snapshot(self) -> Dict:

        with self._lock:

            return {

                'product': self.product,

                'cvd': self.cvd,

                'depth_timestamp': self._depth_timestamp,

                'trade_count': len(self._trades),

                'price_history_len': len(self._price_history),

                'cvd_history_len': len(self._cvd_history),

                'footprint_bars': {k: len(v) for k, v in self._footprint_bars.items()},

                'anchor_vwap_count': len(self._anchor_vwap)

            }

