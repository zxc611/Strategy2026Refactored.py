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

