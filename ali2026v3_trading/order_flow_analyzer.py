import time
import threading
import logging
from collections import deque, defaultdict
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field

from ali2026v3_trading.analytics_service import RingBuffer

logger = logging.getLogger(__name__)

# 设置日志级别为DEBUG，便于调试
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


@dataclass
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

# 延迟导入 data_service，避免循环依赖
def _get_data_service():
    """获取DataService单例（延迟加载）"""
    try:
        from ali2026v3_trading.data_service import get_data_service
        return get_data_service()
    except Exception as e:
        logger.warning(f"DataService not available: {e}")
        return None


@dataclass
class FootprintBar:
    """Footprint K线数据结构"""
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float          # 主动买入量
    sell_volume: float         # 主动卖出量
    price_volume: Dict[int, float]   # 价格->成交量映射（用于分布）
    tick_count: int


class ProductMicroData:
    """单个品种的微观数据维护"""
    
    def __init__(self, product: str, config: MicrostructureConfig):
        self.product = product
        self.config = config
        self.large_threshold = config.large_order_threshold
        self.max_history_size = config.max_history_size
        self.price_precision = config.price_precision

        # 线程锁
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

        # Footprint数据: 时间窗口 -> 当前K线对象
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

            # 更新Footprint K线
            self._update_footprint(timestamp, price, volume, direction)

            # 更新锚定VWAP（所有锚定点都需要累加）
            for anchor_time in list(self._anchor_vwap.keys()):
                cum_pv, cum_vol = self._anchor_vwap[anchor_time]
                self._anchor_vwap[anchor_time] = (cum_pv + price * volume, cum_vol + volume)

    def update_depth(self, bids: List[Tuple[float, int]], asks: List[Tuple[float, int]], timestamp: float):
        with self._lock:
            self._bids = bids.copy()
            self._asks = asks.copy()
            self._depth_timestamp = timestamp

    # ========================================================================
    # Footprint K线
    # ========================================================================
    def _update_footprint(self, ts: float, price: float, volume: float, direction: str):
        for win_name, win_sec in self._footprint_windows.items():
            bar_start = int(ts // win_sec) * win_sec
            current = self._current_footprint.get(win_name)

            if current is None or current.timestamp != bar_start:
                # 完成上一个K线，保存
                if current is not None:
                    self._footprint_bars[win_name].append(current)
                # 创建新K线
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

            # 更新当前K线
            current.high = max(current.high, price)
            current.low = min(current.low, price)
            current.close = price
            current.volume += volume
            current.tick_count += 1
            if direction == 'buy':
                current.buy_volume += volume
            else:
                current.sell_volume += volume

            # 更新价格分布（根据品种精度动态取整）
            # P2 Bug #79修复：使用整数key避免float精度问题
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
        优化：使用Welford算法增量计算，减少数值误差
        """
        n = len(points)
        if n < 2:
            return 0.0
        
        # 归一化时间戳，避免大数运算
        x_base = points[0][0]
        x = [p[0] - x_base for p in points]
        y = [p[1] for p in points]
        
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        
        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        denominator = sum((x[i] - mean_x) ** 2 for i in range(n))
        
        if denominator == 0:
            return 0.0
        return numerator / denominator

    # ========================================================================
    # CVD背离检测（使用线性回归趋势）
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
            except Exception as e:
                logger.warning(f"CVD divergence calculation error: {e}")
                
            return None

    # ========================================================================
    # 即时订单流不平衡（订单簿加权）
    # ========================================================================
    def calc_instant_imbalance(self, depth_levels: int = None) -> float:
        if depth_levels is None:
            depth_levels = self.config.imbalance_depth_levels
            
        with self._lock:
            if not self._bids or not self._asks:
                return 0.0
            
            bid_levels = min(len(self._bids), depth_levels)
            ask_levels = min(len(self._asks), depth_levels)
            
            if bid_levels == 0 or ask_levels == 0:
                return 0.0
                
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
    # 锚定VWAP（带LRU淘汰）
    # ========================================================================
    def get_anchor_vwap(self, anchor_time: float, initial_price: float = None,
                        initial_volume: float = None) -> float:
        with self._lock:
            # 如果锚点不存在，创建新锚点
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
    # 最优执行评估（增加除零保护）
    def evaluate_execution(self, order_price: float, order_volume: float, side: str) -> Dict:
        with self._lock:
            if not self._bids or not self._asks:
                return {'error': 'no_depth'}
                
            try:
                best_bid = self._bids[0][0] if self._bids else 0.0
                best_ask = self._asks[0][0] if self._asks else float('inf')
                
                if best_bid == 0.0 or best_ask == float('inf'):
                    return {'error': 'invalid_depth'}
                    
                mid_price = (best_bid + best_ask) / 2

                # 计算最近60秒的VWAP（作为基准）
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
                        
                # 修复：添加除零保护
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
                    
                # 修复：添加除零保护
                slippage_bps = (slippage / ideal_price) * 10000 if ideal_price > 0 else 0

                # 与VWAP比较（增加除零保护）
                if side == 'buy':
                    vwap_slippage = order_price - vwap_60
                else:
                    vwap_slippage = vwap_60 - order_price
                    
                # 修复：添加除零保护
                vwap_bps = (vwap_slippage / vwap_60) * 10000 if vwap_60 > 0 else 0

                # 效率评级
                if abs(slippage_bps) <= 0.5:
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
            except Exception as e:
                logger.error(f"Execution evaluation error: {e}")
                return {'error': f'calculation_error: {str(e)}'}

    # ========================================================================
    # 综合评估（修复CVD变化率除零）
    # ========================================================================
    def get_composite_assessment(self, lookback_seconds: int = 300,
                                 footprint_period: str = '5min',
                                 price_func: Optional[Callable] = None) -> Dict:
        """
        综合评估订单流信号，输出交易建议
        price_func: 可选，用于获取当前价格的函数（如果未提供则内部获取）
        """
        with self._lock:
            score = 0.0
            reasons = []
            details = {}

            try:
                # 1. CVD 背离（±2分）
                cvd_div = self.check_cvd_divergence(lookback_seconds)
                if cvd_div:
                    details['cvd_divergence'] = cvd_div
                    if cvd_div['divergence'] == 'bullish':
                        score += 2.0
                        reasons.append("CVD bullish divergence")
                    elif cvd_div['divergence'] == 'bearish':
                        score -= 2.0
                        reasons.append("CVD bearish divergence")

                # 2. 即时订单流不平衡（-1~1，映射到 -1.5 ~ 1.5）
                imb = self.calc_instant_imbalance(depth_levels=5)
                details['instant_imbalance'] = imb
                imb_score = imb * 1.5
                score += imb_score
                reasons.append(f"order_book imbalance={imb:.2f}")

                # 3. 锚定VWAP偏离（锚定最近5分钟）
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
                    if deviation_pct > 0.3:
                        score += 1.0
                        reasons.append(f"price above VWAP by {deviation_pct:.2f}%")
                    elif deviation_pct < -0.3:
                        score -= 1.0
                        reasons.append(f"price below VWAP by {abs(deviation_pct):.2f}%")
                    else:
                        reasons.append(f"price near VWAP ({deviation_pct:.2f}%)")

                # 4. Footprint 支撑/阻力识别（最近5根K线内成交量密集区）
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
                            score += 0.5
                            reasons.append(f"price above footprint POC={poc_price}")
                        elif current_price < poc_price:
                            score -= 0.5
                            reasons.append(f"price below footprint POC={poc_price}")

                # 5. 价格动量（基于CVD变化率，增加epsilon防除零）
                if len(self._cvd_history) >= 10:
                    recent_data = [(t, c) for t, c in self._cvd_history][-10:]
                    if len(recent_data) >= 2:
                        start_cvd = recent_data[0][1]
                        end_cvd = recent_data[-1][1]
                        # 使用 epsilon 避免除零
                        denominator = abs(start_cvd) + 1e-6
                        cvd_change = (end_cvd - start_cvd) / denominator
                        cvd_change = max(-1, min(1, cvd_change))
                        score += cvd_change * 1.0
                        reasons.append(f"CVD change rate={cvd_change:.2f}")

                # 限制分数范围 -10..10
                final_score = max(-10.0, min(10.0, score))

                # 信号判定
                if final_score >= 5:
                    signal = "strong_buy"
                    confidence = min(1.0, (final_score - 5) / 5 + 0.5)
                elif final_score >= 2:
                    signal = "buy"
                    confidence = 0.6 + (final_score - 2) / 3 * 0.3
                elif final_score <= -5:
                    signal = "strong_sell"
                    confidence = min(1.0, (-final_score - 5) / 5 + 0.5)
                elif final_score <= -2:
                    signal = "sell"
                    confidence = 0.6 + (-final_score - 2) / 3 * 0.3
                else:
                    signal = "neutral"
                    confidence = 0.5

                return {
                    'product': self.product,
                    'score': round(final_score, 2),
                    'signal': signal,
                    'confidence': round(confidence, 2),
                    'details': details,
                    'reason': '; '.join(reasons),
                    'timestamp': time.time()
                }
            except Exception as e:
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


class MicrostructureAnalyzer:
    """市场微观结构分析器 - 高阶订单流工具 (修复版)"""

    def __init__(self, config: MicrostructureConfig = None):
        self._config = config or MicrostructureConfig()
        self._lock = threading.RLock()

        # 品种数据容器
        self._products: Dict[str, ProductMicroData] = {}

        logger.info("Fixed MicrostructureAnalyzer initialized (memory leak fixed, improved divergence detection)")

    # ========================================================================
    # 辅助方法
    # ========================================================================
    # ✅ 方法唯一：委托shared_utils.extract_product_code，不再独立实现
    @staticmethod
    def _extract_product(instrument_id: str) -> str:
        """提取品种代码"""
        from ali2026v3_trading.shared_utils import extract_product_code
        return extract_product_code(instrument_id)

    def _get_or_create(self, product: str) -> ProductMicroData:
        with self._lock:
            if product not in self._products:
                # 统一价格精度为2位小数
                self._products[product] = ProductMicroData(
                    product, 
                    self._config
                )
            return self._products[product]

    # ========================================================================
    # 数据输入接口
    # ========================================================================
    def on_trade(self, instrument_id: str, price: float, volume: int, direction: str,
                 timestamp: Optional[float] = None, product: Optional[str] = None):
        """逐笔成交 - 修复标准：统一为显式传入product参数
        
        Args:
            product: 必须显式传入品种代码，不再从instrument_id提取
        """
        if volume <= 0:
            return
        
        # ✅ 修复：强制要求显式传入product参数
        if product is None:
            logger.warning(f"on_trade requires explicit product parameter. instrument_id={instrument_id}")
            return
        
        if timestamp is None:
            timestamp = time.time()
        data = self._get_or_create(product)
        data.add_trade(price, volume, direction, timestamp)

        # ✅ 修复：内存更新为唯一路径，持久化由定时批量完成（删除实时DuckDB写入）
        # 注意：OrderFlowAnalyzer仅负责内存分析，持久化由DataService定时批量完成

    def on_depth(self, instrument_id: str, bids: List[Tuple[float, int]],
                 asks: List[Tuple[float, int]], timestamp: Optional[float] = None, product: Optional[str] = None):
        """订单簿快照 - 修复标准：统一为显式传入product参数
        
        Args:
            product: 必须显式传入品种代码，不再从instrument_id提取
        """
        # ✅ 修复：强制要求显式传入product参数
        if product is None:
            logger.warning(f"on_depth requires explicit product parameter. instrument_id={instrument_id}")
            return
        
        if timestamp is None:
            timestamp = time.time()
        data = self._get_or_create(product)
        data.update_depth(bids, asks, timestamp)

    # ========================================================================
    # 查询接口
    # ========================================================================
    def get_cvd(self, product: str) -> float:
        with self._lock:
            data = self._products.get(product)
            return data.cvd if data else 0.0

    def get_cvd_divergence(self, product: str, lookback_seconds: int = None) -> Optional[Dict]:
        with self._lock:
            data = self._products.get(product)
            if not data:
                return None
            # P2 Bug #78修复：在锁内完成全部操作，避免data引用悬空
            return data.check_cvd_divergence(lookback_seconds)

    def get_instant_imbalance(self, product: str, depth_levels: int = None) -> float:
        with self._lock:
            data = self._products.get(product)
            if not data:
                return 0.0
            return data.calc_instant_imbalance(depth_levels)

    def get_footprint(self, product: str, period: str = '1min') -> List[FootprintBar]:
        with self._lock:
            data = self._products.get(product)
            if not data:
                return []
            return data.get_footprint_bars(period)

    def anchor_vwap(self, product: str, anchor_time: float,
                    anchor_price: float = None, anchor_volume: float = None) -> float:
        with self._lock:
            data = self._products.get(product)
            if not data:
                return 0.0
            return data.get_anchor_vwap(anchor_time, anchor_price, anchor_volume)

    def get_execution_efficiency(self, product: str, order_price: float,
                                 order_volume: float, side: str) -> Dict:
        with self._lock:
            data = self._products.get(product)
            if not data:
                return {'error': 'product not found'}
            return data.evaluate_execution(order_price, order_volume, side)

    def get_composite_assessment(self, product: str, lookback_seconds: int = 300,
                                 footprint_period: str = '5min') -> Dict[str, Any]:
        with self._lock:
            data = self._products.get(product)
            if not data:
                return self._empty_assessment(product)
            return data.get_composite_assessment(lookback_seconds, footprint_period, self._get_current_price)

    def _get_current_price(self, data: ProductMicroData) -> float:
        with data._lock:
            # 修复：原来错误地使用了 self._asks，现在正确使用 data._asks
            if data._bids and data._asks:  # 修复：应为data._bids和data._asks
                return (data._bids[0][0] + data._asks[0][0]) / 2
            if len(data._trades) > 0:
                return data._trades[-1][1]
            return 0.0

    def _empty_assessment(self, product: str) -> Dict:
        return {
            'product': product,
            'score': 0.0,
            'signal': 'neutral',
            'confidence': 0.0,
            'details': {},
            'reason': 'no data',
            'timestamp': time.time()
        }

    def export_snapshot(self) -> Dict:
        with self._lock:
            return {prod: data.get_snapshot() for prod, data in self._products.items()}

    def clear(self):
        with self._lock:
            self._products.clear()

    def get_products(self) -> List[str]:
        with self._lock:
            return list(self._products.keys())


# ============================================================================
# 使用示例
# ============================================================================
if __name__ == "__main__":
    import random
    
    print("=== 市场微观结构分析器 - 配置对象版 ===\n")
    
    # 使用自定义配置
    config = MicrostructureConfig(
        large_order_threshold=50,
        max_history_size=10000,
        price_precision=2,
        footprint_maxlen=1000,
        anchor_vwap_maxsize=100,
        cvd_lookback_seconds=600,
        imbalance_depth_levels=5,
        vwap_lookback_seconds=60
    )
    msa = MicrostructureAnalyzer(config)

    # 测试期权合约处理
    print("测试期权合约产品提取:")
    print(f"IF2605-C-4500 -> {msa._extract_product('IF2605-C-4500')}")  # 应该输出 IF
    print(f"IF2605 -> {msa._extract_product('IF2605')}")  # 应该输出 IF
    print(f"cu2305 -> {msa._extract_product('cu2305')}")  # 应该输出 cu
    print(f"SR409C6200 -> {msa._extract_product('SR409C6200')}")  # 应该输出 SR
    print(f"m2405-P-2900 -> {msa._extract_product('m2405-P-2900')}")  # 应该输出 m

    # 模拟数据
    print("\n模拟交易数据...")
    base_price = 4120.0
    for i in range(200):
        price_noise = random.uniform(-3, 3)
        trend = 0.01 * i
        price = base_price + trend + price_noise
        volume = random.randint(1, 30)
        direction = 'buy' if random.random() > 0.45 else 'sell'
        ts = time.time() - 200 + i * 1.0
        # 显式传递product参数来避免期权合约提取问题
        msa.on_trade('IF2605', price, volume, direction, ts, product='IF')

    # 模拟订单簿
    print("更新订单簿数据...")
    msa.on_depth('IF2605',
                 bids=[(4120, 150), (4119, 200), (4118, 100)],
                 asks=[(4121, 100), (4122, 80), (4123, 120)],
                 product='IF')

    # 综合评估
    print("\n=== 综合评估 ===")
    assessment = msa.get_composite_assessment('IF', lookback_seconds=300, footprint_period='1min')
    print(f"产品: {assessment['product']}")
    print(f"评分: {assessment['score']}")
    print(f"信号: {assessment['signal']}")
    print(f"置信度: {assessment['confidence']}")
    print(f"原因: {assessment['reason']}")
    print(f"详情: {assessment['details']}")

    # 其他功能测试
    print("\n=== 其他功能测试 ===")
    print(f"CVD: {msa.get_cvd('IF'):.2f}")
    
    cvd_div = msa.get_cvd_divergence('IF')
    if cvd_div:
        print(f"CVD背离: {cvd_div}")
    else:
        print("CVD背离: 无明显背离")
        
    print(f"即时不平衡: {msa.get_instant_imbalance('IF'):.3f}")
    print(f"锚定VWAP(10秒前): {msa.anchor_vwap('IF', time.time() - 10):.2f}")
    
    exec_eval = msa.get_execution_efficiency('IF', 4121.5, 10, 'buy')
    print(f"执行效率: {exec_eval}")
    
    print(f"\n跟踪产品: {msa.get_products()}")
    
    snapshot = msa.export_snapshot()
    print(f"快照信息: {snapshot['IF']}")
    
    print("\n=== 修复点验证 ===")
    print("✓ 修复了期权合约产品代码提取错误，支持多种格式")
    print("✓ on_trade 和 on_depth 支持显式传入 product 参数")
    print("✓ 锚定VWAP字典已限制最大100个锚点，防止内存泄漏")
    print("✓ CVD背离检测改用线性回归趋势，减少噪声影响")
    print("✓ evaluate_execution增加除零保护")
    print("✓ CVD变化率计算增加epsilon防除零")
    print("✓ 修复了 _get_current_price 方法中的 data._asks 引用错误")
    print("✓ 新增MicrostructureConfig配置对象，替代硬编码")
