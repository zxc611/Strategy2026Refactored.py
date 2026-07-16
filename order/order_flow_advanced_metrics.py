# [M1-44-03] 订单流分析器-高级指标

import time
import threading
from collections import deque
import math
from typing import Any, Dict, List, Optional

from order.order_flow_data_structures import MicrostructureConfig


class VolumeWeightedOrderFlow:

    def __init__(self, large_order_threshold: int = 50, smart_money_threshold: int = 100,

                 lookback_seconds: int = 300, decay_factor: float = 0.95):

        self._large_threshold = large_order_threshold

        self._smart_money_threshold = smart_money_threshold

        self._lookback = lookback_seconds

        self._decay = decay_factor

        self._trades: Dict[str, deque] = {}

        self._cumulative_weighted_flow: Dict[str, float] = {}

        self._lock = threading.RLock()

        self._stats = {

            'total_trades': 0, 'large_trades': 0,

            'smart_money_trades': 0, 'noise_filtered': 0,

        }



    def on_trade(self, product: str, price: float, volume: int, direction: str) -> Dict[str, Any]:

        self._stats['total_trades'] += 1

        with self._lock:

            if product not in self._trades:

                self._trades[product] = deque(maxlen=5000)

            is_large = volume >= self._large_threshold

            is_smart_money = volume >= self._smart_money_threshold

            if is_smart_money:

                self._stats['smart_money_trades'] += 1

            elif is_large:

                self._stats['large_trades'] += 1

            else:

                self._stats['noise_filtered'] += 1

            weight = self._calc_volume_weight(volume)

            self._trades[product].append({

                'timestamp': time.time(), 'price': price, 'volume': volume,

                'direction': direction, 'weight': weight,

                'is_large': is_large, 'is_smart_money': is_smart_money,

            })

            delta = volume * weight if direction == 'buy' else -volume * weight

            self._cumulative_weighted_flow[product] = (

                self._cumulative_weighted_flow.get(product, 0.0) * self._decay + delta)

            return {

                'product': product, 'volume': volume, 'weight': weight,

                'is_large': is_large, 'is_smart_money': is_smart_money,

                'cumulative_flow': self._cumulative_weighted_flow[product],

            }



    def calc_volume_weighted_imbalance(self, product: str, depth_levels: int = 5) -> float:

        with self._lock:

            history = self._trades.get(product)

            if not history:

                return 0.0

            now = time.time()

            cutoff = now - self._lookback

            weighted_buy = weighted_sell = 0.0

            for trade in history:

                if trade['timestamp'] < cutoff:

                    continue

                if trade['direction'] == 'buy':

                    weighted_buy += trade['volume'] * trade['weight']

                else:

                    weighted_sell += trade['volume'] * trade['weight']

            total = weighted_buy + weighted_sell

            if total == 0:

                return 0.0

            return (weighted_buy - weighted_sell) / total



    def calc_smart_money_flow(self, product: str) -> Dict[str, Any]:

        with self._lock:

            history = self._trades.get(product)

            if not history:

                return {'smart_buy': 0.0, 'smart_sell': 0.0, 'net_flow': 0.0, 'signal': 'neutral'}

            now = time.time()

            cutoff = now - self._lookback

            smart_buy = smart_sell = 0.0

            for trade in history:

                if trade['timestamp'] < cutoff or not trade['is_smart_money']:

                    continue

                if trade['direction'] == 'buy':

                    smart_buy += trade['volume']

                else:

                    smart_sell += trade['volume']

            net = smart_buy - smart_sell

            total = smart_buy + smart_sell

            if total > 0 and net / total > MicrostructureConfig.LARGE_ORDER_RATIO_THRESHOLD:

                signal = 'strong_buy'

            elif total > 0 and net / total > 0.1:

                signal = 'buy'

            elif total > 0 and net / total < -MicrostructureConfig.LARGE_ORDER_RATIO_THRESHOLD:

                signal = 'strong_sell'

            elif total > 0 and net / total < -0.1:

                signal = 'sell'

            else:

                signal = 'neutral'

            return {'smart_buy': smart_buy, 'smart_sell': smart_sell, 'net_flow': net, 'signal': signal}



    def _calc_volume_weight(self, volume: int) -> float:

        if volume >= self._smart_money_threshold:

            return math.log(volume + 1) / math.log(self._smart_money_threshold + 1) * MicrostructureConfig.SMART_MONEY_WEIGHT

        elif volume >= self._large_threshold:

            return math.log(volume + 1) / math.log(self._large_threshold + 1) * MicrostructureConfig.LARGE_ORDER_WEIGHT

        return math.log(volume + 1) / math.log(self._large_threshold + 1) * MicrostructureConfig.VOLUME_NORMALIZATION_FACTOR



    def get_cumulative_flow(self, product: str) -> float:

        with self._lock:

            return self._cumulative_weighted_flow.get(product, 0.0)



    def get_stats(self) -> Dict[str, Any]:

        with self._lock:

            return {

                'service_name': 'VolumeWeightedOrderFlow', **self._stats,

                'tracked_products': len(self._trades),

                'noise_filter_ratio': self._stats['noise_filtered'] / max(1, self._stats['total_trades']),

            }





# R33-P2-6标记: 已集成到OrderFlowBridge生产链路

class LiquidityConsumptionTracker:

    """流动性消耗追踪：实时计算大单对订单簿的消耗速率



    原理：追踪大单成交后订单簿深度的变化源

    计算liquidity_consumption_rate_-1），

    以及trade_size_factor（单笔成交平均成交的倍数）_

    """



    def __init__(self, large_order_threshold: int = 50,

                 depth_lookback: int = 20):

        self._large_threshold = large_order_threshold

        self._depth_lookback = depth_lookback

        self._recent_sizes: Deque[float] = deque(maxlen=depth_lookback)

        self._recent_depth_consumed: Deque[float] = deque(maxlen=depth_lookback)

        self._stats = {'total_trades': 0, 'large_trades': 0, 'total_consumption': 0.0}



    def on_trade(self, volume: float, depth_before: float = 0.0,

                 depth_after: float = 0.0) -> Dict[str, float]:

        self._recent_sizes.append(volume)

        self._stats['total_trades'] += 1



        avg_size = sum(self._recent_sizes) / max(1, len(self._recent_sizes))

        trade_size_factor = volume / max(avg_size, 1.0)



        consumption = 0.0

        if depth_before > 0:

            consumed = max(0.0, depth_before - depth_after)

            consumption = min(1.0, consumed / depth_before)

            self._recent_depth_consumed.append(consumption)

            self._stats['total_consumption'] += consumption



        is_large = volume >= self._large_threshold

        if is_large:

            self._stats['large_trades'] += 1



        return {

            'liquidity_consumption': round(consumption, 4),

            'trade_size_factor': round(trade_size_factor, 4),

            'is_large_order': is_large,

        }



    def get_avg_consumption(self) -> float:

        if not self._recent_depth_consumed:

            return 0.0

        return sum(self._recent_depth_consumed) / len(self._recent_depth_consumed)



    def get_stats(self) -> Dict[str, Any]:

        return {

            'service_name': 'LiquidityConsumptionTracker', **self._stats,

            'avg_consumption': round(self.get_avg_consumption(), 4),

        }

