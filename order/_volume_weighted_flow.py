# MODULE_ID: M1-138
"""
_volume_weighted_flow.py - 成交量加权订单流分析

拆分自 order_flow_analyzer.py (2026-06-30)
职责：成交量加权订单流分析，聪明钱流向识别
"""

from __future__ import annotations
import time
import threading
from collections import deque
from typing import Dict, List, Any, Optional

__all__ = ['VolumeWeightedOrderFlow']


class VolumeWeightedOrderFlow:
    """成交量加权订单流分析器
    
    基于成交量加权计算订单流不平衡，识别聪明钱流向
    """
    
    def __init__(self, max_history: int = 10000):
        self._max_history = max_history
        self._flows: Dict[str, deque] = {}
        self._cumulative: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._stats = {
            'total_trades': 0,
            'total_buy_volume': 0,
            'total_sell_volume': 0,
        }
    
    def on_trade(self, instrument_id: str, price: float, volume: int,
                 direction: str = 'BUY') -> None:
        """成交事件输入
        
        Args:
            instrument_id: 合约ID
            price: 成交价格
            volume: 成交量
            direction: 方向（BUY/SELL）
        """
        with self._lock:
            if instrument_id not in self._flows:
                self._flows[instrument_id] = deque(maxlen=self._max_history)
                self._cumulative[instrument_id] = 0.0
            
            # 计算成交量权重
            weight = self._calc_volume_weight(volume)
            
            # 计算流向（买入为正，卖出为负）
            flow = weight * volume if direction == 'BUY' else -weight * volume
            
            self._flows[instrument_id].append({
                'timestamp': time.time(),
                'price': price,
                'volume': volume,
                'direction': direction,
                'weight': weight,
                'flow': flow,
            })
            
            self._cumulative[instrument_id] += flow
            self._stats['total_trades'] += 1
            if direction == 'BUY':
                self._stats['total_buy_volume'] += volume
            else:
                self._stats['total_sell_volume'] += volume
    
    def calc_volume_weighted_imbalance(self, instrument_id: str,
                                       lookback_seconds: float = 60.0) -> float:
        """计算成交量加权不平衡
        
        Args:
            instrument_id: 合约ID
            lookback_seconds: 回看时间（秒）
        
        Returns:
            float: 不平衡度（-1到1，正值为买方主导）
        """
        with self._lock:
            if instrument_id not in self._flows:
                return 0.0
            
            now = time.time()
            cutoff = now - lookback_seconds
            
            flows = [
                item['flow']
                for item in self._flows[instrument_id]
                if item['timestamp'] >= cutoff
            ]
            
            if not flows:
                return 0.0
            
            total_flow = sum(flows)
            total_volume = sum(abs(f) for f in flows)
            
            if total_volume == 0:
                return 0.0
            
            return total_flow / total_volume
    
    def calc_smart_money_flow(self, instrument_id: str,
                              lookback_seconds: float = 60.0,
                              large_volume_threshold: int = 100) -> float:
        """计算聪明钱流向
        
        基于大单识别聪明钱流向
        
        Args:
            instrument_id: 合约ID
            lookback_seconds: 回看时间（秒）
            large_volume_threshold: 大单阈值
        
        Returns:
            float: 聪明钱流向（-1到1）
        """
        with self._lock:
            if instrument_id not in self._flows:
                return 0.0
            
            now = time.time()
            cutoff = now - lookback_seconds
            
            large_flows = [
                item['flow']
                for item in self._flows[instrument_id]
                if item['timestamp'] >= cutoff and item['volume'] >= large_volume_threshold
            ]
            
            if not large_flows:
                return 0.0
            
            total_flow = sum(large_flows)
            total_volume = sum(abs(f) for f in large_flows)
            
            if total_volume == 0:
                return 0.0
            
            return total_flow / total_volume
    
    def _calc_volume_weight(self, volume: int) -> float:
        """计算成交量权重
        
        大单权重更高
        """
        if volume <= 0:
            return 0.0
        
        # 使用对数权重
        import math
        return 1.0 + math.log10(max(volume, 1))
    
    def get_cumulative_flow(self, instrument_id: str) -> float:
        """获取累计流向"""
        with self._lock:
            return self._cumulative.get(instrument_id, 0.0)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                **self._stats,
                'num_instruments': len(self._flows),
            }
    
    def clear(self) -> None:
        """清空所有数据"""
        with self._lock:
            self._flows.clear()
            self._cumulative.clear()
            self._stats = {
                'total_trades': 0,
                'total_buy_volume': 0,
                'total_sell_volume': 0,
            }