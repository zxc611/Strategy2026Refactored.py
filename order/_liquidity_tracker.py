# MODULE_ID: M1-137
"""
_liquidity_tracker.py - 流动性消耗追踪器

拆分自 order_flow_analyzer.py (2026-06-30)
职责：追踪流动性消耗，统计平均消耗率
"""

from __future__ import annotations
import time
import threading
from collections import deque
from typing import Dict, Any

__all__ = ['LiquidityConsumptionTracker']


class LiquidityConsumptionTracker:
    """流动性消耗追踪器
    
    追踪成交对流动性的消耗情况，用于评估市场深度质量
    """
    
    def __init__(self, max_history: int = 1000):
        self._max_history = max_history
        self._consumptions: Dict[str, deque] = {}
        self._lock = threading.Lock()
        self._stats = {
            'total_trades': 0,
            'total_consumption': 0.0,
        }
    
    def on_trade(self, instrument_id: str, price: float, volume: int,
                 bid_price: float = 0.0, ask_price: float = 0.0) -> None:
        """成交事件输入
        
        Args:
            instrument_id: 合约ID
            price: 成交价格
            volume: 成交量
            bid_price: 买一价
            ask_price: 卖一价
        """
        with self._lock:
            if instrument_id not in self._consumptions:
                self._consumptions[instrument_id] = deque(maxlen=self._max_history)
            
            # 计算流动性消耗（成交价距离中间价的距离）
            if bid_price > 0 and ask_price > 0:
                mid_price = (bid_price + ask_price) / 2.0
                consumption = abs(price - mid_price) / mid_price if mid_price > 0 else 0.0
                self._consumptions[instrument_id].append({
                    'timestamp': time.time(),
                    'consumption': consumption,
                    'volume': volume,
                })
                self._stats['total_trades'] += 1
                self._stats['total_consumption'] += consumption
    
    def get_avg_consumption(self, instrument_id: str, lookback_seconds: float = 60.0) -> float:
        """获取平均流动性消耗
        
        Args:
            instrument_id: 合约ID
            lookback_seconds: 回看时间（秒）
        
        Returns:
            float: 平均消耗率
        """
        with self._lock:
            if instrument_id not in self._consumptions:
                return 0.0
            
            now = time.time()
            cutoff = now - lookback_seconds
            
            consumptions = [
                item['consumption']
                for item in self._consumptions[instrument_id]
                if item['timestamp'] >= cutoff
            ]
            
            if not consumptions:
                return 0.0
            
            return sum(consumptions) / len(consumptions)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                **self._stats,
                'num_instruments': len(self._consumptions),
            }
    
    def clear(self) -> None:
        """清空所有数据"""
        with self._lock:
            self._consumptions.clear()
            self._stats = {
                'total_trades': 0,
                'total_consumption': 0.0,
            }