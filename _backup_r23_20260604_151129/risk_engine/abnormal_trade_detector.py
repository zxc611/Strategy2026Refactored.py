from __future__ import annotations

import time
import logging
from typing import Dict, Any
from collections import deque

class AbnormalTradeDetector:
    def __init__(self):
        self._trade_history: deque = deque(maxlen=1000)
        self._cancel_counts: Dict[str, int] = {}
        self._burst_window_sec: float = 60.0
        self._burst_threshold: int = 20
        self._cancel_rate_threshold: float = 0.8
        self._self_trade_pairs: Dict[str, float] = {}

    def record_trade(self, instrument_id: str, direction: str, volume: float, price: float, timestamp: float = None) -> None:
        ts = timestamp or time.time()
        self._trade_history.append({
            'instrument_id': instrument_id,
            'direction': direction,
            'volume': volume,
            'price': price,
            'timestamp': ts,
        })

    def detect_burst_trading(self, instrument_id: str = '', window_sec: float = None) -> Dict[str, Any]:
        _window = window_sec or self._burst_window_sec
        now = time.time()
        cutoff = now - _window
        recent = [t for t in self._trade_history if t['timestamp'] >= cutoff]
        if instrument_id:
            recent = [t for t in recent if t['instrument_id'] == instrument_id]
        count = len(recent)
        is_burst = count >= self._burst_threshold
        return {
            'is_anomaly': is_burst,
            'anomaly_type': 'burst_trading',
            'trade_count': count,
            'window_sec': _window,
            'threshold': self._burst_threshold,
            'action': 'block' if is_burst else 'none',
        }

    def detect_self_trade(self, instrument_id: str, direction: str, price: float, tolerance_pct: float = 0.001) -> Dict[str, Any]:
        now = time.time()
        opposite = 'SELL' if direction == 'BUY' else 'BUY'
        for t in self._trade_history:
            if (t['instrument_id'] == instrument_id and
                t['direction'] == opposite and
                abs(t['price'] - price) / max(price, 1e-10) < tolerance_pct and
                now - t['timestamp'] < 5.0):
                return {
                    'is_anomaly': True,
                    'anomaly_type': 'self_trade',
                    'instrument_id': instrument_id,
                    'action': 'block',
                }
        return {'is_anomaly': False, 'anomaly_type': 'self_trade', 'action': 'none'}

    def detect_price_deviation(self, instrument_id: str, order_price: float, market_price: float, threshold_pct: float = 0.02) -> Dict[str, Any]:
        if market_price <= 0:
            return {'is_anomaly': False, 'anomaly_type': 'price_deviation', 'action': 'none'}
        deviation = abs(order_price - market_price) / market_price
        is_anomaly = deviation > threshold_pct
        return {
            'is_anomaly': is_anomaly,
            'anomaly_type': 'price_deviation',
            'deviation_pct': round(deviation * 100, 2),
            'threshold_pct': round(threshold_pct * 100, 2),
            'action': 'block' if is_anomaly else 'none',
        }

    def detect_volume_spike(self, instrument_id: str, volume: float, avg_volume: float, spike_factor: float = 5.0) -> Dict[str, Any]:
        if avg_volume <= 0:
            return {'is_anomaly': False, 'anomaly_type': 'volume_spike', 'action': 'none'}
        ratio = volume / avg_volume
        is_anomaly = ratio > spike_factor
        return {
            'is_anomaly': is_anomaly,
            'anomaly_type': 'volume_spike',
            'volume_ratio': round(ratio, 2),
            'spike_factor': spike_factor,
            'action': 'alert' if is_anomaly else 'none',
        }
