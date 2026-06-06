"""
Tick聚合器 — 从preprocess_ticks.py拆分
职责: Tick→分钟Bar聚合、成交量加权价格计算
"""
from __future__ import annotations

import logging
import numpy as np
from typing import Any, Dict, List, Optional


class TickAggregator:
    def __init__(self, bar_interval_sec: int = 60):
        self._bar_interval_sec = bar_interval_sec
        self._current_bar: Optional[Dict[str, Any]] = None
        self._bar_start_time: Optional[float] = None

    def reset(self) -> None:
        self._current_bar = None
        self._bar_start_time = None

    def update(self, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tick_time = tick.get('timestamp', 0)
        price = tick.get('price', 0.0)
        volume = tick.get('volume', 0)

        if self._bar_start_time is None:
            self._bar_start_time = tick_time - (tick_time % self._bar_interval_sec)
            self._current_bar = {
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': volume, 'tick_count': 1,
                'vwap_num': price * volume, 'vwap_den': volume,
                'start_time': self._bar_start_time,
            }
            return None

        bar_end = self._bar_start_time + self._bar_interval_sec
        if tick_time >= bar_end:
            completed_bar = self._finalize_bar()
            self._bar_start_time = tick_time - (tick_time % self._bar_interval_sec)
            self._current_bar = {
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': volume, 'tick_count': 1,
                'vwap_num': price * volume, 'vwap_den': volume,
                'start_time': self._bar_start_time,
            }
            return completed_bar

        bar = self._current_bar
        bar['high'] = max(bar['high'], price)
        bar['low'] = min(bar['low'], price)
        bar['close'] = price
        bar['volume'] += volume
        bar['tick_count'] += 1
        bar['vwap_num'] += price * volume
        bar['vwap_den'] += volume
        return None

    def _finalize_bar(self) -> Dict[str, Any]:
        bar = self._current_bar
        if bar is None:
            return {}
        bar['vwap'] = bar['vwap_num'] / bar['vwap_den'] if bar['vwap_den'] > 0 else bar['close']
        bar.pop('vwap_num', None)
        bar.pop('vwap_den', None)
        return bar

    def flush(self) -> Optional[Dict[str, Any]]:
        if self._current_bar is not None:
            return self._finalize_bar()
        return None