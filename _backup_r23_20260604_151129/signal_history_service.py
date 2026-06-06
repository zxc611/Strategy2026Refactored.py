"""
信号历史服务 — 从signal_service.py拆分
职责: 信号历史记录、统计、查询
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
from collections import deque


class SignalHistoryService:
    def __init__(self, max_history: int = 10000):
        self._history: deque = deque(maxlen=max_history)
        self._signal_counts: Dict[str, int] = {}
        self._rejected_counts: Dict[str, int] = {}

    def record_signal(self, signal: Dict[str, Any]) -> None:
        signal['_record_time'] = time.time()
        self._history.append(signal)
        reason = signal.get('reason', 'unknown')
        self._signal_counts[reason] = self._signal_counts.get(reason, 0) + 1

    def record_rejection(self, reason: str) -> None:
        self._rejected_counts[reason] = self._rejected_counts.get(reason, 0) + 1

    def get_recent(self, n: int = 100) -> List[Dict[str, Any]]:
        return list(self._history)[-n:]

    def get_statistics(self) -> Dict[str, Any]:
        return {
            'total_signals': len(self._history),
            'signal_counts': dict(self._signal_counts),
            'rejected_counts': dict(self._rejected_counts),
        }

    def clear(self) -> None:
        self._history.clear()
        self._signal_counts.clear()
        self._rejected_counts.clear()