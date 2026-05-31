"""
quant_infra.py - 量化系统基础设施原语

包含：
- rate_limit_log: 日志速率限制
- NumpyRingBuffer: numpy环形缓冲区（替代deque，原子迭代）
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List

import numpy as np


_last_log: Dict[str, float] = {}
_last_log_lock = threading.Lock()


def rate_limit_log(logger: logging.Logger, level: int, msg: str,
                   key: str, min_interval: float = 60.0) -> None:
    now = time.monotonic()
    with _last_log_lock:
        last = _last_log.get(key, 0.0)
        if now - last >= min_interval:
            _last_log[key] = now
            logger.log(level, msg)


class NumpyRingBuffer:
    """
    numpy环形缓冲区，替代collections.deque。

    解决问题：deque迭代期间非原子，多线程可能读到半更新状态。
    方案：numpy固定数组+原子head指针，snapshot读取保证一致性。
    """

    __slots__ = ('_lock', '_buffer', '_capacity', '_head', '_count', '_dtype')

    def __init__(self, capacity: int, dtype: np.dtype = np.float64):
        self._lock = threading.RLock()
        self._capacity = capacity
        self._buffer = np.zeros(capacity, dtype=dtype)
        self._head = 0
        self._count = 0
        self._dtype = dtype

    def append(self, value: float) -> None:
        with self._lock:
            self._buffer[self._head] = value
            self._head = (self._head + 1) % self._capacity
            if self._count < self._capacity:
                self._count += 1

    def snapshot(self) -> np.ndarray:
        with self._lock:
            if self._count < self._capacity:
                return self._buffer[:self._count].copy()
            return np.concatenate([
                self._buffer[self._head:],
                self._buffer[:self._head],
            ]).copy()

    @property
    def count(self) -> int:
        return self._count

    def __len__(self) -> int:
        return self._count

    def sum(self) -> float:
        with self._lock:
            return float(np.sum(self._buffer[:self._count]))

    def mean(self) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            return float(np.mean(self._buffer[:self._count]))

    def std(self) -> float:
        with self._lock:
            if self._count < 2:
                return 0.0
            return float(np.std(self._buffer[:self._count]))

    def last(self) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            idx = (self._head - 1) % self._capacity
            return float(self._buffer[idx])

    def sorted_values(self) -> np.ndarray:
        with self._lock:
            return np.sort(self._buffer[:self._count])

    def percentile(self, p: float) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            sorted_arr = np.sort(self._buffer[:self._count])
            idx = min(int(self._count * p / 100.0), self._count - 1)
            return float(sorted_arr[idx])

    def to_list(self) -> List[float]:
        return self.snapshot().tolist()
