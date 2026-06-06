"""
生命周期并行层 — 从strategy_lifecycle_mixin.py拆分
职责: 并行运行模式、结果比较、A/B测试
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional


class LifecycleParallel:
    def __init__(self, provider):
        self._provider = provider
        self._parallel_mode = False
        self._parallel_results: Dict[str, Any] = {}
        self._parallel_lock = threading.Lock()

    def enter_parallel_running(self) -> None:
        with self._parallel_lock:
            self._parallel_mode = True
            self._parallel_results.clear()

    def exit_parallel_running(self) -> None:
        with self._parallel_lock:
            self._parallel_mode = False

    def record_parallel_result(self, key: str, result: Any) -> None:
        with self._parallel_lock:
            self._parallel_results[key] = result

    def compare_parallel_results(self) -> Dict[str, Any]:
        with self._parallel_lock:
            return dict(self._parallel_results)

    @property
    def is_parallel_running(self) -> bool:
        return self._parallel_mode