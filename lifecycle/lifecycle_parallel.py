"""
生命周期并行层 — 从strategy_lifecycle_mixin.py拆分
职责: 并行运行模式、结果比较、A/B测试、影子策略管理
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


class LifecycleParallel:
    def __init__(self, provider):
        self._provider = provider
        self._parallel_mode = False
        self._parallel_results: Dict[str, Any] = {}
        self._parallel_lock = threading.Lock()
        self._shadow_strategy: Optional[Any] = None
        self._parallel_start_time: float = 0.0
        self._parallel_duration_sec: float = 0.0
        self._promotion_result: Optional[Dict[str, Any]] = None

    def enter_parallel_running(self, shadow_strategy: Any = None,
                               duration_sec: float = 3600.0) -> None:
        with self._parallel_lock:
            self._parallel_mode = True
            self._parallel_results.clear()
            self._shadow_strategy = shadow_strategy
            self._parallel_start_time = time.time()
            self._parallel_duration_sec = duration_sec
            self._promotion_result = None
        logging.info("[LifecycleParallel] 进入并行模式, duration=%.0fs", duration_sec)

    def exit_parallel_running(self, promote_new: bool = False) -> bool:
        with self._parallel_lock:
            was_parallel = self._parallel_mode
            self._parallel_mode = False
            shadow = self._shadow_strategy
            self._shadow_strategy = None
            if was_parallel and promote_new and shadow is not None:
                self._promotion_result = {
                    'promoted': True,
                    'shadow_strategy': shadow,
                    'timestamp': time.time(),
                }
                logging.info("[LifecycleParallel] 退出并行模式, promote_new=True")
                return True
        if was_parallel:
            logging.info("[LifecycleParallel] 退出并行模式, promote_new=False")
        return False

    def record_parallel_result(self, key: str, result: Any) -> None:
        with self._parallel_lock:
            self._parallel_results[key] = result

    def compare_parallel_results(self) -> Dict[str, Any]:
        with self._parallel_lock:
            results = dict(self._parallel_results)
        if len(results) < 2:
            return {'comparable': False, 'reason': 'insufficient_results', 'count': len(results)}
        keys = list(results.keys())
        comparison = {'comparable': True, 'keys': keys, 'count': len(results)}
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                k1, k2 = keys[i], keys[j]
                v1, v2 = results[k1], results[k2]
                if isinstance(v1, dict) and isinstance(v2, dict):
                    diff_keys = set(v1.keys()) ^ set(v2.keys())
                    common_keys = set(v1.keys()) & set(v2.keys())
                    value_diffs = {}
                    for ck in common_keys:
                        if isinstance(v1[ck], (int, float)) and isinstance(v2[ck], (int, float)):
                            value_diffs[ck] = v2[ck] - v1[ck]
                    comparison[f'{k1}_vs_{k2}'] = {
                        'key_diff': list(diff_keys),
                        'value_diffs': value_diffs,
                    }
        return comparison

    @property
    def is_parallel_running(self) -> bool:
        return self._parallel_mode

    @property
    def parallel_elapsed_sec(self) -> float:
        if not self._parallel_mode:
            return 0.0
        return time.time() - self._parallel_start_time

    @property
    def should_exit_parallel(self) -> bool:
        if not self._parallel_mode:
            return False
        return self.parallel_elapsed_sec >= self._parallel_duration_sec

    @property
    def promotion_result(self) -> Optional[Dict[str, Any]]:
        return self._promotion_result

    @property
    def shadow_strategy(self) -> Optional[Any]:
        with self._parallel_lock:
            return self._shadow_strategy
