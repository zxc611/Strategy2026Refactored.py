"""
infra/concurrent_utils.py — 并发工具+事件发布 合并模块

合并自:
  - concurrent_utils.py (线程池拒绝策略 R16-P1-RES-05)
  - event_publisher.py (事件发布器 CC-02+CC-04)
"""
from __future__ import annotations

import logging
import queue
from enum import Enum
from typing import Any, Callable, Dict, Optional


# ============================================================
# Section 1: 线程池拒绝策略 (原 concurrent_utils.py)
# ============================================================

class RejectionPolicy(Enum):
    ABORT = 'abort'
    DROP = 'drop'
    CALLER_RUNS = 'caller_runs'
    DISCARD_OLDEST = 'discard_oldest'


class ThreadPoolExecutorWithPolicy:
    def __init__(self, executor, task_queue: queue.Queue,
                 rejection_policy: RejectionPolicy = RejectionPolicy.ABORT,
                 name: str = 'ThreadPool'):
        self._executor = executor
        self._queue = task_queue
        self._policy = rejection_policy
        self._name = name
        self._drop_count = 0
        self._caller_runs_count = 0

    def submit(self, fn: Callable, *args, **kwargs) -> Any:
        try:
            return self._executor.submit(fn, *args, **kwargs)
        except Exception as e:
            if 'queue.Full' in str(type(e).__name__) or isinstance(e, queue.Full):
                return self._handle_rejection(fn, args, kwargs)
            raise

    def submit_to_queue(self, item: Any) -> bool:
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            return self._handle_queue_rejection(item)

    def _handle_rejection(self, fn: Callable, args: tuple, kwargs: dict) -> Any:
        if self._policy == RejectionPolicy.ABORT:
            logging.error("[R16-P1-RES-05] %s 线程池满，ABORT策略抛出异常", self._name)
            raise RuntimeError(f"{self._name} thread pool queue full, task rejected (ABORT)")

        elif self._policy == RejectionPolicy.DROP:
            self._drop_count += 1
            if self._drop_count % 100 == 1:
                logging.warning("[R16-P1-RES-05] %s 线程池满，DROP策略丢弃任务(累计%d)",
                              self._name, self._drop_count)
            return None

        elif self._policy == RejectionPolicy.CALLER_RUNS:
            self._caller_runs_count += 1
            if self._caller_runs_count <= 10 or self._caller_runs_count % 100 == 0:
                logging.info("[R16-P1-RES-05] %s 线程池满，CALLER_RUNS策略调用者执行(累计%d)",
                           self._name, self._caller_runs_count)
            return fn(*args, **kwargs)

        elif self._policy == RejectionPolicy.DISCARD_OLDEST:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait((fn, args, kwargs))
                return True
            except Exception:
                return False

        return None

    def _handle_queue_rejection(self, item: Any) -> bool:
        if self._policy == RejectionPolicy.ABORT:
            raise RuntimeError(f"{self._name} queue full, item rejected (ABORT)")

        elif self._policy == RejectionPolicy.DROP:
            self._drop_count += 1
            return False

        elif self._policy == RejectionPolicy.DISCARD_OLDEST:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(item)
                return True
            except Exception:
                return False

        return False

    def get_stats(self) -> dict:
        return {
            'drop_count': self._drop_count,
            'caller_runs_count': self._caller_runs_count,
            'policy': self._policy.value,
        }


# ============================================================
# Section 2: 事件发布器 (原 event_publisher.py)
# ============================================================

class EventPublisher:
    def __init__(self, provider: Any = None):
        self._provider = provider

    def publish(self, event_type: str, data: Dict[str, Any],
                event_bus: Any = None, strategy_id: str = '') -> None:
        run_id = 'N/A'
        if self._provider:
            run_id = getattr(self._provider, '_lifecycle_run_id', 'N/A')
            if not strategy_id:
                strategy_id = getattr(self._provider, 'strategy_id', '')
        bus = event_bus
        if bus is None and self._provider:
            bus = getattr(self._provider, '_event_bus', None)
        if bus:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'strategy_id': strategy_id,
                    'run_id': run_id,
                    'source_type': 'event-tail',
                    **data
                })()
                bus.publish(event, async_mode=True)
            except (AttributeError, RuntimeError, ValueError, TypeError, KeyError) as e:
                logging.debug(
                    "[EventPublisher][strategy_id=%s][run_id=%s] "
                    "Failed to publish %s: %s",
                    strategy_id, run_id, event_type, e
                )


__all__ = [
    'RejectionPolicy', 'ThreadPoolExecutorWithPolicy',
    'EventPublisher',
]
