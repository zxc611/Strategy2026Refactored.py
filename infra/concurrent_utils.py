# [M1-76] 线程池拒绝策略
"""
R16-P1-RES-05修复: 线程池拒绝策略枚举
"""
from enum import Enum
from typing import Callable, Any
import logging
import queue

__all__ = [
    'RejectionPolicy', 'ThreadPoolExecutorWithPolicy',
]


class RejectionPolicy(Enum):
    """线程池任务队列满时的拒绝策略"""
    ABORT = 'abort'  # 抛出异常
    DROP = 'drop'  # 静默丢弃
    CALLER_RUNS = 'caller_runs'  # 调用者线程执行
    DISCARD_OLDEST = 'discard_oldest'  # 丢弃最旧任务


class ThreadPoolExecutorWithPolicy:
    """带拒绝策略的线程池包装器
    
    R16-P1-RES-05修复: 为queue.Full提供显式拒绝策略
    """
    
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
        """提交任务，队列满时按拒绝策略处理"""
        try:
            return self._executor.submit(fn, *args, **kwargs)
        except Exception as e:
            if 'queue.Full' in str(type(e).__name__) or isinstance(e, queue.Full):
                return self._handle_rejection(fn, args, kwargs)
            raise
    
    def submit_to_queue(self, item: Any) -> bool:
        """直接提交到队列，满时按策略处理"""
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            return self._handle_queue_rejection(item)
    
    def _handle_rejection(self, fn: Callable, args: tuple, kwargs: dict) -> Any:
        """处理线程池拒绝"""
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
        """处理队列拒绝"""
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
        """获取拒绝统计"""
        return {
            'drop_count': self._drop_count,
            'caller_runs_count': self._caller_runs_count,
            'policy': self._policy.value,
        }
