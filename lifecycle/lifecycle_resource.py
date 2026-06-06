"""
生命周期资源层 — 从strategy_lifecycle_mixin.py拆分
职责: 资源所有权、线程池、调度器生命周期管理、清理回调注册与执行
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Optional


class LifecycleResource:
    def __init__(self, provider):
        self._provider = provider
        self._owned_resources: Dict[str, Any] = {}
        self._cleanup_callbacks: List[Callable] = []
        self._resource_lock = threading.Lock()
        self._thread_pools: Dict[str, Any] = {}
        self._scheduler: Optional[Any] = None

    def register_resource(self, name: str, resource: Any) -> None:
        with self._resource_lock:
            self._owned_resources[name] = resource

    def release_resource(self, name: str) -> Optional[Any]:
        with self._resource_lock:
            resource = self._owned_resources.pop(name, None)
            if resource is not None:
                cleanup = getattr(resource, 'shutdown', None) or getattr(resource, 'close', None)
                if callable(cleanup):
                    try:
                        cleanup()
                    except Exception as e:
                        logging.warning("[LifecycleResource] 资源清理失败: %s err=%s", name, e)
            return resource

    def register_cleanup(self, callback: Callable) -> None:
        self._cleanup_callbacks.append(callback)

    def cleanup_all(self, level: str = 'normal') -> None:
        """清理所有注册的资源

        Args:
            level: 'normal' — 正常停止，保留可恢复资源（仅shutdown）
                   'final'  — 最终销毁，释放所有资源（shutdown + close + clear）
        """
        errors = []
        for callback in reversed(self._cleanup_callbacks):
            try:
                callback()
            except Exception as e:
                errors.append(str(e))
                logging.warning("[LifecycleResource] 清理回调失败: %s", e)

        with self._resource_lock:
            for name, resource in list(self._owned_resources.items()):
                # normal级别只shutdown，final级别额外close
                cleanup = getattr(resource, 'shutdown', None)
                if level == 'final':
                    cleanup = cleanup or getattr(resource, 'close', None)
                if callable(cleanup):
                    try:
                        cleanup()
                    except Exception as e:
                        errors.append(str(e))
                        logging.warning("[LifecycleResource] 资源清理失败: %s err=%s", name, e)

            # final级别清空所有资源引用
            if level == 'final':
                self._cleanup_callbacks.clear()
                self._owned_resources.clear()

        if errors:
            logging.warning("[LifecycleResource] cleanup_all(level=%s)完成但有%d个错误", level, len(errors))

    @property
    def owned_resource_names(self) -> List[str]:
        with self._resource_lock:
            return list(self._owned_resources.keys())

    def register_thread_pool(self, name: str, pool: Any) -> None:
        self._thread_pools[name] = pool
        self.register_resource(f'thread_pool_{name}', pool)

    def shutdown_thread_pools(self, wait: bool = True, timeout: float = 10.0) -> None:
        for name, pool in list(self._thread_pools.items()):
            shutdown = getattr(pool, 'shutdown', None)
            if callable(shutdown):
                try:
                    shutdown(wait=wait, timeout=timeout if wait else None)
                except Exception as e:
                    logging.warning("[LifecycleResource] 线程池%s关闭失败: %s", name, e)
        self._thread_pools.clear()

    def register_scheduler(self, scheduler: Any) -> None:
        self._scheduler = scheduler
        self.register_resource('scheduler', scheduler)

    def stop_scheduler(self) -> None:
        if self._scheduler is not None:
            stop = getattr(self._scheduler, 'stop', None) or getattr(self._scheduler, 'shutdown', None)
            if callable(stop):
                try:
                    stop()
                except Exception as e:
                    logging.warning("[LifecycleResource] 调度器停止失败: %s", e)
            self._scheduler = None

    def get_resource(self, name: str) -> Optional[Any]:
        with self._resource_lock:
            return self._owned_resources.get(name)
