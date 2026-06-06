"""
生命周期资源层 — 从strategy_lifecycle_mixin.py拆分
职责: 资源所有权、线程池、调度器生命周期管理
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional


class LifecycleResource:
    def __init__(self, provider):
        self._provider = provider
        self._owned_resources: Dict[str, Any] = {}
        self._cleanup_callbacks: List[Callable] = []

    def register_resource(self, name: str, resource: Any) -> None:
        self._owned_resources[name] = resource

    def release_resource(self, name: str) -> Optional[Any]:
        return self._owned_resources.pop(name, None)

    def register_cleanup(self, callback: Callable) -> None:
        self._cleanup_callbacks.append(callback)

    def cleanup_all(self) -> None:
        for cb in reversed(self._cleanup_callbacks):
            try:
                cb()
            except Exception as e:
                logging.warning("[LifecycleResource] 清理回调失败: %s", e)
        self._cleanup_callbacks.clear()
        self._owned_resources.clear()

    @property
    def owned_resource_names(self) -> List[str]:
        return list(self._owned_resources.keys())