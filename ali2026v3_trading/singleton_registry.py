"""Singleton namespace isolation — R19-AP-P1-06 fix.

Provides a centralized registry for module-level singletons with namespace
isolation, replacing scattered _instance variables across 10+ files.

Usage:
    from ali2026v3_trading.singleton_registry import SingletonRegistry

    # Register a singleton
    registry = SingletonRegistry.get_registry('risk_service')
    registry.set('risk_service_instance', my_instance)

    # Retrieve a singleton
    instance = registry.get('risk_service_instance')

    # Reset all singletons (for testing)
    SingletonRegistry.reset_all()
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Dict, List, Optional


class _Namespace:
    """Thread-safe namespace for a group of related singletons."""

    def __init__(self, name: str):
        self._name = name
        self._store: Dict[str, Any] = {}
        self._cleanup_fns: Dict[str, Callable] = {}  # R21-MEM-P1-01修复: cleanup回调注册表
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._store.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._store[key] = value

    def has(self, key: str) -> bool:
        with self._lock:
            return key in self._store

    def remove(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def reset(self) -> None:
        with self._lock:
            self._store.clear()
            self._cleanup_fns.clear()  # R21-MEM-P1-01修复: 重置时清理回调

    def keys(self) -> List[str]:  # [R22-P2-TS14]
        with self._lock:
            return list(self._store.keys())


class SingletonRegistry:
    """Centralized singleton registry with namespace isolation.

    Each module gets its own namespace, preventing cross-module singleton
    name collisions and enabling fine-grained test resets.
    """
    _namespaces: Dict[str, _Namespace] = {}
    _lock = threading.RLock()

    @classmethod
    def get_registry(cls, namespace: str) -> _Namespace:
        """Get or create a namespace for a module."""
        with cls._lock:
            if namespace not in cls._namespaces:
                cls._namespaces[namespace] = _Namespace(namespace)
            return cls._namespaces[namespace]

    @classmethod
    def reset_all(cls) -> None:
        """Reset all singleton namespaces (for testing)."""
        with cls._lock:
            for ns in cls._namespaces.values():
                ns.reset()

    @classmethod
    def reset_namespace(cls, namespace: str) -> None:
        """Reset a specific namespace."""
        with cls._lock:
            if namespace in cls._namespaces:
                cls._namespaces[namespace].reset()

    @classmethod
    def list_namespaces(cls) -> List[str]:  # [R22-P2-TS15]
        """List all registered namespace names."""
        with cls._lock:
            return list(cls._namespaces.keys())

    @classmethod
    def total_singleton_count(cls) -> int:
        """Count total singletons across all namespaces."""
        with cls._lock:
            return sum(len(ns.keys()) for ns in cls._namespaces.values())

    # R21-MEM-P1-01修复: 注册带cleanup回调的单例
    @classmethod
    def register_singleton(cls, name: str, instance: Any, cleanup_fn: Optional[Callable] = None) -> None:
        """注册单例实例，可选附带cleanup回调用于资源释放。

        Args:
            name: 单例名称（格式: 'namespace.key'，如 'risk_service.instance'）
            instance: 单例实例
            cleanup_fn: 可选清理回调，在cleanup_all_singletons()时调用
        """
        parts = name.split('.', 1)
        namespace = parts[0] if len(parts) > 1 else 'default'
        key = parts[1] if len(parts) > 1 else name
        with cls._lock:
            ns = cls.get_registry(namespace)
            with ns._lock:
                ns._store[key] = instance
                if cleanup_fn is not None:
                    ns._cleanup_fns[key] = cleanup_fn

    # R21-MEM-P1-01修复: 全局清理所有单例（调用cleanup回调后释放引用）
    @classmethod
    def cleanup_all_singletons(cls) -> None:
        """调用所有已注册的cleanup回调，然后清空所有命名空间。

        适用于策略生命周期结束时释放大对象（如DataFrame、线程池等）。
        """
        with cls._lock:
            for ns in cls._namespaces.values():
                with ns._lock:
                    # 先调用所有cleanup回调
                    for key, cleanup_fn in ns._cleanup_fns.items():
                        try:
                            cleanup_fn()
                        except Exception as e:
                            import logging
                            logging.warning(f"[SingletonRegistry] cleanup failed for {key}: {e}")
                    ns._cleanup_fns.clear()
                    ns._store.clear()

# R21-MEM-P2-09修复: 对象池基础框架注释
# 当前SingletonRegistry仅管理单例的注册/查找/清理，缺少对象池(Object Pool)功能：
# 1. 对象池应支持对象复用（acquire/release），而非仅单例注册
# 2. 适用于高频创建/销毁的临时对象（如回测结果dict、交易记录NamedTuple）
# 3. 池化可减少GC压力和内存分配开销
# 4. 建议实现: ObjectPool<T> with acquire() -> T, release(T), max_size, idle_timeout
# 5. 可与SingletonRegistry共享命名空间隔离机制
# 示例框架:
# class ObjectPool:
#     def __init__(self, factory, max_size=64, idle_timeout=300):
#         self._factory = factory  # 对象工厂函数
#         self._pool: List[Any] = []  # 空闲对象列表
#         self._in_use: Set[int] = set()  # 使用中对象id集合
#         self._max_size = max_size
#         self._idle_timeout = idle_timeout
#     def acquire(self) -> Any:
#         if self._pool:
#             return self._pool.pop()
#         return self._factory()
#     def release(self, obj) -> None:
#         if len(self._pool) < self._max_size:
#             self._pool.append(obj)
#         # else: 丢弃，让GC回收
