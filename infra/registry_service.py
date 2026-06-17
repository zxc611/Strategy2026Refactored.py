# [M1-67] 注册表服务
# MODULE_ID: M1-100
"""registry_service.py - 注册表服务合�?
合并�?callback_registry.py + singleton_registry.py + state_store.py (2026-06-12)
"""
import atexit
import copy
from ali2026v3_trading.infra.logging_utils import get_logger
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

_logger = get_logger(__name__)  # R9-5

__all__ = [
    # Section 1: CallbackRegistry
    'CallbackRegistry', 'CallbackGroup', 'get_callback_group', 'reset_callback_group',
    'ON_TICK', 'ON_KLINE', 'ON_ORDER_UPDATE', 'ON_POSITION_CHANGE', 'ON_RISK_STATE_CHANGE',
    # Section 2: SingletonRegistry
    'SingletonRegistry',
    # Section 3: StateStore
    'StateSnapshot', 'StateStore', 'get_state_store', 'reset_state_store',
]


# ============================================================
# Section 1: CallbackRegistry (from callback_registry.py)
# ============================================================

ON_TICK = 'on_tick'
ON_KLINE = 'on_kline'
ON_ORDER_UPDATE = 'on_order_update'
ON_POSITION_CHANGE = 'on_position_change'
ON_RISK_STATE_CHANGE = 'on_risk_state_change'

_THREAD_POOL = ThreadPoolExecutor(max_workers=4)

_CALLBACK_TO_EVENT_MAP = {
    ON_TICK: 'TickEvent',
    ON_KLINE: 'KLineEvent',
    ON_ORDER_UPDATE: 'OrderEvent',
    ON_POSITION_CHANGE: 'PositionEvent',
    ON_RISK_STATE_CHANGE: 'RiskEvent',
}


def _shutdown_callback_pool():
    try:
        _THREAD_POOL.shutdown(wait=False)
    except (RuntimeError, AttributeError) as _err:
        logging.debug("[callback_registry] 线程操作降级: %s", _err)


atexit.register(_shutdown_callback_pool)

# P2-14修复: 注册到lifecycle_resource统一管理
try:
    from ali2026v3_trading.lifecycle.lifecycle_resource import register_thread_pool
    register_thread_pool('callback_registry_pool', _THREAD_POOL)
except (ImportError, AttributeError) as _err:
    logging.debug("[callback_registry] 属性访问降�? %s", _err)


def _publish_to_event_bus(event_name: str, *args, **kwargs) -> None:
    try:
        from ali2026v3_trading.infra.event_bus import EventBus
        event_bus = EventBus.get_instance()
        event_bus.publish(event_name, {'args': args, 'kwargs': kwargs})
    except (ImportError, AttributeError) as _err:
        logging.debug("[callback_registry] 属性访问降�? %s", _err)


class CallbackRegistry:
    def __init__(self, name: str = ''):
        self._name = name
        self._callbacks: List[Callable] = []
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self._name

    def register(self, callback: Callable) -> Callable:
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

        def _unregister():
            self.unregister(callback)

        return _unregister

    def unregister(self, callback: Callable) -> bool:
        with self._lock:
            try:
                self._callbacks.remove(callback)
                return True
            except ValueError:
                return False

    def invoke(self, *args, **kwargs) -> List[Any]:
        snapshot = self._callbacks[:]
        results = []
        for cb in snapshot:
            try:
                results.append(cb(*args, **kwargs))
            except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, ZeroDivisionError, ArithmeticError) as _err:
                logging.debug("[callback_registry] 未预期异�? %s", _err)
                results.append(None)
        event_name = _CALLBACK_TO_EVENT_MAP.get(self._name, self._name)
        _publish_to_event_bus(event_name, *args, **kwargs)
        return results

    def invoke_async(self, *args, **kwargs) -> None:
        snapshot = self._callbacks[:]
        for cb in snapshot:
            _THREAD_POOL.submit(self._safe_call, cb, args, kwargs)
        event_name = _CALLBACK_TO_EVENT_MAP.get(self._name, self._name)
        _THREAD_POOL.submit(_publish_to_event_bus, event_name, *args, **kwargs)

    @staticmethod
    def _safe_call(cb: Callable, args: tuple, kwargs: dict) -> None:
        try:
            cb(*args, **kwargs)
        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, ZeroDivisionError, ArithmeticError) as _err:
            logging.debug("[callback_registry] 线程操作降级: %s", _err)

    def __len__(self) -> int:
        return len(self._callbacks)

    def __contains__(self, callback: Callable) -> bool:
        return callback in self._callbacks

    def clear(self) -> None:
        with self._lock:
            self._callbacks.clear()

    def get_callbacks(self) -> List[Callable]:
        return self._callbacks[:]


class CallbackGroup:
    def __init__(self):
        self._registries: Dict[str, CallbackRegistry] = {}
        self._lock = threading.Lock()

    def get_registry(self, name: str) -> CallbackRegistry:
        with self._lock:
            if name not in self._registries:
                self._registries[name] = CallbackRegistry(name)
            return self._registries[name]

    def remove_registry(self, name: str) -> bool:
        with self._lock:
            if name in self._registries:
                del self._registries[name]
                return True
            return False

    def invoke_all(self, name: str, *args, **kwargs) -> Dict[str, List[Any]]:
        with self._lock:
            registry = self._registries.get(name)
        if registry is None:
            return {}
        return {name: registry.invoke(*args, **kwargs)}

    def list_registries(self) -> List[str]:
        with self._lock:
            return list(self._registries.keys())


_global_callback_group: Optional[CallbackGroup] = None
_group_lock = threading.Lock()


def get_callback_group() -> CallbackGroup:
    global _global_callback_group
    if _global_callback_group is None:
        with _group_lock:
            if _global_callback_group is None:
                _global_callback_group = CallbackGroup()
    return _global_callback_group


def reset_callback_group() -> None:
    global _global_callback_group
    with _group_lock:
        _global_callback_group = None


# ============================================================
# Section 2: SingletonRegistry (from singleton_registry.py)
# ============================================================

class _Namespace:
    """Thread-safe namespace for a group of related singletons."""

    def __init__(self, name: str):
        self._name = name
        self._store: Dict[str, Any] = {}
        self._cleanup_fns: Dict[str, Callable] = {}  # R21-MEM-P1-01修复: cleanup回调注册�?
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
            self._cleanup_fns.clear()  # R21-MEM-P1-01修复: 重置时清理回�?

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

    # R21-MEM-P1-01修复: 注册带cleanup回调的单�?
    @classmethod
    def register_singleton(cls, name: str, instance: Any, cleanup_fn: Optional[Callable] = None) -> None:
        """注册单例实例，可选附带cleanup回调用于资源释放�?

        Args:
            name: 单例名称（格�? 'namespace.key'，如 'risk_service.instance'�?
            instance: 单例实例
            cleanup_fn: 可选清理回调，在cleanup_all_singletons()时调�?
        """
        # P1-40修复: 同步注册到ServiceContainer统一DI入口
        try:
            from ali2026v3_trading.infra.service_container import ServiceContainer
            ServiceContainer().register(name, instance)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] singleton registry ServiceContainer register suppressed: %s", _r3_err)
            pass
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
        """调用所有已注册的cleanup回调，然后清空所有命名空间�?

        适用于策略生命周期结束时释放大对象（如DataFrame、线程池等）�?
        """
        with cls._lock:
            for ns in cls._namespaces.values():
                with ns._lock:
                    # 先调用所有cleanup回调
                    for key, cleanup_fn in ns._cleanup_fns.items():
                        try:
                            cleanup_fn()
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            import logging
                            logging.warning(f"[SingletonRegistry] cleanup failed for {key}: {e}")
                    ns._cleanup_fns.clear()
                    ns._store.clear()

# R21-MEM-P2-09修复: 对象池基础框架注释
# 当前SingletonRegistry仅管理单例的注册/查找/清理，缺少对象池(Object Pool)功能�?
# 1. 对象池应支持对象复用（acquire/release），而非仅单例注�?
# 2. 适用于高频创�?销毁的临时对象（如回测结果dict、交易记录NamedTuple�?
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


# ============================================================
# Section 3: StateStore (from state_store.py)
# ============================================================

_IMMUTABLE_TYPES = (int, float, str, bool, type(None), tuple)


def _deep_copy_if_mutable(value: Any) -> Any:
    if isinstance(value, _IMMUTABLE_TYPES):
        return value
    try:
        return copy.deepcopy(value)
    except (TypeError, copy.Error):
        return value


class StateSnapshot:

    def __init__(self, data: Dict[str, Any], version: Dict[str, int], timestamp: float):
        object.__setattr__(self, '_data', data)
        object.__setattr__(self, '_version', version)
        object.__setattr__(self, '_timestamp', timestamp)

    @property
    def data(self) -> Dict[str, Any]:
        return _deep_copy_if_mutable(object.__getattribute__(self, '_data'))

    @property
    def version(self) -> Dict[str, int]:
        return dict(object.__getattribute__(self, '_version'))

    @property
    def timestamp(self) -> float:
        return object.__getattribute__(self, '_timestamp')

    def is_fresh(self, max_age_ms: float) -> bool:
        elapsed_ms = (time.monotonic() - object.__getattribute__(self, '_timestamp')) * 1000.0
        return elapsed_ms <= max_age_ms

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("StateSnapshot is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("StateSnapshot is immutable")


class StateStore:

    FORMAT_VERSION = '1.0'  # P0-16修复: 状态格式版本号

    def __init__(self):
        self._dict: Dict[str, Any] = {}
        self._version_vector: Dict[str, int] = {}
        self._rwlatch = threading.RLock()
        self._callbacks: Dict[str, List[Callable]] = {}
        self._history: List[StateSnapshot] = []
        self._max_history = 64
        self._dict['__format_version__'] = self.FORMAT_VERSION  # P0-16修复: 初始化时写入版本�?

    def get(self, key: str, default: Any = None) -> Any:
        with self._rwlatch:
            if key not in self._dict:
                return default
            return _deep_copy_if_mutable(self._dict[key])

    def get_ref(self, key: str, default: Any = None) -> Any:
        with self._rwlatch:
            if key not in self._dict:
                return default
            return self._dict[key]

    def set(self, key: str, value: Any) -> None:
        with self._rwlatch:
            old_value = self._dict.get(key)
            self._dict[key] = _deep_copy_if_mutable(value)
            self._version_vector[key] = self._version_vector.get(key, 0) + 1
            new_value = self._dict[key]
        self._notify_callbacks(key, old_value, new_value)

    def set_ref(self, key: str, value: Any) -> None:
        with self._rwlatch:
            old_value = self._dict.get(key)
            self._dict[key] = value
            self._version_vector[key] = self._version_vector.get(key, 0) + 1
            new_value = self._dict[key]
        self._notify_callbacks(key, old_value, new_value)

    def snapshot(self, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        with self._rwlatch:
            if keys is None:
                data = _deep_copy_if_mutable(self._dict)
            else:
                data = {}
                for k in keys:
                    if k in self._dict:
                        data[k] = _deep_copy_if_mutable(self._dict[k])
        return data

    def take_snapshot(self, keys: Optional[List[str]] = None) -> StateSnapshot:
        with self._rwlatch:
            if keys is None:
                data = _deep_copy_if_mutable(self._dict)
            else:
                data = {}
                for k in keys:
                    if k in self._dict:
                        data[k] = _deep_copy_if_mutable(self._dict[k])
            version = dict(self._version_vector)
            timestamp = time.monotonic()
            snap = StateSnapshot(data, version, timestamp)
            self._history.append(snap)
            if len(self._history) > self._max_history:
                self._history.pop(0)
        return snap

    def get_version(self, key: str) -> int:
        with self._rwlatch:
            return self._version_vector.get(key, 0)

    def get_all_versions(self) -> Dict[str, int]:
        with self._rwlatch:
            return dict(self._version_vector)

    def _validate_format_version(self, loaded_dict: Dict[str, Any]) -> bool:
        """P0-16修复: 验证加载的状态字典format_version是否兼容

        Args:
            loaded_dict: 从持久化加载的状态字�?

        Returns:
            bool: True=版本兼容, False=版本不兼�?
        """
        loaded_ver = loaded_dict.get('__format_version__', None)
        if loaded_ver is None:
            import logging
            logging.warning("[P0-16] StateStore: loaded state missing format_version, assuming compatible")
            return True
        if loaded_ver != self.FORMAT_VERSION:
            import logging
            logging.error(
                "[P0-16] StateStore: format_version mismatch: loaded=%s, expected=%s",
                loaded_ver, self.FORMAT_VERSION
            )
            return False
        return True

    def apply_diff(self, diff: Dict[str, Any]) -> None:
        # P0-16修复: 验证format_version兼容�?
        if not self._validate_format_version(diff):
            raise ValueError(f"[P0-16] StateStore.apply_diff: format_version mismatch, diff rejected")
        notifications = []
        with self._rwlatch:
            for key, value in diff.items():
                old_value = self._dict.get(key)
                self._dict[key] = _deep_copy_if_mutable(value)
                self._version_vector[key] = self._version_vector.get(key, 0) + 1
                new_value = self._dict[key]
                notifications.append((key, old_value, new_value))
        for key, old_value, new_value in notifications:
            self._notify_callbacks(key, old_value, new_value)

    def rollback(self, version_vector: Dict[str, int]) -> None:
        with self._rwlatch:
            target_snap = None
            for snap in reversed(self._history):
                snap_version = object.__getattribute__(snap, '_version')
                match = True
                for k, v in version_vector.items():
                    if snap_version.get(k, 0) != v:
                        match = False
                        break
                if match:
                    target_snap = snap
                    break
            if target_snap is None:
                raise ValueError(f"No snapshot found for version vector: {version_vector}")
            snap_data = object.__getattribute__(target_snap, '_data')
            snap_version = object.__getattribute__(target_snap, '_version')
            self._dict = _deep_copy_if_mutable(snap_data)
            self._version_vector = dict(snap_version)

    def register_callback(self, key: str, callback: Callable) -> None:
        """P1-05修复: 同时注册到EventBus"""
        with self._rwlatch:
            if key not in self._callbacks:
                self._callbacks[key] = []
            self._callbacks[key].append(callback)
        try:
            from ali2026v3_trading.infra.event_bus import EventBus
            event_bus = EventBus.get_instance()
            event_bus.subscribe(f"state_store.{key}.changed", callback)
        except (ImportError, AttributeError) as _err:
            logging.debug("[state_store] 属性访问降�? %s", _err)

    def _notify_callbacks(self, key: str, old_value: Any, new_value: Any) -> None:
        """P1-05修复: 统一通过EventBus发布"""
        callbacks = self._callbacks.get(key, [])
        try:
            from ali2026v3_trading.infra.event_bus import EventBus
            event_bus = EventBus.get_instance()
            event_bus.publish(f"state_store.{key}.changed", {
                'key': key, 'old_value': old_value, 'new_value': new_value,
            })
        except (ImportError, AttributeError) as _err:
            logging.debug("[state_store] 属性访问降�? %s", _err)
            for cb in callbacks:
                try:
                    cb(key, old_value, new_value)
                except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as _cb_err:
                    logging.debug("[state_store] 未预期异�? %s", _cb_err)


_global_state_store: Optional[StateStore] = None
_store_lock = threading.Lock()


def get_state_store() -> StateStore:
    global _global_state_store
    if _global_state_store is None:
        with _store_lock:
            if _global_state_store is None:
                _global_state_store = StateStore()
    return _global_state_store


def reset_state_store() -> None:
    global _global_state_store
    with _store_lock:
        _global_state_store = None
