import atexit
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    'CallbackRegistry', 'CallbackGroup', 'get_callback_group', 'reset_callback_group',
    'ON_TICK', 'ON_KLINE', 'ON_ORDER_UPDATE', 'ON_POSITION_CHANGE', 'ON_RISK_STATE_CHANGE',
]

ON_TICK = 'on_tick'
ON_KLINE = 'on_kline'
ON_ORDER_UPDATE = 'on_order_update'
ON_POSITION_CHANGE = 'on_position_change'
ON_RISK_STATE_CHANGE = 'on_risk_state_change'

_THREAD_POOL = ThreadPoolExecutor(max_workers=4)


def _shutdown_callback_pool():  # [R27-AUDIT] P0修复: 模块级ThreadPoolExecutor添加atexit清理
    try:
        _THREAD_POOL.shutdown(wait=False)
    except Exception:
        pass


atexit.register(_shutdown_callback_pool)


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
            except Exception:
                results.append(None)
        return results

    def invoke_async(self, *args, **kwargs) -> None:
        snapshot = self._callbacks[:]
        for cb in snapshot:
            _THREAD_POOL.submit(self._safe_call, cb, args, kwargs)

    @staticmethod
    def _safe_call(cb: Callable, args: tuple, kwargs: dict) -> None:
        try:
            cb(*args, **kwargs)
        except Exception:
            pass

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
