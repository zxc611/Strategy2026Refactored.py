import copy
import threading
import time
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    'StateSnapshot', 'StateStore', 'get_state_store', 'reset_state_store',
]


_IMMUTABLE_TYPES = (int, float, str, bool, type(None), tuple)


def _deep_copy_if_mutable(value: Any) -> Any:
    if isinstance(value, _IMMUTABLE_TYPES):
        return value
    try:
        return copy.deepcopy(value)
    except (TypeError, AttributeError) as e:
        # 无法pickle的对象（如threading.Lock），返回原对象引用
        # 这在状态存储场景中是安全的，因为这些对象本身就是线程安全的
        import logging
        logging.debug(f"[StateStore] Cannot deepcopy {type(value).__name__}, using reference: {e}")
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
        self._dict['__format_version__'] = self.FORMAT_VERSION  # P0-16修复: 初始化时写入版本号

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
            loaded_dict: 从持久化加载的状态字典

        Returns:
            bool: True=版本兼容, False=版本不兼容
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
        # P0-16修复: 验证format_version兼容性
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
        with self._rwlatch:
            if key not in self._callbacks:
                self._callbacks[key] = []
            self._callbacks[key].append(callback)

    def _notify_callbacks(self, key: str, old_value: Any, new_value: Any) -> None:
        callbacks = self._callbacks.get(key, [])
        for cb in callbacks:
            try:
                cb(key, old_value, new_value)
            except Exception:
                pass


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
