"""
quant_services.py - 量化系统服务层

包含：
- LightweightPersistence: 轻量持久化（SQLite WAL+JSON快照+批量写入）
- HotConfigManager: 热配置管理（文件监控+Schema验证+回调通知）
- NumbaJITHelper: Numba可选JIT预编译（运行时检测+纯python回退）
- SingletonManager: 单例生命周期管理（弱引用+显式shutdown+gc）
"""
from __future__ import annotations

import gc
import json
import logging
import os
import sqlite3
import threading
import time
import weakref
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional

import numpy as np

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer

_CHINA_TZ = timezone(timedelta(hours=8))

try:
    from .quant_infra import rate_limit_log
except ImportError:
    from quant_infra import rate_limit_log

# OPTIONAL-DEPENDENCY: numba用于JIT编译加速，缺失时降级为纯Python实现
try:
    import numba as _numba
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    _numba = None

_CONFIG_SCHEMA = {
    'adx_period': int, 'pca_window': int, 'hmm_n_states': int,
    'vol_lookback': int, 'coint_window': int, 'bar_interval_sec': int,
    'bar_vol_threshold': int, 'snapshot_interval_ms': int,
}


class LightweightPersistence:
    """轻量持久化，SQLite WAL模式+JSON快照，Windows兼容（替代RocksDB）。"""

    __slots__ = ('_lock', '_db_path', '_conn', '_json_dir', '_snapshot_interval',
                 '_last_snapshot_ms', '_table_created', '_batch_buffer', '_batch_size')

    def __init__(self, db_dir: Optional[str] = None,
                 snapshot_interval_ms: int = 5000, batch_size: int = 50):
        self._lock = threading.RLock()
        base = db_dir or os.path.join(os.path.dirname(os.path.dirname(__file__)), 'quant_data')
        os.makedirs(base, exist_ok=True)
        self._db_path = os.path.join(base, 'quant_state.db')
        self._json_dir = os.path.join(base, 'snapshots')
        os.makedirs(self._json_dir, exist_ok=True)
        self._snapshot_interval = snapshot_interval_ms
        self._last_snapshot_ms = 0
        self._table_created = False
        self._conn: Optional[sqlite3.Connection] = None
        self._batch_buffer: List[tuple] = []
        self._batch_size = batch_size

    def _ensure_conn(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA synchronous=NORMAL")
            if not self._table_created:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS kv_store (
                        key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_ms INTEGER NOT NULL
                    )
                """)
                self._conn.commit()
                self._table_created = True
            return self._conn

    def save(self, key: str, value: Any) -> None:
        with self._lock:
            try:
                conn = self._ensure_conn()
                json_val = json_dumps(value)
                now_ms = int(time.time() * 1000)
                self._batch_buffer.append((key, json_val, now_ms))
                if len(self._batch_buffer) >= self._batch_size:
                    self._flush_batch(conn)
            except Exception as e:
                rate_limit_log(logging.getLogger(), logging.ERROR,
                               f"[Persistence] save error for {key}: {e}",
                               f"persist_save_{key}", 60.0)

    def _flush_batch(self, conn: sqlite3.Connection) -> None:
        if not self._batch_buffer:
            return
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO kv_store (key, value, updated_ms) VALUES (?, ?, ?)",
                self._batch_buffer
            )
            conn.commit()
            self._batch_buffer.clear()
        except Exception as e:
            rate_limit_log(logging.getLogger(), logging.ERROR,
                           f"[Persistence] batch flush error: {e}", "persist_batch", 60.0)

    def load(self, key: str, default: Any = None) -> Any:
        with self._lock:
            try:
                conn = self._ensure_conn()
                cursor = conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
                row = cursor.fetchone()
                return json.loads(row[0]) if row else default
            except Exception as e:
                rate_limit_log(logging.getLogger(), logging.ERROR,
                               f"[Persistence] load error for {key}: {e}",
                               f"persist_load_{key}", 60.0)
                return default

    def save_snapshot(self, name: str, data: Dict[str, Any]) -> None:
        now_ms = int(time.time() * 1000)
        if now_ms - self._last_snapshot_ms < self._snapshot_interval:
            return
        self._last_snapshot_ms = now_ms
        with self._lock:
            try:
                ts = datetime.now(_CHINA_TZ).strftime('%Y%m%d_%H%M%S')
                filepath = os.path.join(self._json_dir, f"{name}_{ts}.json")
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, default=self._json_serializer, ensure_ascii=False, indent=2)
                self._cleanup_old_snapshots(name, max_files=10)
            except Exception as e:
                rate_limit_log(logging.getLogger(), logging.ERROR,
                               f"[Persistence] snapshot error: {e}", "persist_snapshot", 60.0)

    def load_latest_snapshot(self, name: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            try:
                files = sorted(
                    [f for f in os.listdir(self._json_dir) if f.startswith(name) and f.endswith('.json')],
                    reverse=True
                )
                if not files:
                    return None
                with open(os.path.join(self._json_dir, files[0]), 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                rate_limit_log(logging.getLogger(), logging.ERROR,
                               f"[Persistence] load snapshot error: {e}", "persist_load_snapshot", 60.0)
                return None

    def _cleanup_old_snapshots(self, name: str, max_files: int = 10) -> None:
        try:
            files = sorted(f for f in os.listdir(self._json_dir) if f.startswith(name) and f.endswith('.json'))
            while len(files) > max_files:
                os.remove(os.path.join(self._json_dir, files[0]))
                files.pop(0)
        except OSError:
            pass

    @staticmethod
    def _json_serializer(obj: Any) -> Any:
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        return str(obj)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    if self._batch_buffer:
                        self._flush_batch(self._conn)
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None


class HotConfigManager:
    """热配置管理，JSON文件监控+Schema验证+回调通知（替代etcd/consul）。"""

    __slots__ = ('_lock', '_config_path', '_config', '_callbacks',
                 '_watcher_thread', '_running', '_last_mtime', '_poll_interval',
                 '_callback_failures')

    def __init__(self, config_path: Optional[str] = None, poll_interval: float = 5.0):
        self._lock = threading.RLock()
        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._callbacks: Dict[str, List[Callable[[str, Any], None]]] = {}
        self._watcher_thread: Optional[threading.Thread] = None
        self._running = False
        self._last_mtime = 0.0
        self._poll_interval = poll_interval
        self._callback_failures: Dict[str, str] = {}
        if config_path and os.path.exists(config_path):
            self._load_config()

    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        validated = {}
        for key, expected_type in _CONFIG_SCHEMA.items():
            if key in config:
                try:
                    validated[key] = expected_type(config[key])
                except (ValueError, TypeError):
                    rate_limit_log(logging.getLogger(), logging.WARNING,
                                   f"[HotConfig] invalid type for {key}: {config[key]}",
                                   f"hotconfig_validate_{key}", 60.0)
        for key in config:
            if key not in _CONFIG_SCHEMA:
                validated[key] = config[key]
        return validated

    def _load_config(self) -> bool:
        if not self._config_path or not os.path.exists(self._config_path):
            return False
        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                new_config = json.load(f)
            new_config = self._validate_config(new_config)
            with self._lock:
                old_config = dict(self._config)
                self._config = new_config
                self._last_mtime = os.path.getmtime(self._config_path)
            self._notify_changes(old_config, new_config)
            return True
        except Exception as e:
            rate_limit_log(logging.getLogger(), logging.ERROR,
                           f"[HotConfig] load error: {e}", "hotconfig_load", 60.0)
            return False

    def _notify_changes(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
        for key in set(old_config.keys()) | set(new_config.keys()):
            if old_config.get(key) != new_config.get(key) and key in self._callbacks:
                for cb in self._callbacks[key]:
                    try:
                        cb(key, new_config.get(key))
                        self._callback_failures.pop(key, None)
                    except Exception as e:
                        self._callback_failures[key] = str(e)
                        rate_limit_log(logging.getLogger(), logging.ERROR,
                                       f"[HotConfig] callback error for {key}: {e}",
                                       f"hotconfig_cb_{key}", 60.0)

    def register_callback(self, key: str, callback: Callable[[str, Any], None]) -> None:
        with self._lock:
            self._callbacks.setdefault(key, []).append(callback)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._config.get(key, default)

    def get_all(self) -> Dict[str, Any]:
        with self._lock:
            result = dict(self._config)
            if self._callback_failures:
                result['_callback_failures'] = dict(self._callback_failures)
            return result

    def start_watching(self) -> None:
        if self._running or not self._config_path or not os.path.exists(self._config_path):
            return
        self._running = True
        self._watcher_thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._watcher_thread.start()
        logging.info("[HotConfig] Started watching %s", self._config_path)

    def stop_watching(self) -> None:
        self._running = False
        if self._watcher_thread is not None:
            self._watcher_thread.join(timeout=2.0)
            self._watcher_thread = None

    def _watch_loop(self) -> None:
        while self._running:
            try:
                if self._config_path and os.path.exists(self._config_path):
                    current_mtime = os.path.getmtime(self._config_path)
                    if current_mtime != self._last_mtime:
                        self._load_config()
            except OSError:
                pass
            time.sleep(self._poll_interval)

    def update_config(self, key: str, value: Any) -> None:
        with self._lock:
            self._config[key] = value
        if self._config_path:
            try:
                with open(self._config_path, 'w', encoding='utf-8') as f:
                    json.dump(self._config, f, ensure_ascii=False, indent=2)
            except Exception as e:
                rate_limit_log(logging.getLogger(), logging.ERROR,
                               f"[HotConfig] write error: {e}", "hotconfig_write", 60.0)
        for callback in self._callbacks.get(key, []):
            try:
                callback(key, value)
            except Exception as e:
                rate_limit_log(logging.getLogger(), logging.WARNING,
                               f"[HotConfig] callback error: {e}", f"callback_{key}", 60.0)


class NumbaJITHelper:
    """Numba可选JIT预编译，运行时检测可用性，自动选择@jit或纯python回退。"""

    __slots__ = ('_compiled_functions', '_warmup_done')

    def __init__(self):
        self._compiled_functions: Dict[str, Any] = {}
        self._warmup_done = False

    @staticmethod
    def jit(nopython: bool = True, cache: bool = True, fastmath: bool = True):
        def decorator(func):
            if HAS_NUMBA and _numba is not None:
                try:
                    return _numba.jit(nopython=nopython, cache=cache, fastmath=fastmath)(func)
                except Exception:
                    return func
            return func
        return decorator

    def warmup(self) -> None:
        if self._warmup_done:
            return
        if not HAS_NUMBA:
            self._warmup_done = True
            return
        try:
            _warmup_numba()
        except Exception as e:
            rate_limit_log(logging.getLogger(), logging.WARNING,
                           f"[NumbaJIT] warmup failed: {e}", "numba_warmup", 300.0)
        self._warmup_done = True

    @property
    def is_available(self) -> bool:
        return HAS_NUMBA


if HAS_NUMBA and _numba is not None:
    @_numba.jit(nopython=True, cache=True, fastmath=True)
    def _warmup_numba() -> None:
        _acc = 0.0
        for _i in range(100):
            _acc += float(_i)
else:
    def _warmup_numba() -> None:
        pass


numba_helper = NumbaJITHelper()


class SingletonManager:
    """单例生命周期管理，弱引用注册表+显式shutdown+gc，防止循环引用内存泄漏。"""

    _instance: Optional['SingletonManager'] = None
    _lock = threading.Lock()

    __slots__ = ('_registry', '_shutdown_order', '_shutting_down')

    def __init__(self):
        self._registry: Dict[str, weakref.ref] = {}
        self._shutdown_order: List[str] = []
        self._shutting_down = False

    @classmethod
    def get_instance(cls) -> 'SingletonManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def register(self, name: str, obj: Any, shutdown_method: str = 'shutdown') -> None:
        ref = weakref.ref(obj, lambda r: self._on_gc(name))
        self._registry[name] = ref
        if name not in self._shutdown_order:
            self._shutdown_order.append(name)
        setattr(obj, '_singleton_shutdown_method', shutdown_method)
        setattr(obj, '_singleton_name', name)

    def _on_gc(self, name: str) -> None:
        self._registry.pop(name, None)
        if name in self._shutdown_order:
            self._shutdown_order.remove(name)

    def get(self, name: str) -> Optional[Any]:
        ref = self._registry.get(name)
        if ref is not None:
            obj = ref()
            if obj is not None:
                return obj
            del self._registry[name]
        return None

    def shutdown_all(self) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        for name in reversed(self._shutdown_order):
            obj = self.get(name)
            if obj is not None:
                method_name = getattr(obj, '_singleton_shutdown_method', 'shutdown')
                method = getattr(obj, method_name, None)
                if callable(method):
                    try:
                        method()
                    except Exception as e:
                        logging.error("[SingletonManager] shutdown %s error: %s", name, e)
        self._registry.clear()
        self._shutdown_order.clear()
        gc.collect()
        self._shutting_down = False
