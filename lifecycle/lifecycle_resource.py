# MODULE_ID: M1-127
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
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
        # P2-2: 托管线程（threading.Thread + threading.Event 对）注册表
        # 根因: on_start/on_stop/pause 三处分散维护 W1-W5 线程列表，
        #       新增线程时需同步修改三处，易遗漏（历史根因 R-4 即由此产生）。
        # 修复: 一处注册（on_start 调用 register_thread），多处复用（on_stop/pause 调用 stop_managed_threads）。
        self._managed_threads: List[Dict[str, Any]] = []

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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning("[LifecycleResource] 线程池%s关闭失败: %s", name, e)
        self._thread_pools.clear()

    # ==================== P2-2: 托管线程生命周期管理 ====================

    def register_thread(self, name: str, thread_attr: str, stop_attr: str,
                        join_timeout: float = 10.0,
                        categories=('subscribe',)) -> None:
        """注册一个托管线程（threading.Thread + threading.Event 对）

        在 on_start 中一处注册，on_stop/pause 复用 stop_managed_threads 统一停止。
        替代原 on_start/on_stop/pause 三处分散维护 W1-W5 列表的模式。

        Args:
            name: 线程描述名（用于日志，如 'W1-db-subscribe'）
            thread_attr: 宿主对象上的属性名（存储 threading.Thread 实例）
            stop_attr: 宿主对象上的属性名（存储 threading.Event 实例）
            join_timeout: on_stop/pause 时的 join 超时秒数（默认 10s）
            categories: 分类元组，用于按类别停止（如 ('subscribe',) 或 ('kline',)）
                        'all' 类别自动包含，stop_managed_threads 默认停所有
        """
        self._managed_threads.append({
            'name': name,
            'thread_attr': thread_attr,
            'stop_attr': stop_attr,
            'join_timeout': join_timeout,
            'categories': set(categories) | {'all'},
        })

    def stop_managed_threads(self, host: Any, categories=None,
                             join_timeout: float = None) -> None:
        """统一停止并 join 所有托管线程

        替代 on_stop/pause 中分散的 for 循环。先 set stop event，再 join(timeout)，
        最后清理线程引用（setattr None）。

        Args:
            host: 宿主对象（线程和 event 属性所在的对象，通常是 strategy_core_service 实例）
            categories: None=停止所有托管线程；
                        或指定类别如 ('subscribe', 'kline') 只停匹配的线程
            join_timeout: 覆盖注册时的 join_timeout（None 用各线程注册时的值）
        """
        for entry in self._managed_threads:
            # 按类别筛选
            if categories is not None:
                if not (set(categories) & entry['categories']):
                    continue
            _name = entry['name']
            try:
                _stop_evt = getattr(host, entry['stop_attr'], None)
                if _stop_evt is not None and hasattr(_stop_evt, 'set'):
                    _stop_evt.set()
                    logging.info("[LifecycleResource] %s停止事件已设置", _name)
                _t = getattr(host, entry['thread_attr'], None)
                if _t is not None and hasattr(_t, 'is_alive') and _t.is_alive():
                    _to = join_timeout if join_timeout is not None else entry['join_timeout']
                    _t.join(timeout=_to)
                    if _t.is_alive():
                        logging.warning("[LifecycleResource] %s线程join超时(%ss)，继续（daemon将自行退出）",
                                        _name, _to)
                    else:
                        logging.info("[LifecycleResource] %s线程已退出", _name)
                    setattr(host, entry['thread_attr'], None)
            except (ValueError, KeyError, TypeError, AttributeError) as _e:
                logging.warning("[LifecycleResource] %s线程停止失败: %s", _name, _e)

    def register_scheduler(self, scheduler: Any) -> None:
        self._scheduler = scheduler
        self.register_resource('scheduler', scheduler)


# 模块级函数（用于向后兼容）
_default_resource_manager: Optional[LifecycleResource] = None


def register_thread_pool(name: str, pool: Any) -> None:
    """注册线程池（模块级函数，用于向后兼容）
    
    Args:
        name: 线程池名称
        pool: 线程池实例
    """
    global _default_resource_manager
    if _default_resource_manager is None:
        logging.debug("[lifecycle_resource] register_thread_pool called but no default manager set")
        return
    _default_resource_manager.register_thread_pool(name, pool)


def set_default_resource_manager(manager: LifecycleResource) -> None:
    """设置默认资源管理器
    
    Args:
        manager: LifecycleResource实例
    """
    global _default_resource_manager
    _default_resource_manager = manager


__all__ = [
    'LifecycleResource',
    'register_thread_pool',
    'set_default_resource_manager',
]


def stop_scheduler(self) -> None:
    if self._scheduler is not None:
        stop = getattr(self._scheduler, 'stop', None) or getattr(self._scheduler, 'shutdown', None)
        if callable(stop):
            try:
                stop()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[LifecycleResource] 调度器停止失败: %s", e)
        self._scheduler = None

def get_resource(self, name: str) -> Optional[Any]:
    with self._resource_lock:
        return self._owned_resources.get(name)
