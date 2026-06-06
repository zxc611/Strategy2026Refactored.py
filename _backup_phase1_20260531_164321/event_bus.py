"""
事件总线模块 - 线程安全的发布/订阅模式实现
来源：新建文件
功能：服务间解耦通信，事件驱动架构核心

优化 v2.2 (2026-03-15):
- ✓添加线程池管理（防止线程爆炸）
- ✓使用 deque 优化事件存储（O(1) 操作）
- ✓添加背压机制（限制队列长度）
- ✓修复全局单例线程安全问题
- ✓补全类型注解
- ✓优化日志级别
- ✓[NEW] 事件优先级支持
- ✓[NEW] 事件过滤器（条件订阅）
- ✓[NEW] 速率限制器
- ✓[P2] 移除冗余事件类（使用 pythongo 导入）
"""
import threading
import logging
import os
import weakref  # R21-MEM-P1-02修复: 弱引用支持
from typing import Dict, List, Callable, Any, Optional, Set, TypeVar, Generic, Tuple
import copy
from datetime import datetime
from ali2026v3_trading.shared_utils import CHINA_TZ
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future
import time
from functools import wraps

# R27-P1修复: 导入回调异常隔离和订阅者快照守卫
from ali2026v3_trading.resilience_utils import safe_callback_wrapper, SubscriberSnapshotGuard


def _lazy_repr(obj: Any, max_len: int = 200) -> str:
    """R15-P1-LOG-17修复: 智能repr截断，保留type/key信息，截断冗余value"""
    try:
        s = repr(obj)
    except Exception:
        return f'<repr-failed:{type(obj).__name__}>'
    if len(s) <= max_len:
        return s
    type_name = type(obj).__name__
    if isinstance(obj, dict):
        keys = list(obj.keys())[:5]
        key_str = ','.join(str(k) for k in keys)
        return f'<dict:{key_str}...+{len(obj)-len(keys)} keys, repr_truncated>'
    return f'<{type_name}:len={len(s)}, repr_truncated>'


class EventBus:
    """线程安全的事件总线（优化版 v2.2）
    
    特性：
    - 支持多订阅者
    - 异步事件分发（带线程池管理）
    - 异常隔离（单个订阅者错误不影响其他订阅者）
    - 线程安全
    - 背压机制（防止内存溢出）
    - 高效事件存储（deque O(1) 操作）
    - 事件优先级支持（高优先级优先处理）
    - 事件过滤器（条件订阅）
    - 速率限制（防 flooding）
    """
    
    # 常量定义
    DEFAULT_MAX_HISTORY_SIZE = 1000
    DEFAULT_MAX_WORKERS = 10
    # R13-P2-LOG-06修复: 事件持久化截断限制增大为可配置，默认5000提升至50000
    DEFAULT_MAX_PERSISTED_EVENTS = 50000
    DEFAULT_BACKPRESSURE_LIMIT = 5000
    DEFAULT_RATE_LIMIT = 100  # 每秒最大事件数
    
    def __init__(self, max_workers: int = None, max_history_size: int = None, 
                 backpressure_limit: int = None, rate_limit: int = None,
                 enable_persistence: bool = False, persistence_dir: str = None):
        """初始化事件总线
        
        Args:
            max_workers: 线程池最大工作线程数（默认10）
            max_history_size: 最大事件历史记录数（默认1000）
            backpressure_limit: 背压限制，待处理事件上限（默认5000）
            rate_limit: 速率限制，每秒最大发布次数（默认100，0=不限制）
            enable_persistence: 是否启用事件持久化（默认False）
            persistence_dir: 持久化目录（默认None）
        """
        self._subscribers: Dict[str, List[Tuple[Callable, int]]] = {}
        self._callback_mapping: Dict[Callable, Callable] = {}  # P1 Bug #59修复：original_callback -> wrapped_callback映射
        self._callback_set: Dict[str, Set[Callable]] = {}  # P2 Bug #116修复：使用set存储回调引用，O(1)查找
        self._lock = threading.RLock()
        
        # 优化 1: 使用 deque 替代 list，O(1) 追加和弹出
        self._event_history: deque = deque(maxlen=max_history_size or self.DEFAULT_MAX_HISTORY_SIZE)
        
        # 优化 2: 添加线程池管理 - 使用有界队列防止内存溢出
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=max_workers or self.DEFAULT_MAX_WORKERS,
            thread_name_prefix='event_bus'
        )
        # R23-P1-05修复: 注册atexit确保线程池生命周期管理
        import atexit as _atexit
        _atexit.register(self.shutdown)
        
        # 优化 3: 添加背压机制
        self._backpressure_limit: int = backpressure_limit or self.DEFAULT_BACKPRESSURE_LIMIT
        self._pending_events: int = 0
        self._pending_events_lock = threading.Lock()  # P1 Bug #57修复：保护_pending_events的原子操作
        self._dropped_events_count: int = 0
        self._count_lock = threading.Lock()
        # R15-P1-RES-15修复: 背压丢弃计数器和告警
        self._backpressure_counter: int = 0
        self._backpressure_warn_interval: int = 1000
        
        # P2 新增：速率限制
        self._rate_limit: int = rate_limit or self.DEFAULT_RATE_LIMIT
        self._rate_limiter: Optional['RateLimiter'] = None
        if self._rate_limit > 0:
            self._rate_limiter = RateLimiter(self._rate_limit)
        
        # #139 新增：事件发布回调
        self._publish_callbacks: List[Callable[[str, Any, bool], None]] = []
        
        # R27-P1-RC-03修复: 订阅者快照守卫，防止注销与发布竞争
        self._snapshot_guard = SubscriberSnapshotGuard()
        
        # #354 新增：事件持久化支持
        self._enable_persistence = enable_persistence
        self._persistence_dir = persistence_dir
        if self._enable_persistence and self._persistence_dir:
            os.makedirs(self._persistence_dir, exist_ok=True)
            self._persisted_events: deque = deque(maxlen=self.DEFAULT_MAX_PERSISTED_EVENTS)  # R13-P2-LOG-06修复
        
        # #358 新增：缓存预热标记
        self._cache_warmed = False
        
        # P2 Bug #115修复：shutdown标志
        self._shutdown = False
        
        # 统计信息
        self._published_count: int = 0
        self._failed_count: int = 0
        self._publish_lock = threading.Lock()  # P1 Bug #58修复：保护_published_count的原子自增

        # P1-3: 线程池监控属性
        self._executor_monitor_enabled: bool = True
        self._executor_warn_threshold: int = 8
        self._executor_error_threshold: int = 10
        self._executor_stats: Dict[str, Any] = {}
    
    def subscribe(self, event_type: str, callback: Callable, priority: int = 0,
                  filter_func: Optional[Callable[[Any], bool]] = None,
                  replay_last: int = 0) -> None:
        """
        P2 Bug #115修复：订阅事件（支持优先级和可选过滤器，检查shutdown状态）

        # R13-P2-API-06修复: 文档化shutdown行为
        # [R14-P1-API-17] 更新: shutdown后subscribe改为warning+return(不抛RuntimeError)
        **行为变更说明**：
        - 当EventBus已调用shutdown()后，subscribe()将logging.warning并静默返回。
          这是为了避免shutdown后注册回调导致调用方崩溃（R14-P1-API-17修复）。
        - 如果需要在shutdown后仍能注册（如测试场景），请先调用reset_global_event_bus()。

        Args:
            event_type: 事件类型名称
            callback: 回调函数，接收一个事件对象参数
            priority: 优先级（0=普通，>0=更高优先级，数字越大优先级越高）
            filter_func: 可选过滤器函数，返回True时才执行回调
            replay_last: R5-T-02修复: 订阅后重播最近N个历史事件（默认0不重播）
            
        Example:
            # 普通订阅
            event_bus.subscribe('SignalEvent', lambda e: print(f'收到信号：{e}'))
            
            # 高优先级订阅（优先执行）
            event_bus.subscribe('RiskEvent', handle_risk, priority=10)
            
            # 带过滤器订阅
            event_bus.subscribe('SignalEvent', on_buy, filter_func=lambda e: getattr(e, 'signal_type', '') == 'BUY')
        """
        if not event_type or not callable(callback):
            logging.error(f"[EventBus] Invalid subscription: event_type={event_type}, callback={callback}")
            return
        
        if filter_func is not None and not callable(filter_func):
            logging.error(f"[EventBus] Invalid filter function for '{event_type}'")
            return
        
        # P2 Bug #115修复：检查是否已shutdown
        if self._shutdown:
            logging.warning("[R14-P1-API-17] EventBus已关闭，subscribe静默忽略: event_type=%s", event_type)
            return
        
        actual_callback = callback
        if filter_func is not None:
            @wraps(callback)
            def wrapped_callback(event):
                try:
                    if filter_func(event):
                        callback(event)
                    else:
                        logging.debug(f"[EventBus] Filter blocked event '{event_type}'")
                except Exception as e:
                    logging.error(f"[EventBus] Filter function error: {e}")
                    callback(event)
            actual_callback = wrapped_callback
            with self._lock:
                self._callback_mapping[callback] = actual_callback
        
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
                self._callback_set[event_type] = set()
            
            if actual_callback not in self._callback_set[event_type]:
                self._subscribers[event_type].append((actual_callback, priority))
                self._callback_set[event_type].add(actual_callback)
                self._subscribers[event_type].sort(key=lambda x: x[1], reverse=True)
                logging.debug(f"[EventBus] Subscribed to '{event_type}' with priority {priority} "
                            f"(total: {len(self._subscribers[event_type])} subscribers)")

            # R5-T-02修复: 订阅后重播最近N个历史事件，防止订阅前发布的事件丢失
            if replay_last > 0:
                history_events = [rec for rec in self._event_history
                                  if rec.get('type') == event_type]
                for rec in history_events[-replay_last:]:
                    try:
                        actual_callback(rec.get('_raw_event', rec))
                        logging.debug(f"[EventBus] R5-T-02 replayed event '{event_type}' to new subscriber")
                    except Exception as e:
                        logging.error(f"[EventBus] R5-T-02 replay callback error: {e}", exc_info=True)

    # R21-MEM-P1-02修复: 弱引用订阅，当回调对象被GC时自动清理
    def subscribe_weak(self, event_type: str, callback: Callable, priority: int = 0) -> None:
        """弱引用订阅。当回调所属对象被GC回收时自动取消订阅，防止内存泄漏。

        仅适用于绑定方法（bound method）和可弱引用的对象方法。
        普通函数，lambda不支持weakref，请使用普通subscribe()。

        Args:
            event_type: 事件类型名称
            callback: 回调函数（必须是绑定方法，即有__self__属性的方法）
            priority: 优先级
        """
        if not event_type or not callable(callback):
            logging.error(f"[EventBus] Invalid weak subscription: event_type={event_type}, callback={callback}")
            return

        if self._shutdown:
            raise RuntimeError("EventBus已关闭")

        # 尝试获取绑定方法的self对象
        obj_ref = None
        if hasattr(callback, '__self__'):
            obj_ref = weakref.ref(callback.__self__, lambda ref: self._on_weakref_dead(event_type, callback))
        elif hasattr(callback, '__weakref__'):
            obj_ref = weakref.ref(callback, lambda ref: self._on_weakref_dead(event_type, callback))
        else:
            # 不支持弱引用的对象，降级为普通订阅
            logging.warning(f"[EventBus] subscribe_weak: callback不支持弱引用，降级为普通subscribe: {callback}")
            self.subscribe(event_type, callback, priority)
            return

        # 使用弱引用包装器
        func_ref = weakref.ref(callback.__func__ if hasattr(callback, '__func__') else callback)

        @wraps(callback)
        def weak_wrapper(event):
            obj = obj_ref() if obj_ref is not None else None
            func = func_ref()
            if obj is not None and func is not None:
                func(obj, event)
            elif func is not None:
                func(event)
            # 对象已被GC，静默跳过（将在下次清理时移除）

        # 保存映射以便清理
        with self._lock:
            if not hasattr(self, '_weak_refs'):
                self._weak_refs: Dict[Callable, Tuple[weakref.ref, Any]] = {}
            self._weak_refs[callback] = (obj_ref, weak_wrapper)

        self.subscribe(event_type, weak_wrapper, priority)

    def _on_weakref_dead(self, event_type: str, original_callback: Callable) -> None:
        """R21-MEM-P1-02修复: 弱引用对象被GC时的回调，自动清理订阅"""
        with self._lock:
            if hasattr(self, '_weak_refs') and original_callback in self._weak_refs:
                _, weak_wrapper = self._weak_refs.pop(original_callback)
                # 从订阅者列表中移除weak_wrapper
                if event_type in self._subscribers:
                    self._subscribers[event_type] = [
                        (cb, pri) for cb, pri in self._subscribers[event_type]
                        if cb is not weak_wrapper
                    ]
                    if event_type in self._callback_set:
                        self._callback_set[event_type].discard(weak_wrapper)
                    if not self._subscribers[event_type]:
                        del self._subscribers[event_type]
                        if event_type in self._callback_set:
                            del self._callback_set[event_type]
                logging.debug(f"[EventBus] Weak ref cleaned up for '{event_type}'")
    
    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """
        P1 Bug #59修复：取消订阅（支持subscribe_with_filter注册的回调）
        
        Args:
            event_type: 事件类型名称
            callback: 要移除的回调函数
        """
        with self._lock:
            # P1 Bug #59修复：查找是否有wrapped_callback映射
            actual_callback = self._callback_mapping.get(callback, callback)
            
            if event_type in self._subscribers:
                # P2 Bug #116修复：从set中移除
                if event_type in self._callback_set:
                    self._callback_set[event_type].discard(actual_callback)
                
                # 从列表中移除
                original_len = len(self._subscribers[event_type])
                self._subscribers[event_type] = [
                    (cb, priority) for cb, priority in self._subscribers[event_type]
                    if cb != actual_callback
                ]
                
                # R13-P1-DEAD-07修复: 记录订阅者计数递减，确保unsubscribe可追踪
                removed_count = original_len - len(self._subscribers[event_type])
                if removed_count > 0:
                    logging.debug(
                        f"[EventBus] Unsubscribed from '{event_type}' "
                        f"(removed={removed_count}, remaining={len(self._subscribers[event_type])})"
                    )
                else:
                    logging.warning(f"[EventBus] Callback not found for '{event_type}'")
                
                # R13-P1-DEAD-07修复: 当事件类型无订阅者时清理空列表，防止内存泄漏
                if not self._subscribers[event_type]:
                    del self._subscribers[event_type]
                    if event_type in self._callback_set:
                        del self._callback_set[event_type]
            
            # 清理映射
            if callback in self._callback_mapping:
                del self._callback_mapping[callback]
    
    def publish(self, event: Any, async_mode: bool = True, force: bool = False, 
                priority: Optional[int] = None) -> bool:
        """
        P2 Bug #115修复：发布事件（检查shutdown状态）
        
        Args:
            event: 事件对象
            async_mode: True=异步分发 False=同步分发
            force: True=强制发布(忽略背压限制) False=受背压限制
            priority: 事件优先级(None=使用事件自带优先级或0)
            
        Returns:
            bool: 发布是否成功
            
        Example:
            event_bus.publish(SignalEvent(instrument='IF2501', signal='BUY'))
        """
        if event is None:
            logging.warning("[EventBus][owner_scope=shared-service][source_type=event-tail] " "Attempted to publish None event")
            return False
        
        # P2 Bug #115修复：检查是否已shutdown
        if self._shutdown and not force:
            raise RuntimeError(
                "[EventBus][owner_scope=shared-service][source_type=event-tail] EventBus已关闭"
            )
        
        # P2 新增：速率限制检查
        if self._rate_limiter and not force:
            if not self._rate_limiter.acquire():
                with self._count_lock:
                    self._dropped_events_count += 1
                logging.warning(
                    f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                    f"Rate limit exceeded ({self._rate_limit}/s), "
                    f"dropping event: {type(event).__name__} (total dropped: {self._dropped_events_count})"
                )
                # #139/#309: 通知回调
                self._notify_publish_callbacks(type(event).__name__, event, False)
                return False
        
        # 优化 4: 背压检查
        with self._pending_events_lock:  # P1 Bug #57修复：原子操作
            if not force and self._pending_events >= self._backpressure_limit:
                # RES-P1-15修复: 关键事件降级为同步分发
                _event_type_name = getattr(event, 'type', type(event).__name__)
                _CRITICAL_EVENT_TYPES = frozenset(['SignalEvent', 'OrderEvent', 'RiskEvent', 'CircuitBreakerTriggeredEvent'])
                if _event_type_name in _CRITICAL_EVENT_TYPES:
                    logging.warning("[RES-P1-15] 背压降级: 关键事件%s转为同步分发", _event_type_name)
                    with self._lock:
                        callbacks = self._subscribers.get(_event_type_name, []).copy()
                    if callbacks:
                        self._invoke_all_callbacks(callbacks, event, _event_type_name)
                    return True
                with self._count_lock:
                    self._dropped_events_count += 1
                    # R15-P1-RES-15修复: 每1000次丢弃告警
                    self._backpressure_counter += 1
                if self._backpressure_counter % self._backpressure_warn_interval == 0:
                    logging.warning(
                        "R15-P1-RES-15: 背压丢弃累计%d次，每%d次告警",
                        self._backpressure_counter, self._backpressure_warn_interval
                    )
                logging.warning(
                    f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                    f"Backpressure limit reached ({self._pending_events}/{self._backpressure_limit}), "
                    f"dropping event: {type(event).__name__} (total dropped: {self._dropped_events_count})"
                )
                # #139/#309: 通知回调
                self._notify_publish_callbacks(type(event).__name__, event, False)
                return False
        
        # 获取事件类型
        event_type = getattr(event, 'type', type(event).__name__)
        
        # #354: 持久化关键事件
        if self._enable_persistence and event_type in ['SignalEvent', 'OrderEvent', 'RiskEvent']:
            self._persist_event(event_type, event)
        
        # 记录事件历史
        self._record_event(event_type, event)
        
        # 获取所有订阅者
        with self._lock:
            callbacks = self._subscribers.get(event_type, []).copy()
        
        if not callbacks:
            logging.debug(
                f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                f"No subscribers for event type '{event_type}'"
            )
            # #139: 即使没有订阅者也返回成功
            self._notify_publish_callbacks(event_type, event, True)
            return True
        
        # P1 Bug #58修复：在锁内执行自增
        with self._publish_lock:
            self._published_count += 1
        logging.debug(
            f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
            f"Publishing '{event_type}' to {len(callbacks)} subscriber(s)"
        )
        
        # 分发事件
        if async_mode:
            # P1-3: 线程池监控检查
            if self._executor_monitor_enabled:
                self.get_executor_stats()

            # 优化 5: 使用线程池而非每次创建新线程
            try:
                with self._pending_events_lock:  # P1 Bug #57修复：原子操作
                    self._pending_events += 1
                _submit_time = time.monotonic()
                future = self._executor.submit(self._invoke_all_callbacks, callbacks, event, event_type)
                future.add_done_callback(
                    lambda f, st=_submit_time: self._on_callback_complete(f, event_type, event, st)
                )
            except Exception as e:
                with self._pending_events_lock:  # P1 Bug #57修复：原子操作
                    self._pending_events -= 1
                logging.error(
                    f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                    f"Failed to submit async task: {e}"
                )
                # #139/#140: 通知回调并抛出异常
                self._notify_publish_callbacks(event_type, event, False)
                raise RuntimeError(f"Event dispatch failed: {e}") from e
        else:
            # 同步分发
            success = self._invoke_all_callbacks(callbacks, event, event_type)
            # #139/#309: 通知回调
            self._notify_publish_callbacks(event_type, event, success)
            return success
        
        return True
    
    def _notify_publish_callbacks(self, event_type: str, event: Any, success: bool) -> None:
        """#139: 通知发布回调
        
        Args:
            event_type: 事件类型
            event: 事件对象
            success: 发布是否成功
        """
        for callback in self._publish_callbacks:
            try:
                callback(event_type, event, success)
            except Exception as e:
                logging.error(
                    f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                    f"Publish callback error: {e}"
                )
    
    def _invoke_all_callbacks(self, callbacks: List[tuple], event: Any, event_type: str) -> bool:
        # R15-P1-PERF-09修复: 按priority降序排序，高优先级先执行
        sorted_callbacks = sorted(callbacks, key=lambda cb: cb[1], reverse=True)
        # R5-E-06/R5-T-06修复: 单个订阅者异常不影响其他订阅者（已有try/except隔离）；事件不丢失（all_success标记但不中断）
        all_success = True
        for callback, priority in sorted_callbacks:
            try:
                callback(event)
            except Exception as e:
                all_success = False
                # R5-E-07修复: 记录完整堆栈便于调试
                logging.error(
                    f"[EventBus] Callback exception for '{event_type}': {e}\n"
                    f"Callback: {callback}\n"
                    f"Event: {event}",
                    exc_info=True
                )
        return all_success
    
    def _on_callback_complete(self, future: Future, event_type: str, event: Any,
                              submit_time: float = 0.0) -> None:
        """回调完成后的处理
        
        Args:
            future: 未来对象
            event_type: 事件类型
            event: 事件对象
            submit_time: 任务提交时间(monotonic)，用于计算延迟
        """
        with self._pending_events_lock:
            self._pending_events -= 1

        # P1-3: 计算延迟并记录数据流性能
        latency_ms = 0.0
        if submit_time > 0.0:
            latency_ms = (time.monotonic() - submit_time) * 1000.0
            try:
                record_dataflow_latency(event_type, latency_ms)
            except Exception:
                pass
        
        # 检查是否有异常
        try:
            exception = future.exception()
            success = exception is None
            if exception:
                with self._count_lock:
                    self._failed_count += 1
                logging.error(
                    f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                    f"Async callback failed: {exception}",
                    exc_info=True,
                )
                self._notify_publish_callbacks(event_type, event, False)
                # #311: 发送失败通知
                self._send_nack(event_type, event, str(exception), _nack_depth=1)
            else:
                self._notify_publish_callbacks(event_type, event, True)

            # P1-3: 数据流异常检测
            try:
                detect_dataflow_anomaly(event_type, latency_ms, success)
            except Exception:
                pass
        except Exception as e:
            logging.error(
                f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                f"Failed to check callback result: {e}"
            )
            self._notify_publish_callbacks(event_type, event, False)
    
    def _send_nack(self, event_type: str, event: Any, reason: str, _nack_depth: int = 0) -> None:
        """#311: 发送否定确认（处理失败通知）

        Args:
            event_type: 事件类型
            event: 事件对象
            reason: 失败原因
            _nack_depth: 递归深度限制（内部使用，防止无限递归）
        """
        # DFG-06修复: 递归深度限制，防止NACK事件处理失败再次触发NACK导致无限递归
        if _nack_depth >= 3:
            logging.error(
                f"[EventBus] NACK recursion depth limit reached ({_nack_depth}), "
                f"dropping NACK for {event_type}: {reason}"
            )
            return
        nack_event = {
            'type': f'{event_type}_NACK',
            'original_type': event_type,
            'original_event': _lazy_repr(event, 200),
            'reason': reason,
            'timestamp': datetime.now(CHINA_TZ).isoformat()
        }
        logging.warning(f"[EventBus] NACK for {event_type}: {reason}")
        try:
            self.publish(nack_event, async_mode=False, force=True)
        except Exception as e:
            logging.error(f"[EventBus] Failed to publish NACK event: {e}")
    
    def _invoke_callback(self, callback: Callable, event: Any, event_type: str) -> None:
        """调用回调函数（带异常处理）
        
        Args:
            callback: 回调函数
            event: 事件对象
            event_type: 事件类型（用于日志）
        """
        try:
            callback(event)
        except Exception as e:
            # 异常隔离：单个订阅者的错误不影响其他订阅者
            logging.error(
                f"[EventBus] Callback exception for '{event_type}': {e}\n"
                f"Callback: {callback}\n"
                f"Event: {event}",
                exc_info=True  # R5-E-07修复: 记录完整堆栈便于调试
            )
    
    def _record_event(self, event_type: str, event: Any) -> None:
        """记录事件历史（优化版）
        
        Args:
            event_type: 事件类型
            event: 事件对象
        """
        try:
            # 优化 7: 使用 namedtuple 或简单对象替代dict，减少内存占用
            event_record = {
                'type': event_type,
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'data': _lazy_repr(event, 200),
                '_raw_event': event
            }
            
            # deque 自动处理大小限制，无需手动检查
            self._event_history.append(event_record)
        except Exception as e:
            logging.error(f"[EventBus] Failed to record event history: {e}")
    
    def _persist_event(self, event_type: str, event: Any) -> None:
        """#354: 持久化关键事件
        
        Args:
            event_type: 事件类型
            event: 事件对象
        """
        if not self._enable_persistence or not self._persistence_dir:
            return
        
        try:
            timestamp = datetime.now(CHINA_TZ).strftime('%Y%m%d_%H%M%S_%f')
            filename = f"{self._persistence_dir}/{event_type}_{timestamp}.json"
            
            # 简单序列化
            event_data = {
                'type': event_type,
                'timestamp': timestamp,
                'data': str(event)[:500]
            }
            
            import json
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(event_data, f, ensure_ascii=False)
            
            self._persisted_events.append(filename)
            logging.debug(f"[EventBus] Persisted event '{event_type}' to {filename}")
        except Exception as e:
            logging.error(f"[EventBus] Failed to persist event: {e}")
    
    def warmup_cache(self) -> None:
        """#358: 缓存预热（从持久化存储恢复关键事件）
        
        Example:
            event_bus.warmup_cache()  # 启动时调用
        """
        if not self._enable_persistence or not self._persistence_dir:
            logging.debug("[EventBus] Persistence not enabled, skip cache warmup")
            return
        
        if self._cache_warmed:
            logging.debug("[EventBus] Cache already warmed")
            return
        
        try:
            import glob
            pattern = f"{self._persistence_dir}/*.json"
            files = sorted(glob.glob(pattern))[-100:]  # 最近100个事件
            
            for filepath in files:
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        import json
                        event_data = json.load(f)
                        self._event_history.append(event_data)
                        logging.info(f"[EventBus] Restored event from {filepath}")
                except Exception as e:
                    logging.warning(f"[EventBus] Failed to restore {filepath}: {e}")
            
            self._cache_warmed = True
            logging.info(f"[EventBus] Cache warmed up with {len(files)} events")
        except Exception as e:
            logging.error(f"[EventBus] Cache warmup failed: {e}")
    
    def get_event_history(self, event_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """获取事件历史
        
        Args:
            event_type: 事件类型（可选，不指定则返回所有类型）
            limit: 返回数量限制
            
        Returns:
            事件历史记录列表
        """
        with self._lock:
            history = list(self._event_history)
        
        if event_type:
            history = [e for e in history if e['type'] == event_type]
        
        result = history[-limit:]
        return [{k: v for k, v in rec.items() if k != '_raw_event'} for rec in result]
    
    def clear_history(self) -> None:
        """清空事件历史"""
        with self._lock:
            self._event_history.clear()
        logging.debug("[EventBus] Event history cleared")
    
    def get_subscriber_count(self, event_type: str = None) -> int:
        """获取订阅者数量
        
        Args:
            event_type: 事件类型（可选，不指定则返回总数）
            
        Returns:
            订阅者数量
        """
        with self._lock:
            if event_type:
                return len(self._subscribers.get(event_type, []))
            else:
                return sum(len(subs) for subs in self._subscribers.values())
    
    def list_event_types(self) -> List[str]:
        """列出所有有订阅者的事件类型
        
        Returns:
            事件类型列表
        """
        with self._lock:
            return list(self._subscribers.keys())
    
    def has_subscribers(self, event_type: str) -> bool:
        """检查是否有订阅者
        
        Args:
            event_type: 事件类型
            
        Returns:
            True 如果有订阅者，否则 False
        """
        with self._lock:
            return event_type in self._subscribers and len(self._subscribers[event_type]) > 0
    
    # ✓ID唯一：get_stats统一接口，返回值含service_name="EventBus"
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息

        Returns:
            统计信息字典
        """
        # ✓P0修复: 分别获取各计数器对应的锁，避免数据竞争读到不一致值
        with self._publish_lock:
            published = self._published_count
        with self._count_lock:
            failed = self._failed_count
            dropped = self._dropped_events_count
            backpressure_dropped = self._backpressure_counter
        with self._pending_events_lock:
            pending = self._pending_events
        with self._lock:
            total_subscribers = sum(len(subs) for subs in self._subscribers.values())
            event_types = len(self._subscribers)
        return {
            'service_name': 'EventBus',  # ✓ID唯一：统一标识服务来源
            'published_events': published,
            'failed_events': failed,
            'dropped_events': dropped,
            'backpressure_dropped_events': backpressure_dropped,  # DFG-05修复: 背压丢弃计数器
            'pending_events': pending,
            'total_subscribers': total_subscribers,
            'event_types': event_types,
            'executor_stats': self.get_executor_stats(),
        }

    def get_executor_stats(self) -> Dict[str, Any]:
        """P1-3: 获取线程池监控统计

        Returns:
            线程池监控数据字典，包含队列深度、工作线程数、利用率等
        """
        if not self._executor_monitor_enabled:
            return {'monitor_enabled': False}

        try:
            _queue = getattr(self._executor, '_work_queue', None)
            queue_size = _queue.qsize() if _queue and hasattr(_queue, 'qsize') else -1
            max_workers = self._executor._max_workers if hasattr(self._executor, '_max_workers') else 0
            utilization = queue_size / max_workers if max_workers > 0 else 0.0

            if queue_size > self._executor_error_threshold:
                logging.error(
                    "[EventBus] 线程池队列深度=%d > 阈值%d，严重过载",
                    queue_size, self._executor_error_threshold,
                )
            elif queue_size > self._executor_warn_threshold:
                logging.warning(
                    "[EventBus] 线程池队列深度=%d > 阈值%d，负载过高",
                    queue_size, self._executor_warn_threshold,
                )

            stats = {
                'queue_size': queue_size,
                'max_workers': max_workers,
                'utilization': round(utilization, 4),
                'pending_events': self._pending_events,
                'warn_threshold': self._executor_warn_threshold,
                'error_threshold': self._executor_error_threshold,
            }
            self._executor_stats = stats
            return stats
        except Exception as e:
            logging.warning("[EventBus] get_executor_stats异常: %s", e)
            return {'queue_size': -1, 'error': str(e)}

    def on_publish(self, callback: Callable[[str, Any, bool], None]) -> None:
        """#139: 注册发布回调（监听所有事件发布结果）

        # R13-P2-API-09修复: 回调签名校验
        回调签名必须为callback(event_type: str, event: Any, success: bool) -> None

        Args:
            callback: 回调函数，接收(event_type, event, success)

        Example:
            def on_result(event_type, event, success):
                if not success:
                    logging.error(f"Failed to publish {event_type}")

            event_bus.on_publish(on_result)
        """
        import inspect
        try:
            sig = inspect.signature(callback)
            params = list(sig.parameters.values())
            required_count = sum(1 for p in params if p.default is inspect.Parameter.empty)
            if required_count > 3:
                logging.error(f"[EventBus] on_publish回调签名不匹配，需要3个必选参数: {callback}")
                return
        except (ValueError, TypeError) as e:
            logging.warning(f"[EventBus] on_publish回调签名检查失败: {e}")
        self._publish_callbacks.append(callback)
    
    def remove_publish_callback(self, callback: Callable[[str, Any, bool], None]) -> None:
        """移除发布回调
        
        Args:
            callback: 要移除的回调函数
        """
        if callback in self._publish_callbacks:
            self._publish_callbacks.remove(callback)
    
    def shutdown(self, wait: bool = True) -> None:
        logging.info(
            f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
            f"Shutting down (wait={wait})..."
        )
        
        pending = getattr(self, '_pending_events', 0)
        if pending > 0:
            logging.info(
                f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                f"Pending callbacks at shutdown: {pending} (drain_policy=wait-if-possible)"
            )
        
        self._shutdown = True

        # [R22-RES-P1-03] 通知订阅者shutdown，级联清理
        with self._lock:
            for _etype, _subs in list(self._subscribers.items()):
                for _cb, _pri in _subs:
                    if hasattr(_cb, '__self__') and hasattr(_cb.__self__, 'on_event_bus_shutdown'):
                        try:
                            _cb.__self__.on_event_bus_shutdown()
                        except Exception as _shutdown_err:
                            logging.warning("[R22-RES-P1-03] Subscriber shutdown notification failed: %s", _shutdown_err)

        # R13-P0-LOG-07修复: 保存最终统计数据，不在shutdown中清理
        self._final_stats = {
            'published_events': self._published_count,
            'failed_events': self._failed_count,
            'dropped_events': self._dropped_events_count,
            'pending_events': self._pending_events,
            'total_subscribers': sum(len(subs) for subs in self._subscribers.values()),
            'event_types': len(self._subscribers),
        }
        
        self._executor.shutdown(wait=wait)
        
        # 清理运行时资源，但保留统计数据
        with self._count_lock:
            self._failed_count = 0
            self._dropped_events_count = 0
        with self._pending_events_lock:
            self._pending_events = 0
        with self._lock:
            self._subscribers.clear()
            # R15-P1-LOG-20修复: shutdown保留事件历史供事后诊断，仅在reset_global_event_bus时清理
            # self._event_history.clear()
            logging.info("[EventBus] R15-P1-LOG-20: 事件历史保留(%d条)供事后诊断", len(self._event_history))
            if hasattr(self, '_persisted_events'):
                self._persisted_events.clear()
            self._published_count = 0
            self._publish_callbacks.clear()
        
        logging.info("[EventBus][owner_scope=shared-service][source_type=event-tail] " "Event bus shut down complete")

    # R13-P0-LOG-07修复: shutdown后仍可获取最终统计数据
    def get_final_stats(self) -> Dict[str, int]:
        """获取shutdown前的最终统计数据

        Returns:
            统计信息字典，即使EventBus已shutdown仍可访问
        """
        if hasattr(self, '_final_stats'):
            return dict(self._final_stats)
        # 如果尚未shutdown，返回当前统计
        return self.get_stats()
    
    def get_pending_count(self) -> int:
        with self._pending_events_lock:
            return self._pending_events

    # P2-1修复: 事件类型注册器，集中声明所有合法事件类型
    _REGISTERED_EVENT_TYPES = {
        'TickEvent', 'KLineEvent', 'SignalEvent', 'OrderEvent',
        'PositionEvent', 'RiskEvent', 'SystemEvent',
        'SubscriptionCompletedEvent', 'DataFormatChangedEvent',
        'signal.priority', 'signal.close', 'signal.compensation',
        # OPS-P1-02~05修复: 新增运维告警事件类型
        'HealthStatusEvent', 'ParamChangedEvent',
        'CircuitBreakerTriggeredEvent', 'DailyDrawdownHaltEvent',
        # OPS-P1-06~19修复: 运维操作事件类型
        'OpsOperationEvent',
        # P2修复: 补全事件类型注册 - HMM标签事件、配置更新事件、NACK事件
        'HMMTagEvent', 'ConfigUpdatedEvent',
        'SignalEvent_NACK', 'OrderEvent_NACK', 'RiskEvent_NACK',
        'TickEvent_NACK', 'KLineEvent_NACK',
    }

    def _register_event_types(self, event_types: Optional[List[str]] = None) -> None:
        """P2-1修复: 注册事件类型到事件总线

        集中声明合法事件类型，未注册的事件类型发布时输出WARNING

        Args:
            event_types: 额外要注册的事件类型列表
        """
        if event_types:
            for et in event_types:
                self._REGISTERED_EVENT_TYPES.add(et)
        logging.debug("[EventBus] 已注册事件类型: %s", self._REGISTERED_EVENT_TYPES)

    def is_event_type_registered(self, event_type: str) -> bool:
        """P2-1修复: 检查事件类型是否已注册"""
        return event_type in self._REGISTERED_EVENT_TYPES

    # P2-1修复: 数据格式版本协商
    def negotiate_format(self, producer_version: str,
                          consumer_version: str) -> Dict[str, Any]:
        """P2-1修复: 数据格式变更时的版本协商

        当生产者和消费者版本不一致时，协商出兼容的格式版本。

        Args:
            producer_version: 生产者数据格式版本
            consumer_version: 消费者支持的格式版本

        Returns:
            Dict: {compatible, negotiated_version, action}
        """
        if producer_version == consumer_version:
            return {
                'compatible': True,
                'negotiated_version': producer_version,
                'action': 'proceed',
            }
        # 简化协商，选择较低版本作为协商结果
        negotiated = min(producer_version, consumer_version)
        logging.info(
            "[EventBus] 格式协商: producer=%s consumer=%s →negotiated=%s",
            producer_version, consumer_version, negotiated,
        )
        return {
            'compatible': True,
            'negotiated_version': negotiated,
            'action': 'downgrade_to_negotiated',
        }

    # P2-13修复: 事件流重放能力
    def replay_events(self, event_type: Optional[str] = None,
                       start_time: Optional[str] = None,
                       end_time: Optional[str] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """P2-13修复: 重放历史事件

        从事件历史中筛选指定条件的事件并重放给当前订阅者。

        Args:
            event_type: 事件类型过滤（None=所有类型）
            start_time: 起始时间ISO格式（None=不限制）
            end_time: 结束时间ISO格式（None=不限制）
            limit: 最大重放数量

        Returns:
            List[Dict]: 重放的事件列表
        """
        with self._lock:
            history = list(self._event_history)

        # 按事件类型过滤
        if event_type:
            history = [e for e in history if e.get('type') == event_type]

        # 按时间范围过滤
        if start_time:
            history = [e for e in history if e.get('timestamp', '') >= start_time]
        if end_time:
            history = [e for e in history if e.get('timestamp', '') <= end_time]

        replayed = history[-limit:]
        replay_count = 0

        for event_record in replayed:
            etype = event_record.get('type', '')
            with self._lock:
                callbacks = self._subscribers.get(etype, []).copy()
            for callback, _priority in callbacks:
                try:
                    callback(event_record)
                    replay_count += 1
                except Exception as e:
                    logging.debug("[EventBus] replay callback error: %s", e)

        logging.info(
            "[EventBus] 事件重放完成: type=%s range=%s~%s replayed=%d",
            event_type or 'ALL', start_time or '*', end_time or '*', replay_count,
        )
        return replayed


# ============================================================================
# 辅助类
# ============================================================================

class RateLimiter:
    """令牌桶速率限制器
    
    特性：
    - 平滑限流
    - 支持突发流量（有令牌时）
    - 线程安全
    """
    
    def __init__(self, rate: int, capacity: int = None):
        """初始化速率限制器
        
        Args:
            rate: 每秒生成的令牌数
            capacity: 桶容量（默认等于rate）
        """
        self._rate = rate
        self._capacity = capacity or rate
        self._tokens = float(self._capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> bool:
        """获取令牌
        
        Args:
            tokens: 需要的令牌数
            
        Returns:
            bool: 是否成功获取
        """
        with self._lock:
            now = time.time()
            # 补充令牌
            elapsed = now - self._last_update
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_update = now
            
            # 检查是否有足够令牌
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False
    
    def get_available_tokens(self) -> int:
        """获取当前可用令牌数"""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            return int(tokens)


# ============================================================================
# 基础事件类
# ============================================================================

# 当前策略的主运行链路不依赖pythongo的事件类接口。
# 为避免不同pythongo版本造成误导性告警，这里统一使用本地事件定义。
_USING_PYTHONGO = False

class BaseEvent:
    """基础事件类"""
    
    def __init__(self, event_type: str = None):
        self.type = event_type or type(self).__name__
        self.timestamp = datetime.now(CHINA_TZ)
    
    def __str__(self) -> str:
        return f"{self.type}({self.timestamp})"


class TickEvent(BaseEvent):
    """Tick 数据事件"""

    def __init__(self, instrument_id: str, tick_data: Any):
        super().__init__('TickEvent')
        self.instrument_id = instrument_id
        self.tick_data = tick_data

    def __str__(self) -> str:
        return f"TickEvent(instrument={self.instrument_id}, time={self.timestamp})"


class KLineEvent(BaseEvent):
    """K线更新事件"""

    def __init__(self, instrument_id: str, period: str, kline_data: Any):
        super().__init__('KLineEvent')
        self.instrument_id = instrument_id
        self.period = period
        self.kline_data = kline_data

    def __str__(self) -> str:
        return f"KLineEvent(instrument={self.instrument_id}, period={self.period})"


class SignalEvent(BaseEvent):
    """交易信号事件"""

    def __init__(self, instrument_id: str, signal_type: str,
                 price: float = 0.0, volume: float = 0.0, reason: str = ""):
        super().__init__('SignalEvent')
        self.instrument_id = instrument_id
        self.signal_type = signal_type  # 'BUY', 'SELL', 'CLOSE_LONG', 'CLOSE_SHORT'
        self.price = price
        self.volume = volume
        self.reason = reason

    def __str__(self) -> str:
        return (f"SignalEvent(instrument={self.instrument_id}, "
                f"type={self.signal_type}, price={self.price}, volume={self.volume})")


class OrderEvent(BaseEvent):
    """订单事件"""

    def __init__(self, order_id: str, instrument_id: str,
                 action: str, volume: float, price: float = 0.0):
        super().__init__('OrderEvent')
        self.order_id = order_id
        self.instrument_id = instrument_id
        self.action = action  # 'OPEN_LONG', 'OPEN_SHORT', 'CLOSE_LONG', 'CLOSE_SHORT', 'CANCEL'
        self.volume = volume
        self.price = price

    def __str__(self) -> str:
        return (f"OrderEvent(order_id={self.order_id}, "
                f"action={self.action}, volume={self.volume})")


class PositionEvent(BaseEvent):
    """持仓变更事件"""

    def __init__(self, instrument_id: str, position: float,
                 avg_price: float, action: str):
        super().__init__('PositionEvent')
        self.instrument_id = instrument_id
        self.position = position
        self.avg_price = avg_price
        self.action = action  # 'OPENED', 'CLOSED', 'UPDATED'

    def __str__(self) -> str:
        return (f"PositionEvent(instrument={self.instrument_id}, "
                f"position={self.position}, action={self.action})")


class RiskEvent(BaseEvent):
    """风控事件"""

    def __init__(self, risk_type: str, level: str, message: str):
        super().__init__('RiskEvent')
        self.risk_type = risk_type
        self.level = level  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
        self.message = message
    
    def __str__(self) -> str:
        return f"RiskEvent(type={self.risk_type}, level={self.level})"


class SystemEvent(BaseEvent):
    """系统事件"""

    def __init__(self, system_type: str, status: str, message: str = ""):
        super().__init__('SystemEvent')
        self.system_type = system_type  # 'STARTUP', 'SHUTDOWN', 'ERROR', 'HEARTBEAT'
        self.status = status
        self.message = message

    def __str__(self) -> str:
        return f"SystemEvent(type={self.system_type}, status={self.status})"


class SubscriptionCompletedEvent(BaseEvent):
    """订阅完成事件"""

    def __init__(self, success_count: int = 0, failure_count: int = 0):
        super().__init__('SubscriptionCompletedEvent')
        self.success_count = success_count
        self.failure_count = failure_count

    def __str__(self) -> str:
        return f"SubscriptionCompletedEvent(success={self.success_count}, failed={self.failure_count})"


class DataFormatChangedEvent(BaseEvent):
    """UPG-P1-10修复: 数据格式变更事件

    当数据格式（schema版本、列定义等）发生变更时发布此事件。
    通知订阅者刷新缓存或重新加载数据。
    """

    def __init__(self, source: str, old_version: str = "",
                 new_version: str = "", affected_tables: Optional[List[str]] = None,
                 change_description: str = ""):
        super().__init__('DataFormatChangedEvent')
        self.source = source
        self.old_version = old_version
        self.new_version = new_version
        self.affected_tables = affected_tables or []
        self.change_description = change_description

    def __str__(self) -> str:
        return (f"DataFormatChangedEvent(source={self.source}, "
                f"{self.old_version}→{self.new_version}, "
                f"tables={self.affected_tables})")


# OPS-P1-02修复: 健康状态主动推送事件
class HealthStatusEvent(BaseEvent):
    """OPS-P1-02修复: 健康状态主动推送事件

    定时通过EventBus发布健康状态，替代仅被动查询的模式。
    """

    def __init__(self, overall_status: str = "", component_status: Optional[Dict[str, str]] = None,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__('HealthStatusEvent')
        self.overall_status = overall_status
        self.component_status = component_status or {}
        self.details = details or {}

    def __str__(self) -> str:
        return f"HealthStatusEvent(status={self.overall_status})"


# OPS-P1-03修复: 参数变更通知事件
class ParamChangedEvent(BaseEvent):
    """OPS-P1-03修复: 参数变更通知事件

    参数变更时通过EventBus发布通知，替代仅日志记录的模式。
    """

    def __init__(self, key: str = "", old_value: Any = None, new_value: Any = None,
                 source: str = "", changed_keys: Optional[List[str]] = None):
        super().__init__('ParamChangedEvent')
        self.key = key
        self.old_value = old_value
        self.new_value = new_value
        self.source = source
        self.changed_keys = changed_keys or [key]

    def __str__(self) -> str:
        return f"ParamChangedEvent(key={self.key}, {self.old_value}→{self.new_value})"


# OPS-P1-04修复: 断路器触发告警事件
class CircuitBreakerTriggeredEvent(BaseEvent):
    """OPS-P1-04修复: 断路器触发告警事件

    断路器触发时通过EventBus发布告警，替代仅日志记录的模式。
    """

    def __init__(self, reason: str = "", drop_pct: float = 0.0,
                 threshold: float = 0.0, pause_duration: float = 0.0,
                 calm_period: float = 0.0):
        super().__init__('CircuitBreakerTriggeredEvent')
        self.reason = reason
        self.drop_pct = drop_pct
        self.threshold = threshold
        self.pause_duration = pause_duration
        self.calm_period = calm_period

    def __str__(self) -> str:
        return f"CircuitBreakerTriggeredEvent(reason={self.reason}, drop={self.drop_pct:.2%})"


# OPS-P1-05修复: 日回撤硬停止告警事件
class DailyDrawdownHaltEvent(BaseEvent):
    """OPS-P1-05修复: 日回撤硬停止告警事件

    日回撤硬停止触发时通过EventBus发布告警，替代仅日志记录的模式。
    """

    def __init__(self, drawdown_pct: float = 0.0, threshold_pct: float = 0.0,
                 current_loss: float = 0.0, max_daily_loss: float = 0.0,
                 trigger_type: str = ""):
        super().__init__('DailyDrawdownHaltEvent')
        self.drawdown_pct = drawdown_pct
        self.threshold_pct = threshold_pct
        self.current_loss = current_loss
        self.max_daily_loss = max_daily_loss
        self.trigger_type = trigger_type

    def __str__(self) -> str:
        return f"DailyDrawdownHaltEvent(drawdown={self.drawdown_pct:.2%})"


# OPS-P1-06~19修复: 运维操作事件
class OpsOperationEvent(BaseEvent):
    """OPS-P1-06~19修复: 运维操作生命周期事件

    运维操作开始、完成/失败/回滚时通过EventBus发布通知。
    """

    def __init__(self, operation_id: str = "", operation_type: str = "",
                 phase: str = "", status: str = "", message: str = "",
                 details: Optional[Dict[str, Any]] = None):
        super().__init__('OpsOperationEvent')
        self.operation_id = operation_id
        self.operation_type = operation_type
        self.phase = phase  # started/completed/failed/rolled_back
        self.status = status
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        return f"OpsOperationEvent(id={self.operation_id}, type={self.operation_type}, phase={self.phase})"


# ============================================================================
# 全局事件总线实例（可选）
# ============================================================================

# 全局事件总线单例
_global_event_bus: Optional[EventBus] = None
_global_event_bus_lock = threading.Lock()


def safe_import_event_bus():
    """接口唯一修复：委托get_global_event_bus，返回EventBus实例而非模块"""
    try:
        return get_global_event_bus()
    except Exception:
        return None


def get_global_event_bus() -> EventBus:
    """获取全局事件总线单例（线程安全）
    
    Returns:
        全局 EventBus 实例
    """
    global _global_event_bus
    with _global_event_bus_lock:
        if _global_event_bus is None:
            _global_event_bus = EventBus()
        return _global_event_bus


def reset_global_event_bus() -> None:
    """重置全局事件总线（主要用于测试）"""
    global _global_event_bus
    with _global_event_bus_lock:
        if _global_event_bus is not None:
            _global_event_bus.shutdown(wait=False)
        _global_event_bus = None


# ============================================================================
# P2修复: 数据流图 - 生产者/消费者装饰器、性能监控、异常检测
# ============================================================================

# P2修复: 数据流图文档注释
# 数据流路径
# 1. TickEvent: MarketDataService → EventBus → SignalService (关键路径)
# 2. KLineEvent: MarketDataService → EventBus → SignalService (关键路径)
# 3. SignalEvent: SignalService → EventBus → RiskService → OrderService (关键路径)
# 4. OrderEvent: OrderService → EventBus → PositionService (关键路径)
# 5. PositionEvent: PositionService → EventBus → RiskDashboardService
# 6. RiskEvent: RiskService → EventBus → RiskDashboardService, OpsService
# 7. SystemEvent: 各服务 → EventBus → HealthMonitor
# 8. DataFormatChangedEvent: DsSchemaManager → EventBus → 各数据消费者
# 9. HealthStatusEvent: MaintenanceService → EventBus → OpsMonitor
# 10. ParamChangedEvent: ParamsService → EventBus → 各参数消费者
# 11. CircuitBreakerTriggeredEvent: RiskService → EventBus → OpsService
# 12. DailyDrawdownHaltEvent: RiskService → EventBus → OpsService

# P2修复: 生产者/消费者关系显式声明
_PRODUCER_CONSUMER_REGISTRY: Dict[str, Dict[str, List[str]]] = {
    'TickEvent': {
        'producers': ['MarketDataService'],
        'consumers': ['SignalService'],
    },
    'KLineEvent': {
        'producers': ['MarketDataService'],
        'consumers': ['SignalService'],
    },
    'SignalEvent': {
        'producers': ['SignalService'],
        'consumers': ['RiskService', 'OrderService'],
    },
    'OrderEvent': {
        'producers': ['OrderService'],
        'consumers': ['PositionService'],
    },
    'PositionEvent': {
        'producers': ['PositionService'],
        'consumers': ['RiskDashboardService'],
    },
    'RiskEvent': {
        'producers': ['RiskService'],
        'consumers': ['RiskDashboardService', 'OpsService'],
    },
    'SystemEvent': {
        'producers': ['*'],
        'consumers': ['HealthMonitor'],
    },
    'DataFormatChangedEvent': {
        'producers': ['DsSchemaManager'],
        'consumers': ['*'],
    },
    'HealthStatusEvent': {
        'producers': ['MaintenanceService'],
        'consumers': ['OpsMonitor'],
    },
    'ParamChangedEvent': {
        'producers': ['ParamsService'],
        'consumers': ['*'],
    },
    'CircuitBreakerTriggeredEvent': {
        'producers': ['RiskService'],
        'consumers': ['OpsService'],
    },
    'DailyDrawdownHaltEvent': {
        'producers': ['RiskService'],
        'consumers': ['OpsService'],
    },
}


def producer(event_type: str):
    """P2修复: 生产者装饰器 - 声明函数是某事件类型的生产者

    用法:
        @producer('SignalEvent')
        def generate_signal(self, ...):
            event_bus.publish(SignalEvent(...))
    """
    def decorator(func):
        func._producer_of = event_type
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper._producer_of = event_type
        return wrapper
    return decorator


def consumer(event_type: str):
    """P2修复: 消费者装饰器 - 声明函数是某事件类型的消费者

    用法:
        @consumer('SignalEvent')
        def on_signal(self, event):
            ...
    """
    def decorator(func):
        func._consumer_of = event_type
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper._consumer_of = event_type
        return wrapper
    return decorator


def get_producer_consumer_registry() -> Dict[str, Dict[str, List[str]]]:
    """P2修复: 获取生产者/消费者注册表"""
    return dict(_PRODUCER_CONSUMER_REGISTRY)


# P2修复: 数据流性能监控计数器
_dataflow_perf_counters: Dict[str, Dict[str, Any]] = {}
_dataflow_perf_lock = threading.Lock()


def record_dataflow_latency(event_type: str, latency_ms: float) -> None:
    """P2修复: 记录数据流延迟

    Args:
        event_type: 事件类型
        latency_ms: 延迟毫秒数
    """
    with _dataflow_perf_lock:
        if event_type not in _dataflow_perf_counters:
            _dataflow_perf_counters[event_type] = {
                'count': 0, 'total_ms': 0.0,
                'min_ms': float('inf'), 'max_ms': 0.0,
                'p99_ms': 0.0, '_samples': deque(maxlen=1000),
            }
        counter = _dataflow_perf_counters[event_type]
        counter['count'] += 1
        counter['total_ms'] += latency_ms
        counter['min_ms'] = min(counter['min_ms'], latency_ms)
        counter['max_ms'] = max(counter['max_ms'], latency_ms)
        counter['_samples'].append(latency_ms)
        # 计算P99
        if len(counter['_samples']) >= 100:
            sorted_samples = sorted(counter['_samples'])
            p99_idx = int(len(sorted_samples) * 0.99)
            counter['p99_ms'] = sorted_samples[min(p99_idx, len(sorted_samples) - 1)]


def get_dataflow_perf_stats() -> Dict[str, Dict[str, Any]]:
    """P2修复: 获取数据流性能统计

    Returns:
        Dict: 每种事件类型的性能统计
    """
    with _dataflow_perf_lock:
        result = {}
        for etype, counter in _dataflow_perf_counters.items():
            result[etype] = {
                'count': counter['count'],
                'avg_ms': counter['total_ms'] / max(counter['count'], 1),
                'min_ms': counter['min_ms'] if counter['min_ms'] != float('inf') else 0.0,
                'max_ms': counter['max_ms'],
                'p99_ms': counter['p99_ms'],
            }
        return result


# P2修复: 数据流异常检测
_dataflow_anomaly_config: Dict[str, Any] = {
    'max_latency_ms': 5000.0,  # 单次延迟超过5秒视为异常
    'max_drop_rate': 0.1,      # 丢弃率超过10%视为异常
    'min_throughput_per_min': 1,  # 每分钟至少1个事件（关键路径）
}
_dataflow_anomaly_events: deque = deque(maxlen=1000)
_dataflow_anomaly_lock = threading.Lock()


def detect_dataflow_anomaly(event_type: str, latency_ms: float = 0.0,
                             success: bool = True) -> Optional[Dict[str, Any]]:
    """P2修复: 检测数据流异常

    Args:
        event_type: 事件类型
        latency_ms: 延迟毫秒数
        success: 事件是否成功处理

    Returns:
        异常信息字典，无异常返回None
    """
    anomaly = None

    # 检查延迟异常
    if latency_ms > _dataflow_anomaly_config['max_latency_ms']:
        anomaly = {
            'event_type': event_type,
            'anomaly_type': 'high_latency',
            'latency_ms': latency_ms,
            'threshold_ms': _dataflow_anomaly_config['max_latency_ms'],
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
        }

    # 检查丢弃率异常
    if not success:
        stats = get_dataflow_perf_stats()
        if event_type in stats:
            # 简化，通过EventBus统计检查丢弃率
            try:
                bus = get_global_event_bus()
                bus_stats = bus.get_stats()
                total = bus_stats.get('published_events', 1)
                dropped = bus_stats.get('dropped_events', 0)
                if total > 0 and (dropped / total) > _dataflow_anomaly_config['max_drop_rate']:
                    anomaly = {
                        'event_type': event_type,
                        'anomaly_type': 'high_drop_rate',
                        'drop_rate': dropped / total,
                        'threshold': _dataflow_anomaly_config['max_drop_rate'],
                        'timestamp': datetime.now(CHINA_TZ).isoformat(),
                    }
            except Exception:
                pass

    if anomaly is not None:
        with _dataflow_anomaly_lock:
            _dataflow_anomaly_events.append(anomaly)
        logging.warning(
            "[EventBus] P2修复: 数据流异常检测type=%s anomaly=%s",
            event_type, anomaly.get('anomaly_type'),
        )

    return anomaly


def get_dataflow_anomalies(limit: int = 100) -> List[Dict[str, Any]]:
    """P2修复: 获取数据流异常记录

    Args:
        limit: 返回数量限制

    Returns:
        异常记录列表
    """
    with _dataflow_anomaly_lock:
        return list(_dataflow_anomaly_events)[-limit:]
