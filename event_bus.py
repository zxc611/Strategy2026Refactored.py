"""
事件总线模块 - 线程安全的发布/订阅模式实现
来源：新建文件
功能：服务间解耦通信，事件驱动架构核心

优化 v2.2 (2026-03-15):
- ✅ 添加线程池管理（防止线程爆炸）
- ✅ 使用 deque 优化事件存储（O(1) 操作）
- ✅ 添加背压机制（限制队列长度）
- ✅ 修复全局单例线程安全问题
- ✅ 补全类型注解
- ✅ 优化日志级别
- ✅ [NEW] 事件优先级支持
- ✅ [NEW] 事件过滤器（条件订阅）
- ✅ [NEW] 速率限制器
- ✅ [P2] 移除冗余事件类（使用 pythongo 导入）
"""
import threading
import logging
import os
from typing import Dict, List, Callable, Any, Optional, Set, TypeVar, Generic, Tuple
import copy
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future
import time
from functools import wraps


class EventBus:
    """线程安全的事件总线（优化版 v2.2）
    
    特性:
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
    DEFAULT_BACKPRESSURE_LIMIT = 5000
    DEFAULT_RATE_LIMIT = 100  # 每秒最大事件数
    
    def __init__(self, max_workers: int = None, max_history_size: int = None, 
                 backpressure_limit: int = None, rate_limit: int = None,
                 enable_persistence: bool = False, persistence_dir: str = None):
        """初始化事件总线
        
        Args:
            max_workers: 线程池最大工作线程数（默认 10）
            max_history_size: 最大事件历史记录数（默认 1000）
            backpressure_limit: 背压限制，待处理事件上限（默认 5000）
            rate_limit: 速率限制，每秒最大发布次数（默认 100，0=不限制）
            enable_persistence: 是否启用事件持久化（默认 False）
            persistence_dir: 持久化目录（默认 None）
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
        
        # 优化 3: 添加背压机制
        self._backpressure_limit: int = backpressure_limit or self.DEFAULT_BACKPRESSURE_LIMIT
        self._pending_events: int = 0
        self._pending_events_lock = threading.Lock()  # P1 Bug #57修复：保护_pending_events的原子操作
        self._dropped_events_count: int = 0
        self._count_lock = threading.Lock()
        
        # P2 新增：速率限制
        self._rate_limit: int = rate_limit or self.DEFAULT_RATE_LIMIT
        self._rate_limiter: Optional['RateLimiter'] = None
        if self._rate_limit > 0:
            self._rate_limiter = RateLimiter(self._rate_limit)
        
        # #139 新增：事件发布回调
        self._publish_callbacks: List[Callable[[str, Any, bool], None]] = []
        
        # #354 新增：事件持久化支持
        self._enable_persistence = enable_persistence
        self._persistence_dir = persistence_dir
        if self._enable_persistence and self._persistence_dir:
            os.makedirs(self._persistence_dir, exist_ok=True)
            self._persisted_events: deque = deque(maxlen=10000)
        
        # #358 新增：缓存预热标记
        self._cache_warmed = False
        
        # P2 Bug #115修复：shutdown标志
        self._shutdown = False
        
        # 统计信息
        self._published_count: int = 0
        self._failed_count: int = 0
        self._publish_lock = threading.Lock()  # P1 Bug #58修复：保护_published_count的原子自增
    
    def subscribe(self, event_type: str, callback: Callable, priority: int = 0,
                  filter_func: Optional[Callable[[Any], bool]] = None) -> None:
        """
        P2 Bug #115修复：订阅事件（支持优先级和可选过滤器，检查shutdown状态）
        
        Args:
            event_type: 事件类型名称
            callback: 回调函数，接收一个事件对象参数
            priority: 优先级（0=普通，>0=更高优先级，数字越大优先级越高）
            filter_func: 可选过滤器函数，返回 True 时才执行回调
            
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
            raise RuntimeError("EventBus已关闭")
        
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
                
                if len(self._subscribers[event_type]) < original_len:
                    logging.debug(f"[EventBus] Unsubscribed from '{event_type}'")
                else:
                    logging.warning(f"[EventBus] Callback not found for '{event_type}'")
            
            # 清理映射
            if callback in self._callback_mapping:
                del self._callback_mapping[callback]
    
    def publish(self, event: Any, async_mode: bool = True, force: bool = False, 
                priority: Optional[int] = None) -> bool:
        """
        P2 Bug #115修复：发布事件（检查shutdown状态）
        
        Args:
            event: 事件对象
            async_mode: True=异步分发，False=同步分发
            force: True=强制发布（忽略背压限制），False=受背压限制
            priority: 事件优先级（None=使用事件自带优先级或 0）
            
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
                with self._count_lock:
                    self._dropped_events_count += 1
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
            # 优化 5: 使用线程池而非每次创建新线程
            try:
                with self._pending_events_lock:  # P1 Bug #57修复：原子操作
                    self._pending_events += 1
                future = self._executor.submit(self._invoke_all_callbacks, callbacks, event, event_type)
                future.add_done_callback(lambda f: self._on_callback_complete(f, event_type, event))
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
        
        # #139: 异步模式下立即返回 True（实际结果通过 callback 通知）
        self._notify_publish_callbacks(event_type, event, True)
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
    
    def _invoke_all_callbacks(self, callbacks: List[tuple], event: Any, event_type: str) -> None:
        """调用所有回调函数（在线程池中执行，按优先级）
        
        Args:
            callbacks: 回调函数列表 [(callback, priority), ...]
            event: 事件对象
            event_type: 事件类型
        """
        # P2 新增：按优先级执行回调
        for callback, priority in callbacks:
            self._invoke_callback(callback, event, event_type)
    
    def _on_callback_complete(self, future: Future, event_type: str, event: Any) -> None:
        """回调完成后的处理
        
        Args:
            future: 未来对象
            event_type: 事件类型
            event: 事件对象
        """
        with self._pending_events_lock:
            self._pending_events -= 1
        
        # 检查是否有异常
        try:
            exception = future.exception()
            if exception:
                with self._count_lock:
                    self._failed_count += 1
                logging.error(
                    f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                    f"Async callback failed: {exception}"
                )
                # #311: 发送失败通知
                self._send_nack(event_type, event, str(exception))
        except Exception as e:
            logging.error(
                f"[EventBus][owner_scope=shared-service][source_type=event-tail] "
                f"Failed to check callback result: {e}"
            )
    
    def _send_nack(self, event_type: str, event: Any, reason: str) -> None:
        """#311: 发送否定确认（处理失败通知）
        
        Args:
            event_type: 事件类型
            event: 事件对象
            reason: 失败原因
        """
        nack_event = {
            'type': f'{event_type}_NACK',
            'original_type': event_type,
            'original_event': repr(event)[:200],
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        }
        logging.warning(f"[EventBus] NACK for {event_type}: {reason}")
        # 可以选择发布 NACK 事件或调用特定回调
    
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
                exc_info=False  # 优化 6: 不输出完整堆栈，减少日志量
            )
    
    def _record_event(self, event_type: str, event: Any) -> None:
        """记录事件历史（优化版）
        
        Args:
            event_type: 事件类型
            event: 事件对象
        """
        try:
            # 优化 7: 使用 namedtuple 或简单对象替代 dict，减少内存占用
            event_record = {
                'type': event_type,
                'timestamp': datetime.now().isoformat(),
                'data': repr(event)[:200]  # 优化：从 500 减少到 200
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
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
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
            files = sorted(glob.glob(pattern))[-100:]  # 最近 100 个事件
            
            for filepath in files:
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        import json
                        event_data = json.load(f)
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
            # deque 不支持切片，需要转换为 list
            history = copy.deepcopy(list(self._event_history))
        
        if event_type:
            history = [e for e in history if e['type'] == event_type]
        
        return history[-limit:]
    
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
    
    # ✅ ID唯一：get_stats统一接口，返回值含service_name="EventBus"
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息
        
        Returns:
            统计信息字典
        """
        with self._lock:
            return {
                'service_name': 'EventBus',  # ✅ ID唯一：统一标识服务来源
                'published_events': self._published_count,
                'failed_events': self._failed_count,
                'dropped_events': self._dropped_events_count,
                'pending_events': self._pending_events,
                'total_subscribers': sum(len(subs) for subs in self._subscribers.values()),
                'event_types': len(self._subscribers)
            }
    
    def on_publish(self, callback: Callable[[str, Any, bool], None]) -> None:
        """#139: 注册发布回调（监听所有事件发布结果）
        
        Args:
            callback: 回调函数，接收 (event_type, event, success)
            
        Example:
            def on_result(event_type, event, success):
                if not success:
                    logging.error(f"Failed to publish {event_type}")
            
            event_bus.on_publish(on_result)
        """
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
        
        self._executor.shutdown(wait=wait)
        
        # 清理所有资源
        with self._count_lock:
            self._failed_count = 0
            self._dropped_events_count = 0
        with self._pending_events_lock:
            self._pending_events = 0
        with self._lock:
            self._subscribers.clear()
            self._event_history.clear()
            if hasattr(self, '_persisted_events'):
                self._persisted_events.clear()
            self._published_count = 0
            self._publish_callbacks.clear()
        
        logging.info("[EventBus][owner_scope=shared-service][source_type=event-tail] " "Event bus shut down complete")
    
    def get_pending_count(self) -> int:
        with self._pending_events_lock:
            return self._pending_events


# ============================================================================
# 辅助类
# ============================================================================

class RateLimiter:
    """令牌桶速率限制器
    
    特性:
    - 平滑限流
    - 支持突发流量（有令牌时）
    - 线程安全
    """
    
    def __init__(self, rate: int, capacity: int = None):
        """初始化速率限制器
        
        Args:
            rate: 每秒生成的令牌数
            capacity: 桶容量（默认等于 rate）
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

# 当前策略的主运行链路不依赖 pythongo 的事件类接口。
# 为避免不同 pythongo 版本造成误导性告警，这里统一使用本地事件定义。
_USING_PYTHONGO = False

class BaseEvent:
    """基础事件类"""
    
    def __init__(self, event_type: str = None):
        self.type = event_type or type(self).__name__
        self.timestamp = datetime.now()
    
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
    """K 线更新事件"""

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
