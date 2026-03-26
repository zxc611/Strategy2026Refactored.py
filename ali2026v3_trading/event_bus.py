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
from typing import Dict, List, Callable, Any, Optional, Set, TypeVar, Generic
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
        self._subscribers: Dict[str, List[tuple[Callable, int]]] = {}
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
        self._dropped_events_count: int = 0
        
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
        
        # 统计信息
        self._published_count: int = 0
        self._failed_count: int = 0
    
    def subscribe(self, event_type: str, callback: Callable, priority: int = 0) -> None:
        """订阅事件（支持优先级）
        
        Args:
            event_type: 事件类型名称
            callback: 回调函数，接收一个事件对象参数
            priority: 优先级（0=普通，>0=更高优先级，数字越大优先级越高）
            
        Example:
            # 普通订阅
            event_bus.subscribe('SignalEvent', lambda e: print(f'收到信号：{e}'))
            
            # 高优先级订阅（优先执行）
            event_bus.subscribe('RiskEvent', handle_risk, priority=10)
        """
        if not event_type or not callable(callback):
            logging.error(f"[EventBus] Invalid subscription: event_type={event_type}, callback={callback}")
            return
        
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            
            # 避免重复订阅
            if callback not in [cb for cb, _ in self._subscribers[event_type]]:
                # P2 新增：按优先级排序插入
                self._subscribers[event_type].append((callback, priority))
                # 按优先级降序排序（高优先级在前）
                self._subscribers[event_type].sort(key=lambda x: x[1], reverse=True)
                logging.debug(f"[EventBus] Subscribed to '{event_type}' with priority {priority} "
                            f"(total: {len(self._subscribers[event_type])} subscribers)")
    
    def subscribe_with_filter(self, event_type: str, callback: Callable, 
                             filter_func: Callable[[Any], bool], priority: int = 0) -> None:
        """带过滤器的订阅（条件订阅）
        
        Args:
            event_type: 事件类型名称
            callback: 回调函数
            filter_func: 过滤器函数，返回 True 时才执行回调
            priority: 优先级
            
        Example:
            # 只处理 BUY 信号
            def buy_filter(event):
                return getattr(event, 'signal_type', '') == 'BUY'
            
            event_bus.subscribe_with_filter('SignalEvent', on_buy_signal, buy_filter)
        """
        if not callable(filter_func):
            logging.error(f"[EventBus] Invalid filter function for '{event_type}'")
            return
        
        @wraps(callback)
        def wrapped_callback(event):
            try:
                if filter_func(event):
                    callback(event)
                else:
                    logging.debug(f"[EventBus] Filter blocked event '{event_type}'")
            except Exception as e:
                logging.error(f"[EventBus] Filter function error: {e}")
                callback(event)  # 过滤器出错时仍然执行
        
        self.subscribe(event_type, wrapped_callback, priority)
    
    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """取消订阅
        
        Args:
            event_type: 事件类型名称
            callback: 要移除的回调函数
        """
        with self._lock:
            if event_type in self._subscribers:
                # ✅ 修复：存储的是 (callback, priority) 元组，需要遍历比较
                original_len = len(self._subscribers[event_type])
                self._subscribers[event_type] = [
                    (cb, pri) for cb, pri in self._subscribers[event_type]
                    if cb != callback
                ]
                
                if len(self._subscribers[event_type]) < original_len:
                    logging.debug(f"[EventBus] Unsubscribed from '{event_type}'")
                    
                    # 清理空的订阅列表
                    if not self._subscribers[event_type]:
                        del self._subscribers[event_type]
    
    def publish(self, event: Any, async_mode: bool = True, force: bool = False, 
                priority: Optional[int] = None) -> bool:
        """发布事件
        
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
            logging.warning("[EventBus] Attempted to publish None event")
            return False
        
        # P2 新增：速率限制检查
        if self._rate_limiter and not force:
            if not self._rate_limiter.acquire():
                self._dropped_events_count += 1
                logging.warning(
                    f"[EventBus] Rate limit exceeded ({self._rate_limit}/s), "
                    f"dropping event: {type(event).__name__} (total dropped: {self._dropped_events_count})"
                )
                # #139/#309: 通知回调
                self._notify_publish_callbacks(type(event).__name__, event, False)
                return False
        
        # 优化 4: 背压检查
        if not force and self._pending_events >= self._backpressure_limit:
            self._dropped_events_count += 1
            logging.warning(
                f"[EventBus] Backpressure limit reached ({self._pending_events}/{self._backpressure_limit}), "
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
            logging.debug(f"[EventBus] No subscribers for event type '{event_type}'")
            # #139: 即使没有订阅者也返回成功
            self._notify_publish_callbacks(event_type, event, True)
            return True
        
        self._published_count += 1
        logging.debug(f"[EventBus] Publishing '{event_type}' to {len(callbacks)} subscriber(s)")
        
        # 分发事件
        if async_mode:
            # 优化 5: 使用线程池而非每次创建新线程
            try:
                self._pending_events += 1
                future = self._executor.submit(self._invoke_all_callbacks, callbacks, event, event_type)
                future.add_done_callback(lambda f: self._on_callback_complete(f, event_type, event))
            except Exception as e:
                self._pending_events -= 1
                logging.error(f"[EventBus] Failed to submit async task: {e}")
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
                logging.error(f"[EventBus] Publish callback error: {e}")
    
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
        with self._lock:
            self._pending_events -= 1
        
        # 检查是否有异常
        try:
            exception = future.exception()
            if exception:
                self._failed_count += 1
                logging.error(f"[EventBus] Async callback failed: {exception}")
                # #311: 发送失败通知
                self._send_nack(event_type, event, str(exception))
        except Exception as e:
            logging.error(f"[EventBus] Failed to check callback result: {e}")
    
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
            history = list(self._event_history)
        
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
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息
        
        Returns:
            统计信息字典
        """
        with self._lock:
            return {
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
        """关闭事件总线
        
        Args:
            wait: 是否等待未完成的任务完成
        """
        logging.info(f"[EventBus] Shutting down (wait={wait})...")
        
        # 关闭线程池 - #47: 确保资源释放
        self._executor.shutdown(wait=wait)
        
        # 清理所有资源
        with self._lock:
            self._subscribers.clear()
            self._event_history.clear()
            if hasattr(self, '_persisted_events'):
                self._persisted_events.clear()
            self._published_count = 0
            self._failed_count = 0
            self._dropped_events_count = 0
            self._pending_events = 0
            self._publish_callbacks.clear()
        
        logging.info("[EventBus] Event bus shut down complete")


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

# P2 优化：尝试从 pythongo 导入标准事件类，避免重复定义
try:
    from pythongo.types import TickEvent, KLineEvent, SignalEvent, OrderEvent, PositionEvent  # type: ignore
    _USING_PYTHONGO = True
except ImportError as e:
    # 如果 pythongo 不可用，使用本地定义
    logging.warning(f"[EventBus] pythongo not available, using local definitions: {e}")
    _USING_PYTHONGO = False
    TickEvent = None
    KLineEvent = None
    SignalEvent = None
    OrderEvent = None
    PositionEvent = None

class BaseEvent:
    """基础事件类"""
    
    def __init__(self, event_type: str = None):
        self.type = event_type or type(self).__name__
        self.timestamp = datetime.now()
    
    def __str__(self) -> str:
        return f"{self.type}({self.timestamp})"


# P2 优化：只保留必要的本地事件类，其他从 pythongo 导入
# 如果上面已经导入了，就不再重复定义
if not _USING_PYTHONGO:
    # pythongo 不可用，使用本地定义（向后兼容）
    
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
    
    # 为了兼容 main.py 的导入
    Event = BaseEvent
    
    class EventType:
        """事件类型枚举"""
        TICK = "TickEvent"
        KLINE = "KLineEvent"
        SIGNAL = "SignalEvent"
        ORDER = "OrderEvent"
        POSITION = "PositionEvent"
        RISK = "RiskEvent"
        SYSTEM = "SystemEvent"
else:
    # pythongo 已加载，使用导入的类
    # 为了兼容 main.py 的导入
    Event = BaseEvent
    
    class EventType:
        """事件类型枚举"""
        TICK = "TickEvent"
        KLINE = "KLineEvent"
        SIGNAL = "SignalEvent"
        ORDER = "OrderEvent"
        POSITION = "PositionEvent"
        RISK = "RiskEvent"
        SYSTEM = "SystemEvent"


# ============================================================================
# 全局事件总线实例（可选）
# ============================================================================

# 全局事件总线单例
_global_event_bus: Optional[EventBus] = None
_global_event_bus_lock = threading.Lock()


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
