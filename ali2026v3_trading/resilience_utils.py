"""
resilience_utils.py — R27-P1+P2修复: 通用容错/退避/看门狗/断路器/浮点精度辅助模块

聚合所有P1降级容错(DR-P1-01~24)、竞态条件(RC-P1-01~09)、浮点精度(FP-P1-01~08)修复所需的工具。
P2修复: 降级日志级别(DR-P2-01~04)、熔断仪表盘(DR-P2-05~07)、看门狗配置外置(DR-P2-08~11)、
         日志线程安全(RC-P2-01~07)、确定性舍入(FP-P2-01~12)、float→int安全转换(FP-P2-13~17)、
         兼容性桥接(FC-P2-01~03)、@deprecated标记(FC-P2-04~06)
"""

import math
import time
import threading
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque
from functools import wraps

logger = logging.getLogger(__name__)

# R27-P1-FP-01修复: 浮点容差常量（全项目统一）
PRICE_TOLERANCE = 1e-6
FLOAT_COMPARE_TOLERANCE = 1e-9

# R27-P2-FP-01修复: 确定性舍入常量（全项目统一显示精度）
PRICE_DISPLAY_DP = 4
RATIO_DISPLAY_DP = 4
GREEKS_DISPLAY_DP = 6
STATS_DISPLAY_DP = 2
PERCENTAGE_DISPLAY_DP = 2

# R27-P2-RC-01修复: ThreadLocal日志格式化缓冲
_log_format_local = threading.local()


# R27-P2-FP-01修复: 确定性舍入（替代round()银行家舍入）
def deterministic_round(value: float, ndigits: int = 0) -> float:
    """确定性舍入：使用math.floor替代round()避免银行家舍入歧义"""
    multiplier = 10 ** ndigits
    return math.floor(value * multiplier + 0.5) / multiplier


# R27-P2-FP-13修复: float→int安全转换（先舍入再截断）
def safe_float_to_int(value: float) -> int:
    """float→int安全转换：先四舍五入再转int，避免截断偏差"""
    return int(math.floor(value + 0.5))


# R27-P2-RC-01修复: 线程安全日志格式化
def thread_safe_log(logger_instance: logging.Logger, level: int, msg: str, *args) -> None:
    """线程安全日志：使用ThreadLocal缓冲区格式化，避免多线程交错"""
    try:
        formatted = msg % args if args else msg
    except (TypeError, ValueError):
        formatted = msg
    setattr(_log_format_local, 'last_msg', formatted)
    logger_instance.log(level, formatted)


# R27-P2-FC-04修复: @deprecated装饰器
def deprecated(reason: str = '') -> Callable:
    """标记已废弃函数/类，使用时发出DeprecationWarning"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            import warnings
            warnings.warn(
                f"{func.__name__} is deprecated. {reason}",
                category=DeprecationWarning,
                stacklevel=2
            )
            return func(*args, **kwargs)
        wrapper._deprecated = True
        wrapper._deprecated_reason = reason
        return wrapper
    return decorator


# ============================================================================
# DR-P1-01修复: 指数退避算法（替代固定间隔重试）
# ============================================================================
class ExponentialBackoff:
    """指数退避重试策略，支持jitter防雷群效应"""

    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0,
                 max_retries: int = 5, jitter: bool = True):
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._max_retries = max_retries
        self._jitter = jitter
        self._attempt = 0

    def next_delay(self) -> Optional[float]:
        if self._attempt >= self._max_retries:
            return None
        delay = min(self._base_delay * (2 ** self._attempt), self._max_delay)
        if self._jitter:
            import random
            delay = delay * (0.5 + random.random() * 0.5)
        self._attempt += 1
        return delay

    def reset(self) -> None:
        self._attempt = 0

    @property
    def attempt(self) -> int:
        return self._attempt

    @property
    def exhausted(self) -> bool:
        return self._attempt >= self._max_retries


# ============================================================================
# DR-P1-02修复: tick处理超时保护
# ============================================================================
class TimeoutGuard:
    """超时守卫：在指定时间内执行函数，超时则返回默认值"""

    def __init__(self, timeout_sec: float, default_result: Any = None):
        self._timeout = timeout_sec
        self._default = default_result

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        result = [self._default]
        error = [None]

        def _target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=_target, daemon=True)
        t.start()
        t.join(timeout=self._timeout)
        if t.is_alive():
            logging.warning("[DR-P1-02] tick处理超时(%.1fs), 函数=%s", self._timeout, func.__name__)
            # R23-P2-17修复: join超时后检查线程状态
            if t.is_alive():
                logging.warning("[R23-P2-17] 超时线程未终止: %s", func.__name__)
            return self._default
        if error[0] is not None:
            raise error[0]
        return result[0]


# ============================================================================
# DR-P1-03修复: 数据库写入重试最大次数控制
# ============================================================================
class BoundedRetry:
    """有界重试：固定最大次数，超限后丢弃并记录"""

    def __init__(self, max_retries: int = 3, base_delay: float = 0.1,
                 max_delay: float = 5.0):
        self._max_retries = max_retries
        self._backoff = ExponentialBackoff(base_delay, max_delay, max_retries, jitter=False)
        self._total_retries = 0
        self._total_failures = 0
        self._lock = threading.Lock()

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        self._backoff.reset()
        for i in range(self._max_retries):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result
            except Exception as e:
                # R27-P2-DR-01修复: 重试异常日志从debug提升到warning
                logging.warning("[DR-P1-03] 重试%d/%d异常: %s", i + 1, self._max_retries, e)
            delay = self._backoff.next_delay()
            if delay and i < self._max_retries - 1:
                time.sleep(delay)
        with self._lock:
            self._total_failures += 1
        logging.critical("[DR-P1-03] 有界重试耗尽(%d次), 函数=%s", self._max_retries, getattr(func, '__name__', '?'))
        return None

    @property
    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {'total_retries': self._total_retries, 'total_failures': self._total_failures}


# ============================================================================
# DR-P1-04修复: 策略崩溃后自动重启
# ============================================================================
class StrategyRestartGuard:
    """策略崩溃重启守卫：捕获异常后自动重启，带最大重启次数"""

    def __init__(self, max_restarts: int = 3, restart_delay_sec: float = 5.0,
                 restart_func: Optional[Callable] = None):
        self._max_restarts = max_restarts
        self._restart_delay = restart_delay_sec
        self._restart_func = restart_func
        self._restart_count = 0
        self._lock = threading.Lock()

    def run_with_restart(self, func: Callable, *args, **kwargs) -> Any:
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                with self._lock:
                    self._restart_count += 1
                    if self._restart_count > self._max_restarts:
                        logging.critical(
                            "[DR-P1-04] 策略崩溃重启耗尽(%d次), 最后异常: %s",
                            self._max_restarts, e
                        )
                        raise
                    current_count = self._restart_count
                logging.warning(
                    "[DR-P1-04] 策略崩溃(异常=%s), %0.1fs后第%d次重启(上限%d)",
                    type(e).__name__, self._restart_delay, current_count, self._max_restarts
                )
                time.sleep(self._restart_delay)
                if self._restart_func:
                    try:
                        self._restart_func()
                    except Exception as re_err:
                        logging.error("[DR-P1-04] 重启函数异常: %s", re_err)


# ============================================================================
# DR-P1-05修复: 数据库查询超时
# ============================================================================
class DbQueryTimeout:
    """数据库查询超时保护"""

    def __init__(self, default_timeout_sec: float = 5.0):
        self._default_timeout = default_timeout_sec

    def query_with_timeout(self, func: Callable, timeout_sec: Optional[float] = None,
                           *args, **kwargs) -> Any:
        _timeout = timeout_sec or self._default_timeout
        result = [None]
        error = [None]

        def _target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=_target, daemon=True)
        t.start()
        t.join(timeout=_timeout)
        if t.is_alive():
            logging.warning("[DR-P1-05] 数据库查询超时(%.1fs): %s", _timeout, getattr(func, '__name__', '?'))
            return None
        if error[0] is not None:
            logging.warning("[DR-P1-05] 数据库查询异常: %s", error[0])
            return None
        return result[0]


# ============================================================================
# DR-P1-06修复: 看门狗定时器
# ============================================================================
class Watchdog:
    """看门狗定时器：超时未feed则触发回调"""

    def __init__(self, timeout_sec: float = 30.0, on_timeout: Optional[Callable] = None,
                 name: str = 'watchdog'):
        self._timeout = timeout_sec
        self._on_timeout = on_timeout
        self._name = name
        self._last_feed = time.time()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._timeout_count = 0

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._last_feed = time.time()
        self._thread = threading.Thread(target=self._watch_loop, daemon=True, name=f'wd_{self._name}')
        self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            # R23-P2-17修复: join超时后检查线程状态
            if self._thread.is_alive():
                logging.warning("[R23-P2-17] 看门狗线程'%s'未在2.0s内终止", self._name)

    def feed(self) -> None:
        with self._lock:
            self._last_feed = time.time()

    def _watch_loop(self) -> None:
        while True:
            with self._lock:
                if not self._running:
                    break
                elapsed = time.time() - self._last_feed
            if elapsed > self._timeout:
                self._timeout_count += 1
                logging.warning("[DR-P1-06] 看门狗'%s'超时(%.1fs>%.1fs), 第%d次",
                                self._name, elapsed, self._timeout, self._timeout_count)
                if self._on_timeout:
                    try:
                        self._on_timeout()
                    except Exception as e:
                        logging.error("[DR-P1-06] 看门狗回调异常: %s", e)
                with self._lock:
                    self._last_feed = time.time()
            time.sleep(min(self._timeout / 2, 5.0))

    @property
    def timeout_count(self) -> int:
        return self._timeout_count


# ============================================================================
# DR-P1-07修复: 心跳监控
# ============================================================================
class HeartbeatMonitor:
    """心跳监控：检测心跳超时并触发告警"""

    def __init__(self, heartbeat_interval_sec: float = 10.0,
                 missed_threshold: int = 3,
                 on_heartbeat_lost: Optional[Callable] = None):
        self._interval = heartbeat_interval_sec
        self._missed_threshold = missed_threshold
        self._on_lost = on_heartbeat_lost
        self._last_heartbeat = time.time()
        self._missed_count = 0
        self._lock = threading.Lock()
        self._lost = False

    def beat(self) -> None:
        with self._lock:
            self._last_heartbeat = time.time()
            self._missed_count = 0
            if self._lost:
                self._lost = False
                # R27-P2-DR-02修复: 心跳恢复日志从info提升到warning（恢复是重要状态变更）
                logging.warning("[DR-P1-07] 心跳恢复")

    def check(self) -> bool:
        with self._lock:
            elapsed = time.time() - self._last_heartbeat
            if elapsed > self._interval * (self._missed_count + 1):
                self._missed_count += 1
                if self._missed_count >= self._missed_threshold and not self._lost:
                    self._lost = True
                    logging.warning(
                        "[DR-P1-07] 心跳丢失! 连续%d次未检测到(间隔%.0fs), 已过%.1fs",
                        self._missed_count, self._interval, elapsed
                    )
                    if self._on_lost:
                        try:
                            self._on_lost()
                        except Exception as e:
                            logging.error("[DR-P1-07] 心跳丢失回调异常: %s", e)
        return not self._lost

    @property
    def is_lost(self) -> bool:
        with self._lock:
            return self._lost


# ============================================================================
# DR-P1-08修复: 信号超时清理
# ============================================================================
class SignalExpiryManager:
    """信号超时清理：定期清理过期信号"""

    def __init__(self, default_ttl_sec: float = 300.0, cleanup_interval_sec: float = 60.0):
        self._default_ttl = default_ttl_sec
        self._cleanup_interval = cleanup_interval_sec
        self._signals: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._expired_count = 0

    def put(self, signal_id: str, signal: Any, ttl_sec: Optional[float] = None) -> None:
        with self._lock:
            expiry = time.time() + (ttl_sec or self._default_ttl)
            self._signals[signal_id] = (expiry, signal)

    def get(self, signal_id: str) -> Optional[Any]:
        with self._lock:
            if signal_id in self._signals:
                expiry, signal = self._signals[signal_id]
                if time.time() < expiry:
                    return signal
                del self._signals[signal_id]
                self._expired_count += 1
        return None

    def cleanup(self) -> int:
        with self._lock:
            now = time.time()
            expired = [k for k, (exp, _) in self._signals.items() if now >= exp]
            for k in expired:
                del self._signals[k]
            self._expired_count += len(expired)
            return len(expired)

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._signals)


# ============================================================================
# DR-P1-09修复: 断路器半开恢复策略
# ============================================================================
class CircuitBreakerHalfOpen:
    """断路器三态（CLOSED/OPEN/HALF_OPEN）+半开恢复策略"""

    CLOSED = 'CLOSED'
    OPEN = 'OPEN'
    HALF_OPEN = 'HALF_OPEN'

    def __init__(self, failure_threshold: int = 5, open_duration_sec: float = 60.0,
                 half_open_max_probes: int = 3):
        self._failure_threshold = failure_threshold
        self._open_duration = open_duration_sec
        self._half_open_max_probes = half_open_max_probes
        self._state = self.CLOSED
        self._failure_count = 0
        self._opened_at: float = 0.0
        self._half_open_probes = 0
        self._half_open_successes = 0
        self._lock = threading.Lock()

    def record_success(self) -> None:
        with self._lock:
            if self._state == self.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self._half_open_max_probes:
                    self._state = self.CLOSED
                    self._failure_count = 0
                    self._half_open_probes = 0
                    self._half_open_successes = 0
                    # R27-P2-DR-03修复: 断路器半开恢复日志从info提升到warning
                    logging.warning("[DR-P1-09] 断路器半开探测成功, 恢复CLOSED")
            elif self._state == self.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            if self._state == self.HALF_OPEN:
                self._state = self.OPEN
                self._opened_at = time.time()
                logging.warning("[DR-P1-09] 断路器半开探测失败, 回退OPEN")
            elif self._state == self.CLOSED and self._failure_count >= self._failure_threshold:
                self._state = self.OPEN
                self._opened_at = time.time()
                logging.warning("[DR-P1-09] 断路器触发OPEN(failures=%d>=threshold=%d)",
                                self._failure_count, self._failure_threshold)

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == self.CLOSED:
                return True
            if self._state == self.OPEN:
                if time.time() - self._opened_at >= self._open_duration:
                    self._state = self.HALF_OPEN
                    self._half_open_probes = 0
                    self._half_open_successes = 0
                    # R27-P2-DR-04修复: 断路器进入HALF_OPEN日志从info提升到warning
                    logging.warning("[DR-P1-09] 断路器进入HALF_OPEN, 开始探测")
                    return True
                return False
            if self._state == self.HALF_OPEN:
                if self._half_open_probes < self._half_open_max_probes:
                    self._half_open_probes += 1
                    return True
                return False
            return False

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    # R27-P2-DR-05修复: 熔断仪表盘状态查询方法
    def get_dashboard(self) -> Dict[str, Any]:
        """熔断仪表盘：返回断路器完整状态供监控面板消费"""
        with self._lock:
            return {
                'state': self._state,
                'failure_count': self._failure_count,
                'failure_threshold': self._failure_threshold,
                'open_duration_sec': self._open_duration,
                'half_open_probes': self._half_open_probes,
                'half_open_max_probes': self._half_open_max_probes,
                'half_open_successes': self._half_open_successes,
                'opened_at': self._opened_at,
                'allow_request': self._state == self.CLOSED or (
                    self._state == self.OPEN and
                    time.time() - self._opened_at >= self._open_duration
                ) or (
                    self._state == self.HALF_OPEN and
                    self._half_open_probes < self._half_open_max_probes
                ),
            }


# ============================================================================
# DR-P1-10修复: 线程池耗尽保护
# ============================================================================
class ThreadPoolGuard:
    """线程池耗尽保护：监控活跃线程数，超限则拒绝新任务"""

    def __init__(self, max_active_threads: int = 50, reject_callback: Optional[Callable] = None):
        self._max_active = max_active_threads
        self._reject_callback = reject_callback
        self._active_count = 0
        self._rejected_count = 0
        self._lock = threading.Lock()

    def enter(self) -> bool:
        with self._lock:
            if self._active_count >= self._max_active:
                self._rejected_count += 1
                logging.warning("[DR-P1-10] 线程池耗尽(active=%d>=max=%d), 拒绝任务",
                                self._active_count, self._max_active)
                if self._reject_callback:
                    try:
                        self._reject_callback()
                    except Exception:
                        pass
                return False
            self._active_count += 1
            return True

    def exit(self) -> None:
        with self._lock:
            self._active_count = max(0, self._active_count - 1)


# ============================================================================
# DR-P1-12修复: 慢查询检测
# ============================================================================
class SlowQueryDetector:
    """慢查询检测：记录超过阈值的查询"""

    def __init__(self, slow_threshold_sec: float = 1.0, max_records: int = 100):
        self._threshold = slow_threshold_sec
        self._max_records = max_records
        self._slow_queries: deque = deque(maxlen=max_records)
        self._lock = threading.Lock()

    def record(self, query_name: str, duration_sec: float) -> None:
        if duration_sec > self._threshold:
            with self._lock:
                self._slow_queries.append({
                    'query': query_name,
                    'duration_sec': duration_sec,
                    'timestamp': time.time(),
                })
            logging.warning("[DR-P1-12] 慢查询: %s 耗时%.3fs>阈值%.1fs",
                            query_name, duration_sec, self._threshold)

    @property
    def slow_count(self) -> int:
        with self._lock:
            return len(self._slow_queries)


# ============================================================================
# DR-P1-14修复: 数据陈旧检测
# ============================================================================
class DataStalenessDetector:
    """数据陈旧检测：检测数据更新超时"""

    def __init__(self, staleness_threshold_sec: float = 60.0):
        self._threshold = staleness_threshold_sec
        self._last_update: Dict[str, float] = {}
        self._lock = threading.Lock()

    def record_update(self, data_key: str) -> None:
        with self._lock:
            self._last_update[data_key] = time.time()

    def is_stale(self, data_key: str) -> bool:
        with self._lock:
            last = self._last_update.get(data_key, 0.0)
            if last == 0.0:
                return True
            return (time.time() - last) > self._threshold

    def check_and_alert(self, data_key: str) -> bool:
        stale = self.is_stale(data_key)
        if stale:
            logging.warning("[DR-P1-14] 数据陈旧: %s (超过%.0fs未更新)", data_key, self._threshold)
        return stale


# ============================================================================
# DR-P1-15修复: 内存压力检测
# ============================================================================
class MemoryPressureGuard:
    """内存压力检测：超阈值时触发GC或拒绝分配"""

    def __init__(self, warning_mb: int = 512, critical_mb: int = 1024):
        self._warning_mb = warning_mb
        self._critical_mb = critical_mb

    def check(self) -> str:
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / (1024 * 1024)
        except ImportError:
            return 'normal'
        except Exception:
            return 'normal'
        if mem_mb > self._critical_mb:
            logging.critical("[DR-P1-15] 内存压力CRITICAL: %.0fMB>=%dMB", mem_mb, self._critical_mb)
            return 'critical'
        if mem_mb > self._warning_mb:
            logging.warning("[DR-P1-15] 内存压力WARNING: %.0fMB>=%dMB", mem_mb, self._warning_mb)
            return 'warning'
        return 'normal'


# ============================================================================
# DR-P1-16修复: 配置热加载原子性
# ============================================================================
class AtomicConfigRef:
    """原子配置引用：读操作无锁，写操作用锁保护"""

    def __init__(self, initial_config: Optional[Dict[str, Any]] = None):
        self._config = initial_config or {}
        self._lock = threading.Lock()
        self._version = 0

    def get(self) -> Dict[str, Any]:
        return dict(self._config)

    def set(self, new_config: Dict[str, Any]) -> int:
        with self._lock:
            self._config = dict(new_config)
            self._version += 1
            return self._version

    @property
    def version(self) -> int:
        return self._version


# ============================================================================
# DR-P1-17修复: 日志风暴抑制
# ============================================================================
class RateLimitedLogger:
    """日志风暴抑制：同一条日志在窗口内仅输出N次"""

    def __init__(self, window_sec: float = 60.0, max_per_window: int = 3):
        self._window = window_sec
        self._max = max_per_window
        self._counts: Dict[str, Tuple[int, float]] = {}
        self._lock = threading.Lock()

    def log(self, level: int, msg: str, *args) -> None:
        with self._lock:
            now = time.time()
            count, window_start = self._counts.get(msg, (0, now))
            if now - window_start > self._window:
                count = 1
                window_start = now
            else:
                count += 1
            self._counts[msg] = (count, window_start)
            if count <= self._max:
                logging.log(level, msg, *args)
            elif count == self._max + 1:
                logging.log(level, "[DR-P1-17] 日志抑制: '%s' 在%.0fs内已输出%d次, 后续静默",
                            msg[:50], self._window, self._max)


# ============================================================================
# DR-P1-18修复: 优雅降级策略
# ============================================================================
class GracefulDegradation:
    """优雅降级：多级fallback策略"""

    def __init__(self, strategies: List[Callable], name: str = 'unnamed'):
        self._strategies = strategies
        self._name = name
        self._current_level = 0

    def execute(self, *args, **kwargs) -> Any:
        for i, strategy in enumerate(self._strategies):
            try:
                result = strategy(*args, **kwargs)
                if result is not None:
                    if i > 0 and i != self._current_level:
                        # R27-P2-DR-06修复: 降级执行日志从info提升到warning
                        logging.warning("[DR-P1-18] 降级执行: %s level=%d/%d", self._name, i, len(self._strategies))
                        self._current_level = i
                    return result
            except Exception as e:
                # R27-P2-DR-07修复: 降级策略失败日志从debug提升到warning
                logging.warning("[DR-P1-18] 策略%d/%d失败: %s", i + 1, len(self._strategies), e)
        logging.warning("[DR-P1-18] 所有降级策略失败: %s", self._name)
        return None


# ============================================================================
# DR-P1-19修复: 队列溢出保护
# ============================================================================
class BoundedQueue:
    """有界队列：溢出时按策略处理（丢弃最旧/拒绝最新）"""

    DISCARD_OLDEST = 'discard_oldest'
    REJECT_NEWEST = 'reject_newest'

    def __init__(self, maxlen: int = 10000, overflow_policy: str = 'discard_oldest'):
        self._maxlen = maxlen
        self._policy = overflow_policy
        self._queue: deque = deque(maxlen=maxlen)
        self._overflow_count = 0
        self._lock = threading.Lock()

    def put(self, item: Any) -> bool:
        with self._lock:
            if len(self._queue) >= self._maxlen:
                self._overflow_count += 1
                if self._policy == self.DISCARD_OLDEST:
                    if self._queue:
                        self._queue.popleft()
                    self._queue.append(item)
                    return True
                return False
            self._queue.append(item)
            return True

    def get(self) -> Optional[Any]:
        with self._lock:
            return self._queue.popleft() if self._queue else None

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    @property
    def overflow_count(self) -> int:
        return self._overflow_count


# ============================================================================
# DR-P1-20修复: 资源泄漏检测
# ============================================================================
class ResourceLeakDetector:
    """资源泄漏检测器：追踪资源获取/释放不匹配"""

    def __init__(self, name: str = 'resource', check_interval_sec: float = 300.0):
        self._name = name
        self._acquired: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._leak_count = 0

    def acquire(self, resource_id: str) -> None:
        with self._lock:
            self._acquired[resource_id] = time.time()

    def release(self, resource_id: str) -> None:
        with self._lock:
            self._acquired.pop(resource_id, None)

    def check(self, max_age_sec: float = 3600.0) -> int:
        with self._lock:
            now = time.time()
            leaked = [k for k, t in self._acquired.items() if now - t > max_age_sec]
            self._leak_count += len(leaked)
            if leaked:
                logging.warning("[DR-P1-20] 资源泄漏: %s %d个资源超过%.0fs未释放(总计%d次)",
                                self._name, len(leaked), max_age_sec, self._leak_count)
            return len(leaked)


# ============================================================================
# DR-P1-21修复: 异步任务超时
# ============================================================================
class AsyncTaskTimeout:
    """异步任务超时监控"""

    def __init__(self, default_timeout_sec: float = 30.0):
        self._default_timeout = default_timeout_sec
        self._pending: Dict[str, Tuple[float, str]] = {}
        self._lock = threading.Lock()
        self._timeout_count = 0

    def submit(self, task_id: str, timeout_sec: Optional[float] = None) -> None:
        with self._lock:
            self._pending[task_id] = (time.time() + (timeout_sec or self._default_timeout), task_id)

    def complete(self, task_id: str) -> None:
        with self._lock:
            self._pending.pop(task_id, None)

    def check_timeouts(self) -> List[str]:
        with self._lock:
            now = time.time()
            timed_out = [tid for tid, (deadline, _) in self._pending.items() if now > deadline]
            for tid in timed_out:
                del self._pending[tid]
            self._timeout_count += len(timed_out)
        if timed_out:
            logging.warning("[DR-P1-21] 异步任务超时: %d个任务(总计%d次)", len(timed_out), self._timeout_count)
        return timed_out


# ============================================================================
# DR-P1-22修复: 批量操作分片
# ============================================================================
class BatchPartitioner:
    """批量操作分片：防止单次操作过大"""

    def __init__(self, max_batch_size: int = 1000):
        self._max_batch = max_batch_size

    def partition(self, items: List[Any]) -> List[List[Any]]:
        return [items[i:i + self._max_batch] for i in range(0, len(items), self._max_batch)]


# ============================================================================
# DR-P1-23修复: 回调异常隔离
# ============================================================================
def safe_callback_wrapper(callback: Callable, name: str = 'unnamed') -> Callable:
    """回调异常隔离：确保单个回调异常不影响其他回调"""
    @wraps(callback)
    def wrapper(*args, **kwargs):
        try:
            return callback(*args, **kwargs)
        except Exception as e:
            logging.error("[DR-P1-23] 回调'%s'异常隔离: %s", name, e, exc_info=True)
            return None
    return wrapper


# ============================================================================
# DR-P1-24修复: 进程级健康状态
# ============================================================================
class ProcessHealthState:
    """进程级健康状态聚合器"""

    def __init__(self):
        self._components: Dict[str, str] = {}
        self._lock = threading.Lock()

    def set_status(self, component: str, status: str) -> None:
        with self._lock:
            self._components[component] = status

    def is_healthy(self) -> bool:
        with self._lock:
            return all(s == 'healthy' for s in self._components.values())

    def get_status(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._components)


# ============================================================================
# FP-P1-01修复: math.fsum替代sum()用于浮点累加
# ============================================================================
def stable_sum(values: List[float]) -> float:
    """使用math.fsum进行高精度浮点累加"""
    return math.fsum(values)


def stable_mean(values: List[float]) -> float:
    """高精度均值计算"""
    if not values:
        return 0.0
    return math.fsum(values) / len(values)


def stable_variance(values: List[float]) -> float:
    """高精度方差计算（两遍算法）"""
    if len(values) < 2:
        return 0.0
    mean = stable_mean(values)
    deviations = [(v - mean) ** 2 for v in values]
    return math.fsum(deviations) / len(values)


def stable_std(values: List[float]) -> float:
    """高精度标准差计算"""
    var = stable_variance(values)
    return math.sqrt(var) if var > 0 else 0.0


# ============================================================================
# FP-P1-02修复: 浮点容差比较
# ============================================================================
def approx_equal(a: float, b: float, tolerance: float = PRICE_TOLERANCE) -> bool:
    return abs(a - b) <= tolerance


def approx_less_equal(a: float, b: float, tolerance: float = PRICE_TOLERANCE) -> bool:
    return a <= b + tolerance


def approx_greater_equal(a: float, b: float, tolerance: float = PRICE_TOLERANCE) -> bool:
    return a >= b - tolerance


def approx_less(a: float, b: float, tolerance: float = PRICE_TOLERANCE) -> bool:
    return a < b - tolerance


def approx_greater(a: float, b: float, tolerance: float = PRICE_TOLERANCE) -> bool:
    return a > b + tolerance


# ============================================================================
# FP-P1-03修复: 高精度夏普比率计算
# ============================================================================
def compute_sharpe_stable(returns: List[float], risk_free_rate: float = 0.0,
                          annualize_factor: float = 252.0) -> float:
    """高精度夏普比率计算"""
    if len(returns) < 2:
        return 0.0
    excess_returns = [r - risk_free_rate for r in returns]
    mean_excess = stable_mean(excess_returns)
    std_excess = stable_std(excess_returns)
    if std_excess < FLOAT_COMPARE_TOLERANCE:
        return 0.0
    sharpe = mean_excess / std_excess
    if annualize_factor > 0:
        sharpe *= math.sqrt(annualize_factor)
    return sharpe


# ============================================================================
# FP-P1-04修复: EWMA浮点稳定计算
# ============================================================================
def stable_ewma(current_value: float, new_value: float, alpha: float,
                min_alpha: float = 1e-10, max_alpha: float = 1.0) -> float:
    """EWMA浮点稳定更新"""
    alpha = max(min_alpha, min(max_alpha, alpha))
    return (1.0 - alpha) * current_value + alpha * new_value


# ============================================================================
# FP-P1-05修复: 权重归一化浮点安全
# ============================================================================
def safe_normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    """权重归一化，防止浮点偏差"""
    total = math.fsum(weights.values())
    if abs(total) < FLOAT_COMPARE_TOLERANCE:
        return {k: 0.0 for k in weights}
    return {k: v / total for k, v in weights.items()}


# ============================================================================
# FP-P1-06修复: 止损/止盈比较安全
# ============================================================================
def should_trigger_stop_loss(current_price: float, stop_loss_price: float,
                              direction: str, tolerance: float = PRICE_TOLERANCE) -> bool:
    """安全止损触发判断"""
    if direction == 'BUY':
        return current_price <= stop_loss_price + tolerance
    return current_price >= stop_loss_price - tolerance


def should_trigger_take_profit(current_price: float, take_profit_price: float,
                                direction: str, tolerance: float = PRICE_TOLERANCE) -> bool:
    """安全止盈触发判断"""
    if direction == 'BUY':
        return current_price >= take_profit_price - tolerance
    return current_price <= take_profit_price + tolerance


# ============================================================================
# FP-P1-07修复: 浮点Kahan求和器
# ============================================================================
class KahanSummation:
    """Kahan补偿求和算法"""

    def __init__(self):
        self._sum = 0.0
        self._compensation = 0.0

    def add(self, value: float) -> None:
        y = value - self._compensation
        t = self._sum + y
        self._compensation = (t - self._sum) - y
        self._sum = t

    @property
    def value(self) -> float:
        return self._sum

    def reset(self) -> None:
        self._sum = 0.0
        self._compensation = 0.0


# ============================================================================
# FP-P1-08修复: 安全除法
# ============================================================================
def safe_divide(numerator: float, denominator: float, default: float = 0.0,
                min_denominator: float = FLOAT_COMPARE_TOLERANCE) -> float:
    """安全除法，防止除零和极小分母"""
    if abs(denominator) < min_denominator:
        return default
    return numerator / denominator


# ============================================================================
# RC-P1-03修复: EventBus注销与发布竞态保护
# ============================================================================
class SubscriberSnapshotGuard:
    """订阅者快照守卫：发布时复制订阅者列表，防止注销竞态"""

    def __init__(self):
        self._lock = threading.RLock()

    def safe_publish(self, subscribers: List[Tuple[Callable, int]],
                     event: Any) -> List[Exception]:
        errors = []
        snapshot = list(subscribers)
        for callback, priority in snapshot:
            try:
                callback(event)
            except Exception as e:
                errors.append(e)
                logging.error("[RC-P1-03] 订阅者回调异常隔离: %s", e)
        return errors


# ============================================================================
# RC-P1-04修复: 配置热加载与读取竞态保护
# ============================================================================
class ConfigSnapshot:
    """配置快照：提供原子读取，防止热加载竞态"""

    def __init__(self, config: Dict[str, Any]):
        self._config = dict(config)
        self._timestamp = time.time()

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._config)

    @property
    def timestamp(self) -> float:
        return self._timestamp


# ============================================================================
# RC-P1-05修复: 影子引擎数据同步窗口
# ============================================================================
class ShadowSyncBarrier:
    """影子引擎同步屏障：确保主引擎与影子引擎数据一致窗口"""

    def __init__(self, sync_timeout_sec: float = 5.0):
        self._timeout = sync_timeout_sec
        self._barrier = threading.Barrier(2, timeout=sync_timeout_sec)
        self._sync_count = 0
        self._timeout_count = 0

    def wait_sync(self) -> bool:
        try:
            self._barrier.wait()
            self._sync_count += 1
            return True
        except threading.BrokenBarrierError:
            self._timeout_count += 1
            logging.warning("[RC-P1-05] 影子引擎同步超时(%.1fs)", self._timeout)
            return False


# ============================================================================
# RC-P1-06修复: 多策略共享全局变量保护
# ============================================================================
class SharedStateRegistry:
    """共享状态注册表：为多策略共享全局变量提供线程安全访问"""

    def __init__(self):
        self._state: Dict[str, Any] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def get(self, key: str, default: Any = None) -> Any:
        with self._global_lock:
            lock = self._locks.get(key)
        if lock:
            with lock:
                return self._state.get(key, default)
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            lock = self._locks[key]
        with lock:
            self._state[key] = value


# ============================================================================
# FC-P1-01修复: 策略注册/发现机制
# ============================================================================
class StrategyRegistry:
    """策略注册表：自动注册与发现"""

    _instance: Optional['StrategyRegistry'] = None
    _instance_lock = threading.Lock()

    def __init__(self):
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> 'StrategyRegistry':
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def register(self, strategy_id: str, strategy_type: str,
                 factory: Optional[Callable] = None, metadata: Optional[Dict] = None) -> None:
        with self._lock:
            self._registry[strategy_id] = {
                'strategy_id': strategy_id,
                'strategy_type': strategy_type,
                'factory': factory,
                'metadata': metadata or {},
                'registered_at': time.time(),
            }
            logging.info("[FC-P1-01] 策略已注册: %s type=%s", strategy_id, strategy_type)

    def discover(self, strategy_type: Optional[str] = None) -> List[str]:
        with self._lock:
            if strategy_type:
                return [sid for sid, info in self._registry.items()
                        if info['strategy_type'] == strategy_type]
            return list(self._registry.keys())

    def get_factory(self, strategy_id: str) -> Optional[Callable]:
        with self._lock:
            info = self._registry.get(strategy_id)
            return info['factory'] if info else None


# ============================================================================
# FC-P1-02修复: 参数池CLI回滚
# ============================================================================
class ParamVersionManager:
    """参数版本管理：支持CLI --param-version回滚"""

    def __init__(self):
        self._versions: Dict[str, Dict[str, Any]] = {}
        self._current_version: str = ''
        self._lock = threading.Lock()

    def save_version(self, version: str, params: Dict[str, Any]) -> None:
        with self._lock:
            self._versions[version] = dict(params)
            logging.info("[FC-P1-02] 参数版本已保存: %s (%d参数)", version, len(params))

    def rollback(self, version: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            params = self._versions.get(version)
            if params is not None:
                self._current_version = version
                logging.info("[FC-P1-02] 参数版本已回滚至: %s", version)
                return dict(params)
            logging.warning("[FC-P1-02] 参数版本不存在: %s", version)
            return None

    def list_versions(self) -> List[str]:
        with self._lock:
            return sorted(self._versions.keys())


# ============================================================================
# FC-P1-03~08修复: 信号生命周期管理
# ============================================================================
class SignalLifecycleManager:
    """信号生命周期六阶段管理（GENERATED→FILTERED→SCORED→EXECUTED→CONFIRMED→COMPLETED）"""

    GENERATED = 'GENERATED'
    FILTERED = 'FILTERED'
    SCORED = 'SCORED'
    EXECUTED = 'EXECUTED'
    CONFIRMED = 'CONFIRMED'
    COMPLETED = 'COMPLETED'
    EXPIRED = 'EXPIRED'
    REJECTED = 'REJECTED'

    VALID_TRANSITIONS = {
        GENERATED: {FILTERED, REJECTED, EXPIRED},
        FILTERED: {SCORED, REJECTED, EXPIRED},
        SCORED: {EXECUTED, REJECTED, EXPIRED},
        EXECUTED: {CONFIRMED, REJECTED, EXPIRED},
        CONFIRMED: {COMPLETED, EXPIRED},
        COMPLETED: set(),
        EXPIRED: set(),
        REJECTED: set(),
    }

    def __init__(self, default_ttl_sec: float = 300.0):
        self._signals: Dict[str, Dict[str, Any]] = {}
        self._default_ttl = default_ttl_sec
        self._lock = threading.Lock()

    def create(self, signal_id: str, signal_data: Any) -> None:
        with self._lock:
            self._signals[signal_id] = {
                'state': self.GENERATED,
                'data': signal_data,
                'created_at': time.time(),
                'updated_at': time.time(),
                'history': [self.GENERATED],
            }

    def transition(self, signal_id: str, new_state: str) -> bool:
        with self._lock:
            info = self._signals.get(signal_id)
            if info is None:
                return False
            current = info['state']
            if new_state in self.VALID_TRANSITIONS.get(current, set()):
                info['state'] = new_state
                info['updated_at'] = time.time()
                info['history'].append(new_state)
                return True
            logging.warning("[FC-P1-06] 信号状态转换非法: %s %s→%s", signal_id, current, new_state)
            return False

    def get_state(self, signal_id: str) -> Optional[str]:
        with self._lock:
            info = self._signals.get(signal_id)
            return info['state'] if info else None

    def cleanup_expired(self) -> int:
        with self._lock:
            now = time.time()
            expired = [sid for sid, info in self._signals.items()
                       if now - info['created_at'] > self._default_ttl
                       and info['state'] not in (self.COMPLETED, self.REJECTED, self.EXPIRED)]
            for sid in expired:
                self._signals[sid]['state'] = self.EXPIRED
                self._signals[sid]['history'].append(self.EXPIRED)
        if expired:
            # R27-P2-DR-08修复: 信号过期清理日志从info提升到warning
            logging.warning("[FC-P1-06] 信号过期清理: %d个", len(expired))
        return len(expired)


# 全局单例
_process_health = ProcessHealthState()
_strategy_registry = StrategyRegistry.get_instance()
_param_version_mgr = ParamVersionManager()
_signal_lifecycle = SignalLifecycleManager()


def get_process_health() -> ProcessHealthState:
    return _process_health


def get_strategy_registry() -> StrategyRegistry:
    return _strategy_registry


def get_param_version_manager() -> ParamVersionManager:
    return _param_version_mgr


def get_signal_lifecycle() -> SignalLifecycleManager:
    return _signal_lifecycle


# ============================================================================
# R27-P2-FC-02修复: 兼容性桥接 — 旧round()调用平滑迁移
# ============================================================================
@deprecated("请使用deterministic_round替代，避免银行家舍入歧义")
def compat_round(value: float, ndigits: int = 0) -> float:
    """兼容性桥接：行为同round()但标记为deprecated，引导迁移至deterministic_round"""
    return round(value, ndigits)


# R27-P2-FC-03修复: 兼容性桥接 — 旧int()调用平滑迁移
@deprecated("请使用safe_float_to_int替代，避免浮点截断偏差")
def compat_float_to_int(value: float) -> int:
    """兼容性桥接：行为同int()但标记为deprecated，引导迁移至safe_float_to_int"""
    return int(value)
