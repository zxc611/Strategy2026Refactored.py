# [M1-62] 弹性计算工具
# MODULE_ID: M1-101
"""resilience/统一容错模块 —合并7个子模块

子模块
  resilience_numeric.py  —数值稳
  resilience_circuit.py  —断路
  resilience_watchdog.py —看门狗心跳
  resilience_state.py    —状态管理
  resilience_retry.py    —重试/退避超时
  resilience_config.py   —容错配置/校验
  resilience_utils.py    —资源守卫 + 兼容工具 + re-export
"""
from __future__ import annotations

import math
import time
import threading
import logging
import os
import errno as _errno
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque
from functools import wraps

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

try:
    from ali2026v3_trading.infra.shared_utils import safe_int
except ImportError:  # circular-import guard: shared_utils →resilience_utils →resilience
    def safe_int(value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

logger = get_logger(__name__)  # R9-5


# ============================================================
# Section 1: 数值稳定(←resilience_numeric.py)
# ============================================================

PRICE_TOLERANCE = 1e-6
FLOAT_COMPARE_TOLERANCE = 1e-9


def stable_sum(values: List[float]) -> float:
    return math.fsum(values)


def stable_mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return math.fsum(values) / len(values)


def stable_variance(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = stable_mean(values)
    deviations = [(v - mean) ** 2 for v in values]
    return math.fsum(deviations) / len(values)


def stable_std(values: List[float]) -> float:
    var = stable_variance(values)
    return math.sqrt(var) if var > 0 else 0.0


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


def compute_sharpe_stable(returns: List[float], risk_free_rate: float = 0.0,
                          annualize_factor: float = 252.0,
                          use_sample_std: bool = False) -> float:
    """计算夏普比率（数值稳定版)

    Args:
        returns: 收益率序列
        risk_free_rate: 逐期无风险利率（非年化）
        annualize_factor: 年化因子（日频252，分钟频252*240）
        use_sample_std: True使用样本标准差(n-1分母)，False使用总体标准差(n分母)
    """
    if len(returns) < 2:
        return 0.0
    excess_returns = [r - risk_free_rate for r in returns]
    mean_excess = stable_mean(excess_returns)
    if use_sample_std:
        n = len(excess_returns)
        deviations = [(r - mean_excess) ** 2 for r in excess_returns]
        var_excess = math.fsum(deviations) / (n - 1) if n > 1 else 0.0
        std_excess = math.sqrt(var_excess) if var_excess > 0 else 0.0
    else:
        std_excess = stable_std(excess_returns)
    if std_excess < FLOAT_COMPARE_TOLERANCE:
        return 0.0
    sharpe = mean_excess / std_excess
    if annualize_factor > 0:
        sharpe *= math.sqrt(annualize_factor)
    return sharpe


def stable_ewma(current_value: float, new_value: float, alpha: float,
                min_alpha: float = 1e-10, max_alpha: float = 1.0) -> float:
    alpha = max(min_alpha, min(max_alpha, alpha))
    return (1.0 - alpha) * current_value + alpha * new_value


def safe_normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    total = math.fsum(weights.values())
    if abs(total) < FLOAT_COMPARE_TOLERANCE:
        return {k: 0.0 for k in weights}
    return {k: v / total for k, v in weights.items()}


def should_trigger_stop_loss(current_price: float, stop_loss_price: float,
                              is_long: bool = True, rtol: float = 1e-8) -> bool:
    """P0-03修复: 统一止损触发判断 —生产/回测共用

    is_long=True→current_price<=stop_loss_price触发), is_long=False→current_price>=stop_loss_price触发)
    rtol使用np.isclose相对容差，与STOP_PRICE_RTOL对齐
    """
    if stop_loss_price <= 0:
        return False
    try:
        import numpy as np
        if is_long:
            return current_price <= stop_loss_price or np.isclose(current_price, stop_loss_price, rtol=rtol)
        return current_price >= stop_loss_price or np.isclose(current_price, stop_loss_price, rtol=rtol)
    except ImportError:
        if is_long:
            return current_price <= stop_loss_price
        return current_price >= stop_loss_price


def should_trigger_take_profit(current_price: float, take_profit_price: float,
                                is_long: bool = True, rtol: float = 1e-8) -> bool:
    """P0-03修复: 统一止盈触发判断 —生产/回测共用"""
    if take_profit_price <= 0:
        return False
    try:
        import numpy as np
        if is_long:
            return current_price >= take_profit_price or np.isclose(current_price, take_profit_price, rtol=rtol)
        return current_price <= take_profit_price or np.isclose(current_price, take_profit_price, rtol=rtol)
    except ImportError:
        if is_long:
            return current_price >= take_profit_price
        return current_price <= take_profit_price


class KahanSummation:
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


def safe_divide(numerator: float, denominator: float, default: float = 0.0,
                min_denominator: float = FLOAT_COMPARE_TOLERANCE) -> float:
    if abs(denominator) < min_denominator:
        return default
    return numerator / denominator


# ============================================================
# Section 2: 断路器(←resilience_circuit.py)
# ============================================================

class CircuitBreakerHalfOpen:
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

    @property
    def opened_at(self) -> float:
        """断路器进入OPEN状态的时间戳（委托给CircuitBreakerService用于计算暂停截止时间）"""
        with self._lock:
            return self._opened_at

    @property
    def open_duration(self) -> float:
        """OPEN状态持续时长（秒）"""
        with self._lock:
            return self._open_duration

    def force_open(self, open_duration_sec: Optional[float] = None,
                   opened_at: Optional[float] = None) -> None:
        """强制将断路器置为OPEN状态（由CircuitBreakerService在触发熔断时调用）'
        Args:
            open_duration_sec: 新的OPEN持续时长（秒），None则保持原值
            opened_at: OPEN状态起始时间戳，None则使用time.time()
        """
        with self._lock:
            if open_duration_sec is not None:
                self._open_duration = open_duration_sec
            self._state = self.OPEN
            self._opened_at = opened_at if opened_at is not None else time.time()
            self._half_open_probes = 0
            self._half_open_successes = 0
            logging.warning("[DR-P1-09] 断路器强制OPEN (duration=%.1fs)", self._open_duration)

    def get_dashboard(self) -> Dict[str, Any]:
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

class RateLimitedLogger:
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
                logging.log(level, "[DR-P1-17] 日志抑制: '%s' %.0fs内已输出%d次 后续静默",
                            msg[:50], self._window, self._max)


class GracefulDegradation:
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
                        logging.warning("[DR-P1-18] 降级执行: %s level=%d/%d", self._name, i, len(self._strategies))
                        self._current_level = i
                    return result
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[DR-P1-18] 策略%d/%d失败: %s", i + 1, len(self._strategies), e)
        logging.warning("[DR-P1-18] 所有降级策略失败 %s", self._name)
        return None


# ============================================================
# Section 3: 看门狗心跳 (←resilience_watchdog.py)
# ============================================================

class Watchdog:
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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        logging.error("[DR-P1-06] 看门狗回调异常 %s", e)
                with self._lock:
                    self._last_feed = time.time()
            time.sleep(min(self._timeout / 2, 5.0))

    @property
    def timeout_count(self) -> int:
        return self._timeout_count


class HeartbeatMonitor:
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
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            logging.error("[DR-P1-07] 心跳丢失回调异常: %s", e)
        return not self._lost

    @property
    def is_lost(self) -> bool:
        with self._lock:
            return self._lost


class ProcessHealthState:
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

class AsyncTaskTimeout:
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
            logging.warning("[DR-P1-21] 异步任务超时: %d个任务总计%d次", len(timed_out), self._timeout_count)
        return timed_out


# --- watchdog module-level singleton ---

_process_health = ProcessHealthState()


def get_process_health() -> ProcessHealthState:
    return _process_health


# ============================================================
# Section 4: 状态管理(←resilience_state.py)
# ============================================================

class AtomicConfigRef:
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


class ConfigSnapshot:
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


class SharedStateRegistry:
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


class ParamVersionManager:
    def __init__(self):
        self._versions: Dict[str, Dict[str, Any]] = {}
        self._current_version: str = ''
        self._lock = threading.Lock()

    def save_version(self, version: str, params: Dict[str, Any]) -> None:
        with self._lock:
            self._versions[version] = dict(params)
            logging.info("[FC-P1-02] 参数版本已保存 %s (%d参数)", version, len(params))
        # P1-46修复: 同步版本到config_version_tracker统一追踪
        try:
            from ali2026v3_trading.config.config_version_tracker import record_param_version
            record_param_version(version, params)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] resilience record_param_version suppressed: %s", _r3_err)
            pass

    def rollback(self, version: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            params = self._versions.get(version)
            if params is not None:
                self._current_version = version
                logging.info("[FC-P1-02] 参数版本已回滚至: %s", version)
                return dict(params)
            logging.warning("[FC-P1-02] 参数版本不存在 %s", version)
            return None

    def list_versions(self) -> List[str]:
        with self._lock:
            return sorted(self._versions.keys())


class SignalLifecycleManager:
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
            logging.warning("[FC-P1-06] 信号状态转换非法 %s %s→s", signal_id, current, new_state)
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
            logging.warning("[FC-P1-06] 信号过期清理: %d条", len(expired))
        return len(expired)


# --- state module-level singletons ---

_param_version_mgr = ParamVersionManager()
_signal_lifecycle = SignalLifecycleManager()


def get_param_version_manager() -> ParamVersionManager:
    return _param_version_mgr


def get_signal_lifecycle() -> SignalLifecycleManager:
    return _signal_lifecycle


# ============================================================
# Section 5: 重试/退避超时 (←resilience_retry.py)
# ============================================================

# PRICE_TOLERANCE / FLOAT_COMPARE_TOLERANCE 已在 Section 1 定义，此处不再重复

PRICE_DISPLAY_DP = 4
RATIO_DISPLAY_DP = 4
GREEKS_DISPLAY_DP = 6
STATS_DISPLAY_DP = 2
PERCENTAGE_DISPLAY_DP = 2

_log_format_local = threading.local()


def deterministic_round(value: float, ndigits: int = 0) -> float:
    multiplier = 10 ** ndigits
    return math.floor(value * multiplier + 0.5) / multiplier


def safe_float_to_int(value: float) -> int:
    return int(math.floor(value + 0.5))


def thread_safe_log(logger_instance: logging.Logger, level: int, msg: str, *args) -> None:
    try:
        formatted = msg % args if args else msg
    except (TypeError, ValueError):
        formatted = msg
    setattr(_log_format_local, 'last_msg', formatted)
    logger_instance.log(level, formatted)


def deprecated(reason: str = "", migration_guide: str = "", removal_version: str = ""):
    """P1-21: deprecated装饰器权威定义底层模块，避免循环导入

    shared_utils.py从此处re-export，其他模块统一从shared_utils导入。'
    """
    import functools
    import warnings as _warnings
    def decorator(func):
        func._deprecated = True
        func._deprecated_reason = reason
        func._deprecated_migration_guide = migration_guide
        func._deprecated_removal_version = removal_version

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            msg_parts = [f"{func.__name__} is deprecated"]
            if reason:
                msg_parts.append(f"原因: {reason}")
            if migration_guide:
                msg_parts.append(f"迁移指南: {migration_guide}")
            if removal_version:
                msg_parts.append(f"计划在版本{removal_version} 中移除")
            msg = ". ".join(msg_parts) + "."
            _warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        # 保留装饰器元数据
        wrapper._deprecated = True
        wrapper._deprecated_reason = reason
        wrapper._deprecated_migration_guide = migration_guide
        wrapper._deprecated_removal_version = removal_version
        # 保留api_version元数据（如果有的话）'
        for attr in ('_api_version', '_api_changelog', '_api_versioned'):
            val = getattr(func, attr, None)
            if val is not None:
                setattr(wrapper, attr, val)
        return wrapper
    return decorator


# [P2-12] 权威重试实现 —retry_with_limit/ExponentialBackoff/BoundedRetry
# 为全系统统一的重试模式权威定义。其他模块中的重试实现应逐步委托到此处。'
# 已知非权威重试实现
#   - order/order_persistence.py: NetworkRetryManager (TODO: 委托到BoundedRetry)
#   - infra/_ops_framework.py: _execute_with_retry_and_timeout (TODO: 委托到BoundedRetry)
#   - position/position_command_service.py: _schedule_close_retry (TODO: 委托到BoundedRetry)
#   - infra/shared_utils.py: retry_with_limit (re-export, 权威定义在此模块)
class ExponentialBackoff:
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


class TimeoutGuard:
    def __init__(self, timeout_sec: float, default_result: Any = None):
        self._timeout = timeout_sec
        self._default = default_result

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        result = [self._default]
        error = [None]

        def _target():
            try:
                result[0] = func(*args, **kwargs)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                error[0] = e

        exec_thread = threading.Thread(target=_target, daemon=True)
        t.start()
        t.join(timeout=self._timeout)
        if t.is_alive():
            logging.warning("[DR-P1-02] tick处理超时(%.1fs), 函数=%s", self._timeout, func.__name__)
            if t.is_alive():
                logging.warning("[R23-P2-17] 超时线程未终止 %s", func.__name__)
            return self._default
        if error[0] is not None:
            raise error[0]
        return result[0]


class BoundedRetry:
    def __init__(self, max_retries: int = 3, base_delay: float = 0.1,
                 max_delay: float = 5.0, cancel_event: Optional[threading.Event] = None,
                 retry_on: Optional[Tuple[type, ...]] = None,
                 on_exhausted: str = "return_none",
                 backoff_strategy: str = "exponential",
                 backoff_factor: float = 2.0):
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._cancel_event = cancel_event
        self._retry_on = retry_on
        self._on_exhausted = on_exhausted
        self._backoff_strategy = backoff_strategy
        self._backoff_factor = backoff_factor
        self._backoff = ExponentialBackoff(base_delay, max_delay, max_retries, jitter=False)
        self._total_retries = 0
        self._total_failures = 0
        self._lock = threading.Lock()

    def _calc_delay(self, attempt: int) -> float:
        if self._backoff_strategy == "linear":
            delay = min(self._base_delay * (attempt + 1), self._max_delay)
        elif self._backoff_strategy == "fixed":
            delay = self._base_delay
        else:
            delay = min(self._base_delay * (self._backoff_factor ** attempt), self._max_delay)
        return delay

    def _sleep_or_cancel(self, delay: float) -> None:
        if self._cancel_event is not None:
            if self._cancel_event.wait(timeout=delay):
                raise InterruptedError("BoundedRetry: 重试被cancel_event中断")
        else:
            time.sleep(delay)

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        self._backoff.reset()
        last_exception = None
        for i in range(self._max_retries):
            try:
                result = func(*args, **kwargs)
                with self._lock:
                    self._total_retries += i
                return result
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ConnectionError) as e:
                last_exception = e
                if self._retry_on is not None and not isinstance(e, tuple(self._retry_on)):
                    with self._lock:
                        self._total_retries += i
                        self._total_failures += 1
                    raise
                logging.warning("[DR-P1-03] 重试%d/%d异常: %s", i + 1, self._max_retries, e)
            if i < self._max_retries - 1:
                delay = self._calc_delay(i)
                self._sleep_or_cancel(delay)
        with self._lock:
            self._total_failures += 1
        func_name = getattr(func, '__name__', '?')
        if self._on_exhausted == "raise":
            logging.critical("[DR-P1-03] 有界重试耗尽(%d次, 函数=%s", self._max_retries, func_name)
            raise last_exception
        elif self._on_exhausted == "warn":
            logging.warning("[DR-P1-03] 有界重试耗尽(%d次, 函数=%s, 继续执行", self._max_retries, func_name)
            return None
        else:
            logging.critical("[DR-P1-03] 有界重试耗尽(%d次, 函数=%s", self._max_retries, func_name)
            return None

    @property
    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {'total_retries': self._total_retries, 'total_failures': self._total_failures}


class StrategyRestartGuard:
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                with self._lock:
                    self._restart_count += 1
                    if self._restart_count > self._max_restarts:
                        logging.critical(
                            "[DR-P1-04] 策略崩溃重启耗尽(%d次, 最后异常 %s",
                            self._max_restarts, e
                        )
                        raise
                    current_count = self._restart_count
                logging.warning(
                    "[DR-P1-04] 策略崩溃(异常=%s), %0.1fs后第%d次重试上限%d)",
                    type(e).__name__, self._restart_delay, current_count, self._max_restarts
                )
                time.sleep(self._restart_delay)
                if self._restart_func:
                    try:
                        self._restart_func()
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as re_err:
                        logging.error("[DR-P1-04] 重启函数异常: %s", re_err)


class DbQueryTimeout:
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                error[0] = e

        exec_thread = threading.Thread(target=_target, daemon=True)
        t.start()
        t.join(timeout=_timeout)
        if t.is_alive():
            logging.warning("[DR-P1-05] 数据库查询超时(%.1fs): %s", _timeout, getattr(func, '__name__', '?'))
            return None
        if error[0] is not None:
            logging.warning("[DR-P1-05] 数据库查询异常 %s", error[0])
            return None
        return result[0]


# ============================================================
# Section 6: 容错配置/校验 (←resilience_config.py)
# ============================================================

def check_multi_level_cache_consistency() -> bool:
    """P2-15修复: 委托到config_params.check_multi_level_cache_consistency"""
    try:
        from ali2026v3_trading.config.config_params import check_multi_level_cache_consistency as _impl
        return _impl()
    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
        return True


def check_indicator_freshness(max_age_seconds: float = 300.0) -> bool:
    """P2-15修复: 委托到config_params.check_indicator_freshness"""
    try:
        from ali2026v3_trading.config.config_params import check_indicator_freshness as _impl
        return _impl(max_age_seconds)
    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
        return True


def check_risk_data_freshness(max_age_seconds: float = 60.0) -> bool:
    """P2-15修复: 委托到config_params.check_risk_data_freshness"""
    try:
        from ali2026v3_trading.config.config_params import check_risk_data_freshness as _impl
        return _impl(max_age_seconds)
    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
        return True


def _verify_default_param_table_integrity() -> bool:
    try:
        from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE
        required_keys = ['max_kline', 'kline_style', 'exchange']
        return all(k in DEFAULT_PARAM_TABLE for k in required_keys)
    except ImportError:
        return False


def validate_resilience_params(params: Dict[str, Any], strict: bool = False) -> Dict[str, Any]:
    errors = []
    if not isinstance(params, dict):
        return {'valid': False, 'errors': ['params must be a dict']}
    # P1-57修复: max_risk_ratio/close_stop_loss_ratio校验委托到validate_param_range
    try:
        from ali2026v3_trading.config.config_params import validate_param_range
        for key in ('max_risk_ratio', 'close_stop_loss_ratio'):
            if key in params:
                v = params[key]
                if not isinstance(v, (int, float)):
                    errors.append(f'{key}={v} is not numeric')
                elif not validate_param_range(key, v):
                    errors.append(f'{key}={v} out of valid range')
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        logging.debug("[R3-L2] resilience validate_param_range suppressed: %s", _r3_err)
        # 回退到原始校验
        if 'max_risk_ratio' in params:
            v = params['max_risk_ratio']
            if not isinstance(v, (int, float)) or v < 0 or v > 1:
                errors.append(f'max_risk_ratio={v} not in [0,1]')
        if 'close_stop_loss_ratio' in params:
            v = params['close_stop_loss_ratio']
            if not isinstance(v, (int, float)) or v < 0 or v > 1:
                errors.append(f'close_stop_loss_ratio={v} not in [0,1]')
    if 'signal_cooldown_sec' in params:
        v = params['signal_cooldown_sec']
        if not isinstance(v, (int, float)) or v < 0:
            errors.append(f'signal_cooldown_sec={v} must be non-negative')
    return {'valid': len(errors) == 0, 'errors': errors}


def validate_config_schema(config: Dict[str, Any]) -> Dict[str, Any]:
    errors = []
    if not isinstance(config, dict):
        return {'valid': False, 'errors': ['config must be a dict']}
    return {'valid': len(errors) == 0, 'errors': errors}


def validate_ui_params(params: Dict[str, Any]) -> Dict[str, Any]:
    errors = []
    if not isinstance(params, dict):
        return {'valid': False, 'errors': ['params must be a dict']}
    return {'valid': len(errors) == 0, 'errors': errors}


def _auto_recovery_decision_tree(error_type: str, error_count: int, elapsed_sec: float) -> str:
    if error_count <= 2 and elapsed_sec < 30:
        return 'retry'
    elif error_count <= 5 and elapsed_sec < 120:
        if error_type in ('db_lock', 'network'):
            return 'circuit_break'
        return 'degrade'
    elif error_count <= 10 and elapsed_sec < 300:
        if error_type == 'oom':
            return 'restart'
        return 'degrade'
    else:
        return 'halt'


def check_hot_reload_compatibility(updates: Dict[str, Any]) -> Dict[str, Any]:
    """P1-45修复: 原check_config_hot_reload重命名，消除与config_loader同名混淆。'
    检查哪些配置项支持热更新（不支持的返回requires_restart=True）。"""
    _UNSUPPORTED_KEYS = frozenset([
        'tvf_enabled', 'tvf_sigmoid_scale', 'mode_engine_version',
        'max_risk_ratio', 'close_stop_loss_ratio', 'signal_cooldown_sec',
    ])
    unsupported = set(updates.keys()) & _UNSUPPORTED_KEYS
    return {
        'can_hot_reload': len(unsupported) == 0,
        'unsupported_keys': sorted(unsupported),
        'requires_restart': len(unsupported) > 0,
    }


# P1-45修复: 保留旧名作为别名，标记为deprecated
def check_config_hot_reload(updates: Dict[str, Any]) -> Dict[str, Any]:
    """Deprecated: 请使用check_hot_reload_compatibility替代"""
    import warnings
    warnings.warn("check_config_hot_reload已弃用，请使用check_hot_reload_compatibility", DeprecationWarning, stacklevel=2)
    return check_hot_reload_compatibility(updates)


CHECKPOINT_INTERVAL_SECONDS = safe_int(os.getenv('CHECKPOINT_INTERVAL_SECONDS', '300'))

RESILIENCE_REGISTRY = {
    'circuit_breaker': {'enabled': True, 'failure_threshold': 5, 'recovery_timeout': 60},
    'retry_with_backoff': {'enabled': True, 'max_retries': 3, 'base_delay': 1.0, 'max_delay': 30.0},
    'bulkhead': {'enabled': True, 'max_concurrent': 10, 'max_queue': 100},
    'timeout': {'enabled': True, 'default_timeout': 30.0},
    'fallback': {'enabled': True, 'fallback_value': None},
}

WATCHDOG_TIMEOUT_ORDER_SEC = float(os.getenv('WATCHDOG_TIMEOUT_ORDER_SEC', '30.0'))
WATCHDOG_TIMEOUT_POSITION_SEC = float(os.getenv('WATCHDOG_TIMEOUT_POSITION_SEC', '30.0'))
WATCHDOG_TIMEOUT_RISK_SEC = float(os.getenv('WATCHDOG_TIMEOUT_RISK_SEC', '30.0'))
WATCHDOG_TIMEOUT_SIGNAL_SEC = float(os.getenv('WATCHDOG_TIMEOUT_SIGNAL_SEC', '60.0'))
WATCHDOG_TIMEOUT_TICK_SEC = float(os.getenv('WATCHDOG_TIMEOUT_TICK_SEC', '10.0'))

DEGRADATION_FEATURES = [
    'greeks_calculation', 'alpha_monitoring', 'shadow_strategy',
    'hft_signal_filter', 'cycle_resonance', 'parameter_optimization',
]

SLA_CONFIG = {
    'max_recovery_time_seconds': 300,
    'max_data_staleness_seconds': 60,
    'min_tick_throughput_per_sec': 10,
    'max_order_latency_ms': 500,
    'max_position_reconciliation_diff': 0,
    'availability_target_pct': 99.9,
}

ALARM_LEVELS = {
    'P0_CRITICAL': {'description': '系统不可用，需立即介入', 'notify': ['phone', 'sms'], 'escalation_sec': 60},
    'P1_HIGH': {'description': '功能严重降级，需30分钟内介入', 'notify': ['sms', 'email'], 'escalation_sec': 300},
    'P2_MEDIUM': {'description': '性能劣化，需当日处理', 'notify': ['email'], 'escalation_sec': 3600},
    'P3_LOW': {'description': '潜在风险，记录跟踪', 'notify': ['log'], 'escalation_sec': 86400},
}

class ISOLATION_LEVELS(Enum):
    NONE = "none"
    PROCESS = "process"
    THREAD = "thread"
    SERVICE = "service"
    CONTAINER = "container"

CAPACITY_LIMITS = {
    'max_instruments': 5000, 'max_open_positions': 50, 'max_orders_per_sec': 20,
    'max_memory_mb': 4096, 'max_cpu_pct': 80, 'max_disk_gb': 50, 'max_connections': 100,
}

COLD_START_TIMEOUT = safe_int(os.getenv('COLD_START_TIMEOUT', '120'))
COLD_START_PHASES = {
    'config_load': 5, 'db_init': 15, 'history_load': 30, 'instrument_subscribe': 20,
    'model_init': 10, 'shadow_engine_init': 10, 'health_check': 5, 'warmup_ticks': 25,
}


# ============================================================
# Section 7: 资源守卫 + 兼容工具 (←resilience_utils.py)
# ============================================================

class ThreadPoolGuard:
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
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] resilience reject_callback suppressed: %s", _r3_err)
                        pass
                return False
            self._active_count += 1
            return True

    def exit(self) -> None:
        with self._lock:
            self._active_count = max(0, self._active_count - 1)


class SlowQueryDetector:
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
            logging.warning("[DR-P1-12] 慢查询 %s 耗时%.3fs>阈值%.1fs",
                            query_name, duration_sec, self._threshold)

    @property
    def slow_count(self) -> int:
        with self._lock:
            return len(self._slow_queries)

class DataStalenessDetector:
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
            logging.warning("[DR-P1-14] 数据陈旧: %s (超过%.0fs未更新", data_key, self._threshold)
        return stale

class MemoryPressureGuard:
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
        except (ValueError, KeyError, TypeError, AttributeError):
            return 'normal'
        if mem_mb > self._critical_mb:
            logging.critical("[DR-P1-15] 内存压力CRITICAL: %.0fMB>=%dMB", mem_mb, self._critical_mb)
            return 'critical'
        if mem_mb > self._warning_mb:
            logging.warning("[DR-P1-15] 内存压力WARNING: %.0fMB>=%dMB", mem_mb, self._warning_mb)
            return 'warning'
        return 'normal'

class BoundedQueue:
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

class ResourceLeakDetector:
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
                logging.warning("[DR-P1-20] 资源泄漏: %s %d个资源超时.0fs未释放总计%d个",
                                self._name, len(leaked), max_age_sec, self._leak_count)
            return len(leaked)

class BatchPartitioner:
    def __init__(self, max_batch_size: int = 1000):
        self._max_batch = max_batch_size

    def partition(self, items: List[Any]) -> List[List[Any]]:
        return [items[i:i + self._max_batch] for i in range(0, len(items), self._max_batch)]

def safe_callback_wrapper(callback: Callable, name: str = 'unnamed') -> Callable:
    @wraps(callback)
    def wrapper(*args, **kwargs):
        try:
            return callback(*args, **kwargs)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[DR-P1-23] 回调'%s'异常隔离: %s", name, e, exc_info=True)
            return None
    return wrapper

class SignalExpiryManager:
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

class SubscriberSnapshotGuard:
    def __init__(self):
        self._lock = threading.RLock()

    def safe_publish(self, subscribers: List[Tuple[Callable, int]],
                     event: Any) -> List[Exception]:
        errors = []
        snapshot = list(subscribers)
        for callback, priority in snapshot:
            try:
                callback(event)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                errors.append(e)
                logging.error("[RC-P1-03] 订阅者回调异常隔离 %s", e)
        return errors

class ShadowSyncBarrier:
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

class StrategyRegistry:
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
            logging.info("[FC-P1-01] 策略已注册 %s type=%s", strategy_id, strategy_type)

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


# --- utils module-level singletons ---

_strategy_registry = StrategyRegistry.get_instance()


def get_strategy_registry() -> StrategyRegistry:
    return _strategy_registry

@deprecated("请使用deterministic_round替代，避免银行家舍入歧义")
def compat_round(value: float, ndigits: int = 0) -> float:
    return round(value, ndigits)


@deprecated("请使用safe_float_to_int替代，避免浮点截断偏差")
def compat_float_to_int(value: float) -> int:
    return int(value)


def is_disk_full_error(exc: Exception) -> bool:
    if isinstance(exc, OSError) and getattr(exc, 'errno', None) == _errno.ENOSPC:
        return True
    if isinstance(exc, OSError) and 'No space left on device' in str(exc):
        return True
    _msg = str(exc).lower()
    if 'out of space' in _msg or 'no space left' in _msg or 'enosp' in _msg:
        return True
    return False


# ============================================================
# Master __all__
# ============================================================

__all__ = [
    # Section 1: numeric
    'PRICE_TOLERANCE', 'FLOAT_COMPARE_TOLERANCE',
    'stable_sum', 'stable_mean', 'stable_variance', 'stable_std',
    'approx_equal', 'approx_less_equal', 'approx_greater_equal',
    'approx_less', 'approx_greater',
    'compute_sharpe_stable', 'stable_ewma', 'safe_normalize_weights',
    'should_trigger_stop_loss', 'should_trigger_take_profit',
    'KahanSummation', 'safe_divide',
    # Section 2: circuit
    'CircuitBreakerHalfOpen', 'RateLimitedLogger', 'GracefulDegradation',
    # Section 3: watchdog
    'Watchdog', 'HeartbeatMonitor', 'ProcessHealthState', 'AsyncTaskTimeout',
    'get_process_health',
    # Section 4: state
    'AtomicConfigRef', 'ConfigSnapshot', 'SharedStateRegistry',
    'ParamVersionManager', 'SignalLifecycleManager',
    'get_param_version_manager', 'get_signal_lifecycle',
    # Section 5: retry
    'PRICE_DISPLAY_DP', 'RATIO_DISPLAY_DP', 'GREEKS_DISPLAY_DP',
    'STATS_DISPLAY_DP', 'PERCENTAGE_DISPLAY_DP',
    'deterministic_round', 'safe_float_to_int', 'thread_safe_log', 'deprecated',
    'ExponentialBackoff', 'TimeoutGuard', 'BoundedRetry', 'StrategyRestartGuard',
    'DbQueryTimeout',
    # Section 6: config
    'check_multi_level_cache_consistency', 'check_indicator_freshness',
    'check_risk_data_freshness', '_verify_default_param_table_integrity',
    'validate_resilience_params', 'validate_config_schema', 'validate_ui_params',
    '_auto_recovery_decision_tree', 'check_config_hot_reload',
    'CHECKPOINT_INTERVAL_SECONDS', 'RESILIENCE_REGISTRY',
    'WATCHDOG_TIMEOUT_ORDER_SEC', 'WATCHDOG_TIMEOUT_POSITION_SEC',
    'WATCHDOG_TIMEOUT_RISK_SEC', 'WATCHDOG_TIMEOUT_SIGNAL_SEC',
    'WATCHDOG_TIMEOUT_TICK_SEC',
    'DEGRADATION_FEATURES', 'SLA_CONFIG', 'ALARM_LEVELS',
    'ISOLATION_LEVELS', 'CAPACITY_LIMITS',
    'COLD_START_TIMEOUT', 'COLD_START_PHASES',
    # Section 7: utils
    'ThreadPoolGuard', 'SlowQueryDetector', 'DataStalenessDetector',
    'MemoryPressureGuard', 'BoundedQueue', 'ResourceLeakDetector',
    'BatchPartitioner', 'safe_callback_wrapper', 'SignalExpiryManager',
    'SubscriberSnapshotGuard', 'ShadowSyncBarrier', 'StrategyRegistry',
    'get_strategy_registry', 'compat_round', 'compat_float_to_int',
    'is_disk_full_error',
]
