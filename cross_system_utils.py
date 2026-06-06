"""
跨系统公共工具模块 — 第四轮审计修复 (R4-D系列)

R4-D-05: 浮点精度安全计算
R4-D-07: 统一日志级别配置
R4-D-08: 回测/实盘模型差异标注
R4-D-09: 线程安全工具
R4-D-13: 性能监控
R4-D-14: 异常堆栈完整记录
"""
from __future__ import annotations

import functools
import logging
import math
import threading
import time
import traceback
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# R4-D-05: 浮点精度安全计算
# ============================================================================

# R33-P1-8修复: safe_divide统一为resilience_utils实现，消除重复
try:
    from ali2026v3_trading.resilience_utils import safe_divide  # noqa: F401 — 统一入口
except ImportError:
    # fallback: resilience_utils不可用时保留本地实现
    def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
        if abs(denominator) < 1e-15:
            return default
        result = numerator / denominator
        if not math.isfinite(result):
            return default
        return result


def safe_equity_update(current_equity: float, pnl: float, max_precision_loss: float = 1e-10) -> float:
    """安全权益更新 — 防止浮点精度损失累积

    连续复利计算中 float64 精度约15-16位有效数字，
    当权益>1e8且PnL<1e-2时可能出现精度损失。
    """
    new_equity = current_equity + pnl
    if not math.isfinite(new_equity):
        logger.warning("[R4-D-05] 权益计算NaN/inf: equity=%.6f pnl=%.6f", current_equity, pnl)
        return current_equity
    # 精度防护: 极小变动忽略
    if abs(pnl) < max_precision_loss * abs(current_equity):
        return current_equity
    return new_equity


# ============================================================================
# R4-D-07: 统一日志级别配置
# ============================================================================

_LOG_LEVEL_CONFIG: Dict[str, int] = {
    "default": logging.INFO,
    "RiskService": logging.INFO,
    "SignalService": logging.INFO,
    "TaskScheduler": logging.INFO,
    "StrategyEcosystem": logging.INFO,
    "CascadeJudge": logging.INFO,
    "GovernanceEngine": logging.INFO,
}


def configure_log_levels(level_map: Optional[Dict[str, int]] = None) -> None:
    """R4-D-07修复: 统一日志级别配置

    解决不同模块使用不同日志级别导致关键异常被低级别日志淹没的问题。
    通过统一的级别配置，确保WARNING及以上级别的日志始终可见。

    Args:
        level_map: 模块名到日志级别的映射，覆盖默认配置
    """
    if level_map:
        _LOG_LEVEL_CONFIG.update(level_map)
    for module_name, level in _LOG_LEVEL_CONFIG.items():
        if module_name == "default":
            continue
        module_logger = logging.getLogger(f"ali2026v3_trading.{module_name}")
        module_logger.setLevel(level)
    # 确保根日志级别不低于INFO
    root = logging.getLogger()
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    logger.info("[R4-D-07] 日志级别配置完成: %s", _LOG_LEVEL_CONFIG)


# ============================================================================
# R4-D-08: 回测/实盘模型差异标注
# ============================================================================

# 已知差异记录: 回测使用bar收盘价执行，实盘使用对手价，滑点模型不一致
_EXECUTION_MODEL_DIFFERENCES = {
    "backtest_execution": "bar_close_price",      # 回测: bar收盘价
    "live_execution": "counterparty_price",        # 实盘: 对手价
    "backtest_slippage": "fixed_bps_cascade",      # 回测: 固定bps+级联
    "live_slippage": "market_impact_model",        # 实盘: 市场冲击模型
    "backtest_latency": "zero",                    # 回测: 零延迟
    "live_latency": "network_plus_exchange",       # 实盘: 网络+交易所延迟
}


def get_execution_model_differences() -> Dict[str, str]:
    """R4-D-08修复: 返回回测/实盘模型差异清单

    明确标注回测与实盘之间的模型差异，为后续对齐提供依据。
    """
    return dict(_EXECUTION_MODEL_DIFFERENCES)


# ============================================================================
# R4-D-09: 线程安全工具
# ============================================================================

class ThreadSafeCounter:
    """R4-D-09修复: 线程安全计数器

    回测引擎并行运行时，共享的equity_series和position状态需加锁。
    """

    def __init__(self, initial: float = 0.0):
        self._value = initial
        self._lock = threading.Lock()

    def get(self) -> float:
        with self._lock:
            return self._value

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def add(self, delta: float) -> float:
        with self._lock:
            self._value += delta
            return self._value


class ThreadSafeDict:
    """R4-D-09修复: 线程安全字典包装

    用于保护共享的position状态和equity_series。
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def items(self):
        with self._lock:
            return list(self._data.items())

    def keys(self):
        with self._lock:
            return list(self._data.keys())


# ============================================================================
# R4-D-13: 性能监控
# ============================================================================

_perf_stats: Dict[str, Dict[str, float]] = {}
_perf_lock = threading.Lock()


# R21-MEM-P2-17修复: 生成器未正确关闭 — perf_monitor()为@contextmanager生成器，
# with语句保证GeneratorExit异常触发__exit__，自动调用生成器.close()。
# 若不使用with而直接调用perf_monitor()，需手动调用gen.close()确保资源释放。
# 当前所有调用点均使用with perf_monitor(...)语法，无需额外.close()保护。
@contextmanager
def perf_monitor(label: str, warn_threshold_ms: float = 100.0):
    """R4-D-13修复: 性能监控上下文管理器

    记录代码块执行时间，超过阈值时输出WARNING。
    统计信息可通过 get_perf_stats() 查询。

    Args:
        label: 监控标签
        warn_threshold_ms: 超过此时间(ms)输出WARNING
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        with _perf_lock:
            if label not in _perf_stats:
                _perf_stats[label] = {"count": 0, "total_ms": 0, "max_ms": 0}
            _perf_stats[label]["count"] += 1
            _perf_stats[label]["total_ms"] += elapsed_ms
            _perf_stats[label]["max_ms"] = max(_perf_stats[label]["max_ms"], elapsed_ms)
        if elapsed_ms > warn_threshold_ms:
            logger.warning(
                "[R4-D-13] 性能警告: %s 耗时 %.1fms > 阈值 %.1fms",
                label, elapsed_ms, warn_threshold_ms,
            )


def get_perf_stats() -> Dict[str, Dict[str, float]]:
    """返回性能监控统计信息"""
    with _perf_lock:
        return dict(_perf_stats)


# ============================================================================
# R4-D-14: 异常堆栈完整记录
# ============================================================================

def log_exception_with_stack(logger_instance: logging.Logger, message: str,
                           level: int = logging.ERROR) -> None:
    """R4-D-14修复: 完整记录异常堆栈信息

    确保异常信息包含完整的堆栈追踪，便于调试。

    Args:
        logger_instance: 日志实例
        message: 错误消息
        level: 日志级别
    """
    stack = traceback.format_exc()
    logger_instance.log(level, "%s\n%s", message, stack)


def safe_execute(func: Callable, *args, logger_instance: Optional[logging.Logger] = None,
                 default: Any = None, **kwargs) -> Any:
    """安全执行函数 — 捕获异常并完整记录堆栈

    R4-D-14修复: 所有异常都完整记录堆栈信息，避免调试困难。
    """
    _log = logger_instance or logger
    try:
        return func(*args, **kwargs)
    except Exception as e:
        _log.error(
            "[R4-D-14] %s 执行异常: %s\n%s",
            func.__name__, e, traceback.format_exc(),
        )
        return default


# ============================================================================
# NEW-P2-03修复: DRY — 统一multiprocessing spawn context获取
# ============================================================================

_spawn_context = None
_spawn_context_lock = threading.Lock()


def get_spawn_context():
    """获取multiprocessing的spawn context（单例缓存）

    统一替代 __import__('multiprocessing').get_context('spawn') 的重复代码。
    l2_optimizer.py 和 preprocess_ticks.py 均通过此函数获取spawn context，
    避免DRY违反并确保context配置一致性。

    Returns:
        multiprocessing.BaseContext: spawn context实例
    """
    global _spawn_context
    if _spawn_context is None:
        with _spawn_context_lock:
            if _spawn_context is None:
                import multiprocessing
                _spawn_context = multiprocessing.get_context('spawn')
    return _spawn_context
