# [M1-68] 交易工具
# MODULE_ID: M1-119

"""trading_utils.py - 交易工具服务合并

合并自cross_system.py + deviation_classifier.py (2026-06-12)
佣金计算部分已去重至 commission_utils.py，此处通过re-export保持兼容

"""

from __future__ import annotations

import functools
import logging
import math
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from ali2026v3_trading.infra._helpers import get_logger
from ali2026v3_trading.infra.resilience import safe_float_to_int, safe_divide  # noqa: F401
from ali2026v3_trading.infra.shared_utils import is_same_day
from ali2026v3_trading.infra.commission_utils import (  # 去重：佣金计算规范版本位于 commission_utils.py
    FEE_STRUCTURE, FEE_STRUCTURE_V2, EXCHANGE_COMMISSION_RATES,
    DEFAULT_COMMISSION_RATE,
    _infer_contract_type, _infer_exchange_id, _infer_exchange_from_id, _infer_instrument_type,
    compute_commission, calc_trade_fee, estimate_commission_simple, get_commission_per_lot,
)

logger = get_logger(__name__)


# ============================================================
# Section 2: Cross-system integration (from cross_system.py)
# ============================================================

@dataclass(slots=True)

class UnifiedSlippageModel:

    base_bps: float = 2.0

    impact_factor: float = 0.05


    def estimate_price(self, mid_price: float, volume: float, side: str) -> float:

        if mid_price <= 0:

            return 0.0

        signed = 1.0 if str(side).upper() in ('BUY', 'CLOSE_SHORT') else -1.0

        impact_bps = self.base_bps + self.impact_factor * math.sqrt(max(volume, 0.0))

        return mid_price * (1.0 + signed * impact_bps / 10000.0)


@dataclass(slots=True)

class UnifiedPositionSizer:

    max_risk_ratio: float = 0.2  # R26-P2对齐标注: 评判系统保守定位(0.2),生产默认0.8,属有意差异——评判系统需更严格风控

    def size(self, equity: float, risk_per_lot: float, lots_min: int = 1, lots_max: int = 100) -> int:

        if equity <= 0 or risk_per_lot <= 0:

            return lots_min

        # R27-P2-FP-10修复: int()截断→safe_float_to_int()

        raw = safe_float_to_int((equity * self.max_risk_ratio) / risk_per_lot)

        return max(lots_min, min(lots_max, raw))


@dataclass(slots=True)

class BacktestLatencyController:

    min_cycle_delay_ms: int = 5


    def should_yield(self, last_cycle_ts: float) -> bool:

        return (time.time() - last_cycle_ts) * 1000.0 < self.min_cycle_delay_ms


@dataclass(slots=True)

class BacktestOrderSplitter:

    max_lots_per_child: int = 10


    def split(self, lots: int) -> List[int]:

        # R27-P2-FP-11修复: int()截断→safe_float_to_int()

        remain = max(0, safe_float_to_int(lots))

        chunks: List[int] = []

        while remain > 0:

            child = min(self.max_lots_per_child, remain)

            chunks.append(child)

            remain -= child

        return chunks or [0]


@dataclass(slots=True)

class CrossSystemExecutionKernel:

    slippage_model: UnifiedSlippageModel = field(default_factory=UnifiedSlippageModel)

    position_sizer: UnifiedPositionSizer = field(default_factory=UnifiedPositionSizer)

    latency_controller: BacktestLatencyController = field(default_factory=BacktestLatencyController)

    order_splitter: BacktestOrderSplitter = field(default_factory=BacktestOrderSplitter)


    def snapshot(self) -> Dict[str, Any]:

        return {

            'slippage_base_bps': self.slippage_model.base_bps,

            'position_max_risk_ratio': self.position_sizer.max_risk_ratio,

            'min_cycle_delay_ms': self.latency_controller.min_cycle_delay_ms,

            'max_lots_per_child': self.order_splitter.max_lots_per_child,

        }


# ============================================================================

# R4-D-05: 浮点精度安全计算

# ============================================================================


def safe_equity_update(current_equity: float, pnl: float, max_precision_loss: float = 1e-10) -> float:

    """安全权益更新 _防止浮点精度损失累积


    连续复利计算: float64 精度5-16位有效数字,

    当权益1e8且PnL<1e-2时可能出现精度损失败   """

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


    解决不同模块使用不同日志级别导致关键异常被低级别日志淹没的问题.'
    通过统一的级别配置,确保WARNING及以上级别的日志始终可见.

    Args:

        level_map: 模块名到日志级别的映射,覆盖默认配置

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


# 已知差异记录: 回测使用bar收盘价执行,实盘使用对手价,滑点模型不一.

_EXECUTION_MODEL_DIFFERENCES = {

    "backtest_execution": "bar_close_price",       # 回测: bar收盘盘

    "live_execution": "counterparty_price",        # 实盘: 对手手

    "backtest_slippage": "fixed_bps_cascade",      # 回测: 固定bps+级联

    "live_slippage": "market_impact_model",        # 实盘: 市场冲击模型

    "backtest_latency": "zero",                    # 回测: 零延迟

    "live_latency": "network_plus_exchange",       # 实盘: 网络+交易所延迟

}


def get_execution_model_differences() -> Dict[str, str]:

    """R4-D-08修复: 返回回测/实盘模型差异清单

    明确标注回测与实盘之间的模型差异,为后续对齐提供依据"""

    return dict(_EXECUTION_MODEL_DIFFERENCES)


# ============================================================================

# R4-D-09: 线程安全工具

# ============================================================================


class ThreadSafeCounter:
    """Thread-safe counter for shared equity_series and position state in parallel backtest"""

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

    """线程安全字典包装


    用于保护共享的position状态和equity_series_"""


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


# R21-MEM-P2-17修复: 生成器未正确关闭: perf_monitor()为@contextmanager生成器,

# with语句保证GeneratorExit异常触发布布exit__,自动调用生成器.close().若不使用with而直接调用perf_monitor(),需手动调用gen.close()确保资源释放放

# 当前所有调用点均使用with perf_monitor(...)语法,无需额外.close()保护.

@contextmanager

def perf_monitor(label: str, warn_threshold_ms: float = 100.0):

    """R4-D-13修复: 性能监控上下文管理器


    记录代码块执行时间,超过阈值时输出WARNING_

    统计信息可通过 get_perf_stats() 查询询

    Args:

        label: 监控标签

        warn_threshold_ms: 超过此时(ms)输出WARNING

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

                "[R4-D-13] 性能警告: %s 耗时 %.1fms > 阈值.1fms",

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


    确保异常信息包含完整的堆栈追踪,便于调试.

    Args:

        logger_instance: 日志实例

        message: 错误消息

        level: 日志级别

    """

    stack = traceback.format_exc()

    logger_instance.log(level, "%s\n%s", message, stack)


def safe_execute(func: Callable, *args, logger_instance: Optional[logging.Logger] = None,

                 default: Any = None, **kwargs) -> Any:

    """安全执行函数: 捕获异常并完整记录堆栈信息
    
    R4-D-14修复: 所有异常都完整记录堆栈信息,避免调试困难"""
    _log = logger_instance or logger

    try:

        return func(*args, **kwargs)

    except (ValueError, KeyError, TypeError, AttributeError) as e:

        _log.error(

            "[R4-D-14] %s 执行异常: %s\n%s",

            func.__name__, e, traceback.format_exc(),

        )

        return default


# ============================================================================

# NEW-P2-03修复: DRY: 统一multiprocessing spawn context获取

# ============================================================================


_spawn_context = None

_spawn_context_lock = threading.Lock()


def get_spawn_context():

    """获取multiprocessing的spawn context(单例缓存)
    统一替代 __import__('multiprocessing').get_context('spawn') 的重复代码,l2_optimizer.py / _preprocess.py 均通过此函数获取spawn context_避免DRY违反并确保context配置一致性能

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


# ============================================================

# Section 3: Deviation classification (from deviation_classifier.py)

# ============================================================


# ============================================================================

# 数据结构

# ============================================================================


@dataclass

class Signal:

    """统一信号表示: 回测围栏对比的基本单播"""

    timestamp: float

    direction: str  # 'BUY' | 'SELL' | 'HOLD' | None

    price: float

    volume: int = 0

    source: str = ''


@dataclass

class DeviationResult:

    """偏差分类结果"""

    category: str   # MATCH | CORRECT_DEVIATION | NUMERICAL_NOISE | CRITICAL_DEVIATION | NEEDS_REVIEW

    description: str

    baseline: Optional[Signal] = None

    actual: Optional[Signal] = None


# ============================================================================

# 分类阈值(可配置)'
# ============================================================================


# 浮点噪声阈值:价格差小于此值视为浮点累加顺序差

NUMERICAL_NOISE_ABS_TOL: float = 1e-6


# 可接受微小差异阈值:价格差小于此值视为可接受的数值偏差

ACCEPTABLE_PRICE_DIFF: float = 0.01


# ============================================================================

# 核心分类

# ============================================================================


def classify_deviation(

    baseline: Optional[Signal],

    actual: Optional[Signal],

    *,

    noise_tol: float = NUMERICAL_NOISE_ABS_TOL,

    acceptable_diff: float = ACCEPTABLE_PRICE_DIFF,

) -> DeviationResult:

    """偏差分类: 逐笔对比重构前后信号


    Args:

        baseline: 重构前(BEFORE)信号

        actual: 重构后(AFTER)信号

        noise_tol: 浮点噪声绝对容差

        acceptable_diff: 可接受价格差异阈值

    Returns:

        DeviationResult with category and description

    """

    # 两侧均无信号

    if baseline is None and actual is None:

        return DeviationResult('MATCH', '两侧均无信号')


    # 两侧均有信号

    if baseline is not None and actual is not None:

        # 方向一.+ 价格接近 => MATCH

        if (baseline.direction == actual.direction

                and math.isclose(baseline.price, actual.price, abs_tol=noise_tol)):

            return DeviationResult('MATCH', '信号完全一致', baseline, actual)


        # 方向性差.=> CRITICAL_DEVIATION(PA-06阻断项)'
        if baseline.direction != actual.direction:

            return DeviationResult(

                'CRITICAL_DEVIATION',

                f'方向性差.baseline={baseline.direction} vs actual={actual.direction}',

                baseline, actual,

            )


        # 价格差异分类

        price_diff = abs(baseline.price - actual.price)

        if price_diff < noise_tol:

            return DeviationResult(

                'NUMERICAL_NOISE',

                f'浮点累加顺序差异: price_diff={price_diff:.2e}',

                baseline, actual,

            )

        elif price_diff < acceptable_diff:

            return DeviationResult(

                'NUMERICAL_NOISE',

                f'微小价格差异(可接收 price_diff={price_diff:.4f}',

                baseline, actual,

            )

        else:

            return DeviationResult(

                'NEEDS_REVIEW',

                f'价格差异需溯源: price_diff={price_diff:.4f}',

                baseline, actual,

            )


    # baseline有信号,actual为空(未来函数消除)

    if baseline is not None and actual is None:

        return DeviationResult(

            'CORRECT_DEVIATION',

            f'未来函数消除导致的信号消息方向={baseline.direction})',

            baseline, actual,

        )


    # baseline无信号,actual为新增信号

    if baseline is None and actual is not None:

        return DeviationResult(

            'NEEDS_REVIEW',

            f'新增信号(方向={actual.direction}),需审查',

            baseline, actual,

        )


    return DeviationResult('NEEDS_REVIEW', '未知差异模式', baseline, actual)


# ============================================================================

# 批量分类 + 报告

# ============================================================================


def classify_signal_sequence(

    baseline_signals: List[Optional[Signal]],

    actual_signals: List[Optional[Signal]],

    *,

    noise_tol: float = NUMERICAL_NOISE_ABS_TOL,

    acceptable_diff: float = ACCEPTABLE_PRICE_DIFF,

) -> Dict[str, Any]:

    """批量信号序列偏差分类


    Args:

        baseline_signals: 重构前信号序列

        actual_signals: 重构后信号序列

        noise_tol: 浮点噪声容差

        acceptable_diff: 可接受价格差.'
    Returns:

        报告字典,包含

        - total: 总对比笔.

        - category_counts: 各分类计量       - critical_deviations: CRITICAL_DEVIATION详情列表

        - needs_review: NEEDS_REVIEW详情列表

        - pass_pa06: 是否通过PA-06(CRITICAL_DEVIATION=0)

    """

    if len(baseline_signals) != len(actual_signals):

        return {

            'total': -1,

            'error': f'序列长度不一.baseline={len(baseline_signals)} vs actual={len(actual_signals)}',

            'pass_pa06': False,

        }


    deviations = []

    for i, (b, a) in enumerate(zip(baseline_signals, actual_signals)):

        dev = classify_deviation(b, a, noise_tol=noise_tol, acceptable_diff=acceptable_diff)

        dev.description = f'[#{i}] {dev.description}'

        deviations.append(dev)


    category_counts: Dict[str, int] = {}

    for d in deviations:

        category_counts[d.category] = category_counts.get(d.category, 0) + 1


    critical = [d for d in deviations if d.category == 'CRITICAL_DEVIATION']

    needs_review = [d for d in deviations if d.category == 'NEEDS_REVIEW']


    return {

        'total': len(deviations),

        'category_counts': category_counts,

        'critical_deviations': critical,

        'needs_review': needs_review,

        'pass_pa06': len(critical) == 0,

    }


# ============================================================

# Unified aliases

# ============================================================


CommissionCalculator = type('CommissionCalculator', (), {

    'FEE_STRUCTURE': FEE_STRUCTURE,

    'FEE_STRUCTURE_V2': FEE_STRUCTURE_V2,

    'EXCHANGE_COMMISSION_RATES': EXCHANGE_COMMISSION_RATES,

    'DEFAULT_COMMISSION_RATE': DEFAULT_COMMISSION_RATE,

    'compute_commission': staticmethod(compute_commission),

    'calc_trade_fee': staticmethod(calc_trade_fee),

    'estimate_commission_simple': staticmethod(estimate_commission_simple),

    'get_commission_per_lot': staticmethod(get_commission_per_lot),

})


CrossSystemService = CrossSystemExecutionKernel


DeviationClassifier = type('DeviationClassifier', (), {

    'NUMERICAL_NOISE_ABS_TOL': NUMERICAL_NOISE_ABS_TOL,

    'ACCEPTABLE_PRICE_DIFF': ACCEPTABLE_PRICE_DIFF,

    'classify_deviation': staticmethod(classify_deviation),

    'classify_signal_sequence': staticmethod(classify_signal_sequence),

})


__all__ = [

    # Section 1: Commission calculation (from commission_utils.py)

    "FEE_STRUCTURE", "FEE_STRUCTURE_V2", "EXCHANGE_COMMISSION_RATES",

    "DEFAULT_COMMISSION_RATE",

    "_infer_contract_type", "_infer_exchange_id", "_infer_exchange_from_id", "_infer_instrument_type",

    "compute_commission", "calc_trade_fee", "estimate_commission_simple", "get_commission_per_lot",

    "CommissionCalculator",

    # Section 2: Cross-system integration (from cross_system.py)

    "UnifiedSlippageModel", "UnifiedPositionSizer", "BacktestLatencyController",

    "BacktestOrderSplitter", "CrossSystemExecutionKernel",

    "safe_equity_update", "safe_divide",

    "configure_log_levels", "get_execution_model_differences",

    "ThreadSafeCounter", "ThreadSafeDict",

    "perf_monitor", "get_perf_stats",

    "log_exception_with_stack", "safe_execute", "get_spawn_context",

    "CrossSystemService",

    # Section 3: Deviation classification (from deviation_classifier.py)

    "Signal", "DeviationResult",

    "NUMERICAL_NOISE_ABS_TOL", "ACCEPTABLE_PRICE_DIFF",

    "classify_deviation", "classify_signal_sequence",

    "DeviationClassifier",

]

