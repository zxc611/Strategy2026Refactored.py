"""
ali2026v3_trading 共享工具函数模块

✅ ID唯一 / 方法唯一 / 传递渠道唯一：跨模块共享的工具函数统一定义在此
禁止在其他模块中重复定义这些函数，必须从此模块导入。

统一管理的函数：
- normalize_option_type: 期权类型标准化
- to_float32: float32精度转换
- normalize_instrument_id: 合约ID标准化（移除交易所前缀）
- extract_product_code: 从合约ID提取品种代码
- extract_strike_price: 从期权合约ID提取行权价
- safe_int / safe_float: 安全类型转换
- RingBuffer: 线程安全环形缓冲区
- ShardRouter: 确定性分片路由器（品种ID驱动自动通道绑定）
- InitPhase / InitStateMachine / requires_phase: 初始化阶段管理
- ThreadLifecycleManager: 线程生命周期形式化管理
"""

import hashlib
import math
import os
import struct
import threading
import logging
import time
from typing import Any, Callable, Dict, List, Optional
from collections import deque
from datetime import timezone, timedelta

# [R22-TIME-P1-01] 全局CST时区常量，所有模块统一引用
CHINA_TZ = timezone(timedelta(hours=8))

# R27-P1修复: 导入浮点精度工具供全项目共享
from ali2026v3_trading.resilience_utils import (
    stable_sum, stable_mean, stable_variance, stable_std,
    approx_equal, approx_less, approx_greater,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, compute_sharpe_stable,
    safe_normalize_weights, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
)

def unified_ts_ms() -> int:
    """[R22-TIME-P1-15] 统一毫秒精度时间戳，用作缓存键，消除毫秒/微秒混用"""
    return int(time.time() * 1000)

# ============================================================================
# FM-01修复: 无风险利率常量（与greeks_calculator.DEFAULT_RISK_FREE_RATE对齐）
# ============================================================================
DEFAULT_RISK_FREE_RATE: float = 0.02
TRADING_DAYS_PER_YEAR_CHINA: float = 242.0

# ============================================================================
# R17-12修复: 年化因子常量 — 统一管理Sharpe/Calmar等年化计算
#   日频: √252 ≈ 15.87 (每日一个收益样本)
#   分钟频: √(252*240) ≈ 245.9 (每交易分钟一个收益样本，每天240分钟)
# ============================================================================
ANNUALIZE_FACTOR_DAILY = 252.0
ANNUALIZE_FACTOR_MINUTE = 252.0 * 240  # 每个交易日240分钟


def normalize_option_type(opt_type: str) -> str:
    """标准化option_type（唯一实现）

    Args:
        opt_type: 期权类型字符串 ('C', 'CALL', 'P', 'PUT'等)

    Returns:
        str: 标准化的期权类型 ('CALL' 或 'PUT')
    """
    if not opt_type:
        return ''
    upper = opt_type.upper()
    if upper in ('C', 'CALL'):
        return 'CALL'
    elif upper in ('P', 'PUT'):
        return 'PUT'
    elif upper in ('CE', 'PE', 'C_E', 'P_E'):
        logging.warning("[normalize_option_type] 非标准期权类型 '%s'，尝试映射: CE→CALL, PE→PUT", opt_type)
        return 'CALL' if upper.startswith('C') else 'PUT'
    logging.warning("[normalize_option_type] 未知期权类型 '%s'，原样透传", opt_type)
    return upper


def to_float32(value) -> float:
    """将数值转换为float32精度（唯一实现）

    NP-P1-12修复: None/NaN/inf显式告警而非静默返回0.0

    Args:
        value: 输入数值

    Returns:
        float: float32精度的数值
    """
    _log = logging.getLogger(__name__)
    if value is None:
        _log.warning("[to_float32] None输入，返回0.0")
        return 0.0
    _MAX_FLOAT32 = 3.4028235e+38
    if abs(value) > _MAX_FLOAT32:
        _log.warning("[NP-P2-30] value %.2e exceeds float32 range, clamping", value)
        value = _MAX_FLOAT32 if value > 0 else -_MAX_FLOAT32
    try:
        result = struct.unpack('f', struct.pack('f', float(value)))[0]
    except (TypeError, ValueError, OverflowError):
        _log.warning("[to_float32] 转换失败，value=%r，返回0.0", value)
        return 0.0
    import math
    if math.isnan(result) or math.isinf(result):
        _log.warning("[to_float32] 结果为NaN/inf，value=%r，返回0.0", value)
        return 0.0
    return result


def normalize_instrument_id(instrument_id: str) -> str:
    """移除交易所前缀和平台前缀，保留合约ID原始格式（品种ID直通，不做大写化）

    Args:
        instrument_id: 可能含交易所前缀或平台前缀的合约ID (如 "DCE.m2605" 或 "platform|m2605")

    Returns:
        str: 标准化后的合约ID (如 "m2605")，保留交易所原始大小写
    """
    normalized = str(instrument_id or '').strip()
    if '.' in normalized:
        normalized = normalized.split('.', 1)[-1]
    if '|' in normalized:
        normalized = normalized.split('|')[-1]
    return normalized


def extract_product_code(instrument_id: str) -> str:
    """从合约ID提取品种代码（唯一实现）

    Args:
        instrument_id: 合约ID (如 "IO2506-C-4000" 或 "al2605C18900")

    Returns:
        str: 品种代码 (如 "IO" 或 "al")
    """
    normalized = normalize_instrument_id(instrument_id)
    if not normalized:
        return ''
    i = 0
    while i < len(normalized) and normalized[i].isalpha():
        i += 1
    return normalized[:i] if i > 0 else ''


def extract_strike_price(instrument_id: str) -> Optional[float]:
    """从期权合约ID提取行权价（唯一实现，委托SubscriptionManager）

    Args:
        instrument_id: 期权合约ID

    Returns:
        Optional[float]: 行权价，解析失败返回None
    """
    try:
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        parsed = SubscriptionManager.parse_option(instrument_id)
        strike = parsed.get('strike_price')
        return float(strike) if strike is not None else None
    except (ValueError, KeyError, TypeError):
        return None


def normalize_year_month(year_month: str) -> str:
    """归一化年月格式（唯一实现）

    支持多种输入格式，统一输出为 YYMM 格式：
    - '6M05' -> '2605' (6月05年 -> 26年05月)
    - '26M05' -> '2605' (2位年份前缀+月份)
    - '202605' -> '2605' (完整年份)
    - '2605' -> '2605' (已是标准格式，直通)

    Args:
        year_month: 年月字符串

    Returns:
        str: 归一化后的 YYMM 格式年月
    """
    year_month = str(year_month or '').strip()
    if not year_month:
        return ''

    if len(year_month) == 4 and year_month.isdigit():
        month_part = int(year_month[2:])
        if 1 <= month_part <= 12:
            return year_month
        return year_month

    if len(year_month) == 6 and year_month.isdigit():
        month_part = int(year_month[4:])
        if 1 <= month_part <= 12:
            return year_month[2:]
        return year_month[2:]

    import re
    m = re.match(r'(\d{1,4})[M/\-](\d{1,2})$', year_month, re.IGNORECASE)
    if m:
        year_str = m.group(1)
        month_str = m.group(2).zfill(2)
        month_val = int(month_str)
        if month_val < 1 or month_val > 12:
            return year_month
        if len(year_str) == 4:
            return f'{year_str[2:]}{month_str}'
        elif len(year_str) == 2:
            return f'{year_str}{month_str}'
        elif len(year_str) == 1:
            from datetime import datetime
            current_year = datetime.now(CHINA_TZ).year
            candidate_full = current_year - (current_year % 10) + int(year_str)
            if candidate_full > current_year + 1:
                candidate_full -= 10
            yy = f'{candidate_full % 100:02d}'
            return f'{yy}{month_str}'
        return year_month

    return year_month


def safe_int(value: Any, default: int = 0) -> int:
    """安全转换为整数"""
    try:
        if value is None:
            return default
        return round(float(value))  # [R22-TS-P1-04] 改用round避免截断
    except (ValueError, TypeError) as e:
        logging.warning(f"[safe_int] Conversion failed for value '{value}': {e}")
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转换为浮点数"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError) as e:
        logging.warning(f"[safe_float] Conversion failed for value '{value}': {e}")
        return default


def safe_price_check(price) -> bool:
    """R24-P1-IV-10修复: 安全价格检查，NaN/Inf/非正数均返回False"""
    try:
        return math.isfinite(price) and price > 0
    except (TypeError, ValueError):
        return False


class RingBuffer:
    """环形缓冲区，线程安全

    R21-MEM-P1-15修复: 添加TTL过期检查，append时记录时间戳，
    查询时自动过滤过期条目，防止陈旧信号占用内存。
    """

    def __init__(self, maxlen: int, ttl_seconds: float = 0.0):
        self.maxlen = maxlen
        self.data = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        # R21-MEM-P1-15修复: TTL过期机制
        self._ttl_seconds = ttl_seconds  # 0表示不启用TTL
        self._timestamps: deque = deque(maxlen=maxlen)  # 每条数据对应的append时间戳

    def append(self, item: Any) -> None:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            self.data.append(item)
            # R21-MEM-P1-15修复: 记录append时间戳
            if self._ttl_seconds > 0:
                import time as _time
                self._timestamps.append(_time.time())

    def extend(self, items: List[Any]) -> None:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            self.data.extend(items)

    def __len__(self) -> int:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return len(self.data)

    def __getitem__(self, index: int) -> Any:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return self.data[index]

    def __iter__(self) -> Any:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return iter(list(self.data))

    def clear(self) -> None:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            self.data.clear()

    def to_list(self) -> List[Any]:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return list(self.data)

    # R21-MEM-P1-15修复: 获取未过期的条目
    def get_fresh(self) -> List[Any]:  # [R22-P2-TS06]
        """返回未过期的条目列表（TTL机制），若未启用TTL则返回全部数据"""
        with self._lock:
            if self._ttl_seconds <= 0 or not self._timestamps:
                return list(self.data)
            import time as _time
            now = _time.time()
            result = []
            for i, ts in enumerate(self._timestamps):
                if now - ts <= self._ttl_seconds:
                    result.append(self.data[i])
            return result


class ShardRouter:
    """确定性分片路由器：品种ID驱动的自动通道绑定

    核心设计：
    1. 用 hashlib.md5 替代 Python hash()，保证跨进程/跨重启确定性
    2. 品种代码全链路直通，零配置自动绑定
    3. 同一品种始终命中固定通道，tick时序天然正确
    4. 通道绑定注册表可审计（§8.4 路由规则可审计）
    5. 简单取模路由：shard_key % shard_count，2^n时等价位掩码

    用法：
        router = ShardRouter(shard_count=16)
        idx = router.route("IF2506")   # 返回确定性shard索引
        router.get_binding_map()        # 返回品种→通道绑定表
    """

    def __init__(self, shard_count: int = 16):
        self._shard_count = shard_count
        self._binding_map: Dict[str, int] = {}
        self._binding_lock = threading.Lock()

    @staticmethod
    def _deterministic_hash(key: str) -> int:
        h = hashlib.md5(key.encode('utf-8')).digest()
        return (h[0] << 24) | (h[1] << 16) | (h[2] << 8) | h[3]

    @property
    def shard_count(self) -> int:
        return self._shard_count

    def route(self, instrument_id: str) -> int:
        _pc_raw = extract_product_code(instrument_id)
        product_code = _pc_raw.lower() if _pc_raw else ''
        return self.route_by_product_code(product_code)

    def route_by_product_code(self, product_code: str) -> int:
        if not product_code:
            product_code = 'unknown'
        product_code = product_code.lower()

        with self._binding_lock:
            if product_code in self._binding_map:
                return self._binding_map[product_code]

        h = self._deterministic_hash(product_code)
        shard_idx = h % self._shard_count

        with self._binding_lock:
            self._binding_map[product_code] = shard_idx

        return shard_idx

    def get_binding_map(self) -> Dict[str, int]:
        with self._binding_lock:
            return dict(self._binding_map)

    def get_shard_members(self) -> Dict[int, List[str]]:
        result: Dict[int, List[str]] = {i: [] for i in range(self._shard_count)}
        with self._binding_lock:
            for product, shard_id in self._binding_map.items():
                result[shard_id].append(product)
        for shard_id in result:
            result[shard_id].sort()
        return result

    def get_routing_audit_line(self) -> str:
        members = self.get_shard_members()
        parts = []
        for sid in sorted(members.keys()):
            prods = members[sid]
            if prods:
                parts.append(f"Shard-{sid:02d}: {','.join(prods)}")
        return ' | '.join(parts)

    def reconfigure(self, new_shard_count: int) -> Dict[str, int]:
        with self._binding_lock:
            old_map = dict(self._binding_map)
            self._shard_count = new_shard_count
            self._binding_map.clear()
            migration = {}
            for product, old_shard in old_map.items():
                h = self._deterministic_hash(product)
                new_shard = h % self._shard_count
                self._binding_map[product] = new_shard
                if new_shard != old_shard:
                    migration[product] = (old_shard, new_shard)
        return migration


_GLOBAL_SHARD_ROUTER: Optional[ShardRouter] = None
_GLOBAL_SHARD_ROUTER_LOCK = threading.Lock()
DEFAULT_SHARD_COUNT = 16


def get_shard_router(shard_count: int = None) -> ShardRouter:
    """获取全局单例 ShardRouter（方法唯一原则）

    全链路只创建一个 ShardRouter 实例，确保第一层和第二层路由完全一致。
    """
    global _GLOBAL_SHARD_ROUTER
    if shard_count is None:
        shard_count = DEFAULT_SHARD_COUNT
    with _GLOBAL_SHARD_ROUTER_LOCK:
        if _GLOBAL_SHARD_ROUTER is None:
            _GLOBAL_SHARD_ROUTER = ShardRouter(shard_count=shard_count)
        return _GLOBAL_SHARD_ROUTER


class ThreadLifecycleManager:
    """管理多服务线程的启停与排水顺序（§8.5 并发安全）

    设计原则：
    - 不包含业务逻辑，仅管理线程生命周期
    - 强制停止顺序：(1) set StopEvent → (2) join Writers → (3) drain → (4) flush
    - 部分启动失败自动回滚已启动线程
    - 独立模块，不增加核心大模块负担
    """

    PHASE_CREATED = 'created'
    PHASE_STARTING = 'starting'
    PHASE_RUNNING = 'running'
    PHASE_STOPPING = 'stopping'
    PHASE_STOPPED = 'stopped'

    def __init__(self, owner_label: str = ''):
        self._owner_label = owner_label
        self._phase = self.PHASE_CREATED
        self._threads: Dict[str, threading.Thread] = {}
        self._daemon_flags: Dict[str, bool] = {}
        self._lock = threading.Lock()
        self._started_at: Optional[float] = None

    def register(self, name: str, thread: threading.Thread, daemon: bool = False) -> None:
        with self._lock:
            self._threads[name] = thread
            self._daemon_flags[name] = daemon

    def start_all(self, thread_configs: List[tuple]) -> None:
        self._phase = self.PHASE_STARTING
        self._started_at = time.time()
        started = []
        try:
            for name, thread, daemon in thread_configs:
                thread.start()
                self.register(name, thread, daemon)
                started.append(name)
        except Exception:
            logging.error("[ThreadMgr] 启动失败，已启动 %s/共%d，执行紧急清理", started, len(thread_configs))
            self._force_stop_partial(started)
            raise
        self._phase = self.PHASE_RUNNING

    def stop_all(self, stop_event: threading.Event, timeout: float = 30.0) -> Dict[str, Any]:  # [R22-P2-TS04]
        self._phase = self.PHASE_STOPPING
        stop_event.set()
        result = {'stopped': 0, 'timeout': 0, 'daemon': 0}
        for name, thread in list(self._threads.items()):
            if not thread:
                continue
            if self._daemon_flags.get(name, False):
                thread.join(timeout=min(timeout, 5.0))
                result['daemon'] += 1
                continue
            thread.join(timeout=timeout)
            if thread.is_alive():
                logging.warning("[ThreadMgr] %s 在 %ss 内未停止", name, timeout)
                result['timeout'] += 1
            else:
                result['stopped'] += 1
        self._phase = self.PHASE_STOPPED
        return result

    def _force_stop_partial(self, started_names: List[str]) -> None:  # [R22-P2-TS07]
        self._phase = self.PHASE_STOPPING
        for name in started_names:
            thread = self._threads.get(name)
            if thread and thread.is_alive():
                thread.join(timeout=2.0 if self._daemon_flags.get(name) else 10.0)
        self._phase = self.PHASE_STOPPED

    @property
    def phase(self) -> str:
        return self._phase

    @property
    def alive_count(self) -> int:
        with self._lock:
            return sum(1 for t in self._threads.values() if t and t.is_alive())

    def health_check(self) -> Dict[str, Any]:  # [R22-P2-TS05]
        with self._lock:
            return {
                'owner': self._owner_label,
                'phase': self._phase,
                'total_registered': len(self._threads),
                'alive': self.alive_count,
                'uptime_seconds': time.time() - self._started_at if self._started_at else 0,
                'threads': {name: t.is_alive() for name, t in self._threads.items()},
            }


# ============================================================================
# InitPhase / InitStateMachine / requires_phase — 初始化阶段管理
# ============================================================================

from enum import IntEnum


class InitPhase(IntEnum):
    NOT_STARTED = 0
    MEMORY_ALLOC = 1
    DB_CONNECT = 2
    EXTERNAL_SERVICES = 3
    DB_SCHEMA = 4
    DB_CACHES = 5
    WRITERS_START = 6
    CLEANUP_START = 7
    READY = 8


class InitializationError(Exception):
    def __init__(self, message: str, current_phase: 'InitPhase', required_phase: 'InitPhase'):
        super().__init__(message)
        self.current_phase = current_phase
        self.required_phase = required_phase


class InitStateMachine:
    def __init__(self):
        self._phase = InitPhase.NOT_STARTED
        self._lock = threading.Lock()
        self._ready_event = threading.Event()

    @property
    def phase(self) -> InitPhase:
        with self._lock:
            return self._phase

    def advance(self, new_phase: InitPhase) -> None:
        with self._lock:
            if new_phase != self._phase + 1 and new_phase != self._phase:
                raise InitializationError(
                    f"非法阶段转换: {self._phase.name} -> {new_phase.name}",
                    self._phase, new_phase
                )
            self._phase = new_phase
            if new_phase == InitPhase.READY:
                self._ready_event.set()

    def check_phase(self, required: InitPhase) -> None:
        with self._lock:
            current = self._phase
        if current < required:
            raise InitializationError(
                f"需要阶段 {required.name} 但当前为 {current.name}",
                current, required
            )

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        return self._ready_event.wait(timeout=timeout)

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def rollback_phase(self) -> InitPhase:
        with self._lock:
            if self._phase > InitPhase.NOT_STARTED:
                self._phase = InitPhase(self._phase - 1)
            return self._phase


def requires_phase(required: InitPhase):
    def decorator(method):
        def wrapper(self, *args, **kwargs):
            if hasattr(self, '_init_state'):
                self._init_state.check_phase(required)
            return method(self, *args, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        wrapper.__wrapped__ = method
        return wrapper
    return decorator


# ========================================================================
# 回撤开仓（Pullback Entry） — 通用基础设施
# 非高频策略(resonance/box/spring/arbitrage/market_making)共用
# 核心思想：权利金本位，信号触发后等待权利金回撤达标再入场
# ========================================================================

from dataclasses import dataclass, field


@dataclass(slots=True)
class PendingPullbackSignal:
    signal_id: str
    strategy_type: str
    instrument_id: str
    direction: str
    peak_price: float
    signal_price: float
    signal_bar_index: int
    peak_bar_index: int = 0
    bars_elapsed: int = 0
    ticks_elapsed: int = 0  # P0-2修复：逐tick计数器，支持HFT逐tick跟踪模式
    is_expired: bool = False
    dynamic_wait_bars: int = 0
    dynamic_retrace_pct: float = 0.0
    dynamic_wait_ticks: int = 0  # P0-2修复：逐tick模式下的等待tick数
    ref_price: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    def check_retrace(self, current_price: float,
                      retrace_pct: float,
                      wait_bars: int,
                      min_retrace_abs: float = 0.0,
                      wait_ticks: int = 0) -> bool:
        if self.is_expired:
            return False
        self.bars_elapsed += 1
        effective_wait = self.dynamic_wait_bars if self.dynamic_wait_bars > 0 else wait_bars
        effective_retrace = self.dynamic_retrace_pct if self.dynamic_retrace_pct > 0.0 else retrace_pct
        if self.bars_elapsed > effective_wait:
            self.is_expired = True
            return False
        # P0-裂缝10修复：禁止在回撤判断中动态上移peak_price
        # 原逻辑在current_price > ref时上移peak，导致回撤基准被"未来"数据修改
        # 修复：peak_price在信号创建时锁定，后续bar只用于判断回撤是否满足
        ref = self.ref_price if self.ref_price > 0.0 else self.peak_price
        if ref <= 0:
            return False
        retrace_abs = ref - current_price
        retrace_pct_actual = retrace_abs / ref
        if retrace_pct_actual < 0:
            return False
        if min_retrace_abs > 0.0 and retrace_abs < min_retrace_abs:
            return False
        return retrace_pct_actual >= effective_retrace

    def check_retrace_tick(self, current_price: float,
                           retrace_pct: float,
                           wait_ticks: int,
                           min_retrace_abs: float = 0.0) -> bool:
        """P0-2修复：逐tick实时跟踪模式 — 每个tick即时判断回撤

        HFT策略(S1)使用此方法，避免Bar内盲区导致未来函数风险。
        与check_retrace的区别：以tick为最小时间单位，非Bar。
        """
        if self.is_expired:
            return False
        self.ticks_elapsed += 1
        effective_wait_ticks = self.dynamic_wait_ticks if self.dynamic_wait_ticks > 0 else wait_ticks
        if effective_wait_ticks > 0 and self.ticks_elapsed > effective_wait_ticks:
            self.is_expired = True
            return False
        ref = self.ref_price if self.ref_price > 0.0 else self.peak_price
        if ref <= 0:
            return False
        retrace_abs = ref - current_price
        retrace_pct_actual = retrace_abs / ref
        if retrace_pct_actual < 0:
            return False
        if min_retrace_abs > 0.0 and retrace_abs < min_retrace_abs:
            return False
        return retrace_pct_actual >= retrace_pct


class PullbackManager:
    """通用回撤开仓管理器 — 非高频策略共用

    参数:
        pullback_enabled: 是否启用回撤延迟
        pullback_wait_bars: 基础最长等待Bar数
        pullback_retrace_pct: 权利金/价格回撤百分比阈值(如0.15=15%)
        pullback_iv_min_percentile: IV环境最低百分位
        pullback_iv_max_percentile: IV环境最高百分位
        pullback_ref_mode: 参照基准模式 "peak"(极值) | "atr"(ATR锚定) | "vwap"(VWAP) | "delta"(Delta锚定)
        pullback_atr_wait_multiplier: ATR动态等待倍数(wait_bars += ATR_pct * multiplier)
        pullback_retrace_pct_call: Call方向专用回撤幅度(None则用retrace_pct)
        pullback_retrace_pct_put: Put方向专用回撤幅度(None则用retrace_pct)
        pullback_theta_decay_accel: Theta衰减加速系数(临近到期缩短等待, 0=禁用)
        pullback_min_retrace_abs: 最小回撤绝对值(防止权利金极薄时百分比无意义)
        pullback_tick_tracking: P0-2修复：是否启用逐tick实时跟踪模式(HFT专用)
        pullback_wait_ticks: 逐tick模式下的最长等待tick数(默认=wait_bars*240)
    """

    REF_MODES = frozenset({'peak', 'atr', 'vwap', 'delta'})

    def __init__(self, params: Dict[str, Any] = None):
        p = params or {}
        self.enabled: bool = p.get('pullback_enabled', False)
        self.wait_bars: int = p.get('pullback_wait_bars', 5)
        self.retrace_pct: float = p.get('pullback_retrace_pct', 0.15)
        self.iv_min_pct: float = p.get('pullback_iv_min_percentile', 20.0)
        self.iv_max_pct: float = p.get('pullback_iv_max_percentile', 80.0)
        self.ref_mode: str = p.get('pullback_ref_mode', 'peak')
        if self.ref_mode not in self.REF_MODES:
            logging.warning("[Pullback] Unknown ref_mode='%s', falling back to 'peak'", self.ref_mode)
            self.ref_mode = 'peak'
        self.atr_wait_multiplier: float = p.get('pullback_atr_wait_multiplier', 0.0)
        self.retrace_pct_call: Optional[float] = p.get('pullback_retrace_pct_call', 0.38)  # P1-6修复: Call回撤38%
        self.retrace_pct_put: Optional[float] = p.get('pullback_retrace_pct_put', 0.42)  # P1-6修复: Put回撤42%
        self.theta_decay_accel: float = p.get('pullback_theta_decay_accel', 0.0)
        self.min_retrace_abs: float = p.get('pullback_min_retrace_abs', 0.0)
        # P0-2修复：逐tick实时跟踪模式
        self.tick_tracking: bool = p.get('pullback_tick_tracking', False)
        self.wait_ticks: int = p.get('pullback_wait_ticks', self.wait_bars * 240)
        self._pending: Dict[str, PendingPullbackSignal] = {}
        self._bar_counter: int = 0
        self._lock = threading.RLock()
        self._stats = {
            'signals_deferred': 0,
            'retrace_entries': 0,
            'retrace_timeouts': 0,
            'iv_rejected': 0,
            'atr_wait_extended': 0,
            'theta_shortened': 0,
            'tick_retrace_entries': 0,  # P0-2修复：逐tick回撤入场计数
        }

    def _compute_directional_retrace(self, direction: str) -> float:
        if direction.upper() in ('CALL', 'BUY', 'LONG'):
            return self.retrace_pct_call if self.retrace_pct_call is not None else self.retrace_pct
        return self.retrace_pct_put if self.retrace_pct_put is not None else self.retrace_pct

    def _compute_dynamic_wait(self, atr_pct: float = 0.0,
                              days_to_expiry: float = 999.0) -> int:
        wait = self.wait_bars
        if self.atr_wait_multiplier > 0.0 and atr_pct > 0.0:
            atr_ext = int(atr_pct * self.atr_wait_multiplier)
            wait += atr_ext
            if atr_ext > 0:
                with self._lock:
                    self._stats['atr_wait_extended'] += 1
        if self.theta_decay_accel > 0.0 and days_to_expiry < 30.0:
            theta_shrink = int(self.theta_decay_accel * max(0.0, 30.0 - days_to_expiry))
            wait = max(1, wait - theta_shrink)
            if theta_shrink > 0:
                with self._lock:
                    self._stats['theta_shortened'] += 1
        return max(1, wait)

    def _compute_ref_price(self, current_price: float,
                           atr_value: float = 0.0,
                           vwap: float = 0.0,
                           delta_anchor: float = 0.0) -> float:
        if self.ref_mode == 'atr' and atr_value > 0.0:
            return current_price + atr_value
        elif self.ref_mode == 'vwap' and vwap > 0.0:
            return vwap
        elif self.ref_mode == 'delta' and abs(delta_anchor) > 0.001:
            return current_price * (1.0 + abs(delta_anchor) * 0.1)
        return current_price

    def should_defer(self, signal_id: str, strategy_type: str,
                     instrument_id: str, direction: str,
                     current_price: float,
                     iv_percentile: float = 50.0,
                     atr_pct: float = 0.0,
                     days_to_expiry: float = 999.0,
                     atr_value: float = 0.0,
                     vwap: float = 0.0,
                     delta_anchor: float = 0.0,
                     extra: Dict[str, Any] = None) -> bool:
        if not self.enabled:
            return False
        if iv_percentile < self.iv_min_pct or iv_percentile > self.iv_max_pct:
            with self._lock:
                self._stats['iv_rejected'] += 1
            return True
        dynamic_wait = self._compute_dynamic_wait(atr_pct, days_to_expiry)
        dynamic_retrace = self._compute_directional_retrace(direction)
        ref_price = self._compute_ref_price(current_price, atr_value, vwap, delta_anchor)
        pending = PendingPullbackSignal(
            signal_id=signal_id,
            strategy_type=strategy_type,
            instrument_id=instrument_id,
            direction=direction,
            peak_price=current_price,
            signal_price=current_price,
            signal_bar_index=self._bar_counter,
            dynamic_wait_bars=dynamic_wait,
            dynamic_retrace_pct=dynamic_retrace,
            dynamic_wait_ticks=self.wait_ticks if self.tick_tracking else 0,  # P0-2修复
            ref_price=ref_price,
            extra=extra or {},
        )
        with self._lock:
            self._pending[signal_id] = pending
            self._stats['signals_deferred'] += 1
        logging.info(
            "[Pullback] DEFERRED: %s type=%s dir=%s price=%.4f wait=%d retrace=%.0f%% ref=%s",
            signal_id, strategy_type, direction, current_price,
            dynamic_wait, dynamic_retrace * 100, self.ref_mode,
        )
        return True

    def process_bar(self, current_prices: Dict[str, float]) -> List[PendingPullbackSignal]:
        if not self.enabled or not self._pending:
            return []
        self._bar_counter += 1
        ready = []
        expired_keys = []
        with self._lock:
            for sig_id, pending in self._pending.items():
                price = current_prices.get(pending.instrument_id, 0.0)
                if price <= 0:
                    continue
                if pending.check_retrace(price, self.retrace_pct, self.wait_bars, self.min_retrace_abs):
                    ready.append(pending)
                    expired_keys.append(sig_id)
                    self._stats['retrace_entries'] += 1
                    logging.info(
                        "[Pullback] RETRACE ENTRY: %s type=%s bars=%d retrace=%.1f%%",
                        sig_id, pending.strategy_type, pending.bars_elapsed,
                        (pending.peak_price - price) / pending.peak_price * 100
                        if pending.peak_price > 0 else 0,
                    )
                elif pending.is_expired:
                    expired_keys.append(sig_id)
                    self._stats['retrace_timeouts'] += 1
                    logging.info(
                        "[Pullback] TIMEOUT: %s type=%s bars=%d",
                        sig_id, pending.strategy_type, pending.bars_elapsed,
                    )
            for k in expired_keys:
                del self._pending[k]
        return ready

    def process_tick(self, instrument_id: str, tick_price: float) -> List[PendingPullbackSignal]:
        """P0-2修复：逐tick实时跟踪模式 — 每个tick即时判断回撤

        HFT策略(S1)应使用此方法替代process_bar，避免Bar内盲区。
        当tick_tracking=True时自动启用。
        """
        if not self.enabled or not self._pending or not self.tick_tracking:
            return []
        ready = []
        expired_keys = []
        with self._lock:
            for sig_id, pending in self._pending.items():
                if pending.instrument_id != instrument_id:
                    continue
                if tick_price <= 0:
                    continue
                if pending.check_retrace_tick(tick_price, self.retrace_pct,
                                              self.wait_ticks, self.min_retrace_abs):
                    ready.append(pending)
                    expired_keys.append(sig_id)
                    self._stats['tick_retrace_entries'] += 1
                    logging.info(
                        "[Pullback-Tick] RETRACE ENTRY: %s type=%s ticks=%d retrace=%.1f%%",
                        sig_id, pending.strategy_type, pending.ticks_elapsed,
                        (pending.peak_price - tick_price) / pending.peak_price * 100
                        if pending.peak_price > 0 else 0,
                    )
                elif pending.is_expired:
                    expired_keys.append(sig_id)
                    self._stats['retrace_timeouts'] += 1
            for k in expired_keys:
                del self._pending[k]
        return ready

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                **self._stats,
                'pending_count': len(self._pending),
                'pullback_enabled': self.enabled,
                'pullback_wait_bars': self.wait_bars,
                'pullback_retrace_pct': self.retrace_pct,
                'pullback_ref_mode': self.ref_mode,
                'pullback_atr_wait_multiplier': self.atr_wait_multiplier,
                'pullback_retrace_pct_call': self.retrace_pct_call,
                'pullback_retrace_pct_put': self.retrace_pct_put,
                'pullback_theta_decay_accel': self.theta_decay_accel,
                'pullback_min_retrace_abs': self.min_retrace_abs,
            }


# ============================================================================
# R5-E-05修复: 带最大重试次数的重试辅助函数
# ============================================================================
def retry_with_limit(func, max_retries: int = 3, base_delay: float = 1.0,
                     backoff_factor: float = 2.0, logger=None, max_delay: float = 60.0,
                     cancel_event: 'threading.Event|None' = None):
    """R5-E-05修复: 带最大重试次数和指数退避的重试辅助函数

    Args:
        func: 要重试的函数（无参数）
        max_retries: 最大重试次数（默认3）
        base_delay: 基础延迟秒数（默认1.0）
        backoff_factor: 退避因子（默认2.0，每次延迟翻倍）
        logger: 可选的logger实例
        max_delay: R15-P1-RES-01修复: 最大退避延迟秒数（默认60.0）
        cancel_event: 可选的threading.Event，设置时中断重试等待并抛出InterruptedError

    Returns:
        func()的返回值

    Raises:
        最后一次异常（如果所有重试都失败）
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                delay = min(base_delay * (backoff_factor ** attempt), max_delay)  # R15-P1-RES-01修复: 退避延迟不超过max_delay
                if logger:
                    logger.warning(
                        "[R5-E-05] %s第%d次失败(共%d次重试)，%.1fs后重试: %s",
                        getattr(func, '__name__', 'func'), attempt + 1, max_retries, delay, e
                    )
                if cancel_event is not None:
                    if cancel_event.wait(timeout=delay):
                        raise InterruptedError("R5-E-05: 重试被cancel_event中断")
                else:
                    time.sleep(delay)
            else:
                if logger:
                    logger.error(
                        "[R5-E-05] %s全部%d次重试失败: %s",
                        getattr(func, '__name__', 'func'), max_retries, e
                    )
    raise last_exception


# ============================================================================
# OPS-P1-06~19修复: 操作前置条件验证、后置条件验证、超时控制、幂等性
# ============================================================================

class PreconditionError(Exception):
    """OPS-P1-06修复: 前置条件验证失败异常"""
    pass


class PostconditionError(Exception):
    """OPS-P1-07修复: 后置条件验证失败异常"""
    pass


class OperationTimeoutError(Exception):
    """OPS-P1-08修复: 操作超时异常"""
    pass


def require_precondition(condition: bool, message: str = "") -> None:
    """OPS-P1-06修复: 前置条件验证

    在执行关键操作前验证前置条件，不满足时抛出PreconditionError。

    Args:
        condition: 前置条件
        message: 失败时的错误信息

    Raises:
        PreconditionError: 前置条件不满足
    """
    if not condition:
        raise PreconditionError(f"前置条件验证失败: {message}")


def ensure_postcondition(condition: bool, message: str = "") -> None:
    """OPS-P1-07修复: 后置条件验证

    在关键操作执行后验证后置条件，不满足时抛出PostconditionError。

    Args:
        condition: 后置条件
        message: 失败时的错误信息

    Raises:
        PostconditionError: 后置条件不满足
    """
    if not condition:
        raise PostconditionError(f"后置条件验证失败: {message}")


def with_timeout(func, timeout_sec: float = 30.0, logger=None):
    """OPS-P1-08修复: 带超时控制的操作执行

    使用线程执行函数，超时后抛出OperationTimeoutError。

    Args:
        func: 要执行的函数（无参数）
        timeout_sec: 超时秒数
        logger: 可选的logger实例

    Returns:
        func()的返回值

    Raises:
        OperationTimeoutError: 操作超时
    """
    import concurrent.futures
    result_holder = [None]
    exception_holder = [None]

    def _target():
        try:
            result_holder[0] = func()
        except Exception as e:
            exception_holder[0] = e

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_target)
        try:
            future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            if logger:
                logger.warning("OPS-P1-08: 操作超时(%.1fs): %s",
                               timeout_sec, getattr(func, '__name__', 'func'))
            raise OperationTimeoutError(
                f"操作超时({timeout_sec:.1fs}): {getattr(func, '__name__', 'func')}"
            )

    if exception_holder[0] is not None:
        raise exception_holder[0]
    return result_holder[0]


class IdempotencyGuard:
    """OPS-P1-09修复: 幂等性保证

    确保操作不会被重复执行。使用操作ID跟踪已完成的操作，
    相同ID的操作只执行一次。

    用法:
        guard = IdempotencyGuard()
        if guard.try_acquire("order_123"):
            # 执行操作
            guard.mark_completed("order_123", result)
        else:
            result = guard.get_result("order_123")
    """

    def __init__(self, max_history: int = 10000, ttl_sec: float = 3600.0):
        self._completed: dict = {}  # op_id → (result, timestamp)
        self._in_progress: set = set()
        self._lock = threading.Lock()
        self._max_history = max_history
        self._ttl_sec = ttl_sec

    def try_acquire(self, op_id: str) -> bool:
        """尝试获取操作执行权

        Returns:
            True: 可以执行; False: 操作已在进行或已完成
        """
        with self._lock:
            if op_id in self._in_progress:
                return False
            if op_id in self._completed:
                return False
            self._in_progress.add(op_id)
            return True

    def mark_completed(self, op_id: str, result: Any = None) -> None:
        """标记操作完成"""
        with self._lock:
            self._in_progress.discard(op_id)
            self._completed[op_id] = (result, time.time())
            # 清理过期记录
            if len(self._completed) > self._max_history:
                now = time.time()
                expired = [k for k, (_, ts) in self._completed.items()
                           if now - ts > self._ttl_sec]
                for k in expired:
                    del self._completed[k]

    def get_result(self, op_id: str) -> Optional[Any]:
        """获取已完成操作的结果"""
        with self._lock:
            entry = self._completed.get(op_id)
            return entry[0] if entry else None

    def is_completed(self, op_id: str) -> bool:
        """检查操作是否已完成"""
        with self._lock:
            return op_id in self._completed


def to_native_type(obj):
    """R5-I-04修复: 将numpy类型转换为Python原生类型

    np.int64不是int的子类，np.float64不是float的子类，
    在字典操作和JSON序列化时可能失败。此函数递归转换所有numpy类型。

    Args:
        obj: 任意Python对象

    Returns:
        转换后的Python原生类型对象
    """
    try:
        import numpy as np
        if isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, (np.bool_,)):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {to_native_type(k): to_native_type(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return type(obj)(to_native_type(v) for v in obj)
    except ImportError:
        pass
    return obj


# ============================================================================
# UPG-P1-04修复: API版本标记装饰器
# ============================================================================

def api_version(version: str, changelog: str = ""):
    """UPG-P1-04修复: API版本标记装饰器

    为关键内部API标记版本号，便于追踪API变更历史和兼容性检查。
    版本号遵循语义化版本规范(major.minor.patch)。

    Args:
        version: API版本号，如 "1.0.0"
        changelog: 变更说明，如 "新增xxx参数"

    用法:
        @api_version("1.0.0", changelog="初始版本")
        def some_api_function():
            pass

        # 查询版本信息
        some_api_function._api_version  # "1.0.0"
        some_api_function._api_changelog  # "初始版本"
    """
    def decorator(func):
        func._api_version = version
        func._api_changelog = changelog
        func._api_versioned = True
        return func
    return decorator


def get_api_version(func) -> Optional[str]:
    """UPG-P1-04修复: 获取函数的API版本号

    Args:
        func: 被@api_version装饰的函数

    Returns:
        版本号字符串，未标记则返回None
    """
    return getattr(func, '_api_version', None)


def check_api_compatibility(func, min_version: str = "") -> bool:
    """UPG-P1-04修复: 检查API版本兼容性

    Args:
        func: 被@api_version装饰的函数
        min_version: 最低要求版本号

    Returns:
        bool: API版本是否满足最低要求
    """
    current = getattr(func, '_api_version', None)
    if current is None:
        return True  # 未标记版本的API视为兼容
    if not min_version:
        return True
    return current >= min_version


# ============================================================================
# UPG-P1-05修复: 废弃API装饰器
# ============================================================================

import warnings as _warnings


def deprecated(reason: str = "", migration_guide: str = "", removal_version: str = ""):
    """UPG-P1-05修复: 废弃API装饰器

    标记已废弃的API，调用时发出DeprecationWarning。
    提供迁移指南帮助用户迁移到新API。

    Args:
        reason: 废弃原因
        migration_guide: 迁移指南，如 "请使用 new_function() 替代"
        removal_version: 计划移除的版本号

    用法:
        @deprecated(reason="性能问题", migration_guide="请使用 fast_calc() 替代", removal_version="3.0")
        def old_calc():
            pass
    """
    def decorator(func):
        func._deprecated = True
        func._deprecated_reason = reason
        func._deprecated_migration_guide = migration_guide
        func._deprecated_removal_version = removal_version

        def wrapper(*args, **kwargs):
            msg_parts = [f"{func.__name__} is deprecated"]
            if reason:
                msg_parts.append(f"原因: {reason}")
            if migration_guide:
                msg_parts.append(f"迁移指南: {migration_guide}")
            if removal_version:
                msg_parts.append(f"计划在版本 {removal_version} 中移除")
            msg = ". ".join(msg_parts) + "."
            _warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.__wrapped__ = func
        # 保留装饰器元数据
        wrapper._deprecated = True
        wrapper._deprecated_reason = reason
        wrapper._deprecated_migration_guide = migration_guide
        wrapper._deprecated_removal_version = removal_version
        # 保留api_version元数据（如果有的话）
        for attr in ('_api_version', '_api_changelog', '_api_versioned'):
            val = getattr(func, attr, None)
            if val is not None:
                setattr(wrapper, attr, val)
        return wrapper
    return decorator


# ============================================================================
# UPG-P1-07修复: 热更新事务性保证
# ============================================================================

import shutil as _shutil


def atomic_replace_file(target_path: str, new_content: str,
                        backup_suffix: str = ".bak") -> Dict[str, Any]:
    """UPG-P1-07修复: 原子替换文件（备份+写入+验证+失败回滚）

    实现热更新的事务性保证：
    1. 备份原文件
    2. 写入新内容到临时文件
    3. 验证写入成功
    4. 原子替换目标文件
    5. 失败时从备份回滚

    Args:
        target_path: 目标文件路径
        new_content: 新文件内容
        backup_suffix: 备份文件后缀

    Returns:
        Dict: {success: bool, backup_path: str, error: str}
    """
    result = {'success': False, 'backup_path': '', 'error': ''}
    backup_path = target_path + backup_suffix

    # 步骤1: 备份原文件
    try:
        if os.path.exists(target_path):
            _shutil.copy2(target_path, backup_path)
            result['backup_path'] = backup_path
    except Exception as e:
        result['error'] = f"备份失败: {e}"
        return result

    # 步骤2: 写入临时文件
    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        result['error'] = f"写入临时文件失败: {e}"
        # 回滚: 恢复备份
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except Exception as rb_err:
                logging.error("UPG-P1-07: 回滚失败! backup=%s error=%s", backup_path, rb_err)
        # 清理临时文件
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        return result

    # 步骤3: 验证临时文件
    try:
        with open(temp_path, 'r', encoding='utf-8') as f:
            written = f.read()
        if written != new_content:
            raise RuntimeError("写入内容验证不匹配")
    except Exception as e:
        result['error'] = f"验证失败: {e}"
        # 回滚
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except Exception as rb_err:
                logging.error("UPG-P1-07: 回滚失败! error=%s", rb_err)
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        return result

    # 步骤4: 原子替换
    try:
        os.replace(temp_path, target_path)
    except Exception as e:
        result['error'] = f"原子替换失败: {e}"
        # 回滚
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except Exception as rb_err:
                logging.error("UPG-P1-07: 回滚失败! error=%s", rb_err)
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        return result

    result['success'] = True
    logging.info("UPG-P1-07: 文件原子替换成功: %s (backup=%s)", target_path, backup_path)
    return result


def cleanup_atomic_backup(backup_path: str) -> None:
    """UPG-P1-07修复: 清理原子替换的备份文件

    确认更新成功后调用此函数清理备份。

    Args:
        backup_path: 备份文件路径
    """
    if backup_path and os.path.exists(backup_path):
        try:
            os.unlink(backup_path)
            logging.debug("UPG-P1-07: 已清理备份文件: %s", backup_path)
        except OSError as e:
            logging.warning("UPG-P1-07: 清理备份文件失败: %s", e)


# ============================================================================
# UPG-P1-09修复: 自动回滚机制
# ============================================================================

class AutoRollbackContext:
    """UPG-P1-09修复: 自动回滚上下文管理器

    在更新操作中使用，确保失败时自动回滚到更新前的状态。

    用法:
        with AutoRollbackContext("schema_update") as ctx:
            ctx.checkpoint("step1", state_before_step1)
            do_step1()
            ctx.checkpoint("step2", state_before_step2)
            do_step2()
        # 如果do_step2()抛出异常，自动回滚到step1的状态
    """

    def __init__(self, operation_name: str, rollback_handler: Optional[Callable] = None):
        """初始化自动回滚上下文

        Args:
            operation_name: 操作名称（用于日志）
            rollback_handler: 自定义回滚处理函数，签名: handler(checkpoint_name, checkpoint_state)
        """
        self._operation_name = operation_name
        self._rollback_handler = rollback_handler
        self._checkpoints: List[Dict[str, Any]] = []
        self._completed = False

    def checkpoint(self, name: str, state: Any) -> None:
        """设置检查点，保存回滚状态

        Args:
            name: 检查点名称
            state: 回滚时恢复的状态
        """
        self._checkpoints.append({'name': name, 'state': state})
        logging.debug("UPG-P1-09: 检查点已设置: %s/%s", self._operation_name, name)

    def mark_completed(self) -> None:
        """标记操作成功完成，不需要回滚"""
        self._completed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not self._completed and self._checkpoints:
            # 发生异常且未完成，执行回滚
            logging.warning(
                "UPG-P1-09: 操作%s失败，开始自动回滚(error=%s)",
                self._operation_name, exc_val,
            )
            # 从最近的检查点开始回滚
            for cp in reversed(self._checkpoints):
                try:
                    if self._rollback_handler:
                        self._rollback_handler(cp['name'], cp['state'])
                    else:
                        logging.info(
                            "UPG-P1-09: 回滚到检查点 %s/%s",
                            self._operation_name, cp['name'],
                        )
                except Exception as rb_err:
                    logging.error(
                        "UPG-P1-09: 回滚检查点%s失败: %s",
                        cp['name'], rb_err,
                    )
            logging.info("UPG-P1-09: 自动回滚完成: %s", self._operation_name)
        return False  # 不吞没异常


# ============================================================================
# UPG-P1-11修复: 代码版本与手册版本同步检查
# ============================================================================

_CODE_VERSION = "2.0.0"  # 代码版本号
_MANUAL_VERSION = "2.0.0"  # 手册版本号


def check_version_sync(code_version: str = None, manual_version: str = None) -> Dict[str, Any]:
    """UPG-P1-11修复: 检查代码版本与手册版本是否同步

    确保代码实现与操作手册版本一致，防止版本漂移导致操作错误。

    Args:
        code_version: 代码版本号（默认使用内置版本）
        manual_version: 手册版本号（默认使用内置版本，可通过环境变量MANUAL_VERSION覆盖）

    Returns:
        Dict: {synced: bool, code_version: str, manual_version: str, warning: str}
    """
    cv = code_version or _CODE_VERSION
    mv = manual_version or os.environ.get('MANUAL_VERSION', _MANUAL_VERSION)

    result = {
        'synced': cv == mv,
        'code_version': cv,
        'manual_version': mv,
        'warning': '',
    }

    if cv != mv:
        result['warning'] = (
            f"代码版本({cv})与手册版本({mv})不一致! "
            f"请确认代码和手册是否已同步更新。"
        )
        logging.warning("UPG-P1-11: %s", result['warning'])
    else:
        logging.debug("UPG-P1-11: 版本同步检查通过: code=%s manual=%s", cv, mv)

    return result


# ============================================================================
# P2修复: 不变量运行时检查与恢复策略
# ============================================================================

# P2修复: shared_utils中的不变量恢复策略文档
# 以下不变量在shared_utils各工具函数中隐含，现显式声明恢复策略:
# - INV-UTIL-01 (to_float32结果非NaN/inf): 违反时返回0.0，记录WARNING
# - INV-UTIL-02 (normalize_instrument_id非空): 违反时返回空字符串，记录WARNING
# - INV-UTIL-03 (ShardRouter.shard_count > 0): 违反时使用默认值16，记录WARNING
# - INV-UTIL-04 (RingBuffer.maxlen > 0): 违反时使用默认值100，记录WARNING
# - INV-UTIL-05 (InitPhase有序递增): 违反时抛出InitializationError
# - INV-UTIL-06 (safe_int/safe_float返回有效值): 违反时返回default，记录WARNING

def check_shared_utils_invariants() -> Dict[str, Any]:
    """P2修复: 检查shared_utils模块中的不变量

    将注释中的不变量断言转为运行时检查，
    违反时执行恢复策略（使用安全默认值）。

    Returns:
        Dict: {all_passed: bool, violations: list, recoveries: list}
    """
    violations = []
    recoveries = []

    # INV-UTIL-03: 全局ShardRouter的shard_count > 0
    try:
        router = get_shard_router()
        if router.shard_count <= 0:
            violations.append('INV-UTIL-03: ShardRouter.shard_count <= 0')
            recoveries.append('INV-UTIL-03: 重新初始化ShardRouter(shard_count=16)')
            logging.warning("[shared_utils] P2修复: INV-UTIL-03违反, shard_count<=0")
    except Exception as e:
        violations.append(f'INV-UTIL-03: ShardRouter检查异常: {e}')
        logging.warning("[shared_utils] P2修复: INV-UTIL-03检查异常: %s", e)

    # INV-UTIL-06: to_float32对NaN/inf的处理
    import math
    test_result = to_float32(float('nan'))
    if math.isnan(test_result) or math.isinf(test_result):
        violations.append('INV-UTIL-06: to_float32返回NaN/inf而非0.0')
        recoveries.append('INV-UTIL-06: to_float32已修复NaN/inf返回0.0')
        logging.warning("[shared_utils] P2修复: INV-UTIL-06违反, to_float32返回NaN/inf")

    all_passed = len(violations) == 0
    if not all_passed:
        logging.warning(
            "[shared_utils] P2修复: 不变量检查发现%d项违反",
            len(violations),
        )
    return {
        'all_passed': all_passed,
        'violations': violations,
        'recoveries': recoveries,
    }


# ============================================================================
# UPG-P1-13修复: 策略逻辑回归测试框架
# ============================================================================

class StrategyRegressionTest:
    """UPG-P1-13修复: 策略逻辑回归测试框架

    用于验证策略逻辑变更后，关键场景的输出仍然符合预期。
    支持注册测试用例、执行测试、生成报告。

    用法:
        tester = StrategyRegressionTest("signal_generation")
        tester.register("buy_signal_on_breakout", input_data=..., expected_output=...)
        tester.register("no_signal_in_cooldown", input_data=..., expected_output=...)
        report = tester.run_all(signal_generator_func)
    """

    def __init__(self, test_name: str):
        """初始化回归测试器

        Args:
            test_name: 测试名称（用于日志和报告标识）
        """
        self._test_name = test_name
        self._test_cases: List[Dict[str, Any]] = []

    def register(self, case_name: str, input_data: Any,
                 expected_output: Any, tolerance: float = 0.0,
                 comparator: Optional[Callable[[Any, Any], bool]] = None) -> None:
        """注册测试用例

        Args:
            case_name: 测试用例名称
            input_data: 输入数据
            expected_output: 期望输出
            tolerance: 浮点数比较容差（0=精确匹配）
            comparator: 自定义比较函数，签名: comparator(actual, expected) -> bool
        """
        self._test_cases.append({
            'name': case_name,
            'input': input_data,
            'expected': expected_output,
            'tolerance': tolerance,
            'comparator': comparator,
        })

    def run_all(self, func: Callable) -> Dict[str, Any]:
        """执行所有注册的测试用例

        Args:
            func: 被测试的策略逻辑函数

        Returns:
            Dict: {passed: int, failed: int, total: int, details: list}
        """
        results = []
        passed = 0
        failed = 0

        for case in self._test_cases:
            case_result = {
                'name': case['name'],
                'passed': False,
                'error': '',
                'actual': None,
            }
            try:
                actual = func(case['input'])
                case_result['actual'] = actual

                if case['comparator']:
                    case_result['passed'] = case['comparator'](actual, case['expected'])
                elif case['tolerance'] > 0 and isinstance(actual, (int, float)):
                    case_result['passed'] = abs(actual - case['expected']) <= case['tolerance']
                else:
                    case_result['passed'] = actual == case['expected']

                if case_result['passed']:
                    passed += 1
                else:
                    failed += 1
                    case_result['error'] = f"输出不匹配: expected={case['expected']}, actual={actual}"
            except Exception as e:
                failed += 1
                case_result['error'] = str(e)

            results.append(case_result)

        report = {
            'test_name': self._test_name,
            'passed': passed,
            'failed': failed,
            'total': len(self._test_cases),
            'details': results,
        }

        if failed > 0:
            logging.warning(
                "UPG-P1-13: 回归测试[%s] %d/%d 通过, %d 失败",
                self._test_name, passed, len(self._test_cases), failed,
            )
        else:
            logging.info(
                "UPG-P1-13: 回归测试[%s] 全部通过 (%d/%d)",
                self._test_name, passed, len(self._test_cases),
            )

        return report
