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
import struct
import threading
import logging
import time
from typing import Any, Dict, List, Optional
from collections import deque


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

    Args:
        value: 输入数值

    Returns:
        float: float32精度的数值
    """
    try:
        return struct.unpack('f', struct.pack('f', float(value)))[0]
    except (TypeError, ValueError, OverflowError):
        return 0.0


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
            current_year = datetime.now().year
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
        return int(float(value))
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


class RingBuffer:
    """环形缓冲区，线程安全"""

    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.data = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, item):
        with self._lock:
            self.data.append(item)

    def extend(self, items):
        with self._lock:
            self.data.extend(items)

    def __len__(self):
        with self._lock:
            return len(self.data)

    def __getitem__(self, index):
        with self._lock:
            return self.data[index]

    def __iter__(self):
        with self._lock:
            return iter(list(self.data))

    def clear(self):
        with self._lock:
            self.data.clear()

    def to_list(self):
        with self._lock:
            return list(self.data)


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

    def stop_all(self, stop_event: threading.Event, timeout: float = 30.0) -> dict:
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

    def _force_stop_partial(self, started_names: list) -> None:
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

    def health_check(self) -> dict:
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


@dataclass
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
    is_expired: bool = False
    dynamic_wait_bars: int = 0
    dynamic_retrace_pct: float = 0.0
    ref_price: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    def check_retrace(self, current_price: float,
                      retrace_pct: float,
                      wait_bars: int,
                      min_retrace_abs: float = 0.0) -> bool:
        if self.is_expired:
            return False
        self.bars_elapsed += 1
        effective_wait = self.dynamic_wait_bars if self.dynamic_wait_bars > 0 else wait_bars
        effective_retrace = self.dynamic_retrace_pct if self.dynamic_retrace_pct > 0.0 else retrace_pct
        if self.bars_elapsed > effective_wait:
            self.is_expired = True
            return False
        ref = self.ref_price if self.ref_price > 0.0 else self.peak_price
        if current_price > ref:
            self.peak_price = current_price
            if self.ref_price <= 0.0:
                self.ref_price = current_price
            self.peak_bar_index += self.bars_elapsed
        if ref <= 0:
            return False
        retrace_abs = ref - current_price
        retrace_pct_actual = retrace_abs / ref
        if retrace_pct_actual < 0:
            return False
        if min_retrace_abs > 0.0 and retrace_abs < min_retrace_abs:
            return False
        return retrace_pct_actual >= effective_retrace


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
        self.retrace_pct_call: Optional[float] = p.get('pullback_retrace_pct_call', None)
        self.retrace_pct_put: Optional[float] = p.get('pullback_retrace_pct_put', None)
        self.theta_decay_accel: float = p.get('pullback_theta_decay_accel', 0.0)
        self.min_retrace_abs: float = p.get('pullback_min_retrace_abs', 0.0)
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
