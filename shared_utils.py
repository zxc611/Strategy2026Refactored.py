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

    if len(year_month) == 6 and year_month.isdigit():
        return year_month[2:]

    if len(year_month) == 4 and 'M' in year_month.upper():
        import re
        m = re.match(r'(\d)M(\d{2})', year_month, re.IGNORECASE)
        if m:
            month_digit = m.group(1)
            year_suffix = m.group(2)
            return f'2{month_digit}{year_suffix}'

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
        return len(self.data)

    def __getitem__(self, index):
        return self.data[index]

    def __iter__(self):
        return iter(self.data)

    def clear(self):
        self.data.clear()

    def to_list(self):
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
        if not product_code:
            product_code = 'unknown'

        with self._binding_lock:
            if product_code in self._binding_map:
                return self._binding_map[product_code]

        h = self._deterministic_hash(product_code)
        shard_idx = h % self._shard_count

        with self._binding_lock:
            self._binding_map[product_code] = shard_idx

        return shard_idx

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
_DEFAULT_SHARD_COUNT = 16


def get_shard_router(shard_count: int = None) -> ShardRouter:
    """获取全局单例 ShardRouter（方法唯一原则）

    全链路只创建一个 ShardRouter 实例，确保第一层和第二层路由完全一致。
    """
    global _GLOBAL_SHARD_ROUTER
    if shard_count is None:
        shard_count = _DEFAULT_SHARD_COUNT
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
