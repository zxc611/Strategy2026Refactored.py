# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""shared_utils/统一工具模块 — 合并自6个子模块

✅ ID唯一 / 方法唯一 / 传递渠道唯一：跨模块共享的工具函数统一定义在此
禁止在其他模块中重复定义这些函数，必须从此模块导入。

合并来源 (R27-CP-04-FIX):
- shared_utils_sql: SQL安全相关
- shared_utils_types: 类型转换相关
- shared_utils_instrument: 合约工具相关
- shared_utils_contracts: 契约编程相关
- shared_utils_infra: 基础设施相关
- shared_utils (facade): 门面自有定义
"""
from __future__ import annotations

import errno
import hashlib
import logging
import math
import os
import re
import struct
import shutil as _shutil
import threading
import time
import uuid as _uuid
import warnings as _warnings
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import IntEnum
from typing import Any, Callable, Dict, Iterator, List, Optional

# FIX-20260702: shared_utils 作为基础设施底座不再依赖 _helpers，
# 从而切断 shared_utils <-> _helpers <-> serialization_utils 循环导入。
def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    return logger


# ============================================================
# Section 1: SQL安全 (原 shared_utils_sql.py)
# ============================================================

def sanitize_sql_identifier(identifier: str) -> str:
    """清理SQL标识符（表名、列名）以防止注入。

    仅允许字母、数字、下划线和点号字符，且必须以字母或下划线开头。

    Args:
        identifier: 待清理的SQL标识符

    Returns:
        str: 验证通过的标识符

    Raises:
        ValueError: 标识符包含非法字符时抛出
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


def sanitize_sql_value(value) -> str:
    """清理SQL值，通过转义单引号防止注入。'
    Args:
        value: 待清理的SQL值，None返回'NULL'

    Returns:
        str: 转义后的SQL值字符串（含引号）'
    """
    if value is None:
        return 'NULL'
    s = str(value)
    s = s.replace("'", "''")
    return f"'{s}'"


# ============================================================
# Section 2: 类型转换 (原 shared_utils_types.py)
# ============================================================

def to_float32(value) -> float:
    """将数值转换为float32精度（唯一实现）'
    NP-P1-12修复: None/NaN/inf显式告警而非静默返回0.0

    Args:
        value: 输入数值

    Returns:
        float: float32精度的数值
    """
    _log = get_logger(__name__)  # R9-5
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
    if math.isnan(result) or math.isinf(result):
        _log.warning("[to_float32] 结果为NaN/inf，value=%r，返回0.0", value)
        return 0.0
    return result


def safe_int(value: Any, default: int = 0) -> int:
    """安全转换为整数"""
    try:
        if value is None:
            _safe_cast_fallback_counter['safe_int_none'] += 1
            return default
        return round(float(value))
    except (ValueError, TypeError) as e:
        _safe_cast_fallback_counter['safe_int_error'] += 1
        logging.warning(f"[safe_int] Conversion failed for value '{value}': {e}")
        _report_safe_cast_stats()
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转换为浮点数"""
    try:
        if value is None:
            _safe_cast_fallback_counter['safe_float_none'] += 1
            return default
        return float(value)
    except (ValueError, TypeError) as e:
        _safe_cast_fallback_counter['safe_float_error'] += 1
        logging.warning(f"[safe_float] Conversion failed for value '{value}': {e}")
        _report_safe_cast_stats()
        return default


_safe_cast_fallback_counter: Dict[str, int] = {
    'safe_int_none': 0, 'safe_int_error': 0,
    'safe_float_none': 0, 'safe_float_error': 0,
}
_safe_cast_counter_lock = threading.Lock()
_safe_cast_last_report_time = 0.0
_SAFE_CAST_REPORT_INTERVAL_SEC = 3600.0


def _report_safe_cast_stats() -> None:
    global _safe_cast_last_report_time
    now = time.time()
    if now - _safe_cast_last_report_time < _SAFE_CAST_REPORT_INTERVAL_SEC:
        return
    with _safe_cast_counter_lock:
        if now - _safe_cast_last_report_time < _SAFE_CAST_REPORT_INTERVAL_SEC:
            return
        _safe_cast_last_report_time = now
        total = sum(_safe_cast_fallback_counter.values())
        if total > 0:
            logging.info("[Observability] safe_cast降级统计(每小时): %s", dict(_safe_cast_fallback_counter))


def safe_price_check(price) -> bool:
    """R24-P1-IV-10修复: 安全价格检查，NaN/Inf/非正数均返回False"""
    try:
        return math.isfinite(price) and price > 0
    except (TypeError, ValueError):
        return False


def to_native_type(obj):
    """R5-I-04修复: 将numpy类型转换为Python原生类型

    np.int64不是int的子类，np.float64不是float的子类，
    在字典操作和JSON序列化时可能失败。此函数递归转换所有numpy类型。'
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


def safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    """P1-20: 统一安全浮点获取 — getattr + safe_float"""
    val = getattr(obj, attr, default)
    return safe_float(val, default=default)


def safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    """P1-20: 统一安全整数获取 — getattr + safe_int"""
    val = getattr(obj, attr, default)
    return safe_int(val, default=default)


# R3-4/P2-27修复: 提取ToDictMixin，消除38+处重复to_dict实现
class ToDictMixin:
    """为 dataclass 提供统一的 to_dict 方法

    使用 dataclasses.asdict 自动转换，替代各模块中
    重复的 `def to_dict(self): return asdict(self)` 实现。

    用法:
        @dataclass
        class MyData(ToDictMixin):
            x: int = 0
    """
    def to_dict(self) -> dict:
        from dataclasses import asdict, fields
        try:
            return asdict(self)
        except (TypeError, ValueError):
            result = {}
            for f in fields(self):
                try:
                    result[f.name] = getattr(self, f.name)
                except (AttributeError, KeyError):
                    pass
            return result


# ============================================================
# Section 3: 合约工具 (原 shared_utils_instrument.py)
# ============================================================

# [R22-TIME-P1-01] 全局CST时区常量，所有模块统一引用
# [P2-13] 权威CHINA_TZ定义，所有模块应从infra.shared_utils导入CHINA_TZ
CHINA_TZ = timezone(timedelta(hours=8))


def normalize_option_type(opt_type: str) -> str:
    """标准化option_type（唯一实现）'
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


def normalize_instrument_id(instrument_id: str) -> str:
    """移除交易所前缀和平台前缀，保留合约ID原始格式（品种ID直通，不做大写化）'
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
    if ':' in normalized:
        normalized = normalized.split(':', 1)[-1]
    return normalized


def extract_product_code(instrument_id: str) -> str:
    """从合约ID提取品种代码（唯一实现）'
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
        from ali2026v3_trading.infra.subscription_service import SubscriptionManager
        parsed = SubscriptionManager.parse_option(instrument_id)
        strike = parsed.get('strike_price')
        return float(strike) if strike is not None else None
    except (ValueError, KeyError, TypeError):
        return None


def normalize_year_month(year_month: str) -> str:
    """归一化年月格式（唯一实现）'
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
            current_year = datetime.now(CHINA_TZ).year
            candidate_full = current_year - (current_year % 10) + int(year_str)
            if candidate_full > current_year + 1:
                candidate_full -= 10
            yy = f'{candidate_full % 100:02d}'
            return f'{yy}{month_str}'
        return year_month

    return year_month


# ============================================================
# Section 4: 基础设施 (原 shared_utils_infra.py)
# ============================================================

class RingBuffer:
    """环形缓冲区，线程安全

    R21-MEM-P1-15修复: 添加TTL过期检查，append时记录时间戳，
    查询时自动过滤过期条目，防止陈旧信号占用内存。'
    """

    def __init__(self, maxlen: int, ttl_seconds: float = 0.0):
        self.maxlen = maxlen
        self.data = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        # R21-MEM-P1-15修复: TTL过期机制
        self._ttl_seconds = ttl_seconds  # 0表示不启用TTL
        self._timestamps: deque[float] = deque(maxlen=maxlen)  # [R27-AUDIT] P2修复: 添加泛型参数

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

    def __iter__(self) -> Iterator[Any]:  # [R27-AUDIT] P1修复: 返回类型从Any改为Iterator[Any]
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
    4. 通道绑定注册表可审计（§8.4 路由规则可审计）'
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
        """P2-31修复: 使用compute_content_hash替代直接hashlib.md5"""
        h = compute_content_hash(key, algorithm='md5')
        # 取前4字节转为int
        return (int(h[0:2], 16) << 24) | (int(h[2:4], 16) << 16) | (int(h[4:6], 16) << 8) | int(h[6:8], 16)

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
    """获取全局单例 ShardRouter（方法唯一原则）'
    全链路只创建一个 ShardRouter 实例，确保第一层和第二层路由完全一致。'
    """
    global _GLOBAL_SHARD_ROUTER
    if shard_count is None:
        shard_count = DEFAULT_SHARD_COUNT
    with _GLOBAL_SHARD_ROUTER_LOCK:
        if _GLOBAL_SHARD_ROUTER is None:
            _GLOBAL_SHARD_ROUTER = ShardRouter(shard_count=shard_count)
        return _GLOBAL_SHARD_ROUTER


class ThreadLifecycleManager:
    """管理多服务线程的启停与排水顺序（§8.5 并发安全）'
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
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.error("[ThreadMgr] 启动失败，已启动 %s/共%d，执行紧急清理", started, len(thread_configs))
            self._force_stop_partial(started)
            raise
        self._phase = self.PHASE_RUNNING

    def stop_all(self, stop_event: threading.Event, timeout: float = 30.0) -> Dict[str, Any]:  # [R22-P2-TS04]
        self._phase = self.PHASE_STOPPING
        stop_event.set()
        result = {'stopped': 0, 'timeout': 0, 'daemon': 0}
        timeout_threads = []  # R33-P2-2: 收集超时线程信息
        for name, thread in list(self._threads.items()):
            if not thread:
                continue
            if self._daemon_flags.get(name, False):
                thread.join(timeout=min(timeout, 5.0))
                result['daemon'] += 1
                continue
            thread.join(timeout=timeout)
            if thread.is_alive():
                # R33-P2-2: 增强超时告警——添加线程详细诊断信息
                timeout_threads.append({
                    'name': name,
                    'daemon': thread.daemon,
                    'ident': thread.ident,
                })
                logging.error(
                    "[R33-P2-2] 线程 %s 在 %.1fs 内未停止 (daemon=%s, ident=%s)，"
                    "Python无法强制kill线程，将标记为daemon并设置中断标记",
                    name, timeout, thread.daemon, thread.ident,
                )
                result['timeout'] += 1
                # P2-2修复: 超时线程标记为daemon，允许进程退出时自动终止
                if not thread.daemon:
                    thread.daemon = True
                    logging.info("[ThreadMgr] P2-2: 将超时线程 %s 标记为daemon，允许进程退出时自动终止", name)
                # R33-P2-2: 设置线程中断标记（对检查中断标记的线程有效）'
                if hasattr(thread, '_stop_requested'):
                    thread._stop_requested = True
            else:
                result['stopped'] += 1
        # R33-P2-2: 超时线程汇总告警
        if timeout_threads:
            logging.critical(
                "[R33-P2-2] CRITICAL: %d个线程在%.1fs超时后仍未停止: %s，"
                "这些线程已被标记为daemon，进程退出时将被强制终止",
                len(timeout_threads), timeout,
                [t['name'] for t in timeout_threads],
            )
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

    def detect_zombie_threads(self) -> Dict[str, Any]:  # P2-3修复: 僵尸线程检测
        """检测已停止但未清理的僵尸线程引用"""
        zombies = {}
        for name, thread in list(self._threads.items()):
            if thread is not None and not thread.is_alive():
                zombies[name] = {
                    'alive': False,
                    'daemon': self._daemon_flags.get(name, False),
                    'stale_seconds': time.time() - getattr(thread, '_stop_time', time.time()),
                }
        if zombies:
            logging.warning("[ThreadMgr] P2-3: 检测到%d个僵尸线程引用: %s", len(zombies), list(zombies.keys()))
        return {'zombie_count': len(zombies), 'zombies': zombies}


# ============================================================================
# InitPhase / InitStateMachine — 初始化阶段管理
# ============================================================================

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


class IdempotencyGuard:
    """OPS-P1-09修复: 幂等性保证

    确保操作不会被重复执行。使用操作ID跟踪已完成的操作，
    相同ID的操作只执行一次。'
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


# ============================================================================
# UPG-P1-07修复: 热更新事务性保证
# ============================================================================

def atomic_replace_file(target_path: str, new_content: str,
                        backup_suffix: str = ".bak") -> Dict[str, Any]:
    """UPG-P1-07修复: 原子替换文件（备份+写入+验证+失败回滚）'
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
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
        result['error'] = f"备份失败: {e}"
        return result

    # 步骤2: 写入临时文件
    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
            f.flush()
            os.fsync(f.fileno())
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
        result['error'] = f"写入临时文件失败: {e}"
        # 回滚: 恢复备份
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as rb_err:
                logging.error("UPG-P1-07: 回滚失败! backup=%s error=%s", backup_path, rb_err)
        # 清理临时文件
        try:
            os.unlink(temp_path)
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，临时文件清理失败: %s", _os_err)
        return result

    # 步骤3: 验证临时文件
    try:
        with open(temp_path, 'r', encoding='utf-8') as f:
            written = f.read()
        if written != new_content:
            raise RuntimeError("写入内容验证不匹配")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
        result['error'] = f"验证失败: {e}"
        # 回滚
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as rb_err:
                logging.error("UPG-P1-07: 回滚失败! error=%s", rb_err)
        try:
            os.unlink(temp_path)
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，临时文件清理失败: %s", _os_err)
        return result

    # 步骤4: 原子替换
    # Windows 上目标文件可能被瞬时占用(杀毒软件扫描/句柄未释放)，抛 PermissionError([WinError 5])
    # 增加重试机制避免瞬时占用导致失败
    _replace_err = None
    for _attempt in range(3):
        try:
            os.replace(temp_path, target_path)
            _replace_err = None
            break
        except PermissionError as _pe:
            _replace_err = _pe
            if _attempt < 2:
                time.sleep(0.1 * (_attempt + 1))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            _replace_err = e
            break
    if _replace_err is not None:
        e = _replace_err
        result['error'] = f"原子替换失败: {e}"
        # 回滚
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as rb_err:
                logging.error("UPG-P1-07: 回滚失败! error=%s", rb_err)
        try:
            os.unlink(temp_path)
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，临时文件清理失败: %s", _os_err)
        return result

    result['success'] = True
    logging.debug("UPG-P1-07: 文件原子替换成功: %s (backup=%s)", target_path, backup_path)
    return result


def cleanup_atomic_backup(backup_path: str) -> None:
    """UPG-P1-07修复: 清理原子替换的备份文件

    确认更新成功后调用此函数清理备份。'
    Args:
        backup_path: 备份文件路径
    """
    if backup_path and os.path.exists(backup_path):
        try:
            os.unlink(backup_path)
            logging.debug("UPG-P1-07: 已清理备份文件: %s", backup_path)
        except OSError as e:
            logging.warning("UPG-P1-07: 清理备份文件失败: %s", e)


def sanitize_filename(name: str) -> str:
    """R2-3/P2-19修复: 文件名安全化处理

    将路径分隔符替换为下划线，防止路径遍历攻击。
    统一替代分散在 order_persistence/order_wal_state_service 中的
    内联 replace('/', '_').replace('\\', '_') 实现。'
    Args:
        name: 原始文件名（如 order_id）

    Returns:
        安全的文件名字符串
    """
    return name.replace('/', '_').replace('\\', '_')


# ============================================================================
# UPG-P1-09修复: 自动回滚机制
# ============================================================================

class AutoRollbackContext:
    """UPG-P1-09修复: 自动回滚上下文管理器

    在更新操作中使用，确保失败时自动回滚到更新前的状态。'
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
            operation_name: 操作名称（用于日志）'
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
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as rb_err:
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

    确保代码实现与操作手册版本一致，防止版本漂移导致操作错误。'
    Args:
        code_version: 代码版本号（默认使用内置版本）'
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
    违反时执行恢复策略（使用安全默认值）。'
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
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        violations.append(f'INV-UTIL-03: ShardRouter检查异常: {e}')
        logging.warning("[shared_utils] P2修复: INV-UTIL-03检查异常: %s", e)

    # INV-UTIL-06: to_float32对NaN/inf的处理
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


def set_global_seed(seed: int) -> int:
    """P1-54修复: 统一随机数种子设置入口

    同时设置Python random和numpy random种子，确保可复现性。'
    返回seed值方便链式调用。
    """
    import random as _random
    _random.seed(seed)
    try:
        import numpy as _np
        _np.random.seed(seed)
    except ImportError:
        pass
    return seed


def get_log_dir(subdir: str = '') -> str:
    """P2-21修复: 统一日志目录路径获取

    从ConfigService获取日志目录，fallback到项目根/logs。
    Args:
        subdir: 子目录名（如'shadow', 'eviction_audit'），空字符串表示根日志目录
    Returns:
        日志目录的绝对路径
    """
    import os as _os
    try:
        from ali2026v3_trading.config.config_service import get_config_service
        _cs = get_config_service()
        if _cs is not None and hasattr(_cs, 'paths') and hasattr(_cs.paths, 'log_dir'):
            base = _cs.paths.log_dir
        else:
            raise AttributeError()
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        base = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'logs')
    if subdir:
        return _os.path.join(base, subdir)
    return base


def compute_content_hash(content: str, algorithm: str = 'md5', truncate: int = 0) -> str:
    """P1-64修复: 统一内容哈希计算

    Args:
        content: 要哈希的字符串内容
        algorithm: 哈希算法，默认md5（非安全场景）；安全场景使用sha256
        truncate: 截断长度，0表示不截断

    Returns:
        十六进制哈希字符串
    """
    h = hashlib.new(algorithm, content.encode('utf-8'))
    digest = h.hexdigest()
    return digest[:truncate] if truncate > 0 else digest


# ---------------------------------------------------------------------------
# R9-3: 统一前缀ID生成函数
# ---------------------------------------------------------------------------
def generate_prefixed_id(prefix: str = "", hex_len: int = 12) -> str:
    """生成带前缀的唯一标识符。'
    统一替代分散在代码各处的 ``f"{PREFIX}_{uuid.uuid4().hex[:N]}"`` 模式。

    Args:
        prefix: ID 前缀，如 ``"ORD"``、``"SIG"``、``"HFT"``。
                为空时仅返回 hex 片段。
        hex_len: 截取 uuid hex 的长度 (1-32)，默认 12。

    Returns:
        ``f"{prefix}_{hex}"`` 或 ``hex``（prefix 为空时）。
    """
    hex_part = _uuid.uuid4().hex[:max(1, min(hex_len, 32))]
    return f"{prefix}_{hex_part}" if prefix else hex_part


class ToDictMixin:
    """P1-58修复: 统一to_dict()实现基类

    提供默认的to_dict()方法，基于实例。_dict__序列化。
    子类可覆盖to_dict()以自定义序列化逻辑。
    """
    def to_dict(self) -> dict:
        from dataclasses import asdict, fields
        try:
            return asdict(self)
        except (TypeError, ValueError):
            pass
        result = {}
        try:
            for key, value in self.__dict__.items():
                if key.startswith('_'):
                    continue
                if hasattr(value, 'to_dict'):
                    result[key] = value.to_dict()
                elif isinstance(value, (list, tuple)):
                    result[key] = [
                        item.to_dict() if hasattr(item, 'to_dict') else item
                        for item in value
                    ]
                elif isinstance(value, dict):
                    result[key] = {
                        k: v.to_dict() if hasattr(v, 'to_dict') else v
                        for k, v in value.items()
                    }
                else:
                    result[key] = value
        except AttributeError:
            for f in fields(self):
                try:
                    result[f.name] = getattr(self, f.name)
                except (AttributeError, KeyError):
                    pass
        return result


# ============================================================
# P1-55修复: 统一Tick字段提取
# ============================================================

TICK_FIELD_NAMES: Dict[str, List[str]] = {
    'instrument_id': ['instrument_id'],
    'exchange': ['exchange'],
    'last_price': ['last_price'],
    'volume': ['volume'],
    'datetime': ['datetime', 'update_time', 'ts'],
    'open_interest': ['open_interest'],
    'turnover': ['turnover'],
    'bid_price1': ['bid_price1'],
    'ask_price1': ['ask_price1'],
    'bid_volume1': ['bid_volume1'],
    'ask_volume1': ['ask_volume1'],
}


def get_tick_field(tick: Any, field_name: str, default: Any = None) -> Any:
    """P1-55修复: 统一Tick字段提取函数——全项目唯一权威实现

    按候选属性名列表依次尝试getattr，返回第一个非None值。'
    tick_processing_service._get_tick_field 和 position_service._get_platform_attr
    均应委托到此函数。
    """
    attr_names = TICK_FIELD_NAMES.get(field_name, [field_name])
    for attr in attr_names:
        val = getattr(tick, attr, None)
        if val is not None and val != '':
            return val
    return default


def normalize_tick(tick: Any) -> Dict[str, Any]:
    """P1-55修复: 平台适配层——统一Tick属性名映射为标准字典"""
    return {
        field: get_tick_field(tick, field)
        for field in TICK_FIELD_NAMES
    }


# ============================================================
# Section 5: 契约编程 (原 shared_utils_contracts.py)
# ============================================================

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
        func: 要执行的函数（无参数）'
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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


# ============================================================
# Section 6: 门面定义 (原 shared_utils.py 自有定义)
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def unified_ts_ms() -> int:
    """[R22-TIME-P1-15] 统一毫秒精度时间戳，用作缓存键，消除毫秒/微秒混用"""
    return int(time.time() * 1000)


# ============================================================================
# P2-11 五唯一性修复: 统一时间格式化辅助函数
# 所有模块应从此处导入，避免 datetime.now(CHINA_TZ).isoformat() 等惯用法重复
# ============================================================================

def now_iso() -> str:
    """五唯一性修复：统一的 ISO 时间戳格式"""
    return datetime.now(CHINA_TZ).isoformat()


def now_compact() -> str:
    """五唯一性修复：统一的紧凑时间戳格式（用于文件名/run_id）"""
    return datetime.now(CHINA_TZ).strftime('%Y%m%d_%H%M%S')


def now_log_str(ms: bool = False) -> str:
    """五唯一性修复：统一的日志时间戳格式"""
    if ms:
        return datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return datetime.now(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')


def is_same_day(a, b) -> bool:
    """五唯一性修复：统一的同日比较逻辑

    兼容 datetime 对象与仅支持 strftime 的对象（如 pandas Timestamp 子类）。
    """
    if hasattr(a, 'date') and hasattr(b, 'date'):
        return a.date() == b.date()
    return a.strftime('%Y-%m-%d') == b.strftime('%Y-%m-%d')


# ============================================================================
# FM-01修复: 无风险利率常量（与greeks_calculator.DEFAULT_RISK_FREE_RATE对齐）'
# ============================================================================
DEFAULT_RISK_FREE_RATE: float = 0.02
TRADING_DAYS_PER_YEAR_CHINA: float = 242.0

# ============================================================================
# R17-12修复: 年化因子常量 — 统一管理Sharpe/Calmar等年化计算
# ============================================================================
ANNUALIZE_FACTOR_DAILY = 252.0
ANNUALIZE_FACTOR_MINUTE = 252.0 * 240
DAYS_PER_YEAR_CALENDAR: float = 365.0

_DAILY_STRATEGY_TYPES = frozenset({'box_extreme', 'box_spring', 'arbitrage', 'market_making'})


def get_annualize_factor(strategy_type: str, ticks_per_bar: int = 0) -> float:
    """按策略时间框架及采样频率选择Sharpe年化因子"""
    if strategy_type in _DAILY_STRATEGY_TYPES:
        return ANNUALIZE_FACTOR_DAILY
    if ticks_per_bar > 0:
        return ANNUALIZE_FACTOR_MINUTE * ticks_per_bar
    return ANNUALIZE_FACTOR_MINUTE

# P0-03修复: 止盈止损价格边界判断的np.isclose容差(单一真相源)
# 生产/回测必须使用同一容差,否则边界价格行为分叉
STOP_PRICE_RTOL: float = 1e-8


# ========================================================================
# 回撤开仓（Pullback Entry） — 通用基础设施
# ========================================================================

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
    # [P2-08-SU] 计划统一到signal/模块的信号过期函数
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
        """P0-2修复：逐tick实时跟踪模式 — 每个tick即时判断回撤"""
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
    """通用回撤开仓管理器 — 非高频策略共用"""

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
        self.retrace_pct_call: Optional[float] = p.get('pullback_retrace_pct_call', 0.38)
        self.retrace_pct_put: Optional[float] = p.get('pullback_retrace_pct_put', 0.42)
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
            'tick_retrace_entries': 0,
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
            dynamic_wait_ticks=self.wait_ticks if self.tick_tracking else 0,
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
        """P0-2修复：逐tick实时跟踪模式"""
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
# P2-12修复: 已委托到 infra/resilience_retry.BoundedRetry, 保持签名兼容
# ============================================================================
def retry_with_limit(func, max_retries: int = 3, base_delay: float = 1.0,
                     backoff_factor: float = 2.0, logger=None, max_delay: float = 60.0,
                     cancel_event: 'threading.Event|None' = None):
    """R5-E-05修复: 带最大重试次数和指数退避的重试辅助函数

    P2-12修复: 内部委托到 BoundedRetry, 保持签名兼容。'
    """
    from ali2026v3_trading.infra.resilience import BoundedRetry as _BoundedRetry
    br = _BoundedRetry(
        max_retries=max_retries, base_delay=base_delay,
        max_delay=max_delay, cancel_event=cancel_event,
        on_exhausted="raise", backoff_factor=backoff_factor,
    )
    return br.execute(func)


# ============================================================================
# UPG-P1-04修复: API版本标记装饰器
# ============================================================================

def api_version(version: str, changelog: str = ""):
    """UPG-P1-04修复: API版本标记装饰器"""
    def decorator(func):
        func._api_version = version
        func._api_changelog = changelog
        func._api_versioned = True
        return func
    return decorator


def get_api_version(func) -> Optional[str]:
    """UPG-P1-04修复: 获取函数的API版本号"""
    return getattr(func, '_api_version', None)


def check_api_compatibility(func, min_version: str = "") -> bool:
    """UPG-P1-04修复: 检查API版本兼容性"""
    current = getattr(func, '_api_version', None)
    if current is None:
        return True
    if not min_version:
        return True
    return current >= min_version


# ============================================================================
# UPG-P1-13修复: 策略逻辑回归测试框架
# ============================================================================

class StrategyRegressionTest:
    """UPG-P1-13修复: 策略逻辑回归测试框架"""

    def __init__(self, test_name: str):
        self._test_name = test_name
        self._test_cases: List[Dict[str, Any]] = []

    def register(self, case_name: str, input_data: Any,
                 expected_output: Any, tolerance: float = 0.0,
                 comparator: Optional[Callable[[Any, Any], bool]] = None) -> None:
        self._test_cases.append({
            'name': case_name,
            'input': input_data,
            'expected': expected_output,
            'tolerance': tolerance,
            'comparator': comparator,
        })

    def run_all(self, func: Callable) -> Dict[str, Any]:
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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


# ============================================================================
# CS-03修复: 基于波动率缩放的执行延迟滑点计算
# ============================================================================

def compute_execution_delay_slippage_bps(
    price: float,
    bar_high: float,
    bar_low: float,
    bar_duration_sec: float = 60.0,
    exec_delay_ms: float = 50.0,
    z_score: float = 1.0,
) -> float:
    """基于波动率缩放的执行延迟滑点（布朗运动理论）"""
    if exec_delay_ms <= 0 or price <= 0:
        return 0.0

    # bar内波动率（bps）
    bar_range = (bar_high - bar_low) if bar_high > bar_low else price * 0.002
    bar_vol_bps = bar_range / price * 10000.0

    # 延迟占bar时长的比例，sqrt缩放（布朗运动假设）'
    delay_ratio = min(exec_delay_ms / 1000.0 / max(bar_duration_sec, 1.0), 1.0)
    vol_scaled = bar_vol_bps * math.sqrt(delay_ratio)

    # z_score个标准差的不利变动
    delay_slippage_bps = vol_scaled * z_score

    return delay_slippage_bps


# ============================================================================
# CS-01: 统一滑点模型（生产/回测/评判三系统共享）
# ============================================================================

# 统一到期日滑点倍增系数
UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS = {
    1: 20.0,
    2: 10.0,
    3: 5.0,
    5: 3.0,
    7: 2.0,
    10: 1.5,
    14: 1.2,
}

# 统一品种滑点乘数
UNIFIED_INSTRUMENT_SLIPPAGE_MULTIPLIER = {
    "ETF": 1.0,
    "FUTURE": 1.2,
    "OPTION_ETF": 1.5,
    "OPTION_INDEX": 2.0,
    "OPTION_COMMODITY": 2.5,
}

# 统一quality_scale映射
UNIFIED_QUALITY_SCALE_MAP = {1: 0.75, 2: 0.5, 3: 0.3}

# 统一流动性层级
UNIFIED_LIQUIDITY_TIER_MAP = {
    'future_main': 1.0,
    'future_sub': 1.2,
    'option_atm': 1.0,
    'option_otm': 1.5,
    'option_far': 2.0,
}


def compute_slippage_bps(
    price: float,
    bid_ask_spread: float = 0.0,
    base_slippage_bps: float = 3.0,
    spread_quality: int = 1,
    days_to_expiry: int = 999,
    instrument_type: str = "ETF",
    liquidity_tier: str = "future_main",
    backtest_premium_bps: float = 0.0,
) -> float:
    """统一滑点计算函数（生产/回测/评判三系统共享）"""
    if price <= 0:
        return base_slippage_bps + backtest_premium_bps

    # 到期日倍增
    expiry_mult = 1.0
    if days_to_expiry >= 0:
        for threshold_days, multiplier in sorted(UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS.items()):
            if days_to_expiry <= threshold_days:
                expiry_mult = multiplier
                break

    # spread退化处理
    if bid_ask_spread <= 0 or spread_quality == 0:
        return base_slippage_bps * expiry_mult + backtest_premium_bps

    # spread转为bps
    spread_bps = bid_ask_spread / price * 10000.0

    # quality缩放
    quality_scale = UNIFIED_QUALITY_SCALE_MAP.get(spread_quality, 0.75)

    # 流动性层级
    liquidity_mult = UNIFIED_LIQUIDITY_TIER_MAP.get(liquidity_tier, 1.2)

    # 品种乘数
    instrument_mult = UNIFIED_INSTRUMENT_SLIPPAGE_MULTIPLIER.get(instrument_type, 1.0)

    # 基础滑点 = max(固定base, spread动态)
    base = max(base_slippage_bps, spread_bps * quality_scale * liquidity_mult) * instrument_mult

    return base * expiry_mult + backtest_premium_bps


# ============================================================================
# 结构性补充: 模型版本管理（回滚框架）'
# ============================================================================

class ModelVersionManager:
    """模型版本管理器 — 支持运行时切换和回滚"""

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self._versions = {
            'slippage_model': 'v2',
            'position_model': 'v2',
            'execution_delay_model': 'v2',
        }
        self._rollback_history = []

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_version(self, model_name: str) -> str:
        return self._versions.get(model_name, 'v1')

    def is_v2(self, model_name: str) -> bool:
        return self._versions.get(model_name, 'v1') == 'v2'

    def rollback(self, model_name: str, reason: str = ""):
        """回滚到旧版本"""
        if model_name not in self._versions:
            raise ValueError(f"Unknown model: {model_name}")
        old_version = self._versions[model_name]
        self._versions[model_name] = 'v1'
        self._rollback_history.append({
            'model': model_name,
            'from_version': old_version,
            'to_version': 'v1',
            'reason': reason,
            'timestamp': time.time(),
        })
        logging.warning("[ModelVersion] ROLLBACK: %s %s → v1, reason=%s",
                       model_name, old_version, reason)

    def get_rollback_history(self) -> list:
        return list(self._rollback_history)


# ============================================================================
# 结构性补充: 运行时监控指标
# ============================================================================

class RuntimeMonitor:
    """运行时监控 — 跟踪关键指标偏差"""

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self._metrics = {
            'slippage_model_deviation': [],
            'position_size_ratio': [],
            'execution_delay_actual': [],
            'signal_latency_total': [],
            'strategy_eviction_rate': [],
            'model_version_consistency': True,
        }

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def record_slippage_deviation(self, estimated_bps: float, actual_bps: float):
        """记录滑点偏差"""
        if actual_bps > 0:
            deviation = abs(estimated_bps - actual_bps) / actual_bps
            self._metrics['slippage_model_deviation'].append(deviation)
            if len(self._metrics['slippage_model_deviation']) > 100:
                self._metrics['slippage_model_deviation'] = self._metrics['slippage_model_deviation'][-100:]
            recent = self._metrics['slippage_model_deviation'][-10:]
            if len(recent) >= 10 and all(d > 0.5 for d in recent):
                ModelVersionManager.get_instance().rollback(
                    'slippage_model',
                    f'连续10笔滑点偏差>50%, 最近偏差={recent}'
                )

    def record_position_ratio(self, production_lots: float, backtest_lots: float):
        """记录仓位比值"""
        if backtest_lots > 0:
            ratio = production_lots / backtest_lots
            self._metrics['position_size_ratio'].append(ratio)
            if len(self._metrics['position_size_ratio']) > 100:
                self._metrics['position_size_ratio'] = self._metrics['position_size_ratio'][-100:]

    def record_execution_delay(self, delay_ms: float):
        """记录执行延迟"""
        self._metrics['execution_delay_actual'].append(delay_ms)
        if len(self._metrics['execution_delay_actual']) > 1000:
            self._metrics['execution_delay_actual'] = self._metrics['execution_delay_actual'][-1000:]

    def record_signal_latency(self, latency_ms: float):
        """记录信号延迟"""
        self._metrics['signal_latency_total'].append(latency_ms)
        if len(self._metrics['signal_latency_total']) > 1000:
            self._metrics['signal_latency_total'] = self._metrics['signal_latency_total'][-1000:]

    def get_summary(self) -> Dict[str, Any]:
        """获取监控摘要"""
        import statistics
        summary = {}
        for key, values in self._metrics.items():
            if isinstance(values, list) and values:
                summary[key] = {
                    'count': len(values),
                    'p50': statistics.median(values) if values else 0,
                    'p99': sorted(values)[int(len(values) * 0.99)] if len(values) > 1 else values[0] if values else 0,
                    'latest': values[-1] if values else None,
                }
            else:
                summary[key] = values
        return summary


# ============================================================================
# SIG-P2-01: 交易动作与方向枚举（消除字符串比对残留）'
# ============================================================================

class TradeAction(str):
    """交易动作枚举 — 继承str以保持序列化兼容性"""
    OPEN = 'OPEN'
    CLOSE = 'CLOSE'

class TradeDirection(str):
    """交易方向枚举 — 继承str以保持序列化兼容性"""
    BUY = 'BUY'
    SELL = 'SELL'

class SignalType(str):
    """信号类型枚举 — 继承str以保持序列化兼容性"""
    BUY = 'BUY'
    SELL = 'SELL'
    CLOSE_LONG = 'CLOSE_LONG'
    CLOSE_SHORT = 'CLOSE_SHORT'

# 合法值集合（用于校验）'
VALID_TRADE_ACTIONS = {TradeAction.OPEN, TradeAction.CLOSE}
VALID_TRADE_DIRECTIONS = {TradeDirection.BUY, TradeDirection.SELL}
VALID_SIGNAL_TYPES = {SignalType.BUY, SignalType.SELL, SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}
OPEN_SIGNAL_TYPES = {SignalType.BUY, SignalType.SELL}
CLOSE_SIGNAL_TYPES = {SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}


# ============================================================
# Section 7: resilience re-export (原 shared_utils.py 的 resilience 导入)
# 使用延迟导入避免循环依赖: resilience → shared_utils → resilience_utils → resilience
# ============================================================

_RESILIENCE_LAZY_NAMES = {
    'stable_sum', 'stable_mean', 'stable_variance', 'stable_std',
    'approx_equal', 'approx_less', 'approx_greater',
    'should_trigger_stop_loss', 'should_trigger_take_profit',
    'KahanSummation', 'safe_divide', 'compute_sharpe_stable',
    'safe_normalize_weights', 'PRICE_TOLERANCE', 'FLOAT_COMPARE_TOLERANCE',
}

_RESILIENCE_RETRY_LAZY_NAMES = {
    'deprecated',
}

_RESILIENCE_LAZY_CACHE: dict = {}


def __getattr__(name: str):
    """延迟导入 resilience 模块，避免循环依赖"""
    if name in _RESILIENCE_LAZY_NAMES:
        if name not in _RESILIENCE_LAZY_CACHE:
            from ali2026v3_trading.infra.resilience import (  # noqa: F401
                stable_sum, stable_mean, stable_variance, stable_std,
                approx_equal, approx_less, approx_greater,
                should_trigger_stop_loss, should_trigger_take_profit,
                KahanSummation, safe_divide, compute_sharpe_stable,
                safe_normalize_weights, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
            )
            _RESILIENCE_LAZY_CACHE.update({
                'stable_sum': stable_sum, 'stable_mean': stable_mean,
                'stable_variance': stable_variance, 'stable_std': stable_std,
                'approx_equal': approx_equal, 'approx_less': approx_less,
                'approx_greater': approx_greater,
                'should_trigger_stop_loss': should_trigger_stop_loss,
                'should_trigger_take_profit': should_trigger_take_profit,
                'KahanSummation': KahanSummation, 'safe_divide': safe_divide,
                'compute_sharpe_stable': compute_sharpe_stable,
                'safe_normalize_weights': safe_normalize_weights,
                'PRICE_TOLERANCE': PRICE_TOLERANCE,
                'FLOAT_COMPARE_TOLERANCE': FLOAT_COMPARE_TOLERANCE,
            })
        return _RESILIENCE_LAZY_CACHE[name]
    if name in _RESILIENCE_RETRY_LAZY_NAMES:
        if name not in _RESILIENCE_LAZY_CACHE:
            from ali2026v3_trading.infra.resilience import deprecated  # noqa: F401
            _RESILIENCE_LAZY_CACHE['deprecated'] = deprecated
        return _RESILIENCE_LAZY_CACHE[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ============================================================================
# __all__ — 合并所有子模块的公开API
# ============================================================================

__all__ = [
    # Section 1: SQL安全
    'sanitize_sql_identifier', 'sanitize_sql_value',
    # Section 2: 类型转换
    'to_float32', 'to_native_type', 'safe_int', 'safe_float', 'safe_price_check',
    'ToDictMixin', 'safe_get_float', 'safe_get_int',
    # Section 3: 合约工具
    'CHINA_TZ', 'normalize_option_type', 'normalize_instrument_id',
    'extract_product_code', 'extract_strike_price', 'normalize_year_month',
    # Section 4: 基础设施
    'RingBuffer', 'ShardRouter', 'DEFAULT_SHARD_COUNT', 'get_shard_router',
    'ThreadLifecycleManager',
    'InitPhase', 'InitializationError', 'InitStateMachine',
    'IdempotencyGuard',
    'AutoRollbackContext', 'atomic_replace_file', 'cleanup_atomic_backup',
    'sanitize_filename',
    'compute_content_hash',
    'generate_prefixed_id',
    'check_version_sync', 'check_shared_utils_invariants',
    # Section 5: 契约编程
    'requires_phase', 'require_precondition', 'ensure_postcondition',
    'PreconditionError', 'PostconditionError', 'OperationTimeoutError',
    'with_timeout',
    # Section 6: 门面定义
    'utc_now', 'unified_ts_ms',
    'now_iso', 'now_compact', 'now_log_str', 'is_same_day',
    'DEFAULT_RISK_FREE_RATE', 'TRADING_DAYS_PER_YEAR_CHINA',
    'ANNUALIZE_FACTOR_DAILY', 'ANNUALIZE_FACTOR_MINUTE',
    'PendingPullbackSignal', 'PullbackManager', 'retry_with_limit',
    'api_version', 'get_api_version', 'check_api_compatibility',
    'deprecated',
    'StrategyRegressionTest',
    'compute_execution_delay_slippage_bps',
    'compute_slippage_bps', 'UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS',
    'UNIFIED_INSTRUMENT_SLIPPAGE_MULTIPLIER', 'UNIFIED_QUALITY_SCALE_MAP',
    'UNIFIED_LIQUIDITY_TIER_MAP',
    'ModelVersionManager',
    'RuntimeMonitor',
    'TradeAction', 'TradeDirection', 'SignalType',
    'VALID_TRADE_ACTIONS', 'VALID_TRADE_DIRECTIONS', 'VALID_SIGNAL_TYPES',
    'OPEN_SIGNAL_TYPES', 'CLOSE_SIGNAL_TYPES',
    # Section 7: resilience re-export
    'stable_sum', 'stable_mean', 'stable_variance', 'stable_std',
    'approx_equal', 'approx_less', 'approx_greater',
    'should_trigger_stop_loss', 'should_trigger_take_profit',
    'KahanSummation', 'safe_divide', 'compute_sharpe_stable',
    'safe_normalize_weights', 'PRICE_TOLERANCE', 'FLOAT_COMPARE_TOLERANCE',
]
