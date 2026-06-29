# MODULE_ID: M1-059
"""
width_cache_types.py — 数据类型定义
包含: _NoOpDiagnosisProbeManager, SortEntry, 模块常量
"""
from __future__ import annotations

import threading
import logging
import struct
from contextlib import contextmanager

# #89修复：DiagnosisProbeManager不可用时的no-op替代
class _NoOpDiagnosisProbeManager:
    """DiagnosisProbeManager导入失败时的降级替代，所有操作为no-op"""
    @staticmethod
    @contextmanager
    def startup_step(name):
        yield
    @staticmethod
    def mark_startup_event(name, detail=''):
        pass
import sys
import heapq
import time
import zlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field
from ali2026v3_trading.infra.shared_utils import to_float32, CHINA_TZ

__all__ = [
    'SortEntry', 'WidthStrengthCache',
    'MAX_OPTION_CACHE_SIZE', 'MAX_INSTRUMENT_ID_MAP_SIZE', 'MAX_FUTURE_CACHE_SIZE',
    'MAX_STATUS_COUNTS_SIZE', 'MAX_SORT_BUCKETS_FUTURES',
]


# ============================================================================
# SortEntry - 排序桶条目（by_sort_bucket 索引结构）'
# ============================================================================

@dataclass(order=True, slots=True)
class SortEntry:
    """
    分组D准备：排序桶条目，用于 by_sort_bucket 索引结构
    
    排序优先级（从高到低）：
    1. sync_flag: True > False (同步虚值优先)
    2. volume: 成交量越大越靠前
    3. strike_distance: 虚值距离越小越靠前（更接近实值）
    4. last_tick_ts: 最新tick时间越新越靠前
    5. internal_id: 兜底排序，保证稳定性
    
    注意：使用负值实现降序排列（heapq是最小堆）
    """
    sort_key: Tuple = field(compare=True)  # 排序键
    internal_id: int = field(compare=False)  # internal_id
    instrument_id: str = field(compare=False)  # instrument_id（供外部消费）
    option_type: str = field(compare=False)  # CALL/PUT
    strike_price: float = field(compare=False)  # 行权价
    volume: float = field(compare=False)  # 成交量
    price: float = field(compare=False)  # 最新价
    sync_flag: bool = field(compare=False)  # 是否同步
    month: str = field(compare=False)  # 月份
    product: str = field(compare=False)  # 产品
    
    @staticmethod
    def create(internal_id: int, instrument_id: str, option_type: str, 
               strike_price: float, volume: float, price: float,
               sync_flag: bool, m_price: float, month: str, product: str,
               last_tick_ts: float = 0.0) -> 'SortEntry':
        """
        创建排序条目
        """
        # 计算虚值距离
        if option_type == 'CALL':
            strike_distance = strike_price - m_price if m_price > 0 else float('inf')
        else:  # PUT
            strike_distance = m_price - strike_price if m_price > 0 else float('inf')
        
        # 构建排序键（使用负值实现降序）'
        sort_key = (
            not sync_flag,
            -(volume if volume > 0 else 0.01),
            strike_distance,
            -last_tick_ts,
            internal_id,
        )
        
        return SortEntry(
            sort_key=sort_key,
            internal_id=internal_id,
            instrument_id=instrument_id,
            option_type=option_type,
            strike_price=strike_price,
            volume=volume,
            price=price,
            sync_flag=sync_flag,
            month=month,
            product=product,
        )


# ============================================================================
# WidthStrengthCache - 内嵌类：实时维护期权宽度强度
# ============================================================================

# P1 修复：从 event_bus 模块统一导入 EventBus
try:
    from ali2026v3_trading.infra.event_bus import get_global_event_bus, EventBus
    _HAS_EVENT_BUS = get_global_event_bus() is not None
except ImportError as e:
    logging.warning(f"[TTypeService] EventBus import failed: {e}")
    EventBus = None
    get_global_event_bus = None
    _HAS_EVENT_BUS = False

try:
    from ali2026v3_trading.infra.event_bus import KLineEvent as EventBusKLineEvent
except ImportError:
    EventBusKLineEvent = None

MAX_OPTION_CACHE_SIZE = 20000
MAX_INSTRUMENT_ID_MAP_SIZE = 20000
MAX_FUTURE_CACHE_SIZE = 500
MAX_STATUS_COUNTS_SIZE = 20000
MAX_SORT_BUCKETS_FUTURES = 500
_CACHE_TTL_SECONDS = 86400
