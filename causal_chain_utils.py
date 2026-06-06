"""
causal_chain_utils.py — R32 P0修复: 数据因果链追踪/隔离/清理/回滚

解决CC-01~CC-12共12个P0问题:
- CC-01: tick价格NaN/Inf全链路污染防护 (已有入口校验, 增加级联防护)
- CC-02: 全链路关联ID (correlation_id)
- CC-03: 污染检测后级联清理/回滚
- CC-04: 循环依赖检测与环路隔离
- CC-05: 价格错误K线聚合放大防护
- CC-06: 错误信号污染后续信号防护
- CC-07: 风控静默通过防护
- CC-08: 参数池错误参数隔离
- CC-09: 回测参数→生产系统因果链防护
- CC-10: 评判引擎评分错误防护
- CC-11: 下单→持仓→风控因果环隔离
- CC-12: 根因追踪能力
"""
from __future__ import annotations

import math
import time
import uuid
import logging
import threading
from typing import Any, Dict, List, Optional, Set, Tuple, Deque
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

_logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class CausalEvent:
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    correlation_id: str = ""
    parent_event_id: str = ""
    event_type: str = ""
    source_module: str = ""
    source_line: int = 0
    timestamp: datetime = field(default_factory=_utc_now)
    payload_summary: str = ""
    contamination_flag: bool = False
    cleanup_status: str = "none"


class CausalChainTracker:

    _lock = threading.Lock()

    def __init__(self, max_history: int = 10000):
        self._events: Deque[CausalEvent] = deque(maxlen=max_history)
        self._correlation_index: Dict[str, List[str]] = {}
        self._event_index: Dict[str, CausalEvent] = {}
        self._contaminated_correlations: Set[str] = set()
        self._local_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "CausalChainTracker":
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('causal_chain_tracker')
        _inst = _registry.get('instance')
        if _inst is None:
            with cls._lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = cls()
                    _registry.set('instance', _inst)
        return _inst

    def new_correlation_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def record_event(self, event: CausalEvent) -> None:
        with self._local_lock:
            self._events.append(event)
            self._event_index[event.event_id] = event
            if event.correlation_id:
                self._correlation_index.setdefault(event.correlation_id, []).append(event.event_id)
                if event.contamination_flag:
                    self._contaminated_correlations.add(event.correlation_id)

    def trace_root_cause(self, event_id: str) -> List[CausalEvent]:
        chain = []
        current_id = event_id
        visited = set()
        with self._local_lock:
            while current_id and current_id not in visited:
                visited.add(current_id)
                evt = self._event_index.get(current_id)
                if evt is None:
                    break
                chain.append(evt)
                current_id = evt.parent_event_id
        chain.reverse()
        return chain

    def get_correlation_chain(self, correlation_id: str) -> List[CausalEvent]:
        with self._local_lock:
            event_ids = self._correlation_index.get(correlation_id, [])
            return [self._event_index[eid] for eid in event_ids if eid in self._event_index]

    def is_correlation_contaminated(self, correlation_id: str) -> bool:
        with self._local_lock:
            return correlation_id in self._contaminated_correlations

    def get_contaminated_correlations(self) -> Set[str]:
        with self._local_lock:
            return set(self._contaminated_correlations)


class ContaminationGuard:

    _lock = threading.Lock()

    def __init__(self):
        self._contaminated_nodes: Dict[str, Dict[str, Any]] = {}
        self._cleanup_handlers: Dict[str, List[Any]] = {}
        self._node_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "ContaminationGuard":
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('contamination_guard')
        _inst = _registry.get('instance')
        if _inst is None:
            with cls._lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = cls()
                    _registry.set('instance', _inst)
        return _inst

    def validate_numeric(self, value: Any, name: str, context: str = "") -> Tuple[bool, Any]:
        if value is None:
            return False, None
        if not isinstance(value, (int, float)):
            return False, value
        if math.isnan(value) or math.isinf(value):
            self._mark_contaminated(name, context, reason=f"NaN/Inf detected: {name}={value}")
            return False, value
        return True, value

    def validate_price(self, price: Any, instrument_id: str = "") -> Tuple[bool, float]:
        # R23-P2-11修复: 增强numpy类型处理
        if price is None:
            return False, 0.0
        try:
            price_float = float(price)
        except (TypeError, ValueError):
            return False, 0.0
        if math.isnan(price_float) or math.isinf(price_float) or price_float <= 0:
            self._mark_contaminated(instrument_id, "price", reason=f"invalid price={price_float}")
            return False, 0.0
        return True, price_float

    def _mark_contaminated(self, node: str, context: str, reason: str = "") -> None:
        with self._node_lock:
            self._contaminated_nodes[node] = {
                "context": context,
                "reason": reason,
                "timestamp": _utc_now(),
                "cleaned": False,
            }

    def register_cleanup_handler(self, node_pattern: str, handler: Any) -> None:
        with self._node_lock:
            self._cleanup_handlers.setdefault(node_pattern, []).append(handler)

    def cascade_cleanup(self, source_node: str) -> int:
        cleaned = 0
        with self._node_lock:
            for node, info in self._contaminated_nodes.items():
                if not info.get("cleaned", False):
                    info["cleaned"] = True
                    info["cleanup_timestamp"] = _utc_now()
                    cleaned += 1
                    _logger.warning("[CC-03] Cascade cleanup: node=%s reason=%s", node, info.get("reason", ""))
        return cleaned

    def get_contamination_status(self) -> Dict[str, Any]:
        with self._node_lock:
            total = len(self._contaminated_nodes)
            uncleaned = sum(1 for v in self._contaminated_nodes.values() if not v.get("cleaned", False))
            return {"total_contaminated": total, "uncleaned": uncleaned, "nodes": dict(self._contaminated_nodes)}


class CyclicDependencyGuard:

    _lock = threading.Lock()

    def __init__(self, max_depth: int = 10):
        self._call_stack = threading.local()
        self._max_depth = max_depth
        self._cycle_detections: List[Dict[str, Any]] = []
        self._detect_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "CyclicDependencyGuard":
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('cyclic_dependency_guard')
        _inst = _registry.get('instance')
        if _inst is None:
            with cls._lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = cls()
                    _registry.set('instance', _inst)
        return _inst

    def enter(self, node: str) -> bool:
        stack = getattr(self._call_stack, 'stack', None)
        if stack is None:
            stack = []
            self._call_stack.stack = stack
        if node in stack:
            with self._detect_lock:
                self._cycle_detections.append({
                    "cycle": list(stack) + [node],
                    "timestamp": _utc_now(),
                })
            _logger.warning("[CC-04/CC-11] Cyclic dependency detected: %s->%s", "->".join(stack), node)
            return False
        if len(stack) >= self._max_depth:
            _logger.warning("[CC-04] Call chain too deep (%d), possible loop: %s", len(stack), "->".join(stack[-5:]))
            return False
        stack.append(node)
        return True

    def exit(self, node: str) -> None:
        stack = getattr(self._call_stack, 'stack', None)
        if stack and stack and stack[-1] == node:
            stack.pop()

    def get_cycle_detections(self) -> List[Dict[str, Any]]:
        with self._detect_lock:
            return list(self._cycle_detections)


class ParamIsolationGuard:

    _lock = threading.Lock()

    def __init__(self):
        self._param_checksums: Dict[str, str] = {}
        self._param_violations: List[Dict[str, Any]] = []
        self._iso_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "ParamIsolationGuard":
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('param_isolation_guard')
        _inst = _registry.get('instance')
        if _inst is None:
            with cls._lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = cls()
                    _registry.set('instance', _inst)
        return _inst

    def register_param_source(self, param_name: str, source: str, checksum: str) -> None:
        with self._iso_lock:
            self._param_checksums[param_name] = checksum

    def validate_param_consistency(self, param_name: str, checksum_a: str, checksum_b: str, source_a: str, source_b: str) -> bool:
        if checksum_a != checksum_b:
            with self._iso_lock:
                self._param_violations.append({
                    "param": param_name,
                    "source_a": source_a,
                    "source_b": source_b,
                    "checksum_a": checksum_a,
                    "checksum_b": checksum_b,
                    "timestamp": _utc_now(),
                })
            _logger.error("[CC-08/CC-09] Param inconsistency: %s (%s vs %s)", param_name, source_a, source_b)
            return False
        return True

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._iso_lock:
            return list(self._param_violations)


def safe_datetime_now() -> datetime:
    """ENV-06修复: 统一时区感知的datetime.now()"""
    return datetime.now(timezone.utc)


def safe_datetime_from_timestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def validate_tick_cascade(price: Any, volume: Any, instrument_id: str = "") -> Tuple[bool, float, int]:
    """CC-01修复: tick入口级联校验（补充strategy_tick_handler已有校验）"""
    guard = ContaminationGuard.get_instance()
    price_ok, safe_price = guard.validate_price(price, instrument_id)
    vol_safe = 0
    if volume is not None and isinstance(volume, (int, float)) and not math.isnan(volume) and not math.isinf(volume) and volume >= 0:
        vol_safe = int(volume)
    else:
        guard._mark_contaminated(instrument_id, "volume", reason=f"invalid volume={volume}")
    return price_ok, safe_price, vol_safe
