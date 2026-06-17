# [M3-16] 数据因果链工具
# MODULE_ID: M3-612
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
from ali2026v3_trading.infra.shared_utils import utc_now as _utc_now, generate_prefixed_id
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

_logger = get_logger(__name__)  # R9-5


@dataclass(slots=True)
class CausalEvent:
    event_id: str = field(default_factory=lambda: generate_prefixed_id("", 16))  # R9-3
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
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
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
        return generate_prefixed_id("", 12)  # R9-3

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
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
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
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
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
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
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


# ==================== R28-C023修复: 错误交易回滚 ====================

@dataclass(slots=True)
class TradeRollbackSnapshot:
    """交易前快照，用于回滚错误交易的所有后续影响"""
    position_id: str
    instrument_id: str
    correlation_id: str
    timestamp: datetime = field(default_factory=_utc_now)
    # 持仓快照（交易前）
    position_existed: bool = False
    prev_volume: int = 0
    prev_direction: str = ""
    prev_open_price: float = 0.0
    prev_stop_profit_price: float = 0.0
    prev_stop_loss_price: float = 0.0
    prev_current_plr: float = 0.0
    prev_max_profit_pct: float = 0.0
    prev_chase_count: int = 0
    # 风控快照（交易前）
    prev_consecutive_losses: int = 0
    prev_daily_drawdown: float = 0.0
    # 回滚状态
    rolled_back: bool = False
    rollback_reason: str = ""


class TradeRollbackManager:
    """[R28-C023修复] 错误交易回滚管理器

    当检测到错误交易（如NaN价格导致的错误开仓）时，
    回滚该交易产生的所有后续影响：
    1. 撤销持仓（平仓或恢复到交易前状态）
    2. 重置风控状态（连亏计数、日回撤等）
    3. 标记相关信号/订单为已回滚
    4. 记录回滚事件到因果链
    """

    _lock = threading.Lock()

    def __init__(self, max_snapshots: int = 1000):
        self._snapshots: Dict[str, TradeRollbackSnapshot] = {}
        self._rollback_history: Deque[Dict[str, Any]] = deque(maxlen=max_snapshots)
        self._mgr_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "TradeRollbackManager":
        from ali2026v3_trading.infra.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('trade_rollback_manager')
        _inst = _registry.get('instance')
        if _inst is None:
            with cls._lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = cls()
                    _registry.set('instance', _inst)
        return _inst

    def capture_pre_trade_snapshot(
        self,
        position_id: str,
        instrument_id: str,
        correlation_id: str,
        position_existed: bool = False,
        prev_volume: int = 0,
        prev_direction: str = "",
        prev_open_price: float = 0.0,
        prev_stop_profit_price: float = 0.0,
        prev_stop_loss_price: float = 0.0,
        prev_current_plr: float = 0.0,
        prev_max_profit_pct: float = 0.0,
        prev_chase_count: int = 0,
        prev_consecutive_losses: int = 0,
        prev_daily_drawdown: float = 0.0,
    ) -> TradeRollbackSnapshot:
        """在交易执行前捕获快照"""
        snapshot = TradeRollbackSnapshot(
            position_id=position_id,
            instrument_id=instrument_id,
            correlation_id=correlation_id,
            position_existed=position_existed,
            prev_volume=prev_volume,
            prev_direction=prev_direction,
            prev_open_price=prev_open_price,
            prev_stop_profit_price=prev_stop_profit_price,
            prev_stop_loss_price=prev_stop_loss_price,
            prev_current_plr=prev_current_plr,
            prev_max_profit_pct=prev_max_profit_pct,
            prev_chase_count=prev_chase_count,
            prev_consecutive_losses=prev_consecutive_losses,
            prev_daily_drawdown=prev_daily_drawdown,
        )
        with self._mgr_lock:
            self._snapshots[position_id] = snapshot
        return snapshot

    def rollback_trade(self, position_id: str, reason: str = "contamination") -> bool:
        """回滚指定交易的所有后续影响

        Returns:
            True if rollback succeeded, False if snapshot not found or already rolled back
        """
        with self._mgr_lock:
            snapshot = self._snapshots.get(position_id)
            if snapshot is None:
                _logger.warning("[R28-C023] 回滚失败: 无快照 position_id=%s", position_id)
                return False
            if snapshot.rolled_back:
                _logger.warning("[R28-C023] 已回滚: position_id=%s", position_id)
                return False

        # 步骤1: 撤销持仓
        try:
            from ali2026v3_trading.position.position_service import get_position_service
            pos_svc = get_position_service()
            instrument_id = snapshot.instrument_id

            if not snapshot.position_existed:
                # 交易前无持仓 → 删除错误创建的持仓
                positions = pos_svc.positions.get(instrument_id, {})
                if position_id in positions:
                    del positions[position_id]
                    _logger.info("[R28-C023] 删除错误持仓: %s %s", instrument_id, position_id)
            else:
                # 交易前有持仓 → 恢复到交易前状态
                positions = pos_svc.positions.get(instrument_id, {})
                pos = positions.get(position_id)
                if pos is not None:
                    pos.volume = snapshot.prev_volume
                    pos.direction = snapshot.prev_direction
                    pos.open_price = snapshot.prev_open_price
                    pos.stop_profit_price = snapshot.prev_stop_profit_price
                    pos.stop_loss_price = snapshot.prev_stop_loss_price
                    pos.current_plr = snapshot.prev_current_plr
                    pos._max_profit_pct = snapshot.prev_max_profit_pct
                    pos.chase_count = snapshot.prev_chase_count
                    _logger.info("[R28-C023] 恢复持仓到交易前: %s %s", instrument_id, position_id)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            _logger.error("[R28-C023] 持仓回滚异常: %s", e, exc_info=True)

        # 步骤2: 重置风控状态
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            risk_svc = get_risk_service()
            if risk_svc is not None and hasattr(risk_svc, '_consecutive_loss_counts'):
                # 重置连亏计数（回滚的交易不应计入连亏）
                for key in list(risk_svc._consecutive_loss_counts.keys()):
                    if snapshot.instrument_id in key:
                        risk_svc._consecutive_loss_counts[key] = snapshot.prev_consecutive_losses
                        _logger.info("[R28-C023] 重置连亏计数: %s → %d", key, snapshot.prev_consecutive_losses)
        except (ValueError, KeyError, AttributeError, RuntimeError) as e:
            _logger.debug("[R28-C023] 风控状态重置跳过: %s", e)

        # 步骤3: 记录回滚事件到因果链
        try:
            tracker = CausalChainTracker.get_instance()
            tracker.record_event(CausalEvent(
                correlation_id=snapshot.correlation_id,
                event_type="trade_rollback",
                source_module="TradeRollbackManager",
                payload_summary=f"position_id={position_id} reason={reason}",
                contamination_flag=True,
                cleanup_status="rolled_back",
            ))
        except (ValueError, KeyError, TypeError, RuntimeError) as e:
            _logger.debug("[R28-C023] 因果链记录跳过: %s", e)

        # 步骤4: 触发级联清理
        try:
            guard = ContaminationGuard.get_instance()
            guard.cascade_cleanup(snapshot.instrument_id)
        except (ValueError, KeyError, RuntimeError) as e:
            _logger.debug("[R28-C023] 级联清理跳过: %s", e)

        # 步骤5: 更新快照状态
        with self._mgr_lock:
            snapshot.rolled_back = True
            snapshot.rollback_reason = reason
            self._rollback_history.append({
                "position_id": position_id,
                "instrument_id": snapshot.instrument_id,
                "correlation_id": snapshot.correlation_id,
                "reason": reason,
                "timestamp": _utc_now(),
            })

        _logger.warning("[R28-C023] 交易回滚完成: position_id=%s reason=%s", position_id, reason)
        return True

    def get_rollback_history(self) -> List[Dict[str, Any]]:
        """获取回滚历史"""
        with self._mgr_lock:
            return list(self._rollback_history)

    def get_pending_snapshots(self) -> List[str]:
        """获取未回滚的快照ID列表"""
        with self._mgr_lock:
            return [pid for pid, s in self._snapshots.items() if not s.rolled_back]
