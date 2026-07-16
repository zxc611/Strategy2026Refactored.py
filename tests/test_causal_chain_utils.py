# MODULE_ID: M2-310
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# mock optuna before importing demo to avoid SystemExit
import types
from unittest.mock import MagicMock
_optuna = types.ModuleType('optuna')
_optuna.__version__ = '3.5.0'
_optuna.samplers = MagicMock(TPESampler=MagicMock)
_optuna.pruners = MagicMock(HyperbandPruner=MagicMock)
sys.modules['optuna'] = _optuna
sys.modules['optuna.samplers'] = _optuna.samplers
sys.modules['optuna.pruners'] = _optuna.pruners

import math
from unittest.mock import patch

from strategy_judgment.causal_chain_utils import (
    CausalEvent,
    CausalChainTracker,
    ContaminationGuard,
    CyclicDependencyGuard,
    ParamIsolationGuard,
    TradeRollbackManager,
    TradeRollbackSnapshot,
    validate_tick_cascade,
)


# ------------------------------------------------------------------
# CausalChainTracker (映射用户描述的 CausalChain / causal_layer / new_node / trace_record)
# ------------------------------------------------------------------

def test_causal_chain_tracker_new_bypass_init():
    """使用 __new__ 绕过 __init__ 实例化 CausalChainTracker"""
    obj = object.__new__(CausalChainTracker)
    assert isinstance(obj, CausalChainTracker)


def test_causal_layer_and_new_node():
    """测试 new_correlation_id 与 record_event（模拟 new_node / causal_layer）"""
    tracker = object.__new__(CausalChainTracker)
    tracker.__init__(max_history=100)

    corr_id = tracker.new_correlation_id()
    assert isinstance(corr_id, str)
    assert len(corr_id) > 0

    evt = CausalEvent(
        correlation_id=corr_id,
        event_type="test_event",
        source_module="test_mod",
        contamination_flag=False,
    )
    tracker.record_event(evt)

    chain = tracker.get_correlation_chain(corr_id)
    assert len(chain) == 1
    assert chain[0].event_type == "test_event"


def test_trace_record_root_cause():
    """测试 trace_root_cause 追溯父事件（映射 trace_record）"""
    tracker = object.__new__(CausalChainTracker)
    tracker.__init__(max_history=100)

    parent = CausalEvent(event_type="parent")
    child = CausalEvent(event_type="child", parent_event_id=parent.event_id)

    tracker.record_event(parent)
    tracker.record_event(child)

    root = tracker.trace_root_cause(child.event_id)
    assert len(root) == 2
    assert root[0].event_type == "parent"
    assert root[1].event_type == "child"


def test_contamination_flag():
    """测试污染标记检测"""
    tracker = object.__new__(CausalChainTracker)
    tracker.__init__(max_history=100)

    corr_id = "corr_001"
    evt = CausalEvent(correlation_id=corr_id, contamination_flag=True)
    tracker.record_event(evt)

    assert tracker.is_correlation_contaminated(corr_id) is True
    assert tracker.get_contaminated_correlations() == {corr_id}


# ------------------------------------------------------------------
# ContaminationGuard (映射用户描述的 GuardBlock / check_block)
# ------------------------------------------------------------------

def test_contamination_guard_new_bypass_init():
    """使用 __new__ 绕过 __init__ 实例化 ContaminationGuard"""
    obj = object.__new__(ContaminationGuard)
    assert isinstance(obj, ContaminationGuard)


def test_check_block_validate_numeric():
    """测试 validate_numeric 校验数值（映射 check_block）"""
    guard = object.__new__(ContaminationGuard)
    guard.__init__()

    ok, val = guard.validate_numeric(3.14, "price")
    assert ok is True
    assert val == 3.14

    ok, val = guard.validate_numeric(None, "price")
    assert ok is False

    ok, val = guard.validate_numeric(float('nan'), "price")
    assert ok is False

    ok, val = guard.validate_numeric(float('inf'), "price")
    assert ok is False


def test_check_block_validate_price():
    """测试 validate_price 校验价格（映射 check_block）"""
    guard = object.__new__(ContaminationGuard)
    guard.__init__()

    ok, price = guard.validate_price(100.5, "IC2506")
    assert ok is True
    assert price == 100.5

    ok, price = guard.validate_price(None, "IC2506")
    assert ok is False
    assert price == 0.0

    ok, price = guard.validate_price(-10, "IC2506")
    assert ok is False
    assert price == 0.0

    ok, price = guard.validate_price(float('nan'), "IC2506")
    assert ok is False


def test_cascade_cleanup():
    """测试 cascade_cleanup 清理污染节点"""
    guard = object.__new__(ContaminationGuard)
    guard.__init__()

    guard._mark_contaminated("node_a", "ctx", "reason_a")
    guard._mark_contaminated("node_b", "ctx", "reason_b")

    status_before = guard.get_contamination_status()
    assert status_before["total_contaminated"] == 2
    assert status_before["uncleaned"] == 2

    cleaned = guard.cascade_cleanup("node_a")
    assert cleaned == 2

    status_after = guard.get_contamination_status()
    assert status_after["uncleaned"] == 0


# ------------------------------------------------------------------
# CyclicDependencyGuard (映射用户描述的 enter/exit / push/pop)
# ------------------------------------------------------------------

def test_cyclic_guard_new_bypass_init():
    """使用 __new__ 绕过 __init__ 实例化 CyclicDependencyGuard"""
    obj = object.__new__(CyclicDependencyGuard)
    assert isinstance(obj, CyclicDependencyGuard)


def test_enter_exit_push_pop():
    """测试 enter/exit 的栈语义（映射 push/pop）"""
    guard = object.__new__(CyclicDependencyGuard)
    guard.__init__(max_depth=5)

    assert guard.enter("A") is True
    assert guard.enter("B") is True
    assert guard.enter("C") is True

    # 循环依赖检测
    assert guard.enter("A") is False

    guard.exit("C")
    assert guard.enter("C") is True
    guard.exit("C")
    guard.exit("B")
    guard.exit("A")

    assert len(guard.get_cycle_detections()) >= 1


def test_max_depth():
    """测试超过 max_depth 时 enter 返回 False"""
    guard = object.__new__(CyclicDependencyGuard)
    guard.__init__(max_depth=2)

    assert guard.enter("A") is True
    assert guard.enter("B") is True
    assert guard.enter("C") is False  # 超过深度

    guard.exit("B")
    guard.exit("A")


# ------------------------------------------------------------------
# ParamIsolationGuard
# ------------------------------------------------------------------

def test_param_isolation_guard():
    """测试参数隔离校验"""
    iso = object.__new__(ParamIsolationGuard)
    iso.__init__()

    iso.register_param_source("param1", "source_a", "chk_a")

    ok = iso.validate_param_consistency("param1", "chk_a", "chk_a", "source_a", "source_b")
    assert ok is True

    ok = iso.validate_param_consistency("param1", "chk_a", "chk_b", "source_a", "source_b")
    assert ok is False

    violations = iso.get_violations()
    assert len(violations) == 1
    assert violations[0]["param"] == "param1"


# ------------------------------------------------------------------
# TradeRollbackManager
# ------------------------------------------------------------------

def test_trade_rollback_manager_new_bypass_init():
    """使用 __new__ 绕过 __init__ 实例化 TradeRollbackManager"""
    obj = object.__new__(TradeRollbackManager)
    assert isinstance(obj, TradeRollbackManager)


def test_capture_and_rollback_snapshot():
    """测试交易快照捕获与回滚"""
    mgr = object.__new__(TradeRollbackManager)
    mgr.__init__(max_snapshots=100)

    snapshot = mgr.capture_pre_trade_snapshot(
        position_id="pos_001",
        instrument_id="IC2506",
        correlation_id="corr_001",
        position_existed=False,
        prev_volume=0,
        prev_direction="",
        prev_open_price=0.0,
        prev_stop_profit_price=0.0,
        prev_stop_loss_price=0.0,
        prev_current_plr=0.0,
        prev_max_profit_pct=0.0,
        prev_chase_count=0,
        prev_consecutive_losses=0,
        prev_daily_drawdown=0.0,
    )
    assert isinstance(snapshot, TradeRollbackSnapshot)
    assert snapshot.position_id == "pos_001"

    # 首次回滚应成功
    with patch('strategy_judgment.causal_chain_utils._logger'):
        result = mgr.rollback_trade("pos_001", reason="test")
    assert result is True

    # 重复回滚应失败
    with patch('strategy_judgment.causal_chain_utils._logger'):
        result = mgr.rollback_trade("pos_001", reason="test")
    assert result is False

    history = mgr.get_rollback_history()
    assert len(history) == 1
    assert history[0]["position_id"] == "pos_001"


def test_rollback_missing_snapshot():
    """测试对不存在的快照回滚返回 False"""
    mgr = object.__new__(TradeRollbackManager)
    mgr.__init__()

    with patch('strategy_judgment.causal_chain_utils._logger'):
        result = mgr.rollback_trade("nonexistent", reason="test")
    assert result is False


# ------------------------------------------------------------------
# validate_tick_cascade
# ------------------------------------------------------------------

def test_validate_tick_cascade():
    """测试 tick 级联校验函数"""
    ok, price, vol = validate_tick_cascade(100.5, 10, "IC2506")
    assert ok is True
    assert price == 100.5
    assert vol == 10

    ok, price, vol = validate_tick_cascade(None, 10, "IC2506")
    assert ok is False
    assert price == 0.0

    ok, price, vol = validate_tick_cascade(100.5, -1, "IC2506")
    assert ok is True  # 价格OK，volume 非法但只标记污染
    assert vol == 0

    ok, price, vol = validate_tick_cascade(float('nan'), float('inf'), "IC2506")
    assert ok is False
    assert price == 0.0
    assert vol == 0
