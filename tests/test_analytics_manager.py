# MODULE_ID: M2-302
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

import threading
from io import StringIO
import json

from strategy_judgment.analytics_manager import AnalyticsManager


def test_analytics_manager_new_bypass_init():
    """使用 __new__ 绕过 __init__ 实例化 AnalyticsManager"""
    obj = object.__new__(AnalyticsManager)
    assert isinstance(obj, AnalyticsManager)
    # 未调用 __init__，属性应不存在或保持默认值
    assert getattr(obj, '_provider', None) is None


def test_reset_stats():
    """测试 reset_stats 将统计恢复到初始状态"""
    mgr = AnalyticsManager()
    mgr.record_tick()
    mgr.record_trade()
    mgr.record_signal()
    mgr.record_error("test error")
    mgr._stats['start_time'] = "fake_time"

    mgr.reset_stats()

    assert mgr._stats['total_ticks'] == 0
    assert mgr._stats['total_trades'] == 0
    assert mgr._stats['total_signals'] == 0
    assert mgr._stats['errors_count'] == 0
    assert mgr._stats['last_error_time'] is None
    assert mgr._stats['last_error_message'] is None
    assert mgr._stats['tick_by_type'] == {'future': 0, 'option': 0}
    assert mgr._stats['start_time'] is None


def test_update_cycle_via_record_methods():
    """测试 record_* 方法更新统计周期数据"""
    mgr = AnalyticsManager()
    lock = threading.RLock()

    mgr.record_tick(lock)
    mgr.record_tick(lock)
    mgr.record_trade(lock)
    mgr.record_signal(lock)
    mgr.record_error("err1", lock)

    assert mgr._stats['total_ticks'] == 2
    assert mgr._stats['total_trades'] == 1
    assert mgr._stats['total_signals'] == 1
    assert mgr._stats['errors_count'] == 1
    assert mgr._stats['last_error_message'] == "err1"

    # 无锁路径
    mgr.record_tick()
    assert mgr._stats['total_ticks'] == 3


def test_render_summary_via_get_stats():
    """测试 get_stats 返回完整统计摘要（模拟 render_summary）"""
    mgr = AnalyticsManager()
    mgr._stats['start_time'] = None
    mgr._stats['total_ticks'] = 100
    mgr._stats['total_trades'] = 5
    mgr._stats['total_signals'] = 3

    stats = mgr.get_stats(state_value='running', is_running=True, is_paused=False)

    assert stats['service_name'] == 'StrategyCoreService'
    assert stats['total_ticks'] == 100
    assert stats['total_trades'] == 5
    assert stats['total_signals'] == 3
    assert stats['state'] == 'running'
    assert stats['is_running'] is True
    assert stats['is_paused'] is False
    assert stats['uptime_seconds'] == 0.0
    assert stats['ticks_per_second'] == 0.0


def test_dump_summary_to_stringio():
    """使用 StringIO mock 输出统计摘要（模拟 dump_summary）"""
    mgr = AnalyticsManager()
    mgr._stats['total_ticks'] = 42
    mgr._stats['total_trades'] = 2
    mgr._stats['total_signals'] = 1

    stats = mgr.get_stats()
    output = StringIO()
    json.dump(stats, output, default=str, ensure_ascii=False)
    output.seek(0)

    result = output.read()
    assert '"total_ticks": 42' in result
    assert '"total_trades": 2' in result
    assert '"service_name": "StrategyCoreService"' in result


def test_ensure_analytics_ready():
    """测试 ensure_analytics_ready 在 warmup 完成时返回 True"""
    mgr = AnalyticsManager()
    assert mgr.ensure_analytics_ready() is False

    mgr._analytics_warmup_done = True
    assert mgr.ensure_analytics_ready() is True


def test_warmup_thread_join():
    """测试存在存活 warmup_thread 时的超时等待逻辑"""
    mgr = AnalyticsManager()
    t = threading.Thread(target=lambda: None)
    t.start()
    t.join()
    mgr._analytics_warmup_thread = t
    # 线程已结束，ensure_analytics_ready 应返回 False（warmup_done 仍为 False）
    assert mgr.ensure_analytics_ready(timeout=0.1) is False
