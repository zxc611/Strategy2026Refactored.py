"""功能最后环节运行时断言验证

验证对象: lifecycle_callbacks.py / lifecycle_service.py / strategy_config_layer.py
验证重点:
1. pause() 原子同步四元状态，并设置 W6/W7/W8/W9/W10/W14 的停止事件
2. on_stop() 原子同步四元状态，并设置/等待 W7/W8/W9/W11 线程退出
3. _cancel_all_timers() 扩展停止 W7/W8/W9/W11
4. 启动阶段 daemon 线程(stop 事件)在 pause/stop 时能被正确通知
"""
from __future__ import annotations

import os
import sys
import threading
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PASS_COUNT = 0
FAIL_COUNT = 0


def assert_true(condition, msg):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"  [PASS] {msg}")
    else:
        FAIL_COUNT += 1
        print(f"  [FAIL] {msg}")
        traceback.print_stack(limit=5)


class _FakeSchedulerManager:
    def __init__(self):
        self.paused = False
        self.removed = 0

    def pause_scheduler(self):
        self.paused = True

    def resume_scheduler(self):
        self.paused = False

    def remove_jobs_by_owner(self, owner):
        self.removed += 1
        return self.removed

    def wait_for_jobs_zero(self, timeout=10.0):
        return True


class _FakeTickSvc:
    def __init__(self):
        self.stopped = False

    def on_stop(self):
        self.stopped = True


class _FakeTickHandler:
    def __init__(self):
        self.flushed = False

    def _flush_tick_buffer(self):
        self.flushed = True


class _FakeStorage:
    def __init__(self):
        self.drained = False

    def drain_all_queues(self, timeout_per_queue=2.0):
        self.drained = True
        return {}


class _StoppableThread(threading.Thread):
    """模拟长时间运行的启动阶段 daemon 线程"""
    def __init__(self, stop_event, desc):
        super().__init__(target=self._run, args=(stop_event, desc), name=f"{desc}[test]", daemon=True)
        self._stop_event = stop_event
        self._desc = desc
        self.exited = False
        self.started_flag = threading.Event()

    def _run(self, stop_event, desc):
        self.started_flag.set()
        # 等待停止事件，模拟工作
        stop_event.wait(timeout=10.0)
        self.exited = True


class _MockProvider:
    def __init__(self):
        from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
        self._lock = threading.RLock()
        self._state = StrategyState.RUNNING
        self._is_running = True
        self._is_paused = False
        self._is_trading = True
        self._destroyed = False
        self._initialized = True
        self.strategy_id = 9999
        self._lifecycle_run_id = "test-run"
        self._scheduler_manager = _FakeSchedulerManager()
        self._tick_handler = _FakeTickHandler()
        self._tick_svc = _FakeTickSvc()
        self.storage = _FakeStorage()
        self._state_store = None
        self._lifecycle_mgr = None
        self._event_bus = None
        self._lifecycle_platform = self
        self._lifecycle_resource = self
        self._stats = {}

        # 线程与停止事件
        self._platform_subscribe_stop = threading.Event()
        self._bulk_subscribe_stop = threading.Event()
        self._subscribe_retry_stop = threading.Event()
        self._deferred_subscribe_stop = threading.Event()
        self._historical_kline_stop = threading.Event()
        self._platform_subscribe_thread = None
        self._bulk_subscribe_thread = None
        self._subscribe_retry_thread = None
        self._deferred_subscribe_thread = None
        self._historical_kline_thread = None

        # 回调标记
        self.stop_scheduler_called = False
        self.unsubscribe_called = False
        self.shutdown_runtime_called = False
        self.flush_tick_called = False
        self.save_state_called = False
        self.events_published = []

    def transition_to(self, state):
        from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
        self._state = state
        if state == StrategyState.RUNNING:
            self._is_running = True
            self._is_paused = False
        elif state == StrategyState.PAUSED:
            self._is_running = False
            self._is_paused = True
            self._is_trading = False
        elif state == StrategyState.STOPPED:
            self._is_running = False
            self._is_paused = False
            self._is_trading = False

    def _stop_scheduler(self):
        self.stop_scheduler_called = True

    def _unsubscribe_all_instruments(self):
        self.unsubscribe_called = True

    def _shutdown_runtime_services(self):
        self.shutdown_runtime_called = True

    def _flush_tick_buffer(self):
        self.flush_tick_called = True

    def save_state(self):
        self.save_state_called = True
        return True

    def _publish_event(self, event_type, data):
        self.events_published.append((event_type, data))

    def unsubscribe_all(self):
        self.unsubscribe_called = True

    def cleanup_all(self, level='normal'):
        pass

    def _log_resource_ownership_table(self, phase='unknown'):
        pass

    def _cancel_all_timers(self):
        # 测试入口会由 LifecycleService 提供，Mock 不需要实现
        pass


def _make_callbacks(provider):
    from ali2026v3_trading.lifecycle.lifecycle_callbacks import LifecycleCallbacks
    return LifecycleCallbacks(provider)


def test_pause_stops_background_works():
    print("\n=== pause() 停止后台工作运行时验证 ===")
    from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
    p = _MockProvider()
    cb = _make_callbacks(p)

    # 模拟 W7/W8/W9 正在运行
    p._bulk_subscribe_thread = _StoppableThread(p._bulk_subscribe_stop, 'db-subscribe')
    p._subscribe_retry_thread = _StoppableThread(p._subscribe_retry_stop, 'subscribe-retry')
    p._deferred_subscribe_thread = _StoppableThread(p._deferred_subscribe_stop, 'deferred-subscribe')
    for t in (p._bulk_subscribe_thread, p._subscribe_retry_thread, p._deferred_subscribe_thread):
        t.start()
        t.started_flag.wait(timeout=2.0)

    result = cb.pause()

    # 四元状态
    assert_true(result is True, "pause()返回True")
    assert_true(p._state == StrategyState.PAUSED, "pause()后_state=PAUSED")
    assert_true(p._is_running is False, "pause()后_is_running=False")
    assert_true(p._is_paused is True, "pause()后_is_paused=True")
    assert_true(p._is_trading is False, "pause()后_is_trading=False")

    # 调度器/缓存
    assert_true(p._scheduler_manager.paused is True, "pause()调用pause_scheduler")
    assert_true(p._tick_handler.flushed is True, "pause()flush tick buffer")
    assert_true(p.storage.drained is True, "pause()drain storage queues")

    # 停止事件
    assert_true(p._bulk_subscribe_stop.is_set(), "pause()设置_bulk_subscribe_stop")
    assert_true(p._subscribe_retry_stop.is_set(), "pause()设置_subscribe_retry_stop")
    assert_true(p._deferred_subscribe_stop.is_set(), "pause()设置_deferred_subscribe_stop")
    assert_true(p._platform_subscribe_stop.is_set(), "pause()设置_platform_subscribe_stop")
    assert_true(p._tick_svc.stopped is True, "pause()调用_tick_svc.on_stop()")

    # daemon 线程应快速退出
    for t in (p._bulk_subscribe_thread, p._subscribe_retry_thread, p._deferred_subscribe_thread):
        if t is None:
            continue
        t.join(timeout=3.0)
        assert_true(not t.is_alive(), f"pause()后 {t.name} 线程已退出")


def test_on_stop_stops_all_threads():
    print("\n=== on_stop() 停止所有线程运行时验证 ===")
    from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
    p = _MockProvider()
    cb = _make_callbacks(p)

    # 模拟 W7/W8/W9/W11 正在运行
    p._bulk_subscribe_thread = _StoppableThread(p._bulk_subscribe_stop, 'db-subscribe')
    p._subscribe_retry_thread = _StoppableThread(p._subscribe_retry_stop, 'subscribe-retry')
    p._deferred_subscribe_thread = _StoppableThread(p._deferred_subscribe_stop, 'deferred-subscribe')
    p._historical_kline_thread = _StoppableThread(p._historical_kline_stop, 'kline-load-async')
    for t in (p._bulk_subscribe_thread, p._subscribe_retry_thread,
              p._deferred_subscribe_thread, p._historical_kline_thread):
        t.start()
        t.started_flag.wait(timeout=2.0)

    try:
        result = cb.on_stop()
    except Exception as _e:
        import traceback as _tb
        with open('on_stop_error.log', 'w', encoding='utf-8') as _f:
            _f.write("[DEBUG] cb.on_stop() raised:\n")
            _tb.print_exc(file=_f)
        print("[DEBUG] cb.on_stop() raised: see on_stop_error.log")
        raise

    assert_true(result is True, "on_stop()返回True")
    assert_true(p._state == StrategyState.STOPPED, "on_stop()后_state=STOPPED")
    assert_true(p._is_running is False, "on_stop()后_is_running=False")
    assert_true(p._is_paused is False, "on_stop()后_is_paused=False")
    assert_true(p._is_trading is False, "on_stop()后_is_trading=False")

    # 所有停止事件已设置
    assert_true(p._bulk_subscribe_stop.is_set(), "on_stop()设置_bulk_subscribe_stop")
    assert_true(p._subscribe_retry_stop.is_set(), "on_stop()设置_subscribe_retry_stop")
    assert_true(p._deferred_subscribe_stop.is_set(), "on_stop()设置_deferred_subscribe_stop")
    assert_true(p._historical_kline_stop.is_set(), "on_stop()设置_historical_kline_stop")
    assert_true(p._platform_subscribe_stop.is_set(), "on_stop()设置_platform_subscribe_stop")

    # 所有线程应退出（on_stop可能已将引用置None，按原始引用join）
    _threads_to_join = [
        p._bulk_subscribe_thread, p._subscribe_retry_thread,
        p._deferred_subscribe_thread, p._historical_kline_thread,
    ]
    for t in _threads_to_join:
        if t is None:
            continue
        t.join(timeout=3.0)
        assert_true(not t.is_alive(), f"on_stop()后 {t.name} 线程已退出")

    assert_true(p.stop_scheduler_called, "on_stop()调用_stop_scheduler")
    assert_true(p.unsubscribe_called, "on_stop()调用unsubscribe")
    assert_true(p.shutdown_runtime_called, "on_stop()调用_shutdown_runtime_services")
    assert_true(p.flush_tick_called, "on_stop()调用_flush_tick_buffer")
    assert_true(p.save_state_called, "on_stop()调用save_state")


def test_cancel_all_timers_extension():
    print("\n=== _cancel_all_timers() 扩展停止运行时验证 ===")
    from ali2026v3_trading.strategy.lifecycle_service import LifecycleService
    p = _MockProvider()
    svc = LifecycleService(provider=p, strategy_id='test')

    p._bulk_subscribe_thread = _StoppableThread(p._bulk_subscribe_stop, 'db-subscribe')
    p._subscribe_retry_thread = _StoppableThread(p._subscribe_retry_stop, 'subscribe-retry')
    p._deferred_subscribe_thread = _StoppableThread(p._deferred_subscribe_stop, 'deferred-subscribe')
    p._historical_kline_thread = _StoppableThread(p._historical_kline_stop, 'kline-load-async')
    for t in (p._bulk_subscribe_thread, p._subscribe_retry_thread,
              p._deferred_subscribe_thread, p._historical_kline_thread):
        t.start()
        t.started_flag.wait(timeout=2.0)

    svc._cancel_all_timers()

    assert_true(p._bulk_subscribe_stop.is_set(), "_cancel_all_timers设置_bulk_subscribe_stop")
    assert_true(p._subscribe_retry_stop.is_set(), "_cancel_all_timers设置_subscribe_retry_stop")
    assert_true(p._deferred_subscribe_stop.is_set(), "_cancel_all_timers设置_deferred_subscribe_stop")
    assert_true(p._historical_kline_stop.is_set(), "_cancel_all_timers设置_historical_kline_stop")

    for t in (p._bulk_subscribe_thread, p._subscribe_retry_thread,
              p._deferred_subscribe_thread, p._historical_kline_thread):
        if t is None:
            continue
        t.join(timeout=3.0)
        assert_true(not t.is_alive(), f"_cancel_all_timers后 {t.name} 线程已退出")


def test_thread_stop_events_initialized():
    print("\n=== 线程停止事件已初始化验证 ===")
    from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
    # 创建一个 StrategyCoreService 实例，验证 config_layer 已初始化所有退出事件
    core = StrategyCoreService(strategy_id='test_init')
    p = core
    assert_true(hasattr(p, '_bulk_subscribe_stop') and isinstance(p._bulk_subscribe_stop, threading.Event),
                "StrategyCoreService._bulk_subscribe_stop已初始化")
    assert_true(hasattr(p, '_subscribe_retry_stop') and isinstance(p._subscribe_retry_stop, threading.Event),
                "StrategyCoreService._subscribe_retry_stop已初始化")
    assert_true(hasattr(p, '_deferred_subscribe_stop') and isinstance(p._deferred_subscribe_stop, threading.Event),
                "StrategyCoreService._deferred_subscribe_stop已初始化")
    assert_true(hasattr(p, '_historical_kline_stop') and isinstance(p._historical_kline_stop, threading.Event),
                "StrategyCoreService._historical_kline_stop已初始化")
    assert_true(hasattr(p, '_historical_kline_thread'), "StrategyCoreService._historical_kline_thread已初始化")


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("=" * 70)
    print("20260714 生命周期功能最后环节运行时断言验证")
    print("=" * 70)

    test_thread_stop_events_initialized()
    test_pause_stops_background_works()
    test_on_stop_stops_all_threads()
    test_cancel_all_timers_extension()

    summary_lines = [
        "",
        "=" * 70,
        f"结果: {PASS_COUNT} PASS, {FAIL_COUNT} FAIL",
        "[RUNTIME-VERIFIED] 所有运行时断言验证通过" if FAIL_COUNT == 0 else "[RUNTIME-FAILED] 存在失败项",
        "=" * 70,
    ]
    print("\n".join(summary_lines))
    with open('runtime_verification_summary.txt', 'w', encoding='utf-8') as _f:
        _f.write("\n".join(summary_lines) + "\n")
    sys.exit(0 if FAIL_COUNT == 0 else 1)
