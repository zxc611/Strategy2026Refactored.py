"""验证脚本: 20260714 修复断言验证

验证所有根因修复:
R1: auto_start在strategy_2026.py on_init()恢复
R2/R3/R7: _sync_trading_to_outer_ref方法存在并在on_start/on_stop中调用
R4: pause()补全停止W6/W10/W14
R5/R9/R17: on_stop()补全停止W7/W8/W9 + 保存线程引用
R11: on_start()幂等性检查
R14/R15: pause()/resume()同步trading
R16: on_stop()中_is_paused=False
"""
import sys
import os

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


def read_file(rel_path):
    with open(rel_path, 'r', encoding='utf-8') as f:
        return f.read()


def test_R1_auto_start_restored():
    """R1: auto_start在strategy_2026.py on_init()恢复"""
    print("\n=== R1: auto_start恢复 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    on_init_section = content.split('def on_init')[1].split('def on_stop')[0]

    # on_init中应包含auto_start调用
    assert_true('self.on_start()' in on_init_section,
                "on_init()中恢复auto_start调用self.on_start()")

    # 应包含auto_start注释
    assert_true('AUTO_START' in on_init_section or 'auto_start' in on_init_section,
                "on_init()包含auto_start注释标记")


def test_R11_on_start_idempotent():
    """R11: on_start()幂等性检查"""
    print("\n=== R11: on_start()幂等性 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    on_start_section = content.split('def on_start')[1].split('def _onStart_step_config_load')[0]

    # 应包含RUNNING状态检查
    assert_true('StrategyState.RUNNING' in on_start_section,
                "on_start()检查RUNNING状态")

    # 应包含IDEMPOTENT标记
    assert_true('IDEMPOTENT' in on_start_section,
                "on_start()包含IDEMPOTENT幂等标记")

    # 应包含return True（跳过重复执行）
    assert_true('return True' in on_start_section,
                "on_start()幂等跳过时返回True")


def test_R2_R7_sync_trading_to_outer_ref():
    """R2/R3/R7: _sync_trading_to_outer_ref方法"""
    print("\n=== R2/R3/R7: _sync_trading_to_outer_ref ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')

    # 方法存在
    assert_true('def _sync_trading_to_outer_ref' in content,
                "_sync_trading_to_outer_ref方法存在")

    # 在_onStart_step_status_set中调用
    status_set_section = content.split('def _onStart_step_status_set')[1].split('def _onStart_step_core_init')[0]
    assert_true('_sync_trading_to_outer_ref(True)' in status_set_section,
                "_onStart_step_status_set中调用_sync_trading_to_outer_ref(True)")

    # 在on_stop中调用
    on_stop_section = content.split('def on_stop')[1].split('def pause')[0]
    assert_true('_sync_trading_to_outer_ref(False)' in on_stop_section,
                "on_stop中调用_sync_trading_to_outer_ref(False)")

    # 使用_outer_ref
    assert_true('_outer_ref' in content.split('def _sync_trading_to_outer_ref')[1].split('def on_start')[0],
                "_sync_trading_to_outer_ref使用_outer_ref")


def test_R4_pause_stop_works():
    """R4: pause()补全停止W6/W10/W14"""
    print("\n=== R4: pause()补全停止 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    pause_section = content.split('def pause(self)')[1].split('def resume(self)')[0]

    # W6: DiagnosisProbeManager停止
    assert_true('stop_contract_watch' in pause_section,
                "pause()停止W6 DiagnosisProbeManager")

    # W10: _platform_subscribe_stop设置
    assert_true('_platform_subscribe_stop' in pause_section,
                "pause()停止W10 _platform_subscribe_stop.set()")

    # W14: _tick_svc.on_stop()调用
    assert_true('_tick_svc' in pause_section and 'on_stop' in pause_section,
                "pause()停止W14 TickBufferFlushFallback")


def test_R5_R9_R17_on_stop_join_threads():
    """R5/R9/R17: on_stop()补全停止W7/W8/W9"""
    print("\n=== R5/R9/R17: on_stop()补全停止 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_stop_section = content.split('def on_stop(self)')[1].split('def on_destroy')[0]

    # W7: _bulk_subscribe_thread join
    assert_true('_bulk_subscribe_thread' in on_stop_section and 'join' in on_stop_section,
                "on_stop()停止W7 _bulk_subscribe_thread")

    # W8: _subscribe_retry_thread join
    assert_true('_subscribe_retry_thread' in on_stop_section,
                "on_stop()停止W8 _subscribe_retry_thread")

    # W9: _deferred_subscribe_thread join
    assert_true('_deferred_subscribe_thread' in on_stop_section,
                "on_stop()停止W9 _deferred_subscribe_thread")


def test_R17_thread_refs_saved():
    """R17: 保存线程引用"""
    print("\n=== R17: 保存线程引用 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')

    # T2: subscribe-retry线程引用保存
    assert_true('_subscribe_retry_thread' in content and '_retry_thread' in content,
                "保存T2 subscribe-retry线程引用")

    # T3: deferred-subscribe线程引用保存
    assert_true('_deferred_subscribe_thread' in content and '_deferred_thread' in content,
                "保存T3 deferred-subscribe线程引用")


def test_R14_R15_pause_resume_trading_sync():
    """R14/R15: pause()/resume()同步trading"""
    print("\n=== R14/R15: pause/resume同步trading ===")
    content = read_file('t_type_bootstrap.py')

    # pause()中设置trading=False
    pause_section = content.split('def pause(self)')[1].split('def on_pause')[0]
    assert_true('self.trading = False' in pause_section,
                "pause()同步trading=False")

    # resume()中设置trading=True
    resume_section = content.split('def resume(self)')[1].split('def on_resume')[0]
    assert_true('self.trading = True' in resume_section,
                "resume()同步trading=True")


def test_R16_on_stop_is_paused_false():
    """R16: on_stop()中_is_paused=False"""
    print("\n=== R16: on_stop()_is_paused=False ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_stop_section = content.split('def on_stop(self)')[1].split('def on_destroy')[0]

    # 在on_stop的四元状态同步中，_is_paused应为False
    # 找到状态同步代码段
    assert_true('p._is_paused = False' in on_stop_section,
                "on_stop()中_is_paused=False（而非True）")


def test_on_start_outer_ref_sync_in_idempotent():
    """验证幂等检查中也同步trading到outer_ref"""
    print("\n=== 幂等检查中同步trading ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    on_start_section = content.split('def on_start')[1].split('def _onStart_step_config_load')[0]

    # 幂等跳过时也调用_sync_trading_to_outer_ref
    assert_true('_sync_trading_to_outer_ref(True)' in on_start_section,
                "幂等跳过时也调用_sync_trading_to_outer_ref(True)")


def test_R9_interruptible_sleep():
    """验证启动阶段daemon线程使用可中断sleep，避免stop期间阻塞"""
    print("\n=== R9: 可中断sleep检查 ===")
    content_cb = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')

    # subscribe-retry线程应使用stop_event.wait替代time.sleep
    retry_section = content_cb.split('def _retry_platform_subscribe')[1].split('def _load_deferred_instruments')[0]
    assert_true('.wait(' in retry_section,
                "subscribe-retry线程使用stop_event.wait()替代time.sleep")

    # deferred-subscribe线程应使用stop_event.wait替代time.sleep
    deferred_section = content_cb.split('def _load_deferred_instruments')[1].split('def _start_historical_kline_load_async')[0]
    assert_true('.wait(' in deferred_section,
                "deferred-subscribe线程使用stop_event.wait()替代time.sleep")

    # db-subscribe重试阶段应使用可中断sleep
    db_sub_section = content_cb.split('def _async_db_subscribe')[1].split('def _start_historical_kline_load_async')[0]
    assert_true('_bulk_subscribe_stop.wait(' in db_sub_section,
                "db-subscribe重试阶段使用stop_event.wait()替代time.sleep")


def test_R10_kline_join_interruptible():
    """验证历史K线线程在join订阅线程期间响应停止事件"""
    print("\n=== R10: K线join可中断检查 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_bind.py')
    kline_section = content.split('def _start_historical_kline_load_async')[1].split('def _start_historical_kline_load')[0]

    # 应使用循环join并检查_historical_kline_stop
    assert_true('while _t.is_alive()' in kline_section,
                "K线线程使用循环join订阅线程")
    assert_true('p._historical_kline_stop.is_set()' in kline_section,
                "K线线程在join期间检查_historical_kline_stop")


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("=" * 70)
    print("20260714 修复断言验证脚本")
    print("=" * 70)

    test_R1_auto_start_restored()
    test_R11_on_start_idempotent()
    test_R2_R7_sync_trading_to_outer_ref()
    test_R4_pause_stop_works()
    test_R5_R9_R17_on_stop_join_threads()
    test_R17_thread_refs_saved()
    test_R14_R15_pause_resume_trading_sync()
    test_R16_on_stop_is_paused_false()
    test_on_start_outer_ref_sync_in_idempotent()
    test_R9_interruptible_sleep()
    test_R10_kline_join_interruptible()

    print("\n" + "=" * 70)
    print(f"结果: {PASS_COUNT} PASS, {FAIL_COUNT} FAIL")
    if FAIL_COUNT == 0:
        print("[FIX-VERIFIED] 所有断言验证通过")
    else:
        print("[FIX-FAILED] 存在失败项")
    print("=" * 70)
    sys.exit(0 if FAIL_COUNT == 0 else 1)
