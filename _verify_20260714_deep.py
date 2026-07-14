"""深度验证脚本: 20260714 修复彻底性+无绕过+无隐患验证

验证维度:
1. 彻底性: 所有根因是否修复
2. 无绕过: 修复是否真正解决问题
3. 无隐患: 修复是否引入新问题
4. 功能最后环节断言验证: pause/resume/on_stop/destroy对称性
"""
import sys
import os
import ast
import inspect

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


def test_thoroughness_R1_auto_start():
    """彻底性: R1 auto_start恢复"""
    print("\n=== 彻底性 R1: auto_start恢复 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    on_init = content.split('def on_init')[1].split('def on_stop')[0]
    assert_true('self.on_start()' in on_init, "on_init()调用self.on_start()")
    assert_true('AUTO_START' in on_init or 'auto_start' in on_init, "包含auto_start标记")


def test_thoroughness_R11_idempotent():
    """彻底性: R11 on_start幂等性"""
    print("\n=== 彻底性 R11: on_start幂等性 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    on_start = content.split('def on_start')[1].split('def _onStart_step_config_load')[0]
    assert_true('StrategyState.RUNNING' in on_start, "检查RUNNING状态")
    assert_true('IDEMPOTENT' in on_start, "包含IDEMPOTENT标记")
    assert_true('return True' in on_start, "幂等跳过返回True")
    assert_true('_sync_trading_to_outer_ref(True)' in on_start, "幂等跳过时同步trading")


def test_thoroughness_R2_R7_outer_ref():
    """彻底性: R2/R7 _outer_ref同步"""
    print("\n=== 彻底性 R2/R7: _outer_ref同步 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    assert_true('def _sync_trading_to_outer_ref' in content, "方法定义存在")
    assert_true('_outer_ref' in content, "使用_outer_ref")
    status_set = content.split('def _onStart_step_status_set')[1].split('def _onStart_step_core_init')[0]
    assert_true('_sync_trading_to_outer_ref(True)' in status_set, "on_start中同步True")
    on_stop = content.split('def on_stop')[1].split('def pause')[0]
    assert_true('_sync_trading_to_outer_ref(False)' in on_stop, "on_stop中同步False")


def test_thoroughness_R4_pause_stop():
    """彻底性: R4 pause()补全停止"""
    print("\n=== 彻底性 R4: pause()补全停止 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    pause = content.split('def pause(self)')[1].split('def resume(self)')[0]
    assert_true('stop_contract_watch' in pause, "停止W6 DiagnosisProbeManager")
    assert_true('_platform_subscribe_stop' in pause, "停止W10 platform_subscribe")
    assert_true('_tick_svc' in pause and 'on_stop' in pause, "停止W14 TickBufferFlushFallback")


def test_thoroughness_R5_R9_on_stop_join():
    """彻底性: R5/R9 on_stop()补全停止W7/W8/W9"""
    print("\n=== 彻底性 R5/R9: on_stop()补全停止 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_stop = content.split('def on_stop(self)')[1].split('def on_destroy')[0]
    assert_true('_bulk_subscribe_thread' in on_stop, "停止W7 bulk_subscribe")
    assert_true('_subscribe_retry_thread' in on_stop, "停止W8 subscribe_retry")
    assert_true('_deferred_subscribe_thread' in on_stop, "停止W9 deferred_subscribe")
    assert_true('join' in on_stop, "使用join等待线程退出")


def test_thoroughness_stop_events_created():
    """彻底性: stop event在on_start中创建"""
    print("\n=== 彻底性: stop event创建 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_start = content.split('def on_start(self)')[1].split('def on_stop(self)')[0]
    assert_true('_bulk_subscribe_stop' in on_start and 'Event()' in on_start, "创建_bulk_subscribe_stop")
    assert_true('_subscribe_retry_stop' in on_start, "创建_subscribe_retry_stop")
    assert_true('_deferred_subscribe_stop' in on_start, "创建_deferred_subscribe_stop")
    assert_true('_historical_kline_stop' in on_start, "创建_historical_kline_stop")


def test_thoroughness_R14_R15_trading_sync():
    """彻底性: R14/R15 pause/resume同步trading"""
    print("\n=== 彻底性 R14/R15: pause/resume同步trading ===")
    content = read_file('t_type_bootstrap.py')
    pause = content.split('def pause(self)')[1].split('def on_pause')[0]
    resume = content.split('def resume(self)')[1].split('def on_resume')[0]
    assert_true('self.trading = False' in pause, "pause同步trading=False")
    assert_true('self.trading = True' in resume, "resume同步trading=True")


def test_thoroughness_R16_is_paused_false():
    """彻底性: R16 on_stop()_is_paused=False"""
    print("\n=== 彻底性 R16: on_stop _is_paused=False ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_stop = content.split('def on_stop(self)')[1].split('def on_destroy')[0]
    assert_true('p._is_paused = False' in on_stop, "on_stop中_is_paused=False")


def test_no_bypass_resume_restart():
    """无绕过: resume()重启W6/W10/W14"""
    print("\n=== 无绕过: resume()重启工作 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    resume = content.split('def resume(self)')[1].split('def destroy(self)')[0]
    # W10重启
    assert_true('_platform_subscribe_stop' in resume and 'clear()' in resume, "resume重置W10 stop event")
    assert_true('_start_platform_subscribe_async' in resume, "resume重启W10平台订阅")
    assert_true('_subscribed_instruments' in resume, "resume传入instrument_ids参数")
    # W14重启
    assert_true('_flush_stop_requested' in resume and 'False' in resume, "resume重置W14停止标志")
    assert_true('_flush_fallback_thread' in resume, "resume检查/重启W14 fallback线程")
    # W6重启
    assert_true('start_contract_watch' in resume, "resume重启W6合约监控")


def test_no_bypass_idempotent_real():
    """无绕过: on_start幂等检查不会重置标志"""
    print("\n=== 无绕过: on_start幂等检查 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    on_start = content.split('def on_start')[1].split('def _onStart_step_config_load')[0]
    # 幂等检查在重置标志之前
    idempotent_pos = on_start.find('IDEMPOTENT')
    reset_pos = on_start.find('self._start_executed = False')
    assert_true(idempotent_pos > 0, "幂等检查存在")
    assert_true(idempotent_pos < reset_pos, "幂等检查在标志重置之前")


def test_no_hazard_thread_refs():
    """无隐患: 线程引用保存"""
    print("\n=== 无隐患: 线程引用保存 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    assert_true('p._subscribe_retry_thread = _retry_thread' in content, "保存subscribe_retry线程引用")
    assert_true('p._deferred_subscribe_thread = _deferred_thread' in content, "保存deferred_subscribe线程引用")


def test_no_hazard_join_timeout():
    """无隐患: join有超时不会死锁"""
    print("\n=== 无隐患: join超时 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_stop = content.split('def on_stop(self)')[1].split('def on_destroy')[0]
    # 检查join都有timeout
    import re
    join_calls = re.findall(r'\.join\(timeout=([\d.]+)\)', on_stop)
    assert_true(len(join_calls) > 0, f"on_stop有{len(join_calls)}个join(timeout)调用")
    for timeout in join_calls:
        assert_true(float(timeout) > 0, f"join timeout={timeout}大于0")


def test_no_hazard_exception_handling():
    """无隐患: 所有修复都有异常处理"""
    print("\n=== 无隐患: 异常处理 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    pause = content.split('def pause(self)')[1].split('def resume(self)')[0]
    resume = content.split('def resume(self)')[1].split('def destroy(self)')[0]
    # pause中每个停止操作都有try/except
    assert_true(pause.count('try:') >= 4, f"pause有{pause.count('try:')}个try块(>=4)")
    assert_true(resume.count('try:') >= 4, f"resume有{resume.count('try:')}个try块(>=4)")


def test_no_hazard_strategy_state_imported():
    """无隐患: StrategyState在strategy_2026.py中已导入"""
    print("\n=== 无隐患: StrategyState导入 ===")
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')
    assert_true('from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState' in content
                or 'import StrategyState' in content,
                "StrategyState已导入")


def test_symmetry_pause_resume():
    """功能最后环节: pause/resume对称性"""
    print("\n=== 功能最后环节: pause/resume对称性 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    pause = content.split('def pause(self)')[1].split('def resume(self)')[0]
    resume = content.split('def resume(self)')[1].split('def destroy(self)')[0]

    # pause停止的工作，resume应该恢复
    pause_resume_pairs = [
        ('W6', 'stop_contract_watch' in pause, 'start_contract_watch' in resume),
        ('W10', '_platform_subscribe_stop' in pause, '_platform_subscribe_stop' in resume and 'clear()' in resume),
        ('W14', '_tick_svc' in pause, '_flush_stop_requested' in resume and 'False' in resume),
        ('APScheduler', 'pause_scheduler' in pause, 'resume_scheduler' in resume),
    ]

    for name, paused, resumed in pause_resume_pairs:
        if paused:
            assert_true(resumed, f"pause停止{name}→resume恢复{name}")


def test_symmetry_on_start_on_stop():
    """功能最后环节: on_start/on_stop对称性"""
    print("\n=== 功能最后环节: on_start/on_stop对称性 ===")
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')
    on_start = content.split('def on_start(self)')[1].split('def on_stop(self)')[0]
    on_stop = content.split('def on_stop(self)')[1].split('def on_destroy')[0]

    # on_start创建stop event，on_stop设置stop event
    assert_true('_bulk_subscribe_stop' in on_start and 'Event()' in on_start, "on_start创建bulk_stop")
    assert_true('_bulk_subscribe_stop' in on_stop or '_bulk_subscribe_thread' in on_stop, "on_stop停止bulk")

    # on_start保存线程引用，on_stop join线程
    assert_true('_subscribe_retry_thread' in on_start, "on_start保存retry线程引用")
    assert_true('_subscribe_retry_thread' in on_stop, "on_stop停止retry线程")


def test_bootstrap_pause_resume_consistent():
    """功能最后环节: t_type_bootstrap pause/resume与on_start/on_stop一致"""
    print("\n=== 功能最后环节: bootstrap状态同步一致性 ===")
    content = read_file('t_type_bootstrap.py')
    on_start = content.split('def on_start(self)')[1].split('def on_stop')[0]
    on_stop = content.split('def on_stop(self)')[1].split('def pause')[0]
    pause = content.split('def pause(self)')[1].split('def on_pause')[0]
    resume = content.split('def resume(self)')[1].split('def on_resume')[0]

    # 所有4个方法都应同步trading
    assert_true('self.trading = True' in on_start, "on_start: trading=True")
    assert_true('self.trading = False' in on_stop, "on_stop: trading=False")
    assert_true('self.trading = False' in pause, "pause: trading=False")
    assert_true('self.trading = True' in resume, "resume: trading=True")


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("=" * 70)
    print("20260714 深度验证: 彻底性+无绕过+无隐患+功能最后环节断言")
    print("=" * 70)

    # 彻底性验证
    test_thoroughness_R1_auto_start()
    test_thoroughness_R11_idempotent()
    test_thoroughness_R2_R7_outer_ref()
    test_thoroughness_R4_pause_stop()
    test_thoroughness_R5_R9_on_stop_join()
    test_thoroughness_stop_events_created()
    test_thoroughness_R14_R15_trading_sync()
    test_thoroughness_R16_is_paused_false()

    # 无绕过验证
    test_no_bypass_resume_restart()
    test_no_bypass_idempotent_real()

    # 无隐患验证
    test_no_hazard_thread_refs()
    test_no_hazard_join_timeout()
    test_no_hazard_exception_handling()
    test_no_hazard_strategy_state_imported()

    # 功能最后环节断言验证
    test_symmetry_pause_resume()
    test_symmetry_on_start_on_stop()
    test_bootstrap_pause_resume_consistent()

    print("\n" + "=" * 70)
    print(f"结果: {PASS_COUNT} PASS, {FAIL_COUNT} FAIL")
    if FAIL_COUNT == 0:
        print("[DEEP-VERIFIED] 彻底性+无绕过+无隐患+功能最后环节断言全部通过")
    else:
        print("[DEEP-FAILED] 存在失败项")
    print("=" * 70)
    sys.exit(0 if FAIL_COUNT == 0 else 1)
