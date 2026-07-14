"""验证脚本: 确认移除auto_start绕过逻辑后的修复正确性

验证要点:
1. t_type_bootstrap.on_init()不包含auto_start逻辑
2. strategy_2026.py的on_init()不读取auto_start_after_init
3. strategy_2026.py的onTick没有自愈机制
4. t_type_bootstrap生命周期方法正确委托
5. LifecycleCallbacks系统委托链完整
"""
import ast
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

PASS_COUNT = 0
FAIL_COUNT = 0

def assert_true(condition, msg):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        logger.info("[PASS] %s", msg)
    else:
        FAIL_COUNT += 1
        logger.error("[FAIL] %s", msg)

def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

# ============================================================
# 测试1: t_type_bootstrap.on_init()不包含auto_start逻辑
# ============================================================
def test_bootstrap_no_auto_start():
    """验证on_init()不调用on_start()"""
    content = read_file('t_type_bootstrap.py')

    # 不应包含auto_start调用
    assert_true('self.on_start()' not in content.split('def on_init')[1].split('def on_start')[0],
                "on_init()不调用self.on_start()")

    # 不应包含auto_start_after_init读取
    assert_true('auto_start_after_init' not in content.split('def on_init')[1].split('def on_start')[0],
                "on_init()不读取auto_start_after_init配置")

    # 不应包含同步调用on_start的注释
    assert_true('同步调用on_start' not in content,
                "不包含同步调用on_start的注释")

    # 版本号应为2.9.0
    assert_true("__version__ = '2.9.0'" in content,
                "版本号为2.9.0")

# ============================================================
# 测试2: strategy_2026.py的on_init()不读取auto_start_after_init
# ============================================================
def test_strategy2026_no_auto_start():
    """验证Strategy2026.on_init()不包含auto_start逻辑"""
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')

    # on_init方法中不应包含auto_start_after_init
    on_init_section = content.split('def on_init')[1].split('def on_stop')[0]
    assert_true('auto_start_after_init' not in on_init_section,
                "Strategy2026.on_init()不读取auto_start_after_init")

    # on_init方法中不应包含self.on_start()调用
    assert_true('self.on_start()' not in on_init_section,
                "Strategy2026.on_init()不调用self.on_start()")

    # 不应存储_auto_start_after_init属性
    assert_true('_auto_start_after_init' not in on_init_section,
                "Strategy2026.on_init()不存储_auto_start_after_init")

# ============================================================
# 测试3: strategy_2026.py的onTick没有自愈机制
# ============================================================
def test_strategy2026_no_self_heal():
    """验证onTick没有自愈机制"""
    content = read_file('ali2026v3_trading/strategy/strategy_2026.py')

    # onTick方法中不应包含自愈机制
    on_tick_section = content.split('def onTick')[1].split('def on')[0] if 'def onTick' in content else ''

    # 不应包含_auto_running_attempt
    assert_true('_auto_running_attempt' not in on_tick_section,
                "onTick不包含_auto_running_attempt自愈逻辑")

    # 不应包含AUTO-RUNNING日志
    assert_true('AUTO-RUNNING' not in on_tick_section,
                "onTick不包含AUTO-RUNNING日志")

    # 不应包含_allow_self_heal
    assert_true('_allow_self_heal' not in on_tick_section,
                "onTick不包含_allow_self_heal")

# ============================================================
# 测试4: t_type_bootstrap生命周期方法正确委托
# ============================================================
def test_bootstrap_delegation():
    """验证t_type_bootstrap生命周期方法正确委托"""
    content = read_file('t_type_bootstrap.py')

    # on_start委托到real.on_start()
    on_start_section = content.split('def on_start')[1].split('def on_stop')[0]
    assert_true('real.on_start()' in on_start_section,
                "on_start()委托到real.on_start()")
    assert_true('self.trading = True' in on_start_section,
                "on_start()设置self.trading=True")

    # on_stop委托到real.on_stop()
    on_stop_section = content.split('def on_stop')[1].split('def onTick')[0]
    assert_true('real.on_stop()' in on_stop_section,
                "on_stop()委托到real.on_stop()")
    assert_true('self.trading = False' in on_stop_section,
                "on_stop()设置self.trading=False")

    # pause委托到real.pause()
    pause_section = content.split('def pause')[1].split('def on_pause')[0]
    assert_true('real.pause()' in pause_section,
                "pause()委托到real.pause()")

    # resume委托到real.resume()
    resume_section = content.split('def resume')[1].split('def on_resume')[0]
    assert_true('real.resume()' in resume_section,
                "resume()委托到real.resume()")

    # on_destroy委托到real.on_destroy()
    on_destroy_section = content.split('def on_destroy')[1].split('def internal')[0]
    assert_true('real.on_destroy()' in on_destroy_section,
                "on_destroy()委托到real.on_destroy()")

# ============================================================
# 测试5: LifecycleCallbacks系统委托链完整
# ============================================================
def test_lifecycle_callbacks_chain():
    """验证LifecycleCallbacks系统委托链完整"""
    content = read_file('ali2026v3_trading/lifecycle/lifecycle_callbacks.py')

    # on_start方法存在
    assert_true('def on_start(self)' in content,
                "LifecycleCallbacks.on_start()存在")

    # on_stop方法存在
    assert_true('def on_stop(self)' in content,
                "LifecycleCallbacks.on_stop()存在")

    # on_destroy方法存在
    assert_true('def on_destroy(self)' in content,
                "LifecycleCallbacks.on_destroy()存在")

    # pause方法存在
    assert_true('def pause(self)' in content,
                "LifecycleCallbacks.pause()存在")

    # resume方法存在
    assert_true('def resume(self)' in content,
                "LifecycleCallbacks.resume()存在")

    # transition_to调用存在于on_start
    on_start_section = content.split('def on_start(self)')[1].split('def on_stop(self)')[0]
    assert_true('transition_to(StrategyState.RUNNING)' in on_start_section,
                "on_start()调用transition_to(RUNNING)")

    # 四元状态同步存在于on_start
    assert_true('_is_paused = False' in on_start_section,
                "on_start()设置_is_paused=False")
    assert_true('_is_running = True' in on_start_section,
                "on_start()设置_is_running=True")
    assert_true('_is_trading = True' in on_start_section,
                "on_start()设置_is_trading=True")

# ============================================================
# 测试6: LifecycleService.transition_to同步_state_store
# ============================================================
def test_lifecycle_service_state_sync():
    """验证LifecycleService.transition_to同步_state_store"""
    content = read_file('ali2026v3_trading/strategy/lifecycle_service.py')

    # transition_to方法存在
    assert_true('def transition_to(self, new_state)' in content,
                "LifecycleService.transition_to()存在")

    # 同步到_state_store
    assert_true("_ss.set('_state'" in content,
                "transition_to()同步_state到_state_store")
    assert_true("_ss.set('_is_running'" in content,
                "transition_to()同步_is_running到_state_store")

# ============================================================
# 测试7: StrategyCoreService委托链完整
# ============================================================
def test_core_service_delegation():
    """验证StrategyCoreService委托链完整"""
    content = read_file('ali2026v3_trading/strategy/strategy_core_service.py')

    # on_start委托到_lifecycle_svc
    assert_true('def on_start(self): return self._lifecycle_svc.on_start()' in content,
                "StrategyCoreService.on_start()委托到_lifecycle_svc")

    # on_stop委托到_lifecycle_svc
    assert_true('def on_stop(self): return self._lifecycle_svc.on_stop()' in content,
                "StrategyCoreService.on_stop()委托到_lifecycle_svc")

    # on_destroy委托到_lifecycle_svc
    assert_true('def on_destroy(self): return self._lifecycle_svc.on_destroy()' in content,
                "StrategyCoreService.on_destroy()委托到_lifecycle_svc")

    # start委托到_lifecycle_svc
    assert_true('def start(self): return self._lifecycle_svc.start()' in content,
                "StrategyCoreService.start()委托到_lifecycle_svc")

    # pause委托到_lifecycle_svc
    assert_true('def pause(self): return self._lifecycle_svc.pause()' in content,
                "StrategyCoreService.pause()委托到_lifecycle_svc")

    # resume委托到_lifecycle_svc
    assert_true('def resume(self): return self._lifecycle_svc.resume()' in content,
                "StrategyCoreService.resume()委托到_lifecycle_svc")


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    logger.info("=" * 70)
    logger.info("FIX-20260714-ROOT-CAUSE 验证脚本")
    logger.info("移除auto_start绕过逻辑，回归C++平台正常状态管理")
    logger.info("=" * 70)

    test_bootstrap_no_auto_start()
    test_strategy2026_no_auto_start()
    test_strategy2026_no_self_heal()
    test_bootstrap_delegation()
    test_lifecycle_callbacks_chain()
    test_lifecycle_service_state_sync()
    test_core_service_delegation()

    logger.info("=" * 70)
    logger.info("结果: %d PASS, %d FAIL", PASS_COUNT, FAIL_COUNT)
    if FAIL_COUNT == 0:
        logger.info("[ROOT-CAUSE-FIXED] 所有验证通过")
    else:
        logger.error("[ROOT-CAUSE-FAILED] 存在失败项")
    logger.info("=" * 70)

    sys.exit(0 if FAIL_COUNT == 0 else 1)
