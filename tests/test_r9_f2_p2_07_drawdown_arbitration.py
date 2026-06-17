# MODULE_ID: M2-549
"""Round9-F2 断言测试：P2-07 DrawdownManager订阅EventBus日回撤硬停止事件
验证运行时行为：
1) DrawdownManager有_subscribe_daily_hard_stop_event方法
2) 硬停止触发后should_reduce_size/halt_new/full_stop返回True
3) EventBus事件能触发DrawdownManager状态同步
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_drawdown_manager_has_eventbus_subscription():
    """DrawdownManager应有_subscribe_daily_hard_stop_event方法"""
    from ali2026v3_trading.governance.mode_exit_rules import DrawdownManager, DrawdownAction
    dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
    assert hasattr(dm, '_subscribe_daily_hard_stop_event'), \
        "DrawdownManager缺少_subscribe_daily_hard_stop_event方法"
    assert hasattr(dm, '_on_daily_hard_stop_event'), \
        "DrawdownManager缺少_on_daily_hard_stop_event回调"
    print("  OK: DrawdownManager有EventBus订阅方法")


def test_hard_stop_triggers_conservative_decisions():
    """硬停止触发后，所有决策方法应返回最保守结果"""
    from ali2026v3_trading.governance.mode_exit_rules import DrawdownManager, DrawdownAction
    dm = DrawdownManager(DrawdownAction.REDUCE_SIZE)
    # 初始状态：无硬停止
    assert dm.should_reduce_size() == False, "初始状态不应触发减仓"
    assert dm.should_halt_new() == False, "初始状态不应触发暂停"
    assert dm.should_full_stop() == False, "初始状态不应触发全停"

    # 触发硬停止
    dm._on_daily_hard_stop_event(None)
    assert dm.should_reduce_size() == True, "硬停止后应触发减仓"
    assert dm.should_halt_new() == True, "硬停止后应触发暂停"
    assert dm.should_full_stop() == True, "硬停止后应触发全停"
    print("  OK: 硬停止后所有决策返回最保守结果")


def test_eventbus_event_triggers_hard_stop():
    """EventBus DailyDrawdownHaltEvent能触发DrawdownManager硬停止"""
    from ali2026v3_trading.governance.mode_exit_rules import DrawdownManager, DrawdownAction
    dm = DrawdownManager(DrawdownAction.HALT_NEW)
    # 模拟EventBus事件触发
    dm._on_daily_hard_stop_event(type('Event', (), {'drawdown_pct': 0.06}))
    assert dm._daily_hard_stop_triggered == True, \
        "EventBus事件后_daily_hard_stop_triggered应为True"
    assert dm.should_full_stop() == True, \
        "EventBus事件后应触发全停"
    print("  OK: EventBus事件成功触发DrawdownManager硬停止")


if __name__ == '__main__':
    print("=== Round9-F2: P2-07 DrawdownManager EventBus同步 ===")
    test_drawdown_manager_has_eventbus_subscription()
    test_hard_stop_triggers_conservative_decisions()
    test_eventbus_event_triggers_hard_stop()
    print("ALL ASSERTIONS PASSED")
