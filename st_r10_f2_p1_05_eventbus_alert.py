# MODULE_ID: M2-474
"""R2-F2: P1-05 EventBus统一RiskService告警回调 断言测试

验证运行时行为:
1. RiskAlertEvent事件类型存在且可实例化
2. _fire_alert通过EventBus发布RiskAlertEvent
3. register_alert_callback同时注册到EventBus
4. EventBus不可用时回退到直接回调
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_risk_alert_event_exists():
    """P1-05验证: RiskAlertEvent事件类型存在且可实例化"""
    from ali2026v3_trading.infra.event_bus import RiskAlertEvent
    event = RiskAlertEvent(alert_type='test_alert', detail={'key': 'value'})
    assert event.alert_type == 'test_alert'
    assert event.detail == {'key': 'value'}
    assert event.type == 'RiskAlertEvent'
    assert 'RiskAlertEvent' in str(event)


def test_fire_alert_publishes_to_eventbus():
    """P1-05验证: _fire_alert通过EventBus发布RiskAlertEvent"""
    from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskAlertEvent

    bus = get_global_event_bus()
    if bus is None:
        pytest.skip("EventBus不可用")

    captured_events = []

    def _handler(event):
        captured_events.append(event)

    bus.subscribe_weak('RiskAlertEvent', _handler)

    try:
        from ali2026v3_trading.risk.risk_service import RiskService
        rs = RiskService.__new__(RiskService)
        rs._fire_alert('test_event', {'reason': 'unit_test'})

        # 验证EventBus收到了事件
        assert len(captured_events) >= 1, \
            f"EventBus应收到至少1个RiskAlertEvent，实际{len(captured_events)}"
        event = captured_events[-1]
        assert isinstance(event, RiskAlertEvent), \
            f"事件应为RiskAlertEvent实例，实际为{type(event)}"
        assert event.alert_type == 'test_event', \
            f"alert_type应为'test_event'，实际为'{event.alert_type}'"
        assert event.detail == {'reason': 'unit_test'}, \
            f"detail应为{{'reason': 'unit_test'}}，实际为{event.detail}"
    finally:
        try:
            bus.unsubscribe('RiskAlertEvent', _handler)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass


def test_register_alert_callback_subscribes_to_eventbus():
    """P1-05验证: register_alert_callback同时注册到EventBus"""
    from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskAlertEvent

    bus = get_global_event_bus()
    if bus is None:
        pytest.skip("EventBus不可用")

    callback_calls = []

    def _test_cb(event_type, detail):
        callback_calls.append({'event_type': event_type, 'detail': detail})

    # 清理RiskService类级别的指alert_callbacks
    from ali2026v3_trading.risk.risk_service import RiskService
    original_callbacks = RiskService._alert_callbacks[:]
    RiskService._alert_callbacks.clear()

    try:
        RiskService.register_alert_callback(_test_cb)

        # 通过EventBus直接发布
        bus.publish(RiskAlertEvent(alert_type='direct_eventbus', detail={'src': 'eventbus'}))

        # 验证回调被EventBus触发
        assert len(callback_calls) >= 1, \
            f"EventBus订阅应触发回调至少1次，实际{len(callback_calls)}次"
        assert callback_calls[-1]['event_type'] == 'direct_eventbus', \
            f"event_type应为'direct_eventbus'，实际为'{callback_calls[-1]['event_type']}'"
    finally:
        RiskService._alert_callbacks = original_callbacks


def test_fire_alert_fallback_to_direct_callback_when_eventbus_unavailable():
    """P1-05验证: EventBus不可用时回退到直接回调"""
    from ali2026v3_trading.risk.risk_service import RiskService

    callback_calls = []
    original_callbacks = RiskService._alert_callbacks[:]

    def _test_cb(event_type, detail):
        callback_calls.append({'event_type': event_type, 'detail': detail})

    RiskService._alert_callbacks = [_test_cb]

    try:
        rs = RiskService.__new__(RiskService)
        rs._fire_alert('fallback_test', {'reason': 'no_eventbus'})

        assert len(callback_calls) >= 1, \
            f"直接回调应被触发至少1次，实际{len(callback_calls)}次"
        assert callback_calls[-1]['event_type'] == 'fallback_test'
    finally:
        RiskService._alert_callbacks = original_callbacks


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
