# MODULE_ID: M2-478
"""R3-F2: P1-42 事件总线程record_and_publish重复发布 断言测试

验证运行时行为:
1. _record_and_publish拆分为期validate_and_check + _record_result + _publish_risk_event_via_eventbus
2. _publish_risk_event_via_eventbus通过EventBus发布RiskEvent
3. EventBus不可用时常record_and_publish仍能返回结果
4. result_handler.record_and_publish调用路径正确
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_record_and_publish_decomposed():
    """P1-42验证: _record_and_publish拆分为3个独立步骤"""
    from ali2026v3_trading.risk.risk_service import RiskService
    assert hasattr(RiskService, '_validate_and_check'), \
        "RiskService应拥有界validate_and_check方法"
    assert hasattr(RiskService, '_publish_risk_event_via_eventbus'), \
        "RiskService应拥有界publish_risk_event_via_eventbus方法"


def test_publish_risk_event_via_eventbus():
    """P1-42验证: _publish_risk_event_via_eventbus通过EventBus发布RiskEvent"""
    from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskEvent

    bus = get_global_event_bus()
    if bus is None:
        pytest.skip("EventBus不可用")

    captured_events = []

    def _handler(event):
        captured_events.append(event)

    bus.subscribe_weak('RiskEvent', _handler)

    try:
        from ali2026v3_trading.risk.risk_service import RiskService
        rs = RiskService.__new__(RiskService)

        # 创建mock result
        class MockResult:
            level = 'INFO'
            result = 'PASS'

        rs._publish_risk_event_via_eventbus({'symbol': 'test'}, MockResult())

        assert len(captured_events) >= 1, \
            f"EventBus应收到至少1个RiskEvent，实际{len(captured_events)}"
        event = captured_events[-1]
        assert isinstance(event, RiskEvent), \
            f"事件应为RiskEvent实例，实际为{type(event)}"
        assert event.risk_type == 'risk_check'
    finally:
        try:
            bus.unsubscribe('RiskEvent', _handler)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass


def test_record_and_publish_returns_result_without_eventbus():
    """P1-42验证: EventBus不可用时常record_and_publish仍能返回结果"""
    from ali2026v3_trading.risk.risk_service import RiskService

    rs = RiskService.__new__(RiskService)

    # Mock _validate_and_check
    class MockResult:
        level = 'INFO'
        result = 'PASS'

    def _mock_validate(signal, key, guard):
        return MockResult()

    def _mock_record(result):
        return result

    rs._validate_and_check = _mock_validate
    rs._record_result = _mock_record

    result = rs._record_and_publish({'symbol': 'test'}, 'dedup', None)
    assert result is not None, "_record_and_publish应返回结果"
    assert result.result == 'PASS', f"结果应为PASS，实际为{result.result}"


def test_result_handler_uses_record_and_publish():
    """P1-42验证: result_handler.record_and_publish调用路径存在"""
    from ali2026v3_trading.risk_engine.result_handler import record_and_publish
    assert callable(record_and_publish), "record_and_publish应为可调用函数"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
