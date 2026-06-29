"""
event_publisher.py - EventPublisher
Phase 2 (CC-02+CC-04): 从_LifecycleMixin提取的事件发布Manager

职责：
- 发布策略事件（StrategyInitialized/Started/Stopped等）

Provider接口：
- provider._event_bus: 事件总线
- provider.strategy_id: 策略ID
"""

import logging
from typing import Any, Dict, Optional


class EventPublisher:
    """事件发布Manager"""

    def __init__(self, provider: Any = None):
        self._provider = provider

    def publish(self, event_type: str, data: Dict[str, Any],
                event_bus: Any = None, strategy_id: str = '') -> None:
        """发布事件"""
        run_id = 'N/A'
        if self._provider:
            run_id = getattr(self._provider, '_lifecycle_run_id', 'N/A')
            if not strategy_id:
                strategy_id = getattr(self._provider, 'strategy_id', '')
        bus = event_bus
        if bus is None and self._provider:
            bus = getattr(self._provider, '_event_bus', None)
        if bus:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'strategy_id': strategy_id,
                    'run_id': run_id,
                    'source_type': 'event-tail',
                    **data
                })()
                bus.publish(event, async_mode=True)
            except (AttributeError, RuntimeError, ValueError, TypeError, KeyError) as e:
                logging.debug(
                    "[EventPublisher][strategy_id=%s][run_id=%s] "
                    "Failed to publish %s: %s",
                    strategy_id, run_id, event_type, e
                )
