"""
health_check_manager.py - HealthCheckManager
Phase 2 (CC-02+CC-04): 从_LifecycleMixin提取的健康检查Manager

职责：
- 策略健康检查（状态、错误率、E2E计数器）

Provider接口：
- provider._state: 策略状态
- provider._stats: 性能统计
- provider._e2e_counters: 端到端计数器
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState, _state_is
from ali2026v3_trading.shared_utils import CHINA_TZ


class HealthCheckManager:
    """健康检查Manager"""

    def __init__(self, provider: Any = None):
        self._provider = provider

    def health_check(self, state: Any = None, stats: Optional[Dict] = None,
                     e2e_counters: Optional[Dict] = None,
                     e2e_shard_enqueued: Optional[Dict] = None,
                     e2e_shard_persisted: Optional[Dict] = None,
                     strategy_id: str = '') -> Dict[str, Any]:
        """执行健康检查"""
        issues = []
        status = 'healthy'

        if state is None and self._provider:
            state = getattr(self._provider, '_state', None)
        if stats is None and self._provider:
            stats = getattr(self._provider, '_stats', {})
        if e2e_counters is None and self._provider:
            e2e_counters = getattr(self._provider, '_e2e_counters', {})
        if e2e_shard_enqueued is None and self._provider:
            e2e_shard_enqueued = getattr(self._provider, '_e2e_shard_enqueued', {})
        if e2e_shard_persisted is None and self._provider:
            e2e_shard_persisted = getattr(self._provider, '_e2e_shard_persisted', {})
        if not strategy_id and self._provider:
            strategy_id = getattr(self._provider, 'strategy_id', '')

        if _state_is(state, StrategyState.ERROR):
            issues.append("Strategy in ERROR state")
            status = 'unhealthy'

        if _state_is(state, StrategyState.DEGRADED):
            issues.append("Strategy in DEGRADED state: API未就绪或订阅部分失败")
            status = 'degraded' if status == 'healthy' else status

        uptime = 0.0
        if stats and stats.get('start_time'):
            uptime = (datetime.now(CHINA_TZ) - stats['start_time']).total_seconds()

        if uptime > 60 and stats:
            error_rate = stats.get('errors_count', 0) / uptime
            if error_rate > 0.1:
                issues.append(f"High error rate: {error_rate*60:.2f}/min")
                status = 'warning' if status == 'healthy' else 'unhealthy'

        if e2e_counters and uptime > 30 and e2e_counters.get('configured_instruments', 0) > 0:
            if e2e_counters.get('first_tick_received', 0) == 0:
                issues.append("E2E: 运行30秒后仍未收到任何Tick")
                status = 'warning' if status == 'healthy' else status
            if e2e_counters.get('preregistered_instruments', 0) == 0:
                issues.append("E2E: 未完成任何合约预注册")
                status = 'warning' if status == 'healthy' else status
            if e2e_counters.get('kline_persisted_count', 0) == 0 and uptime > 60:
                issues.append("E2E: 运行60秒后仍无K线数据落盘")
                status = 'warning' if status == 'healthy' else status

        result = {
            'status': status,
            'issues': issues,
            'strategy_id': strategy_id,
            'uptime_seconds': uptime,
            'e2e_counters': e2e_counters or {},
            'timestamp': datetime.now(CHINA_TZ).isoformat()
        }
        if e2e_shard_enqueued is not None:
            result['e2e_shard_enqueued'] = dict(e2e_shard_enqueued)
        if e2e_shard_persisted is not None:
            result['e2e_shard_persisted'] = dict(e2e_shard_persisted)
        if state and hasattr(state, 'value'):
            result['state'] = state.value

        return result
