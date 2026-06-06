"""
StrategyCoreService 监控层 — 从strategy_core_service.py拆分
职责: 健康检查、紧急停止、日回撤硬停止、诊断任务
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional


class StrategyMonitoringLayer:
    _AUTO_CALLER_IDS = frozenset({'timer', 'scheduler', 'auto', 'cron', 'periodic', 'heartbeat'})

    def __init__(self, provider):
        self._provider = provider

    def get_health_status(self) -> Dict[str, Any]:
        from ali2026v3_trading.health_check_aggregator import HealthCheckAggregator
        _aggregator = HealthCheckAggregator(self._provider)
        return _aggregator.aggregate()

    def confirm_daily_resume(self, caller_id: str = "unknown") -> bool:
        if caller_id.lower() in self._AUTO_CALLER_IDS:
            logging.warning(
                "[R10-P1-11] confirm_daily_resume被自动来源'%s'调用，已拒绝。"
                "手册9.6节要求仅通过人工确认恢复，换日不自动重置。", caller_id)
            return False
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self._provider, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(
                params=getattr(self._provider, '_params', None),
                strategy_id=_sid
            )
            if safety:
                result = safety.confirm_daily_resume(caller_id=caller_id)
                if result:
                    logging.critical("[StrategyMonitoringLayer] 人工确认恢复交易：日回撤硬停止已解除 caller_id=%s", caller_id)
                    self._provider._health_pause_new_open = False
                return result
            else:
                logging.error("[StrategyMonitoringLayer] confirm_daily_resume失败: SafetyMetaLayer不可用")
                return False
        except Exception as e:
            logging.error("[StrategyMonitoringLayer] confirm_daily_resume异常: %s", e)
            return False

    def is_hard_stop_triggered(self) -> bool:
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self._provider, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(
                params=getattr(self._provider, '_params', None),
                strategy_id=_sid
            )
            if safety:
                return safety.is_hard_stop_triggered()
            return False
        except Exception as e:
            logging.warning("[StrategyMonitoringLayer] is_hard_stop_triggered error: %s", e)
            return False

    def emergency_stop(self, caller_id: str = "unknown", reason: str = "") -> Dict[str, Any]:
        result = {
            'paused': False,
            'orders_cancelled': 0,
            'positions_closed': 0,
            'caller_id': caller_id,
            'reason': reason,
        }
        try:
            provider = self._provider
            if hasattr(provider, 'pause'):
                provider.pause()
                result['paused'] = True
            order_svc = getattr(provider, '_order_service', None)
            if order_svc and hasattr(order_svc, 'cancel_all_orders'):
                try:
                    cancelled = order_svc.cancel_all_orders()
                    result['orders_cancelled'] = cancelled if isinstance(cancelled, int) else 0
                except Exception:
                    pass
            pos_svc = getattr(provider, '_position_service', None)
            if pos_svc and hasattr(pos_svc, 'close_all_positions'):
                try:
                    closed = pos_svc.close_all_positions()
                    result['positions_closed'] = closed if isinstance(closed, int) else 0
                except Exception:
                    pass
            logging.critical("[OPS-03] emergency_stop执行完成: %s", result)
        except Exception as e:
            logging.error("[StrategyMonitoringLayer] emergency_stop异常: %s", e)
        return result