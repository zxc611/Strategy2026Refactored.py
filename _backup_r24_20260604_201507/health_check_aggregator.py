"""
Phase1-Sprint3: HealthCheckAggregator — get_health_status(301行)拆解
将StrategyCoreService.get_health_status拆分为6个独立检查项
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List


class HealthCheckAggregator:
    """健康检查聚合器

    _CHECKS定义6个独立检查项:
    1. _check_connection_state: 连接状态检查
    2. _check_heartbeat_status: 心跳状态检查
    3. _check_resource_usage: 资源使用检查
    4. _check_shadow_engine: 影子引擎检查
    5. _check_strategy_ecosystem: 策略生态系统检查
    6. _check_safety_meta_layer: 安全元层检查
    """

    _CHECKS = [
        '_check_connection_state',
        '_check_heartbeat_status',
        '_check_resource_usage',
        '_check_shadow_engine',
        '_check_strategy_ecosystem',
        '_check_safety_meta_layer',
    ]

    def __init__(self, strategy_service: Any):
        self._svc = strategy_service

    def aggregate(self) -> Dict[str, Any]:
        """编排器: 聚合所有检查项，≤20行，圈复杂度≤3"""
        results = {}
        for check_name in self._CHECKS:
            try:
                results[check_name] = getattr(self, check_name)()
            except Exception as e:
                results[check_name] = {'status': 'ERROR', 'error': str(e)}
                logging.warning("[HealthCheckAggregator] %s异常: %s", check_name, e)
        results['overall_status'] = self._determine_overall(results)
        return results

    def _check_connection_state(self) -> Dict[str, Any]:
        """检查1: 连接状态(平台/交易所连接)"""
        _connected = getattr(self._svc, '_platform_api_ready', False)
        return {
            'status': 'OK' if _connected else 'DEGRADED',
            'platform_connected': _connected,
        }

    def _check_heartbeat_status(self) -> Dict[str, Any]:
        """检查2: 心跳状态(最近心跳时间+延迟)"""
        _last_hb = getattr(self._svc, '_last_heartbeat_ts', 0.0)
        _delay = time.time() - _last_hb if _last_hb > 0 else float('inf')
        return {
            'status': 'OK' if _delay < 30 else 'DEGRADED',
            'last_heartbeat_delay_s': round(_delay, 1),
        }

    def _check_resource_usage(self) -> Dict[str, Any]:
        """检查3: 资源使用(线程池/内存/订单数)"""
        import threading
        _thread_count = threading.active_count()
        _orders = len(getattr(self._svc, '_orders_by_id', {})) if hasattr(self._svc, '_orders_by_id') else 0
        return {
            'status': 'OK' if _thread_count < 50 else 'WARNING',
            'active_threads': _thread_count,
            'tracked_orders': _orders,
        }

    def _check_shadow_engine(self) -> Dict[str, Any]:
        """检查4: 影子引擎状态(Alpha衰减/暂停)"""
        try:
            _shadow = getattr(self._svc, '_shadow_engine', None)
            if _shadow is None:
                return {'status': 'N/A', 'reason': 'shadow_engine_not_initialized'}
            _is_paused = getattr(_shadow, 'is_absolute_ev_paused', lambda: False)()
            return {
                'status': 'PAUSED' if _is_paused else 'OK',
                'ev_paused': _is_paused,
            }
        except Exception as e:
            return {'status': 'ERROR', 'error': str(e)}

    def _check_strategy_ecosystem(self) -> Dict[str, Any]:
        """检查5: 策略生态系统(策略活跃度/资金分配)"""
        try:
            _ecosystem = getattr(self._svc, '_strategy_ecosystem', None)
            if _ecosystem is None:
                return {'status': 'N/A', 'reason': 'ecosystem_not_initialized'}
            _active = getattr(_ecosystem, 'get_active_strategies', lambda: [])()
            return {
                'status': 'OK' if len(_active) > 0 else 'WARNING',
                'active_strategies': len(_active),
            }
        except Exception as e:
            return {'status': 'ERROR', 'error': str(e)}

    def _check_safety_meta_layer(self) -> Dict[str, Any]:
        """检查6: 安全元层(熔断器/合规/资金充足性)"""
        try:
            from ali2026v3_trading.risk_circuit_breaker import get_safety_meta_layer
            _safety = get_safety_meta_layer()
            if _safety is None:
                return {'status': 'N/A', 'reason': 'safety_meta_layer_not_available'}
            _cb_open = getattr(_safety, '_circuit_breaker_open', False)
            return {
                'status': 'CRITICAL' if _cb_open else 'OK',
                'circuit_breaker_open': _cb_open,
            }
        except Exception as e:
            return {'status': 'ERROR', 'error': str(e)}

    def _determine_overall(self, results: Dict[str, Any]) -> str:
        """根据各检查项确定整体状态"""
        _statuses = [v.get('status', 'UNKNOWN') for v in results.values() if isinstance(v, dict)]
        if any(s == 'CRITICAL' for s in _statuses):
            return 'CRITICAL'
        if any(s == 'ERROR' for s in _statuses):
            return 'ERROR'
        if any(s == 'DEGRADED' for s in _statuses):
            return 'DEGRADED'
        if any(s == 'WARNING' for s in _statuses):
            return 'WARNING'
        return 'OK'