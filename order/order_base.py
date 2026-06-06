"""
order_base.py - OrderService基础定义
提取: OrderResult, BaseService, 状态转换表, 单例工厂
"""
from __future__ import annotations

import threading
import logging
import time
import os
from typing import Any, Callable, Dict, List, Optional
from collections import deque
from dataclasses import dataclass

from ali2026v3_trading.infra.event_bus import RateLimiter
from ali2026v3_trading.order.order_risk_guard import OrderRiskGuard
from ali2026v3_trading.order.order_chase_service import OrderChaseService
from ali2026v3_trading.order.order_state_manager import OrderStateManager
from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService

try:
    from ali2026v3_trading.order.order_persistence import OrderPersistenceService
    _HAS_PERSISTENCE_SERVICE = True
except ImportError:
    _HAS_PERSISTENCE_SERVICE = False
    OrderPersistenceService = None

try:
    from ali2026v3_trading.causal_chain_utils import CyclicDependencyGuard, ContaminationGuard
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False

@dataclass(slots=True)
class OrderResult:
    order_id: Optional[str]
    success: bool
    error_code: str = ''
    error_message: str = ''
    @staticmethod
    def ok(order_id: str) -> 'OrderResult':
        return OrderResult(order_id=order_id, success=True)
    @staticmethod
    def fail(error_code: str, error_message: str) -> 'OrderResult':
        return OrderResult(order_id=None, success=False, error_code=error_code, error_message=error_message)
    def __bool__(self) -> bool:
        return self.success
    def __str__(self) -> str:
        return self.order_id or ''

class BaseService:
    def __init__(self, event_bus=None):
        self.event_bus = event_bus
    def health_check(self) -> bool:
        return True
    def get_service_name(self) -> str:
        return self.__class__.__name__

_VALID_ORDER_TRANSITIONS = {
    'PENDING': {'SUBMITTED', 'CANCELLED', 'FAILED'},
    'SUBMITTED': {'PARTIAL_FILLED', 'FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
    'PARTIAL_FILLED': {'FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
    'PARTIAL': {'FILLED', 'ALL_FILLED', 'PARTIAL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
    'TIMEOUT': {'SUBMITTED', 'CANCELLED', 'FAILED'},
    'FILLED': set(), 'ALL_FILLED': set(), 'CANCELLED': set(),
    'FAILED': {'SUBMITTED'}, 'ORPHANED': {'CANCELLED', 'FAILED'},
    '全成': set(),
    '部分成交': {'FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
}

def _validate_order_status_transition(old_status: str, new_status: str) -> bool:
    if old_status == new_status:
        return True
    allowed = _VALID_ORDER_TRANSITIONS.get(old_status)
    return True if allowed is None else new_status in allowed

_mask_id = lambda s: s[:3] + '***' + s[-3:] if len(str(s)) > 6 else '***'

_order_service_instance: Optional[Any] = None
_order_service_lock = threading.Lock()

def reset_order_service() -> None:
    global _order_service_instance
    with _order_service_lock:
        _order_service_instance = None

def get_order_service() -> Any:
    global _order_service_instance
    if _order_service_instance is None:
        with _order_service_lock:
            if _order_service_instance is None:
                from ali2026v3_trading.order.order_service import OrderService
                _order_service_instance = OrderService()
    if _order_service_instance is None:
        raise RuntimeError("[R23-P1-06-FIX] get_order_service()返回None，单例初始化失败")
    return _order_service_instance

def init_order_service_attrs(svc, params=None):
    """OrderService.__init__属性初始化委托"""
    from ali2026v3_trading.order.order_platform_auth import PlatformAuthenticator
    svc._platform_api_ready = False
    svc.authenticator = PlatformAuthenticator()
    svc._risk_guard = OrderRiskGuard(svc)
    svc._chase_service = OrderChaseService(svc)
    svc._wal_state_service = OrderWALStateService(svc)
    svc._state_manager = OrderStateManager()
    svc._persistence_service = None
    if _HAS_PERSISTENCE_SERVICE:
        try:
            svc._persistence_service = OrderPersistenceService()
        except Exception:
            pass
    _rate = 60
    try:
        from ali2026v3_trading.params_service import get_params_service
        _rate = get_params_service().get_int('rate_limit_global_per_min', 60) or 60
    except Exception:
        pass
    svc.rate_limiter = RateLimiter(rate=max(_rate / 60.0, 1.0))
    svc._orders_by_id = {}
    svc._recent_orders_by_instrument = {}
    svc._chase_tasks = {}
    svc._platform_id_to_order_id = {}
    svc._lock = threading.RLock()
    svc._platform_insert_order = None
    svc._platform_cancel_order = None
    svc._platform_insert_order_params = set()
    svc._order_timeout_seconds = 5.0
    svc._platform_submit_timeout_seconds = 5.0
    svc._cancel_count_window = deque(maxlen=1000)
    svc._order_count_window = deque(maxlen=1000)
    svc._self_trade_bans = {}
    _pget = getattr(params, 'get', lambda *a: None) if params and hasattr(params, 'get') else lambda *a: None
    svc._operation_timeouts = {
        'open': float(_pget('order_open_timeout', 5.0) or 5.0),
        'close': float(_pget('order_close_timeout', 3.0) or 3.0),
        'cancel': float(_pget('order_cancel_timeout', 2.0) or 2.0),
        'query': float(_pget('order_query_timeout', 2.0) or 2.0),
        'default': float(_pget('order_timeout_seconds', 5.0) or 5.0),
    }
    svc._max_chase_retries = 3
    svc._retry_backoff_base = 1.0
    svc._retry_backoff_max = 30.0
    svc._last_retry_time = {}
    svc._stats = {'total_orders': 0, 'successful_orders': 0, 'failed_orders': 0, 'cancelled_orders': 0, 'chase_tasks_active': 0}
    svc._consecutive_failures = 0
    svc._circuit_breaker_threshold = 5
    svc._circuit_breaker_open = False
    svc._circuit_breaker_opened_at = 0.0
    svc._circuit_breaker_auto_recovery_sec = 60.0
    svc._circuit_breaker_half_open = False
    svc._order_state_audit_log = []
    svc._order_last_update_time = {}
    svc._risk_block_until = {}
    svc._risk_block_timeout = 300.0
    svc._history_audit = []
    svc._self_trade_ban_minutes = 30.0
    svc._last_cleanup = time.time()
    svc._cleanup_interval = 300
    svc._MAX_ORDERS_TRACKED = 10000
    svc._get_position_count = None
    svc._wal_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_wal_orders')
    svc._ensure_wal_dir()
    svc._order_idempotent_set = set()
    svc._idempotent_state_file = os.path.join(svc._wal_dir, 'idempotent_state.jsonl')
    svc._idempotent_lock = threading.Lock()
    svc._order_state_file = os.path.join(svc._wal_dir, 'order_state.jsonl')
    svc._order_state_lock = threading.Lock()
    svc._append_state_fail_count = 0
    svc._append_state_fail_critical_threshold = 3
    svc._ensure_wal_dir()
    svc._recover_orphaned_orders()
    svc._recover_idempotent_state()
    svc._recover_order_state()


class OrderQueryMixin:
    """OrderService查询/工具方法Mixin，从Facade提取"""
    _PLATFORM_ATTR_MAP = {'order_id': 'OrderRef', 'status': 'OrderStatus', 'filled_volume': 'VolumeTraded'}

    def bind_position_count_func(self, func): self._get_position_count = func; logging.info("[OrderService] R25-TO-02-FIX: 仓位快照查询回调已绑定")

    @staticmethod
    def _get_platform_attr(obj, *attr_names, default=None):
        for attr in attr_names:
            val = getattr(obj, attr, None)
            if val is not None: return val
        return default

    def _normalize_platform_result(self, result): return {k: self._get_platform_attr(result, k, v) for k, v in self._PLATFORM_ATTR_MAP.items()}
    def get_order_status(self, order_id):
        with self._lock: o = self._orders_by_id.get(order_id); return str(o.get('status', 'UNKNOWN')) if o else 'UNKNOWN'
    def get_order(self, order_id):
        with self._lock: return self._orders_by_id.get(order_id)
    def get_order_by_platform_id(self, platform_order_id):
        with self._lock:
            iid = self._platform_id_to_order_id.get(str(platform_order_id))
            if iid: return self._orders_by_id.get(iid)
            for o in self._orders_by_id.values():
                if o.get('platform_order_id') == platform_order_id: return o
        return None
    def get_orders_by_instrument(self, instrument_id):
        with self._lock: return [o for o in self._orders_by_id.values() if o['instrument_id'] == instrument_id]
    def get_stats(self):
        self._cleanup_orders()
        with self._lock: return {'service_name': 'OrderService', **self._stats, 'active_orders': self._state_manager.pending_count, 'total_tracked': len(self._orders_by_id), 'chase_tasks': len(self._chase_tasks)}
    def _generate_order_id(self): import uuid; return f"ORD_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    def _generate_chase_order_id(self, original_order_id): import uuid; return f"CHASE_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    def get_order_data_age(self, order_id):
        _last = self._order_last_update_time.get(order_id); return (time.time() - _last) if _last is not None else None
    def _get_rate_limit(self):
        try:
            from ali2026v3_trading.params_service import get_params_service
            rate = get_params_service().get_int('rate_limit_global_per_min', 60)
            if rate > 0: return rate
        except Exception: pass
        return 60