"""
订单服务模块 - CQRS 架构 Command 层
来源：09_executor.py + 10_gate.py (部分)
功能：订单执行 + 平台认证 + 限流控制 + 撤单追单
"""
from __future__ import annotations

import threading
import logging
import time
import math
import json
import os
import hmac
import hashlib
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from collections import deque

from ali2026v3_trading.event_bus import RateLimiter
from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.shared_utils import compute_slippage_bps
from ali2026v3_trading.shared_utils import TradeAction, TradeDirection, VALID_TRADE_ACTIONS, VALID_TRADE_DIRECTIONS
from ali2026v3_trading.serialization_utils import json_dumps, safe_jsonl_append_line
from ali2026v3_trading.resilience_utils import is_disk_full_error
from ali2026v3_trading.order_risk_guard import OrderRiskGuard
from ali2026v3_trading.order_chase_service import OrderChaseService
from ali2026v3_trading.order_wal_state_service import OrderWALStateService

from dataclasses import dataclass

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

try:
    from ali2026v3_trading.causal_chain_utils import (
        CyclicDependencyGuard, ContaminationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False

_mask_id = lambda s: s[:3] + '***' + s[-3:] if len(str(s)) > 6 else '***'


_VALID_ORDER_TRANSITIONS = {
    'PENDING': {'SUBMITTED', 'CANCELLED', 'FAILED'},
    'SUBMITTED': {'PARTIAL_FILLED', 'FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
    'PARTIAL_FILLED': {'FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
    'PARTIAL': {'FILLED', 'ALL_FILLED', 'PARTIAL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
    'TIMEOUT': {'SUBMITTED', 'CANCELLED', 'FAILED'},
    'FILLED': set(),
    'ALL_FILLED': set(),
    'CANCELLED': set(),
    'FAILED': {'SUBMITTED'},
    'ORPHANED': {'CANCELLED', 'FAILED'},
    '全成': set(),
    '部分成交': {'FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', 'TIMEOUT'},
}


def _validate_order_status_transition(old_status: str, new_status: str) -> bool:
    if old_status == new_status:
        return True
    allowed = _VALID_ORDER_TRANSITIONS.get(old_status)
    if allowed is None:
        return True
    return new_status in allowed


class BaseService:
    def __init__(self, event_bus=None):
        self.event_bus = event_bus

    def health_check(self) -> bool:
        return True

    def get_service_name(self) -> str:
        return self.__class__.__name__



_order_service_instance: Optional['OrderService'] = None
_order_service_lock = threading.Lock()


def reset_order_service() -> None:
    """R27-CP-03-FIX: 重置OrderService单例，用于测试隔离"""
    global _order_service_instance
    with _order_service_lock:
        _order_service_instance = None


def get_order_service() -> 'OrderService':
    global _order_service_instance
    if _order_service_instance is None:
        with _order_service_lock:
            if _order_service_instance is None:
                _order_service_instance = OrderService()
    # [R23-P1-06-FIX] 工厂函数返回None防护
    if _order_service_instance is None:
        raise RuntimeError("[R23-P1-06-FIX] get_order_service()返回None，单例初始化失败")
    return _order_service_instance


class OrderService(BaseService):
    def __init__(self, event_bus=None, params=None):  # R27-P0-FIX: 添加params参数
        super().__init__(event_bus)
        # R23-IN-P1-09-FIX: 平台API就绪守卫
        self._platform_api_ready: bool = False
        self.authenticator = PlatformAuthenticator()
        self._risk_guard = OrderRiskGuard(self)
        self._chase_service = OrderChaseService(self)
        self._wal_state_service = OrderWALStateService(self)
        rate_per_min = self._get_rate_limit()
        self.rate_limiter = RateLimiter(rate=max(rate_per_min / 60.0, 1.0))
        self._orders_by_id: Dict[str, Dict] = {}
        self._recent_orders_by_instrument: Dict[str, List[tuple]] = {}
        self._chase_tasks: Dict[str, Dict] = {}
        self._platform_id_to_order_id: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._platform_insert_order = None
        self._platform_cancel_order = None
        self._platform_insert_order_params = set()
        self._order_timeout_seconds = 5.0
        self._platform_submit_timeout_seconds = 5.0
        # R27-P0-FIX: 添加撤单/下单计数窗口，供SafetyMetaLayer算法熔断检查使用
        self._cancel_count_window: deque = deque(maxlen=1000)
        self._order_count_window: deque = deque(maxlen=1000)
        self._self_trade_bans: Dict[str, float] = {}  # R27-P0-FIX: 自成交禁止记录
        # P1-12修复: 按操作类型差异化超时
        self._operation_timeouts = {
            'open': float(getattr(params, 'get', lambda *a: 5.0)('order_open_timeout', 5.0)) if params and hasattr(params, 'get') else 5.0,
            'close': float(getattr(params, 'get', lambda *a: 3.0)('order_close_timeout', 3.0)) if params and hasattr(params, 'get') else 3.0,
            'cancel': float(getattr(params, 'get', lambda *a: 2.0)('order_cancel_timeout', 2.0)) if params and hasattr(params, 'get') else 2.0,
            'query': float(getattr(params, 'get', lambda *a: 2.0)('order_query_timeout', 2.0)) if params and hasattr(params, 'get') else 2.0,
            'default': float(getattr(params, 'get', lambda *a: 5.0)('order_timeout_seconds', 5.0)) if params and hasattr(params, 'get') else 5.0,
        }
        self._max_chase_retries = 3
        # [ID-P1-08-FIX] 指数退避: 重试间隔基数和上限
        self._retry_backoff_base = 1.0
        self._retry_backoff_max = 30.0
        self._last_retry_time: Dict[str, float] = {}
        self._stats = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'cancelled_orders': 0,
            'chase_tasks_active': 0,
        }
        self._consecutive_failures: int = 0
        self._circuit_breaker_threshold: int = 5
        self._circuit_breaker_open: bool = False
        self._circuit_breaker_opened_at: float = 0.0
        self._circuit_breaker_auto_recovery_sec: float = 60.0
        self._circuit_breaker_half_open: bool = False
        # [R23-P2-SM-05-FIX] 订单状态变更审计记录
        self._order_state_audit_log: List[Dict[str, Any]] = []
        # [R23-P2-FR-08-FIX] 订单数据年龄监控
        self._order_last_update_time: Dict[str, float] = {}
        # R24-P1-CF-03修复: 跨策略风控异常自动恢复 — 阻断超时后自动解除
        self._risk_block_until: Dict[str, float] = {}
        self._risk_block_timeout = 300.0  # 风控阻断5分钟后自动恢复
        # R24-P1-TR-09修复: 订单清理前保存审计关键字段
        self._history_audit: List[Dict[str, Any]] = []
        self._self_trade_bans: Dict[str, float] = {}
        self._self_trade_ban_minutes: float = 30.0
        # 定期清理已完成订单防止内存泄漏
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5分钟
        # R26-P2-MEM-01修复: 订单追踪容量上限，防止高频场景内存无限增长
        self._MAX_ORDERS_TRACKED = 10000
        # [R25-TO-02-FIX] TOCTOU仓位快照验证回调
        self._get_position_count: Optional[Callable[[str], int]] = None
        # [R25-TO-03-FIX] WAL预写日志路径与记录
        self._wal_dir: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_wal_orders')
        self._ensure_wal_dir()
        # R16-P0-RES-02修复: 幂等去重集合+JSONL持久化
        self._order_idempotent_set: set = set()
        self._idempotent_state_file: str = os.path.join(self._wal_dir, 'idempotent_state.jsonl')
        self._idempotent_lock = threading.Lock()
        # R16-P0-RES-05修复: 订单状态JSONL追加写
        self._order_state_file: str = os.path.join(self._wal_dir, 'order_state.jsonl')
        self._order_state_lock = threading.Lock()
        # R33-P1-10: 订单状态追加写连续失败计数
        self._append_state_fail_count: int = 0
        self._append_state_fail_critical_threshold: int = 3
        self._ensure_wal_dir()
        self._recover_orphaned_orders()
        self._recover_idempotent_state()
        self._recover_order_state()

    _PLATFORM_IDEMPOTENCY_FIELDS = (
        'client_order_id',
        'request_id',
        'order_ref',
        'order_id',
    )

    def bind_platform_apis(self, insert_order_func, cancel_order_func):
        self._platform_insert_order = insert_order_func
        self._platform_cancel_order = cancel_order_func
        self._platform_insert_order_params = set()
        if insert_order_func and callable(insert_order_func):
            # R23-IN-P1-09-FIX: 标记平台API已就绪
            self._platform_api_ready = True
            try:
                import inspect
                sig = inspect.signature(insert_order_func)
                has_var_keyword = any(
                    p.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
                    for p in sig.parameters.values()
                )
                if has_var_keyword:
                    self._platform_insert_order_params = set()
                    logging.info("[OrderService] 平台下单API含*args/**kwargs，跳过参数过滤: %s", list(sig.parameters.keys()))
                else:
                    self._platform_insert_order_params = set(sig.parameters.keys())
                    logging.info("[OrderService] 平台下单API参数签名: %s", list(sig.parameters.keys()))
            except Exception as e:
                logging.warning("[OrderService] 无法检测平台API签名: %s", e)
        logging.info("[OrderService] 平台下单/撤单API已绑定")

    # [R25-TO-02-FIX] 绑定仓位查询回调，用于TOCTOU二次验证
    def bind_position_count_func(self, func: Callable[[str], int]) -> None:
        self._get_position_count = func
        logging.info("[OrderService] R25-TO-02-FIX: 仓位快照查询回调已绑定")

    _PLATFORM_ATTR_MAP = {
        'order_id': 'OrderRef',
        'status': 'OrderStatus',
        'filled_volume': 'VolumeTraded',
    }

    @staticmethod
    def _get_platform_attr(obj: Any, *attr_names: str, default: Any = None) -> Any:
        for attr in attr_names:
            val = getattr(obj, attr, None)
            if val is not None:
                return val
        return default

    def _normalize_platform_result(self, result: Any) -> Dict[str, Any]:
        normalized = {}
        for new_attr, old_attr in self._PLATFORM_ATTR_MAP.items():
            normalized[new_attr] = self._get_platform_attr(result, new_attr, old_attr, default=None)
        return normalized

    def _build_platform_insert_params(
        self,
        *,
        order_id: str,
        instrument_id: str,
        exchange: str,
        volume: float,
        price: float,
        direction: str,
        action: str,
    ) -> Dict[str, Any]:
        all_params = {
            'exchange': exchange,
            'instrument_id': instrument_id,
            'volume': int(volume),
            'price': price,
            'direction': direction,
            'action': action,
            # R27-P0-DR-02修复: 传递稳定幂等键，避免超时/抖动重试生成新的平台单
            'client_order_id': order_id,
            'request_id': order_id,
            'order_ref': order_id,
            'order_id': order_id,
        }
        param_name_map = {
            'direction': 'order_direction',
            'action': 'order_type',
        }
        if self._platform_insert_order_params:
            mapped_params = {}
            for key, value in all_params.items():
                target_key = param_name_map.get(key, key)
                if target_key in self._platform_insert_order_params:
                    mapped_params[target_key] = value
            return mapped_params
        return all_params

    def _invoke_platform_insert_with_timeout(self, filtered_params: Dict[str, Any]) -> Any:
        result_holder: Dict[str, Any] = {}
        error_holder: Dict[str, BaseException] = {}
        done = threading.Event()

        def _target() -> None:
            try:
                result_holder['result'] = self._platform_insert_order(**filtered_params)
            except BaseException as exc:
                error_holder['error'] = exc
            finally:
                done.set()

        worker = threading.Thread(
            target=_target,
            name='OrderServicePlatformInsert',
            daemon=True,
        )
        worker.start()
        timeout_seconds = max(float(self._platform_submit_timeout_seconds), 0.01)
        if not done.wait(timeout_seconds):
            raise TimeoutError(f'platform insert timeout after {timeout_seconds:.2f}s')
        if 'error' in error_holder:
            raise error_holder['error']
        return result_holder.get('result')

    # R33-P1-12: 撤单API超时保护（与_invoke_platform_insert_with_timeout对齐）
    def _invoke_platform_cancel_with_timeout(self, platform_id: str) -> None:
        """撤单API带超时保护，防止撤单调用阻塞"""
        error_holder: Dict[str, BaseException] = {}
        done = threading.Event()

        def _target() -> None:
            try:
                self._platform_cancel_order(platform_id)
            except BaseException as exc:
                error_holder['error'] = exc
            finally:
                done.set()

        worker = threading.Thread(
            target=_target,
            name='OrderServicePlatformCancel',
            daemon=True,
        )
        worker.start()
        cancel_timeout = self._operation_timeouts.get('cancel', 2.0)
        if not done.wait(cancel_timeout):
            raise TimeoutError(f'[R33-P1-12] platform cancel timeout after {cancel_timeout:.2f}s, platform_id={platform_id}')
        if 'error' in error_holder:
            raise error_holder['error']

    def send_order(
        self,
        instrument_id: str,
        volume: float,
        price: float,
        direction: str = 'BUY',
        action: str = 'OPEN',
        exchange: str = '',
        priority: str = 'NORMAL',
        is_chase: bool = False,
        signal_id: str = '',
        expected_position_count: int = -2,  # R26-P0-TO-04修复: 默认-2(自动获取), -1=禁用, >=0=显式指定
        open_reason: str = '',
        decision_score: float = 0.0,
        position_scale: float = 1.0,
        decision_action: str = '',
        dimension_scores: Optional[Dict[str, float]] = None,
        dimension_weights: Optional[Dict[str, float]] = None,
    ) -> OrderResult:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_PIPELINED_SEND_ORDER'):
            from ali2026v3_trading.order_executor import OrderExecutor, OrderContext
            _executor = OrderExecutor(self)
            _ctx = OrderContext(
                instrument_id=instrument_id, volume=volume, price=price,
                direction=direction, action=action, exchange=exchange,
                priority=priority, is_chase=is_chase, signal_id=signal_id,
                expected_position_count=expected_position_count,
                open_reason=open_reason, decision_score=decision_score,
                position_scale=position_scale, decision_action=decision_action,
                dimension_scores=dimension_scores, dimension_weights=dimension_weights,
            )
            return _executor.execute(_ctx)
        from ali2026v3_trading.order_executor import OrderExecutor, OrderContext
        _executor = OrderExecutor(self)
        _ctx = OrderContext(
            instrument_id=instrument_id, volume=volume, price=price,
            direction=direction, action=action, exchange=exchange,
            priority=priority, is_chase=is_chase, signal_id=signal_id,
            expected_position_count=expected_position_count,
            open_reason=open_reason, decision_score=decision_score,
            position_scale=position_scale, decision_action=decision_action,
            dimension_scores=dimension_scores, dimension_weights=dimension_weights,
        )
        return _executor.execute(_ctx)

    def _compute_pnl_correlation(self, pnl_series_a, pnl_series_b) -> float:
        return self._risk_guard.compute_pnl_correlation(pnl_series_a, pnl_series_b)

    def _check_cross_strategy_risk(self) -> float:
        return self._risk_guard.check_cross_strategy_risk()

    def send_order_split(
        self,
        instrument_id: str,
        volume: float,
        price: float,
        direction: str = 'BUY',
        action: str = 'OPEN',
        exchange: str = '',
        signal_strength: float = 1.0,
        bids: Optional[List] = None,
        asks: Optional[List] = None,
        open_reason: str = '',
        signal_id: str = '',
    ) -> List[str]:
        if direction not in VALID_TRADE_DIRECTIONS:
            logging.error("[OrderService] R24-P0-IV-05: send_order_split direction必须是BUY/SELL, 实际=%s, 订单被拒: %s", direction, instrument_id)
            return []
        if not signal_id:
            logging.warning("[OrderService] R24-P1-TR-08: send_order_split signal_id为空, 订单无法追溯到信号: %s %s", instrument_id, direction)
        split_threshold = getattr(self, '_split_volume_threshold', 5)
        if volume > split_threshold and (bids or asks):
            split_orders = self._plan_volume_split(volume, price, direction, bids, asks, signal_strength)
            if split_orders:
                violations = check_self_trade_across_splits(split_orders)
                if violations:
                    logging.warning("[OrderService] SOS-FAKE-01修复: 拆单自成交检测发现%d个违规, 降级为单笔下单", len(violations))
                else:
                    executed_ids = []
                    for i, sub in enumerate(split_orders):
                        result = self.send_order(
                            instrument_id=instrument_id,
                            volume=sub['volume'],
                            price=sub['price'],
                            direction=direction,
                            action=action,
                            exchange=exchange,
                            signal_id=f"{signal_id}_split{i}" if signal_id else '',
                            open_reason=open_reason,
                        )
                        if result and result.order_id:
                            executed_ids.append(result.order_id)
                        else:
                            logging.warning("[OrderService] SOS-FAKE-01修复: 拆单第%d笔失败, 已执行%d笔", i + 1, len(executed_ids))
                            break
                    if executed_ids:
                        logging.info("[OrderService] SOS-FAKE-01修复: 拆单执行完成 %d笔/%d笔 instrument=%s", len(executed_ids), len(split_orders), instrument_id)
                        return executed_ids
                    logging.warning("[OrderService] SOS-FAKE-01修复: 拆单全部失败, 降级为单笔下单")
        order_id = self.send_order(
            instrument_id=instrument_id,
            volume=volume,
            price=price,
            direction=direction,
            action=action,
            exchange=exchange,
            signal_id=signal_id,
            open_reason=open_reason,
        )
        if order_id:
            return [order_id.order_id]
        return []

    def _plan_volume_split(
        self,
        volume: float,
        price: float,
        direction: str,
        bids: Optional[List],
        asks: Optional[List],
        signal_strength: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """SOS-FAKE-01修复: 基于盘口深度的实际拆单规划

        根据bid/ask盘口深度将大单拆分为多个小单，减少市场冲击
        """
        try:
            lob = asks if direction == TradeDirection.BUY else bids
            if not lob or len(lob) == 0:
                return []
            remaining = volume
            splits = []
            for level in lob:
                level_price = level[0] if isinstance(level, (list, tuple)) and len(level) >= 2 else price
                level_vol = level[1] if isinstance(level, (list, tuple)) and len(level) >= 2 else 0
                if not isinstance(level_price, (int, float)) or not isinstance(level_vol, (int, float)):
                    continue
                if remaining <= 0:
                    break
                take_vol = min(remaining, level_vol * signal_strength)
                if take_vol >= 1:
                    splits.append({'price': level_price, 'volume': int(take_vol), 'direction': direction})
                    remaining -= int(take_vol)
            if remaining > 0 and splits:
                splits[-1]['volume'] += int(remaining)
            elif remaining > 0:
                splits.append({'price': price, 'volume': int(remaining), 'direction': direction})
            return splits if len(splits) > 1 else []
        except Exception as e:
            logging.error("[OrderService] SOS-FAKE-01修复: 拆单规划异常: %s", e)
            return []

    def execute_by_ranking(self, targets: List[Dict[str, Any]], direction: str = 'BUY', action: str = 'OPEN') -> List[str]:
        if not targets:
            return []
        results = []
        for target in targets:
            instrument_id = target.get('instrument_id', '')
            volume = target.get('lots', 1)
            price = target.get('price', 0)
            target_direction = target.get('direction', direction)
            target_action = target.get('action', action)
            if not instrument_id or price <= 0:
                continue
            tick_size = self._get_tick_size(instrument_id)
            if target_direction == TradeDirection.BUY:
                price = self._correct_price(price + tick_size, instrument_id)
            elif target_direction == TradeDirection.SELL:
                price = self._correct_price(max(0.01, price - tick_size), instrument_id)
            order_id = self.send_order(
                instrument_id=instrument_id,
                volume=volume,
                price=price,
                direction=target_direction,
                action=target_action,
                signal_id=target.get('signal_id', ''),
                open_reason=target.get('open_reason', ''),
                decision_score=target.get('decision_score', 0.0),
                position_scale=target.get('position_scale', 1.0),
                decision_action=target.get('decision_action', ''),
                dimension_scores=target.get('dimension_scores'),
                dimension_weights=target.get('dimension_weights'),
            )
            if order_id:
                results.append(order_id.order_id)
            else:
                logging.warning("[OrderService] execute_by_ranking 下单失败: %s, error=%s", instrument_id, order_id.error_code if order_id else 'unknown')
        return results

    def cancel_order(self, order_id: str) -> bool:
        return self._chase_service.cancel_order(order_id)

    def cancel_all_pending(self) -> int:
        return self._chase_service.cancel_all_pending()

    def emergency_close_all_positions(self, caller_id: str = "unknown") -> Dict[str, Any]:
        return self._chase_service.emergency_close_all_positions(caller_id)

    def on_trade_update(self, trade_data: Any) -> None:
        try:
            normalized = self._normalize_platform_result(trade_data)
            oid = normalized.get('order_id', '')
            sts = normalized.get('status', '')
            filled = normalized.get('filled_volume', 0)
            with self._lock:
                order = self._orders_by_id.get(oid)
                if not order:
                    mapped_id = self._platform_id_to_order_id.get(oid)
                    if mapped_id:
                        order = self._orders_by_id.get(mapped_id)
                    if not order:
                        for o in self._orders_by_id.values():
                            if o.get('platform_order_id') == oid:
                                order = o
                                break
                if order:
                    old_sts = order.get('status', '')
                    if not _validate_order_status_transition(old_sts, sts):
                        logging.warning(
                            "[R14-P0-BIZ-08] 非法订单状态转换: order_id=%s %s->%s, 拒绝更新",
                            oid, old_sts, sts,
                        )
                        return
                    order['status'] = sts
                    order['filled_volume'] = filled
                    order['updated_at'] = datetime.now(CHINA_TZ)
                    # [R23-P2-SM-05-FIX] 订单状态变更审计
                    self._order_state_audit_log.append({
                        'timestamp': time.time(), 'order_id': oid,
                        'old_status': order.get('status', ''), 'new_status': sts,
                    })
                    if len(self._order_state_audit_log) > 1000:
                        self._order_state_audit_log = self._order_state_audit_log[-500:]
                    # [R23-P2-FR-08-FIX] 更新订单数据年龄
                    self._order_last_update_time[oid] = time.time()
                    # R23-SM-P1-01-FIX: PARTIAL_FILLED→FILLED自动转移
                    if sts in ('PARTIAL_FILLED', '部分成交') and filled >= order.get('volume', 0) and order.get('volume', 0) > 0:
                        order['status'] = 'FILLED'
                        logging.info("[R23-SM-P1-01-FIX] PARTIAL_FILLED→FILLED自动转移: order_id=%s filled=%d volume=%d", oid, filled, order.get('volume', 0))
                    # BIZ-P1-10修复: 真正部分成交时执行保护动作
                    elif sts in ('PARTIAL_FILLED', '部分成交') and 0 < filled < order.get('volume', 0):
                        _partial_ratio = filled / order.get('volume', 0)
                        logging.warning("[BIZ-P1-10] 订单部分成交: order_id=%s filled=%d/%d ratio=%.1f%%", oid, filled, order.get('volume', 0), _partial_ratio * 100)
                        try:
                            from ali2026v3_trading.event_bus import get_global_event_bus
                            _bus = get_global_event_bus()
                            if _bus is not None:
                                _bus.publish('order.partial_filled', {'order_id': oid, 'filled_volume': filled, 'total_volume': order.get('volume', 0), 'partial_ratio': _partial_ratio}, async_mode=True)
                        except Exception:
                            pass
                    if sts in ('FILLED', 'ALL_FILLED', '全成') or order['status'] == 'FILLED':
                        self._stats['successful_orders'] += 1
                        # [R14-P0-BIZ-01] 订单完成时释放保证金预留
                        self._release_margin_reservation(oid)
                    if sts in ('CANCELLED', 'FAILED'):
                        # [R14-P0-BIZ-01] 订单取消/失败时释放保证金预留
                        self._release_margin_reservation(oid)
                    # P1-R9-27修复: FAILED状态自动重试逻辑
                    if sts == 'FAILED':
                        _retry_count = order.get('retry_count', 0)
                        _max_retries = 3
                        if _retry_count < _max_retries:
                            logging.info("[P1-R9-27] 订单FAILED, 触发自动重试: order=%s retry=%d/%d", oid, _retry_count + 1, _max_retries)
                            self._chase_reorder(order, _retry_count + 1)
                        else:
                            logging.warning("[P1-R9-27] 订单FAILED且已达最大重试次数: order=%s retries=%d", oid, _retry_count)
        except Exception as e:
            logging.error("[R16-P2-8.5] on_trade_update异常: %s", e, exc_info=True)

    def scan_order_timeouts(self) -> int:
        """R23-SM-04-FIX: 独立超时扫描函数，扫描并处理超时订单
        
        Returns:
            处理的超时订单数量
        """
        _before = len([o for o in self._orders_by_id.values() if o.get('status') in ('SUBMITTED', 'PENDING')])
        self.check_pending_orders()
        _after = len([o for o in self._orders_by_id.values() if o.get('status') in ('SUBMITTED', 'PENDING')])
        _processed = _before - _after
        if _processed > 0:
            logging.info("[R23-SM-04-FIX] scan_order_timeouts: 处理了%d个超时订单", _processed)
        return max(_processed, 0)

    def check_pending_orders(self) -> None:
        now = datetime.now(CHINA_TZ)
        timeout_orders = []
        with self._lock:
            expired_order_ids = []
            for order_id, order in list(self._orders_by_id.items()):
                if order['status'] not in ('SUBMITTED', 'PENDING'):
                    pid = order.get('platform_order_id')
                    if pid and str(pid) in self._platform_id_to_order_id:
                        del self._platform_id_to_order_id[str(pid)]
                    if order['status'] in ('FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', '全成', 'ORPHANED'):
                        elapsed = (now - order.get('updated_at', order['created_at'])).total_seconds()
                        if elapsed > 300:
                            expired_order_ids.append(order_id)
                    continue
                elapsed = (now - order['created_at']).total_seconds()
                # P1-12修复: 根据订单操作类型选择差异化超时
                _order_action = order.get('action', '')
                _order_op_type = 'close' if _order_action in ('CLOSE', 'close', 'SELL', 'sell') else 'open'
                _order_timeout = self._operation_timeouts.get(_order_op_type, self._operation_timeouts['default'])
                if elapsed > _order_timeout:
                    timeout_orders.append((order_id, dict(order)))
            for oid in expired_order_ids:
                # R16-P0-RES-02修复: 终态订单清理时同步移除幂等key，允许同参数重复开仓
                # R27-P0-RC-03修复: 已在self._lock内，无需再获取_idempotent_lock
                _expired_order = self._orders_by_id.get(oid, {})
                _expired_key = f"{_expired_order.get('instrument_id', '')}_{_expired_order.get('direction', '')}_{_expired_order.get('action', '')}_{_expired_order.get('volume', '')}_{round(_expired_order.get('price', 0), 4)}"
                self._order_idempotent_set.discard(_expired_key)
                del self._orders_by_id[oid]
        for order_id, order_snapshot in timeout_orders:
            with self._lock:
                current_order = self._orders_by_id.get(order_id)
                if not current_order or current_order['status'] not in ('SUBMITTED', 'PENDING'):
                    continue
                # R23-SM-04-FIX: 超时订单先标记TIMEOUT中间态，区分"主动撤单"与"超时撤单"
                current_order['status'] = 'TIMEOUT'
                current_order['updated_at'] = datetime.now(CHINA_TZ)
                logging.info("[R23-SM-04-FIX] 订单超时标记TIMEOUT: order_id=%s elapsed=%.1fs", order_id, (datetime.now(CHINA_TZ) - current_order['created_at']).total_seconds())
            _cancel_ok = self.cancel_order(order_id)
            if not _cancel_ok:
                logging.error("[R27-P0-FIX] 撤单失败,跳过追单避免重复持仓: order_id=%s", order_id)
                continue
            retry_count = current_order.get('retry_count', 0) if current_order else order_snapshot.get('retry_count', 0)
            if retry_count < self._max_chase_retries:
                self._chase_reorder(current_order or order_snapshot, retry_count + 1)

    def _correct_price(self, price: float, instrument_id: str) -> float:
        return self._risk_guard.correct_price(price, instrument_id)

    def _get_tick_size(self, instrument_id: str) -> float:
        return self._risk_guard.get_tick_size(instrument_id)

    def _get_last_market_price(self, instrument_id: str) -> Optional[float]:
        return self._risk_guard.get_last_market_price(instrument_id, self._orders_by_id)

    def _estimate_slippage(self, instrument_id: str, price: float, volume: int,
                           bid_ask_spread: float = 0.0, days_to_expiry: int = 999,
                           spread_quality: int = 1) -> float:
        return self._risk_guard.estimate_slippage(instrument_id, price, volume, bid_ask_spread, days_to_expiry, spread_quality)

    def _release_margin_reservation(self, order_id: str) -> None:
        self._risk_guard.release_margin_reservation(order_id)

    def _chase_reorder(self, original_order: Dict, retry_count: int) -> None:
        self._chase_service._chase_reorder(original_order, retry_count)

    def get_order_status(self, order_id: str) -> str:
        with self._lock:
            order = self._orders_by_id.get(order_id)
            if not order:
                return 'UNKNOWN'
            return str(order.get('status', 'UNKNOWN'))

    def get_order(self, order_id: str) -> Optional[Dict]:
        with self._lock:
            return self._orders_by_id.get(order_id)

    # [R16-P2-6.1修复] 通过平台订单ID查询订单，消除_order_id_map死路径
    def get_order_by_platform_id(self, platform_order_id: str) -> Optional[Any]:
        with self._lock:
            internal_id = self._platform_id_to_order_id.get(str(platform_order_id))
            if internal_id:
                return self._orders_by_id.get(internal_id)
            for o in self._orders_by_id.values():
                if o.get('platform_order_id') == platform_order_id:
                    return o
        return None

    def get_orders_by_instrument(self, instrument_id: str) -> List[Dict]:
        with self._lock:
            return [o for o in self._orders_by_id.values() if o['instrument_id'] == instrument_id]

    # ✅ ID唯一：get_stats统一接口，返回值含service_name="OrderService"
    def get_stats(self) -> Dict[str, Any]:
        # ✅ P2修复：将订单清理逻辑移到独立方法，避免隐藏副作用
        self._cleanup_orders()
        
        with self._lock:
            return {
                'service_name': 'OrderService',  # ✅ ID唯一：统一标识服务来源
                **self._stats,
                'active_orders': sum(1 for o in self._orders_by_id.values() if o['status'] == 'SUBMITTED'),
                'total_tracked': len(self._orders_by_id),
                'chase_tasks': len(self._chase_tasks),
            }
    
    def _remove_order_and_idempotent_key(self, order_id: str, order: Dict) -> None:
        """R26-P2-IDEM-01修复: 从_orders_by_id移除订单并同步清理幂等键"""
        _idempotent_key = f"{order.get('instrument_id', '')}_{order.get('direction', '')}_{order.get('action', '')}_{order.get('volume', '')}_{round(order.get('price', 0), 4)}"
        self._order_idempotent_set.discard(_idempotent_key)
        self._orders_by_id.pop(order_id, None)

    def _cleanup_orders(self) -> None:
        """✅ P2修复：独立的订单清理方法，消除get_stats的隐藏副作用"""
        now = time.time()
        if now - self._last_cleanup <= self._cleanup_interval:
            return
        
        with self._lock:
            self._last_cleanup = now
            to_remove = [oid for oid, o in self._orders_by_id.items() 
                        if o['status'] in ('FILLED', 'CANCELLED', 'FAILED', 'ORPHANED') 
                        and (datetime.now(CHINA_TZ) - o['updated_at']).total_seconds() > 3600]
            for oid in to_remove:
                # R24-P1-TR-09修复: 订单清理前保存审计关键字段
                _order = self._orders_by_id.get(oid, {})
                self._history_audit.append({
                    'order_id': oid, 'instrument_id': _order.get('instrument_id', ''),
                    'direction': _order.get('direction', ''), 'signal_id': _order.get('signal_id', ''),
                    'status': _order.get('status', ''), 'cleanup_time': time.time(),
                })
                del self._orders_by_id[oid]
            # R26-P2-WAL-01修复: 清理终态订单时同步删除WAL文件
            for oid in to_remove:
                self._wal_delete(oid)
            if to_remove:
                logging.info("[OrderService] 清理%d个已完成订单", len(to_remove))

    _DEDUP_TTL_SECONDS = 5.0

    def _is_duplicate_order(self, instrument_id: str, order_key: str) -> bool:
        with self._lock:
            if instrument_id not in self._recent_orders_by_instrument:
                self._recent_orders_by_instrument[instrument_id] = []
            now = time.time()
            recent = self._recent_orders_by_instrument[instrument_id]
            recent[:] = [(k, t) for k, t in recent if now - t < self._DEDUP_TTL_SECONDS]
            for k, _ in recent:
                if k == order_key:
                    return True
            recent.append((order_key, now))
        return False

    def _generate_order_id(self) -> str:
        import uuid
        timestamp = int(time.time() * 1000)
        unique_suffix = uuid.uuid4().hex[:8]
        return f"ORD_{timestamp}_{unique_suffix}"

    # [ID-03-FIX] chase订单ID前缀"CHASE_"区分，CLOSE订单去重
    def _generate_chase_order_id(self, original_order_id: str) -> str:
        import uuid
        timestamp = int(time.time() * 1000)
        unique_suffix = uuid.uuid4().hex[:8]
        return f"CHASE_{timestamp}_{unique_suffix}"

    # [R23-P2-FR-08-FIX] 订单数据年龄查询
    def get_order_data_age(self, order_id: str) -> Optional[float]:
        """返回指定订单数据年龄(秒)，None表示未找到"""
        _last = self._order_last_update_time.get(order_id)
        if _last is None:
            return None
        return time.time() - _last

    def _get_rate_limit(self) -> int:
        try:
            from ali2026v3_trading.params_service import get_params_service

            params_svc = get_params_service()
            rate = params_svc.get_int('rate_limit_global_per_min', 60)
            if rate > 0:
                return rate
        except Exception as e:
            # ✅ P1修复：添加异常日志，便于诊断
            logging.debug("[OrderService._get_rate_limit] Failed to get rate limit from ParamsService: %s", e)
        return 60

    def check_risk_block(self, instrument_id: str) -> bool:
        return self._risk_guard.check_risk_block(instrument_id)

    def set_risk_block(self, instrument_id: str, duration: float = None) -> None:
        self._risk_guard.set_risk_block(instrument_id, duration)

    # R26: WAL+STATE方法委托到OrderWALStateService
    def _ensure_wal_dir(self) -> None:
        return self._wal_state_service._ensure_wal_dir()

    def _wal_path(self, order_id: str) -> str:
        return self._wal_state_service._wal_path(order_id)

    def _wal_write(self, order_id: str, state: str, order: Dict) -> None:
        return self._wal_state_service._wal_write(order_id, state, order)

    def _wal_read(self, order_id: str) -> Optional[Dict]:
        return self._wal_state_service._wal_read(order_id)

    def _wal_delete(self, order_id: str) -> None:
        return self._wal_state_service._wal_delete(order_id)

    def _recover_orphaned_orders(self) -> None:
        return self._wal_state_service._recover_orphaned_orders()

    def _persist_idempotent_key(self, key: str) -> None:
        return self._wal_state_service._persist_idempotent_key(key)

    def _recover_idempotent_state(self) -> None:
        return self._wal_state_service._recover_idempotent_state()

    def _rotate_jsonl_if_needed(self, filepath: str) -> None:
        return self._wal_state_service._rotate_jsonl_if_needed(filepath)

    def _append_order_state(self, order_id: str, state: str, order: Dict) -> None:
        return self._wal_state_service._append_order_state(order_id, state, order)

    def _recover_order_state(self) -> None:
        return self._wal_state_service._recover_order_state()

    def _execute_with_compensation_v2(
        self,
        steps: List[Dict[str, Any]],
        result_ids: List[str],
        compensate_fn: Optional[Callable] = None,
    ) -> List[str]:
        return self._wal_state_service._execute_with_compensation_v2(steps, result_ids, compensate_fn)











from ali2026v3_trading.order_platform_auth import PlatformAuthenticator
from ali2026v3_trading.order_sync import sync_order_status_with_exchange
from ali2026v3_trading.order_split_models import OrderSplitStrategy, SplitOrderResult, SmartOrderSplitter
from ali2026v3_trading.order_market_impact import almgren_chriss_impact, estimate_fill_probability
from ali2026v3_trading.order_compliance import check_self_trade_across_splits, AlgoTradingCompliance, WashTradeDetector

__all__ = ['OrderService', 'RateLimiter', 'PlatformAuthenticator', 'get_order_service',
           'sync_order_status_with_exchange', 'almgren_chriss_impact',
           'estimate_fill_probability', 'check_self_trade_across_splits',
           'AlgoTradingCompliance', 'WashTradeDetector',
           'SmartOrderSplitter', 'OrderSplitStrategy', 'SplitOrderResult']
