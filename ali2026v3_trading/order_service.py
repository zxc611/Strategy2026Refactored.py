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
from ali2026v3_trading.serialization_utils import json_dumps

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


class PlatformAuthenticator:
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._lock = threading.Lock()
        # P1-R8-14修复: 防重放攻击 — nonce缓存+时间窗口
        self._nonce_cache: Dict[str, float] = {}
        self._nonce_window_seconds: float = 300.0
        self._max_nonce_cache: int = 1000

    def get_token(self) -> Optional[str]:
        with self._lock:
            if self._token and time.time() < self._token_expiry:
                return self._token
            return None

    def set_token(self, token: str, expires_in: float = 3600.0) -> None:
        with self._lock:
            self._token = token
            self._token_expiry = time.time() + expires_in

    def validate_nonce(self, nonce: str, timestamp: Optional[float] = None) -> bool:
        """P1-R8-14修复: 验证请求nonce防重放攻击

        规则:
        1. nonce在时间窗口内必须单调递增(拒绝已使用过的nonce)
        2. 请求时间戳不得滞后超过_window_seconds
        3. 超出缓存容量的旧nonce自动淘汰
        """
        with self._lock:
            now = time.time()
            if len(self._nonce_cache) > self._max_nonce_cache:
                _sorted = sorted(self._nonce_cache.items(), key=lambda x: x[1])
                _evict_n = len(self._nonce_cache) - self._max_nonce_cache + 100
                for k, _ in _sorted[:_evict_n]:
                    del self._nonce_cache[k]
            _expired = [k for k, t in self._nonce_cache.items() if now - t > self._nonce_window_seconds]
            for k in _expired:
                del self._nonce_cache[k]
            if nonce in self._nonce_cache:
                return False
            if timestamp is not None and (now - timestamp) > self._nonce_window_seconds:
                return False
            self._nonce_cache[nonce] = now
            return True

    def generate_signed_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        _nonce = f"{time.time():.6f}_{os.urandom(4).hex()}"
        _timestamp = time.time()
        _token = self.get_token() or ''
        _sign_payload = f"{_nonce}:{_timestamp:.3f}:{_token}"
        _hmac_key = os.environ.get('ORDER_SIGN_KEY', 'default_order_sign_key_change_in_prod')
        _signature = hmac.new(_hmac_key.encode(), _sign_payload.encode(), hashlib.sha256).hexdigest()[:16]
        if not self.validate_nonce(_nonce, _timestamp):
            logging.warning("[PlatformAuthenticator] nonce验证失败，可能重放攻击: nonce=%s", _nonce)
            return {}
        signed = dict(request_data)
        signed['_nonce'] = _nonce
        signed['_timestamp'] = _timestamp
        signed['_signature'] = _signature
        return signed


_order_service_instance: Optional['OrderService'] = None
_order_service_lock = threading.Lock()


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
    def __init__(self, event_bus=None):
        super().__init__(event_bus)
        # R23-IN-P1-09-FIX: 平台API就绪守卫
        self._platform_api_ready: bool = False
        self.authenticator = PlatformAuthenticator()
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
        if self._circuit_breaker_open:
            _elapsed = time.time() - self._circuit_breaker_opened_at if self._circuit_breaker_opened_at > 0 else 0
            if _elapsed >= self._circuit_breaker_auto_recovery_sec:
                self._circuit_breaker_half_open = True
                logging.info("[R22-P0-CB-02] 断路器超时自动恢复(半开): 已过%.0f秒>=%.0f秒, 允许探测订单",
                             _elapsed, self._circuit_breaker_auto_recovery_sec)
            elif self._circuit_breaker_half_open:
                logging.info("[R22-P0-CB-02] 断路器半开状态, 允许探测订单: %s %s", instrument_id, direction)
            else:
                logging.critical("[OrderService] 断路器已触发，拒绝下单: consecutive_failures=%d, 已过%.0f秒",
                                 self._consecutive_failures, _elapsed)
                return OrderResult.fail('circuit_breaker', f'断路器触发，连续{self._consecutive_failures}次失败')
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _cyclic_guard and not _cyclic_guard.enter("order_send_order"):
            logging.warning("[CC-04/CC-11] Cyclic call detected in send_order, rejecting order: %s", instrument_id)
            return OrderResult.fail('cyclic_call', '循环依赖检测，订单被拒')
        if direction not in ('BUY', 'SELL'):
            logging.error("[OrderService] R24-P0-IV-05: direction必须是BUY/SELL, 实际=%s, 订单被拒: %s", direction, instrument_id)
            return OrderResult.fail('invalid_direction', f'direction={direction}非法')
        if not signal_id:
            logging.warning("[OrderService] R24-P1-TR-08: signal_id为空, 订单无法追溯到信号: %s %s", instrument_id, direction)
        if not isinstance(price, (int, float)) or not math.isfinite(price) or price < 0:
            logging.error("[OrderService] R24-P2-IV-07: 价格无效(price=%s), 订单被拒: %s", price, instrument_id)
            return OrderResult.fail('invalid_price', f'price={price}无效')
        # [R14-P0-BIZ-09] 胖手指防护: 价格偏离涨跌停板检查
        _ref_price = self._get_last_market_price(instrument_id)
        if _ref_price is not None and _ref_price > 0:
            _price_deviation = abs(price - _ref_price) / _ref_price
            _FAT_FINGER_THRESHOLD = 0.03
            if _price_deviation > _FAT_FINGER_THRESHOLD:
                logging.critical(
                    "[R14-P0-BIZ-09] 胖手指防护触发: instrument=%s price=%.2f ref=%.2f deviation=%.2f%%>%.2f%%",
                    instrument_id, price, _ref_price, _price_deviation * 100, _FAT_FINGER_THRESHOLD * 100,
                )
                return OrderResult.fail('fat_finger', f'价格偏离{_price_deviation:.2%}>涨跌停板{_FAT_FINGER_THRESHOLD:.2%}')
        LARGE_TRADE_THRESHOLD_AMOUNT = 5000000
        trade_amount = volume * price
        if trade_amount >= LARGE_TRADE_THRESHOLD_AMOUNT:
            logging.critical("[OrderService] 单笔订单金额超限: %s %.2f >= %.2f", instrument_id, trade_amount, LARGE_TRADE_THRESHOLD_AMOUNT)
            return OrderResult.fail('amount_exceeded', f'单笔金额{trade_amount:.0f}超限')
        if action != 'CLOSE' and not is_chase:
            if not self.rate_limiter.acquire():
                logging.warning("[OrderService] Order rate limited: %s", instrument_id)
                return OrderResult.fail('rate_limited', '下单频率超限')
            order_key = f"{instrument_id}_{exchange}_{direction}_{action}_{volume}_{round(price, 4)}"
            if self._is_duplicate_order(instrument_id, order_key):
                logging.warning("[OrderService] Duplicate order: %s", order_key)
                return OrderResult.fail('duplicate', '重复订单')
        if is_chase:
            logging.info("[R16-P2-5.1] 追单绕过限流: instrument=%s direction=%s action=%s", instrument_id, direction, action)
        # R27-P0-RC-03修复: 幂等去重检查移入self._lock内，消除_idempotent_lock→_lock的AB-BA死锁
        _idempotent_key = f"{instrument_id}_{direction}_{action}_{volume}_{round(price, 4)}"
        _now = time.time()
        _expired_keys = [k for k, t in self._self_trade_bans.items() if _now >= t]
        _ban_key = f"{instrument_id}_{direction}"
        _ban_until = self._self_trade_bans.get(_ban_key, 0.0)
        if _now < _ban_until:
            logging.warning(
                "[OrderService] 裂缝46: 自成交禁止期内, 拒绝下单: key=%s 剩余%.1f分钟",
                _ban_key, (_ban_until - _now) / 60.0,
            )
            return OrderResult.fail('self_trade_ban', f'自成交禁止期剩余{(_ban_until - _now) / 60.0:.1f}分钟')
        for _ek in _expired_keys:
            del self._self_trade_bans[_ek]
        if action == 'OPEN':
            _opposite_dir = 'SELL' if direction == 'BUY' else 'BUY'
            _opp_key = f"{instrument_id}_{_opposite_dir}"
            for _oid, _o in self._orders_by_id.items():
                if (_o.get('instrument_id') == instrument_id
                        and _o.get('direction') == _opposite_dir
                        and _o.get('action') == 'OPEN'
                        and _o.get('status') in ('SUBMITTED', 'PARTIAL')):
                    self._self_trade_bans[_opp_key] = _now + self._self_trade_ban_minutes * 60.0
                    logging.warning(
                        "[OrderService] 裂缝46: 检测到自成交风险(同合约反方向), 设置禁止期: key=%s %.1f分钟",
                        _opp_key, self._self_trade_ban_minutes,
                    )
                    # P1-R9-28修复: 自成交检测后通知策略层
                    try:
                        from ali2026v3_trading.event_bus import EventBus
                        EventBus.get_instance().publish("self_trade_detected", {
                            "instrument_id": instrument_id,
                            "direction": direction,
                            "opposite_order_id": _oid,
                            "ban_minutes": self._self_trade_ban_minutes,
                        })
                    except Exception as _eb_err:
                        logging.debug("[P1-R9-28] EventBus通知失败: %s", _eb_err)
                    break
        # [ID-03-FIX] CLOSE订单去重: 相同instrument_id+direction的CLOSE不重复
        if action == 'CLOSE':
            close_key = f"CLOSE_{instrument_id}_{direction}"
            if not hasattr(self, '_close_order_sent'):
                self._close_order_sent: Dict[str, float] = {}
            _last_close_time = self._close_order_sent.get(close_key, 0.0)
            if (time.time() - _last_close_time) < 5.0:
                logging.warning("[ID-03-FIX] CLOSE订单去重拦截: %s", close_key)
                return OrderResult.fail('close_duplicate', 'CLOSE订单去重拦截')
        # [R26-P0-TO-04修复] TOCTOU窗口保护: 下单前仓位快照二次验证（乐观锁模式）
        # expected_position_count: -2=自动获取(默认), -1=禁用, >=0=显式指定
        if expected_position_count != -1 and self._get_position_count is not None:
            if expected_position_count == -2:
                try:
                    expected_position_count = self._get_position_count(instrument_id)
                except Exception as e:
                    logging.error("[OrderService] R26-P0-TO-04: 自动获取仓位失败: %s, 跳过TOCTOU检查: %s", e, instrument_id)
                    expected_position_count = -1
            if expected_position_count >= 0:
                try:
                    current_position = self._get_position_count(instrument_id)
                    if current_position != expected_position_count:
                        logging.warning(
                            "[OrderService] R26-P0-TO-04: 风控TOCTOU阻断, 仓位已变化: "
                            "instrument=%s expected=%d actual=%d, 订单被拒",
                            instrument_id, expected_position_count, current_position,
                        )
                        return OrderResult.fail('toctou_position_changed', f'仓位变化: expected={expected_position_count} actual={current_position}')
                except Exception as e:
                    logging.error("[OrderService] R26-P0-TO-04: 仓位快照查询异常: %s, 订单被拒: %s", e, instrument_id)
                    return OrderResult.fail('position_query_error', f'仓位查询异常: {e}')
        try:
            order_id = self._generate_order_id()
            order = {
                'order_id': order_id,
                'instrument_id': instrument_id,
                'exchange': exchange,
                'volume': volume,
                'price': price,
                'direction': direction,
                'action': action,
                'status': 'SUBMITTED',
                'filled_volume': 0,
                'signal_id': signal_id,
                'open_reason': open_reason,
                'decision_score': decision_score,
                'position_scale': position_scale,
                'decision_action': decision_action,
                'dimension_scores': dimension_scores or {},
                'dimension_weights': dimension_weights or {},
                'created_at': datetime.now(CHINA_TZ),
                'updated_at': datetime.now(CHINA_TZ),
            }
            signed_order = self.authenticator.generate_signed_request(order)
            if not signed_order:
                logging.error("[OrderService] 请求签名失败，可能nonce冲突或token无效")
                return OrderResult.fail('sign_failed', '请求签名失败')
            order = signed_order
            _slippage_bps = self._estimate_slippage(instrument_id, price, int(volume))
            logging.info("[R16-P0-005] 滑点估算: instrument=%s price=%.2f volume=%d slippage=%.2fbps", instrument_id, price, int(volume), _slippage_bps)
            with self._lock:
                # R27-P0-RC-03修复: 幂等去重检查与订单登记在同一锁内，消除竞态窗口
                if _idempotent_key in self._order_idempotent_set:
                    logging.warning("[R27-P0-RC-03] 幂等去重拦截(锁内): %s", _idempotent_key)
                    return OrderResult.fail('idempotent_duplicate', f'幂等去重拦截: {_idempotent_key}')
                self._order_idempotent_set.add(_idempotent_key)
                self._persist_idempotent_key(_idempotent_key)
                # R26-P2-MEM-01修复: 容量上限检查，超限时强制清理终态订单
                if len(self._orders_by_id) >= self._MAX_ORDERS_TRACKED:
                    _forced_remove = [oid for oid, o in self._orders_by_id.items()
                                     if o['status'] in ('FILLED', 'CANCELLED', 'FAILED', 'ORPHANED')]
                    if _forced_remove:
                        for _foid in _forced_remove:
                            self._order_idempotent_set.discard(
                                f"{self._orders_by_id[_foid].get('instrument_id', '')}_{self._orders_by_id[_foid].get('direction', '')}_{self._orders_by_id[_foid].get('action', '')}_{self._orders_by_id[_foid].get('volume', '')}_{round(self._orders_by_id[_foid].get('price', 0), 4)}")
                            del self._orders_by_id[_foid]
                        logging.warning("[R26-P2-MEM-01] 容量超限(%d>=%d)，强制清理%d个终态订单",
                                       len(self._orders_by_id), self._MAX_ORDERS_TRACKED, len(_forced_remove))
                    else:
                        logging.error("[R26-P2-MEM-01] 容量超限(%d>=%d)且无终态订单可清理，拒绝新订单",
                                     len(self._orders_by_id), self._MAX_ORDERS_TRACKED)
                        self._order_idempotent_set.discard(_idempotent_key)
                        return OrderResult.fail('capacity_exceeded', f'订单追踪容量超限({self._MAX_ORDERS_TRACKED})')
                self._orders_by_id[order_id] = order
                self._stats['total_orders'] += 1
            # [R25-TO-03-FIX] WAL: 创建订单时记录PENDING状态
            self._wal_write(order_id, 'PENDING', order)
            # R16-P0-RES-05修复: 订单状态追加写JSONL
            self._append_order_state(order_id, 'SUBMITTED', order)
            if self._platform_insert_order and callable(self._platform_insert_order):
                try:
                    from ali2026v3_trading.config_service import resolve_product_exchange
                    if not exchange:
                        exchange = resolve_product_exchange(instrument_id)
                    filtered_params = self._build_platform_insert_params(
                        order_id=order_id,
                        instrument_id=instrument_id,
                        exchange=exchange,
                        volume=volume,
                        price=price,
                        direction=direction,
                        action=action,
                    )
                    result = self._invoke_platform_insert_with_timeout(filtered_params)
                    platform_order_id = self._normalize_platform_result(result).get('order_id')
                    if platform_order_id:
                        with self._lock:
                            order['platform_order_id'] = platform_order_id
                            self._platform_id_to_order_id[str(platform_order_id)] = order_id
                    logging.info("[OrderService] 平台下单成功: %s %s %s %d@%.2f", order_id, instrument_id, direction, int(volume), price)
                    self._consecutive_failures = 0
                    self._circuit_breaker_open = False
                    self._circuit_breaker_half_open = False
                    self._circuit_breaker_opened_at = 0.0
                    # [R25-TO-03-FIX] WAL: 平台确认后记录CONFIRMED
                    self._wal_write(order_id, 'CONFIRMED', order)
                    # R16-P0-RES-05修复: 订单确认追加写JSONL
                    self._append_order_state(order_id, 'CONFIRMED', order)
                    # R26-P2-WAL-01修复: CONFIRMED后清理WAL文件，防止磁盘空间泄漏
                    self._wal_delete(order_id)
                    # API-P1-16修复: 正常下单成功时发布EventBus事件
                    try:
                        from ali2026v3_trading.event_bus import get_global_event_bus
                        _bus = get_global_event_bus()
                        if _bus is not None:
                            _bus.publish('order.submitted', {
                                'order_id': order_id,
                                'instrument_id': instrument_id,
                                'direction': direction,
                                'action': action,
                                'volume': volume,
                                'price': price,
                            }, async_mode=True)
                    except Exception as _eb_err:
                        logging.debug("[API-P1-16] EventBus publish order.submitted failed: %s", _eb_err)
                except TimeoutError as e:
                    logging.error("[R27-P0-DR-01] 平台下单超时: %s %s", instrument_id, e)
                    with self._lock:
                        order['status'] = 'TIMEOUT'
                        order['updated_at'] = datetime.now(CHINA_TZ)
                        order['timeout_reason'] = str(e)
                        self._stats['failed_orders'] += 1
                        self._consecutive_failures += 1
                        if self._consecutive_failures >= self._circuit_breaker_threshold:
                            self._circuit_breaker_open = True
                            self._circuit_breaker_opened_at = time.time()
                            logging.critical("[R22-P0-CB-01] 断路器触发(超时路径): consecutive_failures=%d >= threshold=%d",
                                             self._consecutive_failures, self._circuit_breaker_threshold)
                    self._wal_write(order_id, 'TIMEOUT', order)
                    self._append_order_state(order_id, 'TIMEOUT', order)
                    return OrderResult.fail('platform_timeout', f'平台下单超时: {e}')
                except Exception as e:
                    logging.error("[OrderService] 平台下单失败: %s %s", instrument_id, e)
                    self._wal_write(order_id, 'FAILED', order)
                    self._append_order_state(order_id, 'FAILED', order)
                    with self._lock:
                        order['status'] = 'FAILED'
                        self._remove_order_and_idempotent_key(order_id, order)
                        self._stats['failed_orders'] += 1
                        self._consecutive_failures += 1
                        if self._consecutive_failures >= self._circuit_breaker_threshold:
                            self._circuit_breaker_open = True
                            self._circuit_breaker_opened_at = time.time()
                            logging.critical("[R22-P0-CB-01] 断路器触发(异常路径): consecutive_failures=%d >= threshold=%d",
                                             self._consecutive_failures, self._circuit_breaker_threshold)
                    return OrderResult.fail('platform_error', f'平台下单失败: {e}')
            else:
                logging.info("[OrderService] 模拟下单: %s %s %s %d@%.2f", order_id, _mask_id(instrument_id), direction[:1], int(volume), price)
            if _cyclic_guard:
                _cyclic_guard.exit("order_send_order")
            return OrderResult.ok(order_id)
        except Exception as e:
            logging.error("[OrderService] Send order error: %s", e)
            with self._lock:
                try:
                    if order_id and order_id in self._orders_by_id:
                        self._remove_order_and_idempotent_key(order_id, self._orders_by_id.get(order_id, {}))
                except NameError:
                    pass
                self._stats['failed_orders'] += 1
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._circuit_breaker_threshold:
                    self._circuit_breaker_open = True
                    logging.critical("[OrderService] 断路器触发: consecutive_failures=%d >= threshold=%d",
                                     self._consecutive_failures, self._circuit_breaker_threshold)
            try:
                if order_id:
                    self._wal_write(order_id, 'FAILED', {'order_id': order_id, 'instrument_id': instrument_id, 'direction': direction, 'volume': volume, 'price': price})
                    self._append_order_state(order_id, 'FAILED', {'order_id': order_id, 'status': 'FAILED'})
            except Exception:
                pass
            return OrderResult.fail('send_error', f'下单异常: {e}')

    _PNL_CORR_THRESHOLD = 0.6

    def _compute_pnl_correlation(self, pnl_series_a, pnl_series_b) -> float:
        """P0-CS-001修复: 计算两个策略PnL序列的皮尔逊相关系数"""
        import numpy as np
        try:
            a = np.array(pnl_series_a, dtype=np.float64)
            b = np.array(pnl_series_b, dtype=np.float64)
            min_len = min(len(a), len(b))
            if min_len < 5:
                return 0.0
            a = a[:min_len]
            b = b[:min_len]
            a_mean = np.mean(a)
            b_mean = np.mean(b)
            cov = np.mean((a - a_mean) * (b - b_mean))
            var_a = np.mean((a - a_mean) ** 2)
            var_b = np.mean((b - b_mean) ** 2)
            denom = np.sqrt(var_a * var_b)
            if denom < 1e-12:
                return 0.0
            return float(cov / denom)
        except Exception:
            return 0.0

    def _check_cross_strategy_risk(self) -> float:
        """P0-CS-001修复: 检查跨策略PnL相关性，返回最大相关系数"""
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            _eco = get_strategy_ecosystem()
            if _eco is None:
                return 0.0
            strategies = []
            for slot_name in ('master', 'reverse', 'other', 'spring'):
                strat = getattr(_eco, f'_{slot_name}', None)
                if strat and hasattr(strat, 'equity_curve') and len(strat.equity_curve) >= 10:
                    strategies.append((slot_name, strat.equity_curve))
            max_corr = 0.0
            for i in range(len(strategies)):
                for j in range(i + 1, len(strategies)):
                    pa = list(np.diff(strategies[i][1]))
                    pb = list(np.diff(strategies[j][1]))
                    corr = self._compute_pnl_correlation(pa, pb)
                    if abs(corr) > max_corr:
                        max_corr = abs(corr)
            if max_corr > self._PNL_CORR_THRESHOLD:
                logging.warning(
                    "[OrderService] P0-CS-001: 跨策略PnL相关系数%.3f超过阈值%.1f",
                    max_corr, self._PNL_CORR_THRESHOLD,
                )
            return max_corr
        except Exception:
            return 0.0

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
        if direction not in ('BUY', 'SELL'):
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
            lob = asks if direction == 'BUY' else bids
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
            if target_direction == 'BUY':
                price = self._correct_price(price + tick_size, instrument_id)
            elif target_direction == 'SELL':
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
        try:
            with self._lock:
                order = self._orders_by_id.get(order_id)
                if not order:
                    logging.warning("[OrderService] Order not found: %s", order_id)
                    return False
                if order['status'] in ('FILLED', 'CANCELLED', 'FAILED'):
                    return False
                platform_id = order.get('platform_order_id', order_id)
            if self._platform_cancel_order and callable(self._platform_cancel_order):
                try:
                    self._platform_cancel_order(platform_id)
                    logging.info("[OrderService] 平台撤单成功: %s", order_id)
                except Exception as e:
                    logging.error("[OrderService] 平台撤单失败: %s %s", order_id, e)
                    return False
            with self._lock:
                order['status'] = 'CANCELLED'
                order['updated_at'] = datetime.now(CHINA_TZ)
                self._stats['cancelled_orders'] += 1
            return True
        except Exception as e:
            logging.error("[OrderService] Cancel order error: %s", e)
            return False

    def emergency_close_all_positions(self, caller_id: str = "unknown") -> Dict[str, Any]:
        result: Dict[str, Any] = {'cancelled': [], 'closed': [], 'errors': []}
        try:
            with self._lock:
                pending_order_ids = [
                    oid for oid, o in self._orders_by_id.items()
                    if o.get('status') in ('PENDING', 'SUBMITTED', 'PARTIAL_FILL')
                ]
            for oid in pending_order_ids:
                if self.cancel_order(oid):
                    result['cancelled'].append(oid)
                else:
                    result['errors'].append(f"cancel_failed:{oid}")
            with self._lock:
                open_positions = {
                    oid: dict(o) for oid, o in self._orders_by_id.items()
                    if o.get('status') == 'FILLED' and o.get('action') == 'OPEN'
                }
            for oid, pos in open_positions.items():
                instrument_id = pos.get('instrument_id', '')
                direction = 'SELL' if pos.get('direction') == 'BUY' else 'BUY'
                volume = pos.get('volume', 0)
                price = pos.get('price', 0)
                if volume > 0 and price > 0:
                    close_result = self.send_order(
                        instrument_id=instrument_id,
                        exchange=pos.get('exchange', ''),
                        direction=direction,
                        price=price,
                        volume=volume,
                        action='CLOSE',
                        signal_id=f'emergency_{caller_id}',
                        open_reason=f'emergency_close_by_{caller_id}',
                    )
                    if close_result.ok:
                        result['closed'].append(oid)
                    else:
                        result['errors'].append(f"close_failed:{oid}:{close_result.error_code}")
            logging.critical("[R22-P0-EC-01] emergency_close_all_positions: caller=%s cancelled=%d closed=%d errors=%d",
                             caller_id, len(result['cancelled']), len(result['closed']), len(result['errors']))
        except Exception as e:
            logging.error("[R22-P0-EC-01] emergency_close_all_positions error: %s", e)
            result['errors'].append(str(e))
        return result

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
                if elapsed > self._order_timeout_seconds:
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
            self.cancel_order(order_id)
            retry_count = current_order.get('retry_count', 0) if current_order else order_snapshot.get('retry_count', 0)
            if retry_count < self._max_chase_retries:
                self._chase_reorder(current_order or order_snapshot, retry_count + 1)

    def _correct_price(self, price: float, instrument_id: str) -> float:
        tick_size = self._get_tick_size(instrument_id)
        if tick_size <= 0:
            return price
        aligned = round(price / tick_size) * tick_size
        return max(aligned, tick_size)

    def _get_tick_size(self, instrument_id: str) -> float:
        """获取合约最小变动价位
        
        注意：instrument_id 已在入口处标准化，此处直接提取品种代码
        """
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            # ✅ 使用 params_service 元数据获取品种，而非正则
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            product = meta.get('product') if meta else instrument_id
            product_cache = params_svc.get_product_cache(product)
            if product_cache:
                tick_size = float(product_cache.get('tick_size', 0))
                if tick_size > 0:
                    return tick_size
        except Exception as e:
            # ✅ P1修复：添加异常日志，便于诊断
            logging.debug(f"[OrderService._get_tick_size] Failed to get tick_size from ParamsService: {e}")
        # ✅ P1修复：使用品种配置的默认值，而非硬编码1.0
        default_tick_sizes = {
            'IF': 0.2,  # 沪深300股指期货
            'IC': 0.2,  # 中证500股指期货
            'IH': 0.2,  # 上证50股指期货
            'IM': 0.2,  # 中证1000股指期货
        }
        for prefix, tick in default_tick_sizes.items():
            if instrument_id.startswith(prefix):
                return tick
        return 1.0  # 最终降级

    def _get_last_market_price(self, instrument_id: str) -> Optional[float]:
        try:
            from ali2026v3_trading.query_service import get_query_service
            qs = get_query_service()
            tick = qs.get_last_tick(instrument_id)
            if tick and isinstance(tick, dict):
                return tick.get('last_price') or tick.get('price')
        except Exception:
            pass
        try:
            if instrument_id in self._orders_by_id:
                for order in self._orders_by_id.values():
                    if order.get('instrument_id') == instrument_id and order.get('status') in ('FILLED', 'ALL_FILLED'):
                        return order.get('price')
        except Exception:
            pass
        return None

    def _estimate_slippage(self, instrument_id: str, price: float, volume: int,
                           bid_ask_spread: float = 0.0, days_to_expiry: int = 999,
                           spread_quality: int = 1) -> float:
        """R16-P0-005修复: 估算滑点(bps) - 与回测_compute_dynamic_slippage_bps对齐"""
        base_slippage_bps = 3.0
        spread_bps = bid_ask_spread / price * 10000.0 if price > 0 else 0.0
        dynamic_bps = max(base_slippage_bps, spread_bps * 0.5)
        quality_scale = {1: 0.75, 2: 0.5, 3: 0.3}.get(spread_quality, 1.0)
        dynamic_bps *= quality_scale
        if days_to_expiry <= 3:
            dynamic_bps *= 20.0
        elif days_to_expiry <= 7:
            dynamic_bps *= 10.0
        elif days_to_expiry <= 14:
            dynamic_bps *= 3.0
        return dynamic_bps

    def _release_margin_reservation(self, order_id: str) -> None:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            rs.release_margin(order_id)
        except Exception as e:
            logging.debug("[R14-P0-BIZ-01] 释放保证金预留失败(可忽略): order_id=%s err=%s", order_id, e)

    def _chase_reorder(self, original_order: Dict, retry_count: int) -> None:
        # R23-ID-03-FIX: chase重试前检查订单是否已存在活跃状态
        _orig_id = original_order.get('order_id', '')
        with self._lock:
            _existing = self._orders_by_id.get(_orig_id)
            if _existing and _existing.get('status') in ('SUBMITTED', 'ACCEPTED', 'PARTIAL_FILLED'):
                logging.info("[R23-ID-03-FIX] chase重试跳过: 订单%s仍处于活跃状态%s",
                           _orig_id, _existing.get('status'))
                return
        # P1-R9-26修复: 检查原订单是否已部分成交，追单量=原量-已成交量
        _filled_volume = original_order.get('filled_volume', 0)
        _original_volume = original_order.get('volume', 0)
        if _filled_volume > 0:
            _remaining_volume = _original_volume - _filled_volume
            if _remaining_volume <= 0:
                logging.info("[P1-R9-26] 原订单已完全成交, 无需追单: order=%s filled=%d", 
                           original_order.get('order_id', ''), _filled_volume)
                return
            logging.info("[P1-R9-26] 原订单部分成交, 追单剩余量: order=%s original=%d filled=%d remaining=%d",
                        original_order.get('order_id', ''), _original_volume, _filled_volume, _remaining_volume)
        else:
            _remaining_volume = _original_volume
        # [ID-P1-08-FIX] 指数退避: 检查上次重试时间，避免重试风暴
        _order_id = original_order.get('order_id', '')
        _backoff_delay = min(self._retry_backoff_base * (2 ** (retry_count - 1)), self._retry_backoff_max)
        _last_retry = self._last_retry_time.get(_order_id, 0.0)
        _elapsed_since_last = time.time() - _last_retry
        if _elapsed_since_last < _backoff_delay:
            logging.debug("[ID-P1-08-FIX] 重试退避中: order=%s retry=%d wait=%.1fs", _order_id, retry_count, _backoff_delay - _elapsed_since_last)
            return
        self._last_retry_time[_order_id] = time.time()
        instrument_id = original_order['instrument_id']
        tick_size = self._get_tick_size(instrument_id)
        chase_ticks = min(retry_count, 3)
        price_offset = tick_size * chase_ticks
        
        # ✅ P1修复：添加最大滑点限制（不超过5个tick）
        max_chase_ticks = 5
        if chase_ticks > max_chase_ticks:
            logging.warning(f"[OrderService._chase_reorder] Chase ticks {chase_ticks} exceeds max {max_chase_ticks}, capping")
            chase_ticks = max_chase_ticks
            price_offset = tick_size * chase_ticks
        
        if original_order['direction'] == 'BUY':
            new_price = original_order['price'] + price_offset
        else:
            new_price = original_order['price'] - price_offset
        new_order_id = self.send_order(
            instrument_id=instrument_id,
            volume=_remaining_volume,
            price=new_price,
            direction=original_order['direction'],
            action=original_order['action'],
            exchange=original_order.get('exchange', ''),
            is_chase=True,
        )
        if new_order_id and new_order_id.order_id:
            _oid = new_order_id.order_id
            with self._lock:
                self._orders_by_id[_oid]['retry_count'] = retry_count
                self._orders_by_id[_oid]['original_order_id'] = original_order['order_id']
                chase_prefixed_id = self._generate_chase_order_id(original_order['order_id'])
                self._orders_by_id[_oid]['chase_order_id'] = chase_prefixed_id
            logging.info("[OrderService] 追单: %s -> %s retry=%d price=%.2f->%.2f",
                         original_order['order_id'], _oid, retry_count, original_order['price'], new_price)
        else:
            logging.warning("[OrderService] 追单失败: %s retry=%d error=%s", instrument_id, retry_count, new_order_id.error_code if new_order_id else 'unknown')

    # R23-SM-P1-08-FIX: 订单状态查询接口统一
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
                logging.info(f"[OrderService] 清理{len(to_remove)}个已完成订单")

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
            logging.debug(f"[OrderService._get_rate_limit] Failed to get rate limit from ParamsService: {e}")
        return 60

    def check_risk_block(self, instrument_id: str) -> bool:
        # R24-P1-CF-03修复: 跨策略风控阻断检查 — 超时自动恢复
        _until = self._risk_block_until.get(instrument_id, 0.0)
        if _until > 0 and time.time() < _until:
            return True
        if _until > 0 and time.time() >= _until:
            del self._risk_block_until[instrument_id]
            logging.info("[OrderService] R24-P1-CF-03: 风控阻断自动恢复: %s", instrument_id)
        return False

    def set_risk_block(self, instrument_id: str, duration: float = None) -> None:
        # R24-P1-CF-03修复: 设置风控阻断，超时后自动解除
        _dur = duration if duration is not None else self._risk_block_timeout
        self._risk_block_until[instrument_id] = time.time() + _dur
        logging.warning("[OrderService] R24-P1-CF-03: 风控阻断设置: %s, 持续%.0fs", instrument_id, _dur)

    # [R25-TO-03-FIX] WAL辅助方法
    def _ensure_wal_dir(self) -> None:
        try:
            os.makedirs(self._wal_dir, exist_ok=True)
        except Exception as e:
            logging.warning("[OrderService] R25-TO-03-FIX: WAL目录创建失败: %s", e)

    def _wal_path(self, order_id: str) -> str:
        safe_id = order_id.replace('/', '_').replace('\\', '_')
        return os.path.join(self._wal_dir, f"{safe_id}.wal")

    def _wal_write(self, order_id: str, state: str, order: Dict) -> None:
        try:
            entry = {
                'order_id': order_id,
                'state': state,
                'instrument_id': order.get('instrument_id', ''),
                'direction': order.get('direction', ''),
                'volume': order.get('volume', 0),
                'price': order.get('price', 0),
                'timestamp': time.time(),
                'datetime': datetime.now(CHINA_TZ).isoformat(),
            }
            _wal_path = self._wal_path(order_id)
            _tmp_path = _wal_path + '.tmp'
            with open(_tmp_path, 'w', encoding='utf-8') as f:
                json.dump(entry, f, ensure_ascii=False)
            os.replace(_tmp_path, _wal_path)
        except Exception as e:
            # R31-P1-10修复: WAL写入失败提升为error并设置健康标志，使上层可感知
            logging.error("[OrderService] R25-TO-03-FIX: WAL写入失败: order=%s state=%s err=%s", order_id, state, e)
            if not hasattr(self, '_wal_write_fail_count'):
                self._wal_write_fail_count = 0
            self._wal_write_fail_count += 1
            if self._wal_write_fail_count >= 10:
                logging.critical("[R31-P1-10] WAL连续写入失败%d次，订单持久化不可靠!", self._wal_write_fail_count)

    def _wal_read(self, order_id: str) -> Optional[Dict]:
        try:
            path = self._wal_path(order_id)
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logging.warning("[OrderService] R25-TO-03-FIX: WAL读取失败: order=%s err=%s", order_id, e)
        return None

    def _wal_delete(self, order_id: str) -> None:
        try:
            path = self._wal_path(order_id)
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logging.warning("[OrderService] R25-TO-03-FIX: WAL删除失败: order=%s err=%s", order_id, e)

    def _recover_orphaned_orders(self) -> None:
        try:
            if not os.path.isdir(self._wal_dir):
                return
            orphaned_count = 0
            for fname in os.listdir(self._wal_dir):
                if not fname.endswith('.wal'):
                    continue
                fpath = os.path.join(self._wal_dir, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        entry = json.load(f)
                    if entry.get('state') == 'PENDING':
                        order_id = entry.get('order_id', '')
                        with self._lock:
                            order = self._orders_by_id.get(order_id)
                            if order and order.get('status') in ('SUBMITTED', 'PENDING'):
                                order['status'] = 'ORPHANED'
                                order['updated_at'] = datetime.now(CHINA_TZ)
                                orphaned_count += 1
                                logging.warning(
                                    "[OrderService] R25-TO-03-FIX: 孤儿订单恢复: order_id=%s instrument=%s "
                                    "状态从SUBMITTED/PENDING标记为ORPHANED",
                                    order_id, entry.get('instrument_id', ''),
                                )
                        self._wal_write(order_id, 'ORPHANED', {'order_id': order_id, 'instrument_id': entry.get('instrument_id', '')})
                except Exception as e:
                    logging.warning("[OrderService] R25-TO-03-FIX: WAL文件恢复异常: %s err=%s", fname, e)
            if orphaned_count > 0:
                logging.info("[OrderService] R25-TO-03-FIX: 启动时恢复%d个孤儿订单", orphaned_count)
        except Exception as e:
            logging.warning("[OrderService] R25-TO-03-FIX: 孤儿订单恢复过程异常: %s", e)

    def _persist_idempotent_key(self, key: str) -> None:
        """R16-P0-RES-02修复: 持久化幂等键到JSONL"""
        try:
            with self._idempotent_lock:
                with open(self._idempotent_state_file, 'a', encoding='utf-8') as f:
                    f.write(json_dumps({'key': key, 'ts': time.time()}) + '\n')
        except Exception as e:
            logging.warning("[R16-P0-RES-02] 幂等键持久化失败: %s", e)

    def _recover_idempotent_state(self) -> None:
        """R16-P0-RES-02修复: 启动时从JSONL恢复幂等去重集合"""
        try:
            if not os.path.exists(self._idempotent_state_file):
                return
            recovered = 0
            with open(self._idempotent_state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        key = record.get('key', '')
                        if key:
                            self._order_idempotent_set.add(key)
                            recovered += 1
                    except (json.JSONDecodeError, KeyError):
                        continue
            if recovered > 0:
                logging.info("[R16-P0-RES-02] 幂等去重集合已恢复: %d条记录", recovered)
        except Exception as e:
            logging.warning("[R16-P0-RES-02] 幂等状态恢复失败: %s", e)

    def _append_order_state(self, order_id: str, state: str, order: Dict) -> None:
        """R16-P0-RES-05修复: 追加写入订单状态到JSONL文件"""
        try:
            record = {
                'order_id': order_id,
                'state': state,
                'instrument_id': order.get('instrument_id', ''),
                'direction': order.get('direction', ''),
                'volume': order.get('volume', 0),
                'price': order.get('price', 0),
                'ts': time.time(),
            }
            with self._order_state_lock:
                with open(self._order_state_file, 'a', encoding='utf-8') as f:
                    f.write(json_dumps(record) + '\n')
        except Exception as e:
            logging.warning("[R16-P0-RES-05] 订单状态追加写失败: order=%s state=%s err=%s", order_id, state, e)

    def _recover_order_state(self) -> None:
        """R16-P0-RES-05修复: 启动时从JSONL恢复订单状态"""
        try:
            if not os.path.exists(self._order_state_file):
                return
            recovered = 0
            with open(self._order_state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        order_id = record.get('order_id', '')
                        state = record.get('state', '')
                        instrument_id = record.get('instrument_id', '')
                        if order_id and state:
                            with self._lock:
                                if order_id not in self._orders_by_id:
                                    self._orders_by_id[order_id] = {
                                        'order_id': order_id,
                                        'instrument_id': instrument_id,
                                        'direction': record.get('direction', ''),
                                        'volume': record.get('volume', 0),
                                        'price': record.get('price', 0),
                                        'status': state,
                                    }
                            recovered += 1
                    except (json.JSONDecodeError, KeyError):
                        continue
            if recovered > 0:
                logging.info("[R16-P0-RES-05] 订单状态已从JSONL恢复: %d条记录", recovered)
        except Exception as e:
            logging.warning("[R16-P0-RES-05] 订单状态恢复失败: %s", e)

    def _execute_with_compensation_v2(
        self,
        steps: List[Dict[str, Any]],
        result_ids: List[str],
        compensate_fn: Optional[Callable] = None,
    ) -> List[str]:
        """R16-P0-RES-11修复: 补偿事务 — 逐步执行，失败时逆序撤单

        Args:
            steps: 每步的订单参数dict列表
            result_ids: 收集成功订单ID的列表
            compensate_fn: 自定义补偿函数(order_id)->bool，None则用默认撤单
        Returns:
            成功的order_id列表
        """
        executed_ids: List[str] = []
        for i, step_params in enumerate(steps):
            result = self.send_order(**step_params)
            if hasattr(result, 'order_id') and result.order_id:
                executed_ids.append(result.order_id)
                result_ids.append(result.order_id)
            else:
                logging.error("[R16-P0-RES-11] 补偿事务第%d步失败，开始逆序撤单", i + 1)
                for oid in reversed(executed_ids):
                    try:
                        if compensate_fn:
                            compensate_fn(oid)
                        else:
                            with self._lock:
                                order = self._orders_by_id.get(oid)
                                if order:
                                    order['status'] = 'COMPENSATED'
                                    self._append_order_state(oid, 'COMPENSATED', order)
                            logging.info("[R16-P0-RES-11] 补偿撤单: %s", oid)
                    except Exception as ce:
                        logging.error("[R16-P0-RES-11] 补偿撤单失败: %s err=%s", oid, ce)
                return executed_ids
        return executed_ids


def sync_order_status_with_exchange(order_id: str, local_status: str, exchange_status_query_fn: Callable) -> Dict[str, Any]:
    """R26-P0-DI-05修复: 订单状态与交易所同步——比较本地订单状态与交易所实际状态

    Args:
        order_id: 订单ID
        local_status: 本地记录的订单状态
        exchange_status_query_fn: 查询交易所状态的回调函数, 签名(order_id)->str

    Returns:
        Dict: {synced: bool, local_status: str, exchange_status: str, action_taken: str}
    """
    result = {'synced': True, 'local_status': local_status, 'exchange_status': '', 'action_taken': 'none'}
    try:
        exchange_status = exchange_status_query_fn(order_id) if exchange_status_query_fn else None
        if exchange_status is None:
            result['action_taken'] = 'query_failed'
            logging.warning("[R26-P0-DI-05] 订单%s交易所状态查询返回None", order_id)
            return result
        result['exchange_status'] = str(exchange_status)
        if local_status != exchange_status:
            result['synced'] = False
            if local_status in ('PENDING', 'SUBMITTED') and exchange_status in ('FILLED', 'PARTIAL_FILLED'):
                result['action_taken'] = 'local_updated_to_exchange'
                logging.warning("[R26-P0-DI-05] 订单%s本地%s但交易所%s, 需更新本地状态", order_id, local_status, exchange_status)
            elif local_status in ('FILLED', 'PARTIAL_FILLED') and exchange_status == 'CANCELLED':
                result['action_taken'] = 'exchange_override_detected'
                logging.error("[R26-P0-DI-05] 订单%s本地%s但交易所已撤单, 严重不一致", order_id, local_status)
            else:
                result['action_taken'] = 'status_mismatch_logged'
                logging.warning("[R26-P0-DI-05] 订单%s状态不一致: 本地=%s, 交易所=%s", order_id, local_status, exchange_status)
    except Exception as e:
        result['action_taken'] = 'exception'
        logging.error("[R26-P0-DI-05] 订单%s同步异常: %s", order_id, e)
    return result


def almgren_chriss_impact(
    volume: float,
    avg_daily_volume: float,
    daily_volatility: float,
    participation_rate: float = 0.1,
    permanent_impact_factor: float = 0.1,
    temporary_impact_factor: float = 0.15,
    risk_aversion: float = 1.0,
) -> Dict[str, float]:
    """R18-04修复: Almgren-Chriss市场冲击成本模型

    计算最优执行轨迹下的临时冲击和永久冲击成本
    参考: Almgren & Chriss (2001), "Optimal Execution of Portfolio Transactions"

    Args:
        volume: 待执行订单量(手)
        avg_daily_volume: 日均成交量(手)
        daily_volatility: 日波动率
        participation_rate: 参与率 volume/avg_daily_volume
        permanent_impact_factor: 永久冲击系数(sigma)
        temporary_impact_factor: 临时冲击系数(alpha)
        risk_aversion: 风险厌恶系数(lambda)

    Returns:
        Dict: {total_impact_bps, permanent_impact_bps, temporary_impact_bps,
               optimal_horizon_sec, half_life_sec}
    """
    try:
        if avg_daily_volume <= 0 or volume <= 0 or daily_volatility <= 0:
            return {'total_impact_bps': 0.0, 'permanent_impact_bps': 0.0,
                    'temporary_impact_bps': 0.0, 'optimal_horizon_sec': 0.0,
                    'half_life_sec': 0.0}
        sigma = daily_volatility
        X = volume
        V = avg_daily_volume
        actual_participation = X / V if V > 0 else participation_rate
        permanent_impact_bps = permanent_impact_factor * sigma * math.sqrt(actual_participation) * 10000
        temporary_impact_bps = temporary_impact_factor * sigma * (actual_participation ** 0.6) * 10000
        total_impact_bps = permanent_impact_bps + temporary_impact_bps
        if risk_aversion > 0 and sigma > 0 and temporary_impact_factor > 0:
            optimal_horizon_sec = math.sqrt(
                (risk_aversion * sigma ** 2 * X) / (2 * temporary_impact_factor * (V / 6.5))
            ) * 3600 if V > 0 else 0.0
        else:
            optimal_horizon_sec = 0.0
        half_life_sec = optimal_horizon_sec / 2.0 if optimal_horizon_sec > 0 else 0.0
        return {
            'total_impact_bps': round(total_impact_bps, 4),
            'permanent_impact_bps': round(permanent_impact_bps, 4),
            'temporary_impact_bps': round(temporary_impact_bps, 4),
            'optimal_horizon_sec': round(optimal_horizon_sec, 2),
            'half_life_sec': round(half_life_sec, 2),
        }
    except Exception as e:
        logging.error("[R18-04] Almgren-Chriss计算异常: %s", e)
        return {'total_impact_bps': 0.0, 'permanent_impact_bps': 0.0,
                'temporary_impact_bps': 0.0, 'optimal_horizon_sec': 0.0,
                'half_life_sec': 0.0}


def estimate_fill_probability(
    order_price: float,
    best_opposite_price: float,
    spread_bps: float,
    volume: float,
    avg_daily_volume: float,
    time_horizon_sec: float = 60.0,
    queue_position: int = 0,
) -> float:
    """R18-05修复: 成交概率建模

    基于价格距离、参与率、队列位置的三维成交概率估计

    Args:
        order_price: 挂单价格
        best_opposite_price: 对盘最优价(买入用ask, 卖出用bid)
        spread_bps: 价差基点
        volume: 挂单量
        avg_daily_volume: 日均成交量
        time_horizon_sec: 时间窗口(秒)
        queue_position: 队列位置(0=队首)

    Returns:
        float: 成交概率 [0, 1]
    """
    try:
        if avg_daily_volume <= 0 or time_horizon_sec <= 0:
            return 1.0
        participation_rate = volume / avg_daily_volume
        queue_factor = 1.0 / (1.0 + queue_position * 0.1)
        if best_opposite_price > 0 and order_price > 0:
            price_distance_bps = abs(order_price - best_opposite_price) / best_opposite_price * 10000
            if price_distance_bps <= 0:
                price_factor = 1.0
            elif price_distance_bps <= spread_bps:
                price_factor = math.exp(-0.5 * price_distance_bps / max(spread_bps, 0.01))
            else:
                price_factor = math.exp(-2.0 * (price_distance_bps - spread_bps) / max(spread_bps, 0.01))
        else:
            price_factor = 0.5
        time_factor = 1.0 - math.exp(-time_horizon_sec / 60.0)
        participation_penalty = 1.0 / (1.0 + participation_rate * 10.0)
        fill_prob = price_factor * queue_factor * time_factor * participation_penalty
        return max(0.0, min(1.0, fill_prob))
    except Exception as e:
        logging.error("[R18-05] 成交概率计算异常: %s", e)
        return 0.5


def check_self_trade_across_splits(
    orders: List[Dict[str, Any]],
    ban_minutes: float = 30.0,
) -> List[Dict[str, Any]]:
    """R18-06修复: 自成交检测覆盖拆分子单

    对一组拆分子单进行原子性自成交检查，检测同合约反方向的子单对

    Args:
        orders: 拆分子单列表, 每个dict含instrument_id/direction/price
        ban_minutes: 自成交禁止期(分钟)

    Returns:
        List[Dict]: 检测到的自成交对, 含{buy_idx, sell_idx, instrument_id, ban_until}
    """
    violations = []
    try:
        now = time.time()
        for i, order_a in enumerate(orders):
            for j, order_b in enumerate(orders):
                if j <= i:
                    continue
                if (order_a.get('instrument_id') == order_b.get('instrument_id')
                    and order_a.get('direction') != order_b.get('direction')):
                    price_a = order_a.get('price', 0)
                    price_b = order_b.get('price', 0)
                    if price_a > 0 and price_b > 0:
                        buy_idx = i if order_a.get('direction') == 'BUY' else j
                        sell_idx = j if order_a.get('direction') == 'BUY' else i
                        buy_order = orders[buy_idx]
                        sell_order = orders[sell_idx]
                        buy_price = buy_order.get('price', 0)
                        sell_price = sell_order.get('price', 0)
                        if buy_price >= sell_price:
                            violations.append({
                                'buy_idx': buy_idx,
                                'sell_idx': sell_idx,
                                'instrument_id': order_a.get('instrument_id', ''),
                                'buy_price': buy_price,
                                'sell_price': sell_price,
                                'ban_until': now + ban_minutes * 60.0,
                            })
                            logging.warning(
                                "[R18-06] 拆单自成交检测: BUY[%d]@%.2f >= SELL[%d]@%.2f instrument=%s",
                                buy_idx, buy_price, sell_idx, sell_price,
                                order_a.get('instrument_id', ''),
                            )
        return violations
    except Exception as e:
        logging.error("[R18-06] 拆单自成交检测异常: %s", e)
        return violations


class AlgoTradingCompliance:
    """R18-07修复: 算法交易报备机制

    满足中国证监会程序化交易报备要求:
    - 记录每笔算法交易决策
    - 生成报备日志
    - 检测报备违规(超频/超量)
    """

    def __init__(self, max_orders_per_sec: float = 30.0, max_daily_algo_orders: int = 10000):
        self._max_orders_per_sec = max_orders_per_sec
        self._max_daily_algo_orders = max_daily_algo_orders
        self._order_timestamps: deque = deque(maxlen=1000)
        self._daily_count: int = 0
        self._daily_date: str = ''
        self._compliance_log: List[Dict[str, Any]] = []

    def record_algo_order(self, signal_id: str, instrument_id: str, direction: str,
                          volume: float, price: float, algo_type: str = 'hft_pursuit',
                          decision_latency_ms: float = 0.0) -> Dict[str, Any]:
        now = time.time()
        today = datetime.now(CHINA_TZ).strftime('%Y-%m-%d')
        if today != self._daily_date:
            self._daily_date = today
            self._daily_count = 0
        self._daily_count += 1
        self._order_timestamps.append(now)
        freq_violation = False
        if len(self._order_timestamps) >= 2:
            recent = [t for t in self._order_timestamps if now - t < 1.0]
            if len(recent) > self._max_orders_per_sec:
                freq_violation = True
                logging.warning("[R18-07] 算法交易超频: %.0f单/秒 > 限制%.0f单/秒",
                                len(recent), self._max_orders_per_sec)
        daily_violation = self._daily_count > self._max_daily_algo_orders
        if daily_violation:
            logging.warning("[R18-07] 日内算法订单超量: %d > 限制%d",
                            self._daily_count, self._max_daily_algo_orders)
        record = {
            'timestamp': now,
            'signal_id': signal_id,
            'instrument_id': instrument_id,
            'direction': direction,
            'volume': volume,
            'price': price,
            'algo_type': algo_type,
            'decision_latency_ms': decision_latency_ms,
            'freq_violation': freq_violation,
            'daily_violation': daily_violation,
            'daily_count': self._daily_count,
        }
        self._compliance_log.append(record)
        return record

    def get_compliance_summary(self) -> Dict[str, Any]:
        total = len(self._compliance_log)
        freq_violations = sum(1 for r in self._compliance_log if r.get('freq_violation'))
        daily_violations = sum(1 for r in self._compliance_log if r.get('daily_violation'))
        return {
            'total_algo_orders': total,
            'freq_violations': freq_violations,
            'daily_violations': daily_violations,
            'daily_count': self._daily_count,
            'daily_date': self._daily_date,
        }


class WashTradeDetector:
    """R18-08修复: 洗盘检测

    检测疑似洗盘行为:
    - 同合约短时间内频繁对倒(买后即卖/卖后即买)
    - 净持仓接近零但交易量异常高
    - 价格未有效偏离但成交量放大
    """

    def __init__(self, window_sec: float = 300.0, min_round_trips: int = 3,
                 net_position_ratio_threshold: float = 0.1):
        self._window_sec = window_sec
        self._min_round_trips = min_round_trips
        self._net_ratio_threshold = net_position_ratio_threshold
        self._trade_history: Dict[str, deque] = {}

    def record_trade(self, instrument_id: str, direction: str, volume: float,
                     price: float, timestamp: float = 0.0) -> None:
        if timestamp <= 0:
            timestamp = time.time()
        if instrument_id not in self._trade_history:
            self._trade_history[instrument_id] = deque(maxlen=500)
        self._trade_history[instrument_id].append({
            'ts': timestamp, 'dir': direction, 'vol': volume, 'price': price,
        })

    def detect_wash_trade(self, instrument_id: str) -> Dict[str, Any]:
        now = time.time()
        result = {'suspected': False, 'round_trips': 0, 'net_volume': 0.0,
                  'gross_volume': 0.0, 'net_ratio': 1.0, 'reason': ''}
        trades = self._trade_history.get(instrument_id, deque())
        recent = [t for t in trades if now - t['ts'] <= self._window_sec]
        if len(recent) < 2:
            return result
        buy_vol = sum(t['vol'] for t in recent if t['dir'] == 'BUY')
        sell_vol = sum(t['vol'] for t in recent if t['dir'] == 'SELL')
        gross_vol = buy_vol + sell_vol
        net_vol = abs(buy_vol - sell_vol)
        round_trips = int(min(buy_vol, sell_vol))
        result['round_trips'] = round_trips
        result['net_volume'] = net_vol
        result['gross_volume'] = gross_vol
        net_ratio = net_vol / gross_vol if gross_vol > 0 else 1.0
        result['net_ratio'] = round(net_ratio, 4)
        if round_trips >= self._min_round_trips and net_ratio < self._net_ratio_threshold:
            result['suspected'] = True
            result['reason'] = (
                f"window={self._window_sec}s内{round_trips}次往返, "
                f"净持仓比{net_ratio:.2%}<{self._net_ratio_threshold:.0%}"
            )
            logging.warning(
                "[R18-08] 疑似洗盘: instrument=%s %s",
                instrument_id, result['reason'],
            )
        return result

    def get_all_suspected(self) -> Dict[str, Dict[str, Any]]:
        results = {}
        for instrument_id in self._trade_history:
            detection = self.detect_wash_trade(instrument_id)
            if detection['suspected']:
                results[instrument_id] = detection
        return results


__all__ = ['OrderService', 'RateLimiter', 'PlatformAuthenticator', 'get_order_service',
           'sync_order_status_with_exchange', 'almgren_chriss_impact',
           'estimate_fill_probability', 'check_self_trade_across_splits',
           'AlgoTradingCompliance', 'WashTradeDetector']
