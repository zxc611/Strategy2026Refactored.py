# [M1-42] ________Facade

# MODULE_ID: M1-141

"""

订单服务模块 - Facade (CQRS Command）

瘦身身 仅保留初始化+委托给re-export

"""

from __future__ import annotations

import logging, time, uuid

from typing import Any, Callable, Dict, List, Optional

from ali2026v3_trading.order.order_base import (

    OrderResult, BaseService, OrderQueryService, OrderQueryMixin, _validate_order_status_transition, _mask_id,

    _VALID_ORDER_TRANSITIONS, _HAS_CAUSAL_CHAIN, get_order_service, reset_order_service, init_order_service_attrs)

from ali2026v3_trading.infra.shared_utils import CHINA_TZ, TradeAction, TradeDirection, VALID_TRADE_ACTIONS, VALID_TRADE_DIRECTIONS



_WAL_DELEGATES = {'_ensure_wal_dir','_wal_path','_wal_write','_wal_read','_wal_delete',

    '_recover_orphaned_orders','_persist_idempotent_key','_recover_idempotent_state',

    '_rotate_jsonl_if_needed','_append_order_state','_recover_order_state','_execute_with_compensation_v2'}

_RISK_NAME_MAP = {'_compute_pnl_correlation':'compute_pnl_correlation','_check_cross_strategy_risk':'check_cross_strategy_risk',

    '_correct_price':'correct_price','_get_tick_size':'get_tick_size','_release_margin_reservation':'release_margin_reservation',

    'check_risk_block':'check_risk_block','set_risk_block':'set_risk_block'}



class OrderService(BaseService):

    def __init__(self, event_bus=None, params=None):

        super().__init__(event_bus); init_order_service_attrs(self, params)

        self._query_service = OrderQueryService(self)

    _PLATFORM_IDEMPOTENCY_FIELDS = ('client_order_id', 'request_id', 'order_id')



    def __getattr__(self, name):

        # 递归保护: 防止损损query_service 双向委托导致无限递归

        # FIX-20260713-THREADSAFE-GETATTR

        _local = self.__dict__.get('_getattr_tl')

        if _local is None:

            import threading as _t

            _local = _t.local()

            self.__dict__['_getattr_tl'] = _local

        if getattr(_local, 'recursing', False):

            raise AttributeError(name)

        _local.recursing = True

        try:

            _qs = self.__dict__.get('_query_service')

            if _qs is not None:

                try:

                    return getattr(_qs, name)

                except AttributeError:

                    pass

            if name in _WAL_DELEGATES:

                return getattr(self.__dict__.get('_wal_state_service'), name)

            mapped = _RISK_NAME_MAP.get(name)

            if mapped:

                return getattr(self.__dict__.get('_risk_guard'), mapped)

            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

        finally:

            _local.recursing = False



    # ── 委托给OrderExecutor ──

    def send_order(self, instrument_id: str, volume: float, price: float, direction: str = 'BUY',

                   action: str = 'OPEN', exchange: str = '', priority: str = 'NORMAL', is_chase: bool = False,

                   signal_id: str = '', expected_position_count: int = -2, open_reason: str = '',

                   decision_score: float = 0.0, position_scale: float = 1.0, decision_action: str = '',

                   dimension_scores: Optional[Dict[str, float]] = None,

                   dimension_weights: Optional[Dict[str, float]] = None,

                   ref_price: float = 0.0) -> OrderResult:

        from ali2026v3_trading.order.order_executor import OrderExecutor, OrderContext

        return OrderExecutor(self).execute(OrderContext(

            instrument_id=instrument_id, volume=volume, price=price, direction=direction, action=action,

            exchange=exchange, priority=priority, is_chase=is_chase, signal_id=signal_id,

            expected_position_count=expected_position_count, open_reason=open_reason, decision_score=decision_score,

            position_scale=position_scale, decision_action=decision_action, dimension_scores=dimension_scores, dimension_weights=dimension_weights,

            ref_price=ref_price))

    def send_order_split(self, instrument_id, volume, price, direction='BUY', action='OPEN', exchange='',

                         signal_strength=1.0, bids=None, asks=None, open_reason='', signal_id=''):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self).send_order_split(instrument_id=instrument_id, volume=volume, price=price,

            direction=direction, action=action, exchange=exchange, signal_strength=signal_strength, bids=bids, asks=asks, open_reason=open_reason, signal_id=signal_id)

    def enable_hft_enhancements(self):

        """启用HFT增强功能（标记）"""

        self._hft_enabled = True

    def send_defensive_order(self, instrument_id, volume, price, direction='BUY', action='CLOSE',

                             signal_strength=1.0, **kwargs):

        """发送防御性订单（做市商防御）"""

        from ali2026v3_trading.order.order_flow_bridge import MarketMakerDefenseEngine

        defense = MarketMakerDefenseEngine()

        orders = defense.create_defensive_order(

            instrument_id=instrument_id, direction=direction, volume=volume,

            price=price, signal_strength=signal_strength, tick_size=0.2,

        )

        _def_ref_price = kwargs.get('ref_price', 0.0)

        order_ids = []

        for o in orders:

            oid = self.send_order(

                instrument_id=o.instrument_id,

                volume=o.volume,

                price=o.price,

                direction=o.direction,

                action=action,

                ref_price=_def_ref_price,

            )

            if oid:

                order_ids.append(oid)

        return order_ids

    def _plan_volume_split(self, volume, price, direction, bids, asks, signal_strength=1.0):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self)._plan_volume_split(volume=volume, price=price, direction=direction, bids=bids, asks=asks, signal_strength=signal_strength)

    def execute_by_ranking(self, targets, direction='BUY', action='OPEN'):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self).execute_by_ranking(targets, direction=direction, action=action)

    def bind_platform_apis(self, insert_order_func, cancel_order_func):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self).bind_platform_apis(insert_order_func, cancel_order_func)

    def _build_platform_insert_params(self, *, order_id, instrument_id, exchange, volume, price, direction, action):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self)._build_platform_insert_params(order_id=order_id, instrument_id=instrument_id, exchange=exchange, volume=volume, price=price, direction=direction, action=action)

    def _invoke_platform_insert_with_timeout(self, filtered_params):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self)._invoke_platform_insert_with_timeout(filtered_params)

    def _invoke_platform_cancel_with_timeout(self, platform_id):

        from ali2026v3_trading.order.order_executor import OrderExecutor

        return OrderExecutor(self)._invoke_platform_cancel_with_timeout(platform_id)

    # ── 委托给OrderStateManager ──

    def on_trade_update(self, trade_data): return self._state_manager.on_trade_update(self, trade_data)

    def scan_order_timeouts(self): return self._state_manager.scan_order_timeouts(self)

    def check_pending_orders(self): return self._state_manager.check_pending_orders_full(self)

    def _cleanup_orders(self): return self._state_manager._cleanup_orders(self)

    # ── 需要self参数的risk/wal特殊委托 ──

    def _is_duplicate_order(self, instrument_id, order_key): return self._risk_guard.is_duplicate_order(self, instrument_id, order_key)

    def _get_last_market_price(self, instrument_id): return self._risk_guard.get_last_market_price(instrument_id, self._orders_by_id)

    def _estimate_slippage(self, instrument_id, price, volume, bid_ask_spread=0.0, days_to_expiry=999, spread_quality=1):

        return self._risk_guard.estimate_slippage(instrument_id, price, volume, bid_ask_spread, days_to_expiry, spread_quality)

    def _remove_order_and_idempotent_key(self, order_id, order): return self._wal_state_service.remove_order_and_idempotent_key(self, order_id, order)

    # ── 委托给OrderChaseService ──

    def cancel_order(self, order_id): return self._chase_service.cancel_order(order_id)

    def cancel_all_pending(self): return self._chase_service.cancel_all_pending()

    def emergency_close_all_positions(self, caller_id="unknown"): return self._chase_service.emergency_close_all_positions(caller_id)

    def _chase_reorder(self, original_order, retry_count): self._chase_service._chase_reorder(original_order, retry_count)



from ali2026v3_trading.infra.event_bus import RateLimiter

from ali2026v3_trading.order.order_platform_auth import PlatformAuthenticator

from ali2026v3_trading.order.order_sync import sync_order_status_with_exchange

from ali2026v3_trading.order.order_split_models import OrderSplitStrategy, SplitOrderResult, SmartOrderSplitter, PlanOrderSplitResult

from ali2026v3_trading.order.order_market_impact import almgren_chriss_impact, estimate_fill_probability

from ali2026v3_trading.order.order_compliance import check_self_trade_across_splits, AlgoTradingCompliance, WashTradeDetector

_OPEN_REASON_CODES = frozenset({'BOX_SPRING', 'CORRECT_RESONANCE', 'CORRECT_DIVERGENCE', 'SHADOW_A_REVERSAL', 'SHADOW_B_RANDOM', 'OTHER_SCALP', 'BOX_EXTREME', 'ARBITRAGE', 'HFT_TICK_CONFIRM'})

__all__ = ['OrderService', 'OrderResult', 'RateLimiter', 'PlatformAuthenticator', 'get_order_service',

           'reset_order_service', '_validate_order_status_transition', '_VALID_ORDER_TRANSITIONS',

           '_HAS_CAUSAL_CHAIN', '_mask_id', 'BaseService',

           'sync_order_status_with_exchange', 'almgren_chriss_impact',

           'estimate_fill_probability', 'check_self_trade_across_splits',

           'AlgoTradingCompliance', 'WashTradeDetector',

           'SmartOrderSplitter', 'OrderSplitStrategy', 'SplitOrderResult', 'PlanOrderSplitResult', '_OPEN_REASON_CODES']

