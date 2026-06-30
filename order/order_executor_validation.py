# [M1-46] ____ִ____

# MODULE_ID: M1-134

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""

OrderExecutor _Phase1-Sprint1: send_order三阶段流水线拆解

从order_service.py的OrderService.send_order(329_拆分析

  1. _pre_send_checks: 前置校验(断路器胖手动TOCTOU+自成交幂等去重)

  2. _execute_platform_insert: 平台下单(构建参数+超时调用+结果归一。

  3. _post_send_persist: 后置持久化WAL写入+幂等记录+状态更新

原OrderService.send_order改为编排序列0_，委托到OrderExecutor

"""

from __future__ import annotations



import logging

import time

import math

import threading

from datetime import datetime

from typing import Any, Callable, Dict, List, Optional

from dataclasses import dataclass, field



from ali2026v3_trading.infra.shared_utils import CHINA_TZ

from ali2026v3_trading.order.order_base import OrderResult

from ali2026v3_trading.infra.shared_utils import TradeAction, TradeDirection, VALID_TRADE_DIRECTIONS



try:

    from ali2026v3_trading.strategy_judgment.causal_chain_utils import CyclicDependencyGuard

    _HAS_CAUSAL_CHAIN = True

except ImportError:

    _HAS_CAUSAL_CHAIN = False





@dataclass

class OrderContext:

    instrument_id: str = ''

    volume: float = 0.0

    price: float = 0.0

    direction: str = 'BUY'

    action: str = 'OPEN'

    exchange: str = ''

    priority: str = 'NORMAL'

    is_chase: bool = False

    signal_id: str = ''

    expected_position_count: int = -2

    open_reason: str = ''

    decision_score: float = 0.0

    position_scale: float = 1.0

    decision_action: str = ''

    dimension_scores: Optional[Dict[str, float]] = None

    dimension_weights: Optional[Dict[str, float]] = None

    idempotent_key: str = ''

    order_id: str = ''

    order: Dict[str, Any] = field(default_factory=dict)

    rejected: bool = False

    reject_code: str = ''

    reject_message: str = ''

    _order_submit_start_ts: float = 0.0

    _cyclic_guard: Any = None

    ref_price: float = 0.0







class OrderExecutor:

    """send_order三阶段流水线执行行



    阶段1 _pre_send_checks: 断路器胖手动TOCTOU+自成交幂等去重 (~80_

    阶段2 _execute_platform_insert: 构建参数+超时调用+结果归一（(~60_

    阶段3 _post_send_persist: WAL写入+幂等记录+状态更新(~50_

    """



    def __init__(self, order_service: Any):

        self._svc = order_service



    def execute(self, ctx: OrderContext) -> OrderResult:

        """编排序 三阶段流水线，≤30行，圈复杂度量"""

        ctx = self._pre_send_checks(ctx)

        if ctx.rejected:

            # FIX-R31-CYCLIC-LEAK: 订单被拒绝时必须调用exit，否则order_send_order留在调用栈中
            # 导致后续所有send_order都被循环依赖检测拦截（enter返回False）
            if ctx._cyclic_guard:
                ctx._cyclic_guard.exit("order_send_order")

            return OrderResult.fail(ctx.reject_code, ctx.reject_message)

        ctx = self._execute_platform_insert(ctx)

        if ctx.rejected:

            # FIX-R31-CYCLIC-LEAK: 同上，_execute_platform_insert的拒绝路径也需要exit
            if ctx._cyclic_guard:
                ctx._cyclic_guard.exit("order_send_order")

            return OrderResult.fail(ctx.reject_code, ctx.reject_message)

        return self._post_send_persist(ctx)



    def _pre_send_checks(self, ctx: OrderContext) -> OrderContext:

        """阶段1: 前置校验 _断路器胖手动TOCTOU+自成交幂等去重"""

        svc = self._svc

        _op_type = 'close' if ctx.action in ('CLOSE', 'close') else 'open'

        _timeout = svc._operation_timeouts.get(_op_type, svc._operation_timeouts['default'])



        if svc._circuit_breaker_open:

            _elapsed = time.time() - svc._circuit_breaker_opened_at if svc._circuit_breaker_opened_at > 0 else 0

            if _elapsed >= svc._circuit_breaker_auto_recovery_sec:

                svc._circuit_breaker_half_open = True

                logging.info("[R22-P0-CB-02] 断路器超时自动恢复半开): 已过%.0f_=%.0f_, _elapsed", svc._circuit_breaker_auto_recovery_sec)
            elif svc._circuit_breaker_half_open:

                logging.info("[R22-P0-CB-02] 断路器半开状态 允许探测订单: %s %s", ctx.instrument_id, ctx.direction)

            else:

                logging.critical("[OrderService] 断路器已触发，拒绝下线 consecutive_failures=%d", svc._consecutive_failures)

                ctx.rejected = True

                ctx.reject_code = 'circuit_breaker'

                ctx.reject_message = f'断路器触发，连续{svc._consecutive_failures}次失败'

                return ctx



        if _HAS_CAUSAL_CHAIN:

            ctx._cyclic_guard = CyclicDependencyGuard.get_instance()

            if ctx._cyclic_guard and not ctx._cyclic_guard.enter("order_send_order"):


                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'cyclic_call', '循环依赖检查'

                return ctx



        if ctx.direction not in VALID_TRADE_DIRECTIONS:

            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'invalid_direction', f'direction={ctx.direction}非法'

            return ctx



        if not isinstance(ctx.price, (int, float)) or not math.isfinite(ctx.price) or ctx.price < 0:

            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'invalid_price', f'price={ctx.price}无效'

            return ctx



        _ref_price = ctx.ref_price if getattr(ctx, 'ref_price', 0.0) > 0 else None
        _ref_is_open_price_fallback = False
        if _ref_price is None or _ref_price <= 0:
            _ref_price = svc._get_last_market_price(ctx.instrument_id)
        else:
            _ref_is_open_price_fallback = True

        if _ref_price is not None and _ref_price > 0:

            _price_deviation = abs(ctx.price - _ref_price) / _ref_price

            _fat_finger_threshold = 0.03
            if ctx.action in ('CLOSE', 'close'):
                _fat_finger_threshold = 0.20
                if _ref_is_open_price_fallback:
                    _fat_finger_threshold = 0.50

            if _price_deviation > _fat_finger_threshold:

                logging.critical("[R14-P0-BIZ-09] 胖手指防护触发 %s deviation=%.2f%% threshold=%.0f%% action=%s ref_price=%.4f", ctx.instrument_id, _price_deviation * 100, _fat_finger_threshold * 100, ctx.action, _ref_price)

                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'fat_finger', f'价格偏离{_price_deviation:.2%}>涨跌停板{_fat_finger_threshold:.0%}'

                return ctx



        ctx.price = svc._correct_price(ctx.price, ctx.instrument_id)

        ctx._order_submit_start_ts = time.perf_counter()



        _trade_amount = ctx.volume * ctx.price

        if _trade_amount >= 5000000:

            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'amount_exceeded', f'单笔金额{_trade_amount:.0f}超限'

            return ctx



        if ctx.action != TradeAction.CLOSE and not ctx.is_chase:

            if not svc.rate_limiter.acquire():

                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'rate_limited', '下单频率超限'

                return ctx

            _order_key = f"{ctx.instrument_id}_{ctx.exchange}_{ctx.direction}_{ctx.action}_{ctx.volume}_{round(ctx.price, 4)}"

            if svc._is_duplicate_order(ctx.instrument_id, _order_key):

                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'duplicate', '重复订单'

                return ctx



        # FIX-R28: CLOSE订单的idempotent_key加入signal_id，区分同instrument不同持仓的平仓
        if ctx.action in ('CLOSE', 'close'):
            _sig_suffix = f"_{ctx.signal_id}" if ctx.signal_id else ""
            ctx.idempotent_key = f"{ctx.instrument_id}_{ctx.direction}_{ctx.action}_{ctx.volume}_{round(ctx.price, 4)}{_sig_suffix}"
        else:
            _open_sig_suffix = f"_{ctx.signal_id}" if ctx.signal_id else ""
            ctx.idempotent_key = f"{ctx.instrument_id}_{ctx.direction}_{ctx.action}_{ctx.volume}_{round(ctx.price, 4)}{_open_sig_suffix}"



        _now = time.time()

        # FIX-R29: CLOSE订单不受自成交禁止期限制（平仓方向与开仓相反是正常的）
        if ctx.action != TradeAction.CLOSE:
            _ban_key = f"{ctx.instrument_id}_{ctx.direction}"

            _ban_until = svc._self_trade_bans.get(_ban_key, 0.0)

            if _now < _ban_until:

                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'self_trade_ban', f'自成交禁止期剩余{(_ban_until - _now) / 60.0:.1f}分钟'

                return ctx



        _expired_keys = [k for k, t in svc._self_trade_bans.items() if _now >= t]

        for _ek in _expired_keys:

            del svc._self_trade_bans[_ek]



        # FIX-R29: CLOSE订单不触发自成交禁止期（平仓与开仓方向相反是正常行为）
        if ctx.action == TradeAction.OPEN:

            _opposite_dir = TradeDirection.SELL if ctx.direction == TradeDirection.BUY else TradeDirection.BUY

            _opp_key = f"{ctx.instrument_id}_{_opposite_dir}"

            for _oid, _o in svc._orders_by_id.items():

                if (_o.get('instrument_id') == ctx.instrument_id

                        and _o.get('direction') == _opposite_dir

                        and _o.get('action') == TradeAction.OPEN

                        and _o.get('status') in ('SUBMITTED', 'PARTIAL', '未成交', '部分成交', '已报入未应答', '待报入')):

                    svc._self_trade_bans[_opp_key] = _now + svc._self_trade_ban_minutes * 60.0

                    logging.warning("[OrderService] 自成交风险检查 key=%s %.1f分钟", _opp_key, svc._self_trade_ban_minutes)

                    try:

                        from ali2026v3_trading.infra.event_bus import EventBus

                        EventBus.get_instance().publish("self_trade_detected", {

                            "instrument_id": ctx.instrument_id, "direction": ctx.direction,

                            "opposite_order_id": _oid, "ban_minutes": svc._self_trade_ban_minutes,

                        })

                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                        logging.debug("[R3-L2] suppressed exception", exc_info=True)

                        pass

                        pass

                    break



        if ctx.action == TradeAction.CLOSE:
            # FIX-R29: CLOSE去重key包含signal_id，区分同instrument不同持仓的平仓
            _close_key = f"CLOSE_{ctx.instrument_id}_{ctx.direction}_{ctx.signal_id}"

            if not hasattr(svc, '_close_order_sent'):

                svc._close_order_sent: Dict[str, float] = {}

            _last_close_time = svc._close_order_sent.get(_close_key, 0.0)

            if (time.time() - _last_close_time) < 5.0:

                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'close_duplicate', 'CLOSE订单去重拦截'

                return ctx



        if ctx.expected_position_count != -1 and svc._get_position_count is not None:

            _epc = ctx.expected_position_count

            if _epc == -2:

                try:

                    _epc = svc._get_position_count(ctx.instrument_id)

                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                    _epc = -1

            if _epc >= 0:

                try:

                    _current = svc._get_position_count(ctx.instrument_id)

                    if _current != _epc:

                        logging.warning("[R26-P0-TO-04] TOCTOU阻断: %s expected=%d actual=%d", ctx.instrument_id, _epc, _current)

                        ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'toctou_position_changed', f'仓位变化: expected={_epc} actual={_current}'

                        return ctx

                except (ValueError, KeyError, TypeError, AttributeError) as e:

                    ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'position_query_error', f'仓位查询异常: {e}'

                    return ctx



        return ctx
