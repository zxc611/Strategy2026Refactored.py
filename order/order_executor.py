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



    def _execute_platform_insert(self, ctx: OrderContext) -> OrderContext:

        """阶段2: 平台下单 _构建参数+超时调用+结果归一）"""
        svc = self._svc

        try:

            ctx.order_id = svc._generate_order_id()

            ctx.order = {

                'order_id': ctx.order_id,

                'instrument_id': ctx.instrument_id,

                'exchange': ctx.exchange,

                'volume': ctx.volume,

                'price': ctx.price,

                'direction': ctx.direction,

                'action': ctx.action,

                'status': 'SUBMITTED',

                'traded_volume': 0,


                'signal_id': ctx.signal_id,

                'open_reason': ctx.open_reason,

                'decision_score': ctx.decision_score,

                'position_scale': ctx.position_scale,

                'decision_action': ctx.decision_action,

                'dimension_scores': ctx.dimension_scores or {},

                'dimension_weights': ctx.dimension_weights or {},

                'created_at': datetime.now(CHINA_TZ),

                'updated_at': datetime.now(CHINA_TZ),

            }



            signed_order = svc.authenticator.generate_signed_request(ctx.order)

            if not signed_order:

                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'sign_failed', '请求签名失败'

                return ctx

            ctx.order = signed_order



            _slippage_bps = svc._estimate_slippage(ctx.instrument_id, ctx.price, int(ctx.volume))

            logging.info("[R16-P0-005] 滑点估算: instrument=%s slippage=%.2fbps", ctx.instrument_id, _slippage_bps)



            with svc._lock:

                if ctx.idempotent_key in svc._order_idempotent_set:

                    if ctx.action in ('CLOSE', 'close'):
                        svc._order_idempotent_set.discard(ctx.idempotent_key)
                        # FIX-R28-LOG-NOISE: 同一合约5分钟内仅打印1次CLOSE-RETRY日志
                        _r28_inst = ctx.instrument_id
                        _r28_log_key = f'_r28_close_retry_logged_{_r28_inst}'
                        _r28_last_ts = getattr(svc, _r28_log_key, 0)
                        if (time.time() - _r28_last_ts) > 300:
                            logging.warning("[R28-CLOSE-RETRY] CLOSE订单幂等key已移除，允许重试: %s", ctx.idempotent_key)
                            setattr(svc, _r28_log_key, time.time())
                    else:
                        logging.warning("[R27-P0-RC-03] 幂等去重拦截(锁内): %s", ctx.idempotent_key)
                        ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'idempotent_duplicate', f'幂等去重拦截: {ctx.idempotent_key}'
                        return ctx

                svc._order_idempotent_set.add(ctx.idempotent_key)

                svc._persist_idempotent_key(ctx.idempotent_key)



                if len(svc._orders_by_id) >= svc._MAX_ORDERS_TRACKED:

                    _forced_remove = [oid for oid, o in svc._orders_by_id.items()

                                     if o['status'] in ('FILLED', 'CANCELLED', 'FAILED', 'ORPHANED', '全部成交', '已撤销', '部成部撤', 'ALL_FILLED')]

                    if _forced_remove:

                        for _foid in _forced_remove:

                            _fo = svc._orders_by_id[_foid]

                            _fa = _fo.get('action', '')

                            # FIX-R28: CLOSE订单的key含signal_id，同步构造以正确移除
                            if _fa in ('CLOSE', 'close'):
                                _fo_sig = _fo.get('signal_id', '')
                                _fo_sig_suffix = f"_{_fo_sig}" if _fo_sig else ""
                                svc._order_idempotent_set.discard(
                                    f"{_fo.get('instrument_id', '')}_{_fo.get('direction', '')}_{_fa}_{_fo.get('volume', '')}_{round(_fo.get('price', 0), 4)}{_fo_sig_suffix}")
                            else:
                                _fo_sig = _fo.get('signal_id', '')
                                _fo_sig_suffix = f"_{_fo_sig}" if _fo_sig else ""
                                svc._order_idempotent_set.discard(
                                    f"{_fo.get('instrument_id', '')}_{_fo.get('direction', '')}_{_fa}_{_fo.get('volume', '')}_{round(_fo.get('price', 0), 4)}{_fo_sig_suffix}")

                            del svc._orders_by_id[_foid]

                    else:

                        svc._order_idempotent_set.discard(ctx.idempotent_key)

                        ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'capacity_exceeded', f'订单追踪容量超限({svc._MAX_ORDERS_TRACKED})'

                        return ctx



                svc._orders_by_id[ctx.order_id] = ctx.order

                svc._stats['total_orders'] += 1



            svc._wal_write(ctx.order_id, 'PENDING', ctx.order)

            svc._append_order_state(ctx.order_id, 'SUBMITTED', ctx.order)



            if svc._platform_insert_order and callable(svc._platform_insert_order):

                try:

                    from ali2026v3_trading.config.config_service import resolve_product_exchange

                    if not ctx.exchange:

                        ctx.exchange = resolve_product_exchange(ctx.instrument_id)

                    filtered_params = svc._build_platform_insert_params(

                        order_id=ctx.order_id, instrument_id=ctx.instrument_id,

                        exchange=ctx.exchange, volume=ctx.volume,

                        price=ctx.price, direction=ctx.direction, action=ctx.action,

                    )


                    result = svc._invoke_platform_insert_with_timeout(filtered_params)

                    if result is None or (isinstance(result, int) and result < 0):
                        _trading_flag = getattr(svc._platform_insert_order, '__self__', None)
                        _is_trading = getattr(_trading_flag, 'trading', 'N/A') if _trading_flag else 'N/A'
                        # FIX-R37-TRADING-RECOVERY: 当trading=False时尝试恢复trading=True并重试一次
                        # FIX-R37-CLOSE-ONLY: 仅CLOSE订单允许恢复trading（必须平仓），OPEN订单尊重风控决策
                        if _is_trading is False and _trading_flag is not None:
                            _allow_recovery = ctx.action in ('CLOSE', 'close')
                            if _allow_recovery:
                                # FIX-R37-LOG-NOISE: 同一合约5分钟内仅打印1次RECOVERY日志
                                _recovery_log_key = f'_r37_recovery_logged_{ctx.instrument_id}'
                                _last_recovery_log_ts = getattr(svc, _recovery_log_key, 0)
                                _should_log_recovery = (time.time() - _last_recovery_log_ts) > 300
                                try:
                                    setattr(_trading_flag, 'trading', True)
                                    if _should_log_recovery:
                                        logging.warning("[R37-TRADING-RECOVERY] 检测到trading=False，已恢复trading=True，重试平仓: inst=%s dir=%s",
                                                        ctx.instrument_id, ctx.direction)
                                        setattr(svc, _recovery_log_key, time.time())
                                    result = svc._invoke_platform_insert_with_timeout(filtered_params)
                                except (AttributeError, TypeError) as _recover_err:
                                    if _should_log_recovery:
                                        logging.warning("[R37-TRADING-RECOVERY] 恢复trading=True失败: %s", _recover_err)
                            else:
                                logging.warning("[R37-TRADING-BLOCK] trading=False且为OPEN订单，尊重风控决策不恢复: inst=%s dir=%s",
                                                ctx.instrument_id, ctx.direction)
                        if result is None or (isinstance(result, int) and result < 0):
                            # FIX-ERROR-LOG-NOISE: 同一合约5分钟内仅打印1次下单失败ERROR
                            _err_log_key = f'_order_fail_logged_{ctx.instrument_id}_{ctx.direction}'
                            _err_last_ts = getattr(svc, _err_log_key, 0)
                            if (time.time() - _err_last_ts) > 300:
                                logging.error("[OrderService] 平台下单返回失败: result=%s %s %s trading=%s platform_func=%s",
                                              result, ctx.instrument_id, ctx.direction, _is_trading,
                                              getattr(svc._platform_insert_order, '__name__', str(type(svc._platform_insert_order))))
                                setattr(svc, _err_log_key, time.time())
                            # FIX-P0-1: trading=False时标记失败时间戳，供策略层过滤
                            if _is_trading is False:
                                try:
                                    _provider_ref = getattr(svc, '_provider_ref', None)
                                    if _provider_ref is not None:
                                        _fail_key = f'_p01_recent_fail_{ctx.instrument_id}_{ctx.action}'
                                        setattr(_provider_ref, _fail_key, time.time())
                                except (ValueError, KeyError, TypeError, AttributeError):
                                    pass
                            with svc._lock:
                                ctx.order['status'] = 'FAILED'
                                ctx.order['updated_at'] = datetime.now(CHINA_TZ)
                                svc._remove_order_and_idempotent_key(ctx.order_id, ctx.order)
                                svc._stats['failed_orders'] += 1
                                svc._consecutive_failures += 1
                            svc._wal_write(ctx.order_id, 'FAILED', ctx.order)
                            svc._append_order_state(ctx.order_id, 'FAILED', ctx.order)
                            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'platform_rejected', f'平台下单返回失败: result={result}'
                            return ctx

                    _platform_order_id = svc._normalize_platform_result(result).get('order_id')

                    # FIX-R33-PID: 忽略以ORD_开头的platform_order_id（内部ID被平台回传），
                    # 只接受真实的平台ID（如int或非ORD_开头的字符串）
                    if _platform_order_id and not str(_platform_order_id).startswith('ORD_'):

                        with svc._lock:

                            ctx.order['platform_order_id'] = _platform_order_id

                            svc._platform_id_to_order_id[str(_platform_order_id)] = ctx.order_id

                    elif _platform_order_id:

                        logging.warning("[R33-PID] 平台返回内部ID作为order_id，忽略: %s internal_id=%s",

                                       _platform_order_id, ctx.order_id)

                    logging.info("[OrderService] 平台下单成功: %s %s %s %d@%.2f", ctx.order_id, ctx.instrument_id, ctx.direction, int(ctx.volume), ctx.price)

                    svc._consecutive_failures = 0

                    svc._circuit_breaker_open = False

                    svc._circuit_breaker_half_open = False

                    svc._circuit_breaker_opened_at = 0.0

                    svc._wal_write(ctx.order_id, 'CONFIRMED', ctx.order)

                    svc._append_order_state(ctx.order_id, 'CONFIRMED', ctx.order)

                    svc._wal_delete(ctx.order_id)

                except TimeoutError as e:

                    logging.error("[R27-P0-DR-01] 平台下单超时: %s %s", ctx.instrument_id, e)

                    with svc._lock:

                        ctx.order['status'] = 'TIMEOUT'

                        ctx.order['updated_at'] = datetime.now(CHINA_TZ)

                        ctx.order['timeout_reason'] = str(e)

                        # FIX-R28: 超时后移除idempotent_key，允许重试平仓（否则key留在集合中永久阻断重试）
                        svc._remove_order_and_idempotent_key(ctx.order_id, ctx.order)

                        svc._stats['failed_orders'] += 1

                        svc._consecutive_failures += 1

                        if svc._consecutive_failures >= svc._circuit_breaker_threshold:

                            svc._circuit_breaker_open = True

                            svc._circuit_breaker_opened_at = time.time()

                    svc._wal_write(ctx.order_id, 'TIMEOUT', ctx.order)

                    svc._append_order_state(ctx.order_id, 'TIMEOUT', ctx.order)

                    ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'platform_timeout', f'平台下单超时: {e}'

                    return ctx

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    logging.error("[OrderService] 平台下单失败: %s %s", ctx.instrument_id, e)

                    svc._wal_write(ctx.order_id, 'FAILED', ctx.order)

                    svc._append_order_state(ctx.order_id, 'FAILED', ctx.order)

                    with svc._lock:

                        ctx.order['status'] = 'FAILED'

                        svc._remove_order_and_idempotent_key(ctx.order_id, ctx.order)

                        svc._stats['failed_orders'] += 1

                        svc._consecutive_failures += 1

                        if svc._consecutive_failures >= svc._circuit_breaker_threshold:

                            svc._circuit_breaker_open = True

                            svc._circuit_breaker_opened_at = time.time()

                    ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'platform_error', f'平台下单失败: {e}'

                    return ctx

            else:

                logging.info("[OrderService] 模拟下单: %s %s %s %d@%.2f", ctx.order_id, ctx.instrument_id[:3] + '***', ctx.direction[:1], int(ctx.volume), ctx.price)



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[OrderService] Send order error: %s", e)

            with svc._lock:

                try:

                    if ctx.order_id and ctx.order_id in svc._orders_by_id:

                        svc._remove_order_and_idempotent_key(ctx.order_id, svc._orders_by_id.get(ctx.order_id, {}))

                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                    logging.debug("[R3-L2] suppressed exception", exc_info=True)

                    pass

                    pass

                svc._stats['failed_orders'] += 1

                svc._consecutive_failures += 1

                if svc._consecutive_failures >= svc._circuit_breaker_threshold:

                    svc._circuit_breaker_open = True

            try:

                if ctx.order_id:

                    svc._wal_write(ctx.order_id, 'FAILED', {'order_id': ctx.order_id, 'instrument_id': ctx.instrument_id, 'direction': ctx.direction, 'volume': ctx.volume, 'price': ctx.price})

                    svc._append_order_state(ctx.order_id, 'FAILED', {'order_id': ctx.order_id, 'status': 'FAILED'})

            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                logging.debug("[R3-L2] suppressed exception", exc_info=True)

                pass

                pass

            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'send_error', f'下单异常: {e}'

            return ctx



        return ctx



    def _post_send_persist(self, ctx: OrderContext) -> OrderResult:

        """阶段3: 后置持久化化EventBus通知+延迟打点+返回结果"""

        svc = self._svc

        try:

            from ali2026v3_trading.infra.event_bus import get_global_event_bus

            _bus = get_global_event_bus()

            if _bus is not None:

                _bus.publish('order.submitted', {

                    'order_id': ctx.order_id, 'instrument_id': ctx.instrument_id,

                    'direction': ctx.direction, 'action': ctx.action,

                    'volume': ctx.volume, 'price': ctx.price,

                }, async_mode=True)

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] suppressed exception", exc_info=True)

            pass

            pass



        _delay_ms = (time.perf_counter() - ctx._order_submit_start_ts) * 1000.0

        if _delay_ms > 10.0:
            _sig02_log_key = f'_sig02_delay_logged_{ctx.instrument_id}'
            _sig02_last_ts = getattr(svc, _sig02_log_key, 0)
            if (time.time() - _sig02_last_ts) > 300:
                logging.warning('[SIG-02] 订单提交延迟: %.1fms instrument=%s', _delay_ms, ctx.instrument_id)
                setattr(svc, _sig02_log_key, time.time())



        if ctx._cyclic_guard:

            ctx._cyclic_guard.exit("order_send_order")



        return OrderResult.ok(ctx.order_id)



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

        from ali2026v3_trading.infra.shared_utils import VALID_TRADE_DIRECTIONS

        from ali2026v3_trading.order.order_compliance import check_self_trade_across_splits

        svc = self._svc

        if direction not in VALID_TRADE_DIRECTIONS:

            logging.error("[OrderService] R24-P0-IV-05: send_order_split direction必须是BUY/SELL, 实际=%s, 订单被拒: %s", direction, instrument_id)

            return []

        if not signal_id:

            logging.warning("[OrderService] R24-P1-TR-08: send_order_split signal_id为空, 订单无法追溯到信号 %s %s", instrument_id, direction)

        split_threshold = getattr(svc, '_split_volume_threshold', 5)

        if volume > split_threshold and (bids or asks):

            split_orders = self._plan_volume_split(volume, price, direction, bids, asks, signal_strength)

            if split_orders:

                violations = check_self_trade_across_splits(split_orders)

                if violations:

                    logging.warning("[OrderService] SOS-FAKE-01修复: 拆单自成交检测发现%d个违规，降级为单笔下单", len(violations))

                else:

                    executed_ids = []

                    for i, sub in enumerate(split_orders):

                        result = svc.send_order(

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

                            logging.warning("[OrderService] SOS-FAKE-01修复: 拆单播%d笔失败 已执行%d", i + 1, len(executed_ids))

                            break

                    if executed_ids:

                        logging.info("[OrderService] SOS-FAKE-01修复: 拆单执行完成 %d_%d_instrument=%s", len(executed_ids), len(split_orders), instrument_id)

                        return executed_ids

                    logging.warning("[OrderService] SOS-FAKE-01修复: 拆单全部失败", 降级为单笔下线)
        order_id = svc.send_order(

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

        # P2-04修复: 委托 SmartOrderSplitter 统一拆单逻辑

        from ali2026v3_trading.order.order_split_models import SmartOrderSplitter, OrderSplitStrategy

        try:

            splitter = SmartOrderSplitter(split_threshold=1, strategy=OrderSplitStrategy.AGGRESSIVE)

            result = splitter.plan_order_split(

                instrument_id='', volume=volume, direction=direction,

                signal_strength=signal_strength, bids=bids, asks=asks,

                strategy=OrderSplitStrategy.AGGRESSIVE,

            )

            # 转换 PlanOrderSplitResult -> List[Dict]

            if not result or not result.child_orders:

                return []

            splits = []

            for child in result.child_orders:

                if isinstance(child, dict):

                    splits.append({

                        'price': child.get('price', price),

                        'volume': int(child.get('volume', 0)),

                        'direction': direction,

                    })

                else:

                    splits.append({

                        'price': getattr(child, 'price', price),

                        'volume': int(getattr(child, 'volume', 0)),

                        'direction': direction,

                    })

            return splits if len(splits) > 1 else []

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[OrderService] SOS-FAKE-01修复: 拆单规划异常: %s", e)

            return []



    def execute_by_ranking(self, targets: List[Dict[str, Any]], direction: str = 'BUY', action: str = 'OPEN') -> List[str]:

        from ali2026v3_trading.infra.shared_utils import TradeDirection

        svc = self._svc

        if not targets:

            return []

        results = []

        # FIX-OPEN-UNIQUE-07: 同合约防重复开仓检查 - 同一合约同一方向已有未成交OPEN订单时跳过
        # 避免批量下单时同一合约被多次触发开仓信号导致重复挂单
        _seen_open_instruments = set()
        # FIX-P1-2: 跨周期"已尝试开仓"状态持久化，避免每30秒重复尝试同一合约
        # 证据: ag2607C16000在2小时内被32次尝试同方向开仓，全部被去重跳过
        # 根因: _seen_open_instruments仅在单次execute_by_ranking调用内有效
        if not hasattr(svc, '_cross_cycle_open_attempted'):
            svc._cross_cycle_open_attempted = {}
        _now_ts = time.time()
        _cross_cycle_ttl = 300  # 5分钟内不重复尝试同一合约开仓

        for target in targets:

            instrument_id = target.get('instrument_id', '')

            volume = target.get('lots', 1)

            price = target.get('price', 0)

            target_direction = target.get('direction', direction)

            target_action = target.get('action', action)

            if not instrument_id or price <= 0:

                continue

            # FIX-OPEN-UNIQUE-07: OPEN动作时检查同合约同方向是否已有未成交订单
            if target_action == 'OPEN':
                _dedup_key = (instrument_id, target_direction)
                if _dedup_key in _seen_open_instruments:
                    logging.warning("[OPEN-UNIQUE-07] execute_by_ranking同合约同方向重复开仓target跳过: inst=%s dir=%s",
                                    instrument_id, target_direction)
                    continue
                # FIX-P1-2: 跨周期去重 — 5分钟内已尝试开仓的合约不再重复
                _cross_key = f"{instrument_id}_{target_direction}"
                _last_attempt_ts = svc._cross_cycle_open_attempted.get(_cross_key, 0)
                if _last_attempt_ts and (_now_ts - _last_attempt_ts) < _cross_cycle_ttl:
                    continue
                # 同时检查order_service中是否已有同合约同方向未成交OPEN订单
                try:
                    _has_pending_open = False
                    for _o in svc._orders_by_id.values():
                        if (_o.get('instrument_id') == instrument_id
                            and _o.get('action') == 'OPEN'
                            and _o.get('direction') == target_direction
                            and _o.get('status') in ('PENDING', 'NEW', 'ACCEPTED', 'PARTIAL_FILLED')):
                            _has_pending_open = True
                            break
                    if _has_pending_open:
                        logging.warning("[OPEN-UNIQUE-07] inst=%s dir=%s 已有未成交OPEN订单，跳过本次开仓",
                                        instrument_id, target_direction)
                        continue
                except (ValueError, KeyError, TypeError, AttributeError) as _chk_err:
                    logging.debug("[OPEN-UNIQUE-07] 检查未成交OPEN订单异常: %s", _chk_err)
                # FIX-OPEN-UNIQUE-07-ASSERT: 断言去重key未重复添加
                assert _dedup_key not in _seen_open_instruments, \
                    f"去重key重复添加: {_dedup_key} (不应到达此处)"
                _seen_open_instruments.add(_dedup_key)
                # FIX-P1-2: 标记跨周期开仓尝试时间
                svc._cross_cycle_open_attempted[_cross_key] = _now_ts

            tick_size = svc._get_tick_size(instrument_id)

            if target_direction == TradeDirection.BUY:

                price = svc._correct_price(price + tick_size, instrument_id)

            elif target_direction == TradeDirection.SELL:

                price = svc._correct_price(max(0.01, price - tick_size), instrument_id)

            order_id = svc.send_order(

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

            if order_id and order_id.success:

                results.append(order_id.order_id)

            else:

                # FIX-R31-ERR-LOG: 修复运算符优先级问题，原代码等价于
                # (order_id.error_code or order_id.error_message or 'unknown') if order_id else 'unknown'
                # 当order_id.success=False时，if order_id为False（__bool__返回success），导致_err永远为'unknown'
                # 必须用is not None判断，因为OrderResult对象总是truthy的（除非success=False）
                if order_id is not None:
                    _err = order_id.error_code or order_id.error_message or 'unknown'
                else:
                    _err = 'unknown'
                logging.debug("[OrderService] execute_by_ranking 下单失败: %s, error=%s", instrument_id, _err)

        return results



    def bind_platform_apis(self, insert_order_func, cancel_order_func):

        svc = self._svc

        svc._platform_insert_order = insert_order_func

        svc._platform_cancel_order = cancel_order_func

        svc._platform_insert_order_params = set()

        if insert_order_func and callable(insert_order_func):

            svc._platform_api_ready = True

            try:

                import inspect

                sig = inspect.signature(insert_order_func)

                has_var_keyword = any(

                    p.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)

                    for p in sig.parameters.values()

                )

                if has_var_keyword:

                    svc._platform_insert_order_params = set()

                    logging.info("[OrderService] 平台下单API_args/**kwargs，跳过参数过滤 %s", list(sig.parameters.keys()))

                else:

                    svc._platform_insert_order_params = set(sig.parameters.keys())

                    logging.info("[OrderService] 平台下单API参数签名: %s", list(sig.parameters.keys()))

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logging.warning("[OrderService] 无法检测平台API签名: %s", e)

        logging.info("[OrderService] 平台下单/撤单API已绑定")



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

        svc = self._svc

        _order_direction = 'buy' if direction == 'BUY' else ('sell' if direction == 'SELL' else direction.lower())

        _offset = '0' if action == 'OPEN' else ('1' if action in ('CLOSE', 'close') else ('3' if action == 'CLOSE_TODAY' else action))

        all_params = {

            'exchange': exchange,

            'instrument_id': instrument_id,

            'volume': int(volume),

            'price': price,

            'order_direction': _order_direction,

            'offset': _offset,

            'order_type': 'GFD',

            'investor': '',

            'hedgeflag': '1',

            'memo': order_id,

        }

        if svc._platform_insert_order_params:

            mapped_params = {}

            for key, value in all_params.items():

                if key in svc._platform_insert_order_params:

                    mapped_params[key] = value

            if 'offset' not in mapped_params and 'offset' in all_params:

                mapped_params['offset'] = all_params['offset']

            return mapped_params

        return all_params



    def _invoke_platform_insert_with_timeout(self, filtered_params: Dict[str, Any]) -> Any:

        svc = self._svc

        result_holder: Dict[str, Any] = {}

        error_holder: Dict[str, BaseException] = {}

        done = threading.Event()



        def _target() -> None:

            try:

                result_holder['result'] = svc._platform_insert_order(**filtered_params)

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

        timeout_seconds = max(float(svc._platform_submit_timeout_seconds), 0.01)

        if not done.wait(timeout_seconds):

            raise TimeoutError(f'platform insert timeout after {timeout_seconds:.2f}s')

        if 'error' in error_holder:

            raise error_holder['error']

        return result_holder.get('result')



    def _invoke_platform_cancel_with_timeout(self, platform_id: str) -> None:

        svc = self._svc

        error_holder: Dict[str, BaseException] = {}

        done = threading.Event()



        def _target() -> None:

            try:

                svc._platform_cancel_order(platform_id)

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

        cancel_timeout = svc._operation_timeouts.get('cancel', 2.0)

        if not done.wait(cancel_timeout):

            raise TimeoutError(f'[R33-P1-12] platform cancel timeout after {cancel_timeout:.2f}s, platform_id={platform_id}')

        if 'error' in error_holder:

            raise error_holder['error']