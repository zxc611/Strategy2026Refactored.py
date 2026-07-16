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



from infra.shared_utils import CHINA_TZ

from order.order_base import OrderResult

from infra.shared_utils import TradeAction, TradeDirection, VALID_TRADE_DIRECTIONS



try:

    from strategy_judgment.causal_chain_utils import CyclicDependencyGuard

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







class _OrderExecutorBase:

    """send_order三阶段流水线执行行



    阶段1 _pre_send_checks: 断路器胖手动TOCTOU+自成交幂等去重 (~80_

    阶段2 _execute_platform_insert: 构建参数+超时调用+结果归一（(~60_

    阶段3 _post_send_persist: WAL写入+幂等记录+状态更新(~50_

    """



    def __init__(self, order_service: Any):

        self._svc = order_service



    def execute(self, ctx: OrderContext) -> OrderResult:

        """编排序 三阶段流水线，≤30行，圈复杂度量"""

        # FIX-20260707-DRY-RUN: dry_run模式下跳过平台下单，注入虚拟成交回调
        # 根因: 实盘关闭开仓后策略仍需正常运行（信号/风控/持仓/快照），
        # 为回测准备，dry_run不实际下单但保留完整策略链路和快照记录
        _dry_run = False
        _svc = getattr(self, '_svc', None)
        if _svc is not None:
            _dry_run = getattr(_svc, '_dry_run_mode', False)
            if not isinstance(_dry_run, bool):
                _dry_run = bool(_dry_run)

        ctx = self._pre_send_checks(ctx, dry_run=_dry_run)

        if ctx.rejected:

            # FIX-R31-CYCLIC-LEAK: 订单被拒绝时必须调用exit，否则order_send_order留在调用栈中
            # 导致后续所有send_order都被循环依赖检测拦截（enter返回False）
            if ctx._cyclic_guard:
                ctx._cyclic_guard.exit("order_send_order")

            return OrderResult.fail(ctx.reject_code, ctx.reject_message)

        # FIX-20260707-DRY-RUN: dry_run分支 — 构造虚拟成交，驱动持仓+快照
        if _dry_run:
            return self._execute_dry_run(ctx)

        ctx = self._execute_platform_insert(ctx)

        if ctx.rejected:

            # FIX-R31-CYCLIC-LEAK: 同上，_execute_platform_insert的拒绝路径也需要exit
            if ctx._cyclic_guard:
                ctx._cyclic_guard.exit("order_send_order")

            return OrderResult.fail(ctx.reject_code, ctx.reject_message)

        return self._post_send_persist(ctx)


    def _execute_dry_run(self, ctx: OrderContext) -> OrderResult:
        """FIX-20260707-DRY-RUN: dry_run模式执行路径

        不实际下单，但：
        1. 构造虚拟成交结果（order_id/traded_volume/FILLED状态）
        2. 通过虚拟回调注入驱动持仓更新（_add_position / _reduce_position）
        3. 触发快照采集（OPEN_SNAPSHOT / CLOSE_SNAPSHOT）
        4. 写入position_state.jsonl持久化记录

        这确保策略在dry_run模式下仍完整运行开仓/平仓逻辑，
        为正式回测和实盘恢复提供充分准备。
        """
        import time as _time
        from datetime import datetime as _dt
        svc = self._svc

        # [FIX-20260708-V4-SIG02] dry_run路径_pre_send_checks提前返回(line 369-370)
        # 未设置ctx._order_submit_start_ts，导致_post_send_persist中
        # _delay_ms = (perf_counter() - 0.0) * 1000 = 进程运行时长(ms) = 50080378ms
        # 此处补设start_ts，确保延迟计算正确
        if getattr(ctx, '_order_submit_start_ts', 0.0) == 0.0:
            ctx._order_submit_start_ts = _time.perf_counter()

        # 1. 构造虚拟订单ID和成交结果
        _ts_ms = int(_time.time() * 1000)
        ctx.order_id = f"DRY_{ctx.instrument_id}_{_ts_ms}"
        ctx.order = {
            'order_id': ctx.order_id,
            'instrument_id': ctx.instrument_id,
            'exchange': ctx.exchange or '',
            'volume': ctx.volume,
            'price': ctx.price,
            'direction': ctx.direction,
            'action': ctx.action,
            'status': 'FILLED',
            'traded_volume': ctx.volume,  # 虚拟全额成交
            'signal_id': ctx.signal_id,
            'open_reason': ctx.open_reason,
            'is_dry_run': True,  # 标记为dry_run订单
            'created_at': _dt.now(),
            'updated_at': _dt.now(),
        }

        # 2. 记录到订单追踪（便于诊断）
        with svc._lock:
            svc._orders_by_id[ctx.order_id] = ctx.order
            svc._stats['total_orders'] += 1

        logging.info("[DRY-RUN] 模拟下单成功: %s %s %s %d@%.2f action=%s",
                     ctx.order_id, ctx.instrument_id, ctx.direction,
                     int(ctx.volume), ctx.price, ctx.action)

        # 3. 异步注入虚拟成交回调，驱动持仓更新和快照记录
        _delay = getattr(svc, '_dry_run_callback_delay_sec', 0.05)
        self._inject_dry_run_callback(ctx, delay=_delay)

        # 4. 持久化
        svc._append_order_state(ctx.order_id, 'FILLED', ctx.order)

        # 5. 调用后处理（EventBus通知等）
        try:
            return self._post_send_persist(ctx)
        except Exception as _post_err:
            logging.warning("[DRY-RUN] _post_send_persist异常(非致命): %s", _post_err)
            return OrderResult.ok(ctx.order_id, ctx.order)


    def _inject_dry_run_callback(self, ctx, delay: float = 0.05):
        """FIX-20260707-DRY-RUN: 注入虚拟成交回调

        模拟平台成交回报，触发持仓管理器的 _add_position（OPEN）
        或 _reduce_position（CLOSE），从而驱动快照采集和
        position_state.jsonl 持久化。
        """
        import threading as _threading
        import time as _time
        svc = self._svc

        def _virtual_callback():
            _time.sleep(delay)  # 模拟网络延迟
            try:
                # [FIX-20260708-PROVIDER-FALLBACK] 多级回退获取_provider_ref
                _provider_ref = getattr(svc, '_provider_ref', None)
                _fb_source = 'direct'
                if _provider_ref is None:
                    # 回退1: 通过_platform_insert_order.__self__回溯策略对象
                    # FIX-PROVIDER-FALLBACK-1: __self__是OrderService自身而非策略对象，
                    # 获取到后_position_service仍为None，需跳过此无效回退
                    _platform_fn = getattr(svc, '_platform_insert_order', None)
                    if _platform_fn is not None:
                        _host = getattr(_platform_fn, '__self__', None)
                        if _host is not None and _host is not svc:
                            _provider_ref = _host
                            _fb_source = 'platform_fn_self'
                if _provider_ref is None:
                    # 回退2: 通过get_cached_params查找strategy对象
                    try:
                        from config.config_service import get_cached_params as _gcp_v
                        _strategy_obj = _gcp_v().get('strategy', None)
                        if _strategy_obj is not None:
                            _core = getattr(_strategy_obj, 'strategy_core', None)
                            _provider_ref = _core if _core else _strategy_obj
                            _fb_source = 'cached_params'
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                        pass
                if _provider_ref is not None:
                    _fb_ok = getattr(svc, '_dry_run_provider_fallback_ok_count', 0) + 1
                    svc._dry_run_provider_fallback_ok_count = _fb_ok
                    if _fb_ok <= 3 or _fb_ok % 100 == 1:
                        logging.info("[DRY-RUN] provider回退成功: source=%s order_id=%s",
                                     _fb_source, getattr(ctx, 'order_id', '?'))
                if _provider_ref is None:
                    _fb_count = getattr(svc, '_dry_run_provider_fallback_fail_count', 0) + 1
                    svc._dry_run_provider_fallback_fail_count = _fb_count
                    if _fb_count <= 5 or _fb_count % 100 == 1:
                        logging.warning("[DRY-RUN] 所有回退均无法获取provider，虚拟回调注入跳过(累计%d次) order_id=%s",
                                        _fb_count, getattr(ctx, 'order_id', '?'))
                    return
                _ps = getattr(_provider_ref, '_position_service', None)
                if _ps is None and hasattr(_provider_ref, '_ensure_position_service'):
                    _provider_ref._ensure_position_service()
                    _ps = getattr(_provider_ref, '_position_service', None)
                if _ps is None:
                    logging.warning("[DRY-RUN] _position_service为None（ensure后仍为空），虚拟回调注入跳过 order_id=%s",
                                    getattr(ctx, 'order_id', '?'))
                    return

                _action = (ctx.action or '').upper()
                _instrument_id = ctx.instrument_id
                _direction = (ctx.direction or 'BUY').upper()
                _volume = int(ctx.volume or 1)
                _price = float(ctx.price or 0.0)
                _order_id = ctx.order_id
                _signal_id = getattr(ctx, 'signal_id', '') or ''
                _open_reason = getattr(ctx, 'open_reason', '') or 'dry_run'
                # [FIX-20260708-V4] 修复_add_position/_reduce_position参数不匹配
                # _add_position签名: (exchange, instrument_id, volume, price, open_reason, order_id, signal_id)
                # direction通过volume正负传递: BUY=正值, SELL=负值
                _exchange = getattr(ctx, 'exchange', '') or ''
                if not _exchange:
                    # 从instrument_id推断exchange
                    try:
                        from config.params_service import get_params_service
                        _ps_svc = get_params_service()
                        if _ps_svc is not None:
                            _meta = _ps_svc.get_instrument_meta(_instrument_id) if hasattr(_ps_svc, 'get_instrument_meta') else None
                            if _meta:
                                _exchange = _meta.get('exchange', '') or ''
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                        pass
                _signed_volume = _volume if _direction == 'BUY' else -_volume

                if _action in ('OPEN', ''):
                    # 开仓回调 → _add_position → OPEN快照
                    _ps._add_position(
                        exchange=_exchange,
                        instrument_id=_instrument_id,
                        volume=_signed_volume,
                        price=_price,
                        order_id=_order_id,
                        signal_id=_signal_id,
                        open_reason=f"dry_run:{_open_reason}",
                    )
                    logging.info("[DRY-RUN] 虚拟开仓回调注入成功: %s %s %d@%.2f",
                                _instrument_id, _direction, _volume, _price)
                elif _action in ('CLOSE',):
                    # 平仓回调 → _reduce_position → CLOSE快照
                    # _reduce_position签名: (exchange, instrument_id, volume, is_buy, price)
                    _ps._reduce_position(
                        exchange=_exchange,
                        instrument_id=_instrument_id,
                        volume=_volume,
                        is_buy=(_direction == 'BUY'),
                        price=_price,
                    )
                    logging.info("[DRY-RUN] 虚拟平仓回调注入成功: %s %s %d@%.2f",
                                _instrument_id, _direction, _volume, _price)
            except Exception as e:
                logging.error("[DRY-RUN] 虚拟回调注入失败: %s", e, exc_info=True)

        _t = _threading.Thread(target=_virtual_callback, daemon=True,
                               name=f"DryRunCallback_{ctx.order_id}")
        _t.start()


    def _pre_send_checks(self, ctx: OrderContext, dry_run: bool = False) -> OrderContext:

        """阶段1: 前置校验 _断路器胖手动TOCTOU+自成交幂等去重"""

        if dry_run:
            return ctx

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
                # FIX-FAT-FINGER-OPTION: 期权CLOSE订单放宽fat_finger阈值至0.80
                # 期权时间价值衰减导致开仓价与当前市价偏离极大(>50%常见)，
                # 原阈值0.50导致合法期权平仓被误杀→幽灵持仓(如sc2608P440偏离62.50%)
                # 期权合约ID格式: 品种+年月+P/C+行权价 (如sc2608P440, sc2608C470, HO2609-C-3200)
                import re as _re
                _is_option = bool(_re.search(r'\d[-]?[CP][-]?\d', ctx.instrument_id))
                if _is_option:
                    _fat_finger_threshold = 0.80

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

                        from infra.event_bus import EventBus

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
