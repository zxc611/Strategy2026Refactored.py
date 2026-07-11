# [M1-48] ____׷______

# MODULE_ID: M1-132

"""

order_chase_service.py - OrderChaseService

Phase 2 (CC-P1-01): 从OrderService提取的撤单追单职责任



职责任

- 撤单 (cancel_order, cancel_all_pending)

- 紧急平台(emergency_close_all_positions)

- 追单 (_chase_reorder)

- 拆单 (_plan_volume_split)



Provider接口：通过provider（OrderService实例）获取共享属性

"""

from __future__ import annotations



import logging

import time

from datetime import datetime

from typing import Any, Dict, List, Optional



from ali2026v3_trading.infra.shared_utils import CHINA_TZ, TradeAction, TradeDirection





class OrderChaseService:

    """撤单/追单服务 _从OrderService提取的独立可测试组件"""



    def __init__(self, provider=None):

        self._provider = provider



    def cancel_order(self, order_id: str) -> bool:

        svc = self._provider

        try:

            with svc._lock:

                order = svc._orders_by_id.get(order_id)

                if not order:

                    logging.warning("[OrderChaseService] Order not found: %s", order_id)

                    return False

                if order['status'] in ('FILLED', 'CANCELLED', 'FAILED', '全部成交', '已撤销', '部成部撤', 'ALL_FILLED'):

                    return False

                platform_id = order.get('platform_order_id')
                if not platform_id:
                    for _pid, _iid in svc._platform_id_to_order_id.items():
                        if _iid == order_id:
                            platform_id = _pid
                            break
                if isinstance(platform_id, str):
                    try:
                        platform_id = int(platform_id)
                    except (ValueError, TypeError):
                        pass

                _platform_cancel_ok = False

            if svc._platform_cancel_order and callable(svc._platform_cancel_order):

                try:

                    if not isinstance(platform_id, int):
                        logging.warning("[OrderChaseService] platform_id非int，平台撤单跳过: %s order_id=%s", platform_id, order_id)
                        # FIX-R32-CANCEL: 平台撤单失败时，不标记CANCELLED，保持TIMEOUT状态
                        # 返回True允许追单，但平台回调返回成交时仍能找到订单（status=TIMEOUT）
                    else:
                        svc._invoke_platform_cancel_with_timeout(platform_id)

                        logging.info("[OrderChaseService] 平台撤单成功: %s", order_id)

                        with svc._lock:

                            svc._cancel_count_window.append(time.time())

                        _platform_cancel_ok = True

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    logging.error("[OrderChaseService] 平台撤单失败: %s %s", order_id, e)

                    return False

            # FIX-R32-CANCEL: 只有平台撤单成功时，才标记CANCELLED
            # 平台撤单失败时保持TIMEOUT状态，等待平台回调返回成交结果
            if _platform_cancel_ok:
                with svc._lock:

                    order['status'] = 'CANCELLED'

                    order['updated_at'] = datetime.now(CHINA_TZ)

                    svc._stats['cancelled_orders'] += 1

            return True

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[OrderChaseService] Cancel order error: %s", e)

            return False



    def cancel_all_pending(self) -> int:

        svc = self._provider

        cancelled = 0

        try:

            with svc._lock:

                pending_ids = [

                    oid for oid, o in svc._orders_by_id.items()

                    if o.get('status') in ('PENDING', 'SUBMITTED', 'PARTIAL_FILLED', '未成交', '部分成交', '已报入未应答', '待报入')

                ]

            for oid in pending_ids:

                if self.cancel_order(oid):

                    cancelled += 1

                else:

                    logging.warning("[R27-P0-RC-04] cancel_all_pending: 撤单失败 %s", oid)

            if cancelled > 0:

                logging.info("[R27-P0-RC-04] cancel_all_pending: 撤销%d/%d笔挂单", cancelled, len(pending_ids))

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[R27-P0-RC-04] cancel_all_pending异常: %s", e)

        return cancelled



    def emergency_close_all_positions(self, caller_id: str = "unknown") -> Dict[str, Any]:

        svc = self._provider

        result: Dict[str, Any] = {'cancelled': [], 'closed': [], 'errors': []}

        try:

            with svc._lock:

                pending_order_ids = [

                    oid for oid, o in svc._orders_by_id.items()

                    if o.get('status') in ('PENDING', 'SUBMITTED', 'PARTIAL_FILLED', '未成交', '部分成交', '已报入未应答', '待报入')

                ]

            for oid in pending_order_ids:

                if self.cancel_order(oid):

                    result['cancelled'].append(oid)

                else:

                    result['errors'].append(f"cancel_failed:{oid}")

            with svc._lock:

                open_positions = {

                    oid: dict(o) for oid, o in svc._orders_by_id.items()

                    if o.get('status') in ('FILLED', 'ALL_FILLED', '全部成交') and o.get('action') == TradeAction.OPEN

                }

            for oid, pos in open_positions.items():

                instrument_id = pos.get('instrument_id', '')

                direction = TradeDirection.SELL if pos.get('direction') == TradeDirection.BUY else TradeDirection.BUY

                volume = pos.get('volume', 0)

                price = pos.get('price', 0)

                if volume > 0 and price > 0:

                    # FIX-R37-UNIQUE-CLOSE: 紧急平仓必须设置_closing标志，
                    # 否则止盈止损/时间止损检查时_closing=False会重复触发平仓。
                    # 通过PositionService查找对应持仓并设置_closing。
                    try:
                        from ali2026v3_trading.position.position_service import get_position_service
                        _pos_svc = get_position_service()
                        if _pos_svc:
                            with _pos_svc._get_instrument_lock(instrument_id):
                                for _rec in _pos_svc.positions.get(instrument_id, {}).values():
                                    if getattr(_rec, '_closing', False):
                                        continue
                                    _rec_dir = 'BUY' if _rec.volume > 0 else 'SELL'
                                    if _rec_dir == pos.get('direction', ''):
                                        _rec._closing = True
                                        _rec.closing_order_id = f"PENDING_EMERGENCY_{_rec.position_id}"
                                        # FIX-CLOSE-UNIQUE-12: 紧急平仓设置PENDING状态时同时设置close_method，
                                        # 否则平仓失败时close_method为空，无法追溯平仓触发路径
                                        if hasattr(_rec, 'close_method'):
                                            _rec.close_method = 'emergency_close'
                                        break
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                        pass

                    _emergency_ref_price = 0.0
                    try:
                        from ali2026v3_trading.position.position_service import get_position_service
                        _pos_svc_tmp = get_position_service()
                        if _pos_svc_tmp:
                            with _pos_svc_tmp._get_instrument_lock(instrument_id):
                                for _rec_tmp in _pos_svc_tmp.positions.get(instrument_id, {}).values():
                                    if price > 0 and price == getattr(_rec_tmp, 'open_price', 0):
                                        _emergency_ref_price = price
                                        break
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                        pass

                    close_result = svc.send_order(

                        instrument_id=instrument_id,

                        exchange=pos.get('exchange', ''),

                        direction=direction,

                        price=price,

                        volume=volume,

                        action='CLOSE',

                        signal_id=f'emergency_{caller_id}',

                        open_reason=f'emergency_close_by_{caller_id}',

                        ref_price=_emergency_ref_price,

                    )

                    if close_result.ok:

                        result['closed'].append(oid)

                        # FIX-R37-UNIQUE-CLOSE(A1): 紧急平仓成功后必须更新 closing_order_id 为实际 order_id，
                        # 否则 closing_order_id 永远停留在 PENDING_EMERGENCY_{pos_id}，
                        # 下游 _reset_closing_flag_on_order_failure 无法匹配实际订单ID，
                        # 且 on_order_filled 时无法关联到持仓。
                        # FIX-R37-UNIQUE-CLOSE(D1): 同时设置 close_method/close_reason/realized_pnl
                        try:
                            _actual_oid = getattr(close_result, 'order_id', '') or oid
                            from ali2026v3_trading.position.position_service import get_position_service
                            _pos_svc = get_position_service()
                            if _pos_svc:
                                with _pos_svc._get_instrument_lock(instrument_id):
                                    for _rec in _pos_svc.positions.get(instrument_id, {}).values():
                                        if getattr(_rec, '_closing', False) and \
                                           getattr(_rec, 'closing_order_id', '').startswith('PENDING_EMERGENCY_'):
                                            _rec.closing_order_id = _actual_oid
                                            _rec.close_method = 'emergency_close'
                                            _rec.close_reason = f'emergency_close_by_{caller_id}'
                                            _pnl_mult = 1.0 if getattr(_rec, 'direction', '') in ('long', 'BUY') else -1.0
                                            _rec.realized_pnl = _pnl_mult * (price - _rec.open_price) * volume
                                            break
                        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _a1_err:
                            logging.debug("[R37-UNIQUE-CLOSE] A1紧急平仓更新closing_order_id失败: %s", _a1_err)

                    else:

                        result['errors'].append(f"close_failed:{oid}:{close_result.error_code}")

                        # FIX-R37-UNIQUE-CLOSE(A1): 紧急平仓失败时重置 _closing 标志，避免持仓卡死
                        try:
                            from ali2026v3_trading.position.position_service import get_position_service
                            _pos_svc = get_position_service()
                            if _pos_svc:
                                with _pos_svc._get_instrument_lock(instrument_id):
                                    for _rec in _pos_svc.positions.get(instrument_id, {}).values():
                                        if getattr(_rec, '_closing', False) and \
                                           getattr(_rec, 'closing_order_id', '').startswith('PENDING_EMERGENCY_'):
                                            _rec._closing = False
                                            _rec.closing_order_id = ''
                                            # FIX-CLOSE-UNIQUE-12: 紧急平仓失败时重置close_method
                                            if hasattr(_rec, 'close_method'):
                                                _rec.close_method = ''
                                            logging.warning("[R37-UNIQUE-CLOSE] A1紧急平仓失败,重置_closing: inst=%s pos_id=%s",
                                                            instrument_id, getattr(_rec, 'position_id', ''))
                                            break
                        except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                            pass

            logging.critical("[R22-P0-EC-01] emergency_close_all_positions: caller=%s cancelled=%d closed=%d errors=%d",

                             caller_id, len(result['cancelled']), len(result['closed']), len(result['errors']))

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[R22-P0-EC-01] emergency_close_all_positions error: %s", e)

            result['errors'].append(str(e))

        return result



    def _chase_reorder(self, original_order: Dict, retry_count: int) -> None:

        assert isinstance(original_order, dict), f"_chase_reorder: original_order必须是dict, got {type(original_order)}"
        assert retry_count >= 1, f"_chase_reorder: retry_count必须>=1, got {retry_count}"

        svc = self._provider

        _orig_id = original_order.get('order_id', '')

        with svc._lock:

            _existing = svc._orders_by_id.get(_orig_id)

            if _existing and _existing.get('status') in ('SUBMITTED', 'PARTIAL_FILLED', '未成交', '部分成交', '已报入未应答', '待报入'):

                logging.info("[R23-ID-03-FIX] chase重试跳过: 订单%s仍处于活跃状态s",

                           _orig_id, _existing.get('status'))

                return

        _traded_volume = original_order.get('traded_volume', 0)

        _original_volume = original_order.get('volume', 0)

        if _traded_volume > 0:

            _remaining_volume = _original_volume - _traded_volume

            if _remaining_volume <= 0:

                logging.info("[P1-R9-26] 原订单已完全成交, 无需追单: order=%s traded=%d",

                           original_order.get('order_id', ''), _traded_volume)

                return

            logging.info("[P1-R9-26] 原订单部分成交 追单剩余） order=%s original=%d traded=%d remaining=%d",

                        original_order.get('order_id', ''), _original_volume, _traded_volume, _remaining_volume)

        else:

            _remaining_volume = _original_volume

        _order_id = original_order.get('order_id', '')

        _backoff_delay = min(svc._retry_backoff_base * (2 ** (retry_count - 1)), svc._retry_backoff_max)

        _last_retry = svc._last_retry_time.get(_order_id, 0.0)

        _elapsed_since_last = time.time() - _last_retry

        if _elapsed_since_last < _backoff_delay:

            logging.debug("[ID-P1-08-FIX] 重试退避中: order=%s retry=%d wait=%.1fs", _order_id, retry_count, _backoff_delay - _elapsed_since_last)

            return

        svc._last_retry_time[_order_id] = time.time()

        instrument_id = original_order['instrument_id']

        # FIX-R38-CHASE-POS-CHECK: CLOSE订单追单前检查持仓是否仍然存在。
        # 如果持仓已被前序成交平掉（延迟成交场景），不应再追单放置新的CLOSE订单，
        # 否则会导致多个CLOSE订单同时成交，产生"持仓不足"错误。
        _action = original_order.get('action', '')
        if _action == 'CLOSE':
            try:
                from ali2026v3_trading.position.position_service import get_position_service
                _pos_svc = get_position_service()
                if _pos_svc:
                    with _pos_svc._get_instrument_lock(instrument_id):
                        _pos_dict = _pos_svc.positions.get(instrument_id, {})
                        _total_closeable = 0
                        _is_sell = original_order.get('direction', '') == 'SELL'
                        _closing_count = 0
                        for _r in _pos_dict.values():
                            _rv = getattr(_r, 'volume', 0)
                            if (_is_sell and _rv > 0) or (not _is_sell and _rv < 0):
                                _total_closeable += abs(_rv)
                            if getattr(_r, 'closing_order_id', ''):
                                _closing_count += 1
                        if _total_closeable <= 0:
                            logging.info("[R38-CHASE-POS-CHECK] 持仓已清零，跳过CLOSE追单: inst=%s orig_order=%s retry=%d",
                                         instrument_id, _orig_id, retry_count)
                            return
                        if _total_closeable < _remaining_volume:
                            logging.info("[R38-CHASE-POS-CHECK] 可平仓量(%d)<追单量(%d)，调整追单量为可平仓量: inst=%s orig_order=%s",
                                         _total_closeable, _remaining_volume, instrument_id, _orig_id)
                            _remaining_volume = _total_closeable
                            if _remaining_volume <= 0:
                                return
                        if _closing_count > 0:
                            logging.info("[R38-CHASE-POS-CHECK] 存在%d个平仓订单在途(closing_order_id非空)，跳过CLOSE追单: inst=%s orig_order=%s",
                                         _closing_count, instrument_id, _orig_id)
                            return
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _pos_err:
                logging.debug("[R38-CHASE-POS-CHECK] 持仓检查失败(继续追单): inst=%s err=%s", instrument_id, _pos_err)

        tick_size = svc._get_tick_size(instrument_id)

        chase_ticks = min(retry_count, 3)

        price_offset = tick_size * chase_ticks

        max_chase_ticks = 5

        if chase_ticks > max_chase_ticks:

            logging.warning("[OrderChaseService._chase_reorder] Chase ticks %d exceeds max %d, capping", chase_ticks, max_chase_ticks)

            chase_ticks = max_chase_ticks

            price_offset = tick_size * chase_ticks

        if original_order['direction'] == TradeDirection.BUY:

            new_price = original_order['price'] + price_offset

        else:

            new_price = original_order['price'] - price_offset

        new_order_id = svc.send_order(

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

            with svc._lock:

                svc._orders_by_id[_oid]['retry_count'] = retry_count

                svc._orders_by_id[_oid]['original_order_id'] = original_order['order_id']

                chase_prefixed_id = svc._generate_chase_order_id(original_order['order_id'])

                svc._orders_by_id[_oid]['chase_order_id'] = chase_prefixed_id

            logging.info("[OrderChaseService] 追单: %s -> %s retry=%d price=%.2f->%.2f",
                         original_order['order_id'], _oid, retry_count, original_order['price'], new_price)
            if _action == 'CLOSE':
                try:
                    from ali2026v3_trading.position.position_service import get_position_service
                    _pos_svc = get_position_service()
                    if _pos_svc:
                        with _pos_svc._get_instrument_lock(instrument_id):
                            _pos_dict = _pos_svc.positions.get(instrument_id, {})
                            for _r in _pos_dict.values():
                                _r_closing_oid = getattr(_r, 'closing_order_id', '')
                                _r_pos_id = getattr(_r, 'position_id', '')
                                # FIX-R37-UNIQUE-CLOSE(A4): 追单更新 closing_order_id 必须匹配所有 PENDING_ 变体前缀
                                # 原代码只匹配 PENDING_{pos_id}，遗漏了 PENDING_EMERGENCY_/PENDING_RISK_REDUCE_，
                                # 导致紧急平仓/熔断减仓的追单无法更新 closing_order_id
                                _pending_variants = (
                                    f"PENDING_{_r_pos_id}",
                                    f"PENDING_EMERGENCY_{_r_pos_id}",
                                    f"PENDING_RISK_REDUCE_{_r_pos_id}",
                                )
                                if _r_closing_oid and (_r_closing_oid == original_order['order_id'] or
                                                         _r_closing_oid in _pending_variants or
                                                         _r_closing_oid.startswith('PARTIAL_')):
                                    _r.closing_order_id = _oid
                                    # FIX-CLOSE-UNIQUE-14: 追单成功更新closing_order_id时同时更新close_method，
                                    # 标记为'chase_reorder'以便后续追溯平仓是通过追单完成的
                                    if hasattr(_r, 'close_method'):
                                        _prev_method = getattr(_r, 'close_method', '')
                                        _r.close_method = _prev_method or 'chase_reorder'
                                    logging.info("[CHASE-CLOSING-OID-UPDATE] 追单成功更新closing_order_id: inst=%s old=%s new=%s",
                                                 instrument_id, _r_closing_oid, _oid)
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _upd_err:
                    logging.debug("[CHASE-CLOSING-OID-UPDATE] 更新closing_order_id异常: %s", _upd_err)

        else:

            # FIX-R37-CHASE-NOISE: 追单失败通常因idempotent_duplicate/rate_limited等预期行为，降为DEBUG
            logging.debug("[OrderChaseService] 追单失败: %s retry=%d error=%s", instrument_id, retry_count, new_order_id.error_code if new_order_id else 'unknown')



    def _plan_volume_split(self, volume, price, direction, bids, asks, signal_strength):

        try:

            from ali2026v3_trading.order.order_split_models import SmartOrderSplitter

            splitter = SmartOrderSplitter()

            result = splitter.split(volume, price, direction, strategy='EQUAL')

            if result and len(result) > 1:

                return result

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] suppressed exception", exc_info=True)

            pass

            pass

        svc = self._provider

        split_threshold = getattr(svc, '_split_volume_threshold', 5)

        if volume <= split_threshold or not (bids or asks):

            return None

        import math

        n_splits = min(max(2, math.ceil(volume / split_threshold)), 10)

        single_vol = volume / n_splits

        splits = []

        for i in range(n_splits):

            splits.append({'volume': single_vol, 'price': price, 'index': i})

        return splits