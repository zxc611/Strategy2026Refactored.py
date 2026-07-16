# [M1-47] ____״̬______

# MODULE_ID: M1-143

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""

订单状态管理器 _从order_service.py拆分

职责: 订单状态追踪、待处理订单扫描、超时检查

"""

from __future__ import annotations



import logging

import threading

import time

from typing import Any, Dict, List, Optional

from collections import deque



from infra.shared_utils import CHINA_TZ





class OrderStateManager:

    def __init__(self, max_pending: int = 1000):

        self._pending_orders: Dict[str, Dict[str, Any]] = {}

        self._order_timestamps: Dict[str, float] = {}

        self._max_pending = max_pending

        self._timeout_sec = 30.0

        # 线程安全：调度器线程与主线程并发访问保护

        self._lock = threading.Lock()



    def add_pending(self, order_id: str, order_info: Dict[str, Any]) -> None:

        with self._lock:

            self._pending_orders[order_id] = order_info

            self._order_timestamps[order_id] = time.time()



    def remove_pending(self, order_id: str) -> Optional[Dict[str, Any]]:

        with self._lock:

            self._order_timestamps.pop(order_id, None)

            return self._pending_orders.pop(order_id, None)



    def get_pending(self, order_id: str) -> Optional[Dict[str, Any]]:

        with self._lock:

            return self._pending_orders.get(order_id)



    def scan_timeouts(self, timeout_sec: Optional[float] = None) -> List[str]:

        _timeout = timeout_sec if timeout_sec is not None else self._timeout_sec

        _now = time.time()

        with self._lock:

            timed_out = [oid for oid, ts in self._order_timestamps.items() if _now - ts > _timeout]

        return timed_out



    def check_pending_orders(self) -> List[str]:

        return self.scan_timeouts()



    @property

    def pending_count(self) -> int:

        with self._lock:

            return len(self._pending_orders)



    def on_trade_update(self, svc, trade_data: Any) -> None:

        from datetime import datetime

        from order.order_service import _validate_order_status_transition

        try:

            normalized = svc._normalize_platform_result(trade_data)

            oid = normalized.get('order_id', '')

            sts = normalized.get('status', '')

            filled = normalized.get('traded_volume', 0)

            if not oid:
                _raw_oid = getattr(trade_data, 'order_id', None)
                if _raw_oid is not None:
                    oid = str(_raw_oid)
                    logging.debug("[R28-TRADE-UPDATE] order_id为空，使用fallback oid=%s type=%s", oid, type(_raw_oid).__name__)

            with svc._lock:

                order = svc._orders_by_id.get(oid)

                if not order:

                    mapped_id = svc._platform_id_to_order_id.get(oid)

                    if mapped_id:

                        order = svc._orders_by_id.get(mapped_id)

                    if not order:

                        for o in svc._orders_by_id.values():

                            # FIX-CLOSE-LOOKUP: 使用str()比较避免int/string类型不匹配
                            if str(o.get('platform_order_id')) == str(oid):

                                order = o

                                break

                    # FIX-R34-MEMO: PythonGO平台规范 - TradeData.memo字段包含发送订单时填入的内部ID(ORD_xxx)
                    # 在instrument匹配之前，优先使用memo字段直接查找内部订单，避免匹配到错误订单
                    if not order:
                        _memo = ''
                        if isinstance(trade_data, dict):
                            _memo = str(trade_data.get('memo', '') or '')
                        else:
                            _memo = str(getattr(trade_data, 'memo', '') or '')
                        if _memo.startswith('ORD_'):
                            _memo_order = svc._orders_by_id.get(_memo)
                            if _memo_order:
                                order = _memo_order
                                # 同时建立platform_id→internal_id映射，后续on_trade可直接使用
                                _existing_pid = order.get('platform_order_id', '')
                                _need_update = (not _existing_pid
                                                or str(_existing_pid) == _memo
                                                or str(_existing_pid).startswith('ORD_'))
                                if _need_update and oid:
                                    order['platform_order_id'] = str(oid)
                                    svc._platform_id_to_order_id[str(oid)] = _memo
                                logging.info("[R34-MEMO-TRADE] memo直接查找成功: memo=%s platform_id=%s inst=%s action=%s",
                                             _memo, oid, order.get('instrument_id', ''), order.get('action', ''))

                    if not order and not oid:
                        _inst = getattr(trade_data, 'instrument_id', '')
                        _dir = getattr(trade_data, 'direction', '')
                        if _inst:
                            _candidates = [o for o in svc._orders_by_id.values()
                                           if o.get('instrument_id') == _inst
                                           and o.get('status') in ('SUBMITTED', 'TIMEOUT', 'PENDING')]
                            if _candidates:
                                order = max(_candidates, key=lambda x: x.get('created_at', datetime.min))
                                logging.info("[R28-TRADE-MATCH] oid为空，通过instrument匹配: inst=%s matched_order=%s", _inst, order.get('order_id', ''))

                    if not order and oid:
                        _inst = getattr(trade_data, 'instrument_id', '')
                        if _inst:
                            _now_ts = time.time()
                            def _safe_order_ts(o):
                                _ts = o.get('updated_at') or o.get('created_at')
                                if isinstance(_ts, datetime):
                                    return _ts.timestamp()
                                try:
                                    return float(_ts) if _ts else 0.0
                                except (ValueError, TypeError):
                                    return 0.0
                            # FIX-R34-DIR-OFFSET: PythonGO平台规范 - TradeData有direction和offset字段
                            # offset=0(OPEN)→匹配action=OPEN的订单; offset=1(CLOSE)→匹配action=CLOSE的订单
                            # 避免CLOSE订单被错误匹配到OPEN交易，导致持仓被错误更新
                            _trade_offset = ''
                            if isinstance(trade_data, dict):
                                _trade_offset = str(trade_data.get('offset', '') or '')
                            else:
                                _trade_offset = str(getattr(trade_data, 'offset', '') or '')
                            _expected_action = ''
                            if _trade_offset in ('0', 'OPEN', 'open', '开'):
                                _expected_action = 'OPEN'
                            elif _trade_offset in ('1', 'CLOSE', 'close', '平'):
                                _expected_action = 'CLOSE'
                            # FIX-R32-PID-MAP: 原条件 not o.get('platform_order_id') 过于严格，
                            # 当platform_order_id被设置为内部ID(如ORD_xxx)作为fallback时，无法匹配。
                            # 修改为：匹配没有platform_order_id、或platform_order_id是内部ID的订单
                            _candidates = [o for o in svc._orders_by_id.values()
                                           if o.get('instrument_id') == _inst
                                           and (not o.get('platform_order_id') or str(o.get('platform_order_id', '')).startswith('ORD_'))
                                           and (o.get('status') not in ('CANCELLED', 'FAILED', 'ORPHANED', '已撤销', '部成部撤')
                                                 or (o.get('status') in ('CANCELLED', '已撤销') and _now_ts - _safe_order_ts(o) < 120))]
                            # FIX-R37-STALE-ORDER: 排除重启恢复的旧session订单(无created_at字段)。
                            # 这些订单的action可能为空，direction可能不正确，匹配它们会导致错单。
                            # 仅当没有当前session订单时才考虑旧订单作为最后fallback。
                            _fresh_candidates = [o for o in _candidates if o.get('created_at') is not None]
                            if _fresh_candidates:
                                _candidates = _fresh_candidates
                            # FIX-R38-ACTION-EMPTY: 排除action为空的订单，这些通常是重启恢复的旧订单
                            # 或被evict后通过platform_id错误匹配的订单，其direction可能不正确，
                            # 匹配它们会导致错单（如OPEN BUY被误匹配为CLOSE SELL）。
                            # 仅当没有非空action订单时才考虑空action订单作为最后fallback。
                            _non_empty_action = [o for o in _candidates if o.get('action', '')]
                            if _non_empty_action:
                                _candidates = _non_empty_action
                            # FIX-R34-DIR-OFFSET: 如果能从offset推断出action，优先匹配相同action的订单
                            if _expected_action and _candidates:
                                _filtered = [o for o in _candidates if o.get('action', '') == _expected_action]
                                if _filtered:
                                    _candidates = _filtered
                                    logging.info("[R34-DIR-OFFSET] offset=%s→action=%s, 过滤后剩余%d个候选订单",
                                                 _trade_offset, _expected_action, len(_candidates))
                            if _candidates:
                                order = max(_candidates, key=lambda x: x.get('created_at', datetime.min))
                                logging.info("[R28-TRADE-MATCH] oid=%s无映射，通过instrument匹配: inst=%s matched_order=%s action=%s offset=%s",
                                             oid, _inst, order.get('order_id', ''), order.get('action', ''), _trade_offset)

                if order:

                    _matched_internal_id = None
                    for _ioid, _io in svc._orders_by_id.items():
                        if _io is order:
                            _matched_internal_id = _ioid
                            break
                    if _matched_internal_id and oid:
                        # FIX-R32-PID-MAP: 当platform_order_id未设置、或为内部ID(ORD_xxx)时，更新为真实平台ID
                        _existing_pid = order.get('platform_order_id', '')
                        _need_update = (not _existing_pid
                                        or str(_existing_pid) == _matched_internal_id
                                        or str(_existing_pid).startswith('ORD_'))
                        if _need_update:
                            order['platform_order_id'] = str(oid)
                            svc._platform_id_to_order_id[str(oid)] = _matched_internal_id
                            logging.info("[R28-PID-MAP] on_trade_update补建映射: platform_id=%s → internal_id=%s inst=%s action=%s",
                                         oid, _matched_internal_id, order.get('instrument_id', ''), order.get('action', ''))

                    old_sts = order.get('status', '')

                    if not _validate_order_status_transition(old_sts, sts):

                        logging.warning(

                            "[R14-P0-BIZ-08] 非法订单状态转换 order_id=%s %s->%s, 拒绝更新",

                            oid, old_sts, sts,

                        )

                        return

                    order['status'] = sts

                    # FIX-R37-STALE-TRADED: 不覆盖旧session恢复订单的traded_volume。
                    # 旧session订单(无created_at)的traded_volume记录的是上一session的累计成交量，
                    # 直接覆盖会导致增量成交计算(_traded_volume - _prev_traded)出错。
                    if order.get('created_at') is not None:
                        order['traded_volume'] = filled
                    else:
                        # 旧session订单: 仅在traded_volume小于当前filled时更新(取较大值)
                        _prev_traded = order.get('traded_volume', 0) or 0
                        if filled > _prev_traded:
                            order['traded_volume'] = filled
                        logging.debug("[R37-STALE-TRADED] 旧session订单traded_volume保守更新: oid=%s prev=%d new=%d",
                                        oid, _prev_traded, filled)


                    order['updated_at'] = datetime.now(CHINA_TZ)

                    svc._order_state_audit_log.append({

                        'timestamp': time.time(), 'order_id': oid,

                        'old_status': order.get('status', ''), 'new_status': sts,

                    })

                    if len(svc._order_state_audit_log) > 1000:

                        svc._order_state_audit_log = svc._order_state_audit_log[-500:]

                    svc._order_last_update_time[oid] = time.time()

                    if sts in ('PARTIAL_FILLED', '部分成交') and filled >= order.get('volume', 0) and order.get('volume', 0) > 0:

                        order['status'] = 'FILLED'

                        logging.info("[R23-SM-P1-01-FIX] PARTIAL_FILLED→FILLED自动转移: order_id=%s filled=%d volume=%d", oid, filled, order.get('volume', 0))

                    elif sts in ('PARTIAL_FILLED', '部分成交') and 0 < filled < order.get('volume', 0):

                        _partial_ratio = filled / order.get('volume', 0)

                        logging.warning("[BIZ-P1-10] 订单部分成交: order_id=%s filled=%d/%d ratio=%.1f%%", oid, filled, order.get('volume', 0), _partial_ratio * 100)

                        try:

                            from infra.event_bus import get_global_event_bus

                            _bus = get_global_event_bus()

                            if _bus is not None:

                                _bus.publish('order.partial_filled', {'order_id': oid, 'traded_volume': filled, 'total_volume': order.get('volume', 0), 'partial_ratio': _partial_ratio}, async_mode=True)

                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                            logging.debug("[R3-L2] suppressed exception", exc_info=True)

                            pass

                            pass

                    if sts in ('FILLED', 'ALL_FILLED', '全成', '全部成交', '部成部撤'):

                        svc._stats['successful_orders'] += 1

                        svc._release_margin_reservation(oid)

                        self.remove_pending(oid)

                    if sts in ('CANCELLED', 'FAILED', '已撤销', '部成部撤'):

                        svc._release_margin_reservation(oid)

                        self.remove_pending(oid)

                    if sts == 'FAILED':

                        _retry_count = order.get('retry_count', 0)

                        _max_retries = 3

                        if _retry_count < _max_retries:

                            logging.info("[P1-R9-27] 订单FAILED, 触发自动重试: order=%s retry=%d/%d", oid, _retry_count + 1, _max_retries)

                            svc._chase_reorder(order, _retry_count + 1)

                        else:

                            logging.warning("[P1-R9-27] 订单FAILED且已达最大重试次数 order=%s retries=%d", oid, _retry_count)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[R16-P2-8.5] on_trade_update异常: %s", e, exc_info=True)



    def scan_order_timeouts(self, svc) -> int:

        _before = self.pending_count

        self.check_pending_orders_full(svc)

        _after = self.pending_count

        _processed = _before - _after

        if _processed > 0:

            logging.info("[R23-SM-04-FIX] scan_order_timeouts: 处理器d个超时订单", _processed)
        return max(_processed, 0)



    def check_pending_orders_full(self, svc) -> None:

        from datetime import datetime

        now = datetime.now(CHINA_TZ)

        # [FIX-20260708-TZ] 安全时间差计算：兼容offset-aware和offset-naive的datetime
        def _safe_elapsed(dt_ref, dt_cmp):
            """计算时间差，自动处理timezone-aware/naive混合"""
            try:
                return (dt_ref - dt_cmp).total_seconds()
            except TypeError:
                # offset-aware与offset-naive混合：剥离timezone信息
                _naive_now = dt_ref.replace(tzinfo=None) if dt_ref.tzinfo else dt_ref
                _naive_cmp = dt_cmp.replace(tzinfo=None) if dt_cmp.tzinfo else dt_cmp
                return (_naive_now - _naive_cmp).total_seconds()

        timeout_orders = []

        with svc._lock:

            expired_order_ids = []

            for order_id, order in list(svc._orders_by_id.items()):

                if order['status'] not in ('SUBMITTED', 'PENDING', '未成交', '已报入未应答', '待报入', '部分成交', '部分撤单还在队列中', '部成部撤还在队列中', 'PARTIAL_FILLED'):

                    self.remove_pending(order_id)

                    # FIX-CLOSE-LOOKUP: 不再立即删除platform_id映射，否则on_trade回调查询订单时找不到映射，
                    # 导致平仓订单被误判为开仓（_add_position替代_reduce_position），触发自成交检测阻断。
                    # 映射将在订单过期清理时统一删除。
                    if order['status'] in ('FILLED', 'ALL_FILLED', 'CANCELLED', 'FAILED', '全成', '全部成交', '已撤销', '部成部撤', 'ORPHANED'):

                        elapsed = _safe_elapsed(now, order.get('updated_at', order.get('created_at', now)))

                        if elapsed > 300:

                            expired_order_ids.append(order_id)

                    continue

                if not self.get_pending(order_id):

                    self.add_pending(order_id, order)

                elapsed = _safe_elapsed(now, order.get('created_at', now))

                _order_action = order.get('action', '')

                _order_op_type = 'close' if _order_action in ('CLOSE', 'close') else 'open'

                _order_timeout = svc._operation_timeouts.get(_order_op_type, svc._operation_timeouts['default'])

                if elapsed > _order_timeout:

                    timeout_orders.append((order_id, dict(order)))

            for oid in expired_order_ids:

                _expired_order = svc._orders_by_id.get(oid, {})

                # FIX-R28-CONSISTENCY: 幂等key构造必须包含signal_id，与order_executor.py保持一致
                _expired_sig = _expired_order.get('signal_id', '')
                _expired_sig_suffix = f"_{_expired_sig}" if _expired_sig else ""
                _expired_key = f"{_expired_order.get('instrument_id', '')}_{_expired_order.get('direction', '')}_{_expired_order.get('action', '')}_{_expired_order.get('volume', '')}_{round(_expired_order.get('price', 0), 4)}{_expired_sig_suffix}"

                svc._order_idempotent_set.discard(_expired_key)

                # FIX-CLOSE-LOOKUP: 订单过期清理时统一删除platform_id映射
                _expired_pid = _expired_order.get('platform_order_id')
                if _expired_pid and str(_expired_pid) in svc._platform_id_to_order_id:
                    del svc._platform_id_to_order_id[str(_expired_pid)]

                del svc._orders_by_id[oid]

        for order_id, order_snapshot in timeout_orders:

            with svc._lock:

                current_order = svc._orders_by_id.get(order_id)

                if not current_order or current_order['status'] not in ('SUBMITTED', 'PENDING'):

                    self.remove_pending(order_id)

                    continue

                current_order['status'] = 'TIMEOUT'

                current_order['updated_at'] = datetime.now(CHINA_TZ)

                logging.info("[R23-SM-04-FIX] 订单超时标记TIMEOUT: order_id=%s elapsed=%.1fs", order_id, _safe_elapsed(datetime.now(CHINA_TZ), current_order.get('created_at', datetime.now(CHINA_TZ))))

            self.remove_pending(order_id)

            _current_traded = current_order.get('traded_volume', 0) if current_order else 0
            _current_volume = current_order.get('volume', 0) if current_order else 0
            _current_action = current_order.get('action', '') if current_order else ''

            if _current_traded > 0 and _current_action == 'CLOSE':
                logging.info("[R38-TIMEOUT-TRADED] CLOSE订单已部分/全部成交(traded=%d/%d)，跳过追单避免双重平仓: order_id=%s",
                             _current_traded, _current_volume, order_id)
                continue

            _cancel_ok = svc.cancel_order(order_id)

            if not _cancel_ok:

                logging.error("[R27-P0-FIX] 撤单失败,跳过追单避免重复持仓: order_id=%s", order_id)

                continue

            retry_count = current_order.get('retry_count', 0) if current_order else order_snapshot.get('retry_count', 0)

            if retry_count < svc._max_chase_retries:

                svc._chase_reorder(current_order or order_snapshot, retry_count + 1)



    def _cleanup_orders(self, svc) -> None:

        from datetime import datetime

        now = time.time()

        if now - svc._last_cleanup <= svc._cleanup_interval:

            return

        with svc._lock:

            svc._last_cleanup = now

            to_remove = [oid for oid, o in svc._orders_by_id.items()

                        if o['status'] in ('FILLED', 'CANCELLED', 'FAILED', 'ORPHANED', '全部成交', '已撤销', '部成部撤', 'ALL_FILLED')

                        and _safe_elapsed(datetime.now(CHINA_TZ), o['updated_at']) > 3600]

            for oid in to_remove:

                _order = svc._orders_by_id.get(oid, {})

                svc._history_audit.append({

                    'order_id': oid, 'instrument_id': _order.get('instrument_id', ''),

                    'direction': _order.get('direction', ''), 'signal_id': _order.get('signal_id', ''),

                    'status': _order.get('status', ''), 'cleanup_time': time.time(),

                })

                del svc._orders_by_id[oid]

            for oid in to_remove:

                svc._wal_delete(oid)

            if to_remove:

                logging.info("[OrderService] 清理%d个已完成订单", len(to_remove))