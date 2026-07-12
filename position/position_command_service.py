# MODULE_ID: M1-203
"""Position Command Service - 命令处理(写操作)

从position_service.py拆分(CC-09 Step2):
- on_trade: 成交回报处理
- on_tick: 行情检查
- _add_position: 添加持仓
- _reduce_position: 减少持仓
- _trigger_close_position: 触发平仓
- _schedule_close_retry: 平仓重试
- _cleanup_close_retry_executor: 清理重试线程池
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any, Tuple

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False
    np = None
if _HAS_NUMPY and np is None:
    _HAS_NUMPY = False

from ali2026v3_trading.infra.metrics_registry import count_call
from ali2026v3_trading.infra.shared_utils import api_version  # P1-21: 统一从shared_utils导入
from ali2026v3_trading.position.position_greeks import _REASON_STRATEGY_MAP


class PositionCommandService:
    """命令处理服务 — 从PositionService拆分"""

    CLOSE_RETRY_MAX_ATTEMPTS = 3
    # FIX-20260704-RETRY-INTERVAL: 重试基础间隔从0.1s增加到1.0s，避免收盘后200ms内8次无效重试
    # 根因: 收盘后所有平仓返回result=-1，0.1s间隔导致76笔下单在2秒内全部失败+耗尽重试
    CLOSE_RETRY_BASE_DELAY_SEC = 1.0
    CLOSE_RETRY_MAX_THREADS = 10
    _MAX_CLOSE_RETRY_THREADS = CLOSE_RETRY_MAX_THREADS

    def __init__(self, position_service: Any):
        self._ps = position_service
        self._close_retry_executor = None

    def _resolve_strategy_group(self, open_reason: str) -> str:
        mapper = getattr(self._ps, '_map_reason_to_strategy', None)
        if callable(mapper):
            try:
                return mapper(open_reason)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as err:
                logging.warning("[PositionCommandService] _map_reason_to_strategy调用失败，使用回退映射: %s", err)
        return _REASON_STRATEGY_MAP.get(open_reason, 'high_freq')

    @staticmethod
    def _compute_profit_slope(history: list) -> float | None:
        """P2-32修复: 提取线性回归斜率计算，消除2x重复代码块"""
        if len(history) < 5:
            return None
        if _HAS_NUMPY:
            try:
                return float(np.polyfit(range(len(history)), history, 1)[0])
            except (ValueError, np.linalg.LinAlgError):
                pass
        # 纯Python fallback: OLS斜率
        n = len(history)
        x_mean = (n - 1) / 2.0
        y_mean = sum(history) / n
        num = sum((i - x_mean) * (history[i] - y_mean) for i in range(n))
        den = sum((i - x_mean) ** 2 for i in range(n))
        return num / den if den > 0 else 0.0

    def _infer_close_from_position(self, inst_id: str, is_buy: bool, is_open: bool, volume: int) -> bool:
        if not is_open or not inst_id:
            return False
        try:
            with self._ps._get_instrument_lock(inst_id):
                pos_dict = self._ps.positions.get(inst_id, {})
                if not pos_dict:
                    return False
                for rec in pos_dict.values():
                    if is_buy and rec.volume < 0:
                        return True
                    if not is_buy and rec.volume > 0:
                        return True
                return False
        except (ValueError, KeyError, TypeError, AttributeError):
            return False

    def on_trade(self, trade: Any) -> None:
        from ali2026v3_trading.infra.shared_utils import require_precondition
        require_precondition(trade is not None, "on_trade: trade不能为None")
        try:
            # FIX-CLOSE-LOOKUP: 兼容dict和object两种trade格式
            # FIX-R35-DICT: 修复原实现只使用getattr不支持dict的bug
            def _get_trade_attr(name, default=None):
                if isinstance(trade, dict):
                    return trade.get(name, default)
                return getattr(trade, name, default)

            inst_id = _get_trade_attr("instrument_id", "")
            exch = _get_trade_attr("exchange", "")
            d_raw = _get_trade_attr("direction", "")
            o_raw = _get_trade_attr("offset", "")
            price = _get_trade_attr("price", 0)
            volume = _get_trade_attr("volume", 0)
            order_id = str(_get_trade_attr("order_id", ""))
            # FIX-R34-MEMO: PythonGO平台规范 - TradeData.memo字段包含发送订单时填入的内部ID(ORD_xxx)
            # memo比order_id(平台ID)更可靠，优先使用memo查找内部订单
            trade_memo = str(_get_trade_attr("memo", "") or "")
            logging.info("[R28-ON_TRADE] inst=%s dir=%s offset=%s price=%s vol=%s order_id=%s memo=%s",
                        inst_id, d_raw, o_raw, price, volume, order_id, trade_memo)
            try:
                price = float(price) if price is not None else 0.0
                volume = round(float(volume)) if volume is not None else 0  # [R26-AUDIT] round替代int(float())避免截断
            except (ValueError, TypeError):
                logging.warning("[R24-P2-IV-06] on_trade: price/volume类型异常 price=%s volume=%s instrument=%s",
                              price, volume, inst_id)
                price = 0.0
                volume = 0
            is_buy = (str(d_raw) == "0")
            is_open = (str(o_raw) == "0")

            # FIX-CLOSE-LOOKUP: 通过order_id回查订单action和direction，修正平台offset/direction字段不正确导致平仓被误判为开仓的问题
            # 同时添加fallback机制：order_id查找失败时通过instrument+action=CLOSE查找最近的平仓订单
            # FIX-R30: fallback排除CANCELLED/FAILED而非包含特定状态，避免遗漏PARTIAL_FILLED等状态；
            # FIX-R30: datetime比较使用安全默认值，避免updated_at缺失时与int比较报错
            def _safe_ts_fallback(_o):
                try:
                    from datetime import datetime as _dt
                    _ts = _o.get('updated_at') or _o.get('created_at')
                    if isinstance(_ts, _dt):
                        return _ts.timestamp()
                    try:
                        return float(_ts) if _ts else 0.0
                    except (ValueError, TypeError):
                        return 0.0
                except Exception:
                    return 0.0

            def _fallback_find_close_order(_inst, _osvc):
                """通过instrument查找最近的CLOSE订单作为fallback"""
                try:
                    from datetime import datetime as _dt
                    _now_ts = __import__('time').time()
                    _candidates = []
                    # FIX-R32-ACTION-EMPTY: 当action为空时（重启后恢复的旧订单），
                    # 也检查direction与持仓方向相反的订单作为CLOSE候选
                    _pos_dict = self._ps.positions.get(_inst, {})
                    _has_long_pos = any(getattr(_r, 'volume', 0) > 0 for _r in _pos_dict.values())
                    _has_short_pos = any(getattr(_r, 'volume', 0) < 0 for _r in _pos_dict.values())
                    for _o in _osvc._orders_by_id.values():
                        if _o.get('instrument_id') != _inst:
                            continue
                        _o_action = _o.get('action', '')
                        _o_dir = _o.get('direction', '')
                        _o_status = _o.get('status', '')
                        if _o_status in ('CANCELLED', 'FAILED', 'ORPHANED', '已撤销', '部成部撤') and not (_o_status in ('CANCELLED', '已撤销') and _now_ts - _safe_ts_fallback(_o) < 120):
                            continue
                        # action=='CLOSE' 直接匹配
                        if _o_action == 'CLOSE':
                            _candidates.append(_o)
                        # FIX-R32-ACTION-EMPTY: action为空时，通过direction与持仓方向推断
                        elif not _o_action:
                            if (_o_dir in ('SELL', '1') and _has_long_pos) or (_o_dir in ('BUY', '0') and _has_short_pos):
                                _candidates.append(_o)
                    if _candidates:
                        def _safe_ts(_o):
                            _ts = _o.get('updated_at') or _o.get('created_at')
                            if isinstance(_ts, _dt):
                                return _ts.timestamp()
                            try:
                                return float(_ts) if _ts else 0.0
                            except (ValueError, TypeError):
                                return 0.0
                        # FIX-R37-STALE-ORDER: 优先选择有created_at的当前session订单，
                        # 避免匹配到重启恢复的旧session订单(direction可能不正确)
                        _fresh = [o for o in _candidates if o.get('created_at') is not None]
                        if _fresh:
                            _candidates = _fresh
                        return max(_candidates, key=_safe_ts)
                except (ValueError, KeyError, TypeError):
                    pass
                return None

            if order_id or trade_memo.startswith('ORD_'):
                try:
                    from ali2026v3_trading.order.order_service import get_order_service
                    _osvc = get_order_service()
                    if _osvc:
                        # FIX-R34-MEMO: 优先使用memo(内部ID)查找订单，比order_id(平台ID)更可靠
                        # memo是发送订单时填入的内部ID(ORD_xxx)，平台回调原样返回
                        _order = None
                        if trade_memo.startswith('ORD_'):
                            _order = _osvc.get_order(trade_memo)
                            if _order:
                                logging.info("[R34-MEMO-ON_TRADE] memo直接查找成功: memo=%s action=%s dir=%s inst=%s",
                                            trade_memo, _order.get('action', ''), _order.get('direction', ''),
                                            _order.get('instrument_id', ''))
                                # 同时建立platform_id→internal_id映射
                                if order_id:
                                    _existing_pid = _order.get('platform_order_id', '')
                                    _need_update = (not _existing_pid
                                                    or str(_existing_pid) == trade_memo
                                                    or str(_existing_pid).startswith('ORD_'))
                                    if _need_update:
                                        with _osvc._lock:
                                            _order['platform_order_id'] = str(order_id)
                                            _osvc._platform_id_to_order_id[str(order_id)] = trade_memo
                        if not _order and order_id:
                            _order = _osvc.get_order(order_id)
                        # FIX-R29b: 平台回调的order_id可能是平台ID(如"27")，需用platform_id映射
                        if not _order and order_id:
                            _order = _osvc.get_order_by_platform_id(order_id)
                        if _order:
                            _action = _order.get('action', '')
                            if _action == 'CLOSE':
                                is_open = False
                            elif _action == 'OPEN':
                                is_open = True
                            # FIX-R37-ACTION-EMPTY-ROOT: 仅当action非空(可信订单)时才用订单direction覆盖is_buy。
                            # action为空说明是重启恢复的旧订单或被evict后通过platform_id错误匹配到的旧订单，
                            # 其direction可能属于完全不同的订单(如OPEN BUY被误匹配为CLOSE SELL)，
                            # 此时信任平台回调的原始direction/offset更安全。
                            _dir = _order.get('direction', '')
                            if _action:  # action非空才信任direction
                                # FIX-DIR-DUAL-TRACK: 兼容策略内部值('BUY'/'SELL')和平台规范值('0'/'1')
                                if _dir in ('BUY', '0'):
                                    is_buy = True
                                elif _dir in ('SELL', '1'):
                                    is_buy = False
                            else:
                                _matched_by_closing_oid = False
                                with self._ps._get_instrument_lock(inst_id):
                                    _pos_dict = self._ps.positions.get(inst_id, {})
                                    for _r in _pos_dict.values():
                                        _r_closing_oid = getattr(_r, 'closing_order_id', '')
                                        if _r_closing_oid and _r_closing_oid in (order_id, trade_memo, str(order_id)):
                                            is_open = False
                                            is_buy = _r.volume < 0
                                            _matched_by_closing_oid = True
                                            logging.info("[R37-CLOSING-OID-MATCH] action为空但closing_order_id精确匹配: "
                                                        "inst=%s closing_oid=%s -> is_open=False is_buy=%s",
                                                        inst_id, _r_closing_oid, is_buy)
                                            break
                                if not _matched_by_closing_oid:
                                    logging.warning("[R37-ACTION-EMPTY] order_id=%s memo=%s action为空且无closing_order_id匹配，"
                                                   "保留平台原始方向 is_open=%s is_buy=%s order_dir=%s",
                                                   order_id, trade_memo, is_open, is_buy, _dir)
                            logging.info("[R29b-on_trade] order_id=%s memo=%s 回查成功 action=%s dir=%s -> is_open=%s is_buy=%s",
                                        order_id, trade_memo, _action, _dir, is_open, is_buy)
                        else:
                            # FIX-CLOSE-LOOKUP: order_id查找失败时，尝试通过instrument fallback查找最近的CLOSE订单
                            logging.warning("[R29b-on_trade] order_id=%s memo=%s 未找到对应订单，尝试fallback查找",
                                          order_id, trade_memo)
                            _fallback = _fallback_find_close_order(inst_id, _osvc)
                            if _fallback:
                                is_open = False
                                _fb_dir = _fallback.get('direction', '')
                                if _fb_dir in ('BUY', '0'):
                                    is_buy = True
                                elif _fb_dir in ('SELL', '1'):
                                    is_buy = False
                                logging.info("[R29b-on_trade] fallback匹配成功 inst=%s order_id=%s action=CLOSE -> is_open=False",
                                            inst_id, _fallback.get('order_id', ''))
                            else:
                                logging.warning("[R29b-on_trade] order_id=%s memo=%s fallback也未找到CLOSE订单",
                                              order_id, trade_memo)
                                _matched_by_closing_oid = False
                                with self._ps._get_instrument_lock(inst_id):
                                    _pos_dict = self._ps.positions.get(inst_id, {})
                                    for _r in _pos_dict.values():
                                        _r_closing_oid = getattr(_r, 'closing_order_id', '')
                                        if _r_closing_oid and _r_closing_oid in (order_id, trade_memo, str(order_id)):
                                            is_open = False
                                            is_buy = _r.volume < 0
                                            _matched_by_closing_oid = True
                                            logging.info("[R29b-CLOSING-OID-MATCH] fallback通过closing_order_id精确匹配: "
                                                        "inst=%s closing_oid=%s -> is_open=False is_buy=%s",
                                                        inst_id, _r_closing_oid, is_buy)
                                            break
                                if not _matched_by_closing_oid and self._infer_close_from_position(inst_id, is_buy, is_open, volume):
                                    is_open = False
                                    logging.info("[R29b-on_trade] 基于持仓推断为平仓: inst=%s is_buy=%s -> is_open=False", inst_id, is_buy)
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r29_err:
                    logging.debug("[R29b-on_trade] order_id=%s memo=%s action/direction回查失败: %s",
                                order_id, trade_memo, _r29_err)
            else:
                # FIX-CLOSE-LOOKUP: order_id为空时，尝试通过instrument fallback查找最近的CLOSE订单
                logging.warning("[R29b-on_trade] trade对象无order_id，尝试通过instrument=%s fallback查找CLOSE订单", inst_id)
                try:
                    from ali2026v3_trading.order.order_service import get_order_service
                    _osvc = get_order_service()
                    if _osvc:
                        _fallback = _fallback_find_close_order(inst_id, _osvc)
                        if _fallback:
                            is_open = False
                            _fb_dir = _fallback.get('direction', '')
                            if _fb_dir in ('BUY', '0'):
                                is_buy = True
                            elif _fb_dir in ('SELL', '1'):
                                is_buy = False
                            logging.info("[R29b-on_trade] 无order_id fallback匹配成功 inst=%s order_id=%s action=CLOSE -> is_open=False",
                                        inst_id, _fallback.get('order_id', ''))
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _fb_err:
                    logging.warning("[R29b-on_trade] 无order_id fallback查找异常: %s", _fb_err)

            if is_open and inst_id and self._ps.positions.get(inst_id):
                if self._infer_close_from_position(inst_id, is_buy, is_open, volume):
                    is_open = False
                    logging.info("[R29b-on_trade] 基于持仓推断为平仓(最终fallback): inst=%s is_buy=%s -> is_open=False", inst_id, is_buy)

            if self._ps.partial_fill_handler is not None:
                try:
                    traded_volume = _get_trade_attr("volume", volume)
                    total_volume = _get_trade_attr("volume", volume)
                    if order_id and total_volume > 0:
                        self._ps.partial_fill_handler.record_partial_fill(
                            order_id=order_id, instrument_id=inst_id,
                            traded_volume=traded_volume, total_volume=total_volume,
                        )
                        def _cancel_order(oid):
                            from ali2026v3_trading.order.order_service import get_order_service
                            osvc = get_order_service()
                            if osvc and hasattr(osvc, 'cancel_order'):
                                osvc.cancel_order(oid)
                            if self._ps.self_trade_detector is not None:
                                self._ps.self_trade_detector.remove_order(oid)
                        self._ps.partial_fill_handler.check_and_cancel_remaining(order_id, cancel_func=_cancel_order)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug(f"[PositionService.on_trade] PartialFillHandler error: {e}")

            if is_open:
                vol_signed = volume if is_buy else -volume
                open_reason = self._ps._get_open_reason_from_order(inst_id, order_id=order_id)
                # CHAIN-BUG-5 fix: 回查开仓signal_id贯通至持仓记录，平仓订单可回溯开仓信号
                _sig_id = self._ps._get_signal_id_from_order(inst_id, order_id=order_id)
                self._add_position(exch, inst_id, vol_signed, price, open_reason=open_reason, order_id=order_id, signal_id=_sig_id)
            else:
                self._reduce_position(exch, inst_id, volume, is_buy, price)
                if self._ps.self_trade_detector is not None and order_id:
                    self._ps.self_trade_detector.remove_order(order_id)
            logging.debug(f"[PositionService.on_trade] Updated: {inst_id} vol={volume}")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService.on_trade] Error: {e}")

    def on_tick(self, tick: Any) -> None:
        try:
            price = self._ps._get_platform_attr(tick, "last_price", default=0)
            from ali2026v3_trading.infra.shared_utils import safe_price_check
            if not safe_price_check(price):
                return
            inst_id = self._ps._get_platform_attr(tick, "instrument_id", default="")
            if not inst_id:
                return
            self._ps._pnl_svc._check_option_expiry(inst_id)
            _slope_updates = {}
            with self._ps._get_instrument_lock(inst_id):
                if inst_id in self._ps.positions:
                    for pid in list(self._ps.positions[inst_id]):
                        record = self._ps.positions[inst_id].get(pid)
                        if record is None:
                            continue
                        self._ps._pnl_svc._check_stop_profit(record, price)
                        self._ps._pnl_svc._check_stop_loss(record, price)
                        if record.volume != 0 and record.open_price > 0:
                            record.current_price = price
                            if record.volume > 0:
                                profit_pct = (price - record.open_price) / record.open_price
                            else:
                                profit_pct = (record.open_price - price) / record.open_price
                            prev_max = getattr(record, '_max_profit_pct', 0.0)
                            if profit_pct > prev_max:
                                record._max_profit_pct = profit_pct
                            if record._profit_history is None:
                                record._profit_history = []
                            record._profit_history.append(profit_pct)
                            if len(record._profit_history) >= 5:
                                _slope_updates[pid] = list(record._profit_history)

            _computed_slopes = {}
            for pid, history_snapshot in _slope_updates.items():
                _computed_slope = None
                if len(history_snapshot) >= 5:
                    _computed_slope = self._compute_profit_slope(history_snapshot)
                    if _computed_slope is not None:
                        _computed_slopes[pid] = _computed_slope

            if _computed_slopes:
                with self._ps._get_instrument_lock(inst_id):
                    if inst_id in self._ps.positions:
                        for pid, slope in _computed_slopes.items():
                            record = self._ps.positions[inst_id].get(pid)
                            if record is not None:
                                # FIX-UPDATE-TIMELY-08: 写入slope前验证profit_history长度未变，
                                # 避免锁外计算期间profit_history被追加新数据导致slope基于过期快照
                                _snapshot_len = len(_slope_updates.get(pid, []))
                                _current_hist_len = len(record._profit_history) if record._profit_history else 0
                                if _current_hist_len == _snapshot_len:
                                    record.profit_slope = slope
                                else:
                                    logging.debug("[UPDATE-TIMELY-08] profit_history长度变化(%d->%d)，跳过过期slope写入: inst=%s pid=%s",
                                                  _snapshot_len, _current_hist_len, inst_id, pid)

            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, TickEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(TickEvent(instrument_id=inst_id, tick_data=tick), async_mode=True)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService.on_tick] Error: {e}")

    @count_call()
    def _add_position(self, exchange: str, instrument_id: str,
                     volume: int, price: float, open_reason: str = '',
                     order_id: str = '', signal_id: str = '') -> None:
        # FIX-OPEN-UNIQUE(P5): price>0 校验必须在保证金检查之前执行，
        # 否则 price<=0 时保证金计算 new_margin=0 会错误通过检查，后续才抛异常
        if price <= 0:
            logging.error("[R26-P0-FI-09] _add_position拒绝price<=0: inst=%s price=%s", instrument_id, price)
            return
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            if rs:
                existing_margin = 0.0
                # FIX-OPEN-UNIQUE(P6): 保证金检查读取 positions 必须在 global_lock 内，
                # 否则并发开仓时可能读到不一致的持仓状态(如正在被 _reduce_position 修改的持仓)
                # 使用快照方式避免长时间持锁
                try:
                    with self._ps.global_lock:
                        _pos_snapshot = [(iid, dict(pdict)) for iid, pdict in self._ps.positions.items()]
                    for _inst_id, pos_dict in _pos_snapshot:
                        for _pid, rec in pos_dict.items():
                            if rec.volume != 0 and rec.open_price > 0:
                                _m_ratio = rs._get_margin_ratio(_inst_id) if hasattr(rs, '_get_margin_ratio') else 0.1
                                existing_margin += abs(rec.volume) * rec.open_price * _m_ratio
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
                _new_m_ratio = rs._get_margin_ratio(instrument_id) if hasattr(rs, '_get_margin_ratio') else 0.1  # R27-P0-FIX: symbol→instrument_id
                new_margin = abs(volume) * price * _new_m_ratio
                equity = 0.0
                try:
                    from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
                    _sid = str(getattr(self._ps, 'strategy_id', '') or 'global')
                    safety = get_safety_meta_layer(params=self._ps._params if hasattr(self._ps, '_params') else None, strategy_id=_sid)
                    if safety and safety._equity_series:
                        equity = safety._equity_series[-1]
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
                if equity > 0:
                    result = rs.check_capital_sufficiency(equity=equity, required_margin=new_margin, existing_margin_used=existing_margin)
                    if not result.get('sufficient', True):
                        logging.critical("[PositionService] R13-V4-001: 保证金不足防御性阻断! equity=%.2f existing_margin=%.2f new_margin=%.2f", equity, existing_margin, new_margin)
                        return
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[PositionService] R13-V4-001 margin check failed: %s", e)

        # FIX-OPEN-UNIQUE-06: self_trade_detector.add_order必须在instrument_lock内执行，
        # 否则并发开仓时两个线程可能同时通过自成交检测但都未及时add_order，导致后续订单漏检
        _pending_new_order_for_lock = None
        if self._ps.self_trade_detector is not None:
            try:
                from ali2026v3_trading.order.order_persistence import OrderRecord
                new_order = OrderRecord(
                    order_id=f"temp_{int(datetime.now(_CHINA_TZ).timestamp()*1000)}",
                    instrument_id=instrument_id, direction="buy" if volume > 0 else "sell",
                    price=price, volume=abs(volume), timestamp=datetime.now(_CHINA_TZ).timestamp(),
                )
                if self._ps.self_trade_detector is not None:  # [R27-AUDIT] P1修复: 添加None检查
                    is_self_trade, alert_msg = self._ps.self_trade_detector.check_self_trade(new_order)
                    if is_self_trade:
                        # FIX-R30-DEFENSE: 自成交阻断时检查是否存在近期CLOSE订单，若存在则说明
                        # 此trade实为平仓被误判为开仓（所有order_id查找和fallback均失败），回退到_reduce_position
                        _redirected = False
                        try:
                            from ali2026v3_trading.order.order_service import get_order_service
                            _osvc = get_order_service()
                            if _osvc:
                                _has_close = False
                                for _o in _osvc._orders_by_id.values():
                                    if (_o.get('instrument_id') == instrument_id
                                        and _o.get('action') == 'CLOSE'
                                        and _o.get('status') not in ('CANCELLED', 'FAILED', 'ORPHANED', '已撤销', '部成部撤')):
                                        _has_close = True
                                        break
                                # FIX-R32-ACTION-EMPTY: 当action为空时（重启后恢复的旧订单），
                                # 无法通过action=='CLOSE'识别平仓订单。此时通过持仓方向推断：
                                # 如果当前_add_position的volume方向与现有持仓相反，说明是平仓被误判为开仓
                                if not _has_close:
                                    with self._ps._get_instrument_lock(instrument_id):
                                        _pos_dict = self._ps.positions.get(instrument_id, {})
                                        for _rec in _pos_dict.values():
                                            _rec_vol = getattr(_rec, 'volume', 0)
                                            if _rec_vol != 0 and ((volume > 0 and _rec_vol < 0) or (volume < 0 and _rec_vol > 0)):
                                                _has_close = True
                                                logging.warning("[R32-ACTION-EMPTY] inst=%s action为空，通过持仓方向推断为平仓(现有vol=%d 新增vol=%d)",
                                                              instrument_id, _rec_vol, volume)
                                                break
                                if _has_close:
                                    logging.warning("[R30-DEFENSE] 自成交阻断但检测到CLOSE订单，回退到_reduce_position: inst=%s vol=%s price=%s",
                                                  instrument_id, volume, price)
                                    self._reduce_position(exchange, instrument_id, abs(volume), volume > 0, price)
                                    _redirected = True
                        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _def_err:
                            logging.debug("[R30-DEFENSE] CLOSE订单检查异常: %s", _def_err)
                        if not _redirected:
                            logging.error(f"[PositionService._add_position] 自成交检测阻断: {alert_msg}")
                            return
                        # FIX-R30: redirected时也return，避免继续执行_add_position的持仓创建逻辑导致重复持仓
                        return
                    else:
                        # FIX-OPEN-UNIQUE-06: 延迟到instrument_lock内执行add_order，保证检测-注册原子性
                        _pending_new_order_for_lock = new_order
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning(f"[PositionService._add_position] 自成交检测异常: {e}")

        with self._ps._get_instrument_lock(instrument_id):
            # FIX-OPEN-UNIQUE-06: 在instrument_lock内注册self_trade_detector订单，
            # 保证check_self_trade通过后立即注册，避免并发开仓漏检
            if _pending_new_order_for_lock is not None and self._ps.self_trade_detector is not None:
                try:
                    self._ps.self_trade_detector.add_order(_pending_new_order_for_lock)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _add_err:
                    logging.warning("[PositionService._add_position] self_trade_detector.add_order异常: %s", _add_err)
            if instrument_id not in self._ps.positions:
                self._ps.positions[instrument_id] = {}
            # FIX-OPEN-UNIQUE(P4): signal_id 级别开仓去重，
            # 防止同一信号重复开仓导致持仓重复。原代码仅有策略组级别去重(>=4)，
            # 但同一 signal_id 的重复开仓(如信号重放/网络重试)会导致同一信号创建多个持仓。
            if signal_id:
                _dup_count = sum(1 for _r in self._ps.positions[instrument_id].values()
                                 if getattr(_r, 'signal_id', '') == signal_id
                                 and getattr(_r, 'volume', 0) != 0)
                if _dup_count > 0:
                    logging.warning("[OPEN-UNIQUE] signal_id重复开仓被去重阻断: inst=%s signal_id=%s existing=%d",
                                    instrument_id, signal_id, _dup_count)
                    return
            _strategy_group = self._resolve_strategy_group(open_reason)
            # FIX-OPEN-UNIQUE-05: 同合约同策略组同方向持仓计数时，排除已在平仓中的持仓(closing_order_id非空)，
            # 否则已触发平仓但尚未完全成交的持仓会阻止新的开仓信号
            # FIX-OPEN-UNIQUE-09: 策略组为空时单独归类为'_default_'，避免空策略组与所有空策略组持仓合并计数
            _group_key = _strategy_group if _strategy_group else '_default_'
            _existing_same_group = sum(1 for _r in self._ps.positions[instrument_id].values()
                                       if (getattr(_r, 'strategy_group', '') or '_default_') == _group_key
                                       and getattr(_r, 'volume', 0) * volume > 0
                                       and not getattr(_r, 'closing_order_id', ''))
            if _existing_same_group >= 4:
                logging.warning("[OPEN-UNIQUE] 同合约同策略组同方向已有%d个持仓(排除平仓中)，跳过开仓: inst=%s group=%s dir=%s",
                                _existing_same_group, instrument_id, _group_key, "long" if volume > 0 else "short")
                return
            # FIX-OPEN-UNIQUE-08: position_id增加微秒精度+8位随机ID，避免高频场景下毫秒时间戳冲突
            _ts_us = int(datetime.now(_CHINA_TZ).timestamp() * 1000000)
            pos_id = f"{instrument_id}_{_strategy_group}_{_ts_us}_{generate_prefixed_id('', 8)}"
            _signal_snapshot = f"sig={signal_id}|reason={open_reason}|strat={_strategy_group}|order={order_id}"
            direction_str = "long" if volume > 0 else "short"
            p_type = "long" if volume > 0 else "short"
            sp_price = 0.0
            sl_price = 0.0
            try:
                tp_ratio, sl_ratio = self._ps._get_tp_sl_ratios_by_reason(open_reason)
                sl_ratio = self._ps._apply_crm_stop_loss_adjustment(sl_ratio, open_reason)
                self._ps._verify_tp_sl_alignment_with_backtest(open_reason, tp_ratio, sl_ratio)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning(f"[PositionService._add_position] Failed to get TP/SL ratio: {e}")
                tp_ratio = self._ps.DEFAULT_TP_RATIO
                sl_ratio = self._ps.DEFAULT_SL_RATIO
            # [R28-C009修复] NaN/Inf检查: P2-20修复提取为期validate_finite辅助
            import math as _math
            def _validate_finite(value, name):
                if not _math.isfinite(value):
                    raise ValueError(f"[R28-C009] {name} is not finite (NaN/Inf): {value}, instrument={instrument_id}")
            _validate_finite(price, 'open_price')
            if price <= 0:
                raise ValueError(f"[R26-P0-FI-09] open_price={price:.2f}<=0, instrument={instrument_id}")
            _validate_finite(tp_ratio, 'tp_ratio')
            _validate_finite(sl_ratio, 'sl_ratio')
            if tp_ratio <= 0:
                raise ValueError(f"[R26-P0-BV-03] tp_ratio={tp_ratio:.4f}<=0, instrument={instrument_id}")
            if volume == 0:
                raise ValueError(f"[R28-C009] volume=0, instrument={instrument_id}")
            if not _math.isfinite(volume):
                raise ValueError(f"[R28-C009] volume is not finite: {volume}, instrument={instrument_id}")

            # 计算止盈止损价格（所有参数已通过有效性校验）'
            if volume > 0:
                sp_price = price * tp_ratio
                sl_price = price * (1 - sl_ratio)
            else:
                sp_price = price / tp_ratio
                sl_price = price * (1 + sl_ratio)

            from ali2026v3_trading.position.position_service import PositionRecord

            # [R28-C023修复] 交易前捕获快照，用于错误交易回滚
            _prev_pos = self._ps.positions[instrument_id].get(pos_id)
            try:
                from ali2026v3_trading.strategy_judgment.causal_chain_utils import TradeRollbackManager
                _rbm = TradeRollbackManager.get_instance()
                _rbm.capture_pre_trade_snapshot(
                    position_id=pos_id,
                    instrument_id=instrument_id,
                    correlation_id=signal_id or pos_id,
                    position_existed=_prev_pos is not None,
                    prev_volume=_prev_pos.volume if _prev_pos else 0,
                    prev_direction=_prev_pos.direction if _prev_pos else "",
                    prev_open_price=_prev_pos.open_price if _prev_pos else 0.0,
                    prev_stop_profit_price=_prev_pos.stop_profit_price if _prev_pos else 0.0,
                    prev_stop_loss_price=_prev_pos.stop_loss_price if _prev_pos else 0.0,
                    prev_current_plr=_prev_pos.current_plr if _prev_pos else 0.0,
                    prev_max_profit_pct=_prev_pos._max_profit_pct if _prev_pos else 0.0,
                    prev_chase_count=_prev_pos.chase_count if _prev_pos else 0,
                )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _rb_e:
                logging.debug("[R28-C023] 快照捕获跳过: %s", _rb_e)

            record = PositionRecord(
                position_id=pos_id, instrument_id=instrument_id, exchange=exchange,
                volume=volume, direction=direction_str, open_price=price,
                open_time=datetime.now(_CHINA_TZ), open_date=datetime.now(_CHINA_TZ).date(),
                position_type=p_type, stop_profit_price=sp_price, stop_loss_price=sl_price,
                open_reason=open_reason, order_id=order_id, signal_id=signal_id,
                strategy_group=_strategy_group,
                option_premium=self._ps._compute_option_premium(instrument_id, price),
                open_signal_snapshot=_signal_snapshot,
                # FIX-OPEN-UNIQUE(P3): 显式传 _closing=False，避免依赖 dataclass 默认值
                # 在未来字段顺序变更或默认值修改时导致 _closing 误设为 True
                _closing=False,
                closing_order_id='',
            )
            self._ps.positions[instrument_id][pos_id] = record
            assert '_' in pos_id, f"position_id必须包含策略组分隔符: {pos_id}"
            assert record.strategy_group, f"strategy_group不能为空: pos_id={pos_id}"
            assert record.open_signal_snapshot, f"open_signal_snapshot不能为空: pos_id={pos_id}"
            assert record.instrument_id in pos_id, f"position_id必须以instrument_id开头: {pos_id} vs {instrument_id}"
            logging.info(f"[PositionService._add_position] Added: {instrument_id} {volume}手@ {price} reason={open_reason}")
            try:
                from ali2026v3_trading.strategy_judgment.market_snapshot_collector import SnapshotTrigger
                _sc = getattr(self._ps, '_snapshot_collector', None)
                if _sc is not None and hasattr(_sc, 'capture_order_event'):
                    _sc.capture_order_event(
                        timestamp=None,
                        event_type=SnapshotTrigger.ORDER_OPENED,
                        trigger_detail=f"OPEN {instrument_id} {direction_str} reason={open_reason}",
                        order_info={'position_id': pos_id, 'instrument_id': instrument_id,
                                    'direction': direction_str, 'price': price, 'volume': abs(volume),
                                    'open_reason': open_reason, 'signal_id': signal_id},
                    )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _snap_err:
                logging.debug("[PositionService] 开仓快照采集跳过: %s", _snap_err)
            self._ps._append_position_state(instrument_id, pos_id, 'OPEN', {'volume': volume, 'price': price, 'open_reason': open_reason,
                # FIX-R37-POS-RECOVER: 保存完整PositionRecord快照，重启后可恢复完整持仓
                # (open_time/stop_profit_price/stop_loss_price等)，否则时间止损/止盈止损检查失效
                'snapshot': record.to_dict()}, signal_id=signal_id)
            if self._ps._structured_logger is not None:
                try:
                    self._ps._structured_logger.log_order({
                        "order_id": pos_id, "instrument_id": instrument_id,
                        "direction": "buy" if volume > 0 else "sell", "price": price,
                        "volume": abs(volume), "order_type": "OPEN", "status": "filled",
                        "traded_volume": abs(volume), "remaining_volume": 0,
                    })
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, PositionEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(PositionEvent(instrument_id=instrument_id, position=float(volume), avg_price=price, action='OPENED'), async_mode=True)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

    def _reduce_position(self, exchange: str, instrument_id: str, volume: int, is_buy: bool, price: float) -> None:
        volume = abs(volume)
        assert volume > 0, f"_reduce_position: volume必须>0, got {volume} inst={instrument_id}"
        assert price > 0, f"_reduce_position: price必须>0, got {price} inst={instrument_id}"
        with self._ps._get_instrument_lock(instrument_id):
            # FIX-P2-1: 平仓前强制刷新持仓，避免异步成交回报导致closeable=0误判
            # 证据: CF609P15600 closeable=0但策略尝试平仓，持仓可能已被前序成交平掉
            # 根因: 持仓数据在成交回报回调中更新，但平仓请求时可能读到旧数据
            try:
                _refresh_fn = getattr(self._ps, '_refresh_position_from_platform', None)
                if _refresh_fn and callable(_refresh_fn):
                    _refresh_fn(instrument_id)
            except (ValueError, KeyError, TypeError, AttributeError) as _refresh_err:
                logging.debug("[FIX-P2-1] 持仓刷新失败(使用缓存数据): %s", _refresh_err)
            if instrument_id not in self._ps.positions:
                # FIX-R37-POS-NOT-FOUND: 持仓不存在时记录警告，便于排查"平仓时持仓不足"问题
                logging.warning("[R37-POS-NOT-FOUND] 平仓时持仓不存在: inst=%s vol=%d is_buy=%s price=%.4f",
                                instrument_id, volume, is_buy, price)
                return
            records = sorted(self._ps.positions[instrument_id].values(), key=lambda x: x.open_time)
            # FIX-R37-POS-INSUFFICIENT: 预计算可平仓总量，提前检测持仓不足
            _total_closeable = sum(abs(r.volume) for r in records
                                    if (is_buy and r.volume < 0) or (not is_buy and r.volume > 0))
            if _total_closeable < volume:
                logging.warning("[R37-POS-INSUFFICIENT] 平仓量超过持仓量: inst=%s close_vol=%d closeable=%d is_buy=%s price=%.4f",
                                 instrument_id, volume, _total_closeable, is_buy, price)
                # FIX-R37-POS-INSUFFICIENT-ROOT: closeable==0时直接return，避免重复平仓导致
                # 持仓数据被错误修改(持仓已为0但仍执行reduce逻辑，产生误导性"Reduced"日志)
                if _total_closeable == 0:
                    logging.warning("[R37-POS-INSUFFICIENT] inst=%s closeable=0, 跳过平仓(持仓可能已被前序成交平掉)", instrument_id)
                    return
            remaining_close = min(volume, _total_closeable) if _total_closeable < volume else volume
            # FIX-R37-ACTUAL-CLOSED: 保存截断后的初始remaining_close，用于正确计算_actual_closed
            # 原逻辑 _actual_closed = volume - remaining_close 在截断后会得到错误的值(=volume而非实际平仓量)
            _initial_remaining = remaining_close
            keys_to_remove = []
            for rec in records:
                if remaining_close <= 0:
                    break
                if is_buy and rec.volume > 0:
                    continue
                if not is_buy and rec.volume < 0:
                    continue
                can_close = abs(rec.volume)
                if can_close <= remaining_close:
                    remaining_close -= can_close
                    keys_to_remove.append(rec.position_id)
                    # FIX-R37-REALIZED-PNL: 按实际平仓量计算并累加已实现盈亏到服务级总计
                    # FIX-DIR-DUAL-TRACK: 兼容record.direction的'long'/'short'和防御性兼容'BUY'/'0'
                    _pnl_mult = 1.0 if getattr(rec, 'direction', '') in ('long', 'BUY', '0') else -1.0
                    _closed_pnl = _pnl_mult * (price - rec.open_price) * can_close
                    self._ps._total_realized_pnl = getattr(self._ps, '_total_realized_pnl', 0.0) + _closed_pnl
                else:
                    if rec.volume > 0:
                        rec.volume -= remaining_close
                    else:
                        rec.volume += remaining_close
                    # FIX-R37-REALIZED-PNL: 部分平仓也累加已实现盈亏
                    # FIX-DIR-DUAL-TRACK: 兼容record.direction的'long'/'short'和防御性兼容'BUY'/'0'
                    _pnl_mult = 1.0 if getattr(rec, 'direction', '') in ('long', 'BUY', '0') else -1.0
                    _partial_pnl = _pnl_mult * (price - rec.open_price) * remaining_close
                    self._ps._total_realized_pnl = getattr(self._ps, '_total_realized_pnl', 0.0) + _partial_pnl
                    # FIX-R35-CLOSING-RESET: 部分平仓后必须重置_closing标志，
                    # 否则后续止盈止损/时间止损检查时_trigger_close_position会因_closing=True直接返回，
                    # 导致剩余持仓无法再次平仓，持仓永远无法清零
                    if hasattr(rec, '_closing') and rec._closing:
                        rec._closing = False
                        if hasattr(rec, 'closing_order_id'):
                            rec.closing_order_id = ''
                        # FIX-CLOSE-UNIQUE-10: 部分平仓后重置close_method，避免残留的平仓方式
                        # 误导后续平仓触发路径(如close_method='stop_loss'但实际是部分平仓后剩余持仓)
                        if hasattr(rec, 'close_method'):
                            rec.close_method = ''
                        # FIX-CLOSE-UNIQUE-10-ASSERT: 断言部分平仓后状态已重置
                        assert not rec._closing, f"部分平仓后_closing必须为False: pos_id={rec.position_id}"
                        assert not getattr(rec, 'closing_order_id', ''), \
                            f"部分平仓后closing_order_id必须为空: pos_id={rec.position_id}"
                        assert not getattr(rec, 'close_method', ''), \
                            f"部分平仓后close_method必须为空: pos_id={rec.position_id}"
                        logging.info("[R35-CLOSING-RESET] 部分平仓后重置_closing: inst=%s pos_id=%s remaining_vol=%d",
                                     instrument_id, rec.position_id, abs(rec.volume))
                    if hasattr(rec, 'stop_profit_price') and rec.stop_profit_price and rec.stop_profit_price > 0:
                        _orig_tp = rec.stop_profit_price
                        _tp_ratio = abs(_orig_tp / rec.open_price - 1) if rec.open_price > 0 else 0
                        rec.stop_profit_price = rec.open_price * (1 + _tp_ratio) if rec.open_price > 0 else _orig_tp
                    if hasattr(rec, 'stop_loss_price') and rec.stop_loss_price and rec.stop_loss_price > 0:
                        _orig_sl = rec.stop_loss_price
                        _sl_ratio = abs(rec.open_price / _orig_sl - 1) if _orig_sl > 0 and rec.open_price > 0 else 0
                        rec.stop_loss_price = rec.open_price * (1 - _sl_ratio) if rec.open_price > 0 else _orig_sl
                    logging.info("[PositionService] R14-P1-BIZ-08: 部分平仓后重算止盈止损 instrument=%s vol=%d", rec.instrument_id, rec.volume)
                    # FIX-R37-UNIQUE-UPDATE(E2): 部分平仓必须持久化 UPDATE 记录，
                    # 否则重启后 volume 恢复为开仓原始量，导致持仓量与交易所不一致。
                    # 原代码仅修改内存 rec.volume，未调用 _append_position_state，
                    # 重启后恢复逻辑从 OPEN snapshot 重建，volume 为开仓原始量。
                    try:
                        self._ps._append_position_state(
                            instrument_id, rec.position_id, 'UPDATE',
                            {'snapshot': rec.to_dict(),
                             'partial_closed_volume': remaining_close,
                             'remaining_volume': abs(rec.volume)},
                            signal_id=getattr(rec, 'signal_id', ''))
                    except (ValueError, KeyError, TypeError, AttributeError) as _e2_err:
                        logging.debug("[R37-UNIQUE-UPDATE] E2部分平仓持久化失败: %s", _e2_err)
                    remaining_close = 0
            for k in keys_to_remove:
                rec = self._ps.positions[instrument_id].get(k)
                _greeks_snapshot = {}
                if rec is not None:
                    try:
                        from ali2026v3_trading.risk.risk_service import get_risk_service
                        _rs = get_risk_service()
                        if _rs and hasattr(_rs, '_get_greeks_calculator'):
                            _gc = _rs._get_greeks_calculator()
                            if _gc and hasattr(_gc, 'get_greeks'):
                                _greeks = _gc.get_greeks(rec.instrument_id)
                                if _greeks:
                                    _greeks_snapshot = {k: _greeks.get(k, 0.0) for k in ('delta', 'gamma', 'vega', 'theta')}
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] suppressed exception", exc_info=True)
                        pass
                        pass
                del self._ps.positions[instrument_id][k]
                if rec is not None:
                    rec.volume = 0
                    rec._closing = False
                    rec.closing_order_id = ''
                    # FIX-CLOSE-UNIQUE-11: 删除持仓时重置close_method，避免被引用的rec对象
                    # 残留close_method导致后续误判(如rollback恢复时读到旧close_method)
                    if hasattr(rec, 'close_method'):
                        rec.close_method = ''
                    # FIX-CLOSE-UNIQUE-11-ASSERT: 断言持仓删除后状态已完全重置
                    assert not rec._closing, f"删除持仓后_closing必须为False: pos_id={k}"
                    assert not getattr(rec, 'closing_order_id', ''), f"删除持仓后closing_order_id必须为空: pos_id={k}"
                    assert not getattr(rec, 'close_method', ''), f"删除持仓后close_method必须为空: pos_id={k}"
                _close_detail = {'close_price': price}
                if _greeks_snapshot:
                    _close_detail['greeks_snapshot'] = _greeks_snapshot
                self._ps._append_position_state(instrument_id, k, 'CLOSE', _close_detail, signal_id=getattr(rec, 'signal_id', ''))
            _actual_closed = _initial_remaining - remaining_close
            if _actual_closed > 0:
                logging.info(f"[PositionService._reduce_position] Reduced: {instrument_id} {_actual_closed}@ {price}")
            else:
                logging.debug(f"[PositionService._reduce_position] 无实际平仓量: {instrument_id} requested={volume} closeable={_total_closeable}")
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, PositionEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(PositionEvent(instrument_id=instrument_id, position=0.0, avg_price=price, action='CLOSED'), async_mode=True)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

    # FIX-R37-UNIQUE-CLOSE: 统一的持仓回滚入口，确保所有持仓删除/恢复都经过审计日志和状态同步。
    # TradeRollbackManager必须通过此方法删除持仓，不能直接del positions[pid]。
    def _rollback_position(self, instrument_id: str, position_id: str, reason: str = 'rollback') -> bool:
        """回滚(删除)错误创建的持仓。统一入口，确保审计日志和状态同步。"""
        with self._ps._get_instrument_lock(instrument_id):
            _pos_dict = self._ps.positions.get(instrument_id, {})
            rec = _pos_dict.get(position_id)
            if rec is None:
                return False
            # 重置_closing标志，避免回滚后卡住
            rec._closing = False
            rec.closing_order_id = ''
            rec.volume = 0
            # FIX-UPDATE-TIMELY-11: 回滚时重置close_method，避免回滚后被引用的rec对象
            # 残留close_method导致后续误判(如快照恢复时读到旧close_method)
            if hasattr(rec, 'close_method'):
                rec.close_method = ''
            # FIX-UPDATE-TIMELY-11-ASSERT: 断言回滚后状态已完全重置
            assert not rec._closing, f"回滚后_closing必须为False: pos_id={position_id}"
            assert not getattr(rec, 'closing_order_id', ''), \
                f"回滚后closing_order_id必须为空: pos_id={position_id}"
            assert not getattr(rec, 'close_method', ''), \
                f"回滚后close_method必须为空: pos_id={position_id}"
            # 记录审计日志(通过_append_position_state统一入口)
            self._ps._append_position_state(instrument_id, position_id, 'ROLLBACK',
                                            {'reason': reason}, signal_id=getattr(rec, 'signal_id', ''))
            del _pos_dict[position_id]
            if not _pos_dict:
                del self._ps.positions[instrument_id]
            logging.info("[R37-ROLLBACK] 持仓回滚删除: inst=%s pos_id=%s reason=%s", instrument_id, position_id, reason)
            # 发布事件通知
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, PositionEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(PositionEvent(instrument_id=instrument_id, position=0.0,
                                              avg_price=0.0, action='ROLLED_BACK'), async_mode=True)
            except (ValueError, KeyError, TypeError, AttributeError):
                pass
            return True

    def _restore_position_state(self, position_id: str, instrument_id: str, snapshot) -> None:
        """恢复持仓到交易前状态。统一入口，确保审计日志和状态同步。"""
        with self._ps._get_instrument_lock(instrument_id):
            _pos_dict = self._ps.positions.get(instrument_id, {})
            pos = _pos_dict.get(position_id)
            if pos is not None:
                pos.volume = snapshot.prev_volume
                pos.direction = snapshot.prev_direction
                pos.open_price = snapshot.prev_open_price
                pos.stop_profit_price = snapshot.prev_stop_profit_price
                pos.stop_loss_price = snapshot.prev_stop_loss_price
                pos.current_plr = snapshot.prev_current_plr
                pos._max_profit_pct = snapshot.prev_max_profit_pct
                pos.chase_count = snapshot.prev_chase_count
                # FIX-R37-UNIQUE-CLOSE: 恢复时重置_closing，避免回滚后卡住
                pos._closing = False
                pos.closing_order_id = ''
                # 记录审计日志
                self._ps._append_position_state(instrument_id, position_id, 'ROLLBACK_RESTORE',
                                                {'reason': 'trade_rollback_restore',
                                                 'restored_volume': snapshot.prev_volume},
                                                signal_id=getattr(pos, 'signal_id', ''))
                logging.info("[R37-ROLLBACK] 持仓恢复到交易前: inst=%s pos_id=%s vol=%d",
                             instrument_id, position_id, snapshot.prev_volume)
                # FIX-R37-UNIQUE-CLOSE(D2): _restore_position_state 必须发布 PositionEvent，
                # 否则下游缓存(risk_service/strategy_ecosystem等)无法感知持仓恢复，导致脏读
                try:
                    from ali2026v3_trading.infra.event_bus import get_global_event_bus, PositionEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(PositionEvent(instrument_id=instrument_id,
                                                  position=float(snapshot.prev_volume),
                                                  avg_price=snapshot.prev_open_price,
                                                  action='RESTORED'), async_mode=True)
                except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                    pass

    def _trigger_close_position(self, record, reason: str, current_price: float = 0.0) -> None:
        assert record is not None, "_trigger_close_position: record不能为None"
        assert hasattr(record, 'instrument_id'), "_trigger_close_position: record必须有instrument_id属性"
        assert reason, "_trigger_close_position: reason(平仓原因)不能为空"

        # FIX-20260704-STALE-POSITION: 持仓时长异常清理WARNING
        # 根因: LIVE日志显示FG608P990持仓4859分钟(81小时)、FG608P970持仓4904分钟(82小时)
        # 说明前几日EOD平仓未执行，持仓跨日堆积
        # 修复: 持仓时长>24小时(1440分钟)时输出WARNING，提醒运维介入
        try:
            _open_ts = getattr(record, 'open_time', None)
            if _open_ts is not None:
                import time as _stale_time
                _now_ts = _stale_time.time()
                if isinstance(_open_ts, (int, float)):
                    _hold_sec = _now_ts - _open_ts
                    _hold_min = _hold_sec / 60.0
                    if _hold_min > 1440.0:  # >24小时
                        logging.warning("[STALE-POSITION] 持仓时长异常: inst=%s pos_id=%s hold=%.0f分钟(%.1f小时) reason=%s，疑似EOD平仓失效，请运维介入",
                                        record.instrument_id, getattr(record, 'position_id', ''), _hold_min, _hold_min/60.0, reason)
        except (ValueError, KeyError, TypeError, AttributeError) as _stale_err:
            logging.debug("[STALE-POSITION] 持仓时长检查跳过(非致命): %s", _stale_err)

        # FIX-20260704-MARKET-CLOSED: 收盘后不尝试平仓下单，避免result=-1+重试耗尽+CANNOT_CLOSE
        # 根因: 23:36 CZCE夜盘23:30结束，策略仍尝试平仓→平台拒绝result=-1→重试耗尽→CANNOT_CLOSE
        # 修复: 收盘后直接延后重试，不浪费下单调用，不污染日志
        # FIX-20260709-PRE-MARKET: 区分"尚未开盘"和"已收盘"，尚未开盘时等待短时间后重试
        # FIX-20260704-EXCH-INFER: record.exchange可能为空(从JSONL恢复时缺失)，从instrument_id推断交易所
        try:
            from ali2026v3_trading.infra.market_time_service import get_market_status
            _exch = getattr(record, 'exchange', '') or ''
            if not _exch:
                # FIX-20260704-EXCH-INFER: 从instrument_id推断交易所作为fallback
                _inst_id = getattr(record, 'instrument_id', '') or ''
                if _inst_id:
                    try:
                        from ali2026v3_trading.config.config_service import resolve_product_exchange
                        _exch = resolve_product_exchange(_inst_id) or ''
                    except (ImportError, AttributeError, Exception):
                        pass
            if _exch:
                _mkt_status = get_market_status(_exch)
                if _mkt_status == 'PRE_MARKET':
                    # 尚未开盘：等待短时间后重试（而非延后到下一轮重试周期）
                    logging.info("[_trigger_close_position] 交易所尚未开盘，等待后重试平仓: inst=%s exchange=%s reason=%s",
                                 record.instrument_id, _exch, reason)
                    self._schedule_close_retry(record, current_price)
                    return
                elif _mkt_status == 'CLOSED':
                    logging.info("[_trigger_close_position] 交易所已收盘，延后平仓: inst=%s exchange=%s reason=%s",
                                 record.instrument_id, _exch, reason)
                    self._schedule_close_retry(record, current_price)
                    return
        except (ImportError, AttributeError, Exception) as _mc_err:
            logging.debug("[_trigger_close_position] 交易所状态检查跳过(非致命): %s", _mc_err)

        with self._ps._get_instrument_lock(record.instrument_id):
            _existing_closing_oid = getattr(record, 'closing_order_id', '')
            # FIX-CLOSE-UNIQUE-07: PENDING_xxx检查不充分 - 需区分PENDING/RETRY/实际order_id三种状态
            # PENDING_xxx: 平仓已触发但订单尚未发送(本函数内)，应跳过
            # RETRY_xxx: 重试中，应跳过
            # 实际order_id: 订单已发送，应跳过
            if _existing_closing_oid:
                _pos_id = getattr(record, 'position_id', '')
                if _existing_closing_oid.startswith('PENDING_'):
                    logging.debug("[CLOSE-UNIQUE-07] 跳过平仓: PENDING状态(订单未发送) inst=%s pos_id=%s reason=%s",
                                  record.instrument_id, _pos_id, reason)
                elif _existing_closing_oid.startswith('RETRY_'):
                    logging.debug("[CLOSE-UNIQUE-07] 跳过平仓: RETRY状态(重试中) inst=%s pos_id=%s reason=%s",
                                  record.instrument_id, _pos_id, reason)
                else:
                    logging.debug("[CLOSE-UNIQUE-07] 跳过平仓: 已有平仓订单 oid=%s inst=%s reason=%s",
                                  _existing_closing_oid, record.instrument_id, reason)
                return
            if abs(getattr(record, 'volume', 0)) == 0:
                logging.debug("[R38-POS-ZERO] 持仓量为0，跳过平仓: inst=%s reason=%s",
                              record.instrument_id, reason)
                return
            record.closing_order_id = f"PENDING_{record.position_id}"
            record._closing = True
            record.close_method = reason
            assert reason, f"close_method(平仓方式)不能为空: pos_id={record.position_id}"
            # FIX-CLOSE-UNIQUE-07: 断言closing_order_id已正确设置为PENDING状态
            assert record.closing_order_id == f"PENDING_{record.position_id}", \
                f"closing_order_id未正确设置PENDING状态: got={record.closing_order_id} pos_id={record.position_id}"
            need_retry = False
            try:
                from ali2026v3_trading.order.order_service import get_order_service
                order_svc = get_order_service()
                direction = 'SELL' if record.volume > 0 else 'BUY'
                price = 0.0
                _used_open_price_fallback = False
                try:
                    from ali2026v3_trading.data.data_service import get_data_service
                    ds = get_data_service()
                    if ds and ds.realtime_cache:
                        tick = ds.realtime_cache._latest_ticks.get(record.instrument_id)
                        if tick:
                            price = tick.get('bid_price1' if direction == 'SELL' else 'ask_price1', 0.0)
                            if price <= 0:
                                price = tick.get('last_price', 0.0)
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
                if price <= 0:
                    base = current_price or 0.0
                    if base <= 0:
                        base = getattr(record, 'current_price', 0.0) or 0.0
                    if base <= 0:
                        try:
                            from ali2026v3_trading.data.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                _tick2 = ds.realtime_cache._latest_ticks.get(record.instrument_id)
                                if _tick2:
                                    base = _tick2.get('last_price', 0.0) or 0.0
                        except (ValueError, KeyError, TypeError, AttributeError):
                            pass
                    if base <= 0:
                        try:
                            from ali2026v3_trading.data.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                base = ds.realtime_cache.get_latest_price(record.instrument_id) or 0.0
                        except (ValueError, KeyError, TypeError, AttributeError):
                            pass
                    # FIX-R26: realtime_cache被定期清理导致平仓无价格，增加WidthStrengthCache._option_price作为fallback
                    if base <= 0:
                        try:
                            from ali2026v3_trading.data.width_cache import get_width_cache
                            from ali2026v3_trading.config.params_service import get_params_service
                            _wc = get_width_cache()
                            _ps = get_params_service()
                            if _wc and _ps:
                                _iid = _ps.get_internal_id(record.instrument_id)
                                if _iid is not None:
                                    _opt_price = getattr(_wc, '_option_price', {})
                                    base = float(_opt_price.get(_iid, 0.0) or 0.0)
                                    if base > 0:
                                        logging.info("[R26-CLOSE] 从WidthStrengthCache获取期权价格: %s iid=%d price=%.4f", record.instrument_id, _iid, base)
                        except (ValueError, KeyError, TypeError, AttributeError) as _r26_err:
                            logging.debug("[R26-CLOSE] WidthStrengthCache fallback failed: %s", _r26_err)
                    # FIX-R26b: 最后fallback使用开仓价格（优于无法平仓导致持仓泄漏）

                    if base <= 0:
                        _open_price = getattr(record, 'open_price', 0.0) or 0.0
                        if _open_price > 0:
                            base = float(_open_price)
                            _used_open_price_fallback = True
                            # FIX-20260704-R26B-CLOSE-NOISE: 降级为DEBUG避免188次INFO洪泛
                            logging.debug("[R26b-CLOSE] 使用开仓价格作为fallback: %s open_price=%.4f", record.instrument_id, base)
                    if base > 0:
                        try:
                            from ali2026v3_trading.config.params_service import get_params_service
                            tick_size = get_params_service().get_float('tick_size', 1.0)
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            tick_size = 1.0
                        price = base - tick_size if direction == 'SELL' else base + tick_size
                        # FIX-20260704-CLOSE-NEG-PRICE: tick_size过大(如期权base=0.5/tick_size=1.0)导致price<=0
                        # 根因: 全局tick_size=1.0对低权力期权不适用，0.5-1.0=-0.5触发price<=0跳过下单+死循环重试
                        # 修复: price<=0时退化为百分比偏移(1%)，确保price>0
                        if price <= 0:
                            price = base * 0.99 if direction == 'SELL' else base * 1.01
                            logging.info("[R26c-CLOSE] tick_size=%.4f导致price<=0，退化为百分比偏移: %s base=%.4f price=%.4f",
                                         tick_size, record.instrument_id, base, price)
                    else:
                        logging.warning("[PositionService._trigger_close_position] 无法获取有效价格，将重试平仓: %s", record.instrument_id)
                        need_retry = True
                # FIX-R26c: 无有效价格时跳过下单，避免price=0被平台拒绝导致无限重试
                if need_retry or price <= 0:
                    if price <= 0:
                        logging.warning("[R26c-CLOSE] price<=0跳过下单, 安排重试: %s price=%.4f", record.instrument_id, price)
                    self._schedule_close_retry(record, max(price, 0.0))
                    return
                # FIX-R28: close_signal_id包含position_id，确保同instrument多持仓的平仓key唯一
                _close_signal_id = getattr(record, 'signal_id', '') or f"CLOSE_{record.instrument_id}_{getattr(record, 'position_id', id(record))}"
                _close_ref_price = base if _used_open_price_fallback else 0.0

                if self._ps.network_retry_manager is not None:
                    def _send_order_wrapper():
                        return order_svc.send_order(instrument_id=record.instrument_id, volume=abs(record.volume),
                                                    price=price, direction=direction, action='CLOSE',
                                                    exchange=record.exchange or '', signal_id=_close_signal_id,
                                                    ref_price=_close_ref_price)
                    # FIX-CLOSE-UNIQUE-08: operation_id包含signal_id后缀，确保同一持仓不同信号
                    # 的平仓操作在网络重试管理器中拥有独立的operation_id，避免重试状态被错误共享
                    _operation_id = f"close_{record.instrument_id}_{record.position_id}"
                    if _close_signal_id:
                        _operation_id += f"_{_close_signal_id}"
                    # FIX-CLOSE-UNIQUE-08-ASSERT: 断言operation_id包含position_id确保唯一性
                    assert record.position_id in _operation_id, \
                        f"operation_id必须包含position_id: operation_id={_operation_id} pos_id={record.position_id}"
                    order_id = self._ps.network_retry_manager.execute_with_retry(
                        operation_id=_operation_id, func=_send_order_wrapper)
                else:
                    order_id = order_svc.send_order(instrument_id=record.instrument_id, volume=abs(record.volume),
                                                    price=price, direction=direction, action='CLOSE',
                                                    exchange=record.exchange or '', signal_id=_close_signal_id,
                                                    ref_price=_close_ref_price)
                if order_id:
                    _actual_oid = getattr(order_id, 'order_id', '') if hasattr(order_id, 'order_id') else str(order_id)
                    record.closing_order_id = _actual_oid
                    record._closing = True
                    record.close_reason = reason
                    # FIX-DIR-DUAL-TRACK: 兼容record.direction的'long'/'short'和防御性兼容'BUY'/'0'
                    _pnl_mult = 1.0 if record.direction in ('long', 'BUY', '0') else -1.0
                    record.realized_pnl = _pnl_mult * (price - record.open_price) * abs(record.volume)
                    logging.info("[PositionService._trigger_close_position] %s for %s vol=%d price=%.2f", reason, record.instrument_id, abs(record.volume), price)
                    try:
                        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import SnapshotTrigger
                        _sc = getattr(self._ps, '_snapshot_collector', None)
                        if _sc is not None and hasattr(_sc, 'capture_order_event'):
                            _sc.capture_order_event(
                                timestamp=None,
                                event_type=SnapshotTrigger.ORDER_CLOSED,
                                trigger_detail=f"CLOSE {record.instrument_id} {reason} sg={getattr(record, 'strategy_group', '')}",
                                order_info={'position_id': record.position_id, 'instrument_id': record.instrument_id,
                                            'direction': direction, 'price': price, 'volume': abs(record.volume),
                                            'open_reason': getattr(record, 'open_reason', ''), 'close_reason': reason,
                                            'signal_id': _close_signal_id},
                            )
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _snap_err:
                        logging.debug("[PositionService] 平仓快照采集跳过: %s", _snap_err)
                else:
                    # FIX-R31-CLOSE-LOG: 记录详细的失败原因，便于排查
                    _err_code = ''
                    _err_msg = ''
                    if hasattr(order_id, 'error_code'):
                        _err_code = order_id.error_code or ''
                        _err_msg = order_id.error_message or ''
                    # FIX-20260704-GHOST-POSITION: 平仓被平台拒绝时，检查该持仓是否为幽灵持仓
                    # 根因: 从position_state.jsonl恢复的历史持仓在交易平台上可能已不存在，
                    # 每次重启都触发TimeStop/StopProfit→平台拒绝result=-1→重试耗尽→CANNOT_CLOSE
                    # 下次重启CANNOT_CLOSE标记丢失，又重复同样流程，产生321条/轮ERROR+WARNING洪泛
                    # 修复: 平台拒绝时用_platform_get_position验证持仓是否存在，不存在则直接删除
                    _is_ghost = False
                    if 'platform_rejected' in str(_err_code) or 'result=-1' in str(_err_msg):
                        _platform_get_pos = getattr(self._ps, '_platform_get_position', None)
                        if callable(_platform_get_pos):
                            try:
                                _platform_pos = _platform_get_position(record.instrument_id)
                                if _platform_pos is None or (hasattr(_platform_pos, 'volume') and getattr(_platform_pos, 'volume', 0) == 0):
                                    _is_ghost = True
                                    logging.warning("[GHOST-POSITION] 平台确认持仓不存在，删除幽灵持仓: inst=%s pos_id=%s reason=%s",
                                                    record.instrument_id, getattr(record, 'position_id', ''), reason)
                                    self._rollback_position(record.instrument_id, getattr(record, 'position_id', ''), reason=f'ghost_position:{reason}')
                                    return
                            except (ValueError, KeyError, TypeError, AttributeError) as _gpe:
                                logging.debug("[GHOST-POSITION] 平台持仓查询失败，无法确认: %s", _gpe)
                    # FIX-20260704-CLOSE-FAIL-NOISE: 全局速率限制，60s内最多20条WARNING，超出降级DEBUG避免233次洪泛
                    _cf_now = time.time()
                    _cf_window = getattr(self, '_close_fail_warn_window', 0.0)
                    if _cf_now - _cf_window > 60.0:
                        self._close_fail_warn_count = 0
                        self._close_fail_warn_window = _cf_now
                    _cf_count = getattr(self, '_close_fail_warn_count', 0)
                    if _cf_count < 20:
                        logging.warning("[PositionService._trigger_close_position] 平仓下单失败: %s, 将重试, error_code=%s, error_message=%s",
                                        record.instrument_id, _err_code, _err_msg)
                        self._close_fail_warn_count = _cf_count + 1
                    else:
                        logging.debug("[PositionService._trigger_close_position] 平仓下单失败(速率限制): %s, error_code=%s",
                                      record.instrument_id, _err_code)
                    # FIX-R31-CLOSING-PREVENT: 平仓失败时也设置_closing=True，防止在重试期间
                    # 时间止损/止盈/到期等路径再次触发_trigger_close_position导致重复下单。
                    # 重试逻辑(_schedule_close_retry)会在所有重试失败后重置_closing=False。
                    record._closing = True
                    try:
                        from ali2026v3_trading.order.order_service import get_order_service as _get_os
                        _osvc = _get_os()
                        if _osvc and hasattr(_osvc, '_order_idempotent_set'):
                            _sig_suffix = f"_{_close_signal_id}" if _close_signal_id else ""
                            _ik = f"{record.instrument_id}_{direction}_CLOSE_{abs(record.volume)}_{round(price, 4)}{_sig_suffix}"
                            if _ik in _osvc._order_idempotent_set:
                                _osvc._order_idempotent_set.discard(_ik)
                                logging.info("[R28-CLOSE-CLEANUP] 平仓失败后清理幂等key: %s", _ik)
                    except (ValueError, KeyError, TypeError, AttributeError) as _ik_err:
                        logging.debug("[R28-CLOSE-CLEANUP] 清理幂等key异常: %s", _ik_err)
                    need_retry = True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error("[PositionService._trigger_close_position] Error: %s", e)
                # FIX-R31-EXCEPT-CLOSING: 异常时也设置_closing=True并安排重试，避免持仓卡住
                record._closing = True
                need_retry = True
        if need_retry:
            self._schedule_close_retry(record, price)

    def _cleanup_close_retry_executor(self):
        if self._close_retry_executor is not None:
            self._close_retry_executor.shutdown(wait=False)
            self._close_retry_executor = None

    def _schedule_close_retry(self, record, price: float) -> None:
        from concurrent.futures import ThreadPoolExecutor
        if self._close_retry_executor is None:
            self._close_retry_executor = ThreadPoolExecutor(
                max_workers=self.CLOSE_RETRY_MAX_THREADS, thread_name_prefix='pos_retry')
            import atexit as _atexit
            _atexit.register(self._cleanup_close_retry_executor)
            # P2-14修复: 注册到lifecycle_resource统一管理
            try:
                from ali2026v3_trading.lifecycle.lifecycle_resource import register_thread_pool
                register_thread_pool('position_close_retry', self._close_retry_executor)
            except (ImportError, AttributeError) as _err:
                logging.debug("[position_command_service] 属性访问降级: %s", _err)
            try:
                _dummy_future = self._close_retry_executor.submit(lambda: None)
                _dummy_future.result(timeout=2.0)
                for _t in getattr(self._close_retry_executor, '_threads', set()):
                    _t.daemon = True
            except (ValueError, KeyError, TypeError, AttributeError, RuntimeError) as _r3_err:
                # FIX-R28: RuntimeError: cannot set daemon status of active thread — ThreadPoolExecutor线程已启动
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass

        def _retry_worker():
            from ali2026v3_trading.infra.resilience import BoundedRetry as _BoundedRetry
            from ali2026v3_trading.order.order_service import get_order_service

            def _attempt_close():
                # FIX-20260704-RETRY-MARKET-CLOSED: 收盘后不重试平仓，返回'skip'避免消耗重试次数
                # 根因: 收盘后所有平仓返回result=-1，重试3次全部失败标记CANNOT_CLOSE
                # 修复: 检查交易所状态，收盘时返回'skip'保留重试次数，待开盘后自动重试
                # FIX-20260709-PRE-MARKET: 区分"尚未开盘"和"已收盘"，尚未开盘时等待后重试
                # FIX-20260704-EXCH-INFER: record.exchange为空时从instrument_id推断交易所
                try:
                    from ali2026v3_trading.infra.market_time_service import get_market_status
                    _retry_exch = getattr(record, 'exchange', '') or ''
                    if not _retry_exch:
                        _retry_inst = getattr(record, 'instrument_id', '') or ''
                        if _retry_inst:
                            try:
                                from ali2026v3_trading.config.config_service import resolve_product_exchange
                                _retry_exch = resolve_product_exchange(_retry_inst) or ''
                            except (ImportError, AttributeError, Exception):
                                pass
                    if _retry_exch:
                        _retry_status = get_market_status(_retry_exch)
                        if _retry_status == 'PRE_MARKET':
                            logging.info("[RETRY-PRE-MARKET] 交易所尚未开盘，等待开盘后继续: %s exchange=%s", record.instrument_id, _retry_exch)
                            # 尚未开盘时等待最多60秒，开盘后继续执行平仓（不返回skip，不消耗重试次数）
                            for _wait in range(12):
                                time.sleep(5.0)
                                _check_status = get_market_status(_retry_exch)
                                if _check_status == 'OPEN':
                                    logging.info("[RETRY-PRE-MARKET] 交易所已开盘，继续平仓: %s exchange=%s", record.instrument_id, _retry_exch)
                                    break
                                if _check_status == 'CLOSED':
                                    logging.info("[RETRY-MARKET-CLOSED] 等待期间交易所已收盘: %s exchange=%s", record.instrument_id, _retry_exch)
                                    return 'skip'
                            else:
                                # 等待60秒后仍未开盘，返回skip保留重试次数
                                logging.info("[RETRY-PRE-MARKET] 等待60秒后仍未开盘，保留重试: %s exchange=%s", record.instrument_id, _retry_exch)
                                return 'skip'
                            # 已开盘，继续执行后续平仓逻辑
                        elif _retry_status == 'CLOSED':
                            logging.info("[RETRY-MARKET-CLOSED] 交易所已收盘，跳过本次重试(保留次数): %s exchange=%s", record.instrument_id, _retry_exch)
                            return 'skip'
                except (ImportError, AttributeError, Exception) as _rmc_err:
                    logging.debug("[RETRY-MARKET-CLOSED] 交易所状态检查跳过(非致命): %s", _rmc_err)
                with self._ps._get_instrument_lock(record.instrument_id):
                    _pos_dict = self._ps.positions.get(record.instrument_id, {})
                    _still_exists = any(getattr(_r, 'position_id', None) == record.position_id for _r in _pos_dict.values())
                    if not _still_exists or record.volume == 0:
                        logging.info("[PositionService] retry skipped, position already closed or removed: %s", record.instrument_id)
                        return 'skip'
                    if not getattr(record, '_closing', False):
                        record._closing = True
                        if not getattr(record, 'closing_order_id', ''):
                            record.closing_order_id = f"RETRY_{record.position_id}"
                        logging.info("[R37-RETRY-RACE] 重试前重新设置_closing=True: inst=%s pos_id=%s",
                                     record.instrument_id, getattr(record, 'position_id', ''))
                # FIX-CLOSE-UNIQUE-09: 重试worker读取record属性必须在instrument_lock内，
                # 避免并发场景下主线程修改record属性导致读取到不一致状态
                # 以下属性读取需在锁内完成，先捕获快照再使用
                with self._ps._get_instrument_lock(record.instrument_id):
                    _retry_volume = abs(getattr(record, 'volume', 0))
                    _retry_exchange = getattr(record, 'exchange', '') or ''
                    _retry_pos_id = getattr(record, 'position_id', '')
                    _retry_signal_id = getattr(record, 'signal_id', '') or ''
                    _retry_current_price = getattr(record, 'current_price', 0.0) or 0.0
                    _retry_open_price = getattr(record, 'open_price', 0.0) or 0.0
                    _retry_direction = 'SELL' if getattr(record, 'volume', 0) > 0 else 'BUY'
                # FIX-R26d: 重试时重新获取价格，避免使用过期的price=0
                _retry_price = price
                _retry_used_open_price = False
                if _retry_price <= 0:
                    _retry_price = _retry_current_price
                if _retry_price <= 0:
                    _retry_price = _retry_open_price
                    _retry_used_open_price = True
                if _retry_price <= 0:
                    # FIX-R26: 从WidthStrengthCache获取期权价格
                    try:
                        from ali2026v3_trading.data.width_cache import get_width_cache
                        from ali2026v3_trading.config.params_service import get_params_service
                        _wc = get_width_cache()
                        _ps_r26 = get_params_service()
                        if _wc and _ps_r26:
                            _iid = _ps_r26.get_internal_id(record.instrument_id)
                            if _iid is not None:
                                _retry_price = float(getattr(_wc, '_option_price', {}).get(_iid, 0.0) or 0.0)
                    except (ValueError, KeyError, TypeError, AttributeError):
                        pass
                if _retry_price <= 0:
                    logging.warning("[R26d-RETRY] 仍无法获取价格，跳过本次重试: %s", record.instrument_id)
                    return None
                order_svc = get_order_service()
                _retry_ref_price = _retry_price if _retry_used_open_price else 0.0

                order_id = order_svc.send_order(
                    instrument_id=record.instrument_id, volume=_retry_volume, price=_retry_price,
                    direction=_retry_direction, action='CLOSE', exchange=_retry_exchange,
                    signal_id=_retry_signal_id or f"RETRY_CLOSE_{record.instrument_id}_{_retry_pos_id}",
                    ref_price=_retry_ref_price)
                if order_id:
                    _actual_oid = getattr(order_id, 'order_id', '') if hasattr(order_id, 'order_id') else str(order_id)
                    with self._ps._get_instrument_lock(record.instrument_id):
                        record._closing = True
                        record.closing_order_id = _actual_oid
                    logging.info("[PositionService] retry succeeded: %s", record.instrument_id)
                    return order_id
                return None

            # FIX-UPDATE-TIMELY-10: _BoundedRetry重试耗尽时从warn升级为error，并触发告警
            br = _BoundedRetry(
                max_retries=self.CLOSE_RETRY_MAX_ATTEMPTS,
                base_delay=self.CLOSE_RETRY_BASE_DELAY_SEC,
                max_delay=self.CLOSE_RETRY_BASE_DELAY_SEC * (2 ** self.CLOSE_RETRY_MAX_ATTEMPTS),
                on_exhausted="error",
            )
            result = br.execute(_attempt_close)
            if result is None:
                with self._ps._get_instrument_lock(record.instrument_id):
                    _exhausted_method = getattr(record, 'close_method', '')
                # FIX-20260704-GHOST-CLOSE: 重试耗尽的持仓直接删除(幽灵持仓)
                # 根因: CANNOT_CLOSE标记不会被持久化到position_state.jsonl，
                # 重启后closing_order_id被重置为空，同一批幽灵持仓再次触发平仓洪泛
                # 修复: 重试耗尽=平台无法平仓=幽灵持仓，直接_rollback_position删除并持久化ROLLBACK
                logging.error("[UPDATE-TIMELY-10] 平仓重试全部耗尽(inst=%s pos_id=%s close_method=%s)，"
                              "判定为幽灵持仓，直接删除",
                              record.instrument_id, getattr(record, 'position_id', ''), _exhausted_method)
                self._rollback_position(record.instrument_id, getattr(record, 'position_id', ''),
                                        reason=f'cannot_close:{_exhausted_method}')

        self._close_retry_executor.submit(_retry_worker)
