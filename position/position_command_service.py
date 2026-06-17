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
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any, Tuple

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False
    np = None
if _HAS_NUMPY and np is None:
    _HAS_NUMPY = False

from ali2026v3_trading.infra.performance_monitor import count_call
from ali2026v3_trading.infra.shared_utils import api_version  # P1-21: 统一从shared_utils导入


class PositionCommandService:
    """命令处理服务 — 从PositionService拆分"""

    CLOSE_RETRY_MAX_ATTEMPTS = 3
    CLOSE_RETRY_BASE_DELAY_SEC = 0.1
    CLOSE_RETRY_MAX_THREADS = 10
    _MAX_CLOSE_RETRY_THREADS = CLOSE_RETRY_MAX_THREADS

    def __init__(self, position_service: Any):
        self._ps = position_service
        self._close_retry_executor = None

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

    def on_trade(self, trade: Any) -> None:
        from ali2026v3_trading.infra.shared_utils import require_precondition
        require_precondition(trade is not None, "on_trade: trade不能为None")
        try:
            inst_id = self._ps._get_platform_attr(trade, "instrument_id", "InstrumentID", default="")
            exch = self._ps._get_platform_attr(trade, "exchange", "ExchangeID", default="")
            d_raw = self._ps._get_platform_attr(trade, "direction", "Direction", default="")
            o_raw = self._ps._get_platform_attr(trade, "offset_flag", "OffsetFlag", default="")
            price = self._ps._get_platform_attr(trade, "price", "Price", default=0)
            volume = self._ps._get_platform_attr(trade, "volume", "Volume", default=0)
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
            order_id = self._ps._get_platform_attr(trade, "order_id", "OrderID", default="")

            if self._ps.partial_fill_handler is not None:
                try:
                    filled_volume = self._ps._get_platform_attr(trade, "filled_volume", "FilledVolume", default=volume)
                    total_volume = self._ps._get_platform_attr(trade, "total_volume", "TotalVolume", default=volume)
                    if order_id and total_volume > 0:
                        self._ps.partial_fill_handler.record_partial_fill(
                            order_id=order_id, instrument_id=inst_id,
                            filled_volume=filled_volume, total_volume=total_volume,
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
                self._add_position(exch, inst_id, vol_signed, price, open_reason=open_reason, order_id=order_id)
            else:
                self._reduce_position(exch, inst_id, volume, is_buy, price)
                if self._ps.self_trade_detector is not None and order_id:
                    self._ps.self_trade_detector.remove_order(order_id)
            logging.debug(f"[PositionService.on_trade] Updated: {inst_id} vol={volume}")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService.on_trade] Error: {e}")

    def on_tick(self, tick: Any) -> None:
        try:
            price = self._ps._get_platform_attr(tick, "last_price", "LastPrice", "price", "last", default=0)
            from ali2026v3_trading.infra.shared_utils import safe_price_check
            if not safe_price_check(price):
                return
            inst_id = self._ps._get_platform_attr(tick, "instrument_id", "InstrumentID", default="")
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
                                record.profit_slope = slope

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
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            if rs:
                existing_margin = 0.0
                try:
                    for _inst_id, pos_dict in self._ps.positions.items():
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
                        logging.error(f"[PositionService._add_position] 自成交检测阻断: {alert_msg}")
                        return
                    self._ps.self_trade_detector.add_order(new_order)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning(f"[PositionService._add_position] 自成交检测异常: {e}")

        with self._ps._get_instrument_lock(instrument_id):
            if instrument_id not in self._ps.positions:
                self._ps.positions[instrument_id] = {}
            pos_id = f"{instrument_id}_{int(datetime.now(_CHINA_TZ).timestamp()*1000)}_{generate_prefixed_id('', 6)}"  # R9-3
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
            # [R28-C009修复] NaN/Inf检查: P2-20修复提取为_validate_finite辅助
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

            # 计算止盈止损价格（所有参数已通过有效性校验）
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
                open_reason=open_reason, order_id=order_id,
                option_premium=self._ps._compute_option_premium(instrument_id, price),
            )
            self._ps.positions[instrument_id][pos_id] = record
            logging.info(f"[PositionService._add_position] Added: {instrument_id} {volume}手@ {price} reason={open_reason}")
            self._ps._append_position_state(instrument_id, pos_id, 'OPEN', {'volume': volume, 'price': price, 'open_reason': open_reason}, signal_id=signal_id)
            if self._ps._structured_logger is not None:
                try:
                    self._ps._structured_logger.log_order({
                        "order_id": pos_id, "instrument_id": instrument_id,
                        "direction": "buy" if volume > 0 else "sell", "price": price,
                        "volume": abs(volume), "order_type": "OPEN", "status": "filled",
                        "filled_volume": abs(volume), "remaining_volume": 0,
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
        with self._ps._get_instrument_lock(instrument_id):
            if instrument_id not in self._ps.positions:
                return
            records = sorted(self._ps.positions[instrument_id].values(), key=lambda x: x.open_time)
            remaining_close = volume
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
                else:
                    if rec.volume > 0:
                        rec.volume -= remaining_close
                    else:
                        rec.volume += remaining_close
                    if hasattr(rec, 'stop_profit_price') and rec.stop_profit_price and rec.stop_profit_price > 0:
                        _orig_tp = rec.stop_profit_price
                        _tp_ratio = abs(_orig_tp / rec.open_price - 1) if rec.open_price > 0 else 0
                        rec.stop_profit_price = rec.open_price * (1 + _tp_ratio) if rec.open_price > 0 else _orig_tp
                    if hasattr(rec, 'stop_loss_price') and rec.stop_loss_price and rec.stop_loss_price > 0:
                        _orig_sl = rec.stop_loss_price
                        _sl_ratio = abs(rec.open_price / _orig_sl - 1) if _orig_sl > 0 and rec.open_price > 0 else 0
                        rec.stop_loss_price = rec.open_price * (1 - _sl_ratio) if rec.open_price > 0 else _orig_sl
                    logging.info("[PositionService] R14-P1-BIZ-08: 部分平仓后重算止盈止损 instrument=%s vol=%d", rec.instrument_id, rec.volume)
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
                _close_detail = {'close_price': price}
                if _greeks_snapshot:
                    _close_detail['greeks_snapshot'] = _greeks_snapshot
                self._ps._append_position_state(instrument_id, k, 'CLOSE', _close_detail, signal_id=getattr(rec, 'signal_id', ''))
            logging.info(f"[PositionService._reduce_position] Reduced: {instrument_id} {volume}@ {price}")
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, PositionEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(PositionEvent(instrument_id=instrument_id, position=0.0, avg_price=price, action='CLOSED'), async_mode=True)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

    def _trigger_close_position(self, record, reason: str, current_price: float = 0.0) -> None:
        with self._ps._get_instrument_lock(record.instrument_id):
            if record._closing:
                return
            need_retry = False
            try:
                from ali2026v3_trading.order.order_service import get_order_service
                order_svc = get_order_service()
                direction = 'SELL' if record.volume > 0 else 'BUY'
                price = 0.0
                try:
                    from ali2026v3_trading.data.data_service import get_data_service
                    ds = get_data_service()
                    if ds and ds.realtime_cache:
                        tick = ds.realtime_cache._latest_ticks.get(record.instrument_id)
                        if tick:
                            price = tick.get('bid_price' if direction == 'SELL' else 'ask_price', 0.0)
                            if price <= 0:
                                price = tick.get('price', 0.0)
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
                if price <= 0:
                    base = current_price or 0.0
                    if base <= 0:
                        try:
                            from ali2026v3_trading.data.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                base = ds.realtime_cache.get_latest_price(record.instrument_id) or 0.0
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[R3-L2] suppressed exception", exc_info=True)
                            pass
                            pass
                    if base > 0:
                        try:
                            from ali2026v3_trading.config.params_service import get_params_service
                            tick_size = get_params_service().get_float('tick_size', 1.0)
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            tick_size = 1.0
                        price = base - tick_size if direction == 'SELL' else base + tick_size
                    else:
                        logging.warning("[PositionService._trigger_close_position] 无法获取有效价格，跳过平仓: %s", record.instrument_id)
                        return
                _close_signal_id = getattr(record, 'signal_id', '') or f"CLOSE_{record.instrument_id}"
                if self._ps.network_retry_manager is not None:
                    def _send_order_wrapper():
                        return order_svc.send_order(instrument_id=record.instrument_id, volume=abs(record.volume),
                                                    price=price, direction=direction, action='CLOSE',
                                                    exchange=record.exchange or '', signal_id=_close_signal_id)
                    order_id = self._ps.network_retry_manager.execute_with_retry(
                        operation_id=f"close_{record.instrument_id}_{record.position_id}", func=_send_order_wrapper)
                else:
                    order_id = order_svc.send_order(instrument_id=record.instrument_id, volume=abs(record.volume),
                                                    price=price, direction=direction, action='CLOSE',
                                                    exchange=record.exchange or '', signal_id=_close_signal_id)
                if order_id:
                    record._closing = True
                    logging.info("[PositionService._trigger_close_position] %s for %s vol=%d price=%.2f", reason, record.instrument_id, abs(record.volume), price)
                else:
                    logging.warning("[PositionService._trigger_close_position] 平仓下单失败: %s, 将重试", record.instrument_id)
                    need_retry = True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error("[PositionService._trigger_close_position] Error: %s", e)
                return
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
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

        def _retry_worker():
            from ali2026v3_trading.infra.resilience import BoundedRetry as _BoundedRetry
            from ali2026v3_trading.order.order_service import get_order_service

            def _attempt_close():
                order_svc = get_order_service()
                direction = 'SELL' if record.volume > 0 else 'BUY'
                order_id = order_svc.send_order(
                    instrument_id=record.instrument_id, volume=abs(record.volume), price=price,
                    direction=direction, action='CLOSE', exchange=record.exchange or '',
                    signal_id=getattr(record, 'signal_id', '') or f"RETRY_CLOSE_{record.instrument_id}")
                if order_id:
                    with self._ps._get_instrument_lock(record.instrument_id):
                        record._closing = True
                    logging.info("[PositionService] retry succeeded: %s", record.instrument_id)
                    return order_id
                return None

            br = _BoundedRetry(
                max_retries=self.CLOSE_RETRY_MAX_ATTEMPTS,
                base_delay=self.CLOSE_RETRY_BASE_DELAY_SEC,
                max_delay=self.CLOSE_RETRY_BASE_DELAY_SEC * (2 ** self.CLOSE_RETRY_MAX_ATTEMPTS),
                on_exhausted="warn",
            )
            result = br.execute(_attempt_close)
            if result is None:
                with self._ps._get_instrument_lock(record.instrument_id):
                    record._closing = False
                logging.error("[PositionService] all retries failed, reset _closing: %s", record.instrument_id)

        self._close_retry_executor.submit(_retry_worker)
