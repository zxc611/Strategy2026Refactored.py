"""
回测持仓管理器 — 从backtest_runner_base.py拆分
职责: 开仓5阶段分解、持仓检查(止盈止损/时间止损/EOD/状态反转)
"""
from __future__ import annotations

import logging
from typing import Dict, Optional, Any, Tuple

import numpy as np
import pandas as pd


def try_open_check_gates(bt, bar, params, bar_time, strategy_type,
                         _is_consecutive_loss_paused, _STATE_REASON_MAP,
                         BACKTEST_THRESHOLDS, safe_price_check):
    if hasattr(bt, 'new_open_blocked') and bt.new_open_blocked:
        logging.info("[P0-R9-02] 开仓被质量检查阻断，跳过")
        return None
    if pd.isna(bar_time):
        logging.warning("[R22-TIME-03] bar缺少minute字段，跳过开仓判断")
        return None
    cooldown = params.get("signal_cooldown_sec", 60.0)
    if bt.last_signal_time is not None and cooldown > 0:
        elapsed = (bar_time - bt.last_signal_time).total_seconds()
        if elapsed < cooldown:
            return None
    if _is_consecutive_loss_paused(bt, params, bar_time):
        return None
    max_signals = int(params.get("max_signals_per_window", 5))
    if bt.total_signals >= max_signals:
        return None
    reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
    symbol = bar.get("symbol", "unknown")
    price = bar.get("close", 0.0)
    if not safe_price_check(price):
        return None
    strength = bar.get("strength", 0.0)
    if strategy_type == "main" and reason == "CORRECT_RESONANCE" and strength < BACKTEST_THRESHOLDS["correct_resonance_min_strength"]:
        return None
    return reason, symbol, price


def try_open_compute_lots(bt, params, price, sl_ratio, reason, bar,
                          _compute_lots_with_risk_budget):
    lots = _compute_lots_with_risk_budget(
        bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
        recent_pnls=bt.recent_pnls, current_positions=bt.positions)
    if lots <= 0:
        return 0
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs_p22 = get_risk_service(scope_id='backtest')
        _tvf_lots = _rs_p22.compute_mode_position_size(
            equity=bt.equity, entry_price=price, stop_price=price * (1 - sl_ratio),
            sortino_ratio=getattr(bt, 'sortino_ratio', 0.0),
            calmar_ratio=getattr(bt, 'calmar_ratio', 0.0),
            sharpe_ratio=getattr(bt, 'sharpe_ratio', 0.0),
            ofi=bar.get('ofi', 0.0), cvd_divergence=bar.get('cvd_divergence', 0.0),
            smart_money_flow=bar.get('smart_money_flow', 0.0),
            delta=bar.get('delta', 0.0), gamma=bar.get('gamma', 0.0),
            theta=bar.get('theta', 0.0), vega=bar.get('vega', 0.0),
        )
        if _tvf_lots > 0 and lots > 0:
            _tvf_factor = _tvf_lots / lots
            lots = max(1, round(lots * _tvf_factor))
    except Exception as _p22_err:
        logging.warning("[R16-P0-007] TVF仓位计算失败，使用风险预算回退: %s", _p22_err)
    return lots


def try_open_risk_checks(bt, params, symbol, price, lots, direction, bar, bar_time,
                         _position_volume, _open_positions):
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _signal = {
            "symbol": symbol, "direction": "BUY" if direction > 0 else "SELL",
            "price": price, "volume": lots, "amount": price * lots,
            "is_valid": True, "action": "OPEN", "account_id": "backtest",
            "signal_id": f"BT_{symbol}_{int(bar_time.timestamp())}",
        }
        _chk = _rs.check_before_trade(_signal)
        if _chk.is_block:
            logging.debug("[P-09] _try_open blocked by RiskService: %s", _chk.reason)
            return False
    except Exception as _rs_err:
        logging.warning("[P2-R3-D-19] RiskService check_before_trade failed, fail-safe block: %s", _rs_err)
        return False
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _position_data = {"instrument_id": bar.get("symbol", ""), "volume": _position_volume}
        _compliance = _rs.check_regulatory_compliance(_position_data)
        if not _compliance.get('compliant', True):
            logging.warning("[P-01] _try_open blocked by regulatory compliance: %s", _compliance.get('violations', []))
            return False
    except Exception as _compliance_err:
        logging.warning("[P2-R3-D-19] regulatory compliance check failed, fail-safe block: %s", _compliance_err)
        return False
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _use_span = params.get("use_span_margin", False)
        if _use_span:
            try:
                from risk_service import SimplifiedSPAN
                _span = SimplifiedSPAN()
                _required_margin = _span.calc_margin([{"instrument_id": symbol, "price": price, "quantity": lots, "delta": 0.5}])
            except Exception:
                _required_margin = price * lots * params.get("margin_ratio", 0.1)
        else:
            _required_margin = price * lots * params.get("margin_ratio", 0.1)
        _capital = _rs.check_capital_sufficiency(bt.equity, _required_margin, _open_positions)
        if not _capital.get('sufficient', False):
            logging.warning("[P-02] _try_open blocked by capital sufficiency: %s", _capital.get('reason', ''))
            return False
    except Exception as _capital_err:
        logging.warning("[P2-R3-D-19] capital sufficiency check failed, fail-safe block: %s", _capital_err)
        return False
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _exchange_status = _rs.check_exchange_status()
        if _exchange_status.get('status') != 'OPEN':
            logging.warning("[P-03] _try_open blocked by exchange status: %s", _exchange_status.get('status', 'UNKNOWN'))
            return False
    except Exception as _exchange_err:
        logging.warning("[P2-R3-D-19] exchange status check failed, fail-safe block: %s", _exchange_err)
        return False
    return True


def try_open_quality_gates(bar, params,
                           _infer_instrument_type):
    _sq = bar.get('_spread_quality', 0)
    if isinstance(_sq, property):
        _sq = 1
    try:
        _sq = int(_sq)
    except (TypeError, ValueError):
        _sq = 1
    if _sq == 0:
        logging.warning("[BACKTEST] _try_open blocked: _spread_quality=0, spread data unreliable")
        return False
    try:
        from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
        _sse = get_shadow_strategy_engine()
        if _sse and hasattr(_sse, 'is_absolute_ev_paused') and _sse.is_absolute_ev_paused():
            logging.warning("[BACKTEST] _try_open blocked: absolute EV paused, no new opens allowed")
            return False
    except Exception as _sse_e:
        logging.warning("[BACKTEST] _try_open shadow engine check failed, fail-safe block: %s", _sse_e)
        return False
    _omq = bar.get('_option_metadata_quality', 0)
    if isinstance(_omq, property):
        _omq = 0
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 0
    if _omq == 0:
        _instr_type = bar.get('instrument_type', 'future')
        if _instr_type in ('option_buyer', 'option_seller'):
            logging.warning("[P-39] _try_open blocked: _option_metadata_quality=0, option metadata incomplete")
            return False
    return True


def try_open_execute(bt, bar, params, symbol, price, lots, direction, reason,
                     bar_time, strategy_type, tp_ratio, sl_ratio,
                     _infer_instrument_type, _compute_fill_quantity,
                     _simulate_limit_order_queue, _backtest_order_split,
                     _compute_dynamic_slippage_bps, _compute_market_impact_v2,
                     _compute_commission, _get_contract_multiplier,
                     _infer_exchange_id, _infer_exchange_from_id,
                     _bt_capture_snapshot, _PendingOrder, _BacktestPosition,
                     compute_execution_delay_slippage_bps,
                     ENABLE_QUEUE_SIMULATION):
    _instrument_type_key = _infer_instrument_type(symbol)
    _instrument_participation_rate = params.get("max_participation_rate", 1.0)
    if _instrument_type_key in ('option_buyer', 'option_seller'):
        _instrument_participation_rate = min(_instrument_participation_rate, params.get("option_max_participation_rate", 0.10))
    elif _instrument_type_key == 'future':
        _instrument_participation_rate = min(_instrument_participation_rate, params.get("future_max_participation_rate", 0.15))
    _adjusted_params = dict(params)
    _adjusted_params["max_participation_rate"] = _instrument_participation_rate

    actual_lots = _compute_fill_quantity(lots, bar, _adjusted_params)

    _order_type = params.get("default_order_type", "taker")
    if _order_type == "maker" and params.get("enable_queue_simulation", ENABLE_QUEUE_SIMULATION):
        _queue_result = _simulate_limit_order_queue(
            order_price=price, current_price=price, bar=bar,
            order_lots=actual_lots, enable_queue=True
        )
        if not _queue_result["filled"]:
            logging.debug("[BF-P1-02] 限价单排队未成交: %s", symbol)
            return
        if _queue_result["fill_lots"] < actual_lots:
            actual_lots = _queue_result["fill_lots"]
    if actual_lots <= 0:
        if params.get("execution_model", "standard") == "institutional" and lots > 0:
            if len(bt.pending_orders) < 100:
                bt.pending_orders.append(_PendingOrder(
                    symbol=symbol, order_type="open", volume=direction * lots,
                    lots=lots, reason=reason, signal_bar_idx=bt.bar_idx,
                    signal_price=price, tp_ratio=tp_ratio, sl_ratio=sl_ratio,
                    params_snapshot=dict(params), created_at_bar=bt.bar_idx,
                    fee_type=params.get("default_order_type", "taker"), retry_count=0,
                ))
        return
    _fill_ratio = actual_lots / lots if lots > 0 else 1.0
    if actual_lots < lots:
        lots = actual_lots

    volume = direction * lots

    _exec_delay_ms = params.get("execution_delay_ms", 50)
    if _exec_delay_ms > 0:
        _bar_high = bar.get("high", price) if bar is not None else price
        _bar_low = bar.get("low", price) if bar is not None else price
        _bar_dur = float(params.get("bar_duration_sec", 60.0))
        _delay_slippage_bps = compute_execution_delay_slippage_bps(
            price=price, bar_high=_bar_high, bar_low=_bar_low,
            bar_duration_sec=_bar_dur, exec_delay_ms=_exec_delay_ms, z_score=1.0
        )
        _delay_adj = price * _delay_slippage_bps / 10000.0
        if direction > 0:
            price += _delay_adj
        else:
            price -= _delay_adj

    sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
    sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 0)
    _max_sub_lots = int(params.get("backtest_max_sub_order_lots", 5))
    _sub_orders = _backtest_order_split(lots, max_sub_order_lots=_max_sub_lots)
    _total_slippage_cost = 0.0
    _multiplier = _get_contract_multiplier(symbol)
    for _sub_lots in _sub_orders:
        _sub_slippage = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
        _total_slippage_cost += _sub_lots * price * _multiplier * _sub_slippage / 10000.0
    slip_cost = _total_slippage_cost
    _market_impact_cost = _compute_market_impact_v2(lots, bar, price, params)
    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(symbol)))
    bt.equity -= (commission + slip_cost + _market_impact_cost)

    if params.get("execution_model", "standard") == "institutional":
        open_delay = int(params.get("open_execution_delay_bars", 0))
        if open_delay > 0:
            if len(bt.pending_orders) >= 100:
                bt.pending_orders.pop(0)
            bt.pending_orders.append(_PendingOrder(
                symbol=symbol, order_type="open", volume=volume,
                lots=lots, reason=reason, signal_bar_idx=bt.bar_idx,
                signal_price=price, tp_ratio=tp_ratio, sl_ratio=sl_ratio,
                params_snapshot=dict(params), created_at_bar=bt.bar_idx,
                fee_type=params.get("default_order_type", "taker"),
            ))
            bt.last_signal_time = bar_time
            bt.total_signals += 1
            _bt_capture_snapshot(bt, "signal", f"{symbol} {direction} {reason} (pending)", strategy_type, bar)
            return

    pos = _BacktestPosition(
        instrument_id=symbol, volume=volume, open_price=price,
        open_time=bar_time, stop_profit_price=sp_price, stop_loss_price=sl_price,
        open_reason=reason, lots=lots, open_state=bt.current_state,
        open_strength=bar.get("strength", 0.0),
        instrument_type=_infer_instrument_type(symbol), fill_ratio=_fill_ratio,
    )
    bt.positions[symbol] = pos
    bt.last_signal_time = bar_time
    bt.total_signals += 1
    _bt_capture_snapshot(bt, "signal", f"{symbol} {direction} {reason}", strategy_type, bar)


def check_option_metadata_quality(bt, bar):
    _omq = bar.get('_option_metadata_quality', 0) if isinstance(bar, pd.Series) else getattr(bar, '_option_metadata_quality', 0)
    if isinstance(_omq, property):
        _omq = 0
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 0
    if _omq == 0:
        _instr_type = bar.get('instrument_type', 'future')
        if _instr_type in ('option_buyer', 'option_seller'):
            logging.warning("[BACKTEST] _check_positions: _option_metadata_quality=0, Greeks data unreliable, blocking new opens")
            bt.new_open_blocked = True
        else:
            logging.debug("[BACKTEST] _check_positions: _option_metadata_quality=0 for non-option instrument, proceeding")


def check_stop_profit_loss(pos, price, params):
    should_close = False
    close_reason = ""
    if pos.volume > 0:
        if price >= pos.stop_profit_price or np.isclose(price, pos.stop_profit_price):
            should_close = True
            close_reason = "StopProfit"
        elif price <= pos.stop_loss_price or np.isclose(price, pos.stop_loss_price):
            should_close = True
            close_reason = "StopLoss"
    elif pos.volume < 0:
        if price <= pos.stop_profit_price or np.isclose(price, pos.stop_profit_price):
            should_close = True
            close_reason = "StopProfit"
        elif price >= pos.stop_loss_price or np.isclose(price, pos.stop_loss_price):
            should_close = True
            close_reason = "StopLoss"
    return should_close, close_reason


def check_time_stops(pos, price, bar_time, params, hold_minutes, open_reason,
                     _check_two_stage_stop, _resolve_time_stop, current_state):
    should_close = False
    close_reason = ""
    hard_stop_min = _resolve_time_stop(params, open_reason, current_state)
    if _check_two_stage_stop(pos, price, bar_time, params):
        should_close = True
        close_reason = "TwoStageTimeStop"
    elif hold_minutes >= hard_stop_min:
        should_close = True
        close_reason = "HardTimeStop"
    return should_close, close_reason


def check_eod_close(bar_time):
    eod_hour = bar_time.hour
    eod_minute = bar_time.minute
    if eod_hour == 14 and eod_minute >= 55:
        return True, "EOD"
    elif eod_hour == 2 and eod_minute >= 25:
        return True, "EOD_NIGHT"
    return False, ""
