"""
回测主循环 — 从backtest_runner_base.py拆分
职责: 主循环、待执行订单处理、Bar内止损、动态滑点、平仓决策、平仓执行
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _compute_dynamic_slippage_bps(
    price: float,
    bid_ask_spread: float,
    base_slippage_bps: float = None,
    spread_quality: int = 1,
    bar: pd.Series = None,
    params: Dict[str, float] = None,
) -> float:
    """动态滑点模型

    核心：滑点 = max(base_slippage, bid_ask_spread占比放大) * 到期日倍增
    - 流动性好（spread小）：用base_slippage（如1bps）
    - 流动性差（spread大）：spread/price放大为bps
    - 远月/深度虚值spread可达5-20个tick，此时滑点远超1bps
    - P0-6修复: 到期日附近滑点倍增(3天20x/7天10x/14天3x)

    P2-R3-P-14: spread NaN时退化 — 当bid_ask_spread为NaN或spread_quality=0时,
    退化为静态base_slippage_bps，丢失动态滑点信息。已知限制:
    深度虚值/远月合约spread数据常缺失，退化为静态滑点会导致回测低估实盘滑点，
    建议在数据预处理阶段补全spread(用近月合约spread×虚值程度倍数)。

    P1-R11-01修复: 回测使用历史spread，实盘使用实时行情spread。
    两者存在系统性差异：历史spread为快照(tick级)，实盘为实时盘口。
    为弥补此差异，回测路径添加backtest_slippage_premium_bps保守溢价，
    确保回测滑点略高于实盘，避免回测过度乐观。
    返回结果中包含spread_source字段标识数据来源。
    """
    from ali2026v3_trading.参数池.backtest_runner_base import (
        SLIPPAGE_BPS, safe_price_check,
        _infer_liquidity_tier, _get_expiry_slippage_multiplier,
    )
    if base_slippage_bps is None:
        base_slippage_bps = SLIPPAGE_BPS

    _premium_bps = 0.0
    if params is not None:
        _premium_bps = float(params.get("backtest_slippage_premium_bps", 0.5))

    if not safe_price_check(price):
        return base_slippage_bps + _premium_bps
    if bid_ask_spread <= 0 or spread_quality == 0:
        if spread_quality == 0 and bid_ask_spread <= 0:
            logger.debug("[SLIPPAGE_DEGRADE] spread=%.4f quality=%d, using static %.1fbps", bid_ask_spread, spread_quality, base_slippage_bps)
        return base_slippage_bps + _premium_bps
    spread_bps = bid_ask_spread / price * 10000.0 if price > 0 else 0.0
    _quality_scale_map = {1: 0.75, 2: 0.5, 3: 0.3}
    _quality_scale = _quality_scale_map.get(spread_quality, 0.75)
    _liquidity_tier_map = {
        'future_main': 1.0,
        'future_sub': 1.2,
        'option_atm': 1.0,
        'option_otm': 1.5,
        'option_far': 2.0,
    }
    _liquidity_tier = _liquidity_tier_map.get(_infer_liquidity_tier(bar), 1.2)
    base = max(base_slippage_bps, spread_bps * _quality_scale * _liquidity_tier)
    expiry_mult = 1.0
    if bar is not None and params is not None:
        expiry_mult = _get_expiry_slippage_multiplier(bar, params)
    return base * expiry_mult + _premium_bps


def _check_intra_bar_stop_loss(
    pos,
    bar: pd.Series,
    prev_bar: Optional[pd.Series],
    params: Dict[str, float],
) -> Tuple[bool, str, float]:
    """BF-01: Bar内止损路径推断 + 跨Bar跳空处理

    实现逻辑:
    - 跳空穿透优先检测: 开盘价直接跳过止损线 → 成交价=开盘价
    - Bar内检测: high/low跨越止损线 → 路径推断确定触发顺序
    - 多头止损: low或open穿透止损线 → 触发
    - 空头止损: high或open穿透止损线 → 触发
    - 优先级: (跳空 > Bar内) × (止损 > 止盈)
    - 路径推断: 阳线→low先触及(先检查止损), 阴线→high先触及(先检查止盈)

    Args:
        pos: 当前持仓
        bar: 当前Bar行情
        prev_bar: 上一Bar行情(用于跳空检测)
        params: 参数字典

    Returns:
        Tuple[triggered, reason, fill_price]
    """
    if pos is None:
        return (False, "", 0.0)

    bar_open = bar.get("open", 0.0)
    bar_high = bar.get("high", 0.0)
    bar_low = bar.get("low", 0.0)
    bar_close = bar.get("close", 0.0)

    if bar_open <= 0 or bar_high <= 0 or bar_low <= 0:
        logger.warning("[C-3] OHLC字段缺失: open=%.4f high=%.4f low=%.4f", bar_open, bar_high, bar_low)
        return (False, "", 0.0)

    enable_gap = params.get("enable_gap_handling", False)
    tp_price = pos.stop_profit_price
    sl_price = pos.stop_loss_price

    if pos.volume > 0:
        prev_close = prev_bar.get("close", bar_open) if prev_bar is not None and enable_gap else bar_open
        if enable_gap and prev_close > 0:
            if bar_open <= sl_price:
                return (True, "StopLoss_GapDown", bar_open)
            if bar_open >= tp_price:
                return (True, "StopProfit_GapUp", bar_open)
        is_bullish = bar_close >= bar_open
        if is_bullish:
            if bar_low <= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
            if bar_high >= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
        else:
            if bar_high >= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
            if bar_low <= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
    elif pos.volume < 0:
        prev_close = prev_bar.get("close", bar_open) if prev_bar is not None and enable_gap else bar_open
        if enable_gap and prev_close > 0:
            if bar_open >= sl_price:
                return (True, "StopLoss_GapUp", bar_open)
            if bar_open <= tp_price:
                return (True, "StopProfit_GapDown", bar_open)
        is_bullish = bar_close >= bar_open
        if is_bullish:
            if bar_low <= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
            if bar_high >= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
        else:
            if bar_high >= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
            if bar_low <= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)

    return (False, "", 0.0)


def _determine_close_decision(pos, price, bar_time, bar, prev_bar, params, hold_minutes, hard_stop_min):


    should_close = False
    close_reason = ""

    if params.get("enable_intra_bar_stop_loss", False):
        intra_triggered, intra_reason, intra_fill = _check_intra_bar_stop_loss(pos, bar, prev_bar, params)
        if intra_triggered:
            should_close = True
            close_reason = intra_reason
            price = intra_fill

    if not should_close and pos.volume > 0:
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

    if not should_close and _check_two_stage_stop(pos, price, bar_time, params):
        should_close = True
        close_reason = "TwoStageTimeStop"

    if not should_close and hold_minutes >= hard_stop_min:
        should_close = True
        close_reason = "HardTimeStop"

    if not should_close:
        eod_hour = bar_time.hour
        eod_minute = bar_time.minute
        if eod_hour == 14 and eod_minute >= 55:
            should_close = True
            close_reason = "EOD"
        elif eod_hour == 2 and eod_minute >= 25:
            should_close = True
            close_reason = "EOD_NIGHT"

    if should_close:
        _close_delay = int(params.get("close_profit_delay_bars", 0))
        _is_stop_loss_close = "StopLoss" in str(close_reason)
        _stop_no_delay = params.get("stop_loss_no_delay", True)
        if _close_delay > 0 and not (_is_stop_loss_close and _stop_no_delay):
            _delay_remaining = getattr(pos, '_close_delay_remaining', None)
            if _delay_remaining is None:
                setattr(pos, '_close_delay_remaining', _close_delay - 1)
                logger.debug("[P1-R11-04] 平仓延迟: 延迟%d bar, 原因=%s", _close_delay, close_reason)
                should_close = False
            elif _delay_remaining > 0:
                pos._close_delay_remaining = _delay_remaining - 1
                should_close = False

    return should_close, close_reason, price


def _execute_position_close(bt, pos, symbol, price, close_reason, bar, bar_time, params, float_pnl_pct):
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _get_contract_multiplier, _compute_dynamic_slippage_bps,
        _compute_cascade_slippage_bps, _compute_commission,
        _safe_equity_add, _bt_capture_snapshot, _ClosedTrade,
        BACKTEST_THRESHOLDS, _infer_exchange_id, _infer_exchange_from_id,
    )
    _mult = _get_contract_multiplier(symbol)
    pnl = (price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 0)
    slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
    _cascade_slip_bps = _compute_cascade_slippage_bps(
        base_slippage_bps=slip_bps,
        close_volume=pos.lots,
        avg_volume=bar.get("volume", 1.0),
        is_state_switch=(close_reason in ("state_conflict_force_close", "HardTimeStop")),
    )
    slip = price * _cascade_slip_bps / 10000 * pos.lots
    commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(symbol)))
    net_pnl = pnl - slip - commission
    if not np.isfinite(net_pnl):
        logger.warning("[R4-P-06] NaN/inf detected in net_pnl: pnl=%.4f, slip=%.4f, commission=%.4f, net_pnl=%.4f", pnl, slip, commission, net_pnl)
        net_pnl = 0.0
    _safe_equity_add(bt, net_pnl)
    bt.total_trades += 1
    _bt_capture_snapshot(bt, "close", f"{close_reason} {symbol}", "", bar)
    bt.recent_pnls.append(net_pnl)
    if len(bt.recent_pnls) > 50:
        bt.recent_pnls = bt.recent_pnls[-50:]
    pnl_pct = float_pnl_pct if pos.open_price > 0 else 0.0
    hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
    bt.closed_trades.append(_ClosedTrade(
        pnl=net_pnl, pnl_pct=pnl_pct, close_reason=close_reason,
        hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
        premium_pnl=0.0, delta_pnl=0.0,
        stage1_passed=getattr(pos, 'stage1_passed', False),
    ))
    del bt.positions[symbol]

    daily_dd = (bt.daily_start_equity - (bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity)) / bt.daily_start_equity if bt.daily_start_equity > 0 else 0
    if daily_dd > BACKTEST_THRESHOLDS["circuit_breaker_daily_dd"]:
        pause_sec = params.get("circuit_breaker_pause_sec", 180.0)
        bt.circuit_breaker_until = bar_time + pd.Timedelta(seconds=pause_sec)
        bt.circuit_breaker_events.append({
            'trigger_time': str(bar_time),
            'daily_dd': daily_dd,
            'pause_sec': pause_sec,
            'resume_time': str(bt.circuit_breaker_until),
            'equity_at_trigger': bt.equity,
        })


def _run_backtest_main_loop(bt, bar_data, params, strategy_type):
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _check_positions, _backfill_bar_fields, _update_mtm_equity,
        _reset_daily, _check_state_transition, _check_logic_reversal,
        _check_safety, _check_backtest_health, _try_open,
        _calculate_limit_prices, _infer_instrument_type,
        BACKTEST_THRESHOLDS,
    )
    from ali2026v3_trading.shared_utils import compute_execution_delay_slippage_bps

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        prev_bar = bar_data.iloc[idx - 1] if idx > 0 else None
        bt.bar_idx = idx
        _execution_delay_ms = params.get("execution_delay_ms", 50)
        if _execution_delay_ms > 0:
            _bar_high = bar.get("high", 0.0)
            _bar_low = bar.get("low", 0.0)
            _bar_close = bar.get("close", 0.0)
            _bar_dur = float(params.get("bar_duration_sec", 60.0))
            _main_delay_bps = compute_execution_delay_slippage_bps(
                price=_bar_close, bar_high=_bar_high, bar_low=_bar_low,
                bar_duration_sec=_bar_dur, exec_delay_ms=_execution_delay_ms, z_score=1.0
            )
            if _main_delay_bps > 0 and hasattr(bt, 'equity'):
                bt.equity -= bt.equity * _main_delay_bps / 10000.0 * 0.001
        _backfill_bar_fields(bar)
        _update_mtm_equity(bt, bar, params)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        _bar_ts = bar.name if isinstance(bar.name, (int, float)) else getattr(bar, 'timestamp', None)
        if _bar_ts is not None and hasattr(bt, '_last_bar_ts') and bt._last_bar_ts is not None:
            if _bar_ts == bt._last_bar_ts:
                logger.warning("[P1-R9-30] 重复Bar时间戳: %s, 跳过", _bar_ts)
                continue
            elif _bar_ts < bt._last_bar_ts:
                logger.warning("[P1-R9-30] Bar时间戳回退: %s < %s, 跳过", _bar_ts, bt._last_bar_ts)
                continue
        bt._last_bar_ts = _bar_ts
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params, prev_bar=prev_bar)
            elif params.get("enable_intra_bar_stop_loss", False):
                _check_positions(bt, bar, params, prev_bar=None)

        _process_pending_orders(bt, bar, params, idx)

        _bar_limit_info = _calculate_limit_prices(bar, instrument_type=_infer_instrument_type(bar.get('symbol', '')).replace('option_buyer', 'OPTION_ETF').replace('option_seller', 'OPTION_ETF').replace('future', 'FUTURE'))
        if _bar_limit_info['is_limit_up'] or _bar_limit_info['is_limit_down']:
            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)
            continue

        state_check_interval_sec = params.get("state_check_interval_sec", 180.0)
        if idx > 0:
            prev_time = bar_data.iloc[idx - 1].get("minute", None)
            curr_time = bar.get("minute", None)
            if prev_time is not None and curr_time is not None:
                try:
                    if hasattr(prev_time, 'timestamp') and hasattr(curr_time, 'timestamp'):
                        time_gap_sec = (curr_time - prev_time).total_seconds()
                        if time_gap_sec > state_check_interval_sec:
                            n_skipped_checks = round(time_gap_sec / state_check_interval_sec)
                            for _ in range(n_skipped_checks):
                                bt.state_confirm_count += 1
                except Exception as _e:
                    logger.warning("[BACKTEST] state_check_interval calculation failed: %s", _e)

        decision_interval = params.get("decision_interval_minutes", params.get("bar_interval", 1))
        if decision_interval > 1 and (idx % decision_interval != 0):
            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)
            continue

        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            _health_ok = _check_backtest_health(bt, params, bar_time)
            strength = bar.get("strength", 0)
            should_open = _health_ok and strength > 0.3 and len(bt.positions) < int(params.get("max_open_positions", 3))
            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False
            if strategy_type == "shadow_random":
                should_open = np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3))
            if should_open:
                _try_open(bt, bar, params, strategy_type=strategy_type)

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)


def _process_pending_orders(bt, bar, params, idx):
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _simulate_order_cancel, _compute_fill_quantity,
        _compute_dynamic_slippage_bps, _get_contract_multiplier,
        _compute_commission, _BacktestPosition, _infer_instrument_type,
        _infer_exchange_id, _infer_exchange_from_id,
        CANCEL_DELAY_MS, CANCEL_FAILURE_RATE, ENABLE_CANCEL_SIMULATION,
        safe_price_check,
    )
    if not (params.get("execution_model", "standard") == "institutional" and bt.pending_orders):
        return
    _remaining_orders = []
    for order in bt.pending_orders:
        order_age = idx - order.created_at_bar
        required_delay = params.get("open_execution_delay_bars", 1) if order.order_type == "open" else params.get("close_profit_delay_bars", 1)
        if order_age >= required_delay:
            _cancel_delay_bars = max(1, int(params.get("cancel_delay_ms", CANCEL_DELAY_MS) / 60000.0))
            if order_age > required_delay + _cancel_delay_bars:
                _cancel_result = _simulate_order_cancel(
                    order_id=f"{order.symbol}_{order.created_at_bar}",
                    cancel_delay_ms=int(params.get("cancel_delay_ms", CANCEL_DELAY_MS)),
                    failure_rate=float(params.get("cancel_failure_rate", CANCEL_FAILURE_RATE)),
                    enable_cancel=params.get("enable_cancel_simulation", ENABLE_CANCEL_SIMULATION),
                )
                if _cancel_result["success"]:
                    logger.debug("[BF-P1-09] 延迟订单撤单成功: %s (延迟%d bar)", order.symbol, order_age)
                    continue
                else:
                    logger.warning("[BF-P1-09] 延迟订单撤单失败，继续执行: %s, 原因=%s", order.symbol, _cancel_result["reason"])
            if order.symbol not in bt.positions:
                exec_price = bar.get("open", bar.get("close", 0.0))
                if safe_price_check(exec_price):
                    if order.order_type == "open":
                        _delay_fill_lots = _compute_fill_quantity(order.lots, bar, order.params_snapshot)
                        if _delay_fill_lots <= 0:
                            if order.retry_count < 3:
                                order.retry_count += 1
                                _remaining_orders.append(order)
                            continue
                        _delay_lots = _delay_fill_lots
                    else:
                        _delay_lots = order.lots
                    bid_ask = bar.get("bid_ask_spread", 0.0)
                    spread_q = bar.get("_spread_quality", 0)
                    slip_bps = _compute_dynamic_slippage_bps(exec_price, bid_ask, spread_quality=spread_q, bar=bar, params=order.params_snapshot)
                    _mult = _get_contract_multiplier(order.symbol)
                    slip_cost = exec_price * slip_bps / 10000 * _delay_lots
                    commission = _compute_commission(order.symbol, _delay_lots, is_open=(order.order_type == "open"), exchange_id=_infer_exchange_id(order.symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(order.symbol)), open_time=bar.get("minute", pd.NaT) if order.order_type == "open" else None, close_time=bar.get("minute", pd.NaT) if order.order_type != "open" else None)
                    if order.order_type == "open":
                        direction = 1 if order.volume > 0 else -1
                        _delay_volume = direction * _delay_lots
                        if direction > 0:
                            exec_price += exec_price * slip_bps / 10000
                        else:
                            exec_price -= exec_price * slip_bps / 10000
                        sp_price = exec_price * order.tp_ratio if _delay_volume > 0 else exec_price / order.tp_ratio
                        sl_price = exec_price * (1 - order.sl_ratio) if _delay_volume > 0 else exec_price * (1 + order.sl_ratio)
                        _delay_fill_ratio = _delay_lots / order.lots if order.lots > 0 else 1.0
                        pos = _BacktestPosition(
                            instrument_id=order.symbol, volume=_delay_volume,
                            open_price=exec_price, open_time=bar.get("minute", pd.NaT),
                            stop_profit_price=sp_price, stop_loss_price=sl_price,
                            open_reason=order.reason, lots=_delay_lots,
                            open_state=bt.current_state, open_strength=0.0,
                            instrument_type=_infer_instrument_type(order.symbol),
                            fill_ratio=_delay_fill_ratio,
                        )
                        bt.positions[order.symbol] = pos
                        bt.equity -= (commission + slip_cost)
                        bt.total_signals += 1
                        bt.total_trades += 1
                        logger.debug("[BF-02/03] 延迟开仓执行: %s @ %.4f (延迟%d bar)", order.symbol, exec_price, order_age)
        else:
            if order_age < required_delay + 3:
                _remaining_orders.append(order)
            else:
                logger.warning("[BF-02/03] 延迟订单超时放弃: %s (延迟%d bar)", order.symbol, order_age)
    bt.pending_orders = _remaining_orders


def _try_open(
    bt,
    bar: pd.Series,
    params: Dict[str, float],
    strategy_type: str = "main",
    prev_bar: Optional[pd.Series] = None,
) -> None:
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _is_consecutive_loss_paused, _STATE_REASON_MAP, BACKTEST_THRESHOLDS,
        safe_price_check, _resolve_tp_sl, _compute_lots_with_risk_budget,
        _infer_instrument_type, _compute_fill_quantity,
        _simulate_limit_order_queue, _backtest_order_split,
        _compute_dynamic_slippage_bps, _compute_market_impact_v2,
        _compute_commission, _get_contract_multiplier,
        _infer_exchange_id, _infer_exchange_from_id,
        _bt_capture_snapshot, _PendingOrder, _BacktestPosition,
        compute_execution_delay_slippage_bps, ENABLE_QUEUE_SIMULATION,
    )
    from ali2026v3_trading.参数池 import backtest_position_manager as _bpm
    bar_time = bar.get("minute", pd.NaT)
    _gate_result = _bpm.try_open_check_gates(
        bt, bar, params, bar_time, strategy_type,
        _is_consecutive_loss_paused, _STATE_REASON_MAP,
        BACKTEST_THRESHOLDS, safe_price_check)
    if _gate_result is None:
        return
    reason, symbol, price = _gate_result
    tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
    lots = _bpm.try_open_compute_lots(bt, params, price, sl_ratio, reason, bar, _compute_lots_with_risk_budget)
    if lots <= 0:
        return
    imbalance = bar.get("imbalance", 0)
    if imbalance == 0:
        return
    direction = 1 if imbalance > 0 else -1
    if strategy_type == "shadow_reverse":
        direction = -direction
    _position_volume = sum(abs(int(getattr(pos, 'volume', 0))) for pos in bt.positions.values())
    _open_positions = len(bt.positions)
    if not _bpm.try_open_risk_checks(bt, params, symbol, price, lots, direction, bar, bar_time, _position_volume, _open_positions):
        return
    if not _bpm.try_open_quality_gates(bar, params, _infer_instrument_type):
        return
    _bpm.try_open_execute(
        bt, bar, params, symbol, price, lots, direction, reason,
        bar_time, strategy_type, tp_ratio, sl_ratio,
        _infer_instrument_type, _compute_fill_quantity,
        _simulate_limit_order_queue, _backtest_order_split,
        _compute_dynamic_slippage_bps, _compute_market_impact_v2,
        _compute_commission, _get_contract_multiplier,
        _infer_exchange_id, _infer_exchange_from_id,
        _bt_capture_snapshot, _PendingOrder, _BacktestPosition,
        compute_execution_delay_slippage_bps,
        ENABLE_QUEUE_SIMULATION)


def _check_two_stage_stop(
    pos,
    price: float,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
) -> bool:
    _bar_period = float(params.get("bar_period", 1.0))
    stage1_min_minutes = params.get("stage1_min_minutes", 90.0) * _bar_period
    stage1_profit_threshold = params.get("stage1_profit_threshold", 0.002)
    stage2_slope_window = max(2, int(params.get("stage2_slope_window", 10)))
    stage2_slope_threshold = params.get("stage2_slope_threshold", 0.0)

    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct

    if not pos.stage1_passed:
        if hold_minutes >= stage1_min_minutes and pos.max_float_profit >= stage1_profit_threshold:
            pos.stage1_passed = True

    if not pos.stage1_passed:
        return False

    pos.profit_history.append(float_pnl_pct)
    if isinstance(pos.profit_history, list) and len(pos.profit_history) > pos._PROFIT_HISTORY_MAXLEN:
        pos.profit_history = pos.profit_history[-1000:]
    if pos.last_check_time is not None and pos.last_check_time == bar_time:
        pass
    pos.last_check_time = bar_time

    if len(pos.profit_history) >= stage2_slope_window:
        window = pos.profit_history[-stage2_slope_window:]
        _bar_period_min = float(params.get("bar_period", 1.0))
        slope = (window[-1] - window[0]) / (stage2_slope_window * _bar_period_min)
        if slope < stage2_slope_threshold:
            return True

    return False


def _compute_option_mtm_price(
    spot_price: float,
    strike_price: float,
    remaining_days: int,
    iv: float,
    option_type: str = "call",
    risk_free_rate: float = 0.02,
) -> float:
    """BF-05: 使用Black-Scholes模型计算期权理论价格用于逐Bar MTM

    Args:
        spot_price: 标的现价
        strike_price: 行权价
        remaining_days: 剩余天数
        iv: 隐含波动率
        option_type: 期权类型 call/put
        risk_free_rate: 无风险利率

    Returns:
        float: 期权理论价格
    """
    if spot_price <= 0 or strike_price <= 0 or remaining_days <= 0 or iv <= 0:
        if spot_price <= 0 or iv <= 0:
            logger.debug("[C-3] _compute_option_mtm_price无效输入: spot_price=%.4f iv=%.4f", spot_price, iv)
        return 0.0
    try:
        import math as _m
        t = remaining_days / 365.0
        d1 = (_m.log(spot_price / strike_price) + (risk_free_rate + 0.5 * iv ** 2) * t) / (iv * _m.sqrt(t))
        d2 = d1 - iv * _m.sqrt(t)
        def _norm_cdf(x):
            return 0.5 * (1.0 + _m.erf(x / _m.sqrt(2.0)))
        if option_type == "call":
            price = spot_price * _norm_cdf(d1) - strike_price * _m.exp(-risk_free_rate * t) * _norm_cdf(d2)
        else:
            price = strike_price * _m.exp(-risk_free_rate * t) * _norm_cdf(-d2) - spot_price * _norm_cdf(-d1)
        return max(0.0, price)
    except Exception:
        return 0.0


def _update_mtm_equity(
    bt,
    bar: pd.Series,
    params: Dict[str, float],
) -> None:
    """BF-05: 逐Bar更新MTM权益

    当enable_mtm_equity=True时，逐Bar计算持仓Mark-to-Market权益，
    使用BS模型对期权持仓进行理论定价，期货持仓使用最新价。

    Args:
        bt: 回测状态
        bar: 当前Bar行情
        params: 参数字典
    """
    from ali2026v3_trading.参数池.backtest_runner_base import _get_contract_multiplier
    if not params.get("enable_mtm_equity", False):
        return
    mtm = bt.equity
    for sym, pos in bt.positions.items():
        if getattr(pos, 'instrument_type', 'future') in ('option_buyer', 'option_seller'):
            if params.get("use_option_bs_pricing", False):
                spot = bar.get("underlying_price", 0.0)
                strike = getattr(pos, 'strike_price', 0.0)
                days = getattr(pos, 'remaining_days', 0)
                iv = getattr(pos, 'open_iv', 0.0)
                opt_type = getattr(pos, 'option_type', 'call')
                theo = _compute_option_mtm_price(spot, strike, days, iv, opt_type)
                if theo > 0:
                    mtm += (theo - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
            else:
                current_price = bar.get("close", pos.open_price)
                mtm += (current_price - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
        else:
            current_price = bar.get("close", pos.open_price)
            mtm += (current_price - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
    bt.mtm_equity = mtm
    bt.mtm_equity_curve.append(mtm)
    if mtm > bt.mtm_peak_equity:
        bt.mtm_peak_equity = mtm
    dd = (bt.mtm_peak_equity - mtm) / bt.mtm_peak_equity if bt.mtm_peak_equity > 0 else 0.0
    if dd > bt.mtm_max_drawdown:
        bt.mtm_max_drawdown = dd


def _compute_fill_quantity(
    order_lots: int,
    bar: pd.Series,
    params: Dict[str, float],
) -> int:
    """BF-06: 基于参与率限制计算实际成交量

    参与率 = 成交量 / Bar成交量, 限制不超过max_participation_rate
    当enable_partial_fill=False时返回原始手数(向后兼容)

    Args:
        order_lots: 请求成交手数
        bar: 当前Bar行情
        params: 参数字典

    Returns:
        int: 实际可成交手数
    """
    if not params.get("enable_partial_fill", True):
        return order_lots
    if order_lots <= 0:
        return 0
    max_part_rate = params.get("max_participation_rate", 1.0)
    min_fill = int(params.get("min_fill_lots", 1))
    bar_volume = bar.get("volume", 0)
    if bar_volume <= 0 or max_part_rate >= 1.0:
        return order_lots
    max_fill = max(min_fill, int(bar_volume * max_part_rate))
    return min(order_lots, max_fill)


def _compute_market_impact_v2(
    fill_lots: int,
    bar: pd.Series,
    price: float,
    params: Dict[str, float],
) -> float:
    """BF-06: Almgren-Chriss冲击成本（二级模型，默认关闭）

    当enable_market_impact=False时返回0.0(向后兼容)
    冲击成本 = eta * sigma * |X|/V + gamma * sigma * (X/V)^2

    Args:
        fill_lots: 成交手数
        bar: 当前Bar行情
        price: 成交价格
        params: 参数字典

    Returns:
        float: 冲击成本(价格单位)
    """
    if not params.get("enable_market_impact", False):
        return 0.0
    if fill_lots <= 0 or price <= 0:
        return 0.0
    eta = params.get("market_impact_eta", 0.1)
    gamma = params.get("market_impact_gamma", 0.05)
    bar_volume = bar.get("volume", 1.0)
    if bar_volume <= 0:
        bar_volume = 1.0
    participation = abs(fill_lots) / bar_volume
    bar_range = bar.get("high", price) - bar.get("low", price)
    sigma = bar_range / price if price > 0 else 0.01
    impact = eta * sigma * participation + gamma * sigma * participation ** 2
    return price * impact


def _apply_fidelity_presets(params: Dict[str, Any]) -> Dict[str, Any]:
    """BF-06: 保真度预设一键切换

    当backtest_fidelity_mode为institutional时，一键设置所有保真度参数，
    启用Bar内止损、跳空处理、延迟执行、MTM权益等机构级回测特性。
    当mode为standard时保持默认值(向后兼容)。

    Args:
        params: 参数字典(会被原地修改)

    Returns:
        Dict: 修改后的参数字典
    """
    mode = params.get("backtest_fidelity_mode", "standard")
    if mode == "institutional":
        params.setdefault("execution_model", "institutional")
        params.setdefault("enable_intra_bar_stop_loss", True)
        params.setdefault("enable_gap_handling", True)
        params.setdefault("open_execution_delay_bars", 1)
        params.setdefault("close_profit_delay_bars", 1)
        params.setdefault("stop_loss_no_delay", True)
        params.setdefault("default_order_type", "maker")
        params.setdefault("enable_mtm_equity", True)
        params.setdefault("use_option_bs_pricing", True)
        params.setdefault("enable_partial_fill", True)
        params.setdefault("max_participation_rate", 0.15)
        params.setdefault("enable_market_impact", True)
        params.setdefault("market_impact_eta", 0.1)
        params.setdefault("market_impact_gamma", 0.05)
        params.setdefault("enable_queue_simulation", True)
        params.setdefault("enable_slippage_model", True)
        params.setdefault("backtest_slippage_premium_bps", 1.0)
    return params


def _check_positions_phase1_state_conflict(bt, bar, params):
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _check_safety, _get_contract_multiplier,
        _compute_dynamic_slippage_bps, _compute_commission,
        _safe_equity_add, _infer_instrument_type,
        _infer_exchange_id, _infer_exchange_from_id,
        _calculate_limit_prices, _ClosedTrade,
    )
    from ali2026v3_trading.参数池 import backtest_position_manager as _bpm2
    _bpm2.check_option_metadata_quality(bt, bar)
    bar_time_p09 = bar.get("minute", pd.NaT)
    if pd.isna(bar_time_p09):
        logger.warning("[R22-TIME-03] bar缺少minute字段，跳过P0-9状态反转平仓检查")
        return
    if not _check_safety(bt, bar_time_p09, params, allow_close=True, bar=bar):
        return
    for sym in list(bt.positions.keys()):
        pos = bt.positions[sym]
        _state_conflict = False
        if hasattr(bt, 'current_state') and hasattr(pos, 'open_reason'):
            if bt.current_state == "incorrect_reversal" and "correct" in str(pos.open_reason):
                _state_conflict = True
            elif bt.current_state == "correct_trending" and "incorrect" in str(pos.open_reason):
                _state_conflict = True
        if _state_conflict:
            from ali2026v3_trading.参数池 import backtest_safety_checker as _bsc2
            _bsc2._force_close_position(bt, sym, pos, bar, bar_time_p09, "state_conflict_force_close",
                _get_contract_multiplier=_get_contract_multiplier,
                _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
                _compute_commission=_compute_commission,
                _safe_equity_add=_safe_equity_add,
                _infer_instrument_type=_infer_instrument_type,
                _infer_exchange_id=_infer_exchange_id,
                _infer_exchange_from_id=_infer_exchange_from_id,
                _calculate_limit_prices=_calculate_limit_prices,
                _ClosedTrade=_ClosedTrade, params=params)


def _check_positions_phase2_close_check(bt, bar, params, prev_bar):
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _resolve_time_stop, _determine_close_decision, _execute_position_close,
    )
    symbol = bar.get("symbol", "")
    price = bar.get("close", 0.0)
    bar_time = bar.get("minute", pd.NaT)
    if pd.isna(bar_time):
        logger.warning("[R22-TIME-03] bar缺少minute字段，跳过持仓检查")
        return
    if symbol not in bt.positions or price <= 0:
        return

    pos = bt.positions[symbol]
    open_reason = getattr(pos, 'open_reason', 'CORRECT_RESONANCE')
    hard_stop_min = _resolve_time_stop(params, open_reason, bt.current_state)
    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0

    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct
    if float_pnl_pct > pos.max_float_profit:
        pos.max_float_profit = float_pnl_pct

    should_close, close_reason, price = _determine_close_decision(
        pos, price, bar_time, bar, prev_bar, params, hold_minutes, hard_stop_min)

    if should_close:
        _execute_position_close(bt, pos, symbol, price, close_reason, bar, bar_time, params, float_pnl_pct)


def _run_backtest_setup(params, bar_data, train, strategy_type):
    from ali2026v3_trading.参数池.backtest_runner_base import (
        _check_bar_data_monotonic, _apply_fidelity_presets,
        _BacktestState, _sync_random_seed, _get_life_estimator,
    )
    import uuid as _uuid_mod
    _trial_isolation_flag = _uuid_mod.uuid4().hex[:12]
    try:
        import threading as _threading_mod
        _threading_mod.current_thread().trial_isolation_flag = _trial_isolation_flag
    except Exception:
        pass
    if bar_data.empty:
        return {"error": "无数据", "params": params}, None

    try:
        from ali2026v3_trading.strategy_core_service import StrategyCoreService
        _init_pending = getattr(StrategyCoreService, '_init_pending', False)
        if _init_pending:
            import time as _wait_time
            _wait_deadline = _wait_time.time() + 10.0
            while getattr(StrategyCoreService, '_init_pending', False) and _wait_time.time() < _wait_deadline:
                _wait_time.sleep(0.05)
            if getattr(StrategyCoreService, '_init_pending', False):
                logging.warning("[R23-IN-P1-04] 策略onInit未在10s内完成，回测继续(降级)")
    except Exception as _p1_04_e:
        logging.debug("[R23-IN-P1-04] 策略初始化等待守卫跳过: %s", _p1_04_e)

    _check_bar_data_monotonic(bar_data)
    _apply_fidelity_presets(params)
    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)

    if params.get("shadow_param_violation", False):
        bt.new_open_blocked = True
        logger.warning("[P1-R9-11] 影子参数独立性违反，回测新开仓已阻断")

    try:
        from ali2026v3_trading.state_param_manager import get_state_param_manager
        _spm = get_state_param_manager()
        _spm_params = _spm.get_params(strategy_type)
        if _spm_params and isinstance(_spm_params, dict):
            params = {**params, **_spm_params}
    except Exception as _d07_e:
        logger.debug("[R3-D-07] StateParamManager加载失败(使用默认params): %s", _d07_e)

    if "position_scale" not in params:
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _cp = get_cached_params()
            _ps_key = f"{strategy_type}_position_scale"
            params["position_scale"] = float(_cp.get(_ps_key, _cp.get("position_scale", 1.0)))
        except Exception:
            params.setdefault("position_scale", 1.0)

    estimator = _get_life_estimator()
    if estimator is not None and (not hasattr(estimator, '_life_dict') or not estimator._life_dict):
        try:
            estimator.build_life_dict(bar_data)
        except Exception as _e:
            logger.error("[R3-P-09] build_life_dict失败(严重): %s", _e)
            bt.get("diagnostics", {}).setdefault("degraded_features", []).append("life_dict")

    return params, bt


def exclude_rollover_signals(bar_data: pd.DataFrame,
                              rollover_points: List[Dict[str, Any]],
                              skip_days: int = 3,
                              date_col: str = "minute") -> pd.DataFrame:
    """P0-裂缝5：在展期日前后各skip_days天剔除所有策略信号

    在bar_data中添加 'rollover_excluded' 列，展期窗口内的bar标记为True。
    """
    if not rollover_points or date_col not in bar_data.columns:
        bar_data = bar_data.copy()
        bar_data["rollover_excluded"] = False
        return bar_data

    bar_data = bar_data.copy()
    bar_data["rollover_excluded"] = False

    bar_dates = bar_data[date_col].values
    for rp in rollover_points:
        idx = rp["index"]
        if idx < len(bar_dates):
            rollover_date = bar_dates[idx]
            try:
                if hasattr(rollover_date, 'date'):
                    rd = rollover_date.date()
                else:
                    rd = rollover_date
                from datetime import timedelta
                for offset in range(-skip_days, skip_days + 1):
                    target = rd + timedelta(days=offset)
                    mask = bar_data[date_col].apply(
                        lambda x, t=target: hasattr(x, 'date') and x.date() == t
                    )
                    bar_data.loc[mask, "rollover_excluded"] = True
            except Exception:
                start = max(0, idx - skip_days * 240)
                end = min(len(bar_data), idx + skip_days * 240)
                bar_data.loc[bar_data.index[start:end], "rollover_excluded"] = True

    n_excluded = bar_data["rollover_excluded"].sum()
    if n_excluded > 0:
        logger.info("[裂缝5-展期剔除] 标记 %d/%d bar为展期排除区", n_excluded, len(bar_data))

    return bar_data


def _build_risk_dimension_defaults():
    try:
        from ali2026v3_trading.risk_service import RiskService
        _src = RiskService.RISK_DIMENSION_DEFAULTS
        _key_map = {
            'state_strength': 'decision.score.state_strength_weight',
            'order_flow': 'decision.score.order_flow_weight',
            'cycle_resonance': 'decision.score.cycle_resonance_weight',
            'tri_validation': 'decision.score.tri_validation_weight',
            'life_expectancy': 'decision.score.life_expectancy_weight',
            'phase_quality': 'decision.score.phase_quality_weight',
            'greeks_usage': 'decision.score.greeks_usage_weight',
            'asymmetric_drawdown': 'decision.score.asymmetric_drawdown_weight',
            'consecutive_loss': 'decision.score.consecutive_loss_weight',
            'alpha_decay': 'decision.score.alpha_decay_weight',
            'cross_correlation': 'decision.score.cross_correlation_weight',
        }
        return {_key_map.get(k, k): v for k, v in _src.items() if k in _key_map}
    except Exception:
        return {
            "decision.score.state_strength_weight": 0.15,
            "decision.score.order_flow_weight": 0.10,
            "decision.score.cycle_resonance_weight": 0.10,
            "decision.score.tri_validation_weight": 0.10,
            "decision.score.life_expectancy_weight": 0.10,
            "decision.score.phase_quality_weight": 0.08,
            "decision.score.greeks_usage_weight": 0.07,
            "decision.score.asymmetric_drawdown_weight": 0.05,
            "decision.score.consecutive_loss_weight": 0.07,
            "decision.score.alpha_decay_weight": 0.10,
            "decision.score.cross_correlation_weight": 0.08,
        }


def validate_shadow_param_independence(threshold: float = 0.20) -> Dict[str, float]:
    """P0-Q1质量门：验证影子策略参数与主策略差异度>threshold

    对每个策略组，计算影子A/B与主策略的关键参数差异度。
    差异度 = avg(|shadow_param - main_param| / main_param)
    """
    from ali2026v3_trading.param_pool.backtest_param_grids import (
        PARAM_DEFAULTS, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B,
        PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B,
        PARAM_DEFAULTS_BOX_EXTREME, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B,
        PARAM_DEFAULTS_BOX_SPRING, PARAM_DEFAULTS_BOX_SPRING_SHADOW_A, PARAM_DEFAULTS_BOX_SPRING_SHADOW_B,
        PARAM_DEFAULTS_ARBITRAGE, PARAM_DEFAULTS_ARBITRAGE_SHADOW_A, PARAM_DEFAULTS_ARBITRAGE_SHADOW_B,
        PARAM_DEFAULTS_MARKET_MAKING, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B,
    )
    _SHADOW_DIFF_KEYS = [
        "close_take_profit_ratio", "close_stop_loss_ratio",
        "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
        "resonance_hard_time_stop_min", "box_hard_time_stop_min",
        "max_risk_ratio",
    ]
    results = {}
    for group_name, main_params, shadow_a, shadow_b in [
        ("S2_main", PARAM_DEFAULTS, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B),
        ("S1_hft", PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B),
        ("S3_box_extreme", PARAM_DEFAULTS_BOX_EXTREME,
         PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B),
        ("S4_box_spring", PARAM_DEFAULTS_BOX_SPRING,
         PARAM_DEFAULTS_BOX_SPRING_SHADOW_A, PARAM_DEFAULTS_BOX_SPRING_SHADOW_B),
        # P1-裂缝37：补充S5/S6影子参数差异度验证
        ("S5_arbitrage", PARAM_DEFAULTS_ARBITRAGE,
         PARAM_DEFAULTS_ARBITRAGE_SHADOW_A, PARAM_DEFAULTS_ARBITRAGE_SHADOW_B),
        ("S6_market_making", PARAM_DEFAULTS_MARKET_MAKING,
         PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B),
    ]:
        for shadow_name, shadow_params in [("shadow_a", shadow_a), ("shadow_b", shadow_b)]:
            diffs = []
            for key in _SHADOW_DIFF_KEYS:
                if key in main_params and key in shadow_params and main_params[key] != 0:
                    diffs.append(abs(shadow_params[key] - main_params[key]) / abs(main_params[key]))
            avg_diff = sum(diffs) / max(1, len(diffs))
            label = f"{group_name}.{shadow_name}"
            results[label] = round(avg_diff, 4)
            if avg_diff < threshold:
                logger.warning("[P0-Q1 FAIL] %s 参数差异度 %.2f%% < %.0f%% 阈值", label, avg_diff * 100, threshold * 100)
            else:
                logger.info("[P0-Q1 PASS] %s 参数差异度 %.2f%%", label, avg_diff * 100)
    return results


def _resolve_time_stop_hard(params: Dict[str, float], open_reason: str) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟）

    策略分层时间参数：
    - HFT: hft_hard_time_stop_ms → 转换为分钟
    - 弹簧: spring_hard_time_stop_sec → 转换为分钟
    - 共振: resonance_hard_time_stop_min → 直接使用
    - 箱体: box_hard_time_stop_min → 直接使用
    """
    from ali2026v3_trading.参数池.backtest_runner_base import _REASON_TIME_STOP_SOURCE, REASON_MULTIPLIERS
    reason_key = str(open_reason or "").upper()
    param_key, unit, fallback = _REASON_TIME_STOP_SOURCE.get(
        reason_key, ("resonance_hard_time_stop_min", "min", 5.0)
    )
    raw_val = float(params.get(param_key, fallback) or fallback)
    if unit == "ms":
        base_time = raw_val / 60000.0
    elif unit == "sec":
        base_time = raw_val / 60.0
    else:
        base_time = raw_val
    mult = REASON_MULTIPLIERS.get(open_reason, {"time_mult": 1.0})
    return base_time * mult["time_mult"]


def _resolve_time_stop(params: Dict[str, float], open_reason: str, current_state: str = None) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟），支持行情寿命字典动态调整

    # R16-P1-002修复: _resolve_time_stop已升级为分层参数模型
    # 手册版本仅支持单一max_hold_minutes，代码版本支持stage1/stage2分层+策略类型覆盖

    策略分层时间参数：
    - HFT: hft_hard_time_stop_ms → 转换为分钟
    - 弹簧: spring_hard_time_stop_sec → 转换为分钟
    - 共振: resonance_hard_time_stop_min → 直接使用
    - 箱体: box_hard_time_stop_min → 直接使用
    """
    from ali2026v3_trading.参数池.backtest_runner_base import _resolve_time_stop_hard, _get_life_estimator, REASON_MULTIPLIERS
    # 行情寿命字典：用HMM状态寿命的p75分位数作为动态时间止损上限
    if current_state is not None:
        estimator = _get_life_estimator()
        if estimator is not None and hasattr(estimator, '_life_dict') and estimator._life_dict:
            life = estimator.get_life_expectancy(current_state)
            if life is not None and life.is_valid():
                # 寿命p75（分钟）作为该状态下的最大持仓时间
                life_stop = life.duration.get('p75', 0)
                if life_stop > 0:
                    # 取硬编码时间止损和寿命p75的较小值
                    hard_stop = _resolve_time_stop_hard(params, open_reason)
                    return max(1.0, min(hard_stop, life_stop))
    # P2-裂缝32：effective_wait最小值约束
    # pullback_atr_wait_multiplier与pullback_theta_decay_accel可同时生效
    # 但可能出现负的effective_wait（例如近月高波动），强制最小值=1
    hard_stop = _resolve_time_stop_hard(params, open_reason)
    return max(1.0, hard_stop)


def _is_consecutive_loss_paused(bt, params: Dict[str, float], bar_time: pd.Timestamp) -> bool:
    """连亏暂停：达到阈值后在暂停窗口内阻断新开仓。"""
    max_losses = int(params.get("max_consecutive_losses", 0) or 0)
    if max_losses <= 0:
        return False

    pause_until = getattr(bt, 'consecutive_loss_pause_until', None)
    if pause_until is not None:
        if bar_time < pause_until:
            return True
        bt.consecutive_loss_pause_until = None

    streak = 0
    for pnl in reversed(bt.recent_pnls):
        if pnl < 0:
            streak += 1
        else:
            break
    bt.consecutive_loss_streak = streak

    if streak >= max_losses:
        pause_sec = float(params.get("consecutive_loss_pause_sec", 1800.0) or 1800.0)
        bt.consecutive_loss_pause_until = bar_time + pd.Timedelta(seconds=max(1.0, pause_sec))
        logger.warning(
            "[BACKTEST] 连亏暂停触发: streak=%d threshold=%d pause_until=%s",
            streak, max_losses, bt.consecutive_loss_pause_until,
        )
        return True
    return False


def _get_expiry_slippage_multiplier(bar: pd.Series, params: Dict[str, float]) -> float:
    """P0-6修复: 根据距到期日天数返回滑点倍增系数

    到期日附近流动性急剧下降，bid-ask spread暴增，
    回测必须建模此效应，否则到期附近交易回测盈利实盘巨亏。
    """
    import math
    from ali2026v3_trading.参数池.backtest_runner_base import EXPIRY_SLIPPAGE_MULTIPLIERS
    days_to_expiry = bar.get("days_to_expiry", None)
    # R3-P-03修复: 处理NaN值 — np.nan != None，需显式检查
    if days_to_expiry is None or (isinstance(days_to_expiry, float) and math.isnan(days_to_expiry)):
        # R3-P-03修复: 字段名从expiry_date改为expire_date，与数据库schema对齐
        expiry_str = bar.get("expire_date", "")
        if not expiry_str:
            expiry_str = bar.get("expiry_date", "")
        if expiry_str:
            try:
                from datetime import datetime
                expiry_dt = pd.Timestamp(expiry_str)
                bar_time = bar.get("timestamp", pd.NaT)
                if pd.isna(bar_time):
                    logger.warning("[R22-TIME-03] bar缺少timestamp字段，跳过到期时间计算")
                    return 1.0
                days_to_expiry = (expiry_dt - pd.Timestamp(bar_time)).days
            except Exception:
                return 1.0
    if days_to_expiry is None or (isinstance(days_to_expiry, float) and math.isnan(days_to_expiry)):
        return 1.0
    # P0-6修复: numpy.int64不是int的子类，需用np.issubdtype或try/except
    try:
        dte = int(days_to_expiry)
    except (TypeError, ValueError):
        return 1.0
    if dte < 0:
        return 1.0
    for threshold_days, multiplier in sorted(EXPIRY_SLIPPAGE_MULTIPLIERS.items()):
        if dte <= threshold_days:
            return multiplier
    return 1.0


def _compute_cascade_slippage_bps(
    base_slippage_bps: float,
    close_volume: float = 0.0,
    avg_volume: float = 1.0,
    is_state_switch: bool = False,
) -> float:
    import math
    from ali2026v3_trading.参数池.backtest_runner_base import CASCADE_SLIPPAGE_MULTIPLIER, CASCADE_SLIPPAGE_CAP_BPS
    if not is_state_switch:
        return base_slippage_bps
    participation_rate = close_volume / avg_volume if avg_volume > 0 else 0.0
    volume_impact = 1.0 + math.sqrt(max(participation_rate, 0.0))
    result = base_slippage_bps * volume_impact * CASCADE_SLIPPAGE_MULTIPLIER
    return min(result, CASCADE_SLIPPAGE_CAP_BPS)


def _check_backtest_health(bt, params: Dict[str, float], bar_time) -> bool:
    """P0-R11-06修复: 回测健康检查机制

    模拟实盘get_health_status()的CRITICAL暂停逻辑：
    - 当回测权益低于初始权益的50%时，标记为CRITICAL，暂停新开仓
    - 当连续亏损次数超过阈值时，标记为CRITICAL
    - 与实盘strategy_ecosystem.get_health_status()对齐
    """
    # 权益生存检查：权益低于初始权益50%视为CRITICAL
    if hasattr(bt, 'initial_equity') and bt.initial_equity > 0:
        _health_equity = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
        equity_ratio = _health_equity / bt.initial_equity
        if equity_ratio < 0.50:
            return False  # CRITICAL: 暂停新开仓

    # 连续亏损检查：连续亏损超过max_consecutive_losses视为CRITICAL
    max_consecutive = int(params.get("max_consecutive_losses", 5))
    if hasattr(bt, 'consecutive_loss_streak') and bt.consecutive_loss_streak >= max_consecutive:
        return False  # CRITICAL: 暂停新开仓

    return True  # HEALTHY: 允许新开仓