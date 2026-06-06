"""
回测安全检查器 — 从backtest_runner_base.py拆分
职责: 保证金追保、断路器暂停、日回撤硬停止
"""
from __future__ import annotations

import logging
from typing import Dict, Optional, Any

import numpy as np
import pandas as pd


def _force_close_position(bt, sym, pos, bar, bar_time, close_reason, *,
                          _get_contract_multiplier, _compute_dynamic_slippage_bps,
                          _compute_commission, _safe_equity_add, _infer_instrument_type,
                          _infer_exchange_id, _infer_exchange_from_id,
                          _calculate_limit_prices, _ClosedTrade, params):
    _limit_info = _calculate_limit_prices(
        bar,
        instrument_type=_infer_instrument_type(sym)
        .replace('option_buyer', 'OPTION_ETF')
        .replace('option_seller', 'OPTION_ETF')
        .replace('future', 'FUTURE')
    )
    _is_limit_up = _limit_info['is_limit_up']
    _is_limit_down = _limit_info['is_limit_down']
    _close_dir = 'SELL' if pos.volume > 0 else 'BUY'
    if _is_limit_up and _close_dir == 'SELL':
        return False
    if _is_limit_down and _close_dir == 'BUY':
        return False
    close_price = bar.get("close", pos.open_price)
    _mult = _get_contract_multiplier(sym)
    pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 0)
    slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
    slip = close_price * slip_bps / 10000 * pos.lots
    commission = _compute_commission(
        sym, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"),
        is_open=False, exchange_id=_infer_exchange_id(sym),
        exchange=_infer_exchange_from_id(_infer_exchange_id(sym))
    )
    net_pnl = pnl - slip - commission
    _safe_equity_add(bt, net_pnl)
    bt.total_trades += 1
    if np.isclose(close_price, pos.open_price, rtol=1e-10):
        float_pnl_pct = 0.0
    else:
        float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct
    hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
    bt.closed_trades.append(_ClosedTrade(
        pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason=close_reason,
        hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
        premium_pnl=0.0, delta_pnl=0.0,
        stage1_passed=getattr(pos, 'stage1_passed', False),
    ))
    bt.recent_pnls.append(net_pnl)
    if len(bt.recent_pnls) > 50:
        bt.recent_pnls = bt.recent_pnls[-50:]
    del bt.positions[sym]
    return True


def check_margin_call(bt, params, bar, *,
                      _get_contract_multiplier, _safe_equity_add):
    if bt.equity <= 0 or len(bt.positions) == 0:
        return False
    _risk_equity = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
    try:
        _total_margin = 0.0
        for _sym, _pos in bt.positions.items():
            _mult = _get_contract_multiplier(_sym)
            _margin_ratio = params.get('margin_ratio', 0.1)
            _total_margin += abs(_pos.volume) * _pos.open_price * _mult * _margin_ratio
        _margin_occupancy = _total_margin / _risk_equity
        if _margin_occupancy > 0.95 and bar is not None:
            logging.warning("[EX-08] 保证金占用率%.1f%%>95%%, 强制平仓", _margin_occupancy * 100)
            for _sym in list(bt.positions.keys()):
                _pos = bt.positions[_sym]
                _close_price = bar.get("close", _pos.open_price)
                _pnl = (_close_price - _pos.open_price) * _pos.volume * _get_contract_multiplier(_sym)
                _safe_equity_add(bt, _pnl)
                del bt.positions[_sym]
                bt.total_trades += 1
            return True
    except Exception:
        pass
    return False


def check_safety(bt, bar_time, params, allow_close=False, bar=None, **deps):
    _get_contract_multiplier = deps['_get_contract_multiplier']
    _compute_dynamic_slippage_bps = deps['_compute_dynamic_slippage_bps']
    _compute_commission = deps['_compute_commission']
    _safe_equity_add = deps['_safe_equity_add']
    _infer_instrument_type = deps['_infer_instrument_type']
    _infer_exchange_id = deps['_infer_exchange_id']
    _infer_exchange_from_id = deps['_infer_exchange_from_id']
    _calculate_limit_prices = deps['_calculate_limit_prices']
    _ClosedTrade = deps['_ClosedTrade']

    check_margin_call(bt, params, bar,
                      _get_contract_multiplier=_get_contract_multiplier,
                      _safe_equity_add=_safe_equity_add)

    is_circuit_breaker_paused = (
        bt.circuit_breaker_until is not None and bar_time < bt.circuit_breaker_until
    )
    if is_circuit_breaker_paused and not allow_close:
        if bar is not None and len(bt.positions) > 0:
            for sym in list(bt.positions.keys()):
                pos = bt.positions[sym]
                _force_close_position(
                    bt, sym, pos, bar, bar_time, "circuit_breaker_force_close",
                    _get_contract_multiplier=_get_contract_multiplier,
                    _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
                    _compute_commission=_compute_commission,
                    _safe_equity_add=_safe_equity_add,
                    _infer_instrument_type=_infer_instrument_type,
                    _infer_exchange_id=_infer_exchange_id,
                    _infer_exchange_from_id=_infer_exchange_from_id,
                    _calculate_limit_prices=_calculate_limit_prices,
                    _ClosedTrade=_ClosedTrade,
                    params=params,
                )
        return False

    hard_stop = params.get("daily_loss_hard_stop_pct", 0.05)
    _risk_equity_dd = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
    _risk_daily_start = bt.daily_start_equity
    if _risk_daily_start > 0:
        daily_drawdown = (_risk_daily_start - _risk_equity_dd) / _risk_daily_start
        if daily_drawdown >= hard_stop and not allow_close:
            if bar is not None and len(bt.positions) > 0:
                for sym in list(bt.positions.keys()):
                    pos = bt.positions[sym]
                    _force_close_position(
                        bt, sym, pos, bar, bar_time, "daily_drawdown_force_close",
                        _get_contract_multiplier=_get_contract_multiplier,
                        _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
                        _compute_commission=_compute_commission,
                        _safe_equity_add=_safe_equity_add,
                        _infer_instrument_type=_infer_instrument_type,
                        _infer_exchange_id=_infer_exchange_id,
                        _infer_exchange_from_id=_infer_exchange_from_id,
                        _calculate_limit_prices=_calculate_limit_prices,
                        _ClosedTrade=_ClosedTrade,
                        params=params,
                    )
            return False

    return True