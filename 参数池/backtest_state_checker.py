from __future__ import annotations

import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _check_state_transition(bt, bar: pd.Series, params: Dict[str, float],
                            _check_safety_fn, BACKTEST_THRESHOLDS: dict) -> str:
    for _field in ("correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct"):
        _val = bar.get(_field, None)
        if _field not in bar or pd.isna(_val):
            logger.warning("[P0-1] 五态标签字段 %s 缺失或NaN，默认为0", _field)
            if _field in bar.index:
                bar[_field] = 0.0

    non_other = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0) + bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
    try:
        from ali2026v3_trading.width_cache import get_width_strength_cache
        _wc = get_width_strength_cache()
        if hasattr(_wc, '_classify_status'):
            _ufid = int(bar.get("underlying_future_id", 0))
            _month = str(bar.get("month", ""))
            _opt_type = str(bar.get("option_type", ""))
            _current_price = float(bar.get("close", 0.0))
            _prev_price = float(bar.get("open", 0.0))
            candidate = _wc._classify_status(_ufid, _month, _opt_type, _current_price, _prev_price)
        else:
            raise ImportError("_classify_status not available")
    except Exception:
        threshold = params.get("non_other_ratio_threshold", 0.65)
        if non_other < threshold:
            candidate = "other"
        else:
            correct_rise = bar.get("correct_rise_pct", 0)
            correct_fall = bar.get("correct_fall_pct", 0)
            wrong_rise = bar.get("wrong_rise_pct", 0)
            wrong_fall = bar.get("wrong_fall_pct", 0)
            correct_total = correct_rise + correct_fall
            incorrect_total = wrong_rise + wrong_fall
            if correct_total >= incorrect_total:
                candidate = "correct_trending" if correct_rise >= correct_fall else "correct_trending_defensive"
            else:
                candidate = "incorrect_reversal" if wrong_rise >= wrong_fall else "incorrect_reversal_defensive"

    confirm_bars = int(params.get("state_confirm_bars", 5))
    bar_period = float(params.get("bar_period", 1.0))
    if bar_period <= 0.1:
        hft_state_confirm_seconds = float(params.get("hft_state_confirm_seconds", 5.0))
        confirm_bars = max(1, int(hft_state_confirm_seconds / (bar_period * 60.0)))

    if candidate == bt.current_state:
        bt.pending_state = None
        bt.state_confirm_count = 0
        return bt.current_state

    if bt.pending_state == candidate:
        bt.state_confirm_count += 1
    else:
        bt.pending_state = candidate
        bt.state_confirm_count = 1

    if bt.state_confirm_count >= confirm_bars:
        bt.current_state = candidate
        bt.pending_state = None
        bt.state_confirm_count = 0

    return bt.current_state


def _check_logic_reversal(
    bt, bar: pd.Series, params: Dict[str, float],
    _check_safety_fn, _get_contract_multiplier_fn,
    _compute_dynamic_slippage_bps_fn, _compute_commission_fn,
    _safe_equity_add_fn, _infer_exchange_id_fn, _infer_exchange_from_id_fn,
    _bt_capture_snapshot_fn, BACKTEST_THRESHOLDS: dict,
) -> bool:
    _bar_time_lr = bar.get("minute", pd.NaT)
    if not pd.isna(_bar_time_lr):
        if not _check_safety_fn(bt, _bar_time_lr, params, allow_close=True, bar=bar):
            return True
    for symbol, pos in list(bt.positions.items()):
        if pos.open_reason != "CORRECT_RESONANCE":
            continue
        correct_pct = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0)
        wrong_pct = bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
        reversal_threshold = params.get("logic_reversal_threshold", 1.5)
        if wrong_pct > correct_pct * reversal_threshold and wrong_pct > BACKTEST_THRESHOLDS["logic_reversal_min_wrong_pct"]:
            price = bar.get("close", 0.0)
            bar_time = bar.get("minute", pd.NaT)
            if pd.isna(bar_time):
                logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar")
                continue
            if price <= 0:
                continue
            if hasattr(bt, '_last_bar_time') and bt._last_bar_time is not None:
                if bar_time < bt._last_bar_time:
                    logger.error("[NP-P2-13] wrong_pct来源bar时间%s < 回测游标%s，未来数据污染，跳过本bar", bar_time, bt._last_bar_time)
                    continue
            _multiplier = _get_contract_multiplier_fn(symbol)
            signed_volume = pos.volume if pos.volume != 0 else 0
            pnl = (price - pos.open_price) * signed_volume * _multiplier
            _premium_pnl = 0.0
            _delta_pnl = 0.0
            if getattr(pos, 'instrument_type', 'future') in ('option_buyer', 'option_seller'):
                _open_premium = getattr(pos, 'option_premium', 0.0)
                _close_premium = bar.get('option_premium', _open_premium + (price - pos.open_price))
                _premium_pnl = (_close_premium - _open_premium) * pos.lots
                _delta_pnl = pnl - _premium_pnl
            bid_ask = bar.get("bid_ask_spread", 0.0)
            spread_q = bar.get("_spread_quality", 0)
            slip_bps = _compute_dynamic_slippage_bps_fn(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
            slip = price * slip_bps / 10000 * pos.lots
            commission = _compute_commission_fn(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id_fn(symbol), exchange=_infer_exchange_from_id_fn(_infer_exchange_id_fn(symbol)))
            _safe_equity_add_fn(bt, pnl - slip - commission)
            bt.total_trades += 1
            _bt_capture_snapshot_fn(bt, "close", f"逻辑反转平仓 {symbol}", "", bar)
            bt.recent_pnls.append(pnl - slip - commission)
            if len(bt.recent_pnls) > 50:
                bt.recent_pnls = bt.recent_pnls[-50:]
            del bt.positions[symbol]
            logger.debug("逻辑反转平仓: %s @ %.2f (wrong=%.2f > correct*%.1f=%.2f)", symbol, price, wrong_pct, reversal_threshold, correct_pct * reversal_threshold)
    return True