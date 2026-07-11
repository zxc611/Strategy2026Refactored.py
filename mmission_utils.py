"""
infra/commission_utils.py — 交易费用+共享交易常量 合并模块

合并自:
  - commission_utils.py (交易费用计算)
  - shared_trading_constants.py (共享交易常量 R27-CP-01-FIX)
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from ali2026v3_trading.infra._helpers import get_logger
from ali2026v3_trading.infra.shared_utils import is_same_day

logger = get_logger(__name__)


# ============================================================
# Section 1: 交易费用计算 (原 commission_utils.py)
# ============================================================

FEE_STRUCTURE = {
    "50ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "300ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "IO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "MO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "COMMODITY_OPTION": {"open": 5.0, "close_today": 5.0, "close_overnight": 5.0, "multiplier": 1000},
    "COMMODITY_FUTURE": {"open": 10.0, "close_today": 10.0, "close_overnight": 10.0, "multiplier": 10},
    "DEFAULT": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "multiplier": 10000},
}

FEE_STRUCTURE_V2 = {
    "SSE": {
        "50ETF_OPTION": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
        "300ETF_OPTION": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
        "DEFAULT": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
    },
    "CFFEX": {
        "IO_INDEX_OPTION": {"maker_open": 10.0, "maker_close_today": 10.0, "maker_close_overnight": 10.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
        "MO_INDEX_OPTION": {"maker_open": 10.0, "maker_close_today": 10.0, "maker_close_overnight": 10.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
        "DEFAULT": {"maker_open": 12.0, "maker_close_today": 12.0, "maker_close_overnight": 12.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
    },
    "DCE": {
        "COMMODITY_OPTION": {"maker_open": 3.0, "maker_close_today": 3.0, "maker_close_overnight": 3.0, "taker_open": 5.0, "taker_close_today": 5.0, "taker_close_overnight": 5.0, "unit": "per_lot"},
        "COMMODITY_FUTURE": {"maker_open": 8.0, "maker_close_today": 8.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 10.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
        "DEFAULT": {"maker_open": 8.0, "maker_close_today": 8.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 10.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
    },
    "SHFE": {
        "DEFAULT": {"maker_open": 8.0, "maker_close_today": 0.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 0.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
    },
    "CZCE": {
        "DEFAULT": {"maker_open": 4.0, "maker_close_today": 4.0, "maker_close_overnight": 4.0, "taker_open": 6.0, "taker_close_today": 6.0, "taker_close_overnight": 6.0, "unit": "per_lot"},
    },
    "INE": {
        "DEFAULT": {"maker_open": 8.0, "maker_close_today": 0.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 0.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
    },
    "SZSE": {
        "DEFAULT": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
    },
    "DEFAULT": {
        "DEFAULT": {"maker_open": 12.0, "maker_close_today": 12.0, "maker_close_overnight": 12.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
    },
}

EXCHANGE_COMMISSION_RATES = {}

DEFAULT_COMMISSION_RATE = 0.00003


def _infer_contract_type(symbol: str) -> str:
    s = str(symbol).upper()
    if "50ETF" in s:
        return "50ETF_OPTION"
    elif "300ETF" in s:
        return "300ETF_OPTION"
    elif s.startswith("IO") or "沪深300" in s:
        return "IO_INDEX_OPTION"
    elif s.startswith("MO") or "中证1000" in s:
        return "MO_INDEX_OPTION"
    elif any(k in s for k in ("M", "Y", "A", "C", "SR", "CF", "TA", "RU", "CU", "AL", "ZN", "AU", "AG")):
        if len(s) > 4 and any(c.isdigit() for c in s):
            return "COMMODITY_OPTION"
    return "DEFAULT"


def _infer_exchange_id(symbol: str) -> str:
    if not symbol:
        return None
    symbol_upper = symbol.upper()
    if any(p in symbol_upper for p in ['SHFE', 'AU', 'AG', 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'RB', 'WR', 'HC', 'SS', 'BU', 'RU', 'SP', 'FU', 'SC', 'NR', 'BC', 'LU']):
        return 'SHFE'
    elif any(p in symbol_upper for p in ['DCE', 'C', 'CS', 'A', 'B', 'M', 'Y', 'P', 'FB', 'BB', 'JD', 'RR', 'LH']):
        return 'DCE'
    elif any(p in symbol_upper for p in ['CZCE', 'CF', 'SR', 'TA', 'MA', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SF', 'SM', 'WH', 'PM', 'RI', 'ER', 'AP', 'CJ', 'PK', 'PF', 'SH', 'SA']):
        return 'CZCE'
    elif any(p in symbol_upper for p in ['IF', 'IH', 'IC', 'IM', 'TF', 'T', 'TS', 'TL']):
        return 'CFFEX'
    elif any(p in symbol_upper for p in ['INE', 'SC', 'LU', 'NR', 'BC']):
        return 'INE'
    return None


def _infer_exchange_from_id(exchange_id: str) -> str:
    _map = {"SSE": "SSE", "SZSE": "SZSE", "CFFEX": "CFFEX", "DCE": "DCE", "SHFE": "SHFE", "CZCE": "CZCE", "INE": "INE"}
    return _map.get(exchange_id, "DEFAULT")


def _infer_instrument_type(symbol: str) -> str:
    if "-C-" in str(symbol) or "-P-" in str(symbol):
        return "option_buyer"
    return "future"


def compute_commission(symbol: str, lots: int, open_time=None, close_time=None,
                       trade_value: float = 0.0, is_open: bool = True,
                       exchange_id: str = None, order_type: str = "taker",
                       exchange: str = None) -> float:
    _t0 = time.monotonic()
    try:
        result = _compute_commission_impl(symbol, lots, open_time, close_time,
                                          trade_value, is_open, exchange_id, order_type, exchange)
        return result
    finally:
        _elapsed_us = (time.monotonic() - _t0) * 1_000_000
        with _commission_perf_lock:
            _commission_perf['count'] += 1
            _commission_perf['total_us'] += _elapsed_us
            _commission_perf['max_us'] = max(_commission_perf['max_us'], _elapsed_us)
            if _commission_perf['count'] % 1000 == 0:
                avg_us = _commission_perf['total_us'] / _commission_perf['count']
                logger.info("[Observability] compute_commission每千次统计: count=%d avg=%.1fus max=%.1fus",
                            _commission_perf['count'], avg_us, _commission_perf['max_us'])


_commission_perf: Dict[str, float] = {'count': 0, 'total_us': 0.0, 'max_us': 0.0}
_commission_perf_lock = threading.Lock()


def _compute_commission_impl(symbol, lots, open_time, close_time,
                              trade_value, is_open, exchange_id, order_type, exchange):
    contract_type = _infer_contract_type(symbol)
    if exchange is not None:
        exchange_fees = FEE_STRUCTURE_V2.get(exchange, FEE_STRUCTURE_V2.get("DEFAULT", {}))
        contract_fee = exchange_fees.get(contract_type, exchange_fees.get("DEFAULT", None))
        if contract_fee is not None:
            is_close_today = False
            if not is_open and open_time is not None and close_time is not None:
                try:
                    is_close_today = is_same_day(open_time, close_time)
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] commission_utils date comparison suppressed: %s", _r3_err)
                    pass
            if is_open:
                fee_key = f"{order_type}_open"
            elif is_close_today:
                fee_key = f"{order_type}_close_today"
            else:
                fee_key = f"{order_type}_close_overnight"
            rate = contract_fee.get(fee_key, contract_fee.get("taker_open", 15.0))
            return rate * lots
    fee_info = FEE_STRUCTURE.get(contract_type, FEE_STRUCTURE["DEFAULT"])
    if fee_info["unit"] == "per_lot":
        if is_open:
            base_fee = fee_info["open"] * lots
        else:
            if open_time and close_time:
                hold_hours = (close_time - open_time).total_seconds() / 3600.0
                if hold_hours < 4:
                    base_fee = fee_info["close_today"] * lots
                else:
                    base_fee = fee_info["close_overnight"] * lots
            else:
                base_fee = fee_info["close_overnight"] * lots
    else:
        base_fee = max(fee_info["min"], trade_value * fee_info["rate"])
    if exchange_id is not None:
        exchange_rate = EXCHANGE_COMMISSION_RATES.get(exchange_id)
        if exchange_rate is not None:
            multiplier = exchange_rate.get("multiplier", 1.0)
            if multiplier != 1.0:
                base_fee = base_fee * multiplier
    return base_fee


def calc_trade_fee(contract_type: str, open_time, close_time,
                   lots: int, trade_value: float = 0.0, broker_policy: str = "default") -> float:
    fee = FEE_STRUCTURE.get(contract_type, FEE_STRUCTURE["DEFAULT"])
    if fee["unit"] == "per_lot":
        open_fee = fee["open"] * lots
        hold_hours = (close_time - open_time).total_seconds() / 3600.0 if close_time and open_time else 999
        if broker_policy == "pingjin_free" and hold_hours < 4:
            if open_time and close_time:
                if is_same_day(open_time, close_time):
                    close_fee = fee["close_today"] * lots
                else:
                    close_fee = fee["close_overnight"] * lots
            else:
                close_fee = fee["close_today"] * lots
        else:
            close_fee = fee["close_overnight"] * lots
        return open_fee + close_fee
    else:
        return max(fee["min"], trade_value * fee["rate"])


def estimate_commission_simple(price: float, quantity: int, rate: float = DEFAULT_COMMISSION_RATE) -> float:
    return abs(price * quantity * rate)


def get_commission_per_lot(symbol: str, exchange: str = None) -> float:
    contract_type = _infer_contract_type(symbol)
    fee_info = FEE_STRUCTURE.get(contract_type, FEE_STRUCTURE["DEFAULT"])
    if fee_info["unit"] == "per_lot":
        return fee_info.get("open", 15.0)
    return 15.0


# ============================================================
# Section 2: 共享交易常量 (原 shared_trading_constants.py)
# ============================================================

CONTRACT_MULTIPLIER_MAP = {
    'IF': 300.0,
    'IC': 200.0,
    'IH': 300.0,
    'TS': 200.0,
    'TF': 10000.0,
    'T': 10000.0,
    'AU': 1000.0,
    'AG': 15.0,
    'CU': 5.0,
    'RB': 10.0,
}

OPTION_TO_FUTURE_MAP = {'MO': 'IM', 'IO': 'IF', 'HO': 'IH'}

REASON_MULTIPLIERS = {
    "CORRECT_RESONANCE":    {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
    "HIGH_FREQ":            {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
    "DIVERGENCE_REVERSAL":  {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 0.67},  # FIX-20260711-P2
    "BOX_SPRING":           {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 0.8},  # FIX-20260711-P2
    "OTHER_SCALP":          {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 0.33},  # FIX-20260711-P2
    "BOX_EXTREME":          {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 0.33},  # FIX-20260711-P2
    "ARBITRAGE":            {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 0.25},  # FIX-20260711-P2
    "MARKET_MAKING":        {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},  # FIX-20260711-P2
    "MANUAL":               {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
}

P0_IRON_RULES = {
    "max_oos_decay": -0.30,
    "oos_decay_warn": -0.20,
    "min_train_sharpe": 0.5,
    "min_test_sharpe": 0.3,
    "min_lr_threshold": 0.8,
    "lr_threshold_warn": 1.0,
    "max_drawdown_limit": -0.50,
    "min_signal_count": 30,
    "max_daily_trigger": 2.0,
    "max_loss_hit_rate": 0.20,
    "min_two_x_recovery_rate": 0.30,
    "min_oos_retention": 0.50,
    "alpha_threshold_hft": 0.5,
    "alpha_threshold_minute": 0.5,
    "alpha_threshold_box_extreme": 0.3,
    "alpha_threshold_box_spring": 0.4,
    "alpha_threshold_arbitrage": 0.3,
    "alpha_threshold_market_making": 0.2,
    "alpha_pct_threshold": 30,
}


def detect_rollover_gaps(bar_data: pd.DataFrame,
                          contract_col: str = "symbol",
                          price_col: str = "close",
                          gap_threshold: float = 0.02) -> List[Dict[str, Any]]:
    if bar_data.empty or contract_col not in bar_data.columns:
        return []

    rollover_points = []
    contract_series = bar_data[contract_col].values
    price_series = bar_data[price_col].values if price_col in bar_data.columns else None

    for i in range(1, len(contract_series)):
        if contract_series[i] != contract_series[i - 1]:
            point = {
                "index": i,
                "prev_contract": str(contract_series[i - 1]),
                "new_contract": str(contract_series[i]),
                "gap_pct": 0.0,
                "is_significant": False,
            }
            if price_series is not None and price_series[i - 1] > 0:
                gap = (price_series[i] - price_series[i - 1]) / price_series[i - 1]
                point["gap_pct"] = round(gap * 100, 2)
                point["is_significant"] = abs(gap) > gap_threshold
            rollover_points.append(point)

    return rollover_points


def compute_rollover_cost(rollover_points: List[Dict[str, Any]],
                           bar_data: pd.DataFrame,
                           params: Dict[str, float],
                           calendar_basis_bps: float = 5.0,
                           rollover_slippage_bps: float = 3.0) -> Dict[str, Any]:
    if not rollover_points:
        return {"total_rollover_cost_bps": 0.0, "rollover_count": 0}

    total_gap_bps = 0.0
    for rp in rollover_points:
        total_gap_bps += abs(rp.get("gap_pct", 0.0)) * 100

    n_rollovers = len(rollover_points)
    calendar_basis_total = calendar_basis_bps * n_rollovers
    slippage_total = rollover_slippage_bps * n_rollovers * 2

    total_cost_bps = total_gap_bps + calendar_basis_total + slippage_total

    logger.info(
        "[裂缝5-换月成本] gap=%.1fbps calendar_basis=%.1fbps slippage=%.1fbps total=%.1fbps (n=%d)",
        total_gap_bps, calendar_basis_total, slippage_total, total_cost_bps, n_rollovers,
    )

    return {
        "total_rollover_cost_bps": round(total_cost_bps, 2),
        "gap_cost_bps": round(total_gap_bps, 2),
        "calendar_basis_bps": round(calendar_basis_total, 2),
        "calendar_basis_per_rollover_bps": calendar_basis_bps,
        "slippage_bps": round(slippage_total, 2),
        "slippage_per_rollover_bps": rollover_slippage_bps,
        "rollover_count": n_rollovers,
        "annualized_cost_bps": round(total_cost_bps * 12 / max(1, n_rollovers), 2),
    }


__all__ = [
    "FEE_STRUCTURE", "FEE_STRUCTURE_V2", "EXCHANGE_COMMISSION_RATES",
    "DEFAULT_COMMISSION_RATE",
    "_infer_contract_type", "_infer_exchange_id", "_infer_exchange_from_id", "_infer_instrument_type",
    "compute_commission", "calc_trade_fee", "estimate_commission_simple", "get_commission_per_lot",
    "CONTRACT_MULTIPLIER_MAP", "OPTION_TO_FUTURE_MAP",
    "REASON_MULTIPLIERS", "P0_IRON_RULES",
    "detect_rollover_gaps", "compute_rollover_cost",
]
