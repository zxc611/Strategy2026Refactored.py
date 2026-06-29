from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.infra.shared_utils import is_same_day  # 五唯一性修复：统一同日比较逻辑

logger = get_logger(__name__)  # R9-5

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


__all__ = [
    "FEE_STRUCTURE", "FEE_STRUCTURE_V2", "EXCHANGE_COMMISSION_RATES",
    "DEFAULT_COMMISSION_RATE",
    "_infer_contract_type", "_infer_exchange_id", "_infer_exchange_from_id", "_infer_instrument_type",
    "compute_commission", "calc_trade_fee", "estimate_commission_simple", "get_commission_per_lot",
]