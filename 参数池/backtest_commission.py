from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

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

COMMISSION_PER_LOT = 1.5

LIMIT_UP_RATIO = {
    "ETF": 0.10,
    "FUTURE": 0.10,
    "OPTION_ETF": 0.10,
    "OPTION_INDEX": 0.10,
    "OPTION_COMMODITY": 0.08,
}
LIMIT_DOWN_RATIO = {
    "ETF": 0.10,
    "FUTURE": 0.10,
    "OPTION_ETF": 0.10,
    "OPTION_INDEX": 0.10,
    "OPTION_COMMODITY": 0.08,
}


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
    logging.getLogger(__name__).warning("[EX-P0-06] 未识别合约类型, 使用DEFAULT费率: %s", symbol)
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


def _compute_commission(symbol: str, lots: int, open_time=None, close_time=None,
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
                    is_close_today = (open_time.date() == close_time.date()) if hasattr(open_time, 'date') else (open_time.strftime('%Y-%m-%d') == close_time.strftime('%Y-%m-%d'))
                except Exception:
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
        if is_open:
            base_fee = max(fee_info["min"], trade_value * fee_info["rate"])
        else:
            base_fee = max(fee_info["min"], trade_value * fee_info["rate"])
    if exchange_id is not None:
        exchange_rate = EXCHANGE_COMMISSION_RATES.get(exchange_id)
        if exchange_rate is not None:
            multiplier = exchange_rate.get("multiplier", 1.0)
            if multiplier != 1.0:
                base_fee = base_fee * multiplier
    return base_fee


def calc_trade_fee(contract_type: str, open_time: pd.Timestamp, close_time: pd.Timestamp,
                   lots: int, trade_value: float = 0.0, broker_policy: str = "default") -> float:
    fee = FEE_STRUCTURE.get(contract_type, FEE_STRUCTURE["DEFAULT"])
    if fee["unit"] == "per_lot":
        open_fee = fee["open"] * lots
        hold_hours = (close_time - open_time).total_seconds() / 3600.0 if close_time and open_time else 999
        if broker_policy == "pingjin_free" and hold_hours < 4:
            if open_time and close_time:
                is_same_day = (open_time.date() == close_time.date()) if hasattr(open_time, 'date') else (open_time.strftime('%Y-%m-%d') == close_time.strftime('%Y-%m-%d'))
                if is_same_day:
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


def _calculate_limit_prices(
    bar: pd.Series,
    prev_close: float = 0.0,
    instrument_type: str = "FUTURE",
) -> Dict[str, Any]:
    if bar.get('is_limit_up', None) is not None or bar.get('is_limit_down', None) is not None:
        return {
            "is_limit_up": bool(bar.get('is_limit_up', False)),
            "is_limit_down": bool(bar.get('is_limit_down', False)),
            "limit_up_price": 0.0,
            "limit_down_price": 0.0,
        }
    if prev_close <= 0:
        prev_close = bar.get('prev_close', 0.0)
    if prev_close <= 0:
        return {"is_limit_up": False, "is_limit_down": False, "limit_up_price": 0.0, "limit_down_price": 0.0}
    _up_ratio = LIMIT_UP_RATIO.get(instrument_type, 0.10)
    _down_ratio = LIMIT_DOWN_RATIO.get(instrument_type, 0.10)
    _limit_up_price = round(prev_close * (1 + _up_ratio), 2)
    _limit_down_price = round(prev_close * (1 - _down_ratio), 2)
    _close = bar.get('close', 0.0)
    _high = bar.get('high', 0.0)
    _low = bar.get('low', 0.0)
    _is_limit_up = (_close >= _limit_up_price * 0.999) or (_high >= _limit_up_price * 0.999 and _close >= _high * 0.999)
    _is_limit_down = (_close <= _limit_down_price * 1.001) or (_low <= _limit_down_price * 1.001 and _close <= _low * 1.001)
    return {
        "is_limit_up": _is_limit_up,
        "is_limit_down": _is_limit_down,
        "limit_up_price": _limit_up_price,
        "limit_down_price": _limit_down_price,
    }