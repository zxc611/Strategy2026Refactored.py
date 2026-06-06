from __future__ import annotations

import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ENABLE_QUEUE_SIMULATION = False
QUEUE_TIMEOUT_SECONDS = 300
MARKET_ORDER_SLIPPAGE_BPS = 5.0
MARKET_ORDER_PRICE_MODE = "weighted"
ENABLE_CANCEL_SIMULATION = True
CANCEL_DELAY_MS = 100
CANCEL_FAILURE_RATE = 0.05

INSTRUMENT_SLIPPAGE_MULTIPLIER = {
    "ETF": 1.0,
    "FUTURE": 1.2,
    "OPTION_ETF": 1.5,
    "OPTION_INDEX": 2.0,
    "OPTION_COMMODITY": 2.5,
}


def _simulate_limit_order_queue(
    order_price: float,
    current_price: float,
    bar: pd.Series,
    order_lots: int = 1,
    timeout_seconds: int = QUEUE_TIMEOUT_SECONDS,
    enable_queue: bool = ENABLE_QUEUE_SIMULATION
) -> Dict[str, Any]:
    if not enable_queue:
        return {"filled": True, "fill_price": order_price, "fill_lots": order_lots, "queue_time": 0, "queue_position": 0}
    bar_volume = bar.get("volume", 0) if bar is not None else 0
    bar_high = bar.get("high", current_price) if bar is not None else current_price
    bar_low = bar.get("low", current_price) if bar is not None else current_price
    _price_range = bar_high - bar_low if bar_high > bar_low else current_price * 0.001
    _price_offset = abs(order_price - current_price)
    if _price_range > 0:
        _queue_position = min(100, int(_price_offset / _price_range * 100))
    else:
        _queue_position = 50
    _max_participation_rate = 0.15
    if bar_volume > 0:
        _available_volume = int(bar_volume * _max_participation_rate)
        _fill_ratio = max(0.0, 1.0 - _queue_position / 100.0)
        _fill_lots = max(0, min(order_lots, int(_available_volume * _fill_ratio)))
    else:
        _fill_lots = order_lots
    _bar_duration_sec = 60
    _queue_time = int(_queue_position / 100.0 * _bar_duration_sec)
    if _queue_time > timeout_seconds:
        _fill_lots = 0
    _filled = _fill_lots > 0
    _fill_price = order_price if _filled else current_price
    if _filled and _fill_lots < order_lots:
        logger.debug("BF-P1-02: 限价单部分成交, 计划%d手, 实际%d手, 排队位置=%d", order_lots, _fill_lots, _queue_position)
    elif not _filled:
        logger.debug("BF-P1-02: 限价单未成交, 排队位置=%d, 超时=%ds", _queue_position, _queue_time)
    return {"filled": _filled, "fill_price": _fill_price, "fill_lots": _fill_lots, "queue_time": _queue_time, "queue_position": _queue_position}


def _simulate_market_order_slippage(
    bar: pd.Series,
    slippage_bps: float = MARKET_ORDER_SLIPPAGE_BPS,
    price_mode: str = MARKET_ORDER_PRICE_MODE,
    direction: int = 1
) -> float:
    _base_price = bar.get("close", 0.0)
    if price_mode == "close":
        _base_price = bar.get("close", _base_price)
    elif price_mode == "weighted":
        _base_price = (
            bar.get("open", 0.0) + bar.get("high", 0.0) +
            bar.get("low", 0.0) + bar.get("close", 0.0)
        ) / 4.0
    elif price_mode == "open":
        _base_price = bar.get("open", _base_price)
    elif price_mode == "high":
        _base_price = bar.get("high", _base_price)
    elif price_mode == "low":
        _base_price = bar.get("low", _base_price)
    elif price_mode == "random":
        _high = bar.get("high", _base_price)
        _low = bar.get("low", _base_price)
        _base_price = _low + (_high - _low) * np.random.random()
    _slippage = _base_price * slippage_bps / 10000.0
    _fill_price = _base_price + direction * _slippage
    logger.debug(f"BF-P1-03占位: 市价单滑点模拟，模式={price_mode}, 滑点={slippage_bps}bps")
    return _fill_price


def _simulate_order_cancel(
    order_id: str,
    cancel_delay_ms: int = CANCEL_DELAY_MS,
    failure_rate: float = CANCEL_FAILURE_RATE,
    enable_cancel: bool = ENABLE_CANCEL_SIMULATION
) -> Dict[str, Any]:
    if not enable_cancel:
        return {"success": True, "delay_ms": 0, "reason": "cancel_simulation_disabled"}
    _success = True
    _reason = "immediate_success"
    _delay = 0
    if np.random.random() < failure_rate:
        _success = False
        _reason = "market_already_filled"
        logger.warning(f"BF-P1-09占位: 撤单失败，订单{order_id}已成交")
    else:
        _delay = cancel_delay_ms
        _reason = "cancel_accepted"
        logger.debug(f"BF-P1-09占位: 撤单成功，订单{order_id}，延迟{_delay}ms")
    return {"success": _success, "delay_ms": _delay, "reason": _reason}