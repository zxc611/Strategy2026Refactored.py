# MODULE_ID: M1-153
"""回测保真度模拟函数 — 从backtest_runner_base.py提取

职责: 限价单排队、市价单滑点、品种滑点差异、撤单模拟、
     成交量计算、保真度预设、到期日滑点倍增

依赖: 仅 backtest_config (常量) + 标准库/第三方
不依赖: backtest_runner_base, backtest_runner_utils, backtest_state
"""
from __future__ import annotations

import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

from ali2026v3_trading.infra._helpers import get_logger
from ali2026v3_trading.param_pool.backtest.backtest_config import (
    QUEUE_TIMEOUT_SECONDS,
    ENABLE_QUEUE_SIMULATION,
    MARKET_ORDER_SLIPPAGE_BPS,
    MARKET_ORDER_PRICE_MODE,
    SLIPPAGE_BPS,
    INSTRUMENT_SLIPPAGE_MULTIPLIER,
    CANCEL_DELAY_MS,
    CANCEL_FAILURE_RATE,
    ENABLE_CANCEL_SIMULATION,
    EXPIRY_SLIPPAGE_MULTIPLIERS,
)

logger = get_logger(__name__)


def _simulate_limit_order_queue(
    order_price: float,
    current_price: float,
    bar: pd.Series,
    order_lots: int = 1,
    timeout_seconds: int = QUEUE_TIMEOUT_SECONDS,
    enable_queue: bool = ENABLE_QUEUE_SIMULATION
) -> Dict[str, Any]:
    """BF-P1-02已修复: 限价单排队模拟"""
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
    """BF-P1-03占位实现: 市价单滑点模拟"""
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


def _get_instrument_type_slippage(
    symbol: str,
    base_slippage_bps: float = SLIPPAGE_BPS,
    multiplier_dict: Dict[str, float] = None
) -> float:
    """BF-P1-07占位实现: 品种滑点差异化"""
    if multiplier_dict is None:
        multiplier_dict = INSTRUMENT_SLIPPAGE_MULTIPLIER

    _s = str(symbol).upper()
    _instrument_type = "ETF"

    if "50ETF" in _s or "300ETF" in _s:
        if "P" in _s or "C" in _s:
            _instrument_type = "OPTION_ETF"
        else:
            _instrument_type = "ETF"
    elif _s.startswith("IO") or _s.startswith("MO"):
        _instrument_type = "OPTION_INDEX"
    elif any(k in _s for k in ("M", "Y", "A", "C", "SR", "CF", "TA", "RU", "CU", "AL", "ZN", "AU", "AG")):
        if len(_s) > 4 and any(c.isdigit() for c in _s):
            _instrument_type = "OPTION_COMMODITY"
        else:
            _instrument_type = "FUTURE"

    _multiplier = multiplier_dict.get(_instrument_type, 1.0)
    _adjusted_slippage = base_slippage_bps * _multiplier

    logger.debug(f"BF-P1-07占位: 品种={_instrument_type}, 滑点乘数={_multiplier}, 调整后滑点={_adjusted_slippage}bps")
    return _adjusted_slippage


def _simulate_order_cancel(
    order_id: str,
    cancel_delay_ms: int = CANCEL_DELAY_MS,
    failure_rate: float = CANCEL_FAILURE_RATE,
    enable_cancel: bool = ENABLE_CANCEL_SIMULATION
) -> Dict[str, Any]:
    """BF-P1-09占位实现: 撤单模拟"""
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


def _compute_fill_quantity(
    order_lots: int,
    bar: pd.Series,
    params: Dict[str, Any],
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


def _apply_fidelity_presets(params: Dict[str, Any]) -> Dict[str, Any]:
    """BF-06: 保真度预设一键切换"""
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


def _get_expiry_slippage_multiplier(bar: pd.Series, params: Dict[str, float]) -> float:
    """P0-6修复: 根据距到期日天数返回滑点倍增系数

    到期日附近流动性急剧下降，bid-ask spread暴增，
    回测必须建模此效应，否则到期附近交易回测盈利实盘巨亏。
    """
    import math
    days_to_expiry = bar.get("days_to_expiry", None)
    if days_to_expiry is None or (isinstance(days_to_expiry, float) and math.isnan(days_to_expiry)):
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
            except (ValueError, KeyError, TypeError):
                return 1.0
    if days_to_expiry is None or (isinstance(days_to_expiry, float) and math.isnan(days_to_expiry)):
        return 1.0
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


def run_backtest_fidelity_analysis(
    strategy,
    params: Dict[str, Any],
    price_array: np.ndarray,
    signal_list: list,
) -> Dict[str, Any]:
    """运行回测保真度分析，评估策略在历史数据上的执行保真度"""
    if len(price_array) == 0 or len(signal_list) == 0:
        return {"fidelity_score": 0.0, "latency_impact": 0.0, "slippage_cost": 0.0, "fill_rate": 0.0}

    _latency_ms = params.get("execution_delay_ms", 0)
    _slippage_bps = params.get("slippage_bps", MARKET_ORDER_SLIPPAGE_BPS)
    _position_scale = params.get("position_scale", 1.0)

    _slippage_cost = 0.0
    _latency_impact = 0.0
    _filled = 0
    for i, signal in enumerate(signal_list):
        if i >= len(price_array) - 1:
            break
        _price = float(price_array[i])
        _direction = 1 if signal > 0 else (-1 if signal < 0 else 0)
        if _direction == 0:
            continue
        _slippage_cost += abs(_simulate_slippage(_price, _slippage_bps, _direction) - _price)
        _latency_impact += abs(_simulate_latency(_price, _latency_ms) - _price)
        _filled += 1

    _total = max(1, len(signal_list))
    _fill_rate = _filled / _total
    _fidelity = max(0.0, 1.0 - (_slippage_cost + _latency_impact) / max(1.0, np.mean(np.abs(price_array))))

    return {
        "fidelity_score": float(_fidelity),
        "latency_impact": float(_latency_impact),
        "slippage_cost": float(_slippage_cost),
        "fill_rate": float(_fill_rate),
        "position_scale": float(_position_scale),
    }


def _simulate_latency(base_price: float, latency_ms: float) -> float:
    """模拟延迟对成交价格的影响（延迟越大，价格偏离越大）"""
    if base_price <= 0 or latency_ms <= 0:
        return float(base_price)
    _volatility_per_ms = 1e-5
    _drift = base_price * _volatility_per_ms * latency_ms
    return float(base_price + _drift)


def _simulate_slippage(price: float, slippage_bps: float, direction: int) -> float:
    """模拟滑点对成交价格的影响"""
    if price <= 0 or slippage_bps <= 0:
        return float(price)
    _slippage = price * slippage_bps / 10000.0
    return float(price + direction * _slippage)