# [M1-102] 回测配置
#!/usr/bin/env python3
# MODULE_ID: M1-155
"""
回测配置常量模块 — 从backtest_runner_base.py拆分(P1-5)

包含：全局常量、FEE_STRUCTURE、保真度配置参数、滑点/手续费配置
"""
from __future__ import annotations

import os
import logging
from infra._helpers import get_logger  # R9-5
from typing import Dict, List, Optional
from enum import Enum, auto

from infra.shared_utils import UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS

logger = get_logger(__name__)  # R9-5

# P1-15: 统一从infra.shared_utils导入
from infra.shared_utils import DEFAULT_RISK_FREE_RATE

from pathlib import Path

# P1-53修复: PREPROCESSED_DB/RESULTS_DB 使用绝对路径，作为全项目唯一权威定义
_PARAM_POOL_DIR = Path(__file__).resolve().parent
PREPROCESSED_DB = str(_PARAM_POOL_DIR / "preprocessed.duckdb")
RESULTS_DB = str(_PARAM_POOL_DIR / "optuna_results.duckdb")
MAX_WORKERS = min(max(1, (os.cpu_count() or 4) // 2), 16)
TRAIN_START = "2023-01-01"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"
TARGET_SYMBOLS: Optional[List[str]] = None
INITIAL_EQUITY = 1_000_000.0

ENABLE_QUEUE_SIMULATION = False
QUEUE_TIMEOUT_SECONDS = 300
MARKET_ORDER_SLIPPAGE_BPS = 5.0
SLIPPAGE_BPS = MARKET_ORDER_SLIPPAGE_BPS
MARKET_ORDER_PRICE_MODE = "weighted"
INSTRUMENT_SLIPPAGE_MULTIPLIER = {
    "ETF": 1.0,
    "FUTURE": 1.2,
    "OPTION_ETF": 1.5,
    "OPTION_INDEX": 2.0,
    "OPTION_COMMODITY": 2.5,
}
ENABLE_CANCEL_SIMULATION = True
CANCEL_DELAY_MS = 100
CANCEL_FAILURE_RATE = 0.05

EXPIRY_SLIPPAGE_MULTIPLIERS = UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS

CASCADE_SLIPPAGE_MULTIPLIER = 1.5
CASCADE_SLIPPAGE_CAP_BPS = 50.0

_CONTRACT_MULTIPLIER_CACHE: Dict[str, float] = {}

_EQUITY_MIN = 1.0
_EQUITY_MAX = 1e15

EXCHANGE_COMMISSION_RATES: Dict = {}

FEE_STRUCTURE = {
    "50ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "300ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "IO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "MO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "COMMODITY_OPTION": {"open": 5.0, "close_today": 5.0, "close_overnight": 5.0, "unit": "per_lot", "multiplier": 1000},
    "COMMODITY_FUTURE": {"open": 10.0, "close_today": 10.0, "close_overnight": 10.0, "unit": "per_lot", "multiplier": 10},
    "DEFAULT": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot", "multiplier": 10000},
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

# ── 回测状态枚举（从 backtest_runner_types.py 合并，Phase 1 轮次5）──

class BacktestStateEnum(Enum):
    INIT = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPED = auto()
    COMPLETED = auto()

# 五唯一性修复：_STATE_REASON_MAP 已统一从 strategy_config_layer 导入（含13项完整映射）
from strategy.strategy_config_layer import _STATE_REASON_MAP  # noqa: E402,F401  五唯一性修复

# ── Metrics (merged from backtest_metrics.py on 2026-06-12) ──

"""
回测指标计算器 — 从backtest_runner_base.py拆分
职责: 盈亏比指标、存活分析修正、持仓分层统计
"""

import logging
from typing import Dict, List, Any

import numpy as np


def compute_profit_loss_ratio_metrics(closed_trades, equity_curve, strategy_type='', ticks_per_bar=0,
                                      _get_annualize_factor=None):
    wins = [t for t in closed_trades if t.pnl > 0]
    losses = [t for t in closed_trades if t.pnl < 0]
    n_trades = len(closed_trades)
    n_wins = len(wins)
    n_losses = len(losses)

    avg_win_pct = float(np.mean([t.pnl_pct for t in wins])) if wins else 0.0
    avg_loss_pct = abs(float(np.mean([t.pnl_pct for t in losses]))) if losses else 0.0
    total_win = sum(t.pnl for t in wins)
    total_loss = abs(sum(t.pnl for t in losses))
    profit_factor = total_win / total_loss if total_loss > 1e-4 else 0.0
    win_loss_ratio = avg_win_pct / avg_loss_pct if avg_loss_pct > 1e-4 else 0.0
    win_rate = n_wins / n_trades if n_trades > 0 else 0.0

    expected_value = 0.0
    if n_trades > 0:
        avg_win = float(np.mean([t.pnl for t in wins])) if wins else 0.0
        avg_loss = abs(float(np.mean([t.pnl for t in losses]))) if losses else 0.0
        expected_value = win_rate * avg_win - (1 - win_rate) * avg_loss

    max_consecutive_losses = 0
    current_streak = 0
    for t in closed_trades:
        if t.pnl < 0:
            current_streak += 1
            max_consecutive_losses = max(max_consecutive_losses, current_streak)
        else:
            current_streak = 0

    max_consecutive_wins = 0
    current_streak = 0
    for t in closed_trades:
        if t.pnl > 0:
            current_streak += 1
            max_consecutive_wins = max(max_consecutive_wins, current_streak)
        else:
            current_streak = 0

    recovery_efficiency = _compute_recovery_efficiency(equity_curve)
    calmar = _compute_calmar(equity_curve, strategy_type, ticks_per_bar, _get_annualize_factor)
    survival_adjusted_wlr = _compute_survival_adjusted_wlr(closed_trades, win_loss_ratio)
    hold_buckets, reason_counts = _compute_trade_statistics(closed_trades)

    return {
        "win_loss_ratio": win_loss_ratio,
        "profit_factor": profit_factor,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "win_rate": win_rate,
        "total_trades": n_trades,
        "win_trades": n_wins,
        "loss_trades": n_losses,
        "max_consecutive_losses": max_consecutive_losses,
        "max_consecutive_wins": max_consecutive_wins,
        "recovery_efficiency": recovery_efficiency,
        "calmar": calmar,
        "survival_adjusted_win_loss_ratio": survival_adjusted_wlr,
        "hold_minutes_buckets": hold_buckets,
        "open_reason_counts": reason_counts,
    }


def _compute_recovery_efficiency(equity_curve):
    recovery_efficiency = 0.0
    if len(equity_curve) > 1:
        cummax = np.maximum.accumulate(equity_curve)
        drawdown_mask = equity_curve < cummax
        if np.any(drawdown_mask):
            dd_indices = np.where(drawdown_mask)[0]
            if len(dd_indices) > 0:
                dd_depth = float(np.max(cummax[dd_indices] - equity_curve[dd_indices]))
                new_high_indices = np.where(equity_curve >= cummax)[0]
                if len(new_high_indices) > 1:
                    total_recovery_bars = 0
                    recovery_count = 0
                    for i in range(1, len(new_high_indices)):
                        gap = int(new_high_indices[i] - new_high_indices[i - 1])
                        if gap > 1:
                            total_recovery_bars += gap
                            recovery_count += 1
                    if recovery_count > 0 and dd_depth > 0:
                        avg_recovery_bars = total_recovery_bars / recovery_count
                        recovery_efficiency = dd_depth / (avg_recovery_bars * float(equity_curve[0])) if equity_curve[0] > 0 else 0.0
                        recovery_efficiency = min(recovery_efficiency * 100.0, 10.0)
    return recovery_efficiency


def _compute_calmar(equity_curve, strategy_type, ticks_per_bar, _get_annualize_factor):
    calmar = 0.0
    if len(equity_curve) > 1:
        total_ret = (equity_curve[-1] / equity_curve[0] - 1) if equity_curve[0] > 0 else 0.0
        if _get_annualize_factor is not None:
            annualize = _get_annualize_factor(strategy_type, ticks_per_bar=ticks_per_bar)
        else:
            annualize = 245.9
        annualized_ret = total_ret * (annualize / len(equity_curve)) if len(equity_curve) > 0 else 0.0
        cummax = np.maximum.accumulate(equity_curve)
        safe_cummax = np.where(cummax > 0, cummax, 1.0)
        # [P2-02] 计划统一到infra/shared_utils.py的max_drawdown函数
        max_dd_pct = float(np.min(equity_curve / safe_cummax - 1))
        if np.isnan(max_dd_pct) or np.isinf(max_dd_pct):
            max_dd_pct = 0.0
        if abs(max_dd_pct) > 1e-10:
            calmar = annualized_ret / abs(max_dd_pct)
        elif annualized_ret > 0:
            calmar = 999.0
    return calmar


def _compute_survival_adjusted_wlr(closed_trades, win_loss_ratio):
    survival_adjusted_wlr = win_loss_ratio
    try:
        n_total = len(closed_trades)
        NON_SURVIVAL_REASONS = frozenset({'stop_loss', 'circuit_breaker_force_close', 'daily_drawdown_force_close'})
        n_survived = sum(1 for t in closed_trades if getattr(t, 'close_reason', '') not in NON_SURVIVAL_REASONS)
        if n_total > 0 and n_survived < n_total:
            survival_rate = n_survived / n_total
            survival_adjusted_wlr = win_loss_ratio * survival_rate
    except (ValueError, KeyError, TypeError):
        pass
    return survival_adjusted_wlr


def _compute_trade_statistics(closed_trades):
    hold_buckets = {"short": 0, "medium": 0, "long": 0}
    reason_counts = {}
    try:
        for t in closed_trades:
            hm = getattr(t, 'hold_minutes', 0)
            if hm < 30:
                hold_buckets["short"] += 1
            elif hm < 120:
                hold_buckets["medium"] += 1
            else:
                hold_buckets["long"] += 1
            reason = getattr(t, 'open_reason', 'unknown')
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    except (ValueError, KeyError, TypeError):
        pass
    return hold_buckets, reason_counts

# ── Pricing (merged from backtest_pricing.py on 2026-06-12) ──

"""
回测定价与识别模块 — 从 backtest_state.py 拆分(Phase 1 轮次4)

职责: 合约/交易所/品种识别、涨跌停计算、手续费计算、
      期权定价、滑点计算、合约乘数查询、流动性分级、到期滑点倍数
"""

import logging
import math
from typing import Any, Dict

import pandas as pd

from infra.shared_utils import DEFAULT_RISK_FREE_RATE, DAYS_PER_YEAR_CALENDAR
from governance.greeks_calculator import _norm_cdf as _bs_norm_cdf


# ── 合约/交易所/品种识别 ──

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
    logging.warning("[EX-P0-06] 未识别合约类型, 使用DEFAULT费率: %s", symbol)
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


# ── 涨跌停计算 ──

def _calculate_limit_prices(
    bar: pd.Series,
    prev_close: float = 0.0,
    instrument_type: str = "FUTURE",
) -> Dict[str, Any]:
    """BF-P1-06已修复: 涨跌停价格计算"""
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


# ── 手续费计算 ──

def calc_trade_fee(contract_type: str, open_time: pd.Timestamp, close_time: pd.Timestamp,
                   lots: int, trade_value: float = 0.0, broker_policy: str = "default") -> float:
    try:
        from infra.commission_utils import calc_trade_fee as _calc_trade_fee
        return _calc_trade_fee(contract_type, open_time, close_time, lots, trade_value, broker_policy)
    except ImportError:
        pass
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


# ── 合约乘数查询 ──

def _get_contract_multiplier(instrument_id: str) -> float:
    """P0-R12-NP-05修复: 获取合约乘数"""
    if instrument_id in _CONTRACT_MULTIPLIER_CACHE:
        return _CONTRACT_MULTIPLIER_CACHE[instrument_id]
    multiplier = 1.0
    try:
        from config.params_service import get_params_service
        params_svc = get_params_service()
        meta = params_svc.get_instrument_meta_by_id(instrument_id)
        if meta:
            product = meta.get('product', '')
            product_cache = params_svc.get_product_cache(product)
            if product_cache:
                multiplier = float(product_cache.get('contract_size', 1.0))
    except (ValueError, KeyError, TypeError, AttributeError) as _e:
        logging.debug("[R3-L2] contract_multiplier获取跳过: %s", _e)
        pass
    _CONTRACT_MULTIPLIER_CACHE[instrument_id] = multiplier
    return multiplier


# ── 流动性分级 ──

def _infer_liquidity_tier(bar) -> str:
    if bar is None:
        return 'future_main'
    _oi = bar.get('open_interest', 0)
    _spread = bar.get('bid_ask_spread', 0)
    _strike = bar.get('strike_price', 0)
    _underlying = bar.get('underlying_price', 0)
    if _strike > 0 and _underlying > 0:
        _moneyness = abs(_strike - _underlying) / _underlying
        if _moneyness < 0.05:
            return 'option_atm'
        elif _moneyness < 0.15:
            return 'option_otm'
        else:
            return 'option_far'
    if _oi > 50000:
        return 'future_main'
    return 'future_sub'


# ── 到期滑点倍数 ──

def _get_expiry_slippage_multiplier(bar: pd.Series, params: Dict[str, float]) -> float:
    """P0-6修复: 根据距到期日天数返回滑点倍增系数"""
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
                    from infra._helpers import get_logger
                    get_logger(__name__).warning("[R22-TIME-03] bar缺少timestamp字段，跳过到期时间计算")
                    return 1.0
                days_to_expiry = (expiry_dt - pd.Timestamp(bar_time)).days
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                days_to_expiry = None
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


# ── 级联滑点计算 ──

def _compute_cascade_slippage_bps(
    base_slippage_bps: float,
    close_volume: float = 0.0,
    avg_volume: float = 1.0,
    is_state_switch: bool = False,
) -> float:
    if not is_state_switch:
        return base_slippage_bps
    participation_rate = close_volume / avg_volume if avg_volume > 0 else 0.0
    volume_impact = 1.0 + math.sqrt(max(participation_rate, 0.0))
    result = base_slippage_bps * volume_impact * CASCADE_SLIPPAGE_MULTIPLIER
    return min(result, CASCADE_SLIPPAGE_CAP_BPS)


# ── 期权定价 ──

def _compute_option_mtm_price(
    spot_price: float,
    strike_price: float,
    remaining_days: int,
    iv: float,
    option_type: str = "call",
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> float:
    """BF-05: 使用Black-Scholes模型计算期权理论价格用于逐Bar MTM"""
    if spot_price <= 0 or strike_price <= 0 or remaining_days <= 0 or iv <= 0:
        if spot_price <= 0 or iv <= 0:
            from infra._helpers import get_logger
            get_logger(__name__).debug("[C-3] _compute_option_mtm_price无效输入: spot_price=%.4f iv=%.4f", spot_price, iv)
        return 0.0
    try:
        import math as _m
        t = remaining_days / DAYS_PER_YEAR_CALENDAR
        d1 = (_m.log(spot_price / strike_price) + (risk_free_rate + 0.5 * iv ** 2) * t) / (iv * _m.sqrt(t))
        d2 = d1 - iv * _m.sqrt(t)
        if option_type == "call":
            price = spot_price * _bs_norm_cdf(d1) - strike_price * _m.exp(-risk_free_rate * t) * _bs_norm_cdf(d2)
        else:
            price = strike_price * _m.exp(-risk_free_rate * t) * _bs_norm_cdf(-d2) - spot_price * _bs_norm_cdf(-d1)
        return max(0.0, price)
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        return 0.0

# ── Task Scheduler (merged from task_scheduler.py on 2026-06-12) ──

#!/usr/bin/env python3
"""task_scheduler - Facade模块 (R27-CP-08-FIX: 拆分为3个子模块，本文件保留向后兼容re-export)

原task_scheduler.py(2423行)已拆分为:
  - ts_param_grids.py: 参数网格定义 (DEFAULT_RISK_FREE_RATE, BACKTEST_THRESHOLDS, *_GRID等)
  - ts_backtest_strategies.py: 6策略组回测函数 (_worker_task, run_cycle_resonance_backtest_sweep等)
  - ts_result_writer.py: 结果写入与质量门 (_insert_results, _execute_round, main_scheduler等)


re-export from ts_result_scheduler:
  - main_scheduler: 四阶段参数扫描主流程(实现在ts_result_scheduler.py,通过ts_result_writer.py委托)

本文件仅做re-export，确保所有 from param_pool.task_scheduler import XXX 继续有效。'
循环依赖的re-export已改为期。getattr__延迟导入。
"""
from infra.commission_utils import (
    P0_IRON_RULES,
    REASON_MULTIPLIERS,
    detect_rollover_gaps,
    compute_rollover_cost,
)
from infra.shared_utils import DEFAULT_RISK_FREE_RATE
from param_pool._param_grids import (
    BACKTEST_THRESHOLDS,
)
from param_pool._param_defaults import (
    MULTISCALE_BAR_LENGTHS,
    BAR_INTERVAL_GRID,
    KLINE_LENGTH_PARAM_GRID,
    _SUBPROCESS_NEEDED_COLS,
    CR_PARAM_GRID,
    HFT_TICK_PARAMS,
    PARAM_DEFAULTS,
    PARAM_DEFAULTS_HFT,
    PARAM_DEFAULTS_BOX_EXTREME,
    PARAM_DEFAULTS_BOX_SPRING,
    PARAM_DEFAULTS_ARBITRAGE,
    PARAM_DEFAULTS_MARKET_MAKING,
    PARAM_DEFAULTS_SHADOW_A,
    PARAM_DEFAULTS_SHADOW_B,
    PARAM_DEFAULTS_HFT_SHADOW_A,
    PARAM_DEFAULTS_HFT_SHADOW_B,
    PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A,
    PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B,
    PARAM_DEFAULTS_BOX_SPRING_SHADOW_A,
    PARAM_DEFAULTS_BOX_SPRING_SHADOW_B,
    PARAM_DEFAULTS_ARBITRAGE_SHADOW_A,
    PARAM_DEFAULTS_ARBITRAGE_SHADOW_B,
    PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A,
    PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B,
    STRATEGY_SHADOW_DEFAULTS,
    SCAN_TARGET_SORT_METRIC,
)
from param_pool._param_grids import _BacktestPosition, _BacktestState
from param_pool._param_grids import (
    DEEP_VALIDATION_TIERS,
    PARAM_TIERS,
    L2_HYPERPARAMS,
    L2_PARAM_GRID,
    L2_CONFLICT_RESOLUTION,
    PARAM_SOURCE_ANNOTATION,
)


_lazy_names = {
    '_prepare_df_for_subprocess', '_worker_init', 'cleanup_global_data',
    '_worker_task', 'run_cycle_resonance_backtest_sweep', 'run_cr_params_sweep',
    '_validate_ddl_column_names', '_ensure_results_table', '_get_completed_task_ids',
    '_query_minute_table', '_load_data_for_period', '_load_multiscale_data',
    '_resample_bars_runtime', '_interpolate_ticks_in_bar', '_insert_results',
    '_execute_round', '_validate_params_via_params_service', 'build_and_save_life_dict',
    'main_scheduler',
    'check_l2_conflict', 'compute_alpha_confidence_interval',
    'optimize_l2_params_step1', 'run_step2_smoke_test',
    'run_deep_validation_tiered', 'validate_doomed_tests',
    'validate_market_friendliness_baseline',
    '_select_top_k_train', 'validate_default_values_in_grids',
    'validate_shadow_param_independence', '_check_two_stage_stop',
    '_try_open',
}


def __getattr__(name):
    if name not in _lazy_names:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name in ('_prepare_df_for_subprocess', '_worker_init', 'cleanup_global_data',
                '_worker_task', 'run_cycle_resonance_backtest_sweep', 'run_cr_params_sweep'):
        from param_pool import ts_backtest_strategies as _mod
        val = getattr(_mod, name)
    elif name in ('_validate_ddl_column_names', '_ensure_results_table', '_get_completed_task_ids',
                  '_query_minute_table', '_load_data_for_period', '_load_multiscale_data',
                  '_resample_bars_runtime', '_interpolate_ticks_in_bar', '_insert_results',
                  '_execute_round', '_validate_params_via_params_service', 'build_and_save_life_dict',
                  'main_scheduler'):
        from param_pool import ts_result_writer as _mod
        val = getattr(_mod, name)
    elif name in ('check_l2_conflict', 'compute_alpha_confidence_interval',
                  'optimize_l2_params_step1', 'run_step2_smoke_test'):
        from param_pool import validation_l2_hyperparams as _mod
        val = getattr(_mod, name)
    elif name in ('run_deep_validation_tiered', 'validate_doomed_tests',
                  'validate_market_friendliness_baseline'):
        from param_pool import validation_deep_orchestrator as _mod
        val = getattr(_mod, name)
    elif name in ('_select_top_k_train', 'validate_default_values_in_grids',
                  'validate_shadow_param_independence', '_check_two_stage_stop',
                  '_BacktestPosition', '_BacktestState', '_try_open'):
        from param_pool import backtest_runner_base as _mod
        val = getattr(_mod, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = val
    return val
