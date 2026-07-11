# [M1-99] 回测工具函数
# MODULE_ID: M1-157
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
from __future__ import annotations

import itertools
import math
import json
import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
import multiprocessing
import os
import re
import random
import sys
import threading
import time
import copy
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import numpy as np

from ali2026v3_trading.infra.shared_utils import STOP_PRICE_RTOL as _STOP_PRICE_RTOL
from ali2026v3_trading.infra.resilience import (
    should_trigger_stop_loss as _should_trigger_stop_loss,
    should_trigger_take_profit as _should_trigger_take_profit,
)
import pandas as pd
import warnings

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading.infra.shared_utils import compute_slippage_bps, UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS, compute_execution_delay_slippage_bps
# compute_profit_loss_ratio_metrics defined locally in this module
from ali2026v3_trading.param_pool.backtest.backtest_config import compute_profit_loss_ratio_metrics as _compute_plr_metrics_delegated
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
from ali2026v3_trading.infra.shared_utils import get_annualize_factor as _get_annualize_factor
from ali2026v3_trading.infra.shared_utils import safe_price_check
from ali2026v3_trading.infra.trading_utils import CrossSystemExecutionKernel
from ali2026v3_trading.data.data_access import get_data_access
from ali2026v3_trading.data.db_adapter import connect_duckdb

_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()
logger = get_logger(__name__)  # R9-5

from ali2026v3_trading.param_pool.backtest.backtest_config import (
    DEFAULT_RISK_FREE_RATE,
    PREPROCESSED_DB,
    RESULTS_DB,
    MAX_WORKERS,
    TRAIN_START,
    TEST_START,
    TEST_END,
    TARGET_SYMBOLS,
    INITIAL_EQUITY,
    ENABLE_QUEUE_SIMULATION,
    QUEUE_TIMEOUT_SECONDS,
    MARKET_ORDER_SLIPPAGE_BPS,
    SLIPPAGE_BPS,
    MARKET_ORDER_PRICE_MODE,
    INSTRUMENT_SLIPPAGE_MULTIPLIER,
    ENABLE_CANCEL_SIMULATION,
    CANCEL_DELAY_MS,
    CANCEL_FAILURE_RATE,
    EXPIRY_SLIPPAGE_MULTIPLIERS,
    FEE_STRUCTURE,
    FEE_STRUCTURE_V2,
    COMMISSION_PER_LOT,
    CASCADE_SLIPPAGE_MULTIPLIER,
    CASCADE_SLIPPAGE_CAP_BPS,
    _CONTRACT_MULTIPLIER_CACHE,
    _EQUITY_MIN,
    _EQUITY_MAX,
    EXCHANGE_COMMISSION_RATES,
    LIMIT_UP_RATIO,
    LIMIT_DOWN_RATIO,
)

from ali2026v3_trading.param_pool.backtest._backtest_fidelity import (  # noqa: F401
    _simulate_limit_order_queue,
    _simulate_market_order_slippage,
    _get_instrument_type_slippage,
    _simulate_order_cancel,
    _compute_fill_quantity,
    _apply_fidelity_presets,
)

from ali2026v3_trading.param_pool._param_grids import (
    _PendingOrder,
    _BacktestPosition,
    _ClosedTrade,
    _BacktestState,
)
from ali2026v3_trading.param_pool.backtest.backtest_state import (
    _get_life_estimator,
    _get_cascade_judge_module,
    _infer_contract_type,
    _infer_exchange_id,
    _infer_exchange_from_id,
    _infer_instrument_type,
    _calculate_limit_prices,
    calc_trade_fee,
    _compute_commission,
    _resolve_tp_sl,
    _resolve_time_stop,
    _resolve_time_stop_hard,
    _is_consecutive_loss_paused,
    _backtest_order_split,
    _infer_liquidity_tier,
    _get_expiry_slippage_multiplier,
    _compute_dynamic_slippage_bps,
    _get_contract_multiplier,
    _safe_equity_add,
    _compute_cascade_slippage_bps,
    _compute_option_mtm_price,
    _update_mtm_equity,
    _check_intra_bar_stop_loss,
    _check_two_stage_stop,
    _get_reason_tp_sl_from_position_service,
)

from ali2026v3_trading.param_pool._param_grids import (
    PARAM_DEFAULTS, PARAM_GRID, PULLBACK_DEFAULTS, PULLBACK_GRID,
    PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B,
    PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B,
    PARAM_GRID_ROUND1, PARAM_GRID_HFT,
)
from ali2026v3_trading.infra.commission_utils import REASON_MULTIPLIERS, P0_IRON_RULES
from ali2026v3_trading.infra.commission_utils import detect_rollover_gaps, compute_rollover_cost

from ali2026v3_trading.param_pool._param_grids import _sync_random_seed, _compute_market_impact_v2, _compute_dynamic_slippage_bps
from ali2026v3_trading.param_pool.backtest.backtest_config import BacktestStateEnum, _STATE_REASON_MAP





def set_global_random_seed(seed: int) -> int:
    """P1-54修复: 委托到shared_utils.set_global_seed统一入口

    用于回测可复现性和测试fixture。返回seed值方便链式调用。
    """
    from ali2026v3_trading.infra.shared_utils import set_global_seed
    return set_global_seed(seed)


# P1-48修复: _build_risk_dimension_defaults 统一由 backtest_loop_risk 提供


def _sync_reason_multipliers_with_position_service() -> None:
    """使用PositionService作为TP/SL规则权威源"""
    try:
        from ali2026v3_trading.position.position_service import PositionService

        base_tp = float(getattr(PositionService, 'DEFAULT_TP_RATIO', 1.8) or 1.8)
        base_sl = float(getattr(PositionService, 'DEFAULT_SL_RATIO', 0.30) or 0.30)
        reason_defaults = dict(getattr(PositionService, 'TP_SL_REASON_DEFAULTS', {}) or {})

        for reason, ratios in reason_defaults.items():
            if not isinstance(ratios, tuple) or len(ratios) != 2:
                continue
            tp_ratio, sl_ratio = ratios
            if base_tp <= 0 or base_sl <= 0:
                continue
            profile = REASON_MULTIPLIERS.setdefault(reason, {"tp_mult": 1.0, "sl_mult": 1.0, "time_mult": 1.0})
            profile["tp_mult"] = float(tp_ratio) / base_tp
            profile["sl_mult"] = float(sl_ratio) / base_sl
    except (ImportError, AttributeError) as e:
        get_logger(__name__)  # R9-5.debug("[task_scheduler] sync REASON_MULTIPLIERS skipped: %s", e)


def _sync_thresholds_from_runtime_config() -> None:
    """将评判阈值与运行时参数联动"""
    from ali2026v3_trading.param_pool._param_grids import (
        _BACKTEST_THRESHOLD_LINK_MAP, _P0_IRON_RULE_LINK_MAP, BACKTEST_THRESHOLDS,
    )
    try:
        from ali2026v3_trading.config.config_service import get_cached_params
        cp = get_cached_params() or {}
    except (ImportError, AttributeError) as e:
        logger.debug("[task_scheduler] 阈值联动跳过，读取参数失败: %s", e)
        return

    for bt_key, cp_key in _BACKTEST_THRESHOLD_LINK_MAP.items():
        val = cp.get(cp_key)
        if isinstance(val, (int, float)):
            BACKTEST_THRESHOLDS[bt_key] = float(val)

    for p0_key, cp_key in _P0_IRON_RULE_LINK_MAP.items():
        val = cp.get(cp_key)
        if isinstance(val, (int, float)):
            P0_IRON_RULES[p0_key] = float(val)

    logger.info("[task_scheduler] 评判阈值已与运行时参数联动同步")


def _select_top_k_train(results: List[Dict[str, Any]], metric: str, k: int,
                        fallback_metric: str = "minute_sharpe",
                        scan_param: Optional[str] = None) -> List[Dict[str, Any]]:
    from ali2026v3_trading.param_pool._param_defaults import PARAM_SORT_METRIC_OVERRIDE
    if scan_param and scan_param in PARAM_SORT_METRIC_OVERRIDE:
        metric = PARAM_SORT_METRIC_OVERRIDE[scan_param]
    train_rows = [
        r for r in results
        if r.get("is_train") and r.get(metric) is not None
    ]
    if not train_rows and fallback_metric and fallback_metric != metric:
        train_rows = [
            r for r in results
            if r.get("is_train") and r.get(fallback_metric) is not None
        ]
        metric = fallback_metric
    train_rows.sort(key=lambda r: r.get(metric, 0) or 0, reverse=True)
    return train_rows[:k]


from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import (
    validate_default_values_in_grids,
    validate_shadow_param_independence,
    _check_safety,
    _check_backtest_health,
    _check_bar_data_monotonic,
)



def _try_open(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    strategy_type: str = "main",
    prev_bar: Optional[pd.Series] = None,
) -> None:
    from ali2026v3_trading.param_pool import backtest_state as _bpm
    from ali2026v3_trading.param_pool._param_grids import BACKTEST_THRESHOLDS
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


def _check_positions(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    prev_bar: Optional[pd.Series] = None,
) -> None:
    from ali2026v3_trading.param_pool import backtest_state as _bpm2
    from ali2026v3_trading.param_pool._param_grids import BACKTEST_THRESHOLDS
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
            from ali2026v3_trading.param_pool import backtest_safety_checker as _bsc2
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

    should_close = False
    close_reason = ""

    if params.get("enable_intra_bar_stop_loss", False):
        intra_triggered, intra_reason, intra_fill = _check_intra_bar_stop_loss(pos, bar, prev_bar, params)
        if intra_triggered:
            should_close = True
            close_reason = intra_reason
            price = intra_fill

    if not should_close and pos.volume > 0:
        if _should_trigger_take_profit(price, pos.stop_profit_price, is_long=True, rtol=_STOP_PRICE_RTOL):
            should_close = True
            close_reason = "StopProfit"
        elif _should_trigger_stop_loss(price, pos.stop_loss_price, is_long=True, rtol=_STOP_PRICE_RTOL):
            should_close = True
            close_reason = "StopLoss"
    elif pos.volume < 0:
        if _should_trigger_take_profit(price, pos.stop_profit_price, is_long=False, rtol=_STOP_PRICE_RTOL):
            should_close = True
            close_reason = "StopProfit"
        elif _should_trigger_stop_loss(price, pos.stop_loss_price, is_long=False, rtol=_STOP_PRICE_RTOL):
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
                logger.debug("[P1-R11-04] 平仓延迟: %s 延迟%d bar, 原因=%s", symbol, _close_delay, close_reason)
                should_close = False
            elif _delay_remaining > 0:
                pos._close_delay_remaining = _delay_remaining - 1
                should_close = False

    if should_close:
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
            logger.warning("[R4-P-06] NaN/inf detected in net_pnl: pnl=%.4f, slip=%.4f, commission=%.4f, net_pnl=%.4f",
                           pnl, slip, commission, net_pnl)
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


def _compute_profit_loss_ratio_metrics(closed_trades: List[_ClosedTrade], equity_curve: np.ndarray, strategy_type: str = '', ticks_per_bar: int = 0) -> Dict[str, Any]:
    return _compute_plr_metrics_delegated(
        closed_trades, equity_curve, strategy_type=strategy_type,
        ticks_per_bar=ticks_per_bar, _get_annualize_factor=_get_annualize_factor
    )


def _build_backtest_result(
    bt: _BacktestState,
    strategy_type: str,
    bar_data: pd.DataFrame,
    params: Dict[str, float],
    extra_fields: Optional[Dict[str, Any]] = None,
    ticks_per_bar: int = 0,
) -> Dict[str, Any]:
    """统一计算回测汇总指标（从 backtest_runner_result.py 合并，Phase 1 轮次5）"""
    from ali2026v3_trading.param_pool.backtest.backtest_config import DEFAULT_RISK_FREE_RATE, INITIAL_EQUITY
    from ali2026v3_trading.infra.commission_utils import detect_rollover_gaps, compute_rollover_cost
    import math as _math

    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        _safe_prev = np.where(equity_arr[:-1] > 0, equity_arr[:-1], 1.0)
        returns = np.diff(equity_arr) / _safe_prev
        _af = _get_annualize_factor(strategy_type, ticks_per_bar=ticks_per_bar)
        sharpe = np.sqrt(_af) * (np.mean(returns) - DEFAULT_RISK_FREE_RATE / _af) / np.std(returns, ddof=1) if np.std(returns, ddof=1) > 1e-10 else 0.0
        sharpe = float(np.clip(sharpe, -10.0, 10.0))
    else:
        sharpe = 0.0

    if len(equity_arr) > 0:
        _cummax = np.maximum.accumulate(equity_arr)
        _safe_cummax = np.where(_cummax > 0, _cummax, 1.0)
        max_dd = float(np.min(equity_arr / _safe_cummax - 1))
        if np.isnan(max_dd) or np.isinf(max_dd):
            max_dd = 0.0
    else:
        max_dd = 0.0

    plr_metrics = _compute_profit_loss_ratio_metrics(bt.closed_trades, equity_arr, strategy_type, ticks_per_bar=ticks_per_bar)

    rollover_cost_bps = 0.0
    try:
        rollover_points = detect_rollover_gaps(
            bar_data,
            gap_threshold=params.get('rollover_gap_threshold', 0.02)
        )
        if rollover_points:
            bar_data = exclude_rollover_signals(bar_data, rollover_points, skip_days=params.get('rollover_skip_days', 3))
            n_excluded = bar_data['rollover_excluded'].sum() if 'rollover_excluded' in bar_data.columns else 0
            if n_excluded > 0:
                logger.info("[裂缝5] 展期排除: %d/%d bar被标记", n_excluded, len(bar_data))
            rollover_result = compute_rollover_cost(rollover_points, bar_data, params)
            rollover_cost_bps = rollover_result.get("total_rollover_cost_bps", 0.0)
            if rollover_cost_bps > 0 and bt.equity > 0:
                bt.equity -= bt.equity * rollover_cost_bps / 10000.0
                total_return = bt.equity / INITIAL_EQUITY - 1
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.warning("[P0-7] rollover cost deduction failed: %s", e)

    daily_returns_list = list(bt.daily_returns) if hasattr(bt, 'daily_returns') else []
    try:
        from ali2026v3_trading.risk.risk_service import calculate_var_historical
        var_95 = calculate_var_historical(daily_returns_list, 0.95) if len(daily_returns_list) >= 10 else 0.0
    except (ImportError, AttributeError):
        var_95 = 0.0
    result = {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
        "rollover_cost_bps": rollover_cost_bps,
        "var_95": var_95,
        **plr_metrics,
        "closed_trades_count": len(bt.closed_trades),
        "circuit_breaker_count": len(bt.circuit_breaker_events),
        "profit_factor": plr_metrics.get("profit_factor", 0.0),
        "win_rate": plr_metrics.get("win_rate", 0.0),
        "calmar": plr_metrics.get("calmar", 0.0),
        "mtm_max_drawdown": getattr(bt, 'mtm_max_drawdown', 0.0),
        "mtm_equity_curve_len": len(getattr(bt, 'mtm_equity_curve', [])),
        "mtm_equity_curve": list(getattr(bt, 'mtm_equity_curve', [])),
        "max_drawdown_ratio": (getattr(bt, 'mtm_max_drawdown', 0.0) / max(getattr(bt, 'max_drawdown', 0.001), 0.001)),
        "avg_fill_ratio": float(np.mean([getattr(p, 'fill_ratio', 1.0) for p in bt.positions.values()])) if bt.positions else 1.0,
    }
    for _k, _v in list(result.items()):
        if isinstance(_v, float) and (_math.isnan(_v) or _math.isinf(_v)):
            logger.warning("[R24-P1-IV-08] 参数池输出异常: %s=%s, 置为0", _k, _v)
            result[_k] = 0.0
    if extra_fields:
        result.update(extra_fields)
    _fidelity_score = 75.0
    if params.get('enable_tick_backtest', False):
        _fidelity_score = 95.0
    elif params.get('enable_mtm_equity', False):
        _fidelity_score = 80.0
    result['backtest_fidelity_estimate'] = {
        'fidelity_score': _fidelity_score,
        'bias_direction': 'optimistic',
        'estimated_slippage_bias_bps': params.get('slippage_bps', 0),
        'estimated_delay_bias_bars': params.get('open_execution_delay_bars', 0),
        'notes': '回测偏差方向:乐观(回测收益倾向高于实盘)。评分:75=Bar回测,80=MTM启用,95=Tick回测'
    }
    return result


def _reset_daily(bt: _BacktestState, current_date: str) -> None:
    if bt.prev_date is not None and current_date != bt.prev_date:
        if bt.daily_start_equity > 0:
            daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
            bt.daily_returns.append(daily_ret)
        bt.daily_start_equity = bt.equity
        bt.daily_loss = 0.0
        bt.circuit_breaker_until = None
    bt.prev_date = current_date


def _backfill_bar_fields(bar):
    if "underlying_future_id" not in bar or not bar.get("underlying_future_id"):
        _sym = bar.get("symbol", "")
        if _sym:
            _m = re.match(r'^([A-Za-z]+)', _sym)
            if _m:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
                    bar["underlying_future_id"] = hash(_m.group(1)) % 1000
            _ym = re.search(r'(\d{4})', _sym)
            if _ym:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
                    bar["month"] = _ym.group(1)[-2:]



def exclude_rollover_signals(bar_data: pd.DataFrame,
                              rollover_points: List[Dict[str, Any]],
                              skip_days: int = 3,
                              date_col: str = "minute") -> pd.DataFrame:
    """P0-裂缝5：在展期日前后各skip_days天剔除所有策略信号"""
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
            except (ValueError, KeyError, TypeError):
                start = max(0, idx - skip_days * 240)
                end = min(len(bar_data), idx + skip_days * 240)
                bar_data.loc[bar_data.index[start:end], "rollover_excluded"] = True

    n_excluded = bar_data["rollover_excluded"].sum()
    if n_excluded > 0:
        logger.info("[裂缝5-展期剔除] 标记 %d/%d bar为展期排除区", n_excluded, len(bar_data))

    return bar_data


from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import (
    _bt_capture_snapshot, _check_state_transition,
    _compute_lots_with_risk_budget, _check_logic_reversal,
    _infer_trend_scores_from_bar,
)


__all__ = [
    '_get_annualize_factor',
    '_sync_random_seed',
    '_build_risk_dimension_defaults',
    '_sync_reason_multipliers_with_position_service',
    '_sync_thresholds_from_runtime_config',
    '_select_top_k_train',
    'validate_default_values_in_grids',
    'validate_shadow_param_independence',
    '_bt_capture_snapshot',
    '_check_state_transition',
    '_compute_lots_with_risk_budget',
    '_check_logic_reversal',
    '_check_safety',
    '_check_backtest_health',
    '_try_open',
    '_check_positions',
    '_compute_profit_loss_ratio_metrics',
    '_check_bar_data_monotonic',
    '_build_backtest_result',
    '_reset_daily',
    '_backfill_bar_fields',
    '_infer_trend_scores_from_bar',
    'exclude_rollover_signals',
    '_check_intra_bar_stop_loss',
    '_check_two_stage_stop',
]

# ── Merged from backtest_loop_risk.py on 2026-06-12 ──

"""
回测风控与止损 — 从backtest_loop.py拆分
职责: 风险维度默认值、影子参数独立性验证、时间止损、连亏暂停
"""

import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from typing import Dict

import pandas as pd

logger = get_logger(__name__)  # R9-5


def _build_risk_dimension_defaults():
    try:
        from ali2026v3_trading.risk.risk_service import RiskService
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
    except (ValueError, KeyError, TypeError):
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


from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence  # F-09: 权威版在backtest_runner_validation，此处re-export


def _resolve_time_stop_hard(params: Dict[str, float], open_reason: str) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟）'
    策略分层时间参数：
    - HFT: hft_hard_time_stop_ms → 转换为分钟
    - 弹簧: spring_hard_time_stop_sec → 转换为分钟
    - 共振: resonance_hard_time_stop_min → 直接使用
    - 箱体: box_hard_time_stop_min → 直接使用
    """
    from ali2026v3_trading.param_pool._param_defaults import _REASON_TIME_STOP_SOURCE
    from ali2026v3_trading.infra.commission_utils import REASON_MULTIPLIERS
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
    from ali2026v3_trading.param_pool.backtest.backtest_state import _resolve_time_stop_hard, _get_life_estimator
    from ali2026v3_trading.infra.commission_utils import REASON_MULTIPLIERS
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

# ── Merged from backtest_loop_positions.py on 2026-06-12 ──

"""
回测持仓管理循环 — 从backtest_loop.py拆分
职责: 平仓决策、平仓执行、两阶段止损、Bar内止损
"""

import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.shared_utils import STOP_PRICE_RTOL as _STOP_PRICE_RTOL
from ali2026v3_trading.infra.resilience import (
    should_trigger_stop_loss as _should_trigger_stop_loss,
    should_trigger_take_profit as _should_trigger_take_profit,
)

logger = get_logger(__name__)  # R9-5


from ali2026v3_trading.param_pool.backtest.backtest_state import (
    _check_intra_bar_stop_loss,
    _check_two_stage_stop,
)


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
        if _should_trigger_take_profit(price, pos.stop_profit_price, is_long=True, rtol=_STOP_PRICE_RTOL):
            should_close = True
            close_reason = "StopProfit"
        elif _should_trigger_stop_loss(price, pos.stop_loss_price, is_long=True, rtol=_STOP_PRICE_RTOL):
            should_close = True
            close_reason = "StopLoss"
    elif pos.volume < 0:
        if _should_trigger_take_profit(price, pos.stop_profit_price, is_long=False, rtol=_STOP_PRICE_RTOL):
            should_close = True
            close_reason = "StopProfit"
        elif _should_trigger_stop_loss(price, pos.stop_loss_price, is_long=False, rtol=_STOP_PRICE_RTOL):
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
    from ali2026v3_trading.param_pool.backtest.backtest_state import (
        _get_contract_multiplier, _compute_dynamic_slippage_bps,
        _compute_cascade_slippage_bps, _compute_commission,
        _safe_equity_add, _infer_exchange_id, _infer_exchange_from_id,
    )
    from ali2026v3_trading.param_pool._param_grids import BACKTEST_THRESHOLDS
    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _bt_capture_snapshot
    from ali2026v3_trading.param_pool._param_grids import _ClosedTrade
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
