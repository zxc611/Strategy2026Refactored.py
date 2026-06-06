from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02
INITIAL_EQUITY = 1_000_000.0


def _build_backtest_result(
    bt,
    strategy_type: str,
    bar_data: pd.DataFrame,
    params: Dict[str, float],
    extra_fields: Optional[Dict[str, Any]] = None,
    ticks_per_bar: int = 0,
    _compute_profit_loss_ratio_metrics_fn=None,
    _get_annualize_factor_fn=None,
    detect_rollover_gaps_fn=None,
    exclude_rollover_signals_fn=None,
    compute_rollover_cost_fn=None,
) -> Dict[str, Any]:
    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        _safe_prev = np.where(equity_arr[:-1] > 0, equity_arr[:-1], 1.0)
        returns = np.diff(equity_arr) / _safe_prev
        _af = _get_annualize_factor_fn(strategy_type, ticks_per_bar=ticks_per_bar) if _get_annualize_factor_fn else 240
        sharpe = np.sqrt(_af) * (np.mean(returns) - RISK_FREE_RATE / _af) / np.std(returns, ddof=1) if np.std(returns, ddof=1) > 1e-10 else 0.0
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

    plr_metrics = {}
    if _compute_profit_loss_ratio_metrics_fn is not None:
        plr_metrics = _compute_profit_loss_ratio_metrics_fn(bt.closed_trades, equity_arr, strategy_type, ticks_per_bar=ticks_per_bar)

    rollover_cost_bps = 0.0
    if detect_rollover_gaps_fn and exclude_rollover_signals_fn and compute_rollover_cost_fn:
        try:
            rollover_points = detect_rollover_gaps_fn(bar_data)
            if rollover_points:
                bar_data = exclude_rollover_signals_fn(bar_data, rollover_points, skip_days=params.get('rollover_skip_days', 3))
                n_excluded = bar_data['rollover_excluded'].sum() if 'rollover_excluded' in bar_data.columns else 0
                if n_excluded > 0:
                    logger.info("[裂缝5] 展期排除: %d/%d bar被标记", n_excluded, len(bar_data))
                rollover_result = compute_rollover_cost_fn(rollover_points, bar_data, params)
                rollover_cost_bps = rollover_result.get("total_rollover_cost_bps", 0.0)
                if rollover_cost_bps > 0 and bt.equity > 0:
                    bt.equity -= bt.equity * rollover_cost_bps / 10000.0
                    total_return = bt.equity / INITIAL_EQUITY - 1
        except Exception as e:
            logger.warning("[P0-7] rollover cost deduction failed: %s", e)

    daily_returns_list = list(bt.daily_returns) if hasattr(bt, 'daily_returns') else []
    try:
        from ali2026v3_trading.risk_service import calculate_var_historical
        var_95 = calculate_var_historical(daily_returns_list, 0.95) if len(daily_returns_list) >= 10 else 0.0
    except Exception:
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
        if isinstance(_v, float) and (math.isnan(_v) or math.isinf(_v)):
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