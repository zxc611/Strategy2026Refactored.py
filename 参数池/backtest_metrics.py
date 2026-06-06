"""
回测指标计算器 — 从backtest_runner_base.py拆分
职责: 盈亏比指标、存活分析修正、持仓分层统计
"""
from __future__ import annotations

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
    except Exception:
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
    except Exception:
        pass
    return hold_buckets, reason_counts