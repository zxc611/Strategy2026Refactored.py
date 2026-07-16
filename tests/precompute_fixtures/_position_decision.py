# MODULE_ID: M1-181
"""Position decision vectorized pre-computation module — 6-dim TVF + Kelly."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from infra._helpers import get_logger

logger = get_logger(__name__)


def compute_position_decision_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(
            np.zeros((0, 8)),
            columns=[
                "tvf_trend",
                "tvf_volatility",
                "tvf_flow",
                "tvf_risk",
                "tvf_pullback",
                "tvf_entropy",
                "kelly_fraction",
                "position_suggestion",
            ],
        )
    close = df["close"].to_numpy(dtype=np.float64)
    iv = (
        df["iv"].to_numpy(dtype=np.float64)
        if "iv" in df.columns
        else np.full(n, 0.2)
    )
    imbalance = (
        df["imbalance"].to_numpy(dtype=np.float64)
        if "imbalance" in df.columns
        else np.zeros(n)
    )
    consistency = (
        df["consistency"].to_numpy(dtype=np.float64)
        if "consistency" in df.columns
        else np.zeros(n)
    )
    signal_s1 = (
        df["signal_s1"].to_numpy(dtype=np.float64)
        if "signal_s1" in df.columns
        else np.zeros(n)
    )
    signal_s6 = (
        df["signal_s6"].to_numpy(dtype=np.float64)
        if "signal_s6" in df.columns
        else np.zeros(n)
    )
    state_entropy = (
        df["state_entropy"].to_numpy(dtype=np.float64)
        if "state_entropy" in df.columns
        else np.full(n, 0.5)
    )
    pullback_pct = (
        df["pullback_pct_peak"].to_numpy(dtype=np.float64)
        if "pullback_pct_peak" in df.columns
        else np.zeros(n)
    )

    tvf_trend = np.clip(np.abs(signal_s1), 0.0, 1.0)
    tvf_volatility = np.clip(1.0 - iv / 1.0, 0.0, 1.0)
    tvf_flow = np.clip(
        np.abs(imbalance) * (0.5 + 0.5 * np.abs(consistency)), 0.0, 1.0
    )
    tvf_risk = np.clip(1.0 - signal_s6, 0.0, 1.0)
    tvf_pullback = np.clip(1.0 - pullback_pct * 10.0, 0.0, 1.0)
    tvf_entropy = np.clip(1.0 - state_entropy, 0.0, 1.0)

    weights = np.array([0.25, 0.15, 0.15, 0.20, 0.15, 0.10])
    tvf_components = np.stack(
        [tvf_trend, tvf_volatility, tvf_flow, tvf_risk, tvf_pullback, tvf_entropy],
        axis=1,
    )
    tvf_score = np.dot(tvf_components, weights)

    price_ret = np.zeros(n)
    safe_close_prev = np.where(close[:-1] > 1e-6, close[:-1], close[1:])
    gross_pnl = close[1:] - safe_close_prev
    transaction_cost_rate = 0.0003
    cost_per_bar = transaction_cost_rate * (close[1:] + safe_close_prev)
    price_ret[1:] = gross_pnl - cost_per_bar
    price_ret = np.nan_to_num(price_ret, nan=0.0)
    window = 120
    min_samples = max(40, window // 3)
    kelly_fraction = np.full(n, 0.1)
    
    if n > window:
        from numpy.lib.stride_tricks import sliding_window_view
        windows = sliding_window_view(price_ret, window)[:-1]
        
        win_mask = windows > 0
        loss_mask = windows < 0
        win_counts = np.sum(win_mask, axis=1)
        loss_counts = np.sum(loss_mask, axis=1)
        total_counts = win_counts + loss_counts
        
        valid_mask = total_counts >= min_samples
        
        p_win = np.zeros(len(windows))
        p_win[valid_mask] = win_counts[valid_mask] / total_counts[valid_mask]
        
        win_windows = np.where(win_mask, windows, 0.0)
        loss_windows = np.where(loss_mask, np.abs(windows), 0.0)
        
        win_sums = np.sum(win_windows, axis=1)
        loss_sums = np.sum(loss_windows, axis=1)
        
        avg_win = np.where(win_counts > 0, win_sums / win_counts, 0.01)
        avg_loss = np.where(loss_counts > 0, loss_sums / loss_counts, 0.01)
        
        valid_ratio_mask = valid_mask & (avg_win > 1e-10) & (avg_loss > 1e-10)
        win_loss_ratio = np.full(len(windows), 1.0)
        win_loss_ratio[valid_ratio_mask] = avg_win[valid_ratio_mask] / avg_loss[valid_ratio_mask]
        
        kelly = p_win - (1 - p_win) / win_loss_ratio
        kelly = np.where(valid_ratio_mask, kelly, 0.0)
        
        kelly_fraction[window:] = np.clip(kelly * 0.5, -0.25, 0.25)

    position_suggestion = np.clip(tvf_score * kelly_fraction * 2.0, 0.0, 1.0)
    return pd.DataFrame(
        {
            "tvf_trend": tvf_trend,
            "tvf_volatility": tvf_volatility,
            "tvf_flow": tvf_flow,
            "tvf_risk": tvf_risk,
            "tvf_pullback": tvf_pullback,
            "tvf_entropy": tvf_entropy,
            "kelly_fraction": kelly_fraction,
            "position_suggestion": position_suggestion,
        }
    )