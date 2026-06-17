# MODULE_ID: M1-183
"""Signal S1-S6 vectorized computation module."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


def compute_s1_trend_resonance(kl_rpd_k, kl_rpd_d, imbalance, consistency):
    w_k, w_d, w_imb = 0.4, 0.3, 0.3
    k_component = np.sign(kl_rpd_k) * np.abs(kl_rpd_k) / 3.0
    s1 = w_k * k_component + w_d * kl_rpd_d + w_imb * imbalance
    s1 *= 0.5 + 0.5 * np.abs(consistency)
    return np.clip(np.nan_to_num(s1, nan=0.0), -1.0, 1.0)


def compute_s2_spring_compression(kl_rpd_k, kl_rpd_l, iv, close, bb_period=20):
    n = len(close)
    if n < bb_period:
        return np.zeros(n)
    close_series = pd.Series(close)
    sma = close_series.rolling(window=bb_period, min_periods=bb_period).mean().values
    std = close_series.rolling(window=bb_period, min_periods=bb_period).std().values
    bb_width = np.nan_to_num(
        2.0 * std / np.where(sma > 0, sma, 1.0), nan=0.0
    )
    p75 = (
        np.nanpercentile(bb_width[bb_width > 0], 75)
        if np.any(bb_width > 0)
        else 1.0
    )
    bb_compression = 1.0 - np.clip(bb_width / (p75 + 1e-8), 0, 1)
    k_neutral = 1.0 - np.abs(kl_rpd_k) / 3.0
    s2 = 0.4 * bb_compression + 0.3 * k_neutral + 0.3 * kl_rpd_l
    close_shifted = pd.Series(close).shift(5).values
    trend_dir = np.where(close_shifted > 0, np.sign(close - close_shifted), 0.0)
    return np.clip(np.nan_to_num(s2 * trend_dir, nan=0.0), -1.0, 1.0)


def compute_s3_box_boundary(kl_rpd_l, kl_rpd_r, close, high, low, lookback=60):
    n = len(close)
    if n < lookback:
        return np.zeros(n)
    high_series = pd.Series(high)
    low_series = pd.Series(low)
    rolling_high = high_series.rolling(window=lookback, min_periods=lookback).max().values
    rolling_low = low_series.rolling(window=lookback, min_periods=lookback).min().values
    rng = rolling_high - rolling_low
    pos = np.where(rng > 1e-8, (close - rolling_low) / rng, 0.5)
    s3 = np.zeros(n)
    high_pos_mask = pos > 0.8
    low_pos_mask = pos < 0.2
    s3[high_pos_mask] = -(pos[high_pos_mask] - 0.5) * 2.0
    s3[low_pos_mask] = (0.5 - pos[low_pos_mask]) * 2.0
    s3 *= 0.5 + 0.3 * kl_rpd_l + 0.2 * (1.0 - np.abs(kl_rpd_r))
    return np.clip(np.nan_to_num(s3, nan=0.0), -1.0, 1.0)


def compute_s4_hf_momentum(kl_rpd_k, kl_rpd_r, imbalance, volume, short_period=5):
    n = len(volume)
    if n < short_period:
        return np.zeros(n)
    volume_series = pd.Series(volume)
    avg_vol = volume_series.rolling(window=short_period, min_periods=short_period).mean().values
    vol_ratio = np.where(avg_vol > 0, volume / avg_vol, 1.0)
    s4 = (
        0.35 * np.sign(kl_rpd_k) * np.abs(kl_rpd_k) / 3.0
        + 0.35 * kl_rpd_r
        + 0.30 * imbalance
    )
    s4 *= np.clip(vol_ratio / 2.0, 0.5, 2.0)
    return np.clip(np.nan_to_num(s4, nan=0.0), -1.0, 1.0)


def compute_s5_cross_period_confirmation(s1, s2, s3, s4):
    signals = np.stack([s1, s2, s3, s4], axis=0)
    signs = np.sign(signals)
    magnitudes = np.abs(signals)
    sign_consistency = np.abs(np.mean(signs, axis=0))
    avg_magnitude = np.mean(magnitudes, axis=0)
    return np.clip(
        np.nan_to_num(sign_consistency * avg_magnitude, nan=0.0), 0.0, 1.0
    )


def compute_s6_risk_circuit(state_entropy, kl_rpd_l, close, max_drawdown_pct=3.0):
    peak = np.maximum.accumulate(close)
    drawdown = np.where(peak > 0, (peak - close) / peak, 0.0)
    entropy_risk = state_entropy
    liquidity_risk = 1.0 - kl_rpd_l
    drawdown_risk = np.clip(drawdown / (max_drawdown_pct / 100.0), 0.0, 1.0)
    s6 = 0.35 * entropy_risk + 0.30 * liquidity_risk + 0.35 * drawdown_risk
    return np.clip(np.nan_to_num(s6, nan=0.0), 0.0, 1.0)


def compute_signals_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(
            np.zeros((0, 6)),
            columns=[
                "signal_s1",
                "signal_s2",
                "signal_s3",
                "signal_s4",
                "signal_s5",
                "signal_s6",
            ],
        )
    close = df["close"].to_numpy(dtype=np.float64)
    high = (
        df["high"].to_numpy(dtype=np.float64)
        if "high" in df.columns
        else close
    )
    low = (
        df["low"].to_numpy(dtype=np.float64)
        if "low" in df.columns
        else close
    )
    volume = df["volume"].to_numpy(dtype=np.float64)
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
    iv = (
        df["iv"].to_numpy(dtype=np.float64)
        if "iv" in df.columns
        else np.full(n, 0.2)
    )
    kl_rpd_k = (
        df["kl_rpd_k"].to_numpy(dtype=np.float64)
        if "kl_rpd_k" in df.columns
        else np.zeros(n)
    )
    kl_rpd_l = (
        df["kl_rpd_l"].to_numpy(dtype=np.float64)
        if "kl_rpd_l" in df.columns
        else np.full(n, 0.5)
    )
    kl_rpd_r = (
        df["kl_rpd_r"].to_numpy(dtype=np.float64)
        if "kl_rpd_r" in df.columns
        else np.zeros(n)
    )
    kl_rpd_d = (
        df["kl_rpd_d"].to_numpy(dtype=np.float64)
        if "kl_rpd_d" in df.columns
        else np.zeros(n)
    )
    state_entropy = (
        df["state_entropy"].to_numpy(dtype=np.float64)
        if "state_entropy" in df.columns
        else np.full(n, 0.5)
    )

    s1 = compute_s1_trend_resonance(kl_rpd_k, kl_rpd_d, imbalance, consistency)
    s2 = compute_s2_spring_compression(kl_rpd_k, kl_rpd_l, iv, close)
    s3 = compute_s3_box_boundary(kl_rpd_l, kl_rpd_r, close, high, low)
    s4 = compute_s4_hf_momentum(kl_rpd_k, kl_rpd_r, imbalance, volume)
    s5 = compute_s5_cross_period_confirmation(s1, s2, s3, s4)
    s6 = compute_s6_risk_circuit(state_entropy, kl_rpd_l, close)
    return pd.DataFrame(
        {
            "signal_s1": s1,
            "signal_s2": s2,
            "signal_s3": s3,
            "signal_s4": s4,
            "signal_s5": s5,
            "signal_s6": s6,
        }
    )