# MODULE_ID: M1-178
"""KL-RPD four-dimensional vectorized computation module."""
from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


def compute_keltner_deviation(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    ema_period: int = 20,
    atr_period: int = 14,
    atr_mult: float = 2.0,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = len(close)
    if n < max(ema_period, atr_period) + 1:
        return np.zeros(n), np.full(n, np.nan), np.full(n, np.nan)

    close_series = pd.Series(close)
    ema = close_series.ewm(span=ema_period, adjust=False).mean().values

    tr = np.maximum(
        high - low,
        np.maximum(
            np.abs(high - np.roll(close, 1)),
            np.abs(low - np.roll(close, 1)),
        ),
    )
    tr[0] = high[0] - low[0]
    tr_series = pd.Series(tr)
    atr = tr_series.ewm(span=atr_period, adjust=False).mean().values

    atr_safe = np.where(np.isfinite(atr) & (atr > 1e-8), atr, 1e-8)
    keltner_dev = (close - ema) / (atr_mult * atr_safe)
    keltner_dev = np.clip(np.nan_to_num(keltner_dev, nan=0.0), -3.0, 3.0)
    return keltner_dev, ema, atr


def compute_liquidity_depth(
    bid_price1: np.ndarray,
    ask_price1: np.ndarray,
    bid_vol1: np.ndarray,
    ask_vol1: np.ndarray,
    volume: np.ndarray,
) -> np.ndarray:
    n = len(volume)
    bid_vol = np.nan_to_num(bid_vol1, nan=0.0).astype(np.float64)
    ask_vol = np.nan_to_num(ask_vol1, nan=0.0).astype(np.float64)
    vol = np.nan_to_num(volume, nan=0.0).astype(np.float64)
    total_depth = bid_vol + ask_vol
    avg_volume = vol + 1.0
    raw_depth = total_depth / (2.0 * avg_volume)
    window = min(60, n)
    if n < window:
        return np.clip(raw_depth, 0.0, 1.0)
    raw_depth_series = pd.Series(raw_depth)
    rolling_rank = raw_depth_series.rolling(window=window, min_periods=window).apply(
        lambda x: np.searchsorted(np.sort(x), x.iloc[-1]) / len(x), raw=True
    ).values
    liquidity = np.clip(rolling_rank, 0.0, 1.0)
    liquidity[:window] = 0.5
    return liquidity


def compute_risk_premium(
    iv: np.ndarray,
    close: np.ndarray,
    rv_period: int = 20,
) -> np.ndarray:
    n = len(close)
    iv_safe = np.nan_to_num(iv, nan=0.2).astype(np.float64)
    log_ret = np.zeros(n)
    log_ret[1:] = np.log(
        close[1:] / np.where(close[:-1] > 0, close[:-1], 1.0)
    )
    log_ret = np.nan_to_num(log_ret, nan=0.0)
    rv = np.full(n, np.nan)
    if n >= rv_period:
        cumsum = np.cumsum(log_ret**2)
        cumsum_shifted = np.concatenate([[0], cumsum[:-rv_period]])
        rv[rv_period - 1:] = np.sqrt((cumsum[rv_period - 1:] - cumsum_shifted) / rv_period) * np.sqrt(252)
    rv_safe = np.nan_to_num(rv, nan=iv_safe)
    denom = np.maximum(np.maximum(iv_safe, np.abs(rv_safe)), 1e-8)
    risk_premium = (iv_safe - rv_safe) / denom
    return np.clip(np.nan_to_num(risk_premium, nan=0.0), -1.0, 1.0)


def compute_price_divergence(
    close: np.ndarray,
    rsi_period: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> np.ndarray:
    n = len(close)
    if n < macd_slow + macd_signal:
        return np.zeros(n)
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    alpha_rsi = 2.0 / (rsi_period + 1)
    avg_gain = np.empty(n)
    avg_loss = np.empty(n)
    avg_gain[:rsi_period] = np.nan
    avg_loss[:rsi_period] = np.nan
    avg_gain[rsi_period - 1] = np.mean(gain[:rsi_period])
    avg_loss[rsi_period - 1] = np.mean(loss[:rsi_period])
    
    gain_series = pd.Series(gain[rsi_period - 1:])
    loss_series = pd.Series(loss[rsi_period - 1:])
    avg_gain[rsi_period - 1:] = gain_series.ewm(alpha=alpha_rsi, adjust=False).mean().values
    avg_loss[rsi_period - 1:] = loss_series.ewm(alpha=alpha_rsi, adjust=False).mean().values
    rs = np.where(avg_loss > 1e-10, avg_gain / avg_loss, 100.0)
    rsi = np.nan_to_num(100.0 - 100.0 / (1.0 + rs), nan=50.0)
    trend_window = 5
    price_trend = np.zeros(n)
    rsi_trend = np.zeros(n)
    if n > trend_window:
        from numpy.lib.stride_tricks import sliding_window_view
        x = np.arange(trend_window, dtype=np.float64)
        x_mean = (trend_window - 1) / 2.0
        x_centered = x - x_mean
        denom = np.sum(x_centered ** 2)
        
        if denom > 1e-10:
            price_windows = sliding_window_view(close, trend_window)
            rsi_windows = sliding_window_view(rsi, trend_window)
            
            price_y_means = np.mean(price_windows, axis=1)
            rsi_y_means = np.mean(rsi_windows, axis=1)
            
            price_xy = np.sum(price_windows * x_centered, axis=1) - price_y_means * np.sum(x_centered)
            rsi_xy = np.sum(rsi_windows * x_centered, axis=1) - rsi_y_means * np.sum(x_centered)
            
            price_slopes = price_xy / denom
            rsi_slopes = rsi_xy / denom
            
            price_trend[trend_window - 1:] = np.sign(price_slopes)
            rsi_trend[trend_window - 1:] = np.sign(rsi_slopes)
    divergence = np.where(
        price_trend * rsi_trend < 0,
        -np.abs(price_trend - rsi_trend) * 0.5,
        np.abs(price_trend + rsi_trend) * 0.5,
    )
    return np.clip(np.nan_to_num(divergence, nan=0.0), -1.0, 1.0)


def compute_kl_rpd_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(
            np.zeros((0, 4)),
            columns=["kl_rpd_k", "kl_rpd_l", "kl_rpd_r", "kl_rpd_d"],
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
    k_dev, _, _ = compute_keltner_deviation(close, high, low)
    if all(c in df.columns for c in ["bid_price1", "ask_price1"]):
        bid_v = df.get("bid_vol1", pd.Series(np.zeros(n))).to_numpy(
            dtype=np.float64
        )
        ask_v = df.get("ask_vol1", pd.Series(np.zeros(n))).to_numpy(
            dtype=np.float64
        )
        vol = df["volume"].to_numpy(dtype=np.float64)
        liquidity = compute_liquidity_depth(
            df["bid_price1"].to_numpy(dtype=np.float64),
            df["ask_price1"].to_numpy(dtype=np.float64),
            bid_v,
            ask_v,
            vol,
        )
    elif "bid_ask_spread" in df.columns:
        spread = df["bid_ask_spread"].to_numpy(dtype=np.float64)
        close_safe = np.where(close > 0, close, 1.0)
        liquidity = 1.0 - np.clip(spread / close_safe, 0.0, 1.0)
    else:
        liquidity = np.full(n, 0.5)
    iv = (
        df["iv"].to_numpy(dtype=np.float64)
        if "iv" in df.columns
        else np.full(n, 0.2)
    )
    risk_premium = compute_risk_premium(iv, close)
    divergence = compute_price_divergence(close)
    return pd.DataFrame(
        {
            "kl_rpd_k": k_dev,
            "kl_rpd_l": liquidity,
            "kl_rpd_r": risk_premium,
            "kl_rpd_d": divergence,
        }
    )