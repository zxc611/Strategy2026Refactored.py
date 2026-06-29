# MODULE_ID: M1-180
"""Overbought/oversold vectorized pre-computation module."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


def compute_rsi(close, period=14):
    n = len(close)
    if n < period + 1:
        return np.full(n, 50.0)
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    gain_series = pd.Series(gain)
    loss_series = pd.Series(loss)
    alpha = 1.0 / period
    avg_gain = gain_series.ewm(alpha=alpha, adjust=False).mean().values
    avg_loss = loss_series.ewm(alpha=alpha, adjust=False).mean().values
    avg_gain[:period] = np.mean(gain[:period])
    avg_loss[:period] = np.mean(loss[:period])
    rs = np.where(avg_loss > 1e-10, avg_gain / avg_loss, 100.0)
    return np.nan_to_num(100.0 - 100.0 / (1.0 + rs), nan=50.0)


def compute_stochastic_k(close, high, low, period=14):
    n = len(close)
    if n < period:
        return np.full(n, 50.0)
    high_series = pd.Series(high)
    low_series = pd.Series(low)
    close_series = pd.Series(close)
    rolling_high = high_series.rolling(window=period, min_periods=period).max().values
    rolling_low = low_series.rolling(window=period, min_periods=period).min().values
    range_val = rolling_high - rolling_low
    stoch_k = np.where(range_val > 1e-10, 100.0 * (close - rolling_low) / range_val, 50.0)
    stoch_k[:period-1] = 50.0
    return stoch_k


def compute_cci(close, high, low, period=20):
    n = len(close)
    if n < period:
        return np.zeros(n)
    tp = (high + low + close) / 3.0
    tp_series = pd.Series(tp)
    rolling_mean = tp_series.rolling(window=period, min_periods=period).mean().values
    rolling_md = tp_series.rolling(window=period, min_periods=period).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True).values
    cci = np.where(rolling_md > 1e-10, (tp - rolling_mean) / (0.015 * rolling_md), 0.0)
    cci[:period-1] = 0.0
    return cci


def compute_williams_r(close, high, low, period=14):
    n = len(close)
    if n < period:
        return np.full(n, -50.0)
    high_series = pd.Series(high)
    low_series = pd.Series(low)
    rolling_high = high_series.rolling(window=period, min_periods=period).max().values
    rolling_low = low_series.rolling(window=period, min_periods=period).min().values
    range_val = rolling_high - rolling_low
    wr = np.where(range_val > 1e-10, -100.0 * (rolling_high - close) / range_val, -50.0)
    wr[:period-1] = -50.0
    return wr


def compute_obos_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(
            np.zeros((0, 5)),
            columns=[
                "obos_rsi",
                "obos_stoch_k",
                "obos_cci",
                "obos_williams_r",
                "obos_signal",
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
    rsi = compute_rsi(close)
    stoch_k = compute_stochastic_k(close, high, low)
    cci = compute_cci(close, high, low)
    wr = compute_williams_r(close, high, low)
    rsi_norm = (rsi - 50.0) / 50.0
    stoch_norm = (stoch_k - 50.0) / 50.0
    cci_norm = np.clip(cci / 200.0, -1.0, 1.0)
    wr_norm = (wr + 50.0) / 50.0
    obos_signal = (rsi_norm + stoch_norm + cci_norm + wr_norm) / 4.0
    return pd.DataFrame(
        {
            "obos_rsi": rsi,
            "obos_stoch_k": stoch_k,
            "obos_cci": cci,
            "obos_williams_r": wr,
            "obos_signal": np.clip(obos_signal, -1.0, 1.0),
        }
    )