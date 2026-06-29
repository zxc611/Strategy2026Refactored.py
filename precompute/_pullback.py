# MODULE_ID: M1-151
"""Pullback indicator vectorized pre-computation module.

ATR support level: atr_support_level = close - 2.0 * ATR
  Represents the price level 2 ATR units below current close, commonly used
  as a dynamic support threshold. If price falls below this level, the
  pullback is considered significant (2x average true range from current price).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


def compute_pullback_vectorized(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    ma_period: int = 20,
    atr_period: int = 14,
) -> pd.DataFrame:
    n = len(close)
    if n == 0:
        return pd.DataFrame()
    peak = np.maximum.accumulate(high)
    pullback_peak = np.where(peak > 0, (peak - close) / peak, 0.0)
    close_series = pd.Series(close)
    entry_price = close_series.shift(ma_period).values
    entry_price[:ma_period] = close[0]
    pullback_entry = np.where(
        np.isfinite(entry_price) & (entry_price > 0),
        (entry_price - close) / entry_price,
        0.0,
    )
    ma = close_series.rolling(window=ma_period, min_periods=ma_period).mean().values
    pullback_ma = np.where(
        np.isfinite(ma) & (ma > 0), (ma - close) / ma, 0.0
    )
    tr = np.maximum(
        high - low,
        np.maximum(
            np.abs(high - np.roll(close, 1)),
            np.abs(low - np.roll(close, 1)),
        ),
    )
    tr[0] = high[0] - low[0]
    tr_series = pd.Series(tr)
    atr = tr_series.rolling(window=atr_period, min_periods=atr_period).mean().values
    atr_support_level = close - 2.0 * np.nan_to_num(atr, nan=0.0)
    pullback_atr = np.where(atr_support_level > 0, (atr_support_level - close) / atr_support_level, 0.0)
    pullback_atr = np.clip(pullback_atr, -1.0, 0.5)
    return pd.DataFrame(
        {
            "pullback_pct_peak": np.clip(
                np.nan_to_num(pullback_peak, nan=0.0), 0.0, 1.0
            ),
            "pullback_pct_entry": np.clip(
                np.nan_to_num(pullback_entry, nan=0.0), -0.5, 0.5
            ),
            "pullback_pct_ma": np.clip(
                np.nan_to_num(pullback_ma, nan=0.0), -0.5, 0.5
            ),
            "pullback_pct_atr": np.clip(
                np.nan_to_num(pullback_atr, nan=0.0), -1.0, 0.5
            ),
        }
    )