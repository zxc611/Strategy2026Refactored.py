# MODULE_ID: M1-155
"""Multi-period trend score vectorized pre-computation module."""
from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


def compute_trend_scores_vectorized(
    close: np.ndarray,
    periods: Tuple[int, int, int] = (20, 60, 240),
) -> Tuple[np.ndarray, np.ndarray]:
    n = len(close)
    n_periods = len(periods)
    trend_scores = np.zeros((n, n_periods))
    trend_directions = np.zeros((n, n_periods), dtype=np.int32)
    
    close_series = pd.Series(close)
    for p_idx, period in enumerate(periods):
        if n < period:
            continue
        
        rolling_mean = close_series.rolling(window=period, min_periods=period).mean().values
        rolling_sum = close_series.rolling(window=period, min_periods=period).sum().values
        
        x = np.arange(period, dtype=np.float64)
        x_mean = (period - 1) / 2.0
        x_centered = x - x_mean
        denom = np.sum(x_centered ** 2)
        
        if denom < 1e-10:
            continue
        
        from numpy.lib.stride_tricks import sliding_window_view
        windows = sliding_window_view(close, period)
        
        y_means = rolling_mean[period:]
        xy_sums = np.sum(windows * x_centered, axis=1) - y_means * np.sum(x_centered)
        slopes = xy_sums / denom
        
        scores = np.clip(slopes / (y_means * 0.001 + 1e-8), -1.0, 1.0)
        
        trend_scores[period:, p_idx] = scores
        trend_directions[period:, p_idx] = np.sign(scores).astype(np.int32)
    
    return trend_scores, trend_directions