# MODULE_ID: M1-182
"""Signal decay & linkage vectorized computation module.

Decay model: decay(t) = exp(-lambda * delta_t_minutes)
delta_t is always computed from real time differences (not row indices),
so missing bars or irregular spacing are handled correctly.

Overnight protection: when count_trading_minutes_only=True, non-trading gaps
are capped at a configurable ceiling (default 60 min) so that signals do not
fully decay across overnight breaks.

Fix history:
  - I-4: replaced index-based age with time-delta-based age to handle
    missing bars / irregular spacing correctly
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from infra._helpers import get_logger

logger = get_logger(__name__)

LINKAGE_WEIGHTS = np.array(
    [
        [1.0, 0.3, 0.1, 0.5],
        [0.3, 1.0, 0.4, 0.2],
        [0.1, 0.4, 1.0, 0.1],
        [0.5, 0.2, 0.1, 1.0],
    ]
)


def compute_signal_decay(signals, time_delta_minutes, decay_lambda=0.02):
    decay_factor = np.exp(-decay_lambda * time_delta_minutes)
    decayed = signals * decay_factor
    return np.clip(np.nan_to_num(decayed, nan=0.0), -1.0, 1.0)


def compute_signal_linkage(s1, s2, s3, s4, weights=None):
    if weights is None:
        weights = LINKAGE_WEIGHTS
    n = len(s1)
    signals_matrix = np.stack([s1, s2, s3, s4], axis=1)
    linkage = signals_matrix @ weights.T
    max_possible = np.sum(np.abs(weights), axis=1)
    linkage_normalized = linkage / max_possible[np.newaxis, :]
    return np.clip(np.nan_to_num(linkage_normalized, nan=0.0), -1.0, 1.0)


def _compute_time_deltas_minutes(minutes_array: np.ndarray) -> np.ndarray:
    n = len(minutes_array)
    if n == 0:
        return np.array([], dtype=np.float64)
    try:
        minutes_series = pd.to_datetime(minutes_array, errors="coerce")
        deltas = minutes_series.diff().dt.total_seconds().fillna(60.0).clip(lower=1.0).values / 60.0
        deltas[0] = 1.0
        return deltas.astype(np.float64)
    except (ValueError, TypeError, AttributeError):
        return np.ones(n, dtype=np.float64)


def compute_signal_age_minutes(
    signals,
    minutes_array,
    threshold=0.1,
    count_trading_minutes_only=False,
    overnight_gap_ceiling=60,
):
    """Compute signal age in **real minutes** (not row indices).

    For each bar i, age[i] = total elapsed minutes since the last bar
    where |signal| > threshold.  When count_trading_minutes_only=True,
    any single inter-bar gap > 5 min is capped at overnight_gap_ceiling.
    
    Vectorized implementation using numpy cumsum.
    """
    n = len(signals)
    age = np.zeros(n, dtype=np.float64)
    time_deltas = _compute_time_deltas_minutes(minutes_array)
    
    if count_trading_minutes_only:
        time_deltas = np.where(time_deltas > 5, np.minimum(time_deltas, overnight_gap_ceiling), time_deltas)
    
    active_mask = np.abs(signals) > threshold
    
    if not np.any(active_mask):
        return age
    
    active_indices = np.where(active_mask)[0]
    
    last_active_idx = np.searchsorted(active_indices, np.arange(n), side='right') - 1
    last_active_idx = np.where(last_active_idx >= 0, active_indices[last_active_idx], -1)
    
    cumsum_time = np.concatenate([[0], np.cumsum(time_deltas)])
    
    age = np.where(
        (last_active_idx == -1) | (last_active_idx == np.arange(n)),
        0.0,
        cumsum_time[np.arange(n) + 1] - cumsum_time[last_active_idx + 1]
    )
    
    return age


def compute_decay_and_linkage_vectorized(
    df: pd.DataFrame,
    count_trading_minutes_only: bool = False,
    overnight_gap_ceiling: int = 60,
) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        cols = [
            "signal_s1_decayed",
            "signal_s2_decayed",
            "signal_s3_decayed",
            "signal_s4_decayed",
            "linkage_s1",
            "linkage_s2",
            "linkage_s3",
            "linkage_s4",
        ]
        return pd.DataFrame(np.zeros((0, 8)), columns=cols)

    s1 = (
        df["signal_s1"].to_numpy(dtype=np.float64)
        if "signal_s1" in df.columns
        else np.zeros(n)
    )
    s2 = (
        df["signal_s2"].to_numpy(dtype=np.float64)
        if "signal_s2" in df.columns
        else np.zeros(n)
    )
    s3 = (
        df["signal_s3"].to_numpy(dtype=np.float64)
        if "signal_s3" in df.columns
        else np.zeros(n)
    )
    s4 = (
        df["signal_s4"].to_numpy(dtype=np.float64)
        if "signal_s4" in df.columns
        else np.zeros(n)
    )

    if "minute" not in df.columns:
        minutes_array = np.arange(n, dtype=np.float64)
    else:
        minutes_array = df["minute"].to_numpy()

    age_s1 = compute_signal_age_minutes(
        s1, minutes_array,
        count_trading_minutes_only=count_trading_minutes_only,
        overnight_gap_ceiling=overnight_gap_ceiling,
    )
    age_s2 = compute_signal_age_minutes(
        s2, minutes_array,
        count_trading_minutes_only=count_trading_minutes_only,
        overnight_gap_ceiling=overnight_gap_ceiling,
    )
    age_s3 = compute_signal_age_minutes(
        s3, minutes_array,
        count_trading_minutes_only=count_trading_minutes_only,
        overnight_gap_ceiling=overnight_gap_ceiling,
    )
    age_s4 = compute_signal_age_minutes(
        s4, minutes_array,
        count_trading_minutes_only=count_trading_minutes_only,
        overnight_gap_ceiling=overnight_gap_ceiling,
    )
    s1_decayed = compute_signal_decay(s1, age_s1)
    s2_decayed = compute_signal_decay(s2, age_s2)
    s3_decayed = compute_signal_decay(s3, age_s3)
    s4_decayed = compute_signal_decay(s4, age_s4)
    linkage = compute_signal_linkage(s1, s2, s3, s4)

    return pd.DataFrame(
        {
            "signal_s1_decayed": s1_decayed,
            "signal_s2_decayed": s2_decayed,
            "signal_s3_decayed": s3_decayed,
            "signal_s4_decayed": s4_decayed,
            "linkage_s1": linkage[:, 0],
            "linkage_s2": linkage[:, 1],
            "linkage_s3": linkage[:, 2],
            "linkage_s4": linkage[:, 3],
        }
    )
