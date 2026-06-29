# MODULE_ID: M1-179
"""L0 state diagnosis & state smoothing vectorized computation module."""
from __future__ import annotations

import logging
from enum import IntEnum

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


class L0State(IntEnum):
    TREND_UP = 0
    TREND_DOWN = 1
    RANGE = 2
    COMPRESSION = 3
    EXHAUSTION = 4
    CHAOS = 5


def compute_l0_raw_state(signal_s1, signal_s2, signal_s4, signal_s6, kl_rpd_k):
    n = len(signal_s1)
    raw_state = np.full(n, L0State.RANGE, dtype=np.int32)
    
    is_chaos = signal_s6 > 0.7
    raw_state[is_chaos] = L0State.CHAOS
    
    is_exhaustion = (~is_chaos) & (np.abs(signal_s1) > 0.2) & (np.abs(signal_s4) > 0.2) & (np.sign(signal_s1) != np.sign(signal_s4))
    raw_state[is_exhaustion] = L0State.EXHAUSTION
    
    is_compression = (~is_chaos) & (~is_exhaustion) & (np.abs(signal_s2) > 0.4)
    raw_state[is_compression] = L0State.COMPRESSION
    
    is_trend_up = (~is_chaos) & (~is_exhaustion) & (~is_compression) & (signal_s1 > 0.3) & (signal_s4 > 0.2) & (kl_rpd_k > 0.5)
    raw_state[is_trend_up] = L0State.TREND_UP
    
    is_trend_down = (~is_chaos) & (~is_exhaustion) & (~is_compression) & (~is_trend_up) & (signal_s1 < -0.3) & (signal_s4 < -0.2) & (kl_rpd_k < -0.5)
    raw_state[is_trend_down] = L0State.TREND_DOWN
    
    return raw_state


def compute_l0_smoothed_state(raw_state, smoothing_alpha=0.15, min_duration=3):
    n = len(raw_state)
    n_states = len(L0State)
    state_probs = np.zeros((n, n_states))
    state_probs[np.arange(n), raw_state] = 1.0
    
    state_probs_df = pd.DataFrame(state_probs)
    smoothed_probs = state_probs_df.ewm(alpha=smoothing_alpha, adjust=False).mean().values
    
    smoothed_state = np.argmax(smoothed_probs, axis=1).astype(np.int32)
    
    if n < 2:
        return smoothed_state
    
    state_changes = np.concatenate([[False], smoothed_state[1:] != smoothed_state[:-1]])
    change_indices = np.where(state_changes)[0]
    
    if len(change_indices) == 0:
        return smoothed_state
    
    run_starts = np.concatenate([[0], change_indices])
    run_lengths = np.concatenate([change_indices, [n]]) - run_starts
    
    short_mask = (run_lengths < min_duration) & (np.arange(len(run_starts)) > 0)
    for idx in np.where(short_mask)[0]:
        smoothed_state[run_starts[idx]:run_starts[idx] + run_lengths[idx]] = smoothed_state[run_starts[idx - 1]]
    
    return smoothed_state


def compute_l0_state_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(
            np.zeros((0, 3), dtype=np.int32),
            columns=["l0_raw_state", "l0_smoothed_state", "l0_state_entropy"],
        )
    signal_s1 = (
        df["signal_s1"].to_numpy(dtype=np.float64)
        if "signal_s1" in df.columns
        else np.zeros(n)
    )
    signal_s2 = (
        df["signal_s2"].to_numpy(dtype=np.float64)
        if "signal_s2" in df.columns
        else np.zeros(n)
    )
    signal_s4 = (
        df["signal_s4"].to_numpy(dtype=np.float64)
        if "signal_s4" in df.columns
        else np.zeros(n)
    )
    signal_s6 = (
        df["signal_s6"].to_numpy(dtype=np.float64)
        if "signal_s6" in df.columns
        else np.zeros(n)
    )
    kl_rpd_k = (
        df["kl_rpd_k"].to_numpy(dtype=np.float64)
        if "kl_rpd_k" in df.columns
        else np.zeros(n)
    )
    raw_state = compute_l0_raw_state(signal_s1, signal_s2, signal_s4, signal_s6, kl_rpd_k)
    smoothed_state = compute_l0_smoothed_state(raw_state)
    entropy = np.full(n, 0.5)
    window = 20
    if n > window:
        from numpy.lib.stride_tricks import sliding_window_view
        state_windows = sliding_window_view(raw_state, window)
        transitions = np.sum(state_windows[:, 1:] != state_windows[:, :-1], axis=1)
        entropy[window - 1:] = transitions / (window - 1)
    return pd.DataFrame(
        {
            "l0_raw_state": raw_state,
            "l0_smoothed_state": smoothed_state,
            "l0_state_entropy": entropy.astype(np.float64),
        }
    )