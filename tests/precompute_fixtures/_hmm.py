# MODULE_ID: M1-145
"""HMM state vectorized pre-computation module.

**IMPORTANT — This is a SIMPLIFIED volatility-clustering classifier, NOT a
true Hidden Markov Model.**  It assigns 3 states (LOW_VOL / NORMAL / HIGH_VOL)
based on rolling realized-volatility percentiles combined with implied
volatility.  A future upgrade should replace this with a proper HMM
implementation using hmmlearn (GaussianHMM + Baum-Welch + Viterbi).

Upgrade path:
  - Replace the body of compute_hmm_state_vectorized() with hmmlearn calls
  - Keep the same function signature and return types
  - The posterior probabilities should come from the forward-backward algorithm
"""
from __future__ import annotations

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger

logger = get_logger(__name__)


def compute_hmm_state_vectorized(
    close: np.ndarray,
    volume: np.ndarray,
    iv: np.ndarray,
    n_states: int = 3,
    window: int = 60,
    hmm_model: Optional[object] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Compute HMM-like state labels (simplified: volatility clustering).

    Parameters
    ----------
    hmm_model : optional pre-trained hmmlearn GaussianHMM instance.
        If provided, use it for inference instead of the percentile classifier.
        This enables a drop-in upgrade path without changing callers.
    """
    n = len(close)
    hmm_state = np.full(n, 1, dtype=np.int32)
    hmm_posterior = np.full((n, 3), [0.2, 0.6, 0.2])
    if n < window:
        return hmm_state, hmm_posterior

    if hmm_model is not None:
        return _infer_with_hmmlearn(hmm_model, close, volume, iv, n)

    log_ret = np.zeros(n)
    log_ret[1:] = np.log(
        close[1:] / np.where(close[:-1] > 0, close[:-1], 1.0)
    )
    log_ret = np.nan_to_num(log_ret, nan=0.0)
    log_ret_series = pd.Series(log_ret)
    rv = log_ret_series.rolling(window=window, min_periods=window).std(ddof=1).values * np.sqrt(252)

    rv_valid = rv[window:]
    if len(rv_valid) > 10:
        rv_33 = np.nanpercentile(rv_valid, 33)
        rv_67 = np.nanpercentile(rv_valid, 67)
        for i in range(window, n):
            rv_i = rv[i] if np.isfinite(rv[i]) else 0.2
            iv_i = iv[i] if np.isfinite(iv[i]) else 0.2
            vol_indicator = 0.6 * rv_i + 0.4 * iv_i
            if vol_indicator < rv_33:
                hmm_state[i] = 0
                hmm_posterior[i] = [0.7, 0.25, 0.05]
            elif vol_indicator > rv_67:
                hmm_state[i] = 2
                hmm_posterior[i] = [0.05, 0.25, 0.7]
            else:
                hmm_state[i] = 1
                hmm_posterior[i] = [0.15, 0.7, 0.15]
    return hmm_state, hmm_posterior


def _infer_with_hmmlearn(
    model, close, volume, iv, n
) -> Tuple[np.ndarray, np.ndarray]:
    """Inference using a pre-trained hmmlearn GaussianHMM model.

    The model must have .predict() and .predict_proba() methods.
    """
    log_ret = np.zeros(n)
    log_ret[1:] = np.log(
        close[1:] / np.where(close[:-1] > 0, close[:-1], 1.0)
    )
    log_ret = np.nan_to_num(log_ret, nan=0.0)
    obs = np.column_stack([log_ret, np.nan_to_num(iv, nan=0.2)])
    try:
        hmm_state = model.predict(obs).astype(np.int32)
        hmm_posterior = model.predict_proba(obs)
        if hmm_posterior.shape[1] != 3:
            padded = np.full((n, 3), 1.0 / 3)
            padded[:, : hmm_posterior.shape[1]] = hmm_posterior
            hmm_posterior = padded
    except (ValueError, RuntimeError, AttributeError) as exc:
        logger.warning("hmmlearn inference failed, fallback to simplified: %s", exc)
        hmm_state = np.full(n, 1, dtype=np.int32)
        hmm_posterior = np.full((n, 3), [0.2, 0.6, 0.2])
    return hmm_state, hmm_posterior