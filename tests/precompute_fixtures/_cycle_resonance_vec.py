# MODULE_ID: M1-142
"""Cycle resonance vectorized adapter — batch-replay CRM without per-row Python calls.

This module extracts the three sub-computations from CycleResonanceModule
(directional_bias, resonance_strength, phase) into numpy vectorized functions
that produce **identical** results to the per-row CRM.update() for the
stateless components (directional_bias, resonance_strength).

The stateful components (state_entropy, cr_phase) use rolling-window numpy
operations that approximate the CRM's deque-based history.  These have
path-dependent divergence from the original but produce consistent distributions.

Fix history:
  - I-5: corrected _vectorized_resonance_strength to match CRM's per-row
    scalar logic exactly (sign_agreement over 3 elements, not cross-row)
  - I-5: corrected _vectorized_state_entropy to use hmm_state string history
    matching CRM's _hmm_state_history deque semantics
  - I-5: added end-to-end equivalence test confirming bias/strength exact match
"""
from __future__ import annotations

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from infra._helpers import get_logger

logger = get_logger(__name__)

_PHASE_CHARGE = 0
_PHASE_RELEASE = 1
_PHASE_EXHAUST = 2
_PHASE_CHAOS = 3


def _vectorized_directional_bias(
    trend_scores: np.ndarray,
    imbalance: np.ndarray,
    trend_weight_short: float = 0.2,
    trend_weight_medium: float = 0.5,
    imbalance_coeff: float = 0.3,
) -> np.ndarray:
    weights = np.array(
        [trend_weight_short, trend_weight_medium, 1.0 - trend_weight_short - trend_weight_medium]
    )
    trend_component = np.sum(weights * np.sign(trend_scores) * np.abs(trend_scores), axis=1)
    imbalance_component = imbalance_coeff * imbalance
    return np.clip(trend_component + imbalance_component, -1.0, 1.0)


def _vectorized_resonance_strength(
    trend_scores: np.ndarray,
    strength: np.ndarray,
    hmm_posterior: np.ndarray,
    consistency_sign_weight: float = 0.5,
    consistency_mag_weight: float = 0.5,
    hmm_stability_coeff: float = 0.5,
) -> np.ndarray:
    n = len(strength)
    abs_scores = np.abs(trend_scores)
    sum_abs = np.sum(abs_scores, axis=1)

    consistency = np.ones(n)
    mask = sum_abs > 0.01
    if np.any(mask):
        signs = np.sign(trend_scores[mask])
        first_sign = signs[:, 0:1]
        sign_agreement = np.mean(signs == first_sign, axis=1)
        mean_abs = np.mean(abs_scores[mask], axis=1)
        std_abs = np.std(abs_scores[mask], axis=1)
        magnitude_balance = np.clip(1.0 - std_abs / (mean_abs + 1e-8), 0.0, 1.0)
        consistency[mask] = consistency_sign_weight * sign_agreement + consistency_mag_weight * magnitude_balance

    hmm_stability = 1.0 - 2.0 * np.min(hmm_posterior, axis=1)
    hmm_stability = np.clip(hmm_stability, 0.0, 1.0)

    res = consistency * strength * (hmm_stability_coeff + (1.0 - hmm_stability_coeff) * hmm_stability)
    return np.clip(res, 0.0, 1.0)


def _vectorized_state_entropy(
    hmm_state: np.ndarray, window: int = 20
) -> np.ndarray:
    n = len(hmm_state)
    entropy = np.full(n, 0.5)
    if n < 2:
        return entropy
    transitions = (hmm_state[1:] != hmm_state[:-1]).astype(np.float64)
    cum_trans = np.cumsum(transitions)
    cum_trans = np.insert(cum_trans, 0, 0.0)
    if n > window:
        entropy[window:] = (cum_trans[window:] - cum_trans[:-window]) / window
    entropy[:min(window, n)] = 0.5
    return np.clip(entropy, 0.0, 1.0)


def _vectorized_phase(
    resonance_strength: np.ndarray,
    state_entropy: np.ndarray,
    hmm_state: np.ndarray,
    directional_bias: np.ndarray,
    chaos_entropy_threshold: float = 0.7,
    phase_transition_threshold: float = 0.3,
    release_strength_threshold: float = 0.5,
    release_bias_threshold: float = 0.3,
    exhaust_strength_threshold: float = 0.2,
    exhaust_highvol_threshold: float = 0.4,
    secondary_chaos_entropy: float = 0.4,
    strength_trend_release_threshold: float = 0.05,
    phase_params: Optional[object] = None,
) -> np.ndarray:
    if phase_params is not None:
        chaos_entropy_threshold = getattr(phase_params, 'chaos_entropy_threshold', chaos_entropy_threshold)
        phase_transition_threshold = getattr(phase_params, 'phase_transition_threshold', phase_transition_threshold)
        release_strength_threshold = getattr(phase_params, 'release_strength_threshold', release_strength_threshold)
        release_bias_threshold = getattr(phase_params, 'release_bias_threshold', release_bias_threshold)
        exhaust_strength_threshold = getattr(phase_params, 'exhaust_strength_threshold', exhaust_strength_threshold)
        exhaust_highvol_threshold = getattr(phase_params, 'exhaust_highvol_threshold', exhaust_highvol_threshold)
        secondary_chaos_entropy = getattr(phase_params, 'secondary_chaos_entropy', secondary_chaos_entropy)
        strength_trend_release_threshold = getattr(phase_params, 'strength_trend_release_threshold', strength_trend_release_threshold)
    n = len(resonance_strength)
    phase = np.full(n, _PHASE_CHARGE, dtype=np.int32)

    strength_trend = np.zeros(n)
    if n >= 3:
        strength_trend[2:] = resonance_strength[2:] - resonance_strength[:-2]

    is_chaos_primary = state_entropy > chaos_entropy_threshold
    phase[is_chaos_primary] = _PHASE_CHAOS

    is_low_vol_charge = (~is_chaos_primary) & (hmm_state == 0) & (resonance_strength < phase_transition_threshold)
    phase[is_low_vol_charge] = _PHASE_CHARGE

    is_release = (
        (~is_chaos_primary)
        & (resonance_strength > release_strength_threshold)
        & (np.abs(directional_bias) > release_bias_threshold)
        & (strength_trend >= 0)
    )
    phase[is_release & (phase == _PHASE_CHARGE)] = _PHASE_RELEASE

    is_exhaust = (
        (phase == _PHASE_CHARGE)
        & (resonance_strength < exhaust_strength_threshold)
    ) | (
        (hmm_state == 2)
        & (resonance_strength < exhaust_highvol_threshold)
        & (phase == _PHASE_CHARGE)
    )
    phase[is_exhaust] = _PHASE_EXHAUST

    is_chaos_secondary = (phase == _PHASE_CHARGE) & (state_entropy > secondary_chaos_entropy)
    phase[is_chaos_secondary] = _PHASE_CHAOS

    is_release_trend = (phase == _PHASE_CHARGE) & (strength_trend > strength_trend_release_threshold)
    phase[is_release_trend] = _PHASE_RELEASE

    return phase


def compute_cycle_resonance_vectorized(
    df: pd.DataFrame,
    hmm_state_col: str = "hmm_state",
    hmm_posterior_cols: Tuple[str, str, str] = ("hmm_posterior_low", "hmm_posterior_normal", "hmm_posterior_high"),
    trend_score_cols: Tuple[str, str, str] = ("trend_score_short", "trend_score_medium", "trend_score_long"),
    strength_col: str = "strength",
    imbalance_col: str = "imbalance",
    phase_params: Optional[object] = None,
) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(
            np.zeros((0, 4)),
            columns=["directional_bias", "resonance_strength", "cr_phase", "state_entropy"],
        )

    hmm_state = df[hmm_state_col].to_numpy(dtype=np.int32) if hmm_state_col in df.columns else np.ones(n, dtype=np.int32)
    hmm_posterior = np.column_stack([df[c].to_numpy(dtype=np.float64) for c in hmm_posterior_cols]) if all(c in df.columns for c in hmm_posterior_cols) else np.full((n, 3), [0.2, 0.6, 0.2])
    trend_scores = np.column_stack([df[c].to_numpy(dtype=np.float64) for c in trend_score_cols]) if all(c in df.columns for c in trend_score_cols) else np.zeros((n, 3))
    strength = df[strength_col].to_numpy(dtype=np.float64) if strength_col in df.columns else np.zeros(n)
    imbalance = df[imbalance_col].to_numpy(dtype=np.float64) if imbalance_col in df.columns else np.zeros(n)

    directional_bias = _vectorized_directional_bias(trend_scores, imbalance)
    resonance_strength = _vectorized_resonance_strength(trend_scores, strength, hmm_posterior)
    state_entropy = _vectorized_state_entropy(hmm_state)
    cr_phase = _vectorized_phase(
        resonance_strength, state_entropy, hmm_state, directional_bias,
        phase_params=phase_params,
    )

    return pd.DataFrame({
        "directional_bias": directional_bias,
        "resonance_strength": resonance_strength,
        "cr_phase": cr_phase,
        "state_entropy": state_entropy,
    })
