from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _infer_trend_scores_from_bar(bar: pd.Series):
    strength = bar.get("strength", 0.0)
    imbalance = bar.get("imbalance", 0.0)
    direction = 1.0 if imbalance > 0 else -1.0
    short_score = direction * min(abs(imbalance) * 2, 1.0)
    medium_score = direction * min(strength * 2, 1.0)
    long_score = direction * min((strength + abs(imbalance)) * 0.5, 1.0)
    scores = (short_score, medium_score, long_score)
    directions = (np.sign(short_score), np.sign(medium_score), np.sign(long_score))
    return scores, directions


def _bt_capture_snapshot(bt, trigger_name: str, detail: str = "",
                         strategy_type: str = "", bar: Any = None,
                         _get_life_estimator_fn=None) -> None:
    if bt.snapshot_collector is None:
        return
    try:
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import (
            SnapshotTrigger, StrategyStateSnapshot, SIX_STRATEGY_KEYS, THREE_VARIANTS,
            LifeExpectancySnapshot, CyclePredictionSnapshot, RiskDimensionScores,
        )
        trigger_map = {
            "signal": SnapshotTrigger.SIGNAL_GENERATED,
            "open": SnapshotTrigger.ORDER_OPENED,
            "close": SnapshotTrigger.ORDER_CLOSED,
        }
        trigger = trigger_map.get(trigger_name, SnapshotTrigger.SIGNAL_GENERATED)
        states = []
        for sk in SIX_STRATEGY_KEYS:
            for variant in THREE_VARIANTS:
                states.append(StrategyStateSnapshot(
                    strategy_id=f"{sk}_{variant}",
                    strategy_type=sk,
                ))
        ts = np.datetime64('now')
        if bar is not None and hasattr(bar, 'get'):
            bar_ts = bar.get('minute', None)
            if bar_ts is not None:
                ts = np.datetime64(str(bar_ts))

        life_snap = None
        hmm_state = getattr(bt, 'current_state', None)
        if hmm_state:
            estimator = _get_life_estimator_fn() if _get_life_estimator_fn else None
            if estimator is not None and hasattr(estimator, '_life_dict') and estimator._life_dict:
                try:
                    life = estimator.get_life_expectancy(hmm_state)
                    if life is not None and life.is_valid():
                        life_snap = LifeExpectancySnapshot(
                            hmm_state=hmm_state,
                            duration_p25=life.duration.get('p25', 0),
                            duration_p50=life.duration.get('p50', 0),
                            duration_p75=life.duration.get('p75', 0),
                            duration_p99=life.duration.get('p99', 0),
                            magnitude_p50=life.magnitude.get('p50', 0),
                            magnitude_p75=life.magnitude.get('p75', 0),
                            sample_count=life.sample_count,
                            decay_r_squared=life.decay_r_squared,
                            degradation_level=life.degradation_level,
                            is_valid=True,
                        )
                except Exception as _e:
                    logger.warning("[SNAPSHOT] life_estimator failed: %s", _e)

        cycle_snap = None
        if hmm_state:
            cycle_snap = CyclePredictionSnapshot()
            if bar is not None and hasattr(bar, 'get'):
                trend_scores, trend_directions = _infer_trend_scores_from_bar(bar)
                cycle_snap.trend_scores_short = trend_scores[0]
                cycle_snap.trend_scores_medium = trend_scores[1]
                cycle_snap.trend_scores_long = trend_scores[2]
                cycle_snap.trend_directions_short = trend_directions[0]
                cycle_snap.trend_directions_medium = trend_directions[1]
                cycle_snap.trend_directions_long = trend_directions[2]
            if life_snap is not None and life_snap.duration_p75 > 0:
                cycle_snap.phase_remaining_estimate = life_snap.duration_p75

        risk_dims = RiskDimensionScores()
        if life_snap is not None and life_snap.is_valid:
            risk_dims.d3_life_expectancy = {0: 1.0, 1: 0.7, 2: 0.4, 3: 0.2}.get(
                life_snap.degradation_level, 0.5)
        if cycle_snap is not None:
            risk_dims.d4_cycle_resonance = cycle_snap.resonance_strength if cycle_snap.resonance_strength > 0 else 0.5
            risk_dims.d5_phase_quality = cycle_snap.phase_quality

        bt.snapshot_collector.capture(
            timestamp=ts, trigger=trigger, trigger_detail=detail,
            strategy_states=states,
            life_expectancy=life_snap,
            cycle_prediction=cycle_snap,
            risk_dimensions=risk_dims,
        )
    except Exception as e:
        logging.debug("[_bt_capture_snapshot] error: %s", e)