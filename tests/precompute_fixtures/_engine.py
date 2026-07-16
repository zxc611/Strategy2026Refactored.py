# [M1-109] 预计算引擎
# MODULE_ID: M1-177
"""V5 precompute engine — full/incremental/parallel/breakpoint-resume."""
from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from infra._helpers import get_logger
from precompute._schema import (
    SCHEMA_REQUIRED_COLUMNS_V5,
    sync_db_schema_v5,
)
from precompute._params import PrecomputeParams

logger = get_logger(__name__)


def _validate_crm_vectorized_vs_perrow(df, cr_result, n, hmm_state, hmm_posterior, trend_scores, trend_directions, strength, imbalance):
    try:
        from param_pool.optimization.cycle_sharpe import CycleResonanceModule, Phase
        crm = CycleResonanceModule()
        hmm_names = {0: "LOW_VOL", 1: "NORMAL", 2: "HIGH_VOL"}
        phase_map = {"CHARGE": 0, "RELEASE": 1, "EXHAUST": 2, "CHAOS": 3}
        sample_idx = np.random.choice(n, size=min(50, n), replace=False)
        max_bias_diff = 0.0
        max_strength_diff = 0.0
        for i in sample_idx:
            output = crm.update(
                hmm_state=hmm_names.get(int(hmm_state[i]), "NORMAL"),
                hmm_posterior=tuple(hmm_posterior[i]),
                trend_scores=tuple(trend_scores[i]),
                trend_directions=tuple(trend_directions[i].astype(float)),
                strength=float(strength[i]),
                imbalance=float(imbalance[i]),
            )
            bias_diff = abs(output.directional_bias - cr_result["directional_bias"].iloc[i])
            strength_diff = abs(output.resonance_strength - cr_result["resonance_strength"].iloc[i])
            max_bias_diff = max(max_bias_diff, bias_diff)
            max_strength_diff = max(max_strength_diff, strength_diff)
        if max_bias_diff > 0.05 or max_strength_diff > 0.05:
            logger.warning("CRM runtime check: max_bias_diff=%.4f, max_strength_diff=%.4f", max_bias_diff, max_strength_diff)
        else:
            logger.info("CRM runtime check passed: max_bias_diff=%.4f, max_strength_diff=%.4f", max_bias_diff, max_strength_diff)
    except (ImportError, ValueError, RuntimeError) as e:
        logger.debug("CRM runtime check skipped: %s", e)


def _enrich_bars_with_cycle_resonance(
    df: pd.DataFrame, use_vectorized: bool = True
) -> pd.DataFrame:
    from precompute._hmm import (
        compute_hmm_state_vectorized,
    )
    from precompute._trend_scores import (
        compute_trend_scores_vectorized,
    )

    n = len(df)
    if n == 0:
        return df
    close = df["close"].to_numpy(dtype=np.float64)
    volume = df["volume"].to_numpy(dtype=np.float64)
    iv = (
        df["iv"].to_numpy(dtype=np.float64)
        if "iv" in df.columns
        else np.full(n, 0.2)
    )
    imbalance = (
        df["imbalance"].to_numpy(dtype=np.float64)
        if "imbalance" in df.columns
        else np.zeros(n)
    )
    strength = (
        df["strength"].to_numpy(dtype=np.float64)
        if "strength" in df.columns
        else np.zeros(n)
    )

    hmm_state, hmm_posterior = compute_hmm_state_vectorized(close, volume, iv)
    df["hmm_state"] = hmm_state
    df["hmm_posterior_low"] = hmm_posterior[:, 0]
    df["hmm_posterior_normal"] = hmm_posterior[:, 1]
    df["hmm_posterior_high"] = hmm_posterior[:, 2]

    trend_scores, trend_directions = compute_trend_scores_vectorized(close)
    df["trend_score_short"] = trend_scores[:, 0]
    df["trend_score_medium"] = trend_scores[:, 1]
    df["trend_score_long"] = trend_scores[:, 2]
    df["trend_direction_short"] = trend_directions[:, 0]
    df["trend_direction_medium"] = trend_directions[:, 1]
    df["trend_direction_long"] = trend_directions[:, 2]

    if use_vectorized:
        try:
            from precompute._cycle_resonance_vec import (
                compute_cycle_resonance_vectorized,
            )
            cr_result = compute_cycle_resonance_vectorized(df, phase_params=self._params.cycle_resonance if hasattr(self._params, 'cycle_resonance') else None)
            for col in cr_result.columns:
                df[col] = cr_result[col].values
            
            if n >= 100 and os.environ.get("CRM_RUNTIME_CHECK", "").lower() in ("1", "true"):
                _validate_crm_vectorized_vs_perrow(df, cr_result, n, hmm_state, hmm_posterior, trend_scores, trend_directions, strength, imbalance)
            
            return df
        except (ImportError, AttributeError, ValueError, RuntimeError) as exc:
            logger.warning(
                "vectorized CRM failed, fallback to per-row: %s", exc
            )

    from param_pool.optimization.cycle_sharpe import (
        CycleResonanceModule,
        Phase,
    )

    crm = CycleResonanceModule()
    directional_bias = np.zeros(n)
    resonance_strength = np.zeros(n)
    phase = np.full(n, 3, dtype=np.int32)
    state_entropy = np.full(n, 0.5)
    hmm_names = {0: "LOW_VOL", 1: "NORMAL", 2: "HIGH_VOL"}
    phase_map = {"CHARGE": 0, "RELEASE": 1, "EXHAUST": 2, "CHAOS": 3}
    for i in range(n):
        try:
            output = crm.update(
                hmm_state=hmm_names.get(int(hmm_state[i]), "NORMAL"),
                hmm_posterior=tuple(hmm_posterior[i]),
                trend_scores=tuple(trend_scores[i]),
                trend_directions=tuple(trend_directions[i].astype(float)),
                strength=float(strength[i]),
                imbalance=float(imbalance[i]),
            )
            directional_bias[i] = output.directional_bias
            resonance_strength[i] = output.resonance_strength
            phase[i] = phase_map.get(output.phase.value, 3)
            state_entropy[i] = output.state_entropy
        except (ValueError, TypeError, AttributeError, RuntimeError) as _crm_err:
            logger.warning("CRM per-row update failed at idx=%d: %s", i, _crm_err)
    df["directional_bias"] = directional_bias
    df["resonance_strength"] = resonance_strength
    df["cr_phase"] = phase
    df["state_entropy"] = state_entropy
    return df


class PrecomputeEngine:
    PROGRESS_FILE = "precompute_progress.json"

    def __init__(
        self,
        db_path: str = "preprocessed.duckdb",
        max_workers: int = 4,
        batch_size: int = 50000,
        symbols: Optional[List[str]] = None,
        params: Optional[PrecomputeParams] = None,
    ):
        self._db_path = db_path
        self._max_workers = max_workers
        self._batch_size = batch_size
        self._symbols = symbols or []
        self._params = params or PrecomputeParams()
        self._progress = self._load_progress()

    def _load_progress(self) -> Dict:
        progress_path = Path(self.PROGRESS_FILE)
        if progress_path.exists():
            try:
                with open(progress_path, "r") as f:
                    from infra.serialization_utils import json_loads
                    return json_loads(f.read())
            except (OSError, ValueError, KeyError):
                pass
        return {"completed_symbols": {}, "last_run": None}

    def _save_progress(self) -> None:
        import portalocker
        self._progress["last_run"] = datetime.now().isoformat()
        with open(self.PROGRESS_FILE, "w") as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            from infra.serialization_utils import json_dumps
            f.write(json_dumps(self._progress, indent=2))
            portalocker.unlock(f)

    def _precompute_symbol(self, symbol: str, since_date: Optional[str] = None) -> int:
        from data.db_adapter import connect

        con = connect(self._db_path)
        try:
            sync_db_schema_v5(con)

            if since_date:
                df = con.execute(
                    "SELECT * FROM minute_data WHERE symbol = ? AND minute >= ? ORDER BY minute",
                    [symbol, since_date],
                ).fetchdf()
            else:
                df = con.execute(
                    "SELECT * FROM minute_data WHERE symbol = ? ORDER BY minute",
                    [symbol],
                ).fetchdf()
            if df.empty:
                return 0

            from precompute._kl_rpd import (
                compute_kl_rpd_vectorized,
            )
            from precompute._obos import (
                compute_obos_vectorized,
            )
            from precompute._pullback import (
                compute_pullback_vectorized,
            )

            kl_rpd = compute_kl_rpd_vectorized(df)
            obos = compute_obos_vectorized(df)
            pullback = compute_pullback_vectorized(
                df["close"].values,
                df["high"].values if "high" in df.columns else df["close"].values,
                df["low"].values if "low" in df.columns else df["close"].values,
            )
            for col in kl_rpd.columns:
                df[col] = kl_rpd[col].values
            for col in obos.columns:
                df[col] = obos[col].values
            for col in pullback.columns:
                df[col] = pullback[col].values

            df = _enrich_bars_with_cycle_resonance(df)

            from precompute._signals import (
                compute_signals_vectorized,
            )

            signals = compute_signals_vectorized(df)
            for col in signals.columns:
                df[col] = signals[col].values

            from precompute._l0_state import (
                compute_l0_state_vectorized,
            )
            from precompute._signal_decay import (
                compute_decay_and_linkage_vectorized,
            )
            from precompute._position_decision import (
                compute_position_decision_vectorized,
            )

            l0 = compute_l0_state_vectorized(df)
            for col in l0.columns:
                df[col] = l0[col].values

            decay_linkage = compute_decay_and_linkage_vectorized(df)
            for col in decay_linkage.columns:
                df[col] = decay_linkage[col].values

            position = compute_position_decision_vectorized(df)
            for col in position.columns:
                df[col] = position[col].values

            from strategy.divergence_reversal import (
                DivergenceReversalModule,
            )

            _div_mod = DivergenceReversalModule()
            div_output = _div_mod.update(df)
            div_df = div_output.to_dataframe()
            for col in div_df.columns:
                df[col] = div_df[col].values

            v5_cols = list(SCHEMA_REQUIRED_COLUMNS_V5.keys())
            update_cols = [c for c in v5_cols if c in df.columns]
            df_update = df[["minute", "symbol"] + update_cols].copy()

            try:
                con.execute("DROP TABLE IF EXISTS _v5_update")
            except (OSError, ValueError, RuntimeError):
                pass
            
            existing_cols = {row[0] for row in con.execute("DESCRIBE minute_data").fetchall()}
            schema_col_set = set(SCHEMA_REQUIRED_COLUMNS_V5.keys())
            safe_update_cols = [c for c in update_cols if c in existing_cols and c in schema_col_set]
            df_update = df[["minute", "symbol"] + safe_update_cols].copy()
            
            con.execute(
                "CREATE TEMP TABLE _v5_update AS SELECT * FROM df_update LIMIT 0"
            )
            con.execute("INSERT INTO _v5_update SELECT * FROM df_update")
            set_clause = ", ".join(f"{col} = u.{col}" for col in safe_update_cols)
            if set_clause:
                logger.debug("UPDATE columns: %s", safe_update_cols)
                con.execute(
                    f"UPDATE minute_data m SET {set_clause} "
                    f"FROM _v5_update u WHERE m.minute = u.minute AND m.symbol = u.symbol"
                )
            try:
                con.execute("DROP TABLE _v5_update")
            except (OSError, ValueError, RuntimeError):
                pass

            return len(df)
        finally:
            con.close()

    def _precompute_symbol_incremental(
        self, symbol: str, since_date: str
    ) -> int:
        return self._precompute_symbol(symbol, since_date=since_date)

    def run_full_precompute(self, skip_daily_pivot: bool = False) -> None:
        start_time = time.time()
        logger.info("===== V5 precompute engine started =====")
        logger.info(
            "symbols: %d, workers: %d", len(self._symbols), self._max_workers
        )

        if self._max_workers > 1:
            logger.warning("DuckDB does not support concurrent writes. Forcing max_workers=1")
            self._max_workers = 1

        for sym in self._symbols:
            try:
                result = self._precompute_symbol(sym)
                self._progress["completed_symbols"][sym] = {
                    "status": "done",
                    "rows": result,
                }
                self._save_progress()
            except (RuntimeError, ValueError, OSError) as e:
                logger.error("symbol %s precompute failed: %s", sym, e)
                self._progress["completed_symbols"][sym] = {
                    "status": "failed",
                    "error": str(e),
                }
                self._save_progress()

        elapsed = time.time() - start_time
        logger.info(
            "===== V5 precompute engine finished: %.1f seconds =====", elapsed
        )

        if not skip_daily_pivot:
            logger.info("Auto-proceeding to daily pivot precompute")
            self.run_daily_pivot_precompute()
        else:
            logger.info("Skipping daily pivot precompute (skip_daily_pivot=True)")

    def run_daily_pivot_precompute(self) -> None:
        from data.db_adapter import connect
        from precompute._daily_pivot import (
            compute_daily_pivots_for_symbol,
            ensure_daily_key_pivots_table,
        )

        con = connect(self._db_path)
        try:
            ensure_daily_key_pivots_table(con)
            dp = self._params.daily_pivot
            total_pivots = 0
            for sym in self._symbols:
                try:
                    n = compute_daily_pivots_for_symbol(
                        con, sym,
                        multiplier=dp.multiplier,
                        min_bars=dp.min_bars,
                        compute_version=dp.compute_version,
                    )
                    total_pivots += n
                    logger.info("daily pivot %s: %d pivots", sym, n)
                except (OSError, ValueError, KeyError, RuntimeError) as e:
                    logger.error("daily pivot %s failed: %s", sym, e)
            logger.info("daily pivot total: %d pivots across %d symbols", total_pivots, len(self._symbols))
        finally:
            con.close()

    def run_incremental_precompute(
        self, since_date: Optional[str] = None
    ) -> None:
        if since_date is None:
            last_run = self._progress.get("last_run")
            if last_run:
                since_date = last_run[:10]
            else:
                logger.warning(
                    "no previous run record, executing full precompute"
                )
                self.run_full_precompute()
                return

        logger.info("incremental precompute: since_date=%s", since_date)
        for sym in self._symbols:
            try:
                self._precompute_symbol_incremental(sym, since_date)
            except (RuntimeError, ValueError, OSError) as e:
                logger.error(
                    "symbol %s incremental precompute failed: %s", sym, e
                )