"""Multi-scale aggregation corrected module — Greeks recompute + volume-weighted order flow.

Fix history:
  - I-1: minute column type-safety — coerce str→datetime before .dt.floor()
  - I-2: last_cols vectorized — replace per-row lambda with groupby.last() + merge
  - I-3: signal recompute — carry forward signal columns from 1m, then recompute
"""
from __future__ import annotations

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from ali2026v3_trading.infra._helpers import get_logger

logger = get_logger(__name__)


def _ensure_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    return pd.to_datetime(series, errors="coerce")


def resample_multiscale_corrected(
    df_1m: pd.DataFrame,
    bar_lengths: List[int] = None,
) -> Dict[int, pd.DataFrame]:
    if bar_lengths is None:
        bar_lengths = [5, 15, 60]
    if df_1m.empty:
        return {}
    results = {}
    for bl in bar_lengths:
        df_multi = _resample_corrected(df_1m, bl)
        if not df_multi.empty:
            results[bl] = df_multi
    return results


def _resample_corrected(df_1m: pd.DataFrame, bar_length: int) -> pd.DataFrame:
    if df_1m.empty or bar_length <= 1:
        return pd.DataFrame()
    df = df_1m.copy()
    df["minute"] = _ensure_datetime(df["minute"])
    df["_group"] = df["minute"].dt.floor(f"{bar_length}min")

    ohlcv_agg = {
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
        "volume": ("volume", "sum"),
    }
    if "turnover" in df.columns:
        ohlcv_agg["turnover"] = ("turnover", "sum")
    if "tick_count" in df.columns:
        ohlcv_agg["tick_count"] = ("tick_count", "sum")
    if "open_interest" in df.columns:
        ohlcv_agg["open_interest"] = ("open_interest", "last")

    group_cols = ["_group"]
    if "symbol" in df.columns:
        group_cols.append("symbol")
    result = df.groupby(group_cols, sort=False).agg(**ohlcv_agg).reset_index()
    result.rename(columns={"_group": "minute"}, inplace=True)

    if "volume" in result.columns and "turnover" in result.columns:
        result["vwap"] = np.where(
            result["volume"] > 0,
            result["turnover"] / result["volume"],
            result["close"],
        )

    last_cols = [
        "strike_price",
        "expire_date",
        "option_type",
        "underlying_price",
        "bid_ask_spread",
        "days_to_expiry",
        "iv",
    ]
    last_agg = {}
    for col in last_cols:
        if col in df.columns:
            last_agg[col] = (col, "last")
    if last_agg:
        last_df = df.groupby(group_cols, sort=False).agg(**last_agg).reset_index()
        last_df.rename(columns={"_group": "minute"}, inplace=True)
        merge_cols = ["minute"]
        if "symbol" in result.columns:
            merge_cols.append("symbol")
        result = result.merge(last_df, on=merge_cols, how="left")

    if "imbalance" in df.columns and "volume" in df.columns:
        vol = df["volume"].to_numpy(dtype=np.float64)
        vol_safe = np.where(vol > 0, vol, 1.0)
        df["_imb_w"] = df["imbalance"].to_numpy(dtype=np.float64) * vol_safe
        df["_vol_sum"] = vol_safe
        imb_agg = df.groupby(group_cols, sort=False).agg(
            _imb_w=("_imb_w", "sum"), _vol_sum=("_vol_sum", "sum")
        ).reset_index()
        imb_agg.rename(columns={"_group": "minute"}, inplace=True)
        merge_cols = ["minute"]
        if "symbol" in result.columns:
            merge_cols.append("symbol")
        result = result.merge(imb_agg, on=merge_cols, how="left")
        result["imbalance"] = np.clip(
            result["_imb_w"] / np.maximum(result["_vol_sum"], 1e-8), -1.0, 1.0
        )
        result.drop(columns=["_imb_w", "_vol_sum"], inplace=True)

    signal_cols = [
        "signal_s1", "signal_s2", "signal_s3", "signal_s4",
        "signal_s5", "signal_s6",
        "kl_rpd_k", "kl_rpd_l", "kl_rpd_r", "kl_rpd_d",
        "strength", "consistency",
    ]
    carry_forward_agg = {}
    for col in signal_cols:
        if col in df.columns:
            carry_forward_agg[col] = (col, "last")
    if carry_forward_agg:
        cf_df = df.groupby(group_cols, sort=False).agg(**carry_forward_agg).reset_index()
        cf_df.rename(columns={"_group": "minute"}, inplace=True)
        merge_cols = ["minute"]
        if "symbol" in result.columns:
            merge_cols.append("symbol")
        result = result.merge(cf_df, on=merge_cols, how="left")

    if "iv" in result.columns and "close" in result.columns:
        try:
            from ali2026v3_trading.precompute._kl_rpd import (
                compute_kl_rpd_vectorized,
            )
            kl_rpd = compute_kl_rpd_vectorized(result)
            for col in kl_rpd.columns:
                result[col] = kl_rpd[col].values
        except (ImportError, ValueError, RuntimeError) as _kl_err:
            logger.warning("multiscale kl_rpd recompute failed: %s", _kl_err)

    signal_required_cols = ["signal_s1", "kl_rpd_k", "strength", "imbalance", "close"]
    if all(c in result.columns for c in signal_required_cols):
        try:
            from ali2026v3_trading.precompute._signals import (
                compute_signals_vectorized,
            )
            signals = compute_signals_vectorized(result)
            for col in signals.columns:
                result[col] = signals[col].values
        except (ImportError, ValueError, RuntimeError) as _sig_err:
            logger.warning("multiscale signal recompute failed: %s", _sig_err)

    result["bar_length_minutes"] = bar_length
    return result.dropna(subset=["open", "close"])
