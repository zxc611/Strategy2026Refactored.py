"""
feature_engine.py - FeatureEngine
Phase 2 (CC-P1-03): 从preprocess_ticks.py提取的特征工程/衍生指标职责域

职责：
- 五态分类 (compute_option_state_vectorized)
- 订单流指标 (compute_order_flow_vectorized)
- Greeks计算 (compute_greeks_vectorized + BS解析解辅助函数)
"""
from __future__ import annotations

import logging
import math as _math
import numpy as np
import pandas as pd
from datetime import timedelta, timezone
from typing import Tuple

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, TRADING_DAYS_PER_YEAR_CHINA

_CHINA_TZ = timezone(timedelta(hours=8))

try:
    from scipy.stats import norm as _scipy_norm
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False

logger = logging.getLogger(__name__)


def _norm_cdf(x: float) -> float:
    return (1.0 + _math.erf(x / _math.sqrt(2.0))) / 2.0


def _norm_pdf(x: float) -> float:
    return _math.exp(-x * x / 2.0) / _math.sqrt(2.0 * _math.pi)


def _bs_price_scalar(S: float, K: float, T: float, r: float, q: float,
                      sigma: float, option_type: str) -> float:
    if T <= 0 or sigma <= 0 or K <= 0 or S <= 0:
        return max(0.0, (S - K) if option_type == 'CALL' else (K - S))
    if sigma < 1e-6:
        return max(0.0, (S - K) if option_type == 'CALL' else (K - S))
    try:
        d1 = (_math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * _math.sqrt(T))
        d2 = d1 - sigma * _math.sqrt(T)
        if option_type == 'CALL':
            return S * _math.exp(-q * T) * _norm_cdf(d1) - K * _math.exp(-r * T) * _norm_cdf(d2)
        else:
            return K * _math.exp(-r * T) * _norm_cdf(-d2) - S * _math.exp(-q * T) * _norm_cdf(-d1)
    except Exception:
        return 0.0


def _bs_greeks_scalar(S: float, K: float, T: float, r: float, q: float,
                       sigma: float, option_type: str) -> Tuple[float, float, float, float]:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return (0.0, 0.0, 0.0, 0.0)
    if sigma < 1e-6:
        return (1.0 if option_type == 'CALL' else -1.0, 0.0, 0.0, 0.0)
    try:
        d1 = (_math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * _math.sqrt(T))
        d2 = d1 - sigma * _math.sqrt(T)
        if option_type == 'CALL':
            delta = _math.exp(-q * T) * _norm_cdf(d1)
            theta = (-_math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * _math.sqrt(T))
                     - r * K * _math.exp(-r * T) * _norm_cdf(d2)
                     + q * S * _math.exp(-q * T) * _norm_cdf(d1)) / TRADING_DAYS_PER_YEAR_CHINA
        else:
            delta = _math.exp(-q * T) * (_norm_cdf(d1) - 1.0)
            theta = (-_math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * _math.sqrt(T))
                     + r * K * _math.exp(-r * T) * _norm_cdf(-d2)
                     - q * S * _math.exp(-q * T) * _norm_cdf(-d1)) / TRADING_DAYS_PER_YEAR_CHINA
        gamma = _math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * _math.sqrt(T))
        vega = S * _math.exp(-q * T) * _norm_pdf(d1) * _math.sqrt(T) / 100.0
        return (round(delta, 6), round(gamma, 4), round(theta, 6), round(vega, 4))
    except Exception:
        return (0.0, 0.0, 0.0, 0.0)


def _implied_volatility_scalar(market_price: float, S: float, K: float, T: float,
                                r: float, q: float, option_type: str,
                                initial_guess: float = 0.2, max_iter: int = 50,
                                tol: float = 1e-6) -> float:
    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return initial_guess
    sigma = initial_guess
    for _ in range(max_iter):
        price = _bs_price_scalar(S, K, T, r, q, sigma, option_type)
        greeks = _bs_greeks_scalar(S, K, T, r, q, sigma, option_type)
        vega = greeks[3] * 100
        if vega < 1e-10:
            return initial_guess
        diff = price - market_price
        if abs(diff) < tol:
            return sigma
        sigma = sigma - diff / vega
        sigma = max(0.01, min(5.0, sigma))
    return sigma


def _compute_greeks_fallback(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(np.zeros((0, 5)), columns=["iv", "delta", "gamma", "vega", "theta"])
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    close_safe = np.where(close > 0, close, 1.0)
    hl_range = np.where(close_safe > 0, (high - low) / close_safe, 0.0)
    iv = np.clip(hl_range * np.sqrt(ANNUALIZE_FACTOR_DAILY) * 0.5, 0.05, 2.0)
    delta = np.clip((close - (high + low) / 2) / close_safe, -1, 1)
    gamma = np.clip(1.0 / (close_safe * iv + 1e-8), 0, 0.1)
    vega = np.clip(iv * 0.1, 0, 0.05)
    theta = -iv * 0.01
    logger.debug("Greeks降级: 缺少strike_price/expire_date，使用日内波幅估算")
    return pd.DataFrame({"iv": iv, "delta": delta, "gamma": gamma, "vega": vega, "theta": theta})


def compute_option_state_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    base_cols = ["correct_rise_pct", "correct_fall_pct", "wrong_rise_pct",
                 "wrong_fall_pct", "other_pct", "strength"]
    if n == 0:
        return pd.DataFrame(np.zeros((0, len(base_cols))), columns=base_cols)

    has_real_data = (
        "underlying_price" in df.columns
        and "option_type" in df.columns
        and df["underlying_price"].notna().any()
        and df["option_type"].notna().any()
    )

    if has_real_data:
        opt_type = df["option_type"].fillna("").str.upper().values
        underlying = df["underlying_price"].values
        opt_close = df["close"].values
        prev_underlying = np.empty_like(underlying); prev_underlying[1:] = underlying[:-1]; prev_underlying[0] = underlying[0]
        prev_opt = np.empty_like(opt_close); prev_opt[1:] = opt_close[:-1]; prev_opt[0] = opt_close[0]
        underlying_rising = underlying > prev_underlying
        opt_rising = opt_close > prev_opt
        is_call = (opt_type == "CALL")
        is_put = (opt_type == "PUT")
        valid_move = (underlying != prev_underlying) & (opt_close != prev_opt)
        correct_rise = valid_move & ((is_call & underlying_rising & opt_rising) | (is_put & ~underlying_rising & opt_rising))
        correct_fall = valid_move & ((is_call & ~underlying_rising & ~opt_rising) | (is_put & underlying_rising & ~opt_rising))
        wrong_rise = valid_move & ((is_call & ~underlying_rising & opt_rising) | (is_put & underlying_rising & opt_rising))
        wrong_fall = valid_move & ((is_call & underlying_rising & ~opt_rising) | (is_put & ~underlying_rising & ~opt_rising))
    else:
        close = df["close"].values
        open_ = df["open"].values
        pct_change = np.where(open_ > 0, (close - open_) / open_, 0.0)
        pct_change = np.where(np.isfinite(pct_change), pct_change, 0.0)
        is_rise = pct_change > 0
        is_correct = np.abs(pct_change) > 0.005
        correct_rise = is_rise & is_correct
        correct_fall = (~is_rise) & is_correct
        wrong_rise = is_rise & (~is_correct)
        wrong_fall = (~is_rise) & (~is_correct) & (pct_change < 0)
        pct_change_abs = np.abs(pct_change)
        logger.debug("五态降级: 缺少underlying_price/option_type，使用价格动量简化分类")

    try:
        total = np.maximum(
            correct_rise.astype(np.float64) + correct_fall.astype(np.float64)
            + wrong_rise.astype(np.float64) + wrong_fall.astype(np.float64),
            1.0,
        )
    except (ValueError, TypeError) as _e:
        logger.warning("[NP-P2-29] astype float64 conversion failed: %s, using zeros", _e)
        total = np.maximum(np.zeros_like(pct_change_abs, dtype=np.float64), 1.0)
    other_mask = ~(correct_rise | correct_fall | wrong_rise | wrong_fall)

    if has_real_data:
        underlying = df["underlying_price"].values
        prev_underlying = np.empty_like(underlying); prev_underlying[1:] = underlying[:-1]; prev_underlying[0] = underlying[0]
        strength_raw = np.abs(underlying - prev_underlying) / np.where(prev_underlying > 0, prev_underlying, 1.0)
        strength = np.clip(strength_raw * 20, 0, 1)
    else:
        close = df["close"].values
        open_ = df["open"].values
        pct_change = np.where(open_ > 0, (close - open_) / open_, 0.0)
        strength = np.clip(np.abs(pct_change) * 20, 0, 1)

    return pd.DataFrame({
        "correct_rise_pct": correct_rise.astype(float, errors='coerce') / total,
        "correct_fall_pct": correct_fall.astype(float, errors='coerce') / total,
        "wrong_rise_pct": wrong_rise.astype(float, errors='coerce') / total,
        "wrong_fall_pct": wrong_fall.astype(float, errors='coerce') / total,
        "other_pct": other_mask.astype(float, errors='coerce') / total,
        "strength": strength,
        "_classification_mode": np.full(n, 1 if has_real_data else 0, dtype=np.int32),
    })


def compute_order_flow_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        return pd.DataFrame(np.zeros((0, 2)), columns=["imbalance", "consistency"])

    close = df["close"].to_numpy(dtype=float)
    open_ = df["open"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float) if "high" in df.columns else np.maximum(open_, close)
    low = df["low"].to_numpy(dtype=float) if "low" in df.columns else np.minimum(open_, close)
    vol = df["volume"].to_numpy(dtype=float)
    vol_safe = np.where(vol > 0, vol, 1.0)

    spread = high - low
    spread = np.nan_to_num(spread, nan=0.0)
    spread_safe = np.where(spread > 1e-8, spread, 1e-8)
    price_position = (close - low) / spread_safe
    price_position = np.clip(price_position, 0.0, 1.0)

    raw_imbalance = 2.0 * price_position - 1.0
    vol_weight = np.log1p(vol_safe)
    vol_weight_sum = np.sum(vol_weight) + 1e-8
    weighted_imbalance = raw_imbalance * vol_weight

    alpha = 2.0 / (5.0 + 1.0)
    ema_warmup = 5
    imbalance = np.empty(n)
    imbalance[0] = weighted_imbalance[0] / vol_weight[0] if vol_weight[0] > 0 else 0.0
    for i in range(1, n):
        imbalance[i] = alpha * (weighted_imbalance[i] / vol_weight[i] if vol_weight[i] > 0 else 0.0) + (1.0 - alpha) * imbalance[i - 1]
    for i in range(min(ema_warmup, n)):
        imbalance[i] = np.nan
    imbalance = np.clip(np.nan_to_num(imbalance, nan=0.0), -1.0, 1.0)

    signs = np.sign(raw_imbalance)
    if n >= 3:
        consistency_ema = np.empty(n)
        consistency_ema[0] = signs[0]
        for i in range(1, n):
            consistency_ema[i] = alpha * signs[i] + (1.0 - alpha) * consistency_ema[i - 1]
        for i in range(min(ema_warmup, n)):
            consistency_ema[i] = np.nan
        consistency = np.clip(np.nan_to_num(consistency_ema, nan=0.0) * np.abs(imbalance), -1.0, 1.0)
    else:
        consistency = np.clip(imbalance * 0.8, -1.0, 1.0)

    return pd.DataFrame({"imbalance": imbalance, "consistency": consistency})


def compute_greeks_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    greek_cols = ["iv", "delta", "gamma", "vega", "theta"]
    if n == 0:
        return pd.DataFrame(np.zeros((0, 5)), columns=greek_cols)

    has_bs_data = (
        "strike_price" in df.columns
        and "expire_date" in df.columns
        and "option_type" in df.columns
        and "underlying_price" in df.columns
        and df["strike_price"].notna().any()
        and df["expire_date"].notna().any()
    )

    if not has_bs_data:
        return _compute_greeks_fallback(df)

    RISK_FREE_RATE = 0.02
    DIVIDEND_YIELD = 0.0
    reference_date = df["minute"].min() if "minute" in df.columns else pd.Timestamp.now(tz=_CHINA_TZ)

    S = df["underlying_price"].to_numpy(dtype=np.float64)
    K = df["strike_price"].to_numpy(dtype=np.float64)
    market_price = df["close"].to_numpy(dtype=np.float64)
    opt_type = df["option_type"].fillna("").str.upper().values

    expire_ts = pd.to_datetime(df["expire_date"].values, errors="coerce")
    ref_ts = pd.to_datetime(reference_date)
    T_years = (expire_ts - ref_ts).total_seconds() / (365.25 * 86400.0)
    T_years = np.clip(T_years, 1.0 / 365.25, 5.0)

    iv_arr = np.full(n, 0.2)
    delta_arr = np.zeros(n)
    gamma_arr = np.zeros(n)
    vega_arr = np.zeros(n)
    theta_arr = np.zeros(n)

    for i in range(n):
        s_i, k_i, t_i, mp_i = S[i], K[i], T_years[i], market_price[i]
        ot_i = opt_type[i]
        if s_i <= 0 or k_i <= 0 or t_i <= 0 or mp_i <= 0:
            continue
        if ot_i not in ("CALL", "PUT"):
            continue
        iv_i = _implied_volatility_scalar(mp_i, s_i, k_i, t_i, RISK_FREE_RATE, DIVIDEND_YIELD, ot_i)
        iv_arr[i] = iv_i
        greeks = _bs_greeks_scalar(s_i, k_i, t_i, RISK_FREE_RATE, DIVIDEND_YIELD, iv_i, ot_i)
        delta_arr[i] = greeks[0]
        gamma_arr[i] = greeks[1]
        theta_arr[i] = greeks[2]
        vega_arr[i] = greeks[3]

    return pd.DataFrame({
        "iv": iv_arr, "delta": delta_arr, "gamma": gamma_arr,
        "vega": vega_arr, "theta": theta_arr,
    })