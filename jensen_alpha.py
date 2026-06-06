"""Jensen's Alpha — Standard Alpha calculation [R19-ATT-02 complement].

Provides industry-standard Jensen's Alpha calculation as an alternative
to the simplified (Sharpe_diff) formula currently used in
shadow_strategy_engine.compute_alpha_metrics().

Jensen's Alpha = Rp - [Rf + Beta * (Rm - Rf)]
where:
  Rp = portfolio return (annualized)
  Rf = risk-free rate
  Beta = Cov(Rp, Rm) / Var(Rm)
  Rm = market benchmark return
"""
from __future__ import annotations

import math
import logging
import numpy as np
from typing import Optional, Tuple
from dataclasses import dataclass

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class JensenAlphaResult:
    """Result of Jensen's Alpha calculation."""
    alpha: float
    beta: float
    portfolio_return_annual: float
    benchmark_return_annual: float
    risk_free_rate: float
    r_squared: float
    residual_std: float
    ci_lower: float = 0.0  # P1-R9-12修复: 95% CI下界
    ci_upper: float = 0.0  # P1-R9-12修复: 95% CI上界


def compute_jensen_alpha(
    portfolio_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    risk_free_rate: float = 0.02,
    annualize_factor: float = ANNUALIZE_FACTOR_DAILY
) -> JensenAlphaResult:
    """Compute Jensen's Alpha using CAPM regression.

    Args:
        portfolio_returns: Period-by-period portfolio returns
        benchmark_returns: Period-by-period benchmark returns (same length)
        risk_free_rate: Annualized risk-free rate
        annualize_factor: Annualization factor (252 for daily, 60480 for minute)

    Returns:
        JensenAlphaResult with alpha, beta, and regression statistics
    """
    n = len(portfolio_returns)
    if n < 3 or len(benchmark_returns) != n:
        return JensenAlphaResult(
            alpha=0.0, beta=1.0,
            portfolio_return_annual=0.0, benchmark_return_annual=0.0,
            risk_free_rate=risk_free_rate, r_squared=0.0, residual_std=0.0
        )

    rf_period = risk_free_rate / annualize_factor

    rp_mean = np.mean(portfolio_returns)
    rm_mean = np.mean(benchmark_returns)

    excess_p = portfolio_returns - rf_period
    excess_m = benchmark_returns - rf_period

    cov_pm = np.cov(portfolio_returns, benchmark_returns, ddof=1)[0, 1]
    var_m = np.var(benchmark_returns, ddof=1)

    if var_m < 1e-15:
        beta = 1.0
    else:
        beta = cov_pm / var_m

    alpha_period = rp_mean - rf_period - beta * (rm_mean - rf_period)
    alpha_annual = alpha_period * annualize_factor

    rp_annual = rp_mean * annualize_factor
    rm_annual = rm_mean * annualize_factor

    predicted = rf_period + beta * (benchmark_returns - rf_period)
    residuals = portfolio_returns - predicted
    residual_std = float(np.std(residuals, ddof=1))

    if var_m > 1e-15:
        var_p = np.var(portfolio_returns, ddof=1)
        r_squared = (beta ** 2 * var_m) / var_p if var_p > 1e-15 else 0.0
    else:
        r_squared = 0.0

    # P1-R9-12修复: 使用bootstrap计算alpha的95% CI
    ci_lower = 0.0
    ci_upper = 0.0
    if n >= 20:
        try:
            n_bootstrap = 1000
            rng = np.random.RandomState(42)
            boot_alphas = np.empty(n_bootstrap)
            for i in range(n_bootstrap):
                idx = rng.randint(0, n, size=n)
                bp = portfolio_returns[idx]
                bm = benchmark_returns[idx]
                cov_b = np.cov(bp, bm, ddof=1)[0, 1]
                var_b = np.var(bm, ddof=1)
                beta_b = cov_b / var_b if var_b > 1e-15 else 1.0
                alpha_b = (np.mean(bp) - rf_period - beta_b * (np.mean(bm) - rf_period)) * annualize_factor
                boot_alphas[i] = alpha_b
            ci_lower = float(np.percentile(boot_alphas, 2.5))
            ci_upper = float(np.percentile(boot_alphas, 97.5))
        except Exception as _bs_err:
            logger.debug("[P1-R9-12] bootstrap CI失败: %s", _bs_err)

    return JensenAlphaResult(
        alpha=alpha_annual,
        beta=float(beta),
        portfolio_return_annual=rp_annual,
        benchmark_return_annual=rm_annual,
        risk_free_rate=risk_free_rate,
        r_squared=float(r_squared),
        residual_std=residual_std,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
    )


def compute_information_ratio(
    portfolio_returns: np.ndarray,
    benchmark_returns: np.ndarray
) -> float:
    """Compute Information Ratio = Alpha / Tracking Error."""
    excess = portfolio_returns - benchmark_returns
    if len(excess) < 2:
        return 0.0
    mean_excess = np.mean(excess)
    tracking_error = np.std(excess, ddof=1)
    if tracking_error < 1e-10:
        return 0.0
    return float(mean_excess / tracking_error)
