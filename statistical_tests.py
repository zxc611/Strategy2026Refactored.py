"""Statistical Test Enhancements — R19-STAT-P1 fix.

Implements missing statistical tests identified in P1 audit:
  STAT-P1-05: Sharpe ratio significance t-test
  STAT-P1-09: Deflated Sharpe Ratio (DSR)
  STAT-P1-10: Max Drawdown confidence interval (Monte Carlo)
  STAT-P1-06: Non-normality adjustment (skewness/kurtosis)
  STAT-P1-07: Regression toward mean detection
  STAT-P1-11: Optuna trial independence verification
"""
from __future__ import annotations

import math
import logging
import numpy as np
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY
# R27-P1-FP-03修复: 引入浮点稳定计算替代内置sum/mean
try:
    from ali2026v3_trading.resilience_utils import stable_sum, stable_mean, stable_variance, safe_divide
except ImportError:
    import math as _m
    stable_sum = sum
    stable_mean = lambda x: sum(x) / len(x) if x else 0.0
    stable_variance = lambda x: sum((v - stable_mean(x))**2 for v in x) / len(x) if x else 0.0
    safe_divide = lambda a, b, default=0.0: a / b if b != 0 else default

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SharpeTestResult:
    """Result of Sharpe ratio statistical significance test."""
    sharpe: float
    t_stat: float
    p_value: float
    is_significant: bool
    skewness: float = 0.0
    kurtosis: float = 0.0
    adjusted_p_value: float = 0.0


def sharpe_significance_test(
    returns: np.ndarray,
    annualize_factor: float = ANNUALIZE_FACTOR_DAILY,
    risk_free_rate: float = 0.02,
    alpha: float = 0.05
) -> SharpeTestResult:
    """STAT-P1-05: Test if Sharpe ratio is statistically significantly > 0.

    Uses the Jobson-Moodie t-test approximation:
        t = Sharpe * sqrt(n - 1) / sqrt(1 - 0.5 * Sharpe^2)
    Under H0: Sharpe = 0, t ~ t(n-1)
    """
    n = len(returns)
    if n < 3:
        return SharpeTestResult(sharpe=0.0, t_stat=0.0, p_value=1.0, is_significant=False)

    mean_r = np.mean(returns)
    std_r = np.std(returns, ddof=1)
    if std_r < 1e-10:
        return SharpeTestResult(sharpe=0.0, t_stat=0.0, p_value=1.0, is_significant=False)

    rf_per_period = risk_free_rate / annualize_factor
    sharpe_period = (mean_r - rf_per_period) / std_r
    sharpe_annual = sharpe_period * math.sqrt(annualize_factor)

    t_stat = sharpe_period * math.sqrt(n - 1) / math.sqrt(max(1.0 - 0.5 * sharpe_period ** 2, 1e-10))

    from scipy import stats as sp_stats
    p_value = 1.0 - sp_stats.t.cdf(t_stat, df=n - 1)

    return SharpeTestResult(
        sharpe=sharpe_annual,
        t_stat=t_stat,
        p_value=p_value,
        is_significant=p_value < alpha
    )


def sharpe_nonnormality_adjusted_test(
    returns: np.ndarray,
    annualize_factor: float = ANNUALIZE_FACTOR_DAILY,
    risk_free_rate: float = 0.02,
    alpha: float = 0.05
) -> SharpeTestResult:
    """STAT-P1-06: Sharpe significance with non-normality adjustment.

    Adjusts the standard error using skewness and excess kurtosis
    (Memmel, 2003; Ledoit & Wolf, 2008).
    """
    n = len(returns)
    if n < 3:
        return SharpeTestResult(sharpe=0.0, t_stat=0.0, p_value=1.0, is_significant=False)

    mean_r = np.mean(returns)
    std_r = np.std(returns, ddof=1)
    if std_r < 1e-10:
        return SharpeTestResult(sharpe=0.0, t_stat=0.0, p_value=1.0, is_significant=False)

    rf_per_period = risk_free_rate / annualize_factor
    sharpe_period = (mean_r - rf_per_period) / std_r
    sharpe_annual = sharpe_period * math.sqrt(annualize_factor)

    skewness = float(np.mean(((returns - mean_r) / std_r) ** 3))
    kurtosis = float(np.mean(((returns - mean_r) / std_r) ** 4) - 3.0)

    se_adjusted = math.sqrt(
        (1.0 + 0.5 * sharpe_period ** 2 - skewness * sharpe_period + 0.25 * (kurtosis - 3.0) * sharpe_period ** 2) / n
    )
    if se_adjusted < 1e-10:
        se_adjusted = 1.0 / math.sqrt(n)

    t_stat = sharpe_period / se_adjusted

    try:
        from scipy import stats as sp_stats
        p_value = 1.0 - sp_stats.norm.cdf(t_stat)
    except ImportError:
        p_value = 1.0 - _norm_cdf(t_stat)

    return SharpeTestResult(
        sharpe=sharpe_annual,
        t_stat=t_stat,
        p_value=p_value,
        is_significant=p_value < alpha,
        skewness=skewness,
        kurtosis=kurtosis,
        adjusted_p_value=p_value
    )


def _norm_cdf(x: float) -> float:
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


@dataclass(slots=True)
class DeflatedSharpeResult:
    """Result of Deflated Sharpe Ratio test."""
    dsr: float
    sharpe: float
    expected_max_sharpe: float
    is_significant: bool


def deflated_sharpe_ratio(
    returns: np.ndarray,
    n_trials: int,
    annualize_factor: float = ANNUALIZE_FACTOR_DAILY,
    risk_free_rate: float = 0.02,
    alpha: float = 0.05
) -> DeflatedSharpeResult:
    """STAT-P1-09: Deflated Sharpe Ratio (Harvey & Liu, 2015).

    Accounts for multiple testing by comparing observed Sharpe against
    the expected maximum Sharpe under the null across n_trials.

    E[max_SR] ≈ sqrt(V[SR]) * (gamma - ln(ln(N)) / (2*gamma))
    where gamma ≈ sqrt(2*ln(N)), V[SR] ≈ 1/T
    """
    n = len(returns)
    if n < 3 or n_trials < 1:
        return DeflatedSharpeResult(dsr=0.0, sharpe=0.0, expected_max_sharpe=0.0, is_significant=False)

    mean_r = np.mean(returns)
    std_r = np.std(returns, ddof=1)
    if std_r < 1e-10:
        return DeflatedSharpeResult(dsr=0.0, sharpe=0.0, expected_max_sharpe=0.0, is_significant=False)

    rf_per_period = risk_free_rate / annualize_factor
    sharpe_period = (mean_r - rf_per_period) / std_r
    sharpe_annual = sharpe_period * math.sqrt(annualize_factor)

    variance_sr = 1.0 / n
    gamma = math.sqrt(2.0 * math.log(n_trials))
    if gamma > 0:
        expected_max_sr = math.sqrt(variance_sr) * (gamma - math.log(math.log(max(n_trials, 2))) / (2.0 * gamma))
    else:
        expected_max_sr = 0.0

    dsr = sharpe_annual - expected_max_sr * math.sqrt(annualize_factor)

    return DeflatedSharpeResult(
        dsr=dsr,
        sharpe=sharpe_annual,
        expected_max_sharpe=expected_max_sr * math.sqrt(annualize_factor),
        is_significant=dsr > 0
    )


@dataclass(slots=True)
class MaxDDConfidenceResult:
    """Result of Max Drawdown confidence interval."""
    max_dd: float
    ci_lower: float
    ci_upper: float
    confidence_level: float


def max_drawdown_confidence_interval(
    returns: np.ndarray,
    n_bootstrap: int = 1000,
    confidence_level: float = 0.95,
    random_seed: int = 42
) -> MaxDDConfidenceResult:
    """STAT-P1-10: Max Drawdown confidence interval via bootstrap."""
    n = len(returns)
    if n < 5:
        return MaxDDConfidenceResult(max_dd=0.0, ci_lower=0.0, ci_upper=0.0, confidence_level=confidence_level)

    def _compute_max_dd(rets: np.ndarray) -> float:
        curve = np.cumprod(1.0 + rets)
        peak = np.maximum.accumulate(curve)
        dd = (peak - curve) / np.where(peak > 0, peak, 1.0)
        return float(np.max(dd))

    observed_dd = _compute_max_dd(returns)

    rng = np.random.RandomState(random_seed)
    boot_dds = []
    for _ in range(n_bootstrap):
        idx = rng.randint(0, n, size=n)
        boot_dds.append(_compute_max_dd(returns[idx]))

    alpha = 1.0 - confidence_level
    ci_lower = float(np.percentile(boot_dds, alpha / 2 * 100))
    ci_upper = float(np.percentile(boot_dds, (1.0 - alpha / 2) * 100))

    return MaxDDConfidenceResult(
        max_dd=observed_dd,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        confidence_level=confidence_level
    )


def detect_regression_toward_mean(
    in_sample_sharpe: float,
    out_sample_sharpe: float,
    threshold_ratio: float = 0.5
) -> Dict[str, Any]:
    """STAT-P1-07: Detect regression toward mean in Sharpe.

    If out-of-sample Sharpe drops below threshold_ratio * in-sample Sharpe,
    significant regression toward mean is detected.
    """
    if abs(in_sample_sharpe) < 1e-10:
        return {"detected": False, "ratio": 0.0, "in_sample": in_sample_sharpe, "out_sample": out_sample_sharpe}

    ratio = out_sample_sharpe / in_sample_sharpe
    return {
        "detected": ratio < threshold_ratio,
        "ratio": ratio,
        "in_sample": in_sample_sharpe,
        "out_sample": out_sample_sharpe
    }


def verify_trial_independence(
    trial_scores: np.ndarray,
    max_autocorr: float = 0.1
) -> Dict[str, Any]:
    """STAT-P1-11: Verify Optuna trial independence via autocorrelation check."""
    n = len(trial_scores)
    if n < 4:
        return {"independent": True, "autocorr_lag1": 0.0, "n_trials": n}

    mean_s = np.mean(trial_scores)
    centered = trial_scores - mean_s
    var = np.var(centered)
    if var < 1e-10:
        return {"independent": True, "autocorr_lag1": 0.0, "n_trials": n}

    autocorr_lag1 = float(np.mean(centered[:-1] * centered[1:]) / var)

    return {
        "independent": abs(autocorr_lag1) < max_autocorr,
        "autocorr_lag1": autocorr_lag1,
        "n_trials": n
    }
