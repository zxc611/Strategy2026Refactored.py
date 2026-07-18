# MODULE_ID: M1-030
"""
quant_cointegration.py - 协整与生存分析模块

包含:
  CointegrationScanner   - 跨品种协整扫描（双向ADF+残差方差选向）   ~1ms
  SurvivalAnalyzer       - 生存分析（Cox+Levenberg-Marquardt）     ~5ms
"""
from __future__ import annotations

import logging
import math
import threading
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    from .quant_infra import NumpyRingBuffer
except ImportError:
    from quant_infra import NumpyRingBuffer


_ADF_CRITICAL_5PCT = {
    25: -3.00, 30: -2.97, 35: -2.95, 40: -2.93, 50: -2.91,
    60: -2.89, 70: -2.88, 80: -2.87, 90: -2.86, 100: -2.86,
    150: -2.85, 200: -2.85, 250: -2.84, 300: -2.84, 500: -2.83,
}


def _get_adf_critical_5pct(n: int) -> float:
    keys = sorted(_ADF_CRITICAL_5PCT.keys())
    if n <= keys[0]:
        return _ADF_CRITICAL_5PCT[keys[0]]
    for k in keys:
        if n <= k:
            return _ADF_CRITICAL_5PCT[k]
    return _ADF_CRITICAL_5PCT[keys[-1]]


class CointegrationScanner:
    """
    跨品种协整扫描。

    P0修复：方向判断使用双向回归残差方差比较，
    选择残差方差更小的方向（更稳定的协整关系），替代SVD奇异值比。

    时序约束：scan()在update_price()每scan_interval次tick后自动触发。
    调用方必须保证同一组symbol的tick按时间顺序依次调用update_price()，
    不可跨品种乱序推送，否则ADF检验的时序假设被破坏。
    scan_interval最小值为20，防止频繁扫描导致延迟超标。
    """

    __slots__ = ('_lock', '_window', '_price_buffers', '_pairs', '_coint_results', '_scan_interval', '_scan_counter')

    def __init__(self, window: int = 120, scan_interval: int = 50):
        self._lock = threading.RLock()
        self._window = window
        self._price_buffers: Dict[str, NumpyRingBuffer] = {}
        self._pairs: List[Tuple[str, str]] = []
        self._coint_results: Dict[str, Dict[str, Any]] = {}
        self._scan_interval = max(scan_interval, 20)
        self._scan_counter = 0

    def add_symbol(self, symbol: str) -> None:
        with self._lock:
            if symbol not in self._price_buffers:
                self._price_buffers[symbol] = NumpyRingBuffer(self._window)
                self._rebuild_pairs()

    def update_price(self, symbol: str, price: float) -> None:
        if price <= 0:
            return
        with self._lock:
            if symbol not in self._price_buffers:
                self._price_buffers[symbol] = NumpyRingBuffer(self._window)
                self._rebuild_pairs()
            self._price_buffers[symbol].append(price)
            self._scan_counter += 1

    def scan(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            if self._scan_counter < self._scan_interval:
                return dict(self._coint_results)
            self._scan_counter = 0
            results = {}
            for sym_a, sym_b in self._pairs:
                buf_a, buf_b = self._price_buffers.get(sym_a), self._price_buffers.get(sym_b)
                if buf_a is None or buf_b is None or len(buf_a) < 30 or len(buf_b) < 30:
                    continue
                min_len = min(len(buf_a), len(buf_b))
                snap_a = buf_a.snapshot()
                snap_b = buf_b.snapshot()
                prices_a = snap_a[-min_len:]
                prices_b = snap_b[-min_len:]
                result = self._test_cointegration_bidirectional(prices_a, prices_b)
                if result['is_cointegrated']:
                    results[f"{sym_a}_{sym_b}"] = result
            self._coint_results = results
            return dict(results)

    def _test_cointegration_bidirectional(self, y: np.ndarray, x: np.ndarray) -> Dict[str, Any]:
        if len(y) < 30:
            return self._no_coint()
        fwd = self._test_cointegration(y, x)
        rev = self._test_cointegration(x, y)
        if not fwd['is_cointegrated'] and not rev['is_cointegrated']:
            return self._no_coint()
        if fwd['is_cointegrated'] and rev['is_cointegrated']:
            return fwd if fwd.get('spread_variance', float('inf')) <= rev.get('spread_variance', float('inf')) else rev
        return fwd if fwd['is_cointegrated'] else rev

    def _test_cointegration(self, y: np.ndarray, x: np.ndarray) -> Dict[str, Any]:
        n = len(y)
        if n < 30:
            return self._no_coint()
        X = np.column_stack([np.ones(n), x])
        try:
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            return self._no_coint()
        alpha, slope = beta[0], beta[1]
        residuals = y - X @ beta
        adf_stat = self._adf_statistic(residuals)
        critical_5pct = _get_adf_critical_5pct(n)
        half_life = self._estimate_half_life(residuals)
        is_cointegrated = adf_stat < critical_5pct and 1.0 < half_life < 50.0
        spread_std = float(np.std(residuals))
        return {
            'is_cointegrated': is_cointegrated,
            'adf_statistic': float(adf_stat), 'adf_critical_5pct': critical_5pct,
            'coint_alpha': float(alpha), 'coint_beta': float(slope),
            'half_life': float(half_life),
            'current_spread': float(residuals[-1]),
            'spread_zscore': float(residuals[-1] / spread_std) if spread_std > 1e-10 else 0.0,
            'spread_variance': float(np.var(residuals)),
            'n_observations': n,
        }

    def _adf_statistic(self, series: np.ndarray) -> float:
        n = len(series)
        if n < 10:
            return 1.0
        dy = np.diff(series)
        y_lag = series[:-1]
        k = max(min(int(12 * (n / 100) ** 0.25), n - 3), 1)
        X = np.column_stack([np.ones(n - 1), y_lag])
        for lag in range(1, k + 1):
            if lag < n - 1:
                col = np.zeros(n - 1)
                col[lag:] = dy[:n - 1 - lag]
                X = np.column_stack([X, col])
        try:
            beta = np.linalg.lstsq(X, dy, rcond=None)[0]
        except np.linalg.LinAlgError:
            return 1.0
        residuals = dy - X @ beta
        sigma2 = np.sum(residuals ** 2) / max(n - k - 2, 1)
        try:
            # R21-MATH-P1-02修复: 条件数检查 — 共线时使用伪逆(pinv)替代inv
            XtX = X.T @ X
            _cond = np.linalg.cond(XtX)
            if _cond > 1e10:
                logging.warning(
                    "[R21-MATH-P1-02修复] X.T@X条件数过大(%.2e), 使用伪逆替代inv",
                    _cond,
                )
                XtX_inv = np.linalg.pinv(XtX)
            else:
                XtX_inv = np.linalg.inv(XtX)
        except np.linalg.LinAlgError:
            return 1.0
        se_beta1 = np.sqrt(sigma2 * XtX_inv[1, 1]) if XtX_inv[1, 1] > 0 else 1.0
        return float(beta[1] / se_beta1) if se_beta1 > 1e-10 else 1.0

    def _estimate_half_life(self, residuals: np.ndarray) -> float:
        n = len(residuals)
        if n < 10:
            return 999.0
        X = np.column_stack([np.ones(n - 1), residuals[:-1]])
        try:
            beta = np.linalg.lstsq(X, np.diff(residuals), rcond=None)[0]
        except np.linalg.LinAlgError:
            return 999.0
        lam = beta[1]
        if lam >= 0:
            return 999.0
        return max(0.1, min(-math.log(2) / lam, 999.0))

    def _rebuild_pairs(self) -> None:
        symbols = sorted(self._price_buffers.keys())
        self._pairs = [(symbols[i], symbols[j]) for i in range(len(symbols)) for j in range(i + 1, len(symbols))]

    def _no_coint(self) -> Dict[str, Any]:
        return {
            'is_cointegrated': False, 'adf_statistic': 1.0, 'adf_critical_5pct': -2.86,
            'coint_alpha': 0.0, 'coint_beta': 0.0, 'half_life': 999.0,
            'current_spread': 0.0, 'spread_zscore': 0.0, 'spread_variance': 0.0, 'n_observations': 0,
        }


class SurvivalAnalyzer:
    """生存分析，Cox比例风险模型+Levenberg-Marquardt正则化。'
    R21-MATH-P2-08修复 — 收敛判据文档:
    - 最大迭代次数: max_iterations(默认10)
    - 收敛容差: convergence_tol(默认1e-3)
    - 收敛条件: max(|delta|) < convergence_tol，即Newton步长最大分量小于容差
    - 正则化: Levenberg-Marquardt阻尼项 lm_damping(默认1e-4)，Hessian加对角正则化
    - 步长控制: step_size = min(1.0, 1.0/(1+max(|delta|)))，防止大步长发散
    - 条件数保护: Hessian条件数>1e10时使用lstsq替代solve
    """

    _MAX_OBSERVATIONS = 5000

    __slots__ = (
        '_lock', '_max_iterations', '_convergence_tol', '_lm_damping',
        '_events', '_covariates', '_durations',
        '_beta', '_baseline_cumulative_hazard',
        '_is_fitted', '_n_events', '_n_samples',
        '_cached_T', '_cached_E', '_cached_X', '_cache_valid',
    )

    def __init__(self, max_iterations: int = 10, convergence_tol: float = 1e-3, lm_damping: float = 1e-4):
        self._lock = threading.RLock()
        self._max_iterations = max_iterations
        self._convergence_tol = convergence_tol
        self._lm_damping = lm_damping
        self._events: List[int] = []
        self._covariates: List[np.ndarray] = []
        self._durations: List[float] = []
        self._beta: Optional[np.ndarray] = None
        self._baseline_cumulative_hazard: Optional[Dict[float, float]] = None
        self._is_fitted = False
        self._n_events = 0
        self._n_samples = 0
        self._cached_T: Optional[np.ndarray] = None
        self._cached_E: Optional[np.ndarray] = None
        self._cached_X: Optional[np.ndarray] = None
        self._cache_valid = False

    def add_observation(self, duration: float, event: int, covariates: np.ndarray) -> None:
        with self._lock:
            self._durations.append(duration)
            self._events.append(1 if event else 0)
            self._covariates.append(np.array(covariates, dtype=np.float64))
            self._n_samples += 1
            if event:
                self._n_events += 1
            self._is_fitted = False
            self._cache_valid = False
            if self._n_samples > self._MAX_OBSERVATIONS:
                self._durations = self._durations[-self._MAX_OBSERVATIONS:]
                self._events = self._events[-self._MAX_OBSERVATIONS:]
                self._covariates = self._covariates[-self._MAX_OBSERVATIONS:]
                self._n_samples = len(self._durations)
                self._n_events = sum(self._events)
                self._cache_valid = False

    def fit(self) -> Dict[str, Any]:
        with self._lock:
            if self._n_events < 5 or self._n_samples < 10:
                return {'is_fitted': False, 'message': 'Insufficient data'}
            if not self._cache_valid:
                self._cached_T = np.array(self._durations, dtype=np.float64)
                self._cached_E = np.array(self._events, dtype=np.int32)
                self._cached_X = np.array(self._covariates, dtype=np.float64)
                self._cache_valid = True
            T, E, X = self._cached_T, self._cached_E, self._cached_X
            p = X.shape[1]
            beta = self._beta if self._beta is not None else np.zeros(p, dtype=np.float64)
            unique_times = np.sort(np.unique(T[E == 1]))
            sort_idx = np.argsort(T)
            T_sorted, E_sorted, X_sorted = T[sort_idx], E[sort_idx], X[sort_idx]
            risk_set_ends = np.searchsorted(T_sorted, unique_times, side='right')
            delta = np.zeros(p, dtype=np.float64)
            iteration = 0
            for iteration in range(self._max_iterations):
                score = np.zeros(p, dtype=np.float64)
                hessian = np.zeros((p, p), dtype=np.float64)
                risk_scores = np.exp(np.clip(X_sorted @ beta, -50, 50))  # R21-MATH-P2-01修复: 截断边界从[-20,20]放宽到[-50,50]
                XR = X_sorted * risk_scores[:, np.newaxis]
                cum_risk = np.cumsum(risk_scores[::-1])[::-1]
                cum_cov_risk = np.cumsum(X_sorted[::-1] * risk_scores[::-1, np.newaxis], axis=0)[::-1]
                for idx, t in enumerate(unique_times):
                    end = risk_set_ends[idx]
                    events_mask = (T_sorted[:end] == t) & (E_sorted[:end] == 1)
                    risk_sum = cum_risk[0] if end == len(T_sorted) else (cum_risk[end - 1] if end > 0 else 0)
                    cov_risk_sum = cum_cov_risk[0] if end == len(T_sorted) else (cum_cov_risk[end - 1] if end > 0 else np.zeros(p))
                    d_j = np.sum(events_mask)
                    if d_j == 0 or risk_sum < 1e-30:
                        continue
                    x_events_sum = np.sum(X_sorted[:end][events_mask], axis=0)
                    score += d_j * (x_events_sum / d_j - cov_risk_sum / risk_sum)
                    hessian -= d_j / (risk_sum ** 2) * (XR[:end].T @ XR[:end])
                    hessian += d_j * np.outer(cov_risk_sum, cov_risk_sum) / (risk_sum ** 2)
                hessian_reg = hessian - self._lm_damping * np.eye(p)
                try:
                    # R21-MATH-P1-01修复: Hessian条件数检查 — 条件数过大时使用lstsq替代solve
                    _cond = np.linalg.cond(hessian_reg)
                    if _cond > 1e10:
                        logging.warning(
                            "[R21-MATH-P1-01修复] Hessian条件数过大(%.2e), 使用lstsq替代solve",
                            _cond,
                        )
                        delta = np.linalg.lstsq(hessian_reg, score, rcond=None)[0]
                    else:
                        delta = np.linalg.solve(hessian_reg, score)
                except np.linalg.LinAlgError:
                    delta = np.linalg.lstsq(hessian_reg, score, rcond=None)[0]
                step_size = min(1.0, 1.0 / (1.0 + np.max(np.abs(delta))))
                beta += step_size * delta
                if np.max(np.abs(delta)) < self._convergence_tol:
                    break
            self._beta = beta
            risk_scores = np.exp(np.clip(X @ beta, -50, 50))  # R21-MATH-P2-01修复: 截断边界从[-20,20]放宽到[-50,50]
            baseline_hazard = {}
            for t in unique_times:
                d_j = np.sum((T == t) & (E == 1))
                risk_sum = np.sum(risk_scores[T >= t])
                baseline_hazard[t] = d_j / risk_sum if risk_sum > 0 else 0.0
            cum_hazard, cum = {}, 0.0
            for t in sorted(baseline_hazard.keys()):
                cum += baseline_hazard[t]
                cum_hazard[t] = cum
            self._baseline_cumulative_hazard = cum_hazard
            self._is_fitted = True
            se_beta = np.zeros(p)
            try:
                _cov_inv_matrix = -hessian - self._lm_damping * np.eye(p)
                _se_cond = np.linalg.cond(_cov_inv_matrix)
                if _se_cond > 1e10:
                    se_beta = np.sqrt(np.abs(np.diag(np.linalg.pinv(_cov_inv_matrix))))
                else:
                    se_beta = np.sqrt(np.abs(np.diag(np.linalg.inv(_cov_inv_matrix))))
            except np.linalg.LinAlgError:
                try:
                    se_beta = np.sqrt(np.abs(np.diag(np.linalg.pinv(-hessian - self._lm_damping * np.eye(p)))))
                except Exception:
                    pass
            return {
                'is_fitted': True, 'beta': beta.tolist(), 'se_beta': se_beta.tolist(),
                'n_events': self._n_events, 'n_samples': self._n_samples,
                'iterations': iteration + 1, 'converged': np.max(np.abs(delta)) < self._convergence_tol,
            }

    def predict_survival(self, covariates: np.ndarray, time_points: Optional[np.ndarray] = None) -> Dict[str, Any]:
        with self._lock:
            if not self._is_fitted or self._beta is None or self._baseline_cumulative_hazard is None:
                return {'survival_probability': 1.0, 'hazard_ratio': 1.0}
            x = np.array(covariates, dtype=np.float64)
            risk_score = float(np.exp(np.clip(x @ self._beta, -50, 50)))  # R21-MATH-P2-01修复: 截断边界从[-20,20]放宽到[-50,50]
            if time_points is None:
                time_points = np.array([max(self._baseline_cumulative_hazard.keys())])
            surv_probs = []
            for t in time_points:
                cum_h = 0.0
                for ht, hv in self._baseline_cumulative_hazard.items():
                    if ht <= t:
                        cum_h = hv
                    else:
                        break
                surv_probs.append(float(math.exp(-risk_score * cum_h)))
            return {
                'survival_probability': surv_probs[0] if len(surv_probs) == 1 else surv_probs,
                'hazard_ratio': risk_score, 'time_points': time_points.tolist(),
                'survival_curve': surv_probs,
            }

    def get_median_survival(self, covariates: np.ndarray) -> float:
        with self._lock:
            if not self._is_fitted or self._baseline_cumulative_hazard is None:
                return float('inf')
            x = np.array(covariates, dtype=np.float64)
            risk_score = float(np.exp(np.clip(x @ self._beta, -50, 50)))  # R21-MATH-P2-01修复: 截断边界从[-20,20]放宽到[-50,50]
            risk_score = min(risk_score, 1e15)
            target_cum_h = math.log(2) / risk_score if risk_score > 0 else float('inf')
            prev_h, prev_t = 0.0, 0.0
            for t in sorted(self._baseline_cumulative_hazard.keys()):
                h = self._baseline_cumulative_hazard[t] * risk_score
                if h >= target_cum_h:
                    if h - prev_h > 1e-10:
                        return prev_t + (target_cum_h - prev_h) / (h - prev_h) * (t - prev_t)
                    return t
                prev_h, prev_t = h, t
            return float('inf')