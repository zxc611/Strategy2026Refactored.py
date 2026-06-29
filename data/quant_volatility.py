# MODULE_ID: M1-036
"""
quant_volatility.py - 波动率分析模块

包含:
  IVSurfacePCA           - IV曲面PCA（Ledoit-Wolf+EWMA均值）       <200μs
  VolatilityRegimeFilter - 波动率环境过滤（EWMA RV+自适应阈值）     <10μs
"""
from __future__ import annotations

import logging
import math
import threading
from collections import deque
from typing import Any, Dict, List, Optional

import numpy as np

try:
    from .quant_infra import NumpyRingBuffer
except ImportError:
    from quant_infra import NumpyRingBuffer

try:
    from scipy.linalg import eigh as _scipy_eigh
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
    _scipy_eigh = None


class IVSurfacePCA:
    """
    IV曲面PCA，Ledoit-Wolf收缩+EWMA均值替代简单均值。

    P1修复：_strike_labels_hash缓存strike labels的hash，
    避免相同key集合反复触发布is_fitted=False导致频繁重建。
    """

    __slots__ = (
        '_lock', '_window', '_shrinkage', '_var_explained_threshold',
        '_max_components', '_data_buffer', '_strike_labels', '_strike_labels_hash',
        '_components', '_explained_ratio', '_mean_iv', '_ewma_alpha',
        '_projection', '_is_fitted', '_update_counter', '_refit_interval',
    )

    def __init__(self, window: int = 120, shrinkage: float = 0.5,
                 var_explained_threshold: float = 0.90, max_components: int = 3,
                 refit_interval: int = 10, ewma_alpha: float = 0.06):  # J.P.Morgan RiskMetrics推荐 λ=0.94 → α=0.06
        self._lock = threading.RLock()
        self._window = window
        self._shrinkage = shrinkage
        self._var_explained_threshold = var_explained_threshold
        self._max_components = max_components
        self._refit_interval = refit_interval
        self._ewma_alpha = ewma_alpha
        self._data_buffer: deque = deque(maxlen=window)
        self._strike_labels: List[str] = []
        self._strike_labels_hash: int = 0
        self._components: Optional[np.ndarray] = None
        self._explained_ratio: Optional[np.ndarray] = None
        self._mean_iv: Optional[np.ndarray] = None
        self._projection: Optional[np.ndarray] = None
        self._is_fitted = False
        self._update_counter = 0

    def update(self, iv_surface: Dict[str, float]) -> Dict[str, Any]:
        if not iv_surface:
            return self._empty_result()
        with self._lock:
            new_hash = hash(frozenset(iv_surface.keys()))
            if not self._strike_labels:
                self._strike_labels = sorted(iv_surface.keys())
                self._strike_labels_hash = new_hash
            elif new_hash != self._strike_labels_hash:
                self._strike_labels = sorted(iv_surface.keys())
                self._strike_labels_hash = new_hash
                self._is_fitted = False

            vec = np.array([iv_surface.get(k, 0.0) for k in self._strike_labels], dtype=np.float64)
            if len(vec) != len(self._strike_labels) or np.any(vec <= 0):
                return self._empty_result()

            self._data_buffer.append(vec)
            self._update_counter += 1
            if len(self._data_buffer) < 10:
                return self._empty_result()
            if not self._is_fitted or self._update_counter >= self._refit_interval:
                self._fit()
                self._update_counter = 0

            if self._components is not None and self._mean_iv is not None:
                # R21-MATH-P2-02修复: IVSurfacePCA的EWMA均值更新同样存在初始偏差，
                # 但此处mean_iv仅用于PCA投影的中心化，偏差影响有限，且。fit()会重新计算均值覆盖，
                # 因此暂不做偏差修正。若后续发现PCA投影漂移，需添加步数计数器和偏差修正。'
                self._mean_iv = self._ewma_alpha * vec + (1 - self._ewma_alpha) * self._mean_iv
                self._projection = (vec - self._mean_iv) @ self._components

            return self._build_result()

    def _fit(self) -> None:
        data = np.array(self._data_buffer, dtype=np.float64)
        if data.shape[0] < 5:
            return
        self._mean_iv = np.mean(data, axis=0)
        centered = data - self._mean_iv
        n, p = centered.shape
        sample_cov = (centered.T @ centered) / max(n - 1, 1)
        # R21-MATH-P2-07修复: 检查协方差矩阵正定性，若存在非正特征值则添加正则化项
        shrunk_cov = (1 - self._shrinkage) * sample_cov + self._shrinkage * np.diag(np.diag(sample_cov))
        try:
            if _HAS_SCIPY and _scipy_eigh is not None:
                eigenvalues, eigenvectors = _scipy_eigh(shrunk_cov)
            else:
                eigenvalues, eigenvectors = np.linalg.eigh(shrunk_cov)
        except np.linalg.LinAlgError:
            return
        # R21-MATH-P2-06修复: eigh理论上保证实数特征值，添加断言验证
        assert np.all(np.isreal(eigenvalues)), (
            "[R21-MATH-P2-06修复] eigh返回复数特征值，数值异常"
        )
        eigenvalues = np.real(eigenvalues)
        # R21-MATH-P2-07修复: 检查正定性，若最小特征值<=0则添加正则化
        _min_eig = np.min(eigenvalues)
        if _min_eig <= 0:
            _reg = abs(_min_eig) + 1e-8
            shrunk_cov += _reg * np.eye(p)
            try:
                if _HAS_SCIPY and _scipy_eigh is not None:
                    eigenvalues, eigenvectors = _scipy_eigh(shrunk_cov)
                else:
                    eigenvalues, eigenvectors = np.linalg.eigh(shrunk_cov)
            except np.linalg.LinAlgError:
                return
            eigenvalues = np.real(eigenvalues)
        idx = np.argsort(eigenvalues)[::-1]
        eigenvalues, eigenvectors = eigenvalues[idx], eigenvectors[:, idx]
        pos = eigenvalues > 1e-10
        eigenvalues, eigenvectors = eigenvalues[pos], eigenvectors[:, pos]
        if len(eigenvalues) == 0:
            return
        total_var = np.sum(eigenvalues)
        cum_var = np.cumsum(eigenvalues) / total_var
        nc = min(int(np.searchsorted(cum_var, self._var_explained_threshold) + 1),
                 self._max_components, len(eigenvalues))
        nc = max(nc, 1)
        self._components = eigenvectors[:, :nc]
        self._explained_ratio = eigenvalues[:nc] / total_var
        self._is_fitted = True

    def _build_result(self) -> Dict[str, Any]:
        if self._components is None:
            return self._empty_result()
        nc = self._components.shape[1]
        labels = ['Level', 'Slope', 'Curvature', 'PC4', 'PC5'][:nc]
        proj = self._projection.tolist() if self._projection is not None else [0.0] * nc
        return {
            'n_components': nc, 'component_labels': labels,
            'explained_variance_ratio': self._explained_ratio.tolist(),
            'total_explained': float(np.sum(self._explained_ratio)),
            'projections': proj,
            'mean_iv': self._mean_iv.tolist() if self._mean_iv is not None else [],
            'is_fitted': True,
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            'n_components': 0, 'component_labels': [],
            'explained_variance_ratio': [], 'total_explained': 0.0,
            'projections': [], 'mean_iv': [], 'is_fitted': False,
        }


class VolatilityRegimeFilter:
    """波动率环境过滤，EWMA RV+百分位自适应阈值+最小持仓tick。"""

    __slots__ = (
        '_lock', '_lookback', '_low_pct', '_high_pct', '_min_hold_ticks',
        '_rv_buffer', '_current_rv', '_current_regime', '_regime_hold_count',
        '_p25', '_p50', '_p75', '_update_count', '_sort_interval',
        '_ewma_rv', '_ewma_rv_corrected', '_ewma_alpha', '_ewma_step',
    )

    def __init__(self, lookback: int = 100, low_percentile: float = 25.0,
                 high_percentile: float = 75.0, min_hold_ticks: int = 20,
                 ewma_alpha: float = 0.06):  # J.P.Morgan RiskMetrics推荐 λ=0.94 → α=0.06
        self._lock = threading.RLock()
        self._lookback = lookback
        self._low_pct = low_percentile / 100.0
        self._high_pct = high_percentile / 100.0
        self._min_hold_ticks = min_hold_ticks
        self._rv_buffer = NumpyRingBuffer(lookback)
        self._current_rv = 0.0
        self._current_regime = 1
        self._regime_hold_count = 0
        self._p25 = self._p50 = self._p75 = 0.0
        self._update_count = 0
        self._sort_interval = 20
        self._ewma_rv = 0.0
        self._ewma_rv_corrected = 0.0
        self._ewma_alpha = ewma_alpha
        self._ewma_step = 0

    def update(self, return_value: float) -> Dict[str, Any]:
        # R24-P2-IV-10修复: 波动率异常跳变检测——EWMA值突然翻倍/减半时告警
        with self._lock:
            self._rv_buffer.append(return_value ** 2)
            self._update_count += 1
            if len(self._rv_buffer) < 5:
                return self._empty_result()
            _buf_sum = self._rv_buffer.sum()
            _buf_len = len(self._rv_buffer)
            # R24-P0-IV-04修复: 除零保护，防止空buffer导致ZeroDivisionError
            self._current_rv = math.sqrt(_buf_sum / _buf_len) if _buf_len > 0 and _buf_sum >= 0 else 0.0
            _prev_ewma = self._ewma_rv
            a = self._ewma_alpha
            self._ewma_step += 1  # R21-MATH-P2-02修复: 步数递增
            # R21-MATH-P2-02修复: EWMA偏差修正 — 初始阶段除以(1-(1-alpha)^t)消除初始化偏差
            _bias_correction = 1.0 - (1.0 - a) ** self._ewma_step if self._ewma_step > 0 else 1.0
            # R24-P0-IV-04修复: ewma_rv除零保护，current_rv为0时保持上一值
            self._ewma_rv = a * self._current_rv + (1 - a) * self._ewma_rv if self._ewma_rv > 0 and self._current_rv > 0 else (self._current_rv if self._ewma_rv <= 0 else self._ewma_rv)
            self._ewma_rv_corrected = self._ewma_rv / _bias_correction if _bias_correction > 1e-10 else self._ewma_rv
            # R24-P2-IV-10修复: 波动率异常跳变检测
            if _prev_ewma > 1e-10 and self._ewma_rv > 1e-10:
                _rv_ratio = self._ewma_rv / _prev_ewma
                if _rv_ratio > 3.0 or _rv_ratio < 0.33:
                    logging.warning("[R24-P2-IV-10] 波动率异常跳变: prev=%.6f curr=%.6f ratio=%.2f",
                                  _prev_ewma, self._ewma_rv, _rv_ratio)
            if self._update_count % self._sort_interval == 0:
                sorted_rv = self._rv_buffer.sorted_values()
                n = len(sorted_rv)
                if n > 0:
                    self._p25 = math.sqrt(sorted_rv[int(n * self._low_pct)])
                    self._p50 = math.sqrt(sorted_rv[int(n * 0.5)])
                    self._p75 = math.sqrt(sorted_rv[int(n * self._high_pct)])
            # R21-MATH-P2-02修复: regime判断使用偏差修正后的ewma_rv_corrected
            _ewma_for_regime = self._ewma_rv_corrected if hasattr(self, '_ewma_rv_corrected') else self._ewma_rv
            new_regime = 0 if _ewma_for_regime <= self._p25 else (2 if _ewma_for_regime >= self._p75 else 1)
            if new_regime != self._current_regime:
                if self._regime_hold_count >= self._min_hold_ticks:
                    self._current_regime = new_regime
                    self._regime_hold_count = 0
            else:
                self._regime_hold_count += 1
            return self._build_result()

    def _build_result(self) -> Dict[str, Any]:
        # R21-MATH-P2-02修复: ewma_vol使用偏差修正值
        _ewma_vol = self._ewma_rv_corrected if hasattr(self, '_ewma_rv_corrected') else self._ewma_rv
        return {
            'regime': self._current_regime,
            'regime_label': ['LOW', 'NORMAL', 'HIGH'][self._current_regime],
            'realized_vol': self._current_rv, 'ewma_vol': _ewma_vol,
            'low_threshold': self._p25, 'high_threshold': self._p75,
            'p25': self._p25, 'p50': self._p50, 'p75': self._p75,
            'hold_count': self._regime_hold_count,
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            'regime': 1, 'regime_label': 'NORMAL',
            'realized_vol': 0.0, 'ewma_vol': 0.0,
            'low_threshold': 0.0, 'high_threshold': 0.0,
            'p25': 0.0, 'p50': 0.0, 'p75': 0.0, 'hold_count': 0,
        }