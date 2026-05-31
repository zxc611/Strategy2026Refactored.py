"""
quant_core.py - 量化系统核心算法模块

包含6个核心量化模块：
1. MultiPeriodTrendScorer  - 多周期趋势评分（Wilder ADX+动态EMA）   <100μs
2. IVSurfacePCA           - IV曲面PCA（Ledoit-Wolf+EWMA均值）       <200μs
3. AdaptiveHMM            - 自适应HMM（异步EM+方差地板）             <50μs
4. VolatilityRegimeFilter - 波动率环境过滤（EWMA RV+自适应阈值）     <10μs
5. CointegrationScanner   - 跨品种协整扫描（双向ADF+残差方差选向）   ~1ms
6. SurvivalAnalyzer       - 生存分析（Cox+Levenberg-Marquardt）     ~5ms

R21-MATH-P2-05修复说明: FFT频率混叠检查 — 当前代码库中未使用FFT/detect_cycle，
若后续引入FFT频谱分析（如周期检测），必须在FFT前做抗混叠处理：
(1) 检查采样间隔一致性，非均匀采样需先插值重采样；
(2) 对输入数据做低通滤波（截止频率≤奈奎斯特频率）；
(3) 验证频谱能量在奈奎斯特频率处衰减至可忽略水平。
"""
from __future__ import annotations

import logging
import math
import threading
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    from .quant_infra import NumpyRingBuffer, rate_limit_log
except ImportError:
    from quant_infra import NumpyRingBuffer, rate_limit_log

# OPTIONAL-DEPENDENCY: scipy.linalg用于HMM等高级数学计算
try:
    from scipy.linalg import eigh as _scipy_eigh
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
    _scipy_eigh = None


# ============================================================================
# 1. MultiPeriodTrendScorer
# ============================================================================

class MultiPeriodTrendScorer:
    """
    多周期趋势评分器。

    P0修复：Wilder初始化使用前adx_period个值的简单平均，
    而非第一个TR直接赋值（导致ATR/DM初始值偏差巨大）。
    """

    __slots__ = (
        '_lock', '_periods', '_weights', '_wilder_alpha', '_ema_alphas', '_adx_period',
        '_atr_buffers', '_plus_dm_buffers', '_minus_dm_buffers',
        '_dx_buffers', '_adx_buffers', '_ema_buffers',
        '_vol_buffers', '_dynamic_periods',
        '_prices', '_highs', '_lows',
        '_initialized', '_tick_count',
        '_init_tr_accum', '_init_plus_dm_accum', '_init_minus_dm_accum', '_init_count',
    )

    def __init__(self, periods: Tuple[int, ...] = (5, 20, 60),
                 weights: Tuple[float, ...] = (0.2, 0.5, 0.3),
                 adx_period: int = 14):
        self._lock = threading.RLock()
        self._periods = periods
        self._weights = np.array(weights, dtype=np.float64)
        self._weights /= self._weights.sum()
        self._wilder_alpha = 1.0 / adx_period
        self._adx_period = adx_period
        self._ema_alphas = [2.0 / (p + 1) for p in periods]

        n = len(periods)
        self._atr_buffers = [0.0] * n
        self._plus_dm_buffers = [0.0] * n
        self._minus_dm_buffers = [0.0] * n
        self._dx_buffers = [0.0] * n
        self._adx_buffers = [0.0] * n
        self._ema_buffers = [0.0] * n
        self._vol_buffers = [NumpyRingBuffer(20) for _ in range(n)]
        self._dynamic_periods = list(periods)

        self._prices = NumpyRingBuffer(3)
        self._highs = NumpyRingBuffer(3)
        self._lows = NumpyRingBuffer(3)
        self._initialized = False
        self._tick_count = 0
        self._init_tr_accum = [0.0] * n
        self._init_plus_dm_accum = [0.0] * n
        self._init_minus_dm_accum = [0.0] * n
        self._init_count = 0

    def update(self, high: float, low: float, close: float) -> Dict[str, Any]:
        # R24-P1-IV-02修复: close/high/low价格过滤增加NaN/Inf检查
        import math
        if (close <= 0 or high < low
            or (isinstance(close, float) and (math.isnan(close) or math.isinf(close)))
            or (isinstance(high, float) and (math.isnan(high) or math.isinf(high)))
            or (isinstance(low, float) and (math.isnan(low) or math.isinf(low)))):
            return self._empty_result()
        with self._lock:
            self._prices.append(close)
            self._highs.append(high)
            self._lows.append(low)
            self._tick_count += 1
            if len(self._prices) < 3:
                return self._empty_result()
            return self._compute()

    def _compute(self) -> Dict[str, Any]:
        prices_snap = self._prices.snapshot()
        highs_snap = self._highs.snapshot()
        lows_snap = self._lows.snapshot()

        prev_high, prev_low = highs_snap[-2], lows_snap[-2]
        curr_high, curr_low = highs_snap[-1], lows_snap[-1]
        curr_close, prev_close = prices_snap[-1], prices_snap[-2]

        tr = max(curr_high - curr_low, abs(curr_high - prev_close), abs(curr_low - prev_close))
        up_move = curr_high - prev_high
        down_move = prev_low - curr_low
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0

        scores = [0.0] * len(self._periods)
        adx_values = [0.0] * len(self._periods)
        ema_values = [0.0] * len(self._periods)

        for i in range(len(self._periods)):
            if not self._initialized:
                self._init_tr_accum[i] += tr
                self._init_plus_dm_accum[i] += plus_dm
                self._init_minus_dm_accum[i] += minus_dm
                self._ema_buffers[i] = curr_close
                continue

            a = self._wilder_alpha
            self._atr_buffers[i] = a * tr + (1 - a) * self._atr_buffers[i]
            self._plus_dm_buffers[i] = a * plus_dm + (1 - a) * self._plus_dm_buffers[i]
            self._minus_dm_buffers[i] = a * minus_dm + (1 - a) * self._minus_dm_buffers[i]

            atr = self._atr_buffers[i]
            if atr > 0:
                plus_di = 100.0 * self._plus_dm_buffers[i] / atr
                minus_di = 100.0 * self._minus_dm_buffers[i] / atr
                di_sum = plus_di + minus_di
                dx = 100.0 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0.0
            else:
                plus_di = minus_di = dx = 0.0

            self._dx_buffers[i] = dx
            self._adx_buffers[i] = a * dx + (1 - a) * self._adx_buffers[i]

            momentum = (curr_close - prev_close) / prev_close if prev_close > 1e-10 else 0.0  # R24-P2-IV-02修复: 使用1e-10替代0防止极端低价合约产生巨大中间值
            adx_enhanced = min(self._adx_buffers[i] * (1.0 + 0.3 * abs(momentum) * 100.0), 100.0)

            self._vol_buffers[i].append(tr)
            vbuf = self._vol_buffers[i]
            cv = vbuf.std() / vbuf.mean() if len(vbuf) > 2 and vbuf.mean() > 0 else 1.0
            cv = min(cv, 3.0)

            dynamic_period = max(3, int(self._periods[i] / (1.0 + cv)))
            self._dynamic_periods[i] = dynamic_period
            dynamic_alpha = 2.0 / (dynamic_period + 1)
            self._ema_buffers[i] = dynamic_alpha * curr_close + (1 - dynamic_alpha) * self._ema_buffers[i]

            trend_dir = 1.0 if curr_close > self._ema_buffers[i] else -1.0
            scores[i] = trend_dir * adx_enhanced / 100.0
            adx_values[i] = adx_enhanced
            ema_values[i] = self._ema_buffers[i]

        if not self._initialized and self._tick_count >= 3:
            self._init_count += 1
            if self._init_count >= self._adx_period:
                for i in range(len(self._periods)):
                    cnt = max(self._init_count, 1)
                    self._atr_buffers[i] = self._init_tr_accum[i] / cnt
                    self._plus_dm_buffers[i] = self._init_plus_dm_accum[i] / cnt
                    self._minus_dm_buffers[i] = self._init_minus_dm_accum[i] / cnt
                    dx_sum = self._plus_dm_buffers[i] + self._minus_dm_buffers[i]
                    if dx_sum > 0 and self._atr_buffers[i] > 0:
                        plus_di = 100.0 * self._plus_dm_buffers[i] / self._atr_buffers[i]
                        minus_di = 100.0 * self._minus_dm_buffers[i] / self._atr_buffers[i]
                        self._dx_buffers[i] = 100.0 * abs(plus_di - minus_di) / (plus_di + minus_di)
                    self._adx_buffers[i] = self._dx_buffers[i]
                self._initialized = True

        composite = max(-1.0, min(1.0, sum(w * s for w, s in zip(self._weights, scores))))
        return {
            'composite_score': composite,
            'trend_direction': 'UP' if composite > 0.05 else ('DOWN' if composite < -0.05 else 'FLAT'),
            'strength': abs(composite),
            'period_scores': scores,
            'adx_values': adx_values,
            'ema_values': ema_values,
            'dynamic_periods': list(self._dynamic_periods),
            'ema_warmup': not self._initialized,
        }

    def _empty_result(self) -> Dict[str, Any]:
        n = len(self._periods)
        return {
            'composite_score': 0.0, 'trend_direction': 'FLAT', 'strength': 0.0,
            'period_scores': [0.0] * n, 'adx_values': [0.0] * n,
            'ema_values': [0.0] * n, 'dynamic_periods': list(self._periods),
        }


# ============================================================================
# 2. IVSurfacePCA
# ============================================================================

class IVSurfacePCA:
    """
    IV曲面PCA，Ledoit-Wolf收缩+EWMA均值替代简单均值。

    P1修复：_strike_labels_hash缓存strike labels的hash，
    避免相同key集合反复触发_is_fitted=False导致频繁重建。
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
                # 但此处mean_iv仅用于PCA投影的中心化，偏差影响有限，且_fit()会重新计算均值覆盖，
                # 因此暂不做偏差修正。若后续发现PCA投影漂移，需添加步数计数器和偏差修正。
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


# ============================================================================
# 3. AdaptiveHMM
# ============================================================================

class AdaptiveHMM:
    """
    自适应HMM，异步EM+方差地板。

    P0修复：EM计算时先拷贝数据释放锁，再在无锁状态下计算，
    最后加锁写回结果，避免持锁执行EM导致update阻塞。
    """

    __slots__ = (
        '_lock', '_n_states', '_update_interval',
        '_transition', '_means', '_variances',
        '_initial_prob', '_log_transition', '_log_initial',
        '_observation_buffer', '_forward_alpha',
        '_current_posterior', '_current_state',
        '_observation_count', '_last_em_count', '_needs_em',
        '_em_thread', '_em_running', '_em_learning_rate',
        '_var_floor_ratio', '_min_variance',
    )

    def __init__(self, n_states: int = 3, update_interval: int = 100,
                 var_floor_ratio: float = 0.01, min_variance: float = 1e-10,
                 hmm_transition_matrix=None, hmm_means=None, hmm_variances=None):
        self._lock = threading.RLock()
        self._n_states = n_states
        self._update_interval = update_interval
        self._transition = np.array(hmm_transition_matrix, dtype=np.float64) if hmm_transition_matrix is not None else np.array([[0.90, 0.08, 0.02], [0.05, 0.85, 0.10], [0.02, 0.08, 0.90]], dtype=np.float64)
        self._means = np.array(hmm_means, dtype=np.float64) if hmm_means is not None else np.array([0.0001, 0.0005, 0.002], dtype=np.float64)
        self._variances = np.array(hmm_variances, dtype=np.float64) if hmm_variances is not None else np.array([1e-6, 5e-6, 2e-5], dtype=np.float64)
        self._initial_prob = np.ones(n_states, dtype=np.float64) / n_states
        self._log_transition = np.log(self._transition + 1e-30)
        self._log_initial = np.log(self._initial_prob + 1e-30)
        self._observation_buffer = NumpyRingBuffer(500)
        self._forward_alpha = np.log(self._initial_prob + 1e-30)
        self._current_posterior = np.ones(n_states, dtype=np.float64) / n_states
        self._current_state = 1
        self._observation_count = 0
        self._last_em_count = 0
        self._needs_em = False
        self._em_thread: Optional[threading.Thread] = None
        self._em_running = False
        self._var_floor_ratio = var_floor_ratio
        self._min_variance = min_variance
        self._em_learning_rate = 0.1
        # R27-P0-FC-02修复: 转移矩阵行和验证标志
        self._transition_validated: bool = False
        # R27-P0-FC-03修复: 状态驻留时间监控
        self._state_entry_time: Dict[int, float] = {}
        self._state_dwell_times: Dict[int, List[float]] = {k: [] for k in range(n_states)}
        self._max_dwell_times: Dict[int, float] = {k: 3600.0 for k in range(n_states)}  # 默认1小时告警

    def update(self, observation: float) -> Dict[str, Any]:
        if not math.isfinite(observation):
            return self._empty_result()
        with self._lock:
            # R27-P0-FC-02修复: 转移矩阵行和验证(每1000次观测)
            if not self._transition_validated and self._observation_count % 1000 == 0:
                self._validate_transition_matrix()
            self._observation_buffer.append(observation)
            self._observation_count += 1
            log_obs = self._log_emission_prob_list(observation)
            K = self._n_states
            new_alpha = [-1e30] * K
            for j in range(K):
                best = max(self._forward_alpha[i] + self._log_transition[i, j] for i in range(K))
                new_alpha[j] = best + log_obs[j]
            max_alpha = max(new_alpha)
            if max_alpha > -1e29:
                exp_sum = sum(math.exp(a - max_alpha) for a in new_alpha)
                log_norm = math.log(exp_sum)
                new_alpha = [a - max_alpha - log_norm for a in new_alpha]
            else:
                new_alpha = [math.log(1.0 / K)] * K
            self._forward_alpha = np.array(new_alpha, dtype=np.float64)
            exp_sum = sum(math.exp(a - max_alpha) for a in new_alpha) if max_alpha > -1e29 else 1.0
            posterior = [math.exp(a - max_alpha) / exp_sum for a in new_alpha] if max_alpha > -1e29 else [1.0 / K] * K
            self._current_posterior = np.array(posterior, dtype=np.float64)
            prev_state = self._current_state
            self._current_state = max(range(K), key=lambda k: posterior[k])
            # R27-P0-FC-03修复: 状态驻留时间监控
            import time as _time
            now = _time.time()
            if prev_state != self._current_state:
                if prev_state in self._state_entry_time:
                    dwell = now - self._state_entry_time[prev_state]
                    self._state_dwell_times[prev_state].append(dwell)
                    if len(self._state_dwell_times[prev_state]) > 100:
                        self._state_dwell_times[prev_state] = self._state_dwell_times[prev_state][-50:]
                self._state_entry_time[self._current_state] = now
            elif self._current_state not in self._state_entry_time:
                self._state_entry_time[self._current_state] = now
            # 驻留超限告警
            for k in range(K):
                if k in self._state_entry_time:
                    dwell = now - self._state_entry_time[k]
                    if dwell > self._max_dwell_times.get(k, 3600.0) and k == self._current_state:
                        if int(dwell) % 300 < 2:  # 每5分钟告警一次
                            logging.warning("[R27-P0-FC-03] HMM状态%d驻留超限: %.0fs>%.0fs",
                                            k, dwell, self._max_dwell_times.get(k, 3600.0))
            if self._observation_count - self._last_em_count >= self._update_interval:
                self._needs_em = True
                self._last_em_count = self._observation_count
            return self._build_result()

    def run_em_if_needed(self) -> None:
        with self._lock:
            if self._needs_em and not self._em_running:
                self._em_running = True
                self._em_thread = threading.Thread(target=self._async_em_worker, daemon=True)
                self._em_thread.start()

    # R27-P0-FC-02修复: 转移矩阵行和验证
    def _validate_transition_matrix(self) -> bool:
        """验证转移矩阵每行和为1(容差1e-6)，不满足则归一化修正"""
        try:
            row_sums = self._transition.sum(axis=1)
            tol = 1e-6
            valid = all(abs(rs - 1.0) < tol for rs in row_sums)
            if not valid:
                logging.warning(
                    "[R27-P0-FC-02] HMM转移矩阵行和不为1: %s, 执行归一化修正",
                    [f"{rs:.6f}" for rs in row_sums]
                )
                for i in range(self._n_states):
                    rs = self._transition[i].sum()
                    if rs > 0:
                        self._transition[i] /= rs
                self._log_transition = np.log(self._transition + 1e-30)
            self._transition_validated = True
            return valid
        except Exception as e:
            logging.warning("[R27-P0-FC-02] 转移矩阵验证异常: %s", e)
            return False

    def _async_em_worker(self) -> None:
        try:
            with self._lock:
                if not self._needs_em:
                    return
                obs_snapshot = self._observation_buffer.snapshot().copy()
                params_snapshot = {  # R21-MEM-P2-11修复: 级联复制 — snapshot().copy() + 6个ndarray.copy()，共7次内存拷贝；可考虑只读引用+写时复制
                    'transition': self._transition.copy(), 'means': self._means.copy(),
                    'variances': self._variances.copy(), 'initial_prob': self._initial_prob.copy(),
                    'log_transition': self._log_transition.copy(), 'log_initial': self._log_initial.copy(),
                    'n_states': self._n_states, 'var_floor_ratio': self._var_floor_ratio,
                    'min_variance': self._min_variance,
                }
            new_params = self._compute_em_step(obs_snapshot, params_snapshot)
            with self._lock:
                if new_params is not None:
                    lr = self._em_learning_rate
                    self._means = (1 - lr) * self._means + lr * new_params['means']
                    self._variances = (1 - lr) * self._variances + lr * new_params['variances']
                    self._transition = (1 - lr) * self._transition + lr * new_params['transition']
                    self._initial_prob = (1 - lr) * self._initial_prob + lr * new_params['initial_prob']
                    self._initial_prob /= np.sum(self._initial_prob)
                    self._apply_variance_floor()
                    self._log_transition = np.log(self._transition + 1e-30)
                    self._log_initial = np.log(self._initial_prob + 1e-30)
                    self._forward_alpha = np.log(self._current_posterior + 1e-30)
                self._needs_em = False
        except Exception as e:
            rate_limit_log(logging.getLogger(), logging.ERROR, f"[AdaptiveHMM] EM error: {e}", "hmm_em_error", 60.0)
        finally:
            self._em_running = False

    def _apply_variance_floor(self) -> None:
        for k in range(self._n_states):
            floor = max(self._means[k] ** 2 * self._var_floor_ratio, self._min_variance)
            if self._variances[k] < floor:
                self._variances[k] = floor

    _LOG_OBS_FLOOR = -500.0

    def _log_emission_prob_list(self, obs: float) -> List[float]:
        result = [0.0] * self._n_states
        for k in range(self._n_states):
            var = max(self._variances[k], self._min_variance)
            diff = obs - self._means[k]
            log_p = -0.5 * math.log(2 * math.pi * var) - 0.5 * diff * diff / var
            result[k] = max(log_p, self._LOG_OBS_FLOOR)
        return result

    def _compute_em_step(self, obs_arr: np.ndarray, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """EM单步迭代（前向-后向算法+E步+M步）

        R21-MEM-P2-14修复 — 递归深度/栈溢出风险评估:
        本方法为纯循环实现（for循环遍历观测序列），无递归调用，无栈溢出风险。
        前向-后向算法的时间复杂度O(n*K^2)，空间复杂度O(n*K)，
        n受max_em_obs=200限制，K为状态数(默认3)，不会导致调用栈溢出。
        注意：若未来改为递归实现前向/后向算法，需在入口检查
        sys.getrecursionlimit()，确保递归深度不超过限制(默认1000)。

        R21-MATH-P2-08修复 — 收敛判据文档:
        本方法执行单次EM迭代（非迭代至收敛），收敛由调用方控制：
        - 触发条件: 每_update_interval(默认100)个观测触发一次EM
        - 学习率: _em_learning_rate=0.3，新旧参数加权混合，隐式控制收敛速度
        - 方差地板: _apply_variance_floor()防止方差退化至0
        - 无显式收敛判据: EM在后台线程单步执行，不检查对数似然变化量
        - 已知限制: 无法保证EM收敛到全局最优，学习率0.3可能过大导致振荡
        """
        n = len(obs_arr)
        if n < 10:
            return None
        max_em_obs = 200
        if n > max_em_obs:
            obs_arr = obs_arr[-max_em_obs:]
            n = len(obs_arr)

        K = params['n_states']
        transition, means, variances = params['transition'], params['means'], params['variances']
        initial_prob, log_transition, log_initial = params['initial_prob'], params['log_transition'], params['log_initial']

        log_alpha = np.full((n, K), -np.inf)
        log_beta = np.full((n, K), -np.inf)

        diff0 = obs_arr[0] - means
        log_obs0 = -0.5 * np.log(2 * np.pi * variances) - 0.5 * diff0 * diff0 / variances
        log_alpha[0] = log_initial + log_obs0

        for t in range(1, n):
            diff_t = obs_arr[t] - means
            log_obs = -0.5 * np.log(2 * np.pi * variances) - 0.5 * diff_t * diff_t / variances
            for j in range(K):
                temp = log_alpha[t - 1] + log_transition[:, j]
                mt = np.max(temp)
                if mt > -np.inf:
                    log_alpha[t, j] = mt + np.log(np.sum(np.exp(temp - mt))) + log_obs[j]

        log_beta[-1] = 0.0
        for t in range(n - 2, -1, -1):
            diff_next = obs_arr[t + 1] - means
            log_obs_next = -0.5 * np.log(2 * np.pi * variances) - 0.5 * diff_next * diff_next / variances
            for i in range(K):
                temp = log_transition[i, :] + log_obs_next + log_beta[t + 1]
                mt = np.max(temp)
                if mt > -np.inf:
                    log_beta[t, i] = mt + np.log(np.sum(np.exp(temp - mt)))

        log_gamma = log_alpha + log_beta
        for t in range(n):
            row = log_gamma[t]
            mr = np.max(row)
            if mr > -np.inf:
                log_gamma[t] = row - mr - np.log(np.sum(np.exp(row - mr)))
            else:
                log_gamma[t] = -np.log(K)

        gamma = np.exp(log_gamma)
        new_means, new_vars, new_initial = np.zeros(K), np.zeros(K), gamma[0]

        for k in range(K):
            w = gamma[:, k]
            ws = np.sum(w)
            if ws > 1e-10:
                new_means[k] = np.sum(w * obs_arr) / ws
                new_vars[k] = np.sum(w * (obs_arr - new_means[k]) ** 2) / ws
            else:
                new_means[k], new_vars[k] = means[k], variances[k]

        new_trans = np.copy(transition)
        if n > 1:
            gamma_i_sums = np.sum(gamma[:n - 1, :K], axis=0)
            for i in range(K):
                for j in range(K):
                    log_xi_terms = []
                    for t in range(n - 1):
                        diff_next = obs_arr[t + 1] - new_means
                        log_obs_next = -0.5 * np.log(2 * np.pi * new_vars) - 0.5 * diff_next * diff_next / new_vars
                        log_xi_terms.append(log_alpha[t, i] + log_transition[i, j] + log_obs_next[j] + log_beta[t + 1, j])
                    if log_xi_terms:
                        ml = max(log_xi_terms)
                        xs = sum(math.exp(lx - ml) for lx in log_xi_terms)
                        lxt = ml + math.log(xs) if xs > 0 else -1e30
                    else:
                        lxt = -1e30
                    gi = gamma_i_sums[i]
                    if gi > 1e-10:
                        ea = max(-500.0, min(500.0, lxt - math.log(gi)))
                        new_trans[i, j] = math.exp(ea)
                    else:
                        new_trans[i, j] = transition[i, j]
            for i in range(K):
                rs = np.sum(new_trans[i])
                if rs > 0:
                    new_trans[i] /= rs
                else:
                    new_trans[i] = 1.0 / K

        return {'means': new_means, 'variances': new_vars, 'transition': new_trans, 'initial_prob': new_initial}

    def _build_result(self) -> Dict[str, Any]:
        state_labels = ['LOW_VOL', 'NORMAL', 'HIGH_VOL']
        result = {
            'state': self._current_state,
            'state_label': state_labels[self._current_state] if self._current_state < len(state_labels) else f'STATE_{self._current_state}',
            'posterior': self._current_posterior.tolist(),
            'means': self._means.tolist(), 'variances': self._variances.tolist(),
            'transition_diag': np.diag(self._transition).tolist(),
            'observation_count': self._observation_count,
        }
        # DFG-P1-01修复: HMM五态标签通过EventBus发布事件，供信号服务和风控消费
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.publish('hmm.state_changed', {
                    'type': 'hmm.state_changed',
                    'state': result['state'],
                    'state_label': result['state_label'],
                    'posterior': result['posterior'],
                    'observation_count': result['observation_count'],
                }, async_mode=True)
        except Exception:
            pass
        return result

    def _empty_result(self) -> Dict[str, Any]:
        return {
            'state': 1, 'state_label': 'NORMAL',
            'posterior': [1.0 / self._n_states] * self._n_states,
            'means': self._means.tolist(), 'variances': self._variances.tolist(),
            'transition_diag': np.diag(self._transition).tolist(),
            'observation_count': self._observation_count,
        }


# ============================================================================
# 4. VolatilityRegimeFilter
# ============================================================================

class VolatilityRegimeFilter:
    """波动率环境过滤，EWMA RV+百分位自适应阈值+最小持仓tick。"""

    __slots__ = (
        '_lock', '_lookback', '_low_pct', '_high_pct', '_min_hold_ticks',
        '_rv_buffer', '_current_rv', '_current_regime', '_regime_hold_count',
        '_p25', '_p50', '_p75', '_update_count', '_sort_interval',
        '_ewma_rv', '_ewma_alpha', '_ewma_step',
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
        self._ewma_alpha = ewma_alpha
        self._ewma_step = 0  # R21-MATH-P2-02修复: EWMA步数计数器，用于偏差修正

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


# ============================================================================
# 5. CointegrationScanner
# ============================================================================

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


# ============================================================================
# 6. SurvivalAnalyzer
# ============================================================================

class SurvivalAnalyzer:
    """生存分析，Cox比例风险模型+Levenberg-Marquardt正则化。

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
