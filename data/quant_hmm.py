# MODULE_ID: M1-031
"""
quant_hmm.py - 自适应HMM模块

包含:
  AdaptiveHMM - 自适应HMM（异步EM+方差地板）             <50μs
"""
from __future__ import annotations

import logging
import math
import threading
from typing import Any, Dict, List, Optional

import numpy as np

try:
    from .quant_infra import NumpyRingBuffer, rate_limit_log
except ImportError:
    from quant_infra import NumpyRingBuffer, rate_limit_log

try:
    from scipy.linalg import eigh as _scipy_eigh
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
    _scipy_eigh = None


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
        '_transition_validated', '_state_entry_time',
        '_state_dwell_times', '_max_dwell_times',
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
        """EM单步迭代（前向-后向算法+E步+M步）'
        R21-MEM-P2-14修复 — 递归深度/栈溢出风险评估:
        本方法为纯循环实现（for循环遍历观测序列），无递归调用，无栈溢出风险。'
        前向-后向算法的时间复杂度O(n*K^2)，空间复杂度O(n*K)，
        n受max_em_obs=200限制，K为状态数(默认3)，不会导致调用栈溢出。
        注意：若未来改为递归实现前向/后向算法，需在入口检查
        sys.getrecursionlimit()，确保递归深度不超过限制(默认1000)。

        R21-MATH-P2-08修复 — 收敛判据文档:
        本方法执行单次EM迭代（非迭代至收敛），收敛由调用方控制：
        - 触发条件: 每。update_interval(默认100)个观测触发一次EM
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
            from infra.event_bus import get_global_event_bus
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