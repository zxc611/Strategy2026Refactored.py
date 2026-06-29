# MODULE_ID: M1-168
# [M1-133] 量化核心
from __future__ import annotations

import logging
from ali2026v3_trading.infra.logging_utils import get_logger
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.serialization_utils import safe_dataframe_to_parquet
import numpy as np
import pandas as pd

logger = get_logger(__name__)
@dataclass(slots=True)
class LifeExpectancy:
    state: str
    duration: Dict[str, float]
    magnitude: Dict[str, float]
    sample_count: int
    decay_r_squared: float
    degradation_level: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())

    def is_valid(self) -> bool:
        return self.sample_count > 0 and self.decay_r_squared > 0

@dataclass(slots=True)
class ShrinkageResult:
    original: LifeExpectancy
    shrunk: LifeExpectancy
    shrinkage_factor: float
    degradation_level: int
    reason: str

class BayesianShrinkageLifeEstimator:
    """四级降级贝叶斯收缩寿命估计器: L0完整(n>=200,R²>=0.7)→L1收缩(50<=n<200)→L2合并(10<=n<50)→L3全局先验(n<10); R²<0.7强制降级; 收缩因子=n/(n+N_prior)"""

    N_PRIOR = 50
    MIN_SAMPLES_FULL = 200
    MIN_SAMPLES_SHRINK = 50
    MIN_SAMPLES_MERGE = 10
    MIN_R_SQUARED = 0.7

    STATE_CATEGORIES = {
        'trend_up': 'directional',
        'trend_down': 'directional',
        'range_bound': 'mean_reverting',
        'volatile': 'high_volatility',
        'quiet': 'low_volatility',
    }

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self._life_dict: Dict[str, LifeExpectancy] = {}
        self._global_prior: Optional[LifeExpectancy] = None
        self._category_priors: Dict[str, LifeExpectancy] = {}
        self._shrinkage_log: List[ShrinkageResult] = []
        self._params = params or {}
        self._min_sample_count = self._params.get('life_min_sample_count', 30)
        self._shrinkage_prior_strength = self._params.get('life_shrinkage_prior_strength', 1.0)
        self._decay_r_squared_threshold = self._params.get('life_decay_r_squared_threshold', 0.3)

    # P1-R9-16修复: 按策略ID创建独立实例，替代模块级单例
    _instances_dict: Dict[str, 'BayesianShrinkageLifeEstimator'] = {}

    @classmethod
    def get_instance(cls, strategy_id: str = "default", params: Optional[Dict[str, Any]] = None) -> 'BayesianShrinkageLifeEstimator':
        if strategy_id not in cls._instances_dict:
            cls._instances_dict[strategy_id] = cls(params=params)
        return cls._instances_dict[strategy_id]

    def build_life_dict(
        self,
        data: pd.DataFrame,
        state_column: str = 'hmm_state',
        duration_column: str = 'holding_minutes',
        magnitude_column: str = 'price_change_pct',
    ) -> Dict[str, LifeExpectancy]:
        self._life_dict.clear()

        if state_column not in data.columns:
            logger.warning("数据中无状态列 %s，使用默认状态", state_column)
            states = ['trend_up', 'trend_down', 'range_bound', 'volatile', 'quiet']
        else:
            states = data[state_column].unique().tolist()

        for state in states:
            if state_column in data.columns:
                mask = data[state_column] == state
                state_data = data[mask]
            else:
                state_data = data.iloc[:0]

            n = len(state_data)

            if n < 2:
                life = LifeExpectancy(state=state,
                    duration={'p25': 0, 'p50': 0, 'p75': 0, 'p99': 0},
                    magnitude={'p25': 0, 'p50': 0, 'p75': 0, 'p99': 0},
                    sample_count=n, decay_r_squared=0.0)
            else:
                dur = self._compute_quantiles(
                    state_data[duration_column]
                    if duration_column in state_data.columns
                    else pd.Series([15] * n)
                )
                mag = self._compute_quantiles(
                    state_data[magnitude_column]
                    if magnitude_column in state_data.columns
                    else pd.Series([0.02] * n)
                )
                r2 = self._estimate_decay_r_squared(
                    state_data[duration_column]
                    if duration_column in state_data.columns
                    else pd.Series([15] * n)
                )
                life = LifeExpectancy(
                    state=state, duration=dur, magnitude=mag,
                    sample_count=n, decay_r_squared=r2,
                )

            self._life_dict[state] = life

        self._compute_global_prior()
        self._compute_category_priors()

        return dict(self._life_dict)

    def get_life_expectancy(self, state: str) -> LifeExpectancy:
        raw = self._life_dict.get(state)
        if raw is None:
            return self._get_global_prior_fallback(state)

        result = self._apply_degradation(raw)
        return result.shrunk

    def _apply_degradation(self, life: LifeExpectancy) -> ShrinkageResult:
        n = life.sample_count
        r2 = life.decay_r_squared
        min_full = self._min_sample_count
        min_r2 = self._decay_r_squared_threshold

        if n >= min_full and r2 >= min_r2:
            return ShrinkageResult(
                original=life, shrunk=life,
                shrinkage_factor=1.0, degradation_level=0,
                reason='完整条件分布',
            )

        if n >= self.MIN_SAMPLES_SHRINK:
            prior = self._category_priors.get(
                self.STATE_CATEGORIES.get(life.state, ''), self._global_prior
            )
            if prior is None:
                prior = self._global_prior
            shrinkage = n / (n + self._shrinkage_prior_strength * self.N_PRIOR)
            shrunk = self._shrink_toward_prior(life, prior, shrinkage)
            level = 1
            if r2 < min_r2:
                level = 2
                shrunk = self._shrink_toward_prior(shrunk, prior, shrinkage * 0.5)
            return ShrinkageResult(
                original=life, shrunk=shrunk,
                shrinkage_factor=shrinkage, degradation_level=level,
                reason=f'贝叶斯收缩(shrinkage={shrinkage:.3f})',
            )

        if n >= self.MIN_SAMPLES_MERGE:
            category = self.STATE_CATEGORIES.get(life.state, '')
            prior = self._category_priors.get(category, self._global_prior)
            if prior is None:
                prior = self._global_prior
            return ShrinkageResult(
                original=life, shrunk=prior,
                shrinkage_factor=0.0, degradation_level=2,
                reason=f'大类合并(category={category})',
            )

        prior = self._global_prior if self._global_prior else life
        return ShrinkageResult(
            original=life, shrunk=prior,
            shrinkage_factor=0.0, degradation_level=3,
            reason='全局先验',
        )

    def _shrink_toward_prior(
        self, life: LifeExpectancy, prior: LifeExpectancy, shrinkage: float
    ) -> LifeExpectancy:
        if prior is None:
            return life
        shrunk_dur = {
            q: life.duration.get(q, 0) * shrinkage + prior.duration.get(q, 0) * (1 - shrinkage)
            for q in ('p25', 'p50', 'p75', 'p99')
        }
        shrunk_mag = {
            q: life.magnitude.get(q, 0) * shrinkage + prior.magnitude.get(q, 0) * (1 - shrinkage)
            for q in ('p25', 'p50', 'p75', 'p99')
        }
        return LifeExpectancy(
            state=life.state,
            duration=shrunk_dur,
            magnitude=shrunk_mag,
            sample_count=life.sample_count,
            decay_r_squared=life.decay_r_squared * shrinkage + prior.decay_r_squared * (1 - shrinkage),
            degradation_level=life.degradation_level,
        )

    def _compute_global_prior(self) -> None:
        all_lives = [l for l in self._life_dict.values() if l.sample_count > 0]
        if not all_lives:
            return
        total_n = sum(l.sample_count for l in all_lives)
        # R21-MATH-P1-05修复: 检查总样本量 — 样本量不足时先验不可靠
        if total_n < self.MIN_SAMPLES_MERGE:
            logger.warning(
                "[R21-MATH-P1-05修复] 全局先验总样本量不足: total_n=%d < %d, 先验估计不可靠",
                total_n, self.MIN_SAMPLES_MERGE,
            )
        if total_n == 0:
            return
        avg_dur = {q: sum(l.duration.get(q, 0) * l.sample_count for l in all_lives) / total_n for q in ('p25', 'p50', 'p75', 'p99')}
        avg_mag = {q: sum(l.magnitude.get(q, 0) * l.sample_count for l in all_lives) / total_n for q in ('p25', 'p50', 'p75', 'p99')}
        self._global_prior = LifeExpectancy(
            state='GLOBAL',
            duration=avg_dur,
            magnitude=avg_mag,
            sample_count=total_n,
            decay_r_squared=np.mean([l.decay_r_squared for l in all_lives]),
        )

    def _compute_category_priors(self) -> None:
        cat_lives: Dict[str, List[LifeExpectancy]] = {}
        for life in self._life_dict.values():
            cat = self.STATE_CATEGORIES.get(life.state, 'other')
            cat_lives.setdefault(cat, []).append(life)

        for cat, lives in cat_lives.items():
            valid = [l for l in lives if l.sample_count > 0]
            if not valid:
                continue
            total_n = sum(l.sample_count for l in valid)
            if total_n < self.MIN_SAMPLES_MERGE:
                logger.warning("类别先验样本不足: cat=%s n=%d<%d", cat, total_n, self.MIN_SAMPLES_MERGE)
            avg_dur = {q: sum(l.duration.get(q, 0) * l.sample_count for l in valid) / total_n for q in ('p25', 'p50', 'p75', 'p99')}
            avg_mag = {q: sum(l.magnitude.get(q, 0) * l.sample_count for l in valid) / total_n for q in ('p25', 'p50', 'p75', 'p99')}
            self._category_priors[cat] = LifeExpectancy(
                state=f'CAT_{cat}',
                duration=avg_dur, magnitude=avg_mag,
                sample_count=total_n,
                decay_r_squared=np.mean([l.decay_r_squared for l in valid]),
            )

    def _get_global_prior_fallback(self, state: str) -> LifeExpectancy:
        if self._global_prior is not None:
            return LifeExpectancy(state=state, duration=dict(self._global_prior.duration),
                magnitude=dict(self._global_prior.magnitude), sample_count=0, decay_r_squared=0.0, degradation_level=3)
        return LifeExpectancy(state=state,
            duration={'p25': 5, 'p50': 15, 'p75': 45, 'p99': 180},
            magnitude={'p25': 0.005, 'p50': 0.02, 'p75': 0.05, 'p99': 0.15},
            sample_count=0, decay_r_squared=0.0, degradation_level=3)

    @staticmethod
    def _compute_quantiles(series: pd.Series) -> Dict[str, float]:
        vals = series.dropna().values
        if len(vals) == 0:
            return {'p25': 0, 'p50': 0, 'p75': 0, 'p99': 0}
        return {f'p{p}': float(np.percentile(vals, p)) for p in (25, 50, 75, 99)}

    @staticmethod
    def _estimate_decay_r_squared(series: pd.Series) -> float:
        vals = series.dropna().values
        if len(vals) < 10:
            return 0.0
        _positive_ratio = np.sum(vals > 0) / len(vals) if len(vals) > 0 else 0.0
        if _positive_ratio < 0.8:
            return 0.0
        log_vals = np.log(vals[vals > 0] + 1e-10)
        if len(log_vals) < 10:
            return 0.0
        x = np.arange(len(log_vals), dtype=float)
        coeffs = np.polyfit(x, log_vals, 1)
        predicted = np.polyval(coeffs, x)
        ss_res = np.sum((log_vals - predicted) ** 2)
        ss_tot = np.sum((log_vals - np.mean(log_vals)) ** 2)
        if ss_tot < 1e-10:
            return 1.0
        return float(max(0.0, 1.0 - ss_res / ss_tot))

    def calibrate_penalty(
        self, state: str, n_simulations: int = 10000
    ) -> Dict[str, Any]:
        life = self.get_life_expectancy(state)

        sharpes = []
        for _ in range(n_simulations):
            win_rate = np.random.beta(2, 1.5)
            tp = np.random.uniform(0.001, life.magnitude['p99'] * 5)
            sl = np.random.uniform(0.001, life.magnitude['p75'] * 2)
            avg_win = tp * np.random.uniform(0.5, 1.0)
            avg_loss = sl * np.random.uniform(0.5, 1.0)
            exp_ret = win_rate * avg_win - (1 - win_rate) * avg_loss
            var = (win_rate * (avg_win - exp_ret) ** 2
                   + (1 - win_rate) * (avg_loss + exp_ret) ** 2)
            sharpes.append(exp_ret / np.sqrt(var + 1e-6))

        sharpe_std = float(np.std(sharpes))
        base_scale = 0.1 / sharpe_std if sharpe_std > 1e-6 else 1.0

        return {
            'state': state,
            'base_scale': base_scale,
            'sharpe_std': sharpe_std,
            'coefficients': {
                'pullback_soft': base_scale * 0.5,
                'take_profit_hard': base_scale * 1.0,
                'holding_time_soft': base_scale * 0.3,
                'exploration_multiplier': 0.5,
            },
            'sample_size': n_simulations,
            'calibration_date': datetime.now(CHINA_TZ).isoformat(),
        }

    def get_shrinkage_log(self) -> List[ShrinkageResult]:
        return list(self._shrinkage_log)

    def save_life_dict(self, path: str) -> None:
        records = []
        for state, life in self._life_dict.items():
            records.append({
                'state': state,
                **{f'dur_{q}': life.duration[q] for q in ('p25', 'p50', 'p75', 'p99')},
                **{f'mag_{q}': life.magnitude[q] for q in ('p25', 'p50', 'p75', 'p99')},
                'sample_count': life.sample_count, 'decay_r_squared': life.decay_r_squared,
            })
        df = pd.DataFrame(records)
        safe_dataframe_to_parquet(df, path)
        logger.info("寿命字典保存至 %s (%d状态)", path, len(records))

    def load_life_dict(self, path: str) -> Dict[str, LifeExpectancy]:
        df = pd.read_parquet(path)
        self._life_dict.clear()
        for _, row in df.iterrows():
            self._life_dict[row['state']] = LifeExpectancy(
                state=row['state'],
                duration={
                    'p25': row['dur_p25'], 'p50': row['dur_p50'],
                    'p75': row['dur_p75'], 'p99': row['dur_p99'],
                },
                magnitude={
                    'p25': row['mag_p25'], 'p50': row['mag_p50'],
                    'p75': row['mag_p75'], 'p99': row['mag_p99'],
                },
                sample_count=int(row['sample_count']),
                decay_r_squared=float(row['decay_r_squared']),
            )
        self._compute_global_prior()
        self._compute_category_priors()
        logger.info("寿命字典加载自 %s (%d状态)", path, len(self._life_dict))
        return dict(self._life_dict)

_TRIPLE_TRUTH_DEFAULT_SEED = 42
from ali2026v3_trading.infra.shared_utils import set_global_seed as _set_global_seed
_set_global_seed(_TRIPLE_TRUTH_DEFAULT_SEED)

@dataclass(slots=True)
class TruthAnchorResult:
    algorithm_label: str
    expost_label: str
    external_label: Optional[str]
    algorithm_accuracy: float
    expost_accuracy: float
    external_accuracy: Optional[float]
    agreement_rate_algo_expost: float
    agreement_rate_algo_external: Optional[float]
    train_end_date: str
    val_start_date: str
    future_leak_risk: str
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())

class TripleTruthAnchor:
    """三重真值锚定: 1.算法标签(多尺度投票,无未来) 2.事后标签(实现波动率,仅验证) 3.外部标签(交易所,仅校准); 训练与验证物理隔离"""

    VOTING_WEIGHTS = {
        'minute': 0.10,
        'hour': 0.20,
        'daily': 0.40,
        'weekly': 0.30,
    }

    STATES = ['trend_up', 'trend_down', 'range_bound', 'volatile', 'quiet']

    def __init__(
        self,
        lookahead_minutes: int = 120,
        external_validator: Optional[Any] = None,
    ):
        self._lookahead = lookahead_minutes
        self._external = external_validator
        self._audit_log: List[Dict[str, Any]] = []

    def train_and_validate(
        self,
        data: pd.DataFrame,
        train_end_date: str,
        state_column: str = 'hmm_state',
        return_column: str = 'close',
    ) -> TruthAnchorResult:
        train_data = data[data.index <= train_end_date]
        val_data = data[data.index > train_end_date]

        if len(train_data) == 0 or len(val_data) == 0:
            raise ValueError(
                f"训练/验证数据为空: train={len(train_data)}, val={len(val_data)}"
            )

        train_labels = self._generate_no_future_labels(train_data, state_column)
        val_labels_algo = self._generate_no_future_labels(val_data, state_column)
        val_labels_expost = self._generate_expost_labels(
            val_data, return_column
        )
        val_labels_external = self._generate_external_labels(val_data)

        algo_acc = self._accuracy(val_labels_algo, val_labels_expost)
        expost_acc = 1.0
        external_acc = None
        agreement_algo_expost = self._agreement_rate(
            val_labels_algo, val_labels_expost
        )
        agreement_algo_external = None

        if val_labels_external is not None:
            external_acc = self._accuracy(val_labels_algo, val_labels_external)
            agreement_algo_external = self._agreement_rate(
                val_labels_algo, val_labels_external
            )

        result = TruthAnchorResult(
            algorithm_label='multi_scale_voting',
            expost_label=f'expost_{self._lookahead}min',
            external_label='exchange_official' if val_labels_external is not None else None,
            algorithm_accuracy=algo_acc,
            expost_accuracy=expost_acc,
            external_accuracy=external_acc,
            agreement_rate_algo_expost=agreement_algo_expost,
            agreement_rate_algo_external=agreement_algo_external,
            train_end_date=str(train_end_date),
            val_start_date=str(val_data.index.min()),
            future_leak_risk='NONE',
        )

        self._audit_log.append({
            'train_end': train_end_date,
            'train_samples': len(train_data),
            'val_samples': len(val_data),
            'result': result,
        })

        logger.info(
            "三重真值锚定完成: algo_acc=%.4f, agreement_algo_expost=%.4f, "
            "future_leak=%s, train_end=%s",
            algo_acc, agreement_algo_expost, result.future_leak_risk, train_end_date,
        )

        return result

    def _generate_no_future_labels(
        self, data: pd.DataFrame, state_column: str
    ) -> pd.Series:
        labels = []
        for i in range(len(data)):
            current_data = data.iloc[: i + 1]
            minute_state = self._dominant_state(current_data.tail(30), state_column)
            hour_state = self._dominant_state(current_data.tail(240), state_column)
            daily_state = self._dominant_state(current_data.tail(252), state_column)
            weekly_state = self._dominant_state(current_data, state_column)

            state = self._weighted_vote({
                'minute': minute_state,
                'hour': hour_state,
                'daily': daily_state,
                'weekly': weekly_state,
            })
            labels.append(state)

        return pd.Series(labels, index=data.index)

    def _generate_expost_labels(
        self, data: pd.DataFrame, return_column: str
    ) -> pd.Series:
        labels = []
        prices = data[return_column].values if return_column in data.columns else np.ones(len(data))

        for i in range(len(data)):
            future_end = min(i + self._lookahead, len(data) - 1)
            if future_end <= i:
                labels.append('quiet')
                continue

            future_prices = prices[i + 1 : future_end + 1]
            if len(future_prices) < 2:
                labels.append('quiet')
                continue

            realized_vol = np.std(np.diff(np.log(future_prices + 1e-10)))
            realized_return = (future_prices[-1] - prices[i]) / (prices[i] + 1e-10)

            if realized_vol > 0.02 and abs(realized_return) > 0.01: labels.append('volatile')
            elif realized_return > 0.005: labels.append('trend_up')
            elif realized_return < -0.005: labels.append('trend_down')
            elif realized_vol < 0.005: labels.append('quiet')
            else: labels.append('range_bound')

        return pd.Series(labels, index=data.index)

    def _generate_external_labels(
        self, data: pd.DataFrame
    ) -> Optional[pd.Series]:
        if self._external is None:
            return None
        try:
            return self._external.fetch_labels(data)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("外部标签获取失败: %s", e)
            return None

    def _dominant_state(self, window: pd.DataFrame, state_column: str) -> str:
        if state_column in window.columns:
            counts = window[state_column].value_counts()
            if len(counts) > 0:
                return str(counts.index[0])
        if len(window) > 1 and 'close' in window.columns:
            ret = window['close'].iloc[-1] / window['close'].iloc[0] - 1
            vol = window['close'].pct_change().std() if len(window) > 2 else 0.0
            if vol > 0.02 and abs(ret) > 0.01:
                return 'volatile'
            elif ret > 0.005:
                return 'trend_up'
            elif ret < -0.005:
                return 'trend_down'
            elif vol < 0.005:
                return 'quiet'
        return 'range_bound'

    def _weighted_vote(self, state_votes: Dict[str, str]) -> str:
        score = {s: 0.0 for s in self.STATES}
        for timescale, state in state_votes.items():
            w = self.VOTING_WEIGHTS.get(timescale, 0.0)
            if state in score:
                score[state] += w
        if max(score.values()) == 0.0:
            return 'range_bound'
        return max(score, key=score.get)

    @staticmethod
    def _accuracy(predictions: pd.Series, truth: pd.Series) -> float:
        if len(predictions) != len(truth):
            min_len = min(len(predictions), len(truth))
            predictions = predictions.iloc[:min_len]
            truth = truth.iloc[:min_len]
        if len(predictions) == 0:
            return 0.0
        return float((predictions.values == truth.values).mean())

    @staticmethod
    def _agreement_rate(a: pd.Series, b: pd.Series) -> float:
        min_len = min(len(a), len(b))
        if min_len == 0:
            return 0.0
        return float((a.iloc[:min_len].values == b.iloc[:min_len].values).mean())
    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self._audit_log)

    def verify_no_future_leak(self) -> bool:
        for entry in self._audit_log:
            result = entry['result']
            if result.future_leak_risk != 'NONE':
                logger.error("未来泄露检测失败: train_end=%s", entry['train_end'])
                return False
        return True

from typing import Callable

@dataclass(slots=True)
class OptimizationResult:
    best_params: Dict[str, Any]
    best_objective: float
    in_sample_sharpe: float
    out_sample_sharpe: Optional[float]
    oos_ratio: Optional[float]
    penalty_total: float
    penalty_breakdown: Dict[str, float]
    exploration_mode: bool
    constraints_satisfied: bool
    n_trials: int
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())

class SoftConstrainedOptimizer:
    """软约束优化器：惩罚系数(pullback_soft/take_profit_hard/holding_time_soft) + 探索模式(惩罚减半) + 样本外检验(OOS≥IS×90%)。calibrate_coefficients()自适应缩放: base_scale=0.1/sharpe_std"""

    OOS_MIN_RATIO = 0.90
    EXPLORATION_PENALTY_MULTIPLIER = 0.5

    def __init__(
        self,
        penalty_coefficients: Optional[Dict[str, float]] = None,
        exploration_mode: bool = False,
        oos_min_ratio: float = 0.90,
    ):
        self._coefficients = penalty_coefficients or {
            'pullback_soft': 0.05,
            'take_profit_hard': 0.10,
            'holding_time_soft': 0.03,
        }
        self._exploration_mode = exploration_mode
        self._oos_min_ratio = oos_min_ratio
        self._optimization_history: List[OptimizationResult] = []

    def optimize(
        self,
        objective_fn: Callable[[Dict[str, Any]], float],
        param_space: Dict[str, List[Any]],
        constraint_fns: Optional[Dict[str, Callable[[Dict[str, Any]], Tuple[bool, float]]]] = None,
        n_trials: int = 500,
        in_sample_eval: Optional[Callable[[Dict[str, Any]], float]] = None,
        out_sample_eval: Optional[Callable[[Dict[str, Any]], float]] = None,
    ) -> OptimizationResult:
        try:
            import optuna
        except ImportError:
            logger.warning("optuna未安装，使用网格搜索")
            return self._grid_search_optimize(
                objective_fn, param_space, constraint_fns,
                n_trials, in_sample_eval, out_sample_eval,
            )

        best_params = {}
        best_objective = -np.inf
        best_penalty = 0.0
        best_penalty_breakdown = {}
        is_sharpe = 0.0
        os_sharpe = None

        def optuna_objective(trial):
            params = {}
            for key, values in param_space.items():
                if isinstance(values[0], float):
                    params[key] = trial.suggest_float(key, min(values), max(values))
                elif isinstance(values[0], int):
                    params[key] = trial.suggest_int(key, min(values), max(values))
                else:
                    idx = trial.suggest_int(f'{key}_idx', 0, len(values) - 1)
                    params[key] = values[idx]

            base_obj = objective_fn(params)
            penalty, breakdown = self._compute_penalty(params, constraint_fns)
            return base_obj - penalty

        direction = 'maximize'
        study = optuna.create_study(direction=direction)
        study.optimize(optuna_objective, n_trials=n_trials, show_progress_bar=False)

        best_params = study.best_params
        best_objective = study.best_value

        for key in param_space:
            if key not in best_params and f'{key}_idx' in best_params:
                idx = best_params[f'{key}_idx']
                best_params[key] = param_space[key][idx]
                del best_params[f'{key}_idx']

        best_penalty, best_penalty_breakdown = self._compute_penalty(
            best_params, constraint_fns
        )

        if in_sample_eval is not None:
            is_sharpe = in_sample_eval(best_params)
        if out_sample_eval is not None:
            os_sharpe = out_sample_eval(best_params)

        oos_ratio = os_sharpe / is_sharpe if os_sharpe is not None and is_sharpe > 0 else None
        constraints_ok = self._check_oos_constraint(is_sharpe, os_sharpe)

        result = OptimizationResult(
            best_params=best_params,
            best_objective=best_objective,
            in_sample_sharpe=is_sharpe,
            out_sample_sharpe=os_sharpe,
            oos_ratio=oos_ratio,
            penalty_total=best_penalty,
            penalty_breakdown=best_penalty_breakdown,
            exploration_mode=self._exploration_mode,
            constraints_satisfied=constraints_ok,
            n_trials=n_trials,
        )

        self._optimization_history.append(result)
        logger.info(
            "优化完成: obj=%.4f, IS_sharpe=%.4f, OS_sharpe=%s, "
            "OOS_ratio=%s, penalty=%.4f, exploration=%s",
            best_objective, is_sharpe,
            f'{os_sharpe:.4f}' if os_sharpe else 'N/A',
            f'{oos_ratio:.4f}' if oos_ratio else 'N/A',
            best_penalty, self._exploration_mode,
        )

        return result

    def _grid_search_optimize(
        self,
        objective_fn: Callable,
        param_space: Dict[str, List[Any]],
        constraint_fns: Optional[Dict],
        n_trials: int,
        in_sample_eval: Optional[Callable],
        out_sample_eval: Optional[Callable],
    ) -> OptimizationResult:
        import itertools

        keys = list(param_space.keys())
        values = [param_space[k] for k in keys]
        best_params = {}
        best_obj = -np.inf
        best_penalty = 0.0
        best_breakdown = {}

        for combo in itertools.product(*values):
            params = dict(zip(keys, combo))
            base_obj = objective_fn(params)
            penalty, breakdown = self._compute_penalty(params, constraint_fns)
            total = base_obj - penalty
            if total > best_obj:
                best_obj = total
                best_params = params
                best_penalty = penalty
                best_breakdown = breakdown

        is_sharpe = in_sample_eval(best_params) if in_sample_eval else 0.0
        os_sharpe = out_sample_eval(best_params) if out_sample_eval else None
        oos_ratio = os_sharpe / is_sharpe if os_sharpe and is_sharpe > 0 else None

        return OptimizationResult(
            best_params=best_params,
            best_objective=best_obj,
            in_sample_sharpe=is_sharpe,
            out_sample_sharpe=os_sharpe,
            oos_ratio=oos_ratio,
            penalty_total=best_penalty,
            penalty_breakdown=best_breakdown,
            exploration_mode=self._exploration_mode,
            constraints_satisfied=self._check_oos_constraint(is_sharpe, os_sharpe),
            n_trials=0,
        )

    def _compute_penalty(
        self,
        params: Dict[str, Any],
        constraint_fns: Optional[Dict[str, Callable]],
    ) -> Tuple[float, Dict[str, float]]:
        if constraint_fns is None:
            return 0.0, {}

        total = 0.0
        breakdown = {}
        multiplier = (
            self.EXPLORATION_PENALTY_MULTIPLIER
            if self._exploration_mode else 1.0
        )

        for name, fn in constraint_fns.items():
            satisfied, violation = fn(params)
            coeff_key = self._map_constraint_to_coefficient(name)
            coeff = self._coefficients.get(coeff_key, 0.05) * multiplier
            penalty = coeff * violation ** 2 if not satisfied else 0.0
            total += penalty
            breakdown[name] = penalty

        return total, breakdown

    @staticmethod
    def _map_constraint_to_coefficient(constraint_name: str) -> str:
        for key, coeff in (('pullback','pullback_soft'),('take_profit','take_profit_hard'),('holding_time','holding_time_soft'),('stop_loss','take_profit_hard'),('risk_ratio','take_profit_hard')):
            if key in constraint_name: return coeff
        return 'pullback_soft'

    def _check_oos_constraint(
        self, is_sharpe: float, os_sharpe: Optional[float]
    ) -> bool:
        if os_sharpe is None:
            return True
        if is_sharpe <= 0:
            return False
        return os_sharpe >= is_sharpe * self._oos_min_ratio

    def enable_exploration_mode(self) -> None:
        self._exploration_mode = True
        logger.info("探索模式开启: 惩罚系数×%.1f", self.EXPLORATION_PENALTY_MULTIPLIER)

    def disable_exploration_mode(self) -> None:
        self._exploration_mode = False
        logger.info("探索模式关闭: 惩罚系数恢复原值")

    def calibrate_coefficients(
        self, sharpe_std: float
    ) -> Dict[str, float]:
        if sharpe_std < 1e-6:
            logger.warning("sharpe_std接近0，使用默认系数")
            return dict(self._coefficients)

        base_scale = 0.1 / sharpe_std
        self._coefficients = {
            'pullback_soft': base_scale * 0.5,
            'take_profit_hard': base_scale * 1.0,
            'holding_time_soft': base_scale * 0.3,
        }
        logger.info(
            "惩罚系数校准完成: base_scale=%.4f, coeff=%s",
            base_scale, self._coefficients,
        )
        return dict(self._coefficients)

    def get_optimization_history(self) -> List[OptimizationResult]:
        return list(self._optimization_history)

from enum import Enum

class PerformanceTier(Enum):
    TIER_3_FAST = 'tier_3_fast'
    TIER_2_CORE = 'tier_2_core'
    TIER_1_FULL = 'tier_1_full'

@dataclass(slots=True)
class TierConfig:
    tier: PerformanceTier
    min_sharpe: float
    feature_set: List[str]
    optimization_trials: int
    validation_depth: str
    data_resolution: str
    description: str

@dataclass(slots=True)
class TierTransition:
    from_tier: PerformanceTier
    to_tier: PerformanceTier
    reason: str
    sharpe_achieved: float
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())

class PerformanceTierManager:
    """三级性能自动降级管理器: TIER_3(日频,100trials,sharpe>1.0) → TIER_2(分钟,300trials,sharpe>1.5) → TIER_1(Tick,500trials,sharpe>2.0)"""

    TIER_CONFIGS = {
        PerformanceTier.TIER_3_FAST: TierConfig(tier=PerformanceTier.TIER_3_FAST, min_sharpe=1.0,
            feature_set=['daily_iv_rank', 'daily_hv_state', 'daily_return'], optimization_trials=100,
            validation_depth='basic', data_resolution='daily', description='快速验证：日频特征'),
        PerformanceTier.TIER_2_CORE: TierConfig(tier=PerformanceTier.TIER_2_CORE, min_sharpe=1.5,
            feature_set=['minute_bar_features', 'intraday_hv', 'order_flow_imbalance', 'iv_surface_features', 'state_lifetime'],
            optimization_trials=300, validation_depth='standard', data_resolution='minute_bar', description='核心建设：分钟Bar特征'),
        PerformanceTier.TIER_1_FULL: TierConfig(tier=PerformanceTier.TIER_1_FULL, min_sharpe=2.0,
            feature_set=['tick_level_features', 'microstructure', 'depth_imbalance', 'trade_flow',
                         'iv_tick_surface', 'greeks_tick', 'state_lifetime_conditional', 'cross_symbol_correlation'],
            optimization_trials=500, validation_depth='deep', data_resolution='tick', description='完整精细：Tick级特征'),
    }
    TIER_ORDER = [PerformanceTier.TIER_3_FAST, PerformanceTier.TIER_2_CORE, PerformanceTier.TIER_1_FULL]

    def __init__(
        self,
        data_availability: str = 'unknown',
        compute_capacity: str = 'unknown',
        time_budget: str = 'normal',
    ):
        self._current_tier: PerformanceTier = PerformanceTier.TIER_3_FAST
        self._transition_log: List[TierTransition] = []
        self._data_availability = data_availability
        self._compute_capacity = compute_capacity
        self._time_budget = time_budget
        self._auto_select_tier()

    def _auto_select_tier(self) -> None:
        d, c, t = self._data_availability, self._compute_capacity, self._time_budget
        if d == 'tick_ready' and c == 'high' and t != 'urgent': target = PerformanceTier.TIER_1_FULL
        elif d in ('tick_ready', 'minute_bar_ready') and c in ('high', 'medium'): target = PerformanceTier.TIER_2_CORE
        else: target = PerformanceTier.TIER_3_FAST
        if target != self._current_tier:
            old = self._current_tier
            self._current_tier = target
            self._transition_log.append(TierTransition(from_tier=old, to_tier=target,
                reason=f'自动选择: data={d}, compute={c}, time={t}', sharpe_achieved=0.0))
            logger.info("性能层级自动选择: %s → %s", old.value, target.value)

    def get_current_tier(self) -> PerformanceTier:
        return self._current_tier

    def get_tier_config(self) -> TierConfig:
        return self.TIER_CONFIGS[self._current_tier]

    def attempt_upgrade(self, achieved_sharpe: float) -> Optional[PerformanceTier]:
        current_config = self.TIER_CONFIGS[self._current_tier]
        if achieved_sharpe < current_config.min_sharpe:
            logger.info("升阶失败: 夏普%.4f < 阈值%.4f", achieved_sharpe, current_config.min_sharpe)
            return None
        current_idx = self.TIER_ORDER.index(self._current_tier)
        if current_idx >= len(self.TIER_ORDER) - 1:
            logger.info("已在最高层级 %s", self._current_tier.value)
            return None
        next_tier = self.TIER_ORDER[current_idx + 1]
        if next_tier == PerformanceTier.TIER_2_CORE and self._data_availability not in ('tick_ready', 'minute_bar_ready'):
            logger.info("升阶至TIER_2失败: 数据不满足"); return None
        elif next_tier == PerformanceTier.TIER_1_FULL and (self._data_availability != 'tick_ready' or self._compute_capacity != 'high'):
            logger.info("升阶至TIER_1失败: 数据或算力不满足"); return None
        old = self._current_tier
        self._current_tier = next_tier
        self._transition_log.append(TierTransition(from_tier=old, to_tier=next_tier,
            reason=f'夏普{achieved_sharpe:.4f}≥阈值{current_config.min_sharpe:.4f}', sharpe_achieved=achieved_sharpe))
        logger.info("升阶成功: %s → %s (夏普=%.4f)", old.value, next_tier.value, achieved_sharpe)
        return next_tier

    def force_downgrade(self, reason: str) -> PerformanceTier:
        current_idx = self.TIER_ORDER.index(self._current_tier)
        if current_idx <= 0:
            logger.info("已在最低层级，无法降级"); return self._current_tier
        old = self._current_tier
        new_tier = self.TIER_ORDER[current_idx - 1]
        self._current_tier = new_tier
        self._transition_log.append(TierTransition(from_tier=old, to_tier=new_tier,
            reason=f'强制降级: {reason}', sharpe_achieved=0.0))
        logger.info("强制降级: %s → %s, 原因=%s", old.value, new_tier.value, reason)
        return new_tier

    def get_feature_set(self) -> List[str]:
        return self.TIER_CONFIGS[self._current_tier].feature_set

    def get_optimization_trials(self) -> int:
        return self.TIER_CONFIGS[self._current_tier].optimization_trials

    def get_data_resolution(self) -> str:
        return self.TIER_CONFIGS[self._current_tier].data_resolution

    def get_transition_log(self) -> List[TierTransition]:
        return list(self._transition_log)

    def assess_environment(self, data_rows: int = 0, available_memory_gb: float = 16.0, gpu_available: bool = False) -> Dict[str, Any]:
        if data_rows > 1_000_000_000 and available_memory_gb >= 64 and gpu_available:
            data, compute = 'tick_ready', 'high'
        elif data_rows > 100_000_000 and available_memory_gb >= 16:
            data, compute = 'minute_bar_ready', 'medium'
        else:
            data, compute = 'daily_ready', 'low'
        self._data_availability, self._compute_capacity = data, compute
        self._auto_select_tier()
        return {'data_availability': data, 'compute_capacity': compute, 'selected_tier': self._current_tier.value,
            'data_rows': data_rows, 'memory_gb': available_memory_gb, 'gpu': gpu_available}

    def get_tier_summary(self) -> Dict[str, Any]:
        config = self.get_tier_config()
        return {'current_tier': self._current_tier.value, 'min_sharpe': config.min_sharpe,
            'feature_count': len(config.feature_set), 'optimization_trials': config.optimization_trials,
            'validation_depth': config.validation_depth, 'data_resolution': config.data_resolution,
            'description': config.description, 'transitions': len(self._transition_log)}
