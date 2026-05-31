import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from ali2026v3_trading.shared_utils import CHINA_TZ

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


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
    """
    四级降级阶梯的贝叶斯收缩寿命估计器

    降级阶梯：
    Level 0 — 完整条件分布（样本>=200, R²>=0.7）
    Level 1 — 贝叶斯收缩向大类先验（50<=样本<200）
    Level 2 — 大类合并（10<=样本<50）
    Level 3 — 全局先验（样本<10）

    附加规则：
    - 衰减曲线R²<0.7 → 强制降级1级
    - 小样本自动向全局先验收缩
    - 收缩因子 = n / (n + N_prior)，N_prior=50
    """

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
                life = LifeExpectancy(
                    state=state,
                    duration={'p25': 0, 'p50': 0, 'p75': 0, 'p99': 0},
                    magnitude={'p25': 0, 'p50': 0, 'p75': 0, 'p99': 0},
                    sample_count=n,
                    decay_r_squared=0.0,
                )
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
            for q in ['p25', 'p50', 'p75', 'p99']
        }
        shrunk_mag = {
            q: life.magnitude.get(q, 0) * shrinkage + prior.magnitude.get(q, 0) * (1 - shrinkage)
            for q in ['p25', 'p50', 'p75', 'p99']
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
        avg_dur = {
            q: sum(l.duration.get(q, 0) * l.sample_count for l in all_lives) / total_n
            for q in ['p25', 'p50', 'p75', 'p99']
        }
        avg_mag = {
            q: sum(l.magnitude.get(q, 0) * l.sample_count for l in all_lives) / total_n
            for q in ['p25', 'p50', 'p75', 'p99']
        }
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
            # R21-MATH-P1-05修复: 检查类别总样本量 — 样本量不足时类别先验不可靠
            if total_n < self.MIN_SAMPLES_MERGE:
                logger.warning(
                    "[R21-MATH-P1-05修复] 类别先验样本量不足: category=%s total_n=%d < %d",
                    cat, total_n, self.MIN_SAMPLES_MERGE,
                )
            avg_dur = {
                q: sum(l.duration.get(q, 0) * l.sample_count for l in valid) / total_n
                for q in ['p25', 'p50', 'p75', 'p99']
            }
            avg_mag = {
                q: sum(l.magnitude.get(q, 0) * l.sample_count for l in valid) / total_n
                for q in ['p25', 'p50', 'p75', 'p99']
            }
            self._category_priors[cat] = LifeExpectancy(
                state=f'CAT_{cat}',
                duration=avg_dur, magnitude=avg_mag,
                sample_count=total_n,
                decay_r_squared=np.mean([l.decay_r_squared for l in valid]),
            )

    def _get_global_prior_fallback(self, state: str) -> LifeExpectancy:
        if self._global_prior is not None:
            return LifeExpectancy(
                state=state,
                duration=dict(self._global_prior.duration),
                magnitude=dict(self._global_prior.magnitude),
                sample_count=0,
                decay_r_squared=0.0,
                degradation_level=3,
            )
        return LifeExpectancy(
            state=state,
            duration={'p25': 5, 'p50': 15, 'p75': 45, 'p99': 180},
            magnitude={'p25': 0.005, 'p50': 0.02, 'p75': 0.05, 'p99': 0.15},
            sample_count=0,
            decay_r_squared=0.0,
            degradation_level=3,
        )

    @staticmethod
    def _compute_quantiles(series: pd.Series) -> Dict[str, float]:
        vals = series.dropna().values
        if len(vals) == 0:
            return {'p25': 0, 'p50': 0, 'p75': 0, 'p99': 0}
        return {
            'p25': float(np.percentile(vals, 25)),
            'p50': float(np.percentile(vals, 50)),
            'p75': float(np.percentile(vals, 75)),
            'p99': float(np.percentile(vals, 99)),
        }

    @staticmethod
    def _estimate_decay_r_squared(series: pd.Series) -> float:
        vals = series.dropna().values
        if len(vals) < 10:
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
                **{f'dur_{q}': life.duration[q] for q in ['p25', 'p50', 'p75', 'p99']},
                **{f'mag_{q}': life.magnitude[q] for q in ['p25', 'p50', 'p75', 'p99']},
                'sample_count': life.sample_count,
                'decay_r_squared': life.decay_r_squared,
            })
        df = pd.DataFrame(records)
        df.to_parquet(path, index=False)
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
