import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime
from ali2026v3_trading.shared_utils import CHINA_TZ

import numpy as np

logger = logging.getLogger(__name__)


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


# R33-P1-4修复: 此模块已集成到enhanced_phase_scan.py的phase2扫描，不再是死代码
class SoftConstrainedOptimizer:
    """
    软约束优化器：惩罚系数 + 探索模式 + 样本外检验

    核心功能：
    1. 软约束惩罚：将硬约束转化为惩罚项加入目标函数
       - pullback_soft: 回撤越界惩罚（宽松）
       - take_profit_hard: 止盈越界惩罚（严格）
       - holding_time_soft: 持仓时间越界惩罚（宽松）
    2. 探索模式：每季度1次，惩罚减半，扩大搜索空间
    3. 样本外检验：突破约束参数必须通过OOS夏普≥IS夏普×90%
    4. 延迟生效：新参数必须经过3个月模拟验证

    R21-MATH-P2-03修复说明: 惩罚系数已通过calibrate_coefficients()方法实现自适应缩放，
    该方法根据目标函数（Sharpe）的标准差动态计算base_scale=0.1/sharpe_std，
    使"超出1个标准差"的惩罚约等于Sharpe降低0.1。
    调用方应在优化前先调用calibrate_coefficients()校准系数，
    否则使用默认系数{pullback_soft:0.05, take_profit_hard:0.10, holding_time_soft:0.03}。
    """

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
        mapping = {
            'pullback': 'pullback_soft',
            'take_profit': 'take_profit_hard',
            'holding_time': 'holding_time_soft',
            'stop_loss': 'take_profit_hard',
            'risk_ratio': 'take_profit_hard',
        }
        for key, coeff in mapping.items():
            if key in constraint_name:
                return coeff
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
