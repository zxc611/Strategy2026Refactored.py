"""Statistical Validity Extensions — R19-P2 统计有效性7项修复

P2-统计1: 市场状态结构性变化(regime change)检测
P2-统计2: 回测期间幸存者偏差检测
P2-统计3: 策略复杂度惩罚(AIC/BIC)
P2-统计4: 交易次数与统计可靠性关系
P2-统计5: 极端事件(黑天鹅)影响分析
P2-统计6: 回测结果对佣金/滑点参数的敏感性分析
P2-统计7: 不同市场状态下策略表现稳定性检验
"""
from __future__ import annotations

import logging
import math
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RegimeChangeResult:
    """市场状态结构性变化检测结果 [P2-统计1]"""
    n_regimes: int
    change_points: List[int]
    regime_sharpes: List[float]
    is_stable: bool
    max_sharpe_drop: float


def detect_regime_changes(
    returns: np.ndarray,
    min_segment_length: int = 20,
    sharpe_diff_threshold: float = 1.0
) -> RegimeChangeResult:
    """检测收益序列中的结构性变化点 [P2-统计1]

    使用滚动Sharpe变化检测regime change。
    """
    n = len(returns)
    if n < 2 * min_segment_length:
        return RegimeChangeResult(n_regimes=1, change_points=[], regime_sharpes=[_compute_sharpe_simple(returns)], is_stable=True, max_sharpe_drop=0.0)

    change_points = []
    window = min_segment_length
    regime_sharpes = []

    left_sharpe = _compute_sharpe_simple(returns[:window])
    regime_sharpes.append(left_sharpe)

    for i in range(window, n - window + 1, window // 2):
        right_sharpe = _compute_sharpe_simple(returns[i:i + window])
        if abs(left_sharpe - right_sharpe) > sharpe_diff_threshold:
            change_points.append(i)
            regime_sharpes.append(right_sharpe)
        left_sharpe = right_sharpe

    if not change_points:
        regime_sharpes = [_compute_sharpe_simple(returns)]

    max_drop = 0.0
    for i in range(1, len(regime_sharpes)):
        drop = regime_sharpes[i - 1] - regime_sharpes[i]
        if drop > max_drop:
            max_drop = drop

    return RegimeChangeResult(
        n_regimes=len(regime_sharpes),
        change_points=change_points,
        regime_sharpes=regime_sharpes,
        is_stable=len(change_points) == 0,
        max_sharpe_drop=max_drop
    )


@dataclass(slots=True)
class SurvivorshipBiasResult:
    """幸存者偏差检测结果 [P2-统计2]"""
    bias_score: float
    n_surviving_strategies: int
    n_total_strategies: int
    survival_rate: float
    adjusted_sharpe: float


def detect_survivorship_bias(
    all_strategy_sharpes: np.ndarray,
    surviving_strategy_sharpes: np.ndarray,
    selection_threshold: float = 0.0
) -> SurvivorshipBiasResult:
    """检测幸存者偏差 [P2-统计2]"""
    n_total = len(all_strategy_sharpes)
    n_surviving = len(surviving_strategy_sharpes)

    if n_total < 2:
        return SurvivorshipBiasResult(bias_score=0.0, n_surviving_strategies=n_surviving, n_total_strategies=n_total, survival_rate=1.0, adjusted_sharpe=0.0)

    survival_rate = n_surviving / n_total

    mean_all = np.mean(all_strategy_sharpes)
    mean_surviving = np.mean(surviving_strategy_sharpes) if n_surviving > 0 else 0.0
    bias_score = mean_surviving - mean_all

    n_trials = n_total
    gamma = math.sqrt(2.0 * math.log(max(n_trials, 2)))
    expected_max_bias = gamma / math.sqrt(n_total) if n_total > 0 else 0.0
    adjusted_sharpe = mean_surviving - expected_max_bias

    return SurvivorshipBiasResult(
        bias_score=bias_score,
        n_surviving_strategies=n_surviving,
        n_total_strategies=n_total,
        survival_rate=survival_rate,
        adjusted_sharpe=adjusted_sharpe
    )


@dataclass(slots=True)
class ComplexityPenaltyResult:
    """策略复杂度惩罚结果 [P2-统计3]"""
    original_sharpe: float
    n_parameters: int
    n_observations: int
    aic: float
    bic: float
    penalized_sharpe: float


def apply_complexity_penalty(
    sharpe: float,
    n_parameters: int,
    n_observations: int,
    log_likelihood: Optional[float] = None
) -> ComplexityPenaltyResult:
    """应用AIC/BIC复杂度惩罚 [P2-统计3]"""
    if log_likelihood is None:
        log_likelihood = -0.5 * n_observations * (1.0 + math.log(2.0 * math.pi) + math.log(max(n_observations, 1)))

    aic = -2.0 * log_likelihood + 2.0 * n_parameters
    bic = -2.0 * log_likelihood + n_parameters * math.log(max(n_observations, 1))

    penalty_bic = n_parameters * math.log(max(n_observations, 1)) / (2.0 * n_observations)
    penalized_sharpe = sharpe * math.exp(-penalty_bic)

    return ComplexityPenaltyResult(
        original_sharpe=sharpe,
        n_parameters=n_parameters,
        n_observations=n_observations,
        aic=aic,
        bic=bic,
        penalized_sharpe=penalized_sharpe
    )


@dataclass(slots=True)
class StatisticalReliabilityResult:
    """交易次数与统计可靠性 [P2-统计4]"""
    n_trades: int
    min_trades_for_reliability: int
    is_reliable: bool
    reliability_score: float
    sharpe_standard_error: float


def assess_statistical_reliability(
    n_trades: int,
    sharpe: float = 0.0,
    min_trades: int = 30
) -> StatisticalReliabilityResult:
    """评估交易次数与统计可靠性 [P2-统计4]

    经验法则: Sharpe标准误差 ≈ sqrt((1 + 0.5*SR^2) / n)
    需要至少30笔交易，推荐>100笔。
    """
    if n_trades < 2:
        return StatisticalReliabilityResult(
            n_trades=n_trades, min_trades_for_reliability=min_trades,
            is_reliable=False, reliability_score=0.0, sharpe_standard_error=float('inf')
        )

    se = math.sqrt((1.0 + 0.5 * sharpe ** 2) / n_trades)
    reliability_score = min(1.0, n_trades / 100.0) * min(1.0, 1.0 / (se * math.sqrt(ANNUALIZE_FACTOR_DAILY)))
    is_reliable = n_trades >= min_trades and se < 0.5

    return StatisticalReliabilityResult(
        n_trades=n_trades,
        min_trades_for_reliability=min_trades,
        is_reliable=is_reliable,
        reliability_score=reliability_score,
        sharpe_standard_error=se
    )


@dataclass(slots=True)
class ExtremeEventResult:
    """极端事件影响分析结果 [P2-统计5]"""
    n_extreme_events: int
    extreme_event_pct: float
    pnl_contribution_from_extreme: float
    sharpe_without_extreme: float
    sharpe_with_extreme: float
    extreme_impact_ratio: float


def analyze_extreme_events(
    returns: np.ndarray,
    pnl_contribution: Optional[np.ndarray] = None,
    threshold_sigma: float = 3.0
) -> ExtremeEventResult:
    """分析极端事件(黑天鹅)对回测指标的影响 [P2-统计5]"""
    n = len(returns)
    if n < 5:
        return ExtremeEventResult(0, 0.0, 0.0, 0.0, 0.0, 0.0)

    mean_r = np.mean(returns)
    std_r = np.std(returns, ddof=1)
    if std_r < 1e-10:
        return ExtremeEventResult(0, 0.0, 0.0, 0.0, 0.0, 0.0)

    extreme_mask = np.abs(returns - mean_r) > threshold_sigma * std_r
    n_extreme = int(np.sum(extreme_mask))

    sharpe_with = _compute_sharpe_simple(returns)
    normal_mask = ~extreme_mask
    normal_returns = returns[normal_mask]
    sharpe_without = _compute_sharpe_simple(normal_returns) if len(normal_returns) > 2 else sharpe_with

    if pnl_contribution is not None:
        extreme_pnl = float(np.sum(np.abs(pnl_contribution[extreme_mask])))
    else:
        extreme_pnl = float(np.sum(np.abs(returns[extreme_mask])))

    total_pnl = float(np.sum(np.abs(returns)))
    extreme_ratio = extreme_pnl / total_pnl if total_pnl > 1e-10 else 0.0

    return ExtremeEventResult(
        n_extreme_events=n_extreme,
        extreme_event_pct=n_extreme / n,
        pnl_contribution_from_extreme=extreme_pnl,
        sharpe_without_extreme=sharpe_without,
        sharpe_with_extreme=sharpe_with,
        extreme_impact_ratio=extreme_ratio
    )


@dataclass(slots=True)
class CostSensitivityResult:
    """佣金/滑点敏感性分析结果 [P2-统计6]"""
    base_sharpe: float
    sensitivity_points: Dict[str, Tuple[float, float]]
    sharpe_elasticity: float


def analyze_cost_sensitivity(
    returns: np.ndarray,
    commissions: np.ndarray,
    base_commission_bps: float = 1.0,
    slippage_bps: float = 0.5,
    test_multipliers: Optional[List[float]] = None
) -> CostSensitivityResult:
    """分析回测结果对佣金/滑点参数的敏感性 [P2-统计6]"""
    if test_multipliers is None:
        test_multipliers = [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]

    n = len(returns)
    if n < 5:
        return CostSensitivityResult(0.0, {}, 0.0)

    base_net_returns = returns - commissions * base_commission_bps / 10000.0
    base_sharpe = _compute_sharpe_simple(base_net_returns)

    sensitivity = {}
    for mult in test_multipliers:
        adjusted_commission = base_commission_bps * mult
        net_returns = returns - commissions * adjusted_commission / 10000.0
        sharpe_at_mult = _compute_sharpe_simple(net_returns)
        sensitivity[f"commission_x{mult}"] = (adjusted_commission, sharpe_at_mult)

    if base_sharpe > 1e-10 and len(test_multipliers) >= 2:
        m1, m2 = test_multipliers[0], test_multipliers[-1]
        s1 = sensitivity.get(f"commission_x{m1}", (0, 0))[1]
        s2 = sensitivity.get(f"commission_x{m2}", (0, 0))[1]
        elasticity = (s2 - s1) / base_sharpe / (m2 - m1) if (m2 - m1) != 0 else 0.0
    else:
        elasticity = 0.0

    return CostSensitivityResult(
        base_sharpe=base_sharpe,
        sensitivity_points=sensitivity,
        sharpe_elasticity=elasticity
    )


@dataclass(slots=True)
class MarketStabilityResult:
    """市场状态下策略稳定性检验结果 [P2-统计7]"""
    regime_sharpes: Dict[str, float]
    is_stable_across_regimes: bool
    stability_score: float
    worst_regime: str
    worst_regime_sharpe: float


def test_market_stability(
    returns: np.ndarray,
    volatility_regimes: Optional[Dict[str, np.ndarray]] = None
) -> MarketStabilityResult:
    """检验不同市场状态下策略表现稳定性 [P2-统计7]"""
    n = len(returns)
    if n < 20:
        return MarketStabilityResult({}, True, 1.0, "unknown", 0.0)

    if volatility_regimes is None:
        rolling_std = np.array([np.std(returns[max(0, i-20):i+1]) for i in range(n)])
        q33 = np.percentile(rolling_std, 33)
        q66 = np.percentile(rolling_std, 66)
        volatility_regimes = {
            "low_vol": returns[rolling_std <= q33],
            "mid_vol": returns[(rolling_std > q33) & (rolling_std <= q66)],
            "high_vol": returns[rolling_std > q66],
        }

    regime_sharpes = {}
    for name, regime_ret in volatility_regimes.items():
        if len(regime_ret) >= 10:
            regime_sharpes[name] = _compute_sharpe_simple(regime_ret)

    if not regime_sharpes:
        return MarketStabilityResult({}, True, 1.0, "unknown", 0.0)

    sharpes = list(regime_sharpes.values())
    mean_s = np.mean(sharpes)
    std_s = np.std(sharpes) if len(sharpes) > 1 else 0.0
    stability_score = max(0.0, 1.0 - std_s / max(abs(mean_s), 0.1))

    worst_regime = min(regime_sharpes, key=regime_sharpes.get)
    worst_sharpe = regime_sharpes[worst_regime]

    positive_count = sum(1 for s in sharpes if s > 0)
    is_stable = positive_count == len(sharpes) and worst_sharpe > 0

    return MarketStabilityResult(
        regime_sharpes=regime_sharpes,
        is_stable_across_regimes=is_stable,
        stability_score=stability_score,
        worst_regime=worst_regime,
        worst_regime_sharpe=worst_sharpe
    )


def _compute_sharpe_simple(returns: np.ndarray, risk_free_rate: float = 0.02, use_log_return: bool = False) -> float:
    """简单Sharpe计算（内部辅助）
    P1-R9-32修复: 扣除无风险利率rf=0.02，并添加对数收益率选项
    """
    if len(returns) < 2:
        return 0.0
    r = returns
    if use_log_return:
        r = np.log1p(r)
    rf_period = risk_free_rate / ANNUALIZE_FACTOR_DAILY
    mean_r = np.mean(r) - rf_period
    std_r = np.std(r, ddof=1)
    if std_r < 1e-10:
        return 0.0
    return float(mean_r / std_r * math.sqrt(ANNUALIZE_FACTOR_DAILY))
