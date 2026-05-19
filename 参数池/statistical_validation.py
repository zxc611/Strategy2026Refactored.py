import numpy as np
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class CounterfactualResult:
    actual_expected_value: float
    percentile_95: float
    p_value: float
    delta_contribution_pct: float
    passed: bool
    n_shuffles: int
    null_distribution_mean: float
    null_distribution_std: float


class CounterfactualValidator:
    def __init__(self, n_shuffles: int = 1000, confidence_level: float = 0.95,
                 min_delta_contribution_pct: float = 0.40):
        self._n_shuffles = n_shuffles
        self._confidence_level = confidence_level
        self._min_delta_contribution_pct = min_delta_contribution_pct

    def validate(self, trade_results: List[Dict[str, Any]],
                 option_returns: Optional[List[float]] = None) -> CounterfactualResult:
        if not trade_results:
            return CounterfactualResult(0.0, 0.0, 1.0, 0.0, False, 0, 0.0, 0.0)

        actual_ev = np.mean([t.get('pnl', 0.0) for t in trade_results])
        total_pnl = sum(abs(t.get('pnl', 0.0)) for t in trade_results)
        delta_pnl = sum(abs(t.get('delta_pnl', 0.0)) for t in trade_results)
        delta_contribution_pct = delta_pnl / total_pnl if total_pnl > 0 else 0.0

        if option_returns is None:
            option_returns = [t.get('option_return', 0.0) for t in trade_results]

        null_evs = []
        pnls = [t.get('pnl', 0.0) for t in trade_results]
        for _ in range(self._n_shuffles):
            shuffled_returns = np.random.permutation(option_returns)
            shuffled_pnls = np.array(pnls) * np.sign(shuffled_returns[:len(pnls)])
            null_evs.append(np.mean(shuffled_pnls))

        null_evs = np.array(null_evs)
        percentile_threshold = np.percentile(null_evs, self._confidence_level * 100)
        p_value = np.mean(null_evs >= actual_ev)

        ev_passed = actual_ev > percentile_threshold
        delta_passed = delta_contribution_pct >= self._min_delta_contribution_pct
        passed = ev_passed and delta_passed

        logger.info(
            "[CounterfactualValidator] n=%d actual_ev=%.4f p95=%.4f p=%.4f delta_pct=%.2f%% passed=%s",
            self._n_shuffles, actual_ev, percentile_threshold, p_value,
            delta_contribution_pct * 100, passed,
        )

        return CounterfactualResult(
            actual_expected_value=actual_ev,
            percentile_95=percentile_threshold,
            p_value=p_value,
            delta_contribution_pct=delta_contribution_pct,
            passed=passed,
            n_shuffles=self._n_shuffles,
            null_distribution_mean=float(np.mean(null_evs)),
            null_distribution_std=float(np.std(null_evs)),
        )


@dataclass
class MonteCarloResult:
    survival_rate: float
    n_simulations: int
    n_years: float
    max_risk_ratio: float
    passed: bool
    median_final_equity: float
    worst_final_equity: float
    bankruptcy_count: int


class MonteCarloBankruptcyValidator:
    def __init__(self, n_simulations: int = 1000, n_years: float = 5.0,
                 min_survival_rate: float = 0.99):
        self._n_simulations = n_simulations
        self._n_years = n_years
        self._min_survival_rate = min_survival_rate

    def validate(self, initial_equity: float, win_rate: float, win_loss_ratio: float,
                 max_risk_ratio: float, n_trades_per_year: int = 252,
                 commission_per_trade: float = 0.0,
                 slippage_bps: float = 1.0) -> MonteCarloResult:
        n_total_trades = int(n_trades_per_year * self._n_years)
        final_equities = []
        bankruptcy_count = 0

        for _ in range(self._n_simulations):
            equity = initial_equity
            bankrupt = False
            for _ in range(n_total_trades):
                if equity <= 0:
                    bankrupt = True
                    break
                risk_amount = equity * max_risk_ratio
                if np.random.random() < win_rate:
                    pnl = risk_amount * win_loss_ratio
                else:
                    pnl = -risk_amount
                pnl -= commission_per_trade + equity * slippage_bps / 10000
                equity += pnl
                if equity <= 0:
                    bankrupt = True
                    break
            final_equities.append(max(0, equity))
            if bankrupt:
                bankruptcy_count += 1

        survival_rate = 1.0 - bankruptcy_count / self._n_simulations
        passed = survival_rate >= self._min_survival_rate
        final_arr = np.array(final_equities)

        logger.info(
            "[MonteCarloValidator] n=%d years=%.1f risk=%.2f survival=%.2f%% passed=%s",
            self._n_simulations, self._n_years, max_risk_ratio,
            survival_rate * 100, passed,
        )

        return MonteCarloResult(
            survival_rate=survival_rate,
            n_simulations=self._n_simulations,
            n_years=self._n_years,
            max_risk_ratio=max_risk_ratio,
            passed=passed,
            median_final_equity=float(np.median(final_arr)),
            worst_final_equity=float(np.min(final_arr)),
            bankruptcy_count=bankruptcy_count,
        )
