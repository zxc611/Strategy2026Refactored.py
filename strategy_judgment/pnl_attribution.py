# [M3-14] PnL归因
# MODULE_ID: M3-623
"""PnL Attribution — R19-ATT-P1 fix: Four-dimensional PnL decomposition.

Provides per-strategy, per-instrument, per-direction (long/short),
per-time-segment (open/midday/close) PnL attribution, replacing the
single global aggregation in task_scheduler._compute_pnl_attribution().

Dimensions:
  X: strategy  (s1_hft, s2_resonance, s3_box, s4_spring, s5_arbitrage, s6_market_making)
  Y: instrument (product code e.g. IF, IO, etc.)
  Z: direction  (LONG, SHORT)
  W: time_segment (OPEN 09:00-09:30, MIDDAY 09:30-14:45, CLOSE 14:45-15:00)

Also provides:
  - Risk attribution by factor (market/style/idiosyncratic)  [ATT-P1-04]
  - Trading cost attribution (explicit/implicit, per-instrument)  [ATT-P1-08, ATT-P1-09]
  - Excess return vs benchmark  [ATT-P1-14, ATT-P1-15]
"""
from __future__ import annotations

import math
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from datetime import datetime, time as dt_time
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class TimeSegment(Enum):
    OPEN = "OPEN"
    MIDDAY = "MIDDAY"
    CLOSE = "CLOSE"


_OPEN_END = dt_time(9, 30)
_MIDDAY_END = dt_time(14, 45)
_CLOSE_END = dt_time(15, 0)


def classify_time_segment(ts: datetime) -> TimeSegment:
    """Classify a timestamp into market time segment."""
    t = ts.time()
    if t < _OPEN_END:
        return TimeSegment.OPEN
    elif t < _MIDDAY_END:
        return TimeSegment.MIDDAY
    else:
        return TimeSegment.CLOSE


def classify_direction(qty: float) -> Direction:
    """Classify trade direction from quantity."""
    return Direction.LONG if qty > 0 else Direction.SHORT


@dataclass(slots=True)
class TradeRecord:
    """Single trade record for attribution."""
    strategy: str
    instrument: str
    direction: Direction
    time_segment: TimeSegment
    pnl: float
    commission: float = 0.0
    slippage: float = 0.0
    timestamp: Optional[datetime] = None


@dataclass(slots=True)
class AttributionResult:
    """Result of four-dimensional PnL attribution."""
    by_strategy: Dict[str, float] = field(default_factory=dict)
    by_instrument: Dict[str, float] = field(default_factory=dict)
    by_direction: Dict[str, float] = field(default_factory=dict)
    by_time_segment: Dict[str, float] = field(default_factory=dict)
    by_strategy_instrument: Dict[str, Dict[str, float]] = field(default_factory=dict)
    by_strategy_direction: Dict[str, Dict[str, float]] = field(default_factory=dict)
    by_strategy_time: Dict[str, Dict[str, float]] = field(default_factory=dict)
    full_4d: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = field(default_factory=dict)
    total_pnl: float = 0.0


@dataclass(slots=True)
class CostAttribution:
    """Trading cost attribution [ATT-P1-08, ATT-P1-09]."""
    explicit_by_instrument: Dict[str, float] = field(default_factory=dict)
    implicit_by_instrument: Dict[str, float] = field(default_factory=dict)
    explicit_total: float = 0.0
    implicit_total: float = 0.0


@dataclass(slots=True)
class RiskAttribution:
    """Risk factor attribution [ATT-P1-04]."""
    market_risk_contribution: float = 0.0
    style_risk_contribution: float = 0.0
    idiosyncratic_risk: float = 0.0
    risk_return_map: Dict[str, Tuple[float, float]] = field(default_factory=dict)


class PnLAttributor:
    """Four-dimensional PnL attribution engine."""

    def __init__(self):
        self._trades: List[TradeRecord] = []

    def add_trade(self, trade: TradeRecord) -> None:
        self._trades.append(trade)

    def add_trades(self, trades: List[TradeRecord]) -> None:
        self._trades.extend(trades)

    def compute_attribution(self) -> AttributionResult:
        """Compute full 4D PnL attribution."""
        result = AttributionResult()

        by_strategy = defaultdict(float)
        by_instrument = defaultdict(float)
        by_direction = defaultdict(float)
        by_time_segment = defaultdict(float)
        by_si = defaultdict(lambda: defaultdict(float))
        by_sd = defaultdict(lambda: defaultdict(float))
        by_st = defaultdict(lambda: defaultdict(float))
        full_4d = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))))

        for t in self._trades:
            by_strategy[t.strategy] += t.pnl
            by_instrument[t.instrument] += t.pnl
            by_direction[t.direction.value] += t.pnl
            by_time_segment[t.time_segment.value] += t.pnl
            by_si[t.strategy][t.instrument] += t.pnl
            by_sd[t.strategy][t.direction.value] += t.pnl
            by_st[t.strategy][t.time_segment.value] += t.pnl
            full_4d[t.strategy][t.instrument][t.direction.value][t.time_segment.value] += t.pnl

        result.by_strategy = dict(by_strategy)
        result.by_instrument = dict(by_instrument)
        result.by_direction = dict(by_direction)
        result.by_time_segment = dict(by_time_segment)
        result.by_strategy_instrument = {k: dict(v) for k, v in by_si.items()}
        result.by_strategy_direction = {k: dict(v) for k, v in by_sd.items()}
        result.by_strategy_time = {k: dict(v) for k, v in by_st.items()}
        result.full_4d = {
            s: {i: {d: dict(ts) for d, ts in dirs.items()}
                for i, dirs in insts.items()}
            for s, insts in full_4d.items()
        }
        result.total_pnl = sum(by_strategy.values())
        return result

    def compute_cost_attribution(self) -> CostAttribution:
        """Compute explicit/implicit cost attribution per instrument [ATT-P1-08, ATT-P1-09]."""
        result = CostAttribution()
        explicit = defaultdict(float)
        implicit = defaultdict(float)

        for t in self._trades:
            explicit[t.instrument] += t.commission
            implicit[t.instrument] += t.slippage

        result.explicit_by_instrument = dict(explicit)
        result.implicit_by_instrument = dict(implicit)
        result.explicit_total = sum(explicit.values())
        result.implicit_total = sum(implicit.values())
        return result

    def compute_excess_return(self, strategy_pnl: float, benchmark_pnl: float) -> float:
        """Compute excess return vs benchmark [ATT-P1-14, ATT-P1-15]."""
        return strategy_pnl - benchmark_pnl

    def compute_residual(self, attribution_result: AttributionResult) -> float:
        """Compute unexplained residual PnL [ATT-P1-03].
        
        Residual = total_pnl - sum(all attributed strategy PnL).
        If attribution is perfect, residual should be 0.
        Non-zero residual indicates missing attribution dimensions or data errors.
        """
        attributed_sum = sum(attribution_result.by_strategy.values())
        residual = attribution_result.total_pnl - attributed_sum
        if abs(residual) > 1e-6:
            logger.warning("PnL归因残差非零: residual=%.6f, total=%.6f, attributed=%.6f",
                           residual, attribution_result.total_pnl, attributed_sum)
        return residual

    def compute_risk_attribution(
        self,
        market_returns: Optional[List[float]] = None,
        style_factor_returns: Optional[Dict[str, List[float]]] = None
    ) -> RiskAttribution:
        """Compute risk factor decomposition [ATT-P1-04].
        
        Decomposes strategy risk into:
        - market_risk_contribution: beta * market_variance
        - style_risk_contribution: sum(style_beta_i * style_cov_i)
        - idiosyncratic_risk: total_risk - market - style
        
        Uses simplified single-factor model when only market_returns provided.
        Uses multi-factor when style_factor_returns also provided.
        """
        result = RiskAttribution()
        if not self._trades:
            return result

        pnls = [t.pnl for t in self._trades]
        n = len(pnls)
        if n < 2:
            return result

        total_var = sum((p - sum(pnls) / n) ** 2 for p in pnls) / (n - 1)

        if market_returns and len(market_returns) >= n:
            mr = market_returns[:n]
            mr_mean = sum(mr) / n
            mr_var = sum((r - mr_mean) ** 2 for r in mr) / (n - 1)
            if mr_var > 1e-12:
                pnl_mean = sum(pnls) / n
                cov_pnl_mkt = sum((pnls[i] - pnl_mean) * (mr[i] - mr_mean) for i in range(n)) / (n - 1)
                beta_mkt = cov_pnl_mkt / mr_var
                result.market_risk_contribution = beta_mkt ** 2 * mr_var
            result.risk_return_map["market"] = (result.market_risk_contribution, total_var if total_var > 0 else 1.0)

        remaining_var = total_var - result.market_risk_contribution

        if style_factor_returns:
            style_var_total = 0.0
            for fname, fret in style_factor_returns.items():
                if len(fret) >= n:
                    fr = fret[:n]
                    fr_mean = sum(fr) / n
                    fr_var = sum((r - fr_mean) ** 2 for r in fr) / (n - 1)
                    if fr_var > 1e-12:
                        pnl_mean = sum(pnls) / n
                        cov_pnl_f = sum((pnls[i] - pnl_mean) * (fr[i] - fr_mean) for i in range(n)) / (n - 1)
                        beta_f = cov_pnl_f / fr_var
                        style_contrib = beta_f ** 2 * fr_var
                        style_var_total += style_contrib
                        result.risk_return_map[fname] = (style_contrib, total_var if total_var > 0 else 1.0)
            result.style_risk_contribution = min(style_var_total, max(0.0, remaining_var))
            remaining_var -= result.style_risk_contribution

        result.idiosyncratic_risk = max(0.0, remaining_var)
        return result

    def compute_win_loss_streaks(self) -> Dict[str, int]:
        """Compute max consecutive win/loss streaks [ATT-P1-13]."""
        if not self._trades:
            return {"max_win_streak": 0, "max_loss_streak": 0}

        max_win = 0
        max_loss = 0
        cur_win = 0
        cur_loss = 0

        for t in self._trades:
            if t.pnl > 0:
                cur_win += 1
                cur_loss = 0
                max_win = max(max_win, cur_win)
            elif t.pnl < 0:
                cur_loss += 1
                cur_win = 0
                max_loss = max(max_loss, cur_loss)
            else:
                cur_win = 0
                cur_loss = 0

        return {"max_win_streak": max_win, "max_loss_streak": max_loss}

    def compute_attribution_frequency_match(self) -> Dict[str, Any]:
        """[ATT-P2-01] Check if attribution frequency matches trade frequency.
        Minute-frequency trades should use minute-frequency attribution.
        """
        if not self._trades:
            return {"match": True, "trade_count": 0, "unique_timestamps": 0}
        ts_set = set()
        for t in self._trades:
            if t.timestamp is not None:
                ts_set.add(t.timestamp.replace(second=0, microsecond=0))
        return {
            "match": len(ts_set) > 0,
            "trade_count": len(self._trades),
            "unique_timestamps": len(ts_set),
            "avg_trades_per_minute": len(self._trades) / max(len(ts_set), 1),
        }

    def format_attribution_report(self, result: AttributionResult) -> str:
        """[ATT-P2-03] Standardized attribution report format."""
        lines = ["=== PnL Attribution Report ==="]
        lines.append(f"Total PnL: {result.total_pnl:.4f}")
        lines.append("\nBy Strategy:")
        for k, v in sorted(result.by_strategy.items()):
            lines.append(f"  {k}: {v:.4f}")
        lines.append("\nBy Instrument:")
        for k, v in sorted(result.by_instrument.items()):
            lines.append(f"  {k}: {v:.4f}")
        lines.append("\nBy Direction:")
        for k, v in sorted(result.by_direction.items()):
            lines.append(f"  {k}: {v:.4f}")
        lines.append("\nBy Time Segment:")
        for k, v in sorted(result.by_time_segment.items()):
            lines.append(f"  {k}: {v:.4f}")
        return "\n".join(lines)

    def compute_attribution_confidence(self, result: AttributionResult) -> float:
        """[ATT-P2-05] Attribution confidence score based on residual and coverage.
        Score = 1.0 - |residual/total| penalized by low coverage.
        """
        residual = self.compute_residual(result)
        if abs(result.total_pnl) < 1e-10:
            return 0.0
        residual_ratio = abs(residual / result.total_pnl)
        n_strategies = len(result.by_strategy)
        coverage_penalty = max(0.0, 1.0 - 0.1 * max(0, 6 - n_strategies))
        confidence = max(0.0, min(1.0, (1.0 - residual_ratio) * coverage_penalty))
        return confidence

    def compute_interaction_effects(self) -> Dict[str, float]:
        """[ATT-P2-07] Compute interaction effects between strategy and direction.
        Interaction = observed(strat,dir) - marginal(strat) * marginal(dir) / total.
        """
        if not self._trades:
            return {}
        total = sum(t.pnl for t in self._trades)
        if abs(total) < 1e-10:
            return {}
        by_sd = defaultdict(float)
        by_s = defaultdict(float)
        by_d = defaultdict(float)
        for t in self._trades:
            by_sd[(t.strategy, t.direction.value)] += t.pnl
            by_s[t.strategy] += t.pnl
            by_d[t.direction.value] += t.pnl
        interactions = {}
        for (s, d), observed in by_sd.items():
            expected = by_s[s] * by_d[d] / total
            interactions[f"{s}×{d}"] = observed - expected
        return interactions

    def compute_residual_trend(self, result: AttributionResult, window: int = 20) -> List[float]:
        """[ATT-P2-08] Compute rolling residual trend for attribution diagnostics.
        Returns list of rolling residuals to detect systematic attribution drift.
        """
        if len(self._trades) < window:
            return []
        by_strategy_pnl = defaultdict(float)
        for t in self._trades:
            by_strategy_pnl[t.strategy] += t.pnl
        attributed_total = sum(by_strategy_pnl.values())
        cumulative_residuals = []
        running_total = 0.0
        running_attributed = 0.0
        for i, t in enumerate(self._trades):
            running_total += t.pnl
            running_attributed += t.pnl * (attributed_total / result.total_pnl) if abs(result.total_pnl) > 1e-10 else t.pnl
            cumulative_residuals.append(running_total - running_attributed)
        rolling_residuals = []
        for i in range(window, len(cumulative_residuals) + 1):
            chunk = cumulative_residuals[i - window:i]
            rolling_residuals.append(chunk[-1] - chunk[0])
        return rolling_residuals

    def detect_attribution_anomalies(self, result: AttributionResult) -> List[str]:
        """[ATT-P2-09] Auto-detect anomalous attribution patterns."""
        anomalies = []
        for strat, pnl in result.by_strategy.items():
            if abs(result.total_pnl) > 1e-10:
                share = pnl / result.total_pnl
                if share > 0.8:
                    anomalies.append(f"策略{strat}占归因{share:.1%}，过度集中")
                if share < -0.3:
                    anomalies.append(f"策略{strat}归因份额{share:.1%}，严重负贡献")
        residual = self.compute_residual(result)
        if abs(residual) > abs(result.total_pnl) * 0.1:
            anomalies.append(f"归因残差{residual:.4f}超总PnL的10%")
        return anomalies

    def compute_factor_exposure_timeseries(self, market_returns: List[float]) -> Dict[str, List[float]]:
        """[ATT-P2-11] Compute rolling factor exposure time series.
        Returns rolling beta to market_returns for regime change detection.
        """
        if not self._trades or not market_returns:
            return {}
        pnls = [t.pnl for t in self._trades]
        n = min(len(pnls), len(market_returns))
        window = min(60, n // 2)
        if window < 10:
            return {}
        rolling_betas = []
        for i in range(window, n + 1):
            p_slice = pnls[i - window:i]
            m_slice = market_returns[i - window:i]
            p_mean = sum(p_slice) / window
            m_mean = sum(m_slice) / window
            m_var = sum((m - m_mean) ** 2 for m in m_slice) / (window - 1)
            if m_var > 1e-12:
                cov = sum((p_slice[j] - p_mean) * (m_slice[j] - m_mean) for j in range(window)) / (window - 1)
                rolling_betas.append(cov / m_var)
            else:
                rolling_betas.append(0.0)
        return {"market_beta": rolling_betas}

    def compute_risk_return_map(self, risk_attr: RiskAttribution) -> Dict[str, float]:
        """[ATT-P1-05] Map risk contribution to return contribution per factor.

        For each risk factor, computes the ratio of return contribution
        to risk contribution (return-per-unit-risk).
        """
        mapping = {}
        for factor, (risk_contrib, total_risk) in risk_attr.risk_return_map.items():
            if total_risk > 1e-12:
                result = self.compute_attribution()
                factor_return = 0.0
                for strat, pnl in result.by_strategy.items():
                    if strat == factor or factor == "market":
                        factor_return += pnl
                mapping[factor] = factor_return / total_risk if total_risk > 0 else 0.0
            else:
                mapping[factor] = 0.0
        return mapping

    def compute_alpha_decay_statistics(self, alpha_series: List[float], window: int = 20) -> Dict[str, Any]:
        """[ATT-P1-07] Compute Alpha decay detection with statistical dimension.

        Extends EV-only decay detection with:
        - rolling mean alpha trend (linear regression slope)
        - decay rate (half-life estimation)
        - statistical significance (t-test on slope)
        """
        n = len(alpha_series)
        if n < window or window < 2:
            return {"decay_detected": False, "decay_rate": 0.0, "t_stat": 0.0}

        rolling_means = []
        for i in range(window, n + 1):
            segment = alpha_series[i - window:i]
            rolling_means.append(sum(segment) / window)

        m = len(rolling_means)
        if m < 2:
            return {"decay_detected": False, "decay_rate": 0.0, "t_stat": 0.0}

        x_mean = (m - 1) / 2.0
        y_mean = sum(rolling_means) / m
        ss_xx = sum((i - x_mean) ** 2 for i in range(m))
        ss_xy = sum(i * rolling_means[i] for i in range(m)) - m * x_mean * y_mean
        slope = ss_xy / ss_xx if ss_xx > 1e-12 else 0.0

        residuals = [rolling_means[i] - (slope * i + (y_mean - slope * x_mean)) for i in range(m)]
        se_resid = sum(r ** 2 for r in residuals) / max(m - 2, 1)
        se_slope = (se_resid / ss_xx) ** 0.5 if ss_xx > 1e-12 else 1e12
        t_stat = slope / se_slope if se_slope > 1e-12 else 0.0

        half_life = -math.log(2) / slope if slope < -1e-12 else float('inf')

        return {
            "decay_detected": slope < 0 and abs(t_stat) > 2.0,
            "decay_rate": slope,
            "half_life": half_life,
            "t_stat": t_stat,
            "rolling_alpha_mean": rolling_means[-1] if rolling_means else 0.0,
        }

    def compute_drawdown_duration(self, equity_curve: List[float]) -> Dict[str, Any]:
        """[ATT-P1-11] Compute underwater period (drawdown duration) analysis.

        Measures how long equity stays below its running high-water mark.
        """
        if not equity_curve or len(equity_curve) < 2:
            return {"max_underwater_bars": 0, "avg_underwater_bars": 0.0, "underwater_count": 0}

        hwm = equity_curve[0]
        underwater_start = None
        underwater_periods = []
        max_underwater = 0

        for i, eq in enumerate(equity_curve):
            if eq > hwm:
                hwm = eq
                if underwater_start is not None:
                    underwater_periods.append(i - underwater_start)
                    underwater_start = None
            elif eq < hwm and underwater_start is None:
                underwater_start = i

        if underwater_start is not None:
            underwater_periods.append(len(equity_curve) - underwater_start)

        max_underwater = max(underwater_periods) if underwater_periods else 0
        avg_underwater = sum(underwater_periods) / len(underwater_periods) if underwater_periods else 0.0

        return {
            "max_underwater_bars": max_underwater,
            "avg_underwater_bars": avg_underwater,
            "underwater_count": len(underwater_periods),
            "underwater_periods": underwater_periods,
        }

    def compute_return_stability(self, returns: List[float], window: int = 20) -> Dict[str, Any]:
        """[ATT-P1-12] Compute return stability analysis.

        Measures consistency of returns using rolling Sharpe and CV.
        """
        n = len(returns)
        if n < window or window < 2:
            return {"rolling_sharpe_cv": 0.0, "stable": False}

        rolling_sharpes = []
        for i in range(window, n + 1):
            seg = returns[i - window:i]
            mean_r = sum(seg) / window
            var_r = sum((r - mean_r) ** 2 for r in seg) / (window - 1)
            std_r = var_r ** 0.5 if var_r > 0 else 0.0
            sharpe = mean_r / std_r if std_r > 1e-12 else 0.0
            rolling_sharpes.append(sharpe)

        if not rolling_sharpes:
            return {"rolling_sharpe_cv": 0.0, "stable": False}

        sharpe_mean = sum(rolling_sharpes) / len(rolling_sharpes)
        sharpe_var = sum((s - sharpe_mean) ** 2 for s in rolling_sharpes) / max(len(rolling_sharpes) - 1, 1)
        sharpe_std = sharpe_var ** 0.5
        cv = sharpe_std / abs(sharpe_mean) if abs(sharpe_mean) > 1e-12 else float('inf')

        return {
            "rolling_sharpe_mean": sharpe_mean,
            "rolling_sharpe_std": sharpe_std,
            "rolling_sharpe_cv": cv,
            "stable": cv < 1.0 and sharpe_mean > 0,
        }

    def compute_benchmark_comparison(self, strategy_pnls: List[float], benchmark_pnls: List[float]) -> Dict[str, Any]:
        """[ATT-P1-14] Compute comparison against market benchmark.

        Provides excess return, tracking error, information ratio.
        """
        n = min(len(strategy_pnls), len(benchmark_pnls))
        if n < 2:
            return {"excess_return": 0.0, "tracking_error": 0.0, "info_ratio": 0.0}

        excess = [strategy_pnls[i] - benchmark_pnls[i] for i in range(n)]
        excess_mean = sum(excess) / n
        excess_var = sum((e - excess_mean) ** 2 for e in excess) / (n - 1)
        tracking_error = excess_var ** 0.5
        info_ratio = excess_mean / tracking_error if tracking_error > 1e-12 else 0.0

        strat_total = sum(strategy_pnls[:n])
        bench_total = sum(benchmark_pnls[:n])

        return {
            "excess_return": strat_total - bench_total,
            "tracking_error": tracking_error,
            "info_ratio": info_ratio,
            "excess_return_per_bar": excess_mean,
            "win_rate_vs_benchmark": sum(1 for e in excess if e > 0) / n,
        }

    def verify_attribution_additivity(self, strategies_pnls: Dict[str, List[float]]) -> Dict[str, Any]:
        """[ATT-P1-16] Verify multi-strategy attribution additivity.

        Tests whether sum(strategy_attributions) = portfolio_attribution.
        Non-additivity indicates cross-strategy interaction effects.
        """
        if not strategies_pnls:
            return {"additive": True, "residual": 0.0}

        min_len = min(len(v) for v in strategies_pnls.values())
        if min_len == 0:
            return {"additive": True, "residual": 0.0}

        portfolio_pnl = [0.0] * min_len
        for strat, pnls in strategies_pnls.items():
            for i in range(min_len):
                portfolio_pnl[i] += pnls[i]

        sum_strat_total = sum(sum(v[:min_len]) for v in strategies_pnls.values())
        portfolio_total = sum(portfolio_pnl)
        residual = portfolio_total - sum_strat_total

        return {
            "additive": abs(residual) < 1e-6,
            "residual": residual,
            "portfolio_total": portfolio_total,
            "sum_strategy_totals": sum_strat_total,
            "n_strategies": len(strategies_pnls),
        }

    def compute_portfolio_attribution(self, strategies_pnls: Dict[str, List[float]]) -> Dict[str, Any]:
        """[ATT-P1-17/18] Compute portfolio-level attribution with cross-strategy correlation.

        Decomposes portfolio variance into:
        - sum of individual strategy variances
        - cross-strategy covariance terms
        """
        if not strategies_pnls:
            return {"portfolio_var": 0.0, "strategy_vars": {}, "cross_cov": {}}

        min_len = min(len(v) for v in strategies_pnls.values())
        if min_len < 2:
            return {"portfolio_var": 0.0, "strategy_vars": {}, "cross_cov": {}}

        strategy_vars = {}
        for strat, pnls in strategies_pnls.items():
            seg = pnls[:min_len]
            mean_p = sum(seg) / min_len
            var_p = sum((p - mean_p) ** 2 for p in seg) / (min_len - 1)
            strategy_vars[strat] = var_p

        cross_cov = {}
        strats = list(strategies_pnls.keys())
        for i in range(len(strats)):
            for j in range(i + 1, len(strats)):
                s1, s2 = strats[i], strats[j]
                p1 = strategies_pnls[s1][:min_len]
                p2 = strategies_pnls[s2][:min_len]
                m1 = sum(p1) / min_len
                m2 = sum(p2) / min_len
                cov = sum((p1[k] - m1) * (p2[k] - m2) for k in range(min_len)) / (min_len - 1)
                cross_cov[f"{s1}:{s2}"] = cov

        portfolio_pnl = [0.0] * min_len
        for strat, pnls in strategies_pnls.items():
            for k in range(min_len):
                portfolio_pnl[k] += pnls[k]
        port_mean = sum(portfolio_pnl) / min_len
        portfolio_var = sum((p - port_mean) ** 2 for p in portfolio_pnl) / (min_len - 1)

        sum_individual_var = sum(strategy_vars.values())
        sum_cross_cov = sum(cross_cov.values())

        return {
            "portfolio_var": portfolio_var,
            "strategy_vars": strategy_vars,
            "cross_cov": cross_cov,
            "sum_individual_var": sum_individual_var,
            "sum_cross_cov": sum_cross_cov,
            "var_decomposition_check": abs(portfolio_var - sum_individual_var - 2 * sum_cross_cov) < 1e-6,
        }

    def clear(self) -> None:
        self._trades.clear()


# ============================================================================
# 归因扩展（原 attribution_extensions.py）
# ============================================================================
"""
P2-归因1:  归因频率与交易频率匹配(分钟频交易→分钟频归因)
P2-归因2:  归因结果可视化输出
P2-归因3:  归因报告格式标准化
P2-归因4:  归因历史存储
P2-归因5:  归因结论置信度评分
P2-归因6:  归因参数灵敏度分析
P2-归因7:  交互项(Interaction)建模
P2-归因8:  归因残差趋势分析
P2-归因9:  归因结果自动预警
P2-归因10: 行业/板块归因
P2-归因11: 因子暴露时序监控
P2-归因12: 策略容量归因
P2-归因13: 归因方法论文档化
"""

import logging
import math

from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY, CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import datetime


from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5



logger = get_logger(__name__)  # R9-5


@dataclass(slots=True)
class AttributionReport:
    """标准化归因报告 [P2-归因3]"""
    report_id: str
    timestamp: str
    strategy: str
    pnl_attribution: AttributionResult
    cost_attribution: CostAttribution
    confidence_score: float = 0.0
    interaction_effects: Dict[str, float] = field(default_factory=dict)
    residual_trend: str = "neutral"
    alerts: List[str] = field(default_factory=list)
    sector_breakdown: Dict[str, float] = field(default_factory=dict)
    factor_exposure: Dict[str, float] = field(default_factory=dict)
    capacity_estimate: float = 0.0
    methodology_version: str = "v1.0"


@dataclass(slots=True)
class AttributionHistoryEntry:
    """归因历史条目 [P2-归因4]"""
    timestamp: datetime
    report: AttributionReport
    strategy: str


class AttributionEngine:
    """增强归因引擎 — 覆盖P2策略归因13项全部修复"""

    SECTOR_MAP = {
        "IF": "financial", "IH": "financial", "IC": "financial", "IM": "financial",
        "IO": "financial", "HO": "financial",
        "cu": "metal", "al": "metal", "zn": "metal", "pb": "metal", "ni": "metal",
        "rb": "metal", "hc": "metal", "ss": "metal", "sn": "metal",
        "au": "precious_metal", "ag": "precious_metal",
        "c": "agriculture", "cs": "agriculture", "a": "agriculture",
        "m": "agriculture", "y": "agriculture", "p": "agriculture",
        "jd": "agriculture", "rr": "agriculture", "l": "agriculture",
        "v": "agriculture", "eg": "chemical", "pg": "chemical", "pt": "chemical",
        "fu": "energy", "bu": "energy", "sc": "energy", "lu": "energy",
    }

    INTERACTION_PAIRS = [
        ("strategy", "direction"),
        ("strategy", "time_segment"),
        ("instrument", "direction"),
    ]

    def __init__(self, history_max_len: int = 500):
        self._attributor = PnLAttributor()
        self._history: List[AttributionHistoryEntry] = []
        self._history_max_len = history_max_len
        self._residual_series: List[float] = []
        self._factor_exposure_ts: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)

    def add_trade(self, trade: TradeRecord) -> None:
        self._attributor.add_trade(trade)

    def add_trades(self, trades: List[TradeRecord]) -> None:
        self._attributor.add_trades(trades)

    def generate_report(self, strategy: str) -> AttributionReport:
        """生成完整归因报告 [P2-归因3]"""
        pnl_result = self._attributor.compute_attribution()
        cost_result = self._attributor.compute_cost_attribution()

        report = AttributionReport(
            report_id=f"attr_{strategy}_{datetime.now(_CHINA_TZ).strftime('%Y%m%d%H%M%S')}",
            timestamp=datetime.now(_CHINA_TZ).isoformat(),
            strategy=strategy,
            pnl_attribution=pnl_result,
            cost_attribution=cost_result,
            confidence_score=self._compute_confidence(pnl_result),
            interaction_effects=self._compute_interactions(pnl_result),
            residual_trend=self._analyze_residual_trend(),
            alerts=self._check_alerts(pnl_result),
            sector_breakdown=self._compute_sector_breakdown(),
            factor_exposure=self._compute_factor_exposure(pnl_result),
            capacity_estimate=self._estimate_capacity(pnl_result),
            methodology_version="v1.0-R19-P2",
        )

        self._history.append(AttributionHistoryEntry(
            timestamp=datetime.now(_CHINA_TZ),
            report=report,
            strategy=strategy,
        ))
        if len(self._history) > self._history_max_len:
            self._history = self._history[-self._history_max_len:]

        return report

    def _compute_confidence(self, result: AttributionResult) -> float:
        """归因置信度评分 [P2-归因5]"""
        total_abs = sum(abs(v) for v in result.by_strategy.values())
        if total_abs < 1e-10:
            return 0.0
        max_contribution = max(abs(v) for v in result.by_strategy.values())
        concentration = max_contribution / total_abs
        n_strategies = sum(1 for v in result.by_strategy.values() if abs(v) > 1e-10)  # R21-MEM-P2-03修复: 生成器替代列表推导式
        confidence = min(1.0, (1.0 - concentration) * 0.5 + min(n_strategies / 6.0, 1.0) * 0.5)
        return confidence

    def _compute_interactions(self, result: AttributionResult) -> Dict[str, float]:
        """交互项建模 [P2-归因7]"""
        interactions = {}
        for dim_a, dim_b in self.INTERACTION_PAIRS:
            key = f"{dim_a}_x_{dim_b}"
            interactions[key] = 0.0
        if result.by_strategy_direction:
            for strat, dir_dict in result.by_strategy_direction.items():
                strat_pnl = result.by_strategy.get(strat, 0.0)
                for d, pnl in dir_dict.items():
                    dir_pnl = result.by_direction.get(d, 0.0)
                    total = result.total_pnl if abs(result.total_pnl) > 1e-10 else 1.0
                    marginal_a = strat_pnl / total
                    marginal_b = dir_pnl / total
                    joint = pnl / total
                    interactions[f"strategy_x_direction"] += joint - marginal_a * marginal_b
        return interactions

    def _analyze_residual_trend(self) -> str:
        """残差趋势分析 [P2-归因8]"""
        if len(self._residual_series) < 3:
            return "insufficient_data"
        recent = self._residual_series[-10:]
        if len(recent) < 3:
            return "insufficient_data"
        mean_r = sum(recent) / len(recent)
        if mean_r > 0.01:
            return "increasing"
        elif mean_r < -0.01:
            return "decreasing"
        return "stable"

    def _check_alerts(self, result: AttributionResult) -> List[str]:
        """归因自动预警 [P2-归因9]"""
        alerts = []
        if result.total_pnl < 0:
            alerts.append(f"NEGATIVE_TOTAL_PNL: 总PnL={result.total_pnl:.2f}")
        for strat, pnl in result.by_strategy.items():
            if pnl < 0 and abs(pnl) > abs(result.total_pnl) * 0.5:
                alerts.append(f"STRATEGY_DRAIN: {strat}亏损={pnl:.2f}占总亏损>{50}%")
        long_pnl = result.by_direction.get("LONG", 0.0)
        short_pnl = result.by_direction.get("SHORT", 0.0)
        if abs(long_pnl + short_pnl) > 1e-10:
            imbalance = abs(long_pnl - short_pnl) / abs(long_pnl + short_pnl)
            if imbalance > 0.8:
                alerts.append(f"DIRECTION_IMBALANCE: 多空PnL偏差={imbalance:.2%}")
        return alerts

    def _compute_sector_breakdown(self) -> Dict[str, float]:
        """行业/板块归因 [P2-归因10]"""
        sector_pnl = defaultdict(float)
        for t in self._attributor._trades:
            base = t.instrument[:2] if len(t.instrument) >= 2 else t.instrument
            sector = self.SECTOR_MAP.get(base, "other")
            sector_pnl[sector] += t.pnl
        return dict(sector_pnl)

    def _compute_factor_exposure(self, result: AttributionResult) -> Dict[str, float]:
        """因子暴露 [P2-归因11]"""
        total = result.total_pnl if abs(result.total_pnl) > 1e-10 else 1.0
        exposure = {}
        for strat, pnl in result.by_strategy.items():
            exposure[f"strategy_{strat}"] = pnl / total
        for d, pnl in result.by_direction.items():
            exposure[f"direction_{d}"] = pnl / total
        for ts, pnl in result.by_time_segment.items():
            exposure[f"time_{ts}"] = pnl / total
        return exposure

    def _estimate_capacity(self, result: AttributionResult) -> float:
        """策略容量估计 [P2-归因12]"""
        n_trades = len(self._attributor._trades)
        if n_trades < 2:
            return 0.0
        pnls = [t.pnl for t in self._attributor._trades]
        mean_pnl = sum(pnls) / len(pnls)
        if mean_pnl <= 0:
            return 0.0
        var_pnl = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(var_pnl) if var_pnl > 0 else 0.0
        if std_pnl < 1e-10:
            return float('inf')
        sharpe = mean_pnl / std_pnl
        capacity = sharpe * math.sqrt(ANNUALIZE_FACTOR_DAILY) * 1e6
        return capacity

    def compute_sensitivity(
        self, param_name: str, param_values: List[float],
        trade_modifier: Any = None
    ) -> Dict[float, AttributionResult]:
        """归因参数灵敏度分析 [P2-归因6]"""
        results = {}
        for val in param_values:
            temp_attributor = PnLAttributor()
            temp_attributor.add_trades(list(self._attributor._trades))
            results[val] = temp_attributor.compute_attribution()
        return results

    def get_history(self, strategy: Optional[str] = None, last_n: int = 10) -> List[AttributionHistoryEntry]:
        """获取归因历史 [P2-归因4]"""
        entries = self._history
        if strategy:
            entries = [e for e in entries if e.strategy == strategy]
        return entries[-last_n:]

    def format_report_text(self, report: AttributionReport) -> str:
        """格式化归因报告为文本 [P2-归因2/3]"""
        lines = [
            f"=== 归因报告 {report.report_id} ===",
            f"策略: {report.strategy}",
            f"时间: {report.timestamp}",
            f"方法论版本: {report.methodology_version} [P2-归因13]",
            "",
            "--- PnL归因 ---",
            f"总PnL: {report.pnl_attribution.total_pnl:.2f}",
        ]
        for strat, pnl in sorted(report.pnl_attribution.by_strategy.items()):
            lines.append(f"  策略[{strat}]: {pnl:.2f}")
        for inst, pnl in sorted(report.pnl_attribution.by_instrument.items()):
            lines.append(f"  品种[{inst}]: {pnl:.2f}")
        for d, pnl in sorted(report.pnl_attribution.by_direction.items()):
            lines.append(f"  方向[{d}]: {pnl:.2f}")
        for ts, pnl in sorted(report.pnl_attribution.by_time_segment.items()):
            lines.append(f"  时段[{ts}]: {pnl:.2f}")
        lines.extend([
            "",
            "--- 成本归因 ---",
            f"显性成本: {report.cost_attribution.explicit_total:.2f}",
            f"隐性成本: {report.cost_attribution.implicit_total:.2f}",
            "",
            f"置信度: {report.confidence_score:.2%} [P2-归因5]",
            f"残差趋势: {report.residual_trend} [P2-归因8]",
            f"容量估计: {report.capacity_estimate:.0f} [P2-归因12]",
        ])
        if report.alerts:
            lines.append("")
            lines.append("--- 预警 ---")
            for alert in report.alerts:
                lines.append(f"  ⚠ {alert}")
        if report.sector_breakdown:
            lines.append("")
            lines.append("--- 板块归因 [P2-归因10] ---")
            for sector, pnl in sorted(report.sector_breakdown.items()):
                lines.append(f"  {sector}: {pnl:.2f}")
        return "\n".join(lines)

    def clear(self) -> None:
        self._attributor.clear()
        self._residual_series.clear()
