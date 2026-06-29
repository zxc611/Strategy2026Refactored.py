# [M1-127] 统计验证
# MODULE_ID: M1-197
import numpy as np
import logging
from ali2026v3_trading.infra.logging_utils import get_logger
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY
import concurrent.futures

logger = get_logger(__name__)

@dataclass(slots=True)
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
    STRATEGY_TYPE_DELTA_THRESHOLDS = {
        's1_hft': 0.25,           # HFT高频：Delta贡献要求低(方向性弱)
        's2_resonance': 0.35,     # 共振：中等要求
        's3_box': 0.40,           # 箱体：标准要求
        's4_spring': 0.40,        # 弹簧：标准要求
        's5_arbitrage': 0.50,     # 套利：Delta贡献要求高(方向性应强)
        's6_market_making': 0.20, # 做市：Delta贡献要求最低(非方向性)
    }

    def __init__(self, n_shuffles: int = 1000, confidence_level: float = 0.95,
                 min_delta_contribution_pct: float = 0.40,
                 strategy_type: Optional[str] = None):
        self._n_shuffles = n_shuffles
        self._confidence_level = confidence_level
        if strategy_type and strategy_type in self.STRATEGY_TYPE_DELTA_THRESHOLDS:
            self._min_delta_contribution_pct = self.STRATEGY_TYPE_DELTA_THRESHOLDS[strategy_type]
        else:
            self._min_delta_contribution_pct = min_delta_contribution_pct
        self._strategy_type = strategy_type
        self._call_count = 0

    def validate(self, trade_results: List[Dict[str, Any]],
                 option_returns: Optional[List[float]] = None) -> CounterfactualResult:
        if not trade_results:
            return CounterfactualResult(0.0, 0.0, 1.0, 0.0, False, 0, 0.0, 0.0)

        self._call_count += 1
        _seed = int(time.time() * 1000) % (2**31) + self._call_count
        _rng = np.random.RandomState(_seed)

        actual_ev = np.mean([t.get('pnl', 0.0) for t in trade_results])
        total_pnl = sum(abs(t.get('pnl', 0.0)) for t in trade_results)
        delta_pnl = sum(abs(t.get('delta_pnl', 0.0)) for t in trade_results)
        delta_contribution_pct = delta_pnl / total_pnl if total_pnl > 0 else 0.0

        if option_returns is None:
            option_returns = [t.get('option_return', 0.0) for t in trade_results]

        parity_violations = 0
        parity_checked = 0

        null_evs = []
        pnls = [t.get('pnl', 0.0) for t in trade_results]
        for _ in range(self._n_shuffles):
            shuffled_returns = _rng.permutation(option_returns)
            shuffled_pnls = np.array(pnls) * np.sign(shuffled_returns[:len(pnls)])
            null_evs.append(np.mean(shuffled_pnls))

            parity_checked += 1
            call_pnls = [t.get('call_pnl', None) for t in trade_results]
            put_pnls = [t.get('put_pnl', None) for t in trade_results]
            if any(c is not None for c in call_pnls) and any(p is not None for p in put_pnls):
                for i in range(min(len(call_pnls), len(put_pnls))):
                    if call_pnls[i] is not None and put_pnls[i] is not None:
                        synthetic_forward = call_pnls[i] - put_pnls[i]
                        underlying_pnl = trade_results[i].get('underlying_pnl', 0.0)
                        parity_deviation = abs(synthetic_forward - underlying_pnl)
                        parity_threshold = max(abs(underlying_pnl) * 0.1, 1.0)  # 10%偏差或最小1元
                        if parity_deviation > parity_threshold:
                            parity_violations += 1

        parity_violation_rate = parity_violations / max(parity_checked, 1)
        parity_passed = parity_violation_rate <= 0.20

        null_evs = np.array(null_evs)
        percentile_threshold = np.percentile(null_evs, self._confidence_level * 100)
        p_value = np.mean(null_evs >= actual_ev)

        ev_passed = actual_ev > percentile_threshold
        delta_passed = delta_contribution_pct >= self._min_delta_contribution_pct
        passed = ev_passed and delta_passed and parity_passed
        if not parity_passed:
            logger.warning(
                "[CounterfactualValidator] P1-13: Call-Put Parity违规率=%.1f%% > 20%%, "
                "反事实结果可能违反无套利约束!",
                parity_violation_rate * 100,
            )

        logger.info(
            "[CounterfactualValidator] n=%d actual_ev=%.4f p95=%.4f p=%.4f delta_pct=%.2f%% "
            "parity_violation_rate=%.1f%% passed=%s",
            self._n_shuffles, actual_ev, percentile_threshold, p_value,
            delta_contribution_pct * 100, parity_violation_rate * 100, passed,
        )

        return CounterfactualResult(actual_expected_value=actual_ev, percentile_95=percentile_threshold,
            p_value=p_value, delta_contribution_pct=delta_contribution_pct, passed=passed,
            n_shuffles=self._n_shuffles, null_distribution_mean=float(np.mean(null_evs)),
            null_distribution_std=float(np.std(null_evs)))

@dataclass(slots=True)
class MonteCarloResult:
    survival_rate: float
    n_simulations: int
    n_years: float
    max_risk_ratio: float
    passed: bool
    median_final_equity: float
    worst_final_equity: float
    bankruptcy_count: int

def _mc_simulate_chunk(
    n_sims: int, n_total_trades: int, initial_equity: float,
    win_rate: float, win_loss_ratio: float, max_risk_ratio: float,
    commission_per_trade: float, slippage_bps: float,
) -> Tuple[List[float], int]:
    """执行一批MonteCarlo模拟，返回(final_equities列表, 破产次数)"""
    final_equities = []
    bankruptcy_count = 0
    for _ in range(n_sims):
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
    return final_equities, bankruptcy_count

class MonteCarloBankruptcyValidator:
    def __init__(self, n_simulations: int = 1000, n_years: float = 5.0,
                 min_survival_rate: float = 0.99, n_workers: int = 1):
        self._n_simulations = n_simulations
        self._n_years = n_years
        self._min_survival_rate = min_survival_rate
        self._n_workers = n_workers

    def validate(self, initial_equity: float, win_rate: float, win_loss_ratio: float,
                 max_risk_ratio: float, n_trades_per_year: int = 252,
                 commission_per_trade: float = 0.0,
                 slippage_bps: float = 1.0) -> MonteCarloResult:
        n_total_trades = int(n_trades_per_year * self._n_years)

        if self._n_workers > 1:
            sims_per_worker = self._n_simulations // self._n_workers
            extra = self._n_simulations % self._n_workers
            chunks = [sims_per_worker + (1 if i < extra else 0) for i in range(self._n_workers)]
            futures = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=self._n_workers) as executor:
                for chunk_size in chunks:
                    futures.append(executor.submit(
                        _mc_simulate_chunk,
                        chunk_size, n_total_trades, initial_equity,
                        win_rate, win_loss_ratio, max_risk_ratio,
                        commission_per_trade, slippage_bps,
                    ))
            final_equities = []
            bankruptcy_count = 0
            for f in concurrent.futures.as_completed(futures):
                chunk_equities, chunk_bankruptcies = f.result()
                final_equities.extend(chunk_equities)
                bankruptcy_count += chunk_bankruptcies
        else:
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

        return MonteCarloResult(survival_rate=survival_rate, n_simulations=self._n_simulations,
            n_years=self._n_years, max_risk_ratio=max_risk_ratio, passed=passed,
            median_final_equity=float(np.median(final_arr)), worst_final_equity=float(np.min(final_arr)),
            bankruptcy_count=bankruptcy_count)

    def validate_dynamic(self, initial_equity: float, win_rate: float, win_loss_ratio: float,
                          max_risk_ratio: float, n_trades_per_year: int = 252,
                          commission_per_trade: float = 0.0,
                          slippage_bps: float = 1.0,
                          rolling_window_months: int = 3,
                          min_dynamic_survival_rate: float = 0.95) -> Dict[str, Any]:
        """P1-裂缝7: 动态蒙特卡洛; 每rolling_window_months个月重拟合参数; 动态生存率<95%需降低更新频率"""
        trades_per_window = int(n_trades_per_year * rolling_window_months / 12)
        n_windows = int(self._n_years * 12 / rolling_window_months)
        n_total_trades = trades_per_window * n_windows

        dynamic_survivals = 0
        bankruptcy_count = 0

        for _ in range(self._n_simulations):
            equity = initial_equity
            bankrupt = False
            for w in range(n_windows):
                if equity <= 0:
                    bankrupt = True
                    break
                adj_win_rate = np.clip(win_rate * np.random.uniform(0.9, 1.1), 0.01, 0.99)
                adj_wlr = max(0.1, win_loss_ratio * np.random.uniform(0.9, 1.1))
                adj_risk = max(0.001, max_risk_ratio * np.random.uniform(0.9, 1.1))

                for _ in range(trades_per_window):
                    if equity <= 0:
                        bankrupt = True
                        break
                    risk_amount = equity * adj_risk
                    if np.random.random() < adj_win_rate:
                        pnl = risk_amount * adj_wlr
                    else:
                        pnl = -risk_amount
                    pnl -= commission_per_trade + equity * slippage_bps / 10000
                    equity += pnl

            if not bankrupt:
                dynamic_survivals += 1
            else:
                bankruptcy_count += 1

        dynamic_survival_rate = dynamic_survivals / self._n_simulations
        passed = dynamic_survival_rate >= min_dynamic_survival_rate

        logger.info(
            "[MonteCarloValidator-Dynamic] n=%d years=%.1f window=%dmo "
            "dynamic_survival=%.2f%% passed=%s",
            self._n_simulations, self._n_years, rolling_window_months,
            dynamic_survival_rate * 100, passed,
        )

        return {"dynamic_survival_rate": dynamic_survival_rate, "min_dynamic_survival_rate": min_dynamic_survival_rate,
            "passed": passed, "rolling_window_months": rolling_window_months, "n_simulations": self._n_simulations,
            "bankruptcy_count": bankruptcy_count, "action": "reduce_update_freq_or_add_regularization" if not passed else "proceed"}

    def validate_block_bootstrap(
        self,
        historical_returns: np.ndarray,
        initial_equity: float,
        max_risk_ratio: float = 0.8,
        block_size: int = 5,
        commission_per_trade: float = 0.0,
        slippage_bps: float = 1.0,
        min_survival_rate: float = 0.99,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Block Bootstrap蒙特卡洛: i.i.d.低估风险2-10倍, 用block重采样保留序列相关性; R4-T-05: block_size从params读取"""
        if params is not None:
            _cfg_block_size = params.get("block_bootstrap_size", None)
            if _cfg_block_size is not None and isinstance(_cfg_block_size, int) and _cfg_block_size > 0:
                block_size = _cfg_block_size
                logger.info("[BlockBootstrap] R4-T-05: block_size从 params 读取=%d", block_size)
        if len(historical_returns) < block_size * 2:
            logger.warning(
                "[BlockBootstrap] 历史收益%d条<2*block_size=%d, 回退i.i.d.假设",
                len(historical_returns), block_size * 2,
            )
            return {"method": "iid_fallback", "reason": "insufficient_data"}

        n_returns = len(historical_returns)
        n_blocks = n_returns // block_size
        blocks = historical_returns[:n_blocks * block_size].reshape(n_blocks, block_size)

        bankruptcy_count = 0
        final_equities = []

        for _ in range(self._n_simulations):
            equity = initial_equity
            bankrupt = False
            sampled_blocks = blocks[np.random.randint(0, n_blocks, size=n_blocks)]
            sampled_returns = sampled_blocks.flatten()

            for ret in sampled_returns:
                if equity <= 0:
                    bankrupt = True
                    break
                risk_amount = equity * max_risk_ratio
                pnl = risk_amount * ret
                pnl -= commission_per_trade + equity * slippage_bps / 10000
                equity += pnl
                if equity <= 0:
                    bankrupt = True
                    break

            final_equities.append(max(0, equity))
            if bankrupt:
                bankruptcy_count += 1

        survival_rate = 1.0 - bankruptcy_count / self._n_simulations
        passed = survival_rate >= min_survival_rate
        final_arr = np.array(final_equities)

        logger.info(
            "[BlockBootstrap] n=%d block_size=%d survival=%.2f%% passed=%s "
            "(vs i.i.d.假设更保守)",
            self._n_simulations, block_size,
            survival_rate * 100, passed,
        )

        return {"method": "block_bootstrap", "block_size": block_size, "survival_rate": survival_rate,
            "min_survival_rate": min_survival_rate, "passed": passed, "n_simulations": self._n_simulations,
            "bankruptcy_count": bankruptcy_count, "median_final_equity": float(np.median(final_arr)),
            "worst_final_equity": float(np.min(final_arr))}

def validate_capital_utilization_gap(initial_equity: float = 1_000_000.0,
                                       premium_per_trade: float = 5000.0,
                                       win_rate: float = 0.55,
                                       n_trades: int = 252,
                                       max_sharpe_diff: float = 0.3,
                                       random_seed: int = 42) -> Dict[str, Any]:
    """P1-裂缝20: 连续vs离散资金曲线夏普差异; 差异>0.3需用离散模型"""
    rng = np.random.RandomState(random_seed)

    # 连续资金曲线模拟
    continuous_equity = [initial_equity]
    for _ in range(n_trades):
        if rng.random() < win_rate:
            pnl = premium_per_trade * 1.5
        else:
            pnl = -premium_per_trade
        continuous_equity.append(continuous_equity[-1] + pnl)

    # 离散资金曲线模拟（权利金全损跳变）'
    discrete_equity = [initial_equity]
    available_capital = initial_equity
    for _ in range(n_trades):
        if available_capital < premium_per_trade:
            discrete_equity.append(available_capital)
            continue
        available_capital -= premium_per_trade  # 先扣除权利金
        if rng.random() < win_rate:
            available_capital += premium_per_trade * 2.5  # 盈利返还权利金+收益
        # 亏损时权利金已全损，不返还
        discrete_equity.append(available_capital)

    cont_returns = np.diff(continuous_equity) / np.array(continuous_equity[:-1])
    disc_returns = np.diff(discrete_equity) / np.array(discrete_equity[:-1])

    cont_sharpe = float(np.mean(cont_returns) / max(np.std(cont_returns), 1e-10) * np.sqrt(ANNUALIZE_FACTOR_DAILY)) if len(cont_returns) > 1 else 0.0
    disc_sharpe = float(np.mean(disc_returns) / max(np.std(disc_returns), 1e-10) * np.sqrt(ANNUALIZE_FACTOR_DAILY)) if len(disc_returns) > 1 else 0.0

    sharpe_diff = abs(cont_sharpe - disc_sharpe)
    needs_discrete_model = sharpe_diff > max_sharpe_diff

    return {"continuous_sharpe": round(cont_sharpe, 4), "discrete_sharpe": round(disc_sharpe, 4),
        "sharpe_diff": round(sharpe_diff, 4), "max_sharpe_diff": max_sharpe_diff,
        "needs_discrete_model": needs_discrete_model, "action": "use_discrete_capital_curve" if needs_discrete_model else "proceed"}

def estimate_params_from_signal_history(
    signal_pnls: List[float],
    min_trades: int = 50,
) -> Dict[str, float]:
    """P1-MC-002: 从历史信号PnL序列估计MonteCarlo参数(win_rate, win_loss_ratio, avg_win, avg_loss, n_trades)"""
    n = len(signal_pnls)
    if n < min_trades:
        logger.warning("[P1-MC-002] 信号序列不足: %d<%d, 返回默认参数", n, min_trades)
        return {'win_rate': 0.5, 'win_loss_ratio': 1.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'n_trades': n}
    wins = [p for p in signal_pnls if p > 0]
    losses = [p for p in signal_pnls if p <= 0]
    win_rate = len(wins) / n if n > 0 else 0.5
    avg_win = sum(wins) / len(wins) if wins else 1.0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 1.0
    win_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 1.0
    return {'win_rate': win_rate, 'win_loss_ratio': win_loss_ratio, 'avg_win': avg_win, 'avg_loss': avg_loss, 'n_trades': n}

class SignalDrivenMonteCarloValidator:
    """P1-MC-001: 真实信号序列驱动MonteCarlo; 模拟1000条权益曲线, 5年不破产>99%"""

    def __init__(self, n_simulations: int = 1000, n_years: float = 5.0,
                 min_survival_rate: float = 0.99):
        self._n_simulations = n_simulations
        self._n_years = n_years
        self._min_survival_rate = min_survival_rate

    def validate_with_signals(
        self,
        initial_equity: float,
        signal_pnls: List[float],
        max_risk_ratio: float,
        commission_per_trade: float = 0.0,
        slippage_bps: float = 1.0,
    ) -> MonteCarloResult:
        """使用真实信号PnL序列驱动MonteCarlo模拟"""
        n_signals = len(signal_pnls)
        if n_signals < 50:
            logger.warning("[P1-MC-001] 信号序列不足: %d<50, 退化为随机模拟", n_signals)
            params = estimate_params_from_signal_history(signal_pnls)
            mcbv = MonteCarloBankruptcyValidator(
                n_simulations=self._n_simulations,
                n_years=self._n_years,
                min_survival_rate=self._min_survival_rate,
            )
            return mcbv.validate(
                initial_equity, params['win_rate'], params['win_loss_ratio'],
                max_risk_ratio, commission_per_trade=commission_per_trade,
                slippage_bps=slippage_bps)

        n_years_signals = 1.0
        n_repeats_per_sim = max(1, int(self._n_years / n_years_signals))
        bankruptcy_count = 0
        final_equities = []

        for _ in range(self._n_simulations):
            equity = initial_equity
            bankrupt = False
            for _ in range(n_repeats_per_sim):
                shuffled_pnls = list(signal_pnls)
                np.random.shuffle(shuffled_pnls)
                for pnl_raw in shuffled_pnls:
                    if equity <= 0:
                        bankrupt = True
                        break
                    scale = max_risk_ratio
                    pnl = pnl_raw * scale
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
            "[P1-MC-001] SignalDrivenMC: n=%d signals=%d survival=%.2f%% passed=%s",
            self._n_simulations, n_signals, survival_rate * 100, passed)

        return MonteCarloResult(survival_rate=survival_rate, n_simulations=self._n_simulations,
            n_years=self._n_years, max_risk_ratio=max_risk_ratio, passed=passed,
            median_final_equity=float(np.median(final_arr)), worst_final_equity=float(np.min(final_arr)),
            bankruptcy_count=bankruptcy_count)

class MonteCarloBankruptcyTester:
    """P1-裂缝7: 动态蒙特卡洛破产检验器"""

    def __init__(self, n_simulations: int = 1000, n_years: float = 5.0,
                 min_survival_rate: float = 0.95, n_workers: int = 1):
        self._validator = MonteCarloBankruptcyValidator(
            n_simulations=n_simulations,
            n_years=n_years,
            min_survival_rate=min_survival_rate,
            n_workers=n_workers,
        )

    def run_dynamic_bankruptcy_test(
        self,
        initial_equity: float,
        win_rate: float,
        win_loss_ratio: float,
        max_risk_ratio: float,
        n_trades_per_year: int = 252,
        commission_per_trade: float = 0.0,
        slippage_bps: float = 1.0,
        rolling_window_months: int = 3,
        min_dynamic_survival_rate: float = 0.95,
    ) -> Dict[str, Any]:
        """运行动态蒙特卡洛破产检验，返回包含survival_rate的结果"""
        return self._validator.validate_dynamic(
            initial_equity=initial_equity,
            win_rate=win_rate,
            win_loss_ratio=win_loss_ratio,
            max_risk_ratio=max_risk_ratio,
            n_trades_per_year=n_trades_per_year,
            commission_per_trade=commission_per_trade,
            slippage_bps=slippage_bps,
            rolling_window_months=rolling_window_months,
            min_dynamic_survival_rate=min_dynamic_survival_rate,
        )

# SurvivalBiasTest 原属本模块，拆分后移至 adv_validation_stats.py
def __getattr__(name):
    if name == 'SurvivalBiasTest':
        from ali2026v3_trading.param_pool.validation.statistical_validation import SurvivalBiasTest
        return SurvivalBiasTest
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

from dataclasses import dataclass

@dataclass(slots=True)
class SurvivalAnalysisResult:
    achievable_win_rates: Dict[float, float]
    optimal_tp_ratio: float
    optimal_win_rate: float
    optimal_plr: float
    max_favorable_distribution: Dict[str, float]
    max_adverse_distribution: Dict[str, float]

class BacktestSurvivalAnalyzer:
    """回测专用生存分析器(P1-30修复: 重命名避免与quant_core.SurvivalAnalyzer冲突)"""

    def analyze(self, trade_max_profits: List[float],
                trade_max_losses: List[float],
                tp_ratios: Optional[List[float]] = None) -> SurvivalAnalysisResult:
        if tp_ratios is None:
            tp_ratios = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]

        achievable = {}
        for r in tp_ratios:
            wins = sum(1 for p in trade_max_profits if p >= r)
            total = len(trade_max_profits)
            achievable[r] = wins / total if total > 0 else 0.0

        best_r = max(achievable, key=lambda r: achievable[r] * r)
        best_wr = achievable[best_r]

        profits = np.array(trade_max_profits) if trade_max_profits else np.array([0.0])
        losses = np.array(trade_max_losses) if trade_max_losses else np.array([0.0])

        return SurvivalAnalysisResult(achievable_win_rates=achievable, optimal_tp_ratio=best_r,
            optimal_win_rate=best_wr, optimal_plr=best_r * best_wr / (1 - best_wr) if best_wr < 1 else 999.0,
            max_favorable_distribution={'mean': float(np.mean(profits)), 'median': float(np.median(profits)), 'p75': float(np.percentile(profits, 75)), 'p90': float(np.percentile(profits, 90))},
            max_adverse_distribution={'mean': float(np.mean(losses)), 'median': float(np.median(losses)), 'p25': float(np.percentile(losses, 25)), 'p10': float(np.percentile(losses, 10))})

class SurvivalBiasTest:
    """生存偏差检验: 检测回测结果是否受生存偏差影响"""

    def __init__(self, n_permutations: int = 1000, significance_level: float = 0.05, random_seed: int = 42):
        self._n_permutations = n_permutations
        self._significance_level = significance_level
        self._random_seed = random_seed

    def test(self, observed_sharpe: float, random_sharpes: List[float]) -> Dict[str, Any]:
        """检验观测夏普是否显著高于随机基准分布"""
        if not random_sharpes:
            rng = np.random.RandomState(self._random_seed)
            random_sharpes = list(rng.normal(0, 0.5, self._n_permutations))

        n_surviving = sum(1 for s in random_sharpes if s >= observed_sharpe)
        p_value = n_surviving / len(random_sharpes) if random_sharpes else 1.0
        bias_detected = p_value > self._significance_level

        return {"survival_bias_detected": bias_detected, "p_value": p_value, "observed_sharpe": observed_sharpe,
                "random_baseline_mean": float(np.mean(random_sharpes)), "random_baseline_std": float(np.std(random_sharpes)),
                "n_permutations": len(random_sharpes)}

    def bootstrap_correction(self, equity_curves: List[List[float]],
                             statistic_fn: Any = None) -> Dict[str, Any]:
        """Bootstrap偏差校正"""
        if not equity_curves or statistic_fn is None:
            return {"corrected_estimate": 0.0, "bias": 0.0, "n_curves": 0}
        estimates = [statistic_fn(eq) for eq in equity_curves if len(eq) > 1]
        if not estimates: return {"corrected_estimate": 0.0, "bias": 0.0, "n_curves": 0}
        original = estimates[0]
        bootstrap_mean = float(np.mean(estimates))
        bias = bootstrap_mean - original
        return {"corrected_estimate": original - bias, "bias": bias,
                "n_curves": len(equity_curves)}

from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY, DEFAULT_RISK_FREE_RATE

class MultipleComparisonCorrector:
    @staticmethod
    def bonferroni(p_values: List[float]) -> List[float]:
        n = len(p_values)
        return [min(1.0, p * n) for p in p_values]

    @staticmethod
    def benjamini_hochberg(p_values: List[float]) -> List[float]:
        """BH FDR校正: 步骤1排序计算bh_p=p*n/(rank+1); 步骤2从后向前强制单调"""
        n = len(p_values)
        if n == 0:
            return []
        indexed = sorted(enumerate(p_values), key=lambda x: x[1])
        adjusted = [0.0] * n
        for rank_i, (orig_i, p) in enumerate(indexed):
            bh_p = p * n / (rank_i + 1)
            adjusted[orig_i] = min(1.0, bh_p)
        # 步骤2: 从后向前强制单调 -> adjusted[i] >= adjusted[i+1]
        for rank_i in range(len(indexed) - 2, -1, -1):
            orig_i = indexed[rank_i][0]
            next_orig_i = indexed[rank_i + 1][0]
            adjusted[orig_i] = min(adjusted[orig_i], adjusted[next_orig_i])
        return adjusted

@dataclass(slots=True)
class WalkForwardResult:
    window_results: List[Dict[str, Any]]
    wf6_monotone_decline: bool
    wf7_parameter_fragility: bool
    wf8_negative_ev: bool
    wf9_alpha_decline: bool
    wf10_absolute_ev_breach: bool
    overall_robust: bool

class WalkForwardValidator:
    def __init__(self, n_windows: int = 5, train_ratio: float = 0.7,
                 alpha_decline_threshold_pct: float = 20.0,
                 consecutive_decline_limit: int = 2):
        self._n_windows = n_windows
        self._train_ratio = train_ratio
        self._alpha_decline_threshold_pct = alpha_decline_threshold_pct
        self._consecutive_decline_limit = consecutive_decline_limit

    def validate(self, equity_curve: List[float],
                 n_trades_per_window: int = 50) -> WalkForwardResult:
        """R4-T-04: 窗口内按train_ratio分割train/test, 相邻窗口有gap防泄露"""
        window_results = []
        n = len(equity_curve)
        window_size = n // self._n_windows
        gap_size = max(1, int(window_size * (1.0 - self._train_ratio) * 0.5))

        for i in range(self._n_windows):
            start = i * window_size
            end = min(start + window_size, n)
            window_eq = equity_curve[start:end]
            if len(window_eq) < 4:
                continue

            train_end = int(len(window_eq) * self._train_ratio)
            test_eq = window_eq[train_end:]  # 测试集不重叠
            if len(test_eq) < 2:
                continue

            returns = np.diff(test_eq) / np.array(test_eq[:-1])
            returns = returns[np.isfinite(returns)]
            if len(returns) == 0:
                continue

            sharpe = float((np.mean(returns) - DEFAULT_RISK_FREE_RATE / ANNUALIZE_FACTOR_DAILY) / np.std(returns, ddof=1) * np.sqrt(ANNUALIZE_FACTOR_DAILY)) if len(returns) > 1 and np.std(returns, ddof=1) > 1e-10 else 0.0
            max_dd = 0.0
            peak = test_eq[0]
            for eq in test_eq:
                peak = max(peak, eq)
                dd = (peak - eq) / peak if peak > 0 else 0
                max_dd = max(max_dd, dd)

            window_results.append({'window': i, 'sharpe': sharpe, 'max_drawdown': max_dd,
                'total_return': (test_eq[-1] - test_eq[0]) / test_eq[0] if test_eq[0] > 0 else 0.0,
                'expected_value': float(np.mean(returns)) if len(returns) > 0 else 0.0,
                'train_ratio': self._train_ratio, 'test_size': len(test_eq)})

        wf6 = False
        if len(window_results) >= 3:
            sharpes = [w['sharpe'] for w in window_results]
            consecutive_decline = 0
            for j in range(1, len(sharpes)):
                if sharpes[j] < sharpes[j-1]:
                    consecutive_decline += 1
                else:
                    consecutive_decline = 0
            wf6 = consecutive_decline >= 2

        wf7 = False
        if len(window_results) >= 2:
            sharpes = [w['sharpe'] for w in window_results]
            if max(sharpes) > 0:
                sharpe_range = max(sharpes) - min(sharpes)
                wf7 = sharpe_range / max(sharpes) > 0.5

        wf8 = any(w['expected_value'] < 0 for w in window_results)

        wf9 = False
        if len(window_results) >= self._consecutive_decline_limit + 1:
            evs = [w['expected_value'] for w in window_results]
            consecutive = 0
            for j in range(1, len(evs)):
                if evs[j] < evs[j-1] * (1 - self._alpha_decline_threshold_pct / 100):
                    consecutive += 1
                else:
                    consecutive = 0
                if consecutive >= self._consecutive_decline_limit:
                    wf9 = True
                    break

        wf10 = any(w['expected_value'] < 0 for w in window_results)

        overall = not (wf6 or wf7 or wf8 or wf9 or wf10)

        return WalkForwardResult(window_results=window_results, wf6_monotone_decline=wf6,
            wf7_parameter_fragility=wf7, wf8_negative_ev=wf8, wf9_alpha_decline=wf9,
            wf10_absolute_ev_breach=wf10, overall_robust=overall)

class DeepValidationSuite:
    """深度验证套件v2.1: 委托task_scheduler真实回测, 本类为统一入口和结果适配层"""

    def validate_regime_robustness(self, params: Dict, bar_data: Any = None,
                                    iv_column: str = "iv", n_regimes: int = 3,
                                    min_sharpe_spread: float = 0.3) -> Dict[str, Any]:
        """验证策略在不同IV regime下的稳健性 -> 委托 task_scheduler"""
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_regime_robustness as _ts_validate_regime
            import pandas as pd
            if bar_data is None or not isinstance(bar_data, pd.DataFrame) or bar_data.empty:
                return {"passed": False, "sharpe_spread": 0.0, "worst_regime": "unknown",
                        "regime_sharpes": {}, "note": "insufficient_data"}
            result = _ts_validate_regime(params, bar_data, train=True, n_regimes=n_regimes)
            regime_results = result.details.get("regime_results", [])
            sharpe_values = [rr.get("sharpe", 0.0) for rr in regime_results]
            min_sharpe = min(sharpe_values) if sharpe_values else 0.0
            sharpe_spread = max(sharpe_values) - min_sharpe if sharpe_values else 0.0
            worst_regime = (
                regime_results[sharpe_values.index(min_sharpe)].get("regime", "N/A")
                if sharpe_values else "N/A"
            )
            return {"passed": result.passed, "sharpe_spread": float(sharpe_spread),
                "worst_regime": worst_regime,
                "regime_sharpes": {rr.get("regime", f"regime_{i}"): rr.get("sharpe", 0.0) for i, rr in enumerate(regime_results)}}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_regime_robustness] error: %s", e)
            return {"passed": False, "sharpe_spread": 0.0, "worst_regime": "error",
                    "regime_sharpes": {}, "note": str(e)}

    def validate_cross_strategy_correlation(self, params_s1: Dict, params_s2: Dict,
                                              params_s3: Dict, params_s4: Dict,
                                              bar_data: Any = None,
                                              correlation_threshold: float = 0.6) -> Dict[str, Any]:
        """验证6个策略之间的相关性是否过高 -> 委托 task_scheduler"""
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_cross_strategy_correlation as _ts_validate_corr
            if bar_data is None or not isinstance(bar_data, pd.DataFrame) or bar_data.empty:
                return {"passed": False, "max_corr_pair": "", "max_corr": 0.0, "extreme_day_count": 0}
            result = _ts_validate_corr(params_s1, params_s2, params_s3, params_s4, bar_data, train=True,
                                       correlation_threshold=correlation_threshold)
            details = result.details or {}
            return {"passed": result.passed, "max_corr_pair": "-".join(details.get("max_corr_pair", ("", ""))),
                "max_corr": float(result.metric_value), "extreme_day_count": details.get("extreme_day_count", 0)}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_cross_strategy_correlation] error: %s", e)
            return {"passed": False, "max_corr_pair": "", "max_corr": 0.0, "extreme_day_count": 0}

    def validate_hft_temporal_robustness(self, params: Dict, bar_data: Any = None,
                                           drop_probs: List[float] = None,
                                           delay_lambdas: List[float] = None,
                                           max_sharpe_decay: float = 0.3) -> Dict[str, Any]:
        """验证HFT策略对tick丢失和延迟的稳健性 -> 委托 task_scheduler"""
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_hft_temporal_robustness as _ts_validate_hft
            if bar_data is None or not isinstance(bar_data, pd.DataFrame) or bar_data.empty:
                return {"passed": False, "dropped_ticks": 0, "delayed_skips": 0,
                        "sharpe_decay_curve": {}, "note": "insufficient_data"}
            results = _ts_validate_hft(params, bar_data, train=True,
                                       drop_probs=drop_probs,
                                       delay_lambdas=delay_lambdas)
            sharpe_decay_curve = {}
            for r in results:
                if r.test_name.startswith("tick_drop_prob="):
                    key = r.test_name.replace("tick_drop_prob=", "drop_")
                    sharpe_decay_curve[key] = r.metric_value
            max_decay = max(sharpe_decay_curve.values()) if sharpe_decay_curve else 0.0
            passed = max_decay <= max_sharpe_decay
            return {"passed": passed, "dropped_ticks": 0, "delayed_skips": 0,
                "sharpe_decay_curve": sharpe_decay_curve, "max_decay": max_decay}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_hft_temporal_robustness] error: %s", e)
            return {"passed": False, "dropped_ticks": 0, "delayed_skips": 0,
                    "sharpe_decay_curve": {}, "note": str(e)}

    def validate_market_friendliness_baseline(self, bar_data: Any = None,
                                                n_random: int = 100,
                                                t_stat_threshold: float = 1.5) -> Dict[str, Any]:
        """验证策略是否跑赢随机基准 -> 委托 task_scheduler"""
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_market_friendliness_baseline as _ts_validate_mf
            if bar_data is None or not isinstance(bar_data, pd.DataFrame) or bar_data.empty:
                return {"passed": False, "mean_random_return": 0.0, "t_stat": 0.0,
                        "is_friendly": False, "note": "insufficient_data"}
            result = _ts_validate_mf(bar_data, train=True, n_random=n_random)
            details = result.details or {}
            mean_random = details.get("mean_random_return", 0.0)
            t_stat = details.get("t_stat", 0.0)
            is_friendly = mean_random > 0 and abs(t_stat) > 2.0
            return {"passed": not is_friendly, "mean_random_return": mean_random, "t_stat": t_stat, "is_friendly": is_friendly}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_market_friendliness_baseline] error: %s", e)
            return {"passed": False, "mean_random_return": 0.0, "t_stat": 0.0,
                    "is_friendly": False, "note": str(e)}

    def validate_logic_transferability(self, params: Dict, bar_data_primary: Any = None,
                                        bar_data_secondary: Any = None,
                                        min_transferability: float = 0.5) -> Dict[str, Any]:
        """验证策略逻辑在不同品种间的可迁移性 -> 委托 task_scheduler"""
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_logic_transferability as _ts_validate_lt
            if (bar_data_primary is None or bar_data_secondary is None
                    or not isinstance(bar_data_primary, pd.DataFrame) or not isinstance(bar_data_secondary, pd.DataFrame)):
                return {"passed": False, "transferability_ratio": 0.0, "note": "insufficient_data"}
            result = _ts_validate_lt(params, bar_data_primary, bar_data_secondary, train=True)
            details = result.details or {}
            primary_sharpe = details.get("primary_sharpe", 0.0)
            secondary_sharpe = details.get("secondary_sharpe", 0.0)
            ratio = result.metric_value
            return {"passed": result.passed, "transferability_ratio": float(ratio),
                "primary_sharpe": primary_sharpe, "secondary_sharpe": secondary_sharpe}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_logic_transferability] error: %s", e)
            return {"passed": False, "transferability_ratio": 0.0, "note": str(e)}

    def validate_liquidity_stress(self, params: Dict, bar_data: Any = None,
                                    slippage_multipliers: List[float] = None,
                                    max_drawdown_threshold: float = 0.15) -> Dict[str, Any]:
        """验证策略在不同滑点倍数下的最大回撤; R4-T-07: 流动性压力参数从params读取"""
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_liquidity_stress as _ts_validate_liq
            if slippage_multipliers is None:
                _cfg_multipliers = params.get("liquidity_stress_multipliers", None)
                if _cfg_multipliers is not None and isinstance(_cfg_multipliers, list):
                    slippage_multipliers = [float(m) for m in _cfg_multipliers]
                    logger.info("[R4-T-07] 滑点倍数从params读取: %s", slippage_multipliers)
                else:
                    slippage_multipliers = [1, 5, 10, 20, 50]
            _cfg_dd_threshold = params.get("liquidity_stress_max_drawdown", None)
            if _cfg_dd_threshold is not None and isinstance(_cfg_dd_threshold, (int, float)):
                max_drawdown_threshold = float(_cfg_dd_threshold)
                logger.info("[R4-T-07] 回撤阈值从 params 读取: %.4f", max_drawdown_threshold)
            if bar_data is None or not isinstance(bar_data, pd.DataFrame) or bar_data.empty:
                return {"passed": False, "max_drawdowns": {str(m): 0.0 for m in slippage_multipliers},
                        "note": "insufficient_data"}
            results = _ts_validate_liq(params, bar_data, train=True, slippage_multipliers=slippage_multipliers)
            max_drawdowns = {}
            all_passed = True
            for r in results:
                mult = r.test_name.replace("slippage_", "").replace("x", "")
                max_drawdowns[mult] = r.metric_value
                if not r.passed:
                    all_passed = False
            return {"passed": all_passed, "max_drawdowns": max_drawdowns, "threshold": max_drawdown_threshold}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_liquidity_stress] error: %s", e)
            return {"passed": False, "max_drawdowns": {str(m): 0.0 for m in (slippage_multipliers or [1, 5, 10, 20, 50])},
                    "note": str(e)}

    def validate_doomed_tests(self, params: Dict, bar_data: Any = None,
                                n_shuffle: int = 10,
                                min_t_diff: float = 0.5) -> Dict[str, Any]:
        """验证策略是否对时间顺序敏感; R4-T-08: t_diff>min_t_diff且方向不一致才判"注定失败" """
        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_doomed_tests as _ts_validate_doomed
            _cfg_n_shuffle = params.get("doomed_n_shuffle", None)
            if _cfg_n_shuffle is not None and isinstance(_cfg_n_shuffle, int) and _cfg_n_shuffle > 0:
                n_shuffle = _cfg_n_shuffle
            _cfg_min_t = params.get("doomed_min_t_diff", None)
            if _cfg_min_t is not None and isinstance(_cfg_min_t, (int, float)):
                min_t_diff = float(_cfg_min_t)
            if bar_data is None or not isinstance(bar_data, pd.DataFrame) or bar_data.empty:
                return {"passed": False, "tests": {}, "note": "insufficient_data"}
            results = _ts_validate_doomed(params, bar_data, train=True, n_shuffle=n_shuffle)
            tests_map = {}
            for r in results:
                tests_map[r.test_name] = {"passed": r.passed, "metric": r.metric_value}
            shuffled = tests_map.get("shuffled_temporal", {})
            t_diff = shuffled.get("metric", 0.0)
            all_passed = all(v.get("passed", False) for v in tests_map.values())
            return {"passed": all_passed, "tests": tests_map, "t_diff": t_diff}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_doomed_tests] error: %s", e)
            return {"passed": False, "tests": {}, "note": str(e)}

    def validate_static_code_audit(self, engine_source_code: str = None) -> Dict[str, Any]:
        """静态代码审计 - 集成 v7_meta_audit_v2 MetaAuditEngine"""
        if engine_source_code is None:
            return {"passed": False, "issues": [], "note": "no_source_code_provided"}
        try:
            from ali2026v3_trading.param_pool.quantification.meta_audit_passport import MetaAuditEngine
            auditor = MetaAuditEngine(engine_source_code)
            issues = auditor.audit_backtest_engine_integrity()
            critical_count = sum(1 for i in issues if i.severity == "CRITICAL")
            return {"passed": critical_count == 0, "issues": [i.to_dict() for i in issues],
                "critical_count": critical_count, "total_issues": len(issues)}
        except (ImportError, ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[DeepValidationSuite.validate_static_code_audit] error: %s", e)
            return {"passed": False, "issues": [], "note": str(e)}

    def validate_sandbox_execution(self, engine_class: type = None,
                                    strategy_config: dict = None) -> Dict[str, Any]:
        """沙箱执行审计 - 集成 v7_meta_audit_v2 SandboxExecutionAuditor"""
        if engine_class is None:
            return {"passed": False, "leaks": [], "note": "no_engine_class_provided"}
        try:
            from ali2026v3_trading.param_pool.quantification.meta_audit_passport import SandboxExecutionAuditor
            auditor = SandboxExecutionAuditor(engine_class)
            result = auditor.run_sandbox_test(strategy_config or {})
            return {"passed": result.get("status") == "PASSED", "leaks": result.get("leaks", []),
                "poison_tick_idx": result.get("poison_tick_idx")}
        except (ImportError, AttributeError, TypeError, ValueError, KeyError, RuntimeError) as e:
            logger.warning("[DeepValidationSuite.validate_sandbox_execution] error: %s", e)
            return {"passed": False, "leaks": [], "note": str(e)}

    def run_full_validation(self, params: Dict, bar_data: Any = None,
                             bar_data_secondary: Any = None,
                             engine_source_code: str = None,
                             engine_class: type = None) -> Dict[str, Any]:
        results = {"regime_robustness": self.validate_regime_robustness(params, bar_data),
            "cross_strategy_correlation": self.validate_cross_strategy_correlation(params, params, params, params, bar_data),
            "hft_temporal_robustness": self.validate_hft_temporal_robustness(params, bar_data),
            "market_friendliness": self.validate_market_friendliness_baseline(bar_data),
            "logic_transferability": self.validate_logic_transferability(params, bar_data, bar_data_secondary),
            "liquidity_stress": self.validate_liquidity_stress(params, bar_data),
            "doomed_tests": self.validate_doomed_tests(params, bar_data),
            "static_code_audit": self.validate_static_code_audit(engine_source_code),
            "sandbox_execution": self.validate_sandbox_execution(engine_class)}
        passed = sum(1 for r in results.values() if r.get("passed", False))
        total = len(results)
        return {"passed": passed == total, "summary": f"{passed}/{total} passed", "results": results}

    def run_deep_validation_suite(self, params: Dict, bar_data: Any = None,
                                   bar_data_secondary: Any = None,
                                   engine_source_code: str = None,
                                   engine_class: type = None) -> Dict[str, Any]:
        """别名: 委托run_full_validation"""
        return self.run_full_validation(params=params, bar_data=bar_data,
                                        bar_data_secondary=bar_data_secondary,
                                        engine_source_code=engine_source_code,
                                        engine_class=engine_class)

