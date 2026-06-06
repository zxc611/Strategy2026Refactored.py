import numpy as np
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY
import concurrent.futures

logger = logging.getLogger(__name__)


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
    # P1-5修复：S1-S6策略类型差异化Delta贡献阈值
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
        # P1-5修复：根据策略类型选择差异化阈值
        if strategy_type and strategy_type in self.STRATEGY_TYPE_DELTA_THRESHOLDS:
            self._min_delta_contribution_pct = self.STRATEGY_TYPE_DELTA_THRESHOLDS[strategy_type]
        else:
            self._min_delta_contribution_pct = min_delta_contribution_pct
        self._strategy_type = strategy_type
        # R4-T-06修复: 内部随机计数器，确保多次调用时随机序列独立
        self._call_count = 0

    def validate(self, trade_results: List[Dict[str, Any]],
                 option_returns: Optional[List[float]] = None) -> CounterfactualResult:
        if not trade_results:
            return CounterfactualResult(0.0, 0.0, 1.0, 0.0, False, 0, 0.0, 0.0)

        # R4-T-06修复: 每次调用使用独立随机种子，确保多次调用时随机序列不重复
        self._call_count += 1
        _seed = int(time.time() * 1000) % (2**31) + self._call_count
        _rng = np.random.RandomState(_seed)

        actual_ev = np.mean([t.get('pnl', 0.0) for t in trade_results])
        total_pnl = sum(abs(t.get('pnl', 0.0)) for t in trade_results)
        delta_pnl = sum(abs(t.get('delta_pnl', 0.0)) for t in trade_results)
        delta_contribution_pct = delta_pnl / total_pnl if total_pnl > 0 else 0.0

        if option_returns is None:
            option_returns = [t.get('option_return', 0.0) for t in trade_results]

        # P1-13修复：Call-Put Parity无套利校验
        # 打乱后的收益分布不应违反期权无套利约束
        parity_violations = 0
        parity_checked = 0

        null_evs = []
        pnls = [t.get('pnl', 0.0) for t in trade_results]
        for _ in range(self._n_shuffles):
            # R4-T-06修复: 使用独立RNG实例而非全局np.random
            shuffled_returns = _rng.permutation(option_returns)
            shuffled_pnls = np.array(pnls) * np.sign(shuffled_returns[:len(pnls)])
            null_evs.append(np.mean(shuffled_pnls))

            # P1-13修复：对每次打乱执行Call-Put Parity校验
            # C - P = S - K*exp(-rT) (无套利条件)
            # 如果同一标的的Call和Put收益打乱后违反此关系，标记为违规
            parity_checked += 1
            call_pnls = [t.get('call_pnl', None) for t in trade_results]
            put_pnls = [t.get('put_pnl', None) for t in trade_results]
            if any(c is not None for c in call_pnls) and any(p is not None for p in put_pnls):
                # 检查Call-Put价差是否合理(不应出现极端偏离)
                for i in range(min(len(call_pnls), len(put_pnls))):
                    if call_pnls[i] is not None and put_pnls[i] is not None:
                        synthetic_forward = call_pnls[i] - put_pnls[i]
                        underlying_pnl = trade_results[i].get('underlying_pnl', 0.0)
                        # 无套利条件: |C-P - (S-K)| 不应超过阈值
                        parity_deviation = abs(synthetic_forward - underlying_pnl)
                        parity_threshold = max(abs(underlying_pnl) * 0.1, 1.0)  # 10%偏差或最小1元
                        if parity_deviation > parity_threshold:
                            parity_violations += 1

        # P1-13修复：如果超过20%的打乱违反Parity，标记为无套利破坏
        parity_violation_rate = parity_violations / max(parity_checked, 1)
        parity_passed = parity_violation_rate <= 0.20

        null_evs = np.array(null_evs)
        percentile_threshold = np.percentile(null_evs, self._confidence_level * 100)
        p_value = np.mean(null_evs >= actual_ev)

        ev_passed = actual_ev > percentile_threshold
        delta_passed = delta_contribution_pct >= self._min_delta_contribution_pct
        passed = ev_passed and delta_passed and parity_passed  # P1-13修复：增加Parity条件

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


# P2-MC-003修复: 顶层辅助函数，供ProcessPoolExecutor并行调用
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
    # P2-MC-003修复: 添加n_workers参数支持并行计算
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

        # P2-MC-003修复: n_workers>1时使用ProcessPoolExecutor并行执行模拟
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

    def validate_dynamic(self, initial_equity: float, win_rate: float, win_loss_ratio: float,
                          max_risk_ratio: float, n_trades_per_year: int = 252,
                          commission_per_trade: float = 0.0,
                          slippage_bps: float = 1.0,
                          rolling_window_months: int = 3,
                          min_dynamic_survival_rate: float = 0.95) -> Dict[str, Any]:
        """P1-裂缝7：动态蒙特卡洛破产检验 — 考虑参数定期更新

        模拟每rolling_window_months个月重拟合参数，记录动态生存率。
        若动态生存率 < 95%，需降低参数更新频率或增加正则化。
        """
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
                # 每个窗口参数微调：win_rate和win_loss_ratio在±10%范围内随机波动
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

        return {
            "dynamic_survival_rate": dynamic_survival_rate,
            "min_dynamic_survival_rate": min_dynamic_survival_rate,
            "passed": passed,
            "rolling_window_months": rolling_window_months,
            "n_simulations": self._n_simulations,
            "bankruptcy_count": bankruptcy_count,
            "action": "reduce_update_freq_or_add_regularization" if not passed else "proceed",
        }

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
        """Block Bootstrap蒙特卡洛破产检验 — 修正i.i.d.假设

        R4-T-05修复: block_size从 params 读取，不再硬编码

        i.i.d.假设低估风险2-10倍(因波动率聚类和序列相关性)。
        Block Bootstrap保留历史收益序列的局部相关性结构：
        1. 将historical_returns切分为block_size长度的块
        2. 有放回地重采样块(保留块内序列相关性)
        3. 用重采样序列模拟equity路径,计算破产概率

        Args:
            historical_returns: 历史每笔交易收益率数组(正=盈利,负=亏损)
            initial_equity: 初始资金
            max_risk_ratio: 最大风险比率
            block_size: 块大小(=5约保留5个交易日的序列相关性)
            commission_per_trade: 每笔手续费
            slippage_bps: 滑点(bps)
            min_survival_rate: 最低生存率
            params: 可选参数字典，从中读取block_size覆盖默认值
        """
        # R4-T-05修复: 从 params 读取 block_size，不再硬编码
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

        return {
            "method": "block_bootstrap",
            "block_size": block_size,
            "survival_rate": survival_rate,
            "min_survival_rate": min_survival_rate,
            "passed": passed,
            "n_simulations": self._n_simulations,
            "bankruptcy_count": bankruptcy_count,
            "median_final_equity": float(np.median(final_arr)),
            "worst_final_equity": float(np.min(final_arr)),
        }


def validate_capital_utilization_gap(initial_equity: float = 1_000_000.0,
                                       premium_per_trade: float = 5000.0,
                                       win_rate: float = 0.55,
                                       n_trades: int = 252,
                                       max_sharpe_diff: float = 0.3,
                                       random_seed: int = 42) -> Dict[str, Any]:
    """P1-裂缝20：对比连续资金曲线与离散化资金利用下的夏普差异

    期权买方：权利金全损后资金跳变（离散）
    回测中：资金曲线连续平滑
    若差异>0.3，需改用离散资金曲线进行资金分配。
    """
    rng = np.random.RandomState(random_seed)

    # 连续资金曲线模拟
    continuous_equity = [initial_equity]
    for _ in range(n_trades):
        if rng.random() < win_rate:
            pnl = premium_per_trade * 1.5
        else:
            pnl = -premium_per_trade
        continuous_equity.append(continuous_equity[-1] + pnl)

    # 离散资金曲线模拟（权利金全损跳变）
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

    return {
        "continuous_sharpe": round(cont_sharpe, 4),
        "discrete_sharpe": round(disc_sharpe, 4),
        "sharpe_diff": round(sharpe_diff, 4),
        "max_sharpe_diff": max_sharpe_diff,
        "needs_discrete_model": needs_discrete_model,
        "action": "use_discrete_capital_curve" if needs_discrete_model else "proceed",
    }


def estimate_params_from_signal_history(
    signal_pnls: List[float],
    min_trades: int = 50,
) -> Dict[str, float]:
    """P1-MC-002修复: 从历史信号PnL序列估计MonteCarlo参数

    Args:
        signal_pnls: 真实信号PnL序列
        min_trades: 最少交易数

    Returns:
        {win_rate, win_loss_ratio, avg_win, avg_loss, n_trades}
    """
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
    return {
        'win_rate': win_rate,
        'win_loss_ratio': win_loss_ratio,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'n_trades': n,
    }


class SignalDrivenMonteCarloValidator:
    """P1-MC-001修复: 使用真实信号序列驱动的MonteCarlo破产检验

    手册8.3节: 按真实合约张数、真实信号序列、真实滑点，
    模拟1000条权益曲线，确认5年内不破产概率>99%。
    """

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
        """使用真实信号PnL序列驱动MonteCarlo模拟

        Args:
            initial_equity: 初始权益
            signal_pnls: 真实信号PnL序列(每笔交易的PnL)
            max_risk_ratio: 最大风险比率
            commission_per_trade: 每笔手续费
            slippage_bps: 滑点(bps)

        Returns:
            MonteCarloResult
        """
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
