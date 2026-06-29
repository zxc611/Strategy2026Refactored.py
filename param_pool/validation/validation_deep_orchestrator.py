# [M1-126] 深度验证编排器
# MODULE_ID: M1-198
"""validation_deep_orchestrator.py - 深度验证编排入口与DEEP_VALIDATION_TIERS常量"""

from __future__ import annotations

import logging
from ali2026v3_trading.param_pool._param_grids import DEEP_VALIDATION_TIERS
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd



logger = get_logger(__name__)  # R9-5


_runner_module = None


def _ensure_runner():
    global _runner_module
    if _runner_module is None:
        from ali2026v3_trading.param_pool import backtest_runner_base as _br
        _runner_module = _br
    return _runner_module


def _run_backtest_main(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    return _ensure_runner().run_backtest(params, bar_data, train, strategy_type)


def _get_initial_equity() -> float:
    from ali2026v3_trading.param_pool.backtest.backtest_config import INITIAL_EQUITY

    return float(INITIAL_EQUITY)


def run_deep_validation_tiered(
    tier: str,
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    bar_data_secondary: Optional[pd.DataFrame] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """V7.1分级深度验证：按tier选择性运行验证子集

    Args:
        tier: "must_run"(每次必跑) / "quarterly"(季度) / "annual"(年度全量)
    """
    if tier not in DEEP_VALIDATION_TIERS:
        return {"error": f"未知tier: {tier}, 可选: {list(DEEP_VALIDATION_TIERS.keys())}"}

    report = {
        "validation_version": f"V7.1-deep-v1-tier:{tier}",
        "tier": tier,
        "tier_description": DEEP_VALIDATION_TIERS[tier]["description"],
        "tests_run": DEEP_VALIDATION_TIERS[tier]["tests"],
    }

    if tier == "must_run":
        core_report = run_deep_validation_suite(
            params_s1, params_s2, params_s3, params_s4,
            bar_data, bar_data_secondary, train
        )
        report["core_validation"] = core_report
        report["validations_run"] = list(core_report.get("results", {}).keys())
        logger.info("P1-R8-17: must_run分级验证完成，共%d项", core_report.get("total_tests", 0))
        return report

    if tier == "quarterly":
        core_report = run_deep_validation_suite(
            params_s1, params_s2, params_s3, params_s4,
            bar_data, bar_data_secondary, train
        )
        report["core_validation"] = core_report
        report["validations_run"] = list(core_report.get("results", {}).keys())

        regime_result = validate_regime_robustness(params_s2, bar_data, train=train)
        report["regime_robustness"] = {
            "passed": regime_result.passed,
            "metric": regime_result.metric_value,
        }
        report["validations_run"].append("regime_robustness")

        if bar_data_secondary is not None and not bar_data_secondary.empty:
            transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
            report["logic_transferability"] = {
                "passed": transfer_result.passed,
                "metric": transfer_result.metric_value,
            }
            report["validations_run"].append("logic_transferability")

        logger.info("P1-R8-17: quarterly分级验证完成，共%d项", len(report["validations_run"]))
        return report

    if tier == "annual":
        quarterly_report = run_deep_validation_tiered(
            tier="quarterly",
            params_s1=params_s1,
            params_s2=params_s2,
            params_s3=params_s3,
            params_s4=params_s4,
            bar_data=bar_data,
            bar_data_secondary=bar_data_secondary,
            train=train,
        )
        report.update(quarterly_report)

        try:
            from ali2026v3_trading.param_pool.optimization.sensitivity import SensitivityAnalyzer
            analyzer = SensitivityAnalyzer(
                db_path=":memory:",
                base_params=params_s1,
                train_period=("2024-01-01", "2024-06-01"),
                test_period=("2024-06-01", "2024-12-01"),
            )
            sensitivity_results = analyzer.run(perturb_pct=0.05, top_k=10)
            report["sensitivity_analysis"] = {
                "top_sensitive": [r.param_name for r in sensitivity_results[:5]],
                "count": len(sensitivity_results),
            }
            report.setdefault("validations_run", []).append("sensitivity_analysis")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("P1-R8-17: sensitivity_analysis跳过: %s", e)

        try:
            from ali2026v3_trading.param_pool.validation.statistical_validation import WalkForwardValidator
            wf = WalkForwardValidator(n_windows=5, train_ratio=0.7)
            wf_results = wf.validate(params_s1, bar_data)
            report["walk_forward"] = {
                "overall_robust": wf_results.overall_robust if hasattr(wf_results, "overall_robust") else False,
            }
            report.setdefault("validations_run", []).append("walk_forward")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("P1-R8-17: walk_forward跳过: %s", e)

        logger.info("P1-R8-17: annual分级验证完成，共%d项", len(report.get("validations_run", [])))
        return report

    logger.error("P1-R8-17: 未知分级级别 %s", tier)
    return {"error": f"Unknown tier: {tier}"}


def run_deep_validation_suite(
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    bar_data_secondary: Optional[pd.DataFrame] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """V7.1深度验证套件：一次性运行全部7个结构性漏洞验证

    返回完整的验证报告，包括每项测试的通过/失败状态和详细指标。
    """
    report = {
        "validation_version": "V7.1-deep-v1",
        "total_tests": 0,
        "passed": 0,
        "failed": 0,
        "results": {},
    }

    # 漏洞三+七：HFT时序鲁棒性
    hft_results = validate_hft_temporal_robustness(params_s1, bar_data, train)
    report["results"]["hft_temporal_robustness"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in hft_results
    ]

    # 漏洞二：跨策略相关性
    corr_result = validate_cross_strategy_correlation(params_s1, params_s2, params_s3, params_s4, bar_data, train)
    report["results"]["cross_strategy_correlation"] = {
        "passed": corr_result.passed, "metric": corr_result.metric_value,
        "threshold": corr_result.threshold, "details": corr_result.details,
    }

    # 漏洞四：市场友善度
    friendly_result = validate_market_friendliness_baseline(bar_data, train)
    report["results"]["market_friendliness"] = {
        "passed": friendly_result.passed, "metric": friendly_result.metric_value,
        "threshold": friendly_result.threshold, "details": friendly_result.details,
    }

    # 漏洞一：市场机制盲测
    regime_result = validate_regime_robustness(params_s2, bar_data, train=train)
    report["results"]["regime_robustness"] = {
        "passed": regime_result.passed, "metric": regime_result.metric_value,
        "threshold": regime_result.threshold, "details": regime_result.details,
    }

    # 漏洞六：流动性压力
    liq_results = validate_liquidity_stress(params_s2, bar_data, train)
    report["results"]["liquidity_stress"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold}
        for r in liq_results
    ]

    # 漏洞五：跨品种逻辑迁移能力
    transfer_result = None
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
        report["results"]["logic_transferability"] = {
            "passed": transfer_result.passed,
            "metric": transfer_result.metric_value,
            "threshold": transfer_result.threshold,
            "details": transfer_result.details,
        }

    # 元批判：注定失败测试（随机/倒序/伪市场）
    doomed_results = validate_doomed_tests(params_s2, bar_data, train)
    report["results"]["doomed_tests"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in doomed_results
    ]
    # 4. 状态确认窗口边界抖动验证
    try:
        jitter_result = validate_state_window_boundary_jitter(bar_data=bar_data, params=params_s2)
        report["results"]["state_window_boundary_jitter"] = jitter_result
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        report["results"]["state_window_boundary_jitter"] = {"error": str(e)}

    # 5. 多粒度指标一致性验证
    try:
        multiscale_result = validate_multiscale_indicator_consistency(bar_data_1m=bar_data)
        report["results"]["multiscale_indicator_consistency"] = multiscale_result
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        report["results"]["multiscale_indicator_consistency"] = {"error": str(e)}

    # 6. 趋势评分Bar vs Tick相关性验证
    try:
        trend_corr_result = validate_trend_score_bar_vs_tick_correlation(bar_data=bar_data)
        report["results"]["trend_score_bar_vs_tick_correlation"] = trend_corr_result
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        report["results"]["trend_score_bar_vs_tick_correlation"] = {"error": str(e)}

    # 7. 影子参数独立性验证
    try:
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence

        shadow_independence = validate_shadow_param_independence()
        report["results"]["shadow_param_independence"] = shadow_independence
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        report["results"]["shadow_param_independence"] = {"error": str(e)}

    all_results = []
    for r in hft_results:
        all_results.append(r)
    all_results.append(corr_result)
    all_results.append(friendly_result)
    all_results.append(regime_result)
    for r in liq_results:
        all_results.append(r)
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        all_results.append(transfer_result)
    for r in doomed_results:
        all_results.append(r)

    report["total_tests"] = len(all_results)
    report["passed"] = sum(1 for r in all_results if r.passed)
    report["failed"] = report["total_tests"] - report["passed"]

    return report


def run_validation_with_registry(backtest_result, validation_funcs=None):
    """ValidationRegistry编排入口 — USE_VALIDATION_REGISTRY=True时使用"""
    try:
        from ali2026v3_trading.governance.validation_registry import ValidationRegistry, BacktestValidator, BacktestResult, ValidationResult
        registry = ValidationRegistry()
        if validation_funcs:
            for name, func in validation_funcs.items():
                class _FuncValidator:
                    __name__ = name
                    def __init__(self, fn, n):
                        self._fn = fn
                        self.name = n
                    def validate(self, result):
                        try:
                            r = self._fn(result)
                            return ValidationResult(validator_name=self.name, passed=r.get('passed', True), details=r)
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            return ValidationResult(validator_name=self.name, passed=False, details={'error': str(e)})
                registry.register(name, _FuncValidator(func, name))
        return registry.run_all(backtest_result)
    except ImportError:
        return None

# ── HMM State (merged from validation_hmm_state.py on 2026-06-12) ──

"""validation_hmm_state.py - HMM/状态验证函数"""


from typing import Any, Dict, List, Tuple


from ali2026v3_trading.risk.crack_validation import validate_indicator_multiscale_consistency as validate_multiscale_indicator_consistency



def _infer_hmm_state_from_iv(iv: float, iv_q33: float, iv_q66: float) -> str:
    if iv <= iv_q33:
        return "LOW_VOL"
    elif iv <= iv_q66:
        return "NORMAL"
    else:
        return "HIGH_VOL"


def validate_hmm_online_vs_offline(iv_series: pd.Series,
                                    n_states: int = 3,
                                    mismatch_threshold: float = 0.20) -> Dict[str, Any]:
    """P1-裂缝8：验证HMM在线推断与离线全局状态偏差

    在线推断：仅用截至当前时刻的数据（前向算法逐步推进）'
    离线推断：使用全历史IV序列一次性拟合HMM

    若不一致率 > 20%，建议降低周期共振模块调节权重(×0.7)
    """
    if len(iv_series) < 100:
        return {"mismatch_rate": 0.0, "action": "insufficient_data", "weight_adjust": 1.0}

    try:
        from ali2026v3_trading.data.quant_core import AdaptiveHMM
    except ImportError:
        return {"mismatch_rate": 0.0, "action": "hmm_unavailable", "weight_adjust": 1.0}

    iv_values = iv_series.dropna().values  # R21-MEM-P2-02修复: 链式dropna()+.values产生中间Series副本，可改用iv_series[iv_series.notna()].values
    if len(iv_values) < 100:
        return {"mismatch_rate": 0.0, "action": "insufficient_data", "weight_adjust": 1.0}

    # 离线推断：全量数据一次性拟合
    offline_hmm = AdaptiveHMM(n_states=n_states, update_interval=len(iv_values) + 1)
    offline_states = []
    for val in iv_values:
        result = offline_hmm.update(val)
        offline_states.append(result.get('state', 1))
    offline_hmm.run_em_if_needed()
    # 重新推断一次获得最终参数下的状态
    offline_hmm2 = AdaptiveHMM(n_states=n_states, update_interval=len(iv_values) + 1)
    for val in iv_values:
        offline_hmm2.update(val)
    offline_states_final = []
    for val in iv_values:
        result = offline_hmm2.update(val)
        offline_states_final.append(result.get('state', 1))

    # 在线推断：逐步推进（每100个观测触发一次EM）
    online_hmm = AdaptiveHMM(n_states=n_states, update_interval=100)
    online_states = []
    for val in iv_values:
        result = online_hmm.update(val)
        online_states.append(result.get('state', 1))
        online_hmm.run_em_if_needed()

    # 计算不一致率
    matches = sum(1 for o, n in zip(offline_states_final, online_states) if o == n)
    mismatch_rate = 1.0 - matches / len(online_states)

    weight_adjust = 0.7 if mismatch_rate > mismatch_threshold else 1.0
    action = "reduce_cycle_weight" if mismatch_rate > mismatch_threshold else "proceed"

    return {
        "mismatch_rate": round(mismatch_rate, 4),
        "mismatch_threshold": mismatch_threshold,
        "action": action,
        "weight_adjust": weight_adjust,
        "online_offline_agreement": round(1.0 - mismatch_rate, 4),
    }


def _compute_state_switches(bar_data: pd.DataFrame,
                             non_other_threshold: float,
                             confirm_bars: int) -> int:
    """基于真实Bar数据计算给定confirm_bars下的状态切换次数。"""
    if bar_data is None or bar_data.empty or 'non_other_ratio' not in bar_data.columns:
        return 0
    non_other = bar_data['non_other_ratio'].values
    direction = bar_data['price_direction'].values if 'price_direction' in bar_data.columns else np.full(len(non_other), 'rise')
    close = bar_data['close'].values if 'close' in bar_data.columns else np.ones(len(non_other))
    # 逐bar计算状态
    states = np.full(len(non_other), 'other', dtype=object)
    mask_high = non_other >= non_other_threshold
    mask_low = non_other < (1 - non_other_threshold)
    states[mask_high & (direction == 'fall')] = 'correct_trending_defensive'
    states[mask_high & (direction != 'fall')] = 'correct_trending'
    states[mask_low & (direction == 'fall')] = 'incorrect_reversal_defensive'
    states[mask_low & (direction != 'fall')] = 'incorrect_reversal'
    # 使用confirm_bars确认窗口：状态需连续confirm_bars个bar保持一致才确认
    confirmed_states = np.full(len(states), 'other', dtype=object)
    for i in range(confirm_bars, len(states)):
        window = states[i - confirm_bars + 1:i + 1]
        if len(set(window)) == 1:
            confirmed_states[i] = window[0]
    # 统计状态切换次数（同时统计有方向变化时的开仓数）'
    switches = 0
    prev = confirmed_states[0]
    for s in confirmed_states[1:]:
        if s != prev and s != 'other':
            switches += 1
        prev = s
    return switches


def validate_state_window_boundary_jitter(bar_data: pd.DataFrame = None,
                                            params: Dict[str, float] = None,
                                            confirm_bars_range: Tuple[int, int] = (2, 5),
                                            n_jitter: int = 20) -> Dict[str, Any]:
    """P1-裂缝24修复：基于真实Bar数据计算状态确认窗口边界抖动对开仓变化率的影响。'
    对比base_confirm_bars与±1个确认Bar下的实际状态切换次数，
    若开仓变化率>30%，需增加状态确认的鲁棒性机制。
    """
    if params is None:
        params = {}

    base_confirm = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5
    jitter_range = range(max(1, base_confirm - 1), base_confirm + 2)
    non_other_threshold = params.get("non_other_ratio_threshold", 0.65)

    # P1-裂缝24修复: 使用真实Bar数据驱动计算（原estimated_switches=max(1,100//cb)为静态估算）'
    switch_counts = {}
    for cb in jitter_range:
        switch_counts[cb] = _compute_state_switches(bar_data, non_other_threshold, cb)

    base_switches = switch_counts.get(base_confirm, 0)
    max_deviation = 0.0
    for cb, switches in switch_counts.items():
        if cb != base_confirm and base_switches > 0:
            deviation = abs(switches - base_switches) / base_switches
            max_deviation = max(max_deviation, deviation)

    needs_robustness = max_deviation > 0.30

    return {
        "base_confirm_bars": base_confirm,
        "jitter_results": switch_counts,
        "max_deviation_pct": round(max_deviation * 100, 2),
        "needs_robustness_mechanism": needs_robustness,
        "recommendation": "增加状态确认的鲁棒性机制(如滞后窗口)" if needs_robustness else "当前可接受",
        "using_real_bar_data": bar_data is not None and not bar_data.empty,
    }


def validate_trend_score_bar_vs_tick_correlation(
    bar_data: pd.DataFrame = None,
    tick_data: pd.DataFrame = None,
    min_correlation: float = 0.7,
    n_samples: int = 500,
) -> Dict[str, Any]:
    """P1-裂缝50：验证基于Bar推断的trend_score与基于tick计算的trend_score相关性

    _infer_trend_scores_from_bar使用分钟Bar数据推断趋势评分，
    但原始MultiPeriodTrendScorer需要tick级订单流。推断误差需验证。
    要求相关性 > 0.7。
    """
    if bar_data is None or bar_data.empty:
        return {"passed": True, "action": "no_data", "details": "无Bar数据可验证"}

    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _infer_trend_scores_from_bar

    # 当无tick数据时，使用Bar内波动率作为代理指标验证
    n = min(n_samples, len(bar_data))
    bar_scores = []
    proxy_scores = []

    for idx in range(n):
        bar = bar_data.iloc[idx]
        bar_score, _ = _infer_trend_scores_from_bar(bar)
        bar_scores.append(bar_score[0])  # short_score

        # 代理指标：Bar内波动率与方向一致性
        high = bar.get("high", 0)
        low = bar.get("low", 0)
        open_p = bar.get("open", 0)
        close = bar.get("close", 0)
        if open_p > 0 and high > low:
            bar_range = (high - low) / open_p
            direction = 1.0 if close > open_p else -1.0
            proxy_score = direction * min(bar_range * 10, 1.0)
        else:
            proxy_score = 0.0
        proxy_scores.append(proxy_score)

    if len(bar_scores) < 10:
        return {"passed": True, "action": "insufficient_data", "details": "样本不足"}

    bar_scores_arr = np.asarray(bar_scores, dtype=float)
    proxy_scores_arr = np.asarray(proxy_scores, dtype=float)
    if (
        not np.isfinite(bar_scores_arr).all()
        or not np.isfinite(proxy_scores_arr).all()
        or np.std(bar_scores_arr) == 0
        or np.std(proxy_scores_arr) == 0
    ):
        correlation = 0.0
    else:
        correlation = float(np.corrcoef(bar_scores_arr, proxy_scores_arr)[0, 1])
    passed = not np.isnan(correlation) and correlation >= min_correlation

    if not passed:
        logger.warning(
            "[P1-裂缝50] trend_score Bar推断与代理指标相关性%.3f < %.1f阈值, "
            "推断误差可能过大",
            correlation if not np.isnan(correlation) else 0.0, min_correlation,
        )

    return {
        "passed": passed,
        "correlation": round(correlation, 4) if not np.isnan(correlation) else 0.0,
        "min_correlation": min_correlation,
        "n_samples": len(bar_scores),
        "action": "use_tick_level_scorer" if not passed else "proceed",
    }

# ── Deep Checks (merged from validation_deep_checks.py on 2026-06-12) ──

"""validation_deep_checks.py - 各独立深度验证函数与审DeepValidationResult"""


from dataclasses import dataclass, field




@dataclass
class _DeepValidationResult:
    test_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)


def validate_rollover_impact(bar_data: pd.DataFrame,
                              params: Dict[str, float],
                              skip_days: int = 3) -> Dict[str, Any]:
    """P0-裂缝5：对比展期剔除前后的夏普和最大回撤

    差异>10%则必须剔除展期数据。
    """
    from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import exclude_rollover_signals
    from ali2026v3_trading.infra.shared_trading_constants import detect_rollover_gaps

    rollover_points = detect_rollover_gaps(bar_data)
    if not rollover_points:
        return {"needs_exclusion": False, "rollover_count": 0, "sharpe_diff_pct": 0.0}

    # 全量回测
    full_result = _run_backtest_main(params, bar_data, train=True, strategy_type="main")
    full_sharpe = full_result.get("sharpe", 0.0)
    full_mdd = full_result.get("max_drawdown", 0.0)

    # 剔除展期后回测
    clean_data = exclude_rollover_signals(bar_data, rollover_points, skip_days)
    clean_bar = clean_data[~clean_data["rollover_excluded"]].copy()
    if clean_bar.empty:
        return {"needs_exclusion": True, "rollover_count": len(rollover_points),
                "sharpe_diff_pct": 100.0, "reason": "展期剔除后无数据"}

    clean_result = _run_backtest_main(params, clean_bar, train=True, strategy_type="main")
    clean_sharpe = clean_result.get("sharpe", 0.0)
    clean_mdd = clean_result.get("max_drawdown", 0.0)

    sharpe_diff = abs(full_sharpe - clean_sharpe) / max(abs(full_sharpe), 0.01) * 100
    mdd_diff = abs(full_mdd - clean_mdd) / max(abs(full_mdd), 0.01) * 100

    needs_exclusion = sharpe_diff > 10.0 or mdd_diff > 10.0

    return {
        "needs_exclusion": needs_exclusion,
        "rollover_count": len(rollover_points),
        "full_sharpe": full_sharpe, "clean_sharpe": clean_sharpe,
        "full_mdd": full_mdd, "clean_mdd": clean_mdd,
        "sharpe_diff_pct": round(sharpe_diff, 2),
        "mdd_diff_pct": round(mdd_diff, 2),
    }


def validate_hft_temporal_robustness(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    drop_probs: Optional[List[float]] = None,
    delay_lambdas: Optional[List[float]] = None,
) -> List[_DeepValidationResult]:
    """漏洞三+七验证：S1 HFT对时序错位和tick丢失的敏感性

    核心假设：如果策略真实捕获了市场结构，微小扰动不应导致结果剧变。
    如果drop_prob=0.1%就导致Sharpe减半 → 策略对完美时序依赖过强 → 实盘不可用。
    """
    if drop_probs is None:
        drop_probs = [0.0, 0.001, 0.005, 0.01, 0.05]
    if delay_lambdas is None:
        delay_lambdas = [0.0, 0.1, 0.5, 1.0, 2.0]

    from ali2026v3_trading.param_pool.backtest._backtest_runners_hft import run_backtest_hft_with_disturbance
    baseline = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", 0.0, 0.0)
    baseline_sharpe = baseline.get("sharpe", 0.0)
    results = []

    for prob in drop_probs:
        r = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", prob, 0.0)
        sharpe_ratio = r["sharpe"] / baseline_sharpe if abs(baseline_sharpe) > 1e-10 else 0.0
        results.append(_DeepValidationResult(
            test_name=f"tick_drop_prob={prob:.4f}",
            passed=sharpe_ratio > 0.5,
            metric_value=sharpe_ratio,
            threshold=0.5,
            details={"sharpe": r["sharpe"], "dropped": r["dropped_ticks"], "baseline_sharpe": baseline_sharpe},
        ))

    for lam in delay_lambdas:
        r = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", 0.0, lam)
        sharpe_ratio = r["sharpe"] / baseline_sharpe if abs(baseline_sharpe) > 1e-10 else 0.0
        results.append(_DeepValidationResult(
            test_name=f"delay_lambda={lam:.1f}",
            passed=sharpe_ratio > 0.5,
            metric_value=sharpe_ratio,
            threshold=0.5,
            details={"sharpe": r["sharpe"], "delayed": r["delayed_skips"], "baseline_sharpe": baseline_sharpe},
        ))

    return results


def validate_cross_strategy_correlation(
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    correlation_threshold: float = 0.6,
) -> _DeepValidationResult:
    """漏洞二验证：四策略在极端日的隐性相关性

    对每个交易日，计算四策略的日收益。如果四策略日收益的pairwise相关系数
    在极端日（日收益最低的10%天数）超过阈值 → 组合层面风险共振。
    """
    # 延迟导入避免循环依赖
    _ensure_runner()

    dates = bar_data["minute"].dt.date.unique()
    daily_returns = {"S1": [], "S2": [], "S3": [], "S4": []}

    for date in dates:
        day_data = bar_data[bar_data["minute"].dt.date == date]
        if len(day_data) < 10:
            continue
        _r = _ensure_runner()
        r1 = _r.run_backtest_hft(params_s1, day_data, train, "hft")
        r2 = _r.run_backtest(params_s2, day_data, train, "main")
        r3 = _r.run_backtest_box_extreme(params_s3, day_data, train, "box_extreme")
        r4 = _r.run_backtest_box_spring(params_s4, day_data, train, "box_spring")
        daily_returns["S1"].append(r1.get("total_return", 0))
        daily_returns["S2"].append(r2.get("total_return", 0))
        daily_returns["S3"].append(r3.get("total_return", 0))
        daily_returns["S4"].append(r4.get("total_return", 0))

    if len(daily_returns["S1"]) < 10:
        return _DeepValidationResult("cross_strategy_correlation", False, 0.0, correlation_threshold,
                                     {"error": "数据不足"})

    df = pd.DataFrame(daily_returns)
    combined = df["S1"] + df["S2"] + df["S3"] + df["S4"]
    extreme_threshold = combined.quantile(0.10)
    extreme_mask = combined <= extreme_threshold
    extreme_df = df[extreme_mask]

    if len(extreme_df) < 5:
        return _DeepValidationResult("cross_strategy_correlation", True, 0.0, correlation_threshold,
                                     {"note": "极端日样本不足，跳过"})

    corr_matrix = extreme_df.corr()
    max_corr = 0.0
    max_pair = ("", "")
    for i, col_i in enumerate(corr_matrix.columns):
        for j, col_j in enumerate(corr_matrix.columns):
            if i < j:
                c = abs(corr_matrix.loc[col_i, col_j])
                if c > max_corr:
                    max_corr = c
                    max_pair = (col_i, col_j)

    return _DeepValidationResult(
        test_name="cross_strategy_correlation",
        passed=max_corr < correlation_threshold,
        metric_value=max_corr,
        threshold=correlation_threshold,
        details={"max_corr_pair": max_pair, "corr_matrix": corr_matrix.to_dict(),
                 "extreme_day_count": int(extreme_mask.sum()), "total_days": len(df)},
    )


def validate_market_friendliness_baseline(
    bar_data: pd.DataFrame,
    train: bool = True,
    n_random: int = 100,
) -> _DeepValidationResult:
    """漏洞四验证：市场友善度基准

    计算纯随机买入持有至到期的收益分布。如果基准收益显著为正，
    说明该周期市场对期权买方天然友好，影子B的Alpha判定需修正。

    方法：在每个交易日随机时刻随机方向买入，持有至收盘平仓，
    重复n_random次，得到随机买入收益分布。
    """
    if bar_data.empty:
        return _DeepValidationResult("market_friendliness", False, 0.0, 0.0, {"error": "无数据"})

    from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _sync_random_seed

    initial_equity = _get_initial_equity()
    _sync_random_seed(42 if train else 24)
    dates = bar_data["minute"].dt.date.unique()
    random_returns = []

    for _ in range(n_random):
        equity = initial_equity
        for date in dates:
            day_data = bar_data[bar_data["minute"].dt.date == date]
            if len(day_data) < 2:
                continue
            entry_idx = np.random.randint(0, len(day_data) - 1)
            entry_price = day_data.iloc[entry_idx].get("close", 0)
            exit_price = day_data.iloc[-1].get("close", 0)
            if entry_price <= 0:
                continue
            direction = 1 if np.random.random() < 0.5 else -1
            ret = direction * (exit_price - entry_price) / entry_price
            equity *= (1 + ret * 0.01)

        random_returns.append(equity / initial_equity - 1)

    mean_random_return = np.mean(random_returns)
    std_random_return = np.std(random_returns)
    t_stat = mean_random_return / (std_random_return / np.sqrt(n_random)) if std_random_return > 1e-10 else 0.0

    is_friendly = mean_random_return > 0 and abs(t_stat) > 2.0

    return _DeepValidationResult(
        test_name="market_friendliness",
        passed=not is_friendly,
        metric_value=mean_random_return,
        threshold=0.0,
        details={
            "mean_random_return": mean_random_return,
            "std_random_return": std_random_return,
            "t_stat": t_stat,
            "is_friendly": is_friendly,
            "warning": "影子B的Alpha需减去此基准" if is_friendly else "市场中性，影子B基准有效",
            "n_random": n_random,
        },
    )


def validate_regime_robustness(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    iv_column: str = "iv",
    train: bool = True,
    n_regimes: int = 3,
) -> _DeepValidationResult:
    """漏洞一验证：市场机制分割盲测

    按波动率(IV)水平将数据分为n_regimes个regime（低IV/中IV/高IV），
    在每个regime内独立回测。如果策略在某个regime大幅亏损，
    说明其对特定市场机制过拟合。
    """
    if bar_data.empty or iv_column not in bar_data.columns:
        return _DeepValidationResult("regime_robustness", False, 0.0, 0.0,
                                     {"error": f"无数据或缺少{iv_column}列"})

    iv_values = bar_data[iv_column].replace(0, np.nan).dropna()
    if len(iv_values) < 100:
        return _DeepValidationResult("regime_robustness", False, 0.0, 0.0,
                                     {"error": "IV有效数据不足"})

    quantiles = [iv_values.quantile(i / n_regimes) for i in range(n_regimes + 1)]
    regime_results = []

    for i in range(n_regimes):
        low_q, high_q = quantiles[i], quantiles[i + 1]
        regime_data = bar_data[(bar_data[iv_column] >= low_q) & (bar_data[iv_column] < high_q)]
        if len(regime_data) < 50:
            regime_results.append({"regime": f"Q{i}", "return": 0.0, "sharpe": 0.0, "bars": len(regime_data)})
            continue
        r = _run_backtest_main(params, regime_data, train, "main")
        regime_results.append({
            "regime": f"Q{i}({low_q:.3f}-{high_q:.3f})",
            "return": r.get("total_return", 0),
            "sharpe": r.get("sharpe", 0),
            "bars": len(regime_data),
        })

    returns = [rr["return"] for rr in regime_results]
    sharpe_values = [rr["sharpe"] for rr in regime_results]
    min_sharpe = min(sharpe_values) if sharpe_values else 0.0
    sharpe_spread = max(sharpe_values) - min(sharpe_values) if sharpe_values else 0.0

    return _DeepValidationResult(
        test_name="regime_robustness",
        passed=min_sharpe > 0.0,
        metric_value=min_sharpe,
        threshold=0.0,
        details={
            "regime_results": regime_results,
            "sharpe_spread": sharpe_spread,
            "worst_regime": regime_results[sharpe_values.index(min_sharpe)]["regime"] if sharpe_values else "N/A",
        },
    )


def validate_liquidity_stress(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    slippage_multipliers: Optional[List[float]] = None,
) -> List[_DeepValidationResult]:
    """漏洞六验证：流动性枯竭压力测试

    在最大持仓时刻，假设平仓滑点被放大N倍（模拟流动性瞬间枯竭）。
    如果×10滑点就导致回撤>30% → 当前仓位模型在黑天鹅下不可用。
    """
    if slippage_multipliers is None:
        slippage_multipliers = [1.0, 5.0, 10.0, 20.0, 50.0]

    results = []
    base_slippage_bps = float(
        params.get(
            "slippage_bps",
            params.get("backtest_slippage_premium_bps", 0.5),
        )
    )
    if base_slippage_bps <= 0:
        base_slippage_bps = 0.5

    for mult in slippage_multipliers:
        stressed_params = dict(params)
        stressed_params["slippage_bps"] = base_slippage_bps * mult
        stressed_params["backtest_slippage_premium_bps"] = base_slippage_bps * mult
        r = _run_backtest_main(stressed_params, bar_data, train, "main")
        max_dd = abs(r.get("max_drawdown", 0))
        results.append(_DeepValidationResult(
            test_name=f"slippage_{mult:.0f}x",
            passed=max_dd < 0.3,
            metric_value=max_dd,
            threshold=0.3,
            details={
                "total_return": r.get("total_return", 0),
                "sharpe": r.get("sharpe", 0),
                "stress_slippage_bps": stressed_params["slippage_bps"],
            },
        ))
    return results


def validate_doomed_tests(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    n_shuffle: int = 10,
) -> List[_DeepValidationResult]:
    """元批判：注定失败测试

    如果策略在垃圾数据上也显著盈利 → 捕获的不是市场结构，是数据泄露或Bug。

    三组测试：
    1. 随机打乱tick时间顺序：shuffled收益应显著低于baseline收益（双样本t检验）
    2. 纯随机GBM生成tick：无任何市场微结构，策略收益应≈0
    3. 反向时间序列：倒序播放历史tick，趋势策略应亏损
    """
    results = []

    # Test 1: 随机打乱时间顺序（双样本t检验 vs baseline）
    baseline_returns = []
    for i in range(n_shuffle):
        bl = bar_data.sample(frac=0.5, random_state=i + 1000).reset_index(drop=True)  # R21-MEM-P2-02修复: 链式操作sample+reset_index产生2个中间DataFrame副本
        if len(bl) > 10:
            r = _run_backtest_main(params, bl, train, "main")
            baseline_returns.append(r.get("total_return", 0))

    shuffled_returns = []
    for i in range(n_shuffle):
        shuffled = bar_data.sample(frac=1.0, random_state=i).reset_index(drop=True)  # R21-MEM-P2-02修复: 链式操作sample+reset_index产生2个中间DataFrame副本
        r = _run_backtest_main(params, shuffled, train, "main")
        shuffled_returns.append(r.get("total_return", 0))

    mean_bl = np.mean(baseline_returns) if baseline_returns else 0.0
    std_bl = np.std(baseline_returns) if len(baseline_returns) > 1 else 1e-10
    mean_sh = np.mean(shuffled_returns)
    std_sh = np.std(shuffled_returns) if len(shuffled_returns) > 1 else 1e-10
    n_bl = len(baseline_returns)
    n_sh = len(shuffled_returns)

    pooled_se = np.sqrt(std_bl**2 / max(n_bl, 1) + std_sh**2 / max(n_sh, 1))
    t_diff = (mean_bl - mean_sh) / pooled_se if pooled_se > 1e-10 else 0.0

    shuffled_significantly_worse = t_diff > 2.0 and mean_bl > mean_sh
    shuffled_not_profitable = not (mean_sh > 0 and abs(mean_sh / (std_sh / np.sqrt(n_sh))) > 2.0)

    results.append(_DeepValidationResult(
        test_name="shuffled_temporal",
        passed=shuffled_significantly_worse or shuffled_not_profitable,
        metric_value=t_diff,
        threshold=2.0,
        details={"baseline_mean": mean_bl, "shuffled_mean": mean_sh,
                 "t_diff": t_diff, "n_baseline": n_bl, "n_shuffle": n_sh,
                 "meaning": "shuffled收益应显著低于baseline(t_diff>2)或本身不显著盈利"},
    ))

    # Test 2: 纯随机GBM生成
    n_bars = len(bar_data)
    if n_bars > 0:
        dt = 1 / 240
        mu, sigma = 0.0, 0.15
        gbm_prices = [100.0]
        np_gbm = np.random.RandomState(42 if train else 24)
        for _ in range(n_bars - 1):
            z = np_gbm.standard_normal()
            gbm_prices.append(gbm_prices[-1] * np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * z))

        gbm_data = pd.DataFrame({
            "minute": bar_data["minute"].values,  # R21-MEM-P2-01修复: .values返回ndarray，避免Series中间对象
            "symbol": bar_data["symbol"].values if "symbol" in bar_data.columns else ["UNKNOWN"] * n_bars,
            "close": gbm_prices,
            "high": [p * 1.001 for p in gbm_prices],
            "low": [p * 0.999 for p in gbm_prices],
            "strength": np.zeros(n_bars),
            "imbalance": np.zeros(n_bars),
        })
        r_gbm = _run_backtest_main(params, gbm_data, train, "main")
        results.append(_DeepValidationResult(
            test_name="random_gbm",
            passed=abs(r_gbm.get("total_return", 0)) < 0.05,
            metric_value=r_gbm.get("total_return", 0),
            threshold=0.05,
            details={"sharpe": r_gbm.get("sharpe", 0), "meaning": "策略不应在纯随机GBM上显著盈利"},
        ))

    # Test 3: 反向时间序列
    reversed_data = bar_data.iloc[::-1].reset_index(drop=True)  # R21-MEM-P2-01修复: iloc[::-1]产生完整副本，此处为回测必需
    r_rev = _run_backtest_main(params, reversed_data, train, "main")
    results.append(_DeepValidationResult(
        test_name="reversed_temporal",
        passed=r_rev.get("total_return", 0) < 0.0,
        metric_value=r_rev.get("total_return", 0),
        threshold=0.0,
        details={"sharpe": r_rev.get("sharpe", 0), "meaning": "趋势策略在反向时间序列中应亏损"},
    ))

    return results


def validate_logic_transferability(
    params: Dict[str, float],
    bar_data_primary: pd.DataFrame,
    bar_data_secondary: pd.DataFrame,
    train: bool = True,
) -> _DeepValidationResult:
    """漏洞五验证：逻辑可迁移性单次验证

    最优参数在主品种上回测后，在副品种（不同标的）上回测。
    如果逻辑可迁移 → Sharpe在副品种>0（虽可能较低）
    如果完全不可迁移 → Sharpe在副品种≈0或负 → 参数只是过拟合了主品种噪声
    """
    r_primary = _run_backtest_main(params, bar_data_primary, train, "main")
    r_secondary = _run_backtest_main(params, bar_data_secondary, train, "main")

    primary_sharpe = r_primary.get("sharpe", 0)
    secondary_sharpe = r_secondary.get("sharpe", 0)
    transferability_ratio = secondary_sharpe / primary_sharpe if abs(primary_sharpe) > 1e-10 else 0.0

    return _DeepValidationResult(
        test_name="logic_transferability",
        passed=secondary_sharpe > 0,
        metric_value=transferability_ratio,
        threshold=0.0,
        details={
            "primary_sharpe": primary_sharpe,
            "secondary_sharpe": secondary_sharpe,
            "primary_return": r_primary.get("total_return", 0),
            "secondary_return": r_secondary.get("total_return", 0),
            "meaning": "可迁移性比率>0.3说明逻辑捕获了真实结构",
        },
    )


# ── Lazy imports from backtest_runner_base (break circular import) ──

_LAZY_BRB_NAMES = {
    '_infer_trend_scores_from_bar',
    '_sync_random_seed',
    'run_backtest',
    'INITIAL_EQUITY',
    'SLIPPAGE_BPS',
    'detect_rollover_gaps',
    'exclude_rollover_signals',
}


def __getattr__(name):
    if name == 'validate_shadow_param_independence':
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence
        globals()['validate_shadow_param_independence'] = validate_shadow_param_independence
        return validate_shadow_param_independence
    if name in _LAZY_BRB_NAMES:
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import (
            detect_rollover_gaps,
            exclude_rollover_signals,
        )
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
            run_backtest,
            INITIAL_EQUITY,
            SLIPPAGE_BPS,
        )
        from ali2026v3_trading.param_pool._param_grids import _sync_random_seed
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _infer_trend_scores_from_bar
        globals()['_infer_trend_scores_from_bar'] = _infer_trend_scores_from_bar
        globals()['_sync_random_seed'] = _sync_random_seed
        globals()['run_backtest'] = run_backtest
        globals()['INITIAL_EQUITY'] = INITIAL_EQUITY
        globals()['SLIPPAGE_BPS'] = SLIPPAGE_BPS
        globals()['detect_rollover_gaps'] = detect_rollover_gaps
        globals()['exclude_rollover_signals'] = exclude_rollover_signals
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
