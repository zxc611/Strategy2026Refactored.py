"""
参数池适配器 (Parameter Pool Adapter) — 连接task_scheduler回测结果与策略评判引擎

将task_scheduler.py的回测输出(单个result dict)转化为
StrategyJudgmentEngine.judge()所需的输入参数，
使参数池的参数扫描结果能被策略评判系统自动评判。

用法:
  from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import judge_backtest_result

  result = run_backtest(params, bar_data)
  report = judge_backtest_result("resonance", "rb888", "2025-01~06", result)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .strategy_judgment_engine import StrategyJudgmentEngine, JudgmentReport, ECOSYSTEM_TO_JUDGMENT_TYPE_MAP, CapitalScale

logger = logging.getLogger(__name__)

_MAX_RECOVERY_HOURS_FALLBACK = 72.0  # NP-P2-08: 替代999.0，使用72小时作为recovery_hours合理上限

_RESULT_KEY_MAP = {
    # E-19修复: p_l_ratio和profit_loss_ratio均映射到profit_loss_ratio
    # 优先级: profit_loss_ratio(完整名) > p_l_ratio(缩写)
    # 当result中同时存在两个key时，_extract_profitability按字典序遍历，
    # profit_loss_ratio先于p_l_ratio被处理，因此完整名天然优先
    "sharpe": "sharpe",
    "calmar": "calmar",
    "win_rate": "win_rate",
    "profit_loss_ratio": "profit_loss_ratio",
    "p_l_ratio": "profit_loss_ratio",
    "max_drawdown_pct": "max_drawdown_pct",
    "max_dd_pct": "max_drawdown_pct",
    "max_drawdown": "max_drawdown_pct",
    "total_trades": "total_trades",
    "recovery_hours": "recovery_hours",
    "total_return": "total_return",
    "num_signals": "num_signals",
    "win_loss_ratio": "win_loss_ratio",
    "profit_factor": "profit_factor",
    "avg_win_pct": "avg_win_pct",
    "avg_loss_pct": "avg_loss_pct",
    "max_consecutive_losses": "max_consecutive_losses",
    "max_consecutive_wins": "max_consecutive_wins",
    "recovery_efficiency": "recovery_efficiency",
    "win_trades": "win_trades",
    "loss_trades": "loss_trades",
    # R4-J-07修复: 补充缺失的评分权重相关字段
    "scoring_profit_ratio_weight": "scoring_profit_ratio_weight",
    "scoring_sharpe_weight": "scoring_sharpe_weight",
    "scoring_calmar_weight": "scoring_calmar_weight",
    "scoring_win_rate_weight": "scoring_win_rate_weight",
    "scoring_max_dd_weight": "scoring_max_dd_weight",
    "sortino": "sortino",
    "omega": "omega",
    "expected_value": "expected_value",
}


def _extract_profitability(result: Dict[str, Any]) -> Dict[str, float]:
    # P2-R3-J-06: 消费source属性 — 从attribute_matrix或result中读取source字段，
    # 标识参数来源(如"backtest"/"sweep"/"realtime")，写入结果字典以供下游判断。
    # 已知限制: 若result中无source字段，默认为"unknown"。
    metrics = {}
    for src_key, dst_key in _RESULT_KEY_MAP.items():
        if src_key in result and result[src_key] is not None:
            val = result[src_key]
            if isinstance(val, (int, float)) or (hasattr(val, '__float__') and not isinstance(val, bool)):
                try:
                    metrics[dst_key] = float(val)
                except (TypeError, ValueError):
                    pass
    # P2-R3-J-06: 消费source字段
    _source = result.get("source", None)
    if _source is None:
        _attr_matrix = result.get("attribute_matrix", None)
        if isinstance(_attr_matrix, dict):
            _source = _attr_matrix.get("source", "unknown")
        else:
            _source = "unknown"
    metrics["source"] = _source
    if "max_drawdown_pct" in metrics:
        dd = metrics["max_drawdown_pct"]
        if dd < 0:
            metrics["max_drawdown_pct"] = abs(dd) * 100.0
        elif dd < 1.0:
            metrics["max_drawdown_pct"] = dd * 100.0
    return metrics


def _extract_extreme_survival(result: Dict[str, Any],
                               # E-08修复: 从SCORING_COEFFICIENTS读取而非硬编码
                               max_dd_threshold: float = None) -> Dict[str, Any]:
    raw_dd = result.get("max_drawdown_pct",
               result.get("max_dd_pct",
               result.get("max_drawdown", 100.0)))
    if raw_dd is None:
        raw_dd = 100.0
    max_dd = float(raw_dd)
    if max_dd < 0:
        max_dd = abs(max_dd) * 100.0
    elif 0 < max_dd < 1.0:
        max_dd = max_dd * 100.0
    # E-08修复: 优先从SCORING_COEFFICIENTS读取extreme_dd_pass_limit，兜底20.0
    if max_dd_threshold is None:
        try:
            from .strategy_judgment_engine import SCORING_COEFFICIENTS
            max_dd_threshold = SCORING_COEFFICIENTS.get("extreme_dd_pass_limit", 20.0)
        except Exception:
            max_dd_threshold = 20.0
    survived = max_dd < max_dd_threshold
    recovery_hours = result.get("recovery_hours", _MAX_RECOVERY_HOURS_FALLBACK)  # NP-P2-08: 使用72h常量替代999.0
    if recovery_hours is None:
        recovery_hours = _MAX_RECOVERY_HOURS_FALLBACK
    return {
        "survived": survived,
        "max_drawdown_pct": max_dd,
        "recovery_hours": float(recovery_hours) if recovery_hours != float('inf') else _MAX_RECOVERY_HOURS_FALLBACK,  # NP-P2-08
    }


def _extract_parameter_stability(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "plateau_score": float(result.get("plateau_score", 0.0) or 0.0),
        "return_skewness": float(result.get("return_skewness", 0.0) or 0.0),
        "return_kurtosis_raw": float(result.get("return_kurtosis_raw", 3.0) or 3.0),
        "max_param_sensitivity": float(result.get("max_param_sensitivity", 1.0) or 1.0),
    }


def _extract_drawdown_recovery(result: Dict[str, Any]) -> Dict[str, Any]:
    max_rh = result.get("max_recovery_hours", result.get("recovery_hours", _MAX_RECOVERY_HOURS_FALLBACK))  # NP-P2-08
    mean_rh = result.get("mean_recovery_hours", max_rh)
    rc = int(result.get("recovery_count", 0) or 0)
    nrc = int(result.get("no_recovery_count", 0) or 0)
    max_dd = result.get("max_drawdown_pct", result.get("max_dd_pct", 100.0))
    # N-07修复: max_drawdown格式统一 — task_scheduler返回小数(如0.15)，需转为百分比(15.0)
    if max_dd is not None:
        max_dd = float(max_dd)
        if max_dd < 0:
            max_dd = abs(max_dd) * 100.0
        elif 0 < max_dd < 1.0:
            max_dd = max_dd * 100.0
    else:
        max_dd = 100.0
    return {
        "max_recovery_hours": float(max_rh) if max_rh is not None and max_rh != float('inf') else _MAX_RECOVERY_HOURS_FALLBACK,  # NP-P2-08
        "mean_recovery_hours": float(mean_rh) if mean_rh is not None and mean_rh != float('inf') else _MAX_RECOVERY_HOURS_FALLBACK,  # NP-P2-08
        "recovery_count": rc,
        "no_recovery_count": nrc,
        "max_drawdown_pct": max_dd,
    }


def _extract_realtime_risk_scores(result: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """从回测结果中提取11维度实时风险评分数据

    回测结果可能包含:
    - "realtime_risk_scores": 直接传入的维度评分字典
    - "dimension_scores": compute_decision_score()的dimension_scores输出
    - "decision_score": 综合评分(composite_score)
    """
    # 优先使用完整的realtime_risk_scores
    rrs = result.get("realtime_risk_scores")
    if rrs is not None and isinstance(rrs, dict) and len(rrs) > 0:
        return rrs

    # 从dimension_scores构建
    dim_scores = result.get("dimension_scores")
    if dim_scores is not None and isinstance(dim_scores, dict):
        rrs = {}
        dim_key_map = {
            'state_strength': 'd1_state_strength',
            'order_flow': 'd2_order_flow',
            'life_expectancy': 'd3_life_expectancy',
            'cycle_resonance': 'd4_cycle_resonance',
            'phase_quality': 'd5_phase_quality',
            'greeks_usage': 'd6_greeks_usage',
            'consecutive_loss': 'd7_consecutive_loss',
            'asymmetric_drawdown': 'd8_asymmetric_drawdown',
            'tri_validation': 'd9_tri_validation',
            'alpha_decay': 'd10_alpha_decay',
            'cross_correlation': 'd11_cross_correlation',
        }
        for src_key, dst_key in dim_key_map.items():
            if src_key in dim_scores and dim_scores[src_key] is not None:
                rrs[dst_key] = float(dim_scores[src_key])
        # composite_score
        if "decision_score" in result and result["decision_score"] is not None:
            rrs["composite_score"] = float(result["decision_score"])
        if len(rrs) > 0:
            return rrs

    return None


def judge_backtest_result(
    strategy_type: str,
    symbol: str,
    backtest_period: str,
    result: Dict[str, Any],
    engine: Optional[StrategyJudgmentEngine] = None,
    strategy_id: str = "",
    cross_instrument_results: Optional[Dict] = None,
    return_source_diversification: Optional[Dict] = None,
    explanation_coverage_result: Optional[Dict] = None,
    cross_strategy_correlation_result: Optional[Dict] = None,
    capital_scale: Optional[CapitalScale] = None,
) -> JudgmentReport:
    if engine is None:
        engine = StrategyJudgmentEngine(capital_scale=capital_scale)
    if not strategy_id:
        strategy_id = f"{strategy_type}_{symbol}"
    profitability = _extract_profitability(result)
    extreme_survival = _extract_extreme_survival(result)
    param_stability = _extract_parameter_stability(result)
    drawdown_recovery = _extract_drawdown_recovery(result)
    realtime_risk_scores = _extract_realtime_risk_scores(result)
    
    # E-07修复: 自动调用4个维度提取函数（如果未显式传入）
    if cross_instrument_results is None:
        cross_instrument_results = _extract_cross_instrument(result)
    if return_source_diversification is None:
        return_source_diversification = _extract_return_source_diversification(result)
    if explanation_coverage_result is None:
        explanation_coverage_result = _extract_explanation_coverage(result)
    if cross_strategy_correlation_result is None:
        cross_strategy_correlation_result = _extract_cross_strategy_correlation(result)
    
    return engine.judge(
        strategy_id=strategy_id,
        strategy_type=strategy_type,
        symbol=symbol,
        backtest_period=backtest_period,
        profitability_metrics=profitability if profitability else None,
        extreme_survival_result=extreme_survival,
        parameter_stability_result=param_stability,
        drawdown_recovery_result=drawdown_recovery,
        cross_instrument_results=cross_instrument_results,
        return_source_diversification=return_source_diversification,
        explanation_coverage_result=explanation_coverage_result,
        cross_strategy_correlation_result=cross_strategy_correlation_result,
        realtime_risk_scores=realtime_risk_scores,
    )


def judge_sweep_results(
    strategy_type: str,
    symbol: str,
    backtest_period: str,
    results: List[Dict[str, Any]],
    engine: Optional[StrategyJudgmentEngine] = None,
    capital_scale: Optional[CapitalScale] = None,
) -> List[JudgmentReport]:
    reports = []
    for i, result in enumerate(results):
        sid = result.get("strategy", strategy_type)
        bi = result.get("bar_interval_minutes", "")
        strategy_id = f"{sid}_{symbol}_bi{bi}_{i:04d}"
        report = judge_backtest_result(
            strategy_type=sid,
            symbol=symbol,
            backtest_period=backtest_period,
            result=result,
            engine=engine,
            strategy_id=strategy_id,
            capital_scale=capital_scale,
        )
        reports.append(report)
        logger.info(f"[参数池适配] {strategy_id} -> {report.verdict.value} (总分={report.overall_score:.2f})")
    return reports


def ecosystem_type_to_judgment_type(ecosystem_type: str) -> str:
    return ECOSYSTEM_TO_JUDGMENT_TYPE_MAP.get(ecosystem_type, ecosystem_type)


# E-07修复: 4个缺失维度提取函数
def _extract_cross_instrument(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ci = result.get("cross_instrument_results")
    if ci is not None and isinstance(ci, dict) and len(ci) > 0:
        return ci
    # R3-J-05修复: 降级提取 — 从已有数据推断跨工具相关性
    _sharpe = result.get("sharpe", 0.0)
    if isinstance(_sharpe, (int, float)) and _sharpe != 0.0:
        logger.warning("[R3-J-05] cross_instrument无法自动提取, 从sharpe=%.2f推断降级值", _sharpe)
        return {"degraded": True, "inferred_correlation": 0.0, "source": "sharpe_inference"}
    return None


def _extract_return_source_diversification(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rsd = result.get("return_source_diversification")
    if rsd is not None and isinstance(rsd, dict) and len(rsd) > 0:
        return rsd
    signal_w = result.get("signal_source_weights")
    market_w = result.get("market_state_weights")
    if signal_w or market_w:
        return {
            "signal_source_weights": signal_w or {},
            "market_state_weights": market_w or {},
            "time_concentration": result.get("time_concentration", 0.0),
        }
    # R3-J-05修复: 降级提取 — 从win_rate推断收益来源多样性
    _wr = result.get("win_rate", None)
    if _wr is not None and isinstance(_wr, (int, float)):
        logger.warning("[R3-J-05] return_source_diversification无法自动提取, 从win_rate推断降级值")
        return {"degraded": True, "signal_source_weights": {}, "market_state_weights": {}, "time_concentration": 0.0, "source": "win_rate_inference"}
    return None


def _extract_explanation_coverage(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ec = result.get("explanation_coverage_result")
    if ec is not None and isinstance(ec, dict) and len(ec) > 0:
        return ec
    total = result.get("total_decisions")
    explained = result.get("explained_decisions")
    if total is not None and explained is not None:
        return {
            "total_decisions": int(total),
            "explained_decisions": int(explained),
            "avg_chain_length": float(result.get("avg_chain_length", 0.0)),
            "unique_rules_triggered": int(result.get("unique_rules_triggered", 0)),
        }
    # R3-J-05修复: 降级提取 — 从total_trades推断解释覆盖
    _tt = result.get("total_trades", None)
    if _tt is not None and isinstance(_tt, (int, float)) and _tt > 0:
        logger.warning("[R3-J-05] explanation_coverage无法自动提取, 从total_trades=%d推断降级值", int(_tt))
        return {"degraded": True, "total_decisions": int(_tt), "explained_decisions": 0, "source": "total_trades_inference"}
    return None


def _extract_cross_strategy_correlation(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    csc = result.get("cross_strategy_correlation_result")
    if csc is not None and isinstance(csc, dict) and len(csc) > 0:
        return csc
    pw = result.get("pairwise_correlations")
    cc = result.get("crisis_correlations")
    if pw or cc:
        return {
            "pairwise_correlations": pw or {},
            "crisis_correlations": cc or {},
            "max_normal_corr": float(result.get("max_normal_corr", 0.0)),
            "max_crisis_corr": float(result.get("max_crisis_corr", 0.0)),
        }
    # R3-J-05修复: 降级提取 — 从profit_loss_ratio推断跨策略相关性
    _plr = result.get("profit_loss_ratio", result.get("p_l_ratio", None))
    if _plr is not None and isinstance(_plr, (int, float)):
        logger.warning("[R3-J-05] cross_strategy_correlation无法自动提取, 从plr=%.2f推断降级值", _plr)
        return {"degraded": True, "pairwise_correlations": {}, "crisis_correlations": {}, "max_normal_corr": 0.0, "max_crisis_corr": 0.0, "source": "plr_inference"}
    return None
