"""
参数池适配器 (Parameter Pool Adapter) — 连接task_scheduler回测结果与策略评判引擎

将task_scheduler.py的回测输出(单个result dict)转化为
StrategyJudgmentEngine.judge()所需的输入参数，
使参数池的参数扫描结果能被策略评判系统自动评判。

用法:
  from ali2026v3_trading.策略评判.parameter_pool_adapter import judge_backtest_result

  result = run_backtest(params, bar_data)
  report = judge_backtest_result("resonance", "rb888", "2025-01~06", result)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .strategy_judgment_engine import StrategyJudgmentEngine, JudgmentReport, ECOSYSTEM_TO_JUDGMENT_TYPE_MAP, CapitalScale

logger = logging.getLogger(__name__)

_RESULT_KEY_MAP = {
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
}


def _extract_profitability(result: Dict[str, Any]) -> Dict[str, float]:
    metrics = {}
    for src_key, dst_key in _RESULT_KEY_MAP.items():
        if src_key in result and result[src_key] is not None:
            val = result[src_key]
            if isinstance(val, (int, float)):
                metrics[dst_key] = float(val)
    if "max_drawdown_pct" in metrics:
        dd = metrics["max_drawdown_pct"]
        if dd < 0:
            metrics["max_drawdown_pct"] = abs(dd) * 100.0
        elif dd < 1.0:
            metrics["max_drawdown_pct"] = dd * 100.0
    return metrics


def _extract_extreme_survival(result: Dict[str, Any]) -> Dict[str, Any]:
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
    survived = max_dd < 20.0
    recovery_hours = result.get("recovery_hours", float('inf'))
    if recovery_hours is None:
        recovery_hours = float('inf')
    return {
        "survived": survived,
        "max_drawdown_pct": max_dd,
        "recovery_hours": float(recovery_hours) if recovery_hours != float('inf') else float('inf'),
    }


def _extract_parameter_stability(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "plateau_score": float(result.get("plateau_score", 0.0) or 0.0),
        "return_skewness": float(result.get("return_skewness", 0.0) or 0.0),
        "return_kurtosis_raw": float(result.get("return_kurtosis_raw", 3.0) or 3.0),
        "max_param_sensitivity": float(result.get("max_param_sensitivity", 1.0) or 1.0),
    }


def _extract_drawdown_recovery(result: Dict[str, Any]) -> Dict[str, Any]:
    max_rh = result.get("max_recovery_hours", result.get("recovery_hours", float('inf')))
    mean_rh = result.get("mean_recovery_hours", max_rh)
    rc = int(result.get("recovery_count", 0) or 0)
    nrc = int(result.get("no_recovery_count", 0) or 0)
    max_dd = result.get("max_drawdown_pct", result.get("max_dd_pct", 100.0))
    return {
        "max_recovery_hours": float(max_rh) if max_rh is not None and max_rh != float('inf') else float('inf'),
        "mean_recovery_hours": float(mean_rh) if mean_rh is not None and mean_rh != float('inf') else float('inf'),
        "recovery_count": rc,
        "no_recovery_count": nrc,
        "max_drawdown_pct": float(max_dd) if max_dd is not None else 100.0,
    }


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
