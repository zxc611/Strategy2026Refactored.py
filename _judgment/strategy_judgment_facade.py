# [M3-01] 评判Facade核心
# MODULE_ID: M3-626
"""
策略评判引擎 (Strategy Judgment Engine) — Facade层 v1.5

Facade模式：仅保留judge()入口方法，将。judge_*方法委托给已提取的Judger类。
子模块:
  - judgment_types: 数据类型与配置常量
  - judgment_profitability: ProfitabilityJudger
  - judgment_behavior: BehaviorJudger
  - judgment_risk: RiskJudger
  - judgment_calibration: CalibrationJudger
  - judgment_deep_validation: run_deep_validations
"""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import numpy as np

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

from ali2026v3_trading.infra.serialization_utils import yaml_safe_load
from ali2026v3_trading.infra._helpers import get_logger  # R9-5

from .judgment_types import (
    JudgmentVerdict,
    CapitalScale,
    _JudgmentDimension,
    JudgmentReport,
    _safe_float,
    _safe_clip_score,
    _get_contract_multiplier,
    _validate_dimension_names,
    _build_judgment_summary_lines,
    DEFAULT_THRESHOLDS,
    DEFAULT_WEIGHTS,
    SCORING_COEFFICIENTS,
    STRATEGY_TYPE_WEIGHT_OVERRIDES,
    STRATEGY_TYPE_THRESHOLD_OVERRIDES,
    ECOSYSTEM_TO_JUDGMENT_TYPE_MAP,
    JUDGMENT_TO_ECOSYSTEM_TYPE_MAP,
    CAPITAL_SCALE_CONFIGS,
    BLOCKING_DIMENSIONS_DEFAULT,
    DIM_RISK_BUDGET_COMPLIANCE,
    DIM_EXTREME_SURVIVAL,
    DIM_BEHAVIOR_CONSISTENCY,
    DIM_DISPLAY_NAMES,
    CONTRACT_MULTIPLIER_MAP,
)
from .strategy_behavior_diagnosis import DiagnosisReport, DiagnosisSeverity
from .judgment_profitability import ProfitabilityJudger
from .judgment_behavior import BehaviorJudger
from .judgment_risk import RiskJudger
from .judgment_calibration import CalibrationJudger
from .judgment_three_layer import SignalSourceABCJudger
from ._judgment_services import run_deep_validations
from ._judgment_services import chicory_eviction_score, activity_weighted_score, run_ecosystem_integrations

if TYPE_CHECKING:
    from ..param_pool.optimization.cycle_sharpe import CycleResonanceOutput

logger = get_logger(__name__)  # R9-5


class StrategyJudgmentEngine:
    """
    策略评判引擎 — Facade层 v1.6

    13维度 + 阻塞项 + 收紧阈值 + 消除默认通过 + 边际递减 + 权重归一化
    委托架构:
      _profitability_judger -> ProfitabilityJudger
      _behavior_judger -> BehaviorJudger
      _risk_judger -> RiskJudger
      _calibration_judger -> CalibrationJudger
      _three_layer_judger -> ThreeLayerDecisionSourceJudger  (v1.6新增)
    """

    SCORING_COEFFICIENTS = SCORING_COEFFICIENTS

    def __init__(
        self,
        thresholds: Optional[Dict[str, float]] = None,
        weights: Optional[Dict[str, float]] = None,
        blocking_dimensions: Optional[set] = None,
        overall_pass_threshold: float = 0.75,
        overall_conditional_threshold: float = 0.60,
        min_samples: int = 30,
        min_instruments: int = 3,
        min_signals_for_stability: int = 100,
        insufficient_evidence_missing_count: int = 3,
        max_recovery_hours: float = 24.0,
        extreme_max_recovery_hours: Optional[float] = None,
        capital_scale: Optional[CapitalScale] = None,
    ):
        self._thresholds = thresholds or DEFAULT_THRESHOLDS
        self._weights = weights or DEFAULT_WEIGHTS
        self._blocking_dimensions = blocking_dimensions if blocking_dimensions is not None else BLOCKING_DIMENSIONS_DEFAULT
        self._pass_threshold = overall_pass_threshold
        self._conditional_threshold = overall_conditional_threshold
        self._min_samples = min_samples
        self._min_instruments = min_instruments
        self._min_signals_stability = min_signals_for_stability
        self._insufficient_missing_count = insufficient_evidence_missing_count
        self._max_recovery_hours = max_recovery_hours
        self._extreme_max_recovery_hours = extreme_max_recovery_hours if extreme_max_recovery_hours is not None else max_recovery_hours
        self._capital_scale = capital_scale
        _merged_coeffs = dict(SCORING_COEFFICIENTS)
        try:
            _config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "judgment_scoring_config.yaml",
            )
            with open(_config_path, encoding="utf-8") as _f:
                _cfg = yaml_safe_load(_f)
            _yaml_coeffs = _cfg.get("judgment_scoring_coefficients", {})
            if isinstance(_yaml_coeffs, dict):
                _merged_coeffs.update(_yaml_coeffs)
        except (IOError, ValueError, KeyError) as _yaml_err:
            logging.warning("[C-34修复] judgment_scoring_config.yaml加载失败: %s, 使用硬编码默认值", _yaml_err)
        self.SCORING_COEFFICIENTS = _merged_coeffs
        self._shadow_metrics: Dict[str, Any] = {}
        self._load_shadow_jsonl()
        _validate_dimension_names()
        if capital_scale is not None:
            scale_cfg = self._resolve_scale_config(capital_scale)
            self._pass_threshold = scale_cfg["overall_pass_threshold"]
            self._conditional_threshold = scale_cfg["overall_conditional_threshold"]
            self._max_recovery_hours = scale_cfg["drawdown_recovery_max_hours"]
            self._extreme_max_recovery_hours = scale_cfg["extreme_max_recovery_hours"]

        self._profitability_judger = ProfitabilityJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            capital_scale=self._capital_scale,
            shadow_metrics=self._shadow_metrics,
        )
        self._behavior_judger = BehaviorJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            min_instruments=self._min_instruments,
        )
        self._risk_judger = RiskJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            min_samples=self._min_samples,
            max_recovery_hours=self._max_recovery_hours,
            extreme_max_recovery_hours=self._extreme_max_recovery_hours,
        )
        self._calibration_judger = CalibrationJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
        )
        self._three_layer_judger = SignalSourceABCJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
        )

    @classmethod
    def from_config(cls, config_path: Optional[str] = None,
                    params: Optional[Dict[str, Any]] = None,
                    **kwargs) -> "StrategyJudgmentEngine":
        _merged = dict(SCORING_COEFFICIENTS)
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "judgment_scoring_config.yaml",
            )
        try:
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml_safe_load(f)
            yaml_coeffs = cfg.get("judgment_scoring_coefficients", {})
            if isinstance(yaml_coeffs, dict):
                _merged.update(yaml_coeffs)
        except (IOError, ValueError, KeyError) as _yaml_err:
            logging.warning("[P1-8修复] judgment_scoring_config.yaml加载失败: %s, 使用硬编码默认值", _yaml_err)
        if params and isinstance(params, dict):
            for k, v in params.items():
                if k in _merged:
                    _merged[k] = v
        instance = cls(**kwargs)
        instance.SCORING_COEFFICIENTS = _merged
        instance._rebuild_judgers()
        return instance

    def _rebuild_judgers(self) -> None:
        self._profitability_judger = ProfitabilityJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            capital_scale=self._capital_scale,
            shadow_metrics=self._shadow_metrics,
        )
        self._behavior_judger = BehaviorJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            min_instruments=self._min_instruments,
        )
        self._risk_judger = RiskJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            min_samples=self._min_samples,
            max_recovery_hours=self._max_recovery_hours,
            extreme_max_recovery_hours=self._extreme_max_recovery_hours,
        )
        self._calibration_judger = CalibrationJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
        )
        self._three_layer_judger = SignalSourceABCJudger(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
        )

    def _load_shadow_jsonl(self) -> None:
        import json as _json
        import glob as _glob
        try:
            _log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs', 'shadow')
            _files = _glob.glob(os.path.join(_log_dir, 'shadow_*.jsonl'))
            for _f in _files:
                try:
                    with open(_f, 'r', encoding='utf-8') as _fh:
                        for _line in _fh:
                            _line = _line.strip()
                            if not _line:
                                continue
                            _rec = _json.loads(_line)
                            _key = f"{_rec.get('shadow_type', '')}_{_rec.get('strategy_group', '')}"
                            self._shadow_metrics.setdefault(_key, []).append(_rec)
                except (IOError, ValueError, KeyError) as _e:
                    logging.debug("[P2-R9-01] 读取影子JSONL失败: %s, %s", _f, _e)
            if self._shadow_metrics:
                logging.info("[P2-R9-01] 读取到%d个影子策略组数据", len(self._shadow_metrics))
        except (IOError, ValueError, KeyError, AttributeError) as e:
            logging.debug("[P2-R9-01] 影子JSONL加载失败: %s", e)

    def _resolve_scale_config(self, capital_scale: CapitalScale) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.governance.mode_engine import ModeEngine, CAPITAL_MODE_CONFIGS
            _scale_map = {
                CapitalScale.SMALL: 'small',
                CapitalScale.MEDIUM: 'medium',
                CapitalScale.LARGE: 'large',
            }
            scale_str = _scale_map.get(capital_scale, 'medium')
            mc = CAPITAL_MODE_CONFIGS.get(scale_str)
            if mc is not None:
                return {
                    "profitability_mode": mc.profitability_mode,
                    "profitability_weights": {k: v for k, v in mc.profitability_weights},
                    "win_loss_ratio_full_score_at": mc.win_loss_ratio_full_score_at,
                    "profit_factor_full_score_at": mc.profit_factor_full_score_at,
                    "recovery_efficiency_full_score_at": mc.recovery_efficiency_full_score_at,
                    "sharpe_full_score_at": mc.sharpe_full_score_at,
                    "calmar_full_score_at": mc.calmar_full_score_at,
                    "max_consecutive_losses_full": mc.max_consecutive_losses_full,
                    "max_consecutive_losses_zero": mc.max_consecutive_losses_zero,
                    "drawdown_recovery_max_hours": mc.drawdown_recovery_max_hours,
                    "extreme_max_recovery_hours": mc.extreme_max_recovery_hours,
                    "overall_pass_threshold": mc.overall_pass_threshold,
                    "overall_conditional_threshold": mc.overall_conditional_threshold,
                }
        except (ImportError, ValueError, KeyError, AttributeError) as _resolve_err:
            logging.info("[StrategyJudgmentEngine] _resolve_scale_config fallback: %s", _resolve_err)

    def judge(
        self,
        strategy_id: str,
        strategy_type: str,
        symbol: str,
        backtest_period: str,
        diagnosis_report: Optional[DiagnosisReport] = None,
        resonance_accuracy: Optional[Dict[str, Any]] = None,
        snapshot_statistics: Optional[Dict[str, Any]] = None,
        extreme_survival_result: Optional[Dict[str, Any]] = None,
        cross_instrument_results: Optional[Dict[str, DiagnosisReport]] = None,
        profitability_metrics: Optional[Dict[str, float]] = None,
        parameter_stability_result: Optional[Dict[str, Any]] = None,
        return_source_diversification: Optional[Dict[str, Any]] = None,
        drawdown_recovery_result: Optional[Dict[str, Any]] = None,
        explanation_coverage_result: Optional[Dict[str, Any]] = None,
        cross_strategy_correlation_result: Optional[Dict[str, Any]] = None,
        realtime_risk_scores: Optional[Dict[str, float]] = None,
        three_layer_decision_source_result: Optional[Dict[str, Any]] = None,
    ) -> JudgmentReport:
        if not strategy_id:
            logging.warning("[StrategyJudgmentEngine] judge()缺少strategy_id参数")
        if not strategy_type:
            logging.warning("[StrategyJudgmentEngine] judge()缺少strategy_type参数")
        if not symbol:
            logging.warning("[StrategyJudgmentEngine] judge()缺少symbol参数")
        if not backtest_period:
            logging.warning("[StrategyJudgmentEngine] judge()缺少backtest_period参数")
        if diagnosis_report is not None:
            if not hasattr(diagnosis_report, '_per_trade_returns') or not hasattr(diagnosis_report, '_p_values'):
                logging.warning("[P1-7] diagnosis_report缺少。per_trade_returns/_p_values属性，MCBV/BH将降级")
        effective_weights = dict(self._weights)
        type_overrides = STRATEGY_TYPE_WEIGHT_OVERRIDES.get(strategy_type, {})
        effective_weights.update(type_overrides)
        weight_sum = sum(effective_weights.values())
        if weight_sum > 0 and abs(weight_sum - 1.0) > 1e-6:
            effective_weights = {k: v / weight_sum for k, v in effective_weights.items()}
        _invalid_weights = {k: v for k, v in effective_weights.items() if v < 0 or v > 1}
        if _invalid_weights:
            for k, v in _invalid_weights.items():
                effective_weights[k] = max(0.0, min(1.0, v))
            warnings_list_e18 = [f"权重{k}={v:.4f}超出[0,1]范围，已截断" for k, v in _invalid_weights.items()]
            logging.warning("[E-18] %s", "; ".join(warnings_list_e18))
        effective_thresholds = dict(self._thresholds)
        threshold_overrides = STRATEGY_TYPE_THRESHOLD_OVERRIDES.get(strategy_type, {})
        effective_thresholds.update(threshold_overrides)
        original_weights = self._weights
        original_thresholds = self._thresholds
        self._weights = effective_weights
        self._thresholds = effective_thresholds

        try:
            dimensions = []
            dim1 = self._judge_profitability(profitability_metrics)
            dimensions.append(dim1)
            dim2 = self._judge_behavior_consistency(diagnosis_report)
            dimensions.append(dim2)
            dim3 = self._judge_process_explainability(diagnosis_report, explanation_coverage_result)
            dimensions.append(dim3)
            dim4 = self._judge_statistical_significance(diagnosis_report)
            dimensions.append(dim4)
            dim5 = self._judge_risk_budget_compliance(diagnosis_report)
            dimensions.append(dim5)
            dim6 = self._judge_extreme_survival(extreme_survival_result)
            dimensions.append(dim6)
            dim7 = self._judge_cross_instrument_consistency(cross_instrument_results)
            dimensions.append(dim7)
            dim8 = self._judge_prediction_calibration(resonance_accuracy)
            dimensions.append(dim8)
            dim9 = self._judge_parameter_stability(parameter_stability_result)
            dimensions.append(dim9)
            dim10 = self._judge_return_source_diversification(return_source_diversification)
            dimensions.append(dim10)
            dim11 = self._judge_drawdown_recovery(drawdown_recovery_result)
            dimensions.append(dim11)
            dim12 = self._judge_cross_strategy_correlation(cross_strategy_correlation_result)
            dimensions.append(dim12)
            dim13 = self._judge_realtime_risk_score(realtime_risk_scores)
            dimensions.append(dim13)
            dim14 = self._judge_three_layer_decision_source(three_layer_decision_source_result)
            dimensions.append(dim14)
        finally:
            self._weights = original_weights
            self._thresholds = original_thresholds

        overall = sum(d.weight * d.score for d in dimensions)
        verdict, blockers, conditions, warnings = self._determine_verdict(overall, dimensions, diagnosis_report)
        recommendations = self._generate_recommendations(verdict, dimensions, blockers, conditions)
        is_auditable = all(d.score >= d.threshold * self.SCORING_COEFFICIENTS["auditability_threshold_ratio"] for d in dimensions) and len(blockers) == 0
        is_reproducible = dim4.score >= self.SCORING_COEFFICIENTS["reproducibility_min_score"]

        overall = run_ecosystem_integrations(
            self,
            strategy_id, strategy_type, symbol, backtest_period,
            diagnosis_report, resonance_accuracy, snapshot_statistics,
            extreme_survival_result, cross_instrument_results,
            profitability_metrics, parameter_stability_result,
            return_source_diversification, drawdown_recovery_result,
            overall, dimensions, verdict, blockers, conditions, warnings,
        )

        return JudgmentReport(
            strategy_id=strategy_id, strategy_type=strategy_type, symbol=symbol,
            backtest_period=backtest_period, verdict=verdict,
            overall_score=float(np.clip(overall, 0, 1)), dimensions=dimensions,
            diagnosis_report=diagnosis_report, resonance_accuracy=resonance_accuracy or {},
            snapshot_statistics=snapshot_statistics or {}, blockers=blockers,
            conditions=conditions, warnings=warnings, recommendations=recommendations,
            is_auditable=is_auditable, is_reproducible=is_reproducible,
            judgment_timestamp=datetime.now(_CHINA_TZ).isoformat(),
            capital_scale=self._capital_scale.value if self._capital_scale else "",
        )

    # ========================================================================

    # 委托方法：将。judge_*委托给已提取的Judger子模块
    # ========================================================================

    def _judge_profitability(self, metrics):
        return self._profitability_judger.judge_profitability(metrics, threshold=self._thresholds["profitability"], weight=self._weights["profitability"])

    def _judge_behavior_consistency(self, report):
        return self._behavior_judger.judge_behavior_consistency(report, threshold=self._thresholds["behavior_consistency"], weight=self._weights["behavior_consistency"])

    def _judge_process_explainability(self, report, explanation_coverage=None):
        return self._behavior_judger.judge_process_explainability(report, explanation_coverage=explanation_coverage, threshold=self._thresholds["process_explainability"], weight=self._weights["process_explainability"])

    def _judge_statistical_significance(self, report):
        return self._risk_judger.judge_statistical_significance(report, threshold=self._thresholds["statistical_significance"], weight=self._weights["statistical_significance"])

    def _judge_risk_budget_compliance(self, report):
        return self._risk_judger.judge_risk_budget_compliance(report, threshold=self._thresholds["risk_budget_compliance"], weight=self._weights["risk_budget_compliance"])

    def _judge_extreme_survival(self, result):
        return self._risk_judger.judge_extreme_survival(result, threshold=self._thresholds["extreme_survival"], weight=self._weights["extreme_survival"])

    def _judge_cross_instrument_consistency(self, results):
        return self._behavior_judger.judge_cross_instrument_consistency(results, threshold=self._thresholds["cross_instrument_consistency"], weight=self._weights["cross_instrument_consistency"])

    def _judge_prediction_calibration(self, accuracy):
        return self._calibration_judger.judge_prediction_calibration(accuracy, threshold=self._thresholds["prediction_calibration"], weight=self._weights["prediction_calibration"])

    def _judge_parameter_stability(self, result):
        return self._calibration_judger.judge_parameter_stability(result, threshold=self._thresholds["parameter_stability"], weight=self._weights["parameter_stability"])

    def _judge_return_source_diversification(self, result):
        return self._calibration_judger.judge_return_source_diversification(result, threshold=self._thresholds["return_source_diversification"], weight=self._weights["return_source_diversification"])

    def _judge_drawdown_recovery(self, result):
        return self._risk_judger.judge_drawdown_recovery(result, threshold=self._thresholds["drawdown_recovery"], weight=self._weights["drawdown_recovery"])

    def _judge_cross_strategy_correlation(self, result):
        return self._calibration_judger.judge_cross_strategy_correlation(result, threshold=self._thresholds["cross_strategy_correlation"], weight=self._weights["cross_strategy_correlation"])

    def _judge_realtime_risk_score(self, result):
        return self._risk_judger.judge_realtime_risk_score(result, threshold=self._thresholds["realtime_risk_score"], weight=self._weights["realtime_risk_score"])

    def _judge_three_layer_decision_source(self, result):
        return self._three_layer_judger.judge_signal_source(result, threshold=self._thresholds.get("signal_source_abc", 0.60), weight=self._weights.get("signal_source_abc", 0.05))

    # ========================================================================
    # 深度验证委托
    # ========================================================================

    def _run_deep_validations(self, strategy_id, strategy_type, symbol, backtest_period,
                              diagnosis_report, resonance_accuracy, snapshot_statistics,
                              extreme_survival_result, cross_instrument_results,
                              profitability_metrics, parameter_stability_result,
                              return_source_diversification, drawdown_recovery_result):
        _w, _b, _c, _degraded = run_deep_validations(
            scoring_coefficients=self.SCORING_COEFFICIENTS,
            capital_scale=self._capital_scale,
            strategy_id=strategy_id, strategy_type=strategy_type,
            symbol=symbol, backtest_period=backtest_period,
            diagnosis_report=diagnosis_report, resonance_accuracy=resonance_accuracy,
            snapshot_statistics=snapshot_statistics,
            extreme_survival_result=extreme_survival_result,
            cross_instrument_results=cross_instrument_results,
            profitability_metrics=profitability_metrics,
            parameter_stability_result=parameter_stability_result,
            return_source_diversification=return_source_diversification,
            drawdown_recovery_result=drawdown_recovery_result,
        )
        if _degraded:
            self._validation_degraded = True
        return _w, _b, _c

    # ========================================================================
    # 判定逻辑、推荐生成（保留在Facade中）'
    # ========================================================================

    def _determine_verdict(self, overall, dimensions, report):
        blockers = []
        conditions = []
        warnings = []

        if getattr(self, '_validation_degraded', False):
            warnings.append("[R7-H-01] 深度验证降级: 部分验证器未执行，评判置信度降低，建议检查导入依赖")

        if report is not None and report.extreme_point_count < self._min_samples:
            return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                    [f"极值样本{report.extreme_point_count}<{self._min_samples}，增加回测期或降时间维度"], [])

        if report is None:
            _trade_count_dim = next((d for d in dimensions if "盈利" in d.name), None)
            if _trade_count_dim is not None and _trade_count_dim.score == 0.0 and not _trade_count_dim.passed:
                warnings.append(f"[E-17] report=None且盈利维度未通过，样本量可能不足，评判降级为INSUFFICIENT_EVIDENCE")
                return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                        [f"report缺失且盈利数据不足，无法确认样本量>={self._min_samples}"], [])

        missing_sources = sum(1 for d in dimensions if d.score == 0.0 and not d.passed)
        if missing_sources >= self._insufficient_missing_count:
            missing_names = [d.name for d in dimensions if d.score == 0.0 and not d.passed]
            return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                    [f"{missing_sources}项数据源缺失({', '.join(missing_names)})，证据不足无法评判"], [])

        _display_to_key = {v: k for k, v in DIM_DISPLAY_NAMES.items()}
        for d in dimensions:
            _dim_key = _display_to_key.get(d.name, d.name)
            is_blocker = _dim_key in self._blocking_dimensions
            d.is_blocker = is_blocker
            if is_blocker and not d.passed:
                blockers.append(f"{d.name}: 得分{d.score:.2f}<阈值{d.threshold:.2f} — 一票否决")
            elif not d.passed:
                conditions.append(f"{d.name}: 得分{d.score:.2f}<阈值{d.threshold:.2f}")
            elif d.score < d.threshold + self.SCORING_COEFFICIENTS["near_threshold_margin"]:
                warnings.append(f"{d.name}: 得分{d.score:.2f}接近阈值{d.threshold:.2f}")

        if blockers:
            return JudgmentVerdict.FAIL, blockers, conditions, warnings
        if overall >= self._pass_threshold and len(conditions) == 0:
            return JudgmentVerdict.PASS, [], [], warnings
        if overall >= self._conditional_threshold and len(conditions) <= self.SCORING_COEFFICIENTS["conditional_pass_max_conditions"]:
            return JudgmentVerdict.CONDITIONAL_PASS, [], conditions, warnings
        return JudgmentVerdict.FAIL, [], conditions + [f"总分{overall:.2f}低于门槛{self._conditional_threshold}"], warnings

    chicory_eviction_score = staticmethod(chicory_eviction_score)
    activity_weighted_score = staticmethod(activity_weighted_score)

    def _generate_recommendations(self, verdict, dimensions, blockers, conditions):
        recs = []
        if blockers:
            recs.append("策略存在阻塞项，判定为不可上线：")
            for b in blockers:
                recs.append(f"  ** {b}")
            recs.append("必须修复阻塞项后方可重新评判。")
        elif verdict == JudgmentVerdict.PASS:
            recs.append("策略通过评判，可进入实盘候选池。")
            recs.append("建议：上线前进行至少1周影子跟踪验证。")
        elif verdict == JudgmentVerdict.CONDITIONAL_PASS:
            recs.append("策略有条件通过，需修复：")
            for c in conditions:
                recs.append(f"  - {c}")
            recs.append("注意：修复策略逻辑漏洞，而非调整参数。")
        elif verdict == JudgmentVerdict.FAIL:
            failed = [d.name for d in dimensions if not d.passed]
            recs.append("策略未通过评判，判定为回测幻觉。")
            recs.append(f"失败维度: {', '.join(failed)}")
            recs.append("建议：重新审查策略核心逻辑。")
        elif verdict == JudgmentVerdict.INSUFFICIENT_EVIDENCE:
            recs.append("证据不足，无法判定。建议：")
            recs.append(f"  1. 增加回测期（至少覆盖{self._min_samples}个局部高低点）")
            recs.append(f"  2. 降时间维度（分钟级数据产生更多极值样本）")
            recs.append(f"  3. 跨品种验证（≥{self._min_instruments}品种）")
        return recs

    def batch_judge(self, strategies: List[Dict[str, Any]]) -> List[JudgmentReport]:
        reports = []
        for params in strategies:
            report = self.judge(**params)
            reports.append(report)
            logger.info("[策略评判] %s -> %s (总分=%.2f)", report.strategy_id, report.verdict.value, report.overall_score)
        return reports
