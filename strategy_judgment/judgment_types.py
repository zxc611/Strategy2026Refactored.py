# [M3-02] 评判数据类型与配置常量
# MODULE_ID: M3-620
"""
评判数据类型 (Judgment Types) — 从 strategy_judgment_engine.py 拆分

包含:
  - JudgmentVerdict: 评判结论枚举
  - CapitalScale: 资金规模枚举
  - _JudgmentDimension: 评判维度数据类
  - JudgmentReport: 评判报告数据类
  - 工具函数: _safe_clip_score, _safe_float, _get_contract_multiplier
  - 配置常量: DEFAULT_THRESHOLDS, DEFAULT_WEIGHTS, SCORING_COEFFICIENTS, ...
"""
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import numpy as np
from datetime import datetime

from infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

from infra.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load

from .strategy_behavior_diagnosis import (
    StrategyBehaviorDiagnosis, BehaviorConsistencyScore,
    DiagnosisReport, DiagnosisSeverity,
)
from .turning_point_analysis import ResonanceTurningPointMarker
from .market_snapshot_collector import MarketSnapshotCollector, MarketSnapshot, SnapshotTrigger

if TYPE_CHECKING:
    from ..param_pool.optimization.cycle_sharpe import CycleResonanceOutput

logger = logging.getLogger(__name__)


def _safe_clip_score(score: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """NP-P1-16修复: NaN安全clip，NaN/inf→lo"""
    if not np.isfinite(score):
        return lo
    return float(np.clip(score, lo, hi))


def _safe_float(val, default: float = 0.0) -> float:
    """NP-P1-20修复: 安全float转换，None/异常→default"""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


from infra.commission_utils import CONTRACT_MULTIPLIER_MAP


def _get_contract_multiplier(instrument_id: str) -> float:
    """R19-CS-01修复: 跨系统合约乘数对齐

    从维护的乘数映射表获取合约乘数，与production/risk_service.py对齐。
    默认1.0（安全边界：不低估风险）。
    """
    prefix = instrument_id[:2] if len(instrument_id) >= 2 else instrument_id
    return CONTRACT_MULTIPLIER_MAP.get(prefix, 1.0)


class JudgmentVerdict(Enum):
    PASS = "PASS_可上线"
    CONDITIONAL_PASS = "CONDITIONAL_PASS_有条件通过"
    FAIL = "FAIL_不可上线"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE_证据不足"


class CapitalScale(Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


@dataclass(slots=True)
class _JudgmentDimension:
    name: str
    score: float
    weight: float
    passed: bool
    threshold: float
    is_blocker: bool
    detail: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class JudgmentReport:
    strategy_id: str
    strategy_type: str
    symbol: str
    backtest_period: str
    verdict: JudgmentVerdict
    overall_score: float
    dimensions: List[_JudgmentDimension]
    diagnosis_report: Optional[DiagnosisReport]
    resonance_accuracy: Dict[str, Any]
    snapshot_statistics: Dict[str, Any]
    blockers: List[str]
    conditions: List[str]
    warnings: List[str]
    recommendations: List[str]
    is_auditable: bool
    is_reproducible: bool
    judgment_timestamp: str = ""
    version: str = "v1.5-production"
    capital_scale: str = ""

    @property
    def signal_decay_consistency(self) -> float:
        if self.diagnosis_report:
            return self.diagnosis_report.overall_score.signal_decay_consistency
        return 0.0

    @property
    def position_smoothness(self) -> float:
        if self.diagnosis_report:
            return self.diagnosis_report.overall_score.position_smoothness
        return 0.0

    @property
    def greeks_rationality(self) -> float:
        if self.diagnosis_report:
            return self.diagnosis_report.overall_score.greeks_rationality
        return 0.0

    @property
    def pnl_path_quality(self) -> float:
        if self.diagnosis_report:
            return self.diagnosis_report.overall_score.pnl_path_quality
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['verdict'] = self.verdict.value
        if self.diagnosis_report is not None:
            d['diagnosis_report'] = self.diagnosis_report.to_dict()
        d['overall_score'] = float(d['overall_score'])
        d['dimensions'] = [{
            'name': dim['name'], 'score': float(dim['score']),
            'weight': float(dim['weight']), 'passed': dim['passed'],
            'threshold': float(dim['threshold']), 'is_blocker': dim['is_blocker'],
            'detail': dim['detail'],
        } for dim in d['dimensions']]
        return d

    def to_json(self, indent: int = 2) -> str:
        return json_dumps(self.to_dict(), indent=indent)

    def save(self, file_path: str) -> None:
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(self.to_json())

    def to_markdown(self) -> str:
        lines = []
        lines.append(f"# 策略评判报告 — {self.strategy_id}")
        lines.append(f"")
        lines.append(f"| 属性 | 值 |")
        lines.append(f"|------|-----|")
        lines.append(f"| 策略ID | {self.strategy_id} |")
        lines.append(f"| 策略类型 | {self.strategy_type} |")
        lines.append(f"| 品种 | {self.symbol} |")
        lines.append(f"| 回测区间 | {self.backtest_period} |")
        lines.append(f"| 评判结论 | **{self.verdict.value}** |")
        lines.append(f"| 总分 | {self.overall_score:.2f} |")
        lines.append(f"| 可审计 | {'是' if self.is_auditable else '否'} |")
        lines.append(f"| 可复现 | {'是' if self.is_reproducible else '否'} |")
        lines.append(f"| 版本 | {self.version} |")
        if self.capital_scale:
            lines.append(f"| 资金规模 | {self.capital_scale} |")
        lines.append(f"")

        if self.blockers:
            lines.append(f"## 阻塞项 (一票否决)")
            for b in self.blockers:
                lines.append(f"- **{b}**")
            lines.append(f"")

        lines.append(f"## 十二维度评判详情")
        lines.append(f"")
        lines.append(f"| 维度 | 得分 | 阈值 | 权重 | 通过 | 阻塞项 | 说明 |")
        lines.append(f"|------|------|------|------|------|--------|------|")
        for d in self.dimensions:
            passed_mark = "Y" if d.passed else "N"
            blocker_mark = "Y" if d.is_blocker else "-"
            untested_flag = " \u26a0" if d.score == 0.0 and not d.passed else ""
            lines.append(f"| {d.name}{untested_flag} | {d.score:.2f} | {d.threshold:.2f} | {d.weight:.0%} | {passed_mark} | {blocker_mark} | {d.detail} |")
        lines.append(f"")

        if self.diagnosis_report:
            dr = self.diagnosis_report
            bcs = dr.overall_score
            lines.append(f"## 行为诊断细粒度分数")
            lines.append(f"")
            lines.append(f"| 子项 | 得分 |")
            lines.append(f"|------|------|")
            lines.append(f"| 信号衰减一致性 | {bcs.signal_decay_consistency:.2f} |")
            lines.append(f"| 仓位平滑度 | {bcs.position_smoothness:.2f} |")
            lines.append(f"| Greeks合理性 | {bcs.greeks_rationality:.2f} |")
            lines.append(f"| 盈亏路径质量 | {bcs.pnl_path_quality:.2f} |")
            lines.append(f"| 极值样本量 | {bcs.sample_count} |")
            lines.append(f"| 置信度 | {bcs.confidence:.2f} |")
            lines.append(f"")

        if self.conditions:
            lines.append(f"## 条件项 (需修复)")
            for c in self.conditions:
                lines.append(f"- {c}")
            lines.append(f"")

        if self.warnings:
            lines.append(f"## 警告项")
            for w in self.warnings:
                lines.append(f"- {w}")
            lines.append(f"")

        lines.append(f"## 建议")
        for r in self.recommendations:
            lines.append(f"- {r}")

        return "\n".join(lines)

    def save_markdown(self, file_path: str) -> None:
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else ".", exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(self.to_markdown())


# E-14修复: 维度名枚举常量，替代中文硬编码
DIM_RISK_BUDGET_COMPLIANCE = "risk_budget_compliance"
DIM_EXTREME_SURVIVAL = "extreme_survival"
DIM_BEHAVIOR_CONSISTENCY = "behavior_consistency"
# 并列信号源A/B/C评判维度（最终落地方案v2.0 5对齐）
DIM_THREE_LAYER_DECISION_SOURCE = "signal_source_abc"
# 中文显示名映射(仅用于报告展示)
DIM_DISPLAY_NAMES = {
    DIM_RISK_BUDGET_COMPLIANCE: "风险预算遵守",
    DIM_EXTREME_SURVIVAL: "极端生存能力",
    DIM_BEHAVIOR_CONSISTENCY: "行为一致性",
    DIM_THREE_LAYER_DECISION_SOURCE: "并列信号源A/B/C",
}

BLOCKING_DIMENSIONS_DEFAULT = frozenset({DIM_RISK_BUDGET_COMPLIANCE, DIM_EXTREME_SURVIVAL, DIM_BEHAVIOR_CONSISTENCY})


def _validate_dimension_names():
    # C-33修复: 添加维度名称一致性验证，防止字符串匹配静默失效
    if not isinstance(BLOCKING_DIMENSIONS_DEFAULT, frozenset):
        raise TypeError(
            f"C-33: BLOCKING_DIMENSIONS_DEFAULT必须为frozenset，"
            f"当前类型: {type(BLOCKING_DIMENSIONS_DEFAULT).__name__}，"
            f"set可变导致运行时维度静默丢失风险"
        )
    known_dimensions = set(DEFAULT_THRESHOLDS.keys()) | set(DEFAULT_WEIGHTS.keys())
    for dim_name in BLOCKING_DIMENSIONS_DEFAULT:
        if dim_name not in known_dimensions:
            raise ValueError(
                f"C-33: 阻塞维度 '{dim_name}' 不在已知维度集合中，"
                f"字符串匹配将静默失效。已知维度: {sorted(known_dimensions)}"
            )


DEFAULT_THRESHOLDS = {
    "profitability": 0.50,
    "behavior_consistency": 0.70,
    "process_explainability": 0.65,
    "statistical_significance": 0.70,
    "risk_budget_compliance": 0.70,
    "extreme_survival": 0.60,
    "cross_instrument_consistency": 0.60,
    "prediction_calibration": 0.50,
    "parameter_stability": 0.50,
    "return_source_diversification": 0.50,
    "drawdown_recovery": 0.50,
    "cross_strategy_correlation": 0.70,
    "realtime_risk_score": 0.50,  # 11维度实时风险评分阈值
    "signal_source_abc": 0.60,  # 并列信号源A/B/C维度阈值（最终落地方案v2.0）
}

DEFAULT_WEIGHTS = {
    "profitability": 0.09,
    "behavior_consistency": 0.15,  # 0.18→0.15 为三层决策源让出0.03
    "process_explainability": 0.06,
    "statistical_significance": 0.06,
    "risk_budget_compliance": 0.11,
    "extreme_survival": 0.08,
    "cross_instrument_consistency": 0.04,
    "prediction_calibration": 0.05,
    "parameter_stability": 0.05,
    "return_source_diversification": 0.06,
    "drawdown_recovery": 0.05,
    "cross_strategy_correlation": 0.07,
    "realtime_risk_score": 0.08,  # 0.10→0.08 为信号源评判让出0.02
    # 并列信号源A/B/C维度权重（最终落地方案v2.0；权重总和=1.00）
    "signal_source_abc": 0.05,
}

SCORING_COEFFICIENTS = {
    # profitability
    "win_rate_full_score_at": 0.6,
    "profit_sharpe_w": 0.3, "profit_calmar_w": 0.3, "profit_win_w": 0.2, "profit_pl_w": 0.2,
    # process_explainability
    "explain_coverage_w": 0.50, "explain_chain_w": 0.25, "explain_rule_w": 0.25,
    "chain_full_at": 3.0, "rule_full_at": 10.0,
    # extreme_survival
    "extreme_dd_norm": 30.0,
    "extreme_survival_w": 0.70, "extreme_recovery_w": 0.30,
    "extreme_dd_pass_limit": 20.0,
    # prediction_calibration
    "predict_hit_w": 0.5, "predict_dir_w": 0.5,
    # parameter_stability
    "skewness_norm": 2.0, "kurtosis_norm": 5.0,
    "param_plat_w": 0.4, "param_dist_w": 0.3, "param_sens_w": 0.3,
    # return_source_diversification
    "diversify_signal_w": 0.35, "diversify_market_w": 0.35, "diversify_time_w": 0.30,
    # drawdown_recovery
    "mean_recovery_norm_ratio": 0.5,
    "dd_max_rec_w": 0.40, "dd_mean_rec_w": 0.30, "dd_rec_rate_w": 0.30,
    # cross_strategy_correlation
    "cross_normal_corr_tolerance": 0.30,
    "cross_max_div_w": 0.40, "cross_avg_div_w": 0.30, "cross_crisis_w": 0.30,
    # auditability
    "auditability_threshold_ratio": 0.5,
    "reproducibility_min_score": 0.5,
    # verdict
    "near_threshold_margin": 0.1,
    "conditional_pass_max_conditions": 2,
    # deep_validation
    "l2_non_other_ratio_threshold": 0.65,
    "l2_state_confirm_bars": 5,  # R24-P1-DF-01修复: 统一默认值为5
    "l2_logic_reversal_threshold": 1.5,
    "mcbv_simulations": 100000,
    "mcbv_initial_equity": 100000,
    "mcbv_bankruptcy_threshold": 0.5,
    "mcbv_max_drawdown_multiple": 2.0,
    "mcbv_max_risk_ratio": 0.3,
    "mcbv_daily_loss_rate": 0.02,
    "bh_significance_level": 0.05,
    "l2_accuracy_threshold": 0.6,
    "realtime_risk_dd_norm": 30.0,
    "realtime_risk_score_w": 0.50,
    "realtime_risk_dd_w": 0.50,
    "state_confirm_bars": 5,
    "contract_multiplier_default": 1.0,
    "absolute_ev_floor": -0.5,
    # signal_source_abc（最终落地方案v2.0 并列信号源A/B/C评判）
    "tl_source_consistency_w": 0.40,   # 信号源一致性权重
    "tl_calmar_w": 0.30,               # Calmar比率权重
    "tl_dmcv_pvalue_w": 0.30,          # Diebold-Mariano检验p值权重
    "tl_calmar_full_at": 1.0,           # Calmar满分阈值
    "tl_dmcv_pvalue_full_at": 0.05,    # p值满分阈值(越小越好)
    "sensitivity_analysis": {
        "db_path": "trades.db",
        "train_period": ("2024-01-01", "2024-06-30"),
        "test_period": ("2024-07-01", "2024-12-31"),
    },
}

STRATEGY_TYPE_WEIGHT_OVERRIDES = {
    "s2_resonance": {"prediction_calibration": 0.08, "behavior_consistency": 0.18, "cross_instrument_consistency": 0.04},
    "s1_hft": {"prediction_calibration": 0.04, "behavior_consistency": 0.22},
    "s3_box": {"behavior_consistency": 0.18, "risk_budget_compliance": 0.16},
    "s4_spring": {"behavior_consistency": 0.18, "risk_budget_compliance": 0.16},
    "s5_arbitrage": {"return_source_diversification": 0.03, "cross_strategy_correlation": 0.08},
    "s6_market_making": {"return_source_diversification": 0.05, "cross_strategy_correlation": 0.06},
    "s7_divergence": {"behavior_consistency": 0.16, "cross_instrument_consistency": 0.06, "prediction_calibration": 0.06},
    "other": {"behavior_consistency": 0.10, "risk_budget_compliance": 0.10},
}

STRATEGY_TYPE_THRESHOLD_OVERRIDES = {
    "s5_arbitrage": {
        "return_source_diversification": 0.30,
        "behavior_consistency": 0.60,
        "drawdown_recovery": 0.55,
        "parameter_stability": 0.55,
    },
    "s1_hft": {
        "drawdown_recovery": 0.60,
        "behavior_consistency": 0.65,
        "prediction_calibration": 0.55,
    },
    "s6_market_making": {
        "return_source_diversification": 0.35,
        "behavior_consistency": 0.60,
        "risk_budget_compliance": 0.65,
        "parameter_stability": 0.55,
    },
    "s4_spring": {
        "behavior_consistency": 0.60,
        "extreme_survival": 0.55,
        "risk_budget_compliance": 0.65,
    },
    "s2_resonance": {
        "behavior_consistency": 0.60,
        "prediction_calibration": 0.55,
        "cross_instrument_consistency": 0.55,
    },
    "s3_box": {
        "behavior_consistency": 0.60,
        "extreme_survival": 0.55,
        "risk_budget_compliance": 0.65,
    },
    "s7_divergence": {
        "behavior_consistency": 0.60,
        "cross_instrument_consistency": 0.55,
        "prediction_calibration": 0.55,
        "extreme_survival": 0.55,
    },
    "other": {
        "behavior_consistency": 0.55,
        "risk_budget_compliance": 0.60,
    },
}

ECOSYSTEM_TO_JUDGMENT_TYPE_MAP = {
    "correct_trending": "s1_hft",
    "correct_trending_defensive": "s1_hft",
    "incorrect_reversal": "s7_divergence",
    "incorrect_reversal_defensive": "s7_divergence",
    "box_extreme": "s3_box",
    "box_spring": "s4_spring",
    "s5_arbitrage": "s5_arbitrage",
    "s6_market_making": "s6_market_making",
    "divergence_reversal": "s7_divergence",
    "divergence_reversal_defensive": "s7_divergence",
    "other": "other",
}

JUDGMENT_TO_ECOSYSTEM_TYPE_MAP = {v: k for k, v in ECOSYSTEM_TO_JUDGMENT_TYPE_MAP.items()}

CAPITAL_SCALE_CONFIGS = {
    CapitalScale.SMALL: {
        "profitability_mode": "profit_loss_ratio",
        "profitability_weights": {
            "win_loss_ratio": 0.30,
            "profit_factor": 0.20,
            "sortino": 0.15,
            "calmar": 0.10,
            "recovery_efficiency": 0.15,
            "consecutive_loss_tolerance": 0.10,
        },
        "win_loss_ratio_full_score_at": 2.0,
        "profit_factor_full_score_at": 1.5,
        "sortino_full_score_at": 1.5,
        "calmar_full_score_at": 0.8,
        "recovery_efficiency_full_score_at": 2.0,
        "max_consecutive_losses_full": 3,
        "max_consecutive_losses_zero": 10,
        "drawdown_recovery_max_hours": 48.0,
        "extreme_max_recovery_hours": 72.0,
        "overall_pass_threshold": 0.65,
        "overall_conditional_threshold": 0.50,
    },
    CapitalScale.MEDIUM: {
        "profitability_mode": "balanced",
        "profitability_weights": {
            "win_loss_ratio": 0.30,
            "profit_factor": 0.10,
            "sharpe": 0.20,
            "calmar": 0.20,
            "recovery_efficiency": 0.10,
            "consecutive_loss_tolerance": 0.10,
        },
        "win_loss_ratio_full_score_at": 2.0,
        "profit_factor_full_score_at": 1.5,
        "sharpe_full_score_at": 2.0,
        "calmar_full_score_at": 2.0,
        "recovery_efficiency_full_score_at": 2.0,
        "max_consecutive_losses_full": 4,
        "max_consecutive_losses_zero": 12,
        "drawdown_recovery_max_hours": 36.0,
        "extreme_max_recovery_hours": 48.0,
        "overall_pass_threshold": 0.70,
        "overall_conditional_threshold": 0.55,
    },
    CapitalScale.LARGE: {
        "profitability_mode": "sharpe_dominant",
        "profitability_weights": {
            "sharpe": 0.30,
            "calmar": 0.30,
            "win_loss_ratio": 0.10,
            "profit_factor": 0.10,
            "recovery_efficiency": 0.10,
            "consecutive_loss_tolerance": 0.10,
        },
        "sharpe_full_score_at": 2.0,
        "calmar_full_score_at": 2.0,
        "win_loss_ratio_full_score_at": 2.0,
        "profit_factor_full_score_at": 1.5,
        "recovery_efficiency_full_score_at": 2.0,
        "max_consecutive_losses_full": 5,
        "max_consecutive_losses_zero": 15,
        "drawdown_recovery_max_hours": 24.0,
        "extreme_max_recovery_hours": 24.0,
        "overall_pass_threshold": 0.75,
        "overall_conditional_threshold": 0.60,
    },
}


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-03修复: 字符串拼接改为join，减少多次内存分配
def _build_judgment_summary_lines(dimensions: List[_JudgmentDimension]) -> str:
    """用join替代+=拼接，减少多次字符串内存分配开销"""
    lines = [
        f"  {d.name}: score={d.score:.3f}, weight={d.weight:.2f}, pass={'Y' if d.passed else 'N'}"
        for d in dimensions
    ]
    return '\n'.join(lines)


SURVIVED_THRESHOLD = DEFAULT_THRESHOLDS["extreme_survival"]

SURVIVED_THRESHOLD_DESCRIPTION = """
策略存活阈值（Strategy Survived Threshold）

定义：
  综合评分低于此阈值的策略被判定为"无法在极端市场环境下存活"，
  标记为FAIL_不可上线，禁止投入实盘交易。'
取值依据：
  1. 与overall_conditional_threshold对齐（StrategyJudgmentEngine.__init__默认值0.60）
  2. 与DEFAULT_THRESHOLDS['extreme_survival']对齐（极端存活维度阈值0.60）
  3. 参数池统一执行方案与使用手册。V7.0_工程落地版 §7.4节
"""


@dataclass
class JudgmentResult:
    """R8-2: 评判结果结构化 — 替代(warnings, blockers, conditions, validation_degraded) tuple"""
    warnings: List[str] = field(default_factory=list)
    blockers: List[str] = field(default_factory=list)
    conditions: List[str] = field(default_factory=list)
    validation_degraded: bool = False
    score: float = 0.0
    passed: bool = False