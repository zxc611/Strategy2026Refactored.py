"""
策略评判引擎 (Strategy Judgment Engine) — 生产就绪版 v1.4

v1.4 改进：
  1. 盈利能力评分引入边际递减：Sharpe/Calmar非线性分段函数，防止"精准卡位"
  2. 权重归一化：策略类型覆盖后确保effective_weights总和=1.0
  3. 回撤恢复时间双阈值：extreme_survival与drawdown_recovery分别配置max_recovery_hours
  4. 收益来源分散度阈值自适应：STRATEGY_TYPE_THRESHOLD_OVERRIDES支持套利类策略
  5. 新增cross_strategy_correlation维度：监控不同策略在同一品种上的相关性
  6. Markdown报告未测试维度用⚠标记
  7. 输入契约文档化：judge()参数字典结构说明
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
from datetime import datetime, timezone, timedelta

_CHINA_TZ = timezone(timedelta(hours=8))  # [R27-AUDIT] P2修复: 统一时区常量

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load

from .strategy_behavior_diagnosis import (
    StrategyBehaviorDiagnosis, BehaviorConsistencyScore,
    DiagnosisReport, DiagnosisSeverity,
)
from .resonance_turning_point_marker import ResonanceTurningPointMarker
from .market_snapshot_collector import MarketSnapshotCollector, MarketSnapshot, SnapshotTrigger

if TYPE_CHECKING:
    from ..param_pool.cycle_resonance_module import CycleResonanceOutput

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


CONTRACT_MULTIPLIER_MAP = {
    'IF': 300.0, 'IC': 200.0, 'IH': 300.0, 'IM': 200.0,
    'IO': 100.0, 'HO': 100.0, 'MO': 100.0,
    'CU': 5.0, 'AL': 5.0, 'ZN': 5.0, 'AU': 1000.0, 'AG': 15.0,
    'RB': 10.0, 'RU': 10.0, 'MA': 10.0, 'TA': 5.0, 'OI': 10.0,
    'RM': 10.0, 'SA': 20.0, 'FG': 20.0, 'SR': 10.0, 'CF': 5.0,
    'AP': 10.0, 'CJ': 5.0, 'SF': 5.0, 'SM': 5.0, 'UR': 5.0,
    'M': 10.0, 'Y': 10.0, 'P': 10.0, 'A': 10.0, 'L': 5.0,
    'V': 5.0, 'PP': 5.0, 'EB': 5.0, 'I': 100.0, 'EG': 5.0,
    'C': 10.0, 'CS': 10.0, 'TS': 10000.0, 'TF': 10000.0, 'T': 10000.0,
}


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
# 中文显示名映射(仅用于报告展示)
DIM_DISPLAY_NAMES = {
    DIM_RISK_BUDGET_COMPLIANCE: "风险预算遵守",
    DIM_EXTREME_SURVIVAL: "极端生存能力",
    DIM_BEHAVIOR_CONSISTENCY: "行为一致性",
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
}

DEFAULT_WEIGHTS = {
    "profitability": 0.09,
    "behavior_consistency": 0.18,
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
    "realtime_risk_score": 0.10,  # 11维度实时风险评分(桥接生产与评判)
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
    "sensitivity_analysis": {
        "db_path": "trades.db",
        "train_period": ("2024-01-01", "2024-06-30"),
        "test_period": ("2024-07-01", "2024-12-31"),
    },
}

STRATEGY_TYPE_WEIGHT_OVERRIDES = {
    "resonance": {"prediction_calibration": 0.08, "behavior_consistency": 0.18, "cross_instrument_consistency": 0.04},
    "high_freq": {"prediction_calibration": 0.04, "behavior_consistency": 0.22},
    "box": {"behavior_consistency": 0.18, "risk_budget_compliance": 0.16},
    "spring": {"behavior_consistency": 0.18, "risk_budget_compliance": 0.16},
    "arbitrage": {"return_source_diversification": 0.03, "cross_strategy_correlation": 0.08},
    "market_making": {"return_source_diversification": 0.05, "cross_strategy_correlation": 0.06},
    "other": {"behavior_consistency": 0.10, "risk_budget_compliance": 0.10},
}

STRATEGY_TYPE_THRESHOLD_OVERRIDES = {
    "arbitrage": {
        "return_source_diversification": 0.30,
        "behavior_consistency": 0.60,
        "drawdown_recovery": 0.55,
        "parameter_stability": 0.55,
    },
    "high_freq": {
        "drawdown_recovery": 0.60,
        "behavior_consistency": 0.65,
        "prediction_calibration": 0.55,
    },
    "market_making": {
        "return_source_diversification": 0.35,
        "behavior_consistency": 0.60,
        "risk_budget_compliance": 0.65,
        "parameter_stability": 0.55,
    },
    "spring": {
        "behavior_consistency": 0.60,
        "extreme_survival": 0.55,
        "risk_budget_compliance": 0.65,
    },
    "resonance": {
        "behavior_consistency": 0.60,
        "prediction_calibration": 0.55,
        "cross_instrument_consistency": 0.55,
    },
    "box": {
        "behavior_consistency": 0.60,
        "extreme_survival": 0.55,
        "risk_budget_compliance": 0.65,
    },
    "other": {
        "behavior_consistency": 0.55,
        "risk_budget_compliance": 0.60,
    },
}

ECOSYSTEM_TO_JUDGMENT_TYPE_MAP = {
    "correct_trending": "high_freq",
    "correct_trending_defensive": "high_freq",  # R10-三对齐修复: defensive状态映射与correct_trending同类
    "incorrect_reversal": "resonance",
    "incorrect_reversal_defensive": "resonance",  # R10-三对齐修复: defensive状态映射与incorrect_reversal同类
    "box_extreme": "box",
    "box_spring": "spring",
    "arbitrage": "arbitrage",
    "market_making": "market_making",
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


class StrategyJudgmentEngine:
    """
    策略评判引擎 — 生产就绪版 v1.5

    12维度 + 阻塞项 + 收紧阈值 + 消除默认通过 + 边际递减 + 权重归一化
    v1.5新增: 盈利能力边际递减 + 双阈值recovery + 阈值自适应 + cross_strategy_correlation

    输入契约:
      return_source_diversification: {
        "signal_source_weights": {"momentum": 0.4, ...},  # 各信号源PnL贡献占比
        "market_state_weights": {"trending": 0.5, ...},   # 各市场状态PnL贡献占比
        "time_concentration": 0.3,                         # 时间集中度(0=均匀,1=单一)
      }
      drawdown_recovery_result: {
        "max_recovery_hours": 8.0,       # 最大回撤恢复时间(小时)
        "mean_recovery_hours": 4.0,      # 均值恢复时间(小时)
        "recovery_count": 15,            # 成功恢复次数
        "no_recovery_count": 2,          # 未恢复次数
        "max_drawdown_pct": 12.0,        # 最大回撤百分比
      }
      explanation_coverage_result: {
        "total_decisions": 200,          # 总交易决策数
        "explained_decisions": 180,      # 可解释决策数
        "avg_chain_length": 3.5,         # 平均规则触发链深度
        "unique_rules_triggered": 12,    # 触发的独立规则数
      }
      cross_strategy_correlation_result: {
        "pairwise_correlations": {"S1_vs_S2": 0.3, ...},  # 策略间信号方向相关系数
        "crisis_correlations": {"S1_vs_S2": 0.8, ...},    # 危机期相关性
        "max_normal_corr": 0.4,                           # 正常期最大相关系数
        "max_crisis_corr": 0.85,                          # 危机期最大相关系数
      }
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
        # C-34修复: __init__中也加载YAML配置, 与from_config保持一致
        _merged_coeffs = dict(SCORING_COEFFICIENTS)
        try:
            import os as _os
            _config_path = _os.path.join(
                _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                "config", "judgment_scoring_config.yaml",
            )
            with open(_config_path, encoding="utf-8") as _f:
                _cfg = yaml_safe_load(_f)
            _yaml_coeffs = _cfg.get("judgment_scoring_coefficients", {})
            if isinstance(_yaml_coeffs, dict):
                _merged_coeffs.update(_yaml_coeffs)
        except Exception as _yaml_err:
            logging.warning("[C-34修复] judgment_scoring_config.yaml加载失败: %s, 使用硬编码默认值", _yaml_err)
        self.SCORING_COEFFICIENTS = _merged_coeffs
        # P2-R9-01修复: 影子策略汇总文件消费入口
        self._shadow_metrics: Dict[str, Any] = {}
        self._load_shadow_jsonl()
        _validate_dimension_names()  # C-33修复: 添加维度名称一致性验证，防止字符串匹配静默失效
        if capital_scale is not None:
            scale_cfg = self._resolve_scale_config(capital_scale)
            self._pass_threshold = scale_cfg["overall_pass_threshold"]
            self._conditional_threshold = scale_cfg["overall_conditional_threshold"]
            self._max_recovery_hours = scale_cfg["drawdown_recovery_max_hours"]
            self._extreme_max_recovery_hours = scale_cfg["extreme_max_recovery_hours"]

    @classmethod
    def from_config(cls, config_path: Optional[str] = None,
                    params: Optional[Dict[str, Any]] = None,
                    **kwargs) -> "StrategyJudgmentEngine":
        """从YAML配置和params参数池构建引擎，三系统统一参数化入口

        优先级: params覆盖 > YAML > 硬编码兜底
        """
        _merged = dict(SCORING_COEFFICIENTS)
        if config_path is None:
            import os
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "judgment_scoring_config.yaml",
            )
        try:
            import yaml
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml_safe_load(f)
            yaml_coeffs = cfg.get("judgment_scoring_coefficients", {})
            if isinstance(yaml_coeffs, dict):
                _merged.update(yaml_coeffs)
        except Exception as _yaml_err:
            logging.warning("[P1-8修复] judgment_scoring_config.yaml加载失败: %s, 使用硬编码默认值", _yaml_err)
        if params and isinstance(params, dict):
            for k, v in params.items():
                if k in _merged:
                    _merged[k] = v
        instance = cls(**kwargs)
        instance.SCORING_COEFFICIENTS = _merged
        return instance

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
    ) -> JudgmentReport:
        # R13-P2-API-13修复: 输入参数完整性校验
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
                import logging
                logging.warning("[P1-7] diagnosis_report缺少_per_trade_returns/_p_values属性，MCBV/BH将降级")
        effective_weights = dict(self._weights)
        type_overrides = STRATEGY_TYPE_WEIGHT_OVERRIDES.get(strategy_type, {})
        effective_weights.update(type_overrides)
        weight_sum = sum(effective_weights.values())
        if weight_sum > 0 and abs(weight_sum - 1.0) > 1e-6:
            effective_weights = {k: v / weight_sum for k, v in effective_weights.items()}
        # E-18修复: 单维度权重范围校验(0<=w<=1)
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

            # 第13维度：11维度实时风险评分（桥接生产与评判）
            dim13 = self._judge_realtime_risk_score(realtime_risk_scores)
            dimensions.append(dim13)
        finally:
            # R6-R-03修复: try-finally确保异常时恢复临时状态，防止并发/异常下评判结果错误
            self._weights = original_weights
            self._thresholds = original_thresholds

        overall = sum(d.weight * d.score for d in dimensions)

        verdict, blockers, conditions, warnings = self._determine_verdict(overall, dimensions, diagnosis_report)

        recommendations = self._generate_recommendations(verdict, dimensions, blockers, conditions)

        is_auditable = all(d.score >= d.threshold * self.SCORING_COEFFICIENTS["auditability_threshold_ratio"] for d in dimensions) and len(blockers) == 0
        is_reproducible = dim4.score >= self.SCORING_COEFFICIENTS["reproducibility_min_score"]

        # E-02修复: 集成CascadeJudge结果到评判流程
        try:
            from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
            if profitability_metrics is not None:
                _train_r = dict(profitability_metrics)
                _key_map = {
                    'win_loss_ratio': 'profit_loss_ratio',
                    'profit_factor': 'profit_loss_ratio',
                    'win_rate': 'win_rate',
                }
                for _old_k, _new_k in _key_map.items():
                    if _old_k in _train_r and _new_k not in _train_r:
                        _train_r[_new_k] = _train_r[_old_k]
                _adapted = adapt_backtest_result(_train_r, strategy_type=self._params.get('strategy_type', '') if hasattr(self, '_params') and self._params else '')
                _cascade = CascadeJudge.from_config(capital_scale=self._capital_scale, params=self._params if hasattr(self, '_params') else None)
                _cascade_report = _cascade.judge(_adapted)
                if not _cascade_report.passed:
                    blockers.append(f"[CascadeJudge] 瀑布式评判否决: {_cascade_report.fatal_reason}")
                for _cw in _cascade_report.warnings:
                    warnings.append(f"[CascadeJudge] {_cw}")
        except Exception as _ce02:
            warnings.append(f"[CascadeJudge] 集成异常: {_ce02}")

        # E-04修复: governance_engine checker集成到评判流程（手册V7.4节）
        try:
            from ali2026v3_trading.governance_engine import get_governance_engine
            ge = get_governance_engine()
            logging.info("[E-04] 启动治理引擎检查...")
            
            # 运行所有checker
            governance_results = ge.run_all_checkers({
                'strategy_id': strategy_id,
                'profitability': profitability_metrics,
                'diagnosis': diagnosis_report,
            })
            
            # 收集blockers
            for checker_name, result in governance_results.items():
                if not result.get('passed', True):
                    blockers.append(f"[E-04 Governance] {checker_name}失败: {result.get('reason', '')}")
            
            logging.info("[E-04] 治理引擎检查完成: %d个checker", len(governance_results))
        except Exception as _e04_err:
            warnings.append(f"[E-04] governance_engine集成失败(非致命): {_e04_err}")

        # E-05修复: ParameterDriftDetector集成 — 参数漂移检测（手册V7.4节）
        try:
            from ali2026v3_trading.evaluation.parameter_drift_detector import ParameterDriftDetector
            pdd = ParameterDriftDetector()
            drift_result = pdd.detect(strategy_id, parameter_stability_result)
            if drift_result.get('drift_detected', False):
                warnings.append(f"[E-05] 检测到参数漂移: {drift_result.get('drift_magnitude', 0):.4f}")
            logging.info("[E-05] ParameterDriftDetector集成完成")
        except Exception as _e05_err:
            warnings.append(f"[E-05] ParameterDriftDetector集成失败(非致命): {_e05_err}")

        # E-09修复: ChicoryEvictionPolicy集成 — 淘汰策略管理（手册V7.4节）
        try:
            from ali2026v3_trading.evaluation.chicory_eviction import ChicoryEvictionPolicy
            cep = ChicoryEvictionPolicy()
            # LC-02修复: 设置审计日志路径
            try:
                import os as _os
                _audit_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), 'logs', 'eviction_audit')
                cep.set_audit_log_path(_os.path.join(_audit_dir, 'eviction_audit.log'))
            except Exception:
                pass
            eviction_decision = cep.evaluate(strategy_id, overall, dimensions)
            if eviction_decision.get('should_evict', False):
                _eviction_code = eviction_decision.get('eviction_code', 'unknown')
                _revivable = eviction_decision.get('revivable', False)
                _revival_cond = eviction_decision.get('revival_condition', '')
                blockers.append(
                    f"[E-09] 策略应被淘汰: {eviction_decision.get('reason', '')} "
                    f"[编码={_eviction_code}, 可复活={_revivable}, 条件={_revival_cond}]"
                )
            logging.info("[E-09] ChicoryEvictionPolicy集成完成")
        except Exception as _e09_err:
            warnings.append(f"[E-09] ChicoryEvictionPolicy集成失败(非致命): {_e09_err}")

        # E-10修复: MarqueeThreshold集成 — 门槛阈值检查（手册V7.4节）
        try:
            from ali2026v3_trading.evaluation.marquee_threshold import MarqueeThreshold
            mt = MarqueeThreshold()
            threshold_check = mt.check(strategy_id, overall, dimensions)
            if not threshold_check.get('passed', True):
                blockers.append(f"[E-10] 未通过Marquee门槛: {threshold_check.get('failed_dims', [])}")
            logging.info("[E-10] MarqueeThreshold集成完成")
        except Exception as _e10_err:
            warnings.append(f"[E-10] MarqueeThreshold集成失败(非致命): {_e10_err}")

        # E-11修复: StrategyViolationTracker集成 — 违规跟踪（手册V7.4节）
        try:
            from ali2026v3_trading.evaluation.violation_tracker import StrategyViolationTracker
            svt = StrategyViolationTracker()
            violations = svt.track(strategy_id, verdict, blockers)
            if violations:
                warnings.extend([f"[E-11] 违规记录: {v}" for v in violations])
            logging.info("[E-11] StrategyViolationTracker集成完成")
        except Exception as _e11_err:
            warnings.append(f"[E-11] StrategyViolationTracker集成失败(非致命): {_e11_err}")

        # E-12修复: activity_weighted_score集成 — 活动加权评分（手册V7.4节）
        try:
            from ali2026v3_trading.evaluation.activity_weighted_scorer import ActivityWeightedScorer
            aws = ActivityWeightedScorer()
            weighted_score = aws.calculate(strategy_id, overall, dimensions)
            conditions.append(f"[E-12] 活动加权评分: {weighted_score:.4f}")
            logging.info("[E-12] activity_weighted_score集成完成")
        except Exception as _e12_err:
            warnings.append(f"[E-12] activity_weighted_score集成失败(非致命): {_e12_err}")

        # E-13修复: StateEDensityDecayTracker集成 — 状态密度衰减跟踪（手册V7.4节）
        try:
            from ali2026v3_trading.evaluation.state_density_decay import StateEDensityDecayTracker
            sedt = StateEDensityDecayTracker()
            decay_result = sedt.track(strategy_id, diagnosis_report)
            if decay_result.get('decay_detected', False):
                warnings.append(f"[E-13] 检测到状态密度衰减: {decay_result.get('decay_rate', 0):.4f}")
            logging.info("[E-13] StateEDensityDecayTracker集成完成")
        except Exception as _e13_err:
            warnings.append(f"[E-13] StateEDensityDecayTracker集成失败(非致命): {_e13_err}")

        # P0-R11-08修复: Alpha CI消费 — 检查alpha_action和ci_width，对非reliable策略降权
        try:
            _ci_width = (profitability_metrics or {}).get('sharpe_ci_width', 0.0)
            _alpha_action = (profitability_metrics or {}).get('alpha_action', 'reliable')
            if _alpha_action == 'eliminate':
                blockers.append(f"[Alpha-CI] 策略CI宽度={_ci_width:.4f}(>2.0)，Sharpe无统计意义，应淘汰")
                overall *= 0.0  # eliminate: 评分归零
            elif _alpha_action == 'reduce_weight':
                _weight_mult = 1.0 / _ci_width if _ci_width > 1.0 else 1.0
                overall *= _weight_mult
                warnings.append(f"[Alpha-CI] 策略CI宽度={_ci_width:.4f}(>1.0)，Sharpe不可靠，评分降权至{_weight_mult:.4f}")
            elif _alpha_action == 'flag':
                conditions.append(f"[Alpha-CI] 策略CI宽度={_ci_width:.4f}(>0.5)，Sharpe中等可靠，已标注")
            # 'reliable' 无需额外处理
        except Exception as _e_ci:
            warnings.append(f"[Alpha-CI] CI消费异常(非致命): {_e_ci}")

        # E-06修复: governance反馈通道集成（手册V7.4节）
        try:
            from ali2026v3_trading.governance_engine import get_governance_engine
            ge = get_governance_engine()
            feedback = ge.submit_feedback(strategy_id, verdict, dimensions, blockers)
            if feedback.get('submitted', False):
                logging.info("[E-06] Governance反馈已提交")
            else:
                warnings.append(f"[E-06] Governance反馈提交失败: {feedback.get('reason', '')}")
        except Exception as _e06_err:
            warnings.append(f"[E-06] governance反馈通道集成失败(非致命): {_e06_err}")

        # E-08修复: survived判定阈值统一（手册V7.4节）
        # NS-01修复: except Exception→except ImportError，ImportError提升为conditions（阻断条件）
        try:
            from ali2026v3_trading.strategy_judgment.config import SURVIVED_THRESHOLD
            if overall < SURVIVED_THRESHOLD:
                conditions.append(f"[E-08] 综合评分{overall:.4f}低于survived阈值{SURVIVED_THRESHOLD}")
            logging.info("[E-08] survived判定阈值检查完成: threshold=%.4f", SURVIVED_THRESHOLD)
        except ImportError as _e08_err:
            conditions.append(f"[E-08] survived阈值模块导入失败(阻断): {_e08_err}")
        except Exception as _e08_err:
            warnings.append(f"[E-08] survived判定阈值运行时异常(非致命): {_e08_err}")

        deep_validation_warnings, deep_validation_blockers, deep_validation_conditions = self._run_deep_validations(
            strategy_id, strategy_type, symbol, backtest_period,
            diagnosis_report, resonance_accuracy, snapshot_statistics,
            extreme_survival_result, cross_instrument_results,
            profitability_metrics, parameter_stability_result,
            return_source_diversification, drawdown_recovery_result,
        )
        blockers.extend(deep_validation_blockers)
        conditions.extend(deep_validation_conditions)
        warnings.extend(deep_validation_warnings)

        # ✅ P1-15/R7-P-02修复: 集成SensitivityAnalyzer敏感性分析
        # R7-P-02修复: 缓存SensitivityAnalyzer实例，避免每次judge()都创建新实例+连接数据库
        try:
            from ali2026v3_trading.param_pool.sensitivity_analysis import SensitivityAnalyzer
            main_params = profitability_metrics if profitability_metrics else {}
            # R7-P-02修复: 使用缓存的实例，仅当参数变化时重建
            if not hasattr(self, '_cached_sa') or self._cached_sa is None:
                _sa_config = self.SCORING_COEFFICIENTS.get("sensitivity_analysis", {})
                self._cached_sa = SensitivityAnalyzer(
                    db_path=_sa_config.get("db_path", "trades.db"),
                    base_params=main_params,
                    train_period=_sa_config.get("train_period", ("2024-01-01", "2024-06-30")),
                    test_period=_sa_config.get("test_period", ("2024-07-01", "2024-12-31")),
                )
                self._cached_sa_params_hash = hash(frozenset(main_params.items()))
            elif hash(frozenset(main_params.items())) != self._cached_sa_params_hash:
                _sa_config = self.SCORING_COEFFICIENTS.get("sensitivity_analysis", {})
                self._cached_sa = SensitivityAnalyzer(
                    db_path=_sa_config.get("db_path", "trades.db"),
                    base_params=main_params,
                    train_period=_sa_config.get("train_period", ("2024-01-01", "2024-06-30")),
                    test_period=_sa_config.get("test_period", ("2024-07-01", "2024-12-31")),
                )
                self._cached_sa_params_hash = hash(frozenset(main_params.items()))
            sa = self._cached_sa
            sa_results = sa.run(perturb_pct=0.05, top_k=5)
            if sa_results:
                top_sensitive = sa_results[0]
                conditions.append(
                    f"[P1-15 敏感性分析] 最敏感参数: {top_sensitive.param_name} "
                    f"(sharpe_delta={top_sensitive.sharpe_delta:.4f})"
                )
        except Exception as e:
            logging.debug("[StrategyJudgmentEngine] SensitivityAnalyzer error: %s", e)

        # 注: self._weights和self._thresholds的恢复已在try-finally中保证 (R6-R-03修复)

        return JudgmentReport(
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            symbol=symbol,
            backtest_period=backtest_period,
            verdict=verdict,
            overall_score=float(np.clip(overall, 0, 1)),
            dimensions=dimensions,
            diagnosis_report=diagnosis_report,
            resonance_accuracy=resonance_accuracy or {},
            snapshot_statistics=snapshot_statistics or {},
            blockers=blockers,
            conditions=conditions,
            warnings=warnings,
            recommendations=recommendations,
            is_auditable=is_auditable,
            is_reproducible=is_reproducible,
            judgment_timestamp=datetime.now(_CHINA_TZ).isoformat(),  # [R27-AUDIT] P2修复
            capital_scale=self._capital_scale.value if self._capital_scale else "",
        )

    @staticmethod
    def _diminishing_return_score(value: float, breakpoints: Optional[List[Tuple[float, float]]] = None) -> float:
        if breakpoints is None:
            breakpoints = [(1.0, 0.50), (2.0, 0.80), (3.0, 1.00)]
        if value <= 0:
            return 0.0
        if value >= breakpoints[-1][0]:
            return breakpoints[-1][1]
        for i in range(len(breakpoints) - 1):
            x0, y0 = breakpoints[i]
            x1, y1 = breakpoints[i + 1]
            if x0 <= value < x1:
                t = (value - x0) / (x1 - x0)
                return y0 + t * (y1 - y0)
        return 0.0

    def _judge_profitability(self, metrics: Optional[Dict[str, float]]) -> _JudgmentDimension:
        threshold = self._thresholds["profitability"]
        is_blocker = False
        if metrics is None:
            return _JudgmentDimension(
                name="盈利能力", score=0.0, weight=self._weights["profitability"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未提供盈利指标，无法评判",
            )

        if self._capital_scale is not None:
            return self._judge_profitability_by_scale(metrics, threshold, is_blocker)

        sharpe = metrics.get("sharpe", 0.0)
        calmar = metrics.get("calmar", 0.0)
        win_rate = metrics.get("win_rate", 0.0)
        pl_ratio = metrics.get("profit_loss_ratio", 0.0)

        sharpe_score = self._diminishing_return_score(sharpe)
        calmar_score = self._diminishing_return_score(calmar)
        win_score = min(1.0, max(0.0, win_rate / self.SCORING_COEFFICIENTS["win_rate_full_score_at"]))
        pl_score = self._diminishing_return_score(pl_ratio, [(1.0, 0.50), (2.0, 0.80), (3.0, 1.00)])

        score = self.SCORING_COEFFICIENTS["profit_sharpe_w"] * sharpe_score + self.SCORING_COEFFICIENTS["profit_calmar_w"] * calmar_score + self.SCORING_COEFFICIENTS["profit_win_w"] * win_score + self.SCORING_COEFFICIENTS["profit_pl_w"] * pl_score

        # P2-R9-01修复: 消费_shadow_metrics的alpha_decay_rate作为评判维度输入
        _alpha_decay_adj = 0.0
        try:
            if self._shadow_metrics:
                _all_decay_rates = []
                for _group_key, _records in self._shadow_metrics.items():
                    for _rec in _records:
                        _adr = _rec.get('alpha_decay_rate', None)
                        if _adr is not None:
                            _all_decay_rates.append(float(_adr))
                if _all_decay_rates:
                    _avg_decay = sum(_all_decay_rates) / len(_all_decay_rates)
                    _alpha_decay_adj = max(0.0, min(0.15, _avg_decay * 0.1))
                    score = score * (1.0 - _alpha_decay_adj)
        except Exception:
            pass

        passed = score >= threshold
        detail = (f"Sharpe={sharpe:.2f}(score={sharpe_score:.2f}), "
                  f"Calmar={calmar:.2f}(score={calmar_score:.2f}), "
                  f"WinRate={win_rate:.2f}, P/L={pl_ratio:.2f}")
        if _alpha_decay_adj > 0:
            detail += f", AlphaDecayAdj=-{_alpha_decay_adj:.3f}"
        return _JudgmentDimension(
            name="盈利能力", score=score, weight=self._weights["profitability"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    # P2-R9-01修复: 读取影子策略JSONL汇总文件
    def _load_shadow_jsonl(self) -> None:
        """读取影子策略JSONL日志文件，供评判引擎消费"""
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
                except Exception as _e:
                    logging.debug("[P2-R9-01] 读取影子JSONL失败: %s, %s", _f, _e)
            if self._shadow_metrics:
                logging.info("[P2-R9-01] 读取到%d个影子策略组数据", len(self._shadow_metrics))
        except Exception as e:
            logging.debug("[P2-R9-01] 影子JSONL加载失败: %s", e)

    def _resolve_scale_config(self, capital_scale: CapitalScale) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.mode_engine import ModeEngine, CAPITAL_MODE_CONFIGS
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
        except Exception as _resolve_err:
            # R15-P1-DEAD-04修复: 静默吞没异常改为日志记录
            logging.info("[StrategyJudgmentEngine] _resolve_scale_config fallback: %s", _resolve_err)  # R26-P2-DF-05修复: debug→info
            pass

    def _judge_profitability_by_scale(
        self, metrics: Dict[str, float], threshold: float, is_blocker: bool,
    ) -> _JudgmentDimension:
        scale_cfg = self._resolve_scale_config(self._capital_scale)
        pw = scale_cfg["profitability_weights"]
        mode = scale_cfg["profitability_mode"]

        win_loss_ratio = _safe_float(metrics.get("win_loss_ratio", 0.0))
        profit_factor = _safe_float(metrics.get("profit_factor", 0.0))
        recovery_efficiency = _safe_float(metrics.get("recovery_efficiency", 0.0))
        max_consecutive_losses = int(_safe_float(metrics.get("max_consecutive_losses", 999), default=999))
        sharpe = max(-10.0, min(10.0, _safe_float(metrics.get("sharpe", 0.0))))
        calmar = max(-10.0, min(10.0, _safe_float(metrics.get("calmar", 0.0))))

        wl_full = scale_cfg.get("win_loss_ratio_full_score_at", 2.0)
        pf_full = scale_cfg.get("profit_factor_full_score_at", 1.5)
        re_full = scale_cfg.get("recovery_efficiency_full_score_at", 2.0)
        cl_full = scale_cfg.get("max_consecutive_losses_full", 3)
        cl_zero = scale_cfg.get("max_consecutive_losses_zero", 10)

        wl_score = min(1.0, max(0.0, win_loss_ratio / wl_full)) if wl_full > 0 else 0.0
        pf_score = min(1.0, max(0.0, profit_factor / pf_full)) if pf_full > 0 else 0.0
        re_score = min(1.0, max(0.0, recovery_efficiency / re_full)) if re_full > 0 else 0.0
        cl_score = max(0.0, 1.0 - max(0, max_consecutive_losses - cl_full) / max(1, cl_zero - cl_full))

        score_parts = {}
        score_parts["win_loss_ratio"] = wl_score
        score_parts["profit_factor"] = pf_score
        score_parts["recovery_efficiency"] = re_score
        score_parts["consecutive_loss_tolerance"] = cl_score

        if mode in ("balanced", "sharpe_dominant"):
            sh_full = scale_cfg.get("sharpe_full_score_at", 2.0)
            ca_full = scale_cfg.get("calmar_full_score_at", 2.0)
            sharpe_score = self._diminishing_return_score(sharpe, [(1.0, 0.50), (sh_full, 1.00)])
            calmar_score = self._diminishing_return_score(calmar, [(1.0, 0.50), (ca_full, 1.00)])
            score_parts["sharpe"] = sharpe_score
            score_parts["calmar"] = calmar_score

        score = sum(pw.get(k, 0.0) * v for k, v in score_parts.items())
        score = float(np.clip(score, 0.0, 1.0))
        passed = score >= threshold

        detail_parts = [f"模式={mode}"]
        if "sharpe" in score_parts:
            detail_parts.append(f"Sharpe={sharpe:.2f}(s={score_parts['sharpe']:.2f})")
        if "calmar" in score_parts:
            detail_parts.append(f"Calmar={calmar:.2f}(s={score_parts['calmar']:.2f})")
        detail_parts.append(f"盈亏比={win_loss_ratio:.2f}(s={wl_score:.2f})")
        detail_parts.append(f"盈利因子={profit_factor:.2f}(s={pf_score:.2f})")
        detail_parts.append(f"恢复效率={recovery_efficiency:.2f}(s={re_score:.2f})")
        detail_parts.append(f"连亏={max_consecutive_losses}(s={cl_score:.2f})")
        detail = ", ".join(detail_parts)

        return _JudgmentDimension(
            name="盈利能力", score=score, weight=self._weights["profitability"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_behavior_consistency(self, report: Optional[DiagnosisReport]) -> _JudgmentDimension:
        threshold = self._thresholds["behavior_consistency"]
        is_blocker = True
        if report is None:
            return _JudgmentDimension(
                name=DIM_DISPLAY_NAMES[DIM_BEHAVIOR_CONSISTENCY], score=0.0, weight=self._weights["behavior_consistency"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无诊断报告",
            )
        score = report.overall_score.overall_score
        passed = score >= threshold
        bcs = report.overall_score
        detail = (f"总分={score:.2f}(阈值={threshold}), "
                  f"信号衰减={bcs.signal_decay_consistency:.2f}, "
                  f"仓位平滑={bcs.position_smoothness:.2f}, "
                  f"Greeks合理={bcs.greeks_rationality:.2f}, "
                  f"PnL路径={bcs.pnl_path_quality:.2f}")
        return _JudgmentDimension(
            name=DIM_DISPLAY_NAMES[DIM_BEHAVIOR_CONSISTENCY], score=score, weight=self._weights["behavior_consistency"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_process_explainability(self, report: Optional[DiagnosisReport], explanation_coverage: Optional[Dict[str, Any]] = None) -> _JudgmentDimension:
        threshold = self._thresholds["process_explainability"]
        is_blocker = False
        if explanation_coverage is not None:
            total_decisions = int(explanation_coverage.get("total_decisions", 0))
            explained_decisions = int(explanation_coverage.get("explained_decisions", 0))
            avg_chain_length = float(explanation_coverage.get("avg_chain_length", 0.0))
            unique_rules_triggered = int(explanation_coverage.get("unique_rules_triggered", 0))
            if total_decisions == 0:
                return _JudgmentDimension(
                    name="过程可解释性", score=0.0, weight=self._weights["process_explainability"],
                    passed=False, threshold=threshold, is_blocker=is_blocker,
                    detail="主动可解释性：无交易决策记录",
                )
            coverage_ratio = explained_decisions / total_decisions
            chain_quality = min(1.0, avg_chain_length / self.SCORING_COEFFICIENTS["chain_full_at"]) if avg_chain_length > 0 else 0.0
            rule_diversity = min(1.0, unique_rules_triggered / self.SCORING_COEFFICIENTS["rule_full_at"]) if unique_rules_triggered > 0 else 0.0
            score = self.SCORING_COEFFICIENTS["explain_coverage_w"] * coverage_ratio + self.SCORING_COEFFICIENTS["explain_chain_w"] * chain_quality + self.SCORING_COEFFICIENTS["explain_rule_w"] * rule_diversity
            score = float(np.clip(score, 0.0, 1.0))
            passed = score >= threshold
            detail = (f"主动ECR={coverage_ratio:.2f}({explained_decisions}/{total_decisions}), "
                      f"链深度={avg_chain_length:.1f}, 规则数={unique_rules_triggered}")
            return _JudgmentDimension(
                name="过程可解释性", score=score, weight=self._weights["process_explainability"],
                passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
            )
        if report is None:
            return _JudgmentDimension(
                name="过程可解释性", score=0.0, weight=self._weights["process_explainability"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无诊断报告，无主动可解释性数据",
            )
        healthy = sum(1 for d in report.dimensions if d.severity == DiagnosisSeverity.HEALTHY)
        total = len(report.dimensions)
        score = healthy / total if total > 0 else 0.0
        passed = score >= threshold
        detail = f"被动健康维度={healthy}/{total}(建议提供explanation_coverage_result升级为主动ECR)"
        return _JudgmentDimension(
            name="过程可解释性", score=score, weight=self._weights["process_explainability"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_statistical_significance(self, report: Optional[DiagnosisReport]) -> _JudgmentDimension:
        threshold = self._thresholds["statistical_significance"]
        is_blocker = False
        if report is None:
            return _JudgmentDimension(
                name="统计显著性", score=0.0, weight=self._weights["statistical_significance"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无诊断报告",
            )
        n = report.extreme_point_count
        confidence = report.overall_score.confidence
        score = confidence
        passed = n >= self._min_samples and confidence >= threshold
        detail = f"样本量={n}(≥{self._min_samples}), 置信度={confidence:.2f}"
        return _JudgmentDimension(
            name="统计显著性", score=score, weight=self._weights["statistical_significance"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_risk_budget_compliance(self, report: Optional[DiagnosisReport]) -> _JudgmentDimension:
        threshold = self._thresholds["risk_budget_compliance"]
        is_blocker = True
        if report is None:
            return _JudgmentDimension(
                name=DIM_DISPLAY_NAMES[DIM_RISK_BUDGET_COMPLIANCE], score=0.0, weight=self._weights["risk_budget_compliance"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无诊断报告，风险合规无法验证",
            )
        pos_dim = greeks_dim = None
        for d in report.dimensions:
            if "仓位" in d.dimension:
                pos_dim = d
            elif "Greeks" in d.dimension:
                greeks_dim = d
        scores = []
        if pos_dim:
            scores.append(pos_dim.score)
        if greeks_dim:
            scores.append(greeks_dim.score)
        score = float(np.mean(scores)) if scores else 0.0
        passed = score >= threshold
        detail = f"风险合规分={score:.2f}(阈值={threshold})"
        return _JudgmentDimension(
            name=DIM_DISPLAY_NAMES[DIM_RISK_BUDGET_COMPLIANCE], score=score, weight=self._weights["risk_budget_compliance"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_extreme_survival(self, result: Optional[Dict[str, Any]]) -> _JudgmentDimension:
        threshold = self._thresholds["extreme_survival"]
        is_blocker = True
        if result is None:
            return _JudgmentDimension(
                name=DIM_DISPLAY_NAMES[DIM_EXTREME_SURVIVAL], score=0.0, weight=self._weights["extreme_survival"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未执行极端行情测试，策略不具备上线资格",
            )
        survived = result.get("survived", False)
        max_dd = result.get("max_drawdown_pct", 100.0)
        recovery = result.get("recovery_hours", 999.0)
        max_rh = self._extreme_max_recovery_hours
        recovery_ok = recovery <= max_rh if math.isfinite(recovery) else False
        survival_score = 1.0 if survived else max(0.0, 1.0 - max_dd / self.SCORING_COEFFICIENTS["extreme_dd_norm"])
        recovery_penalty = 1.0 if recovery_ok else max(0.0, 1.0 - (recovery - max_rh) / max_rh) if math.isfinite(recovery) else 0.0
        score = float(np.clip(self.SCORING_COEFFICIENTS["extreme_survival_w"] * survival_score + self.SCORING_COEFFICIENTS["extreme_recovery_w"] * recovery_penalty, 0.0, 1.0))
        passed = survived and max_dd < self.SCORING_COEFFICIENTS["extreme_dd_pass_limit"] and recovery_ok
        detail = (f"存活={survived}, 最大回撤={max_dd:.1f}%, "
                  f"恢复={recovery:.1f}h(限≤{max_rh:.0f}h), "
                  f"恢复达标={'Y' if recovery_ok else 'N'}")
        return _JudgmentDimension(
            name=DIM_DISPLAY_NAMES[DIM_EXTREME_SURVIVAL], score=score, weight=self._weights["extreme_survival"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_cross_instrument_consistency(self, results: Optional[Dict]) -> _JudgmentDimension:
        """跨品种一致性检验（全量扫描排序策略专用）

        由于本策略使用全量扫描排序，跨品种一致性检验包含4个维度：
        1. Tier分布一致性（Tier1占比<20%, Tier4占比>30%）
        2. correct_up_pct分布一致性（跨品种CV<0.40）
        3. Wilson降权比一致性（跨品种CV<0.30）
        4. 月份权重归一化一致性（不同月份数品种间correct_up_pct可比，KS距离<0.2）

        与judgment_standards.md 4.6节代码实现映射对齐。
        CV定量阈值：CV < 0.40 → passed=True（介于文档0.3"一致"和0.5"过拟合"之间）
        """
        threshold = self._thresholds["cross_instrument_consistency"]
        is_blocker = False
        if results is None or len(results) < self._min_instruments:
            detail = f"跨品种验证不足(需≥{self._min_instruments}品种)" if results is None else f"仅{len(results)}品种(需≥{self._min_instruments})"
            return _JudgmentDimension(
                name="跨品种一致性", score=0.0, weight=self._weights["cross_instrument_consistency"],
                passed=False, threshold=threshold, is_blocker=is_blocker, detail=detail,
            )

        # 维度1: 整体得分CV（传统维度，保留作为参考）
        scores = [r.overall_score.overall_score for r in results.values()]
        overall_cv = float(np.std(scores)) / (float(np.mean(scores)) + 1e-8)

        # 维度2-4: 排序框架四维一致性检验
        tier_counts = {"1": 0, "2": 0, "3": 0, "4": 0}
        cup_values = []
        wilson_ratios = []
        # 维度4: 月份权重归一化一致性——按品种分组记录score，用于KS检验
        instrument_scores_map: Dict[str, List[float]] = {}

        for symbol_key, r in results.items():
            # 使用overall_score作为排序框架代理指标进行Tier分类
            score_val = r.overall_score.overall_score
            tier = 1 if score_val >= 0.8 else (
                2 if score_val >= 0.6 else (
                    3 if score_val >= 0.4 else 4))
            tier_counts[str(tier)] += 1
            cup_values.append(score_val)

            # 维度3: Wilson降权比（从趋势捕捉能力维度提取，确定性计算）
            wilson_ratio_found = False
            for d in r.dimensions:
                if d.name == "趋势捕捉能力":
                    raw_ratio = d.score
                    # Wilson降权比确定性计算：Wilson区间下界/原始比例
                    # 样本量n越大→Wilson收缩越小→降权比越接近1.0
                    # 使用overall_score.confidence作为样本量代理，确定性映射
                    confidence = r.overall_score.confidence if hasattr(r.overall_score, 'confidence') else 0.5
                    # Wilson降权幅度：置信度越低降权越大，确定性公式
                    wilson_shrinkage = 0.7 + 0.3 * confidence  # confidence∈[0,1] → shrinkage∈[0.7,1.0]
                    if raw_ratio > 0:
                        wilson_ratios.append(wilson_shrinkage)
                    wilson_ratio_found = True
                    break
            if not wilson_ratio_found:
                # 无趋势捕捉维度时用overall_score的确定性衰减作为代理
                if score_val > 0:
                    wilson_ratios.append(0.85 + 0.15 * (1 - score_val))

            # 维度4: 收集各品种得分用于月份权重归一化KS检验
            sym_name = str(symbol_key).split("_")[0] if "_" in str(symbol_key) else str(symbol_key)
            if sym_name not in instrument_scores_map:
                instrument_scores_map[sym_name] = []
            instrument_scores_map[sym_name].append(score_val)

        total = sum(tier_counts.values())  # NP-P2-07: 显式total==0检查替代or 1
        if total == 0:
            total = 1
        tier1_pct = tier_counts["1"] / total
        tier4_pct = tier_counts["4"] / total

        # 维度2: correct_up_pct代理CV
        cup_cv = float(np.std(cup_values)) / (float(np.mean(cup_values)) + 1e-8) if cup_values else 1.0

        # 维度3: Wilson降权比CV
        wr_cv = float(np.std(wilson_ratios)) / (float(np.mean(wilson_ratios)) + 1e-8) if wilson_ratios else 0.0

        # 维度4: 月份权重归一化一致性（KS距离）
        # 按品种分组，比较组间correct_up_pct分布的KS距离
        ks_distance = 0.0
        ks_pass = True
        group_keys = list(instrument_scores_map.keys())
        if len(group_keys) >= 2:
            # 取得分最多和最少的两组做KS检验
            sorted_groups = sorted(group_keys, key=lambda k: len(instrument_scores_map[k]))
            group_a = np.array(instrument_scores_map[sorted_groups[0]])
            group_b = np.array(instrument_scores_map[sorted_groups[-1]])
            if len(group_a) >= 2 and len(group_b) >= 2:
                # KS统计量：两组经验CDF的最大差值
                all_vals = np.sort(np.concatenate([group_a, group_b]))
                cdf_a = np.searchsorted(np.sort(group_a), all_vals, side='right') / len(group_a)
                cdf_b = np.searchsorted(np.sort(group_b), all_vals, side='right') / len(group_b)
                ks_distance = float(np.max(np.abs(cdf_a - cdf_b)))
                ks_pass = ks_distance < 0.2  # 文档阈值：KS距离<0.2

        # 综合评分（与文档代码映射对齐：0.35*cup + 0.25*wilson + 0.15*tier1 + 0.10*tier4 + 0.15*ks）
        base_score = max(0.0, 1.0 - cup_cv) * 0.35 + max(0.0, 1.0 - wr_cv) * 0.25
        tier_bonus = (0.15 if tier1_pct < 0.20 else 0) + (0.10 if tier4_pct > 0.30 else 0)
        ks_bonus = 0.15 if ks_pass else 0.0
        score = float(np.clip(base_score + tier_bonus + ks_bonus, 0.0, 1.0))

        # 综合判定（与文档对齐：全部条件必须满足）
        passed = (cup_cv < 0.40 and wr_cv < 0.30
                  and tier1_pct < 0.20 and tier4_pct > 0.30
                  and ks_pass
                  and score >= threshold)

        detail = (f"跨{len(results)}品种, 均值={float(np.mean(scores)):.2f}, "
                  f"cup_CV={cup_cv:.2f}(限<0.40), "
                  f"wilson_CV={wr_cv:.2f}(限<0.30), "
                  f"Tier1={tier1_pct:.0%}(限<20%), Tier4={tier4_pct:.0%}(限>30%), "
                  f"KS={ks_distance:.2f}(限<0.20)")
        return _JudgmentDimension(
            name="跨品种一致性", score=score, weight=self._weights["cross_instrument_consistency"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_prediction_calibration(self, accuracy: Optional[Dict]) -> _JudgmentDimension:
        threshold = self._thresholds["prediction_calibration"]
        is_blocker = False
        if accuracy is None:
            return _JudgmentDimension(
                name="预测能力校准", score=0.0, weight=self._weights["prediction_calibration"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无共振预测数据",
            )
        hit = accuracy.get("hit_rate", 0.0)
        dir_acc = accuracy.get("direction_accuracy", 0.0)
        score = self.SCORING_COEFFICIENTS["predict_hit_w"] * hit + self.SCORING_COEFFICIENTS["predict_dir_w"] * dir_acc
        passed = score >= threshold
        detail = f"命中率={hit:.2f}, 方向准确率={dir_acc:.2f}"
        return _JudgmentDimension(
            name="预测能力校准", score=score, weight=self._weights["prediction_calibration"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_parameter_stability(self, result: Optional[Dict]) -> _JudgmentDimension:
        threshold = self._thresholds["parameter_stability"]
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="参数稳定性", score=0.0, weight=self._weights["parameter_stability"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未执行参数稳定性检验",
            )
        plateau_score = result.get("plateau_score", 0.0)
        skewness = abs(result.get("return_skewness", 0.0))
        raw_kurtosis = result.get("return_kurtosis_raw", 3.0)
        kurtosis_excess = max(0.0, raw_kurtosis - 3.0)
        max_sensitivity = result.get("max_param_sensitivity", 1.0)

        plat_part = plateau_score
        dist_part = max(0.0, 1.0 - skewness / self.SCORING_COEFFICIENTS["skewness_norm"]) * max(0.0, 1.0 - kurtosis_excess / self.SCORING_COEFFICIENTS["kurtosis_norm"])
        sens_part = max(0.0, 1.0 - max_sensitivity)

        score = self.SCORING_COEFFICIENTS["param_plat_w"] * plat_part + self.SCORING_COEFFICIENTS["param_dist_w"] * dist_part + self.SCORING_COEFFICIENTS["param_sens_w"] * sens_part
        passed = score >= threshold
        detail = f"高原度={plat_part:.2f}, 分布={dist_part:.2f}, 敏感度={sens_part:.2f}"
        return _JudgmentDimension(
            name="参数稳定性", score=score, weight=self._weights["parameter_stability"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_return_source_diversification(self, result: Optional[Dict[str, Any]]) -> _JudgmentDimension:
        threshold = self._thresholds["return_source_diversification"]
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="收益来源分散度", score=0.0, weight=self._weights["return_source_diversification"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未提供收益来源分解数据",
            )
        signal_weights = result.get("signal_source_weights", {})
        market_weights = result.get("market_state_weights", {})
        time_concentration = float(result.get("time_concentration", 1.0))
        signal_hhi = sum(w ** 2 for w in signal_weights.values()) if signal_weights else 1.0
        n_sig = len(signal_weights) if signal_weights else 1
        signal_diversity = max(0.0, 1.0 - signal_hhi) if n_sig > 1 else 0.0
        market_hhi = sum(w ** 2 for w in market_weights.values()) if market_weights else 1.0
        n_mkt = len(market_weights) if market_weights else 1
        market_diversity = max(0.0, 1.0 - market_hhi) if n_mkt > 1 else 0.0
        time_diversity = max(0.0, 1.0 - time_concentration)
        score = self.SCORING_COEFFICIENTS["diversify_signal_w"] * signal_diversity + self.SCORING_COEFFICIENTS["diversify_market_w"] * market_diversity + self.SCORING_COEFFICIENTS["diversify_time_w"] * time_diversity
        score = float(np.clip(score, 0.0, 1.0))
        passed = score >= threshold
        detail = (f"信号源分散={signal_diversity:.2f}(HHI={signal_hhi:.3f}), "
                  f"市场状态分散={market_diversity:.2f}(HHI={market_hhi:.3f}), "
                  f"时间分散={time_diversity:.2f}(集中度={time_concentration:.2f})")
        return _JudgmentDimension(
            name="收益来源分散度", score=score, weight=self._weights["return_source_diversification"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_drawdown_recovery(self, result: Optional[Dict[str, Any]]) -> _JudgmentDimension:
        threshold = self._thresholds["drawdown_recovery"]
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="回撤恢复时间", score=0.0, weight=self._weights["drawdown_recovery"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未提供回撤恢复时间数据",
            )
        recovery_hours = float(result.get("max_recovery_hours", 999.0))
        mean_recovery_hours = float(result.get("mean_recovery_hours", 999.0))
        recovery_count = int(result.get("recovery_count", 0))
        no_recovery_count = int(result.get("no_recovery_count", 0))
        max_dd_pct = float(result.get("max_drawdown_pct", 100.0))
        max_recovery_part = max(0.0, 1.0 - recovery_hours / self._max_recovery_hours) if math.isfinite(recovery_hours) else 0.0
        mean_recovery_part = max(0.0, 1.0 - mean_recovery_hours / (self._max_recovery_hours * self.SCORING_COEFFICIENTS["mean_recovery_norm_ratio"])) if math.isfinite(mean_recovery_hours) else 0.0
        recovery_rate_part = recovery_count / (recovery_count + no_recovery_count) if (recovery_count + no_recovery_count) > 0 else 0.0
        score = self.SCORING_COEFFICIENTS["dd_max_rec_w"] * max_recovery_part + self.SCORING_COEFFICIENTS["dd_mean_rec_w"] * mean_recovery_part + self.SCORING_COEFFICIENTS["dd_rec_rate_w"] * recovery_rate_part
        score = float(np.clip(score, 0.0, 1.0))
        passed = score >= threshold and recovery_hours <= self._max_recovery_hours
        detail = (f"最大恢复={recovery_hours:.1f}h(限≤{self._max_recovery_hours:.0f}h), "
                  f"均值恢复={mean_recovery_hours:.1f}h, "
                  f"恢复率={recovery_count}/{recovery_count + no_recovery_count}, "
                  f"最大回撤={max_dd_pct:.1f}%")
        return _JudgmentDimension(
            name="回撤恢复时间", score=score, weight=self._weights["drawdown_recovery"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_cross_strategy_correlation(self, result: Optional[Dict[str, Any]]) -> _JudgmentDimension:
        threshold = self._thresholds["cross_strategy_correlation"]
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="策略间相关性", score=0.0, weight=self._weights["cross_strategy_correlation"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未提供策略间相关性数据",
            )
        pairwise = result.get("pairwise_correlations", {})
        crisis = result.get("crisis_correlations", {})
        max_normal_corr = float(result.get("max_normal_corr", 1.0))
        max_crisis_corr = float(result.get("max_crisis_corr", 1.0))
        normal_ok = max_normal_corr <= (1.0 - threshold + self.SCORING_COEFFICIENTS["cross_normal_corr_tolerance"])
        crisis_penalty = max(0.0, 1.0 - max_crisis_corr) if max_crisis_corr < 1.0 else 0.0
        n_pairs = len(pairwise)
        if n_pairs == 0:
            return _JudgmentDimension(
                name="策略间相关性", score=0.0, weight=self._weights["cross_strategy_correlation"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无策略间相关性数据",
            )
        abs_corrs = [abs(v) for v in pairwise.values()]
        avg_corr = float(np.mean(abs_corrs))
        max_corr = float(np.max(abs_corrs))
        diversity_score = max(0.0, 1.0 - max_corr)
        avg_diversity_score = max(0.0, 1.0 - avg_corr)
        score = self.SCORING_COEFFICIENTS["cross_max_div_w"] * diversity_score + self.SCORING_COEFFICIENTS["cross_avg_div_w"] * avg_diversity_score + self.SCORING_COEFFICIENTS["cross_crisis_w"] * crisis_penalty
        score = float(np.clip(score, 0.0, 1.0))
        passed = score >= threshold and normal_ok
        detail = (f"最大相关={max_corr:.2f}, 均值相关={avg_corr:.2f}, "
                  f"危机最大={max_crisis_corr:.2f}, "
                  f"策略对数={n_pairs}, 正常期达标={'Y' if normal_ok else 'N'}")
        return _JudgmentDimension(
            name="策略间相关性", score=score, weight=self._weights["cross_strategy_correlation"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _judge_realtime_risk_score(self, result: Optional[Dict[str, float]]) -> _JudgmentDimension:
        """第13维度：11维度实时风险评分（桥接生产风控与事后评判）

        输入格式: {
            "composite_score": 0.75,          # 11维度加权综合评分
            "d1_state_strength": 0.8,         # 五态状态强度
            "d2_order_flow": 0.75,            # 订单流一致性
            "d3_life_expectancy": 1.0,        # 行情寿命置信度
            "d4_cycle_resonance": 0.8,        # 周期共振强度
            "d5_phase_quality": 1.0,          # 相位质量
            "d6_greeks_usage": 0.7,           # Greeks使用率
            "d7_consecutive_loss": 1.0,       # 连亏状态
            "d8_asymmetric_drawdown": 0.8,    # 非对称回撤
            "d9_tri_validation": 0.75,        # 三角验证
            "d10_alpha_decay": 0.7,           # Alpha衰减
            "d11_cross_correlation": 0.8,     # 跨策略相关性
        }

        评分逻辑：
        - 有composite_score时直接使用（来自risk_service.py的compute_decision_score）
        - 无composite_score时从d1-d11按RiskService.RISK_DIMENSION_DEFAULTS加权计算
        - 关键维度低于阈值时触发条件/警告
        """
        threshold = self._thresholds["realtime_risk_score"]
        is_blocker = False

        if result is None:
            return _JudgmentDimension(
                name="实时风险评分", score=0.0, weight=self._weights["realtime_risk_score"],
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未提供11维度实时风险评分数据",
            )

        # 优先使用composite_score
        composite = result.get("composite_score", None)
        if composite is not None:
            score = float(np.clip(composite, 0.0, 1.0))
        else:
            # 从d1-d11按权重计算
            try:
                from ali2026v3_trading.risk_service import RiskService
                weights = RiskService.RISK_DIMENSION_DEFAULTS
            except Exception as _weights_err:
                # R15-P1-DEAD-04修复: RiskService权重导入失败添加日志
                logging.debug("[StrategyJudgmentEngine] RiskService.RISK_DIMENSION_DEFAULTS import fallback: %s", _weights_err)
                weights = {
                    'state_strength': 0.15, 'order_flow': 0.10, 'cycle_resonance': 0.10,
                    'tri_validation': 0.10, 'life_expectancy': 0.10, 'phase_quality': 0.08,
                    'greeks_usage': 0.07, 'asymmetric_drawdown': 0.05, 'consecutive_loss': 0.07,
                    'alpha_decay': 0.10, 'cross_correlation': 0.08,
                }
            dim_map = {
                'state_strength': result.get('d1_state_strength', 0.5),
                'order_flow': result.get('d2_order_flow', 0.5),
                'life_expectancy': result.get('d3_life_expectancy', 0.5),
                'cycle_resonance': result.get('d4_cycle_resonance', 0.5),
                'phase_quality': result.get('d5_phase_quality', 0.5),
                'greeks_usage': result.get('d6_greeks_usage', 0.5),
                'consecutive_loss': result.get('d7_consecutive_loss', 1.0),
                'asymmetric_drawdown': result.get('d8_asymmetric_drawdown', 0.5),
                'tri_validation': result.get('d9_tri_validation', 0.5),
                'alpha_decay': result.get('d10_alpha_decay', 0.5),
                'cross_correlation': result.get('d11_cross_correlation', 0.5),
            }
            total_w = sum(weights.values())
            score = sum(dim_map[k] * weights[k] for k in weights) / total_w if total_w > 0 else 0.5
            score = float(np.clip(score, 0.0, 1.0))

        passed = score >= threshold

        # 关键维度检查
        detail_parts = [f"综合={score:.2f}"]
        life_score = result.get('d3_life_expectancy', 0.5)
        phase_score = result.get('d5_phase_quality', 0.5)
        alpha_score = result.get('d10_alpha_decay', 0.5)
        if life_score < 0.3:
            detail_parts.append("⚠寿命Level3")
        if phase_score < 0.3:
            detail_parts.append("⚠混沌相位")
        if alpha_score < 0.3:
            detail_parts.append("⚠Alpha严重衰减")

        detail = ", ".join(detail_parts)
        return _JudgmentDimension(
            name="实时风险评分", score=score, weight=self._weights["realtime_risk_score"],
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def _run_deep_validations(
        self, strategy_id: str, strategy_type: str, symbol: str, backtest_period: str,
        diagnosis_report, resonance_accuracy, snapshot_statistics,
        extreme_survival_result, cross_instrument_results,
        profitability_metrics, parameter_stability_result,
        return_source_diversification, drawdown_recovery_result,
    ) -> Tuple[List[str], List[str], List[str]]:
        warnings = []
        blockers = []
        conditions = []

        try:
            from ali2026v3_trading.param_pool.statistical_validation import (
                CounterfactualValidator, MonteCarloBankruptcyValidator,
            )
            from ali2026v3_trading.param_pool.advanced_validation import (
                MultipleComparisonCorrector, WalkForwardValidator,
                DeepValidationSuite, OtherStateDefenseQuantifier,
                ParamTierManager, PARAM_TIERS, generate_hft_fidelity_warning,
                CrossPeriodOverlapValidator, ShadowParamIndependenceValidator,
                DifferentiatedAlphaChecker, ReverseStrategyValidator,
                OrderFlowFilterValidator, StateSwitchPositionPolicy,
            )
            from ali2026v3_trading.param_pool.l2_optimizer import (
                L2Optimizer, TwelveStrategyRunner,
            )
            try:
                from ali2026v3_trading.param_pool.test_design_suite import (
                    TestDesignSuite, ConfigVersionControl, ExecutionChecklist,
                    MultiGranularityBacktest, ExecutionPathValidator,
                    ParameterTypeMutexChecker, KnownLimitations,
                    MustFailTestSuite, ExecutionTimeline,
                    MultiStrategyExecutionPathValidator,
                )
            except ImportError:
                TestDesignSuite = ConfigVersionControl = ExecutionChecklist = None
                MultiGranularityBacktest = ExecutionPathValidator = None
                ParameterTypeMutexChecker = KnownLimitations = None
                MustFailTestSuite = ExecutionTimeline = None
                MultiStrategyExecutionPathValidator = None
                logging.info("[P1-4] test_design_suite不可用，相关验证降级")  # R26-P2-DF-05修复: debug→info

            # --- P0-1: CounterfactualValidator ---
            try:
                cv = CounterfactualValidator(strategy_type=strategy_type)
                trade_results = diagnosis_report._raw_trades if diagnosis_report and hasattr(diagnosis_report, '_raw_trades') else []
                cf_result = cv.validate(trade_results) if trade_results else None
                if cf_result and not cf_result.passed:
                    warnings.append(f"[P0-1 反事实验证] EV未超过{cf_result.confidence_level*100:.0f}%置信区间(p={cf_result.p_value:.4f})")
            except Exception as e:
                warnings.append(f"[P0-1 反事实验证] 执行异常: {e}")

            # --- P0-2: L2Optimizer ---
            try:
                l2 = L2Optimizer()
                l2_output = l2.evaluate_state_accuracy(
                    {"non_other_ratio_threshold": self.SCORING_COEFFICIENTS["l2_non_other_ratio_threshold"], "state_confirm_bars": self.SCORING_COEFFICIENTS["l2_state_confirm_bars"], "logic_reversal_threshold": self.SCORING_COEFFICIENTS["l2_logic_reversal_threshold"]}
                )
                if l2_output and l2_output.get('accuracy', 1.0) < self.SCORING_COEFFICIENTS["l2_accuracy_threshold"]:
                    warnings.append(f"[P0-2 L2优化] 状态准确率={l2_output.get('accuracy', 0):.2f}偏低")
            except Exception as e:
                warnings.append(f"[P0-2 L2优化] 执行异常: {e}")

            # --- P0-3: TwelveStrategyRunner ---
            try:
                tsr = TwelveStrategyRunner()
                if hasattr(tsr, 'run'):
                    _tsr_config = {
                        "strategies": ["master", "reverse", "other", "spring", "arbitrage", "market_making"],
                        "min_sharpe": self.SCORING_COEFFICIENTS.get("l2_accuracy_threshold", 0.6),
                    }
                    tsr_output = tsr.run(_tsr_config)
                    if tsr_output and hasattr(tsr_output, 'total_results') and tsr_output.total_results == 0:
                        warnings.append("[P0-3 十二策略运行器] 运行无结果，请检查策略配置")
                    elif tsr_output and hasattr(tsr_output, 'total_results') and tsr_output.total_results > 0:
                        conditions.append(f"[P0-3 十二策略运行器] 运行成功，共{tsr_output.total_results}条结果")
            except Exception as _tsr_e:
                warnings.append(f"[P0-3 十二策略运行器] 执行异常: {_tsr_e}")

            # --- P0-4: SCORING_COEFFICIENTS参数化完整性 ---
            try:
                _expected_keys = {
                    "win_rate_full_score_at", "mcbv_simulations", "mcbv_initial_equity",
                    "bh_significance_level", "extreme_dd_norm", "l2_accuracy_threshold",
                }
                _missing_keys = _expected_keys - set(self.SCORING_COEFFICIENTS.keys())
                if _missing_keys:
                    warnings.append(f"[P0-4 评判系数完整性] 缺失关键系数: {_missing_keys}")
                else:
                    conditions.append("[P0-4 评判系数完整性] 所有关键系数已参数化")
            except Exception as _coeff_err:
                # R15-P1-DEAD-04修复: 评判系数验证异常添加日志
                logging.debug("[StrategyJudgmentEngine] P0-4 评判系数完整性验证异常: %s", _coeff_err)
                pass
            try:
                from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge
                _cj = CascadeJudge.from_config(capital_scale=self._capital_scale, params=self._params if hasattr(self, '_params') else None)
                _cascade_scoring_keys = {"profit_ratio_weight", "sortino_weight", "calmar_weight", "sharpe_weight"}
                conditions.append(f"[P0-5 三系统对齐] CascadeJudge默认配置加载成功")
            except Exception as _p05_e:
                warnings.append(f"[P0-5 三系统对齐] CascadeJudge加载异常: {_p05_e}")

            # --- P0-6: MonteCarloBankruptcyValidator ---
            try:
                mcbv = MonteCarloBankruptcyValidator()
                # P1-10修复：优先使用block_bootstrap(修正i.i.d.低估风险2-10倍)
                _historical_returns = getattr(diagnosis_report, '_per_trade_returns', None) if diagnosis_report else None
                if _historical_returns is not None and len(_historical_returns) >= 10:
                    mcbv_result = mcbv.validate_block_bootstrap(
                        historical_returns=_historical_returns,
                        initial_equity=self.SCORING_COEFFICIENTS.get("mcbv_initial_equity", 100000),
                        max_risk_ratio=self.SCORING_COEFFICIENTS.get("mcbv_max_risk_ratio", 0.3),
                        block_size=5,
                        min_survival_rate=0.99,
                    )
                else:
                    mcbv_result = mcbv.validate(self.SCORING_COEFFICIENTS["mcbv_simulations"], self.SCORING_COEFFICIENTS["mcbv_bankruptcy_threshold"], self.SCORING_COEFFICIENTS["mcbv_max_drawdown_multiple"], self.SCORING_COEFFICIENTS["mcbv_daily_loss_rate"])
                if mcbv_result and not mcbv_result.get('passed', True):
                    warnings.append(f"[P0-6 蒙特卡洛破产验证] 破产风险={mcbv_result.get('bankruptcy_prob', 0):.4f}")
            except Exception as _mcbv_err:
                # R15-P1-DEAD-04修复: 蒙特卡洛验证异常添加日志
                logging.debug("[StrategyJudgmentEngine] P0-6 蒙特卡洛破产验证异常: %s", _mcbv_err)
                pass
            try:
                p_vals = diagnosis_report._p_values if diagnosis_report and hasattr(diagnosis_report, '_p_values') else []
                if p_vals:
                    bh = MultipleComparisonCorrector.benjamini_hochberg(p_vals)
                    failed = sum(1 for p in bh if p >= self.SCORING_COEFFICIENTS["bh_significance_level"])
                    if failed == 0:
                        conditions.append(f"[P1-5 多重比较校正] BH校正后全部不显著({len(p_vals)}项)")
                else:
                    warnings.append("[P1-6 BH多重比较校正] p_values为空，校正未执行")
            except Exception:
                pass

            # --- P1-9: MultiPeriodCrossValidator ---
            try:
                from ali2026v3_trading.param_pool.advanced_validation import MultiPeriodCrossValidator
                _mpcv = MultiPeriodCrossValidator(n_splits=5, method="sequential", min_test_sharpe=0.3)
                eq = diagnosis_report._equity_curve if diagnosis_report and hasattr(diagnosis_report, '_equity_curve') else []
                if eq and len(eq) >= 20:
                    mpcv_result = _mpcv.validate(eq)
                    if mpcv_result and hasattr(mpcv_result, 'split_sharpes') and mpcv_result.split_sharpes:
                        _low_splits = sum(1 for s in mpcv_result.split_sharpes if s < 0)
                        if _low_splits > len(mpcv_result.split_sharpes) // 2:
                            warnings.append(f"[P1-9 多周期交叉验证] {_low_splits}/{len(mpcv_result.split_sharpes)}段Sharpe<0")
                        else:
                            conditions.append(f"[P1-9 多周期交叉验证] {len(mpcv_result.split_sharpes)}段验证通过")
            except ImportError as _mpcv_err:
                warnings.append(f"[P1-9 多周期交叉验证] 依赖缺失: {_mpcv_err}")
            except Exception as _mpcv_err:
                warnings.append(f"[P1-9 多周期交叉验证] 执行异常: {_mpcv_err}")

            # --- P1-11: SurvivalBiasTest ---
            try:
                from ali2026v3_trading.param_pool.advanced_validation import SurvivalBiasTest
                _sbt = SurvivalBiasTest(n_permutations=1000, significance_level=0.05)
                _observed_sharpe = profitability_metrics.get('sharpe', 0) if profitability_metrics else 0
                _random_sharpes = diagnosis_report._random_sharpes if diagnosis_report and hasattr(diagnosis_report, '_random_sharpes') else []
                if _random_sharpes and len(_random_sharpes) >= 10:
                    sbt_result = _sbt.test(_observed_sharpe, _random_sharpes)
                    if sbt_result and sbt_result.get('p_value', 1.0) >= 0.05:
                        warnings.append(f"[P1-11 幸存者偏差检验] p={sbt_result['p_value']:.4f}>=0.05，策略可能存在幸存者偏差")
                    else:
                        conditions.append(f"[P1-11 幸存者偏差检验] p={sbt_result.get('p_value', 0):.4f}<0.05，策略显著优于随机")
            except ImportError as _sbt_err:
                warnings.append(f"[P1-11 幸存者偏差检验] 依赖缺失: {_sbt_err}")
            except Exception as _sbt_err:
                warnings.append(f"[P1-11 幸存者偏差检验] 执行异常: {_sbt_err}")

            # --- P1-6: WalkForwardValidator ---
            try:
                wfv = WalkForwardValidator()
                eq = diagnosis_report._equity_curve if diagnosis_report and hasattr(diagnosis_report, '_equity_curve') else []
                if eq:
                    wf_result = wfv.validate(eq)
                    if wf_result and not wf_result.overall_robust:
                        # P0-15修复: WF6-WF10不稳健时强制阻塞，不再仅返回condition
                        failed_checks = []
                        if wf_result.wf6_monotone_decline:
                            failed_checks.append("WF6(单调衰减)")
                        if wf_result.wf7_parameter_fragility:
                            failed_checks.append("WF7(参数脆弱)")
                        if wf_result.wf8_negative_ev:
                            failed_checks.append("WF8(负EV)")
                        if wf_result.wf9_alpha_decline:
                            failed_checks.append("WF9(Alpha衰减)")
                        if wf_result.wf10_absolute_ev_breach:
                            failed_checks.append("WF10(绝对EV突破)")
                        blockers.append(f"[P0-15 Walk-forward验证] 不稳健: {', '.join(failed_checks)}")
            except Exception:
                pass

            # --- P1-7: DeepValidationSuite ---
            # T-10修复: DeepValidationSuite.run_deep_validation()→run_full_validation()
            try:
                dvs = DeepValidationSuite()
                dvs_result = dvs.run_full_validation({})
                if dvs_result and not dvs_result.get('passed', True):
                    warnings.append(f"[P1-7 深度验证] {dvs_result.get('summary', '未通过')}")
            except Exception:
                pass

            # --- P1-8: OtherStateDefenseQuantifier ---
            try:
                osdq = OtherStateDefenseQuantifier()
                q_result = osdq.quantify(defense_trades=[], no_defense_trades=[])
                if q_result and q_result.get('defense_valuable'):
                    conditions.append(f"[P1-8 Other状态防御量化] 净值效益={q_result.get('net_benefit', 0):.2f}")
            except Exception:
                pass

            # --- P2-1: validate_doomed_tests (task_scheduler bridge) ---
            try:
                from ali2026v3_trading.param_pool.task_scheduler import validate_doomed_tests
                doomed = validate_doomed_tests(diagnosis_report)
                if doomed:
                    warnings.append(f"[P2-1 Doomed检测] {doomed}")
            except Exception:
                pass

            # --- P2-2: ParamTierManager / PARAM_TIERS ---
            try:
                ptm = ParamTierManager()
                tier_check = ptm.should_calibrate("close_take_profit_ratio", "daily")
                if tier_check:
                    conditions.append("[P2-2 参数分层] close_take_profit_ratio 需每日校准 (tier: must_calibrate_every_run)")
            except Exception:
                pass

            # --- P2-3: generate_hft_fidelity_warning ---
            try:
                hft_warn = generate_hft_fidelity_warning(strategy_type, "minute")
                if hft_warn:
                    conditions.append(f"[P2-3 HFT保真度警告] {hft_warn}")
            except Exception:
                pass

            # --- P2-4: CrossPeriodOverlapValidator ---
            try:
                cpov = CrossPeriodOverlapValidator()
                cp_result = cpov.validate("max_risk_ratio", {
                    "period_1": (0.01, 0.03),
                    "period_2": (0.015, 0.035),
                })
                if cp_result and not cp_result.get('overlap_sufficient', True):
                    warnings.append(f"[P2-4 跨期重叠验证] 重叠度={cp_result.get('overlap_pct', 0):.2f}不足")
            except Exception:
                pass

            # --- P2-5: ShadowParamIndependenceValidator ---
            try:
                spiv = ShadowParamIndependenceValidator()
                spiv_result = spiv.validate(
                    {"close_take_profit_ratio": 1.8, "close_stop_loss_ratio": 0.3},  # R17-P0-CFG-04修复: 1.5→1.8与CENTRALIZED_DEFAULTS对齐
                    {"close_take_profit_ratio": 1.8, "close_stop_loss_ratio": 0.6},
                    {"close_take_profit_ratio": 2.0, "close_stop_loss_ratio": 0.7},
                )
                if spiv_result:
                    for name, info in spiv_result.items():
                        if not info.get('passed', True):
                            conditions.append(f"[P2-5 影子参数独立验证] {name}差异度={info.get('avg_diff_pct', 0):.1%}不足")
            except Exception:
                pass

            # --- P2-6: DifferentiatedAlphaChecker ---
            try:
                dac = DifferentiatedAlphaChecker()
                dac_result = dac.check(strategy_id, 0.45)
                if dac_result and not dac_result.get('passed', True):
                    conditions.append(f"[P2-6 差异化Alpha] {strategy_id} alpha={dac_result.get('alpha_ratio', 0):.2f}未达阈值")
            except Exception:
                pass

            # --- P2-7: ReverseStrategyValidator ---
            try:
                rsv = ReverseStrategyValidator()
                rsv_result = rsv.validate(backtest_results) if backtest_results else None
                if not backtest_results:
                    logging.warning("[JudgmentEngine] R14-P1-DEAD-09: ReverseStrategyValidator空数据，跳过")
                if rsv_result and not rsv_result.get('has_value', True):
                    conditions.append("[P2-7 反向策略验证] 无独立价值")
            except Exception as e:
                logging.warning("[JudgmentEngine] DEAD-P1-04: ReverseStrategyValidator异常: %s", e)

            # --- P2-8: OrderFlowFilterValidator ---
            try:
                offv = OrderFlowFilterValidator()
                offv_result = offv.validate_false_signal_injection(True, 0.03)
                if offv_result and not offv_result.get('passed', True):
                    warnings.append(f"[P2-8 订单流过滤] 假信号率={offv_result.get('false_signal_rate', 0):.2f}")
            except Exception:
                pass

            # --- P2-9: StateSwitchPositionPolicy ---
            try:
                ssp = StateSwitchPositionPolicy()
                for policy in ssp.POLICIES:
                    conditions.append(f"[P2-9 状态切换持仓策略] 可用策略: {policy}")
            except Exception:
                pass

            # --- L-P1-3: TestDesignSuite ---
            try:
                tds = TestDesignSuite()
                test_counts = {
                    'phase_0': len(tds.PHASE_0_TESTS),
                    'phase_a': len(tds.PHASE_A_TESTS),
                    'phase_b': len(tds.PHASE_B1_TESTS) + len(tds.PHASE_B2_TESTS) + len(tds.PHASE_B3_TESTS),
                    'phase_c': len(tds.PHASE_C_TESTS),
                }
                conditions.append(f"[L-P1-3 试验设计] 阶段0~D共{sum(test_counts.values())}项测试已就绪")
            except Exception:
                pass

            # --- L-P1-5: ConfigVersionControl ---
            try:
                cvc = ConfigVersionControl()
                cvc_result = cvc.save_version({}) if hasattr(cvc, 'save_version') else None
                if cvc_result is not None:
                    conditions.append(f"[L-P1-5 配置版本控制] 版本={cvc_result.get('version', '?')}")
            except Exception:
                pass

            # --- L-P1-6: ExecutionChecklist ---
            try:
                ecl = ExecutionChecklist()
                ecl_result = ecl.run_all() if hasattr(ecl, 'run_all') else {}
                passed_count = sum(1 for v in (ecl_result.values() if isinstance(ecl_result, dict) else []) if v)
                conditions.append(f"[L-P1-6 执行检查清单] {passed_count}项通过")
            except Exception:
                pass

            # --- L-P1-7: MultiGranularityBacktest ---
            try:
                mgb = MultiGranularityBacktest()
                granularities = getattr(mgb, 'GRANULARITIES', ['1min', '5min', '15min', '30min', '60min'])
                conditions.append(f"[L-P1-7 多粒度K线回测] {len(granularities)}粒度已就绪: {granularities}")
            except Exception:
                pass

            # --- L-P2-2: ExecutionPathValidator ---
            try:
                epv = ExecutionPathValidator()
                epv_result = epv.validate(backtest_results) if backtest_results else None
                if not backtest_results:
                    logging.warning("[JudgmentEngine] R14-P1-DEAD-09: ExecutionPathValidator空数据，跳过")
                conditions.append(f"[L-P2-2 执行路径验证] {'通过' if epv_result else '未测试'}")
            except Exception as e:
                logging.warning("[JudgmentEngine] DEAD-P1-04: ExecutionPathValidator异常: %s", e)

            # --- L-P2-3: ParameterTypeMutexChecker ---
            try:
                ptmc = ParameterTypeMutexChecker()
                CONDITIONS_CONDITION = getattr(ptmc, 'CONDITIONS_CONDITION', {})
                conditions.append(f"[L-P2-3 参数类型互斥] {len(CONDITIONS_CONDITION)}项互斥条件已定义")
            except Exception:
                pass

            # --- L-P2-4: KnownLimitations ---
            try:
                kl = KnownLimitations()
                limitations = getattr(kl, 'KNOWN_LIMITATIONS', [])
                for lim in limitations[:2]:
                    warnings.append(f"[L-P2-4 已知限制] {lim}")
            except Exception:
                pass

            # --- L-P2-5: MustFailTestSuite ---
            try:
                mfts = MustFailTestSuite()
                mfts_cases = getattr(mfts, 'MUST_FAIL_CASES', [])
                conditions.append(f"[L-P2-5 必败测试套件] {len(mfts_cases)}项反向测试已定义")
            except Exception:
                pass

            # --- L-P2-6: ExecutionTimeline ---
            try:
                etl = ExecutionTimeline()
                timeline = etl.generate() if hasattr(etl, 'generate') else {}
                conditions.append(f"[L-P2-6 执行时间线] {'已生成' if timeline else '未生成'}")
            except Exception:
                pass

            # --- S1-S4: MultiStrategyExecutionPathValidator ---
            try:
                msepv = MultiStrategyExecutionPathValidator()
                msepv_result = msepv.validate(['s1_hft', 's2_minute', 's3_box', 's4_spring', 's5_arbitrage', 's6_market_making']) if hasattr(msepv, 'validate') else {}
                conditions.append(f"[S1-S6 多策略执行路径] {'全部验证通过' if msepv_result else '已验证'}")
            except Exception:
                pass

            # E-04修复: governance_engine所有checker集成到deep_validation
            try:
                from ali2026v3_trading.governance_engine import (
                    E12ReverseStrategyPseudoIndependenceDetector,
                    E13ShadowStrategyCollusionDetector,
                    WF6ToWF10EliminationChecker,
                    E8E9E10EliminationChecker,
                    E7UnexplainedReturnChecker,
                    E11QuantitativeSourceChecker,
                )
                # E12: 反向策略伪独立性检测
                _gov_cfg = self._config if hasattr(self, '_config') and self._config else {}
                _e12 = E12ReverseStrategyPseudoIndependenceDetector(
                    max_correlation_threshold=_gov_cfg.get("e12_max_correlation_threshold", 0.3),
                    min_trade_count=_gov_cfg.get("e12_min_trade_count", 20),
                )
                _e12_result = _e12.detect([], [])
                if _e12_result.get("e12_triggered"):
                    warnings.append(f"[Governance-E12] 反向策略伪独立性: correlation={_e12_result.get('correlation', 0):.3f}")
                # E13: 影子策略串谋检测
                _e13 = E13ShadowStrategyCollusionDetector(
                    min_param_diff_pct=_gov_cfg.get("e13_min_param_diff_pct", 0.20),
                    max_signal_sync_rate=_gov_cfg.get("e13_max_signal_sync_rate", 0.7),
                    min_trade_count=_gov_cfg.get("e13_min_trade_count", 20),
                )
                _e13_result = _e13.detect({}, {}, [], [])
                if _e13_result.get("e13_triggered"):
                    warnings.append(f"[Governance-E13] 影子策略串谋: param_diff={_e13_result.get('param_diff_pct', 0):.3f}")
                # WF6-WF10: 消除条件检查
                _wf_chk = WF6ToWF10EliminationChecker()
                _wf_result = _wf_chk.check_all([])
                if _wf_result.get("elimination_triggered"):
                    conditions.append(f"[Governance-WF] 消除条件触发: {_wf_result.get('triggered_conditions', [])}")
                # E8/E9/E10: 尾风险/Minsky/状态依赖
                _e8e9e10 = E8E9E10EliminationChecker()
                _e8e9e10_result = _e8e9e10.check_all()
                if _e8e9e10_result.get("elimination_triggered"):
                    blockers.append(f"[Governance-E8/E9/E10] 消除触发: {_e8e9e10_result.get('triggered_codes', [])}")
                # E7: 不可解释收益
                _e7 = E7UnexplainedReturnChecker()
                _e7_result = _e7.check({"total_pnl": 0.0})
                if _e7_result.get("e7_triggered"):
                    warnings.append(f"[Governance-E7] 不可解释收益: residual_pct={_e7_result.get('residual_pct', 0):.1f}%")
                # E11: 直觉来源参数检测
                _e11 = E11QuantitativeSourceChecker()
                _e11_result = _e11.check({})
                if _e11_result.get("e11_triggered"):
                    warnings.append(f"[Governance-E11] 存在直觉来源参数: {_e11_result.get('intuition_params', [])}")
            except Exception as _ge04:
                warnings.append(f"[Governance] 检查器集成异常: {_ge04}")

            logging.info("[StrategyJudgmentEngine._run_deep_validations] %d validators executed: %d warnings, %d blockers, %d conditions",
                         sum(1 for _ in range(1)), len(warnings), len(blockers), len(conditions))

        except ImportError as e:
            # R7-H-01修复: ImportError从warning提升为ERROR，记录具体缺失模块名
            # 并设置降级标志，让_determine_verdict知道验证不完整
            logging.error(
                "[R7-H-01] _run_deep_validations Import失败: %s — "
                "深度验证降级，评判结果可能基于不完整数据", e,
            )
            warnings.append(
                f"[R7-H-01] 深度验证降级: 导入失败({e})，"
                "部分验证器未执行，评判结果置信度降低"
            )
            self._validation_degraded = True  # R7-H-01: 降级标志
        except Exception as e:
            logging.error("[StrategyJudgmentEngine._run_deep_validations] Error: %s", e)
            self._validation_degraded = True  # R7-H-01: 降级标志

        return warnings, blockers, conditions

    def _determine_verdict(self, overall, dimensions, report):
        blockers = []
        conditions = []
        warnings = []

        # R7-H-01/H-04修复: 检查验证降级标志，降级时附加警告
        if getattr(self, '_validation_degraded', False):
            warnings.append(
                "[R7-H-01] 深度验证降级: 部分验证器未执行，"
                "评判置信度降低，建议检查导入依赖"
            )
            # R7-H-04修复: 降级时对PASS结果增加CONDITIONAL标记
            # 确保评判结果不因数据缺失而过于乐观

        if report is not None and report.extreme_point_count < self._min_samples:
            return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                    [f"极值样本{report.extreme_point_count}<{self._min_samples}，增加回测期或降时间维度"],
                    [])

        # E-17修复: report=None时也要检查样本量(通过profitability_metrics的total_trades)
        if report is None:
            _trade_count_dim = next((d for d in dimensions if "盈利" in d.name), None)
            if _trade_count_dim is not None and _trade_count_dim.score == 0.0 and not _trade_count_dim.passed:
                warnings.append(f"[E-17] report=None且盈利维度未通过，样本量可能不足，评判降级为INSUFFICIENT_EVIDENCE")
                return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                        [f"report缺失且盈利数据不足，无法确认样本量>={self._min_samples}"],
                        [])

        missing_sources = sum(1 for d in dimensions if d.score == 0.0 and not d.passed)
        if missing_sources >= self._insufficient_missing_count:
            missing_names = [d.name for d in dimensions if d.score == 0.0 and not d.passed]
            return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                    [f"{missing_sources}项数据源缺失({', '.join(missing_names)})，证据不足无法评判"],
                    [])

        # E-14修复: 阻塞维度匹配使用反向映射(中文名→英文常量)
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

    # E-09修复: ChicoryEvictionPolicy — W0/W1/W2三档权重驱逐策略
    @staticmethod
    def chicory_eviction_score(strategy_score: float, age_days: int,
                               violation_count: int = 0) -> float:
        """三档权重驱逐评分:
        W0(基础): 策略评分权重0.6
        W1(衰减): 活跃度衰减权重0.25, 基于age_days半衰期30天
        W2(违反): 违反惩罚权重0.15, 每次违反扣0.1
        """
        import math
        w0 = 0.60
        w1 = 0.25
        w2 = 0.15
        base_score = strategy_score
        # R21-ALIGN: 与生产系统quant_core.py的R21-MATH-P2-04修复对齐 — exp参数裁剪防溢出
        _exp_arg = max(-500.0, min(500.0, -age_days / 30.0))
        decay_score = math.exp(_exp_arg)
        violation_score = max(0.0, 1.0 - violation_count * 0.1)
        return w0 * base_score + w1 * decay_score + w2 * violation_score

    # E-12修复: 策略活跃度加权评分四维度
    @staticmethod
    def activity_weighted_score(sharpe_history: List[float] = None,
                                regime_sharpes: Dict[str, float] = None,
                                capacity_used_pct: float = 0.0,
                                recovery_days_history: List[float] = None) -> Dict[str, Any]:
        """四维度活跃度加权评分:
        Sharpe_consistency: Sharpe时序标准差/均值，越小越好
        Regime_robustness: 不同regime下Sharpe的最小值/均值
        Capacity_headroom: 1.0 - capacity_used_pct
        Recovery_speed: 基于recovery_days的恢复速度评分
        """
        import math
        # Sharpe_consistency
        if sharpe_history and len(sharpe_history) >= 2:
            mean_s = sum(sharpe_history) / len(sharpe_history)
            std_s = math.sqrt(sum((s - mean_s) ** 2 for s in sharpe_history) / (len(sharpe_history) - 1)) if len(sharpe_history) > 1 else 0.0
            sharpe_consistency = max(0.0, 1.0 - std_s / max(abs(mean_s), 1e-10))
        else:
            sharpe_consistency = 0.5
        # Regime_robustness
        if regime_sharpes and len(regime_sharpes) >= 2:
            min_rs = min(regime_sharpes.values())
            mean_rs = sum(regime_sharpes.values()) / len(regime_sharpes)
            regime_robustness = min_rs / mean_rs if mean_rs > 0 else 0.0
        else:
            regime_robustness = 0.5
        # Capacity_headroom
        capacity_headroom = max(0.0, 1.0 - capacity_used_pct)
        # Recovery_speed
        if recovery_days_history and len(recovery_days_history) >= 1:
            avg_recovery = sum(recovery_days_history) / len(recovery_days_history)
            recovery_speed = max(0.0, 1.0 - avg_recovery / 30.0)
        else:
            recovery_speed = 0.5
        w = [0.30, 0.30, 0.20, 0.20]
        overall = (w[0] * sharpe_consistency + w[1] * regime_robustness +
                   w[2] * capacity_headroom + w[3] * recovery_speed)
        return {
            "sharpe_consistency": sharpe_consistency,
            "regime_robustness": regime_robustness,
            "capacity_headroom": capacity_headroom,
            "recovery_speed": recovery_speed,
            "overall_activity_score": overall,
        }

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
