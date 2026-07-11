# [M3-05] 风险合规评判服务
# MODULE_ID: M3-618
"""Risk Compliance Judgment Service"""
from __future__ import annotations

import logging, math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .judgment_types import (_JudgmentDimension, DIM_RISK_BUDGET_COMPLIANCE, DIM_EXTREME_SURVIVAL, DIM_DISPLAY_NAMES, SCORING_COEFFICIENTS)
from ali2026v3_trading.infra._helpers import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class RiskJudger:
    """Risk compliance judgment service."""

    def __init__(self, scoring_coefficients=None, min_samples=30, max_recovery_hours=24.0, extreme_max_recovery_hours=24.0):
        self.SCORING_COEFFICIENTS = scoring_coefficients or SCORING_COEFFICIENTS
        self._min_samples = min_samples
        self._max_recovery_hours = max_recovery_hours
        self._extreme_max_recovery_hours = extreme_max_recovery_hours

    def judge_statistical_significance(self, report, threshold=0.70, weight=0.06) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if report is None:
            return _JudgmentDimension(
                name="统计显著性", score=0.0, weight=weight,
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无诊断报告",
            )
        n = report.extreme_point_count
        confidence = report.overall_score.confidence
        score = confidence
        passed = n >= self._min_samples and confidence >= threshold
        detail = f"样本量={n}(≥{self._min_samples}), 置信度={confidence:.2f}"
        return _JudgmentDimension(
            name="统计显著性", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_risk_budget_compliance(self, report, threshold=0.70, weight=0.11) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = True
        if report is None:
            return _JudgmentDimension(
                name=DIM_DISPLAY_NAMES[DIM_RISK_BUDGET_COMPLIANCE], score=0.0, weight=weight,
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
            name=DIM_DISPLAY_NAMES[DIM_RISK_BUDGET_COMPLIANCE], score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_extreme_survival(self, result, threshold=0.60, weight=0.08) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = True
        if result is None:
            return _JudgmentDimension(
                name=DIM_DISPLAY_NAMES[DIM_EXTREME_SURVIVAL], score=0.0, weight=weight,
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
            name=DIM_DISPLAY_NAMES[DIM_EXTREME_SURVIVAL], score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_drawdown_recovery(self, result, threshold=0.50, weight=0.05) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="回撤恢复时间", score=0.0, weight=weight,
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
            name="回撤恢复时间", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_realtime_risk_score(self, result, threshold=0.50, weight=0.10) -> _JudgmentDimension:
        """第13维度：11维度实时风险评分（桥接生产风控与事后评判）'
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
        threshold = threshold
        is_blocker = False

        if result is None:
            return _JudgmentDimension(
                name="实时风险评分", score=0.0, weight=weight,
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
                from ali2026v3_trading.risk.risk_service import RiskService
                weights = RiskService.RISK_DIMENSION_DEFAULTS
            except (ImportError, ValueError, KeyError, AttributeError) as _weights_err:
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
            name="实时风险评分", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )
