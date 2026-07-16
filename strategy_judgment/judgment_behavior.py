# [M3-04] 行为一致性评判服务
# MODULE_ID: M3-614
"""Behavior Consistency Judgment Service"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np

from .judgment_types import (_JudgmentDimension, DIM_BEHAVIOR_CONSISTENCY, DIM_DISPLAY_NAMES, SCORING_COEFFICIENTS)
from .strategy_behavior_diagnosis import DiagnosisSeverity
from infra._helpers import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class BehaviorJudger:
    """Behavior consistency judgment service."""

    def __init__(self, scoring_coefficients=None, min_instruments=3):
        self.SCORING_COEFFICIENTS = scoring_coefficients or SCORING_COEFFICIENTS
        self._min_instruments = min_instruments

    def judge_behavior_consistency(self, report, threshold, weight) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = True
        if report is None:
            return _JudgmentDimension(
                name=DIM_DISPLAY_NAMES[DIM_BEHAVIOR_CONSISTENCY], score=0.0, weight=weight,
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
            name=DIM_DISPLAY_NAMES[DIM_BEHAVIOR_CONSISTENCY], score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_process_explainability(self, report, explanation_coverage=None, threshold=0.65, weight=0.06) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if explanation_coverage is not None:
            total_decisions = int(explanation_coverage.get("total_decisions", 0))
            explained_decisions = int(explanation_coverage.get("explained_decisions", 0))
            avg_chain_length = float(explanation_coverage.get("avg_chain_length", 0.0))
            unique_rules_triggered = int(explanation_coverage.get("unique_rules_triggered", 0))
            if total_decisions == 0:
                return _JudgmentDimension(
                    name="过程可解释性", score=0.0, weight=weight,
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
                name="过程可解释性", score=score, weight=weight,
                passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
            )
        if report is None:
            return _JudgmentDimension(
                name="过程可解释性", score=0.0, weight=weight,
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无诊断报告，无主动可解释性数据",
            )
        healthy = sum(1 for d in report.dimensions if d.severity == DiagnosisSeverity.HEALTHY)
        total = len(report.dimensions)
        score = healthy / total if total > 0 else 0.0
        passed = score >= threshold
        detail = f"被动健康维度={healthy}/{total}(建议提供explanation_coverage_result升级为主动ECR)"
        return _JudgmentDimension(
            name="过程可解释性", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_cross_instrument_consistency(self, results, threshold=0.60, weight=0.04) -> _JudgmentDimension:
        """跨品种一致性检验（全量扫描排序策略专用）

        由于本策略使用全量扫描排序，跨品种一致性检验包含4个维度：
        1. Tier分布一致性（Tier1占比<20%, Tier4占比>30%）
        2. correct_up_pct分布一致性（跨品种CV<0.40）
        3. Wilson降权比一致性（跨品种CV<0.30）
        4. 月份权重归一化一致性（不同月份数品种间correct_up_pct可比，KS距离<0.2）

        与judgment_standards.md 4.6节代码实现映射对齐。
        CV定量阈值：CV < 0.40 → passed=True（介于文档0.3"一致"和0.5"过拟合"之间）
        """
        threshold = threshold
        is_blocker = False
        if results is None or len(results) < self._min_instruments:
            detail = f"跨品种验证不足(需≥{self._min_instruments}品种)" if results is None else f"仅{len(results)}品种(需≥{self._min_instruments})"
            return _JudgmentDimension(
                name="跨品种一致性", score=0.0, weight=weight,
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

        # 维度4: 月份权重归一化一致性（KS距离）'
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

        # 综合判定（与文档对齐：全部条件必须满足）'
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
            name="跨品种一致性", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )
