# [M3-06] 预测校准评判服务
# MODULE_ID: M3-615
"""Prediction Calibration Judgment Service"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np

from .judgment_types import (_JudgmentDimension, SCORING_COEFFICIENTS)
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class CalibrationJudger:
    """Prediction calibration judgment service."""

    def __init__(self, scoring_coefficients=None):
        self.SCORING_COEFFICIENTS = scoring_coefficients or SCORING_COEFFICIENTS

    def judge_prediction_calibration(self, accuracy, threshold=0.50, weight=0.05) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if accuracy is None:
            return _JudgmentDimension(
                name="预测能力校准", score=0.0, weight=weight,
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="无共振预测数据",
            )
        hit = accuracy.get("hit_rate", 0.0)
        dir_acc = accuracy.get("direction_accuracy", 0.0)
        score = self.SCORING_COEFFICIENTS["predict_hit_w"] * hit + self.SCORING_COEFFICIENTS["predict_dir_w"] * dir_acc
        passed = score >= threshold
        detail = f"命中率={hit:.2f}, 方向准确率={dir_acc:.2f}"
        return _JudgmentDimension(
            name="预测能力校准", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_parameter_stability(self, result, threshold=0.50, weight=0.05) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="参数稳定性", score=0.0, weight=weight,
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
            name="参数稳定性", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_return_source_diversification(self, result, threshold=0.50, weight=0.06) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="收益来源分散度", score=0.0, weight=weight,
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
            name="收益来源分散度", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def judge_cross_strategy_correlation(self, result, threshold=0.70, weight=0.07) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if result is None:
            return _JudgmentDimension(
                name="策略间相关性", score=0.0, weight=weight,
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
                name="策略间相关性", score=0.0, weight=weight,
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
            name="策略间相关性", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )
