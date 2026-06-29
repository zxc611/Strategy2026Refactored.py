# [M3-06] 并列信号源A/B/C评判服务
# MODULE_ID: M3-629
"""Signal Source A/B/C Judgment Service

依据最终落地方案v2.0：三个并列独立信号源评判
  - 信号源一致性（A/B/C 信号重合度）
  - Calmar比率（选定信号源的回测Calmar）
  - Diebold-Mariano检验p值（选定信号源 vs 次优源统计显著性）

输入格式:
    {
        "selected_source": "C",                 # 选定的信号源
        "source_consistency": 0.85,            # A/B/C信号一致性 [0,1]
        "calmar_ratio": 1.2,                   # 选定信号源回测Calmar
        "dmcv_pvalue": 0.03,                   # Diebold-Mariano检验p值
        "source_a_signal_count": 120,          # 信号源A信号数
        "source_b_signal_count": 95,           # 信号源B信号数
        "source_c_signal_count": 80,           # 信号源C信号数
        "ab_test_significant": True,           # A/B测试是否统计显著
    }
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np

from .judgment_types import (
    _JudgmentDimension,
    DIM_THREE_LAYER_DECISION_SOURCE,
    DIM_DISPLAY_NAMES,
    SCORING_COEFFICIENTS,
)
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class SignalSourceABCJudger:
    """并列信号源A/B/C评判服务。

    依据最终落地方案v2.0，对三个并列独立信号源进行量化评判：
      1. 信号源一致性（source_consistency）：衡量A/B/C信号重合度
      2. Calmar比率（calmar_ratio）：选定信号源的回测Calmar
      3. Diebold-Mariano检验p值（dmcv_pvalue）：信号源间统计显著性
    """

    def __init__(self, scoring_coefficients=None):
        self.SCORING_COEFFICIENTS = scoring_coefficients or SCORING_COEFFICIENTS

    def judge_signal_source(
        self,
        result: Optional[Dict[str, Any]],
        threshold: float = 0.60,
        weight: float = 0.05,
    ) -> _JudgmentDimension:
        """并列信号源A/B/C评判。"""
        is_blocker = False

        if result is None:
            return _JudgmentDimension(
            name=DIM_DISPLAY_NAMES.get(DIM_THREE_LAYER_DECISION_SOURCE, "并列信号源"),
                score=0.0,
                weight=weight,
                passed=False,
                threshold=threshold,
                is_blocker=is_blocker,
                detail="未提供信号源评判数据（signal_source未启用时此维度不评分）",
            )

        # 提取评分系数
        w_consistency = self.SCORING_COEFFICIENTS.get("tl_source_consistency_w", 0.40)
        w_calmar = self.SCORING_COEFFICIENTS.get("tl_calmar_w", 0.30)
        w_dmcv = self.SCORING_COEFFICIENTS.get("tl_dmcv_pvalue_w", 0.30)
        calmar_full_at = self.SCORING_COEFFICIENTS.get("tl_calmar_full_at", 1.0)
        dmcv_full_at = self.SCORING_COEFFICIENTS.get("tl_dmcv_pvalue_full_at", 0.05)

        # 1. 三层信号一致性评分 [0, 1]
        consistency = float(result.get("source_consistency", 0.0))
        consistency = max(0.0, min(1.0, consistency))
        consistency_score = consistency

        # 2. Calmar比率评分（归一化到[0,1]）
        calmar = float(result.get("calmar_ratio", 0.0))
        calmar_score = max(0.0, min(1.0, calmar / calmar_full_at)) if calmar_full_at > 0 else 0.0

        # 3. Diebold-Mariano检验p值评分（p值越小越好，归一化到[0,1]）
        dmcv_pvalue = float(result.get("dmcv_pvalue", 1.0))
        if dmcv_full_at > 0:
            # p=0 -> 满分1.0; p>=dmcv_full_at -> 0分
            dmcv_score = max(0.0, min(1.0, 1.0 - (dmcv_pvalue / dmcv_full_at)))
        else:
            dmcv_score = 0.0

        # 加权综合评分
        score = (
            w_consistency * consistency_score
            + w_calmar * calmar_score
            + w_dmcv * dmcv_score
        )
        score = float(np.clip(score, 0.0, 1.0))

        passed = score >= threshold

        # 构建详情
        detail_parts = [
            f"一致性={consistency_score:.2f}",
            f"Calmar={calmar_score:.2f}",
            f"DM_p={dmcv_score:.2f}",
            f"综合={score:.2f}",
        ]

        selected_source = result.get("selected_source", "unknown")
        detail_parts.append(f"选定源={selected_source}")

        # 信号数检查
        l1_count = result.get("source_a_signal_count", 0)
        l2_count = result.get("source_b_signal_count", 0)
        l3_count = result.get("source_c_signal_count", 0)
        total_signals = l1_count + l2_count + l3_count
        if total_signals < 30:
            detail_parts.append("⚠样本不足")
        if not result.get("ab_test_significant", False):
            detail_parts.append("⚠A/B未显著")

        detail = ", ".join(detail_parts)

        return _JudgmentDimension(
            name=DIM_DISPLAY_NAMES.get(DIM_THREE_LAYER_DECISION_SOURCE, "三层决策源"),
            score=score,
            weight=weight,
            passed=passed,
            threshold=threshold,
            is_blocker=is_blocker,
            detail=detail,
        )
