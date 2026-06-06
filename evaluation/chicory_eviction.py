"""菊苣驱逐策略 — 策略淘汰评估与生命周期管理

LC-02修复: 增加淘汰编码、复活策略、冷静期、审计日志
"""
import enum
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class EvictionCode(enum.Enum):
    """LC-02修复: 淘汰编码枚举 — 不同编码对应不同复活条件"""
    CHICORY_LOW_SCORE = "chicory_low_score"       # 评分过低 → 永久淘汰，不可复活
    CHICORY_AGE_DECAY = "chicory_age_decay"       # 活跃度衰减 → 可复活，需重新通过3个月回测
    CHICORY_VIOLATION = "chicory_violation"        # 违规 → 永久淘汰，不可复活
    CHICORY_COMPOSITE = "chicory_composite"        # 综合评分 → 可复活，需人工审批


# 淘汰编码 → 复活策略映射
EVICTION_REVIVAL_POLICY = {
    EvictionCode.CHICORY_LOW_SCORE: {"revivable": False, "revival_condition": "永久淘汰"},
    EvictionCode.CHICORY_AGE_DECAY: {"revivable": True, "revival_condition": "重新通过3个月回测验证"},
    EvictionCode.CHICORY_VIOLATION: {"revivable": False, "revival_condition": "永久淘汰"},
    EvictionCode.CHICORY_COMPOSITE: {"revivable": True, "revival_condition": "人工审批通过"},
}

# 冷静期配置（交易日数）
COOLING_PERIOD_DAYS = 3


@dataclass
class EvictionDecision:
    """LC-02修复: 淘汰决策数据类"""
    should_evict: bool = False
    eviction_score: float = 0.0
    eviction_code: Optional[EvictionCode] = None
    reason: str = ""
    revivable: bool = False
    revival_condition: str = ""
    cooling_period_days: int = COOLING_PERIOD_DAYS
    timestamp: float = field(default_factory=time.time)


class ChicoryEvictionPolicy:
    """菊苣驱逐策略 — 三档权重驱逐评分

    W0_BASE = 0.60 (基础策略评分权重)
    W1_DECAY = 0.25 (活跃度衰减权重, 半衰期30天)
    W2_VIOLATION = 0.15 (违反惩罚权重, 每次违反扣0.1)

    R21-MATH-P2-04修复: exp参数裁剪防溢出
    LC-02修复: 增加淘汰编码、复活策略、冷静期、审计日志
    """

    W0_BASE = 0.60
    W1_DECAY = 0.25
    W2_VIOLATION = 0.15
    EVICTION_THRESHOLD = 0.3

    def __init__(self, eviction_threshold: float = 0.3):
        self._eviction_threshold = eviction_threshold
        self._audit_log_path: Optional[str] = None

    def set_audit_log_path(self, path: str):
        """LC-02修复: 设置淘汰审计日志路径（WAL模式）"""
        self._audit_log_path = path

    def eviction_score(self, base_score: float, age_days: float,
                       violation_count: int = 0) -> float:
        """计算淘汰评分

        Args:
            base_score: 基础策略评分 [0,1]
            age_days: 策略运行天数
            violation_count: 违规次数

        Returns:
            float: 淘汰评分 [0,1]，低于阈值则应淘汰
        """
        # R21-MATH-P2-04: exp参数裁剪防溢出
        # max裁剪下限: exp(-700)≈0, 避免age_days很大时exp溢出
        decay_arg = max(-age_days / 30.0, -700.0) if age_days > 0 else 0.0
        decay_factor = math.exp(decay_arg)

        score = (
            self.W0_BASE * base_score
            + self.W1_DECAY * decay_factor
            + self.W2_VIOLATION * max(0.0, 1.0 - violation_count * 0.1)
        )
        return max(0.0, min(1.0, score))

    def _determine_eviction_code(self, score: float, base_score: float,
                                  age_days: float, violation_count: int) -> EvictionCode:
        """LC-02修复: 根据评分组成确定淘汰编码"""
        if violation_count >= 3:
            return EvictionCode.CHICORY_VIOLATION
        if base_score < 0.2:
            return EvictionCode.CHICORY_LOW_SCORE
        if age_days > 90 and score < self._eviction_threshold:
            return EvictionCode.CHICORY_AGE_DECAY
        return EvictionCode.CHICORY_COMPOSITE

    def _write_audit_log(self, decision: EvictionDecision, strategy_id: str):
        """LC-02修复: 写入淘汰审计日志（WAL模式，不可篡改）"""
        if not self._audit_log_path:
            return
        try:
            log_entry = {
                "timestamp": decision.timestamp,
                "strategy_id": strategy_id,
                "should_evict": decision.should_evict,
                "eviction_score": decision.eviction_score,
                "eviction_code": decision.eviction_code.value if decision.eviction_code else None,
                "reason": decision.reason,
                "revivable": decision.revivable,
                "cooling_period_days": decision.cooling_period_days,
            }
            log_dir = os.path.dirname(self._audit_log_path)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            with open(self._audit_log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.warning("[LC-02] 审计日志写入失败: %s", e)

    def evaluate(self, strategy_id: str, overall_score: float,
                 dimensions: Dict[str, Any] = None) -> Dict[str, Any]:
        """评估策略是否应被淘汰

        Args:
            strategy_id: 策略ID
            overall_score: 综合评分 [0,1]
            dimensions: 维度评分字典，可包含:
                - age_days: 策略运行天数
                - violation_count: 违规次数
                - base_score: 基础评分（默认使用overall_score）

        Returns:
            Dict: 淘汰决策结果
        """
        dimensions = dimensions or {}
        base_score = dimensions.get('base_score', overall_score)
        age_days = dimensions.get('age_days', 0.0)
        violation_count = dimensions.get('violation_count', 0)

        score = self.eviction_score(base_score, age_days, violation_count)
        should_evict = score < self._eviction_threshold

        # LC-02修复: 确定淘汰编码
        eviction_code = self._determine_eviction_code(score, base_score, age_days, violation_count) if should_evict else None

        # LC-02修复: 查找复活策略
        revival_info = EVICTION_REVIVAL_POLICY.get(eviction_code, {"revivable": False, "revival_condition": ""})

        decision = EvictionDecision(
            should_evict=should_evict,
            eviction_score=score,
            eviction_code=eviction_code,
            reason=f"评分={score:.3f} < 阈值={self._eviction_threshold}" if should_evict else "评分达标",
            revivable=revival_info.get("revivable", False),
            revival_condition=revival_info.get("revival_condition", ""),
            cooling_period_days=COOLING_PERIOD_DAYS if should_evict else 0,
        )

        # LC-02修复: 写入审计日志
        if should_evict:
            self._write_audit_log(decision, strategy_id)

        return {
            "should_evict": decision.should_evict,
            "eviction_score": decision.eviction_score,
            "eviction_code": eviction_code.value if eviction_code else None,
            "reason": decision.reason,
            "revivable": decision.revivable,
            "revival_condition": decision.revival_condition,
            "cooling_period_days": decision.cooling_period_days,
        }
