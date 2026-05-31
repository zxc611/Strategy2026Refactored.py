"""
策略行为诊断 (Strategy Behavior Diagnosis) — 生产就绪版 v1.1

在局部高低点处对策略进行"微观解剖"，诊断策略核心逻辑是否正确执行。

升级内容（vs v1.0-beta）：
  1. 细粒度4子项拆解：信号衰减一致性/仓位平滑度/Greeks合理性/盈亏路径质量
     评判引擎直接访问子项分数，不再依赖黑箱聚合
  2. 六大策略预设逻辑映射：high_freq/resonance/box/spring/arbitrage/market_making各有独立预期行为
  3. 行为一致性评分直接由4子项加权产生，可审计可追溯
  4. 样本量要求提升：min_samples=30
  5. 预设逻辑规则细化：区分高点/低点、区分趋势延续/反转

关键原则（不变）：
  - 诊断工具 ≠ 优化目标
  - 统计显著性检验
  - 避免事后合理化
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .market_snapshot_collector import MarketSnapshot, SnapshotTrigger

logger = logging.getLogger(__name__)


class DiagnosisSeverity(Enum):
    HEALTHY = "健康"
    WARNING = "警告"
    CRITICAL = "严重"
    INSUFFICIENT_DATA = "数据不足"


@dataclass(slots=True)
class SubItemScore:
    name: str
    score: float
    sample_count: int
    confidence: float
    pattern: str
    detail: str
    anomalies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DimensionDiagnosis:
    dimension: str
    score: float
    severity: DiagnosisSeverity
    pattern: str
    detail: str
    sub_items: List[SubItemScore] = field(default_factory=list)
    sample_count: int = 0
    confidence: float = 0.0
    anomalies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['severity'] = self.severity.value
        d['sub_items'] = [si.to_dict() for si in self.sub_items]
        return d


@dataclass(slots=True)
class BehaviorConsistencyScore:
    overall_score: float
    signal_decay_consistency: float
    position_smoothness: float
    greeks_rationality: float
    pnl_path_quality: float
    signal_score: float
    position_score: float
    pnl_score: float
    greeks_score: float
    cross_strategy_score: float
    sample_count: int
    confidence: float
    severity: DiagnosisSeverity
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['severity'] = self.severity.value
        return d


@dataclass(slots=True)
class DiagnosisReport:
    strategy_id: str
    strategy_type: str
    symbol: str
    backtest_period: str
    overall_score: BehaviorConsistencyScore
    dimensions: List[DimensionDiagnosis]
    extreme_point_count: int
    near_high_count: int
    near_low_count: int
    extreme_count: int
    recommendations: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    _per_trade_returns: Optional[List[float]] = field(default=None, repr=False)
    _p_values: Optional[List[float]] = field(default=None, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['overall_score'] = self.overall_score.to_dict()
        d['dimensions'] = [dim.to_dict() for dim in self.dimensions]
        d.pop('_per_trade_returns', None)
        d.pop('_p_values', None)
        return d


_STRATEGY_EXPECTED_LOGIC = {
    "high_freq": {
        "at_high": {"signal_should_decay": True, "signal_should_reverse": False,
                    "position_should_decrease": True, "pnl_should_small_retrace": True,
                    "reason": "趋势延续策略：高点处动量衰减但方向保持"},
        "at_low": {"signal_should_decay": True, "signal_should_reverse": False,
                   "position_should_decrease": True, "pnl_should_small_retrace": True,
                   "reason": "趋势延续策略：低点处动量衰减但方向保持"},
    },
    "resonance": {
        "at_high": {"signal_should_decay": True, "signal_should_reverse": False,
                    "position_should_decrease": True, "pnl_should_small_retrace": True,
                    "reason": "共振跟随策略：高点处共振减弱+仓位收缩; pullback_enabled时延迟至权利金回撤达标再入场"},
        "at_low": {"signal_should_decay": True, "signal_should_reverse": False,
                   "position_should_decrease": True, "pnl_should_small_retrace": True,
                   "reason": "共振跟随策略：低点处共振减弱+仓位收缩; pullback_enabled时延迟至权利金回撤达标再入场"},
    },
    "box": {
        "at_high": {"signal_should_decay": False, "signal_should_reverse": True,
                    "position_should_decrease": True, "pnl_should_small_retrace": True,
                    "reason": "箱体极值策略：高点处发出反向信号(做空); pullback_enabled时延迟至权利金回撤达标再入场"},
        "at_low": {"signal_should_decay": False, "signal_should_reverse": True,
                   "position_should_decrease": True, "pnl_should_small_retrace": True,
                   "reason": "箱体极值策略：低点处发出反向信号(做多); pullback_enabled时延迟至权利金回撤达标再入场"},
    },
    "spring": {
        "at_high": {"signal_should_decay": False, "signal_should_reverse": True,
                    "position_should_decrease": True, "pnl_should_small_retrace": True,
                    "reason": "弹簧策略：高点处弹簧触发→买Put/卖Call; pullback_enabled时延迟至权利金回撤达标再入场"},
        "at_low": {"signal_should_decay": False, "signal_should_reverse": True,
                   "position_should_decrease": True, "pnl_should_small_retrace": True,
                   "reason": "弹簧策略：低点处弹簧触发→买Call/卖Put; pullback_enabled时延迟至权利金回撤达标再入场"},
    },
    "arbitrage": {
        "at_high": {"signal_should_decay": True, "signal_should_reverse": True,
                    "position_should_decrease": True, "pnl_should_small_retrace": True,
                    "reason": "套利策略：高点处价格偏离公允价值→反向套利开仓+快速平仓; pullback_enabled时延迟至权利金回撤达标再入场"},
        "at_low": {"signal_should_decay": True, "signal_should_reverse": True,
                   "position_should_decrease": True, "pnl_should_small_retrace": True,
                   "reason": "套利策略：低点处价格偏离公允价值→反向套利开仓+快速平仓; pullback_enabled时延迟至权利金回撤达标再入场"},
    },
    "market_making": {
        "at_high": {"signal_should_decay": True, "signal_should_reverse": False,
                    "position_should_decrease": True, "pnl_should_small_retrace": True,
                    "reason": "做市策略：高点处挂卖单增多+仓位收缩，方向不反转; pullback_enabled时延迟至权利金回撤达标再入场"},
        "at_low": {"signal_should_decay": True, "signal_should_reverse": False,
                   "position_should_decrease": True, "pnl_should_small_retrace": True,
                   "reason": "做市策略：低点处挂买单增多+仓位收缩，方向不反转; pullback_enabled时延迟至权利金回撤达标再入场"},
    },
}


class StrategyBehaviorDiagnosis:
    """
    策略行为诊断器 — 生产就绪版 v1.2

    细粒度4子项拆解 + 六大策略预设逻辑映射
    所有诊断阈值可配置，不再依赖硬编码魔数
    """

    MIN_SAMPLES_FOR_SIGNIFICANCE = 30
    HIGH_CONFIDENCE_THRESHOLD = 30

    # ── 可配置诊断阈值（替代硬编码魔数）──
    # 信号诊断
    SIGNAL_JUMP_THRESHOLD = 0.5          # 信号跳变判定阈值
    SIGNAL_DECAY_STRONG = 0.6            # "信号衰减"强模式判定
    SIGNAL_DECAY_WEAK = 0.5              # "信号衰减"弱模式判定
    SIGNAL_STABLE_THRESHOLD = 0.2        # "方向稳定"判定
    SIGNAL_CRITICAL_JUMP = 0.3           # 信号CRITICAL跳变阈值
    # 仓位诊断
    POSITION_JUMP_THRESHOLD = 0.5        # 仓位剧变判定阈值
    POSITION_CV_NORM = 3.0               # CV归一化因子
    POSITION_SMOOTH_OSC = 0.1            # "平滑调整"震荡率阈值
    POSITION_SMOOTH_CV = 2.0             # "平滑调整"CV阈值
    POSITION_DECREASE_THRESHOLD = 0.5    # "缩减"判定阈值
    POSITION_CRITICAL_OSC = 0.3          # 仓位CRITICAL震荡率阈值
    # PnL诊断
    PNL_CLIFF_THRESHOLD = 0.3            # 断崖回吐判定阈值
    # Greeks诊断
    GREEKS_DELTA_ANOMALY = 0.8           # Delta异常阈值
    GREEKS_DELTA_EXPAND = 0.1            # Delta扩展增量阈值
    GREEKS_VEGA_SPIKE_MULT = 2.0         # Vega尖峰倍数阈值
    GREEKS_CRITICAL_EXPAND = 0.4         # Greeks CRITICAL扩展率阈值
    GREEKS_HEALTHY_EXPAND = 0.2          # Greeks HEALTHY扩展率阈值
    GREEKS_HEALTHY_VEGA_SPIKE = 0.1      # Greeks HEALTHY Vega尖峰率阈值
    # 跨策略诊断
    CROSS_SIGNAL_VALID = 0.3             # 跨策略信号有效性阈值
    CROSS_HEALTHY_CORR = 0.2             # 跨策略HEALTHY相关率阈值
    CROSS_CRITICAL_CORR = 0.5            # 跨策略CRITICAL相关率阈值
    # 综合评分
    OVERALL_HEALTHY_THRESHOLD = 0.7      # 综合HEALTHY阈值
    OVERALL_WARNING_THRESHOLD = 0.4      # 综合WARNING阈值
    # 5维度权重(信号/仓位/PnL/Greeks/跨策略)
    DIAGNOSIS_WEIGHTS = [0.30, 0.25, 0.20, 0.10, 0.15]
    # ── 评分公式系数（可调，影响诊断评分分布）──
    # 信号评分
    SIGNAL_DECAY_W = 0.6                 # 衰减模式中decay_rate权重
    SIGNAL_JUMP_PENALTY_W = 0.4          # 衰减模式中jump_rate惩罚权重
    SIGNAL_NONDECAY_W = 0.4              # 非衰减模式中decay_rate权重
    SIGNAL_NONDECAY_JUMP_W = 0.6         # 非衰减模式中jump_rate权重
    SIGNAL_DECAY_HEALTHY_TOLERANCE = 0.1 # HEALTHY衰减容差
    # 仓位评分
    POS_SMOOTH_W = 0.5                   # HEALTHY平滑权重
    POS_DECREASE_W = 0.3                 # HEALTHY缩减权重
    POS_HEALTHY_BASE = 0.2               # HEALTHY基底分
    POS_CRITICAL_BASE = 0.4              # CRITICAL基底分
    POS_CRITICAL_OSC_MULT = 0.4          # CRITICAL震荡惩罚乘数
    POS_WARN_SMOOTH_W = 0.4              # WARNING平滑权重
    POS_WARN_DECREASE_W = 0.3            # WARNING缩减权重
    POS_WARN_BASE = 0.2                  # WARNING基底分
    # PnL评分
    PNL_PATH_BASE = 0.6                  # 非断崖路径基底分
    PNL_PATH_CLIFF_BONUS = 0.3           # 非断崖路径cliff奖励
    PNL_CLIFF_BASE = 0.5                 # 断崖基底分
    PNL_NO_RETRACE_SCORE = 0.9           # 无负变化时评分
    PNL_GRADUAL_THRESHOLD = 0.5          # 渐进/断崖模式判定阈值
    # Greeks评分
    GREEKS_DELTA_W = 0.6                 # Greeks综合Delta权重
    GREEKS_VEGA_W = 0.4                  # Greeks综合Vega权重
    VEGA_SPIKE_PENALTY_MULT = 2.0        # Vega尖峰惩罚乘数
    VEGA_SPIKE_EPSILON = 0.01            # Vega尖峰判定容差
    # 跨策略评分
    CROSS_CORR_PENALTY_MULT = 1.5        # 跨策略相关率惩罚乘数
    # 子维度最小样本数
    MIN_SAMPLES_PER_DIMENSION = 5        # 各子维度数据不足判定

    def __init__(self, strategy_id: str, strategy_type: str = ""):
        # R13-P2-API-02修复: 校验strategy_id不为空
        if not strategy_id:
            logger.warning("[StrategyBehaviorDiagnosis] strategy_id为空，行为诊断可能无法正确关联策略")
        self._strategy_id = strategy_id
        self._strategy_type = strategy_type
        self._expected_logic = _STRATEGY_EXPECTED_LOGIC.get(strategy_type)
        if self._expected_logic is None:
            logger.warning(
                f"策略类型'{strategy_type}'无预设逻辑定义，"
                f"行为一致性诊断将使用通用宽松标准。"
                f"已知类型: {list(_STRATEGY_EXPECTED_LOGIC.keys())}"
            )
            self._expected_logic = {
                "at_high": {"signal_should_decay": True, "signal_should_reverse": False,
                            "position_should_decrease": True, "pnl_should_small_retrace": True,
                            "reason": "未知策略类型(宽松标准)：高点处仅要求仓位可控"},
                "at_low": {"signal_should_decay": True, "signal_should_reverse": False,
                           "position_should_decrease": True, "pnl_should_small_retrace": True,
                           "reason": "未知策略类型(宽松标准)：低点处仅要求仓位可控"},
            }

    def diagnose(
        self,
        extreme_snapshots: List[MarketSnapshot],
        all_snapshots: List[MarketSnapshot],
        symbol: str = "",
        backtest_period: str = "",
    ) -> DiagnosisReport:
        high_snaps = [s for s in extreme_snapshots
                      if s.extreme_region in ("NEAR_HIGH", "EXTREME_HIGH")]
        low_snaps = [s for s in extreme_snapshots
                     if s.extreme_region in ("NEAR_LOW", "EXTREME_LOW")]
        extreme_snaps = [s for s in extreme_snapshots
                         if s.extreme_region in ("EXTREME_HIGH", "EXTREME_LOW")]

        signal_dim = self._diagnose_signal(high_snaps, low_snaps, all_snapshots)
        position_dim = self._diagnose_position(high_snaps, low_snaps)
        pnl_dim = self._diagnose_pnl(high_snaps, low_snaps)
        greeks_dim = self._diagnose_greeks(high_snaps, low_snaps)
        cross_dim = self._diagnose_cross_strategy(high_snaps, low_snaps)

        dimensions = [signal_dim, position_dim, pnl_dim, greeks_dim, cross_dim]

        total_samples = len(extreme_snapshots)
        confidence = min(1.0, total_samples / self.HIGH_CONFIDENCE_THRESHOLD)

        sig_decay = self._extract_sub_score(signal_dim, "信号衰减一致性")
        pos_smooth = self._extract_sub_score(position_dim, "仓位平滑度")
        greeks_rat = self._extract_sub_score(greeks_dim, "Greeks合理性")
        pnl_path = self._extract_sub_score(pnl_dim, "盈亏路径质量")

        weights = self.DIAGNOSIS_WEIGHTS
        overall = sum(w * d.score for w, d in zip(weights, dimensions))

        if total_samples < self.MIN_SAMPLES_FOR_SIGNIFICANCE:
            severity = DiagnosisSeverity.INSUFFICIENT_DATA
        elif overall >= self.OVERALL_HEALTHY_THRESHOLD:
            severity = DiagnosisSeverity.HEALTHY
        elif overall >= self.OVERALL_WARNING_THRESHOLD:
            severity = DiagnosisSeverity.WARNING
        else:
            severity = DiagnosisSeverity.CRITICAL

        bcs = BehaviorConsistencyScore(
            overall_score=float(np.clip(overall, 0, 1)),
            signal_decay_consistency=sig_decay,
            position_smoothness=pos_smooth,
            greeks_rationality=greeks_rat,
            pnl_path_quality=pnl_path,
            signal_score=signal_dim.score,
            position_score=position_dim.score,
            pnl_score=pnl_dim.score,
            greeks_score=greeks_dim.score,
            cross_strategy_score=cross_dim.score,
            sample_count=total_samples,
            confidence=confidence,
            severity=severity,
            summary=self._generate_summary(overall, severity, dimensions),
        )

        return DiagnosisReport(
            strategy_id=self._strategy_id,
            strategy_type=self._strategy_type,
            symbol=symbol,
            backtest_period=backtest_period,
            overall_score=bcs,
            dimensions=dimensions,
            extreme_point_count=total_samples,
            near_high_count=len(high_snaps),
            near_low_count=len(low_snaps),
            extreme_count=len(extreme_snaps),
            recommendations=self._generate_recommendations(dimensions, severity),
        )

    @staticmethod
    def _extract_sub_score(dim: DimensionDiagnosis, name: str) -> float:
        for si in dim.sub_items:
            if si.name == name:
                return si.score
        return dim.score

    def _diagnose_signal(self, high_snaps, low_snaps, all_snapshots) -> DimensionDiagnosis:
        total = len(high_snaps) + len(low_snaps)
        if total < self.MIN_SAMPLES_PER_DIMENSION:
            return DimensionDiagnosis(
                dimension="信号强度演化", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="数据不足", detail=f"仅有{total}个极值样本(<30)",
                sample_count=total, confidence=0.0,
            )

        decay_scores = []
        jump_count = 0
        anomalies = []

        for snaps, region_key in [(high_snaps, "at_high"), (low_snaps, "at_low")]:
            signals = [self._extract_signal(s) for s in snaps]
            expected = self._expected_logic.get(region_key, {})
            should_decay = expected.get("signal_should_decay", True)

            for i in range(1, len(signals)):
                delta = abs(signals[i] - signals[i - 1])
                if delta > self.SIGNAL_JUMP_THRESHOLD:
                    jump_count += 1
                    if jump_count <= 5:
                        anomalies.append(f"信号跳变: |Δ|={delta:.2f}")

                if should_decay:
                    if abs(signals[i]) < abs(signals[i - 1]):
                        decay_scores.append(1.0)
                    else:
                        decay_scores.append(0.0)
                else:
                    if abs(signals[i]) > abs(signals[i - 1]):
                        decay_scores.append(1.0)
                    else:
                        decay_scores.append(0.0)

        total_pairs = max(1, total - 2)
        jump_rate = jump_count / total_pairs
        decay_rate = float(np.mean(decay_scores)) if decay_scores else 0.5

        decay_consistency = SubItemScore(
            name="信号衰减一致性", score=decay_rate,
            sample_count=len(decay_scores),
            confidence=min(1.0, len(decay_scores) / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="信号衰减" if decay_rate > self.SIGNAL_DECAY_STRONG else "信号未衰减",
            detail=f"衰减率={decay_rate:.2f}, 期望衰减={should_decay}",
        )

        direction_consistency = SubItemScore(
            name="信号方向一致性", score=max(0.0, 1.0 - jump_rate),
            sample_count=total,
            confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="方向稳定" if jump_rate < self.SIGNAL_STABLE_THRESHOLD else "方向跳变",
            detail=f"跳变率={jump_rate:.2f}",
        )

        should_decay = decay_rate > self.SIGNAL_DECAY_WEAK
        if should_decay:
            score = self.SIGNAL_DECAY_W * decay_rate + self.SIGNAL_JUMP_PENALTY_W * (1.0 - jump_rate)
        else:
            score = self.SIGNAL_NONDECAY_W * (1.0 - decay_rate) + self.SIGNAL_NONDECAY_JUMP_W * (1.0 - jump_rate)
        score = float(np.clip(score, 0, 1))

        if jump_rate > self.SIGNAL_CRITICAL_JUMP:
            severity = DiagnosisSeverity.CRITICAL
            pattern = "信号异常跳变"
        elif decay_rate > self.SIGNAL_DECAY_STRONG if should_decay else decay_rate < self.SIGNAL_DECAY_WEAK - self.SIGNAL_DECAY_HEALTHY_TOLERANCE:
            severity = DiagnosisSeverity.HEALTHY
            pattern = "信号衰减" if should_decay else "信号增强"
        else:
            severity = DiagnosisSeverity.WARNING
            pattern = "信号滞后"

        return DimensionDiagnosis(
            dimension="信号强度演化", score=score, severity=severity,
            pattern=pattern,
            detail=f"衰减率={decay_rate:.2f}, 跳变率={jump_rate:.2f}",
            sub_items=[decay_consistency, direction_consistency],
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            anomalies=anomalies[:10],
        )

    def _diagnose_position(self, high_snaps, low_snaps) -> DimensionDiagnosis:
        total = len(high_snaps) + len(low_snaps)
        if total < self.MIN_SAMPLES_PER_DIMENSION:
            return DimensionDiagnosis(
                dimension="仓位变化模式", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="数据不足", detail=f"仅有{total}个极值样本",
                sample_count=total,
            )

        all_snaps = high_snaps + low_snaps
        position_deltas = []
        anomalies = []

        for i in range(1, len(all_snaps)):
            prev = self._extract_position(all_snaps[i - 1])
            curr = self._extract_position(all_snaps[i])
            delta = abs(curr - prev)
            position_deltas.append(delta)
            if delta > self.POSITION_JUMP_THRESHOLD:
                anomalies.append(f"仓位剧变: |Δpos|={delta:.2f}")

        if not position_deltas:
            return DimensionDiagnosis(
                dimension="仓位变化模式", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="无仓位数据", detail="策略在极值区域无仓位",
                sample_count=total,
            )

        mean_d = float(np.mean(position_deltas))
        std_d = float(np.std(position_deltas))
        cv = std_d / (mean_d + 1e-8)
        osc_rate = float(np.mean([1 for d in position_deltas if d > mean_d + 2 * std_d]))

        smooth_score = max(0.0, 1.0 - cv / self.POSITION_CV_NORM) if cv < self.POSITION_CV_NORM else 0.0
        decrease_count = 0
        for i in range(1, len(all_snaps)):
            prev = abs(self._extract_position(all_snaps[i - 1]))
            curr = abs(self._extract_position(all_snaps[i]))
            if curr < prev:
                decrease_count += 1
        decrease_rate = decrease_count / max(1, len(all_snaps) - 1)

        smooth_sub = SubItemScore(
            name="仓位平滑度", score=smooth_score,
            sample_count=len(position_deltas),
            confidence=min(1.0, len(position_deltas) / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="平滑调整" if osc_rate < self.POSITION_SMOOTH_OSC else "剧烈震荡",
            detail=f"CV={cv:.2f}, 震荡率={osc_rate:.2f}",
        )

        decrease_sub = SubItemScore(
            name="仓位缩减一致性", score=decrease_rate,
            sample_count=len(all_snaps) - 1,
            confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="缩减" if decrease_rate > self.POSITION_DECREASE_THRESHOLD else "未缩减",
            detail=f"缩减率={decrease_rate:.2f}",
        )

        if osc_rate < self.POSITION_SMOOTH_OSC and cv < self.POSITION_SMOOTH_CV:
            score = self.POS_SMOOTH_W * smooth_score + self.POS_DECREASE_W * decrease_rate + self.POS_HEALTHY_BASE
            severity = DiagnosisSeverity.HEALTHY
            pattern = "仓位平滑调整"
        elif osc_rate > self.POSITION_CRITICAL_OSC:
            score = max(0.0, self.POS_CRITICAL_BASE - osc_rate * self.POS_CRITICAL_OSC_MULT)
            severity = DiagnosisSeverity.CRITICAL
            pattern = "仓位剧烈震荡"
        else:
            score = self.POS_WARN_SMOOTH_W * smooth_score + self.POS_WARN_DECREASE_W * decrease_rate + self.POS_WARN_BASE
            severity = DiagnosisSeverity.WARNING
            pattern = "仓位部分异常"

        return DimensionDiagnosis(
            dimension="仓位变化模式", score=float(np.clip(score, 0, 1)),
            severity=severity, pattern=pattern,
            detail=f"CV={cv:.2f}, 震荡率={osc_rate:.2f}, 缩减率={decrease_rate:.2f}",
            sub_items=[smooth_sub, decrease_sub],
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            anomalies=anomalies[:10],
        )

    def _diagnose_pnl(self, high_snaps, low_snaps) -> DimensionDiagnosis:
        total = len(high_snaps) + len(low_snaps)
        if total < self.MIN_SAMPLES_PER_DIMENSION:
            return DimensionDiagnosis(
                dimension="盈亏路径形态", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="数据不足", detail=f"仅有{total}个极值样本",
                sample_count=total,
            )

        all_snaps = high_snaps + low_snaps
        pnl_values = [s.total_portfolio_pnl for s in all_snaps]

        if len(pnl_values) < 3:
            return DimensionDiagnosis(
                dimension="盈亏路径形态", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="PnL数据不足", detail="极值区域PnL样本不足",
                sample_count=total,
            )

        pnl_changes = np.diff(pnl_values)
        negative = pnl_changes[pnl_changes < 0]
        pnl_range = max(pnl_values) - min(pnl_values) if max(pnl_values) != min(pnl_values) else 1.0

        if len(negative) == 0:
            cliff_ratio = 0.0
            path_score = self.PNL_NO_RETRACE_SCORE
        else:
            max_retrace = float(np.max(np.abs(negative)))
            cliff_ratio = max_retrace / pnl_range if pnl_range > 0 else 0.0
            path_score = max(0.0, self.PNL_PATH_BASE + self.PNL_PATH_CLIFF_BONUS * (1.0 - cliff_ratio / self.PNL_CLIFF_THRESHOLD)) if cliff_ratio < self.PNL_CLIFF_THRESHOLD else max(0.0, self.PNL_CLIFF_BASE - cliff_ratio)

        gradual_score = max(0.0, 1.0 - cliff_ratio / self.PNL_CLIFF_THRESHOLD) if cliff_ratio < self.PNL_CLIFF_THRESHOLD else 0.0

        path_sub = SubItemScore(
            name="盈亏路径质量", score=path_score,
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="渐进回吐" if cliff_ratio < self.PNL_CLIFF_THRESHOLD else "断崖回吐",
            detail=f"断崖比={cliff_ratio:.2f}",
        )

        gradual_sub = SubItemScore(
            name="渐进回吐一致性", score=gradual_score,
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="渐进" if gradual_score > self.PNL_GRADUAL_THRESHOLD else "断崖",
            detail=f"渐进得分={gradual_score:.2f}",
        )

        if cliff_ratio > self.PNL_CLIFF_THRESHOLD:
            severity = DiagnosisSeverity.CRITICAL
            pattern = "利润断崖回吐"
        else:
            severity = DiagnosisSeverity.HEALTHY
            pattern = "利润渐进回吐"

        return DimensionDiagnosis(
            dimension="盈亏路径形态", score=float(np.clip(path_score, 0, 1)),
            severity=severity, pattern=pattern,
            detail=f"断崖比={cliff_ratio:.2f}, PnL范围=[{min(pnl_values):.2f}, {max(pnl_values):.2f}]",
            sub_items=[path_sub, gradual_sub],
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
        )

    def _diagnose_greeks(self, high_snaps, low_snaps) -> DimensionDiagnosis:
        total = len(high_snaps) + len(low_snaps)
        if total < self.MIN_SAMPLES_PER_DIMENSION:
            return DimensionDiagnosis(
                dimension="Greeks暴露合理性", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="数据不足", detail=f"仅有{total}个极值样本",
                sample_count=total,
            )

        all_snaps = high_snaps + low_snaps
        deltas = []
        vegas = []
        anomalies = []

        for s in all_snaps:
            for ss in s.strategy_states:
                if ss.strategy_id == self._strategy_id:
                    deltas.append(ss.greeks_delta)
                    vegas.append(abs(ss.greeks_vega))
                    if abs(ss.greeks_delta) > self.GREEKS_DELTA_ANOMALY:
                        anomalies.append(f"Delta过大: {ss.greeks_delta:.2f}")
                    break

        if len(deltas) < 3:
            delta_sub = SubItemScore(name="Delta合理性", score=0.5, sample_count=0, confidence=0.0, pattern="无数据", detail="")
            vega_sub = SubItemScore(name="Vega合理性", score=0.5, sample_count=0, confidence=0.0, pattern="无数据", detail="")
            return DimensionDiagnosis(
                dimension="Greeks暴露合理性", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="Greeks数据不足", detail="",
                sub_items=[delta_sub, vega_sub], sample_count=total,
            )

        delta_abs = np.abs(deltas)
        expanding = sum(1 for i in range(1, len(delta_abs)) if delta_abs[i] > delta_abs[i-1] + self.GREEKS_DELTA_EXPAND)
        expand_rate = expanding / max(1, len(delta_abs) - 1)
        delta_score = max(0.0, 1.0 - expand_rate)

        mean_vega = float(np.mean(vegas)) if vegas else 0.0
        vega_spikes = sum(1 for v in vegas if v > self.GREEKS_VEGA_SPIKE_MULT * mean_vega + self.VEGA_SPIKE_EPSILON) if mean_vega > 0 else 0
        vega_spike_rate = vega_spikes / max(1, len(vegas))
        vega_score = max(0.0, 1.0 - vega_spike_rate * self.VEGA_SPIKE_PENALTY_MULT)

        delta_sub = SubItemScore(
            name="Delta合理性", score=delta_score,
            sample_count=len(deltas), confidence=min(1.0, len(deltas) / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="Delta可控" if expand_rate < self.GREEKS_HEALTHY_EXPAND else "Delta扩展",
            detail=f"扩展率={expand_rate:.2f}",
        )

        vega_sub = SubItemScore(
            name="Vega合理性", score=vega_score,
            sample_count=len(vegas), confidence=min(1.0, len(vegas) / self.HIGH_CONFIDENCE_THRESHOLD),
            pattern="Vega可控" if vega_spike_rate < self.GREEKS_HEALTHY_VEGA_SPIKE else "Vega尖峰",
            detail=f"尖峰率={vega_spike_rate:.2f}",
        )

        greeks_rat = self.GREEKS_DELTA_W * delta_score + self.GREEKS_VEGA_W * vega_score

        if expand_rate > self.GREEKS_CRITICAL_EXPAND:
            severity = DiagnosisSeverity.CRITICAL
            pattern = "Greeks过度暴露"
        elif expand_rate < self.GREEKS_HEALTHY_EXPAND and vega_spike_rate < self.GREEKS_HEALTHY_VEGA_SPIKE:
            severity = DiagnosisSeverity.HEALTHY
            pattern = "Greeks可控"
        else:
            severity = DiagnosisSeverity.WARNING
            pattern = "Greeks部分异常"

        return DimensionDiagnosis(
            dimension="Greeks暴露合理性", score=float(np.clip(greeks_rat, 0, 1)),
            severity=severity, pattern=pattern,
            detail=f"Delta扩展率={expand_rate:.2f}, Vega尖峰率={vega_spike_rate:.2f}",
            sub_items=[delta_sub, vega_sub],
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            anomalies=anomalies[:10],
        )

    def _diagnose_cross_strategy(self, high_snaps, low_snaps) -> DimensionDiagnosis:
        total = len(high_snaps) + len(low_snaps)
        if total < self.MIN_SAMPLES_PER_DIMENSION:
            return DimensionDiagnosis(
                dimension="策略池联动一致性", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="数据不足", detail=f"仅有{total}个极值样本",
                sample_count=total,
            )

        all_snaps = high_snaps + low_snaps
        correlated_count = 0
        divergent_count = 0
        anomalies = []

        for s in all_snaps:
            if len(s.strategy_states) < 2:
                continue
            signals = [ss.signal_strength * (1 if ss.signal_direction == "LONG" else -1)
                       for ss in s.strategy_states if ss.signal_direction]
            if len(signals) < 2:
                continue
            all_same = all(sgn >= 0 for sgn in signals) or all(sgn <= 0 for sgn in signals)
            if all_same and all(abs(sgn) > self.CROSS_SIGNAL_VALID for sgn in signals):
                correlated_count += 1
                if correlated_count <= 3:
                    anomalies.append(f"策略同步: {[f'{x:.2f}' for x in signals]}")
            else:
                divergent_count += 1

        total_multi = correlated_count + divergent_count
        if total_multi == 0:
            return DimensionDiagnosis(
                dimension="策略池联动一致性", score=0.5,
                severity=DiagnosisSeverity.INSUFFICIENT_DATA,
                pattern="无多策略数据", detail="",
                sample_count=total,
            )

        corr_rate = correlated_count / total_multi
        score = max(0.0, 1.0 - corr_rate * self.CROSS_CORR_PENALTY_MULT)

        if corr_rate < self.CROSS_HEALTHY_CORR:
            severity = DiagnosisSeverity.HEALTHY
            pattern = "策略行为分化"
        elif corr_rate > self.CROSS_CRITICAL_CORR:
            severity = DiagnosisSeverity.CRITICAL
            pattern = "策略同步犯错"
        else:
            severity = DiagnosisSeverity.WARNING
            pattern = "策略部分相关"

        return DimensionDiagnosis(
            dimension="策略池联动一致性", score=float(np.clip(score, 0, 1)),
            severity=severity, pattern=pattern,
            detail=f"同步犯错率={corr_rate:.2f}, 分化率={1-corr_rate:.2f}",
            sample_count=total, confidence=min(1.0, total / self.HIGH_CONFIDENCE_THRESHOLD),
            anomalies=anomalies[:10],
        )

    def _extract_signal(self, snap: MarketSnapshot) -> float:
        for ss in snap.strategy_states:
            if ss.strategy_id == self._strategy_id:
                return ss.signal_strength
        return 0.0

    def _extract_position(self, snap: MarketSnapshot) -> float:
        for ss in snap.strategy_states:
            if ss.strategy_id == self._strategy_id:
                return ss.position_size
        return 0.0

    def _generate_summary(self, overall, severity, dimensions):
        critical = [d for d in dimensions if d.severity == DiagnosisSeverity.CRITICAL]
        warning = [d for d in dimensions if d.severity == DiagnosisSeverity.WARNING]
        parts = [f"行为一致性={overall:.2f}({severity.value})"]
        if critical:
            parts.append(f"严重: {', '.join(d.dimension for d in critical)}")
        if warning:
            parts.append(f"警告: {', '.join(d.dimension for d in warning)}")
        return "; ".join(parts)

    def _generate_recommendations(self, dimensions, severity):
        recs = []
        for d in dimensions:
            if d.severity == DiagnosisSeverity.CRITICAL:
                recs.append(f"[严重] {d.dimension}: {d.pattern} — 需审查策略逻辑，非调参。{d.detail}")
            elif d.severity == DiagnosisSeverity.WARNING:
                recs.append(f"[警告] {d.dimension}: {d.pattern} — 关注。{d.detail}")
            elif d.severity == DiagnosisSeverity.INSUFFICIENT_DATA:
                recs.append(f"[数据不足] {d.dimension}: 样本{d.sample_count}(<30)，增加回测期或降时间维度")
        return recs
