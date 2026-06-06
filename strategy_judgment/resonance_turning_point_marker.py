"""
周期共振拐点标记 (Resonance Turning Point Marker)

结合周期共振模块(CycleResonanceModule)的四变量输出，
标识预期转折点和实际转折点，形成拐点前-中-后的完整标记链。

核心设计：
  1. 预期转折点：周期共振相位转换时（蓄力→释放、释放→衰竭等）
     标记"市场预期即将发生转折"，但价格尚未确认
  2. 实际转折点：EnhancedBar的极值区域 + 共振方向确认
     标记"价格已实际发生转折"，与预期转折点配对
  3. 转折点分类：
     - 趋势延续型转折：短周期均线走平但长周期仍延续 → 趋势中继
     - 趋势反转型转折：均线收敛+价格偏离度≥2σ → 反转前兆
     - 震荡极值型转折：混沌相位+缠绕排列 → 震荡区间边界
  4. 预期-实际配对：评估共振模块的预测能力（预期→实际的时间差和方向一致性）
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ..param_pool.cycle_resonance_module import CycleResonanceOutput

import numpy as np

from .turning_point_microscope import (
    EnhancedBar, ExtremeRegion, MAAlignment, TurningPointMicroscope,
)

logger = logging.getLogger(__name__)


class TurningPointType(Enum):
    EXPECTED_TREND_CONTINUATION = "预期_趋势延续"
    EXPECTED_TREND_REVERSAL = "预期_趋势反转"
    EXPECTED_OSCILLATION_EXTREME = "预期_震荡极值"
    ACTUAL_TREND_CONTINUATION = "实际_趋势延续"
    ACTUAL_TREND_REVERSAL = "实际_趋势反转"
    ACTUAL_OSCILLATION_EXTREME = "实际_震荡极值"


class _ResonancePhase(Enum):
    CHARGE = "蓄力"
    RELEASE = "释放"
    EXHAUST = "衰竭"
    CHAOS = "混沌"


@dataclass(slots=True)
class TurningPointRecord:
    timestamp: np.datetime64
    symbol: str
    tp_type: TurningPointType
    price: float
    phase: str
    directional_bias: float
    resonance_strength: float
    state_entropy: float
    ma_alignment: str
    price_deviation_sigma: float
    vwap: float
    extreme_region: str
    paired_with: Optional[str] = None
    confirmation_bars: int = 0
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expected(self) -> bool:
        return self.tp_type.value.startswith("预期")

    @property
    def is_actual(self) -> bool:
        return self.tp_type.value.startswith("实际")

    @property
    def is_reversal(self) -> bool:
        return "反转" in self.tp_type.value

    @property
    def is_continuation(self) -> bool:
        return "延续" in self.tp_type.value

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['tp_type'] = self.tp_type.value
        d['timestamp'] = str(self.timestamp)
        return d


@dataclass(slots=True)
class _ExpectedTP:
    record: TurningPointRecord
    created_bar_idx: int
    confirmed: bool = False
    confirmation_record: Optional[TurningPointRecord] = None


class ResonanceTurningPointMarker:
    """
    周期共振拐点标记器

    在线处理流程：
      1. 每个EnhancedBar完成后调用 process_bar()
      2. 检测周期共振相位转换 → 生成预期转折点
      3. 检测极值区域+共振确认 → 生成实际转折点
      4. 尝试预期-实际配对
    """

    def __init__(
        self,
        symbol: str,
        confirmation_window_bars: int = 10,
        deviation_reversal_threshold: float = 2.0,
        deviation_continuation_threshold: float = 0.8,
        resonance_reversal_min_strength: float = 0.3,
        max_unconfirmed_expected: int = 50,
    ):
        self._symbol = symbol
        self._confirmation_window = confirmation_window_bars
        self._deviation_reversal_threshold = deviation_reversal_threshold
        self._deviation_continuation_threshold = deviation_continuation_threshold
        self._resonance_reversal_min = resonance_reversal_min_strength
        self._max_unconfirmed = max_unconfirmed_expected

        self._last_phase: Optional[_ResonancePhase] = None
        self._last_directional_bias: float = 0.0
        self._last_resonance_strength: float = 0.0
        self._last_state_entropy: float = 0.0
        self._bias_direction_history: deque = deque(maxlen=20)

        self._unconfirmed_expected: List[_ExpectedTP] = []
        self._all_turning_points: List[TurningPointRecord] = []
        self._bar_count = 0

    def process_bar(
        self,
        bar: EnhancedBar,
        cr_output: Optional[Any] = None,
    ) -> List[TurningPointRecord]:
        """
        处理一个增强Bar，检测并记录转折点

        Args:
            bar: EnhancedBar（含极值区域和均线位置）
            cr_output: CycleResonanceOutput（含四变量），可选
                       若为None则降级为纯价格极值检测

        Returns:
            本Bar新产生的转折点列表
        """
        new_tps = []

        phase = None
        directional_bias = 0.0
        resonance_strength = 0.0
        state_entropy = 0.5

        if cr_output is not None:
            phase = _ResonancePhase(cr_output.phase.value)
            directional_bias = cr_output.directional_bias
            resonance_strength = cr_output.resonance_strength
            state_entropy = cr_output.state_entropy

        if self._last_phase is not None and phase is not None:
            if phase != self._last_phase:
                expected_tp = self._detect_expected_turning_point(
                    bar, phase, directional_bias, resonance_strength, state_entropy,
                )
                if expected_tp is not None:
                    new_tps.append(expected_tp)
                    self._unconfirmed_expected.append(
                        _ExpectedTP(record=expected_tp, created_bar_idx=self._bar_count)
                    )

        if bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,
                                  ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):
            actual_tp = self._detect_actual_turning_point(
                bar, phase, directional_bias, resonance_strength, state_entropy,
            )
            if actual_tp is not None:
                new_tps.append(actual_tp)
                self._try_pair_expected(actual_tp)

        self._last_phase = phase
        self._last_directional_bias = directional_bias
        self._last_resonance_strength = resonance_strength
        self._last_state_entropy = state_entropy
        self._bias_direction_history.append(np.sign(directional_bias))
        self._bar_count += 1

        self._expire_unconfirmed()

        self._all_turning_points.extend(new_tps)
        return new_tps

    def _detect_expected_turning_point(
        self,
        bar: EnhancedBar,
        new_phase: _ResonancePhase,
        bias: float,
        strength: float,
        entropy: float,
    ) -> Optional[TurningPointRecord]:
        """
        相位转换时检测预期转折点

        逻辑：
          - 蓄力→释放：预期趋势爆发（延续或反转取决于bias方向）
          - 释放→衰竭：预期趋势即将结束
          - X→混沌：预期市场失序，不可交易
          - 混沌→蓄力：预期市场将进入新周期
        """
        max_dev = 0.0
        for v in bar.price_ma_deviation_sigma.values():
            max_dev = max(max_dev, abs(v))

        tp_type = None
        confidence = 0.0

        if new_phase == _ResonancePhase.RELEASE:
            if abs(bias) > 0.3 and strength > 0.4:
                if max_dev > self._deviation_reversal_threshold:
                    tp_type = TurningPointType.EXPECTED_TREND_REVERSAL
                    confidence = min(1.0, strength * abs(bias) * 0.8)
                else:
                    tp_type = TurningPointType.EXPECTED_TREND_CONTINUATION
                    confidence = min(1.0, strength * 0.6)
        elif new_phase == _ResonancePhase.EXHAUST:
            tp_type = TurningPointType.EXPECTED_TREND_REVERSAL
            confidence = min(1.0, (1.0 - entropy) * strength * 0.7)
        elif new_phase == _ResonancePhase.CHAOS:
            tp_type = TurningPointType.EXPECTED_OSCILLATION_EXTREME
            confidence = min(1.0, entropy * 0.5)
        elif new_phase == _ResonancePhase.CHARGE:
            tp_type = TurningPointType.EXPECTED_OSCILLATION_EXTREME
            confidence = min(1.0, (1.0 - entropy) * 0.4)

        if tp_type is None or confidence < 0.1:
            return None

        return TurningPointRecord(
            timestamp=bar.timestamp,
            symbol=self._symbol,
            tp_type=tp_type,
            price=bar.vwap,
            phase=new_phase.value,
            directional_bias=bias,
            resonance_strength=strength,
            state_entropy=entropy,
            ma_alignment=bar.ma_alignment.value,
            price_deviation_sigma=max_dev,
            vwap=bar.vwap,
            extreme_region=bar.extreme_region.value,
            confidence=confidence,
        )

    def _detect_actual_turning_point(
        self,
        bar: EnhancedBar,
        phase: Optional[_ResonancePhase],
        bias: float,
        strength: float,
        entropy: float,
    ) -> Optional[TurningPointRecord]:
        """
        极值区域+共振确认 → 实际转折点

        分类逻辑：
          - 均线收敛 + 价格偏离度≥2σ + 共振强度>0.3 → 反转
          - 短周期走平 + 长周期延续 → 延续
          - 混沌相位 + 缠绕排列 → 震荡极值
        """
        is_high = bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.EXTREME_HIGH)
        is_low = bar.extreme_region in (ExtremeRegion.NEAR_LOW, ExtremeRegion.EXTREME_LOW)

        max_dev = 0.0
        for v in bar.price_ma_deviation_sigma.values():
            max_dev = max(max_dev, abs(v))

        tp_type = None
        confidence = 0.0

        if bar.ma_alignment in (MAAlignment.CONVERGENT, MAAlignment.INTERTWINED):
            if max_dev >= self._deviation_reversal_threshold and strength > self._resonance_reversal_min:
                tp_type = TurningPointType.ACTUAL_TREND_REVERSAL
                confidence = min(1.0, max_dev / 4.0 * strength * 0.9)
            elif phase == _ResonancePhase.CHAOS:
                tp_type = TurningPointType.ACTUAL_OSCILLATION_EXTREME
                confidence = min(1.0, entropy * 0.7)
            else:
                tp_type = TurningPointType.ACTUAL_OSCILLATION_EXTREME
                confidence = 0.3

        elif bar.ma_alignment == MAAlignment.BULLISH:
            if is_high and max_dev >= self._deviation_reversal_threshold:
                tp_type = TurningPointType.ACTUAL_TREND_REVERSAL
                confidence = min(1.0, max_dev / 3.0 * 0.8)
            elif is_low:
                tp_type = TurningPointType.ACTUAL_TREND_CONTINUATION
                confidence = min(1.0, strength * 0.6)

        elif bar.ma_alignment == MAAlignment.BEARISH:
            if is_low and max_dev >= self._deviation_reversal_threshold:
                tp_type = TurningPointType.ACTUAL_TREND_REVERSAL
                confidence = min(1.0, max_dev / 3.0 * 0.8)
            elif is_high:
                tp_type = TurningPointType.ACTUAL_TREND_CONTINUATION
                confidence = min(1.0, strength * 0.6)

        if tp_type is None or confidence < 0.1:
            return None

        direction = "high" if is_high else ("low" if is_low else "neutral")

        return TurningPointRecord(
            timestamp=bar.timestamp,
            symbol=self._symbol,
            tp_type=tp_type,
            price=bar.vwap,
            phase=phase.value if phase else "UNKNOWN",
            directional_bias=bias,
            resonance_strength=strength,
            state_entropy=entropy,
            ma_alignment=bar.ma_alignment.value,
            price_deviation_sigma=max_dev,
            vwap=bar.vwap,
            extreme_region=bar.extreme_region.value,
            confidence=confidence,
            metadata={"direction": direction, "bar_extreme": bar.extreme_region.value},
        )

    def _try_pair_expected(self, actual_tp: TurningPointRecord) -> None:
        """
        尝试将实际转折点与最近的未确认预期转折点配对

        配对条件：
          1. 预期与实际类型方向一致（都预期反转→实际反转）
          2. 时间差在确认窗口内
          3. 方向一致（同为高点或低点）
        """
        best_match = None
        best_idx = -1
        best_distance = float('inf')

        for i, exp in enumerate(self._unconfirmed_expected):
            if exp.confirmed:
                continue

            bars_elapsed = self._bar_count - exp.created_bar_idx
            if bars_elapsed > self._confirmation_window:
                continue

            type_compatible = self._are_types_compatible(exp.record.tp_type, actual_tp.tp_type)
            if not type_compatible:
                continue

            if bars_elapsed < best_distance:
                best_distance = bars_elapsed
                best_match = exp
                best_idx = i

        if best_match is not None:
            best_match.confirmed = True
            best_match.confirmation_record = actual_tp
            best_match.record.paired_with = id(actual_tp)
            actual_tp.paired_with = id(best_match.record)
            actual_tp.confirmation_bars = best_distance

    def _are_types_compatible(self, expected: TurningPointType, actual: TurningPointType) -> bool:
        if "反转" in expected.value and "反转" in actual.value:
            return True
        if "延续" in expected.value and "延续" in actual.value:
            return True
        if "震荡" in expected.value and "震荡" in actual.value:
            return True
        return False

    def _expire_unconfirmed(self) -> None:
        self._unconfirmed_expected = [
            exp for exp in self._unconfirmed_expected
            if not exp.confirmed and (self._bar_count - exp.created_bar_idx) <= self._confirmation_window
        ]
        if len(self._unconfirmed_expected) > self._max_unconfirmed:
            self._unconfirmed_expected = self._unconfirmed_expected[-self._max_unconfirmed:]

    def get_all_turning_points(self) -> List[TurningPointRecord]:
        return list(self._all_turning_points)

    def get_expected_turning_points(self) -> List[TurningPointRecord]:
        return [tp for tp in self._all_turning_points if tp.is_expected]

    def get_actual_turning_points(self) -> List[TurningPointRecord]:
        return [tp for tp in self._all_turning_points if tp.is_actual]

    def get_paired_turning_points(self) -> List[Tuple[TurningPointRecord, TurningPointRecord]]:
        pairs = []
        expected_map = {}
        for exp in self._unconfirmed_expected:
            if exp.confirmed and exp.confirmation_record is not None:
                pairs.append((exp.record, exp.confirmation_record))
        return pairs

    def get_prediction_accuracy(self) -> Dict[str, Any]:
        """
        周期共振模块的拐点预测能力评估

        Returns:
            包含命中率、平均确认时间、方向一致性等统计
        """
        expected = self.get_expected_turning_points()
        paired = self.get_paired_turning_points()

        total_expected = len(expected)
        total_paired = len(paired)
        hit_rate = total_paired / total_expected if total_expected > 0 else 0.0

        confirmation_bars = [p[1].confirmation_bars for p in paired]
        avg_confirmation = float(np.mean(confirmation_bars)) if confirmation_bars else 0.0

        direction_match = 0
        direction_total = 0
        for exp_tp, act_tp in paired:
            exp_is_reversal = exp_tp.is_reversal
            act_is_reversal = act_tp.is_reversal
            if exp_is_reversal == act_is_reversal:
                direction_match += 1
            direction_total += 1

        direction_accuracy = direction_match / direction_total if direction_total > 0 else 0.0

        return {
            "total_expected": total_expected,
            "total_actual": len(self.get_actual_turning_points()),
            "total_paired": total_paired,
            "hit_rate": hit_rate,
            "avg_confirmation_bars": avg_confirmation,
            "direction_accuracy": direction_accuracy,
        }
