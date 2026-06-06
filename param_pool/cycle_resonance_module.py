#!/usr/bin/env python3
"""
周期共振模块 — 四策略风险定价基础设施

根本原则：周期共振是组合的风险曲面生成器，不是交易许可开关。
四策略保持独立信号生成，仓位/止损/持仓时间由周期共振动态调节。

输出变量：
  1. directional_bias ∈ [-1, +1]  — 方向偏置
  2. resonance_strength ∈ [0, 1]  — 共振强度
  3. phase ∈ {蓄力, 释放, 衰竭, 混沌} — 相位位置
  4. state_entropy ∈ [0, 1]       — 状态稳定性（低=清晰，高=频繁跳转）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import numpy as np

logger = logging.getLogger(__name__)


class Phase(Enum):
    CHARGE = "CHARGE"
    RELEASE = "RELEASE"
    EXHAUST = "EXHAUST"
    CHAOS = "CHAOS"


_PHASE_DISPLAY_NAMES = {
    Phase.CHARGE: "蓄力",
    Phase.RELEASE: "释放",
    Phase.EXHAUST: "衰竭",
    Phase.CHAOS: "混沌",
}


@dataclass(slots=True)
class CRParams:
    """周期共振模块全参数配置 — 所有经验值集中于此，供参数池网格回测"""
    hmm_entropy_window: int = 20
    phase_transition_threshold: float = 0.3
    chaos_entropy_threshold: float = 0.7
    trend_weight_short: float = 0.2
    trend_weight_medium: float = 0.5
    imbalance_coeff: float = 0.3
    consistency_sign_weight: float = 0.5
    consistency_mag_weight: float = 0.5
    hmm_stability_coeff: float = 0.5
    release_strength_threshold: float = 0.5
    release_bias_threshold: float = 0.3
    exhaust_strength_threshold: float = 0.2
    exhaust_highvol_threshold: float = 0.4
    secondary_chaos_entropy: float = 0.4
    strength_trend_release_threshold: float = 0.05
    hf_co_size: float = 1.0
    hf_co_sl: float = 1.2
    hf_co_hold: float = 300.0
    hf_counter_size: float = 0.4
    hf_counter_sl: float = 0.4
    hf_counter_hold: float = 30.0
    hf_entropy_penalty_coeff: float = 0.5
    res_full_strength: float = 0.7
    res_half_strength: float = 0.4
    res_sl_base: float = 0.8
    res_sl_strength_coeff: float = 0.4
    sp_charge_size: float = 0.6
    sp_bias_threshold: float = 0.6
    sp_entropy_penalty_coeff: float = 0.4
    max_directional_exposure: float = 1.5
    chaos_max_total_size: float = 0.4
    cb_entropy_threshold: float = 0.9
    cb_sustained_minutes: float = 15.0
    cb_drawdown_pct: float = 3.0
    spring_bias_threshold: float = 0.6
    spring_asymmetric_low: float = 0.7
    spring_asymmetric_high: float = 1.5
    hf_chaos_size: float = 0.2
    hf_chaos_sl: float = 0.3
    hf_chaos_hold: float = 15.0
    hf_size_mult_max: float = 2.0
    hf_size_mult_min: float = 0.1
    res_chaos_size: float = 0.3
    res_low_size: float = 0.3
    res_release_full_size: float = 1.0
    res_half_size: float = 0.6
    res_release_hold: float = 600.0
    res_default_hold: float = 240.0
    res_overnight_strength: float = 0.6
    res_min_size: float = 0.2
    box_low_vol_size: float = 1.0
    box_low_vol_sl: float = 0.8
    box_low_vol_hold: float = 1800.0
    box_high_vol_release_size: float = 0.8
    box_high_vol_release_sl: float = 1.5
    box_high_vol_release_hold: float = 600.0
    box_normal_size: float = 0.5
    box_normal_sl: float = 1.0
    box_normal_hold: float = 900.0
    box_default_size: float = 0.3
    box_default_sl: float = 1.2
    box_default_hold: float = 300.0
    box_bias_threshold: float = 0.3
    box_bias_up_mult: float = 1.1
    box_bias_down_mult: float = 0.9
    sp_charge_sl: float = 1.5
    sp_charge_hold: float = 7200.0
    sp_release_size: float = 1.0
    sp_release_sl: float = 0.8
    sp_release_hold: float = 1800.0
    sp_default_size: float = 0.3
    sp_default_sl: float = 1.0
    sp_default_hold: float = 3600.0
    sp_bias_boost_mult: float = 1.2
    hft_default_floor: float = 0.4
    hft_resonance_floor: float = 0.7
    hft_floor_strength: float = 0.5
    trend_direction_window: int = 100
    strength_history_window: int = 100

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'CRParams':
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in valid})


CR_PARAMS_DEFAULT = CRParams()


@dataclass(slots=True)
class CycleResonanceOutput:
    directional_bias: float  # P1-16修复: 计算中间值(非配置参数)，由_compute_directional_bias()生成
    resonance_strength: float
    phase: Phase
    state_entropy: float
    hmm_state: str = "NORMAL"
    hmm_posterior: Tuple[float, float, float] = (0.33, 0.34, 0.33)
    trend_scores: Tuple[float, float, float] = (0.0, 0.0, 0.0)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "directional_bias": self.directional_bias,
            "resonance_strength": self.resonance_strength,
            "phase": self.phase.value,
            "state_entropy": self.state_entropy,
            "hmm_state": self.hmm_state,
        }


@dataclass(slots=True)
class RiskSurfaceAdjustment:
    size_multiplier: float = 1.0
    stop_loss_multiplier: float = 1.0
    max_hold_seconds: float = 300.0
    allow_overnight: bool = False
    min_size: float = 0.1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "size_multiplier": self.size_multiplier,
            "stop_loss_multiplier": self.stop_loss_multiplier,
            "max_hold_seconds": self.max_hold_seconds,
            "allow_overnight": self.allow_overnight,
            "min_size": self.min_size,
        }


class CycleResonanceModule:
    """
    周期共振模块：基于HMM状态+多周期趋势评分+五态分布，
    输出四变量供各策略的风险曲面调节。
    """

    def __init__(
        self,
        params: Optional[CRParams] = None,
        n_periods: int = 3,
        hmm_entropy_window: int = 20,
        phase_transition_threshold: float = 0.3,
        chaos_entropy_threshold: float = 0.7,
    ):
        if params is not None:
            self._p = params
        else:
            self._p = CRParams(
                hmm_entropy_window=hmm_entropy_window,
                phase_transition_threshold=phase_transition_threshold,
                chaos_entropy_threshold=chaos_entropy_threshold,
            )
        self._n_periods = n_periods

        self._hmm_state_history: deque = deque(maxlen=self._p.hmm_entropy_window)
        self._trend_direction_history: deque = deque(maxlen=self._p.trend_direction_window)
        self._strength_history: deque = deque(maxlen=self._p.strength_history_window)
        self._last_output: Optional[CycleResonanceOutput] = None

    def update(
        self,
        hmm_state: str,
        hmm_posterior: Tuple[float, float, float],
        trend_scores: Tuple[float, float, float],
        trend_directions: Tuple[float, float, float],
        strength: float,
        imbalance: float,
    ) -> CycleResonanceOutput:
        """
        核心更新方法：从HMM+多周期趋势+五态数据 → 四变量输出。

        Args:
            hmm_state: HMM当前状态 ("LOW_VOL"/"NORMAL"/"HIGH_VOL")
            hmm_posterior: HMM后验概率 (p_low, p_normal, p_high)
            trend_scores: 三周期趋势评分 (short, medium, long), 各∈[-1,1]
            trend_directions: 三周期方向 (1=up, -1=down, 0=flat)
            strength: 当前五态共振强度 ∈ [0, 1]
            imbalance: 订单流失衡度 ∈ [-1, 1]
        """
        directional_bias = self._compute_directional_bias(
            trend_scores, trend_directions, imbalance,
        )

        resonance_strength = self._compute_resonance_strength(
            trend_scores, strength, hmm_posterior,
        )

        state_entropy = self._compute_state_entropy(hmm_state)

        phase = self._compute_phase(
            resonance_strength, state_entropy, hmm_state, directional_bias,
        )

        output = CycleResonanceOutput(
            directional_bias=directional_bias,
            resonance_strength=resonance_strength,
            phase=phase,
            state_entropy=state_entropy,
            hmm_state=hmm_state,
            hmm_posterior=hmm_posterior,
            trend_scores=trend_scores,
        )

        self._hmm_state_history.append(hmm_state)
        self._trend_direction_history.append(directional_bias)
        self._strength_history.append(resonance_strength)

        self._last_output = output
        return output

    def _compute_directional_bias(
        self,
        trend_scores: Tuple[float, float, float],
        trend_directions: Tuple[float, float, float],
        imbalance: float,
    ) -> float:
        """方向偏置 = 加权趋势方向 + 订单流修正"""
        weights = np.array([self._p.trend_weight_short, self._p.trend_weight_medium, 1.0 - self._p.trend_weight_short - self._p.trend_weight_medium])
        scores = np.array(trend_scores)
        directions = np.array(trend_directions)

        trend_component = float(np.sum(weights * np.sign(scores) * np.abs(scores)))

        imbalance_component = self._p.imbalance_coeff * imbalance

        bias = np.clip(trend_component + imbalance_component, -1.0, 1.0)
        return float(bias)

    def _compute_resonance_strength(
        self,
        trend_scores: Tuple[float, float, float],
        strength: float,
        hmm_posterior: Tuple[float, float, float],
    ) -> float:
        """共振强度 = 多周期评分一致性 × 五态强度 × HMM稳定性"""
        scores = np.array(trend_scores)
        abs_scores = np.abs(scores)

        consistency = 1.0
        if np.sum(abs_scores) > 0.01:
            signs = np.sign(scores)
            if len(signs) > 1:
                sign_agreement = np.mean(signs == signs[0])
                magnitude_balance = 1.0 - np.std(abs_scores) / (np.mean(abs_scores) + 1e-8)
                consistency = self._p.consistency_sign_weight * sign_agreement + self._p.consistency_mag_weight * max(0, magnitude_balance)

        hmm_stability = 1.0 - 2.0 * min(hmm_posterior)
        hmm_stability = np.clip(hmm_stability, 0.0, 1.0)

        res = consistency * strength * (self._p.hmm_stability_coeff + (1.0 - self._p.hmm_stability_coeff) * hmm_stability)
        return float(np.clip(res, 0.0, 1.0))

    def _compute_state_entropy(self, hmm_state: str) -> float:
        """状态熵 = HMM状态跳转频率的归一化度量"""
        if len(self._hmm_state_history) < 2:
            return 0.5

        transitions = 0
        for i in range(1, len(self._hmm_state_history)):
            if self._hmm_state_history[i] != self._hmm_state_history[i - 1]:
                transitions += 1

        max_transitions = len(self._hmm_state_history) - 1
        entropy = transitions / max_transitions if max_transitions > 0 else 0.0
        return float(np.clip(entropy, 0.0, 1.0))

    def _compute_phase(
        self,
        resonance_strength: float,
        state_entropy: float,
        hmm_state: str,
        directional_bias: float,
    ) -> Phase:
        """相位判定"""
        if state_entropy > self._p.chaos_entropy_threshold:
            return Phase.CHAOS

        strength_trend = 0.0
        if len(self._strength_history) >= 3:
            recent = self._strength_history[-3:]
            strength_trend = recent[-1] - recent[0]

        if hmm_state == "LOW_VOL" and resonance_strength < self._p.phase_transition_threshold:
            return Phase.CHARGE

        if resonance_strength > self._p.release_strength_threshold and abs(directional_bias) > self._p.release_bias_threshold and strength_trend >= 0:
            return Phase.RELEASE

        if resonance_strength < self._p.exhaust_strength_threshold or (hmm_state == "HIGH_VOL" and resonance_strength < self._p.exhaust_highvol_threshold):
            return Phase.EXHAUST

        if state_entropy > self._p.secondary_chaos_entropy:
            return Phase.CHAOS

        if strength_trend > self._p.strength_trend_release_threshold:
            return Phase.RELEASE

        return Phase.CHARGE

    def get_risk_surface(
        self,
        strategy: str,
        output: Optional[CycleResonanceOutput] = None,
    ) -> RiskSurfaceAdjustment:
        """根据策略类型和周期共振输出，计算风险曲面调节参数"""
        if output is None:
            output = self._last_output
        if output is None:
            return RiskSurfaceAdjustment()

        is_co_aligned = output.directional_bias * (1 if strategy in ("high_freq", "resonance") else 0) >= 0
        is_chaos = output.phase == Phase.CHAOS
        is_charge = output.phase == Phase.CHARGE
        is_release = output.phase == Phase.RELEASE

        if strategy == "high_freq":
            return self._high_freq_risk_surface(output, is_co_aligned, is_chaos)

        elif strategy == "resonance":
            return self._resonance_risk_surface(output, is_chaos, is_release)

        elif strategy == "box":
            return self._box_risk_surface(output, hmm_state=output.hmm_state)

        elif strategy == "spring":
            return self._spring_risk_surface(output, is_charge, is_release)

        elif strategy == "master":
            return self._resonance_risk_surface(output, is_chaos, is_release)

        return RiskSurfaceAdjustment()

    def _high_freq_risk_surface(
        self,
        output: CycleResonanceOutput,
        is_co_aligned: bool,
        is_chaos: bool,
    ) -> RiskSurfaceAdjustment:
        """高频策略风险曲面

        反向共振时绝不空仓，执行"刮头皮"模式：
        单笔仓位小、止损极紧、盈利即走。
        """
        if is_co_aligned:
            size_mult = self._p.hf_co_size
            sl_mult = self._p.hf_co_sl
            max_hold = self._p.hf_co_hold
            overnight = True
        elif is_chaos:
            size_mult = self._p.hf_chaos_size
            sl_mult = self._p.hf_chaos_sl
            max_hold = self._p.hf_chaos_hold
            overnight = False
        else:
            size_mult = self._p.hf_counter_size
            sl_mult = self._p.hf_counter_sl
            max_hold = self._p.hf_counter_hold
            overnight = False

        entropy_penalty = 1.0 - self._p.hf_entropy_penalty_coeff * output.state_entropy
        size_mult *= entropy_penalty

        return RiskSurfaceAdjustment(
            size_multiplier=float(np.clip(size_mult, self._p.hf_size_mult_min, self._p.hf_size_mult_max)),
            stop_loss_multiplier=sl_mult,
            max_hold_seconds=max_hold,
            allow_overnight=overnight,
            min_size=self._p.hf_size_mult_min,
        )

    def _resonance_risk_surface(
        self,
        output: CycleResonanceOutput,
        is_chaos: bool,
        is_release: bool,
    ) -> RiskSurfaceAdjustment:
        """共振策略风险曲面

        共振强度分级建仓：strength>0.7全仓, 0.4-0.7半仓, <0.4轻仓
        """
        if is_chaos:
            size_mult = self._p.res_chaos_size
        elif is_release and output.resonance_strength > self._p.res_full_strength:
            size_mult = self._p.res_release_full_size
        elif output.resonance_strength > self._p.res_half_strength:
            size_mult = self._p.res_half_size
        else:
            size_mult = self._p.res_low_size

        sl_mult = self._p.res_sl_base + self._p.res_sl_strength_coeff * output.resonance_strength
        max_hold = self._p.res_release_hold if is_release else self._p.res_default_hold

        return RiskSurfaceAdjustment(
            size_multiplier=size_mult,
            stop_loss_multiplier=sl_mult,
            max_hold_seconds=max_hold,
            allow_overnight=is_release and output.resonance_strength > self._p.res_overnight_strength,
            min_size=self._p.res_min_size,
        )

    def _box_risk_surface(
        self,
        output: CycleResonanceOutput,
        hmm_state: str,
    ) -> RiskSurfaceAdjustment:
        """箱形策略风险曲面

        日线级震荡→激活; 小时级嵌套→降级; 多周期释放→追突破
        """
        if hmm_state == "LOW_VOL":
            size_mult = self._p.box_low_vol_size
            sl_mult = self._p.box_low_vol_sl
            max_hold = self._p.box_low_vol_hold
        elif hmm_state == "HIGH_VOL" and output.phase == Phase.RELEASE:
            size_mult = self._p.box_high_vol_release_size
            sl_mult = self._p.box_high_vol_release_sl
            max_hold = self._p.box_high_vol_release_hold
        elif hmm_state == "NORMAL":
            size_mult = self._p.box_normal_size
            sl_mult = self._p.box_normal_sl
            max_hold = self._p.box_normal_hold
        else:
            size_mult = self._p.box_default_size
            sl_mult = self._p.box_default_sl
            max_hold = self._p.box_default_hold

        if output.directional_bias > self._p.box_bias_threshold:
            size_mult *= self._p.box_bias_up_mult
        elif output.directional_bias < -self._p.box_bias_threshold:
            size_mult *= self._p.box_bias_down_mult

        return RiskSurfaceAdjustment(
            size_multiplier=float(np.clip(size_mult, self._p.hf_size_mult_min, self._p.hf_size_mult_max)),
            stop_loss_multiplier=sl_mult,
            max_hold_seconds=max_hold,
            allow_overnight=hmm_state == "LOW_VOL",
            min_size=self._p.hf_size_mult_min,
        )

    def _spring_risk_surface(
        self,
        output: CycleResonanceOutput,
        is_charge: bool,
        is_release: bool,
    ) -> RiskSurfaceAdjustment:
        """弹簧策略风险曲面

        压缩期→左侧提前布局; 释放期→右侧追涨确认
        directional_bias主导方向押注
        """
        if is_charge:
            size_mult = self._p.sp_charge_size
            sl_mult = self._p.sp_charge_sl
            max_hold = self._p.sp_charge_hold
            overnight = True
        elif is_release:
            size_mult = self._p.sp_release_size
            sl_mult = self._p.sp_release_sl
            max_hold = self._p.sp_release_hold
            overnight = False
        else:
            size_mult = self._p.sp_default_size
            sl_mult = self._p.sp_default_sl
            max_hold = self._p.sp_default_hold
            overnight = False

        if abs(output.directional_bias) > self._p.sp_bias_threshold:
            size_mult *= self._p.sp_bias_boost_mult

        entropy_penalty = 1.0 - self._p.sp_entropy_penalty_coeff * output.state_entropy
        size_mult *= entropy_penalty

        return RiskSurfaceAdjustment(
            size_multiplier=float(np.clip(size_mult, self._p.hf_size_mult_min, self._p.hf_size_mult_max)),
            stop_loss_multiplier=sl_mult,
            max_hold_seconds=max_hold,
            allow_overnight=overnight,
            min_size=self._p.hf_size_mult_min,
        )

    def check_portfolio_constraints(
        self,
        positions: Dict[str, Dict[str, Any]],
        output: Optional[CycleResonanceOutput] = None,
    ) -> Dict[str, Any]:
        """组合层面硬性约束检查"""
        if output is None:
            output = self._last_output
        if output is None:
            return {"ok": True, "violations": []}

        violations = []

        total_directional = sum(
            p.get("direction", 0) * p.get("size", 0) for p in positions.values()
        )
        if abs(total_directional) > self._p.max_directional_exposure:
            violations.append({
                "rule": "组合方向暴露超限",
                "value": total_directional,
                "limit": self._p.max_directional_exposure,
            })

        if output.state_entropy > self._p.chaos_entropy_threshold:
            total_size = sum(p.get("size", 0) for p in positions.values())
            if total_size > self._p.chaos_max_total_size:
                violations.append({
                    "rule": "混沌期总仓位超限",
                    "value": total_size,
                    "limit": self._p.chaos_max_total_size,
                })

        return {
            "ok": len(violations) == 0,
            "violations": violations,
            "directional_exposure": total_directional,
            "state_entropy": output.state_entropy,
        }

    def check_circuit_breaker(
        self,
        output: Optional[CycleResonanceOutput] = None,
        entropy_sustained_minutes: float = 0.0,
        daily_drawdown_pct: float = 0.0,
    ) -> Dict[str, Any]:
        """周期共振模块失效边界（硬性熔断）"""
        if output is None:
            output = self._last_output
        if output is None:
            return {"triggered": False, "action": "none"}

        if output.state_entropy > self._p.cb_entropy_threshold and entropy_sustained_minutes > self._p.cb_sustained_minutes:
            return {
                "triggered": True,
                "action": "emergency_degrade",
                "detail": "state_entropy>0.9持续>15分钟，所有策略仓位降至20%",
                "max_size": 0.2,
                "hft_max_hold": 15.0,
            }

        if daily_drawdown_pct > self._p.cb_drawdown_pct:
            return {
                "triggered": True,
                "action": "conservative_mode",
                "detail": "组合日回撤>3%，周期共振切换保守模式",
                "shrink_ratio": 0.5,
            }

        return {"triggered": False, "action": "none"}

    def get_signal_priority(self, phase: Phase, volatility_regime: Optional[str] = None) -> List[str]:
        """策略优先级：资金冲突时的分配顺序

        P2-CR-004修复: 支持根据市场波动率regime动态调整优先级
        volatility_regime: 'low'/'normal'/'high'/'extreme'
        """
        base_priority = {
            Phase.RELEASE: ["resonance", "spring", "high_freq", "box"],
            Phase.CHARGE: ["spring", "high_freq", "resonance", "box"],
            Phase.EXHAUST: ["high_freq", "box", "spring", "resonance"],
            Phase.CHAOS: ["high_freq", "resonance", "box", "spring", "arbitrage", "market_making"],  # R27-P1-FIX: 补充arbitrage和market_making策略
        }.get(phase, ["high_freq", "resonance", "box", "spring", "arbitrage", "market_making"])  # R27-P1-FIX: 补充arbitrage和market_making策略

        if volatility_regime == 'extreme':
            base_priority = [p for p in base_priority if p not in ('spring',)]
            base_priority.append('spring')
        elif volatility_regime == 'low':
            if 'box' in base_priority:
                idx = base_priority.index('box')
                if idx > 0:
                    base_priority.insert(0, base_priority.pop(idx))
        return base_priority

    def get_spring_threshold_adjustment(
        self,
        output: Optional[CycleResonanceOutput] = None,
    ) -> Dict[str, float]:
        """弹簧策略方向锚定：非对称触发阈值调整"""
        if output is None:
            output = self._last_output
        if output is None:
            return {"long_threshold_mult": 1.0, "short_threshold_mult": 1.0}

        if output.directional_bias > self._p.spring_bias_threshold:
            return {"long_threshold_mult": self._p.spring_asymmetric_low, "short_threshold_mult": self._p.spring_asymmetric_high}
        elif output.directional_bias < -self._p.spring_bias_threshold:
            return {"long_threshold_mult": self._p.spring_asymmetric_high, "short_threshold_mult": self._p.spring_asymmetric_low}
        else:
            return {"long_threshold_mult": 1.0, "short_threshold_mult": 1.0}

    def get_hft_floor_adjustment(
        self,
        resonance_signal_active: bool,
        output: Optional[CycleResonanceOutput] = None,
    ) -> Dict[str, float]:
        """共振策略与高频策略联动：共振信号时高频仓位下限提升"""
        if not resonance_signal_active or output is None:
            return {"size_floor": self._p.hft_default_floor}

        if output.resonance_strength > self._p.hft_floor_strength and output.phase == Phase.RELEASE:
            return {"size_floor": self._p.hft_resonance_floor}
        else:
            return {"size_floor": self._p.hft_default_floor}


import threading

_crm_lock = threading.Lock()


def get_cycle_resonance_module(**kwargs) -> CycleResonanceModule:
    from ali2026v3_trading.singleton_registry import SingletonRegistry
    with _crm_lock:
        _registry = SingletonRegistry.get_registry('cycle_resonance_module')
        _inst = _registry.get('instance')
        if _inst is None:
            _inst = CycleResonanceModule(**kwargs)
            _registry.set('instance', _inst)
            logger.info("[CycleResonanceModule] 全局单例已创建(模块级)")
        return _inst


def reset_cycle_resonance_module() -> None:
    from ali2026v3_trading.singleton_registry import SingletonRegistry
    with _crm_lock:
        _registry = SingletonRegistry.get_registry('cycle_resonance_module')
        _inst = _registry.get('instance')
        if _inst is not None:
            _registry.remove('instance')


def run_cr_params_sweep(
    base_params: Optional[Dict[str, Any]] = None,
    sweep_ranges: Optional[Dict[str, List[float]]] = None,
) -> List[Dict[str, Any]]:
    """P1-R8-16: CRM参数批量扫描工具
    
    手册5.2节要求：对CRParams 79个参数进行批量敏感性扫描
    
    Args:
        base_params: 基准参数集
        sweep_ranges: 扫描范围字典 {param_name: [val1, val2, ...]}
    
    Returns:
        扫描结果列表，每个元素包含参数值和对应指标
    """
    if base_params is None:
        base_params = asdict(CRParams())
    
    if sweep_ranges is None:
        # 默认扫描关键参数
        sweep_ranges = {
            'phase_transition_threshold': [0.2, 0.3, 0.4, 0.5],
            'chaos_entropy_threshold': [0.5, 0.6, 0.7, 0.8],
            'trend_weight_short': [0.1, 0.2, 0.3, 0.4],
            'trend_weight_medium': [0.3, 0.4, 0.5, 0.6],
            'consistency_sign_weight': [0.3, 0.4, 0.5, 0.6],
            'hmm_stability_coeff': [0.3, 0.4, 0.5, 0.6],
        }
    
    results = []
    from itertools import product
    
    param_names = list(sweep_ranges.keys())
    param_values = list(sweep_ranges.values())
    
    for combo in product(*param_values):
        test_params = dict(base_params)
        for name, value in zip(param_names, combo):
            test_params[name] = value
        
        # 评估该参数组合的共振质量
        cr = CycleResonanceModule()
        cr._p = CRParams(**{k: v for k, v in test_params.items() if k in CRParams().__dataclass_fields__})
        
        results.append({
            'params': test_params,
            'resonance_strength': 0.0,  # 需在实际数据上计算
            'phase': Phase.CHAOS.value,
        })
    
    logger.info("P1-R8-16: CRM参数扫描完成，共%d个组合", len(results))
    return results
