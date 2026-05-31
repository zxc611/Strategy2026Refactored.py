import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
from ali2026v3_trading.shared_utils import CHINA_TZ
from enum import Enum

import numpy as np

logger = logging.getLogger(__name__)


class PerformanceTier(Enum):
    TIER_3_FAST = 'tier_3_fast'
    TIER_2_CORE = 'tier_2_core'
    TIER_1_FULL = 'tier_1_full'


@dataclass(slots=True)
class TierConfig:
    tier: PerformanceTier
    min_sharpe: float
    feature_set: List[str]
    optimization_trials: int
    validation_depth: str
    data_resolution: str
    description: str


@dataclass(slots=True)
class TierTransition:
    from_tier: PerformanceTier
    to_tier: PerformanceTier
    reason: str
    sharpe_achieved: float
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())


class PerformanceTierManager:
    """
    三级性能自动降级管理器

    TIER_3 快速验证：日频特征，Optuna 100 trials，夏普>1.0
    TIER_2 核心建设：分钟Bar特征，Optuna 300 trials，夏普>1.5
    TIER_1 完整精细：Tick级特征，Optuna 500+ trials，夏普>2.0

    降级规则：
    - 数据不足(8年Tick未就绪) → 降级至TIER_3
    - 算力不足(GPU/内存不足) → 降级至TIER_2
    - 时间不足(需快速出结果) → 降级至TIER_3
    - 数据就绪+算力充足 → TIER_1

    升级规则：
    - TIER_3通过(夏普>1.0) → 可申请TIER_2
    - TIER_2通过(夏普>1.5) → 可申请TIER_1
    """

    TIER_CONFIGS = {
        PerformanceTier.TIER_3_FAST: TierConfig(
            tier=PerformanceTier.TIER_3_FAST,
            min_sharpe=1.0,
            feature_set=['daily_iv_rank', 'daily_hv_state', 'daily_return'],
            optimization_trials=100,
            validation_depth='basic',
            data_resolution='daily',
            description='快速验证：日频特征，验证链路通顺',
        ),
        PerformanceTier.TIER_2_CORE: TierConfig(
            tier=PerformanceTier.TIER_2_CORE,
            min_sharpe=1.5,
            feature_set=[
                'minute_bar_features', 'intraday_hv', 'order_flow_imbalance',
                'iv_surface_features', 'state_lifetime',
            ],
            optimization_trials=300,
            validation_depth='standard',
            data_resolution='minute_bar',
            description='核心建设：分钟Bar特征，条件寿命字典',
        ),
        PerformanceTier.TIER_1_FULL: TierConfig(
            tier=PerformanceTier.TIER_1_FULL,
            min_sharpe=2.0,
            feature_set=[
                'tick_level_features', 'microstructure', 'depth_imbalance',
                'trade_flow', 'iv_tick_surface', 'greeks_tick',
                'state_lifetime_conditional', 'cross_symbol_correlation',
            ],
            optimization_trials=500,
            validation_depth='deep',
            data_resolution='tick',
            description='完整精细：Tick级特征，全量验证',
        ),
    }

    def __init__(
        self,
        data_availability: str = 'unknown',
        compute_capacity: str = 'unknown',
        time_budget: str = 'normal',
    ):
        self._current_tier: PerformanceTier = PerformanceTier.TIER_3_FAST
        self._transition_log: List[TierTransition] = []
        self._data_availability = data_availability
        self._compute_capacity = compute_capacity
        self._time_budget = time_budget
        self._auto_select_tier()

    def _auto_select_tier(self) -> None:
        data = self._data_availability
        compute = self._compute_capacity
        time = self._time_budget

        if data == 'tick_ready' and compute == 'high' and time != 'urgent':
            target = PerformanceTier.TIER_1_FULL
        elif data in ('tick_ready', 'minute_bar_ready') and compute in ('high', 'medium'):
            target = PerformanceTier.TIER_2_CORE
        else:
            target = PerformanceTier.TIER_3_FAST

        if target != self._current_tier:
            old = self._current_tier
            self._current_tier = target
            self._transition_log.append(TierTransition(
                from_tier=old, to_tier=target,
                reason=f'自动选择: data={data}, compute={compute}, time={time}',
                sharpe_achieved=0.0,
            ))
            logger.info("性能层级自动选择: %s → %s", old.value, target.value)

    def get_current_tier(self) -> PerformanceTier:
        return self._current_tier

    def get_tier_config(self) -> TierConfig:
        return self.TIER_CONFIGS[self._current_tier]

    def attempt_upgrade(
        self, achieved_sharpe: float
    ) -> Optional[PerformanceTier]:
        current_config = self.TIER_CONFIGS[self._current_tier]
        if achieved_sharpe < current_config.min_sharpe:
            logger.info(
                "升阶失败: 夏普%.4f < 当前层级阈值%.4f",
                achieved_sharpe, current_config.min_sharpe,
            )
            return None

        tier_order = [
            PerformanceTier.TIER_3_FAST,
            PerformanceTier.TIER_2_CORE,
            PerformanceTier.TIER_1_FULL,
        ]
        current_idx = tier_order.index(self._current_tier)
        if current_idx >= len(tier_order) - 1:
            logger.info("已在最高层级 %s", self._current_tier.value)
            return None

        next_tier = tier_order[current_idx + 1]

        if next_tier == PerformanceTier.TIER_2_CORE:
            if self._data_availability not in ('tick_ready', 'minute_bar_ready'):
                logger.info("升阶至TIER_2失败: 数据不满足")
                return None
        elif next_tier == PerformanceTier.TIER_1_FULL:
            if self._data_availability != 'tick_ready' or self._compute_capacity != 'high':
                logger.info("升阶至TIER_1失败: 数据或算力不满足")
                return None

        old = self._current_tier
        self._current_tier = next_tier
        self._transition_log.append(TierTransition(
            from_tier=old, to_tier=next_tier,
            reason=f'夏普{achieved_sharpe:.4f}≥阈值{current_config.min_sharpe:.4f}',
            sharpe_achieved=achieved_sharpe,
        ))
        logger.info(
            "升阶成功: %s → %s (夏普=%.4f)",
            old.value, next_tier.value, achieved_sharpe,
        )
        return next_tier

    def force_downgrade(self, reason: str) -> PerformanceTier:
        tier_order = [
            PerformanceTier.TIER_3_FAST,
            PerformanceTier.TIER_2_CORE,
            PerformanceTier.TIER_1_FULL,
        ]
        current_idx = tier_order.index(self._current_tier)
        if current_idx <= 0:
            logger.info("已在最低层级，无法降级")
            return self._current_tier

        old = self._current_tier
        new_tier = tier_order[current_idx - 1]
        self._current_tier = new_tier
        self._transition_log.append(TierTransition(
            from_tier=old, to_tier=new_tier,
            reason=f'强制降级: {reason}',
            sharpe_achieved=0.0,
        ))
        logger.info("强制降级: %s → %s, 原因=%s", old.value, new_tier.value, reason)
        return new_tier

    def get_feature_set(self) -> List[str]:
        return self.TIER_CONFIGS[self._current_tier].feature_set

    def get_optimization_trials(self) -> int:
        return self.TIER_CONFIGS[self._current_tier].optimization_trials

    def get_data_resolution(self) -> str:
        return self.TIER_CONFIGS[self._current_tier].data_resolution

    def get_transition_log(self) -> List[TierTransition]:
        return list(self._transition_log)

    def assess_environment(
        self,
        data_rows: int = 0,
        available_memory_gb: float = 16.0,
        gpu_available: bool = False,
    ) -> Dict[str, Any]:
        if data_rows > 1_000_000_000 and available_memory_gb >= 64 and gpu_available:
            data = 'tick_ready'
            compute = 'high'
        elif data_rows > 100_000_000 and available_memory_gb >= 16:
            data = 'minute_bar_ready'
            compute = 'medium'
        else:
            data = 'daily_ready'
            compute = 'low'

        self._data_availability = data
        self._compute_capacity = compute
        self._auto_select_tier()

        return {
            'data_availability': data,
            'compute_capacity': compute,
            'selected_tier': self._current_tier.value,
            'data_rows': data_rows,
            'memory_gb': available_memory_gb,
            'gpu': gpu_available,
        }

    def get_tier_summary(self) -> Dict[str, Any]:
        config = self.get_tier_config()
        return {
            'current_tier': self._current_tier.value,
            'min_sharpe': config.min_sharpe,
            'feature_count': len(config.feature_set),
            'optimization_trials': config.optimization_trials,
            'validation_depth': config.validation_depth,
            'data_resolution': config.data_resolution,
            'description': config.description,
            'transitions': len(self._transition_log),
        }
