# MODULE_ID: M1-271
"""strategy_ecosystem._models - 数据模型、枚举、常量、装饰器"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from enum import Enum as _Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.config.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, STRATEGY_MODE_OTHER,
)

logger = get_logger(__name__)  # R9-5


# ---------------------------------------------------------------------------
# 装饰器
# ---------------------------------------------------------------------------

def _require_interface(required_attrs: Tuple[str, ...]) -> Callable:
    """R5-I-06修复: 运行时接口检查装饰器，替代裸duck typing

    对关键方法入口检查self/参数是否实现所需属性/方法，
    在调用方传入错误类型时立即抛TypeError而非运行时AttributeError。
    """
    def decorator(method: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            missing = [a for a in required_attrs if not hasattr(self, a)]
            if missing:
                raise TypeError(
                    f"R5-I-06: {type(self).__name__}缺少接口属性{missing}，"
                    f"调用{method.__name__}前需确保实现{required_attrs}"
                )
            return method(self, *args, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        wrapper.__wrapped__ = method
        return wrapper
    return decorator


def _require_arg_interface(arg_name: str, required_attrs: Tuple[str, ...]) -> Callable:
    """R5-I-06修复: 参数级接口检查装饰器，检查指定参数是否实现所需属性/方法"""
    def decorator(method: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            from inspect import signature
            sig = signature(method)
            params = list(sig.parameters.keys())
            if arg_name in kwargs:
                target = kwargs[arg_name]
            else:
                idx = params.index(arg_name) if arg_name in params else -1
                target = args[idx - 1] if idx > 0 and idx - 1 < len(args) else None
            if target is not None:
                missing = [a for a in required_attrs if not hasattr(target, a)]
                if missing:
                    raise TypeError(
                        f"R5-I-06: 参数'{arg_name}'类型{type(target).__name__}缺少接口{missing}，"
                        f"调用{method.__name__}需确保实现{required_attrs}"
                    )
            return method(*args, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        wrapper.__wrapped__ = method
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# 模块级常量
# ---------------------------------------------------------------------------

# P2-R11-14修复: EV缓存TTL模块级常量 — 按策略类型自适应，可被params覆盖
EV_CACHE_TTL_DEFAULTS = {
    'hft': 1.0,      # HFT策略tick频率>5Hz，TTL应更短
    'minute': 3.0,    # 分钟级策略
    'default': 5.0,   # 默认策略
}
EV_CACHE_TTL = 5.0  # 向后兼容默认值

OBSERVATION_PERIOD_SEC = 1800.0


# ---------------------------------------------------------------------------
# 枚举
# ---------------------------------------------------------------------------

# R15-P1-API-13修复: 槽位状态枚举类型定义（重命名为SlotState以区分lifecycle StrategyState）
class SlotState(_Enum):
    INACTIVE = 'inactive'
    STANDBY = 'standby'
    ACTIVE = 'active'
    HANDOVER = 'handover'
    RETIRED = 'retired'

# INV-07/INV-STA-02: 策略状态合法转换集合
VALID_STRATEGY_TRANSITIONS = {
    SlotState.INACTIVE: {SlotState.STANDBY, SlotState.ACTIVE, SlotState.RETIRED},
    SlotState.STANDBY: {SlotState.ACTIVE, SlotState.INACTIVE, SlotState.HANDOVER, SlotState.RETIRED},
    SlotState.ACTIVE: {SlotState.STANDBY, SlotState.HANDOVER, SlotState.INACTIVE, SlotState.RETIRED},
    SlotState.HANDOVER: {SlotState.STANDBY, SlotState.INACTIVE, SlotState.RETIRED},
    SlotState.RETIRED: {SlotState.STANDBY},  # 仅允许复活审批后回到standby
}


# ---------------------------------------------------------------------------
# 数据类
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class StrategySlot:
    """策略槽位"""

    strategy_id: str
    strategy_type: str
    state: 'SlotState' = None  # 默认值在__post_init__中设置
    capital_allocation: float = 0.0
    position_count: int = 0
    total_pnl: float = 0.0
    expected_value: float = 0.0
    sharpe: float = 0.0
    win_loss_ratio: float = 0.0
    profit_factor: float = 0.0
    consecutive_losses: int = 0
    target_plr: float = 2.0
    params_locked: bool = False
    params_hash: str = ''
    paused: bool = False
    pause_reason: str = ''
    frozen: bool = False
    pause_timestamp: float = 0.0  # R17-LC-03修复: 记录暂停时间戳，用于resume前冷却期检查
    last_direction: str = ''
    last_open_reason: str = ''
    _closed_pnl_sum: float = 0.0
    _closed_count: int = 0
    _win_pnl_sum: float = 0.0
    _loss_pnl_sum: float = 0.0
    _win_count: int = 0
    _loss_count: int = 0
    _kahan_c_total: float = 0.0
    _kahan_c_closed: float = 0.0
    _kahan_c_win: float = 0.0
    _kahan_c_loss: float = 0.0
    _observation_period_until: Optional[float] = None
    _observation_position_scale: float = 1.0
    _sub_strategy_stats: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        's1_hft': {'trade_count': 0, 'pnl': 0.0, 'wins': 0},
        's2_resonance': {'trade_count': 0, 'pnl': 0.0, 'wins': 0},
    })

    def __post_init__(self):
        if self.state is None:
            self.state = SlotState.INACTIVE

    def record_sub_strategy_trade(self, sub_id: str, pnl: float):
        """R16-P1-17修复: 记录子策略独立交易统计"""
        if sub_id in self._sub_strategy_stats:
            stats = self._sub_strategy_stats[sub_id]
            stats['trade_count'] += 1
            stats['pnl'] += pnl
            if pnl > 0:
                stats['wins'] += 1

    def get_sub_strategy_ev(self, sub_id: str) -> float:
        """R16-P1-17修复: 获取子策略独立EV"""
        if sub_id in self._sub_strategy_stats:
            stats = self._sub_strategy_stats[sub_id]
            if stats['trade_count'] > 0:
                return stats['pnl'] / stats['trade_count']
        return 0.0

    def validate_state_transition(self, new_state) -> bool:
        """INV-07/INV-STA-02: 验证策略状态转换是否合法"""
        if self.state == new_state:
            return True
        allowed = VALID_STRATEGY_TRANSITIONS.get(self.state, set())
        if new_state not in allowed:
            logger.warning(
                "[INV-07] 非法策略状态转换: %s→%s (允许: %s), strategy_id=%s",
                self.state, new_state, allowed, self.strategy_id,
            )
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CapitalRoute:
    """资金路由配置

    P1-R10-02修复: 字段修改需通过update_from_dict()方法，
    确保所有修改经过ecosystem锁保护。
    """
    master_base: float = 0.60
    reverse_base: float = 0.25
    other_base: float = 0.15
    spring_base: float = 0.15
    arbitrage_base: float = 0.10
    market_making_base: float = 0.10
    divergence_base: float = 0.10
    alpha_ratio: float = 0.0  # P1-R9-13修复: Alpha占比数据，用于资金路由调整
    dynamic_enabled: bool = True
    master_active_boost: float = 0.15
    min_maintenance_ratio: float = 0.05
    _frozen: bool = field(default=False, repr=False)  # P1-R10-02: 冻结标记

    def __post_init__(self):
        object.__setattr__(self, '_frozen', False)

    def __setattr__(self, name: str, value: Any) -> None:
        """P1-R10-02修复: 冻结后禁止外部直接修改字段"""
        try:
            frozen = object.__getattribute__(self, '_frozen')
        except AttributeError:
            frozen = False
        if frozen and name != '_frozen':
            raise RuntimeError(
                f"[P1-R10-02] CapitalRoute已冻结，禁止直接修改字段'{name}'。"
                f"请使用update_from_dict()方法，确保修改经过ecosystem锁保护。"
            )
        object.__setattr__(self, name, value)

    def freeze(self) -> None:
        """冻结字段，阻止外部直接修改"""
        object.__setattr__(self, '_frozen', True)

    def unfreeze(self) -> None:
        """解冻字段，仅限ecosystem内部使用"""
        object.__setattr__(self, '_frozen', False)

    def update_from_dict(self, updates: Dict[str, Any]) -> None:
        """安全更新字段（由ecosystem锁保护调用）"""
        object.__setattr__(self, '_frozen', False)
        try:
            for k, v in updates.items():
                if hasattr(self, k) and k not in ('_frozen',):
                    object.__setattr__(self, k, v)
        finally:
            object.__setattr__(self, '_frozen', True)

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if k != '_frozen'}

    @property
    def total_base(self) -> float:
        return self.master_base + self.reverse_base + self.other_base


@dataclass(slots=True)
class EcosystemTradeRecord:
    """生态系统交易记录"""
    trade_id: str
    strategy_id: str
    timestamp: str
    action: str = ''
    instrument_id: str = ''
    direction: str = ''
    price: float = 0.0
    quantity: int = 0
    open_reason: str = ''
    pnl: float = 0.0
    net_pnl: float = 0.0
    is_open: bool = True
    market_state: str = ''
    target_plr: float = 0.0
    actual_plr: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
