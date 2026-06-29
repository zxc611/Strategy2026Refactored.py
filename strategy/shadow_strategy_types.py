from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict


@dataclass(slots=True)
class ShadowTradeRecord:
    """影子策略交易记录"""
    trade_id: str
    shadow_type: str
    timestamp: str
    instrument_id: str = ""
    direction: str = ""
    price: float = 0.0
    quantity: int = 0
    open_reason: str = ""
    close_reason: str = ""
    pnl: float = 0.0
    commission: float = 0.0
    net_pnl: float = 0.0
    is_open: bool = True
    market_state: str = ""
    signal_strength: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AlphaMetrics:
    """Alpha比率监控指标 — 覆盖6策略组×3变体"""
    timestamp: str
    # S1-S6 各组master/shadow_a/shadow_b sharpe
    s1_master_sharpe: float = 0.0
    s1_shadow_a_sharpe: float = 0.0
    s1_shadow_b_sharpe: float = 0.0
    s2_master_sharpe: float = 0.0
    s2_shadow_a_sharpe: float = 0.0
    s2_shadow_b_sharpe: float = 0.0
    s3_master_sharpe: float = 0.0
    s3_shadow_a_sharpe: float = 0.0
    s3_shadow_b_sharpe: float = 0.0
    s4_master_sharpe: float = 0.0
    s4_shadow_a_sharpe: float = 0.0
    s4_shadow_b_sharpe: float = 0.0
    s5_master_sharpe: float = 0.0
    s5_shadow_a_sharpe: float = 0.0
    s5_shadow_b_sharpe: float = 0.0
    s6_master_sharpe: float = 0.0
    s6_shadow_a_sharpe: float = 0.0
    s6_shadow_b_sharpe: float = 0.0
    # P1-R8-19修复: 各策略组独立Alpha比率
    s1_alpha: float = 0.0
    s2_alpha: float = 0.0
    s3_alpha: float = 0.0
    s4_alpha: float = 0.0
    s5_alpha: float = 0.0
    s6_alpha: float = 0.0
    # 聚合指标(向后兼容，取S2组)
    master_sharpe: float = 0.0
    shadow_a_sharpe: float = 0.0
    shadow_b_sharpe: float = 0.0
    master_max_drawdown: float = 1.0
    alpha_ratio: float = 0.0
    master_expected_value: float = 0.0
    shadow_a_expected_value: float = 0.0
    shadow_b_expected_value: float = 0.0
    alpha_ratio_prev: float = 0.0
    alpha_ratio_decline_pct: float = 0.0
    consecutive_decline_windows: int = 0
    degradation_triggered: bool = False
    absolute_ev_breached: bool = False
    master_sharpe_eliminate: bool = False  # P1-2修复：主策略Sharpe<=0时ELIMINATE标记
    jensen_alpha: float = 0.0  # R19-ATT-02: 标准Jensen's Alpha（CAPM回归）

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ShadowParamsSnapshot:
    """影子策略参数快照（独立锁定）"""
    shadow_type: str
    locked_at: str
    param_set: Dict[str, Any] = field(default_factory=dict)
    param_yaml_path: str = ""
    param_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

__all__ = [
    'ShadowTradeRecord',
    'AlphaMetrics',
    'ShadowParamsSnapshot',
]
