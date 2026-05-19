"""
模式切换引擎 — 统一入口

解决:
  G1: 封装 ModeEngine 统一入口类，聚合各组件模式相关调用
  G2: ModeConfig dataclass 替代 CAPITAL_SCALE_CONFIGS 字典
  G3: 原子性模式切换（预检查 + 回滚机制）
  G4: 凯利公式仓位计算
  G5: time_decay_cutoff 期权时间价值衰减过滤
  G6: ExitRuleEngine / DrawdownManager 显式抽象
"""
from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple


class CapitalMode(Enum):
    SHARPE_MAX = auto()
    PROFIT_RATIO = auto()
    BALANCED = auto()


class TakeProfitMethod(Enum):
    TRAILING = "trailing"
    FIXED = "fixed"
    TIERED = "tiered"


class StopLossMethod(Enum):
    VOLATILITY = "volatility"
    FIXED = "fixed"
    TIME_DECAY = "time_decay"


class DrawdownAction(Enum):
    REDUCE_SIZE = "reduce_size"
    HALT_NEW = "halt_new"
    FULL_STOP = "full_stop"


class PyramidingRule(Enum):
    NONE = "none"
    TREND_CONFIRMED = "trend_confirmed"
    VOLATILITY_EXPANSION = "volatility_expansion"


@dataclass(frozen=True)
class ModeConfig:
    mode: CapitalMode
    max_positions: int
    single_position_cap: float
    take_profit_method: TakeProfitMethod
    stop_loss_method: StopLossMethod
    pyramiding: bool
    pyramiding_rule: PyramidingRule
    drawdown_action: DrawdownAction
    recovery_target: float
    min_signal_strength: float
    time_decay_cutoff: float
    profitability_mode: str = ""
    profitability_weights: Tuple[Tuple[str, float], ...] = ()
    win_loss_ratio_full_score_at: float = 2.0
    profit_factor_full_score_at: float = 1.5
    sharpe_full_score_at: float = 2.0
    calmar_full_score_at: float = 2.0
    recovery_efficiency_full_score_at: float = 2.0
    max_consecutive_losses_full: int = 3
    max_consecutive_losses_zero: int = 10
    drawdown_recovery_max_hours: float = 48.0
    extreme_max_recovery_hours: float = 72.0
    overall_pass_threshold: float = 0.65
    overall_conditional_threshold: float = 0.50
    kelly_fraction: float = 0.0
    min_estimated_plr: float = 0.0
    plr_filter_enabled: bool = False
    consecutive_loss_limit: int = 7
    recovery_timeout_seconds: float = 3600.0
    time_decay_min_days: int = 5
    risk_pct_default: float = 0.02
    asymmetric_risk_enabled: bool = False
    profit_multiplier: float = 1.5
    loss_multiplier: float = 0.7
    tvf_enabled: bool = False
    tvf_l1_weight: float = 0.40
    tvf_l2_weight: float = 0.35
    tvf_l3_weight: float = 0.25
    tvf_sortino_threshold: float = 1.5
    tvf_calmar_threshold: float = 0.8
    tvf_sharpe_threshold: float = 1.2
    tvf_gamma_threshold: float = 0.05
    tvf_theta_threshold: float = -0.02
    tvf_vega_threshold: float = 0.10
    tvf_sortino_scale: float = 0.50
    tvf_calmar_scale: float = 0.30
    tvf_sharpe_scale: float = 0.40
    tvf_ofi_scale: float = 3.00
    tvf_smf_scale: float = 2.00
    tvf_gamma_scale: float = 0.02
    tvf_theta_scale: float = 0.01
    tvf_vega_scale: float = 0.05
    tvf_l1_inner_sortino_weight: float = 0.40
    tvf_l1_inner_calmar_weight: float = 0.30
    tvf_l1_inner_sharpe_weight: float = 0.30
    tvf_l2_inner_ofi_weight: float = 0.40
    tvf_l2_inner_cvd_weight: float = 0.30
    tvf_l2_inner_smf_weight: float = 0.30
    tvf_l3_inner_delta_weight: float = 0.30
    tvf_l3_inner_gamma_weight: float = 0.25
    tvf_l3_inner_theta_weight: float = 0.25
    tvf_l3_inner_vega_weight: float = 0.20


# ============================================================================
# TVF公共默认参数 — 唯一权威定义在tvf_param_loader.TVF_DEFAULT_PARAMS
# ============================================================================
_DEFAULT_TVF_PARAMS = None


def _get_default_tvf_params():
    global _DEFAULT_TVF_PARAMS
    if _DEFAULT_TVF_PARAMS is None:
        from ali2026v3_trading.tvf_param_loader import TVF_DEFAULT_PARAMS
        _DEFAULT_TVF_PARAMS = TVF_DEFAULT_PARAMS
    return _DEFAULT_TVF_PARAMS


def _make_mode_config(
    mode: CapitalMode,
    max_positions: int,
    single_position_cap: float,
    take_profit_method: TakeProfitMethod,
    stop_loss_method: StopLossMethod,
    pyramiding: bool,
    pyramiding_rule: PyramidingRule,
    drawdown_action: DrawdownAction,
    recovery_target: float,
    min_signal_strength: float,
    time_decay_cutoff: float,
    profitability_mode: str,
    profitability_weights: Tuple[Tuple[str, float], ...],
    win_loss_ratio_full_score_at: float,
    profit_factor_full_score_at: float,
    recovery_efficiency_full_score_at: float,
    max_consecutive_losses_full: int,
    max_consecutive_losses_zero: int,
    drawdown_recovery_max_hours: float,
    extreme_max_recovery_hours: float,
    overall_pass_threshold: float,
    overall_conditional_threshold: float,
    kelly_fraction: float,
    min_estimated_plr: float,
    plr_filter_enabled: bool,
    consecutive_loss_limit: int,
    recovery_timeout_seconds: float,
    asymmetric_risk_enabled: bool,
    tvf_enabled: bool,
    sharpe_full_score_at: float = 2.0,
    calmar_full_score_at: float = 2.0,
) -> ModeConfig:
    """ModeConfig工厂函数 — 自动合并TVF公共默认参数，消除重复定义"""
    # ✅ P0-20集成: 优先从TVFParamLoader(YAML参数池)加载，失败回退到_DEFAULT_TVF_PARAMS
    _tvf_kwargs = dict(_get_default_tvf_params())
    try:
        from ali2026v3_trading.tvf_param_loader import get_tvf_param_loader
        _tvf_loader = get_tvf_param_loader()
        _tvf_params = _tvf_loader.get_params()
        if _tvf_params and _tvf_params.get('tvf_enabled', False):
            _flat = {
                'tvf_l1_weight': _tvf_params.get('layer_weights', {}).get('l1_risk_return', _tvf_kwargs['tvf_l1_weight']),
                'tvf_l2_weight': _tvf_params.get('layer_weights', {}).get('l2_market_micro', _tvf_kwargs['tvf_l2_weight']),
                'tvf_l3_weight': _tvf_params.get('layer_weights', {}).get('l3_option_greeks', _tvf_kwargs['tvf_l3_weight']),
                'tvf_sortino_threshold': _tvf_params.get('l1_tri_validation', {}).get('sortino_tri_center', _tvf_kwargs['tvf_sortino_threshold']),
                'tvf_calmar_threshold': _tvf_params.get('l1_tri_validation', {}).get('calmar_tri_center', _tvf_kwargs['tvf_calmar_threshold']),
                'tvf_sharpe_threshold': _tvf_params.get('l1_tri_validation', {}).get('sharpe_tri_center', _tvf_kwargs['tvf_sharpe_threshold']),
                'tvf_sortino_scale': _tvf_params.get('l1_tri_validation', {}).get('sortino_tri_scale', _tvf_kwargs['tvf_sortino_scale']),
                'tvf_calmar_scale': _tvf_params.get('l1_tri_validation', {}).get('calmar_tri_scale', _tvf_kwargs['tvf_calmar_scale']),
                'tvf_sharpe_scale': _tvf_params.get('l1_tri_validation', {}).get('sharpe_tri_scale', _tvf_kwargs['tvf_sharpe_scale']),
                'tvf_ofi_scale': _tvf_params.get('l2_order_flow', {}).get('ofi_scale', _tvf_kwargs['tvf_ofi_scale']),
                'tvf_smf_scale': _tvf_params.get('l2_order_flow', {}).get('smf_scale', _tvf_kwargs['tvf_smf_scale']),
                'tvf_gamma_threshold': _tvf_params.get('l3_greeks', {}).get('gamma_threshold', _tvf_kwargs['tvf_gamma_threshold']),
                'tvf_theta_threshold': _tvf_params.get('l3_greeks', {}).get('theta_threshold', _tvf_kwargs['tvf_theta_threshold']),
                'tvf_vega_threshold': _tvf_params.get('l3_greeks', {}).get('vega_threshold', _tvf_kwargs['tvf_vega_threshold']),
                'tvf_gamma_scale': _tvf_params.get('l3_greeks', {}).get('gamma_scale', _tvf_kwargs['tvf_gamma_scale']),
                'tvf_theta_scale': _tvf_params.get('l3_greeks', {}).get('theta_scale', _tvf_kwargs['tvf_theta_scale']),
                'tvf_vega_scale': _tvf_params.get('l3_greeks', {}).get('vega_scale', _tvf_kwargs['tvf_vega_scale']),
                'tvf_l1_inner_sortino_weight': _tvf_params.get('l1_tri_validation', {}).get('inner_weights', {}).get('sortino', _tvf_kwargs['tvf_l1_inner_sortino_weight']),
                'tvf_l1_inner_calmar_weight': _tvf_params.get('l1_tri_validation', {}).get('inner_weights', {}).get('calmar', _tvf_kwargs['tvf_l1_inner_calmar_weight']),
                'tvf_l1_inner_sharpe_weight': _tvf_params.get('l1_tri_validation', {}).get('inner_weights', {}).get('sharpe', _tvf_kwargs['tvf_l1_inner_sharpe_weight']),
                'tvf_l2_inner_ofi_weight': _tvf_params.get('l2_order_flow', {}).get('inner_weights', {}).get('ofi', _tvf_kwargs['tvf_l2_inner_ofi_weight']),
                'tvf_l2_inner_cvd_weight': _tvf_params.get('l2_order_flow', {}).get('inner_weights', {}).get('cvd_divergence', _tvf_kwargs['tvf_l2_inner_cvd_weight']),
                'tvf_l2_inner_smf_weight': _tvf_params.get('l2_order_flow', {}).get('inner_weights', {}).get('smart_money_flow', _tvf_kwargs['tvf_l2_inner_smf_weight']),
                'tvf_l3_inner_delta_weight': _tvf_params.get('l3_greeks', {}).get('inner_weights', {}).get('delta', _tvf_kwargs['tvf_l3_inner_delta_weight']),
                'tvf_l3_inner_gamma_weight': _tvf_params.get('l3_greeks', {}).get('inner_weights', {}).get('gamma', _tvf_kwargs['tvf_l3_inner_gamma_weight']),
                'tvf_l3_inner_theta_weight': _tvf_params.get('l3_greeks', {}).get('inner_weights', {}).get('theta', _tvf_kwargs['tvf_l3_inner_theta_weight']),
                'tvf_l3_inner_vega_weight': _tvf_params.get('l3_greeks', {}).get('inner_weights', {}).get('vega', _tvf_kwargs['tvf_l3_inner_vega_weight']),
            }
            _tvf_kwargs.update(_flat)
            logging.info("[ModeEngine] TVF参数已从参数池YAML加载（P0-20集成）")
    except Exception as _tvf_err:
        logging.debug("[ModeEngine] TVFParamLoader加载失败，使用默认TVF参数: %s", _tvf_err)

    return ModeConfig(
        mode=mode,
        max_positions=max_positions,
        single_position_cap=single_position_cap,
        take_profit_method=take_profit_method,
        stop_loss_method=stop_loss_method,
        pyramiding=pyramiding,
        pyramiding_rule=pyramiding_rule,
        drawdown_action=drawdown_action,
        recovery_target=recovery_target,
        min_signal_strength=min_signal_strength,
        time_decay_cutoff=time_decay_cutoff,
        profitability_mode=profitability_mode,
        profitability_weights=profitability_weights,
        win_loss_ratio_full_score_at=win_loss_ratio_full_score_at,
        profit_factor_full_score_at=profit_factor_full_score_at,
        sharpe_full_score_at=sharpe_full_score_at,
        calmar_full_score_at=calmar_full_score_at,
        recovery_efficiency_full_score_at=recovery_efficiency_full_score_at,
        max_consecutive_losses_full=max_consecutive_losses_full,
        max_consecutive_losses_zero=max_consecutive_losses_zero,
        drawdown_recovery_max_hours=drawdown_recovery_max_hours,
        extreme_max_recovery_hours=extreme_max_recovery_hours,
        overall_pass_threshold=overall_pass_threshold,
        overall_conditional_threshold=overall_conditional_threshold,
        kelly_fraction=kelly_fraction,
        min_estimated_plr=min_estimated_plr,
        plr_filter_enabled=plr_filter_enabled,
        consecutive_loss_limit=consecutive_loss_limit,
        recovery_timeout_seconds=recovery_timeout_seconds,
        asymmetric_risk_enabled=asymmetric_risk_enabled,
        profit_multiplier=1.5,
        loss_multiplier=0.7,
        tvf_enabled=tvf_enabled,
        **_tvf_kwargs,
    )


SHARPE_CONFIG = _make_mode_config(
    mode=CapitalMode.SHARPE_MAX,
    max_positions=12,
    single_position_cap=0.08,
    take_profit_method=TakeProfitMethod.TIERED,
    stop_loss_method=StopLossMethod.VOLATILITY,
    pyramiding=False,
    pyramiding_rule=PyramidingRule.NONE,
    drawdown_action=DrawdownAction.REDUCE_SIZE,
    recovery_target=1.0,
    min_signal_strength=0.75,
    time_decay_cutoff=0.03,
    profitability_mode="sharpe_dominant",
    profitability_weights=(
        ("sharpe", 0.30), ("calmar", 0.30),
        ("win_loss_ratio", 0.10), ("profit_factor", 0.10),
        ("recovery_efficiency", 0.10), ("consecutive_loss_tolerance", 0.10),
    ),
    win_loss_ratio_full_score_at=2.0,
    profit_factor_full_score_at=1.5,
    recovery_efficiency_full_score_at=2.0,
    max_consecutive_losses_full=5,
    max_consecutive_losses_zero=15,
    drawdown_recovery_max_hours=24.0,
    extreme_max_recovery_hours=24.0,
    overall_pass_threshold=0.75,
    overall_conditional_threshold=0.60,
    kelly_fraction=0.33,
    min_estimated_plr=0.0,
    plr_filter_enabled=False,
    consecutive_loss_limit=10,
    recovery_timeout_seconds=7200.0,
    asymmetric_risk_enabled=False,
    tvf_enabled=True,
)

PROFIT_CONFIG = _make_mode_config(
    mode=CapitalMode.PROFIT_RATIO,
    max_positions=3,
    single_position_cap=0.40,
    take_profit_method=TakeProfitMethod.TRAILING,
    stop_loss_method=StopLossMethod.FIXED,
    pyramiding=True,
    pyramiding_rule=PyramidingRule.TREND_CONFIRMED,
    drawdown_action=DrawdownAction.HALT_NEW,
    recovery_target=2.0,
    min_signal_strength=0.55,
    time_decay_cutoff=0.08,
    profitability_mode="profit_loss_ratio",
    profitability_weights=(
        ("win_loss_ratio", 0.40), ("profit_factor", 0.25),
        ("recovery_efficiency", 0.20), ("consecutive_loss_tolerance", 0.15),
    ),
    win_loss_ratio_full_score_at=2.0,
    profit_factor_full_score_at=1.5,
    recovery_efficiency_full_score_at=2.0,
    max_consecutive_losses_full=3,
    max_consecutive_losses_zero=10,
    drawdown_recovery_max_hours=48.0,
    extreme_max_recovery_hours=72.0,
    overall_pass_threshold=0.65,
    overall_conditional_threshold=0.50,
    kelly_fraction=0.0,
    min_estimated_plr=2.0,
    plr_filter_enabled=True,
    consecutive_loss_limit=5,
    recovery_timeout_seconds=1800.0,
    asymmetric_risk_enabled=True,
    tvf_enabled=True,
)

BALANCED_CONFIG = _make_mode_config(
    mode=CapitalMode.BALANCED,
    max_positions=6,
    single_position_cap=0.15,
    take_profit_method=TakeProfitMethod.TIERED,
    stop_loss_method=StopLossMethod.VOLATILITY,
    pyramiding=True,
    pyramiding_rule=PyramidingRule.TREND_CONFIRMED,
    drawdown_action=DrawdownAction.HALT_NEW,
    recovery_target=1.5,
    min_signal_strength=0.65,
    time_decay_cutoff=0.05,
    profitability_mode="balanced",
    profitability_weights=(
        ("win_loss_ratio", 0.30), ("profit_factor", 0.10),
        ("sharpe", 0.20), ("calmar", 0.20),
        ("recovery_efficiency", 0.10), ("consecutive_loss_tolerance", 0.10),
    ),
    win_loss_ratio_full_score_at=2.0,
    profit_factor_full_score_at=1.5,
    recovery_efficiency_full_score_at=2.0,
    max_consecutive_losses_full=4,
    max_consecutive_losses_zero=12,
    drawdown_recovery_max_hours=36.0,
    extreme_max_recovery_hours=48.0,
    overall_pass_threshold=0.70,
    overall_conditional_threshold=0.55,
    kelly_fraction=0.0,
    min_estimated_plr=1.5,
    plr_filter_enabled=True,
    consecutive_loss_limit=7,
    recovery_timeout_seconds=3600.0,
    asymmetric_risk_enabled=False,
    tvf_enabled=False,
)

CAPITAL_MODE_CONFIGS: Dict[str, ModeConfig] = {
    'small': PROFIT_CONFIG,
    'medium': BALANCED_CONFIG,
    'large': SHARPE_CONFIG,
}


class ExitRuleEngine:
    def __init__(self, method: TakeProfitMethod, stop_method: StopLossMethod):
        self._tp_method = method
        self._sl_method = stop_method

    @property
    def take_profit_method(self) -> TakeProfitMethod:
        return self._tp_method

    @property
    def stop_loss_method(self) -> StopLossMethod:
        return self._sl_method

    def compute_take_profit_levels(
        self, entry_price: float, volatility_1d: float, size: float,
        direction: str = 'BUY',
    ) -> List[Dict[str, Any]]:
        """计算止盈价位，支持BUY和SELL两个方向"""
        sign = 1.0 if direction == 'BUY' else -1.0
        if self._tp_method == TakeProfitMethod.TIERED:
            return [
                {'pct': 0.50, 'price': entry_price + sign * volatility_1d, 'volume_ratio': 0.50},
                {'pct': 0.30, 'price': entry_price + sign * 2 * volatility_1d, 'volume_ratio': 0.30},
                {'pct': 0.20, 'price': None, 'volume_ratio': 0.20},
            ]
        elif self._tp_method == TakeProfitMethod.TRAILING:
            return [
                {'activation': entry_price * (1.05 if direction == 'BUY' else 0.95), 'trail_pct': 0.10},
            ]
        else:
            return [
                {'price': entry_price * (1.10 if direction == 'BUY' else 0.90), 'volume_ratio': 1.0},
            ]

    def compute_stop_loss(
        self, entry_price: float, stop_distance: float, volatility_20d: float = 0.0,
        direction: str = 'BUY',
    ) -> Dict[str, Any]:
        """计算止损价位，支持BUY和SELL两个方向"""
        sign = 1.0 if direction == 'BUY' else -1.0
        if self._sl_method == StopLossMethod.VOLATILITY and volatility_20d > 0:
            sl_distance = max(stop_distance, entry_price * volatility_20d * 1.5)
            return {'method': 'volatility', 'price': entry_price - sign * sl_distance, 'distance': sl_distance}
        elif self._sl_method == StopLossMethod.TIME_DECAY:
            sl_distance = stop_distance
            return {'method': 'time_decay', 'price': entry_price - sign * sl_distance, 'distance': sl_distance}
        else:
            return {'method': 'fixed', 'price': entry_price - sign * stop_distance, 'distance': stop_distance}


class DrawdownManager:
    def __init__(self, action: DrawdownAction, recovery_target: float = 1.0):
        self._action = action
        self._recovery_target = recovery_target
        self._current_drawdown: float = 0.0
        self._drawdown_low: float = 0.0
        self._recovery_start_equity: float = 0.0

    @property
    def action(self) -> DrawdownAction:
        return self._action

    @property
    def recovery_target(self) -> float:
        return self._recovery_target

    def update_drawdown(self, current_drawdown: float) -> None:
        self._current_drawdown = current_drawdown
        if current_drawdown < self._drawdown_low:
            self._drawdown_low = current_drawdown

    def should_reduce_size(self) -> bool:
        return self._action == DrawdownAction.REDUCE_SIZE and self._current_drawdown < 0

    def should_halt_new(self) -> bool:
        return self._action == DrawdownAction.HALT_NEW and self._current_drawdown < 0

    def should_full_stop(self) -> bool:
        return self._action == DrawdownAction.FULL_STOP and self._current_drawdown < -0.05

    def is_recovered(self, current_equity: float, peak_equity: float) -> bool:
        if self._drawdown_low >= 0:
            return True
        required_recovery = abs(self._drawdown_low) * self._recovery_target
        actual_recovery = current_equity - (peak_equity + self._drawdown_low)
        return actual_recovery >= required_recovery


class SixDimPositionAdjustmentFactor:
    """六维仓位调整因子 — 将风险指标、订单流、希腊字母内化为仓位乘数

    六维体系:
    L1 风险收益层 (三角验证): Sortino + Calmar + Sharpe
    L2 市场微观层 (订单流): OFI + CVD偏离 + 智能资金流向
    L3 期权风险层 (希腊字母): Delta暴露 + Gamma风险 + Theta衰减 + Vega波动
    """

    def compute_adjustment(
        self,
        config: ModeConfig,
        sortino: float = 0.0,
        calmar: float = 0.0,
        sharpe: float = 0.0,
        ofi_score: float = 0.0,
        cvd_divergence: float = 0.0,
        smart_money_flow: float = 0.0,
        delta_exposure: float = 0.0,
        gamma_risk: float = 0.0,
        theta_decay: float = 0.0,
        vega_exposure: float = 0.0,
    ) -> float:
        """计算六维仓位调整因子 [0, 1]"""
        if not config.tvf_enabled:
            return 1.0

        # === L1: 三角验证因子 ===
        sortino_factor = self._sigmoid_adjust(
            sortino, config.tvf_sortino_threshold, config.tvf_sortino_scale
        )
        calmar_factor = self._sigmoid_adjust(
            calmar, config.tvf_calmar_threshold, config.tvf_calmar_scale
        )
        sharpe_factor = self._sigmoid_adjust(
            sharpe, config.tvf_sharpe_threshold, config.tvf_sharpe_scale
        )
        l1_tvf = (
            config.tvf_l1_inner_sortino_weight * sortino_factor
            + config.tvf_l1_inner_calmar_weight * calmar_factor
            + config.tvf_l1_inner_sharpe_weight * sharpe_factor
        )

        # === L2: 订单流因子 ===
        ofi_norm = 1.0 / (1.0 + math.exp(-ofi_score / config.tvf_ofi_scale))
        cvd_factor = min(1.0, max(0.0, 0.5 + cvd_divergence * 0.5))
        smf_norm = 1.0 / (1.0 + math.exp(-smart_money_flow / config.tvf_smf_scale))
        l2_tvf = (
            config.tvf_l2_inner_ofi_weight * ofi_norm
            + config.tvf_l2_inner_cvd_weight * cvd_factor
            + config.tvf_l2_inner_smf_weight * smf_norm
        )

        # === L3: 希腊字母因子 ===
        delta_factor = 1.0 - abs(delta_exposure)
        gamma_factor = 1.0 / (
            1.0 + math.exp((gamma_risk - config.tvf_gamma_threshold) / config.tvf_gamma_scale)
        )
        theta_factor = 1.0 / (
            1.0 + math.exp((theta_decay - config.tvf_theta_threshold) / config.tvf_theta_scale)
        )
        vega_factor = 1.0 / (
            1.0 + math.exp((vega_exposure - config.tvf_vega_threshold) / config.tvf_vega_scale)
        )
        l3_tvf = (
            config.tvf_l3_inner_delta_weight * delta_factor
            + config.tvf_l3_inner_gamma_weight * gamma_factor
            + config.tvf_l3_inner_theta_weight * theta_factor
            + config.tvf_l3_inner_vega_weight * vega_factor
        )

        # === 六维综合 ===
        final_tvf = (
            config.tvf_l1_weight * l1_tvf
            + config.tvf_l2_weight * l2_tvf
            + config.tvf_l3_weight * l3_tvf
        )
        return min(1.0, max(0.0, final_tvf))

    @staticmethod
    def _sigmoid_adjust(value: float, threshold: float, scale: float) -> float:
        """Sigmoid调整: 阈值处=0.5, 2倍阈值处≈0.88"""
        return 1.0 / (1.0 + math.exp(-(value - threshold) / scale))


class DefensiveDrawdownChecker:
    """防御性减仓检查器 — 当实时风险指标显著低于入场时触发减仓"""

    def check(
        self,
        current_sortino: float,
        current_calmar: float,
        entry_sortino: float,
        entry_calmar: float,
        decay_threshold: float = 0.5,
    ) -> float:
        """
        返回减仓因子 [0, 1]
        1.0 = 不减仓, 0.5 = 减仓50%, 0.0 = 清仓
        """
        if entry_sortino <= 0 or entry_calmar <= 0:
            return 1.0

        sortino_decay = current_sortino / entry_sortino
        calmar_decay = current_calmar / entry_calmar

        if sortino_decay < decay_threshold or calmar_decay < decay_threshold:
            return min(sortino_decay, calmar_decay)

        return 1.0


def kelly_fraction(win_rate: float, win_loss_ratio: float, fraction: float = 1.0) -> float:
    if win_rate <= 0 or win_rate >= 1 or win_loss_ratio <= 0:
        return 0.0
    kelly = (win_rate * win_loss_ratio - (1 - win_rate)) / win_loss_ratio
    return max(0.0, kelly * fraction)


def kelly_position_size(
    equity: float, win_rate: float, win_loss_ratio: float,
    entry_price: float, stop_price: float,
    kelly_fraction_param: float = 0.33, max_cap: float = 0.08,
) -> float:
    if equity <= 0 or kelly_fraction_param <= 0 or entry_price <= 0 or stop_price <= 0:
        return 0.0
    kf = kelly_fraction(win_rate, win_loss_ratio, kelly_fraction_param)
    if kf <= 0:
        return 0.0
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share < 1e-10:
        return 0.0
    risk_amount = equity * kf
    shares = risk_amount / risk_per_share
    max_shares = equity * max_cap / entry_price
    return min(shares, max_shares)


class ModeEngine:
    _instance: Optional['ModeEngine'] = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> 'ModeEngine':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._lock:
            cls._instance = None

    @classmethod
    def create_engine(cls, scale_str: str = 'medium') -> 'ModeEngine':
        engine = cls()
        config = CAPITAL_MODE_CONFIGS.get(scale_str)
        if config is not None:
            engine._config = config
            engine._scale_str = scale_str
            engine._exit_engine = ExitRuleEngine(config.take_profit_method, config.stop_loss_method)
            engine._drawdown_mgr = DrawdownManager(config.drawdown_action, config.recovery_target)
        return engine

    def __init__(self):
        self._config: Optional[ModeConfig] = None
        self._scale_str: str = 'medium'
        self._exit_engine: Optional[ExitRuleEngine] = None
        self._drawdown_mgr: Optional[DrawdownManager] = None
        self._propagation_lock = threading.RLock()
        self._propagated_components: List[str] = []
        self._component_registry: Dict[str, Any] = {}

    @property
    def config(self) -> Optional[ModeConfig]:
        return self._config

    @property
    def scale_str(self) -> str:
        return self._scale_str

    @property
    def exit_engine(self) -> Optional[ExitRuleEngine]:
        return self._exit_engine

    @property
    def drawdown_manager(self) -> Optional[DrawdownManager]:
        return self._drawdown_mgr

    def register_component(self, name: str, component: Any) -> None:
        self._component_registry[name] = component

    def switch_mode(self, scale_str: str) -> Dict[str, Any]:
        with self._propagation_lock:
            config = CAPITAL_MODE_CONFIGS.get(scale_str)
            if config is None:
                logging.error('[ModeEngine] Unknown scale: %s', scale_str)
                return {'success': False, 'error': f'Unknown scale: {scale_str}'}

            old_config = self._config
            old_scale = self._scale_str
            propagated = []
            errors = []

            try:
                self._apply_to_risk_service(config, scale_str, propagated, errors)
                self._apply_to_signal_service(config, scale_str, propagated, errors)
                self._apply_to_state_param_manager(scale_str, propagated, errors)
                self._apply_to_box_spring_strategy(scale_str, propagated, errors)
            except Exception as e:
                logging.error('[ModeEngine] Propagation failed, rolling back: %s', e)
                self._rollback(old_config, old_scale, propagated)
                return {'success': False, 'error': str(e), 'rolled_back': True}

            self._config = config
            self._scale_str = scale_str
            self._exit_engine = ExitRuleEngine(config.take_profit_method, config.stop_loss_method)
            self._drawdown_mgr = DrawdownManager(config.drawdown_action, config.recovery_target)
            self._propagated_components = propagated

            logging.info(
                '[ModeEngine] Mode switched: %s -> %s (mode=%s, propagated=%s)',
                old_scale, scale_str, config.mode.name, propagated,
            )
            return {
                'success': True,
                'old_scale': old_scale,
                'new_scale': scale_str,
                'mode': config.mode.name,
                'propagated': propagated,
                'errors': errors,
            }

    def filter_signal_by_mode(
        self, signal_type: str, estimated_plr: float = 0.0,
        signal_strength: float = 0.0, days_to_expiry: int = 999,
    ) -> Tuple[bool, str]:
        if self._config is None:
            return True, ''

        if self._config.plr_filter_enabled and self._config.min_estimated_plr > 0:
            if signal_type in ('BUY', 'SELL') and estimated_plr < self._config.min_estimated_plr:
                return False, f'PLR filter: {estimated_plr:.2f} < {self._config.min_estimated_plr:.2f}'

        if self._config.min_signal_strength > 0 and signal_strength > 0:
            if signal_strength < self._config.min_signal_strength:
                return False, f'Signal strength: {signal_strength:.2f} < {self._config.min_signal_strength:.2f}'

        if self._config.time_decay_cutoff > 0 and days_to_expiry < 999:
            if days_to_expiry <= self._config.time_decay_min_days:
                return False, f'Time decay: days_to_expiry={days_to_expiry} <= {self._config.time_decay_min_days}'

        return True, ''

    def compute_position_size(
        self, equity: float, entry_price: float, stop_price: float,
        win_rate: float = 0.0, win_loss_ratio: float = 0.0,
        sortino: float = 0.0, calmar: float = 0.0, sharpe: float = 0.0,
        ofi_score: float = 0.0, cvd_divergence: float = 0.0,
        smart_money_flow: float = 0.0,
        delta_exposure: float = 0.0, gamma_risk: float = 0.0,
        theta_decay: float = 0.0, vega_exposure: float = 0.0,
    ) -> float:
        """计算仓位大小，集成六维仓位调整因子"""
        if self._config is None:
            return 0.0

        # 0. 前置边界检查
        if equity <= 0 or entry_price <= 0 or stop_price <= 0:
            return 0.0

        # 1. 基础仓位（凯利或固定风险）
        if self._config.kelly_fraction > 0 and win_rate > 0 and win_loss_ratio > 0:
            base_size = kelly_position_size(
                equity, win_rate, win_loss_ratio,
                entry_price, stop_price,
                kelly_fraction_param=self._config.kelly_fraction,
                max_cap=self._config.single_position_cap,
            )
        else:
            risk_pct = self._config.risk_pct_default
            risk_amount = equity * risk_pct
            stop_distance = abs(entry_price - stop_price)
            if stop_distance < 1e-10:
                return 0.0
            base_size = risk_amount / stop_distance

        # 2. 六维仓位调整因子 [0, 1]
        if self._config.tvf_enabled:
            tvf = SixDimPositionAdjustmentFactor().compute_adjustment(
                config=self._config,
                sortino=sortino, calmar=calmar, sharpe=sharpe,
                ofi_score=ofi_score,
                cvd_divergence=cvd_divergence,
                smart_money_flow=smart_money_flow,
                delta_exposure=delta_exposure,
                gamma_risk=gamma_risk,
                theta_decay=theta_decay,
                vega_exposure=vega_exposure,
            )
            base_size = base_size * tvf

        # 3. 单仓上限约束
        max_shares = equity * self._config.single_position_cap / entry_price
        return min(base_size, max_shares)

    def _apply_to_risk_service(
        self, config: ModeConfig, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            rs = self._component_registry.get('RiskService')
            if rs is None:
                from ali2026v3_trading.risk_service import get_risk_service
                rs = get_risk_service()
            rs.set_capital_scale(scale_str)
            propagated.append('RiskService')
        except Exception as e:
            errors.append(f'RiskService: {e}')
            logging.warning('[ModeEngine] RiskService propagation failed: %s', e)
            raise

    def _apply_to_signal_service(
        self, config: ModeConfig, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            ss = self._component_registry.get('SignalService')
            if ss is None:
                from ali2026v3_trading.signal_service import get_signal_service
                ss = get_signal_service()
            if config.plr_filter_enabled and config.min_estimated_plr > 0:
                ss.enable_plr_filter(min_estimated_plr=config.min_estimated_plr)
            else:
                ss.disable_plr_filter()
            propagated.append('SignalService')
        except Exception as e:
            errors.append(f'SignalService: {e}')
            logging.warning('[ModeEngine] SignalService propagation failed: %s', e)
            raise

    def _apply_to_state_param_manager(
        self, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            spm = self._component_registry.get('StateParamManager')
            if spm is None:
                from ali2026v3_trading.state_param_manager import get_state_param_manager
                spm = get_state_param_manager()
            spm.set_capital_scale(scale_str)
            propagated.append('StateParamManager')
        except Exception as e:
            errors.append(f'StateParamManager: {e}')
            logging.warning('[ModeEngine] StateParamManager propagation failed: %s', e)
            raise

    def _apply_to_box_spring_strategy(
        self, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            bss = self._component_registry.get('BoxSpringStrategy')
            if bss is None:
                from ali2026v3_trading.box_spring_strategy import get_box_spring_strategy
                bss = get_box_spring_strategy()
            if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                bss.set_capital_scale(scale_str)
            else:
                bss._capital_scale = scale_str
            propagated.append('BoxSpringStrategy')
        except Exception as e:
            errors.append(f'BoxSpringStrategy: {e}')
            logging.warning('[ModeEngine] BoxSpringStrategy propagation failed: %s', e)
            raise

    def _rollback(
        self, old_config: Optional[ModeConfig], old_scale: str,
        propagated: List[str],
    ) -> None:
        logging.warning('[ModeEngine] Rolling back %d components', len(propagated))
        for comp in reversed(propagated):
            try:
                if comp == 'RiskService':
                    rs = self._component_registry.get('RiskService')
                    if rs is None:
                        from ali2026v3_trading.risk_service import get_risk_service
                        rs = get_risk_service()
                    rs.set_capital_scale(old_scale)
                elif comp == 'SignalService':
                    if old_config is None:
                        continue
                    ss = self._component_registry.get('SignalService')
                    if ss is None:
                        from ali2026v3_trading.signal_service import get_signal_service
                        ss = get_signal_service()
                    if old_config.plr_filter_enabled:
                        ss.enable_plr_filter(min_estimated_plr=old_config.min_estimated_plr)
                    else:
                        ss.disable_plr_filter()
                elif comp == 'StateParamManager':
                    spm = self._component_registry.get('StateParamManager')
                    if spm is None:
                        from ali2026v3_trading.state_param_manager import get_state_param_manager
                        spm = get_state_param_manager()
                    spm.set_capital_scale(old_scale)
                elif comp == 'BoxSpringStrategy':
                    bss = self._component_registry.get('BoxSpringStrategy')
                    if bss is None:
                        from ali2026v3_trading.box_spring_strategy import get_box_spring_strategy
                        bss = get_box_spring_strategy()
                    if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                        bss.set_capital_scale(old_scale)
                    else:
                        bss._capital_scale = old_scale
            except Exception as re:
                logging.error('[ModeEngine] Rollback failed for %s: %s', comp, re)

    def evaluate_strategy_fit(
        self, train_result: Dict[str, Any],
        test_result: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        if self._config is None:
            return None
        try:
            from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
            judge = CascadeJudge.from_config()
            metrics = adapt_backtest_result(train_result, test_result, params)
            return judge.judge(metrics)
        except Exception as e:
            logging.warning('[ModeEngine] CascadeJudge evaluation failed: %s', e)
            return None

    def reload_tvf_params(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """从YAML参数池热重载TVF参数到当前ModeConfig

        Args:
            config_path: 可选的YAML配置文件路径

        Returns:
            Dict: 重载结果 {success, tvf_enabled, params_source}
        """
        try:
            from ali2026v3_trading.tvf_param_loader import get_tvf_param_loader
            loader = get_tvf_param_loader()
            yaml_params = loader.reload(config_path)
            if self._config is None:
                return {'success': False, 'error': 'No active ModeConfig'}
            new_config = loader.apply_to_mode_config(self._config)
            self._config = new_config
            logging.info(
                '[ModeEngine] TVF参数热重载成功: tvf_enabled=%s source=yaml',
                new_config.tvf_enabled,
            )
            return {
                'success': True,
                'tvf_enabled': new_config.tvf_enabled,
                'params_source': 'yaml' if yaml_params else 'default',
            }
        except Exception as e:
            logging.error('[ModeEngine] TVF参数热重载失败: %s', e)
            return {'success': False, 'error': str(e)}

    def _should_make_decision(self, current_time: Optional[float] = None) -> bool:
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            return eco.should_make_decision(current_time)
        except Exception as e:
            logging.debug('[ModeEngine] should_make_decision failed: %s', e)
            return True

    def _get_active_params(self) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            active = eco.get_active_strategy()
            # P1-6修复: 根据模式自动设置决策间隔
            try:
                mode_name = self._current_mode.value if hasattr(self, '_current_mode') and self._current_mode else ''
                interval = 5 if 'SHARPE' in mode_name else 3
                eco.set_decision_interval(active, interval)
            except Exception:
                pass
            if active == 'reverse':
                params = eco.get_s2_params()
            else:
                params = eco.get_s1_params()
            # P1-7修复: 参数变更时同步回写ecosystem
            try:
                current_mode = getattr(self, '_current_mode', None)
                if current_mode and hasattr(current_mode, 'value'):
                    mode_params = getattr(self, '_mode_configs', {}).get(current_mode.value, {})
                    if mode_params:
                        if active == 'reverse':
                            eco.update_s2_params(**mode_params)
                        else:
                            eco.update_s1_params(**mode_params)
            except Exception:
                pass
            return params
        except Exception as e:
            logging.debug('[ModeEngine] get_active_params failed: %s', e)
            return {}

    def auto_select_mode(
        self, train_result: Dict[str, Any],
        test_result: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # ✅ P0-5/P1-3: 集成策略生态API
        if not self._should_make_decision():
            logging.info('[ModeEngine] 决策间隔未到，跳过模式选择')
            return {'success': True, 'scale': self._scale_str, 'skipped': 'decision_interval'}
        active_params = self._get_active_params()
        if active_params:
            logging.debug('[ModeEngine] 活跃策略参数: %s', active_params.keys())

        report = self.evaluate_strategy_fit(train_result, test_result, params)
        if report is None:
            return {'success': False, 'error': 'CascadeJudge evaluation failed', 'scale': self._scale_str}
        if not hasattr(report, 'passed') or not report.passed:
            return {'success': False, 'error': 'Strategy did not pass cascade', 'scale': self._scale_str, 'report': report}
        metrics = getattr(report, 'metrics', None)
        if metrics is None:
            return {'success': True, 'scale': self._scale_str, 'report': report}
        sortino = getattr(metrics, 'sortino_ratio', 0.0)
        sharpe = getattr(metrics, 'sharpe_ratio', 0.0)
        plr = getattr(metrics, 'profit_loss_ratio', 0.0)
        if sharpe >= 2.0 and sortino >= 1.5:
            target_scale = 'large'
        elif plr >= 2.0:
            target_scale = 'small'
        else:
            target_scale = 'medium'
        if target_scale != self._scale_str:
            return self.switch_mode(target_scale)
        return {'success': True, 'scale': self._scale_str, 'report': report}
