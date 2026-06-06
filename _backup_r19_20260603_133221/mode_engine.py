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
import statistics
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

# R27-P1修复: 导入共享状态注册表
from ali2026v3_trading.resilience_utils import (
    SharedStateRegistry, AtomicConfigRef, stable_sum, stable_mean,
    safe_normalize_weights, PRICE_TOLERANCE,
)
from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
)


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


@dataclass(frozen=True, slots=True)
class ModeConfig:
    # R14-P1-DOC-P1-08修复: 各阈值添加量化来源注释
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
    win_loss_ratio_full_score_at: float = 2.0  # 来源: V7.0手册§4.2 盈亏比满分线
    profit_factor_full_score_at: float = 1.5  # 来源: V7.0手册§4.2 利润因子满分线
    sharpe_full_score_at: float = 2.0  # 来源: V7.0手册§4.3 Sharpe满分线
    calmar_full_score_at: float = 2.0  # 来源: V7.0手册§4.3 Calmar满分线
    recovery_efficiency_full_score_at: float = 2.0  # 来源: 回测统计均值+1σ
    max_consecutive_losses_full: int = 3  # 来源: V7.0手册§5.1 连亏满分线
    max_consecutive_losses_zero: int = 10  # 来源: V7.0手册§5.1 连亏零分线
    drawdown_recovery_max_hours: float = 48.0  # 来源: V7.0手册§5.2 回撤恢复时限
    extreme_max_recovery_hours: float = 72.0  # 来源: V7.0手册§5.2 极端恢复时限
    overall_pass_threshold: float = 0.65  # 来源: V7.0手册§6.1 总体通过阈值
    overall_conditional_threshold: float = 0.50  # 来源: V7.0手册§6.1 总体条件阈值
    kelly_fraction: float = 0.0  # R17-07修复: Kelly比例，内部已通过cost_ratio扣除交易成本
    min_estimated_plr: float = 1.0  # T4修复: 大资金也需要PLR过滤; 来源: V7.0手册§3.1
    plr_filter_enabled: bool = True  # T4修复: 大资金也启用PLR过滤
    consecutive_loss_limit: int = 7  # 来源: V7.0手册§5.1 连亏限制
    recovery_timeout_seconds: float = 3600.0
    time_decay_min_days: int = 5
    risk_pct_default: float = 0.02  # 来源: V7.0手册§7.1 默认风险占比
    asymmetric_risk_enabled: bool = False
    profit_multiplier: float = 1.5  # 来源: V7.0手册§7.2 盈利乘数
    loss_multiplier: float = 0.7  # 来源: V7.0手册§7.2 亏损乘数
    tvf_enabled: bool = True  # R17-PAR-02修复: 与tvf_params.yaml(UPG-03)对齐，三源统一为True
    # R27-P1-HZ-07文档化: tvf_enabled=True时,TVF因子将仓位乘以[0,1]区间值(六维sigmoid乘积),
    # 相比tvf_enabled=False(使用全量凯利仓位),实际仓位将缩减。此行为变更是R26-P0-CD-02修复
    # (从False改为True)的预期结果。当所有六维指标均达到sigmoid中心点时,TVF≈0.5^6≈1.6%,
    # 极端保守;指标优异时TVF趋近1.0。参见V7.0手册§8.1"TVF仓位微调原理"。
    tvf_l1_weight: float = 0.40  # 来源: V7.0手册§8.1 TVF-L1权重
    tvf_l2_weight: float = 0.35  # 来源: V7.0手册§8.1 TVF-L2权重
    tvf_l3_weight: float = 0.25  # 来源: V7.0手册§8.1 TVF-L3权重
    tvf_sortino_threshold: float = 1.5  # 来源: V7.0手册§8.2 Sortino三轴中心
    tvf_calmar_threshold: float = 0.8  # 来源: V7.0手册§8.2 Calmar三轴中心
    tvf_sharpe_threshold: float = 1.2  # 来源: V7.0手册§8.2 Sharpe三轴中心
    tvf_gamma_threshold: float = 0.05  # 来源: V7.0手册§8.3 Gamma阈值
    tvf_theta_threshold: float = -0.02  # 来源: V7.0手册§8.3 Theta阈值
    tvf_vega_threshold: float = 0.10  # 来源: V7.0手册§8.3 Vega阈值
    tvf_sortino_scale: float = 0.50  # 来源: V7.0手册§8.2 Sortino三轴缩放
    tvf_calmar_scale: float = 0.30  # 来源: V7.0手册§8.2 Calmar三轴缩放
    tvf_sharpe_scale: float = 0.40  # 来源: V7.0手册§8.2 Sharpe三轴缩放
    tvf_ofi_scale: float = 3.00  # 来源: V7.0手册§8.4 OFI缩放
    tvf_smf_scale: float = 2.00  # 来源: V7.0手册§8.4 SMF缩放
    tvf_gamma_scale: float = 0.02  # 来源: V7.0手册§8.3 Gamma缩放
    tvf_theta_scale: float = 0.01  # 来源: V7.0手册§8.3 Theta缩放
    tvf_vega_scale: float = 0.05  # 来源: V7.0手册§8.3 Vega缩放
    tvf_sigmoid_scale: float = 1.0  # R26-P0-CD-01修复: 三源统一 Code=1.0/YAML=1.0/手册=1.0(V7.0§8.1)
    tvf_l1_inner_sortino_weight: float = 0.40  # 来源: V7.0手册§8.2 L1内层Sortino权重
    tvf_l1_inner_calmar_weight: float = 0.30  # 来源: V7.0手册§8.2 L1内层Calmar权重
    tvf_l1_inner_sharpe_weight: float = 0.30  # 来源: V7.0手册§8.2 L1内层Sharpe权重
    tvf_l2_inner_ofi_weight: float = 0.40  # 来源: V7.0手册§8.4 L2内层OFI权重
    tvf_l2_inner_cvd_weight: float = 0.30  # 来源: V7.0手册§8.4 L2内层CVD权重
    tvf_l2_inner_smf_weight: float = 0.30  # 来源: V7.0手册§8.4 L2内层SMF权重
    tvf_l3_inner_delta_weight: float = 0.30  # 来源: V7.0手册§8.3 L3内层Delta权重
    tvf_l3_inner_gamma_weight: float = 0.25  # 来源: V7.0手册§8.3 L3内层Gamma权重
    tvf_l3_inner_theta_weight: float = 0.25  # 来源: V7.0手册§8.3 L3内层Theta权重
    tvf_l3_inner_vega_weight: float = 0.20  # 来源: V7.0手册§8.3 L3内层Vega权重


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


# P1-R11-23修复: YAML键名到ModeConfig字段名的显式映射
# 确保tvf_params.yaml中的键名与ModeConfig dataclass字段名一致，
# 避免因命名不匹配导致参数静默丢失。
_YAML_TO_MODECONFIG_MAP = {
    # YAML键 → ModeConfig字段名
    "tvf_enabled": "tvf_enabled",
    "layer_weights.l1_risk_return": "tvf_l1_weight",
    "layer_weights.l2_market_micro": "tvf_l2_weight",
    "layer_weights.l3_option_greeks": "tvf_l3_weight",
    "l1_tri_validation.sortino_tri_center": "tvf_sortino_threshold",
    "l1_tri_validation.calmar_tri_center": "tvf_calmar_threshold",
    "l1_tri_validation.sharpe_tri_center": "tvf_sharpe_threshold",
    "l1_tri_validation.sortino_tri_scale": "tvf_sortino_scale",
    "l1_tri_validation.calmar_tri_scale": "tvf_calmar_scale",
    "l1_tri_validation.sharpe_tri_scale": "tvf_sharpe_scale",
    "l2_order_flow.ofi_scale": "tvf_ofi_scale",
    "l2_order_flow.smf_scale": "tvf_smf_scale",
    "l3_greeks.gamma_threshold": "tvf_gamma_threshold",
    "l3_greeks.theta_threshold": "tvf_theta_threshold",
    "l3_greeks.vega_threshold": "tvf_vega_threshold",
    "l3_greeks.gamma_scale": "tvf_gamma_scale",
    "l3_greeks.theta_scale": "tvf_theta_scale",
    "l3_greeks.vega_scale": "tvf_vega_scale",
    "l1_tri_validation.inner_weights.sortino": "tvf_l1_inner_sortino_weight",
    "l1_tri_validation.inner_weights.calmar": "tvf_l1_inner_calmar_weight",
    "l1_tri_validation.inner_weights.sharpe": "tvf_l1_inner_sharpe_weight",
    "l2_order_flow.inner_weights.ofi": "tvf_l2_inner_ofi_weight",
    "l2_order_flow.inner_weights.cvd_divergence": "tvf_l2_inner_cvd_weight",
    "l2_order_flow.inner_weights.smart_money_flow": "tvf_l2_inner_smf_weight",
    "l3_greeks.inner_weights.delta": "tvf_l3_inner_delta_weight",
    "l3_greeks.inner_weights.gamma": "tvf_l3_inner_gamma_weight",
    "l3_greeks.inner_weights.theta": "tvf_l3_inner_theta_weight",
    "l3_greeks.inner_weights.vega": "tvf_l3_inner_vega_weight",
}


def _validate_yaml_modeconfig_mapping(yaml_params: Dict[str, Any]) -> None:
    """P1-R11-23修复: 验证YAML键名与ModeConfig字段名映射一致性

    读取tvf_params.yaml中所有键，检查每个键是否在_YAML_TO_MODECONFIG_MAP中有映射条目。
    对未映射的键记录警告，防止参数静默丢失。

    Args:
        yaml_params: 从tvf_params.yaml加载的参数字典
    """
    if not yaml_params:
        return

    # 收集YAML中所有嵌套键路径
    def _collect_keys(d: Dict, prefix: str = "") -> List[str]:
        keys = []
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                keys.extend(_collect_keys(v, full_key))
            else:
                keys.append(full_key)
        return keys

    yaml_keys = _collect_keys(yaml_params)
    mapped_yaml_keys = set(_YAML_TO_MODECONFIG_MAP.keys())

    unmapped = [k for k in yaml_keys if k not in mapped_yaml_keys and k != "tvf_enabled"]
    if unmapped:
        logging.warning(
            "[P1-R11-23] tvf_params.yaml中存在未映射的键: %s，"
            "这些键不会被ModeConfig识别，请检查_YAML_TO_MODECONFIG_MAP",
            unmapped,
        )

    # 检查映射中引用的ModeConfig字段是否真实存在
    modeconfig_fields = {f.name for f in ModeConfig.__dataclass_fields__.values()} if hasattr(ModeConfig, '__dataclass_fields__') else set()
    if modeconfig_fields:
        for yaml_key, mc_field in _YAML_TO_MODECONFIG_MAP.items():
            if mc_field not in modeconfig_fields and mc_field != "tvf_enabled":
                logging.warning(
                    "[P1-R11-23] 映射目标ModeConfig字段'%s'不存在(源YAML键: '%s')",
                    mc_field, yaml_key,
                )


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
    """ModeConfig工厂函数 — 自动合并TVF公共默认参数，消除重复定义

    R14-P1-DOC-P1-05修复: 补全参数列表说明
    Args:
        mode: 资金模式(SHARPE_MAX/CONSERVATIVE/AGGRESSIVE)
        max_positions: 最大持仓数
        single_position_cap: 单仓上限占比
        take_profit_method: 止盈方法
        stop_loss_method: 止损方法
        pyramiding: 是否加仓
        pyramiding_rule: 加仓规则
        drawdown_action: 回撤动作
        recovery_target: 恢复目标
        min_signal_strength: 最小信号强度
        time_decay_cutoff: 时间衰减截止
        profitability_mode: 盈利模式
        profitability_weights: 盈利权重元组
        win_loss_ratio_full_score_at: 盈亏比满分阈值
        profit_factor_full_score_at: 利润因子满分阈值
        recovery_efficiency_full_score_at: 恢复效率满分阈值
        max_consecutive_losses_full: 连亏满分阈值
        max_consecutive_losses_zero: 连亏零分阈值
        drawdown_recovery_max_hours: 回撤恢复最大小时
        extreme_max_recovery_hours: 极端最大恢复小时
        overall_pass_threshold: 总体通过阈值
        overall_conditional_threshold: 总体条件阈值
        kelly_fraction: Kelly比例
        min_estimated_plr: 最小估计PLR
        plr_filter_enabled: PLR过滤开关
        consecutive_loss_limit: 连亏限制
        recovery_timeout_seconds: 恢复超时秒数
        asymmetric_risk_enabled: 非对称风险开关
        tvf_enabled: TVF开关
        sharpe_full_score_at: Sharpe满分阈值
        calmar_full_score_at: Calmar满分阈值

    Returns:
        ModeConfig: 合并TVF参数后的ModeConfig实例
    """
    # P1-R11-23: tvf_params.yaml is loaded as a global config.
    # ModeConfig mappings should be validated against tvf_params at startup.
    # Consider adding automated consistency check.
    # R14-P1-DOC-P1-02修复: YAML加载后被硬编码ModeConfig覆盖(P0-20集成: TVFParamLoader参数池加载，失败回退到_DEFAULT_TVF_PARAMS)
    _tvf_kwargs = dict(_get_default_tvf_params())
    try:
        from ali2026v3_trading.tvf_param_loader import get_tvf_param_loader
        _tvf_loader = get_tvf_param_loader()
        _tvf_params = _tvf_loader.get_params()
        if _tvf_params:
            tvf_enabled = bool(_tvf_params.get('tvf_enabled', tvf_enabled))
            # P1-R11-23修复: 验证YAML键名与ModeConfig字段名映射一致性
            _validate_yaml_modeconfig_mapping(_tvf_params)
        if _tvf_params and tvf_enabled:
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
                'tvf_l1_inner_sortino_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l1_tri_validation.inner_weights.sortino')) is not None else _tvf_kwargs['tvf_l1_inner_sortino_weight']),
                'tvf_l1_inner_calmar_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l1_tri_validation.inner_weights.calmar')) is not None else _tvf_kwargs['tvf_l1_inner_calmar_weight']),
                'tvf_l1_inner_sharpe_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l1_tri_validation.inner_weights.sharpe')) is not None else _tvf_kwargs['tvf_l1_inner_sharpe_weight']),
                'tvf_l2_inner_ofi_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l2_order_flow.inner_weights.ofi')) is not None else _tvf_kwargs['tvf_l2_inner_ofi_weight']),
                'tvf_l2_inner_cvd_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l2_order_flow.inner_weights.cvd_divergence')) is not None else _tvf_kwargs['tvf_l2_inner_cvd_weight']),
                'tvf_l2_inner_smf_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l2_order_flow.inner_weights.smart_money_flow')) is not None else _tvf_kwargs['tvf_l2_inner_smf_weight']),
                'tvf_l3_inner_delta_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l3_greeks.inner_weights.delta')) is not None else _tvf_kwargs['tvf_l3_inner_delta_weight']),
                'tvf_l3_inner_gamma_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l3_greeks.inner_weights.gamma')) is not None else _tvf_kwargs['tvf_l3_inner_gamma_weight']),
                'tvf_l3_inner_theta_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l3_greeks.inner_weights.theta')) is not None else _tvf_kwargs['tvf_l3_inner_theta_weight']),
                'tvf_l3_inner_vega_weight': (_v if (_v := _get_flattened_param(_tvf_params, 'l3_greeks.inner_weights.vega')) is not None else _tvf_kwargs['tvf_l3_inner_vega_weight']),
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
    min_estimated_plr=1.0,  # T4修复: 大资金也需要PLR过滤
    plr_filter_enabled=True,  # T4修复: 大资金也启用PLR过滤
    consecutive_loss_limit=10,
    recovery_timeout_seconds=7200.0,
    asymmetric_risk_enabled=False,
    tvf_enabled=True,  # R3-P-04修复: 与ModeConfig默认值True和R17-PAR-02"三源统一True"对齐
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
    tvf_enabled=True,  # R3-P-04修复: 与ModeConfig默认值True和R17-PAR-02"三源统一True"对齐
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
    tvf_enabled=True,  # R3-P-04修复: 与ModeConfig默认值True和R17-PAR-02"三源统一True"对齐
)

# N-08标记: CAPITAL_MODE_CONFIGS为硬编码默认值，优先从yaml/tvf_params加载
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
    """回撤管理器 — 含VaR/ATR动态升级机制（R7-M-01修复）

    手册第268行要求："动态升级采用历史滚动VaR(95%)或ATR为基准"
    当VaR或ATR超过阈值时，自动收紧仓位缩减/暂停/停止条件。
    """

    def __init__(self, action: DrawdownAction, recovery_target: float = 1.0,
                 var_confidence: float = 0.95, var_upgrade_threshold: float = 0.02,
                 atr_upgrade_threshold: float = 0.015):
        self._action = action
        self._recovery_target = recovery_target
        self._current_drawdown: float = 0.0
        self._drawdown_low: float = 0.0
        self._recovery_start_equity: float = 0.0
        # R7-M-01修复: VaR/ATR动态升级标志
        self._var_upgrade_active: bool = False
        self._atr_upgrade_active: bool = False
        self._var_confidence: float = var_confidence
        self._var_upgrade_threshold: float = var_upgrade_threshold
        self._atr_upgrade_threshold: float = atr_upgrade_threshold
        self._last_var_value: float = 0.0
        self._last_atr_value: float = 0.0

    @property
    def action(self) -> DrawdownAction:
        return self._action

    @property
    def recovery_target(self) -> float:
        return self._recovery_target

    @property
    def var_upgrade_active(self) -> bool:
        """R7-M-01修复: VaR动态升级是否激活"""
        return self._var_upgrade_active

    @property
    def atr_upgrade_active(self) -> bool:
        """R7-M-01修复: ATR动态升级是否激活"""
        return self._atr_upgrade_active

    @property
    def last_var_value(self) -> float:
        """最近一次VaR计算值"""
        return self._last_var_value

    @property
    def last_atr_value(self) -> float:
        """最近一次ATR值"""
        return self._last_atr_value

    def update_var_baseline(self, returns: List[float], confidence: float = 0.95) -> float:
        """R7-M-01修复: 计算历史滚动VaR并更新升级标志

        手册第268行: "动态升级采用历史滚动VaR(95%)为基准"
        当VaR超过阈值时，激活动态升级 → 收紧should_reduce_size/halt_new/full_stop的触发阈值

        Args:
            returns: 收益率序列
            confidence: VaR置信水平 (默认0.95=95%)

        Returns:
            float: VaR值(正值=潜在损失百分比)
        """
        if len(returns) < 20:
            return 0.0
        sorted_returns = sorted(returns)
        idx = int(len(sorted_returns) * (1.0 - confidence))
        var_value = abs(sorted_returns[idx]) if idx < len(sorted_returns) else abs(sorted_returns[-1])
        self._last_var_value = var_value
        self._var_confidence = confidence
        # VaR超过升级阈值时激活动态升级
        if var_value > self._var_upgrade_threshold:
            self._var_upgrade_active = True
            logging.warning(
                "[R7-M-01] VaR动态升级激活: VaR=%.4f > threshold=%.4f (confidence=%.0f%%)",
                var_value, self._var_upgrade_threshold, confidence * 100)
        else:
            self._var_upgrade_active = False
        return var_value

    def update_atr_baseline(self, atr_value: float) -> None:
        """R7-M-01修复: 更新ATR基准并检查是否激活动态升级

        手册第268行: "或ATR为基准"

        Args:
            atr_value: 当前ATR值(相对于价格的比例，如0.015=1.5%)
        """
        self._last_atr_value = atr_value
        if atr_value > self._atr_upgrade_threshold:
            self._atr_upgrade_active = True
            logging.warning(
                "[R7-M-01] ATR动态升级激活: ATR=%.4f > threshold=%.4f",
                atr_value, self._atr_upgrade_threshold)
        else:
            self._atr_upgrade_active = False

    def update_drawdown(self, current_drawdown: float) -> None:
        self._current_drawdown = current_drawdown
        if current_drawdown < self._drawdown_low:
            self._drawdown_low = current_drawdown

    def should_reduce_size(self) -> bool:
        """R7-M-01修复: VaR/ATR升级时阈值更敏感"""
        _dd_active = self._current_drawdown < 0
        if self._var_upgrade_active or self._atr_upgrade_active:
            # VaR/ATR升级: 更早介入减仓 (任何负回撤即触发)
            if _dd_active:
                logging.info("[R7-M-01] VaR/ATR升级模式: should_reduce_size提前触发")
                return True
        return self._action == DrawdownAction.REDUCE_SIZE and _dd_active

    def should_halt_new(self) -> bool:
        """R7-M-01修复: VaR/ATR升级时暂停新开仓阈值收紧"""
        _dd_active = self._current_drawdown < 0
        if self._var_upgrade_active or self._atr_upgrade_active:
            # VaR/ATR升级: 更早暂停新开仓 (任何负回撤即触发)
            return _dd_active
        return self._action == DrawdownAction.HALT_NEW and _dd_active

    def should_full_stop(self) -> bool:
        """R7-M-01修复: VaR/ATR升级时全停阈值从-5%收紧到-2%"""
        if self._var_upgrade_active or self._atr_upgrade_active:
            # VaR/ATR升级: 全停阈值从-5%收紧到-2%
            if self._current_drawdown < -0.02:
                logging.warning("[R7-M-01] VaR/ATR升级模式: should_full_stop阈值收紧(-5%%→-2%%)")
                return True
            return False
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
        market_state: str = "",  # P-28补全修复: 传递市场状态到_sigmoid_adjust
    ) -> float:
        """计算六维仓位调整因子 [0, 1]"""
        if not config.tvf_enabled:
            return 1.0

        # === L1: 三角验证因子 ===
        sortino_factor = self._sigmoid_adjust(
            sortino, config.tvf_sortino_threshold, config.tvf_sortino_scale,
            market_state=market_state  # P-28补全修复
        )
        calmar_factor = self._sigmoid_adjust(
            calmar, config.tvf_calmar_threshold, config.tvf_calmar_scale,
            market_state=market_state  # P-28补全修复
        )
        sharpe_factor = self._sigmoid_adjust(
            sharpe, config.tvf_sharpe_threshold, config.tvf_sharpe_scale,
            market_state=market_state  # P-28补全修复
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
    def _sigmoid_adjust(value: float, threshold: float, scale: float,
                        market_state: str = "") -> float:
        """Sigmoid调整: 阈值处=0.5, 2倍阈值处≈0.88
        
        P-28修复: 市场状态自适应scale — 高波动市场scale放大(更保守)，低波动市场scale缩小(更激进)
        R21-MATH-P2-04修复: 添加输入裁剪防止exp溢出
        """
        # P-28修复: 市场状态自适应scale调整
        _adaptive_scale = scale
        if market_state == STRATEGY_MODE_CORRECT_TRENDING:  # R25-SE-P1-02-FIX
            _adaptive_scale = scale * 0.8  # 趋势市场更激进
        elif market_state == STRATEGY_MODE_INCORRECT_REVERSAL:  # R25-SE-P1-02-FIX
            _adaptive_scale = scale * 1.2  # 反转市场更保守
        elif market_state == STRATEGY_MODE_OTHER:  # R25-SE-P1-02-FIX
            _adaptive_scale = scale * 1.5  # 震荡市场最保守
        # R21-MATH-P2-04修复: 裁剪指数参数防止exp溢出
        _exp_arg = -(value - threshold) / _adaptive_scale if abs(_adaptive_scale) > 1e-10 else (-500.0 if value < threshold else 500.0)
        _exp_arg = max(-500.0, min(500.0, _exp_arg))
        return 1.0 / (1.0 + math.exp(_exp_arg))


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


def kelly_fraction(win_rate: float, win_loss_ratio: float, fraction: float = 1.0,
                   cost_ratio: float = 0.0) -> float:
    """Kelly公式（含交易成本调整）

    R17-07修复: 交易成本从赢利中扣除，公式:
      kelly = (win_rate * (win_loss_ratio - cost_ratio) - (1 - win_rate)) / (win_loss_ratio - cost_ratio)

    Args:
        win_rate: 胜率 (0~1)
        win_loss_ratio: 盈亏比
        fraction: Kelly比例系数 (0~1)
        cost_ratio: 单位交易成本占赌注比例 (默认0)

    Returns:
        float: Kelly仓位比例 [0, 1]
    """
    if win_rate <= 0 or win_rate >= 1 or win_loss_ratio <= 0:
        return 0.0
    # 扣除交易成本后的有效盈亏比
    effective_odds = win_loss_ratio - cost_ratio
    if effective_odds <= 0:
        return 0.0
    kelly = (win_rate * effective_odds - (1 - win_rate)) / effective_odds
    return max(0.0, kelly * fraction)


def kelly_position_size(
    equity: float, win_rate: float, win_loss_ratio: float,
    entry_price: float, stop_price: float,
    kelly_fraction_param: float = 0.33, max_cap: float = 0.08,
    cost_ratio: float = 0.0,
    instrument_type: str = "future", option_discount: float = 0.6,
) -> float:
    """Kelly仓位计算（含交易成本调整 + 期权买方折扣）

    R17-07修复: cost_ratio传递给kelly_fraction进行成本扣除
    R7-M-02修复: instrument_type + option_discount 期权买方仓位打折(手册第854行要求0.5~0.7)
    """
    if equity <= 0 or kelly_fraction_param <= 0 or entry_price <= 0 or stop_price <= 0:
        return 0.0
    kf = kelly_fraction(win_rate, win_loss_ratio, kelly_fraction_param, cost_ratio=cost_ratio)
    if kf <= 0:
        return 0.0
    # 期权买方应用折扣系数（手册第854行: 半凯利×折扣(0.5~0.7)）
    if instrument_type == "option_buyer" and option_discount > 0:
        kf *= option_discount
    risk_per_share = abs(entry_price - stop_price)
    if risk_per_share < 1e-10:
        return 0.0
    risk_amount = equity * kf
    shares = risk_amount / risk_per_share
    max_shares = equity * max_cap / entry_price
    return min(shares, max_shares)


# P-07修复: PredictiveStateEngine — 预测状态引擎（手册6.2节）
class PredictiveStateEngine:

    _lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> 'PredictiveStateEngine':
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        with cls._lock:
            _registry = SingletonRegistry.get_registry('predictive_state_engine')
            _inst = _registry.get('instance')
            if _inst is None:
                _inst = cls()
                _registry.set('instance', _inst)
            return _inst

    @classmethod
    def reset_instance(cls) -> None:
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        with cls._lock:
            _registry = SingletonRegistry.get_registry('predictive_state_engine')
            _registry.remove('instance')

    def __init__(self):
        self._state_counts: Dict[str, int] = {
            "correct_rise": 0, "correct_fall": 0,
            "wrong_rise": 0, "wrong_fall": 0,
        }
        self._position_multipliers: Dict[str, float] = {
            "correct_rise": 1.0, "correct_fall": 1.0,
            "wrong_rise": 0.5, "wrong_fall": 0.5,
        }
        self._transition_history: List[Dict[str, Any]] = []
        self._max_history: int = 1000

    def classify_state(self, prediction_correct: bool, price_direction: int) -> str:
        """将预测正确性+价格方向分类为四方向状态

        Args:
            prediction_correct: 预测是否正确
            price_direction: 价格变化方向 (1=上涨, -1=下跌)

        Returns:
            str: correct_rise/correct_fall/wrong_rise/wrong_fall
        """
        if prediction_correct:
            return "correct_rise" if price_direction > 0 else "correct_fall"
        else:
            return "wrong_rise" if price_direction > 0 else "wrong_fall"

    def get_position_multiplier(self, state: str) -> float:
        """获取对应方向的仓位调整乘数

        Args:
            state: 四方向状态名

        Returns:
            float: 仓位乘数 (0.5~1.0)
        """
        return self._position_multipliers.get(state, 0.75)

    def record_transition(self, from_state: str, to_state: str, pnl: float = 0.0) -> None:
        """记录状态转换事件

        Args:
            from_state: 源状态
            to_state: 目标状态
            pnl: 本次转换对应的盈亏
        """
        self._state_counts[to_state] = self._state_counts.get(to_state, 0) + 1
        logging.info("[PSE] LOG-P1-02: 状态转换 %s→%s, pnl=%.2f", from_state, to_state, pnl)
        self._transition_history.append({
            "from": from_state, "to": to_state, "pnl": pnl,
            "timestamp": time.time(),
        })
        if len(self._transition_history) > self._max_history:
            self._transition_history = self._transition_history[-self._max_history:]
        # DFG-P1-03修复: PredictiveStateEngine状态转换结果通过EventBus传播
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.publish('pse.state_transition', {
                    'type': 'pse.state_transition',
                    'from_state': from_state,
                    'to_state': to_state,
                    'pnl': pnl,
                    'position_multiplier': self._position_multipliers.get(to_state, 0.75),
                }, async_mode=True)
        except Exception as _bus_err:
            logging.warning('[R22-EP-06] state_transition事件发布失败: %s', _bus_err)

    def get_state_stats(self) -> Dict[str, Any]:
        """获取四方向状态统计"""
        total = max(sum(self._state_counts.values()), 1)
        return {
            "counts": dict(self._state_counts),
            "ratios": {k: v / total for k, v in self._state_counts.items()},
            "correct_ratio": (self._state_counts["correct_rise"] + self._state_counts["correct_fall"]) / total,
            "wrong_ratio": (self._state_counts["wrong_rise"] + self._state_counts["wrong_fall"]) / total,
        }


class ModeEngine:
    _lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> 'ModeEngine':
        import warnings
        warnings.warn(
            "ModeEngine.get_instance() is deprecated; use get_mode_engine() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        with cls._lock:
            _registry = SingletonRegistry.get_registry('mode_engine')
            _inst = _registry.get('instance')
            if _inst is None:
                _inst = cls()
                _registry.set('instance', _inst)
            return _inst

    @classmethod
    def reset_instance(cls) -> None:
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        with cls._lock:
            _registry = SingletonRegistry.get_registry('mode_engine')
            _registry.remove('instance')

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
        # P1-R9-31修复: 在__init__中自动注册所有已知组件
        self._auto_register_known_components()
        # R27-P0-DR-08修复: 降级恢复自动检测 — 记录降级状态和降级时间
        self._degraded_at: Optional[float] = None
        self._degraded_from: Optional[str] = None
        self._auto_recovery_check_interval: float = 300.0  # 每5分钟检查一次
        self._last_recovery_check: float = 0.0

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
        with self._propagation_lock:
            self._component_registry[name] = component

    def switch_mode(self, scale_str: str, reason: str = 'unknown') -> Dict[str, Any]:  # R24-P1-TR-03修复: 添加切换原因
        # SM-P1-04修复: 模式切换前取消旧模式关联的定时器，防止旧定时器在新模式下继续触发
        try:
            _scheduler_proxy = getattr(self, '_scheduler_mgr_proxy', None)
            if _scheduler_proxy is not None and hasattr(_scheduler_proxy, 'get_scheduler'):
                _sched = _scheduler_proxy.get_scheduler()
                if _sched is not None:
                    _old_scale = getattr(self, '_scale_str', None)
                    if _old_scale:
                        _jobs_to_remove = [
                            job.id for job in _sched.get_jobs()
                            if getattr(job, 'tags', set()) and f"mode_{_old_scale}" in getattr(job, 'tags', set())
                        ]
                        for _jid in _jobs_to_remove:
                            _sched.remove_job(_jid)
                        if _jobs_to_remove:
                            logging.info("[SM-P1-04] 已取消旧模式'%s'关联定时器: %s", _old_scale, _jobs_to_remove)
        except Exception as _timer_err:
            logging.warning("[SM-P1-04] 定时器清理异常(非致命): %s", _timer_err)
        # R15-P1-API-05修复: capital_scale类型校验，确保为str
        if not isinstance(scale_str, str):
            logging.error('[ModeEngine] capital_scale type error: expected str, got %s: %s', type(scale_str).__name__, scale_str)
            scale_str = str(scale_str)
        _switch_start = time.time()
        _switch_timeout_sec = 30.0  # R23-SM-05-FIX: 模式切换超时阈值
        with self._propagation_lock:
            config = CAPITAL_MODE_CONFIGS.get(scale_str)
            if config is None:
                logging.error('[ModeEngine] Unknown scale: %s', scale_str)
                return {'success': False, 'error': f'Unknown scale: {scale_str}'}

            old_config = self._config
            old_scale = self._scale_str
            propagated = []
            errors = []

            # P-23修复: 组件注册表同步检查 — 传播前验证注册组件一致性
            _registry_before = set(self._component_registry.keys())

            _CRITICAL_COMPONENTS = {'RiskService', 'SignalService'}
            self._apply_to_risk_service(config, scale_str, propagated, errors)
            self._apply_to_predictive_state_engine(config, scale_str, propagated, errors)
            self._apply_to_signal_service(config, scale_str, propagated, errors)
            self._apply_to_state_param_manager(scale_str, propagated, errors)
            self._apply_to_box_spring_strategy(scale_str, propagated, errors)
            _critical_errors = [e for e in errors if any(e.startswith(c + ':') for c in _CRITICAL_COMPONENTS)]
            if _critical_errors:
                logging.error('[ModeEngine] Critical component propagation failed, rolling back: %s', _critical_errors)
                self._rollback(old_config, old_scale, propagated)
                _registry_after = set(self._component_registry.keys())
                if _registry_before != _registry_after:
                    logging.warning('[P-23] 组件注册表不一致: before=%s after=%s',
                                    _registry_before, _registry_after)
                return {'success': False, 'error': str(_critical_errors), 'rolled_back': True}

            self._config = config
            self._scale_str = scale_str
            self._exit_engine = ExitRuleEngine(config.take_profit_method, config.stop_loss_method)
            self._drawdown_mgr = DrawdownManager(config.drawdown_action, config.recovery_target)
            self._propagated_components = propagated
            # R27-P0-DR-08修复: 记录降级状态 — 规模缩小时记录为降级
            _scale_order = {'small': 0, 'medium': 1, 'large': 2}
            if old_scale and _scale_order.get(scale_str, 1) < _scale_order.get(old_scale, 1):
                self._degraded_at = time.time()
                self._degraded_from = old_scale
                logging.warning("[R27-P0-DR-08] 降级记录: %s→%s, 将定期检查恢复条件", old_scale, scale_str)
            elif self._degraded_at is not None:
                self._degraded_at = None
                self._degraded_from = None
                logging.info("[R27-P0-DR-08] 降级恢复: 已升级到%s", scale_str)

            logging.info(
                '[ModeEngine] Mode switched: %s -> %s (mode=%s, propagated=%s, reason=%s)',  # R24-P1-TR-03修复: 记录切换原因
                old_scale, scale_str, config.mode.name, propagated, reason,
            )
            # R24-P1-TR-12修复: 模式切换时记录结构化策略切换日志
            try:
                from ali2026v3_trading.health_check_api import StructuredJsonlLogger
                _sjl = StructuredJsonlLogger()
                _sjl.log_strategy_switch(old_scale or 'unknown', scale_str, reason)
            except Exception as _sjl_err:
                logging.debug("[R22-EP-06-残留] StructuredJsonlLogger策略切换日志记录失败: %s", _sjl_err)
            _switch_elapsed = time.time() - _switch_start
            if _switch_elapsed > _switch_timeout_sec:
                logging.error("[R23-SM-05-FIX] 模式切换超时: elapsed=%.1fs > timeout=%.1fs, scale=%s->%s",
                             _switch_elapsed, _switch_timeout_sec, old_scale, scale_str)
            return {
                'success': True,
                'old_scale': old_scale,
                'new_scale': scale_str,
                'mode': config.mode.name,
                'propagated': propagated,
                'errors': errors,
                'reason': reason,  # R24-P1-TR-03修复: 返回切换原因
            }

    def filter_signal_by_mode(
        self, signal_type: str, estimated_plr: float = 0.0,
        signal_strength: float = 0.0, days_to_expiry: int = 999,
    ) -> Tuple[bool, str]:
        if self._config is None:
            return True, ''

        if self._config.plr_filter_enabled and self._config.min_estimated_plr > 0:
            if signal_type in ('BUY', 'SELL') and estimated_plr < self._config.min_estimated_plr:
                _reason = f'PLR filter: {estimated_plr:.2f} < {self._config.min_estimated_plr:.2f}'
                logging.info("[ModeEngine] LOG-P1-01: Signal filtered - type=%s reason=%s", signal_type, _reason)
                return False, _reason

        if self._config.min_signal_strength > 0 and signal_strength > 0:
            if signal_strength < self._config.min_signal_strength:
                _reason = f'Signal strength: {signal_strength:.2f} < {self._config.min_signal_strength:.2f}'
                logging.info("[ModeEngine] LOG-P1-01: Signal filtered - type=%s reason=%s", signal_type, _reason)
                return False, _reason

        # [R16-P2-3.3修复] signal_strength=0时静默跳过，添加debug日志可追踪
        if signal_strength == 0 and self._config.min_signal_strength > 0:
            logging.debug("[R16-P2-3.3] signal_strength=0, 过滤静默跳过: signal_type=%s", signal_type)

        if self._config.time_decay_cutoff > 0 and days_to_expiry < 999:
            if days_to_expiry <= self._config.time_decay_min_days:
                _reason = f'Time decay: days_to_expiry={days_to_expiry} <= {self._config.time_decay_min_days}'
                logging.info("[ModeEngine] LOG-P1-01: Signal filtered - type=%s reason=%s", signal_type, _reason)
                return False, _reason

        return True, ''

    def compute_position_size(
        self, equity: float, entry_price: float, stop_price: float,
        win_rate: float = 0.0, win_loss_ratio: float = 0.0,
        sortino: float = 0.0, calmar: float = 0.0, sharpe: float = 0.0,
        ofi_score: float = 0.0, cvd_divergence: float = 0.0,
        smart_money_flow: float = 0.0,
        delta_exposure: float = 0.0, gamma_risk: float = 0.0,
        theta_decay: float = 0.0, vega_exposure: float = 0.0,
        instrument_type: str = "future", option_discount: float = 0.6,
        market_state: str = "",  # P-28补全修复: 传递市场状态
    ) -> float:
        """计算仓位大小，集成六维仓位调整因子
        
        R7-M-02修复: instrument_type + option_discount 传递到kelly_position_size
        R13-P0-LOG-01修复: 添加详细日志记录
        """
        if self._config is None:
            logging.info("[ModeEngine.compute_position_size] config为None，返回0.0")
            return 0.0

        # 0. 前置边界检查
        if equity <= 0 or entry_price <= 0 or stop_price <= 0:
            logging.info(
                "[ModeEngine.compute_position_size] 边界检查失败: "
                "equity=%.2f, entry_price=%.2f, stop_price=%.2f",
                equity, entry_price, stop_price
            )
            return 0.0

        # 1. 基础仓位（凯利或固定风险）
        if self._config.kelly_fraction > 0 and win_rate > 0 and win_loss_ratio > 0:
            logging.info(
                "[ModeEngine.compute_position_size] 使用凯利公式计算仓位: "
                "equity=%.2f, win_rate=%.2f, win_loss_ratio=%.2f, entry_price=%.2f, stop_price=%.2f",
                equity, win_rate, win_loss_ratio, entry_price, stop_price
            )
            base_size = kelly_position_size(
                equity, win_rate, win_loss_ratio,
                entry_price, stop_price,
                kelly_fraction_param=self._config.kelly_fraction,
                max_cap=self._config.single_position_cap,
                instrument_type=instrument_type, option_discount=option_discount,
            )
            logging.info("[ModeEngine.compute_position_size] 凯利公式计算基础仓位: %.4f", base_size)
        else:
            risk_pct = self._config.risk_pct_default
            risk_amount = equity * risk_pct
            stop_distance = abs(entry_price - stop_price)
            if stop_distance < 1e-10:
                logging.info(
                    "[ModeEngine.compute_position_size] 止损距离过小: %.10f，返回0.0",
                    stop_distance
                )
                return 0.0
            base_size = risk_amount / stop_distance
            logging.info(
                "[ModeEngine.compute_position_size] 使用固定风险计算仓位: "
                "risk_pct=%.4f, risk_amount=%.2f, stop_distance=%.2f, base_size=%.4f",
                risk_pct, risk_amount, stop_distance, base_size
            )

        # 2. 六维仓位调整因子 [0, 1]
        if self._config.tvf_enabled:
            logging.info(
                "[ModeEngine.compute_position_size] TVF调整因子启用，输入参数: "
                "sortino=%.2f, calmar=%.2f, sharpe=%.2f, ofi_score=%.2f, "
                "cvd_divergence=%.2f, smart_money_flow=%.2f, delta_exposure=%.2f, "
                "gamma_risk=%.2f, theta_decay=%.2f, vega_exposure=%.2f",
                sortino, calmar, sharpe, ofi_score, cvd_divergence,
                smart_money_flow, delta_exposure, gamma_risk,
                theta_decay, vega_exposure
            )
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
                market_state=market_state,  # P-28补全修复
            )
            logging.info("[ModeEngine.compute_position_size] TVF调整因子: %.4f", tvf)
            base_size = base_size * tvf
            logging.info("[ModeEngine.compute_position_size] TVF调整后仓位: %.4f", base_size)
        else:
            logging.info("[ModeEngine.compute_position_size] TVF调整因子未启用，使用基础仓位: %.4f", base_size)
        # R24-P2-CF-06修复: tvf_enabled=False时使用全量凯利仓位（设计意图：不调整=信任基础仓位计算）
        # 当tvf_enabled=False，base_size不做任何调整，即使用全量凯利仓位

        # 3. 单仓上限约束
        max_shares = equity * self._config.single_position_cap / entry_price
        logging.info(
            "[ModeEngine.compute_position_size] 单仓上限约束: "
            "equity=%.2f, single_position_cap=%.4f, entry_price=%.2f, max_shares=%.4f",
            equity, self._config.single_position_cap, entry_price, max_shares
        )
        
        final_size = min(base_size, max_shares)
        logging.info(
            "[ModeEngine.compute_position_size] 最终仓位大小: base_size=%.4f, max_shares=%.4f, final_size=%.4f",
            base_size, max_shares, final_size
        )
        return final_size

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
            # R3-D-08修复: 将ModeConfig关键参数传递给RiskService
            try:
                if hasattr(rs, 'params') and rs.params is not None:
                    rs.params['consecutive_loss_limit'] = config.consecutive_loss_limit
                    rs.params['asymmetric_risk_enabled'] = config.asymmetric_risk_enabled
                    rs.params['recovery_timeout_seconds'] = config.recovery_timeout_seconds
            except Exception as _d08_e:
                logging.debug('[R3-D-08] ModeConfig参数传递到RiskService.params失败: %s', _d08_e)
            propagated.append('RiskService')
        except Exception as e:
            errors.append(f'RiskService: {e}')
            logging.error('[ModeEngine] RiskService propagation failed (critical): %s', e)

    # P-07修复: 应用PredictiveStateEngine到RiskService（手册6.2节）
    def _apply_to_predictive_state_engine(
        self, config: ModeConfig, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            pse = PredictiveStateEngine.get_instance()
            # 注册到组件注册表，供其他模块使用
            self._component_registry['PredictiveStateEngine'] = pse
            propagated.append('PredictiveStateEngine')
            logging.info('[P-07] PredictiveStateEngine activated for scale=%s', scale_str)
        except Exception as e:
            errors.append(f'PredictiveStateEngine: {e}')
            logging.error('[ModeEngine] PredictiveStateEngine propagation failed (non-critical, skipping): %s', e)

    # P1-R9-31修复: 自动注册所有已知组件
    def _auto_register_known_components(self) -> None:
        """在__init__中尝试自动注册所有已知组件，避免仅注册单个组件"""
        _known_components = [
            ('RiskService', 'ali2026v3_trading.risk_service', 'get_risk_service'),
            ('SignalService', 'ali2026v3_trading.signal_service', 'get_signal_service'),
            ('StateParamManager', 'ali2026v3_trading.state_param_manager', 'get_state_param_manager'),
            ('BoxSpringStrategy', 'ali2026v3_trading.box_spring_strategy', 'get_box_spring_strategy'),
        ]
        for name, module_path, factory_name in _known_components:
            if name in self._component_registry:
                continue
            try:
                import importlib
                mod = importlib.import_module(module_path)
                factory = getattr(mod, factory_name, None)
                if factory and callable(factory):
                    instance = factory()
                    self._component_registry[name] = instance
            except Exception:
                pass

    # P-23修复: ModeEngine.switch_mode组件注册表同步增强（手册29节）
    def _sync_component_registry(self, component_name: str, component_instance: Any) -> None:
        """确保组件注册表与import获取的实例同步"""
        try:
            self._component_registry[component_name] = component_instance
            logging.debug('[P-23] Component registry synced: %s', component_name)
        except Exception as e:
            logging.warning('[P-23] Component registry sync failed for %s: %s', component_name, e)

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
            logging.error('[ModeEngine] SignalService propagation failed (critical): %s (mode=%s)', e, scale_str)

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
            logging.error('[ModeEngine] StateParamManager propagation failed (non-critical, skipping): %s', e)  # R24-P0-CF-02修复

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
            logging.error('[ModeEngine] BoxSpringStrategy propagation failed (non-critical, skipping): %s', e)  # R24-P0-CF-02修复

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
                elif comp == 'OrderService':
                    logging.info('[ModeEngine] R25-TO-P1-01-FIX: OrderService回滚(无需特殊操作)')
                elif comp == 'PositionService':
                    logging.info('[ModeEngine] R25-TO-P1-01-FIX: PositionService回滚(无需特殊操作)')
                elif comp == 'PredictiveStateEngine':
                    logging.info('[ModeEngine] R26-P1-TO-05: PredictiveStateEngine回滚(无需特殊操作)')
                else:
                    logging.warning('[ModeEngine] R26-P1-TO-05: _rollback: 未知组件 %s，无法回滚', comp)
            except Exception as re:
                logging.error('[ModeEngine] Rollback failed for %s: %s', comp, re)

    # R27-P0-DR-08修复: 降级恢复自动检测
    def check_auto_recovery(self) -> Optional[str]:
        """定期检查降级后是否满足恢复条件，满足则自动切回原规模
        Returns:
            恢复到的规模字符串，或None（未恢复）
        """
        if self._degraded_at is None or self._degraded_from is None:
            return None
        now = time.time()
        if now - self._last_recovery_check < self._auto_recovery_check_interval:
            return None
        self._last_recovery_check = now
        try:
            # 检查风控断路器是否已解除
            rs = self._component_registry.get('RiskService')
            if rs is None:
                from ali2026v3_trading.risk_service import get_risk_service
                rs = get_risk_service()
            safety = getattr(rs, '_safety_meta_layer', None)
            if safety is not None:
                paused_until = getattr(safety, '_trading_paused_until', 0.0)
                if paused_until > now:
                    logging.info("[R27-P0-DR-08] 断路器仍在暂停中(剩余%.0fs)，暂不恢复",
                                paused_until - now)
                    return None
                hard_stop = getattr(safety, '_daily_hard_stop_triggered', False)
                if hard_stop:
                    logging.info("[R27-P0-DR-08] 日回撤硬停止仍生效，暂不恢复")
                    return None
            # 恢复条件满足: 断路器解除 + 无硬停止
            target_scale = self._degraded_from
            logging.info("[R27-P0-DR-08] 降级恢复条件满足: %s→%s", self._scale_str, target_scale)
            result = self.switch_mode(target_scale, reason='auto_recovery_from_degradation')
            if result.get('success'):
                return target_scale
            else:
                logging.warning("[R27-P0-DR-08] 自动恢复切换失败: %s", result.get('error'))
        except Exception as e:
            logging.warning("[R27-P0-DR-08] 自动恢复检查异常: %s", e)
        return None

    def evaluate_strategy_fit(
        self, train_result: Dict[str, Any],
        test_result: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        capital_scale: str = "medium",
    ) -> Optional[Any]:
        if self._config is None:
            return None
        try:
            from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
            judge = CascadeJudge.from_config(capital_scale=capital_scale, params=params)
            metrics = adapt_backtest_result(train_result, test_result, params, strategy_type=params.get('strategy_type', '') if params else '')
            return judge.judge(metrics)
        except Exception as e:
            logging.warning('[ModeEngine] CascadeJudge evaluation failed: %s', e)
            return None

    def reload_tvf_params(self, config_path: Optional[str] = None, caller_id: str = "unknown") -> Dict[str, Any]:
        """从YAML参数池热重载TVF参数到当前ModeConfig

        R10-P0-04修复: 全部在_propagation_lock内重载+传播，防止并发时
        switch_mode()和reload_tvf_params()交叉修改_config导致不一致

        P1-R11-20修复: 添加调用方鉴权
        """
        _TRUSTED_RELOAD_CALLERS = frozenset({
            'mode_engine', 'config_service', 'strategy_ecosystem', 'risk_service', 'main',
        })
        if caller_id not in _TRUSTED_RELOAD_CALLERS:
            logging.warning("[ModeEngine] reload_tvf_params被非授权调用方拒绝: caller_id=%s", caller_id)
            return {'success': False, 'error': f'Unauthorized caller: {caller_id}'}
        try:
            from ali2026v3_trading.tvf_param_loader import get_tvf_param_loader
            loader = get_tvf_param_loader()
            yaml_params = loader.reload(config_path)
            # R10-P0-04修复: _config修改和组件传播在锁内，与switch_mode互斥
            with self._propagation_lock:
                if self._config is None:
                    return {'success': False, 'error': 'No active ModeConfig'}
                new_config = loader.apply_to_mode_config(self._config)
                # R14-P1-BIZ-04修复: TVF参数热重载范围校验
                _TVF_BOUNDS = {
                    'tvf_enabled': (None, None),
                    'tvf_sigmoid_scale': (0.1, 100.0),
                    'tvf_base_multiplier': (0.01, 10.0),
                    'tvf_risk_weight': (0.0, 1.0),
                    'tvf_sharpe_weight': (0.0, 1.0),
                    'tvf_sortino_weight': (0.0, 1.0),
                }
                for attr, (lo, hi) in _TVF_BOUNDS.items():
                    val = getattr(new_config, attr, None)
                    if val is not None and lo is not None and hi is not None:
                        clamped = max(lo, min(hi, val))
                        if clamped != val:
                            logging.warning('[ModeEngine] R14-P1-BIZ-04: TVF参数越界钳位 %s=%.4f→%.4f [%.2f,%.2f]',
                                            attr, val, clamped, lo, hi)
                            setattr(new_config, attr, clamped)
                self._config = new_config
                for comp_name in self._propagated_components:
                    comp = self._component_registry.get(comp_name)
                    if comp is not None:
                        try:
                            if hasattr(comp, 'update_tvf_config'):
                                comp.update_tvf_config(new_config)
                            elif hasattr(comp, 'set_params'):
                                comp.set_params({'tvf_enabled': new_config.tvf_enabled})
                        except Exception as _e:
                            logging.warning('[P-24] 更新组件%s参数失败: %s', comp_name, _e)
                logging.info(
                    '[ModeEngine] TVF参数热重载成功: tvf_enabled=%s source=yaml propagated=%s',
                    new_config.tvf_enabled, self._propagated_components,
                )
            return {
                'success': True,
                'tvf_enabled': new_config.tvf_enabled,
                'params_source': 'yaml' if yaml_params else 'default',
            }
        except Exception as e:
            logging.error('[ModeEngine] TVF参数热重载失败: %s', e)
            return {'success': False, 'error': str(e)}

    # R14-P0-DOC-01修复: 移除废弃的_should_make_decision方法
    # should_make_decision已废弃13轮(始终返回True)，_should_make_decision是唯一调用方
    # enter_mode()中的if not self._should_make_decision()分支永远不会触发，已删除

    # R14-P0-DOC-02修复: _get_active_params拆分为纯只读查询+独立回写方法
    def _get_active_params(self) -> Dict[str, Any]:
        """纯只读: 获取当前活跃策略参数，无副作用"""
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            active = eco.get_active_strategy()
            if active == 'reverse':
                return eco.get_s2_params()
            else:
                return eco.get_s1_params()
        except (ImportError, AttributeError) as e:
            logging.debug('[ModeEngine] get_active_params failed: %s', e)
            return {}

    def _sync_params_to_ecosystem(self) -> None:
        """独立回写: 将当前模式参数同步到策略生态系统(从_get_active_params拆出的副作用)"""
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            active = eco.get_active_strategy()
            current_mode = getattr(self, '_current_mode', None)
            if current_mode and hasattr(current_mode, 'value'):
                mode_params = getattr(self, '_mode_configs', {}).get(current_mode.value, {})
                if mode_params:
                    if active == 'reverse':
                        eco.update_s2_params(**mode_params)
                    else:
                        eco.update_s1_params(**mode_params)
        except (ImportError, AttributeError, KeyError) as e:
            logging.warning("[ModeEngine] sync_params_to_ecosystem failed: %s", e)  # R24-P1-DF-03修复: debug→warning

    def auto_select_mode(
        self, train_result: Dict[str, Any],
        test_result: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        capital_scale: str = "medium",
    ) -> Dict[str, Any]:
        # ✅ P0-5/P1-3: 集成策略生态API  # R17-P2-DOC-03: P0修复标记保留用于追溯
        # R14-P0-DOC-01修复: 移除_should_make_decision()调用(废弃方法始终True，分支永不触发)
        active_params = _get_active_params_cached(self)
        if active_params:
            logging.debug('[ModeEngine] 活跃策略参数: %s', active_params.keys())
            # R14-P0-DOC-02修复: 回写副作用从_get_active_params拆出到独立方法
            self._sync_params_to_ecosystem()

        report = self.evaluate_strategy_fit(train_result, test_result, params, capital_scale=capital_scale)
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
            return self.switch_mode(target_scale, reason='auto_select')  # R24-P1-TR-03修复: 传递切换原因
        return {'success': True, 'scale': self._scale_str, 'report': report}


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-06修复: _get_active_params添加缓存，避免每次遍历TVF参数
_active_params_cache = None
_active_params_cache_ts = 0.0
_ACTIVE_PARAMS_CACHE_TTL = 1.0  # 1秒缓存，状态切换频率远低于tick频率

def _get_active_params_cached(mode_engine_instance):
    """缓存_get_active_params结果，TTL内避免重复遍历TVF参数"""
    global _active_params_cache, _active_params_cache_ts
    _now = time.time()
    if _active_params_cache is not None and (_now - _active_params_cache_ts) < _ACTIVE_PARAMS_CACHE_TTL:
        return _active_params_cache
    _active_params_cache = mode_engine_instance._get_active_params()
    _active_params_cache_ts = _now
    return _active_params_cache


# R15-P2-PERF-15修复: 大字典多层嵌套查询添加_flatten_cache
_flatten_cache: Dict[str, Any] = {}
_flatten_cache_ts: float = 0.0

def _get_flattened_param(d: Dict[str, Any], key_path: str, separator: str = '.') -> Any:
    """扁平化缓存查询: 支持'a.b.c'路径查询嵌套字典，结果缓存"""
    if key_path in _flatten_cache:
        return _flatten_cache[key_path]
    keys = key_path.split(separator)
    val = d
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k)
        else:
            return None
        if val is None:
            return None
    _flatten_cache[key_path] = val
    return val


# R15-P2-PERF-16修复: 条件判断重复调用同一函数，缓存结果到局部变量
# 示例: 将 if f() > 0 and f() < 10 改为 _f_val = f(); if _f_val > 0 and _f_val < 10
# 此处添加装饰器缓存函数结果
import functools

def _cache_result_in_call(func):
    """单次调用内缓存: 同一调用帧内多次调用同一函数时返回缓存值
    注意: 仅适用于无参纯函数或参数不变的场景
    """
    _cached_val = None
    _cached = False

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal _cached_val, _cached
        if _cached:
            return _cached_val
        _cached_val = func(*args, **kwargs)
        _cached = True
        return _cached_val

    return wrapper

# R15-P2-PERF-16: 已检查mode_engine.py中无同一函数在条件判断中被重复调用的模式，无需应用缓存优化


# AP-03: 模块级单例获取函数，替代ModeEngine.get_instance()
def get_mode_engine() -> 'ModeEngine':
    """获取ModeEngine全局单例 — 统一单路径入口"""
    from ali2026v3_trading.singleton_registry import SingletonRegistry
    with ModeEngine._lock:
        _registry = SingletonRegistry.get_registry('mode_engine')
        _inst = _registry.get('instance')
        if _inst is None:
            _inst = ModeEngine()
            _registry.set('instance', _inst)
            _registry.register_singleton("mode_engine.instance", _inst)
        return _inst
