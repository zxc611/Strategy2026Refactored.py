# MODULE_ID: M1-071
"""
mode_config - Enum + ModeConfig dataclass + configs + YAML/factory

Split from mode_engine.py - P2 split
"""
from __future__ import annotations

import logging
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
        from ali2026v3_trading.config.tvf_param_loader import TVF_DEFAULT_PARAMS
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
        logging.debug(
            "[P1-R11-23] tvf_params.yaml中存在未映射的键: %s，"
            "这些键不会被ModeConfig识别，请检查_YAML_TO_MODECONFIG_MAP",
            unmapped,
        )

    # 检查映射中引用的ModeConfig字段是否真实存在
    modeconfig_fields = {f.name for f in ModeConfig.__dataclass_fields__.values()} if hasattr(ModeConfig, '__dataclass_fields__') else set()
    if modeconfig_fields:
        for yaml_key, mc_field in _YAML_TO_MODECONFIG_MAP.items():
            if mc_field not in modeconfig_fields and mc_field != "tvf_enabled":
                logging.debug(
                    "[P1-R11-23] 映射目标ModeConfig字段'%s'不存在(源YAML键: '%s')",
                    mc_field, yaml_key,
                )



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
        from ali2026v3_trading.config.tvf_param_loader import get_tvf_param_loader
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
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _tvf_err:
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

