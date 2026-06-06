"""
策略配置模块 — 从config_params.py拆分
职责: 策略模式常量、策略参数表(策略相关部分)、策略参数缓存管理
"""
from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Any, Dict, List, Optional

STRATEGY_MODE_CORRECT_TRENDING = "correct_trending"
STRATEGY_MODE_INCORRECT_REVERSAL = "incorrect_reversal"
STRATEGY_MODE_SPRING = "spring"
STRATEGY_MODE_RESONANCE = "resonance"
STRATEGY_MODE_BOX = "box"
STRATEGY_MODE_HFT = "hft"
STRATEGY_MODE_BOX_EXTREME = "box_extreme"
STRATEGY_MODE_BOX_SPRING = "box_spring"
STRATEGY_MODE_ARBITRAGE = "arbitrage"
STRATEGY_MODE_MARKET_MAKING = "market_making"
STRATEGY_MODE_HIGH_FREQ = "high_freq"

ALL_STRATEGY_MODES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_SPRING,
    STRATEGY_MODE_RESONANCE,
    STRATEGY_MODE_BOX,
    STRATEGY_MODE_HFT,
)

STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE = "correct_trending_defensive"
STRATEGY_MODE_OTHER = "other"
STRATEGY_MODE_CORRECT_RESONANCE = "CORRECT_RESONANCE"
STRATEGY_MODE_CORRECT_DIVERGENCE = "CORRECT_DIVERGENCE"

ALL_MARKET_STATES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_DIVERGENCE,
)

_STATE_REASON_MAP = {
    STRATEGY_MODE_CORRECT_TRENDING: STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: STRATEGY_MODE_CORRECT_DIVERGENCE,
    STRATEGY_MODE_INCORRECT_REVERSAL: 'INCORRECT_REVERSAL',
    'incorrect_reversal_defensive': 'INCORRECT_DIVERGENCE',
    STRATEGY_MODE_OTHER: 'OTHER_SCALP',
}

_strategy_param_caches: Dict[str, Dict[str, Any]] = {}
_strategy_cache_timestamps: Dict[str, float] = {}
_param_age_monitor: Dict[str, float] = {}
_invalidate_idempotent: Dict[str, float] = {}
_INVALIDATE_IDEMPOTENT_SEC = 1.0
_strategy_cache_lock = threading.Lock()


def resolve_open_reason_from_state(state: str) -> str:
    reason = _STATE_REASON_MAP.get(state, '')
    if not reason:
        logging.warning("[strategy_config] 未知状态'%s'，默认OTHER_SCALP", state)
        reason = 'OTHER_SCALP'
    return reason


def get_strategy_param_cache(strategy_id: str) -> Optional[Dict[str, Any]]:
    with _strategy_cache_lock:
        return _strategy_param_caches.get(strategy_id)


def set_strategy_param_cache(strategy_id: str, params: Dict[str, Any]) -> None:
    with _strategy_cache_lock:
        _strategy_param_caches[strategy_id] = params
        _strategy_cache_timestamps[strategy_id] = time.time()


def invalidate_strategy_cache(strategy_id: str) -> bool:
    _now = time.time()
    if strategy_id in _invalidate_idempotent and (_now - _invalidate_idempotent[strategy_id]) < _INVALIDATE_IDEMPOTENT_SEC:
        return False
    _invalidate_idempotent[strategy_id] = _now
    with _strategy_cache_lock:
        if strategy_id in _strategy_param_caches:
            del _strategy_param_caches[strategy_id]
            _strategy_cache_timestamps.pop(strategy_id, None)
            logging.info("[strategy_config] 已清理策略级参数缓存 strategy_id=%s", strategy_id)
            return True
    return False


def get_all_strategy_modes():
    return ALL_STRATEGY_MODES


def get_all_market_states():
    return ALL_MARKET_STATES


# ============================================================================
# 策略级默认参数常量 — 从config_params.py迁移
# ============================================================================

STRATEGY_DEFAULTS = {
    'signal_cooldown_sec': 60.0,
    'close_take_profit_ratio': 1.8,
    'close_stop_loss_ratio': 0.3,
    'max_risk_ratio': 0.8,
    'default_slippage_bps': 3.0,
    'max_open_positions': 3,
    'tvf_enabled': True,
    'tvf_l1_weight': 0.40,
    'tvf_l2_weight': 0.35,
    'tvf_l3_weight': 0.25,
    'STRATEGY_SL_RATIOS': {
        'correct_trending': 0.4,
        'incorrect_reversal': 0.6,
        'other': 0.5,
        'box_extreme': 0.3,
        'hft': 0.2,
    },
    'max_signals_per_window': 5,
    'state_confirm_bars': 5,
    'circuit_breaker_pause_sec': 180.0,
    'max_hold_minutes': 120,
    'lots_min': 3,
    'option_buy_lots_min': 1,
    'option_buy_lots_max': 100,
    'position_limit_max_ratio': 0.2,
    'position_timeout_sec': 3600,
    'daily_drawdown_multiplier': 2.0,
    'circuit_breaker_trigger_sigma': 3.0,
}

# R24-P1-DF-06修复: 中心化默认值常量体系、单一真相源
# R26-P2-DF-01修复: 核心参数语义注释
CENTRALIZED_DEFAULTS = {
    'max_risk_ratio': 0.8,  # 最大风险比率，风控拒绝阈值(0~1)
    'close_stop_loss_ratio': 0.3,  # 全局止损比率，持仓亏损达此比例触发平仓
    'close_take_profit_ratio': 1.8,  # R24-P1-DF-07修复: 全局止盈比率，持仓盈利达此比例触发平仓
    'signal_cooldown_sec': 60.0,  # R24-P1-DF-10修复: 信号冷却时间(秒)，同合约两次信号最小间隔
    'state_confirm_bars': 5,  # 状态确认K线数，连续N根K线确认趋势状态
    'alpha_window_days': 7,  # Alpha衰减计算窗口(天)，评估策略Alpha衰减的时间跨度
    'circuit_breaker_pause_sec': 180.0,  # 断路器暂停时间(秒)，触发后暂停交易时长
    'daily_drawdown_multiplier': 2.0,  # 日内回撤乘数，最大回撤=波动率×此乘数
    'max_hold_minutes': 120,  # 最大持仓时间(分钟)，超时强制平仓
    'max_signals_per_window': 10,  # 窗口内最大信号数，防止信号洪泛
    'lots_min': 3,  # R24-P1-DF-08修复: 最小开仓手数
    'option_buy_lots_min': 1,  # 期权最小买入手数
    'option_buy_lots_max': 100,  # 期权最大买入手数
    'tvf_enabled': True,  # P1-01修复: TVF三因子投票开关
    'tvf_l1_weight': 0.40,  # P1-01修复: TVF L1因子权重(趋势验证)
    'tvf_l2_weight': 0.35,  # P1-01修复: TVF L2因子权重(订单流验证)
    'tvf_l3_weight': 0.25,  # P1-01修复: TVF L3因子权重(Greeks验证)
    # R19-P1-02修复: 补齐params_default.json中但CENTRALIZED_DEFAULTS缺失的风控关键参数
    'position_limit_max_ratio': 0.2,  # 单合约持仓上限占总资金比例
    'circuit_breaker_trigger_sigma': 3.0,  # 断路器触发标准差倍数
    'kline_max_age_sec': 60,  # K线最大有效期(秒)，过期K线不参与计算
    'signal_max_age_sec': 180,  # 信号最大有效期(秒)，过期信号不执行
    'subscription_batch_size': 10,  # 订阅批量大小，单次订阅合约数
    'rate_limit_min_interval_sec': 1,  # API调用最小间隔(秒)，限频保护
    'enforce_trading_session_boundary': True,  # 是否强制交易时段边界检查
    'last_trading_day_close_ahead_days': 3,  # 到期前N天提前平仓
    'delivery_slippage_multiplier_max': 3.0,  # 交割滑点乘数上限
    'default_slippage_bps': 3.0,  # R27-P0-FIX: 默认滑点(基点)，与DEFAULT_PARAM_TABLE对齐
    'intraday_max_total_ticks': 5000000,  # 日内最大tick总量，超量丢弃
    'tick_shard_count': 16,  # tick分片数，并发写入分片数
    'CACHE_TTL': 60.0,  # 缓存有效期(秒)
    # R24-P1-DF-05修复: 策略级止损比率默认值集中管理
    'STRATEGY_SL_RATIOS': {
        'correct_trending': 0.4,  # 正确趋势止损比率
        'incorrect_reversal': 0.6,  # R27-P0-FIX: 错误反转止损比率，与V7.0手册§9.2及position_service对齐
        'other': 0.5,  # 其他状态止损比率
        'box_extreme': 0.3,  # 箱体极值止损比率
        'hft': 0.2,  # HFT止损比率
    },
    'position_timeout_sec': 3600,  # R24-P1-DF-12修复: 持仓超时默认值(秒)
    'max_retry_count': 3,  # R24-P1-DF-14修复: 最大重试次数默认值
    'retry_delay_sec': 5.0,  # R24-P1-DF-15修复: 重试间隔默认值(秒)
    'log_retention_days': 7,  # R24-P1-DF-17修复: 日志保留天数默认值
}

# 策略缓存TTL — R23-FR-01-FIX: 缓存TTL从00秒缩短至60秒，极端行情下减少过期数据风险
# R17-P2-CFG-02: 可配置化，建议通过环境变量CONFIG_CACHE_TTL_SEC覆盖
CACHE_TTL: float = 60.0

# ============================================================================
# 策略参数监控与通知 — 从config_params.py迁移
# ============================================================================

# [FR-P1-08~15-FIX] 通用数据新鲜度增量
_cache_refresh_timestamps: Dict[str, float] = {}
_param_change_subscribers: List = []
_pipeline_latency_monitor: Dict[str, float] = {}
_computation_timestamps: Dict[str, float] = {}
_indicator_freshness_cache: Dict[str, tuple] = {}
_risk_data_freshness: Dict[str, float] = {}


def subscribe_param_changes(callback) -> None:
    """[FR-P1-10-FIX] 参数变更事件订阅"""
    _param_change_subscribers.append(callback)


def _notify_param_change(param_key: str, old_val: Any, new_val: Any) -> None:
    """[FR-P1-10-FIX] 参数变更事件发布通知"""
    for cb in _param_change_subscribers:
        try:
            cb(param_key, old_val, new_val)
        except Exception:
            pass


def record_pipeline_latency(stage: str, latency_sec: float) -> None:
    """[FR-P1-12-FIX] 数据管道延迟监控"""
    _pipeline_latency_monitor[stage] = latency_sec


def mark_computation_timestamp(computation_id: str) -> None:
    """[FR-P1-13-FIX] 计算结果时间戳标记"""
    _computation_timestamps[computation_id] = time.time()


def check_indicator_freshness(indicator_key: str, max_age_sec: float = 60.0) -> bool:
    """[FR-P1-14-FIX] 指标计算新鲜度校验"""
    _entry = _indicator_freshness_cache.get(indicator_key)
    if _entry is None:
        return False
    _ts, _ = _entry
    return (time.time() - _ts) < max_age_sec


def record_indicator_value(indicator_key: str, value: Any) -> None:
    """[FR-P1-14-FIX] 记录指标值和时间戳"""
    _indicator_freshness_cache[indicator_key] = (time.time(), value)


def check_risk_data_freshness(risk_key: str, max_age_sec: float = 5.0) -> bool:
    """[FR-P1-15-FIX] 风控数据新鲜度校验"""
    _ts = _risk_data_freshness.get(risk_key, 0.0)
    return (time.time() - _ts) < max_age_sec


def update_risk_data_timestamp(risk_key: str) -> None:
    """[FR-P1-15-FIX] 更新风控数据时间戳"""
    _risk_data_freshness[risk_key] = time.time()


# [R23-P2-FR-10-FIX] 参数年龄监控汇总方法
def get_param_age_summary() -> Dict[str, Any]:
    """返回参数年龄监控汇总"""
    _now = time.time()
    _summary = {}
    for _sid, _at in _param_age_monitor.items():
        _summary[_sid] = {'age_sec': _now - _at, 'last_access': _at}
    return _summary


def check_multi_level_cache_consistency() -> Dict[str, bool]:
    """[FR-P1-11-FIX] 多级缓存一致性校验"""
    _now = time.time()
    result = {}
    for _sid in _strategy_param_caches:
        _strategy_ts = _strategy_cache_timestamps.get(_sid, 0.0)
        result[_sid] = (_now - _strategy_ts) < CACHE_TTL
    return result