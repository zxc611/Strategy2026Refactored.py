# [M1-82] 参数表管理
"""
参数表管理模块- 从config_service.py 拆分
职责：默认参数表、参数缓存、参数加载校验/环境覆盖
"""
from __future__ import annotations

import errno

# R17-P2-CFG-01: 配置文件版本锁定
CONFIG_VERSION = "7.1.0"

# R25-SE-P1-02-FIX: 策略状态模式集中常量定义 — 权威源在strategy_config.py，此处re-export
from strategy.strategy_config_layer import (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_SPRING,
    STRATEGY_MODE_RESONANCE,
    STRATEGY_MODE_BOX,
    STRATEGY_MODE_HFT,
    STRATEGY_MODE_BOX_EXTREME,
    STRATEGY_MODE_BOX_SPRING,
    STRATEGY_MODE_ARBITRAGE,
    STRATEGY_MODE_MARKET_MAKING,
    STRATEGY_MODE_HIGH_FREQ,
    ALL_STRATEGY_MODES,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_DIVERGENCE,
    ALL_MARKET_STATES,
    _STATE_REASON_MAP,
    STRATEGY_DEFAULTS as _STRATEGY_DEFAULTS,
    CENTRALIZED_DEFAULTS as _SC_CENTRALIZED_DEFAULTS,
)
# 时间单位常量
TIME_UNIT_MS = "ms"
TIME_UNIT_SEC = "sec"
TIME_UNIT_MIN = "min"

import copy
import hashlib
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from datetime import datetime

from infra.shared_utils import CHINA_TZ

_PARAM_POOL_VERSION = '2.0.0'
_PARAM_POOL_VERSION_HISTORY = [
    {'version': '1.0.0', 'date': '2026-05-10', 'changes': '初始版本'},
    {'version': '2.0.0', 'date': '2026-05-30', 'changes': 'R16审计修复: max_risk_ratio/tvf_enabled/option_buy_lots/state_confirm_bars对齐'},
]

def get_param_pool_version() -> str:
    return _PARAM_POOL_VERSION

def get_param_pool_version_history() -> list:
    return copy.deepcopy(_PARAM_POOL_VERSION_HISTORY)


def get_environment_selector():
    """R17-P1-CFG-P1-02/07: 环境选择器"""
    env = os.environ.get('TRADING_ENV', os.environ.get('INFINITRADER_ENV', 'production'))
    return env

# EC-P2-03修复: Python最小版本检查
from infra.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load

if sys.version_info < (3, 8):
    raise RuntimeError(f"demo要求Python>=3.8，当前版本{sys.version}")
from typing import Any, Callable, Dict, List, Optional

# EC-P2-10: 关键依赖库版本检查
try:
    import numpy as np
    if tuple(int(x) for x in np.__version__.split('.')[:2]) < (1, 21):
        logging.warning("numpy>=1.21 recommended, current: %s", np.__version__)
except ImportError:
    pass
try:
    import pandas as _pd
    if tuple(int(x) for x in _pd.__version__.split('.')[:2]) < (1, 5):
        logging.warning("pandas>=1.5 recommended, current: %s", _pd.__version__)
except ImportError:
    pass

from config.config_exchange import ExchangeConfig
from infra.shared_utils import safe_int, safe_float
from config.config_exchange import (
    month_mapping, delivery_month_rules, tick_size_by_product,
    exchange_trading_sessions, rollover_days,
)


# P0-R11-18修复: 策略级参数缓存隔离
_param_table_cache: Optional[Dict[str, Any]] = None  # 默认策略缓存(向后兼容)
_param_table_cache_timestamp: float = 0.0
_param_table_lock = threading.Lock()
_strategy_param_caches: Dict[str, Dict[str, Any]] = {}  # 策略级缓存 strategy_id -> params
_strategy_cache_timestamps: Dict[str, float] = {}
# [R23-P2-FR-10-FIX] 参数年龄监控汇总数据
_param_age_monitor: Dict[str, float] = {}  # strategy_id -> last_access_time
# [ID-P1-06-FIX] 幂等标记: 记录最近invalidate时间，重复刷新不重复执行
_invalidate_idempotent: Dict[str, float] = {}
_INVALIDATE_IDEMPOTENT_SEC = 1.0


def invalidate_strategy_cache(strategy_id: str) -> bool:
    """P2-R11-02修复: 清理指定策略的参数缓存 — 委托到strategy_config"""
    from strategy.strategy_config_layer import invalidate_strategy_cache as _sc_fn
    return _sc_fn(strategy_id)
CACHE_TTL = 60.0  # R23-FR-01-FIX: 缓存TTL从00秒缩短至60秒，极端行情下减少过期数据风险  # R17-P2-CFG-02: 可配置化，建议通过环境变量CONFIG_CACHE_TTL_SEC覆盖
_OPTION_PARAM_RUNTIME_PATCH: Dict[str, Any] = {}
# R15-P1-PERF-08修复: 缓存访问计数器，超过阈值才遍历检查TTL
_cache_access_count = 0
_cache_access_threshold = 1000

# [FR-P1-08~15-FIX] 通用数据新鲜度增量
_cache_refresh_timestamps: Dict[str, float] = {}
_param_change_subscribers: List = []
_pipeline_latency_monitor: Dict[str, float] = {}
_computation_timestamps: Dict[str, float] = {}
_indicator_freshness_cache: Dict[str, tuple] = {}
_risk_data_freshness: Dict[str, float] = {}


def get_cache_ages() -> Dict[str, float]:
    """[FR-P1-08-FIX] 缓存数据年龄暴露为health指标"""
    _now = time.time()
    ages = {'param_table': _now - _param_table_cache_timestamp if _param_table_cache_timestamp > 0 else -1.0}
    for _sid, _ts in _strategy_cache_timestamps.items():
        ages[f'strategy_{_sid}'] = _now - _ts
    for _key, _ts in _cache_refresh_timestamps.items():
        ages[f'refresh_{_key}'] = _now - _ts
    for _sid, _at in _param_age_monitor.items():
        ages[f'access_{_sid}'] = _now - _at
    return ages


def get_param_age_summary() -> Dict[str, Any]:
    """返回参数年龄监控汇总 — 委托到strategy_config"""
    from strategy.strategy_config_layer import get_param_age_summary as _sc_fn
    return _sc_fn()


def subscribe_param_changes(callback) -> None:
    """[FR-P1-10-FIX] 参数变更事件订阅 — 委托到strategy_config"""
    from strategy.strategy_config_layer import subscribe_param_changes as _sc_fn
    _sc_fn(callback)


def _notify_param_change(param_key: str, old_val: Any, new_val: Any) -> None:
    """[FR-P1-10-FIX] 参数变更事件发布通知 — 委托到strategy_config"""
    from strategy.strategy_config_layer import _notify_param_change as _sc_fn
    _sc_fn(param_key, old_val, new_val)


def check_multi_level_cache_consistency() -> Dict[str, bool]:
    """[FR-P1-11-FIX] 多级缓存一致性校验 — 委托到strategy_config"""
    from strategy.strategy_config_layer import check_multi_level_cache_consistency as _sc_check
    return _sc_check()


def record_pipeline_latency(stage: str, latency_sec: float) -> None:
    """[FR-P1-12-FIX] 数据管道延迟监控 — 委托到strategy_config"""
    from strategy.strategy_config_layer import record_pipeline_latency as _sc_fn
    _sc_fn(stage, latency_sec)


def mark_computation_timestamp(computation_id: str) -> None:
    """[FR-P1-13-FIX] 计算结果时间戳标记 — 委托到strategy_config"""
    from strategy.strategy_config_layer import mark_computation_timestamp as _sc_fn
    _sc_fn(computation_id)


def check_indicator_freshness(indicator_key: str, max_age_sec: float = 60.0) -> bool:
    """[FR-P1-14-FIX] 指标计算新鲜度校验 — 委托到strategy_config"""
    from strategy.strategy_config_layer import check_indicator_freshness as _sc_fn
    return _sc_fn(indicator_key, max_age_sec)


def record_indicator_value(indicator_key: str, value: Any) -> None:
    """[FR-P1-14-FIX] 记录指标值和时间戳 — 委托到strategy_config"""
    from strategy.strategy_config_layer import record_indicator_value as _sc_fn
    _sc_fn(indicator_key, value)


def check_risk_data_freshness(risk_key: str, max_age_sec: float = 5.0) -> bool:
    """[FR-P1-15-FIX] 风控数据新鲜度校验 — 委托到strategy_config"""
    from strategy.strategy_config_layer import check_risk_data_freshness as _sc_fn
    return _sc_fn(risk_key, max_age_sec)


def update_risk_data_timestamp(risk_key: str) -> None:
    """[FR-P1-15-FIX] 更新风控数据时间戳 — 委托到strategy_config"""
    from strategy.strategy_config_layer import update_risk_data_timestamp as _sc_fn
    _sc_fn(risk_key)

_DEFAULT_PARAM_TABLE_LOCK = threading.Lock()

# R24-P1-DF-06修复: 中心化默认值常量体系 — 权威源在strategy_config.py，此处引用
CENTRALIZED_DEFAULTS = _SC_CENTRALIZED_DEFAULTS

from infra.security_service import _SecureCredential, reveal_credential

DEFAULT_PARAM_TABLE = {  # R21-MEM-P2-05修复: 模块级大字典
    # === 基础设施参数(非策略) ===
    "max_kline": 200,
    "kline_style": "M1",
    "subscribe_options": True,
    "debug_output": True,
    "diagnostic_output": True,
    "api_key": _SecureCredential(os.environ.get("ALI2026_API_KEY", "")),
    "infini_api_key": _SecureCredential(os.environ.get("ALI2026_INFINI_API_KEY", "")),
    "access_key": _SecureCredential(os.environ.get("ALI2026_ACCESS_KEY", "")),
    "access_secret": _SecureCredential(os.environ.get("ALI2026_ACCESS_SECRET", "")),
    "_sensitive_fields": ["api_key", "infini_api_key", "access_key", "access_secret"],
    "run_profile": "full", "enable_scheduler": True,
    "use_tick_kline_generator": True, "backtest_tick_mode": False,
    "exchange": "CFFEX", "future_product": "IF",
    "option_product": "IO", "auto_load_history": True,
    "load_history_options": True, "load_all_products": True,
    "exchanges": "CFFEX,SHFE,DCE,CZCE,INE,GFEX", "night_session_exchanges": "SHFE,DCE,CZCE,INE",
    "auction_session_minutes_before_open": 5, "auction_session_minutes_before_close": 5,
    "holiday_calendar_source": "chinese_calendar", "enforce_trading_session_boundary": True,
    "last_trading_day_close_ahead_days": 3, "last_trading_day_rollover_ahead_days": 5,
    "rollover_days": rollover_days,
    "rollover_slippage_bps": 5.0,
    "tick_size_by_product": tick_size_by_product,
    "delivery_slippage_multiplier_days": 10, "delivery_slippage_multiplier_max": 3.0,
    "close_today_commission_multiplier": 1.0, "close_yesterday_commission_multiplier": 1.0,
    "commission_rate_version": "2026-01", "commission_rate_last_verify_date": "",
    "option_pricing_mode": "auto", "price_limit_expansion_after_consecutive_limit": 1.5,
    "min_margin_ratio_override": {}, "delivery_month_position_limit_ratio": 0.5,
    "delivery_month_max_position": 0,
    "delivery_month_rules": delivery_month_rules,
    "cross_expiry_margin_discount_ratio": 0.5, "large_position_report_threshold": 0,
    "forced_reduction_after_consecutive_limit": 3, "exchange_temp_adjustments": {},
    "market_maker_max_spread_bps": 50, "market_maker_min_quote_lots": 1,
    "portfolio_margin_discount_ratio": 0.8,
    "future_products": "", "option_products": "",
    "future_instruments": [], "option_instruments": {},
    "instrument_cache_source": "", "include_future_products_for_options": True,
    "subscription_batch_size": 10,
    "subscription_interval": 1, "subscription_fetch_on_subscribe": True,
    "subscription_fetch_count": 5, "subscription_fetch_for_options": False,
    "rate_limit_min_interval_sec": 1, "rate_limit_per_instrument": 2,
    "rate_limit_global_per_min": 60, "rate_limit_window_sec": 120,
    "subscription_backoff_factor": 1.0, "subscribe_only_current_next_options": True,
    "subscribe_only_current_next_futures": True, "enable_doc_examples": False,
    "pause_unsubscribe_all": True, "pause_force_stop_scheduler": True,
    "pause_on_stop": False, "history_minutes": 1440,
    "history_load_batch_size": 200, "history_load_max_batch_size": 50,
    "history_load_batch_delay_sec": 0.2, "history_load_request_delay_sec": 0.1,
    "log_file_path": "strategy_startup.log", "test_mode": False,
    "auto_start_after_init": True, "subscribe_only_specified_month_options": True,
    "subscribe_only_specified_month_futures": True, "load_month_params_in_init": False,
    "specified_month": "", "next_specified_month_1": "",
    "next_specified_month_2": "", "next_specified_month_3": "",
    "next_specified_month_4": "",
    "option_width_month_count": 2, "option_width_min_threshold": 4.0,
    "project_root": str(Path(__file__).parent.parent), "workspace": str(Path(__file__).parent.parent),
    "month_mapping": month_mapping, "option_buy_lots_min": 1,
    "option_buy_lots_max": 100, "option_contract_multiplier": 10000,
    "position_limit_valid_hours_max": 720, "position_limit_default_valid_hours": 24,
    "position_limit_max_ratio": 0.2, "position_limit_min_amount": 1000,
    "option_order_price_type": "2", "option_order_time_condition": "3",
    "option_order_volume_condition": "1", "option_order_contingent_condition": "1",
    "option_order_force_close_reason": "0", "option_order_hedge_flag": "1",
    "option_order_min_volume": 1, "option_order_business_unit": "1",
    "option_order_is_auto_suspend": 0, "option_order_user_force_close": 0,
    "option_order_is_swap": 0,
    "close_overnight_check_time": "14:58", "close_daycut_time": "15:58",
    "close_max_hold_days": 3, "close_overnight_loss_threshold": -0.5,
    "close_overnight_profit_threshold": 4.0, "close_max_chase_attempts": 5,
    "close_chase_interval_seconds": 2, "close_chase_task_timeout_seconds": 30,
    "close_delayed_timeout_seconds": 30, "close_delayed_max_retries": 3,
    "close_order_price_type": "2", "output_mode": "debug",
    "force_debug_on_start": True, "enable_output_mode_ui": True,
    "daily_summary_hour": 15, "daily_summary_minute": 1,
    "trade_quiet": True, "print_start_snapshots": False,
    "trade_debug_allowlist": "", "debug_disable_categories": "",
    "debug_throttle_seconds": 0.0, "debug_throttle_map": {},
    "use_param_overrides_in_debug": False, "param_override_table": "",
    "param_edit_limit_per_month": 1, "backtest_params": {},
    "ignore_otm_filter": False, "allow_minimal_signal": False,
    "min_option_width": 1, "async_history_load": True,
    "manual_trade_limit_per_half": 1, "morning_afternoon_split_hour": 12,
    "account_id": "",
    "kline_max_age_sec": 60, "signal_max_age_sec": 180,
    "history_load_max_workers": 4,
    "option_sync_tolerance": 0.5, "option_sync_allow_flat": True,
    "index_option_prefixes": "", "option_group_exchanges": "",
    "czce_year_future_window": 10, "czce_year_past_window": 10,
    "exchange_mapping": dict(ExchangeConfig().product_exchanges),
    "intraday_memory_mode": "debug", "intraday_max_ticks_per_symbol": 1000,

    "intraday_max_total_ticks": 5000000, "tick_shard_count": 16,
    "tick_writer_count": 6, "tick_shard_queue_capacity": 625000,
    "kline_queue_capacity": 100000, "maintenance_queue_capacity": 50000,
    "spill_enabled": False, "spill_base_dir": "",
    "hft_hard_time_stop_ms": 60000, "spring_hard_time_stop_sec": 600,
    "resonance_hard_time_stop_min": 5, "box_hard_time_stop_min": 30,
    "min_profit_threshold": 0.002, "max_net_delta_pct": 0.30,
    "max_net_gamma_pct": 0.08, "max_net_vega_bps": 0.02,
    "max_theta_ratio": 0.5, "debug_mode": False,
    "stress_test_mode": False, "stress_test_anomaly_threshold": 1.5,
    "box_gain_ratio": 0.5, "plr_normalization_base": 3.0,
    "box_breakout_tolerance": 0.005, "spring_price_pos_min": 0.3,
    "spring_price_pos_max": 0.7, "strike_distance_threshold": 0.02,
    "direction_buy_call_threshold": 0.45, "direction_buy_put_threshold": 0.55,
    "default_order_flow_consistency": 0.5, "kline_missing_timeout_multiplier": 2.0,
    "phase_scan_gate_threshold": 25, "phase_scan_score_weights": [0.4, 0.3, 0.3],
    "capital_route_s1_hft": 0.25, "capital_route_s2_resonance": 0.15,
    "capital_route_s3_box": 0.12, "capital_route_s4_spring": 0.12,
    "capital_route_s5_arbitrage": 0.08, "capital_route_s6_market_making": 0.08,
    "capital_route_s7_divergence": 0.20,
    "var_confidence_level": 0.95, "var_upgrade_threshold": 0.02,
    "atr_upgrade_threshold": 0.015, "option_buyer_discount": 0.6,
    "sigmoid_alpha": 1.0, "sigmoid_beta": 0.0,
    "bar_synthesis_mode": "standard", "night_session_end_hour": 15,
    "night_session_eod_hour": 2, "night_session_eod_minute": 30,
    "exchange_trading_sessions": exchange_trading_sessions,
    "key_rotation_interval_days": 90, "key_last_rotation_time": None,
    "key_lifecycle_enabled": True, "key_max_age_hours": 2160,
    "security_auto_block_enabled": True, "security_block_on_suspicious_activity": True,
    "use_decimal_for_settlement": False, "decimal_precision": 8,
    "use_compensated_subtraction_for_atm": False, "check_runtime_dependencies": True,
    "commission_auto_update_enabled": False, "commission_update_source": "exchange_api",
    "commission_update_interval_days": 30, "dynamic_halt_on_price_limit": False,
    "price_limit_halt_duration_minutes": 30, "auto_rollover_enabled": False,
    "rollover_trigger_days_before_expiry": 5, "rollover_slippage_bps_auto": 5.0,
    "dynamic_slippage_model": "fixed_tick", "slippage_adaptive_vol_window": 20,
    "slippage_adaptive_multiplier": 2.0, "css_premium_threshold": 0.5,
    "entry_button": 0.7, "max_position_count": 3,
    "alpha_window_days": 7, "max_retry_count": 3,
    "log_retention_days": 7, "retry_delay_sec": 5.0,
    **_STRATEGY_DEFAULTS,
    "max_signals_per_window": 10,
    "close_take_profit_ratio": 1.8,
}

class _ParamTableProxyDict(dict):
    """代理字典 — 对外完全兼容dict，内部委托到ParamTableProvider
    启用USE_PARAM_TABLE_PROVIDER FeatureFlag时，DEFAULT_PARAM_TABLE
    自动升级为此代理，480处引用方无需任何修改。'
    """
    def __getitem__(self, key):
        try:
            from governance.param_table_provider import get_param_table_provider
            _provider = get_param_table_provider()
            _val = _provider.get_default(key)
            if _val is not None:
                return _val
        except Exception:
            pass
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            from governance.param_table_provider import get_param_table_provider
            _provider = get_param_table_provider()
            _val = _provider.get_default(key)
            if _val is not None:
                return _val
        except Exception:
            pass
        return super().get(key, default)

    def __contains__(self, key):
        try:
            from governance.param_table_provider import get_param_table_provider
            _provider = get_param_table_provider()
            return key in _provider.list_strategies() or super().__contains__(key)
        except Exception:
            pass
        return super().__contains__(key)

try:
    from infra.metrics_registry import PhaseFeatureFlag
    if PhaseFeatureFlag.is_enabled('USE_PARAM_TABLE_PROVIDER'):
        _proxy = _ParamTableProxyDict(DEFAULT_PARAM_TABLE)
        DEFAULT_PARAM_TABLE = _proxy
        logging.info("[config_params] DEFAULT_PARAM_TABLE已升级为ParamTableProxyDict")
except Exception:
    pass

_DEFAULT_PARAM_TABLE_ORIGINAL = None
_DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED = False

def get_param_via_provider(key: str, default: Any = None) -> Any:

    try:
        from governance.param_table_provider import get_param_table_provider
        return get_param_table_provider().get_default(key, default)
    except Exception:
        return DEFAULT_PARAM_TABLE.get(key, default)

def _capture_default_param_table_original():
    global _DEFAULT_PARAM_TABLE_ORIGINAL, _DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED
    if not _DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED:
        _DEFAULT_PARAM_TABLE_ORIGINAL = copy.deepcopy(DEFAULT_PARAM_TABLE)
        _DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED = True

def _verify_default_param_table_integrity():

    from infra.resilience import _verify_default_param_table_integrity as _rc_fn
    _rc_fn()


_PARAM_CHANGE_SUBSCRIBERS: List[Callable[[Dict[str, Any]], None]] = []
_PARAM_CHANGE_SUBSCRIBERS_LOCK = threading.Lock()

# R24-P1-TR-10修复: 参数版本hash 从基于DEFAULT_PARAM_TABLE内容计算，用于审计追踪
_params_hash = hashlib.md5(str(sorted(DEFAULT_PARAM_TABLE.items())).encode()).hexdigest()[:8]


def register_param_change_callback(callback: Callable[[Dict[str, Any]], None]) -> None:
    if not callable(callback):
        raise TypeError("callback must be callable")
    with _PARAM_CHANGE_SUBSCRIBERS_LOCK:
        if callback not in _PARAM_CHANGE_SUBSCRIBERS:
            _PARAM_CHANGE_SUBSCRIBERS.append(callback)


def unregister_param_change_callback(callback: Callable[[Dict[str, Any]], None]) -> None:
    with _PARAM_CHANGE_SUBSCRIBERS_LOCK:
        if callback in _PARAM_CHANGE_SUBSCRIBERS:
            _PARAM_CHANGE_SUBSCRIBERS.remove(callback)


def _notify_param_change(changed_keys: List[str], source: str) -> None:
    with _PARAM_CHANGE_SUBSCRIBERS_LOCK:
        subscribers = list(_PARAM_CHANGE_SUBSCRIBERS)
    if not subscribers:
        return
    event = {'source': source, 'changed_keys': list(changed_keys), 'timestamp': time.time()}
    for cb in subscribers:
        try:
            cb(event)
        except Exception as e:
            logging.warning("[config_params] 参数变更回调执行失败: %s", e)


def _load_and_validate_params() -> Dict[str, Any]:
    try:
        cache = copy.deepcopy(load_default_params_from_json())
    except Exception as e:
        logging.warning("[config_params] JSON加载失败(%s)", e)
        cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
    if _OPTION_PARAM_RUNTIME_PATCH:
        cache.update(copy.deepcopy(_OPTION_PARAM_RUNTIME_PATCH))
    if 'state_param_sets' not in cache:
        cache['state_param_sets'] = get_default_state_param_sets()
    try:
        _v = validate_params(cache)
        if not _v.get('valid', True):
            for _e in _v.get('errors', []): logging.error("[config_params] 参数约束违规 %s", _e)
            logging.critical("[config_params] 参数约束校验失败，回退到DEFAULT_PARAM_TABLE")
            cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
            if 'state_param_sets' not in cache: cache['state_param_sets'] = get_default_state_param_sets()
        for _w in _v.get('warnings', []): logging.warning("[config_params] 参数约束警告: %s", _w)
    except Exception as _ve:
        logging.error("[config_params] 参数约束校验异常 %s", _ve)
    return cache


def get_cached_params(strategy_id: Optional[str] = None) -> Dict[str, Any]:
    global _param_table_cache, _param_table_cache_timestamp, _cache_access_count
    _param_ver = get_param_pool_version()
    if _param_ver and not hasattr(get_cached_params, '_last_param_version'):
        logging.info("[R16-P2-08] 参数池版本: %s", _param_ver)
        get_cached_params._last_param_version = _param_ver
    _now_integrity = time.time()
    if not hasattr(get_cached_params, '_last_integrity_check') or (_now_integrity - get_cached_params._last_integrity_check) > 60.0:
        _verify_default_param_table_integrity()
        get_cached_params._last_integrity_check = _now_integrity
    _param_age_monitor[strategy_id or 'default'] = time.time()
    if strategy_id is not None:
        with _param_table_lock:
            now = time.time()
            if strategy_id in _strategy_param_caches:
                ts = _strategy_cache_timestamps.get(strategy_id, 0.0)
                if now - ts < CACHE_TTL:
                    return _sanitize_for_return(_strategy_param_caches[strategy_id])
            cache = _load_and_validate_params()
            _strategy_param_caches[strategy_id] = cache
            _strategy_cache_timestamps[strategy_id] = now
            logging.info("[config_params] 策略级参数表已加载strategy_id=%s，包含%d个参数", strategy_id, len(cache))
            return _sanitize_for_return(cache)
    with _param_table_lock:
        now = time.time()
        _cache_access_count += 1
        is_refresh = _param_table_cache is not None
        if (_param_table_cache is not None and now - _param_table_cache_timestamp < CACHE_TTL):
            return _sanitize_for_return(_param_table_cache)
        try:
            _merge_lock = getattr(get_cached_params, '_merge_lock', None)
            if _merge_lock is None:
                _merge_lock = threading.RLock()
                get_cached_params._merge_lock = _merge_lock
            if not _merge_lock.acquire(timeout=5.0):
                if _param_table_cache is not None:
                    return _sanitize_for_return(_param_table_cache)
            try:
                pass
            finally:
                _merge_lock.release()
        except Exception:
            pass
        if (_param_table_cache is not None and _cache_access_count < _cache_access_threshold):
            return _sanitize_for_return(_param_table_cache)
        _param_table_cache = _load_and_validate_params()
        _param_table_cache_timestamp = now
        _cache_access_count = 0
        _cache_refresh_timestamps['param_table'] = now
        action = "刷新" if is_refresh else "加载"
        logging.debug("[config_params] 参数表已%s，包含%d个参数(TTL=%ss)", action, len(_param_table_cache), CACHE_TTL)
        return _sanitize_for_return(_param_table_cache)


def reset_param_cache() -> None:
    global _param_table_cache, _PARAMS_JSON_CACHE
    with _param_table_lock:
        _param_table_cache = None
        _PARAMS_JSON_CACHE = None



# R4-D-10修复: 配置热更新 — 已从config_json_loader.py导入check_config_hot_reload

# R4-D-12修复: 参数版本追踪 — 权威源在config_version_tracker.py
from config._params_canary_env import (
    _param_version_counter,
    _param_version_hash,
    _param_version_history,
    _record_param_version_snapshot,
    get_param_version,
    list_param_version_history,
    rollback_param_version,
)


# 安全函数/常量 — 权威源在security_config.py，此处从security_config引用
from infra.security_service import (
    SENSITIVE_KEYS,
    _sanitize_for_return,
    _TRUSTED_CREDENTIAL_CALLERS,
    get_sensitive_credential,
)



_TRUSTED_UPDATE_CALLERS = frozenset([
    'params_service', 'strategy_ecosystem', 'strategy_core_service',
    'risk_service', 'config_params', 'task_scheduler', 'main',
    'param_pool', 'param_version_manager', 'state_param_manager',
    '_inject_runtime_context',
])
_RISK_CRITICAL_PARAMS = frozenset([
    'max_risk_ratio', 'close_stop_loss_ratio', 'close_take_profit_ratio',
    'circuit_breaker_pause_sec', 'signal_cooldown_sec',
    'max_net_delta_pct', 'max_net_gamma_pct', 'max_net_vega_bps',
])

_SENSITIVE_KEY_PATTERNS = frozenset([
    'password', 'token', 'secret', 'key', 'credential', 'api_key', 'auth'
])

def _filter_sensitive_keys(keys: list) -> list:
    """过滤敏感键，用于日志输出"""
    if not keys:
        return []
    return [k for k in keys if not any(s in k.lower() for s in _SENSITIVE_KEY_PATTERNS)]


def update_cached_params(updates: Dict[str, Any], sync_default_table: bool = True, caller_id: str = "unknown", strategy_id: Optional[str] = None) -> Dict[str, Any]:
    """P2-R8-07修复: caller_id鉴权，只有受信任调用方可修改参数缓存"""
    global _param_table_cache, _param_table_cache_timestamp
    normalized_updates = dict(updates or {})
    if not normalized_updates:
        return get_cached_params(strategy_id=strategy_id)
    
    if caller_id not in _TRUSTED_UPDATE_CALLERS:
        logging.error("[config_params] P2-R8-07 鉴权失败: caller_id=%s", caller_id)
        try:
            from audit_log_utils import structured_audit_log as _sal_unauth
            _sal_unauth('param_change_unauthorized', 'blocked', {
                'caller_id': caller_id, 'keys': sorted(normalized_updates.keys()),
            })
        except Exception:
            pass
        return get_cached_params(strategy_id=strategy_id)
    

    _risk_modifications = set(normalized_updates.keys()) & _RISK_CRITICAL_PARAMS
    if _risk_modifications:
        logging.warning("[config_params] R15-SEC-05 风控参数修改: %s caller_id=%s", sorted(_risk_modifications), caller_id)
    with _param_table_lock:
        if strategy_id is not None:
            if strategy_id not in _strategy_param_caches:
                _strategy_param_caches[strategy_id] = copy.deepcopy(DEFAULT_PARAM_TABLE)
            target_cache = _strategy_param_caches[strategy_id]
        else:
            if _param_table_cache is None:
                _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
            target_cache = _param_table_cache
        caller_info = 'unknown'
        try:
            import traceback as _tb; _s = _tb.extract_stack()
            if len(_s) >= 2: caller_info = f'{_s[-2].filename}:{_s[-2].lineno}'
        except Exception:
            pass
        audit_changes = []
        for key, value in normalized_updates.items():
            old_value = target_cache.get(key, '<NEW>')
            target_cache[key] = value
            audit_changes.append(f'{key}: {old_value} -> {value}')
        if strategy_id is not None:
            _strategy_cache_timestamps[strategy_id] = time.time()
        else:
            _param_table_cache_timestamp = time.time()
    # R13-V2-004: 审计日志记录每次参数修改
    for change in audit_changes:
        try:
            _hk = os.environ.get('AUDIT_HMAC_KEY', 'default_audit_key_change_in_prod')
            if _hk == 'default_audit_key_change_in_prod':
                import secrets as _secrets
                _hk = _secrets.token_hex(16)
                os.environ['AUDIT_HMAC_KEY'] = _hk
                if not hasattr(update_cached_params, '_hmac_auto_gen_warned'):
                    update_cached_params._hmac_auto_gen_warned = True
                    logging.info(
                        "[SECURITY] AUDIT_HMAC_KEY未配置，已自动生成随机密钥"
                    )
            # FIX-20260704-AUDIT-HMAC-EMPTY: hashlib.new误用为hmac.new导致_sig始终为空
            # 根因: hashlib.new第一个参数是算法名(如'sha256')，传入密钥会抛ValueError被except捕获→_sig=''
            # 修复: 使用hmac.new(key, msg, digestmod)正确计算HMAC签名
            import hmac as _hmac_mod
            _sig = _hmac_mod.new(_hk.encode(), f'{change}:{caller_info}:{time.time()}'.encode(), hashlib.sha256).hexdigest()[:16]
        except Exception:
            _sig = ''
        logging.debug("[config_params] AUDIT: %s caller=%s hmac=%s", change, caller_id, _sig)
    safe_keys = _filter_sensitive_keys(list(normalized_updates.keys()))
    logging.info("[config_params] 已更新参数缓存字段：%s", ', '.join(sorted(safe_keys)))
    try:
        _record_param_version_snapshot('update_cached_params', target_cache)
    except (TypeError, AttributeError, copy.Error) as _deepcopy_err:
        logging.debug("[config_params] _record_param_version_snapshot skipped (unpicklable value): %s", _deepcopy_err)
    _notify_param_change(sorted(safe_keys), 'update_cached_params')
    for _k, _v in normalized_updates.items():
        try:
            from audit_log_utils import structured_audit_log as _sal
            _sal('param_change', 'updated', {'key': _k, 'old': str(target_cache.get(_k, '<NEW>')), 'new': str(_v), 'params_hash': _params_hash})
        except Exception:
            pass
    return target_cache



# JSON加载/热重载 — 权威源在config_json_loader.py
from config.config_loader import (
    _resolve_params_json_path,
    _resolve_runtime_env_name,
    _apply_env_overrides,
    load_default_params_from_json,
    check_config_hot_reload,
    _PARAMS_JSON_CACHE,
    PARAMS_JSON_CACHE_TTL,
    _PARAMS_JSON_LOCK,
)


def get_params_metadata(json_path: Optional[str] = None) -> Dict[str, Any]:
    """获取参数元数据（stub实现）"""
    return {}


def validate_params_with_metadata(params: Dict[str, Any], json_path: Optional[str] = None) -> Dict[str, Any]:
    """验证参数（stub实现）"""
    return {'valid': True, 'errors': [], 'warnings': []}


def apply_environment(params: Dict[str, Any], env: str) -> Dict[str, Any]:
    """应用环境配置（stub实现）"""
    return params


def resolve_config_with_priority(
    base_params: Optional[Dict[str, Any]] = None, 
    env_prefix: str = "PARAM_OVERRIDE_",
    default_table: Optional[Dict[str, Any]] = None,
    json_loader: Optional[Callable] = None,
) -> Dict[str, Any]:
    """按优先级解析配置（stub实现）"""
    if base_params:
        return base_params
    if default_table:
        return default_table
    return {}


def get_params_metadata_wrapper(json_path: Optional[str] = None) -> Dict[str, Any]:
    return get_params_metadata(json_path)


def validate_params(params: Dict[str, Any], json_path: Optional[str] = None) -> Dict[str, Any]:
    return validate_params_with_metadata(params, json_path)


def apply_environment_wrapper(params: Dict[str, Any], env: str) -> Dict[str, Any]:
    return apply_environment(params, env)


def rebuild_default_param_table(env: Optional[str] = None) -> Dict[str, Any]:

    result = load_default_params_from_json()
    if env: result = apply_environment(result, env)
    validation = validate_params(result)
    if not validation['valid']:
        for err in validation['errors']: logging.error("[config_params] 参数校验失败: %s", err)
        raise ValueError(f"参数校验失败，共 {len(validation['errors'])} 个错误")
    for w in validation.get('warnings', []): logging.warning("[config_params] 参数校验警告: %s", w)
    return result


def _normalize_option_params_payload(payload: Any, source_path: str) -> Dict[str, Dict[str, Any]]:
    from config.config_loader import _normalize_option_params_payload as _fn
    return _fn(payload, source_path)

def load_option_params_from_file() -> Dict[str, Any]:
    from config.config_loader import load_option_params_from_file as _fn
    return _fn()

def merge_option_params_to_default():
    from config.config_loader import merge_option_params_to_default as _fn
    _fn()

def _sync_defaults_from_attribute_matrix() -> None:
    pass

def verify_param_pool_sync():
    pass

def get_params_metadata(json_path: Optional[str] = None) -> Dict[str, Any]:
    return get_params_metadata_wrapper(json_path)

def validate_params_wrapper(params: Dict[str, Any], json_path: Optional[str] = None) -> Dict[str, Any]:
    return validate_params_with_metadata(params, json_path)

def apply_environment_wrapper2(params: Dict[str, Any], env: str) -> Dict[str, Any]:
    return apply_environment(params, env)


try:
    merge_option_params_to_default()
except Exception:
    pass
try:
    _sync_defaults_from_attribute_matrix()
except Exception:
    pass
try:
    verify_param_pool_sync()
except Exception:
    pass


# R4-D-01修复: 配置加载优先级(1.环境变量>2.JSON>3.YAML>4.代码默认)
_CONFIG_PRIORITY_LOGGED = False


def resolve_config_with_priority_wrapper(
    base_params: Optional[Dict[str, Any]] = None, env_prefix: str = "PARAM_OVERRIDE_",
) -> Dict[str, Any]:
    return resolve_config_with_priority(base_params, env_prefix, DEFAULT_PARAM_TABLE, load_default_params_from_json)


# 状态参数集默认值 — 从config_state_defaults re-export

# R13-P1-CFG-10修复: 环境变量文档 — 权威源在config_resolver.py
from config.config_loader import _DOCUMENTED_ENV_VARS




# R15-P2 性能+数据质量配置
import functools

@functools.lru_cache(maxsize=256)
def _get_config_value_cached(key: str, default: Any = None) -> Any:
    """带lru_cache的配置值读取"""
    try:
        params = get_cached_params()
        return params.get(key, default)
    except Exception:
        return default


_PARAM_ALIASES: Dict[str, str] = {
    'max_position_count': 'max_open_positions',
}


def get_param(key: str, default: Any = None) -> Any:
    """统一参数读取入口，支持别名解析"""
    try:
        params = get_cached_params()
        if key in params: return params[key]
        canonical = _PARAM_ALIASES.get(key)
        if canonical and canonical in params: return params[canonical]
    except Exception:
        pass
    return default


DATA_CLEANING_RULES = {
    'remove_outliers': True,
    'outlier_sigma': 3.0,
    'fill_missing': True,
    'validate_timestamps': True,
}

DATA_EXPORT_FORMAT = 'parquet'
_DATA_SOURCE_TAG_FIELD = 'data_source'


from config.config_loader import _data_quality_score



# R15-P2 容错配置 — 权威源在resilience.py
from infra.resilience import (
    _auto_recovery_decision_tree,
    CHECKPOINT_INTERVAL_SECONDS,
    RESILIENCE_REGISTRY,
    WATCHDOG_TIMEOUT_ORDER_SEC,
    WATCHDOG_TIMEOUT_POSITION_SEC,
    WATCHDOG_TIMEOUT_RISK_SEC,
    WATCHDOG_TIMEOUT_SIGNAL_SEC,
    WATCHDOG_TIMEOUT_TICK_SEC,
    DEGRADATION_FEATURES,
    SLA_CONFIG,
    ALARM_LEVELS,
    ISOLATION_LEVELS,
    CAPACITY_LIMITS,
    COLD_START_TIMEOUT,
)


_HOT_RELOAD_UNSUPPORTED_KEYS = frozenset([
    'tvf_enabled', 'tvf_sigmoid_scale', 'mode_engine_version',
    'max_risk_ratio', 'close_stop_loss_ratio', 'signal_cooldown_sec',
])

def get_hot_reload_status() -> Dict[str, Any]:

    return {
        'supported_keys': sorted(set(DEFAULT_PARAM_TABLE.keys()) - _HOT_RELOAD_UNSUPPORTED_KEYS),
        'unsupported_keys': sorted(_HOT_RELOAD_UNSUPPORTED_KEYS),
        'requires_restart_hint': f"以下参数变更需重启: {', '.join(sorted(_HOT_RELOAD_UNSUPPORTED_KEYS))}",
    }


# Phase3-Sprint7: 向后兼容重导出
from infra.security_service import (
    check_key_lifecycle,
    SecurityEventResponder,
    get_security_responder,
)

# R27: Re-export from config_state_defaults for backward compatibility
def get_default_state_param_sets() -> Dict[str, Dict[str, Any]]:
    return {
        'default': {},
        'conservative': {},
        'aggressive': {},
    }

def get_documented_env_vars() -> Dict[str, Dict[str, str]]:
    return _DOCUMENTED_ENV_VARS

from infra.resilience import (
    validate_config_schema,
    validate_ui_params,
)


# Phase3-Sprint8: ParamTableProvider re-export
from governance.param_table_provider import get_param_table_provider
