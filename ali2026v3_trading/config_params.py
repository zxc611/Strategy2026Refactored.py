"""
参数表管理模块- 从config_service.py 拆分
职责：默认参数表、参数缓存、参数加载校验/环境覆盖
"""
from __future__ import annotations

# R25-SE-P1-02-FIX: 策略状态模式集中常量定义，消除+文件硬编码
STRATEGY_MODE_CORRECT_TRENDING = "correct_trending"
STRATEGY_MODE_INCORRECT_REVERSAL = "incorrect_reversal"
STRATEGY_MODE_SPRING = "spring"
STRATEGY_MODE_RESONANCE = "resonance"
STRATEGY_MODE_BOX = "box"
STRATEGY_MODE_HFT = "hft"
ALL_STRATEGY_MODES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_SPRING,
    STRATEGY_MODE_RESONANCE,
    STRATEGY_MODE_BOX,
    STRATEGY_MODE_HFT,
)
# R25-SE-P1-02-FIX: 五态市场状态集中常量（state_param_manager使用）
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
# 时间单位常量
TIME_UNIT_MS = "ms"
TIME_UNIT_SEC = "sec"
TIME_UNIT_MIN = "min"

import copy
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from datetime import datetime

from ali2026v3_trading.shared_utils import CHINA_TZ

_PARAM_POOL_VERSION = '2.0.0'
_PARAM_POOL_VERSION_HISTORY = [
    {'version': '1.0.0', 'date': '2026-05-10', 'changes': '初始版本'},
    {'version': '2.0.0', 'date': '2026-05-30', 'changes': 'R16审计修复: max_risk_ratio/tvf_enabled/option_buy_lots/state_confirm_bars对齐'},
]

def get_param_pool_version() -> str:
    return _PARAM_POOL_VERSION

def get_param_pool_version_history() -> list:
    return copy.deepcopy(_PARAM_POOL_VERSION_HISTORY)

# EC-P2-03修复: Python最小版本检查
from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load

if sys.version_info < (3, 8):
    raise RuntimeError(f"ali2026v3_trading要求Python>=3.8，当前版本{sys.version}")
from typing import Any, Callable, Dict, List, Optional

# EC-P2-10: 关键依赖库版本检查
try:
    import numpy as _np
    if tuple(int(x) for x in _np.__version__.split('.')[:2]) < (1, 21):
        logging.warning("numpy>=1.21 recommended, current: %s", _np.__version__)
except ImportError:
    pass
try:
    import pandas as _pd
    if tuple(int(x) for x in _pd.__version__.split('.')[:2]) < (1, 5):
        logging.warning("pandas>=1.5 recommended, current: %s", _pd.__version__)
except ImportError:
    pass

from ali2026v3_trading.config_exchange import ExchangeConfig
from ali2026v3_trading.shared_utils import safe_int, safe_float


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
    """P2-R11-02修复: 清理指定策略的参数缓存"""  # R25-P0-FIX
    import time as _t
    _now = _t.time()
    if strategy_id in _invalidate_idempotent and (_now - _invalidate_idempotent[strategy_id]) < _INVALIDATE_IDEMPOTENT_SEC:
        return False
    _invalidate_idempotent[strategy_id] = _now
    if strategy_id in _strategy_param_caches:
        del _strategy_param_caches[strategy_id]
        _strategy_cache_timestamps.pop(strategy_id, None)
        logging.info("[config_params] P2-R11-02: 已清理策略级参数缓存 strategy_id=%s", strategy_id)
        return True
    return False
CACHE_TTL = 60.0  # R23-FR-01-FIX: 缓存TTL从00秒缩短至60秒，极端行情下减少过期数据风险
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
    import time as _t
    _now = _t.time()
    ages = {}
    ages['param_table'] = _now - _param_table_cache_timestamp if _param_table_cache_timestamp > 0 else -1.0
    for _sid, _ts in _strategy_cache_timestamps.items():
        ages[f'strategy_{_sid}'] = _now - _ts
    for _key, _ts in _cache_refresh_timestamps.items():
        ages[f'refresh_{_key}'] = _now - _ts
    # [R23-P2-FR-10-FIX] 参数年龄监控汇总
    for _sid, _at in _param_age_monitor.items():
        ages[f'access_{_sid}'] = _now - _at
    return ages


# [R23-P2-FR-10-FIX] 参数年龄监控汇总方法
def get_param_age_summary() -> Dict[str, Any]:
    """返回参数年龄监控汇总"""
    import time as _t
    _now = _t.time()
    _summary = {}
    for _sid, _at in _param_age_monitor.items():
        _summary[_sid] = {'age_sec': _now - _at, 'last_access': _at}
    return _summary


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


def check_multi_level_cache_consistency() -> Dict[str, bool]:
    """[FR-P1-11-FIX] 多级缓存一致性校验"""
    import time as _t
    _now = _t.time()
    result = {}
    for _sid in _strategy_param_caches:
        _strategy_ts = _strategy_cache_timestamps.get(_sid, 0.0)
        result[_sid] = (_now - _strategy_ts) < CACHE_TTL
    return result


def record_pipeline_latency(stage: str, latency_sec: float) -> None:
    """[FR-P1-12-FIX] 数据管道延迟监控"""
    _pipeline_latency_monitor[stage] = latency_sec


def mark_computation_timestamp(computation_id: str) -> None:
    """[FR-P1-13-FIX] 计算结果时间戳标记"""
    import time as _t
    _computation_timestamps[computation_id] = _t.time()


def check_indicator_freshness(indicator_key: str, max_age_sec: float = 60.0) -> bool:
    """[FR-P1-14-FIX] 指标计算新鲜度校验"""
    import time as _t
    _entry = _indicator_freshness_cache.get(indicator_key)
    if _entry is None:
        return False
    _ts, _ = _entry
    return (_t.time() - _ts) < max_age_sec


def record_indicator_value(indicator_key: str, value: Any) -> None:
    """[FR-P1-14-FIX] 记录指标值和时间戳"""
    import time as _t
    _indicator_freshness_cache[indicator_key] = (_t.time(), value)


def check_risk_data_freshness(risk_key: str, max_age_sec: float = 5.0) -> bool:
    """[FR-P1-15-FIX] 风控数据新鲜度校验"""
    import time as _t
    _ts = _risk_data_freshness.get(risk_key, 0.0)
    return (_t.time() - _ts) < max_age_sec


def update_risk_data_timestamp(risk_key: str) -> None:
    """[FR-P1-15-FIX] 更新风控数据时间戳"""
    import time as _t
    _risk_data_freshness[risk_key] = _t.time()

_DEFAULT_PARAM_TABLE_LOCK = threading.Lock()

# R24-P1-DF-06修复: 中心化默认值常量体系、单一真相源
CENTRALIZED_DEFAULTS = {
    'max_risk_ratio': 0.8,
    'close_stop_loss_ratio': 0.3,
    'close_take_profit_ratio': 1.8,  # R24-P1-DF-07修复: close_take_profit_ratio 1.5改.8，与V7手册S1对齐
    'signal_cooldown_sec': 60.0,  # R24-P1-DF-10修复: 三系统默认值一致CENTRALIZED_DEFAULTS=60.0, DEFAULT_PARAM_TABLE=60.0, risk_service=60.0, task_scheduler=60.0, signal_service=60.0)
    'state_confirm_bars': 5,
    'alpha_window_days': 7,
    'circuit_breaker_pause_sec': 180.0,
    'daily_drawdown_multiplier': 2.0,
    'max_hold_minutes': 120,
    'max_signals_per_window': 10,
    'lots_min': 3,  # R24-P1-DF-08修复: lots_min 5改，与V7手册S1对齐
    'option_buy_lots_min': 1,
    'option_buy_lots_max': 100,
    'tvf_enabled': True,  # P1-01修复: TVF三因子投票开关
    'tvf_l1_weight': 0.40,  # P1-01修复: TVF L1因子权重
    'tvf_l2_weight': 0.35,  # P1-01修复: TVF L2因子权重
    'tvf_l3_weight': 0.25,  # P1-01修复: TVF L3因子权重
    # R19-P1-02修复: 补齐params_default.json中但CENTRALIZED_DEFAULTS缺失的风控关键参数
    'position_limit_max_ratio': 0.2,
    'circuit_breaker_trigger_sigma': 3.0,
    'kline_max_age_sec': 60,
    'signal_max_age_sec': 180,
    'subscription_batch_size': 10,
    'rate_limit_min_interval_sec': 1,
    'enforce_trading_session_boundary': True,
    'last_trading_day_close_ahead_days': 3,
    'delivery_slippage_multiplier_max': 3.0,
    'default_slippage_bps': 0.0,
    'intraday_max_total_ticks': 5000000,
    'tick_shard_count': 16,
    'CACHE_TTL': 60.0,
    # R24-P1-DF-05修复: 策略级止损比率默认值集中管理
    'STRATEGY_SL_RATIOS': {
        'correct_trending': 0.4,
        'incorrect_reversal': 0.3,
        'other': 0.5,
        'box_extreme': 0.3,
        'hft': 0.2,
    },
    'position_timeout_sec': 3600,  # R24-P1-DF-12修复: 持仓超时默认值
    'max_retry_count': 3,  # R24-P1-DF-14修复: 最大重试次数默认值
    'retry_delay_sec': 5.0,  # R24-P1-DF-15修复: 重试间隔默认值
    'log_retention_days': 7,  # R24-P1-DF-17修复: 日志保留天数默认值
}

class _SecureCredential:
    __slots__ = ('_obfuscated', '_seed')

    def __init__(self, value: str):
        self._seed = os.urandom(8)
        self._obfuscated = self._obfuscate(value)

    def _obfuscate(self, value: str) -> bytes:
        if not value:
            return b''
        data = value.encode('utf-8')
        key = self._seed * ((len(data) // len(self._seed)) + 1)
        return bytes(a ^ b for a, b in zip(data, key[:len(data)]))

    def reveal(self) -> str:
        if not self._obfuscated:
            return ''
        key = self._seed * ((len(self._obfuscated) // len(self._seed)) + 1)
        data = bytes(a ^ b for a, b in zip(self._obfuscated, key[:len(self._obfuscated)]))
        return data.decode('utf-8')

    def __repr__(self) -> str:
        return '***REDACTED***'

    def __str__(self) -> str:
        return '***REDACTED***'

def reveal_credential(cred) -> str:
    if isinstance(cred, _SecureCredential):
        return cred.reveal()
    return str(cred) if cred else ''

DEFAULT_PARAM_TABLE = {  # R21-MEM-P2-05修复: 模块级大字典(~200个，无分片机制；若内存敏感可按功能域拆分为子字典懒加载
    "max_kline": 200,
    "kline_style": "M1",
    "subscribe_options": True,
    "debug_output": True,
    "diagnostic_output": True,
    # R14-P0-CFG-06修复: 敏感凭据从环境变量读取，空字符串兜底仅用于离线回测
    "api_key": _SecureCredential(os.environ.get("ALI2026_API_KEY", "")),
    "infini_api_key": _SecureCredential(os.environ.get("ALI2026_INFINI_API_KEY", "")),
    "access_key": _SecureCredential(os.environ.get("ALI2026_ACCESS_KEY", "")),
    "access_secret": _SecureCredential(os.environ.get("ALI2026_ACCESS_SECRET", "")),
    "_sensitive_fields": ["api_key", "infini_api_key", "access_key", "access_secret"],
    "run_profile": "full",
    "enable_scheduler": True,
    "use_tick_kline_generator": True,
    "backtest_tick_mode": False,
    "exchange": "CFFEX",
    "future_product": "IF",
    "option_product": "IO",
    "auto_load_history": True,
    "load_history_options": True,
    "load_all_products": True,
    "exchanges": "CFFEX,SHFE,DCE,CZCE,INE,GFEX",
    # EX-P1-01/02: CFFEX股指期货(IF/IC/IM/IH)无夜盘，但配置中未显式标注；
    # SHFE/DCE/CZCE/INE部分品种有夜盘21:00-次日02:30)，需交易所规则对接确认
    "night_session_exchanges": "SHFE,DCE,CZCE,INE",
    # EX-P1-03: 集合竞价时段配置（开盘前5分钟+收盘后分钟；
    "auction_session_minutes_before_open": 5,
    "auction_session_minutes_before_close": 5,
    # EX-P1-04: 节假日配置（chinese_calendar库或内置日历源
    "holiday_calendar_source": "chinese_calendar",
    # EX-P1-05: 交易时间边界Bar严格校验
    "enforce_trading_session_boundary": True,
    # EX-P1-06: 最后交易日规则（到期前N日强制平仓移仓；
    "last_trading_day_close_ahead_days": 3,
    "last_trading_day_rollover_ahead_days": 5,
    # EX-P2-08: 交割日自动移仓配置（各交易所移仓提前天数）
    "rollover_days": {
        "CFFEX": 5,
        "SHFE": 5,
        "DCE": 5,
        "CZCE": 7,
        "INE": 5,
        "GFEX": 5,
    },
    # EX-P1-07: 移仓滑点模拟（基点）
    "rollover_slippage_bps": 5.0,
    # EX-P1-08: 各品种最小价格变动tick_size)配置，用于价格对齐到交易所整数倍
    "tick_size_by_product": {
        "IF": 0.2, "IC": 0.2, "IH": 0.2, "IM": 0.2,
        "rb": 1, "hc": 1, "i": 0.5, "j": 0.5, "jm": 0.5,
        "cu": 10, "al": 5, "zn": 5, "pb": 5, "ni": 10, "sn": 10, "au": 0.02, "ag": 1,
        "m": 1, "y": 2, "a": 1, "c": 1, "cs": 1, "p": 2,
        "SR": 1, "CF": 5, "TA": 2, "MA": 1, "FG": 1, "SA": 1,
        "sc": 0.1, "lu": 5, "fu": 1, "pg": 1, "bc": 0.1, "ec": 0.1,
    },
    # EX-P1-09: 交割日临近滑点倍增系数
    "delivery_slippage_multiplier_days": 10,
    "delivery_slippage_multiplier_max": 3.0,
    # EX-P1-10: 平今/平昨手续费率差异
    "close_today_commission_multiplier": 1.0,
    "close_yesterday_commission_multiplier": 1.0,
    # EX-P1-11: 手续费率版本与校验日期
    "commission_rate_version": "2026-01",
    "commission_rate_last_verify_date": "",
    # EX-P1-12: 期权权利金计算模式（CFFEX欧式/大商所美式期权
    "option_pricing_mode": "auto",
    # EX-P1-13: 涨跌停板扩板规则（连续涨跌停后扩板倍数等
    "price_limit_expansion_after_consecutive_limit": 1.5,
    # EX-P1-14: 最低保证金比例调整（交易所临时调整)
    "min_margin_ratio_override": {},
    # EX-P1-15: 限仓额度动态变化（交割月限仓比例）
    "delivery_month_position_limit_ratio": 0.5,
    # EX-P1-16: 交割月持仓限制（手数上限)
    "delivery_month_max_position": 0,
    # EX-P2-03: 各交易所交割月份规则（含INE能源交易中心)
    "delivery_month_rules": {
        "CFFEX": {"delivery_months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "notice_day_offset": -3, "last_trade_day_offset": -1},
        "SHFE": {"delivery_months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "notice_day_offset": -5, "last_trade_day_offset": -3},
        "DCE":  {"delivery_months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "notice_day_offset": -5, "last_trade_day_offset": -3},
        "CZCE": {"delivery_months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "notice_day_offset": -5, "last_trade_day_offset": -3},
        "GFEX": {"delivery_months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "notice_day_offset": -5, "last_trade_day_offset": -3},
        "INE":  {"delivery_months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "notice_day_offset": -5, "last_trade_day_offset": -3},
    },
    # EX-P1-17: 跨期套利保证金优惠比例)
    "cross_expiry_margin_discount_ratio": 0.5,
    # EX-P1-18: 大户报告阈值（手数)
    "large_position_report_threshold": 0,
    # EX-P1-19: 强行减仓规则（单方向连续涨跌停N次后扩板)
    "forced_reduction_after_consecutive_limit": 3,
    # EX-P1-20: 交易所临时调整通知（字典：{exchange: {param: value}})
    "exchange_temp_adjustments": {},
    # EX-P1-21: 做市商义务（报价价差上限/最小报价量等)
    "market_maker_max_spread_bps": 50,
    "market_maker_min_quote_lots": 1,
    # EX-P1-22: 组合保证金优惠（跨品种对冲折扣）
    "portfolio_margin_discount_ratio": 0.8,
    "future_products": "",
    "option_products": "",
    "future_instruments": [],
    "option_instruments": {},
    "instrument_cache_source": "",
    "include_future_products_for_options": True,
    "subscription_batch_size": 10,
    "subscription_interval": 1,
    "subscription_fetch_on_subscribe": True,
    "subscription_fetch_count": 5,
    "subscription_fetch_for_options": False,
    "rate_limit_min_interval_sec": 1,
    "rate_limit_per_instrument": 2,
    "rate_limit_global_per_min": 60,
    "rate_limit_window_sec": 120,
    "subscription_backoff_factor": 1.0,
    "subscribe_only_current_next_options": True,
    "subscribe_only_current_next_futures": True,
    "enable_doc_examples": False,
    "pause_unsubscribe_all": True,
    "pause_force_stop_scheduler": True,
    "pause_on_stop": False,
    "history_minutes": 1440,
    "history_load_batch_size": 200,
    "history_load_max_batch_size": 50,
    "history_load_batch_delay_sec": 0.2,
    "history_load_request_delay_sec": 0.1,
    "log_file_path": "strategy_startup.log",
    "test_mode": False,
    "auto_start_after_init": False,
    "subscribe_only_specified_month_options": True,
    "subscribe_only_specified_month_futures": True,
    "load_month_params_in_init": False,
    "specified_month": "",
    "next_specified_month_1": "",
    "next_specified_month_2": "",
    "next_specified_month_3": "",
    "next_specified_month_4": "",
    "option_width_month_count": 2,
    "option_width_min_threshold": 4.0,
    "project_root": str(Path(__file__).parent.parent),
    "workspace": str(Path(__file__).parent.parent),
    "month_mapping": {
        "IF": ["IF2602", "IF2603", "IF2606", "IF2609", "IF2612"],
        "IH": ["IH2602", "IH2603", "IH2606", "IH2609", "IH2612"],
        "IM": ["IM2602", "IM2603", "IM2606", "IM2609", "IM2612"],
        "CU": ["CU2603", "CU2604", "CU2605", "CU2606", "CU2607"],
        "AL": ["AL2603", "AL2604", "AL2605", "AL2606", "AL2607"],
        "ZN": ["ZN2603", "ZN2604", "ZN2605", "ZN2606", "ZN2607"],
        "RB": ["RB2603", "RB2605", "RB2606", "RB2610", "RB2611"],
        "AU": ["AU2603", "AU2604", "AU2606", "AU2608", "AU2612"],
        "AG": ["AG2603", "AG2604", "AG2606", "AG2609", "AG2612"],
        "M":  ["M2603",  "M2605",  "M2607",  "M2608",  "M2609"],
        "Y":  ["Y2603",  "Y2605",  "Y2607",  "Y2608",  "Y2609"],
        "A":  ["A2603",  "A2605",  "A2607",  "A2609",  "A2611"],
        "JM": ["JM2604", "JM2605", "JM2606", "JM2609", "JM2612"],
        "I":  ["I2603",  "I2605",  "I2606",  "I2609",  "I2610"],
        "J":  ["J2604",  "J2605",  "J2606",  "J2609",  "J2612"],
        "CF": ["CF2603", "CF2605", "CF2607", "CF2609", "CF2611"],
        "SR": ["SR2603", "SR2605", "SR2607", "SR2609", "SR2611"],
        "MA": ["MA2603", "MA2605", "MA2606", "MA2609", "MA2612"],
        "TA": ["TA2603", "TA2605", "TA2606", "TA2609", "TA2612"],
    },
    "signal_cooldown_sec": 60.0,  # R13-P1-CFG-02修复: 默认值从0改为60(V7.0手册之前；允许无冷却高频开仓极不安全
    "option_buy_lots_min": 1,  # R24-P2-DF-04修复: 期权最小买入手数)
    "option_buy_lots_max": 100,  # R24-P2-DF-04修复: 期权最大买入手数)
    "option_contract_multiplier": 10000,  # R24-P2-DF-04修复: 期权合约乘数
    "position_limit_valid_hours_max": 720,
    "position_limit_default_valid_hours": 24,
    "position_limit_max_ratio": 0.2,
    "position_limit_min_amount": 1000,
    "option_order_price_type": "2",
    "option_order_time_condition": "3",
    "option_order_volume_condition": "1",
    "option_order_contingent_condition": "1",
    "option_order_force_close_reason": "0",
    "option_order_hedge_flag": "1",
    "option_order_min_volume": 1,
    "option_order_business_unit": "1",
    "option_order_is_auto_suspend": 0,
    "option_order_user_force_close": 0,
    "option_order_is_swap": 0,
    "close_take_profit_ratio": 1.8,  # R24-P1-DF-07修复: close_take_profit_ratio 1.5改.8，与V7手册S1对齐；全局兜底默认值，被get_default_state_param_sets()中各状态特定值覆盖correct_trending=2.5, incorrect_reversal=1.3, other_scalp=1.1)
    "close_overnight_check_time": "14:58",
    "close_daycut_time": "15:58",
    "close_max_hold_days": 3,
    "close_overnight_loss_threshold": -0.5,
    "close_overnight_profit_threshold": 4.0,
    "close_max_chase_attempts": 5,
    "close_chase_interval_seconds": 2,
    "close_chase_task_timeout_seconds": 30,
    "close_delayed_timeout_seconds": 30,
    "close_delayed_max_retries": 3,
    "close_order_price_type": "2",
    "output_mode": "debug",
    "force_debug_on_start": True,
    "enable_output_mode_ui": True,
    "daily_summary_hour": 15,
    "daily_summary_minute": 1,
    "trade_quiet": True,
    "print_start_snapshots": False,
    "trade_debug_allowlist": "",
    "debug_disable_categories": "",
    "debug_throttle_seconds": 0.0,
    "debug_throttle_map": {},
    "use_param_overrides_in_debug": False,
    "param_override_table": "",
    "param_edit_limit_per_month": 1,
    "backtest_params": {},
    "ignore_otm_filter": False,
    "allow_minimal_signal": False,
    "min_option_width": 1,
    "async_history_load": True,
    "manual_trade_limit_per_half": 1,
    "morning_afternoon_split_hour": 12,
    "account_id": "",
    "kline_max_age_sec": 60,  # R23-FR-04-FIX: K线过期淘汰启用，60秒超时
    "signal_max_age_sec": 180,
    "lots_min": 3,  # R24-P1-DF-08修复: lots_min 5改，与V7手册S1对齐
    "history_load_max_workers": 4,
    "option_sync_tolerance": 0.5,
    "option_sync_allow_flat": True,
    "index_option_prefixes": "",
    "option_group_exchanges": "",
    "czce_year_future_window": 10,
    "czce_year_past_window": 10,
    "exchange_mapping": dict(ExchangeConfig().product_exchanges),
    "intraday_memory_mode": "debug",
    "intraday_max_ticks_per_symbol": 1000,
    "intraday_max_total_ticks": 5000000,
    "tick_shard_count": 16,
    "tick_writer_count": 6,
    "tick_shard_queue_capacity": 625000,
    "kline_queue_capacity": 100000,
    "maintenance_queue_capacity": 50000,
    "spill_enabled": False,
    "spill_base_dir": "",
    "circuit_breaker_pause_sec": 180.0,
    "circuit_breaker_trigger_sigma": 3.0,
    "daily_drawdown_multiplier": 2.0,
    "hft_hard_time_stop_ms": 1000,
    "spring_hard_time_stop_sec": 30,
    "resonance_hard_time_stop_min": 5,
    "box_hard_time_stop_min": 30,
    "min_profit_threshold": 0.002,
    "max_net_delta_pct": 0.30,
    "max_net_gamma_pct": 0.08,
    "max_net_vega_bps": 0.02,
    "max_theta_ratio": 0.5,
    "debug_mode": False,
    "stress_test_mode": False,
    "stress_test_anomaly_threshold": 1.5,
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: 默认值从0.5改为0.3，与V7.0手册§9.1对齐(YAML状态：correct_trending=0.4, incorrect_reversal=0.6, other=0.8)
    "max_risk_ratio": 0.8,  # R24-P0-DF-01修复: 默认值从0.3改为0.8，与YAML correct_trending主状态值对齐；YAML状态：correct_trending=0.8, incorrect_reversal=0.3, other=0.2)
    "max_signals_per_window": 10,
    "max_hold_minutes": 120,
    "box_gain_ratio": 0.5,
    "plr_normalization_base": 3.0,
    "box_breakout_tolerance": 0.005,
    "spring_price_pos_min": 0.3,
    "spring_price_pos_max": 0.7,
    "strike_distance_threshold": 0.02,
    "direction_buy_call_threshold": 0.45,
    "direction_buy_put_threshold": 0.55,
    "default_order_flow_consistency": 0.5,
    "kline_missing_timeout_multiplier": 2.0,
    "phase_scan_gate_threshold": 25,
    "phase_scan_score_weights": [0.4, 0.3, 0.3],
    "default_slippage_bps": 3.0,
    # R7-M-12修复: capital_route_s1~s6资金路由参数（手册7.3节）
    "capital_route_s1_hft": 0.30,
    "capital_route_s2_minute": 0.20,
    "capital_route_s3_box_extreme": 0.15,
    "capital_route_s4_box_spring": 0.15,
    "capital_route_s5_arbitrage": 0.10,
    "capital_route_s6_market_making": 0.10,
    # R7-M-01修复: VaR动态升级参数（手册第68行）
    "var_confidence_level": 0.95,
    "var_upgrade_threshold": 0.02,
    "atr_upgrade_threshold": 0.015,
    # R7-M-02修复: 期权买方折扣系数（手册第854行）
    "option_buyer_discount": 0.6,
    # R7-M-13修复: Sigmoid归一化参数统一配置（手册7.2.3节）
    "sigmoid_alpha": 1.0,
    "sigmoid_beta": 0.0,
    # R15-P1-DATA-02修复: Bar合成模式统一选择('standard'/'legacy'/'hft')
    "bar_synthesis_mode": "standard",
    # R15-P1-DATA-03修复: K线合成夜盘结束小时配置取代硬编码datetime边界)
    "night_session_end_hour": 15,
    "night_session_eod_hour": 2,
    "night_session_eod_minute": 30,
    # EX-P2-04: 各交易所交易时段配置（含INE能源交易所）
    "exchange_trading_sessions": {
        "CFFEX": {"day_start": "09:30", "day_end": "15:00", "has_night": False},
        "SHFE": {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "02:30"},
        "DCE":  {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "02:30"},
        "CZCE": {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "23:30"},
        "INE":  {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "02:30"},
        "GFEX": {"day_start": "09:00", "day_end": "15:00", "has_night": False},
    },
    # R15-P1-SEC-06修复: mlock提示注释(Linux下可mlock防止swap)
    # "use_mlock": True,  # Linux: 调用ctypes.CDLL('libc.so.6').mlockall(3)锁住内存防止core dump泄露
    # R15-P1-SEC-07修复: 密钥轮换配置
    "key_rotation_interval_days": 90,
    "key_last_rotation_time": None,
    # R15-P1-SEC-09修复: 密钥生命周期管理(IntervalTimer自动轮换)
    "key_lifecycle_enabled": True,
    "key_max_age_hours": 2160,  # 90秒
    # R15-P1-SEC-12修复: 安全事件自动响应配置
    "security_auto_block_enabled": True,
    "security_block_on_suspicious_activity": True,
    # =========================================================================
    # NP-P2-05: Decimal引入金额计算（架构级，需全链路改造）
    "use_decimal_for_settlement": False,  # NP-P2-05: Decimal金额计算开关（架构级，需全链路改造）
    # NP-P2-28: Decimal模块引入（与NP-P2-05合并)
    "decimal_precision": 8,  # NP-P2-28: Decimal精度位数（当use_decimal_for_settlement=True时生效）
    # NP-P2-19: ATM浮点减法补偿算法
    "use_compensated_subtraction_for_atm": False,  # NP-P2-19: ATM补偿减法开关)
    # EC-P2-13: 跨进程共享内存(DLL依赖
    "check_runtime_dependencies": True,  # EC-P2-13: 运行时DLL依赖检查占用)
    # EX-P2-04: 手续费自动更新机制)
    "commission_auto_update_enabled": False,  # EX-P2-04: 手续费自动更新开关)
    "commission_update_source": "exchange_api",  # EX-P2-04: 更新源（exchange_api/manual/config_file)
    "commission_update_interval_days": 30,  # EX-P2-04: 更新间隔（天)
    # EX-P2-07: 涨跌停动态休市)
    "dynamic_halt_on_price_limit": False,  # EX-P2-07: 涨跌停动态休市开关)
    "price_limit_halt_duration_minutes": 30,  # EX-P2-07: 休市持续时间（分钟）
    # EX-P2-08: 自动移仓
    "auto_rollover_enabled": False,  # EX-P2-08: 自动移仓开关)
    "rollover_trigger_days_before_expiry": 5,  # EX-P2-08: 到期前N日触发移仓)
    "rollover_slippage_bps_auto": 5.0,  # EX-P2-08: 移仓滑点（基点）
    # EX-P2-10: 动态滑点模式)
    "dynamic_slippage_model": "fixed_tick",  # EX-P2-10: 滑点模型（fixed_tick/volume_weighted/volatility_adaptive)
    "slippage_adaptive_vol_window": 20,  # EX-P2-10: 波动率窗口)
    "slippage_adaptive_multiplier": 2.0,  # EX-P2-10: 波动率倍数
    "css_premium_threshold": 0.5,  # P1-1修复: 信用利差权利金阈值，用于_compute_life_score中CSS评分加权
    "entry_button": 0.7,  # P1-2修复: 信号入场确认按钮阈值，多信号共振确认时需超过此阈值方可入场)
    "max_open_positions": 3,  # P1-4修复: 最大持仓合约数量统一参数(规范名称)，task_scheduler/risk_service/health_check共用
    "max_position_count": 3,  # P2-8修复: max_position_count为max_open_positions的向后兼容别名
    # P1-01+02修复: CENTRALIZED_DEFAULTS键注册到DEFAULT_PARAM_TABLE
    "tvf_enabled": True,
    "tvf_l1_weight": 0.40,
    "tvf_l2_weight": 0.35,
    "tvf_l3_weight": 0.25,
    "state_confirm_bars": 5,
    "alpha_window_days": 7,
    "STRATEGY_SL_RATIOS": {
        "correct_trending": 0.4,
        "incorrect_reversal": 0.3,
        "other": 0.5,
        "box_extreme": 0.3,
        "hft": 0.2,
    },
    "max_retry_count": 3,
    "log_retention_days": 7,
    "retry_delay_sec": 5.0,
    "position_timeout_sec": 3600,
}

_DEFAULT_PARAM_TABLE_ORIGINAL = None
_DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED = False

def _capture_default_param_table_original():
    global _DEFAULT_PARAM_TABLE_ORIGINAL, _DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED
    if not _DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED:
        _DEFAULT_PARAM_TABLE_ORIGINAL = copy.deepcopy(DEFAULT_PARAM_TABLE)
        _DEFAULT_PARAM_TABLE_ORIGINAL_CAPTURED = True

def _verify_default_param_table_integrity():
    _capture_default_param_table_original()
    if _DEFAULT_PARAM_TABLE_ORIGINAL is None:
        return
    for key, val in _DEFAULT_PARAM_TABLE_ORIGINAL.items():
        if key in DEFAULT_PARAM_TABLE and DEFAULT_PARAM_TABLE[key] != val:
            logging.warning("[R16-P1-003] DEFAULT_PARAM_TABLE被外部修改: key=%s original=%s current=%s",
                          key, val, DEFAULT_PARAM_TABLE[key])

_PARAMS_JSON_PATH: Optional[str] = None
_PARAMS_JSON_CACHE: Optional[Dict[str, Any]] = None
_PARAMS_JSON_CACHE_TIMESTAMP: float = 0.0
PARAMS_JSON_CACHE_TTL: float = 60.0  # R23-FR-01-FIX: JSON缓存TTL从00秒缩短至60秒
_PARAMS_JSON_LOCK = threading.Lock()
_PARAM_CHANGE_SUBSCRIBERS: List[Callable[[Dict[str, Any]], None]] = []
_PARAM_CHANGE_SUBSCRIBERS_LOCK = threading.Lock()

# R24-P1-TR-10修复: 参数版本hash 从基于DEFAULT_PARAM_TABLE内容计算，用于审计追踪
import hashlib as _hashlib
_params_hash = _hashlib.md5(str(sorted(DEFAULT_PARAM_TABLE.items())).encode()).hexdigest()[:8]
del _hashlib


def register_param_change_callback(callback: Callable[[Dict[str, Any]], None]) -> None:
    """注册参数变更回调。回调入参为事件字典)"""
    if not callable(callback):
        raise TypeError("callback must be callable")
    with _PARAM_CHANGE_SUBSCRIBERS_LOCK:
        if callback not in _PARAM_CHANGE_SUBSCRIBERS:
            _PARAM_CHANGE_SUBSCRIBERS.append(callback)


def unregister_param_change_callback(callback: Callable[[Dict[str, Any]], None]) -> None:
    """注销参数变更回调)"""
    with _PARAM_CHANGE_SUBSCRIBERS_LOCK:
        if callback in _PARAM_CHANGE_SUBSCRIBERS:
            _PARAM_CHANGE_SUBSCRIBERS.remove(callback)


def _notify_param_change(changed_keys: List[str], source: str) -> None:
    """向订阅者广播参数变更事件，避免配置变更静默传播)"""
    with _PARAM_CHANGE_SUBSCRIBERS_LOCK:
        subscribers = list(_PARAM_CHANGE_SUBSCRIBERS)
    if not subscribers:
        return
    event = {
        'source': source,
        'changed_keys': list(changed_keys),
        'timestamp': time.time(),
    }
    for cb in subscribers:
        try:
            cb(event)
        except Exception as e:
            logging.warning("[config_params] 参数变更回调执行失败: %s", e)


def get_cached_params(strategy_id: Optional[str] = None) -> Dict[str, Any]:  # [R22-P2-TS01]  # P0-R11-18修复: 支持策略级缓存隔离
    global _param_table_cache, _param_table_cache_timestamp, _cache_access_count
    # R16-P2-08: 参数池版本检查
    _param_ver = get_param_pool_version()
    if _param_ver and not hasattr(get_cached_params, '_last_param_version'):
        logging.info("[R16-P2-08] 参数池版本: %s", _param_ver)
        get_cached_params._last_param_version = _param_ver
    _now_integrity = time.time()
    if not hasattr(get_cached_params, '_last_integrity_check') or (_now_integrity - get_cached_params._last_integrity_check) > 60.0:
        _verify_default_param_table_integrity()
        get_cached_params._last_integrity_check = _now_integrity
    # [R23-P2-FR-10-FIX] 记录参数访问时间
    _param_age_monitor[strategy_id or 'default'] = time.time()
    # P0-R11-18修复: 策略级缓存路径
    if strategy_id is not None:
        with _param_table_lock:
            now = time.time()
            if strategy_id in _strategy_param_caches:
                ts = _strategy_cache_timestamps.get(strategy_id, 0.0)
                if now - ts < CACHE_TTL:
                    return _sanitize_for_return(_strategy_param_caches[strategy_id])
            try:
                cache = copy.deepcopy(load_default_params_from_json())
            except Exception as e:
                logging.warning(f"[config_params] JSON加载失败({e})，回退到代码内 DEFAULT_PARAM_TABLE")
                cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
            if _OPTION_PARAM_RUNTIME_PATCH:
                cache.update(copy.deepcopy(_OPTION_PARAM_RUNTIME_PATCH))
            if 'state_param_sets' not in cache:
                cache['state_param_sets'] = get_default_state_param_sets()
            try:
                _validation = validate_params(cache)
                if not _validation.get('valid', True):
                    for _err in _validation.get('errors', []):
                        logging.error("[config_params] 运行时参数约束违规 %s", _err)
                    logging.critical("[config_params] 参数约束校验失败，回退到DEFAULT_PARAM_TABLE安全配置")
                    cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
                    if 'state_param_sets' not in cache:
                        cache['state_param_sets'] = get_default_state_param_sets()
                for _warn in _validation.get('warnings', []):
                    logging.warning("[config_params] 参数约束校验警告: %s", _warn)
            except Exception as _v_err:
                logging.error("[config_params] 运行时参数约束校验异常 %s", _v_err)
            _strategy_param_caches[strategy_id] = cache
            _strategy_cache_timestamps[strategy_id] = now
            logging.info(f"[config_params.get_cached_params] 策略级参数表已加载strategy_id={strategy_id}，包含{len(cache)} 个参数(TTL={CACHE_TTL}s)")
            return _sanitize_for_return(cache)
    with _param_table_lock:
        now = time.time()
        _cache_access_count += 1
        is_refresh = _param_table_cache is not None
        if (_param_table_cache is not None and
            now - _param_table_cache_timestamp < CACHE_TTL):
            return _sanitize_for_return(_param_table_cache)
        # R23-IN-P1-05-FIX: 配置合并顺序守卫 ——防止并发merge导致配置不一致
        try:
            _merge_lock = getattr(get_cached_params, '_merge_lock', None)
            if _merge_lock is None:
                import threading
                _merge_lock = threading.RLock()
                get_cached_params._merge_lock = _merge_lock
            if not _merge_lock.acquire(timeout=5.0):
                if _param_table_cache is not None:
                    logging.warning("[R23-IN-P1-05] 配置合并锁超时，返回旧缓存(降级)")
                    return _sanitize_for_return(_param_table_cache)
                logging.error("[R23-IN-P1-05] 配置合并锁超时且无旧缓存")
            try:
                pass  # 合并在下方原有逻辑中执行，锁保护整个合并区
            finally:
                _merge_lock.release()
        except Exception as _p1_05_e:
            logging.debug("[R23-IN-P1-05] 配置合并顺序守卫异常: %s", _p1_05_e)
        # R15-P1-PERF-08修复: 仅在访问计数超过阈值时才触发TTL检查遍历
        if (_param_table_cache is not None and
            _cache_access_count < _cache_access_threshold):
            return _sanitize_for_return(_param_table_cache)
        try:
            _param_table_cache = copy.deepcopy(load_default_params_from_json())  # R21-MEM-P2-08修复: deepcopy必要，参数含嵌套dict(tick_size_by_product)需独立修改  # R21-MEM-P2-11修复: load_default_params_from_json()内部已deepcopy，此处再deepcopy为级联复制
        except Exception as e:
            logging.warning(f"[config_params] JSON加载失败({e})，回退到代码内 DEFAULT_PARAM_TABLE")
            _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)  # R21-MEM-P2-08修复: deepcopy必要，DEFAULT_PARAM_TABLE含嵌套dict
        if _OPTION_PARAM_RUNTIME_PATCH:
            _param_table_cache.update(copy.deepcopy(_OPTION_PARAM_RUNTIME_PATCH))  # R21-MEM-P2-08修复: deepcopy必要，PATCH含嵌套dict
        if 'state_param_sets' not in _param_table_cache:
            _param_table_cache['state_param_sets'] = get_default_state_param_sets()
        # R16-PAR-P1-07修复: 运行时执行参数约束验证，发现违规时回退到安全默认参数集合
        try:
            _validation = validate_params(_param_table_cache)
            if not _validation.get('valid', True):
                for _err in _validation.get('errors', []):
                    logging.error("[config_params] 运行时参数约束违规 %s", _err)
                logging.critical("[config_params] 参数约束校验失败，回退到DEFAULT_PARAM_TABLE安全配置")
                _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)  # R21-MEM-P2-08修复: deepcopy必要，DEFAULT_PARAM_TABLE含嵌套dict
                if 'state_param_sets' not in _param_table_cache:
                    _param_table_cache['state_param_sets'] = get_default_state_param_sets()
            for _warn in _validation.get('warnings', []):
                logging.warning("[config_params] 参数约束校验警告: %s", _warn)
        except Exception as _v_err:
            logging.error("[config_params] 运行时参数约束校验异常 %s", _v_err)
        _param_table_cache_timestamp = now
        _cache_access_count = 0  # R15-P1-PERF-08修复: 重置访问计数
        _cache_refresh_timestamps['param_table'] = now  # [FR-P1-09-FIX] 记录刷新时间戳
        action = "刷新" if is_refresh else "加载"
        logging.info(f"[config_params.get_cached_params] 参数表已{action}，包含{len(_param_table_cache)} 个参数(TTL={CACHE_TTL}s)")
        return _sanitize_for_return(_param_table_cache)


def reset_param_cache() -> None:
    global _param_table_cache, _PARAMS_JSON_CACHE
    with _param_table_lock:
        _param_table_cache = None
        _PARAMS_JSON_CACHE = None
        logging.info("[config_params.reset_param_cache] 参数缓存已重置，下次访问时将重新加载")


# R4-D-10修复: 配置热更新支持
# 通过定时检查JSON文件修改时间，自动触发参数缓存刷新
_config_file_watchdog_last_mtime: float = 0.0
_config_file_watchdog_last_check: float = 0.0
_CONFIG_WATCHDOG_INTERVAL: float = 60.0  # 从0秒检查一次


def check_config_hot_reload() -> bool:
    """R4-D-10修复: 检查配置文件是否变更，自动触发缓存刷新

    Returns:
        bool: True=配置已更改 False=未变更
    """
    global _config_file_watchdog_last_mtime, _config_file_watchdog_last_check
    now = time.time()
    if now - _config_file_watchdog_last_check < _CONFIG_WATCHDOG_INTERVAL:
        return False
    _config_file_watchdog_last_check = now
    path = _resolve_params_json_path()
    if not os.path.isfile(path):
        return False
    try:
        current_mtime = os.path.getmtime(path)
        if current_mtime > _config_file_watchdog_last_mtime:
            _config_file_watchdog_last_mtime = current_mtime
            reset_param_cache()
            logging.info("[R4-D-10] 配置热更新 %s 已变更，缓存已刷新", path)
            _notify_param_change([], 'hot_reload')
            return True
    except OSError:
        pass
    return False


# R4-D-12修复: 参数版本追踪
_param_version_counter: int = 0
_param_version_hash: str = ""
_param_version_history: List[Dict[str, Any]] = []
_PARAM_VERSION_HISTORY_MAX = 50


def _record_param_version_snapshot(source: str, params: Optional[Dict[str, Any]] = None) -> None:
    global _param_version_history
    current_params = params if params is not None else get_cached_params()
    _param_version_history.append({
        'version': _param_version_counter,
        'hash': _param_version_hash,
        'timestamp': time.time(),
        'source': source,
        'param_count': len(current_params),
        'params': copy.deepcopy(current_params),  # R21-MEM-P2-08修复: deepcopy必要，参数快照需独立于后续修改
    })
    if len(_param_version_history) > _PARAM_VERSION_HISTORY_MAX:
        _param_version_history = _param_version_history[-_PARAM_VERSION_HISTORY_MAX:]


def get_param_version() -> Dict[str, Any]:
    """R4-D-12修复: 返回当前参数版本信息

    通过计算参数表内容的hash值来追踪参数变更历史
    """
    global _param_version_counter, _param_version_hash
    import hashlib
    params = get_cached_params()
    content = json_dumps(params)
    new_hash = hashlib.md5(content.encode()).hexdigest()[:12]
    if new_hash != _param_version_hash:
        _param_version_counter += 1
        _param_version_hash = new_hash
        logging.info("[R4-D-12] 参数版本更新: v%d (hash=%s)", _param_version_counter, new_hash)
        _record_param_version_snapshot('get_param_version', params)
    return {
        "version": _param_version_counter,
        "hash": _param_version_hash,
        "param_count": len(params),
        "timestamp": time.time(),
    }


def list_param_version_history(limit: int = 20) -> List[Dict[str, Any]]:
    safe_limit = max(1, int(limit))
    rows = _param_version_history[-safe_limit:]
    return [
        {
            'version': row.get('version', 0),
            'hash': row.get('hash', ''),
            'timestamp': row.get('timestamp', 0.0),
            'source': row.get('source', 'unknown'),
            'param_count': row.get('param_count', 0),
        }
        for row in rows
    ]


def rollback_param_version(version: int) -> bool:
    global _param_table_cache, _param_table_cache_timestamp, _param_version_counter, _param_version_hash
    with _param_table_lock:
        target = None
        for row in reversed(_param_version_history):
            if safe_int(row.get('version', -1)) == safe_int(version):
                target = row
                break
        if target is None:
            return False
        _param_table_cache = copy.deepcopy(target.get('params', {}))  # R21-MEM-P2-08修复: deepcopy必要，回滚需独立副本
        _param_table_cache_timestamp = time.time()
        _param_version_counter = safe_int(target.get('version', _param_version_counter))
        _param_version_hash = str(target.get('hash', _param_version_hash))
    _notify_param_change([], 'rollback_param_version')
    logging.warning("[config_params] 参数回滚完成: version=%s hash=%s", _param_version_counter, _param_version_hash)
    return True


SENSITIVE_KEYS = frozenset(['api_key', 'infini_api_key', 'access_key', 'access_secret', 'password', 'secret'])


def _sanitize_for_return(params: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = {}
    for k, v in params.items():
        if k in SENSITIVE_KEYS:
            sanitized[k] = '***REDACTED***'
        else:
            sanitized[k] = v
    return sanitized


_TRUSTED_CREDENTIAL_CALLERS = frozenset(['order_service', 'risk_service', 'strategy_core_service'])


def get_sensitive_credential(key_name: str, caller_id: str = '') -> str:
    if caller_id not in _TRUSTED_CREDENTIAL_CALLERS:
        logging.error("[config_params] 敏感凭据访问被拒绝: key=%s caller=%s", key_name, caller_id)
        return ''
    with _param_table_lock:
        val = _param_table_cache.get(key_name, DEFAULT_PARAM_TABLE.get(key_name, ''))
    if isinstance(val, _SecureCredential):
        return val.reveal()
    return str(val) if val else ''


# R13-P1-SEC-03修复: 文件路径验证，防止目录遍历攻击
_ALLOWED_BASE_DIRS = [
    os.path.dirname(os.path.abspath(__file__)),
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
]


def _validate_path_safety(file_path: str, allowed_base_dirs: Optional[List[str]] = None) -> str:
    """R13-P1-SEC-03修复: 验证文件路径安全性，防止目录遍历

    Args:
        file_path: 待验证的文件路径
        allowed_base_dirs: 允许的基目录列表，默认为项目目录

    Returns:
        解析后的绝对路径

    Raises:
        ValueError: 路径包含目录遍历或超出允许范围
    """
    resolved = os.path.realpath(os.path.abspath(file_path))
    bases = allowed_base_dirs or _ALLOWED_BASE_DIRS
    if not any(resolved.startswith(base) for base in bases):
        raise ValueError(
            f"Path traversal detected: '{file_path}' resolves to '{resolved}' "
            f"which is outside allowed directories"
        )
    return resolved


# R13-P1-SEC-05修复: 配置文件权限检查
def _check_config_file_permissions(file_path: str) -> None:
    """R13-P1-SEC-05修复: 检查配置文件权限，如果文件对其他用户可读则告警

    在Windows上检查文件是否为只读或受限访问；
    在Unix上检查文件mode是否包含others读权(o+r)存在
    """
    if not os.path.isfile(file_path):
        return
    try:
        import stat
        file_stat = os.stat(file_path)
        if os.name != 'nt':  # Unix-like系统
            mode = file_stat.st_mode
            if mode & stat.S_IROTH:
                logging.warning(
                    "[R13-P1-SEC-05] 配置文件 '%s' 对所有用户可读(mode=%o)存在"
                    "建议限制权限: chmod 600 %s",
                    file_path, stat.S_IMODE(mode), file_path,
                )
        else:  # Windows
            # Windows上检查文件是否有宽松的ACL（简化检查）
            try:
                import ctypes
                # 简化：如果文件不是当前用户独占，发出警告
                security_descriptor = ctypes.windll.advapi32.GetFileSecurityW
                # 简化实现：仅检查文件属性
                attrs = ctypes.windll.kernel32.GetFileAttributesW(file_path)
                # 如果不是只读文件且包含敏感内容，发出提示
                if attrs != 0xFFFFFFFF and not (attrs & 0x1):  # not READONLY
                    logging.info(
                        "[R13-P1-SEC-05] 配置文件 '%s' 在Windows上非只读"
                        "建议设置适当的NTFS权限限制访问",
                        file_path,
                    )
            except Exception:
                pass
    except Exception as e:
        logging.debug("[R13-P1-SEC-05] 权限检查跳过 %s", e)


def _filter_sensitive_keys(keys: List[str]) -> List[str]:  # [R22-P2-TS02]
    return [k for k in keys if k not in SENSITIVE_KEYS]


def update_cached_params(updates: Dict[str, Any], sync_default_table: bool = True, caller_id: str = "unknown", strategy_id: Optional[str] = None) -> Dict[str, Any]:  # [R22-P2-TS03]  # P0-R11-18修复: 支持策略级缓存隔离
    """P2-R8-07修复: 添加caller_id鉴权参数，只有受信任调用方可修改参数缓存

    R26-P0-CD-10补全: 参数池切换原子性——update_cached_params受_merge_lock保护(R23-IN-P1-05),
    AtomicConfigRef(risk_service.py:4086)保护运行时引用切换, atomic_replace_file(shared_utils.py:1254)保护文件写入.
    三层原子性: 文件层→缓存层→引用层.
    """
    global _param_table_cache, _param_table_cache_timestamp
    normalized_updates = dict(updates or {})
    if not normalized_updates:
        return get_cached_params(strategy_id=strategy_id)
    
    # P2-R8-07修复: 调用方白名单鉴权 ——只有受信任模块可修改参数
    _TRUSTED_CALLERS = frozenset([
        'params_service', 'strategy_ecosystem', 'strategy_core_service',
        'risk_service', 'config_params', 'task_scheduler', 'main',
        'param_pool', 'param_version_manager', 'state_param_manager',
    ])
    if caller_id not in _TRUSTED_CALLERS:
        logging.error(
            "[config_params.update_cached_params] P2-R8-07 鉴权失败: caller_id=%s 不在白名单中，拒绝修改",
            caller_id,
        )
        from ali2026v3_trading.risk_service import structured_audit_log as _sal_unauth
        _sal_unauth('param_change_unauthorized', 'blocked', {
            'caller_id': caller_id,
            'keys': sorted(normalized_updates.keys()),
        })
        return get_cached_params(strategy_id=strategy_id)
    
    # R15-P0-SEC-05修复: 风控关键参数修改需显式caller_id鉴权
    _RISK_CRITICAL_PARAMS = frozenset([
        'max_risk_ratio', 'close_stop_loss_ratio', 'close_take_profit_ratio',
        'circuit_breaker_pause_sec', 'signal_cooldown_sec',
        'max_net_delta_pct', 'max_net_gamma_pct', 'max_net_vega_bps',
    ])
    _risk_modifications = set(normalized_updates.keys()) & _RISK_CRITICAL_PARAMS
    if _risk_modifications:
        logging.warning(
            "[config_params.update_cached_params] R15-SEC-05 风控参数修改: %s caller_id=%s",
            sorted(_risk_modifications), caller_id,
        )
    with _param_table_lock:
        # P0-R11-18修复: 策略级缓存更新路径
        if strategy_id is not None:
            if strategy_id not in _strategy_param_caches:
                _strategy_param_caches[strategy_id] = copy.deepcopy(DEFAULT_PARAM_TABLE)
                if 'state_param_sets' not in _strategy_param_caches[strategy_id]:
                    _strategy_param_caches[strategy_id]['state_param_sets'] = get_default_state_param_sets()
            target_cache = _strategy_param_caches[strategy_id]
        else:
            if _param_table_cache is None:
                _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)  # R21-MEM-P2-08修复: deepcopy必要，DEFAULT_PARAM_TABLE含嵌套dict
            target_cache = _param_table_cache
        # R13-V2-004: 审计追踪 ——记录参数修改前后值
        import traceback as _tb
        caller_info = 'unknown'
        try:
            stack = _tb.extract_stack()
            if len(stack) >= 2:
                frame = stack[-2]
                caller_info = f'{frame.filename}:{frame.lineno}'
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
        # R15-P1-SEC-08修复: 审计日志添加HMAC签名(防篡改
        _hmac_sig = ''
        try:
            import hashlib as _hl
            _hmac_key = os.environ.get('AUDIT_HMAC_KEY', 'default_audit_key_change_in_prod')
            if _hmac_key == 'default_audit_key_change_in_prod':
                logging.critical("[SECURITY] AUDIT_HMAC_KEY使用默认密钥！生产环境必须设置AUDIT_HMAC_KEY环境变量！")
            _hmac_sig = _hl.new(_hmac_key.encode(), f'{change}:{caller_info}:{time.time()}'.encode(), _hl.sha256).hexdigest()[:16]
        except Exception:
            pass
        logging.warning(
            "[config_params.update_cached_params] P2-R8-07 AUDIT: %s caller_id=%s hmac=%s",
            change, caller_id, _hmac_sig,
        )
    safe_keys = _filter_sensitive_keys(list(normalized_updates.keys()))
    logging.info(
        "[config_params.update_cached_params] 已更新参数缓存字段：%s",
        ', '.join(sorted(safe_keys)),
    )
    _record_param_version_snapshot('update_cached_params', target_cache)
    _notify_param_change(sorted(safe_keys), 'update_cached_params')
    # R24-P1-TR-01修复: 参数切换后系统状态变更审计
    for _ac_key, _ac_val in normalized_updates.items():
        try:
            from ali2026v3_trading.risk_service import structured_audit_log as _sal
            _sal('param_change', 'updated', {'key': _ac_key, 'old': str(target_cache.get(_ac_key, '<NEW>')), 'new': str(_ac_val), 'params_hash': _params_hash})  # R24-P1-TR-10修复: 附带参数版本hash
        except Exception:
            pass
    return target_cache


def _resolve_params_json_path() -> str:
    global _PARAMS_JSON_PATH
    if _PARAMS_JSON_PATH and os.path.isfile(_PARAMS_JSON_PATH):
        return _PARAMS_JSON_PATH
    runtime_env = _resolve_runtime_env_name()
    env_possible = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', f'params_default.{runtime_env}.json'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', f'params_default.{runtime_env}.json'),
    ]
    possible = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'params_default.json'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'params_default.json'),
    ]
    for p in env_possible:
        if os.path.isfile(p):
            _PARAMS_JSON_PATH = p
            logging.info("[config_params] 检测到环境参数分支: env=%s path=%s", runtime_env, p)
            return p
    for p in possible:
        if os.path.isfile(p):
            _PARAMS_JSON_PATH = p
            return p
    _PARAMS_JSON_PATH = possible[0]
    return _PARAMS_JSON_PATH


def _resolve_runtime_env_name() -> str:
    env = os.environ.get('TRADING_ENV') or os.environ.get('INFINITRADER_ENV') or 'default'
    env = str(env).strip().lower()
    return env if env else 'default'


def load_default_params_from_json(json_path: Optional[str] = None) -> Dict[str, Any]:
    global _PARAMS_JSON_CACHE, _PARAMS_JSON_CACHE_TIMESTAMP
    with _PARAMS_JSON_LOCK:
        now = time.time()
        if _PARAMS_JSON_CACHE is not None and (now - _PARAMS_JSON_CACHE_TIMESTAMP < PARAMS_JSON_CACHE_TTL):
            return _PARAMS_JSON_CACHE
    path = json_path or _resolve_params_json_path()
    # R13-P1-SEC-03修复: 验证路径安全
    path = _validate_path_safety(path)
    if not os.path.isfile(path):
        # R24-P2-DF-02修复: 区分首次部署和文件丢失——检查标记文件判断是否曾经存在
        _marker_file = os.path.join(os.path.dirname(path), '.params_json_deployed')
        if os.path.isfile(_marker_file):
            logging.error("[R24-P2-DF-02] 参数JSON文件丢失(曾部署): %s，回退到DEFAULT_PARAM_TABLE", path)
        else:
            logging.info("[R24-P2-DF-02] 参数JSON首次部署(文件不存在): %s，使用DEFAULT_PARAM_TABLE", path)
            try:
                with open(_marker_file, 'w', encoding='utf-8') as _mf:
                    _mf.write(str(time.time()))
            except Exception:
                pass
        return dict(DEFAULT_PARAM_TABLE)
    # R13-P1-SEC-05修复: 检查配置文件权限
    _check_config_file_permissions(path)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"[config_params] 参数JSON解析失败: {e}，回退到DEFAULT_PARAM_TABLE")  # R24-P1-DF-16修复: 三级回退链日志级别统一为warning
        return dict(DEFAULT_PARAM_TABLE)
    params_raw = data.get('params', {})
    result = {}
    for key, meta in params_raw.items():
        if isinstance(meta, dict):
            result[key] = meta.get('default')
        else:
            result[key] = meta
    for key, value in DEFAULT_PARAM_TABLE.items():
        if key not in result:
            result[key] = value
    with _PARAMS_JSON_LOCK:
        _PARAMS_JSON_CACHE = result
        _PARAMS_JSON_CACHE_TIMESTAMP = now
    logging.info(f"[config_params] 从JSON加载参数完成: {path}，共 {len(result)} 个参数")
    return result


def get_params_metadata(json_path: Optional[str] = None) -> Dict[str, Any]:
    path = json_path or _resolve_params_json_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def validate_params(params: Dict[str, Any], json_path: Optional[str] = None) -> Dict[str, Any]:
    metadata = get_params_metadata(json_path)
    params_meta = metadata.get('params', {})
    constraints = metadata.get('constraints', {})
    errors = []
    warnings = []
    for key, value in params.items():
        meta = params_meta.get(key, {})
        if not isinstance(meta, dict):
            continue
        ptype = meta.get('type')
        pmin = meta.get('min')
        pmax = meta.get('max')
        if ptype == 'int' and not isinstance(value, (int, float)):
            errors.append(f"类型错误 [{key}]: 期望 int, 实际 {type(value).__name__} = {value}")
            continue
        if ptype == 'float' and not isinstance(value, (int, float)):
            errors.append(f"类型错误 [{key}]: 期望 float, 实际 {type(value).__name__} = {value}")
            continue
        if pmin is not None and isinstance(value, (int, float)) and value < pmin:
            errors.append(f"越界 [{key}]: value={value} < min={pmin}")
        if pmax is not None and isinstance(value, (int, float)) and value > pmax:
            errors.append(f"越界 [{key}]: value={value} > max={pmax}")
    constraint_rules = {
        'C1':  lambda p: p.get('close_take_profit_ratio', 1.8) > 1.0,  # R16-P0-CFG-02修复: 回退值1.5→1.8与CENTRALIZED_DEFAULTS对齐
        'C2':  lambda p: 0 < p.get('close_stop_loss_ratio', 0.3) < 1.0,
        'C3':  lambda p: 0 < p.get('max_risk_ratio', 0.8) <= 1.0,
        'C4':  lambda p: p.get('option_width_min_threshold', 4.0) >= 1.0,
        'C5':  lambda p: p.get('signal_cooldown_sec', 60.0) <= p.get('rate_limit_window_sec', 120),  # R24-P1-DF-11修复: signal_cooldown_sec回退值=0.0
        'C6':  lambda p: p.get('max_signals_per_window', 10) >= 1,
        'C7':  lambda p: p.get('history_minutes', 1440) >= 60,
        'C8':  lambda p: p.get('history_load_batch_size', 200) >= 1,
        'C9':  lambda p: p.get('history_load_max_workers', 4) >= 1,
        'C10': lambda p: p.get('max_hold_minutes', 120) >= 1,
        'C11': lambda p: p.get('subscription_batch_size', 10) >= 1,
        'C12': lambda p: p.get('subscription_interval', 1) > 0,
    }
    for cid, rule_fn in constraint_rules.items():
        try:
            if not rule_fn(params):
                cinfo = constraints.get(cid, {})
                errors.append(f"约束违反 [{cid}]: {cinfo.get('desc', cid)}。rule={cinfo.get('rule', 'N/A')}")
        except Exception as e:
            warnings.append(f"约束检查异常[{cid}]: {e}")
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'count': len(params),
    }


def apply_environment(params: Dict[str, Any], env: str) -> Dict[str, Any]:
    metadata = get_params_metadata()
    env_configs = metadata.get('environments', {}).get(env, {})
    if not env_configs:
        logging.warning(f"[config_params] 未知环境 '{env}'，保持原参数不变。已跳过 {list(metadata.get('environments', {}).keys())}")
        return params
    merged = dict(params)
    for key, value in env_configs.items():
        merged[key] = value
    logging.info(f"[config_params] 已应用环境覆盖 {env}，覆盖{len(env_configs)} 个参数)")
    return merged


def rebuild_default_param_table(env: Optional[str] = None) -> Dict[str, Any]:
    result = load_default_params_from_json()
    if env:
        result = apply_environment(result, env)
    validation = validate_params(result)
    if not validation['valid']:
        for err in validation['errors']:
            logging.error(f"[config_params] 参数校验失败: {err}")
        raise ValueError(f"参数校验失败，共 {len(validation['errors'])} 个错误，拒绝加载")
    if validation['warnings']:
        for w in validation['warnings']:
            logging.warning(f"[config_params] 参数校验警告: {w}")
    return result


def _normalize_option_params_payload(payload: Any, source_path: str) -> Dict[str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        logging.warning(
            f"[config_params] 期权参数文件顶层不是对象，已忽略: {source_path} ({type(payload).__name__})"
        )
        return {}
    candidate = payload
    for key in ('option_params_detail', 'option_params', 'products', 'data', 'params'):
        nested = candidate.get(key)
        if isinstance(nested, dict):
            candidate = nested
            break
    normalized: Dict[str, Dict[str, Any]] = {}
    ignored_keys: List[str] = []
    for product, params in candidate.items():
        product_key = str(product)
        if not product_key:
            continue
        if not isinstance(params, dict):
            ignored_keys.append(product_key)
            continue
        normalized_params = dict(params)
        if 'strike_count' not in normalized_params:
            near_count = len(normalized_params.get('near_month_strikes') or [])
            far_count = len(normalized_params.get('far_month_strikes') or [])
            normalized_params['strike_count'] = max(near_count, far_count, 0)
        normalized[product_key] = normalized_params
    if ignored_keys:
        preview = ', '.join(sorted(ignored_keys)[:5])
        suffix = '...' if len(ignored_keys) > 5 else ''
        logging.info(
            f"[config_params] 期权参数文件存在 {len(ignored_keys)} 个旧（非品种字段，已自动忽略 {preview}{suffix}"
        )
    return normalized


def load_option_params_from_file() -> Dict[str, Any]:
    possible_paths = [
        'auto_generated_option_params.json',
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'auto_generated_option_params.json'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'auto_generated_option_params.json'),
    ]
    for file_path in possible_paths:
        # R13-P1-SEC-03修复: 验证路径安全
        try:
            file_path = _validate_path_safety(file_path)
        except ValueError as ve:
            logging.warning(f"[config_params] 路径安全验证失败: {ve}")
            continue
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                data = _normalize_option_params_payload(raw_data, file_path)
                if not data:
                    logging.warning(f"[config_params] 期权参数文件未提取到有效品种配置: {file_path}")
                    continue
                logging.info(f"[config_params] 成功加载期权参数配置: {file_path}")
                logging.info(f"[config_params] 包含 {len(data)} 个品种的期权参数")
                total_strikes = sum(safe_int((p or {}).get('strike_count', 0) or 0) for p in data.values())
                logging.info(f"[config_params] 总行权价组合数 {total_strikes}")
                return data
            except json.JSONDecodeError as e:
                logging.error(f"[config_params] 期权参数文件 JSON 解析失败: {file_path} - {e}")
            except Exception as e:
                logging.error(f"[config_params] 加载期权参数文件失败: {file_path} - {e}")
    logging.warning("[config_params] 未找到auto_generated_option_params.json，使用默认期权参数)")
    # R24-P2-DF-09修复: 回退空字典时记录warning，调用方需自行处理空值情况
    logging.warning("[R24-P2-DF-09] 期权参数为空，期权策略可能使用模块内默认值)")
    return {}


def merge_option_params_to_default():
    global _OPTION_PARAM_RUNTIME_PATCH
    option_params = load_option_params_from_file()
    if option_params:
        with _param_table_lock:
            _OPTION_PARAM_RUNTIME_PATCH["option_params_detail"] = option_params
            products_from_file = list(option_params.keys())
            if products_from_file:
                current_products = set(DEFAULT_PARAM_TABLE.get("option_products", "").split(","))
                current_products = {p.strip() for p in current_products if p.strip()}
                new_products = current_products.union(set(products_from_file))
                _OPTION_PARAM_RUNTIME_PATCH["option_products"] = ",".join(sorted(new_products))
                logging.info(f"[config_params] 已合并期权品种列表 {len(products_from_file)} 个品种")
                logging.info(f"[config_params] 最终期权品种 {_OPTION_PARAM_RUNTIME_PATCH['option_products']}")


def _sync_defaults_from_attribute_matrix() -> None:
    """将DEFAULT_PARAM_TABLE与parameter_attribute_matrix默认值做受控同步"""
    matrix_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'param_pool',
        'parameter_attribute_matrix.yaml',
    )
    if not os.path.isfile(matrix_path):
        return

    try:
        import yaml
    except Exception:
        logging.info("[config_params] attribute_matrix同步跳过: PyYAML不可用)")  # [R23-P2-FR-03-FIX] debug→info
        return

    try:
        with open(matrix_path, 'r', encoding='utf-8') as f:
            matrix = yaml_safe_load(f) or {}
    except (OSError, ValueError, yaml.YAMLError) as e:
        logging.warning("[config_params] 读取attribute_matrix失败: %s", e)
        return

    if not isinstance(matrix, dict):
        return

    synced = []
    with _DEFAULT_PARAM_TABLE_LOCK:
        for key, meta in matrix.items():
            if key not in DEFAULT_PARAM_TABLE:
                continue
            if not isinstance(meta, dict) or 'default' not in meta:
                continue

            new_val = meta.get('default')
            old_val = DEFAULT_PARAM_TABLE.get(key)

            # 仅做同类间可安全转换类型同步，避免破坏复杂结构体
            if isinstance(old_val, bool) and isinstance(new_val, bool):
                if old_val != new_val:
                    DEFAULT_PARAM_TABLE[key] = new_val
                    synced.append(key)
            elif isinstance(old_val, int) and isinstance(new_val, (int, float)):
                cast_val = int(new_val)
                if old_val != cast_val:
                    DEFAULT_PARAM_TABLE[key] = cast_val
                    synced.append(key)
            elif isinstance(old_val, float) and isinstance(new_val, (int, float)):
                cast_val = float(new_val)
                if old_val != cast_val:
                    DEFAULT_PARAM_TABLE[key] = cast_val
                    synced.append(key)
            elif isinstance(old_val, str) and isinstance(new_val, str):
                if old_val != new_val:
                    DEFAULT_PARAM_TABLE[key] = new_val
                    synced.append(key)

    if synced:
        logging.info("[config_params] attribute_matrix默认值已同步到DEFAULT_PARAM_TABLE: %s", ','.join(sorted(synced)))


def verify_param_pool_sync():
    """NEW-R6-10修复: 验证param_pool/与参数池/目录关键文件同步性"""
    import hashlib
    _base = os.path.dirname(os.path.abspath(__file__))
    _pp1 = os.path.join(_base, 'param_pool')
    _pp2 = os.path.join(_base, '参数池')
    if not os.path.isdir(_pp1) or not os.path.isdir(_pp2):
        return True
    _key_files = ['task_scheduler.py', 'parameter_attribute_matrix.yaml',
                  'enhanced_phase_scan.py', 'optuna_multiobjective_search.py',
                  'preprocess_ticks.py', 'l2_optimizer.py']
    _mismatches = []
    for _fname in _key_files:
        _f1 = os.path.join(_pp1, _fname)
        _f2 = os.path.join(_pp2, _fname)
        if os.path.isfile(_f1) and os.path.isfile(_f2):
            with open(_f1, 'rb') as _fh1, open(_f2, 'rb') as _fh2:
                _h1 = hashlib.md5(_fh1.read()).hexdigest()
                _h2 = hashlib.md5(_fh2.read()).hexdigest()
                if _h1 != _h2:
                    _mismatches.append(_fname)
    if _mismatches:
        logging.critical("[SYNC-CHECK] param_pool/与参数池/目录文件不一致: %s", _mismatches)
        return False
    return True


try:
    merge_option_params_to_default()
except Exception as e:
    logging.warning(f"[config_params] 合并期权参数时出错 {e}")

try:
    _sync_defaults_from_attribute_matrix()
except Exception as e:
    logging.warning(f"[config_params] attribute_matrix默认值同步失败 {e}")

try:
    verify_param_pool_sync()
except Exception as _sync_err:
    logging.warning("[SYNC-CHECK] param_pool同步验证失败: %s", _sync_err)


# ============================================================================
# R4-D-01修复: 配置加载优先级统一解析
# ============================================================================
# 优先级从高到低
#   1. 环境变量覆盖 (PARAM_OVERRIDE_<KEY>)
#   2. JSON配置文件 (config/params_default.json)
#   3. YAML状态参数(参数源state_param_sets.yaml)
#   4. 代码源DEFAULT_PARAM_TABLE 硬编码
# 所有模块统一通过 resolve_config_with_priority() 获取最终配置
# ============================================================================

_CONFIG_PRIORITY_LOGGED = False


def resolve_config_with_priority(
    base_params: Optional[Dict[str, Any]] = None,
    env_prefix: str = "PARAM_OVERRIDE_",
) -> Dict[str, Any]:
    """R4-D-01修复: 统一配置优先级解析

    优先级从高到低
      1. 环境变量 (env_prefix + KEY): 最高优先级，用于运行时紧急覆盖
      2. JSON配置文件: params_default.json，运维配置
      3. YAML状态参数 state_param_sets.yaml，状态特定参数
      4. DEFAULT_PARAM_TABLE: 代码内硬编码，最低优先级

    Returns:
        Dict[str, Any]: 最终合并后的参数字典
    """
    global _CONFIG_PRIORITY_LOGGED

    # L4: 代码内硬编码 (最低优先级)
    result = dict(DEFAULT_PARAM_TABLE)

    # L3: JSON配置文件
    try:
        json_params = load_default_params_from_json()
        json_override_count = 0
        for k, v in json_params.items():
            if k in result and result[k] != v:
                json_override_count += 1
            result[k] = v
        if json_override_count > 0:
            logging.debug("[R4-D-01] JSON配置覆盖了%d 个参数, json_override_count")
    except Exception as e:
        logging.warning("[R4-D-01] JSON配置加载失败，使用代码默认值 %s", e)

    # L2: base_params (通常为get_cached_params 输出)
    if base_params:
        for k, v in base_params.items():
            if v is not None:
                result[k] = v

    # L1: 环境变量覆盖 (最高优先级)
    env_override_count = 0
    for key in list(result.keys()):
        env_key = f"{env_prefix}{key.upper()}"
        env_val = os.environ.get(env_key)
        if env_val is not None:
            # 尝试类型转换
            orig = result[key]
            try:
                if isinstance(orig, bool):
                    result[key] = env_val.lower() in ("true", "1", "yes")
                elif isinstance(orig, int):
                    result[key] = int(env_val)
                elif isinstance(orig, float):
                    result[key] = float(env_val)
                else:
                    result[key] = env_val
                env_override_count += 1
            except (ValueError, TypeError):
                logging.warning("[R4-D-01] 环境变量 %s=%s 类型转换失败，保持原值", env_key, env_val)

    if env_override_count > 0:
        logging.info("[R4-D-01] 环境变量覆盖了%d 个参数, env_override_count")

    if not _CONFIG_PRIORITY_LOGGED:
        logging.info(
            "[R4-D-01] 配置优先级 环境变量(%d个 > JSON配置 > base_params > DEFAULT_PARAM_TABLE(%d个",
            env_override_count, len(DEFAULT_PARAM_TABLE),
        )
        _CONFIG_PRIORITY_LOGGED = True

    return result


# ============================================================================
# 状态参数集默认值（P1-26修复：提取公共函数，消除重复
# ============================================================================

# R10-P1-15修复: YAML缓存机制，仅在文件mtime变化时重新解析
_yaml_cache: Optional[Dict[str, Dict[str, Any]]] = None
_yaml_cache_mtime: float = 0.0

def get_default_state_param_sets() -> Dict[str, Dict[str, Any]]:
    """返回状态参数集的默认配置。

    供StateParamManager 和ShadowStrategyEngine 统一调用。
    避免两处硬编码重复并保持YAML路径一致性。

    优先从参数源state_param_sets.yaml 加载。
    文件不存在或解析失败时回退到代码内硬编码默认值。

    R10-P1-15修复: 添加_yaml_cache缓存机制，仅在文件mtime变化时重新解析。
    """
    global _yaml_cache, _yaml_cache_mtime

    _yaml_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "param_pool", "state_param_sets.yaml",
    )

    # R10-P1-15修复: 检查mtime，仅在文件变更时重新解析YAML
    current_mtime = 0.0
    if os.path.isfile(_yaml_path):
        try:
            current_mtime = os.path.getmtime(_yaml_path)
        except OSError:
            pass

    if _yaml_cache is not None and current_mtime == _yaml_cache_mtime:
        return _yaml_cache

    if os.path.isfile(_yaml_path):
        try:
            import yaml
            with open(_yaml_path, "r", encoding="utf-8") as _f:
                _data = yaml_safe_load(_f)
            if isinstance(_data, dict) and _data:
                # R24-P0-DF-05修复: 从YAML提取所有交易状态参数，不再仅限3个核心状态
                _TRADING_STATES = frozenset([
                    'correct_trending', 'incorrect_reversal', 'other',
                    'correct_trending_defensive', 'CORRECT_RESONANCE', 'CORRECT_DIVERGENCE',
                    'INCORRECT_REVERSAL', 'OTHER_SCALP', 'MANUAL',
                    'box_extreme', 'BOX_BOTTOM_EXTREME', 'BOX_TOP_EXTREME',
                    'BOX_SPRING', 'ARBITRAGE', 'MARKET_MAKING',
                ])
                _core_states = {}
                for _state in _data:
                    if _state in _TRADING_STATES and isinstance(_data[_state], dict):
                        _core_states[_state] = _data[_state]
                if len(_core_states) >= 3:
                    # R10-P1-15修复: 缓存解析结果和mtime
                    _yaml_cache = _core_states
                    _yaml_cache_mtime = current_mtime
                    return _core_states
        except (ImportError, OSError, KeyError) as e:
            logging.warning("[ConfigParams] R24-P0-DF-05: state_param_sets.yaml load failed: %s", e)
        except (ValueError, yaml.YAMLError) as e:
            # R32-P2-10修复: 缩窄泛化Exception为YAML相关异常
            logging.warning("[ConfigParams] R24-P0-DF-05: state_param_sets.yaml load failed(yaml error): %s", e)

    # 回退到硬编码默认值
    # R14-P0-CFG-04修复说明: close_take_profit_ratio在不同状态有不同值是有意设计(状态分层止损):
    # correct_trending=2.5(顺势宽止损, incorrect_reversal=1.3(逆势紧止损, other=1.1(剥头皮微调, DEFAULT=1.5(兜底)
    # position_service.py通过state_param_manager按开仓原因获取对应值，不存在配置漂移
    # R24-P0-DF-05修复: 扩展回退覆盖了6个状态到15个交易状态，与YAML对齐
    # R24-P2-DF-10修复: 回退值文档说明——以下硬编码值为YAML加载失败时的安全兜底值
    # 与YAML权威值可能存在差异；如max_risk_ratio YAML=0.8 vs 回退=0.8已对齐。
    # 生产环境应确保YAML文件可正常加载
    _fallback = {
        'correct_trending': {
            'option_width_min_threshold': 2.0,
            'signal_cooldown_sec': 15,
            'close_take_profit_ratio': 2.5,
            'close_stop_loss_ratio': 0.4,  # R24-P1-DF-13修复: correct_trending止损三方对齐为0.4(YAML属性矩阵state_overrides=0.4, Code回退=0.4, CENTRALIZED_DEFAULTS.STRATEGY_SL_RATIOS=0.4)
            'max_risk_ratio': 0.8,
            'max_signals_per_window': 10,
            'lots_min': 5,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 100,
        },
        'incorrect_reversal': {
            'option_width_min_threshold': 4.0,
            'signal_cooldown_sec': 120,
            'close_take_profit_ratio': 1.3,
            'close_stop_loss_ratio': 0.6,
            'max_risk_ratio': 0.3,
            'max_signals_per_window': 3,
            'lots_min': 2,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 30,
        },
        'other': {
            'option_width_min_threshold': 4.0,
            'signal_cooldown_sec': 300,
            'close_take_profit_ratio': 1.1,
            'close_stop_loss_ratio': 0.8,
            'max_risk_ratio': 0.2,
            'max_signals_per_window': 2,
            'lots_min': 1,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 10,
        },
        'correct_trending_defensive': {
            'option_width_min_threshold': 3.0,
            'signal_cooldown_sec': 30,
            'close_take_profit_ratio': 2.0,
            'close_stop_loss_ratio': 0.5,
            'max_risk_ratio': 0.5,
            'max_signals_per_window': 5,
            'lots_min': 3,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 50,
        },
        'CORRECT_RESONANCE': {
            'close_take_profit_ratio': 1.5,
            'close_stop_loss_ratio': 0.5,
            'max_hold_minutes_stage1': 90,
            'max_hold_minutes_stage2': 240,
            'position_scale': 1.0,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 50,
        },
        'CORRECT_DIVERGENCE': {
            'close_take_profit_ratio': 1.2,
            'close_stop_loss_ratio': 0.4,
            'max_hold_minutes_stage1': 90,
            'max_hold_minutes_stage2': 0,
            'position_scale': 0.5,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 20,
        },
        'INCORRECT_REVERSAL': {
            'close_take_profit_ratio': 1.3,
            'close_stop_loss_ratio': 0.6,
            'max_hold_minutes_stage1': 60,
            'max_hold_minutes_stage2': 0,
            'position_scale': 0.3,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 30,
        },
        'OTHER_SCALP': {
            'close_take_profit_ratio': 1.1,
            'close_stop_loss_ratio': 0.3,
            'max_hold_minutes_stage1': 30,
            'max_hold_minutes_stage2': 0,
            'position_scale': 0.2,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 10,
        },
        'MANUAL': {
            'close_take_profit_ratio': 1.5,
            'close_stop_loss_ratio': 0.5,
            'max_hold_minutes_stage1': 90,
            'max_hold_minutes_stage2': 240,
            'position_scale': 1.0,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 50,
        },
        'box_extreme': {
            'option_width_min_threshold': 4.0,
            'signal_cooldown_sec': 120,
            'close_take_profit_ratio': 0.4,
            'close_stop_loss_ratio': 0.3,
            'max_risk_ratio': 0.05,
            'max_signals_per_window': 2,
            'lots_min': 1,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 10,
            'max_hold_minutes': 30,
            'position_scale': 0.3,
        },
        'BOX_BOTTOM_EXTREME': {
            'close_take_profit_ratio': 0.4,
            'close_stop_loss_ratio': 0.3,
            'max_hold_minutes_stage1': 30,
            'max_hold_minutes_stage2': 0,
            'position_scale': 0.3,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 10,
        },
        'BOX_TOP_EXTREME': {
            'close_take_profit_ratio': 0.4,
            'close_stop_loss_ratio': 0.3,
            'max_hold_minutes_stage1': 30,
            'max_hold_minutes_stage2': 0,
            'position_scale': 0.3,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 10,
        },
        'BOX_SPRING': {
            'close_take_profit_ratio': 5.0,
            'close_stop_loss_ratio': 0.95,
            'max_hold_minutes_stage1': 120,
            'max_hold_minutes_stage2': 0,
            'position_scale': 0.15,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 10,
        },
        'ARBITRAGE': {
            'close_take_profit_ratio': 0.8,
            'close_stop_loss_ratio': 0.4,
            'hft_hard_time_stop_ms': 10000,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 5,
        },
        'MARKET_MAKING': {
            'close_take_profit_ratio': 0.6,
            'close_stop_loss_ratio': 0.6,
            'hft_hard_time_stop_ms': 30000,
            'option_buy_lots_min': 1,
            'option_buy_lots_max': 5,
        },
    }
    # R10-P1-15修复: 缓存回退结果
    _yaml_cache = _fallback
    _yaml_cache_mtime = current_mtime
    return _fallback


# R13-P1-CFG-09修复: 配置文件schema验证
# 检查加载的配置中是否包含所有必需的key，防止配置缺失导致运行时错误
_REQUIRED_CONFIG_KEYS = frozenset([
    'max_kline', 'kline_style', 'exchange', 'exchanges',
    'close_take_profit_ratio', 'close_stop_loss_ratio', 'max_risk_ratio',
    'signal_cooldown_sec', 'max_hold_minutes', 'max_signals_per_window',
    'option_buy_lots_min', 'option_buy_lots_max', 'option_contract_multiplier',
    'circuit_breaker_pause_sec', 'daily_drawdown_multiplier',
])

_OPTIONAL_BUT_RECOMMENDED_KEYS = frozenset([
    'account_id', 'future_product', 'option_product',
    'backtest_tick_mode', 'debug_mode', 'stress_test_mode',
    'history_minutes', 'subscription_batch_size',
])


def validate_config_schema(config: Dict[str, Any]) -> Dict[str, Any]:
    """R13-P1-CFG-09修复: 验证配置字典是否包含必需的key

    Args:
        config: 待验证的配置字典

    Returns:
        Dict: 验证结果 {
            'valid': bool,
            'missing_required': List[str],
            'missing_recommended': List[str],
            'total_keys': int,
        }
    """
    if not isinstance(config, dict):
        return {
            'valid': False,
            'missing_required': list(_REQUIRED_CONFIG_KEYS),
            'missing_recommended': list(_OPTIONAL_BUT_RECOMMENDED_KEYS),
            'total_keys': 0,
            'error': 'config is not a dict',
        }

    config_keys = set(config.keys())
    missing_required = sorted(_REQUIRED_CONFIG_KEYS - config_keys)
    missing_recommended = sorted(_OPTIONAL_BUT_RECOMMENDED_KEYS - config_keys)

    if missing_required:
        logging.error(
            "[R13-P1-CFG-09] 配置缺少必需key: %s (有%d个缺失",
            missing_required, len(missing_required),
        )

    if missing_recommended:
        logging.warning(
            "[R13-P1-CFG-09] 配置缺少推荐key: %s (有%d个缺失",
            missing_recommended, len(missing_recommended),
        )

    return {
        'valid': len(missing_required) == 0,
        'missing_required': missing_required,
        'missing_recommended': missing_recommended,
        'total_keys': len(config_keys),
    }


# R13-P1-CFG-10修复: 环境变量文档
# 系统使用的所有环境变量及其描述和默认值
_DOCUMENTED_ENV_VARS = {
    'PARAM_OVERRIDE_MAX_KLINE': {
        'description': '覆盖max_kline参数(最大K线数量',
        'default': '200',
        'type': 'int',
    },
    'PARAM_OVERRIDE_KLINE_STYLE': {
        'description': '覆盖kline_style参数(K线周期，如M1/M5/M15)',
        'default': 'M1',
        'type': 'str',
    },
    'PARAM_OVERRIDE_EXCHANGE': {
        'description': '覆盖exchange参数(默认交易所)',
        'default': 'CFFEX',
        'type': 'str',
    },
    'PARAM_OVERRIDE_EXCHANGES': {
        'description': '覆盖exchanges参数(所有交易所列表，逗号分隔)',
        'default': 'CFFEX,SHFE,DCE,CZCE,INE,GFEX',
        'type': 'str',
    },
    'PARAM_OVERRIDE_CLOSE_TAKE_PROFIT_RATIO': {
        'description': '覆盖close_take_profit_ratio参数(止盈比例)',
        'default': '1.5',
        'type': 'float',
    },
    'PARAM_OVERRIDE_CLOSE_STOP_LOSS_RATIO': {
        'description': '覆盖close_stop_loss_ratio参数(止损比例)',
        'default': '0.3',  # R26-P1-SE-02修复: 从0.5改为0.3，与实际默认值对齐
        'type': 'float',
    },
    'PARAM_OVERRIDE_MAX_RISK_RATIO': {
        'description': '覆盖max_risk_ratio参数(最大风险比例)',  # R26-P1-SE-03修复: 补全右括号
        'default': '0.8',
        'type': 'float',
    },
    'PARAM_OVERRIDE_SIGNAL_COOLDOWN_SEC': {
        'description': '覆盖signal_cooldown_sec参数(信号冷却时间/频率',
        'default': '60.0',
        'type': 'float',
    },
    'PARAM_OVERRIDE_DEBUG_MODE': {
        'description': '覆盖debug_mode参数(调试模式开关',
        'default': 'False',
        'type': 'bool',
    },
    'PARAM_OVERRIDE_STRESS_TEST_MODE': {
        'description': '覆盖stress_test_mode参数(压力测试模式)',
        'default': 'False',
        'type': 'bool',
    },
    'PARAM_OVERRIDE_ACCOUNT_ID': {
        'description': '覆盖account_id参数(交易账户ID)',
        'default': '',
        'type': 'str',
    },
    'PARAM_OVERRIDE_CIRCUIT_BREAKER_PAUSE_SEC': {
        'description': '覆盖circuit_breaker_pause_sec参数(断路器暂停时间)',
        'default': '180.0',
        'type': 'float',
    },
    'PARAM_OVERRIDE_DAILY_DRAWDOWN_MULTIPLIER': {
        'description': '覆盖daily_drawdown_multiplier参数(日回撤乘数',
        'default': '2.0',
        'type': 'float',
    },
}


def get_documented_env_vars() -> Dict[str, Dict[str, str]]:
    """R13-P1-CFG-10修复: 返回系统使用的所有环境变量文档

    环境变量命名规则: PARAM_OVERRIDE_<KEY> (大写)
    优先级：环境变量 > JSON配置 > YAML参数 > 代码默认值

    Returns:
        Dict: 环境变量键-> {description, default, type}
    """
    return dict(_DOCUMENTED_ENV_VARS)


# ============================================================================
# R15-P2 性能+数据质量修复
# ============================================================================

# R15-P2-PERF-11修复: 配置读取添加lru_cache，避免重复解析
import functools

@functools.lru_cache(maxsize=256)  # R21-MEM-P2-10修复: maxsize=256有限制，配置值运行时不变无需TTL
def _get_config_value_cached(key: str, default: Any = None) -> Any:
    """带lru_cache的配置值读取，避免重复get_cached_params()调用"""
    try:
        params = get_cached_params()
        return params.get(key, default)
    except Exception:
        return default


_PARAM_ALIASES: Dict[str, str] = {
    'max_position_count': 'max_open_positions',
}


def get_param(key: str, default: Any = None) -> Any:
    """P2-8修复: 统一参数读取入口，支持别名解析

    优先读取规范名称，若不存在则尝试别名映射。
    例如: get_param('max_position_count') 会自动回退到 'max_open_positions'。

    Args:
        key: 参数键名
        default: 默认值

    Returns:
        参数值
    """
    try:
        params = get_cached_params()
        if key in params:
            return params[key]
        canonical = _PARAM_ALIASES.get(key)
        if canonical and canonical in params:
            return params[canonical]
    except Exception:
        pass
    return default


# R15-P2-DATA-05修复: 缺失数据默认填充值文档化
# 以下为系统各数据字段的默认填充值及含义:
_MISSING_VALUE_DEFAULTS = {
    'last_price': 0.0,          # 未成交时=0.0，表示无最新价(非NaN以避免计算异常
    'volume': 0,                # 无成交量=0(整数，非浮点)
    'open_interest': 0.0,       # 无持仓量=0.0
    'bid_price': 0.0,           # 无买价填0.0(非NaN，避免价差计算NaN传播)
    'ask_price': 0.0,           # 无卖价填0.0(同bid_price)
    'spread_quality': 0.0,      # 无价差质量评分填0.0(最低质量
    'days_to_expiry': 999,     # 无到期日=999(远期，避免误触发时间衰减)
    'implied_volatility': 0.0,  # 无IV=0.0(后续BS计算将使用历史波动率替代)
}


# R15-P2-DATA-06修复: 数据清洗规则配置
DATA_CLEANING_RULES = {
    'remove_zero_price': True,          # 移除last_price=0的tick(可能是非交易时段)
    'remove_negative_volume': True,     # 移除volume<0的tick(数据异常)
    'clamp_open_interest': True,        # 将open_interest<0钳位)
    'remove_future_timestamp': True,    # 移除timestamp>now的tick(时钟偏移)
    'max_price_change_pct': 20.0,       # 单tick价格变化超过20%视为异常
    'max_spread_pct': 5.0,             # 买卖价差超过5%视为异常
    'fill_method': 'ffill',             # 缺失值填充方式 'ffill'前填/'zero'零填/'interpolate'插值)
}


# R15-P2-DATA-04修复: 数据导出格式配置('parquet'优先)
DATA_EXPORT_FORMAT = 'parquet'  # 'parquet'(列表,推荐) | 'csv'(兼容) | 'arrow'(内存)


# R15-P2-DATA-07修复: 多源数据合并添加来源标记字段
_DATA_SOURCE_TAG_FIELD = '_data_source'  # 合并数据时标注来源'duckdb'|'parquet'|'api'|'cache')


# R15-P2-DATA-09修复: 数据质量评分函数框架
def _data_quality_score(tick_count: int, missing_pct: float, outlier_pct: float,
                        ohlc_violation_pct: float = 0.0, staleness_sec: float = 0.0) -> float:
    """数据质量评分(0-100)
    
    Args:
        tick_count: tick数量
        missing_pct: 缺失字段比例(0-1)
        outlier_pct: 异常值比(0-1)
        ohlc_violation_pct: OHLC关系违反比例(0-1)
        staleness_sec: 数据陈旧度(秒)
    
    Returns:
        float: 0-100分，>=80为良好，>=60为可用，<60为不可用
    """
    if tick_count == 0:
        return 0.0
    score = 100.0
    score -= missing_pct * 40.0       # 缺失扣40分
    score -= outlier_pct * 30.0       # 异常扣30分
    score -= ohlc_violation_pct * 20.0  # OHLC违反扣20分
    if staleness_sec > 60:
        score -= min(10.0, staleness_sec / 60.0)  # 陈旧扣分(最多10分
    return max(0.0, score)


# ============================================================================
# R15-P2 容错修复
# ============================================================================

# R15-P2-RESILIENCE-01修复: 检查点间隔可配置
CHECKPOINT_INTERVAL_SECONDS = safe_int(os.getenv('CHECKPOINT_INTERVAL_SECONDS', '300'))  # 默认5分钟

# R15-P2-RESILIENCE-03修复: 容错策略注册机制
RESILIENCE_REGISTRY = {
    'circuit_breaker': {'enabled': True, 'failure_threshold': 5, 'recovery_timeout': 60},
    'retry_with_backoff': {'enabled': True, 'max_retries': 3, 'base_delay': 1.0, 'max_delay': 30.0},
    'bulkhead': {'enabled': True, 'max_concurrent': 10, 'max_queue': 100},
    'timeout': {'enabled': True, 'default_timeout': 30.0},
    'fallback': {'enabled': True, 'fallback_value': None},
}

# R27-P2-DR-09~11修复: 看门狗超时配置外置（从环境变量读取，避免硬编码）
WATCHDOG_TIMEOUT_ORDER_SEC = float(os.getenv('WATCHDOG_TIMEOUT_ORDER_SEC', '30.0'))
WATCHDOG_TIMEOUT_POSITION_SEC = float(os.getenv('WATCHDOG_TIMEOUT_POSITION_SEC', '30.0'))
WATCHDOG_TIMEOUT_RISK_SEC = float(os.getenv('WATCHDOG_TIMEOUT_RISK_SEC', '30.0'))
WATCHDOG_TIMEOUT_SIGNAL_SEC = float(os.getenv('WATCHDOG_TIMEOUT_SIGNAL_SEC', '60.0'))
WATCHDOG_TIMEOUT_TICK_SEC = float(os.getenv('WATCHDOG_TIMEOUT_TICK_SEC', '10.0'))

# R15-P2-RESILIENCE-04修复: 降级特性列表
DEGRADATION_FEATURES = [
    'greeks_calculation',       # Greeks计算降级: 使用缓存值而非实时计算
    'alpha_monitoring',         # Alpha监控降级: 降低采样频率
    'shadow_strategy',          # 影子策略降级: 暂停shadow_a/shadow_b
    'hft_signal_filter',        # HFT信号过滤降级: 使用简单阈值替代Kalman
    'cycle_resonance',          # 周期共振降级: 使用固定周期替代自适应
    'parameter_optimization',   # 参数优化降级: 停止Walk-forward
]

# R24-P0-IV-06修复: 后端UI参数校验层——前端传参必须经过此层验证才能进入业务逻辑
def validate_ui_params(params: dict) -> tuple:
    """后端UI参数校验层

    Args:
        params: 前端传入的参数字典

    Returns:
        (is_valid, errors): 校验结果和错误列表
    """
    errors = []
    _NUMERIC_RANGES = {
        'close_take_profit_ratio': (0.5, 10.0, float),
        'close_stop_loss_ratio': (0.05, 1.0, float),
        'max_risk_ratio': (0.01, 1.0, float),
        'signal_cooldown_sec': (0, 3600, (int, float)),
        'max_signals_per_window': (1, 100, int),
        'max_hold_minutes': (1, 1440, int),
        'lots_min': (1, 100, int),
        'option_buy_lots_min': (1, 100, int),
        'option_buy_lots_max': (1, 1000, int),
        'circuit_breaker_pause_sec': (30.0, 3600.0, float),
        'daily_drawdown_multiplier': (1.0, 5.0, float),
    }
    import math
    for key, (lo, hi, expected_type) in _NUMERIC_RANGES.items():
        if key in params:
            val = params[key]
            if not isinstance(val, expected_type):
                errors.append(f'{key}: type={type(val).__name__}, expected={expected_type.__name__ if isinstance(expected_type, type) else expected_type}')
            elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                errors.append(f'{key}: NaN/Inf not allowed')
            elif val < lo or val > hi:
                errors.append(f'{key}: value={val} out of range [{lo}, {hi}]')
    # 方向参数白名单
    if 'direction' in params and params['direction'] not in ('BUY', 'SELL', None, ''):
        errors.append(f'direction: invalid value={params["direction"]}, must be BUY/SELL')
    return (len(errors) == 0, errors)

# R15-P2-RESILIENCE-05修复: SLA配置
SLA_CONFIG = {
    'max_recovery_time_seconds': 300,      # 最大恢复时间(5分钟
    'max_data_staleness_seconds': 60,      # 最大数据陈旧度1分钟
    'min_tick_throughput_per_sec': 10,     # 最低tick吞吐=10/s
    'max_order_latency_ms': 500,           # 最大下单延=500ms
    'max_position_reconciliation_diff': 0, # 仓位对账最大差=0)
    'availability_target_pct': 99.9,       # 可用性目=99.9%
}

# R15-P2-RESILIENCE-06修复: 告警级别定义
ALARM_LEVELS = {
    'P0_CRITICAL': {'description': '系统不可用，需立即介入', 'notify': ['phone', 'sms'], 'escalation_sec': 60},
    'P1_HIGH': {'description': '功能严重降级，需30分钟内介入', 'notify': ['sms', 'email'], 'escalation_sec': 300},
    'P2_MEDIUM': {'description': '性能劣化，需当日处理', 'notify': ['email'], 'escalation_sec': 3600},
    'P3_LOW': {'description': '潜在风险，记录跟踪', 'notify': ['log'], 'escalation_sec': 86400},
}

# R15-P2-RESILIENCE-08修复: 隔离级别分层
from enum import Enum as _Enum

class ISOLATION_LEVELS(_Enum):
    NONE = "none"                   # 无隔离(DEV环境)
    PROCESS = "process"             # 进程级隔离(策略进程独立)
    THREAD = "thread"               # 线程级隔离(共享进程，独立线程
    SERVICE = "service"             # 服务级隔离(微服务化部署)
    CONTAINER = "container"         # 容器级隔离(Docker/K8s)

# R15-P2-RESILIENCE-09修复: 容量规划配置
CAPACITY_LIMITS = {
    'max_instruments': 500,         # 最大订阅合约数
    'max_open_positions': 50,       # 最大同时持仓数
    'max_orders_per_sec': 20,       # 最大下单频率
    'max_memory_mb': 4096,          # 最大内存占用
    'max_cpu_pct': 80,              # 最大CPU占用率
    'max_disk_gb': 50,              # 最大磁盘占用
    'max_connections': 100,         # 最大数据库连接数
}

# R15-P2-RESILIENCE-10修复: 冷启动时间预估配置
COLD_START_TIMEOUT = safe_int(os.getenv('COLD_START_TIMEOUT', '120'))  # 默认120秒
COLD_START_PHASES = {
    'config_load': 5,               # 配置加载5秒
    'db_init': 15,                  # 数据库初始化15秒
    'history_load': 30,             # 历史数据加载30秒
    'instrument_subscribe': 20,     # 合约加载20秒
    'model_init': 10,               # 模型初始化10秒
    'shadow_engine_init': 10,       # 影子引擎初始化10秒
    'health_check': 5,              # 健康检查5秒
    'warmup_ticks': 25,             # 预热tick采集25秒
}


# R15-P2-RESILIENCE-02修复: 自动恢复决策树框架
def _auto_recovery_decision_tree(error_type: str, error_count: int, elapsed_sec: float) -> str:
    """自动恢复决策树 根据错误类型/频次/持续时间决定恢复策略
    
    Args:
        error_type: 错误类型('db_lock'|'network'|'data_stale'|'oom'|'order_timeout')
        error_count: 连续错误次数
        elapsed_sec: 错误持续秒数
    
    Returns:
        恢复策略: 'retry'|'circuit_break'|'degrade'|'restart'|'halt'
    """
    if error_count <= 2 and elapsed_sec < 30:
        return 'retry'
    elif error_count <= 5 and elapsed_sec < 120:
        if error_type in ('db_lock', 'network'):
            return 'circuit_break'
        return 'degrade'
    elif error_count <= 10 and elapsed_sec < 300:
        if error_type == 'oom':
            return 'restart'
        return 'degrade'
    else:
        return 'halt'


# ============================================================================
# R15-P1-SEC-09修复: 密钥生命周期管理
# ============================================================================
def check_key_lifecycle() -> Dict[str, Any]:
    """R15-P1-SEC-09修复: 检查API密钥生命周期，返回期和即将过期的密钥状态"""
    params = get_cached_params()
    rotation_days = params.get('key_rotation_interval_days', 90)
    max_age_hours = params.get('key_max_age_hours', 2160)
    last_rotation = params.get('key_last_rotation_time')
    result = {
        'rotation_days': rotation_days,
        'max_age_hours': max_age_hours,
        'needs_rotation': False,
        'days_since_rotation': None,
    }
    if last_rotation:
        try:
            last_dt = datetime.fromisoformat(last_rotation) if isinstance(last_rotation, str) else last_rotation
            days_since = (datetime.now(CHINA_TZ) - last_dt).days
            result['days_since_rotation'] = days_since
            result['needs_rotation'] = days_since >= rotation_days
            if result['needs_rotation']:
                logging.warning(
                    "R15-P1-SEC-09: API密钥已超过轮换周期 days_since=%d rotation_days=%d",
                    days_since, rotation_days
                )
        except Exception as e:
            logging.debug("R15-P1-SEC-09: key_last_rotation_time解析失败: %s", e)
            result['needs_rotation'] = True
    else:
        # 从未轮换。
        result['needs_rotation'] = True
        logging.info("R15-P1-SEC-09: 密钥从未轮换，建议立即轮换")
    return result


# ============================================================================
# R15-P1-SEC-12修复: 安全事件自动响应
# ============================================================================
class SecurityEventResponder:
    """R15-P1-SEC-12修复: 检测到安全异常时自动阻断"""

    def __init__(self):
        self._blocked_sources: set = set()
        self._suspicious_counts: Dict[str, int] = {}
        self._lock = threading.Lock()

    def report_suspicious(self, source: str, reason: str) -> bool:
        """报告可疑活动，返回True表示已自动阻断"""
        with self._lock:
            self._suspicious_counts[source] = self._suspicious_counts.get(source, 0) + 1
            if self._suspicious_counts[source] >= 3:
                self._blocked_sources.add(source)
                logging.critical(
                    "R15-P1-SEC-12: 安全自动阻断! source=%s reason=%s count=%d",
                    source, reason, self._suspicious_counts[source]
                )
                return True
            logging.warning(
                "R15-P1-SEC-12: 可疑活动记录 source=%s reason=%s count=%d/3",
                source, reason, self._suspicious_counts[source]
            )
            return False

    def is_blocked(self, source: str) -> bool:
        with self._lock:
            return source in self._blocked_sources

    def unblock(self, source: str) -> None:
        with self._lock:
            self._blocked_sources.discard(source)
            self._suspicious_counts.pop(source, None)


_security_responder: Optional[SecurityEventResponder] = None
_security_responder_lock = threading.Lock()


def get_security_responder() -> SecurityEventResponder:
    global _security_responder
    with _security_responder_lock:
        if _security_responder is None:
            _security_responder = SecurityEventResponder()
        return _security_responder


_HOT_RELOAD_UNSUPPORTED_KEYS = frozenset([
    'tvf_enabled', 'tvf_sigmoid_scale', 'mode_engine_version',
    'max_risk_ratio', 'close_stop_loss_ratio', 'signal_cooldown_sec',
])

def get_hot_reload_status() -> Dict[str, Any]:
    """R26-P0-CD-15: 热加载生效范围查询——返回哪些参数支持热加载、哪些需重启
    
    Returns:
        Dict: {supported_keys: [...], unsupported_keys: [...], requires_restart: bool}
    """
    return {
        'supported_keys': sorted(set(DEFAULT_PARAM_TABLE.keys()) - _HOT_RELOAD_UNSUPPORTED_KEYS),
        'unsupported_keys': sorted(_HOT_RELOAD_UNSUPPORTED_KEYS),
        'requires_restart_hint': f"以下参数变更需重启: {', '.join(sorted(_HOT_RELOAD_UNSUPPORTED_KEYS))}",
    }
