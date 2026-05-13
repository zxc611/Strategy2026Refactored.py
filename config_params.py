"""
参数表管理模块 - 从 config_service.py 拆分
职责：默认参数表、参数缓存、参数加载/校验/环境覆盖
"""
from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ali2026v3_trading.config_exchange import ExchangeConfig


_param_table_cache = None
_param_table_cache_timestamp = 0.0
_param_table_lock = threading.Lock()
CACHE_TTL = 300.0

_DEFAULT_PARAM_TABLE_LOCK = threading.Lock()

DEFAULT_PARAM_TABLE = {
    "max_kline": 200,
    "kline_style": "M1",
    "subscribe_options": True,
    "debug_output": True,
    "diagnostic_output": True,
    "api_key": "",
    "infini_api_key": "",
    "access_key": "",
    "access_secret": "",
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
        "IF": ["IF2602", "IF2603"],
        "IH": ["IH2602", "IH2603"],
        "IM": ["IM2602", "IM2603"],
        "CU": ["CU2603", "CU2604"],
        "AL": ["AL2603", "AL2604"],
        "ZN": ["ZN2603", "ZN2604"],
        "RB": ["RB2603", "RB2604"],
        "AU": ["AU2603", "AU2604"],
        "AG": ["AG2603", "AG2604"],
        "M": ["M2603", "M2605"],
        "Y": ["Y2603", "Y2605"],
        "A": ["A2603", "A2605"],
        "JM": ["JM2604", "JM2605"],
        "I": ["I2603", "I2605"],
        "J": ["J2604", "J2605"],
        "CF": ["CF2603", "CF2605"],
        "SR": ["SR2603", "SR2605"],
        "MA": ["MA2603", "MA2605"],
        "TA": ["TA2603", "TA2605"],
    },
    "signal_cooldown_sec": 0.0,
    "option_buy_lots_min": 1,
    "option_buy_lots_max": 100,
    "option_contract_multiplier": 10000,
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
    "close_take_profit_ratio": 1.5,
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
    "ui_window_width": 260,
    "ui_window_height": 240,
    "ui_font_large": 11,
    "ui_font_small": 10,
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
    "kline_max_age_sec": 0,
    "signal_max_age_sec": 180,
    "top3_rows": 3,
    "lots_min": 5,
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
    "daily_drawdown_multiplier": 2.0,
    "hard_time_stop_minutes": 90.0,
    "min_profit_threshold": 0.002,
    "max_net_delta_pct": 0.30,
    "max_net_gamma_pct": 0.08,
    "max_net_vega_bps": 0.02,
    "max_theta_ratio": 0.5,
    "debug_mode": False,
    "stress_test_mode": False,
}

_PARAMS_JSON_PATH: Optional[str] = None
_PARAMS_JSON_CACHE: Optional[Dict[str, Any]] = None


def get_cached_params() -> dict:
    global _param_table_cache, _param_table_cache_timestamp
    now = time.time()
    with _param_table_lock:
        if (_param_table_cache is not None and
            now - _param_table_cache_timestamp < CACHE_TTL):
            return _param_table_cache
        try:
            _param_table_cache = copy.deepcopy(load_default_params_from_json())
        except Exception as e:
            logging.warning(f"[config_params] JSON加载失败({e})，回退到代码内 DEFAULT_PARAM_TABLE")
            _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
        _param_table_cache_timestamp = now
        logging.info(f"[config_params.get_cached_params] 参数表已{'刷新' if _param_table_cache else '加载'}，包含 {len(_param_table_cache)} 个参数 (TTL={CACHE_TTL}s)")
        return _param_table_cache


def reset_param_cache() -> None:
    global _param_table_cache
    with _param_table_lock:
        _param_table_cache = None
        logging.info("[config_params.reset_param_cache] 参数缓存已重置，下次访问时将重新加载")


def update_cached_params(updates: Dict[str, Any], sync_default_table: bool = True) -> dict:
    global _param_table_cache, _param_table_cache_timestamp
    normalized_updates = dict(updates or {})
    if not normalized_updates:
        return get_cached_params()
    with _param_table_lock:
        if _param_table_cache is None:
            _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
        for key, value in normalized_updates.items():
            _param_table_cache[key] = value
        _param_table_cache_timestamp = time.time()
    logging.info(
        "[config_params.update_cached_params] 已更新参数缓存字段：%s",
        ', '.join(sorted(normalized_updates.keys())),
    )
    return _param_table_cache


def _resolve_params_json_path() -> str:
    possible = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'params_default.json'),
        os.path.join(os.getcwd(), 'config', 'params_default.json'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'params_default.json'),
    ]
    for p in possible:
        if os.path.isfile(p):
            return p
    return possible[0]


def load_default_params_from_json(json_path: Optional[str] = None) -> Dict[str, Any]:
    global _PARAMS_JSON_CACHE
    path = json_path or _resolve_params_json_path()
    if _PARAMS_JSON_CACHE is not None:
        return _PARAMS_JSON_CACHE
    if not os.path.isfile(path):
        logging.warning(f"[config_params] 参数JSON不存在: {path}，回退到代码内 DEFAULT_PARAM_TABLE")
        return dict(DEFAULT_PARAM_TABLE)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"[config_params] 参数JSON解析失败: {e}，回退到 DEFAULT_PARAM_TABLE")
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
    _PARAMS_JSON_CACHE = result
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
        'C1':  lambda p: p.get('close_take_profit_ratio', 1.5) > 1.0,
        'C2':  lambda p: 0 < p.get('close_stop_loss_ratio', 0.5) < 1.0,
        'C3':  lambda p: 0 < p.get('max_risk_ratio', 0.8) <= 1.0,
        'C4':  lambda p: p.get('option_width_min_threshold', 4.0) >= 1.0,
        'C5':  lambda p: p.get('signal_cooldown_sec', 0) <= p.get('rate_limit_window_sec', 120),
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
            warnings.append(f"约束检查异常 [{cid}]: {e}")
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
        logging.warning(f"[config_params] 未知环境 '{env}'，保持原参数不变。已知: {list(metadata.get('environments', {}).keys())}")
        return params
    merged = dict(params)
    for key, value in env_configs.items():
        merged[key] = value
    logging.info(f"[config_params] 已应用环境覆盖: {env}，覆盖 {len(env_configs)} 个参数")
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
            f"[config_params] 期权参数文件存在 {len(ignored_keys)} 个旧版/非品种字段，已自动忽略: {preview}{suffix}"
        )
    return normalized


def load_option_params_from_file() -> Dict[str, Any]:
    possible_paths = [
        'auto_generated_option_params.json',
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'auto_generated_option_params.json'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'auto_generated_option_params.json'),
    ]
    for file_path in possible_paths:
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
                total_strikes = sum(int((p or {}).get('strike_count', 0) or 0) for p in data.values())
                logging.info(f"[config_params] 总行权价组合数: {total_strikes}")
                return data
            except json.JSONDecodeError as e:
                logging.error(f"[config_params] 期权参数文件 JSON 解析失败: {file_path} - {e}")
            except Exception as e:
                logging.error(f"[config_params] 加载期权参数文件失败: {file_path} - {e}")
    logging.warning("[config_params] 未找到 auto_generated_option_params.json，使用默认期权参数")
    return {}


def merge_option_params_to_default():
    global DEFAULT_PARAM_TABLE
    option_params = load_option_params_from_file()
    if option_params:
        with _DEFAULT_PARAM_TABLE_LOCK:
            DEFAULT_PARAM_TABLE["option_params_detail"] = option_params
            products_from_file = list(option_params.keys())
            if products_from_file:
                current_products = set(DEFAULT_PARAM_TABLE.get("option_products", "").split(","))
                current_products = {p.strip() for p in current_products if p.strip()}
                new_products = current_products.union(set(products_from_file))
                DEFAULT_PARAM_TABLE["option_products"] = ",".join(sorted(new_products))
                logging.info(f"[config_params] 已合并期权品种列表: {len(products_from_file)} 个品种")
                logging.info(f"[config_params] 最终期权品种: {DEFAULT_PARAM_TABLE['option_products']}")


try:
    merge_option_params_to_default()
except Exception as e:
    logging.warning(f"[config_params] 合并期权参数时出错: {e}")
