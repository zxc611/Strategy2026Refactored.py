from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional


def resolve_config_with_priority(
    base_params: Optional[Dict[str, Any]] = None,
    env_prefix: str = "PARAM_OVERRIDE_",
    default_param_table: Optional[Dict[str, Any]] = None,
    load_json_fn=None,
) -> Dict[str, Any]:
    if default_param_table is None:
        default_param_table = {}
    result = dict(default_param_table)

    if load_json_fn is not None:
        try:
            json_params = load_json_fn()
            json_override_count = 0
            for k, v in json_params.items():
                if k in result and result[k] != v:
                    json_override_count += 1
                result[k] = v
            if json_override_count > 0:
                logging.debug("[R4-D-01] JSON配置覆盖了%d 个参数", json_override_count)
        except Exception as e:
            logging.warning("[R4-D-01] JSON配置加载失败，使用代码默认值 %s", e)

    if base_params:
        for k, v in base_params.items():
            if v is not None:
                result[k] = v

    env_override_count = 0
    for key in list(result.keys()):
        env_key = f"{env_prefix}{key.upper()}"
        env_val = os.environ.get(env_key)
        if env_val is not None:
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
        logging.info("[R4-D-01] 环境变量覆盖了%d 个参数", env_override_count)

    return result


def _data_quality_score(tick_count: int, missing_pct: float, outlier_pct: float,
                        ohlc_violation_pct: float = 0.0, staleness_sec: float = 0.0) -> float:
    if tick_count == 0:
        return 0.0
    score = 100.0
    score -= missing_pct * 40.0
    score -= outlier_pct * 30.0
    score -= ohlc_violation_pct * 20.0
    if staleness_sec > 60:
        score -= min(10.0, staleness_sec / 60.0)
    return max(0.0, score)


_DOCUMENTED_ENV_VARS = {
    'PARAM_OVERRIDE_MAX_KLINE': {'description': '覆盖max_kline参数(最大K线数量)', 'default': '200', 'type': 'int'},
    'PARAM_OVERRIDE_KLINE_STYLE': {'description': '覆盖kline_style参数(K线周期，如M1/M5/M15)', 'default': 'M1', 'type': 'str'},
    'PARAM_OVERRIDE_EXCHANGE': {'description': '覆盖exchange参数(默认交易所)', 'default': 'CFFEX', 'type': 'str'},
    'PARAM_OVERRIDE_EXCHANGES': {'description': '覆盖exchanges参数(所有交易所列表，逗号分隔)', 'default': 'CFFEX,SHFE,DCE,CZCE,INE,GFEX', 'type': 'str'},
    'PARAM_OVERRIDE_CLOSE_TAKE_PROFIT_RATIO': {'description': '覆盖close_take_profit_ratio参数(止盈比例)', 'default': '1.5', 'type': 'float'},
    'PARAM_OVERRIDE_CLOSE_STOP_LOSS_RATIO': {'description': '覆盖close_stop_loss_ratio参数(止损比例)', 'default': '0.3', 'type': 'float'},
    'PARAM_OVERRIDE_MAX_RISK_RATIO': {'description': '覆盖max_risk_ratio参数(最大风险比例)', 'default': '0.8', 'type': 'float'},
    'PARAM_OVERRIDE_SIGNAL_COOLDOWN_SEC': {'description': '覆盖signal_cooldown_sec参数(信号冷却时间/频率)', 'default': '60.0', 'type': 'float'},
    'PARAM_OVERRIDE_DEBUG_MODE': {'description': '覆盖debug_mode参数(调试模式开关)', 'default': 'False', 'type': 'bool'},
    'PARAM_OVERRIDE_STRESS_TEST_MODE': {'description': '覆盖stress_test_mode参数(压力测试模式)', 'default': 'False', 'type': 'bool'},
    'PARAM_OVERRIDE_ACCOUNT_ID': {'description': '覆盖account_id参数(交易账户ID)', 'default': '', 'type': 'str'},
    'PARAM_OVERRIDE_CIRCUIT_BREAKER_PAUSE_SEC': {'description': '覆盖circuit_breaker_pause_sec参数(断路器暂停时间)', 'default': '180.0', 'type': 'float'},
    'PARAM_OVERRIDE_DAILY_DRAWDOWN_MULTIPLIER': {'description': '覆盖daily_drawdown_multiplier参数(日回撤乘数)', 'default': '2.0', 'type': 'float'},
}


import json


def get_params_metadata(json_path: Optional[str] = None) -> Dict[str, Any]:
    if json_path is None:
        try:
            from ali2026v3_trading.config_params import _resolve_params_json_path
            json_path = _resolve_params_json_path()
        except Exception:
            return {}
    if not os.path.isfile(json_path):
        return {}
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def validate_params_with_metadata(params: Dict[str, Any], json_path: Optional[str] = None) -> Dict[str, Any]:
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
        'C1':  lambda p: p.get('close_take_profit_ratio', 1.8) > 1.0,
        'C2':  lambda p: 0 < p.get('close_stop_loss_ratio', 0.3) < 1.0,
        'C3':  lambda p: 0 < p.get('max_risk_ratio', 0.8) <= 1.0,
        'C4':  lambda p: p.get('option_width_min_threshold', 4.0) >= 1.0,
        'C5':  lambda p: p.get('signal_cooldown_sec', 60.0) <= p.get('rate_limit_window_sec', 120),
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
    return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings, 'count': len(params)}


def apply_environment(params: Dict[str, Any], env: str) -> Dict[str, Any]:
    metadata = get_params_metadata()
    env_configs = metadata.get('environments', {}).get(env, {})
    if not env_configs:
        logging.warning("[config_resolver] 未知环境 '%s'，保持原参数不变", env)
        return params
    merged = dict(params)
    for key, value in env_configs.items():
        merged[key] = value
    logging.info("[config_resolver] 已应用环境覆盖 %s，覆盖%d 个参数", env, len(env_configs))
    return merged