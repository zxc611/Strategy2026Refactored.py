# [M1-83] 配置加载器
# MODULE_ID: M1-010

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""

config_loader.py _合并发config_json_loader / config_yaml_loader / config_option_loader

包含:

  [JSON加载/热重载]

    _resolve_params_json_path, _resolve_runtime_env_name, _apply_env_overrides,

    load_default_params_from_json, check_config_hot_reload

  [YAML加载与迁移]

    _load_yaml_file, load_cascade_threshold_grid, load_plr_thresholds

  [期权参数加载]

    _normalize_option_params_payload, load_option_params_from_file, merge_option_params_to_default

"""

from __future__ import annotations



import errno

import json  # R5-1: 保留用于json.JSONDecodeError

from ali2026v3_trading.infra.logging_utils import get_logger

import logging

import os

import threading

import time

from typing import Any, Dict, Optional



from ali2026v3_trading.infra.serialization_utils import json_loads, yaml_safe_load

from ali2026v3_trading.infra.shared_utils import safe_int



_logger = get_logger(__name__)  # R9-5





# ============================================================================

# Section 1: JSON加载/热重复(源自 config_json_loader.py)

# ============================================================================



_PARAMS_JSON_PATH: Optional[str] = None

_PARAMS_JSON_CACHE: Optional[Dict[str, Any]] = None

_PARAMS_JSON_CACHE_TIMESTAMP: float = 0.0

PARAMS_JSON_CACHE_TTL: float = 60.0

_PARAMS_JSON_LOCK = threading.Lock()



_config_file_watchdog_last_mtime: float = 0.0

_config_file_watchdog_last_check: float = 0.0

_CONFIG_WATCHDOG_INTERVAL: float = 60.0





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

            logging.info("[config_json_loader] 检测到环境参数分支: env=%s path=%s", runtime_env, p)

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





def _apply_env_overrides(params: Dict[str, Any]) -> Dict[str, Any]:

    env = _resolve_runtime_env_name()

    if env == 'production':

        pass

    elif env == 'testing':

        params['test_mode'] = True

        logging.info("[CFG-01] TRADING_ENV=testing, override test_mode=True")

    elif env == 'development':

        params['test_mode'] = True

        params['log_level'] = 'DEBUG'

        logging.info("[CFG-01] TRADING_ENV=development, override test_mode=True + log_level=DEBUG")

    return params





# [P1-23] 权威配置源/ 权威持久化源

# 配置加载优先先 JSON > YAML > 默认证
def load_default_params_from_json(json_path: Optional[str] = None) -> Dict[str, Any]:

    global _PARAMS_JSON_CACHE, _PARAMS_JSON_CACHE_TIMESTAMP

    from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE

    from ali2026v3_trading.infra.security import _validate_path_safety

    with _PARAMS_JSON_LOCK:

        now = time.time()

        if _PARAMS_JSON_CACHE is not None and (now - _PARAMS_JSON_CACHE_TIMESTAMP < PARAMS_JSON_CACHE_TTL):

            return _PARAMS_JSON_CACHE

    path = json_path or _resolve_params_json_path()

    path = _validate_path_safety(path)

    if not os.path.isfile(path):

        _marker_file = os.path.join(os.path.dirname(path), '.params_json_deployed')

        if os.path.isfile(_marker_file):

            logging.error("[R24-P2-DF-02] 参数JSON文件丢失(曾部署: %s，回退到DEFAULT_PARAM_TABLE", path)

        else:

            logging.info("[R24-P2-DF-02] 参数JSON首次部署(文件不存在: %s，使用DEFAULT_PARAM_TABLE", path)

            try:

                with open(_marker_file, 'w', encoding='utf-8') as _mf:

                    _mf.write(str(time.time()))

            except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:

                pass

        try:

            from ali2026v3_trading.governance.param_table_provider import get_param_table_provider

            base = get_param_table_provider().get_params('_default')

            return _apply_env_overrides(dict(base) if base else dict(DEFAULT_PARAM_TABLE))

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _r3_err:

            return _apply_env_overrides(dict(DEFAULT_PARAM_TABLE))

    try:

        with open(path, 'r', encoding='utf-8') as f:

            data = json_loads(f.read())  # R5-1

    except (json.JSONDecodeError, IOError) as e:

        logging.warning("[config_json_loader] 参数JSON解析失败: %s，回退到DEFAULT_PARAM_TABLE", e)

        return _apply_env_overrides(dict(DEFAULT_PARAM_TABLE))

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

    logging.debug("[config_json_loader] 从JSON加载参数完成: %s，共 %d 个参数", path, len(result))

    return _apply_env_overrides(result)





def check_config_hot_reload() -> bool:

    global _config_file_watchdog_last_mtime, _config_file_watchdog_last_check

    from ali2026v3_trading.config.config_params import get_cached_params, reset_param_cache, _notify_param_change

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

            try:

                with open(path, 'r', encoding='utf-8') as _cfg_f:

                    _raw_config = json_loads(_cfg_f.read())  # R5-1

                _format_ver = _raw_config.get('format_version', None)

                if _format_ver is not None and _format_ver != '1.0':

                    logging.warning("[P1-FIX] Config hot-reload: format_version mismatch: %s (expected 1.0)", _format_ver)

                    _config_file_watchdog_last_mtime = current_mtime

                    return False

            except (json.JSONDecodeError, IOError) as _cfg_err:

                logging.warning("[P1-FIX] Config hot-reload: failed to read config: %s", _cfg_err)

                _config_file_watchdog_last_mtime = current_mtime

                return False

            _config_file_watchdog_last_mtime = current_mtime

            reset_param_cache()

            logging.info("[R4-D-10] 配置热更新%s 已变更，缓存已刷新", path)
            try:

                from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads

                _reloaded_params = get_cached_params()

                _serialized = json_dumps(_reloaded_params)

                _deserialized = json_loads(_serialized)

                if _reloaded_params != _deserialized:

                    logging.warning(

                        "SER-P1-14: 热重载后序列化格式不一） 原始keys=%d, 反序列化keys=%d",

                        len(_reloaded_params) if isinstance(_reloaded_params, dict) else 0,

                        len(_deserialized) if isinstance(_deserialized, dict) else 0,

                    )

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _ser_err:

                logging.warning("SER-P1-14: 热重载后序列化格式验证失败 %s", _ser_err)

            _notify_param_change([], 'hot_reload')

            return True

    except OSError as _os_err:

        if getattr(_os_err, 'errno', None) == errno.ENOSPC:

            logging.critical("[P1-16] 磁盘空间不足，配置热重载失败: %s", _os_err)

        else:

            logging.debug("[P1-16] 配置热重载OSError(非ENOSPC): %s", _os_err)

    return False





# ============================================================================

# Section 2: YAML加载与迁移(源自 config_yaml_loader.py)

# ============================================================================



def _load_yaml_file(yaml_path: str) -> Dict[str, Any]:

    """P2-30修复: 统一使用yaml_safe_load，消除ruamel fallback绕过守卫的风控"""

    try:

        with open(yaml_path, 'r', encoding='utf-8') as _f:

            data = yaml_safe_load(_f)

    except ImportError:

        logging.error("[ConfigService] P1-4: yaml不可用，无法加载YAML配置")

        return {}

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

        logging.error("[ConfigService] 加载YAML配置失败: %s", e)

        return {}

    if not isinstance(data, dict):

        return {}

    _required_top_keys = {'products', 'thresholds', 'default'}

    _found_keys = set(data.keys())

    if _required_top_keys & _found_keys:

        for _k, _v in data.items():

            if _k in ('products', 'thresholds') and not isinstance(_v, (dict, list)):

                logging.error("[SEC-P1-03] YAML字段类型错误: %s期望dict/list, 实际%s", _k, type(_v).__name__)

    return data if isinstance(data, dict) else {}





def load_cascade_threshold_grid() -> Dict[str, Any]:

    _module_dir = os.path.dirname(os.path.abspath(__file__))

    _yaml_path = os.path.join(_module_dir, "config", "cascade_threshold_grid.yaml")

    if not os.path.exists(_yaml_path):

        logging.debug("[ConfigService] cascade_threshold_grid.yaml缺失，使用空阈值表: %s", _yaml_path)

        return {}

    data = _load_yaml_file(_yaml_path)

    logging.info("[ConfigService] P1-4: load_cascade_threshold_grid() 加载成功, path=%s", _yaml_path)

    return data





def load_plr_thresholds() -> Dict[str, Any]:

    _module_dir = os.path.dirname(os.path.abspath(__file__))

    _project_dir = os.path.dirname(_module_dir)

    _yaml_path = os.path.join(_project_dir, "param_pool", "param_configs.yaml")

    if not os.path.exists(_yaml_path):

        _fallback_path = os.path.join(_module_dir, "param_pool", "param_configs.yaml")

        if os.path.exists(_fallback_path):

            _yaml_path = _fallback_path

        else:

            logging.error("[ConfigService] R32-P2-09: param_configs.yaml 不存在(param_pool/ _参数据 均未找到, PLR评判将无阈值")

            return {}

    data = _load_yaml_file(_yaml_path)

    plr_data = data.get('plr_thresholds', data) if isinstance(data, dict) else data

    logging.info("[ConfigService] P1-4: load_plr_thresholds() 加载成功, path=%s", _yaml_path)

    return plr_data





# ============================================================================

# Section 3: 期权参数加载 (源自 config_option_loader.py)

# ============================================================================



def _normalize_option_params_payload(payload: Any, source_path: str) -> Dict[str, Dict[str, Any]]:

    if not isinstance(payload, dict):

        logging.warning(

            "[config_option_loader] 期权参数文件顶层不是对象，已忽略: %s (%s)",

            source_path, type(payload).__name__,

        )

        return {}

    candidate = payload

    for key in ('option_params_detail', 'option_params', 'products', 'data', 'params'):

        nested = candidate.get(key)

        if isinstance(nested, dict):

            candidate = nested

            break

    normalized: Dict[str, Dict[str, Any]] = {}

    ignored_keys = []

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

            "[config_option_loader] 期权参数文件存在 %d 个非品种字段，已自动忽略 %s%s",

            len(ignored_keys), preview, suffix,

        )

    return normalized





def load_option_params_from_file() -> Dict[str, Any]:

    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    possible_paths = [

        os.path.join(_project_root, 'auto_generated_option_params.json'),

    ]

    for file_path in possible_paths:

        if not os.path.exists(file_path):

            continue

        try:

            from ali2026v3_trading.infra.security import _validate_path_safety

            file_path = _validate_path_safety(file_path)

        except ValueError as ve:

            logging.debug("[config_option_loader] 路径安全验证失败，跳过可选文档 %s", ve)

            continue

        try:

            with open(file_path, 'r', encoding='utf-8') as f:

                raw_data = json_loads(f.read())  # R5-1

            data = _normalize_option_params_payload(raw_data, file_path)

            if not data:

                logging.info("[config_option_loader] 期权参数文件无有效品种配置，继续使用默认参数: %s", file_path)

                continue

            logging.info("[config_option_loader] 成功加载期权参数配置: %s", file_path)

            logging.info("[config_option_loader] 包含 %d 个品种的期权参数", len(data))

            total_strikes = sum(safe_int((p or {}).get('strike_count', 0) or 0) for p in data.values())

            logging.info("[config_option_loader] 总行权价组合并%d", total_strikes)

            return data

        except json.JSONDecodeError as e:

            logging.error("[config_option_loader] 期权参数文件 JSON 解析失败: %s - %s", file_path, e)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

            logging.error("[config_option_loader] 加载期权参数文件失败: %s - %s", file_path, e)

    logging.info("[config_option_loader] 未找到可用的auto_generated_option_params.json", 使用默认期权参数)
    logging.debug("[R24-P2-DF-09] 期权参数为空", 期权策略可能使用模块内默认证)
    return {}





def merge_option_params_to_default():

    from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE, _param_table_lock, _OPTION_PARAM_RUNTIME_PATCH

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

                logging.info("[config_option_loader] 已合并期权品种列表%d 个品种", len(products_from_file))

                logging.info("[config_option_loader] 最终期权品种%s", _OPTION_PARAM_RUNTIME_PATCH['option_products'])





__all__ = [

    # JSON加载/热重复
    '_resolve_params_json_path', '_resolve_runtime_env_name', '_apply_env_overrides',

    'load_default_params_from_json', 'check_config_hot_reload',

    'PARAMS_JSON_CACHE_TTL',

    # YAML加载与迁移
    '_load_yaml_file', 'load_cascade_threshold_grid', 'load_plr_thresholds',

    # 期权参数加载

    '_normalize_option_params_payload', 'load_option_params_from_file', 'merge_option_params_to_default',

]





# ============================================================================

# Section 4: 配置解析与优先级 (_config_resolver.py)

# ============================================================================

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

                logging.debug("[R4-D-01] JSON配置覆盖率d 个参数", json_override_count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[R4-D-01] JSON配置加载失败，使用代码默认证%s", e)



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

                logging.warning("[R4-D-01] 环境变量 %s=%s 类型转换失败，保持原子, env_key", env_val)


    if env_override_count > 0:

        logging.info("[R4-D-01] 环境变量覆盖率d 个参数", env_override_count)


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

    'PARAM_OVERRIDE_MAX_KLINE': {'description': '覆盖max_kline参数(最大K线数据', 'default': '200', 'type': 'int'},

    'PARAM_OVERRIDE_KLINE_STYLE': {'description': '覆盖kline_style参数(K线周期，如M1/M5/M15)', 'default': 'M1', 'type': 'str'},

    'PARAM_OVERRIDE_EXCHANGE': {'description': '覆盖exchange参数(默认交易所)', 'default': 'CFFEX', 'type': 'str'},

    'PARAM_OVERRIDE_EXCHANGES': {'description': '覆盖exchanges参数(所有交易所列表，逗号分隔)', 'default': 'CFFEX,SHFE,DCE,CZCE,INE,GFEX', 'type': 'str'},

    'PARAM_OVERRIDE_CLOSE_TAKE_PROFIT_RATIO': {'description': '覆盖close_take_profit_ratio参数(止盈比例)', 'default': '1.5', 'type': 'float'},

    'PARAM_OVERRIDE_CLOSE_STOP_LOSS_RATIO': {'description': '覆盖close_stop_loss_ratio参数(止损比例)', 'default': '0.3', 'type': 'float'},

    'PARAM_OVERRIDE_MAX_RISK_RATIO': {'description': '覆盖max_risk_ratio参数(最大风险比。', 'default': '0.8', 'type': 'float'},

    'PARAM_OVERRIDE_SIGNAL_COOLDOWN_SEC': {'description': '覆盖signal_cooldown_sec参数(信号冷却时间/频率)', 'default': '60.0', 'type': 'float'},

    'PARAM_OVERRIDE_DEBUG_MODE': {'description': '覆盖debug_mode参数(调试模式开启', 'default': 'False', 'type': 'bool'},

    'PARAM_OVERRIDE_STRESS_TEST_MODE': {'description': '覆盖stress_test_mode参数(压力测试模式)', 'default': 'False', 'type': 'bool'},

    'PARAM_OVERRIDE_ACCOUNT_ID': {'description': '覆盖account_id参数(交易账户ID)', 'default': '', 'type': 'str'},

    'PARAM_OVERRIDE_CIRCUIT_BREAKER_PAUSE_SEC': {'description': '覆盖circuit_breaker_pause_sec参数(断路器暂停时常', 'default': '180.0', 'type': 'float'},

    'PARAM_OVERRIDE_DAILY_DRAWDOWN_MULTIPLIER': {'description': '覆盖daily_drawdown_multiplier参数(日回撤乘。', 'default': '2.0', 'type': 'float'},

}





import json  # R5-1: 保留用于json.JSONDecodeError

from ali2026v3_trading.infra.serialization_utils import json_loads





def get_params_metadata(json_path: Optional[str] = None) -> Dict[str, Any]:

    if json_path is None:

        try:

            from ali2026v3_trading.config.config_loader import _resolve_params_json_path

            json_path = _resolve_params_json_path()

        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:

            return {}

    if not os.path.isfile(json_path):

        return {}

    try:

        with open(json_path, 'r', encoding='utf-8') as f:

            return json_loads(f.read())  # R5-1

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

        'C2':  lambda p: __import__('ali2026v3_trading.config.config_params', fromlist=['validate_param_range']).validate_param_range('close_stop_loss_ratio', p.get('close_stop_loss_ratio', 0.3)),  # P1-57: 统一校验

        'C3':  lambda p: __import__('ali2026v3_trading.config.config_params', fromlist=['validate_param_range']).validate_param_range('max_risk_ratio', p.get('max_risk_ratio', 0.8)),  # P1-57: 统一校验

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

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

    logging.info("[config_resolver] 已应用环境覆盖%s，覆盖%d 个参数", env, len(env_configs))

    return merged

