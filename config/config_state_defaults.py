"""
config_state_defaults.py - 状态参数默认值+配置校验
R27: 从config_params.py提取的状态参数默认值和校验函数

职责：
- 状态参数集默认值 (get_default_state_param_sets)
- 配置schema校验 (validate_config_schema)
- UI参数校验 (validate_ui_params)
- 环境变量文档 (get_documented_env_vars)
"""
from __future__ import annotations

import copy
import logging
import os
from typing import Any, Dict, List, Optional

# P1-2-FIX: 移除顶层DEFAULT_PARAM_TABLE导入（未使用且造成双向循环依赖）
# DEFAULT_PARAM_TABLE在此模块中未被任何函数引用

# P1-2-FIX: 将所有config_params顶层导入改为函数内延迟导入，彻底打破循环依赖
# yaml_safe_load / _REQUIRED_CONFIG_KEYS / _OPTIONAL_BUT_RECOMMENDED_KEYS / _DOCUMENTED_ENV_VARS
# 均在各自函数内部按需导入

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

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
            # P1-2-FIX: 延迟导入yaml_safe_load，打破循环依赖
            try:
                from ali2026v3_trading.config.config_params import yaml_safe_load as _yaml_safe_load
            except ImportError:
                _yaml_safe_load = None
            with open(_yaml_path, "r", encoding="utf-8") as _f:
                _data = _yaml_safe_load(_f) if _yaml_safe_load else yaml.safe_load(_f)
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



def validate_config_schema(config: Dict[str, Any]) -> Dict[str, Any]:
    """R13-P1-CFG-09修复: 验证配置字典是否包含必需的key

    SER-P1-16修复: 增强类型校验 — 对每个配置项检查类型
    (int/float/str/bool/list/dict)，类型不匹配则记录WARNING。

    Args:
        config: 待验证的配置字典

    Returns:
        Dict: 验证结果 {
            'valid': bool,
            'missing_required': List[str],
            'missing_recommended': List[str],
            'total_keys': int,
            'type_warnings': List[str],
        }
    """
    # P1-2-FIX: 延迟导入_REQUIRED_CONFIG_KEYS和_OPTIONAL_BUT_RECOMMENDED_KEYS
    try:
        from ali2026v3_trading.config.config_params import _REQUIRED_CONFIG_KEYS, _OPTIONAL_BUT_RECOMMENDED_KEYS
    except ImportError:
        _REQUIRED_CONFIG_KEYS = set()
        _OPTIONAL_BUT_RECOMMENDED_KEYS = set()

    if not isinstance(config, dict):
        return {
            'valid': False,
            'missing_required': list(_REQUIRED_CONFIG_KEYS),
            'missing_recommended': list(_OPTIONAL_BUT_RECOMMENDED_KEYS),
            'total_keys': 0,
            'error': 'config is not a dict',
            'type_warnings': [],
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

    # SER-P1-16修复: 增强类型校验 — 对每个配置项检查类型
    _VALID_SCALAR_TYPES = (int, float, str, bool, list, dict)
    _type_warnings = []
    for _key, _value in config.items():
        if _value is None:
            continue  # None值允许
        if not isinstance(_value, _VALID_SCALAR_TYPES):
            _type_warnings.append(
                f"配置项 '{_key}' 类型异常: {type(_value).__name__} (期望int/float/str/bool/list/dict)"
            )
            logging.warning(
                "SER-P1-16: 配置项 '%s' 类型不匹配: %s (期望int/float/str/bool/list/dict)",
                _key, type(_value).__name__,
            )
        # 递归检查嵌套dict的值类型
        if isinstance(_value, dict):
            for _sub_key, _sub_value in _value.items():
                if _sub_value is None:
                    continue
                if not isinstance(_sub_value, _VALID_SCALAR_TYPES):
                    _type_warnings.append(
                        f"配置项 '{_key}.{_sub_key}' 类型异常: {type(_sub_value).__name__}"
                    )
                    logging.warning(
                        "SER-P1-16: 配置项 '%s.%s' 类型不匹配: %s",
                        _key, _sub_key, type(_sub_value).__name__,
                    )

    return {
        'valid': len(missing_required) == 0,
        'missing_required': missing_required,
        'missing_recommended': missing_recommended,
        'total_keys': len(config_keys),
        'type_warnings': _type_warnings,
    }



def get_documented_env_vars() -> Dict[str, Dict[str, str]]:
    """R13-P1-CFG-10修复: 返回系统使用的所有环境变量文档

    环境变量命名规则: PARAM_OVERRIDE_<KEY> (大写)
    优先级：环境变量 > JSON配置 > YAML参数 > 代码默认值

    Returns:
        Dict: 环境变量键-> {description, default, type}
    """
    # P1-2-FIX: 延迟导入_DOCUMENTED_ENV_VARS
    try:
        from ali2026v3_trading.config.config_params import _DOCUMENTED_ENV_VARS
    except ImportError:
        _DOCUMENTED_ENV_VARS = {}
    return dict(_DOCUMENTED_ENV_VARS)



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

