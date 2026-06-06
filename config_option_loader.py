"""
Phase3-Sprint8: config_option_loader.py — 从config_params.py提取的期权参数加载逻辑
包含: _normalize_option_params_payload, load_option_params_from_file, merge_option_params_to_default
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

from ali2026v3_trading.shared_utils import safe_int


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
    possible_paths = [
        'auto_generated_option_params.json',
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'auto_generated_option_params.json'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'auto_generated_option_params.json'),
    ]
    for file_path in possible_paths:
        try:
            from ali2026v3_trading.security_config import _validate_path_safety
            file_path = _validate_path_safety(file_path)
        except ValueError as ve:
            logging.warning("[config_option_loader] 路径安全验证失败: %s", ve)
            continue
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                data = _normalize_option_params_payload(raw_data, file_path)
                if not data:
                    logging.warning("[config_option_loader] 期权参数文件未提取到有效品种配置: %s", file_path)
                    continue
                logging.info("[config_option_loader] 成功加载期权参数配置: %s", file_path)
                logging.info("[config_option_loader] 包含 %d 个品种的期权参数", len(data))
                total_strikes = sum(safe_int((p or {}).get('strike_count', 0) or 0) for p in data.values())
                logging.info("[config_option_loader] 总行权价组合数 %d", total_strikes)
                return data
            except json.JSONDecodeError as e:
                logging.error("[config_option_loader] 期权参数文件 JSON 解析失败: %s - %s", file_path, e)
            except Exception as e:
                logging.error("[config_option_loader] 加载期权参数文件失败: %s - %s", file_path, e)
    logging.warning("[config_option_loader] 未找到auto_generated_option_params.json，使用默认期权参数")
    logging.warning("[R24-P2-DF-09] 期权参数为空，期权策略可能使用模块内默认值")
    return {}


def merge_option_params_to_default():
    from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE, _param_table_lock, _OPTION_PARAM_RUNTIME_PATCH
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
                logging.info("[config_option_loader] 已合并期权品种列表 %d 个品种", len(products_from_file))
                logging.info("[config_option_loader] 最终期权品种 %s", _OPTION_PARAM_RUNTIME_PATCH['option_products'])