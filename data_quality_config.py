"""
Phase3-Sprint8: data_quality_config.py — 从config_params.py提取的数据质量/清洗配置
包含: _MISSING_VALUE_DEFAULTS, DATA_CLEANING_RULES, DATA_EXPORT_FORMAT, _DATA_SOURCE_TAG_FIELD
"""
from __future__ import annotations

_MISSING_VALUE_DEFAULTS = {
    'last_price': 0.0,
    'volume': 0,
    'open_interest': 0.0,
    'bid_price': 0.0,
    'ask_price': 0.0,
    'spread_quality': 0.0,
    'days_to_expiry': 999,
    'implied_volatility': 0.0,
}

DATA_CLEANING_RULES = {
    'remove_zero_price': True,
    'remove_negative_volume': True,
    'clamp_open_interest': True,
    'remove_future_timestamp': True,
    'max_price_change_pct': 20.0,
    'max_spread_pct': 5.0,
    'fill_method': 'ffill',
}

DATA_EXPORT_FORMAT = 'parquet'

_DATA_SOURCE_TAG_FIELD = '_data_source'