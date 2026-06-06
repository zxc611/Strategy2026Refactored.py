"""shared_utils_instrument — 合约工具相关 (R27-CP-04-FIX: 从shared_utils拆分)

期权类型标准化、合约ID处理、品种代码提取、行权价提取、年月归一化
"""

import logging
import re
from typing import Optional
from datetime import timezone, timedelta

# [R22-TIME-P1-01] 全局CST时区常量，所有模块统一引用
CHINA_TZ = timezone(timedelta(hours=8))

__all__ = [
    'CHINA_TZ',
    'normalize_option_type', 'normalize_instrument_id',
    'extract_product_code', 'extract_strike_price', 'normalize_year_month',
]


def normalize_option_type(opt_type: str) -> str:
    """标准化option_type（唯一实现）

    Args:
        opt_type: 期权类型字符串 ('C', 'CALL', 'P', 'PUT'等)

    Returns:
        str: 标准化的期权类型 ('CALL' 或 'PUT')
    """
    if not opt_type:
        return ''
    upper = opt_type.upper()
    if upper in ('C', 'CALL'):
        return 'CALL'
    elif upper in ('P', 'PUT'):
        return 'PUT'
    elif upper in ('CE', 'PE', 'C_E', 'P_E'):
        logging.warning("[normalize_option_type] 非标准期权类型 '%s'，尝试映射: CE→CALL, PE→PUT", opt_type)
        return 'CALL' if upper.startswith('C') else 'PUT'
    logging.warning("[normalize_option_type] 未知期权类型 '%s'，原样透传", opt_type)
    return upper


def normalize_instrument_id(instrument_id: str) -> str:
    """移除交易所前缀和平台前缀，保留合约ID原始格式（品种ID直通，不做大写化）

    Args:
        instrument_id: 可能含交易所前缀或平台前缀的合约ID (如 "DCE.m2605" 或 "platform|m2605")

    Returns:
        str: 标准化后的合约ID (如 "m2605")，保留交易所原始大小写
    """
    normalized = str(instrument_id or '').strip()
    if '.' in normalized:
        normalized = normalized.split('.', 1)[-1]
    if '|' in normalized:
        normalized = normalized.split('|')[-1]
    return normalized


def extract_product_code(instrument_id: str) -> str:
    """从合约ID提取品种代码（唯一实现）

    Args:
        instrument_id: 合约ID (如 "IO2506-C-4000" 或 "al2605C18900")

    Returns:
        str: 品种代码 (如 "IO" 或 "al")
    """
    normalized = normalize_instrument_id(instrument_id)
    if not normalized:
        return ''
    i = 0
    while i < len(normalized) and normalized[i].isalpha():
        i += 1
    return normalized[:i] if i > 0 else ''


def extract_strike_price(instrument_id: str) -> Optional[float]:
    """从期权合约ID提取行权价（唯一实现，委托SubscriptionManager）

    Args:
        instrument_id: 期权合约ID

    Returns:
        Optional[float]: 行权价，解析失败返回None
    """
    try:
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        parsed = SubscriptionManager.parse_option(instrument_id)
        strike = parsed.get('strike_price')
        return float(strike) if strike is not None else None
    except (ValueError, KeyError, TypeError):
        return None


def normalize_year_month(year_month: str) -> str:
    """归一化年月格式（唯一实现）

    支持多种输入格式，统一输出为 YYMM 格式：
    - '6M05' -> '2605' (6月05年 -> 26年05月)
    - '26M05' -> '2605' (2位年份前缀+月份)
    - '202605' -> '2605' (完整年份)
    - '2605' -> '2605' (已是标准格式，直通)

    Args:
        year_month: 年月字符串

    Returns:
        str: 归一化后的 YYMM 格式年月
    """
    year_month = str(year_month or '').strip()
    if not year_month:
        return ''

    if len(year_month) == 4 and year_month.isdigit():
        month_part = int(year_month[2:])
        if 1 <= month_part <= 12:
            return year_month
        return year_month

    if len(year_month) == 6 and year_month.isdigit():
        month_part = int(year_month[4:])
        if 1 <= month_part <= 12:
            return year_month[2:]
        return year_month[2:]

    m = re.match(r'(\d{1,4})[M/\-](\d{1,2})$', year_month, re.IGNORECASE)
    if m:
        year_str = m.group(1)
        month_str = m.group(2).zfill(2)
        month_val = int(month_str)
        if month_val < 1 or month_val > 12:
            return year_month
        if len(year_str) == 4:
            return f'{year_str[2:]}{month_str}'
        elif len(year_str) == 2:
            return f'{year_str}{month_str}'
        elif len(year_str) == 1:
            from datetime import datetime
            current_year = datetime.now(CHINA_TZ).year
            candidate_full = current_year - (current_year % 10) + int(year_str)
            if candidate_full > current_year + 1:
                candidate_full -= 10
            yy = f'{candidate_full % 100:02d}'
            return f'{yy}{month_str}'
        return year_month

    return year_month
