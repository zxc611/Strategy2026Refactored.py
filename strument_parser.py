# MODULE_ID: M1-119
"""
instrument_parser.py - 合约解析工具（基础设施层）

职责：
- 提供合约ID解析的纯函数式API
- 支持期货/期权多种格式
- 无状态，无业务依赖

提取自 SubscriptionInstrumentService (2026-06-30)
"""

from __future__ import annotations
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from ali2026v3_trading.infra.shared_utils import normalize_instrument_id, CHINA_TZ


__all__ = [
    'strip_exchange_prefix',
    'normalize_product_code',
    'parse_future',
    'parse_option',
    'is_option',
    'normalize_option_year_month',
    'classify_instruments',
]


def strip_exchange_prefix(instrument_id: str) -> str:
    """剥离交易所前缀（A-02修复：统一使用normalize_instrument_id）
    
    Args:
        instrument_id: 合约ID（可能带交易所前缀，如 'CFFEX.IF2603'）
    Returns:
        str: 纯净的合约ID（如 'IF2603'）
    """
    from ali2026v3_trading.infra.shared_utils import normalize_instrument_id
    return normalize_instrument_id(instrument_id)


def normalize_product_code(product: str) -> str:
    """标准化产品代码，用于内部比较和分析
    
    Args:
        product: 产品代码字符串
    Returns:
        str: 标准化的产品代码（SHFE品种小写，其他大写）
    """
    if not product:
        return ''
    return str(product)


def parse_future(instrument_id: str) -> Dict[str, Any]:
    """解析期货合约
    
    Args:
        instrument_id: 期货合约ID
    Returns:
        Dict: {product, year_month}
    Raises:
        ValueError: 无法解析时抛出
    """
    clean_id = strip_exchange_prefix(instrument_id)
    match = re.match(r'^([A-Za-z]+)(\d{3,4})$', clean_id)
    if not match:
        raise ValueError(f"无法解析期货：{instrument_id}")
    
    raw_product = match.group(1)
    year_month = match.group(2)
    if len(year_month) == 3:
        year_month = '2' + year_month
    
    product = normalize_product_code(raw_product)
    return {'product': product, 'year_month': year_month}


def parse_option(instrument_id: str) -> Dict[str, Any]:
    r"""解析期权合约(支持标准格式、连字符格式、紧凑格式、MS迷你格式)
    
    支持格式:
    - 标准: CU2603C5000, IO2606C4000
    - 连字符: CU2603-C-5000
    - CZCE迷你: SR607MSC4700, SR607MSP4700 (MSC=Mini Call, MSP=Mini Put)
    - DCE迷你: c2607-MS-C-2040, m2607-MS-P-2400
    
    Args:
        instrument_id: 期权合约ID
    Returns:
        Dict: {product, year_month, option_type, strike_price, format}
    Raises:
        ValueError: 无法解析时抛出
    """
    clean_id = strip_exchange_prefix(instrument_id)
    
    # MS迷你格式（紧凑）
    ms_compact = re.match(r'^([A-Za-z]+)(\d{3,4})MS([CP])(\d+)$', clean_id)
    if ms_compact:
        product = ms_compact.group(1)
        year_month_raw = ms_compact.group(2)
        option_type = ms_compact.group(3)
        strike_price = float(ms_compact.group(4))
        year_month = normalize_option_year_month(year_month_raw)
        return {
            'product': product,
            'year_month': year_month,
            'option_type': option_type,
            'strike_price': strike_price,
            'format': 'ms_compact',
        }
    
    # MS迷你格式（连字符）
    ms_dash = re.match(r'^([A-Za-z]+)(\d{3,4})-MS-([CP])-?(\d+(?:\.\d+)?)$', clean_id)
    if ms_dash:
        product = ms_dash.group(1)
        year_month_raw = ms_dash.group(2)
        option_type = ms_dash.group(3)
        strike_price = float(ms_dash.group(4))
        year_month = normalize_option_year_month(year_month_raw)
        return {
            'product': product,
            'year_month': year_month,
            'option_type': option_type,
            'strike_price': strike_price,
            'format': 'ms_dash',
        }
    
    # 标准格式/连字符格式（A-03修复：支持Call/Put全称格式）
    # 先尝试Call/Put全称格式
    match_call_put = re.match(r'^([A-Za-z]+)(\d{3,4})-?(Call|Put)-?(\d+(?:\.\d+)?)$', clean_id, re.IGNORECASE)
    if match_call_put:
        product = match_call_put.group(1)
        year_month_raw = match_call_put.group(2)
        option_type_raw = match_call_put.group(3)
        option_type = 'C' if option_type_raw.upper() == 'CALL' else 'P'
        strike_price = float(match_call_put.group(4))
        year_month = normalize_option_year_month(year_month_raw)
        fmt = 'dash' if '-' in clean_id else 'compact'
        return {
            'product': product,
            'year_month': year_month,
            'option_type': option_type,
            'strike_price': strike_price,
            'format': fmt,
        }
    
    # 再尝试C/P单字母格式
    match = re.match(r'^([A-Za-z]+)(\d{3,4})-?([CP])-?(\d+(?:\.\d+)?)$', clean_id)
    if match:
        product = match.group(1)
        year_month_raw = match.group(2)
        option_type = match.group(3)
        strike_price = float(match.group(4))
        year_month = normalize_option_year_month(year_month_raw)
        fmt = 'dash' if '-' in clean_id else 'compact'
        return {
            'product': product,
            'year_month': year_month,
            'option_type': option_type,
            'strike_price': strike_price,
            'format': fmt,
        }
    
    raise ValueError(f"无法解析期权: {instrument_id}")


def is_option(instrument_id: str) -> bool:
    """判断是否为期权
    
    Args:
        instrument_id: 合约ID
    Returns:
        bool: True为期权，False为期货或其他
    """
    try:
        parse_option(normalize_instrument_id(instrument_id))
        return True
    except (ValueError, Exception):
        return False


def normalize_option_year_month(year_month_raw: str) -> str:
    """归一化期权年月
    
    Args:
        year_month_raw: 原始年月编码（如 '607'）
    Returns:
        str: 4位年月（如 '2607'）
    Raises:
        ValueError: 非法编码时抛出
    """
    normalized = normalize_instrument_id(year_month_raw)
    if len(normalized) == 4:
        return normalized
    
    if len(normalized) != 3 or not normalized.isdigit():
        raise ValueError(f"非法期权月份编码：{year_month_raw}")
    
    current_year = datetime.now(CHINA_TZ).year % 100
    year_digit = int(normalized[0])
    month_digits = normalized[1:]
    current_decade = (current_year // 10) * 10
    candidate_years = []
    for decade_offset in (-10, 0, 10):
        candidate_year = current_decade + decade_offset + year_digit
        if 0 <= candidate_year <= 99:
            candidate_years.append(candidate_year)
    resolved_year = min(candidate_years, key=lambda year: (abs(year - current_year), -year))
    return f"{resolved_year:02d}{month_digits}"


def classify_instruments(instrument_ids: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
    """分类合约列表
    
    Args:
        instrument_ids: 合约ID列表
    Returns:
        Tuple: (futures_list, options_dict)
        - futures_list: 期货合约列表
        - options_dict: 期权合约字典，key为underlying标识(product+year_month)
    
    Example:
        >>> classify_instruments(['IF2603', 'CU2603-C-5000'])
        (['IF2603'], {'CU2603': ['CU2603-C-5000']})
    """
    futures_list = []
    options_dict = {}
    for inst_id in instrument_ids:
        try:
            parsed = parse_option(inst_id)
            underlying = f"{parsed['product']}{parsed['year_month']}"
            if underlying not in options_dict:
                options_dict[underlying] = []
            options_dict[underlying].append(inst_id)
        except ValueError:
            futures_list.append(inst_id)
    return futures_list, options_dict