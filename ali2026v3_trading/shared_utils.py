"""
ali2026v3_trading 共享工具函数模块

✅ ID唯一 / 方法唯一 / 传递渠道唯一：跨模块共享的工具函数统一定义在此
禁止在其他模块中重复定义这些函数，必须从此模块导入。

统一管理的函数：
- normalize_option_type: 期权类型标准化
- to_float32: float32精度转换
- normalize_instrument_id: 合约ID标准化（移除交易所前缀）
- extract_product_code: 从合约ID提取品种代码
- extract_strike_price: 从期权合约ID提取行权价
"""

import struct
from typing import Optional


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
    return upper


def to_float32(value) -> float:
    """将数值转换为float32精度（唯一实现）

    Args:
        value: 输入数值

    Returns:
        float: float32精度的数值
    """
    try:
        return struct.unpack('f', struct.pack('f', float(value)))[0]
    except (TypeError, ValueError, OverflowError):
        return 0.0


def normalize_instrument_id(instrument_id: str) -> str:
    """移除交易所前缀和平台前缀，统一合约ID格式（唯一实现）

    Args:
        instrument_id: 可能含交易所前缀或平台前缀的合约ID (如 "DCE.m2605" 或 "platform|m2605")

    Returns:
        str: 标准化后的合约ID (如 "m2605")
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
    # 提取前导字母部分
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
