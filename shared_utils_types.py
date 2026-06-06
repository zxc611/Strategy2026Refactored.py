"""shared_utils_types — 类型转换相关 (R27-CP-04-FIX: 从shared_utils拆分)

安全类型转换、精度控制、numpy原生类型转换
"""

import logging
import math
import struct
from typing import Any

__all__ = ['to_float32', 'to_native_type', 'safe_int', 'safe_float', 'safe_price_check']


def to_float32(value) -> float:
    """将数值转换为float32精度（唯一实现）

    NP-P1-12修复: None/NaN/inf显式告警而非静默返回0.0

    Args:
        value: 输入数值

    Returns:
        float: float32精度的数值
    """
    _log = logging.getLogger(__name__)
    if value is None:
        _log.warning("[to_float32] None输入，返回0.0")
        return 0.0
    _MAX_FLOAT32 = 3.4028235e+38
    if abs(value) > _MAX_FLOAT32:
        _log.warning("[NP-P2-30] value %.2e exceeds float32 range, clamping", value)
        value = _MAX_FLOAT32 if value > 0 else -_MAX_FLOAT32
    try:
        result = struct.unpack('f', struct.pack('f', float(value)))[0]
    except (TypeError, ValueError, OverflowError):
        _log.warning("[to_float32] 转换失败，value=%r，返回0.0", value)
        return 0.0
    if math.isnan(result) or math.isinf(result):
        _log.warning("[to_float32] 结果为NaN/inf，value=%r，返回0.0", value)
        return 0.0
    return result


def safe_int(value: Any, default: int = 0) -> int:
    """安全转换为整数"""
    try:
        if value is None:
            return default
        return round(float(value))  # [R22-TS-P1-04] 改用round避免截断
    except (ValueError, TypeError) as e:
        logging.warning(f"[safe_int] Conversion failed for value '{value}': {e}")
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转换为浮点数"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError) as e:
        logging.warning(f"[safe_float] Conversion failed for value '{value}': {e}")
        return default


def safe_price_check(price) -> bool:
    """R24-P1-IV-10修复: 安全价格检查，NaN/Inf/非正数均返回False"""
    try:
        return math.isfinite(price) and price > 0
    except (TypeError, ValueError):
        return False


def to_native_type(obj):
    """R5-I-04修复: 将numpy类型转换为Python原生类型

    np.int64不是int的子类，np.float64不是float的子类，
    在字典操作和JSON序列化时可能失败。此函数递归转换所有numpy类型。

    Args:
        obj: 任意Python对象

    Returns:
        转换后的Python原生类型对象
    """
    try:
        import numpy as np
        if isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, (np.bool_,)):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {to_native_type(k): to_native_type(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return type(obj)(to_native_type(v) for v in obj)
    except ImportError:
        pass
    return obj
