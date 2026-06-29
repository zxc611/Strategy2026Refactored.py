# MODULE_ID: M1-249
"""box_spring_executor_helpers.py — 弹簧执行器辅助函数

提供 _compute_hedge_ratio, _check_cross_strategy_risk,
_record_spring_trade, _find_straddle_pair 的默认实现。
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


def _compute_hedge_ratio(self_or_signal, signal=None) -> float:
    """计算对冲比率（默认1.0，即全额对冲）

    支持两种调用方式:
    - 作为模块函数: _compute_hedge_ratio(signal)
    - 作为实例方法: self._compute_hedge_ratio(signal)
    """
    return 1.0


def _check_cross_strategy_risk(self_or_signal, signal=None) -> bool:
    """检查跨策略风险（默认通过）

    支持两种调用方式:
    - 作为模块函数: _check_cross_strategy_risk(signal)
    - 作为实例方法: self._check_cross_strategy_risk(signal)
    """
    return True


def _record_spring_trade(self_or_signal, signal=None) -> Dict[str, Any]:
    """记录弹簧交易（默认空记录）

    支持两种调用方式:
    - 作为模块函数: _record_spring_trade(signal)
    - 作为实例方法: self._record_spring_trade(signal)
    """
    actual_signal = signal if signal is not None else self_or_signal
    return {"recorded": True, "signal_id": actual_signal.get("signal_id", "unknown")}


def _find_straddle_pair(self_or_signal, signal=None) -> Tuple[str, float, str]:
    """查找跨式配对（默认返回信号自身信息）

    支持两种调用方式:
    - 作为模块函数: _find_straddle_pair(signal)
    - 作为实例方法: self._find_straddle_pair(signal)
    """
    actual_signal = signal if signal is not None else self_or_signal
    instrument_id = actual_signal.get("instrument_id", "")
    premium = actual_signal.get("premium", 0.0)
    opt_type = actual_signal.get("option_type", "CALL")
    return instrument_id, premium, opt_type
