"""Position Utility Functions - 模块级辅助函数

从position_service.py拆分(CC-09 Step4):
- _calc_max_volume_from_capital: 计算最大可用手数
- _cancel_and_resend: 取消并重新发单
- _check_available_amount: 检查可用数量
- reconcile_positions_with_exchange: 持仓对账
"""
from __future__ import annotations

import logging
from typing import Dict, Any


def _calc_max_volume_from_capital(capital: float, price: float, leverage: float = 1.0, margin_rate: float = 0.15) -> int:
    try:
        if capital <= 0 or price <= 0:
            return 0
        margin_per_lot = price * margin_rate
        if margin_per_lot <= 0:
            return 0
        return int((capital / margin_per_lot) // 1)
    except Exception:
        return 0


def _cancel_and_resend(order_id: str, new_params: Dict[str, Any]) -> bool:
    try:
        from ali2026v3_trading.order_service import get_order_service
        order_svc = get_order_service()
        if not order_svc.cancel_order(order_id):
            return False
        try:
            from ali2026v3_trading.position_service import get_position_service
            _ps = get_position_service()
            if _ps and _ps.self_trade_detector is not None:
                _ps.self_trade_detector.remove_order(order_id)
        except Exception:
            pass
        if 'signal_id' not in new_params:
            new_params['signal_id'] = f"RESUBMIT_{order_id}"
        new_order_id = order_svc.send_order(**new_params)
        return bool(new_order_id) and new_order_id.success
    except Exception as e:
        logging.error("[position_service._cancel_and_resend] Failed: %s: %s", order_id, e)
        return False


def _check_available_amount(position_data: Dict[str, Any], amount: int) -> bool:
    try:
        if not position_data:
            return False
        return int(position_data.get('available', 0)) >= amount
    except Exception:
        return False


def reconcile_positions_with_exchange(local_positions: Dict[str, Any], exchange_positions: Dict[str, Any]) -> Dict[str, Any]:
    """R26-P0-DI-01: 持仓对账"""
    if local_positions is None:
        local_positions = {}
    if exchange_positions is None:
        exchange_positions = {}
    result = {'is_matched': True, 'diffs': [], 'local_only': [], 'exchange_only': []}
    all_keys = set(local_positions.keys()) | set(exchange_positions.keys())
    for _key in all_keys:
        _local = local_positions.get(_key)
        _exchange = exchange_positions.get(_key)
        if _local is None and _exchange is not None:
            result['exchange_only'].append(_key)
            result['is_matched'] = False
        elif _local is not None and _exchange is None:
            result['local_only'].append(_key)
            result['is_matched'] = False
        elif _local is not None and _exchange is not None:
            _lv = _local.get('volume', 0) if isinstance(_local, dict) else 0
            _ev = _exchange.get('volume', 0) if isinstance(_exchange, dict) else 0
            if _lv != _ev:
                result['diffs'].append({'instrument_id': _key, 'local_volume': _lv, 'exchange_volume': _ev})
                result['is_matched'] = False
    if not result['is_matched']:
        logging.warning("[R26-P0-DI-01] 持仓对账不一致: diffs=%d", len(result['diffs']))
    return result
