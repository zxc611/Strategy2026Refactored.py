"""
order_sync.py - 订单状态同步
R27: 从order_service.py提取的订单状态与交易所同步函数

职责：
- 订单状态与交易所同步 (sync_order_status_with_exchange)
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict


def sync_order_status_with_exchange(order_id: str, local_status: str, exchange_status_query_fn: Callable) -> Dict[str, Any]:
    """R26-P0-DI-05修复: 订单状态与交易所同步——比较本地订单状态与交易所实际状态

    Args:
        order_id: 订单ID
        local_status: 本地记录的订单状态
        exchange_status_query_fn: 查询交易所状态的回调函数, 签名(order_id)->str

    Returns:
        Dict: {synced: bool, local_status: str, exchange_status: str, action_taken: str}
    """
    result = {'synced': True, 'local_status': local_status, 'exchange_status': '', 'action_taken': 'none'}
    try:
        exchange_status = exchange_status_query_fn(order_id) if exchange_status_query_fn else None
        if exchange_status is None:
            result['action_taken'] = 'query_failed'
            logging.warning("[R26-P0-DI-05] 订单%s交易所状态查询返回None", order_id)
            return result
        result['exchange_status'] = str(exchange_status)
        if local_status != exchange_status:
            result['synced'] = False
            if local_status in ('PENDING', 'SUBMITTED') and exchange_status in ('FILLED', 'PARTIAL_FILLED'):
                result['action_taken'] = 'local_updated_to_exchange'
                logging.warning("[R26-P0-DI-05] 订单%s本地%s但交易所%s, 需更新本地状态", order_id, local_status, exchange_status)
            elif local_status in ('FILLED', 'PARTIAL_FILLED') and exchange_status == 'CANCELLED':
                result['action_taken'] = 'exchange_override_detected'
                logging.error("[R26-P0-DI-05] 订单%s本地%s但交易所已撤单, 严重不一致", order_id, local_status)
            else:
                result['action_taken'] = 'status_mismatch_logged'
                logging.warning("[R26-P0-DI-05] 订单%s状态不一致: 本地=%s, 交易所=%s", order_id, local_status, exchange_status)
    except Exception as e:
        result['action_taken'] = 'exception'
        logging.error("[R26-P0-DI-05] 订单%s同步异常: %s", order_id, e)
    return result
