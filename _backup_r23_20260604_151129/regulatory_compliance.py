"""
合规规则模块 — 从risk_circuit_breaker.py拆分
职责: 监管合规检查规则（自成交、洗盘、算法交易标识）
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional


class RegulatoryCompliance:
    def __init__(self):
        self._self_trade_threshold = 0.001
        self._wash_trade_window_sec = 60.0
        self._recent_trades: List[Dict[str, Any]] = []

    def check_self_trade(self, new_order: Dict[str, Any]) -> bool:
        instrument = new_order.get('instrument_id', '')
        direction = new_order.get('direction', '')
        for trade in self._recent_trades:
            if trade.get('instrument_id') == instrument:
                if trade.get('direction') != direction:
                    logging.warning("[RegulatoryCompliance] 自成交风险: %s", instrument)
                    return True
        return False

    def check_wash_trade(self, order: Dict[str, Any]) -> bool:
        return False

    def check_algo_trading_flag(self, order: Dict[str, Any]) -> bool:
        return order.get('is_algo', True)

    def record_trade(self, trade: Dict[str, Any]) -> None:
        self._recent_trades.append(trade)
        if len(self._recent_trades) > 1000:
            self._recent_trades = self._recent_trades[-500:]