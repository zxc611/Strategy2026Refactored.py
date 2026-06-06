"""R27-SML-FIX: 保证金管理子模块 — 从SafetyMetaLayer提取的保证金预留与检查职责"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict


class MarginManager:
    """保证金预留管理器

    负责保证金预留、释放、过期清理及资金充足性检查。
    从SafetyMetaLayer中提取，实现职责分离。
    """

    def __init__(self) -> None:
        self._reserved_margin: float = 0.0
        self._margin_reservations: Dict[str, Dict[str, Any]] = {}
        self._RESERVATION_TTL_SEC: int = 300  # 预留保证金5分钟TTL
        self._lock = threading.RLock()

    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,
                                  open_positions: int = 0, max_positions: int = 50,
                                  existing_margin_used: float = 0.0,
                                  daily_start_equity: float = 0.0) -> Dict[str, Any]:
        with self._lock:
            if equity <= 0:
                return {"sufficient": False, "available_margin": 0.0, "reason": "equity_non_positive"}
            daily_start = daily_start_equity or equity
            self._purge_expired_reservations()
            available = equity - existing_margin_used - required_margin - self._reserved_margin
            if available < daily_start * 0.05:
                return {"sufficient": False, "available_margin": available, "reason": "margin_below_5pct"}
            if open_positions >= max_positions:
                return {"sufficient": False, "available_margin": available, "reason": "position_limit_reached"}
            return {"sufficient": True, "available_margin": available, "reason": "ok"}

    def reserve_margin(self, reservation_id: str, amount: float) -> bool:
        with self._lock:
            self._purge_expired_reservations()
            if amount <= 0:
                return False
            now = time.time()
            self._margin_reservations[reservation_id] = {
                'amount': amount,
                'timestamp': now,
                'expires_at': now + self._RESERVATION_TTL_SEC,
            }
            self._reserved_margin += amount
            logging.debug(
                "[RiskService] R13-P0-BIZ-10: 保证金预留 id=%s amount=%.2f total_reserved=%.2f",
                reservation_id, amount, self._reserved_margin,
            )
            return True

    def release_margin(self, reservation_id: str) -> bool:
        with self._lock:
            reservation = self._margin_reservations.pop(reservation_id, None)
            if reservation:
                self._reserved_margin -= reservation['amount']
                if self._reserved_margin < 0:
                    self._reserved_margin = 0.0
                logging.debug(
                    "[RiskService] R13-P0-BIZ-10: 保证金释放 id=%s amount=%.2f total_reserved=%.2f",
                    reservation_id, reservation['amount'], self._reserved_margin,
                )
                return True
            return False

    def _purge_expired_reservations(self) -> None:
        now = time.time()
        expired_ids = [
            rid for rid, res in self._margin_reservations.items()
            if now >= res['expires_at']
        ]
        for rid in expired_ids:
            res = self._margin_reservations.pop(rid)
            self._reserved_margin -= res['amount']
            logging.debug(
                "[RiskService] R13-P0-BIZ-10: 保证金预留过期自动释放 id=%s amount=%.2f",
                rid, res['amount'],
            )
        if self._reserved_margin < 0:
            self._reserved_margin = 0.0
