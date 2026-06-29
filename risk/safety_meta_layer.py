# MODULE_ID: M1-222

# [M1-40] __ȫԪ______߼_

"""

安全元层模块 _从risk_circuit_breaker.py拆分

职责: SafetyMetaLayer核心逻辑（日回撤硬停止、断路器、保证金追保存

"""

from __future__ import annotations



import logging

import time

from typing import Any, Dict, Optional





class SafetyMetaLayer:

    def __init__(self, params: Optional[Dict[str, Any]] = None, strategy_id: str = 'global'):

        self._params = params or {}

        self._strategy_id = strategy_id

        self._daily_start_equity: float = 0.0

        self._hard_stop_triggered: bool = False

        self._daily_resume_confirmed: bool = False

        self._circuit_breaker_active: bool = False

        self._circuit_breaker_until: Optional[float] = None



    def set_daily_start_equity(self, equity: float) -> None:

        self._daily_start_equity = equity

        self._hard_stop_triggered = False

        self._daily_resume_confirmed = False



    def check_daily_drawdown(self, current_equity: float) -> bool:

        if self._daily_start_equity <= 0:

            return False

        hard_stop_pct = self._params.get('daily_loss_hard_stop_pct', 0.05)

        dd = (self._daily_start_equity - current_equity) / self._daily_start_equity

        if dd >= hard_stop_pct:

            self._hard_stop_triggered = True

            return True

        return False



    def is_hard_stop_triggered(self) -> bool:

        return self._hard_stop_triggered



    def confirm_daily_resume(self, caller_id: str = "unknown") -> bool:

        auto_callers = frozenset({'timer', 'scheduler', 'auto', 'cron', 'periodic', 'heartbeat'})

        if caller_id.lower() in auto_callers:

            logging.warning("[SafetyMetaLayer] 自动来源'%s'调用confirm_daily_resume，已拒绝", caller_id)

            return False

        self._hard_stop_triggered = False

        self._daily_resume_confirmed = True

        return True



    def activate_circuit_breaker(self, pause_sec: float = 180.0) -> None:

        self._circuit_breaker_active = True

        self._circuit_breaker_until = time.time() + pause_sec



    def is_circuit_breaker_active(self) -> bool:

        if self._circuit_breaker_active and self._circuit_breaker_until is not None:

            if time.time() >= self._circuit_breaker_until:

                self._circuit_breaker_active = False

        return self._circuit_breaker_active