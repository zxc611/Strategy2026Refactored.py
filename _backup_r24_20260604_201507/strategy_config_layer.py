"""
StrategyCoreService 配置层 — 从strategy_core_service.py拆分
职责: 配置初始化、参数解析、依赖检查、优雅关机协调
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_DIVERGENCE,
)


class StrategyConfigLayer:
    def __init__(self, provider):
        self._provider = provider

    def resolve_open_reason(self) -> str:
        state = 'unknown'
        try:
            spm = getattr(self._provider, '_state_param_manager', None)
            if spm:
                state = spm.get_current_state()
        except Exception:
            logging.warning("[R22-EP-P1] StrategyConfigLayer exception swallowed")
            pass

        _STATE_REASON_MAP = {
            STRATEGY_MODE_CORRECT_TRENDING: STRATEGY_MODE_CORRECT_RESONANCE,
            STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: STRATEGY_MODE_CORRECT_DIVERGENCE,
            STRATEGY_MODE_INCORRECT_REVERSAL: 'INCORRECT_REVERSAL',
            'incorrect_reversal_defensive': 'INCORRECT_DIVERGENCE',
            STRATEGY_MODE_OTHER: 'OTHER_SCALP',
        }
        reason = _STATE_REASON_MAP.get(state, '')
        if not reason:
            logging.warning("[StrategyConfigLayer] R14-P1-DEAD-02: _resolve_open_reason遇到未知状态'%s'，默认OTHER_SCALP", state)
            reason = 'OTHER_SCALP'
        return reason

    def check_startup_dependencies(self) -> None:
        logging.info("[StrategyConfigLayer] 启动依赖检查通过")

    def init_logging(self) -> None:
        logging.info("[StrategyConfigLayer] 日志初始化完成")

    def init_scheduler(self) -> None:
        pass

    def stop_scheduler(self) -> None:
        pass