"""
策略配置模块 — 从config_params.py拆分
职责: 策略模式常量、策略参数表(策略相关部分)、策略参数缓存管理
"""
from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Any, Dict, List, Optional

STRATEGY_MODE_CORRECT_TRENDING = "correct_trending"
STRATEGY_MODE_INCORRECT_REVERSAL = "incorrect_reversal"
STRATEGY_MODE_SPRING = "spring"
STRATEGY_MODE_RESONANCE = "resonance"
STRATEGY_MODE_BOX = "box"
STRATEGY_MODE_HFT = "hft"
STRATEGY_MODE_BOX_EXTREME = "box_extreme"
STRATEGY_MODE_BOX_SPRING = "box_spring"
STRATEGY_MODE_ARBITRAGE = "arbitrage"
STRATEGY_MODE_MARKET_MAKING = "market_making"
STRATEGY_MODE_HIGH_FREQ = "high_freq"

ALL_STRATEGY_MODES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_SPRING,
    STRATEGY_MODE_RESONANCE,
    STRATEGY_MODE_BOX,
    STRATEGY_MODE_HFT,
)

STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE = "correct_trending_defensive"
STRATEGY_MODE_OTHER = "other"
STRATEGY_MODE_CORRECT_RESONANCE = "CORRECT_RESONANCE"
STRATEGY_MODE_CORRECT_DIVERGENCE = "CORRECT_DIVERGENCE"

ALL_MARKET_STATES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_DIVERGENCE,
)

_STATE_REASON_MAP = {
    STRATEGY_MODE_CORRECT_TRENDING: STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: STRATEGY_MODE_CORRECT_DIVERGENCE,
    STRATEGY_MODE_INCORRECT_REVERSAL: 'INCORRECT_REVERSAL',
    'incorrect_reversal_defensive': 'INCORRECT_DIVERGENCE',
    STRATEGY_MODE_OTHER: 'OTHER_SCALP',
}

_strategy_param_caches: Dict[str, Dict[str, Any]] = {}
_strategy_cache_timestamps: Dict[str, float] = {}
_param_age_monitor: Dict[str, float] = {}
_invalidate_idempotent: Dict[str, float] = {}
_INVALIDATE_IDEMPOTENT_SEC = 1.0
_strategy_cache_lock = threading.Lock()


def resolve_open_reason_from_state(state: str) -> str:
    reason = _STATE_REASON_MAP.get(state, '')
    if not reason:
        logging.warning("[strategy_config] 未知状态'%s'，默认OTHER_SCALP", state)
        reason = 'OTHER_SCALP'
    return reason


def get_strategy_param_cache(strategy_id: str) -> Optional[Dict[str, Any]]:
    with _strategy_cache_lock:
        return _strategy_param_caches.get(strategy_id)


def set_strategy_param_cache(strategy_id: str, params: Dict[str, Any]) -> None:
    with _strategy_cache_lock:
        _strategy_param_caches[strategy_id] = params
        _strategy_cache_timestamps[strategy_id] = time.time()


def invalidate_strategy_cache(strategy_id: str) -> bool:
    _now = time.time()
    if strategy_id in _invalidate_idempotent and (_now - _invalidate_idempotent[strategy_id]) < _INVALIDATE_IDEMPOTENT_SEC:
        return False
    _invalidate_idempotent[strategy_id] = _now
    with _strategy_cache_lock:
        if strategy_id in _strategy_param_caches:
            del _strategy_param_caches[strategy_id]
            _strategy_cache_timestamps.pop(strategy_id, None)
            logging.info("[strategy_config] 已清理策略级参数缓存 strategy_id=%s", strategy_id)
            return True
    return False


def get_all_strategy_modes():
    return ALL_STRATEGY_MODES


def get_all_market_states():
    return ALL_MARKET_STATES