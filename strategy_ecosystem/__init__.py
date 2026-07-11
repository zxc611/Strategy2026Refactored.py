# MODULE_ID: M1-269
"""strategy_ecosystem - 策略生态系统包（re-export门面 + 工厂函数）

将拆分后的子模块统一导出，保持与原 strategy_ecosystem.py 完全相同的公开API。
工厂函数和性能缓存（从。factory.py迁入）也在此定义。
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional

from ali2026v3_trading.infra._helpers import get_logger
from ali2026v3_trading.infra.resilience import safe_divide

from ali2026v3_trading.strategy.strategy_ecosystem._models import (
    SlotState,
    VALID_STRATEGY_TRANSITIONS,
    StrategySlot,
    CapitalRoute,
    EcosystemTradeRecord,
    EV_CACHE_TTL_DEFAULTS,
    EV_CACHE_TTL,
    OBSERVATION_PERIOD_SEC,
    _require_interface,
    _require_arg_interface,
)

from ali2026v3_trading.strategy.strategy_ecosystem._core import StrategyEcosystem

_logger = get_logger(__name__)


# ============================================================
# 工厂函数 + 模块级单例 + 性能缓存 (从 _factory.py 迁入)
# ============================================================

def validate_ev_cache_time_decay(ecosystem=None,
                                   half_life_days: float = 5.0,
                                   n_days: int = 365,
                                   reversal_day: int = 180) -> Dict[str, Any]:
    """P1-裂缝22：验证EV缓存在市场反转后是否及时衰减"""
    import numpy as np
    rng = np.random.RandomState(42)

    ev_series = []
    for day in range(n_days):
        if day < reversal_day:
            ev = rng.gauss(0.02, 0.005)
        else:
            ev = rng.gauss(-0.01, 0.005)
        ev_series.append(ev)

    decay_factors = [0.5, 0.7, 0.9, 0.95, 0.99]
    results = {}

    for alpha in decay_factors:
        cached_ev = 0.0
        cached_evs = []
        for ev in ev_series:
            cached_ev = alpha * cached_ev + (1 - alpha) * ev
            cached_evs.append(cached_ev)

        reversal_cached = cached_evs[reversal_day - 1]
        target = reversal_cached * 0.5
        half_life = 0
        for d in range(reversal_day, n_days):
            if cached_evs[d] <= target:
                half_life = d - reversal_day
                break

        results[f"alpha_{alpha}"] = {
            "half_life_days": half_life if half_life > 0 else n_days,
            "reversal_cached_ev": round(reversal_cached, 6),
            "final_cached_ev": round(cached_evs[-1], 6),
        }

    best_alpha = None
    for alpha, r in results.items():
        hl = r["half_life_days"]
        if 0 < hl <= half_life_days:
            best_alpha = alpha
            break

    needs_time_weighting = best_alpha is None

    return {
        "decay_results": results,
        "half_life_threshold_days": half_life_days,
        "recommended_alpha": best_alpha,
        "needs_time_weighting": needs_time_weighting,
        "recommendation": "增加时间加权折扣因子" if needs_time_weighting else f"使用衰减因子{best_alpha}",
    }


# ---------------------------------------------------------------------------
# 模块级单例
# ---------------------------------------------------------------------------

_ecosystem: Optional['StrategyEcosystem'] = None
_ecosystem_lock = threading.Lock()
_ecosystem_initializing: bool = False


def get_strategy_ecosystem(**kwargs) -> 'StrategyEcosystem':
    global _ecosystem, _ecosystem_initializing
    if _ecosystem is not None:
        return _ecosystem
    with _ecosystem_lock:
        if _ecosystem_initializing:
            raise RuntimeError("[R23-IN-P1-14] strategy_ecosystem初始化中，检测到循环依赖")
        if _ecosystem is None:
            _ecosystem_initializing = True
            try:
                from ali2026v3_trading.strategy.strategy_ecosystem._core import StrategyEcosystem
                _ecosystem = StrategyEcosystem(**kwargs)
            finally:
                _ecosystem_initializing = False
        return _ecosystem


# ---------------------------------------------------------------------------
# 性能缓存
# ---------------------------------------------------------------------------

# R15-P2-PERF-07修复: 资金路由中浮点数多次乘除，添加预计算缓存
_capital_route_precomputed: Dict[str, float] = {}


def _precompute_capital_route_ratios(base_capital: float, ratios: Dict[str, float]) -> Dict[str, float]:
    """预计算资金路由比例: base_capital * ratio，结果缓存避免重复乘法"""
    global _capital_route_precomputed
    _cache_key = f"{base_capital:.2f}"
    if _cache_key in _capital_route_precomputed:
        return _capital_route_precomputed
    _capital_route_precomputed.clear()
    _capital_route_precomputed.update({k: base_capital * v for k, v in ratios.items()})
    return _capital_route_precomputed


# PERF-P2-14修复: 启用资金路由预计算
_capital_route_cache = _precompute_capital_route_ratios(0.0, {})

# R15-P2-PERF-14修复: 策略生态系统重复浮点除法，预计算缓存
_division_cache: Dict[str, float] = {}


def _precompute_division(numerator: float, denominator: float, key: str = '') -> float:
    """预计算浮点除法并缓存，避免重复除法运算"""
    if not key:
        key = f"{numerator:.6f}/{denominator:.6f}"
    if key in _division_cache:
        return _division_cache[key]
    result = safe_divide(numerator, denominator, default=0.0)
    _division_cache[key] = result
    return result


__all__ = [
    'StrategyEcosystem',
    'StrategySlot',
    'CapitalRoute',
    'EcosystemTradeRecord',
    'get_strategy_ecosystem',
]
