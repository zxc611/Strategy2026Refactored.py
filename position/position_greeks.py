# MODULE_ID: M1-204
"""Position Greeks - 跨策略Greeks敞口聚合+跨策略风控守卫

从position_service.py拆分(CC-09 Step3):
- GreeksExposure: Greeks敞口聚合结果
- CrossStrategyRiskGuard: 跨策略分层风控守卫
- aggregate_greeks_exposure: 聚合所有持仓Greeks
- _is_option_instrument: 期权合约判断
- _estimate_option_delta/vega/gamma: 期权Greeks估计
- _REASON_STRATEGY_MAP: 开仓理由→策略映射
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple


@dataclass(slots=True)
class GreeksExposure:
    """跨策略Greeks敞口聚合结果"""
    net_delta: float = 0.0
    gross_delta: float = 0.0
    net_vega: float = 0.0
    gross_vega: float = 0.0
    net_gamma: float = 0.0
    gross_gamma: float = 0.0
    by_strategy: Dict[str, Dict[str, float]] = field(default_factory=dict)
    by_instrument: Dict[str, Dict[str, float]] = field(default_factory=dict)
    total_futures_lots: int = 0
    total_option_lots: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'net_delta': round(self.net_delta, 4), 'gross_delta': round(self.gross_delta, 4),
            'net_vega': round(self.net_vega, 4), 'gross_vega': round(self.gross_vega, 4),
            'net_gamma': round(self.net_gamma, 4), 'gross_gamma': round(self.gross_gamma, 4),
            'by_strategy': self.by_strategy, 'by_instrument': self.by_instrument,
            'total_futures_lots': self.total_futures_lots, 'total_option_lots': self.total_option_lots,
        }


_REASON_STRATEGY_MAP = {
    'CORRECT_RESONANCE': 'resonance', 'HIGH_FREQ': 'high_freq',
    'DIVERGENCE_REVERSAL': 'divergence',
    'OTHER_SCALP': 'box', 'BOX_SPRING': 'spring', 'BOX_EXTREME': 'box',
    'ARBITRAGE': 'arbitrage', 'MARKET_MAKING': 'market_making',
    'MANUAL': 'manual',
}


def _is_option_instrument(instrument_id: str) -> bool:
    return '-C-' in instrument_id or '-P-' in instrument_id


def _try_greeks_calculator(instrument_id: str, greek_name: str) -> Optional[float]:
    """P1-03修复: 统一委托GreeksCalculator获取精确Greeks值。返回None表示不可用需fallback。"""
    try:
        from ali2026v3_trading.risk.risk_service import get_risk_service
        _rs = get_risk_service()
        if _rs:
            _gc = _rs._get_greeks_calculator() if hasattr(_rs, '_get_greeks_calculator') else None
            if _gc:
                _greeks = _gc.get_greeks(instrument_id)
                if _greeks and greek_name in _greeks:
                    return _greeks[greek_name]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[P1-03] GreeksCalculator委托失败(将降级到常量): %s", e)
    return None


def _estimate_option_delta(instrument_id: str, direction: str, volume: int) -> float:
    # P1-03修复: 优先委托GreeksCalculator，fallback时记录降级日志
    precise = _try_greeks_calculator(instrument_id, 'delta')
    if precise is not None:
        sign = 1.0 if direction in ('long', 'BUY') else -1.0
        return sign * precise * abs(volume)
    logging.debug("[P1-03] delta降级到常量估算: %s", instrument_id)
    from ali2026v3_trading.position.position_service import PositionService
    delta_per_lot = PositionService.OPTION_DELTA_PER_LOT_CALL if '-C-' in instrument_id else (PositionService.OPTION_DELTA_PER_LOT_PUT if '-P-' in instrument_id else 0.0)
    sign = 1.0 if direction in ('long', 'BUY') else -1.0
    return sign * delta_per_lot * abs(volume)


def _estimate_option_vega(instrument_id: str, volume: int) -> float:
    # P1-03修复: 优先委托GreeksCalculator，fallback时记录降级日志
    precise = _try_greeks_calculator(instrument_id, 'vega')
    if precise is not None:
        return precise * abs(volume)
    logging.debug("[P1-03] vega降级到常量估算: %s", instrument_id)
    from ali2026v3_trading.position.position_service import PositionService
    return PositionService.OPTION_VEGA_PER_LOT * abs(volume) if ("-C-" in instrument_id or "-P-" in instrument_id) else 0.0


def _estimate_option_gamma(instrument_id: str, volume: int) -> float:
    # P1-03修复: 优先委托GreeksCalculator，fallback时记录降级日志
    precise = _try_greeks_calculator(instrument_id, 'gamma')
    if precise is not None:
        return precise * abs(volume)
    logging.debug("[P1-03] gamma降级到常量估算: %s", instrument_id)
    from ali2026v3_trading.position.position_service import PositionService
    return PositionService.OPTION_GAMMA_PER_LOT * abs(volume) if ("-C-" in instrument_id or "-P-" in instrument_id) else 0.0


def aggregate_greeks_exposure(positions: Dict[str, Dict[str, Any]], position_service=None) -> GreeksExposure:
    """聚合所有持仓的Greeks等效敞口

    Args:
        positions: 持仓字典
        position_service: PositionService实例，用于获取global_lock保护遍历
    """
    result = GreeksExposure()
    if position_service is not None and hasattr(position_service, 'global_lock'):
        with position_service.global_lock:
            return _aggregate_greeks_exposure_inner(positions, result)
    return _aggregate_greeks_exposure_inner(positions, result)


def _aggregate_greeks_exposure_inner(positions: Dict[str, Dict[str, Any]], result: GreeksExposure) -> GreeksExposure:
    for instrument_id, pos_dict in positions.items():
        if not pos_dict:
            continue
        inst_delta = inst_vega = inst_gamma = 0.0
        is_option = _is_option_instrument(instrument_id)
        for rec in pos_dict.values():
            if rec.volume == 0:
                continue
            strategy = _REASON_STRATEGY_MAP.get(rec.open_reason, 'unknown')
            if is_option:
                delta = _estimate_option_delta(instrument_id, rec.direction, rec.volume)
                vega = _estimate_option_vega(instrument_id, rec.volume)
                gamma = _estimate_option_gamma(instrument_id, rec.volume)
                result.total_option_lots += abs(rec.volume)
            else:
                sign = 1.0 if rec.direction in ('long', 'BUY') else -1.0
                delta, vega, gamma = sign * abs(rec.volume), 0.0, 0.0
                result.total_futures_lots += abs(rec.volume)
            inst_delta += delta
            inst_vega += vega
            inst_gamma += gamma
            if strategy not in result.by_strategy:
                result.by_strategy[strategy] = {'delta': 0.0, 'vega': 0.0, 'gamma': 0.0, 'lots': 0}
            result.by_strategy[strategy]['delta'] += delta
            result.by_strategy[strategy]['vega'] += vega
            result.by_strategy[strategy]['gamma'] += gamma
            result.by_strategy[strategy]['lots'] += abs(rec.volume)
        result.net_delta += inst_delta
        result.gross_delta += abs(inst_delta)
        result.net_vega += inst_vega
        result.gross_vega += abs(inst_vega)
        result.net_gamma += inst_gamma
        result.gross_gamma += abs(inst_gamma)
        result.by_instrument[instrument_id] = {'delta': round(inst_delta, 4), 'vega': round(inst_vega, 4), 'gamma': round(inst_gamma, 4)}
    return result


class CrossStrategyRiskGuard:
    """跨策略分层风控守卫"""
    WARN = 'WARN'
    REDUCE = 'REDUCE'
    BLOCK = 'BLOCK'
    CIRCUIT_BREAK = 'CIRCUIT_BREAK'
    PASS = 'PASS'
    DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT = 3.0
    DEFAULT_DELTA_LIMIT = 3.0
    DEFAULT_VEGA_LIMIT = 1.5
    DEFAULT_GAMMA_LIMIT = 0.5
    TIER_CIRCUIT_BREAK_MULTIPLIER = 2.0
    TIER_BLOCK_MULTIPLIER = 1.5
    TIER_REDUCE_MULTIPLIER = 1.2

    def __init__(self, delta_limit: float = None, vega_limit: float = None, gamma_limit: float = None):
        self._delta_limit = delta_limit if delta_limit is not None else self.DEFAULT_DELTA_LIMIT
        self._vega_limit = vega_limit if vega_limit is not None else self.DEFAULT_VEGA_LIMIT
        self._gamma_limit = gamma_limit if gamma_limit is not None else self.DEFAULT_GAMMA_LIMIT
        self._daily_drawdown_pct = 0.0
        self._stats = {'checks': 0, 'warns': 0, 'reduces': 0, 'blocks': 0, 'circuit_breaks': 0}

    def set_daily_drawdown(self, pct: float):
        self._daily_drawdown_pct = pct

    def check(self, exposure: GreeksExposure) -> Tuple[str, str, Dict[str, Any]]:
        import logging as _logging
        self._stats['checks'] += 1
        gd, nd, gv, gg, dl = exposure.gross_delta, abs(exposure.net_delta), exposure.gross_vega, exposure.gross_gamma, self._delta_limit
        if self._daily_drawdown_pct > self.DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT:
            self._stats['circuit_breaks'] += 1
            _logging.warning("[CrossStrategyRiskGuard] LOG-P1-04: CIRCUIT_BREAK - 日回撤%.2f%%超阈值%.2f%%", self._daily_drawdown_pct, self.DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT)
            return self.CIRCUIT_BREAK, f'日回撤超{self.DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT}%, 触发熔断', {'drawdown_pct': self._daily_drawdown_pct}
        if gd > dl * self.TIER_CIRCUIT_BREAK_MULTIPLIER or nd > dl * self.TIER_CIRCUIT_BREAK_MULTIPLIER:
            self._stats['circuit_breaks'] += 1
            _logging.warning("[CrossStrategyRiskGuard] LOG-P1-04: CIRCUIT_BREAK - Gross Delta(%.1f)超熔断线", gd)
            return self.CIRCUIT_BREAK, f'Gross Delta({gd:.1f})超熔断线', {'gross_delta': gd}
        if gd > dl * self.TIER_BLOCK_MULTIPLIER or nd > dl * self.TIER_BLOCK_MULTIPLIER:
            self._stats['blocks'] += 1
            _logging.warning("[CrossStrategyRiskGuard] LOG-P1-04: BLOCK - Gross Delta(%.1f)超硬阻断线", gd)
            return self.BLOCK, f'Gross Delta({gd:.1f})超硬阻断线', {'gross_delta': gd}
        if gv > self._vega_limit * self.TIER_BLOCK_MULTIPLIER:
            self._stats['blocks'] += 1
            _logging.warning("[CrossStrategyRiskGuard] LOG-P1-04: BLOCK - Gross Vega(%.2f)超Vega限额", gv)
            return self.BLOCK, f'Gross Vega({gv:.2f})超Vega限额', {'gross_vega': gv}
        if gg > self._gamma_limit * self.TIER_BLOCK_MULTIPLIER:
            self._stats['blocks'] += 1
            _logging.warning("[CrossStrategyRiskGuard] LOG-P1-04: BLOCK - Gross Gamma(%.4f)超Gamma限额", gg)
            return self.BLOCK, f'Gross Gamma({gg:.4f})超Gamma限额', {'gross_gamma': gg}
        if gd > dl * self.TIER_REDUCE_MULTIPLIER or nd > dl * self.TIER_REDUCE_MULTIPLIER:
            self._stats['reduces'] += 1
            _logging.info("[CrossStrategyRiskGuard] LOG-P1-04: REDUCE - Gross Delta(%.1f)超预警线", gd)
            return self.REDUCE, f'Gross Delta({gd:.1f})超预警线', {'gross_delta': gd}
        if gd > dl or nd > dl:
            self._stats['warns'] += 1
            _logging.info("[CrossStrategyRiskGuard] LOG-P1-04: WARN - Gross Delta(%.1f)接近限额", gd)
            return self.WARN, f'Gross Delta({gd:.1f})接近限额', {'gross_delta': gd}
        if gv > self._vega_limit:
            self._stats['warns'] += 1
            return self.WARN, f'Gross Vega({gv:.2f})接近Vega限额', {'gross_vega': gv}
        if gg > self._gamma_limit:
            self._stats['warns'] += 1
            return self.WARN, f'Gross Gamma({gg:.4f})接近Gamma限额', {'gross_gamma': gg}
        return self.PASS, '', {}

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._stats)


_cross_strategy_risk_guard: Optional[CrossStrategyRiskGuard] = None
_cross_strategy_risk_guard_lock = threading.Lock()


def get_cross_strategy_risk_guard() -> CrossStrategyRiskGuard:
    global _cross_strategy_risk_guard
    with _cross_strategy_risk_guard_lock:
        if _cross_strategy_risk_guard is None:
            _cross_strategy_risk_guard = CrossStrategyRiskGuard()
        return _cross_strategy_risk_guard
