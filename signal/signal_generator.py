# MODULE_ID: M1-239
"""
SignalGenerator — Phase1-Sprint1: generate_signal过滤链模式拆解
从signal_service.py的SignalService.generate_signal(339行)拆分为:
  - 7个独立过滤器方法，每个≤30行，圈复杂度≤5
  - generate_signal编排器≤30行
  - SignalContext数据类承载过滤链上下文
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.shared_utils import SignalType, VALID_SIGNAL_TYPES, OPEN_SIGNAL_TYPES
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id  # R9-3


@dataclass
class SignalContext:
    instrument_id: str = ''
    signal_type: str = ''
    price: float = 0.0
    volume: float = 0.0
    reason: str = ''
    priority: int = 0
    cooldown_seconds: Optional[float] = None
    estimated_plr: float = 0.0
    signal_strength: float = 0.0
    days_to_expiry: int = 999
    correlation_id: str = ''
    tick: Any = None
    rejected: bool = False
    reject_reason: str = ''
    filter_name: str = ''
    decision_result: Optional[Dict[str, Any]] = None
    signal: Optional[Dict[str, Any]] = None


class SignalGenerator:
    """generate_signal过滤链模式

    _FILTER_CHAIN定义7层过滤链执行顺序:
    1. _filter_by_strength: 信号强度过滤
    2. _filter_by_plr: PLR过滤
    3. _filter_by_mode_engine: ModeEngine模式过滤
    4. _filter_by_cooldown: 冷却过滤
    5. _filter_by_decision_score: 决策评分过滤
    6. _filter_by_hft: HFT时序过滤
    7. _filter_by_adaptive: 自适应阈值过滤
    """

    _FILTER_CHAIN = [
        '_filter_by_strength',
        '_filter_by_plr',
        '_filter_by_mode_engine',
        '_filter_by_cooldown',
        '_filter_by_decision_score',
        '_filter_by_hft',
        '_filter_by_adaptive',
    ]

    def __init__(self, signal_service: Any):
        self._svc = signal_service

    def generate_signal(self, ctx: SignalContext) -> SignalContext:
        for filter_name in self._FILTER_CHAIN:
            ctx = getattr(self, filter_name)(ctx)
            if ctx.rejected:
                return ctx
        ctx = self._create_signal_record(ctx)
        return ctx

    def _filter_by_strength(self, ctx: SignalContext) -> SignalContext:
        if ctx.signal_type in OPEN_SIGNAL_TYPES and ctx.signal_strength <= 0.0:
            self._svc._stats['filtered_signals'] += 1
            self._svc._stats['strength_filtered'] = self._svc._stats.get('strength_filtered', 0) + 1
            logging.debug("[SignalService] Signal filtered (zero_strength): %s %s", ctx.instrument_id, ctx.signal_type)
            ctx.rejected = True
            ctx.reject_reason = 'zero_strength'
            ctx.filter_name = 'strength'
        return ctx

    def _filter_by_plr(self, ctx: SignalContext) -> SignalContext:
        if self._svc._plr_filter_enabled and self._svc._min_estimated_plr > 0:
            if ctx.signal_type in OPEN_SIGNAL_TYPES and ctx.estimated_plr < self._svc._min_estimated_plr:
                self._svc._stats['filtered_signals'] += 1
                self._svc._stats['plr_filtered'] += 1
                logging.debug("[SignalService] Signal filtered (PLR): %s plr=%.2f < %.2f",
                              ctx.instrument_id, ctx.estimated_plr, self._svc._min_estimated_plr)
                ctx.rejected = True
                ctx.reject_reason = 'plr_filtered'
                ctx.filter_name = 'plr'
        return ctx

    def _filter_by_mode_engine(self, ctx: SignalContext) -> SignalContext:
        try:
            from ali2026v3_trading.governance.mode_engine import ModeEngine
            _me = ModeEngine.get_instance()
            _passed, _reason = _me.filter_signal_by_mode(
                ctx.signal_type, estimated_plr=ctx.estimated_plr,
                signal_strength=ctx.signal_strength, days_to_expiry=ctx.days_to_expiry,
            )
            if not _passed:
                self._svc._stats['filtered_signals'] += 1
                self._svc._stats['mode_filtered'] += 1
                logging.debug("[SignalService] Signal filtered (ModeEngine): %s %s", ctx.instrument_id, _reason)
                ctx.rejected = True
                ctx.reject_reason = _reason
                ctx.filter_name = 'mode_engine'
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SignalGenerator] ModeEngine过滤异常, fail-safe阻断: %s", e)
            ctx.rejected = True
            ctx.reject_reason = 'mode_engine_exception'
            ctx.filter_name = 'mode_engine'
        return ctx

    def _filter_by_cooldown(self, ctx: SignalContext) -> SignalContext:
        _effective = ctx.cooldown_seconds if ctx.cooldown_seconds is not None else self._svc._default_cooldown_seconds
        _cooldown_key = self._svc._make_cooldown_key(ctx.instrument_id, ctx.signal_type)
        if self._svc._is_in_cooldown(_cooldown_key, _effective):
            self._svc._stats['filtered_signals'] += 1
            self._svc._stats['cooldown_filtered'] += 1
            logging.debug("[SignalService] Signal filtered (cooldown): %s %s", ctx.instrument_id, ctx.signal_type)
            ctx.rejected = True
            ctx.reject_reason = 'cooldown'
            ctx.filter_name = 'cooldown'
        return ctx

    def _filter_by_decision_score(self, ctx: SignalContext) -> SignalContext:
        _dim_kwargs = self._svc._collect_decision_dimensions(ctx.instrument_id)
        if self._svc._decision_score_filter_enabled:
            try:
                _tmp_signal = {
                    'signal_id': '', 'instrument_id': ctx.instrument_id, 'signal_type': ctx.signal_type,
                    'price': ctx.price, 'volume': ctx.volume, 'reason': ctx.reason, 'priority': ctx.priority,
                    'estimated_plr': ctx.estimated_plr, 'filtered': False, 'filter_reason': '',
                }
                ctx.decision_result = self._svc.apply_decision_score_filter(
                    _tmp_signal, state_strength=ctx.signal_strength,
                    order_flow_consistency=self._svc._default_order_flow_consistency, **_dim_kwargs,
                )
                if ctx.decision_result.get('filtered'):
                    self._svc._stats['filtered_signals'] += 1
                    self._svc._stats['decision_filtered'] += 1
                    logging.info("[SignalService] Signal filtered (decision_score): %s %s",
                                 ctx.instrument_id, ctx.decision_result.get('filter_reason', ''))
                    ctx.rejected = True
                    ctx.reject_reason = ctx.decision_result.get('filter_reason', '')
                    ctx.filter_name = 'decision_score'
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[SignalGenerator] decision_score_filter异常, fail-safe阻断: %s", e)
                ctx.rejected = True
                ctx.reject_name = 'decision_score_exception'
                ctx.filter_name = 'decision_score'
        else:
            try:
                from ali2026v3_trading.risk.risk_service import get_risk_service
                rs = get_risk_service()
                ctx.decision_result = rs.compute_decision_score(
                    ctx.signal_strength, self._svc._default_order_flow_consistency, **_dim_kwargs,
                )
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
        return ctx

    def _filter_by_hft(self, ctx: SignalContext) -> SignalContext:
        if self._svc._hft_filter_enabled and self._svc._hft_signal_filter is not None:
            try:
                hft_result = self._svc.filter_with_hft(ctx.instrument_id, ctx.signal_strength)
                if hft_result is not None and not hft_result.get('signal_passed', True):
                    self._svc._stats['hft_filtered'] = self._svc._stats.get('hft_filtered', 0) + 1
                    logging.info("[SignalService] HFT过滤阻断: %s %s", ctx.instrument_id, hft_result.get('reason', ''))
                    ctx.rejected = True
                    ctx.reject_reason = hft_result.get('reason', '')
                    ctx.filter_name = 'hft'
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[SignalGenerator] HFT过滤异常, fail-safe阻断: %s", e)
                ctx.rejected = True
                ctx.reject_reason = 'hft_exception'
                ctx.filter_name = 'hft'
        return ctx

    def _filter_by_adaptive(self, ctx: SignalContext) -> SignalContext:
        if self._svc._adaptive_threshold is not None:
            try:
                current_threshold = self._svc._adaptive_threshold.threshold
                if ctx.signal_strength < current_threshold:
                    self._svc._adaptive_threshold.record_signal(passed=False, pnl=0.0)
                    self._svc._stats['adaptive_filtered'] = self._svc._stats.get('adaptive_filtered', 0) + 1
                    logging.info("[SignalService] 自适应阈值过滤: strength=%.2f < threshold=%.2f",
                                 ctx.signal_strength, current_threshold)
                    ctx.rejected = True
                    ctx.reject_reason = f'strength={ctx.signal_strength:.2f} < threshold={current_threshold:.2f}'
                    ctx.filter_name = 'adaptive'
                else:
                    self._svc._adaptive_threshold.record_signal(passed=True, pnl=0.0)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[SignalGenerator] AdaptiveThreshold异常, fail-safe阻断: %s", e)
                ctx.rejected = True
                ctx.reject_reason = 'adaptive_exception'
                ctx.filter_name = 'adaptive'
        return ctx

    def _create_signal_record(self, ctx: SignalContext) -> SignalContext:
        _dr = ctx.decision_result
        ctx.signal = {
            'signal_id': generate_prefixed_id("SIG", 12),  # R9-3
            'instrument_id': ctx.instrument_id,
            'signal_type': ctx.signal_type,
            'price': ctx.price,
            'volume': ctx.volume,
            'reason': ctx.reason,
            'open_reason': ctx.reason if ctx.signal_type in ('OPEN', 'open', 'BUY', 'SELL') else '',
            'priority': ctx.priority,
            'estimated_plr': ctx.estimated_plr,
            'correlation_id': ctx.correlation_id,
            'generated_at': datetime.now(CHINA_TZ),
            'source_tick_arrival_ts': getattr(ctx.tick, '_arrival_ts', None) if ctx.tick else None,
            'signal_generated_perf_ts': time.perf_counter(),
            'status': 'EMITTED',
            'decision_score': _dr.get('decision_score', 0.0) if _dr else 0.0,
            'position_scale': _dr.get('position_scale', 1.0) if _dr else 1.0,
            'decision_action': _dr.get('action', 'normal_open') if _dr else 'normal_open',
            'dimension_scores': _dr.get('dimension_scores', {}) if _dr else {},
            'dimension_weights': _dr.get('dimension_weights', {}) if _dr else {},
        }
        return ctx