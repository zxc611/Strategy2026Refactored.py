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
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.shared_utils import SignalType, VALID_SIGNAL_TYPES, OPEN_SIGNAL_TYPES


class SignalState:
    GENERATED = 'GENERATED'
    FILTERED = 'FILTERED'
    SCORED = 'SCORED'
    EXECUTED = 'EXECUTED'
    CONFIRMED = 'CONFIRMED'
    COMPLETED = 'COMPLETED'
    EXPIRED = 'EXPIRED'
    REJECTED = 'REJECTED'


SIGNAL_STATE_TRANSITIONS = {
    SignalState.GENERATED: [SignalState.FILTERED, SignalState.EXPIRED, SignalState.REJECTED],
    SignalState.FILTERED: [SignalState.SCORED, SignalState.EXPIRED, SignalState.REJECTED],
    SignalState.SCORED: [SignalState.EXECUTED, SignalState.EXPIRED, SignalState.REJECTED],
    SignalState.EXECUTED: [SignalState.CONFIRMED, SignalState.EXPIRED],
    SignalState.CONFIRMED: [SignalState.COMPLETED],
    SignalState.COMPLETED: [], SignalState.EXPIRED: [], SignalState.REJECTED: [],
}

SIGNAL_SERVICE_CONSTANTS = {
    'SIGNAL_HISTORY_MAX_LEN': 1000,
    'DEFAULT_COOLDOWN_SECONDS': 60.0,
    'CLEANUP_INTERVAL_SECONDS': 300,
    'CONFIG_COOLDOWN_KEY': 'signal_cooldown_sec',
    'CONFIG_CLEANUP_KEY': 'signal_cleanup_interval_seconds',
    'MARKET_CLOSE_HOUR': 15,
    'MARKET_CLOSE_MINUTE_START': 15,
    'MARKET_CLOSE_MINUTE_END': 20,
    'DEFAULT_ORDER_FLOW_CONSISTENCY': 0.5,
    'SIGNAL_HALF_LIFE_MS': 5000.0,
    'LATENCY_BUDGET_MS': {
        'signal_generation': {'p50': 20.0, 'p99': 50.0},
        'event_bus_publish': {'p50': 2.0, 'p99': 10.0},
        'schedule_cycle': {'p50': 10.0, 'p99': 30.0},
        'risk_check': {'p50': 15.0, 'p99': 50.0},
        'order_submit': {'p50': 3.0, 'p99': 10.0},
    },
}


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
        """编排器: 过滤链模式，≤30行，圈复杂度≤5"""
        for filter_name in self._FILTER_CHAIN:
            ctx = getattr(self, filter_name)(ctx)
            if ctx.rejected:
                return ctx
        ctx = self._create_signal_record(ctx)
        return ctx

    def _filter_by_strength(self, ctx: SignalContext) -> SignalContext:
        """过滤器1: 信号强度过滤(开仓信号strength>0)"""
        if ctx.signal_type in OPEN_SIGNAL_TYPES and ctx.signal_strength <= 0.0:
            self._svc._stats['filtered_signals'] += 1
            self._svc._stats['strength_filtered'] = self._svc._stats.get('strength_filtered', 0) + 1
            logging.debug("[SignalService] Signal filtered (zero_strength): %s %s", ctx.instrument_id, ctx.signal_type)
            ctx.rejected = True
            ctx.reject_reason = 'zero_strength'
            ctx.filter_name = 'strength'
        return ctx

    def _filter_by_plr(self, ctx: SignalContext) -> SignalContext:
        """过滤器2: PLR过滤(预估盈亏比低于阈值)"""
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
        """过滤器3: ModeEngine模式过滤(信号强度+time_decay)"""
        try:
            from ali2026v3_trading.mode_engine import ModeEngine
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
        except Exception as e:
            logging.warning("[R22-EP-P1] ModeEngine过滤异常, fail-safe阻断: %s", e)
            ctx.rejected = True
            ctx.reject_reason = 'mode_engine_exception'
            ctx.filter_name = 'mode_engine'
        return ctx

    def _filter_by_cooldown(self, ctx: SignalContext) -> SignalContext:
        """过滤器4: 冷却过滤(同一合约+信号类型冷却期内)"""
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
        """过滤器5: 决策评分过滤(11维度评分低于阈值)"""
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
            except Exception as e:
                logging.warning("[R22-EP-P1] decision_score_filter异常, fail-safe阻断: %s", e)
                ctx.rejected = True
                ctx.reject_name = 'decision_score_exception'
                ctx.filter_name = 'decision_score'
        else:
            try:
                from ali2026v3_trading.risk_service import get_risk_service
                rs = get_risk_service()
                ctx.decision_result = rs.compute_decision_score(
                    ctx.signal_strength, self._svc._default_order_flow_consistency, **_dim_kwargs,
                )
            except Exception:
                pass
        return ctx

    def _filter_by_hft(self, ctx: SignalContext) -> SignalContext:
        """过滤器6: HFT时序过滤(Kalman/EMA信号时序滤波)"""
        if self._svc._hft_filter_enabled and self._svc._hft_signal_filter is not None:
            try:
                hft_result = self._svc.filter_with_hft(ctx.instrument_id, ctx.signal_strength)
                if hft_result is not None and not hft_result.get('signal_passed', True):
                    self._svc._stats['hft_filtered'] = self._svc._stats.get('hft_filtered', 0) + 1
                    logging.info("[SignalService] HFT过滤阻断: %s %s", ctx.instrument_id, hft_result.get('reason', ''))
                    ctx.rejected = True
                    ctx.reject_reason = hft_result.get('reason', '')
                    ctx.filter_name = 'hft'
            except Exception as e:
                logging.warning("[R22-EP-P1] HFT过滤异常, fail-safe阻断: %s", e)
                ctx.rejected = True
                ctx.reject_reason = 'hft_exception'
                ctx.filter_name = 'hft'
        return ctx

    def _filter_by_adaptive(self, ctx: SignalContext) -> SignalContext:
        """过滤器7: 自适应阈值过滤(AdaptiveSignalThreshold)"""
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
            except Exception as e:
                logging.warning("[R22-EP-P1] AdaptiveThreshold异常, fail-safe阻断: %s", e)
                ctx.rejected = True
                ctx.reject_reason = 'adaptive_exception'
                ctx.filter_name = 'adaptive'
        return ctx

    def _create_signal_record(self, ctx: SignalContext) -> SignalContext:
        """创建信号记录(通过所有过滤后)"""
        _dr = ctx.decision_result
        ctx.signal = {
            'signal_id': f"SIG_{uuid.uuid4().hex[:12]}",
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

    @staticmethod
    def validate_signal(signal: Dict[str, Any]) -> tuple:
        from ali2026v3_trading.shared_utils import VALID_SIGNAL_TYPES
        required_fields = ['instrument_id', 'signal_type', 'price', 'volume']
        for field in required_fields:
            if field not in signal:
                return False, f"Missing required field: {field}"
        if signal['signal_type'] not in VALID_SIGNAL_TYPES:
            return False, f"Invalid signal type: {signal['signal_type']}"
        if signal['price'] <= 0 or signal['volume'] <= 0:
            return False, "Price and volume must be positive"
        return True, "Valid signal"

    @staticmethod
    def collect_decision_dimensions(instrument_id: str) -> Dict[str, Any]:
        kwargs = {}
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            if hasattr(rs, 'get_greeks_dashboard'):
                try:
                    gd = rs.get_greeks_dashboard(instrument_id)
                    if isinstance(gd, dict):
                        kwargs['greeks_dashboard'] = gd
                except Exception:
                    pass
            if hasattr(rs, 'params') and isinstance(rs.params, dict):
                try:
                    kwargs['consecutive_losses'] = int(rs.params.get('_consecutive_losses', 0))
                    kwargs['current_pnl'] = float(rs.params.get('_current_pnl', 0.0))
                    kwargs['drawdown_pct'] = float(rs.params.get('_drawdown_pct', 0.0))
                except Exception:
                    pass
        except Exception:
            pass
        try:
            from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
            _sse = get_shadow_strategy_engine()
            if _sse and hasattr(_sse, 'alpha_ratio'):
                kwargs['alpha_ratio'] = _sse.alpha_ratio
        except Exception:
            pass
        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            if spm and hasattr(spm, 'current_state'):
                kwargs['hmm_state'] = str(spm.current_state)
        except Exception:
            pass
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            se = get_strategy_ecosystem()
            if se and hasattr(se, 'cross_correlation'):
                kwargs['cross_correlation'] = se.cross_correlation
        except Exception:
            pass
        return kwargs

    @staticmethod
    def apply_decision_score_filter(signal: Dict[str, Any], state_strength: float,
                                     order_flow_consistency: float,
                                     hmm_state: Optional[str] = None,
                                     cr_output: Optional[Any] = None,
                                     greeks_dashboard: Optional[Dict[str, Any]] = None,
                                     consecutive_losses: int = 0,
                                     current_pnl: float = 0.0,
                                     drawdown_pct: float = 0.0,
                                     alpha_ratio: Optional[float] = None,
                                     cross_correlation: Optional[float] = None,
                                     tri_validation_score: Optional[float] = None,
                                     slippage_source: str = 'LIVE') -> Dict[str, Any]:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            result = rs.compute_decision_score(
                state_strength, order_flow_consistency,
                hmm_state=hmm_state, cr_output=cr_output,
                greeks_dashboard=greeks_dashboard,
                consecutive_losses=consecutive_losses,
                current_pnl=current_pnl, drawdown_pct=drawdown_pct,
                alpha_ratio=alpha_ratio,
                cross_correlation=cross_correlation,
                tri_validation_score=tri_validation_score,
                slippage_source=slippage_source,
            )
            if result["action"] == "no_open_wait":
                signal["filtered"] = True
                signal["filter_reason"] = f"decision_score_low: score={result['decision_score']:.2f}"
            signal["decision_score"] = result["decision_score"]
            signal["position_scale"] = result["position_scale"]
            signal["decision_action"] = result["action"]
            signal["dimension_scores"] = result.get("dimension_scores", {})
            signal["dimension_weights"] = result.get("dimension_weights", {})
        except Exception as e:
            logging.debug("[SignalService] apply_decision_score_filter error: %s", e)
        return signal

    @staticmethod
    def init_attributes(svc: Any, event_bus: Any, default_order_flow_consistency: float) -> None:
        import os
        svc._history_service = SignalHistoryService(max_history=1000)
        svc._signal_history = svc._history_service._history
        svc._cooldown_times = {}
        svc._cooldown_durations = {}
        svc._last_cleanup = time.time()
        svc._signal_dedup_cache = {}
        svc._stats = {'total_signals': 0, 'filtered_signals': 0, 'plr_filtered': 0, 'mode_filtered': 0, 'cooldown_filtered': 0, 'decision_filtered': 0, 'emitted_signals': 0, 'dedup_filtered': 0, 'last_signal_time': None}
        svc._lock = __import__('threading').RLock()
        svc._signal_queue = __import__('collections').deque(maxlen=2000)
        svc._signal_states = {}
        svc._signal_max_age_sec = 180.0
        svc._default_cooldown_seconds = 60.0
        svc._cleanup_interval = 300
        svc._default_order_flow_consistency = default_order_flow_consistency
        svc._event_bus = event_bus
        svc._min_estimated_plr = 0.0
        svc._plr_filter_enabled = False
        svc._hft_signal_filter = None
        svc._hft_filter_enabled = False
        from ali2026v3_trading.signal_filter_chain import SignalFilterChain
        from ali2026v3_trading.cooldown_manager import CooldownManager
        svc._filter_chain = SignalFilterChain(svc)
        svc._cooldown_mgr = CooldownManager()
        svc._decision_score_filter_enabled = True
        svc._adaptive_threshold = None
        svc._log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        svc._history_service.set_log_dir(svc._log_dir)
        svc._daily_report_generated = {}
        svc._structured_logger = None


    @staticmethod
    def init_from_config(svc: Any) -> None:
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _params = get_cached_params()
            if isinstance(_params, dict):
                if 'signal_max_age_sec' in _params:
                    svc._signal_max_age_sec = float(_params['signal_max_age_sec'])
                if 'default_order_flow_consistency' in _params:
                    svc._default_order_flow_consistency = _params['default_order_flow_consistency']
        except Exception:
            pass
        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            _cooldown_cfg = _cfg.get(svc._CONFIG_COOLDOWN_KEY, None) if hasattr(_cfg, 'get') else None
            _cleanup_cfg = _cfg.get(svc._CONFIG_CLEANUP_KEY, None) if hasattr(_cfg, 'get') else None
            if _cooldown_cfg is not None:
                svc._default_cooldown_seconds = float(_cooldown_cfg)
            if _cleanup_cfg is not None:
                svc._cleanup_interval = float(_cleanup_cfg)
        except Exception:
            pass
        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            if getattr(_cfg, 'trading', None) and getattr(_cfg.trading, 'enable_hft_filter', None):
                svc.enable_hft_filter()
            else:
                _strategy_type = getattr(_cfg, 'strategy_type', 'normal') if hasattr(_cfg, 'strategy_type') else 'normal'
                if _strategy_type in ('hft', 'high_frequency'):
                    svc.enable_hft_filter()
        except Exception:
            pass
        try:
            from ali2026v3_trading.signal_service import AdaptiveSignalThreshold
            svc._adaptive_threshold = AdaptiveSignalThreshold()
        except Exception:
            svc._adaptive_threshold = None
        try:
            from ali2026v3_trading.health_check_api import StructuredJsonlLogger
            svc._structured_logger = StructuredJsonlLogger(log_dir=svc._log_dir)
        except Exception:
            svc._structured_logger = None

    @staticmethod
    def run_benchmark(signal_service: Any, iterations: int = 100, cooldown_enabled: bool = False) -> Dict[str, Any]:
        import statistics
        latencies = []
        start_total = time.time()
        original_cooldown = signal_service._default_cooldown_seconds
        if not cooldown_enabled:
            signal_service._default_cooldown_seconds = 0.0
        try:
            for i in range(iterations):
                test_instrument = f"TEST{str(i).zfill(3)}"
                test_price = 3500.0 + (i % 10)
                start = time.time()
                signal = signal_service.generate_signal(
                    instrument_id=test_instrument,
                    signal_type='BUY',
                    price=test_price,
                    volume=1,
                    reason=f'Benchmark signal {i}'
                )
                elapsed = time.time() - start
                if signal:
                    latencies.append(elapsed)
            total_time = time.time() - start_total
            results = {
                'iterations': iterations,
                'successful_signals': len(latencies),
                'filtered_signals': iterations - len(latencies),
                'total_time_seconds': total_time,
                'avg_latency_ms': statistics.mean(latencies) * 1000 if latencies else 0,
                'min_latency_ms': min(latencies) * 1000 if latencies else 0,
                'max_latency_ms': max(latencies) * 1000 if latencies else 0,
                'median_latency_ms': statistics.median(latencies) * 1000 if latencies else 0,
                'stddev_latency_ms': statistics.stdev(latencies) * 1000 if len(latencies) > 1 else 0,
                'signals_per_second': len(latencies) / total_time if total_time > 0 and latencies else 0,
                'success_rate': len(latencies) / max(1, iterations)
            }
            logging.info("[SignalService] Benchmark completed: %d/%d signals, avg latency=%.3fms, %.1f signals/s",
                        len(latencies), iterations, results['avg_latency_ms'], results['signals_per_second'])
            return results
        finally:
            signal_service._default_cooldown_seconds = original_cooldown