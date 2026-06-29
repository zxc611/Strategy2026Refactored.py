# MODULE_ID: M1-241
"""信号服务 - Facade层 + SignalGenerator反向合并(P0-S2)
SignalGenerator仅1消费者(SignalService)，反向合并消除间接调用开销。
signal_generator.py保留为重导出模块，维持外部API兼容。
"""
from __future__ import annotations

import threading
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.shared_utils import SignalType, VALID_SIGNAL_TYPES, OPEN_SIGNAL_TYPES
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id  # R9-3
from ali2026v3_trading.signal.signal_timing_filter import KalmanFilter1D, EMASignalFilter, SignalTimingFilter, AdaptiveSignalThreshold
from ali2026v3_trading.signal.signal_history_service import SignalHistoryService
try:
    from ali2026v3_trading.governance.mode_engine import ModeEngine
except ImportError:
    ModeEngine = None
try:
    from ali2026v3_trading.infra.event_bus import EventBus, get_global_event_bus
    _HAS_EVENT_BUS = True
except ImportError:
    _HAS_EVENT_BUS = False
    EventBus = None


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
    """generate_signal过滤链模式(反向合并入signal_service)

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
            try:
                ctx = getattr(self, filter_name)(ctx)
            except Exception as e:
                if not getattr(self, '_filter_exc_logged', False):
                    self._filter_exc_logged = True
                    logging.exception("[SignalService] filter %s exception, fail-safe pass: %s", filter_name, e)
                else:
                    logging.warning("[SignalService] filter %s exception, fail-safe pass: %s", filter_name, e)
            if ctx.rejected:
                return ctx
        ctx = self._create_signal_record(ctx)
        return ctx

    def _filter_by_strength(self, ctx: SignalContext) -> SignalContext:
        if not ctx.instrument_id or not ctx.instrument_id.strip():
            self._svc._stats['filtered_signals'] += 1
            logging.debug("[SignalService] Signal filtered (empty_instrument_id): %s %s", ctx.instrument_id, ctx.signal_type)
            ctx.rejected = True
            ctx.reject_reason = 'empty_instrument_id'
            ctx.filter_name = 'strength'
            return ctx
        if ctx.signal_type in OPEN_SIGNAL_TYPES and ctx.signal_strength <= 0.0:
            self._svc._stats['filtered_signals'] += 1
            self._svc._stats['strength_filtered'] = self._svc._stats.get('strength_filtered', 0) + 1
            logging.debug("[SignalService] Signal filtered (zero_strength): %s %s", ctx.instrument_id, ctx.signal_type)
            ctx.rejected = True
            ctx.reject_reason = 'zero_strength'
            ctx.filter_name = 'strength'
        return ctx

    def _filter_by_plr(self, ctx: SignalContext) -> SignalContext:
        if getattr(self._svc, '_plr_filter_enabled', False) and getattr(self._svc, '_min_estimated_plr', 0) > 0:
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
            logging.warning("[R22-EP-P1] ModeEngine过滤异常, fail-safe阻断: %s", e)
            ctx.rejected = True
            ctx.reject_reason = 'mode_engine_exception'
            ctx.filter_name = 'mode_engine'
        return ctx

    def _filter_by_cooldown(self, ctx: SignalContext) -> SignalContext:
        _effective = ctx.cooldown_seconds if ctx.cooldown_seconds is not None else getattr(self._svc, '_default_cooldown_seconds', 60.0)
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
        if getattr(self._svc, '_decision_score_filter_enabled', True):
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, NameError) as e:
                if not getattr(self._svc, '_dsf_warn_suppressed', False):
                    logging.warning("[R22-EP-P1] decision_score_filter异常, fail-safe阻断: %s (后续同类异常静默)", e)
                    self._svc._dsf_warn_suppressed = True
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
        if getattr(self._svc, '_hft_filter_enabled', False) and getattr(self._svc, '_hft_signal_filter', None) is not None:
            try:
                hft_result = self._svc.filter_with_hft(ctx.instrument_id, ctx.signal_strength)
                if hft_result is not None and not hft_result.get('signal_passed', True):
                    self._svc._stats['hft_filtered'] = self._svc._stats.get('hft_filtered', 0) + 1
                    logging.info("[SignalService] HFT过滤阻断: %s %s", ctx.instrument_id, hft_result.get('reason', ''))
                    ctx.rejected = True
                    ctx.reject_reason = hft_result.get('reason', '')
                    ctx.filter_name = 'hft'
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[R22-EP-P1] HFT过滤异常, fail-safe阻断: %s", e)
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
                logging.warning("[R22-EP-P1] AdaptiveThreshold异常, fail-safe阻断: %s", e)
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

    @staticmethod
    def validate_signal(signal: Dict[str, Any]) -> tuple:
        from ali2026v3_trading.infra.shared_utils import VALID_SIGNAL_TYPES
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
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            if hasattr(rs, 'get_greeks_dashboard'):
                try:
                    gd = rs.get_greeks_dashboard(instrument_id)
                    if isinstance(gd, dict):
                        kwargs['greeks_dashboard'] = gd
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
            if hasattr(rs, 'params') and isinstance(rs.params, dict):
                try:
                    kwargs['consecutive_losses'] = int(rs.params.get('_consecutive_losses', 0))
                    kwargs['current_pnl'] = float(rs.params.get('_current_pnl', 0.0))
                    kwargs['drawdown_pct'] = float(rs.params.get('_drawdown_pct', 0.0))
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine
            _sse = get_shadow_strategy_engine()
            if _sse and hasattr(_sse, 'alpha_ratio'):
                kwargs['alpha_ratio'] = _sse.alpha_ratio
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.config.state_param import get_state_param_manager
            spm = get_state_param_manager()
            if spm and hasattr(spm, 'current_state'):
                kwargs['hmm_state'] = str(spm.current_state)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            se = get_strategy_ecosystem()
            if se and hasattr(se, 'cross_correlation'):
                kwargs['cross_correlation'] = se.cross_correlation
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
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
            from ali2026v3_trading.risk.risk_service import get_risk_service
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
        from ali2026v3_trading.signal.signal_filter_chain import SignalFilterChain
        from ali2026v3_trading.signal.cooldown_manager import CooldownManager
        svc._filter_chain = SignalFilterChain(svc)
        svc._cooldown_mgr = CooldownManager()
        svc._decision_score_filter_enabled = True
        svc._adaptive_threshold = None
        from ali2026v3_trading.config._constants import DEFAULT_LOG_DIR
        svc._log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_LOG_DIR)
        svc._history_service.set_log_dir(svc._log_dir)
        svc._daily_report_generated = {}
        svc._structured_logger = None

    @staticmethod
    def init_from_config(svc: Any) -> None:
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            _params = get_cached_params()
            if isinstance(_params, dict):
                if 'signal_max_age_sec' in _params:
                    svc._signal_max_age_sec = float(_params['signal_max_age_sec'])
                if 'default_order_flow_consistency' in _params:
                    svc._default_order_flow_consistency = _params['default_order_flow_consistency']
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.config.config_service import get_config_service
            _cfg = get_config_service()
            _cooldown_cfg = _cfg.get(svc._CONFIG_COOLDOWN_KEY, None) if hasattr(_cfg, 'get') else None
            _cleanup_cfg = _cfg.get(svc._CONFIG_CLEANUP_KEY, None) if hasattr(_cfg, 'get') else None
            if _cooldown_cfg is not None:
                svc._default_cooldown_seconds = float(_cooldown_cfg)
            if _cleanup_cfg is not None:
                svc._cleanup_interval = float(_cleanup_cfg)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.config.config_service import get_config_service
            _cfg = get_config_service()
            if getattr(_cfg, 'trading', None) and getattr(_cfg.trading, 'enable_hft_filter', None):
                svc.enable_hft_filter()
            else:
                _strategy_type = getattr(_cfg, 'strategy_type', 'normal') if hasattr(_cfg, 'strategy_type') else 'normal'
                if _strategy_type in ('hft', 'high_frequency'):
                    svc.enable_hft_filter()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.signal.signal_service import AdaptiveSignalThreshold
            svc._adaptive_threshold = AdaptiveSignalThreshold()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            svc._adaptive_threshold = None
        try:
            from ali2026v3_trading.infra.health_monitor import StructuredJsonlLogger
            svc._structured_logger = StructuredJsonlLogger(log_dir=svc._log_dir)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
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


class SignalService:
    """信号服务Facade"""
    SIGNAL_HISTORY_MAX_LEN=1000;DEFAULT_COOLDOWN_SECONDS=60.0;CLEANUP_INTERVAL_SECONDS=300;_CONFIG_COOLDOWN_KEY='signal_cooldown_sec';_CONFIG_CLEANUP_KEY='signal_cleanup_interval_seconds';MARKET_CLOSE_HOUR=15;MARKET_CLOSE_MINUTE_START=15;MARKET_CLOSE_MINUTE_END=20;DEFAULT_ORDER_FLOW_CONSISTENCY=0.5;SIGNAL_HALF_LIFE_MS=5000.0;LATENCY_BUDGET_MS={'signal_generation':{'p50':20.0,'p99':50.0},'event_bus_publish':{'p50':2.0,'p99':10.0},'schedule_cycle':{'p50':10.0,'p99':30.0},'risk_check':{'p50':15.0,'p99':50.0},'order_submit':{'p50':3.0,'p99':10.0}}
    def __init__(self, event_bus=None, default_order_flow_consistency=0.5):
        SignalGenerator.init_attributes(self, event_bus or (get_global_event_bus() if _HAS_EVENT_BUS else None), default_order_flow_consistency)
        SignalGenerator.init_from_config(self)
        self._DEDUP_HARD_LIMIT=1000;self._DEDUP_EVICT_COUNT=500;self._DEDUP_TTL_SECONDS=5.0;self._DEDUP_CACHE_HARD_LIMIT=500;self._signal_drop_count=0;self._signal_queue_max_size=2000
    def generate_signal(self, instrument_id, signal_type, price, volume, reason='', priority=0, cooldown_seconds=None, estimated_plr=0.0, signal_strength=0.0, days_to_expiry=999, correlation_id="", tick=None):
        _r=SignalGenerator(self).generate_signal(SignalContext(instrument_id=instrument_id,signal_type=signal_type,price=price,volume=volume,reason=reason,priority=priority,cooldown_seconds=cooldown_seconds,estimated_plr=estimated_plr,signal_strength=signal_strength,days_to_expiry=days_to_expiry,correlation_id=correlation_id,tick=tick));return None if _r.rejected else _r.signal
    def enable_hft_filter(self, threshold=0.6, use_kalman=True): self._filter_chain.enable_hft_filter(threshold,use_kalman);self._hft_filter_enabled=True;self._hft_signal_filter=self._filter_chain._hft_signal_filter
    def enable_plr_filter(self, min_estimated_plr=2.0): self._filter_chain.enable_plr_filter(min_estimated_plr);self._min_estimated_plr=min_estimated_plr;self._plr_filter_enabled=True
    def disable_plr_filter(self): self._filter_chain.disable_plr_filter();self._plr_filter_enabled=False
    def filter_with_hft(self, instrument_id, resonance_strength): return self._filter_chain.filter_with_hft(instrument_id,resonance_strength)
    def transition_signal_state(self, signal_id, new_state): return self._filter_chain.transition_signal_state(signal_id,new_state,self._signal_states,SIGNAL_STATE_TRANSITIONS)
    def expire_stale_signals(self):
        # P2-08/P2-13修复: 委托 SignalExpiryManager 统一过期管理
        from ali2026v3_trading.infra.resilience import SignalExpiryManager
        if not hasattr(self, '_expiry_mgr'):
            self._expiry_mgr = SignalExpiryManager(default_ttl_sec=self._signal_max_age_sec)
        # 1) SignalExpiryManager 清理过期缓存
        expired_count = self._expiry_mgr.cleanup()
        # 2) filter_chain 负责状态转换
        result = self._filter_chain.expire_stale_signals(self._history_service.get_recent(n=len(self._signal_history)),self._signal_states,self._signal_max_age_sec,self._lock)
        return result
    def _make_cooldown_key(self, instrument_id, signal_type=''): return self._cooldown_mgr.make_cooldown_key(instrument_id,signal_type)
    def _is_in_cooldown(self, cooldown_key, cooldown_seconds): return self._cooldown_mgr.is_in_cooldown(cooldown_key,cooldown_seconds)
    def set_cooldown(self, instrument_id, cooldown_seconds, signal_type=''): self._cooldown_mgr.set_cooldown(instrument_id,cooldown_seconds,signal_type);self._cooldown_times=self._cooldown_mgr.cooldown_times;self._cooldown_durations=self._cooldown_mgr.cooldown_durations
    def clear_cooldown(self, instrument_id): return self._cooldown_mgr.clear_cooldown(instrument_id)
    def get_signal_history(self, instrument_id=None, limit=100):
        with self._lock: _h=self._history_service.get_recent(n=len(self._signal_history))
        return [s for s in _h if not instrument_id or s['instrument_id']==instrument_id][-limit:]
    def reset_signal_history(self):
        with self._lock: self._history_service.clear()
    def get_stats(self):
        with self._lock:
            _now=time.time()
            if _now-self._last_cleanup>self._cleanup_interval:self._last_cleanup=_now;self._cooldown_mgr.cleanup_expired(self._default_cooldown_seconds,0,0)
            _s=dict(self._stats);self.expire_stale_signals()
            return {'service_name':'SignalService',**_s,'stats_invariant_ok':_s.get('total_signals',0)==_s.get('filtered_signals',0)+_s.get('emitted_signals',0),'history_size':self._history_service.get_statistics()['total_signals'],'active_cooldowns':self._cooldown_mgr.count_active_cooldowns(self._default_cooldown_seconds)}
    def validate_signal(self, signal): return SignalGenerator.validate_signal(signal)
    def apply_decision_score_filter(self, signal, state_strength, order_flow_consistency, hmm_state=None, cr_output=None, greeks_dashboard=None, consecutive_losses=0, current_pnl=0.0, drawdown_pct=0.0, alpha_ratio=None, cross_correlation=None, tri_validation_score=None, slippage_source='LIVE'):
        return SignalGenerator.apply_decision_score_filter(signal,state_strength,order_flow_consistency,hmm_state=hmm_state,cr_output=cr_output,greeks_dashboard=greeks_dashboard,consecutive_losses=consecutive_losses,current_pnl=current_pnl,drawdown_pct=drawdown_pct,alpha_ratio=alpha_ratio,cross_correlation=cross_correlation,tri_validation_score=tri_validation_score,slippage_source=slippage_source)
    def _collect_decision_dimensions(self, instrument_id): return SignalGenerator.collect_decision_dimensions(instrument_id)
    def run_benchmark(self, iterations=100, cooldown_enabled=False): return SignalGenerator.run_benchmark(self,iterations,cooldown_enabled)
    def generate_daily_signal_report(self, date=None): return self._history_service.generate_daily_signal_report(date)
    def check_market_close_and_report(self): return self._history_service.check_market_close_and_report()
_signal_service_instance=None;_signal_service_lock=threading.Lock()
def get_signal_service(**kwargs):
    global _signal_service_instance
    with _signal_service_lock:
        if _signal_service_instance is None: _signal_service_instance=SignalService(**kwargs)
    return _signal_service_instance
__all__=['SignalService','SignalGenerator','SignalContext','SignalState','SIGNAL_STATE_TRANSITIONS','SIGNAL_SERVICE_CONSTANTS','KalmanFilter1D','EMASignalFilter','SignalTimingFilter','AdaptiveSignalThreshold','get_signal_service']
