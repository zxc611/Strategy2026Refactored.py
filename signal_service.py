"""信号服务 - Facade层"""
import threading, logging, time
from ali2026v3_trading.signal_generator import SignalGenerator, SignalContext, SignalState, SIGNAL_STATE_TRANSITIONS
from ali2026v3_trading.signal_timing_filter import KalmanFilter1D, EMASignalFilter, SignalTimingFilter, AdaptiveSignalThreshold
try: from ali2026v3_trading.event_bus import EventBus, get_global_event_bus; _HAS_EVENT_BUS=True
except ImportError: _HAS_EVENT_BUS=False; EventBus=None
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
    def expire_stale_signals(self): return self._filter_chain.expire_stale_signals(self._history_service.get_recent(n=len(self._signal_history)),self._signal_states,self._signal_max_age_sec,self._lock)
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
__all__=['SignalService','KalmanFilter1D','EMASignalFilter','SignalTimingFilter','get_signal_service']
