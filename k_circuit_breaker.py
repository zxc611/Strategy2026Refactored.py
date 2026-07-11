# [M1-30] __ض_·__

# MODULE_ID: M1-213

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

from __future__ import annotations



import logging

import threading

from datetime import datetime

from typing import Any, Dict, List, Optional, Tuple



from ali2026v3_trading.risk.safety_meta_circuit import CircuitBreakerService

from ali2026v3_trading.risk.safety_meta_equity import DrawdownMonitorService

from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService

from ali2026v3_trading.risk.risk_support import (

    _get_tz_aware_now, safe_get_float, safe_get_int, api_version, structured_audit_log,

    CircuitBreakerStateStore,

)

from ali2026v3_trading.infra.resilience import (

    ExponentialBackoff, BoundedRetry, DbQueryTimeout, Watchdog, HeartbeatMonitor,

    CircuitBreakerHalfOpen, SlowQueryDetector, DataStalenessDetector,

    MemoryPressureGuard, RateLimitedLogger, GracefulDegradation,

    ResourceLeakDetector, AsyncTaskTimeout, safe_callback_wrapper,

    get_process_health, ProcessHealthState, AtomicConfigRef,

    stable_sum, stable_mean, stable_variance, stable_std,

    approx_equal, approx_less, approx_greater, approx_less_equal, approx_greater_equal,

    should_trigger_stop_loss, should_trigger_take_profit,

    KahanSummation, safe_divide, compute_sharpe_stable, safe_normalize_weights,

    stable_ewma, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,

)



from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ



class SafetyMetaLayer:

    """L-1安全元层 _Facade模式(P2拆分析



    通过构造函数注册个服务实例，以组合替代继承，外部接口不变换

    - _circuit_breaker_svc: 断路器核心逻辑

    - _drawdown_monitor_svc: 回撤监控逻辑

    - _hard_time_stop_svc: 硬时间止损合规+保证金展期+健康+统计



    三条硬规则（最高权限，不可被策略参数覆盖）_

    1. 速率断路器：1分钟内权益回撤超过滚动σ，暂停交易。

    2. 持仓时间硬止损：开仓后max_hold_minutes_hard分钟内浮盈从未达到min_profit_threshold_

       则在max_hold_minutes_hard+30分钟强制平仓

    3. 日最大回撤硬停止：当日累计回撤超过前5日平均日收益的daily_drawdown_multiplier倍，

       立即平掉所有仓位，禁止当日任何后续交易，直到下个交易日人工确认后恢复

    """



    ANOMALY_THRESHOLD_MULTIPLIER = 3.0

    DEFAULT_ANOMALY_THRESHOLD = 0.05

    DEFAULT_MAX_DRAWDOWN = 0.05



    # 属性委托 将外部对Facade的直接属性访问代理到子服务

    _DELEGATED_TO_DRAWDOWN_MONITOR = frozenset({

        '_daily_hard_stop_triggered', '_daily_new_open_blocked', '_daily_drawdown',

        '_daily_start_equity', '_daily_peak_equity', '_prev_5day_avg_profit',

        '_current_date', '_equity_series', '_equity_timestamps',

    })

    _DELEGATED_TO_CIRCUIT_BREAKER = frozenset({

    })



    def __getattr__(self, name: str) -> Any:

        if name in SafetyMetaLayer._DELEGATED_TO_DRAWDOWN_MONITOR:

            return getattr(self._drawdown_monitor_svc, name)

        if name in SafetyMetaLayer._DELEGATED_TO_CIRCUIT_BREAKER:

            return getattr(self._circuit_breaker_svc, name)

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")



    def __setattr__(self, name: str, value: Any) -> None:

        if name in SafetyMetaLayer._DELEGATED_TO_DRAWDOWN_MONITOR:

            setattr(self._drawdown_monitor_svc, name, value)

            return

        if name in SafetyMetaLayer._DELEGATED_TO_CIRCUIT_BREAKER:

            setattr(self._circuit_breaker_svc, name, value)

            return

        super().__setattr__(name, value)



    def __init__(self, params: Any = None):

        self.params = params

        self._lock = threading.RLock()

        self._stats_lock = threading.Lock()



        self._stats = {

            "circuit_breaker_triggers": 0,

            "circuit_breaker_calm_rejects": 0,

            "hard_time_stop_triggers": 0,

            "daily_drawdown_triggers": 0,

            "daily_hard_stop_triggers": 0,

            "total_equity_updates": 0,

        }



        self._circuit_breaker_svc = CircuitBreakerService(params, self)

        self._drawdown_monitor_svc = DrawdownMonitorService(params, self)

        self._hard_time_stop_svc = HardTimeStopAndComplianceService(params, self)



        loaded = self._circuit_breaker_svc.load_state()

        if loaded:

            self._drawdown_monitor_svc._daily_hard_stop_triggered = loaded.get("daily_hard_stop_triggered", False)

            self._drawdown_monitor_svc._daily_new_open_blocked = loaded.get("daily_new_open_blocked", False)

            self._drawdown_monitor_svc._daily_start_equity = loaded.get("daily_start_equity")

            self._drawdown_monitor_svc._daily_peak_equity = loaded.get("daily_peak_equity", 0.0)

            self._drawdown_monitor_svc._daily_drawdown = loaded.get("daily_drawdown", 0.0)

            self._drawdown_monitor_svc._prev_5day_avg_profit = loaded.get("prev_5day_avg_profit", 0.0)

            self._drawdown_monitor_svc._current_date = loaded.get("current_date")



        self._last_resume_date: Optional[str] = None

        self._last_resume_time: float = 0.0



        self._risk_db_timeout = DbQueryTimeout(default_timeout_sec=5.0)

        self._risk_watchdog = Watchdog(timeout_sec=60.0, name='risk_service')

        self._risk_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=10.0, missed_threshold=3)

        self._risk_memory_guard = MemoryPressureGuard()

        self._risk_rate_limited_logger = RateLimitedLogger(window_sec=60.0, max_per_window=3)

        self._risk_process_health = get_process_health()

        self._atomic_config = AtomicConfigRef(initial_config=params if params else {})



    def _save_circuit_breaker_state(self) -> None:

        self._circuit_breaker_svc.save_state(self._drawdown_monitor_svc.get_state_snapshot())



    def _load_circuit_breaker_state(self) -> None:

        pass



    def on_equity_update(self, equity: float) -> None:

        self._drawdown_monitor_svc.on_equity_update(

            equity, self._stats, self._circuit_breaker_svc,

            save_callback=self._save_circuit_breaker_state,

        )



    def _notify_equity_change(self, equity: float) -> None:

        self._drawdown_monitor_svc._notify_equity_change(

            equity, self._stats, save_callback=self._save_circuit_breaker_state,

        )



    def _update_equity_metrics(self, equity: float, now: float, today: str) -> None:

        self._drawdown_monitor_svc._update_equity_metrics(equity, now, today, self._stats)



    def _check_drawdown_limits(self) -> None:

        self._drawdown_monitor_svc._check_drawdown_limits(

            self._stats, self._circuit_breaker_svc,

            save_callback=self._save_circuit_breaker_state,

        )



    def _check_equity_curve_monotonicity(self, equity: float, now: float) -> None:

        self._drawdown_monitor_svc._check_equity_curve_monotonicity(equity, now)



    def _check_circuit_breaker(self, now: float) -> None:

        self._circuit_breaker_svc.check_circuit_breaker(

            now, self._drawdown_monitor_svc._equity_series,

            self._drawdown_monitor_svc._drop_pct_history,

            self._stats, self._drawdown_monitor_svc._daily_hard_stop_triggered,

        )



    def _evaluate_circuit_conditions(self, now: float):

        return self._circuit_breaker_svc._evaluate_circuit_conditions(

            now, self._drawdown_monitor_svc._equity_series,

            self._drawdown_monitor_svc._drop_pct_history, self._stats,

        )



    def _trigger_circuit_break(self, now: float, drop_pct: float, threshold: float):

        return self._circuit_breaker_svc._trigger_circuit_break(now, drop_pct, threshold, self._stats)



    def _reset_circuit_state(self, drop_pct: float, threshold: float,

                              pause_duration: float, calm_period: float) -> None:

        self._circuit_breaker_svc._reset_circuit_state(

            drop_pct, threshold, pause_duration, calm_period, self._stats,

        )



    def _check_daily_drawdown(self) -> None:

        self._drawdown_monitor_svc._check_daily_drawdown(

            self._stats, save_callback=self._save_circuit_breaker_state,

        )



    def check_position_hard_time_stop(self, position_id: str, open_time,

                                       max_profit_reached: float,

                                       profit_slope: float = 0.0,

                                       peak_profit_pct: float = 0.0,

                                       current_profit_pct: float = 0.0,

                                       bar_time: Optional[float] = None,

                                       strategy_group: str = '') -> Optional[str]:

        return self._hard_time_stop_svc.check_position_hard_time_stop(

            position_id, open_time, max_profit_reached, profit_slope,

            peak_profit_pct, current_profit_pct, bar_time, self._stats,

            strategy_group=strategy_group,

        )



    def set_logic_reversal_triggered(self, position_id: str, triggered: bool) -> None:

        self._hard_time_stop_svc.set_logic_reversal_triggered(position_id, triggered)



    def check_regulatory_compliance(self, *args, **kwargs):

        return self._hard_time_stop_svc.check_regulatory_compliance(*args, **kwargs)



    @api_version("1.0")

    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,

                                   open_positions: int = 0, max_positions: int = 50,

                                   existing_margin_used: float = 0.0) -> Dict[str, Any]:

        return self._hard_time_stop_svc.check_capital_sufficiency(

            equity, required_margin, open_positions, max_positions, existing_margin_used,

            daily_start_equity=self._drawdown_monitor_svc._daily_start_equity,

        )



    def reserve_margin(self, *args, **kwargs):

        return self._hard_time_stop_svc.reserve_margin(*args, **kwargs)



    def release_margin(self, *args, **kwargs):

        return self._hard_time_stop_svc.release_margin(*args, **kwargs)



    def check_exchange_status(self, exchange: str = "AUTO") -> Dict[str, Any]:

        return self._hard_time_stop_svc.check_exchange_status(exchange)



    def is_trading_paused(self) -> Tuple[bool, str]:

        return self._circuit_breaker_svc.is_trading_paused()



    def is_circuit_breaker_shadow_mode(self) -> bool:

        return self._circuit_breaker_svc.is_circuit_breaker_shadow_mode(

            save_callback=self._save_circuit_breaker_state,

        )



    def is_new_open_blocked(self) -> bool:

        return self._circuit_breaker_svc.is_new_open_blocked(

            self._drawdown_monitor_svc._daily_new_open_blocked,

        )



    def can_open(self) -> bool:

        return self._circuit_breaker_svc.can_open(

            self._drawdown_monitor_svc._daily_new_open_blocked,

        )



    def compute_and_track_rollover_cost(self, bar_data, params: Optional[Dict] = None) -> Dict[str, Any]:

        return self._hard_time_stop_svc.compute_and_track_rollover_cost(bar_data, params)



    def get_rollover_cost_summary(self) -> Dict[str, Any]:

        return self._hard_time_stop_svc.get_rollover_cost_summary()



    def can_close(self) -> bool:

        return True



    def is_algo_paused(self) -> Tuple[bool, str]:

        return self._circuit_breaker_svc.is_algo_paused()



    def _check_algo_circuit_breaker(self) -> Optional[str]:

        return self._circuit_breaker_svc.check_algo_circuit_breaker()



    def is_hard_stop_triggered(self) -> bool:

        return self._circuit_breaker_svc.is_hard_stop_triggered(

            self._drawdown_monitor_svc._daily_hard_stop_triggered,

        )



    _resume_token: Optional[str] = None



    def generate_resume_token(self) -> str:

        return self._circuit_breaker_svc.generate_resume_token()



    def confirm_daily_resume(self, caller_id: str = "unknown", resume_token: Optional[str] = None,

                              approval_context: Optional[Dict[str, Any]] = None) -> bool:

        return self._hard_time_stop_svc.confirm_daily_resume(

            caller_id, resume_token, approval_context,

            drawdown_monitor=self._drawdown_monitor_svc,

            circuit_breaker_service=self._circuit_breaker_svc,

            stats=self._stats,

        )



    def set_prev_5day_avg_profit(self, avg_profit: float) -> None:

        self._drawdown_monitor_svc.set_prev_5day_avg_profit(avg_profit)



    def _get_circuit_breaker_pause_sec(self) -> float:

        return self._circuit_breaker_svc._get_circuit_breaker_pause_sec()



    def _get_circuit_breaker_calm_period_sec(self) -> float:

        return self._circuit_breaker_svc._get_circuit_breaker_calm_period_sec()



    def _cancel_pending_on_circuit_breaker(self) -> None:

        self._circuit_breaker_svc._cancel_pending_on_circuit_breaker()



    def _force_position_reduction_on_circuit_breaker(self) -> None:

        self._circuit_breaker_svc._force_position_reduction_on_circuit_breaker()



    def _get_daily_drawdown_multiplier(self) -> float:

        return self._drawdown_monitor_svc._get_daily_drawdown_multiplier()



    def _verify_market_safety_before_resume(self) -> bool:

        return self._circuit_breaker_svc.verify_market_safety_before_resume(

            self._drawdown_monitor_svc._equity_series,

            self._drawdown_monitor_svc._daily_start_equity,

            self._drawdown_monitor_svc._daily_peak_equity,

            self._drawdown_monitor_svc._daily_drawdown,

        )



    def _get_hard_time_stop_minutes(self) -> float:

        return self._hard_time_stop_svc._get_hard_time_stop_minutes()



    def _get_min_profit_threshold(self) -> float:

        return self._hard_time_stop_svc._get_min_profit_threshold()



    def _get_stage1_minutes(self) -> float:

        return self._hard_time_stop_svc._get_stage1_minutes()



    def _get_stage2_minutes(self) -> float:

        return self._hard_time_stop_svc._get_stage2_minutes()



    def _get_stage1_profit_threshold(self) -> float:

        return self._hard_time_stop_svc._get_stage1_profit_threshold()



    def get_health_status(self) -> Dict[str, Any]:

        # [P0-29修复] __risk_cb_half_open 派生暂停/冷静截止时间

        cb = self._circuit_breaker_svc._risk_cb_half_open

        _trading_paused_until = (cb.opened_at + cb.open_duration

                                 if cb is not None and cb.state == CircuitBreakerHalfOpen.OPEN and cb.opened_at > 0

                                 else 0.0)

        _circuit_breaker_calm_until = (cb.opened_at + self._circuit_breaker_svc._calm_period_duration

                                       if cb is not None and cb.opened_at > 0 and self._circuit_breaker_svc._calm_period_duration > 0

                                       else 0.0)

        return self._hard_time_stop_svc.get_health_status(

            _trading_paused_until,

            _circuit_breaker_calm_until,

            self._circuit_breaker_svc._pause_reason,

            self._drawdown_monitor_svc._daily_hard_stop_triggered,

            self._drawdown_monitor_svc._daily_drawdown,

            self._drawdown_monitor_svc._daily_new_open_blocked,

            self._circuit_breaker_svc._circuit_breaker_shadow_mode,

            self._circuit_breaker_svc._circuit_breaker_shadow_until,

            self._circuit_breaker_svc._algo_paused,

            self._circuit_breaker_svc._algo_paused_until,

            self._circuit_breaker_svc._algo_pause_reason,

        )



    def get_stats(self) -> Dict[str, Any]:

        # [P0-29修复] __risk_cb_half_open 派生暂停截止时间

        cb = self._circuit_breaker_svc._risk_cb_half_open

        _trading_paused_until = (cb.opened_at + cb.open_duration

                                 if cb is not None and cb.state == CircuitBreakerHalfOpen.OPEN and cb.opened_at > 0

                                 else 0.0)

        return self._hard_time_stop_svc.get_stats(

            self._stats,

            _trading_paused_until,

            self._drawdown_monitor_svc._daily_new_open_blocked,

            self._drawdown_monitor_svc._daily_hard_stop_triggered,

            self._drawdown_monitor_svc._daily_drawdown,

        )





_safety_meta_layer: Optional[SafetyMetaLayer] = None

_safety_meta_layer_lock = threading.Lock()

_strategy_safety_layers: Dict[str, SafetyMetaLayer] = {}

_strategy_safety_lock = threading.Lock()

_MAX_STRATEGY_SAFETY_LAYERS = 50





def get_safety_meta_layer(params: Any = None, strategy_id: Optional[str] = None) -> SafetyMetaLayer:

    if strategy_id is not None:

        with _strategy_safety_lock:

            if strategy_id not in _strategy_safety_layers:

                if len(_strategy_safety_layers) >= _MAX_STRATEGY_SAFETY_LAYERS:

                    logging.warning("[SafetyMetaLayer] 策略级实例数已达上限%d，清理最旧实现", _MAX_STRATEGY_SAFETY_LAYERS)
                    _oldest_sid = next(iter(_strategy_safety_layers))

                    del _strategy_safety_layers[_oldest_sid]

                _strategy_safety_layers[strategy_id] = SafetyMetaLayer(params=params)

                logging.info('[SafetyMetaLayer] 策略级实例初始化完成 strategy_id=%s params_type=%s',

                             strategy_id, type(params).__name__ if params else 'None')

            else:

                layer = _strategy_safety_layers[strategy_id]

                if params is not None and layer.params is None:

                    layer.params = params

                    logging.info('[SafetyMetaLayer] 策略级实例参数已更新 strategy_id=%s params_type=%s',

                                 strategy_id, type(params).__name__)

            return _strategy_safety_layers[strategy_id]

    global _safety_meta_layer

    if _safety_meta_layer is None:

        with _safety_meta_layer_lock:

            if _safety_meta_layer is None:

                _safety_meta_layer = SafetyMetaLayer(params=params)

                logging.info('[SafetyMetaLayer] 首次初始化完成params_type=%s',

                             type(params).__name__ if params else 'None')

    else:

        if params is not None and _safety_meta_layer.params is None:

            with _safety_meta_layer_lock:

                if params is not None and _safety_meta_layer.params is None:

                    _safety_meta_layer.params = params

                    logging.info('[SafetyMetaLayer] 参数已更新params_type=%s',

                                 type(params).__name__)

    return _safety_meta_layer





def cleanup_safety_layer(strategy_id: str):

    with _strategy_safety_lock:

        if strategy_id in _strategy_safety_layers:

            del _strategy_safety_layers[strategy_id]

            logging.info('[SafetyMetaLayer] 策略级实例已清理 strategy_id=%s', strategy_id)





def get_risk_circuit_breaker(params: Any = None, strategy_id: Optional[str] = None) -> Optional[SafetyMetaLayer]:

    """获取风控熔断器实例（get_safety_meta_layer 的别名）_"""

    return get_safety_meta_layer(params=params, strategy_id=strategy_id)

