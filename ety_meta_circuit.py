# [M1-36] __ȫԪ__-__·______

# MODULE_ID: M1-220

from __future__ import annotations



import json

import logging

import math

import os

import time

import threading

from collections import deque

from datetime import datetime

from typing import Any, Dict, List, Optional, Tuple



from ali2026v3_trading.config import config_params

from ali2026v3_trading.infra.shared_utils import safe_int, safe_float, CHINA_TZ as _CHINA_TZ, stable_mean, stable_variance  # P2-13: 统一CHINA_TZ; P2-32: 统一stable_mean/stable_variance

from ali2026v3_trading.risk.risk_support import (

    safe_get_float, safe_get_int, api_version, structured_audit_log,

    _get_tz_aware_now, CircuitBreakerStateStore,

)

from ali2026v3_trading.infra.resilience import (

    BoundedRetry, DbQueryTimeout, Watchdog, HeartbeatMonitor,

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









class CircuitBreakerService:

    """断路器服务务从SafetyMetaLayer提取的断路器核心逻辑



    负责：断路器触发/评估/重置/状态持久化/冷静期影子模式/算法熔断

    """



    def __init__(self, params: Any, owner: Any):

        self.params = params

        self.owner = owner

        self._lock = threading.RLock()



        # [P0-29修复] 暂停/冷静状态完全委托给 _risk_cb_half_open，不再维护平行属性能

        # 暂停截止时间 = _risk_cb_half_open.opened_at + _risk_cb_half_open.open_duration

        # 冷静截止时间 = _risk_cb_half_open.opened_at + _calm_period_duration

        self._calm_period_duration: float = 0.0

        self._pause_reason: str = ""

        self._circuit_breaker_activated_at: float = 0.0

        self._circuit_breaker_shadow_mode: bool = False

        self._circuit_breaker_shadow_until: float = 0.0



        self._algo_breakers: Dict[str, Any] = {

            'high_cancel_rate': {'triggered': False, 'paused_until': 0.0, 'threshold': 0.70, 'pause_sec': 1800},

            'excessive_self_trade': {'triggered': False, 'paused_until': 0.0, 'threshold': 3, 'pause_sec': 3600,

                                      'window_sec': 3600},

            'high_order_frequency': {'triggered': False, 'paused_until': 0.0, 'threshold': 100, 'pause_sec': 300,

                                      'window_sec': 60},

        }

        self._algo_paused: bool = False

        self._algo_paused_until: float = 0.0

        self._algo_pause_reason: str = ""



        self._circuit_breaker_state_path: str = os.path.join(

            os.path.dirname(os.path.abspath(__file__)), ".circuit_breaker_state.json"

        )



        self._risk_cb_half_open = CircuitBreakerHalfOpen(failure_threshold=3, open_duration_sec=120.0)



    def check_circuit_breaker(self, now: float, equity_series: deque, drop_pct_history: deque,

                               stats: Dict[str, Any], daily_hard_stop_triggered: bool) -> None:

        if daily_hard_stop_triggered:

            return

        result = self._evaluate_circuit_conditions(now, equity_series, drop_pct_history, stats)

        if result is None:

            return

        drop_pct, threshold = result

        pause_duration, calm_period = self._trigger_circuit_break(now, drop_pct, threshold, stats)

        self._reset_circuit_state(drop_pct, threshold, pause_duration, calm_period, stats)



    def _evaluate_circuit_conditions(self, now: float, equity_series: deque,

                                     drop_pct_history: deque, stats: Dict[str, Any]):

        if len(equity_series) < 10:

            return None

        recent = list(equity_series)

        if len(recent) < 3:

            return None

        current = recent[-1]

        one_min_ago_idx = max(0, len(recent) - 6)

        one_min_ago_val = recent[one_min_ago_idx]

        if one_min_ago_val <= 0:

            return None

        drop_pct = (one_min_ago_val - current) / one_min_ago_val

        drop_pct_history.append(drop_pct)

        _cb_sigma = safe_get_float(self.params, "circuit_breaker_trigger_sigma", 3.0)

        if len(drop_pct_history) >= 10:

            drop_pct_values = list(drop_pct_history)

            mean_drop = stable_mean(drop_pct_values)  # P2-32: 统一使用stable_mean

            std_drop = stable_variance(drop_pct_values) ** 0.5 if len(drop_pct_values) >= 2 else 0.0  # P2-32: 统一使用stable_variance

            threshold = mean_drop + _cb_sigma * std_drop

        else:

            diffs = [recent[i] - recent[i - 1] for i in range(1, len(recent))]

            mean_diff = stable_mean(diffs)  # P2-32: 统一使用stable_mean

            if len(diffs) >= 2:

                std_diff = stable_variance(diffs) ** 0.5  # P2-32: 统一使用stable_variance

            else:

                std_diff = abs(mean_diff) if mean_diff != 0 else 1.0

            threshold = _cb_sigma * std_diff / one_min_ago_val if one_min_ago_val > 0 else 0.05

        if drop_pct <= max(threshold, 0.02):

            return None

        if hasattr(self.owner, '_isolation_level'):

            from ali2026v3_trading.config.config_params import ISOLATION_LEVELS

            if self.owner._isolation_level in (ISOLATION_LEVELS.PROCESS, ISOLATION_LEVELS.CONTAINER):

                if drop_pct <= max(threshold, 0.02) * 0.8:

                    return None

        # [P0-29修复] 冷静期判断委托给 _risk_cb_half_open

        _calm_until = (self._risk_cb_half_open.opened_at + self._calm_period_duration

                       if self._risk_cb_half_open is not None

                          and self._risk_cb_half_open.opened_at > 0

                          and self._calm_period_duration > 0

                       else 0.0)

        if now < _calm_until:

            stats["circuit_breaker_calm_rejects"] += 1

            logging.info(

                "[SafetyMetaLayer] 熔断冷静期内，忽略二次触发(冷静期剩。.0fs)",

                _calm_until - now,

            )

            return None

        return (drop_pct, threshold)



    def _trigger_circuit_break(self, now: float, drop_pct: float, threshold: float,

                                stats: Dict[str, Any]):

        pause_duration = self._get_circuit_breaker_pause_sec()



        # [P0-29修复] 通过 _risk_cb_half_open 统一管理暂停状态

        # 计算有效暂停时长（如果已在暂停中则取最大值）'
        effective_pause_duration = pause_duration

        if (self._risk_cb_half_open is not None

                and self._risk_cb_half_open.state == CircuitBreakerHalfOpen.OPEN):

            current_end = self._risk_cb_half_open.opened_at + self._risk_cb_half_open.open_duration

            if current_end > now:

                new_end = now + pause_duration

                effective_end = max(current_end, new_end)

                logging.warning(

                    "[SafetyMetaLayer] R13-BIZ-07: 断路器连续触发，暂停时间延长 %.0f->%.0f",

                    current_end - now, effective_end - now,

                )

                effective_pause_duration = effective_end - now



        # 计算当前冷静期截止时间（_force_open 之前，因。force_open 会重复opened_at_

        current_calm_end = 0.0

        if (self._calm_period_duration > 0

                and self._risk_cb_half_open is not None

                and self._risk_cb_half_open.opened_at > 0):

            current_calm_end = self._risk_cb_half_open.opened_at + self._calm_period_duration



        # 强制将断路器置为 OPEN 状态（单一状态源权

        if self._risk_cb_half_open is not None:

            self._risk_cb_half_open.force_open(open_duration_sec=effective_pause_duration, opened_at=now)



        self._pause_reason = f"速率断路器 1min回撤{drop_pct:.2%} > 2.5σ阈值{threshold:.2%}"

        stats["circuit_breaker_triggers"] += 1

        self._circuit_breaker_activated_at = now



        # [P0-29修复] 冷静期通过 _calm_period_duration 管理

        calm_period = self._get_circuit_breaker_calm_period_sec()

        new_calm_end = now + calm_period

        effective_calm_end = max(current_calm_end, new_calm_end)

        # _calm_period_duration 相对手。risk_cb_half_open.opened_at（此时已等于 now_

        self._calm_period_duration = effective_calm_end - now



        self._circuit_breaker_shadow_mode = True

        self._circuit_breaker_shadow_until = now + calm_period + 300

        logging.warning(

            "[SafetyMetaLayer] _速率断路器触发！1min回撤=%.2f%%, 阈值%.2f%%, 暂停%.0f秒 冷静期%.0f秒 影子观察%.0f秒",

            drop_pct * 100, threshold * 100, pause_duration, calm_period, 300

        )

        try:

            structured_audit_log('circuit_breaker_triggered', 'blocked',

                                 {'drop_pct': round(drop_pct, 6), 'threshold': round(threshold, 6),

                                  'pause_sec': pause_duration, 'calm_sec': calm_period,

                                  'shadow_sec': 300}, severity="WARNING")

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] suppressed exception", exc_info=True)

            pass

            pass

        try:

            from ali2026v3_trading.config.config_params import get_security_responder

            _responder = get_security_responder()

            _strategy_id_ns = getattr(self.owner, '_strategy_id_ns', 'unknown')

            _blocked = _responder.report_suspicious(

                source=f"circuit_breaker:{_strategy_id_ns}",

                reason=f"1min_drawdown={drop_pct*100:.2f}%>threshold={threshold*100:.2f}%"

            )

            if _blocked:

                logging.critical("[SEC-P1-12] 安全自动阻断已激活 source=%s", _strategy_id_ns)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _sec_ex:

            logging.debug("[SEC-P1-12] report_suspicious调用失败: %s", _sec_ex)

        return (pause_duration, calm_period)



    def _reset_circuit_state(self, drop_pct: float, threshold: float,

                              pause_duration: float, calm_period: float,

                              stats: Dict[str, Any]) -> None:

        self._cancel_pending_on_circuit_breaker()

        self._force_position_reduction_on_circuit_breaker()

        try:

            from ali2026v3_trading.infra.event_bus import get_global_event_bus, CircuitBreakerTriggeredEvent

            bus = get_global_event_bus()

            cb_event = CircuitBreakerTriggeredEvent(

                reason=self._pause_reason,

                drop_pct=drop_pct,

                threshold=threshold,

                pause_duration=pause_duration,

                calm_period=calm_period,

            )

            bus.publish(cb_event, async_mode=True)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _ops04_e:

            logging.debug("[SafetyMetaLayer] OPS-P1-04: EventBus断路器告警推送异常 %s", _ops04_e)

        self.save_state({})



    def _get_circuit_breaker_pause_sec(self) -> float:

        return safe_get_float(self.params, "circuit_breaker_pause_sec", 180.0)



    def _get_circuit_breaker_calm_period_sec(self) -> float:

        return safe_get_float(self.params, "circuit_breaker_calm_period_sec", 600.0)



    def _cancel_pending_on_circuit_breaker(self) -> None:

        try:

            from ali2026v3_trading.order.order_service import get_order_service

            osvc = get_order_service()

            if osvc:

                with osvc._lock:

                    count = osvc.cancel_all_pending()

                if count > 0:

                    logging.warning("[SafetyMetaLayer] 断路器触发，已撤销 %d 笔未成交订单", count)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[SafetyMetaLayer] cancel_pending error: %s", e)

        try:

            from ali2026v3_trading.config.state_param import get_state_param_manager

            spm = get_state_param_manager()

            if spm and hasattr(spm, 'reset_state_cache'):

                spm.reset_state_cache()

                logging.info("[SafetyMetaLayer] 断路器触发后已清空state_param_manager状态缓存")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[SafetyMetaLayer] state_param_manager reset error: %s", e)

        try:

            from ali2026v3_trading.data.width_cache import get_width_cache

            wc = get_width_cache()

            if wc and hasattr(wc, 'reset_cache'):

                wc.reset_cache()

                logging.info("[SafetyMetaLayer] 断路器触发后已清空width_cache状态缓存")

            elif wc and hasattr(wc, 'clear'):

                wc.clear()

                logging.info("[SafetyMetaLayer] 断路器触发后已清空width_cache")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[SafetyMetaLayer] width_cache reset error: %s", e)



    def _force_position_reduction_on_circuit_breaker(self) -> None:

        try:

            from ali2026v3_trading.position.position_service import get_position_service

            _ps = get_position_service()

            if _ps is None:

                return

            positions = getattr(_ps, 'positions', {})

            if not positions:

                return

            reduction_ratio = safe_get_float(self.params, "circuit_breaker_reduction_ratio", 0.5)

            reduced_count = 0

            for inst_id in list(positions.keys()):

                pos_dict = positions.get(inst_id)

                if pos_dict is None:

                    continue

                with _ps._get_instrument_lock(inst_id):
                    for pid in list(pos_dict.keys()):
                        rec = pos_dict.get(pid)
                        if rec is None or getattr(rec, 'volume', 0) == 0:
                            continue
                        if getattr(rec, 'closing_order_id', '') or getattr(rec, '_closing', False):
                            continue
                        try:
                            reduce_volume = max(1, int(abs(rec.volume) * reduction_ratio))
                            from ali2026v3_trading.order.order_service import get_order_service
                            osvc = get_order_service()
                            if osvc:
                                direction = 'SELL' if rec.volume > 0 else 'BUY'
                                price = 0.0
                                try:
                                    from ali2026v3_trading.data.data_service import get_data_service
                                    ds = get_data_service()
                                    if ds and ds.realtime_cache:
                                        mp = ds.realtime_cache.get_latest_price(rec.instrument_id)
                                        if mp and mp > 0:
                                            price = mp
                                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                    pass
                                if price <= 0:
                                    price = getattr(rec, 'current_price', 0) or getattr(rec, 'open_price', 0)
                                _risk_ref_price = 0.0
                                if price > 0 and price == getattr(rec, 'open_price', 0):
                                    _risk_ref_price = price
                                if price > 0:
                                    rec._closing = True
                                    rec.closing_order_id = f"PENDING_RISK_REDUCE_{rec.position_id}"
                                    rec.close_method = 'risk_reduce'
                                    rec.close_reason = f'circuit_breaker_reduce_{reduction_ratio*100:.0f}pct'
                                    _risk_result = osvc.send_order(
                                        instrument_id=rec.instrument_id,
                                        volume=reduce_volume,
                                        price=price,
                                        direction=direction,
                                        action='CLOSE',
                                        exchange=getattr(rec, 'exchange', ''),
                                        signal_id=f"RISK_REDUCE_{rec.instrument_id}",
                                        ref_price=_risk_ref_price,
                                    )
                                    if _risk_result is not None and getattr(_risk_result, 'ok', False):
                                        reduced_count += 1
                                        _actual_oid = getattr(_risk_result, 'order_id', '')
                                        if _actual_oid:
                                            rec.closing_order_id = _actual_oid
                                        _pnl_mult = 1.0 if getattr(rec, 'direction', '') in ('long', 'BUY') else -1.0
                                        rec.realized_pnl = _pnl_mult * (price - rec.open_price) * reduce_volume
                                    else:
                                        rec.closing_order_id = 'CANNOT_CLOSE'
                                        if hasattr(rec, 'close_method'):
                                            rec.close_method = ''
                                        logging.error("[R37-UNIQUE-CLOSE] A2熔断减仓send_order失败,设置CANNOT_CLOSE: inst=%s pos_id=%s",
                                                      rec.instrument_id, rec.position_id)
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            logging.warning(
                                "[SafetyMetaLayer] INV-P1-10: 减仓失败 instrument=%s pid=%s error=%s",
                                inst_id, pid, e,
                            )

            if reduced_count > 0:

                logging.critical(

                    "[SafetyMetaLayer] INV-P1-10: 熔断后强制减仓完成reduced=%d positions ratio=%.0f%%",

                    reduced_count, reduction_ratio * 100,

                )

                try:

                    from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskEvent

                    _eb = get_global_event_bus()

                    if _eb and not getattr(_eb, '_shutdown', True):

                        _eb.publish(RiskEvent(

                            risk_type='circuit_breaker_force_reduction',

                            level='CRITICAL',

                            message=f"INV-P1-10: 熔断后强制减。reduced={reduced_count} ratio={reduction_ratio*100:.0f}%",

                        ), async_mode=True)

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _eb_e:

                    logging.debug("[SafetyMetaLayer] INV-P1-10: 事件总线告警失败: %s", _eb_e)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[SafetyMetaLayer] INV-P1-10: 强制减仓异常: %s", e)



    def is_trading_paused(self) -> Tuple[bool, str]:

        with self._lock:

            now = time.time()

            # [P0-29修复] 暂停判断委托给给risk_cb_half_open，不再使。_trading_paused_until

            cb = self._risk_cb_half_open

            if cb is not None and cb.state == CircuitBreakerHalfOpen.OPEN and cb.opened_at > 0:

                pause_end = cb.opened_at + cb.open_duration

                if now < pause_end:

                    remaining = pause_end - now

                    return True, f"{self._pause_reason}, 剩余{remaining:.0f}秒"

            if now < self._algo_paused_until:

                remaining = self._algo_paused_until - now

                return True, f"{self._algo_pause_reason}, 剩余{remaining:.0f}秒"

            if self._pause_reason:

                _prev_reason = self._pause_reason

                self._pause_reason = ""

                logging.info("[P1-R9-21] 交易暂停已恢复 原暂停原子 %s", _prev_reason)

                if self._risk_cb_half_open is not None:

                    self._risk_cb_half_open.record_success()

            return False, ""



    def is_circuit_breaker_shadow_mode(self, save_callback=None) -> bool:

        with self._lock:

            if self._circuit_breaker_shadow_mode:

                now = time.time()

                if now >= self._circuit_breaker_shadow_until:

                    self._circuit_breaker_shadow_mode = False

                    if save_callback:

                        save_callback()

                    logging.info("[SafetyMetaLayer] 影子模式观察期结束，恢复正常交易")

                    return False

                return True

            return False



    def is_new_open_blocked(self, daily_new_open_blocked: bool) -> bool:

        with self._lock:

            return daily_new_open_blocked



    def can_open(self, daily_new_open_blocked: bool) -> bool:

        paused, _ = self.is_trading_paused()

        if not paused and self._risk_cb_half_open is not None:

            if not self._risk_cb_half_open.allow_request():

                return False

        return not paused and not daily_new_open_blocked



    def is_algo_paused(self) -> Tuple[bool, str]:

        with self._lock:

            if self._algo_paused and time.time() < self._algo_paused_until:

                remaining = self._algo_paused_until - time.time()

                return True, f"{self._algo_pause_reason}, 剩余{remaining:.0f}秒"

            if self._algo_paused and time.time() >= self._algo_paused_until:

                self._algo_paused = False

                self._algo_pause_reason = ""

                logging.info("[SafetyMetaLayer] CMP-P1-13: 算法交易熔断已自动恢复")

            return False, ""



    def check_algo_circuit_breaker(self) -> Optional[str]:

        try:

            now = time.time()

            from ali2026v3_trading.order.order_service import get_order_service

            _os = get_order_service()

            if not _os:

                return None

            cutoff_300 = now - 300

            with _os._lock:

                recent_cancels = sum(1 for t in _os._cancel_count_window if t >= cutoff_300)

                recent_orders = sum(1 for t in _os._order_count_window if t >= cutoff_300)

            if recent_orders > 0:

                cancel_rate = recent_cancels / recent_orders

                if cancel_rate > 0.70:

                    reason = f"撤单率{cancel_rate:.1%}>70%, 暂停算法交易30分钟"

                    logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)

                    with self._lock:

                        self._algo_paused = True

                        self._algo_paused_until = now + 1800

                        self._algo_pause_reason = reason

                        self._algo_breakers['high_cancel_rate']['triggered'] = True

                        self._algo_breakers['high_cancel_rate']['paused_until'] = self._algo_paused_until

                    return reason

            cutoff_3600 = now - 3600

            with _os._lock:

                recent_st_bans = sum(1 for ban_until in _os._self_trade_bans.values()

                                      if ban_until > now and ban_until - 3600 <= now)

            if recent_st_bans > 3:

                reason = f"自成交禁止{recent_st_bans}_小时>3, 暂停算法交易1小时"

                logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)

                with self._lock:

                    self._algo_paused = True

                    self._algo_paused_until = now + 3600

                    self._algo_pause_reason = reason

                    self._algo_breakers['excessive_self_trade']['triggered'] = True

                    self._algo_breakers['excessive_self_trade']['paused_until'] = self._algo_paused_until

                return reason

            cutoff_60 = now - 60

            with _os._lock:

                recent_orders_60 = sum(1 for t in _os._order_count_window if t >= cutoff_60)

            if recent_orders_60 > 100:

                reason = f"报单频率{recent_orders_60}_分钟>100, 暂停5分钟"

                logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)

                with self._lock:

                    self._algo_paused = True

                    self._algo_paused_until = now + 300

                    self._algo_pause_reason = reason

                    self._algo_breakers['high_order_frequency']['triggered'] = True

                    self._algo_breakers['high_order_frequency']['paused_until'] = self._algo_paused_until

                return reason

            return None

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.debug("[SafetyMetaLayer] CMP-P1-13: 算法熔断检查异常 %s", e)

            return None



    def is_hard_stop_triggered(self, daily_hard_stop_triggered: bool) -> bool:

        with self._lock:

            return daily_hard_stop_triggered



    _resume_token: Optional[str] = None



    def generate_resume_token(self) -> str:

        import secrets as _secrets

        self._resume_token = _secrets.token_hex(16)

        logging.critical(

            "[SafetyMetaLayer] 恢复令牌已生） %s... (必须传入confirm_daily_resume验证)",

            self._resume_token[:8]

        )

        return self._resume_token



    def verify_market_safety_before_resume(self, equity_series: deque,

                                            daily_start_equity: Optional[float],

                                            daily_peak_equity: float,

                                            daily_drawdown: float) -> bool:

        now = time.time()

        # [P0-29修复] 暂停/冷静期判断委托给 _risk_cb_half_open

        cb = self._risk_cb_half_open

        if cb is not None and cb.state == CircuitBreakerHalfOpen.OPEN and cb.opened_at > 0:

            pause_end = cb.opened_at + cb.open_duration

            if pause_end > now:

                logging.warning(

                    "[SafetyMetaLayer] INV-RSK-01: 断路器暂停期未结束paused_until=%.0f",

                    pause_end,

                )

                return False

        _calm_until = (cb.opened_at + self._calm_period_duration

                       if cb is not None and cb.opened_at > 0 and self._calm_period_duration > 0

                       else 0.0)

        if _calm_until > now:

            logging.warning(

                "[SafetyMetaLayer] INV-RSK-01: 断路器冷静期未结束calm_until=%.0f",

                _calm_until,

            )

            return False

        if len(equity_series) >= 3 and daily_start_equity and daily_start_equity > 0:

            recent_equities = list(equity_series)[-3:]

            drawdowns = [(daily_peak_equity - eq) / daily_start_equity for eq in recent_equities]

            if all(d1 < d2 for d1, d2 in zip(drawdowns, drawdowns[1:])):

                logging.warning(

                    "[SafetyMetaLayer] INV-RSK-01: 回撤仍在恶化 drawdowns=[%s]",

                    ', '.join(f'{d:.4f}' for d in drawdowns),

                )

                return False

        with self._lock:

            try:

                from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine

                _sse = get_shadow_strategy_engine()

                if _sse is not None:

                    if _sse.is_absolute_ev_paused():

                        logging.warning("[SafetyMetaLayer] INV-RSK-01: 影子引擎EV暂停仍生效")

                        return False

                    if _sse.is_degradation_active():

                        logging.warning("[SafetyMetaLayer] INV-RSK-01: 影子引擎降级仍生效")

                        return False

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _sm_ev_err:

                logging.warning("[R22-EP-01b] SafetyMetaLayer影子引擎检查异常 %s", _sm_ev_err)

                return False

        if self._algo_paused and self._algo_paused_until > now:

            logging.warning(

                "[SafetyMetaLayer] INV-RSK-01: 算法熔断仍生。paused_until=%.0f",

                self._algo_paused_until,

            )

            return False

        return True



    def save_state(self, state_data: Dict[str, Any]) -> None:

        """委托CircuitBreakerStateStore进行状态持久化"""

        if not hasattr(self, "_state_store") or self._state_store is None:

            self._state_store = CircuitBreakerStateStore(self.params)

        # [P0-29修复] __risk_cb_half_open 派生暂停/冷静截止时间

        cb = self._risk_cb_half_open

        _trading_paused_until = (cb.opened_at + cb.open_duration

                                 if cb is not None and cb.state == CircuitBreakerHalfOpen.OPEN and cb.opened_at > 0

                                 else 0.0)

        _circuit_breaker_calm_until = (cb.opened_at + self._calm_period_duration

                                       if cb is not None and cb.opened_at > 0 and self._calm_period_duration > 0

                                       else 0.0)

        self._state_store.save_state(

            _trading_paused_until, self._pause_reason,

            _circuit_breaker_calm_until, self._circuit_breaker_shadow_mode,

            self._circuit_breaker_shadow_until, self._circuit_breaker_activated_at,

            state_data,

        )



    def load_state(self) -> Dict[str, Any]:

        """委托CircuitBreakerStateStore进行状态恢复"""

        if not hasattr(self, "_state_store") or self._state_store is None:

            self._state_store = CircuitBreakerStateStore(self.params)

        result = self._state_store.load_state()

        # Apply loaded raw state to self

        raw = result.pop("_loaded_raw", {})

        if raw:

            now = time.time()

            # [P0-29修复] 通过 force_open 恢复暂停状态

            if raw.get("trading_paused_until", 0) > now:

                remaining = raw["trading_paused_until"] - now

                if self._risk_cb_half_open is not None:

                    self._risk_cb_half_open.force_open(open_duration_sec=remaining, opened_at=now)

                self._pause_reason = raw.get("pause_reason", "")

            # [P0-29修复] 通过 _calm_period_duration 恢复冷静期

            if raw.get("circuit_breaker_calm_until", 0) > now:

                calm_until = raw["circuit_breaker_calm_until"]

                if self._risk_cb_half_open is not None and self._risk_cb_half_open.opened_at > 0:

                    self._calm_period_duration = calm_until - self._risk_cb_half_open.opened_at

                else:

                    # 暂停已过期但冷静期未过期，以激活时间或当前时间为基。'
                    activated_at = raw.get("circuit_breaker_activated_at", 0.0)

                    if activated_at > 0:

                        self._calm_period_duration = calm_until - activated_at

                    else:

                        self._calm_period_duration = calm_until - now

            if raw.get("circuit_breaker_shadow_mode", False):

                if raw.get("circuit_breaker_shadow_until", 0) > now:

                    self._circuit_breaker_shadow_mode = True

                    self._circuit_breaker_shadow_until = raw["circuit_breaker_shadow_until"]

            self._circuit_breaker_activated_at = raw.get("circuit_breaker_activated_at", 0.0)

        return result

