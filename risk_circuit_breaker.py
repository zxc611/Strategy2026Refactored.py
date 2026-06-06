from __future__ import annotations

import json
import logging
import math
import os
import time
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading import config_params
from ali2026v3_trading.shared_utils import safe_int, safe_float
from ali2026v3_trading.resilience_utils import (
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
from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_jsonl_append_line
from ali2026v3_trading.margin_manager import MarginManager
from ali2026v3_trading.compliance_checker import ComplianceChecker

_CHINA_TZ = timezone(timedelta(hours=8))


def _get_tz_aware_now() -> datetime:
    return datetime.now(_CHINA_TZ)


def safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_float] Error getting %s: %s", attr, e)
        return default


def safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return int(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_int] Error getting %s: %s", attr, e)
        return default


def api_version(version: str):
    def decorator(func):
        func._api_version = version
        func._api_versioned = True
        return func
    return decorator


def structured_audit_log(event_type: str, action: str, details: Dict[str, Any] = None,
                         severity: str = "INFO") -> None:
    try:
        from ali2026v3_trading.risk_service import audit_chain_append
        audit_chain_append(event_type, {'action': action, 'details': details or {}, 'severity': severity})
    except ImportError:
        logging.debug("[structured_audit_log] audit_chain unavailable, skipping log for %s/%s", event_type, action)
    except Exception as e:
        logging.debug("[structured_audit_log] log failed: %s", e)


class SafetyMetaLayer:
    """L-1安全元层 — 独立于策略模型的账户级生存保障

    P2-R11-09修复: 隔离说明 — SafetyMetaLayer通过get_safety_meta_layer(strategy_id)实现策略级实例隔离
    每个strategy_id对应独立的SafetyMetaLayer实例，断路器/回撤监控互不影响
    注意: 全局默认实例(_safety_meta_layer)仍存在，仅用于strategy_id=None的向后兼容路径
    新代码应始终传递strategy_id参数以获得策略级隔离

    三条硬规则（最高权限，不可被策略参数覆盖）：
    1. 速率断路器：1分钟内权益回撤超过滚动3σ，暂停交易。
       熔断冷静期(circuit_breaker_calm_period_sec)：首次触发后N秒内不再二次触发，防止极端行情中频繁"抽搐"
    2. 持仓时间硬止损：开仓后max_hold_minutes_hard分钟内浮盈从未达到min_profit_threshold，
       则在max_hold_minutes_hard+30分钟强制平仓
    3. 日最大回撤硬停止：当日累计回撤超过前5日平均日收益的daily_drawdown_multiplier倍，
       立即平掉所有仓位，禁止当日任何后续交易，直到下个交易日人工确认后恢复

    可回测优化参数：
    - ANOMALY_THRESHOLD_MULTIPLIER: 异常检测阈值乘数（默认3.0σ）
    - DEFAULT_ANOMALY_THRESHOLD: 无历史数据时的默认阈值
    - DEFAULT_MAX_DRAWDOWN: 无历史均值时的默认最大回撤阈值
    """

    ANOMALY_THRESHOLD_MULTIPLIER = 3.0
    DEFAULT_ANOMALY_THRESHOLD = 0.05
    DEFAULT_MAX_DRAWDOWN = 0.05

    def __init__(self, params: Any = None):
        self.params = params
        self._lock = threading.RLock()
        self._stats_lock = threading.Lock()

        self._equity_series: deque = deque(maxlen=60)
        self._equity_timestamps: deque = deque(maxlen=60)
        self._drop_pct_history: deque = deque(maxlen=60)

        self._last_equity_date: Optional[str] = None

        self._trading_paused_until: float = 0.0
        self._pause_reason: str = ""
        self._circuit_breaker_calm_until: float = 0.0
        self._circuit_breaker_activated_at: float = 0.0
        self._circuit_breaker_shadow_mode: bool = False
        self._circuit_breaker_shadow_until: float = 0.0

        self._daily_start_equity: Optional[float] = None
        self._daily_peak_equity: float = 0.0
        self._daily_drawdown: float = 0.0
        self._prev_5day_avg_profit: float = 0.0
        self._daily_new_open_blocked: bool = False
        self._daily_hard_stop_triggered: bool = False
        self._current_date: Optional[str] = None
        self._last_resume_date: Optional[str] = None
        self._last_resume_time: float = 0.0

        self._stats = {
            "circuit_breaker_triggers": 0,
            "circuit_breaker_calm_rejects": 0,
            "hard_time_stop_triggers": 0,
            "daily_drawdown_triggers": 0,
            "daily_hard_stop_triggers": 0,
            "total_equity_updates": 0,
        }

        self._logic_reversal_priority: Dict[str, bool] = {}

        self._rollover_cost_bps: float = 0.0
        self._rollover_count: int = 0

        self._equity_intraday_low: Optional[float] = None
        self._equity_monotonicity_anomaly_count: int = 0

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

        # R27-SML-FIX: 委托子模块
        self._margin_manager = MarginManager()
        self._compliance_checker = ComplianceChecker(self)

        self._circuit_breaker_state_path: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".circuit_breaker_state.json"
        )
        self._load_circuit_breaker_state()

        self._risk_db_timeout = DbQueryTimeout(default_timeout_sec=5.0)
        self._risk_watchdog = Watchdog(timeout_sec=config_params.WATCHDOG_TIMEOUT_RISK_SEC, name='risk_service')
        self._risk_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=10.0, missed_threshold=3)
        self._risk_cb_half_open = CircuitBreakerHalfOpen(failure_threshold=3, open_duration_sec=120.0)
        self._risk_memory_guard = MemoryPressureGuard()
        self._risk_rate_limited_logger = RateLimitedLogger(window_sec=60.0, max_per_window=3)
        self._risk_process_health = get_process_health()
        self._atomic_config = AtomicConfigRef(initial_config=self.params if hasattr(self, 'params') else {})

    def _save_circuit_breaker_state(self) -> None:
        try:
            now = time.time()
            state = {
                "trading_paused_until": self._trading_paused_until,
                "pause_reason": self._pause_reason,
                "circuit_breaker_calm_until": self._circuit_breaker_calm_until,
                "circuit_breaker_shadow_mode": self._circuit_breaker_shadow_mode,
                "circuit_breaker_shadow_until": self._circuit_breaker_shadow_until,
                "circuit_breaker_activated_at": getattr(self, '_circuit_breaker_activated_at', 0.0),
                "daily_hard_stop_triggered": self._daily_hard_stop_triggered,
                "daily_new_open_blocked": self._daily_new_open_blocked,
                "daily_start_equity": self._daily_start_equity,
                "daily_peak_equity": self._daily_peak_equity,
                "daily_drawdown": self._daily_drawdown,
                "prev_5day_avg_profit": self._prev_5day_avg_profit,
                "current_date": self._current_date,
                "saved_at": now,
            }
            tmp_path = self._circuit_breaker_state_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(json_dumps(state))
            os.replace(tmp_path, self._circuit_breaker_state_path)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] 断路器状态持久化失败: %s", e)

    def _load_circuit_breaker_state(self) -> None:
        try:
            if not os.path.exists(self._circuit_breaker_state_path):
                return
            with open(self._circuit_breaker_state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            now = time.time()
            if state.get("trading_paused_until", 0) > now:
                self._trading_paused_until = state["trading_paused_until"]
                self._pause_reason = state.get("pause_reason", "")
            if state.get("circuit_breaker_calm_until", 0) > now:
                self._circuit_breaker_calm_until = state["circuit_breaker_calm_until"]
            if state.get("circuit_breaker_shadow_mode", False):
                if state.get("circuit_breaker_shadow_until", 0) > now:
                    self._circuit_breaker_shadow_mode = True
                    self._circuit_breaker_shadow_until = state["circuit_breaker_shadow_until"]
            self._circuit_breaker_activated_at = state.get("circuit_breaker_activated_at", 0.0)
            saved_date = state.get("current_date")
            today = _get_tz_aware_now().strftime("%Y-%m-%d")
            if saved_date and saved_date == today:
                self._daily_hard_stop_triggered = state.get("daily_hard_stop_triggered", False)
                self._daily_new_open_blocked = state.get("daily_new_open_blocked", False)
                self._daily_start_equity = state.get("daily_start_equity")
                self._daily_peak_equity = state.get("daily_peak_equity", 0.0)
                self._daily_drawdown = state.get("daily_drawdown", 0.0)
                self._prev_5day_avg_profit = state.get("prev_5day_avg_profit", 0.0)
                self._current_date = saved_date
            else:
                _saved_hard_stop = state.get("daily_hard_stop_triggered", False)
                if _saved_hard_stop:
                    self._daily_hard_stop_triggered = True
                    logging.warning(
                        "[P0-R9-08] DR-08: 断路器hard_stop跨日保留(%s→%s), 需人工confirm_daily_resume()恢复",
                        saved_date, today,
                    )
                else:
                    self._daily_hard_stop_triggered = False
                self._daily_new_open_blocked = False
                logging.info(
                    "[SafetyMetaLayer] DR-08: 断路器状态跨日(%s→%s), 日回撤状态已重置",
                    saved_date, today,
                )
            logging.info(
                "[SafetyMetaLayer] 断路器状态已恢复: paused_until=%.1f calm_until=%.1f shadow=%s hard_stop=%s new_open_blocked=%s",
                self._trading_paused_until, self._circuit_breaker_calm_until,
                self._circuit_breaker_shadow_mode,
                self._daily_hard_stop_triggered, self._daily_new_open_blocked,
            )
        except Exception as e:
            logging.warning("[SafetyMetaLayer] 断路器状态恢复失败: %s", e)

    def on_equity_update(self, equity: float) -> None:
        now = time.time()
        _dt_now = datetime.fromtimestamp(now, tz=_CHINA_TZ)
        if _dt_now.hour >= 18:
            today = _dt_now.strftime("%Y-%m-%d")
        else:
            _yesterday = _dt_now - timedelta(days=1)
            today = _yesterday.strftime("%Y-%m-%d")

        with self._lock:
            self._stats["total_equity_updates"] += 1

            if equity <= 0:
                self._notify_equity_change(equity)
                return

            self._update_equity_metrics(equity, now, today)
            self._check_drawdown_limits()

    def _notify_equity_change(self, equity: float) -> None:
        """负权益紧急处理：阻断交易、撤销挂单、强制减仓"""
        self._daily_hard_stop_triggered = True
        self._daily_new_open_blocked = True
        self._daily_drawdown = 1.0
        self._stats["negative_equity_triggers"] = self._stats.get("negative_equity_triggers", 0) + 1
        logging.critical(
            "[SafetyMetaLayer] INV-CAP-01: negative equity hard stop! equity=%.2f <= 0, "
            "force block all trading, forced position reduction, manual confirm required",
            equity,
        )
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                count = osvc.cancel_all_pending()
                if count > 0:
                    logging.critical("[SafetyMetaLayer] negative equity, cancelled %d pending orders", count)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] negative equity cancel failed: %s", e)
        try:
            from ali2026v3_trading.position_service import get_position_service
            _ps = get_position_service()
            if _ps and hasattr(_ps, 'positions'):
                _skipped_limit = 0
                _closed_ok = 0
                for _inst_id, _pos_dict in list(_ps.positions.items()):
                    for _pid, _rec in list(_pos_dict.items()):
                        if _rec.volume == 0:
                            continue
                        _close_dir = 'SELL' if _rec.volume > 0 else 'BUY'
                        _skip = False
                        try:
                            from ali2026v3_trading.risk_service import get_risk_service
                            _rs = get_risk_service()
                            if _rs:
                                _lp = 0.0
                                try:
                                    from ali2026v3_trading.data_service import get_data_service
                                    _ds = get_data_service()
                                    if _ds and _ds.realtime_cache:
                                        _lp = _ds.realtime_cache.get_latest_price(_inst_id) or 0.0
                                except Exception as _lp_err:
                                    logging.warning("[R22-P1-NEW] 负权益减仓: 行情获取失败(跳过涨跌停检查): %s", _lp_err)
                                if _lp > 0:
                                    _lim = _rs.is_at_price_limit(_inst_id, _lp)
                                    if _lim.get('is_limit_up') and _close_dir == 'SELL':
                                        logging.warning("[SafetyMetaLayer] EX-04: 涨停板卖出排队 %s price=%.2f", _inst_id, _lp)
                                        _skip = True
                                    if _lim.get('is_limit_down') and _close_dir == 'BUY':
                                        logging.warning("[SafetyMetaLayer] EX-04: 跌停板买入排队 %s price=%.2f", _inst_id, _lp)
                                        _skip = True
                        except Exception as _lim_err:
                            logging.warning("[R22-P1-NEW] 负权益减仓: 涨跌停检查异常(减仓不受约束): %s", _lim_err)
                        if _skip:
                            _skipped_limit += 1
                            continue
                        try:
                            _ps._trigger_close_position(_rec, "INV-CAP-01: negative equity forced reduction")
                            _closed_ok += 1
                        except Exception as _ce:
                            logging.warning("[SafetyMetaLayer] EX-04: force close %s failed: %s", _inst_id, _ce)
                logging.critical("[SafetyMetaLayer] INV-CAP-01: forced reduction closed=%d skipped_limit=%d", _closed_ok, _skipped_limit)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] INV-CAP-01: forced position reduction failed: %s", e)
        self._save_circuit_breaker_state()

    def _update_equity_metrics(self, equity: float, now: float, today: str) -> None:
        """更新权益指标：日切、峰值/回撤、权益序列"""
        if self._current_date != today:
            self._current_date = today
            self._daily_start_equity = equity
            self._daily_peak_equity = equity
            self._daily_drawdown = 0.0
            self._drop_pct_history.clear()
            if self._daily_hard_stop_triggered:
                self._daily_new_open_blocked = True
                logging.warning(
                    "[SafetyMetaLayer] 新交易日(%s)，日回撤硬停止仍然生效。"
                    "必须调用confirm_daily_resume()经审批后才能恢复交易。",
                    today,
                )
            else:
                self._daily_new_open_blocked = False

        self._daily_peak_equity = max(self._daily_peak_equity, equity)
        if self._daily_start_equity and self._daily_start_equity > 0:
            unrealized_pnl = 0.0
            try:
                from ali2026v3_trading.position_service import get_position_service
                _ps = get_position_service()
                if _ps:
                    for _inst_id, pos_dict in _ps.positions.items():
                        for _pid, rec in pos_dict.items():
                            if rec.volume != 0 and rec.open_price > 0:
                                try:
                                    from ali2026v3_trading.data_service import get_data_service
                                    _ds = get_data_service()
                                    _mp = rec.open_price
                                    if _ds and _ds.realtime_cache:
                                        _lp = _ds.realtime_cache.get_latest_price(rec.instrument_id)
                                        if _lp and _lp > 0:
                                            _mp = _lp
                                    if rec.volume > 0:
                                        unrealized_pnl += rec.volume * (_mp - rec.open_price)
                                    else:
                                        unrealized_pnl += rec.volume * (rec.open_price - _mp)
                                except Exception:
                                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                    pass
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass
            equity_with_unrealized = equity + unrealized_pnl
            self._daily_drawdown = (self._daily_peak_equity - equity_with_unrealized) / self._daily_start_equity

        _safe_equity = equity if math.isfinite(equity) else self._daily_peak_equity
        if not math.isfinite(equity):
            logging.warning("[R4-P-06] Non-finite equity detected: %.6f, replaced with daily_peak=%.6f", equity, self._daily_peak_equity)
        self._equity_series.append(_safe_equity)
        self._equity_timestamps.append(now)

        today_str = _get_tz_aware_now().strftime("%Y-%m-%d")
        if self._last_equity_date is not None and self._last_equity_date != today_str:
            self._equity_series.clear()
            self._equity_timestamps.clear()
            self._equity_series.append(_safe_equity)
            self._equity_timestamps.append(now)
            logging.info(
                "[SafetyMetaLayer] R13-P2-BIZ-03: 检测到隔夜缺口(%s→%s), "
                "已重置滚动窗口防止虚假熔断触发",
                self._last_equity_date, today_str,
            )
        self._last_equity_date = today_str

        self._check_equity_curve_monotonicity(equity, now)

    def _check_drawdown_limits(self) -> None:
        """检查断路器和日回撤限制"""
        if not self._daily_hard_stop_triggered:
            self._check_circuit_breaker(time.time())
            self._check_daily_drawdown()
    def _check_equity_curve_monotonicity(self, equity: float, now: float) -> None:
        try:
            if self._equity_intraday_low is None:
                self._equity_intraday_low = equity
            else:
                if equity < self._equity_intraday_low:
                    prev_equity = list(self._equity_series)[-2] if len(self._equity_series) >= 2 else None
                    if prev_equity is not None and prev_equity > self._equity_intraday_low:
                        drop_pct = (self._equity_intraday_low - equity) / self._equity_intraday_low
                        if drop_pct > 0.01:
                            self._equity_monotonicity_anomaly_count += 1
                            logging.warning(
                                "[SafetyMetaLayer] INV-P1-11: 权益曲线单调性异常! "
                                "equity=%.2f < intraday_low=%.2f drop=%.2f%% anomaly_count=%d",
                                equity, self._equity_intraday_low, drop_pct * 100,
                                self._equity_monotonicity_anomaly_count,
                            )
                            try:
                                from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                                _eb = get_global_event_bus()
                                if _eb and not getattr(_eb, '_shutdown', True):
                                    _eb.publish(RiskEvent(
                                        risk_type='equity_curve_non_monotonic',
                                        level='HIGH' if self._equity_monotonicity_anomaly_count < 3 else 'CRITICAL',
                                        message=f"INV-P1-11: 权益曲线单调性异常 "
                                                f"equity={equity:.2f} < intraday_low={self._equity_intraday_low:.2f} "
                                                f"drop={drop_pct*100:.2f}% anomaly_count={self._equity_monotonicity_anomaly_count}",
                                    ), async_mode=True)
                            except Exception as _eb_e:
                                logging.debug("[SafetyMetaLayer] INV-P1-11: 事件总线告警失败: %s", _eb_e)
                            if self._equity_monotonicity_anomaly_count >= 3:
                                logging.critical(
                                    "[SafetyMetaLayer] INV-P1-11: 权益曲线连续异常(%d次)，"
                                    "可能存在未记录亏损或数据错误",
                                    self._equity_monotonicity_anomaly_count,
                                )
                    self._equity_intraday_low = equity
                else:
                    if self._equity_monotonicity_anomaly_count > 0:
                        self._equity_monotonicity_anomaly_count = 0
        except Exception as e:
            logging.debug("[SafetyMetaLayer] INV-P1-11: 权益曲线单调性检查异常: %s", e)

    def _check_circuit_breaker(self, now: float) -> None:
        result = self._evaluate_circuit_conditions(now)
        if result is None:
            return
        drop_pct, threshold = result
        pause_duration, calm_period = self._trigger_circuit_break(now, drop_pct, threshold)
        self._reset_circuit_state(drop_pct, threshold, pause_duration, calm_period)

    def _evaluate_circuit_conditions(self, now: float):
        """评估断路器触发条件，返回(drop_pct, threshold)或None"""
        if len(self._equity_series) < 10:
            return None

        recent = list(self._equity_series)
        if len(recent) < 3:
            return None

        current = recent[-1]
        one_min_ago_idx = max(0, len(recent) - 6)
        one_min_ago_val = recent[one_min_ago_idx]

        if one_min_ago_val <= 0:
            return None

        drop_pct = (one_min_ago_val - current) / one_min_ago_val

        self._drop_pct_history.append(drop_pct)

        import statistics
        _cb_sigma = safe_get_float(self.params, "circuit_breaker_trigger_sigma", self.ANOMALY_THRESHOLD_MULTIPLIER)
        if len(self._drop_pct_history) >= 10:
            drop_pct_values = list(self._drop_pct_history)
            mean_drop = statistics.mean(drop_pct_values)
            std_drop = statistics.stdev(drop_pct_values) if len(drop_pct_values) >= 2 else 0.0
            threshold = mean_drop + _cb_sigma * std_drop
        else:
            diffs = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
            mean_diff = statistics.mean(diffs)
            if len(diffs) >= 2:
                std_diff = statistics.stdev(diffs)
            else:
                std_diff = abs(mean_diff) if mean_diff != 0 else 1.0
            threshold = _cb_sigma * std_diff / one_min_ago_val if one_min_ago_val > 0 else self.DEFAULT_ANOMALY_THRESHOLD

        if drop_pct <= max(threshold, 0.02):
            return None

        if hasattr(self, '_isolation_level'):
            from ali2026v3_trading.config_params import ISOLATION_LEVELS
            if self._isolation_level in (ISOLATION_LEVELS.PROCESS, ISOLATION_LEVELS.CONTAINER):
                if drop_pct <= max(threshold, 0.02) * 0.8:
                    return None

        if now < self._circuit_breaker_calm_until:
            self._stats["circuit_breaker_calm_rejects"] += 1
            logging.info(
                "[SafetyMetaLayer] 熔断冷静期内，忽略二次触发 (冷静期剩余%.0fs)",
                self._circuit_breaker_calm_until - now,
            )
            return None

        return (drop_pct, threshold)

    def _trigger_circuit_break(self, now: float, drop_pct: float, threshold: float):
        """触发断路器：设置暂停时间、冷静期、影子模式、审计日志。返回(pause_duration, calm_period)"""
        pause_duration = self._get_circuit_breaker_pause_sec()
        new_paused_until = now + pause_duration
        if self._trading_paused_until > now:
            new_paused_until = max(self._trading_paused_until, new_paused_until)
            logging.warning(
                "[SafetyMetaLayer] R13-BIZ-07: 断路器连续触发，暂停时间延长 %.0f->%.0f",
                self._trading_paused_until - now, new_paused_until - now,
            )
        self._trading_paused_until = new_paused_until
        self._pause_reason = f"速率断路器: 1min回撤{drop_pct:.2%} > 2.5σ阈值{threshold:.2%}"
        self._stats["circuit_breaker_triggers"] += 1
        self._circuit_breaker_activated_at: float = now
        # R27-P0-DR-12修复: 断路器触发时记录失败到半开断路器
        if self._risk_cb_half_open is not None:
            self._risk_cb_half_open.record_failure()

        calm_period = self._get_circuit_breaker_calm_period_sec()
        new_calm_until = now + calm_period
        self._circuit_breaker_calm_until = max(self._circuit_breaker_calm_until, new_calm_until)

        self._circuit_breaker_shadow_mode = True
        self._circuit_breaker_shadow_until = now + calm_period + 300

        logging.warning(
            "[SafetyMetaLayer] ⚡ 速率断路器触发！1min回撤=%.2f%%, 阈值=%.2f%%, 暂停%.0f秒, 冷静期%.0f秒, 影子观察%.0f秒",
            drop_pct * 100, threshold * 100, pause_duration, calm_period, 300
        )

        try:
            structured_audit_log('circuit_breaker_triggered', 'blocked',
                                 {'drop_pct': round(drop_pct, 6), 'threshold': round(threshold, 6),
                                  'pause_sec': pause_duration, 'calm_sec': calm_period,
                                  'shadow_sec': 300}, severity="WARNING")
        except Exception:
            pass

        try:
            from ali2026v3_trading.config_params import get_security_responder
            _responder = get_security_responder()
            _strategy_id_ns = getattr(self, '_strategy_id_ns', 'unknown')
            _blocked = _responder.report_suspicious(
                source=f"circuit_breaker:{_strategy_id_ns}",
                reason=f"1min_drawdown={drop_pct*100:.2f}%>threshold={threshold*100:.2f}%"
            )
            if _blocked:
                logging.critical("[SEC-P1-12] 安全自动阻断已激活: source=%s", _strategy_id_ns)
        except Exception as _sec_ex:
            logging.debug("[SEC-P1-12] report_suspicious调用失败: %s", _sec_ex)

        return (pause_duration, calm_period)

    def _reset_circuit_state(self, drop_pct: float, threshold: float,
                              pause_duration: float, calm_period: float) -> None:
        """断路器触发后：撤销挂单、强制减仓、发布事件、持久化状态"""
        self._cancel_pending_on_circuit_breaker()
        self._force_position_reduction_on_circuit_breaker()

        try:
            from ali2026v3_trading.event_bus import get_global_event_bus, CircuitBreakerTriggeredEvent
            bus = get_global_event_bus()
            cb_event = CircuitBreakerTriggeredEvent(
                reason=self._pause_reason,
                drop_pct=drop_pct,
                threshold=threshold,
                pause_duration=pause_duration,
                calm_period=calm_period,
            )
            bus.publish(cb_event, async_mode=True)
        except Exception as _ops04_e:
            logging.debug("[SafetyMetaLayer] OPS-P1-04: EventBus断路器告警推送异常: %s", _ops04_e)

        self._save_circuit_breaker_state()
    def _check_daily_drawdown(self) -> None:
        if self._daily_hard_stop_triggered:
            return

        multiplier = self._get_daily_drawdown_multiplier()
        if multiplier <= 0:
            return

        triggered = False

        if self._prev_5day_avg_profit <= 0:
            if self._daily_start_equity and self._daily_start_equity > 0:
                max_dd = safe_get_float(self.params, "daily_loss_hard_stop_pct", self.DEFAULT_MAX_DRAWDOWN)
                if self._daily_drawdown >= max_dd:
                    triggered = True
                    self._stats["daily_drawdown_triggers"] += 1
                    logging.warning(
                        "[SafetyMetaLayer] 🛑 日最大回撤硬停止触发！回撤=%.2f%% >= 阈值%.2f%%",
                        self._daily_drawdown * 100, max_dd * 100
                    )
        else:
            max_daily_loss = self._prev_5day_avg_profit * multiplier
            current_loss = (self._daily_peak_equity - self._equity_series[-1]) if self._equity_series else 0

            if current_loss >= max_daily_loss:
                triggered = True
                self._stats["daily_drawdown_triggers"] += 1
                logging.warning(
                    "[SafetyMetaLayer] 🛑 日最大回撤硬停止触发！当前亏损=%.2f >= 5日均值×%.1f=%.2f",
                    current_loss, multiplier, max_daily_loss
                )

        if triggered:
            self._daily_hard_stop_triggered = True
            self._daily_new_open_blocked = True
            self._stats["daily_hard_stop_triggers"] += 1
            try:
                from ali2026v3_trading.position_service import get_cross_strategy_risk_guard
                guard = get_cross_strategy_risk_guard()
                if guard and hasattr(guard, 'set_daily_drawdown'):
                    guard.set_daily_drawdown(self._daily_drawdown * 100)
            except Exception as e:
                logging.debug("[SafetyMetaLayer] set_daily_drawdown sync error: %s", e)
            logging.critical(
                "[SafetyMetaLayer] 🛑 日回撤硬停止触发！回撤=%.2f%%, 禁止新开仓, 需人工确认恢复",
                self._daily_drawdown * 100
            )
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, DailyDrawdownHaltEvent
                bus = get_global_event_bus()
                dd_event = DailyDrawdownHaltEvent(
                    drawdown_pct=self._daily_drawdown,
                    threshold_pct=max_dd if self._prev_5day_avg_profit <= 0 else 0.0,
                    current_loss=current_loss if self._prev_5day_avg_profit > 0 else 0.0,
                    max_daily_loss=max_daily_loss if self._prev_5day_avg_profit > 0 else 0.0,
                    trigger_type='fixed_threshold' if self._prev_5day_avg_profit <= 0 else 'avg_multiplier',
                )
                bus.publish(dd_event, async_mode=True)
            except Exception as _ops05_e:
                logging.debug("[SafetyMetaLayer] OPS-P1-05: EventBus日回撤告警推送异常: %s", _ops05_e)
            self._save_circuit_breaker_state()

    def check_position_hard_time_stop(self, position_id: str, open_time,
                                       max_profit_reached: float,
                                       profit_slope: float = 0.0,
                                       peak_profit_pct: float = 0.0,
                                       current_profit_pct: float = 0.0,
                                       bar_time: Optional[float] = None) -> Optional[str]:
        if isinstance(open_time, datetime):
            open_time = open_time.timestamp()
        elif not isinstance(open_time, (int, float)):
            try:
                open_time = float(open_time)
            except (ValueError, TypeError):
                logging.warning("[SafetyMetaLayer] open_time类型无效: %s, 跳过硬时间止损检查", type(open_time).__name__)
                return None

        if hasattr(self, '_logic_reversal_priority'):
            if self._logic_reversal_priority.get(position_id, False):
                logging.info(
                    "[SafetyMetaLayer] P1-裂缝36: position=%s 逻辑反转已触发，"
                    "硬时间止损降级为备用（优先级：逻辑反转>阶段2>阶段1）",
                    position_id,
                )
                return None

        stage1_min = self._get_stage1_minutes()
        stage2_min = self._get_stage2_minutes()
        stage1_threshold = self._get_stage1_profit_threshold()

        now = bar_time if bar_time is not None else time.time()
        elapsed_min = (now - open_time) / 60.0

        if elapsed_min >= stage1_min and max_profit_reached < stage1_threshold:
            with self._stats_lock:
                self._stats["hard_time_stop_triggers"] += 1
            logging.warning(
                "[SafetyMetaLayer] ⏰ 阶段1硬止损触发！position=%s, 已持%.0fmin, "
                "最高浮盈=%.2f%% < 阶段1要求=%.2f%%",
                position_id, elapsed_min, max_profit_reached * 100, stage1_threshold * 100
            )
            return f"HardTimeStop@{elapsed_min:.0f}min(profit<{stage1_threshold:.1%})"

        if stage1_min <= elapsed_min < stage2_min:
            stage2_slope_threshold = -0.01
            _slope_tolerance = 1e-9
            if profit_slope < stage2_slope_threshold - _slope_tolerance:
                with self._stats_lock:
                    self._stats["hard_time_stop_triggers"] += 1
                logging.warning(
                    "[SafetyMetaLayer] 阶段2硬止损触发(斜率衰减)！position=%s, 已持%.0fmin, "
                    "profit_slope=%.6f < threshold=%.4f",
                    position_id, elapsed_min, profit_slope, stage2_slope_threshold
                )
                return f"HardTimeStop@{elapsed_min:.0f}min(slope<{stage2_slope_threshold})"

            if peak_profit_pct > 0 and current_profit_pct < peak_profit_pct * 0.5:
                with self._stats_lock:
                    self._stats["hard_time_stop_triggers"] += 1
                logging.warning(
                    "[SafetyMetaLayer] ⏰ 阶段2硬止损触发(回撤超50%%)！position=%s, 已持%.0fmin, "
                    "peak=%.2f%% current=%.2f%%",
                    position_id, elapsed_min, peak_profit_pct * 100, current_profit_pct * 100
                )
                return f"HardTimeStop@{elapsed_min:.0f}min(drawdown>50%)"

        return None

    def set_logic_reversal_triggered(self, position_id: str, triggered: bool) -> None:
        with self._lock:
            self._logic_reversal_priority[position_id] = triggered
            if triggered:
                logging.info(
                    "[SafetyMetaLayer] P1-裂缝36: position=%s 逻辑反转触发，"
                    "硬时间止损降级为备用",
                    position_id,
                )

    def check_regulatory_compliance(self, *args, **kwargs):
        return self._compliance_checker.check_regulatory_compliance(*args, **kwargs)

    @api_version("1.0")
    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,
                                  open_positions: int = 0, max_positions: int = 50,
                                  existing_margin_used: float = 0.0) -> Dict[str, Any]:
        daily_start = self._daily_start_equity or equity
        return self._margin_manager.check_capital_sufficiency(
            equity, required_margin, open_positions, max_positions, existing_margin_used,
            daily_start_equity=daily_start,
        )

    def reserve_margin(self, *args, **kwargs):
        return self._margin_manager.reserve_margin(*args, **kwargs)

    def release_margin(self, *args, **kwargs):
        return self._margin_manager.release_margin(*args, **kwargs)

    def check_exchange_status(self, exchange: str = "AUTO") -> Dict[str, Any]:
        exchanges_status: Dict[str, str] = {}
        tradeable = True
        reason = "ok"
        try:
            from ali2026v3_trading.scheduler_service import is_market_open
            target_exchanges = [exchange] if exchange != "AUTO" else ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX']
            for exch in target_exchanges:
                try:
                    is_open = is_market_open(exch)
                    exchanges_status[exch] = "OPEN" if is_open else "CLOSED"
                    if not is_open:
                        tradeable = False
                        reason = f"{exch}_closed"
                except Exception:
                    exchanges_status[exch] = "UNKNOWN"
        except ImportError:
            exchanges_status["fallback"] = "scheduler_service_unavailable"
            reason = "scheduler_unavailable"
        return {"tradeable": tradeable, "exchanges": exchanges_status, "reason": reason}

    def is_trading_paused(self) -> Tuple[bool, str]:
        with self._lock:
            now = time.time()
            # P1-R9-22修复: 算法暂停与断路器暂停独立检查
            if now < self._trading_paused_until:
                remaining = self._trading_paused_until - now
                return True, f"{self._pause_reason}, 剩余{remaining:.0f}秒"
            if now < self._algo_paused_until:
                remaining = self._algo_paused_until - now
                return True, f"{self._algo_pause_reason}, 剩余{remaining:.0f}秒"
            if self._pause_reason:
                _prev_reason = self._pause_reason
                self._pause_reason = ""
                logging.info("[P1-R9-21] 交易暂停已恢复, 原暂停原因: %s", _prev_reason)
                # R27-P0-DR-12修复: 暂停恢复时记录成功到半开断路器
                if self._risk_cb_half_open is not None:
                    self._risk_cb_half_open.record_success()
            return False, ""

    def is_circuit_breaker_shadow_mode(self) -> bool:
        with self._lock:
            if self._circuit_breaker_shadow_mode:
                now = time.time()
                if now >= self._circuit_breaker_shadow_until:
                    self._circuit_breaker_shadow_mode = False
                    self._save_circuit_breaker_state()
                    logging.info("[SafetyMetaLayer] 影子模式观察期结束，恢复正常交易")
                    return False
                return True
            return False

    def is_new_open_blocked(self) -> bool:
        with self._lock:
            return self._daily_new_open_blocked

    def can_open(self) -> bool:
        paused, _ = self.is_trading_paused()
        # R27-P0-DR-12修复: 集成CircuitBreakerHalfOpen半开探测
        if not paused and self._risk_cb_half_open is not None:
            if not self._risk_cb_half_open.allow_request():
                return False
        return not paused and not self._daily_new_open_blocked

    def compute_and_track_rollover_cost(self, bar_data, params: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.shared_trading_constants import detect_rollover_gaps, compute_rollover_cost  # R27-CP-01-FIX
            rollover_points = detect_rollover_gaps(bar_data)
            if rollover_points:
                cost_result = compute_rollover_cost(
                    rollover_points, bar_data, params or {},
                    calendar_basis_bps=5.0, rollover_slippage_bps=3.0,
                )
                with self._lock:
                    self._rollover_cost_bps += cost_result.get('total_rollover_cost_bps', 0.0)
                    self._rollover_count += cost_result.get('rollover_count', 0)
                logging.info(
                    "[SafetyMetaLayer] P1-1: 换月成本累计=%.1fbps (累计%d次)",
                    self._rollover_cost_bps, self._rollover_count,
                )
                return cost_result
            return {"total_rollover_cost_bps": 0.0, "rollover_count": 0}
        except Exception as e:
            logging.debug("[SafetyMetaLayer] P1-1: compute_rollover_cost调用失败: %s", e)
            return {"total_rollover_cost_bps": 0.0, "rollover_count": 0, "error": str(e)}

    def get_rollover_cost_summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_rollover_cost_bps": self._rollover_cost_bps,
                "rollover_count": self._rollover_count,
            }

    def can_close(self) -> bool:
        return True

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

    def _check_algo_circuit_breaker(self) -> Optional[str]:
        try:
            now = time.time()
            from ali2026v3_trading.order_service import get_order_service
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
                reason = f"自成交禁止{recent_st_bans}次/小时>3, 暂停算法交易1小时"
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
                reason = f"报单频率{recent_orders_60}次/分钟>100, 暂停5分钟"
                logging.critical("[SafetyMetaLayer] CMP-P1-13: %s", reason)
                with self._lock:
                    self._algo_paused = True
                    self._algo_paused_until = now + 300
                    self._algo_pause_reason = reason
                    self._algo_breakers['high_order_frequency']['triggered'] = True
                    self._algo_breakers['high_order_frequency']['paused_until'] = self._algo_paused_until
                return reason

            return None
        except Exception as e:
            logging.debug("[SafetyMetaLayer] CMP-P1-13: 算法熔断检查异常: %s", e)
            return None

    def is_hard_stop_triggered(self) -> bool:
        with self._lock:
            return self._daily_hard_stop_triggered

    _resume_token: Optional[str] = None

    def generate_resume_token(self) -> str:
        import secrets as _secrets
        self._resume_token = _secrets.token_hex(16)
        logging.critical(
            "[SafetyMetaLayer] 恢复令牌已生成: %s... (必须传入confirm_daily_resume验证)",
            self._resume_token[:8]
        )
        return self._resume_token

    def confirm_daily_resume(self, caller_id: str = "unknown", resume_token: Optional[str] = None,
                              approval_context: Optional[Dict[str, Any]] = None) -> bool:
        with self._lock:
            if not self._daily_hard_stop_triggered:
                logging.info("[SafetyMetaLayer] confirm_daily_resume: 未处于硬停止状态，无需恢复")
                return False

            if approval_context is None or not approval_context.get('approver_id'):
                logging.critical(
                    "[OPS-14] confirm_daily_resume缺少审批! 必须提供approval_context包含approver_id. "
                    "caller_id=%s", caller_id
                )
                try:
                    from ali2026v3_trading.risk_service import alert as _alert, AlertLevel as _AlertLevel
                    _alert(_AlertLevel.P0, 'confirm_resume_no_approval',
                           f'confirm_daily_resume缺少审批 caller={caller_id}',
                           {'caller_id': caller_id})
                except Exception:
                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                    pass
                return False

            if resume_token is not None:
                if resume_token != self._resume_token:
                    logging.critical(
                        "[SafetyMetaLayer] R15-SEC-04: 恢复令牌不匹配！拒绝恢复 caller=%s", caller_id
                    )
                    return False
                self._resume_token = None
            else:
                if not caller_id.startswith("MANUAL_"):
                    logging.critical(
                        "[SafetyMetaLayer] R15-SEC-04: 非人工操作尝试恢复交易被拒绝！caller=%s",
                        caller_id
                    )
                    return False
            _now_t = time.time()
            _dt_now = datetime.fromtimestamp(_now_t, tz=_CHINA_TZ)
            _today_key = _dt_now.strftime("%Y-%m-%d") if _dt_now.hour >= 18 else (_dt_now - timedelta(days=1)).strftime("%Y-%m-%d")
            if hasattr(self, '_last_resume_date') and self._last_resume_date == _today_key:
                logging.critical(
                    "[SafetyMetaLayer] P1-R11-10: 同交易日内已执行过恢复操作，拒绝重复恢复！"
                    "caller=%s today=%s last_resume=%s",
                    caller_id, _today_key, getattr(self, '_last_resume_time', 'N/A'),
                )
                return False
            logging.critical(
                "[SafetyMetaLayer] ✅ 人工确认恢复交易！日回撤硬停止已解除，交易恢复正常 "
                "caller_id=%s timestamp=%s",
                caller_id, _get_tz_aware_now().isoformat()
            )

            _market_safe = self._verify_market_safety_before_resume()
            if not _market_safe:
                logging.critical(
                    "[SafetyMetaLayer] INV-RSK-01: 市场安全验证未通过，恢复被拒绝! "
                    "caller_id=%s, 市场仍处于异常状态",
                    caller_id,
                )
                return False

            self._daily_hard_stop_triggered = False
            self._daily_new_open_blocked = False
            self._daily_drawdown = 0.0
            self._last_resume_date = _today_key
            self._last_resume_time = _now_t
            self._stats['confirm_resume_history'] = self._stats.get('confirm_resume_history', [])
            resume_record = {
                'timestamp': _get_tz_aware_now().isoformat(),
                'caller_id': caller_id,
                'daily_drawdown_at_resume': self._daily_drawdown,
            }
            self._stats['confirm_resume_history'].append(resume_record)

            try:
                audit_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
                os.makedirs(audit_dir, exist_ok=True)
                audit_path = os.path.join(audit_dir, 'compliance_audit.jsonl')
                audit_entry = {
                    'event': 'confirm_daily_resume',
                    'timestamp': _get_tz_aware_now().isoformat(),
                    'caller_id': caller_id,
                    'approval_context': approval_context or {},
                    'previous_state': {
                        'daily_hard_stop_triggered': True,
                        'daily_drawdown': self._daily_drawdown,
                    },
                    'new_state': {
                        'daily_hard_stop_triggered': False,
                        'daily_new_open_blocked': False,
                    },
                }
                with open(audit_path, 'a', encoding='utf-8') as f:
                    safe_jsonl_append_line(f, audit_entry)
                logging.info("[SafetyMetaLayer] CMP-P1-02: 合规审批记录已写入 %s", audit_path)
            except Exception as e:
                logging.warning("[SafetyMetaLayer] CMP-P1-02: 合规审批记录写入失败: %s", e)
            return True

    def set_prev_5day_avg_profit(self, avg_profit: float) -> None:
        with self._lock:
            self._prev_5day_avg_profit = avg_profit

    def _get_circuit_breaker_pause_sec(self) -> float:
        return safe_get_float(self.params, "circuit_breaker_pause_sec", 180.0)

    def _get_circuit_breaker_calm_period_sec(self) -> float:
        return safe_get_float(self.params, "circuit_breaker_calm_period_sec", 600.0)

    def _cancel_pending_on_circuit_breaker(self) -> None:
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                with osvc._lock:
                    count = osvc.cancel_all_pending()
                if count > 0:
                    logging.warning("[SafetyMetaLayer] 断路器触发，已撤销 %d 笔未成交订单", count)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] cancel_pending error: %s", e)

        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            if spm and hasattr(spm, 'reset_state_cache'):
                spm.reset_state_cache()
                logging.info("[SafetyMetaLayer] 断路器触发后已清空state_param_manager状态缓存")
        except Exception as e:
            logging.warning("[SafetyMetaLayer] state_param_manager reset error: %s", e)

        try:
            from ali2026v3_trading.width_cache import get_width_cache
            wc = get_width_cache()
            if wc and hasattr(wc, 'reset_cache'):
                wc.reset_cache()
                logging.info("[SafetyMetaLayer] 断路器触发后已清空width_cache状态缓存")
            elif wc and hasattr(wc, 'clear'):
                wc.clear()
                logging.info("[SafetyMetaLayer] 断路器触发后已清空width_cache")
        except Exception as e:
            logging.warning("[SafetyMetaLayer] width_cache reset error: %s", e)

    def _force_position_reduction_on_circuit_breaker(self) -> None:
        try:
            from ali2026v3_trading.position_service import get_position_service
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
                for pid in list(pos_dict.keys()):
                    rec = pos_dict.get(pid)
                    if rec is None or getattr(rec, 'volume', 0) == 0:
                        continue
                    if getattr(rec, '_closing', False):
                        continue
                    try:
                        reduce_volume = max(1, int(abs(rec.volume) * reduction_ratio))
                        from ali2026v3_trading.order_service import get_order_service
                        osvc = get_order_service()
                        if osvc:
                            direction = 'SELL' if rec.volume > 0 else 'BUY'
                            price = 0.0
                            try:
                                from ali2026v3_trading.data_service import get_data_service
                                ds = get_data_service()
                                if ds and ds.realtime_cache:
                                    mp = ds.realtime_cache.get_latest_price(rec.instrument_id)
                                    if mp and mp > 0:
                                        price = mp
                            except Exception:
                                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                pass
                            if price <= 0:
                                price = getattr(rec, 'current_price', 0) or getattr(rec, 'open_price', 0)
                            if price > 0:
                                osvc.send_order(
                                    instrument_id=rec.instrument_id,
                                    volume=reduce_volume,
                                    price=price,
                                    direction=direction,
                                    action='CLOSE',
                                    exchange=getattr(rec, 'exchange', ''),
                                    signal_id=f"RISK_REDUCE_{rec.instrument_id}",
                                )
                                reduced_count += 1
                    except Exception as e:
                        logging.warning(
                            "[SafetyMetaLayer] INV-P1-10: 减仓失败 instrument=%s pid=%s error=%s",
                            inst_id, pid, e,
                        )

            if reduced_count > 0:
                logging.critical(
                    "[SafetyMetaLayer] INV-P1-10: 熔断后强制减仓完成 reduced=%d positions ratio=%.0f%%",
                    reduced_count, reduction_ratio * 100,
                )
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='circuit_breaker_force_reduction',
                            level='CRITICAL',
                            message=f"INV-P1-10: 熔断后强制减仓 reduced={reduced_count} ratio={reduction_ratio*100:.0f}%",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[SafetyMetaLayer] INV-P1-10: 事件总线告警失败: %s", _eb_e)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] INV-P1-10: 强制减仓异常: %s", e)

    def _get_daily_drawdown_multiplier(self) -> float:
        return safe_get_float(self.params, "daily_drawdown_multiplier", 2.0)

    def _verify_market_safety_before_resume(self) -> bool:
        now = time.time()

        if self._trading_paused_until > now:
            logging.warning(
                "[SafetyMetaLayer] INV-RSK-01: 断路器暂停期未结束 paused_until=%.0f",
                self._trading_paused_until,
            )
            return False

        if self._circuit_breaker_calm_until > now:
            logging.warning(
                "[SafetyMetaLayer] INV-RSK-01: 断路器冷静期未结束 calm_until=%.0f",
                self._circuit_breaker_calm_until,
            )
            return False

        if len(self._equity_series) >= 3 and self._daily_start_equity and self._daily_start_equity > 0:
            recent_equities = list(self._equity_series)[-3:]
            drawdowns = [(self._daily_peak_equity - eq) / self._daily_start_equity for eq in recent_equities]
            if all(d1 < d2 for d1, d2 in zip(drawdowns, drawdowns[1:])):
                logging.warning(
                    "[SafetyMetaLayer] INV-RSK-01: 回撤仍在恶化 drawdowns=[%s]",
                    ', '.join(f'{d:.4f}' for d in drawdowns),
                )
                return False

        with self._lock:
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_absolute_ev_paused():
                        logging.warning("[SafetyMetaLayer] INV-RSK-01: 影子引擎EV暂停仍生效")
                        return False
                    if _sse.is_degradation_active():
                        logging.warning("[SafetyMetaLayer] INV-RSK-01: 影子引擎降级仍生效")
                        return False
            except Exception as _sm_ev_err:
                logging.warning("[R22-EP-01b] SafetyMetaLayer影子引擎检查异常: %s", _sm_ev_err)
                return False

        if self._algo_paused and self._algo_paused_until > now:
            logging.warning(
                "[SafetyMetaLayer] INV-RSK-01: 算法熔断仍生效 paused_until=%.0f",
                self._algo_paused_until,
            )
            return False

        return True

    def _get_hard_time_stop_minutes(self) -> float:
        hft_ms = safe_get_float(self.params, "hft_hard_time_stop_ms", 0)
        if hft_ms > 0:
            return hft_ms / 60000.0
        spring_sec = safe_get_float(self.params, "spring_hard_time_stop_sec", 0)
        if spring_sec > 0:
            return spring_sec / 60.0
        resonance_min = safe_get_float(self.params, "resonance_hard_time_stop_min", 0)
        if resonance_min > 0:
            return resonance_min
        box_min = safe_get_float(self.params, "box_hard_time_stop_min", 0)
        if box_min > 0:
            return box_min
        return 5.0

    def _get_min_profit_threshold(self) -> float:
        return safe_get_float(self.params, "min_profit_threshold", 0.002)

    def _get_stage1_minutes(self) -> float:
        _bar_period = safe_get_float(self.params, "bar_period", 1.0)
        val = safe_get_float(self.params, "stage1_min_minutes", None)
        if val is not None and val > 0:
            return val * _bar_period
        _fallback = safe_get_float(self.params, "stage1_minutes", 90.0)
        if _fallback <= 0:
            logging.warning("[R5-L-01] stage1_minutes=%s<=0，使用默认90.0", _fallback)
            _fallback = 90.0
        return _fallback * _bar_period

    def _get_stage2_minutes(self) -> float:
        return safe_get_float(self.params, "stage2_minutes", 240.0)

    def _get_stage1_profit_threshold(self) -> float:
        return safe_get_float(self.params, "stage1_profit_threshold", 0.002)

    def get_health_status(self) -> Dict[str, Any]:
        _trigger_source = None
        health_status = 'OK'
        details: Dict[str, Any] = {}

        with self._lock:
            now = time.time()
            trading_paused = now < self._trading_paused_until
            circuit_breaker_active = now < self._circuit_breaker_calm_until
            if trading_paused or circuit_breaker_active:
                health_status = 'CRITICAL'
                _trigger_source = 'circuit_breaker'
                details['circuit_breaker'] = {
                    'trading_paused': trading_paused,
                    'calm_until': self._circuit_breaker_calm_until,
                    'pause_reason': self._pause_reason,
                }

            if self._daily_hard_stop_triggered:
                health_status = 'CRITICAL'
                _trigger_source = 'daily_drawdown' if _trigger_source is None else _trigger_source
                details['daily_hard_stop'] = {
                    'triggered': True,
                    'daily_drawdown_pct': self._daily_drawdown,
                    'new_open_blocked': self._daily_new_open_blocked,
                }

            if self._algo_paused and now < self._algo_paused_until:
                if health_status != 'CRITICAL':
                    health_status = 'WARNING'
                details['algo_paused'] = {
                    'paused': True,
                    'reason': self._algo_pause_reason,
                    'remaining_sec': max(0, self._algo_paused_until - now),
                }

            if self._circuit_breaker_shadow_mode and now < self._circuit_breaker_shadow_until:
                if health_status == 'OK':
                    health_status = 'WARNING'
                details['shadow_mode'] = {
                    'active': True,
                    'remaining_sec': max(0, self._circuit_breaker_shadow_until - now),
                }

            if self._daily_new_open_blocked and health_status == 'OK':
                health_status = 'WARNING'
                details['new_open_blocked'] = True

        return {
            'health_status': health_status,
            'triggered_by': _trigger_source if health_status == 'CRITICAL' else None,
            'details': details,
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['trading_paused'] = time.time() < self._trading_paused_until
            stats['new_open_blocked'] = self._daily_new_open_blocked
            stats['hard_stop_triggered'] = self._daily_hard_stop_triggered
            stats['daily_drawdown_pct'] = self._daily_drawdown
            return stats


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
                    logging.warning("[SafetyMetaLayer] 策略级实例数已达上限%d，清理最旧实例", _MAX_STRATEGY_SAFETY_LAYERS)
                    _oldest_sid = next(iter(_strategy_safety_layers))
                    del _strategy_safety_layers[_oldest_sid]
                _strategy_safety_layers[strategy_id] = SafetyMetaLayer(params=params)
                logging.info('[SafetyMetaLayer] 策略级实例初始化完成 strategy_id=%s params_type=%s',
                             strategy_id, type(params).__name__ if params else 'None')
            else:
                layer = _strategy_safety_layers[strategy_id]
                if params is not None and layer._params is None:
                    layer._params = params
                    logging.info('[SafetyMetaLayer] 策略级实例参数已更新 strategy_id=%s params_type=%s',
                                 strategy_id, type(params).__name__)
            return _strategy_safety_layers[strategy_id]
    global _safety_meta_layer
    if _safety_meta_layer is None:
        with _safety_meta_layer_lock:
            if _safety_meta_layer is None:
                _safety_meta_layer = SafetyMetaLayer(params=params)
                logging.info('[SafetyMetaLayer] 首次初始化完成 params_type=%s',
                             type(params).__name__ if params else 'None')
    else:
        if params is not None and _safety_meta_layer._params is None:
            with _safety_meta_layer_lock:
                if params is not None and _safety_meta_layer._params is None:
                    _safety_meta_layer._params = params
                    logging.info('[SafetyMetaLayer] 参数已更新 params_type=%s',
                                 type(params).__name__)
    return _safety_meta_layer


def cleanup_safety_layer(strategy_id: str):
    with _strategy_safety_lock:
        if strategy_id in _strategy_safety_layers:
            del _strategy_safety_layers[strategy_id]
            logging.info('[SafetyMetaLayer] 策略级实例已清理 strategy_id=%s', strategy_id)
