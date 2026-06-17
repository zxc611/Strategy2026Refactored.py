# [M1-37] ��ȫԪ��-�س����
# MODULE_ID: M1-221
from __future__ import annotations

import logging
import math
import time
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.risk.safety_meta_audit import safe_get_float, _get_tz_aware_now, structured_audit_log

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ


# [P2-07-DMS] 职责: 权益监控+日回撤检�?单调性检�?负权益紧急处�?
# 与governance/mode_exit_rules.py的DrawdownManager分工: DMS=权益监控, DM=规则决策
class DrawdownMonitorService:
    """回撤监控服务 �?从SafetyMetaLayer提取的回�?权益监控逻辑

    负责：权益更�?日回撤检�?单调性检�?负权益紧急处�?
    """

    def __init__(self, params: Any, owner: Any):
        self.params = params
        self.owner = owner
        self._lock = threading.RLock()

        self._equity_series: deque = deque(maxlen=60)
        self._equity_timestamps: deque = deque(maxlen=60)
        self._drop_pct_history: deque = deque(maxlen=60)
        self._last_equity_date: Optional[str] = None

        self._daily_start_equity: Optional[float] = None
        self._daily_peak_equity: float = 0.0
        self._daily_drawdown: float = 0.0
        self._prev_5day_avg_profit: float = 0.0
        self._daily_new_open_blocked: bool = False
        self._daily_hard_stop_triggered: bool = False
        self._current_date: Optional[str] = None

        self._equity_intraday_low: Optional[float] = None
        self._equity_monotonicity_anomaly_count: int = 0

    def on_equity_update(self, equity: float, stats: Dict[str, Any],
                          circuit_breaker_service,
                          save_callback=None) -> None:
        now = time.time()
        _dt_now = datetime.fromtimestamp(now, tz=_CHINA_TZ)
        if _dt_now.hour >= 18:
            today = _dt_now.strftime("%Y-%m-%d")
        else:
            _yesterday = _dt_now - timedelta(days=1)
            today = _yesterday.strftime("%Y-%m-%d")

        with self._lock:
            stats["total_equity_updates"] += 1
            if equity <= 0:
                self._notify_equity_change(equity, stats, save_callback)
                return
            self._update_equity_metrics(equity, now, today, stats)
            self._check_drawdown_limits(stats, circuit_breaker_service, save_callback)

    def _notify_equity_change(self, equity: float, stats: Dict[str, Any],
                               save_callback=None) -> None:
        self._daily_hard_stop_triggered = True
        self._daily_new_open_blocked = True
        self._daily_drawdown = 1.0
        stats["negative_equity_triggers"] = stats.get("negative_equity_triggers", 0) + 1
        logging.critical(
            "[SafetyMetaLayer] INV-CAP-01: negative equity hard stop! equity=%.2f <= 0, "
            "force block all trading, forced position reduction, manual confirm required",
            equity,
        )
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                count = osvc.cancel_all_pending()
                if count > 0:
                    logging.critical("[SafetyMetaLayer] negative equity, cancelled %d pending orders", count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] negative equity cancel failed: %s", e)
        try:
            from ali2026v3_trading.position.position_service import get_position_service
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
                            from ali2026v3_trading.risk.risk_service import get_risk_service
                            _rs = get_risk_service()
                            if _rs:
                                _lp = 0.0
                                try:
                                    from ali2026v3_trading.data.data_service import get_data_service
                                    _ds = get_data_service()
                                    if _ds and _ds.realtime_cache:
                                        _lp = _ds.realtime_cache.get_latest_price(_inst_id) or 0.0
                                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lp_err:
                                    logging.warning("[R22-P1-NEW] 负权益减�? 行情获取失败(跳过涨跌停检�?: %s", _lp_err)
                                if _lp > 0:
                                    _lim = _rs.is_at_price_limit(_inst_id, _lp)
                                    if _lim.get('is_limit_up') and _close_dir == 'SELL':
                                        logging.warning("[SafetyMetaLayer] EX-04: 涨停板卖出排�?%s price=%.2f", _inst_id, _lp)
                                        _skip = True
                                    if _lim.get('is_limit_down') and _close_dir == 'BUY':
                                        logging.warning("[SafetyMetaLayer] EX-04: 跌停板买入排�?%s price=%.2f", _inst_id, _lp)
                                        _skip = True
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lim_err:
                            logging.warning("[R22-P1-NEW] 负权益减�? 涨跌停检查异�?减仓不受约束): %s", _lim_err)
                        if _skip:
                            _skipped_limit += 1
                            continue
                        try:
                            _ps._trigger_close_position(_rec, "INV-CAP-01: negative equity forced reduction")
                            _closed_ok += 1
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _ce:
                            logging.warning("[SafetyMetaLayer] EX-04: force close %s failed: %s", _inst_id, _ce)
                logging.critical("[SafetyMetaLayer] INV-CAP-01: forced reduction closed=%d skipped_limit=%d", _closed_ok, _skipped_limit)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] INV-CAP-01: forced position reduction failed: %s", e)
        if save_callback:
            save_callback()

    def _update_equity_metrics(self, equity: float, now: float, today: str,
                                stats: Dict[str, Any]) -> None:
        if self._current_date != today:
            self._current_date = today
            self._daily_start_equity = equity
            self._daily_peak_equity = equity
            self._daily_drawdown = 0.0
            self._drop_pct_history.clear()
            if self._daily_hard_stop_triggered:
                self._daily_new_open_blocked = True
                logging.warning(
                    "[SafetyMetaLayer] 新交易日(%s)，日回撤硬停止仍然生效�?
                    "必须调用confirm_daily_resume()经审批后才能恢复交易�?,
                    today,
                )
            else:
                self._daily_new_open_blocked = False
        self._daily_peak_equity = max(self._daily_peak_equity, equity)
        if self._daily_start_equity and self._daily_start_equity > 0:
            unrealized_pnl = 0.0
            try:
                from ali2026v3_trading.position.position_service import get_position_service
                _ps = get_position_service()
                if _ps:
                    for _inst_id, pos_dict in _ps.positions.items():
                        for _pid, rec in pos_dict.items():
                            if rec.volume != 0 and rec.open_price > 0:
                                try:
                                    from ali2026v3_trading.data.data_service import get_data_service
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
                                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                    logging.warning("[R22-EP-P1] RiskService exception swallowed")
                                    pass
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
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
                "[SafetyMetaLayer] R13-P2-BIZ-03: 检测到隔夜缺口(%s�?s), "
                "已重置滚动窗口防止虚假熔断触�?,
                self._last_equity_date, today_str,
            )
        self._last_equity_date = today_str
        self._check_equity_curve_monotonicity(equity, now)

    def _check_drawdown_limits(self, stats: Dict[str, Any],
                                circuit_breaker_service,
                                save_callback=None) -> None:
        if not self._daily_hard_stop_triggered:
            circuit_breaker_service.check_circuit_breaker(
                time.time(), self._equity_series, self._drop_pct_history,
                stats, self._daily_hard_stop_triggered
            )
            self._check_daily_drawdown(stats, save_callback)

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
                                "[SafetyMetaLayer] INV-P1-11: 权益曲线单调性异�? "
                                "equity=%.2f < intraday_low=%.2f drop=%.2f%% anomaly_count=%d",
                                equity, self._equity_intraday_low, drop_pct * 100,
                                self._equity_monotonicity_anomaly_count,
                            )
                            try:
                                from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskEvent
                                _eb = get_global_event_bus()
                                if _eb and not getattr(_eb, '_shutdown', True):
                                    _eb.publish(RiskEvent(
                                        risk_type='equity_curve_non_monotonic',
                                        level='HIGH' if self._equity_monotonicity_anomaly_count < 3 else 'CRITICAL',
                                        message=f"INV-P1-11: 权益曲线单调性异�?"
                                                f"equity={equity:.2f} < intraday_low={self._equity_intraday_low:.2f} "
                                                f"drop={drop_pct*100:.2f}% anomaly_count={self._equity_monotonicity_anomaly_count}",
                                    ), async_mode=True)
                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _eb_e:
                                logging.debug("[SafetyMetaLayer] INV-P1-11: 事件总线告警失败: %s", _eb_e)
                            if self._equity_monotonicity_anomaly_count >= 3:
                                logging.critical(
                                    "[SafetyMetaLayer] INV-P1-11: 权益曲线连续异常(%d�?�?
                                    "可能存在未记录亏损或数据错误",
                                    self._equity_monotonicity_anomaly_count,
                                )
                    self._equity_intraday_low = equity
                else:
                    if self._equity_monotonicity_anomaly_count > 0:
                        self._equity_monotonicity_anomaly_count = 0
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[SafetyMetaLayer] INV-P1-11: 权益曲线单调性检查异�? %s", e)

    def _check_daily_drawdown(self, stats: Dict[str, Any], save_callback=None) -> None:
        if self._daily_hard_stop_triggered:
            return
        multiplier = self._get_daily_drawdown_multiplier()
        if multiplier <= 0:
            return
        hard_stop_pct = safe_get_float(self.params, "daily_loss_hard_stop_pct", 0.05)
        from ali2026v3_trading.infra.risk_rules import resolve_and_check_daily_drawdown
        should_stop, reason = resolve_and_check_daily_drawdown(
            daily_drawdown_pct=self._daily_drawdown,
            hard_stop_pct=hard_stop_pct,
            prev_5day_avg_profit=self._prev_5day_avg_profit,
            multiplier=multiplier,
            daily_start_equity=self._daily_start_equity,
        )
        if should_stop:
            stats["daily_drawdown_triggers"] += 1
            logging.warning("[SafetyMetaLayer] 🛑 日最大回撤硬停止触发�?s", reason)
            self._daily_hard_stop_triggered = True
            self._daily_new_open_blocked = True
            stats["daily_hard_stop_triggers"] += 1
            try:
                from ali2026v3_trading.position.position_service import get_cross_strategy_risk_guard
                guard = get_cross_strategy_risk_guard()
                if guard and hasattr(guard, 'set_daily_drawdown'):
                    guard.set_daily_drawdown(self._daily_drawdown * 100)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug("[SafetyMetaLayer] set_daily_drawdown sync error: %s", e)
            logging.critical(
                "[SafetyMetaLayer] 🛑 日回撤硬停止触发！回�?%.2f%%, 禁止新开�? 需人工确认恢复",
                self._daily_drawdown * 100
            )
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus, DailyDrawdownHaltEvent
                bus = get_global_event_bus()
                trigger_type = 'avg_multiplier' if self._prev_5day_avg_profit > 0 else 'fixed_threshold'
                current_loss = (self._daily_peak_equity - self._equity_series[-1]) if self._equity_series else 0.0
                max_daily_loss = self._prev_5day_avg_profit * multiplier if trigger_type == 'avg_multiplier' else 0.0
                dd_event = DailyDrawdownHaltEvent(
                    drawdown_pct=self._daily_drawdown,
                    threshold_pct=hard_stop_pct if trigger_type == 'fixed_threshold' else 0.0,
                    current_loss=current_loss if trigger_type == 'avg_multiplier' else 0.0,
                    max_daily_loss=max_daily_loss,
                    trigger_type=trigger_type,
                )
                bus.publish(dd_event, async_mode=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _ops05_e:
                logging.debug("[SafetyMetaLayer] OPS-P1-05: EventBus日回撤告警推送异�? %s", _ops05_e)
            if save_callback:
                save_callback()

    def _get_daily_drawdown_multiplier(self) -> float:
        return safe_get_float(self.params, "daily_drawdown_multiplier", 2.0)

    def set_prev_5day_avg_profit(self, avg_profit: float) -> None:
        with self._lock:
            self._prev_5day_avg_profit = avg_profit

    def get_state_snapshot(self) -> Dict[str, Any]:
        return {
            "daily_start_equity": self._daily_start_equity,
            "daily_peak_equity": self._daily_peak_equity,
            "daily_drawdown": self._daily_drawdown,
            "prev_5day_avg_profit": self._prev_5day_avg_profit,
            "daily_new_open_blocked": self._daily_new_open_blocked,
            "daily_hard_stop_triggered": self._daily_hard_stop_triggered,
            "current_date": self._current_date,
        }
