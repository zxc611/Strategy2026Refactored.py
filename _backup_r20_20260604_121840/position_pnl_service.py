"""Position PnL Service - PnL计算+盈亏统计

从position_service.py拆分(CC-09):
- _check_stop_profit: 止盈检查
- _check_stop_loss: 止损检查
- _check_time_stop: 时间止损检查
- _check_two_stage_stop: 两阶段止损检查
- _check_option_expiry: 期权到期检查
- _calc_days_to_expiry: 计算到期天数
- _check_option_expiry_force_close: 期权到期强制平仓
- _check_eod_close: 日内平仓检查
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Any

_CHINA_TZ = timezone(timedelta(hours=8))


def _is_option_instrument(instrument_id: str) -> bool:
    return '-C-' in instrument_id or '-P-' in instrument_id


class PositionPnlService:
    """PnL计算+盈亏统计服务 — 从PositionService拆分"""

    DEFAULT_MAX_HOLD_MINUTES = 120
    EOD_CLOSE_HOUR = 14
    EOD_CLOSE_MINUTE = 55
    NIGHT_EOD_CLOSE_HOUR = 2
    NIGHT_EOD_CLOSE_MINUTE = 30
    PLR_RATIO_EXCELLENT = 1.5
    PLR_RATIO_GOOD = 1.0
    PLR_RATIO_POOR = 0.5
    PLR_RATIO_WARNING = 0.8
    PLR_HOLD_MULTIPLIER_EXCELLENT = 1.5
    PLR_HOLD_MULTIPLIER_GOOD = 1.2
    PLR_HOLD_MULTIPLIER_POOR = 0.6
    PLR_HOLD_MULTIPLIER_WARNING = 0.8
    TWO_STAGE_STOP_CONFIG = {
        'stage1_min_minutes': 90,
        'stage1_profit_threshold': 0.002,  # P0-1修复: 与回测引擎对齐，浮盈达标阈值
        'stage2_slope_window': 10,         # P0-1修复: 利润斜率窗口
        'stage2_slope_threshold': 0.0,     # P0-1修复: 斜率衰减阈值
    }

    def __init__(self, position_service: Any):
        self._ps = position_service

    def _check_stop_profit(self, record, current_price: float) -> None:
        if record.volume == 0:
            return
        if record.stop_profit_price > 0:
            triggered = False
            is_long = record.volume > 0
            is_short = record.volume < 0
            if is_long and current_price >= record.stop_profit_price:
                triggered = True
            elif is_short and current_price <= record.stop_profit_price:
                triggered = True
            if triggered:
                logging.info(
                    '[PositionService] R13-P0-LOG-02修复: 止盈触发, instrument=%s direction=%s price=%.2f tp_price=%.2f',
                    record.instrument_id, 'LONG' if is_long else 'SHORT', current_price, record.stop_profit_price,
                )
                self._ps._trigger_close_position(record, f"StopProfit@{current_price:.2f}", current_price)
            else:
                logging.debug(
                    '[PositionService] R13-P0-LOG-02修复: 止盈未触发, instrument=%s price=%.2f tp_price=%.2f (距离=%.2f)',
                    record.instrument_id, current_price, record.stop_profit_price,
                    abs(current_price - record.stop_profit_price),
                )

    def _check_stop_loss(self, record, current_price: float) -> None:
        if record.volume == 0:
            return
        if record.stop_loss_price <= 0:
            if record.volume != 0:
                if record.stop_loss_price == 0:
                    logging.warning("[R25-BV-P1-04-FIX] 止损价格=0,持仓无保护: inst=%s vol=%d open=%.2f",
                                    record.instrument_id, record.volume, record.open_price)
                else:
                    logging.error("[R26-P1-BV-04] 止损价格<0(异常值),持仓无保护: inst=%s vol=%d sl=%.2f open=%.2f",
                                  record.instrument_id, record.volume, record.stop_loss_price, record.open_price)
            return
        triggered = False
        is_long = record.volume > 0
        is_short = record.volume < 0
        if is_long and current_price <= record.stop_loss_price:
            triggered = True
        elif is_short and current_price >= record.stop_loss_price:
            triggered = True
        if triggered:
            logging.info(
                '[PositionService] R13-P0-LOG-02修复: 止损触发, instrument=%s direction=%s price=%.2f sl_price=%.2f',
                record.instrument_id, 'LONG' if is_long else 'SHORT', current_price, record.stop_loss_price,
            )
            self._ps._trigger_close_position(record, f"StopLoss@{current_price:.2f}", current_price)
        else:
            logging.debug(
                '[PositionService] R13-P0-LOG-02修复: 止损未触发, instrument=%s price=%.2f sl_price=%.2f (距离=%.2f)',
                record.instrument_id, current_price, record.stop_loss_price,
                abs(current_price - record.stop_loss_price),
            )

    def _check_option_expiry(self, instrument_id: str) -> None:
        if not _is_option_instrument(instrument_id):
            return
        with self._ps._get_instrument_lock(instrument_id):
            if instrument_id not in self._ps.positions:
                return
            for pid in list(self._ps.positions[instrument_id]):
                record = self._ps.positions[instrument_id].get(pid)
                if record is None:
                    continue
                if record.volume == 0:
                    continue
                try:
                    days_to_expiry = self._calc_days_to_expiry(instrument_id)
                    if days_to_expiry is not None and days_to_expiry <= 0:
                        logging.warning(
                            '[PositionService] R13-P1-BIZ-04修复: 期权到期强制平仓, '
                            'instrument=%s days_to_expiry=%d, 触发强制平仓',
                            instrument_id, days_to_expiry,
                        )
                        self._ps._trigger_close_position(record, f"OptionExpiry@{instrument_id}")
                except Exception as e:
                    logging.debug('[PositionService] _check_option_expiry error for %s: %s', instrument_id, e)

    @staticmethod
    def _calc_days_to_expiry(instrument_id: str) -> Optional[int]:
        try:
            parts = instrument_id.split('-')
            if len(parts) < 2:
                return None
            code_part = parts[0]
            year_month = ''
            for c in reversed(code_part):
                if c.isdigit():
                    year_month = c + year_month
                else:
                    break
            if len(year_month) < 3:
                return None
            year = 2000 + int(year_month[:2]) if len(year_month) == 4 else 2000 + int(year_month[:2])
            month = int(year_month[2:]) if len(year_month) == 4 else int(year_month[2:])
            if month < 1 or month > 12:
                return None
            from datetime import date
            first_day = date(year, month, 1)
            first_friday = first_day
            while first_friday.weekday() != 4:
                first_friday = first_friday + timedelta(days=1)
            third_friday = first_friday + timedelta(days=14)
            today = datetime.now(_CHINA_TZ).date()
            return (third_friday - today).days
        except Exception:
            return None

    def _check_time_stop(self, record, now: datetime = None) -> None:
        now = now or datetime.now(_CHINA_TZ)
        open_reason = getattr(record, 'open_reason', '')
        max_hold_minutes = self.DEFAULT_MAX_HOLD_MINUTES
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            max_hold_minutes = ps.get_int('max_hold_minutes', self.DEFAULT_MAX_HOLD_MINUTES)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] params_service load failed: %s", e)
        try:
            from param_pool.cycle_resonance_module import get_cycle_resonance_module
            crm = get_cycle_resonance_module()
            strategy = self._ps._map_reason_to_strategy(open_reason)
            rs = crm.get_risk_surface(strategy)
            max_hold_minutes = rs.max_hold_seconds / 60.0
        except (ImportError, AttributeError, ZeroDivisionError) as e:
            logging.warning("[PositionService] cycle_resonance load failed: %s", e)
        if hasattr(self._ps, '_check_svc') and self._ps._check_svc is not None:
            trailing_reason = self._ps._check_svc.check_trailing_stop(record)
        else:
            from ali2026v3_trading.position_check_service import PositionCheckService
            trailing_reason = PositionCheckService(self._ps).check_trailing_stop(record)
        if trailing_reason:
            self._ps._trigger_close_position(record, trailing_reason)
            return
        if record.open_time:
            elapsed = (now - record.open_time).total_seconds() / 60
            adjusted_hold = max_hold_minutes
            current_plr = getattr(record, 'current_plr', 0.0)
            target_plr = getattr(record, 'target_plr', 0.0)
            if target_plr > 0 and current_plr > 0:
                plr_ratio = current_plr / target_plr
                orig_hold = adjusted_hold
                if plr_ratio >= self.PLR_RATIO_EXCELLENT:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_EXCELLENT
                elif plr_ratio >= self.PLR_RATIO_GOOD:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_GOOD
                elif plr_ratio < self.PLR_RATIO_POOR:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_POOR
                elif plr_ratio < self.PLR_RATIO_WARNING:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_WARNING
                if adjusted_hold != orig_hold:
                    logging.info(
                        '[PositionService] 时间止损PLR弹性调整: instrument=%s current_plr=%.2f target_plr=%.2f '
                        'plr_ratio=%.2f max_hold=%.1fmin -> adjusted=%.1fmin',
                        record.instrument_id, current_plr, target_plr, plr_ratio,
                        orig_hold, adjusted_hold,
                    )
            if elapsed >= adjusted_hold:
                logging.info(
                    '[PositionService] 时间止损触发: instrument=%s elapsed=%.1fmin adjusted_hold=%.1fmin reason=%s',
                    record.instrument_id, elapsed, adjusted_hold, f"TimeStop@{elapsed:.0f}min(plr_adj)",
                )
                self._ps._trigger_close_position(record, f"TimeStop@{elapsed:.0f}min(plr_adj)")
                return

            try:
                from ali2026v3_trading.risk_service import get_safety_meta_layer
                _sid = str(getattr(self._ps, 'strategy_id', '') or 'global')
                safety = get_safety_meta_layer(params=self._ps._params if hasattr(self._ps, '_params') else None, strategy_id=_sid)
                open_ts = record.open_time
                if isinstance(open_ts, datetime):
                    open_ts = open_ts.timestamp()
                elif not isinstance(open_ts, (int, float)):
                    open_ts = 0
                if open_ts > 0:
                    max_profit = getattr(record, '_max_profit_pct', 0.0)
                    profit_slope = getattr(record, 'profit_slope', 0.0)
                    peak_profit_pct = getattr(record, '_max_profit_pct', 0.0)
                    current_profit_pct = 0.0
                    current_price = getattr(record, 'current_price', 0.0)
                    if record.open_price > 0 and current_price > 0:
                        if record.volume > 0:
                            current_profit_pct = (current_price - record.open_price) / record.open_price
                        else:
                            current_profit_pct = (record.open_price - current_price) / record.open_price
                    hard_stop_reason = safety.check_position_hard_time_stop(
                        position_id=str(record.position_id) if hasattr(record, 'position_id') else record.instrument_id,
                        open_time=open_ts,
                        max_profit_reached=max_profit,
                        profit_slope=profit_slope,
                        peak_profit_pct=peak_profit_pct,
                        current_profit_pct=current_profit_pct,
                        bar_time=now.timestamp() if now else None,
                    )
                    if hard_stop_reason:
                        self._ps._trigger_close_position(record, hard_stop_reason)
            except Exception as e:
                logging.debug(f"[PositionService._check_time_stop] SafetyMetaLayer check error: {e}")

    def _check_two_stage_stop(self, record, now: datetime = None) -> None:
        # P0-1修复: 与回测引擎_check_two_stage_stop逻辑对齐
        # Stage1: 持仓时间>=阈值 AND 最大浮盈>=阈值 → 标记stage1_passed
        # Stage2: stage1通过后，利润斜率衰减→触发平仓
        now = now or datetime.now(_CHINA_TZ)
        if not record.open_time or record.volume == 0:
            return
        elapsed_minutes = (now - record.open_time).total_seconds() / 60.0
        current_price = getattr(record, 'current_price', 0.0)
        if record.open_price <= 0 or current_price <= 0:
            return
        # 计算浮动盈亏百分比
        if record.volume > 0:
            float_pnl_pct = (current_price - record.open_price) / record.open_price
        else:
            float_pnl_pct = (record.open_price - current_price) / record.open_price
        # 更新最大浮盈
        if float_pnl_pct > record._max_profit_pct:
            record._max_profit_pct = float_pnl_pct
        # 更新利润历史
        if record._profit_history is None:
            record._profit_history = []
        record._profit_history.append(float_pnl_pct)
        if len(record._profit_history) > 1000:
            record._profit_history = record._profit_history[-1000:]
        # 读取参数
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            stage1_min_minutes = ps.get_float('two_stage_stop_stage1_min_minutes', self.TWO_STAGE_STOP_CONFIG['stage1_min_minutes'])
            stage1_profit_threshold = ps.get_float('stage1_profit_threshold', 0.002)
            stage2_slope_window = max(2, ps.get_int('stage2_slope_window', 10))
            stage2_slope_threshold = ps.get_float('stage2_slope_threshold', 0.0)
        except (ImportError, AttributeError):
            stage1_min_minutes = self.TWO_STAGE_STOP_CONFIG['stage1_min_minutes']
            stage1_profit_threshold = 0.002
            stage2_slope_window = 10
            stage2_slope_threshold = 0.0
        # Stage1: 浮盈达标 → 标记通过
        if not record.stage1_passed:
            if elapsed_minutes >= stage1_min_minutes and record._max_profit_pct >= stage1_profit_threshold:
                record.stage1_passed = True
                logging.info(
                    '[PositionService] 两阶段止损 Stage1通过: instrument=%s elapsed=%.1fmin max_profit=%.4f threshold=%.4f',
                    record.instrument_id, elapsed_minutes, record._max_profit_pct, stage1_profit_threshold,
                )
        # Stage1未通过则不检查Stage2
        if not record.stage1_passed:
            return
        # Stage2: 利润斜率衰减 → 触发平仓
        if len(record._profit_history) >= stage2_slope_window:
            window = record._profit_history[-stage2_slope_window:]
            slope = (window[-1] - window[0]) / stage2_slope_window
            record.profit_slope = slope
            if slope < stage2_slope_threshold:
                logging.info(
                    '[PositionService] 两阶段止损 Stage2触发(利润斜率衰减): instrument=%s slope=%.6f threshold=%.6f',
                    record.instrument_id, slope, stage2_slope_threshold,
                )
                self._ps._trigger_close_position(record, f"TwoStageStop-S2-Slope@{elapsed_minutes:.0f}min")

    def _check_option_expiry_force_close(self) -> None:
        with self._ps.global_lock:
            for inst_id in list(self._ps.positions):
                self._check_option_expiry(inst_id)

    def _check_eod_close(self, now: datetime = None) -> None:
        now = now or datetime.now(_CHINA_TZ)
        eod_close_hour = self.EOD_CLOSE_HOUR
        eod_close_minute = self.EOD_CLOSE_MINUTE
        night_eod_close_hour = self.NIGHT_EOD_CLOSE_HOUR
        night_eod_close_minute = self.NIGHT_EOD_CLOSE_MINUTE
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            eod_close_hour = ps.get_int('eod_close_hour', self.EOD_CLOSE_HOUR)
            eod_close_minute = ps.get_int('eod_close_minute', self.EOD_CLOSE_MINUTE)
            night_eod_close_hour = ps.get_int('night_session_eod_hour', self.NIGHT_EOD_CLOSE_HOUR)
            night_eod_close_minute = ps.get_int('night_session_eod_minute', self.NIGHT_EOD_CLOSE_MINUTE)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] EOD params load failed: %s", e)
        is_eod = False
        eod_reason = ""
        _is_trading_day = now.weekday() < 5
        if not _is_trading_day:
            logging.debug("[PositionService] R14-P1-BIZ-13: 非交易日(weekday=%d)，跳过EOD平仓", now.weekday())
            return
        if now.hour == eod_close_hour and now.minute >= eod_close_minute:
            is_eod = True
            eod_reason = "EOD_Close"
        elif now.hour == night_eod_close_hour and now.minute >= night_eod_close_minute:
            is_eod = True
            eod_reason = "EOD_Night_Close"
        if is_eod:
            if eod_reason == "EOD_Close":
                self._check_option_expiry_force_close()
            with self._ps.global_lock:
                for inst_id in list(self._ps.positions):
                    pos_dict = self._ps.positions.get(inst_id)
                    if pos_dict is None:
                        continue
                    for pid in list(pos_dict):
                        record = pos_dict.get(pid)
                        if record is None:
                            continue
                        if record.volume != 0:
                            self._ps._trigger_close_position(record, eod_reason)
