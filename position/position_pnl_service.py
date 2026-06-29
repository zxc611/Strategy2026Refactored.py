# MODULE_ID: M1-206
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
from datetime import datetime, timedelta
from typing import Optional, Any

import numpy as np

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.resilience import should_trigger_take_profit, should_trigger_stop_loss  # P0-03: 统一止盈止损判断
from ali2026v3_trading.position.position_greeks import _REASON_STRATEGY_MAP


def _is_option_instrument(instrument_id: str) -> bool:
    return '-C-' in instrument_id or '-P-' in instrument_id


def _calc_effective_trading_minutes(open_time: datetime, now: datetime) -> float:
    """
    计算有效交易时间（分钟），排除非交易日和非交易时段
    
    Args:
        open_time: 开仓时间
        now: 当前时间
    
    Returns:
        float: 有效交易时间（分钟）
    
    场景：
    1. 周五15:00开仓 → 周一9:00检查 → 自然时间66小时，有效时间约18小时（周五15-15:30 + 周一9:00-...）
    2. 节假日前开仓 → 节假日后检查 → 排除整个假期
    3. 夜盘开仓 → 次日夜盘检查 → 正确计算跨夜时间
    """
    if now <= open_time:
        return 0.0
    
    try:
        from ali2026v3_trading.infra.market_time_service import get_market_time_service
        mts = get_market_time_service()
    except (ImportError, AttributeError):
        return (now - open_time).total_seconds() / 60.0
    
    total_trading_minutes = 0.0
    current_date = open_time.date()
    end_date = now.date()
    
    day_session_hours = 4.0
    night_session_hours = 5.0
    
    while current_date <= end_date:
        if mts.is_trading_day(current_date):
            day_start = datetime(current_date.year, current_date.month, current_date.day, 9, 0, tzinfo=_CHINA_TZ)
            day_end = datetime(current_date.year, current_date.month, current_date.day, 15, 0, tzinfo=_CHINA_TZ)
            
            if current_date.weekday() < 5:
                session_start = max(open_time, day_start)
                session_end = min(now, day_end)
                if session_end > session_start:
                    total_trading_minutes += (session_end - session_start).total_seconds() / 60.0
            
            next_date = current_date + timedelta(days=1)
            night_start = datetime(current_date.year, current_date.month, current_date.day, 21, 0, tzinfo=_CHINA_TZ)
            night_end = datetime(next_date.year, next_date.month, next_date.day, 2, 30, tzinfo=_CHINA_TZ)
            
            session_start = max(open_time, night_start)
            session_end = min(now, night_end)
            if session_end > session_start:
                total_trading_minutes += (session_end - session_start).total_seconds() / 60.0
        
        current_date += timedelta(days=1)
    
    return max(0.0, total_trading_minutes)


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

    def _resolve_strategy_group(self, open_reason: str) -> str:
        mapper = getattr(self._ps, '_map_reason_to_strategy', None)
        if callable(mapper):
            try:
                return mapper(open_reason)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as err:
                logging.warning("[PositionPnlService] _map_reason_to_strategy调用失败，使用回退映射: %s", err)
        return _REASON_STRATEGY_MAP.get(open_reason, 'high_freq')

    def _check_stop_profit(self, record, current_price: float) -> None:
        if record.volume == 0:
            return
        if record.stop_profit_price > 0:
            is_long = record.volume > 0
            triggered = should_trigger_take_profit(current_price, record.stop_profit_price, is_long=is_long)
            if triggered:
                if getattr(record, '_closing', False) or getattr(record, 'closing_order_id', ''):
                    return
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
        if should_trigger_stop_loss(current_price, record.stop_loss_price, is_long=is_long):
            triggered = True
        if triggered:
            if getattr(record, '_closing', False) or getattr(record, 'closing_order_id', ''):
                return
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
                        self._ps._trigger_close_position(record, f"OptionExpiry@{instrument_id}", current_price=getattr(record, 'current_price', 0.0))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return None

    def _check_time_stop(self, record, now: datetime = None) -> None:
        now = now or datetime.now(_CHINA_TZ)
        open_reason = getattr(record, 'open_reason', '')
        _sg = getattr(record, 'strategy_group', '')
        max_hold_minutes = self.DEFAULT_MAX_HOLD_MINUTES
        _STRATEGY_HOLD_OVERRIDES = {
            'spring': 120.0, 'box': 60.0, 'arbitrage': 30.0,
            'market_making': 15.0, 'high_freq': 45.0,
        }
        if _sg in _STRATEGY_HOLD_OVERRIDES:
            max_hold_minutes = _STRATEGY_HOLD_OVERRIDES[_sg]
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            ps = get_params_service()
            max_hold_minutes = ps.get_int('max_hold_minutes', max_hold_minutes)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] params_service load failed: %s", e)
        try:
            from ali2026v3_trading.param_pool.optimization.cycle_sharpe import get_cycle_resonance_module
            crm = get_cycle_resonance_module()
            strategy = self._resolve_strategy_group(open_reason)
            rs = crm.get_risk_surface(strategy)
            max_hold_minutes = rs.max_hold_seconds / 60.0
        except (ImportError, AttributeError, ZeroDivisionError) as e:
            logging.warning("[PositionService] cycle_resonance load failed: %s", e)
        if hasattr(self._ps, '_check_svc') and self._ps._check_svc is not None:
            trailing_reason = self._ps._check_svc.check_trailing_stop(record)
        else:
            from ali2026v3_trading.position.position_check_service import PositionCheckService
            trailing_reason = PositionCheckService(self._ps).check_trailing_stop(record)
        if trailing_reason:
            self._ps._trigger_close_position(record, trailing_reason, current_price=getattr(record, 'current_price', 0.0))
            return
        if record.open_time:
            _use_effective_time = True
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps_tmp = get_params_service()
                _use_effective_time = _ps_tmp.get_bool('use_effective_trading_time', True)
            except (ImportError, AttributeError):
                pass
            
            if _use_effective_time:
                elapsed = _calc_effective_trading_minutes(record.open_time, now)
            else:
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
                if getattr(record, 'closing_order_id', '') or getattr(record, '_closing', False):
                    return
                _snapshot_strat = ''
                _oss = getattr(record, 'open_signal_snapshot', '')
                if _oss and 'strat=' in _oss:
                    _snapshot_strat = _oss.split('strat=')[-1].split('|')[0]
                logging.info(
                    '[PositionService] 时间止损触发: instrument=%s elapsed=%.1fmin adjusted_hold=%.1fmin '
                    'strategy_group=%s open_signal_strat=%s reason=%s',
                    record.instrument_id, elapsed, adjusted_hold,
                    _sg, _snapshot_strat, f"TimeStop@{elapsed:.0f}min(plr_adj)",
                )
                self._ps._trigger_close_position(record, f"TimeStop@{elapsed:.0f}min(plr_adj)", current_price=getattr(record, 'current_price', 0.0))
                return

            try:
                from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
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
                        strategy_group=getattr(record, 'strategy_group', ''),
                    )
                    if hard_stop_reason:
                        self._ps._trigger_close_position(record, hard_stop_reason, current_price=getattr(record, 'current_price', 0.0))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug(f"[PositionService._check_time_stop] SafetyMetaLayer check error: {e}")

    def _check_two_stage_stop(self, record, now: datetime = None) -> None:
        # P0-1修复: 与回测引擎擎check_two_stage_stop逻辑对齐
        # Stage1: 持仓时间>=阈值 AND 最大浮盈>=阈值 → 标记stage1_passed
        # Stage2: stage1通过后，利润斜率衰减→触发平仓
        now = now or datetime.now(_CHINA_TZ)
        if not record.open_time or record.volume == 0:
            return
        _use_effective_time = True
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            _ps_tmp = get_params_service()
            _use_effective_time = _ps_tmp.get_bool('use_effective_trading_time', True)
        except (ImportError, AttributeError):
            pass
        
        if _use_effective_time:
            elapsed_minutes = _calc_effective_trading_minutes(record.open_time, now)
        else:
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
        # 读取参数（按strategy_group差异化）
        _sg = getattr(record, 'strategy_group', '')
        _TWO_STAGE_STRATEGY_OVERRIDES = {
            'spring': {'stage1_min_minutes': 60.0, 'stage1_profit_threshold': 0.001},
            'box': {'stage1_min_minutes': 30.0, 'stage1_profit_threshold': 0.002},
            'arbitrage': {'stage1_min_minutes': 15.0, 'stage1_profit_threshold': 0.005},
            'market_making': {'stage1_min_minutes': 10.0, 'stage1_profit_threshold': 0.01},
            'high_freq': {'stage1_min_minutes': 20.0, 'stage1_profit_threshold': 0.003},
        }
        _override = _TWO_STAGE_STRATEGY_OVERRIDES.get(_sg, {})
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            ps = get_params_service()
            stage1_min_minutes = _override.get('stage1_min_minutes', ps.get_float('two_stage_stop_stage1_min_minutes', self.TWO_STAGE_STOP_CONFIG['stage1_min_minutes']))
            stage1_profit_threshold = _override.get('stage1_profit_threshold', ps.get_float('stage1_profit_threshold', 0.002))
            stage2_slope_window = max(2, ps.get_int('stage2_slope_window', 10))
            stage2_slope_threshold = ps.get_float('stage2_slope_threshold', 0.0)
        except (ImportError, AttributeError):
            stage1_min_minutes = _override.get('stage1_min_minutes', self.TWO_STAGE_STOP_CONFIG['stage1_min_minutes'])
            stage1_profit_threshold = _override.get('stage1_profit_threshold', 0.002)
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
                self._ps._trigger_close_position(record, f"TwoStageStop-S2-Slope@{elapsed_minutes:.0f}min", current_price=getattr(record, 'current_price', 0.0))

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
            from ali2026v3_trading.config.params_service import get_params_service
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
            _eod_close_records = []
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
                            _sg = getattr(record, 'strategy_group', '')
                            if _sg in ('spring', 'arbitrage') and eod_reason == "EOD_Night_Close":
                                continue
                            _eod_close_records.append(record)
            for record in _eod_close_records:
                try:
                    self._ps._trigger_close_position(record, eod_reason, current_price=getattr(record, 'current_price', 0.0))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError):
                    pass
