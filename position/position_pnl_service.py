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
import time
from datetime import datetime, timedelta
from typing import Optional, Any

import numpy as np

from infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from infra.resilience import should_trigger_take_profit, should_trigger_stop_loss  # P0-03: 统一止盈止损判断
from position.position_greeks import _REASON_STRATEGY_MAP


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
        from infra.market_time_service import get_market_time_service
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
        # FIX-SG-MAP-20260724: 查表前规范化open_reason，剥离'dry_run:'等模式前缀
        # 根因: dry_run模式给open_reason加'dry_run:'前缀,但_REASON_STRATEGY_MAP的key不带前缀,
        #   导致.get()匹配失败→回退'high_freq'→所有策略持仓错误归到high_freq组
        #   (position_id/strategy_group/风控止盈止损参数全部按S1-HFT处理)
        # 修复: 先精确匹配,失败则剥离最后一个':'后的裸reason再查,兼容带/不带前缀
        # FIX-SG-MAP-FAIL-LOUD-20260725: 从根因消除"回退high_freq"隐患(见position_command_service.py注释)
        _result = _REASON_STRATEGY_MAP.get(open_reason)
        if _result is None and open_reason and ':' in open_reason:
            _result = _REASON_STRATEGY_MAP.get(open_reason.rsplit(':', 1)[-1])
        if not _result:
            logging.critical(
                "[FIX-SG-MAP-FAIL-LOUD] open_reason='%s' 未注册到_REASON_STRATEGY_MAP, "
                "返回'unknown'策略组(不使用high_freq风控, 避免资金崩盘).",
                open_reason,
            )
            return 'unknown'
        return _result

    def _is_s3_s4_s5_risk_bypassed(self, record) -> bool:
        """ADD-S345-RISK-BYPASS-20260731: 判断是否跳过S3/S4/S5风控

        用户决策: dry_run模式下暂时关闭S3/S4/S5风控, 只为验证策略跑通模拟下单
        安全保障: 仅在dry_run=True时生效; 实盘模式风控始终启用
        """
        _sg = getattr(record, 'strategy_group', '')
        if _sg not in ('s3_box', 's4_spring', 's5_overnight'):
            return False
        try:
            from strategy.strategy_config_layer import STRATEGY_DEFAULTS as _bypass_cfg
            _bypass_enabled = _bypass_cfg.get('s3_s4_s5_risk_bypass_in_dry_run', True)
        except Exception:
            _bypass_enabled = True
        if not _bypass_enabled:
            return False
        _is_dry_run = False
        try:
            from config.params_service import get_params_service
            _is_dry_run = get_params_service().get_bool('dry_run_mode', False) or False
        except Exception:
            pass
        if not _is_dry_run:
            _is_dry_run = bool(getattr(self._ps, '_dry_run_active', False))
        if _is_dry_run:
            return True
        return False

    def _check_stop_profit(self, record, current_price: float) -> None:
        # FIX-20260704-STARTUP-GRACE: 启动后30秒内跳过止盈检查
        # ADD-S345-RISK-BYPASS-20260731: dry_run模式下跳过S3/S4/S5止盈检查
        if self._is_s3_s4_s5_risk_bypassed(record):
            return
        _now_ts = time.time()
        if not hasattr(self._ps, '_startup_close_grace_until'):
            self._ps._startup_close_grace_until = _now_ts + 30.0
        if _now_ts < self._ps._startup_close_grace_until:
            return
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
        # FIX-20260704-STARTUP-GRACE: 启动后30秒内跳过止损检查
        # ADD-S345-RISK-BYPASS-20260731: dry_run模式下跳过S3/S4/S5止损检查
        if self._is_s3_s4_s5_risk_bypassed(record):
            return
        _now_ts = time.time()
        if not hasattr(self._ps, '_startup_close_grace_until'):
            self._ps._startup_close_grace_until = _now_ts + 30.0
        if _now_ts < self._ps._startup_close_grace_until:
            return
        # FIX-HARDSTOP-MARKET-CLOSED-20260728: 收盘后跳过止损检查
        # 根因: 15:00收盘后, 硬止损检查仍每30s触发, _trigger_close_position因交易所收盘延后平仓,
        # 但硬止损反复触发57次浪费CPU/IO(15:00:07-15:29:16 si2609-C-8500案例, GFEX收盘)
        # 修复: 收盘后跳过止损检查, 次日开盘恢复(类似S5 gate, 与FIX-S5-POST-CLOSE-GATE-V2一致)
        # 不改变策略逻辑: 硬止损触发条件(_unrealized_pts <= -2.0)不变, 仅收盘后停止无效检查(无法平仓)
        # fail-OPEN: MarketOpenCache不可用时不跳过(继续检查, 保持原行为, 避免遗漏止损)
        try:
            from infra.market_time_service import get_market_open_cache
            if not get_market_open_cache().is_open():
                return
        except Exception:
            pass  # MarketOpenCache不可用, 不跳过止损检查(fail-OPEN, 保持原行为)
        if record.volume == 0:
            return
        # FIX-20260709-P0: 增加-2点绝对硬止损 (不依赖stop_loss_price设置)
        # 根因: 期权买卖价差约1点，5分钟时间止损导致每笔亏-1点；
        # 原止损close_stop_loss_ratio=0.3(30%)对低价期权过于宽松，需绝对点数止损截断亏损
        if record.open_price > 0 and current_price > 0:
            is_long = record.volume > 0
            if is_long:
                _unrealized_pts = current_price - record.open_price
            else:
                _unrealized_pts = record.open_price - current_price
            if _unrealized_pts <= -2.0:
                if getattr(record, '_closing', False) or getattr(record, 'closing_order_id', ''):
                    return
                logging.info(
                    '[PositionService] FIX-20260709-P0: -2点绝对硬止损触发, instrument=%s direction=%s '
                    'open=%.2f current=%.2f loss=%.2fpt',
                    record.instrument_id, 'LONG' if is_long else 'SHORT',
                    record.open_price, current_price, _unrealized_pts,
                )
                self._ps._trigger_close_position(
                    record, f"HardStopLoss@{current_price:.2f}(loss={_unrealized_pts:.2f}pt)", current_price)
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
                # 注: 期权到期平仓是平仓操作, 不影响开仓验证, 不设dry_run跳过开关(用户决策2026-07-31)
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
        # FIX-20260704-STARTUP-GRACE: 启动后30秒内跳过时间止损检查
        # 根因: 从JSONL恢复的持仓open_time为旧时间，elapsed远超hold_time，
        # 启动时立即触发全量平仓→平台拒绝result=-1→重试耗尽→CANNOT_CLOSE
        # 修复: 启动后30秒宽限期内跳过时间止损，待平台就绪+行情到达后再正常检查
        # 注: 时间止损是平仓操作, 不影响开仓验证, 不设dry_run跳过开关(用户决策2026-07-31)
        _now_ts = time.time()
        if not hasattr(self._ps, '_startup_close_grace_until'):
            self._ps._startup_close_grace_until = _now_ts + 30.0
        if _now_ts < self._ps._startup_close_grace_until:
            return
        # FIX-R37-TIMESTOP-VOLCHECK-20260728: volume=0时跳过时间止损
        # 根因: _check_stop_profit和_check_stop_loss都有if record.volume == 0: return,
        #   但_check_time_stop缺少此检查→已平仓持仓(volume=0)仍触发时间止损→
        #   _reduce_position发现closeable=0→R37-POS-INSUFFICIENT警告(36次)
        # 修复: 与stop_profit/stop_loss一致, volume=0时直接return
        if record.volume == 0:
            return
        now = now or datetime.now(_CHINA_TZ)
        open_reason = getattr(record, 'open_reason', '')
        _sg = getattr(record, 'strategy_group', '')
        max_hold_minutes = self.DEFAULT_MAX_HOLD_MINUTES
        _STRATEGY_HOLD_OVERRIDES = {
            'spring': 5.0, 'box': 60.0, 'arbitrage': 30.0,
            'market_making': 15.0, 'high_freq': 1.0,
            'intraday': 240.0, 'divergence': 45.0, 'resonance': 5.0,
            # ADD-S5-20260731: S5隔夜仓持仓时间(24小时, 与max_hold_minutes一致)
            's5_overnight': 1440.0,
        }
        if _sg in _STRATEGY_HOLD_OVERRIDES:
            max_hold_minutes = _STRATEGY_HOLD_OVERRIDES[_sg]
        try:
            from config.params_service import get_params_service
            ps = get_params_service()
            max_hold_minutes = ps.get_int('max_hold_minutes', max_hold_minutes)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] params_service load failed: %s", e)
        try:
            from param_pool.optimization.cycle_sharpe import get_cycle_resonance_module
            crm = get_cycle_resonance_module()
            strategy = self._resolve_strategy_group(open_reason)
            rs = crm.get_risk_surface(strategy)
            max_hold_minutes = rs.max_hold_seconds / 60.0
        except (ImportError, AttributeError, ZeroDivisionError) as e:
            logging.warning("[PositionService] cycle_resonance load failed: %s", e)
        if hasattr(self._ps, '_check_svc') and self._ps._check_svc is not None:
            trailing_reason = self._ps._check_svc.check_trailing_stop(record)
        else:
            from position.position_check_service import PositionCheckService
            trailing_reason = PositionCheckService(self._ps).check_trailing_stop(record)
        if trailing_reason:
            self._ps._trigger_close_position(record, trailing_reason, current_price=getattr(record, 'current_price', 0.0))
            return
        if record.open_time:
            _use_effective_time = True
            try:
                from config.params_service import get_params_service
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
                    # V4-FIX-C4: PLR调整上限=原始max_hold, 不允许超过(防止盈利持仓过度延长)
                    # 原则: PLR弹性调整可缩短持有时间(止损加速), 但不可延长超过原始max_hold
                    if adjusted_hold > max_hold_minutes:
                        adjusted_hold = max_hold_minutes
                        logging.warning(
                            '[V4-FIX-C4] PLR调整超过原始max_hold, 截断: instrument=%s plr_ratio=%.2f '
                            'adjusted=%.1fmin->max_hold=%.1fmin (不可延长硬止损)',
                            record.instrument_id, plr_ratio, orig_hold, max_hold_minutes,
                        )
                    else:
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
                from risk.risk_service import get_safety_meta_layer
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
                    # FIX-HARDSTOP-EFFECTIVE-TIME-20260728: 硬时间止损使用有效交易时间，与时间止损一致
                    # 根因: check_position_hard_time_stop内部用 elapsed_min=(now-open_time)/60.0 计算原始时间差,
                    #   但_check_time_stop自身已用_calc_effective_trading_minutes计算有效交易时间(排除隔夜)。
                    #   跨会话持仓的open_time来自前一交易日,原始时间差含隔夜(如684min=11.4h),
                    #   但有效交易时间可能仅294min(夜盘4h+日盘35min)。
                    #   原代码传bar_time=now.timestamp()→硬止损用原始时间差→新持仓立即触发"已持684min"。
                    # 修复: 传bar_time=open_ts+elapsed*60,使硬止损的elapsed_min=有效交易时间elapsed。
                    #   不改变策略逻辑: 仍fail-closed(有效时间达标仍触发止损),仅修正时间口径一致。
                    _effective_bar_time = (open_ts + elapsed * 60.0) if open_ts > 0 and elapsed >= 0 else (now.timestamp() if now else None)
                    hard_stop_reason = safety.check_position_hard_time_stop(
                        position_id=str(record.position_id) if hasattr(record, 'position_id') else record.instrument_id,
                        open_time=open_ts,
                        max_profit_reached=max_profit,
                        profit_slope=profit_slope,
                        peak_profit_pct=peak_profit_pct,
                        current_profit_pct=current_profit_pct,
                        bar_time=_effective_bar_time,
                        strategy_group=getattr(record, 'strategy_group', ''),
                    )
                    if hard_stop_reason:
                        self._ps._trigger_close_position(record, hard_stop_reason, current_price=getattr(record, 'current_price', 0.0))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug(f"[PositionService._check_time_stop] SafetyMetaLayer check error: {e}")

    def _check_two_stage_stop(self, record, now: datetime = None) -> None:
        # FIX-20260704-STARTUP-GRACE: 启动后30秒内跳过两阶段止损检查
        # 根因: 同_check_time_stop，恢复的持仓open_time为旧时间，立即触发止损
        # ADD-S345-RISK-BYPASS-20260731: dry_run模式下跳过S3/S4/S5两阶段止损
        if self._is_s3_s4_s5_risk_bypassed(record):
            return
        _now_ts = time.time()
        if not hasattr(self._ps, '_startup_close_grace_until'):
            self._ps._startup_close_grace_until = _now_ts + 30.0
        if _now_ts < self._ps._startup_close_grace_until:
            return
        # P0-1修复: 与回测引擎擎check_two_stage_stop逻辑对齐
        # Stage1: 持仓时间>=阈值 AND 最大浮盈>=阈值 → 标记stage1_passed
        # Stage2: stage1通过后，利润斜率衰减→触发平仓
        now = now or datetime.now(_CHINA_TZ)
        if not record.open_time or record.volume == 0:
            return
        _use_effective_time = True
        try:
            from config.params_service import get_params_service
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
            'spring': {'stage1_min_minutes': 3.0, 'stage1_profit_threshold': 0.001},
            'box': {'stage1_min_minutes': 30.0, 'stage1_profit_threshold': 0.002},
            'arbitrage': {'stage1_min_minutes': 15.0, 'stage1_profit_threshold': 0.005},
            'market_making': {'stage1_min_minutes': 10.0, 'stage1_profit_threshold': 0.01},
            'high_freq': {'stage1_min_minutes': 1.0, 'stage1_profit_threshold': 0.003},
            'intraday': {'stage1_min_minutes': 120.0, 'stage1_profit_threshold': 0.002},
            'resonance': {'stage1_min_minutes': 3.0, 'stage1_profit_threshold': 0.005},
            'divergence': {'stage1_min_minutes': 15.0, 'stage1_profit_threshold': 0.003},
        }
        _override = _TWO_STAGE_STRATEGY_OVERRIDES.get(_sg, {})
        try:
            from config.params_service import get_params_service
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
            from config.params_service import get_params_service
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
                            # 注: EOD平仓是平仓操作, 不影响开仓验证, 不设dry_run跳过开关(用户决策2026-07-31)
                            # FIX-S5-EOD-20260731: S5隔夜仓不应被EOD平仓(持仓>12小时, 跨日)
                            # 即使实盘模式, s5_overnight也不应被EOD_Close/EOD_Night_Close平仓
                            if _sg == 's5_overnight':
                                continue
                            _eod_close_records.append(record)
            for record in _eod_close_records:
                try:
                    self._ps._trigger_close_position(record, eod_reason, current_price=getattr(record, 'current_price', 0.0))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError):
                    pass
