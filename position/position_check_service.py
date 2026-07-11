# MODULE_ID: M1-202
"""Position Check Service - 风控检查联动

从position_service.py拆分(CC-09):
- check_position_limit: 检查持仓限额
- calculate_position_risk: 计算持仓风险
- validate_net_position_consistency: 净持仓一致性验证
- check_trailing_stop: 浮动止盈检查
- check_all_positions: 全持仓风控检查
- _validate_pnl_equity_consistency: PnL权益一致性校验
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional, Any

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ


class PositionCheckService:
    """风控检查联动服务 — 从PositionService拆分"""

    DEFAULT_TARGET_PLR = 2.0
    DEFAULT_TP_RATIO = 1.8
    TRAILING_STOP_ACTIVATION_PCT = 0.5
    TRAILING_STOP_RETRACEMENT_PCT = 0.2

    def __init__(self, position_service: Any):
        self._ps = position_service

    def check_position_limit(self, account_id: str, required_amount: float) -> bool:
        try:
            if self._ps._risk_bridge is not None:
                result = self._ps._risk_bridge.check_position_limit(account_id, required_amount)
                from ali2026v3_trading.risk.risk_support import BridgeRiskLevel
                return result.level == BridgeRiskLevel.PASS
            else:
                logging.error("[PositionService.check_position_limit] RiskService not available, BLOCKING position check (fail-safe)")
                return False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService.check_position_limit] Error: {e}")
            return False

    def calculate_position_risk(self, instrument_id: str) -> float:
        try:
            position_info = self._ps.get_position(instrument_id)
            volume = position_info.get("volume", 0)
            average_price = position_info.get("average_price", 0)
            multiplier = 1.0
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                ps = get_params_service()
                meta = ps.get_instrument_meta_by_id(instrument_id)
                if meta:
                    multiplier = float(meta.get("contract_size", 1.0))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                logging.debug(f"[PositionService.calculate_position_risk] 获取合约乘数失败，使用默认值1.0: {_e}")
            risk = abs(volume) * average_price * multiplier
            self.validate_net_position_consistency(instrument_id)
            return risk
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService.calculate_position_risk] Error: {e}")
            return 0.0

    def validate_net_position_consistency(self, instrument_id: str) -> bool:
        with self._ps._get_instrument_lock(instrument_id):
            if instrument_id not in self._ps.positions:
                return True
            long_volume = 0
            short_volume = 0
            for rec in self._ps.positions[instrument_id].values():
                if rec.volume > 0:
                    long_volume += rec.volume
                elif rec.volume < 0:
                    short_volume += abs(rec.volume)
            independent_net = long_volume - short_volume
            stored_net = sum(rec.volume for rec in self._ps.positions[instrument_id].values())
            if independent_net != stored_net:
                logging.critical(
                    "[PositionService] INV-POS-02: 净持仓不一致! instrument=%s "
                    "long=%d short=%d independent_net=%d stored_net=%d",
                    instrument_id, long_volume, short_volume, independent_net, stored_net,
                )
                try:
                    from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='net_position_inconsistency',
                            level='CRITICAL',
                            message=f"INV-P1-02: 净持仓不一致 instrument={instrument_id} "
                                    f"long={long_volume} short={short_volume} "
                                    f"independent_net={independent_net} stored_net={stored_net}",
                        ), async_mode=True)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _eb_e:
                    logging.debug("[PositionService] INV-P1-02: 事件总线告警失败: %s", _eb_e)
                return False
            return True

    def check_trailing_stop(self, record) -> Optional[str]:
        current_price = getattr(record, 'current_price', 0.0)
        open_price = record.open_price
        from ali2026v3_trading.infra.shared_utils import safe_price_check
        if not safe_price_check(open_price) or not safe_price_check(current_price):
            return None
        is_long = record.volume > 0
        if is_long:
            current_profit_pct = (current_price - open_price) / open_price
        else:
            current_profit_pct = (open_price - current_price) / open_price
        target_plr = getattr(record, 'target_plr', self.DEFAULT_TARGET_PLR)
        tp_ratio = getattr(record, 'take_profit_ratio', self.DEFAULT_TP_RATIO)
        target_profit_pct = tp_ratio * self.TRAILING_STOP_ACTIVATION_PCT
        if current_profit_pct <= target_profit_pct:
            return None
        _key = f"trailing_stop_{record.instrument_id}_{record.position_id if hasattr(record, 'position_id') else id(record)}"
        if not hasattr(self._ps, '_trailing_stop_activated'):
            self._ps._trailing_stop_activated = {}
        if _key not in self._ps._trailing_stop_activated:
            self._ps._trailing_stop_activated[_key] = True
            logging.info(
                '[PositionService] 浮动止盈激活: instrument=%s profit=%.2f%% > target_half=%.2f%%',
                record.instrument_id, current_profit_pct * 100, target_profit_pct * 100,
            )
        peak_key = f"trailing_peak_{_key}"
        if not hasattr(self._ps, '_trailing_stop_peaks'):
            self._ps._trailing_stop_peaks = {}
        prev_peak = self._ps._trailing_stop_peaks.get(peak_key, current_profit_pct)
        peak_profit = max(prev_peak, current_profit_pct)
        self._ps._trailing_stop_peaks[peak_key] = peak_profit
        trailing_stop_pct = peak_profit * self.TRAILING_STOP_RETRACEMENT_PCT
        if current_profit_pct < trailing_stop_pct and peak_profit > target_profit_pct:
            logging.info(
                '[PositionService] 浮动止盈触发: instrument=%s peak=%.2f%% current=%.2f%% trailing_stop=%.2f%%',
                record.instrument_id, peak_profit * 100, current_profit_pct * 100, trailing_stop_pct * 100,
            )
            return f"TrailingStop@{current_profit_pct:.1%}(peak={peak_profit:.1%})"
        return None

    def check_all_positions(self) -> None:
        try:
            from ali2026v3_trading.strategy_judgment.causal_chain_utils import CyclicDependencyGuard
            from ali2026v3_trading.position.position_service import _HAS_CAUSAL_CHAIN
            _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        except ImportError:
            _cyclic_guard = None
        if _cyclic_guard and not _cyclic_guard.enter("position_check_all"):

            return
        try:
            with self._ps._cross_shard_lock:
                now = datetime.now(_CHINA_TZ)
                # FIX-READ-UNIQUE-10: 收集需要检查的record快照在global_lock内，
                # 但实际_check_time_stop/_check_two_stage_stop(可能触发_trigger_close_position)
                # 移到global_lock外执行，避免在global_lock内触发平仓导致死锁
                # (_trigger_close_position需要instrument_lock，若另一线程持有instrument_lock
                # 并等待global_lock则形成死锁)
                _records_to_check = []
                with self._ps.global_lock:
                    for inst_id in list(self._ps.positions):
                        pos_dict = self._ps.positions.get(inst_id)
                        if pos_dict is None:
                            continue
                        for pid in list(pos_dict):
                            record = pos_dict.get(pid)
                            if record is None:
                                continue
                            if record.volume == 0:
                                continue
                            _records_to_check.append((inst_id, pid, record))
                # 在global_lock外执行可能触发平仓的检查
                # FIX-20260708-CLOSE-BREAK: 原代码只调用_check_time_stop和_check_two_stage_stop，
                # 遗漏了_check_stop_profit和_check_stop_loss，导致止盈止损永远不会被定时检查。
                # 这是7/6夜盘146条持仓无平仓动作的根因之一。
                for _inst_id, _pid, _record in _records_to_check:
                    try:
                        # 获取当前价格供止盈止损检查
                        _current_price = getattr(_record, 'current_price', 0.0)
                        if _current_price <= 0:
                            try:
                                from ali2026v3_trading.data.data_service import get_data_service
                                _ds = get_data_service()
                                if _ds and _ds.realtime_cache:
                                    _current_price = _ds.realtime_cache.get_latest_price(_inst_id) or 0.0
                            except (ValueError, KeyError, TypeError, AttributeError):
                                pass
                        if _current_price > 0:
                            if not getattr(_record, 'current_price', 0.0):
                                _record.current_price = _current_price
                            self._ps._check_stop_profit(_record, _current_price)
                            self._ps._check_stop_loss(_record, _current_price)
                            self._ps.check_trailing_stop(_record)
                        self._ps._check_time_stop(_record, now)
                        self._ps._check_two_stage_stop(_record, now)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _check_err:
                        logging.warning("[READ-UNIQUE-10] 持仓检查异常 inst=%s pid=%s: %s",
                                        _inst_id, _pid, _check_err)
                self._ps._check_eod_close(now)
        finally:
            if _cyclic_guard:
                _cyclic_guard.exit("position_check_all")

        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self._ps, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._ps._params if hasattr(self._ps, '_params') else None, strategy_id=_sid)
            total_equity = 0.0
            with self._ps.global_lock:
                for _inst_id, pos_dict in self._ps.positions.items():
                    for _pid, rec in pos_dict.items():
                        if rec.volume != 0 and rec.open_price > 0:
                            market_price = rec.open_price
                            try:
                                from ali2026v3_trading.data.data_service import get_data_service
                                ds = get_data_service()
                                if ds and ds.realtime_cache:
                                    mp = ds.realtime_cache.get_latest_price(rec.instrument_id)
                                    if mp and mp > 0:
                                        market_price = mp
                            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                                pass
                                pass
                            total_equity += abs(rec.volume) * market_price
            if total_equity > 0:
                safety.on_equity_update(total_equity)
                try:
                    from ali2026v3_trading.position.position_service import get_cross_strategy_risk_guard
                    guard = get_cross_strategy_risk_guard()
                    if hasattr(safety, '_peak_equity') and safety._peak_equity > 0:
                        drawdown_pct = max(0.0, (safety._peak_equity - total_equity) / safety._peak_equity * 100.0)
                        guard.set_daily_drawdown(drawdown_pct)
                except (ImportError, AttributeError, ZeroDivisionError) as e:
                    logging.debug("[PositionService] drawdown update failed: %s", e)
        # FIX-R32-EXCEPT: 扩大异常捕获范围，防止equity update中的任何异常
        # (如TypeError)阻断整个check_all_positions，导致后续PnL校验和下一轮持仓检查无法运行
        except (ImportError, AttributeError, TypeError, ValueError, RuntimeError) as e:
            logging.debug("[PositionService] equity update failed: %s", e)

        self._validate_pnl_equity_consistency()

    def _validate_pnl_equity_consistency(self) -> None:
        try:
            # FIX-20260708-V6: dry_run模式下跳过PnL权益一致性校验
            # 根因: dry_run模式下equity来自平台真实账户，而realized_pnl仅记录虚拟交易，
            # 两者口径不同导致恒定误报INV-P1-01(ERROR级日志噪音)
            _dry_run = getattr(self._ps, '_dry_run_active', False)
            if not _dry_run:
                _dry_run = getattr(self._ps, '_dry_run_mode', False)
            if not _dry_run:
                try:
                    from ali2026v3_trading.config.config_service import get_cached_params
                    _dry_run = bool((get_cached_params() or {}).get('dry_run_mode', False))
                except (ImportError, AttributeError, TypeError):
                    pass
            if _dry_run:
                return
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self._ps, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._ps._params if hasattr(self._ps, '_params') else None, strategy_id=_sid)
            if safety is None:
                return
            equity = 0.0
            if safety._equity_series:
                equity = list(safety._equity_series)[-1] if safety._equity_series else 0.0
            if equity <= 0:
                return
            initial_capital = getattr(safety, '_daily_start_equity', None)
            if initial_capital is None or initial_capital <= 0:
                return
            realized_pnl = 0.0
            _has_active_position = False
            # FIX-READ-UNIQUE-11: 使用instrument_lock逐合约加锁而非global_lock，
            # 避免长时间持有global_lock阻塞其他需要global_lock的操作(如check_all_positions)
            for _inst_id in list(self._ps.positions.keys()):
                with self._ps._get_instrument_lock(_inst_id):
                    pos_dict = self._ps.positions.get(_inst_id, {})
                    for _pid, rec in pos_dict.items():
                        if rec.volume == 0 and rec.open_price > 0:
                            if hasattr(rec, 'realized_pnl'):
                                realized_pnl += getattr(rec, 'realized_pnl', 0.0)
                        elif rec.volume != 0 and rec.open_price > 0:
                            _has_active_position = True
            # FIX-R37-REALIZED-PNL: 合并服务级累加器，覆盖已删除持仓记录的realized_pnl
            realized_pnl += getattr(self._ps, '_total_realized_pnl', 0.0)
            expected_pnl = equity - initial_capital
            if _has_active_position:
                return
            if abs(expected_pnl) > 0 and abs(realized_pnl - expected_pnl) / max(abs(expected_pnl), 1.0) > 0.005:
                logging.error(
                    "[PositionService] INV-P1-01: PnL与权益不一致! "
                    "realized_pnl=%.2f equity=%.2f initial_capital=%.2f expected_pnl=%.2f "
                    "deviation=%.2f%%",
                    realized_pnl, equity, initial_capital, expected_pnl,
                    abs(realized_pnl - expected_pnl) / max(abs(expected_pnl), 1.0) * 100,
                )
                try:
                    from ali2026v3_trading.infra.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='pnl_equity_inconsistency',
                            level='HIGH',
                            message=f"INV-P1-01: PnL与权益不一致 "
                                    f"realized_pnl={realized_pnl:.2f} expected_pnl={expected_pnl:.2f} "
                                    f"equity={equity:.2f} initial_capital={initial_capital:.2f}",
                        ), async_mode=True)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _eb_e:
                    logging.debug("[PositionService] INV-P1-01: 事件总线告警失败: %s", _eb_e)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[PositionService] INV-P1-01: PnL权益一致性校验异常: %s", e)
