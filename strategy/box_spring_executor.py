# MODULE_ID: M1-248
"""
box_spring_executor.py - 箱体波动率脉冲策略（弹簧策略）- 执行Mixin

包含交易执行、平仓、资金计算等执行相关方法。
从BoxSpringStrategy类中提取，作为Mixin使用。
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from ali2026v3_trading.infra.resilience import safe_float_to_int
from ali2026v3_trading.strategy._box_spring_types import (
    SpringState, SpringSignal, SpringPosition,
)



class BoxSpringExecutorService:

    OPEN_REASON = 'SPRING'

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        import threading
        _p = params or {}
        self._lock = threading.RLock()
        self._positions: Dict[str, Any] = {}
        self._signals: Dict[str, Any] = {}
        self._boxes: Dict[str, Any] = {}
        self._stats = {
            'positions_opened': 0,
            'positions_closed_tp': 0,
            'positions_closed_sl': 0,
            'positions_closed_expired': 0,
            'total_pnl': 0.0,
            'win_count': 0,
            'loss_count': 0,
        }
        self._max_active_positions = _p.get('max_active_positions', 3)
        self._max_loss_pct = _p.get('max_loss_pct', 0.5)
        self._max_risk_ratio = _p.get('max_risk_ratio', 0.02)
        self._option_multiplier = _p.get('option_contract_multiplier', 10000)
        self._max_position_pct = _p.get('max_position_pct', 0.1)
        self._stop_profit_ratio = _p.get('stop_profit_ratio', 1.8)
        self._fallback_strike_search = _p.get('fallback_strike_search', True)
        self._strike_distance_threshold = _p.get('strike_distance_threshold', 0.02)
        self._min_estimated_plr = _p.get('min_estimated_plr', 0.0)
        self._dynamic_tp_sl_enabled = _p.get('dynamic_tp_sl_enabled', False)
        self._capital_scale = _p.get('capital_scale', 100000.0)
        self.params = _p

    def _get_now(self):
        from datetime import datetime
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        return datetime.now(CHINA_TZ)


    # ========================================================================
    # 资金自动推算手数 + 同月低权利金行权价降级选择
    # ========================================================================

    def compute_equity_based_lots(
        self,
        premium_price: float,
        account_equity: float,
        max_loss_pct: Optional[float] = None,
        instrument_id: str = '',  # R13-P2-BIZ-01修复: 新增instrument_id参数用于查找已有持仓
    ) -> int:
        """
        根据实时资金和权利金自动推算下单手数。
        公式: lots = floor(account_equity * max_risk_ratio / (premium_price * 10000))
        其中 premium_price * 10000 为单手权利金成本(期权1手=10000张)。
        至少1手，受max_position_pct约束。

        R13-P2-BIZ-01修复: 计算lots时应减去该合约已有的持仓手数，
        避免在已有持仓的情况下重复计算导致超仓。
        """
        if premium_price <= 0 or account_equity <= 0:
            return 1
        max_loss = max_loss_pct if max_loss_pct is not None else self._max_loss_pct
        risk_budget = account_equity * self._max_risk_ratio
        cost_per_lot = premium_price * self._option_multiplier
        # R27-P2-FP-17修复: int()截断→safe_float_to_int()
        lots = max(1, safe_float_to_int(risk_budget / cost_per_lot))
        max_lots_by_position_pct = max(1, safe_float_to_int(account_equity * self._max_position_pct / cost_per_lot))
        lots = min(lots, max_lots_by_position_pct)

        # R13-P2-BIZ-01修复: 减去该合约已有的持仓手数
        if instrument_id and hasattr(self, '_positions') and self._positions:
            existing_lots = 0
            for pos in self._positions.values():
                pos_inst = getattr(pos, 'instrument_id', '') if hasattr(pos, 'instrument_id') else ''
                if pos_inst == instrument_id:
                    pos_lots = abs(getattr(pos, 'lots', 0) or getattr(pos, 'volume', 0) or 0)
                    existing_lots += pos_lots
            lots = max(0, lots - existing_lots)
            if lots <= 0:
                logging.info("[BoxSpring] R13-P2-BIZ-01: instrument=%s 已有持仓%d手, 无需额外开仓", instrument_id, existing_lots)
                return 0

        return lots

    def find_cheaper_strike_same_month(
        self,
        instrument_id: str,
        current_premium: float,
        account_equity: float,
        direction: str,
    ) -> Optional[Dict[str, Any]]:
        """
        资金不足时，在同月期权中选择权利金较小的行权价品种。
        按权利金升序排列同月同类型期权，返回能买1手的最低权利金候选。
        """
        if not self._fallback_strike_search:
            return None
        try:
            from ali2026v3_trading.data.width_cache import get_width_cache
            cache = get_width_cache()
            if cache is None:
                return None
        except (ImportError, RuntimeError):
            return None

        current_info = None
        for iid, info in cache._option_info.items():
            if iid == instrument_id or info.get('instrument_id', '') == instrument_id:
                current_info = info
                break
        if current_info is None:
            return None

        current_month = current_info.get('month', '')
        opt_type = current_info.get('option_type', 'CALL')
        underlying_fid = current_info.get('underlying_future_id', '')

        candidates = []
        for iid, info in cache._option_info.items():
            if info.get('underlying_future_id', '') != underlying_fid:
                continue
            if info.get('option_type', 'CALL') != opt_type:
                continue
            if info.get('month', '') != current_month:
                continue
            premium = info.get('last_price', 0.0)
            if premium <= 0 or premium >= current_premium:
                continue
            strike = info.get('strike_price', 0.0)
            if strike <= 0:
                continue
            cost_per_lot = premium * self._option_multiplier
            if cost_per_lot > account_equity * self._max_risk_ratio:
                continue
            distance = abs(info.get('underlying_price', 0) - strike) / max(info.get('underlying_price', 1), 1)
            if distance > self._strike_distance_threshold * 2:
                continue
            candidates.append({
                'instrument_id': info.get('instrument_id', iid),
                'strike_price': strike,
                'premium_price': premium,
                'distance': distance,
            })

        if not candidates:
            return None

        candidates.sort(key=lambda x: x['premium_price'])
        best = candidates[0]
        logging.info(
            "[BoxSpring] FALLBACK_STRIKE: %s premium=%.4f->%.4f strike=%.1f->%.1f month=%s",
            direction, current_premium, best['premium_price'],
            current_info.get('strike_price', 0), best['strike_price'], current_month,
        )
        return best

    # ========================================================================
    # 下单执行
    # ========================================================================

    def execute_spring_entry(self, signal: SpringSignal) -> Optional[str]:
        with self._lock:
            active_count = sum(1 for p in self._positions.values() if p.is_open)
            if active_count >= self._max_active_positions:
                logging.debug("[BoxSpring] Max positions reached: %d", active_count)
                return None

            if not self._check_cross_strategy_risk(signal):
                logging.warning("[BoxSpring] 跨策略风控阻断, 跳过Spring入场")
                return None

            # ✅ P1-10修复: 铁律检查——防止趋势转换
            if not self.prevent_trend_conversion(
                signal.instrument_id, 'OPEN', signal.open_reason
            ):
                logging.warning("[BoxSpring] 铁律阻断: 趋势转换被阻止 %s", signal.instrument_id)
                return None

            estimated_plr = 0.0
            if self._min_estimated_plr > 0 or self._dynamic_tp_sl_enabled:
                estimated_plr = self.estimate_plr_before_entry(signal.instrument_id)
                if self._min_estimated_plr > 0 and estimated_plr < self._min_estimated_plr:
                    logging.debug("[BoxSpring] PLR过滤: estimated_plr=%.2f < min=%.2f", estimated_plr, self._min_estimated_plr)
                    return None

            try:
                from ali2026v3_trading.order.order_service import get_order_service
                osvc = get_order_service()
                if not osvc:
                    return None

                self._record_spring_trade(signal)

                if signal.direction == 'BUY_STRADDLE':
                    return self._execute_straddle_entry(signal)

                action_map = {
                    'BUY_CALL': ('BUY', 'OPEN'),
                    'BUY_PUT': ('BUY', 'OPEN'),
                }
                direction, action = action_map.get(signal.direction, ('BUY', 'OPEN'))

                equity_lots = self.compute_equity_based_lots(
                    signal.premium_price, signal.account_equity if hasattr(signal, 'account_equity') else 100000.0,
                )
                actual_lots = min(signal.lots, equity_lots) if signal.lots > 0 else equity_lots

                if actual_lots <= 0:
                    cheaper = self.find_cheaper_strike_same_month(
                        signal.option_instrument_id, signal.premium_price,
                        signal.account_equity if hasattr(signal, 'account_equity') else 100000.0,
                        signal.direction,
                    )
                    if cheaper is None:
                        logging.warning("[BoxSpring] 资金不足且无更低权利金候选, 跳过: %s", signal.option_instrument_id)
                        return None
                    signal.option_instrument_id = cheaper['instrument_id']
                    signal.strike_price = cheaper['strike_price']
                    signal.premium_price = cheaper['premium_price']
                    actual_lots = self.compute_equity_based_lots(
                        cheaper['premium_price'],
                        signal.account_equity if hasattr(signal, 'account_equity') else 100000.0,
                    )
                    logging.info("[BoxSpring] 降级选择低权利金行权价: %s lots=%d", cheaper['instrument_id'], actual_lots)

                signal.lots = actual_lots

                order_id = osvc.send_order(
                    instrument_id=signal.option_instrument_id,
                    volume=signal.lots,
                    price=signal.premium_price,
                    direction=direction,
                    action=action,
                    open_reason=self.OPEN_REASON,
                    signal_id=getattr(signal, 'signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
                )

                if order_id:
                    # R27-P2-FP-15修复: int()截断→safe_float_to_int()
                    pos_id = f"SIG_POS_{signal.option_instrument_id}_{safe_float_to_int(time.time()*1000)}"
                    position = SpringPosition(
                        position_id=pos_id,
                        signal_id=signal.signal_id,
                        instrument_id=signal.instrument_id,
                        option_instrument_id=signal.option_instrument_id,
                        direction=signal.direction,
                        entry_premium=signal.premium_price,
                        current_premium=signal.premium_price,
                        entry_time=self._get_now(),
                        stop_profit_ratio=self._stop_profit_ratio,
                        max_loss_pct=self._max_loss_pct,
                        box_id=signal.box_id,
                        lots=signal.lots,
                    )
                    self._positions[pos_id] = position
                    if self._dynamic_tp_sl_enabled and estimated_plr > 0:
                        position.adjust_tp_sl_by_plr(estimated_plr)
                    signal.spring_state = SpringState.ACTIVE
                    signal.is_consumed = True
                    self._stats['positions_opened'] += 1

                    logging.info(
                        "[BoxSpring] ENTRY: %s dir=%s premium=%.4f stop_profit=%.1fx max_loss=%.0f%%",
                        signal.option_instrument_id, signal.direction, signal.premium_price,
                        self._stop_profit_ratio, self._max_loss_pct * 100
                    )
                    return order_id

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.error("[BoxSpring] Entry error: %s", e)

        return None

    def _execute_straddle_entry(self, signal: SpringSignal) -> Optional[str]:
        paired_instrument_id, paired_premium, signal_opt_type = self._find_straddle_pair(signal)

        if not paired_instrument_id or not signal_opt_type:
            logging.warning("[BoxSpring] STRADDLE: no pair found for %s (opt_type=%s)",
                            signal.option_instrument_id, signal_opt_type)
            return None

        is_call = signal_opt_type == 'CALL'
        call_instrument = signal.option_instrument_id if is_call else paired_instrument_id
        put_instrument = paired_instrument_id if is_call else signal.option_instrument_id
        call_premium = signal.premium_price if is_call else paired_premium
        put_premium = paired_premium if is_call else signal.premium_price

        try:
            from ali2026v3_trading.order.order_service import get_order_service
            osvc = get_order_service()
            if not osvc:
                return None
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.error("[BoxSpring] STRADDLE: get_order_service failed: %s", e)
            return None

        call_order_id = osvc.send_order(
            instrument_id=call_instrument,
            volume=signal.lots,
            price=call_premium,
            direction='BUY',
            action='OPEN',
            open_reason=self.OPEN_REASON,
            signal_id=getattr(signal, 'signal_id', ''),
        )

        if not call_order_id:
            logging.warning("[BoxSpring] STRADDLE: Call order failed for %s, aborting straddle",
                            call_instrument)
            return None

        put_order_id = osvc.send_order(
            instrument_id=put_instrument,
            volume=signal.lots,
            price=put_premium,
            direction='BUY',
            action='OPEN',
            open_reason=self.OPEN_REASON,
            signal_id=getattr(signal, 'signal_id', ''),
        )

        if not put_order_id:
            logging.warning("[BoxSpring] STRADDLE: Put order failed for %s, closing Call leg to avoid single-leg risk",
                            put_instrument)
            try:
                osvc.send_order(
                    instrument_id=call_instrument,
                    volume=signal.lots,
                    price=call_premium,
                    direction='SELL',
                    action='CLOSE',
                    open_reason=self.OPEN_REASON,
                    signal_id=getattr(signal, 'signal_id', ''),
                )
            except (ImportError, AttributeError, RuntimeError) as e:
                logging.error("[BoxSpring] STRADDLE: failed to close Call leg after Put failure: %s", e)
            return None

        total_entry_premium = call_premium + put_premium

        # R27-P2-FP-16修复: int()截断→safe_float_to_int()
        pos_id = f"SIG_POS_STRADDLE_{signal.instrument_id}_{safe_float_to_int(time.time()*1000)}"
        position = SpringPosition(
            position_id=pos_id,
            signal_id=signal.signal_id,
            instrument_id=signal.instrument_id,
            option_instrument_id=call_instrument,
            direction='BUY_STRADDLE',
            entry_premium=total_entry_premium,
            current_premium=call_premium,
            entry_time=self._get_now(),
            stop_profit_ratio=self._stop_profit_ratio,
            max_loss_pct=self._max_loss_pct,
            box_id=signal.box_id,
            paired_instrument_id=put_instrument,
            paired_current_premium=put_premium,
        )
        with self._lock:
            self._positions[pos_id] = position
            estimated_plr = self.estimate_plr_before_entry(signal.instrument_id) if hasattr(self, 'estimate_plr_before_entry') else 0.0
            if self._dynamic_tp_sl_enabled and estimated_plr > 0:
                position.adjust_tp_sl_by_plr(estimated_plr)
            signal.is_consumed = True
            self._stats['positions_opened'] += 1

        logging.info(
            "[BoxSpring] STRADDLE ENTRY: call=%s(oid=%s) put=%s(oid=%s) "
            "call_prem=%.4f put_prem=%.4f total=%.4f stop_profit=%.1fx max_loss=%.0f%%",
            call_instrument, call_order_id, put_instrument, put_order_id,
            call_premium, put_premium, total_entry_premium,
            self._stop_profit_ratio, self._max_loss_pct * 100
        )

        return call_order_id

    # ========================================================================
    # 平仓纪律：弹簧松开即走 / 接受归零
    # ========================================================================

    def on_premium_update(self, option_instrument_id: str, current_premium: float) -> Optional[Dict[str, Any]]:
        if current_premium <= 0:
            return None

        with self._lock:
            open_positions = [
                p for p in self._positions.values()
                if (p.option_instrument_id == option_instrument_id or
                    p.paired_instrument_id == option_instrument_id) and p.is_open
            ]

        if not open_positions:
            return None

        pos = open_positions[0]

        if pos.direction == 'BUY_STRADDLE' and pos.paired_instrument_id:
            if option_instrument_id == pos.paired_instrument_id:
                pos.paired_current_premium = current_premium
            else:
                pos.current_premium = current_premium
            total_premium = pos.current_premium + pos.paired_current_premium
            if total_premium > pos.peak_premium:
                pos.peak_premium = total_premium
                pos.peak_time = self._get_now()

            close_action = self._evaluate_close_straddle(pos)
            if close_action:
                pos.current_premium = total_premium
                return self._execute_close(pos, close_action)
            return None

        pos.current_premium = current_premium

        if current_premium > pos.peak_premium:
            pos.peak_premium = current_premium
            pos.peak_time = self._get_now()

        close_action = self._evaluate_close(pos)
        if close_action:
            return self._execute_close(pos, close_action)

        return None

    def _evaluate_close_straddle(self, pos: SpringPosition) -> Optional[str]:
        total_premium = pos.current_premium + pos.paired_current_premium
        if pos.entry_premium <= 0:
            return 'STOP_LOSS'
        pnl_ratio = total_premium / pos.entry_premium
        pnl_ratio = max(min(pnl_ratio, 100.0), -100.0)  # NP-P2-11: pnl_ratio溢出clip
        if pnl_ratio >= pos.stop_profit_ratio:
            return 'TAKE_PROFIT'
        loss_pct = 1.0 - (total_premium / pos.entry_premium)
        if loss_pct >= pos.max_loss_pct:
            return 'STOP_LOSS'
        hold_minutes = (self._get_now() - pos.entry_time).total_seconds() / 60.0
        max_hold = self.params.get('max_spring_hold_minutes', 120)
        if hold_minutes > max_hold:
            return 'TIME_EXPIRE'
        box = self._boxes.get(pos.instrument_id)
        if box and not box.is_active:
            return 'BOX_BROKEN'
        return None

    def _evaluate_close(self, pos: SpringPosition) -> Optional[str]:
        if pos.should_take_profit:
            return 'TAKE_PROFIT'

        if pos.should_accept_loss:
            return 'STOP_LOSS'

        hold_minutes = (self._get_now() - pos.entry_time).total_seconds() / 60.0
        max_hold = self.params.get('max_spring_hold_minutes', 120)
        if hold_minutes > max_hold:
            return 'TIME_EXPIRE'

        box = self._boxes.get(pos.instrument_id)
        if box and not box.is_active:
            return 'BOX_BROKEN'

        return None

    def _execute_close(self, pos: SpringPosition, reason: str) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                _CLOSE_DIRECTION_MAP = {
                    'BUY_CALL': 'SELL', 'BUY_PUT': 'SELL', 'BUY_STRADDLE': 'SELL',
                    'SELL_CALL': 'BUY', 'SELL_PUT': 'BUY',
                }
                close_direction = _CLOSE_DIRECTION_MAP.get(pos.direction, 'SELL')
                close_lots = pos.lots if hasattr(pos, 'lots') and pos.lots > 0 else 1
                osvc.send_order(
                    instrument_id=pos.option_instrument_id,
                    volume=close_lots,
                    price=pos.current_premium,
                    direction=close_direction,
                    action='CLOSE',
                    signal_id=getattr(pos, 'signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
                )
                if pos.direction == 'BUY_STRADDLE' and pos.paired_instrument_id:
                    paired_close_dir = _CLOSE_DIRECTION_MAP.get(pos.direction, 'SELL')
                    osvc.send_order(
                        instrument_id=pos.paired_instrument_id,
                        volume=close_lots,
                        price=pos.paired_current_premium,
                        direction=paired_close_dir,
                        action='CLOSE',
                        signal_id=getattr(pos, 'signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
                    )
                osvc.persist_close_event(
                    order_id=pos.position_id,
                    close_reason=f'SIG_{reason}',
                    pnl=(pos.current_premium + pos.paired_current_premium) - pos.entry_premium if pos.direction == 'BUY_STRADDLE' else pos.current_premium - pos.entry_premium,
                )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.error("[BoxSpring] Close error: %s", e)

        pos.is_open = False
        if reason == 'TIME_EXPIRE':
            sig = self._signals.get(pos.signal_id)
            if sig:
                sig.spring_state = SpringState.EXPIRED
        pnl = (pos.current_premium + pos.paired_current_premium) - pos.entry_premium if pos.direction == 'BUY_STRADDLE' else pos.current_premium - pos.entry_premium

        with self._lock:
            self._stats['total_pnl'] += pnl
            if reason == 'TAKE_PROFIT':
                self._stats['positions_closed_tp'] += 1
                self._stats['win_count'] += 1
            elif reason == 'STOP_LOSS':
                self._stats['positions_closed_sl'] += 1
                self._stats['loss_count'] += 1
            else:
                self._stats['positions_closed_expired'] += 1
                self._stats['loss_count'] += 1

        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            rs.record_trade_result('spring', pnl, self._capital_scale)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[BoxSpring] record_trade_result failed: %s", e)

        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            _close_lots = pos.lots if hasattr(pos, 'lots') and pos.lots > 0 else 1
            _est_commission = _close_lots * 3.0
            _est_slippage = abs(pnl) * 3.0 / 10000 if pnl != 0 else 0.0
            eco.record_strategy_pnl('spring', pnl, commission=_est_commission, slippage=_est_slippage)
            eco.update_plr_stats('spring')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[BoxSpring] record_strategy_pnl/update_plr_stats failed: %s", e)

        logging.info(
            "[BoxSpring] CLOSE: %s reason=%s entry=%.4f exit=%.4f pnl=%.4f ratio=%.2f%%",
            pos.option_instrument_id, reason, pos.entry_premium, pos.current_premium,
            pnl, pos.pnl_ratio * 100  # NP-P2-02: pnl_ratio显示加*100和%%
        )

        return {
            'position_id': pos.position_id,
            'reason': reason,
            'entry_premium': pos.entry_premium,
            'exit_premium': pos.current_premium,
            'pnl': pnl,
            'pnl_ratio': pos.pnl_ratio,
            'peak_premium': pos.peak_premium,
        }

    def _compute_hedge_ratio(self, instrument_id: str = '') -> float:
        """计算delta中性对冲比率

        使用greeks_calculator获取当前持仓的net_delta，
        计算需要多少期货手数来对冲至delta中性。

        Returns:
            float: 对冲比率（需要买入/卖出的期货手数，正=买入，负=卖出）
        """
        try:
            from ali2026v3_trading.position.position_service import (
                aggregate_greeks_exposure,
                get_position_service,
            )
            pos_svc = get_position_service()
            if pos_svc is None:
                return 0.0
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            net_delta = exposure.net_delta
            if abs(net_delta) < 1e-6:
                return 0.0
            hedge_lots = -net_delta
            logging.info(
                "[BoxSpring] R13-P0-BIZ-06: 对冲比率计算 net_delta=%.4f hedge_lots=%.2f",
                net_delta, hedge_lots,
            )
            return hedge_lots
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[BoxSpring] _compute_hedge_ratio failed: %s", e)
            return 0.0

    def _check_cross_strategy_risk(self, signal: SpringSignal) -> bool:
        try:
            from ali2026v3_trading.position.position_service import (
                aggregate_greeks_exposure,
                get_cross_strategy_risk_guard,
                get_position_service,
            )
            guard = get_cross_strategy_risk_guard()
            pos_svc = get_position_service()
            if pos_svc is None:
                logging.warning("[BoxSpring._check_cross_strategy_risk] PositionService unavailable, fail-safe阻断")
                return False
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            level, reason, detail = guard.check(exposure)
            if level in (guard.BLOCK, guard.CIRCUIT_BREAK):
                return False
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            import logging
            logging.warning("[BoxSpring._check_cross_strategy_risk] Error: %s, fail-safe阻断", e)
            return False

    def _record_spring_trade(self, signal: SpringSignal):
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            _spring_pnl = getattr(signal, 'pnl', 0.0) if hasattr(signal, 'pnl') else 0.0
            eco.record_spring_trade(signal.direction, pnl=_spring_pnl)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[BoxSpring] _record_spring_trade failed: %s", e)

    def _find_straddle_pair(self, signal: SpringSignal) -> Tuple[str, float, str]:
        try:
            from ali2026v3_trading.data.t_type_service import get_t_type_service
            t_type = get_t_type_service()
            try:
                if not t_type or not t_type._width_cache:
                    return '', 0.0, ''
                cache = t_type._width_cache
            except AttributeError as e:
                logging.warning(
                    "[BoxSpring] _find_straddle_pair: failed to access t_type._width_cache "
                    "(internal API may have changed): %s", e
                )
                return '', 0.0, ''

            strike = signal.strike_price

            signal_opt_type = ''
            try:
                with cache._lock:
                    for iid, info in cache._option_info.items():
                        if info.get('instrument_id') == signal.option_instrument_id:
                            signal_opt_type = info.get('option_type', '')
                            break
            except AttributeError as e:
                logging.warning(
                    "[BoxSpring] _find_straddle_pair: failed to access cache._lock or cache._option_info "
                    "(internal API may have changed): %s", e
                )
                return '', 0.0, ''

            if not signal_opt_type:
                return '', 0.0, ''

            target_type = 'PUT' if signal_opt_type == 'CALL' else 'CALL'

            try:
                with cache._lock:
                    for iid, info in cache._option_info.items():
                        if info.get('underlying_future_id') != signal.instrument_id:
                            continue
                        if info.get('strike_price') != strike:
                            continue
                        if info.get('option_type') != target_type:
                            continue
                        inst_id = info.get('instrument_id', '')
                        if not inst_id or inst_id == signal.option_instrument_id:
                            continue

                        premium = 0.0
                        try:
                            from ali2026v3_trading.data.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                premium = ds.realtime_cache.get_latest_price(inst_id) or 0.0
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _ds_err:
                            logging.debug("[R22-EP-04] data_service获取premium失败: %s", _ds_err)
                        if premium <= 0:
                            premium = signal.premium_price

                        return inst_id, premium, signal_opt_type
            except AttributeError as e:
                logging.warning(
                    "[BoxSpring] _find_straddle_pair: failed to access cache._lock or cache._option_info "
                    "(internal API may have changed): %s", e
                )
                return '', 0.0, ''
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[BoxSpring] _find_straddle_pair error: %s", e)

        return '', 0.0, ''

BoxSpringExecutorMixin = BoxSpringExecutorService
