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

from infra.resilience import safe_float_to_int
from strategy.box_spring_detector import (
    SpringState, SpringSignal, SpringPosition,
)
def _compute_hedge_ratio(self_or_signal, signal=None) -> Optional[float]:
    """V4-FIX-O15: 对冲比率计算(上策增强版)

    STRADDLE（买入同行权价Call+Put）: 天然delta中性, hedge_ratio=1.0
    非STRADDLE: 基于delta/underlying_price计算, delta不可用时fail-closed返回None
    """
    _signal_type = ''
    _delta = None
    _underlying_price = 0.0
    try:
        _sig = signal if signal is not None else self_or_signal
        _signal_type = getattr(_sig, 'signal_type', '') or ''
        _delta = getattr(_sig, 'delta', None)
        _underlying_price = getattr(_sig, 'underlying_price', 0.0) or getattr(_sig, 'future_price', 0.0)
    except Exception:
        pass
    # STRADDLE: 天然delta中性, hedge_ratio=1.0语义正确
    if 'STRADDLE' in _signal_type.upper() or _signal_type == '':
        return 1.0
    # 非STRADDLE: 需要delta数据
    if _delta is None or _underlying_price <= 0:
        logging.warning("[V4-FIX-O15] 非STRADDLE场景delta/underlying_price不可用, 返回None (fail-closed)")
        return None
    hedge_ratio = abs(_delta) / _underlying_price
    return min(max(hedge_ratio, 0.0), 2.0)  # 合理范围[0, 2]


def _check_cross_strategy_risk(self_or_signal, signal=None) -> bool:
    """V4-FIX-O14: 跨策略风控检查（fail-closed）

    原则: 数据不可用=不开仓（return False）
    检查维度: S4净敞口（数据可用时检查，不可用时fail-closed阻断）
    """
    try:
        from risk.risk_service import get_risk_service
        rs = get_risk_service()
        # 检查S4净敞口
        _net_exposure = None
        if hasattr(rs, 'get_strategy_net_exposure'):
            _net_exposure = rs.get_strategy_net_exposure('s4_spring')
        if _net_exposure is None:
            # 数据不可用=fail-closed阻断
            logging.warning("[S4-V4-FIX-O14] 跨策略净敞口数据不可用, fail-closed阻断")
            return False
        # 检查净敞口是否超限
        _max_exposure = getattr(rs, 'get_strategy_max_exposure', lambda s: 10.0)('s4_spring') if hasattr(rs, 'get_strategy_max_exposure') else 10.0
        if abs(_net_exposure) > _max_exposure:
            logging.warning("[S4-V4-FIX-O14] S4净敞口超限: exposure=%.2f max=%.2f", _net_exposure, _max_exposure)
            return False
        return True
    except Exception as _csre:
        # 异常=fail-closed阻断
        logging.warning("[S4-V4-FIX-O14] 跨策略风控检查异常, fail-closed阻断: %s", _csre)
        return False


def _record_spring_trade(self_or_signal, signal=None) -> Dict[str, Any]:
    actual_signal = signal if signal is not None else self_or_signal
    # FIX-20260718-SANDBOX-E2E: 兼容 SpringSignal dataclass（原仅支持 dict）
    # 根因: execute_spring_entry L230 传入 SpringSignal dataclass，调用 .get() 导致
    #       AttributeError 被 L309 except 捕获后返回 None，S3/S4 模拟下单为 0
    if isinstance(actual_signal, dict):
        sig_id = actual_signal.get("signal_id", "unknown")
    else:
        sig_id = getattr(actual_signal, "signal_id", "unknown")
    return {"recorded": True, "signal_id": sig_id}





class BoxSpringExecutorService:

    OPEN_REASON = 'BOX_SPRING'  # [FIX-20260712-S4-P0] 原'SPRING'不在_REASON_STRATEGY_MAP中，_resolve_strategy_group回退到's1_hft'→S4持仓错误映射到高频策略组(5min→60s)，改为'BOX_SPRING'使映射到's4_spring'策略组(5min持仓)

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
        from infra.shared_utils import CHINA_TZ
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
        risk_budget = account_equity * min(self._max_risk_ratio, max_loss)  # FIX-A5-20260805: max_loss参与计算
        cost_per_lot = premium_price * self._option_multiplier
        # R27-P2-FP-17修复: int()截断→safe_float_to_int()
        lots = max(1, safe_float_to_int(risk_budget / cost_per_lot))
        max_lots_by_position_pct = max(1, safe_float_to_int(account_equity * self._max_position_pct / cost_per_lot))
        lots = min(lots, max_lots_by_position_pct)

        # R13-P2-BIZ-01修复: 减去该合约已有的持仓手数
        if instrument_id and hasattr(self, '_positions') and self._positions:
            existing_lots = 0
            for pos in self._positions.values():
                pos_inst = getattr(pos, 'option_instrument_id', '') if hasattr(pos, 'option_instrument_id') else ''
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
            from data.width_cache import get_width_cache
            cache = get_width_cache()
            if cache is None:
                return None
        except Exception:  # FIX-S4-EXCEPT-20260721: 窄异常→Exception(实时回调路径硬约束)
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
        # [DIAG-S4-EXEC-20260721] 诊断日志：定位execute_spring_entry返回None的根因
        _diag_exec_count = getattr(self, '_diag_exec_count', 0) + 1
        self._diag_exec_count = _diag_exec_count
        if _diag_exec_count <= 10 or _diag_exec_count % 100 == 0:
            logging.info("[DIAG-S4-EXEC] execute_spring_entry #%d: opt=%s prem=%.4f dir=%s state=%s",
                         _diag_exec_count, signal.option_instrument_id, signal.premium_price,
                         signal.direction, signal.spring_state)
        with self._lock:
            active_count = sum(1 for p in self._positions.values() if p.is_open)
            if active_count >= self._max_active_positions:
                logging.info("[DIAG-S4-EXEC] REJECT: max_positions=%d limit=%d", active_count, self._max_active_positions)
                return None

            if not self._check_cross_strategy_risk(signal):
                logging.info("[DIAG-S4-EXEC] REJECT: cross_strategy_risk")
                logging.warning("[BoxSpring] 跨策略风控阻断, 跳过Spring入场")
                return None

            # ✅ P1-10修复: 铁律检查——防止趋势转换
            # FIX-S4-ROOT-20260720: signal.instrument_id现在是期货合约ID(如IF2607),
            # 但is_spring_position匹配的是option_instrument_id(如IO2607C4000)
            # 必须传option_instrument_id才能正确匹配
            if not self.prevent_trend_conversion(
                signal.option_instrument_id, 'OPEN', signal.open_reason
            ):
                logging.info("[DIAG-S4-EXEC] REJECT: trend_conversion_blocked opt=%s", signal.option_instrument_id)
                logging.warning("[BoxSpring] 铁律阻断: 趋势转换被阻止 %s", signal.option_instrument_id)
                return None

            estimated_plr = 0.0
            if self._min_estimated_plr > 0 or self._dynamic_tp_sl_enabled:
                estimated_plr = self.estimate_plr_before_entry(signal.instrument_id)
                if self._min_estimated_plr > 0 and estimated_plr < self._min_estimated_plr:
                    logging.info("[DIAG-S4-EXEC] REJECT: plr=%.2f < min=%.2f", estimated_plr, self._min_estimated_plr)
                    return None

            try:
                from order.order_service import get_order_service
                osvc = get_order_service()
                if not osvc:
                    logging.info("[DIAG-S4-EXEC] REJECT: order_service is None")
                    return None

                # FIX-S4-PREM-EXEC-20260721: premium_price<=0时send_order被L754拒绝
                # 根因: premium_price=0→send_order(price=0)→order_executor_platform L754 `if price<=0: continue`
                # FIX-S4-PREM已在targets构建时用future_price*0.005兜底,此处作最后防线
                if signal.premium_price <= 0:
                    logging.info("[DIAG-S4-EXEC] REJECT: premium_price=%.4f<=0 (L754会拒绝)", signal.premium_price)
                    return None

                self._record_spring_trade(signal)


                action_map = {
                    'BUY_CALL': ('BUY', 'OPEN'),
                    'BUY_PUT': ('BUY', 'OPEN'),
                }
                direction, action = action_map.get(signal.direction, ('BUY', 'OPEN'))

                equity_lots = self.compute_equity_based_lots(
                    signal.premium_price, signal.account_equity if hasattr(signal, 'account_equity') else 0.0,  # FIX-FAKE-EQUITY-20260731: 100000.0→0.0(fail-closed)
                    instrument_id=signal.option_instrument_id,  # FIX-A2-20260805: 传入instrument_id避免超仓
                )
                actual_lots = min(signal.lots, equity_lots) if signal.lots > 0 else equity_lots

                if actual_lots <= 0:
                    cheaper = self.find_cheaper_strike_same_month(
                        signal.option_instrument_id, signal.premium_price,
                        signal.account_equity if hasattr(signal, 'account_equity') else 0.0,  # FIX-FAKE-EQUITY-20260731: 100000.0→0.0(fail-closed)
                        signal.direction,
                    )
                    if cheaper is None:
                        logging.info("[DIAG-S4-EXEC] REJECT: lots=0且无更低价候选 opt=%s prem=%.4f", signal.option_instrument_id, signal.premium_price)
                        logging.warning("[BoxSpring] 资金不足且无更低权利金候选, 跳过: %s", signal.option_instrument_id)
                        return None
                    signal.option_instrument_id = cheaper['instrument_id']
                    signal.strike_price = cheaper['strike_price']
                    signal.premium_price = cheaper['premium_price']
                    actual_lots = self.compute_equity_based_lots(
                        cheaper['premium_price'],
                        signal.account_equity if hasattr(signal, 'account_equity') else 0.0,  # FIX-FAKE-EQUITY-20260731: 100000.0→0.0(fail-closed)
                        instrument_id=cheaper['instrument_id'],  # FIX-A2-20260805: 传入instrument_id避免超仓
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
                    # FIX-R37-UNIQUE-ID: 增加随机熵，避免同毫秒同合约pos_id冲突导致持仓覆盖
                    from infra.shared_utils import generate_prefixed_id as _gen_id
                    pos_id = f"SIG_POS_{signal.option_instrument_id}_{safe_float_to_int(time.time()*1000)}_{_gen_id('', 8)}"
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

            except Exception as e:  # FIX-S4-EXCEPT-20260721: 窄异常元组→Exception(实时回调路径硬约束)
                # 原except (ValueError,KeyError,TypeError,RuntimeError,AttributeError,ImportError)
                # 遗漏ZeroDivisionError/OverflowError/OSError/RecursionError等→穿透到C++平台
                logging.error("[BoxSpring] Entry error: %s", e)

        return None


    # ========================================================================
    # 平仓纪律：弹簧松开即走 / 接受归零
    # ========================================================================

    def on_premium_update(self, option_instrument_id: str, current_premium: float) -> Optional[Dict[str, Any]]:
        if current_premium <= 0:
            return None

        with self._lock:
            open_positions = [
                p for p in self._positions.values()
                if p.option_instrument_id == option_instrument_id and p.is_open
            ]

        if not open_positions:
            return None

        pos = open_positions[0]

        pos.current_premium = current_premium

        if current_premium > pos.peak_premium:
            pos.peak_premium = current_premium
            pos.peak_time = self._get_now()

        close_action = self._evaluate_close(pos)
        if close_action:
            return self._execute_close(pos, close_action)

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
            from order.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                _CLOSE_DIRECTION_MAP = {
                    'BUY_CALL': 'SELL', 'BUY_PUT': 'SELL',
                    'SELL_CALL': 'BUY', 'SELL_PUT': 'BUY',
                }
                close_direction = _CLOSE_DIRECTION_MAP.get(pos.direction, 'SELL')
                close_lots = pos.lots if hasattr(pos, 'lots') and pos.lots > 0 else 1
                # FIX-R37-UNIQUE-CLOSE(A5): _execute_close 必须设置 PositionService 持仓 _closing 标志，
                # 否则止盈止损/时间止损检查时 _closing=False 会重复触发平仓，导致双重平仓。
                # box_spring 有自己的 _positions(SpringPosition)，但 PositionService.positions
                # 也可能有对应持仓(通过 instrument_id 关联)，必须同步设置 _closing。
                # FIX-OPEN-UNIQUE(P1/C1): 策略层并行持仓追踪与 PositionService.positions 不同步，
                # 且 pos_id 格式不一致(SIG_POS_ vs instrument_group_)。
                # 通过 signal_id 精确匹配 PositionService 持仓，signal_id 是贯通两层的唯一关联键。
                _pos_svc = None
                try:
                    from position.position_service import get_position_service
                    _pos_svc = get_position_service()
                    _spring_sig_id = getattr(pos, 'signal_id', '')
                    if _pos_svc:
                        with _pos_svc._get_instrument_lock(pos.option_instrument_id):
                            _matched = False
                            for _rec in _pos_svc.positions.get(pos.option_instrument_id, {}).values():
                                if not getattr(_rec, '_closing', False):
                                    # P1/C1: 优先通过 signal_id 精确匹配
                                    if _spring_sig_id and getattr(_rec, 'signal_id', '') == _spring_sig_id:
                                        _matched = True
                                    # 无 signal_id 时回退到 instrument_id 匹配
                                    elif not _spring_sig_id:
                                        _matched = True
                                    if _matched:
                                        _rec._closing = True
                                        _rec.closing_order_id = f"PENDING_SPRING_{_rec.position_id}"
                                        _rec.close_method = f'spring_{reason.lower()}'
                                        _rec.close_reason = f'SPRING_{reason}'
                                        break
                except Exception as _a5_err:  # FIX-S4-EXCEPT-20260721: 窄异常→Exception
                    logging.debug("[R37-UNIQUE-CLOSE] A5设置_closing失败: %s", _a5_err)
                osvc.send_order(
                    instrument_id=pos.option_instrument_id,
                    volume=close_lots,
                    price=pos.current_premium,
                    direction=close_direction,
                    action='CLOSE',
                    signal_id=getattr(pos, 'signal_id', ''),
                    ref_price=pos.current_premium,
                )
                osvc.persist_close_event(
                    order_id=pos.position_id,
                    close_reason=f'SIG_{reason}',
                    pnl=pos.current_premium - pos.entry_premium,
                )
        except Exception as e:  # FIX-S4-EXCEPT-20260721: 窄异常→Exception
            logging.error("[BoxSpring] Close error: %s", e)
            _close_success = False
        else:
            _close_success = True

        # V4-FIX-C5: is_open仅在send_order成功后设置False
        # 原则: send_order失败时保留is_open=True供后续重试，而非无条件标记已平仓
        if _close_success:
            pos.is_open = False
        else:
            logging.error("[V4-FIX-C5] 平仓send_order失败, 保留is_open=True供重试: pos_id=%s",
                          getattr(pos, 'position_id', ''))
        if reason == 'TIME_EXPIRE':
            sig = self._signals.get(pos.signal_id)
            if sig:
                sig.spring_state = SpringState.EXPIRED
        pnl = pos.current_premium - pos.entry_premium

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
            from risk.risk_service import get_risk_service
            rs = get_risk_service()
            rs.record_trade_result('s4_spring', pnl, self._capital_scale)
        except Exception as e:  # FIX-S4-EXCEPT-20260721: 窄异常→Exception
            logging.debug("[BoxSpring] record_trade_result failed: %s", e)

        try:
            from strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            _close_lots = pos.lots if hasattr(pos, 'lots') and pos.lots > 0 else 1
            _est_commission = _close_lots * 3.0
            _est_slippage = abs(pnl) * 3.0 / 10000 if pnl != 0 else 0.0
            eco.record_strategy_pnl('s4_spring', pnl, commission=_est_commission, slippage=_est_slippage)
            eco.update_plr_stats('s4_spring')
        except Exception as e:  # FIX-S4-EXCEPT-20260721: 窄异常→Exception
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

    _compute_hedge_ratio = _compute_hedge_ratio
    _check_cross_strategy_risk = _check_cross_strategy_risk
    _record_spring_trade = _record_spring_trade

BoxSpringExecutorMixin = BoxSpringExecutorService
