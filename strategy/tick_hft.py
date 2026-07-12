# MODULE_ID: M1-277
"""HFT引擎与追仓/金字塔策略 - 合并自tick_hft_dispatch.py和strategy_tick_handler.py (2026-06-12)"""

import logging
import math
import time
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.config import config_params
from ali2026v3_trading.strategy_judgment.causal_chain_utils import (
    CausalChainTracker, ContaminationGuard, CyclicDependencyGuard,
    validate_tick_cascade, CausalEvent,
)

# R27-P1修复: 导入容错/浮点工具
from ali2026v3_trading.infra.resilience import (
    TimeoutGuard, Watchdog, HeartbeatMonitor,
    stable_sum, stable_mean, stable_variance,
    approx_equal, approx_less, approx_greater,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, PRICE_TOLERANCE as _RESILIENCE_TOLERANCE,
    get_signal_lifecycle, SignalLifecycleManager,
    deterministic_round, safe_float_to_int,
)

# R27-P0-FP-01修复: 浮点容差常量，止盈止损比较使用
_PRICE_TOLERANCE = 1e-6

__all__ = [
    # from tick_hft_dispatch
    'dispatch_hft_tick',
    'execute_pursuit_exit',
    'execute_pursuit_entry',
    'execute_pursuit_add',
    'handle_arbitrage_signal',
    'handle_transition_signal',
    'handle_smart_money_signal',
    'handle_filtered_signal',
    'get_last_arbitrage_signal',
    # from strategy_tick_handler
    'TickHandlerMixin',
    'DynamicPursuitEngine',
    'PursuitPosition',
    'MarketEvent',
    'TickEvent',
    'BarCompletedEvent',
    'TickProcessingService',
]

logger = get_logger(__name__)  # R9-5


# ============================================================================
# HFT分发函数 (原 tick_hft_dispatch.py)
# ============================================================================

def dispatch_hft_tick(svc, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    try:
        hft = svc._state_store.get_ref('_hft_engine') if svc._state_store else None
        if hft is None:
            if svc._ensure_hft_engine_fn is not None:
                try:
                    svc._ensure_hft_engine_fn()
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] _ensure_hft_engine_fn failed: %s", _r3_err)
                    pass
                hft = svc._state_store.get_ref('_hft_engine') if svc._state_store else None

        # FIX-P0-25: 原width_resonance计算被耦合在if hft is not None:块内
        # 导致HFT引擎未初始化时get_width_strength()从不被调用 → query_count=0
        # width_resonance用于StateParamManager.update_market_context，与HFT引擎无关
        # 应独立执行，确保宽度强度计算和状态参数更新不受HFT可用性影响
        bid_price = svc._get_tick_field(tick, 'bid_price1', 0.0)
        ask_price = svc._get_tick_field(tick, 'ask_price1', 0.0)
        direction_raw = ''
        try:
            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
            if SubscriptionManager.is_option(instrument_id):
                direction_raw = 'buy'
            else:
                rc = None
                ds = svc._state_store.get_ref('_data_service') if svc._state_store else None
                if ds:
                    rc = getattr(ds, 'realtime_cache', None)
                if rc:
                    tick_data = rc.get_tick(instrument_id)
                    if tick_data:
                        direction_raw = tick_data.get('direction', '')
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            direction_raw = ''

        product = ''
        try:
            from ali2026v3_trading.infra.shared_utils import extract_product_code
            product = extract_product_code(instrument_id)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] extract_product_code failed: %s", _r3_err)
            pass

        width_resonance = 0.0
        try:
            ps = None
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                ps = get_params_service()
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] get_params_service failed: %s", _r3_err)
                pass
            if ps:
                meta = ps.get_instrument_meta_by_id(instrument_id)
                if meta:
                    uf_id = meta.get('underlying_future_id')
                    if not uf_id:
                        uf_id = meta.get('internal_id')
                    if uf_id:
                        tts = svc._state_store.get_ref('t_type_service') if svc._state_store else None
                        if tts is None:
                            try:
                                from ali2026v3_trading.data.t_type_service import get_t_type_service
                                tts = get_t_type_service()
                            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                                logging.debug("[R3-L2] get_t_type_service failed: %s", _r3_err)
                                pass
                        wc = getattr(tts, '_width_cache', None) if tts else None
                        if wc:
                            ws_method = getattr(wc, 'get_width_strength', None)
                            get_months_method = getattr(wc, 'get_all_months', None)
                            if ws_method and get_months_method:
                                months = get_months_method(int(uf_id))
                                if months:
                                    ws = ws_method(int(uf_id), months)
                                    width_resonance = min(ws / 10.0, 1.0) if ws > 0 else 0.0
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] width_resonance computation failed: %s", _r3_err)
            pass

        resonance_strength = 0.0
        prev_resonance_strength = 0.0
        try:
            spm = svc._state_store.get_ref('_state_param_manager') if svc._state_store else None
            if spm:
                spm.update_market_context(width_resonance, last_price)
                resonance_strength = getattr(spm, '_last_resonance_strength', 0.0) or 0.0
                prev_resonance_strength = getattr(spm, '_prev_resonance_strength', 0.0) or 0.0
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] spm resonance_strength failed: %s", _r3_err)
            pass

        current_state = 'other'
        prev_state = 'other'
        try:
            spm2 = svc._state_store.get_ref('_state_param_manager') if svc._state_store else None
            if spm2:
                current_state = spm2.get_current_state()
                prev_state = getattr(spm2, '_prev_state', 'other')
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] spm2 current_state failed: %s", _r3_err)
            pass

        if hft is not None:
            hft_result = hft.on_tick_enhanced(
                instrument_id=instrument_id, price=last_price, volume=volume,
                direction=direction_raw, product=product,
                bid_price=bid_price, ask_price=ask_price,
                resonance_strength=resonance_strength,
                prev_resonance_strength=prev_resonance_strength,
                current_state=current_state, prev_state=prev_state,
            )

            if hft_result:
                pursuit_signal = hft_result.get('pursuit_signal')
                if pursuit_signal:
                    action = pursuit_signal.get('action', '')
                    if action == 'OPEN_POSITION':
                        execute_pursuit_entry(svc, hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)
                    elif action == 'ADD_POSITION':
                        execute_pursuit_add(svc, hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)

                pursuit_exit = hft_result.get('pursuit_exit')
                if pursuit_exit:
                    logging.info("[HFT] pursuit exit: %s %s reason=%s pnl=%.2f",
                                 pursuit_exit.get('instrument_id', ''),
                                 pursuit_exit.get('direction', ''),
                                 pursuit_exit.get('reason', ''),
                                 pursuit_exit.get('pnl', 0.0))
                    try:
                        execute_pursuit_exit(svc, hft, pursuit_exit, instrument_id)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as ex_e:
                        logging.debug("[_dispatch_hft_tick] HFT pursuit exit execution error: %s", ex_e)

                arbitrage_signal = hft_result.get('arbitrage_signal')
                if arbitrage_signal:
                    handle_arbitrage_signal(svc, arbitrage_signal, instrument_id)

                transition_signal = hft_result.get('transition_signal')
                if transition_signal:
                    hft_mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else last_price
                    handle_transition_signal(svc, transition_signal, instrument_id, last_price, mid_price=hft_mid_price)

                smart_money_signal = hft_result.get('smart_money_signal')
                if smart_money_signal:
                    handle_smart_money_signal(svc, smart_money_signal, instrument_id)

                signal_filter_result = hft_result.get('signal_filter')
                if signal_filter_result and signal_filter_result.get('threshold_crossed'):
                    handle_filtered_signal(signal_filter_result, instrument_id, last_price)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as hft_e:
        svc._hft_dispatch_error_count = getattr(svc, '_hft_dispatch_error_count', 0) + 1
        if svc._hft_dispatch_error_count <= 10 or svc._hft_dispatch_error_count % 100 == 0:
            logging.warning("[R16-P1-8.2] HFT engine分发异常(累计%d次): %s",
                           svc._hft_dispatch_error_count, hft_e)


def execute_pursuit_exit(svc, hft: Any, exit_signal: Dict[str, Any], instrument_id: str) -> None:
    direction = exit_signal.get('direction', '')
    volume = exit_signal.get('volume', 0)
    price = exit_signal.get('price', 0.0)
    reason = exit_signal.get('reason', '')
    platform_order_ids = exit_signal.get('platform_order_ids', [])
    if not instrument_id or volume <= 0:
        return
    pe = hft.pursuit_engine
    with pe._lock:
        pos = pe._positions.get(instrument_id)
        if pos and not pos.platform_confirmed and platform_order_ids:
            pos.platform_order_ids = platform_order_ids
            pos.platform_confirmed = bool(platform_order_ids)
            _os = svc._state_store.get_ref('_order_service') if svc._state_store else None
            if _os and hasattr(_os, 'get_order_by_platform_id'):
                for _poid in platform_order_ids:
                    _found = _os.get_order_by_platform_id(_poid)
                    if _found:
                        logging.debug("[R16-P2-6.1] pursuit exit: platform_order_id=%s matched internal order", _poid)
        elif pos and not pos.platform_confirmed and not platform_order_ids:
            pe._positions.pop(instrument_id, None)
            logging.warning("[HFT] pursuit exit: %s was never opened on platform, cleaned up", instrument_id)
            return
    close_signal_type = 'CLOSE_LONG' if direction == 'SELL' else 'CLOSE_SHORT'
    # FIX-R37-UNIQUE-CLOSE(A6): execute_pursuit_exit 必须设置 PositionService 持仓 _closing 标志，
    # 否则止盈止损/时间止损检查时 _closing=False 会重复触发平仓，导致双重平仓。
    # pursuit_engine 有自己的 _positions(PursuitPosition)，但 PositionService.positions
    # 也可能有对应持仓(通过 instrument_id 关联)，必须同步设置 _closing。
    try:
        from ali2026v3_trading.position.position_service import get_position_service
        _pos_svc = get_position_service()
        if _pos_svc:
            with _pos_svc._get_instrument_lock(instrument_id):
                for _rec in _pos_svc.positions.get(instrument_id, {}).values():
                    if not getattr(_rec, '_closing', False):
                        _rec._closing = True
                        _rec.closing_order_id = f"PENDING_PURSUIT_{_rec.position_id}"
                        _rec.close_method = f'pursuit_{reason}'
                        _rec.close_reason = f'PURSUIT_{reason}'
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _a6_err:
        logging.debug("[R37-UNIQUE-CLOSE] A6设置_closing失败: %s", _a6_err)
    try:
        if svc._ensure_order_service_fn is not None:
            try:
                svc._ensure_order_service_fn()
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] _ensure_order_service_fn failed: %s", _r3_err)
                pass
        order_svc = svc._state_store.get_ref('_order_service') if svc._state_store else None
        if order_svc:
            try:
                signal_strength = 1.0
                defensive_orders = order_svc.send_defensive_order(
                    instrument_id=instrument_id,
                    volume=volume,
                    price=price,
                    direction=direction,
                    action='CLOSE',
                    exchange='',
                    signal_strength=signal_strength,
                    is_stop_order='stop' in reason,
                )
                if defensive_orders:
                    logging.info("[HFT] pursuit exit via defensive order: %s %s vol=%d reason=%s order_ids=%s",
                                 instrument_id, direction, volume, reason, defensive_orders)
                    return
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as def_e:
                logging.warning("[HFT] send_defensive_order failed, fallback to signal: %s", def_e)
        sig_svc = svc._state_store.get_ref('_signal_service') if svc._state_store else None
        if sig_svc is None:
            try:
                from ali2026v3_trading.signal.signal_service import SignalService
                sig_svc = SignalService()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _sig_init_err:
                logging.debug("[R22-P1-NEW] 平仓SignalService初始化失败(平仓信号可能丢失): %s", _sig_init_err)
        if sig_svc:
            close_signal = sig_svc.generate_signal(
                instrument_id=instrument_id,
                signal_type=close_signal_type,
                price=price,
                volume=volume,
                reason=f"hft_{reason}",
                priority=10,
                cooldown_seconds=0,
                signal_strength=1.0,
            )
            if close_signal:
                logging.info("[HFT] pursuit exit signal emitted: %s %s vol=%d reason=%s",
                             instrument_id, direction, volume, reason)
            else:
                logging.debug("[HFT] pursuit exit signal rejected by cooldown: %s", instrument_id)
        else:
            logging.warning("[HFT] pursuit exit: no signal_service available, exit=%s %s vol=%d",
                            direction, instrument_id, volume)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.warning("[HFT] _execute_pursuit_exit failed: %s", e)


def execute_pursuit_entry(svc, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                          instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    direction = pursuit_signal.get('direction', '')
    signal_volume = pursuit_signal.get('volume', 1)
    price = pursuit_signal.get('price', last_price)
    strength_delta = pursuit_signal.get('strength_delta', 0.0)
    estimated_plr = pursuit_signal.get('estimated_plr', 0.0)
    min_pursuit_plr = svc._state_store.get('_min_pursuit_plr') if svc._state_store else None
    if min_pursuit_plr is None:
        min_pursuit_plr = 1.5
    if min_pursuit_plr > 0 and estimated_plr > 0 and estimated_plr < min_pursuit_plr:
        logging.debug("[HFT] pursuit entry blocked: estimated_plr=%.2f < min=%.2f for %s",
                      estimated_plr, min_pursuit_plr, instrument_id)
        return
    if not svc._check_hft_open_risk(instrument_id, direction, price, signal_volume, pursuit_signal):
        return
    try:
        from ali2026v3_trading.config.config_params import get_param
        _hft_confirm_ticks = int(get_param('hft_confirm_ticks', 3))
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        _hft_confirm_ticks = 3
    _confirm_count = pursuit_signal.get('confirm_ticks', 1)
    if _confirm_count < _hft_confirm_ticks:
        logging.debug("[HFT] pursuit entry: confirm_ticks=%d < hft_confirm_ticks=%d, deferring %s",
                     _confirm_count, _hft_confirm_ticks, instrument_id)
        return
    try:
        if svc._ensure_order_service_fn is not None:
            try:
                svc._ensure_order_service_fn()
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] _ensure_order_service_fn failed: %s", _r3_err)
                pass
        order_svc = svc._state_store.get_ref('_order_service') if svc._state_store else None
        if not order_svc:
            logging.warning("[HFT] pursuit entry: no order_service available, %s %s", instrument_id, direction)
            return
        bid_price = svc._get_tick_field(tick, 'bid_price1', 0.0)
        ask_price = svc._get_tick_field(tick, 'ask_price1', 0.0)
        bids = [(bid_price, 100)] if bid_price > 0 else None
        asks = [(ask_price, 100)] if ask_price > 0 else None
        signal_strength = min(abs(strength_delta) / 0.3, 1.0)
        order_ids = order_svc.send_order_split(
            instrument_id=instrument_id, volume=signal_volume, price=price,
            direction=direction, action='OPEN', exchange=exchange,
            signal_strength=signal_strength, bids=bids, asks=asks,
            open_reason='HIGH_FREQ',  # [FIX-20260712-S1] 改为HIGH_FREQ以使用60秒持仓/1分钟硬止损(原CORRECT_RESONANCE映射到resonance组=5分钟)
            signal_id=pursuit_signal.get('signal_id', ''),
        )
        if order_ids:
            pe = hft.pursuit_engine
            confirmed = all(pe.confirm_position_on_platform(instrument_id, oid) for oid in order_ids)
            if not confirmed:
                logging.warning("[HFT] pursuit entry: some confirm_position_on_platform failed for %s", instrument_id)
            logging.info("[HFT] pursuit entry order placed: %s %s vol=%d price=%.2f order_ids=%s",
                         instrument_id, direction, signal_volume, price, order_ids)
        else:
            logging.warning("[HFT] pursuit entry order failed: %s %s vol=%d", instrument_id, direction, signal_volume)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.warning("[HFT] _execute_pursuit_entry failed: %s", e)


def execute_pursuit_add(svc, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                         instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    direction = pursuit_signal.get('direction', '')
    add_volume = pursuit_signal.get('volume', 1)
    price = pursuit_signal.get('price', last_price)
    if not svc._check_hft_open_risk(instrument_id, direction, price, add_volume, pursuit_signal):
        return
    try:
        if svc._ensure_order_service_fn is not None:
            try:
                svc._ensure_order_service_fn()
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] _ensure_order_service_fn failed: %s", _r3_err)
                pass
        order_svc = svc._state_store.get_ref('_order_service') if svc._state_store else None
        if not order_svc:
            logging.warning("[HFT] pursuit add: no order_service available, %s %s", instrument_id, direction)
            return
        bid_price = svc._get_tick_field(tick, 'bid_price1', 0.0)
        ask_price = svc._get_tick_field(tick, 'ask_price1', 0.0)
        bids = [(bid_price, 100)] if bid_price > 0 else None
        asks = [(ask_price, 100)] if ask_price > 0 else None
        order_ids = order_svc.send_order_split(
            instrument_id=instrument_id, volume=add_volume, price=price,
            direction=direction, action='OPEN', exchange=exchange,
            signal_strength=0.9, bids=bids, asks=asks,
            open_reason='HIGH_FREQ',  # [FIX-20260712-S1] 改为HIGH_FREQ以使用60秒持仓/1分钟硬止损
            signal_id=pursuit_signal.get('signal_id', ''),
        )
        if order_ids:
            pe = hft.pursuit_engine
            confirmed = all(pe.add_platform_order_id(instrument_id, oid) for oid in order_ids)
            if not confirmed:
                logging.warning("[HFT] pursuit add: some add_platform_order_id failed for %s", instrument_id)
            logging.info("[HFT] pursuit add order placed: %s %s vol=%d price=%.2f order_ids=%s",
                         instrument_id, direction, add_volume, price, order_ids)
        else:
            logging.warning("[HFT] pursuit add order failed: %s %s vol=%d", instrument_id, direction, add_volume)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.warning("[HFT] _execute_pursuit_add failed: %s", e)


_last_arbitrage_signal: Optional[Dict[str, Any]] = None
_last_arbitrage_signal_ts: float = 0.0
_ARBITRAGE_SIGNAL_TTL_SEC: float = 60.0


def get_last_arbitrage_signal() -> Optional[Dict[str, Any]]:
    global _last_arbitrage_signal, _last_arbitrage_signal_ts
    if _last_arbitrage_signal is None:
        return None
    if time.time() - _last_arbitrage_signal_ts > _ARBITRAGE_SIGNAL_TTL_SEC:
        _last_arbitrage_signal = None
        return None
    return _last_arbitrage_signal


def handle_arbitrage_signal(svc, arbitrage_signal: Dict[str, Any], instrument_id: str) -> None:
    direction = arbitrage_signal.get('direction', '')
    deviation = arbitrage_signal.get('deviation_bps', 0.0)
    confidence = arbitrage_signal.get('confidence', 0.0)
    if confidence < 0.6:
        return
    global _last_arbitrage_signal, _last_arbitrage_signal_ts
    _last_arbitrage_signal = dict(arbitrage_signal)
    _last_arbitrage_signal['instrument_id'] = instrument_id
    _last_arbitrage_signal_ts = time.time()
    logging.info("[HFT] microstructure arbitrage: %s dir=%s deviation=%.1fbps confidence=%.2f",
                 instrument_id, direction, deviation, confidence)
    try:
        sig_svc = svc._state_store.get_ref('_signal_service') if svc._state_store else None
        if sig_svc is None:
            try:
                from ali2026v3_trading.signal.signal_service import SignalService
                sig_svc = SignalService()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _arb_sig_err:
                logging.debug("[R22-P1-NEW] 套利SignalService初始化失败(交易机会可能丢失): %s", _arb_sig_err)
        if sig_svc and abs(deviation) > 10:
            signal_type = 'OPEN_LONG' if direction == 'BUY' else 'OPEN_SHORT'
            sig_svc.generate_signal(
                instrument_id=instrument_id, signal_type=signal_type,
                price=arbitrage_signal.get('entry_price', 0.0), volume=1,
                reason='hft_arbitrage_deviation', priority=7, cooldown_seconds=5,
                signal_strength=min(abs(deviation) / 100.0, 1.0),
            )
        # [FIX-20260711-S5S6] S5套利改为实盘模拟监控模式，不直接下单
        # 原代码直接通过OrderService.send_order下单，现改为通过ArbitrageMonitor生成模拟信号+快照
        if abs(deviation) > 10:
            try:
                from ali2026v3_trading.strategy.monitor.arbitrage_monitor import ArbitrageMonitor
                _arb_monitor = ArbitrageMonitor.get_instance()
                _arb_monitor.on_arbitrage_signal(arbitrage_signal)
                _arb_monitor.save_snapshot(arbitrage_signal, instrument_id)
                logging.info("[S5-MONITOR] 套利信号模拟保存: %s dir=%s deviation=%.1fbps (不下单，仅监控)",
                             instrument_id, direction, deviation)
                _last_arbitrage_signal['hft_consumed'] = True
                _last_arbitrage_signal['monitor_saved'] = True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _mon_err:
                logging.warning("[S5-MONITOR] 套利监控模块调用失败(降级为纯日志): %s", _mon_err)
                _last_arbitrage_signal['hft_consumed'] = True
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[HFT] _handle_arbitrage_signal error: %s", e)


def handle_transition_signal(svc, transition_signal: Dict[str, Any],
                              instrument_id: str, last_price: float,
                              mid_price: float = 0.0) -> None:
    event = transition_signal.get('event', {})
    if not event:
        return
    transition_type = event.get('type', '')
    from_state = event.get('from_state', '')
    to_state = event.get('to_state', '')
    logging.info("[HFT] state transition captured: %s %s -> %s type=%s",
                 instrument_id, from_state, to_state, transition_type)
    if transition_type in ('OTHER_TO_CORRECT', 'OTHER_TO_INCORRECT'):
        try:
            sig_svc = svc._state_store.get_ref('_signal_service') if svc._state_store else None
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal.signal_service import SignalService
                    sig_svc = SignalService()
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _mm_sig_err:
                    logging.debug("[R22-P1-NEW] 做市SignalService初始化失败(交易机会可能丢失): %s", _mm_sig_err)
            if sig_svc:
                signal_price = mid_price if mid_price > 0 else last_price
                sig_svc.generate_signal(
                    instrument_id=instrument_id, signal_type='OPEN_LONG',
                    price=signal_price, volume=1,
                    reason='hft_transition_capture',
                    priority=9, cooldown_seconds=3,
                    signal_strength=0.8,
                )
                logging.info("[HFT] transition entry signal: %s dir=BUY reason=%s price=%.4f",
                             instrument_id, transition_type, signal_price)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[HFT] _handle_transition_signal error: %s", e)


def handle_smart_money_signal(svc, smart_money_signal: Dict[str, Any], instrument_id: str) -> None:
    signal = smart_money_signal.get('signal', 'neutral')
    # P2-09: 信号强度字段统一为 signal_strength（与 SignalContext 规范对齐）
    strength = smart_money_signal.get('signal_strength', 0.0)
    if signal == 'neutral' or strength < 0.3:
        return
    direction = 'BUY' if signal == 'buy' else 'SELL'
    logging.info("[HFT] smart money flow: %s dir=%s strength=%.3f",
                 instrument_id, direction, strength)
    try:
        sig_svc = svc._state_store.get_ref('_signal_service') if svc._state_store else None
        if sig_svc is None:
            try:
                from ali2026v3_trading.signal.signal_service import SignalService
                sig_svc = SignalService()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _res_sig_err:
                logging.debug("[R22-P1-NEW] 共振SignalService初始化失败(交易机会可能丢失): %s", _res_sig_err)
        if sig_svc and strength > 0.5:
            signal_type = 'OPEN_LONG' if direction == 'BUY' else 'OPEN_SHORT'
            sig_svc.generate_signal(
                instrument_id=instrument_id, signal_type=signal_type,
                price=0.0, volume=1,
                reason='hft_smart_money_flow',
                priority=8, cooldown_seconds=5,
                signal_strength=strength,
            )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[HFT] _handle_smart_money_signal error: %s", e)


def handle_filtered_signal(filter_result: Dict[str, Any], instrument_id: str, last_price: float) -> None:
    smooth = filter_result.get('smoothed_value', 0.0)
    velocity = filter_result.get('velocity', 0.0)
    logging.debug("[HFT] signal_filter passed: %s smooth=%.3f vel=%.4f",
                  instrument_id, smooth, velocity)


# ============================================================================
# 追仓/金字塔策略 (原 strategy_tick_handler.py)
# ============================================================================

@dataclass(slots=True)
class PursuitPosition:
    position_id: str
    instrument_id: str
    direction: str
    entries: List[Dict[str, Any]]
    total_volume: int
    weighted_avg_price: float
    current_stop_profit: float
    current_stop_loss: float
    peak_strength: float
    is_open: bool = True
    created_at: float = field(default_factory=time.time)
    platform_confirmed: bool = False
    platform_order_ids: List[str] = field(default_factory=list)


# R27-P0-FC-01修复: 实盘硬时间止损检查入口函数
def check_hard_time_stop_for_position(risk_service, position_id: str, open_time: float,
                                       max_profit_reached: float, profit_slope: float = 0.0,
                                       peak_profit_pct: float = 0.0, current_profit_pct: float = 0.0,
                                       bar_time: Optional[float] = None, strategy_group: str = '') -> Optional[str]:
    """实盘两阶段硬时间止损检查入口，调用SafetyMetaLayer.check_position_hard_time_stop"""
    safety = getattr(risk_service, '_safety_meta_layer', None)
    if safety is None:
        return None
    try:
        return safety.check_position_hard_time_stop(
            position_id, open_time, max_profit_reached,
            profit_slope, peak_profit_pct, current_profit_pct,
            bar_time=bar_time, strategy_group=strategy_group
        )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.warning("[R27-P0-FC-01] 硬时间止损检查异常: %s", e)
        return None


class DynamicPursuitEngine:
    def __init__(self, surge_threshold: float = 0.3, max_add_positions: int = 3,
                 add_volume_ratio: float = 0.5, stop_profit_trail_ratio: float = 0.3,
                 max_total_position_pct: float = 0.15, tight_stop_loss_pct: float = 0.15):
        self._surge_threshold = surge_threshold
        self._max_add_positions = max_add_positions
        self._add_volume_ratio = add_volume_ratio
        self._stop_profit_trail_ratio = stop_profit_trail_ratio
        self._max_total_position_pct = max_total_position_pct
        self._tight_sl_pct = tight_stop_loss_pct
        self._positions: Dict[str, PursuitPosition] = {}
        self._lock = threading.RLock()
        self._stats = {
            'total_pursuit_entries': 0, 'surge_detected': 0,
            'stop_profit_trails': 0, 'positions_closed': 0,
        }

    def evaluate_surge(self, instrument_id: str, current_strength: float,
                       prev_strength: float, current_price: float,
                       direction: str, account_equity: float = 100000.0) -> Optional[Dict[str, Any]]:
        if direction not in ('BUY', 'SELL'):
            logging.warning("[DynamicPursuitEngine] Invalid direction '%s', rejected", direction)
            return None
        if current_price <= 0:
            return None
        strength_delta = current_strength - prev_strength
        if strength_delta < self._surge_threshold:
            return None
        self._stats['surge_detected'] += 1
        with self._lock:
            pos = self._positions.get(instrument_id)
            if pos and pos.is_open:
                if pos.direction != direction:
                    return None
                add_count = len(pos.entries) - 1
                if add_count >= self._max_add_positions:
                    return None
                total_exposure = sum(e['volume'] * e['price'] for e in pos.entries)
                if account_equity > 0 and total_exposure / account_equity > self._max_total_position_pct:
                    return None
                base_volume = pos.entries[0]['volume']
                add_volume = max(1, int(base_volume * self._add_volume_ratio))
                new_stop_profit = self._calc_trailing_stop(pos.weighted_avg_price, current_price, pos.direction)
                pos.entries.append({
                    'price': current_price, 'volume': add_volume, 'strength': current_strength,
                    'strength_delta': strength_delta, 'timestamp': time.time(), 'entry_type': 'pursuit_add',
                })
                pos.total_volume += add_volume
                pos.weighted_avg_price = self._recalc_avg_price(pos.entries)
                pos.current_stop_profit = new_stop_profit
                pos.peak_strength = max(pos.peak_strength, current_strength)
                self._stats['total_pursuit_entries'] += 1
                return {
                    'action': 'ADD_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                    'volume': add_volume, 'price': current_price, 'new_stop_profit': new_stop_profit,
                    'total_volume': pos.total_volume, 'avg_price': pos.weighted_avg_price,
                    'strength_delta': strength_delta,
                }
            else:
                stop_profit = self._calc_initial_stop(current_price, direction)
                # FIX-R37-UNIQUE-ID: 增加随机熵，避免同毫秒同合约pos_id冲突导致持仓覆盖
                from ali2026v3_trading.infra.shared_utils import generate_prefixed_id as _gen_id
                pos = PursuitPosition(
                    position_id=f"PURSUIT_{instrument_id}_{int(time.time()*1000)}_{_gen_id('', 8)}",
                    instrument_id=instrument_id, direction=direction,
                    entries=[{'price': current_price, 'volume': 1, 'strength': current_strength,
                              'strength_delta': strength_delta, 'timestamp': time.time(), 'entry_type': 'initial'}],
                    total_volume=1, weighted_avg_price=current_price,
                    current_stop_profit=stop_profit,
                    current_stop_loss=self._calc_initial_stop_loss(current_price, direction),
                    peak_strength=current_strength,
                )
                self._positions[instrument_id] = pos
                self._stats['total_pursuit_entries'] += 1
                return {
                    'action': 'OPEN_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                    'volume': 1, 'price': current_price, 'stop_profit': stop_profit,
                    'strength_delta': strength_delta,
                }
        return None

    def update_trailing_stop(self, instrument_id: str, current_price: float, direction: str = '') -> Optional[float]:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return None
            pos_dir = pos.direction
            # R27-P0-FP-01修复: 使用浮点容差比较
            if pos_dir == 'BUY' and current_price <= pos.weighted_avg_price + _PRICE_TOLERANCE:
                return None
            if pos_dir == 'SELL' and current_price >= pos.weighted_avg_price - _PRICE_TOLERANCE:
                return None
            new_sp = self._calc_trailing_stop(pos.weighted_avg_price, current_price, pos_dir)
            improved = (new_sp > pos.current_stop_profit) if pos_dir == 'BUY' else (new_sp < pos.current_stop_profit)
            if improved:
                pos.current_stop_profit = new_sp
                self._stats['stop_profit_trails'] += 1
                return new_sp
        return None

    def check_exit(self, instrument_id: str, current_price: float) -> Optional[Dict[str, Any]]:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return None
            if not pos.platform_confirmed:
                pending_sec = time.time() - pos.created_at
                if pending_sec < 30.0:
                    return None
                pos.is_open = False
                self._stats['positions_closed'] += 1
                logging.warning("[DynamicPursuitEngine] %s exit unconfirmed position (timed out %.0fs)",
                                instrument_id, pending_sec)
                return None
            direction = pos.direction
            should_exit = False
            reason = ''
            if direction == 'BUY':
                # R27-P0-FP-01修复: 使用浮点容差比较，防止因浮点精度导致止盈止损误触发/漏触发
                if current_price > pos.current_stop_profit - _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_take_profit'
                elif current_price < pos.current_stop_loss + _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_stop_loss'
            else:
                # R27-P0-FP-01修复: 使用浮点容差比较
                if current_price > pos.current_stop_loss - _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_stop_loss'
                elif current_price < pos.current_stop_profit + _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_take_profit'
            if should_exit:
                pos.is_open = False
                self._stats['positions_closed'] += 1
                pnl = self._calc_pnl(pos, current_price)
                return {
                    'action': 'CLOSE_ALL', 'instrument_id': instrument_id,
                    'direction': 'SELL' if direction == 'BUY' else 'BUY',
                    'volume': pos.total_volume, 'price': current_price,
                    'reason': reason, 'pnl': pnl, 'entries': len(pos.entries),
                    'platform_order_ids': list(pos.platform_order_ids),
                }
        return None

    def confirm_position_on_platform(self, instrument_id: str, order_id: str) -> bool:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return False
            pos.platform_confirmed = True
            if order_id:
                pos.platform_order_ids.append(order_id)
            return True

    def add_platform_order_id(self, instrument_id: str, order_id: str) -> bool:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return False
            if order_id:
                pos.platform_order_ids.append(order_id)
            pos.platform_confirmed = True
            return True

    def _calc_trailing_stop(self, avg_price: float, current_price: float, direction: str) -> float:
        if direction == 'BUY':
            profit = current_price - avg_price
            if profit <= 0:
                return avg_price
            return current_price - profit * self._stop_profit_trail_ratio
        profit = avg_price - current_price
        if profit <= 0:
            return avg_price
        return avg_price - profit * self._stop_profit_trail_ratio

    def _calc_initial_stop(self, price: float, direction: str) -> float:
        if direction == 'BUY':
            return price * (1 + 0.005)
        return price * (1 - 0.005)

    def _calc_initial_stop_loss(self, price: float, direction: str) -> float:
        if direction == 'BUY':
            return price - price * self._tight_sl_pct
        return price + price * self._tight_sl_pct

    def _recalc_avg_price(self, entries: List[Dict]) -> float:
        total_vol = sum(e['volume'] for e in entries)
        if total_vol <= 0:
            return 0.0
        return sum(e['volume'] * e['price'] for e in entries) / total_vol

    def _calc_pnl(self, pos: PursuitPosition, exit_price: float) -> float:
        if pos.direction == 'BUY':
            return (exit_price - pos.weighted_avg_price) * pos.total_volume
        return (pos.weighted_avg_price - exit_price) * pos.total_volume

    def _cleanup_closed_positions(self, max_closed: int = 50) -> None:
        closed_keys = [k for k, p in self._positions.items() if not p.is_open]
        if len(closed_keys) > max_closed:
            for k in closed_keys[:len(closed_keys) - max_closed]:
                del self._positions[k]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_closed_positions()
            return {
                'service_name': 'DynamicPursuitEngine', **self._stats,
                'active_positions': sum(1 for p in self._positions.values() if p.is_open),
            }


class PyramidAddPositionEngine:
    """金字塔加仓引擎：信号增强时逐级递减加仓

    原理：每次加仓量为前次的pyramid_ratio倍（如0.5），
    形成金字塔结构——底部仓位大、顶部仓位小。'
    ATR自适应：加仓量与当前ATR反相关，高波动时减量。
    """

    def __init__(self, max_levels: int = 4,
                 pyramid_ratio: float = 0.5,
                 atr_adaptive: bool = True,
                 atr_reference: float = 0.02,
                 min_plr_for_add: float = 1.5):
        self._max_levels = max_levels
        self._pyramid_ratio = pyramid_ratio
        self._atr_adaptive = atr_adaptive
        self._atr_reference = atr_reference
        self._min_plr_for_add = min_plr_for_add
        self._positions: Dict[str, Dict] = {}
        self._stats = {'total_adds': 0, 'total_volume_added': 0, 'plr_blocked_adds': 0}

    def calc_add_volume(self, instrument_id: str, base_volume: int,
                        current_level: int, current_atr: float = 0.0,
                        current_plr: float = 0.0) -> int:
        if current_level >= self._max_levels:
            return 0
        if self._min_plr_for_add > 0 and current_plr > 0 and current_plr < self._min_plr_for_add:
            self._stats['plr_blocked_adds'] += 1
            return 0
        volume = int(base_volume * (self._pyramid_ratio ** current_level))
        if self._atr_adaptive and current_atr > 0 and self._atr_reference > 0:
            atr_scale = min(2.0, max(0.3, self._atr_reference / current_atr))
            volume = max(1, int(volume * atr_scale))
        self._stats['total_adds'] += 1
        self._stats['total_volume_added'] += volume
        return volume

    def get_stats(self) -> Dict[str, Any]:
        return {'service_name': 'PyramidAddPositionEngine', **self._stats}

    def _cleanup_closed_positions(self, max_closed: int = 50) -> None:
        closed_keys = [k for k, p in self._positions.items() if not p.get('is_open', True)]
        if len(closed_keys) > max_closed:
            for k in closed_keys[:len(closed_keys) - max_closed]:
                del self._positions[k]


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-05修复: tick热路径logging改用%格式化，避免f-string在未命中日志级别时的求值开销
# 使用示例: logger.debug("tick %s price=%.2f vol=%d", instrument_id, price, volume)
# 而非:    logger.debug(f"tick {instrument_id} price={price:.2f} vol={volume}")
# 已在此文件中逐步替换热路径(>1000calls/s)的f-string为%格式化
# 标记: 非热路径(<10calls/s)保留f-string可读性


def check_hmm_dwell_anomaly(handler_instance, current_state: str) -> None:
    """R23-SM-09-FIX: HMM状态驻留时间异常检测

    检测两种异常：
    1. 驻留时间过长(>_hmm_state_dwell_max_sec) — 状态卡死
    2. 切换频率过高(>_hmm_state_max_switches_per_window per _hmm_state_switch_window_sec) — 震荡
    """
    if not hasattr(handler_instance, '_hmm_state_entry_time'):
        return
    _now = time.time()
    # R23-SM-P1-03-FIX: HMM转移矩阵退化检测 — 某状态长期未被访问
    _all_states = set(getattr(handler_instance, '_hmm_state_entry_time', {}).keys())
    _all_states.add(current_state)
    for _s in _all_states:
        _last_visit = handler_instance._hmm_state_entry_time.get(_s, 0.0)
        if _last_visit > 0 and (_now - _last_visit) > handler_instance._hmm_state_dwell_max_sec and _s != current_state:
            logging.warning("[R23-SM-P1-03-FIX] HMM状态长期未被访问(退化): state=%s unvisited=%.1fs > max=%.1fs",
                           _s, _now - _last_visit, handler_instance._hmm_state_dwell_max_sec)
    _prev_state = getattr(handler_instance, '_hmm_last_state', None)
    if _prev_state is not None and _prev_state != current_state:
        _entry_time = handler_instance._hmm_state_entry_time.get(_prev_state, 0.0)
        if _entry_time > 0:
            _dwell = _now - _entry_time
            if _dwell > handler_instance._hmm_state_dwell_max_sec:
                logging.warning("[R23-SM-09-FIX] HMM状态驻留超限: state=%s dwell=%.1fs > max=%.1fs",
                               _prev_state, _dwell, handler_instance._hmm_state_dwell_max_sec)
            if _dwell < handler_instance._hmm_state_dwell_min_sec:
                logging.warning("[R23-SM-09-FIX] HMM状态切换过快: state=%s dwell=%.3fs < min=%.3fs",
                               _prev_state, _dwell, handler_instance._hmm_state_dwell_min_sec)
        _window = handler_instance._hmm_state_switch_window_sec
        _window_key = f"{_prev_state}_{int(_now // _window)}"
        handler_instance._hmm_state_switch_counts[_window_key] = handler_instance._hmm_state_switch_counts.get(_window_key, 0) + 1
        if handler_instance._hmm_state_switch_counts[_window_key] > handler_instance._hmm_state_max_switches_per_window:
            logging.warning("[R23-SM-09-FIX] HMM状态震荡: state=%s switches=%d > max=%d in %.0fs window",
                           _prev_state, handler_instance._hmm_state_switch_counts[_window_key],
                           handler_instance._hmm_state_max_switches_per_window, _window)
    handler_instance._hmm_state_entry_time[current_state] = _now
    handler_instance._hmm_last_state = current_state

# R15-P2-PERF-09标记: 循环中.append改为列表推导式(仅标记，不改逻辑)
# 识别位置: _shard_buffers、_probe_logged_instruments等append调用
# TODO(R17-P2-DOC-02): 将for循环中的.append改为列表推导式，例如:
#   results = [process(item) for item in items]  替代  results=[]; for item in items: results.append(process(item))

# R15-P2-PERF-10标记: 多次重复import移到模块顶部
# 识别位置: 函数内 from ali2026v3_trading.infra.shared_utils import ... 重复调用
# TODO: 将函数内延迟import移到模块顶部，仅在解决循环依赖时保留函数内import  # R17-P2-DEP-03: 模块级/函数内import混用标记


# ============================================================================
# 延迟导入 (避免循环依赖)
# ============================================================================

def __getattr__(name):
    """延迟导入 tick_processing_service 中的名称，避免循环依赖"""
    _lazy_names = {
        'TickProcessingService', 'MarketEvent', 'TickEvent', 'BarCompletedEvent',
        'TickHandlerMixin',
    }
    if name in _lazy_names:
        from ali2026v3_trading.strategy.tick_processing_service import (
            TickProcessingService, MarketEvent, TickEvent, BarCompletedEvent,
        )
        # 向后兼容别名: TickHandlerMixin = TickProcessingService
        globals().update(
            TickProcessingService=TickProcessingService,
            MarketEvent=MarketEvent,
            TickEvent=TickEvent,
            BarCompletedEvent=BarCompletedEvent,
            TickHandlerMixin=TickProcessingService,
        )
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
