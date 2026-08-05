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

from infra._helpers import get_logger  # R9-5
from infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from config import config_params
from strategy_judgment.causal_chain_utils import (
    CausalChainTracker, ContaminationGuard, CyclicDependencyGuard,
    validate_tick_cascade, CausalEvent,
)

# R27-P1修复: 导入容错/浮点工具
from infra.resilience import (
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
    # DEL-S1-ARB-20260729: 套利交易已删除(用户决策: 风险太大)
    # S1-HFT仅保留 tick级期权排序 + tick级订单流 开仓
    # 'handle_arbitrage_signal',  -- 已删除
    # 'get_last_arbitrage_signal',  -- 已删除
    'handle_transition_signal',
    'handle_smart_money_signal',
    'handle_filtered_signal',
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
                # FIX-75 D1 P1: 窄except扩展为Exception，避免吞掉MemoryError/RuntimeError等
                # 根因: 原except仅捕获(ValueError, KeyError, TypeError, AttributeError)，
                #       RuntimeError(如线程池已关闭)/ImportError/OSError等会被抛出中断tick处理
                except Exception as _r3_err:
                    logging.debug("[R3-L2] _ensure_hft_engine_fn failed: %s", _r3_err)
                    pass
                hft = svc._state_store.get_ref('_hft_engine') if svc._state_store else None

        # FIX-P0-25: 原width_resonance计算被耦合在if hft is not None:块内
        # 导致HFT引擎未初始化时get_width_strength()从不被调用 → query_count=0
        # width_resonance用于StateParamManager.update_market_context，与HFT引擎无关
        # 应独立执行，确保宽度强度计算和状态参数更新不受HFT可用性影响
        bid_price = svc._get_tick_field(tick, 'bid_price1', 0.0)
        ask_price = svc._get_tick_field(tick, 'ask_price1', 0.0)

        # DEL-S5-20260729: S5套利策略已彻底删除(用户决策放弃), tick投喂块已移除

        direction_raw = ''
        # FIX-HFT-DIR-20260724: direction获取根因修复(S1-HFT全天0下单根因)
        # 根因: rc.get_tick()方法不存在(RealTimeCache无此方法,仅有get_authoritative_state/
        #   get_latest_price/get_recent_ticks), 且tick_entry不存储direction字段(仅存
        #   price/volume/bid_price/ask_price/option_type/strike_price)。
        #   → rc.get_tick()抛AttributeError被except捕获 → direction_raw永远为空
        #   → V4-FIX-O1每tick拒绝(25324次/小时) → S1-HFT全天0下单
        #   原始tick对象也无direction字段(仅有bid/ask_price1-5、bid/ask_volume1-5、
        #   last_price、volume、open_interest、turnover)。
        # 修复原则(用户2026-07-25决策): 永远只做条件成就时的正确交易, 不做任何"增强"推断
        #   - tick稀少/数据不全时 = 不开仓(fail-closed), 而非用替代数据源制造交易信号
        #   - 已彻底删除的增强措施(原FIX-HFT-DIR v1的错误):
        #     A. 用缓存tick的bid/ask补充当前tick缺失值 (制造非零bid/ask → 假信号)
        #     B. bid_volume1/ask_volume1订单簿不平衡推断dead-zone方向 (制造方向 → 假信号)
        #   - rc查找块已整体移除(不再需要缓存数据, 消除死代码)
        #   - 保留V4-FIX-O1 fail-closed(direction仍为空则不开仓)
        try:
            from infra.subscription_service import SubscriptionManager
            if SubscriptionManager.is_option(instrument_id):
                direction_raw = 'buy'
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            pass  # direction_raw保持'', 后续条件推断或fail-closed
        # direction推断: 仅当bid/ask都有效且last_price明确偏向一侧时才推断
        # 删除增强B: dead-zone(last_price在spread中位)时不再用volume推断, 直接fail-closed
        # FIX-S1S7-FUTURES-DIR-20260727: 期货direction推断根因修复
        # 根因1: 期货spread极小(1-2tick), dead_zone_ratio按比例计算→dead-zone绝对宽度<1点
        #        last_price在spread中间→落在dead-zone→direction为空→0下单
        #        例: rb2612 bid=3760 ask=3761 spread=1, ratio=0.40
        #            buy阈值=3761-0.40=3760.60, sell阈值=3760+0.40=3760.40
        #            last_price=3760.5在(3760.40,3760.60)→dead-zone→0下单
        # 根因2: 期货tick的bid/ask有时=0(非连续竞价/集合竞价/C++推送异常)
        #        →L161条件不满足→dead-zone推断不执行→direction永远空
        # 修复策略(三层, 从严到宽, 均保持fail-closed):
        #   1. 期权: direction_raw='buy'(不变, 期权有隐含方向)
        #   2. 期货bid/ask有效: 按绝对tick偏移推断(last_price距bid/ask≤1tick即推断)
        #      期货spread小, 1tick偏移覆盖大部分情况; 期权spread大走比例推断
        #   3. 期货bid/ask无效(=0): 缓存上一次有效bid/ask用于推断; 无缓存则fail-closed
        # 不改变策略逻辑: 仍然是fail-closed(数据不可用=不开仓), 仅修正推断算法适配期货特性
        _dead_zone_ratio = 0.40  # 默认值(期权用)
        try:
            from config.config_service import get_cached_params
            _cached = get_cached_params()
            if _cached and 'hft_dead_zone_ratio' in _cached:
                _dead_zone_ratio = float(_cached['hft_dead_zone_ratio'])
        except (ImportError, AttributeError, TypeError, ValueError):
            pass

        # FIX-S1S7-FUTURES-DIR-20260727: 期货direction推断
        # 判断是否为期货合约
        _is_future_inst = False
        try:
            from infra.subscription_service import SubscriptionManager
            _is_future_inst = not SubscriptionManager.is_option(instrument_id)
        except Exception:
            pass  # 无法判断时保守处理

        if not direction_raw and bid_price > 0 and ask_price > 0:
            if _is_future_inst:
                # 期货: 按绝对tick偏移推断
                # 期货spread通常1-2tick, last_price距bid/ask≤1tick即推断方向
                # 这比比例推断更适配期货特性(比例推断在spread=1时dead-zone绝对宽度<1点)
                spread = ask_price - bid_price
                if spread > 0:
                    _fut_tick_size = 1.0  # 最小变动价位(大部分期货1, 股指0.2等)
                    # 根据合约类型调整tick_size
                    _inst_upper = instrument_id.upper()
                    if _inst_upper[:2] in ('IF', 'IC', 'IH', 'IM'):
                        _fut_tick_size = 0.2  # 股指期货最小变动0.2点
                    elif _inst_upper[:2] in ('TF', 'TS', 'TL', 'T'):
                        _fut_tick_size = 0.005  # 国债期货
                    # 期货direction推断策略: 用mid分界(last_price>=mid→buy, <mid→sell)
                    # 期货spread极小(1-5tick), mid是最自然的分界线, 无需dead-zone
                    # 期权spread大, 需要ratio-based dead-zone(40%), 但期货不需要
                    # 例: rb2612 spread=1 bid=3760 ask=3761 mid=3760.5
                    #     last=3760.5→buy(>=mid), last=3760→sell(<mid)
                    # 例: IF2502 spread=0.6 bid=3831.6 ask=3832.2 mid=3831.9
                    #     last=3832.0→buy(>=mid), last=3831.6→sell(<mid)
                    _fut_mid = (bid_price + ask_price) / 2.0
                    if last_price >= _fut_mid:
                        direction_raw = 'buy'
                    else:
                        direction_raw = 'sell'
            else:
                # 期权: 按比例推断(spread大, 比例推断更合适)
                mid = (bid_price + ask_price) / 2.0
                if mid > 0:
                    spread = ask_price - bid_price
                    if last_price >= ask_price - spread * _dead_zone_ratio:
                        direction_raw = 'buy'
                    elif last_price <= bid_price + spread * _dead_zone_ratio:
                        direction_raw = 'sell'

        # FIX-S1S7-CACHE-20260729: 缓存更新独立于direction推断执行
        # 根因: 原cache更新代码在 `if not direction_raw` 块内的else分支(line 196-202),
        #   当bid>0 AND ask>0时direction_raw已在line 143-168设置→not direction_raw=False
        #   →跳过整个块→cache从不更新→后续tick bid/ask=0时cache为空→direction永远为空
        # 修复: 将cache更新移到独立块,只要期货bid>0 AND ask>0就更新cache(无论direction是否已设置)
        if _is_future_inst and bid_price > 0 and ask_price > 0:
            _cache_map_upd = dispatch_hft_tick.__dict__.get('_last_valid_bid_ask', {})
            if not isinstance(_cache_map_upd, dict):
                _cache_map_upd = {}
                dispatch_hft_tick._last_valid_bid_ask = _cache_map_upd
            _cache_map_upd[instrument_id] = (bid_price, ask_price)

        # FIX-S1S7-FUTURES-DIR-20260727: 期货bid/ask=0时，用缓存的上一次; last_price与缓存mid对比推断
        if not direction_raw and _is_future_inst and last_price > 0:
            if bid_price <= 0 or ask_price <= 0:
                # bid/ask无效，尝试使用缓存的最后一次有效值
                _cache_map = dispatch_hft_tick.__dict__.get('_last_valid_bid_ask', {})
                if not isinstance(_cache_map, dict):
                    _cache_map = {}
                    dispatch_hft_tick._last_valid_bid_ask = _cache_map
                _cached_ba = _cache_map.get(instrument_id)
                if _cached_ba and len(_cached_ba) == 2:
                    _cb, _ca = _cached_ba
                    if _cb > 0 and _ca > 0:
                        _cached_mid = (_cb + _ca) / 2.0
                        if last_price >= _cached_mid:
                            direction_raw = 'buy'
                        else:
                            direction_raw = 'sell'
        # V4-FIX-O1: direction为空=无方向=不开仓(fail-closed)
        # 原则: 数据不可用=不开仓，而非数据不可用=默认buy方向
        if not direction_raw and last_price > 0:
            # DIAG-S1S7-20260727: 期货direction为空时记录bid/ask/last_price/DeadZoneRatio，定位根因
            # 仅对期货合约、仅INFO级别、节流60s
            try:
                from infra.subscription_service import SubscriptionManager
                _is_opt_diag = SubscriptionManager.is_option(instrument_id)
            except Exception:
                _is_opt_diag = True  # 降级时不打诊断
            if not _is_opt_diag:
                _diag_cls = svc.__class__
                _diag_ts_map = getattr(_diag_cls, '_s1s7_diag_ts', {})
                if not isinstance(_diag_ts_map, dict):
                    _diag_ts_map = {}
                    setattr(_diag_cls, '_s1s7_diag_ts', _diag_ts_map)
                _diag_now = time.time()
                if _diag_now - _diag_ts_map.get(instrument_id, 0.0) >= 60:
                    logging.info("[DIAG-S1S7] 期货direction为空 inst=%s bid=%.2f ask=%.2f last=%.2f ratio=%.2f spread=%.4f",
                                instrument_id, bid_price, ask_price, last_price, _dead_zone_ratio,
                                (ask_price - bid_price) if bid_price > 0 and ask_price > 0 else 0.0)
                    _diag_ts_map[instrument_id] = _diag_now
            # FIX-20260723-O1-THROTTLE: 日志限频(300s冷却)
            _o1_now = time.time()
            _o1_last = svc.__class__.__dict__.get('_o1_warn_ts', {})
            if not isinstance(_o1_last, dict):
                _o1_last = {}
                setattr(svc.__class__, '_o1_warn_ts', _o1_last)
            if _o1_now - _o1_last.get(instrument_id, 0.0) >= 300:
                logging.debug("[V4-FIX-O1] direction为空, 返回None (数据不可用=不开仓) inst=%s", instrument_id)
                _o1_last[instrument_id] = _o1_now
            return None

        product = ''
        try:
            from infra.shared_utils import extract_product_code
            product = extract_product_code(instrument_id)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] extract_product_code failed: %s", _r3_err)
            pass

        width_resonance = 0.0
        # FIX-WR-CHAIN-PROBE-20260731: INFO级链路诊断(前20+每1000次), 定位width_resonance=0断裂点
        _wr_chain_n = getattr(dispatch_hft_tick, '_wr_chain_count', 0) + 1
        dispatch_hft_tick._wr_chain_count = _wr_chain_n
        _wr_chain_log = _wr_chain_n <= 20 or _wr_chain_n % 1000 == 0
        try:
            ps = None
            try:
                from config.params_service import get_params_service
                ps = get_params_service()
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                if _wr_chain_log:
                    logging.info("[WR-CHAIN] #%d inst=%s BREAK: get_params_service failed: %s", _wr_chain_n, instrument_id, _r3_err)
                pass
            # FIX-WR-PS-BOOL-20260731: 改用 `ps is None` 代替 `not ps`
            # 根因: ParamsService实现了__len__()方法(返回len(_params)),
            #   当_params字典为空时len=0, Python调用__len__使bool(ps)=False,
            #   导致有效的ParamsService实例被`not ps`误判为None → 整条width_resonance链路断裂
            # 这是一个Python陷阱: 有__len__且len=0的对象, bool()返回False
            # 影响范围: 全部6个候选断裂点中ps=None是实际断裂点(WR-CHAIN日志100%在此断)
            if ps is None:
                if _wr_chain_log:
                    logging.info("[WR-CHAIN] #%d inst=%s BREAK: ps=None", _wr_chain_n, instrument_id)
            else:
                meta = ps.get_instrument_meta_by_id(instrument_id)
                if not meta:
                    if _wr_chain_log:
                        logging.info("[WR-CHAIN] #%d inst=%s BREAK: meta=None", _wr_chain_n, instrument_id)
                else:
                    uf_id = meta.get('underlying_future_id')
                    if not uf_id:
                        uf_id = meta.get('internal_id')
                    if not uf_id:
                        if _wr_chain_log:
                            logging.info("[WR-CHAIN] #%d inst=%s BREAK: uf_id=None (both underlying_future_id and internal_id empty)", _wr_chain_n, instrument_id)
                    else:
                        tts = svc._state_store.get_ref('t_type_service') if svc._state_store else None
                        tts_src = 'state_store'
                        if tts is None:
                            try:
                                from data.t_type_service import get_t_type_service
                                tts = get_t_type_service()
                                tts_src = 'singleton'
                            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                                if _wr_chain_log:
                                    logging.info("[WR-CHAIN] #%d inst=%s BREAK: tts=None (state_store+singleton both failed: %s)", _wr_chain_n, instrument_id, _r3_err)
                                pass
                        if tts is None:
                            if _wr_chain_log:
                                logging.info("[WR-CHAIN] #%d inst=%s BREAK: tts=None", _wr_chain_n, instrument_id)
                        else:
                            wc = getattr(tts, '_width_cache', None) if tts else None
                            if not wc:
                                if _wr_chain_log:
                                    logging.info("[WR-CHAIN] #%d inst=%s BREAK: wc=None (tts=%s src=%s, _width_cache attr missing)", _wr_chain_n, instrument_id, type(tts).__name__, tts_src)
                            else:
                                ws_method = getattr(wc, 'get_width_strength', None)
                                get_months_method = getattr(wc, 'get_all_months', None)
                                if not (ws_method and get_months_method):
                                    if _wr_chain_log:
                                        logging.info("[WR-CHAIN] #%d inst=%s BREAK: methods missing (ws=%s months=%s)", _wr_chain_n, instrument_id, ws_method is not None, get_months_method is not None)
                                else:
                                    months = get_months_method(int(uf_id))
                                    if not months:
                                        if _wr_chain_log:
                                            # 输出_months键样本帮助诊断键不匹配
                                            _sample_keys = list(getattr(wc, '_months', {}).keys())[:5]
                                            _opt_info_count = len(getattr(wc, '_option_info', {}))
                                            _sync_otm_count = len(getattr(wc, '_sync_otm_count', {}))
                                            # FIX-WR-CHAIN-20260731: 增加WC params_service诊断
                                            _wc_ps = getattr(wc, '_params_service', None)
                                            _wc_ps_meta_count = len(getattr(_wc_ps, '_instrument_meta_by_id', {})) if _wc_ps else -1
                                            logging.info("[WR-CHAIN] #%d inst=%s BREAK: months=[] (uf_id=%r type=%s, _months_keys=%s, _opt_info=%d, _sync_otm_fids=%d, wc_ps_meta=%d, tts_src=%s)",
                                                         _wr_chain_n, instrument_id, uf_id, type(uf_id).__name__, _sample_keys, _opt_info_count, _sync_otm_count, _wc_ps_meta_count, tts_src)
                                    else:
                                        ws = ws_method(int(uf_id), months)
                                        width_resonance = min(ws / 10.0, 1.0) if ws > 0 else 0.0
                                        if _wr_chain_log:
                                            logging.info("[WR-CHAIN] #%d inst=%s OK: uf_id=%r months=%s ws=%d wr=%.4f", _wr_chain_n, instrument_id, uf_id, months, ws, width_resonance)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            if _wr_chain_log:
                logging.info("[WR-CHAIN] #%d inst=%s BREAK: exception: %s", _wr_chain_n, instrument_id, _r3_err)
            pass

        # FIX-SYNC-OTM-20260730: 诊断日志，确认width_resonance和resonance_strength是否非零
        # 限频(60s per instrument_id)
        _wr_now = time.time()
        _wr_last = dispatch_hft_tick.__dict__.get('_wr_log_ts', {})
        if not isinstance(_wr_last, dict):
            _wr_last = {}
            dispatch_hft_tick._wr_log_ts = _wr_last
        if _wr_now - _wr_last.get(instrument_id, 0.0) >= 60:
            logging.info("[FIX-SYNC-OTM] width_resonance: inst=%s wr=%.4f", instrument_id, width_resonance)
            _wr_last[instrument_id] = _wr_now

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
                    # FIX-MARKET-CLOSE: 收盘后禁止开仓/加仓（仅允许平仓）
                    # 根因: dispatch_hft_tick在onTick回调中直接触发，不受scheduler门控。
                    # 15:00后如果C++平台仍推送tick，HFT追仓入场/加仓可能产生不应有的开仓订单。
                    # MarketTimeService已正确配置6交易所收盘时间(全部15:00)。
                    if action in ('OPEN_POSITION', 'ADD_POSITION'):
                        try:
                            from infra.market_time_service import get_market_open_cache as _gmo_cache_20260720
                            if not _gmo_cache_20260720().is_open():
                                logging.info("[FIX-MARKET-CLOSE] HFT %s跳过: 市场已收盘 inst=%s", action, instrument_id)
                                pursuit_signal = None  # 清除信号，阻止执行
                        except Exception as _mkt_err1:  # V4-FIX-O3: fail-closed
                            pursuit_signal = None  # 门控不可用=不交易
                            logging.warning("[V4-FIX-O3] MarketTimeService异常(OPEN/ADD), 阻断 (fail-closed): %s", _mkt_err1)
                    if pursuit_signal is not None:
                        if action == 'OPEN_POSITION':
                            execute_pursuit_entry(svc, hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)
                        elif action == 'ADD_POSITION':
                            execute_pursuit_add(svc, hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)
                        # S1-HFT单路径架构: 信号完全由路径1(dispatch_hft_tick)实时处理

                pursuit_exit = hft_result.get('pursuit_exit')
                if pursuit_exit:
                    # FIX-MARKET-CLOSE: 收盘后交易所通道关闭，平仓订单也无法提交
                    # 根因: 交易所收盘后通道关闭，任何订单(包括平仓)都无法提交执行。
                    # pursuit_exit本质是提交平仓订单到交易所，收盘后无意义。
                    _exit_blocked = False
                    try:
                        from infra.market_time_service import get_market_open_cache as _gmo_cache_20260720
                        if not _gmo_cache_20260720().is_open():
                            _exit_blocked = True
                            logging.info("[FIX-MARKET-CLOSE] HFT pursuit_exit跳过: 市场已收盘 inst=%s", instrument_id)
                    except Exception as _mkt_err2:  # V4-FIX-O3: fail-closed
                        _exit_blocked = True  # 门控不可用=不交易
                        logging.warning("[V4-FIX-O3] MarketTimeService异常(exit), 阻断 (fail-closed): %s", _mkt_err2)
                    if not _exit_blocked:
                        logging.info("[HFT] pursuit exit: %s %s reason=%s pnl=%.2f",
                                     pursuit_exit.get('instrument_id', ''),
                                     pursuit_exit.get('direction', ''),
                                     pursuit_exit.get('reason', ''),
                                     pursuit_exit.get('pnl', 0.0))
                        try:
                            execute_pursuit_exit(svc, hft, pursuit_exit, instrument_id)
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as ex_e:
                            logging.debug("[_dispatch_hft_tick] HFT pursuit exit execution error: %s", ex_e)

                # DEL-S1-ARB-20260729: 套利信号分发已删除(用户决策: 风险太大)
                # S1-HFT不再执行微观结构套利交易，仅保留期权排序+订单流开仓
                # arbitrage_signal = hft_result.get('arbitrage_signal')
                # if arbitrage_signal: ... handle_arbitrage_signal(...)

                transition_signal = hft_result.get('transition_signal')
                if transition_signal:
                    # FIX-MARKET-CLOSE: 收盘后交易所通道关闭，转换信号无法执行
                    _trans_blocked = False
                    try:
                        from infra.market_time_service import get_market_open_cache as _gmo_cache_20260720
                        if not _gmo_cache_20260720().is_open():
                            _trans_blocked = True
                    except Exception as _mkt_err4:  # V4-FIX-O3: fail-closed
                        _trans_blocked = True  # 门控不可用=不交易
                        logging.warning("[V4-FIX-O3] MarketTimeService异常(trans), 阻断 (fail-closed): %s", _mkt_err4)
                    if not _trans_blocked:
                        hft_mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else last_price
                        handle_transition_signal(svc, transition_signal, instrument_id, last_price, mid_price=hft_mid_price)

                smart_money_signal = hft_result.get('smart_money_signal')
                if smart_money_signal:
                    # FIX-MARKET-CLOSE: 收盘后交易所通道关闭，聪明钱信号无法执行
                    _sm_blocked = False
                    try:
                        from infra.market_time_service import get_market_open_cache as _gmo_cache_20260720
                        if not _gmo_cache_20260720().is_open():
                            _sm_blocked = True
                    except Exception as _mkt_err5:  # V4-FIX-O3: fail-closed
                        _sm_blocked = True  # 门控不可用=不交易
                        logging.warning("[V4-FIX-O3] MarketTimeService异常(sm), 阻断 (fail-closed): %s", _mkt_err5)
                    if not _sm_blocked:
                        handle_smart_money_signal(svc, smart_money_signal, instrument_id)

                signal_filter_result = hft_result.get('signal_filter')
                if signal_filter_result and signal_filter_result.get('threshold_crossed'):
                    # FIX-MARKET-CLOSE: 收盘后交易所通道关闭，过滤信号无法执行
                    _sf_blocked = False
                    try:
                        from infra.market_time_service import get_market_open_cache as _gmo_cache_20260720
                        if not _gmo_cache_20260720().is_open():
                            _sf_blocked = True
                    except Exception as _mkt_err6:  # V4-FIX-O3: fail-closed
                        _sf_blocked = True  # 门控不可用=不交易
                        logging.warning("[V4-FIX-O3] MarketTimeService异常(sf), 阻断 (fail-closed): %s", _mkt_err6)
                    if not _sf_blocked:
                        handle_filtered_signal(signal_filter_result, instrument_id, last_price)
    except Exception as hft_e:
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
    # FIX-S1-DYNAMIC-COOLDOWN-20260730: 记录平仓;仅reason包含stop_loss时计入止损计数
    pe.record_exit(instrument_id, reason)
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
            # FIX-HFT-NEVER-OPENED-NOISE-20260803: 频率控制(60s冷却+汇总计数)
            # 根因: DRY-RUN模式下每60s超时清理产生大量"never opened"警告(6081条/小时)
            #   淹没其他重要日志, 且此警告在DRY-RUN模式属正常行为(虚拟开仓回调模拟)
            # 修复: 60s冷却+汇总计数, 与V4-FIX-O12模式一致
            _never_opened_now = time.time()
            _never_opened_last = getattr(pe.__class__, '_never_opened_warn_ts', None) or {}
            _never_opened_count = _never_opened_last.get('_count', 0) + 1
            _never_opened_last['_count'] = _never_opened_count
            if _never_opened_now - _never_opened_last.get('_last_log', 0.0) >= 60:
                logging.warning("[HFT] pursuit exit: %d positions were never opened on platform, cleaned up (DRY-RUN normal)", _never_opened_count)
                _never_opened_last['_last_log'] = _never_opened_now
                _never_opened_last['_count'] = 0
            setattr(pe.__class__, '_never_opened_warn_ts', _never_opened_last)
            return
    close_signal_type = 'CLOSE_LONG' if direction == 'SELL' else 'CLOSE_SHORT'
    # FIX-R37-UNIQUE-CLOSE(A6): execute_pursuit_exit 必须设置 PositionService 持仓 _closing 标志，
    # 否则止盈止损/时间止损检查时 _closing=False 会重复触发平仓，导致双重平仓。
    # pursuit_engine 有自己的 _positions(PursuitPosition)，但 PositionService.positions
    # 也可能有对应持仓(通过 instrument_id 关联)，必须同步设置 _closing。
    try:
        from position.position_service import get_position_service
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
                # V4-FIX-O4: signal_strength使用实际评估强度(非硬编码1.0)
                # 从pursuit_signal获取strength_delta, 无法获取时fail-closed使用0.0
                _exit_strength_delta = 0.0
                try:
                    _pos = hft._positions.get(instrument_id) if hft else None
                    if _pos:
                        _exit_strength_delta = getattr(_pos, 'current_stop_profit', 0.0) - getattr(_pos, 'current_stop_loss', 0.0)
                except Exception:
                    pass
                signal_strength = min(abs(_exit_strength_delta) / max(abs(price) * 0.01, 1e-6), 1.0) if _exit_strength_delta != 0.0 else 0.0
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
            except Exception as def_e:
                logging.warning("[HFT] send_defensive_order failed, fallback to signal: %s", def_e)
        sig_svc = svc._state_store.get_ref('_signal_service') if svc._state_store else None
        if sig_svc is None:
            try:
                from signal.signal_service import SignalService
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
                signal_strength=signal_strength,  # V4-FIX-O4: 使用实际评估强度
            )
            if close_signal:
                logging.info("[HFT] pursuit exit signal emitted: %s %s vol=%d reason=%s",
                             instrument_id, direction, volume, reason)
            else:
                logging.debug("[HFT] pursuit exit signal rejected by cooldown: %s", instrument_id)
        else:
            logging.warning("[HFT] pursuit exit: no signal_service available, exit=%s %s vol=%d",
                            direction, instrument_id, volume)
    except Exception as e:
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
        from config.config_params import get_param
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
        # FIX-S1-SUPerset-20260730: 按signal_source适配信号强度归一化分母
        # FIX-S1-RESONANCE-REQUIRED-20260803: degrade_order_flow已删除(无共振不开仓)
        # surge(默认): delta/0.3; level_resonance: delta/0.45
        _sig_src = pursuit_signal.get('signal_source', 'surge')
        _strength_denom = {'surge': 0.3, 'level_resonance': 0.45}.get(_sig_src, 0.3)
        signal_strength = min(abs(strength_delta) / _strength_denom, 1.0) if _strength_denom > 0 else 0.0
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
            # FIX-S1-DYNAMIC-COOLDOWN-20260730: 记录成功开仓
            pe.record_entry(instrument_id, is_add=False)
            logging.info("[HFT] pursuit entry order placed: %s %s vol=%d price=%.2f order_ids=%s",
                         instrument_id, direction, signal_volume, price, order_ids)
        else:
            logging.warning("[HFT] pursuit entry order failed: %s %s vol=%d", instrument_id, direction, signal_volume)
    except Exception as e:
        logging.warning("[HFT] _execute_pursuit_entry failed: %s", e)


def execute_pursuit_add(svc, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                         instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    direction = pursuit_signal.get('direction', '')
    add_volume = pursuit_signal.get('volume', 1)
    price = pursuit_signal.get('price', last_price)
    # FIX-S1-STRENGTH-DELTA-20260729: 补充strength_delta变量定义
    # 根因: execute_pursuit_add第619行引用strength_delta但函数体未定义该变量
    #   → NameError: name 'strength_delta' is not defined → 加仓永远失败
    # 修复: 从pursuit_signal获取(与execute_pursuit_entry第538行一致)
    strength_delta = pursuit_signal.get('strength_delta', 0.0)
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
        # FIX-S1-SUPerset-ADD-20260730: 加仓信号强度按signal_source归一化(与execute_pursuit_entry对齐)
        # FIX-S1-RESONANCE-REQUIRED-20260803: degrade_order_flow已删除(无共振不开仓)
        # surge: delta/0.3; level_resonance: delta/0.45
        _add_sig_src = pursuit_signal.get('signal_source', 'surge')
        _add_strength_denom = {'surge': 0.3, 'level_resonance': 0.45}.get(_add_sig_src, 0.3)
        _add_signal_strength = (min(abs(strength_delta) / _add_strength_denom, 1.0) * 0.8
                                if strength_delta != 0.0 and _add_strength_denom > 0 else 0.0)
        order_ids = order_svc.send_order_split(
            instrument_id=instrument_id, volume=add_volume, price=price,
            direction=direction, action='OPEN', exchange=exchange,
            signal_strength=_add_signal_strength, bids=bids, asks=asks,
            open_reason='HIGH_FREQ',  # [FIX-20260712-S1] 改为HIGH_FREQ以使用60秒持仓/1分钟硬止损
            signal_id=pursuit_signal.get('signal_id', ''),
        )
        if order_ids:
            pe = hft.pursuit_engine
            confirmed = all(pe.add_platform_order_id(instrument_id, oid) for oid in order_ids)
            if not confirmed:
                logging.warning("[HFT] pursuit add: some add_platform_order_id failed for %s", instrument_id)
            # FIX-S1-DYNAMIC-COOLDOWN-20260730: 记录成功加仓(加仓也计入连续交易)
            pe.record_entry(instrument_id, is_add=True)
            logging.info("[HFT] pursuit add order placed: %s %s vol=%d price=%.2f order_ids=%s",
                         instrument_id, direction, add_volume, price, order_ids)
        else:
            logging.warning("[HFT] pursuit add order failed: %s %s vol=%d", instrument_id, direction, add_volume)
    except Exception as e:
        logging.warning("[HFT] _execute_pursuit_add failed: %s", e)


# DEL-S1-ARB-20260729: 套利交易全局状态和函数已删除(用户决策: 风险太大)
# S1-HFT仅保留 tick级期权排序 + tick级订单流 开仓
# _last_arbitrage_signal: Optional[Dict[str, Any]] = None
# _last_arbitrage_signal_ts: float = 0.0
# _ARBITRAGE_SIGNAL_TTL_SEC: float = 60.0
#
# def get_last_arbitrage_signal() -> Optional[Dict[str, Any]]: ... (已删除)
# def handle_arbitrage_signal(svc, arbitrage_signal, instrument_id) -> None: ... (已删除)
# 原handle_arbitrage_signal会将套利偏离信号通过SignalService.generate_signal(reason='hft_arbitrage_deviation')
# 生成OPEN_LONG/OPEN_SHORT信号并下单，现已完全移除


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
                    from signal.signal_service import SignalService
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
        except Exception as e:
            # FIX-60 D1: 扩展except为Exception并升级日志级别(debug→warning)
            logging.warning("[HFT] _handle_transition_signal error: %s", e)


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
                from signal.signal_service import SignalService
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
    except Exception as e:
        # FIX-60 D1: 扩展except为Exception并升级日志级别(debug→warning)
        logging.warning("[HFT] _handle_smart_money_signal error: %s", e)


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
    # FIX-S1-CONFIRM-TICKS-20260729: 确认tick计数器
    # 根因: evaluate_surge创建position后返回OPEN_POSITION,但execute_pursuit_entry的
    #   confirm_ticks门控(默认1<3)阻断入场。后续tick走ADD_POSITION路径无法重试入场
    #   → platform_confirmed永远False → 30秒后timeout → 854次timeout零下单
    # 修复: 追踪confirm_ticks,未确认时重试OPEN_POSITION(非ADD_POSITION)
    confirm_ticks: int = 0


# R27-P0-FC-01修复: 实盘硬时间止损检查入口函数
def clear_hard_time_stop_closing_flag(position_id: str) -> None:
    try:
        from risk.risk_circuit_breaker import get_safety_meta_layer
        safety = get_safety_meta_layer(None)
        if safety is not None and hasattr(safety, 'clear_closing_flag'):
            safety.clear_closing_flag(position_id)
    except Exception:
        pass


def check_hard_time_stop_for_position(risk_service, position_id: str, open_time: float,
                                       max_profit_reached: float, profit_slope: float = 0.0,
                                       peak_profit_pct: float = 0.0, current_profit_pct: float = 0.0,
                                       bar_time: Optional[float] = None, strategy_group: str = '') -> Optional[str]:
    """实盘两阶段硬时间止损检查入口，调用SafetyMetaLayer.check_position_hard_time_stop"""
    # FIX-62 D3 P0: 直接调用get_safety_meta_layer()工厂函数，不依赖risk_service._safety_meta_layer属性
    # 根因: RiskService.__init__ 未设置 self._safety_meta_layer 属性
    #       → getattr(risk_service, '_safety_meta_layer', None) 永远返回 None
    #       → check_hard_time_stop_for_position L605 直接 return None
    #       → 硬时间止损检查永远不执行（超时持仓无法止损）
    # 修复: 通过工厂函数获取 SafetyMetaLayer 实例，与 risk_service.py L349-351 调用方式一致
    safety = None
    try:
        from risk.risk_circuit_breaker import get_safety_meta_layer
        _sid = str(getattr(risk_service, '_scope_id', '') or getattr(risk_service, 'strategy_id', '') or 'global')
        safety = get_safety_meta_layer(None, strategy_id=_sid)
    except Exception as _safety_init_err:
        logging.warning("[R27-P0-FC-01] SafetyMetaLayer获取失败: %s", _safety_init_err)
    if safety is None:
        return None
    try:
        return safety.check_position_hard_time_stop(
            position_id, open_time, max_profit_reached,
            profit_slope, peak_profit_pct, current_profit_pct,
            bar_time=bar_time, strategy_group=strategy_group
        )
    except Exception as e:
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
        # FIX-S1-DYNAMIC-COOLDOWN-20260730: 基于交易表现的动态冷却
        self._instrument_stats: Dict[str, Dict[str, Any]] = {}
        self._cooldown_config = self._load_cooldown_config()

    def _load_cooldown_config(self) -> Dict[str, Any]:
        """FIX-S1-DYNAMIC-COOLDOWN-20260730: 读取动态冷却参数,失败时使用默认值"""
        defaults = {
            'trade_threshold': 10,
            'stop_loss_threshold': 2,
            'cooldown_sec': 10.0,
        }
        try:
            from config.config_params import get_param
            defaults['trade_threshold'] = int(get_param('hft_cooldown_trade_threshold', 10))
            defaults['stop_loss_threshold'] = int(get_param('hft_cooldown_stop_loss_threshold', 2))
            defaults['cooldown_sec'] = float(get_param('hft_dynamic_cooldown_sec', 10.0))
        except Exception as _cfg_err:
            logging.debug("[HFT] 动态冷却参数读取失败,使用默认值: %s", _cfg_err)
        return defaults

    def _should_cooldown(self, instrument_id: str) -> bool:
        """FIX-S1-DYNAMIC-COOLDOWN-20260730: 基于交易表现判断是否应冷却

        规则:
        - 已有未确认持仓的重试(confirm_ticks累积)跳过冷却
        - 冷却期内直接返回True
        - 连续交易>=trade_threshold 或 止损>=stop_loss_threshold 时触发新冷却
        """
        with self._lock:
            now = time.time()
            pos = self._positions.get(instrument_id)
            if pos and pos.is_open and not pos.platform_confirmed:
                return False
            stats = self._instrument_stats.setdefault(instrument_id, {
                'consecutive_trades': 0,
                'stop_loss_count': 0,
                'cooldown_until': 0.0,
            })
            if now < stats['cooldown_until']:
                return True
            if (stats['consecutive_trades'] >= self._cooldown_config['trade_threshold'] or
                    stats['stop_loss_count'] >= self._cooldown_config['stop_loss_threshold']):
                stats['cooldown_until'] = now + self._cooldown_config['cooldown_sec']
                stats['consecutive_trades'] = 0
                stats['stop_loss_count'] = 0
                logging.info("[HFT] 触发动态冷却: %s (cooldown_until=%.0f)",
                             instrument_id, stats['cooldown_until'])
                return True
            return False

    def record_entry(self, instrument_id: str, is_add: bool = False) -> None:
        """FIX-S1-DYNAMIC-COOLDOWN-20260730: 记录一次成功开仓/加仓"""
        with self._lock:
            stats = self._instrument_stats.setdefault(instrument_id, {
                'consecutive_trades': 0,
                'stop_loss_count': 0,
                'cooldown_until': 0.0,
            })
            stats['consecutive_trades'] += 1

    def record_exit(self, instrument_id: str, reason: str) -> None:
        """FIX-S1-DYNAMIC-COOLDOWN-20260730: 记录平仓;仅stop_loss计入止损计数"""
        with self._lock:
            stats = self._instrument_stats.setdefault(instrument_id, {
                'consecutive_trades': 0,
                'stop_loss_count': 0,
                'cooldown_until': 0.0,
            })
            if reason and 'stop_loss' in reason.lower():
                stats['stop_loss_count'] += 1

    def evaluate_surge(self, instrument_id: str, current_strength: float,
                       prev_strength: float, current_price: float,
                       direction: str, account_equity: float = 0.0,  # FIX-FAKE-EQUITY-20260731: 100000.0→0.0(fail-closed)
                       product: str = '') -> Optional[Dict[str, Any]]:
        if direction not in ('BUY', 'SELL'):
            logging.warning("[DynamicPursuitEngine] Invalid direction '%s', rejected", direction)
            return None
        if current_price <= 0:
            return None
        strength_delta = current_strength - prev_strength
        _effective_threshold = self._surge_threshold
        _signal_source = 'surge'  # FIX-S1-SUPerset: 信号来源标记(surge/level_resonance)
        # FIX-S1-DYNAMIC-COOLDOWN-20260730: 基于交易表现的动态冷却
        # 未确认持仓重试(confirm_ticks累积)在_should_cooldown内部被放行
        if self._should_cooldown(instrument_id):
            return None
        # FIX-S1-RESONANCE-REQUIRED-20260803: S1本意=共振+订单流，必须通过共振后才能开仓
        # 用户决策(2026-08-03): 无共振数据时不交易(tick级数据总会有共振数据)
        # 删除的降级路径(违背S1本意):
        #   A. 期货价格动量路径(FIX-S1-FUT-MOMENTUM-20260728): resonance=0时用价格突破开仓 → 无共振开仓
        #   B. 期权order_flow降级路径(FIX-S1-DEGRADE-20260730): resonance=0时用order_flow开仓 → 无共振开仓
        # 保留的路径(有共振数据):
        #   1. surge路径: strength_delta >= surge_threshold(0.3) — 共振跳变
        #   2. level_resonance路径: current_strength >= 0.45 — 共振稳态高位
        if current_strength == 0 and prev_strength == 0:
            # 无共振数据 → fail-closed不开仓(用户2026-08-03决策)
            _no_res_now = time.time()
            _no_res_last = self.__class__.__dict__.get('_s1_no_res_ts', {})
            if not isinstance(_no_res_last, dict):
                _no_res_last = {}
            _no_res_count = _no_res_last.get('_count', 0) + 1
            _no_res_last['_count'] = _no_res_count
            if _no_res_now - _no_res_last.get('_last_log', 0.0) >= 60:
                logging.info("[FIX-S1-RESONANCE-REQUIRED] 无共振数据,不开仓(fail-closed) inst=%s ×%d",
                             instrument_id, _no_res_count)
                _no_res_last['_last_log'] = _no_res_now
                _no_res_last['_count'] = 0
            setattr(self.__class__, '_s1_no_res_ts', _no_res_last)
            return None

        # FIX-S1-SUPerset-20260730: 绝对共振level路径(对齐S2)
        # 根因: S1测DELTA(变化量>=0.3), S2测LEVEL(绝对水平>=0.45), 正交条件
        #   当共振稳定在高位(如0.8): delta=0.0<0.3 → S1永不触发, 但S2持续触发
        #   → S1只在共振跳变瞬间触发一次, 稳态完全失活; S2在稳态持续触发 → 反转
        # 修复: 当current_strength>=0.45(对齐S2的_S2_RESONANCE_THRESH)时,
        #   即使delta<0.3也允许pursuit信号(30s冷却防重复入场)
        _level_threshold = 0.45  # 对齐S2的_S2_RESONANCE_THRESH
        _level_ok = current_strength >= _level_threshold
        if _level_ok and strength_delta < _effective_threshold:
            # FIX-S1-DYNAMIC-COOLDOWN-20260730: 稳态共振路径不再使用固定30秒冷却,
            #   统一由evaluate_surge入口的_should_cooldown基于交易表现决策。
            #   未确认持仓重试已在_should_cooldown内部放行。
            strength_delta = current_strength  # 用绝对level替代delta作为信号强度
            _signal_source = 'level_resonance'

        if strength_delta < _effective_threshold and not _level_ok:
            return None
        self._stats['surge_detected'] += 1
        # FIX-20260712-S1-P0: 为每次追击信号生成唯一signal_id，防止HFT订单重复/追踪断裂
        from infra.shared_utils import generate_prefixed_id as _gen_id
        _signal_id = f"PURSUIT_SIG_{instrument_id}_{int(time.time()*1000)}_{_gen_id('', 8)}"
        with self._lock:
            pos = self._positions.get(instrument_id)
            if pos and pos.is_open:
                if pos.direction != direction:
                    return None
                # FIX-S1-CONFIRM-TICKS-20260729: 未确认持仓重试入场(非加仓)
                # 根因: 首次evaluate_surge创建PursuitPosition(confirm_ticks=1)返回OPEN_POSITION,
                #   但execute_pursuit_entry的confirm_ticks门控(1<hft_confirm_ticks=3)阻断入场。
                #   后续tick走ADD_POSITION路径→execute_pursuit_add(有strength_delta bug)→
                #   platform_confirmed永远False→30秒后check_exit触发timeout→854次timeout零下单
                # 修复: 未确认持仓时递增confirm_ticks并返回OPEN_POSITION(非ADD_POSITION),
                #   使execute_pursuit_entry能在confirm_ticks>=hft_confirm_ticks时通过门控下单
                if not pos.platform_confirmed:
                    pos.confirm_ticks += 1
                    return {
                        'action': 'OPEN_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                        'volume': 1, 'price': current_price, 'stop_profit': pos.current_stop_profit,
                        'strength_delta': strength_delta, 'signal_id': _signal_id,
                        'confirm_ticks': pos.confirm_ticks,
                        'signal_source': _signal_source,  # FIX-S1-SUPerset: 信号来源标记
                    }
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
                pos.peak_strength = max(pos.peak_strength, current_strength, strength_delta)
                self._stats['total_pursuit_entries'] += 1
                return {
                    'action': 'ADD_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                    'volume': add_volume, 'price': current_price, 'new_stop_profit': new_stop_profit,
                    'total_volume': pos.total_volume, 'avg_price': pos.weighted_avg_price,
                    'strength_delta': strength_delta, 'signal_id': _signal_id,
                    'signal_source': _signal_source,  # FIX-S1-SUPerset: 信号来源标记
                }
            else:
                stop_profit = self._calc_initial_stop(current_price, direction)
                # FIX-R37-UNIQUE-ID: 增加随机熵，避免同毫秒同合约pos_id冲突导致持仓覆盖
                pos = PursuitPosition(
                    position_id=f"PURSUIT_{instrument_id}_{int(time.time()*1000)}_{_gen_id('', 8)}",
                    instrument_id=instrument_id, direction=direction,
                    entries=[{'price': current_price, 'volume': 1, 'strength': current_strength,
                              'strength_delta': strength_delta, 'timestamp': time.time(), 'entry_type': 'initial'}],
                    total_volume=1, weighted_avg_price=current_price,
                    current_stop_profit=stop_profit,
                    current_stop_loss=self._calc_initial_stop_loss(current_price, direction),
                    peak_strength=max(current_strength, strength_delta),
                    confirm_ticks=1,  # FIX-S1-CONFIRM-TICKS-20260729: 首次检测=1个确认tick
                )
                self._positions[instrument_id] = pos
                self._stats['total_pursuit_entries'] += 1
                return {
                    'action': 'OPEN_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                    'volume': 1, 'price': current_price, 'stop_profit': stop_profit,
                    'strength_delta': strength_delta, 'signal_id': _signal_id,
                    'confirm_ticks': 1,  # FIX-S1-CONFIRM-TICKS-20260729: 传递给execute_pursuit_entry门控
                    'signal_source': _signal_source,  # FIX-S1-SUPerset: 信号来源标记
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
                # V4-FIX-C6: platform_confirmed超时, 生成CLOSE信号(防止持仓泄漏), 而非静默关闭
                # 原则: 平台未确认≠交易所无持仓, 静默关闭(is_open=False+return None)会导致本地无持仓+交易所有持仓=泄漏
                # 修复: 返回CLOSE_ALL信号, 让调用方执行平仓操作(fail-safe, 宁可多平也不泄漏)
                _direction = pos.direction
                pos.is_open = False
                self._stats['positions_closed'] += 1
                _dpe_log_level = logging.DEBUG
                if not getattr(self, '_dpe_first_timeout_logged', False):
                    _dpe_log_level = logging.WARNING
                    self._dpe_first_timeout_logged = True
                logging.log(_dpe_log_level,
                    "[V4-FIX-C6] %s platform_confirmed超时(%.0fs), 生成CLOSE信号 (防止持仓泄漏) dir=%s",
                    instrument_id, pending_sec, _direction)
                pnl = self._calc_pnl(pos, current_price)
                return {
                    'action': 'CLOSE_ALL', 'instrument_id': instrument_id,
                    'direction': 'SELL' if _direction == 'BUY' else 'BUY',
                    'volume': pos.total_volume, 'price': current_price,
                    'reason': 'platform_confirmed_timeout', 'pnl': pnl,
                    'entries': len(pos.entries),
                    'platform_order_ids': list(pos.platform_order_ids),
                }
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
            self._cleanup_stale_stats()
            return {
                'service_name': 'DynamicPursuitEngine', **self._stats,
                'active_positions': sum(1 for p in self._positions.values() if p.is_open),
            }

    def _cleanup_stale_stats(self, max_entries: int = 200) -> None:
        """FIX-S1-DYNAMIC-COOLDOWN-20260730: 清理过期的instrument_stats

        防止长期运行时 _instrument_stats 无限增长。
        冷却期已过且无持仓的品种可安全清理。
        """
        now = time.time()
        # 清理 _instrument_stats: 冷却期已过且无持仓
        if len(self._instrument_stats) > max_entries:
            _active = {k for k, p in self._positions.items() if p.is_open}
            _stale = [k for k, v in self._instrument_stats.items()
                      if k not in _active and now >= v.get('cooldown_until', 0.0)]
            for k in _stale[:len(self._instrument_stats) - max_entries]:
                del self._instrument_stats[k]


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
# 识别位置: 函数内 from infra.shared_utils import ... 重复调用
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
        from strategy.tick_processing_service import (
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
