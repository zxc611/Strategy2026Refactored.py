# MODULE_ID: M1-279b
"""
Tick分发与聚合函数模块 - 从tick_processing_service.py拆分

包含所有顶级函数（原tick_aggregator_service和tick_dispatch_service的合并逻辑）：
- Tick聚合：is_option_instrument, _tick_time_to_epoch, check_hard_time_stop_live,
  process_tick_unified_path, process_tick_core
- Tick分发：tick_dispatch_layer, dispatch_tick_degraded, dispatch_tick,
  _dispatch_tick_inner, output_periodic_summary, flush_tick_buffer,
  save_tick_snapshot, atexit_flush, tick_preflight_check
"""

import logging
import math
import os
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from infra._helpers import get_logger
from infra.shared_utils import CHINA_TZ as _CHINA_TZ
from infra.event_bus import TickEvent, MarketEvent, BarCompletedEvent
from infra.resilience import (
    TimeoutGuard, Watchdog, HeartbeatMonitor,
)
from config import config_params
from strategy_judgment.causal_chain_utils import (
    CausalChainTracker, ContaminationGuard, CyclicDependencyGuard,
    validate_tick_cascade, CausalEvent,
)

_PRICE_TOLERANCE = 1e-6

_dispatch_locks: Dict[str, threading.Lock] = {}
_dispatch_locks_guard = threading.Lock()
_MAX_DISPATCH_LOCKS = 500

__all__ = [
    'is_option_instrument',
    'check_hard_time_stop_live',
    'process_tick_unified_path',
    'process_tick_core',
    'tick_dispatch_layer',
    'dispatch_tick_degraded',
    'dispatch_tick',
    'output_periodic_summary',
    'flush_tick_buffer',
    'save_tick_snapshot',
    'atexit_flush',
    'tick_preflight_check',
]

logger = get_logger(__name__)


# ========== Tick Aggregation (merged from tick_aggregator_service.py) ==========

def is_option_instrument(instrument_id: str) -> bool:
    if not instrument_id:
        return False
    _upper = instrument_id.upper()
    if any(p in _upper for p in ['-C', '-P', '_C', '_P', 'CALL', 'PUT']):
        return True
    m = re.match(r'^([A-Za-z]+\d+)([CP])(\d+)$', instrument_id)
    if m:
        return True
    return False


def _tick_time_to_epoch(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    if isinstance(value, datetime):
        try:
            if value.tzinfo is None:
                return value.replace(tzinfo=_CHINA_TZ).timestamp()
            return value.timestamp()
        except (TypeError, ValueError, OSError):
            return 0.0
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return 0.0
        try:
            return float(raw)
        except ValueError:
            pass
        try:
            if raw.endswith('Z'):
                raw = raw[:-1] + '+00:00'
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=_CHINA_TZ)
            return parsed.timestamp()
        except (TypeError, ValueError, OSError):
            return 0.0
    return 0.0


def check_hard_time_stop_live(svc, instrument_id: str) -> None:
    """每tick检查持仓的两阶段硬时间止损(实盘主链路入口)"""
    try:
        _risk_svc = svc._state_store.get_ref('_risk_service') if svc._state_store else None
        # FIX-71 D3 P0: 补全_risk_svc fallback，通过工厂函数获取
        # 根因: 如果ensure_position_service()提前return(因_position_service已存在)或
        #       _state_store注册失败，_risk_svc为None导致整个硬时间止损链路断裂
        #       （超时持仓无法止损）。FIX-65只处理了注册，未处理消费端fallback。
        if _risk_svc is None:
            try:
                from risk.risk_service import get_risk_service
                _sid = str(getattr(svc, 'strategy_id', '') or 'global')
                _risk_svc = get_risk_service(None, scope_id=_sid)
                if _risk_svc is not None and svc._state_store is not None:
                    svc._state_store.set_ref('_risk_service', _risk_svc)
            except Exception as _rs_fb_err:
                logging.debug("[R27-P0-FC-01] _risk_svc fallback获取失败: %s", _rs_fb_err)
        if _risk_svc is None:
            return
        _pos_svc = svc._state_store.get_ref('_position_service') if svc._state_store else None
        if _pos_svc is None:
            if svc._ensure_position_service_fn is not None:
                try:
                    svc._ensure_position_service_fn()
                except Exception as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
                _pos_svc = svc._state_store.get_ref('_position_service') if svc._state_store else None
        if _pos_svc is None:
            return
        positions = getattr(_pos_svc, 'positions', {})
        for _inst_id, pos_dict in list(positions.items()):
            # FIX-79: 内层循环也需list()快照，防止多线程修改pos_dict时
            # 抛出RuntimeError: dictionary changed size during iteration
            # 根因: 外层已用list()快照positions，但内层pos_dict.items()未快照
            #       当止损平仓导致持仓被删除时，pos_dict大小变化→RuntimeError
            #       → 整个止损检查被except Exception捕获→剩余持仓无法检查止损
            for _pid, rec in list(pos_dict.items()):
                # FIX-76 D1 P0: per-position异常隔离，单个持仓异常不影响其他持仓止损检查
                # 根因: 原代码内层循环无try/except，一个坏持仓(record格式异常/类型错误)会抛出异常
                #       被外层except Exception捕获，导致循环中断，剩余持仓不再检查止损
                #       → 超时持仓无法被检出，可能造成重大亏损
                try:
                    if isinstance(rec, dict):
                        _open_time = rec.get('open_time') or rec.get('created_at')
                        if _open_time is None:
                            continue
                        # FIX-77 D2 P1: 统一_open_time类型转换（支持datetime/string/numeric）
                        # 根因: dict路径未转换_open_time，若为ISO字符串(如"2026-07-17T10:00:00")
                        #       → check_hard_time_stop_for_position期望float → TypeError
                        _open_time = _tick_time_to_epoch(_open_time)
                        if _open_time <= 0:
                            continue
                        _max_profit = rec.get('max_profit_pct', 0.0)
                        _slope = rec.get('profit_slope', 0.0)
                        _peak = rec.get('peak_profit_pct', 0.0)
                        _current = rec.get('current_profit_pct', 0.0)
                        _sg = rec.get('strategy_group', '')
                    elif hasattr(rec, 'open_time'):
                        _open_time = rec.open_time
                        if _open_time is None:
                            continue
                        # FIX-77 D2 P1: 统一使用_tick_time_to_epoch转换（替代原仅datetime处理）
                        _open_time = _tick_time_to_epoch(_open_time)
                        if _open_time <= 0:
                            continue
                        _max_profit = getattr(rec, '_max_profit_pct', 0.0)
                        _slope = getattr(rec, 'profit_slope', 0.0)
                        _peak = getattr(rec, '_max_profit_pct', 0.0)
                        _current = 0.0
                        _cp = getattr(rec, 'current_price', 0.0)
                        _op = getattr(rec, 'open_price', 0.0)
                        if _op > 0 and _cp > 0:
                            _current = (_cp - _op) / _op if getattr(rec, 'volume', 0) > 0 else (_op - _cp) / _op
                        _sg = getattr(rec, 'strategy_group', '')
                    else:
                        continue
                    from strategy.tick_hft import check_hard_time_stop_for_position
                    _result = check_hard_time_stop_for_position(
                        _risk_svc, _pid, _open_time, _max_profit,
                        _slope, _peak, _current,
                        bar_time=svc._state_store.get('_current_bar_time') if svc._state_store else None,
                        strategy_group=_sg
                    )
                    if _result is not None:
                        # FIX-57: 硬时间止损触发后必须执行平仓，不能只记录日志
                        # 根因: 原代码仅logging.warning，持仓从未被平仓→每tick重复触发705次
                        # 修复: 1)检查是否已有平仓单在挂(去重) 2)调用_trigger_close_position执行平仓
                        _already_closing = False
                        if isinstance(rec, dict):
                            _already_closing = bool(rec.get('closing_order_id')) or bool(rec.get('_closing', False))
                        elif hasattr(rec, 'closing_order_id'):
                            # FIX-57b: closing_order_id默认值是空字符串''，is not None恒为True导致平仓永远跳过
                            # 修复: 改用bool()真值检查，空字符串=未平仓，非空=平仓中
                            _already_closing = bool(getattr(rec, 'closing_order_id', '')) or bool(getattr(rec, '_closing', False))
                        if _already_closing:
                            logging.debug("[R27-P0-FC-01] 持仓%s已在平仓中，跳过重复止损触发", _pid)
                            continue
                        logging.warning("[R27-P0-FC-01] 硬时间止损触发: position=%s reason=%s", _pid, _result)
                        # 执行平仓
                        try:
                            _close_fn = getattr(_pos_svc, '_trigger_close_position', None)
                            if _close_fn is None:
                                _close_fn = getattr(_pos_svc, 'close_position', None)
                            if _close_fn is not None:
                                _current_price = rec.get('current_price', 0.0) if isinstance(rec, dict) else getattr(rec, 'current_price', 0.0)
                                _close_fn(rec, _result, current_price=_current_price)
                                logging.info("[FIX-57] 硬时间止损平仓已执行: position=%s reason=%s", _pid, _result)
                                try:
                                    from strategy.tick_hft import clear_hard_time_stop_closing_flag
                                    clear_hard_time_stop_closing_flag(_pid)
                                except Exception:
                                    pass
                            else:
                                logging.error("[FIX-57] _pos_svc无_trigger_close_position方法，无法平仓: position=%s", _pid)
                        except Exception as _close_err:
                            logging.error("[FIX-57] 硬时间止损平仓执行异常: position=%s err=%s", _pid, _close_err, exc_info=True)
                except Exception as _pos_err:
                    logging.warning("[R27-P0-FC-01] 持仓止损检查异常(跳过该持仓) position=%s: %s", _pid, _pos_err)
    except Exception as _hts_err:
        # FIX-71 D1 P0: 窄except扩展为Exception，避免吞掉MemoryError/ConnectionError/TimeoutError等
        logging.error("[R22-P0-NEW] 硬时间止损检查整体异常(超时持仓可能无法止损!): %s", _hts_err, exc_info=True)


def process_tick_unified_path(svc, tick: Any) -> None:
    """统一实盘/回测Tick处理主路径"""
    process_tick_core(svc, tick)
    try:
        _norm_tick = svc._normalize_tick(tick)
        _inst_id = _norm_tick.get('instrument_id', '')
        if _inst_id and is_option_instrument(_inst_id):
            from risk.risk_service import get_risk_service
            _rs = get_risk_service()
            _gc = _rs._get_greeks_calculator() if hasattr(_rs, '_get_greeks_calculator') else None
            if _gc is not None:
                _tick_price = float(_norm_tick.get('last_price', 0))
                _underlying_price = float(_norm_tick.get('underlying_price', 0))
                _bar_ts = svc._state_store.get('_current_bar_time') if svc._state_store else None
                _bar_date = None
                if _bar_ts:
                    from datetime import date as _date
                    _bar_date = _date.fromtimestamp(_bar_ts)
                if _tick_price > 0 and _underlying_price > 0:
                    _gc.update_greeks_from_tick(
                        instrument_id=_inst_id,
                        price=_tick_price,
                        underlying_price=_underlying_price,
                        expiry_date=None,
                        option_type=None,
                        future_product=None,
                        strike=None,
                        iv=None,
                        bar_date=_bar_date,
                    )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _gc_err:
        logging.debug("[GreeksIntegration] update_greeks_from_tick failed: %s", _gc_err)


def process_tick_core(svc, tick: Any) -> None:
    """Tick数据处理核心逻辑（从TickProcessingService._process_tick提取）"""
    try:
        _causal_tracker = CausalChainTracker.get_instance()
        _correlation_id = _causal_tracker.new_correlation_id()

        exchange = svc._get_tick_field(tick, 'exchange', '')
        instrument_id = svc._get_tick_field(tick, 'instrument_id', '')
        last_price = svc._get_tick_field(tick, 'last_price', 0.0)
        volume = svc._get_tick_field(tick, 'volume', 0)

        # R24-P0-IV-01修复: tick入口NaN/Inf/负值过滤
        # FIX-20260701: price<=0不再丢弃tick（期权快照last_price=0是合法数据）
        # 仅丢弃NaN/Inf（真正无效数据），price<=0改为警告保留
        _tick_valid, _safe_price, _safe_vol = validate_tick_cascade(last_price, volume, instrument_id)
        _price_is_nan_inf = False
        try:
            _pf = float(last_price) if last_price is not None else None
            _price_is_nan_inf = _pf is not None and (math.isnan(_pf) or math.isinf(_pf))
        except (TypeError, ValueError):
            _price_is_nan_inf = True
        if not _tick_valid:
            if _price_is_nan_inf:
                try:
                    from infra.health_monitor import record_tick_probe
                    record_tick_probe('validate_drop', instrument_id, last_price, f'price={last_price} vol={volume}')
                except Exception:
                    pass
                try:
                    _causal_tracker.record_event(CausalEvent(
                        correlation_id=_correlation_id,
                        event_type="tick_contamination",
                        source_module="tick_processing_service",
                        contamination_flag=True,
                        payload_summary="invalid price=%s vol=%s inst=%s" % (last_price, volume, instrument_id),
                    ))
                    ContaminationGuard.get_instance().cascade_cleanup(instrument_id)
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
                if not hasattr(svc, '_iv01_drop_count'):
                    svc._iv01_drop_count = 0
                svc._iv01_drop_count += 1
                if svc._iv01_drop_count <= 10 or svc._iv01_drop_count % 1000 == 0:
                    logging.warning("[R24-P0-IV-01] Tick dropped(NaN/Inf): price=%s vol=%s inst=%s (total=%d)",
                                   last_price, volume, instrument_id, svc._iv01_drop_count)
                return
            else:
                try:
                    from infra.health_monitor import record_tick_probe
                    record_tick_probe('validate_drop', instrument_id, last_price, f'price<=0 preserved price={last_price} vol={volume}')
                except Exception:
                    pass
                if not hasattr(svc, '_iv01_price_zero_count'):
                    svc._iv01_price_zero_count = 0
                svc._iv01_price_zero_count += 1
                if svc._iv01_price_zero_count <= 10 or svc._iv01_price_zero_count % 1000 == 0:
                    logging.warning("[FIX-20260701] Tick price<=0 preserved: price=%s vol=%s inst=%s (total=%d)",
                                   last_price, volume, instrument_id, svc._iv01_price_zero_count)
        try:
            from infra.health_monitor import record_tick_probe
            record_tick_probe('validate_pass', instrument_id, last_price)
        except Exception:
            pass
        last_price = _safe_price
        if _safe_vol > 0:
            volume = _safe_vol
        if (not isinstance(volume, (int, float))
            or math.isnan(volume) or math.isinf(volume) or volume < 0):
            if not hasattr(svc, '_iv01_vol_drop_count'):
                svc._iv01_vol_drop_count = 0
            svc._iv01_vol_drop_count += 1
            if svc._iv01_vol_drop_count <= 10 or svc._iv01_vol_drop_count % 1000 == 0:
                logging.warning("[R24-P0-IV-01] Tick dropped: invalid volume=%s instrument=%s (total_dropped=%d)",
                               volume, instrument_id, svc._iv01_vol_drop_count)
            volume = 0

        # R24-P1-IV-09修复: tick时间戳单调性验证
        # FIX-20260627: 模拟交易环境tick推送不保证时间单调，非单调tick改为警告而非丢弃
        source_timestamp = svc._get_tick_field(tick, 'datetime', 0.0)
        _ts_float = _tick_time_to_epoch(source_timestamp)
        if instrument_id and _ts_float > 0:
            _last_ts = svc._last_tick_timestamp.get(instrument_id, 0.0)
            if _last_ts > 0 and _ts_float < _last_ts:
                svc._tick_timestamp_violation_count += 1
                if svc._state_store is not None:
                    svc._state_store.set('_tick_timestamp_violation_count', svc._tick_timestamp_violation_count)
                _replay_skew_sec = float(os.environ.get('ALI2026_REPLAY_CLOCK_SKEW_SEC', '300'))
                _is_replay_clock = time.time() - max(_ts_float, _last_ts) > _replay_skew_sec
                _log_every = 100000 if _is_replay_clock else 10000
                if svc._tick_timestamp_violation_count <= 10 or svc._tick_timestamp_violation_count % _log_every == 0:
                    logging.warning(
                        "[R24-P1-IV-09] Tick时间戳非单调: instrument=%s current_ts=%.6f < last_ts=%.6f, 累计%d次 (replay_clock=%s, 警告，继续处理)",
                        instrument_id, _ts_float, _last_ts, svc._tick_timestamp_violation_count, _is_replay_clock,
                    )
                if _is_replay_clock:
                    svc._last_tick_timestamp[instrument_id] = _ts_float
                else:
                    svc._last_tick_timestamp[instrument_id] = max(_ts_float, _last_ts)
            else:
                svc._last_tick_timestamp[instrument_id] = _ts_float

        _prev_price = svc._price_jump_last_price.get(instrument_id)
        if _prev_price is not None and _prev_price > 0:
            _jump_pct = abs(last_price - _prev_price) / _prev_price
            _is_option = is_option_instrument(instrument_id)
            _threshold = svc._price_jump_threshold_option if _is_option else svc._price_jump_threshold
            if _jump_pct > _threshold:
                svc._price_jump_count += 1
                if svc._state_store is not None:
                    svc._state_store.set('_price_jump_count', svc._price_jump_count)
                try:
                    from infra.health_monitor import record_tick_probe
                    record_tick_probe('price_jump_warn', instrument_id, last_price, f'{_prev_price}->{last_price} ({_jump_pct*100:.1f}%)')
                except Exception:
                    pass
                if svc._price_jump_count <= 3 or svc._price_jump_count % 10000 == 0:
                    logging.warning("[R24-P0-IV-07] Price jump detected: %s %.2f->%.2f (%.1f%% change, total_jumps=%d) - tick preserved",
                                   instrument_id, _prev_price, last_price, _jump_pct*100, svc._price_jump_count)
        svc._price_jump_last_price[instrument_id] = last_price

        # R23-FR-04-FIX: K线过期淘汰
        # FIX-20260627: 模拟交易环境tick时间戳是历史交易时间，time.time()-tick_epoch远超阈值
        # 导致所有tick被误判为过期丢弃。改为：仅淘汰"比同合约上一个tick更旧"的tick（真正过期），
        # 而非与系统时间比较。同时保留未来时间戳检查（明显异常）。
        try:
            from config.config_service import get_cached_params
            _cp = get_cached_params()
            _kline_max_age = _cp.get('kline_max_age_sec', 60) if isinstance(_cp, dict) else 60
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            _kline_max_age = 60
        if hasattr(tick, 'datetime') or hasattr(tick, 'update_time'):
            _tick_ts = getattr(tick, 'datetime', None) or getattr(tick, 'update_time', None)
            if _tick_ts is not None:
                try:
                    _tick_epoch = _tick_time_to_epoch(_tick_ts)
                    if _tick_epoch > 0:
                        _stale_warn = False
                        _stale_reason = ''
                        if _tick_epoch > time.time() + 300:
                            _stale_warn = True
                            _stale_reason = "future_tick"
                        else:
                            _prev_tick_epoch = getattr(svc, '_kline_age_last_tick_epoch', {}).get(instrument_id, 0.0)
                            if _prev_tick_epoch > 0 and _tick_epoch < _prev_tick_epoch - _kline_max_age:
                                _replay_skew_sec = float(os.environ.get('ALI2026_REPLAY_CLOCK_SKEW_SEC', '300'))
                                _is_replay_clock = time.time() - max(_tick_epoch, _prev_tick_epoch) > _replay_skew_sec
                                if _is_replay_clock:
                                    if not hasattr(svc, '_kline_replay_regression_count'):
                                        svc._kline_replay_regression_count = 0
                                    svc._kline_replay_regression_count += 1
                                    if svc._kline_replay_regression_count <= 3 or svc._kline_replay_regression_count % 100000 == 0:
                                        logging.warning(
                                            "[R23-FR-04-FIX] 回放/模拟时钟回退: inst=%s current=%.1f prev=%.1f count=%d",
                                            instrument_id, _tick_epoch, _prev_tick_epoch, svc._kline_replay_regression_count,
                                        )
                                else:
                                    _stale_warn = True
                                    _stale_reason = "stale_vs_prev"
                        if _stale_warn:
                            try:
                                from infra.health_monitor import record_tick_probe
                                record_tick_probe('kline_age_warn', instrument_id, last_price, f'reason={_stale_reason} epoch={_tick_epoch:.1f}')
                            except Exception:
                                pass
                            if not hasattr(svc, '_kline_age_warn_count'):
                                svc._kline_age_warn_count = 0
                            svc._kline_age_warn_count += 1
                            if svc._kline_age_warn_count <= 10 or svc._kline_age_warn_count % 1000 == 1:
                                logging.debug("[R23-FR-04-FIX] K线过期警告(tick保留): inst=%s reason=%s tick_epoch=%.1f warn_count=%d",
                                              instrument_id, _stale_reason, _tick_epoch, svc._kline_age_warn_count)
                        if not hasattr(svc, '_kline_age_last_tick_epoch'):
                            svc._kline_age_last_tick_epoch = {}
                        svc._kline_age_last_tick_epoch[instrument_id] = _tick_epoch
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _kage_err:
                    logging.debug("[R22-P1-NEW] K线年龄校验异常: %s", _kage_err)

        # [FR-P1-03-FIX] 指标数据年龄校验
        _last_indicator_time = svc._state_store.get('_last_indicator_time') if svc._state_store else 0
        if _last_indicator_time and _last_indicator_time > 0:
            _indicator_age = time.time() - _last_indicator_time
            if _indicator_age > _kline_max_age:
                if not hasattr(svc, '_indicator_age_warn_count'):
                    svc._indicator_age_warn_count = 0
                svc._indicator_age_warn_count += 1
                if svc._indicator_age_warn_count <= 10 or svc._indicator_age_warn_count % 1000 == 1:
                    logging.debug("[FR-P1-03-FIX] 指标数据年龄过期: age=%.1fs > max_age=%ds, inst=%s",
                                  _indicator_age, _kline_max_age, instrument_id)
        if svc._state_store is not None:
            svc._state_store.set('_last_indicator_time', time.time())

        # [FR-P1-04-FIX] bar时间一致性校验
        _current_bar_time = svc._state_store.get('_current_bar_time') if svc._state_store else None
        if _current_bar_time is not None:
            _bar_tick_diff = abs(time.time() - _current_bar_time)
            _bar_consistency_threshold = _kline_max_age * 2
            if _bar_tick_diff > _bar_consistency_threshold:
                if not hasattr(svc, '_bar_time_warn_count'):
                    svc._bar_time_warn_count = 0
                svc._bar_time_warn_count += 1
                if svc._bar_time_warn_count <= 10:
                    logging.info("[FR-P1-04-FIX] bar时间不一致: diff=%.1fs > threshold=%ds, inst=%s",
                                   _bar_tick_diff, _bar_consistency_threshold, instrument_id)

        # 分子统计：平台推送过数据的合约
        if instrument_id:
            sm = None
            _stor = svc._state_store.get_ref('storage') if svc._state_store else None
            if _stor is not None and hasattr(_stor, 'subscription_manager'):
                sm = _stor.subscription_manager
            if sm is None:
                try:
                    from data.data_service import get_data_service
                    _ds = get_data_service()
                    if _ds is not None:
                        sm = getattr(_ds, 'subscription_manager', None)
                except Exception:
                    sm = None
            if sm and hasattr(sm, 'record_tick_received'):
                sm.record_tick_received(instrument_id)

        # 详细日志输出
        now_ts = time.time()

        # 订单流数据喂入
        try:
            if not hasattr(svc, '_order_flow_bridge'):
                from order.order_flow_bridge import get_order_flow_bridge
                svc._order_flow_bridge = get_order_flow_bridge()
            bid_price = svc._get_tick_field(tick, 'bid_price1', 0.0)
            ask_price = svc._get_tick_field(tick, 'ask_price1', 0.0)
            mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else last_price

            l2_bids = []
            l2_asks = []
            for i in range(1, 6):
                bp = svc._get_tick_field(tick, f'bid_price{i}', 0.0)
                ap = svc._get_tick_field(tick, f'ask_price{i}', 0.0)
                bv = svc._get_tick_field(tick, f'bid_volume{i}', 0)
                av = svc._get_tick_field(tick, f'ask_volume{i}', 0)
                if bp > 0 and bv > 0:
                    l2_bids.append((bp, bv))
                if ap > 0 and av > 0:
                    l2_asks.append((ap, av))

            svc._order_flow_bridge.on_tick_feed(
                instrument_id, last_price, volume, exchange,
                bid_price=bid_price, ask_price=ask_price,
                mid_price=mid_price,
                l2_bids=l2_bids, l2_asks=l2_asks,
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as ofb_e:
            if not hasattr(svc, '_ofb_error_logged'):
                svc._ofb_error_logged = True
                logging.debug("[_process_tick] OrderFlowBridge feed error: %s", ofb_e)

        if (now_ts - svc._tick_debug_last_ts >= 10.0) and instrument_id:
            diag_on = False
            try:
                from infra.health_monitor import is_monitored_contract
                diag_on = is_monitored_contract(instrument_id)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.debug("[_process_tick] Failed to check monitored contract: %s", e)
                diag_on = False
            if diag_on:
                logging.info("[TickDiag] inst=%s exch=%s price=%s vol=%s", instrument_id, exchange, last_price, volume)
            svc._tick_debug_last_ts = now_ts

        # debug_output日志
        params_dict = svc._state_store.get('params') if svc._state_store else {}
        if isinstance(params_dict, dict):
            debug_output = params_dict.get('debug_output', False)
        else:
            debug_output = getattr(params_dict, 'debug_output', False) if params_dict else False

        if debug_output and instrument_id:
            logging.debug("[Tick] %s.%s price=%s vol=%s", exchange, instrument_id, last_price, volume)

        # SubscriptionManager统一Tick入口
        _sub_mgr = None
        _stor2 = svc._state_store.get_ref('storage') if svc._state_store else None
        if _stor2 is None:
            try:
                from data.data_service import get_data_service
                _stor2 = get_data_service()
            except Exception:
                pass
        if _stor2 is not None:
            _sub_mgr = getattr(_stor2, 'subscription_manager', None)
        # FIX-M9-1: per-tick flag防止on_option_tick双重路由(Step A与Step B都调用TType)
        svc._tts_routed_this_tick = False
        if _sub_mgr:
            try:
                _sub_mgr.on_tick(instrument_id, last_price, volume, is_replay=False)
                svc._tts_routed_this_tick = True
                from infra.health_monitor import record_tick_probe
                record_tick_probe('submgr_route', instrument_id, last_price)
                record_tick_probe('dispatched', instrument_id, last_price)
                if not hasattr(svc, '_tick_route_confirmed'):
                    svc._tick_route_confirmed = True
                    logging.info("[TickRoute] Tick数据成功路由到SubscriptionManager: %s", instrument_id)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as exc:
                from infra.health_monitor import record_tick_probe
                record_tick_probe('error', instrument_id, last_price, str(exc))
                logging.warning("[R16-P1-8.1] SubscriptionManager分发异常: %s", exc)
        else:
            try:
                from infra.health_monitor import record_tick_probe
                record_tick_probe('submgr_miss', instrument_id, last_price, 'sub_mgr_is_None')
            except Exception:
                pass

            # FIX: _sub_mgr为None时（storage未注入state_store），直接调用TTypeService
            # 确保5种状态分类能收到tick数据，否则所有期权恒为'other'
            # FIX2: 必须剥离交易所前缀（如CFFEX.IH2609->IH2609），否则get_instrument_meta_by_id返回None
            try:
                from data.t_type_service import get_t_type_service
                from config.params_service import get_params_service
                import re as _re
                _tts = get_t_type_service()
                _ps = get_params_service()
                # FIX-R20-DIAG: 检查_tts/_ps/instrument_id的值
                if not hasattr(svc, '_tts_diag_logged'):
                    svc._tts_diag_logged = True
                    logging.warning("[TickRoute-DIAG] tts=%s ps=%s inst=%r tts_type=%s ps_type=%s",
                                  _tts is not None, _ps is not None, instrument_id, type(_tts).__name__, type(_ps).__name__)
                # FIX-R20-C: 用 `is not None` 替代 `bool()`，因为 ParamsService.__len__ 在 _params 为空时返回0导致 bool()=False
                if _tts is not None and _ps is not None and instrument_id:
                    # 剥离交易所前缀，与SubscriptionManager._strip_exchange_prefix保持一致
                    _norm_match = _re.search(r'([A-Za-z]+\d+.*?)$', str(instrument_id).strip())
                    _norm_id = _norm_match.group(1) if _norm_match else instrument_id
                    _meta = _ps.get_instrument_meta_by_id(_norm_id)
                    if not _meta and _norm_id != instrument_id:
                        _meta = _ps.get_instrument_meta_by_id(instrument_id)
                    if _meta:
                        _inst_type = _meta.get('type', '')
                        # FIX-R20: 诊断日志 - 首次路由到future/option时记录meta详情
                        if _inst_type == 'future' and not hasattr(svc, '_tts_future_routed'):
                            svc._tts_future_routed = True
                            logging.info("[TickRoute] 首次路由future: inst=%s norm=%s type=%s internal_id=%s",
                                       instrument_id, _norm_id, _inst_type, _meta.get('internal_id'))
                        elif _inst_type == 'option' and not hasattr(svc, '_tts_option_routed'):
                            svc._tts_option_routed = True
                            logging.info("[TickRoute] 首次路由option: inst=%s norm=%s type=%s internal_id=%s",
                                       instrument_id, _norm_id, _inst_type, _meta.get('internal_id'))
                        elif _inst_type not in ('future', 'option') and not hasattr(svc, '_tts_unknown_type'):
                            svc._tts_unknown_type = True
                            logging.warning("[TickRoute] 未知type: inst=%s norm=%s type=%r meta_keys=%s",
                                          instrument_id, _norm_id, _inst_type, list(_meta.keys()))
                        if _inst_type == 'option':
                            _tts.on_option_tick(_norm_id, float(last_price), float(volume))
                            svc._tts_routed_this_tick = True
                        elif _inst_type == 'future':
                            _fid = _meta.get('internal_id')
                            if _fid is not None:
                                _tts.on_future_instrument_tick(int(_fid), float(last_price))
                                svc._tts_routed_this_tick = True
                            else:
                                if not hasattr(svc, '_tts_fid_none_logged'):
                                    svc._tts_fid_none_logged = True
                                    logging.warning("[TickRoute] future internal_id为None: inst=%s meta=%s",
                                                  instrument_id, _meta)
                    else:
                        # meta未找到：计数器日志（每1000次输出一次）
                        if not hasattr(svc, '_tts_meta_miss_count'):
                            svc._tts_meta_miss_count = 0
                        svc._tts_meta_miss_count += 1
                        if svc._tts_meta_miss_count <= 3 or svc._tts_meta_miss_count % 1000 == 1:
                            logging.warning("[TickRoute] meta未找到: inst=%s norm=%s (count=%d)",
                                          instrument_id, _norm_id, svc._tts_meta_miss_count)
                # 计数器日志：每1000次输出一次路由确认
                if not hasattr(svc, '_tts_fallback_count'):
                    svc._tts_fallback_count = 0
                svc._tts_fallback_count += 1
                if svc._tts_fallback_count == 1 or svc._tts_fallback_count % 1000 == 0:
                    logging.info("[TickRoute] TTypeService直连兜底: inst=%s (total=%d)",
                               instrument_id, svc._tts_fallback_count)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _tts_err:
                if not hasattr(svc, '_tts_fallback_err_logged'):
                    svc._tts_fallback_err_logged = True
                    logging.warning("[TickRoute] TTypeService直连兜底失败: %s", _tts_err)

        if _sub_mgr and not hasattr(svc, '_tick_route_confirmed'):
            svc._tick_route_confirmed = True
            logging.info("[TickRoute] Tick数据成功路由到subscription_manager: %s", instrument_id)

        # FIX-20260701: 多源读取_historical_load_in_progress，兼容strategy_historical.py的属性设置
        # 根因: strategy_historical.py设置self._historical_load_in_progress(策略实例属性)，
        #       但tick_dispatch.py只从state_store读取，两者不同步，导致降级路径判断错误。
        _historical_load_in_progress = False
        if svc._state_store:
            _historical_load_in_progress = svc._state_store.get('_historical_load_in_progress', False)
        if not _historical_load_in_progress:
            # 兼容: 从策略实例属性读取
            _strat_ref = None
            if svc._state_store:
                _strat_ref = svc._state_store.get_ref('strategy') or svc._state_store.get_ref('strategy_instance') or svc._state_store.get_ref('strategy_obj')
            if _strat_ref is not None:
                _historical_load_in_progress = getattr(_strat_ref, '_historical_load_in_progress', False)

        if bool(_historical_load_in_progress):
            try:
                from infra.health_monitor import record_tick_probe
                record_tick_probe('hist_degraded', instrument_id, last_price, f'hist_load={_historical_load_in_progress}')
                record_tick_probe('dispatched', instrument_id, last_price)
            except Exception:
                pass
            svc._dispatch_tick_degraded(tick, instrument_id, last_price, volume, exchange)
        else:
            try:
                from infra.health_monitor import record_tick_probe
                record_tick_probe('dispatch_normal', instrument_id, last_price)
                record_tick_probe('dispatched', instrument_id, last_price)
            except Exception:
                pass
            svc._dispatch_tick(tick, instrument_id, last_price, volume, exchange)

        # R23-SM-09-FIX: 每个tick处理时检测HMM状态驻留异常
        _hmm_state = svc._state_store.get('_hmm_current_state') if svc._state_store else None
        if _hmm_state is not None:
            from strategy.tick_hft import check_hmm_dwell_anomaly
            check_hmm_dwell_anomaly(svc, _hmm_state)

        if not bool(_historical_load_in_progress):
            try:
                if not hasattr(svc, '_pullback_mgr_ref'):
                    try:
                        _bss = svc._state_store.get_ref('_box_spring') if svc._state_store else None
                        if _bss is None:
                            _bss = svc._state_store.get_ref('_spring') if svc._state_store else None
                        if _bss is None:
                            _bss = svc._state_store.get_ref('_box_spring_strategy') if svc._state_store else None
                        if _bss and hasattr(_bss, '_pullback_mgr'):
                            svc._pullback_mgr_ref = _bss._pullback_mgr
                        else:
                            svc._pullback_mgr_ref = None
                            logging.debug("[R22-TS-02] pullback_mgr未找到，属性搜索完成")
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _attr_err:
                        svc._pullback_mgr_ref = None
                        logging.debug("[R22-TS-02] pullback_mgr属性查找异常: %s", _attr_err)
                _pbm = svc._pullback_mgr_ref if hasattr(svc, '_pullback_mgr_ref') else None
                if _pbm and getattr(_pbm, 'tick_tracking', False) and instrument_id and last_price > 0:
                    retriggered = _pbm.process_tick(instrument_id, last_price)
                    if retriggered:
                        logging.debug("[P0-2] PullbackManager.process_tick retriggered %d signals for %s", len(retriggered), instrument_id)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _pbm_err:
                if not hasattr(svc, '_pbm_err_logged'):
                    svc._pbm_err_logged = True
                    logging.debug("[P0-2] PullbackManager.process_tick integration error: %s", _pbm_err)

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.error("[TickProcessingService._process_tick] Error: %s", e)
        logging.exception("[TickProcessingService._process_tick] Stack:")
        if not hasattr(svc, '_tick_drop_count'):
            svc._tick_drop_count = 0
        svc._tick_drop_count += 1
        if svc._tick_drop_count <= 10 or svc._tick_drop_count % 1000 == 0:
            logging.warning("[TickDrop] Total ticks dropped due to unhandled errors: %d", svc._tick_drop_count)


# ========== Tick Dispatch (merged from tick_dispatch_service.py) ==========

def _get_dispatch_lock(instrument_id: str) -> threading.Lock:
    """获取per-instrument分发锁"""
    lock = _dispatch_locks.get(instrument_id)
    if lock is not None:
        return lock
    with _dispatch_locks_guard:
        lock = _dispatch_locks.get(instrument_id)
        if lock is None:
            if len(_dispatch_locks) >= _MAX_DISPATCH_LOCKS:
                # 清理最早的一半锁（低频品种）'
                keys = list(_dispatch_locks.keys())[:_MAX_DISPATCH_LOCKS // 2]
                for k in keys:
                    del _dispatch_locks[k]
            lock = threading.Lock()
            _dispatch_locks[instrument_id] = lock
    return lock


def tick_dispatch_layer(svc, tick: Any, instrument_id_raw: str) -> None:
    from infra.subscription_service import SubscriptionManager

    try:
        normalized_tick = svc._normalize_tick(tick)
        exchange = normalized_tick.get('exchange', '')
        instrument_id = normalized_tick.get('instrument_id', '')

        # [FIX-20260708-TICK-NO-DROP] 背压降级：过载时走轻量路径
        _degraded = getattr(svc, '_tick_degraded_mode', False)
        # FIX-MARKET-CLOSE: 收盘后交易所通道关闭，止损平仓订单也无法提交
        _market_closed = False
        try:
            from infra.scheduler_service import is_market_open
            if not is_market_open():
                _market_closed = True
        except (ImportError, AttributeError, TypeError):
            pass
        if _degraded:
            # 轻量路径：仅更新价格缓存+止损检查，跳过classify等重操作
            _last_price = float(normalized_tick.get('last_price', 0))
            _volume = int(normalized_tick.get('volume', 0))
            dispatch_tick_degraded(svc, tick, instrument_id, _last_price, _volume, exchange)
            # FIX-MARKET-CLOSE: 收盘后跳过止损检查（交易所关闭，止损订单无法执行）
            if not _market_closed:
                check_hard_time_stop_live(svc, instrument_id)
            _ss_stats = svc._state_store.get_ref('_stats') if svc._state_store else None
            if _ss_stats is not None:
                _ss_stats['last_tick_path'] = 'degraded'
        else:
            process_tick_unified_path(svc, tick)

            _ss_stats = svc._state_store.get_ref('_stats') if svc._state_store else None
            if _ss_stats is not None:
                _ss_stats['last_tick_path'] = 'unified'

            # FIX-MARKET-CLOSE: 收盘后跳过止损检查（交易所关闭，止损订单无法执行）
            if not _market_closed:
                check_hard_time_stop_live(svc, instrument_id)

        if instrument_id:
            _e2e_first_tick_set = svc._state_store.get_ref('_e2e_first_tick_set') if svc._state_store else None
            _e2e_counters = svc._state_store.get_ref('_e2e_counters') if svc._state_store else None
            _lock = svc._state_store.get_ref('_lock') if svc._state_store else None
            if _e2e_first_tick_set is not None and _lock is not None:
                if instrument_id not in _e2e_first_tick_set:
                    with _lock:
                        if instrument_id not in _e2e_first_tick_set:
                            _e2e_first_tick_set.add(instrument_id)
                            if _e2e_counters is not None:
                                _e2e_counters['first_tick_received'] += 1

        with svc._tick_stats_lock:
            if _ss_stats is not None:
                _ss_stats['total_ticks'] += 1
            svc._tick_count += 1
            if svc._state_store is not None:
                svc._state_store.set('_tick_count', svc._tick_count)

            instrument_type = 'option' if SubscriptionManager.is_option(instrument_id) else 'future'
            if _ss_stats is not None:
                _ss_stats['tick_by_type'][instrument_type] += 1

                if exchange:
                    if exchange not in _ss_stats['tick_by_exchange']:
                        _ss_stats['tick_by_exchange'][exchange] = 0
                    _ss_stats['tick_by_exchange'][exchange] += 1

                if instrument_id:
                    if instrument_id not in _ss_stats['tick_by_instrument']:
                        _ss_stats['tick_by_instrument'][instrument_id] = 0
                    _ss_stats['tick_by_instrument'][instrument_id] += 1

        current_time = time.time()
        if current_time - svc._last_tick_log_time >= svc._tick_log_interval:
            output_periodic_summary(svc)
            svc._last_tick_log_time = current_time

    except Exception as e:
        _tick_dispatch_err_count = getattr(svc, '_tick_dispatch_err_count', 0) + 1
        svc._tick_dispatch_err_count = _tick_dispatch_err_count
        if _tick_dispatch_err_count <= 3 or _tick_dispatch_err_count % 1000 == 0:
            logging.error("[R21-CC-P1-05] Tick分发层统一异常(%d次): instrument=%s error=%s", _tick_dispatch_err_count, instrument_id_raw, e)
        _lock = svc._state_store.get_ref('_lock') if svc._state_store else None
        _ss_stats = svc._state_store.get_ref('_stats') if svc._state_store else None
        if _lock is not None and _ss_stats is not None:
            with _lock:
                _ss_stats['errors_count'] += 1
                _ss_stats['last_error_time'] = datetime.now(_CHINA_TZ)


def dispatch_tick_degraded(svc, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    with svc._probe_lock:
        if not hasattr(svc, '_degraded_tick_count'):
            svc._degraded_tick_count = 0
        svc._degraded_tick_count += 1

    _ds = svc._state_store.get_ref('data_service') if svc._state_store else None
    if _ds is None:
        _ds = svc._state_store.get_ref('storage') if svc._state_store else None
    if _ds and hasattr(_ds, 'realtime_cache') and _ds.realtime_cache:
        try:
            _ds.realtime_cache.update_tick(instrument_id, last_price, time.time(), volume)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _rc_err:
            logging.debug("[DegradedDispatch] realtime_cache.update_tick failed: %s", _rc_err)

    # FIX-R21-B: 降级分发路径也调用TTypeService，确保历史加载期间5态分类也能收到tick数据
    try:
        from data.t_type_service import get_t_type_service
        from config.params_service import get_params_service
        import re as _re_r21b
        _tts_r21b = get_t_type_service()
        _ps_r21b = get_params_service()
        # FIX-P0-16: 移除last_price>0守卫，允许price<=0的期权tick路由到TTypeService
        if _tts_r21b is not None and _ps_r21b is not None and instrument_id:
            _norm_match_r21b = _re_r21b.search(r'([A-Za-z]+\d+.*?)$', str(instrument_id).strip())
            _norm_id_r21b = _norm_match_r21b.group(1) if _norm_match_r21b else instrument_id
            _meta_r21b = _ps_r21b.get_instrument_meta_by_id(_norm_id_r21b)
            if not _meta_r21b and _norm_id_r21b != instrument_id:
                _meta_r21b = _ps_r21b.get_instrument_meta_by_id(instrument_id)
            if _meta_r21b and not getattr(svc, '_tts_routed_this_tick', False):
                # FIX-M9-1: Step A已路由时跳过R21B，避免双重路由
                _inst_type_r21b = _meta_r21b.get('type', '')
                if _inst_type_r21b == 'future':
                    _fid_r21b = _meta_r21b.get('internal_id')
                    if _fid_r21b is not None:
                        _tts_r21b.on_future_instrument_tick(int(_fid_r21b), float(last_price))
                        if not hasattr(svc, '_r21b_future_logged'):
                            svc._r21b_future_logged = True
                            logging.info("[R21B-Degraded] future路由: inst=%s fid=%s price=%s",
                                       instrument_id, _fid_r21b, last_price)
                elif _inst_type_r21b == 'option':
                    _tts_r21b.on_option_tick(_norm_id_r21b, float(last_price), float(volume))
                    if not hasattr(svc, '_r21b_option_logged'):
                        svc._r21b_option_logged = True
                        logging.info("[R21B-Degraded] option路由: inst=%s price=%s",
                                   instrument_id, last_price)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _r21b_err:
        if not hasattr(svc, '_r21b_err_logged'):
            svc._r21b_err_logged = True
            logging.warning("[R21B-Degraded] TTypeService路由失败: %s", _r21b_err)

    _correlation_id = ''
    try:
        from strategy_judgment.causal_chain_utils import CausalChainTracker
        _causal_tracker = CausalChainTracker.get_instance()
        _correlation_id = _causal_tracker.new_correlation_id()
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] CausalChainTracker failed: %s", _r3_err)
        pass
    tick_data = {
        'datetime': svc._get_tick_field(tick, 'datetime', time.time()),
        'instrument_id': instrument_id,
        'exchange': exchange,
        'last_price': last_price,
        'volume': volume,
        'turnover': svc._get_tick_field(tick, 'turnover'),
        'open_interest': svc._get_tick_field(tick, 'open_interest'),
        'bid_price1': svc._get_tick_field(tick, 'bid_price1'),
        'ask_price1': svc._get_tick_field(tick, 'ask_price1'),
        'bid_volume1': svc._get_tick_field(tick, 'bid_volume1'),
        'ask_volume1': svc._get_tick_field(tick, 'ask_volume1'),
        'bid_price2': svc._get_tick_field(tick, 'bid_price2'),
        'ask_price2': svc._get_tick_field(tick, 'ask_price2'),
        'bid_volume2': svc._get_tick_field(tick, 'bid_volume2'),
        'ask_volume2': svc._get_tick_field(tick, 'ask_volume2'),
        'bid_price3': svc._get_tick_field(tick, 'bid_price3'),
        'ask_price3': svc._get_tick_field(tick, 'ask_price3'),
        'bid_volume3': svc._get_tick_field(tick, 'bid_volume3'),
        'ask_volume3': svc._get_tick_field(tick, 'ask_volume3'),
        'bid_price4': svc._get_tick_field(tick, 'bid_price4'),
        'ask_price4': svc._get_tick_field(tick, 'ask_price4'),
        'bid_volume4': svc._get_tick_field(tick, 'bid_volume4'),
        'ask_volume4': svc._get_tick_field(tick, 'ask_volume4'),
        'bid_price5': svc._get_tick_field(tick, 'bid_price5'),
        'ask_price5': svc._get_tick_field(tick, 'ask_price5'),
        'bid_volume5': svc._get_tick_field(tick, 'bid_volume5'),
        'ask_volume5': svc._get_tick_field(tick, 'ask_volume5'),
        'correlation_id': _correlation_id,
    }

    shard_idx = svc._route_shard_index(instrument_id)
    shard_lock = svc._get_shard_lock(shard_idx)
    should_flush = False
    ticks_to_flush = None
    with shard_lock:
        svc._shard_buffers.setdefault(shard_idx, []).append(tick_data)
        svc._shard_last_flush_time.setdefault(shard_idx, time.time())
        try:
            from infra.health_monitor import record_tick_probe
            record_tick_probe('shard_buffer', instrument_id, last_price)
        except Exception:
            pass

        buffer_size = len(svc._shard_buffers[shard_idx])
        threshold = svc._tick_buffer_threshold
        should_flush = buffer_size >= threshold
        if not should_flush and buffer_size > 0:
            elapsed = time.time() - svc._shard_last_flush_time.get(shard_idx, svc._last_buffer_flush_time)
            if elapsed >= svc._tick_buffer_flush_interval:
                should_flush = True

        if should_flush:
            ticks_to_flush = svc._shard_buffers[shard_idx][:]
            svc._shard_buffers[shard_idx].clear()
            svc._last_buffer_flush_time = time.time()
            svc._shard_last_flush_time[shard_idx] = svc._last_buffer_flush_time

    if should_flush and ticks_to_flush:
        try:
            from infra.health_monitor import record_tick_probe
            record_tick_probe('shard_flush', instrument_id, last_price, f'shard={shard_idx} count={len(ticks_to_flush)} degraded=True')
        except Exception:
            pass
        try:
            _stor_degraded = _ds or (svc._state_store.get_ref('storage') if svc._state_store else None)
            if _stor_degraded is not None:
                inserted = _stor_degraded.batch_insert_ticks(ticks_to_flush, use_arrow=True)
                try:
                    from infra.health_monitor import record_tick_probe
                    if inserted == 0:
                        record_tick_probe('db_insert_zero', instrument_id, last_price, f'shard={shard_idx} count={len(ticks_to_flush)} degraded=True')
                    else:
                        record_tick_probe('db_insert_ok', instrument_id, last_price, f'inserted={inserted} degraded=True')
                except Exception:
                    pass
                for tick_item in ticks_to_flush:
                    try:
                        from infra.health_monitor import record_tick_probe
                        record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                    except Exception:
                        pass
        except Exception as e:
            try:
                from infra.health_monitor import record_tick_probe
                record_tick_probe('db_insert_fail', instrument_id, last_price, f'degraded=True {type(e).__name__}: {e}')
            except Exception:
                pass
            with shard_lock:
                svc._shard_buffers.setdefault(shard_idx, []).extend(ticks_to_flush)

    if svc._degraded_tick_count % 10000 == 1:
        logging.info("[DegradedDispatch] 历史加载期间降级分发: instrument=%s, 累计降级=%d",
                     instrument_id, svc._degraded_tick_count)


def output_periodic_summary(svc) -> None:
    try:
        _cross_state = svc._state_store.snapshot(['_is_running', '_is_paused', '_state']) if svc._state_store else {}
        _ss_is_running = _cross_state.get('_is_running', False)
        _ss_is_paused = _cross_state.get('_is_paused', False)
        _ss_state = _cross_state.get('_state')

        _ss_stats = svc._state_store.get_ref('_stats') if svc._state_store else None
        if _ss_stats is None:
            _ss_stats = {}
        # [FIX-20260708-V6-SYNC] 主动同步_ss_stats与provider._stats
        # 根因: state_store的_stats可能是深拷贝快照(由persistence_service的set回退导致)，
        # 与provider._stats(由record_signal/record_trade写入)不是同一对象。
        # 表现为: total_ticks正确(_ss_stats直接递增)但total_signals=0(provider._stats递增)
        # 修复: 每次output_periodic_summary时主动查找provider._stats，
        # 若与_ss_stats不同对象则合并并替换state_store引用
        _v6_provider = None
        if svc._state_store is not None:
            for _ref_key in ('_strategy', '_runtime_strategy_host', '_runtime_strategy_ref'):
                try:
                    _v6_provider = svc._state_store.get_ref(_ref_key)
                    if _v6_provider is not None:
                        break
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass
        if _v6_provider is not None:
            try:
                _p_stats = getattr(_v6_provider, '_stats', None)
                if isinstance(_p_stats, dict) and _p_stats is not _ss_stats:
                    # 不同对象: 合并_ss_stats的total_ticks到_p_stats，然后替换state_store引用
                    _ss_ticks = _ss_stats.get('total_ticks', 0)
                    _p_ticks = _p_stats.get('total_ticks', 0)
                    if _ss_ticks > _p_ticks:
                        _p_stats['total_ticks'] = _ss_ticks
                    # 同步tick统计子项
                    for _tk in ('tick_by_type', 'tick_by_exchange', 'tick_by_instrument'):
                        if _tk in _ss_stats and _tk not in _p_stats:
                            _p_stats[_tk] = _ss_stats[_tk]
                    # 替换state_store引用为provider._stats(真正的live对象)
                    try:
                        svc._state_store.set_ref('_stats', _p_stats)
                    except (TypeError, AttributeError):
                        pass
                    _ss_stats = _p_stats
                    if not hasattr(svc, '_v6_sync_logged'):
                        svc._v6_sync_logged = 0
                    svc._v6_sync_logged += 1
                    if svc._v6_sync_logged <= 3:
                        logging.info("[V6-SYNC] _ss_stats已同步到provider._stats: signals=%d trades=%d ticks=%d",
                                     _p_stats.get('total_signals', 0), _p_stats.get('total_trades', 0),
                                     _p_stats.get('total_ticks', 0))
            except (TypeError, AttributeError, KeyError):
                pass
        # [FIX-20260708-V5-DIAG] 诊断_ss_stats与provider._stats是否同一对象(保留诊断)
        if not hasattr(svc, '_stats_identity_logged'):
            svc._stats_identity_logged = 0
        svc._stats_identity_logged += 1
        if svc._stats_identity_logged <= 3:
            _p_stats_d = getattr(_v6_provider, '_stats', None) if _v6_provider else None
            _p_stats_id = id(_p_stats_d) if _p_stats_d is not None else 'N/A'
            _ss_stats_id = id(_ss_stats) if _ss_stats else 'N/A'
            _p_signals = _p_stats_d.get('total_signals', 'N/A') if isinstance(_p_stats_d, dict) else 'N/A'
            _ss_signals = _ss_stats.get('total_signals', 'N/A') if _ss_stats else 'N/A'
            _same = (_p_stats_id == _ss_stats_id) if _p_stats_id != 'N/A' else 'N/A'
            logging.info("[V5-DIAG] _ss_stats id=%s provider._stats id=%s same=%s ss_signals=%s p_signals=%s",
                         _ss_stats_id, _p_stats_id, _same, _ss_signals, _p_signals)
        # FIX-20260708-V2: 添加start_time回退读取，防止state_store引用丢失时显示"运行时长=0:00:00"
        _start_time = _ss_stats.get('start_time')
        if _start_time is None and svc._state_store is not None:
            _provider = None
            for _ref_key in ('_strategy', '_runtime_strategy_host', '_runtime_strategy_ref'):
                try:
                    _provider = svc._state_store.get_ref(_ref_key)
                    if _provider is not None:
                        break
                except (TypeError, AttributeError, KeyError):
                    pass
            if _provider is None:
                _provider = getattr(svc, '_provider', None) or getattr(svc, '_strategy', None)
            if _provider is not None:
                try:
                    _p_stats = getattr(_provider, '_stats', None)
                    if _p_stats and _p_stats.get('start_time'):
                        _start_time = _p_stats['start_time']
                        # 顺便修复state_store引用
                        if svc._state_store is not None:
                            svc._state_store.set_ref('_stats', _p_stats)
                        _ss_stats = _p_stats
                except (TypeError, AttributeError):
                    pass
        uptime = datetime.now(_CHINA_TZ) - _start_time if _start_time else timedelta(0)
        uptime_str = str(uptime).split('.')[0]
        elapsed_seconds = uptime.total_seconds() if uptime else 1.0
        tick_rate = _ss_stats.get('total_ticks', 0) / max(elapsed_seconds, 1)

        tick_by_type = _ss_stats.get('tick_by_type', {'future': 0, 'option': 0})
        tick_by_exchange = _ss_stats.get('tick_by_exchange', {})
        tick_by_instrument = _ss_stats.get('tick_by_instrument', {})

        sorted_exchanges = sorted(tick_by_exchange.items(), key=lambda x: x[1], reverse=True)[:5]
        sorted_instruments = sorted(tick_by_instrument.items(), key=lambda x: x[1], reverse=True)[:10]

        # FIX-78: 提取含\n的f-string表达式为独立变量，修复Python 3.10 py_compile语法错误
        # 根因: Python 3.11及以下版本不允许f-string表达式部分包含反斜杠(\n)
        # 平台使用Python 3.12(允许)，但py_compile验证需兼容Python 3.10
        _exchange_lines = ''.join([f'  - {exchange}: {count:,} 个\n' for exchange, count in sorted_exchanges])
        _instrument_lines = ''.join([f'  - {inst}: {count:,} 个\n' for inst, count in sorted_instruments])

        summary = f"""
{'='*80}
【3分钟状态汇总】{datetime.now(_CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}
运行时长：{uptime_str}
Tick数据：总计 {_ss_stats.get('total_ticks', 0):,} 个 | 速率 {tick_rate:.1f} ticks/s
  - 期货：{tick_by_type.get('future', 0):,} 个
  - 期权：{tick_by_type.get('option', 0):,} 个
交易数量：总计 {_ss_stats.get('total_trades', 0):,} 笔
信号数量：总计 {_ss_stats.get('total_signals', 0):,} 个
错误统计：{_ss_stats.get('errors_count', 0)} 次
最后错误：{_ss_stats.get('last_error_time').strftime('%H:%M:%S') if _ss_stats.get('last_error_time') else '无'}
策略状态：{getattr(_ss_state, 'value', 'UNKNOWN') if _ss_state is not None else 'UNKNOWN'}
是否运行：{_ss_is_running}
是否暂停：{_ss_is_paused}

【交易所分布】
{_exchange_lines}

【合约活跃度 Top 10】
{_instrument_lines}
{'='*80}
"""
        logging.info(summary)

        try:
            _spm = svc._state_store.get_ref('_state_param_manager') if svc._state_store else None
            if _spm:
                spm = _spm
                spm.update_state_from_width_cache()
                spm_stats = spm.get_stats()
                logging.info(
                    "[StateRoute] 当前状态=%s, 持续%.0fs, 切换%d次, 分布=%s",
                    spm_stats.get('current_state', '?'),
                    spm_stats.get('state_duration_sec', 0),
                    spm_stats.get('state_switches', 0),
                    {k: f"{v:.1%}" for k, v in spm_stats.get('distribution', {}).items()},
                )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as spm_e:
            logging.debug("[StateRoute] StateParamManager output error: %s", spm_e)

        try:
            _sml = svc._state_store.get_ref('_safety_meta_layer') if svc._state_store else None
            if _sml:
                safety_stats = _sml.get_stats()
                logging.info(
                    "[SafetyMeta] 断路器触发=%d, 硬时间止损=%d, 日回撤停止=%d, 暂停中=%s, 禁止开仓=%s",
                    safety_stats.get('circuit_breaker_triggers', 0),
                    safety_stats.get('hard_time_stop_triggers', 0),
                    safety_stats.get('daily_drawdown_triggers', 0),
                    safety_stats.get('trading_paused', False),
                    safety_stats.get('new_open_blocked', False),
                )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as safety_e:
            logging.debug("[SafetyMeta] SafetyMetaLayer output error: %s", safety_e)

        try:
            if hasattr(svc, '_order_flow_bridge') and svc._order_flow_bridge:
                ofb_stats = svc._order_flow_bridge.get_stats()
                logging.info(
                    "[OrderFlow] ticks=%d, depth=%d, buy=%d, sell=%d, products=%d, cache_hit=%d",
                    ofb_stats.get('ticks_fed', 0),
                    ofb_stats.get('depth_updates', 0),
                    ofb_stats.get('direction_buy', 0),
                    ofb_stats.get('direction_sell', 0),
                    ofb_stats.get('products_tracked', 0),
                    ofb_stats.get('cache_hits', 0),
                )
                products = svc._order_flow_bridge.get_products()
                for prod in products[:5]:
                    fc = svc._order_flow_bridge.get_flow_consistency(prod)
                    ofi = svc._order_flow_bridge.get_ofi(prod)
                    logging.info(
                        "[OrderFlow] %s: consistency=%.3f, OFI=%.3f",
                        prod, fc, ofi
                    )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as ofb_e:
            logging.debug("[OrderFlow] output error: %s", ofb_e)

        sm = None
        _storage_ref = svc._state_store.get_ref('storage') if svc._state_store else None
        if _storage_ref is not None and hasattr(_storage_ref, 'subscription_manager'):
            sm = _storage_ref.subscription_manager
        if sm is None:
            try:
                from data.data_service import get_data_service
                _ds = get_data_service()
                if _ds is not None:
                    sm = getattr(_ds, 'subscription_manager', None)
            except Exception:
                sm = None
        if sm and hasattr(sm, 'log_subscription_success_summary'):
            sm.log_subscription_success_summary()

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.error("[Periodic Summary] 输出失败：%s", e)


def flush_tick_buffer(svc) -> None:
    total_flushed = 0
    # FIX-M8-3: 提前获取storage引用，避免循环内定义导致作用域问题
    _storage_flush = svc._state_store.get_ref('storage') if svc._state_store else None
    with svc._shard_buffers_lock:
        shard_snapshots = {}
        for shard_idx, buf in list(svc._shard_buffers.items()):
            if buf:
                shard_snapshots[shard_idx] = buf[:]
                buf.clear()

    for shard_idx, ticks in shard_snapshots.items():
        logging.info("[_flush_tick_buffer] Flushing shard=%d %d ticks", shard_idx, len(ticks))
        shard_lock = svc._get_shard_lock(shard_idx)
        try:
            if _storage_flush:
                if hasattr(_storage_flush, 'process_tick'):
                    _inserted = 0
                    for _tick_item in ticks:
                        _r = _storage_flush.process_tick(_tick_item)
                        _inserted += max(_r, 0)
                    total_flushed += _inserted
                elif hasattr(_storage_flush, 'batch_insert_ticks'):
                    inserted = _storage_flush.batch_insert_ticks(ticks, use_arrow=True)
                    total_flushed += max(inserted, 0)
                else:
                    logging.error("[_flush_tick_buffer] storage无process_tick或batch_insert_ticks方法")
            _e2e_ref = svc._state_store.get_ref('_e2e_counters') if svc._state_store else None
            _lock_ref = svc._state_store.get_ref('_lock') if svc._state_store else None
            if _e2e_ref is not None and _lock_ref is not None:
                with _lock_ref:
                    _e2e_ref['persisted_count'] += len(ticks)
                    _e2e_sp = svc._state_store.get_ref('_e2e_shard_persisted') if svc._state_store else None
                    if _e2e_sp is not None:
                        _e2e_sp[shard_idx] = _e2e_sp.get(shard_idx, 0) + len(ticks)
            from infra.health_monitor import record_tick_probe
            for tick_item in ticks:
                record_tick_probe('saved', tick_item.get('instrument_id', ''), tick_item.get('last_price', 0.0))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logging.error("[_flush_tick_buffer] shard=%d batch flush失败: %s", shard_idx, e)
            with shard_lock:
                svc._shard_buffers.setdefault(shard_idx, []).extend(ticks)
            logging.warning("[_flush_tick_buffer] shard=%d %d/%d 回写", shard_idx, len(ticks), len(ticks))
    if total_flushed:
        logging.info("[_flush_tick_buffer] Total flushed: %d", total_flushed)

    # FIX-M8-3: flush_tick_buffer是周期性调用，顺便flush未完成的K线
    # 解决稀疏tick场景下K线永不完成的问题
    try:
        if _storage_flush and hasattr(_storage_flush, 'flush_incomplete_klines'):
            _storage_flush.flush_incomplete_klines()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _kline_flush_err:
        logging.debug("[FIX-M8-3] flush_incomplete_klines调用失败(非致命): %s", _kline_flush_err)


def dispatch_tick(svc, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    # [R28-C010修复] per-instrument锁保障时序一致性
    _inst_lock = _get_dispatch_lock(instrument_id)
    with _inst_lock:
        _dispatch_tick_inner(svc, tick, instrument_id, last_price, volume, exchange)


def _dispatch_tick_inner(svc, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
    try:
        ds = svc._state_store.get_ref('data_service') if svc._state_store else None
        if ds is None:
            ds = svc._state_store.get_ref('_data_service') if svc._state_store else None
        # FIX-63 D5: 补全 storage fallback，与 L1400-1402 save_tick_snapshot 保持一致
        # 根因: 实际注册键为 'storage'（lifecycle_bind.py:138/strategy_2026.py:393）
        #       但此处仅尝试 'data_service'/'_data_service' → 永远返回 None
        #       → realtime_cache.update_tick 不执行 → tick_hft.py L88 direction_raw 降级
        if ds is None:
            ds = svc._state_store.get_ref('storage') if svc._state_store else None
        if ds and hasattr(ds, 'realtime_cache') and ds.realtime_cache:
            from datetime import datetime as _dt
            bid_p = svc._get_tick_field(tick, 'bid_price1', 0.0)
            ask_p = svc._get_tick_field(tick, 'ask_price1', 0.0)
            ds.realtime_cache.update_tick(
                symbol=instrument_id, price=last_price,
                timestamp=_dt.now(_CHINA_TZ), volume=volume,
                bid_price=bid_p, ask_price=ask_p
            )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[_dispatch_tick] RealTimeCache update skipped: %s", e)

    # FIX-R21: 直接在_dispatch_tick_inner中调用TTypeService，确保5态分类收到tick数据
    # 这是tick数据流的确认执行路径（[Storage]日志证实），不依赖TickRoute fallback
    try:
        from data.t_type_service import get_t_type_service
        from config.params_service import get_params_service
        import re as _re_r21
        _tts_r21 = get_t_type_service()
        _ps_r21 = get_params_service()
        # FIX-R21-DIAG: 无条件诊断日志（前5次），记录进入R21代码块的状态
        if not hasattr(svc, '_r21_diag_count'):
            svc._r21_diag_count = 0
        svc._r21_diag_count += 1
        if svc._r21_diag_count <= 5:
            logging.debug("[R21-DIAG] enter: inst=%r price=%s tts=%s ps=%s ps_len=%s (count=%d)",
                          instrument_id, last_price, _tts_r21 is not None, _ps_r21 is not None,
                          len(_ps_r21) if _ps_r21 is not None else -1, svc._r21_diag_count)
        # FIX-P0-15: 移除last_price>0守卫，允许price<=0的期权tick路由到TTypeService
        # on_option_tick已能正确处理price<=0(P0-10保留方向/P0-05期货方向推断)
        if _tts_r21 is not None and _ps_r21 is not None and instrument_id:
            _norm_match_r21 = _re_r21.search(r'([A-Za-z]+\d+.*?)$', str(instrument_id).strip())
            _norm_id_r21 = _norm_match_r21.group(1) if _norm_match_r21 else instrument_id
            _meta_r21 = _ps_r21.get_instrument_meta_by_id(_norm_id_r21)
            if not _meta_r21 and _norm_id_r21 != instrument_id:
                _meta_r21 = _ps_r21.get_instrument_meta_by_id(instrument_id)
            if svc._r21_diag_count <= 5:
                logging.debug("[R21-DIAG] meta: inst=%r norm=%r meta=%s type=%s",
                              instrument_id, _norm_id_r21, bool(_meta_r21),
                              _meta_r21.get('type', '') if _meta_r21 else 'N/A')
            if _meta_r21 and not getattr(svc, '_tts_routed_this_tick', False):
                # FIX-M9-1: Step A(_sub_mgr/fallback)已路由到TType时跳过R21，避免on_option_tick双重调用
                _inst_type_r21 = _meta_r21.get('type', '')
                if _inst_type_r21 == 'future':
                    _fid_r21 = _meta_r21.get('internal_id')
                    if _fid_r21 is not None:
                        _tts_r21.on_future_instrument_tick(int(_fid_r21), float(last_price))
                        if not hasattr(svc, '_r21_future_logged'):
                            svc._r21_future_logged = True
                            logging.info("[R21-Dispatch] future路由: inst=%s fid=%s price=%s",
                                       instrument_id, _fid_r21, last_price)
                elif _inst_type_r21 == 'option':
                    _tts_r21.on_option_tick(_norm_id_r21, float(last_price), float(volume))
                    if not hasattr(svc, '_r21_option_logged'):
                        svc._r21_option_logged = True
                        logging.info("[R21-Dispatch] option路由: inst=%s price=%s",
                                   instrument_id, last_price)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _r21_err:
        if not hasattr(svc, '_r21_err_logged'):
            svc._r21_err_logged = True
            logging.warning("[R21-Dispatch] TTypeService路由失败: %s", _r21_err)

    _stor3 = svc._state_store.get_ref('storage') if svc._state_store else None
    if _stor3 is None:
        try:
            from data.data_service import get_data_service
            _stor3 = get_data_service()
            if _stor3 is not None and svc._state_store is not None:
                svc._state_store.set_ref('storage', _stor3)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError):
            pass
        if _stor3 is None:
            from infra.health_monitor import record_tick_probe
            record_tick_probe('stor_ref_none', instrument_id, last_price, 'storage_ref_is_None')
    if _stor3 is not None:
        from infra.health_monitor import record_tick_probe
        record_tick_probe('stor_ref_ok', instrument_id, last_price)
        try:
            tick_timestamp = svc._get_tick_field(tick, 'datetime', time.time())
            turnover = svc._get_tick_field(tick, 'turnover', 0.0)
            open_interest = svc._get_tick_field(tick, 'open_interest', 0.0)
            tick_data = {
                'datetime': tick_timestamp,
                'instrument_id': instrument_id,
                'exchange': exchange,
                'last_price': last_price,
                'volume': volume,
                'turnover': turnover,
                'open_interest': open_interest,
                'bid_price1': svc._get_tick_field(tick, 'bid_price1'),
                'ask_price1': svc._get_tick_field(tick, 'ask_price1'),
                'bid_volume1': svc._get_tick_field(tick, 'bid_volume1'),
                'ask_volume1': svc._get_tick_field(tick, 'ask_volume1'),
                'bid_price2': svc._get_tick_field(tick, 'bid_price2'),
                'ask_price2': svc._get_tick_field(tick, 'ask_price2'),
                'bid_volume2': svc._get_tick_field(tick, 'bid_volume2'),
                'ask_volume2': svc._get_tick_field(tick, 'ask_volume2'),
                'bid_price3': svc._get_tick_field(tick, 'bid_price3'),
                'ask_price3': svc._get_tick_field(tick, 'ask_price3'),
                'bid_volume3': svc._get_tick_field(tick, 'bid_volume3'),
                'ask_volume3': svc._get_tick_field(tick, 'ask_volume3'),
                'bid_price4': svc._get_tick_field(tick, 'bid_price4'),
                'ask_price4': svc._get_tick_field(tick, 'ask_price4'),
                'bid_volume4': svc._get_tick_field(tick, 'bid_volume4'),
                'ask_volume4': svc._get_tick_field(tick, 'ask_volume4'),
                'bid_price5': svc._get_tick_field(tick, 'bid_price5'),
                'ask_price5': svc._get_tick_field(tick, 'ask_price5'),
                'bid_volume5': svc._get_tick_field(tick, 'bid_volume5'),
                'ask_volume5': svc._get_tick_field(tick, 'ask_volume5'),
            }

            shard_idx = svc._route_shard_index(instrument_id)
            shard_lock = svc._get_shard_lock(shard_idx)

            # FIX-M3: K线聚合被完全绕过修复
            # 根因: 原代码只把 tick_data 放入 shard_buffers，flush 时调 batch_insert_ticks
            #       直接写 ticks_raw，绕过 storage.process_tick() 中的 _KlineAggregator。
            #       结果: klines_raw 永远无法从 tick 合成，K线覆盖率=0%(全靠 simulated_coverage 填充)。
            # 修复: 在写入 shard_buffers 之前，先调用 storage.aggregate_kline_only(tick_data)
            #       只跑 K线聚合器(不重复写 tick)，避免与 batch_insert_ticks 双写。
            try:
                if _stor3 is not None and hasattr(_stor3, 'aggregate_kline_only'):
                    _stor3.aggregate_kline_only(tick_data)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _m3_err:
                logging.debug("[FIX-M3] aggregate_kline_only 调用失败(非致命): %s", _m3_err)

            with shard_lock:
                svc._shard_buffers.setdefault(shard_idx, []).append(tick_data)
                svc._shard_last_flush_time.setdefault(shard_idx, time.time())
                from infra.health_monitor import record_tick_probe
                record_tick_probe('shard_buffer', instrument_id, last_price)

                buffer_size = len(svc._shard_buffers[shard_idx])
                threshold = svc._tick_buffer_threshold
                should_flush = buffer_size >= threshold
                if not should_flush and buffer_size > 0:
                    elapsed = time.time() - svc._shard_last_flush_time.get(shard_idx, svc._last_buffer_flush_time)
                    if elapsed >= svc._tick_buffer_flush_interval:
                        should_flush = True

                if should_flush:
                    ticks_to_flush = svc._shard_buffers[shard_idx][:]
                    svc._shard_buffers[shard_idx].clear()
                    svc._last_buffer_flush_time = time.time()
                    svc._shard_last_flush_time[shard_idx] = svc._last_buffer_flush_time
                else:
                    ticks_to_flush = None

            _lock = svc._state_store.get_ref('_lock') if svc._state_store else None
            _e2e_counters = svc._state_store.get_ref('_e2e_counters') if svc._state_store else None
            if _lock is not None and _e2e_counters is not None:
                with _lock:
                    _e2e_counters['enqueued_count'] += 1

            with svc._probe_lock:
                svc._snapshot_tick_count += 1
                should_snapshot = (svc._snapshot_tick_count % svc._snapshot_interval == 0)

            if should_snapshot:
                save_tick_snapshot(svc)

            if should_flush and ticks_to_flush:
                from infra.health_monitor import record_tick_probe
                record_tick_probe('shard_flush', instrument_id, last_price, f'shard={shard_idx} count={len(ticks_to_flush)}')
                try:
                    inserted = _stor3.batch_insert_ticks(ticks_to_flush, use_arrow=True)
                    _batch_len = len(ticks_to_flush)
                    if inserted == 0:
                        record_tick_probe('db_insert_zero', instrument_id, last_price, f'shard={shard_idx} count={_batch_len}')
                        if not hasattr(svc, '_db_insert_zero_count'):
                            svc._db_insert_zero_count = 0
                        svc._db_insert_zero_count += _batch_len
                        if svc._db_insert_zero_count <= 10 or svc._db_insert_zero_count % 10000 == 1:
                            logging.warning("[FIX-20260701] batch_insert=0 (ON CONFLICT skip?): inst=%s batch=%d total_zero=%d",
                                            instrument_id, _batch_len, svc._db_insert_zero_count)
                    else:
                        record_tick_probe('db_insert_ok', instrument_id, last_price, f'inserted={inserted}')
                        # FIX-P1-NEW-02: 检测部分插入(ON CONFLICT跳过部分行)
                        if inserted < _batch_len:
                            if not hasattr(svc, '_db_insert_partial_count'):
                                svc._db_insert_partial_count = 0
                            svc._db_insert_partial_count += (_batch_len - inserted)
                            if svc._db_insert_partial_count <= 10 or svc._db_insert_partial_count % 10000 == 1:
                                logging.warning("[FIX-P1-NEW-02] batch部分插入(ON CONFLICT跳过): inst=%s inserted=%d/%d total_skipped=%d",
                                                instrument_id, inserted, _batch_len, svc._db_insert_partial_count)
                    if _lock is not None and _e2e_counters is not None:
                        with _lock:
                            _e2e_counters['persisted_count'] += max(inserted, 0)
                    for tick_item in ticks_to_flush:
                        record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                except Exception as e:
                    record_tick_probe('db_insert_fail', instrument_id, last_price, f'{type(e).__name__}: {e}')
                    with shard_lock:
                        svc._shard_buffers.setdefault(shard_idx, []).extend(ticks_to_flush)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            from infra.health_monitor import record_tick_probe
            record_tick_probe('error', instrument_id, last_price, f"Storage: {e}")
            logging.exception("[_dispatch_tick] Stack trace for instrument: %s", instrument_id)
            if not hasattr(svc, '_storage_error_drop_count'):
                svc._storage_error_drop_count = 0
            svc._storage_error_drop_count += 1
            if svc._storage_error_drop_count <= 10 or svc._storage_error_drop_count % 1000 == 1:
                logging.warning("[FIX-20260701] storage异常导致tick丢失: inst=%s total=%d", instrument_id, svc._storage_error_drop_count)

    # P2-24防护: 此处是tick→position的唯一分发路径，禁止在EventBus上再订阅tick调用pos_svc.on_tick
    try:
        from position.position_service import get_position_service
        pos_svc = get_position_service()
        pos_svc.on_tick(tick)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as pos_e:
        logging.warning("[_dispatch_tick] PositionService.on_tick failed: %s", pos_e)

    from strategy.tick_hft import dispatch_hft_tick
    dispatch_hft_tick(svc, tick, instrument_id, last_price, volume, exchange)


def save_tick_snapshot(svc):
    if not svc._snapshot_path:
        try:
            ds = svc._state_store.get_ref('data_service') if svc._state_store else None
            if ds is None:
                ds = svc._state_store.get_ref('storage') if svc._state_store else None
            if ds and hasattr(ds, '_data_dir'):
                svc._snapshot_path = os.path.join(ds._data_dir, 'tick_snapshot.json')
        except (ValueError, KeyError, TypeError, AttributeError) as _snap_err:
            logging.debug("[_save_tick_snapshot] Path resolution failed: %s", _snap_err)
            return
    if not svc._snapshot_path:
        return
    try:
        rc = svc._state_store.get_ref('data_service') if svc._state_store else None
        if rc is None:
            rc = svc._state_store.get_ref('storage') if svc._state_store else None
        rc = getattr(rc, 'realtime_cache', None) if rc else None
        if not rc:
            return
        from infra.serialization_utils import json_dumps as _json_dumps
        from datetime import datetime as _dt
        snapshot = {'timestamp': _dt.now(_CHINA_TZ).isoformat(), 'ticks': {}}
        with rc._lock:
            for sym, tick in rc._latest_ticks.items():
                ts = tick.get('datetime') or tick.get('update_time')
                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                snapshot['ticks'][sym] = {
                    'p': tick.get('last_price', 0), 't': ts_str, 'v': tick.get('volume', 0),
                    'b': tick.get('bid_price1', 0), 'a': tick.get('ask_price1', 0),
                    'ot': tick.get('option_type'), 'sp': tick.get('strike_price'),
                    'iid': tick.get('internal_id'),
                }
        os.makedirs(os.path.dirname(svc._snapshot_path), exist_ok=True)
        with open(svc._snapshot_path, 'w', encoding='utf-8') as f:
            f.write(_json_dumps(snapshot))
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
        logging.debug("[_save_tick_snapshot] error: %s", e)


def atexit_flush(weak_self):
    svc = weak_self()
    if svc is None:
        return
    try:
        if hasattr(svc, '_shard_buffers') and svc._shard_buffers:
            flush_tick_buffer(svc)
            logging.info("[atexit] Tick缓冲区已刷写")
        save_tick_snapshot(svc)
        logging.info("[atexit] Tick快照已保存")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
        logging.error("[atexit] Tick缓冲区刷写失败: %s", e)


def tick_preflight_check(svc, tick) -> bool:
    """Tick预处理检查（去重/时间戳/年龄/序列号/背压），返回True表示通过"""
    try:
        _dedup_inst = svc._get_tick_field(tick, 'instrument_id', '')
        _dedup_ts = svc._get_tick_field(tick, 'datetime', '')
        if _dedup_inst and _dedup_ts:
            _dedup_key = f"{_dedup_inst}_{_dedup_ts}"
            _dedup_now = time.time()
            _dedup_last = svc._tick_dedup_cache.get(_dedup_key, 0.0)
            if (_dedup_now - _dedup_last) * 1000 < svc._tick_dedup_window_ms:
                if not hasattr(svc, '_dedup_drop_count'):
                    svc._dedup_drop_count = 0
                svc._dedup_drop_count += 1
                if svc._dedup_drop_count <= 10 or svc._dedup_drop_count % 10000 == 1:
                    logging.warning("[FIX-20260701] tick 100ms去重丢弃: inst=%s total=%d", _dedup_inst, svc._dedup_drop_count)
                return False
            svc._tick_dedup_cache[_dedup_key] = _dedup_now
            if len(svc._tick_dedup_cache) > 10000:
                _cutoff = _dedup_now - svc._tick_dedup_window_ms / 1000.0
                svc._tick_dedup_cache = {k: v for k, v in svc._tick_dedup_cache.items() if v > _cutoff}
    except (ValueError, KeyError, TypeError, AttributeError) as _dedup_err:
        logging.debug("[R22-P1-NEW] tick去重缓存更新失败(可能重复处理): %s", _dedup_err)

    try:
        _tick_ts = svc._get_tick_field(tick, 'datetime', None)
        if _tick_ts is not None:
            if isinstance(_tick_ts, (int, float)):
                svc._state_store.set('_current_bar_time', float(_tick_ts))
            elif hasattr(_tick_ts, 'timestamp'):
                svc._state_store.set('_current_bar_time', _tick_ts.timestamp())
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        logging.debug("[R3-L2] _current_bar_time update failed: %s", _r3_err)
        pass

    try:
        _age_inst = svc._get_tick_field(tick, 'instrument_id', '')
        if _age_inst:
            svc._tick_last_data_time[_age_inst] = time.time()
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        logging.debug("[R3-L2] tick_last_data_time update failed: %s", _r3_err)
        pass

    try:
        _seq_field_names = ['sequence_no', 'seq_no', 'update_sequence']
        _seq_val = None
        for _fn in _seq_field_names:
            _seq_val = getattr(tick, _fn, None)
            if _seq_val is not None:
                break
        if _seq_val is not None:
            _inst_id = svc._get_tick_field(tick, 'instrument_id', '')
            if _inst_id:
                try:
                    _seq_int = int(_seq_val)
                    with svc._tick_seq_lock:
                        _last_seq = svc._tick_last_seq.get(_inst_id, -1)
                        if _last_seq >= 0:
                            if _seq_int < _last_seq:
                                # FIX-P1-15: 序列号 <= 改为 <, 允许相等(模拟平台同批次推送可能序列号相同)
                                # 根因: 模拟平台序列号可能重复或回退(同一批次推送), <= 过滤会丢弃合法 tick
                                # 修复: 仅丢弃严格小于的序列号(真正乱序), 允许相等(同批次重复)
                                svc._tick_seq_error_count += 1
                                if svc._tick_seq_error_count <= 10 or svc._tick_seq_error_count % 1000 == 0:
                                    logging.warning(
                                        "[R21-NET-P1-02修复] Tick序列号异常: instrument=%s seq=%d last_seq=%d (乱序/重复), 累计%d次",
                                        _inst_id, _seq_int, _last_seq, svc._tick_seq_error_count,
                                    )
                                svc._tick_last_seq[_inst_id] = _seq_int
                                try:
                                    from infra.event_bus import get_global_event_bus
                                    _eb = get_global_event_bus()
                                    _eb.publish('tick_dropped', {'instrument_id': _inst_id, 'reason': 'sequence_error', 'timestamp': time.time()})
                                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                                    logging.debug("[R3-L2] event_bus publish tick_dropped failed: %s", _r3_err)
                                    pass
                                return False
                            elif _seq_int > _last_seq + 1:
                                svc._tick_seq_error_count += 1
                                if svc._tick_seq_error_count <= 10 or svc._tick_seq_error_count % 1000 == 0:
                                    logging.warning(
                                        "[R21-NET-P1-02修复] Tick序列号跳跃: instrument=%s seq=%d last_seq=%d (丢包?), 累计%d次",
                                        _inst_id, _seq_int, _last_seq, svc._tick_seq_error_count,
                                    )
                        svc._tick_last_seq[_inst_id] = _seq_int
                except (ValueError, TypeError):
                    pass
    except (ValueError, KeyError, TypeError, AttributeError) as _seq_err:
        logging.debug("[R22-P1-NEW] tick序列号处理异常(丢包检测失效): %s", _seq_err)

    _now = time.time()
    if svc._tick_overload_until > _now:
        # [FIX-20260708-TICK-NO-DROP] 背压降级：不丢弃tick，改为标记降级模式
        # 根因：丢弃tick导致价格数据缺失、止盈止损漏检、策略决策偏差
        # 修复：过载期间tick走轻量路径(仅更新价格缓存+止损检查)，不丢弃
        svc._tick_degraded_mode = True
        if not hasattr(svc, '_backpressure_degrade_count'):
            svc._backpressure_degrade_count = 0
        svc._backpressure_degrade_count += 1
        if svc._backpressure_degrade_count <= 10 or svc._backpressure_degrade_count % 10000 == 1:
            logging.warning("[FIX-20260708] tick背压降级(不丢弃): remaining=%.1fs total=%d",
                            svc._tick_overload_until - _now, svc._backpressure_degrade_count)
    else:
        # 过载窗口结束，恢复正常处理
        if getattr(svc, '_tick_degraded_mode', False):
            svc._tick_degraded_mode = False
            logging.info("[FIX-20260708] tick背压恢复：已切回正常处理模式")

    return True