"""
Tick数据处理Mixin模块

[ACTIVE] 此模块通过 TickHandlerMixin 被 StrategyCoreService 继承使用，是当前主链路的组成部分。
    StrategyCoreService(HistoricalKlineMixin, TickHandlerMixin) 是唯一权威链路。

职责：
- 处理Tick数据回调（on_tick）
- Tick数据统计和汇总
- 周期性状态输出
- Tick数据持久化和K线合成

设计原则：
- 单一职责：仅处理Tick相关逻辑
- 高性能：核心热路径，优化性能
- 线程安全：保护共享状态
"""

import atexit
import logging
import math
import time
import threading
import uuid
import weakref
import os
from datetime import datetime, timedelta, timezone
# P1-R11-12修复: 中国标准时间UTC+8，替代裸datetime.now()，确保交易系统时间判断一致
_CHINA_TZ = timezone(timedelta(hours=8))
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

# R27-P0-FP-01修复: 浮点容差常量，止盈止损比较使用
_PRICE_TOLERANCE = 1e-6

# R27-P1修复: 导入容错/浮点工具
from ali2026v3_trading.resilience_utils import (
    TimeoutGuard, Watchdog, HeartbeatMonitor, SignalExpiryManager,
    stable_sum, stable_mean, stable_variance,
    approx_equal, approx_less, approx_greater,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, PRICE_TOLERANCE as _RESILIENCE_TOLERANCE,
    get_signal_lifecycle, SignalLifecycleManager,
    deterministic_round, safe_float_to_int,
)
from ali2026v3_trading import config_params
from ali2026v3_trading.causal_chain_utils import (
    CausalChainTracker, ContaminationGuard, CyclicDependencyGuard,
    validate_tick_cascade, CausalEvent,
)

__all__ = ['TickHandlerMixin', 'DynamicPursuitEngine', 'PursuitPosition']

logger = logging.getLogger(__name__)


class TickHandlerMixin:
    """Tick数据处理Mixin"""

    # R10-P2-03: 背压机制 — 基于tick处理耗时判断是否过载
    MAX_TICK_QUEUE_DEPTH = 1000
    _TICK_OVERLOAD_THRESHOLD_MS = 200.0  # R10-P2-03修复: 单tick处理超过200ms视为过载
    _TICK_OVERLOAD_DROP_WINDOW_SEC = 5.0  # 过载后5秒内丢弃非关键tick
    _UNIFIED_TICK_PATH_ENABLED = True

    _TICK_FIELD_NAMES = {
        'instrument_id': ['instrument_id', 'InstrumentID'],
        'exchange': ['exchange', 'ExchangeID'],
        'last_price': ['last_price', 'LastPrice'],
        'volume': ['volume', 'Volume'],
        'timestamp': ['timestamp', 'Timestamp', 'ts', 'datetime', 'DateTime', 'UpdateTime'],
        'open_interest': ['open_interest', 'OpenInterest', 'openInterest'],
        'amount': ['amount', 'Amount', 'turnover', 'Turnover'],
        'bid_price1': ['bid_price1', 'BidPrice1'],
        'ask_price1': ['ask_price1', 'AskPrice1'],
        'bid_price2': ['bid_price2', 'BidPrice2'],
        'ask_price2': ['ask_price2', 'AskPrice2'],
        'bid_price3': ['bid_price3', 'BidPrice3'],
        'ask_price3': ['ask_price3', 'AskPrice3'],
        'bid_price4': ['bid_price4', 'BidPrice4'],
        'ask_price4': ['ask_price4', 'AskPrice4'],
        'bid_price5': ['bid_price5', 'BidPrice5'],
        'ask_price5': ['ask_price5', 'AskPrice5'],
        'bid_volume1': ['bid_volume1', 'BidVolume1'],
        'ask_volume1': ['ask_volume1', 'AskVolume1'],
        'bid_volume2': ['bid_volume2', 'BidVolume2'],
        'ask_volume2': ['ask_volume2', 'AskVolume2'],
        'bid_volume3': ['bid_volume3', 'BidVolume3'],
        'ask_volume3': ['ask_volume3', 'AskVolume3'],
        'bid_volume4': ['bid_volume4', 'BidVolume4'],
        'ask_volume4': ['ask_volume4', 'AskVolume4'],
        'bid_volume5': ['bid_volume5', 'BidVolume5'],
        'ask_volume5': ['ask_volume5', 'AskVolume5'],
    }
    
    def _init_tick_handler_mixin(self) -> None:
        """初始化Tick处理Mixin的状态

        注意：此方法应在StrategyCoreService.__init__中调用

        R21-CC-P1-08修复: 原全局_tick_lock已拆为分片锁体系：
        - _shard_locks: 按品种分片的16个独立锁（_route_shard_index路由）
        - _probe_lock: 保护_probe_logged_instruments
        - _tick_stats_lock: 保护统计计数器
        - _raw_tick_accum_lock: 保护原始tick累加器
        不再存在全局串行化锁，tick处理并发度由分片数(16)决定。
        """
        from ali2026v3_trading.state_store import get_state_store
        from ali2026v3_trading.callback_registry import get_callback_group, ON_TICK

        self._state_store = get_state_store()
        self._callback_group = get_callback_group()

        self._tick_count = 0

        # [R23-P2-ID-04-FIX] tick处理去重：同一tick的instrument_id+timestamp在100ms内不重复处理
        self._tick_dedup_cache: Dict[str, float] = {}
        self._tick_dedup_window_ms: float = 100.0
        # [R23-P2-FR-09-FIX] tick数据年龄监控
        self._tick_last_data_time: Dict[str, float] = {}

        # R21-NET-P1-02修复: Tick序列号校验 — 按合约记录最近序列号，检测乱序/重复
        self._tick_last_seq: Dict[str, int] = {}
        self._tick_seq_error_count: int = 0
        self._tick_seq_lock = threading.Lock()

        # R23-SM-09-FIX: HMM状态驻留时间异常检测
        self._hmm_state_entry_time: Dict[str, float] = {}
        self._hmm_state_dwell_max_sec: float = 3600.0  # 单态最大驻留时间
        self._hmm_state_dwell_min_sec: float = 1.0  # 单态最小驻留时间(过于频繁切换)
        self._hmm_state_switch_counts: Dict[str, int] = {}
        self._hmm_state_switch_window_sec: float = 60.0
        self._hmm_state_max_switches_per_window: int = 100
        self._hmm_last_state: Optional[str] = None

        # R27-P1-DR-02修复: tick处理超时守卫
        self._tick_timeout_guard = TimeoutGuard(timeout_sec=5.0)
        # R27-P1-DR-06修复: tick处理看门狗; R27-P2-DR-11修复: 超时配置外置
        self._tick_watchdog = Watchdog(timeout_sec=config_params.WATCHDOG_TIMEOUT_TICK_SEC, name='tick_handler')
        # R27-P1-DR-07修复: tick处理心跳
        self._tick_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=5.0, missed_threshold=3)
        self._hmm_state_max_switches_per_window: int = 100

        # R10-P2-03: 背压机制 — tick队列深度计数器
        self._tick_queue_depth = 0
        
        # Tick日志时间控制（30秒汇总输出）
        self._last_tick_log_time = time.time()
        self._tick_log_interval = 180.0
        
        # Tick诊断时间戳
        self._tick_debug_last_ts = 0.0

        # R24-P0-IV-07: 价格跳变实时检测
        self._price_jump_last_price = {}  # instrument_id -> last_valid_price
        self._price_jump_threshold = 0.05  # 5%跳变阈值
        self._price_jump_count = 0

        # R24-P1-IV-09修复: tick时间戳单调性验证
        self._last_tick_timestamp: Dict[str, float] = {}
        self._tick_timestamp_violation_count: int = 0
        
        # Probe日志集合（记录前100个不同合约）
        self._probe_logged_instruments = set()
        self._probe_lock = threading.Lock()  # ✅ P1#26修复：保护_probe_logged_instruments的并发修改
        
        # 第二轮优化：分片缓冲（按品种独立锁，消除全局竞争）
        self._TICK_SHARD_COUNT = 16
        from ali2026v3_trading.shared_utils import get_shard_router
        self._shard_router = get_shard_router(shard_count=self._TICK_SHARD_COUNT)
        self._shard_buffers: Dict[int, list] = {}
        self._shard_locks: Dict[int, threading.Lock] = {}
        self._shard_last_flush_time: Dict[int, float] = {}
        self._shard_buffers_lock = threading.Lock()
        self._tick_buffer_threshold = 50
        # [R16-P2-7.2修复] flush间隔改为环境变量可配置，默认1.0秒
        self._tick_buffer_flush_interval = float(os.environ.get('TICK_HANDLER_FLUSH_INTERVAL', '1.0'))
        self._last_buffer_flush_time = time.time()
        self._shard_key_mode = 'product_code'
        self._tick_stats_lock = threading.Lock()
        # ✅ 覆盖式快照WAL：每100条tick快照RealTimeCache到磁盘，仅保留最后状态
        self._snapshot_path = None
        self._snapshot_tick_count = 0
        self._snapshot_interval = 100
        self._raw_tick_summary_interval = 60.0
        self._raw_tick_last_output_time = 0.0
        self._raw_tick_accum_count = 0
        self._raw_tick_accum_lock = threading.Lock()
        store = self._state_store
        store.set('_tick_count', self._tick_count)
        store.set('_tick_queue_depth', self._tick_queue_depth)
        store.set('_price_jump_count', self._price_jump_count)
        store.set('_tick_timestamp_violation_count', self._tick_timestamp_violation_count)
        store.set('_tick_seq_error_count', self._tick_seq_error_count)
        store.set_ref('_shard_router', self._shard_router)
        store.set('_tick_buffer_threshold', self._tick_buffer_threshold)
        store.set('_tick_buffer_flush_interval', self._tick_buffer_flush_interval)
        store.set('_snapshot_interval', self._snapshot_interval)
        store.set('_tick_log_interval', self._tick_log_interval)
        store.set('_is_running', getattr(self, '_is_running', False))
        store.set('_is_paused', getattr(self, '_is_paused', False))
        store.set_ref('_state', getattr(self, '_state', None))

        if hasattr(self, '_callback_group') and self._callback_group:
            self._callback_group.get_registry('flush_tick_buffer').register(self._flush_tick_buffer)

        # ✅ P1#25修复：使用weakref弱引用注册atexit，避免实例方法引用阻止GC回收  # R17-P2-DOC-05
        # ✅ M20 Bug #3修复：防止重复注册atexit回调  # R17-P2-DOC-05
        if not hasattr(TickHandlerMixin, '_atexit_registered'):
            self._weak_self = weakref.ref(self)
            atexit.register(TickHandlerMixin._atexit_flush, self._weak_self)
            TickHandlerMixin._atexit_registered = True
    
    # ========== 分片路由辅助方法（统一委托ShardRouter） ==========

    def _route_shard_index(self, instrument_id: str) -> int:
        _SHARD_MASK = self._TICK_SHARD_COUNT - 1
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            info = ps.get_instrument_meta_by_id(instrument_id)
            if info:
                shard_key = info.get('shard_key', -1)
                if isinstance(shard_key, int) and shard_key >= 0:
                    return shard_key & _SHARD_MASK
        except Exception as _route_err:
            # R23-P1-09修复: 记录分片路由异常详情，便于问题诊断
            logging.debug("[_route_shard_index] Fallback route exception type=%s: %s", type(_route_err).__name__, _route_err, exc_info=True)
        from ali2026v3_trading.shared_utils import ShardRouter, extract_product_code
        _pc_raw = extract_product_code(instrument_id)
        pc = _pc_raw.lower() if _pc_raw else ''
        return ShardRouter._deterministic_hash(pc) & _SHARD_MASK if pc else 0

    def _get_shard_lock(self, shard_idx: int) -> threading.Lock:
        with self._shard_buffers_lock:
            if shard_idx not in self._shard_locks:
                self._shard_locks[shard_idx] = threading.Lock()
            return self._shard_locks[shard_idx]

    def get_shard_stats(self) -> Dict[int, int]:
        stats = {}
        with self._shard_buffers_lock:
            for key, buf in list(self._shard_buffers.items()):
                stats[key] = len(buf)
        return stats

    # ========== Tick入口处理 ==========
    
    def _get_tick_field(self, tick: Any, field_name: str, default: Any = None) -> Any:
        """统一Tick字段提取入口，使用_TICK_FIELD_NAMES常量"""
        attr_names = self._TICK_FIELD_NAMES.get(field_name, [field_name])
        for attr in attr_names:
            val = getattr(tick, attr, None)
            if val is not None:
                return val
        return default
    
    def _normalize_tick(self, tick: Any) -> Dict[str, Any]:
        """平台适配层：统一Tick属性名，消除多格式属性路径"""
        return {
            field: self._get_tick_field(tick, field)
            for field in self._TICK_FIELD_NAMES
        }

    def _check_hft_open_risk(self, instrument_id: str, direction: str, price: float, volume: int,
                             pursuit_signal: Dict[str, Any]) -> bool:
        try:
            from ali2026v3_trading.risk_service import get_risk_service

            signal = {
                'symbol': instrument_id,
                'direction': direction,
                'price': price,
                'volume': volume,
                'amount': max(float(price), 0.0) * max(int(volume), 0),
                'is_valid': True,
                'action': 'OPEN',
                'account_id': pursuit_signal.get('account_id', 'default'),
                'signal_id': pursuit_signal.get('signal_id', '') or f"HFT_{uuid.uuid4().hex[:12]}",
            }
            check_result = get_risk_service(None).check_before_trade(signal)
            if check_result.is_block:
                logging.warning(
                    "[HFT] pursuit open blocked by RiskService: %s %s reason=%s signal_id=%s",
                    instrument_id,
                    direction,
                    check_result.reason,
                    signal['signal_id'],
                )
                return False
            pursuit_signal['signal_id'] = signal['signal_id']
            _sig_delay = pursuit_signal.get('e2e_delay_ms')
            if _sig_delay is not None and _sig_delay > 100.0:
                logging.warning('[R16-P0-2.1] 信号通过风控后延迟: %.1fms instrument=%s',
                              _sig_delay, instrument_id)
            return True
        except Exception as e:
            logging.error("[HFT] pursuit open risk check failed, fail-safe block: %s", e, exc_info=True)
            return False
    
    # ✅ 方法唯一：on_tick(tick)为平台回调入口，position_service同签名；data_service/subscription_manager为内部层，签名不同但分层合理
    def on_tick(self, tick: Any) -> None:
        """Tick数据回调 - 核心热路径（拆分为探针层→检查层→处理层→统计层）"""
        _tick_arrival_ts = time.perf_counter()
        if hasattr(tick, '__dict__'):
            tick._arrival_ts = _tick_arrival_ts
        # [R23-P2-ID-04-FIX] tick处理去重：同一tick的instrument_id+timestamp在100ms内不重复处理
        try:
            _dedup_inst = self._get_tick_field(tick, 'instrument_id', '')
            _dedup_ts = self._get_tick_field(tick, 'timestamp', '')
            if _dedup_inst and _dedup_ts:
                _dedup_key = f"{_dedup_inst}_{_dedup_ts}"
                _dedup_now = time.time()
                _dedup_last = self._tick_dedup_cache.get(_dedup_key, 0.0)
                if (_dedup_now - _dedup_last) * 1000 < self._tick_dedup_window_ms:
                    return
                self._tick_dedup_cache[_dedup_key] = _dedup_now
                if len(self._tick_dedup_cache) > 10000:
                    _cutoff = _dedup_now - self._tick_dedup_window_ms / 1000.0
                    self._tick_dedup_cache = {k: v for k, v in self._tick_dedup_cache.items() if v > _cutoff}
        except Exception as _dedup_err:
            logging.debug("[R22-P1-NEW] tick去重缓存更新失败(可能重复处理): %s", _dedup_err)
        # P0-R11-14修复: 从tick数据提取时间戳赋值_current_bar_time，供硬时间止损使用
        try:
            _tick_ts = self._get_tick_field(tick, 'timestamp', None)
            if _tick_ts is not None:
                if isinstance(_tick_ts, (int, float)):
                    self._current_bar_time = float(_tick_ts)
                elif hasattr(_tick_ts, 'timestamp'):
                    self._current_bar_time = _tick_ts.timestamp()
        except Exception:
            pass
        # [R23-P2-FR-09-FIX] 更新tick数据年龄
        try:
            _age_inst = self._get_tick_field(tick, 'instrument_id', '')
            if _age_inst:
                self._tick_last_data_time[_age_inst] = time.time()
        except Exception:
            pass
        # R21-NET-P1-02修复: Tick序列号校验 — 检测乱序/重复tick
        try:
            _seq_field_names = ['sequence_no', 'SequenceNo', 'seq_no', 'update_sequence', 'UpdateSequence']
            _seq_val = None
            for _fn in _seq_field_names:
                _seq_val = getattr(tick, _fn, None)
                if _seq_val is not None:
                    break
            if _seq_val is not None:
                _inst_id = self._get_tick_field(tick, 'instrument_id', '')
                if _inst_id:
                    try:
                        _seq_int = int(_seq_val)
                        with self._tick_seq_lock:
                            _last_seq = self._tick_last_seq.get(_inst_id, -1)
                            if _last_seq >= 0:
                                if _seq_int <= _last_seq:
                                    self._tick_seq_error_count += 1
                                    if self._tick_seq_error_count <= 10 or self._tick_seq_error_count % 1000 == 0:
                                        logging.warning(
                                            "[R21-NET-P1-02修复] Tick序列号异常: instrument=%s seq=%d last_seq=%d (乱序/重复), 累计%d次",
                                            _inst_id, _seq_int, _last_seq, self._tick_seq_error_count,
                                        )
                                    self._tick_last_seq[_inst_id] = _seq_int
                                    # R33-P0-04修复: tick序列号异常丢弃时通知EventBus，使position_service感知行情中断
                                    try:
                                        from ali2026v3_trading.event_bus import get_global_event_bus
                                        _eb = get_global_event_bus()
                                        _eb.publish('tick_dropped', {'instrument_id': _inst_id, 'reason': 'sequence_error', 'timestamp': time.time()})
                                    except Exception:
                                        pass
                                    return
                                elif _seq_int > _last_seq + 1:
                                    self._tick_seq_error_count += 1
                                    if self._tick_seq_error_count <= 10 or self._tick_seq_error_count % 1000 == 0:
                                        logging.warning(
                                            "[R21-NET-P1-02修复] Tick序列号跳跃: instrument=%s seq=%d last_seq=%d (丢包?), 累计%d次",
                                            _inst_id, _seq_int, _last_seq, self._tick_seq_error_count,
                                        )
                            self._tick_last_seq[_inst_id] = _seq_int
                    except (ValueError, TypeError):
                        pass
        except Exception as _seq_err:
            logging.debug("[R22-P1-NEW] tick序列号处理异常(丢包检测失效): %s", _seq_err)

        # R10-P2-03修复: 基于处理耗时的背压机制 — 替代无效的计数器方案
        # 同步回调中_tick_queue_depth始终为0或1，无法反映真实负载
        # 改用上次tick处理耗时判断是否过载，过载后进入丢弃窗口
        _now = time.time()
        if getattr(self, '_tick_overload_until', 0) > _now:
            if int((_now * 10) % 100) == 0:  # 降频告警
                logging.warning(
                    "[R10-P2-03] tick过载丢弃窗口中，剩余%.1fs",
                    self._tick_overload_until - _now,
                )
            # R33-P0-04修复: 过载丢弃时通知EventBus，使position_service感知行情中断
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus
                _eb = get_global_event_bus()
                _dropped_inst = self._get_tick_field(tick, 'instrument_id', '')
                _eb.publish('tick_dropped', {'instrument_id': _dropped_inst, 'reason': 'backpressure', 'timestamp': _now})
            except Exception:
                pass
            return

        # R15-P0-PERF-04修复: tick处理耗时计时和超限告警
        _tick_start_ns = time.perf_counter_ns()
        try:
            instrument_id_raw = self._tick_probe_layer(tick)
            if not self._tick_check_layer(instrument_id_raw):
                return
            self._tick_dispatch_layer(tick, instrument_id_raw)
        finally:
            _tick_elapsed_ms = (time.perf_counter_ns() - _tick_start_ns) / 1_000_000.0
            if _tick_elapsed_ms > self._TICK_OVERLOAD_THRESHOLD_MS:
                # R10-P2-03修复: 处理超限→进入过载丢弃窗口
                self._tick_overload_until = _now + self._TICK_OVERLOAD_DROP_WINDOW_SEC
                logging.error(
                    "[R10-P2-03] tick处理超限%.1fms>阈值%.1fms，进入%.0fs过载丢弃窗口",
                    _tick_elapsed_ms, self._TICK_OVERLOAD_THRESHOLD_MS,
                    self._TICK_OVERLOAD_DROP_WINDOW_SEC,
                )
            elif _tick_elapsed_ms > 500.0:
                logging.error("[R15-P0-PERF-04] tick处理严重超时: %.1fms instrument=%s", _tick_elapsed_ms, self._get_tick_field(tick, 'instrument_id', '?'))
            elif _tick_elapsed_ms > 100.0:
                logging.warning("[R15-P0-PERF-04] tick处理超限: %.1fms instrument=%s", _tick_elapsed_ms, self._get_tick_field(tick, 'instrument_id', '?'))

    def _tick_probe_layer(self, tick: Any) -> str:
        """探针层：记录原始合约格式和诊断信息"""
        from ali2026v3_trading.subscription_manager import SubscriptionManager

        instrument_id_raw = self._get_tick_field(tick, 'instrument_id', '')
        if not instrument_id_raw:
            return instrument_id_raw

        with self._probe_lock:
            if len(self._probe_logged_instruments) < 100:
                if instrument_id_raw not in self._probe_logged_instruments:
                    self._probe_logged_instruments.add(instrument_id_raw)
                    logging.info("[PROBE_RAW_INSTRUMENT_ID] raw format: '%s' (type=%s)", instrument_id_raw, type(instrument_id_raw).__name__)

        diag_on = False
        try:
            from ali2026v3_trading.diagnosis_service import is_monitored_contract
            diag_on = is_monitored_contract(instrument_id_raw)
        except Exception as e:
            logging.debug("[on_tick] Failed to check monitored contract: %s", e)

        if diag_on:
            now = time.time()
            with self._raw_tick_accum_lock:
                self._raw_tick_accum_count += 1
                if now - self._raw_tick_last_output_time >= self._raw_tick_summary_interval:
                    # R24-P2-AT-03修复: tick日志添加tick_id结构化字段
                    logging.info("[RAW_TICK_ALL_SUMMARY] tick_id=%s ticks=%d sample=(%s P=%s V=%s)",
                                getattr(tick, 'tick_id', 'N/A'),
                                self._raw_tick_accum_count,
                                instrument_id_raw,
                                self._get_tick_field(tick, 'last_price', 0.0),
                                self._get_tick_field(tick, 'volume', 0))
                    self._raw_tick_accum_count = 0
                    self._raw_tick_last_output_time = now

        from ali2026v3_trading.diagnosis_service import record_tick_probe, DiagnosisProbeManager
        record_tick_probe('entry', instrument_id_raw, 0.0)

        last_price = self._get_tick_field(tick, 'last_price', 0.0)
        volume = self._get_tick_field(tick, 'volume', 0)
        # R24-P2-IV-01修复: 盘口深度bid/ask交叉验证
        bid_price1 = self._get_tick_field(tick, 'bid_price1', 0.0)
        ask_price1 = self._get_tick_field(tick, 'ask_price1', 0.0)
        if bid_price1 > 0 and ask_price1 > 0 and bid_price1 >= ask_price1:
            logging.warning("[R24-P2-IV-01] 盘口交叉异常: bid=%.4f >= ask=%.4f instrument=%s",
                          bid_price1, ask_price1, instrument_id_raw)
        open_interest = self._get_tick_field(tick, 'open_interest', 0)
        contract_type = 'option' if SubscriptionManager.is_option(instrument_id_raw) else 'future'
        DiagnosisProbeManager.on_tick_entry(instrument_id_raw, last_price, volume, open_interest, contract_type)

        return instrument_id_raw

    def _tick_check_layer(self, instrument_id_raw: str) -> bool:
        _cross_state = self._state_store.snapshot(['_is_running', '_is_paused'])
        is_running = _cross_state.get('_is_running', False)
        is_paused = _cross_state.get('_is_paused', False)

        if not is_running:
            _e2e = self._state_store.get_ref('_e2e_counters')
            if _e2e is not None:
                _e2e['dropped_not_running'] = _e2e.get('dropped_not_running', 0) + 1
            # R33-P0-04修复: 策略未运行丢弃tick时通知EventBus
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus
                get_global_event_bus().publish('tick_dropped', {'instrument_id': instrument_id_raw, 'reason': 'not_running', 'timestamp': time.time()})
            except Exception:
                pass
            return False

        if is_paused:
            _e2e = self._state_store.get_ref('_e2e_counters')
            if _e2e is not None:
                _e2e['dropped_paused'] = _e2e.get('dropped_paused', 0) + 1
            # R33-P0-04修复: 策略暂停丢弃tick时通知EventBus
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus
                get_global_event_bus().publish('tick_dropped', {'instrument_id': instrument_id_raw, 'reason': 'paused', 'timestamp': time.time()})
            except Exception:
                pass
            return False

        if hasattr(self, '_emit_historical_kline_diagnostic_on_first_tick'):
            self._emit_historical_kline_diagnostic_on_first_tick()

        if hasattr(self, '_check_and_start_historical_load_on_tick'):
            self._check_and_start_historical_load_on_tick()

        return True

    # R27-P0-FC-01修复: 实盘每tick硬时间止损检查
    def _check_hard_time_stop_live(self, instrument_id: str) -> None:
        """每tick检查持仓的两阶段硬时间止损(实盘主链路入口)"""
        try:
            _risk_svc = self._state_store.get_ref('_risk_service')
            if _risk_svc is None:
                _risk_svc = getattr(self, '_risk_service', None)
            if _risk_svc is None:
                return
            _pos_svc = self._state_store.get_ref('_position_service')
            if _pos_svc is None:
                _pos_svc = getattr(self, '_position_service', None)
            if _pos_svc is None:
                return
            positions = getattr(_pos_svc, 'positions', {})
            for _inst_id, pos_dict in positions.items():
                for _pid, rec in pos_dict.items():
                    if not isinstance(rec, dict):
                        continue
                    _open_time = rec.get('open_time') or rec.get('created_at')
                    if _open_time is None:
                        continue
                    _max_profit = rec.get('max_profit_pct', 0.0)
                    _slope = rec.get('profit_slope', 0.0)
                    _peak = rec.get('peak_profit_pct', 0.0)
                    _current = rec.get('current_profit_pct', 0.0)
                    _result = check_hard_time_stop_for_position(
                        _risk_svc, _pid, _open_time, _max_profit,
                        _slope, _peak, _current,
                        bar_time=self._state_store.get('_current_bar_time')
                    )
                    if _result is not None:
                        logging.warning("[R27-P0-FC-01] 硬时间止损触发: position=%s reason=%s", _pid, _result)
        except Exception as _hts_err:
            logging.error("[R22-P0-NEW] 硬时间止损检查整体异常(超时持仓可能无法止损!): %s", _hts_err, exc_info=True)

    def _tick_dispatch_layer(self, tick: Any, instrument_id_raw: str) -> None:
        """分发+统计层：处理Tick并更新统计

        R21-CC-P1-05修复: 统一异常处理入口，将分散的try/except收敛到此处，
        避免CTP回调链中异常处理分散导致部分异常被吞没。
        """
        from ali2026v3_trading.subscription_manager import SubscriptionManager

        try:
            # ✅ P1-12修复: 集成_normalize_tick统一Tick格式
            normalized_tick = self._normalize_tick(tick)
            exchange = normalized_tick.get('exchange', '')
            instrument_id = normalized_tick.get('instrument_id', '')

            self._process_tick_unified_path(tick)
            self._stats['last_tick_path'] = 'unified'

            # R27-P0-FC-01修复: 每tick检查两阶段硬时间止损(实盘主链路)
            self._check_hard_time_stop_live(instrument_id)

            if instrument_id and instrument_id not in self._e2e_first_tick_set:
                with self._lock:
                    if instrument_id not in self._e2e_first_tick_set:
                        self._e2e_first_tick_set.add(instrument_id)
                        # R21-NET-P1-07修复: 生产环境也必须记录first_tick_received计数
                        self._e2e_counters['first_tick_received'] += 1

            with self._tick_stats_lock:
                self._stats['total_ticks'] += 1
                self._tick_count += 1
                self._state_store.set('_tick_count', self._tick_count)

                instrument_type = 'option' if SubscriptionManager.is_option(instrument_id) else 'future'
                self._stats['tick_by_type'][instrument_type] += 1

                if exchange:
                    if exchange not in self._stats['tick_by_exchange']:
                        self._stats['tick_by_exchange'][exchange] = 0
                    self._stats['tick_by_exchange'][exchange] += 1

                if instrument_id:
                    if instrument_id not in self._stats['tick_by_instrument']:
                        self._stats['tick_by_instrument'][instrument_id] = 0
                    self._stats['tick_by_instrument'][instrument_id] += 1

            current_time = time.time()
            if current_time - self._last_tick_log_time >= self._tick_log_interval:
                self._output_periodic_summary()
                self._last_tick_log_time = current_time

        except Exception as e:
            # R21-CC-P1-05修复: 统一异常处理入口 — 所有tick分发层异常在此统一处理
            logging.error("[R21-CC-P1-05] Tick分发层统一异常: instrument=%s error=%s", instrument_id_raw, e, exc_info=True)
            with self._lock:
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now(_CHINA_TZ)

    def _process_tick_unified_path(self, tick: Any) -> None:
        """统一实盘/回测Tick处理主路径，减少双路径行为漂移。"""
        self._process_tick(tick)
        try:
            _norm_tick = self._normalize_tick(tick)
            _inst_id = _norm_tick.get('instrument_id', '')
            if _inst_id and self._is_option_instrument(_inst_id):
                from ali2026v3_trading.risk_service import get_risk_service
                _rs = get_risk_service()
                _gc = _rs._get_greeks_calculator() if hasattr(_rs, '_get_greeks_calculator') else None
                if _gc is not None:
                    _tick_price = float(_norm_tick.get('last_price', 0) or _norm_tick.get('price', 0))
                    _underlying_price = float(_norm_tick.get('underlying_price', 0))
                    _bar_ts = getattr(self, '_current_bar_time', None)
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
        except Exception as _gc_err:
            logging.debug("[GreeksIntegration] update_greeks_from_tick failed: %s", _gc_err)

    def _is_option_instrument(self, instrument_id: str) -> bool:
        if not instrument_id:
            return False
        _upper = instrument_id.upper()
        return any(p in _upper for p in ['-C', '-P', '_C', '_P', 'CALL', 'PUT'])

    # ========== Tick数据处理 ==========
    
    def _process_tick(self, tick: Any) -> None:
        """处理Tick数据（核心方法）
        
        Args:
            tick: Tick数据对象
            
        处理流程：
        1. 提取关键字段
        2. 输出诊断日志
        3. 分发到SubscriptionManager
        4. 更新RealTimeCache
        5. 保存到数据库并合成K线
        """
        try:
            # CC-02修复: 为每条tick生成correlation_id，支持全链路追踪
            _causal_tracker = CausalChainTracker.get_instance()
            _correlation_id = _causal_tracker.new_correlation_id()

            # 提取关键信息
            exchange = self._get_tick_field(tick, 'exchange', '')
            instrument_id = self._get_tick_field(tick, 'instrument_id', '')
            last_price = self._get_tick_field(tick, 'last_price', 0.0)
            volume = self._get_tick_field(tick, 'volume', 0)

            # R24-P0-IV-01修复: tick入口NaN/Inf/负值过滤，防止源头污染全链路
            # CC-01/CC-05修复: 使用validate_tick_cascade进行级联污染防护
            # PF-P1-03修复: math已在模块顶部导入，移除函数内重复import
            _tick_valid, _safe_price, _safe_vol = validate_tick_cascade(last_price, volume, instrument_id)
            if not _tick_valid:
                # CC-07修复: 记录污染事件到因果链追踪器
                try:
                    _causal_tracker.record_event(CausalEvent(
                        correlation_id=_correlation_id,
                        event_type="tick_contamination",
                        source_module="strategy_tick_handler",
                        contamination_flag=True,
                        payload_summary="invalid price=%s vol=%s inst=%s" % (last_price, volume, instrument_id),
                    ))
                    # CC-03修复: 触发级联清理
                    ContaminationGuard.get_instance().cascade_cleanup(instrument_id)
                except Exception:
                    pass
                if not hasattr(self, '_iv01_drop_count'):
                    self._iv01_drop_count = 0
                self._iv01_drop_count += 1
                if self._iv01_drop_count <= 10 or self._iv01_drop_count % 1000 == 0:
                    logging.warning("[R24-P0-IV-01/CC-05] Tick dropped by validate_tick_cascade: invalid last_price=%s volume=%s instrument=%s (total_dropped=%d)",
                                   last_price, volume, instrument_id, self._iv01_drop_count)
                return
            last_price = _safe_price
            if _safe_vol > 0:
                volume = _safe_vol
            if (not isinstance(volume, (int, float))
                or math.isnan(volume) or math.isinf(volume) or volume < 0):
                if not hasattr(self, '_iv01_vol_drop_count'):
                    self._iv01_vol_drop_count = 0
                self._iv01_vol_drop_count += 1
                if self._iv01_vol_drop_count <= 10 or self._iv01_vol_drop_count % 1000 == 0:
                    logging.warning("[R24-P0-IV-01] Tick dropped: invalid volume=%s instrument=%s (total_dropped=%d)",
                                   volume, instrument_id, self._iv01_vol_drop_count)
                volume = 0  # volume异常时置0而非丢弃整条tick（价格可能仍有效）

            # R24-P1-IV-09修复: tick时间戳单调性验证
            source_timestamp = self._get_tick_field(tick, 'timestamp', 0.0)
            try:
                _ts_float = float(source_timestamp) if source_timestamp is not None else 0.0
            except (TypeError, ValueError):
                _ts_float = 0.0
            if instrument_id and _ts_float > 0:
                _last_ts = self._last_tick_timestamp.get(instrument_id, 0.0)
                if _last_ts > 0 and _ts_float < _last_ts:
                    self._tick_timestamp_violation_count += 1
                    self._state_store.set('_tick_timestamp_violation_count', self._tick_timestamp_violation_count)
                    if self._tick_timestamp_violation_count <= 10 or self._tick_timestamp_violation_count % 1000 == 0:
                        logging.warning(
                            "[R24-P1-IV-09] Tick时间戳非单调: instrument=%s current_ts=%.6f < last_ts=%.6f, 累计%d次, 丢弃该tick",
                            instrument_id, _ts_float, _last_ts, self._tick_timestamp_violation_count,
                        )
                    self._last_tick_timestamp[instrument_id] = _ts_float
                    return
                self._last_tick_timestamp[instrument_id] = _ts_float

            # R24-P0-IV-07: 价格跳变实时检测（跳变超过阈值时丢弃tick，防止错误信号）
            _prev_price = self._price_jump_last_price.get(instrument_id)
            if _prev_price is not None and _prev_price > 0:
                _jump_pct = abs(last_price - _prev_price) / _prev_price
                if _jump_pct > self._price_jump_threshold:
                    self._price_jump_count += 1
                    self._state_store.set('_price_jump_count', self._price_jump_count)
                    if self._price_jump_count <= 10 or self._price_jump_count % 100 == 0:
                        logging.warning("[R24-P0-IV-07] Price jump detected: %s %.2f->%.2f (%.1f%% change, total_jumps=%d) - tick dropped",
                                       instrument_id, _prev_price, last_price, _jump_pct*100, self._price_jump_count)
                    return  # 跳变超过阈值，丢弃该tick，防止错误信号流入下游
            self._price_jump_last_price[instrument_id] = last_price

            # R23-FR-04-FIX: K线过期淘汰 — 检查tick时间戳新鲜度，过期K线数据拒绝进入指标计算
            try:
                from ali2026v3_trading.config_params import get_cached_params
                _cp = get_cached_params()
                _kline_max_age = _cp.get('kline_max_age_sec', 60) if isinstance(_cp, dict) else 60
            except Exception:
                _kline_max_age = 60
            if hasattr(tick, 'timestamp') or hasattr(tick, 'datetime'):
                _tick_ts = getattr(tick, 'timestamp', None) or getattr(tick, 'datetime', None)
                if _tick_ts is not None:
                    try:
                        _tick_epoch = float(_tick_ts) if isinstance(_tick_ts, (int, float)) else 0.0
                        if _tick_epoch > 0 and (time.time() - _tick_epoch) > _kline_max_age:
                            if not hasattr(self, '_kline_age_drop_count'):
                                self._kline_age_drop_count = 0
                            self._kline_age_drop_count += 1
                            if self._kline_age_drop_count <= 10 or self._kline_age_drop_count % 1000 == 1:
                                logging.warning("[R23-FR-04-FIX] K线过期淘汰: inst=%s age=%.1fs > max_age=%ds, drop_count=%d",
                                               instrument_id, time.time() - _tick_epoch, _kline_max_age, self._kline_age_drop_count)
                            return
                    except Exception as _kage_err:
                        logging.debug("[R22-P1-NEW] K线年龄淘汰异常(陈旧数据可能影响决策): %s", _kage_err)

            # [FR-P1-03-FIX] 指标数据年龄校验: 均线/波动率窗口右边界与当前tick时间差
            if hasattr(self, '_last_indicator_time') and self._last_indicator_time > 0:
                _indicator_age = time.time() - self._last_indicator_time
                _kline_max_age_ind = _kline_max_age if '_kline_max_age' in dir() else 60
                if _indicator_age > _kline_max_age_ind:
                    if not hasattr(self, '_indicator_age_warn_count'):
                        self._indicator_age_warn_count = 0
                    self._indicator_age_warn_count += 1
                    if self._indicator_age_warn_count <= 10 or self._indicator_age_warn_count % 1000 == 1:
                        logging.warning("[FR-P1-03-FIX] 指标数据年龄过期: age=%.1fs > max_age=%ds, inst=%s",
                                       _indicator_age, _kline_max_age_ind, instrument_id)
            self._last_indicator_time = time.time()

            # [FR-P1-04-FIX] bar时间一致性校验: 当前bar时间与最新tick时间差
            if hasattr(self, '_current_bar_time') and self._current_bar_time is not None:
                _bar_tick_diff = abs(time.time() - self._current_bar_time)
                _bar_consistency_threshold = _kline_max_age * 2 if '_kline_max_age' in dir() else 120
                if _bar_tick_diff > _bar_consistency_threshold:
                    if not hasattr(self, '_bar_time_warn_count'):
                        self._bar_time_warn_count = 0
                    self._bar_time_warn_count += 1
                    if self._bar_time_warn_count <= 10:
                        logging.warning("[FR-P1-04-FIX] bar时间不一致: diff=%.1fs > threshold=%ds, inst=%s",
                                       _bar_tick_diff, _bar_consistency_threshold, instrument_id)

            # ========== 分子统计：平台推送过数据的合约（在任何内部处理之前）==========
            if instrument_id:
                _stor = self._state_store.get_ref('storage')
                if _stor is not None and hasattr(_stor, 'subscription_manager'):
                    sm = _stor.subscription_manager
                    if sm and hasattr(sm, 'record_tick_received'):
                        sm.record_tick_received(instrument_id)
            
            # ========== 详细日志输出（恢复原有功能） ==========
            # 1. 每10秒输出一次Tick诊断信息
            now_ts = time.time()
            
            # ========== 订单流数据喂入（L4微结构感知层） ==========
            try:
                if not hasattr(self, '_order_flow_bridge'):
                    from ali2026v3_trading.order_flow_bridge import get_order_flow_bridge
                    self._order_flow_bridge = get_order_flow_bridge()
                bid_price = self._get_tick_field(tick, 'bid_price1', 0.0)
                ask_price = self._get_tick_field(tick, 'ask_price1', 0.0)
                mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else last_price

                # MS-07: 提取L2深度数据（1-5档）
                l2_bids = []
                l2_asks = []
                for i in range(1, 6):
                    bp = self._get_tick_field(tick, f'bid_price{i}', 0.0)
                    ap = self._get_tick_field(tick, f'ask_price{i}', 0.0)
                    bv = self._get_tick_field(tick, f'bid_volume{i}', 0)
                    av = self._get_tick_field(tick, f'ask_volume{i}', 0)
                    if bp > 0 and bv > 0:
                        l2_bids.append((bp, bv))
                    if ap > 0 and av > 0:
                        l2_asks.append((ap, av))

                self._order_flow_bridge.on_tick_feed(
                    instrument_id, last_price, volume, exchange,
                    bid_price=bid_price, ask_price=ask_price,
                    mid_price=mid_price,
                    l2_bids=l2_bids, l2_asks=l2_asks,
                )
            except Exception as ofb_e:
                if not hasattr(self, '_ofb_error_logged'):
                    self._ofb_error_logged = True
                    logging.debug("[_process_tick] OrderFlowBridge feed error: %s", ofb_e)
            
            if (now_ts - self._tick_debug_last_ts >= 10.0) and instrument_id:
                diag_on = False
                try:
                    from ali2026v3_trading.diagnosis_service import is_monitored_contract
                    diag_on = is_monitored_contract(instrument_id)
                except Exception as e:
                    # ✅ P1修复：添加debug日志
                    logging.debug("[_process_tick] Failed to check monitored contract: %s", e)
                    diag_on = False
                if diag_on:
                    logging.info("[TickDiag] inst=%s exch=%s price=%s vol=%s", instrument_id, exchange, last_price, volume)
                self._tick_debug_last_ts = now_ts
            
            # 2. 如果开启debug_output，每条Tick都记录
            params_dict = getattr(self, 'params', {})
            if isinstance(params_dict, dict):
                debug_output = params_dict.get('debug_output', False)
            else:
                debug_output = getattr(params_dict, 'debug_output', False)
            
            if debug_output and instrument_id:
                logging.debug("[Tick] %s.%s price=%s vol=%s", exchange, instrument_id, last_price, volume)
            # ===============================================
            
            # ✅ 阶段5.3: 使用SubscriptionManager统一Tick入口
            _sub_mgr = None
            _stor2 = self._state_store.get_ref('storage')
            if _stor2 is not None:
                _sub_mgr = getattr(_stor2, 'subscription_manager', None)
            if _sub_mgr:
                try:
                    _sub_mgr.on_tick(instrument_id, last_price, volume, is_replay=False)
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('dispatched', instrument_id, last_price)
                except Exception as exc:
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('error', instrument_id, last_price, str(exc))
                    logging.warning("[R16-P1-8.1] SubscriptionManager分发异常: %s", exc)

            if _sub_mgr and not hasattr(self, '_tick_route_confirmed'):
                self._tick_route_confirmed = True
                logging.info("[TickRoute] Tick数据成功路由到subscription_manager: %s", instrument_id)

            if bool(getattr(self, '_historical_load_in_progress', False)):
                self._dispatch_tick_degraded(tick, instrument_id, last_price, volume, exchange)
            else:
                self._dispatch_tick(tick, instrument_id, last_price, volume, exchange)

            # R23-SM-09-FIX: 每个tick处理时检测HMM状态驻留异常
            _hmm_state = getattr(self, '_hmm_current_state', None)
            if _hmm_state is not None:
                check_hmm_dwell_anomaly(self, _hmm_state)

            if not bool(getattr(self, '_historical_load_in_progress', False)):
                try:
                    if not hasattr(self, '_pullback_mgr_ref'):
                        try:
                            from ali2026v3_trading.box_spring_strategy import BoxSpringStrategy
                            _bss = getattr(self, '_box_spring', None) or getattr(self, '_spring', None) or getattr(self, '_box_spring_strategy', None)
                            if _bss and hasattr(_bss, '_pullback_mgr'):
                                self._pullback_mgr_ref = _bss._pullback_mgr
                            else:
                                self._pullback_mgr_ref = None
                                logging.debug("[R22-TS-02] pullback_mgr未找到，属性搜索完成")
                        except Exception as _attr_err:
                            self._pullback_mgr_ref = None
                            logging.debug("[R22-TS-02] pullback_mgr属性查找异常: %s", _attr_err)
                    _pbm = self._pullback_mgr_ref if hasattr(self, '_pullback_mgr_ref') else None
                    if _pbm and getattr(_pbm, 'tick_tracking', False) and instrument_id and last_price > 0:
                        retriggered = _pbm.process_tick(instrument_id, last_price)
                        if retriggered:
                            logging.debug("[P0-2] PullbackManager.process_tick retriggered %d signals for %s", len(retriggered), instrument_id)
                except Exception as _pbm_err:
                    if not hasattr(self, '_pbm_err_logged'):
                        self._pbm_err_logged = True
                        logging.debug("[P0-2] PullbackManager.process_tick integration error: %s", _pbm_err)
            
        except Exception as e:
            logging.error("[StrategyCoreService._process_tick] Error: %s", e)
            logging.exception("[StrategyCoreService._process_tick] Stack:")
            if not hasattr(self, '_tick_drop_count'):
                self._tick_drop_count = 0
            self._tick_drop_count += 1
            if self._tick_drop_count <= 10 or self._tick_drop_count % 1000 == 0:
                logging.warning("[TickDrop] Total ticks dropped due to unhandled errors: %d", self._tick_drop_count)
        # ✅ P2#93修复：移除finally中每次tick都调用的close_connection()空操作
        # DuckDB使用线程本地连接，无需每次tick后关闭
    
    def _dispatch_tick_degraded(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        with self._probe_lock:
            if not hasattr(self, '_degraded_tick_count'):
                self._degraded_tick_count = 0
            self._degraded_tick_count += 1

        if hasattr(self, 'data_service') and self.data_service:
            try:
                rc = getattr(self.data_service, 'realtime_cache', None)
                if rc:
                    rc.update_tick(instrument_id, last_price, time.time(), volume)
            except Exception as _rc_err:
                logging.debug("[DegradedDispatch] realtime_cache.update_tick failed: %s", _rc_err)

        # R15-P1-PERF-01修复: tick热路径已使用新构建dict而非dict(tick)拷贝，无需修改
        # R17-P1-PERF-01修复: tick字段一次性提取避免重复构建
        # CC-02修复: tick_data携带correlation_id传播到下游
        tick_data = {
            'timestamp': self._get_tick_field(tick, 'timestamp', time.time()),
            'instrument_id': instrument_id,
            'exchange': exchange,
            'last_price': last_price,
            'volume': volume,
            'correlation_id': _correlation_id,
        }

        shard_idx = self._route_shard_index(instrument_id)
        shard_lock = self._get_shard_lock(shard_idx)
        with shard_lock:
            self._shard_buffers.setdefault(shard_idx, []).append(tick_data)

        if self._degraded_tick_count % 10000 == 1:
            logging.info("[DegradedDispatch] 历史加载期间降级分发: instrument=%s, 累计降级=%d",
                         instrument_id, self._degraded_tick_count)

    # ========== 周期性汇总 ==========
    
    def _output_periodic_summary(self) -> None:
        """输出3分钟定期状态汇总
        
        包含：
        - 运行时长
        - Tick统计（总数、分类、交易所、合约）
        - K线统计
        - 交易和信号统计
        - 错误统计
        - 策略状态
        """
        try:
            _cross_state = self._state_store.snapshot(['_is_running', '_is_paused', '_state'])
            _ss_is_running = _cross_state.get('_is_running', False)
            _ss_is_paused = _cross_state.get('_is_paused', False)
            _ss_state = _cross_state.get('_state')

            _ss_stats = self._state_store.get_ref('_stats')
            if _ss_stats is None:
                _ss_stats = getattr(self, '_stats', {})
            uptime = datetime.now(_CHINA_TZ) - _ss_stats['start_time'] if _ss_stats['start_time'] else timedelta(0)
            uptime_str = str(uptime).split('.')[0]
            elapsed_seconds = uptime.total_seconds() if uptime else 1.0
            tick_rate = _ss_stats['total_ticks'] / max(elapsed_seconds, 1)
            
            kline_summary_line, historical_detail_line = "", ""
            if hasattr(self, '_get_historical_kline_summary_lines'):
                kline_summary_line, historical_detail_line = self._get_historical_kline_summary_lines()
            
            tick_by_type = _ss_stats.get('tick_by_type', {'future': 0, 'option': 0})
            tick_by_exchange = _ss_stats.get('tick_by_exchange', {})
            tick_by_instrument = _ss_stats.get('tick_by_instrument', {})
            
            sorted_exchanges = sorted(tick_by_exchange.items(), key=lambda x: x[1], reverse=True)[:5]
            sorted_instruments = sorted(tick_by_instrument.items(), key=lambda x: x[1], reverse=True)[:10]
            
            summary = f"""
{'='*80}
【3分钟状态汇总】{datetime.now(_CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}
运行时长：{uptime_str}
Tick数据：总计 {_ss_stats['total_ticks']:,} 个 | 速率 {tick_rate:.1f} ticks/s
  - 期货：{tick_by_type.get('future', 0):,} 个
  - 期权：{tick_by_type.get('option', 0):,} 个
{kline_summary_line}
交易数量：总计 {_ss_stats['total_trades']:,} 笔
信号数量：总计 {_ss_stats['total_signals']:,} 个
错误统计：{_ss_stats['errors_count']} 次
最后错误：{_ss_stats['last_error_time'].strftime('%H:%M:%S') if _ss_stats['last_error_time'] else '无'}
策略状态：{getattr(_ss_state, 'value', 'UNKNOWN') if _ss_state is not None else 'UNKNOWN'}
是否运行：{_ss_is_running}
是否暂停：{_ss_is_paused}

{historical_detail_line}

【交易所分布】
{''.join([f'  - {exchange}: {count:,} 个\n' for exchange, count in sorted_exchanges])}

【合约活跃度 Top 10】
{''.join([f'  - {inst}: {count:,} 个\n' for inst, count in sorted_instruments])}
{'='*80}
"""
            logging.info(summary)

            try:
                _spm = self._state_store.get_ref('_state_param_manager')
                if _spm is None:
                    _spm = getattr(self, '_state_param_manager', None)
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
            except Exception as spm_e:
                logging.debug("[StateRoute] StateParamManager output error: %s", spm_e)

            try:
                _sml = self._state_store.get_ref('_safety_meta_layer')
                if _sml is None:
                    _sml = getattr(self, '_safety_meta_layer', None)
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
            except Exception as safety_e:
                logging.debug("[SafetyMeta] SafetyMetaLayer output error: %s", safety_e)

            try:
                if hasattr(self, '_order_flow_bridge') and self._order_flow_bridge:
                    ofb_stats = self._order_flow_bridge.get_stats()
                    logging.info(
                        "[OrderFlow] ticks=%d, depth=%d, buy=%d, sell=%d, products=%d, cache_hit=%d",
                        ofb_stats.get('ticks_fed', 0),
                        ofb_stats.get('depth_updates', 0),
                        ofb_stats.get('direction_buy', 0),
                        ofb_stats.get('direction_sell', 0),
                        ofb_stats.get('products_tracked', 0),
                        ofb_stats.get('cache_hits', 0),
                    )
                    products = self._order_flow_bridge.get_products()
                    for prod in products[:5]:
                        fc = self._order_flow_bridge.get_flow_consistency(prod)
                        ofi = self._order_flow_bridge.get_ofi(prod)
                        logging.info(
                            "[OrderFlow] %s: consistency=%.3f, OFI=%.3f",
                            prod, fc, ofi
                        )
            except Exception as ofb_e:
                logging.debug("[OrderFlow] output error: %s", ofb_e)
            
            # 输出订阅成功率统计
            _storage_ref = self._state_store.get_ref('storage')
            if _storage_ref is not None and hasattr(_storage_ref, 'subscription_manager'):
                sm = _storage_ref.subscription_manager
                if sm and hasattr(sm, 'log_subscription_success_summary'):
                    sm.log_subscription_success_summary()
                    
        except Exception as e:
            logging.error("[Periodic Summary] 输出失败：%s", e)
    
    def _flush_tick_buffer(self) -> None:
        total_flushed = 0
        with self._shard_buffers_lock:
            shard_snapshots = {}
            for shard_idx, buf in list(self._shard_buffers.items()):
                if buf:
                    shard_snapshots[shard_idx] = buf[:]
                    buf.clear()

        for shard_idx, ticks in shard_snapshots.items():
            logging.info("[_flush_tick_buffer] Flushing shard=%d %d ticks", shard_idx, len(ticks))
            shard_lock = self._get_shard_lock(shard_idx)
            failed = []
            for tick_item in ticks:
                try:
                    _storage_flush = self._state_store.get_ref('storage')
                    if _storage_flush is None:
                        _storage_flush = getattr(self, 'storage', None)
                    if _storage_flush:
                        _storage_flush.process_tick(tick_item)
                    _e2e_ref = self._state_store.get_ref('_e2e_counters')
                    _lock_ref = self._state_store.get_ref('_lock')
                    if _e2e_ref is not None and _lock_ref is not None:
                        with _lock_ref:
                            _e2e_ref['persisted_count'] += 1
                            _e2e_sp = self._state_store.get_ref('_e2e_shard_persisted')
                            if _e2e_sp is not None:
                                _e2e_sp[shard_idx] = _e2e_sp.get(shard_idx, 0) + 1
                    elif hasattr(self, '_e2e_counters'):
                        with self._lock:
                            self._e2e_counters['persisted_count'] += 1
                            if hasattr(self, '_e2e_shard_persisted'):
                                self._e2e_shard_persisted[shard_idx] = self._e2e_shard_persisted.get(shard_idx, 0) + 1
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('saved', tick_item.get('instrument_id', ''), tick_item.get('last_price', 0.0))
                    total_flushed += 1
                except Exception as e:
                    logging.error("[_flush_tick_buffer] shard=%d: %s", shard_idx, e)
                    failed.append(tick_item)
            if failed:
                with shard_lock:
                    self._shard_buffers.setdefault(shard_idx, []).extend(failed)
                logging.warning("[_flush_tick_buffer] shard=%d %d/%d 回写", shard_idx, len(failed), len(ticks))
        if total_flushed:
            logging.info("[_flush_tick_buffer] Total flushed: %d", total_flushed)

    def _save_tick_snapshot(self):
        """覆盖式快照：将RealTimeCache全部tick写入磁盘，仅保留最后状态"""
        if not self._snapshot_path:
            try:
                ds = self._state_store.get_ref('data_service') or self._state_store.get_ref('storage')
                if ds is None:
                    ds = getattr(self, 'data_service', None)
                    if ds is None:
                        _stor = self._state_store.get_ref('storage')
                        if _stor is None:
                            _stor = getattr(self, 'storage', None)
                        ds = getattr(_stor, '_data_service', None) if _stor else None
                if ds and hasattr(ds, '_data_dir'):
                    _os = os  # PERF-P2-03/10修复: 使用模块级os
                    self._snapshot_path = _os.path.join(ds._data_dir, 'tick_snapshot.json')
            except Exception as _snap_err:
                logging.debug("[_save_tick_snapshot] Path resolution failed: %s", _snap_err)
                return
        if not self._snapshot_path:
            return
        try:
            rc = self._state_store.get_ref('data_service') or self._state_store.get_ref('storage')
            if rc is None:
                rc = getattr(self, 'data_service', None)
                if rc is None:
                    _stor2 = self._state_store.get_ref('storage')
                    if _stor2 is None:
                        _stor2 = getattr(self, 'storage', None)
                    rc = getattr(_stor2, '_data_service', None) if _stor2 else None
            rc = getattr(rc, 'realtime_cache', None) if rc else None
            if not rc:
                return
            from ali2026v3_trading.serialization_utils import json_dumps as _json_dumps
            _os = os  # PERF-P2-03/10修复: 使用模块级os
            from datetime import datetime as _dt
            snapshot = {'timestamp': _dt.now(_CHINA_TZ).isoformat(), 'ticks': {}}
            with rc._lock:
                for sym, tick in rc._latest_ticks.items():
                    ts = tick.get('timestamp')
                    ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                    snapshot['ticks'][sym] = {
                        'p': tick.get('price', 0), 't': ts_str, 'v': tick.get('volume', 0),
                        'b': tick.get('bid_price', 0), 'a': tick.get('ask_price', 0),
                        'ot': tick.get('option_type'), 'sp': tick.get('strike_price'),
                        'iid': tick.get('internal_id'),
                    }
            _os.makedirs(_os.path.dirname(self._snapshot_path), exist_ok=True)
            with open(self._snapshot_path, 'w', encoding='utf-8') as f:
                f.write(_json_dumps(snapshot))
        except Exception as e:
            logging.debug("[_save_tick_snapshot] error: %s", e)

    # ✅ P1#25修复：静态方法作为atexit回调，通过弱引用访问实例
    @staticmethod
    def _atexit_flush(weak_self):
        self = weak_self()
        if self is None:
            return
        try:
            if hasattr(self, '_shard_buffers') and self._shard_buffers:
                self._flush_tick_buffer()
                logging.info("[atexit] Tick缓冲区已刷写")
            if hasattr(self, '_save_tick_snapshot'):
                self._save_tick_snapshot()
                logging.info("[atexit] Tick快照已保存")
        except Exception as e:
            logging.error("[atexit] Tick缓冲区刷写失败: %s", e)
    
    # ✅ 序号116-118修复：统一Tick分发入口
    def _dispatch_tick(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:  # [R22-TS-P1-02]
        """
        统一Tick分发入口（序号116-118修复）
        
        Args:
            tick: Tick对象
            instrument_id: 合约ID
            last_price: 最新价格
            volume: 成交量
            exchange: 交易所
        """
        # 1. 更新RealTimeCache（内存缓存 + WAL保护）
        try:
            ds = getattr(self, '_data_service', None)
            if ds is None:
                ds = getattr(self, 'data_service', None)
            if ds and hasattr(ds, 'realtime_cache') and ds.realtime_cache:
                from datetime import datetime as _dt
                bid_p = self._get_tick_field(tick, 'bid_price1', 0.0)
                ask_p = self._get_tick_field(tick, 'ask_price1', 0.0)
                ds.realtime_cache.update_tick(
                    symbol=instrument_id, price=last_price,
                    timestamp=_dt.now(_CHINA_TZ), volume=volume,
                    bid_price=bid_p, ask_price=ask_p
                )
        except Exception as e:
            logging.debug("[_dispatch_tick] RealTimeCache update skipped: %s", e)

        # 2. 保存Tick数据到数据库并合成K线
        _stor3 = self._state_store.get_ref('storage')
        if _stor3 is not None:
            try:
                tick_timestamp = self._get_tick_field(tick, 'timestamp', time.time())
                turnover = self._get_tick_field(tick, 'amount', 0.0)
                open_interest = self._get_tick_field(tick, 'open_interest', 0.0)
                tick_data = {
                    'timestamp': tick_timestamp,
                    'instrument_id': instrument_id,
                    'exchange': exchange,
                    'last_price': last_price,
                    'volume': volume,
                    'amount': turnover,
                    'open_interest': open_interest,
                    'bid_price1': self._get_tick_field(tick, 'bid_price1'),
                    'ask_price1': self._get_tick_field(tick, 'ask_price1'),
                }
                
                # 第二轮优化：分片路由 — 按品种代码分配到独立Shard
                shard_idx = self._route_shard_index(instrument_id)
                shard_lock = self._get_shard_lock(shard_idx)

                with shard_lock:
                    self._shard_buffers.setdefault(shard_idx, []).append(tick_data)
                    self._shard_last_flush_time.setdefault(shard_idx, time.time())

                    buffer_size = len(self._shard_buffers[shard_idx])
                    threshold = self._tick_buffer_threshold
                    should_flush = buffer_size >= threshold
                    if not should_flush and buffer_size > 0:
                        elapsed = time.time() - self._shard_last_flush_time.get(shard_idx, self._last_buffer_flush_time)
                        if elapsed >= self._tick_buffer_flush_interval:
                            should_flush = True

                    if should_flush:
                        ticks_to_flush = self._shard_buffers[shard_idx][:]
                        self._shard_buffers[shard_idx].clear()
                        self._last_buffer_flush_time = time.time()
                        self._shard_last_flush_time[shard_idx] = self._last_buffer_flush_time
                    else:
                        ticks_to_flush = None

                with self._lock:
                    self._e2e_counters['enqueued_count'] += 1
                    if hasattr(self, '_e2e_shard_enqueued'):
                        self._e2e_shard_enqueued[shard_idx] = self._e2e_shard_enqueued.get(shard_idx, 0) + 1

                with self._probe_lock:
                    self._snapshot_tick_count += 1
                    should_snapshot = (self._snapshot_tick_count % self._snapshot_interval == 0)
                
                if should_snapshot:
                    self._save_tick_snapshot()

                if should_flush and ticks_to_flush:
                    failed_ticks = []
                    for tick_item in ticks_to_flush:
                        try:
                            _stor3.process_tick(tick_item)
                            with self._lock:
                                self._e2e_counters['persisted_count'] += 1
                                if hasattr(self, '_e2e_shard_persisted'):
                                    self._e2e_shard_persisted[shard_idx] = self._e2e_shard_persisted.get(shard_idx, 0) + 1
                            from ali2026v3_trading.diagnosis_service import record_tick_probe
                            record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                        except Exception as e:
                            logging.warning("[_dispatch_tick] shard=%d flush失败: %s", shard_idx, e)
                            failed_ticks.append(tick_item)
                    if failed_ticks:
                        with shard_lock:
                            self._shard_buffers.setdefault(shard_idx, []).extend(failed_ticks)
                        logging.warning("[_dispatch_tick] shard=%d %d/%d 回写buffer", shard_idx, len(failed_ticks), len(ticks_to_flush))
            except Exception as e:
                from ali2026v3_trading.diagnosis_service import record_tick_probe
                record_tick_probe('error', instrument_id, last_price, f"Storage: {e}")
                logging.exception("[_dispatch_tick] Stack trace for instrument: %s", instrument_id)
        
        # 3. 分发Tick给PositionService做止盈止损检查
        try:
            from ali2026v3_trading.position_service import get_position_service
            pos_svc = get_position_service()
            pos_svc.on_tick(tick)
        except Exception as pos_e:
            logging.warning("[_dispatch_tick] PositionService.on_tick failed: %s", pos_e)

        # 4. 分发Tick给HFT增强引擎(七大竞争优势)
        self._dispatch_hft_tick(tick, instrument_id, last_price, volume, exchange)

    def _dispatch_hft_tick(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        try:
            hft = self._state_store.get_ref('_hft_engine')
            if hft is None:
                hft = getattr(self, '_hft_engine', None)
            if hft is None:
                try:
                    _ensure_hft = getattr(self, '_ensure_hft_engine', None)
                    if _ensure_hft:
                        _ensure_hft()
                    hft = self._state_store.get_ref('_hft_engine')
                    if hft is None:
                        hft = getattr(self, '_hft_engine', None)
                except Exception:
                    pass
            if hft is not None:
                bid_price = self._get_tick_field(tick, 'bid_price1', 0.0)
                ask_price = self._get_tick_field(tick, 'ask_price1', 0.0)
                direction_raw = ''
                try:
                    from ali2026v3_trading.subscription_manager import SubscriptionManager
                    if SubscriptionManager.is_option(instrument_id):
                        direction_raw = 'buy'
                    else:
                        rc = None
                        ds = self._state_store.get_ref('_data_service')
                        if ds is None:
                            ds = getattr(self, '_data_service', None) or getattr(self, 'data_service', None)
                        if ds:
                            rc = getattr(ds, 'realtime_cache', None)
                        if rc:
                            tick_data = rc.get_tick(instrument_id)
                            if tick_data:
                                direction_raw = tick_data.get('direction', '')
                except Exception:
                    direction_raw = ''

                product = ''
                try:
                    from ali2026v3_trading.shared_utils import extract_product_code
                    product = extract_product_code(instrument_id)
                except Exception:
                    pass

                width_resonance = 0.0
                try:
                    ps = None
                    try:
                        from ali2026v3_trading.params_service import get_params_service
                        ps = get_params_service()
                    except Exception:
                        pass
                    if ps:
                        meta = ps.get_instrument_meta_by_id(instrument_id)
                        if meta:
                            uf_id = meta.get('underlying_future_id')
                            if not uf_id:
                                uf_id = meta.get('internal_id')
                            if uf_id:
                                tts = self._state_store.get_ref('t_type_service')
                                if tts is None:
                                    tts = getattr(self, 't_type_service', None)
                                if tts is None:
                                    try:
                                        from ali2026v3_trading.t_type_service import get_t_type_service
                                        tts = get_t_type_service()
                                    except Exception:
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
                except Exception:
                    pass

                resonance_strength = 0.0
                prev_resonance_strength = 0.0
                try:
                    spm = self._state_store.get_ref('_state_param_manager')
                    if spm is None:
                        spm = getattr(self, '_state_param_manager', None)
                    if spm:
                        spm.update_market_context(width_resonance, last_price)
                        resonance_strength = getattr(spm, '_last_resonance_strength', 0.0) or 0.0
                        prev_resonance_strength = getattr(spm, '_prev_resonance_strength', 0.0) or 0.0
                except Exception:
                    pass

                current_state = 'other'
                prev_state = 'other'
                try:
                    spm2 = self._state_store.get_ref('_state_param_manager')
                    if spm2 is None:
                        spm2 = getattr(self, '_state_param_manager', None)
                    if spm2:
                        current_state = spm2.get_current_state()
                        prev_state = getattr(spm2, '_prev_state', 'other')
                except Exception:
                    pass

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
                            self._execute_pursuit_entry(hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)
                        elif action == 'ADD_POSITION':
                            self._execute_pursuit_add(hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)

                    pursuit_exit = hft_result.get('pursuit_exit')
                    if pursuit_exit:
                        logging.info("[HFT] pursuit exit: %s %s reason=%s pnl=%.2f",
                                     pursuit_exit.get('instrument_id', ''),
                                     pursuit_exit.get('direction', ''),
                                     pursuit_exit.get('reason', ''),
                                     pursuit_exit.get('pnl', 0.0))
                        try:
                            self._execute_pursuit_exit(hft, pursuit_exit, instrument_id)
                        except Exception as ex_e:
                            logging.debug("[_dispatch_hft_tick] HFT pursuit exit execution error: %s", ex_e)

                    arbitrage_signal = hft_result.get('arbitrage_signal')
                    if arbitrage_signal:
                        self._handle_arbitrage_signal(arbitrage_signal, instrument_id)

                    transition_signal = hft_result.get('transition_signal')
                    if transition_signal:
                        hft_mid_price = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else last_price
                        self._handle_transition_signal(transition_signal, instrument_id, last_price, mid_price=hft_mid_price)

                    smart_money_signal = hft_result.get('smart_money_signal')
                    if smart_money_signal:
                        self._handle_smart_money_signal(smart_money_signal, instrument_id)

                    signal_filter_result = hft_result.get('signal_filter')
                    if signal_filter_result and signal_filter_result.get('threshold_crossed'):
                        self._handle_filtered_signal(signal_filter_result, instrument_id, last_price)
        except Exception as hft_e:
            self._hft_dispatch_error_count = getattr(self, '_hft_dispatch_error_count', 0) + 1
            if self._hft_dispatch_error_count <= 10 or self._hft_dispatch_error_count % 100 == 0:
                logging.warning("[R16-P1-8.2] HFT engine分发异常(累计%d次): %s",
                               self._hft_dispatch_error_count, hft_e)

    def _execute_pursuit_exit(self, hft: Any, exit_signal: Dict[str, Any], instrument_id: str) -> None:
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
                _os = self._state_store.get_ref('_order_service')
                if _os is None:
                    _os = getattr(self, '_order_service', None)
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
        try:
            _ensure_os = getattr(self, '_ensure_order_service', None)
            if _ensure_os:
                _ensure_os()
            order_svc = self._state_store.get_ref('_order_service')
            if order_svc is None:
                order_svc = getattr(self, '_order_service', None)
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
                except Exception as def_e:
                    logging.warning("[HFT] send_defensive_order failed, fallback to signal: %s", def_e)  # R24-P1-DF-03修复: debug→warning
            sig_svc = self._state_store.get_ref('_signal_service')
            if sig_svc is None:
                sig_svc = getattr(self, '_signal_service', None)
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal_service import SignalService
                    sig_svc = SignalService()
                except Exception as _sig_init_err:
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

    def _execute_pursuit_entry(self, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                                instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        direction = pursuit_signal.get('direction', '')
        signal_volume = pursuit_signal.get('volume', 1)
        price = pursuit_signal.get('price', last_price)
        strength_delta = pursuit_signal.get('strength_delta', 0.0)
        estimated_plr = pursuit_signal.get('estimated_plr', 0.0)
        min_pursuit_plr = self._state_store.get('_min_pursuit_plr')
        if min_pursuit_plr is None:
            min_pursuit_plr = getattr(self, '_min_pursuit_plr', 1.5)
        if min_pursuit_plr > 0 and estimated_plr > 0 and estimated_plr < min_pursuit_plr:
            logging.debug("[HFT] pursuit entry blocked: estimated_plr=%.2f < min=%.2f for %s",
                          estimated_plr, min_pursuit_plr, instrument_id)
            return
        if not self._check_hft_open_risk(instrument_id, direction, price, signal_volume, pursuit_signal):
            return
        # P1-5修复: 生产实盘HFT确认tick数，与回测引擎hft_confirm_ticks对齐
        try:
            from ali2026v3_trading.config_params import get_param
            _hft_confirm_ticks = int(get_param('hft_confirm_ticks', 3))
        except Exception:
            _hft_confirm_ticks = 3
        _confirm_count = pursuit_signal.get('confirm_ticks', 1)
        if _confirm_count < _hft_confirm_ticks:
            logging.debug("[HFT] pursuit entry: confirm_ticks=%d < hft_confirm_ticks=%d, deferring %s",
                         _confirm_count, _hft_confirm_ticks, instrument_id)
            return
        try:
            _ensure_os2 = getattr(self, '_ensure_order_service', None)
            if _ensure_os2:
                _ensure_os2()
            order_svc = self._state_store.get_ref('_order_service')
            if order_svc is None:
                order_svc = getattr(self, '_order_service', None)
            if not order_svc:
                logging.warning("[HFT] pursuit entry: no order_service available, %s %s", instrument_id, direction)
                return
            bid_price = self._get_tick_field(tick, 'bid_price1', 0.0)
            ask_price = self._get_tick_field(tick, 'ask_price1', 0.0)
            bids = [(bid_price, 100)] if bid_price > 0 else None
            asks = [(ask_price, 100)] if ask_price > 0 else None
            signal_strength = min(abs(strength_delta) / 0.3, 1.0)
            order_ids = order_svc.send_order_split(
                instrument_id=instrument_id, volume=signal_volume, price=price,
                direction=direction, action='OPEN', exchange=exchange,
                signal_strength=signal_strength, bids=bids, asks=asks,
                open_reason='CORRECT_RESONANCE',
                signal_id=pursuit_signal.get('signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
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
        except Exception as e:
            logging.warning("[HFT] _execute_pursuit_entry failed: %s", e)

    def _execute_pursuit_add(self, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                              instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        direction = pursuit_signal.get('direction', '')
        add_volume = pursuit_signal.get('volume', 1)
        price = pursuit_signal.get('price', last_price)
        if not self._check_hft_open_risk(instrument_id, direction, price, add_volume, pursuit_signal):
            return
        try:
            _ensure_os3 = getattr(self, '_ensure_order_service', None)
            if _ensure_os3:
                _ensure_os3()
            order_svc = self._state_store.get_ref('_order_service')
            if order_svc is None:
                order_svc = getattr(self, '_order_service', None)
            if not order_svc:
                logging.warning("[HFT] pursuit add: no order_service available, %s %s", instrument_id, direction)
                return
            bid_price = self._get_tick_field(tick, 'bid_price1', 0.0)
            ask_price = self._get_tick_field(tick, 'ask_price1', 0.0)
            bids = [(bid_price, 100)] if bid_price > 0 else None
            asks = [(ask_price, 100)] if ask_price > 0 else None
            order_ids = order_svc.send_order_split(
                instrument_id=instrument_id, volume=add_volume, price=price,
                direction=direction, action='OPEN', exchange=exchange,
                signal_strength=0.9, bids=bids, asks=asks,
                open_reason='CORRECT_RESONANCE',
                signal_id=pursuit_signal.get('signal_id', ''),  # R24-P0-TR-01修复: signal_id链路贯通
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
        except Exception as e:
            logging.warning("[HFT] _execute_pursuit_add failed: %s", e)

    def _handle_arbitrage_signal(self, arbitrage_signal: Dict[str, Any], instrument_id: str) -> None:
        direction = arbitrage_signal.get('direction', '')
        deviation = arbitrage_signal.get('deviation_bps', 0.0)
        confidence = arbitrage_signal.get('confidence', 0.0)
        if confidence < 0.6:
            return
        logging.info("[HFT] microstructure arbitrage: %s dir=%s deviation=%.1fbps confidence=%.2f",
                     instrument_id, direction, deviation, confidence)
        try:
            sig_svc = self._state_store.get_ref('_signal_service')
            if sig_svc is None:
                sig_svc = getattr(self, '_signal_service', None)
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal_service import SignalService
                    sig_svc = SignalService()
                except Exception as _arb_sig_err:
                    logging.debug("[R22-P1-NEW] 套利SignalService初始化失败(交易机会可能丢失): %s", _arb_sig_err)
            if sig_svc and abs(deviation) > 10:
                signal_type = 'OPEN_LONG' if direction == 'BUY' else 'OPEN_SHORT'
                sig_svc.generate_signal(
                    instrument_id=instrument_id, signal_type=signal_type,
                    price=arbitrage_signal.get('entry_price', 0.0), volume=1,
                    reason='hft_arbitrage_deviation', priority=7, cooldown_seconds=5,
                )
        except Exception as e:
            logging.debug("[HFT] _handle_arbitrage_signal error: %s", e)

    def _handle_transition_signal(self, transition_signal: Dict[str, Any],
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
                sig_svc = self._state_store.get_ref('_signal_service')
                if sig_svc is None:
                    sig_svc = getattr(self, '_signal_service', None)
                if sig_svc is None:
                    try:
                        from ali2026v3_trading.signal_service import SignalService
                        sig_svc = SignalService()
                    except Exception as _mm_sig_err:
                        logging.debug("[R22-P1-NEW] 做市SignalService初始化失败(交易机会可能丢失): %s", _mm_sig_err)
                if sig_svc:
                    direction_upper = 'BUY'
                    signal_price = mid_price if mid_price > 0 else last_price
                    sig_svc.generate_signal(
                        instrument_id=instrument_id, signal_type='OPEN_LONG',
                        price=signal_price, volume=1,
                        reason='hft_transition_capture',
                        priority=9, cooldown_seconds=3,
                    )
                    logging.info("[HFT] transition entry signal: %s dir=BUY reason=%s price=%.4f",
                                 instrument_id, transition_type, signal_price)
            except Exception as e:
                logging.debug("[HFT] _handle_transition_signal error: %s", e)

    def _handle_smart_money_signal(self, smart_money_signal: Dict[str, Any], instrument_id: str) -> None:
        signal = smart_money_signal.get('signal', 'neutral')
        strength = smart_money_signal.get('strength', 0.0)
        if signal == 'neutral' or strength < 0.3:
            return
        direction = 'BUY' if signal == 'buy' else 'SELL'
        logging.info("[HFT] smart money flow: %s dir=%s strength=%.3f",
                     instrument_id, direction, strength)
        try:
            sig_svc = self._state_store.get_ref('_signal_service')
            if sig_svc is None:
                sig_svc = getattr(self, '_signal_service', None)
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal_service import SignalService
                    sig_svc = SignalService()
                except Exception as _res_sig_err:
                    logging.debug("[R22-P1-NEW] 共振SignalService初始化失败(交易机会可能丢失): %s", _res_sig_err)
            if sig_svc and strength > 0.5:
                signal_type = 'OPEN_LONG' if direction == 'BUY' else 'OPEN_SHORT'
                sig_svc.generate_signal(
                    instrument_id=instrument_id, signal_type=signal_type,
                    price=0.0, volume=1,
                    reason='hft_smart_money_flow',
                    priority=8, cooldown_seconds=5,
                )
        except Exception as e:
            logging.debug("[HFT] _handle_smart_money_signal error: %s", e)

    def _handle_filtered_signal(self, filter_result: Dict[str, Any], instrument_id: str, last_price: float) -> None:
        smooth = filter_result.get('smoothed_value', 0.0)
        velocity = filter_result.get('velocity', 0.0)
        logging.debug("[HFT] signal_filter passed: %s smooth=%.3f vel=%.4f",
                      instrument_id, smooth, velocity)

    # ✅ P1-18修复：_ensure_order_service已提取到StrategyCoreService基类，此处删除重复定义


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
                                       bar_time: Optional[float] = None) -> Optional[str]:
    """实盘两阶段硬时间止损检查入口，调用SafetyMetaLayer.check_position_hard_time_stop"""
    safety = getattr(risk_service, '_safety_meta_layer', None)
    if safety is None:
        return None
    try:
        return safety.check_position_hard_time_stop(
            position_id, open_time, max_profit_reached,
            profit_slope, peak_profit_pct, current_profit_pct,
            bar_time=bar_time
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
                pos = PursuitPosition(
                    position_id=f"PURSUIT_{instrument_id}_{int(time.time()*1000)}",
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
    形成金字塔结构——底部仓位大、顶部仓位小。
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
# 识别位置: 函数内 from ali2026v3_trading.shared_utils import ... 重复调用
# TODO: 将函数内延迟import移到模块顶部，仅在解决循环依赖时保留函数内import  # R17-P2-DEP-03: 模块级/函数内import混用标记
