# MODULE_ID: M1-279
"""
Tick处理服务模块 - 组合替代继承 (G2b Mixin Elimination Step 2)

将 TickHandlerMixin 提取为独立的 TickProcessingService 类，
通过显式依赖注入替代隐式 self 共享。

职责：
- 处理Tick数据回调（on_tick / on_market_event）
- MarketEvent 抽象：统一实盘 Tick 和回测 Bar 事件
- Tick数据统计和汇总
- 周期性状态输出
- Tick数据持久化和K线合成

设计原则：
- 组合优于继承：通过构造函数注入依赖，不依赖 Mixin 宿主的 self
- 显式依赖：state_store、callback_group 通过构造函数传入
- MarketEvent 抽象：on_market_event(event) 统一入口，on_tick(tick) 向后兼容
- 线程安全：保护共享状态
"""

# Merged from tick_aggregator_service.py and tick_dispatch_service.py on 2026-06-12

import atexit
import logging
import math
import os
import re
import threading
import time
import uuid
import weakref
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id  # R9-3

from ali2026v3_trading.infra.event_bus import TickEvent, MarketEvent, BarCompletedEvent

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
from ali2026v3_trading.config import config_params
from ali2026v3_trading.strategy_judgment.causal_chain_utils import (
    CausalChainTracker, ContaminationGuard, CyclicDependencyGuard,
    validate_tick_cascade, CausalEvent,
)
from ali2026v3_trading.strategy.tick_hft import (
    dispatch_hft_tick, execute_pursuit_exit,
    execute_pursuit_entry, execute_pursuit_add,
    handle_arbitrage_signal, handle_transition_signal,
    handle_smart_money_signal, handle_filtered_signal,
)

# R27-P0-FP-01修复: 浮点容差常量
_PRICE_TOLERANCE = 1e-6

# [R28-C010修复] per-instrument分发锁，保障。dispatch_tick时序一致性
_dispatch_locks: Dict[str, threading.Lock] = {}
_dispatch_locks_guard = threading.Lock()
_MAX_DISPATCH_LOCKS = 500  # 防止锁数量无限增长

__all__ = [
    'MarketEvent', 'TickEvent', 'BarCompletedEvent',
    'TickProcessingService',
    # from tick_aggregator_service
    'process_tick_core', 'process_tick_unified_path',
    'is_option_instrument', 'check_hard_time_stop_live',
    'dispatch_hft_tick', 'execute_pursuit_exit',
    'execute_pursuit_entry', 'execute_pursuit_add',
    'handle_arbitrage_signal', 'handle_transition_signal',
    'handle_smart_money_signal', 'handle_filtered_signal',
    # from tick_dispatch_service
    'tick_dispatch_layer', 'dispatch_tick', 'dispatch_tick_degraded',
    'output_periodic_summary', 'flush_tick_buffer', 'atexit_flush',
    'save_tick_snapshot', 'tick_preflight_check',
]

logger = get_logger(__name__)  # R9-5


# ============================================================================
# TickProcessingService
# ============================================================================

class TickProcessingService:
    """Tick处理服务 - 独立组合类

    替代 TickHandlerMixin，通过构造函数注入所有依赖，
    不再隐式访问 Mixin 宿主的 self 属性。'
    使用方式:
        svc = TickProcessingService(
            state_store=store,
            callback_group=cb_group,
            strategy_id='strat_001',
            is_backtest=False,
            ensure_order_service_fn=provider._ensure_order_service,
            ensure_position_service_fn=provider._ensure_position_service,
            ensure_hft_engine_fn=provider._ensure_hft_engine,
        )
        svc.init_tick_handler()
        # 实盘: svc.on_tick(tick)
        # 统一: svc.on_market_event(TickEvent(instrument_id=..., tick_data=tick))
    """

    # R10-P2-03: 背压机制
    MAX_TICK_QUEUE_DEPTH = 1000
    _TICK_OVERLOAD_THRESHOLD_MS = 200.0
    _TICK_OVERLOAD_DROP_WINDOW_SEC = 5.0
    _UNIFIED_TICK_PATH_ENABLED = True
    # FIX-P0-2: tick处理限流 — 连续过载时延长丢弃窗口，避免classify计数器暴增
    _TICK_OVERLOAD_EXTENDED_DROP_SEC = 30.0
    _TICK_OVERLOAD_CONSECUTIVE_THRESHOLD = 3

    _TICK_FIELD_NAMES = {
        'instrument_id': ['instrument_id'],
        'exchange': ['exchange'],
        'last_price': ['last_price'],
        'volume': ['volume'],
        'datetime': ['datetime', 'update_time', 'ts'],
        'open_interest': ['open_interest'],
        'turnover': ['turnover'],
        'bid_price1': ['bid_price1'],
        'ask_price1': ['ask_price1'],
        'bid_price2': ['bid_price2'],
        'ask_price2': ['ask_price2'],
        'bid_price3': ['bid_price3'],
        'ask_price3': ['ask_price3'],
        'bid_price4': ['bid_price4'],
        'ask_price4': ['ask_price4'],
        'bid_price5': ['bid_price5'],
        'ask_price5': ['ask_price5'],
        'bid_volume1': ['bid_volume1'],
        'ask_volume1': ['ask_volume1'],
        'bid_volume2': ['bid_volume2'],
        'ask_volume2': ['ask_volume2'],
        'bid_volume3': ['bid_volume3'],
        'ask_volume3': ['ask_volume3'],
        'bid_volume4': ['bid_volume4'],
        'ask_volume4': ['ask_volume4'],
        'bid_volume5': ['bid_volume5'],
        'ask_volume5': ['ask_volume5'],
    }

    def __init__(self, state_store, callback_group, strategy_id: str = '',
                 is_backtest: bool = False,
                 ensure_order_service_fn=None,
                 ensure_position_service_fn=None,
                 ensure_hft_engine_fn=None):
        """初始化 TickProcessingService

        Args:
            state_store: StateStore 实例，用于访问共享状态
            callback_group: CallbackGroup 实例，用于注册回调
            strategy_id: 策略ID
            is_backtest: 是否回测模式
            ensure_order_service_fn: 确保 OrderService 初始化的回调
            ensure_position_service_fn: 确保 PositionService 初始化的回调
            ensure_hft_engine_fn: 确保 HFT Engine 初始化的回调
        """
        self._state_store = state_store
        self._callback_group = callback_group
        self._strategy_id = strategy_id
        self._is_backtest = is_backtest

        # Provider 回调函数（替代 Mixin 中 getattr(self, '_ensure_xxx') 的隐式调用）'
        self._ensure_order_service_fn = ensure_order_service_fn
        self._ensure_position_service_fn = ensure_position_service_fn
        self._ensure_hft_engine_fn = ensure_hft_engine_fn

        # 以下属性在 init_tick_handler() 中初始化
        self._tick_count: int = 0
        self._initialized: bool = False

    # ========== 初始化 ==========

    def init_tick_handler(self) -> None:
        """初始化Tick处理服务的状态

        替代原 TickHandlerMixin._init_tick_handler_mixin()。
        必须在构造后显式调用。'
        """
        if self._initialized:
            return

        self._tick_count = 0

        # [R23-P2-ID-04-FIX] tick处理去重
        self._tick_dedup_cache: Dict[str, float] = {}
        self._tick_dedup_window_ms: float = 100.0
        # [R23-P2-FR-09-FIX] tick数据年龄监控
        self._tick_last_data_time: Dict[str, float] = {}

        # R21-NET-P1-02修复: Tick序列号校验
        self._tick_last_seq: Dict[str, int] = {}
        self._tick_seq_error_count: int = 0
        self._tick_seq_lock = threading.Lock()

        # R23-SM-09-FIX: HMM状态驻留时间异常检测
        self._hmm_state_entry_time: Dict[str, float] = {}
        self._hmm_state_dwell_max_sec: float = 3600.0
        self._hmm_state_dwell_min_sec: float = 1.0
        self._hmm_state_switch_counts: Dict[str, int] = {}
        self._hmm_state_switch_window_sec: float = 60.0
        self._hmm_state_max_switches_per_window: int = 100
        self._hmm_last_state: Optional[str] = None

        # R27-P1-DR-02修复: tick处理超时守卫
        self._tick_timeout_guard = TimeoutGuard(timeout_sec=5.0)
        # R27-P1-DR-06修复: tick处理看门狗
        self._tick_watchdog = Watchdog(timeout_sec=config_params.WATCHDOG_TIMEOUT_TICK_SEC, name='tick_handler')
        # R27-P1-DR-07修复: tick处理心跳
        self._tick_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=5.0, missed_threshold=3)

        # R10-P2-03: 背压机制
        self._tick_queue_depth = 0
        self._tick_overload_until: float = 0.0

        # Tick日志时间控制
        self._last_tick_log_time = time.time()
        self._tick_log_interval = 180.0

        # Tick诊断时间戳
        self._tick_debug_last_ts = 0.0

        # R24-P0-IV-07: 价格跳变实时检测
        self._price_jump_last_price = {}
        self._price_jump_threshold = 0.05
        self._price_jump_threshold_option = 0.30
        self._price_jump_count = 0

        # R24-P1-IV-09修复: tick时间戳单调性验证
        self._last_tick_timestamp: Dict[str, float] = {}
        self._tick_timestamp_violation_count: int = 0

        # Probe日志集合
        self._probe_logged_instruments = set()
        self._probe_lock = threading.Lock()

        # 分片缓冲
        self._TICK_SHARD_COUNT = 16
        from ali2026v3_trading.infra.shared_utils import get_shard_router
        self._shard_router = get_shard_router(shard_count=self._TICK_SHARD_COUNT)
        self._shard_buffers: Dict[int, list] = {}
        self._shard_locks: Dict[int, threading.Lock] = {}
        self._shard_last_flush_time: Dict[int, float] = {}
        self._shard_buffers_lock = threading.Lock()
        self._tick_buffer_threshold = 50
        self._tick_buffer_flush_interval = float(os.environ.get('TICK_HANDLER_FLUSH_INTERVAL', '1.0'))
        self._last_buffer_flush_time = time.time()
        self._shard_key_mode = 'product_code'
        self._tick_stats_lock = threading.Lock()

        # 覆盖式快照WAL
        self._snapshot_path = None
        self._snapshot_tick_count = 0
        self._snapshot_interval = 100
        self._raw_tick_summary_interval = 60.0
        self._raw_tick_last_output_time = 0.0
        self._raw_tick_accum_count = 0
        self._raw_tick_accum_lock = threading.Lock()

        # 同步状态到 StateStore
        store = self._state_store
        if store is not None:
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

        # 注册回调
        if self._callback_group is not None:
            registry = self._callback_group.get_registry('flush_tick_buffer')
            if registry is not None:
                registry.register(self._flush_tick_buffer)

        # atexit 弱引用注册
        if not hasattr(TickProcessingService, '_atexit_registered'):
            self._weak_self = weakref.ref(self)
            atexit.register(TickProcessingService._atexit_flush, self._weak_self)
            TickProcessingService._atexit_registered = True

        self._initialized = True

    # ========== Properties ==========

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def stats(self) -> Dict[str, Any]:
        """获取统计信息（从 StateStore 读取）"""
        if self._state_store is not None:
            _ss_stats = self._state_store.get_ref('_stats')
            if _ss_stats is not None:
                return _ss_stats
        return {}

    # ========== MarketEvent 统一入口 ==========

    def on_market_event(self, event: MarketEvent) -> None:
        """统一市场事件入口

        替代 on_tick(tick)，支持 TickEvent 和 BarCompletedEvent。
        实盘和回测路径在事件层统一。'
        Args:
            event: MarketEvent 子类实例（TickEvent 或 BarCompletedEvent）
        """
        if isinstance(event, TickEvent):
            # P1-31修复: TickEvent 统一为 infra 版，字段名 tick_data 替代原 tick
            tick_obj = getattr(event, 'tick_data', None) or getattr(event, 'tick', None)
            if tick_obj is not None:
                self.on_tick(tick_obj)
        elif isinstance(event, BarCompletedEvent):
            self._on_bar_completed(event)
        else:
            logging.warning("[TickProcessingService] Unknown MarketEvent type: %s", type(event).__name__)

    def _on_bar_completed(self, event: BarCompletedEvent) -> None:
        """处理 K 线完成事件（回测模式主路径）'
        将 BarCompletedEvent 转换为类 tick 对象后走统一处理流程。'
        """
        # 构造一个类 tick 对象，使下游代码无需修改
        class _BarTickProxy:
            pass

        proxy = _BarTickProxy()
        proxy.instrument_id = event.instrument_id
        proxy.exchange = ''
        proxy.last_price = event.close
        proxy.volume = event.volume
        proxy.datetime = event.timestamp
        proxy.open_interest = 0
        proxy.turnover = 0.0
        proxy.bid_price1 = 0.0
        proxy.ask_price1 = 0.0

        self.on_tick(proxy)

    # ========== 向后兼容入口 ==========

    def on_tick(self, tick: Any) -> None:
        """Tick数据回调 - 核心热路径（向后兼容入口）'
        拆分为探针层→检查层→处理层→统计层
        """
        _tick_arrival_ts = time.perf_counter()
        if hasattr(tick, '__dict__'):
            tick._arrival_ts = _tick_arrival_ts

        if not tick_preflight_check(self, tick):
            return

        # R15-P0-PERF-04修复: tick处理耗时计时和超限告警
        _tick_start_ns = time.perf_counter_ns()
        _now = time.time()
        try:
            instrument_id_raw = self._tick_probe_layer(tick)
            if not self._tick_check_layer(instrument_id_raw):
                return
            self._tick_dispatch_layer(tick, instrument_id_raw)
        finally:
            _tick_elapsed_ms = (time.perf_counter_ns() - _tick_start_ns) / 1_000_000.0
            if _tick_elapsed_ms > self._TICK_OVERLOAD_THRESHOLD_MS:
                # FIX-P0-2: 连续过载时延长丢弃窗口(5s→30s)，减少classify计数器暴增
                _consecutive_overload = getattr(self, '_consecutive_overload_count', 0) + 1
                self._consecutive_overload_count = _consecutive_overload
                if _consecutive_overload >= self._TICK_OVERLOAD_CONSECUTIVE_THRESHOLD:
                    _drop_window = self._TICK_OVERLOAD_EXTENDED_DROP_SEC
                    logging.warning("[FIX-P0-2] 连续%d次tick过载，延长丢弃窗口至%.0fs",
                                    _consecutive_overload, _drop_window)
                else:
                    _drop_window = self._TICK_OVERLOAD_DROP_WINDOW_SEC
                self._tick_overload_until = _now + _drop_window
                _overload_inst = self._get_tick_field(tick, 'instrument_id', '?')
                _overload_log_key = f'_tick_overload_logged_{_overload_inst}'
                _overload_last_ts = getattr(self, _overload_log_key, 0)
                if _tick_elapsed_ms > 500.0:
                    if (time.time() - _overload_last_ts) > 300:
                        logging.error("[R15-P0-PERF-04] tick处理严重超时: %.1fms instrument=%s",
                                      _tick_elapsed_ms, _overload_inst)
                        setattr(self, _overload_log_key, time.time())
                else:
                    if (time.time() - _overload_last_ts) > 300:
                        logging.warning(
                            "[R10-P2-03] tick处理超限%.1fms>阈值%.1fms，进入%.0fs过载丢弃窗口",
                            _tick_elapsed_ms, self._TICK_OVERLOAD_THRESHOLD_MS,
                            _drop_window,
                        )
                        setattr(self, _overload_log_key, time.time())
            else:
                if hasattr(self, '_consecutive_overload_count'):
                    self._consecutive_overload_count = 0
                if _tick_elapsed_ms > 500.0:
                    _overload_inst2 = self._get_tick_field(tick, 'instrument_id', '?')
                    _overload_log_key2 = f'_tick_overload_logged_{_overload_inst2}'
                    _overload_last_ts2 = getattr(self, _overload_log_key2, 0)
                    if (time.time() - _overload_last_ts2) > 300:
                        logging.error("[R15-P0-PERF-04] tick处理严重超时: %.1fms instrument=%s", _tick_elapsed_ms, _overload_inst2)
                        setattr(self, _overload_log_key2, time.time())
                elif _tick_elapsed_ms > 100.0:
                    _overload_inst3 = self._get_tick_field(tick, 'instrument_id', '?')
                    _overload_log_key3 = f'_tick_overload_logged_{_overload_inst3}'
                    _overload_last_ts3 = getattr(self, _overload_log_key3, 0)
                    if (time.time() - _overload_last_ts3) > 300:
                        logging.warning("[R15-P0-PERF-04] tick处理超限: %.1fms instrument=%s", _tick_elapsed_ms, _overload_inst3)
                        setattr(self, _overload_log_key3, time.time())

    # ========== 分片路由辅助方法 ==========

    def _route_shard_index(self, instrument_id: str) -> int:
        _SHARD_MASK = self._TICK_SHARD_COUNT - 1
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            ps = get_params_service()
            info = ps.get_instrument_meta_by_id(instrument_id)
            if info:
                shard_key = info.get('shard_key', -1)
                if isinstance(shard_key, int) and shard_key >= 0:
                    return shard_key & _SHARD_MASK
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _route_err:
            logging.debug("[_route_shard_index] Fallback route exception type=%s: %s", type(_route_err).__name__, _route_err, exc_info=True)
        from ali2026v3_trading.infra.shared_utils import ShardRouter, extract_product_code
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

    # ========== Tick字段提取 ==========

    def _get_tick_field(self, tick: Any, field_name: str, default: Any = None) -> Any:
        """P1-55修复: 委托到shared_utils.get_tick_field（全项目唯一权威实现）"""
        from ali2026v3_trading.infra.shared_utils import get_tick_field as _get_tick_field_impl
        return _get_tick_field_impl(tick, field_name, default)

    def _normalize_tick(self, tick: Any) -> Dict[str, Any]:
        """P1-55修复: 委托到shared_utils.normalize_tick（全项目唯一权威实现）"""
        from ali2026v3_trading.infra.shared_utils import normalize_tick as _normalize_tick_impl
        return _normalize_tick_impl(tick)

    def _check_hft_open_risk(self, instrument_id: str, direction: str, price: float, volume: int,
                             pursuit_signal: Dict[str, Any]) -> bool:
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service

            signal = {
                'symbol': instrument_id,
                'direction': direction,
                'price': price,
                'volume': volume,
                'amount': max(float(price), 0.0) * max(int(volume), 0),
                'is_valid': True,
                'action': 'OPEN',
                'account_id': pursuit_signal.get('account_id', 'default'),
                'signal_id': pursuit_signal.get('signal_id', '') or generate_prefixed_id("HFT", 12),  # R9-3
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.error("[HFT] pursuit open risk check failed, fail-safe block: %s", e, exc_info=True)
            return False

    # ========== Tick入口处理层 ==========

    def _tick_probe_layer(self, tick: Any) -> str:
        """探针层：记录原始合约格式和诊断信息"""
        from ali2026v3_trading.infra.subscription_service import SubscriptionManager

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
            from ali2026v3_trading.infra.health_monitor import is_monitored_contract
            diag_on = is_monitored_contract(instrument_id_raw)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[on_tick] Failed to check monitored contract: %s", e)

        if diag_on:
            now = time.time()
            with self._raw_tick_accum_lock:
                self._raw_tick_accum_count += 1
                if now - self._raw_tick_last_output_time >= self._raw_tick_summary_interval:
                    logging.info("[RAW_TICK_ALL_SUMMARY] tick_id=%s ticks=%d sample=(%s P=%s V=%s)",
                                getattr(tick, 'tick_id', 'N/A'),
                                self._raw_tick_accum_count,
                                instrument_id_raw,
                                self._get_tick_field(tick, 'last_price', 0.0),
                                self._get_tick_field(tick, 'volume', 0))
                    self._raw_tick_accum_count = 0
                    self._raw_tick_last_output_time = now

        from ali2026v3_trading.infra.health_monitor import record_tick_probe, DiagnosisProbeManager
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
        _cross_state = self._state_store.snapshot(['_is_running', '_is_paused']) if self._state_store else {}
        is_running = _cross_state.get('_is_running', None)
        is_paused = _cross_state.get('_is_paused', False)

        if is_running is None:
            _not_running_log_count = getattr(self, '_not_running_log_count', 0) + 1
            self._not_running_log_count = _not_running_log_count
            if _not_running_log_count <= 3 or _not_running_log_count % 1000 == 0:
                logging.warning(
                    "[TickCheck] _is_running not in StateStore (count=%d), tick will be dropped. "
                    "This means on_start() has not synced _is_running to StateStore yet.",
                    _not_running_log_count,
                )
            is_running = False

        if not is_running:
            _e2e = self._state_store.get_ref('_e2e_counters') if self._state_store else None
            if _e2e is not None:
                _e2e['dropped_not_running'] = _e2e.get('dropped_not_running', 0) + 1
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus
                get_global_event_bus().publish('tick_dropped', {'instrument_id': instrument_id_raw, 'reason': 'not_running', 'timestamp': time.time()})
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass
            return False

        if is_paused:
            _e2e = self._state_store.get_ref('_e2e_counters') if self._state_store else None
            if _e2e is not None:
                _e2e['dropped_paused'] = _e2e.get('dropped_paused', 0) + 1
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus
                get_global_event_bus().publish('tick_dropped', {'instrument_id': instrument_id_raw, 'reason': 'paused', 'timestamp': time.time()})
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass
            return False

        return True

    # R27-P0-FC-01修复: 实盘每tick硬时间止损检查
    def _check_hard_time_stop_live(self, instrument_id: str) -> None:
        """每tick检查持仓的两阶段硬时间止损(实盘主链路入口)"""
        check_hard_time_stop_live(self, instrument_id)

    def _tick_dispatch_layer(self, tick: Any, instrument_id_raw: str) -> None:
        tick_dispatch_layer(self, tick, instrument_id_raw)

    def _process_tick_unified_path(self, tick: Any) -> None:
        """统一实盘/回测Tick处理主路径"""
        process_tick_unified_path(self, tick)

    def _is_option_instrument(self, instrument_id: str) -> bool:
        return is_option_instrument(instrument_id)

    # ========== Tick数据处理核心 ==========

    def _process_tick(self, tick: Any) -> None:
        """处理Tick数据（核心方法）- 委托到tick_aggregator_service"""
        process_tick_core(self, tick)

    def _dispatch_tick_degraded(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        dispatch_tick_degraded(self, tick, instrument_id, last_price, volume, exchange)

    # ========== 周期性汇总 ==========

    def _output_periodic_summary(self) -> None:
        output_periodic_summary(self)

    def _flush_tick_buffer(self) -> None:
        flush_tick_buffer(self)

    def _save_tick_snapshot(self):
        save_tick_snapshot(self)

    @staticmethod
    def _atexit_flush(weak_self):
        atexit_flush(weak_self)

    # ========== Tick分发入口 ==========

    def _dispatch_tick(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        dispatch_tick(self, tick, instrument_id, last_price, volume, exchange)

    def _dispatch_hft_tick(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        dispatch_hft_tick(self, tick, instrument_id, last_price, volume, exchange)

    def _execute_pursuit_exit(self, hft: Any, exit_signal: Dict[str, Any], instrument_id: str) -> None:
        execute_pursuit_exit(self, hft, exit_signal, instrument_id)

    def _execute_pursuit_entry(self, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                                instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        execute_pursuit_entry(self, hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)

    def _execute_pursuit_add(self, hft: Any, pursuit_signal: Dict[str, Any], tick: Any,
                              instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        execute_pursuit_add(self, hft, pursuit_signal, tick, instrument_id, last_price, volume, exchange)

    def _handle_arbitrage_signal(self, arbitrage_signal: Dict[str, Any], instrument_id: str) -> None:
        handle_arbitrage_signal(self, arbitrage_signal, instrument_id)

    def _handle_transition_signal(self, transition_signal: Dict[str, Any],
                                   instrument_id: str, last_price: float,
                                   mid_price: float = 0.0) -> None:
        handle_transition_signal(self, transition_signal, instrument_id, last_price, mid_price=mid_price)

    def _handle_smart_money_signal(self, smart_money_signal: Dict[str, Any], instrument_id: str) -> None:
        handle_smart_money_signal(self, smart_money_signal, instrument_id)

    def _handle_filtered_signal(self, filter_result: Dict[str, Any], instrument_id: str, last_price: float) -> None:
        handle_filtered_signal(filter_result, instrument_id, last_price)


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
        if _risk_svc is None:
            return
        _pos_svc = svc._state_store.get_ref('_position_service') if svc._state_store else None
        if _pos_svc is None:
            if svc._ensure_position_service_fn is not None:
                try:
                    svc._ensure_position_service_fn()
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
                _pos_svc = svc._state_store.get_ref('_position_service') if svc._state_store else None
        if _pos_svc is None:
            return
        positions = getattr(_pos_svc, 'positions', {})
        for _inst_id, pos_dict in positions.items():
            for _pid, rec in pos_dict.items():
                if isinstance(rec, dict):
                    _open_time = rec.get('open_time') or rec.get('created_at')
                    if _open_time is None:
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
                    if isinstance(_open_time, datetime):
                        _open_time = _open_time.timestamp()
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
                from ali2026v3_trading.strategy.strategy_tick_handler import check_hard_time_stop_for_position
                _result = check_hard_time_stop_for_position(
                    _risk_svc, _pid, _open_time, _max_profit,
                    _slope, _peak, _current,
                    bar_time=svc._state_store.get('_current_bar_time') if svc._state_store else None,
                    strategy_group=_sg
                )
                if _result is not None:
                    logging.warning("[R27-P0-FC-01] 硬时间止损触发: position=%s reason=%s", _pid, _result)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _hts_err:
        logging.error("[R22-P0-NEW] 硬时间止损检查整体异常(超时持仓可能无法止损!): %s", _hts_err, exc_info=True)


def process_tick_unified_path(svc, tick: Any) -> None:
    """统一实盘/回测Tick处理主路径"""
    process_tick_core(svc, tick)
    try:
        _norm_tick = svc._normalize_tick(tick)
        _inst_id = _norm_tick.get('instrument_id', '')
        if _inst_id and is_option_instrument(_inst_id):
            from ali2026v3_trading.risk.risk_service import get_risk_service
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
        _tick_valid, _safe_price, _safe_vol = validate_tick_cascade(last_price, volume, instrument_id)
        if not _tick_valid:
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
                logging.warning("[R24-P0-IV-01/CC-05] Tick dropped by validate_tick_cascade: invalid last_price=%s volume=%s instrument=%s (total_dropped=%d)",
                               last_price, volume, instrument_id, svc._iv01_drop_count)
            return
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

        # R24-P0-IV-07: 价格跳变实时检测
        _prev_price = svc._price_jump_last_price.get(instrument_id)
        if _prev_price is not None and _prev_price > 0:
            _jump_pct = abs(last_price - _prev_price) / _prev_price
            _is_option = len(instrument_id) > 6 and any(c.isdigit() for c in instrument_id[-4:])
            _threshold = svc._price_jump_threshold_option if _is_option else svc._price_jump_threshold
            if _jump_pct > _threshold:
                svc._price_jump_count += 1
                if svc._state_store is not None:
                    svc._state_store.set('_price_jump_count', svc._price_jump_count)
                if svc._price_jump_count <= 3 or svc._price_jump_count % 10000 == 0:
                    logging.warning("[R24-P0-IV-07] Price jump detected: %s %.2f->%.2f (%.1f%% change, total_jumps=%d) - tick dropped",
                                   instrument_id, _prev_price, last_price, _jump_pct*100, svc._price_jump_count)
                return
        svc._price_jump_last_price[instrument_id] = last_price

        # R23-FR-04-FIX: K线过期淘汰
        # FIX-20260627: 模拟交易环境tick时间戳是历史交易时间，time.time()-tick_epoch远超阈值
        # 导致所有tick被误判为过期丢弃。改为：仅淘汰"比同合约上一个tick更旧"的tick（真正过期），
        # 而非与系统时间比较。同时保留未来时间戳检查（明显异常）。
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
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
                        _should_drop = False
                        if _tick_epoch > time.time() + 300:
                            _should_drop = True
                            _drop_reason = "future_tick"
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
                                            "[R23-FR-04-FIX] 回放/模拟时钟回退，跳过K线过期淘汰: inst=%s current=%.1f prev=%.1f count=%d",
                                            instrument_id, _tick_epoch, _prev_tick_epoch, svc._kline_replay_regression_count,
                                        )
                                else:
                                    _should_drop = True
                                    _drop_reason = "stale_vs_prev"
                        if _should_drop:
                            if not hasattr(svc, '_kline_age_drop_count'):
                                svc._kline_age_drop_count = 0
                            svc._kline_age_drop_count += 1
                            if svc._kline_age_drop_count <= 10 or svc._kline_age_drop_count % 1000 == 1:
                                logging.debug("[R23-FR-04-FIX] K线过期淘汰: inst=%s reason=%s tick_epoch=%.1f drop_count=%d",
                                              instrument_id, _drop_reason, _tick_epoch, svc._kline_age_drop_count)
                            return
                        if not hasattr(svc, '_kline_age_last_tick_epoch'):
                            svc._kline_age_last_tick_epoch = {}
                        svc._kline_age_last_tick_epoch[instrument_id] = _tick_epoch
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _kage_err:
                    logging.debug("[R22-P1-NEW] K线年龄淘汰异常(陈旧数据可能影响决策): %s", _kage_err)

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
                    from ali2026v3_trading.data.data_service import get_data_service
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
                from ali2026v3_trading.order.order_flow_bridge import get_order_flow_bridge
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
                from ali2026v3_trading.infra.health_monitor import is_monitored_contract
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
                from ali2026v3_trading.data.data_service import get_data_service
                _stor2 = get_data_service()
            except Exception:
                pass
        if _stor2 is not None:
            _sub_mgr = getattr(_stor2, 'subscription_manager', None)
        if _sub_mgr:
            try:
                _sub_mgr.on_tick(instrument_id, last_price, volume, is_replay=False)
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('dispatched', instrument_id, last_price)
                if not hasattr(svc, '_tick_route_confirmed'):
                    svc._tick_route_confirmed = True
                    logging.info("[TickRoute] Tick数据成功路由到SubscriptionManager: %s", instrument_id)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as exc:
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('error', instrument_id, last_price, str(exc))
                logging.warning("[R16-P1-8.1] SubscriptionManager分发异常: %s", exc)
        else:
            if not hasattr(svc, '_sub_mgr_none_logged'):
                svc._sub_mgr_none_logged = True
                _stor2_type = type(_stor2).__name__ if _stor2 is not None else 'None'
                _stor2_has_sm = hasattr(_stor2, 'subscription_manager') if _stor2 is not None else False
                logging.warning("[TickRoute-DIAG] _sub_mgr is None: stor2=%s stor2_has_submgr=%s", _stor2_type, _stor2_has_sm)

            # FIX: _sub_mgr为None时（storage未注入state_store），直接调用TTypeService
            # 确保5种状态分类能收到tick数据，否则所有期权恒为'other'
            # FIX2: 必须剥离交易所前缀（如CFFEX.IH2609->IH2609），否则get_instrument_meta_by_id返回None
            try:
                from ali2026v3_trading.data.t_type_service import get_t_type_service
                from ali2026v3_trading.config.params_service import get_params_service
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
                        elif _inst_type == 'future':
                            _fid = _meta.get('internal_id')
                            if _fid is not None:
                                _tts.on_future_instrument_tick(int(_fid), float(last_price))
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

        _historical_load_in_progress = svc._state_store.get('_historical_load_in_progress') if svc._state_store else False
        if bool(_historical_load_in_progress):
            try:
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('dispatched', instrument_id, last_price)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError):
                pass
            svc._dispatch_tick_degraded(tick, instrument_id, last_price, volume, exchange)
        else:
            try:
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('dispatched', instrument_id, last_price)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError):
                pass
            svc._dispatch_tick(tick, instrument_id, last_price, volume, exchange)

        # R23-SM-09-FIX: 每个tick处理时检测HMM状态驻留异常
        _hmm_state = svc._state_store.get('_hmm_current_state') if svc._state_store else None
        if _hmm_state is not None:
            from ali2026v3_trading.strategy.strategy_tick_handler import check_hmm_dwell_anomaly
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
    from ali2026v3_trading.infra.subscription_service import SubscriptionManager

    try:
        normalized_tick = svc._normalize_tick(tick)
        exchange = normalized_tick.get('exchange', '')
        instrument_id = normalized_tick.get('instrument_id', '')

        process_tick_unified_path(svc, tick)

        _ss_stats = svc._state_store.get_ref('_stats') if svc._state_store else None
        if _ss_stats is not None:
            _ss_stats['last_tick_path'] = 'unified'

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
        from ali2026v3_trading.data.t_type_service import get_t_type_service
        from ali2026v3_trading.config.params_service import get_params_service
        import re as _re_r21b
        _tts_r21b = get_t_type_service()
        _ps_r21b = get_params_service()
        # FIX-R21B-C: 用 `is not None` 替代 `bool()`，因为 ParamsService.__len__ 在 _params 为空时返回0导致 bool()=False
        if _tts_r21b is not None and _ps_r21b is not None and instrument_id and last_price > 0:
            _norm_match_r21b = _re_r21b.search(r'([A-Za-z]+\d+.*?)$', str(instrument_id).strip())
            _norm_id_r21b = _norm_match_r21b.group(1) if _norm_match_r21b else instrument_id
            _meta_r21b = _ps_r21b.get_instrument_meta_by_id(_norm_id_r21b)
            if not _meta_r21b and _norm_id_r21b != instrument_id:
                _meta_r21b = _ps_r21b.get_instrument_meta_by_id(instrument_id)
            if _meta_r21b:
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
        from ali2026v3_trading.strategy_judgment.causal_chain_utils import CausalChainTracker
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
    with shard_lock:
        svc._shard_buffers.setdefault(shard_idx, []).append(tick_data)

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
        uptime = datetime.now(_CHINA_TZ) - _ss_stats.get('start_time') if _ss_stats.get('start_time') else timedelta(0)
        uptime_str = str(uptime).split('.')[0]
        elapsed_seconds = uptime.total_seconds() if uptime else 1.0
        tick_rate = _ss_stats.get('total_ticks', 0) / max(elapsed_seconds, 1)

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
{''.join([f'  - {exchange}: {count:,} 个\n' for exchange, count in sorted_exchanges])}

【合约活跃度 Top 10】
{''.join([f'  - {inst}: {count:,} 个\n' for inst, count in sorted_instruments])}
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
                from ali2026v3_trading.data.data_service import get_data_service
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
            _storage_flush = svc._state_store.get_ref('storage') if svc._state_store else None
            if _storage_flush:
                inserted = _storage_flush.batch_insert_ticks(ticks, use_arrow=True)
                total_flushed += max(inserted, 0)
            _e2e_ref = svc._state_store.get_ref('_e2e_counters') if svc._state_store else None
            _lock_ref = svc._state_store.get_ref('_lock') if svc._state_store else None
            if _e2e_ref is not None and _lock_ref is not None:
                with _lock_ref:
                    _e2e_ref['persisted_count'] += len(ticks)
                    _e2e_sp = svc._state_store.get_ref('_e2e_shard_persisted') if svc._state_store else None
                    if _e2e_sp is not None:
                        _e2e_sp[shard_idx] = _e2e_sp.get(shard_idx, 0) + len(ticks)
            from ali2026v3_trading.infra.health_monitor import record_tick_probe
            for tick_item in ticks:
                record_tick_probe('saved', tick_item.get('instrument_id', ''), tick_item.get('last_price', 0.0))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logging.error("[_flush_tick_buffer] shard=%d batch flush失败: %s", shard_idx, e)
            with shard_lock:
                svc._shard_buffers.setdefault(shard_idx, []).extend(ticks)
            logging.warning("[_flush_tick_buffer] shard=%d %d/%d 回写", shard_idx, len(ticks), len(ticks))
    if total_flushed:
        logging.info("[_flush_tick_buffer] Total flushed: %d", total_flushed)


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
        from ali2026v3_trading.data.t_type_service import get_t_type_service
        from ali2026v3_trading.config.params_service import get_params_service
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
        # FIX-R21-C: 用 `is not None` 替代 `bool()`，因为 ParamsService.__len__ 在 _params 为空时返回0导致 bool()=False
        if _tts_r21 is not None and _ps_r21 is not None and instrument_id and last_price > 0:
            _norm_match_r21 = _re_r21.search(r'([A-Za-z]+\d+.*?)$', str(instrument_id).strip())
            _norm_id_r21 = _norm_match_r21.group(1) if _norm_match_r21 else instrument_id
            _meta_r21 = _ps_r21.get_instrument_meta_by_id(_norm_id_r21)
            if not _meta_r21 and _norm_id_r21 != instrument_id:
                _meta_r21 = _ps_r21.get_instrument_meta_by_id(instrument_id)
            if svc._r21_diag_count <= 5:
                logging.debug("[R21-DIAG] meta: inst=%r norm=%r meta=%s type=%s",
                              instrument_id, _norm_id_r21, bool(_meta_r21),
                              _meta_r21.get('type', '') if _meta_r21 else 'N/A')
            if _meta_r21:
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
            from ali2026v3_trading.data.data_service import get_data_service
            _stor3 = get_data_service()
            if _stor3 is not None and svc._state_store is not None:
                svc._state_store.set_ref('storage', _stor3)
                if not hasattr(svc, '_stor3_fallback_logged'):
                    svc._stor3_fallback_logged = True
                    logging.warning("[_dispatch_tick] storage引用为None，已fallback注入DataService: %s", type(_stor3).__name__)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _stor3_fallback_err:
            if not hasattr(svc, '_stor3_fallback_error_logged'):
                svc._stor3_fallback_error_logged = True
                logging.error("[_dispatch_tick] storage引用为None且DataService fallback失败: %s", _stor3_fallback_err)
        if _stor3 is None and not hasattr(svc, '_stor3_none_warned'):
            svc._stor3_none_warned = True
            logging.error("[_dispatch_tick] storage引用为None! state_store=%s, tick数据将无法写入DB", type(svc._state_store).__name__ if svc._state_store else 'None')
    if _stor3 is not None:
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

            with shard_lock:
                svc._shard_buffers.setdefault(shard_idx, []).append(tick_data)
                svc._shard_last_flush_time.setdefault(shard_idx, time.time())

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
                try:
                    inserted = _stor3.batch_insert_ticks(ticks_to_flush, use_arrow=True)
                    if _lock is not None and _e2e_counters is not None:
                        with _lock:
                            _e2e_counters['persisted_count'] += max(inserted, 0)
                    from ali2026v3_trading.infra.health_monitor import record_tick_probe
                    for tick_item in ticks_to_flush:
                        record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                    logging.warning("[_dispatch_tick] shard=%d batch flush失败: %s", shard_idx, e)
                    with shard_lock:
                        svc._shard_buffers.setdefault(shard_idx, []).extend(ticks_to_flush)
                    logging.warning("[_dispatch_tick] shard=%d %d/%d 回写buffer", shard_idx, len(ticks_to_flush), len(ticks_to_flush))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            from ali2026v3_trading.infra.health_monitor import record_tick_probe
            record_tick_probe('error', instrument_id, last_price, f"Storage: {e}")
            logging.exception("[_dispatch_tick] Stack trace for instrument: %s", instrument_id)

    # P2-24防护: 此处是tick→position的唯一分发路径，禁止在EventBus上再订阅tick调用pos_svc.on_tick
    try:
        from ali2026v3_trading.position.position_service import get_position_service
        pos_svc = get_position_service()
        pos_svc.on_tick(tick)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as pos_e:
        logging.warning("[_dispatch_tick] PositionService.on_tick failed: %s", pos_e)

    from ali2026v3_trading.strategy.tick_hft import dispatch_hft_tick
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
        from ali2026v3_trading.infra.serialization_utils import json_dumps as _json_dumps
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
                            if _seq_int <= _last_seq:
                                svc._tick_seq_error_count += 1
                                if svc._tick_seq_error_count <= 10 or svc._tick_seq_error_count % 1000 == 0:
                                    logging.warning(
                                        "[R21-NET-P1-02修复] Tick序列号异常: instrument=%s seq=%d last_seq=%d (乱序/重复), 累计%d次",
                                        _inst_id, _seq_int, _last_seq, svc._tick_seq_error_count,
                                    )
                                svc._tick_last_seq[_inst_id] = _seq_int
                                try:
                                    from ali2026v3_trading.infra.event_bus import get_global_event_bus
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
        # FIX-R37-TICK-NOISE: 降低过载丢弃窗口内的重复警告频率，每10秒最多记录一次
        if not hasattr(svc, '_tick_overload_last_log_ts'):
            svc._tick_overload_last_log_ts = 0.0
        if _now - svc._tick_overload_last_log_ts > 10.0:
            svc._tick_overload_last_log_ts = _now
            logging.debug(
                "[R10-P2-03] tick过载丢弃窗口中，剩余%.1fs",
                svc._tick_overload_until - _now,
            )
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _eb = get_global_event_bus()
            _dropped_inst = svc._get_tick_field(tick, 'instrument_id', '')
            _eb.publish('tick_dropped', {'instrument_id': _dropped_inst, 'reason': 'backpressure', 'timestamp': _now})
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] event_bus publish tick_dropped backpressure failed: %s", _r3_err)
            pass
        return False

    return True
