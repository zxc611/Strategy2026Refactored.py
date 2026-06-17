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
from ali2026v3_trading.strategy.tick_hft_dispatch import (
    dispatch_hft_tick, execute_pursuit_exit,
    execute_pursuit_entry, execute_pursuit_add,
    handle_arbitrage_signal, handle_transition_signal,
    handle_smart_money_signal, handle_filtered_signal,
)

# R27-P0-FP-01修复: 浮点容差常量
_PRICE_TOLERANCE = 1e-6

# [R28-C010修复] per-instrument分发锁，保障_dispatch_tick时序一致性
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
    不再隐式访问 Mixin 宿主的 self 属性。

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

        # Provider 回调函数（替代 Mixin 中 getattr(self, '_ensure_xxx') 的隐式调用）
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
        必须在构造后显式调用。
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
        实盘和回测路径在事件层统一。

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
        """处理 K 线完成事件（回测模式主路径）

        将 BarCompletedEvent 转换为类 tick 对象后走统一处理流程。
        """
        # 构造一个类 tick 对象，使下游代码无需修改
        class _BarTickProxy:
            pass

        proxy = _BarTickProxy()
        proxy.instrument_id = event.instrument_id
        proxy.exchange = ''
        proxy.last_price = event.close
        proxy.volume = event.volume
        proxy.timestamp = event.timestamp
        proxy.open_interest = 0
        proxy.amount = 0.0
        proxy.bid_price1 = 0.0
        proxy.ask_price1 = 0.0

        self.on_tick(proxy)

    # ========== 向后兼容入口 ==========

    def on_tick(self, tick: Any) -> None:
        """Tick数据回调 - 核心热路径（向后兼容入口）

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
        from ali2026v3_trading.infra.subscription_manager import SubscriptionManager

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
        is_running = _cross_state.get('_is_running', False)
        is_paused = _cross_state.get('_is_paused', False)

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
    return any(p in _upper for p in ['-C', '-P', '_C', '_P', 'CALL', 'PUT'])


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
                if not isinstance(rec, dict):
                    continue
                _open_time = rec.get('open_time') or rec.get('created_at')
                if _open_time is None:
                    continue
                _max_profit = rec.get('max_profit_pct', 0.0)
                _slope = rec.get('profit_slope', 0.0)
                _peak = rec.get('peak_profit_pct', 0.0)
                _current = rec.get('current_profit_pct', 0.0)
                from ali2026v3_trading.strategy.strategy_tick_handler import check_hard_time_stop_for_position
                _result = check_hard_time_stop_for_position(
                    _risk_svc, _pid, _open_time, _max_profit,
                    _slope, _peak, _current,
                    bar_time=svc._state_store.get('_current_bar_time') if svc._state_store else None
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
                _tick_price = float(_norm_tick.get('last_price', 0) or _norm_tick.get('price', 0))
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
        source_timestamp = svc._get_tick_field(tick, 'timestamp', 0.0)
        try:
            _ts_float = float(source_timestamp) if source_timestamp is not None else 0.0
        except (TypeError, ValueError):
            _ts_float = 0.0
        if instrument_id and _ts_float > 0:
            _last_ts = svc._last_tick_timestamp.get(instrument_id, 0.0)
            if _last_ts > 0 and _ts_float < _last_ts:
                svc._tick_timestamp_violation_count += 1
                if svc._state_store is not None:
                    svc._state_store.set('_tick_timestamp_violation_count', svc._tick_timestamp_violation_count)
                if svc._tick_timestamp_violation_count <= 10 or svc._tick_timestamp_violation_count % 1000 == 0:
                    logging.warning(
                        "[R24-P1-IV-09] Tick时间戳非单调: instrument=%s current_ts=%.6f < last_ts=%.6f, 累计%d次, 丢弃该tick",
                        instrument_id, _ts_float, _last_ts, svc._tick_timestamp_violation_count,
                    )
                svc._last_tick_timestamp[instrument_id] = _ts_float
                return
            svc._last_tick_timestamp[instrument_id] = _ts_float

        # R24-P0-IV-07: 价格跳变实时检测
        _prev_price = svc._price_jump_last_price.get(instrument_id)
        if _prev_price is not None and _prev_price > 0:
            _jump_pct = abs(last_price - _prev_price) / _prev_price
            if _jump_pct > svc._price_jump_threshold:
                svc._price_jump_count += 1
                if svc._state_store is not None:
                    svc._state_store.set('_price_jump_count', svc._price_jump_count)
                if svc._price_jump_count <= 10 or svc._price_jump_count % 100 == 0:
                    logging.warning("[R24-P0-IV-07] Price jump detected: %s %.2f->%.2f (%.1f%% change, total_jumps=%d) - tick dropped",
                                   instrument_id, _prev_price, last_price, _jump_pct*100, svc._price_jump_count)
                return
        svc._price_jump_last_price[instrument_id] = last_price

        # R23-FR-04-FIX: K线过期淘汰
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            _cp = get_cached_params()
            _kline_max_age = _cp.get('kline_max_age_sec', 60) if isinstance(_cp, dict) else 60
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            _kline_max_age = 60
        if hasattr(tick, 'timestamp') or hasattr(tick, 'datetime'):
            _tick_ts = getattr(tick, 'timestamp', None) or getattr(tick, 'datetime', None)
            if _tick_ts is not None:
                try:
                    _tick_epoch = float(_tick_ts) if isinstance(_tick_ts, (int, float)) else 0.0
                    if _tick_epoch > 0 and (time.time() - _tick_epoch) > _kline_max_age:
                        if not hasattr(svc, '_kline_age_drop_count'):
                            svc._kline_age_drop_count = 0
                        svc._kline_age_drop_count += 1
                        if svc._kline_age_drop_count <= 10 or svc._kline_age_drop_count % 1000 == 1:
                            logging.warning("[R23-FR-04-FIX] K线过期淘汰: inst=%s age=%.1fs > max_age=%ds, drop_count=%d",
                                           instrument_id, time.time() - _tick_epoch, _kline_max_age, svc._kline_age_drop_count)
                        return
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
                    logging.warning("[FR-P1-03-FIX] 指标数据年龄过期: age=%.1fs > max_age=%ds, inst=%s",
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
                    logging.warning("[FR-P1-04-FIX] bar时间不一致: diff=%.1fs > threshold=%ds, inst=%s",
                                   _bar_tick_diff, _bar_consistency_threshold, instrument_id)

        # 分子统计：平台推送过数据的合约
        if instrument_id:
            _stor = svc._state_store.get_ref('storage') if svc._state_store else None
            if _stor is not None and hasattr(_stor, 'subscription_manager'):
                sm = _stor.subscription_manager
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
        if _stor2 is not None:
            _sub_mgr = getattr(_stor2, 'subscription_manager', None)
        if _sub_mgr:
            try:
                _sub_mgr.on_tick(instrument_id, last_price, volume, is_replay=False)
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('dispatched', instrument_id, last_price)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as exc:
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('error', instrument_id, last_price, str(exc))
                logging.warning("[R16-P1-8.1] SubscriptionManager分发异常: %s", exc)

        if _sub_mgr and not hasattr(svc, '_tick_route_confirmed'):
            svc._tick_route_confirmed = True
            logging.info("[TickRoute] Tick数据成功路由到subscription_manager: %s", instrument_id)

        _historical_load_in_progress = svc._state_store.get('_historical_load_in_progress') if svc._state_store else False
        if bool(_historical_load_in_progress):
            svc._dispatch_tick_degraded(tick, instrument_id, last_price, volume, exchange)
        else:
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
                # 清理最早的一半锁（低频品种）
                keys = list(_dispatch_locks.keys())[:_MAX_DISPATCH_LOCKS // 2]
                for k in keys:
                    del _dispatch_locks[k]
            lock = threading.Lock()
            _dispatch_locks[instrument_id] = lock
    return lock


def tick_dispatch_layer(svc, tick: Any, instrument_id_raw: str) -> None:
    from ali2026v3_trading.infra.subscription_manager import SubscriptionManager

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

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.error("[R21-CC-P1-05] Tick分发层统一异常: instrument=%s error=%s", instrument_id_raw, e, exc_info=True)
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

    _correlation_id = ''
    try:
        from ali2026v3_trading.strategy_judgment.causal_chain_utils import CausalChainTracker
        _causal_tracker = CausalChainTracker.get_instance()
        _correlation_id = _causal_tracker.new_correlation_id()
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] CausalChainTracker failed: %s", _r3_err)
        pass
    tick_data = {
        'timestamp': svc._get_tick_field(tick, 'timestamp', time.time()),
        'instrument_id': instrument_id,
        'exchange': exchange,
        'last_price': last_price,
        'volume': volume,
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

        _storage_ref = svc._state_store.get_ref('storage') if svc._state_store else None
        if _storage_ref is not None and hasattr(_storage_ref, 'subscription_manager'):
            sm = _storage_ref.subscription_manager
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
        failed = []
        for tick_item in ticks:
            try:
                _storage_flush = svc._state_store.get_ref('storage') if svc._state_store else None
                if _storage_flush:
                    _storage_flush.process_tick(tick_item)
                _e2e_ref = svc._state_store.get_ref('_e2e_counters') if svc._state_store else None
                _lock_ref = svc._state_store.get_ref('_lock') if svc._state_store else None
                if _e2e_ref is not None and _lock_ref is not None:
                    with _lock_ref:
                        _e2e_ref['persisted_count'] += 1
                        _e2e_sp = svc._state_store.get_ref('_e2e_shard_persisted') if svc._state_store else None
                        if _e2e_sp is not None:
                            _e2e_sp[shard_idx] = _e2e_sp.get(shard_idx, 0) + 1
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                record_tick_probe('saved', tick_item.get('instrument_id', ''), tick_item.get('last_price', 0.0))
                total_flushed += 1
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                logging.error("[_flush_tick_buffer] shard=%d: %s", shard_idx, e)
                failed.append(tick_item)
        if failed:
            with shard_lock:
                svc._shard_buffers.setdefault(shard_idx, []).extend(failed)
            logging.warning("[_flush_tick_buffer] shard=%d %d/%d 回写", shard_idx, len(failed), len(ticks))
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

    _stor3 = svc._state_store.get_ref('storage') if svc._state_store else None
    if _stor3 is not None:
        try:
            tick_timestamp = svc._get_tick_field(tick, 'timestamp', time.time())
            turnover = svc._get_tick_field(tick, 'amount', 0.0)
            open_interest = svc._get_tick_field(tick, 'open_interest', 0.0)
            tick_data = {
                'timestamp': tick_timestamp,
                'instrument_id': instrument_id,
                'exchange': exchange,
                'last_price': last_price,
                'volume': volume,
                'amount': turnover,
                'open_interest': open_interest,
                'bid_price1': svc._get_tick_field(tick, 'bid_price1'),
                'ask_price1': svc._get_tick_field(tick, 'ask_price1'),
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
                failed_ticks = []
                for tick_item in ticks_to_flush:
                    try:
                        _stor3.process_tick(tick_item)
                        if _lock is not None and _e2e_counters is not None:
                            with _lock:
                                _e2e_counters['persisted_count'] += 1
                        from ali2026v3_trading.infra.health_monitor import record_tick_probe
                        record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                        logging.warning("[_dispatch_tick] shard=%d flush失败: %s", shard_idx, e)
                        failed_ticks.append(tick_item)
                if failed_ticks:
                    with shard_lock:
                        svc._shard_buffers.setdefault(shard_idx, []).extend(failed_ticks)
                    logging.warning("[_dispatch_tick] shard=%d %d/%d 回写buffer", shard_idx, len(failed_ticks), len(ticks_to_flush))
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

    from ali2026v3_trading.strategy.tick_hft_dispatch import dispatch_hft_tick
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
                ts = tick.get('timestamp')
                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                snapshot['ticks'][sym] = {
                    'p': tick.get('price', 0), 't': ts_str, 'v': tick.get('volume', 0),
                    'b': tick.get('bid_price', 0), 'a': tick.get('ask_price', 0),
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
        _dedup_ts = svc._get_tick_field(tick, 'timestamp', '')
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
        _tick_ts = svc._get_tick_field(tick, 'timestamp', None)
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
        _seq_field_names = ['sequence_no', 'SequenceNo', 'seq_no', 'update_sequence', 'UpdateSequence']
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
        if int((_now * 10) % 100) == 0:
            logging.warning(
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
