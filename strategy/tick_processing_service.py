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
import os
import threading
import time
import weakref
from typing import Any, Dict, Optional

from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id  # R9-3

from ali2026v3_trading.infra.event_bus import TickEvent, MarketEvent, BarCompletedEvent

# R27-P1修复: 导入容错/浮点工具
from ali2026v3_trading.infra.resilience import (
    TimeoutGuard, Watchdog, HeartbeatMonitor,
)
from ali2026v3_trading.config import config_params
from ali2026v3_trading.strategy.tick_hft import (
    dispatch_hft_tick, execute_pursuit_exit,
    execute_pursuit_entry, execute_pursuit_add,
    handle_arbitrage_signal, handle_transition_signal,
    handle_smart_money_signal, handle_filtered_signal,
)
from ali2026v3_trading.strategy.tick_dispatch import (
    is_option_instrument, check_hard_time_stop_live,
    process_tick_unified_path, process_tick_core,
    tick_dispatch_layer, dispatch_tick_degraded,
    dispatch_tick, output_periodic_summary,
    flush_tick_buffer, save_tick_snapshot, atexit_flush,
    tick_preflight_check,
)

_PRICE_TOLERANCE = 1e-6

__all__ = [
    'MarketEvent', 'TickEvent', 'BarCompletedEvent',
    'TickProcessingService',
    'process_tick_core', 'process_tick_unified_path',
    'is_option_instrument', 'check_hard_time_stop_live',
    'dispatch_hft_tick', 'execute_pursuit_exit',
    'execute_pursuit_entry', 'execute_pursuit_add',
    'handle_arbitrage_signal', 'handle_transition_signal',
    'handle_smart_money_signal', 'handle_filtered_signal',
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
    _TICK_OVERLOAD_THRESHOLD_MS = 500.0
    _TICK_OVERLOAD_DROP_WINDOW_SEC = 2.0
    _UNIFIED_TICK_PATH_ENABLED = True
    # FIX-P0-2: tick处理限流 — 连续过载时延长丢弃窗口，避免classify计数器暴增
    _TICK_OVERLOAD_EXTENDED_DROP_SEC = 10.0
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
        else:
            # FIX-20260701: _callback_group为None时，启动daemon thread定时flush
            # 根因: _callback_group为None导致flush_tick_buffer未注册定时回调，
            #       shard_buffers中的tick在tick推送停止后无法被flush，导致数据流失。
            _flush_interval = float(getattr(self, '_tick_buffer_flush_interval', 1.0))
            def _periodic_flush_fallback():
                while not getattr(self, '_flush_stop_requested', False):
                    try:
                        time.sleep(_flush_interval)
                        if getattr(self, '_flush_stop_requested', False):
                            break
                        if hasattr(self, '_shard_buffers') and self._shard_buffers:
                            flush_tick_buffer(self)
                    except Exception as _flush_err:
                        logging.debug("[FIX-20260701] periodic_flush_fallback error: %s", _flush_err)
            _flush_thread = threading.Thread(target=_periodic_flush_fallback, daemon=True, name='TickBufferFlushFallback')
            _flush_thread.start()
            self._flush_fallback_thread = _flush_thread
            logging.warning("[FIX-20260701] _callback_group为None，启动daemon thread定时flush shard_buffers (interval=%.1fs)", _flush_interval)

        # atexit 弱引用注册
        if not hasattr(TickProcessingService, '_atexit_registered'):
            self._weak_self = weakref.ref(self)
            atexit.register(TickProcessingService._atexit_flush, self._weak_self)
            TickProcessingService._atexit_registered = True

        self._initialized = True

    # ========== Lifecycle ==========

    def on_stop(self):
        self._flush_stop_requested = True
        _fallback_thread = getattr(self, '_flush_fallback_thread', None)
        if _fallback_thread is not None and _fallback_thread.is_alive():
            _fallback_thread.join(timeout=3.0)
            logging.info("[TickProcessingService] on_stop: TickBufferFlushFallback线程已退出")

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

        # [FIX-20260708-TICK-NO-DROP] 安全重置：过载窗口已过但degraded_mode仍为True时重置
        if getattr(self, '_tick_degraded_mode', False) and time.time() > self._tick_overload_until:
            self._tick_degraded_mode = False

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
                # FIX-P1-NEW-01: 启动期豁免 — 首tick JIT惰性初始化(476ms)不应触发背压丢弃
                # 根因: on_start后首批tick触发缓存预热/JIT编译，单tick耗时远超200ms阈值，
                # 但属正常启动行为，不应激活5s丢弃窗口导致排队tick全部丢失
                _first_tick_ts = getattr(self, '_first_tick_ts', None)
                if _first_tick_ts is None:
                    self._first_tick_ts = _now
                    _first_tick_ts = _now
                _startup_grace_sec = 30.0
                # FIX-M8-4: on_start可能在首tick后数分钟才运行(如19:55订阅→20:06 on_start)
                # 30s启动豁免已过期，但on_start触发历史K线加载导致资源竞争tick变慢
                # 修复: 历史K线加载期间(_ext_kline_load_in_progress=True)也不触发背压
                _ext_kline_loading = False
                try:
                    _stor = self._state_store.get_ref('storage') if self._state_store else None
                    if _stor and getattr(_stor, '_ext_kline_load_in_progress', False):
                        _ext_kline_loading = True
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass
                if (_now - _first_tick_ts) < _startup_grace_sec or _ext_kline_loading:
                    if _ext_kline_loading:
                        logging.info("[FIX-M8-4] 历史K线加载期间tick耗时%.1fms(豁免背压): inst=%s",
                                     _tick_elapsed_ms,
                                     self._get_tick_field(tick, 'instrument_id', '?'))
                    else:
                        logging.info("[FIX-P1-NEW-01] 启动期tick耗时%.1fms(豁免%.0fs内不触发背压): inst=%s",
                                     _tick_elapsed_ms, _startup_grace_sec,
                                     self._get_tick_field(tick, 'instrument_id', '?'))
                else:
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
                    "[TickCheck] _is_running not in StateStore (count=%d). "
                    "FIX-P1-14: 保守放行(原行为=丢弃), 避免启动期 tick 全部丢失.",
                    _not_running_log_count,
                )
            # FIX-P1-14: _is_running is None 被等同 False 丢弃启动期 tick
            # 根因: on_start() 与 StateStore 同步存在竞态窗口，启动阶段所有 tick 被丢弃
            # 修复: None 时保守放行(视为 True)，仅记录告警，不丢弃 tick
            is_running = True

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

