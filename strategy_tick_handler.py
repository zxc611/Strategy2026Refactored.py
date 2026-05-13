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
import time
import threading
import weakref
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

__all__ = ['TickHandlerMixin', 'DynamicPursuitEngine', 'PursuitPosition']


class TickHandlerMixin:
    """Tick数据处理Mixin"""
    
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
    }
    
    def _init_tick_handler_mixin(self) -> None:
        """初始化Tick处理Mixin的状态
        
        注意：此方法应在StrategyCoreService.__init__中调用
        """
        # Tick计数器
        self._tick_count = 0
        
        # Tick日志时间控制（30秒汇总输出）
        self._last_tick_log_time = time.time()
        self._tick_log_interval = 180.0
        
        # Tick诊断时间戳
        self._tick_debug_last_ts = 0.0
        
        # Probe日志集合（记录前100个不同合约）
        self._probe_logged_instruments = set()
        self._probe_lock = threading.Lock()  # ✅ P1#26修复：保护_probe_logged_instruments的并发修改
        
        # 第二轮优化：分片缓冲（按品种独立锁，消除全局竞争）
        self._TICK_SHARD_COUNT = 16
        from ali2026v3_trading.shared_utils import get_shard_router
        self._shard_router = get_shard_router(shard_count=self._TICK_SHARD_COUNT)
        self._shard_buffers: Dict[int, list] = {}
        self._shard_locks: Dict[int, threading.Lock] = {}
        self._shard_buffers_lock = threading.Lock()
        self._tick_buffer_threshold = 50
        self._tick_buffer_flush_interval = 5.0
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
        # ✅ P1#25修复：使用weakref弱引用注册atexit，避免实例方法引用阻止GC回收
        # ✅ M20 Bug #3修复：防止重复注册atexit回调
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
            logging.debug(f"[_route_shard_index] Fallback route: {_route_err}")
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
    
    # ✅ 方法唯一：on_tick(tick)为平台回调入口，position_service同签名；data_service/subscription_manager为内部层，签名不同但分层合理
    def on_tick(self, tick: Any) -> None:
        """Tick数据回调 - 核心热路径（拆分为探针层→检查层→处理层→统计层）"""
        instrument_id_raw = self._tick_probe_layer(tick)
        if not self._tick_check_layer(instrument_id_raw):
            return
        self._tick_dispatch_layer(tick, instrument_id_raw)

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
                    logging.info(f"[PROBE_RAW_INSTRUMENT_ID] raw format: '{instrument_id_raw}' (type={type(instrument_id_raw).__name__})")

        diag_on = False
        try:
            from ali2026v3_trading.diagnosis_service import is_monitored_contract
            diag_on = is_monitored_contract(instrument_id_raw)
        except Exception as e:
            logging.debug(f"[on_tick] Failed to check monitored contract: {e}")

        if diag_on:
            now = time.time()
            with self._raw_tick_accum_lock:
                self._raw_tick_accum_count += 1
                if now - self._raw_tick_last_output_time >= self._raw_tick_summary_interval:
                    logging.info(f"[RAW_TICK_ALL_SUMMARY] ticks={self._raw_tick_accum_count} "
                                f"sample=({instrument_id_raw} P={self._get_tick_field(tick, 'last_price', 0.0)} "
                                f"V={self._get_tick_field(tick, 'volume', 0)})")
                    self._raw_tick_accum_count = 0
                    self._raw_tick_last_output_time = now

        from ali2026v3_trading.diagnosis_service import record_tick_probe, DiagnosisProbeManager
        record_tick_probe('entry', instrument_id_raw, 0.0)

        last_price = self._get_tick_field(tick, 'last_price', 0.0)
        volume = self._get_tick_field(tick, 'volume', 0)
        open_interest = self._get_tick_field(tick, 'open_interest', 0)
        contract_type = 'option' if SubscriptionManager.is_option(instrument_id_raw) else 'future'
        DiagnosisProbeManager.on_tick_entry(instrument_id_raw, last_price, volume, open_interest, contract_type)

        return instrument_id_raw

    def _tick_check_layer(self, instrument_id_raw: str) -> bool:
        """检查层：运行状态检查+历史K线触发"""
        with self._lock:
            is_running = self._is_running
            is_paused = self._is_paused

        if not is_running:
            if hasattr(self, '_e2e_counters'):
                with self._lock:
                    self._e2e_counters['dropped_not_running'] = self._e2e_counters.get('dropped_not_running', 0) + 1
            return False

        if is_paused:
            if hasattr(self, '_e2e_counters'):
                with self._lock:
                    self._e2e_counters['dropped_paused'] = self._e2e_counters.get('dropped_paused', 0) + 1
            return False

        if hasattr(self, '_emit_historical_kline_diagnostic_on_first_tick'):
            self._emit_historical_kline_diagnostic_on_first_tick()

        if hasattr(self, '_check_and_start_historical_load_on_tick'):
            self._check_and_start_historical_load_on_tick()

        return True

    def _tick_dispatch_layer(self, tick: Any, instrument_id_raw: str) -> None:
        """分发+统计层：处理Tick并更新统计"""
        from ali2026v3_trading.subscription_manager import SubscriptionManager

        try:
            exchange = self._get_tick_field(tick, 'exchange', '')
            instrument_id = self._get_tick_field(tick, 'instrument_id', '')

            self._process_tick(tick)

            if instrument_id and instrument_id not in self._e2e_first_tick_set:
                with self._lock:
                    if instrument_id not in self._e2e_first_tick_set:
                        self._e2e_first_tick_set.add(instrument_id)
                        self._e2e_counters['first_tick_received'] += 1

            with self._tick_stats_lock:
                self._stats['total_ticks'] += 1
                self._tick_count += 1

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
            logging.error(f"[StrategyCoreService.on_tick] Error: {e}")
            with self._lock:
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now()
    
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
            # 提取关键信息
            exchange = self._get_tick_field(tick, 'exchange', '')
            instrument_id = self._get_tick_field(tick, 'instrument_id', '')
            last_price = self._get_tick_field(tick, 'last_price', 0.0)
            volume = self._get_tick_field(tick, 'volume', 0)
            
            # ========== 分子统计：平台推送过数据的合约（在任何内部处理之前）==========
            if instrument_id:
                if hasattr(self, 'storage') and self.storage and hasattr(self.storage, 'subscription_manager'):
                    sm = self.storage.subscription_manager
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
                self._order_flow_bridge.on_tick_feed(
                    instrument_id, last_price, volume, exchange,
                    bid_price=bid_price, ask_price=ask_price
                )
            except Exception as ofb_e:
                if not hasattr(self, '_ofb_error_logged'):
                    self._ofb_error_logged = True
                    logging.debug(f"[_process_tick] OrderFlowBridge feed error: {ofb_e}")
            
            if (now_ts - self._tick_debug_last_ts >= 10.0) and instrument_id:
                diag_on = False
                try:
                    from ali2026v3_trading.diagnosis_service import is_monitored_contract
                    diag_on = is_monitored_contract(instrument_id)
                except Exception as e:
                    # ✅ P1修复：添加debug日志
                    logging.debug(f"[_process_tick] Failed to check monitored contract: {e}")
                    diag_on = False
                if diag_on:
                    logging.info(f"[TickDiag] inst={instrument_id} exch={exchange} price={last_price} vol={volume}")
                self._tick_debug_last_ts = now_ts
            
            # 2. 如果开启debug_output，每条Tick都记录
            params_dict = getattr(self, 'params', {})
            if isinstance(params_dict, dict):
                debug_output = params_dict.get('debug_output', False)
            else:
                debug_output = getattr(params_dict, 'debug_output', False)
            
            if debug_output and instrument_id:
                logging.debug(f"[Tick] {exchange}.{instrument_id} price={last_price} vol={volume}")
            # ===============================================
            
            # ✅ 阶段5.3: 使用SubscriptionManager统一Tick入口
            _sub_mgr = None
            if hasattr(self, 'storage') and self.storage:
                _sub_mgr = getattr(self.storage, 'subscription_manager', None)
            if _sub_mgr:
                try:
                    _sub_mgr.on_tick(instrument_id, last_price, volume)
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('dispatched', instrument_id, last_price)
                except Exception as exc:
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('error', instrument_id, last_price, str(exc))

            if _sub_mgr and not hasattr(self, '_tick_route_confirmed'):
                self._tick_route_confirmed = True
                logging.info(f"[TickRoute] Tick数据成功路由到subscription_manager: {instrument_id}")

            if bool(getattr(self, '_historical_load_in_progress', False)):
                self._dispatch_tick_degraded(tick, instrument_id, last_price, volume, exchange)
            else:
                self._dispatch_tick(tick, instrument_id, last_price, volume, exchange)
            
        except Exception as e:
            logging.error(f"[StrategyCoreService._process_tick] Error: {e}")
            logging.exception("[StrategyCoreService._process_tick] Stack:")
            if not hasattr(self, '_tick_drop_count'):
                self._tick_drop_count = 0
            self._tick_drop_count += 1
            if self._tick_drop_count <= 10 or self._tick_drop_count % 1000 == 0:
                logging.warning(f"[TickDrop] Total ticks dropped due to unhandled errors: {self._tick_drop_count}")
        # ✅ P2#93修复：移除finally中每次tick都调用的close_connection()空操作
        # DuckDB使用线程本地连接，无需每次tick后关闭
    
    def _dispatch_tick_degraded(self, tick: Any, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
        # ✅ P0修复：初始化和递增均在锁内，避免竞态
        with getattr(self, '_probe_lock', threading.Lock()):
            if not hasattr(self, '_degraded_tick_count'):
                self._degraded_tick_count = 0
            self._degraded_tick_count += 1

        if hasattr(self, 'data_service') and self.data_service:
            try:
                rc = getattr(self.data_service, 'realtime_cache', None)
                if rc:
                    rc.update_tick(instrument_id, last_price, time.time(), volume)
            except Exception as _rc_err:
                logging.debug(f"[DegradedDispatch] realtime_cache.update_tick failed: {_rc_err}")

        tick_data = {
            'timestamp': self._get_tick_field(tick, 'timestamp', time.time()),
            'instrument_id': instrument_id,
            'exchange': exchange,
            'last_price': last_price,
            'volume': volume,
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
            uptime = datetime.now() - self._stats['start_time'] if self._stats['start_time'] else timedelta(0)
            uptime_str = str(uptime).split('.')[0]
            elapsed_seconds = uptime.total_seconds() if uptime else 1.0
            tick_rate = self._stats['total_ticks'] / max(elapsed_seconds, 1)
            
            kline_summary_line, historical_detail_line = "", ""
            if hasattr(self, '_get_historical_kline_summary_lines'):
                kline_summary_line, historical_detail_line = self._get_historical_kline_summary_lines()
            
            tick_by_type = self._stats.get('tick_by_type', {'future': 0, 'option': 0})
            tick_by_exchange = self._stats.get('tick_by_exchange', {})
            tick_by_instrument = self._stats.get('tick_by_instrument', {})
            
            sorted_exchanges = sorted(tick_by_exchange.items(), key=lambda x: x[1], reverse=True)[:5]
            sorted_instruments = sorted(tick_by_instrument.items(), key=lambda x: x[1], reverse=True)[:10]
            
            summary = f"""
{'='*80}
【3分钟状态汇总】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}
运行时长：{uptime_str}
Tick数据：总计 {self._stats['total_ticks']:,} 个 | 速率 {tick_rate:.1f} ticks/s
  - 期货：{tick_by_type.get('future', 0):,} 个
  - 期权：{tick_by_type.get('option', 0):,} 个
{kline_summary_line}
交易数量：总计 {self._stats['total_trades']:,} 笔
信号数量：总计 {self._stats['total_signals']:,} 个
错误统计：{self._stats['errors_count']} 次
最后错误：{self._stats['last_error_time'].strftime('%H:%M:%S') if self._stats['last_error_time'] else '无'}
策略状态：{self._state.value}
是否运行：{self._is_running}
是否暂停：{self._is_paused}

{historical_detail_line}

【交易所分布】
{''.join([f'  - {exchange}: {count:,} 个\n' for exchange, count in sorted_exchanges])}

【合约活跃度 Top 10】
{''.join([f'  - {inst}: {count:,} 个\n' for inst, count in sorted_instruments])}
{'='*80}
"""
            logging.info(summary)

            try:
                if hasattr(self, '_state_param_manager') and self._state_param_manager:
                    spm = self._state_param_manager
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
                logging.debug(f"[StateRoute] StateParamManager output error: {spm_e}")

            try:
                if hasattr(self, '_safety_meta_layer') and self._safety_meta_layer:
                    safety_stats = self._safety_meta_layer.get_stats()
                    logging.info(
                        "[SafetyMeta] 断路器触发=%d, 硬时间止损=%d, 日回撤停止=%d, 暂停中=%s, 禁止开仓=%s",
                        safety_stats.get('circuit_breaker_triggers', 0),
                        safety_stats.get('hard_time_stop_triggers', 0),
                        safety_stats.get('daily_drawdown_triggers', 0),
                        safety_stats.get('trading_paused', False),
                        safety_stats.get('new_open_blocked', False),
                    )
            except Exception as safety_e:
                logging.debug(f"[SafetyMeta] SafetyMetaLayer output error: {safety_e}")

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
                logging.debug(f"[OrderFlow] output error: {ofb_e}")
            
            # 输出订阅成功率统计
            if hasattr(self, 'storage') and self.storage and hasattr(self.storage, 'subscription_manager'):
                sm = self.storage.subscription_manager
                if sm and hasattr(sm, 'log_subscription_success_summary'):
                    sm.log_subscription_success_summary()
                    
        except Exception as e:
            logging.error(f"[Periodic Summary] 输出失败：{e}")
    
    def _flush_tick_buffer(self) -> None:
        total_flushed = 0
        with self._shard_buffers_lock:
            shard_snapshots = {}
            for shard_idx, buf in list(self._shard_buffers.items()):
                if buf:
                    shard_snapshots[shard_idx] = buf[:]
                    buf.clear()

        for shard_idx, ticks in shard_snapshots.items():
            logging.info(f"[_flush_tick_buffer] Flushing shard={shard_idx} {len(ticks)} ticks")
            shard_lock = self._get_shard_lock(shard_idx)
            failed = []
            for tick_item in ticks:
                try:
                    self.storage.process_tick(tick_item)
                    if hasattr(self, '_e2e_counters'):
                        with self._lock:
                            self._e2e_counters['persisted_count'] += 1
                            if hasattr(self, '_e2e_shard_persisted'):
                                self._e2e_shard_persisted[shard_idx] = self._e2e_shard_persisted.get(shard_idx, 0) + 1
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('saved', tick_item.get('instrument_id', ''), tick_item.get('last_price', 0.0))
                    total_flushed += 1
                except Exception as e:
                    logging.error(f"[_flush_tick_buffer] shard={shard_idx}: {e}")
                    failed.append(tick_item)
            if failed:
                with shard_lock:
                    self._shard_buffers.setdefault(shard_idx, []).extend(failed)
                logging.warning(f"[_flush_tick_buffer] shard={shard_idx} {len(failed)}/{len(ticks)} 回写")
        if total_flushed:
            logging.info(f"[_flush_tick_buffer] Total flushed: {total_flushed}")

    def _save_tick_snapshot(self):
        """覆盖式快照：将RealTimeCache全部tick写入磁盘，仅保留最后状态"""
        if not self._snapshot_path:
            try:
                ds = getattr(self, 'data_service', None) or getattr(self.storage, '_data_service', None)
                if ds and hasattr(ds, '_data_dir'):
                    import os as _os
                    self._snapshot_path = _os.path.join(ds._data_dir, 'tick_snapshot.json')
            except Exception as _snap_err:
                logging.debug(f"[_save_tick_snapshot] Path resolution failed: {_snap_err}")
                return
        if not self._snapshot_path:
            return
        try:
            rc = getattr(self, 'data_service', None) or getattr(self.storage, '_data_service', None)
            rc = getattr(rc, 'realtime_cache', None) if rc else None
            if not rc:
                return
            import json as _json, os as _os
            from datetime import datetime as _dt
            snapshot = {'timestamp': _dt.now().isoformat(), 'ticks': {}}
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
                f.write(_json.dumps(snapshot, ensure_ascii=False))
        except Exception as e:
            logging.debug(f"[_save_tick_snapshot] error: {e}")

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
            logging.error(f"[atexit] Tick缓冲区刷写失败: {e}")
    
    # ✅ 序号116-118修复：统一Tick分发入口
    def _dispatch_tick(self, tick, instrument_id: str, last_price: float, volume: int, exchange: str) -> None:
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
                    timestamp=_dt.now(), volume=volume,
                    bid_price=bid_p, ask_price=ask_p
                )
        except Exception as e:
            logging.debug(f"[_dispatch_tick] RealTimeCache update skipped: {e}")

        # 2. 保存Tick数据到数据库并合成K线
        if hasattr(self, 'storage') and self.storage:
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

                    buffer_size = len(self._shard_buffers[shard_idx])
                    threshold = self._tick_buffer_threshold
                    should_flush = buffer_size >= threshold
                    if not should_flush and buffer_size > 0:
                        elapsed = time.time() - self._last_buffer_flush_time
                        if elapsed >= self._tick_buffer_flush_interval:
                            should_flush = True

                    if should_flush:
                        ticks_to_flush = self._shard_buffers[shard_idx][:]
                        self._shard_buffers[shard_idx].clear()
                        self._last_buffer_flush_time = time.time()
                    else:
                        ticks_to_flush = None

                with self._lock:
                    self._e2e_counters['enqueued_count'] += 1
                    if hasattr(self, '_e2e_shard_enqueued'):
                        self._e2e_shard_enqueued[shard_idx] = self._e2e_shard_enqueued.get(shard_idx, 0) + 1

                # ✅ P0修复：加锁保护_snapshot_tick_count递增
                with getattr(self, '_probe_lock', threading.Lock()):
                    self._snapshot_tick_count += 1
                    should_snapshot = (self._snapshot_tick_count % self._snapshot_interval == 0)
                
                if should_snapshot:
                    self._save_tick_snapshot()

                if should_flush and ticks_to_flush:
                    failed_ticks = []
                    for tick_item in ticks_to_flush:
                        try:
                            self.storage.process_tick(tick_item)
                            with self._lock:
                                self._e2e_counters['persisted_count'] += 1
                                if hasattr(self, '_e2e_shard_persisted'):
                                    self._e2e_shard_persisted[shard_idx] = self._e2e_shard_persisted.get(shard_idx, 0) + 1
                            from ali2026v3_trading.diagnosis_service import record_tick_probe
                            record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                        except Exception as e:
                            logging.warning(f"[_dispatch_tick] shard={shard_idx} flush失败: {e}")
                            failed_ticks.append(tick_item)
                    if failed_ticks:
                        with shard_lock:
                            self._shard_buffers.setdefault(shard_idx, []).extend(failed_ticks)
                        logging.warning(f"[_dispatch_tick] shard={shard_idx} {len(failed_ticks)}/{len(ticks_to_flush)} 回写buffer")
            except Exception as e:
                from ali2026v3_trading.diagnosis_service import record_tick_probe
                record_tick_probe('error', instrument_id, last_price, f"Storage: {e}")
                logging.exception(f"[_dispatch_tick] Stack trace for instrument: {instrument_id}")
        
        # 3. 分发Tick给PositionService做止盈止损检查
        try:
            from ali2026v3_trading.position_service import get_position_service
            pos_svc = get_position_service()
            pos_svc.on_tick(tick)
        except Exception as pos_e:
            logging.warning(f"[_dispatch_tick] PositionService.on_tick failed: {pos_e}")

        # 4. 分发Tick给HFT增强引擎(七大竞争优势)
        try:
            hft = getattr(self, '_hft_engine', None)
            if hft is None:
                try:
                    self._ensure_hft_engine()
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
                    spm = getattr(self, '_state_param_manager', None)
                    if spm:
                        current_state = spm.get_current_state()
                        prev_state = getattr(spm, '_prev_state', 'other')
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
                            logging.debug("[_dispatch_tick] HFT pursuit exit execution error: %s", ex_e)

                    arbitrage_signal = hft_result.get('arbitrage_signal')
                    if arbitrage_signal:
                        self._handle_arbitrage_signal(arbitrage_signal, instrument_id)

                    transition_signal = hft_result.get('transition_signal')
                    if transition_signal:
                        self._handle_transition_signal(transition_signal, instrument_id, last_price)

                    smart_money_signal = hft_result.get('smart_money_signal')
                    if smart_money_signal:
                        self._handle_smart_money_signal(smart_money_signal, instrument_id)

                    signal_filter_result = hft_result.get('signal_filter')
                    if signal_filter_result and signal_filter_result.get('threshold_crossed'):
                        self._handle_filtered_signal(signal_filter_result, instrument_id, last_price)
        except Exception as hft_e:
            if not hasattr(self, '_hft_dispatch_error_logged'):
                self._hft_dispatch_error_logged = True
                logging.warning(f"[_dispatch_tick] HFT engine dispatch error: {hft_e}")

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
        if pos and not pos.platform_confirmed and not platform_order_ids:
            pe._positions.pop(instrument_id, None)
            logging.warning("[HFT] pursuit exit: %s was never opened on platform, cleaned up", instrument_id)
            return
        close_signal_type = 'CLOSE_LONG' if direction == 'SELL' else 'CLOSE_SHORT'
        try:
            self._ensure_order_service()
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
                    logging.debug("[HFT] send_defensive_order failed, fallback to signal: %s", def_e)
            sig_svc = getattr(self, '_signal_service', None)
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal_service import SignalService
                    sig_svc = SignalService()
                except Exception:
                    pass
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
        try:
            self._ensure_order_service()
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
        try:
            self._ensure_order_service()
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
            sig_svc = getattr(self, '_signal_service', None)
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal_service import SignalService
                    sig_svc = SignalService()
                except Exception:
                    pass
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
                                   instrument_id: str, last_price: float) -> None:
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
                sig_svc = getattr(self, '_signal_service', None)
                if sig_svc is None:
                    try:
                        from ali2026v3_trading.signal_service import SignalService
                        sig_svc = SignalService()
                    except Exception:
                        pass
                if sig_svc:
                    direction_upper = 'BUY'
                    sig_svc.generate_signal(
                        instrument_id=instrument_id, signal_type='OPEN_LONG',
                        price=last_price, volume=1,
                        reason='hft_transition_capture',
                        priority=9, cooldown_seconds=3,
                    )
                    logging.info("[HFT] transition entry signal: %s dir=BUY reason=%s",
                                 instrument_id, transition_type)
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
            sig_svc = getattr(self, '_signal_service', None)
            if sig_svc is None:
                try:
                    from ali2026v3_trading.signal_service import SignalService
                    sig_svc = SignalService()
                except Exception:
                    pass
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

    def _ensure_order_service(self) -> None:
        if getattr(self, '_order_service', None) is not None:
            return
        try:
            from ali2026v3_trading.order_service import get_order_service
            self._order_service = get_order_service()
        except Exception as e:
            logging.debug("[_ensure_order_service] %s", e)


@dataclass
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
            if pos_dir == 'BUY' and current_price <= pos.weighted_avg_price:
                return None
            if pos_dir == 'SELL' and current_price >= pos.weighted_avg_price:
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
                if current_price >= pos.current_stop_profit:
                    should_exit, reason = True, 'pursuit_take_profit'
                elif current_price <= pos.current_stop_loss:
                    should_exit, reason = True, 'pursuit_stop_loss'
            else:
                if current_price >= pos.current_stop_loss:
                    should_exit, reason = True, 'pursuit_stop_loss'
                elif current_price <= pos.current_stop_profit:
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
                 atr_reference: float = 0.02):
        self._max_levels = max_levels
        self._pyramid_ratio = pyramid_ratio
        self._atr_adaptive = atr_adaptive
        self._atr_reference = atr_reference
        self._positions: Dict[str, Dict] = {}
        self._stats = {'total_adds': 0, 'total_volume_added': 0}

    def calc_add_volume(self, instrument_id: str, base_volume: int,
                        current_level: int, current_atr: float = 0.0) -> int:
        if current_level >= self._max_levels:
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
        closed_keys = [k for k, p in self._positions.items() if not p.is_open]
        if len(closed_keys) > max_closed:
            for k in closed_keys[:len(closed_keys) - max_closed]:
                del self._positions[k]
