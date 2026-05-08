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
from typing import Any, Dict


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

            with self._lock:
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
            # ✅ P2#92修复：不再静默吞掉异常，记录warning日志
            logging.warning(f"[_dispatch_tick] PositionService.on_tick failed: {pos_e}")
