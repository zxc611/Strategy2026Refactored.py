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
        
        # ✅ P0修复方案C：Tick缓冲机制 - 在业务层聚合后批量传给storage
        self._tick_buffer = []
        self._tick_buffer_lock = threading.Lock()
        self._tick_buffer_threshold = 50  # 累积50条tick再批量保存
        self._tick_buffer_flush_interval = 5.0  # 5秒未满也flush
        self._last_buffer_flush_time = time.time()
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
        """Tick数据回调 - 核心热路径
        
        Args:
            tick: Tick数据对象
            
        处理流程：
        1. 记录原始合约格式（Probe）
        2. 状态检查（运行/暂停）
        3. 触发历史K线诊断和加载
        4. 处理Tick数据
        5. 更新统计信息
        6. 周期性日志输出
        """
        from ali2026v3_trading.subscription_manager import SubscriptionManager

        # PROBE: 记录所有进入on_tick的合约原始格式
        # ✅ 方法唯一修复：字段优先级[PythonGO, INFINIGO]与storage._TICK_FIELD_ALIASES一致
        instrument_id_raw = self._get_tick_field(tick, 'instrument_id', '')
        if instrument_id_raw:
            # 只记录前100个不同的合约，避免日志过多
            # ✅ P1#26修复：加锁保护set的并发修改
            with self._probe_lock:
                if len(self._probe_logged_instruments) < 100:
                    if instrument_id_raw not in self._probe_logged_instruments:
                        self._probe_logged_instruments.add(instrument_id_raw)
                        logging.info(f"[PROBE_RAW_INSTRUMENT_ID] 平台推送原始格式: '{instrument_id_raw}' (type={type(instrument_id_raw).__name__})")
        
        # ✅ 阶段6增强: Tick入口探针（汇总模式）+ 重点合约诊断
        if instrument_id_raw:
            diag_on = False
            try:
                from ali2026v3_trading.diagnosis_service import is_monitored_contract
                diag_on = is_monitored_contract(instrument_id_raw)
            except Exception:
                pass
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
            
            # ✅ 环节4: Tick入口诊断（仅监控合约）
            last_price = self._get_tick_field(tick, 'last_price', 0.0)
            volume = self._get_tick_field(tick, 'volume', 0)
            open_interest = self._get_tick_field(tick, 'open_interest', 0)
            contract_type = 'option' if SubscriptionManager.is_option(instrument_id_raw) else 'future'
            DiagnosisProbeManager.on_tick_entry(instrument_id_raw, last_price, volume, open_interest, contract_type)
        
        # 添加状态检查日志
        with self._lock:  # ✅ M20 Bug #2修复：加锁读取状态
            is_running = self._is_running
            is_paused = self._is_paused
        
        if not is_running:
            if hasattr(self, '_e2e_counters'):
                with self._lock:
                    self._e2e_counters['dropped_not_running'] = self._e2e_counters.get('dropped_not_running', 0) + 1
            return

        if is_paused:
            if hasattr(self, '_e2e_counters'):
                with self._lock:
                    self._e2e_counters['dropped_paused'] = self._e2e_counters.get('dropped_paused', 0) + 1
            return

        # 在第一个Tick时输出历史K线诊断信息
        if hasattr(self, '_emit_historical_kline_diagnostic_on_first_tick'):
            self._emit_historical_kline_diagnostic_on_first_tick()
        
        # 检查并启动历史K线加载
        if hasattr(self, '_check_and_start_historical_load_on_tick'):
            self._check_and_start_historical_load_on_tick()
        
        try:
            # 提取关键信息用于统计
            exchange = self._get_tick_field(tick, 'exchange', '')
            instrument_id = self._get_tick_field(tick, 'instrument_id', '')
            
            # 处理Tick数据
            self._process_tick(tick)
            
            # P0修复：e2e段4 - 首次收到某合约Tick时递增first_tick_received
            if instrument_id and instrument_id not in self._e2e_first_tick_set:
                with self._lock:
                    if instrument_id not in self._e2e_first_tick_set:
                        self._e2e_first_tick_set.add(instrument_id)
                        self._e2e_counters['first_tick_received'] += 1
            
            # 统计（加锁保护）
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

            # ✅ P1#27修复：_tick_count读取也放入锁内，确保递增和判断的原子性
            with self._lock:
                tick_count_snapshot = self._tick_count
            
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
                except Exception:
                    pass
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

            if not bool(getattr(self, '_historical_load_in_progress', False)):
                self._dispatch_tick(tick, instrument_id, last_price, volume, exchange)
            
        except Exception as e:
            logging.error(f"[StrategyCoreService._process_tick] Error: {e}")
            logging.exception("[StrategyCoreService._process_tick] Stack:")
        # ✅ P2#93修复：移除finally中每次tick都调用的close_connection()空操作
        # DuckDB使用线程本地连接，无需每次tick后关闭
    
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
        """flush Tick缓冲区（策略停止时调用）
        
        确保缓冲区中的tick数据全部保存到storage
        """
        # ✅ P1#23+P1#24修复：锁内只做buffer拷贝和清空，DB写入移到锁外
        with self._tick_buffer_lock:
            if self._tick_buffer:
                ticks_to_flush = self._tick_buffer[:]
                buffer_size = len(ticks_to_flush)
                self._tick_buffer.clear()
            else:
                ticks_to_flush = []
                buffer_size = 0
        
        if ticks_to_flush:
            logging.info(f"[_flush_tick_buffer] Flushing {buffer_size} ticks from buffer")
            
            # ✅ 环节5b: Tick flush诊断（仅监控合约）
            from ali2026v3_trading.diagnosis_service import diagnose_tick_flush, is_monitored_contract
            monitored_count = sum(1 for tick in ticks_to_flush 
                                 if is_monitored_contract(tick.get('instrument_id', '') if isinstance(tick, dict) else self._get_tick_field(tick, 'instrument_id', '')))
            if monitored_count > 0:
                diagnose_tick_flush(buffer_size, monitored_count)
            
            failed_ticks = []
            for tick_item in ticks_to_flush:
                try:
                    self.storage.process_tick(tick_item)
                    # P0修复：e2e段6 - Tick落盘计数
                    # ✅ M20 Bug #1修复：统一使用self._lock保护e2e_counters
                    if hasattr(self, '_e2e_counters'):
                        with self._lock:
                            self._e2e_counters['persisted_count'] += 1
                    # ✅ BP-16修复：在实际写入成功后记录Saved探针
                    from ali2026v3_trading.diagnosis_service import record_tick_probe
                    record_tick_probe('saved', tick_item.get('instrument_id', ''), tick_item.get('last_price', 0.0))
                except Exception as e:
                    logging.error(f"[_flush_tick_buffer] Failed to save tick: {e}")
                    failed_ticks.append(tick_item)
            # ✅ BP-14修复：flush失败的tick回写buffer，防止数据丢失
            if failed_ticks:
                with self._tick_buffer_lock:
                    self._tick_buffer.extend(failed_ticks)
                logging.warning(f"[_flush_tick_buffer] {len(failed_ticks)}/{buffer_size} ticks failed, written back to buffer")

    def _save_tick_snapshot(self):
        """覆盖式快照：将RealTimeCache全部tick写入磁盘，仅保留最后状态"""
        if not self._snapshot_path:
            try:
                ds = getattr(self, 'data_service', None) or getattr(self.storage, '_data_service', None)
                if ds and hasattr(ds, '_data_dir'):
                    import os as _os
                    self._snapshot_path = _os.path.join(ds._data_dir, 'tick_snapshot.json')
            except Exception:
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
        """atexit回调：进程退出时刷写Tick缓冲区+最后一次快照"""
        self = weak_self()
        if self is None:
            return
        try:
            if hasattr(self, '_tick_buffer') and self._tick_buffer:
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
                
                # ✅ P0修复方案C：Tick缓冲机制 - 累积到阈值再批量保存
                with self._tick_buffer_lock:
                    self._tick_buffer.append(tick_data)
                    
                    # ✅ 环节5a: Tick缓冲状态监控（仅监控合约）
                    from ali2026v3_trading.diagnosis_service import diagnose_tick_buffer, is_monitored_contract
                    if is_monitored_contract(instrument_id):
                        buffer_size = len(self._tick_buffer)
                        threshold = self._tick_buffer_threshold
                        fill_rate = (buffer_size / threshold * 100) if threshold > 0 else 0
                        diagnose_tick_buffer(buffer_size, threshold, fill_rate)
                    
                    # 达到阈值或超时时批量flush
                    should_flush = len(self._tick_buffer) >= self._tick_buffer_threshold
                    if not should_flush and len(self._tick_buffer) > 0:
                        elapsed = time.time() - self._last_buffer_flush_time
                        if elapsed >= self._tick_buffer_flush_interval:
                            should_flush = True
                    
                    if should_flush:
                        ticks_to_flush = self._tick_buffer[:]
                        self._tick_buffer.clear()
                        self._last_buffer_flush_time = time.time()
                
                # ✅ 死锁修复：e2e计数移到_tick_buffer_lock外，统一锁序 _tick_buffer_lock → _lock
                # P0修复：e2e段5 - Tick入队计数
                with self._lock:
                    self._e2e_counters['enqueued_count'] += 1
                
                # ✅ 覆盖式快照WAL：每N条tick快照RealTimeCache到磁盘
                self._snapshot_tick_count += 1
                if self._snapshot_tick_count % self._snapshot_interval == 0:
                    self._save_tick_snapshot()
                
                # ✅ P1#23+P1#24修复：DB写入移到锁外执行，避免持锁期间阻塞新Tick
                if should_flush:
                    failed_ticks = []
                    for tick_item in ticks_to_flush:
                        try:
                            self.storage.process_tick(tick_item)
                            # P0修复：e2e段6 - Tick落盘计数
                            # ✅ M20 Bug #1修复：统一使用self._lock保护e2e_counters
                            with self._lock:
                                self._e2e_counters['persisted_count'] += 1
                            # ✅ BP-15修复：在实际写入成功后记录Saved探针
                            from ali2026v3_trading.diagnosis_service import record_tick_probe
                            record_tick_probe('saved', tick_item.get('instrument_id', instrument_id), tick_item.get('last_price', last_price))
                        except Exception as e:
                            logging.warning(f"[_dispatch_tick] 批量保存失败: {e}")
                            failed_ticks.append(tick_item)
                    # ✅ BP-14修复：flush失败的tick回写buffer，防止数据丢失
                    if failed_ticks:
                        with self._tick_buffer_lock:
                            self._tick_buffer.extend(failed_ticks)
                        logging.warning(f"[_dispatch_tick] {len(failed_ticks)}/{len(ticks_to_flush)} ticks failed, written back to buffer")
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
