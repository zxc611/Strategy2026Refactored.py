"""交易逻辑与信号处理（依照 Strategy20260105_3.py 重构）。"""
from __future__ import annotations

import threading 
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set
from .market_calendar import is_market_open_safe

class TradingLogicMixin:

    def _init_trading_logic(self) -> None:
        """初始化交易逻辑运行所需状态。"""
        # [Fix] Add lock for run_trading_cycle to prevent overlapping
        self._trading_cycle_lock = threading.Lock()

        try:
            if not hasattr(self, "latest_ticks") or self.latest_ticks is None:
                self.latest_ticks = {}
            if not hasattr(self, "last_check_date_closing"):
                self.last_check_date_closing = None
            if not hasattr(self, "_last_tick_arrival_ts"):
                self._last_tick_arrival_ts = 0.0
            if not hasattr(self, "_last_reconnect_reset_ts"):
                self._last_reconnect_reset_ts = 0.0
        except Exception:
            pass
    
    def on_tick(self, tick: Any) -> None:
        """
        行情处理入口。
        """
        try:
            self._log_tick_debug(tick)
            now_ts = time.time()
            self._check_reconnect_and_clear_cache(now_ts)

            if self._is_tick_stale(tick, now_ts):
                return
            # 1. 保存行情快照
            # Assume self.latest_ticks exists or create it
            if not hasattr(self, "latest_ticks"):
                self.latest_ticks = {}
            
            # Compatible with different Tick objects
            inst = getattr(tick, "instrument_id", "") or getattr(tick, "InstrumentID", "")
            if inst:
                self.latest_ticks[inst] = tick
                try:
                    norm = self._normalize_future_id(inst)
                    if norm and norm != inst:
                        self.latest_ticks[norm] = tick
                except Exception:
                    pass

                # Also store by exchange.inst? 
                exch = getattr(tick, "exchange", "") or getattr(tick, "ExchangeID", "")
                if exch:
                     self.latest_ticks[f"{exch}.{inst}"] = tick
            self._last_tick_arrival_ts = now_ts
            
            # 2. 也是K线生成器的驱动入口
            if hasattr(self, "update_tick_data"):
                self.update_tick_data(tick)

            # (Legacy Support)
            if hasattr(self, "kline_generator") and self.kline_generator:
                try:
                    self.kline_generator.tick_to_kline(tick)
                except Exception: pass
            
            # 3. Position Manager Live Checks (Stop Profit etc)
            if hasattr(self, "position_manager") and self.position_manager:
                self.position_manager.on_tick(tick)

            # 4. Note: Trading Cycle is run by Scheduler, not on_tick loop
            
            # 5. Check Daily Closing (14:58/15:58)
            self._check_daily_closing(tick)

        except Exception as e:
            # self.output(f"on_tick logic error: {e}")
            pass

    def _check_reconnect_and_clear_cache(self, now_ts: float) -> None:
        """断网/重连后清理内存缓存，避免使用过期行情。"""
        try:
            gap_sec = float(getattr(self.params, "reconnect_gap_seconds", 20) or 20)
        except Exception:
            gap_sec = 20.0

        try:
            last_ts = float(getattr(self, "_last_tick_arrival_ts", 0.0) or 0.0)
            if last_ts <= 0:
                return
            if now_ts - last_ts < gap_sec:
                return

            last_reset = float(getattr(self, "_last_reconnect_reset_ts", 0.0) or 0.0)
            if now_ts - last_reset < gap_sec:
                return

            self._clear_realtime_caches()
            self._last_reconnect_reset_ts = now_ts

            if getattr(self.params, "debug_output", False):
                self.output("[Cache] Reconnect detected, realtime caches cleared.")
        except Exception:
            pass

    def _clear_realtime_caches(self) -> None:
        """清理与实时行情相关的缓存。"""
        try:
            if hasattr(self, "latest_ticks"):
                self.latest_ticks = {}
        except Exception:
            pass

        try:
            if hasattr(self, "_kline_mem_cache"):
                with getattr(self, "_kline_cache_lock", threading.Lock()):
                    self._kline_mem_cache.clear()
        except Exception:
            pass

    def _is_tick_stale(self, tick: Any, now_ts: float) -> bool:
        """过期判定：忽略时间戳明显滞后的Tick。"""
        try:
            max_age = float(getattr(self.params, "tick_stale_seconds", 15) or 15)
        except Exception:
            max_age = 15.0

        try:
            tick_ts = self._extract_tick_timestamp(tick)
            if not tick_ts:
                return False
            return (now_ts - tick_ts) > max_age
        except Exception:
            return False

    def _extract_tick_timestamp(self, tick: Any) -> Optional[float]:
        """尽量从Tick中解析出时间戳(秒)。"""
        try:
            for field in ("timestamp", "recv_time", "RecvTime", "local_time", "LocalTime"):
                val = getattr(tick, field, None)
                ts = self._coerce_to_epoch_seconds(val)
                if ts:
                    return ts

            update_time = getattr(tick, "UpdateTime", None) or getattr(tick, "update_time", None)
            trading_day = getattr(tick, "TradingDay", None) or getattr(tick, "trading_day", None)
            action_day = getattr(tick, "ActionDay", None) or getattr(tick, "action_day", None)
            day = trading_day or action_day
            return self._parse_tick_time(update_time, day)
        except Exception:
            return None

    def _coerce_to_epoch_seconds(self, val: Any) -> Optional[float]:
        try:
            if val is None:
                return None
            if isinstance(val, (int, float)):
                ts = float(val)
                if ts > 1e12:
                    ts = ts / 1000.0
                return ts if ts > 0 else None
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                if val.isdigit():
                    ts = float(val)
                    if ts > 1e12:
                        ts = ts / 1000.0
                    return ts if ts > 0 else None
                return self._parse_tick_time(val, None)
            return None
        except Exception:
            return None

    def _parse_tick_time(self, time_str: Any, day_str: Any) -> Optional[float]:
        try:
            if not time_str:
                return None
            t_str = str(time_str).strip()
            if not t_str:
                return None

            day = None
            if day_str:
                day = str(day_str).strip()
                if day.isdigit() and len(day) == 8:
                    day = f"{day[0:4]}-{day[4:6]}-{day[6:8]}"

            patterns = [
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y%m%d %H:%M:%S",
                "%H:%M:%S.%f",
                "%H:%M:%S",
            ]
            if day:
                candidate = f"{day} {t_str}"
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        dt = datetime.strptime(candidate, fmt)
                        return dt.timestamp()
                    except Exception:
                        continue

            for fmt in patterns:
                try:
                    dt = datetime.strptime(t_str, fmt)
                    return dt.timestamp()
                except Exception:
                    continue
            return None
        except Exception:
            return None

    def _log_tick_debug(self, tick: Any) -> None:
        """最小化输出 tick 字段与关键值，用于确认回放来源。"""
        try:
            if not getattr(self.params, "debug_output", False):
                return
        except Exception:
            return

        try:
            now_ts = time.time()
            last_ts = getattr(self, "_tick_debug_last_ts", 0.0)
            if now_ts - last_ts < 10.0:
                return
            self._tick_debug_last_ts = now_ts

            inst = getattr(tick, "instrument_id", "") or getattr(tick, "InstrumentID", "")
            exch = getattr(tick, "exchange", "") or getattr(tick, "ExchangeID", "")
            last_price = (
                getattr(tick, "last_price", None)
                or getattr(tick, "LastPrice", None)
                or getattr(tick, "last", None)
                or getattr(tick, "price", None)
            )
            ask = getattr(tick, "ask", None) or getattr(tick, "AskPrice1", None)
            bid = getattr(tick, "bid", None) or getattr(tick, "BidPrice1", None)
            volume = getattr(tick, "volume", None) or getattr(tick, "Volume", None)
            update_time = (
                getattr(tick, "UpdateTime", None)
                or getattr(tick, "update_time", None)
                or getattr(tick, "timestamp", None)
            )
            recv_time = getattr(tick, "recv_time", None) or getattr(tick, "RecvTime", None)
            local_time = getattr(tick, "local_time", None) or getattr(tick, "LocalTime", None)
            trading_day = getattr(tick, "TradingDay", None) or getattr(tick, "trading_day", None)
            action_day = getattr(tick, "ActionDay", None) or getattr(tick, "action_day", None)
            is_replay = getattr(tick, "is_replay", None) or getattr(tick, "IsReplay", None)
            source = getattr(tick, "source", None) or getattr(tick, "Source", None)

            keys = []
            try:
                keys = list(vars(tick).keys())
            except Exception:
                keys = []
            keys_sample = ",".join(sorted(keys)[:20]) if keys else ""

            self.output(
                "[TickDiag] inst={inst} exch={exch} last={last} bid={bid} ask={ask} vol={vol} "
                "update_time={ut} recv_time={rt} local_time={lt} trading_day={td} action_day={ad} "
                "is_replay={ir} source={src} keys={keys}".format(
                    inst=inst,
                    exch=exch,
                    last=last_price,
                    bid=bid,
                    ask=ask,
                    vol=volume,
                    ut=update_time,
                    rt=recv_time,
                    lt=local_time,
                    td=trading_day,
                    ad=action_day,
                    ir=is_replay,
                    src=source,
                    keys=keys_sample,
                )
            )
        except Exception:
            pass
            
    def _check_daily_closing(self, tick: Any) -> None:
        """Ported from _execute_1558_closing trigger logic"""
        try:
            if not self.position_manager: return
            
            now = datetime.now()
            # Simple Trigger Check
            
            # We need state to avoid repeat trigger
            if not hasattr(self, "last_check_date_closing"):
                self.last_check_date_closing = None
            
            if self.last_check_date_closing == now.date():
                return
            
            # Check Time
            target_str = getattr(self.params, "close_daycut_time", "15:58") 
            th, tm = map(int, target_str.split(":"))
            
            if now.hour == th and now.minute >= tm:
                self.output(f"Triggering Daily Closing at {now}")
                self._execute_daily_closing()
                self.last_check_date_closing = now.date()
                
        except Exception:
            pass

    def _execute_daily_closing(self) -> None:
        """Close all intraday positions or based on logic"""
        # Logic from Source _execute_1558_closing
        if hasattr(self, "_execute_1558_closing"):
            self._execute_1558_closing()
        elif self.position_manager:
            # Fallback if mixin not loaded?
            pass

    def run_trading_cycle(self) -> None:
        """主交易循环：计算 -> 信号 -> 执行"""
        # [Fix] Use lock to ensure only one cycle runs at a time (even if thread spawned)
        if not hasattr(self, "_trading_cycle_lock"):
             self._trading_cycle_lock = threading.Lock()

        # Non-blocking lock check - if locked, skip this cycle
        if not self._trading_cycle_lock.acquire(blocking=False):
            self.output("[Cycle] Previous cycle still running, skipping this trigger.")
            return

        try:
            # [Fix Scenario 3] Pause Check
            if self.is_paused or not self.is_running:
                # 中文注释：强制阻断暂停/未运行状态，避免任何计算路径被绕过
                try:
                    now_ts = time.time()
                    last_ts = getattr(self, "_calc_skip_log_time", 0)
                    if now_ts - last_ts >= 60:
                        self.output(
                            f"[诊断] 交易循环跳过: paused={self.is_paused}, running={self.is_running}",
                            force=True,
                        )
                        self._calc_skip_log_time = now_ts
                except Exception:
                    pass
                return

            # Track last successful cycle start for watchdog checks
            self._last_trading_cycle_ts = time.time()

            # [Optimized] Moved check_and_close_overdue_positions to separate independent job
            # to prevent it from being blocked by heavy calculation or vice-versa.
            # if hasattr(self, "position_manager") and self.position_manager:
            #      self.position_manager.check_and_close_overdue_positions(days_limit=3)

            # 1. 计算所有宽度
            if self.is_paused or not self.is_running: return
            self.calculate_all_option_widths()
            
            # 2. 生成信号
            if self.is_paused or not self.is_running: return
            signals = self.generate_trading_signals_optimized()
            # [Optimize] Even if no signals, let output handle debug printing (e.g. "No signals")
            # if not signals: return 

            # 3. 输出信号日志
            self.output_trading_signals_optimized(signals)
            
            # 4. 执行信号
            self.execute_signals_optimized(signals)

        except Exception as e:
            self.output(f"交易循环执行异常: {e}")
        finally:
            try:
                self._last_trading_cycle_end_ts = time.time()
            except Exception:
                pass
            self._trading_cycle_lock.release()

    def execute_signals_optimized(self, signals: List[Dict[str, Any]]) -> None:
        """执行优化后的信号列表（含时效性检查与签名去重）"""
        if not signals:
             return

        # 1. 开盘时间检查
        if not is_market_open_safe(self):
            # self.output("非开盘时间，忽略交易信号执行")
            return

        # 2. 时效性检查 (Signal Freshness)
        now = datetime.now()
        try:
            max_age = int(getattr(self.params, "signal_max_age_sec", 180) or 180)
        except: max_age = 180
        
        def _normalize_signal_ts(ts_value: Any) -> Optional[datetime]:
            if isinstance(ts_value, datetime):
                return ts_value
            if isinstance(ts_value, str):
                try:
                    return datetime.fromisoformat(ts_value)
                except Exception:
                    return None
            return None

        fresh_signals = []
        missing_ts = 0
        for sig in signals:
            ts = _normalize_signal_ts(sig.get("timestamp"))
            if not isinstance(ts, datetime):
                # Assume fresh if generated by us just now, but if timestamp missing...
                # Actually our calc sets timestamp as str isoformat usually
                missing_ts += 1
                continue
                
            delta = (now - ts).total_seconds()
            if delta <= max_age:
                fresh_signals.append(sig)
            else:
                pass # Expired
        
        if missing_ts:
            try:
                debug_on = bool(getattr(self.params, "debug_output", False))
            except Exception:
                debug_on = False
            if debug_on:
                try:
                    now_ts = time.time()
                    last_ts = getattr(self, "_signal_ts_missing_log_ts", 0.0)
                    if now_ts - last_ts >= 60.0:
                        self._signal_ts_missing_log_ts = now_ts
                        self.output(f"[信号丢弃] 缺失时间戳的信号数量: {missing_ts}")
                except Exception:
                    pass

        if not fresh_signals:
            return
            
        # 3. 签名去重 (Signature Deduplication)
        # 取 Top 3 (或 Top 1，视配置)
        top_n = getattr(self.params, "top3_rows", 3)
        top_signals = fresh_signals[:top_n]
        
        sig_items = []
        for sig in top_signals:
            tm = ",".join(sig.get("target_option_contracts", []))
            # Include action/direction in signature to detect flips
            item = f"{sig.get('exchange')}.{sig.get('future_id')}|{sig.get('signal_type')}|{sig.get('action')}|{tm}"
            sig_items.append(item)
        
        current_signature = "||".join(sig_items)
        last_signature = getattr(self, "top3_last_signature", None)
        
        if current_signature == last_signature:
            # 信号未发生实质变化，跳过执行
            return
            
        self.top3_last_signature = current_signature
        self.output(f"[交易执行] 信号签名更新: {current_signature}")

        # 4. 执行逻辑 (仅执行 Top 1 最优信号，或根据策略需求执行 Top N)
        # 这里默认只执行优先级最高的一个信号
        target_signal = top_signals[0] 
        
        # 检查是否冷却
        future_id = target_signal.get("future_id")
        if self._is_cooling_down(future_id):
             self.output(f"[交易执行] {future_id} 处于冷却期，跳过")
             return

        # 执行
        if target_signal.get("is_direct_option_trade", False):
             self._execute_direct_option_trade(target_signal)


    def _is_cooling_down(self, symbol: str) -> bool:
        """检查信号是否在冷却期"""
        try:
             if not hasattr(self, "signal_times"):
                 self.signal_times = {} 
             
             last_time = self.signal_times.get(symbol, 0)
             if last_time == 0: return False
             
             now = datetime.now().timestamp()
             cooldown = getattr(self.params, "signal_cooldown", 60)
             return (now - last_time) < cooldown
        except Exception:
             return False

    def _record_signal_time(self, symbol: str) -> None:
        if not hasattr(self, "signal_times"):
             self.signal_times = {}
        self.signal_times[symbol] = datetime.now().timestamp()

    def _execute_direct_option_trade(self, signal: Dict[str, Any]) -> None:
        """执行直接期权交易"""
        try:
            future_id = signal.get("future_id")
            exchange = signal.get("exchange")
            target_opts = signal.get("target_option_contracts", [])
            
            if not target_opts:
                return

            # 计算手数
            try:
                # [Fix] Respect user config, remove hardcoded minimum of 5
                volume_min = int(getattr(self.params, "lots_min", 1) or 1)
                if volume_min < 1: volume_min = 1
                volume_max = int(getattr(self.params, "lots_max", 100) or 100)
            except Exception:
                volume_min = 1
                volume_max = 100
                
            volume = volume_min 
            if volume > volume_max: volume = volume_max
            
            # 拆分手数 (50% per contract for straddle/strangle)
            # If volume is 1, opt_volume becomes 1 (int(0.5) is 0, max(1, 0) is 1)
            # If volume is 2, opt_volume becomes 1
            # If volume is 3, opt_volume becomes 1
            # If volume is 4, opt_volume becomes 2
            # Use ceil logic or simple allocation? 
            # Original logic: opt_volume = max(1, int(volume * 0.5))
            # Let's keep strict splitting but allow small lots.
            if len(target_opts) > 1:
                opt_volume = max(1, int(volume / len(target_opts)))
            else:
                opt_volume = volume
            
            self.output(f"执行直接期权交易：目标合约={target_opts}, 总配置手数={volume}, 单合约分配手数={opt_volume}")
            
            if not hasattr(self, "last_open_success_time"):
                 self.last_open_success_time = {}

            for opt_id in target_opts:
                # 1. 冷却检查 (60s Per Option)
                s_key = f"{exchange}|{opt_id}|0"
                last_succ = self.last_open_success_time.get(s_key)
                if last_succ:
                     time_diff = (datetime.now() - last_succ).total_seconds()
                     if time_diff < 60:
                         self.output(f"跳过期权 {opt_id}：冷却中 ({time_diff:.1f}s < 60s)")
                         continue
                
                # 2. 获取价格 (Enhanced Fallback with Retry)
                price = self._get_execution_price_for_option(exchange, opt_id, "buy")
                
                # [Fix] 如果价格无效，尝试强制重新订阅并等待快照 (Emergency Re-sub)
                if (not price or price <= 0):
                    self.output(f"[执行告警] 合约 {opt_id} 价格无效({price})，尝试强制补订行情并重试...")
                    if hasattr(self, "sub_market_data"):
                        self.sub_market_data(exchange=exchange, instrument_id=opt_id)
                    time.sleep(1.0) # 等待行情推送
                    price = self._get_execution_price_for_option(exchange, opt_id, "buy")
                    if price and price > 0:
                        self.output(f"[执行] 补订后成功获取价格: {opt_id} -> {price}")
                
                # No mock price injection; only trade with real price data.

                if price and price > 0:
                    remark = f"Auto_{future_id}_{signal.get('signal_type')}"
                    inst_info = {"ExchangeID": exchange, "InstrumentID": opt_id}
                    
                    # 3. 发送委托
                    order_id = self.send_order_safe(inst_info, opt_volume, "buy", remark, price=price)
                    
                    if order_id:
                        self.last_open_success_time[s_key] = datetime.now()
                        self.output(f"直接期权委托已发送: {opt_id} 买入 @ {price} ID={order_id}")
                        
                        # 4. 注册追单 (Chase)
                        if hasattr(self, "register_open_chase"):
                             self.register_open_chase(opt_id, exchange, "0", [order_id], opt_volume, price)
                else:
                    self.output(f"[下单异常] 从未获取到合约 {opt_id} 的有效价格(price={price})，跳过下单。请检查订阅状态。")
            
            # 全局冷却记录 (Optional backup)
            self._record_signal_time(future_id)
            
            # [Fix] 清除缓存防止重复计算结果触发
            try:
                if hasattr(self, "option_width_results") and future_id in self.option_width_results:
                     if signal.get("is_call", False):
                          self.option_width_results[future_id]["top_active_calls"] = []
                     else:
                          self.option_width_results[future_id]["top_active_puts"] = []
            except Exception:
                pass

        except Exception as e:
            self.output(f"执行期权交易失败 {signal}: {e}")

    def _get_execution_price_for_option(self, exchange: str, option_id: str, direction: str) -> Optional[float]:
        """获取执行价格（卖一价/买一价/最新价）"""
        try:
             # 尝试从 latest_ticks 获取
             tick = None
             if hasattr(self, "latest_ticks"):
                 # Case-insensitive check
                 tick = self.latest_ticks.get(option_id) or \
                        self.latest_ticks.get(f"{exchange}.{option_id}") or \
                        self.latest_ticks.get(option_id.upper()) or \
                        self.latest_ticks.get(f"{exchange}.{option_id}".upper())
             
             if not tick:
                 if hasattr(self, "get_last_tick"):
                     tick = self.get_last_tick(exchange, option_id)
            
             if tick:
                 if direction == "buy":
                     p = getattr(tick, "ask_price1", 0) or getattr(tick, "AskPrice1", 0) or getattr(tick, "ask", 0)
                     if p > 0: return p
                 else:
                     p = getattr(tick, "bid_price1", 0) or getattr(tick, "BidPrice1", 0) or getattr(tick, "bid", 0)
                     if p > 0: return p
                 
                 last = getattr(tick, "last_price", 0) or getattr(tick, "last", 0) or getattr(tick, "close", 0)
                 if last > 0: return last
             
             # Fallback to Kline Close (Recent Non-Zero)
             klines = self._get_kline_series(exchange, option_id)
             if klines and len(klines) > 0:
                 # Try last close
                 p = getattr(klines[-1], "close", 0)
                 if p > 0: return p
                 
                 # Try searching backwards
                 if hasattr(self, "_get_last_nonzero_close"):
                     return self._get_last_nonzero_close(klines)
                 
             return 0.0
        except Exception:
             return 0.0

    def _is_trading_time(self, instrument_id: str = "") -> bool:
        """判断是否在交易时间段内。"""
        try:
            now_time = datetime.now().time()
            now = datetime.now()

            start_str = str(getattr(self.params, "daily_start_time", "09:00:00"))
            stop_str = str(getattr(self.params, "daily_stop_time", "15:00:00"))

            try:
                start_h, start_m, start_s = map(int, start_str.split(":"))
                start_dt = now.replace(hour=start_h, minute=start_m, second=start_s, microsecond=0)
            except Exception:
                start_dt = now.replace(hour=9, minute=0, second=0, microsecond=0)

            try:
                stop_h, stop_m, stop_s = map(int, stop_str.split(":"))
                stop_dt = now.replace(hour=stop_h, minute=stop_m, second=stop_s, microsecond=0)
            except Exception:
                stop_dt = now.replace(hour=15, minute=0, second=0, microsecond=0)

            if stop_dt < start_dt:
                if now >= start_dt or now <= stop_dt:
                    return True
                else:
                    return False
            else:
                if start_dt <= now <= stop_dt:
                    return True
                else:
                    return False

        except Exception as e:
            # self.output(f"交易时间判断出错: {e}")
            return True

    def _is_cooling_down(self, symbol: str) -> bool:
        """检查信号是否在冷却期"""
        try:
             # 需要在 State 中维护 signal_times
             if not hasattr(self, "signal_times"):
                 self.signal_times = {} # symbol -> timestamp
             
             last_time = self.signal_times.get(symbol, 0)
             if last_time == 0: return False
             
             now = datetime.now().timestamp()
             cooldown = getattr(self.params, "signal_cooldown", 60)
             return (now - last_time) < cooldown
        except Exception:
             return False

    def _record_signal_time(self, symbol: str) -> None:
        if not hasattr(self, "signal_times"):
             self.signal_times = {}
        self.signal_times[symbol] = datetime.now().timestamp()


    def _find_atm_options(self, future_symbol: str, options: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """查找平值期权(ATM)。根据标的期货当前价格，寻找行权价最接近的Call和Put"""
        try:
            future_info = None
            for f in self.future_instruments:
                if f.get("InstrumentID") == future_symbol:
                    future_info = f
                    break
            if not future_info:
                return None, None

            exch = future_info.get("ExchangeID", "")
            klines = self._get_kline_series(exch, future_symbol)
            current_price = 0.0

            if klines and len(klines) > 0:
                last_k = klines[-1]
                current_price = getattr(last_k, "close", 0.0)

            if current_price <= 0:
                tick = self.get_last_tick(exch, future_symbol)
                if tick:
                    current_price = getattr(tick, "last_price", 0.0)

            if current_price <= 0:
                self.output(f"无法获取 {future_symbol} 当前价格，无法确定 ATM")
                return None, None

            min_diff = float("inf")
            atm_call = None
            atm_put = None

            grouped_by_strike = {}
            for opt in options:
                try:
                    strike = float(opt.get("StrikePrice", 0))
                    if strike <= 0:
                        continue
                    if strike not in grouped_by_strike:
                        grouped_by_strike[strike] = {"C": None, "P": None}

                    opt_type = str(opt.get("OptionsType", "")).upper()
                    if opt_type == "1" or opt_type == "C" or "CALL" in opt_type:
                        grouped_by_strike[strike]["C"] = opt
                    elif opt_type == "2" or opt_type == "P" or "PUT" in opt_type:
                        grouped_by_strike[strike]["P"] = opt
                except Exception:
                    continue

            for strike, pair in grouped_by_strike.items():
                diff = abs(current_price - strike)
                if diff < min_diff:
                    if pair["C"] and pair["P"]:
                        min_diff = diff
                        atm_call = pair["C"]
                        atm_put = pair["P"]

            return atm_call, atm_put

        except Exception as e:
            self.output(f"寻找 ATM 期权出错: {e}")
            return None, None

    def get_last_tick(self, exchange: str, instrument_id: str) -> Optional[Any]:
        """获取最新Tick"""
        try:
            return self.market_center.get_last_tick(exchange, instrument_id)
        except Exception:
            return None

    def generate_trading_signals_optimized(self) -> List[Dict[str, Any]]:
        """优化版信号生成（排序三原则）"""
        if not hasattr(self, "option_width_results") or not self.option_width_results:
            return []
        
        valid_results = []
        allow_minimal = bool(getattr(self.params, "allow_minimal_signal", False))
        filtered_no_direction_or_width = 0
        debug_samples = [] # type: ignore

        for future_id, result in self.option_width_results.items():
            try:
                has_dir = bool(result.get("has_direction_options", False))
                width = float(result.get("option_width", 0) or 0)
                
                # 采样调试
                if len(debug_samples) < 3:
                     debug_samples.append(f"{future_id}: dir={has_dir} w={width} allow={allow_minimal}")

                if (has_dir and (width > 0 or allow_minimal)):
                    valid_results.append((future_id, result))
                else:
                    filtered_no_direction_or_width += 1
            except Exception as e:
                pass
        
        if not valid_results:
             if hasattr(self, "_debug_throttled"):
                 self._debug_throttled(
                    f"[交易诊断] 无有效信号 | no_dir_or_width={filtered_no_direction_or_width}",
                    category="trade_required",
                    min_interval=180.0,
                 )
             return []
        
        # 划分全部同步和部分同步结果
        all_sync_results = []
        partial_sync_results = []
        for future_id, result in valid_results:
            if result.get("all_sync", False):
                all_sync_results.append((future_id, result))
            else:
                partial_sync_results.append((future_id, result))
        
        # 按期权宽度降序排序
        def sort_key(item: Tuple[str, Dict[str, Any]]) -> float:
            return float(item[1].get("option_width", 0))
            
        all_sync_results.sort(key=sort_key, reverse=True)
        partial_sync_results.sort(key=sort_key, reverse=True)
        
        signals = []
        
        # 原则1：全部同步结果中取期权宽度最大者为最优信号
        if all_sync_results:
            max_all_sync_width = all_sync_results[0][1]["option_width"]
            for future_id, result in all_sync_results:
                if result["option_width"] == max_all_sync_width:
                    signals.append(
                        self._create_signal_dict(future_id, result, "最优信号")
                    )
        
        # 原则2：全部同步结果中较小宽度优于部分同步结果较大宽度
        for future_id, result in all_sync_results[1:]:
             signals.append(
                self._create_signal_dict(future_id, result, "次优信号")
            )
        
        # 原则3：如果没有全部同步结果，或者全部同步结果已经处理完毕
        curr_len = len(all_sync_results)
        if curr_len == 0 or (curr_len == 1 and partial_sync_results):
            if partial_sync_results:
                max_partial_width = partial_sync_results[0][1]["option_width"]
                for future_id, result in partial_sync_results:
                    if result["option_width"] == max_partial_width:
                        signals.append(
                            self._create_signal_dict(future_id, result, "次优信号")
                        )
        
        # 如果没有全部同步结果，只有部分同步结果
        if not all_sync_results and partial_sync_results:
            max_partial_width = partial_sync_results[0][1]["option_width"]
            signals = []  # 重置信号列表
            for future_id, result in partial_sync_results:
                if result["option_width"] == max_partial_width:
                    signals.append(
                        self._create_signal_dict(future_id, result, "部分同步信号")
                    )

        # 全品种统一排序
        priority = {"最优信号": 0, "次优信号": 1, "部分同步信号": 2}
        signals.sort(
            key=lambda s: (
                priority.get(s.get("signal_type"), 3),
                -(s.get("option_width", 0)),
                s.get("future_id", "")
            )
        )

        return signals

    def generate_trading_signals(self) -> List[Dict[str, Any]]:
        """
        生成并返回交易信号列表（旧版逻辑兼容）
        """
        if not hasattr(self, "option_width_results") or not self.option_width_results:
            return []
            
        min_width = int(getattr(self.params, "min_option_width", 1) or 1)
        
        all_sync_results = [] 
        partial_sync_results = []
        
        for future_id, result in self.option_width_results.items():
            if result.get("option_width", 0) < min_width:
                continue
            elif result.get("all_sync", False):
                all_sync_results.append((future_id, result))
            else:
                partial_sync_results.append((future_id, result))
        
        all_sync_results.sort(key=lambda x: x[1].get("option_width", 0), reverse=True)
        partial_sync_results.sort(key=lambda x: x[1].get("option_width", 0), reverse=True)
        
        signals = []
        
        if all_sync_results:
            best_id, best_result = all_sync_results[0]
            signals.append({
                "future_id": best_id,
                "exchange": best_result.get("exchange", ""),
                "signal_type": "最优信号",
                "option_width": best_result.get("option_width"),
                "all_sync": True,
                "action": "买入" if best_result.get("future_rising") else "卖出",
                "price": best_result.get("current_price"),
                "timestamp": best_result.get("timestamp")
            })
            
            if len(all_sync_results) > 1:
                second_best_id, second_best_result = all_sync_results[1]
                signals.append({
                    "future_id": second_best_id,
                    "exchange": best_result.get("exchange", ""),
                    "signal_type": "次优信号",
                    "option_width": second_best_result.get("option_width"),
                    "all_sync": True,
                    "action": "买入" if second_best_result.get("future_rising") else "卖出",
                    "price": second_best_result.get("current_price"),
                    "timestamp": second_best_result.get("timestamp")
                })

        return signals

    def _create_signal_dict(self, future_id: str, result: Dict[str, Any], signal_type: str) -> Dict[str, Any]:
        """创建信号字典"""
        is_call = result.get("future_rising", False)
        action = "买入"
        
        if is_call:
             target_contracts = result.get("top_active_calls", [])
        else:
             target_contracts = result.get("top_active_puts", [])
        
        current = result.get("current_price", 0)
        previous = result.get("previous_price", 0)
        price_change_percent = 0
        if previous > 0:
            price_change_percent = ((current - previous) / previous * 100)
            
        return {
            "future_id": future_id,
            "exchange": result.get("exchange", ""),
            "signal_type": signal_type,
            "option_width": result.get("option_width", 0),
            "all_sync": result.get("all_sync", False),
            "action": action,
            "is_call": is_call,
            "target_option_contracts": target_contracts,
            "is_direct_option_trade": True,
            "price": current,
            "previous_price": previous,
            "price_change_percent": price_change_percent,
            "timestamp": result.get("timestamp", 0),
            "specified_month_count": result.get("specified_month_count", 0),
            "next_specified_month_count": result.get("next_specified_month_count", 0),
            "total_specified_target": result.get("total_specified_target", 0),
            "total_next_specified_target": result.get("total_next_specified_target", 0),
            "total_all_om_specified": result.get("total_all_om_specified", 0),
            "total_all_om_next_specified": result.get("total_all_om_next_specified", 0)
        }

    def get_current_signals(self) -> List[Dict[str, Any]]:
        """获取当前信号状态（主要用于调试/UI展示）"""
        return []

    def print_filtered_readiness(self, symbol_prefix: str, exchanges: Optional[Set[str]] = None, limit: int = 20) -> None:
        """打印过滤后的标的就绪状态 (Debugging Tool)"""
        pass

    def output_trading_signals_optimized(self, signals: List[Dict[str, Any]]) -> None:
        """优化版信号输出"""
        if not signals: return
        try:
             msg_lines = ["[调试] 信号输出:"]
             for s in signals:
                 msg_lines.append(f"  {s.get('signal_type')} {s.get('future_id')} W={s.get('option_width')} {s.get('action')}")
             self.output("\n".join(msg_lines))
        except Exception:
             pass

    def output_trading_signals(self, signals: List[Dict[str, Any]]) -> None:
        """旧版信号输出"""
        self.output_trading_signals_optimized(signals)

    def _check_stop_profit_realtime(self, tick: Any) -> None:
        """实时止盈检查 (Source Alias)"""
        if hasattr(self, "position_manager") and self.position_manager:
            self.position_manager.check_realtime_stop_profit(tick)

    def _output_daily_signal_summary(self) -> None:
        """每日信号统计输出"""
        # Simplified implementation
        if not hasattr(self, "signal_stats"):
             self.output("[日报] 无信号统计数据")
             return
        self.output(f"[日报] 今日信号总数: {len(getattr(self, 'signal_stats', []))}")

    def _daily_summary(self) -> None:
        """每日收盘总结"""
        self._output_daily_signal_summary()
        self._log_status_snapshot(stage="DailySummary")
        self._reset_daily_trades_if_new_day()

    def _reset_daily_trades_if_new_day(self) -> None:
        """重置每日统计数据"""
        now_date = datetime.now().date()
        if not hasattr(self, "_last_reset_date"):
            self._last_reset_date = now_date
            return
            
        if self._last_reset_date != now_date:
            if hasattr(self, "signal_stats"):
                self.signal_stats = [] # Or dict
            if hasattr(self, "_reset_manual_limits_if_new_day"):
                self._reset_manual_limits_if_new_day()
            self._last_reset_date = now_date
            # Source also clears 'traded_today' cache if exists

    def _current_session_half(self) -> str:
        """返回当前时段：Day 或 Night"""
        now = datetime.now()
        # Simple heuristic or use full session logic
        if now.hour >= 20 or now.hour <= 2:
            return "Night"
        return "Day"



