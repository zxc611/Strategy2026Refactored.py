"""交易逻辑与信号处理（依照 Strategy20260105_3.py 重构）。"""
from __future__ import annotations

import threading 
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set

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
        except Exception:
            pass
    
    def on_tick(self, tick: Any) -> None:
        """
        行情处理入口。
        """
        try:
            # 1. 保存行情快照
            # Assume self.latest_ticks exists or create it
            if not hasattr(self, "latest_ticks"):
                self.latest_ticks = {}
            
            # Compatible with different Tick objects
            inst = getattr(tick, "instrument_id", "") or getattr(tick, "InstrumentID", "")
            if inst:
                self.latest_ticks[inst] = tick
                # Also store by exchange.inst? 
                exch = getattr(tick, "exchange", "") or getattr(tick, "ExchangeID", "")
                if exch:
                     self.latest_ticks[f"{exch}.{inst}"] = tick
            
            # 2. 也是K线生成器的驱动入口 (如果需要)
            if hasattr(self, "kline_generator") and self.kline_generator:
                self.kline_generator.tick_to_kline(tick)
            
            # 3. Position Manager Live Checks (Stop Profit etc)
            if hasattr(self, "position_manager") and self.position_manager:
                self.position_manager.on_tick(tick)

            # 4. Note: Trading Cycle is run by Scheduler, not on_tick loop
            
            # 5. Check Daily Closing (14:58/15:58)
            self._check_daily_closing(tick)

        except Exception as e:
            # self.output(f"on_tick logic error: {e}")
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
            if self.is_paused or not self.is_running: return

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
            self._trading_cycle_lock.release()

    def execute_signals_optimized(self, signals: List[Dict[str, Any]]) -> None:
        """执行优化后的信号列表（含时效性检查与签名去重）"""
        if not signals:
             return

        # 1. 开盘时间检查
        if hasattr(self, "is_market_open") and not self.is_market_open():
            # self.output("非开盘时间，忽略交易信号执行")
            return

        # 2. 时效性检查 (Signal Freshness)
        now = datetime.now()
        try:
            max_age = int(getattr(self.params, "signal_max_age_sec", 180) or 180)
        except: max_age = 180
        
        fresh_signals = []
        for sig in signals:
            ts = sig.get("timestamp")
            # Convert str isoformat to datetime if needed
            if isinstance(ts, str):
                try: ts = datetime.fromisoformat(ts)
                except: pass
            
            if not isinstance(ts, datetime):
                # Assume fresh if generated by us just now, but if timestamp missing...
                # Actually our calc sets timestamp as str isoformat usually
                continue
                
            delta = (now - ts).total_seconds()
            if delta <= max_age:
                fresh_signals.append(sig)
            else:
                pass # Expired
        
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
                volume_min = int(getattr(self.params, "lots_min", 1) or 1)
                # [Source Parity] Force at least 5 lots for first order? Not exactly, but source had logic.
                if volume_min < 5: volume_min = 5
                volume_max = int(getattr(self.params, "lots_max", 100) or 100)
            except Exception:
                volume_min = 5
                volume_max = 100
                
            volume = volume_min 
            if volume > volume_max: volume = volume_max
            
            # 拆分手数 (50% per contract)
            opt_volume = max(1, int(volume * 0.5))
            
            self.output(f"执行直接期权交易：目标合约={target_opts}, 单合约手数={opt_volume}")
            
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
                
                # 2. 获取价格 (Enhanced Fallback)
                price = self._get_execution_price_for_option(exchange, opt_id, "buy")
                
                # Mock Price Injection for Debug Mode
                mock_enabled = getattr(self, "DEBUG_ENABLE_MOCK_EXECUTION", False) or str(getattr(self.params, "output_mode", "trade")).lower() == "debug"
                if (not price or price <= 0) and mock_enabled:
                     price = 100.0
                     self.output(f"[调试] 注入模拟价格 {price} 用于 {opt_id}")

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



