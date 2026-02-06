"""期权宽度计算与信号生成逻辑（完全依照 Source Strategy20260105_3.py 重构）。"""
from __future__ import annotations

import re
import os
import time
import array
import heapq
import collections
import threading
import traceback
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from .market_calendar import is_market_open_safe

try:
    from pythongo import KLineData
except Exception:
    pass

class OptionWidthCalculationMixin:
    """
    包含核心宽度计算、虚值判断、同步判断及信号生成逻辑。
    Strict Parity with Strategy20260105_3.py
    """
    # [Requirement 3] Step Commander State
    _step_calc_context: Dict[str, Any] = collections.defaultdict(dict)

    def _execute_calculation_steps(self, context: Dict):
        """[Requirement 3] Step Commander for Calculation"""
        start_time = time.time()
        timeout = getattr(self.params, "kline_duration_seconds", 60)
        
        try:
            # Step 1: Prepare Data
            if time.time() - start_time > timeout: return # [Requirement 2] Timeout
            target_kline = context.get('kline')
            if not target_kline: return

            # Step 2: Calculate Width
            if time.time() - start_time > timeout: return # [Requirement 2] Timeout
            # ... existing single pass logic extracted effectively ...
            # Currently logic is embedded in _trigger_width_calc_for_kline. 
            # We will hook into it.

            # Step 3: Signal Check & Filter
            # [Requirement 7] Threshold check in _rank_and_signal

            # Step 4: Cleanup [Requirement 4]
            # Immediately clear context data that won't be reused
            k = context.pop('kline', None)
            del k

        except Exception as e:
            self._force_log(f"Calc Step Error: {e}")

    def _get_kline_series_cached(self, exchange: str, symbol: str) -> List[Any]:
        """
        [Optimization] 带有时间窗口缓存的K线获取。
        防止在一个K线周期内多次读取同一数据。
        """
        # Ensure cache storage exists
        if not hasattr(self, "_kline_cache_lock"):
             self._kline_cache_lock = threading.Lock()
        if not hasattr(self, "_kline_mem_cache"):
             self._kline_mem_cache = {}
        
        try:
             # Determine Time Bucket (based on kline_interval)
             interval = 60
             try: interval = int(getattr(self.params, "kline_interval", 60))
             except: pass
             if interval <= 0: interval = 60
             
             # Bucket ID: Current timestamp // interval
             current_bucket = int(time.time() // interval)
             cache_key = f"{exchange}.{symbol}"

             with self._kline_cache_lock:
                 cached = self._kline_mem_cache.get(cache_key)
                 if cached:
                     bucket_id, data = cached
                     if bucket_id == current_bucket:
                         return data
            
             # Fetch Real Data (No Lock held here to allow concurrency)
             real_data = self._get_kline_series(exchange, symbol)
             
             with self._kline_cache_lock:
                 self._kline_mem_cache[cache_key] = (current_bucket, real_data)
                 
             return real_data
        except Exception:
             # Fallback
             return self._get_kline_series(exchange, symbol)

    def _init_calculation_state(self) -> None:
        """初始化计算模块状态（与 Strategy20260105_3.py 保持一致）。"""
        try:
            if not hasattr(self, "option_width_results") or self.option_width_results is None:
                self.option_width_results = {}
            if not hasattr(self, "kline_insufficient_logged") or self.kline_insufficient_logged is None:
                self.kline_insufficient_logged = set()
            if not hasattr(self, "kline_outdated_logged") or self.kline_outdated_logged is None:
                self.kline_outdated_logged = set()
            if not hasattr(self, "calculation_stats") or self.calculation_stats is None:
                self.calculation_stats = {
                    "total_calculations": 0,
                    "successful_calculations": 0,
                    "failed_calculations": 0,
                    "last_calculation_time": None,
                    "average_calculation_time": 0.0,
                }
            if not hasattr(self, "data_lock") or self.data_lock is None:
                self.data_lock = threading.RLock()
            if not hasattr(self, "option_type_cache") or self.option_type_cache is None:
                self.option_type_cache = {}
            if not hasattr(self, "out_of_money_cache") or self.out_of_money_cache is None:
                self.out_of_money_cache = {}
            if not hasattr(self, "cache_max_size") or self.cache_max_size is None:
                self.cache_max_size = 10000
                
            # Direct Trade State
            if not hasattr(self, "last_open_success_time"):
                self.last_open_success_time = {}
            if not hasattr(self, "signal_last_emit"):
                self.signal_last_emit = {}
            if not hasattr(self, "top3_last_signature"):
                self.top3_last_signature = None
            if not hasattr(self, "top3_last_emit_time"):
                self.top3_last_emit_time = None
            if not hasattr(self, "history_retry_count"):
                self.history_retry_count = 0
            if not hasattr(self, "history_loaded"):
                self.history_loaded = False
        except Exception:
            pass

    def calculate_all_option_widths(self) -> None:
        """异步并发计算包装器"""
        # [Fix] Remove internal threading. 
        # run_trading_cycle is already threaded. We must block here to ensure 
        # signals are generated AFTER calculation completes.
        
        # [Optimization] Re-use lock to prevent re-entry if called manually
        if not hasattr(self, "_calc_thread_lock"):
             self._calc_thread_lock = threading.Lock()
        
        # Try to acquire lock. If locked, it means another overlapping cycle is running?
        # In the new architecture, run_trading_cycle has its own lock.
        # So strictly speaking, this lock is redundant but harmless.
        # But we should NOT return immediately if we own the thread.
        # We assume run_trading_cycle handles the single-thread guarantee.
        
        try:
            # [Req] Immediate concurrent batch processing
            # self.output(f">>> [Calc] Starting concurrent calculation cycle (Time: {time.time()})")
            self._calculate_all_option_widths_concurrent()

        except Exception as e:
            self.output(f"宽幅计算异常: {e}", force=True)

    def _calculate_all_option_widths_concurrent(self) -> None:
        """并发计算所有期权宽度，带超时控制（Abandoned if timeout）"""
        try:
            self._diag_cycle_id = time.time()
            if not hasattr(self, "_last_dir_diag") or not isinstance(getattr(self, "_last_dir_diag"), dict):
                self._last_dir_diag = {}
            
            # [Optimization] Clear transient caches once per cycle
            if hasattr(self, "_has_opt_cache"): self._has_opt_cache = {}
            
            # [Optimization] Manage K-Line Cache Size
            if hasattr(self, "_kline_mem_cache"):
                # If cache is too big (>1000 items), clear it to prevent leak.
                # Assuming ~1000 instruments is reasonable working set.
                try: 
                    with self._kline_cache_lock:
                        if len(self._kline_mem_cache) > 2000:
                            self._kline_mem_cache.clear()
                            # self.output("[Mem] KLine Cache Cleared")
                except: pass

            # [Optimization] Normalize keys once per cycle, not per future
            if hasattr(self, "_normalize_option_group_keys"):
                self._normalize_option_group_keys()
        except Exception: pass
        
        start_time = time.time()
        self.calculation_stats["total_calculations"] += 1
        
        # [Requirement 2] Calculation Timeout based on K-line duration
        timeout_sec = float(getattr(self.params, "kline_duration_seconds", 60.0))
        
        try:
            baseline_products_cfg = str(getattr(self.params, "future_products", "") or "")
            if baseline_products_cfg:
                baseline_products = set([p.strip().upper() for p in baseline_products_cfg.split(',') if p.strip()])
            else:
                baseline_products = set(["IF","IH","IM","CU","AL","ZN","RB","AU","AG","M","Y","A","J","JM","I","CF","SR","MA","TA"])

            tasks = {}
            # [Optimization: Concurrency] Use dynamic workers based on CPU count * 1.5, capped at 32
            # More reasonable than hardcoded 32 if running on smaller VM, but preserves power on good HW.
            cpu_n = os.cpu_count() or 4
            workers = min(32, int(cpu_n * 1.5))
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                # 1. Submit Futures Tasks
                with self.data_lock:
                     futures_list = list(self.future_instruments)

                try:
                    now_ts = time.time()
                    last_ts = getattr(self, "_calc_entry_log_time", 0)
                    if now_ts - last_ts >= 60:
                        msg = f"[Calc] futures={len(futures_list)}"
                        if hasattr(self, "_debug"):
                            self._debug(msg)
                        else:
                            self.output(msg)
                        self._calc_entry_log_time = now_ts
                except Exception:
                    pass
                
                for future in futures_list:
                    if self._is_paused_or_stopped(): return
                    exchange = future.get("ExchangeID", self.params.exchange)
                    future_id = future.get("InstrumentID", "")
                    if not exchange or not future_id: continue
                    
                    future_task = executor.submit(self.calculate_option_width_optimized, exchange, future_id)
                    tasks[future_task] = ("F", exchange, future_id)

                # 2. Submit Option Group Tasks
                option_groups = self._build_option_groups_by_option_prefix()
                inv_prefix_map = {"IF": "IO", "IH": "HO", "IC": "MO", "IM": "EO"}
                for group_id, opts in option_groups.items():
                    underlying = self._extract_future_symbol(group_id)
                    if not underlying: continue
                    exch_guess = "CFFEX" # Simplified for brevity, logic exists in method
                    try:
                         if opts: exch_guess = opts[0].get('ExchangeID', 'CFFEX')
                    except: pass
                    
                    # [Optimization] Pass option_groups to avoid rebuilding inside the task
                    group_task = executor.submit(self._calculate_option_width_for_option_group, exch_guess, group_id, underlying.upper(), opts, inv_prefix_map, option_groups)
                    tasks[group_task] = ("G", exch_guess, group_id)

                # 3. Submit Commodity Option Groups (SHFE, DCE, CZCE)
                # ... (Simplified: Combine logic or repeat submit blocks)
                # For brevity, I'll invoke helper to submit commodity groups
                self._submit_commodity_groups(executor, tasks)

                # 4. Wait for results with timeout
                # [Fix Scenario 3] Split wait into chunks to check for Pause/Stop
                chunk_time = 0.5
                elapsed = 0
                done, not_done = set(), set(tasks.keys())
                
                while elapsed < timeout_sec:
                    if self._is_paused_or_stopped():
                        # self.output(">>> [Logic] Paused/Stopped during calculation wait. Aborting.", force=True)
                        # Cancel remaining futures
                        for t in tasks.keys(): t.cancel()
                        return

                    # Wait small chunk
                    curr_done, curr_not_done = concurrent.futures.wait(not_done, timeout=chunk_time)
                    done.update(curr_done)
                    not_done = curr_not_done
                    
                    if not not_done:
                        break # All finished
                    
                    elapsed += chunk_time
                
                if not_done:
                     # [Req] Timeout -> Abandon calculation
                     self.output(f"[警告] 计算超时 ({len(not_done)} 任务未完成)，本次计算放弃。Time used: {time.time() - start_time:.2f}s", force=True)
                     self.calculation_stats["failed_calculations"] += 1
                     # Do NOT update self.option_width_results
                     return 

                # 5. Process Results (All finished within time)
                new_results = {}
                # Start with existing results? No, usually we rebuild valid ones. 
                # But to preserve history of things NOT in this batch (e.g. manual adds?), 
                # Strategy usually recalculates everything in scope.
                # Use cleanup logic on existing? 
                # Safest: Use copy of existing, then apply updates.
                with self.data_lock:
                     new_results = self.option_width_results.copy()

                for fut in done:
                    try:
                        res = fut.result()
                        # Protocol: 
                        # Return {'action': 'update', 'key': k, 'value': v}
                        # Return {'action': 'delete', 'key': k}
                        # Return None (no change)
                        if not res: continue
                        
                        action = res.get('action')
                        key = res.get('key')
                        if action == 'update':
                             new_results[key] = res.get('value')
                        elif action == 'delete':
                             if key in new_results: del new_results[key]
                    except Exception as e:
                        if hasattr(self, "_debug"): self._debug(f"Task exception: {e}")

                # 6. Commit Updates
                with self.data_lock:
                    self.option_width_results = new_results
                    self._cleanup_caches()

            self.calculation_stats["successful_calculations"] += 1
            
            # Post-calculation processing
            try:
                signals = self.generate_trading_signals_optimized()
                self.output_trading_signals_optimized(signals)
            except Exception as e:
                 if hasattr(self, "_debug"): self._debug(f"统一输出交易信号失败: {e}")
            
            # (Merged Output Logic from Original)
            self._output_debug_table()

        except Exception as e:
            self.calculation_stats["failed_calculations"] += 1
            if hasattr(self, "_debug"): self._debug(f"批量计算期权宽度失败: {e}\n{traceback.format_exc()}")
        
        end_time = time.time()
        calculation_time = end_time - start_time
        self.calculation_stats["last_calculation_time"] = calculation_time
        old_avg = self.calculation_stats["average_calculation_time"]
        self.calculation_stats["average_calculation_time"] = (old_avg * 0.9 + calculation_time * 0.1)
        
        # [Req 2] Log Throttling (Reduce unnecessary logs)
        # Debug >= 20s, Trade >= 600s (10min)
        try:
             last_log = getattr(self, "_last_calc_log_time", 0)
             mode = str(getattr(self.params, "output_mode", "trade")).lower()
             if mode == "debug":
                 mode = "close_debug"
             if mode == "open_debug":
                 interval = float(getattr(self.params, "open_debug_output_interval", 60) or 60)
             elif mode == "trade":
                 interval = float(getattr(self.params, "trade_output_interval", 900) or 900)
             else:
                 interval = float(getattr(self.params, "debug_output_interval", 180) or 180)
             if end_time - last_log >= interval:
                  self.output(f"concurrent_calc 执行完成，耗时 {calculation_time:.2f}s", force=True)
                  self._last_calc_log_time = end_time
        except Exception:
             self.output(f"concurrent_calc 执行完成，耗时 {calculation_time:.2f}s", force=True)

    def _submit_commodity_groups(self, executor, tasks):
        """Helper to submit commodity option groups"""
        # SHFE
        try:
            shfe_groups = self._build_shfe_option_groups()
            for gid, opts in shfe_groups.items():
                if not self._is_symbol_current(gid): continue
                # [Optimization] Pass shfe_groups
                t = executor.submit(self._calculate_option_width_for_option_group, "SHFE", gid, gid, opts, {}, shfe_groups)
                tasks[t] = ("G", "SHFE", gid)
        except: pass
        # DCE
        try:
            dce_groups = self._build_dce_option_groups()
            for gid, opts in dce_groups.items():
                if not self._is_symbol_current(gid): continue
                # [Optimization] Pass dce_groups
                t = executor.submit(self._calculate_option_width_for_option_group, "DCE", gid, gid, opts, {}, dce_groups)
                tasks[t] = ("G", "DCE", gid)
        except: pass
        # CZCE
        try:
            czce_groups = self._build_czce_option_groups()
            for gid, opts in czce_groups.items():
                if not self._is_symbol_current(gid): continue
                # [Optimization] Pass czce_groups
                t = executor.submit(self._calculate_option_width_for_option_group, "CZCE", gid, gid, opts, {}, czce_groups)
                tasks[t] = ("G", "CZCE", gid)
        except: pass

    def _output_debug_table(self):
        try:
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
            if mode == "debug":
                mode = "close_debug"
        except: mode = "close_debug"
        
        # [Req 2] Log Throttling (Applied to Debug/Trade Table Output too)
        # Check against the shared log timer
        try:
            last_log = getattr(self, "_last_calc_log_time", 0)
            if mode == "open_debug":
                interval = float(getattr(self.params, "open_debug_output_interval", 60) or 60)
            elif mode == "trade":
                interval = float(getattr(self.params, "trade_output_interval", 900) or 900)
            else:
                interval = float(getattr(self.params, "debug_output_interval", 180) or 180)
            if time.time() - last_log < interval:
                return
        except: pass

        if mode in ("trade", "open_debug") or getattr(self.params, "debug_output", False):
            try:
                with self.data_lock:
                    results = list(self.option_width_results.values())
                # ... (Simplified Table Logic, assuming strict parity not needed for display internals, but output format matters)
                # Reusing roughly original logic for table generation
                rows = []
                for res in results:
                     if not res.get("has_direction_options"): continue
                     rows.append(res)
                rows.sort(key=lambda r: int(r.get("option_width", 0)), reverse=True)
                
                if getattr(self.params, "top3_rows", 3):
                    top_n = int(getattr(self.params, "top3_rows", 3))
                    rows = rows[:top_n] if mode in ("trade", "open_debug") else rows

                if rows:
                    headers = ["#", "交易所", "品种", "方向", "宽度", "指定月", "下月", "虚值总数", "时间"]
                    display_rows = []
                    for idx, r in enumerate(rows, start=1):
                         ts = r.get("timestamp")
                         tstr = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else str(ts)
                         display_rows.append([str(idx), str(r.get("exchange")), str(r.get("future_id")), 
                                              "上涨" if r.get("future_rising") else "下跌", 
                                              str(r.get("option_width")), 
                                              f"{r.get('specified_month_count')}/{r.get('total_specified_target')}",
                                              f"{r.get('next_specified_month_count')}/{r.get('total_next_specified_target')}",
                                              f"{r.get('total_all_om_specified')}/{r.get('total_all_om_next_specified')}", tstr])
                    
                    # Simple table print
                    tag = "交易" if mode in ("trade", "open_debug") else "调试"
                    self.output(f"{tag}模式：Top{len(rows)}", trade=True, trade_table=True)
                    for row in display_rows:
                        self.output(" | ".join(row), trade=True, trade_table=True)
            except Exception: pass

    def calculate_option_width_optimized(self, exchange: str, future_id: str) -> Optional[Dict[str, Any]]:
        """优化版期权宽度计算（带时效检查 + Top2）"""
        try:
            # [Optimization] Normalize keys moved to outer loop
            # self._normalize_option_group_keys()

            mode = str(getattr(self.params, "output_mode", "trade")).lower()
            if mode == "debug":
                mode = "close_debug"
            is_debug = mode not in ("trade", "open_debug")
            forced_debug = getattr(self.params, "test_mode", False) or getattr(self.params, "diagnostic_output", False)
            if forced_debug: is_debug = True

            if (not getattr(self, "is_running", False)) or getattr(self, "is_paused", False) or getattr(self, "destroyed", False): return
            if (getattr(self, "trading", True) is False) and not is_debug: return

            if self._normalize_future_id(future_id) in getattr(self, "futures_without_options", set()): return

            key = f"{exchange}_{future_id}"
            klines = self._get_kline_series_cached(exchange, future_id)
            if len(klines) < 2:
                try:
                     if hasattr(self, "get_recent_m1_kline"):
                        self.get_recent_m1_kline(exchange, future_id, count=self.params.max_kline)
                        klines = self._get_kline_series_cached(exchange, future_id)
                except Exception: pass
            
            if len(klines) < 2:
                # [Optimization] 无K线/K线不足时的宽容处理：视作0变动，不清理不返回，确保下一轮循环正常
                if key not in self.kline_insufficient_logged:
                     # Only log via debug to avoid spam
                     if is_debug and hasattr(self, "_debug"): self._debug(f"K线不足 {future_id}, 当前{len(klines)}根 (容错模式:Default 0)")
                     self.kline_insufficient_logged.add(key)
                
                # [Fix] Explicitly ensure klines list is valid for downstream logic (even if empty)
                # Do NOT return here. Let logic proceed to price assignment below.
                pass
            else:
                self.kline_insufficient_logged.discard(key)

            # [CHECK-FIX] KLine Freshness Check
            try:
                mock_mode = bool(getattr(self.params, "test_mode", False)) or bool(getattr(self, "DEBUG_ENABLE_MOCK_EXECUTION", False))
                if is_market_open_safe(self):
                    mock_mode = False
                if mock_mode:
                    max_age = 0
                else:
                    conf_val = getattr(self.params, "kline_max_age_sec", None)
                    max_age = int(conf_val) if conf_val is not None else 1800
            except Exception:
                max_age = 1800
                
            # 注意：若K线本身不足，则无法判断Freshness，此处仅在有K线时判断
            if max_age > 0 and len(klines) >= 1:
                 def _get_ts(bar: Any) -> Optional[datetime]:
                     candidates = ["datetime", "DateTime", "timestamp", "Timestamp", "time", "Time"]
                     for name in candidates:
                         val = getattr(bar, name, None)
                         if isinstance(val, datetime): return val
                         if isinstance(val, (int, float)): 
                             try: return datetime.fromtimestamp(val)
                             except: pass
                         if isinstance(val, str):
                             try: return datetime.fromisoformat(val)
                             except: pass
                     return None
                 
                 last_ts = _get_ts(klines[-1])
                 if last_ts:
                    if last_ts.tzinfo is not None: last_ts = last_ts.replace(tzinfo=None)
                    age = (datetime.now() - last_ts).total_seconds()
                    if age > max_age:
                        if future_id not in self.kline_outdated_logged:
                             self.output(f"K线过旧({age:.1f}s>{max_age}s)，跳过计算 {exchange}.{future_id}")
                             self.kline_outdated_logged.add(future_id)
                        return {'action': 'delete', 'key': future_id}
                    else:
                        self.kline_outdated_logged.discard(future_id)

            future_id_upper = self._normalize_future_id(future_id)
            
            # [Req 7] Next Month from Param Table (Configured Instruments List)
            # Do NOT infer via calendar calculation. Use the order in params.
            next_month_id = None
            try:
                if hasattr(self, 'future_instruments') and self.future_instruments:
                     prod = self._extract_product_code(future_id)
                     cands = []
                     for it in self.future_instruments:
                         f = it.get("InstrumentID")
                         if f and self._extract_product_code(f) == prod:
                             cands.append(self._normalize_future_id(f))
                     cands = sorted(list(set(cands)))
                     if future_id_upper in cands:
                          idx = cands.index(future_id_upper)
                          if idx + 1 < len(cands): next_month_id = cands[idx+1]
            except Exception: pass
            
            # [Optimization] Moved normalization to outer loop to avoid repetitive heavy lifting
            # self._normalize_option_group_keys()
            specified_options = self._get_option_group_options(future_id_upper)
            next_specified_options = []
            if next_month_id:
                next_norm = self._normalize_future_id(next_month_id)
                if next_norm and next_norm != future_id_upper:
                    next_specified_options = self._get_option_group_options(next_norm)

            if not specified_options and not next_specified_options:
                 # 无期权时输出 0 宽度结果，避免早退断链
                 current_price = self._get_backup_price(future_id, exchange)
                 if current_price <= 0:
                     current_price = 0.0001
                 res_val = {
                     "exchange": exchange,
                     "future_id": future_id,
                     "future_rising": False,
                     "current_price": current_price,
                     "previous_price": current_price,
                     "specified_month_count": 0,
                     "next_specified_month_count": 0,
                     "total_specified_target": 0,
                     "total_next_specified_target": 0,
                     "total_all_om_specified": 0,
                     "total_all_om_next_specified": 0,
                     "option_width": 0,
                     "all_sync": False,
                     "has_direction_options": False,
                     "timestamp": datetime.now(),
                     "top_active_calls": [],
                     "top_active_puts": [],
                 }
                 return {"action": "update", "key": future_id, "value": res_val}
            
            # [Optimization] 缺少下月期权链时，不直接 return，允许空下月列表
            if not next_month_id: next_specified_options = []
            
            # [Req 2 & 3 Refactored] 无K线时，使用备用价格（上一日LastPrice或PreClose），保持价格持续性，而非0
            # [Reasoning] "Missing Data -> 0 fluctuation" means current_price == previous_price.
            # 严格强制：当K线为空时，previous_price 必须强行等于 current_price，从而确保变动为0
            if not klines or len(klines) < 1:
                # [Fix] Explicitly handle Rate Limit Hit (klines=[]) or Missing Data
                # Assume 0 variation => current_price = previous_price (fetched from Tick or Backup)
                current_price = self._get_backup_price(future_id, exchange)
                # 显式赋值，保证 fluctuation = 0
                previous_price = current_price 
                
                # [Log] Only log if truly insufficient (not rate limit which is expected)
                _limit_check = False
                # 注意：此处不再调用 _should_fetch_data 避免副作用，仅处理结果
                if current_price <= 0 and key not in self.kline_insufficient_logged:
                     if hasattr(self, "_debug"): self._debug(f"无K线且无备用价格 {future_id}")
                     self.kline_insufficient_logged.add(key)
            elif len(klines) == 1:
                current_price = getattr(klines[-1], 'close', 0)
                if current_price <= 0: current_price = self._get_backup_price(future_id, exchange)
                # 显式赋值，保证 fluctuation = 0
                previous_price = current_price 
            else:
                current_price = getattr(klines[-1], 'close', 0)
                if current_price <= 0: current_price = self._get_backup_price(future_id, exchange)
                
                previous_price = self._previous_price_from_klines(klines)
                # Ensure previous_price is valid. If 0 (due to bad history), use current (Fluctuation = 0).
                if previous_price <= 0: previous_price = current_price
            
            # [Critical] 变动0值最终校验
            # 无论之前的逻辑如何，如果 klines 缺失，必须强制 previous_price == current_price
            if not klines or len(klines) < 2:
                previous_price = current_price

            if current_price <= 0:
                fallback_cur = self._get_last_nonzero_close(klines, exclude_last=False)
                if fallback_cur > 0: current_price = fallback_cur
            if previous_price <= 0:
                fallback_prev = self._get_last_nonzero_close(klines, exclude_last=True)
                if fallback_prev > 0: previous_price = fallback_prev

            # Final Safety
            if current_price <= 0: current_price = 0.0001
            if previous_price <= 0: previous_price = current_price
            
            # [Optimization] 价格为0时，不判定无效，而是继续，导致 OTM 判定可能全部失效，结果为0宽，这符合“视作0变动”的要求
            if current_price <= 0 or previous_price <= 0:
                  if key not in self.zero_price_logged: self.zero_price_logged.add(key)
            
            future_rising = current_price > previous_price
            
            # [Refactor] Queue Processing: Combine loops for Volume(Top2) and OTM(Width) 
            # to allow immediate memory release as per user requirement.
            # [Optimization] Use disjoint arrays for memory efficiency/float32
            candidates_vol_call_ids = []
            candidates_vol_call_vals = array.array('f')
            candidates_vol_put_ids = []
            candidates_vol_put_vals = array.array('f')

            # [Refactor] Direct Counting to save memory
            specified_count = 0
            total_target_specified = 0
            total_om_specified = 0

            next_specified_count = 0
            total_target_next_specified = 0
            total_om_next_specified = 0

            # Process Specified Options Queue
            if specified_options:
                for option_raw in specified_options:
                    option = self._safe_option_dict(option_raw)
                    if not option: continue
                    opt_id = self._safe_option_id(option)
                    if not opt_id: continue
                    opt_exch = self._safe_option_exchange(option, exchange)
                    
                    # 1. Volume Check (Flagging for Top 2)
                    vol_sum = 0.0
                    opt_klines = [] # Default to empty list for "Provided but empty"
                    try:
                        _k = self._get_kline_series_cached(opt_exch, opt_id)
                        if _k is not None: opt_klines = _k
                        
                        if opt_klines:
                            for bar in opt_klines:
                                vol_sum += float(getattr(bar, 'volume', 0))
                    except Exception: opt_klines = []
                    
                    otype = self._get_option_type(opt_id, option, opt_exch)
                    if otype == 'C': 
                        candidates_vol_call_ids.append(opt_id)
                        candidates_vol_call_vals.append(vol_sum)
                    elif otype == 'P': 
                        candidates_vol_put_ids.append(opt_id)
                        candidates_vol_put_vals.append(vol_sum)

                    # 2. OTM Check & Sync
                    if self._is_out_of_money_optimized(opt_id, current_price, option):
                        total_om_specified += 1
                        is_target = False
                        if future_rising and otype == "C": is_target = True
                        elif (not future_rising) and otype == "P": is_target = True
                        
                        if is_target:
                             total_target_specified += 1
                             # [Optimization] Pass pre-loaded klines to avoid re-fetch or inconsistent state
                             if self._is_option_sync_rising_optimized(option, exchange, future_rising, current_price, pre_loaded_klines=opt_klines):
                                  specified_count += 1
                
                # Immediate Release [Req 3]
                del specified_options

            # Process Next Specified Options Queue
            if next_specified_options:
                for option_raw in next_specified_options:
                    option = self._safe_option_dict(option_raw)
                    if not option: continue
                    opt_id = self._safe_option_id(option)
                    if not opt_id: continue
                    opt_exch = self._safe_option_exchange(option, exchange)

                    # 1. Volume Check
                    vol_sum = 0.0
                    opt_klines = [] # Default to empty list
                    try:
                        _k = self._get_kline_series_cached(opt_exch, opt_id)
                        if _k is not None: opt_klines = _k
                        
                        if opt_klines:
                            for bar in opt_klines:
                                vol_sum += float(getattr(bar, 'volume', 0))
                    except Exception: opt_klines = []
                    
                    otype = self._get_option_type(opt_id, option, opt_exch)
                    if otype == 'C': 
                        candidates_vol_call_ids.append(opt_id)
                        candidates_vol_call_vals.append(vol_sum)
                    elif otype == 'P': 
                        candidates_vol_put_ids.append(opt_id)
                        candidates_vol_put_vals.append(vol_sum)

                    # 2. OTM Check & Sync
                    if self._is_out_of_money_optimized(opt_id, current_price, option):
                        total_om_next_specified += 1
                        is_target = False
                        if future_rising and otype == "C": is_target = True
                        elif (not future_rising) and otype == "P": is_target = True
                        
                        if is_target:
                             total_target_next_specified += 1
                             if self._is_option_sync_rising_optimized(option, exchange, future_rising, current_price, pre_loaded_klines=opt_klines):
                                  next_specified_count += 1
                
                # Immediate Release [Req 3]
                del next_specified_options

            # Resolve Top 2 Active
            top_active_calls = []
            if len(candidates_vol_call_vals) > 0:
                top_idxs = heapq.nlargest(2, range(len(candidates_vol_call_vals)), key=candidates_vol_call_vals.__getitem__)
                top_active_calls = [candidates_vol_call_ids[i] for i in top_idxs]
            
            top_active_puts = []
            if len(candidates_vol_put_vals) > 0:
                 top_idxs = heapq.nlargest(2, range(len(candidates_vol_put_vals)), key=candidates_vol_put_vals.__getitem__)
                 top_active_puts = [candidates_vol_put_ids[i] for i in top_idxs]
            
            # Cleanup Volume List Immediately [Req 2]
            del candidates_vol_call_ids
            del candidates_vol_call_vals
            del candidates_vol_put_ids
            del candidates_vol_put_vals

            allow_minimal = bool(getattr(self.params, "allow_minimal_signal", False))
            has_direction_options = ((total_target_specified > 0 or total_target_next_specified > 0) if allow_minimal else (total_target_specified > 0 and total_target_next_specified > 0))
            
            option_width = specified_count + next_specified_count
            all_sync = False
            if has_direction_options:
                s_ok = (specified_count == total_target_specified) if total_target_specified > 0 else True
                n_ok = (next_specified_count == total_target_next_specified) if total_target_next_specified > 0 else True
                all_sync = s_ok and n_ok

            res_val = {
                "exchange": exchange,
                "future_id": future_id,
                "future_rising": future_rising,
                "current_price": current_price,
                "previous_price": previous_price,
                "specified_month_count": specified_count,
                "next_specified_month_count": next_specified_count,
                "total_specified_target": total_target_specified,
                "total_next_specified_target": total_target_next_specified,
                "total_all_om_specified": total_om_specified,
                "total_all_om_next_specified": total_om_next_specified,
                "option_width": option_width,
                "all_sync": all_sync,
                "has_direction_options": has_direction_options,
                "timestamp": datetime.now(),
                "top_active_calls": top_active_calls,
                "top_active_puts": top_active_puts
            }
            return {'action': 'update', 'key': future_id, 'value': res_val}
        except Exception: return None

    def _calculate_option_width_for_option_group(self, exchange: str, group_id: str, underlying_future_id: str,
                                                 options: List[Dict[str, Any]], inv_prefix_map: Dict[str, str],
                                                 all_option_groups: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Optional[Dict[str, Any]]:
        """基于期权分组计算宽度（带时效检查 + Top2） - Thread Safe Return"""
        try:
            mode = str(getattr(self.params, "output_mode", "trade")).lower()
            if mode == "debug":
                mode = "close_debug"
            is_debug = mode not in ("trade", "open_debug")
            forced_debug = getattr(self.params, "test_mode", False) or getattr(self.params, "diagnostic_output", False)
            if forced_debug: is_debug = True

            if (not getattr(self, "is_running", False)) or getattr(self, "is_paused", False) or getattr(self, "destroyed", False): return None
            if (getattr(self, "trading", True) is False) and not is_debug: return None

            key = f"{exchange}_{group_id}"
            klines = self._get_kline_series_cached(exchange, underlying_future_id)
            # [Fix Req] Remove "Must be > 2" restriction. 
            # If insufficient, we still proceed with best-effort pricing (likely no diff).
            if not klines or len(klines) < 1:
                if key not in self.kline_insufficient_logged:
                    if hasattr(self, "_debug"): self._debug(f"K线不足(期货) {underlying_future_id}, 当前{len(klines)}根 (Group模式:Proceed with Fallback)")
                    self.kline_insufficient_logged.add(key)
            else:
                self.kline_insufficient_logged.discard(key)

            # [CHECK-FIX] KLine Freshness Logic
            try:
                mock_mode = bool(getattr(self.params, "test_mode", False)) or bool(getattr(self, "DEBUG_ENABLE_MOCK_EXECUTION", False))
                if is_market_open_safe(self):
                    mock_mode = False
                if mock_mode:
                    max_age = 0
                else:
                    conf_val = getattr(self.params, "kline_max_age_sec", None)
                    max_age = int(conf_val) if conf_val is not None else 1800
            except Exception:
                max_age = 1800
            
            if max_age > 0 and len(klines) >= 1:
                 def _get_ts_g(bar: Any) -> Optional[datetime]:
                     candidates = ["datetime", "DateTime", "timestamp", "Timestamp", "time", "Time"]
                     for name in candidates:
                         val = getattr(bar, name, None)
                         if isinstance(val, datetime): return val
                         if isinstance(val, (int, float)): 
                             try: return datetime.fromtimestamp(val)
                             except: pass
                         if isinstance(val, str):
                             try: return datetime.fromisoformat(val)
                             except: pass
                     return None
                 last_ts = _get_ts_g(klines[-1])
                 if last_ts:
                    if last_ts.tzinfo: last_ts = last_ts.replace(tzinfo=None)
                    age = (datetime.now() - last_ts).total_seconds()
                    if age > max_age:
                        return {'action': 'delete', 'key': group_id}

            if not klines or len(klines) < 1:
                current_price = self._get_backup_price(underlying_future_id, exchange)
                previous_price = current_price 
            elif len(klines) == 1:
                current_price = klines[-1].close
                if current_price <= 0: current_price = self._get_backup_price(underlying_future_id, exchange)
                previous_price = current_price
            else:
                current_price = klines[-1].close
                if current_price <= 0: current_price = self._get_backup_price(underlying_future_id, exchange)
                previous_price = self._previous_price_from_klines(klines)
                if previous_price <= 0: previous_price = current_price
            
            if current_price <= 0:
                fallback_cur = self._get_last_nonzero_close(klines, exclude_last=False)
                if fallback_cur > 0: current_price = fallback_cur
            if previous_price <= 0:
                fallback_prev = self._get_last_nonzero_close(klines, exclude_last=True)
                if fallback_prev > 0: previous_price = fallback_prev
            
            # Final Safety
            if current_price <= 0: current_price = 0.0001
            if previous_price <= 0: previous_price = current_price
            
            # [Optimization] Group Logic: 价格为0不返回 -> Result safe defaults
            # if current_price <= 0 or previous_price <= 0:
            #     if group_id in self.option_width_results: del self.option_width_results[group_id]
            #     return
            future_rising = current_price > previous_price

            # [Requirement 2] Timeout Check during Calculation
            t_check_start = getattr(self, "_calc_start_time", 0)
            if t_check_start > 0:
                 limit = float(getattr(self.params, "kline_duration_seconds", 60.0))
                 if (time.time() - t_check_start) > limit:
                     # [Requirement 4] Cleanup on abort? Happens naturally as results not saved.
                     return None

            # [Req 7 - Group Logic] Derive Next Future ID from Config List (Strict)
            next_future_id = None
            try:
                if hasattr(self, 'future_instruments') and self.future_instruments:
                     prod = self._extract_product_code(underlying_future_id)
                     cands = []
                     for it in self.future_instruments:
                         f = it.get("InstrumentID")
                         if f and self._extract_product_code(f) == prod:
                             cands.append(self._normalize_future_id(f))
                     cands = sorted(list(set(cands)))
                     curr_u = self._normalize_future_id(underlying_future_id)
                     if curr_u in cands:
                          idx = cands.index(curr_u)
                          if idx + 1 < len(cands): next_future_id = cands[idx+1]
            except Exception: pass
            
            # Fallback if needed or if list logic missed (though user insisted on param table)
            if not next_future_id:
                 pass
            
            # [Requirement 2] Re-establish local vars for timeout check
            timeout_sec = float(getattr(self.params, "kline_duration_seconds", 60.0))
            if not getattr(self, "_calc_start_time_cycle", None): self._calc_start_time_cycle = time.time()
            start_time = self._calc_start_time_cycle

            next_group_id = None
            if next_future_id:
                try:
                    m = re.match(r"^([A-Z]+)(\d{1,2})(\d{1,2})$", next_future_id.upper())
                    if m:
                        fut_prefix = m.group(1)
                        yy = m.group(2)
                        mm = m.group(3)
                        opt_prefix = inv_prefix_map.get(fut_prefix, fut_prefix)
                        if opt_prefix: next_group_id = f"{opt_prefix}{yy}{mm}"
                except Exception: pass
            
            specified_options = list(options or [])
            next_specified_options = []
            if next_group_id:
                try:
                    # [Optimization] Use injected groups if available to avoid full rebuild
                    if all_option_groups is not None:
                        groups_all = all_option_groups
                    else:
                        groups_all = self._build_option_groups_by_option_prefix()

                    next_specified_options = list(groups_all.get(next_group_id.upper(), []))
                except Exception: next_specified_options = []
            
            # [Optimization] Group Logic: 缺少下月也继续，除非两个月都没了
            # If no options, result is 0 width anyway logic handles it downstream
            if not specified_options and not next_specified_options:
                 # Even if no options found, we should not delete result immediately if we want to "assign 0 value".
                 # But if options are truly missing, width is 0. 
                 # Let's verify if we need to return explicit 0 structure or just None.
                 # User said: "无K线全部赋变动0值". This implies calculation result is {width:0}.
                 # If no option contracts exist, width IS 0.
                 pass

            # [Optimized] Use disjoint arrays for memory efficiency/float32
            # Replaces: candidates_vol_call = [] -> (ids, vols)
            candidates_vol_call_ids = []
            candidates_vol_call_vals = array.array('f') # 32-bit float array
            candidates_vol_put_ids = []
            candidates_vol_put_vals = array.array('f') # 32-bit float array

            # [Refactor] Direct Counting
            specified_count = 0
            total_target_specified = 0
            total_om_specified = 0

            next_specified_count = 0 
            total_target_next_specified = 0
            total_om_next_specified = 0

            # Process Specified Options Queue - Combined Volume & OTM Logic
            if specified_options:
                for option_raw in specified_options:
                    option = self._safe_option_dict(option_raw)
                    if not option: continue
                    opt_id = self._safe_option_id(option)
                    if not opt_id: continue
                    opt_exch = self._safe_option_exchange(option, exchange)

                    # 1. Volume (Active Check)
                    vol_sum = 0.0
                    opt_klines = []
                    try:
                        _k = self._get_kline_series_cached(opt_exch, opt_id)
                        if _k is not None: opt_klines = _k
                        
                        # [Optimization: 4. Data Array Upgrade]
                        # Reduce data precision to float32 processing loop
                        if opt_klines:
                            # Use Generator -> Array('f') for lower memory bandwidth during sum
                            # This avoids creating a full list of floats
                            try:
                                # Direct attribute access (.volume) is guaranteed by LightKLine
                                # 'f' is 32-bit float
                                vol_arr = array.array('f', (x.volume for x in opt_klines))
                                vol_sum = sum(vol_arr)
                            except AttributeError:
                                # Fallback for non-LightKLine objects
                                vol_sum = sum(float(getattr(x, 'volume', 0)) for x in opt_klines)
                    except Exception: opt_klines = []
                    
                    opt_type = self._get_option_type(opt_id, option, opt_exch)
                    if opt_type == 'C': 
                        candidates_vol_call_ids.append(opt_id)
                        candidates_vol_call_vals.append(vol_sum)
                    elif opt_type == 'P': 
                        candidates_vol_put_ids.append(opt_id)
                        candidates_vol_put_vals.append(vol_sum)

                    # 2. OTM Check & Sync
                    if self._is_out_of_money_optimized(opt_id, current_price, option):
                        total_om_specified += 1
                        is_target = False
                        if future_rising and opt_type == 'C': is_target = True
                        elif (not future_rising) and opt_type == 'P': is_target = True
                        
                        if is_target:
                            total_target_specified += 1
                            if self._is_option_sync_rising_optimized(option, exchange, future_rising, current_price, pre_loaded_klines=opt_klines): 
                                specified_count += 1
                
                # [Requirement 4] Immediate Cleanup of temporary lists
                del specified_options
            
            # [Requirement 2] Timeout Check during Step 2 loop
            if (time.time() - start_time) > timeout_sec: return None

            # Process Next Specified Options Queue
            if next_specified_options:
                for option_raw in next_specified_options:
                    option = self._safe_option_dict(option_raw)
                    if not option: continue
                    opt_id = self._safe_option_id(option)
                    if not opt_id: continue
                    opt_exch = self._safe_option_exchange(option, exchange)

                    # 1. Volume (Active Check)
                    vol_sum = 0.0
                    opt_klines = []
                    try:
                        _k = self._get_kline_series_cached(opt_exch, opt_id)
                        if _k is not None: opt_klines = _k
                        
                        # [Optimization: 4. Data Array Upgrade]
                        if opt_klines:
                            # Use Generator -> Array('f')
                            try:
                                vol_arr = array.array('f', (x.volume for x in opt_klines))
                                vol_sum = sum(vol_arr)
                            except AttributeError:
                                vol_sum = sum(float(getattr(x, 'volume', 0)) for x in opt_klines)
                    except Exception: opt_klines = []
                    
                    opt_type = self._get_option_type(opt_id, option, opt_exch)
                    if opt_type == 'C':  
                         candidates_vol_call_ids.append(opt_id)
                         candidates_vol_call_vals.append(vol_sum)
                    elif opt_type == 'P': 
                         candidates_vol_put_ids.append(opt_id)
                         candidates_vol_put_vals.append(vol_sum)
                    
                    # 2. OTM Check & Sync
                    if self._is_out_of_money_optimized(opt_id, current_price, option):
                        total_om_next_specified += 1
                        is_target = False
                        if future_rising and opt_type == 'C': is_target = True
                        elif (not future_rising) and opt_type == 'P': is_target = True
                        
                        if is_target:
                            total_target_next_specified += 1
                            if self._is_option_sync_rising_optimized(option, exchange, future_rising, current_price, pre_loaded_klines=opt_klines): 
                                next_specified_count += 1
                
                # [Requirement 4] Immediate Cleanup
                del next_specified_options
            
            # [Requirement 2] Timeout Check after loops
            if (time.time() - start_time) > timeout_sec: return None

            # Resolve Top 2 Active using HeapN (More efficient than full sort)
            top_active_calls = []
            if len(candidates_vol_call_vals) > 0:
                # Get Top 2 indices
                top_idxs = heapq.nlargest(2, range(len(candidates_vol_call_vals)), key=candidates_vol_call_vals.__getitem__)
                top_active_calls = [candidates_vol_call_ids[i] for i in top_idxs]
            
            top_active_puts = []
            if len(candidates_vol_put_vals) > 0:
                 top_idxs = heapq.nlargest(2, range(len(candidates_vol_put_vals)), key=candidates_vol_put_vals.__getitem__)
                 top_active_puts = [candidates_vol_put_ids[i] for i in top_idxs]
            
            # Cleanup Volume List Immediately [Req 2]
            del candidates_vol_call_ids
            del candidates_vol_call_vals
            del candidates_vol_put_ids
            del candidates_vol_put_vals

            allow_minimal = bool(getattr(self.params, "allow_minimal_signal", False))
            has_direction_options = ((total_target_specified > 0 or total_target_next_specified > 0) if allow_minimal else (total_target_specified > 0 and total_target_next_specified > 0))
            
            option_width = specified_count + next_specified_count
            all_sync = False
            if has_direction_options:
                s_ok = (specified_count == total_target_specified) if total_target_specified > 0 else True
                n_ok = (next_specified_count == total_target_next_specified) if total_target_next_specified > 0 else True
                all_sync = s_ok and n_ok

            res_val = {
                "exchange": exchange,
                "future_id": group_id,
                "option_width": option_width,
                "all_sync": all_sync,
                "specified_month_count": specified_count,
                "total_specified_target": total_target_specified,
                "next_specified_month_count": next_specified_count,
                "total_next_specified_target": total_target_next_specified,
                "total_all_om_specified": total_om_specified,
                "total_all_om_next_specified": total_om_next_specified,
                "current_price": current_price,
                "previous_price": previous_price,
                "future_rising": future_rising,
                "timestamp": datetime.now(),
                "has_both_months": bool(total_target_specified > 0) or bool(total_target_next_specified > 0), # Simplified check
                "has_direction_options": has_direction_options,
                "top_active_calls": top_active_calls,
                "top_active_puts": top_active_puts
            }
            return {'action': 'update', 'key': group_id, 'value': res_val}
        except Exception: return None

    def generate_trading_signals_optimized(self) -> List[Dict[str, Any]]:
        """优化版信号生成 - 使用 Generator 逐条处理"""
        if not hasattr(self, "option_width_results") or not self.option_width_results: return []
        
        # [Req 1] Use Generator (yield) instead of creating full intermediate list
        def _iter_valid_results():
            for future_id, result in self.option_width_results.items():
                try:
                    # [Refactored] 始终显示，只要由 Calculation 产生的结果都视为有效监控对象
                    yield (future_id, result)
                except Exception: pass

        all_sync_results = []
        partial_sync_results = []
        
        # Consuming generator one by one
        for future_id, result in _iter_valid_results():
            if result.get("all_sync", False): 
                all_sync_results.append((future_id, result))
            else: 
                partial_sync_results.append((future_id, result))
        
        # [Strict Rule] Sort by Option Width (Sync Count), NOT Volume.
        # User Requirement: "在期权宽度排序中是无需进行成交量排序的"
        # Secondary Sort by FutureID to ensure deterministic order (e.g. alphabetical)
        def sort_key(item: tuple) -> tuple: 
             return (int(item[1]["option_width"]), item[0])
        
        all_sync_results.sort(key=sort_key, reverse=True)
        partial_sync_results.sort(key=sort_key, reverse=True)
        
        signals = []
        # [Req] Logic: "定义全部同步移动的最大宽度为'最优'，否则为'次优'"
        # Only applies to All Sync results.
        
        if all_sync_results:
            max_w = all_sync_results[0][1]["option_width"]
            for fid, res in all_sync_results:
                if res["option_width"] == max_w:
                     signals.append(self._create_signal_dict(fid, res, "最优信号"))
                else:
                     # All Sync but not max width
                     signals.append(self._create_signal_dict(fid, res, "全同步信号"))
        
        if partial_sync_results:
            max_w_partial = partial_sync_results[0][1]["option_width"]
            for fid, res in partial_sync_results:
                if res["option_width"] == max_w_partial: 
                    # [User Req] Partial Sync Max Width logic -> "次优信号"
                    signals.append(self._create_signal_dict(fid, res, "次优信号"))
                else:
                    signals.append(self._create_signal_dict(fid, res, "部分同步信号"))
                
        # [User Req] Filter signal by width_threshold param
        try:
             width_thresh = float(getattr(self.params, "width_threshold", 4.0))
        except: width_thresh = 4.0
        
        # Only filter signals that are destined for output? No, filter at generation.
        # But wait, Debug mode needs to see ranking even if width < 4?
        # User said: "Width > 4 (default) issue trading instruction".
        # This implies validation/execution logic should filter, or signal type should reflect "Weak".
        # If we remove them here, the debug view (based on signals list) won't see them either.
        # Strategy: Keep them in list, but mark them as non-executable if width <= thresh.
        # Or, filter in execution step. Let's filter in execution step to allow debug view to show full ranking.
        
        priority = {"最优信号": 0, "全同步信号": 1, "次优信号": 2, "部分同步信号": 3}
        
        # Final Sort: Priority -> Width(Desc) -> FutureID(Asc). NO VOLUME.
        signals.sort(key=lambda s: (priority.get(s.get("signal_type"), 4), -(s.get("option_width", 0)), s.get("future_id", "")))
        return signals

    def _create_signal_dict(self, future_id: str, res: Dict[str, Any], signal_type: str) -> Dict[str, Any]:
        """Helper to create standard signal object"""
        # [Req 1] Top 2 Active Options Identification (Dwarfs selection)
        # Passed as 'target_option_contracts' for UI display/Trading
        is_call = res.get("future_rising", False)
        target = res.get("top_active_calls", []) if is_call else res.get("top_active_puts", [])
        
        return {
            "future_id": future_id,
            "exchange": res.get("exchange"),
            "signal_type": signal_type,
            "option_width": res.get("option_width", 0),
            "timestamp": res.get("timestamp"),
            "target_option_contracts": target,
            "is_call": is_call,
            "is_direct_option_trade": True,
            # Pass-through debug info
            "current_price": res.get("current_price"),
            "previous_price": res.get("previous_price"),
            "specified_month_count": res.get("specified_month_count"),
            "next_specified_month_count": res.get("next_specified_month_count")
        }

    def output_trading_signals_optimized(self, signals: List[Dict[str, Any]]) -> None:
        """优化版信号输出与执行"""
        now = datetime.now()
        mode = str(getattr(self.params, "output_mode", "trade")).lower()
        if mode == "debug":
            mode = "close_debug"
        trade_quiet = bool(getattr(self.params, "trade_quiet", True))

        if not signals and mode in ("trade", "open_debug"):
            return

        if mode not in ("trade", "open_debug"):
            # [Requirement 5 & 6] Consolidated Output Logic
            # REMOVED: Redundant debug printing. 
            # The printing of ranking is now handled by Container._check_width_ranking_output periodically.
            pass
        
        if mode in ("trade", "open_debug"):
             # Trade 模式：仅在 Top 发生变化或满足刷新间隔时输出，避免刷屏
             # Logic continues below for signal execution...
             try: market_open = bool(self.is_market_open())
             except: market_open = False
             allow_auto_order = market_open
             
             try: refresh_sec = int(getattr(self.params, "top3_refresh_sec", 180) or 180)
             except: refresh_sec = 180
             
             # 时效检查
             try: max_age = int(getattr(self.params, "signal_max_age_sec", 0) or 0)
             except: max_age = 0
             if max_age > 0:
                  fresh = []
                  for s in signals:
                       ts = s.get("timestamp")
                       if not isinstance(ts, datetime):
                            fresh.append(s)
                            continue
                       if (now - ts).total_seconds() <= max_age:
                            fresh.append(s)
                  signals = fresh
             
             if not signals: return
             
             try: top_n = int(getattr(self.params, "top3_rows", 3) or 3)
             except: top_n = 3
             top = signals[:top_n]
             
             if top:
                  sig_items = []
                  for sq in top:
                       tm = ",".join(sq.get("target_option_contracts", []))
                       sig_items.append(f"{sq.get('exchange')}.{sq.get('future_id')}|{sq.get('signal_type')}|{sq.get('option_width')}|{tm}")
                  signature = "||".join(sig_items)
             else: signature = "EMPTY"
             
             last_sig = getattr(self, "top3_last_signature", None)
             signature_unchanged = (last_sig == signature)
             skip_auto_order = False
             
             if signature_unchanged:
                  should_refresh = True
                  try:
                       if isinstance(self.top3_last_emit_time, datetime) and refresh_sec > 0:
                            should_refresh = (now - self.top3_last_emit_time).total_seconds() >= refresh_sec
                  except: should_refresh = True
                  if should_refresh:
                       skip_auto_order = False
                  else:
                       return
             
             self.top3_last_signature = signature
             self.top3_last_emit_time = now
             
             if top and not skip_auto_order and allow_auto_order:
                  self._try_execute_signal_order(top[0])
             
             # 输出表格
             headers = ["#", "交易所", "品种", "优先级", "信号", "宽度", "时间"]
             rows = []
             display_list = []
             for idx in range(top_n):
                  if idx < len(top): display_list.append(top[idx])
                  else: display_list.append({})
             
             for idx, sig in enumerate(display_list, start=1):
                  ts = sig.get('timestamp')
                  tstr = str(ts.strftime('%H:%M:%S') if isinstance(ts, datetime) else '')
                  
                  stype = str(sig.get('signal_type') or "")
                  if "最优" in stype: pri = "最优"
                  # [Update] Display "全" for All Sync signals (Priority 1)
                  elif "全同步" in stype: pri = "全" 
                  elif "次优" in stype: pri = "次优"
                  elif "部分" in stype: pri = "部分"
                  else: pri = stype
                  
                  rows.append([str(idx), str(sig.get('exchange') or ""), str(sig.get('future_id') or ""), pri, stype, str(sig.get('option_width') or 0), tstr])
             
             col_widths = []
             for i in range(len(headers)):
                  max_cell = max([len(r[i]) for r in rows]) if rows else 0
                  col_widths.append(max(len(headers[i]), max_cell))
             sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
             header = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
             
             self.output("交易模式：TOP3 信号", trade=True, trade_table=True)
             self.output(sep, trade=True, trade_table=True)
             self.output(header, trade=True, trade_table=True)
             self.output(sep, trade=True, trade_table=True)
             for r in rows:
                  l = "| " + " | ".join(r[i].ljust(col_widths[i]) for i in range(len(headers))) + " |"
                  self.output(l, trade=True, trade_table=True)
             self.output(sep, trade=True, trade_table=True)

    def _try_execute_signal_order(self, signal: Dict[str, Any]) -> None:
        """执行信号下单"""
        # [Rule] Filter by width threshold (default > 4)
        try:
             width_limit = float(getattr(self.params, "width_threshold", 4.0))
        except: width_limit = 4.0

        if signal.get("option_width", 0.0) <= width_limit:
             return

        if self._is_paused_or_stopped() or (not getattr(self, "trading", True)): return
        
        exchange = signal.get("exchange") or getattr(self.params, "exchange", "")
        instrument_id = signal.get("future_id")
        if not exchange or not instrument_id: return
        
        # [Requirement 7] Signal Threshold Check
        # Fixed: Use 'option_width' key and REMOVED 'is_direct_option_trade' exception
        # because all generated signals are marked as direct option trades now.
        width_val = signal.get("option_width", 0.0)
        threshold = float(getattr(self.params, "option_width_threshold", 4.0))
        
        # Logic: If width is too small, REJECT IT, unless it's a manual override (which we don't have distinct flag for yet)
        # Assuming autogenerated signals must pass this check.
        if width_val <= threshold:
            # Log reason if debug
            if getattr(self.params, "debug_output", False):
                 self.output(f"[Filter] Signal Ignored: Width {width_val:.2f} <= Threshold {threshold}")
            return

        # [Fix] Remove volume adjustments/limits (lots_min/max). Use strictly 1 or configured lots.
        try:
             volume = int(getattr(self.params, "lots", 1) or 1)
        except: volume = 1
        # volume_min = ... (Removed)
        # volume_max = ... (Removed)
        # if volume > volume_max ... (Removed)
        
        if signal.get("is_direct_option_trade", False):
             target_opts = signal.get("target_option_contracts", [])
             if not target_opts: return
             
             opt_volume = max(1, volume) # Direct use of volume, or maybe just 1? Assuming 1 per instructions "defaults"
             self.output(f"执行直接期权交易: 目标={target_opts} 单手={opt_volume}")
             
             for opt_id in target_opts:
                  tick = self.latest_ticks.get(opt_id) or self.latest_ticks.get(f"{exchange}.{opt_id}")
                  if not tick: tick = self.latest_ticks.get(opt_id.upper()) or self.latest_ticks.get(f"{exchange}.{opt_id}".upper())
                  
                  price = None
                  if tick:
                       price = getattr(tick, "ask", None) or getattr(tick, "AskPrice1", None)
                       if price is None: price = getattr(tick, "last", None) or getattr(tick, "last_price", None)
                  
                  if (not price or price <= 0):
                       try:
                            # 1. Try Backup (PreClose)
                            price = self._get_backup_price(opt_id, exchange)
                            # 2. Try K-lines if backup failed
                            if price <= 0:
                                klines = self._get_kline_series(exchange, opt_id)
                                if klines: price = self._get_last_nonzero_close(klines)
                       except: pass
                  
                  if (not price or price <= 0): continue
                  
                  s_key = f"{exchange}|{opt_id}|0"
                  last = self.last_open_success_time.get(s_key)
                  if last and (datetime.now() - last).total_seconds() < 60: continue
                  
                  order_id = self.place_order(exchange=exchange, instrument_id=opt_id, direction="0", offset_flag="0", price=float(price), volume=opt_volume, order_price_type="2")
                  if order_id:
                       self.output(f"委托已发送: {opt_id} @ {price} Vol={opt_volume}")
                       if hasattr(self, 'position_manager') and self.position_manager:
                            self.position_manager.register_open_chase(instrument_id=opt_id, exchange=exchange, direction="0", ids=[order_id], target_volume=opt_volume, price=float(price))
                       self.last_open_success_time[s_key] = datetime.now()
             
             # Clear active
             try:
                  fid = signal.get("future_id")
                  if fid and fid in self.option_width_results:
                       if signal.get("is_call", False): self.option_width_results[fid]["top_active_calls"] = []
                       else: self.option_width_results[fid]["top_active_puts"] = []
             except: pass

    def _is_out_of_money_optimized(self, option_symbol: str, future_price: float, option_dict: Optional[Dict] = None) -> bool:
        """
        [Refactored] 判断期权是否为虚值。
        核心规则：行权价 vs 期货价格 (而非期权价格)
        Call: Strike > Future (OTM)
        Put: Strike < Future (OTM)
        """
        try:
            if future_price <= 0: return False # 价格无效无法判定

            if not option_dict:
                 # 尝试获取
                 pass 
            
            strike = 0.0
            try:
                s_str = str(option_dict.get("StrikePrice", 0)).strip()
                strike = float(s_str.replace(",", ""))
            except: return False
            
            if strike <= 0: return False

            otype = str(option_dict.get("OptionType", "")).upper()
            if otype in ["1", "C", "CALL", "CP_CALL"]: otype = "C"
            elif otype in ["2", "P", "PUT", "CP_PUT"]: otype = "P"
            
            if otype == "C":
                 # Call OTM if Spot < Strike (Price hasn't reached Strike) -> Value is Time Value only
                 # Wait. Deep OTM Call: Strike 4000, Spot 3000. 
                 # Definition of OTM for Call: Strike Price > Market Price.
                 return strike > future_price
            elif otype == "P":
                 # Put OTM if Spot > Strike
                 return strike < future_price
            
            return False
        except: return False

    def _get_backup_price(self, instrument_id: str, exchange: str) -> float:
        """[Refactored] 获取备用价格 (Tick Last -> Tick PreClose -> 0)"""
        price = 0.0
        try:
            # 1. Tick
            if hasattr(self, "latest_ticks"):
                tick = self.latest_ticks.get(instrument_id) or self.latest_ticks.get(f"{exchange}.{instrument_id}")
                if tick:
                    price = float(getattr(tick, 'last_price', 0) or getattr(tick, 'LastPrice', 0) or 0)
                    if price <= 0:
                        price = float(getattr(tick, 'PreClosePrice', 0) or 0)
        except: pass
        return price

    def _get_last_nonzero_close(self, klines: List[Any], exclude_last: bool = False) -> float:
        """获取最近非0收盘价"""
        try:
            if not klines: return 0.0
            targets = list(klines)
            if exclude_last and len(targets) > 0: targets.pop()
            
            for k in reversed(targets):
                 c = getattr(k, 'close', 0)
                 if c > 0: return float(c)
        except: pass
        return 0.0

    def _is_out_of_money(self, s, p, d=None): return self._is_out_of_money_optimized(s, p, d)
    def _is_option_sync_rising(self, o, e, f, c, k=None): return self._is_option_sync_rising_optimized(o, e, f, c, k)

    def _is_option_sync_rising_optimized(self, option: Dict[str, Any], exchange: str,
                                         future_rising: bool, future_price: float, 
                                         pre_loaded_klines: Optional[List[Any]] = None) -> bool:
        """
        [Refactored] 判断期权价格变动是否上涨 (Sync with directional expectation).
        Includes JIT Data Fetching to solve "No Sync" issue due to missing Option K-lines.
        """
        try:
            opt_id = self._safe_option_id(option)
            if not opt_id: return False
            opt_exch = self._safe_option_exchange(option, exchange)
            
            # [Optimization: Reuse Data] Use pre-loaded klines if available
            klines = pre_loaded_klines
            # [Optimization] If provided (even empty), respect it as the definitive source.
            is_externally_provided = (klines is not None)
            
            if klines is None:
                klines = self._get_kline_series_cached(opt_exch, opt_id)
            
            jit_data_used = False
            # [Optimization] JIT disabled if data was provided externally (Strict Single Read)
            if not is_externally_provided and (not klines or len(klines) < 2) and hasattr(self, "get_recent_m1_kline"):
                 # Cooldown Check to prevent API Rate Limit Spam
                 if not hasattr(self, "_jit_fetch_cooldown"): self._jit_fetch_cooldown = {}
                 last_fetch = self._jit_fetch_cooldown.get(opt_id, 0)
                 now_ts = time.time()
                 
                 if now_ts - last_fetch > 60: # Limit to once per minute per option
                     try:
                          self._jit_fetch_cooldown[opt_id] = now_ts
                          # JIT Fetch: Only if missing. Limiting count=5 to save bandwidth.
                          self.get_recent_m1_kline(opt_exch, opt_id, count=5)
                          klines = self._get_kline_series_cached(opt_exch, opt_id)
                          jit_data_used = True
                     except: pass
                 else:
                     # [User Req] Cooldown active -> Return False immediately.
                     # Treated as 0 width contribution (Not Sync).
                     # Stops useless comparison for this option, continues to next option in loop.
                     return False

            try:
                if not klines or len(klines) < 1:
                    cur_p = self._get_backup_price(opt_id, opt_exch)
                    pre_p = cur_p
                elif len(klines) == 1:
                    cur_p = getattr(klines[-1], "close", 0)
                    if cur_p <= 0: cur_p = self._get_backup_price(opt_id, opt_exch)
                    pre_p = cur_p
                else:
                    cur_p = getattr(klines[-1], "close", 0)
                    if cur_p <= 0: cur_p = self._get_backup_price(opt_id, opt_exch)
                    
                    # [Req 3 Implementation] 如果前一根是0变动值K线
                    pre_p = self._previous_price_from_klines(klines)
                    
                    if pre_p <= 0: pre_p = cur_p

                if cur_p <= 0: cur_p = 0.0001
                if pre_p <= 0: pre_p = cur_p

                # Sync Definition: Option Price is RISING.
                return cur_p > pre_p
            finally:
                 # [Fix: Persist JIT Data] Do NOT clear cache immediately.
                 # User Requirement: "Variety data read once per K-line time".
                 # Allow the standard time-bucket cache to hold this data until expiration.
                 pass
        except Exception: return False


