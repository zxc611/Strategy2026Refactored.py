"""订阅管理。"""
from __future__ import annotations

from datetime import datetime
import time
from typing import Optional, Set


class SubscriptionMixin:
    def _get_subscription_interval_seconds(self) -> float:
        try:
            interval = float(getattr(self.params, "subscription_interval", 0.2) or 0.2)
        except Exception:
            interval = 0.2
        return max(0.2, interval)

    def _subscribe_in_batches(self) -> None:
        """使用定时器分批订阅合约，避免在启动时阻塞主线程"""
        try:
            self.output("=== 开始分批订阅合约 ===")
            start_time = datetime.now()
            try:
                self._align_month_mapping_to_loaded_futures()
            except Exception:
                pass

            self.subscription_queue = []

            filter_specified_futures = self._resolve_subscribe_flag(
                "subscribe_only_specified_month_futures",
                "subscribe_only_current_next_futures",
                False,
            )

            fut_included = 0
            fut_skipped = 0
            self.output("[调试] 即将首次调用 calculate_all_option_widths")
            for future in self.future_instruments:
                exchange = future.get("ExchangeID", "")
                instrument_id = future.get("InstrumentID", "")
                
                raw_id = future.get("RawInstrumentID", "") or instrument_id
                
                if not exchange or not instrument_id:
                    continue
                instrument_norm = self._normalize_future_id(instrument_id)
                if not self._is_real_month_contract(instrument_norm):
                    fut_skipped += 1
                    continue
                if filter_specified_futures and (not self._is_symbol_specified_or_next(instrument_norm)):
                    fut_skipped += 1
                    continue

                self.subscription_queue.append({
                    "exchange": exchange,
                    "instrument_id": raw_id,
                    "type": "future",
                })
                fut_included += 1

            seen_opt_keys: Set[str] = set()
            included_options = 0
            skipped_options = 0
            if self.params.subscribe_options:
                try:
                    self._normalize_option_group_keys()
                except Exception:
                    pass
                filter_specified_options = self._resolve_subscribe_flag(
                    "subscribe_only_specified_month_options",
                    "subscribe_only_current_next_options",
                    False,
                )

                allowed_future_symbols: Set[str] = set()
                if filter_specified_options:
                    for fid in list(self.option_instruments.keys()):
                        fid_norm = self._normalize_future_id(fid)
                        if self._is_symbol_specified_or_next(fid_norm.upper()):
                            allowed_future_symbols.add(fid_norm.upper())
                    self._debug(f"启用过滤：仅订阅指定月/指定下月期权，允许的分组键数量{len(allowed_future_symbols)}")

                global_seen_opts: Set[str] = set()

                for future_symbol, options in self.option_instruments.items():
                    future_symbol_norm = self._normalize_future_id(future_symbol)
                    if filter_specified_options and future_symbol_norm.upper() not in allowed_future_symbols:
                        skipped_options += len(options)
                        continue
                    for option in options:
                        opt_exchange = option.get("ExchangeID", "")
                        opt_instrument = option.get("InstrumentID", "")
                        
                        raw_opt_id = option.get("RawInstrumentID", "") or opt_instrument
                        
                        if not opt_exchange or not opt_instrument:
                            continue
                        opt_norm = self._normalize_future_id(str(opt_instrument))
                        if filter_specified_options and (not self._is_symbol_specified_or_next(opt_norm)):
                            opt_underlying = None
                            try:
                                opt_underlying = self._extract_future_symbol(opt_norm)
                            except Exception:
                                opt_underlying = None
                            if not opt_underlying or (not self._is_symbol_specified_or_next(opt_underlying)):
                                skipped_options += 1
                                continue
                        opt_key = f"{opt_exchange}_{opt_instrument}"
                        if opt_key in seen_opt_keys:
                            continue
                        seen_opt_keys.add(opt_key)
                        global_key = f"{opt_exchange}_{opt_norm}"
                        if global_key in global_seen_opts:
                            continue
                        global_seen_opts.add(global_key)
                        self.subscription_queue.append({
                            "exchange": opt_exchange,
                            "instrument_id": raw_opt_id,
                            "type": "option",
                        })
                        included_options += 1

            try:
                if getattr(self.params, "debug_output", False):
                    target_prefixes = ["CU"]
                    fut_list = [
                        f"{f.get('ExchangeID','')}.{f.get('InstrumentID','')}"
                        for f in self.future_instruments
                        if any(str(f.get('InstrumentID','')).upper().startswith(p) for p in target_prefixes)
                    ]
                    opt_list = []
                    for fsym, opts in self.option_instruments.items():
                        if any(fsym.upper().startswith(p) for p in target_prefixes):
                            opt_list.extend([f"{o.get('ExchangeID','')}.{o.get('InstrumentID','')}" for o in opts])
                    self._debug(f"[调试] 目标品种期货数量: {len(fut_list)} 示例: {fut_list[:5]}")
                    self._debug(f"[调试] 目标品种期权数量: {len(opt_list)} 示例: {opt_list[:5]}")
            except Exception:
                pass

            build_ms = (datetime.now() - start_time).total_seconds() * 1000
            self.output(f"订阅队列已构建 {len(self.subscription_queue)} 个合约（期货: {fut_included} 个，期权: {included_options} 个），耗时 {build_ms:.1f}ms")
            if not self.subscription_queue:
                try:
                    self.output(
                        f"[警告] 订阅队列为空 | fut_included={fut_included}, opt_included={included_options}, "
                        f"fut_skipped={fut_skipped}, opt_skipped={skipped_options}",
                        force=True,
                    )
                except Exception:
                    pass
            if self._resolve_subscribe_flag(
                "subscribe_only_specified_month_futures",
                "subscribe_only_current_next_futures",
                False,
            ):
                self._debug(f"期货队列统计: 包含 {fut_included} 条，过滤跳过 {fut_skipped} 条")
            if self.params.subscribe_options:
                self._debug(f"期权队列统计: 包含 {included_options} 条，过滤跳过 {skipped_options} 条")

            # 稳定排序：按交易所+合约代码顺序订阅，保持批次逻辑不变
            try:
                type_order = {"future": 0, "option": 1}
                self.subscription_queue.sort(key=lambda item: (
                    type_order.get(str(item.get("type", "")), 9),
                    str(item.get("exchange", "")),
                    str(item.get("instrument_id", "")),
                ))
            except Exception:
                pass

            self._subscribe_next_batch(0)

            batch_size = self._get_subscription_batch_size()
            next_batch_index = batch_size
            if next_batch_index < len(self.subscription_queue):
                next_job_id = f"subscribe_batch_{next_batch_index // batch_size + 1}"
                batch_seq = next_batch_index // batch_size + 1
                base_interval = self._get_subscription_interval_seconds()
                dynamic_delay = max(0.2, base_interval * (1 + self.subscription_backoff_factor * (batch_seq - 1)))
                self._safe_add_once_job(
                    job_id=next_job_id,
                    func=lambda: self._subscribe_next_batch(next_batch_index, next_job_id),
                    delay_seconds=dynamic_delay,
                )
                self.output(f"已安排下一批订阅，批次 {next_batch_index // batch_size + 1}")

            self.output(f"=== 分批订阅完成，共订阅 {len(self.subscription_queue)} 个合约 ===")
        except Exception as e:
            self.output(f"分批订阅失败: {e}")

    def _subscribe_next_batch(self, batch_index: int, job_id: Optional[str] = None) -> None:
        """订阅下一批合约"""
        if (not self.my_is_running) or self.my_is_paused or (self.my_trading is False) or getattr(self, "my_destroyed", False):
            self._debug("已暂停/非运行，跳过本批次订阅并延后重试")
            if not hasattr(self, "_subscribe_retry_counts"):
                self._subscribe_retry_counts = {}
            retry_count = int(self._subscribe_retry_counts.get(batch_index, 0) or 0)
            if retry_count >= 2:
                self._debug(f"订阅重试已达上限(2次)，停止重试 batch_index={batch_index}")
                return
            self._subscribe_retry_counts[batch_index] = retry_count + 1

            base_interval = self._get_subscription_interval_seconds()
            backoff_factor = min(1 + retry_count, 2)
            delay = max(0.2, base_interval * backoff_factor)
            try:
                self._safe_add_once_job(
                    job_id=job_id or f"subscribe_retry_{batch_index}",
                    func=lambda: self._subscribe_next_batch(batch_index, job_id),
                    delay_seconds=delay,
                )
            except Exception:
                pass
            return
        self.output(f"=== _subscribe_next_batch 被调用，batch_index={batch_index} ===")
        if batch_index >= len(self.subscription_queue):
            self._debug("所有合约已订阅完成")
            if job_id:
                self._remove_job_silent(job_id)
            return

        batch_size = self._get_subscription_batch_size()
        batch = self.subscription_queue[batch_index:batch_index + batch_size]
        success_cnt = 0
        fail_cnt = 0
        dup_cnt = 0
        success_by_type = {"future": 0, "option": 0}
        fail_by_type = {"future": 0, "option": 0}
        dup_by_type = {"future": 0, "option": 0}
        for item in batch:
            try:
                # 中文注释：再次拦截暂停/未运行状态，防止订阅逻辑被绕过
                if (not self.my_is_running) or self.my_is_paused or (self.my_trading is False) or getattr(self, "my_destroyed", False):
                    try:
                        now_ts = time.time()
                        last_ts = getattr(self, "_subscribe_skip_log_time", 0)
                        if now_ts - last_ts >= 60:
                            self.output(
                                "[诊断] 订阅跳过: paused/未运行/已销毁",
                                force=True,
                            )
                            self._subscribe_skip_log_time = now_ts
                    except Exception:
                        pass
                    break
                item_type = item.get("type", "future")
                sub_key = f"{item['exchange']}|{item['instrument_id']}"
                if sub_key in self.subscribed_instruments:
                    dup_cnt += 1
                    dup_by_type[item_type] = dup_by_type.get(item_type, 0) + 1
                    continue
                self.sub_market_data(exchange=item["exchange"], instrument_id=item["instrument_id"])
                
                # [Data Logic Fix] Explicitly request historical data for subscribed instruments
                # User Requirement: "对于指定月\指定下月，应当对它们全部显式订阅或请求历史数据至少5根"
                # This ensures that even before the first real-time tick, we have history for calculation.
                if hasattr(self, "get_recent_m1_kline"):
                    try:
                        fetch_enabled = bool(getattr(self.params, "subscription_fetch_on_subscribe", True))
                    except Exception:
                        fetch_enabled = True
                    if fetch_enabled:
                        fetch_for_options = False
                        try:
                            fetch_for_options = bool(getattr(self.params, "subscription_fetch_for_options", False))
                        except Exception:
                            fetch_for_options = False
                        if item_type == "option" and not fetch_for_options:
                            pass
                        else:
                            try:
                                fetch_count = int(getattr(self.params, "subscription_fetch_count", 5) or 5)
                            except Exception:
                                fetch_count = 5
                            fetch_count = max(1, fetch_count)
                            self.get_recent_m1_kline(
                                exchange=item["exchange"],
                                instrument_id=item["instrument_id"],
                                count=fetch_count,
                            )

                self.subscribed_instruments.add(sub_key)
                success_cnt += 1
                success_by_type[item_type] = success_by_type.get(item_type, 0) + 1
                
                # [Optimization] Log Aggregation for Options to reduce spam
                # Only log detailed INFO for futures instantly
                if item_type != "option":
                    self.output(f"已订阅{item['type']}: {item['exchange']}.{item['instrument_id']}")
                else:
                    # Buffer option logs
                    if not hasattr(self, "_option_sub_buffer"):
                        self._option_sub_buffer = []
                        self._option_sub_buffer_last_time = time.time()
                    self._option_sub_buffer.append(f"{item['exchange']}.{item['instrument_id']}")
                    
                    # Flush if buffer is large or time passed
                    should_flush = False
                    now_ts = time.time()
                    if (now_ts - self._option_sub_buffer_last_time) >= 60:
                        should_flush = True
                    elif len(self._option_sub_buffer) >= 100: # Max buffer size
                        should_flush = True
                        
                    if should_flush and self._option_sub_buffer:
                        count = len(self._option_sub_buffer)
                        sample = self._option_sub_buffer[:3]
                        self.output(f"已订阅期权 {count} 个 (最近60s): {sample} ...")
                        self._option_sub_buffer = []
                        self._option_sub_buffer_last_time = now_ts

            except Exception as e:
                fail_cnt += 1
                fail_by_type[item_type] = fail_by_type.get(item_type, 0) + 1
                self.output(f"订阅失败 {item['exchange']}.{item['instrument_id']}: {e}")

        # [Flush Final Batch] If queue empty (last batch), always flush remaining
        if batch_index + batch_size >= len(self.subscription_queue):
             if hasattr(self, "_option_sub_buffer") and self._option_sub_buffer:
                 count = len(self._option_sub_buffer)
                 sample = self._option_sub_buffer[:3]
                 self.output(f"已订阅期权 {count} 个 (最终汇总): {sample} ...")
                 self._option_sub_buffer = []

        next_batch_index = batch_index + batch_size
        self._debug(
            f"订阅批次完成: 成功 {success_cnt}，失败{fail_cnt}，重复{dup_cnt} | "
            f"success(fut/opt)={success_by_type.get('future',0)}/{success_by_type.get('option',0)} "
            f"fail(fut/opt)={fail_by_type.get('future',0)}/{fail_by_type.get('option',0)} "
            f"dup(fut/opt)={dup_by_type.get('future',0)}/{dup_by_type.get('option',0)}"
        )
        if job_id:
            self._remove_job_silent(job_id)
        if next_batch_index < len(self.subscription_queue):
            next_job_id = f"subscribe_batch_{next_batch_index // batch_size + 1}"
            batch_seq = next_batch_index // batch_size + 1
            base_interval = self._get_subscription_interval_seconds()
            dynamic_delay = max(0.2, base_interval * (1 + self.subscription_backoff_factor * (batch_seq - 1)))
            self._safe_add_once_job(
                job_id=next_job_id,
                func=lambda: self._subscribe_next_batch(next_batch_index, next_job_id),
                delay_seconds=dynamic_delay,
            )
            self._debug(f"已安排下一批订阅，批次 {next_batch_index // batch_size + 1}")

    def _get_subscription_batch_size(self) -> int:
        """中文注释：统一从 params 读取订阅批次大小，缺失时回退默认值"""
        try:
            val = getattr(self.params, "subscription_batch_size", None)
            if val is None:
                return 10
            return max(1, int(val))
        except Exception:
            return 10

    def _resolve_subscribe_flag(self, primary: str, legacy: str, default: bool = False) -> bool:
        """解析订阅过滤参数优先级"""
        try:
            # Try Param Table Override
            overrides = getattr(self, "_param_override_cache", None)
            if overrides is None and hasattr(self, "_load_param_table"):
                 overrides = self._load_param_table()
            
            params_block = overrides.get("params") if isinstance(overrides, dict) else None
            if isinstance(params_block, dict):
                if primary in params_block:
                    return bool(params_block.get(primary))
                if legacy in params_block:
                    return bool(params_block.get(legacy))
            
            # Try Attributes
            if hasattr(self.params, primary):
                 return bool(getattr(self.params, primary))
            if hasattr(self.params, legacy):
                 return bool(getattr(self.params, legacy))
            
            return default
        except Exception:
            return default

    def subscribe_market_data(self, subscribe_options: bool = True) -> None:
        """订阅期货行情，并可选地订阅对应的期权行情"""
        future_subscribed = 0
        option_subscribed = 0
        option_skipped = 0
        futures_skipped = 0
        
        # Resolve Flags
        filter_specified_futures = self._resolve_subscribe_flag(
                "subscribe_only_specified_month_futures",
                "subscribe_only_current_next_futures",
                False,
            )

        sorted_futures = sorted(
            self.future_instruments,
            key=lambda item: (
                str(item.get("ExchangeID", "")),
                str(item.get("InstrumentID", "")),
            ),
        )
        for future in sorted_futures:
            exchange = future.get("ExchangeID", "")
            instrument_id = future.get("InstrumentID", "")

            if not (exchange and instrument_id):
                futures_skipped += 1
                self._debug("跳过无效期货合约")
                continue
            instrument_norm = self._normalize_future_id(instrument_id)

            # Check if filtered
            if filter_specified_futures:
                # Use _is_symbol_specified_or_next from InstrumentsMixin
                if hasattr(self, "_is_symbol_specified_or_next"):
                    if not self._is_symbol_specified_or_next(instrument_norm.upper()):
                         self._debug(f"跳过非指定月/指定下月期货: {exchange}.{instrument_id}")
                         futures_skipped += 1
                         continue
                # Fallback if method missing? (Should not happen in correct mixin structure)

            try:
                self.sub_market_data(exchange=exchange, instrument_id=instrument_id)
                future_subscribed += 1
            except Exception as e:
                self.output(f"订阅期货失败 {exchange}.{instrument_id}: {e}")

        if subscribe_options:
            seen_opt_keys = set()
            filter_opts = self._resolve_subscribe_flag(
                "subscribe_only_specified_month_options",
                "subscribe_only_current_next_options",
                False,
            )
            
            allowed_future_symbols: Set[str] = set()
            if filter_opts:
                # Pre-calculate allowed underlying symbols to speed up option filtering
                # This assumes we can derive underlying from option list or we check against futures
                # Strategy logic: filtering requires checking the *option's* underlying or termination
                # Usually we check the future symbol map
                for fid in list(self.option_instruments.keys()):
                    fid_norm = self._normalize_future_id(fid)
                    if self._is_symbol_specified_or_next(fid_norm.upper()):
                        allowed_future_symbols.add(fid_norm.upper())
            
            for future_symbol, options in sorted(self.option_instruments.items()):
                future_symbol_norm = self._normalize_future_id(future_symbol)
                
                # If underlying is not allowed, skip all its options
                if filter_opts and future_symbol_norm.upper() not in allowed_future_symbols:
                    option_skipped += len(options)
                    continue
                    
                sorted_options = sorted(
                    options,
                    key=lambda item: (
                        str(item.get("ExchangeID", "")),
                        str(item.get("InstrumentID", "")),
                    ),
                )
                for option in sorted_options:
                    opt_exchange = option.get("ExchangeID", "")
                    opt_instrument = option.get("InstrumentID", "")
                    if not opt_exchange or not opt_instrument:
                        option_skipped += 1
                        continue

                    # Double check exact option symbol if needed, but usually underlying check is sufficient 
                    # unless option varies by month differently (like ETF options)
                    # The Source logic also checks individual option symbol normalization
                    
                    opt_norm = self._normalize_future_id(str(opt_instrument))
                    
                    # NOTE: _is_symbol_specified_or_next checks if the symbol string matches specified months
                    # For options, we usually check the underlying. 
                    # But Source line 269: if filter_opts and (not self._is_symbol_specified_or_next(opt_norm.upper())):
                    # This implies valid option symbols return True for that check.
                    
                    if filter_opts and (not self._is_symbol_specified_or_next(opt_norm.upper())):
                        opt_underlying = None
                        try:
                            opt_underlying = self._extract_future_symbol(opt_norm)
                        except Exception:
                            opt_underlying = None
                        if not opt_underlying or (not self._is_symbol_specified_or_next(opt_underlying)):
                            option_skipped += 1
                            continue

                    opt_key = f"{opt_exchange}_{opt_instrument}"
                    if opt_key in seen_opt_keys:
                        continue
                    seen_opt_keys.add(opt_key)

                    try:
                        self.sub_market_data(exchange=opt_exchange, instrument_id=opt_instrument)
                        option_subscribed += 1
                    except Exception as e:
                        self.output(f"订阅期权失败 {opt_exchange}.{opt_instrument}: {e}")
                        continue

        self._debug(
            f"订阅完成: 期货 {future_subscribed} 个，期货跳过 {futures_skipped} 个，期权 {option_subscribed} 个，期权跳过 {option_skipped} 个"
        )
        self._debug("订阅完成：定时计算任务将在start() 阶段统一启动")

    def unsubscribe_all(self) -> None:
        """取消订阅所有合约"""
        for future in self.future_instruments:
            exchange = future.get("ExchangeID", "")
            instrument_id = future.get("InstrumentID", "")

            if exchange and instrument_id:
                try:
                    self.unsub_market_data(exchange=exchange, instrument_id=instrument_id)
                except Exception as e:
                    self.output(f"取消订阅期货失败 {exchange}.{instrument_id}: {e}")

        for _, options in self.option_instruments.items():
            for option in options:
                opt_exchange = option.get("ExchangeID", "")
                opt_instrument = option.get("InstrumentID", "")
                if opt_exchange and opt_instrument:
                    try:
                        self.unsub_market_data(exchange=opt_exchange, instrument_id=opt_instrument)
                    except Exception as e:
                        self.output(f"取消订阅期权失败 {opt_exchange}.{opt_instrument}: {e}")

    def sub_market_data(self, exchange: Optional[str] = None, instrument_id: Optional[str] = None, *args, **kwargs) -> None:
        try:
            if (not getattr(self, "is_running", False)) or getattr(self, "is_paused", False) or (getattr(self, "trading", True) is False) or getattr(self, "destroyed", False):
                self._debug(f"已暂停/未运行/销毁，忽略订阅 {exchange}.{instrument_id}")
                return
            try:
                if exchange is None or instrument_id is None:
                    super().sub_market_data(*args, **kwargs)
                else:
                    super().sub_market_data(exchange=exchange, instrument_id=instrument_id)
            except Exception:
                if exchange is None or instrument_id is None:
                    super().sub_market_data(*args, **kwargs)
                else:
                    super().sub_market_data(exchange, instrument_id)
        except Exception as e:
            self.output(f"订阅失败 {exchange}.{instrument_id}: {e}")

    def unsub_market_data(self, exchange: Optional[str] = None, instrument_id: Optional[str] = None, *args, **kwargs) -> None:
        try:
            target = getattr(super(), "unsub_market_data", None)
            if not callable(target):
                if getattr(self.params, "debug_output", False):
                    self._debug("父类缺少 unsub_market_data，跳过退订")
                return
            try:
                if exchange is None or instrument_id is None:
                    target(*args, **kwargs)
                else:
                    target(exchange=exchange, instrument_id=instrument_id)
            except Exception:
                if exchange is None or instrument_id is None:
                    target(*args, **kwargs)
                else:
                    target(exchange, instrument_id)
        except Exception as e:
            self.output(f"退订失败{exchange}.{instrument_id}: {e}")

