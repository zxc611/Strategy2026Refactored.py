"""订阅管理。"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Set


class SubscriptionMixin:
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
                    "instrument_id": instrument_id,
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
                        if not opt_exchange or not opt_instrument:
                            continue
                        opt_norm = self._normalize_future_id(str(opt_instrument))
                        if filter_specified_options and (not self._is_symbol_specified_or_next(opt_norm)):
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
                            "instrument_id": opt_instrument,
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
            if self._resolve_subscribe_flag(
                "subscribe_only_specified_month_futures",
                "subscribe_only_current_next_futures",
                False,
            ):
                self._debug(f"期货队列统计: 包含 {fut_included} 条，过滤跳过 {fut_skipped} 条")
            if self.params.subscribe_options:
                self._debug(f"期权队列统计: 包含 {included_options} 条，过滤跳过 {skipped_options} 条")

            self._subscribe_next_batch(0)

            next_batch_index = self.subscription_batch_size
            if next_batch_index < len(self.subscription_queue):
                next_job_id = f"subscribe_batch_{next_batch_index // self.subscription_batch_size + 1}"
                batch_seq = next_batch_index // self.subscription_batch_size + 1
                dynamic_delay = int(max(1, self.subscription_interval * (1 + self.subscription_backoff_factor * (batch_seq - 1))))
                self._safe_add_once_job(
                    job_id=next_job_id,
                    func=lambda: self._subscribe_next_batch(next_batch_index, next_job_id),
                    delay_seconds=dynamic_delay,
                )
                self.output(f"已安排下一批订阅，批次 {next_batch_index // self.subscription_batch_size + 1}")

            self.output(f"=== 分批订阅完成，共订阅 {len(self.subscription_queue)} 个合约 ===")
        except Exception as e:
            self.output(f"分批订阅失败: {e}")

    def _subscribe_next_batch(self, batch_index: int, job_id: Optional[str] = None) -> None:
        """订阅下一批合约"""
        if (not self.my_is_running) or self.my_is_paused or (self.my_trading is False) or getattr(self, "my_destroyed", False):
            if job_id:
                self._remove_job_silent(job_id)
            self._debug("已暂停/非运行，跳过本批次订阅并停止级联安排")
            return
        self.output(f"=== _subscribe_next_batch 被调用，batch_index={batch_index} ===")
        if batch_index >= len(self.subscription_queue):
            self._debug("所有合约已订阅完成")
            if job_id:
                self._remove_job_silent(job_id)
            return

        batch = self.subscription_queue[batch_index:batch_index + self.subscription_batch_size]
        success_cnt = 0
        fail_cnt = 0
        dup_cnt = 0
        for item in batch:
            try:
                sub_key = f"{item['exchange']}|{item['instrument_id']}"
                if sub_key in self.subscribed_instruments:
                    dup_cnt += 1
                    continue
                self.sub_market_data(exchange=item["exchange"], instrument_id=item["instrument_id"])
                
                # [Data Logic Fix] Explicitly request historical data for subscribed instruments
                # User Requirement: "对于指定月\指定下月，应当对它们全部显式订阅或请求历史数据至少5根"
                # This ensures that even before the first real-time tick, we have history for calculation.
                if hasattr(self, "get_recent_m1_kline"):
                     self.get_recent_m1_kline(item["exchange"], item["instrument_id"], count=5)

                self.subscribed_instruments.add(sub_key)
                success_cnt += 1
                self.output(f"已订阅{item['type']}: {item['exchange']}.{item['instrument_id']}")
            except Exception as e:
                fail_cnt += 1
                self.output(f"订阅失败 {item['exchange']}.{item['instrument_id']}: {e}")

        next_batch_index = batch_index + self.subscription_batch_size
        self._debug(f"订阅批次完成: 成功 {success_cnt}，失败{fail_cnt}，重复{dup_cnt}")
        if job_id:
            self._remove_job_silent(job_id)
        if next_batch_index < len(self.subscription_queue):
            next_job_id = f"subscribe_batch_{next_batch_index // self.subscription_batch_size + 1}"
            batch_seq = next_batch_index // self.subscription_batch_size + 1
            dynamic_delay = int(max(1, self.subscription_interval * (1 + self.subscription_backoff_factor * (batch_seq - 1))))
            self._safe_add_once_job(
                job_id=next_job_id,
                func=lambda: self._subscribe_next_batch(next_batch_index, next_job_id),
                delay_seconds=dynamic_delay,
            )
            self._debug(f"已安排下一批订阅，批次 {next_batch_index // self.subscription_batch_size + 1}")

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

        for future in self.future_instruments:
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
            
            for future_symbol, options in self.option_instruments.items():
                future_symbol_norm = self._normalize_future_id(future_symbol)
                
                # If underlying is not allowed, skip all its options
                if filter_opts and future_symbol_norm.upper() not in allowed_future_symbols:
                    option_skipped += len(options)
                    continue
                    
                for option in options:
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

