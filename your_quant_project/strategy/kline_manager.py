"""K线处理与历史加载。"""
from __future__ import annotations

import concurrent.futures
import types
import time
import re
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

try:
    from pythongo import KLineData, TickData, KLineGenerator  # type: ignore
except Exception:
    try:
        from pythongo.utils import KLineData, TickData, KLineGenerator  # type: ignore
    except Exception:
        try:
            from pythongo.classdef import KLineData, TickData, KLineGenerator  # type: ignore
        except Exception:
            class KLineData:
                pass

            class TickData:
                pass

            class KLineGenerator:
                def __init__(self, callback=None, exchange: str = "", instrument_id: str = "", style: str = "M1", *args, **kwargs) -> None:
                    self.callback = callback
                    self.exchange = exchange
                    self.instrument_id = instrument_id
                    self.style = style

                def tick_to_kline(self, tick: Any) -> None:
                    if self.callback:
                        self.callback(tick)

# [Optimization 4] LightKLine using slots for memory efficiency
# Upgraded: Add slots for exchange/instrument_id/style to prevent dict creation when these are set later
class LightKLine:
    __slots__ = ('open', 'high', 'low', 'close', 'volume', 'datetime', 'exchange', 'instrument_id', 'style')
    def __init__(self, open: float, high: float, low: float, close: float, volume: float, datetime: Any):
        self.open = float(open) # Force float (Point 4: Precision control handled by python float which is C double usually, 
                                # but explicit cast ensures no object overhead from non-native types)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)
        self.volume = float(volume)
        self.datetime = datetime
        # Initialize optional slots to avoid attribute errors if accessed before assignment
        self.exchange = ""
        self.instrument_id = ""
        self.style = ""

class KlineManagerMixin:
    # [Requirement 1] Data Fetch Rate Limit
    
    def _wait_for_rate_limit(self) -> None:
        """Atomic rate limiter to prevent API 429 errors during concurrency."""
        if not hasattr(self, "_rate_lock"):
            self._rate_lock = threading.Lock()
            self._last_atomic_fetch = 0.0

        # Dynamic throttling: 0.2s = 5 QPS (Very Strict for stability)
        limit_interval = 0.2
        
        with self._rate_lock:
            now = time.time()
            target_time = self._last_atomic_fetch + limit_interval
            if now < target_time:
                sleep_time = target_time - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self._last_atomic_fetch = time.time()

    def _should_fetch_data(self, instrument_id: str, exchange: Optional[str] = None) -> bool:
        """Check if we can fetch data for this instrument (Rate Limit: 2 times per minute, immediate)."""
        # [Fix] Thread-Safe Atomic Rate Limiter
        if not hasattr(self, "_limit_lock"):
            self._limit_lock = threading.RLock()
            
        with self._limit_lock:
            if not hasattr(self, "_data_fetch_timestamps"):
                self._data_fetch_timestamps = {}
                
            now = time.time()
            try:
                # Fixed 60s as per requirement "per minute"
                duration = 60
            except Exception:
                duration = 60
            
            # [Req] 2 requests per minute, allowing immediate back-to-back
            max_requests = 2
            
            key = f"{exchange}_{instrument_id}" if exchange else instrument_id
            global_key = "__GLOBAL__"
            
            # Retrieve history, handling legacy float format if exists
            history = self._data_fetch_timestamps.get(key, [])
            global_history = self._data_fetch_timestamps.get(global_key, [])
            if isinstance(history, (float, int)):
                history = [history]
            if isinstance(global_history, (float, int)):
                global_history = [global_history]
                
            # [Cleanup] strictly remove expired timestamps ("used up and delete")
            # Keep only timestamps that are NOT older than 'duration'
            valid_history = [t for t in history if (now - t) < duration]
            valid_global_history = [t for t in global_history if (now - t) < duration]
            
            if len(valid_history) < max_requests and len(valid_global_history) < max_requests:
                valid_history.append(now)
                valid_global_history.append(now)
                self._data_fetch_timestamps[key] = valid_history
                self._data_fetch_timestamps[global_key] = valid_global_history
                return True
            else:
                # Update cache to just keep valid ones even if we reject
                # This prevents list from growing indefinitely if we didn't slice above
                self._data_fetch_timestamps[key] = valid_history
                self._data_fetch_timestamps[global_key] = valid_global_history
                # [Log] Warn about rate limit hit (Optional)
                if hasattr(self, "output") and getattr(self, "params", {}).get("debug_output", False):
                    # Don't spam, check last log time for this key
                    limit_key = f"{key}_limit_log"
                    last_log = getattr(self, limit_key, 0)
                    if now - last_log > 10:
                        try:
                            self.output(f"[RateLimit] Fetch blocked for {key}. Count: {len(valid_history)}/{max_requests} in {duration}s")
                        except: pass
                        setattr(self, limit_key, now)
                return False
    
    def _to_light_kline(self, bar: Any) -> Any:
        """将任意bar 转为轻量K线对象（仅包含open/high/low/close/volume 属性）"""
        # [Optimization: Fast Path] Avoid generic lookup for known types
        if isinstance(bar, LightKLine): 
            return bar
            
        # 1. Fast Object attributes (KLineData, Structs)
        try:
            # Assuming standard naming (pythongo KLineData or TickData)
            return LightKLine(bar.open, bar.high, bar.low, bar.close, bar.volume, bar.datetime)
        except AttributeError:
            pass

        # 2. Fast Dict attributes
        if isinstance(bar, dict):
            try:
                # Common lowercase keys
                return LightKLine(bar['open'], bar['high'], bar['low'], bar['close'], bar['volume'], bar['datetime'])
            except KeyError:
                pass

        # 3. Slow Generic Fallback (Legacy Support)
        try:
            def _get(o: Any, names: list, default: float = 0.0) -> float:
                for n in names:
                    try:
                        if isinstance(o, dict) and n in o:
                            v = o[n]
                        else:
                            v = getattr(o, n, None)
                        if v not in (None, ""):
                            try:
                                return float(v)
                            except Exception:
                                continue
                    except Exception:
                        continue
                return float(default)

            o = _get(bar, ["open", "Open", "o"])
            h = _get(bar, ["high", "High", "h"])
            l = _get(bar, ["low", "Low", "l"])
            c = _get(bar, ["close", "Close", "last", "last_price", "LastPrice", "Last", "c", "C", "price", "Price"])
            v = _get(bar, ["volume", "Volume", "vol", "Vol", "v"])

            ts = None
            ts_candidates = ["datetime", "DateTime", "timestamp", "Timestamp", "time", "Time"]
            for name in ts_candidates:
                try:
                    val = bar[name] if isinstance(bar, dict) and name in bar else getattr(bar, name, None)
                except Exception:
                    val = None
                if val is None:
                    continue
                if isinstance(val, datetime):
                    ts = val
                    break
                if isinstance(val, (int, float)):
                    try:
                        ts = datetime.fromtimestamp(val)
                        break
                    except Exception:
                        pass
                if isinstance(val, str):
                    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y%m%d %H:%M:%S"):
                        try:
                            ts = datetime.fromisoformat(val) if fmt is None else datetime.strptime(val, fmt)
                            break
                        except Exception:
                            continue
                    if ts:
                        break

            if ts is None:
                ts = datetime.now()

            if c <= 0:
                if o > 0:
                    c = o
                elif max(h, l) > 0:
                    c = max(h, l)

            return LightKLine(o, h, l, c, v, ts)
        except Exception:
            return LightKLine(0.0, 0.0, 0.0, 0.0, 0.0, datetime.now())

    def _get_tick_price(self, tick: TickData) -> Optional[float]:
        """兼容不同 TickData 字段，优先使用last/price，其次用一档双边均价"""
        try:
            for field in ("last", "last_price", "price", "LastPrice", "Last", "close", "Close"):
                val = getattr(tick, field, None)
                if val not in (None, ""):
                    try:
                        return float(val)
                    except Exception:
                        continue

            bid = getattr(tick, "bid", None) or getattr(tick, "BidPrice1", None)
            ask = getattr(tick, "ask", None) or getattr(tick, "AskPrice1", None)
            try:
                if bid not in (None, "") and ask not in (None, ""):
                    return (float(bid) + float(ask)) / 2
            except Exception:
                pass
        except Exception:
            pass
        return None

    def _on_kline_from_tick(self, kline: Any) -> None:
        """KLineGenerator 回调：将合成的K 线写入缓存并触发计算"""
        try:
            exch = getattr(kline, "exchange", "")
            inst = getattr(kline, "instrument_id", "")
            style = getattr(kline, "style", getattr(self.params, "kline_style", "M1"))
            light_bar = self._to_light_kline(kline)
            light_bar.exchange = exch
            light_bar.instrument_id = inst
            light_bar.style = style
            self._process_kline_data(exch, inst, style, light_bar)
            self._trigger_width_calc_for_kline(light_bar)
        except Exception as e:
            self._debug(f"Tick 合成 K 线处理失败{e}")

    def update_tick_data(self, tick: TickData) -> None:
        """由Tick 合成 K 线，保证没有平台 K 线推送时也能更新"""
        try:
            exchange = getattr(tick, "exchange", getattr(tick, "ExchangeID", ""))
            instrument_id = getattr(tick, "instrument_id", getattr(tick, "InstrumentID", ""))
            if not (exchange and instrument_id):
                return

            style = getattr(self.params, "kline_style", "M1")
            key_variants = (f"{exchange}_{instrument_id}", f"{exchange}|{instrument_id}")

            generator = None
            for key in key_variants:
                entry = self.kline_data.get(key)
                if entry is None:
                    entry = {"generator": None, "data": []}
                    self.kline_data[key] = entry
                if entry.get("generator") is None:
                    try:
                        entry["generator"] = KLineGenerator(
                            callback=self._on_kline_from_tick,
                            exchange=exchange,
                            instrument_id=instrument_id,
                            style=style,
                        )
                    except Exception:
                        entry["generator"] = None
                if entry.get("generator") is not None and generator is None:
                    generator = entry.get("generator")

            used_generator = False
            if generator is not None:
                try:
                    generator.tick_to_kline(tick)
                    used_generator = True
                except Exception:
                    used_generator = False

            if not used_generator:
                price = self._get_tick_price(tick)
                if price is None:
                    return
                bar = types.SimpleNamespace(
                    exchange=exchange,
                    instrument_id=instrument_id,
                    style=style,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=getattr(tick, "volume", getattr(tick, "Volume", 0)),
                )
                self._on_kline_from_tick(bar)
        except Exception as e:
            self._debug(f"update_tick_data 异常: {e}")

    def _process_kline_data(self, exchange: str, instrument_id: str, frequency: str, kline: KLineData) -> None:
        """处理K线数据并落盘缓存，由平台K线推送直接调用"""
        try:
            try:
                close_val = float(getattr(kline, "close", 0) or 0)
            except Exception:
                close_val = 0.0
            if close_val <= 0:
                try:
                    setattr(kline, "close", 0.0)
                except Exception:
                    pass
            else:
                try:
                    setattr(kline, "close", close_val)
                except Exception:
                    pass

            try:
                if not getattr(kline, "datetime", None):
                    setattr(kline, "datetime", datetime.now())
            except Exception:
                pass
            inst_upper = str(instrument_id).upper()
            exch_upper = str(exchange).upper()
            keys = (
                f"{exchange}|{instrument_id}",
                f"{exchange}_{instrument_id}",
                f"{exch_upper}|{inst_upper}",
                f"{exch_upper}_{inst_upper}",
            )
            seen = set()
            for key in keys:
                if key in seen:
                    continue
                seen.add(key)
                if key not in self.kline_data:
                    self.kline_data[key] = {"generator": None, "data": []}
                if frequency not in self.kline_data[key]:
                    self.kline_data[key][frequency] = []

                self.kline_data[key][frequency].append(kline)
                if len(self.kline_data[key][frequency]) > self.params.max_kline:
                    self.kline_data[key][frequency] = self.kline_data[key][frequency][-self.params.max_kline:]

                data_list = self.kline_data[key].get("data")
                if isinstance(data_list, list):
                    data_list.append(kline)
                    if len(data_list) > self.params.max_kline:
                        self.kline_data[key]["data"] = data_list[-self.params.max_kline:]
                else:
                    self.kline_data[key]["data"] = [kline]
        except Exception as e:
            self.output(f"处理K线数据失败 {e}")

    def _get_kline_series(self, exchange: str, instrument_id: str) -> List[KLineData]:
        """统一获取K线列表，兼容 pipe/underscore 键和值结构差异"""
        freq = getattr(self.params, "kline_style", "M1")
        inst_upper = str(instrument_id).upper()
        exch_upper = str(exchange).upper()
        key_variants = [
            f"{exchange}_{instrument_id}",
            f"{exchange}|{instrument_id}",
            f"{exch_upper}_{inst_upper}",
            f"{exch_upper}|{inst_upper}",
        ]

        seen_keys = set()
        key_variants = [k for k in key_variants if not (k in seen_keys or seen_keys.add(k))]

        for key in key_variants:
            series = self.kline_data.get(key)
            if series is None:
                continue

            if isinstance(series, dict):
                data_list = series.get("data")
                if isinstance(data_list, list):
                    return data_list
                freq_list = series.get(freq)
                if isinstance(freq_list, list):
                    return freq_list

            if isinstance(series, list):
                return series

        return []

    def _previous_price_from_klines(self, klines: List[Any]) -> float:
        """根据当前时间和 K 线序列选择上一根 K 线的 close 值。"""
        try:
            if not klines or len(klines) == 0:
                # if getattr(self, "my_state", "") == "running": self._debug(f"[PrevPrice] Empty Klines")
                return 0

            def _get_bar_ts(bar: Any) -> Optional[datetime]:
                for name in ("datetime", "DateTime", "timestamp", "Timestamp", "time", "Time"):
                    try:
                        val = getattr(bar, name, None)
                    except Exception:
                        val = None
                    if val is None:
                        continue
                    if isinstance(val, datetime):
                        return val
                    if isinstance(val, (int, float)):
                        try:
                            return datetime.fromtimestamp(val)
                        except Exception:
                            continue
                    if isinstance(val, str):
                        for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y%m%d %H:%M:%S"):
                            try:
                                return datetime.fromisoformat(val) if fmt is None else datetime.strptime(val, fmt)
                            except Exception:
                                continue
                return None

            now = datetime.now()
            default_exch = getattr(self.params, "exchange", None)
            sessions = self._get_trading_sessions(now, default_exch)
            in_open = any(s <= now <= e for s, e in sessions)

            if in_open:
                prev = klines[-2] if len(klines) >= 2 else None
                if prev is None:
                    return 0
                prev_vol = getattr(prev, "volume", getattr(prev, "Volume", 0)) or 0
                if prev_vol == 0:
                    return 0
                return getattr(prev, "close", 0) or 0

            past_ends = [e for _, e in sessions if e <= now]
            if not past_ends:
                return 0
            prev_end = max(past_ends)

            candidate = None
            search_scope = klines[:-1] if len(klines) > 1 else []
            for b in reversed(search_scope):
                bts = _get_bar_ts(b)
                if bts and bts <= prev_end:
                    candidate = b
                    break
            if candidate is None:
                candidate = klines[-2] if len(klines) >= 2 else None
            if candidate is None:
                return 0
            prev_vol = getattr(candidate, "volume", getattr(candidate, "Volume", 0)) or 0
            if prev_vol == 0:
                return 0
            return getattr(candidate, "close", 0) or 0
        except Exception:
            return 0

    def _get_last_nonzero_close(self, klines: List[Any], exclude_last: bool = False) -> float:
        """获取最近一根非0收盘价（可选择排除最后一根）。"""
        try:
            if not klines:
                return 0
            iterable = klines[:-1] if exclude_last else klines
            for bar in reversed(iterable):
                try:
                    close = getattr(bar, "close", 0) or 0
                except Exception:
                    close = 0
                if close and close > 0:
                    return float(close)
            return 0
        except Exception:
            return 0

    def get_recent_m1_kline(self, exchange: str, instrument_id: str, count: int = 10, style: Optional[str] = None) -> List[KLineData]:
        """获取最近N根K线（优先使用 MarketCenter.get_kline_data 的count 参数）"""
        # [Requirement 1] Enforce Data Fetch Rate Limit (Per Instrument)
        if not self._should_fetch_data(instrument_id, exchange):
            return []
        # 中文注释：market_center 缺失时降级（仅告警一次）
        if not hasattr(self, "_market_center_missing_warned"):
            self._market_center_missing_warned = False
        if not getattr(self, "market_center", None):
            if not self._market_center_missing_warned:
                self.output("[警告] market_center 缺失，get_recent_m1_kline 将仅使用 infini 或返回空", force=True)
                self._market_center_missing_warned = True
            
        # [Validation 2] Enforce Global API Throttling (Per Strategy) to prevent "Request too frequent"
        # Use Thread-Safe Atomic Rate Limiter
        try:
            self._wait_for_rate_limit()
        except Exception:
            pass

        try:
            mc_get_kline = getattr(self.market_center, "get_kline_data", None) if getattr(self, "market_center", None) else None
            bars = []
            if callable(mc_get_kline):
                primary_style = (style or getattr(self.params, "kline_style", "M1") or "M1").strip()
                styles_to_try = [primary_style] + (["M1"] if primary_style != "M1" else [])
                try:
                    limit = int(getattr(self.params, "kline_request_count", 6) or 6)
                except Exception:
                    limit = 6
                count = max(1, min(abs(int(count)), limit))
                
                # [No Retry] Request once. If failed, return empty to prevent 'Too Frequent' errors.
                # Do not retry on failure (especially "Too Frequent" errors).
                for sty in styles_to_try:
                    try:
                        try:
                            bars = mc_get_kline(
                                exchange=exchange,
                                instrument_id=instrument_id,
                                style=sty,
                                count=-(abs(count)),
                            )
                        except TypeError:
                            end_dt = datetime.now()
                            start_dt = end_dt - timedelta(minutes=abs(count))
                            bars = mc_get_kline(
                                exchange=exchange,
                                instrument_id=instrument_id,
                                style=sty,
                                start_time=start_dt,
                                end_time=end_dt,
                            )
                        
                        if bars and len(bars) > 0:
                            break
                    except Exception:
                        # If a specific call fails, do not retry. Move to next style or finish.
                        pass

            result: List[KLineData] = []
            for bar in bars or []:
                lk = self._to_light_kline(bar)
                result.append(lk)
            if len(result) < count:
                missing = count - len(result)
                for _ in range(missing):
                    result.append(self._to_light_kline({"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "datetime": datetime.now()}))
            if len(result) > count:
                result = result[-count:]
            if result:
                for key_fmt in (f"{exchange}_{instrument_id}", f"{exchange}|{instrument_id}"):
                    if key_fmt not in self.kline_data:
                        self.kline_data[key_fmt] = {"generator": None, "data": []}
                    data_list = self.kline_data[key_fmt].get("data", [])
                    if isinstance(data_list, list):
                        self.kline_data[key_fmt]["data"].extend(result)
                        try:
                            limit = int(getattr(self.params, "kline_request_count", 6) or 6)
                        except Exception:
                            limit = 6
                        if len(self.kline_data[key_fmt]["data"]) > limit:
                            self.kline_data[key_fmt]["data"] = self.kline_data[key_fmt]["data"][-limit:]
            return result
        except Exception as e:
            self.output(f"获取最近K线失败 {exchange}.{instrument_id}: {e}")
            return []

    def get_m1_kline_range(self, exchange: str, instrument_id: str, start_time: datetime, end_time: datetime) -> List[KLineData]:
        """按时间区间获取K线，调用 MarketCenter.get_kline_data(start_time, end_time)"""
        # [Requirement 1] Enforce Data Fetch Rate Limit
        if not self._should_fetch_data(instrument_id, exchange):
            return []
            
        try:
            mc_get_kline = getattr(self.market_center, "get_kline_data", None)
            bars = []
            if callable(mc_get_kline):
                primary_style = (getattr(self.params, "kline_style", "M1") or "M1").strip()
                styles_to_try = [primary_style] + (["M1"] if primary_style != "M1" else [])
                for sty in styles_to_try:
                    try:
                        bars = mc_get_kline(
                            exchange=exchange,
                            instrument_id=instrument_id,
                            style=sty,
                            start_time=start_time,
                            end_time=end_time,
                        )
                        if bars and len(bars) > 0:
                            break
                    except Exception as e:
                        self.output(f"时间区间获取失败 {exchange}.{instrument_id}({sty}): {e}")
                        bars = []
            try:
                limit = int(getattr(self.params, "kline_request_count", 6) or 6)
            except Exception:
                limit = 6
            result: List[KLineData] = []
            for bar in bars or []:
                lk = self._to_light_kline(bar)
                result.append(lk)
            if len(result) < limit:
                missing = limit - len(result)
                for _ in range(missing):
                    result.append(self._to_light_kline({"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "datetime": datetime.now()}))
            if len(result) > limit:
                result = result[-limit:]
            if result:
                for key_fmt in (f"{exchange}_{instrument_id}", f"{exchange}|{instrument_id}"):
                    if key_fmt not in self.kline_data:
                        self.kline_data[key_fmt] = {"generator": None, "data": []}
                    data_list = self.kline_data[key_fmt].get("data", [])
                    if isinstance(data_list, list):
                        self.kline_data[key_fmt]["data"].extend(result)
                        if len(self.kline_data[key_fmt]["data"]) > limit:
                            self.kline_data[key_fmt]["data"] = self.kline_data[key_fmt]["data"][-limit:]
            return result
        except Exception as e:
            self.output(f"时间区间K线失败 {exchange}.{instrument_id}: {e}")
            return []

    def _trigger_width_calc_for_kline(self, kline: KLineData) -> None:
        """在收到K线后触发对应期货的宽度计算（统一使用优化版）"""
        try:
            if (not getattr(self, "is_running", False)) or getattr(self, "is_paused", False) or (getattr(self, "trading", True) is False) or getattr(self, "destroyed", False):
                return
            inst_id = getattr(kline, "instrument_id", "")
            exch = getattr(kline, "exchange", "")
            if not inst_id:
                return

            base_id = self._normalize_future_id(inst_id)
            if not self._is_instrument_allowed(base_id, exch):
                return
            if base_id in self.future_symbol_to_exchange:
                fut_exch = self.future_symbol_to_exchange.get(base_id, exch)
                prod = self._extract_product_code(base_id)
                if prod and not self._has_option_for_product(prod):
                    self._debug(f"[调试] K线触发跳过无期权品种: {fut_exch}.{base_id}")
                    self._cleanup_kline_cache_for_symbol(base_id)
                    return
                if not self._is_symbol_current(base_id):
                    self._debug(f"[调试] K线触发跳过非指定月/指定下月商品合约: {fut_exch}.{base_id}")
                    try:
                        if base_id in self.option_width_results:
                            del self.option_width_results[base_id]
                    except Exception:
                        pass
                    return
                self.calculate_option_width_optimized(fut_exch, base_id)
                return

            fut_symbol = self._extract_future_symbol(base_id)
            if fut_symbol:
                fut_exch = self.future_symbol_to_exchange.get(fut_symbol, exch)
                prod = self._extract_product_code(fut_symbol)
                if prod and not self._has_option_for_product(prod):
                    self._debug(f"[调试] K线触发跳过无期权品种: {fut_exch}.{fut_symbol}")
                    self._cleanup_kline_cache_for_symbol(fut_symbol)
                    return
                if not self._is_symbol_current(fut_symbol):
                    self._debug(f"[调试] K线触发跳过非指定月/指定下月商品合约: {fut_exch}.{fut_symbol}")
                    try:
                        if fut_symbol in self.option_width_results:
                            del self.option_width_results[fut_symbol]
                    except Exception:
                        pass
                    return
                self.calculate_option_width_optimized(fut_exch, fut_symbol)
        except Exception as e:
            self._debug(f"触发宽度计算失败 {exch}.{inst_id}: {e}")

    def load_historical_klines(self) -> None:
        """主动获取历史K线数据（解决模拟环境缺少实时K线的问题）"""
        try:
            self._debug("=== 开始获取历史K线数据 ===")

            # 中文注释：market_center 缺失时降级（仅告警一次）
            if not hasattr(self, "_market_center_missing_warned"):
                self._market_center_missing_warned = False
            if not getattr(self, "market_center", None):
                if not self._market_center_missing_warned:
                    self.output("[警告] market_center 缺失，历史K线将仅尝试 infini 或直接跳过", force=True)
                    self._market_center_missing_warned = True

            mc_get_bars = getattr(self.market_center, "get_bars", None) if getattr(self, "market_center", None) else None
            infini_get_bars = getattr(__import__("pythongo").infini, "get_bars", None) if hasattr(__import__("pythongo"), "infini") else None
            mc_get_kline = getattr(self.market_center, "get_kline_data", None) if getattr(self, "market_center", None) else None

            get_bars_fn = mc_get_bars if callable(mc_get_bars) else None
            get_bars_source = "MarketCenter"
            if get_bars_fn is None and callable(infini_get_bars):
                get_bars_fn = infini_get_bars
                get_bars_source = "infini"

            if get_bars_fn is None and callable(mc_get_kline):
                get_bars_source = "MarketCenter.get_kline_data"

            if get_bars_fn is None and not callable(mc_get_kline):
                self.output("历史K线接口不可用：无 get_bars / get_kline_data，请检查行情源或接口支持")

            all_instruments = []
            all_seen: Set[str] = set()

            for future in self.future_instruments:
                exchange = future.get("ExchangeID", "")
                instrument_id = future.get("InstrumentID", "")
                instrument_norm = self._normalize_future_id(instrument_id)
                if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                    if self._resolve_subscribe_flag(
                        "subscribe_only_specified_month_futures",
                        "subscribe_only_current_next_futures",
                        False,
                    ) and (not self._is_symbol_current_or_next(instrument_norm)):
                        continue
                    rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                    key = f"{exchange}_{str(instrument_id).upper()}"
                    if key not in all_seen:
                        all_seen.add(key)
                        all_instruments.append(rec)

            if self.params.subscribe_options and getattr(self.params, "load_history_options", False):
                allow_only_current_next = self._resolve_subscribe_flag(
                    "subscribe_only_specified_month_options",
                    "subscribe_only_current_next_options",
                    False,
                )
                allowed_future_symbols: Set[str] = set()
                if allow_only_current_next:
                    for fid in list(self.option_instruments.keys()):
                        fid_norm = self._normalize_future_id(fid)
                        if self._is_symbol_current_or_next(fid_norm.upper()):
                            allowed_future_symbols.add(fid_norm.upper())

                for future_symbol, options in self.option_instruments.items():
                    future_symbol_norm = self._normalize_future_id(future_symbol)
                    if allow_only_current_next and future_symbol_norm.upper() not in allowed_future_symbols:
                        continue
                    for option in options:
                        opt_exchange = option.get("ExchangeID", "")
                        opt_instrument = option.get("InstrumentID", "")
                        if opt_exchange and opt_instrument:
                            rec = {
                                "exchange": opt_exchange,
                                "instrument_id": opt_instrument,
                                "type": "option",
                                "_normalized_future": future_symbol_norm,
                            }
                            key = f"{opt_exchange}_{str(opt_instrument).upper()}"
                            if key not in all_seen:
                                all_seen.add(key)
                                all_instruments.append(rec)

            fut_count = sum(1 for i in all_instruments if i.get("type") == "future")
            opt_count = sum(1 for i in all_instruments if i.get("type") == "option")
            self._debug(f"需要获取K线数据的合约总数: {len(all_instruments)} (期货 {fut_count}, 期权 {opt_count})，load_history_options={getattr(self.params, 'load_history_options', False)}")

            empty_keys: List[str] = []
            non_empty_keys: List[str] = []
            history_minutes = getattr(self.params, "history_minutes", 240) or 240
            fallback_windows = [history_minutes]
            if history_minutes < 720:
                fallback_windows.append(max(120, history_minutes * 2))
            if 1440 not in fallback_windows:
                fallback_windows.append(1440)
            primary_style = (getattr(self.params, "kline_style", "M1") or "M1").strip()
            styles_to_try = [primary_style] + (["M1"] if primary_style != "M1" else [])

            def _fetch_single_instrument(instrument):
                exchange = instrument["exchange"]
                instrument_id = instrument["instrument_id"]

                # [Fix] Enforce strict rate limit per instrument (1 min rule)
                # Note: _should_fetch_data auto-updates timestamp if True.
                # So logic is: Check (and mark) -> Fetch. Even if fetch fails, it's marked.
                if not self._should_fetch_data(instrument_id, exchange):
                    # Only log debug if needed, to avoid spam
                    # self._debug(f"[KLineSkip] Skipped {exchange}.{instrument_id} due to rate limit")
                    
                    # [Compliance] "Treat as read regardless of result"
                    # The timestamp was updated inside _should_fetch_data at the moment it returned True.
                    # Since we are returning here because it returned False, it means it WAS recently read.
                    return True, f"{exchange}_{instrument_id}"

                # [Fast Fail] Check if strategy is running
                if hasattr(self, "my_is_running") and not self.my_is_running:
                    return False, f"{exchange}_{instrument_id}"

                # [Fix] Enforce global QPS limit via atomic lock
                try:
                    self._wait_for_rate_limit()
                except Exception:
                    pass

                exch_upper = str(exchange).upper()
                inst_upper = str(instrument_id).upper()
                key = f"{exchange}_{instrument_id}"

                id_candidates = [instrument_id, instrument_id.lower()]
                try:
                    if exch_upper == "CZCE":
                        m_long_fut = re.match(r"^([A-Z]+)(\d{2})(\d{2})$", inst_upper)
                        if m_long_fut:
                            yy = int(m_long_fut.group(2))
                            alt = f"{m_long_fut.group(1)}{yy % 10}{m_long_fut.group(3)}"
                            if alt not in id_candidates:
                                id_candidates.append(alt)
                        m_long_opt = re.match(r"^([A-Z]+)(\d{2})(\d{2})([CP])(\d+)$", inst_upper)
                        if m_long_opt:
                            yy = int(m_long_opt.group(2))
                            alt = f"{m_long_opt.group(1)}{yy % 10}{m_long_opt.group(3)}{m_long_opt.group(4)}{m_long_opt.group(5)}"
                            if alt not in id_candidates:
                                id_candidates.append(alt)
                        alt_long = self._expand_czce_year_month(inst_upper)
                        if alt_long and alt_long != inst_upper and alt_long not in id_candidates:
                            id_candidates.append(alt_long)
                except Exception:
                    pass

                try:
                    bars = None
                    if callable(get_bars_fn):
                        for cand_id in id_candidates:
                            for sty in styles_to_try:
                                try:
                                    try:
                                        limit = int(getattr(self.params, "kline_request_count", 6) or 6)
                                    except Exception:
                                        limit = 6
                                    bars = get_bars_fn(exchange=exchange, instrument_id=cand_id, period=sty, count=limit)
                                except TypeError:
                                    pass
                                except Exception:
                                    pass
                                if not bars:
                                    try:
                                        bars = get_bars_fn(instrument_id=cand_id, period=sty, count=-(abs(limit)))
                                    except Exception:
                                        pass
                                if not bars:
                                    try:
                                        bars = get_bars_fn(instrument_id=cand_id, period=sty, count=limit)
                                    except Exception:
                                        pass
                                if bars and len(bars) > 0:
                                    break
                            if bars and len(bars) > 0:
                                break

                    if callable(mc_get_kline) and (not bars or len(bars) == 0):
                        for cand_id in id_candidates:
                            for sty in styles_to_try:
                                # Attempt 1: Keyword args (Standard)
                                try:
                                    try:
                                        limit = int(getattr(self.params, "kline_request_count", 6) or 6)
                                    except Exception:
                                        limit = 6
                                    bars = mc_get_kline(exchange=exchange, instrument_id=cand_id, style=sty, count=-(abs(limit)))
                                    if bars and len(bars) > 0:
                                        self._debug(f"[KLineSuccess] KWArgs Exchange+ID: {cand_id} {sty} = {len(bars)}")
                                        break
                                except Exception as e_kw:
                                    # self._debug(f"[KLineDebug] KWArgs fail: {e_kw}")
                                    pass
                                
                                # Attempt 2: Positional args (inst, period, count) - Common in some APIs
                                if not bars:
                                    try:
                                        bars = mc_get_kline(cand_id, sty, -(abs(limit)))
                                        if bars and len(bars) > 0:
                                            self._debug(f"[KLineSuccess] Positional ID: {cand_id} {sty} = {len(bars)}")
                                            break
                                    except Exception as e_pos:
                                        # self._debug(f"[KLineDebug] Positional fail: {e_pos}")
                                        pass
                                
                                # Attempt 3: KWArgs but no exchange (some APIs don't want exchange)
                                if not bars:
                                    try:
                                        bars = mc_get_kline(instrument_id=cand_id, style=sty, count=-(abs(limit)))
                                        if bars and len(bars) > 0:
                                            self._debug(f"[KLineSuccess] KWArgs ID-Only: {cand_id} {sty} = {len(bars)}")
                                            break
                                    except Exception:
                                        pass

                                for win in fallback_windows:
                                    try:
                                        end_dt = datetime.now()
                                        start_dt = end_dt - timedelta(minutes=win)
                                        bars = mc_get_kline(exchange=exchange, instrument_id=cand_id, style=sty, start_time=start_dt, end_time=end_dt)
                                        if bars and len(bars) > 0:
                                            break
                                    except Exception:
                                        bars = []
                                if bars and len(bars) > 0:
                                    break
                            if bars and len(bars) > 0:
                                break

                    if (not bars or len(bars) == 0):
                        try:
                            recent = self.get_recent_m1_kline(exchange, instrument_id, count=getattr(self.params, "kline_request_count", 6), style=primary_style)
                            if recent:
                                bars = recent
                        except Exception:
                            pass

                    if bars and len(bars) > 0:
                        key_list = [
                            f"{exchange}_{instrument_id}",
                            f"{exchange}|{instrument_id}",
                            f"{exch_upper}_{inst_upper}",
                            f"{exch_upper}|{inst_upper}",
                        ]
                        if id_candidates:
                            for cand in id_candidates:
                                k_std = f"{exchange}_{cand}"
                                if k_std not in key_list:
                                    key_list.append(k_std)
                                k_up = f"{exch_upper}_{str(cand).upper()}"
                                if k_up not in key_list:
                                    key_list.append(k_up)

                        key_list = list(dict.fromkeys(key_list))

                        processed_bars = []
                        for bar in bars:
                            lk = self._to_light_kline(bar)
                            processed_bars.append(lk)
                        try:
                            limit = int(getattr(self.params, "kline_request_count", 6) or 6)
                        except Exception:
                            limit = 6
                        if len(processed_bars) < limit:
                            missing = limit - len(processed_bars)
                            for _ in range(missing):
                                processed_bars.append(self._to_light_kline({"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "datetime": datetime.now()}))
                        if len(processed_bars) > limit:
                            processed_bars = processed_bars[-limit:]

                        if len(processed_bars) > 0:
                            if not hasattr(self, "data_lock") or self.data_lock is None:
                                self.data_lock = threading.RLock()
                            with self.data_lock:
                                for k_item in key_list:
                                    if k_item not in self.kline_data:
                                        self.kline_data[k_item] = {"generator": None, "data": []}
                                    self.kline_data[k_item]["data"] = processed_bars
                                    if k_item.upper() == k_item:
                                        k_lower = k_item.lower()
                                        if k_lower not in self.kline_data:
                                            self.kline_data[k_lower] = {"generator": None, "data": []}
                                        self.kline_data[k_lower]["data"] = processed_bars

                            self._debug(f"获取历史K线成功: {key}, len: {len(processed_bars)}")
                            return True, key
                        return False, key
                    return False, key
                except Exception as e:
                    self._debug(f"获取历史K线出错{key}: {e}")
                    return False, key

            default_workers = 4
            config_workers = getattr(self.params, "history_load_max_workers", default_workers)
            max_workers = min(int(config_workers or default_workers), 8)

            self.output(f"启动并发加载历史K线，线程数={max_workers} (已限制最大8以防系统卡顿)", force=True)

            total_items = len(all_instruments)
            batch_size = 100

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures_map = {}
                pending_items = list(all_instruments)

                while pending_items or futures_map:
                    # [Check Stop Signal]
                    if hasattr(self, "my_is_running") and not self.my_is_running:
                        self.output(">>> Strategy Stopping, Aborting History Load...", force=True)
                        break

                    while len(futures_map) < max_workers * 2 and pending_items:
                        item = pending_items.pop(0)
                        fut = executor.submit(_fetch_single_instrument, item)
                        futures_map[fut] = item
                        # [Optimization] Throttle requests to avoid 3116/429 errors (Flow Control)
                        time.sleep(0.02)

                    if not futures_map:
                        break

                    done, _ = concurrent.futures.wait(futures_map.keys(), return_when=concurrent.futures.FIRST_COMPLETED)

                    for future in done:
                        _ = futures_map.pop(future)
                        try:
                            success, key = future.result()
                            if success:
                                non_empty_keys.append(key)
                            else:
                                empty_keys.append(key)
                        except Exception:
                            pass

                    if max_workers > 1:
                        time.sleep(0.001)

            ready_cnt = len(non_empty_keys)
            empty_cnt = len(empty_keys)
            if ready_cnt or empty_cnt:
                self.output(f"历史K线加载总结：就绪{ready_cnt} 个，空数据{empty_cnt} 个", force=True)
            if empty_keys:
                preview = ", ".join(empty_keys[:10])
                more = "" if len(empty_keys) <= 10 else f" 等共 {len(empty_keys)} 个"
                self.output(f"历史K线为空({get_bars_source}): {preview}{more}", force=True)
                if self.history_retry_count < 2:
                    try:
                        self.history_retry_done = True
                        self.history_retry_count += 1
                        self.params.history_minutes = max(history_minutes, 1440)
                        self._safe_add_once_job(
                            job_id=f"retry_load_history_{self.history_retry_count}",
                            func=self.load_historical_klines,
                            delay_seconds=2,
                        )
                        self.output(f"检测到历史K线为空，第{self.history_retry_count}次重试已安排（窗口>= 1440 分钟）")
                    except Exception:
                        pass
            self.history_loaded = True
            if ready_cnt > 0:
                self.output("=== 历史K线加载完成 ===", force=True)
            else:
                self.output("=== 历史K线加载失败 ===", force=True)
            self._debug("=== 历史K线数据获取完成 ===")
        except Exception as e:
            self._debug(f"获取历史K线数据失败 {e}")
            self.output(f"=== 历史K线加载失败 {e} ===", force=True)

    def print_kline_counts(self, limit: int = 10) -> None:
        """打印部分期货与期权的K线数量，便于快速核验"""
        try:
            try:
                self._normalize_option_group_keys()
            except Exception:
                pass
            self.output("=== K线就绪情况快照 ===")
            fut = []
            for f in self.future_instruments:
                ex = f.get("ExchangeID", "")
                fid = f.get("InstrumentID", "")
                if not ex or not fid:
                    continue
                fid_norm = self._normalize_future_id(fid)
                if self._resolve_subscribe_flag(
                    "subscribe_only_specified_month_futures",
                    "subscribe_only_current_next_futures",
                    False,
                ):
                    if not self._is_symbol_current_or_next(fid_norm):
                        continue
                fut.append((ex, fid))
            fut_ready = sum(1 for exch, fid in fut if len(self._get_kline_series(exch, fid)) >= 2)
            fut_zero = [f"{ex}.{fid}" for ex, fid in fut if len(self._get_kline_series(ex, fid)) == 0][:5]
            self.output(f"期货合计 {len(fut)}个(>=2根) {fut_ready}，缺0根示例 {fut_zero}")

            for exch, fid in fut[:limit]:
                series = self._get_kline_series(exch, fid)
                self.output(f"期货 {exch}.{fid} K线数: {len(series)}")

            keys = []
            seen_keys: Set[str] = set()
            for k in self.option_instruments.keys():
                kn = self._normalize_future_id(k)
                if self._is_symbol_current_or_next(kn.upper()) and kn not in seen_keys:
                    seen_keys.add(kn)
                    keys.append(kn)
            opt_items = []
            opt_seen: Set[str] = set()
            for k in keys:
                for opt in self.option_instruments.get(k, []):
                    ex = opt.get("ExchangeID", "")
                    oid = opt.get("InstrumentID", "")
                    key = f"{ex}_{self._normalize_future_id(oid)}"
                    if key in opt_seen:
                        continue
                    opt_seen.add(key)
                    opt_items.append((ex, oid))
            opt_ready = sum(1 for ex, oid in opt_items if len(self._get_kline_series(ex, oid)) >= 2)
            opt_zero = [f"{ex}.{oid}" for ex, oid in opt_items if len(self._get_kline_series(ex, oid)) == 0][:5]
            self.output(f"期权合计 {len(opt_items)}个(>=2根) {opt_ready}，缺0根示例 {opt_zero}")

            cnt = 0
            for ex, oid in opt_items:
                series = self._get_kline_series(ex, oid)
                self.output(f"期权 {ex}.{oid} K线数: {len(series)}")
                cnt += 1
                if cnt >= limit:
                    break
            self.output("=== K线快照结束 ===")
        except Exception as e:
            self.output(f"打印K线快照失败 {e}")

    def print_commodity_option_readiness(self, limit: int = 10) -> None:
        """打印商品期货指定月/指定下月期权的就绪摘要"""
        try:
            try:
                self._normalize_option_group_keys()
            except Exception:
                pass
            main_ex = {"SHFE", "DCE", "CZCE"}
            self.output("=== 商品期权就绪摘要（指定月/指定下月） ===")
            rows = []
            seen_keys: Set[str] = set()
            seen_opts: Set[str] = set()
            for fut_symbol, options in self.option_instruments.items():
                fut_norm = self._normalize_future_id(str(fut_symbol))
                if fut_norm in seen_keys:
                    continue
                if not self._is_symbol_current_or_next(fut_norm.upper()):
                    continue
                seen_keys.add(fut_norm)
                total = 0
                ready = 0
                zero = 0
                sample_zero = []
                sample_ready = []
                for opt in options:
                    ex = opt.get("ExchangeID", "")
                    oid = opt.get("InstrumentID", "")
                    if not ex or not oid:
                        continue
                    if ex not in main_ex:
                        continue
                    opt_key = f"{ex}_{self._normalize_future_id(oid)}"
                    if opt_key in seen_opts:
                        continue
                    seen_opts.add(opt_key)
                    series = self._get_kline_series(ex, oid)
                    total += 1
                    if len(series) >= 2:
                        ready += 1
                        if len(sample_ready) < 2:
                            sample_ready.append(f"{ex}.{oid}")
                    elif len(series) == 0:
                        zero += 1
                        if len(sample_zero) < 2:
                            sample_zero.append(f"{ex}.{oid}")
                if total > 0:
                    rows.append((fut_symbol.upper(), total, ready, zero, sample_ready, sample_zero))
            if not rows:
                self.output("无可摘要的商品期权（指定月/指定下月），请检查合约加载或过滤条件")
            else:
                rows.sort(key=lambda r: r[2], reverse=True)
                for fut_symbol, total, ready, zero, s_ready, s_zero in rows[:limit]:
                    extra = ""
                    if s_ready:
                        extra += f"，就绪示例 {s_ready}"
                    if s_zero:
                        extra += f"，缺0根示例 {s_zero}"
                    self.output(f"[{fut_symbol}] 期权合计 {total}个(>=2根) {ready}，缺0根 {zero}{extra}")
            self.output("=== 商品期权就绪摘要结束 ===")
        except Exception as e:
            self.output(f"打印商品期权就绪摘要失败: {e}")
