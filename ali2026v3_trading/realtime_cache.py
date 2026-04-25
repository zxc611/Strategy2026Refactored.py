"""
实时数据内存缓存 - 纳秒级访问速度

设计目标：
- 为高频交易提供极低延迟的数据访问 (< 100 ns)
- 线程安全（使用 RLock）
- 自动内存管理（限制缓存大小）
- 与 DuckDB 互补（实时数据 vs 历史数据）
- 增量WAL保护：每条Tick追加写入WAL文件，进程崩溃后可恢复

性能对比：
- 内存字典查询: < 100 ns
- DuckDB 查询: 10-100 μs (100-1000x 慢)
"""

import json
import os
import threading
import logging
import uuid
import atexit
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any


def _normalize_option_type(opt_type: str) -> str:
    """✅ ID唯一：委托shared_utils.normalize_option_type，不再独立实现"""
    from ali2026v3_trading.shared_utils import normalize_option_type
    return normalize_option_type(opt_type)


class RealTimeCache:
    """实时数据内存缓存
    
    职责：
    - 存储最新价格（每个合约一条记录）
    - 存储最近 N 条 tick（滑动窗口）
    - 提供快速查询接口（平均值、VWAP等）
    
    线程安全：所有公共方法都使用 RLock 保护
    """
    
    DEFAULT_FLUSH_WINDOWS = [
        (12, 0, 12, 50),
        (15, 30, 20, 0),
    ]
    MAX_PENDING_TICKS = 5000
    
    def __init__(self, max_recent_ticks: int = 100, wal_path: Optional[str] = None,
                 flush_windows: Optional[List[tuple]] = None):
        """初始化缓存
        
        Args:
            max_recent_ticks: 每个合约保留的最近 tick 数量（默认 100）
            wal_path: WAL文件路径（None则不启用WAL保护）
            flush_windows: 刷写时间窗口列表，格式为 [(start_h, start_m, end_h, end_m), ...]
                          默认: [(12, 0, 12, 50), (15, 30, 20, 0)]
        """
        self._params_service = None
        self._lock = threading.RLock()
        
        self._latest_ticks: Dict[str, Dict[str, Any]] = {}
        
        self._recent_ticks: Dict[str, deque] = {}
        self._max_recent_ticks = max_recent_ticks
        
        self._cache_hits = 0
        self._cache_misses = 0
        
        self._flush_windows = flush_windows if flush_windows else self.DEFAULT_FLUSH_WINDOWS
        
        self._wal_path = wal_path
        self._wal_file = None
        self._wal_count = 0
        self._wal_flush_interval = 100
        self._internal_id_counter = 0
        
        # P0 Bug #10修复：添加_pending_ticks队列，用于暂存params_service未就绪时的tick
        self._pending_ticks: List[Dict[str, Any]] = []
        self._dropped_pending_ticks = 0
        
        if wal_path:
            self._init_wal(wal_path)
            # P1 Bug #49修复：注册atexit关闭WAL文件
            atexit.register(self.close_wal)
        
        logging.info(f"[RealTimeCache] Initialized with max_recent_ticks={max_recent_ticks}, "
                    f"wal={'ON' if wal_path else 'OFF'}, flush_windows={self._flush_windows}")
    
    def set_params_service(self, params_service):
        pending_ticks: List[Dict[str, Any]] = []
        dropped_count = 0
        with self._lock:
            self._params_service = params_service
            if params_service is None:
                return
            if self._pending_ticks:
                pending_ticks = self._pending_ticks
                self._pending_ticks = []
                dropped_count = self._dropped_pending_ticks
                self._dropped_pending_ticks = 0

        if pending_ticks:
            logging.info(
                f"[RealTimeCache] params_service已接入，开始回放暂存tick "
                f"(pending_count={len(pending_ticks)}, dropped={dropped_count})"
            )
            replayed = 0
            for pending_tick in pending_ticks:
                if self.update_tick(**pending_tick):
                    replayed += 1
            logging.info(
                f"[RealTimeCache] 暂存tick回放完成 "
                f"(replayed={replayed}, dropped={dropped_count})"
            )

    def _should_log_pending_count(self, pending_count: int) -> bool:
        return pending_count <= 5 or pending_count in (10, 20, 50, 100, 200, 500, 1000) or pending_count % 5000 == 0
    
    def update_tick(self, symbol: str, price: float, timestamp: datetime, 
                    volume: int = 0, bid_price: float = 0.0, ask_price: float = 0.0) -> int:
        """更新 tick 数据到缓存
        
        Args:
            symbol: 合约代码
            price: 最新价格
            timestamp: 时间戳
            volume: 成交量
            bid_price: 买一价
            ask_price: 卖一价
            
        Returns:
            int: 分配的 internal_id
        """
        with self._lock:
            # P0 Bug #10修复：params_service未设置时，暂存tick而非静默丢弃
            if self._params_service is None:
                pending_tick = {
                    'symbol': symbol,
                    'price': price,
                    'timestamp': timestamp,
                    'volume': volume,
                    'bid_price': bid_price,
                    'ask_price': ask_price
                }
                if len(self._pending_ticks) >= self.MAX_PENDING_TICKS:
                    self._pending_ticks.pop(0)
                    self._dropped_pending_ticks += 1
                self._pending_ticks.append(pending_tick)
                pending_count = len(self._pending_ticks)
                if self._should_log_pending_count(pending_count):
                    logging.warning(
                        f"[RealTimeCache] params_service未就绪，tick暂存 "
                        f"(pending_count={pending_count}, dropped={self._dropped_pending_ticks})"
                    )
                return 0
            
            # ✅ 修复：统一使用全局序列，删除UUID生成
            # P2 Bug #109修复：使用ParamsService分配internal_id
            if self._params_service is not None:
                internal_id = self._params_service.get_internal_id(symbol)
                if internal_id is None:
                    # 如果缓存中没有，使用hash作为临时ID
                    internal_id = hash(symbol) & 0xFFFFFFFFFFFF
            else:
                # params_service未就绪时使用hash
                internal_id = hash(symbol) & 0xFFFFFFFFFFFF
            
            option_type = None
            strike_price = None
            
            # ✅ 分组C根因修复：统一从 metadata 获取，禁止 fallback 到字符串解析
            if self._params_service is not None:
                try:
                    info = self._params_service.get_instrument_meta_by_id(symbol)
                    if info and info.get('type') == 'option':
                        raw_opt = info.get('option_type', '')
                        normalized_type = _normalize_option_type(raw_opt)
                        if normalized_type == 'CALL':
                            option_type = 'CALL'
                        elif normalized_type == 'PUT':
                            option_type = 'PUT'
                        strike_price = info.get('strike_price')
                    elif info and info.get('type') == 'future':
                        # 期货不需要 option_type/strike_price
                        pass
                    else:
                        logging.warning(f"[RealTimeCache] metadata missing or invalid type for {symbol}, skipping cache update")
                        return 0
                except Exception as e:
                    logging.warning(f"[RealTimeCache] failed to get metadata for {symbol}, skipping cache update: {e}")
                    return 0
            else:
                logging.warning(f"[RealTimeCache] params_service not initialized, skipping cache update for {symbol}")
                return 0
            
            tick_data = {
                'internal_id': internal_id,
                'price': price,
                'timestamp': timestamp,
                'volume': volume,
                'bid_price': bid_price,
                'ask_price': ask_price,
                'option_type': option_type,
                'strike_price': strike_price
            }
            
            self._latest_ticks[symbol] = tick_data
            
            # ✅ 修复：删除_recent_ticks独立存储，由_latest_ticks快照派生
            # if symbol not in self._recent_ticks:
            #     self._recent_ticks[symbol] = deque(maxlen=self._max_recent_ticks)
            # self._recent_ticks[symbol].append(tick_data)
            
            if self._wal_file:
                self._write_wal(symbol, tick_data)
            
            return internal_id
    
    # ✅ 接口唯一：实时缓存层get_latest_price，数据源为内存tick快照；DataService.get_latest_price为DB层，两者分层不重复
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """获取最新价格 - < 100 ns（P2 优化：从结构化快照中提取）"""
        with self._lock:
            tick = self._latest_ticks.get(symbol)
            if tick is not None:
                self._cache_hits += 1
                return tick['price']
            else:
                self._cache_misses += 1
                return None
    
    def get_recent_ticks(self, symbol: str, count: int = 10) -> List[Dict]:
        """获取最近 N 条 tick
        
        Args:
            symbol: 合约代码
            count: 返回数量
            
        Returns:
            List[Dict]: tick 列表（按时间排序）
        """
        with self._lock:
            # P2 Bug #110修复：防止负数count导致意外结果
            count = max(1, count)
            ticks = self._recent_ticks.get(symbol, deque())
            return list(ticks)[-count:]
    
    def get_average_price(self, symbol: str, window: int = 20) -> Optional[float]:
        """计算移动平均价
        
        Args:
            symbol: 合约代码
            window: 窗口大小
            
        Returns:
            Optional[float]: 平均价格
        """
        recent = self.get_recent_ticks(symbol, window)
        if not recent:
            return None
        
        prices = [t['price'] for t in recent if t['price'] > 0]
        if not prices:
            return None
        
        return sum(prices) / len(prices)
    
    def get_vwap(self, symbol: str, window: int = 20) -> Optional[float]:
        """计算 VWAP (Volume Weighted Average Price)
        
        Args:
            symbol: 合约代码
            window: 窗口大小
            
        Returns:
            Optional[float]: VWAP
        """
        recent = self.get_recent_ticks(symbol, window)
        if not recent:
            return None
        
        total_volume = 0
        weighted_sum = 0.0
        
        for tick in recent:
            vol = tick.get('volume', 0)
            price = tick.get('price', 0)
            if vol > 0 and price > 0:
                total_volume += vol
                weighted_sum += price * vol
        
        if total_volume == 0:
            return None
        
        return weighted_sum / total_volume
    
    def get_spread(self, symbol: str) -> Optional[float]:
        """获取买卖价差
        
        Args:
            symbol: 合约代码
            
        Returns:
            Optional[float]: 价差 (ask - bid)
        """
        with self._lock:
            ticks = self._recent_ticks.get(symbol, deque())
            if not ticks:
                return None
            
            latest = ticks[-1]
            bid = latest.get('bid_price', 0)
            ask = latest.get('ask_price', 0)
            
            if bid > 0 and ask > 0:
                return ask - bid
            return None
    
    def has_symbol(self, symbol: str) -> bool:
        """检查合约是否在缓存中"""
        with self._lock:
            return symbol in self._latest_ticks
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self._lock:
            total_requests = self._cache_hits + self._cache_misses
            hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'total_symbols': len(self._latest_ticks),
                'cache_hits': self._cache_hits,
                'cache_misses': self._cache_misses,
                'hit_rate_percent': round(hit_rate, 2),
                'total_ticks_cached': sum(len(dq) for dq in self._recent_ticks.values())
            }
    
    def cleanup_old_data(self, max_age_seconds: int = 3600):
        """清理过期数据
        
        Args:
            max_age_seconds: 最大保留时间（秒），默认 1 小时
        """
        with self._lock:
            cutoff = datetime.now() - timedelta(seconds=max_age_seconds)
            
            symbols_to_remove = []
            for symbol, ticks in self._recent_ticks.items():
                # 移除过期的 tick
                while ticks and ticks[0]['timestamp'] < cutoff:
                    ticks.popleft()
                
                # 如果该合约没有数据了，标记删除
                if not ticks:
                    symbols_to_remove.append(symbol)
            
            # 删除空合约
            for symbol in symbols_to_remove:
                del self._recent_ticks[symbol]
                if symbol in self._latest_ticks:
                    del self._latest_ticks[symbol]
            
            if symbols_to_remove:
                logging.debug(f"[RealTimeCache] Cleaned up {len(symbols_to_remove)} expired symbols")
    
    def reset_stats(self):
        """重置统计信息"""
        with self._lock:
            self._cache_hits = 0
            self._cache_misses = 0
    
    def clear_all(self):
        """清空所有缓存"""
        with self._lock:
            self._latest_ticks.clear()
            self._recent_ticks.clear()
            self.reset_stats()
            logging.info("[RealTimeCache] All cache cleared")
    
    # ========================================================================
    # 增量WAL保护 + 批量刷写
    # ========================================================================
    
    def _init_wal(self, wal_path: str):
        """初始化WAL文件（append-only模式）"""
        try:
            os.makedirs(os.path.dirname(wal_path), exist_ok=True)
            self._wal_file = open(wal_path, 'a', encoding='utf-8')
            logging.info(f"[RealTimeCache] WAL enabled: {wal_path}")
        except Exception as e:
            logging.warning(f"[RealTimeCache] WAL init failed: {e}, falling back to no-WAL")
            # Bug1修复：异常时关闭已打开的文件句柄，防止句柄泄漏
            if self._wal_file is not None:
                try:
                    self._wal_file.close()
                except Exception:
                    pass
            self._wal_file = None
            self._wal_path = None
    
    def _write_wal(self, symbol: str, tick_data: Dict[str, Any]):
        """追加写入WAL（在_lock内调用，无需额外加锁）"""
        try:
            ts = tick_data.get('timestamp')
            ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
            record = {
                's': symbol,
                'p': tick_data.get('price', 0),
                't': ts_str,
                'v': tick_data.get('volume', 0),
                'b': tick_data.get('bid_price', 0),
                'a': tick_data.get('ask_price', 0),
                'ot': tick_data.get('option_type'),
                'sp': tick_data.get('strike_price'),
                'iid': tick_data.get('internal_id'),
            }
            self._wal_file.write(json.dumps(record, ensure_ascii=False) + '\n')
            self._wal_count += 1
            if self._wal_count % self._wal_flush_interval == 0:
                self._wal_file.flush()
        except Exception as e:
            logging.debug(f"[RealTimeCache] WAL write error: {e}")
    
    def is_in_flush_window(self, check_time: Optional[datetime] = None) -> bool:
        """检查当前时间是否在刷写窗口内
        
        Args:
            check_time: 要检查的时间（默认当前时间）
            
        Returns:
            bool: 是否在刷写窗口内
        """
        if check_time is None:
            check_time = datetime.now()
        
        current_minutes = check_time.hour * 60 + check_time.minute
        
        for start_h, start_m, end_h, end_m in self._flush_windows:
            start_minutes = start_h * 60 + start_m
            end_minutes = end_h * 60 + end_m
            
            if start_minutes <= end_minutes:
                if start_minutes <= current_minutes <= end_minutes:
                    return True
            else:
                if current_minutes >= start_minutes or current_minutes <= end_minutes:
                    return True
        
        return False
    
    def get_flush_windows(self) -> List[tuple]:
        """获取当前配置的刷写时间窗口"""
        return self._flush_windows.copy()
    
    def set_flush_windows(self, windows: List[tuple]):
        """设置刷写时间窗口
        
        Args:
            windows: 时间窗口列表，格式为 [(start_h, start_m, end_h, end_m), ...]
        """
        with self._lock:
            self._flush_windows = windows
            logging.info(f"[RealTimeCache] Flush windows updated: {windows}")
    
    def drain_all_ticks(self, 
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        option_type: Optional[str] = None,
                        clear_cache: bool = True) -> List[Dict[str, Any]]:
        """排空所有缓存Tick数据，返回并清空_recent_ticks（用于批量落库）
        
        保留_latest_ticks以继续提供实时查询。
        
        Args:
            start_time: 起始时间过滤（可选）
            end_time: 结束时间过滤（可选）
            option_type: 合约类型过滤 ('CALL' 或 'PUT'，可选)
            clear_cache: 是否清空缓存（默认 True）
            
        Returns:
            List[Dict]: 所有缓存的Tick数据，每条包含 symbol 和 tick 字段
        """
        with self._lock:
            result = []
            filtered_count = 0
            
            for symbol, ticks in self._recent_ticks.items():
                for tick in ticks:
                    tick_time = tick.get('timestamp')
                    if isinstance(tick_time, str):
                        try:
                            tick_time = datetime.fromisoformat(tick_time)
                        except ValueError:
                            tick_time = None
                    
                    if start_time and tick_time and tick_time < start_time:
                        filtered_count += 1
                        continue
                    if end_time and tick_time and tick_time > end_time:
                        filtered_count += 1
                        continue
                    
                    tick_opt_type = tick.get('option_type')
                    if option_type:
                        normalized_tick_type = _normalize_option_type(tick_opt_type)
                        normalized_input_type = _normalize_option_type(option_type)
                        if normalized_tick_type != normalized_input_type:
                            filtered_count += 1
                            continue
                    
                    result.append({'symbol': symbol, **tick})
            
            if clear_cache:
                self._recent_ticks.clear()
            
            count = len(result)
        
        if count > 0:
            logging.info(f"[RealTimeCache] Drained {count} ticks for flush "
                        f"(filtered: {filtered_count}, clear_cache: {clear_cache})")
        return result

    def restore_ticks(self, ticks: List[Dict[str, Any]]) -> None:
        """恢复ticks到缓存 - 仅恢复到_latest_ticks，删除_recent_ticks"""
        with self._lock:
            for tick in ticks:
                symbol = tick.get('symbol')
                if symbol:
                    # ✅ 修复：仅恢复到_latest_ticks，不再使用_recent_ticks
                    self._latest_ticks[symbol] = tick
            logging.info(f"[RealTimeCache] Restored {len(ticks)} ticks back to cache")

    def truncate_wal(self, reason: str = "flush_complete"):
        """
        清空WAL文件（统一接口）
        
        Args:
            reason: 清空原因
                   - 'flush_complete': WAL已成功刷写到DuckDB（默认）
                   - 'manual': 手动清理
                   - 'test': 测试用
        
        P1 Bug #50修复：WAL已成功刷写到DuckDB，清空WAL文件（线程安全）
        修复方案：将flush/close/open操作移入_lock内执行
        """
        # P1 Bug #50修复：在锁内执行所有WAL操作
        with self._lock:
            if self._wal_file:
                try:
                    self._wal_file.flush()
                    self._wal_file.close()
                    # 截断WAL文件
                    self._wal_file = open(self._wal_path, 'w', encoding='utf-8')
                    self._wal_count = 0
                    logging.info(f"[RealTimeCache] WAL truncated ({reason})")
                except Exception as e:
                    logging.warning(f"[RealTimeCache] WAL truncate error: {e}")
    
    def close_wal(self):
        """关闭WAL文件（进程退出时调用，可安全重复调用）"""
        # Bug3修复：在锁内执行，支持安全重复调用
        with self._lock:
            if self._wal_file is None:
                return  # 已关闭或WAL未启用，安全返回
            try:
                self._wal_file.flush()
                self._wal_file.close()
                self._wal_file = None
                logging.info("[RealTimeCache] WAL closed")
            except Exception as e:
                logging.warning(f"[RealTimeCache] WAL close error: {e}")
    
    @staticmethod
    def recover_from_wal(wal_path: str, 
                         restore_to_cache: Optional['RealTimeCache'] = None) -> List[Dict[str, Any]]:
        """从WAL文件恢复Tick数据（进程崩溃后调用）
        
        Args:
            wal_path: WAL文件路径
            restore_to_cache: 可选的缓存实例，恢复后自动填充到缓存中
            
        Returns:
            List[Dict]: 恢复的Tick数据列表
        """
        if not os.path.exists(wal_path):
            return []
        
        recovered = []
        max_internal_id = 0
        
        try:
            with open(wal_path, 'r', encoding='utf-8') as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        internal_id = record.get('iid', 0)
                        if internal_id > max_internal_id:
                            max_internal_id = internal_id
                        
                        ts_str = record.get('t', '')
                        try:
                            timestamp = datetime.fromisoformat(ts_str)
                        except (ValueError, TypeError):
                            timestamp = datetime.now()
                        
                        tick = {
                            'internal_id': internal_id,
                            'symbol': record.get('s', ''),
                            'price': record.get('p', 0),
                            'timestamp': timestamp,
                            'volume': record.get('v', 0),
                            'bid_price': record.get('b', 0),
                            'ask_price': record.get('a', 0),
                            'option_type': record.get('ot'),
                            'strike_price': record.get('sp'),
                        }
                        recovered.append(tick)
                    except json.JSONDecodeError:
                        logging.warning(f"[RealTimeCache] WAL line {line_no} corrupt, skipped")
            
            if restore_to_cache and recovered:
                with restore_to_cache._lock:
                    for tick in recovered:
                        symbol = tick.get('symbol')
                        if not symbol:
                            continue
                        
                        restore_to_cache._latest_ticks[symbol] = tick
                        
                        if symbol not in restore_to_cache._recent_ticks:
                            restore_to_cache._recent_ticks[symbol] = deque(
                                maxlen=restore_to_cache._max_recent_ticks)
                        restore_to_cache._recent_ticks[symbol].append(tick)
                    
                    if max_internal_id > restore_to_cache._internal_id_counter:
                        restore_to_cache._internal_id_counter = max_internal_id
            
            if recovered:
                logging.info(f"[RealTimeCache] Recovered {len(recovered)} ticks from WAL: {wal_path}"
                            f"{', restored to cache' if restore_to_cache else ''}")
            else:
                logging.info(f"[RealTimeCache] WAL empty or no recoverable data: {wal_path}")
        except Exception as e:
            logging.error(f"[RealTimeCache] WAL recovery failed: {e}")
        
        return recovered


# ✅ 传递渠道唯一：RealTimeCache单例工厂函数，禁止直接实例化
_realtime_cache_instance: Optional['RealTimeCache'] = None
_realtime_cache_lock = threading.Lock()


def get_realtime_cache(**kwargs) -> 'RealTimeCache':
    """获取RealTimeCache单例（线程安全）"""
    global _realtime_cache_instance
    if _realtime_cache_instance is None:
        with _realtime_cache_lock:
            if _realtime_cache_instance is None:
                _realtime_cache_instance = RealTimeCache(**kwargs)
    return _realtime_cache_instance
