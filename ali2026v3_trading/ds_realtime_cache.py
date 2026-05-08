"""ds_realtime_cache.py - 当日内存缓存模块

从data_service.py拆分出的RealTimeCache独立模块，包括：
- 环境变量与配置解析
- RealTimeCache类（当日内存状态集合）
- 单例工厂函数
"""
import threading
import collections
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

_INTRADAY_MODE = os.getenv('INTRADAY_MEMORY_MODE', 'debug').lower()
_INTRADAY_MAX_TICKS_PER_SYMBOL = int(os.getenv('INTRADAY_MAX_TICKS_PER_SYMBOL', '1000'))
_INTRADAY_MAX_TOTAL_TICKS = int(os.getenv('INTRADAY_MAX_TOTAL_TICKS', '5000000'))
_INTRADAY_FULL_CAPTURE = _INTRADAY_MODE in ('production', 'prod', 'full')


def _get_data_paths_config():
    try:
        from ali2026v3_trading.config_service import get_config
        return get_config().data_paths
    except ImportError:
        logger.debug("config_service not available, using default data paths")
        return None
    except Exception as e:
        logger.warning(f"Failed to get data paths config: {e}")
        return None


def _get_default_data_dir():
    env_dir = os.getenv('ALI2026_DATA_DIR')
    if env_dir and env_dir.strip():
        return env_dir.strip()
    app_data = os.getenv('APPDATA', os.path.expanduser('~'))
    return os.path.join(app_data, 'InfiniTrader_SimulationX64', 'ali2026_data')


def _resolve_duckdb_file():
    env_val = os.getenv('DUCKDB_FILE')
    if env_val and env_val.strip():
        return env_val.strip()
    cfg = _get_data_paths_config()
    if cfg and cfg.duckdb_file and cfg.duckdb_file.strip():
        return cfg.duckdb_file.strip()
    return os.path.join(_get_default_data_dir(), 'ticks.duckdb')


def _resolve_parquet_path():
    env_val = os.getenv('TICK_DATA_PATH')
    if env_val and env_val.strip():
        return env_val.strip()
    cfg = _get_data_paths_config()
    if cfg and cfg.parquet_path and cfg.parquet_path.strip():
        return cfg.parquet_path.strip()
    return os.path.join(_get_default_data_dir(), 'ticks.parquet')


def _resolve_flush_windows():
    env_val = os.getenv('INTRADAY_FLUSH_WINDOWS')
    if env_val:
        return _parse_flush_windows_str(env_val)
    cfg = _get_data_paths_config()
    if cfg:
        return cfg.get_flush_windows()
    return [(12, 0, 12, 50), (15, 30, 20, 0)]


def _parse_flush_windows_str(windows_str):
    windows = []
    for window in windows_str.split(','):
        window = window.strip()
        if not window:
            continue
        try:
            start_str, end_str = window.split('-')
            sh, sm = map(int, start_str.split(':'))
            eh, em = map(int, end_str.split(':'))
            windows.append((sh, sm, eh, em))
        except (ValueError, TypeError):
            pass
    return windows or [(12, 0, 12, 50), (15, 30, 20, 0)]


class RealTimeCache:
    """当日内存状态集合（全量安全承接层）
    
    模式说明（由环境变量 INTRADAY_MEMORY_MODE 控制）：
    - debug(默认): 仅存每品种最新1条tick，内存上限保守，覆盖式保存
    - production/prod/full: 存当日全量tick序列，64G内存安全承接全天数据
    
    调试电脑：INTRADAY_MEMORY_MODE=debug（默认，内存上限保守）
    生产电脑：INTRADAY_MEMORY_MODE=production（64G内存，全量安全承接）
    
    Flush窗口配置（M-6 外部化）：
    - 环境变量 INTRADAY_FLUSH_WINDOWS='12:0-12:50,15:30-20:0'
    - ConfigService data_paths.flush_windows_str
    - 默认值: [(12,0,12,50), (15,30,20,0)]
    """
    MAX_PENDING_TICKS = 5000

    def __init__(self, max_recent_ticks: int = 100, wal_path=None, flush_windows=None):
        self._params_service = None
        self._lock = threading.RLock()
        self._shard_locks = [threading.Lock() for _ in range(16)]
        self._latest_ticks: Dict[str, Dict[str, Any]] = {}
        self._max_recent_ticks = max_recent_ticks
        self._cache_hits = 0
        self._cache_misses = 0
        self._flush_windows = flush_windows or _resolve_flush_windows()
        self._internal_id_counter = 0
        self._pending_ticks: List[Dict[str, Any]] = []
        self._dropped_pending_ticks = 0
        self._full_capture = _INTRADAY_FULL_CAPTURE
        self._max_per_symbol = _INTRADAY_MAX_TICKS_PER_SYMBOL if self._full_capture else 1
        self._max_total = _INTRADAY_MAX_TOTAL_TICKS if self._full_capture else 0
        self._tick_sequences: Dict[str, collections.deque] = {} if self._full_capture else None
        self._total_tick_count = 0
        self._total_dropped_overflow = 0
        mode_label = 'FULL_CAPTURE' if self._full_capture else 'LATEST_ONLY'
        logging.info(f"[IntradayMemoryState] Initialized mode={mode_label} "
                     f"max_per_symbol={self._max_per_symbol} max_total={self._max_total} "
                     f"env=INTRADAY_MEMORY_MODE={_INTRADAY_MODE}")

    def _get_shard_lock(self, symbol: str) -> threading.Lock:
        h = hash(symbol)
        return self._shard_locks[h & 0x0F]

    def set_params_service(self, params_service):
        pending_ticks = []
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
            logging.info(f"[RealTimeCache] params_service已接入，回放暂存tick (pending_count={len(pending_ticks)}, dropped={dropped_count})")
            replayed = 0
            for pt in pending_ticks:
                if self.update_tick(**pt):
                    replayed += 1
            logging.info(f"[RealTimeCache] 暂存tick回放完成 (replayed={replayed}, dropped={dropped_count})")

    def _should_log_pending_count(self, c: int) -> bool:
        return c <= 5 or c in (10, 20, 50, 100, 200, 500, 1000) or c % 5000 == 0

    def update_tick(self, symbol: str, price: float, timestamp: datetime,
                    volume: int = 0, bid_price: float = 0.0, ask_price: float = 0.0,
                    option_type: str = None, strike_price: float = None,
                    internal_id: int = 0) -> int:
        with self._lock:
            if self._params_service is None:
                pt = {'symbol': symbol, 'price': price, 'timestamp': timestamp,
                      'volume': volume, 'bid_price': bid_price, 'ask_price': ask_price}
                if len(self._pending_ticks) >= self.MAX_PENDING_TICKS:
                    self._pending_ticks.pop(0)
                    self._dropped_pending_ticks += 1
                self._pending_ticks.append(pt)
                pc = len(self._pending_ticks)
                if self._should_log_pending_count(pc):
                    logging.warning(f"[RealTimeCache] params_service未就绪，tick暂存 (pending_count={pc}, dropped={self._dropped_pending_ticks})")
                return 0
            if not internal_id:
                internal_id = self._params_service.get_internal_id(symbol) if self._params_service else 0
                if internal_id is None:
                    internal_id = hash(symbol) & 0xFFFFFFFFFFFF
            if option_type is None and self._params_service is not None:
                try:
                    info = self._params_service.get_instrument_meta_by_id(symbol)
                    if info and info.get('type') == 'option':
                        from ali2026v3_trading.shared_utils import normalize_option_type
                        nt = normalize_option_type(info.get('option_type', ''))
                        option_type = 'CALL' if nt == 'CALL' else ('PUT' if nt == 'PUT' else None)
                        strike_price = info.get('strike_price')
                except Exception:
                    pass
        tick_entry = {
            'internal_id': internal_id, 'price': price, 'timestamp': timestamp,
            'volume': volume, 'bid_price': bid_price, 'ask_price': ask_price,
            'option_type': option_type, 'strike_price': strike_price,
        }
        with self._get_shard_lock(symbol):
            self._latest_ticks[symbol] = tick_entry
            if self._full_capture and self._tick_sequences is not None:
                seq = self._tick_sequences.get(symbol)
                if seq is None:
                    ml = self._max_per_symbol if self._max_per_symbol > 0 else None
                    seq = collections.deque(maxlen=ml)
                    self._tick_sequences[symbol] = seq
                if self._max_total > 0 and self._total_tick_count >= self._max_total:
                    self._total_dropped_overflow += 1
                    if self._total_dropped_overflow <= 10 or self._total_dropped_overflow % 100000 == 0:
                        logging.warning(f"[IntradayMemoryState] 全量上限溢出 total={self._total_tick_count} "
                                       f"max={self._max_total} dropped={self._total_dropped_overflow}")
                else:
                    seq.append(tick_entry)
                    self._total_tick_count += 1
        return internal_id

    def get_latest_price(self, symbol: str) -> Optional[float]:
        with self._get_shard_lock(symbol):
            tick = self._latest_ticks.get(symbol)
        if tick is not None:
            self._cache_hits += 1
            return tick['price']
        self._cache_misses += 1
        return None

    def get_recent_ticks(self, symbol: str, count: int = 10) -> List[Dict]:
        with self._get_shard_lock(symbol):
            tick = self._latest_ticks.get(symbol)
        return [tick] if tick is not None else []

    def get_spread(self, symbol: str) -> Optional[float]:
        with self._get_shard_lock(symbol):
            tick = self._latest_ticks.get(symbol)
        if not tick:
            return None
        bid, ask = tick.get('bid_price', 0), tick.get('ask_price', 0)
        return ask - bid if bid > 0 and ask > 0 else None

    def has_symbol(self, symbol: str) -> bool:
        with self._get_shard_lock(symbol):
            return symbol in self._latest_ticks

    def get_cache_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._cache_hits + self._cache_misses
            return {'total_symbols': len(self._latest_ticks), 'cache_hits': self._cache_hits,
                    'cache_misses': self._cache_misses,
                    'hit_rate_percent': round(self._cache_hits / total * 100, 2) if total > 0 else 0}

    def reset_stats(self):
        with self._lock:
            self._cache_hits = 0
            self._cache_misses = 0

    def clear_all(self):
        with self._lock:
            for sl in self._shard_locks:
                sl.acquire()
            try:
                tick_count = len(self._latest_ticks)
                ticks_to_drain = list(self._latest_ticks.values()) if tick_count > 0 else []
                self._latest_ticks.clear()
                if self._tick_sequences is not None:
                    self._tick_sequences.clear()
                self._total_tick_count = 0
                self._total_dropped_overflow = 0
                self._cache_hits = 0
                self._cache_misses = 0
            finally:
                for sl in self._shard_locks:
                    sl.release()
        if ticks_to_drain:
            logging.warning("[RealTimeCache] clear_all: %d ticks discarded", tick_count)
        logging.info("[RealTimeCache] All cache cleared")
        return ticks_to_drain

    def clear_latest_ticks(self):
        with self._lock:
            for sl in self._shard_locks:
                sl.acquire()
            try:
                count = len(self._latest_ticks)
                self._latest_ticks.clear()
            finally:
                for sl in self._shard_locks:
                    sl.release()
        if count > 0:
            logging.info("[RealTimeCache] Cleared %d ticks after DB flush", count)

    def is_in_flush_window(self, check_time: Optional[datetime] = None) -> bool:
        ct = check_time or datetime.now()
        cm = ct.hour * 60 + ct.minute
        for sh, sm, eh, em in self._flush_windows:
            s, e = sh * 60 + sm, eh * 60 + em
            if s <= e:
                if s <= cm <= e:
                    return True
            else:
                if cm >= s or cm <= e:
                    return True
        return False

    def drain_all_ticks(self, start_time=None, end_time=None, option_type=None, clear_cache=False) -> List[Dict]:
        with self._lock:
            result = []
            for symbol, tick in self._latest_ticks.items():
                tt = tick.get('timestamp')
                if isinstance(tt, str):
                    try:
                        tt = datetime.fromisoformat(tt)
                    except ValueError:
                        tt = None
                if start_time and tt and tt < start_time:
                    continue
                if end_time and tt and tt > end_time:
                    continue
                if option_type:
                    t_ot = tick.get('option_type')
                    if t_ot and option_type and t_ot.upper() != option_type.upper():
                        continue
                result.append({'symbol': symbol, **tick})
            if clear_cache:
                self._latest_ticks.clear()
        if result:
            logging.info(f"[RealTimeCache] Drained {len(result)} ticks (clear_cache={clear_cache})")
        return result

    def restore_ticks(self, ticks: List[Dict]) -> None:
        with self._lock:
            for tick in ticks:
                symbol = tick.get('symbol')
                if symbol:
                    self._latest_ticks[symbol] = tick

    def get_tick_sequence(self, symbol: str, start_time: datetime = None,
                          end_time: datetime = None, limit: int = 0) -> List[Dict]:
        with self._lock:
            if not self._full_capture or self._tick_sequences is None:
                tick = self._latest_ticks.get(symbol)
                return [tick] if tick is not None else []
            seq = self._tick_sequences.get(symbol)
            if seq is None:
                return []
            result = []
            for t in seq:
                ts = t.get('timestamp')
                if start_time and ts and ts < start_time:
                    continue
                if end_time and ts and ts > end_time:
                    continue
                result.append(t)
                if limit > 0 and len(result) >= limit:
                    break
            return result

    def get_intraday_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = {
                'mode': 'FULL_CAPTURE' if self._full_capture else 'LATEST_ONLY',
                'symbols': len(self._latest_ticks),
                'cache_hits': self._cache_hits,
                'cache_misses': self._cache_misses,
            }
            if self._full_capture and self._tick_sequences is not None:
                stats['total_ticks'] = self._total_tick_count
                stats['total_dropped_overflow'] = self._total_dropped_overflow
                stats['max_total'] = self._max_total
                stats['max_per_symbol'] = self._max_per_symbol
                total_bytes = sum(len(s) for s in self._tick_sequences.values()) * 200
                stats['estimated_bytes'] = total_bytes
                stats['estimated_mb'] = round(total_bytes / (1024 * 1024), 1)
            return stats

    def get_kline_from_memory(self, symbol: str, count: int = 10,
                              period_seconds: int = 60) -> List[Dict]:
        if not self._full_capture or self._tick_sequences is None:
            return []
        with self._lock:
            seq = self._tick_sequences.get(symbol)
            if seq is None or len(seq) == 0:
                return []
            ticks = list(seq)
        ticks.sort(key=lambda t: t.get('timestamp', datetime.min))
        klines = []
        if len(ticks) == 0:
            return klines
        period_start = ticks[0].get('timestamp')
        current_bar = {'open': None, 'high': -float('inf'), 'low': float('inf'),
                       'close': None, 'volume': 0, 'start_time': period_start, 'count': 0}
        for t in ticks:
            ts = t.get('timestamp')
            price = t.get('price', 0)
            vol = t.get('volume', 0)
            if ts is None or price <= 0:
                continue
            elapsed = (ts - period_start).total_seconds() if period_start else 0
            if elapsed >= period_seconds:
                if current_bar['count'] > 0:
                    klines.append({
                        'timestamp': current_bar['start_time'],
                        'open': current_bar['open'], 'high': current_bar['high'],
                        'low': current_bar['low'], 'close': current_bar['close'],
                        'volume': current_bar['volume'],
                    })
                period_start = ts
                current_bar = {'open': price, 'high': price, 'low': price,
                               'close': price, 'volume': vol, 'start_time': ts, 'count': 1}
            else:
                if current_bar['open'] is None:
                    current_bar['open'] = price
                current_bar['high'] = max(current_bar['high'], price)
                current_bar['low'] = min(current_bar['low'], price)
                current_bar['close'] = price
                current_bar['volume'] += vol
                current_bar['count'] += 1
        if current_bar['count'] > 0:
            klines.append({
                'timestamp': current_bar['start_time'],
                'open': current_bar['open'], 'high': current_bar['high'],
                'low': current_bar['low'], 'close': current_bar['close'],
                'volume': current_bar['volume'],
            })
        if count > 0 and len(klines) > count:
            klines = klines[-count:]
        return klines

    @property
    def is_authoritative(self) -> bool:
        return True

    def get_authoritative_state(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            tick = self._latest_ticks.get(symbol)
            if tick is None:
                return None
            state = dict(tick)
            if self._full_capture and self._tick_sequences is not None:
                seq = self._tick_sequences.get(symbol)
                if seq is not None:
                    state['sequence_length'] = len(seq)
                    state['oldest_tick_time'] = seq[0]['timestamp'] if seq else None
            return state

    def get_all_active_symbols(self) -> List[str]:
        with self._lock:
            return list(self._latest_ticks.keys())

    def get_memory_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = {
                'mode': 'full_capture' if self._full_capture else 'latest_only',
                'total_symbols': len(self._latest_ticks),
                'total_ticks': self._total_tick_count,
                'dropped_overflow': self._total_dropped_overflow,
                'cache_hits': self._cache_hits,
                'cache_misses': self._cache_misses,
                'is_authoritative': True,
            }
            if self._full_capture and self._tick_sequences is not None:
                stats['sequence_count'] = len(self._tick_sequences)
            return stats

# 单例工厂
_realtime_cache_instance: Optional[RealTimeCache] = None
_realtime_cache_lock = threading.Lock()

def get_realtime_cache(**kwargs) -> RealTimeCache:
    global _realtime_cache_instance
    if _realtime_cache_instance is None:
        with _realtime_cache_lock:
            if _realtime_cache_instance is None:
                _realtime_cache_instance = RealTimeCache(**kwargs)
    return _realtime_cache_instance

_HAS_REALTIME_CACHE = True
