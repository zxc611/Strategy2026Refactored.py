"""ds_realtime_cache.py - 当日内存缓存模块

从data_service.py拆分出的RealTimeCache独立模块，包括：
- 环境变量与配置解析
- RealTimeCache类（当日内存状态集合）
- 单例工厂函数
"""
import threading
import collections
import logging
from ali2026v3_trading.resilience_utils import deterministic_round
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
from ali2026v3_trading.shared_utils import CHINA_TZ

try:
    from ali2026v3_trading.config_params import _MISSING_VALUE_DEFAULTS
except ImportError:
    _MISSING_VALUE_DEFAULTS = {
        'last_price': 0.0,
        'volume': 0,
        'open_interest': 0.0,
        'bid_price': 0.0,
        'ask_price': 0.0,
        'spread_quality': 0.0,
        'days_to_expiry': 999,
        'implied_volatility': 0.0,
    }

logger = logging.getLogger(__name__)

_INTRADAY_MODE = os.getenv('INTRADAY_MEMORY_MODE', 'debug').lower()
_INTRADAY_MAX_TICKS_PER_SYMBOL = int(os.getenv('INTRADAY_MAX_TICKS_PER_SYMBOL', '1000'))
_INTRADAY_MAX_TOTAL_TICKS = int(os.getenv('INTRADAY_MAX_TOTAL_TICKS', '5000000'))
_INTRADAY_FULL_CAPTURE = _INTRADAY_MODE in ('production', 'prod', 'full')

_DEFAULT_FLUSH_WINDOWS = [(12, 0, 12, 50), (15, 30, 20, 0)]
_INTERNAL_ID_HASH_MASK = 0xFFFFFFFFFFFF


def _get_data_paths_config():
    try:
        from ali2026v3_trading.config_service import get_config
        return get_config().data_paths
    except ImportError:
        logger.info("config_service not available, using default data paths")  # R24-P2-DF-03修复: debug→info，数据服务降级需可感知
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
    if cfg and hasattr(cfg, 'get_flush_windows') and callable(cfg.get_flush_windows):
        return cfg.get_flush_windows()
    if cfg and hasattr(cfg, 'flush_windows_str') and cfg.flush_windows_str:
        return _parse_flush_windows_str(cfg.flush_windows_str)
    return _DEFAULT_FLUSH_WINDOWS


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
    return windows or _DEFAULT_FLUSH_WINDOWS


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

    def __init__(self, max_recent_ticks: int = 100, wal_path=None, flush_windows=None,
                 default_flush_windows=None):
        self._params_service = None
        self._lock = threading.RLock()
        self._shard_locks = [threading.Lock() for _ in range(16)]
        self._stats_lock = threading.Lock()
        self._latest_ticks: Dict[str, Dict[str, Any]] = {}
        self._max_recent_ticks = max_recent_ticks
        self._cache_hits = 0
        self._cache_misses = 0
        self._cache_ttl_sec: float = 60.0  # R23-FR-02-FIX: 实时缓存TTL=60秒，停牌/流动性枯竭时返回过期标记
        # R23-IN-P1-12-FIX: 缓存TTL参数初始化守卫 — 确保TTL为正数
        try:
            if self._cache_ttl_sec <= 0:
                logging.warning("[R23-IN-P1-12] _cache_ttl_sec=%.1f非正数，重置为默认60.0", self._cache_ttl_sec)
                self._cache_ttl_sec = 60.0
        except Exception as _ttl_chk_e:
            self._cache_ttl_sec = 60.0
            logging.debug("[R23-IN-P1-12] TTL守卫异常，重置为60.0: %s", _ttl_chk_e)
        self._cache_timestamps: Dict[str, float] = {}
        self._default_flush_windows = default_flush_windows or _DEFAULT_FLUSH_WINDOWS
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
        # INV-P1-04/INV-TIM-02修复: bar_time单调递增追踪
        self._last_bar_time: Dict[str, datetime] = {}
        self._bar_time_violations: int = 0
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
                    volume: int = None, bid_price: float = None, ask_price: float = None,
                    option_type: str = None, strike_price: float = None,
                    internal_id: int = 0) -> int:
        # [DATA-P2-05] 缺失值默认填充配置化
        if volume is None:
            volume = _MISSING_VALUE_DEFAULTS.get('volume', 0)
        if bid_price is None:
            bid_price = _MISSING_VALUE_DEFAULTS.get('bid_price', 0.0)
        if ask_price is None:
            ask_price = _MISSING_VALUE_DEFAULTS.get('ask_price', 0.0)
        # INV-08/INV-DAT-01: 验证bid < ask，防止异常tick数据污染缓存
        if bid_price > 0 and ask_price > 0 and bid_price >= ask_price:
            logging.warning(
                "[INV-08] bid>=ask异常: symbol=%s bid=%.4f ask=%.4f, 丢弃该tick",
                symbol, bid_price, ask_price,
            )
            return 0
        # INV-P1-04/INV-TIM-02修复: bar_time单调递增验证
        if timestamp is not None:
            last_bt = self._last_bar_time.get(symbol)
            if last_bt is not None and timestamp < last_bt:
                self._bar_time_violations += 1
                logging.warning(
                    "[INV-TIM-02] bar_time非单调递增: symbol=%s current=%s < last=%s violations=%d",
                    symbol, timestamp, last_bt, self._bar_time_violations,
                )
                # 不丢弃，仅告警（时间回退可能是数据源重传）
            self._last_bar_time[symbol] = timestamp
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
                    internal_id = hash(symbol) & _INTERNAL_ID_HASH_MASK
            if option_type is None and self._params_service is not None:
                try:
                    info = self._params_service.get_instrument_meta_by_id(symbol)
                    if info and info.get('type') == 'option':
                        from ali2026v3_trading.shared_utils import normalize_option_type
                        nt = normalize_option_type(info.get('option_type', ''))
                        option_type = nt if nt in ('CALL', 'PUT') else None
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
                self._cache_timestamps[symbol] = time.time()  # R23-FR-02-FIX: 记录缓存写入时间
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
                cache_ts = self._cache_timestamps.get(symbol, 0.0)
                if (time.time() - cache_ts) > self._cache_ttl_sec:
                    logging.warning("[R23-FR-02-FIX] 缓存过期: symbol=%s age=%.1fs > ttl=%.1fs",
                                    symbol, time.time() - cache_ts, self._cache_ttl_sec)
                    with self._stats_lock:
                        self._cache_misses += 1
                    return None
                with self._stats_lock:
                    self._cache_hits += 1
                    _total = self._cache_hits + self._cache_misses
                    if _total % 1000 == 0:
                        _rate = self._cache_hits / _total * 100
                        logging.info("[R23-P2-03-FIX] 缓存命中率监控: hits=%d misses=%d hit_rate=%.2f%%",
                                    self._cache_hits, self._cache_misses, _rate)
                return tick['price']
        with self._stats_lock:
            self._cache_misses += 1
        return None

    def get_recent_ticks(self, symbol: str, count: int = 10) -> List[Dict]:
        with self._get_shard_lock(symbol):
            if self._full_capture and self._tick_sequences is not None:
                seq = self._tick_sequences.get(symbol)
                if seq is not None:
                    ticks = list(seq)
                    return ticks[-count:] if count > 0 else ticks
            tick = self._latest_ticks.get(symbol)
        return [tick] if tick is not None else []

    def get_spread(self, symbol: str) -> Optional[float]:
        with self._get_shard_lock(symbol):
            tick = self._latest_ticks.get(symbol)
        if not tick:
            return None
        bid, ask = tick.get('bid_price', _MISSING_VALUE_DEFAULTS.get('bid_price', 0)), tick.get('ask_price', _MISSING_VALUE_DEFAULTS.get('ask_price', 0))
        return ask - bid if bid > 0 and ask > 0 else None

    def has_symbol(self, symbol: str) -> bool:
        with self._get_shard_lock(symbol):
            return symbol in self._latest_ticks

    def _collect_base_stats(self) -> Dict[str, Any]:
        return {
            'total_symbols': len(self._latest_ticks),
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
        }

    def get_cache_age(self, symbol: str) -> Optional[float]:
        """[R23-P2-04-FIX] 返回指定品种缓存数据的年龄（秒），未缓存返回None"""
        cache_ts = self._cache_timestamps.get(symbol)
        if cache_ts is None or cache_ts == 0.0:
            return None
        return time.time() - cache_ts

    def get_cache_stats(self) -> Dict[str, Any]:
        with self._lock:
            base = self._collect_base_stats()
            total = base['cache_hits'] + base['cache_misses']
            # R27-P2-FP-12修复: round()→deterministic_round()
            base['hit_rate_percent'] = deterministic_round(base['cache_hits'] / total * 100, 2) if total > 0 else 0
            return base

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
        # R15-P1-DATA-03修复: 使用配置的night_session_end_hour取代硬编码
        ct = check_time or datetime.now(CHINA_TZ)
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
            stats = self._collect_base_stats()
            stats['mode'] = 'FULL_CAPTURE' if self._full_capture else 'LATEST_ONLY'
            stats['symbols'] = stats.pop('total_symbols')
            if self._full_capture and self._tick_sequences is not None:
                stats['total_ticks'] = self._total_tick_count
                stats['total_dropped_overflow'] = self._total_dropped_overflow
                stats['max_total'] = self._max_total
                stats['max_per_symbol'] = self._max_per_symbol
                total_bytes = sum(len(s) for s in self._tick_sequences.values()) * 200
                stats['estimated_bytes'] = total_bytes
                # R27-P2-FP-12修复: round()→deterministic_round()
                stats['estimated_mb'] = deterministic_round(total_bytes / (1024 * 1024), 1)
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
        # DFG-P1-09修复: tick→Bar转换volume字段语义归一化
        # 语义说明: 不同数据源的tick.volume语义不一致:
        #   - 交易所原始tick: volume为累计成交量（单调递增，跨tick累加）
        #   - Bar聚合需要: volume为周期内增量成交量（每个Bar独立计算）
        # 修复策略: 检测volume字段是否为累计量（采样前20个tick判断单调递增），
        #   若为累计量则转换为增量（当前tick.volume - 前一tick.volume），
        #   确保Bar.volume始终为周期内增量成交量。
        _prev_tick_vol = None
        _volume_is_cumulative = False
        _vol_sample_count = 0
        for _t in ticks[:min(20, len(ticks))]:
            _v = _t.get('volume', _MISSING_VALUE_DEFAULTS.get('volume', 0))
            if _prev_tick_vol is not None and _v > _prev_tick_vol and _vol_sample_count < 10:
                _volume_is_cumulative = True
                _vol_sample_count += 1
            elif _prev_tick_vol is not None and _v < _prev_tick_vol:
                _volume_is_cumulative = False
                break
            _prev_tick_vol = _v
        if _vol_sample_count < 3:
            _volume_is_cumulative = False
        _last_cumulative_vol = 0
        period_start = ticks[0].get('timestamp')
        current_bar = {'open': None, 'high': None, 'low': None,  # R27-P2-07-FIX: 用None替代inf避免序列化问题
                       'close': None, 'volume': 0, 'start_time': period_start, 'count': 0}
        for t in ticks:
            ts = t.get('timestamp')
            price = t.get('price', _MISSING_VALUE_DEFAULTS.get('last_price', 0))
            vol = t.get('volume', _MISSING_VALUE_DEFAULTS.get('volume', 0))
            # DFG-P1-09修复: 累计volume转增量volume
            if _volume_is_cumulative:
                _delta_vol = max(0, vol - _last_cumulative_vol)
                _last_cumulative_vol = vol
                vol = _delta_vol
            if ts is None or price <= 0:
                continue
            elapsed = (ts - period_start).total_seconds() if period_start else 0
            if elapsed >= period_seconds:
                if current_bar['count'] > 0:
                    # INV-09/INV-DAT-04: 周期完成bar的OHLC一致性验证
                    if not _validate_ohlcv(current_bar['open'], current_bar['high'],
                                           current_bar['low'], current_bar['close'],
                                           current_bar['volume']):
                        logging.warning(
                            "[DATA-P2-02] 周期bar OHLC一致性违反，丢弃异常Bar: "
                            "open=%.4f high=%.4f low=%.4f close=%.4f volume=%.4f",
                            current_bar['open'], current_bar['high'],
                            current_bar['low'], current_bar['close'],
                            current_bar['volume'],
                        )
                    else:
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
                current_bar['high'] = price if current_bar['high'] is None else max(current_bar['high'], price)  # R27-P2-07-FIX: None检查
                current_bar['low'] = price if current_bar['low'] is None else min(current_bar['low'], price)  # R27-P2-07-FIX: None检查
                current_bar['close'] = price
                current_bar['volume'] += vol
                current_bar['count'] += 1
        if current_bar['count'] > 0:
            # INV-09/INV-DAT-04: OHLC一致性验证 — 校验失败丢弃异常Bar
            _bar_open = current_bar['open']
            _bar_high = current_bar['high']
            _bar_low = current_bar['low']
            _bar_close = current_bar['close']
            if not _validate_ohlcv(_bar_open, _bar_high, _bar_low, _bar_close, current_bar['volume']):
                logging.warning(
                    "[DATA-P2-02] OHLC一致性违反，丢弃异常Bar: open=%.4f high=%.4f low=%.4f close=%.4f volume=%.4f",
                    _bar_open, _bar_high, _bar_low, _bar_close, current_bar['volume'],
                )
            else:
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
            stats = self._collect_base_stats()
            stats['mode'] = 'full_capture' if self._full_capture else 'latest_only'
            stats['total_ticks'] = self._total_tick_count
            stats['dropped_overflow'] = self._total_dropped_overflow
            stats['is_authoritative'] = True
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

try:
    assert RealTimeCache is not None and callable(get_realtime_cache)
    _HAS_REALTIME_CACHE = True
except (NameError, AssertionError):
    _HAS_REALTIME_CACHE = False


# ============================================================================
# R15-P2 数据质量修复块
# ============================================================================

# R15-P2-DATA-02修复: Bar合成OHLC关系检查
def _validate_ohlcv(open_price: float, high_price: float, low_price: float,
                    close_price: float, volume: float) -> bool:
    """校验OHLCV关系: high>=max(open,close), low<=min(open,close), volume>=0
    
    Returns:
        bool: True=合法, False=违反OHLC关系(数据异常)
    """
    if volume < 0:
        return False
    if high_price < max(open_price, close_price):
        return False
    if low_price > min(open_price, close_price):
        return False
    if low_price > high_price:
        return False
    return True
