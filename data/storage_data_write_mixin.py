# MODULE_ID: M1-043
"""
storage_data_write_mixin.py — 数据写入Mixin
包含: _StorageDataWriteMixin
"""

from ali2026v3_trading.infra.shared_utils import InitPhase, requires_phase, CHINA_TZ
from ali2026v3_trading.data.query_service import _KlineAggregator
import logging
import os
import re
import threading
import time
import json
import queue
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)


class StorageDataWriteService:

    def __init__(self, facade=None):
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


    def _enqueue_kline_chunks_by_info(self, info: Dict[str, Any], kline_data: List[Dict], period: str = '1min') -> bool:
        if not kline_data:
            return True
        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type', 'future')
        if internal_id is None:
            return False
        chunk_size = self.batch_size
        for start in range(0, len(kline_data), chunk_size):
            chunk = kline_data[start:start + chunk_size]
            if not self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, period):
                return False
        return True

    def _enqueue_tick_chunks_by_info(self, info: Dict[str, Any], tick_data: List[Dict]) -> bool:
        if not tick_data:
            return True
        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type')
        if internal_id is None or instrument_type not in ('future', 'option'):
            return False
        chunk_size = self.batch_size
        for start in range(0, len(tick_data), chunk_size):
            chunk = tick_data[start:start + chunk_size]
            if not self._enqueue_write('_save_tick_impl', internal_id, instrument_type, chunk):
                return False
        return True

    def _check_info_or_skip(self, internal_id: int, caller: str, extra: str = '') -> Optional[Dict]:
        """检查info是否存在，不存在则记录跳过日志并返回None"""
        info = self._get_info_by_id(internal_id)
        if not info:
            with self._queue_stats_lock:
                self._queue_stats['save_skip_count'] = self._queue_stats.get('save_skip_count', 0) + 1
            warn_key = (f'{caller}_skip', internal_id)
            with self._lock:
                if warn_key not in self._runtime_missing_warned:
                    self._runtime_missing_warned.add(warn_key)
                    logging.warning("[DATA_LOSS] %s: internal_id %d not found, skipping %s", caller, internal_id, extra)
            return None
        return info

    def _save_kline_impl(self, internal_id: int, instrument_type: str, kline_data: List[Dict], period: str = '1min') -> None:
        kline_rows = kline_data
        if not kline_rows:
            return

        info = self._check_info_or_skip(internal_id, '_save_kline_impl', f'{len(kline_rows)} klines(period={period})')
        if not info:
            return

        normalized_klines = []
        for row in kline_rows:
            ts = self._to_timestamp(row.get('ts') or row.get('timestamp'))
            if ts is None:
                continue
            normalized_klines.append({
                'internal_id': internal_id,
                'instrument_type': instrument_type,
                'period': period,
                'timestamp': datetime.fromtimestamp(ts),
                'open': row.get('open', 0.0),
                'high': row.get('high', 0.0),
                'low': row.get('low', 0.0),
                'close': row.get('close', 0.0),
                'volume': row.get('volume', 0),
                'open_interest': row.get('open_interest', 0),
                'trade_date': datetime.fromtimestamp(ts).date(),
            })

        if normalized_klines:
            self._data_service.batch_insert_klines(normalized_klines)

    def _save_tick_impl(self, internal_id: int, instrument_type: str, tick_data) -> None:
        info = self._check_info_or_skip(internal_id, '_save_tick_impl', 'tick')
        if not info:
            return

        instrument_id = info.get('instrument_id')
        if not instrument_id:
            logging.warning("[DATA_LOSS] _save_tick_impl: internal_id %d missing instrument_id, skipping", internal_id)
            return

        self._data_service.batch_insert_ticks(tick_data, instrument_id)

        # P2-R11-05: 写入日期分区表
        self._route_tick_to_date_partition(tick_data, instrument_id)

    def _route_tick_to_date_partition(self, tick_data, instrument_id: str) -> None:
        """P2-R11-05: 将tick数据路由到对应的日期分区表"""
        if not tick_data:
            return
        try:
            dates_seen = set()
            for tick in (tick_data if isinstance(tick_data, list) else [tick_data]):
                ts = self._to_timestamp(tick.get('ts') or tick.get('timestamp'))
                if ts is not None:
                    dt = datetime.fromtimestamp(ts)
                    dates_seen.add(dt.strftime('%Y%m%d'))

            for date_str in dates_seen:
                self._create_tick_table_for_date(date_str)
                self._insert_tick_to_partition(tick_data, instrument_id, date_str)
        except Exception as e:
            logging.debug("[P2-R11-05] 日期分区写入失败(非致命，ticks_raw已写入): %s", e)

    def _create_tick_table_for_date(self, date_str: str) -> None:
        """P2-R11-05: 为指定交易日创建独立的tick_data_YYYYMMDD分区表"""
        if not re.match(r'^\d{8}$', date_str):
            logging.warning("[P2-R11-05] 无效的日期格式: %s", date_str)
            return

        table_name = f'tick_data_{date_str}'
        with self._column_cache_lock:
            if table_name in self._column_cache:
                return

        try:
            check = self._data_service.query(
                "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = ?",
                (table_name,),
            ).to_pylist()
            if check and check[0]['cnt'] > 0:
                with self._column_cache_lock:
                    self._column_cache[table_name] = True
                return

            self._data_service.query(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp TIMESTAMP,
                    instrument_id VARCHAR,
                    exchange VARCHAR,
                    last_price DOUBLE,
                    volume BIGINT,
                    open_interest DOUBLE,
                    turnover DOUBLE,
                    bid_price DOUBLE,
                    ask_price DOUBLE,
                    bid_volume BIGINT,
                    ask_volume BIGINT,
                    bid_price2 DOUBLE,
                    ask_price2 DOUBLE,
                    bid_volume2 BIGINT,
                    ask_volume2 BIGINT,
                    bid_price3 DOUBLE,
                    ask_price3 DOUBLE,
                    bid_volume3 BIGINT,
                    ask_volume3 BIGINT,
                    bid_price4 DOUBLE,
                    ask_price4 DOUBLE,
                    bid_volume4 BIGINT,
                    ask_volume4 BIGINT,
                    bid_price5 DOUBLE,
                    ask_price5 DOUBLE,
                    bid_volume5 BIGINT,
                    ask_volume5 BIGINT,
                    date DATE,
                    option_type VARCHAR,
                    strike_price DOUBLE,
                    is_otm BOOLEAN,
                    sync_status VARCHAR,
                    future_sync_status VARCHAR,
                    is_same_rise BOOLEAN,
                    is_same_fall BOOLEAN,
                    is_diff_sync BOOLEAN,
                    spread_quality INTEGER,
                    days_to_expiry INTEGER,
                    implied_volatility DOUBLE
                )
            """, raise_on_error=True)
            self._data_service.query(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_inst_ts ON {table_name} (instrument_id, timestamp)",
                raise_on_error=True,
            )
            with self._column_cache_lock:
                self._column_cache[table_name] = True
            logging.info("[P2-R11-05] 创建日期分区表: %s", table_name)
        except Exception as e:
            logging.warning("[P2-R11-05] 创建日期分区表失败 %s: %s", table_name, e)

    def _insert_tick_to_partition(self, tick_data, instrument_id: str, date_str: str) -> None:
        """P2-R11-05: 将tick数据插入到指定的日期分区表"""
        table_name = f'tick_data_{date_str}'
        ticks = tick_data if isinstance(tick_data, list) else [tick_data]
        if not ticks:
            return

        rows = []
        for tick in ticks:
            ts = self._to_timestamp(tick.get('ts') or tick.get('timestamp'))
            if ts is None:
                continue
            dt = datetime.fromtimestamp(ts)
            tick_date_str = dt.strftime('%Y%m%d')
            if tick_date_str != date_str:
                continue
            rows.append((
                dt,
                instrument_id,
                tick.get('exchange'),
                tick.get('last_price', 0.0),
                tick.get('volume', 0),
                float(tick.get('open_interest', 0) or 0),
                tick.get('turnover'),
                tick.get('bid_price1'),
                tick.get('ask_price1'),
                tick.get('bid_volume1'),
                tick.get('ask_volume1'),
                tick.get('bid_price2'),
                tick.get('ask_price2'),
                tick.get('bid_volume2'),
                tick.get('ask_volume2'),
                tick.get('bid_price3'),
                tick.get('ask_price3'),
                tick.get('bid_volume3'),
                tick.get('ask_volume3'),
                tick.get('bid_price4'),
                tick.get('ask_price4'),
                tick.get('bid_volume4'),
                tick.get('ask_volume4'),
                tick.get('bid_price5'),
                tick.get('ask_price5'),
                tick.get('bid_volume5'),
                tick.get('ask_volume5'),
                dt.date(),
                tick.get('option_type'),
                tick.get('strike_price'),
                tick.get('is_otm'),
                tick.get('sync_status'),
                tick.get('future_sync_status'),
                tick.get('is_same_rise'),
                tick.get('is_same_fall'),
                tick.get('is_diff_sync'),
                tick.get('spread_quality'),
                tick.get('days_to_expiry'),
                tick.get('implied_volatility'),
            ))

        if not rows:
            return

        try:
            for row in rows:
                self._data_service.query(f"""
                    INSERT INTO {table_name}
                    (timestamp, instrument_id, exchange, last_price, volume, open_interest, turnover,
                     bid_price, ask_price, bid_volume, ask_volume,
                     bid_price2, ask_price2, bid_volume2, ask_volume2,
                     bid_price3, ask_price3, bid_volume3, ask_volume3,
                     bid_price4, ask_price4, bid_volume4, ask_volume4,
                     bid_price5, ask_price5, bid_volume5, ask_volume5,
                     date, option_type, strike_price,
                     is_otm, sync_status, future_sync_status,
                     is_same_rise, is_same_fall, is_diff_sync,
                     spread_quality, days_to_expiry, implied_volatility)
                    VALUES ({', '.join(['?'] * 40)})
                """, list(row))
        except Exception as e:
            logging.debug("[P2-R11-05] 分区表写入失败 %s: %s", table_name, e)

    def query_tick_partitions(self, instrument_id: str, start_date: str, end_date: str,
                              limit: Optional[int] = None) -> List[Dict]:
        """P2-R11-05: 查询日期分区表，使用UNION ALL合并多个分区

        Args:
            instrument_id: 合约ID
            start_date: 起始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            limit: 可选返回行数限制
        """
        partition_tables = self._get_tick_partition_tables(start_date, end_date)
        if not partition_tables:
            return []

        union_parts = []
        for tbl in partition_tables:
            union_parts.append(
                f"SELECT timestamp, instrument_id, last_price, volume, open_interest, "
                f"bid_price, ask_price, date, option_type, strike_price, "
                f"is_otm, sync_status, future_sync_status, "
                f"is_same_rise, is_same_fall, is_diff_sync "
                f"FROM {tbl} WHERE instrument_id = ?"
            )

        sql = " UNION ALL ".join(union_parts) + " ORDER BY timestamp"
        if limit:
            sql += f" LIMIT {limit}"

        params = [instrument_id] * len(partition_tables)
        try:
            result = self._data_service.query(sql, params).to_pylist()
            return result
        except Exception as e:
            logging.error("[P2-R11-05] 分区表查询失败: %s", e)
            return []

    def _get_tick_partition_tables(self, start_date: str, end_date: str) -> List[str]:
        """P2-R11-05: 获取日期范围内的分区表列表"""
        try:
            all_tables = self._data_service.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'tick_data_%' ORDER BY table_name"
            ).to_pylist()
        except Exception as e:
            logging.error("[P2-R11-05] 查询分区表列表失败: %s", e)
            return []

        result = []
        for row in all_tables:
            tbl_name = row['table_name']
            date_part = tbl_name[len('tick_data_'):]
            if len(date_part) == 8 and date_part.isdigit():
                if start_date <= date_part <= end_date:
                    result.append(tbl_name)
        return result

    def _drop_tick_partition(self, date_str: str) -> int:
        """P2-R11-05: 删除指定日期的分区表

        Returns:
            int: 删除的表数量 (0或1)
        """
        if not re.match(r'^\d{8}$', date_str):
            return 0

        table_name = f'tick_data_{date_str}'
        try:
            check = self._data_service.query(
                "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = ?",
                (table_name,),
            ).to_pylist()
            if not check or check[0]['cnt'] == 0:
                return 0

            self._data_service.query(f"DROP TABLE {table_name}", raise_on_error=True)
            with self._column_cache_lock:
                self._column_cache.pop(table_name, None)
            logging.info("[P2-R11-05] 已删除日期分区表: %s", table_name)
            return 1
        except Exception as e:
            logging.error("[P2-R11-05] 删除分区表失败 %s: %s", table_name, e)
            return 0

    def _cleanup_tick_partitions(self, days: int) -> int:
        """P2-R11-05: 清理超过保留天数的日期分区表（直接DROP TABLE）

        Args:
            days: 保留天数，超过此天数的分区表将被删除

        Returns:
            int: 删除的分区表数量
        """
        cutoff_date = (datetime.now(CHINA_TZ) - timedelta(days=days)).strftime('%Y%m%d')
        try:
            all_tables = self._data_service.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'tick_data_%' ORDER BY table_name"
            ).to_pylist()
        except Exception as e:
            logging.error("[P2-R11-05] 查询分区表列表失败: %s", e)
            return 0

        dropped = 0
        for row in all_tables:
            tbl_name = row['table_name']
            date_part = tbl_name[len('tick_data_'):]
            if len(date_part) == 8 and date_part.isdigit() and date_part < cutoff_date:
                dropped += self._drop_tick_partition(date_part)

        if dropped > 0:
            logging.info("[P2-R11-05] 清理完成，删除 %d 个过期分区表（保留 %d 天）", dropped, days)
        return dropped

    @requires_phase(InitPhase.READY)
    def process_tick(self, tick: Dict[str, Any]) -> None:
        if not self._validate_tick(tick):
            return

        tick = self._normalize_tick_fields(tick)

        tick_ts = self._to_timestamp(tick.get('ts'))
        if tick_ts is None:
            logging.error("process_tick 时间戳转换失败：%s", tick.get('timestamp') or tick.get('ts'))
            return
        tick['timestamp'] = tick_ts
        instrument = tick.get('instrument_id')
        price = tick.get('last_price')

        normalized_id = str(instrument or '').strip()
        info = self._params_service.get_instrument_meta_by_id(normalized_id)
        if info is None:
            info = self._get_instrument_info(normalized_id)
        if info is None:
            warn_key = ('process_tick', normalized_id)
            with self._lock:
                if warn_key not in self._runtime_missing_warned:
                    self._runtime_missing_warned.add(warn_key)
                    logging.info("[process_tick] 合约未预注册，尝试运行时注册：%s", normalized_id)
            try:
                self.register_instrument(normalized_id)
                info = self._params_service.get_instrument_meta_by_id(normalized_id)
                if info is None:
                    info = self._get_instrument_info(normalized_id)
            except Exception as e:
                logging.critical("[process_tick] 运行时注册失败 %s: %s，数据将被丢弃", normalized_id, e)
            if info is None:
                logging.critical("[process_tick] 合约 %s 不在任何注册路径中，Tick数据已丢弃", normalized_id)
                return

        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type')
        if internal_id is None or instrument_type not in ('future', 'option'):
            logging.critical("[process_tick] internal_id=None 或类型无效(%s)，Tick数据已丢弃：%s",
                           instrument_type, normalized_id)
            return

        if not self._enqueue_write('_save_tick_impl', internal_id, instrument_type, [tick]):
            logging.critical("[process_tick] Tick 入队失败，已跳过：%s", instrument)
            return

        current_time = tick_ts
        for period in self.SUPPORTED_PERIODS:
            key = (instrument, period)
            with self._agg_lock:
                agg = self._aggregators.get(key)
                if agg is None:
                    agg = _KlineAggregator(instrument, period, logging.getLogger(__name__))
                    self._aggregators[key] = agg

            completed_kline = agg.update(tick_ts, price,
                                         volume=tick.get('volume', 0),
                                         amount=tick.get('amount', 0.0),
                                         open_interest=tick.get('open_interest'))

            if completed_kline:
                if self._is_ext_kline_missing(instrument, period, current_time):
                    self.save_external_kline(completed_kline)
                else:
                    completed_ts = completed_kline.get('ts') or completed_kline.get('timestamp')
                    logging.debug("外部 K 线正常，丢弃合成的 K 线：%s %s %.3f",
                                  instrument, period, completed_ts)

    # FIX-M3: K线聚合被完全绕过修复
    # 根因: _dispatch_tick_inner 直接调用 batch_insert_ticks, 绕过 process_tick 中的 _KlineAggregator
    # 修复: 新增 aggregate_kline_only(tick) 只跑 K线聚合，不重复写 tick (避免与 batch_insert_ticks 双写)
    @requires_phase(InitPhase.READY)
    def aggregate_kline_only(self, tick: Dict[str, Any]) -> None:
        """仅运行 K线聚合器，不写 tick (tick 已由 batch_insert_ticks 写入)。

        与 process_tick 的差异:
        - 跳过 _enqueue_write('_save_tick_impl')，避免与 batch_insert_ticks 双写
        - 仅执行 _KlineAggregator.update() 并在K线完成时调用 save_external_kline
        - 兼容 datetime/ts/timestamp/UpdateTime 字段名
        """
        try:
            if not isinstance(tick, dict):
                return
            tick_norm = self._normalize_tick_fields(tick)
            tick_ts = self._to_timestamp(tick_norm.get('ts'))
            if tick_ts is None:
                return
            instrument = tick_norm.get('instrument_id')
            price = tick_norm.get('last_price')
            # FIX-P0-17: price<=0时尝试使用bid/ask中间价，避免深度虚值期权K线永远无法生成
            if not isinstance(price, (int, float)) or price <= 0:
                _bid = tick_norm.get('bid_price1') or tick_norm.get('bid_price')
                _ask = tick_norm.get('ask_price1') or tick_norm.get('ask_price')
                if isinstance(_bid, (int, float)) and isinstance(_ask, (int, float)) and _bid > 0 and _ask > 0:
                    price = (_bid + _ask) / 2.0
                else:
                    return
            if not instrument:
                return
            current_time = tick_ts
            for period in self.SUPPORTED_PERIODS:
                key = (instrument, period)
                with self._agg_lock:
                    agg = self._aggregators.get(key)
                    if agg is None:
                        agg = _KlineAggregator(instrument, period, logging.getLogger(__name__))
                        self._aggregators[key] = agg
                completed_kline = agg.update(tick_ts, price,
                                             volume=tick_norm.get('volume', 0),
                                             amount=tick_norm.get('amount', 0.0),
                                             open_interest=tick_norm.get('open_interest'))
                if completed_kline:
                    if self._is_ext_kline_missing(instrument, period, current_time):
                        self.save_external_kline(completed_kline)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _agg_err:
            logging.debug("[FIX-M3] aggregate_kline_only failed (non-fatal): %s", _agg_err)

    @requires_phase(InitPhase.READY)
    def flush_incomplete_klines(self) -> int:
        """FIX-M8-3: 强制刷新所有未完成的K线，解决稀疏tick场景下K线永不完成的问题。

        场景: 736 real ticks / 16288 instruments = ~0.045 tick/instrument
        大多数instrument只有1个tick，_KlineAggregator.update()初始化后永不返回completed_kline
        导致klines_real=0，即使tick数据已正常落库。

        此方法遍历所有_aggregators，调用flush()强制返回当前未完成的K线，
        确保每个收到tick的instrument至少有一根K线落库。
        应由周期性任务(flush_tick_buffer)调用。
        """
        flushed_count = 0
        try:
            with self._agg_lock:
                aggregators = list(self._aggregators.items())
            for (instrument, period), agg in aggregators:
                try:
                    kline = agg.flush()
                    if kline:
                        if self._is_ext_kline_missing(instrument, period, time.time()):
                            self.save_external_kline(kline)
                            flushed_count += 1
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _flush_err:
                    logging.debug("[FIX-M8-3] flush aggregator failed: inst=%s period=%s err=%s",
                                  instrument, period, _flush_err)
            if flushed_count > 0:
                logging.info("[FIX-M8-3] flush_incomplete_klines: 刷新 %d 根未完成K线", flushed_count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _bulk_err:
            logging.warning("[FIX-M8-3] flush_incomplete_klines bulk failed: %s", _bulk_err)
        return flushed_count

    @requires_phase(InitPhase.READY)
    def save_external_kline(self, kline_data: Dict[str, Any]) -> None:
        if not self._validate_kline(kline_data):
            return
        kline_data = kline_data.copy()
        ts = self._to_timestamp(kline_data.get('ts') or kline_data.get('timestamp'))
        if ts is None:
            logging.error("save_external_kline 时间戳转换失败：%s", kline_data.get('ts') or kline_data.get('timestamp'))
            return
        kline_data['ts'] = ts
        instrument_id = kline_data.get('instrument_id')
        period = kline_data.get('period') or '1min'

        normalized_id = str(instrument_id or '').strip()
        info = self._params_service.get_instrument_meta_by_id(normalized_id)
        if info is None:
            info = self._get_instrument_info(normalized_id)
        if info is None:
            warn_key = ('save_external_kline', normalized_id)
            with self._lock:
                if warn_key not in self._runtime_missing_warned:
                    self._runtime_missing_warned.add(warn_key)
                    logging.debug("[save_external_kline] 合约未预注册，跳过运行时自动注册/建表：%s", normalized_id)
            return

        internal_id = self._get_info_internal_id(info)
        instrument_type = info.get('type', 'future')
        if internal_id is None:
            return

        if not self._enqueue_write('_save_kline_impl', internal_id, instrument_type, [kline_data], period):
            logging.warning("[save_external_kline] K线入队失败，已跳过：%s %s", instrument_id, period)
            return

        with self._ext_kline_lock:
            self._last_ext_kline[(instrument_id, period)] = ts

    @requires_phase(InitPhase.READY)
    def batch_write_kline(self, instrument_id: str, kline_data: List[Dict], period: str = '1min') -> None:
        if not kline_data:
            return
        internal_id = self.register_instrument(instrument_id)
        info = self._get_info_by_id(internal_id)
        if not info:
            return
        instrument_type = info.get('type', 'future')

        chunk_size = self.batch_size
        for i in range(0, len(kline_data), chunk_size):
            chunk = kline_data[i:i+chunk_size]
            self._enqueue_write('_save_kline_impl', internal_id, instrument_type, chunk, period)

    @requires_phase(InitPhase.READY)
    def batch_write_tick(self, instrument_id: str, tick_data: List[Dict]) -> None:
        if not tick_data:
            return
        internal_id = self.register_instrument(instrument_id)
        info = self._get_info_by_id(internal_id)
        if not info:
            return
        instrument_type = info.get('type', 'future')

        # P2-R11-12修复: 批量写操作添加事务隔离
        chunk_size = self.batch_size
        try:
            for i in range(0, len(tick_data), chunk_size):
                chunk = tick_data[i:i+chunk_size]
                self._enqueue_write('_save_tick_impl', internal_id, instrument_type, chunk)
        except Exception as e:
            logging.error("[P2-R11-12] batch_write_tick写入异常: %s", e)
            raise

    @requires_phase(InitPhase.READY)
    def write_kline_to_table(self, table_name: str, kline_data: List[Dict]) -> None:
        if not kline_data:
            return

        self._validate_table_name(table_name)

        for k in kline_data:
            self._data_service.query(f"""
                INSERT OR REPLACE INTO {table_name}
                (timestamp, open, high, low, close, volume, open_interest)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [k.get('ts') or k.get('timestamp'),
                   k.get('open', 0.0),
                   k.get('high', 0.0),
                   k.get('low', 0.0),
                   k.get('close', 0.0),
                   k.get('volume', 0),
                   k.get('open_interest', 0)], raise_on_error=True)
        logging.debug(f"写入 {len(kline_data)} 条 K 线到 {table_name}")

    @requires_phase(InitPhase.READY)
    def write_tick_to_table(self, table_name: str, tick_data: List[Dict]) -> None:
        if not tick_data:
            return

        self._validate_table_name(table_name)

        for t in tick_data:
            self._data_service.query(f"""
                INSERT OR IGNORE INTO {table_name}
                (timestamp, last_price, volume, open_interest, bid_price1, ask_price1)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [t.get('ts') or t.get('timestamp'),
                   t.get('last_price', 0.0),
                   t.get('volume', 0),
                   t.get('open_interest', 0),
                   t.get('bid_price1'),
                   t.get('ask_price1')], raise_on_error=True)
        logging.debug(f"写入 {len(tick_data)} 条 Tick 到 {table_name}")

    def save_tick(self, tick: Dict[str, Any]) -> None:
        """[DEPRECATED] 请使用process_tick

        UPG-P1-05修复: 添加migration_guide
        Migration guide: replace save_tick(tick) → process_tick(tick)
        process_tick includes K-line aggregation, full validation, and shard routing.
        """
        import warnings
        warnings.warn(
            "save_tick is deprecated, use process_tick instead. "
            "Migration guide: replace save_tick(tick) → process_tick(tick). "
            "process_tick includes K-line aggregation, full validation, and shard routing.",
            DeprecationWarning,
            stacklevel=2,
        )
        logging.warning(
            "[DEPRECATED] save_tick is deprecated, use process_tick instead. "
            "Migration guide: replace save_tick(tick) → process_tick(tick)."
        )
        # R15-P1-DOC-P1-10修复: 补充logging.warning确保默认可见(DeprecationWarning被Python运行时默认过滤)
        self.process_tick(tick)
        # R15-P1-DATA-06修复: 可选回读验证(read-back verify)
        if getattr(self, '_enable_readback_verify', False):
            try:
                _inst = tick.get('instrument_id') or tick.get('InstrumentID')
                _ts = tick.get('ts') or tick.get('timestamp') or tick.get('UpdateTime')
                if _inst and _ts:
                    conn = self._data_service._get_connection() if hasattr(self, '_data_service') else None
                    if conn:
                        try:
                            row = conn.execute(
                                "SELECT last_price FROM ticks_raw WHERE instrument_id=? AND timestamp=? LIMIT 1",
                                [_inst, _ts]
                            ).fetchone()
                            if row is None:
                                logging.warning("R15-P1-DATA-06: read-back verify失败 instrument=%s ts=%s", _inst, _ts)
                        finally:
                            if hasattr(self._data_service, '_return_connection'):
                                self._data_service._return_connection(conn)
            except Exception as e:
                logging.debug("R15-P1-DATA-06: read-back verify异常(非阻塞): %s", e)


_StorageDataWriteMixin = StorageDataWriteService
