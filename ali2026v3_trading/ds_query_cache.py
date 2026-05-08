"""ds_query_cache.py - 查询缓存与查询方法Mixin

从data_service.py拆分出的查询缓存和查询方法职责，包括：
- on_tick 实时缓存更新
- datetime规范化
- query/_query_with_cache 查询执行与缓存
- 各种查询接口（最新价格、时间范围、K线等）
- 缓存清理
"""
import pyarrow as pa
import pandas as pd
import logging
import hashlib
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timezone

logger = logging.getLogger(__name__)


class QueryCacheMixin:
    """查询缓存与查询方法Mixin - 由DataService组合使用"""

    def on_tick(self, symbol: str, price: float, timestamp: datetime, 
                volume: int = 0, bid_price: float = 0.0, ask_price: float = 0.0):
        """接收 tick 时同步更新内存缓存（非持久化路径）
        
        注意：此方法仅更新 RealTimeCache 内存缓存和清理查询缓存，
        不写入 ticks_raw 表。数据持久化应通过 storage.process_tick() 完成。
        不应将此方法作为数据持久化路径使用。
        """
        if self.realtime_cache:
            self.realtime_cache.update_tick(symbol, price, timestamp, volume, bid_price, ask_price)
        
        with self._cache_lock:
            keys_to_delete = [k for k in self._query_cache.keys() if k == symbol or (isinstance(k, str) and k.startswith(symbol + '.'))]
            for k in keys_to_delete:
                del self._query_cache[k]

    @staticmethod
    def _normalize_datetime_for_cache(dt: Optional[datetime]) -> Optional[str]:
        """将 datetime 规范化为 UTC naive 字符串（无微秒），用于缓存键。"""
        if dt is None:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        dt = dt.replace(microsecond=0)
        return dt.isoformat()

    @staticmethod
    def _to_utc_naive(dt: datetime) -> datetime:
        """将可能有时区的 datetime 转换为 UTC naive，并发出警告。"""
        if dt.tzinfo is not None:
            logger.warning(f"Converting timezone-aware datetime {dt} to UTC naive. "
                           "Ensure your table's timestamp column is UTC.")
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def query(self, sql: str, params: Optional[List] = None, arrow: bool = True, raise_on_error: bool = False, use_cache: bool = True):
        """
        执行 SQL 查询，默认返回 Arrow Table。
        自动检测 SQL 类型：SELECT/PRAGMA 用读连接，INSERT/UPDATE/DELETE 用写连接。
        """
        is_read = sql.strip().upper().startswith(('SELECT', 'PRAGMA', 'DESCRIBE', 'EXPLAIN', 'SHOW'))
        if use_cache and self.QUERY_CACHE_SIZE > 0 and is_read:
            return self._query_with_cache(sql, params, arrow=arrow)
        
        conn = self._get_read_connection() if is_read else self._get_connection()
        try:
            if params:
                rel = conn.execute(sql, params)
            else:
                rel = conn.execute(sql)
            result = rel.arrow() if arrow else rel.df()
            if hasattr(result, 'read_all'):
                result = result.read_all()
            return result
        except Exception as e:
            logger.error(f"Query failed: {e}\nSQL: {sql}")
            if raise_on_error:
                raise
            return pa.table({}) if arrow else pd.DataFrame()

    def _query_with_cache(self, sql: str, params: Optional[List] = None, arrow: bool = True) -> pa.Table:
        """内部缓存查询方法，由query()调用"""
        norm_params = []
        if params:
            for p in params:
                if isinstance(p, datetime):
                    norm_params.append(self._normalize_datetime_for_cache(p))
                else:
                    norm_params.append(p)
        key = hashlib.md5((sql + repr(norm_params) + str(arrow)).encode()).hexdigest()
        with self._cache_lock:
            if key in self._query_cache:
                result, expire = self._query_cache[key]
                if time.time() < expire:
                    return result
                else:
                    del self._query_cache[key]
        result = self.query(sql, params, use_cache=False, arrow=arrow)
        with self._cache_lock:
            self._query_cache[key] = (result, time.time() + self.QUERY_CACHE_TTL)
            self._query_cache.move_to_end(key)
            if len(self._query_cache) > self.QUERY_CACHE_SIZE:
                self._query_cache.popitem(last=False)
        return result

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """获取合约最新价格 - RealTimeCache为唯一实时价格源，DuckDB仅历史回补"""
        if self.realtime_cache:
            price = self.realtime_cache.get_latest_price(symbol)
            if price is not None:
                return price
        
        sql = "SELECT last_price FROM latest_prices WHERE instrument_id = ?"
        result_df = self.query(sql, [symbol], raise_on_error=False, arrow=False, use_cache=False)
        
        if hasattr(result_df, 'empty'):
            return float(result_df['last_price'].iloc[0]) if not result_df.empty else None
        elif hasattr(result_df, 'num_rows'):
            return result_df['last_price'][0].as_py() if result_df.num_rows > 0 else None
        return None

    def batch_get_latest_prices(self, symbols: List[str]) -> pa.Table:
        """批量获取合约最新价格 - RealTimeCache为唯一实时价格源，DuckDB仅历史回补
        
        Args:
            symbols: 合约代码列表
            
        Returns:
            pa.Table: 包含 instrument_id, last_price, timestamp 的结果表
        """
        if not symbols:
            return pa.table({})
        
        if self.realtime_cache:
            cached_results = []
            missing_symbols = []
            for s in symbols:
                price = self.realtime_cache.get_latest_price(s)
                if price is not None:
                    cached_results.append({'instrument_id': s, 'last_price': price, 'timestamp': datetime.now()})
                else:
                    missing_symbols.append(s)
            
            if not missing_symbols:
                return pa.Table.from_pylist(cached_results)
            
            if cached_results and missing_symbols:
                logger.debug(f"[DataService] Partial cache hit: {len(cached_results)}/{len(symbols)}")
                symbols = missing_symbols

        placeholders = ','.join(['?' for _ in symbols])
        sql = f"""
            SELECT instrument_id, last_price, timestamp
            FROM latest_prices
            WHERE instrument_id IN ({placeholders})
        """
        result = self.query(sql, symbols, use_cache=False)
        
        if cached_results and hasattr(result, 'num_rows') and result.num_rows > 0:
            db_results = result.to_pylist()
            return pa.Table.from_pylist(cached_results + db_results)
        
        return result

    def get_time_range(self, instrument_id: str, start: datetime, end: datetime,
                       columns: Optional[List[str]] = None) -> pa.Table:
        """
        时间范围查询。
        注意：表中 timestamp 必须为 UTC naive。传入的 start/end 若带时区，会强制转换为 UTC naive 并发出警告。
        """
        start_utc = self._to_utc_naive(start)
        end_utc = self._to_utc_naive(end)
        if columns is None:
            columns = ['timestamp', 'last_price', 'volume']
        cols = ', '.join(columns)
        sql = f"""
            SELECT {cols}
            FROM ticks_raw
            WHERE instrument_id = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """
        return self.query(sql, [instrument_id, start_utc, end_utc])

    def get_daily_aggregates(self, start_date: date, end_date: date) -> pa.Table:
        sql = "SELECT * FROM daily_aggregates WHERE date BETWEEN ? AND ? ORDER BY date"
        return self.query(sql, [start_date, end_date])

    def get_symbol_daily_ohlc(self, symbol: str, start_date: date, end_date: date) -> pa.Table:
        sql = """
            SELECT * FROM symbol_daily_aggregates
            WHERE instrument_id = ? AND date BETWEEN ? AND ?
            ORDER BY date
        """
        return self.query(sql, [symbol, start_date, end_date])

    def get_kline_range(
        self,
        instrument_id: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None
    ) -> pa.Table:
        """
        获取 K 线时间范围数据（基于 ticks_raw 表）。
        
        Args:
            instrument_id: 合约代码
            start: 开始时间（UTC naive 或带时区，会自动转换）
            end: 结束时间（UTC naive 或带时区，会自动转换）
            limit: 可选的结果数量限制
            
        Returns:
            pa.Table: K 线数据，包含 timestamp, open, high, low, close, volume, open_interest
        """
        start_utc = self._to_utc_naive(start)
        end_utc = self._to_utc_naive(end)
        
        sql = """
            SELECT timestamp, open, high, low, close, volume, open_interest
            FROM ticks_raw
            WHERE instrument_id = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """
        params = [instrument_id, start_utc, end_utc]
        
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        
        return self.query(sql, params)

    def get_latest_klines(
        self,
        instrument_id: str,
        limit: int = 100
    ) -> pa.Table:
        """
        获取最新 N 条 K 线数据（基于 ticks_raw 表）。
        
        Args:
            instrument_id: 合约代码
            limit: 返回数量限制（默认 100）
            
        Returns:
            pa.Table: K 线数据，按时间正序排列
        """
        sql = """
            SELECT timestamp, open, high, low, close, volume, open_interest
            FROM ticks_raw
            WHERE instrument_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        result = self.query(sql, [instrument_id, limit])
        
        if hasattr(result, 'to_pandas'):
            df = result.to_pandas()
            df = df.iloc[::-1].reset_index(drop=True)
            return pa.Table.from_pandas(df)
        
        return result

    def get_kline_count(self, instrument_id: str) -> int:
        """
        获取合约的 K 线总条数。
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            int: K 线总条数
        """
        sql = "SELECT COUNT(*) as cnt FROM ticks_raw WHERE instrument_id = ?"
        result_df = self.query(sql, [instrument_id])
        if hasattr(result_df, 'empty'):
            return int(result_df['cnt'].iloc[0]) if not result_df.empty else 0
        elif hasattr(result_df, 'num_rows'):
            return result_df['cnt'][0].as_py() if result_df.num_rows > 0 else 0
        return 0

    def get_kline_stats(self, instrument_id: str) -> Dict[str, Any]:
        """
        获取合约 K 线统计数据。
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            Dict[str, Any]: 统计信息 {count, first_time, last_time}
        """
        sql = """
            SELECT 
                COUNT(*) as count,
                MIN(timestamp) as first_time,
                MAX(timestamp) as last_time
            FROM ticks_raw
            WHERE instrument_id = ?
        """
        result_df = self.query(sql, [instrument_id])
        if hasattr(result_df, 'empty'):
            if result_df.empty:
                return {}
            return {
                'count': int(result_df['count'].iloc[0]),
                'first_time': result_df['first_time'].iloc[0],
                'last_time': result_df['last_time'].iloc[0]
            }
        elif hasattr(result_df, 'num_rows'):
            if result_df.num_rows == 0:
                return {}
            return {
                'count': result_df['count'][0].as_py(),
                'first_time': result_df['first_time'][0].as_py(),
                'last_time': result_df['last_time'][0].as_py()
            }
        return {}

    def explain(self, sql: str) -> str:
        """返回查询执行计划，仅允许SELECT语句防止SQL注入"""
        sql_stripped = sql.strip().upper()
        if not sql_stripped.startswith('SELECT'):
            raise ValueError("explain() only accepts SELECT statements for security reasons")

        conn = self._get_connection()
        rows = conn.execute(f"EXPLAIN {sql}").fetchall()
        return "\n".join(str(row[0]) for row in rows)

    def clear_cache(self):
        with self._cache_lock:
            self._query_cache.clear()
        logger.info("Query cache cleared.")
        logging.info('[DataService] Cache cleared')
