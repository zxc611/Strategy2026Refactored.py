"""ds_data_writer.py - 数据写入与upsert方法Mixin

从data_service.py拆分出的数据写入职责，包括：
- 期权元数据enrichment
- Arrow schema构建与列补齐
- 批量tick插入
- 合约/品种upsert
- 增量加载
- WAL截断
- 分表同步
"""
import pyarrow as pa
import logging
import os
import time
import threading
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class DataWriterMixin:
    """数据写入与upsert方法Mixin - 由DataService组合使用"""

    def _enrich_tick_option_metadata(self, ticks_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """复用统一解析器，为批量 Tick 插入补齐期权元数据。
        
        修复标准：统一为ParamsService缓存+parse_option降级，删除DB查询
        """
        try:
            from ali2026v3_trading.subscription_manager import SubscriptionManager
        except Exception:
            SubscriptionManager = None

        enriched_ticks: List[Dict[str, Any]] = []
        for tick in ticks_data or []:
            tick_row = dict(tick or {})
            tick_row.setdefault('option_type', None)
            tick_row.setdefault('strike_price', None)

            instrument_id = str(tick_row.get('instrument_id') or '').strip()
            enriched = False
            
            if instrument_id and self.params_service:
                try:
                    info = self.params_service.get_instrument_meta_by_id(instrument_id)
                    if info and info.get('type') == 'option':
                        raw_opt = info.get('option_type', '')
                        raw_opt_upper = raw_opt.upper() if raw_opt else ''
                        tick_row['option_type'] = 'CALL' if raw_opt_upper in ('C', 'CALL') else 'PUT' if raw_opt_upper in ('P', 'PUT') else None
                        tick_row['strike_price'] = float(info.get('strike_price') or 0.0)
                        enriched = True
                except Exception:
                    pass

            if not enriched and instrument_id and SubscriptionManager:
                try:
                    parsed = SubscriptionManager.parse_option(instrument_id)
                    if not tick_row.get('option_type'):
                        tick_row['option_type'] = 'CALL' if parsed.get('option_type') == 'C' else 'PUT'
                    if not tick_row.get('strike_price'):
                        tick_row['strike_price'] = float(parsed.get('strike_price') or 0.0)
                except Exception:
                    pass

            enriched_ticks.append(tick_row)

        return enriched_ticks

    def _ensure_arrow_tick_columns(self, arrow_table: pa.Table) -> pa.Table:
        """确保Arrow表包含数据库表的所有列，缺失的列用NULL填充。"""
        schema = self._build_tick_arrow_schema()
        existing_columns = set(arrow_table.schema.names)
        new_columns = []
        new_schema = []
        
        for field in schema:
            col_name = field.name
            if col_name in existing_columns:
                new_columns.append(arrow_table.column(col_name))
                new_schema.append((col_name, arrow_table.schema.field(col_name).type))
            else:
                null_array = pa.nulls(arrow_table.num_rows, type=field.type)
                new_columns.append(null_array)
                new_schema.append((col_name, field.type))
        
        return pa.Table.from_arrays(new_columns, schema=pa.schema(new_schema))

    def _build_tick_arrow_schema(self) -> pa.Schema:
        """构建ticks_raw表的显式schema，避免PyArrow类型推断错误。"""
        return pa.schema([
            ('timestamp', pa.timestamp('us')),
            ('instrument_id', pa.string()),
            ('last_price', pa.float64()),
            ('volume', pa.int64()),
            ('open_interest', pa.float64()),
            ('bid_price', pa.float64()),
            ('ask_price', pa.float64()),
            ('date', pa.date32()),
            ('option_type', pa.string()),
            ('strike_price', pa.float64()),
            ('is_otm', pa.bool_()),
            ('sync_status', pa.string()),
            ('future_sync_status', pa.string()),
            ('is_same_rise', pa.bool_()),
            ('is_same_fall', pa.bool_()),
            ('is_diff_sync', pa.bool_()),
        ])

    def _get_ticks_raw_column_names(self) -> List[str]:
        """获取ticks_raw表的所有列名（按顺序）。"""
        return [field.name for field in self._build_tick_arrow_schema()]

    def build_tick_arrow_batch(self, tick_rows: List[Dict], instrument_id: str) -> Optional[pa.Table]:
        """构建完整的 Arrow 表，包含数据库表的所有列。"""
        enriched = self._enrich_tick_option_metadata(tick_rows)
        
        normalized_ticks = []
        for row in enriched:
            ts_str = row.get('ts') or row.get('timestamp')
            if ts_str is None:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_str).split('.')[0]).timestamp()
                dt = datetime.fromtimestamp(ts)
                normalized_ticks.append({
                    'timestamp': dt,
                    'instrument_id': instrument_id,
                    'last_price': row.get('last_price', 0.0),
                    'volume': row.get('volume', 0),
                    'open_interest': float(row.get('open_interest', 0) or 0),
                    'bid_price': row.get('bid_price1'),
                    'ask_price': row.get('ask_price1'),
                    'date': dt.date(),
                    'option_type': row.get('option_type'),
                    'strike_price': row.get('strike_price'),
                    'is_otm': None,
                    'sync_status': None,
                    'future_sync_status': None,
                    'is_same_rise': None,
                    'is_same_fall': None,
                    'is_diff_sync': None,
                })
            except Exception:
                continue
        
        if not normalized_ticks:
            return None
        
        return pa.Table.from_pylist(normalized_ticks, schema=self._build_tick_arrow_schema())

    def merge_arrow_tick_tables(self, tables: List[pa.Table]) -> Optional[pa.Table]:
        if not tables:
            return None
        if len(tables) == 1:
            return self._ensure_arrow_tick_columns(tables[0])
        merged = pa.concat_tables(tables)
        return self._ensure_arrow_tick_columns(merged)

    def merge_tick_task_batch(self, batch, info_callback=None) -> List[Tuple[str, Any, Any]]:
        merged = []
        arrow_batches: Dict[Tuple[int, str], List[pa.Table]] = {}
        
        for func_name, args, kwargs in batch:
            if func_name == '_save_tick_impl' and len(args) >= 3:
                internal_id = args[0]
                instrument_type = args[1]
                tick_data = args[2]
                
                key = (internal_id, instrument_type)
                if key not in arrow_batches:
                    arrow_batches[key] = []
                
                if isinstance(tick_data, pa.Table):
                    arrow_batches[key].append(tick_data)
                elif isinstance(tick_data, list) and tick_data:
                    if info_callback:
                        info = info_callback(internal_id)
                        instrument_id = info.get('instrument_id', '') if info else ''
                    else:
                        instrument_id = ''
                    
                    if instrument_id:
                        arrow_table = self.build_tick_arrow_batch(tick_data, instrument_id)
                        if arrow_table is not None:
                            arrow_batches[key].append(arrow_table)
                    else:
                        logger.warning("[merge_tick_task_batch] internal_id=%d info not found, %d ticks dropped",
                                       internal_id, len(tick_data) if isinstance(tick_data, list) else 1)
            else:
                merged.append((func_name, args, kwargs))
        
        for (internal_id, instrument_type), tables in arrow_batches.items():
            if tables:
                merged_table = self.merge_arrow_tick_tables(tables)
                if merged_table is not None:
                    merged.append(('_save_tick_impl', (internal_id, instrument_type, merged_table), {}))
        
        return merged

    def batch_insert_ticks(self, ticks_data, instrument_id: str = None, use_arrow: bool = True) -> int:
        """
        批量插入 Tick 数据（事务级别优化，统一接口）
        
        Args:
            ticks_data: Tick 数据，可以是：
                - pa.Table: PyArrow表（方案3优化：零拷贝路径）
                - List[Dict]: dict列表（向后兼容，自动转为Arrow）
            instrument_id: 合约ID（当ticks_data为dict列表时需要）
            use_arrow: 是否使用 Arrow 批量插入（默认 True，自动转换）
        
        Returns:
            int: 成功插入的记录数
        """
        conn = self._get_connection()
        
        try:
            if isinstance(ticks_data, pa.Table):
                arrow_table = self._ensure_arrow_tick_columns(ticks_data)
                row_count = arrow_table.num_rows
            elif use_arrow and isinstance(ticks_data, list) and len(ticks_data) > 0:
                if instrument_id:
                    arrow_table = self.build_tick_arrow_batch(ticks_data, instrument_id)
                    if arrow_table is None:
                        return 0
                    arrow_table = self._ensure_arrow_tick_columns(arrow_table)
                else:
                    arrow_table = pa.Table.from_pylist(self._enrich_tick_option_metadata(ticks_data))
                    arrow_table = self._ensure_arrow_tick_columns(arrow_table)
                row_count = arrow_table.num_rows
            else:
                return 0
            
            in_transaction = False
            try:
                conn.execute("BEGIN")
            except Exception:
                in_transaction = True
            
            try:
                conn.register('temp_ticks', arrow_table)
                columns = self._get_ticks_raw_column_names()
                columns_str = ', '.join(columns)
                conn.execute(f"""
                    INSERT INTO ticks_raw ({columns_str})
                    SELECT {columns_str}
                    FROM temp_ticks
                """)
                conn.unregister('temp_ticks')
                if not in_transaction:
                    conn.execute("COMMIT")
            except Exception as e:
                if not in_transaction:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                logger.error(f"Arrow batch insert failed, rolled back: {e}")
                if self._is_fatal_database_error(e):
                    self._mark_connection_unhealthy()
                raise
            
            if row_count >= 100:
                logger.info(f"Batch inserted {row_count} ticks via Arrow")
            return row_count

        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise

        return 0

    def batch_insert_from_cache(self, cache_ticks: List[Dict]) -> int:
        if not cache_ticks:
            return 0
        try:
            conn = self._get_connection()
            conn.execute("BEGIN TRANSACTION")
            try:
                inserted = self.batch_insert_ticks(cache_ticks, use_arrow=True)
                conn.execute("COMMIT")
                logger.info(f"batch_insert_from_cache: {inserted} records committed")
                return inserted
            except Exception as inner:
                conn.execute("ROLLBACK")
                logger.error(f"batch_insert_from_cache transaction rolled back: {inner}")
                return 0
        except Exception as e:
            logger.error(f"batch_insert_from_cache failed: {e}")
            return 0

    def truncate_wal(self) -> None:
        try:
            conn = self._get_connection()
            conn.execute("FORCE CHECKPOINT")
            wal_path = os.path.join(os.path.dirname(self.DB_FILE), f"{os.path.basename(self.DB_FILE)}.wal")
            if os.path.isfile(wal_path):
                wal_size = os.path.getsize(wal_path)
                logger.info(f"DuckDB WAL truncated via FORCE CHECKPOINT, WAL file size={wal_size} bytes")
            else:
                logger.info("DuckDB WAL truncated via FORCE CHECKPOINT (WAL file not present)")
        except Exception as e:
            logger.warning(f"DuckDB WAL truncate failed (non-fatal): {e}")

    def upsert_future_instrument(self, instrument_id: str, product: str, exchange: str,
                                 year_month: str, is_active: bool = True) -> int:
        """
        插入或更新期货合约，同时分配 internal_id。

        Args:
            instrument_id: 合约代码，如 'IF2605'
            product: 品种代码，如 'IF'
            exchange: 交易所代码
            year_month: 年月，如 '2605'
            is_active: 是否活跃

        Returns:
            int: internal_id
        """
        conn = self._get_connection()
        try:
            existing = conn.execute("""
                SELECT internal_id FROM futures_instruments WHERE instrument_id = ?
            """, [instrument_id]).fetchone()
            if existing:
                return existing[0]

            next_id = self._get_next_instrument_id()
            conn.execute("""
                INSERT INTO futures_instruments (internal_id, instrument_id, product, exchange, year_month, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [next_id, instrument_id, product, exchange, year_month, is_active])
            logger.info(f"Upserted future instrument: {instrument_id} with internal_id={next_id}")
            return next_id
        except Exception as e:
            logger.error(f"Failed to upsert future instrument {instrument_id}: {e}")
            raise

    def upsert_option_instrument(self, instrument_id: str, product: str, exchange: str,
                                 underlying_future_id: Optional[int], underlying_product: str,
                                 year_month: str, option_type: str, strike_price: float,
                                 is_active: bool = True) -> int:
        """
        插入或更新期权合约，同时分配 internal_id。

        Args:
            instrument_id: 合约代码，如 'HO2605-C-2800'
            product: 品种代码，如 'HO'
            exchange: 交易所代码
            underlying_future_id: 标的期货的 internal_id（由调用方通过映射提供，符合ID直通原则）
            underlying_product: 标的期货品种，如 'IH'
            year_month: 年月，如 '2605'
            option_type: 'C' 或 'P'
            strike_price: 行权价
            is_active: 是否活跃

        Returns:
            int: internal_id
        """
        conn = self._get_connection()
        try:
            existing = conn.execute("""
                SELECT internal_id, underlying_future_id, underlying_product, year_month, exchange,
                       option_type, strike_price, is_active
                FROM option_instruments WHERE instrument_id = ?
            """, [instrument_id]).fetchone()
            if existing:
                updates = []
                params = []

                if underlying_future_id is not None and existing[1] != underlying_future_id:
                    updates.append("underlying_future_id = ?")
                    params.append(underlying_future_id)

                if underlying_product and existing[2] != underlying_product:
                    updates.append("underlying_product = ?")
                    params.append(underlying_product)

                if year_month and existing[3] != year_month:
                    updates.append("year_month = ?")
                    params.append(year_month)

                if exchange and existing[4] != exchange:
                    updates.append("exchange = ?")
                    params.append(exchange)

                if option_type and existing[5] != option_type:
                    updates.append("option_type = ?")
                    params.append(option_type)

                if strike_price and float(existing[6] or 0.0) != float(strike_price):
                    updates.append("strike_price = ?")
                    params.append(strike_price)

                if bool(existing[7]) != bool(is_active):
                    updates.append("is_active = ?")
                    params.append(is_active)

                if updates:
                    params.append(instrument_id)
                    conn.execute("""
                        UPDATE option_instruments
                        SET {updates_sql}
                        WHERE instrument_id = ?
                    """.replace("{updates_sql}", ", ".join(updates)), params)
                    logger.info(
                        "Updated option instrument from config: %s, underlying_future_id=%s",
                        instrument_id,
                        underlying_future_id,
                    )
                return existing[0]

            next_id = self._get_next_instrument_id()
            conn.execute("""
                INSERT INTO option_instruments (internal_id, instrument_id, product, exchange,
                    underlying_future_id, underlying_product, year_month, option_type, strike_price, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [next_id, instrument_id, product, exchange, underlying_future_id,
                  underlying_product, year_month, option_type, strike_price, is_active])
            logger.info(f"Upserted option instrument: {instrument_id} with internal_id={next_id}, underlying_future_id={underlying_future_id}")
            return next_id
        except Exception as e:
            logger.error(f"Failed to upsert option instrument {instrument_id}: {e}")
            raise

    def _get_next_instrument_id(self) -> int:
        """从全局序列获取下一个 internal_id
        
        P1-3根因修复 + ID直通原则：使用统一的全局序列，确保所有合约ID来源一致
        """
        conn = self._get_connection()
        result = conn.execute("SELECT nextval('instrument_id_seq')").fetchone()
        return result[0]

    def batch_insert_klines(self, klines_data: List[Dict[str, Any]]) -> int:
        """
        批量插入 K 线数据
        
        Args:
            klines_data: K 线数据列表，每个元素为 dict，包含 internal_id, instrument_type, timestamp, open, high, low, close, volume, open_interest, trade_date
        
        Returns:
            int: 成功插入的记录数
        """
        if not klines_data:
            return 0
        
        conn = self._get_connection()
        
        try:
            conn.execute("BEGIN")
            try:
                for kline in klines_data:
                    conn.execute("""
                        INSERT INTO klines_raw (internal_id, instrument_type, timestamp, open, high, low, close, volume, open_interest, trade_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        kline.get('internal_id'),
                        kline.get('instrument_type'),
                        kline.get('timestamp'),
                        kline.get('open'),
                        kline.get('high'),
                        kline.get('low'),
                        kline.get('close'),
                        kline.get('volume', 0),
                        kline.get('open_interest', 0),
                        kline.get('trade_date')
                    ])
                
                conn.execute("COMMIT")
                logger.info(f"Batch inserted {len(klines_data)} klines")
                return len(klines_data)
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                logger.error(f"Kline batch insert failed, rolled back: {e}")
                if self._is_fatal_database_error(e):
                    self._mark_connection_unhealthy()
                raise
        except Exception as e:
            logger.error(f"Batch insert klines failed: {e}")
            raise

    def upsert_future_product(self, product: str, exchange: str, format_template: str,
                              tick_size: float = 0.2, contract_size: float = 1.0,
                              is_active: bool = True) -> bool:
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO future_products (product, exchange, format_template, tick_size, contract_size, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (product) DO UPDATE SET
                exchange = excluded.exchange,
                format_template = excluded.format_template,
                tick_size = excluded.tick_size,
                contract_size = excluded.contract_size,
                is_active = excluded.is_active
            """, [product, exchange, format_template, tick_size, contract_size, is_active])
            logger.info(f"Upserted future product: {product}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert future product {product}: {e}")
            return False

    def upsert_option_product(self, product: str, exchange: str, underlying_product: str,
                              format_template: str, tick_size: float = 0.2,
                              contract_size: float = 1.0, is_active: bool = True) -> bool:
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO option_products (product, exchange, underlying_product, format_template, tick_size, contract_size, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (product) DO UPDATE SET
                exchange = excluded.exchange,
                underlying_product = excluded.underlying_product,
                format_template = excluded.format_template,
                tick_size = excluded.tick_size,
                contract_size = excluded.contract_size,
                is_active = excluded.is_active
            """, [product, exchange, underlying_product, format_template, tick_size, contract_size, is_active])
            logger.info(f"Upserted option product: {product}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert option product {product}: {e}")
            return False

    def incremental_load(self, new_parquet_path: str) -> Dict[str, Any]:
        """
        增量加载新数据（只追加，不覆盖）
        
        Args:
            new_parquet_path: 新 Parquet 文件路径
        
        Returns:
            Dict: 统计信息 {
                'loaded_count': int,
                'skipped_count': int,
                'max_timestamp': datetime
            }
        """
        conn = self._get_connection()
        
        try:
            max_ts_result = conn.execute("""
                SELECT MAX(timestamp) as max_ts FROM ticks_raw
            """).fetchone()
            
            max_ts = max_ts_result[0] if max_ts_result and max_ts_result[0] else None
            
            if not max_ts:
                logger.info("No existing data, performing full load...")
                self._load_or_create_table()
                return {
                    'loaded_count': 0, 
                    'skipped_count': 0, 
                    'max_timestamp': None
                }
            
            logger.info(f"Incremental load from {max_ts}...")
            
            new_data = conn.execute("""
                SELECT * FROM read_parquet(?)
                WHERE timestamp > ?
                ORDER BY timestamp, instrument_id
            """, [new_parquet_path, max_ts]).fetch_arrow_table()
            
            if new_data.num_rows == 0:
                logger.info("No new data to load")
                return {
                    'loaded_count': 0, 
                    'skipped_count': 0, 
                    'max_timestamp': max_ts
                }
            
            new_count = new_data.num_rows
            dict_list = new_data.to_pylist()
            self.batch_insert_ticks(dict_list, use_arrow=True)
            
            new_max_ts = conn.execute("SELECT MAX(timestamp) FROM ticks_raw").fetchone()[0]
            
            logger.info(f"Incremental load completed: {new_count} ticks, max_ts={new_max_ts}")
            
            return {
                'loaded_count': new_count,
                'skipped_count': 0,
                'max_timestamp': new_max_ts
            }
        
        except Exception as e:
            logger.error(f"Incremental load failed: {e}", exc_info=True)
            raise

    def refresh_data(self) -> bool:
        """重新加载 Parquet 数据（会删除现有表）。"""
        with self._lock:
            logger.info("Refreshing data...")
            conn = self._get_connection()
            try:
                conn.execute("DROP TABLE IF EXISTS ticks_raw")
                self._load_or_create_table()
                self._create_indexes_and_views()
                self.clear_cache()
                return True
            except Exception as e:
                logger.error(f"Refresh failed: {e}", exc_info=True)
                return False

    def sync_tick_tables_to_ticks_raw(self, storage_db_path: str = None) -> int:
        """
        将 tick_option_* 和 tick_future_* 分表数据同步到 ticks_raw 统一表
        
        Args:
            storage_db_path: 可选，storage使用的数据库路径。如果不提供，使用默认路径。
        
        Returns:
            int: 同步的记录数
        """
        if not self._tick_sync_lock.acquire(blocking=False):
            logger.info("[SyncTicks] Skip new run because another sync is already in progress")
            return 0

        started_at = time.perf_counter()
        external_conn = None
        logger.info("[SyncTicks] sync_tick_tables_to_ticks_raw called")

        try:
            if storage_db_path:
                import duckdb
                external_conn = duckdb.connect(storage_db_path)
                self._configure_connection(external_conn)
                conn = external_conn
                logger.info(f"[SyncTicks] Using storage database: {storage_db_path}")
            else:
                conn = self._get_connection()
                logger.info("[SyncTicks] Using default DataService connection")

            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name='ticks_raw'").fetchall()
            if not tables:
                logger.info("[SyncTicks] Creating ticks_raw table...")
                conn.execute("""
                    CREATE TABLE ticks_raw (
                        timestamp TIMESTAMP,
                        instrument_id VARCHAR,
                        last_price DOUBLE,
                        volume BIGINT,
                        open_interest DOUBLE,
                        bid_price DOUBLE,
                        ask_price DOUBLE,
                        date DATE,
                        option_type VARCHAR,
                        strike_price DOUBLE,
                        is_otm BOOLEAN,
                        sync_status VARCHAR,
                        future_sync_status VARCHAR,
                        is_same_rise BOOLEAN,
                        is_same_fall BOOLEAN,
                        is_diff_sync BOOLEAN
                    )
                """)
                logger.info("[SyncTicks] ticks_raw table created")

            option_tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE 'tick_option_%'
            """).fetchall()
            logger.info(f"[SyncTicks] Found {len(option_tables)} option tick tables")

            future_tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE 'tick_future_%'
            """).fetchall()
            logger.info(f"[SyncTicks] Found {len(future_tables)} future tick tables")

            total_synced = 0

            if option_tables:
                logger.info(f"[SyncTicks] Syncing {len(option_tables)} option tables...")
                for table in option_tables:
                    table_name = table[0]
                    try:
                        option_data = conn.execute(f"""
                            SELECT 
                                t.timestamp, oi.instrument_id, t.last_price, t.volume, t.open_interest,
                                t.bid_price1 as bid_price, t.ask_price1 as ask_price,
                                CAST(t.timestamp AS DATE) as date,
                                oi.option_type, oi.strike_price,
                                NULL as is_otm, NULL as sync_status, 
                                NULL as future_sync_status, NULL as is_same_rise,
                                NULL as is_same_fall, NULL as is_diff_sync
                            FROM {table_name} t
                            JOIN option_instruments oi ON oi.tick_table = '{table_name}'
                        """).fetch_arrow_table()
                        
                        if option_data.num_rows > 0:
                            dict_list = option_data.to_pylist()
                            self.batch_insert_ticks(dict_list, use_arrow=True)
                            total_synced += option_data.num_rows
                    except Exception as e:
                        logger.error(f"[SyncTicks] Failed to sync {table_name}: {e}")

            if future_tables:
                logger.info(f"[SyncTicks] Syncing {len(future_tables)} future tables...")
                for table in future_tables:
                    table_name = table[0]
                    try:
                        future_data = conn.execute(f"""
                            SELECT 
                                t.timestamp, fi.instrument_id, t.last_price, t.volume, t.open_interest,
                                t.bid_price1 as bid_price, t.ask_price1 as ask_price,
                                CAST(t.timestamp AS DATE) as date,
                                NULL as option_type, NULL as strike_price,
                                NULL as is_otm, NULL as sync_status, 
                                NULL as future_sync_status, NULL as is_same_rise,
                                NULL as is_same_fall, NULL as is_diff_sync
                            FROM {table_name} t
                            JOIN futures_instruments fi ON fi.tick_table = '{table_name}'
                        """).fetch_arrow_table()
                        
                        if future_data.num_rows > 0:
                            dict_list = future_data.to_pylist()
                            self.batch_insert_ticks(dict_list, use_arrow=True)
                            total_synced += future_data.num_rows
                    except Exception as e:
                        logger.error(f"[SyncTicks] Failed to sync {table_name}: {e}")

            logger.info(
                "[SyncTicks] Synced %s records to ticks_raw in %.3fs",
                f"{total_synced:,}",
                time.perf_counter() - started_at,
            )
            return total_synced
        finally:
            if external_conn is not None:
                external_conn.close()
            self._tick_sync_lock.release()
