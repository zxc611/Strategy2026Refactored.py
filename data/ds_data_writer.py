# MODULE_ID: M1-023
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
try:
    import pyarrow as pa
except ImportError:
    pa = None
import logging
import os
import time
import threading
from typing import Optional, List, Dict, Any, Tuple
from datetime import date, datetime

logger = logging.getLogger(__name__)


def _resolve_coverage_trade_date(trade_date: Optional[date] = None) -> date:
    if trade_date is not None:
        return trade_date
    try:
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        return datetime.strptime(ExchangeTime().get_trade_date(), '%Y-%m-%d').date()
    except Exception:
        return date.today()


class DataWriterMixin:
    """数据写入与upsert方法Mixin - 由DataService组合使用"""

    # 共享内存降级连接的写锁：当连接池耗尽降级到内存DB时，所有线程共享同一个raw DuckDB连接，
    # 多线程并发的BEGIN/COMMIT/ROLLBACK会互相干扰，导致register()创建的临时视图丢失。
    # 此锁确保同一时刻只有一个线程在共享内存连接上执行事务。
    _shared_memory_write_lock = threading.Lock()

    # R15-P1-RES-07修复: DB写操作失败重试方法
    # [ID-P1-07-FIX] 批量写入失败重试去重: 已写入行跳过逻辑
    def _db_write_retry(self, write_func, max_retries: int = 3, written_tracker: set = None):
        last_exc = None
        for attempt in range(max_retries):
            try:
                return write_func(skip_rows=written_tracker)
            except Exception as e:
                last_exc = e
                if attempt < max_retries - 1:
                    import time as _t
                    _delay = min(0.5 * (2 ** attempt), 5.0)
                    logging.warning("R15-P1-RES-07: DB写失败第%d次, %.1fs后重试: %s", attempt + 1, _delay, e)
                    _t.sleep(_delay)
        raise last_exc

    def _enrich_tick_option_metadata(self, ticks_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """复用统一解析器，为批量 Tick 插入补齐期权元数据。
        
        修复标准：统一为ParamsService缓存+parse_option降级，删除DB查询
        """
        try:
            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
        except Exception:
            SubscriptionManager = None

        enriched_ticks: List[Dict[str, Any]] = []
        for tick in ticks_data or []:
            tick_row = dict(tick or {})
            tick_row.setdefault('option_type', None)
            tick_row.setdefault('strike_price', None)

            # 字段映射：平台TickData使用bid_price1/ask_price1，DB schema使用bid_price/ask_price
            # build_tick_arrow_batch路径(line 171-172)已做此映射，此处补齐无instrument_id路径
            if 'bid_price' not in tick_row and 'bid_price1' in tick_row:
                tick_row['bid_price'] = tick_row['bid_price1']
            if 'ask_price' not in tick_row and 'ask_price1' in tick_row:
                tick_row['ask_price'] = tick_row['ask_price1']
            if 'bid_volume' not in tick_row and 'bid_volume1' in tick_row:
                tick_row['bid_volume'] = tick_row['bid_volume1']
            if 'ask_volume' not in tick_row and 'ask_volume1' in tick_row:
                tick_row['ask_volume'] = tick_row['ask_volume1']
            for _i in range(2, 6):
                _bp = f'bid_price{_i}'
                _ap = f'ask_price{_i}'
                _bv = f'bid_volume{_i}'
                _av = f'ask_volume{_i}'
                if _bp not in tick_row and f'{_bp}' in tick_row:
                    pass
                if _ap not in tick_row and f'{_ap}' in tick_row:
                    pass
                if _bv not in tick_row and f'{_bv}' in tick_row:
                    pass
                if _av not in tick_row and f'{_av}' in tick_row:
                    pass

            if not tick_row.get('ts') and not tick_row.get('timestamp'):
                dt_val = tick_row.get('datetime')
                if dt_val is not None:
                    try:
                        from datetime import datetime as _dt, timezone as _tz
                        if isinstance(dt_val, (int, float)):
                            tick_row['timestamp'] = _dt.fromtimestamp(dt_val, tz=_tz.utc)
                        elif isinstance(dt_val, _dt):
                            tick_row['timestamp'] = dt_val
                        elif isinstance(dt_val, str):
                            _raw_dt_val = dt_val.strip()
                            if _raw_dt_val.endswith('Z'):
                                _raw_dt_val = _raw_dt_val[:-1] + '+00:00'
                            tick_row['timestamp'] = _dt.fromisoformat(_raw_dt_val)
                    except Exception:
                        pass
            if not tick_row.get('date'):
                try:
                    tick_row['date'] = _resolve_coverage_trade_date()
                except Exception:
                    try:
                        from datetime import datetime as _dt, timezone as _tz
                        _date_src = tick_row.get('timestamp') or tick_row.get('ts') or tick_row.get('datetime')
                        if isinstance(_date_src, _dt):
                            tick_row['date'] = _date_src.date()
                        elif isinstance(_date_src, (int, float)):
                            tick_row['date'] = _dt.fromtimestamp(float(_date_src), tz=_tz.utc).date()
                        elif isinstance(_date_src, str) and _date_src.strip():
                            _raw_date_src = _date_src.strip()
                            if _raw_date_src.endswith('Z'):
                                _raw_date_src = _raw_date_src[:-1] + '+00:00'
                            tick_row['date'] = _dt.fromisoformat(_raw_date_src).date()
                    except Exception:
                        pass

            instrument_id = str(tick_row.get('instrument_id') or '').strip()
            enriched = False
            
            if instrument_id and self.params_service:
                try:
                    info = self.params_service.get_instrument_meta_by_id(instrument_id)
                    if info and info.get('type') == 'option':
                        from ali2026v3_trading.infra.shared_utils import normalize_option_type
                        nt = normalize_option_type(info.get('option_type', ''))
                        tick_row['option_type'] = nt if nt in ('CALL', 'PUT') else None
                        raw_sp = info.get('strike_price')
                        tick_row['strike_price'] = float(raw_sp) if raw_sp is not None else None
                        enriched = True
                except Exception:
                    pass

            if not enriched and instrument_id and SubscriptionManager:
                try:
                    parsed = SubscriptionManager.parse_option(instrument_id)
                    if not tick_row.get('option_type'):
                        tick_row['option_type'] = 'CALL' if parsed.get('option_type') == 'C' else 'PUT'
                    if not tick_row.get('strike_price'):
                        raw_sp = parsed.get('strike_price')
                        tick_row['strike_price'] = float(raw_sp) if raw_sp is not None else None
                except Exception:
                    pass

            ts_raw = tick_row.get('ts') or tick_row.get('timestamp')
            if ts_raw is not None and tick_row.get('date') is None:
                try:
                    from datetime import datetime, timezone
                    ts_str = str(ts_raw)
                    if ts_str.endswith('Z'):
                        ts_str = ts_str[:-1] + '+00:00'
                    dot_idx = ts_str.find('.')
                    if dot_idx >= 0:
                        tz_start = -1
                        for i in range(dot_idx + 1, len(ts_str)):
                            if ts_str[i] in ('+', '-'):
                                tz_start = i
                                break
                        if tz_start >= 0:
                            ts_str = ts_str[:dot_idx] + ts_str[tz_start:]
                        else:
                            ts_str = ts_str[:dot_idx]
                    ts = datetime.fromisoformat(ts_str).timestamp()
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    tick_row['date'] = dt.date()
                except Exception:
                    pass

            enriched_ticks.append(tick_row)

        return enriched_ticks

    @staticmethod
    def _compute_spread_quality(bid_price, ask_price, bid_volume, ask_volume) -> int:
        if bid_price is None or ask_price is None or bid_price <= 0 or ask_price <= 0:
            return 0
        spread = ask_price - bid_price
        if spread < 0:
            return 0
        mid = (bid_price + ask_price) / 2.0
        if mid <= 0:
            return 0
        spread_ratio = spread / mid
        if spread_ratio <= 0.001:
            return 3
        elif spread_ratio <= 0.005:
            return 2
        elif spread_ratio <= 0.02:
            return 1
        return 0

    @staticmethod
    def _compute_days_to_expiry(instrument_id: str, ts=None) -> int:
        try:
            parts = instrument_id.split('-')
            code_part = parts[0]
            year_month = ''
            for c in reversed(code_part):
                if c.isdigit():
                    year_month = c + year_month
                else:
                    break
            if len(year_month) != 4:
                return None
            year = 2000 + int(year_month[:2])
            month = int(year_month[2:])
            if month < 1 or month > 12:
                return None
            from datetime import date, timedelta
            first_day = date(year, month, 1)
            first_friday = first_day
            while first_friday.weekday() != 4:
                first_friday = first_friday + timedelta(days=1)
            third_friday = first_friday + timedelta(days=14)
            if ts is not None:
                from datetime import timezone as _tz
                try:
                    if isinstance(ts, datetime):
                        today = ts.date()
                    elif isinstance(ts, (int, float)):
                        today = datetime.fromtimestamp(ts, tz=_tz.utc).date()
                    else:
                        today = datetime.now().date()
                except Exception:
                    today = datetime.now().date()
            else:
                today = datetime.now().date()
            return (third_friday - today).days
        except Exception:
            return None

    @staticmethod
    def _compute_implied_volatility(last_price, strike_price, days_to_expiry, option_type, underlying_price=None) -> float:
        if last_price is None or strike_price is None or days_to_expiry is None:
            return None
        if last_price <= 0 or strike_price <= 0 or days_to_expiry <= 0:
            return None
        if option_type not in ('CALL', 'PUT'):
            return None
        try:
            from ali2026v3_trading.governance.greeks_calculator import IVCalculator
            S = underlying_price if underlying_price and underlying_price > 0 else last_price * 1.05
            T = days_to_expiry / 365.0
            r = 0.03
            q = 0.0
            iv = IVCalculator.implied_volatility(
                market_price=float(last_price),
                S=float(S),
                K=float(strike_price),
                T=T,
                r=r,
                q=q,
                option_type=option_type,
            )
            return iv if iv and iv > 0 else None
        except Exception:
            return None

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
            ('exchange', pa.string()),
            ('last_price', pa.float64()),
            ('volume', pa.int64()),
            ('open_interest', pa.float64()),
            ('turnover', pa.float64()),
            ('bid_price', pa.float64()),
            ('ask_price', pa.float64()),
            ('bid_volume', pa.int64()),
            ('ask_volume', pa.int64()),
            ('bid_price2', pa.float64()),
            ('ask_price2', pa.float64()),
            ('bid_volume2', pa.int64()),
            ('ask_volume2', pa.int64()),
            ('bid_price3', pa.float64()),
            ('ask_price3', pa.float64()),
            ('bid_volume3', pa.int64()),
            ('ask_volume3', pa.int64()),
            ('bid_price4', pa.float64()),
            ('ask_price4', pa.float64()),
            ('bid_volume4', pa.int64()),
            ('ask_volume4', pa.int64()),
            ('bid_price5', pa.float64()),
            ('ask_price5', pa.float64()),
            ('bid_volume5', pa.int64()),
            ('ask_volume5', pa.int64()),
            ('date', pa.date32()),
            ('option_type', pa.string()),
            ('strike_price', pa.float64()),
            ('is_otm', pa.bool_()),
            ('sync_status', pa.string()),
            ('future_sync_status', pa.string()),
            ('is_same_rise', pa.bool_()),
            ('is_same_fall', pa.bool_()),
            ('is_diff_sync', pa.bool_()),
            ('spread_quality', pa.int32()),
            ('days_to_expiry', pa.int32()),
            ('implied_volatility', pa.float64()),
        ])

    def _get_ticks_raw_column_names(self) -> List[str]:
        """获取ticks_raw表的所有列名（按顺序）。"""
        return [field.name for field in self._build_tick_arrow_schema()]

    def build_tick_arrow_batch(self, tick_rows: List[Dict], instrument_id: str) -> Optional[pa.Table]:
        """构建完整的 Arrow 表，包含数据库表的所有列。"""
        enriched = self._enrich_tick_option_metadata(tick_rows)
        
        normalized_ticks = []
        _normalize_skip_ts = 0
        _normalize_skip_parse = 0
        for row in enriched:
            ts_str = row.get('ts') or row.get('timestamp')
            if ts_str is None:
                _normalize_skip_ts += 1
                continue
            try:
                ts_raw = str(ts_str)
                if ts_raw.endswith('Z'):
                    ts_raw = ts_raw[:-1] + '+00:00'
                dot_idx = ts_raw.find('.')
                if dot_idx >= 0:
                    tz_start = -1
                    for i in range(dot_idx + 1, len(ts_raw)):
                        if ts_raw[i] in ('+', '-'):
                            tz_start = i
                            break
                    if tz_start >= 0:
                        ts_raw = ts_raw[:dot_idx] + ts_raw[tz_start:]
                    else:
                        ts_raw = ts_raw[:dot_idx]
                ts = datetime.fromisoformat(ts_raw).timestamp()
                # R23-P1-10修复: 添加时区参数，确保时间转换一致性
                from datetime import timezone
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                normalized_ticks.append({
                    'timestamp': dt,
                    'instrument_id': instrument_id,
                    'exchange': row.get('exchange'),
                    'last_price': row.get('last_price', 0.0),
                    'volume': row.get('volume', 0),
                    'open_interest': float(row.get('open_interest', 0) or 0),
                    'turnover': row.get('turnover'),
                    'bid_price': row.get('bid_price1'),
                    'ask_price': row.get('ask_price1'),
                    'bid_volume': row.get('bid_volume1'),
                    'ask_volume': row.get('ask_volume1'),
                    'bid_price2': row.get('bid_price2'),
                    'ask_price2': row.get('ask_price2'),
                    'bid_volume2': row.get('bid_volume2'),
                    'ask_volume2': row.get('ask_volume2'),
                    'bid_price3': row.get('bid_price3'),
                    'ask_price3': row.get('ask_price3'),
                    'bid_volume3': row.get('bid_volume3'),
                    'ask_volume3': row.get('ask_volume3'),
                    'bid_price4': row.get('bid_price4'),
                    'ask_price4': row.get('ask_price4'),
                    'bid_volume4': row.get('bid_volume4'),
                    'ask_volume4': row.get('ask_volume4'),
                    'bid_price5': row.get('bid_price5'),
                    'ask_price5': row.get('ask_price5'),
                    'bid_volume5': row.get('bid_volume5'),
                    'ask_volume5': row.get('ask_volume5'),
                    'date': _resolve_coverage_trade_date(),
                    'option_type': row.get('option_type'),
                    'strike_price': row.get('strike_price'),
                    'is_otm': None,
                    'sync_status': None,
                    'future_sync_status': None,
                    'is_same_rise': None,
                    'is_same_fall': None,
                    'is_diff_sync': None,
                    'spread_quality': self._compute_spread_quality(
                        row.get('bid_price1'), row.get('ask_price1'),
                        row.get('bid_volume1'), row.get('ask_volume1')),
                    'days_to_expiry': self._compute_days_to_expiry(instrument_id, dt),
                    'implied_volatility': None,
                })
                last_tick = normalized_ticks[-1]
                if last_tick.get('option_type') and last_tick.get('strike_price') and last_tick.get('days_to_expiry') is not None:
                    last_tick['implied_volatility'] = self._compute_implied_volatility(
                        row.get('last_price', 0.0), last_tick['strike_price'],
                        last_tick['days_to_expiry'], last_tick['option_type'])
            except Exception:
                _normalize_skip_parse += 1
                continue
        if _normalize_skip_ts > 0 or _normalize_skip_parse > 0:
            if not hasattr(self, '_normalize_skip_logged'):
                self._normalize_skip_logged = True
                logging.warning("[FIX-20260701] build_tick_arrow_batch normalize skip: no_ts=%d parse_fail=%d inst=%s total_rows=%d",
                                _normalize_skip_ts, _normalize_skip_parse, instrument_id, len(enriched))
        
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
                        # FIX-P0-17: info未找到时保留原task，通过_save_tick_impl路径延迟解析instrument_id
                        merged.append((func_name, args, kwargs))
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
        批量插入 Tick 数据（事务级别优化，统一接口）'
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
            _is_shared_memory = not (hasattr(conn, '_execute_with_timeout') and hasattr(conn, '_conn'))
            if _is_shared_memory:
                if not hasattr(self, '_shared_memory_warned'):
                    self._shared_memory_warned = True
                    logging.error("[DATA_PATH_DIAG] batch_insert_ticks using SHARED MEMORY (in-memory DB)! DB_FILE=%s conn_type=%s", getattr(self, 'DB_FILE', '?'), type(conn).__name__)
                DataWriterMixin._shared_memory_write_lock.acquire()
            
            _view_name = f'temp_ticks_{threading.get_ident()}'
            columns = self._get_ticks_raw_column_names()
            columns_str = ', '.join(columns)
            
            try:
                conn.unregister(_view_name)
            except Exception:
                pass
            
            try:
                if isinstance(ticks_data, pa.Table):
                    conn.register(_view_name, ticks_data)
                else:
                    conn.register(_view_name, arrow_table)
            except Exception as _reg_err:
                logging.error("[DataWriter] register %s failed: %s", _view_name, _reg_err)
                if _is_shared_memory:
                    DataWriterMixin._shared_memory_write_lock.release()
                return 0
            
            try:
                conn.execute("BEGIN")
            except Exception:
                in_transaction = True
            
            try:
                conn.execute(f"""
                    INSERT INTO ticks_raw ({columns_str})
                    SELECT {columns_str}
                    FROM {_view_name}
                    ON CONFLICT (instrument_id, timestamp) DO NOTHING
                """)
            except Exception as _conflict_err:
                if 'UNIQUE' in str(_conflict_err) or 'CONSTRAINT' in str(_conflict_err) or 'INDEX' in str(_conflict_err):
                    if not in_transaction:
                        try:
                            conn.execute("ROLLBACK")
                        except Exception:
                            pass
                        conn.execute("BEGIN")
                    conn.execute(f"""
                        INSERT INTO ticks_raw ({columns_str})
                        SELECT {columns_str}
                        FROM {_view_name}
                    """)
                else:
                    raise
            
            if not in_transaction:
                conn.execute("COMMIT")

            # FIX-20260703: 移除 changes() 调用 — DuckDB 不支持此 SQLite 函数
            # 根因: conn.execute("SELECT changes()") 在 DuckDB 上抛
            #   "Catalog Error: Scalar Function with name changes does not exist!"
            # 该错误通过 _execute_with_timeout 传播，虽被 except Exception: pass 捕获，
            # 但 _execute_with_timeout 内部可能将连接标记为不健康(取决于错误消息内容)，
            # 导致连接被销毁重建 → 新连接上 ticks_raw/latest_prices 不存在 → 恶性循环
            # DuckDB 无直接等效函数，使用 row_count 作为实际插入行数（ON CONFLICT DO NOTHING
            # 跳过的重复行不影响 db_insert_zero 探针的正确触发，因为 row_count=0 时探针仍会触发）
            _actual_inserted = row_count
            try:
                _after_count = conn.execute(f"SELECT COUNT(*) FROM ticks_raw WHERE instrument_id = (SELECT instrument_id FROM {_view_name} LIMIT 1) AND date = (SELECT date FROM {_view_name} LIMIT 1)").fetchone()[0]
            except Exception:
                _after_count = -1
            
            try:
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                _sample_inst = ''
                _sample_price = 0.0
                if isinstance(ticks_data, list) and ticks_data:
                    _sample_inst = ticks_data[0].get('instrument_id', '') if isinstance(ticks_data[0], dict) else ''
                    # FIX-P1-18: 原代码第三参数硬编码0.0，导致探针样本无法反映真实tick价格
                    # 修复: 从tick数据中提取真实last_price
                    _sample_price = float(ticks_data[0].get('last_price', 0.0)) if isinstance(ticks_data[0], dict) else 0.0
                record_tick_probe('arrow_build_ok', _sample_inst, _sample_price, f'rows={row_count} shared_mem={_is_shared_memory} db={getattr(self, "DB_FILE", "?")[-30:]}')
            except Exception:
                pass
            if row_count >= 100:
                logger.info(f"Batch inserted {_actual_inserted}/{row_count} ticks via Arrow (actual/batch)")
            # FIX-P1-NEW-02: 返回实际插入行数，使下游 db_insert_zero 探针可正确触发
            return _actual_inserted
        except Exception as e:
            try:
                from ali2026v3_trading.infra.health_monitor import record_tick_probe
                _sample_inst2 = ''
                if isinstance(ticks_data, list) and ticks_data:
                    _sample_inst2 = ticks_data[0].get('instrument_id', '') if isinstance(ticks_data[0], dict) else ''
                record_tick_probe('arrow_build_fail', _sample_inst2, 0.0, f'{type(e).__name__}: {e}')
            except Exception:
                pass
            if not in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
            logger.error(f"Arrow batch insert failed, rolled back: {e}")
            if 'does not exist' in str(e) and 'ticks_raw' in str(e):
                try:
                    logger.info("[AUTO-REPAIR] ticks_raw表不存在，尝试自动重建...")
                    self._create_empty_table(conn)
                    self._ensure_ticks_raw_schema(conn)
                    logger.info("[AUTO-REPAIR] ticks_raw表已重建，下次写入将成功")
                except Exception as _repair_err:
                    logger.error("[AUTO-REPAIR] ticks_raw表重建失败: %s", _repair_err)
            if self._is_fatal_database_error(e):
                self._mark_connection_unhealthy()
                try:
                    self._handle_fatal_db_error(e)
                except Exception as hfe:
                    logger.warning("[R30-P0-08] 降级处理失败: %s", hfe)
            raise
        finally:
            try:
                conn.unregister(_view_name)
            except Exception:
                pass
            if _is_shared_memory:
                DataWriterMixin._shared_memory_write_lock.release()
            self._return_connection(conn)

    def batch_insert_from_cache(self, cache_ticks: List[Dict]) -> int:
        if not cache_ticks:
            return 0
        try:
            conn = self._get_connection()
            try:
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
            finally:
                self._return_connection(conn)
        except Exception as e:
            logger.error(f"batch_insert_from_cache failed: {e}")
            return 0

    def truncate_wal(self) -> None:
        try:
            conn = self._get_connection()
            try:
                conn.execute("FORCE CHECKPOINT")
                wal_path = os.path.join(os.path.dirname(self.DB_FILE), f"{os.path.basename(self.DB_FILE)}.wal")
                if os.path.isfile(wal_path):
                    wal_size = os.path.getsize(wal_path)
                    logger.info(f"DuckDB WAL truncated via FORCE CHECKPOINT, WAL file size={wal_size} bytes")
                else:
                    logger.info("DuckDB WAL truncated via FORCE CHECKPOINT (WAL file not present)")
            finally:
                self._return_connection(conn)
        except Exception as e:
            logger.warning(f"DuckDB WAL truncate failed (non-fatal): {e}")

    def ensure_config_coverage_for_today(self, trade_date: Optional[date] = None) -> Dict[str, int]:
        """为配置全集写入带标记的模拟覆盖行，并记录配置->落库审计。"""
        target_date = _resolve_coverage_trade_date(trade_date)
        target_ts = datetime.combine(target_date, datetime.min.time())
        conn = self._get_connection()
        try:
            conn.execute("BEGIN")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS chain_coverage_audit (
                    audit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trade_date DATE,
                    config_count BIGINT,
                    subscription_confirmed_count BIGINT,
                    tick_return_count BIGINT,
                    tick_buffer_count BIGINT,
                    ticks_raw_count BIGINT,
                    klines_raw_count BIGINT,
                    active_tick_count BIGINT,
                    simulated_coverage_tick_count BIGINT,
                    simulated_coverage_kline_count BIGINT,
                    notes VARCHAR
                )
            """)

            _prev_date = target_date - __import__('datetime').timedelta(days=1)
            _tables = [r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
            _has_registry = 'instruments_registry' in _tables
            _has_ticks = 'ticks_raw' in _tables
            _has_klines = 'klines_raw' in _tables
            if _has_ticks:
                before_tick_actual = conn.execute("""
                    SELECT COUNT(DISTINCT instrument_id)
                    FROM ticks_raw
                    WHERE date IN (?, ?) AND COALESCE(sync_status, '') <> 'simulated_coverage'
                """, [target_date, _prev_date]).fetchone()[0]
            else:
                before_tick_actual = 0

            if _has_registry and _has_ticks:
                try:
                    conn.execute("""
                        INSERT INTO ticks_raw (
                            timestamp, instrument_id, exchange, last_price, volume, open_interest,
                            turnover, bid_price, ask_price, bid_volume, ask_volume, date,
                            option_type, strike_price, sync_status, future_sync_status
                        )
                        SELECT ?, r.instrument_id, r.exchange, NULL, 0, NULL,
                               NULL, NULL, NULL, 0, 0, ?,
                               r.option_type, r.strike_price, 'simulated_coverage', 'simulated_coverage'
                        FROM instruments_registry r
                        WHERE NOT EXISTS (
                            SELECT 1 FROM ticks_raw t
                            WHERE t.instrument_id = r.instrument_id AND t.date IN (?, ?)
                        )
                        ON CONFLICT (instrument_id, timestamp) DO NOTHING
                    """, [target_ts, target_date, target_date, _prev_date])
                except Exception as _tick_ins_err:
                    logger.debug("[ensure_config_coverage] ticks_raw simulated_coverage insert skipped: %s", _tick_ins_err)

            if _has_registry and _has_klines:
                try:
                    conn.execute("""
                        INSERT INTO klines_raw (
                            internal_id, instrument_type, timestamp, open, high, low, close,
                            volume, open_interest, trade_date, period
                        )
                        SELECT r.internal_id,
                               CASE WHEN r.option_type IS NULL THEN 'future' ELSE 'option' END,
                               ?, NULL, NULL, NULL, NULL, 0, NULL, ?, 'M1'
                        FROM instruments_registry r
                        WHERE r.internal_id IS NOT NULL
                          AND NOT EXISTS (
                            SELECT 1 FROM klines_raw k
                            WHERE k.internal_id = r.internal_id AND k.trade_date = ? AND k.period = 'M1'
                          )
                        ON CONFLICT (internal_id, timestamp, period) DO NOTHING
                    """, [target_ts, target_date, target_date])
                except Exception as _kline_ins_err:
                    logger.debug("[ensure_config_coverage] klines_raw simulated_coverage insert skipped: %s", _kline_ins_err)

            if _has_registry:
                _config_count_sql = "SELECT COUNT(*) FROM instruments_registry"
            elif 'futures_instruments' in _tables and 'option_instruments' in _tables:
                _config_count_sql = "SELECT (SELECT COUNT(*) FROM futures_instruments) + (SELECT COUNT(*) FROM option_instruments)"
            else:
                _config_count_sql = "SELECT 0"

            _params2 = []
            _subsqls2 = []
            _subsqls2.append(f"({_config_count_sql})")
            if _has_ticks:
                _subsqls2.append("(SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date IN (?, ?))")
                _params2.extend([target_date, _prev_date])
            else:
                _subsqls2.append("(SELECT 0)")
            if _has_klines:
                _subsqls2.append("(SELECT COUNT(DISTINCT internal_id) FROM klines_raw WHERE trade_date = ?)")
                _params2.append(target_date)
            else:
                _subsqls2.append("(SELECT 0)")
            if _has_ticks:
                _subsqls2.append("(SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date IN (?, ?) AND sync_status = 'simulated_coverage')")
                _params2.extend([target_date, _prev_date])
            else:
                _subsqls2.append("(SELECT 0)")
            if _has_klines:
                _subsqls2.append("(SELECT COUNT(DISTINCT internal_id) FROM klines_raw WHERE trade_date = ? AND timestamp = ?)")
                _params2.extend([target_date, target_ts])
            else:
                _subsqls2.append("(SELECT 0)")

            counts = conn.execute(f"SELECT {', '.join(_subsqls2)}", _params2).fetchone()
            config_count, ticks_raw_count, klines_raw_count, simulated_ticks, simulated_klines = [int(v or 0) for v in counts]

            conn.execute("""
                INSERT INTO chain_coverage_audit (
                    trade_date, config_count, subscription_confirmed_count,
                    tick_return_count, tick_buffer_count, ticks_raw_count, klines_raw_count,
                    active_tick_count, simulated_coverage_tick_count, simulated_coverage_kline_count, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                target_date, config_count, config_count,
                int(before_tick_actual or 0), int(before_tick_actual or 0), ticks_raw_count, klines_raw_count,
                int(before_tick_actual or 0), simulated_ticks, simulated_klines,
                'simulated_coverage rows are excluded from latest_prices/daily aggregate views',
            ])
            conn.execute("COMMIT")
            return {
                'config_count': config_count,
                'subscription_confirmed_count': config_count,
                'tick_return_count': int(before_tick_actual or 0),
                'tick_buffer_count': int(before_tick_actual or 0),
                'ticks_raw_count': ticks_raw_count,
                'klines_raw_count': klines_raw_count,
                'simulated_coverage_tick_count': simulated_ticks,
                'simulated_coverage_kline_count': simulated_klines,
            }
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            logger.error("ensure_config_coverage_for_today failed: %s", e, exc_info=True)
            raise
        finally:
            self._return_connection(conn)

    # FIX-M1: tick_return冻结修复 — 运行期定时刷新chain_coverage_audit
    # 根因: ensure_config_coverage_for_today 是唯一写入审计表的函数，仅启动时调用一次，
    #       导致 health_monitor 永远读到同一行 (tick_return_count = 启动时快照)。
    # 修复: 新增 refresh_chain_coverage_audit() 由周期诊断每30秒调用一次，
    #       重算 ticks_raw/klines_raw 真实合约数并插入新审计行。
    def refresh_chain_coverage_audit(self, trade_date: Optional[date] = None) -> Dict[str, int]:
        """运行期刷新 chain_coverage_audit 表，记录当前真实覆盖状态。

        与 ensure_config_coverage_for_today 的差异:
        - 不写 simulated_coverage 占位行（启动期已写过）
        - 只统计真实 tick/kline 数（排除 simulated_coverage）
        - 每次调用插入新行，使 health_monitor 读到最新值而非启动快照
        """
        target_date = _resolve_coverage_trade_date(trade_date)
        conn = self._get_connection()
        try:
            conn.execute("BEGIN")

            # 防御: 审计表缺失时重建
            conn.execute("""
                CREATE TABLE IF NOT EXISTS chain_coverage_audit (
                    audit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trade_date DATE,
                    config_count BIGINT,
                    subscription_confirmed_count BIGINT,
                    tick_return_count BIGINT,
                    tick_buffer_count BIGINT,
                    ticks_raw_count BIGINT,
                    klines_raw_count BIGINT,
                    active_tick_count BIGINT,
                    simulated_coverage_tick_count BIGINT,
                    simulated_coverage_kline_count BIGINT,
                    notes VARCHAR
                )
            """)

            _prev_date = target_date - __import__('datetime').timedelta(days=1)
            _tables = [r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
            # FIX-20260706-INSTR-REGISTRY: information_schema可能返回stale metadata导致
            # instruments_registry查询失败(Catalog Error)。改为优先使用futures_instruments+
            # option_instruments(由ds_schema_manager创建的实际表)，仅在其不存在时回退
            if 'futures_instruments' in _tables and 'option_instruments' in _tables:
                _config_count_sql = "SELECT (SELECT COUNT(*) FROM futures_instruments) + (SELECT COUNT(*) FROM option_instruments)"
            elif 'futures_instruments' in _tables:
                _config_count_sql = "SELECT COUNT(*) FROM futures_instruments"
            elif 'instruments_registry' in _tables:
                _config_count_sql = "SELECT COUNT(*) FROM instruments_registry"
            else:
                _config_count_sql = "SELECT 0"
            _has_ticks = 'ticks_raw' in _tables
            _has_klines = 'klines_raw' in _tables
            _params = []
            _subsqls = []
            _subsqls.append(f"({_config_count_sql})")
            if _has_ticks:
                _subsqls.append("(SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date IN (?, ?))")
                _params.extend([target_date, _prev_date])
            else:
                _subsqls.append("(SELECT 0)")
            if _has_ticks:
                _subsqls.append("(SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date IN (?, ?) AND COALESCE(sync_status, '') <> 'simulated_coverage')")
                _params.extend([target_date, _prev_date])
            else:
                _subsqls.append("(SELECT 0)")
            if _has_ticks:
                _subsqls.append("(SELECT COUNT(DISTINCT instrument_id) FROM ticks_raw WHERE date IN (?, ?) AND sync_status = 'simulated_coverage')")
                _params.extend([target_date, _prev_date])
            else:
                _subsqls.append("(SELECT 0)")
            if _has_klines:
                _subsqls.append("(SELECT COUNT(DISTINCT internal_id) FROM klines_raw WHERE trade_date = ?)")
                _params.append(target_date)
            else:
                _subsqls.append("(SELECT 0)")
            if _has_klines:
                _subsqls.append("(SELECT COUNT(DISTINCT internal_id) FROM klines_raw WHERE trade_date = ? AND NOT (open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL AND volume = 0))")
                _params.append(target_date)
            else:
                _subsqls.append("(SELECT 0)")
            counts = conn.execute(f"SELECT {', '.join(_subsqls)}", _params).fetchone()
            (config_count, ticks_raw_count, real_tick_count,
             simulated_ticks, klines_raw_count, real_kline_count) = [int(v or 0) for v in counts]

            conn.execute("""
                INSERT INTO chain_coverage_audit (
                    trade_date, config_count, subscription_confirmed_count,
                    tick_return_count, tick_buffer_count, ticks_raw_count, klines_raw_count,
                    active_tick_count, simulated_coverage_tick_count, simulated_coverage_kline_count, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                target_date, config_count, config_count,
                real_tick_count, real_tick_count, ticks_raw_count, klines_raw_count,
                real_tick_count, simulated_ticks, max(klines_raw_count - real_kline_count, 0),
                'FIX-M1: runtime refresh (real_tick_count from ticks_raw excluding simulated_coverage)',
            ])
            conn.execute("COMMIT")
            return {
                'config_count': config_count,
                'subscription_confirmed_count': config_count,
                'tick_return_count': real_tick_count,
                'tick_buffer_count': real_tick_count,
                'ticks_raw_count': ticks_raw_count,
                'klines_raw_count': klines_raw_count,
                'real_tick_count': real_tick_count,
                'real_kline_count': real_kline_count,
                'simulated_coverage_tick_count': simulated_ticks,
            }
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            conn._unhealthy = True
            logger.warning("[FIX-M1] refresh_chain_coverage_audit failed (non-fatal): %s", e)
            return {}
        finally:
            self._return_connection(conn)

    def upsert_future_instrument(self, instrument_id: str, product: str, exchange: str,
                                 year_month: str, is_active: bool = True,
                                 format_template: str = None,
                                 expire_date: str = None, listing_date: str = None) -> int:
        conn = self._get_connection()
        try:
            existing = conn.execute("""
                SELECT internal_id FROM futures_instruments WHERE instrument_id = ?
            """, [instrument_id]).fetchone()
            if existing:
                return existing[0]

            next_id = self._get_next_instrument_id()
            kline_table = f"kline_future_{next_id}"
            tick_table = f"tick_future_{next_id}"
            product_code = product.lower() if product else None
            from ali2026v3_trading.infra.shared_utils import ShardRouter
            shard_key = ShardRouter._deterministic_hash(product_code) if product_code else None
            conn.execute("""
                INSERT INTO futures_instruments (internal_id, instrument_id, product, exchange, year_month,
                    format, expire_date, listing_date, kline_table, tick_table, product_code, shard_key, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [next_id, instrument_id, product, exchange, year_month,
                  format_template, expire_date, listing_date, kline_table, tick_table,
                  product_code, shard_key, is_active])
            logger.info(f"Upserted future instrument: {instrument_id} with internal_id={next_id}")
            return next_id
        except Exception as e:
            logger.error(f"Failed to upsert future instrument {instrument_id}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def upsert_option_instrument(self, instrument_id: str, product: str, exchange: str,
                                 underlying_future_id: Optional[int], underlying_product: str,
                                 year_month: str, option_type: str, strike_price: float,
                                 is_active: bool = True,
                                 format_template: str = None,
                                 expire_date: str = None, listing_date: str = None) -> int:
        """
        插入或更新期权合约，同时分配 internal_id。

        Args:
            instrument_id: 合约代码，如 'HO2605-C-2800'
            product: 品种代码，如 'HO'
            exchange: 交易所代码
            underlying_future_id: 标的期货的 internal_id（由调用方通过映射提供，符合ID直通原则）'
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

                if strike_price and (existing[6] is None or float(existing[6]) != float(strike_price)):
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
            kline_table = f"kline_option_{next_id}"
            tick_table = f"tick_option_{next_id}"
            product_code = product.lower() if product else None
            from ali2026v3_trading.infra.shared_utils import ShardRouter
            shard_key = ShardRouter._deterministic_hash(product_code) if product_code else None
            conn.execute("""
                INSERT INTO option_instruments (internal_id, instrument_id, product, exchange,
                    underlying_future_id, underlying_product, year_month, option_type, strike_price,
                    format, expire_date, listing_date, kline_table, tick_table, product_code, shard_key, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [next_id, instrument_id, product, exchange, underlying_future_id,
                  underlying_product, year_month, option_type, strike_price,
                  format_template, expire_date, listing_date, kline_table, tick_table,
                  product_code, shard_key, is_active])
            logger.info(f"Upserted option instrument: {instrument_id} with internal_id={next_id}, underlying_future_id={underlying_future_id}")
            return next_id
        except Exception as e:
            logger.error(f"Failed to upsert option instrument {instrument_id}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def _get_next_instrument_id(self) -> int:
        conn = self._get_connection()
        try:
            result = conn.execute("SELECT nextval('instrument_id_seq')").fetchone()
            return result[0]
        finally:
            self._return_connection(conn)

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
                        INSERT INTO klines_raw (internal_id, instrument_type, timestamp, open, high, low, close, volume, open_interest, trade_date, period)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (internal_id, timestamp, period) DO NOTHING
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
                        kline.get('trade_date'),
                        kline.get('period', 'M1'),
                    ])

                conn.execute("COMMIT")
                logger.info(f"Batch inserted {len(klines_data)} klines (with ON CONFLICT dedup)")
                return len(klines_data)
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                logger.error(f"Kline batch insert failed, rolled back: {e}")
                if 'does not exist' in str(e) and 'klines_raw' in str(e):
                    try:
                        logger.info("[AUTO-REPAIR] klines_raw表不存在，尝试自动重建...")
                        conn.execute("""
                            CREATE TABLE IF NOT EXISTS klines_raw (
                                internal_id BIGINT,
                                instrument_type VARCHAR,
                                timestamp TIMESTAMP,
                                open DOUBLE,
                                high DOUBLE,
                                low DOUBLE,
                                close DOUBLE,
                                volume BIGINT,
                                open_interest DOUBLE,
                                trade_date DATE,
                                period VARCHAR DEFAULT 'M1'
                            )
                        """)
                        logger.info("[AUTO-REPAIR] klines_raw表已重建，下次写入将成功")
                    except Exception as _repair_err:
                        logger.error("[AUTO-REPAIR] klines_raw表重建失败: %s", _repair_err)
                if self._is_fatal_database_error(e):
                    self._mark_connection_unhealthy()
                    try:
                        self._handle_fatal_db_error(e)
                    except Exception as hfe:
                        logger.warning("[R30-P0-08] 降级处理失败: %s", hfe)
                raise
        except Exception as e:
            logger.error(f"Batch insert klines failed: {e}")
            raise
        finally:
            self._return_connection(conn)

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
        finally:
            self._return_connection(conn)

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
        finally:
            self._return_connection(conn)

    def incremental_load(self, new_parquet_path: str) -> Dict[str, Any]:
        """
        增量加载新数据（只追加，不覆盖）'
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
        finally:
            self._return_connection(conn)

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
            finally:
                self._return_connection(conn)

    def sync_tick_tables_to_ticks_raw(self, storage_db_path: str = None) -> int:
        """
        将 tick_option_* 和 tick_future_* 分表数据同步到 ticks_raw 统一表
        
        Args:
            storage_db_path: 可选，storage使用的数据库路径。如果不提供，使用默认路径。'
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
                # FIX-20260702: 禁止业务层裸 duckdb.connect；同库复用 DataService 连接，外库才走 db_adapter
                storage_db_path = os.path.normpath(storage_db_path)
                if storage_db_path == os.path.normpath(getattr(self, 'DB_FILE', '')):
                    conn = self._get_connection()
                    logger.info(f"[SyncTicks] Reusing DataService connection for same DB: {storage_db_path}")
                else:
                    from ali2026v3_trading.data.db_adapter import connect as _db_connect
                    external_conn = _db_connect(storage_db_path)
                    self._configure_connection(external_conn)
                    conn = external_conn
                    logger.info(f"[SyncTicks] Using external storage database via db_adapter: {storage_db_path}")
            else:
                conn = self._get_connection()
                logger.info("[SyncTicks] Using default DataService connection")

            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name='ticks_raw'").fetchall()
            if not tables:
                logger.info("[SyncTicks] Creating ticks_raw table...")
                from ali2026v3_trading.data.ds_schema_manager import get_ticks_raw_create_sql
                conn.execute(get_ticks_raw_create_sql())
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
            else:
                self._return_connection(conn)
            self._tick_sync_lock.release()

    # ========== 历史K线加载支持方法 ==========
    # 平台规范：load_historical_klines_with_stop 需要storage对象提供以下方法
    # 这些方法原本定义在未使用的StorageQuery层次上，此处补齐到DataWriterMixin使DataService可用

    @property
    def batch_size(self) -> int:
        """历史K线批量写入大小"""
        return getattr(self, '_kline_batch_size', 500)

    @staticmethod
    def _resolve_kline_provider(market_center: Any) -> tuple:
        """解析K线数据提供者，返回 (provider, provider_type)"""
        if market_center is None:
            return (None, 'none')
        if hasattr(market_center, 'get_kline_data') and callable(getattr(market_center, 'get_kline_data')):
            return (market_center, 'get_kline_data')
        if hasattr(market_center, 'get_kline') and callable(getattr(market_center, 'get_kline')):
            return (market_center, 'get_kline')
        return (None, 'unavailable')

    @staticmethod
    def _normalize_kline_period(period: str) -> str:
        """将平台K线周期(M1/M5/M10)转换为内部表示(1min/5min/10min)"""
        if not period:
            return '1min'
        p = str(period).strip().upper()
        if p.startswith('M') and p[1:].isdigit():
            return f"{p[1:]}min"
        return p.lower()

    def _fetch_historical_kline_data(self, provider, exchange, instrument_id,
                                      kline_style, history_minutes, start_time, end_time):
        """通过平台MarketCenter获取历史K线数据"""
        if provider is None:
            return []
        start_time_dt = start_time.replace(tzinfo=None) if hasattr(start_time, 'tzinfo') and start_time.tzinfo else start_time
        end_time_dt = end_time.replace(tzinfo=None) if hasattr(end_time, 'tzinfo') and end_time.tzinfo else end_time
        start_time_str = start_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        try:
            provider_type = self._resolve_kline_provider(provider)[1]
            if provider_type == 'get_kline_data':
                kline_data = provider.get_kline_data(
                    exchange=exchange, instrument_id=instrument_id,
                    style=kline_style, start_time=start_time_str, end_time=end_time_str,
                )
                return list(kline_data) if kline_data else []
            elif provider_type == 'get_kline':
                kline_data = provider.get_kline(
                    exchange=exchange, instrument_id=instrument_id,
                    style=kline_style, start_time=start_time_str, end_time=end_time_str,
                )
                return list(kline_data) if kline_data else []
        except (TypeError, AttributeError):
            try:
                if provider_type == 'get_kline_data':
                    kline_data = provider.get_kline_data(
                        exchange=exchange, instrument=instrument_id,
                        style=kline_style, count=-1440,
                    )
                    return list(kline_data) if kline_data else []
            except Exception as e:
                logger.debug(f"[_fetch_historical_kline_data] {instrument_id}: {e}")
        except Exception as e:
            logger.debug(f"[_fetch_historical_kline_data] {instrument_id}: {e}")
        return []

    @staticmethod
    def _to_timestamp(dt_value: Any) -> Optional[float]:
        """将各种时间格式转换为timestamp(float)"""
        if dt_value is None:
            return None
        if isinstance(dt_value, (int, float)):
            return float(dt_value)
        try:
            from datetime import datetime as _dt
            if isinstance(dt_value, _dt):
                return dt_value.timestamp()
            s = str(dt_value).replace('T', ' ')
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d'):
                try:
                    return _dt.strptime(s, fmt).timestamp()
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    @staticmethod
    def infer_exchange_from_id(instrument_id: str) -> str:
        """从合约代码推断交易所"""
        iid = str(instrument_id or '').strip()
        if not iid:
            return ''
        iid_lower = iid.lower()
        iid_upper = iid.upper()
        # 期货合约代码规则（大小写不敏感匹配）
        # 注意：CZCE必须在DCE之前检查，因为CZCE的'AP'前缀小写后为'ap'，
        # 会被DCE的'a'前缀错误匹配；同理'MA'会被'm'匹配
        if iid_upper.startswith(('IF', 'IC', 'IH', 'IM', 'TF', 'TS')):
            return 'CFFEX'
        if iid_upper == 'T' or (iid_upper.startswith('T') and iid_upper[1:2].isdigit()):
            return 'CFFEX'
        if iid_lower.startswith(('cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag', 'rb', 'wr', 'hc', 'ss', 'lu', 'bc', 'ao')):
            return 'SHFE'
        if iid_upper.startswith(('AP', 'CF', 'CY', 'CJ', 'SR', 'TA', 'MA', 'OI', 'RM', 'FG', 'SF', 'SM', 'SA', 'UR', 'PF', 'PK', 'PX', 'SH', 'EC')):
            return 'CZCE'
        if iid_lower.startswith(('m', 'y', 'a', 'b', 'p', 'c', 'cs', 'jd', 'rr', 'l', 'v', 'pp', 'j', 'jm', 'i', 'eg', 'eb', 'pg')):
            return 'DCE'
        if iid_lower.startswith(('sc', 'lu', 'bc', 'nr', 'ao', 'ec', 'eu')):
            return 'INE'
        if iid_lower.startswith(('si', 'lc')):
            return 'GFEX'
        return ''

    def _get_info_internal_id(self, info: dict) -> int:
        """从合约信息dict中提取internal_id"""
        if not info:
            return 0
        iid = info.get('internal_id') or info.get('id') or 0
        try:
            return int(iid) if iid is not None else 0
        except (ValueError, TypeError):
            return 0

    def _wait_for_queue_capacity(self, max_fill_rate: float = 60.0, timeout_sec: float = 5.0, source: str = '') -> None:
        """等待队列容量（直接写入模式，无需等待）"""
        return

    def _ensure_direct_queue_stats(self) -> Dict[str, int]:
        if not hasattr(self, '_queue_stats_lock'):
            self._queue_stats_lock = threading.Lock()
        if not hasattr(self, '_queue_stats'):
            self._queue_stats = {
                'total_received': 0,
                'total_written': 0,
                'drops_count': 0,
                'max_queue_size_seen': 0,
                'current_queue_size': 0,
            }
        return self._queue_stats

    def _resolve_instrument_id_by_internal_id(self, internal_id: int, instrument_type: str = '') -> str:
        conn = self._get_connection()
        try:
            tables = []
            if instrument_type == 'future':
                tables = ['futures_instruments']
            elif instrument_type == 'option':
                tables = ['option_instruments']
            else:
                tables = ['futures_instruments', 'option_instruments']
            for table_name in tables:
                try:
                    row = conn.execute(
                        f"SELECT instrument_id FROM {table_name} WHERE internal_id = ? LIMIT 1",
                        [internal_id]
                    ).fetchone()
                    if row and row[0]:
                        return str(row[0])
                except Exception:
                    continue
            return ''
        finally:
            self._return_connection(conn)

    def get_queue_stats(self) -> Dict[str, int]:
        stats = self._ensure_direct_queue_stats()
        with self._queue_stats_lock:
            return stats.copy()

    def _enqueue_write(self, method_name: str, *args, **kwargs) -> bool:
        """入队写入任务（直接执行模式）"""
        stats = self._ensure_direct_queue_stats()
        with self._queue_stats_lock:
            stats['total_received'] += 1
            received_no = stats['total_received']
        if received_no <= 20 or received_no % 1000 == 0:
            logger.info(
                "[MARKET_DATA_QUEUE_ENQUEUE] mode=direct method=%s total_received=%d current_queue_size=0",
                method_name, received_no,
            )
        try:
            if method_name == '_save_kline_impl':
                internal_id = args[0] if len(args) > 0 else 0
                instrument_type = args[1] if len(args) > 1 else 'future'
                klines = args[2] if len(args) > 2 else []
                period = args[3] if len(args) > 3 else 'M1'
                ok = self._save_kline_impl(internal_id, instrument_type, klines, period)
                with self._queue_stats_lock:
                    if ok:
                        stats['total_written'] += len(klines or [])
                    else:
                        stats['drops_count'] += len(klines or [])
                    written = stats['total_written']
                    dropped = stats['drops_count']
                if written <= 20 or written % 1000 == 0 or dropped > 0:
                    logger.info(
                        "[MARKET_DATA_QUEUE_DRAIN] mode=direct method=%s written=%d dropped=%d current_queue_size=0",
                        method_name, written, dropped,
                    )
                return bool(ok)
            if method_name == '_save_tick_impl':
                internal_id = args[0] if len(args) > 0 else 0
                instrument_type = args[1] if len(args) > 1 else ''
                tick_data = args[2] if len(args) > 2 else []
                tick_rows = tick_data if isinstance(tick_data, list) else [tick_data]
                if not tick_rows:
                    return True
                instrument_id = ''
                for row in tick_rows:
                    if isinstance(row, dict) and row.get('instrument_id'):
                        instrument_id = str(row.get('instrument_id')).strip()
                        break
                if not instrument_id:
                    instrument_id = self._resolve_instrument_id_by_internal_id(int(internal_id or 0), instrument_type)
                if not instrument_id:
                    logger.critical(
                        "[DATA_LOSS][MARKET_DATA_QUEUE_DRAIN] _save_tick_impl cannot resolve instrument_id internal_id=%s type=%s rows=%d",
                        internal_id, instrument_type, len(tick_rows),
                    )
                    with self._queue_stats_lock:
                        stats['drops_count'] += len(tick_rows)
                    return False
                normalized_rows = []
                for row in tick_rows:
                    if isinstance(row, dict):
                        tick_row = dict(row)
                        tick_row.setdefault('instrument_id', instrument_id)
                        normalized_rows.append(tick_row)
                inserted = self.batch_insert_ticks(normalized_rows, instrument_id=instrument_id, use_arrow=True)
                with self._queue_stats_lock:
                    if inserted > 0:
                        stats['total_written'] += inserted
                    else:
                        stats['drops_count'] += len(normalized_rows)
                    written = stats['total_written']
                    dropped = stats['drops_count']
                if written <= 20 or written % 1000 == 0 or dropped > 0:
                    logger.info(
                        "[MARKET_DATA_QUEUE_DRAIN] mode=direct method=%s instrument_id=%s rows=%d inserted=%d total_written=%d dropped=%d current_queue_size=0",
                        method_name, instrument_id, len(normalized_rows), inserted, written, dropped,
                    )
                return inserted > 0
            with self._queue_stats_lock:
                stats['drops_count'] += 1
            logger.error("[MARKET_DATA_QUEUE_DRAIN] unsupported method=%s dropped=1", method_name)
            return False
        except Exception as e:
            with self._queue_stats_lock:
                stats['drops_count'] += 1
            logger.error(f"[_enqueue_write] {method_name} failed: {e}")
            return False

    def _save_kline_impl(self, internal_id: int, instrument_type: str, klines: list, period: str) -> bool:
        """保存K线数据到klines_raw表"""
        if not klines:
            return True
        try:
            kline_dicts = []
            for k in klines:
                ts = k.get('ts')
                trade_date = None
                if ts is not None:
                    try:
                        if hasattr(ts, 'date'):
                            trade_date = ts.date()
                        elif isinstance(ts, str):
                            from datetime import datetime as _dt
                            trade_date = _dt.strptime(ts[:10], '%Y-%m-%d').date()
                    except Exception:
                        pass
                kline_dicts.append({
                    'internal_id': internal_id,
                    'instrument_type': instrument_type,
                    'timestamp': ts,
                    'open': k.get('open', 0.0),
                    'high': k.get('high', 0.0),
                    'low': k.get('low', 0.0),
                    'close': k.get('close', 0.0),
                    'volume': k.get('volume', 0),
                    'open_interest': k.get('open_interest', 0),
                    'trade_date': trade_date,
                    'period': period,
                })
            inserted = self.batch_insert_klines(kline_dicts)
            return inserted > 0
        except Exception as e:
            logger.error(f"[_save_kline_impl] failed: {e}")
            return False


def verify_bar_tick_consistency(conn, instrument_id: str, trade_date: str,
                                volume_tolerance: float = 0.01,
                                price_tolerance: float = 0.001) -> Dict[str, Any]:
    """R27-P1-DI-03: bar/tick一致性校验——检查K线聚合与原始tick数据的一致性

    Args:
        conn: DuckDB连接
        instrument_id: 合约ID
        trade_date: 交易日期
        volume_tolerance: 成交量容差比例(默认1%)
        price_tolerance: 价格容差比例(默认0.1%)

    Returns:
        Dict: {is_consistent: bool, bar_count: int, tick_count: int,
               volume_diff_ratio: float, low_diff_ratio: float, high_diff_ratio: float}
    """
    result = {'is_consistent': True, 'bar_count': 0, 'tick_count': 0,
              'volume_diff_ratio': 0.0, 'low_diff_ratio': 0.0, 'high_diff_ratio': 0.0}
    try:
        bar_row = conn.execute(
            "SELECT SUM(volume) as total_vol, MIN(low) as bar_low, MAX(high) as bar_high "
            "FROM klines_raw kr JOIN futures_instruments fi ON kr.internal_id = fi.internal_id "
            "WHERE fi.instrument_id=? AND kr.trade_date=?",
            [instrument_id, trade_date]
        ).fetchone()
        tick_row = conn.execute(
            "SELECT COUNT(*) as tick_count, SUM(volume) as total_vol, "
            "MIN(price) as tick_low, MAX(price) as tick_high "
            "FROM ticks_raw WHERE instrument_id=? AND CAST(timestamp AS DATE)=?",
            [instrument_id, trade_date]
        ).fetchone()
        if not bar_row or not tick_row:
            return result
        bar_vol, bar_low, bar_high = bar_row
        tick_count, tick_vol, tick_low, tick_high = tick_row
        result['bar_count'] = 1
        result['tick_count'] = tick_count or 0
        if bar_vol and tick_vol and tick_vol > 0:
            result['volume_diff_ratio'] = abs(bar_vol - tick_vol) / tick_vol
            if result['volume_diff_ratio'] > volume_tolerance:
                result['is_consistent'] = False
                logger.warning("[R27-P1-DI-03] volume不一致: bar=%.0f tick=%.0f diff=%.4f",
                               bar_vol, tick_vol, result['volume_diff_ratio'])
        if bar_low and tick_low and tick_low > 0:
            result['low_diff_ratio'] = abs(bar_low - tick_low) / tick_low
            if result['low_diff_ratio'] > price_tolerance:
                result['is_consistent'] = False
        if bar_high and tick_high and tick_high > 0:
            result['high_diff_ratio'] = abs(bar_high - tick_high) / tick_high
            if result['high_diff_ratio'] > price_tolerance:
                result['is_consistent'] = False
    except Exception as e:
        logger.error("[R27-P1-DI-03] bar/tick一致性校验异常: %s", e)
        result['is_consistent'] = False
    return result
