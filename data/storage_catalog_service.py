# MODULE_ID: M1-120
"""
storage_catalog_service.py - 存储目录维护服务

迁移自 infra/storage_service.py::StorageCatalogService (2026-06-30)
职责：元数据回填、目录维护、数据归档
"""

from __future__ import annotations
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads
from ali2026v3_trading.infra.shared_utils import sanitize_sql_identifier, sanitize_sql_value, CHINA_TZ
from ali2026v3_trading.infra._helpers import get_logger
from ali2026v3_trading.infra._backup_restore import get_backup_service

logger = get_logger(__name__)

__all__ = ['StorageCatalogService', 'StorageCatalogMixin']


try:
    from ali2026v3_trading.data.data_service import get_data_service, _HAS_DATA_SERVICE
except ImportError:
    _HAS_DATA_SERVICE = False
    def get_data_service():
        return None

try:
    from ali2026v3_trading.lifecycle.product_initializer import get_product_params as _get_product_params
    _HAS_PRODUCT_PARAMS = True
except ImportError:
    _HAS_PRODUCT_PARAMS = False


OPTION_PRODUCT_SPECS = {
    'IO': {'tick_size': 0.2, 'contract_size': 100},
    'HO': {'tick_size': 0.2, 'contract_size': 100},
    'MO': {'tick_size': 0.2, 'contract_size': 100},
    'CU': {'tick_size': 10.0, 'contract_size': 5},
    'AL': {'tick_size': 5.0, 'contract_size': 5},
    'ZN': {'tick_size': 5.0, 'contract_size': 5},
    'AU': {'tick_size': 0.02, 'contract_size': 1000},
    'AG': {'tick_size': 1.0, 'contract_size': 15},
    'RB': {'tick_size': 1.0, 'contract_size': 10},
    'RU': {'tick_size': 5.0, 'contract_size': 10},
    'MA': {'tick_size': 1.0, 'contract_size': 10},
    'TA': {'tick_size': 2.0, 'contract_size': 5},
    'OI': {'tick_size': 1.0, 'contract_size': 10},
    'RM': {'tick_size': 1.0, 'contract_size': 10},
    'SA': {'tick_size': 1.0, 'contract_size': 20},
    'FG': {'tick_size': 1.0, 'contract_size': 20},
    'SR': {'tick_size': 1.0, 'contract_size': 10},
    'CF': {'tick_size': 5.0, 'contract_size': 5},
    'AP': {'tick_size': 1.0, 'contract_size': 10},
    'CJ': {'tick_size': 5.0, 'contract_size': 5},
    'SF': {'tick_size': 2.0, 'contract_size': 5},
    'SM': {'tick_size': 2.0, 'contract_size': 5},
    'UR': {'tick_size': 1.0, 'contract_size': 5},
    'M': {'tick_size': 1.0, 'contract_size': 10},
    'Y': {'tick_size': 2.0, 'contract_size': 10},
    'P': {'tick_size': 2.0, 'contract_size': 10},
    'A': {'tick_size': 1.0, 'contract_size': 10},
    'L': {'tick_size': 1.0, 'contract_size': 5},
    'V': {'tick_size': 5.0, 'contract_size': 5},
    'PP': {'tick_size': 1.0, 'contract_size': 5},
    'EB': {'tick_size': 1.0, 'contract_size': 5},
    'I': {'tick_size': 0.5, 'contract_size': 100},
    'EG': {'tick_size': 1.0, 'contract_size': 5},
    'C': {'tick_size': 1.0, 'contract_size': 10},
    'CS': {'tick_size': 1.0, 'contract_size': 10},
}

DEFAULT_OPTION_SPEC = {'tick_size': 1.0, 'contract_size': 1.0}


def _get_option_spec(product: str) -> Dict[str, Any]:
    if _HAS_PRODUCT_PARAMS:
        return _get_product_params(product)
    return OPTION_PRODUCT_SPECS.get(product.upper(), DEFAULT_OPTION_SPEC)


def _normalize_code(code: str) -> str:
    if not code:
        return ''
    return str(code).upper().strip()


def _result_to_pylist(result) -> list:
    if result is None:
        return []
    try:
        if hasattr(result, 'fetchall'):
            rows = result.fetchall()
            if rows and hasattr(rows[0], 'keys'):
                return [dict(row) for row in rows]
            return rows
        if isinstance(result, list):
            return result
        return list(result)
    except Exception:
        return []


def _table_exists(ds, table_name: str) -> bool:
    try:
        ds.query(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


class StorageCatalogService:
    """存储服务 - 目录维护与归档部分（从StorageCatalogMixin重构为独立Service）"""
    
    def __init__(self, manager=None):
        self.manager = manager
    
    def backfill_metadata_exchange(self) -> int:
        """回填元数据交易所信息
        
        P2 Bug #111修复：明确返回值含义为"待更新行数"而非"已更新行数"
        DuckDB不支持changes()，因此先COUNT待更新行数，再执行UPDATE
        """
        if not _HAS_DATA_SERVICE:
            return 0
        
        ds = get_data_service()
        
        result1 = ds.query("""
            SELECT COUNT(*) as cnt FROM futures_instruments
            WHERE COALESCE(exchange, '') IN ('', 'AUTO') AND product != 'LEGACY'
        """)
        rows = _result_to_pylist(result1)
        futures_count = int(rows[0].get('cnt', 0)) if rows else 0
        
        if futures_count > 0:
            ds.query("""
                UPDATE futures_instruments
                SET exchange = (
                    SELECT fp.exchange FROM future_products fp
                    WHERE fp.product = futures_instruments.product
                )
                WHERE COALESCE(exchange, '') IN ('', 'AUTO')
                  AND product != 'LEGACY'
            """)
        
        result2 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE COALESCE(exchange, '') IN ('', 'AUTO') AND product != 'LEGACY'
        """)
        rows = _result_to_pylist(result2)
        option_count = int(rows[0].get('cnt', 0)) if rows else 0
        
        if option_count > 0:
            ds.query("""
                UPDATE option_instruments
                SET exchange = (
                    SELECT op.exchange FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                )
                WHERE COALESCE(exchange, '') IN ('', 'AUTO')
                  AND product != 'LEGACY'
            """)
        
        return futures_count + option_count
    
    def repair_option_underlying_product_references(self) -> int:
        """修复 option_instruments.underlying_product 指向旧期货品种或缺失占位符的问题。
        P2 Bug #112修复：明确返回值含义为"待更新行数"而非"已更新行数"
        DuckDB不支持changes()，因此先COUNT待更新行数，再执行UPDATE
        """
        if not _HAS_DATA_SERVICE:
            return 0
        
        ds = get_data_service()
        updated = 0
        
        r1 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE EXISTS (SELECT 1 FROM option_products op WHERE UPPER(op.product) = UPPER(option_instruments.product))
              AND NOT EXISTS (SELECT 1 FROM option_products op_bad WHERE UPPER(op_bad.product) = UPPER(option_instruments.underlying_product))
        """)
        rows = _result_to_pylist(r1)
        count1 = int(rows[0].get('cnt', 0)) if rows else 0
        
        if count1 > 0:
            ds.query("""
                UPDATE option_instruments
                SET underlying_product = (
                    SELECT op.product
                    FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                    ORDER BY op.product
                    LIMIT 1
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM option_products op_bad
                    WHERE UPPER(op_bad.product) = UPPER(option_instruments.underlying_product)
                  )
            """)
        updated += count1
        
        r2 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE COALESCE(product, '') = 'LEGACY' AND COALESCE(underlying_product, '') != 'LEGACY'
        """)
        rows = _result_to_pylist(r2)
        count2 = int(rows[0].get('cnt', 0)) if rows else 0
        
        if count2 > 0:
            ds.query("""
                UPDATE option_instruments
                SET underlying_product = 'LEGACY'
                WHERE COALESCE(product, '') = 'LEGACY'
                  AND COALESCE(underlying_product, '') != 'LEGACY'
            """)
        updated += count2
        
        return max(updated, 0)
    
    @staticmethod
    def _get_option_format_template(exchange: str) -> str:
        """按交易所返回期权合约格式模板"""
        exchange_upper = _normalize_code(exchange)
        if exchange_upper == 'CFFEX':
            return 'YYYYMM-C-XXXX'
        return '{product}{year_month}{option_type}{strike_price}'
    
    def ensure_option_product_catalog(self) -> int:
        """补齐并激活option_products 中缺失的真实期权品种配置源"""
        try:
            from ali2026v3_trading.config.config_service import ExchangeConfig
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as exc:
            logging.warning("[StorageMaintenance] 加载 ExchangeConfig 失败，跳过option_products 补齐: %s", exc)
            return 0
        
        updated = 0
        ds = self.manager._data_service if hasattr(self.manager, '_data_service') else None
        if not ds:
            return 0
        
        option_products = ExchangeConfig().option_products
        for option_product, product_meta in option_products.items():
            if not isinstance(product_meta, tuple) or len(product_meta) < 2:
                continue
            
            underlying_product, exchange = product_meta[:2]
            normalized_product = _normalize_code(option_product)
            normalized_underlying = _normalize_code(underlying_product)
            normalized_exchange = _normalize_code(exchange)
            format_template = self._get_option_format_template(normalized_exchange)
            
            result = ds.query(
                "SELECT product, exchange, underlying_product, format_template, is_active "
                "FROM option_products WHERE UPPER(product)=? ORDER BY product LIMIT 1",
                (normalized_product,)
            )
            row = None
            rows = _result_to_pylist(result)
            if rows:
                row = rows[0]
            
            if row is None:
                spec = _get_option_spec(normalized_product)
                tick_size = spec.get('tick_size', DEFAULT_OPTION_SPEC['tick_size'])
                contract_size = spec.get('contract_size', DEFAULT_OPTION_SPEC['contract_size'])
                try:
                    ds.query(
                        "INSERT INTO option_products "
                        "(product, exchange, underlying_product, format_template, tick_size, contract_size, is_active) "
                        "VALUES (?, ?, ?, ?, ?, ?, 1)",
                        (normalized_product, normalized_exchange, normalized_underlying, format_template, tick_size, contract_size),
                    )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
                    logging.warning("[StorageMaintenance] 跳过 option_products 插入 %s: %s", normalized_product, exc)
                    continue
                updated += 1
            continue
        
        return updated
    
    def drop_empty_instrument_tables(self) -> int:
        """删除无数据的合约记录（统一表版本：DELETE而非DROP TABLE）"""
        deleted = 0
        ds = self.manager._data_service if hasattr(self.manager, '_data_service') else None
        if not ds:
            return 0
        
        try:
            registered_rows = _result_to_pylist(ds.query(
                "SELECT internal_id, instrument_id FROM futures_instruments UNION ALL SELECT internal_id, instrument_id FROM option_instruments"
            ))
            all_ids = {int(row['internal_id']) for row in registered_rows if row.get('internal_id') is not None}
            
            kline_ids = {
                int(row['internal_id'])
                for row in _result_to_pylist(ds.query("SELECT DISTINCT internal_id FROM klines_raw"))
                if row.get('internal_id') is not None
            }
            
            tick_instrument_ids = set()
            if _table_exists(ds, 'ticks_raw'):
                tick_instrument_ids = {
                    str(row['instrument_id'])
                    for row in _result_to_pylist(ds.query("SELECT DISTINCT instrument_id FROM ticks_raw"))
                    if row.get('instrument_id') is not None
                }
            
            has_data_ids = set(kline_ids)
            for row in registered_rows:
                internal_id = row.get('internal_id')
                instrument_id = row.get('instrument_id')
                if internal_id is None or instrument_id is None:
                    continue
                if str(instrument_id) in tick_instrument_ids:
                    has_data_ids.add(int(internal_id))
            
            empty_ids = all_ids - has_data_ids
            
            for internal_id in empty_ids:
                ds.query("DELETE FROM futures_instruments WHERE internal_id=?", [internal_id])
                ds.query("DELETE FROM option_instruments WHERE internal_id=?", [internal_id])
                deleted += 1
        
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StorageMaintenance] drop_empty_instrument_tables failed: %s", e)
        
        return deleted
    
    def _diagnose_contract(self, instrument_id: str, is_future: bool) -> Dict:
        """诊断单个合约的状态(DuckDB 版本)
        
        Returns:
            dict: {
                'subscribed': bool,
                'subscribe_error': str,
                'enqueued': bool,
                'enqueue_error': str,
                'received': bool,
                'receive_error': str,
                'stored': bool,
                'store_error': str,
                'kline_table': str,
                'tick_table': str,
                'kline_count': int,
                'tick_count': int,
            }
        """
        result = {
            'subscribed': False,
            'subscribe_error': '',
            'enqueued': False,
            'enqueue_error': '',
            'received': False,
            'receive_error': '',
            'stored': False,
            'store_error': '',
            'kline_table': '',
            'tick_table': '',
            'kline_count': 0,
            'tick_count': 0,
        }
        
        try:
            if self.manager is not None and hasattr(self.manager, '_get_instrument_info'):
                info = self.manager._get_instrument_info(instrument_id)
            else:
                result['subscribe_error'] = f'manager 不支持_get_instrument_info'
                return result
            
            if not info:
                result['subscribe_error'] = f'合约 {instrument_id} 不存在'
                return result
            
            result['kline_table'] = 'klines_raw'
            result['tick_table'] = 'ticks_raw'
            
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                
                kline_rows = _result_to_pylist(ds.query(
                    "SELECT COUNT(*) as cnt FROM klines_raw WHERE internal_id = (SELECT internal_id FROM futures_instruments WHERE instrument_id = ? UNION ALL SELECT internal_id FROM option_instruments WHERE instrument_id = ? LIMIT 1)",
                    [instrument_id, instrument_id]
                ))
                kline_count = int(kline_rows[0].get('cnt', 0)) if kline_rows else 0
                
                tick_rows = _result_to_pylist(ds.query(
                    "SELECT COUNT(*) as cnt FROM ticks_raw WHERE instrument_id = ?",
                    [instrument_id]
                ))
                tick_count = int(tick_rows[0].get('cnt', 0)) if tick_rows else 0
                
                result['kline_count'] = kline_count
                result['tick_count'] = tick_count
                
                if kline_count > 0 or tick_count > 0:
                    result['enqueued'] = True
                    result['received'] = True
                    result['stored'] = True
                else:
                    result['enqueue_error'] = '数据表为空或不存在'
        
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            result['store_error'] = f'诊断过程出错：{str(e)}'
        
        return result
    
    def backup_database(self, force: bool = False, caller_id: str = "system") -> Dict[str, Any]:
        """OPS-09修复: 执行DuckDB数据库备份
        
        将DuckDB文件复制到备份目录，文件名带时间戳
        委托给DuckDBBackupService执行实际备份
        
        Args:
            force: 是否强制执行（忽略时间间隔限制）
            caller_id: 操作人标记
        
        Returns:
            dict: {backup_path, file_size_mb, timestamp, success, error}
        """
        result = {
            'backup_path': None,
            'file_size_mb': 0.0,
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'success': False,
            'error': None,
        }
        
        try:
            backup_svc = get_backup_service()
            backup_path = backup_svc.backup_duckdb(force=force)
            
            if backup_path:
                result['backup_path'] = backup_path
                result['success'] = True
                try:
                    result['file_size_mb'] = round(
                        os.path.getsize(backup_path) / (1024 * 1024), 2
                    )
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                    pass
                logger.info(
                    "[OPS-09] 数据库备份完成 %s (%.2fMB) caller=%s",
                    backup_path, result['file_size_mb'], caller_id,
                )
            else:
                result['error'] = '备份被跳过（间隔未到或数据库文件不存在）'
                logger.info("[OPS-09] 数据库备份跳过 间隔未到 caller=%s", caller_id)
        
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            result['error'] = str(e)
            logger.error("[OPS-09] 数据库备份失败 %s caller=%s", e, caller_id)
        
        return result
    
    def archive_old_data(self, table_name: str,
                         archive_before_days: int = 90,
                         dry_run: bool = False) -> Dict[str, Any]:
        """P2-10修复: 数据归档策略
        
        将指定表中超过archive_before_days天的数据归档到历史表。
        减小活跃表体积，提升查询性能
        
        Args:
            table_name: 要归档的表名
            archive_before_days: 归档多少天前的数据
            dry_run: 仅统计不实际执行
        
        Returns:
            Dict: {rows_archived, archive_table, dry_run, success}
        """
        result = {
            'table_name': table_name,
            'archive_before_days': archive_before_days,
            'rows_archived': 0,
            'archive_table': f'__archived_{table_name}',
            'dry_run': dry_run,
            'success': False,
        }
        
        try:
            cutoff_date = (datetime.now(CHINA_TZ) - timedelta(days=archive_before_days)).isoformat()
            
            db_path = getattr(self, '_db_path', None)
            if db_path is None:
                result['error'] = '未配置数据库路径'
                return result
            
            from ali2026v3_trading.data.data_access import get_data_access
            _da = get_data_access()
            from ali2026v3_trading.data.db_adapter import connect
            conn = connect(db_path, read_only=False)
            
            try:
                count_row = conn.execute(
                    f"SELECT COUNT(*) FROM {sanitize_sql_identifier(table_name)} WHERE created_at < {sanitize_sql_value(cutoff_date)}"
                ).fetchone()
                rows_to_archive = count_row[0] if count_row else 0
                result['rows_to_archive'] = rows_to_archive
                
                if rows_to_archive == 0:
                    result['success'] = True
                    result['message'] = '无需归档'
                    return result
                
                if dry_run:
                    result['success'] = True
                    result['message'] = f'试运行 将归档{rows_to_archive}行'
                    return result
                
                archive_table = result['archive_table']
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {sanitize_sql_identifier(archive_table)} AS "
                    f"SELECT * FROM {sanitize_sql_identifier(table_name)} WHERE 1=0"
                )
                
                conn.execute(
                    f"INSERT INTO {sanitize_sql_identifier(archive_table)} SELECT * FROM {sanitize_sql_identifier(table_name)} "
                    f"WHERE created_at < {sanitize_sql_value(cutoff_date)}"
                )
                result['rows_archived'] = rows_to_archive
                
                conn.execute(
                    f"DELETE FROM {sanitize_sql_identifier(table_name)} WHERE created_at < {sanitize_sql_value(cutoff_date)}"
                )
                
                result['success'] = True
                logger.info(
                    "[MaintenanceService] 归档完成: %s -> %s, %d rows",
                    table_name, archive_table, rows_to_archive,
                )
            
            finally:
                conn.close()
        
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            result['error'] = str(e)
            logger.error("[MaintenanceService] 归档失败: %s - %s", table_name, e)
        
        return result


StorageCatalogMixin = StorageCatalogService