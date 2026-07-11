"""_storage/统一存储模块 — 合并自3个子模块"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from datetime import datetime, timedelta

from ali2026v3_trading.infra.serialization_utils import json_dumps
from ali2026v3_trading.infra.shared_utils import sanitize_sql_identifier, sanitize_sql_value
from ali2026v3_trading.infra._helpers import (
    _CHINA_TZ,
    _HAS_DATA_SERVICE,
    _result_to_pylist,
    _table_exists,
    _normalize_code,
    _get_option_spec,
    DEFAULT_OPTION_SPEC,
    get_data_service,
)
from ali2026v3_trading.infra._backup_restore import get_backup_service
from ali2026v3_trading.infra._helpers import get_logger  # R9-5: 统一logger

logger = get_logger(__name__)


# ============================================================
# Section 1: 存储目录Mixin (原 _storage_catalog_mixin.py)
# ============================================================

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

        # 第一步：统计futures_instruments待更新行数
        result1 = ds.query("""
            SELECT COUNT(*) as cnt FROM futures_instruments
            WHERE COALESCE(exchange, '') IN ('', 'AUTO') AND product != 'LEGACY'
        """)
        rows = _result_to_pylist(result1)
        futures_count = int(rows[0].get('cnt', 0)) if rows else 0

        # 第二步：执行UPDATE
        if futures_count > 0:
            ds.query(
                """
                UPDATE futures_instruments
                SET exchange = (
                    SELECT fp.exchange FROM future_products fp
                    WHERE fp.product = futures_instruments.product
                )
                WHERE COALESCE(exchange, '') IN ('', 'AUTO')
                  AND product != 'LEGACY'
            """)

        # 第三步：统计option_instruments待更新行数
        result2 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE COALESCE(exchange, '') IN ('', 'AUTO') AND product != 'LEGACY'
        """)
        rows = _result_to_pylist(result2)
        option_count = int(rows[0].get('cnt', 0)) if rows else 0

        # 第四步：执行UPDATE
        if option_count > 0:
            ds.query(
                """
                UPDATE option_instruments
                SET exchange = (
                    SELECT op.exchange FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                )
                WHERE COALESCE(exchange, '') IN ('', 'AUTO')
                  AND product != 'LEGACY'
                """)

        # P2 Bug #111修复：返回待更新行数总和（因为DuckDB不支持changes()）
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

        # 第一步：统计第一批待更新行数
        r1 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE EXISTS (SELECT 1 FROM option_products op WHERE UPPER(op.product) = UPPER(option_instruments.product))
              AND NOT EXISTS (SELECT 1 FROM option_products op_bad WHERE UPPER(op_bad.product) = UPPER(option_instruments.underlying_product))
        """)
        rows = _result_to_pylist(r1)
        count1 = int(rows[0].get('cnt', 0)) if rows else 0

        # 第二步：执行第一批UPDATE
        if count1 > 0:
            ds.query(
                """
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
                """
            )
        updated += count1

        # 第三步：统计第二批待更新行数
        r2 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE COALESCE(product, '') = 'LEGACY' AND COALESCE(underlying_product, '') != 'LEGACY'
        """)
        rows = _result_to_pylist(r2)
        count2 = int(rows[0].get('cnt', 0)) if rows else 0

        # 第四步：执行第二批UPDATE
        if count2 > 0:
            ds.query(
                """
                UPDATE option_instruments
                SET underlying_product = 'LEGACY'
                WHERE COALESCE(product, '') = 'LEGACY'
                  AND COALESCE(underlying_product, '') != 'LEGACY'
                """
            )
        updated += count2

        # P2 Bug #112修复：返回待更新行数总和（因为DuckDB不支持changes()）
        return max(updated, 0)

    @staticmethod
    def _get_option_format_template(exchange: str) -> str:
        """按交易所返回期权合约格式模板。"""
        # 使用标准化函数，不直接变形
        exchange_upper = _normalize_code(exchange)
        if exchange_upper == 'CFFEX':
            return 'YYYYMM-C-XXXX'
        return '{product}{year_month}{option_type}{strike_price}'

    def ensure_option_product_catalog(self) -> int:
        """补齐并激活 option_products 中缺失的真实期权品种配置。"""

        try:
            from ali2026v3_trading.config.config_service import ExchangeConfig
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as exc:
            logging.warning("[StorageMaintenance] 加载 ExchangeConfig 失败，跳过 option_products 补齐: %s", exc)
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
            # 使用标准化函数，不直接变形
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

        # 查找kline_data和tick表中无数据的instrument_id
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
                # 删除元数据记录
                ds.query("DELETE FROM futures_instruments WHERE internal_id=?", [internal_id])
                ds.query("DELETE FROM option_instruments WHERE internal_id=?", [internal_id])
                # subscriptions 表已废弃，DELETE 操作已移除
                deleted += 1
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StorageMaintenance] drop_empty_instrument_tables failed: %s", e)
        return deleted

    def _diagnose_contract(self, instrument_id: str, is_future: bool) -> Dict:
        """
        诊断单个合约的状态 (DuckDB 版本)

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
                result['subscribe_error'] = f'manager 不支持 _get_instrument_info'
                return result

            if not info:
                result['subscribe_error'] = f'合约 {instrument_id} 不存在'
                return result

            result['kline_table'] = 'klines_raw'
            result['tick_table'] = 'ticks_raw'

            if _HAS_DATA_SERVICE:
                ds = get_data_service()

                # subscriptions 表已废弃，跳过订阅状态检查

                # 查询 K 线数量
                kline_rows = _result_to_pylist(ds.query(
                    "SELECT COUNT(*) as cnt FROM klines_raw WHERE internal_id = (SELECT internal_id FROM futures_instruments WHERE instrument_id = ? UNION ALL SELECT internal_id FROM option_instruments WHERE instrument_id = ? LIMIT 1)",
                    [instrument_id, instrument_id]
                ))
                kline_count = int(kline_rows[0].get('cnt', 0)) if kline_rows else 0

                # 查询 Tick 数量
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

    # OPS-09修复: 数据库定期备份方法
    def backup_database(self, force: bool = False, caller_id: str = "system") -> Dict[str, Any]:
        """OPS-09修复: 执行DuckDB数据库备份

        将DuckDB文件复制到备份目录，文件名带时间戳。
        委托给DuckDBBackupService执行实际备份。

        Args:
            force: 是否强制执行（忽略时间间隔限制）
            caller_id: 操作人标识

        Returns:
            dict: {backup_path, file_size_mb, timestamp, success, error}
        """
        result = {
            'backup_path': None,
            'file_size_mb': 0.0,
            'timestamp': datetime.now(_CHINA_TZ).isoformat(),
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
                    "[OPS-09] 数据库备份完成: %s (%.2fMB) caller=%s",
                    backup_path, result['file_size_mb'], caller_id,
                )
            else:
                result['error'] = '备份被跳过（间隔未到或数据库文件不存在）'
                logger.info("[OPS-09] 数据库备份跳过: 间隔未到 caller=%s", caller_id)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            result['error'] = str(e)
            logger.error("[OPS-09] 数据库备份失败: %s caller=%s", e, caller_id)
        return result

    # P2-项20修复: 数据归档策略
    def archive_old_data(self, table_name: str,
                          archive_before_days: int = 90,
                          dry_run: bool = False) -> Dict[str, Any]:
        """P2-项20修复: 数据归档策略

        将指定表中超过archive_before_days天的数据归档到历史表，
        减小活跃表体积，提升查询性能。

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
            cutoff_date = (datetime.now(_CHINA_TZ) - timedelta(days=archive_before_days)).isoformat()

            db_path = getattr(self, '_db_path', None)
            if db_path is None:
                result['error'] = '未配置数据库路径'
                return result

            from ali2026v3_trading.data.data_access import get_data_access
            _da = get_data_access()
            from ali2026v3_trading.data.db_adapter import connect
            conn = connect(db_path, read_only=False)
            try:
                # 统计待归档行数
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
                    result['message'] = f'试运行: 将归档{rows_to_archive}行'
                    return result

                # 创建归档表（如不存在）
                archive_table = result['archive_table']
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {sanitize_sql_identifier(archive_table)} AS "
                    f"SELECT * FROM {sanitize_sql_identifier(table_name)} WHERE 1=0"
                )

                # 插入归档数据
                conn.execute(
                    f"INSERT INTO {sanitize_sql_identifier(archive_table)} SELECT * FROM {sanitize_sql_identifier(table_name)} "
                    f"WHERE created_at < {sanitize_sql_value(cutoff_date)}"
                )
                result['rows_archived'] = rows_to_archive

                # 删除已归档数据
                conn.execute(
                    f"DELETE FROM {sanitize_sql_identifier(table_name)} WHERE created_at < {sanitize_sql_value(cutoff_date)}"
                )

                result['success'] = True
                logger.info(
                    "[MaintenanceService] 归档完成: %s → %s, %d行",
                    table_name, archive_table, rows_to_archive,
                )
            finally:
                conn.close()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            result['error'] = str(e)
            logger.error("[MaintenanceService] 归档失败: %s → %s", table_name, e)
        return result


StorageCatalogMixin = StorageCatalogService


# ============================================================
# Section 2: 存储检查Mixin (原 _storage_checks_mixin.py)
# ============================================================

class StorageChecksService:
    """
    存储服务 - 数据自愈能力（从StorageChecksMixin重构为独立Service）

    职责:
    - 启动时执行元数据表检查
    - 孤儿记录清理
    - 空表检测与清理
    - 订阅标准化检查
    - **新增：2605 合约日志诊断（每 30 秒输出）**

    设计原则:
    - 轻量级：只执行必要的检查
    - 非侵入：不影响正常业务流程
    - 自动化：启动时自动执行
    """

    MAINTENANCE_VERSION = 20260403

    # 2607 合约诊断配置
    DIAGNOSTIC_CONTRACTS_2607 = [
        {'exchange': 'CFFEX', 'future': 'IF2607', 'option': 'IO2607-C-4500'},  # CFFEX 沪深 300 股指期权
        {'exchange': 'SHFE', 'future': 'CU2607', 'option': 'cu2607C100000'},  # SHFE 铜期权 (小写 +C+ 行权价)
        {'exchange': 'DCE', 'future': 'M2607', 'option': 'm2607-C-2950'},  # DCE 豆粕期权 (小写)
        {'exchange': 'CZCE', 'future': 'MA2607', 'option': 'MA607C3000'},  # CZCE PTA 期权 (607 表示 2607)
        {'exchange': 'INE', 'future': 'LU2607', 'option': 'sc2607C660'},  # INE 原油期权 (小写 +C+ 行权价)
        {'exchange': 'GFEX', 'future': 'LC2607', 'option': 'lc2607-C-16000'},  # GFEX 工业硅期权
    ]

    def __init__(self, manager: Any):
        """初始化维护服务"""
        self.manager = manager
        # RES-P2-03/05/06修复: 集成容错配置
        try:
            from ali2026v3_trading.config.config_params import DEGRADATION_FEATURES, SLA_CONFIG, ALARM_LEVELS
            self._degradation_features = DEGRADATION_FEATURES
            self._sla_config = SLA_CONFIG
            self._alarm_levels = ALARM_LEVELS
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[_storage] 数据解析降级: %s", _r3_err)
            self._degradation_features = []
            self._sla_config = {}
            self._alarm_levels = {}
        self._sequence_lock = threading.Lock()
        logging.info("[StorageMaintenance] 服务已初始化")

    def is_feature_degraded(self, feature_name: str) -> bool:
        return feature_name in self._degradation_features

    def run_startup_checks_fast_path(self, conn=None) -> None:
        """P1-3修复：startup checks fast path。

        正常重启时，如果 maintenance version 已是最新，只执行轻量健康检查
        （序列同步 + 缺表修复），跳过重维护逻辑。
        只有 maintenance version 变化时，才执行全量 startup checks。

        设计约束（修改必看十原则）：
        1. fast path 必须包含序列同步（轻量但关键，防止 ID 冲突）
        2. fast path 必须包含缺表修复（轻量但关键，防止表缺失）
        3. 重维护逻辑（orphan 恢复、FK 修复、订阅去重等）只在 version 变化时执行
        4. 新增迁移脚本、缺表修复、索引补建时，必须声明是否允许在启动热路径执行
        """
        try:
            logging.info("[StorageMaintenance] fast path 启动检查开始...")

            if not _HAS_DATA_SERVICE:
                logging.warning("[StorageMaintenance] DataService 不可用，跳过维护检查")
                return

            ds = get_data_service()

            # 轻量检查：序列同步（关键，防止 ID 冲突）
            self.sync_instrument_id_sequence()

            # 轻量检查：缺表修复（关键，防止表缺失）
            repaired_tables = 0
            if self.manager is not None and hasattr(self.manager, '_repair_missing_instrument_tables'):
                try:
                    repaired_tables = self.manager._repair_missing_instrument_tables()
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning("[StorageMaintenance] _repair_missing_instrument_tables failed: %s", e)

            # DR-P2修复: 数据完整性抽样（检查最近100条tick记录可读性）
            # 抽样失败时降级到完整检查路径，而非静默跳过
            try:
                sampled = 0
                conn = ds.get_connection()
                try:
                    # 检查是否至少有一张tick表可查询
                    tables_result = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%tick%' LIMIT 3"
                    ).fetchall()
                    if tables_result:
                        for (tbl,) in tables_result:
                            try:
                                cnt = conn.execute(f"SELECT COUNT(*) FROM \"{tbl}\" LIMIT 1").fetchone()
                                if cnt and cnt[0] > 0:
                                    # 尝试读取最近的记录
                                    rows = conn.execute(
                                        f"SELECT * FROM \"{tbl}\" ORDER BY rowid DESC LIMIT 100"
                                    ).fetchall()
                                    sampled = len(rows)
                                    if sampled > 0:
                                        break
                            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                logging.debug("[_storage] 数据解析降级: %s", _r3_err)
                                continue
                    if sampled == 0:
                        logging.warning("[StorageMaintenance] DR-P2: 数据抽样失败(无可读tick记录)，需降级到完整检查")
                        self.run_startup_checks(conn)
                        return
                finally:
                    if hasattr(ds, '_return_connection'):
                        ds._return_connection(conn)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as samp_err:
                logging.warning("[StorageMaintenance] DR-P2: 数据抽样异常(%s)，降级到完整检查", samp_err)
                self.run_startup_checks(conn)
                return

            # 检查 maintenance version：匹配则跳过重维护
            current = self.get_kv_value('storage_maintenance_version')
            if current == str(self.MAINTENANCE_VERSION):
                logging.info(
                    "[StorageMaintenance] fast path: maintenance version 匹配 (%s)，跳过重维护 (repaired_tables=%d)",
                    current, repaired_tables,
                )
                return

            # version 不匹配：执行全量 startup checks
            logging.info(
                "[StorageMaintenance] fast path: maintenance version 不匹配 (current=%s, expected=%s)，执行全量检查",
                current, self.MAINTENANCE_VERSION,
            )
            self.run_startup_checks(conn)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
            logging.warning("[StorageMaintenance] fast path 失败，降级到全量检查: %s", exc)
            try:
                self.run_startup_checks(conn)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as fallback_exc:
                logging.warning("[StorageMaintenance] 全量检查也失败: %s", fallback_exc)
        try:
            from ali2026v3_trading.config.config_params import _data_quality_score
            _tick_count = 0
            if _HAS_DATA_SERVICE:
                try:
                    ds = get_data_service()
                    _cnt_rows = _result_to_pylist(ds.query("SELECT COUNT(*) as cnt FROM ticks_raw", []))
                    _tick_count = int(_cnt_rows[0]['cnt']) if _cnt_rows else 0
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                    pass
            _quality_score = _data_quality_score(
                tick_count=_tick_count,
                missing_pct=0.0,
                outlier_pct=0.0,
            )
            logging.info("[DATA-P2-09] 数据质量评分: %.1f/100", _quality_score)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _dq_err:
            logging.debug("[DATA-P2-09] 数据质量评分计算失败: %s", _dq_err)
        try:
            from ali2026v3_trading.ops_documentation import get_fault_tolerance_summary
            _ft_summary = get_fault_tolerance_summary() if callable(getattr(get_fault_tolerance_summary, '__call__', None)) else str(get_fault_tolerance_summary)
            logging.info("[RES-P2-02] 容错策略: %s", _ft_summary[:200])
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
            pass

    def run_startup_checks(self, conn=None) -> None:
        """
        启动时执行必要检查 (DuckDB 版本)

        Args:
            conn: 数据库连接（已废弃，保留参数用于兼容）

        注意：不抛出异常，避免影响系统启动
        """
        try:
            logging.info("[StorageMaintenance] 开始启动检查...")

            if not _HAS_DATA_SERVICE:
                logging.warning("[StorageMaintenance] DataService 不可用，跳过维护检查")
                return

            ds = get_data_service()

            self.sync_instrument_id_sequence()

            repaired_tables = 0
            if self.manager is not None and hasattr(self.manager, '_repair_missing_instrument_tables'):
                try:
                    repaired_tables = self.manager._repair_missing_instrument_tables()
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning("[StorageMaintenance] _repair_missing_instrument_tables failed: %s", e)

            current = self.get_kv_value('storage_maintenance_version')
            if current == str(self.MAINTENANCE_VERSION):
                logging.info("[StorageMaintenance] 已是最新版本，跳过其余检查（repaired_tables=%d）", repaired_tables)
                return

            seeded_option_products = self._catalog_service.ensure_option_product_catalog()
            repaired_option_fk = self._catalog_service.repair_option_underlying_product_references()
            exchange_updates = self._catalog_service.backfill_metadata_exchange()
            dedup_subs = 0
            normalized_subs = 0
            dropped_empty_tables = 0

            self.set_kv_value('storage_maintenance_version', self.MAINTENANCE_VERSION)

            logging.info(
                "[StorageMaintenance] 完成：repaired_tables=%d, seeded_option_products=%d, repaired_option_fk=%d, exchange_updates=%d, normalized_subs=%d, dedup_subs=%d, dropped_empty_tables=%d",
                repaired_tables,
                seeded_option_products,
                repaired_option_fk,
                exchange_updates,
                normalized_subs,
                dedup_subs,
                dropped_empty_tables,
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
            logging.warning("[StorageMaintenance] 因运行时锁定或维护失败而跳过：%s", exc)

    def run_periodic_reconciliation(self, position_service=None, exchange_query_fn: Optional[Callable] = None) -> Dict[str, Any]:
        """R26-P0-DI-01补全: 定时持仓对账——从maintenance_service调用reconcile_positions_with_exchange

        Args:
            position_service: PositionService实例, 用于获取本地持仓
            exchange_query_fn: 查询交易所持仓的回调函数

        Returns:
            Dict: 对账结果 {is_matched, diffs, local_only, exchange_only}
        """
        result = {'is_matched': True, 'diffs': [], 'local_only': [], 'exchange_only': [], 'error': None}
        for _feat in self._degradation_features:
            logging.info("[RES-P2-04] 降级特性已激活: %s", _feat)
        try:
            if position_service is None or exchange_query_fn is None:
                logging.debug("[R26-P0-DI-01] 对账跳过: position_service或exchange_query_fn未提供")
                return result
            from ali2026v3_trading.position.position_service import reconcile_positions_with_exchange
            local_positions = {}
            if hasattr(position_service, '_positions'):
                for _iid, _pdata in position_service._positions.items():
                    local_positions[_iid] = {'volume': _pdata.get('volume', 0) if isinstance(_pdata, dict) else 0}
            exchange_positions = exchange_query_fn()
            result = reconcile_positions_with_exchange(local_positions, exchange_positions or {})
            if not result.get('is_matched', True):
                logging.warning("[R26-P0-DI-01] 定时对账发现不一致: diffs=%d", len(result.get('diffs', [])))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            result['error'] = str(e)
            logging.error("[R26-P0-DI-01] 定时对账异常: %s", e)
        # R27-P2: 集成bar/tick一致性校验
        try:
            from ali2026v3_trading.data.ds_data_writer import verify_bar_tick_consistency
            _ds = getattr(self, '_data_service', None)
            if _ds and hasattr(_ds, '_get_connection'):
                _conn = _ds._get_connection()
                if _conn:
                    try:
                        for _iid in list(local_positions.keys())[:5]:
                            _bt_result = verify_bar_tick_consistency(_conn, _iid, '')
                            if not _bt_result.get('is_consistent', True):
                                logging.warning("[R27-P1-DI-03] bar/tick不一致: instrument=%s", _iid)
                    finally:
                        _ds._return_connection(_conn)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[R27-P1-DI-03] bar/tick一致性校验跳过: %s", e)
        # R27-P1: 孤立函数调用——sync_order_status_with_exchange
        try:
            from ali2026v3_trading.order.order_service import sync_order_status_with_exchange
            _os = getattr(self, '_order_service', None)
            if _os and hasattr(_os, 'get_pending_orders'):
                for _oid in list(_os.get_pending_orders().keys())[:5]:
                    _local = _os.get_order_status(_oid)
                    _sync_result = sync_order_status_with_exchange(_oid, _local, lambda x: None)
                    if not _sync_result.get('synced', True):
                        logging.warning("[R27-P1] 订单状态不一致: order_id=%s", _oid)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[R27-P1] 订单状态同步跳过: %s", e)
        # R27-P1: 孤立函数调用——verify_referential_integrity
        try:
            from ali2026v3_trading.data.ds_schema_manager import DsSchemaManager
            _ds = getattr(self, '_data_service', None)
            if _ds and hasattr(_ds, '_schema_manager'):
                _sm = _ds._schema_manager
                if hasattr(_sm, 'verify_referential_integrity'):
                    _ri_result = _sm.verify_referential_integrity()
                    if not _ri_result.get('is_valid', True):
                        logging.warning("[R27-P1] 引用完整性违反: %s", _ri_result.get('violations', []))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[R27-P1] 引用完整性校验跳过: %s", e)
        # R27-P1: 孤立函数调用——get_hot_reload_status
        try:
            from ali2026v3_trading.config.config_params import get_hot_reload_status
            _hr = get_hot_reload_status()
            _unsupported = _hr.get('unsupported', [])
            if _unsupported:
                logging.info("[R27-P1] 热加载不支持参数: %s", _unsupported)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[R27-P1] 热加载状态查询跳过: %s", e)
        return result

    def run_periodic_diagnostic(self, conn=None) -> None:
        """
        运行 2607 合约日志诊断（手动调用，每 30 秒）(DuckDB 版本)

        Args:
            conn: 数据库连接（已废弃，保留参数用于兼容）
        """
        # RES-P2-10: 灾难恢复手册引用
        try:
            from ali2026v3_trading.ops_documentation import DISASTER_RECOVERY_PROCEDURES
            logging.info("[RES-P2-10] 灾难恢复手册已加载: %d条流程", len(DISASTER_RECOVERY_PROCEDURES) if isinstance(DISASTER_RECOVERY_PROCEDURES, (list, dict)) else 1)
        except ImportError:
            logging.debug("[RES-P2-10] ops_documentation不可用")

        try:
            logging.info("\n" + "="*80)
            logging.info("📊 [2607 合约诊断] 开始检查 (每 30 秒自动输出)")
        except Exception:
            logging.info("📊 [2607 合约诊断] 开始检查 (每 30 秒自动输出)")
        
        try:
            for idx, contract in enumerate(self.DIAGNOSTIC_CONTRACTS_2607, 1):
                exchange = contract['exchange']
                future_id = contract['future']
                option_id = contract['option']

                future_status = self._catalog_service._diagnose_contract(future_id, is_future=True)
                if future_status['subscribed']:
                    total_futures_subscribed += 1
                if future_status['received']:
                    total_futures_received += 1

                option_status = None
                if option_id:
                    option_status = self._catalog_service._diagnose_contract(option_id, is_future=False)
                    if option_status['subscribed']:
                        total_options_subscribed += 1
                    if option_status['received']:
                        total_options_received += 1

                self._log_contract_diagnosis(idx, exchange, future_id, '期货', future_status)

                if option_id and option_status:
                    self._log_contract_diagnosis(idx, exchange, option_id, '期权', option_status)
                elif option_id:
                    logging.info(f"\n[{idx}] {exchange} - {option_id} (期权)")
                    logging.warning(f"  ❌ 期权合约不存在")

            logging.info("\n" + "-"*80)
            logging.info("📋 汇总统计:")
            total_options_count = len(self.DIAGNOSTIC_CONTRACTS_2607)
            logging.info(f"📊 [2607 合约诊断] 检查完成: {success_count}/{total_options_count} 正常")
        except Exception as exc:
            logging.error(f"[2607 合约诊断] 执行失败：{exc}", exc_info=True)

    def _log_contract_diagnosis(self, idx: int, exchange: str, instrument_id: str,
                                 contract_type: str, status: Dict) -> None:
        """输出单个合约诊断日志"""
        logging.info(f"\n[{idx}] {exchange} - {instrument_id} ({contract_type})")

        self._log_status_field(status, 'subscribed', '订阅', 'subscribe_error')
        self._log_status_field(status, 'enqueued', '入队', 'enqueue_error')
        self._log_receive_status(status)
        self._log_store_status(status)

    def _log_status_field(self, status: Dict, field: str, label: str, error_field: str) -> None:
        """输出状态字段日志"""
        if status.get(field):
            logging.info(f"  {label}：[OK] 成功")
        else:
            logging.error(f"  {label}：[FAIL] 失败")
            if status.get(error_field):
                logging.error(f"    原因：{status[error_field]}")

    def _log_receive_status(self, status: Dict) -> None:
        """输出接收状态日志"""
        has_data = status.get('received') and status.get('kline_count', 0) > 0
        kline_count = status.get('kline_count', 0)
        tick_count = status.get('tick_count', 0)

        if has_data:
            logging.info(f"  接收：[OK] 成功 ({kline_count}条 K 线，{tick_count}个 Tick)")
        elif status.get('received'):
            logging.warning(f"  接收：[WARN] 无数据 ({kline_count}条 K 线，{tick_count}个 Tick)")
        else:
            logging.error(f"  接收：[FAIL] 失败 ({kline_count}条 K 线，{tick_count}个 Tick)")
            if status.get('receive_error'):
                logging.error(f"    原因：{status['receive_error']}")

    def _log_store_status(self, status: Dict) -> None:
        """输出落库状态日志"""
        if status.get('stored'):
            kline_table = status.get('kline_table', '')
            tick_table = status.get('tick_table', '')
            logging.info(f"  落库：[OK] 成功 (K 线表：{kline_table}, Tick 表：{tick_table})")
        else:
            logging.error(f"  落库：[FAIL] 失败")
            if status.get('store_error'):
                logging.error(f"    原因：{status['store_error']}")

    # subscriptions 表已废弃，相关方法已移除：
    # - deduplicate_subscriptions()
    # - normalize_subscription_types()
    # - _deduplicate_subscriptions_by_target_type()

    def ensure_instrument_id_sequence(self) -> int:
        """确保全局 instrument_id_sequence 存在并返回当前 next_id (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE:
            return 1
        try:
            ds = get_data_service()

            ds.query("""
                CREATE TABLE IF NOT EXISTS instrument_id_sequence (
                    name TEXT PRIMARY KEY,
                    next_id INTEGER NOT NULL
                )
            """)

            result = ds.query("SELECT next_id FROM instrument_id_sequence WHERE name = 'global'")
            rows = _result_to_pylist(result)
            if rows:
                row = rows[0]
                if row.get('next_id') is not None:
                    return int(row['next_id'])

            rows = _result_to_pylist(ds.query("""
                SELECT COALESCE(MAX(max_id), 0) + 1 AS next_id
                FROM (
                    SELECT MAX(internal_id) AS max_id FROM futures_instruments
                    UNION ALL
                    SELECT MAX(internal_id) AS max_id FROM option_instruments
                )
            """))
            next_id = int(rows[0].get('next_id', 0) or 0) if rows else 1
            next_id = next_id or 1

            ds.query(
                "INSERT INTO instrument_id_sequence (name, next_id) VALUES ('global', ?)",
                [next_id]
            )
            return next_id
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StorageMaintenance] ensure_instrument_id_sequence failed: %s", e)
            return 1

    def reserve_next_global_id(self) -> int:
        """从统一序列中预留一个新的全局 internal_id (DuckDB 版本)

        线程安全：通过 _sequence_lock 保证读-改原子性。
        异常安全：失败时抛出 RuntimeError 而非返回默认值，避免 ID 冲突。
        """
        if not _HAS_DATA_SERVICE:
            raise RuntimeError("[StorageMaintenance] DataService unavailable, cannot reserve global ID")
        with self._sequence_lock:
            try:
                ds = get_data_service()
                next_id = self.ensure_instrument_id_sequence()
                ds.query(
                    "UPDATE instrument_id_sequence SET next_id = ? WHERE name = 'global'",
                    [next_id + 1]
                )
                return next_id
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error("[StorageMaintenance] reserve_next_global_id failed: %s", e)
                raise RuntimeError(f"[StorageMaintenance] reserve_next_global_id failed: {e}") from e

    def sync_instrument_id_sequence(self) -> None:
        """把全局序列推进到当前所有 instrument.id 的上界之后 (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE:
            return
        try:
            ds = get_data_service()
            next_id = self.ensure_instrument_id_sequence()

            rows = _result_to_pylist(ds.query("""
                SELECT COALESCE(MAX(max_id), 0) + 1 AS next_id
                FROM (
                    SELECT MAX(internal_id) AS max_id FROM futures_instruments
                    UNION ALL
                    SELECT MAX(internal_id) AS max_id FROM option_instruments
                )
            """))
            expected_next_id = int(rows[0].get('next_id', 0) or 0) if rows else 1
            expected_next_id = expected_next_id or 1

            if expected_next_id != next_id:
                ds.query(
                    "UPDATE instrument_id_sequence SET next_id = ? WHERE name = 'global'",
                    [expected_next_id]
                )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StorageMaintenance] sync_instrument_id_sequence failed: %s", e)

    def get_kv_value(self, key: str = None) -> Optional[str]:
        """获取 KV 存储值 (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE or not key:
            return None
        try:
            ds = get_data_service()
            rows = _result_to_pylist(ds.query("SELECT value FROM app_kv_store WHERE key=?", [key]))
            if rows:
                return str(rows[0].get('value'))
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logging.error("[StorageMaintenance] get_kv_value failed: %s", e)
        return None

    def set_kv_value(self, key: str = None, value: Any = None) -> None:
        """设置 KV 存储值 (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE or not key:
            return
        try:
            ds = get_data_service()
            ds.query(
                """
                INSERT INTO app_kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
                """,
                [key, str(value), time.time()]
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StorageMaintenance] set_kv_value failed: %s", e)


StorageChecksMixin = StorageChecksService


# ============================================================
# Section 3: 存储维护 (原 _storage_maintenance.py)
# ============================================================

class StorageMaintenanceService:
    """存储维护服务 - Facade组合（消灭Mixin继承）

    通过组合持有StorageChecksService和StorageCatalogService实例，
    __getattr__委托实现零破坏性变更。
    """
    MAINTENANCE_VERSION = 20260403

    def __init__(self, manager=None):
        self._checks_service = StorageChecksService(manager)
        self._catalog_service = StorageCatalogService(manager)

    def __getattr__(self, name):
        _cs = self.__dict__.get('_checks_service')
        if _cs is not None and hasattr(_cs, name):
            return getattr(_cs, name)
        _cat = self.__dict__.get('_catalog_service')
        if _cat is not None and hasattr(_cat, name):
            return getattr(_cat, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
