"""
maintenance_service.py - 存储维护服务 (独立模块)

从 query_service.py 迁移的 StorageMaintenanceService 类
职责：存储服务的数据自愈能力（启动检查、孤儿清理、空表检测等）

作者：CodeArts 代码智能体
迁移时间：2026-05-04
"""

from __future__ import annotations

import logging
import time
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _normalize_code(code: str) -> str:
    """标准化代码（交易所/产品），保留原始格式

    Args:
        code: 代码字符串（交易所或产品）

    Returns:
        str: 原始格式的代码
    """
    if not code:
        return ''
    return str(code).strip()


try:
    from ali2026v3_trading.data_service import DataService, get_data_service
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning("[MaintenanceService] Failed to import DataService: %s", e)
    _HAS_DATA_SERVICE = False
    DataService = None
    get_data_service = None


def _result_to_pylist(result: Any) -> List[Dict[str, Any]]:
    if result is None:
        return []
    if hasattr(result, 'to_pylist'):
        return result.to_pylist()
    if hasattr(result, 'read_all'):
        materialized = result.read_all()
        if hasattr(materialized, 'to_pylist'):
            return materialized.to_pylist()
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return []


def _table_exists(ds: Any, table_name: str) -> bool:
    rows = _result_to_pylist(ds.query(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ))
    return bool(rows and int(rows[0].get('cnt', 0) or 0) > 0)


# ============================================================================
# 存储维护服务（从 query_service.py 迁移）
# ============================================================================

class StorageMaintenanceService:
    """
    存储服务 - 数据自愈能力
    
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
    
    # 2605 合约诊断配置
    DIAGNOSTIC_CONTRACTS_2605 = [
        {'exchange': 'CFFEX', 'future': 'IF2605', 'option': 'IO2605-C-4500'},  # CFFEX 沪深 300 股指期权
        {'exchange': 'SHFE', 'future': 'CU2605', 'option': 'cu2605C100000'},  # SHFE 铜期权 (小写 +C+ 行权价)
        {'exchange': 'DCE', 'future': 'M2605', 'option': 'm2605-C-2950'},  # DCE 豆粕期权 (小写)
        {'exchange': 'CZCE', 'future': 'MA2605', 'option': 'MA605C3000'},  # CZCE PTA 期权 (605 表示 2605)
        {'exchange': 'INE', 'future': 'LU2605', 'option': 'sc2605C660'},  # INE 原油期权 (小写 +C+ 行权价)
        {'exchange': 'GFEX', 'future': 'LC2605', 'option': 'lc2605-C-16000'},  # GFEX 工业硅期权
    ]
    
    def __init__(self, manager: Any):
        """初始化维护服务"""
        self.manager = manager
        self._sequence_lock = threading.Lock()
        logging.info("[StorageMaintenance] 服务已初始化")
    
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
                except Exception as e:
                    logging.warning("[StorageMaintenance] _repair_missing_instrument_tables failed: %s", e)
            
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
        except Exception as exc:
            logging.warning("[StorageMaintenance] fast path 失败，降级到全量检查: %s", exc)
            try:
                self.run_startup_checks(conn)
            except Exception as fallback_exc:
                logging.warning("[StorageMaintenance] 全量检查也失败: %s", fallback_exc)

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
                except Exception as e:
                    logging.warning("[StorageMaintenance] _repair_missing_instrument_tables failed: %s", e)

            current = self.get_kv_value('storage_maintenance_version')
            if current == str(self.MAINTENANCE_VERSION):
                logging.info("[StorageMaintenance] 已是最新版本，跳过其余检查（repaired_tables=%d）", repaired_tables)
                return
            
            recovered_orphans = self.recover_orphan_option_metadata()
            seeded_option_products = self.ensure_option_product_catalog()
            repaired_option_fk = self.repair_option_underlying_product_references()
            exchange_updates = self.backfill_metadata_exchange()
            dedup_subs = 0
            normalized_subs = 0
            dropped_empty_tables = 0
            
            self.set_kv_value('storage_maintenance_version', self.MAINTENANCE_VERSION)
            
            logging.info(
                "[StorageMaintenance] 完成：repaired_tables=%d, recovered_orphans=%d, seeded_option_products=%d, repaired_option_fk=%d, exchange_updates=%d, normalized_subs=%d, dedup_subs=%d, dropped_empty_tables=%d",
                repaired_tables,
                recovered_orphans,
                seeded_option_products,
                repaired_option_fk,
                exchange_updates,
                normalized_subs,
                dedup_subs,
                dropped_empty_tables,
            )
        except Exception as exc:
            logging.warning("[StorageMaintenance] 因运行时锁定或维护失败而跳过：%s", exc)
    
    def run_periodic_diagnostic(self, conn=None) -> None:
        """
        运行 2605 合约日志诊断（手动调用，每 30 秒）(DuckDB 版本)
        
        Args:
            conn: 数据库连接（已废弃，保留参数用于兼容）
        
        使用方式：
            # 在策略主循环中每 30 秒调用一次
            if time.time() - last_diagnostic_time >= 30:
                maintenance.run_periodic_diagnostic()
                last_diagnostic_time = time.time()
        """
        try:
            logging.info("\n" + "="*80)
            logging.info("📊 [2605 合约诊断] 开始检查 (每 30 秒自动输出)")
            logging.info("="*80)
            
            total_futures_subscribed = 0
            total_futures_received = 0
            total_options_subscribed = 0
            total_options_received = 0
            
            for idx, contract in enumerate(self.DIAGNOSTIC_CONTRACTS_2605, 1):
                exchange = contract['exchange']
                future_id = contract['future']
                option_id = contract['option']
                
                future_status = self._diagnose_contract(future_id, is_future=True)
                if future_status['subscribed']:
                    total_futures_subscribed += 1
                if future_status['received']:
                    total_futures_received += 1
                
                option_status = None
                if option_id:
                    option_status = self._diagnose_contract(option_id, is_future=False)
                    if option_status['subscribed']:
                        total_options_subscribed += 1
                    if option_status['received']:
                        total_options_received += 1
                
                logging.info(f"\n[{idx}] {exchange} - {future_id} (期货)")
                
                if future_status['subscribed']:
                    logging.info(f"  订阅：[OK] 成功")
                else:
                    logging.error(f"  订阅：[FAIL] 失败")
                    if future_status['subscribe_error']:
                        logging.error(f"    原因：{future_status['subscribe_error']}")
                
                if future_status['enqueued']:
                    logging.info(f"  入队：[OK] 成功")
                else:
                    logging.error(f"  入队：[FAIL] 失败")
                    if future_status['enqueue_error']:
                        logging.error(f"    原因：{future_status['enqueue_error']}")
                
                has_data = future_status['received'] and future_status['kline_count'] > 0
                if has_data:
                    logging.info(f"  接收：[OK] 成功 ({future_status['kline_count']}条 K 线，{future_status['tick_count']}个 Tick)")
                elif future_status['received']:
                    logging.warning(f"  接收：[WARN] 无数据 ({future_status['kline_count']}条 K 线，{future_status['tick_count']}个 Tick)")
                else:
                    logging.error(f"  接收：[FAIL] 失败 ({future_status['kline_count']}条 K 线，{future_status['tick_count']}个 Tick)")
                    if future_status['receive_error']:
                        logging.error(f"    原因：{future_status['receive_error']}")
                
                if future_status['stored']:
                    logging.info(f"  落库：[OK] 成功 (K 线表：{future_status['kline_table']}, Tick 表：{future_status['tick_table']})")
                else:
                    logging.error(f"  落库：[FAIL] 失败")
                    if future_status['store_error']:
                        logging.error(f"    原因：{future_status['store_error']}")
                
                if option_id and option_status:
                    logging.info(f"\n[{idx}] {exchange} - {option_id} (期权)")
                    
                    if option_status['subscribed']:
                        logging.info(f"  订阅：[OK] 成功")
                    else:
                        logging.error(f"  订阅：[FAIL] 失败")
                        if option_status['subscribe_error']:
                            logging.error(f"    原因：{option_status['subscribe_error']}")
                    
                    if option_status['enqueued']:
                        logging.info(f"  入队：[OK] 成功")
                    else:
                        logging.error(f"  入队：[FAIL] 失败")
                        if option_status['enqueue_error']:
                            logging.error(f"    原因：{option_status['enqueue_error']}")
                    
                    has_data = option_status['received'] and option_status['kline_count'] > 0
                    if has_data:
                        logging.info(f"  接收：[OK] 成功 ({option_status['kline_count']}条 K 线，{option_status['tick_count']}个 Tick)")
                    elif option_status['received']:
                        logging.warning(f"  接收：[WARN] 无数据 ({option_status['kline_count']}条 K 线，{option_status['tick_count']}个 Tick)")
                    else:
                        logging.error(f"  接收：[FAIL] 失败 ({option_status['kline_count']}条 K 线，{option_status['tick_count']}个 Tick)")
                        if option_status['receive_error']:
                            logging.error(f"    原因：{option_status['receive_error']}")
                    
                    if option_status['stored']:
                        logging.info(f"  落库：[OK] 成功 (K 线表：{option_status['kline_table']}, Tick 表：{option_status['tick_table']})")
                    else:
                        logging.error(f"  落库：[FAIL] 失败")
                        if option_status['store_error']:
                            logging.error(f"    原因：{option_status['store_error']}")
                elif option_id:
                    logging.info(f"\n[{idx}] {exchange} - {option_id} (期权)")
                    logging.warning(f"  ❌ 期权合约不存在")
            
            logging.info("\n" + "-"*80)
            logging.info("📋 汇总统计:")
            total_options_count = len(self.DIAGNOSTIC_CONTRACTS_2605)
            logging.info(f"  期货：{total_futures_subscribed}/{total_options_count} 已订阅，{total_futures_received}/{total_options_count} 有数据")
            logging.info(f"  期权：{total_options_subscribed}/{total_options_count} 已订阅，{total_options_received}/{total_options_count} 有数据")
            logging.info("="*80 + "\n")
            
        except Exception as exc:
            logging.error(f"[2605 合约诊断] 执行失败：{exc}", exc_info=True)
    
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
        except Exception as e:
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
            except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
            logging.error("[StorageMaintenance] set_kv_value failed: %s", e)
    
    def recover_orphan_option_metadata(self) -> int:
        """恢复孤儿期权元数据 (DuckDB 统一表版本)"""
        # ✅ Group A收口：不再使用LEGACYF0000占位符，历史数据已在迁移时处理
        # if self.manager is not None and hasattr(self.manager, 'ensure_legacy_placeholder_roots'):
        #     try:
        #         self.manager.ensure_legacy_placeholder_roots()
        #     except Exception as e:
        #         logging.warning("[StorageMaintenance] ensure_legacy_placeholder_roots failed: %s", e)
        
        # ⚠️ DEPRECATED: 此方法已废弃
        # 旧版本使用 tick_option_data 分表存储期权 Tick 数据，
        # 现已统一迁移到 ticks_raw 表。此方法保留仅为向后兼容，
        # 实际不再执行任何恢复操作。
        logging.warning("[StorageMaintenance] recover_orphan_options is deprecated, using unified ticks_raw table")
        return 0
    
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
            from ali2026v3_trading.config_service import ExchangeConfig
        except Exception as exc:
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
                try:
                    ds.query(
                        "INSERT INTO option_products "
                        "(product, exchange, underlying_product, format_template, tick_size, contract_size, is_active) "
                        "VALUES (?, ?, ?, ?, 1.0, 1.0, 1)",
                        (normalized_product, normalized_exchange, normalized_underlying, format_template),
                    )
                except Exception as exc:
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
        except Exception as e:
            logging.error("[StorageMaintenance] drop_empty_instrument_tables failed: %s", e)
        return deleted
    
    def ensure_legacy_placeholder_roots(self) -> None:
        """确保遗留占位符根存在"""
        # ✅ Group A收口：不再使用LEGACYF0000占位符，此函数已废弃
        logging.warning("[QueryService] ensure_legacy_placeholder_roots is deprecated, LEGACY placeholder removed")
        return

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
            
        except Exception as e:
            result['store_error'] = f'诊断过程出错：{str(e)}'
        
        return result


__all__ = [
    'StorageMaintenanceService',
]
