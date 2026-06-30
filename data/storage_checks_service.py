# MODULE_ID: M1-121
"""
storage_checks_service.py - 存储数据自愈服务

迁移自 infra/storage_service.py::StorageChecksService (2026-06-30)
职责：启动检查、数据自愈、序列同步、持仓对账
"""

from __future__ import annotations
import logging
import threading
import time
from typing import Any, Dict

from ali2026v3_trading.infra._helpers import get_logger

logger = get_logger(__name__)

__all__ = ['StorageChecksService', 'StorageChecksMixin']


class StorageChecksService:
    """存储服务 - 数据自愈能力（从StorageChecksMixin重构为独立Service）
    
    职责:
    - 启动时执行元数据表检查
    - 孤儿记录清理
    - 空表检测与清理
    - 订阅标准化检查
    - **新增 2605/2607 合约日志诊断（每 30 秒输出）**
    
    设计原则:
    - 轻量级：只执行必要的检查
    - 非侵入：不影响正常业务流水
    - 自动化：启动时自动执行
    """
    
    MAINTENANCE_VERSION = 20260403
    
    DIAGNOSTIC_CONTRACTS_2607 = [
        {'exchange': 'CFFEX', 'future': 'IF2607', 'option': 'IO2607-C-4500'},
        {'exchange': 'SHFE', 'future': 'CU2607', 'option': 'cu2607C100000'},
        {'exchange': 'DCE', 'future': 'M2607', 'option': 'm2607-C-2950'},
        {'exchange': 'CZCE', 'future': 'MA2607', 'option': 'MA607C3000'},
        {'exchange': 'INE', 'future': 'LU2607', 'option': 'sc2607C660'},
        {'exchange': 'GFEX', 'future': 'LC2607', 'option': 'lc2607-C-16000'},
    ]
    
    def __init__(self, manager: Any):
        """初始化维护服务"""
        self.manager = manager
        
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
        """P1-3修复：startup checks fast path
        
        正常重启时，如果 maintenance version 已是最新，只执行轻量健康检查
        （序列同步 + 缺表修复），跳过重维护逻辑。
        """
        from ali2026v3_trading.data.storage_catalog_service import StorageCatalogService
        self._run_startup_checks_fast_path_impl(conn)
    
    def _run_startup_checks_fast_path_impl(self, conn=None) -> None:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.run_startup_checks_fast_path(conn)
        except Exception as e:
            logger.error("[StorageChecks] fast_path委托失败: %s", e)
    
    def run_startup_checks(self, conn=None) -> None:
        """全量启动检查"""
        from ali2026v3_trading.data.storage_catalog_service import StorageCatalogService
        self._run_startup_checks_impl(conn)
    
    def _run_startup_checks_impl(self, conn=None) -> None:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.run_startup_checks(conn)
        except Exception as e:
            logger.error("[StorageChecks] startup_checks委托失败: %s", e)
    
    def run_periodic_reconciliation(self, interval_sec: int = 3600) -> None:
        """定时持仓对账"""
        self._run_periodic_reconciliation_impl(interval_sec)
    
    def _run_periodic_reconciliation_impl(self, interval_sec: int = 3600) -> None:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.run_periodic_reconciliation(interval_sec)
        except Exception as e:
            logger.error("[StorageChecks] reconciliation委托失败: %s", e)
    
    def run_periodic_diagnostic(self, conn=None) -> None:
        """定期诊断"""
        self._run_periodic_diagnostic_impl(conn)
    
    def _run_periodic_diagnostic_impl(self, conn=None) -> None:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.run_periodic_diagnostic(conn)
        except Exception as e:
            logger.error("[StorageChecks] diagnostic委托失败: %s", e)
    
    def ensure_instrument_id_sequence(self) -> bool:
        """确保ID序列存在"""
        return self._ensure_instrument_id_sequence_impl()
    
    def _ensure_instrument_id_sequence_impl(self) -> bool:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.ensure_instrument_id_sequence()
        except Exception as e:
            logger.error("[StorageChecks] ensure_sequence委托失败: %s", e)
            return False
    
    def reserve_next_global_id(self) -> int:
        """预留全局ID"""
        return self._reserve_next_global_id_impl()
    
    def _reserve_next_global_id_impl(self) -> int:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.reserve_next_global_id()
        except Exception as e:
            logger.error("[StorageChecks] reserve_id委托失败: %s", e)
            return -1
    
    def sync_instrument_id_sequence(self) -> bool:
        """同步ID序列"""
        return self._sync_instrument_id_sequence_impl()
    
    def _sync_instrument_id_sequence_impl(self) -> bool:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.sync_instrument_id_sequence()
        except Exception as e:
            logger.error("[StorageChecks] sync_sequence委托失败: %s", e)
            return False
    
    def get_kv_value(self, key: str) -> Any:
        """获取KV存储"""
        return self._get_kv_value_impl(key)
    
    def _get_kv_value_impl(self, key: str) -> Any:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.get_kv_value(key)
        except Exception as e:
            logger.error("[StorageChecks] get_kv委托失败: %s", e)
            return None
    
    def set_kv_value(self, key: str, value: Any) -> bool:
        """设置KV存储"""
        return self._set_kv_value_impl(key, value)
    
    def _set_kv_value_impl(self, key: str, value: Any) -> bool:
        """实际实现（委托给原infra层实现）"""
        try:
            from ali2026v3_trading.infra.storage_service import StorageChecksService as InfraChecksService
            infra_service = InfraChecksService(self.manager)
            return infra_service.set_kv_value(key, value)
        except Exception as e:
            logger.error("[StorageChecks] set_kv委托失败: %s", e)
            return False


StorageChecksMixin = StorageChecksService