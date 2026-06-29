# MODULE_ID: M1-040
"""
query_service.py - 数据查询服务 (DuckDB 版本) — 门面模块

从 storage.py 迁移的所有查询功能
职责：专注于数据查询、统计、导出等读操作

核心功能:
1. 合约信息查询
2. K 线/Tick 数据查询
3. 期权链查询
4. 数据统计与摘要
5. 数据导出

架构变更说明 (2026-04-12):
- 所有数据库操作通过 DataService (DuckDB) 进行
- 保留 QueryService 作为查询逻辑封装层
- _KlineAggregator 保留用于 Tick 合成 K 线

拆分说明 (2026-06-10):
- query_kline_aggregator.py: LightKLine + _KlineAggregator + 辅助函数
- query_instrument_service.py: _QueryInstrumentMixin (合约注册/预注册/分类)
- query_data_export.py: _QueryDataExportMixin (数据查询/导出)
- 本文件: QueryService 继承两个 Mixin，re-export 所有公共 API

作者：CodeArts 代码智能体
版本：v2.0 (DuckDB 重构版)
生成时间：2026-03-31
更新时间：2026-06-10
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

# 从拆分模块 re-export 所有公共名称
from ali2026v3_trading.data.query_kline_aggregator import (
    LightKLine,
    _KlineAggregator,
    _normalize_code,
    _result_to_pylist,
    _table_exists,
    _CHINA_TZ,
    _HAS_DATA_SERVICE,
    DataService,
    get_data_service,
    DATA_EXPORT_FORMAT,
)

from ali2026v3_trading.data.query_instrument_service import (
    InstrumentQueryService,
    _QueryInstrumentMixin,
)

from ali2026v3_trading.data.query_data_export import (
    DataExportService,
    _QueryDataExportMixin,
)


class QueryService:
    """
    数据查询服务（Facade组合，消灭Mixin继承）

    职责：
    1. 合约信息查询（缓存优先）
    2. K 线/Tick 数据查询
    3. 期权链关联查询
    4. 数据统计与摘要
    5. 数据导出到 CSV

    使用方式：
        query_service = QueryService(storage_instance)
        instruments = query_service.get_active_instruments_by_product('IF')
        kline_data = query_service.get_latest_kline('IF2603', limit=100)
    """

    def __init__(self, storage_instance):
        """
        初始化查询服务

        Args:
            storage_instance: InstrumentDataManager 实例（用于访问数据库和缓存）'
        """
        self._storage = storage_instance
        self._futures_file_path = ""
        self._internal_id_to_instrument_idx: Dict[int, str] = {}
        self._idx_built = False
        self._idx_lock = threading.Lock()
        self._instrument_service = InstrumentQueryService(storage_instance)
        self._export_service = DataExportService(storage_instance)

    def _get_params_service(self):
        """通过公共单例获取ParamsService，避免穿透storage._params_service私有属性"""
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            return get_params_service()
        except Exception:
            logging.warning("[R22-EP-P1] QueryService exception swallowed")
            return None

    def _get_cached_instruments(self) -> Dict[str, Dict[str, Any]]:
        ps = self._get_params_service()
        return ps.get_all_instrument_cache() if ps else {}

    def _ensure_internal_id_index(self) -> Dict[int, str]:
        """M23-Bug2修复：确保反向索引已构建，O(1)查找（线程安全）"""
        with self._idx_lock:
            if self._idx_built:
                return self._internal_id_to_instrument_idx

            instruments = self._get_cached_instruments()
            self._internal_id_to_instrument_idx.clear()
            for instrument_id, info in instruments.items():
                internal_id = self._get_info_internal_id(info)
                if internal_id is not None:
                    self._internal_id_to_instrument_idx[internal_id] = instrument_id
            self._idx_built = True
            return self._internal_id_to_instrument_idx

    def _get_info_internal_id(self, info: Optional[Dict[str, Any]]) -> Optional[int]:
        return self._storage._get_info_internal_id(info)

    def __getattr__(self, name):
        if hasattr(self._instrument_service, name):
            return getattr(self._instrument_service, name)
        if hasattr(self._export_service, name):
            return getattr(self._export_service, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


from ali2026v3_trading.infra.maintenance_service import StorageMaintenanceService

# ============================================================================
# 合约辅助工具函数（从 market_data_service.py 迁移）'
# ============================================================================

def resolve_subscribe_flag(params: Any, flag_name: str, legacy_flag: str, default: bool) -> bool:
    """
    解析订阅标志

    Args:
        params: 参数对象
        flag_name: 新标志名
        legacy_flag: 旧标志名（兼容）
        default: 默认值

    Returns:
        bool: 订阅标志值
    """
    try:
        # 优先使用新标志
        value = getattr(params, flag_name, None)
        if value is not None:
            return bool(value)
        # 兼容旧标志
        value = getattr(params, legacy_flag, default)
        return bool(value)
    except Exception:
        return default


def is_symbol_current_or_next(symbol: str, params: Any) -> bool:
    """
    检查合约是否为当前或下月合约

    Args:
        symbol: 合约代码（如 IF2603）
        params: 参数对象（包含 month_mapping）

    Returns:
        bool: 是否为当前或下月合约
    """
    try:
        month_mapping = getattr(params, 'month_mapping', {})
        if not month_mapping:
            return True

        # ✅ 使用 params_service 元数据获取品种，而非正则
        from ali2026v3_trading.config.params_service import get_params_service
        ps = get_params_service()
        meta = ps.get_instrument_meta_by_id(symbol)
        if not meta:
            return True

        product = meta.get('product', '').upper()
        if product in month_mapping:
            specified_months = month_mapping[product]
            if isinstance(specified_months, (list, tuple)) and len(specified_months) >= 2:
                return symbol in [specified_months[0], specified_months[1]]
        return True
    except Exception:
        return True


def is_real_month_contract(instrument_id: str) -> bool:
    """
    判断是否为具体的月合约（全量模式，不过滤）'
    ⚠️  警告：此方法已被废弃，不再进行任何过滤！

    原过滤逻辑:
    1. 排除 888, 999, HOT, IDX 等非具体合约
    2. 只保留年月结尾的合约

    Returns:
        bool: 始终返回 True（全量模式）
    """
    # ✅ 全量模式：直接返回 True，不做任何过滤
    return True


_query_service_instance: Optional['QueryService'] = None

def get_query_service(storage_instance=None) -> 'QueryService':
    global _query_service_instance
    if _query_service_instance is None:
        if storage_instance is None:
            try:
                from ali2026v3_trading.data.data_manager import InstrumentDataManager
                storage_instance = InstrumentDataManager()
            except Exception:
                return None
        _query_service_instance = QueryService(storage_instance)
    return _query_service_instance


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'LightKLine',
    '_KlineAggregator',
    'QueryService',
    'StorageMaintenanceService',
    'resolve_subscribe_flag',
    'is_symbol_current_or_next',
    'is_real_month_contract',
    'get_query_service',
]
