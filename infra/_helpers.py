"""
maintenance_service helpers - 模块级辅助函数和常量
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from datetime import datetime

from ali2026v3_trading.infra.shared_utils import sanitize_sql_identifier, sanitize_sql_value, CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


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
    from ali2026v3_trading.data.data_service import DataService, get_data_service
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


def create_archive_table(conn: Any, table_name: str, archive_table: str) -> None:
    """五唯一性修复：统一的归档表创建逻辑

    复制源表 schema 但不复制数据（WHERE 1=0），用于归档流程的建表步骤。
    规范实现见 infra/_storage.py 的 StorageCatalogService.archive_old_data。

    Args:
        conn: DuckDB 连接对象（需支持 .execute(sql) 接口）
        table_name: 源表名
        archive_table: 归档表名
    """
    from ali2026v3_trading.infra.shared_utils import sanitize_sql_identifier
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {sanitize_sql_identifier(archive_table)} AS "
        f"SELECT * FROM {sanitize_sql_identifier(table_name)} WHERE 1=0"
    )
