# MODULE_ID: M1-037
"""
query_data_export.py - 数据查询/导出 服务

从 query_service.py 拆分出的数据查询与导出相关功能

核心功能:
1. K线/Tick数据统计查询
2. 合约摘要统计
3. 数据导出（CSV/Parquet）
4. 合约诊断

重构说明 (2026-06-11):
- _QueryDataExportMixin → DataExportService（服务提取+Facade组合，消灭Mixin）
- 构造函数显式接收 storage / params_service，消除隐式self依赖
"""

from __future__ import annotations

import logging
import csv
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

try:
    from data.data_service import DataService, get_data_service
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning("[QueryService] Failed to import DataService: %s", e)
    _HAS_DATA_SERVICE = False
    DataService = None
    get_data_service = None

try:
    from config.config_params import DATA_EXPORT_FORMAT
except ImportError:
    DATA_EXPORT_FORMAT = 'parquet'


class DataExportService:
    """
    数据查询/导出 服务

    职责：
    1. K线/Tick数据统计查询
    2. 合约摘要统计
    3. 数据导出（CSV/Parquet）
    4. 合约诊断
    """

    def __init__(self, storage, params_service=None):
        self._storage = storage
        self._params_service = params_service

    def _get_params_service(self):
        if self._params_service is not None:
            return self._params_service
        try:
            from config.params_service import get_params_service
            return get_params_service()
        except Exception:
            return None

    def _get_cached_instruments(self):
        ps = self._get_params_service()
        return ps.get_all_instrument_cache() if ps else {}

    def _get_info_internal_id(self, info):
        if info is None:
            return None
        internal_id = info.get('internal_id')
        return int(internal_id) if internal_id is not None else None

    # ========================================================================
    # 统计数据查询
    # ========================================================================

    # 注意：以下统计方法已被 DataService 的 get_kline_stats() 替代
    # 保留这些方法用于向后兼容，建议迁移到 DataService

    # ✅ 接口唯一：委托data_service.get_kline_count，不再独立SQL查询
    def get_kline_count(self, instrument_id: str) -> int:
        """
        获取 K 线总条数 (DuckDB 版本)

        Args:
            instrument_id: 合约代码

        Returns:
            int: K 线总条数
        """
        try:
            if _HAS_DATA_SERVICE:
                return get_data_service().get_kline_count(instrument_id)
        except Exception as e:
            logging.error("[R22-EP-P1] get_kline_count failed(数据库异常→0，调用方无法区分无数据/故障): %s", e, exc_info=True)
        return 0

    def get_tick_count(self, instrument_id: str) -> int:
        """
        获取 Tick 总条数 (DuckDB 版本)

        Args:
            instrument_id: 合约代码

        Returns:
            int: Tick 总条数
        """
        info = self._storage._get_instrument_info(instrument_id)
        if not info:
            return 0

        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                result = ds.query(
                    "SELECT COUNT(*) as cnt FROM ticks_raw WHERE instrument_id=?",
                    [instrument_id],
                    arrow=False  # 返回 pandas DataFrame
                )
                if result is not None and hasattr(result, 'iloc') and len(result) > 0:
                    return int(result.iloc[0]['cnt'])
        except Exception as e:
            logging.error("[R22-EP-P1] get_tick_count failed(数据库异常→0，调用方无法区分无数据/故障): %s", e, exc_info=True)
        return 0

    # ✅ 接口唯一：委托data_service.get_kline_range，不再独立SQL查询
    def get_kline_range(self, instrument_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        获取 K 线数据的时间范围 (DuckDB 版本)

        Args:
            instrument_id: 合约代码

        Returns:
            Tuple[Optional[str], Optional[str]]: (最早时间，最晚时间)
        """
        try:
            if _HAS_DATA_SERVICE:
                return get_data_service().get_kline_range(instrument_id)
        except Exception as e:
            logging.error("[QueryService] get_kline_range failed: %s", e)
        return (None, None)

    def get_tick_range(self, instrument_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        获取 Tick 数据的时间范围 (DuckDB 版本)

        Args:
            instrument_id: 合约代码

        Returns:
            Tuple[Optional[str], Optional[str]]: (最早时间，最晚时间)
        """
        info = self._storage._get_instrument_info(instrument_id)
        if not info:
            return (None, None)

        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                result = ds.query(
                    "SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts "
                    "FROM ticks_raw WHERE instrument_id=?",
                    [instrument_id]
                )
                if hasattr(result, 'num_rows') and result.num_rows > 0:
                    row = result.read_all().to_pylist()[0]
                    min_ts = row.get('min_ts')
                    max_ts = row.get('max_ts')
                    return (
                        str(min_ts) if min_ts else None,
                        str(max_ts) if max_ts else None
                    )
        except Exception as e:
            logging.error("[QueryService] get_tick_range failed: %s", e)
        return (None, None)

    # ========================================================================
    # 统计与摘要
    # ========================================================================

    def get_instrument_summary(self, instrument_id: str) -> Dict[str, Any]:
        """
        获取合约数据摘要统计

        Args:
            instrument_id: 合约代码

        Returns:
            Dict[str, Any]: 合约摘要统计
        """
        info = self._storage._get_instrument_info(instrument_id)
        if not info:
            return {}

        kline_count = self.get_kline_count(instrument_id)
        tick_count = self.get_tick_count(instrument_id)
        kline_range = self.get_kline_range(instrument_id)
        tick_range = self.get_tick_range(instrument_id)

        return {
            'instrument_id': instrument_id,
            'internal_id': self._get_info_internal_id(info),
            'type': info['type'],
            'product': info.get('product'),
            'year_month': info.get('year_month'),
            'kline_count': kline_count,
            'tick_count': tick_count,
            'kline_first': kline_range[0],
            'kline_last': kline_range[1],
            'tick_first': tick_range[0],
            'tick_last': tick_range[1]
        }

    def get_all_instruments_summary(self) -> List[Dict[str, Any]]:
        """
        获取所有合约的摘要统计

        Returns:
            List[Dict[str, Any]]: 所有合约的摘要列表
        """
        summaries = []
        ps = self._get_params_service()
        all_ids = ps.get_all_instrument_ids() if ps else []
        for instrument_id in all_ids:
            summary = self.get_instrument_summary(instrument_id)
            if summary:
                summaries.append(summary)
        return summaries

    def get_storage_stats(self) -> Dict[str, Any]:
        tables = []
        total_size = 0

        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                result = ds.query("""
                    SELECT table_name as name, estimated_size as size_bytes
                    FROM duckdb_tables()
                    ORDER BY estimated_size DESC
                """)

                for row in (result.read_all().to_pylist() if hasattr(result, 'to_pylist') else []):
                    tables.append({'name': row['name'], 'size_bytes': row.get('size_bytes', 0) or 0})
                    total_size += (row.get('size_bytes', 0) or 0)
            else:
                logging.warning("[QueryService] DataService not available for storage stats")
        except Exception as e:
            logging.error("[QueryService] Failed to get storage stats: %s", e)

        return {
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'tables': tables,
            'num_futures': sum(1 for v in self._get_cached_instruments().values() if v.get('type') == 'future'),  # R21-MEM-P2-03修复: 生成器替代列表推导式
            'num_options': sum(1 for v in self._get_cached_instruments().values() if v.get('type') == 'option')  # R21-MEM-P2-03修复: 生成器替代列表推导式
        }

    # ========================================================================
    # 数据导出
    # ========================================================================

    def export_kline_to_csv(self, instrument_id: str, output_file: str,
                           start_time: Optional[str] = None,
                           end_time: Optional[str] = None) -> int:
        """
        导出 K 线数据到 CSV 文件（DuckDB 版本）'
        Args:
            instrument_id: 合约代码
            output_file: 输出文件路径
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）

        Returns:
            int: 导出的记录数
        """
        try:
            # P1 Bug #51修复：使用get_data_service()而非直接构造DataService
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
            else:
                logging.error("导出失败：DataService不可用")
                return 0

            start_dt = datetime.fromisoformat(start_time) if start_time else None
            end_dt = datetime.fromisoformat(end_time) if end_time else None

            if not start_dt or not end_dt:
                stats = ds.get_kline_stats(instrument_id)
                if not stats:
                    logging.error("导出失败：合约不存在 %s", instrument_id)
                    return 0
                start_dt = stats.get('first_time')
                end_dt = stats.get('last_time')

            klines = ds.get_klines(instrument_id, start=start_dt, end=end_dt)

            df = klines.to_pandas()
            if DATA_EXPORT_FORMAT == 'parquet':
                _output_file = output_file if output_file.endswith('.parquet') else output_file.rsplit('.', 1)[0] + '.parquet'
                from infra.serialization_utils import safe_dataframe_to_parquet
                safe_dataframe_to_parquet(df, _output_file, preserve_index=False)
                logging.info("导出 K 线 %d 条到 %s (parquet)", len(df), _output_file)
            else:
                from infra.serialization_utils import safe_csv_write
                safe_csv_write(df, output_file)
                logging.info("导出 K 线 %d 条到 %s (csv)", len(df), output_file)
            return len(df)

        except ImportError:
            logging.error("data_service 不可用，无法导出")
            return 0
        except Exception as e:
            logging.error("[QueryService] export_kline_to_csv failed: %s", e)
            return 0

    def export_tick_to_csv(self, instrument_id: str, output_file: str,
                          start_time: Optional[str] = None,
                          end_time: Optional[str] = None) -> int:
        """
        导出 Tick 数据到 CSV 文件（DuckDB 版本）'
        Args:
            instrument_id: 合约代码
            output_file: 输出文件路径
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）

        Returns:
            int: 导出的记录数
        """
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()

                start_dt = datetime.fromisoformat(start_time) if start_time else None
                end_dt = datetime.fromisoformat(end_time) if end_time else None

                # P1 Bug #52修复：None时使用默认时间范围，避免AttributeError
                if not start_dt or not end_dt:
                    stats = ds.get_kline_stats(instrument_id)
                    if not stats:
                        logging.error("导出失败：合约不存在 %s", instrument_id)
                        return 0
                    start_dt = start_dt or stats.get('min_time')
                    end_dt = end_dt or stats.get('max_time')

                result = ds.get_time_range(instrument_id, start_dt, end_dt)

                if hasattr(result, 'to_pandas'):
                    df = result.to_pandas()
                    if DATA_EXPORT_FORMAT == 'parquet':
                        _output_file = output_file if output_file.endswith('.parquet') else output_file.rsplit('.', 1)[0] + '.parquet'
                        from infra.serialization_utils import safe_dataframe_to_parquet
                        safe_dataframe_to_parquet(df, _output_file, preserve_index=False)
                        logging.info("导出 Tick %d 条到 %s (parquet)", len(df), _output_file)
                    else:
                        from infra.serialization_utils import safe_csv_write
                        safe_csv_write(df, output_file)
                        logging.info("导出 Tick %d 条到 %s (csv)", len(df), output_file)
                    return len(df)
                elif hasattr(result, 'to_pylist'):
                    rows = result.read_all().to_pylist()
                    with open(output_file, 'w', newline='', encoding='utf-8') as f:
                        if rows:
                            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                            writer.writeheader()
                            for row in rows:
                                writer.writerow(row)
                    logging.info("导出 Tick %d 条到 %s", len(rows), output_file)
                    return len(rows)
            else:
                logging.error("DataService 不可用，无法导出")
                return 0
        except Exception as e:
            logging.error("[QueryService] export_tick_to_csv failed: %s", e)
            return 0

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
            info = self._storage._get_instrument_info(instrument_id)

            if not info:
                result['subscribe_error'] = f'合约 {instrument_id} 不存在'
                return result

            internal_id = self._get_info_internal_id(info)
            # ✅ 统一使用 klines_raw 和 ticks_raw 表

            result['kline_table'] = 'klines_raw'
            result['tick_table'] = 'ticks_raw'

            if _HAS_DATA_SERVICE:
                ds = get_data_service()

                # subscriptions 表已废弃，跳过订阅状态检查

                kline_count = self.get_kline_count(instrument_id)
                tick_count = self.get_tick_count(instrument_id)
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


_QueryDataExportMixin = DataExportService
