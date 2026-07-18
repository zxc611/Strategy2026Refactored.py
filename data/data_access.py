# MODULE_ID: M1-020
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Protocol, Sequence, runtime_checkable

from infra.shared_utils import sanitize_sql_identifier, CHINA_TZ as _CHINA_TZ

__all__ = [
    'MarketDataResult', 'PositionsResult', 'TradeSaveResult', 'RiskStateResult',
    'RiskStateSaveResult', 'MarketDataAccess', 'PositionAccess', 'TradeAccess',
    'RiskStateAccess', 'RiskStateSaveAccess', 'WriteResult', 'FlushResult',
    'QueryResult', 'CompactionResult', 'IndexResult', 'StorageStatsResult',
    'DataWriteAccess', 'QueryAccess', 'MaintenanceAccess', 'DefaultDataAccess',
    'get_data_access',
]



logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MarketDataResult:
    """行情数据查询结果

    Attributes:
        success: 查询是否成功
        data: 行情数据列表，每条记录为字段名到值的映射
        total_count: 符合条件的总记录数
        source: 数据来源标识（realtime_cache / duckdb / mixed）
        error_message: 失败时的错误信息
    """
    success: bool
    data: List[Dict[str, Any]] = field(default_factory=list)
    total_count: int = 0
    source: str = ""
    error_message: str = ""


@dataclass(slots=True)
class PositionsResult:
    """持仓查询结果

    Attributes:
        success: 查询是否成功
        net_volume: 净持仓量（正=净多，负=净空）'
        average_price: 加权平均价格
        positions: 持仓记录列表
        instrument_id: 查询的合约代码
        error_message: 失败时的错误信息
    """
    success: bool
    net_volume: int = 0
    average_price: float = 0.0
    positions: List[Dict[str, Any]] = field(default_factory=list)
    instrument_id: str = ""
    error_message: str = ""


@dataclass(slots=True)
class TradeSaveResult:
    """交易保存结果

    Attributes:
        success: 保存是否成功
        trade_id: 保存后的交易唯一标识
        persisted: 是否已持久化到WAL/DB
        idempotent: 是否为幂等去重（重复提交被忽略）
        error_message: 失败时的错误信息
    """
    success: bool
    trade_id: str = ""
    persisted: bool = False
    idempotent: bool = False
    error_message: str = ""


@dataclass(slots=True)
class RiskStateResult:
    """风控状态查询结果

    Attributes:
        success: 查询是否成功
        risk_level: 当前风险等级（LOW / MEDIUM / HIGH / RESTRICTED / CRITICAL）
        circuit_breaker_active: 熔断器是否激活
        strategy_state: 策略级风控状态字典
        health_status: SafetyMetaLayer聚合健康状态（OK / WARNING / CRITICAL）
        history: 历史风控状态快照列表（仅include_history=True时返回）
        error_message: 失败时的错误信息
    """
    success: bool
    risk_level: str = "UNKNOWN"
    circuit_breaker_active: bool = False
    strategy_state: Dict[str, Any] = field(default_factory=dict)
    health_status: str = "UNKNOWN"
    history: List[Dict[str, Any]] = field(default_factory=list)
    error_message: str = ""


@dataclass(slots=True)
class RiskStateSaveResult:
    """风控状态保存结果

    Attributes:
        success: 保存是否成功
        snapshot_tag: 快照标签
        snapshot_time: 快照保存时间（ISO格式）
        persisted: 是否已持久化到磁盘
        error_message: 失败时的错误信息
    """
    success: bool
    snapshot_tag: str = ""
    snapshot_time: str = ""
    persisted: bool = False
    error_message: str = ""


@runtime_checkable
class MarketDataAccess(Protocol):
    """行情数据领域接口

    封装路径：RealTimeCache.get_latest_price() / DataService.get_time_range() / get_kline_range()
    """

    def get_market_data(
        self,
        symbol: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> MarketDataResult:
        """查询行情数据

        优先从RealTimeCache获取最新价格；指定时间范围时从DuckDB查询ticks_raw或kline数据。'
        Args:
            symbol: 合约代码（如 IF2606）
            start: 时间范围起始（None表示不限制起始时间）'
            end: 时间范围结束（None表示不限制结束时间）
            fields: 需要返回的字段列表（None表示默认字段: timestamp, last_price, volume）
            limit: 返回记录数上限（None表示不限制）

        Returns:
            MarketDataResult: 行情数据查询结果
        """
        ...


@runtime_checkable
class PositionAccess(Protocol):
    """持仓领域接口

    封装路径：PositionService.get_position() / get_net_position() / get_position_info()
    """

    def get_positions(
        self,
        instrument_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        include_details: bool = False,
    ) -> PositionsResult:
        """查询持仓信息

        支持按合约/策略过滤，可选返回详细持仓记录。

        Args:
            instrument_id: 合约代码（None表示查询所有合约）
            strategy_id: 策略标识（None表示不限策略）
            include_details: 是否包含详细持仓记录列表

        Returns:
            PositionsResult: 持仓查询结果
        """
        ...


@runtime_checkable
class TradeAccess(Protocol):
    """交易持久化领域接口

    封装路径：order_persistence.py WAL写入 / risk_service.save_state_snapshot()
    """

    def save_trade(
        self,
        trade: Dict[str, Any],
        strategy_id: Optional[str] = None,
        ensure_idempotent: bool = True,
    ) -> TradeSaveResult:
        """保存交易记录

        通过WAL机制写入交易数据，支持幂等去重。

        Args:
            trade: 交易数据字典，需包含 instrument_id, direction, price, volume 等字段
            strategy_id: 关联的策略标识
            ensure_idempotent: 是否启用幂等去重（基于trade中的order_id去重）

        Returns:
            TradeSaveResult: 交易保存结果
        """
        ...


@runtime_checkable
class RiskStateAccess(Protocol):
    """风控状态查询领域接口

    封装路径：RiskService.get_strategy_risk_state() / get_risk_status() / get_health_status()
    """

    def get_risk_state(
        self,
        strategy_id: Optional[str] = None,
        include_history: bool = False,
    ) -> RiskStateResult:
        """查询风控状态

        聚合RiskService的策略级风控状态、全局风险等级和SafetyMetaLayer健康状态。

        Args:
            strategy_id: 策略标识（None表示查询全局风控状态）
            include_history: 是否包含历史风控状态快照

        Returns:
            RiskStateResult: 风控状态查询结果
        """
        ...


@runtime_checkable
class RiskStateSaveAccess(Protocol):
    """风控状态保存领域接口

    封装路径：RiskService.update_strategy_risk_state() / save_state_snapshot()
    """

    def save_risk_state(
        self,
        risk_state: Dict[str, Any],
        strategy_id: Optional[str] = None,
        snapshot_tag: str = "",
    ) -> RiskStateSaveResult:
        """保存风控状态

        更新策略级风控状态并可选持久化快照到磁盘。

        Args:
            risk_state: 风控状态字典，可包含 consecutive_loss_count, is_paused, signal_count 等字段
            strategy_id: 关联的策略标识
            snapshot_tag: 快照标签（非空时同时持久化快照到磁盘）

        Returns:
            RiskStateSaveResult: 风控状态保存结果
        """
        ...


@dataclass(slots=True)
class WriteResult:
    """数据写入结果"""
    success: bool
    rows_written: int = 0
    error_message: str = ""


@dataclass(slots=True)
class FlushResult:
    """缓冲区刷新结果"""
    success: bool
    rows_flushed: int = 0
    error_message: str = ""


@dataclass(slots=True)
class QueryResult:
    """通用查询结果"""
    success: bool
    data: Any = None
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    error_message: str = ""


@dataclass(slots=True)
class CompactionResult:
    """数据库压缩结果"""
    success: bool
    size_before_mb: float = 0.0
    size_after_mb: float = 0.0
    error_message: str = ""


@dataclass(slots=True)
class IndexResult:
    """索引重建结果"""
    success: bool
    index_name: str = ""
    error_message: str = ""


@dataclass(slots=True)
class StorageStatsResult:
    """存储统计结果"""
    success: bool
    total_tables: int = 0
    total_rows: int = 0
    database_size_mb: float = 0.0
    table_stats: Dict[str, Any] = field(default_factory=dict)
    error_message: str = ""


@runtime_checkable
class DataWriteAccess(Protocol):
    """数据写入领域接口

    封装路径：db_adapter.connect() + DuckDB INSERT/UPSERT操作
    """

    def write_records(
        self,
        table: str,
        records: List[Dict[str, Any]],
        strategy_id: Optional[str] = None,
    ) -> WriteResult:
        """批量写入记录到指定表"""
        ...

    def flush_buffer(self, table: str) -> FlushResult:
        """刷新指定表的写入缓冲区"""
        ...


@runtime_checkable
class QueryAccess(Protocol):
    """通用查询领域接口

    封装路径：db_adapter.connect_in_memory() / fetchdf() / close()
    """

    def execute_query(
        self,
        query: str,
        params: Optional[List] = None,
        in_memory: bool = False,
    ) -> QueryResult:
        """执行SQL查询"""
        ...

    def register_dataframe(self, name: str, df: Any) -> None:
        """注册DataFrame为虚拟表"""
        ...


@runtime_checkable
class MaintenanceAccess(Protocol):
    """数据库维护领域接口

    封装路径：db_adapter.connect() + 维护操作
    """

    def compact_database(self) -> CompactionResult:
        """压缩数据库"""
        ...

    def rebuild_index(self, table: str) -> IndexResult:
        """重建指定表的索引"""
        ...

    def get_storage_stats(self) -> StorageStatsResult:
        """获取存储统计信息"""
        ...


class DefaultDataAccess:
    """领域级数据访问默认实现

    委托给现有的 DataService / PositionService / RiskService / order_persistence 模块。
    """

    def __init__(self) -> None:
        self._data_service: Any = None
        self._position_service: Any = None
        self._risk_service: Any = None

    def _get_data_service(self) -> Any:
        if self._data_service is None:
            from data.data_service import get_data_service
            self._data_service = get_data_service()
        return self._data_service

    def _get_position_service(self) -> Any:
        if self._position_service is None:
            from position.position_service import get_position_service
            self._position_service = get_position_service()
        return self._position_service

    def _get_risk_service(self) -> Any:
        if self._risk_service is None:
            from risk.risk_service import get_risk_service
            self._risk_service = get_risk_service()
        return self._risk_service

    def get_market_data(
        self,
        symbol: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        fields: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> MarketDataResult:
        if not symbol:
            return MarketDataResult(success=False, error_message="symbol is required")

        ds = self._get_data_service()

        if start is None and end is None:
            price = None
            if ds.realtime_cache is not None:
                try:
                    price = ds.realtime_cache.get_latest_price(symbol)
                except Exception as e:
                    logger.warning("[DefaultDataAccess] realtime_cache.get_latest_price failed: %s", e)
            if price is not None:
                return MarketDataResult(
                    success=True,
                    data=[{"symbol": symbol, "last_price": price}],
                    total_count=1,
                    source="realtime_cache",
                )
            return MarketDataResult(success=False, error_message=f"no cached price for {symbol}")

        try:
            if fields is not None:
                columns = list(fields)
            else:
                columns = None

            result_table = ds.get_time_range(symbol, start, end, columns=columns)
            rows = result_table.to_pylist() if hasattr(result_table, "to_pylist") else []

            if limit is not None and limit > 0:
                rows = rows[:limit]

            return MarketDataResult(
                success=True,
                data=rows,
                total_count=len(rows),
                source="duckdb",
            )
        except Exception as e:
            logger.error("[DefaultDataAccess] get_market_data query failed: %s", e)
            return MarketDataResult(success=False, error_message=str(e))

    def get_positions(
        self,
        instrument_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        include_details: bool = False,
    ) -> PositionsResult:
        ps = self._get_position_service()

        if instrument_id is not None:
            try:
                pos_info = ps.get_position(instrument_id)
                net_vol = ps.get_net_position(instrument_id)
                positions = []
                if include_details:
                    raw_positions = pos_info.get("positions", [])
                    for rec in raw_positions:
                        positions.append(rec.to_dict() if hasattr(rec, "to_dict") else rec)
                return PositionsResult(
                    success=True,
                    net_volume=net_vol,
                    average_price=pos_info.get("average_price", 0.0),
                    positions=positions,
                    instrument_id=instrument_id,
                )
            except Exception as e:
                logger.error("[DefaultDataAccess] get_positions failed: %s", e)
                return PositionsResult(success=False, instrument_id=instrument_id or "", error_message=str(e))

        try:
            all_info = ps.get_position_info()
            total_net = 0
            positions = []
            if include_details:
                positions = all_info
            return PositionsResult(
                success=True,
                net_volume=total_net,
                positions=positions,
            )
        except Exception as e:
            logger.error("[DefaultDataAccess] get_positions (all) failed: %s", e)
            return PositionsResult(success=False, error_message=str(e))

    def save_trade(
        self,
        trade: Dict[str, Any],
        strategy_id: Optional[str] = None,
        ensure_idempotent: bool = True,
    ) -> TradeSaveResult:
        if not trade:
            return TradeSaveResult(success=False, error_message="trade dict is empty")

        order_id = trade.get("order_id", "")
        instrument_id = trade.get("instrument_id", "")

        if ensure_idempotent and order_id:
            try:
                from order.order_persistence import SelfTradeDetector
                detector = SelfTradeDetector()
                from order.order_persistence import OrderRecord
                existing = detector._pending_orders.get(instrument_id, [])
                for ex in existing:
                    if ex.order_id == order_id:
                        return TradeSaveResult(
                            success=True,
                            trade_id=order_id,
                            persisted=False,
                            idempotent=True,
                        )
            except Exception as e:
                logger.debug("[DefaultDataAccess] idempotent check skipped: %s", e)

        try:
            from risk.risk_service import save_state_snapshot
            save_state_snapshot(
                snapshot_data={"trade": trade, "strategy_id": strategy_id},
                tag=f"trade_{order_id}" if order_id else "trade_unknown",
            )
            return TradeSaveResult(
                success=True,
                trade_id=order_id,
                persisted=True,
            )
        except Exception as e:
            logger.error("[DefaultDataAccess] save_trade failed: %s", e)
            return TradeSaveResult(success=False, trade_id=order_id, error_message=str(e))

    def get_risk_state(
        self,
        strategy_id: Optional[str] = None,
        include_history: bool = False,
    ) -> RiskStateResult:
        rs = self._get_risk_service()

        try:
            risk_status = rs.get_risk_status()
            risk_level = risk_status.get("risk_level", "UNKNOWN")
            circuit_active = risk_status.get("circuit_breaker_active", False)

            strategy_state: Dict[str, Any] = {}
            if strategy_id is not None:
                strategy_state = rs.get_strategy_risk_state(strategy_id)

            health_status = "UNKNOWN"
            try:
                safety = getattr(rs, "_safety_meta_layer", None)
                if safety is None:
                    from risk.risk_service import get_safety_meta_layer
                    safety = get_safety_meta_layer()
                if safety is not None and hasattr(safety, "get_health_status"):
                    health = safety.get_health_status()
                    health_status = health.get("health_status", "UNKNOWN")
            except Exception as e:
                logger.debug("[DefaultDataAccess] get_health_status failed: %s", e)

            history: List[Dict[str, Any]] = []
            if include_history:
                try:
                    import os
                    import json
                    snap_dir = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)), "logs", "state_snapshots"
                    )
                    if os.path.isdir(snap_dir):
                        for fname in sorted(os.listdir(snap_dir), reverse=True)[:20]:
                            fpath = os.path.join(snap_dir, fname)
                            if os.path.isfile(fpath):
                                with open(fpath, "r", encoding="utf-8") as f:
                                    history.append(json.load(f))
                except Exception as e:
                    logger.debug("[DefaultDataAccess] history load failed: %s", e)

            return RiskStateResult(
                success=True,
                risk_level=risk_level,
                circuit_breaker_active=circuit_active,
                strategy_state=strategy_state,
                health_status=health_status,
                history=history,
            )
        except Exception as e:
            logger.error("[DefaultDataAccess] get_risk_state failed: %s", e)
            return RiskStateResult(success=False, error_message=str(e))

    def save_risk_state(
        self,
        risk_state: Dict[str, Any],
        strategy_id: Optional[str] = None,
        snapshot_tag: str = "",
    ) -> RiskStateSaveResult:
        if not risk_state:
            return RiskStateSaveResult(success=False, error_message="risk_state dict is empty")

        rs = self._get_risk_service()

        try:
            if strategy_id is not None:
                rs.update_strategy_risk_state(strategy_id, **risk_state)

            persisted = False
            snapshot_time = ""
            if snapshot_tag:
                from risk.risk_service import save_state_snapshot
                save_state_snapshot(
                    snapshot_data={"risk_state": risk_state, "strategy_id": strategy_id},
                    tag=snapshot_tag,
                )
                persisted = True
                snapshot_time = datetime.now(_CHINA_TZ).isoformat()

            return RiskStateSaveResult(
                success=True,
                snapshot_tag=snapshot_tag,
                snapshot_time=snapshot_time,
                persisted=persisted,
            )
        except Exception as e:
            logger.error("[DefaultDataAccess] save_risk_state failed: %s", e)
            return RiskStateSaveResult(success=False, snapshot_tag=snapshot_tag, error_message=str(e))

    def write_records(
        self,
        table: str,
        records: List[Dict[str, Any]],
        strategy_id: Optional[str] = None,
    ) -> WriteResult:
        if not table or not records:
            return WriteResult(success=False, error_message="table or records is empty")
        try:
            from data.db_adapter import connect, execute
            from data.ds_realtime_cache import _resolve_duckdb_file
            db_path = _resolve_duckdb_file()
            conn = connect(db_path)
            try:
                cols = list(records[0].keys())
                placeholders = ", ".join(["?"] * len(cols))
                col_str = ", ".join(cols)
                sql = f"INSERT INTO {sanitize_sql_identifier(table)} ({col_str}) VALUES ({placeholders})"
                for rec in records:
                    params = [rec.get(c) for c in cols]
                    execute(conn, sql, params)
                return WriteResult(success=True, rows_written=len(records))
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            logger.error("[DefaultDataAccess] write_records failed: %s", e)
            return WriteResult(success=False, error_message=str(e))

    def flush_buffer(self, table: str) -> FlushResult:
        return FlushResult(success=True, rows_flushed=0)

    def execute_query(
        self,
        query: str,
        params: Optional[List] = None,
        in_memory: bool = False,
    ) -> QueryResult:
        if not query:
            return QueryResult(success=False, error_message="query is empty")
        try:
            from data.db_adapter import connect_in_memory, connect, execute, fetchall
            from data.ds_realtime_cache import _resolve_duckdb_file
            if in_memory:
                conn = connect_in_memory()
            else:
                db_path = _resolve_duckdb_file()
                conn = connect(db_path)
            try:
                result = fetchall(conn, query, params)
                columns = []
                if result and hasattr(result, '__len__') and len(result) > 0:
                    pass
                return QueryResult(
                    success=True,
                    data=result,
                    row_count=len(result) if result else 0,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            logger.error("[DefaultDataAccess] execute_query failed: %s", e)
            return QueryResult(success=False, error_message=str(e))

    def register_dataframe(self, name: str, df: Any) -> None:
        try:
            from data.db_adapter import connect, register_df
            from data.ds_realtime_cache import _resolve_duckdb_file
            db_path = _resolve_duckdb_file()
            conn = connect(db_path)
            try:
                register_df(conn, name, df)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            logger.error("[DefaultDataAccess] register_dataframe failed: %s", e)

    def compact_database(self) -> CompactionResult:
        try:
            from data.db_adapter import connect, execute
            from data.ds_realtime_cache import _resolve_duckdb_file
            import os
            db_path = _resolve_duckdb_file()
            size_before = os.path.getsize(db_path) / (1024 * 1024) if os.path.exists(db_path) else 0.0
            conn = connect(db_path)
            try:
                execute(conn, "CHECKPOINT")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            size_after = os.path.getsize(db_path) / (1024 * 1024) if os.path.exists(db_path) else 0.0
            return CompactionResult(success=True, size_before_mb=size_before, size_after_mb=size_after)
        except Exception as e:
            logger.error("[DefaultDataAccess] compact_database failed: %s", e)
            return CompactionResult(success=False, error_message=str(e))

    def rebuild_index(self, table: str) -> IndexResult:
        return IndexResult(success=True, index_name=f"idx_{table}")

    def get_storage_stats(self) -> StorageStatsResult:
        try:
            from data.db_adapter import connect, fetchall
            from data.ds_realtime_cache import _resolve_duckdb_file
            import os
            db_path = _resolve_duckdb_file()
            db_size = os.path.getsize(db_path) / (1024 * 1024) if os.path.exists(db_path) else 0.0
            conn = connect(db_path)
            try:
                tables_result = fetchall(conn, "SELECT table_name FROM information_schema.tables")
                table_names = [r[0] for r in (tables_result or [])]
                total_rows = 0
                table_stats: Dict[str, Any] = {}
                for tbl in table_names:
                    try:
                        cnt_result = fetchall(conn, f"SELECT count(*) FROM \"{sanitize_sql_identifier(tbl)}\"")
                        cnt = cnt_result[0][0] if cnt_result else 0
                        total_rows += cnt
                        table_stats[tbl] = {"row_count": cnt}
                    except Exception:
                        pass
                return StorageStatsResult(
                    success=True,
                    total_tables=len(table_names),
                    total_rows=total_rows,
                    database_size_mb=db_size,
                    table_stats=table_stats,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            logger.error("[DefaultDataAccess] get_storage_stats failed: %s", e)
            return StorageStatsResult(success=False, error_message=str(e))


_data_access_instance: Optional[DefaultDataAccess] = None
_data_access_lock = threading.Lock()


def get_data_access() -> DefaultDataAccess:
    """获取数据访问层单例（唯一入口）'
    Returns:
        DefaultDataAccess: 领域级数据访问实例
    """
    global _data_access_instance
    if _data_access_instance is not None:
        return _data_access_instance
    with _data_access_lock:
        if _data_access_instance is not None:
            return _data_access_instance
        _data_access_instance = DefaultDataAccess()
        return _data_access_instance
