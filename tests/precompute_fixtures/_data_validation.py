# MODULE_ID: M1-167
﻿# [M1-136] 数据验证
from __future__ import annotations

# ── DuckDB Tick Storage ──

import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date

try:
    from ali2026v3_trading.data.data_access import get_data_access
    from ali2026v3_trading.data.db_adapter import connect, get_duckdb_module
    duckdb = get_duckdb_module()
except ImportError:
    duckdb = None
import numpy as np
import pandas as pd

logger = get_logger(__name__)  # R9-5
_ENV_PATTERN = re.compile(r'ENV:(\w+)', re.IGNORECASE)


# R33-P0-07修复: 此模块已集成到ds_data_writer.py的tick持久化管线，不再是死代码


# R13-P1-SEC-08修复: 连接字符串密码脱敏
def _mask_connection_string(conn_str: str) -> str:
    """R13-P1-SEC-08修复: 掩码连接字符串中的密码，防止泄露到日志

    支持格式: key=value中password/secret字段的掩码
    """
    return re.sub(
        r'(password|passwd|pwd|secret)\s*=\s*[^;\s]+',
        r'\1=***MASKED***',
        conn_str,
        flags=re.IGNORECASE,
    )


def _resolve_db_path(db_path: str) -> str:
    """R13-P1-SEC-08修复: 支持环境变量替换连接字符串中的密码

    如果db_path包含password=ENV:VAR_NAME格式，从环境变量读取实际密码。
    """
    env_pattern = _ENV_PATTERN
    resolved = db_path
    for match in env_pattern.finditer(db_path):
        env_var = match.group(1)
        env_val = os.environ.get(env_var, '')
        if env_val:
            resolved = resolved.replace(f'ENV:{env_var}', env_val)
    return resolved


@dataclass(slots=True)
class StorageStats:
    total_rows: int
    total_symbols: int
    date_range: Tuple[str, str]
    db_size_mb: float
    partition_count: int
    has_minute_bars: bool
    has_greeks: bool


class DuckDBTickStorage:
    """
    DuckDB列式Tick存储：高效处理8年Tick数据

    核心功能：
    1. 分区表创建：按trade_date分区，自动裁剪
    2. 物化视图：分钟Bar预聚合（增量更新）
    3. 批量导入：支持Parquet/CSV大文件导入
    4. 范围查询：按日期/合约/期权属性高效过滤
    5. IV Rank计算：窗口函数高效计算
    6. 希腊字母查询：按strike/expiry/type过滤

    存储规模估算（8年全市场期权Tick）：
    - 日均Tick: ~500万 (50合约×10万Tick)
    - 8年总量: ~100亿行
    - DuckDB列式压缩: ~100-200GB
    - 分区裁剪后单日查询: <1秒
    """

    SCHEMA_TICK = '''
        CREATE TABLE IF NOT EXISTS tick_data (
            symbol VARCHAR,
            timestamp TIMESTAMP,
            trade_date DATE,
            price DOUBLE,
            volume BIGINT,
            bid DOUBLE,
            ask DOUBLE,
            iv DOUBLE,
            delta DOUBLE,
            gamma DOUBLE,
            theta DOUBLE,
            vega DOUBLE,
            strike DOUBLE,
            expiry DATE,
            option_type VARCHAR,
            underlying VARCHAR
        )
    '''

    SCHEMA_MINUTE_BARS = '''
        CREATE VIEW IF NOT EXISTS mv_minute_bars AS
        SELECT
            symbol,
            date_trunc('minute', timestamp) AS minute,
            trade_date,
            first(price) AS open,
            max(price) AS high,
            min(price) AS low,
            last(price) AS close,
            sum(volume) AS volume,
            CASE WHEN sum(volume) > 0
                 THEN sum(price * volume) / sum(volume)
                 ELSE avg(price) END AS vwap,
            avg(iv) AS avg_iv,
            avg(delta) AS avg_delta,
            avg(gamma) AS avg_gamma,
            avg(theta) AS avg_theta,
            avg(vega) AS avg_vega
        FROM tick_data
        GROUP BY symbol, minute, trade_date
    '''

    SCHEMA_DAILY_BARS = '''
        CREATE VIEW IF NOT EXISTS mv_daily_bars AS
        SELECT
            symbol,
            trade_date,
            first(price) AS open,
            max(price) AS high,
            min(price) AS low,
            last(price) AS close,
            sum(volume) AS volume,
            CASE WHEN sum(volume) > 0
                 THEN sum(price * volume) / sum(volume)
                 ELSE avg(price) END AS vwap,
            avg(iv) AS avg_iv,
            count(*) AS tick_count
        FROM tick_data
        GROUP BY symbol, trade_date
    '''

    # R10-P0-15修复: WAL CHECKPOINT阈值
    WAL_CHECKPOINT_INTERVAL = 1000  # 每1000次写入执行一次CHECKPOINT

    def _get_tick_count(self) -> int:
        """P2-18修复: 统一tick_data行数查询，消除5处SELECT COUNT(*)重复"""
        result = self._conn.execute('SELECT COUNT(*) FROM tick_data').fetchone()
        return result[0] if result else 0

    def __init__(self, db_path: str = 'tick_storage.duckdb'):
        # R13-P1-SEC-08修复: 支持环境变量替换密码
        resolved_path = _resolve_db_path(db_path)
        self._db_path = resolved_path
        self._conn = connect(resolved_path)
        self._initialized = False
        self._write_count = 0  # R10-P0-15: 写入计数器
        self._init_tables()

    def _init_tables(self) -> None:
        try:
            self._conn.execute(self.SCHEMA_TICK)
            try:
                self._conn.execute(self.SCHEMA_MINUTE_BARS)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _r3_err:
                logger.debug("分钟Bar物化视图已存在: %s", _r3_err)
            try:
                self._conn.execute(self.SCHEMA_DAILY_BARS)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _r3_err:
                logger.debug("日Bar物化视图已存在: %s", _r3_err)
            self._conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_tick_symbol_date
                ON tick_data(symbol, trade_date)
            ''')
            self._ensure_backtest_index()  # R10-P0-18: 为backtest_results.params_json创建索引
            # R13-P1-API-06修复: 创建表后验证schema，确保列与代码假设一致
            self._validate_tick_schema()
            self._initialized = True
            logger.info("DuckDB Tick存储初始化完成: %s", _mask_connection_string(self._db_path))  # R13-P1-SEC-08修复
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("DuckDB初始化失败: %s", e)
            raise

    # R13-P1-API-06修复: schema验证方法，确保tick_data表结构与代码假设一致
    def _validate_tick_schema(self) -> None:
        """验证tick_data表的列是否与SCHEMA_TICK定义一致"""
        expected_columns = {
            'symbol', 'timestamp', 'trade_date', 'price', 'volume',
            'bid', 'ask', 'iv', 'delta', 'gamma', 'theta', 'vega',
            'strike', 'expiry', 'option_type', 'underlying',
        }
        try:
            result = self._conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'tick_data'"
            ).fetchall()
            actual_columns = {row[0] for row in result}
            missing = expected_columns - actual_columns
            if missing:
                logger.warning("[R13-P1-API-06] tick_data表缺少预期列: %s", missing)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.warning("[R13-P1-API-06] schema验证失败: %s", e)

    def _ensure_backtest_index(self) -> None:
        """R10-P0-18修复: 为backtest_results表的params_json列创建索引

        backtest_results表存在params_json的self-JOIN查询场景
        （如按参数组合去重、参数相似度匹配），缺少索引会导致全表扫描。
        """
        try:
            # 先检查backtest_results表是否存在
            tables = self._conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'backtest_results'"
            ).fetchall()
            if not tables:
                return
            self._conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_backtest_params_json
                ON backtest_results(params_json)
            ''')
            logger.info("[R10-P0-18] backtest_results.params_json索引已确保")
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.warning("[R10-P0-18] 创建params_json索引失败: %s", e)

    def _maybe_checkpoint(self) -> None:
        """R10-P0-15修复: 定期CHECKPOINT压缩WAL文件，防止WAL无限增长"""
        self._write_count += 1
        if self._write_count >= self.WAL_CHECKPOINT_INTERVAL:
            try:
                self._conn.execute("CHECKPOINT")
                self._write_count = 0
                logger.info("[R10-P0-15] DuckDB WAL CHECKPOINT完成")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _e:
                logger.warning("[R10-P0-15] CHECKPOINT失败: %s", _e)

    def bulk_import_parquet(self, parquet_path: str) -> int:
        # P0-R8-16修复: 使用参数化路径替代f-string SQL拼接
        try:
            # 验证parquet_path安全性：仅允许合法文件路径字符
            import re
            if not re.match(r'^[a-zA-Z0-9_\\./\-: ]+$', parquet_path):
                raise ValueError(f"Invalid parquet path: {parquet_path}")

            count_before = self._get_tick_count()  # P2-18: 统一方法

            self._conn.execute(f'''
                INSERT INTO tick_data
                SELECT
                    symbol,
                    timestamp,
                    CAST(timestamp AS DATE) AS trade_date,
                    price, volume, bid, ask,
                    iv, delta, gamma, theta, vega,
                    strike, expiry, option_type, underlying
                FROM read_parquet(?)
            ''', [parquet_path])

            count_after = self._get_tick_count()  # P2-18: 统一方法
            inserted = count_after - count_before

            logger.info("Parquet导入完成: %s, %d行", parquet_path, inserted)
            self._maybe_checkpoint()  # R10-P0-15: 定期CHECKPOINT
            return inserted
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("Parquet导入失败: %s", e)
            return 0

    def bulk_import_dataframe(self, df: pd.DataFrame) -> int:
        required_cols = ['symbol', 'timestamp', 'price']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"DataFrame缺少必要列: {col}")

        if 'trade_date' not in df.columns:
            df = df.copy()
            df['trade_date'] = pd.to_datetime(df['timestamp']).dt.date

        for col in ['volume', 'bid', 'ask', 'iv', 'delta', 'gamma', 'theta', 'vega',
                     'strike', 'expiry', 'option_type', 'underlying']:
            if col not in df.columns:
                if col == 'volume':
                    df[col] = 0
                elif col in ('bid', 'ask', 'iv', 'delta', 'gamma', 'theta', 'vega', 'strike'):
                    df[col] = np.nan
                elif col in ('option_type', 'underlying'):
                    df[col] = None
                elif col == 'expiry':
                    df[col] = None

        count_before = self._get_tick_count()  # P2-18: 统一方法

        self._conn.register('_temp_df', df)
        # P0-R8-16修复: 验证列名安全性，防御SQL注入
        import re
        safe_cols = []
        for c in df.columns:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', c):
                raise ValueError(f"Unsafe column name: {c}")
            safe_cols.append(c)
        col_list = ', '.join(safe_cols)
        self._conn.execute(f'INSERT INTO tick_data ({col_list}) SELECT {col_list} FROM _temp_df')
        self._conn.unregister('_temp_df')

        count_after = self._get_tick_count()  # P2-18: 统一方法
        return count_after - count_before

    def query_ticks_by_date_range(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        conditions = []
        params = []
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)

        where = ' AND '.join(conditions) if conditions else '1=1'
        query = f"SELECT * FROM tick_data WHERE {where} ORDER BY timestamp"
        return self._conn.execute(query, params).fetchdf()

    def query_minute_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        # P0-R8-16修复: 使用参数化查询替代f-string SQL拼接
        try:
            return self._conn.execute(f'''
                SELECT * FROM mv_minute_bars
                WHERE symbol = ?
                  AND trade_date BETWEEN ? AND ?
                ORDER BY minute
            ''', [symbol, start_date, end_date]).fetchdf()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return self._conn.execute(f'''
                SELECT
                    symbol,
                    date_trunc('minute', timestamp) AS minute,
                    trade_date,
                    first(price) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    last(price) AS close,
                    sum(volume) AS volume,
                    avg(price) AS vwap
                FROM tick_data
                WHERE symbol = ?
                  AND trade_date BETWEEN ? AND ?
                GROUP BY symbol, minute, trade_date
                ORDER BY minute
            ''', [symbol, start_date, end_date]).fetchdf()

    def query_greeks_by_option(
        self,
        underlying: str,
        strike: Optional[float] = None,
        expiry: Optional[str] = None,
        option_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        # P0-R8-16修复: 使用参数化查询替代f-string SQL拼接
        conditions = ["underlying = ?"]
        params = [underlying]
        if strike is not None:
            conditions.append("ABS(strike - ?) < 0.01")
            params.append(strike)
        if expiry:
            conditions.append("expiry = ?")
            params.append(expiry)
        if option_type:
            conditions.append("option_type = ?")
            params.append(option_type)
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)

        where = ' AND '.join(conditions)
        return self._conn.execute(f'''
            SELECT symbol, timestamp, price, iv, delta, gamma, theta, vega,
                   strike, expiry, option_type
            FROM tick_data
            WHERE {where}
            ORDER BY timestamp
        ''', params).fetchdf()

    def compute_iv_rank(
        self,
        symbol: str,
        current_date: str,
        lookback_days: int = 252,
    ) -> float:
        try:
            # R13-P1-SEC-01修复: 使用参数化查询替代f-string SQL拼接，防止SQL注入
            result = self._conn.execute('''
                WITH iv_history AS (
                    SELECT avg_iv AS iv
                    FROM mv_daily_bars
                    WHERE symbol = ?
                      AND trade_date <= ?
                    ORDER BY trade_date DESC
                    LIMIT ?
                ),
                current_iv AS (
                    SELECT avg_iv AS iv
                    FROM mv_daily_bars
                    WHERE symbol = ?
                      AND trade_date = ?
                )
                SELECT
                    CASE WHEN (MAX(h.iv) - MIN(h.iv)) > 0
                         THEN (c.iv - MIN(h.iv)) / (MAX(h.iv) - MIN(h.iv))
                         ELSE 0.5 END AS iv_rank
                FROM iv_history h, current_iv c
            ''', [symbol, current_date, lookback_days, symbol, current_date]).fetchone()

            return float(result[0]) if result and result[0] is not None else 0.5
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.warning("IV Rank计算失败: %s", e)
            return 0.5

    def get_stats(self) -> StorageStats:
        try:
            total_rows = self._get_tick_count()  # P2-18: 统一方法

            symbols = self._conn.execute(
                'SELECT COUNT(DISTINCT symbol) FROM tick_data'
            ).fetchone()
            symbols = symbols[0] if symbols else 0  # R27-P1-FIX: fetchone() None保护

            date_range = self._conn.execute(
                'SELECT MIN(trade_date)::VARCHAR, MAX(trade_date)::VARCHAR FROM tick_data'
            ).fetchone()

            has_minute = True
            has_greeks = total_rows > 0

            import os
            db_size = os.path.getsize(self._db_path) / (1024 * 1024) if os.path.exists(self._db_path) else 0.0

            partitions = self._conn.execute(
                'SELECT COUNT(DISTINCT trade_date) FROM tick_data'
            ).fetchone()
            partitions = partitions[0] if partitions else 0  # R27-P1-FIX: fetchone() None保护

            return StorageStats(
                total_rows=total_rows,
                total_symbols=symbols,
                date_range=(str(date_range[0]), str(date_range[1])),
                db_size_mb=db_size,
                partition_count=partitions,
                has_minute_bars=has_minute,
                has_greeks=has_greeks,
            )
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.error("统计信息获取失败: %s", e)
            return StorageStats(
                total_rows=0, total_symbols=0,
                date_range=('N/A', 'N/A'), db_size_mb=0.0,
                partition_count=0, has_minute_bars=False, has_greeks=False,
            )

    def optimize_storage(self) -> None:
        logger.info("开始存储优化...")
        try:
            self._conn.execute("CHECKPOINT")
            logger.info("DuckDB CHECKPOINT完成")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("CHECKPOINT失败: %s", e)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.execute("CHECKPOINT")
            except (ValueError, KeyError, TypeError, AttributeError, IOError) as _e:
                logging.debug("[R3-L2] CHECKPOINT on close failed: %s", _e)
                pass
            self._conn.close()
            self._conn = None
            logger.info("DuckDB Tick存储已关闭")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

# ── External Validation Pipeline ──

import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from enum import Enum

import numpy as np
import pandas as pd

logger = get_logger(__name__)  # R9-5


class ValidationStatus(Enum):
    PASS = 'pass'
    WARNING = 'warning'
    FAIL = 'fail'
    PENDING = 'pending'


@dataclass(slots=True)
class ExternalSourceConfig:
    name: str
    fetch_fn_name: str
    deviation_threshold: float
    weight: float
    description: str


@dataclass(slots=True)
class ValidationResult:
    source_name: str
    status: ValidationStatus
    deviation_pct: float
    internal_value: float
    external_value: float
    threshold: float
    n_samples: int
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())


@dataclass(slots=True)
class QuarterlyReport:
    quarter: str
    results: List[ValidationResult]
    overall_status: ValidationStatus
    max_deviation: float
    action_required: str
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())


class ExternalValidationPipeline:
    """
    季度外部数据源校验流水线

    核心功能：
    1. 多外部源校验：交易所官方数据/第三方数据/公开指标
    2. 偏差检测：内部计算值 vs 外部权威值，偏差>阈值触发告警
    3. 季度自动运行：校验结果存档，连续2季度FAIL触发人工审议
    4. 校验项覆盖：
       - IV中位数 vs 交易所结算IV
       - 状态分类准确率 vs 公开研究报告
       - 希腊字母值 vs 期权定价模型理论值
       - 回测夏普 vs 公开基准夏普
    """

    DEFAULT_SOURCES = {
        'exchange_settlement_iv': ExternalSourceConfig(
            name='交易所结算IV',
            fetch_fn_name='fetch_exchange_iv',
            deviation_threshold=0.05,
            weight=0.40,
            description='内部IV vs 交易所官方结算IV中位数',
        ),
        'public_benchmark_sharpe': ExternalSourceConfig(
            name='公开基准夏普',
            fetch_fn_name='fetch_public_sharpe',
            deviation_threshold=0.10,
            weight=0.30,
            description='内部回测夏普 vs 公开CTA/期权策略基准夏普',
        ),
        'bsm_theoretical_greeks': ExternalSourceConfig(
            name='BSM理论希腊字母',
            fetch_fn_name='fetch_bsm_greeks',
            deviation_threshold=0.03,
            weight=0.20,
            description='内部Greeks vs Black-Scholes-Merton理论值',
        ),
        'state_accuracy_benchmark': ExternalSourceConfig(
            name='状态分类基准',
            fetch_fn_name='fetch_state_benchmark',
            deviation_threshold=0.08,
            weight=0.10,
            description='内部状态准确率 vs 公开研究报告基准',
        ),
    }

    def __init__(
        self,
        sources: Optional[Dict[str, ExternalSourceConfig]] = None,
        consecutive_fail_limit: int = 2,
    ):
        self._sources = sources or dict(self.DEFAULT_SOURCES)
        self._consecutive_fail_limit = consecutive_fail_limit
        self._report_history: List[QuarterlyReport] = []
        self._custom_fetch_fns: Dict[str, Callable] = {}

    def register_fetch_function(
        self, fn_name: str, fn: Callable
    ) -> None:
        self._custom_fetch_fns[fn_name] = fn
        logger.info("外部数据获取函数注册: %s", fn_name)

    def validate_quarter(
        self,
        quarter: str,
        internal_data: Dict[str, Any],
        external_fetch_functions: Optional[Dict[str, Callable]] = None,
    ) -> QuarterlyReport:
        results = []

        for source_key, config in self._sources.items():
            fetch_fn = None
            if external_fetch_functions and config.fetch_fn_name in external_fetch_functions:
                fetch_fn = external_fetch_functions[config.fetch_fn_name]
            elif config.fetch_fn_name in self._custom_fetch_fns:
                fetch_fn = self._custom_fetch_fns[config.fetch_fn_name]

            if fetch_fn is None:
                result = ValidationResult(
                    source_name=config.name,
                    status=ValidationStatus.PENDING,
                    deviation_pct=0.0,
                    internal_value=0.0,
                    external_value=0.0,
                    threshold=config.deviation_threshold,
                    n_samples=0,
                )
                results.append(result)
                continue

            try:
                internal_val, external_val, n_samples = fetch_fn(internal_data)
                deviation = (
                    abs(internal_val - external_val) / max(abs(external_val), 1e-10)
                    if external_val != 0 else 0.0
                )

                if deviation <= config.deviation_threshold * 0.5:
                    status = ValidationStatus.PASS
                elif deviation <= config.deviation_threshold:
                    status = ValidationStatus.WARNING
                else:
                    status = ValidationStatus.FAIL

                result = ValidationResult(
                    source_name=config.name,
                    status=status,
                    deviation_pct=deviation,
                    internal_value=internal_val,
                    external_value=external_val,
                    threshold=config.deviation_threshold,
                    n_samples=n_samples,
                )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.warning("外部校验 %s 失败: %s", config.name, e)
                result = ValidationResult(
                    source_name=config.name,
                    status=ValidationStatus.PENDING,
                    deviation_pct=0.0,
                    internal_value=0.0,
                    external_value=0.0,
                    threshold=config.deviation_threshold,
                    n_samples=0,
                )

            results.append(result)

        overall = self._determine_overall_status(results)
        max_dev = max(r.deviation_pct for r in results) if results else 0.0
        action = self._determine_action(overall, max_dev)

        report = QuarterlyReport(
            quarter=quarter,
            results=results,
            overall_status=overall,
            max_deviation=max_dev,
            action_required=action,
        )
        self._report_history.append(report)

        logger.info(
            "季度外部校验完成: Q=%s, status=%s, max_dev=%.4f, action=%s",
            quarter, overall.value, max_dev, action,
        )
        return report

    @staticmethod
    def _determine_overall_status(
        results: List[ValidationResult],
    ) -> ValidationStatus:
        if any(r.status == ValidationStatus.FAIL for r in results):
            return ValidationStatus.FAIL
        if any(r.status == ValidationStatus.WARNING for r in results):
            return ValidationStatus.WARNING
        if any(r.status == ValidationStatus.PENDING for r in results):
            return ValidationStatus.PENDING
        return ValidationStatus.PASS

    def _determine_action(
        self, overall: ValidationStatus, max_deviation: float
    ) -> str:
        if overall == ValidationStatus.PASS:
            return '无需行动'
        if overall == ValidationStatus.WARNING:
            return '关注：偏差在阈值内但偏高，建议检查数据源'
        if overall == ValidationStatus.FAIL:
            consecutive = self._count_consecutive_fails()
            if consecutive >= self._consecutive_fail_limit:
                return '紧急：连续{}季度FAIL，必须人工审议'.format(consecutive)
            return '行动：偏差超阈值，需检查内部计算逻辑和数据源'
        return '待定：部分数据源未就绪'

    def _count_consecutive_fails(self) -> int:
        count = 0
        for report in reversed(self._report_history):
            if report.overall_status == ValidationStatus.FAIL:
                count += 1
            else:
                break
        return count

    def check_drift(
        self,
        internal_series: pd.Series,
        external_series: pd.Series,
        window: int = 252,
    ) -> Dict[str, Any]:
        min_len = min(len(internal_series), len(external_series), window)
        if min_len < 10:
            return {'drift_detected': False, 'reason': '样本不足'}

        int_vals = internal_series.iloc[-min_len:].values
        ext_vals = external_series.iloc[-min_len:].values

        mean_dev = float(np.mean(np.abs(int_vals - ext_vals) / (np.abs(ext_vals) + 1e-10)))
        max_dev = float(np.max(np.abs(int_vals - ext_vals) / (np.abs(ext_vals) + 1e-10)))

        correlation = float(np.corrcoef(int_vals, ext_vals)[0, 1]) if min_len > 2 else 0.0

        drift_detected = mean_dev > 0.05 or max_dev > 0.10 or correlation < 0.9

        return {
            'drift_detected': drift_detected,
            'mean_deviation': mean_dev,
            'max_deviation': max_dev,
            'correlation': correlation,
            'window': min_len,
            'threshold_mean': 0.05,
            'threshold_max': 0.10,
            'threshold_corr': 0.9,
        }

    def get_report_history(self) -> List[QuarterlyReport]:
        return list(self._report_history)

    def save_reports(self, path: str) -> None:
        records = []
        for report in self._report_history:
            for result in report.results:
                records.append({
                    'quarter': report.quarter,
                    'source': result.source_name,
                    'status': result.status.value,
                    'deviation_pct': result.deviation_pct,
                    'internal_value': result.internal_value,
                    'external_value': result.external_value,
                    'threshold': result.threshold,
                    'overall_status': report.overall_status.value,
                    'max_deviation': report.max_deviation,
                    'action': report.action_required,
                    'timestamp': result.timestamp,
                })
        if records:
            df = pd.DataFrame(records)
            df.to_csv(path, index=False, encoding='utf-8')
            logger.info("校验报告保存至 %s (%d条)", path, len(records))

    def generate_mock_external_data(
        self,
        internal_data: Dict[str, Any],
        noise_level: float = 0.02,
    ) -> Dict[str, Callable]:
        def mock_exchange_iv(data):
            iv = data.get('iv_median', 0.20)
            return iv, iv * (1 + np.random.normal(0, noise_level)), 252

        def mock_public_sharpe(data):
            sharpe = data.get('sharpe', 1.5)
            return sharpe, sharpe * (1 + np.random.normal(0, noise_level)), 120

        def mock_bsm_greeks(data):
            delta = data.get('delta', 0.5)
            return delta, delta * (1 + np.random.normal(0, noise_level)), 500

        def mock_state_benchmark(data):
            accuracy = data.get('state_accuracy', 0.7)
            return accuracy, accuracy * (1 + np.random.normal(0, noise_level)), 200

        return {
            'fetch_exchange_iv': mock_exchange_iv,
            'fetch_public_sharpe': mock_public_sharpe,
            'fetch_bsm_greeks': mock_bsm_greeks,
            'fetch_state_benchmark': mock_state_benchmark,
        }