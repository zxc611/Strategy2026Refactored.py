"""ds_option_sync.py - 期权同步状态计算Mixin

从data_service.py拆分出的期权同步职责，包括：
- _refresh_option_sync_stats: 刷新期权同步虚值统计物化视图数据
- _update_option_status_columns: 为期货和期权合约批量计算并更新同步状态列
"""
import logging

logger = logging.getLogger(__name__)


class OptionSyncMixin:
    """期权同步状态计算Mixin - 由DataService组合使用"""

    def _refresh_option_sync_stats(self):
        """
        刷新期权同步虚值统计物化视图数据
        
        功能说明：
        1. 清空旧数据
        2. 重新计算所有期权的同步状态和虚值数量
        3. 按月聚合统计结果
        
        调用时机：
        - 初始化时填充数据
        - 定期刷新（如每日收盘后）
        - 手动触发刷新
        """
        logger.info("Refreshing option_sync_otm_stats data...")
        conn = self._get_connection()
        
        try:
            conn.execute("DELETE FROM option_sync_otm_stats")
            
            conn.execute("""
                INSERT INTO option_sync_otm_stats (
                    month, underlying_symbol, option_type,
                    correct_rise_otm_count, wrong_rise_otm_count, 
                    correct_fall_otm_count, wrong_fall_otm_count, other_otm_count,
                    total_otm_count, total_samples, calculated_at
                )
                WITH option_base AS (
                    SELECT
                        t.instrument_id,
                        t.timestamp,
                        t.last_price AS option_price,
                        oi.underlying_future_id,
                        fi.product AS underlying_symbol,
                        COALESCE(t.option_type, oi.option_type) AS option_type,
                        COALESCE(t.strike_price, oi.strike_price) AS strike_price,
                        f.last_price AS underlying_price
                    FROM ticks_raw t
                    INNER JOIN option_instruments oi ON oi.instrument_id = t.instrument_id
                    INNER JOIN futures_instruments fi ON fi.internal_id = oi.underlying_future_id
                    ASOF LEFT JOIN ticks_raw f
                        ON f.instrument_id = fi.instrument_id
                        AND t.timestamp >= f.timestamp
                    WHERE oi.underlying_future_id IS NOT NULL
                ),
                option_with_lag AS (
                    SELECT
                        *,
                        LAG(underlying_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_underlying_price,
                        LAG(option_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_option_price
                    FROM option_base
                    WHERE underlying_price IS NOT NULL
                ),
                option_calculated AS (
                    SELECT
                        *,
                        CASE 
                            WHEN option_type = 'CALL' AND underlying_price < strike_price THEN TRUE
                            WHEN option_type = 'PUT' AND underlying_price > strike_price THEN TRUE
                            ELSE FALSE
                        END AS is_otm,
                        CASE 
                            WHEN underlying_price IS NULL OR prev_underlying_price IS NULL OR prev_option_price IS NULL THEN 'other'
                            WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                            WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                            WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                            WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                            WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                            WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                            WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                            WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                            ELSE 'other'
                        END AS sync_status
                    FROM option_with_lag
                    WHERE prev_underlying_price IS NOT NULL AND prev_option_price IS NOT NULL
                )
                SELECT
                    strftime(timestamp, '%Y%m') AS month,
                    underlying_symbol,
                    option_type,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'correct_rise') AS correct_rise_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'wrong_rise') AS wrong_rise_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'correct_fall') AS correct_fall_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'wrong_fall') AS wrong_fall_otm_count,
                    COUNT(*) FILTER (WHERE is_otm AND sync_status = 'other') AS other_otm_count,
                    COUNT(*) FILTER (WHERE is_otm) AS total_otm_count,
                    COUNT(*) AS total_samples,
                    MAX(timestamp) AS calculated_at
                FROM option_calculated
                GROUP BY strftime(timestamp, '%Y%m'), underlying_symbol, option_type
                ORDER BY month, underlying_symbol, option_type
            """)
            
            logger.info(f"Refreshed option_sync_otm_stats successfully")
            
        except Exception as e:
            logger.error(f"Failed to refresh option_sync_otm_stats: {e}")
            raise

    def _update_option_status_columns(self, conn):
        """
        为期货和期权合约批量计算并更新同步状态列
        
        核心逻辑：
        1. 识别期权合约（instrument_id 包含 -C- 或 -P-）→ 计算与期货的同步
        2. 识别期货合约（非期权）→ 计算与主力合约的同步
        3. 批量更新 ticks_raw 表
        
        优化：使用 ASOF JOIN 替代 LEFT JOIN LATERAL，将 O(N*M) 降为 O(N log M)。
        """
        logger.info("Updating option and future status columns...")
        
        try:
            # ========== 第一部分：期权同步状态计算 ==========
            conn.execute("""
                CREATE OR REPLACE TEMP VIEW option_status_calc AS
                WITH option_base AS (
                    SELECT
                        t.rowid AS row_id,
                        t.instrument_id,
                        t.timestamp,
                        t.last_price AS option_price,
                        oi.underlying_future_id,
                        fi.product AS underlying_symbol,
                        COALESCE(t.option_type, oi.option_type) AS option_type,
                        COALESCE(t.strike_price, oi.strike_price) AS strike_price,
                        f.last_price AS underlying_price
                    FROM ticks_raw t
                    INNER JOIN option_instruments oi ON oi.instrument_id = t.instrument_id
                    INNER JOIN futures_instruments fi ON fi.internal_id = oi.underlying_future_id
                    ASOF LEFT JOIN ticks_raw f
                        ON f.instrument_id = fi.instrument_id
                        AND t.timestamp >= f.timestamp
                    WHERE oi.underlying_future_id IS NOT NULL
                ),
                option_with_lag AS (
                    SELECT
                        row_id,
                        *,
                        LAG(underlying_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_underlying_price,
                        LAG(option_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_option_price
                    FROM option_base
                    WHERE underlying_price IS NOT NULL
                )
                SELECT
                    row_id,
                    CASE 
                        WHEN option_type = 'CALL' AND underlying_price < strike_price THEN TRUE
                        WHEN option_type = 'PUT' AND underlying_price > strike_price THEN TRUE
                        ELSE FALSE
                    END AS is_otm,
                    CASE 
                        WHEN underlying_price IS NULL OR prev_underlying_price IS NULL OR prev_option_price IS NULL THEN 'other'
                        WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                        WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'correct_rise'
                        WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                        WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price > prev_option_price THEN 'wrong_rise'
                        WHEN option_type = 'CALL' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                        WHEN option_type = 'PUT' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'correct_fall'
                        WHEN option_type = 'CALL' AND underlying_price > prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                        WHEN option_type = 'PUT' AND underlying_price < prev_underlying_price AND option_price < prev_option_price THEN 'wrong_fall'
                        ELSE 'other'
                    END AS sync_status,
                    NULL AS future_sync_status,
                    NULL AS is_same_rise,
                    NULL AS is_same_fall,
                    NULL AS is_diff_sync
                FROM option_with_lag
                WHERE prev_underlying_price IS NOT NULL AND prev_option_price IS NOT NULL
            """)
            
            conn.execute("""
                UPDATE ticks_raw
                SET 
                    is_otm = calc.is_otm,
                    sync_status = calc.sync_status,
                    future_sync_status = calc.future_sync_status,
                    is_same_rise = calc.is_same_rise,
                    is_same_fall = calc.is_same_fall,
                    is_diff_sync = calc.is_diff_sync
                FROM option_status_calc calc
                WHERE ticks_raw.rowid = calc.row_id
            """)
            
            logger.info("Option status columns updated.")
            
            # ========== 第二部分：期货同步状态计算 ==========
            conn.execute("""
                CREATE OR REPLACE TEMP VIEW future_status_calc AS
                WITH future_base AS (
                    SELECT
                        t.rowid AS row_id,
                        t.instrument_id,
                        t.timestamp,
                        t.last_price AS future_price,
                        fi.product AS underlying_symbol,
                        ref.last_price AS main_future_price,
                        ref.volume AS main_volume
                    FROM ticks_raw t
                    JOIN futures_instruments fi ON t.instrument_id = fi.instrument_id
                    ASOF LEFT JOIN (
                        SELECT ref.instrument_id, ref.timestamp, ref.last_price, ref.volume
                        FROM ticks_raw ref
                        JOIN futures_instruments fi2 ON ref.instrument_id = fi2.instrument_id
                    ) ref
                        ON t.timestamp >= ref.timestamp
                        AND fi.product = (SELECT fi2.product FROM futures_instruments fi2 WHERE fi2.instrument_id = ref.instrument_id)
                ),
                future_with_lag AS (
                    SELECT
                        row_id,
                        *,
                        LAG(future_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_future_price,
                        LAG(main_future_price) OVER (PARTITION BY instrument_id ORDER BY timestamp) AS prev_main_future_price
                    FROM future_base
                    WHERE main_future_price IS NOT NULL
                )
                SELECT
                    row_id,
                    CASE 
                        WHEN future_price > prev_future_price 
                             AND main_future_price > prev_main_future_price
                            THEN 'same_rise'
                        WHEN future_price < prev_future_price 
                             AND main_future_price < prev_main_future_price
                            THEN 'same_fall'
                        ELSE 'diff'
                    END AS future_sync_status,
                    CASE 
                        WHEN future_price > prev_future_price 
                             AND main_future_price > prev_main_future_price
                            THEN TRUE ELSE FALSE 
                    END AS is_same_rise,
                    CASE 
                        WHEN future_price < prev_future_price 
                             AND main_future_price < prev_main_future_price
                            THEN TRUE ELSE FALSE 
                    END AS is_same_fall,
                    CASE 
                        WHEN NOT (
                            (future_price > prev_future_price AND main_future_price > prev_main_future_price)
                            OR (future_price < prev_future_price AND main_future_price < prev_main_future_price)
                        )
                        THEN TRUE ELSE FALSE 
                    END AS is_diff_sync,
                    NULL AS is_otm,
                    NULL AS sync_status
                FROM future_with_lag
                WHERE prev_future_price IS NOT NULL AND prev_main_future_price IS NOT NULL
            """)
            
            conn.execute("""
                UPDATE ticks_raw
                SET 
                    is_otm = calc.is_otm,
                    sync_status = calc.sync_status,
                    future_sync_status = calc.future_sync_status,
                    is_same_rise = calc.is_same_rise,
                    is_same_fall = calc.is_same_fall,
                    is_diff_sync = calc.is_diff_sync
                FROM future_status_calc calc
                WHERE ticks_raw.rowid = calc.row_id
            """)
            
            logger.info("Future status columns updated.")
            logger.info("All status columns updated successfully.")
            
        except Exception as e:
            logger.warning(f"Failed to update status columns: {e}")
