# MODULE_ID: M1-143
"""Daily key pivot pre-computation — adaptive ZigZag with ex-post confirmation.

Algorithm: single-pass O(N) state machine scanning daily close prices.
Parameter: min_reversal = volatility / sqrt(252) * multiplier (data-driven).

Critical design decisions vs original V1.0 proposal:
  - Uses db_adapter (not raw duckdb) for connection management
  - pivot_config table merged into DailyPivotParams dataclass
  - pivot_compute_log removed (engine progress tracking suffices)
  - Integrated into PrecomputeEngine pipeline
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from infra._helpers import get_logger

logger = get_logger(__name__)


@dataclass
class PivotRecord:
    symbol_id: str
    pivot_date: date
    pivot_type: str
    pivot_price: float
    confirm_date: date
    confirm_lag: float
    min_reversal: float
    volatility_used: float
    multiplier: float


class AdaptiveZigZagComputer:
    def __init__(self, multiplier: float = 1.5, min_bars: int = 20):
        self.multiplier = multiplier
        self.min_bars = min_bars

    def compute_volatility(self, prices: np.ndarray) -> float:
        if len(prices) < 2:
            return 0.0
        invalid_mask = prices <= 0
        if np.any(invalid_mask):
            logger.warning("compute_volatility: %d/%d prices <= 0, excluded from calculation", np.sum(invalid_mask), len(prices))
        valid_prices = prices[prices > 0]
        if len(valid_prices) < 2:
            return 0.0
        log_returns = np.diff(np.log(valid_prices))
        log_returns = np.nan_to_num(log_returns, nan=0.0)
        if len(log_returns) < 2:
            return 0.0
        daily_vol = np.std(log_returns, ddof=1)
        return daily_vol * np.sqrt(252)

    def compute_min_reversal(self, prices: np.ndarray) -> Tuple[float, float]:
        volatility = self.compute_volatility(prices)
        min_reversal = max(
            volatility / np.sqrt(252) * self.multiplier,
            0.001,
        )
        return min_reversal, volatility

    def scan(
        self, prices: np.ndarray, dates: List[date], symbol_id: str
    ) -> List[PivotRecord]:
        n = len(prices)
        if n < self.min_bars:
            logger.warning(
                "%s: bar count %d < min_bars %d", symbol_id, n, self.min_bars
            )
            return []

        min_reversal, volatility = self.compute_min_reversal(prices)
        logger.info(
            "%s: volatility=%.4f, min_reversal=%.4f",
            symbol_id,
            volatility,
            min_reversal,
        )

        state = "START"
        last_extreme_idx = 0
        last_extreme_price = prices[0]
        pivots: List[PivotRecord] = []

        for i in range(1, n):
            current_price = prices[i]
            current_date = dates[i]

            if state == "START":
                if current_price >= last_extreme_price * (1 + min_reversal):
                    state = "RISING"
                    last_extreme_idx = i
                    last_extreme_price = current_price
                elif current_price <= last_extreme_price * (1 - min_reversal):
                    state = "FALLING"
                    last_extreme_idx = i
                    last_extreme_price = current_price

            elif state == "RISING":
                if current_price > last_extreme_price:
                    last_extreme_idx = i
                    last_extreme_price = current_price
                elif current_price <= last_extreme_price * (1 - min_reversal):
                    delta_td = current_date - dates[last_extreme_idx]
                    delta_days = delta_td.days + delta_td.seconds / 86400.0
                    confirm_lag = max(1.0, delta_days)
                    pivots.append(
                        PivotRecord(
                            symbol_id=symbol_id,
                            pivot_date=dates[last_extreme_idx],
                            pivot_type="HIGH",
                            pivot_price=float(last_extreme_price),
                            confirm_date=current_date,
                            confirm_lag=confirm_lag,
                            min_reversal=min_reversal,
                            volatility_used=volatility,
                            multiplier=self.multiplier,
                        )
                    )
                    state = "FALLING"
                    last_extreme_idx = i
                    last_extreme_price = current_price

            elif state == "FALLING":
                if current_price < last_extreme_price:
                    last_extreme_idx = i
                    last_extreme_price = current_price
                elif current_price >= last_extreme_price * (1 + min_reversal):
                    delta_td = current_date - dates[last_extreme_idx]
                    delta_days = delta_td.days + delta_td.seconds / 86400.0
                    confirm_lag = max(1.0, delta_days)
                    pivots.append(
                        PivotRecord(
                            symbol_id=symbol_id,
                            pivot_date=dates[last_extreme_idx],
                            pivot_type="LOW",
                            pivot_price=float(last_extreme_price),
                            confirm_date=current_date,
                            confirm_lag=confirm_lag,
                            min_reversal=min_reversal,
                            volatility_used=volatility,
                            multiplier=self.multiplier,
                        )
                    )
                    state = "RISING"
                    last_extreme_idx = i
                    last_extreme_price = current_price

        if state in ("RISING", "FALLING") and last_extreme_idx < n - 1:
            pivots.append(
                PivotRecord(
                    symbol_id=symbol_id,
                    pivot_date=dates[last_extreme_idx],
                    pivot_type="HIGH" if state == "RISING" else "LOW",
                    pivot_price=float(last_extreme_price),
                    confirm_date=dates[-1],
                    confirm_lag=0.0,
                    min_reversal=min_reversal,
                    volatility_used=volatility,
                    multiplier=self.multiplier,
                )
            )

        return pivots


def compute_daily_pivots_for_symbol(
    con,
    symbol: str,
    multiplier: float = 1.5,
    min_bars: int = 20,
    as_of_date: Optional[date] = None,
    compute_version: str = "adaptive_zigzag_v1.0",
    source_table: str = "mv_daily_bars",
    source_columns: str = "close",
) -> int:
    computer = AdaptiveZigZagComputer(multiplier=multiplier, min_bars=min_bars)

    try:
        source_cols = con.execute(f"DESCRIBE {source_table}").fetchall()
        source_col_names = {row[0] for row in source_cols}
        required_cols = {"trade_date", source_columns}
        if not required_cols.issubset(source_col_names):
            missing = required_cols - source_col_names
            raise ValueError(f"{source_table} missing columns: {missing}")
        
        df = con.execute(
            f"SELECT trade_date, {source_columns} FROM {source_table} WHERE symbol = ? ORDER BY trade_date",
            [symbol],
        ).fetchdf()
        source_table_used = source_table
    except (OSError, ValueError, KeyError, RuntimeError) as _e:
        logger.warning("%s not available for %s, fallback to daily_bars: %s", source_table, symbol, _e)
        df = con.execute(
            "SELECT trade_date, close FROM daily_bars WHERE symbol = ? ORDER BY trade_date",
            [symbol],
        ).fetchdf()
        source_table_used = "daily_bars"

    if df.empty or len(df) < min_bars:
        return 0

    prices = df["close"].to_numpy(dtype=np.float64)
    dates = pd.to_datetime(df["trade_date"]).dt.date.tolist()

    pivots = computer.scan(prices, dates, symbol)
    if not pivots:
        return 0

    if as_of_date is None:
        as_of_date = dates[-1]

    records = []
    for p in pivots:
        records.append(
            (
                p.symbol_id,
                p.pivot_date,
                p.pivot_type,
                p.pivot_price,
                p.confirm_date,
                p.confirm_lag,
                p.min_reversal,
                p.volatility_used,
                p.multiplier,
                source_table_used,
                source_columns,
                compute_version,
                datetime.now(),
                as_of_date,
            )
        )

    con.executemany(
        """INSERT OR REPLACE INTO daily_key_pivots
           (symbol_id, pivot_date, pivot_type, pivot_price, confirm_date, confirm_lag,
            min_reversal, volatility_used, multiplier, source_table, source_columns,
            compute_version, computed_at, as_of_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    return len(pivots)


def ensure_daily_key_pivots_table(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_key_pivots (
            symbol_id       VARCHAR(20)  NOT NULL,
            pivot_date      DATE         NOT NULL,
            pivot_type      VARCHAR(4)   NOT NULL CHECK (pivot_type IN ('HIGH', 'LOW')),
            pivot_price     DOUBLE       NOT NULL,
            confirm_date    DATE         NOT NULL,
            confirm_lag     DOUBLE       NOT NULL,
            min_reversal    DOUBLE       NOT NULL,
            volatility_used DOUBLE       NOT NULL,
            multiplier      DOUBLE       NOT NULL DEFAULT 1.5,
            source_table    VARCHAR(50)  NOT NULL DEFAULT 'mv_daily_bars',
            source_columns  VARCHAR(100) NOT NULL DEFAULT 'close',
            compute_version VARCHAR(20)  NOT NULL DEFAULT 'adaptive_zigzag_v1.0',
            computed_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            as_of_date      DATE         NOT NULL,
            PRIMARY KEY (symbol_id, pivot_date, as_of_date)
        )
    """)
    try:
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_key_pivots_type ON daily_key_pivots(symbol_id, pivot_type, pivot_date)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_key_pivots_confirm ON daily_key_pivots(symbol_id, confirm_date)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_key_pivots_asof ON daily_key_pivots(as_of_date, symbol_id)"
        )
    except (OSError, ValueError, KeyError, RuntimeError):
        pass