"""
data_validator.py - DataValidator
Phase 2 (CC-P1-03): 从preprocess_ticks.py提取的数据验证/质量门职责域

职责：
- 熔断停牌检测 (validate_circuit_breaker_halts)
- Tick乱序鲁棒性验证 (validate_out_of_order_ticks)
- 期权到期日完整性检查 (validate_expire_date_integrity)
- 数据类定义 (CircuitBreakerResult, OutOfOrderResult, ExpireDateResult)
"""
from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional, NamedTuple

logger = logging.getLogger(__name__)


class _TypedResult(NamedTuple):
    pass


class CircuitBreakerResult(_TypedResult):
    halt_events: List[Dict]
    n_halts: int
    survival_rate_check_needed: bool


class OutOfOrderResult(_TypedResult):
    is_monotonic: bool
    rollback_count: int
    simulated_swap_count: int
    swap_prob: float
    needs_dedup_module: bool
    recommendation: str
    swapped_df: Optional[pd.DataFrame]


class ExpireDateResult(_TypedResult):
    passed: bool
    total_rows: int
    missing_expire_count: int
    missing_expire_pct: float
    issues: List[str]
    action: str


def validate_circuit_breaker_halts(bar_data: pd.DataFrame,
                                    price_col: str = "close",
                                    ref_change_pct: float = 0.50,
                                    halt_duration_bars: int = 5,
                                    resume_slippage_pct: float = 0.50) -> CircuitBreakerResult:
    if bar_data.empty or price_col not in bar_data.columns:
        return CircuitBreakerResult(halt_events=[], n_halts=0, survival_rate_check_needed=False)

    halt_events = []
    prices = bar_data[price_col].values

    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            change_pct = abs(prices[i] - prices[i - 1]) / prices[i - 1]
            if change_pct >= ref_change_pct:
                gap_pct = (prices[i] - prices[i - 1]) / prices[i - 1]
                halt_events.append({
                    "bar_index": i,
                    "gap_pct": round(gap_pct * 100, 2),
                    "halt_duration_bars": halt_duration_bars,
                    "resume_slippage_bps": round(abs(gap_pct) * resume_slippage_pct * 10000, 1),
                    "direction": "up" if gap_pct > 0 else "down",
                })

    return CircuitBreakerResult(
        halt_events=halt_events,
        n_halts=len(halt_events),
        survival_rate_check_needed=len(halt_events) > 0,
    )


def validate_out_of_order_ticks(tick_df: pd.DataFrame,
                                 swap_prob: float = 0.001,
                                 datetime_col: str = "datetime") -> OutOfOrderResult:
    if tick_df.empty or datetime_col not in tick_df.columns:
        return OutOfOrderResult(is_monotonic=True, rollback_count=0, simulated_swap_count=0,
                                swap_prob=swap_prob, needs_dedup_module=False,
                                recommendation="数据时序正常", swapped_df=None)

    n = len(tick_df)
    rng = np.random.RandomState(42)
    swap_indices = rng.random(n - 1) < swap_prob
    swap_count = int(swap_indices.sum())

    dt_series = pd.to_datetime(tick_df[datetime_col])
    is_monotonic = dt_series.is_monotonic_increasing

    time_diffs = dt_series.diff().dt.total_seconds()
    rollback_count = int((time_diffs < 0).sum())

    needs_dedup = rollback_count > 0 or swap_count > n * 0.01

    swapped_df = tick_df.copy()
    dt_values = swapped_df[datetime_col].values.copy()
    swap_positions = np.where(swap_indices)[0]
    for idx in swap_positions:
        if idx + 1 < len(dt_values):
            dt_values[idx], dt_values[idx + 1] = dt_values[idx + 1], dt_values[idx]
    swapped_df[datetime_col] = dt_values
    swapped_df = swapped_df.sort_values(datetime_col).reset_index(drop=True)

    return OutOfOrderResult(
        is_monotonic=is_monotonic,
        rollback_count=rollback_count,
        simulated_swap_count=swap_count,
        swap_prob=swap_prob,
        needs_dedup_module=needs_dedup,
        recommendation="增加tick缓存+去重+排序模块" if needs_dedup else "数据时序正常",
        swapped_df=swapped_df,
    )


def validate_expire_date_integrity(bar_data: pd.DataFrame = None,
                                    symbol_col: str = "symbol",
                                    expire_col: str = "expire_date",
                                    strike_col: str = "strike_price") -> ExpireDateResult:
    if bar_data is None or bar_data.empty:
        return ExpireDateResult(passed=True, total_rows=0, missing_expire_count=0,
                                missing_expire_pct=0.0, issues=[], action="no_data")

    if expire_col not in bar_data.columns:
        return ExpireDateResult(passed=True, total_rows=len(bar_data), missing_expire_count=0,
                                missing_expire_pct=0.0, issues=[], action="no_expire_date_column")

    issues = []
    total_rows = len(bar_data)

    missing_expire = bar_data[expire_col].isna().sum()
    missing_pct = missing_expire / total_rows * 100 if total_rows > 0 else 0
    if missing_pct > 5.0:
        issues.append(f"expire_date缺失率{missing_pct:.1f}%超过5%阈值")

    if symbol_col in bar_data.columns:
        symbol_expire_counts = (
            bar_data.dropna(subset=[expire_col])
            .groupby(symbol_col)[expire_col]
            .nunique()
        )
        multi_expire_symbols = symbol_expire_counts[symbol_expire_counts > 1]
        if len(multi_expire_symbols) > 0:
            issues.append(
                f"发现{len(multi_expire_symbols)}个symbol有多个到期日"
                f"（可能混合周度/月度期权）: "
                f"{list(multi_expire_symbols.index[:5])}"
            )

    if strike_col in bar_data.columns:
        has_strike_no_expire = (
            bar_data[strike_col].notna() & bar_data[expire_col].isna()
        ).sum()
        if has_strike_no_expire > 0:
            issues.append(
                f"发现{has_strike_no_expire}条记录有strike_price但无expire_date，应剔除"
            )

    passed = len(issues) == 0
    _action = "remove_invalid_contracts" if not passed else "proceed"

    if not passed:
        logger.warning("[P1-裂缝34] 期权到期日完整性检查失败: %s", issues)

    return ExpireDateResult(
        passed=passed,
        total_rows=total_rows,
        missing_expire_count=int(missing_expire),
        missing_expire_pct=round(missing_pct, 2),
        issues=issues,
        action=_action,
    )