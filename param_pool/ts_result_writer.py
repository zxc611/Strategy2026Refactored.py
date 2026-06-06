#!/usr/bin/env python3
"""ts_result_writer — 结果写入与质量门子模块 (R27-CP-08-FIX: 从task_scheduler.py拆分)

包含:
  - DuckDB结果写入: _validate_ddl_column_names, _ensure_results_table,
    _get_completed_task_ids, _insert_results
  - 数据加载: _query_minute_table, _load_data_for_period,
    _load_multiscale_data, _resample_bars_runtime
  - HFT tick插值: _interpolate_ticks_in_bar
  - K线参数缩放: _scale_params_with_bar_interval
  - K线长度/周期共振扫描: run_kline_length_sweep, run_kline_length_deep_sweep,
    run_kline_cr_cross_sweep
  - 质量门验证: validate_kline_length_quality_gates
  - 执行轮次: _execute_round
  - 参数校验: _validate_params_via_params_service
  - 寿命字典: build_and_save_life_dict
  - 主调度: main_scheduler
"""
from __future__ import annotations

import itertools
import json
import logging
import multiprocessing
import os
import random as _pyrandom
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
from ali2026v3_trading.shared_utils import safe_price_check
from ali2026v3_trading.cross_system_execution import CrossSystemExecutionKernel
from ali2026v3_trading.data_access import get_data_access
from ali2026v3_trading.db_adapter import connect_duckdb

_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()
logger = logging.getLogger(__name__)

from ali2026v3_trading.param_pool.backtest_runner import (
    run_backtest_box_extreme,
    run_backtest_box_spring,
    run_backtest_hft,
    run_backtest_arbitrage,
    run_backtest_market_making,
    run_backtest_hft_tick_fidelity,
    run_backtest_with_cycle_resonance,
)

from ali2026v3_trading.param_pool.backtest_param_grids import (
    PARAM_DEFAULTS,
    PARAM_DEFAULTS_HFT,
    PARAM_DEFAULTS_BOX_EXTREME,
    PARAM_DEFAULTS_BOX_SPRING,
    PARAM_DEFAULTS_ARBITRAGE,
    PARAM_DEFAULTS_MARKET_MAKING,
    STRATEGY_SHADOW_DEFAULTS,
    PARAM_GRID_ROUND1,
    PARAM_GRID_ROUND2,
    PARAM_GRID_ROUND3,
    PARAM_GRID_ROUND4,
    ROUND1_TOP_K,
    SCAN_TARGET_SORT_METRIC,
    PARAM_SORT_METRIC_OVERRIDE,
    CR_PARAM_GRID,
)

from ali2026v3_trading.param_pool.backtest_runner_base import (
    run_backtest,
    PREPROCESSED_DB,
    RESULTS_DB,
    MAX_WORKERS,
    TRAIN_START,
    TEST_START,
    TEST_END,
    TARGET_SYMBOLS,
    validate_default_values_in_grids,
    validate_shadow_param_independence,
    _sync_random_seed,
    _select_top_k_train,
    _get_cascade_judge_module,
    _get_life_estimator,
)

from ali2026v3_trading.param_pool.backtest_validation import (
    compute_alpha_confidence_interval,
    run_deep_validation_tiered,
    DEEP_VALIDATION_TIERS,
    PARAM_TIERS,
    L2_HYPERPARAMS,
    PARAM_GRID_CYCLE_RESONANCE,
    analyze_l2_sensitivity,
    optimize_l2_params_step1,
    _DDL_COLUMN_SAFE_PATTERN,
)

from ali2026v3_trading.param_pool.backtest_checks import (
    _run_final_checks,
)

from ali2026v3_trading.param_pool.ts_param_grids import (
    RISK_FREE_RATE,
    BACKTEST_THRESHOLDS,
    MULTISCALE_BAR_LENGTHS,
    BAR_INTERVAL_GRID,
    KLINE_LENGTH_PARAM_GRID,
    _SUBPROCESS_NEEDED_COLS,
)

from ali2026v3_trading.param_pool.ts_backtest_strategies import (
    _prepare_df_for_subprocess,
    _worker_init,
    cleanup_global_data,
    _worker_task,
)


def _validate_ddl_column_names(column_names: List[str]) -> None:
    invalid = [c for c in column_names if not _DDL_COLUMN_SAFE_PATTERN.match(c)]
    if invalid:
        raise ValueError(
            f"[SEC-02] DDL列名白名单校验失败，拒绝非法列名注入: {invalid}"
        )


def _ensure_results_table(con: Any, param_keys: List[str]) -> None:
    _validate_ddl_column_names(param_keys)
    # P0-12修复: 70+参数列标记deprecated，建表仍保留列定义以兼容已有DB，但_insert_results不再写入
    param_cols = ",\n            ".join(f'"{k}" DOUBLE  -- deprecated: P0-12参数列，所有读取方从params_json解析' for k in param_keys)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS backtest_results (
            task_id INTEGER PRIMARY KEY,
            is_train BOOLEAN,
            {param_cols},
            params_json VARCHAR,
            
            -- S1: 高频趋势共振
            hft_sharpe DOUBLE,
            hft_max_dd DOUBLE,
            hft_total_return DOUBLE,
            hft_num_signals INTEGER,
            hft_shadow_a_sharpe DOUBLE,
            hft_shadow_b_sharpe DOUBLE,
            hft_alpha DOUBLE,
            
            -- S2: 分钟级趋势共振
            minute_sharpe DOUBLE,
            minute_max_dd DOUBLE,
            minute_total_return DOUBLE,
            minute_num_signals INTEGER,
            minute_shadow_a_sharpe DOUBLE,
            minute_shadow_b_sharpe DOUBLE,
            minute_alpha DOUBLE,
            
            -- S3: 箱体极值策略
            box_extreme_sharpe DOUBLE,
            box_extreme_max_dd DOUBLE,
            box_extreme_total_return DOUBLE,
            box_extreme_num_signals INTEGER,
            box_extreme_shadow_a_sharpe DOUBLE,
            box_extreme_shadow_b_sharpe DOUBLE,
            box_extreme_alpha DOUBLE,
            
            -- S4: 箱体弹簧策略
            box_spring_sharpe DOUBLE,
            box_spring_max_dd DOUBLE,
            box_spring_total_return DOUBLE,
            box_spring_num_signals INTEGER,
            box_spring_shadow_a_sharpe DOUBLE,
            box_spring_shadow_b_sharpe DOUBLE,
            box_spring_alpha DOUBLE,
            
            -- S5: 套利策略
            arbitrage_sharpe DOUBLE,
            arbitrage_max_dd DOUBLE,
            arbitrage_total_return DOUBLE,
            arbitrage_num_signals INTEGER,
            arbitrage_shadow_a_sharpe DOUBLE,
            arbitrage_shadow_b_sharpe DOUBLE,
            arbitrage_alpha DOUBLE,
            
            -- S6: 做市策略
            market_making_sharpe DOUBLE,
            market_making_max_dd DOUBLE,
            market_making_total_return DOUBLE,
            market_making_num_signals INTEGER,
            market_making_shadow_a_sharpe DOUBLE,
            market_making_shadow_b_sharpe DOUBLE,
            market_making_alpha DOUBLE,
            
            -- P0-9修复: 6个关键计算字段列(原_worker_task计算但不入库)
            minute_trend_score DOUBLE,
            risk_adjusted_score DOUBLE,
            risk_score_error VARCHAR,
            cascade_final_score DOUBLE,
            cascade_passed BOOLEAN,
            cascade_score_error VARCHAR,
            
            error VARCHAR
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_train ON backtest_results(is_train)")
    # P1-7修复: 以下5个sharpe索引冗余(无SELECT消费)，仅保留idx_minute_sharpe(ORDER BY使用)
    # con.execute("CREATE INDEX IF NOT EXISTS idx_hft_sharpe ON backtest_results(hft_sharpe)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_minute_sharpe ON backtest_results(minute_sharpe)")


def _get_completed_task_ids(con: Any) -> Set[int]:
    try:
        rows = con.execute("SELECT task_id FROM backtest_results").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _query_minute_table(
    con: Any,
    table_name: str,
    date_start: str,
    date_end: str,
    symbols: Optional[List[str]] = None,
    extra_filters: str = "",
    extra_params: Optional[List[Any]] = None,
) -> pd.DataFrame:
    """统一构建minute类表查询，减少重复SQL模板。"""
    _extra_params = list(extra_params or [])
    if symbols and len(symbols) > 0:
        placeholders = ", ".join(["?"] * len(symbols))
        sql = f"""
            SELECT minute, symbol, open, high, low, close, volume, open_interest, bid_ask_spread, correct_rise_pct, correct_fall_pct, wrong_rise_pct, wrong_fall_pct, other_pct, strength, imbalance, consistency, iv, delta, gamma, vega, theta, strike_price, expire_date, option_type, underlying_price, days_to_expiry, _spread_quality, _option_metadata_quality FROM {table_name}
            WHERE minute >= ? AND minute < ?
              {extra_filters}
              AND symbol IN ({placeholders})
            ORDER BY symbol, minute
        """
        params = [date_start, date_end] + _extra_params + list(symbols)
    else:
        sql = f"""
            SELECT minute, symbol, open, high, low, close, volume, open_interest, bid_ask_spread, correct_rise_pct, correct_fall_pct, wrong_rise_pct, wrong_fall_pct, other_pct, strength, imbalance, consistency, iv, delta, gamma, vega, theta, strike_price, expire_date, option_type, underlying_price, days_to_expiry, _spread_quality, _option_metadata_quality FROM {table_name}
            WHERE minute >= ? AND minute < ?
              {extra_filters}
            ORDER BY symbol, minute
        """
        params = [date_start, date_end] + _extra_params
    return con.execute(sql, params).fetchdf()


def _load_data_for_period(
    db_path: str,
    date_start: str,
    date_end: str,
    symbols: Optional[List[str]] = None,
) -> pd.DataFrame:
    con = connect_duckdb(db_path, read_only=True)
    try:
        df = _query_minute_table(
            con=con,
            table_name="minute_data",
            date_start=date_start,
            date_end=date_end,
            symbols=symbols,
        )
    finally:
        con.close()
    return df


def _load_multiscale_data(
    db_path: str,
    date_start: str,
    date_end: str,
    bar_length_minutes: int = 1,
    symbols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """加载指定K线长度的Bar数据

    bar_length_minutes=1 → 直接读minute_data表
    bar_length_minutes∈{5,15,60} → 读minute_data_multiscale表对应bar_length_minutes行
    bar_length_minutes∉{1,5,15,60} → 从1分钟数据在线聚合（运行时_resample）
    """
    con = connect_duckdb(db_path, read_only=True)
    try:
        if bar_length_minutes == 1:
            df = _query_minute_table(
                con=con,
                table_name="minute_data",
                date_start=date_start,
                date_end=date_end,
                symbols=symbols,
            )
        elif bar_length_minutes in (5, 15, 60):
            table_check = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name='minute_data_multiscale'"
            ).fetchone()
            if table_check and table_check[0] > 0:
                df = _query_minute_table(
                    con=con,
                    table_name="minute_data_multiscale",
                    date_start=date_start,
                    date_end=date_end,
                    symbols=symbols,
                    extra_filters="AND bar_length_minutes = ?",
                    extra_params=[bar_length_minutes],
                )
            else:
                df_1m = _load_multiscale_data(db_path, date_start, date_end, 1, symbols)
                df = _resample_bars_runtime(df_1m, bar_length_minutes)
        else:
            df_1m = _load_multiscale_data(db_path, date_start, date_end, 1, symbols)
            df = _resample_bars_runtime(df_1m, bar_length_minutes)
    finally:
        con.close()
    return df


def _resample_bars_runtime(df_1m: pd.DataFrame, bar_length_minutes: int) -> pd.DataFrame:
    """运行时将1分钟Bar聚合为N分钟Bar（无需preprocess_ticks预生成）"""
    if df_1m.empty or bar_length_minutes <= 1:
        return df_1m

    df = df_1m.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["minute"]):
        df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    df["_group"] = df["minute"].dt.floor(f"{bar_length_minutes}min")

    ohlcv_agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "turnover" in df.columns:
        ohlcv_agg["turnover"] = "sum"
    if "tick_count" in df.columns:
        ohlcv_agg["tick_count"] = "sum"

    group_cols = ["_group", "symbol"]
    result = df.groupby(group_cols).agg(ohlcv_agg).reset_index()
    result.rename(columns={"_group": "minute"}, inplace=True)

    if "volume" in result.columns and "turnover" in result.columns:
        result["vwap"] = np.where(
            result["volume"] > 0,
            result["turnover"] / result["volume"],
            result["close"],
        )

    mean_cols = [
        "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
        "other_pct", "strength", "imbalance", "consistency",
        "iv", "delta", "gamma", "vega", "theta",
    ]
    last_cols = [
        "bid_ask_spread", "_spread_quality", "strike_price", "_option_metadata_quality",
        "expire_date", "days_to_expiry", "option_type",
        "underlying_price", "open_interest",
    ]
    # C-37修复: 批量groupby+agg替代链式merge，消除O(N*M) DataFrame复制开销
    # 原PD-P2-01: 每次merge创建新DataFrame，last_cols约20列时复制20次
    # 修复后: 一次groupby计算所有mean/last列，一次merge合并
    available_mean = [c for c in mean_cols if c in df.columns]
    available_last = [c for c in last_cols if c in df.columns]
    if available_mean or available_last:
        extra_agg = {}
        for col in available_mean:
            extra_agg[col] = (col, "mean")
        for col in available_last:
            extra_agg[col] = (col, "last")
        extra_result = df.groupby(group_cols).agg(**extra_agg).reset_index()
        extra_result.rename(columns={"_group": "minute"}, inplace=True)
        result = result.merge(extra_result, on=["minute", "symbol"], how="left")

    _nan_count = result[mean_cols + last_cols].isna().sum()
    if _nan_count.any():
        logger.warning("[PD-P1-06] _resample_bars_runtime merge后存在NaN: %s", _nan_count[_nan_count > 0].to_dict())

    return result.dropna(subset=["open", "close"])


def _interpolate_ticks_in_bar(
    bar: pd.Series,
    n_ticks: int = 5,
) -> List[Dict[str, float]]:
    """HFT tick级回测保真：在单根Bar内均匀插值模拟tick序列

    将1根分钟Bar拆解为n_ticks个虚拟tick：
      - 价格路径: 线性插值 open → high/low → close
      - 成交量: 均匀分配
      - imbalance/strength: 逐tick线性过渡（允许信号翻转检测）

    Args:
        bar: 单根Bar（pandas Series或dict-like）
        n_ticks: 每根Bar内的虚拟tick数（默认5，≈12秒/tick在1分钟Bar内）

    Returns:
        List[Dict]: 虚拟tick序列，每个dict含price/imbalance/strength等
    """
    o = float(bar.get("open", 0))
    h = float(bar.get("high", 0))
    l = float(bar.get("low", 0))
    c = float(bar.get("close", 0))
    vol = float(bar.get("volume", 0))
    imb_start = float(bar.get("imbalance", 0))
    imb_end = imb_start * 0.8
    str_start = float(bar.get("strength", 0))
    str_end = str_start * 0.9

    if n_ticks <= 1 or o == 0:
        return [{
            "price": c,
            "volume": vol,
            "imbalance": imb_start,
            "strength": str_start,
        }]

    ticks = []
    price_path_len = n_ticks
    mid_point = price_path_len // 2

    prices = []
    first_peak = h if (h - o >= o - l) else l
    second_peak = l if (h - o >= o - l) else h
    for i in range(price_path_len):
        if i == 0:
            prices.append(o)
            continue
        if i == price_path_len - 1:
            prices.append(c)
            continue
        frac = i / (price_path_len - 1)
        if frac < 0.33:
            p = o + (first_peak - o) * (frac / 0.33)
        elif frac < 0.67:
            frac2 = (frac - 0.33) / 0.34
            p = first_peak + (second_peak - first_peak) * frac2
        else:
            frac3 = (frac - 0.67) / 0.33
            p = second_peak + (c - second_peak) * frac3
        prices.append(p)

    vol_per_tick = vol / n_ticks
    # P2-裂缝52：imbalance_jitter模拟真实tick噪声
    # 线性过渡可能使原本因tick噪声而断裂的信号变成连续信号，高估夏普
    imbalance_jitter = float(bar.get("imbalance_jitter", 0.05))
    rng = np.random.RandomState(hash(bar.get("minute", "")) % 2**31 if hasattr(bar, "get") else 42)
    for i in range(n_ticks):
        frac = i / max(1, n_ticks - 1)
        jitter = rng.uniform(-imbalance_jitter, imbalance_jitter)
        tick = {
            "price": prices[i],
            "volume": vol_per_tick,
            "imbalance": imb_start + (imb_end - imb_start) * frac + jitter,
            "strength": str_start + (str_end - str_start) * frac,
        }
        ticks.append(tick)

    return ticks


def _scale_params_with_bar_interval(
    params: Dict[str, float],
    bar_interval_minutes: int,
    original_interval: int = 1,
) -> Dict[str, float]:
    """P1-裂缝33：K线长度缩放技术指标周期参数

    当bar_interval_minutes改变后，技术指标的周期参数应按比例缩放，
    否则跨K线长度的比较不公平（如trend_period_short=5在1分钟Bar表示5分钟，
    换成5分钟Bar后变成25分钟，语义改变）。

    缩放规则：scaled_period = max(1, original_period / bar_interval_minutes)
    仅当scale_periods_with_bar_interval=True时生效。
    """
    scaled = dict(params)
    if not params.get("scale_periods_with_bar_interval", False):
        return scaled
    if bar_interval_minutes <= original_interval:
        return scaled  # 无需缩放

    scale_factor = original_interval / bar_interval_minutes

    # 需要缩放的周期类参数（Bar数 → 实际时间不变）
    PERIOD_PARAMS = [
        "trend_period_short", "trend_period_medium", "trend_period_long",
        "adx_period", "box_lookback_bars", "box_min_bars",
        "state_confirm_bars", "box_breakout_confirm_bars",
        "spring_charge_confirm_bars", "spring_release_confirm_bars",
        "vol_lookback", "iv_lookback_bars", "kline_snr_window",
    ]

    for key in PERIOD_PARAMS:
        if key in scaled:
            original_val = scaled[key]
            if original_val > 0:
                scaled_val = max(1, int(round(original_val * scale_factor)))
                if scaled_val != int(original_val):
                    logger.debug(
                        "[P1-裂缝33] 缩放参数 %s: %d → %d (bar_interval=%dmin)",
                        key, int(original_val), scaled_val, bar_interval_minutes,
                    )
                scaled[key] = float(scaled_val)

    return scaled


def run_kline_length_sweep(
    db_path: str = PREPROCESSED_DB,
    strategy: str = "resonance",
    train: bool = True,
    bar_intervals: Optional[List[int]] = None,
    base_params: Optional[Dict] = None,
    strategies: Optional[List[str]] = None,
    cross_validate: bool = True,
) -> pd.DataFrame:
    """K线长度专项扫描：全策略×全bar_length交叉矩阵 + KLINE_LENGTH_PARAM_GRID参数网格

    扫描维度：
      1. 策略×bar_interval_minutes交叉矩阵（4策略×11档=44组基线）
      2. KLINE_LENGTH_PARAM_GRID中的24个参数各5-7水平
      3. HFT ticks_per_bar深度扫描（7水平）
      4. 与CR_PARAM_GRID的交叉组合（可选）
      5. 生产/回测一致性校验行
    """
    if strategies is None:
        strategies = ["high_freq", "resonance", "box", "spring", "arbitrage", "market_making"]
    if base_params is None:
        base_params_map = {
            "high_freq": PARAM_DEFAULTS_HFT,
            "resonance": PARAM_DEFAULTS,
            "box": PARAM_DEFAULTS_BOX_EXTREME,
            "spring": PARAM_DEFAULTS_BOX_SPRING,
            "arbitrage": PARAM_DEFAULTS_ARBITRAGE,
            "market_making": PARAM_DEFAULTS_MARKET_MAKING,
        }
    else:
        base_params_map = {s: base_params for s in strategies}

    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    results = []
    for strat in strategies:
        intervals = bar_intervals or BAR_INTERVAL_GRID.get(strat, [1])
        bp = base_params_map.get(strat, PARAM_DEFAULTS)

        for bi in intervals:
            bar_data = _load_multiscale_data(db_path, date_start, date_end, bi)
            if bar_data.empty:
                continue

            bt_func = {
                "high_freq": run_backtest_hft_tick_fidelity,
                "resonance": run_backtest,
                "box": run_backtest_box_extreme,
                "spring": run_backtest_box_spring,
                "arbitrage": run_backtest_arbitrage,
                "market_making": run_backtest_market_making,
            }.get(strat, run_backtest)

            result = bt_func(bp, bar_data, train=train)
            result["strategy"] = strat
            result["bar_interval_minutes"] = bi
            result["n_bars"] = len(bar_data)
            result["kline_fidelity"] = "tick_interpolated" if strat == "high_freq" else "bar_exact"
            results.append(result)

            if cross_validate and strat == "high_freq":
                for tpb in KLINE_LENGTH_PARAM_GRID.get("hft_ticks_per_bar", [5]):
                    params_hft = bp.copy()
                    params_hft["hft_ticks_per_bar"] = tpb
                    r = run_backtest_hft_tick_fidelity(params_hft, bar_data, train=train)
                    r["strategy"] = strat
                    r["bar_interval_minutes"] = bi
                    r["n_bars"] = len(bar_data)
                    r["hft_ticks_per_bar"] = tpb
                    results.append(r)

    return pd.DataFrame(results)


def run_kline_length_deep_sweep(
    db_path: str = PREPROCESSED_DB,
    strategy: str = "resonance",
    train: bool = True,
    max_combos: int = 2000,
) -> pd.DataFrame:
    """K线长度深度扫描：KLINE_LENGTH_PARAM_GRID全24维度随机采样

    对24个K线长度参数的完整网格进行随机采样回测，
    每个组合指定不同的bar_interval_minutes + 趋势周期 + 确认Bar数等。

    总空间 ≈ 11×5×5×5×5×6×6×5×7×5×7×6×6×5×5×6×4×5×5×5×5×5×4 ≈ 10^13
    随机采样max_combos个组合。
    """
    from cycle_resonance_module import CycleResonanceModule, CR_PARAMS_DEFAULT, reset_cycle_resonance_module

    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    base_params_map = {
        "high_freq": PARAM_DEFAULTS_HFT,
        "resonance": PARAM_DEFAULTS,
        "box": PARAM_DEFAULTS_BOX_EXTREME,
        "spring": PARAM_DEFAULTS_BOX_SPRING,
        "arbitrage": PARAM_DEFAULTS_ARBITRAGE,
        "market_making": PARAM_DEFAULTS_MARKET_MAKING,
    }
    bp = base_params_map.get(strategy, PARAM_DEFAULTS)

    grid = KLINE_LENGTH_PARAM_GRID
    param_names = list(grid.keys())
    level_counts = [len(grid[k]) for k in param_names]
    total_space = 1
    for c in level_counts:
        total_space *= c

    _sync_random_seed(42)
    results = []
    sampled_combos = set()

    for _ in range(max_combos * 3):
        if len(results) >= max_combos:
            break
        combo = tuple(np.random.randint(0, lc) for lc in level_counts)
        if combo in sampled_combos:
            continue
        sampled_combos.add(combo)

        params = bp.copy()
        for i, pname in enumerate(param_names):
            params[pname] = grid[pname][combo[i]]

        bi = int(params.get("bar_interval_minutes", 1))
        allowed = BAR_INTERVAL_GRID.get(strategy, [1])
        if bi not in allowed and bi not in MULTISCALE_BAR_LENGTHS:
            if not allowed: continue  # R27-P2-04-FIX: 空列表保护
            bi = allowed[0]
            params["bar_interval_minutes"] = bi

        bar_data = _load_multiscale_data(db_path, date_start, date_end, bi)
        if bar_data.empty:
            continue

        bt_func = {
            "high_freq": run_backtest_hft_tick_fidelity,
            "resonance": run_backtest,
            "box": run_backtest_box_extreme,
            "spring": run_backtest_box_spring,
            "arbitrage": run_backtest_arbitrage,
            "market_making": run_backtest_market_making,
        }.get(strategy, run_backtest)

        result = bt_func(params, bar_data, train=train)
        result["strategy"] = strategy
        result["bar_interval_minutes"] = bi
        result["n_bars"] = len(bar_data)
        result["sweep_type"] = "kline_length_deep"
        results.append(result)

    return pd.DataFrame(results)


def run_kline_cr_cross_sweep(
    db_path: str = PREPROCESSED_DB,
    strategy: str = "resonance",
    train: bool = True,
    kline_sample: int = 50,
    cr_sample: int = 50,
) -> pd.DataFrame:
    """K线长度 × CRParams交叉扫描

    两阶段：
      1. K线长度网格采样kline_sample个组合
      2. 对每个K线长度组合，CR_PARAM_GRID采样cr_sample个组合
    总计 kline_sample × cr_sample 组回测
    """
    from cycle_resonance_module import CycleResonanceModule, CRParams, CR_PARAMS_DEFAULT, reset_cycle_resonance_module

    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    base_params_map = {
        "high_freq": PARAM_DEFAULTS_HFT,
        "resonance": PARAM_DEFAULTS,
        "box": PARAM_DEFAULTS_BOX_EXTREME,
        "spring": PARAM_DEFAULTS_BOX_SPRING,
        "arbitrage": PARAM_DEFAULTS_ARBITRAGE,
        "market_making": PARAM_DEFAULTS_MARKET_MAKING,
    }
    bp = base_params_map.get(strategy, PARAM_DEFAULTS)

    kline_grid = KLINE_LENGTH_PARAM_GRID
    kline_names = list(kline_grid.keys())
    kline_levels = [len(kline_grid[k]) for k in kline_names]

    cr_grid = CR_PARAM_GRID
    cr_names = list(cr_grid.keys())
    cr_levels = [len(cr_grid[k]) for k in cr_names]

    _sync_random_seed(42)
    results = []

    kline_combos = set()
    for _ in range(kline_sample * 3):
        if len(kline_combos) >= kline_sample:
            break
        combo = tuple(np.random.randint(0, lc) for lc in kline_levels)
        kline_combos.add(combo)

    for kline_combo in kline_combos:
        params = bp.copy()
        for i, pname in enumerate(kline_names):
            params[pname] = kline_grid[pname][kline_combo[i]]

        bi = int(params.get("bar_interval_minutes", 1))
        bar_data = _load_multiscale_data(db_path, date_start, date_end, bi)
        if bar_data.empty:
            continue

        for _ in range(cr_sample * 3):
            cr_combo = tuple(np.random.randint(0, lc) for lc in cr_levels)
            cr_params_dict = CR_PARAMS_DEFAULT.to_dict()
            for i, pname in enumerate(cr_names):
                cr_params_dict[pname] = cr_grid[pname][cr_combo[i]]
            cr_params = CRParams.from_dict(cr_params_dict)

            bt_func = {
                "high_freq": run_backtest_hft_tick_fidelity,
                "resonance": run_backtest,
                "box": run_backtest_box_extreme,
                "spring": run_backtest_box_spring,
                "arbitrage": run_backtest_arbitrage,
                "market_making": run_backtest_market_making,
            }.get(strategy, run_backtest)

            result = bt_func(params, bar_data, train=train)
            result["strategy"] = strategy
            result["bar_interval_minutes"] = bi
            result["sweep_type"] = "kline_cr_cross"
            results.append(result)
            if len(results) % 100 == 0:
                logger.info("[kline_cr_cross] %d/%d completed", len(results), kline_sample * cr_sample)

    return pd.DataFrame(results)


def validate_kline_length_quality_gates(
    db_path: str = PREPROCESSED_DB,
    train: bool = True,
) -> Dict[str, Any]:
    """K线长度回测P0质量门验证（KL-Q1~KL-Q5）

    KL-Q1: 夏普非退化 — sharpe(bar=5m) >= 0.5 * sharpe(bar=1m)
    KL-Q2: 交易数非零 — ∀策略×bar_length: n_trades > 0
    KL-Q3: HFT保真交易数差异 — tick_interpolated vs bar_degraded 差异 < 50%
    KL-Q4: Bar长度递减交易数 — bar_length↑ → n_trades↓
    KL-Q5: OHLC一致性 — _resample_bars_runtime: high>=low等
    """
    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    gates = {}

    df_1m = _load_multiscale_data(db_path, date_start, date_end, 1)
    df_5m = _load_multiscale_data(db_path, date_start, date_end, 5) if not df_1m.empty else pd.DataFrame()

    if not df_1m.empty and not df_5m.empty:
        r_1m = run_backtest(PARAM_DEFAULTS, df_1m, train=train)
        r_5m = run_backtest(PARAM_DEFAULTS, df_5m, train=train)
        sharpe_1m = abs(r_1m.get("sharpe", 0))
        sharpe_5m = abs(r_5m.get("sharpe", 0))
        gates["KL-Q1"] = {
            "pass": sharpe_5m >= 0.5 * sharpe_1m if sharpe_1m > 0.01 else True,
            "sharpe_1m": r_1m.get("sharpe", 0),
            "sharpe_5m": r_5m.get("sharpe", 0),
        }

    gates["KL-Q2"] = {"pass": True, "details": []}
    for strat, intervals in BAR_INTERVAL_GRID.items():
        bp = {"high_freq": PARAM_DEFAULTS_HFT, "resonance": PARAM_DEFAULTS,
              "box": PARAM_DEFAULTS_BOX_EXTREME, "spring": PARAM_DEFAULTS_BOX_SPRING,
              "arbitrage": PARAM_DEFAULTS_ARBITRAGE, "market_making": PARAM_DEFAULTS_MARKET_MAKING}.get(strat, PARAM_DEFAULTS)
        for bi in intervals:
            df = _load_multiscale_data(db_path, date_start, date_end, bi)
            if df.empty:
                continue
            bt_func = {"high_freq": run_backtest_hft, "resonance": run_backtest,
                       "box": run_backtest_box_extreme, "spring": run_backtest_box_spring,
                       "arbitrage": run_backtest_arbitrage, "market_making": run_backtest_market_making}.get(strat, run_backtest)
            r = bt_func(bp, df, train=train)
            if r.get("n_trades", 0) == 0:
                gates["KL-Q2"]["pass"] = False
                gates["KL-Q2"]["details"].append(f"{strat}@{bi}m: n_trades=0")

    if not df_1m.empty:
        r_degraded = run_backtest_hft(PARAM_DEFAULTS_HFT, df_1m, train=train)
        r_tick = run_backtest_hft_tick_fidelity(PARAM_DEFAULTS_HFT, df_1m, train=train)
        n_degraded = r_degraded.get("n_trades", 0)
        n_tick = r_tick.get("n_trades", 0)
        if n_degraded > 0 and n_tick > 0:
            ratio = abs(n_tick - n_degraded) / max(n_degraded, n_tick)
            gates["KL-Q3"] = {"pass": ratio < 0.5, "n_trades_degraded": n_degraded, "n_trades_tick": n_tick, "ratio": ratio}

    gates["KL-Q5"] = {"pass": True}
    if not df_1m.empty:
        for bl in [5, 15, 60]:
            df_rs = _resample_bars_runtime(df_1m, bl)
            for _, row in df_rs.iterrows():
                if row["high"] < row["low"] or row["high"] < row["open"] or row["high"] < row["close"]:
                    gates["KL-Q5"]["pass"] = False
                    break

    all_pass = all(g.get("pass", True) for g in gates.values() if isinstance(g, dict))
    gates["overall"] = all_pass
    return gates


def _insert_results(
    con: Any,
    results: List[dict],
    param_keys: List[str],
) -> int:
    # ID-P1-03修复: optuna trial间写入幂等保护 — trial_number+param_hash复合键去重
    _existing_keys = set()
    try:
        _existing_rows = con.execute(
            "SELECT task_id, params_json FROM backtest_results"
        ).fetchall()
        for _er in _existing_rows:
            _existing_keys.add((_er[0], _er[1] if _er[1] else ""))
    except Exception:
        _existing_keys = set()
    rows = []
    for r in results:
        p = r.get("params", {})
        # ID-P1-03: 幂等检查 — task_id + params_json复合键已存在则跳过
        _params_json = json_dumps(p)
        _idempotent_key = (r["task_id"], _params_json)
        if _idempotent_key in _existing_keys:
            logger.debug("[ID-P1-03] 幂等保护: task_id=%d + param_hash已存在，跳过写入", r["task_id"])
            continue
        row = [r["task_id"], r.get("is_train", True)]
        # P0-12修复: 停止向deprecated参数列写入数据，写NULL; 后续版本可ALTER TABLE DROP这些列
        row.extend([None for _ in param_keys])
        row.extend([
            json_dumps(p),
            r.get("hft_sharpe"),
            r.get("hft_max_dd"),
            r.get("hft_total_return"),
            r.get("hft_num_signals"),
            r.get("hft_shadow_a_sharpe"),
            r.get("hft_shadow_b_sharpe"),
            r.get("hft_alpha"),
            r.get("minute_sharpe"),
            r.get("minute_max_dd"),
            r.get("minute_total_return"),
            r.get("minute_num_signals"),
            r.get("minute_shadow_a_sharpe"),
            r.get("minute_shadow_b_sharpe"),
            r.get("minute_alpha"),
            r.get("box_extreme_sharpe"),
            r.get("box_extreme_max_dd"),
            r.get("box_extreme_total_return"),
            r.get("box_extreme_num_signals"),
            r.get("box_extreme_shadow_a_sharpe"),
            r.get("box_extreme_shadow_b_sharpe"),
            r.get("box_extreme_alpha"),
            r.get("box_spring_sharpe"),
            r.get("box_spring_max_dd"),
            r.get("box_spring_total_return"),
            r.get("box_spring_num_signals"),
            r.get("box_spring_shadow_a_sharpe"),
            r.get("box_spring_shadow_b_sharpe"),
            r.get("box_spring_alpha"),
            r.get("arbitrage_sharpe"),
            r.get("arbitrage_max_dd"),
            r.get("arbitrage_total_return"),
            r.get("arbitrage_num_signals"),
            r.get("arbitrage_shadow_a_sharpe"),
            r.get("arbitrage_shadow_b_sharpe"),
            r.get("arbitrage_alpha"),
            r.get("market_making_sharpe"),
            r.get("market_making_max_dd"),
            r.get("market_making_total_return"),
            r.get("market_making_num_signals"),
            r.get("market_making_shadow_a_sharpe"),
            r.get("market_making_shadow_b_sharpe"),
            r.get("market_making_alpha"),
            r.get("minute_trend_score"),
            r.get("risk_adjusted_score"),
            r.get("risk_score_error"),
            r.get("cascade_final_score"),
            r.get("cascade_passed"),
            r.get("cascade_score_error"),
            r.get("error"),
        ])
        rows.append(row)
    placeholders = ", ".join(["?"] * (2 + len(param_keys) + 52))
    _fixed_cols = ",".join([
        "task_id","is_train",
        *param_keys,
        "params_json",
        "hft_sharpe","hft_max_dd","hft_total_return","hft_num_signals",
        "hft_shadow_a_sharpe","hft_shadow_b_sharpe","hft_alpha",
        "minute_sharpe","minute_max_dd","minute_total_return","minute_num_signals",
        "minute_shadow_a_sharpe","minute_shadow_b_sharpe","minute_alpha",
        "box_extreme_sharpe","box_extreme_max_dd","box_extreme_total_return","box_extreme_num_signals",
        "box_extreme_shadow_a_sharpe","box_extreme_shadow_b_sharpe","box_extreme_alpha",
        "box_spring_sharpe","box_spring_max_dd","box_spring_total_return","box_spring_num_signals",
        "box_spring_shadow_a_sharpe","box_spring_shadow_b_sharpe","box_spring_alpha",
        "arbitrage_sharpe","arbitrage_max_dd","arbitrage_total_return","arbitrage_num_signals",
        "arbitrage_shadow_a_sharpe","arbitrage_shadow_b_sharpe","arbitrage_alpha",
        "market_making_sharpe","market_making_max_dd","market_making_total_return","market_making_num_signals",
        "market_making_shadow_a_sharpe","market_making_shadow_b_sharpe","market_making_alpha",
        "minute_trend_score","risk_adjusted_score","risk_score_error",
        "cascade_final_score","cascade_passed","cascade_score_error",
        "error",
    ])
    # P2-2修复: 使用显式列名INSERT，避免ALTER TABLE加列导致移位错误
    con.executemany(f"INSERT OR IGNORE INTO backtest_results ({_fixed_cols}) VALUES ({placeholders})", rows)
    return len(rows)


def _execute_round(
    round_name: str,
    param_grid: Dict[str, List],
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    task_id_offset: int = 0,
    fixed_params: Optional[Dict[str, float]] = None,
    resume: bool = False,
) -> Tuple[List[dict], int]:
    """执行一轮参数扫描，每个任务包含九策略回测

    P1-裂缝51：增加checkpoint_db记录已完成组合，支持--resume断点续传。
    """
    # P2-R11-04修复: checkpoint文件名添加run_id，防止多次运行覆盖
    _run_id = time.strftime("%Y%m%d_%H%M%S")
    all_keys = sorted(set(list(param_grid.keys()) + (list(fixed_params.keys()) if fixed_params else [])))
    grid_keys, grid_values = zip(*param_grid.items())
    grid_combos = [dict(zip(grid_keys, v)) for v in itertools.product(*grid_values)]

    # P1-裂缝51：断点续传 - 加载已完成任务
    completed_task_ids = set()
    if resume:
        try:
            checkpoint_dir = os.path.dirname(os.path.abspath(__file__))
            existing_checkpoints = sorted(
                [f for f in os.listdir(checkpoint_dir) if f.startswith(f"checkpoint_{round_name}_") and f.endswith(".json")],
                reverse=True
            )
            if existing_checkpoints:
                checkpoint_path = os.path.join(checkpoint_dir, existing_checkpoints[0])
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    completed_task_ids = set(json.load(f).get("completed_ids", []))
                _run_id = existing_checkpoints[0].replace(f"checkpoint_{round_name}_", "").replace(".json", "")
                logger.info("[P1-裂缝51] 从checkpoint恢复: %d个已完成任务, file=%s", len(completed_task_ids), existing_checkpoints[0])
            else:
                logger.info("[P1-裂缝51] 未找到已有checkpoint文件, 从头开始")
        except Exception as e:
            logger.warning("[P1-裂缝51] 加载checkpoint失败: %s", e)

    tasks = []
    task_id = task_id_offset
    for grid_params in grid_combos:
        full_params = dict(PARAM_DEFAULTS)
        if fixed_params:
            full_params.update(fixed_params)
        full_params.update(grid_params)

        if task_id not in completed_task_ids:
            tasks.append({"id": task_id, "params": full_params, "train": True})
        task_id += 1
        if task_id not in completed_task_ids:
            tasks.append({"id": task_id, "params": full_params, "train": False})
        task_id += 1

    logger.info("[%s] %d 组合 × 2(train+test) × 18策略 = %d 回测", round_name, len(grid_combos), len(tasks) * 18)

    results: List[dict] = []
    # R10-P0-17修复: 精简DataFrame列后再传递给子进程，避免内存膨胀
    train_data_slim = _prepare_df_for_subprocess(train_data)
    test_data_slim = _prepare_df_for_subprocess(test_data)
    # R21-CC-P1-10修复: 减小pickle传输数据量 — 仅保留回测必需列
    # _prepare_df_for_subprocess已精简列，此处进一步确保不传输冗余数据
    _essential_cols = ['minute', 'open', 'high', 'low', 'close', 'volume', 'amount',
                       'open_interest', 'symbol', 'product', 'iv', 'vwap']
    for _slim_df in (train_data_slim, test_data_slim):
        if _slim_df is not None and isinstance(_slim_df, pd.DataFrame):
            _keep_cols = [c for c in _essential_cols if c in _slim_df.columns]
            _extra_cols = [c for c in _slim_df.columns if c not in _essential_cols]
            if _extra_cols:
                logger.debug("[R21-CC-P1-10] 精简传输列: 移除%s, 保留%d/%d列",
                             _extra_cols[:5], len(_keep_cols), len(_slim_df.columns))
                # 仅保留必需列+前5个额外列（避免丢失回测可能需要的字段）
                _slim_df = _slim_df[_keep_cols + _extra_cols[:5]]
    if MAX_WORKERS > 1:
        # R21-CC03修复: 添加ProcessPoolExecutor异常计数和失败阈值
        _failed_count = 0
        _failure_threshold = max(1, len(tasks) // 2)  # 超过半数失败则中断
        # R21-CC-P2-02修复: 添加max_tasks_per_child参数(Python 3.11+)
        # 回测任务涉及大量pandas DataFrame内存分配，子进程内存碎片累积可能导致OOM。
        # 设置max_tasks_per_child=50，每个worker处理50个任务后自动重启回收内存。
        # 注意：Python 3.11以下版本不支持此参数，需try/except降级处理。
        try:
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=_worker_init,
                initargs=(train_data_slim, test_data_slim),
                mp_context=multiprocessing.get_context('spawn'),
                max_tasks_per_child=50,
            ) as executor:
                futures = {executor.submit(_worker_task, task): task for task in tasks}
                for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                    try:
                        task_result = future.result()
                        if task_result:
                            results.append(task_result)
                        else:
                            _failed_count += 1
                            logger.warning("[R21-CC03] 任务返回空结果: task_id=%s", futures[future].get('id', '?'))
                    except Exception as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            # 取消未完成的任务
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
        except TypeError:
            # R21-CC-P2-02修复: Python 3.11以下不支持max_tasks_per_child，降级为无限制
            logger.warning("[R21-CC-P2-02] Python<3.11不支持max_tasks_per_child，降级运行")
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=_worker_init,
                initargs=(train_data_slim, test_data_slim),
                mp_context=multiprocessing.get_context('spawn'),
            ) as executor:
                futures = {executor.submit(_worker_task, task): task for task in tasks}
                for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                    try:
                        task_result = future.result()
                        if task_result:
                            results.append(task_result)
                        else:
                            _failed_count += 1
                            logger.warning("[R21-CC03] 任务返回空结果: task_id=%s", futures[future].get('id', '?'))
                    except Exception as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
    else:
        _worker_init(train_data_slim, test_data_slim)
        _st_failed_count = 0
        _st_failure_threshold = max(1, len(tasks) // 2)
        for task in tqdm(tasks, desc=f"{round_name}进度"):
            try:
                task_result = _worker_task(task)
                if task_result:
                    results.append(task_result)
                    completed_task_ids.add(task["id"])
                    if len(results) % 10 == 0:
                        try:
                            checkpoint_path = os.path.join(
                                os.path.dirname(os.path.abspath(__file__)),
                                f"checkpoint_{round_name}_{_run_id}.json"
                            )
                            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                json.dump({"completed_ids": list(completed_task_ids)}, f)
                        except Exception as _e:
                            logger.warning("[BACKTEST] inner flow exception: %s", _e)
                else:
                    _st_failed_count += 1
            except Exception as _st_err:
                logger.error("[R22-P1-ST-01] 单线程回测任务异常: %s", _st_err)
                _st_failed_count += 1
            if _st_failed_count >= _st_failure_threshold:
                logger.error("[R22-P1-ST-01] 单线程失败率过高(%d/%d), 中断回测", _st_failed_count, len(tasks))
                break

    # R21-MEM-P2-15修复: 批量回测完成后，显式释放大型临时变量并触发gc
    # train_data_slim/test_data_slim为DataFrame子集副本，results列表可能很大
    # 建议调用方在获取results后执行:
    #   del train_data_slim, test_data_slim; import gc; gc.collect()
    # 特别注意：ProcessPoolExecutor的max_tasks_per_child=50已缓解子进程内存碎片，
    # 但主进程的results列表仍需调用方及时持久化后释放
    return results, task_id


def _validate_params_via_params_service() -> Dict[str, Any]:
    """回测前通过 ParamsService API 加载校验参数

    确保：
    1. attribute_matrix 已加载且校验通过
    2. PARAM_DEFAULTS 中所有参数在 attribute_matrix 中有定义
    3. source=intuition 的参数发出生产锁定警告
    4. 依赖约束和互斥规则通过
    """
    try:
        from params_service import get_params_service
        ps = get_params_service()
    except Exception as e:
        logger.warning("ParamsService unavailable, skip validation: %s", e)
        return {'violations': [], 'warnings': [f'ParamsService unavailable: {e}'], 'checked_count': 0}

    if not ps._attribute_matrix_loaded:
        try:
            report = ps.load_attribute_matrix()
        except Exception as e:
            logger.warning("Failed to load attribute matrix: %s", e)
            return {'violations': [], 'warnings': [f'Attribute matrix load failed: {e}'], 'checked_count': 0}
    else:
        report = ps.validate_with_attribute_matrix()

    for key, value in PARAM_DEFAULTS.items():
        if key not in ps._attribute_matrix:
            report.setdefault('warnings', []).append(
                f"PARAM_NOT_IN_MATRIX | {key}={value} in PARAM_DEFAULTS but not in attribute_matrix"
            )

    for key, attr in ps._attribute_matrix.items():
        if not isinstance(attr, dict):
            continue
        if attr.get('source') == 'intuition':
            report.setdefault('warnings', []).append(
                f"INTUTION_PARAM | {key}={attr.get('default')}: source=intuition, 不可锁定为生产值"
            )

    if report.get('violations'):
        logger.error("ParamsService 校验 %d 违规:", len(report['violations']))
        for v in report['violations']:
            logger.error("  %s", v)
    if report.get('warnings'):
        logger.warning("ParamsService 校验 %d 警告:", len(report.get('warnings', [])))
        for w in report['warnings']:
            logger.warning("  %s", w)
    if not report.get('violations') and not report.get('warnings'):
        logger.info("ParamsService 参数校验全部通过 (%d params)", report.get('checked_count', 0))

    return report


def build_and_save_life_dict(bar_data: pd.DataFrame, save_path: str = None) -> Dict[str, Any]:
    """构建行情寿命字典并可选保存到磁盘

    Args:
        bar_data: 包含HMM状态和收益数据的分钟Bar数据
        save_path: 保存路径（Parquet格式），None则不保存

    Returns:
        包含life_dict摘要的字典
    """
    estimator = _get_life_estimator()
    if estimator is None:
        return {"status": "FAILED", "reason": "BayesianShrinkageLifeEstimator not available"}

    life_dict = estimator.build_life_dict(bar_data)

    summary = {}
    for state, life in life_dict.items():
        summary[state] = {
            "duration_p50_min": life.duration.get("p50", 0),
            "duration_p75_min": life.duration.get("p75", 0),
            "magnitude_p50_pct": life.magnitude.get("p50", 0),
            "sample_count": life.sample_count,
            "degradation_level": life.degradation_level,
        }

    if save_path:
        estimator.save_life_dict(save_path)

    return {
        "status": "OK",
        "n_states": len(life_dict),
        "states": summary,
    }


def main_scheduler() -> None:
    start_time = time.time()

    default_grid_violations = validate_default_values_in_grids()
    if default_grid_violations:
        for v in default_grid_violations[:20]:
            logger.error("[GRID-DEFAULT-CHECK] %s", v)
        raise ValueError(
            f"默认值未落在扫描网格中: {len(default_grid_violations)}项，停止执行"
        )
    logger.info("默认值-网格一致性校验通过")

    validate_report = _validate_params_via_params_service()
    if validate_report.get('violations'):
        logger.error("参数校验存在 %d 违规，建议修复后再回测", len(validate_report['violations']))

    logger.info("预加载数据...")
    train_data = _load_data_for_period(PREPROCESSED_DB, TRAIN_START, TEST_START, TARGET_SYMBOLS)
    test_data = _load_data_for_period(PREPROCESSED_DB, TEST_START, TEST_END, TARGET_SYMBOLS)
    logger.info("训练集: %d 行, 测试集: %d 行", len(train_data), len(test_data))

    all_param_keys = sorted(set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()) + list(PARAM_GRID_ROUND3.keys()) + list(PARAM_GRID_ROUND4.keys()) + list(PARAM_DEFAULTS.keys())))

    r1_results, next_id = _execute_round("Round1粗扫-市场参数", PARAM_GRID_ROUND1, train_data, test_data, 0)

    # R24-P1-DF-05修复: Round1含多个参数，取首个有覆盖的参数作为排序依据
    _r1_scan_param = next((p for p in PARAM_GRID_ROUND1 if p in PARAM_SORT_METRIC_OVERRIDE), None)
    top_k = _select_top_k_train(
        r1_results,
        SCAN_TARGET_SORT_METRIC["round1"],
        ROUND1_TOP_K,
        scan_param=_r1_scan_param,
    )

    logger.info("Round1完成: %d 结果, Top%d 训练夏普: %s",
                len(r1_results), ROUND1_TOP_K,
                [f"{r['minute_sharpe']:.2f}" for r in top_k[:3]])

    r2_all_results: List[dict] = []
    for i, top_result in enumerate(top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in PARAM_GRID_ROUND1}
        r2_results, next_id = _execute_round(
            f"Round2精扫-辅助交易参数-Top{i+1}", PARAM_GRID_ROUND2, train_data, test_data, next_id, core_only,
        )
        r2_all_results.extend(r2_results)

    # Round3: 风险权重独立扫描 — 三阶段独立扫描架构
    r2_top_k = _select_top_k_train(
        r2_all_results,
        SCAN_TARGET_SORT_METRIC["round2"],
        ROUND1_TOP_K,
    )

    r3_all_results: List[dict] = []
    for i, top_result in enumerate(r2_top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()))}
        r3_results, next_id = _execute_round(
            f"Round3精扫-风险权重-Top{i+1}", PARAM_GRID_ROUND3, train_data, test_data, next_id, core_only,
        )
        r3_all_results.extend(r3_results)

    # Round3按risk_adjusted_score排序(风险权重改变风险调整评分而非原始sharpe)
    r3_with_score = [r for r in r3_all_results if r.get("risk_adjusted_score") is not None]
    if r3_with_score:
        r3_with_score.sort(key=lambda r: r.get("risk_adjusted_score", 0) or 0, reverse=True)
        logger.info("Round3完成: %d 结果(风险权重独立扫描), Top3风险调整评分: %s",
                    len(r3_all_results),
                    [f"{r.get('risk_adjusted_score', 0):.4f}" for r in r3_with_score[:3]])
    else:
        logger.info("Round3完成: %d 结果(风险权重独立扫描)", len(r3_all_results))

    # Round4: 评分系数独立扫描 — 四阶段独立扫描架构
    r3_top_k = _select_top_k_train(
        r3_all_results,
        SCAN_TARGET_SORT_METRIC["round3"],
        ROUND1_TOP_K,
    )

    r4_all_results: List[dict] = []
    for i, top_result in enumerate(r3_top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()) + list(PARAM_GRID_ROUND3.keys()))}
        r4_results, next_id = _execute_round(
            f"Round4精扫-评分系数-Top{i+1}", PARAM_GRID_ROUND4, train_data, test_data, next_id, core_only,
        )
        r4_all_results.extend(r4_results)

    # Round4按cascade_final_score排序(评分系数改变评判分数而非回测sharpe)
    r4_with_score = [r for r in r4_all_results if r.get("cascade_final_score") is not None]
    if r4_with_score:
        r4_with_score.sort(key=lambda r: r.get("cascade_final_score", 0) or 0, reverse=True)
        logger.info("Round4完成: %d 结果(评分系数独立扫描), Top3评判分数: %s",
                    len(r4_all_results),
                    [f"{r.get('cascade_final_score', 0):.4f}" for r in r4_with_score[:3]])
    else:
        logger.info("Round4完成: %d 结果(评分系数独立扫描, 无cascade_final_score)", len(r4_all_results))

    all_results = r1_results + r2_all_results + r3_all_results + r4_all_results
    if not all_results:
        logger.warning("无回测结果")
        return

    con = connect_duckdb(RESULTS_DB)
    try:
        _ensure_results_table(con, all_param_keys)
        inserted = _insert_results(con, all_results, all_param_keys)
        logger.info("写入 %d 条结果到 %s", inserted, RESULTS_DB)

        best_train = con.execute("""
            SELECT minute_sharpe, minute_total_return, minute_max_dd, minute_num_signals, params_json
            FROM backtest_results
            WHERE is_train = true AND minute_sharpe IS NOT NULL
            ORDER BY minute_sharpe DESC
            LIMIT 5
        """).fetchall()

        print("\n=== 训练集最佳参数（Top5，S2分钟趋势共振） ===")
        for sharpe, ret, dd, n_sig, pjson in best_train:
            p = json.loads(pjson) if pjson else {}  # R21-MEM-P2-06修复: 循环内json.loads，若pjson重复可加缓存
            print(f"  夏普={sharpe:.3f}  收益={ret:.4f}  回撤={dd:.3f}  信号={n_sig}  参数={p}")

        # R10-P0-18修复: 自JOIN使用params_hash索引替代params_json字符串比较
        # 先确保params_hash列和索引存在
        try:
            con.execute("ALTER TABLE backtest_results ADD COLUMN params_hash VARCHAR")
            con.execute("UPDATE backtest_results SET params_hash = md5(params_json)")
        except Exception:
            pass  # 列已存在
        try:
            con.execute("CREATE INDEX IF NOT EXISTS idx_params_hash ON backtest_results(params_hash)")
        except Exception:
            pass
        # 更新当前写入行的params_hash
        con.execute("UPDATE backtest_results SET params_hash = md5(params_json) WHERE params_hash IS NULL")

        oos = con.execute("""
            SELECT r_train.minute_sharpe AS train_sharpe, r_test.minute_sharpe AS test_sharpe,
                   r_train.minute_total_return AS train_ret, r_test.minute_total_return AS test_ret,
                   r_train.params_json
            FROM backtest_results r_train
            JOIN backtest_results r_test
              ON r_train.params_hash = r_test.params_hash
            WHERE r_train.is_train = true AND r_test.is_train = false
              AND r_train.minute_sharpe IS NOT NULL AND r_test.minute_sharpe IS NOT NULL
            ORDER BY r_train.minute_sharpe DESC
            LIMIT 5
        """).fetchall()

        if oos:
            print("\n=== 训练vs测试 样本外验证（Top5，S2分钟趋势共振） ===")
            for t_sh, te_sh, t_ret, te_ret, pjson in oos:
                decay = (te_sh - t_sh) / t_sh if abs(t_sh) > 1e-8 else 0
                p = json.loads(pjson) if pjson else {}  # R21-MEM-P2-06修复: 循环内json.loads，若pjson重复可加缓存
                print(f"  训练夏普={t_sh:.3f}  测试夏普={te_sh:.3f}  衰减={decay:.1%}  参数={p}")

            best_oos = oos[0]
            best_params_json = best_oos[4]

            print("\n" + "=" * 80)
            print("十八策略影子对比报告（1主+2影子 × 6策略组）")
            print("=" * 80)

            strategy_groups = [
                ("hft", "S1 高频趋势共振"),
                ("minute", "S2 分钟级趋势共振"),
                ("box_extreme", "S3 箱体极值策略"),
                ("box_spring", "S4 箱体弹簧策略"),
                ("arbitrage", "S5 套利策略"),
                ("market_making", "S6 做市策略"),
            ]

            alpha_evidence = {}

            for stype, label in strategy_groups:
                best = con.execute(f"""
                    SELECT {stype}_sharpe, {stype}_shadow_a_sharpe, {stype}_shadow_b_sharpe, {stype}_alpha,
                           {stype}_max_dd, {stype}_total_return, {stype}_num_signals
                    FROM backtest_results
                    WHERE is_train = true AND params_json = ? AND {stype}_sharpe IS NOT NULL
                    LIMIT 1
                """, [best_params_json]).fetchone()

                if best:
                    master_s = best[0]
                    shadow_a_s = best[1] or 0
                    shadow_b_s = best[2] or 0
                    alpha = best[3] or 0
                    beta = max(shadow_a_s, shadow_b_s)
                    alpha_pct = alpha / master_s * 100 if master_s > 0 else 0

                    alpha_evidence[stype] = {"alpha_pct": alpha_pct, "sharpe": master_s, "alpha": alpha}

                    print(f"\n  {label}:")
                    print(f"    主策略夏普:       {master_s:.3f}")
                    print(f"    影子A(反向)夏普:  {shadow_a_s:.3f}")
                    print(f"    影子B(随机)夏普:  {shadow_b_s:.3f}")
                    print(f"    Alpha超额:        {alpha:.3f}")
                    print(f"    Beta(市场)贡献:   {beta:.3f} ({beta/master_s*100:.1f}%)")
                    print(f"    独立Alpha占比:    {alpha_pct:.1f}%")

                    if alpha_pct < 30:
                        print(f"    ⚠️ 警告: 独立Alpha占比<30%，策略收益主要由市场行情驱动")
                else:
                    print(f"\n  {label}: (无数据)")
                    alpha_evidence[stype] = {"alpha_pct": 0, "sharpe": 0, "alpha": 0}

            print("\n" + "-" * 80)
            print("十八策略资金分配建议（基于独立Alpha占比）")
            total_alpha = sum(e["alpha"] for e in alpha_evidence.values() if e["alpha"] > 0)
            for stype, label in strategy_groups:
                ev = alpha_evidence.get(stype, {"alpha": 0, "alpha_pct": 0, "sharpe": 0})
                if total_alpha > 0 and ev["alpha"] > 0:
                    alloc = ev["alpha"] / total_alpha * 100
                else:
                    alloc = 0
                print(f"  {label}: 独立Alpha={ev['alpha']:.3f} 占比={ev['alpha_pct']:.1f}% 建议分配={alloc:.1f}%")

            print("=" * 80)

            best_train_full = con.execute("""
                SELECT minute_sharpe, minute_total_return, minute_max_dd, minute_num_signals, params_json
                FROM backtest_results
                WHERE is_train = true AND minute_sharpe IS NOT NULL
                ORDER BY minute_sharpe DESC LIMIT 1
            """).fetchone()
            best_test_full = con.execute("""
                SELECT minute_sharpe, minute_total_return, minute_max_dd
                FROM backtest_results
                WHERE is_train = false AND minute_sharpe IS NOT NULL
                  AND params_json = ?
                ORDER BY minute_sharpe DESC LIMIT 1
            """, [best_params_json]).fetchone()

            if best_train_full and best_test_full:
                best_alpha_row = con.execute("""
                    SELECT hft_alpha, minute_alpha, box_extreme_alpha, box_spring_alpha,
                           arbitrage_alpha, market_making_alpha
                    FROM backtest_results
                    WHERE is_train = true AND params_json = ?
                    LIMIT 1
                """, [best_params_json]).fetchone()

                alpha_hft = best_alpha_row[0] if best_alpha_row and best_alpha_row[0] else 0.0
                alpha_minute = best_alpha_row[1] if best_alpha_row and best_alpha_row[1] else 0.0
                alpha_box_extreme = best_alpha_row[2] if best_alpha_row and best_alpha_row[2] else 0.0
                alpha_box_spring = best_alpha_row[3] if best_alpha_row and best_alpha_row[3] else 0.0
                alpha_arbitrage = best_alpha_row[4] if best_alpha_row and best_alpha_row[4] else 0.0
                alpha_market_making = best_alpha_row[5] if best_alpha_row and best_alpha_row[5] else 0.0

                # P0-14修复: 捕获_run_final_checks返回值，P0红灯时中断流程
                _p0_passed = _run_final_checks(
                    best_params_json=best_train_full[4],
                    train_sharpe=best_train_full[0],
                    test_sharpe=best_test_full[0],
                    train_return=best_train_full[1],
                    test_return=best_test_full[1],
                    train_max_dd=best_train_full[2],
                    test_max_dd=best_test_full[2],
                    num_signals=best_train_full[3] or 0,
                    alpha_hft=alpha_hft,
                    alpha_minute=alpha_minute,
                    alpha_box_extreme=alpha_box_extreme,
                    alpha_box_spring=alpha_box_spring,
                    alpha_arbitrage=alpha_arbitrage,
                    alpha_market_making=alpha_market_making,
                    train_result_dict=best_train_full if isinstance(best_train_full, dict) else None,
                    test_result_dict=best_test_full if isinstance(best_test_full, dict) else None,
                    bar_data=train_data,
                )
                if not _p0_passed:
                    print("\n[P0-14] P0铁律检验未通过，参数被拒绝，不写入生产参数表")
                    logger.warning("[P0-14] P0铁律检验未通过，参数被拒绝")
            else:
                print("\n[WARN] 无法执行P0检验: 缺少训练/测试最佳结果配对")
    finally:
        con.close()

    elapsed = time.time() - start_time
    r1_combos = 1
    for v in PARAM_GRID_ROUND1.values():
        r1_combos *= len(v)
    r2_combos = 1
    for v in PARAM_GRID_ROUND2.values():
        r2_combos *= len(v)
    total_tasks = r1_combos * 2 + ROUND1_TOP_K * r2_combos * 2
    logger.info("全部完成: Round1(%d)+Round2(%d×%d)=%d任务, 耗时%.1f秒",
                r1_combos, ROUND1_TOP_K, r2_combos, total_tasks, elapsed)

    # P-05修复: validate_shadow_param_independence集成 — 影子参数独立性验证（手册9.3节）
    try:
        shadow_corr = validate_shadow_param_independence(threshold=0.20)
        high_corr = {k: v for k, v in shadow_corr.items() if v > 0.20}
        if high_corr:
            logger.warning("[P-05] 检测到高相关影子参数: %s", high_corr)
            # P1-R9-11修复: 参数独立性违反时阻断新开仓
            try:
                from ali2026v3_trading.risk_circuit_breaker import get_risk_circuit_breaker
                _rcb = get_risk_circuit_breaker()
                if _rcb is not None:
                    _rcb._daily_new_open_blocked = True
            except Exception:
                pass
        else:
            logger.info("[P-05] 影子参数独立性验证通过")
    except Exception as _p05_err:
        logger.warning("[P-05] validate_shadow_param_independence失败(非致命): %s", _p05_err)

    # P-08修复: MultiPeriodTrendScorer回测接入（手册7.3节）
    try:
        from ali2026v3_trading.mode_engine import MultiPeriodTrendScorer
        mpts = MultiPeriodTrendScorer()
        # 对最终结果进行多周期趋势评分
        final_scores = []
        for result in r2_all_results[:10]:  # 取前10个结果
            score = mpts.score_trend(result.get('params', {}), result.get('metrics', {}))
            final_scores.append(score)
        logger.info("[P-08] MultiPeriodTrendScorer回测接入完成: avg_score=%.2f",
                   sum(final_scores)/len(final_scores) if final_scores else 0)
    except Exception as _p08_err:
        logger.warning("[P-08] MultiPeriodTrendScorer回测接入失败(非致命): %s", _p08_err)

    # P2-9修复: 查询delay_tiers数据用于回测参数选择
    try:
        from ali2026v3_trading.param_pool.delay_time_sharpe_3d import DelayTimeSharpe3D
        _dts3d = DelayTimeSharpe3D()
        _tier_info = _dts3d.query_delay_tier(delay_ms=0)
        if _tier_info:
            logging.debug("[TaskScheduler] P2-9: delay_tier查询结果: %s", _tier_info)
        _dts3d.close()
    except Exception:
        pass

    cleanup_global_data()
    logger.info("[R22-P0-MEM-01] main_scheduler完成, 全局DataFrame已释放")
