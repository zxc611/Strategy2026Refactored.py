# [M1-131] 结果写入与质量门
# MODULE_ID: M1-192
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
#!/usr/bin/env python3
"""ts_result_writer — 结果写入与质量门子模块 (R27-CP-08-FIX: 从task_scheduler.py拆分)

包含:
  - DuckDB结果写入: _validate_ddl_column_names, _ensure_results_table,
    _get_completed_task_ids, _insert_results
  - 数据加载: (已拆分到 ts_result_data_loader.py, 通过re-export保持兼容)
  - HFT tick插值: _interpolate_ticks_in_bar
  - K线扫描: (已拆分到 ts_result_kline_sweep.py, 通过re-export保持兼容)
  - 执行轮次: (已拆分到 ts_result_executor.py, 通过re-export保持兼容)
  - 参数校验: _validate_params_via_params_service
  - 寿命字典: build_and_save_life_dict
  - 主调度: (已拆分到 ts_result_scheduler.py, 通过re-export保持兼容)
"""
from __future__ import annotations

import itertools
import json
import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import multiprocessing
import os
import random as _pyrandom
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
from ali2026v3_trading.infra.shared_utils import safe_price_check
from ali2026v3_trading.infra.trading_utils import CrossSystemExecutionKernel
from ali2026v3_trading.data.data_access import get_data_access
from ali2026v3_trading.data.db_adapter import connect_duckdb

_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()
logger = get_logger(__name__)  # R9-5

_runner_module = None


def _ensure_runner():
    global _runner_module
    if _runner_module is None:
        from ali2026v3_trading.param_pool import backtest_runner as _br
        _runner_module = _br
    return _runner_module

from ali2026v3_trading.param_pool._param_defaults import (
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

from ali2026v3_trading.param_pool.ts.ts_backtest_strategies import (
    DEFAULT_RISK_FREE_RATE,
    BACKTEST_THRESHOLDS,
    MULTISCALE_BAR_LENGTHS,
    BAR_INTERVAL_GRID,
    KLINE_LENGTH_PARAM_GRID,
    _SUBPROCESS_NEEDED_COLS,
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
    # P0-12修复: 70+参数列标记deprecated，建表仍保留列定义以兼容已有DB，但。insert_results不再写入
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
            
            -- P0-9修复: 6个关键计算字段列(原子worker_task计算但不入库)
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
    except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:
        logging.debug("[R3-L2] _get_completed_task_ids fallback: %s", _r3_err)
        return set()


# ── Re-export from ts_result_data_loader ──


def _interpolate_ticks_in_bar(
    bar: pd.Series,
    n_ticks: int = 5,
) -> List[Dict[str, float]]:
    """HFT tick级回测保真：在单根Bar内均匀插值模拟tick序列

    将1根分钟Bar拆解为n_ticks个虚拟tick：
      - 价格路径: 线性插值 open → high/low → close
      - 成交量: 均匀分配
      - imbalance/strength: 逐tick线性过渡（允许信号翻转检测）'
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
            "last_price": c,
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
            "last_price": prices[i],
            "volume": vol_per_tick,
            "imbalance": imb_start + (imb_end - imb_start) * frac + jitter,
            "strength": str_start + (str_end - str_start) * frac,
        }
        ticks.append(tick)

    return ticks


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
    except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:
        logging.debug("[R3-L2] existing_keys query fallback: %s", _r3_err)
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


# Re-export from ts_result_executor (拆分自本文件)


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
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        logger.warning("ParamsService unavailable, skip validation: %s", e)
        return {'violations': [], 'warnings': [f'ParamsService unavailable: {e}'], 'checked_count': 0}

    if not ps._attribute_matrix_loaded:
        try:
            report = ps.load_attribute_matrix()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
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


# Re-export from ts_result_scheduler (facade for backward compatibility)
# 延迟导入以避免循环import: ts_result_scheduler → ts_result_writer → ts_result_scheduler
def main_scheduler():
    return _real_main_scheduler()

# Re-export from ts_result_kline_sweep (facade for backward compatibility)

# ── Data Loader (merged from ts_result_data_loader.py on 2026-06-12) ──

#!/usr/bin/env python3
"""ts_result_data_loader — 数据加载子模块 (从ts_result_writer.py拆分)

包含:
  - _query_minute_table: 统一构建minute类表查询
  - _load_data_for_period: 加载指定时间段的分钟数据
  - _load_multiscale_data: 加载指定K线长度的Bar数据
  - _resample_bars_runtime: 运行时将1分钟Bar聚合为N分钟Bar
"""

from typing import Any, List, Optional



__all__ = [
    "_query_minute_table",
    "_load_data_for_period",
    "_load_multiscale_data",
    "_resample_bars_runtime",
]


_V5_EXTRA_COLUMNS = [
    "kl_rpd_k", "kl_rpd_l", "kl_rpd_r", "kl_rpd_d",
    "signal_s1", "signal_s2", "signal_s3", "signal_s4", "signal_s5", "signal_s6",
    "signal_s1_decayed", "signal_s2_decayed", "signal_s3_decayed", "signal_s4_decayed",
    "linkage_s1", "linkage_s2", "linkage_s3", "linkage_s4",
    "l0_raw_state", "l0_smoothed_state", "l0_state_entropy",
    "hmm_state", "hmm_posterior_low", "hmm_posterior_normal", "hmm_posterior_high",
    "trend_score_short", "trend_score_medium", "trend_score_long",
    "trend_direction_short", "trend_direction_medium", "trend_direction_long",
    "directional_bias", "resonance_strength", "cr_phase", "state_entropy",
    "pullback_pct_peak", "pullback_pct_entry", "pullback_pct_ma", "pullback_pct_atr",
    "obos_rsi", "obos_stoch_k", "obos_cci", "obos_williams_r", "obos_signal",
    "tvf_trend", "tvf_volatility", "tvf_flow", "tvf_risk", "tvf_pullback", "tvf_entropy",
    "kelly_fraction", "position_suggestion",
    "option_moneyness_state", "div_future_cross_term", "div_option_premium_coll",
    "div_option_near_itm", "div_reversal_signal",
]

_BASE_MINUTE_COLUMN_LIST = [
    "minute", "symbol", "open", "high", "low", "close", "volume", "open_interest",
    "bid_ask_spread", "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
    "other_pct", "strength", "imbalance", "consistency", "iv", "delta", "gamma", "vega", "theta",
    "strike_price", "expire_date", "option_type", "underlying_price", "days_to_expiry",
    "_spread_quality", "_option_metadata_quality",
]

_BASE_MINUTE_COLUMNS = ", ".join(_BASE_MINUTE_COLUMN_LIST)


def _build_minute_select_columns(con: Any, table_name: str) -> str:
    existing = {row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()}
    base_available = [c for c in _BASE_MINUTE_COLUMN_LIST if c in existing]
    v5_available = [c for c in _V5_EXTRA_COLUMNS if c in existing]
    cols = ", ".join(base_available)
    if v5_available:
        cols += ", " + ", ".join(v5_available)
    return cols


def _query_minute_table(
    con: Any,
    table_name: str,
    date_start: str,
    date_end: str,
    symbols: Optional[List[str]] = None,
    extra_filters: str = "",
    extra_params: Optional[List[Any]] = None,
) -> pd.DataFrame:
    """统一构建minute类表查询，减少重复SQL模板。V5扩展列自动检测并包含。"""
    _extra_params = list(extra_params or [])
    _select_cols = _build_minute_select_columns(con, table_name)
    if symbols and len(symbols) > 0:
        placeholders = ", ".join(["?"] * len(symbols))
        sql = f"""
            SELECT {_select_cols} FROM {table_name}
            WHERE minute >= ? AND minute < ?
              {extra_filters}
              AND symbol IN ({placeholders})
            ORDER BY symbol, minute
        """
        params = [date_start, date_end] + _extra_params + list(symbols)
    else:
        sql = f"""
            SELECT {_select_cols} FROM {table_name}
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
    bar_length_minutes∉{1,5,15,60} → 从1分钟数据在线聚合（运行时常resample）
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
        "kl_rpd_k", "kl_rpd_l", "kl_rpd_r", "kl_rpd_d",
        "signal_s1", "signal_s2", "signal_s3", "signal_s4", "signal_s5", "signal_s6",
        "signal_s1_decayed", "signal_s2_decayed", "signal_s3_decayed", "signal_s4_decayed",
        "linkage_s1", "linkage_s2", "linkage_s3", "linkage_s4",
        "l0_state_entropy",
        "hmm_posterior_low", "hmm_posterior_normal", "hmm_posterior_high",
        "trend_score_short", "trend_score_medium", "trend_score_long",
        "directional_bias", "resonance_strength", "state_entropy",
        "pullback_pct_peak", "pullback_pct_entry", "pullback_pct_ma", "pullback_pct_atr",
        "obos_rsi", "obos_stoch_k", "obos_cci", "obos_williams_r", "obos_signal",
        "tvf_trend", "tvf_volatility", "tvf_flow", "tvf_risk", "tvf_pullback", "tvf_entropy",
        "kelly_fraction", "position_suggestion",
        "div_future_cross_term", "div_option_premium_coll",
        "div_option_near_itm", "div_reversal_signal",
    ]
    last_cols = [
        "bid_ask_spread", "_spread_quality", "strike_price", "_option_metadata_quality",
        "expire_date", "days_to_expiry", "option_type",
        "underlying_price", "open_interest",
        "l0_raw_state", "l0_smoothed_state",
        "hmm_state",
        "trend_direction_short", "trend_direction_medium", "trend_direction_long",
        "cr_phase",
        "option_moneyness_state",
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

    _check_cols = available_mean + available_last
    if _check_cols:
        _nan_count = result[_check_cols].isna().sum()
        if _nan_count.any():
            logger.warning("[PD-P1-06] _resample_bars_runtime merge后存在NaN: %s", _nan_count[_nan_count > 0].to_dict())

    return result.dropna(subset=["open", "close"])

# ── Executor (merged from ts_result_executor.py on 2026-06-12) ──

#!/usr/bin/env python3
"""ts_result_executor — 执行轮次子模块 (从ts_result_writer.py拆分)

包含:
  - _execute_round: 执行一轮参数扫描
"""

import json  # R6-5: 保留用于json.JSONDecodeError
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads
from ali2026v3_trading.infra.shared_utils import atomic_replace_file  # R9-1


try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.param_pool._param_grids import (
    PARAM_DEFAULTS,
)

from ali2026v3_trading.param_pool.ts.ts_backtest_strategies import (
    _prepare_df_for_subprocess,
    _worker_init,
    _worker_task,
)



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

    P1-裂缝51：增加checkpoint_db记录已完成组合，支持--resume断点续传。'
    """
    _run_id = time.strftime("%Y%m%d_%H%M%S")
    all_keys = sorted(set(list(param_grid.keys()) + (list(fixed_params.keys()) if fixed_params else [])))
    grid_keys, grid_values = zip(*param_grid.items())
    grid_combos = [dict(zip(grid_keys, v)) for v in itertools.product(*grid_values)]

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
                    completed_task_ids = set(json_loads(f.read()).get("completed_ids", []))  # R6-5
                _run_id = existing_checkpoints[0].replace(f"checkpoint_{round_name}_", "").replace(".json", "")
                logger.info("[P1-裂缝51] 从checkpoint恢复: %d个已完成任务, file=%s", len(completed_task_ids), existing_checkpoints[0])
            else:
                logger.info("[P1-裂缝51] 未找到已有checkpoint文件, 从头开始")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
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
    train_data_slim = _prepare_df_for_subprocess(train_data)
    test_data_slim = _prepare_df_for_subprocess(test_data)
    _essential_cols = ['minute', 'open', 'high', 'low', 'close', 'volume', 'amount',
                       'open_interest', 'symbol', 'product', 'iv', 'vwap']
    for _slim_df in (train_data_slim, test_data_slim):
        if _slim_df is not None and isinstance(_slim_df, pd.DataFrame):
            _keep_cols = [c for c in _essential_cols if c in _slim_df.columns]
            _extra_cols = [c for c in _slim_df.columns if c not in _essential_cols]
            if _extra_cols:
                logger.debug("[R21-CC-P1-10] 精简传输列: 移除%s, 保留%d/%d列",
                             _extra_cols[:5], len(_keep_cols), len(_slim_df.columns))
                _slim_df = _slim_df[_keep_cols + _extra_cols[:5]]
    if MAX_WORKERS > 1:
        _failed_count = 0
        _failure_threshold = max(1, len(tasks) // 2)
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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
        except TypeError:
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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
                            atomic_replace_file(checkpoint_path, json_dumps({"completed_ids": list(completed_task_ids)}))  # R9-1
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _e:
                            logger.warning("[BACKTEST] inner flow exception: %s", _e)
                else:
                    _st_failed_count += 1
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _st_err:
                logger.error("[R22-P1-ST-01] 单线程回测任务异常: %s", _st_err)
                _st_failed_count += 1
            if _st_failed_count >= _st_failure_threshold:
                logger.error("[R22-P1-ST-01] 单线程失败率过高(%d/%d), 中断回测", _st_failed_count, len(tasks))
                break

    return results, task_id


# ── Lazy imports from backtest_runner_base (break circular import) ──

_LAZY_BRB_NAMES = {
    'run_backtest',
    'PREPROCESSED_DB',
    'RESULTS_DB',
    'MAX_WORKERS',
    'TRAIN_START',
    'TEST_START',
    'TEST_END',
    'TARGET_SYMBOLS',
    'validate_default_values_in_grids',
    'validate_shadow_param_independence',
    '_sync_random_seed',
    '_select_top_k_train',
    '_get_cascade_judge_module',
    '_get_life_estimator',
    # from backtest_state (break circular: backtest_config → ts_result_writer → backtest_state → backtest_config)
    'compute_alpha_confidence_interval',
    'run_deep_validation_tiered',
    'DEEP_VALIDATION_TIERS',
    'PARAM_TIERS',
    'L2_HYPERPARAMS',
    'PARAM_GRID_CYCLE_RESONANCE',
    'analyze_l2_sensitivity',
    'optimize_l2_params_step1',
    '_DDL_COLUMN_SAFE_PATTERN',
    # from checks_orchestrator (break circular: backtest_config → ts_result_writer → checks_orchestrator → backtest_state → backtest_config)
    '_run_final_checks',
}


def __getattr__(name):
    if name == 'validate_shadow_param_independence':
        from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence
        globals()['validate_shadow_param_independence'] = validate_shadow_param_independence
        return validate_shadow_param_independence
    if name in _LAZY_BRB_NAMES:
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
            run_backtest,
            PREPROCESSED_DB,
            RESULTS_DB,
            MAX_WORKERS,
            TRAIN_START,
            TEST_START,
            TEST_END,
            TARGET_SYMBOLS,
            validate_default_values_in_grids,
            _sync_random_seed,
            _select_top_k_train,
            _get_cascade_judge_module,
            _get_life_estimator,
        )
        globals()['run_backtest'] = run_backtest
        globals()['PREPROCESSED_DB'] = PREPROCESSED_DB
        globals()['RESULTS_DB'] = RESULTS_DB
        globals()['MAX_WORKERS'] = MAX_WORKERS
        globals()['TRAIN_START'] = TRAIN_START
        globals()['TEST_START'] = TEST_START
        globals()['TEST_END'] = TEST_END
        globals()['TARGET_SYMBOLS'] = TARGET_SYMBOLS
        globals()['validate_default_values_in_grids'] = validate_default_values_in_grids
        globals()['_sync_random_seed'] = _sync_random_seed
        globals()['_select_top_k_train'] = _select_top_k_train
        globals()['_get_cascade_judge_module'] = _get_cascade_judge_module
        globals()['_get_life_estimator'] = _get_life_estimator
        return globals()[name]

    _LAZY_BS_NAMES = {
        'compute_alpha_confidence_interval',
        'run_deep_validation_tiered',
        'DEEP_VALIDATION_TIERS',
        'PARAM_TIERS',
        'L2_HYPERPARAMS',
        'PARAM_GRID_CYCLE_RESONANCE',
        'analyze_l2_sensitivity',
        'optimize_l2_params_step1',
        '_DDL_COLUMN_SAFE_PATTERN',
    }
    if name in _LAZY_BS_NAMES:
        from ali2026v3_trading.param_pool.backtest.backtest_state import (
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
        globals()['compute_alpha_confidence_interval'] = compute_alpha_confidence_interval
        globals()['run_deep_validation_tiered'] = run_deep_validation_tiered
        globals()['DEEP_VALIDATION_TIERS'] = DEEP_VALIDATION_TIERS
        globals()['PARAM_TIERS'] = PARAM_TIERS
        globals()['L2_HYPERPARAMS'] = L2_HYPERPARAMS
        globals()['PARAM_GRID_CYCLE_RESONANCE'] = PARAM_GRID_CYCLE_RESONANCE
        globals()['analyze_l2_sensitivity'] = analyze_l2_sensitivity
        globals()['optimize_l2_params_step1'] = optimize_l2_params_step1
        globals()['_DDL_COLUMN_SAFE_PATTERN'] = _DDL_COLUMN_SAFE_PATTERN
        return globals()[name]

    _LAZY_CO_NAMES = {'_run_final_checks'}
    if name in _LAZY_CO_NAMES:
        from ali2026v3_trading.param_pool.validation.checks_orchestrator import _run_final_checks
        globals()['_run_final_checks'] = _run_final_checks
        return globals()[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# P1-裂缝51: Round2断点续传结果写入
def write_round_results(
    round_name: str,
    results: List[dict],
    checkpoint_path: Optional[str] = None,
    resume: bool = False,
) -> Dict[str, Any]:
    """写入轮次结果，支持checkpoint断点续传

    Args:
        round_name: 轮次名称
        results: 结果列表
        checkpoint_path: checkpoint文件路径
        resume: 是否启用断点续传模式
    Returns:
        Dict含written_count/checkpoint_path
    """
    written = 0
    if checkpoint_path and resume:
        try:
            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                f.write(json_dumps({"round_name": round_name, "results": results, "resume": True}))
            written = len(results)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("[P1-裂缝51] write_round_results checkpoint写入失败: %s", e)
    return {
        "written_count": written,
        "checkpoint_path": checkpoint_path,
        "resume": resume,
    }


# P2-裂缝52: 向后兼容的tick插值函数名
_interpolate_tick_from_bar = _interpolate_ticks_in_bar
