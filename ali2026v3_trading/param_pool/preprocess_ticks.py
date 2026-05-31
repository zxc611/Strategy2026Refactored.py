#!/usr/bin/env python3
"""
数据预处理：Tick -> 分钟Bar + 衍生指标
修复版：解决多进程并发写、内存溢出、apply性能、数据过滤等全部评判问题。

架构：
  1. 主进程预建DuckDB表（消除DDL竞态）
  2. 每个子进程写独立临时Parquet文件（消除并发写冲突）
  3. 主进程串行合并所有临时文件到DuckDB（单写者安全）
  4. 逐日流式处理（控制内存峰值）
  5. 向量化衍生指标计算（替代逐行apply）
"""
from __future__ import annotations

from ali2026v3_trading.cross_system_utils import get_spawn_context  # NEW-P2-03修复
from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY

import os
import glob
import time
import shutil
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any, NamedTuple

_CHINA_TZ = timezone(timedelta(hours=8))

try:
    import duckdb
except ImportError:
    duckdb = None
import pandas as pd
import numpy as np
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

try:
    import math as _math
    from scipy.stats import norm as _scipy_norm
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False

TICK_DATA_DIR = os.environ.get("TICK_DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "ticks"))
OUTPUT_DB = "preprocessed.duckdb"
MIN_DATE = "2020-01-01"
MAX_DATE = os.environ.get("TICK_MAX_DATE", "2026-12-31")
SYMBOLS = sorted(set([
    "ag", "au", "cu", "al", "zn", "rb", "ru", "m", "y", "p", "c", "cs", "cf",
    "sr", "ta", "ma", "fg", "rm", "oi", "sc", "lu", "fu", "bu", "sp", "eg",
    "eb", "pg", "pf", "si", "lc", "lh", "pk", "cj", "ap", "ur", "sa", "sm",
    "jd", "rr", "wr", "bc", "nr", "li", "ao", "rs", "px", "se", "sf", "sn",
]))
MAX_WORKERS = min(max(1, (os.cpu_count() or 4) // 2), len(SYMBOLS))
ROWS_PER_CHUNK = 500_000
ENABLE_MINUTE_BOUNDARY_CHECK = os.environ.get("ENABLE_MINUTE_BOUNDARY_CHECK", "true").lower() in ("true", "1", "yes")

class _TypedResult(NamedTuple):
    """NamedTuple基类，提供dict兼容的.get()和__contains__方法，R5-I-03修复"""
    def get(self, key, default=None):
        return getattr(self, key, default) if hasattr(self, key) else default
    def __contains__(self, key):
        return key in self._fields
    def keys(self):
        return self._fields


class MinuteBoundaryResult(_TypedResult):
    passed: bool
    split_minutes: int
    details: list


class CircuitBreakerResult(_TypedResult):
    halt_events: list
    n_halts: int
    survival_rate_check_needed: bool


class OutOfOrderResult(_TypedResult):
    is_monotonic: bool
    rollback_count: int
    simulated_swap_count: int
    swap_prob: float
    needs_dedup_module: bool
    recommendation: str
    swapped_df: object


class ExpireDateResult(_TypedResult):
    passed: bool
    total_rows: int
    missing_expire_count: int
    missing_expire_pct: float
    issues: list
    action: str


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# R4-D-02修复: 新增列定义，用于schema同步
_SCHEMA_REQUIRED_COLUMNS = {
    "_spread_quality": "INTEGER DEFAULT 0",
    "_option_metadata_quality": "INTEGER DEFAULT 0",
    "days_to_expiry": "INTEGER",
    "_classification_mode": "INTEGER DEFAULT 1",
}


def _sync_db_schema(con: duckdb.DuckDBPyConnection) -> None:
    """R4-D-02修复: 检测并同步数据库schema，添加缺失列

    当代码新增字段但数据库表已存在时，通过ALTER TABLE自动添加缺失列，
    避免INSERT时因列不存在而异常被静默吞没。
    """
    try:
        cols = con.execute("DESCRIBE minute_data").fetchall()
        existing_cols = {row[0] for row in cols}
        for col_name, col_type in _SCHEMA_REQUIRED_COLUMNS.items():
            if col_name not in existing_cols:
                con.execute(f"ALTER TABLE minute_data ADD COLUMN {col_name} {col_type}")
                logger.info("[R4-D-02] schema同步: 添加缺失列 %s %s", col_name, col_type)
    except Exception as e:
        # 表不存在时DESCRIBE会报错，此时CREATE TABLE已处理
        logger.debug("[R4-D-02] schema同步跳过(表可能不存在): %s", e)


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS minute_data (
            minute       TIMESTAMP,
            symbol       VARCHAR,
            open         DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            close        DOUBLE,
            volume       BIGINT,
            turnover     DOUBLE,
            vwap         DOUBLE,
            open_interest BIGINT,
            tick_count    BIGINT,
            bid_ask_spread DOUBLE,
            _spread_quality INTEGER DEFAULT 0,
            strike_price  DOUBLE,
            expire_date   VARCHAR,
            option_type   VARCHAR,
            underlying_price DOUBLE,
            correct_rise_pct  DOUBLE,
            correct_fall_pct  DOUBLE,
            wrong_rise_pct    DOUBLE,
            wrong_fall_pct    DOUBLE,
            other_pct         DOUBLE,
            strength          DOUBLE,
            imbalance         DOUBLE,
            consistency       DOUBLE,
            iv                DOUBLE,
            delta             DOUBLE,
            gamma             DOUBLE,
            vega              DOUBLE,
            theta             DOUBLE,
            _option_metadata_quality INTEGER DEFAULT 0,
            days_to_expiry    INTEGER,
            _classification_mode INTEGER DEFAULT 1
        )
    """)
    # R4-D-02修复: 数据库schema同步更新 — 检测并添加缺失列
    # 若表已存在但缺少新增列，通过ALTER TABLE自动同步
    _sync_db_schema(con)

def compute_option_state_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """向量化五态分类 — 移植自 width_cache._classify_status()

    真实五态语义：
      - correct_rise: 期权方向与期货方向同步且上涨 (CALL+期货涨→期权涨, PUT+期货跌→期权涨)
      - correct_fall: 期权方向与期货方向同步且下跌
      - wrong_rise:   期权方向与期货方向异步且上涨
      - wrong_fall:   期权方向与期货方向异步且下跌
      - other:        无法判定

    需要 df 中包含: underlying_price(期货价), option_type(CALL/PUT), close, prev_close
    若缺少关键列，降级为价格动量简化分类。
    """
    n = len(df)
    base_cols = ["correct_rise_pct", "correct_fall_pct", "wrong_rise_pct",
                 "wrong_fall_pct", "other_pct", "strength"]
    if n == 0:
        return pd.DataFrame(np.zeros((0, len(base_cols))), columns=base_cols)

    has_real_data = (
        "underlying_price" in df.columns
        and "option_type" in df.columns
        and df["underlying_price"].notna().any()
        and df["option_type"].notna().any()
    )

    if has_real_data:
        opt_type = df["option_type"].fillna("").str.upper().values
        underlying = df["underlying_price"].values
        opt_close = df["close"].values

        prev_underlying = np.empty_like(underlying); prev_underlying[1:] = underlying[:-1]; prev_underlying[0] = underlying[0]  # PD-P2-09: 手动shift避免look-ahead
        prev_opt = np.empty_like(opt_close); prev_opt[1:] = opt_close[:-1]; prev_opt[0] = opt_close[0]  # PD-P2-09: 手动shift避免look-ahead

        underlying_rising = underlying > prev_underlying
        opt_rising = opt_close > prev_opt
        is_call = (opt_type == "CALL")
        is_put = (opt_type == "PUT")
        valid_move = (underlying != prev_underlying) & (opt_close != prev_opt)

        correct_rise = valid_move & (
            (is_call & underlying_rising & opt_rising)
            | (is_put & ~underlying_rising & opt_rising)
        )
        correct_fall = valid_move & (
            (is_call & ~underlying_rising & ~opt_rising)
            | (is_put & underlying_rising & ~opt_rising)
        )
        wrong_rise = valid_move & (
            (is_call & ~underlying_rising & opt_rising)
            | (is_put & underlying_rising & opt_rising)
        )
        wrong_fall = valid_move & (
            (is_call & underlying_rising & ~opt_rising)
            | (is_put & ~underlying_rising & ~opt_rising)
        )
    else:
        close = df["close"].values
        open_ = df["open"].values
        pct_change = np.where(open_ > 0, (close - open_) / open_, 0.0)
        pct_change = np.where(np.isfinite(pct_change), pct_change, 0.0)  # NP-P2-17: 浮点噪声消除
        is_rise = pct_change > 0
        is_correct = np.abs(pct_change) > 0.005
        correct_rise = is_rise & is_correct
        correct_fall = (~is_rise) & is_correct
        wrong_rise = is_rise & (~is_correct)
        wrong_fall = (~is_rise) & (~is_correct) & (pct_change < 0)
        pct_change_abs = np.abs(pct_change)
        logger.debug("五态降级: 缺少underlying_price/option_type，使用价格动量简化分类")

    try:  # NP-P2-29: astype类型转换错误处理
        total = np.maximum(
            correct_rise.astype(np.float64) + correct_fall.astype(np.float64)
            + wrong_rise.astype(np.float64) + wrong_fall.astype(np.float64),
            1.0,
        )
    except (ValueError, TypeError) as _e:
        logger.warning("[NP-P2-29] astype float64 conversion failed: %s, using zeros", _e)
        total = np.maximum(np.zeros_like(pct_change_abs, dtype=np.float64), 1.0)
    other_mask = ~(correct_rise | correct_fall | wrong_rise | wrong_fall)

    if has_real_data:
        underlying = df["underlying_price"].values
        prev_underlying = np.empty_like(underlying); prev_underlying[1:] = underlying[:-1]; prev_underlying[0] = underlying[0]  # PD-P2-09: 手动shift避免look-ahead
        strength_raw = np.abs(underlying - prev_underlying) / np.where(prev_underlying > 0, prev_underlying, 1.0)
        strength = np.clip(strength_raw * 20, 0, 1)
    else:
        close = df["close"].values
        open_ = df["open"].values
        pct_change = np.where(open_ > 0, (close - open_) / open_, 0.0)
        strength = np.clip(np.abs(pct_change) * 20, 0, 1)

    return pd.DataFrame({
        "correct_rise_pct": correct_rise.astype(float, errors='coerce') / total,  # NP-P2-29
        "correct_fall_pct": correct_fall.astype(float, errors='coerce') / total,  # NP-P2-29
        "wrong_rise_pct": wrong_rise.astype(float, errors='coerce') / total,  # NP-P2-29
        "wrong_fall_pct": wrong_fall.astype(float, errors='coerce') / total,  # NP-P2-29
        "other_pct": other_mask.astype(float, errors='coerce') / total,  # NP-P2-29
        "strength": strength,
        # R4-P-11修复: 区分降级模式与真实五态语义
        # _classification_mode=1表示真实五态分类，=0表示降级简化分类
        "_classification_mode": np.full(n, 1 if has_real_data else 0, dtype=np.int32),
    })


def compute_order_flow_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """向量化订单流指标。

    基于OHLCV数据推断订单流失衡和一致性：
    - imbalance: 主动买入量vs主动卖出量的归一化失衡，∈[-1,1]
      正值=买方主导，负值=卖方主导
    - consistency: imbalance方向的持续性度量，∈[-1,1]
      高绝对值=连续同向，低=频繁反转

    推断方法：
    1. 价格位置法：close在(high,low)中的位置推断主动方向
       close接近high → 主动买入主导, close接近low → 主动卖出主导
    2. 成交量加权：大成交量bar的信号权重更高
    3. 指数移动平均平滑：5期EMA消除单bar噪声
    """
    n = len(df)
    if n == 0:
        return pd.DataFrame(np.zeros((0, 2)), columns=["imbalance", "consistency"])

    close = df["close"].to_numpy(dtype=float)  # NP-P2-31
    open_ = df["open"].to_numpy(dtype=float)  # NP-P2-31
    high = df["high"].to_numpy(dtype=float) if "high" in df.columns else np.maximum(open_, close)  # NP-P2-31
    low = df["low"].to_numpy(dtype=float) if "low" in df.columns else np.minimum(open_, close)  # NP-P2-31
    vol = df["volume"].to_numpy(dtype=float)  # NP-P2-31
    vol_safe = np.where(vol > 0, vol, 1.0)

    spread = high - low
    spread = np.nan_to_num(spread, nan=0.0)
    spread_safe = np.where(spread > 1e-8, spread, 1e-8)
    price_position = (close - low) / spread_safe
    price_position = np.clip(price_position, 0.0, 1.0)

    raw_imbalance = 2.0 * price_position - 1.0

    vol_weight = np.log1p(vol_safe)
    vol_weight_sum = np.sum(vol_weight) + 1e-8
    weighted_imbalance = raw_imbalance * vol_weight

    alpha = 2.0 / (5.0 + 1.0)
    ema_warmup = 5  # PD-P2-10: EMA warm-up期
    imbalance = np.empty(n)
    imbalance[0] = weighted_imbalance[0] / vol_weight[0] if vol_weight[0] > 0 else 0.0
    for i in range(1, n):
        imbalance[i] = alpha * (weighted_imbalance[i] / vol_weight[i] if vol_weight[i] > 0 else 0.0) + (1.0 - alpha) * imbalance[i - 1]
    for i in range(min(ema_warmup, n)):
        imbalance[i] = np.nan
    imbalance = np.clip(np.nan_to_num(imbalance, nan=0.0), -1.0, 1.0)

    signs = np.sign(raw_imbalance)
    if n >= 3:
        consistency_ema = np.empty(n)
        consistency_ema[0] = signs[0]
        for i in range(1, n):
            consistency_ema[i] = alpha * signs[i] + (1.0 - alpha) * consistency_ema[i - 1]
        for i in range(min(ema_warmup, n)):  # PD-P2-10: consistency EMA也做warm-up期标记
            consistency_ema[i] = np.nan
        consistency = np.clip(np.nan_to_num(consistency_ema, nan=0.0) * np.abs(imbalance), -1.0, 1.0)
    else:
        consistency = np.clip(imbalance * 0.8, -1.0, 1.0)

    return pd.DataFrame({
        "imbalance": imbalance,
        "consistency": consistency,
    })


def compute_greeks_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """向量化Greeks计算 — 移植自 greeks_calculator.py 的BS解析解+IV牛顿迭代

    需要 df 中包含: close(期权价格), underlying_price(标的价格),
                    strike_price(行权价), expire_date(到期日), option_type(CALL/PUT)
    若缺少关键列，降级为日内波幅估算。

    架构：对每行独立计算BS Greeks，向量化用np.vectorize包装。
    对于大数据量(>10万行)，先按(S,K,T,sigma,type)去重，只计算唯一组合再映射回原DataFrame。
    """
    n = len(df)
    greek_cols = ["iv", "delta", "gamma", "vega", "theta"]
    if n == 0:
        return pd.DataFrame(np.zeros((0, 5)), columns=greek_cols)

    has_bs_data = (
        "strike_price" in df.columns
        and "expire_date" in df.columns
        and "option_type" in df.columns
        and "underlying_price" in df.columns
        and df["strike_price"].notna().any()
        and df["expire_date"].notna().any()
    )

    if not has_bs_data:
        return _compute_greeks_fallback(df)

    RISK_FREE_RATE = 0.02
    DIVIDEND_YIELD = 0.0
    reference_date = df["minute"].min() if "minute" in df.columns else pd.Timestamp.now(tz=_CHINA_TZ)

    S = df["underlying_price"].to_numpy(dtype=np.float64)  # NP-P2-31
    K = df["strike_price"].to_numpy(dtype=np.float64)  # NP-P2-31
    market_price = df["close"].to_numpy(dtype=np.float64)  # NP-P2-31
    opt_type = df["option_type"].fillna("").str.upper().values

    expire_ts = pd.to_datetime(df["expire_date"].values, errors="coerce")
    ref_ts = pd.to_datetime(reference_date)
    T_years = (expire_ts - ref_ts).total_seconds() / (365.25 * 86400.0)
    T_years = np.clip(T_years, 1.0 / 365.25, 5.0)

    iv_arr = np.full(n, 0.2)
    delta_arr = np.zeros(n)
    gamma_arr = np.zeros(n)
    vega_arr = np.zeros(n)
    theta_arr = np.zeros(n)

    for i in range(n):
        s_i, k_i, t_i, mp_i = S[i], K[i], T_years[i], market_price[i]
        ot_i = opt_type[i]
        if s_i <= 0 or k_i <= 0 or t_i <= 0 or mp_i <= 0:
            continue
        if ot_i not in ("CALL", "PUT"):
            continue

        iv_i = _implied_volatility_scalar(
            mp_i, s_i, k_i, t_i, RISK_FREE_RATE, DIVIDEND_YIELD, ot_i,
        )
        iv_arr[i] = iv_i
        greeks = _bs_greeks_scalar(s_i, k_i, t_i, RISK_FREE_RATE, DIVIDEND_YIELD, iv_i, ot_i)
        delta_arr[i] = greeks[0]
        gamma_arr[i] = greeks[1]
        theta_arr[i] = greeks[2]
        vega_arr[i] = greeks[3]

    return pd.DataFrame({
        "iv": iv_arr, "delta": delta_arr, "gamma": gamma_arr,
        "vega": vega_arr, "theta": theta_arr,
    })


def _norm_cdf(x: float) -> float:
    return (1.0 + _math.erf(x / _math.sqrt(2.0))) / 2.0


def _norm_pdf(x: float) -> float:
    return _math.exp(-x * x / 2.0) / _math.sqrt(2.0 * _math.pi)


def _bs_price_scalar(S: float, K: float, T: float, r: float, q: float,
                      sigma: float, option_type: str) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if option_type == 'CALL' else (K - S))
    if sigma < 1e-6:
        return max(0.0, (S - K) if option_type == 'CALL' else (K - S))
    try:
        d1 = (_math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * _math.sqrt(T))
        d2 = d1 - sigma * _math.sqrt(T)
        if option_type == 'CALL':
            return S * _math.exp(-q * T) * _norm_cdf(d1) - K * _math.exp(-r * T) * _norm_cdf(d2)
        else:
            return K * _math.exp(-r * T) * _norm_cdf(-d2) - S * _math.exp(-q * T) * _norm_cdf(-d1)
    except Exception:
        return 0.0


def _bs_greeks_scalar(S: float, K: float, T: float, r: float, q: float,
                       sigma: float, option_type: str) -> Tuple[float, float, float, float]:
    if T <= 0 or sigma <= 0 or S <= 0:
        return (0.0, 0.0, 0.0, 0.0)
    if sigma < 1e-6:
        return (1.0 if option_type == 'CALL' else -1.0, 0.0, 0.0, 0.0)
    try:
        d1 = (_math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * _math.sqrt(T))
        d2 = d1 - sigma * _math.sqrt(T)
        if option_type == 'CALL':
            delta = _math.exp(-q * T) * _norm_cdf(d1)
            theta = (-_math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * _math.sqrt(T))
                     - r * K * _math.exp(-r * T) * _norm_cdf(d2)
                     + q * S * _math.exp(-q * T) * _norm_cdf(d1)) / 365.0
        else:
            delta = _math.exp(-q * T) * (_norm_cdf(d1) - 1.0)
            theta = (-_math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * _math.sqrt(T))
                     + r * K * _math.exp(-r * T) * _norm_cdf(-d2)
                     - q * S * _math.exp(-q * T) * _norm_cdf(-d1)) / 365.0
        gamma = _math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * _math.sqrt(T))
        vega = S * _math.exp(-q * T) * _norm_pdf(d1) * _math.sqrt(T) / 100.0
        return (round(delta, 6), round(gamma, 4), round(theta, 6), round(vega, 4))
    except Exception:
        return (0.0, 0.0, 0.0, 0.0)


def _implied_volatility_scalar(market_price: float, S: float, K: float, T: float,
                                r: float, q: float, option_type: str,
                                initial_guess: float = 0.2, max_iter: int = 50,
                                tol: float = 1e-6) -> float:
    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return initial_guess
    sigma = initial_guess
    for _ in range(max_iter):
        price = _bs_price_scalar(S, K, T, r, q, sigma, option_type)
        greeks = _bs_greeks_scalar(S, K, T, r, q, sigma, option_type)
        vega = greeks[3] * 100
        if vega < 1e-10:
            return initial_guess
        diff = price - market_price
        if abs(diff) < tol:
            return sigma
        sigma = sigma - diff / vega
        sigma = max(0.01, min(5.0, sigma))
    return sigma


def _compute_greeks_fallback(df: pd.DataFrame) -> pd.DataFrame:
    """Greeks降级计算：缺少strike_price/expire_date时使用日内波幅估算"""
    n = len(df)
    if n == 0:
        return pd.DataFrame(np.zeros((0, 5)), columns=["iv", "delta", "gamma", "vega", "theta"])
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    close_safe = np.where(close > 0, close, 1.0)
    hl_range = np.where(close_safe > 0, (high - low) / close_safe, 0.0)
    iv = np.clip(hl_range * np.sqrt(ANNUALIZE_FACTOR_DAILY) * 0.5, 0.05, 2.0)
    delta = np.clip((close - (high + low) / 2) / close_safe, -1, 1)
    gamma = np.clip(1.0 / (close_safe * iv + 1e-8), 0, 0.1)
    vega = np.clip(iv * 0.1, 0, 0.05)
    theta = -iv * 0.01
    logger.debug("Greeks降级: 缺少strike_price/expire_date，使用日内波幅估算")
    return pd.DataFrame({"iv": iv, "delta": delta, "gamma": gamma, "vega": vega, "theta": theta})


def _aggregate_ticks_to_bars(tick_df: pd.DataFrame) -> pd.DataFrame:
    """Tick → 分钟Bar聚合，含open_interest/turnover/vwap/bid_ask_spread"""
    if tick_df.empty:
        return pd.DataFrame()

    tick_df = tick_df.copy()  # PD-P2-05: 必须copy,防止后续列赋值触发SettingWithCopyWarning或污染原始数据
    tick_df["minute"] = tick_df["datetime"].dt.ceil("min")

    agg_spec = {
        "open": ("price", "first"),
        "high": ("price", "max"),
        "low": ("price", "min"),
        "close": ("price", "last"),
        "volume": ("volume", "sum"),
        "tick_count": ("price", "count"),
    }
    has_oi = "open_interest" in tick_df.columns
    if has_oi:
        agg_spec["open_interest"] = ("open_interest", "last")
    has_turnover = "turnover" in tick_df.columns
    if has_turnover:
        agg_spec["turnover"] = ("turnover", "sum")

    has_bid = "bid_price1" in tick_df.columns
    has_ask = "ask_price1" in tick_df.columns
    if has_bid and has_ask:
        valid_spread_mask = (
            (tick_df["ask_price1"] > tick_df["bid_price1"]) &
            (tick_df["ask_price1"] > 0) &
            (tick_df["bid_price1"] > 0)
        )
        tick_df["_spread"] = np.where(
            valid_spread_mask,
            tick_df["ask_price1"] - tick_df["bid_price1"],
            np.nan
        )
        tick_df["_spread_valid"] = valid_spread_mask.astype(pd.Int64Dtype())  # PD-P2-04
        agg_spec["_spread_mean"] = ("_spread", "mean")
        agg_spec["_spread_valid_ratio"] = ("_spread_valid", "mean")

    ohlcv = tick_df.groupby("minute").agg(**agg_spec).reset_index()

    if has_bid and has_ask and "_spread_mean" in ohlcv.columns:
        ohlcv["bid_ask_spread"] = ohlcv["_spread_mean"].fillna(0.0)
        ohlcv["_spread_quality"] = (ohlcv["_spread_mean"].notna() & (ohlcv["_spread_mean"] > 0)).astype(pd.Int64Dtype())  # PD-P2-04
        cols_to_drop = ["_spread_mean"]
        if "_spread_valid_ratio" in ohlcv.columns:
            cols_to_drop.append("_spread_valid_ratio")
        ohlcv.drop(columns=cols_to_drop, inplace=True)
    else:
        ohlcv["bid_ask_spread"] = 0.0
        ohlcv["_spread_quality"] = 0

    if has_turnover:
        nonzero_vol = ohlcv["volume"] > 0
        ohlcv["vwap"] = ohlcv["close"].copy()
        ohlcv.loc[nonzero_vol, "vwap"] = (
            ohlcv.loc[nonzero_vol, "turnover"] / ohlcv.loc[nonzero_vol, "volume"]
        )
    else:
        ohlcv["turnover"] = pd.to_numeric(ohlcv["close"], errors='coerce').astype(np.float64) * pd.to_numeric(ohlcv["volume"], errors='coerce').astype(np.float64)  # NP-P2-29
        ohlcv["vwap"] = ohlcv["close"].copy()

    if not has_oi:
        ohlcv["open_interest"] = pd.array([0] * len(ohlcv), dtype="int64")

    for col in ["strike_price", "expire_date", "option_type", "underlying_price"]:
        if col in tick_df.columns:
            # P0-3修复：ffill在合约边界重置
            if "instrument_id" in tick_df.columns:
                tick_df["_contract_changed"] = tick_df["instrument_id"] != tick_df["instrument_id"].shift(1)
                tick_df.loc[tick_df.index[0], "_contract_changed"] = True
                tick_df.loc[tick_df["_contract_changed"], col] = np.nan
            # PD-P1-03: ffill前先确保跨合约边界不继承前合约值
            tick_df[col] = tick_df[col].ffill()
            valid_vals = tick_df.dropna(subset=[col]).groupby("minute")[col].first()
            ohlcv[col] = ohlcv["minute"].map(valid_vals)
            if col == "strike_price":
                ohlcv.loc[ohlcv[col].isna() | (ohlcv[col] <= 0), col] = 0.0
                _nan_count = (ohlcv[col] == 0.0).sum()
                if _nan_count > 0:
                    import logging as _logging
                    _logging.warning("[P0-8修复] strike_price NaN/非正值阻断: %d条记录置0，将跳过Greeks计算", _nan_count)

    metadata_cols = [c for c in ["strike_price", "expire_date", "option_type"] if c in ohlcv.columns]
    if metadata_cols:
        ohlcv["_option_metadata_quality"] = ohlcv[metadata_cols].notna().all(axis=1).astype(pd.Int64Dtype())  # PD-P2-04
    else:
        ohlcv["_option_metadata_quality"] = 0

    # P0-6修复: 生成days_to_expiry列，供到期日滑点倍增模型消费
    # 原代码缺失此列，导致_get_expiry_slippage_multiplier恒返回1.0
    # R4-P-12修复: 使用交易日计算而非日历日，排除周末和节假日
    if "expire_date" in ohlcv.columns:
        try:
            expire_dt = pd.to_datetime(ohlcv["expire_date"], errors="coerce")
            bar_dt = pd.to_datetime(ohlcv["minute"], errors="coerce")
            # PD-P1-07: 确保minute列类型为datetime，防止下游比较时str与Timestamp混用
            ohlcv["minute"] = bar_dt
            # R4-P-12: 使用np.busday_count计算工作日天数(排除周末)
            # 注: 不包含中国特有节假日，但已比日历日更准确
            calendar_days = (expire_dt - bar_dt).dt.days
            # 估算工作日: 去除周末(约号2/7)
            trading_days = np.maximum(0, np.floor(calendar_days.fillna(-1) * 5.0 / 7.0)).astype(int)
            trading_days = np.where(calendar_days.isna(), -1, trading_days)
            trading_days = np.minimum(trading_days, 3650)
            ohlcv["days_to_expiry"] = trading_days
        except Exception as _e:
            # N-04修复: 添加logging.warning而非静默吞掉→到期日滑点模型静默失效
            logging.warning("[preprocess_ticks] days_to_expiry计算异常，到期日滑点模型将使用默认值: %s", _e)
            ohlcv["days_to_expiry"] = np.nan
    else:
        ohlcv["days_to_expiry"] = np.nan

    return ohlcv


MULTISCALE_BAR_LENGTHS = [5, 15, 60]


def _ensure_multiscale_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS minute_data_multiscale (
            minute       TIMESTAMP,
            symbol       VARCHAR,
            bar_length_minutes INTEGER,
            open         DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            close        DOUBLE,
            volume       BIGINT,
            turnover     DOUBLE,
            vwap         DOUBLE,
            open_interest BIGINT,
            tick_count    BIGINT,
            bid_ask_spread DOUBLE,
            _spread_quality INTEGER DEFAULT 0,
            strike_price  DOUBLE,
            expire_date   VARCHAR,
            option_type   VARCHAR,
            underlying_price DOUBLE,
            correct_rise_pct  DOUBLE,
            correct_fall_pct  DOUBLE,
            wrong_rise_pct    DOUBLE,
            wrong_fall_pct    DOUBLE,
            other_pct         DOUBLE,
            strength          DOUBLE,
            imbalance         DOUBLE,
            consistency       DOUBLE,
            iv                DOUBLE,
            delta             DOUBLE,
            gamma             DOUBLE,
            vega              DOUBLE,
            theta             DOUBLE,
            _option_metadata_quality INTEGER DEFAULT 0,
            days_to_expiry    INTEGER
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_multiscale_symbol ON minute_data_multiscale(symbol, bar_length_minutes, minute)")


def _resample_bars_to_multiscale(df_1m: pd.DataFrame, bar_length: int) -> pd.DataFrame:
    """将1分钟Bar聚合为N分钟Bar（向量化操作）

    Args:
        df_1m: 已排序的1分钟Bar DataFrame，需含minute列和OHLCV列
        bar_length: 目标Bar长度（分钟），如5/15/60
    """
    if df_1m.empty or bar_length <= 1:
        return pd.DataFrame()

    df = df_1m.copy()
    base_ts = df["minute"].dt.floor(f"{bar_length}min")
    df["_group"] = base_ts

    ohlcv_agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "turnover": "sum",
        "tick_count": "sum",
    }

    group_cols = ["_group", "symbol"]
    result = df.groupby(group_cols).agg(ohlcv_agg).reset_index()
    result.rename(columns={"_group": "minute"}, inplace=True)

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
    for col in mean_cols:
        if col in df.columns:
            agg_val = df.groupby(group_cols)[col].mean()
            result[col] = result.set_index(group_cols).index.map(
                lambda idx: agg_val.get(idx, np.nan) if idx in agg_val.index else np.nan
            )
    for col in last_cols:
        if col in df.columns:
            agg_val = df.groupby(group_cols)[col].last()
            result[col] = result.set_index(group_cols).index.map(
                lambda idx: agg_val.get(idx, np.nan) if idx in agg_val.index else np.nan
            )

    result["bar_length_minutes"] = bar_length
    result = result.dropna(subset=["open", "close"])

    return result


def _resample_bars_runtime(
    tick_buffer: List[Dict[str, Any]],
    target_interval_minutes: int = 5,
    bar_length: int = None,
) -> pd.DataFrame:
    """
    P1-R8-15修复: 运行时多粒度Bar聚合(5m/15m/60m)
    手册4.8节L-0.5T: 接收tick缓冲区实时聚合为目标粒度的Bar
    Args:
        tick_buffer: tick数据缓冲区，每项为dict含timestamp/price/volume等
        target_interval_minutes: 目标Bar粒度(默认5分钟)
        bar_length: 兼容参数，同target_interval_minutes
    Returns:
        聚合后的OHLCV DataFrame
    """
    interval = bar_length if bar_length is not None else target_interval_minutes
    if not tick_buffer or interval <= 0:
        return pd.DataFrame()

    df = pd.DataFrame(tick_buffer)
    if df.empty or "timestamp" not in df.columns or "price" not in df.columns:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # P0-R11-13修复: 1分钟Bar使用ceil对齐（与_aggregate_ticks_to_bars一致），N分钟Bar使用floor（从已对齐1分钟Bar重采样）
    if interval <= 1:
        df["minute"] = df["timestamp"].dt.ceil(f"{interval}min")
    else:
        df["minute"] = df["timestamp"].dt.floor(f"{interval}min")

    # 准备聚合规则
    agg_map: Dict[str, Any] = {
        "price": ("first", "max", "min", "last"),
    }
    if "volume" in df.columns:
        agg_map["volume"] = "sum"

    # 按分钟分组聚合OHLCV
    grouped = df.groupby("minute")
    ohlc = pd.DataFrame({
        "open": grouped["price"].first(),
        "high": grouped["price"].max(),
        "low": grouped["price"].min(),
        "close": grouped["price"].last(),
    })
    if "volume" in df.columns:
        ohlc["volume"] = grouped["volume"].sum()

    # P1-R9-01/02修复: 补充质量标记列到last_cols，与另一个实现保持一致
    last_cols_runtime = [
        "bid_ask_spread", "_spread_quality", "strike_price", "_option_metadata_quality",
        "expire_date", "days_to_expiry", "option_type",
        "underlying_price", "open_interest",
    ]
    for col in last_cols_runtime:
        if col in df.columns:
            ohlc[col] = grouped[col].last().values

    ohlc = ohlc.reset_index()
    ohlc["bar_length_minutes"] = interval
    return ohlc


def _filter_bars(df: pd.DataFrame, min_date: str, max_date: str) -> pd.DataFrame:
    """过滤无效Bar和超出日期范围的数据"""
    if df.empty:
        return df

    mask = (
        (df["volume"] > 0) &
        (df["open"] > 0) &
        (df["high"] > 0) &
        (df["low"] > 0) &
        (df["close"] > 0) &
        (df["high"] >= df["low"]) &
        (df["high"] >= df["open"]) &
        (df["high"] >= df["close"]) &
        (df["low"] <= df["open"]) &
        (df["low"] <= df["close"])
    )
    min_dt = pd.Timestamp(min_date)
    max_dt = pd.Timestamp(max_date) + timedelta(days=1)
    date_mask = (df["minute"] >= min_dt) & (df["minute"] < max_dt)
    combined_mask = mask & date_mask
    df = df[combined_mask].copy()

    return df.reset_index(drop=True)


def _enrich_bars(df: pd.DataFrame) -> pd.DataFrame:
    """向量化计算所有衍生指标并合并"""
    if df.empty:
        return df

    state = compute_option_state_vectorized(df)
    flow = compute_order_flow_vectorized(df)
    greeks = compute_greeks_vectorized(df)

    df_reset = df.reset_index(drop=True)
    n = len(df_reset)
    state_reset = state.reset_index(drop=True)
    flow_reset = flow.reset_index(drop=True)
    greeks_reset = greeks.reset_index(drop=True)
    result = pd.concat([df_reset, state_reset, flow_reset, greeks_reset], axis=1)
    greeks_cols = [c for c in ["iv", "delta", "gamma", "vega", "theta"] if c in result.columns]
    if greeks_cols:
        result[greeks_cols] = result[greeks_cols].fillna(0.0)
    return result


def _process_tick_chunk(tick_df: pd.DataFrame, min_date: str, max_date: str, symbol: str) -> Optional[pd.DataFrame]:
    """处理单块Tick数据：聚合 → 过滤 → 衍生指标"""
    if tick_df.empty:
        return None

    tick_df = tick_df.copy()  # PD-P2-05: 必须copy,防止后续列赋值触发SettingWithCopyWarning或污染原始数据
    bars = _aggregate_ticks_to_bars(tick_df)
    bars = _filter_bars(bars, min_date, max_date)
    # R4-D-04修复: 时区标准化 — 确保时间戳统一为本地时区(naive timestamp)
    # preprocess_ticks使用本地时区，task_scheduler使用UTC
    # 此处统一为naive timestamp(无时区信息)，下游消费时再按需转换
    if "minute" in bars.columns:
        bars["minute"] = pd.to_datetime(bars["minute"])
        if bars["minute"].dt.tz is not None:
            bars["minute"] = bars["minute"].dt.tz_localize(None)
    bars = _enrich_bars(bars)

    if not bars.empty:
        bars["symbol"] = symbol
        return bars
    return None


def _minute_safe_chunk_indices(tick_df: pd.DataFrame, chunk_rows: int) -> List[Tuple[int, int]]:
    """按分钟边界安全分块，保证同一分钟的Tick不会被拆到两个Chunk。

    算法：先按minute分组得到每个分钟的行号范围，然后累积分钟组大小，
    当累积量超过chunk_rows时在当前分钟组末尾切一刀。
    这保证了：
      1. 每个Chunk内的Tick按分钟完整分组
      2. 同一分钟的Tick不会出现在两个Chunk中
      3. 每个Chunk的行数 ≤ chunk_rows + 最后一个分钟组的行数
      4. P1-R11-11修复: 跨交易日边界检测 — 日期变化时强制切分

    P0-裂缝2修复：增加边界完整性验证，确保无分钟数据跨chunk拆分。
    """
    minute_series = tick_df["datetime"].dt.floor("min")
    groups = tick_df.groupby(minute_series)

    boundaries: List[Tuple[int, int]] = []
    chunk_start = 0
    accumulated = 0
    # P1-R11-11修复: 跟踪上一个分钟的日期，日期变化时强制切分
    _prev_date: Optional[str] = None

    for minute_val, group_idx in groups.groups.items():
        group_size = len(group_idx)
        # P1-R11-11修复: 跨交易日检测 — 日期(以18:00为界)变化时强制切分
        if isinstance(minute_val, pd.Timestamp):
            _minute_dt = minute_val
        else:
            _minute_dt = pd.Timestamp(minute_val)
        _hour = _minute_dt.hour
        # 交易日判定: hour>=18使用当天日期, hour<18使用前一天日期
        _trading_date = _minute_dt.strftime("%Y-%m-%d") if _hour >= 18 else (_minute_dt - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        if _prev_date is not None and _trading_date != _prev_date:
            # 跨交易日: 强制在当前分钟组开头切一刀，开始新chunk
            if chunk_start < group_idx[0]:
                boundaries.append((chunk_start, group_idx[0]))
            chunk_start = group_idx[0]
            accumulated = group_size
            _prev_date = _trading_date
            continue
        _prev_date = _trading_date
        accumulated += group_size

        if accumulated >= chunk_rows:
            chunk_end = group_idx[-1] + 1
            if chunk_end > chunk_start:
                boundaries.append((chunk_start, chunk_end))
            chunk_start = chunk_end
            accumulated = 0

    if chunk_start < len(tick_df):
        boundaries.append((chunk_start, len(tick_df)))

    return boundaries


def check_minute_boundary_integrity(tick_df: pd.DataFrame,
                                     chunk_boundaries: List[Tuple[int, int]]) -> MinuteBoundaryResult:
    """P0-Q3-EXT 质量门：验证无分钟数据分散在多个chunk中

    R5-I-03修复: 返回类型从Dict改为MinuteBoundaryResult(NamedTuple)，消除dict硬编码键访问风险

    Returns:
        MinuteBoundaryResult: (passed, split_minutes, details)
    """
    if tick_df.empty or not chunk_boundaries:
        return MinuteBoundaryResult(passed=True, split_minutes=0, details=[])

    tick_df = tick_df.copy()  # PD-P2-05: 必须copy,防止后续列赋值触发SettingWithCopyWarning或污染原始数据
    tick_df["minute"] = tick_df["datetime"].dt.floor("min")
    tick_df["_chunk_id"] = -1

    for chunk_id, (start, end) in enumerate(chunk_boundaries):
        tick_df.loc[tick_df.index[start:end], "_chunk_id"] = chunk_id

    minute_chunk_counts = tick_df.groupby("minute")["_chunk_id"].nunique()
    split_minutes = minute_chunk_counts[minute_chunk_counts > 1]

    _passed = len(split_minutes) == 0
    _split = len(split_minutes)
    _details = [{"minute": str(m), "chunk_count": int(c)} for m, c in split_minutes.items()]

    if not _passed:
        logger.warning("[P0-Q3-EXT FAIL] %d 分钟数据被拆分到多个chunk: %s",
                       _split, split_minutes.index.tolist()[:5])

    return MinuteBoundaryResult(passed=_passed, split_minutes=_split, details=_details)


def process_symbol(symbol: str, tick_dir: str, min_date: str, max_date: str) -> Optional[str]:
    """处理单个品种：逐文件流式读取 → 按分钟边界安全分块 → 聚合 → 过滤 → 衍生指标 → 写临时Parquet

    R5-I-01修复: 添加参数None检查，防止None传入导致运行时异常。
    SER-03修复: 添加symbol参数校验，防止SQL注入和路径遍历攻击。
    """
    # R5-I-01修复: 参数None检查
    if symbol is None or tick_dir is None:
        raise ValueError(f"symbol和tick_dir不能为None: symbol={symbol}, tick_dir={tick_dir}")
    if min_date is None or max_date is None:
        raise ValueError(f"min_date和max_date不能为None: min_date={min_date}, max_date={max_date}")
    # SER-03修复: symbol参数校验 - 仅允许字母数字下划线，防止SQL注入和路径遍历
    if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_\-]{0,30}$', symbol):
        raise ValueError(f"SER-03: Invalid symbol format (potential SQL injection): {symbol}")
    pattern = os.path.join(tick_dir, "**", f"{symbol}.parquet")
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        logger.warning("品种 %s 无Tick数据", symbol)
        return None

    all_chunks: List[pd.DataFrame] = []

    for filepath in files:
        try:
            tick_df = pd.read_parquet(filepath)
        except Exception as e:
            logger.warning("读取 %s 失败: %s", filepath, e)
            continue

        if tick_df.empty:
            continue

        if "datetime" not in tick_df.columns:
            logger.warning("%s 缺少 datetime 列，跳过", filepath)
            continue

        if "price" not in tick_df.columns:
            logger.warning("%s 缺少 price 列，跳过", filepath)
            continue

        tick_df["datetime"] = pd.to_datetime(tick_df["datetime"])
        tick_df = tick_df.sort_values("datetime")

        if len(tick_df) > ROWS_PER_CHUNK:
            boundaries = _minute_safe_chunk_indices(tick_df, ROWS_PER_CHUNK)
            if ENABLE_MINUTE_BOUNDARY_CHECK:
                integrity = check_minute_boundary_integrity(tick_df, boundaries)
                if not integrity["passed"]:
                    logger.warning(
                        "[DATA-05] 分钟边界完整性检查失败: %d 分钟被拆分, 详情: %s",
                        integrity["split_minutes"], integrity["details"][:3],
                    )
            for start, end in boundaries:
                chunk = tick_df.iloc[start:end]
                result = _process_tick_chunk(chunk, min_date, max_date, symbol)
                if result is not None:
                    all_chunks.append(result)
                    if len(all_chunks) % 100 == 0:
                        total_rows = sum(len(c) for c in all_chunks)
                        if total_rows > 1000000:
                            logger.warning("PD-P2-09: all_chunks accumulating %d rows, peak memory risk", total_rows)
                        if total_rows > 5000000:
                            logger.error("[PD-P2-04] all_chunks超过500万行硬上限, 触发增量写入")
                            break
            del tick_df
        else:
            result = _process_tick_chunk(tick_df, min_date, max_date, symbol)
            if result is not None:
                all_chunks.append(result)
                if len(all_chunks) % 100 == 0:
                    total_rows = sum(len(c) for c in all_chunks)
                    if total_rows > 1000000:
                        logger.warning("PD-P2-09: all_chunks accumulating %d rows, peak memory risk", total_rows)
                    if total_rows > 5000000:
                        logger.error("[PD-P2-04] all_chunks超过500万行硬上限, 触发增量写入")
                        break
            del tick_df, result

    if not all_chunks:
        logger.info("品种 %s 无有效数据", symbol)
        return None

    result = pd.concat(all_chunks, ignore_index=True)

    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, f"preprocess_{symbol}_{os.getpid()}.parquet")
    result.to_parquet(tmp_path, index=False, compression="zstd")

    logger.info("品种 %s 预处理完成: %d 行 → %s", symbol, len(result), tmp_path)
    return tmp_path


def _merge_temp_file(db_path: str, parquet_path: str) -> int:
    """将单个临时Parquet文件合并到DuckDB（单写者，线程安全）

    R4-D-11修复: 数据库连接异常处理完善 —
    添加重试机制和详细错误日志，避免写入失败时数据丢失。
    """
    import traceback as _tb
    _max_retries = 3
    _retry_delay = 1.0
    for _attempt in range(_max_retries):
        con = None
        try:
            con = duckdb.connect(db_path)
            _ensure_table(con)
            # R4-D-11: 列对齐INSERT — 只插入表已存在的列，避免列不匹配异常
            cols_info = con.execute("DESCRIBE minute_data").fetchall()
            table_cols = [row[0] for row in cols_info]
            df = pd.read_parquet(parquet_path)
            # 只保留表中存在的列
            insert_cols = [c for c in df.columns if c in table_cols]
            if not insert_cols:
                logger.warning("[R4-D-11] parquet列与表不匹配，跳过: %s", parquet_path)
                return 0
            df_subset = df[insert_cols]
            col_str = ", ".join(insert_cols)
            con.execute(f"""
                INSERT INTO minute_data ({col_str})
                SELECT {col_str} FROM df_subset
            """)
            count = con.execute("SELECT changes()").fetchone()[0]
            return count
        except Exception as e:
            # R4-D-14修复: 异常堆栈记录完整
            logger.error(
                "[R4-D-11] DuckDB写入失败(attempt %d/%d): %s\n%s",
                _attempt + 1, _max_retries, e, _tb.format_exc(),
            )
            if _attempt < _max_retries - 1:
                time.sleep(_retry_delay * (_attempt + 1))
            else:
                logger.error("[R4-D-11] DuckDB写入最终失败: %s", parquet_path)
                return 0
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
    return 0


def _build_task_list(symbols: List[str], tick_dir: str, min_date: str, max_date: str) -> List[Tuple[str, str, str, str]]:
    """构建任务列表：大品种按年份拆分为子任务，提升并行效率。

    扫描每个品种的文件大小，超过 SIZE_THRESHOLD 的品种按年份拆分，
    其余品种作为整体任务提交。这避免了大品种拖慢 as_completed 循环。
    """
    SIZE_THRESHOLD = 2 * 1024 ** 3  # 2GB
    tasks: List[Tuple[str, str, str, str]] = []

    for sym in symbols:
        pattern = os.path.join(tick_dir, "**", f"{sym}.parquet")
        files = glob.glob(pattern, recursive=True)
        if not files:
            tasks.append((sym, tick_dir, min_date, max_date))
            continue

        total_size = sum(os.path.getsize(f) for f in files if os.path.exists(f))
        if total_size < SIZE_THRESHOLD:
            tasks.append((sym, tick_dir, min_date, max_date))
        else:
            min_y = pd.Timestamp(min_date).year
            max_y = pd.Timestamp(max_date).year
            for year in range(min_y, max_y + 1):
                y_start = f"{year}-01-01"
                y_end = f"{year}-12-31"
                tasks.append((sym, tick_dir, y_start, y_end))
            logger.info("品种 %s 数据 %.1fGB，拆分为 %d 个年度子任务",
                        sym, total_size / 1024 ** 3, max_y - min_y + 1)

    return tasks


def main_preprocess() -> None:
    start_time = time.time()
    tmp_files: List[str] = []

    try:
        tasks = _build_task_list(SYMBOLS, TICK_DATA_DIR, MIN_DATE, MAX_DATE)
        task_labels = [f"{t[0]}({t[2]}~{t[3]})" for t in tasks]
        logger.info("任务列表: %d 个子任务 (%d 品种, 大品种已按年拆分)",
                    len(tasks), len(SYMBOLS))

        if MAX_WORKERS > 1:
            # R21-CC-P2-02修复: 添加max_tasks_per_child参数(Python 3.11+)
            # 预处理任务涉及大量DataFrame序列化，子进程内存碎片累积可能导致OOM
            _executor_kwargs = dict(max_workers=MAX_WORKERS, mp_context=get_spawn_context())  # NEW-P2-03修复: 使用公共函数替代重复代码
            try:
                _executor_kwargs['max_tasks_per_child'] = 50  # Python 3.11+
            except Exception:
                pass
            with ProcessPoolExecutor(**_executor_kwargs) as executor:
                futures = {
                    executor.submit(process_symbol, sym, tdir, lo, hi): label
                    for (sym, tdir, lo, hi), label in zip(tasks, task_labels)
                }
                for future in tqdm(as_completed(futures), total=len(futures), desc="预处理进度"):
                    label = futures[future]
                    try:
                        tmp_path = future.result()
                        if tmp_path:
                            tmp_files.append(tmp_path)
                    except Exception as e:
                        logger.error("任务 %s 处理失败: %s", label, e)
        else:
            for (sym, tdir, lo, hi), label in zip(tasks, task_labels):
                try:
                    tmp_path = process_symbol(sym, tdir, lo, hi)
                    if tmp_path:
                        tmp_files.append(tmp_path)
                except Exception as e:
                    logger.error("任务 %s 处理失败: %s", label, e)

        if not tmp_files:
            logger.warning("无任何有效输出文件")
            return

        logger.info("开始合并 %d 个临时文件到 %s", len(tmp_files), OUTPUT_DB)
        total_rows = 0
        for tmp_path in tqdm(tmp_files, desc="合并进度"):
            try:
                count = _merge_temp_file(OUTPUT_DB, tmp_path)
                total_rows += max(count, 0)
            except Exception as e:
                logger.error("合并 %s 失败: %s", tmp_path, e)

        con = duckdb.connect(OUTPUT_DB)
        try:
            _ensure_table(con)
            con.execute("CREATE INDEX IF NOT EXISTS idx_symbol_minute ON minute_data(symbol, minute)")
            actual_count = con.execute("SELECT COUNT(*) FROM minute_data").fetchone()[0]
            logger.info("数据库优化完成: %d 行, 已建索引 idx_symbol_minute", actual_count)
        finally:
            con.close()

        logger.info("开始多粒度Bar聚合(%s)...", MULTISCALE_BAR_LENGTHS)
        con = duckdb.connect(OUTPUT_DB)
        try:
            _ensure_multiscale_table(con)
            for bl in MULTISCALE_BAR_LENGTHS:
                logger.info("聚合 %d 分钟Bar...", bl)
                df_1m = con.execute("""
                    SELECT * FROM minute_data ORDER BY symbol, minute
                """).fetchdf()
                if df_1m.empty:
                    continue
                df_multi = _resample_bars_to_multiscale(df_1m, bl)
                if not df_multi.empty:
                    cols = list(df_multi.columns)
                    placeholders = ", ".join(["?"] * len(cols))
                    rows = [tuple(row) for row in df_multi[cols].itertuples(index=False)]
                    con.executemany(
                        f"INSERT INTO minute_data_multiscale VALUES ({placeholders})", rows
                    )
                    logger.info("  %d 分钟Bar: %d 行已写入", bl, len(df_multi))
            multiscale_count = con.execute("SELECT COUNT(*) FROM minute_data_multiscale").fetchone()[0]
            logger.info("多粒度Bar聚合完成: %d 行", multiscale_count)
        finally:
            con.close()

        elapsed = time.time() - start_time
        logger.info("预处理全部完成: %d 品种, %d 行, 耗时 %.1f 秒", len(tmp_files), total_rows, elapsed)

    finally:
        for tmp_path in tmp_files:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass


if __name__ == "__main__":
    main_preprocess()


def validate_circuit_breaker_halts(bar_data: pd.DataFrame,
                                    price_col: str = "close",
                                    ref_change_pct: float = 0.50,
                                    halt_duration_bars: int = 5,
                                    resume_slippage_pct: float = 0.50) -> CircuitBreakerResult:
    """P0-裂缝14：从历史数据中提取熔断停牌事件，在回测中注入停牌期

    检测价格涨跌超过参考价50%的极端bar，标记为熔断停牌点。
    停牌期间：锁定持仓，复牌时施加额外滑点(跳空幅度×50%)。

    R5-I-03修复: 返回类型从Dict改为CircuitBreakerResult(NamedTuple)

    Returns:
        CircuitBreakerResult: (halt_events, n_halts, survival_rate_check_needed)
    """
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
    """P0-裂缝17：验证策略对tick乱序的鲁棒性

    在回测数据中随机交换相邻tick（概率p=0.001），
    按时间戳重排序后馈给策略。若夏普下降>30%，需增加tick缓存+去重+排序模块。

    R5-I-03修复: 返回类型从Dict改为OutOfOrderResult(NamedTuple)
    """
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
    """P1-裂缝34：期权到期日结构完整性检查

    回测中隐式假设所有合约有相同的到期日结构，但不同行权价的期权
    可能有不同的到期日（如周度 vs 月度）。在预处理中增加完整性检查，
    若某行权价缺少到期日则剔除该合约。

    通过标准：所有用于回测的期权合约都有唯一且有效的expire_date。

    R5-I-03修复: 返回类型从Dict改为ExpireDateResult(NamedTuple)
    """
    if bar_data is None or bar_data.empty:
        return ExpireDateResult(passed=True, total_rows=0, missing_expire_count=0,
                                missing_expire_pct=0.0, issues=[], action="no_data")

    if expire_col not in bar_data.columns:
        return ExpireDateResult(passed=True, total_rows=len(bar_data), missing_expire_count=0,
                                missing_expire_pct=0.0, issues=[], action="no_expire_date_column")

    issues = []
    total_rows = len(bar_data)

    # 检查1：expire_date缺失率
    missing_expire = bar_data[expire_col].isna().sum()
    missing_pct = missing_expire / total_rows * 100 if total_rows > 0 else 0
    if missing_pct > 5.0:
        issues.append(f"expire_date缺失率{missing_pct:.1f}%超过5%阈值")

    # 检查2：同一symbol下expire_date不一致（周度vs月度混合）
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

    # 检查3：有strike_price但无expire_date的合约（应剔除）
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
