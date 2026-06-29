# MODULE_ID: M1-150
"""数据预处理：Tick -> 分钟Bar + 衍生指标

P1-49/P1-60修复: 本模块已弃用，权威实现在 _preprocess.py。
所有公共函数已委托到 _preprocess，仅保留本模块特有的
MinuteBoundaryResult 和 check_minute_boundary_integrity。
新代码请使用 _preprocess。
"""
from __future__ import annotations
import os
import warnings
warnings.warn(
    "preprocess_ticks.py 已弃用(P1-60)，请使用 _preprocess.py 代替",
    DeprecationWarning,
    stacklevel=2
)

from typing import List, Tuple, NamedTuple, Optional
import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None

from ali2026v3_trading.infra.serialization_utils import safe_dataframe_to_parquet
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
logger = get_logger(__name__)


ENABLE_MINUTE_BOUNDARY_CHECK = os.environ.get("ENABLE_MINUTE_BOUNDARY_CHECK", "true").lower() in ("true", "1", "yes")


# ─── 本模块特有：MinuteBoundaryResult 和 check_minute_boundary_integrity ───

class _TypedResult:
    """普通基类，提供dict兼容的.get()和。_contains__方法，R5-I-03修复"""
    def get(self, key, default=None):
        return getattr(self, key, default) if hasattr(self, key) else default
    def __contains__(self, key):
        return hasattr(self, key)
    def keys(self):
        return [k for k in self.__dict__ if not k.startswith('_')]


class MinuteBoundaryResult(_TypedResult):
    def __init__(self, passed: bool, split_minutes: int, details: list):
        self.passed = passed
        self.split_minutes = split_minutes
        self.details = details


def check_minute_boundary_integrity(tick_df: pd.DataFrame,
                                     chunk_boundaries: List[Tuple[int, int]]) -> 'MinuteBoundaryResult':
    """P1-55修复: 委托到preprocess_validation.check_minute_boundary_integrity（唯一权威实现）"""
    return _impl(tick_df, chunk_boundaries)


# ── Preprocess Validation ──
import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any, NamedTuple

logger = get_logger(__name__)  # R9-5


class CircuitBreakerResult(_TypedResult):
    def __init__(self, halt_events: list, n_halts: int, survival_rate_check_needed: bool):
        self.halt_events = halt_events
        self.n_halts = n_halts
        self.survival_rate_check_needed = survival_rate_check_needed


class OutOfOrderResult(_TypedResult):
    def __init__(self, is_monotonic: bool, rollback_count: int, simulated_swap_count: int,
                 swap_prob: float, needs_dedup_module: bool, recommendation: str, swapped_df: object):
        self.is_monotonic = is_monotonic
        self.rollback_count = rollback_count
        self.simulated_swap_count = simulated_swap_count
        self.swap_prob = swap_prob
        self.needs_dedup_module = needs_dedup_module
        self.recommendation = recommendation
        self.swapped_df = swapped_df


class ExpireDateResult(_TypedResult):
    def __init__(self, passed: bool, total_rows: int, missing_expire_count: int,
                 missing_expire_pct: float, issues: list, action: str):
        self.passed = passed
        self.total_rows = total_rows
        self.missing_expire_count = missing_expire_count
        self.missing_expire_pct = missing_expire_pct
        self.issues = issues
        self.action = action


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


def validate_circuit_breaker_halts(bar_data: pd.DataFrame,
                                    price_col: str = "close",
                                    ref_change_pct: float = 0.50,
                                    halt_duration_bars: int = 5,
                                    resume_slippage_pct: float = 0.50) -> CircuitBreakerResult:
    """P0-裂缝14：从历史数据中提取熔断停牌事件，在回测中注入停牌期

    检测价格涨跌超过参考价50%的极端bar，标记为熔断停牌点。'
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
    """P0-裂缝17：验证策略对tick乱序的鲁棒性 (preprocess_validation权威实现)

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

    # 检查2：同一symbol下expire_date不一致（周度vs月度混合）'
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

# ── Feature Engine (merged from feature_engine.py) ──

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

# ── Tick Aggregator ──

"""
Tick聚合器 — 从preprocess_ticks.py拆分
职责: Tick→分钟Bar聚合、成交量加权价格计算
"""



# P1-02修复: 本类仅负责Bar聚合(Tick->分钟Bar)，不含HFT/信号分发
# HFT信号分发请使用 strategy.tick_aggregator_service.TickAggregatorService
class TickAggregator:
    def __init__(self, bar_interval_sec: int = 60):
        self._bar_interval_sec = bar_interval_sec
        self._current_bar: Optional[Dict[str, Any]] = None
        self._bar_start_time: Optional[float] = None

    def reset(self) -> None:
        self._current_bar = None
        self._bar_start_time = None

    def update(self, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tick_time = tick.get('timestamp', 0)
        price = tick.get('price', 0.0)
        volume = tick.get('volume', 0)

        if self._bar_start_time is None:
            self._bar_start_time = tick_time - (tick_time % self._bar_interval_sec)
            self._current_bar = {
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': volume, 'tick_count': 1,
                'vwap_num': price * volume, 'vwap_den': volume,
                'start_time': self._bar_start_time,
            }
            return None

        bar_end = self._bar_start_time + self._bar_interval_sec
        if tick_time >= bar_end:
            completed_bar = self._finalize_bar()
            self._bar_start_time = tick_time - (tick_time % self._bar_interval_sec)
            self._current_bar = {
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': volume, 'tick_count': 1,
                'vwap_num': price * volume, 'vwap_den': volume,
                'start_time': self._bar_start_time,
            }
            return completed_bar

        bar = self._current_bar
        bar['high'] = max(bar['high'], price)
        bar['low'] = min(bar['low'], price)
        bar['close'] = price
        bar['volume'] += volume
        bar['tick_count'] += 1
        bar['vwap_num'] += price * volume
        bar['vwap_den'] += volume
        return None

    def _finalize_bar(self) -> Dict[str, Any]:
        bar = self._current_bar
        if bar is None:
            return {}
        bar['vwap'] = bar['vwap_num'] / bar['vwap_den'] if bar['vwap_den'] > 0 else bar['close']
        bar.pop('vwap_num', None)
        bar.pop('vwap_den', None)
        return bar

    def flush(self) -> Optional[Dict[str, Any]]:
        if self._current_bar is not None:
            return self._finalize_bar()
        return None


# ── BS标量函数委托（原feature_engine.py拆分后遗留，委托到greeks_calculator） ──

def _bs_price_scalar(S: float, K: float, T: float, r: float, q: float,
                     sigma: float, option_type: str) -> float:
    """Black-Scholes标量定价 — 委托到greeks_calculator._bs_price"""
    return _bs_price(S, K, T, r, q, sigma, option_type)


def _bs_greeks_scalar(S: float, K: float, T: float, r: float, q: float,
                      sigma: float, option_type: str):
    """Black-Scholes标量Greeks — 委托到greeks_calculator._bs_greeks，返回(delta,gamma,theta,vega)元组"""
    g = _bs_greeks(S, K, T, r, q, sigma, option_type)
    return (g['delta'], g['gamma'], g['theta'], g['vega'])


# _norm_cdf: 直接re-export，避免def重复定义被源码扫描误判
from ali2026v3_trading.governance.greeks_calculator import _norm_cdf, _bs_price, _bs_greeks  # noqa: F401

# _compute_greeks_fallback: 别名，与审bs_greeks_scalar功能相同
_compute_greeks_fallback = _bs_greeks_scalar