# MODULE_ID: M1-184
"""Better exit point vectorized pre-computation module.

定义（用户原文）:
    同一策略开仓后到下次开仓前比策略平仓点更好（多/空时平仓价更高/低）的位置点（价格+日期）

算法:
    1. 基于 signal_s1 ~ signal_s5 + div_reversal_signal 模拟7个策略的开仓/平仓
    2. 对每个"开仓→平仓"区间，找出:
       - 多头: 区间内最高价及对应日期（better exit long）
       - 空头: 区间内最低价及对应日期（better exit short）
    3. 计算更好平仓点的提升幅度:
       - 多头: (better_price - strategy_exit_price) / strategy_exit_price
       - 空头: (strategy_exit_price - better_price) / strategy_exit_price
    4. 输出: 7个策略的 better_exit_price / better_exit_date / gain_pct / hold_bars

策略开仓/平仓规则（基于预计算信号）:
    S1 (HFT/趋势共振):    signal_s1 > 0.3 多头; < -0.3 空头; 反向穿越 ±0.1 平仓
    S2 (Spring/弹簧):     signal_s2 > 0.4 多头; < -0.4 空头; |signal_s2| < 0.1 平仓
    S3 (Box/箱体边界):    signal_s3 > 0.5 多头; < -0.5 空头; |signal_s3| < 0.2 平仓
    S4 (HF Momentum):     signal_s4 > 0.4 多头; < -0.4 空头; 反向穿越 ±0.1 平仓
    S5 (Cross Period):    signal_s5 > 0.5 多头; < -0.5 空头; < 0.2 平仓
    S6 (Risk Circuit):    signal_s6 > 0.7 强制平仓（风控）
    S7 (Divergence):      div_reversal_signal > 0.3 多头; < -0.3 空头; 反向 ±0.1 平仓

输出列（写入 minute_data 表）:
    be_s1_long_price / be_s1_long_date / be_s1_long_gain_pct
    be_s1_short_price / be_s1_short_date / be_s1_short_gain_pct
    be_s1_strategy_exit / be_s1_hold_bars
    ... 同上 S2-S7
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from infra._helpers import get_logger

logger = get_logger(__name__)


# ---- 策略参数定义 ----
@dataclass
class StrategyExitConfig:
    """单个策略的开仓/平仓参数"""
    name: str
    signal_field: str            # 信号字段名
    long_entry_threshold: float  # 多头开仓阈值
    short_entry_threshold: float # 空头开仓阈值
    long_exit_threshold: float   # 多头平仓阈值（信号低于此值）
    short_exit_threshold: float  # 空头平仓阈值（信号高于此值）
    exit_abs_threshold: float = 0.0  # 绝对值平仓阈值（若>0则使用abs判断）
    enabled: bool = True


# 默认7策略配置（参数来源：策略生产模块+V7手册）
DEFAULT_STRATEGY_CONFIGS: Dict[str, StrategyExitConfig] = {
    "s1": StrategyExitConfig(
        name="s1_hft",
        signal_field="signal_s1",
        long_entry_threshold=0.3,
        short_entry_threshold=-0.3,
        long_exit_threshold=-0.1,   # 信号反向穿越-0.1则平多头
        short_exit_threshold=0.1,   # 信号反向穿越0.1则平空头
    ),
    "s2": StrategyExitConfig(
        name="s2_resonance",
        signal_field="signal_s2",
        long_entry_threshold=0.4,
        short_entry_threshold=-0.4,
        long_exit_threshold=0.1,
        short_exit_threshold=-0.1,
        exit_abs_threshold=0.1,     # |signal| < 0.1 平仓
    ),
    "s3": StrategyExitConfig(
        name="s3_box",
        signal_field="signal_s3",
        long_entry_threshold=0.5,
        short_entry_threshold=-0.5,
        long_exit_threshold=0.2,
        short_exit_threshold=-0.2,
        exit_abs_threshold=0.2,
    ),
    "s4": StrategyExitConfig(
        name="s4_spring",
        signal_field="signal_s4",
        long_entry_threshold=0.4,
        short_entry_threshold=-0.4,
        long_exit_threshold=-0.1,
        short_exit_threshold=0.1,
    ),
    "s5": StrategyExitConfig(
        name="s5_cross_period",
        signal_field="signal_s5",
        long_entry_threshold=0.5,
        short_entry_threshold=-0.5,
        long_exit_threshold=0.2,
        short_exit_threshold=-0.2,
        exit_abs_threshold=0.2,
    ),
    "s6": StrategyExitConfig(
        name="s6_risk_circuit",
        signal_field="signal_s6",
        long_entry_threshold=0.0,   # S6 不直接开仓，仅作风控平仓
        short_entry_threshold=0.0,
        long_exit_threshold=0.7,    # signal_s6 > 0.7 强制平仓
        short_exit_threshold=0.7,
        enabled=True,               # S6作为风控触发器，需与其他策略叠加
    ),
    "s7": StrategyExitConfig(
        name="s7_divergence",
        signal_field="div_reversal_signal",
        long_entry_threshold=0.3,
        short_entry_threshold=-0.3,
        long_exit_threshold=-0.1,
        short_exit_threshold=0.1,
    ),
}


@dataclass
class TradeCycle:
    """单次开仓→平仓周期"""
    strategy: str
    direction: int             # +1 多头, -1 空头
    entry_idx: int             # 开仓索引
    exit_idx: int              # 平仓索引
    entry_price: float
    exit_price: float
    better_exit_price: float   # 更好平仓价
    better_exit_idx: int       # 更好平仓索引
    hold_bars: int
    gain_pct: float            # 更好平仓点相对策略平仓点的提升幅度（百分比）


def _simulate_strategy_cycles(
    signal: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    config: StrategyExitConfig,
    risk_signal: Optional[np.ndarray] = None,
) -> List[TradeCycle]:
    """模拟单个策略的开仓/平仓周期。

    Args:
        signal: 策略信号数组
        high/low/close: K线价格数组
        config: 策略参数
        risk_signal: 可选的风控信号（signal_s6），>0.7强制平仓

    Returns:
        List[TradeCycle]: 所有完整交易周期
    """
    n = len(signal)
    if n == 0:
        return []

    cycles: List[TradeCycle] = []
    position = 0          # 当前持仓方向：0空仓, +1多头, -1空头
    entry_idx = -1
    entry_price = 0.0

    for i in range(n):
        sig = signal[i]
        # 风控强制平仓（S6 > 0.7）
        if risk_signal is not None and i < len(risk_signal) and risk_signal[i] > 0.7 and position != 0:
            exit_idx = i
            exit_price = close[i]
            better_exit_price, better_exit_idx = _find_better_exit(
                high, low, entry_idx, exit_idx, position
            )
            hold_bars = exit_idx - entry_idx
            gain_pct = _compute_gain_pct(
                position, entry_price, exit_price, better_exit_price
            )
            cycles.append(TradeCycle(
                strategy=config.name,
                direction=position,
                entry_idx=entry_idx,
                exit_idx=exit_idx,
                entry_price=entry_price,
                exit_price=exit_price,
                better_exit_price=better_exit_price,
                better_exit_idx=better_exit_idx,
                hold_bars=hold_bars,
                gain_pct=gain_pct,
            ))
            position = 0
            entry_idx = -1
            entry_price = 0.0
            continue

        if position == 0:
            # 空仓：检查开仓信号
            if sig > config.long_entry_threshold:
                position = 1
                entry_idx = i
                entry_price = close[i]
            elif sig < config.short_entry_threshold:
                position = -1
                entry_idx = i
                entry_price = close[i]
        elif position == 1:
            # 多头持仓：检查平仓信号
            should_exit = False
            if config.exit_abs_threshold > 0:
                should_exit = abs(sig) < config.exit_abs_threshold
            else:
                should_exit = sig < config.long_exit_threshold
            if should_exit:
                exit_idx = i
                exit_price = close[i]
                better_exit_price, better_exit_idx = _find_better_exit(
                    high, low, entry_idx, exit_idx, position
                )
                hold_bars = exit_idx - entry_idx
                gain_pct = _compute_gain_pct(
                    position, entry_price, exit_price, better_exit_price
                )
                cycles.append(TradeCycle(
                    strategy=config.name,
                    direction=position,
                    entry_idx=entry_idx,
                    exit_idx=exit_idx,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    better_exit_price=better_exit_price,
                    better_exit_idx=better_exit_idx,
                    hold_bars=hold_bars,
                    gain_pct=gain_pct,
                ))
                position = 0
                entry_idx = -1
                entry_price = 0.0
        elif position == -1:
            # 空头持仓：检查平仓信号
            should_exit = False
            if config.exit_abs_threshold > 0:
                should_exit = abs(sig) < config.exit_abs_threshold
            else:
                should_exit = sig > config.short_exit_threshold
            if should_exit:
                exit_idx = i
                exit_price = close[i]
                better_exit_price, better_exit_idx = _find_better_exit(
                    high, low, entry_idx, exit_idx, position
                )
                hold_bars = exit_idx - entry_idx
                gain_pct = _compute_gain_pct(
                    position, entry_price, exit_price, better_exit_price
                )
                cycles.append(TradeCycle(
                    strategy=config.name,
                    direction=position,
                    entry_idx=entry_idx,
                    exit_idx=exit_idx,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    better_exit_price=better_exit_price,
                    better_exit_idx=better_exit_idx,
                    hold_bars=hold_bars,
                    gain_pct=gain_pct,
                ))
                position = 0
                entry_idx = -1
                entry_price = 0.0

    # 处理末尾未平仓的持仓（使用最后一根K线的close作为平仓价）
    if position != 0 and entry_idx >= 0:
        exit_idx = n - 1
        exit_price = close[exit_idx]
        better_exit_price, better_exit_idx = _find_better_exit(
            high, low, entry_idx, exit_idx, position
        )
        hold_bars = exit_idx - entry_idx
        gain_pct = _compute_gain_pct(
            position, entry_price, exit_price, better_exit_price
        )
        cycles.append(TradeCycle(
            strategy=config.name,
            direction=position,
            entry_idx=entry_idx,
            exit_idx=exit_idx,
            entry_price=entry_price,
            exit_price=exit_price,
            better_exit_price=better_exit_price,
            better_exit_idx=better_exit_idx,
            hold_bars=hold_bars,
            gain_pct=gain_pct,
        ))

    return cycles


def _find_better_exit(
    high: np.ndarray,
    low: np.ndarray,
    entry_idx: int,
    exit_idx: int,
    direction: int,
) -> Tuple[float, int]:
    """在开仓→平仓区间内寻找更好平仓点。

    多头: 找区间内最高价（不含开仓点，含平仓点）
    空头: 找区间内最低价

    Returns:
        (better_exit_price, better_exit_idx)
    """
    if exit_idx <= entry_idx + 1:
        # 区间太短，没有更好平仓点
        return 0.0, -1

    # 区间：[entry_idx+1, exit_idx]，包含平仓点本身
    start = entry_idx + 1
    end = exit_idx + 1

    if direction == 1:
        # 多头：找最高价
        segment = high[start:end]
        if len(segment) == 0:
            return 0.0, -1
        better_local_idx = int(np.argmax(segment))
        better_idx = start + better_local_idx
        better_price = float(segment[better_local_idx])
    else:
        # 空头：找最低价
        segment = low[start:end]
        if len(segment) == 0:
            return 0.0, -1
        better_local_idx = int(np.argmin(segment))
        better_idx = start + better_local_idx
        better_price = float(segment[better_local_idx])

    return better_price, better_idx


def _compute_gain_pct(
    direction: int,
    entry_price: float,
    exit_price: float,
    better_exit_price: float,
) -> float:
    """计算更好平仓点相对策略平仓点的提升幅度（百分比）。

    多头: (better_price - strategy_exit) / strategy_exit * 100
    空头: (strategy_exit - better_price) / strategy_exit * 100

    若 better_exit_price 为 0（区间太短），返回 0.0
    """
    if better_exit_price <= 0 or exit_price <= 0:
        return 0.0
    if direction == 1:
        # 多头：更好平仓价应高于策略平仓价
        return (better_exit_price - exit_price) / exit_price * 100.0
    else:
        # 空头：更好平仓价应低于策略平仓价
        return (exit_price - better_exit_price) / exit_price * 100.0


def _cycles_to_columns(
    cycles: List[TradeCycle],
    n: int,
    strategy_key: str,
) -> Dict[str, np.ndarray]:
    """将交易周期转换为按bar对齐的列数据。

    每根K线会标记：
    - 若该bar是某个周期的开仓点: be_<s>_entry_price / be_<s>_direction
    - 若该bar是某个周期的策略平仓点: be_<s>_strategy_exit / be_<s>_hold_bars
    - 若该bar是某个周期的更好平仓点: be_<s>_better_price / be_<s>_gain_pct
    """
    prefix = f"be_{strategy_key}"
    cols = {
        f"{prefix}_entry_price": np.zeros(n, dtype=np.float64),
        f"{prefix}_direction": np.zeros(n, dtype=np.int32),
        f"{prefix}_strategy_exit": np.zeros(n, dtype=np.float64),
        f"{prefix}_hold_bars": np.zeros(n, dtype=np.int32),
        f"{prefix}_better_price": np.zeros(n, dtype=np.float64),
        f"{prefix}_gain_pct": np.zeros(n, dtype=np.float64),
    }

    for cycle in cycles:
        if cycle.entry_idx < n:
            cols[f"{prefix}_entry_price"][cycle.entry_idx] = cycle.entry_price
            cols[f"{prefix}_direction"][cycle.entry_idx] = cycle.direction
        if cycle.exit_idx < n:
            cols[f"{prefix}_strategy_exit"][cycle.exit_idx] = cycle.exit_price
            cols[f"{prefix}_hold_bars"][cycle.exit_idx] = cycle.hold_bars
        if 0 <= cycle.better_exit_idx < n:
            cols[f"{prefix}_better_price"][cycle.better_exit_idx] = cycle.better_exit_price
            cols[f"{prefix}_gain_pct"][cycle.better_exit_idx] = cycle.gain_pct

    return cols


def compute_better_exit_vectorized(
    df: pd.DataFrame,
    configs: Optional[Dict[str, StrategyExitConfig]] = None,
) -> pd.DataFrame:
    """计算所有7个策略的更好平仓点。

    Args:
        df: 输入DataFrame，需包含 close/high/low/signal_s1~s5/div_reversal_signal
        configs: 策略配置，若为None则使用DEFAULT_STRATEGY_CONFIGS

    Returns:
        DataFrame: 7策略 × 6列 = 42列的更好平仓点数据
    """
    if configs is None:
        configs = DEFAULT_STRATEGY_CONFIGS

    n = len(df)
    if n == 0:
        # 返回空DataFrame，列名与下方一致
        empty_cols = []
        for key in configs.keys():
            prefix = f"be_{key}"
            empty_cols.extend([
                f"{prefix}_entry_price",
                f"{prefix}_direction",
                f"{prefix}_strategy_exit",
                f"{prefix}_hold_bars",
                f"{prefix}_better_price",
                f"{prefix}_gain_pct",
            ])
        return pd.DataFrame(np.zeros((0, len(empty_cols))), columns=empty_cols)

    # 获取价格数组
    close = df["close"].to_numpy(dtype=np.float64)
    high = (
        df["high"].to_numpy(dtype=np.float64)
        if "high" in df.columns
        else close
    )
    low = (
        df["low"].to_numpy(dtype=np.float64)
        if "low" in df.columns
        else close
    )

    # 获取S6风控信号
    risk_signal = None
    if "signal_s6" in df.columns:
        risk_signal = df["signal_s6"].to_numpy(dtype=np.float64)

    all_columns: Dict[str, np.ndarray] = {}
    cycle_stats: Dict[str, Dict[str, int]] = {}

    for key, config in configs.items():
        if not config.enabled:
            # 禁用的策略填充0
            prefix = f"be_{key}"
            for col_name in [
                f"{prefix}_entry_price",
                f"{prefix}_direction",
                f"{prefix}_strategy_exit",
                f"{prefix}_hold_bars",
                f"{prefix}_better_price",
                f"{prefix}_gain_pct",
            ]:
                if "direction" in col_name or "hold_bars" in col_name:
                    all_columns[col_name] = np.zeros(n, dtype=np.int32)
                else:
                    all_columns[col_name] = np.zeros(n, dtype=np.float64)
            continue

        # 获取信号
        if config.signal_field not in df.columns:
            logger.warning(
                "compute_better_exit: signal field %s not in df, skipping %s",
                config.signal_field, key,
            )
            prefix = f"be_{key}"
            for col_name in [
                f"{prefix}_entry_price",
                f"{prefix}_direction",
                f"{prefix}_strategy_exit",
                f"{prefix}_hold_bars",
                f"{prefix}_better_price",
                f"{prefix}_gain_pct",
            ]:
                if "direction" in col_name or "hold_bars" in col_name:
                    all_columns[col_name] = np.zeros(n, dtype=np.int32)
                else:
                    all_columns[col_name] = np.zeros(n, dtype=np.float64)
            continue

        signal = df[config.signal_field].to_numpy(dtype=np.float64)
        signal = np.nan_to_num(signal, nan=0.0)

        # S6仅作风控触发器，不单独开仓
        if key == "s6":
            # S6的"更好平仓点"概念不适用，仅标记风控触发点
            prefix = "be_s6"
            risk_trigger = (risk_signal > 0.7).astype(np.int32) if risk_signal is not None else np.zeros(n, dtype=np.int32)
            all_columns[f"{prefix}_entry_price"] = np.zeros(n, dtype=np.float64)
            all_columns[f"{prefix}_direction"] = risk_trigger  # 用direction列记录风控触发
            all_columns[f"{prefix}_strategy_exit"] = np.zeros(n, dtype=np.float64)
            all_columns[f"{prefix}_hold_bars"] = np.zeros(n, dtype=np.int32)
            all_columns[f"{prefix}_better_price"] = np.zeros(n, dtype=np.float64)
            all_columns[f"{prefix}_gain_pct"] = np.zeros(n, dtype=np.float64)
            cycle_stats[key] = {"total_cycles": 0, "trigger_bars": int(np.sum(risk_trigger))}
            continue

        # 模拟交易周期
        cycles = _simulate_strategy_cycles(
            signal=signal,
            high=high,
            low=low,
            close=close,
            config=config,
            risk_signal=risk_signal,
        )

        # 转换为按bar对齐的列
        cols = _cycles_to_columns(cycles, n, key)
        all_columns.update(cols)

        # 统计
        long_cycles = [c for c in cycles if c.direction == 1]
        short_cycles = [c for c in cycles if c.direction == -1]
        avg_gain = float(np.mean([c.gain_pct for c in cycles])) if cycles else 0.0
        cycle_stats[key] = {
            "total_cycles": len(cycles),
            "long_cycles": len(long_cycles),
            "short_cycles": len(short_cycles),
            "avg_gain_pct": avg_gain,
        }

    if cycle_stats:
        logger.info("compute_better_exit: %s", cycle_stats)

    return pd.DataFrame(all_columns)


def compute_better_exit_summary(
    df: pd.DataFrame,
    configs: Optional[Dict[str, StrategyExitConfig]] = None,
) -> Dict[str, Dict]:
    """计算更好平仓点的汇总统计信息（供七对齐审计使用）。

    Returns:
        Dict: 每个策略的汇总统计 {
            "s1": {"total_cycles": N, "long_cycles": N, "short_cycles": N,
                   "avg_gain_pct": float, "max_gain_pct": float, ...},
            ...
        }
    """
    if configs is None:
        configs = DEFAULT_STRATEGY_CONFIGS

    n = len(df)
    if n == 0:
        return {key: {} for key in configs}

    close = df["close"].to_numpy(dtype=np.float64)
    high = (
        df["high"].to_numpy(dtype=np.float64)
        if "high" in df.columns
        else close
    )
    low = (
        df["low"].to_numpy(dtype=np.float64)
        if "low" in df.columns
        else close
    )
    risk_signal = (
        df["signal_s6"].to_numpy(dtype=np.float64)
        if "signal_s6" in df.columns
        else None
    )

    summary: Dict[str, Dict] = {}
    for key, config in configs.items():
        if not config.enabled or key == "s6" or config.signal_field not in df.columns:
            summary[key] = {
                "total_cycles": 0,
                "long_cycles": 0,
                "short_cycles": 0,
                "avg_gain_pct": 0.0,
                "max_gain_pct": 0.0,
                "avg_hold_bars": 0,
                "better_exit_hit_rate": 0.0,
            }
            continue

        signal = df[config.signal_field].to_numpy(dtype=np.float64)
        signal = np.nan_to_num(signal, nan=0.0)

        cycles = _simulate_strategy_cycles(
            signal=signal,
            high=high,
            low=low,
            close=close,
            config=config,
            risk_signal=risk_signal,
        )

        if not cycles:
            summary[key] = {
                "total_cycles": 0,
                "long_cycles": 0,
                "short_cycles": 0,
                "avg_gain_pct": 0.0,
                "max_gain_pct": 0.0,
                "avg_hold_bars": 0,
                "better_exit_hit_rate": 0.0,
            }
            continue

        gains = [c.gain_pct for c in cycles]
        holds = [c.hold_bars for c in cycles]
        positive_gains = sum(1 for g in gains if g > 0)
        long_cycles = sum(1 for c in cycles if c.direction == 1)
        short_cycles = sum(1 for c in cycles if c.direction == -1)

        summary[key] = {
            "total_cycles": len(cycles),
            "long_cycles": long_cycles,
            "short_cycles": short_cycles,
            "avg_gain_pct": float(np.mean(gains)),
            "max_gain_pct": float(np.max(gains)),
            "min_gain_pct": float(np.min(gains)),
            "avg_hold_bars": float(np.mean(holds)),
            "better_exit_hit_rate": positive_gains / len(cycles),
            "long_entry_threshold": config.long_entry_threshold,
            "short_entry_threshold": config.short_entry_threshold,
            "signal_field": config.signal_field,
        }

    return summary
