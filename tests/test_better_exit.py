# MODULE_ID: M1-185
"""Better exit point module unit tests — 56 assertions.

Covers: module import, long/short cycle detection, better exit price logic,
42-column output, S6 risk trigger, end-of-data unresolved position,
_cycles_to_columns bar alignment, gain_pct computation, etc.
"""
import numpy as np
import pandas as pd
import pytest

from ali2026v3_trading.precompute._better_exit import (
    StrategyExitConfig,
    DEFAULT_STRATEGY_CONFIGS,
    TradeCycle,
    _simulate_strategy_cycles,
    _find_better_exit,
    _compute_gain_pct,
    _cycles_to_columns,
    compute_better_exit_vectorized,
    compute_better_exit_summary,
)


# ---- 1. Module import & config (1-8) ----

def test_module_import():
    import ali2026v3_trading.precompute._better_exit as mod
    assert mod is not None

def test_default_configs_has_seven_strategies():
    assert len(DEFAULT_STRATEGY_CONFIGS) == 7

def test_s1_config_thresholds():
    c = DEFAULT_STRATEGY_CONFIGS["s1"]
    assert c.long_entry_threshold == 0.3
    assert c.short_entry_threshold == -0.3

def test_s2_config_abs_exit():
    c = DEFAULT_STRATEGY_CONFIGS["s2"]
    assert c.exit_abs_threshold == 0.1

def test_s3_config_thresholds():
    c = DEFAULT_STRATEGY_CONFIGS["s3"]
    assert c.long_entry_threshold == 0.5
    assert c.exit_abs_threshold == 0.2

def test_s4_config_thresholds():
    c = DEFAULT_STRATEGY_CONFIGS["s4"]
    assert c.long_entry_threshold == 0.4

def test_s5_config_thresholds():
    c = DEFAULT_STRATEGY_CONFIGS["s5"]
    assert c.long_entry_threshold == 0.5

def test_s6_config_risk_trigger():
    c = DEFAULT_STRATEGY_CONFIGS["s6"]
    assert c.long_exit_threshold == 0.7

def test_s7_config_signal_field():
    c = DEFAULT_STRATEGY_CONFIGS["s7"]
    assert c.signal_field == "div_reversal_signal"


# ---- 2. Long cycle detection (9-16) ----

def test_long_entry_detected():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 102.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert len(cycles) >= 1
    assert cycles[0].direction == 1

def test_long_exit_by_reverse_cross():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert len(cycles) == 1
    assert cycles[0].entry_idx == 0
    assert cycles[0].exit_idx == 2

def test_long_better_exit_is_highest_high():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].better_exit_price == 103.0

def test_long_gain_pct_positive():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].gain_pct > 0

def test_long_gain_pct_value():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    expected = (103.0 - 99.0) / 99.0 * 100.0
    assert abs(cycles[0].gain_pct - expected) < 0.01

def test_long_hold_bars():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].hold_bars == 2

def test_long_entry_price():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].entry_price == 100.0

def test_long_exit_price():
    signal = np.array([0.5, 0.0, -0.2])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 103.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].exit_price == 99.0


# ---- 3. Short cycle detection (17-24) ----

def test_short_entry_detected():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert len(cycles) >= 1
    assert cycles[0].direction == -1

def test_short_exit_by_reverse_cross():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert len(cycles) == 1
    assert cycles[0].exit_idx == 2

def test_short_better_exit_is_lowest_low():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].better_exit_price == 97.0

def test_short_gain_pct_positive():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].gain_pct > 0

def test_short_gain_pct_value():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    expected = (101.0 - 97.0) / 101.0 * 100.0
    assert abs(cycles[0].gain_pct - expected) < 0.01

def test_short_hold_bars():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].hold_bars == 2

def test_short_entry_price():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].entry_price == 100.0

def test_short_exit_price():
    signal = np.array([-0.5, 0.0, 0.2])
    close = np.array([100.0, 99.0, 101.0])
    high = np.array([101.0, 100.0, 102.0])
    low = np.array([99.0, 97.0, 100.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].exit_price == 101.0


# ---- 4. _find_better_exit edge cases (25-30) ----

def test_find_better_exit_long():
    high = np.array([100.0, 101.0, 105.0, 102.0, 100.0])
    low = np.array([98.0, 99.0, 100.0, 99.0, 97.0])
    price, idx = _find_better_exit(high, low, 0, 4, 1)
    assert price == 105.0
    assert idx == 2

def test_find_better_exit_short():
    high = np.array([100.0, 101.0, 105.0, 102.0, 100.0])
    low = np.array([98.0, 99.0, 100.0, 99.0, 95.0])
    price, idx = _find_better_exit(high, low, 0, 4, -1)
    assert price == 95.0
    assert idx == 4

def test_find_better_exit_too_short():
    high = np.array([100.0, 101.0])
    low = np.array([99.0, 100.0])
    price, idx = _find_better_exit(high, low, 0, 1, 1)
    assert price == 0.0
    assert idx == -1

def test_find_better_exit_empty_segment():
    high = np.array([])
    low = np.array([])
    price, idx = _find_better_exit(high, low, 0, 0, 1)
    assert price == 0.0

def test_find_better_exit_single_bar():
    # exit_idx == entry_idx + 1: 区间太短，返回0.0
    high = np.array([100.0, 105.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    price, idx = _find_better_exit(high, low, 0, 1, 1)
    assert price == 0.0
    assert idx == -1

def test_find_better_exit_short_single_bar():
    # exit_idx == entry_idx + 1: 区间太短，返回0.0
    high = np.array([100.0, 101.0, 100.0])
    low = np.array([99.0, 95.0, 98.0])
    price, idx = _find_better_exit(high, low, 0, 1, -1)
    assert price == 0.0
    assert idx == -1


# ---- 5. _compute_gain_pct (31-36) ----

def test_gain_pct_long_positive():
    g = _compute_gain_pct(1, 100.0, 101.0, 105.0)
    assert g > 0

def test_gain_pct_long_zero_better():
    g = _compute_gain_pct(1, 100.0, 101.0, 0.0)
    assert g == 0.0

def test_gain_pct_short_positive():
    g = _compute_gain_pct(-1, 100.0, 99.0, 95.0)
    assert g > 0

def test_gain_pct_short_zero_better():
    g = _compute_gain_pct(-1, 100.0, 99.0, 0.0)
    assert g == 0.0

def test_gain_pct_long_value():
    g = _compute_gain_pct(1, 100.0, 100.0, 110.0)
    assert abs(g - 10.0) < 0.01

def test_gain_pct_short_value():
    g = _compute_gain_pct(-1, 100.0, 100.0, 90.0)
    assert abs(g - 10.0) < 0.01


# ---- 6. compute_better_exit_vectorized output (37-44) ----

def test_output_has_42_columns():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0, 100.0, 102.0],
        "high": [101.0, 103.0, 100.0, 101.0, 103.0],
        "low": [99.0, 100.0, 98.0, 99.0, 101.0],
        "signal_s1": [0.5, 0.0, -0.2, 0.0, 0.0],
        "signal_s2": [0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.0, 0.0, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0, 0.0, 0.0],
    })
    result = compute_better_exit_vectorized(df)
    assert len(result.columns) == 42

def test_output_column_names_s1():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0],
        "high": [101.0, 103.0, 100.0],
        "low": [99.0, 100.0, 98.0],
        "signal_s1": [0.5, 0.0, -0.2],
        "signal_s2": [0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0],
    })
    result = compute_better_exit_vectorized(df)
    assert "be_s1_entry_price" in result.columns
    assert "be_s1_direction" in result.columns
    assert "be_s1_strategy_exit" in result.columns
    assert "be_s1_hold_bars" in result.columns
    assert "be_s1_better_price" in result.columns
    assert "be_s1_gain_pct" in result.columns

def test_output_column_names_s7():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0],
        "high": [101.0, 103.0, 100.0],
        "low": [99.0, 100.0, 98.0],
        "signal_s1": [0.0, 0.0, 0.0],
        "signal_s2": [0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.0, 0.0],
        "div_reversal_signal": [0.5, 0.0, -0.2],
    })
    result = compute_better_exit_vectorized(df)
    assert "be_s7_entry_price" in result.columns
    assert "be_s7_gain_pct" in result.columns

def test_output_row_count_matches_input():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0, 100.0, 102.0, 101.0],
        "high": [101.0, 103.0, 100.0, 101.0, 103.0, 102.0],
        "low": [99.0, 100.0, 98.0, 99.0, 101.0, 100.0],
        "signal_s1": [0.5, 0.0, -0.2, 0.0, 0.0, 0.0],
        "signal_s2": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    })
    result = compute_better_exit_vectorized(df)
    assert len(result) == 6

def test_output_empty_df():
    df = pd.DataFrame()
    result = compute_better_exit_vectorized(df)
    assert len(result.columns) == 42

def test_s6_risk_trigger_recorded():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0, 100.0],
        "high": [101.0, 102.0, 100.0, 101.0],
        "low": [99.0, 100.0, 98.0, 99.0],
        "signal_s1": [0.5, 0.0, 0.0, 0.0],
        "signal_s2": [0.0, 0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.8, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0, 0.0],
    })
    result = compute_better_exit_vectorized(df)
    assert result["be_s6_direction"].iloc[1] == 1

def test_s6_risk_forces_exit():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0, 100.0],
        "high": [101.0, 102.0, 100.0, 101.0],
        "low": [99.0, 100.0, 98.0, 99.0],
        "signal_s1": [0.5, 0.0, 0.0, 0.0],
        "signal_s2": [0.0, 0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.8, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0, 0.0],
    })
    result = compute_better_exit_vectorized(df)
    assert result["be_s1_strategy_exit"].iloc[1] == 101.0


# ---- 7. _cycles_to_columns alignment (45-50) ----

def test_cycles_to_columns_entry_price():
    cycles = [TradeCycle("s1", 1, 0, 2, 100.0, 99.0, 103.0, 1, 2, 4.04)]
    cols = _cycles_to_columns(cycles, 5, "s1")
    assert cols["be_s1_entry_price"][0] == 100.0

def test_cycles_to_columns_direction():
    cycles = [TradeCycle("s1", 1, 0, 2, 100.0, 99.0, 103.0, 1, 2, 4.04)]
    cols = _cycles_to_columns(cycles, 5, "s1")
    assert cols["be_s1_direction"][0] == 1

def test_cycles_to_columns_strategy_exit():
    cycles = [TradeCycle("s1", 1, 0, 2, 100.0, 99.0, 103.0, 1, 2, 4.04)]
    cols = _cycles_to_columns(cycles, 5, "s1")
    assert cols["be_s1_strategy_exit"][2] == 99.0

def test_cycles_to_columns_hold_bars():
    cycles = [TradeCycle("s1", 1, 0, 2, 100.0, 99.0, 103.0, 1, 2, 4.04)]
    cols = _cycles_to_columns(cycles, 5, "s1")
    assert cols["be_s1_hold_bars"][2] == 2

def test_cycles_to_columns_better_price():
    cycles = [TradeCycle("s1", 1, 0, 2, 100.0, 99.0, 103.0, 1, 2, 4.04)]
    cols = _cycles_to_columns(cycles, 5, "s1")
    assert cols["be_s1_better_price"][1] == 103.0

def test_cycles_to_columns_gain_pct():
    cycles = [TradeCycle("s1", 1, 0, 2, 100.0, 99.0, 103.0, 1, 2, 4.04)]
    cols = _cycles_to_columns(cycles, 5, "s1")
    assert cols["be_s1_gain_pct"][1] == 4.04


# ---- 8. End-of-data unresolved & summary (51-56) ----

def test_unresolved_position_at_end():
    signal = np.array([0.5, 0.0, 0.0, 0.0, 0.0])
    close = np.array([100.0, 101.0, 102.0, 103.0, 104.0])
    high = np.array([101.0, 102.0, 103.0, 104.0, 105.0])
    low = np.array([99.0, 100.0, 101.0, 102.0, 103.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert len(cycles) == 1
    assert cycles[0].exit_idx == 4

def test_unresolved_uses_last_close():
    signal = np.array([0.5, 0.0, 0.0, 0.0, 0.0])
    close = np.array([100.0, 101.0, 102.0, 103.0, 104.0])
    high = np.array([101.0, 102.0, 103.0, 104.0, 105.0])
    low = np.array([99.0, 100.0, 101.0, 102.0, 103.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert cycles[0].exit_price == 104.0

def test_no_cycles_when_no_signal():
    signal = np.array([0.0, 0.0, 0.0])
    close = np.array([100.0, 101.0, 99.0])
    high = np.array([101.0, 102.0, 100.0])
    low = np.array([99.0, 100.0, 98.0])
    cfg = DEFAULT_STRATEGY_CONFIGS["s1"]
    cycles = _simulate_strategy_cycles(signal, high, low, close, cfg)
    assert len(cycles) == 0

def test_summary_returns_seven_keys():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0],
        "high": [101.0, 103.0, 100.0],
        "low": [99.0, 100.0, 98.0],
        "signal_s1": [0.5, 0.0, -0.2],
        "signal_s2": [0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0],
    })
    summary = compute_better_exit_summary(df)
    assert len(summary) == 7

def test_summary_has_cycle_count():
    df = pd.DataFrame({
        "close": [100.0, 101.0, 99.0],
        "high": [101.0, 103.0, 100.0],
        "low": [99.0, 100.0, 98.0],
        "signal_s1": [0.5, 0.0, -0.2],
        "signal_s2": [0.0, 0.0, 0.0],
        "signal_s3": [0.0, 0.0, 0.0],
        "signal_s4": [0.0, 0.0, 0.0],
        "signal_s5": [0.0, 0.0, 0.0],
        "signal_s6": [0.0, 0.0, 0.0],
        "div_reversal_signal": [0.0, 0.0, 0.0],
    })
    summary = compute_better_exit_summary(df)
    assert "total_cycles" in summary["s1"]
    assert summary["s1"]["total_cycles"] >= 1

def test_summary_empty_df():
    df = pd.DataFrame()
    summary = compute_better_exit_summary(df)
    assert len(summary) == 7
