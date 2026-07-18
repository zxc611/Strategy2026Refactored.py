# [M1-100] 回测安全检查器
# MODULE_ID: M1-158
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
from __future__ import annotations

import logging
import math
from infra._helpers import get_logger  # R9-5
from typing import Dict, List, Optional

import pandas as pd

from param_pool._param_grids import (
    PARAM_DEFAULTS, PARAM_GRID_ROUND1, PARAM_GRID_HFT,
    PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B,
    PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B,
)

from param_pool._param_grids import (
    _BacktestState,
    _ClosedTrade,
)
from param_pool.backtest.backtest_state import (
    _get_contract_multiplier,
    _compute_dynamic_slippage_bps,
    _compute_commission,
    _safe_equity_add,
    _infer_instrument_type,
    _infer_exchange_id,
    _infer_exchange_from_id,
    _calculate_limit_prices,
)
from infra.shared_utils import safe_price_check  # noqa: F401

from param_pool._param_grids import (
    PARAM_DEFAULTS, PARAM_GRID_ROUND1, PARAM_GRID_HFT,
    PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B,
    PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B,
)

from param_pool._param_grids import (
    _BacktestState,
    _ClosedTrade,
)
from param_pool.backtest.backtest_state import (
    _get_contract_multiplier,
    _compute_dynamic_slippage_bps,
    _compute_commission,
    _safe_equity_add,
    _infer_instrument_type,
    _infer_exchange_id,
    _infer_exchange_from_id,
    _calculate_limit_prices,
)

logger = get_logger(__name__)  # R9-5


def validate_default_values_in_grids() -> List[str]:
    """验收标准6: 每个扫描参数的默认值必须存在于对应grid列表中。"""
    from param_pool._param_grids import (
        PARAM_GRID_ROUND2, PARAM_GRID_BOX_EXTREME, PARAM_DEFAULTS_BOX_EXTREME,
        PARAM_DEFAULTS_BOX_SPRING, PARAM_GRID_BOX_SPRING,
        PARAM_GRID_ARBITRAGE, PARAM_DEFAULTS_ARBITRAGE,
        PARAM_GRID_MARKET_MAKING, PARAM_DEFAULTS_MARKET_MAKING,
    )

    mapping = [
        ("PARAM_GRID_ROUND1", PARAM_GRID_ROUND1, PARAM_DEFAULTS),
        ("PARAM_GRID_ROUND2", PARAM_GRID_ROUND2, PARAM_DEFAULTS),
        ("PARAM_GRID_BOX_EXTREME", PARAM_GRID_BOX_EXTREME, PARAM_DEFAULTS_BOX_EXTREME),
        ("PARAM_GRID_BOX_SPRING", PARAM_GRID_BOX_SPRING, PARAM_DEFAULTS_BOX_SPRING),
        ("PARAM_GRID_HFT", PARAM_GRID_HFT, PARAM_DEFAULTS_HFT),
        ("PARAM_GRID_ARBITRAGE", PARAM_GRID_ARBITRAGE, PARAM_DEFAULTS_ARBITRAGE),
        ("PARAM_GRID_MARKET_MAKING", PARAM_GRID_MARKET_MAKING, PARAM_DEFAULTS_MARKET_MAKING),
    ]

    violations: List[str] = []
    for grid_name, grid, defaults in mapping:
        for key, values in grid.items():
            if key not in defaults:
                continue
            if not isinstance(values, list):
                continue
            default_val = defaults.get(key)
            if default_val not in values:
                violations.append(
                    f"{grid_name}.{key}: default={default_val!r} not in grid={values!r}"
                )
    return violations


def validate_shadow_param_independence(threshold: float = 0.20) -> Dict[str, float]:
    """P0-Q1质量门：验证影子策略参数与主策略差异度>threshold"""
    from param_pool._param_grids import (
        PARAM_DEFAULTS_BOX_EXTREME, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B,
        PARAM_DEFAULTS_BOX_SPRING, PARAM_DEFAULTS_BOX_SPRING_SHADOW_A, PARAM_DEFAULTS_BOX_SPRING_SHADOW_B,
        PARAM_DEFAULTS_ARBITRAGE, PARAM_DEFAULTS_ARBITRAGE_SHADOW_A, PARAM_DEFAULTS_ARBITRAGE_SHADOW_B,
        PARAM_DEFAULTS_MARKET_MAKING, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B,
    )

    _SHADOW_DIFF_KEYS = [
        "close_take_profit_ratio", "close_stop_loss_ratio",
        "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
        "resonance_hard_time_stop_min", "box_hard_time_stop_min",
        "max_risk_ratio",
    ]
    results = {}
    for group_name, main_params, shadow_a, shadow_b in [
        ("S2_main", PARAM_DEFAULTS, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B),
        ("S1_hft", PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B),
        ("S3_box_extreme", PARAM_DEFAULTS_BOX_EXTREME,
         PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B),
        ("S4_box_spring", PARAM_DEFAULTS_BOX_SPRING,
         PARAM_DEFAULTS_BOX_SPRING_SHADOW_A, PARAM_DEFAULTS_BOX_SPRING_SHADOW_B),
        ("S5_arbitrage", PARAM_DEFAULTS_ARBITRAGE,
         PARAM_DEFAULTS_ARBITRAGE_SHADOW_A, PARAM_DEFAULTS_ARBITRAGE_SHADOW_B),
        ("S6_market_making", PARAM_DEFAULTS_MARKET_MAKING,
         PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B),
    ]:
        for shadow_name, shadow_params in [("shadow_a", shadow_a), ("shadow_b", shadow_b)]:
            diffs = []
            for key in _SHADOW_DIFF_KEYS:
                if key in main_params and key in shadow_params and main_params[key] != 0:
                    diffs.append(abs(shadow_params[key] - main_params[key]) / abs(main_params[key]))
            avg_diff = sum(diffs) / max(1, len(diffs))
            label = f"{group_name}.{shadow_name}"
            results[label] = round(avg_diff, 4)
            if avg_diff < threshold:
                logger.warning("[P0-Q1 FAIL] %s 参数差异度 %.2f%% < %.0f%% 阈值", label, avg_diff * 100, threshold * 100)
            else:
                logger.info("[P0-Q1 PASS] %s 参数差异度 %.2f%%", label, avg_diff * 100)
    return results


def _check_safety(
    bt: _BacktestState,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
    allow_close: bool = False,
    bar: Optional[pd.Series] = None,
) -> bool:
    from param_pool import backtest_runner_validation as _bsc
    return _bsc.check_safety(bt, bar_time, params, allow_close=allow_close, bar=bar,
        _get_contract_multiplier=_get_contract_multiplier,
        _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
        _compute_commission=_compute_commission,
        _safe_equity_add=_safe_equity_add,
        _infer_instrument_type=_infer_instrument_type,
        _infer_exchange_id=_infer_exchange_id,
        _infer_exchange_from_id=_infer_exchange_from_id,
        _calculate_limit_prices=_calculate_limit_prices,
        _ClosedTrade=_ClosedTrade)


def _check_backtest_health(bt, params: Dict[str, float], bar_time) -> bool:
    """P0-R11-06修复: 回测健康检查机制

    模拟实盘get_health_status()的CRITICAL暂停逻辑：
    - 当回测权益低于初始权益的50%时，标记为CRITICAL，暂停新开仓
    - 当连续亏损次数超过阈值时，标记为CRITICAL
    - 与实盘strategy_ecosystem.get_health_status()对齐
    """
    # 权益生存检查：权益低于初始权益50%视为CRITICAL
    if hasattr(bt, 'initial_equity') and bt.initial_equity > 0:
        _health_equity = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
        equity_ratio = _health_equity / bt.initial_equity
        if equity_ratio < 0.50:
            return False  # CRITICAL: 暂停新开仓

    # 连续亏损检查：连续亏损超过max_consecutive_losses视为CRITICAL
    max_consecutive = int(params.get("max_consecutive_losses", 5))
    if hasattr(bt, 'consecutive_loss_streak') and bt.consecutive_loss_streak >= max_consecutive:
        return False  # CRITICAL: 暂停新开仓

    return True  # HEALTHY: 允许新开仓


def _check_bar_data_monotonic(bar_data: pd.DataFrame) -> None:
    """NP-P2-15: 检查bar时间序列是否单调递增

    支持多种时间列名（minute/timestamp/datetime），并按symbol分组检查。'
    """
    if bar_data is None or bar_data.empty:
        return
    time_col = None
    for col in ('minute', 'timestamp', 'datetime'):
        if col in bar_data.columns:
            time_col = col
            break
    if time_col is None:
        return
    try:
        times = pd.to_datetime(bar_data[time_col], errors='coerce')
        if times.isna().any():
            logger.warning("[NP-P2-15] %s列存在无法解析时间，跳过单调性校验", time_col)
            return
        if times.is_monotonic_increasing:
            return
        if 'symbol' in bar_data.columns:
            for sym, grp in bar_data.groupby('symbol'):
                sym_times = pd.to_datetime(grp[time_col], errors='coerce')
                if not sym_times.is_monotonic_increasing:
                    logger.warning("[NP-P2-15] Bar数据非单调递增: symbol=%s", sym)
        else:
            logger.warning("[NP-P2-15] bar_data.%s非单调递增，回测结果可能偏差", time_col)
    except (ValueError, KeyError, TypeError) as e:
        logger.debug("[NP-P2-15] bar_data单调性校验失败: %s", e)


__all__ = [
    'validate_default_values_in_grids',
    'validate_shadow_param_independence',
    '_check_safety',
    '_check_backtest_health',
    '_check_bar_data_monotonic',
]

# ── Merged from backtest_safety_checker.py on 2026-06-12 ──

"""
回测安全检查器 — 从backtest_runner_base.py拆分
职责: 保证金追保、断路器暂停、日回撤硬停止
"""

import logging
from typing import Dict, Optional, Any

import numpy as np
import pandas as pd


def _force_close_position(bt, sym, pos, bar, bar_time, close_reason, *,
                          _get_contract_multiplier, _compute_dynamic_slippage_bps,
                          _compute_commission, _safe_equity_add, _infer_instrument_type,
                          _infer_exchange_id, _infer_exchange_from_id,
                          _calculate_limit_prices, _ClosedTrade, params):
    _limit_info = _calculate_limit_prices(
        bar,
        instrument_type=_infer_instrument_type(sym)
        .replace('option_buyer', 'OPTION_ETF')
        .replace('option_seller', 'OPTION_ETF')
        .replace('future', 'FUTURE')
    )
    _is_limit_up = _limit_info['is_limit_up']
    _is_limit_down = _limit_info['is_limit_down']
    _close_dir = 'SELL' if pos.volume > 0 else 'BUY'
    if _is_limit_up and _close_dir == 'SELL':
        return False
    if _is_limit_down and _close_dir == 'BUY':
        return False
    close_price = bar.get("close", pos.open_price)
    _mult = _get_contract_multiplier(sym)
    pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 0)
    slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
    slip = close_price * slip_bps / 10000 * pos.lots
    commission = _compute_commission(
        sym, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"),
        is_open=False, exchange_id=_infer_exchange_id(sym),
        exchange=_infer_exchange_from_id(_infer_exchange_id(sym))
    )
    net_pnl = pnl - slip - commission
    _safe_equity_add(bt, net_pnl)
    bt.total_trades += 1
    if np.isclose(close_price, pos.open_price, rtol=1e-10):
        float_pnl_pct = 0.0
    else:
        float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct
    hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
    bt.closed_trades.append(_ClosedTrade(
        pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason=close_reason,
        hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
        premium_pnl=0.0, delta_pnl=0.0,
        stage1_passed=getattr(pos, 'stage1_passed', False),
    ))
    bt.recent_pnls.append(net_pnl)
    if len(bt.recent_pnls) > 50:
        bt.recent_pnls = bt.recent_pnls[-50:]
    del bt.positions[sym]
    return True


def check_margin_call(bt, params, bar, *,
                      _get_contract_multiplier, _safe_equity_add):
    if bt.equity <= 0 or len(bt.positions) == 0:
        return False
    _risk_equity = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
    try:
        _total_margin = 0.0
        for _sym, _pos in bt.positions.items():
            _mult = _get_contract_multiplier(_sym)
            _margin_ratio = params.get('margin_ratio', 0.1)
            _total_margin += abs(_pos.volume) * _pos.open_price * _mult * _margin_ratio
        _margin_occupancy = _total_margin / _risk_equity
        if _margin_occupancy > 0.95 and bar is not None:
            logging.warning("[EX-08] 保证金占用率%.1f%%>95%%, 强制平仓", _margin_occupancy * 100)
            for _sym in list(bt.positions.keys()):
                _pos = bt.positions[_sym]
                _close_price = bar.get("close", _pos.open_price)
                _pnl = (_close_price - _pos.open_price) * _pos.volume * _get_contract_multiplier(_sym)
                _safe_equity_add(bt, _pnl)
                del bt.positions[_sym]
                bt.total_trades += 1
            return True
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError):
        pass
    return False


def check_safety(bt, bar_time, params, allow_close=False, bar=None, **deps):
    _get_contract_multiplier = deps['_get_contract_multiplier']
    _compute_dynamic_slippage_bps = deps['_compute_dynamic_slippage_bps']
    _compute_commission = deps['_compute_commission']
    _safe_equity_add = deps['_safe_equity_add']
    _infer_instrument_type = deps['_infer_instrument_type']
    _infer_exchange_id = deps['_infer_exchange_id']
    _infer_exchange_from_id = deps['_infer_exchange_from_id']
    _calculate_limit_prices = deps['_calculate_limit_prices']
    _ClosedTrade = deps['_ClosedTrade']

    check_margin_call(bt, params, bar,
                      _get_contract_multiplier=_get_contract_multiplier,
                      _safe_equity_add=_safe_equity_add)

    is_circuit_breaker_paused = (
        bt.circuit_breaker_until is not None and bar_time < bt.circuit_breaker_until
    )
    if is_circuit_breaker_paused and not allow_close:
        if bar is not None and len(bt.positions) > 0:
            for sym in list(bt.positions.keys()):
                pos = bt.positions[sym]
                _force_close_position(
                    bt, sym, pos, bar, bar_time, "circuit_breaker_force_close",
                    _get_contract_multiplier=_get_contract_multiplier,
                    _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
                    _compute_commission=_compute_commission,
                    _safe_equity_add=_safe_equity_add,
                    _infer_instrument_type=_infer_instrument_type,
                    _infer_exchange_id=_infer_exchange_id,
                    _infer_exchange_from_id=_infer_exchange_from_id,
                    _calculate_limit_prices=_calculate_limit_prices,
                    _ClosedTrade=_ClosedTrade,
                    params=params,
                )
        return False

    _risk_equity_dd = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
    _risk_daily_start = bt.daily_start_equity
    if _risk_daily_start > 0:
        daily_drawdown = (_risk_daily_start - _risk_equity_dd) / _risk_daily_start
        _prev_5day_avg = getattr(bt, 'prev_5day_avg_profit', 0.0) or params.get('prev_5day_avg_profit', 0.0)
        _dd_multiplier = params.get('daily_drawdown_multiplier', 2.0)
        from infra.security_service import resolve_and_check_daily_drawdown
        should_stop, _reason = resolve_and_check_daily_drawdown(
            daily_drawdown_pct=daily_drawdown,
            hard_stop_pct=params.get("daily_loss_hard_stop_pct", None),
            prev_5day_avg_profit=_prev_5day_avg,
            multiplier=_dd_multiplier,
            daily_start_equity=_risk_daily_start,
        )
        if should_stop and not allow_close:
            if bar is not None and len(bt.positions) > 0:
                for sym in list(bt.positions.keys()):
                    pos = bt.positions[sym]
                    _force_close_position(
                        bt, sym, pos, bar, bar_time, "daily_drawdown_force_close",
                        _get_contract_multiplier=_get_contract_multiplier,
                        _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
                        _compute_commission=_compute_commission,
                        _safe_equity_add=_safe_equity_add,
                        _infer_instrument_type=_infer_instrument_type,
                        _infer_exchange_id=_infer_exchange_id,
                        _infer_exchange_from_id=_infer_exchange_from_id,
                        _calculate_limit_prices=_calculate_limit_prices,
                        _ClosedTrade=_ClosedTrade,
                        params=params,
                    )
            return False

    return True

# ── Runner Helpers (merged from _leaf_runner_helpers.py) ──

def _bt_capture_snapshot(bt: _BacktestState, trigger_name: str, detail: str = "",
                          strategy_type: str = "", bar: Any = None) -> None:
    """回测快照采集辅助函数"""
    if bt.snapshot_collector is None:
        return
    try:
        from strategy_judgment.market_snapshot_collector import (
            SnapshotTrigger, StrategyStateSnapshot, SEVEN_STRATEGY_KEYS, THREE_VARIANTS,
            LifeExpectancySnapshot, CyclePredictionSnapshot, RiskDimensionScores,
        )
        trigger_map = {
            "signal": SnapshotTrigger.SIGNAL_GENERATED,
            "open": SnapshotTrigger.ORDER_OPENED,
            "close": SnapshotTrigger.ORDER_CLOSED,
        }
        trigger = trigger_map.get(trigger_name, SnapshotTrigger.SIGNAL_GENERATED)
        states = []
        for sk in SEVEN_STRATEGY_KEYS:
            for variant in THREE_VARIANTS:
                states.append(StrategyStateSnapshot(
                    strategy_id=f"{sk}_{variant}",
                    strategy_type=sk,
                ))
        ts = np.datetime64('now')
        if bar is not None and hasattr(bar, 'get'):
            bar_ts = bar.get('minute', None)
            if bar_ts is not None:
                ts = np.datetime64(str(bar_ts))

        life_snap = None
        hmm_state = getattr(bt, 'current_state', None)
        if hmm_state:
            estimator = _get_life_estimator()
            if estimator is not None and hasattr(estimator, '_life_dict') and estimator._life_dict:
                try:
                    life = estimator.get_life_expectancy(hmm_state)
                    if life is not None and life.is_valid():
                        life_snap = LifeExpectancySnapshot(
                            hmm_state=hmm_state,
                            duration_p25=life.duration.get('p25', 0),
                            duration_p50=life.duration.get('p50', 0),
                            duration_p75=life.duration.get('p75', 0),
                            duration_p99=life.duration.get('p99', 0),
                            magnitude_p50=life.magnitude.get('p50', 0),
                            magnitude_p75=life.magnitude.get('p75', 0),
                            sample_count=life.sample_count,
                            decay_r_squared=life.decay_r_squared,
                            degradation_level=life.degradation_level,
                            is_valid=True,
                        )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                    logger.warning("[SNAPSHOT] life_estimator failed: %s", _e)

        cycle_snap = None
        if hmm_state:
            cycle_snap = CyclePredictionSnapshot()
            if bar is not None and hasattr(bar, 'get'):
                trend_scores, trend_directions = _infer_trend_scores_from_bar(bar)
                cycle_snap.trend_scores_short = trend_scores[0]
                cycle_snap.trend_scores_medium = trend_scores[1]
                cycle_snap.trend_scores_long = trend_scores[2]
                cycle_snap.trend_directions_short = trend_directions[0]
                cycle_snap.trend_directions_medium = trend_directions[1]
                cycle_snap.trend_directions_long = trend_directions[2]
            if life_snap is not None and life_snap.duration_p75 > 0:
                cycle_snap.phase_remaining_estimate = life_snap.duration_p75

        risk_dims = RiskDimensionScores()
        if life_snap is not None and life_snap.is_valid:
            risk_dims.d3_life_expectancy = {0: 1.0, 1: 0.7, 2: 0.4, 3: 0.2}.get(
                life_snap.degradation_level, 0.5)
        if cycle_snap is not None:
            risk_dims.d4_cycle_resonance = cycle_snap.resonance_strength if cycle_snap.resonance_strength > 0 else 0.5
            risk_dims.d5_phase_quality = cycle_snap.phase_quality

        bt.snapshot_collector.capture(
            timestamp=ts, trigger=trigger, trigger_detail=detail,
            bar=bar,
            strategy_states=states,
            life_expectancy=life_snap,
            cycle_prediction=cycle_snap,
            risk_dimensions=risk_dims,
        )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[_bt_capture_snapshot] error: %s", e)

def _check_state_transition(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> str:
    for _field in ("correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct"):
        _val = bar.get(_field, None)
        if _field not in bar or pd.isna(_val):
            logger.warning("[P0-1] 五态标签字段 %s 缺失或NaN，默认为0", _field)
            if _field in bar.index:
                bar[_field] = 0.0

    non_other = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0) + bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
    try:
        from data.width_cache import get_width_strength_cache
        _wc = get_width_strength_cache()
        if hasattr(_wc, '_classify_status'):
            _ufid = int(bar.get("underlying_future_id", 0))
            _month = str(bar.get("month", ""))
            _opt_type = str(bar.get("option_type", ""))
            _current_price = float(bar.get("close", 0.0))
            _prev_price = float(bar.get("open", 0.0))
            candidate = _wc._classify_status(_ufid, _month, _opt_type, _current_price, _prev_price)
        else:
            raise ImportError("_classify_status not available")
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] _classify_status fallback: %s", _r3_err)
        threshold = params.get("non_other_ratio_threshold", 0.65)
        if non_other < threshold:
            candidate = "other"
        else:
            correct_rise = bar.get("correct_rise_pct", 0)
            correct_fall = bar.get("correct_fall_pct", 0)
            wrong_rise = bar.get("wrong_rise_pct", 0)
            wrong_fall = bar.get("wrong_fall_pct", 0)
            correct_total = correct_rise + correct_fall
            incorrect_total = wrong_rise + wrong_fall
            if correct_total >= incorrect_total:
                candidate = "correct_trending" if correct_rise >= correct_fall else "correct_trending_defensive"
            else:
                candidate = "incorrect_reversal" if wrong_rise >= wrong_fall else "incorrect_reversal_defensive"

    confirm_bars = int(params.get("state_confirm_bars", 5))

    bar_period = float(params.get("bar_period", 1.0))
    if bar_period <= 0.1:
        hft_state_confirm_seconds = float(params.get("hft_state_confirm_seconds", 5.0))
        confirm_bars = max(1, int(hft_state_confirm_seconds / (bar_period * 60.0)))

    if candidate == bt.current_state:
        bt.pending_state = None
        bt.state_confirm_count = 0
        return bt.current_state

    if bt.pending_state == candidate:
        bt.state_confirm_count += 1
    else:
        bt.pending_state = candidate
        bt.state_confirm_count = 1

    if bt.state_confirm_count >= confirm_bars:
        bt.current_state = candidate
        bt.pending_state = None
        bt.state_confirm_count = 0

    return bt.current_state

def _compute_lots_with_risk_budget(
    equity: float,
    price: float,
    sl_ratio: float,
    lots_min: int,
    params: Dict[str, float],
    recent_pnls: Optional[List[float]] = None,
    bar: Optional[pd.Series] = None,
    current_positions: Optional[Dict[str, '_BacktestPosition']] = None,
    bt: Optional['_BacktestState'] = None,
) -> int:
    """计算开仓手数，考虑风险预算和已有仓位。"""
    if sl_ratio <= 0:
        return 0
    if not safe_price_check(price):
        return 0

    risk = params.get("max_risk_ratio", 0.8)
    max_loss_per_lot = price * sl_ratio
    if max_loss_per_lot <= 0:
        return 0

    existing_risk_used = 0.0
    if current_positions:
        for pos in current_positions.values():
            if hasattr(pos, 'open_price') and hasattr(pos, 'volume'):
                pos_risk = abs(int(pos.volume)) * pos.open_price * sl_ratio
                existing_risk_used += pos_risk

    available_risk_budget = equity * risk - existing_risk_used
    if available_risk_budget <= 0:
        logger.warning(
            "[R4-P-01] Risk budget exhausted: equity=%.2f, risk=%.2f%%, existing_risk=%.2f, available=%.2f",
            equity, risk * 100, existing_risk_used, available_risk_budget
        )
        return 0

    max_lots = max(1, math.ceil(available_risk_budget / max_loss_per_lot))
    lots = min(lots_min, max_lots)

    max_risk_per_trade = params.get("max_risk_per_trade", 0.05)
    if max_risk_per_trade > 0 and equity > 0:
        max_lots_by_risk = max(1, round(equity * max_risk_per_trade / max_loss_per_lot))
        lots = min(lots, max_lots_by_risk)

    if recent_pnls and len(recent_pnls) >= 3:
        lookback = min(10, len(recent_pnls))
        recent = recent_pnls[-lookback:]
        losses = sum(1 for p in recent if p < 0)
        if losses > lookback * 0.6:
            lots = max(1, round(lots * 0.5))
        elif losses > lookback * 0.4:
            lots = max(1, round(lots * 0.75))

    _hard_stop_streak = int(params.get("consecutive_loss_hard_stop", 6))
    if bt is not None and bt.consecutive_loss_streak >= _hard_stop_streak:
        if bt.consecutive_loss_streak == _hard_stop_streak:
            logger.warning("[R16-P1-015] 连续亏损%d次(硬停止阈值%d)，暂停交易1个周期", bt.consecutive_loss_streak, _hard_stop_streak)
        return 0

    try:
        _position_scale = params.get("position_scale", 1.0)
        if _position_scale != 1.0 and _position_scale > 0:
            lots = max(1, round(lots * _position_scale))
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        logging.debug("[R3-L2] position_scale application skipped: %s", _r3_err)
        pass

    final_lots = max(0, lots)
    if final_lots == 0 and lots_min > 0:
        logger.warning(
            "[R4-P-03] Computed lots=0 (< lots_min=%d): equity=%.2f, price=%.2f, sl_ratio=%.4f, existing_risk=%.2f",
            lots_min, equity, price, sl_ratio, existing_risk_used
        )

    return final_lots

def _check_logic_reversal(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> bool:
    """逻辑反转平仓检测"""
    _bar_time_lr = bar.get("minute", pd.NaT)
    if not pd.isna(_bar_time_lr):
        if not _check_safety(bt, _bar_time_lr, params, allow_close=True, bar=bar):
            return True
    for symbol, pos in list(bt.positions.items()):
        if pos.open_reason != "CORRECT_RESONANCE":
            continue

        correct_pct = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0)
        wrong_pct = bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
        reversal_threshold = params.get("logic_reversal_threshold", 1.5)

        if wrong_pct > correct_pct * reversal_threshold and wrong_pct > BACKTEST_THRESHOLDS["logic_reversal_min_wrong_pct"]:
            price = bar.get("close", 0.0)
            bar_time = bar.get("minute", pd.NaT)
            if pd.isna(bar_time):
                logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar")
                continue
            if price <= 0:
                continue

            if hasattr(bt, '_last_bar_time') and bt._last_bar_time is not None:
                if bar_time < bt._last_bar_time:
                    logger.error(
                        "[NP-P2-13] wrong_pct来源bar时间%s < 回测游标%s，未来数据污染，跳过本bar",
                        bar_time, bt._last_bar_time,
                    )
                    continue

            _multiplier = _get_contract_multiplier(symbol)
            signed_volume = pos.volume if pos.volume != 0 else 0
            pnl = (price - pos.open_price) * signed_volume * _multiplier
            _premium_pnl = 0.0
            _delta_pnl = 0.0
            if getattr(pos, 'instrument_type', 'future') == 'option_buyer' or getattr(pos, 'instrument_type', 'future') == 'option_seller':
                _open_premium = getattr(pos, 'option_premium', 0.0)
                _close_premium = bar.get('option_premium', _open_premium + (price - pos.open_price))
                _premium_pnl = (_close_premium - _open_premium) * pos.lots
                _delta_pnl = pnl - _premium_pnl
            bid_ask = bar.get("bid_ask_spread", 0.0)
            spread_q = bar.get("_spread_quality", 0)
            slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
            slip = price * slip_bps / 10000 * pos.lots
            commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(symbol)))
            _safe_equity_add(bt, pnl - slip - commission)
            bt.total_trades += 1
            _bt_capture_snapshot(bt, "close", f"逻辑反转平仓 {symbol}", "", bar)
            bt.recent_pnls.append(pnl - slip - commission)
            if len(bt.recent_pnls) > 50:
                bt.recent_pnls = bt.recent_pnls[-50:]
            del bt.positions[symbol]
            logger.debug("逻辑反转平仓: %s @ %.2f (wrong=%.2f > correct*%.1f=%.2f)",
                         symbol, price, wrong_pct, reversal_threshold, correct_pct * reversal_threshold)

    return True

def _infer_trend_scores_from_bar(bar: pd.Series) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    strength = bar.get("strength", 0.0)
    imbalance = bar.get("imbalance", 0.0)
    direction = 1.0 if imbalance > 0 else -1.0
    short_score = direction * min(abs(imbalance) * 2, 1.0)
    medium_score = direction * min(strength * 2, 1.0)
    long_score = direction * min((strength + abs(imbalance)) * 0.5, 1.0)
    scores = (short_score, medium_score, long_score)
    directions = (np.sign(short_score), np.sign(medium_score), np.sign(long_score))
    return scores, directions

__all__ = [
    '_bt_capture_snapshot',
    '_check_state_transition',
    '_compute_lots_with_risk_budget',
    '_check_logic_reversal',
    '_infer_trend_scores_from_bar',
]