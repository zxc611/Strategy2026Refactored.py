# MODULE_ID: M1-154
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
from __future__ import annotations

import logging
from ali2026v3_trading.infra._helpers import get_logger
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY
from ali2026v3_trading.infra.resilience import should_trigger_stop_loss, should_trigger_take_profit

from ali2026v3_trading.param_pool.backtest.backtest_state import (
    _BacktestState, _BacktestPosition, _ClosedTrade, _PendingOrder,
    _compute_commission, _infer_exchange_id, _infer_exchange_from_id,
    _infer_instrument_type, _calculate_limit_prices,
    _safe_equity_add, _get_contract_multiplier,
    _compute_dynamic_slippage_bps, _compute_cascade_slippage_bps,
    _resolve_tp_sl, _resolve_time_stop,
    _is_consecutive_loss_paused, _backtest_order_split,
    _check_intra_bar_stop_loss, _check_two_stage_stop,
    _update_mtm_equity, _compute_option_mtm_price,
    _get_life_estimator,
)
from ali2026v3_trading.param_pool.backtest.backtest_config import (
    INITIAL_EQUITY, SLIPPAGE_BPS, ENABLE_QUEUE_SIMULATION,
    CANCEL_DELAY_MS, CANCEL_FAILURE_RATE, ENABLE_CANCEL_SIMULATION,
    FEE_STRUCTURE, FEE_STRUCTURE_V2, COMMISSION_PER_LOT,
    DEFAULT_RISK_FREE_RATE,
)
from ali2026v3_trading.param_pool.backtest._backtest_fidelity import (
    _simulate_limit_order_queue, _simulate_order_cancel,
    _compute_fill_quantity,
    _apply_fidelity_presets,
)
from ali2026v3_trading.param_pool._param_grids import _compute_market_impact_v2
from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import (
    _sync_random_seed, _get_annualize_factor,
    _build_backtest_result, _compute_profit_loss_ratio_metrics,
    _check_bar_data_monotonic, _reset_daily, _backfill_bar_fields,
    _bt_capture_snapshot, _check_state_transition,
    _compute_lots_with_risk_budget, _check_positions, _try_open,
    _infer_trend_scores_from_bar,
    exclude_rollover_signals,
    _STATE_REASON_MAP,
)
from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import (
    _check_backtest_health,
    _check_safety,
)
from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
    _check_logic_reversal,
)
from ali2026v3_trading.param_pool._param_grids import BACKTEST_THRESHOLDS
from ali2026v3_trading.param_pool.ts.ts_backtest_strategies import BAR_INTERVAL_GRID
from ali2026v3_trading.param_pool.backtest.backtest_strategy_runners import (
    _handle_health_critical, _apply_shadow_direction, _check_other_state_block,
    _check_risk_service_before_trade, _check_n1_quality_gates,
    _execute_open_with_slippage, _check_eod_close, _close_position_with_pnl,
    run_backtest_box_extreme, run_backtest_box_spring,
    run_backtest_arbitrage, run_backtest_market_making,
    _ensure_interpolate_ticks,
)

logger = get_logger(__name__)

_interpolate_ticks_in_bar = None
def run_backtest_hft(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
) -> Dict[str, Any]:
    """S1高频趋势共振回测：tick级决策频率

    与S2分钟级趋势共振共享底层逻辑（一致性共振→方向延续），
    但参数集完全独立（毫秒/tick数 vs 分钟/秒）：
    - hft_signal_confirm_ticks: 信号确认所需连续tick数
    - hft_cooldown_ms: 信号冷却时间（毫秒）
    - hft_min_imbalance: 最小允许开仓的imbalance阈值

    影子策略：
    - shadow_reverse: 方向强制反转
    - shadow_random: 随机方向（概率基于信号频率）
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    _sync_random_seed(42 if train else 24)

    # P1-裂缝49：S1 200ms延迟microstructure模型回退
    hft_latency_tier = params.get("hft_latency_tier_ms", 1000)
    if hft_latency_tier <= 200:
        logger.info(
            "[P1-裂缝49] HFT延迟%dms档位: microstructure模型未实现, "
            "回退使用order_flow模型(fallback)",
            int(hft_latency_tier),
        )

    hft_signal_count = 0
    hft_pending_direction = 0

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        # P0-R11-06修复: 回测健康检查
        if not _check_backtest_health(bt, params, bar_time):
            _handle_health_critical(bt, bar)
            continue

        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            imbalance = abs(bar.get("imbalance", 0))
            strength = bar.get("strength", 0)

            if strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["hft_shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3)):
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
                else:
                    should_open_hft = False
            else:
                should_open_hft = False
                if imbalance >= min_imbalance and strength > 0.2:
                    current_dir = 1 if bar.get("imbalance", 0) > 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True


            # Q1修复: other状态下主策略/反向策略不开仓
            if should_open_hft and _check_other_state_block(bt, strategy_type, ("main", "shadow_reverse", "hft")):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                if lots <= 0:
                    continue

                # P0-R11-05修复: hft开仓路径调用RiskService.check_before_trade()
                if not _check_risk_service_before_trade(symbol, hft_pending_direction, price, lots, "BT_HFT", bar_time):
                    continue

                _execute_open_with_slippage(
                    bt, bar, params, symbol, hft_pending_direction, price, lots,
                    tp_ratio, sl_ratio, reason,
                    spread_quality=bar.get("_spread_quality", 0),
                    extra_pos_fields={
                        "open_state": bt.current_state,
                        "open_strength": strength,
                    },
                )
                bt.last_signal_time = bar_time
                hft_signal_count = 0

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "hft_fidelity_warning": "DEGRADED: tick级参数(hft_cooldown_ms/hft_signal_confirm_ticks)在分钟级回测中失真，需HFT回放引擎验证",
        },
    )


def run_backtest_hft_with_disturbance(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
    tick_drop_prob: float = 0.0,
    delay_skip_lambda: float = 0.0,
) -> Dict[str, Any]:
    """漏洞三+七：S1 HFT回测 + 随机tick丢弃 + 微秒延迟注入

    Args:
        tick_drop_prob: 每个tick被丢弃的概率（模拟生产环境网络IO/负载导致漏tick）
        delay_skip_lambda: Poisson分布的lambda，模拟微秒延迟导致跳过tick数
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    _sync_random_seed(42 if train else 24)

    hft_signal_count = 0
    hft_pending_direction = 0
    dropped_ticks = 0
    delayed_skips = 0

    for idx in range(len(bar_data)):
        if tick_drop_prob > 0 and np.random.random() < tick_drop_prob:
            dropped_ticks += 1
            bt.equity_curve.append(bt.equity)
            continue

        if delay_skip_lambda > 0:
            skip_n = np.random.poisson(delay_skip_lambda)
            if skip_n > 0:
                delayed_skips += skip_n
                for _ in range(min(skip_n, len(bar_data) - idx - 1)):
                    idx += 1
                    if idx >= len(bar_data):
                        break
                    bt.equity_curve.append(bt.equity)

        if idx >= len(bar_data):
            break

        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧
        if _check_safety(bt, bar_time, params):
            imbalance = abs(bar.get("imbalance", 0))
            strength = bar.get("strength", 0)

            if strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["hft_shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3)):
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
                else:
                    should_open_hft = False
            else:
                should_open_hft = False
                if imbalance >= min_imbalance and strength > 0.2:
                    current_dir = 1 if bar.get("imbalance", 0) > 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True


            # Q1修复: other状态下主策略/反向策略不开仓
            if should_open_hft and _check_other_state_block(bt, strategy_type, ("main", "shadow_reverse", "hft")):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                if lots <= 0:
                    continue

                _execute_open_with_slippage(
                    bt, bar, params, symbol, hft_pending_direction, price, lots,
                    tp_ratio, sl_ratio, reason,
                    spread_quality=bar.get("_spread_quality", 0),
                    extra_pos_fields={
                        "open_state": bt.current_state,
                        "open_strength": strength,
                    },
                )
                bt.last_signal_time = bar_time
                hft_signal_count = 0

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "dropped_ticks": dropped_ticks,
            "delayed_skips": delayed_skips,
        },
    )


def run_backtest_hft_tick_fidelity(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
    random_seed: Optional[int] = None,  # P-20修复: 种子参数化
) -> Dict[str, Any]:
    """S1高频趋势共振回测（tick级保真版）

    在每根分钟Bar内，使用例interpolate_ticks_in_bar()生成虚拟tick序列，
    使hft_signal_confirm_ticks恢复真实语义（连续N个tick方向一致才确认）。

    与run_backtest_hft的区别：
      - run_backtest_hft: 逐Bar遍历，confirm_ticks含义=连续N根Bar方向一致（失真）
      - 本函数: 逐tick遍历，confirm_ticks含义=连续N个tick方向一致（保真）
    """
    # BF-07修复: 扩展Tick保真回测覆盖范围说明
    supported_strategies = params.get('strategy_names', ['S1'])
    unsupported = [s for s in supported_strategies if s not in ('S1', 'high_freq')]
    if unsupported:
        logger.info("BF-07: 策略 %s 使用Bar回测(保真度75-80分)，S1策略可启用Tick回测(95分)", unsupported)

    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    if not hasattr(bt, 'trade_log'):
        bt.trade_log = []
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    ticks_per_bar = int(params.get("hft_ticks_per_bar", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    _sync_random_seed(random_seed if random_seed is not None else (42 if train else 24))

    hft_signal_count = 0
    hft_pending_direction = 0
    bar_idx_for_state = 0

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        bar_idx_for_state += 1

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    direction = 1 if pos.volume > 0 else -1
                    should_close_pos, close_reason_pos = _check_eod_close(bar_time)
                    if should_close_pos:
                        _close_position_with_pnl(bt, sym, pos, bar_price)
                        bt.trade_log.append({"pnl": bt.equity - bt.equity_curve[-1] if bt.equity_curve else 0})
                        continue
                    if direction == 1:
                        # R1-3修复: 使用统一止盈止损触发函数
                        if should_trigger_stop_loss(bar_price, pos.stop_loss_price, is_long=True) or \
                           should_trigger_take_profit(bar_price, pos.stop_profit_price, is_long=True):
                            pnl = (bar_price - pos.open_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.trade_log.append({"pnl": pnl})
                            del bt.positions[sym]
                    elif direction == -1:
                        if should_trigger_stop_loss(bar_price, pos.stop_loss_price, is_long=False) or \
                           should_trigger_take_profit(bar_price, pos.stop_profit_price, is_long=False):
                            pnl = (pos.open_price - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.trade_log.append({"pnl": pnl})
                            del bt.positions[sym]

        if not _check_safety(bt, bar_time, params):
            continue

        tick_sequence = _ensure_interpolate_ticks()(bar, n_ticks=ticks_per_bar)

        for tick_i, tick in enumerate(tick_sequence):
            imbalance = tick.get("imbalance", 0)
            strength = tick.get("strength", 0)
            price = tick.get("last_price", bar.get("close", 0))

            should_open_hft = False

            if strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["hft_shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3)):
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
            else:
                if abs(imbalance) >= min_imbalance and strength > 0.2:
                    current_dir = 1 if imbalance > 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True


            # Q1修复: other状态下主策略/反向策略不开仓
            if should_open_hft and _check_other_state_block(bt, strategy_type, ("main", "shadow_reverse", "hft")):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                if price <= 0:
                    continue
                direction = hft_pending_direction

                sl_ratio = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复
                tp_ratio = params.get("close_take_profit_ratio", 1.8)  # R17-P0-CFG-04修复
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                if lots <= 0:
                    continue

                entry_price = price
                stop_loss = entry_price * (1 - sl_ratio) if direction == 1 else entry_price * (1 + sl_ratio)
                take_profit = entry_price * (1 + tp_ratio) if direction == 1 else entry_price * (1 - tp_ratio)

                bt.positions[symbol] = _BacktestPosition(
                    instrument_id=symbol,
                    volume=direction * lots,
                    open_price=entry_price,
                    open_time=bar_time,
                    stop_profit_price=take_profit,
                    stop_loss_price=stop_loss,
                    open_reason="HFT_TICK_CONFIRM",
                    lots=lots,
                    open_state=bt.current_state,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.last_signal_time = bar_time
                hft_signal_count = 0

            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    daily_rets = np.array(bt.daily_returns)
    # R17-12修复: 日频年化因子 √252≈15.87
    daily_sharpe = np.sqrt(ANNUALIZE_FACTOR_DAILY) * (np.mean(daily_rets) - DEFAULT_RISK_FREE_RATE / ANNUALIZE_FACTOR_DAILY) / np.std(daily_rets, ddof=1) if len(daily_rets) > 1 and np.std(daily_rets, ddof=1) > 1e-10 else 0.0

    n_trades = len(bt.trade_log)
    win_trades = sum(1 for t in bt.trade_log if t.get("pnl", 0) > 0)
    win_rate = win_trades / n_trades if n_trades > 0 else 0.0
    avg_pnl = np.mean([t.get("pnl", 0) for t in bt.trade_log]) if n_trades > 0 else 0.0

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "daily_sharpe": daily_sharpe,
            "n_trades": n_trades,
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "params": params,
            "hft_fidelity": "TICK_INTERPOLATED",
            "ticks_per_bar": ticks_per_bar,
            "confirm_ticks": confirm_ticks,
        },
        ticks_per_bar=ticks_per_bar,
    )


# ============================================================================
# 套利与做市策略回测
# ============================================================================


# ── Merged from backtest_runner_special.py on 2026-06-12 ──


def _scale_params_with_bar_interval(
    params: Dict[str, float],
    bar_interval_minutes: int,
    original_interval: int = 1,
) -> Dict[str, float]:
    scaled = dict(params)
    if not params.get("scale_periods_with_bar_interval", False):
        return scaled
    if bar_interval_minutes <= original_interval:
        return scaled
    scale_factor = original_interval / bar_interval_minutes
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
                scaled[key] = float(scaled_val)
    return scaled


def _get_scale_params_with_bar_interval():
    return _scale_params_with_bar_interval


def _get_load_multiscale_data():
    from ali2026v3_trading.param_pool.ts.ts_result_writer import _load_multiscale_data
    return _load_multiscale_data


def _get_infer_hmm_state_from_iv():
    from ali2026v3_trading.param_pool.validation.validation_deep_orchestrator import _infer_hmm_state_from_iv
    return _infer_hmm_state_from_iv


def run_backtest_multiscale(
    params: Dict[str, float],
    db_path: str,
    date_start: str,
    date_end: str,
    strategy: str = "resonance",
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """多粒度K线回测：自动选择最优bar_interval_minutes并执行回测"""
    from ali2026v3_trading.param_pool.backtest.backtest_runner_base import run_backtest

    bar_interval = int(params.get("bar_interval_minutes", 1))
    allowed = BAR_INTERVAL_GRID.get(strategy, [1])
    if bar_interval not in allowed:
        bar_interval = allowed[0]

    try:
        from ali2026v3_trading.config.config_service import get_cached_params
        _live_params = get_cached_params()
        _live_kline_style = _live_params.get('kline_style', 'M1')
        _live_interval_map = {'M1': 1, 'M5': 5, 'M15': 15, 'M30': 30, 'H1': 60, 'D1': 1440}
        _live_interval = _live_interval_map.get(_live_kline_style, 1)
        if bar_interval != _live_interval:
            logger.warning(
                "[R13-P1-CFG-07] 回测K线频率(%dmin)与实盘(%s=%dmin)不一致!",
                bar_interval, _live_kline_style, _live_interval,
            )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError):
        pass

    decision_interval = int(bar_interval)
    if decision_interval < bar_interval:
        bar_interval = decision_interval

    params = _get_scale_params_with_bar_interval()(params, bar_interval)
    bar_data = _get_load_multiscale_data()(db_path, date_start, date_end, bar_interval)

    if bar_data.empty:
        return {"error": "无数据", "params": params, "bar_interval_minutes": bar_interval}

    _check_bar_data_monotonic(bar_data)
    bt_func = {
        "high_freq": run_backtest_hft,
        "resonance": run_backtest,
        "box": run_backtest_box_extreme,
        "spring": run_backtest_box_spring,
        "arbitrage": run_backtest_arbitrage,
        "market_making": run_backtest_market_making,
    }.get(strategy, run_backtest)

    result = bt_func(params, bar_data, train=train, strategy_type=strategy_type)
    result["bar_interval_minutes"] = bar_interval
    result["kline_fidelity"] = "tick_interpolated" if strategy == "high_freq" else "bar_exact"
    return result


def _compute_fill_probability(queue_position: int) -> float:
    """P1-裂缝4: 队列位置成交概率 = 1/(1+queue_position)"""
    return 1.0 / (1.0 + float(queue_position))


def _run_backtest_box_extreme(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_extreme",
) -> Dict[str, Any]:
    """裂缝35: 决策时间戳与K线频率对齐的箱体极值回测包装器"""
    decision_interval = int(params.get("decision_interval_minutes", 1))
    if decision_interval > 1 and "minute" in bar_data.columns:
        bar_data = bar_data.copy()
        bar_data["_decision_group"] = bar_data["minute"].dt.floor(f"{decision_interval}min")
        bar_data = bar_data.drop_duplicates(subset=["_decision_group", "symbol"], keep="first")
        bar_data = bar_data.drop(columns=["_decision_group"])
    return run_backtest_box_extreme(params, bar_data, train=train, strategy_type=strategy_type)


def _run_backtest_hft(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
) -> Dict[str, Any]:
    """P1-裂缝49: S1 200ms延迟microstructure回退包装器"""
    hft_latency_tier = params.get("hft_latency_tier_ms", 1000)
    if hft_latency_tier <= 200:
        # microstructure/order_flow回退路径
        if "order_flow" not in params:
            params = dict(params)
            params["order_flow"] = True
            params["microstructure"] = True
    return run_backtest_hft(params, bar_data, train=train, strategy_type=strategy_type)


def run_backtest_with_cycle_resonance(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    strategy: str = "high_freq",
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """周期共振增强回测：每Bar动态调节风险曲面参数

    V5集成：当bar_data包含预计算V5列(directional_bias, resonance_strength, cr_phase,
    state_entropy, hmm_state)时，直接读取预计算值构造CycleResonanceOutput，
    跳过运行时CRM计算，大幅减少回测耗时。'
    """
    from ali2026v3_trading.param_pool.optimization.cycle_sharpe import CycleResonanceModule, CycleResonanceOutput, Phase  # noqa: F401

    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy": strategy}

    _check_bar_data_monotonic(bar_data)
    crm = CycleResonanceModule()

    _v5_cols_available = all(
        c in bar_data.columns
        for c in ("directional_bias", "resonance_strength", "cr_phase", "state_entropy")
    )

    if not _v5_cols_available:
        iv_vals = bar_data.get("iv", pd.Series([0.0] * len(bar_data)))
        iv_clean = iv_vals.replace(0, np.nan).dropna()
        if len(iv_clean) >= 10:
            iv_q33 = float(iv_clean.quantile(0.33))
            iv_q66 = float(iv_clean.quantile(0.66))
        else:
            iv_q33, iv_q66 = 0.15, 0.25

    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)

    crm_stats = {"phase_counts": {}, "avg_strength": 0.0, "avg_entropy": 0.0}
    phase_counts = {"蓄力": 0, "释放": 0, "衰竭": 0, "混沌": 0}
    strength_sum = 0.0
    entropy_sum = 0.0
    n_updates = 0

    _PHASE_MAP = {1: Phase.CHARGE, 2: Phase.RELEASE, 3: Phase.EXHAUST, 4: Phase.CHAOS}
    _HMM_MAP = {1: "LOW_VOL", 2: "NORMAL", 3: "HIGH_VOL"}

    def _get_run_fn(s: str):
        if s == "high_freq":
            return run_backtest_hft
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import run_backtest as _rb
        _fn_map = {
            "resonance": _rb,
            "box": run_backtest_box_extreme,
            "spring": run_backtest_box_spring,
            "arbitrage": run_backtest_arbitrage,
            "market_making": run_backtest_market_making,
        }
        return _fn_map.get(s, _rb)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        if _v5_cols_available:
            directional_bias = float(bar.get("directional_bias", 0.0))
            resonance_strength = float(bar.get("resonance_strength", 0.0))
            cr_phase_int = int(bar.get("cr_phase", 3))
            state_entropy = float(bar.get("state_entropy", 0.5))
            hmm_state_int = int(bar.get("hmm_state", 1))

            hmm_state = _HMM_MAP.get(hmm_state_int, "NORMAL")
            hmm_posterior = (0.33, 0.34, 0.33)
            phase = _PHASE_MAP.get(cr_phase_int, Phase.CHAOS)
            crm_output = CycleResonanceOutput(
                directional_bias=directional_bias,
                resonance_strength=resonance_strength,
                phase=phase,
                state_entropy=state_entropy,
                hmm_state=hmm_state,
                hmm_posterior=hmm_posterior,
            )
            strength = float(bar.get("strength", 0.0))
            imbalance = float(bar.get("imbalance", 0.0))
        else:
            iv = bar.get("iv", 0.0)
            hmm_state = _get_infer_hmm_state_from_iv()(iv, iv_q33, iv_q66)
            hmm_posterior = (0.33, 0.34, 0.33)
            if hmm_state == "LOW_VOL":
                hmm_posterior = (0.7, 0.2, 0.1)
            elif hmm_state == "HIGH_VOL":
                hmm_posterior = (0.1, 0.2, 0.7)

            trend_scores, trend_directions = _infer_trend_scores_from_bar(bar)
            strength = bar.get("strength", 0.0)
            imbalance = bar.get("imbalance", 0.0)

            crm_output = crm.update(
                hmm_state=hmm_state,
                hmm_posterior=hmm_posterior,
                trend_scores=trend_scores,
                trend_directions=trend_directions,
                strength=strength,
                imbalance=imbalance,
            )

        phase_counts[crm_output.phase.value] = phase_counts.get(crm_output.phase.value, 0) + 1
        strength_sum += crm_output.resonance_strength
        entropy_sum += crm_output.state_entropy
        n_updates += 1

        risk_surface = crm.get_risk_surface(strategy, crm_output)

        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    _check_positions(bt, bar, params)

                    pos = bt.positions.get(sym)
                    if pos is not None:
                        hold_sec = (bar_time - pos.open_time).total_seconds()
                        if hold_sec > risk_surface.max_hold_seconds:
                            price = bar.get("close", 0.0)
                            if price > 0:
                                _mult = _get_contract_multiplier(sym)
                                pnl = (price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
                                _bid_ask_mh = bar.get("bid_ask_spread", 0.0)
                                _sq_mh = bar.get("_spread_quality", 0)
                                slip_bps_mh = _compute_dynamic_slippage_bps(price, _bid_ask_mh, spread_quality=_sq_mh, bar=bar, params=params)
                                slip = price * slip_bps_mh / 10000 * pos.lots
                                commission = _compute_commission(sym, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(sym))
                                _safe_equity_add(bt, pnl - slip - commission)
                                bt.total_trades += 1
                                del bt.positions[sym]

        if _check_safety(bt, bar_time, params):
            _health_ok = _check_backtest_health(bt, params, bar_time)
            should_open = _health_ok and strength > 0.3 and len(bt.positions) < int(params.get("max_open_positions", 3))

            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False

            if strategy_type == "shadow_random":
                should_open = np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3))

            if should_open:
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                sl_ratio *= risk_surface.stop_loss_multiplier

                base_lots = int(params.get("lots_min", 1))
                adjusted_lots = max(1, int(base_lots * risk_surface.size_multiplier))
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, adjusted_lots, params,
                    recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                if lots <= 0:
                    continue

                direction = 1 if imbalance > 0 else -1
                if strategy_type == "shadow_reverse":
                    direction = -direction

                volume = direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 0)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                slip_cost = price * slip_bps / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))
                bt.equity -= (commission + slip_cost)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=volume,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason=reason,
                    lots=lots,
                    open_state=bt.current_state,
                    open_strength=strength,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.positions[symbol] = pos
                bt.last_signal_time = bar_time
                bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    if n_updates > 0:
        crm_stats["phase_counts"] = phase_counts
        crm_stats["avg_strength"] = strength_sum / n_updates
        crm_stats["avg_entropy"] = entropy_sum / n_updates

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={"crm_stats": crm_stats},
    )
