# [M1-98] 合并策略入口
# MODULE_ID: M1-160
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
#!/usr/bin/env python3
"""
合并策略入口：box/hft/arb_mm 三类策略的回测函数

Phase 1 轮次1重构：从 backtest_runner_box.py / backtest_runner_hft.py / backtest_runner_arb_mm.py
提取共享逻辑为工具函数，各策略仅保留决策核心。

共享逻辑：
  - _handle_health_critical: CRITICAL时强制平仓并暂停新开仓
  - _apply_shadow_direction: 影子策略方向处理（shadow_reverse/shadow_random）
  - _check_risk_service_before_trade: RiskService.check_before_trade()调用模板
  - _check_n1_quality_gates: N1质量检查+C-13安全检查链
  - _execute_open_with_slippage: 动态滑点+手续费+开仓执行
"""
from __future__ import annotations

import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
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

logger = get_logger(__name__)  # R9-5

_interpolate_ticks_in_bar = None


def _ensure_interpolate_ticks():
    global _interpolate_ticks_in_bar
    if _interpolate_ticks_in_bar is None:
        from ali2026v3_trading.param_pool.ts.ts_result_writer import _interpolate_ticks_in_bar as _fn
        _interpolate_ticks_in_bar = _fn
    return _interpolate_ticks_in_bar


# ============================================================================
# 共享工具函数：提取自三个策略runner的重复代码
# ============================================================================

def _handle_health_critical(bt: _BacktestState, bar: pd.Series) -> None:
    """P0-R11-06: 回测健康检查CRITICAL时强制平仓并暂停新开仓

    三个runner中完全相同的健康检查处理块，提取为共享函数。
    调用前已确认 _check_backtest_health 返回 False。
    """
    for _sym_hc in list(bt.positions.keys()):
        _pos_hc = bt.positions[_sym_hc]
        _hc_cp = bar.get("close", 0)
        if _pos_hc.volume > 0:
            _pnl_hc = (_hc_cp - _pos_hc.open_price) * _pos_hc.lots * _get_contract_multiplier(_sym_hc)
        else:
            _pnl_hc = (_pos_hc.open_price - _hc_cp) * _pos_hc.lots * _get_contract_multiplier(_sym_hc)
        _safe_equity_add(bt, _pnl_hc)
        bt.total_trades += 1
        del bt.positions[_sym_hc]
    bt.peak_equity = max(bt.peak_equity, bt.equity)
    bt.equity_curve.append(bt.equity)


def _apply_shadow_direction(
    direction: int,
    strategy_type: str,
    shadow_random_prob_key: str = "shadow_random_open_prob",
) -> Tuple[int, bool]:
    """影子策略方向处理

    Args:
        direction: 原始方向 (1=做多, -1=做空)
        strategy_type: 策略类型 (main/shadow_reverse/shadow_random)
        shadow_random_prob_key: BACKTEST_THRESHOLDS中shadow_random概率键名

    Returns:
        (调整后方向, 是否应该开仓)
    """
    should_open = True
    if strategy_type == "shadow_reverse":
        direction = -direction
    elif strategy_type == "shadow_random":
        if np.random.random() < BACKTEST_THRESHOLDS[shadow_random_prob_key]:
            direction = 1 if np.random.random() < 0.5 else -1
        else:
            should_open = False
    return direction, should_open


def _check_other_state_block(bt: _BacktestState, strategy_type: str, blocked_types: tuple = ("main", "shadow_reverse")) -> bool:
    """Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐

    Args:
        bt: 回测状态
        strategy_type: 策略类型
        blocked_types: 在other状态下被阻止的策略类型

    Returns:
        True表示应该阻止开仓
    """
    return bt.current_state == "other" and strategy_type in blocked_types


def _check_risk_service_before_trade(
    symbol: str,
    direction: int,
    price: float,
    lots: int,
    signal_prefix: str,
    bar_time,
) -> bool:
    """P0-R11-05: RiskService.check_before_trade()调用模板

    Args:
        symbol: 合约ID
        direction: 方向 (1=BUY, -1=SELL)
        price: 价格
        lots: 手数
        signal_prefix: 信号ID前缀 (如 "BT_BOX_EXTREME")
        bar_time: bar时间戳

    Returns:
        True表示风控通过可以开仓，False表示被拦截
    """
    try:
        from ali2026v3_trading.risk.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _signal = {
            "symbol": symbol,
            "direction": "BUY" if direction > 0 else "SELL",
            "price": price,
            "volume": lots,
            "amount": price * lots,
            "is_valid": True,
            "action": "OPEN",
            "account_id": "backtest",
            "signal_id": f"{signal_prefix}_{symbol}_{int(bar_time.timestamp())}",
        }
        _chk = _rs.check_before_trade(_signal)
        if _chk.is_block:
            logger.debug("[P0-R11-05] %s open blocked by RiskService: %s", signal_prefix, _chk.reason)
            return False
        return True
    except (ImportError, AttributeError, TypeError, RuntimeError, KeyError, ValueError) as _rs_err:
        logger.warning("[P0-R11-05] %s check_before_trade failed, fail-safe block: %s", signal_prefix, _rs_err)
        return False


def _check_n1_quality_gates(
    bar: pd.Series,
    bt: _BacktestState,
    params: Dict[str, float],
) -> bool:
    """N1质量检查 + C-13安全检查链

    检查项：
    1. _spread_quality (N1): 0=不可开仓
    2. _option_metadata_quality (C-13): 0=不可开仓
    3. max_signals_per_window: 信号数上限
    4. shadow EV暂停检查

    Returns:
        True表示通过所有质量门控
    """
    # N1: spread_quality检查
    _sq = bar.get("_spread_quality", 0)
    try:
        _sq = int(_sq)
    except (TypeError, ValueError):
        _sq = 1
    if _sq == 0:
        return False

    # C-13: option_metadata_quality检查
    _omq = bar.get('_option_metadata_quality', 0)
    if isinstance(_omq, property):
        _omq = 0
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 0
    if _omq == 0:
        return False

    # 信号数上限检查
    max_signals = int(params.get("max_signals_per_window", 5))
    if bt.total_signals >= max_signals:
        return False

    # Shadow EV暂停检查
    try:
        from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine
        _sse = get_shadow_strategy_engine()
        if _sse and hasattr(_sse, 'is_absolute_ev_paused') and _sse.is_absolute_ev_paused():
            return False
    except (ImportError, AttributeError, TypeError):
        pass

    return True


def _execute_open_with_slippage(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    symbol: str,
    direction: int,
    price: float,
    lots: int,
    tp_ratio: float,
    sl_ratio: float,
    open_reason: str,
    spread_quality: Any = 0,
    extra_pos_fields: Optional[Dict[str, Any]] = None,
) -> None:
    """动态滑点+手续费+开仓执行

    Args:
        bt: 回测状态
        bar: 当前bar数据
        params: 参数字典
        symbol: 合约ID
        direction: 方向 (1/-1)
        price: 开仓价格
        lots: 手数
        tp_ratio: 止盈比率
        sl_ratio: 止损比率
        open_reason: 开仓原因
        spread_quality: 价差质量
        extra_pos_fields: 额外的持仓字段
    """
    sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
    sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

    # 动态滑点
    bid_ask = bar.get("bid_ask_spread", 0.0)
    slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_quality, bar=bar, params=params)
    slip_cost = price * slip_bps / 10000 * lots
    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))
    bt.equity -= (commission + slip_cost)

    pos_kwargs = dict(
        instrument_id=symbol,
        volume=direction * lots,
        open_price=price,
        open_time=bar.get("minute", pd.NaT),
        stop_profit_price=sp_price,
        stop_loss_price=sl_price,
        open_reason=open_reason,
        lots=lots,
        instrument_type=_infer_instrument_type(symbol),
    )
    if extra_pos_fields:
        pos_kwargs.update(extra_pos_fields)

    pos = _BacktestPosition(**pos_kwargs)
    bt.positions[symbol] = pos
    bt.total_signals += 1


def _check_eod_close(bar_time, hour_morning: int = 14, minute_morning: int = 55,
                     hour_night: int = 2, minute_night: int = 25) -> Tuple[bool, str]:
    """EOD平仓时间检查

    Returns:
        (是否应平仓, 平仓原因)
    """
    if hasattr(bar_time, 'hour'):
        _eod_h = bar_time.hour
        _eod_m = bar_time.minute
    else:
        _eod_ts = pd.Timestamp(bar_time)
        _eod_h = _eod_ts.hour
        _eod_m = _eod_ts.minute
    if _eod_h == hour_morning and _eod_m >= minute_morning:
        return True, "EOD"
    elif _eod_h == hour_night and _eod_m >= minute_night:
        return True, "EOD_NIGHT"
    return False, ""


def _close_position_with_pnl(bt: _BacktestState, sym: str, pos: _BacktestPosition,
                              bar_price: float, extra_callback: Any = None) -> None:
    """平仓并计算PnL

    Args:
        bt: 回测状态
        sym: 合约ID
        pos: 持仓
        bar_price: 当前价格
        extra_callback: 平仓后的额外回调（如更新inventory）
    """
    direction = 1 if pos.volume > 0 else -1
    if direction == 1:
        pnl = (bar_price - pos.open_price) * pos.lots * _get_contract_multiplier(sym)
    else:
        pnl = (pos.open_price - bar_price) * pos.lots * _get_contract_multiplier(sym)
    _safe_equity_add(bt, pnl)
    bt.total_trades += 1
    if extra_callback is not None:
        extra_callback(direction, pos)
    del bt.positions[sym]


# ============================================================================
# Box策略回测
# ============================================================================

def run_backtest_box_extreme(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_extreme",
) -> Dict[str, Any]:
    """箱体极值策略回测

    在other状态下，检测箱体边界极值，做反向操作：
    - 箱底极值（价格触及箱体下沿）→ 做多
    - 箱顶极值（价格触及箱体上沿）→ 做空

    影子策略：
    - shadow_reverse: 方向强制反转（箱底→做空，箱顶→做多）
    - shadow_random: 随机方向（50/50）

    V7.1决策频率机制：决策频率由K线周期 × state_confirm_bars自然决定，每Bar都执行。
    NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    box_threshold = params.get("box_detection_threshold", 0.03)
    box_min_bars = int(params.get("box_min_bars", 20))
    extreme_ratio = params.get("extreme_entry_ratio", 0.5)
    _sync_random_seed(42 if train else 24)

    from ali2026v3_trading.strategy.box_detector import BoxDetector, BoxStrategyParams
    _box_params = BoxStrategyParams(
        box_width_max_pct=box_threshold * 100.0,
        min_bounce_count=int(params.get("box_min_bounce_count", 2)),
    )
    _detector = BoxDetector(
        params=_box_params,
        lookback_bars=box_min_bars * 3,
        min_box_bars=box_min_bars,
        adx_threshold=params.get("box_adx_threshold", 25.0),
        bounce_tolerance_pct=params.get("box_bounce_tolerance_pct", 0.1),
    )

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        # P0-R11-06修复: 回测健康检查，CRITICAL时强制平仓并暂停新开仓
        if not _check_backtest_health(bt, params, bar_time):
            _handle_health_critical(bt, bar)
            continue

        high = bar.get("high", 0.0)
        low = bar.get("low", 0.0)
        close = bar.get("close", 0.0)
        symbol = bar.get("symbol", "")

        _detector.update_bar(high=high, low=low, close=close, timestamp=str(bar_time))

        for sym in list(bt.positions.keys()):
            if sym == symbol:
                _check_positions(bt, bar, params)

        _check_state_transition(bt, bar, params)

        # P0-R11-04修复: box_extreme回测路径添加决策跳帧
        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        box_profile = _detector.detect_box()
        if not box_profile.is_valid:
            continue

        box_high = box_profile.upper
        box_low = box_profile.lower
        box_range = box_high - box_low

        if box_range < close * box_threshold:
            continue

        if _check_safety(bt, bar_time, params) and len(bt.positions) < int(params.get("max_open_positions", 3)):
            position_in_box = (close - box_low) / box_range if box_range > 0 else 0.5

            is_box_bottom = position_in_box < (1 - extreme_ratio)
            is_box_top = position_in_box > extreme_ratio

            should_open = False
            direction = 0

            if is_box_bottom:
                direction = 1
                should_open = True
            elif is_box_top:
                direction = -1
                should_open = True

            if should_open:
                direction, should_open = _apply_shadow_direction(direction, strategy_type)

            # Q1修复: other状态下主策略/反向策略不开仓
            if should_open and _check_other_state_block(bt, strategy_type):
                should_open = False

            if should_open and direction != 0:
                # N1质量检查 + C-13安全检查链
                if not _check_n1_quality_gates(bar, bt, params):
                    continue

                price = close
                tp_ratio = params.get("close_take_profit_ratio", 2.0)
                sl_ratio = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                if lots <= 0:
                    continue

                # P0-R11-05修复: RiskService风控检查
                if not _check_risk_service_before_trade(symbol, direction, price, lots, "BT_BOX_EXTREME", bar_time):
                    continue

                _execute_open_with_slippage(
                    bt, bar, params, symbol, direction, price, lots,
                    tp_ratio, sl_ratio, "BOX_EXTREME",
                    spread_quality=bar.get("_spread_quality", 0),
                )

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )


def run_backtest_box_spring(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_spring",
) -> Dict[str, Any]:
    """箱体弹簧策略回测

    条件：
    - IV极低（低于阈值）
    - 近月期权
    - 价格在箱体内部
    → 预期波动率回归，买入期权做多波动率

    影子策略：
    - shadow_reverse: 方向强制反转（买CALL→买PUT）
    - shadow_random: 随机方向

    V7.1决策频率机制：决策频率由K线周期 × state_confirm_bars自然决定，每Bar都执行。
    NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    iv_threshold = params.get("spring_iv_threshold", 0.20)
    impulse_threshold = params.get("spring_impulse_threshold", 0.02)
    _sync_random_seed(42 if train else 24)

    from ali2026v3_trading.strategy.box_detector import BoxDetector, BoxStrategyParams
    _spring_box_params = BoxStrategyParams(
        box_width_max_pct=params.get("spring_box_width_max_pct", 5.0),
        min_bounce_count=int(params.get("spring_min_bounce_count", 2)),
    )
    _spring_detector = BoxDetector(
        params=_spring_box_params,
        lookback_bars=int(params.get("spring_lookback_bars", 60)),
        min_box_bars=int(params.get("spring_min_box_bars", 20)),
        adx_threshold=params.get("spring_adx_threshold", 25.0),
    )

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
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

        symbol = bar.get("symbol", "")
        close = bar.get("close", 0.0)
        iv = bar.get("iv", 0.0)
        high = bar.get("high", close)
        low = bar.get("low", close)

        _spring_detector.update_bar(high=high, low=low, close=close, timestamp=str(bar_time))

        for sym in list(bt.positions.keys()):
            if sym == symbol:
                _check_positions(bt, bar, params)

        _check_state_transition(bt, bar, params)

        # P0-R11-04修复: box_spring回测路径添加决策跳帧
        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        if _check_safety(bt, bar_time, params) and len(bt.positions) < int(params.get("max_open_positions", 3)):
            _spring_box = _spring_detector.detect_box()
            impulse = (high - low) / close if close > 0 else 0

            is_in_box = _spring_box.is_valid and _spring_box.lower <= close <= _spring_box.upper
            is_low_iv = iv > 0 and iv < iv_threshold
            is_impulse = impulse > impulse_threshold

            should_open = False
            direction = 0

            if is_in_box and is_low_iv and is_impulse:
                direction = 1
                should_open = True

                direction, should_open = _apply_shadow_direction(direction, strategy_type)

                # Q1修复: other状态下主策略/反向策略不开仓
                if should_open and _check_other_state_block(bt, strategy_type):
                    should_open = False

                if should_open and direction != 0:
                    # N1质量检查 + C-13安全检查链
                    if not _check_n1_quality_gates(bar, bt, params):
                        continue

                    price = close
                    tp_ratio = params.get("spring_stop_profit_ratio", 5.0)
                    sl_ratio = params.get("spring_max_loss_pct", 0.90)
                    lots = _compute_lots_with_risk_budget(
                        bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                        recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                    if lots <= 0:
                        continue

                    # P0-R11-05修复: RiskService风控检查
                    if not _check_risk_service_before_trade(symbol, direction, price, lots, "BT_BOX_SPRING", bar_time):
                        continue

                    _execute_open_with_slippage(
                        bt, bar, params, symbol, direction, price, lots,
                        tp_ratio, sl_ratio, "BOX_SPRING",
                        spread_quality=bar.get("_spread_quality", 0),
                    )

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )


# ============================================================================
# HFT策略回测
# ============================================================================


def run_backtest_arbitrage(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "arbitrage",
) -> Dict[str, Any]:
    """S5套利策略回测：微观结构价格偏离检测→快速反向开仓→均值回归平仓

    基于MicrostructureArbitrageDetector的逻辑：
      1. 计算当前价格相对公允价值(implied by imbalance+strength)的偏离
      2. 偏离>arb_deviation_threshold_bps时发出套利信号(反向开仓)
      3. 价格回归至arb_reversion_target_bps以内时平仓
      4. 强制时间止损arb_max_hold_minutes

    特点：收益来源集中(偏离→回归)、持仓时间短、方向总是反转。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    dev_threshold = params.get("arb_deviation_threshold_bps", 50.0)
    reversion_target = params.get("arb_reversion_target_bps", 30.0)
    min_confidence = params.get("arb_min_confidence", 0.6)
    max_hold = params.get("arb_max_hold_minutes", 15.0)
    _sync_random_seed(42 if train else 24)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
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

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    should_close_pos, close_reason_pos = _check_eod_close(bar_time)
                    if should_close_pos:
                        _close_position_with_pnl(bt, sym, pos, bar_price)
                        continue
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    tp_mult, sl_mult = _resolve_tp_sl(params, getattr(pos, 'open_reason', 'ARBITRAGE'))
                    _tp_price = entry * (1 + tp_mult * 0.01) if direction == 1 else entry * (1 - tp_mult * 0.01)
                    _sl_price = entry * (1 - sl_mult * 0.01) if direction == 1 else entry * (1 + sl_mult * 0.01)
                    if should_trigger_take_profit(bar_price, _tp_price, is_long=(direction == 1)) or should_trigger_stop_loss(bar_price, _sl_price, is_long=(direction == 1)) or hold_minutes >= max_hold:
                        _close_position_with_pnl(bt, sym, pos, bar_price)

        # P0-R11-04修复: arbitrage回测路径添加决策跳帧
        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        if _check_safety(bt, bar_time, params):
            imbalance = bar.get("imbalance", 0)
            strength = bar.get("strength", 0)
            confidence = min(abs(imbalance) * 2, 1.0)
            symbol = bar.get("symbol", "")

            if symbol not in bt.positions and confidence >= min_confidence:
                fair_value_shift_bps = imbalance * 100
                deviation_bps = abs(fair_value_shift_bps)
                if deviation_bps >= dev_threshold:
                    arb_direction = -1 if fair_value_shift_bps > 0 else 1
                    # R30-P0-06修复: S5/S6影子策略方向逻辑
                    if strategy_type == "shadow_reverse":
                        arb_direction = -arb_direction
                    elif strategy_type == "shadow_random":
                        if np.random.random() < BACKTEST_THRESHOLDS.get("shadow_random_open_prob", 0.5):
                            arb_direction = 1 if np.random.random() < 0.5 else -1
                        else:
                            continue
                    bar_price = bar.get("close", 0)
                    tp_mult, sl_mult = _resolve_tp_sl(params, "ARBITRAGE")
                    tp_price = bar_price * (1 + tp_mult * 0.01) if arb_direction == 1 else bar_price * (1 - tp_mult * 0.01)
                    sl_price = bar_price * (1 - sl_mult * 0.01) if arb_direction == 1 else bar_price * (1 + sl_mult * 0.01)
                    # R30-ARCH-01修复: S5开仓使用_BacktestPosition替代嵌套dict
                    arb_lots = params.get("lots_min", 1)
                    # P0-R11-05修复: arbitrage开仓路径调用RiskService.check_before_trade()
                    if not _check_risk_service_before_trade(symbol, arb_direction, bar_price, arb_lots, "BT_ARBITRAGE", bar_time):
                        continue
                    bt.positions[symbol] = _BacktestPosition(
                        instrument_id=symbol,
                        volume=arb_direction * arb_lots,
                        open_price=bar_price,
                        open_time=bar_time,
                        stop_profit_price=tp_price,
                        stop_loss_price=sl_price,
                        open_reason="ARBITRAGE",
                        lots=arb_lots,
                        open_state=bt.current_state,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={"total_trades": bt.total_trades},
    )


def run_backtest_market_making(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "market_making",
) -> Dict[str, Any]:
    """S6做市策略回测：双边挂单(买/卖)赚取价差+库存管理

    基于MarketMakerDefenseEngine的逻辑：
      1. 在每Bar以mid_price±spread_target_bps/2挂双边限价单
      2. 单边成交后形成库存，库存>mm_rebalance_threshold时对冲
      3. 库存绝对值>mm_max_inventory_lots时停止该方向挂单
      4. IOC单防御做市商扫单(mm_ioc_signal_threshold)

    特点：收益来源集中(价差收入)、方向不反转(库存管理而非方向性交易)。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    spread_target = params.get("mm_spread_target_bps", 5.0)
    max_inventory = int(params.get("mm_max_inventory_lots", 5))
    rebalance_threshold = int(params.get("mm_rebalance_threshold", 3))
    # P1-8修复：再平衡冷却期和摩擦成本
    rebalance_cooldown_bars = int(params.get("mm_rebalance_cooldown_bars", 5))
    rebalance_friction_bps = params.get("mm_rebalance_friction_bps", 2.0)
    _sync_random_seed(42 if train else 24)

    inventory = 0
    fill_count = 0
    last_rebalance_bar = -rebalance_cooldown_bars  # P1-8修复

    # P0-裂缝14修复v2：预加载熔断停牌事件并注入回测
    circuit_breaker_events = {}
    try:
        from ali2026v3_trading.param_pool._preprocess import validate_circuit_breaker_halts
        cb_result = validate_circuit_breaker_halts(bar_data)
        for evt in cb_result.get("halt_events", []):
            halt_idx = evt.get("bar_index", -1)
            if halt_idx >= 0:
                circuit_breaker_events[halt_idx] = evt
        if circuit_breaker_events:
            logger.info("[P0-裂缝14v2] 预加载%d个熔断停牌事件", len(circuit_breaker_events))
    except (ImportError, AttributeError) as e:
        logger.debug("[P0-裂缝14v2] 熔断事件加载失败: %s", e)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
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

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    should_close_pos, close_reason_pos = _check_eod_close(bar_time)
                    if should_close_pos:
                        def _update_inv(d, p, inv=inventory):
                            nonlocal inventory
                            if d == 1:
                                inventory -= p.lots
                            else:
                                inventory += p.lots
                        _close_position_with_pnl(bt, sym, pos, bar_price, extra_callback=_update_inv)
                        continue
                    tp_mult, sl_mult = _resolve_tp_sl(params, getattr(pos, 'open_reason', 'MARKET_MAKING'))
                    time_stop = _resolve_time_stop(params, getattr(pos, 'open_reason', 'MARKET_MAKING'), bt.current_state)
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    _tp_price = entry * (1 + tp_mult * 0.01) if direction == 1 else entry * (1 - tp_mult * 0.01)
                    _sl_price = entry * (1 - sl_mult * 0.01) if direction == 1 else entry * (1 + sl_mult * 0.01)
                    if should_trigger_take_profit(bar_price, _tp_price, is_long=(direction == 1)) or should_trigger_stop_loss(bar_price, _sl_price, is_long=(direction == 1)) or hold_minutes >= time_stop:
                        def _update_inv2(d, p, inv=inventory):
                            nonlocal inventory
                            if d == 1:
                                inventory -= p.lots
                            else:
                                inventory += p.lots
                        _close_position_with_pnl(bt, sym, pos, bar_price, extra_callback=_update_inv2)

        # P0-裂缝14修复v2：检查当前Bar是否为熔断停牌事件
        if idx in circuit_breaker_events:
            cb_evt = circuit_breaker_events[idx]
            halt_bars = cb_evt.get("halt_duration_bars", 5)
            bt.circuit_breaker_until = idx + halt_bars
            bt.circuit_breaker_events.append({
                'trigger_bar': idx,
                'halt_bars': halt_bars,
                'resume_bar': idx + halt_bars,
                'equity_at_trigger': bt.equity,
            })
            logger.info(
                "[P0-裂缝14v2] Bar#%d触发熔断停牌, 持续%d根Bar",
                idx, halt_bars,
            )

        if _check_safety(bt, bar_time, params):
            bar_price = bar.get("close", 0)
            symbol = bar.get("symbol", "")
            mid = bar_price
            bid_price = mid * (1 - spread_target * 0.0001)
            ask_price = mid * (1 + spread_target * 0.0001)
            imbalance = bar.get("imbalance", 0)

            if abs(imbalance) > 0.3:
                # R30-P0-06修复: S6做市策略影子逻辑
                _skip_rebalance = False
                if strategy_type == "shadow_random":
                    if np.random.random() >= BACKTEST_THRESHOLDS.get("shadow_random_open_prob", 0.5):
                        _skip_rebalance = True
                _is_shadow_reverse = (strategy_type == "shadow_reverse")
                # P1-8修复：再平衡冷却期检查
                in_cooldown = (idx - last_rebalance_bar) < rebalance_cooldown_bars
                if not in_cooldown and not _skip_rebalance and inventory > rebalance_threshold and symbol not in bt.positions:
                    # P0-R11-05修复: market_making再平衡开仓路径调用RiskService.check_before_trade()
                    _mm_dir_1 = 1 if _is_shadow_reverse else -1
                    if not _check_risk_service_before_trade(symbol, _mm_dir_1, bar_price, 1, "BT_MM_REBALANCE", bar_time):
                        continue
                    # P1-8修复：扣除再平衡摩擦成本
                    friction_cost = bar_price * rebalance_friction_bps / 10000
                    bt.equity -= friction_cost
                    if _is_shadow_reverse:
                        _vol, _reason = 1, "MM_REBALANCE_BUY_REVERSED"
                        _sp = bar_price * (1 + params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 - params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory += 1
                    else:
                        _vol, _reason = -1, "MM_REBALANCE_SELL"
                        _sp = bar_price * (1 - params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 + params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory -= 1
                    bt.positions[symbol] = _BacktestPosition(
                        instrument_id=symbol,
                        volume=_vol,
                        open_price=bar_price,
                        open_time=bar_time,
                        stop_profit_price=_sp,
                        stop_loss_price=_sl,
                        open_reason=_reason,
                        lots=1,
                        open_state=bt.current_state,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    last_rebalance_bar = idx  # P1-8修复：记录再平衡Bar
                    bt.total_signals += 1
                    fill_count += 1
                elif not in_cooldown and not _skip_rebalance and inventory < -rebalance_threshold and symbol not in bt.positions:
                    # P0-R11-05修复: market_making再平衡开仓路径调用RiskService.check_before_trade()
                    _mm_dir_2 = -1 if _is_shadow_reverse else 1
                    if not _check_risk_service_before_trade(symbol, _mm_dir_2, bar_price, 1, "BT_MM_REBALANCE2", bar_time):
                        continue
                    # P1-8修复：扣除再平衡摩擦成本
                    friction_cost = bar_price * rebalance_friction_bps / 10000
                    bt.equity -= friction_cost
                    if _is_shadow_reverse:
                        _vol, _reason = -1, "MM_REBALANCE_SELL_REVERSED"
                        _sp = bar_price * (1 - params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 + params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory -= 1
                    else:
                        _vol, _reason = 1, "MM_REBALANCE_BUY"
                        _sp = bar_price * (1 + params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 - params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory += 1
                    bt.positions[symbol] = _BacktestPosition(
                        instrument_id=symbol,
                        volume=_vol,
                        open_price=bar_price,
                        open_time=bar_time,
                        stop_profit_price=_sp,
                        stop_loss_price=_sl,
                        open_reason=_reason,
                        lots=1,
                        open_state=bt.current_state,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    last_rebalance_bar = idx  # P1-8修复：记录再平衡Bar
                    bt.total_signals += 1
                    fill_count += 1

            if abs(inventory) < max_inventory:
                # P1-裂缝4修复v2：改进队列位置估计
                spread_bps_val = abs(ask_price - bid_price) / max(mid, 1e-10) * 10000
                volume_at_price = bar.get("volume", 1)
                spread_ticks = max(1, spread_bps_val / max(float(bar.get("tick_size_bps", 1.0)), 0.1))
                volume_competitors = max(1, int(volume_at_price / max(float(bar.get("avg_trade_size", 1.0)), 1.0)))
                estimated_queue_pos = max(1, int(spread_ticks + volume_competitors * 0.5))
                fill_prob = 1.0 / (1.0 + estimated_queue_pos)
                buy_fill = np.random.random() < fill_prob
                sell_fill = np.random.random() < fill_prob
                if buy_fill or sell_fill:
                    spread_pnl = (ask_price - bid_price) * 0.1
                    _safe_equity_add(bt, spread_pnl)
                    fill_count += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "total_trades": bt.total_trades,
            "fill_count": fill_count,
            "final_inventory": inventory,
        },
    )


def run_backtest_divergence(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "divergence",
) -> Dict[str, Any]:
    """S7背离反转策略回测：三层背离检测→反转信号→开仓→止盈/止损/时间止损

    基于DivergenceReversalModule的逻辑：
      1. L1: 当月期货创新高/低但季月未跟随 → 跨期背离
      2. L2: 远月期权权利金集体未创新高/低 → 集体背离
      3. L3: 当月期权近实值权利金未创新高/低 → 近实值背离
      4. 三层加权合成综合信号，超过signal_threshold触发
      5. 信号方向决定多空，position_scale缩放仓位
      6. take_profit_ratio/stop_loss_ratio/hard_time_stop_min管理持仓

    特点：收益来源集中(背离→反转)、持仓时间中等(60min硬止损)、方向取决于背离类型。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)
    bt = _BacktestState()
    signal_threshold = params.get("divergence_signal_threshold", 0.15)
    take_profit_ratio = params.get("divergence_take_profit_ratio", 1.8)
    stop_loss_ratio = params.get("divergence_stop_loss_ratio", 0.3)
    max_risk_ratio = params.get("divergence_max_risk_ratio", 0.5)
    hard_time_stop_min = params.get("divergence_hard_time_stop_min", 60.0)
    cooldown_bars = int(params.get("divergence_cooldown_bars", 10))
    position_scale = params.get("divergence_position_scale", 0.3)
    _sync_random_seed(42 if train else 24)

    cooldown_counter = 0

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            continue
        current_date = str(bar_time.date())
        _reset_daily(bt, current_date)

        if cooldown_counter > 0:
            cooldown_counter -= 1

        if not _check_backtest_health(bt, params, bar_time):
            _handle_health_critical(bt, bar)
            continue

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    should_close_pos, close_reason_pos = _check_eod_close(bar_time)
                    if should_close_pos:
                        _close_position_with_pnl(bt, sym, pos, bar_price)
                        continue
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    tp_price = entry * (1 + take_profit_ratio * 0.01) if direction == 1 else entry * (1 - take_profit_ratio * 0.01)
                    sl_price = entry * (1 - stop_loss_ratio * 0.01) if direction == 1 else entry * (1 + stop_loss_ratio * 0.01)
                    if should_trigger_take_profit(bar_price, tp_price, is_long=(direction == 1)) or should_trigger_stop_loss(bar_price, sl_price, is_long=(direction == 1)) or hold_minutes >= hard_time_stop_min:
                        _close_position_with_pnl(bt, sym, pos, bar_price)

        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        if _check_safety(bt, bar_time, params):
            strength = bar.get("strength", 0)
            imbalance = bar.get("imbalance", 0)
            divergence_signal = strength * imbalance
            symbol = bar.get("symbol", "")

            if symbol not in bt.positions and cooldown_counter == 0 and abs(divergence_signal) >= signal_threshold:
                div_direction = 1 if divergence_signal > 0 else -1
                if strategy_type == "shadow_reverse":
                    div_direction = -div_direction
                elif strategy_type == "shadow_random":
                    if np.random.random() < BACKTEST_THRESHOLDS.get("shadow_random_open_prob", 0.5):
                        div_direction = 1 if np.random.random() < 0.5 else -1
                    else:
                        continue
                bar_price = bar.get("close", 0)
                tp_price = bar_price * (1 + take_profit_ratio * 0.01) if div_direction == 1 else bar_price * (1 - take_profit_ratio * 0.01)
                sl_price = bar_price * (1 - stop_loss_ratio * 0.01) if div_direction == 1 else bar_price * (1 + stop_loss_ratio * 0.01)
                div_lots = max(1, int(params.get("lots_min", 1) * position_scale))
                if not _check_risk_service_before_trade(symbol, div_direction, bar_price, div_lots, "BT_DIVERGENCE", bar_time):
                    continue
                bt.positions[symbol] = _BacktestPosition(
                    instrument_id=symbol,
                    volume=div_direction * div_lots,
                    open_price=bar_price,
                    open_time=bar_time,
                    stop_profit_price=tp_price,
                    stop_loss_price=sl_price,
                    open_reason="CORRECT_DIVERGENCE" if div_direction == 1 else "INCORRECT_REVERSAL",
                    lots=div_lots,
                    open_state=bt.current_state,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.total_signals += 1
                cooldown_counter = cooldown_bars

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={"total_trades": bt.total_trades},
    )


def __getattr__(name):
    _HFT_NAMES = {
        'run_backtest_hft', 'run_backtest_hft_with_disturbance',
        'run_backtest_hft_tick_fidelity', 'run_backtest_multiscale',
        'run_backtest_with_cycle_resonance', '_run_backtest_box_extreme',
        '_run_backtest_hft', '_scale_params_with_bar_interval',
        '_compute_fill_probability',
    }
    if name in _HFT_NAMES:
        from ali2026v3_trading.param_pool.backtest import _backtest_runners_hft
        return getattr(_backtest_runners_hft, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
