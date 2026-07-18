# [M1-101] 回测状态管理
# MODULE_ID: M1-159
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
#!/usr/bin/env python3
"""
回测状态与数据类模块 — 从backtest_runner_base.py拆分(P1-5)

Phase 1 轮次4: 定价/识别类函数已迁移至 backtest_pricing.py，本文件保留:
- 数据类: _PendingOrder, _BacktestPosition, _ClosedTrade, _BacktestState
- 懒加载单例: _get_life_estimator, _get_cascade_judge_module
- 交易逻辑: _resolve_tp_sl, _resolve_time_stop, _is_consecutive_loss_paused,
            _backtest_order_split, _safe_equity_add, _update_mtm_equity,
            _check_intra_bar_stop_loss, _check_two_stage_stop
- Re-export: 从 backtest_pricing.py 重新导出定价/识别函数（保持下游兼容）
"""
from __future__ import annotations

from infra._helpers import get_logger  # R9-5
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from param_pool.backtest.backtest_config import (
    INITIAL_EQUITY,
    _EQUITY_MIN,
    _EQUITY_MAX,
)
from infra.resilience import safe_divide  # R5-5
from infra.commission_utils import REASON_MULTIPLIERS
from param_pool._param_grids import _compute_dynamic_slippage_bps  # noqa: F401
from infra.commission_utils import compute_commission as _compute_commission  # noqa: F401 re-export

# Re-export from backtest_pricing.py (保持下游导入路径兼容)
from param_pool.backtest.backtest_config import (
    CASCADE_SLIPPAGE_MULTIPLIER,
    CASCADE_SLIPPAGE_CAP_BPS,
)
from param_pool.backtest.backtest_config import (  # noqa: F401
    _infer_contract_type,
    _infer_exchange_id,
    _infer_exchange_from_id,
    _infer_instrument_type,
    _calculate_limit_prices,
    calc_trade_fee,
    _get_contract_multiplier,
    _infer_liquidity_tier,
    _get_expiry_slippage_multiplier,
    _compute_cascade_slippage_bps,
    _compute_option_mtm_price,
)

logger = get_logger(__name__)  # R9-5

_LIFE_ESTIMATOR = None
_LIFE_ESTIMATOR_LOCK = threading.Lock()

_cascade_judge_lock = threading.Lock()
_CascadeJudge = None
_adapt_backtest_result = None


def _get_life_estimator(params: Optional[Dict[str, Any]] = None):
    """懒加载 BayesianShrinkageLifeEstimator 单例"""
    global _LIFE_ESTIMATOR
    with _LIFE_ESTIMATOR_LOCK:
        if _LIFE_ESTIMATOR is None:
            try:
                from precompute._quantification_core import BayesianShrinkageLifeEstimator
                _LIFE_ESTIMATOR = BayesianShrinkageLifeEstimator(params=params)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logger.warning("BayesianShrinkageLifeEstimator not available: %s", e)
        return _LIFE_ESTIMATOR


def _get_cascade_judge_module():
    """懒加载CascadeJudge模块"""
    global _CascadeJudge, _adapt_backtest_result
    with _cascade_judge_lock:
        if _CascadeJudge is None:
            from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
            _CascadeJudge = CascadeJudge
            _adapt_backtest_result = adapt_backtest_result
    return _CascadeJudge, _adapt_backtest_result










def _get_reason_tp_sl_from_position_service(open_reason: str) -> Optional[Tuple[float, float]]:
    """优先复用PositionService的reason级TP/SL默认值"""
    try:
        from position.position_service import PositionService
        defaults = dict(getattr(PositionService, 'TP_SL_REASON_DEFAULTS', {}) or {})
        if open_reason in defaults:
            tp_ratio, sl_ratio = defaults[open_reason]
            tp_val = float(tp_ratio)
            sl_val = float(sl_ratio)
            if tp_val > 0 and 0 < sl_val < 1:
                return tp_val, sl_val
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.debug("[task_scheduler] PositionService TP/SL复用失败: %s", e)
    return None


def _resolve_tp_sl(params: Dict[str, float], open_reason: str) -> Tuple[float, float]:
    linked = _get_reason_tp_sl_from_position_service(open_reason)
    if linked is not None:
        return linked
    base_tp = params.get("close_take_profit_ratio", 1.8)
    base_sl = params.get("close_stop_loss_ratio", 0.3)
    mult = REASON_MULTIPLIERS.get(open_reason, {"tp_mult": 1.0, "sl_mult": 1.0})
    return (base_tp * mult["tp_mult"], base_sl * mult["sl_mult"])


def _resolve_time_stop_hard(params: Dict[str, float], open_reason: str) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟）"""
    from param_pool._param_defaults import _REASON_TIME_STOP_SOURCE
    reason_key = str(open_reason or "").upper()
    param_key, unit, fallback = _REASON_TIME_STOP_SOURCE.get(
        reason_key, ("resonance_hard_time_stop_min", "min", 5.0)
    )
    raw_val = float(params.get(param_key, fallback) or fallback)
    if unit == "ms":
        base_time = raw_val / 60000.0
    elif unit == "sec":
        base_time = raw_val / 60.0
    else:
        base_time = raw_val
    mult = REASON_MULTIPLIERS.get(open_reason, {"time_mult": 1.0})
    return base_time * mult["time_mult"]


def _resolve_time_stop(params: Dict[str, float], open_reason: str, current_state: str = None) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟），支持行情寿命字典动态调整"""
    if current_state is not None:
        estimator = _get_life_estimator()
        if estimator is not None and hasattr(estimator, '_life_dict') and estimator._life_dict:
            life = estimator.get_life_expectancy(current_state)
            if life is not None and life.is_valid():
                life_stop = life.duration.get('p75', 0)
                if life_stop > 0:
                    hard_stop = _resolve_time_stop_hard(params, open_reason)
                    return max(1.0, min(hard_stop, life_stop))
    hard_stop = _resolve_time_stop_hard(params, open_reason)
    return max(1.0, hard_stop)


def _is_consecutive_loss_paused(bt: _BacktestState, params: Dict[str, float], bar_time: pd.Timestamp) -> bool:
    """连亏暂停"""
    max_losses = int(params.get("max_consecutive_losses", 0) or 0)
    if max_losses <= 0:
        return False

    pause_until = getattr(bt, 'consecutive_loss_pause_until', None)
    if pause_until is not None:
        if bar_time < pause_until:
            return True
        bt.consecutive_loss_pause_until = None

    streak = 0
    for pnl in reversed(bt.recent_pnls):
        if pnl < 0:
            streak += 1
        else:
            break
    bt.consecutive_loss_streak = streak

    if streak >= max_losses:
        pause_sec = float(params.get("consecutive_loss_pause_sec", 1800.0) or 1800.0)
        bt.consecutive_loss_pause_until = bar_time + pd.Timedelta(seconds=max(1.0, pause_sec))
        logger.warning(
            "[BACKTEST] 连亏暂停触发: streak=%d threshold=%d pause_until=%s",
            streak, max_losses, bt.consecutive_loss_pause_until,
        )
        return True
    return False


def _backtest_order_split(lots: int, max_sub_order_lots: int = 5) -> list:
    if lots <= max_sub_order_lots:
        return [lots]
    splits = []
    remaining = lots
    while remaining > 0:
        sub = min(remaining, max_sub_order_lots)
        splits.append(sub)
        remaining -= sub
    return splits


def _safe_equity_add(bt: _BacktestState, delta: float) -> None:
    """P0-R12-NP-03修复: 安全equity累加，Kahan补偿算法"""
    y = delta - bt._kahan_c
    t = bt.equity + y
    bt._kahan_c = (t - bt.equity) - y
    new_equity = t
    if not np.isfinite(new_equity):
        logger.warning("[BACKTEST] equity overflow/NaN detected: equity=%.2f delta=%.2f, clamping", bt.equity, delta)
        new_equity = max(_EQUITY_MIN, min(_EQUITY_MAX, bt.equity))
        bt._kahan_c = 0.0
    bt.equity = new_equity


def _update_mtm_equity(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> None:
    """BF-05: 逐Bar更新MTM权益"""
    if not params.get("enable_mtm_equity", False):
        return
    mtm = bt.equity
    for sym, pos in bt.positions.items():
        if getattr(pos, 'instrument_type', 'future') in ('option_buyer', 'option_seller'):
            if params.get("use_option_bs_pricing", False):
                spot = bar.get("underlying_price", 0.0)
                strike = getattr(pos, 'strike_price', 0.0)
                days = getattr(pos, 'remaining_days', 0)
                iv = getattr(pos, 'open_iv', 0.0)
                opt_type = getattr(pos, 'option_type', 'call')
                theo = _compute_option_mtm_price(spot, strike, days, iv, opt_type)
                if theo > 0:
                    mtm += (theo - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
            else:
                current_price = bar.get("close", pos.open_price)
                mtm += (current_price - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
        else:
            current_price = bar.get("close", pos.open_price)
            mtm += (current_price - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
    bt.mtm_equity = mtm
    bt.mtm_equity_curve.append(mtm)
    if mtm > bt.mtm_peak_equity:
        bt.mtm_peak_equity = mtm
    dd = (bt.mtm_peak_equity - mtm) / bt.mtm_peak_equity if bt.mtm_peak_equity > 0 else 0.0
        # [P2-02] 计划统一到infra/shared_utils.py的max_drawdown函数
    if dd > bt.mtm_max_drawdown:
        bt.mtm_max_drawdown = dd


def _check_intra_bar_stop_loss(
    pos: _BacktestPosition,
    bar: pd.Series,
    prev_bar: Optional[pd.Series],
    params: Dict[str, float],
) -> Tuple[bool, str, float]:
    """BF-01: Bar内止损路径推断 + 跨Bar跳空处理"""
    if pos is None:
        return (False, "", 0.0)

    bar_open = bar.get("open", 0.0)
    bar_high = bar.get("high", 0.0)
    bar_low = bar.get("low", 0.0)
    bar_close = bar.get("close", 0.0)

    if bar_open <= 0 or bar_high <= 0 or bar_low <= 0:
        logger.warning("[C-3] OHLC字段缺失: open=%.4f high=%.4f low=%.4f", bar_open, bar_high, bar_low)
        return (False, "", 0.0)

    enable_gap = params.get("enable_gap_handling", False)
    tp_price = pos.stop_profit_price
    sl_price = pos.stop_loss_price

    if pos.volume > 0:
        prev_close = prev_bar.get("close", bar_open) if prev_bar is not None and enable_gap else bar_open
        if enable_gap and prev_close > 0:
            if bar_open <= sl_price:
                return (True, "StopLoss_GapDown", bar_open)
            if bar_open >= tp_price:
                return (True, "StopProfit_GapUp", bar_open)
        is_bullish = bar_close >= bar_open
        if is_bullish:
            if bar_low <= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
            if bar_high >= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
        else:
            if bar_high >= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
            if bar_low <= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
    elif pos.volume < 0:
        prev_close = prev_bar.get("close", bar_open) if prev_bar is not None and enable_gap else bar_open
        if enable_gap and prev_close > 0:
            if bar_open >= sl_price:
                return (True, "StopLoss_GapUp", bar_open)
            if bar_open <= tp_price:
                return (True, "StopProfit_GapDown", bar_open)
        is_bullish = bar_close >= bar_open
        if is_bullish:
            if bar_low <= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
            if bar_high >= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
        else:
            if bar_high >= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
            if bar_low <= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)

    return (False, "", 0.0)


def _check_two_stage_stop(
    pos: _BacktestPosition,
    price: float,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
) -> bool:
    _bar_period = float(params.get("bar_period", 1.0))
    stage1_min_minutes = params.get("stage1_min_minutes", 90.0) * _bar_period
    stage1_profit_threshold = params.get("stage1_profit_threshold", 0.002)
    stage2_slope_window = int(params.get("stage2_slope_window", 10))
    # P1-裂缝42修复: stage2_slope_threshold默认值从0.0改为-0.01，允许微小下降但不允许明显衰减
    stage2_slope_threshold = params.get("stage2_slope_threshold", -0.01)

    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct

    if not pos.stage1_passed:
        if hold_minutes >= stage1_min_minutes and pos.max_float_profit >= stage1_profit_threshold:
            pos.stage1_passed = True

    if not pos.stage1_passed:
        return False

    should_stop = False
    if stage2_slope_window >= 2 and len(pos.profit_history) >= stage2_slope_window:
        window = pos.profit_history[-stage2_slope_window:]
        _bar_period_min = float(params.get("bar_period", 1.0))
        x = np.arange(len(window), dtype=np.float64)
        y = np.array(window, dtype=np.float64)
        n = len(x)
        if n > 1:
            x_mean = x.mean()
            y_mean = y.mean()
            numerator = np.sum((x - x_mean) * (y - y_mean))
            denominator = np.sum((x - x_mean) ** 2)
            if denominator > 0:
                slope = safe_divide(numerator, denominator, default=0.0) / _bar_period_min  # R5-5
            else:
                slope = 0.0
        else:
            slope = 0.0
        logger.debug("[TwoStageStop] slope=%.10f threshold=%.10f window_len=%d history_len=%d",
                     slope, stage2_slope_threshold, len(window), len(pos.profit_history))
        if slope < stage2_slope_threshold - 1e-10:
            should_stop = True

    pos.profit_history.append(float_pnl_pct)
    if isinstance(pos.profit_history, list) and len(pos.profit_history) > pos._PROFIT_HISTORY_MAXLEN:
        pos.profit_history = pos.profit_history[-1000:]
    if pos.last_check_time is not None and pos.last_check_time == bar_time:
        pass
    pos.last_check_time = bar_time

    return should_stop

# ── Position Manager (merged from backtest_position_manager.py on 2026-06-12) ──

"""
回测持仓管理器 — 从backtest_runner_base.py拆分
职责: 开仓5阶段分解、持仓检查(止盈止损/时间止损/EOD/状态反转)
"""

import logging
from typing import Dict, Optional, Any, Tuple

import numpy as np
import pandas as pd

from infra.shared_utils import STOP_PRICE_RTOL as _STOP_PRICE_RTOL


def try_open_check_gates(bt, bar, params, bar_time, strategy_type,
                         _is_consecutive_loss_paused, _STATE_REASON_MAP,
                         BACKTEST_THRESHOLDS, safe_price_check):
    if hasattr(bt, 'new_open_blocked') and bt.new_open_blocked:
        logging.info("[P0-R9-02] 开仓被质量检查阻断，跳过")
        return None
    if pd.isna(bar_time):
        logging.warning("[R22-TIME-03] bar缺少minute字段，跳过开仓判断")
        return None
    cooldown = params.get("signal_cooldown_sec", 60.0)
    if bt.last_signal_time is not None and cooldown > 0:
        elapsed = (bar_time - bt.last_signal_time).total_seconds()
        if elapsed < cooldown:
            return None
    if _is_consecutive_loss_paused(bt, params, bar_time):
        return None
    max_signals = int(params.get("max_signals_per_window", 5))
    if bt.total_signals >= max_signals:
        return None
    reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
    symbol = bar.get("symbol", "unknown")
    price = bar.get("close", 0.0)
    if not safe_price_check(price):
        return None
    strength = bar.get("strength", 0.0)
    if strategy_type == "main" and reason == "CORRECT_RESONANCE" and strength < BACKTEST_THRESHOLDS["correct_resonance_min_strength"]:
        return None
    return reason, symbol, price


def try_open_compute_lots(bt, params, price, sl_ratio, reason, bar,
                          _compute_lots_with_risk_budget):
    lots = _compute_lots_with_risk_budget(
        bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
        recent_pnls=bt.recent_pnls, current_positions=bt.positions)
    if lots <= 0:
        return 0
    try:
        from risk.risk_service import get_risk_service
        _rs_p22 = get_risk_service(scope_id='backtest')
        _tvf_lots = _rs_p22.compute_mode_position_size(
            equity=bt.equity, entry_price=price, stop_price=price * (1 - sl_ratio),
            sortino_ratio=getattr(bt, 'sortino_ratio', 0.0),
            calmar_ratio=getattr(bt, 'calmar_ratio', 0.0),
            sharpe_ratio=getattr(bt, 'sharpe_ratio', 0.0),
            ofi=bar.get('ofi', 0.0), cvd_divergence=bar.get('cvd_divergence', 0.0),
            smart_money_flow=bar.get('smart_money_flow', 0.0),
            delta=bar.get('delta', 0.0), gamma=bar.get('gamma', 0.0),
            theta=bar.get('theta', 0.0), vega=bar.get('vega', 0.0),
        )
        if _tvf_lots > 0 and lots > 0:
            _tvf_factor = _tvf_lots / lots
            lots = max(1, round(lots * _tvf_factor))
    except (ValueError, KeyError, TypeError, AttributeError, RuntimeError) as _p22_err:
        logging.warning("[R16-P0-007] TVF仓位计算失败，使用风险预算回退: %s", _p22_err)
    return lots


def try_open_risk_checks(bt, params, symbol, price, lots, direction, bar, bar_time,
                         _position_volume, _open_positions):
    try:
        from risk.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _signal = {
            "symbol": symbol, "direction": "BUY" if direction > 0 else "SELL",
            "price": price, "volume": lots, "amount": price * lots,
            "is_valid": True, "action": "OPEN", "account_id": "backtest",
            "signal_id": f"BT_{symbol}_{int(bar_time.timestamp())}",
        }
        _chk = _rs.check_before_trade(_signal)
        if _chk.is_block:
            logging.debug("[P-09] _try_open blocked by RiskService: %s", _chk.reason)
            return False
    except (ImportError, AttributeError, ValueError, RuntimeError) as _rs_err:
        logging.warning("[P2-R3-D-19] RiskService check_before_trade failed, fail-safe block: %s", _rs_err)
        return False
    try:
        from risk.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _position_data = {"instrument_id": bar.get("symbol", ""), "volume": _position_volume}
        _compliance = _rs.check_regulatory_compliance(_position_data)
        if not _compliance.get('compliant', True):
            logging.warning("[P-01] _try_open blocked by regulatory compliance: %s", _compliance.get('violations', []))
            return False
    except (ImportError, AttributeError, ValueError, KeyError) as _compliance_err:
        logging.warning("[P2-R3-D-19] regulatory compliance check failed, fail-safe block: %s", _compliance_err)
        return False
    try:
        from risk.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _use_span = params.get("use_span_margin", False)
        if _use_span:
            try:
                from risk_service import SimplifiedSPAN
                _span = SimplifiedSPAN()
                _required_margin = _span.calc_margin([{"instrument_id": symbol, "price": price, "quantity": lots, "delta": 0.5}])
            except (ImportError, ValueError, KeyError, AttributeError) as _span_err:
                logging.debug("[R3-L2] SPAN保证金计算失败(使用简单公式): %s", _span_err)
                _required_margin = price * lots * params.get("margin_ratio", 0.1)
        else:
            _required_margin = price * lots * params.get("margin_ratio", 0.1)
        _capital = _rs.check_capital_sufficiency(bt.equity, _required_margin, _open_positions)
        if not _capital.get('sufficient', False):
            logging.warning("[P-02] _try_open blocked by capital sufficiency: %s", _capital.get('reason', ''))
            return False
    except (ImportError, AttributeError, ValueError, KeyError) as _capital_err:
        logging.warning("[P2-R3-D-19] capital sufficiency check failed, fail-safe block: %s", _capital_err)
        return False
    try:
        from risk.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _exchange_status = _rs.check_exchange_status()
        if _exchange_status.get('status') != 'OPEN':
            logging.warning("[P-03] _try_open blocked by exchange status: %s", _exchange_status.get('status', 'UNKNOWN'))
            return False
    except (ImportError, AttributeError, ValueError, KeyError) as _exchange_err:
        logging.warning("[P2-R3-D-19] exchange status check failed, fail-safe block: %s", _exchange_err)
        return False
    return True


def try_open_quality_gates(bar, params,
                           _infer_instrument_type):
    _sq = bar.get('_spread_quality', 0)
    if isinstance(_sq, property):
        _sq = 1
    try:
        _sq = int(_sq)
    except (TypeError, ValueError):
        _sq = 1
    if _sq == 0:
        logging.warning("[BACKTEST] _try_open blocked: _spread_quality=0, spread data unreliable")
        return False
    try:
        from strategy.shadow_strategy_facade import get_shadow_strategy_engine
        _sse = get_shadow_strategy_engine()
        if _sse and hasattr(_sse, 'is_absolute_ev_paused') and _sse.is_absolute_ev_paused():
            logging.warning("[BACKTEST] _try_open blocked: absolute EV paused, no new opens allowed")
            return False
    except (ImportError, AttributeError, ValueError, RuntimeError) as _sse_e:
        logging.warning("[BACKTEST] _try_open shadow engine check failed, fail-safe block: %s", _sse_e)
        return False
    _omq = bar.get('_option_metadata_quality', 0)
    if isinstance(_omq, property):
        _omq = 0
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 0
    if _omq == 0:
        _instr_type = bar.get('instrument_type', 'future')
        if _instr_type in ('option_buyer', 'option_seller'):
            logging.warning("[P-39] _try_open blocked: _option_metadata_quality=0, option metadata incomplete")
            return False
    return True


def try_open_execute(bt, bar, params, symbol, price, lots, direction, reason,
                     bar_time, strategy_type, tp_ratio, sl_ratio,
                     _infer_instrument_type, _compute_fill_quantity,
                     _simulate_limit_order_queue, _backtest_order_split,
                     _compute_dynamic_slippage_bps, _compute_market_impact_v2,
                     _compute_commission, _get_contract_multiplier,
                     _infer_exchange_id, _infer_exchange_from_id,
                     _bt_capture_snapshot, _PendingOrder, _BacktestPosition,
                     compute_execution_delay_slippage_bps,
                     ENABLE_QUEUE_SIMULATION):
    _instrument_type_key = _infer_instrument_type(symbol)
    _instrument_participation_rate = params.get("max_participation_rate", 1.0)
    if _instrument_type_key in ('option_buyer', 'option_seller'):
        _instrument_participation_rate = min(_instrument_participation_rate, params.get("option_max_participation_rate", 0.10))
    elif _instrument_type_key == 'future':
        _instrument_participation_rate = min(_instrument_participation_rate, params.get("future_max_participation_rate", 0.15))
    _adjusted_params = dict(params)
    _adjusted_params["max_participation_rate"] = _instrument_participation_rate

    actual_lots = _compute_fill_quantity(lots, bar, _adjusted_params)

    _order_type = params.get("default_order_type", "taker")
    if _order_type == "maker" and params.get("enable_queue_simulation", ENABLE_QUEUE_SIMULATION):
        _queue_result = _simulate_limit_order_queue(
            order_price=price, current_price=price, bar=bar,
            order_lots=actual_lots, enable_queue=True
        )
        if not _queue_result["filled"]:
            logging.debug("[BF-P1-02] 限价单排队未成交: %s", symbol)
            return
        if _queue_result["fill_lots"] < actual_lots:
            actual_lots = _queue_result["fill_lots"]
    if actual_lots <= 0:
        if params.get("execution_model", "standard") == "institutional" and lots > 0:
            if len(bt.pending_orders) < 100:
                bt.pending_orders.append(_PendingOrder(
                    symbol=symbol, order_type="open", volume=direction * lots,
                    lots=lots, reason=reason, signal_bar_idx=bt.bar_idx,
                    signal_price=price, tp_ratio=tp_ratio, sl_ratio=sl_ratio,
                    params_snapshot=dict(params), created_at_bar=bt.bar_idx,
                    fee_type=params.get("default_order_type", "taker"), retry_count=0,
                ))
        return
    _fill_ratio = actual_lots / lots if lots > 0 else 1.0
    if actual_lots < lots:
        lots = actual_lots

    volume = direction * lots

    _exec_delay_ms = params.get("execution_delay_ms", 50)
    if _exec_delay_ms > 0:
        _bar_high = bar.get("high", price) if bar is not None else price
        _bar_low = bar.get("low", price) if bar is not None else price
        _bar_dur = float(params.get("bar_duration_sec", 60.0))
        _delay_slippage_bps = compute_execution_delay_slippage_bps(
            price=price, bar_high=_bar_high, bar_low=_bar_low,
            bar_duration_sec=_bar_dur, exec_delay_ms=_exec_delay_ms, z_score=1.0
        )
        _delay_adj = price * _delay_slippage_bps / 10000.0
        if direction > 0:
            price += _delay_adj
        else:
            price -= _delay_adj

    sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
    sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 0)
    _max_sub_lots = int(params.get("backtest_max_sub_order_lots", 5))
    _sub_orders = _backtest_order_split(lots, max_sub_order_lots=_max_sub_lots)
    _total_slippage_cost = 0.0
    _multiplier = _get_contract_multiplier(symbol)
    for _sub_lots in _sub_orders:
        _sub_slippage = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
        _total_slippage_cost += _sub_lots * price * _multiplier * _sub_slippage / 10000.0
    slip_cost = _total_slippage_cost
    _market_impact_cost = _compute_market_impact_v2(lots, bar, price, params)
    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(symbol)))
    bt.equity -= (commission + slip_cost + _market_impact_cost)

    if params.get("execution_model", "standard") == "institutional":
        open_delay = int(params.get("open_execution_delay_bars", 0))
        if open_delay > 0:
            if len(bt.pending_orders) >= 100:
                bt.pending_orders.pop(0)
            bt.pending_orders.append(_PendingOrder(
                symbol=symbol, order_type="open", volume=volume,
                lots=lots, reason=reason, signal_bar_idx=bt.bar_idx,
                signal_price=price, tp_ratio=tp_ratio, sl_ratio=sl_ratio,
                params_snapshot=dict(params), created_at_bar=bt.bar_idx,
                fee_type=params.get("default_order_type", "taker"),
            ))
            bt.last_signal_time = bar_time
            bt.total_signals += 1
            _bt_capture_snapshot(bt, "signal", f"{symbol} {direction} {reason} (pending)", strategy_type, bar)
            return

    pos = _BacktestPosition(
        instrument_id=symbol, volume=volume, open_price=price,
        open_time=bar_time, stop_profit_price=sp_price, stop_loss_price=sl_price,
        open_reason=reason, lots=lots, open_state=bt.current_state,
        open_strength=bar.get("strength", 0.0),
        instrument_type=_infer_instrument_type(symbol), fill_ratio=_fill_ratio,
    )
    bt.positions[symbol] = pos
    bt.last_signal_time = bar_time
    bt.total_signals += 1
    _bt_capture_snapshot(bt, "signal", f"{symbol} {direction} {reason}", strategy_type, bar)


def _close_position_internal(
    bt: '_BacktestState',
    instrument_id: str,
    volume: int,
    is_buy: bool,
    price: float,
    *,
    params: Optional[Dict[str, Any]] = None,
    bar: Optional[pd.Series] = None,
    close_reason: str = 'backtest_close',
) -> bool:
    """[FIX-EXEC-V2-P0-1] 平仓内部实现 — 统一平仓逻辑 (11 步对齐 backtest_runner_validation.py:251-284)

    根因修复: 此前 BacktestStateAdapter.reduce_position 引用本函数但从未创建,
    导致 ImportError 被窄异常吞掉,持仓不减少/权益不平/交易不记录 (P0-1)。

    11 步平仓序列:
    1. 持仓查找 (不存在则 warning 返回)
    2. close_price 解析 (优先 bar.close,回退 price)
    3. pnl 计算 (含 short 方向处理)
    4. slippage 计算 (_compute_dynamic_slippage_bps)
    5. commission 计算 (_compute_commission,close_time 取 bar.datetime)
    6. net_pnl = pnl - slip - commission
    7. 权益更新 (_safe_equity_add, Kahan 累加)
    8. total_trades += 1
    9. pnl_pct 计算 (含 short 方向取反,open_price=0 时降级 0.0)
    10. hold_minutes 计算 (open_time 缺失时降级 0.0)
    11. record_trade + recent_pnls (截断 50) + del bt.positions[instrument_id]

    Args:
        bt: _BacktestState 实例
        instrument_id: 合约 ID
        volume: 平仓手数 (正数)
        is_buy: 平仓订单方向 (True=BUY 平空仓 / False=SELL 平多仓) — 实际方向以 pos.volume 符号为准
        price: 平仓执行价
        params: 当前 trial 参数 (可选,用于 slippage/commission 配置)
        bar: 当前 bar (pd.Series,可选;缺失时降级使用 price)
        close_reason: 平仓原因 (默认 'backtest_close')

    Returns:
        bool: True=平仓成功 / False=持仓不存在或异常

    实时回调路径硬约束 (HC-4 / NEW-1 / EXEC-V2-REFACTOR):
        所有 except 使用 Exception,关键路径用 isinstance 内部分流
    """
    # 参数降级
    if params is None:
        params = {}

    # ===== 步骤 1: 持仓查找 =====
    _pos = bt.positions.get(instrument_id)
    if _pos is None:
        logger.warning("[_close_position_internal] 持仓不存在,跳过平仓: inst=%s", instrument_id)
        return False

    try:
        # ===== 步骤 2: close_price 解析 =====
        # 优先使用 bar.close (含 gap/盘中波动),回退到入参 price
        close_price = bar.get("close", price) if bar is not None else price
        if close_price is None or close_price <= 0:
            close_price = price
            if close_price <= 0:
                logger.warning("[_close_position_internal] close_price<=0,使用 open_price 回退: inst=%s", instrument_id)
                close_price = _pos.open_price

        # ===== 步骤 3: pnl 计算 (含 short 方向) =====
        _mult = _get_contract_multiplier(instrument_id)
        # pos.volume 带符号: 正=多头持仓 / 负=空头持仓
        # pnl = (close - open) * signed_volume * multiplier (short 时 signed_volume<0,pnl 自动取反)
        _signed_close_volume = _pos.volume if abs(_pos.volume) <= volume else (
            volume if _pos.volume > 0 else -volume
        )
        # 全平 vs 部分平: 若请求平仓量 >= |pos.volume|, 全平; 否则部分平
        _is_full_close = abs(volume) >= abs(_pos.volume)
        if _is_full_close:
            _close_volume_signed = _pos.volume  # 全平使用全部持仓
        else:
            _close_volume_signed = _signed_close_volume  # 部分平使用部分量

        pnl = (close_price - _pos.open_price) * _close_volume_signed * _mult if _pos.volume != 0 else 0.0

        # ===== 步骤 4: slippage 计算 =====
        bid_ask = bar.get("bid_ask_spread", 0.0) if bar is not None else 0.0
        spread_q = bar.get("_spread_quality", 0) if bar is not None else 0
        slip_bps = _compute_dynamic_slippage_bps(
            close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params
        )
        _close_lots = abs(_close_volume_signed)
        slip = close_price * slip_bps / 10000.0 * _close_lots

        # ===== 步骤 5: commission 计算 =====
        try:
            _exchange_id = _infer_exchange_id(instrument_id)
            commission = _compute_commission(
                instrument_id, _close_lots,
                open_time=_pos.open_time,
                close_time=bar.get("datetime") if bar is not None else None,
                is_open=False,
                exchange_id=_exchange_id,
                exchange=_infer_exchange_from_id(_exchange_id),
            )
        except Exception as _comm_err:
            # 实时回调路径硬约束: except Exception (commission 计算失败降级为 0)
            logger.debug("[_close_position_internal] commission 计算异常(降级为0): inst=%s err=%s",
                         instrument_id, _comm_err)
            commission = 0.0

        # ===== 步骤 6: net_pnl =====
        net_pnl = pnl - slip - commission

        # ===== 步骤 7: 权益更新 (Kahan 累加) =====
        _safe_equity_add(bt, net_pnl)

        # ===== 步骤 8: total_trades += 1 =====
        bt.total_trades += 1

        # ===== 步骤 9: pnl_pct 计算 (含 short 方向取反) =====
        try:
            if np.isclose(close_price, _pos.open_price, rtol=1e-10):
                float_pnl_pct = 0.0
            else:
                float_pnl_pct = (
                    (close_price - _pos.open_price) / _pos.open_price
                    if _pos.open_price > 0 else 0.0
                )
                if _pos.volume < 0:
                    float_pnl_pct = -float_pnl_pct
        except Exception as _pct_err:
            # 实时回调路径硬约束: except Exception
            float_pnl_pct = 0.0

        # ===== 步骤 10: hold_minutes 计算 (open_time/bar 缺失时降级 0.0) =====
        try:
            _bar_minute = bar.get("minute") if bar is not None else None
            if _pos.open_time is None or _bar_minute is None:
                hold_min = 0.0
            else:
                hold_min = (_bar_minute - _pos.open_time).total_seconds() / 60.0
        except Exception:
            # 实时回调路径硬约束: except Exception
            hold_min = 0.0

        # ===== 步骤 11: record_trade + recent_pnls + del positions =====
        from param_pool._param_grids import _ClosedTrade
        bt.closed_trades.append(_ClosedTrade(
            pnl=net_pnl,
            pnl_pct=float_pnl_pct,
            close_reason=close_reason,
            hold_minutes=hold_min,
            open_reason=getattr(_pos, 'open_reason', ''),
            premium_pnl=0.0,
            delta_pnl=0.0,
            stage1_passed=getattr(_pos, 'stage1_passed', False),
        ))
        bt.recent_pnls.append(net_pnl)
        if len(bt.recent_pnls) > 50:
            bt.recent_pnls = bt.recent_pnls[-50:]

        # 持仓移除/缩减
        if _is_full_close:
            del bt.positions[instrument_id]
        else:
            # 部分平仓: 缩减 pos.volume
            _pos.volume -= _close_volume_signed
            _pos.lots = max(0, _pos.lots - _close_lots)
            logger.debug("[_close_position_internal] 部分平仓: inst=%s closed=%d remaining=%d",
                         instrument_id, _close_lots, _pos.volume)

        return True

    except Exception as _close_err:
        # 实时回调路径硬约束 (HC-4 / NEW-1 / EXEC-V2-REFACTOR): except Exception
        # 禁止窄异常元组,防止 ZeroDivisionError/OverflowError/OSError/RecursionError 穿透
        logger.error("[_close_position_internal] 平仓异常: inst=%s err=%s", instrument_id, _close_err)
        return False


def check_option_metadata_quality(bt, bar):
    _omq = bar.get('_option_metadata_quality', 0) if isinstance(bar, pd.Series) else getattr(bar, '_option_metadata_quality', 0)
    if isinstance(_omq, property):
        _omq = 0
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 0
    if _omq == 0:
        _instr_type = bar.get('instrument_type', 'future')
        if _instr_type in ('option_buyer', 'option_seller'):
            logging.warning("[BACKTEST] _check_positions: _option_metadata_quality=0, Greeks data unreliable, blocking new opens")
            bt.new_open_blocked = True
        else:
            logging.debug("[BACKTEST] _check_positions: _option_metadata_quality=0 for non-option instrument, proceeding")


def check_stop_profit_loss(pos, price, params):
    from infra.resilience import should_trigger_take_profit, should_trigger_stop_loss
    is_long = pos.volume > 0
    should_close = False
    close_reason = ""
    if should_trigger_take_profit(price, pos.stop_profit_price, is_long=is_long):
        should_close = True
        close_reason = "StopProfit"
    elif should_trigger_stop_loss(price, pos.stop_loss_price, is_long=is_long):
        should_close = True
        close_reason = "StopLoss"
    return should_close, close_reason


def check_time_stops(pos, price, bar_time, params, hold_minutes, open_reason,
                     _check_two_stage_stop, _resolve_time_stop, current_state):
    should_close = False
    close_reason = ""
    hard_stop_min = _resolve_time_stop(params, open_reason, current_state)
    if _check_two_stage_stop(pos, price, bar_time, params):
        should_close = True
        close_reason = "TwoStageTimeStop"
    elif hold_minutes >= hard_stop_min:
        should_close = True
        close_reason = "HardTimeStop"
    return should_close, close_reason


def check_eod_close(bar_time, eod_hour=14, eod_minute=55, night_eod_hour=2, night_eod_minute=30):
    _hour = bar_time.hour
    _minute = bar_time.minute
    if _hour == eod_hour and _minute >= eod_minute:
        return True, "EOD"
    elif _hour == night_eod_hour and _minute >= night_eod_minute:
        return True, "EOD_NIGHT"
    return False, ""

# ── Validation (merged from backtest_validation.py on 2026-06-12) ──

"""
backtest_validation.py — Re-export门面

原始代码已拆分为以下模块：
  - validation_l2_hyperparams.py: L-2超参数处理函数与常量
  - validation_hmm_state.py: HMM/状态验证函数
  - validation_deep_checks.py: 各独立深度验证函数
  - validation_deep_orchestrator.py: 编排入口与DEEP_VALIDATION_TIERS

所有公共API通过此门面文件re-export，确保现有import路径零修改。'
"""

import logging
from infra._helpers import get_logger  # R9-5
import re as _re

import numpy as np
import pandas as pd

from infra.shared_utils import DEFAULT_RISK_FREE_RATE

from infra.trading_utils import CrossSystemExecutionKernel
_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()

from param_pool._param_grids import (
    _PendingOrder, _BacktestPosition, _ClosedTrade, _BacktestState,
)
from param_pool._param_grids import (
    DEEP_VALIDATION_TIERS, L2_HYPERPARAMS, L2_PARAM_GRID,
    L2_CONFLICT_RESOLUTION, PARAM_SOURCE_ANNOTATION, PARAM_TIERS,
)
from param_pool._param_defaults import PARAM_DEFAULTS




PARAM_GRID_CYCLE_RESONANCE = {
    "close_take_profit_ratio": [1.1, 1.5, 2.5],
    "close_stop_loss_ratio": [0.3, 0.5, 0.7],
    "max_risk_ratio": [0.2, 0.3, 0.5],
    "lots_min": [1, 3, 5],
    "signal_cooldown_sec": [0.0, 60.0, 120.0],
    "state_confirm_bars": [2, 3, 5],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
}


logger = get_logger(__name__)  # R9-5


# ---- Re-export from validation_l2_hyperparams ----
from param_pool.validation.validation_l2_hyperparams import (
    _DDL_COLUMN_SAFE_PATTERN,
    check_l2_conflict,
    check_l2_statistical_power,
    analyze_l2_sensitivity,
    compute_alpha_confidence_interval,
    evaluate_state_accuracy,
    optimize_l2_params_step1,
    validate_l2_param_conflicts,
    run_step2_smoke_test,
)

# ---- Re-export from validation_hmm_state ----
from param_pool.validation.validation_deep_orchestrator import (
    _infer_hmm_state_from_iv,
    validate_hmm_online_vs_offline,
    validate_state_window_boundary_jitter,
    validate_multiscale_indicator_consistency,
    validate_trend_score_bar_vs_tick_correlation,
)

# ---- Re-export from validation_deep_checks ----
from param_pool.validation.validation_deep_orchestrator import (
    _DeepValidationResult,
    validate_rollover_impact,
    validate_hft_temporal_robustness,
    validate_cross_strategy_correlation,
    validate_market_friendliness_baseline,
    validate_regime_robustness,
    validate_liquidity_stress,
    validate_doomed_tests,
    validate_logic_transferability,
)

# ---- Re-export from validation_deep_orchestrator ----
from param_pool.validation.validation_deep_orchestrator import (
    DEEP_VALIDATION_TIERS,
    _ensure_runner,
    run_deep_validation_tiered,
    run_deep_validation_suite,
    run_validation_with_registry,
)


__all__ = [
    # 门面本地常量
    'DEFAULT_RISK_FREE_RATE',
    'PARAM_TIERS',
    'PARAM_GRID_CYCLE_RESONANCE',
    'HFT_TICK_PARAMS',
    # validation_l2_hyperparams
    'L2_HYPERPARAMS',
    'L2_PARAM_GRID',
    'L2_CONFLICT_RESOLUTION',
    'PARAM_SOURCE_ANNOTATION',
    '_DDL_COLUMN_SAFE_PATTERN',
    'check_l2_conflict',
    'check_l2_statistical_power',
    'analyze_l2_sensitivity',
    'compute_alpha_confidence_interval',
    'evaluate_state_accuracy',
    'optimize_l2_params_step1',
    'validate_l2_param_conflicts',
    'run_step2_smoke_test',
    # validation_hmm_state
    '_infer_hmm_state_from_iv',
    'validate_hmm_online_vs_offline',
    'validate_state_window_boundary_jitter',
    'validate_multiscale_indicator_consistency',
    'validate_trend_score_bar_vs_tick_correlation',
    # validation_deep_checks
    '_DeepValidationResult',
    'validate_rollover_impact',
    'validate_hft_temporal_robustness',
    'validate_cross_strategy_correlation',
    'validate_market_friendliness_baseline',
    'validate_regime_robustness',
    'validate_liquidity_stress',
    'validate_doomed_tests',
    'validate_logic_transferability',
    # validation_deep_orchestrator
    'DEEP_VALIDATION_TIERS',
    '_ensure_runner',
    'run_deep_validation_tiered',
    'run_deep_validation_suite',
    'run_validation_with_registry',
]