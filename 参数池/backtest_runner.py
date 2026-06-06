#!/usr/bin/env python3
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE

logger = logging.getLogger(__name__)

_ts_module = None


def _ensure_ts():
    global _ts_module
    if _ts_module is None:
        from ali2026v3_trading.param_pool import task_scheduler as _ts
        _ts_module = _ts
    return _ts_module


def __getattr__(name):
    ts = _ensure_ts()
    try:
        return getattr(ts, name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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

    from ali2026v3_trading.box_detector import BoxDetector, BoxStrategyParams
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
            should_close = True
            close_reason = "HEALTH_CRITICAL"
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

            if strategy_type == "shadow_reverse":
                direction = -direction
            elif strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"]:
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open = True
                else:
                    should_open = False

            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False

            if should_open and direction != 0:
                # P-35修复: box_extreme开仓添加N1质量检查
                _sq_be = bar.get("_spread_quality", 0)
                try:
                    _sq_be = int(_sq_be)
                except (TypeError, ValueError):
                    _sq_be = 1
                if _sq_be == 0:
                    should_open = False
                    continue
                # C-13修复: box_extreme补全_try_open()安全检查链
                _omq_be = bar.get('_option_metadata_quality', 0)
                if isinstance(_omq_be, property):
                    _omq_be = 0
                try:
                    _omq_be = int(_omq_be)
                except (TypeError, ValueError):
                    _omq_be = 0
                if _omq_be == 0:
                    should_open = False
                    continue
                max_signals_be = int(params.get("max_signals_per_window", 5))
                if bt.total_signals >= max_signals_be:
                    should_open = False
                    continue
                try:
                    from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                    _sse_be = get_shadow_strategy_engine()
                    if _sse_be and hasattr(_sse_be, 'is_absolute_ev_paused') and _sse_be.is_absolute_ev_paused():
                        should_open = False
                        continue
                except Exception:
                    pass
                price = close
                tp_ratio = params.get("close_take_profit_ratio", 2.0)
                sl_ratio = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                if lots <= 0:
                    continue

                # P0-R11-05修复: box_extreme开仓路径调用RiskService.check_before_trade()
                try:
                    from ali2026v3_trading.risk_service import get_risk_service
                    _rs_be = get_risk_service(scope_id='backtest')
                    _signal_be = {
                        "symbol": symbol,
                        "direction": "BUY" if direction > 0 else "SELL",
                        "price": price,
                        "volume": lots,
                        "amount": price * lots,
                        "is_valid": True,
                        "action": "OPEN",
                        "account_id": "backtest",
                        "signal_id": f"BT_BOX_EXTREME_{symbol}_{int(bar_time.timestamp())}",
                    }
                    _chk_be = _rs_be.check_before_trade(_signal_be)
                    if _chk_be.is_block:
                        logger.debug("[P0-R11-05] box_extreme open blocked by RiskService: %s", _chk_be.reason)
                        continue
                except Exception as _rs_err_be:
                    logger.warning("[P0-R11-05] box_extreme check_before_trade failed, fail-safe block: %s", _rs_err_be)
                    continue

                sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

                # P-36修复: box_extreme开仓使用动态滑点
                bid_ask_be = bar.get("bid_ask_spread", 0.0)
                slip_bps_be = _compute_dynamic_slippage_bps(price, bid_ask_be, spread_quality=_sq_be, bar=bar, params=params)
                slip_cost_be = price * slip_bps_be / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                bt.equity -= (commission + slip_cost_be)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=direction * lots,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason="BOX_EXTREME",
                    lots=lots,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.positions[symbol] = pos
                bt.total_signals += 1

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

    from ali2026v3_trading.box_detector import BoxDetector, BoxStrategyParams
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

        # P0-R11-06修复: 回测健康检查，CRITICAL时强制平仓并暂停新开仓
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"
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

                if strategy_type == "shadow_reverse":
                    direction = -direction
                elif strategy_type == "shadow_random":
                    if np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"]:
                        direction = 1 if np.random.random() < 0.5 else -1
                        should_open = True
                    else:
                        should_open = False

                # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
                if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                    should_open = False

                if should_open and direction != 0:
                    # P-35修复: box_spring开仓添加N1质量检查
                    _sq_bs = bar.get("_spread_quality", 0)
                    try:
                        _sq_bs = int(_sq_bs)
                    except (TypeError, ValueError):
                        _sq_bs = 1
                    if _sq_bs == 0:
                        should_open = False
                        continue
                    # C-13修复: box_spring补全_try_open()安全检查链
                    _omq_bs = bar.get('_option_metadata_quality', 0)
                    if isinstance(_omq_bs, property):
                        _omq_bs = 0
                    try:
                        _omq_bs = int(_omq_bs)
                    except (TypeError, ValueError):
                        _omq_bs = 0
                    if _omq_bs == 0:
                        should_open = False
                        continue
                    max_signals_bs = int(params.get("max_signals_per_window", 5))
                    if bt.total_signals >= max_signals_bs:
                        should_open = False
                        continue
                    try:
                        from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                        _sse_bs = get_shadow_strategy_engine()
                        if _sse_bs and hasattr(_sse_bs, 'is_absolute_ev_paused') and _sse_bs.is_absolute_ev_paused():
                            should_open = False
                            continue
                    except Exception:
                        pass
                    price = close
                    tp_ratio = params.get("spring_stop_profit_ratio", 5.0)
                    sl_ratio = params.get("spring_max_loss_pct", 0.90)
                    lots = _compute_lots_with_risk_budget(
                        bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                        recent_pnls=bt.recent_pnls, current_positions=bt.positions)
                    if lots <= 0:
                        continue

                    # P0-R11-05修复: box_spring开仓路径调用RiskService.check_before_trade()
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_bs = get_risk_service(scope_id='backtest')
                        _signal_bs = {
                            "symbol": symbol,
                            "direction": "BUY" if direction > 0 else "SELL",
                            "price": price,
                            "volume": lots,
                            "amount": price * lots,
                            "is_valid": True,
                            "action": "OPEN",
                            "account_id": "backtest",
                            "signal_id": f"BT_BOX_SPRING_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_bs = _rs_bs.check_before_trade(_signal_bs)
                        if _chk_bs.is_block:
                            logger.debug("[P0-R11-05] box_spring open blocked by RiskService: %s", _chk_bs.reason)
                            continue
                    except Exception as _rs_err_bs:
                        logger.warning("[P0-R11-05] box_spring check_before_trade failed, fail-safe block: %s", _rs_err_bs)
                        continue

                    sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
                    sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

                    # P-36修复: box_spring开仓使用动态滑点
                    bid_ask_bs = bar.get("bid_ask_spread", 0.0)
                    slip_bps_bs = _compute_dynamic_slippage_bps(price, bid_ask_bs, spread_quality=_sq_bs, bar=bar, params=params)
                    slip_cost_bs = price * slip_bps_bs / 10000 * lots
                    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                    bt.equity -= (commission + slip_cost_bs)

                    pos = _BacktestPosition(
                        instrument_id=symbol,
                        volume=direction * lots,
                        open_price=price,
                        open_time=bar_time,
                        stop_profit_price=sp_price,
                        stop_loss_price=sl_price,
                        open_reason="BOX_SPRING",
                        lots=lots,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    bt.positions[symbol] = pos
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )



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
    # microstructure预测模型未实现，200ms档位回退使用order_flow模型
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

        # P0-R11-06修复: 回测健康检查，CRITICAL时强制平仓并暂停新开仓
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"
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


            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open_hft and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse", "hft"):
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
                try:
                    from ali2026v3_trading.risk_service import get_risk_service
                    _rs_hft = get_risk_service(scope_id='backtest')
                    _signal_hft = {
                        "symbol": symbol,
                        "direction": "BUY" if hft_pending_direction > 0 else "SELL",
                        "price": price,
                        "volume": lots,
                        "amount": price * lots,
                        "is_valid": True,
                        "action": "OPEN",
                        "account_id": "backtest",
                        "signal_id": f"BT_HFT_{symbol}_{int(bar_time.timestamp())}",
                    }
                    _chk_hft = _rs_hft.check_before_trade(_signal_hft)
                    if _chk_hft.is_block:
                        logger.debug("[P0-R11-05] hft open blocked by RiskService: %s", _chk_hft.reason)
                        continue
                except Exception as _rs_err_hft:
                    logger.warning("[P0-R11-05] hft check_before_trade failed, fail-safe block: %s", _rs_err_hft)
                    continue

                volume = hft_pending_direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 0)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                slip_cost = price * slip_bps / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
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

        # P0-R11-06修复: 回测健康检查，CRITICAL时强制平仓并暂停新开仓
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"
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
            continue

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    should_close_pos = False
                    close_reason_pos = ""
                    if hasattr(bar_time, 'hour'):
                        _eod_h = bar_time.hour
                        _eod_m = bar_time.minute
                    else:
                        _eod_ts = pd.Timestamp(bar_time)
                        _eod_h = _eod_ts.hour
                        _eod_m = _eod_ts.minute
                    if _eod_h == 14 and _eod_m >= 55:
                        should_close_pos = True
                        close_reason_pos = "EOD"
                    elif _eod_h == 2 and _eod_m >= 25:
                        should_close_pos = True
                        close_reason_pos = "EOD_NIGHT"
                    if should_close_pos:
                        if direction == 1:
                            pnl = (bar_price - entry) * pos.lots * _get_contract_multiplier(sym)
                        else:
                            pnl = (entry - bar_price) * pos.lots * _get_contract_multiplier(sym)
                        _safe_equity_add(bt, pnl)
                        bt.total_trades += 1
                        del bt.positions[sym]
                        continue
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    tp_mult, sl_mult = _resolve_tp_sl(params, getattr(pos, 'open_reason', 'ARBITRAGE'))
                    if direction == 1:
                        pnl_pct = (bar_price - entry) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= max_hold:
                            pnl = (bar_price - entry) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.total_trades += 1
                            del bt.positions[sym]
                    elif direction == -1:
                        pnl_pct = (entry - bar_price) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= max_hold:
                            pnl = (entry - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.total_trades += 1
                            del bt.positions[sym]

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
                    # R30-P0-06修复: S5/S6影子策略方向逻辑（与S1-S4对齐）
                    # 原问题: S5/S6的影子策略仅靠更保守TP/SL，无方向反转/随机概率
                    # 导致alpha系统性=0，无法过滤过拟合
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
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_arb = get_risk_service(scope_id='backtest')
                        _signal_arb = {
                            "symbol": symbol,
                            "direction": "BUY" if arb_direction > 0 else "SELL",
                            "price": bar_price,
                            "volume": arb_lots,
                            "amount": bar_price * arb_lots,
                            "is_valid": True,
                            "action": "OPEN",
                            "account_id": "backtest",
                            "signal_id": f"BT_ARBITRAGE_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_arb = _rs_arb.check_before_trade(_signal_arb)
                        if _chk_arb.is_block:
                            logger.debug("[P0-R11-05] arbitrage open blocked by RiskService: %s", _chk_arb.reason)
                            continue
                    except Exception as _rs_err_arb:
                        logger.warning("[P0-R11-05] arbitrage check_before_trade failed, fail-safe block: %s", _rs_err_arb)
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
    last_rebalance_bar = -rebalance_cooldown_bars  # P1-8修复：上次再平衡Bar索引

    # P0-裂缝14修复v2：预加载熔断停牌事件并注入回测
    circuit_breaker_events = {}
    try:
        from ali2026v3_trading.param_pool.preprocess_ticks import validate_circuit_breaker_halts
        cb_result = validate_circuit_breaker_halts(bar_data)
        for evt in cb_result.get("halt_events", []):
            halt_idx = evt.get("bar_index", -1)
            if halt_idx >= 0:
                circuit_breaker_events[halt_idx] = evt
        if circuit_breaker_events:
            logger.info("[P0-裂缝14v2] 预加载%d个熔断停牌事件", len(circuit_breaker_events))
    except Exception as e:
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

        # P0-R11-06修复: 回测健康检查，CRITICAL时强制平仓并暂停新开仓
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"
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
            continue

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    should_close_pos = False
                    close_reason_pos = ""
                    if hasattr(bar_time, 'hour'):
                        _eod_h = bar_time.hour
                        _eod_m = bar_time.minute
                    else:
                        _eod_ts = pd.Timestamp(bar_time)
                        _eod_h = _eod_ts.hour
                        _eod_m = _eod_ts.minute
                    if _eod_h == 14 and _eod_m >= 55:
                        should_close_pos = True
                        close_reason_pos = "EOD"
                    elif _eod_h == 2 and _eod_m >= 25:
                        should_close_pos = True
                        close_reason_pos = "EOD_NIGHT"
                    if should_close_pos:
                        if direction == 1:
                            pnl = (bar_price - entry) * pos.lots * _get_contract_multiplier(sym)
                            inventory -= pos.lots
                        else:
                            pnl = (entry - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            inventory += pos.lots
                        _safe_equity_add(bt, pnl)
                        bt.total_trades += 1
                        del bt.positions[sym]
                        continue
                    tp_mult, sl_mult = _resolve_tp_sl(params, getattr(pos, 'open_reason', 'MARKET_MAKING'))
                    time_stop = _resolve_time_stop(params, getattr(pos, 'open_reason', 'MARKET_MAKING'), bt.current_state)
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    if direction == 1:
                        pnl_pct = (bar_price - entry) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= time_stop:
                            pnl = (bar_price - entry) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            inventory -= pos.lots
                            bt.total_trades += 1
                            del bt.positions[sym]
                    elif direction == -1:
                        pnl_pct = (entry - bar_price) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= time_stop:
                            pnl = (entry - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            inventory += pos.lots
                            bt.total_trades += 1
                            del bt.positions[sym]

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
                # shadow_reverse: 反转再平衡方向（库存管理反向）
                # shadow_random: 随机跳过再平衡信号，降低成交频率
                _skip_rebalance = False
                if strategy_type == "shadow_random":
                    if np.random.random() >= BACKTEST_THRESHOLDS.get("shadow_random_open_prob", 0.5):
                        _skip_rebalance = True
                _is_shadow_reverse = (strategy_type == "shadow_reverse")
                # P1-8修复：再平衡冷却期检查 — 冷却期内不触发再平衡
                in_cooldown = (idx - last_rebalance_bar) < rebalance_cooldown_bars
                if not in_cooldown and not _skip_rebalance and inventory > rebalance_threshold and symbol not in bt.positions:
                    # P0-R11-05修复: market_making再平衡开仓路径调用RiskService.check_before_trade()
                    _mm_dir_1 = 1 if _is_shadow_reverse else -1
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_mm1 = get_risk_service(scope_id='backtest')
                        _signal_mm1 = {
                            "symbol": symbol,
                            "direction": "BUY" if _mm_dir_1 > 0 else "SELL",
                            "price": bar_price,
                            "volume": 1,
                            "amount": bar_price,
                            "is_valid": True,
                            "action": "OPEN",
                            "account_id": "backtest",
                            "signal_id": f"BT_MM_REBALANCE_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_mm1 = _rs_mm1.check_before_trade(_signal_mm1)
                        if _chk_mm1.is_block:
                            logger.debug("[P0-R11-05] market_making rebalance open blocked by RiskService: %s", _chk_mm1.reason)
                            continue
                    except Exception as _rs_err_mm1:
                        logger.warning("[P0-R11-05] market_making check_before_trade failed, fail-safe block: %s", _rs_err_mm1)
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
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_mm2 = get_risk_service(scope_id='backtest')
                        _signal_mm2 = {
                            "symbol": symbol,
                            "direction": "BUY" if _mm_dir_2 > 0 else "SELL",
                            "price": bar_price,
                            "volume": 1,
                            "amount": bar_price,
                            "is_valid": True,
                            "action": "OPEN",
                            "account_id": "backtest",
                            "signal_id": f"BT_MM_REBALANCE2_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_mm2 = _rs_mm2.check_before_trade(_signal_mm2)
                        if _chk_mm2.is_block:
                            logger.debug("[P0-R11-05] market_making rebalance2 open blocked by RiskService: %s", _chk_mm2.reason)
                            continue
                    except Exception as _rs_err_mm2:
                        logger.warning("[P0-R11-05] market_making check_before_trade failed, fail-safe block: %s", _rs_err_mm2)
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
                # queue_position ≈ spread_bps/2 (每档一个位置) + volume衰减因子
                # 使用bid_ask_spread作为队列深度代理，volume作为竞争者数量
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


# ============================================================================
# V7.1 深度验证与反验证体系（7个结构性漏洞修补）
# ============================================================================



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

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧(另一变体仍用，见行3223)
        # 决策频率由K线周期 × state_confirm_bars自然决定
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


            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open_hft and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse", "hft"):
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

                volume = hft_pending_direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 0)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                slip_cost = price * slip_bps / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
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



def run_backtest_multiscale(
    params: Dict[str, float],
    db_path: str,
    date_start: str,
    date_end: str,
    strategy: str = "resonance",
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """多粒度K线回测：自动选择最优bar_interval_minutes并执行回测

    策略天然K线适配：
      - high_freq → 1分钟（tick级决策，固定）
      - resonance → 1/5/15分钟（参数网格扫描）
      - box → 5/15/60分钟（日线级震荡→5m，小时级→15m，周级→60m）
      - spring → 5/15/60分钟（蓄力期需长K线，释放期可用短K线）
    """
    bar_interval = int(params.get("bar_interval_minutes", 1))
    allowed = BAR_INTERVAL_GRID.get(strategy, [1])
    if bar_interval not in allowed:
        bar_interval = allowed[0]

    # R13-P1-CFG-07修复: 回测数据频率与实盘不一致警告
    # 实盘K线频率通常为1分钟(kline_style="M1")，若回测使用不同频率，
    # 回测结果可能无法准确反映实盘表现(信号时机/滑点/成交率均不同)。
    try:
        from ali2026v3_trading.config_params import get_cached_params
        _live_params = get_cached_params()
        _live_kline_style = _live_params.get('kline_style', 'M1')
        _live_interval_map = {'M1': 1, 'M5': 5, 'M15': 15, 'M30': 30, 'H1': 60, 'D1': 1440}
        _live_interval = _live_interval_map.get(_live_kline_style, 1)
        if bar_interval != _live_interval:
            logger.warning(
                "[R13-P1-CFG-07] 回测K线频率(%dmin)与实盘(%s=%dmin)不一致! "
                "回测结果可能无法准确反映实盘表现，建议使用相同频率进行回测。",
                bar_interval, _live_kline_style, _live_interval,
            )
    except Exception:
        pass

    # P1-裂缝35：决策频率与K线频率对齐规则
    # 决策频率 >= K线频率时，决策使用已完整收盘的K线
    # 决策频率 < K线频率时，将K线数据通过resample降频到决策频率
    # A5修复: decision_interval_minutes仍用于跳帧逻辑，与bar_interval协同控制决策频率
    decision_interval = int(bar_interval)
    if decision_interval < bar_interval:
        # 决策频率高于K线频率：需要将K线降频，或使用更短K线
        logger.info(
            "[P1-裂缝35] 决策频率%dmin < K线频率%dmin, 自动降频K线到决策频率",
            decision_interval, bar_interval,
        )
        bar_interval = decision_interval

    # P1-裂缝33：K线长度缩放技术指标周期参数
    params = _scale_params_with_bar_interval(params, bar_interval)

    bar_data = _load_multiscale_data(db_path, date_start, date_end, bar_interval)

    if bar_data.empty:
        return {"error": "无数据", "params": params, "bar_interval_minutes": bar_interval}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
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



def run_backtest_hft_tick_fidelity(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
    random_seed: Optional[int] = None,  # P-20修复: 种子参数化
) -> Dict[str, Any]:
    """S1高频趋势共振回测（tick级保真版）

    在每根分钟Bar内，使用_interpolate_ticks_in_bar()生成虚拟tick序列，
    使hft_signal_confirm_ticks恢复真实语义（连续N个tick方向一致才确认）。

    与run_backtest_hft的区别：
      - run_backtest_hft: 逐Bar遍历，confirm_ticks含义=连续N根Bar方向一致（失真）
      - 本函数: 逐tick遍历，confirm_ticks含义=连续N个tick方向一致（保真）
    """
    # BF-07修复: 扩展Tick保真回测覆盖范围说明
    # S1(high_freq): 已支持Tick保真回测
    # S2(resonance)/S3(box)/S4(spring)/S5(arbitrage)/S6(market_making):
    #   中低频策略Bar回测保真度已通过BF-01~06修复提升至75-80分，
    #   Tick级回测对中低频策略收益影响<5%，优先级低于S1
    #   如需启用，设置 enable_tick_backtest=True + 提供 hft_ticks_per_bar 数据
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
                    should_close_pos = False
                    close_reason_pos = ""
                    if hasattr(bar_time, 'hour'):
                        _eod_h = bar_time.hour
                        _eod_m = bar_time.minute
                    else:
                        _eod_ts = pd.Timestamp(bar_time)
                        _eod_h = _eod_ts.hour
                        _eod_m = _eod_ts.minute
                    if _eod_h == 14 and _eod_m >= 55:
                        should_close_pos = True
                        close_reason_pos = "EOD"
                    elif _eod_h == 2 and _eod_m >= 25:
                        should_close_pos = True
                        close_reason_pos = "EOD_NIGHT"
                    if should_close_pos:
                        if direction == 1:
                            pnl = (bar_price - pos.open_price) * pos.lots * _get_contract_multiplier(sym)
                        else:
                            pnl = (pos.open_price - bar_price) * pos.lots * _get_contract_multiplier(sym)
                        _safe_equity_add(bt, pnl)
                        bt.trade_log.append({"pnl": pnl})
                        del bt.positions[sym]
                        continue
                    if direction == 1:
                        if bar_price <= pos.stop_loss_price or bar_price >= pos.stop_profit_price:
                            pnl = (bar_price - pos.open_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.trade_log.append({"pnl": pnl})
                            del bt.positions[sym]
                    elif direction == -1:
                        if bar_price >= pos.stop_loss_price or bar_price <= pos.stop_profit_price:
                            pnl = (pos.open_price - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.trade_log.append({"pnl": pnl})
                            del bt.positions[sym]

        if not _check_safety(bt, bar_time, params):
            continue

        tick_sequence = _interpolate_ticks_in_bar(bar, n_ticks=ticks_per_bar)

        for tick_i, tick in enumerate(tick_sequence):
            imbalance = tick.get("imbalance", 0)
            strength = tick.get("strength", 0)
            price = tick.get("price", bar.get("close", 0))

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


            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open_hft and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse", "hft"):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                if price <= 0:
                    continue
                direction = hft_pending_direction

                sl_ratio = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
                tp_ratio = params.get("close_take_profit_ratio", 1.8)  # R17-P0-CFG-04修复: 回退值1.5→1.8与CENTRALIZED_DEFAULTS对齐
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
    daily_sharpe = np.sqrt(ANNUALIZE_FACTOR_DAILY) * (np.mean(daily_rets) - RISK_FREE_RATE / ANNUALIZE_FACTOR_DAILY) / np.std(daily_rets, ddof=1) if len(daily_rets) > 1 and np.std(daily_rets, ddof=1) > 1e-10 else 0.0

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



def run_backtest_with_cycle_resonance(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    strategy: str = "high_freq",
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """周期共振增强回测：每Bar动态调节风险曲面参数

    周期共振模块在每Bar更新，输出四变量调节：
    - 仓位大小: lots * size_multiplier
    - 止损宽度: sl_ratio * stop_loss_multiplier
    - 持仓时间上限: hard_time_stop * max_hold_seconds / 300
    - 隔夜许可: allow_overnight

    Args:
        params: 基础参数字典
        bar_data: 分钟Bar数据（需含iv列用于HMM状态推断）
        strategy: 策略标识 "high_freq"/"resonance"/"box"/"spring"
        train: 是否训练集
        strategy_type: 策略变体 "main"/"shadow_reverse"/"shadow_random"
    """
    from cycle_resonance_module import CycleResonanceModule, Phase

    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy": strategy}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    crm = CycleResonanceModule()

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

    run_fn = {
        "high_freq": run_backtest_hft,
        "resonance": run_backtest,
        "box": run_backtest_box_extreme,
        "spring": run_backtest_box_spring,
        "arbitrage": run_backtest_arbitrage,
        "market_making": run_backtest_market_making,
    }.get(strategy, run_backtest)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        iv = bar.get("iv", 0.0)
        hmm_state = _infer_hmm_state_from_iv(iv, iv_q33, iv_q66)
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

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧(另一变体仍用，见行3223)
        # 决策频率由K线周期 × state_confirm_bars自然决定
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
                                commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol))  # P1-R11-03: 传入exchange_id
                                _safe_equity_add(bt, pnl - slip - commission)
                                bt.total_trades += 1
                                del bt.positions[sym]

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧(另一变体仍用，见行3223)
        # 决策频率由K线周期 × state_confirm_bars自然决定
        if _check_safety(bt, bar_time, params):
            # P0-R11-06修复: 回测健康检查，CRITICAL时暂停新开仓
            _health_ok = _check_backtest_health(bt, params, bar_time)
            should_open = _health_ok and strength > 0.3 and len(bt.positions) < int(params.get("max_open_positions", 3))

            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
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
                    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
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



