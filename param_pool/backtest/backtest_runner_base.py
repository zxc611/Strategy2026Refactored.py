# [M1-103] 回测运行器基类
#!/usr/bin/env python3
# MODULE_ID: M1-156
"""
量化任务调度系统：参数网格扫描 + 多进程回测 + 结果汇总

P1-5拆分: 配置常量→backtest_config.py, 保真度模拟→backtest_fidelity.py,
         回测状态+数据类→backtest_state.py, 类型枚举→backtest_runner_types.py,
         工具函数→backtest_runner_utils.py, 本文件保留run_backtest主循环+Facade re-export
"""
from __future__ import annotations

import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from typing import Any, Dict

import numpy as np
import pandas as pd

from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import *  # noqa: F401,F403

from ali2026v3_trading.param_pool.backtest.backtest_config import BacktestStateEnum, _STATE_REASON_MAP  # noqa: F401

from ali2026v3_trading.param_pool._param_grids import _sync_random_seed, _compute_market_impact_v2, _compute_dynamic_slippage_bps
from ali2026v3_trading.param_pool._param_grids import (
    _BacktestState,
    _BacktestPosition,
    _PendingOrder,
    _ClosedTrade,
)
from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import (
    _bt_capture_snapshot,
    _check_state_transition,
    _compute_lots_with_risk_budget,
    _check_logic_reversal,
    _infer_trend_scores_from_bar,
)
from ali2026v3_trading.param_pool.backtest.backtest_state import (
    _get_life_estimator,
    _get_cascade_judge_module,
    _infer_instrument_type,
    _infer_exchange_id,
    _infer_exchange_from_id,
    _infer_contract_type,
    _calculate_limit_prices,
    _resolve_tp_sl,
    _resolve_time_stop,
    _resolve_time_stop_hard,
    _is_consecutive_loss_paused,
    _backtest_order_split,
    _compute_dynamic_slippage_bps,
    _get_contract_multiplier,
    _safe_equity_add,
    _compute_cascade_slippage_bps,
    _update_mtm_equity,
    _check_intra_bar_stop_loss,
    _check_two_stage_stop,
    _get_reason_tp_sl_from_position_service,
    _get_expiry_slippage_multiplier,
    _compute_option_mtm_price,
    _infer_liquidity_tier,
    calc_trade_fee,
    _compute_commission,
)

from ali2026v3_trading.param_pool.backtest.backtest_config import (
    INITIAL_EQUITY,
    CANCEL_DELAY_MS,
    CANCEL_FAILURE_RATE,
    ENABLE_CANCEL_SIMULATION,
    ENABLE_QUEUE_SIMULATION,
    PREPROCESSED_DB,
    RESULTS_DB,
    MAX_WORKERS,
    TRAIN_START,
    TEST_START,
    TEST_END,
    TARGET_SYMBOLS,
    DEFAULT_RISK_FREE_RATE,
    MARKET_ORDER_SLIPPAGE_BPS,
    SLIPPAGE_BPS,
    MARKET_ORDER_PRICE_MODE,
    INSTRUMENT_SLIPPAGE_MULTIPLIER,
    EXPIRY_SLIPPAGE_MULTIPLIERS,
    FEE_STRUCTURE,
    FEE_STRUCTURE_V2,
    COMMISSION_PER_LOT,
    CASCADE_SLIPPAGE_MULTIPLIER,
    CASCADE_SLIPPAGE_CAP_BPS,
    EXCHANGE_COMMISSION_RATES,
    LIMIT_UP_RATIO,
    LIMIT_DOWN_RATIO,
    QUEUE_TIMEOUT_SECONDS,
)


from ali2026v3_trading.infra.shared_utils import safe_price_check, compute_execution_delay_slippage_bps
from ali2026v3_trading.infra.shared_trading_constants import detect_rollover_gaps, compute_rollover_cost

from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence, _check_bar_data_monotonic


# 升A路径T2.3: 风控检查直接耦合
from ali2026v3_trading.risk.risk_service import get_risk_service

logger = get_logger(__name__)  # R9-5


def _pre_trade_risk_check() -> bool:
    """升A路径T2.3: 交易前风控直接检查（独立于position_manager的双重校验）"""
    try:
        _rs = get_risk_service()
        _compliance = _rs.check_regulatory_compliance()
        if not _compliance.get('compliant', True):
            logger.error("[T2.3] 合规检查未通过: %s", _compliance)
            return False
        _exchange_status = _rs.check_exchange_status()
        if _exchange_status.get('status') != 'OPEN':
            logger.error("[T2.3] 交易所状态非OPEN: %s", _exchange_status)
            return False
        return True
    except (ImportError, AttributeError) as _err:
        logger.error("[T2.3] 风控检查异常(放行): %s", _err)
        return True  # 风控服务不可用时放行，避免阻断回测


def __getattr__(name):
    _RUNNER_NAMES_BOX = {
        'run_backtest_box_extreme', 'run_backtest_box_spring',
        'run_backtest_arbitrage', 'run_backtest_market_making',
    }
    _RUNNER_NAMES_HFT = {
        'run_backtest_hft', 'run_backtest_hft_with_disturbance',
        'run_backtest_multiscale', 'run_backtest_hft_tick_fidelity',
        'run_backtest_with_cycle_resonance',
    }
    import importlib
    if name in _RUNNER_NAMES_BOX:
        _mod = importlib.import_module('ali2026v3_trading.param_pool.backtest.backtest_strategy_runners')
        return getattr(_mod, name)
    if name in _RUNNER_NAMES_HFT:
        _mod = importlib.import_module('ali2026v3_trading.param_pool.backtest._backtest_runners_hft')
        return getattr(_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
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
    '_get_annualize_factor',
    'calc_trade_fee',
    '_compute_commission',
    '_build_backtest_result',
    '_compute_profit_loss_ratio_metrics',
    '_check_bar_data_monotonic',
    '_ClosedTrade',
    'detect_rollover_gaps',
    'exclude_rollover_signals',
    'compute_rollover_cost',
    'FEE_STRUCTURE',
    'FEE_STRUCTURE_V2',
    'COMMISSION_PER_LOT',
    'SLIPPAGE_BPS',
    'INITIAL_EQUITY',
    'CANCEL_FAILURE_RATE',
    'EXPIRY_SLIPPAGE_MULTIPLIERS',
    '_PendingOrder',
    '_check_intra_bar_stop_loss',
    '_compute_option_mtm_price',
    '_update_mtm_equity',
    '_compute_fill_quantity',
    '_compute_market_impact_v2',
    '_apply_fidelity_presets',
    '_reset_daily',
    '_bt_capture_snapshot',
    '_check_state_transition',
    '_compute_lots_with_risk_budget',
    '_check_logic_reversal',
    '_infer_trend_scores_from_bar',
    'BacktestStateEnum',
    '_STATE_REASON_MAP',
]


def run_backtest(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """V7共振策略回测：完整信号→决策→执行→风控闭环"""
    from ali2026v3_trading.param_pool._param_grids import BACKTEST_THRESHOLDS

    # P1-52修复: 使用generate_prefixed_id统一ID生成
    from ali2026v3_trading.infra.shared_utils import generate_prefixed_id
    _trial_isolation_flag = generate_prefixed_id('trial', 12)
    try:
        import threading as _threading_mod
        _threading_mod.current_thread().trial_isolation_flag = _trial_isolation_flag
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
        logging.debug("[R3-L2] trial_isolation_flag设置跳过: %s", _e)
        pass
    if bar_data.empty:
        return {"error": "无数据", "params": params}

    try:
        from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
        _init_pending = getattr(StrategyCoreService, '_init_pending', False)
        if _init_pending:
            import time as _wait_time
            _wait_deadline = _wait_time.time() + 10.0
            while getattr(StrategyCoreService, '_init_pending', False) and _wait_time.time() < _wait_deadline:
                _wait_time.sleep(0.05)
            if getattr(StrategyCoreService, '_init_pending', False):
                logging.warning("[R23-IN-P1-04] 策略onInit未在10s内完成，回测继续(降级)")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _p1_04_e:
        logging.debug("[R23-IN-P1-04] 策略初始化等待守卫跳过: %s", _p1_04_e)

    _check_bar_data_monotonic(bar_data)
    _apply_fidelity_presets(params)
    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)

    # 升A路径T2.3: 风控检查直接耦合 — 独立于position_manager的双重校验
    if not _pre_trade_risk_check():
        return {"error": "regulatory_compliance_failed", "sharpe": 0.0, "max_drawdown": -1.0, "num_signals": 0}

    if params.get("shadow_param_violation", False):
        bt.new_open_blocked = True
        logger.warning("[P1-R9-11] 影子参数独立性违反，回测新开仓已阻断")

    try:
        from ali2026v3_trading.config.state_param import get_state_param_manager
        _spm = get_state_param_manager()
        _spm_params = _spm.get_params(strategy_type)
        if _spm_params and isinstance(_spm_params, dict):
            params = {**params, **_spm_params}
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _d07_e:
        logger.debug("[R3-D-07] StateParamManager加载失败(使用默认params): %s", _d07_e)

    if "position_scale" not in params:
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            _cp = get_cached_params()
            _ps_key = f"{strategy_type}_position_scale"
            params["position_scale"] = float(_cp.get(_ps_key, _cp.get("position_scale", 1.0)))
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            params.setdefault("position_scale", 1.0)

    estimator = _get_life_estimator()
    if estimator is not None and (not hasattr(estimator, '_life_dict') or not estimator._life_dict):
        try:
            estimator.build_life_dict(bar_data)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
            logger.error("[R3-P-09] build_life_dict失败(严重): %s, 寿命信息缺失将导致D3维度退化为0.5", _e)
            bt.get("diagnostics", {}).setdefault("degraded_features", []).append("life_dict")

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        prev_bar = bar_data.iloc[idx - 1] if idx > 0 else None
        bt.bar_idx = idx
        _execution_delay_ms = params.get("execution_delay_ms", 50)
        if _execution_delay_ms > 0:
            _bar_high = bar.get("high", 0.0)
            _bar_low = bar.get("low", 0.0)
            _bar_close = bar.get("close", 0.0)
            _bar_dur = float(params.get("bar_duration_sec", 60.0))
            _main_delay_bps = compute_execution_delay_slippage_bps(
                price=_bar_close, bar_high=_bar_high, bar_low=_bar_low,
                bar_duration_sec=_bar_dur, exec_delay_ms=_execution_delay_ms, z_score=1.0
            )
            if _main_delay_bps > 0 and hasattr(bt, 'equity'):
                bt.equity -= bt.equity * _main_delay_bps / 10000.0 * 0.001
        _backfill_bar_fields(bar)
        _update_mtm_equity(bt, bar, params)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        _bar_ts = bar.name if isinstance(bar.name, (int, float)) else getattr(bar, 'timestamp', None)
        if _bar_ts is not None and hasattr(bt, '_last_bar_ts') and bt._last_bar_ts is not None:
            if _bar_ts == bt._last_bar_ts:
                logger.warning("[P1-R9-30] 重复Bar时间戳: %s, 跳过", _bar_ts)
                continue
            elif _bar_ts < bt._last_bar_ts:
                logger.warning("[P1-R9-30] Bar时间戳回退: %s < %s, 跳过", _bar_ts, bt._last_bar_ts)
                continue
        bt._last_bar_ts = _bar_ts
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params, prev_bar=prev_bar)
            elif params.get("enable_intra_bar_stop_loss", False):
                _check_positions(bt, bar, params, prev_bar=None)

        if params.get("execution_model", "standard") == "institutional" and bt.pending_orders:
            _remaining_orders = []
            for order in bt.pending_orders:
                order_age = idx - order.created_at_bar
                required_delay = params.get("open_execution_delay_bars", 1) if order.order_type == "open" else params.get("close_profit_delay_bars", 1)
                if order_age >= required_delay:
                    _cancel_delay_bars = max(1, int(params.get("cancel_delay_ms", CANCEL_DELAY_MS) / 60000.0))
                    if order_age > required_delay + _cancel_delay_bars:
                        _cancel_result = _simulate_order_cancel(
                            order_id=f"{order.symbol}_{order.created_at_bar}",
                            cancel_delay_ms=int(params.get("cancel_delay_ms", CANCEL_DELAY_MS)),
                            failure_rate=float(params.get("cancel_failure_rate", CANCEL_FAILURE_RATE)),
                            enable_cancel=params.get("enable_cancel_simulation", ENABLE_CANCEL_SIMULATION),
                        )
                        if _cancel_result["success"]:
                            logger.debug("[BF-P1-09] 延迟订单撤单成功: %s (延迟%d bar)", order.symbol, order_age)
                            continue
                        else:
                            logger.warning("[BF-P1-09] 延迟订单撤单失败，继续执行: %s, 原因=%s", order.symbol, _cancel_result["reason"])
                    if order.symbol not in bt.positions:
                        exec_price = bar.get("open", bar.get("close", 0.0))
                        if safe_price_check(exec_price):
                            if order.order_type == "open":
                                _delay_fill_lots = _compute_fill_quantity(order.lots, bar, order.params_snapshot)
                                if _delay_fill_lots <= 0:
                                    if order.retry_count < 3:
                                        order.retry_count += 1
                                        _remaining_orders.append(order)
                                    continue
                                _delay_lots = _delay_fill_lots
                            else:
                                _delay_lots = order.lots
                            bid_ask = bar.get("bid_ask_spread", 0.0)
                            spread_q = bar.get("_spread_quality", 0)
                            slip_bps = _compute_dynamic_slippage_bps(exec_price, bid_ask, spread_quality=spread_q, bar=bar, params=order.params_snapshot)
                            _mult = _get_contract_multiplier(order.symbol)
                            slip_cost = exec_price * slip_bps / 10000 * _delay_lots
                            commission = _compute_commission(order.symbol, _delay_lots, is_open=(order.order_type == "open"), exchange_id=_infer_exchange_id(order.symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(order.symbol)), open_time=bar.get("minute", pd.NaT) if order.order_type == "open" else None, close_time=bar.get("minute", pd.NaT) if order.order_type != "open" else None)
                            if order.order_type == "open":
                                direction = 1 if order.volume > 0 else -1
                                _delay_volume = direction * _delay_lots
                                if direction > 0:
                                    exec_price += exec_price * slip_bps / 10000
                                else:
                                    exec_price -= exec_price * slip_bps / 10000
                                sp_price = exec_price * order.tp_ratio if _delay_volume > 0 else exec_price / order.tp_ratio
                                sl_price = exec_price * (1 - order.sl_ratio) if _delay_volume > 0 else exec_price * (1 + order.sl_ratio)
                                _delay_fill_ratio = _delay_lots / order.lots if order.lots > 0 else 1.0
                                pos = _BacktestPosition(
                                    instrument_id=order.symbol,
                                    volume=_delay_volume,
                                    open_price=exec_price,
                                    open_time=bar.get("minute", pd.NaT),
                                    stop_profit_price=sp_price,
                                    stop_loss_price=sl_price,
                                    open_reason=order.reason,
                                    lots=_delay_lots,
                                    open_state=bt.current_state,
                                    open_strength=0.0,
                                    instrument_type=_infer_instrument_type(order.symbol),
                                    fill_ratio=_delay_fill_ratio,
                                )
                                bt.positions[order.symbol] = pos
                                bt.equity -= (commission + slip_cost)
                                bt.total_signals += 1
                                bt.total_trades += 1
                                logger.debug("[BF-02/03] 延迟开仓执行: %s @ %.4f (延迟%d bar)", order.symbol, exec_price, order_age)
                else:
                    if order_age < required_delay + 3:
                        _remaining_orders.append(order)
                    else:
                        logger.warning("[BF-02/03] 延迟订单超时放弃: %s (延迟%d bar)", order.symbol, order_age)
            bt.pending_orders = _remaining_orders

        _bar_limit_info = _calculate_limit_prices(bar, instrument_type=_infer_instrument_type(bar.get('symbol', '')).replace('option_buyer', 'OPTION_ETF').replace('option_seller', 'OPTION_ETF').replace('future', 'FUTURE'))
        if _bar_limit_info['is_limit_up'] or _bar_limit_info['is_limit_down']:
            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)
            continue

        state_check_interval_sec = params.get("state_check_interval_sec", 180.0)
        if idx > 0:
            prev_time = bar_data.iloc[idx - 1].get("minute", None)
            curr_time = bar.get("minute", None)
            if prev_time is not None and curr_time is not None:
                try:
                    if hasattr(prev_time, 'timestamp') and hasattr(curr_time, 'timestamp'):
                        time_gap_sec = (curr_time - prev_time).total_seconds()
                        if time_gap_sec > state_check_interval_sec:
                            n_skipped_checks = round(time_gap_sec / state_check_interval_sec)
                            for _ in range(n_skipped_checks):
                                bt.state_confirm_count += 1
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                    logger.warning("[BACKTEST] state_check_interval calculation failed: %s", _e)

        decision_interval = params.get("decision_interval_minutes", params.get("bar_interval", 1))
        if decision_interval > 1 and (idx % decision_interval != 0):
            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)
            continue

        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            _health_ok = _check_backtest_health(bt, params, bar_time)
            strength = bar.get("strength", 0)
            should_open = _health_ok and strength > 0.3 and len(bt.positions) < int(params.get("max_open_positions", 3))

            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False

            if strategy_type == "shadow_random":
                should_open = np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3))

            if should_open:
                _try_open(bt, bar, params, strategy_type=strategy_type)

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)


    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    if len(bt.positions) > 0:
        from ali2026v3_trading.param_pool.validation.checks_orchestrator import _force_close_all_positions
        _last_bar = bar_data.iloc[-1] if bar_data is not None and len(bar_data) > 0 else None
        if _last_bar is not None:
            _force_close_all_positions(bt, _last_bar, params, reason="backtest_end_force_close")

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )

# ── Runner Snapshot (merged from backtest_runner_snapshot.py on 2026-06-12) ──

import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.param_pool._param_grids import _sync_random_seed, _compute_market_impact_v2, _compute_dynamic_slippage_bps
from ali2026v3_trading.param_pool._param_grids import (
    _BacktestPosition,
    _BacktestState,
)
from ali2026v3_trading.param_pool.backtest.backtest_state import (
    _get_life_estimator,
    _get_contract_multiplier,
    _compute_commission,
    _safe_equity_add,
    _infer_exchange_id,
    _infer_exchange_from_id,
)

from ali2026v3_trading.infra.shared_utils import safe_price_check

from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import _check_safety



from ali2026v3_trading.param_pool.backtest._backtest_fidelity import (  # noqa: F401
    _simulate_limit_order_queue,
    _simulate_market_order_slippage,
    _get_instrument_type_slippage,
    _simulate_order_cancel,
    _compute_fill_quantity,
    _apply_fidelity_presets,
    _get_expiry_slippage_multiplier,
)
