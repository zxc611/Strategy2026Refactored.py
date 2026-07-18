# MODULE_ID: M1-162
# [M1-95] 回测编排器
from __future__ import annotations

import logging
from infra._helpers import get_logger  # R9-5
import uuid
from typing import Any, Dict

import numpy as np
import pandas as pd

logger = get_logger(__name__)  # R9-5


def _run_backtest_init(params: Dict[str, float], bar_data: pd.DataFrame,
                       train: bool, strategy_type: str, bt,
                       _check_bar_data_monotonic_fn, _apply_fidelity_presets_fn,
                       _sync_random_seed_fn, _get_life_estimator_fn) -> Dict[str, Any]:
    from infra.shared_utils import generate_prefixed_id
    _trial_isolation_flag = generate_prefixed_id("", 12)  # R9-3
    try:
        import threading as _threading_mod
        _threading_mod.current_thread().trial_isolation_flag = _trial_isolation_flag
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
        logging.debug("[R3-L2] threading trial_isolation_flag skipped: %s", _e)
        pass
    if bar_data.empty:
        return {"error": "无数据", "params": params}

    try:
        from strategy.strategy_core_service import StrategyCoreService
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

    _check_bar_data_monotonic_fn(bar_data)
    _apply_fidelity_presets_fn(params)
    _sync_random_seed_fn(42 if train else 24)

    if params.get("shadow_param_violation", False):
        bt.new_open_blocked = True
        logger.warning("[P1-R9-11] 影子参数独立性违反，回测新开仓已阻断")

    try:
        from config.state_param import get_state_param_manager
        _spm = get_state_param_manager()
        _spm_params = _spm.get_params(strategy_type)
        if _spm_params and isinstance(_spm_params, dict):
            params = {**params, **_spm_params}
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _d07_e:
        logger.debug("[R3-D-07] StateParamManager加载失败(使用默认params): %s", _d07_e)

    if "position_scale" not in params:
        try:
            from config.config_service import get_cached_params
            _cp = get_cached_params()
            _ps_key = f"{strategy_type}_position_scale"
            params["position_scale"] = float(_cp.get(_ps_key, _cp.get("position_scale", 1.0)))
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] position_scale config load skipped: %s", _r3_err)
            params.setdefault("position_scale", 1.0)

    estimator = _get_life_estimator_fn()
    if estimator is not None and (not hasattr(estimator, '_life_dict') or not estimator._life_dict):
        try:
            estimator.build_life_dict(bar_data)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
            logger.error("[R3-P-09] build_life_dict失败(严重): %s, 寿命信息缺失将导致D3维度退化为0.5", _e)
            bt.get("diagnostics", {}).setdefault("degraded_features", []).append("life_dict")

    return params


def _run_backtest_finalize(bt, bar_data: pd.DataFrame, params: Dict[str, float],
                           strategy_type: str,
                           _build_backtest_result_fn) -> Dict[str, Any]:
    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    if len(bt.positions) > 0:
        try:
            from param_pool.checks_individual import _force_close_all_positions
            _last_bar = bar_data.iloc[-1] if bar_data is not None and len(bar_data) > 0 else None
            if _last_bar is not None:
                _force_close_all_positions(bt, _last_bar, params, reason="backtest_end_force_close")
        except ImportError:
            try:
                from param_pool.checks_individual import _force_close_all_positions
                _last_bar = bar_data.iloc[-1] if bar_data is not None and len(bar_data) > 0 else None
                if _last_bar is not None:
                    _force_close_all_positions(bt, _last_bar, params, reason="backtest_end_force_close")
            except ImportError:
                pass

    return _build_backtest_result_fn(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )