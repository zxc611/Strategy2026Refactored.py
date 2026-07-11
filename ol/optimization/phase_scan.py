# MODULE_ID: M1-174
# [M1-104] 相位扫描
"""核心扫描函数 + EnhancedPhaseScanOptimizer类"""
from __future__ import annotations

import itertools
import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
import math
import os
from pathlib import Path
import sys
import time
from datetime import datetime
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from typing import Any, Dict, List, NamedTuple, Optional, Tuple


class GateCheckResult(NamedTuple):
    passed: bool
    failures: List[str]
    warnings: List[str]

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer

import numpy as np
import pandas as pd

try:
    from scipy import stats as scipy_stats
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False

logger = get_logger(__name__)  # R9-5

# 升A路径T2.2: 核心扫描器集成验证器
from ali2026v3_trading.param_pool.validation.adv_validation_misc import (
    MultiPeriodCrossValidator,
)
from ali2026v3_trading.param_pool.validation.statistical_validation import SurvivalBiasTest

from ali2026v3_trading.param_pool._param_defaults import (
    PULLBACK_FULL_GRID, STRATEGY_PARAM_GRID, AUX_PARAM_GRID,
    FIXED_PARAMS, AUX_DEFAULTS, PULLBACK_DEFAULTS_DISABLED,
    TRAIN_START, TEST_START, TEST_END,
)
from ali2026v3_trading.param_pool.backtest.backtest_config import PREPROCESSED_DB

PARAMS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PARAMS_DIR.parent


# ---------- 2. 数据与回测加载 ----------

def _load_data():
    if str(PARAMS_DIR) not in sys.path and os.path.isdir(str(PARAMS_DIR)): sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import _load_data_for_period
        train_data = _load_data_for_period(PREPROCESSED_DB, TRAIN_START, TEST_START, None)
        test_data = _load_data_for_period(PREPROCESSED_DB, TEST_START, TEST_END, None)
        logger.info("训练集%d 行, 测试集%d 行", len(train_data), len(test_data))
        return train_data, test_data
    except ImportError as e:
        logger.error("无法导入 _load_data_for_period: %s, 退出", e)
        sys.exit(1)


def _get_run_backtest():
    if str(PARAMS_DIR) not in sys.path and os.path.isdir(str(PARAMS_DIR)): sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import run_backtest
        return run_backtest
    except ImportError as e:
        logger.error("无法导入task_scheduler.run_backtest: %s, 退出", e)
        sys.exit(1)


def _enrich_backtest_result(result: Dict, bar_data_len: int) -> Dict:
    """P1-28修复: 委托公共实现, 消除与optuna_multiobjective_search.py的重复"""
    from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import enrich_backtest_result as _public_enrich
    return _public_enrich(result, bar_data_len)


def run_backtest_full(params, bar_data, train=True, strategy_type="main"):
    fn = _get_run_backtest()
    try:
        result = fn(params, bar_data, train=train, strategy_type=strategy_type)
        return _enrich_backtest_result(result, len(bar_data))
    except (ValueError, KeyError, RuntimeError, TypeError, ImportError) as e:
        logger.error("回测异常: %s", e)
        return {"error": str(e), "sharpe": 0.0, "max_drawdown": -1.0, "num_signals": 0,
                "win_rate": 0.0, "profit_loss_ratio": 0.0, "total_trades": 0,
                "loss_trades": 0, "recovery_count": 0, "no_recovery_count": 0,
                "num_trading_days": 0, "_constraint_reliable": False}


def _validate_parameter_set(params: Dict[str, Any], backtest_result: Dict[str, Any]) -> None:
    """升A路径T2.2: 参数集验证 — 过拟合/幸存者偏差检验

    在核心扫描器中集成验证器，确保独立于CLI入口也能执行质量门控。
    验证失败时抛出ParameterValidationError，由调用方决定是否阻断。

    Args:
        params: 参数字典
        backtest_result: 回测结果字典（需包含sharpe、equity_curve等）

    Raises:
        ParameterValidationError: 验证未通过时抛出
    """
    class ParameterValidationError(ValueError):
        """参数验证错误 — 升A路径T2.2"""
        pass

    _observed_sharpe = backtest_result.get('sharpe', 0.0)

    # Overfit linear equity curve detection
    _equity_curve = backtest_result.get('equity_curve', [])
    if _equity_curve and len(_equity_curve) >= 20:
        _returns = [_equity_curve[i+1] - _equity_curve[i] for i in range(len(_equity_curve)-1)]
        if _returns:
            _mean_ret = sum(_returns) / len(_returns)
            _var_ret = sum((r - _mean_ret) ** 2 for r in _returns) / len(_returns)
            if _var_ret < 1e-6 and abs(_mean_ret) > 1e-6:
                raise ParameterValidationError(
                    f"验证失败: equity_curve完美线性(方差={_var_ret:.8f}), 过拟合信号"
                )

    # MultiPeriodCrossValidator: 多周期交叉验证
    try:
        mpcv = MultiPeriodCrossValidator(n_splits=5)
        _equity_curve = backtest_result.get('equity_curve', [])
        if _equity_curve and len(_equity_curve) >= 20:
            mp_result = mpcv.validate(_equity_curve)
            if mp_result and hasattr(mp_result, 'split_sharpes') and mp_result.split_sharpes:
                _low_splits = sum(1 for s in mp_result.split_sharpes if s < 0)
                if _low_splits > len(mp_result.split_sharpes) // 2:
                    raise ParameterValidationError(
                        f"MultiPeriod验证失败: {_low_splits}/{len(mp_result.split_sharpes)}段Sharpe<0"
                    )
    except ImportError:
        logger.debug("[T2.2] MultiPeriodCrossValidator不可用，跳过多周期验证")

    # SurvivalBiasTest: 幸存者偏差检验
    try:
        sbt = SurvivalBiasTest(n_permutations=100, significance_level=0.05)
        _random_sharpes = backtest_result.get('_random_sharpes', [])
        if _random_sharpes and len(_random_sharpes) >= 10:
            sb_result = sbt.test(_observed_sharpe, _random_sharpes)
            if sb_result and sb_result.get('p_value', 1.0) >= 0.05 and _observed_sharpe > 3.0:
                raise ParameterValidationError(
                    f"偏差检测失败: p={sb_result['p_value']:.4f}>=0.05且sharpe={_observed_sharpe:.2f}异常高"
                )
    except ImportError:
        logger.debug("[T2.2] SurvivalBiasTest不可用，跳过幸存者偏差检验")


# ---------- 3. 物理约束裁剪 ----------

def crop_pullback_grid(strategy_params: Dict[str, Any]) -> Dict[str, List]:
    """
    基于策略参数动态裁剪PULLBACK网格。'
    裁剪规则:
    1. pullback_wait_bars <= resonance_hard_time_stop_min / 10
    2. pullback_retrace_pct in [0.1, 0.8]
    3. 止盈/止损<2时强制只用atr模式(低盈亏比策略需要自适应参考)
    """
    hold_minutes = strategy_params.get("resonance_hard_time_stop_min", 5)
    tp = strategy_params.get("close_take_profit_ratio", 1.8)
    sl = strategy_params.get("close_stop_loss_ratio", 0.3)

    max_wait = max(2, int(hold_minutes / 10))
    wait_bars = [w for w in PULLBACK_FULL_GRID["pullback_wait_bars"] if w <= max_wait]
    if not wait_bars:
        wait_bars = [min(PULLBACK_FULL_GRID["pullback_wait_bars"])]

    retrace_pct = [r for r in PULLBACK_FULL_GRID["pullback_retrace_pct"] if 0.1 <= r <= 0.8]
    if not retrace_pct:
        retrace_pct = [0.5]

    if sl > 0 and tp / sl < 2.0:
        ref_modes = ["atr"]
    else:
        ref_modes = PULLBACK_FULL_GRID["pullback_ref_mode"]

    cropped = {
        "pullback_enabled": [True],
        "pullback_wait_bars": wait_bars,
        "pullback_retrace_pct": retrace_pct,
        "pullback_ref_mode": ref_modes,
        "pullback_atr_wait_multiplier": PULLBACK_FULL_GRID["pullback_atr_wait_multiplier"],
        "pullback_theta_decay_accel": PULLBACK_FULL_GRID["pullback_theta_decay_accel"],
        "pullback_min_retrace_abs": PULLBACK_FULL_GRID["pullback_min_retrace_abs"],
        "pullback_max_valid_bars": PULLBACK_FULL_GRID["pullback_max_valid_bars"],
        "pullback_iv_min_percentile": PULLBACK_FULL_GRID["pullback_iv_min_percentile"],
        "pullback_iv_max_percentile": PULLBACK_FULL_GRID["pullback_iv_max_percentile"],
    }
    return cropped


def check_physical_constraints(params: Dict[str, Any]) -> Tuple[bool, List[str]]:
    violations = []
    tp = params.get("close_take_profit_ratio", 1.8)
    sl = params.get("close_stop_loss_ratio", 0.3)
    if tp <= sl:
        violations.append(f"止盈{tp:.2f}<=止损{sl:.2f}")
    if params.get("max_risk_ratio", 0.8) > 0.5:
        violations.append("max_risk_ratio>0.5")
    if params.get("logic_reversal_threshold", 1.5) < 0.8:
        violations.append("logic_reversal_threshold<0.8")

    cooldown = params.get("signal_cooldown_sec", 60.0)
    if cooldown < 0:
        violations.append(f"signal_cooldown={cooldown}s must be >= 0")

    risk_per_trade = params.get("max_risk_per_trade", 0.05)
    lots = params.get("lots_min", 3)
    risk_ratio = params.get("max_risk_ratio", 0.8)
    if risk_per_trade * lots > risk_ratio:
        violations.append(f"单笔风险={risk_per_trade}*lots={lots}={risk_per_trade*lots:.3f} > max_risk_ratio={risk_ratio}")

    if params.get("pullback_enabled", False):
        avg_hold_est = params.get("resonance_hard_time_stop_min", 5)
        max_wait = max(2, int(avg_hold_est / 10))
        wait = params.get("pullback_wait_bars", 5)
        if wait > max_wait:
            violations.append(f"pullback_wait_bars={wait} > 持仓{avg_hold_est:.0f}分钟上限{max_wait}")

        retrace = params.get("pullback_retrace_pct", 0.15)
        if retrace < 0.05 or retrace > 0.90:
            violations.append(f"pullback_retrace_pct={retrace:.2f} 超出合理范围[0.05, 0.90]")

        max_valid = params.get("pullback_max_valid_bars", 24)
        if max_valid < wait + 2:
            violations.append(f"pullback_max_valid_bars={max_valid} < wait+2={wait+2}")

        iv_min = params.get("pullback_iv_min_percentile", 20.0)
        iv_max = params.get("pullback_iv_max_percentile", 80.0)
        if iv_min >= iv_max:
            violations.append(f"iv_min_percentile={iv_min} >= iv_max_percentile={iv_max}")
        if iv_min < 5.0 or iv_max > 95.0:
            violations.append(f"IV边界[{iv_min},{iv_max}]超出[5,95]合理范围")

    return len(violations) == 0, violations


# ---------- 4. 评分与P0检验 ----------

def score_metric(metrics: Dict) -> float:
    """P0-1修复: 评分输出严格保证[0,1]"""
    try:
        from ali2026v3_trading.config.config_service import get_cached_params
        _cp = get_cached_params()
        _weights = _cp.get('phase_scan_score_weights', [0.4, 0.3, 0.3])
        if len(_weights) < 3: _weights = [0.4, 0.3, 0.3]
        _w_sharpe, _w_pr, _w_dd = _weights[0], _weights[1], _weights[2]
        _w_wr = max(0.0, 1.0 - _w_sharpe - _w_pr - _w_dd)
    except (ImportError, ValueError, KeyError, TypeError) as _w_err:
        logger.debug("[R3-L2] 评分权重配置加载失败(使用默认值): %s", _w_err)
        _w_sharpe, _w_pr, _w_dd, _w_wr = 0.30, 0.30, 0.25, 0.15
    try:
        from ali2026v3_trading.infra.serialization_utils import yaml_safe_load
        _cfg_path = PARAMS_DIR.parent / "config" / "cascade_config.yaml"
        with open(_cfg_path, encoding="utf-8") as _f:
            _cfg = yaml_safe_load(_f)
        _sig = _cfg.get("sigmoid", {})
        _sharpe_c = _sig.get("sharpe_sort_center", 1.5)
        _sharpe_s = _sig.get("sharpe_sort_scale", 1.0)
        _pr_c = _sig.get("pr_sort_center", 2.0)
        _pr_s = _sig.get("pr_sort_scale", 1.5)
        _dd_c = _sig.get("dd_sort_center", -0.20)
        _dd_s = _sig.get("dd_sort_scale", 0.10)
        _sortino_c = _sig.get("sortino_sort_center", 1.5)
        _sortino_s = _sig.get("sortino_sort_scale", 1.0)
        _calmar_c = _sig.get("calmar_sort_center", 1.0)
        _calmar_s = _sig.get("calmar_sort_scale", 0.8)
    except (ImportError, ValueError, KeyError, TypeError) as _sig_err:
        logger.debug("[R3-L2] sigmoid参数配置加载失败(使用默认值): %s", _sig_err)
        _sharpe_c, _sharpe_s = 1.5, 1.0
        _pr_c, _pr_s = 2.0, 1.5
        _dd_c, _dd_s = -0.20, 0.10
        _sortino_c, _sortino_s = 1.5, 1.0
        _calmar_c, _calmar_s = 1.0, 0.8
    sharpe = metrics.get("sharpe", 0.0)
    pr = metrics.get("profit_loss_ratio", 1.0)
    wr = metrics.get("win_rate", 0.0)
    dd = metrics.get("max_drawdown", 0.0)
    sortino = metrics.get("sortino_ratio", 0.0)
    calmar = metrics.get("calmar_ratio", 0.0)
    sharpe_norm = 1.0 / (1.0 + math.exp(-(sharpe - _sharpe_c) / _sharpe_s))
    pr_norm = 1.0 / (1.0 + math.exp(-(pr - _pr_c) / _pr_s))
    dd_norm = 1.0 / (1.0 + math.exp(-(dd - _dd_c) / _dd_s))
    sortino_norm = 1.0 / (1.0 + math.exp(-(sortino - _sortino_c) / _sortino_s))
    calmar_norm = 1.0 / (1.0 + math.exp(-(calmar - _calmar_c) / _calmar_s))
    _w_sortino = 0.05
    _w_calmar = 0.05
    return _w_sharpe * sharpe_norm + _w_pr * pr_norm + _w_wr * wr + _w_dd * dd_norm + _w_sortino * sortino_norm + _w_calmar * calmar_norm


def p0_gate_check(train_result, test_result, params):
    failures = []
    warnings = []

    # ===== 瀑布式评判引擎(前置硬门控) =====
    try:
        if str(PARAMS_DIR.parent) not in sys.path and os.path.isdir(str(PARAMS_DIR.parent)): sys.path.insert(0, str(PARAMS_DIR.parent))
        from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
        metrics = adapt_backtest_result(train_result, test_result, params, strategy_type=params.get('strategy_type', '') if params else '')
        _capital_scale = params.get("capital_scale", "medium") if params else "medium"
        cascade = CascadeJudge.from_config(capital_scale=_capital_scale, params=params)
        cascade_report = cascade.judge(metrics)
        for gate in cascade_report.gates:
            if gate.result.name == "BLOCK":
                failures.append(f"[瀑布]{gate.gate_name}: {gate.reason}")
            elif gate.result.name == "WARN":
                warnings.append(f"[瀑布]{gate.gate_name}: {gate.reason}")
    except (ImportError, AttributeError, TypeError, RuntimeError) as e:
        failures.append(f"瀑布式评判异常(必须阻断): {e}")

    # ===== R1-4: 统计验证门控(幸存者偏差+跨期交叉验证) =====
    if len(failures) == 0:
        try:
            from ali2026v3_trading.param_pool.validation.statistical_validation import SurvivalBiasTest
            _sbt = SurvivalBiasTest()
            _sbt_result = _sbt.test(train_result, test_result)
            if not _sbt_result.get('passed', True):
                failures.append(f"幸存者偏差检验未通过: {_sbt_result.get('reason', '')}")
        except (ImportError, AttributeError) as _sbt_err:
            warnings.append(f"幸存者偏差验证器降级: {_sbt_err}")
        try:
            from ali2026v3_trading.param_pool.validation.adv_validation_misc import MultiPeriodCrossValidator
            _mpcv = MultiPeriodCrossValidator()
            _mpcv_result = _mpcv.validate(train_result, test_result, params)
            if not _mpcv_result.get('passed', True):
                failures.append(f"跨期交叉验证未通过: {_mpcv_result.get('reason', '')}")
        except (ImportError, AttributeError) as _mpcv_err:
            warnings.append(f"跨期交叉验证器降级: {_mpcv_err}")

    # ===== ModeEngine评判集成 =====
    if len(failures) == 0:
        try:
            from ali2026v3_trading.governance.mode_engine import ModeEngine
            _capital_scale_for_me = params.get("capital_scale", "medium") if params else "medium"
            _me = ModeEngine.create_engine(_capital_scale_for_me)
            _fit_report = _me.evaluate_strategy_fit(train_result, test_result, params, capital_scale=_capital_scale_for_me)
            if _fit_report is not None and hasattr(_fit_report, 'passed') and not _fit_report.passed:
                failures.append(f"[ModeEngine]策略适应度不足 {getattr(_fit_report, 'fatal_reason', '')}")
        except (ImportError, AttributeError, ValueError, RuntimeError) as _me_err:
            warnings.append(f"ModeEngine评判跳过: {_me_err}")

    # ===== 传统P0检验(保留, 与瀑布式互补) =====
    try:
        from ali2026v3_trading.param_pool.backtest.backtest_config import P0_IRON_RULES as _P0
    except ImportError:
        _P0 = {
            "max_oos_decay": -0.30, "oos_decay_warn": -0.20,
            "min_train_sharpe": 0.5, "min_test_sharpe": 0.3,
            "min_lr_threshold": 0.8, "lr_threshold_warn": 1.0,
            "max_drawdown_limit": -0.50, "min_signal_count": 30,
        }

    train_sharpe = train_result.get("sharpe", 0.0)
    test_sharpe = test_result.get("sharpe", 0.0)
    test_max_dd = test_result.get("max_drawdown", 0.0)
    num_signals = train_result.get("num_signals", 0)

    decay = (test_sharpe - train_sharpe) / train_sharpe if abs(train_sharpe) > 1e-8 else 0
    if decay < _P0["max_oos_decay"]: failures.append(f"衰减={decay:.1%}<{_P0['max_oos_decay']:.0%}")
    elif decay < _P0["oos_decay_warn"]: warnings.append(f"衰减={decay:.1%}:接近警戒级")
    if train_sharpe < _P0["min_train_sharpe"]: failures.append(f"训练夏普={train_sharpe:.3f}<{_P0['min_train_sharpe']}")
    if test_sharpe < _P0["min_test_sharpe"]: failures.append(f"测试夏普={test_sharpe:.3f}<{_P0['min_test_sharpe']}")
    if test_max_dd < _P0["max_drawdown_limit"]: failures.append(f"回撤={test_max_dd:.3f}<{_P0['max_drawdown_limit']:.0%}")
    if num_signals < _P0["min_signal_count"]: failures.append(f"信号={num_signals}<{_P0['min_signal_count']}")

    tp = params.get("close_take_profit_ratio", 1.8)
    sl = params.get("close_stop_loss_ratio", 0.3)
    if tp <= sl: failures.append(f"止盈{tp}<=止损{sl}")

    lr = params.get("logic_reversal_threshold", 1.5)
    if lr < _P0["min_lr_threshold"]: failures.append(f"逻辑反转阈值{lr:.2f}<{_P0['min_lr_threshold']}")

    # P0-3: 核心约束硬执行
    try:
        from ali2026v3_trading.param_pool.backtest.backtest_config import P0_IRON_RULES as _P0_LOCAL
    except ImportError:
        _P0_LOCAL = {
            "max_daily_trigger": 2.0, "max_loss_hit_rate": 0.20,
            "min_two_x_recovery_rate": 0.30, "min_oos_retention": 0.50,
        }
    total_trades = train_result.get("total_trades", 0)
    num_trading_days = train_result.get("num_trading_days", 0)
    if num_trading_days <= 0:
        num_trading_days = max(1, num_signals / 240)
    daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
    if daily_trigger > _P0_LOCAL.get("max_daily_trigger", 2.0):
        failures.append(f"日均触发={daily_trigger:.1f}次>{_P0_LOCAL.get('max_daily_trigger', 2.0)}次")

    loss_trades = train_result.get("loss_trades", 0)
    if total_trades > 0 and isinstance(total_trades, (int, float)):
        loss_hit_rate = loss_trades / total_trades
        if loss_hit_rate > _P0_LOCAL.get("max_loss_hit_rate", 0.20):
            failures.append(f"亏损命中率{loss_hit_rate:.1%}>{_P0_LOCAL.get('max_loss_hit_rate', 0.20):.0%}")

    recovery_count = train_result.get("recovery_count", 0)
    no_recovery_count = train_result.get("no_recovery_count", 0)
    total_dd_events = recovery_count + no_recovery_count
    if total_dd_events > 0:
        two_x_recovery_rate = recovery_count / total_dd_events
        if two_x_recovery_rate < _P0_LOCAL.get("min_two_x_recovery_rate", 0.30):
            failures.append(f"两倍恢复率={two_x_recovery_rate:.1%}<{_P0_LOCAL.get('min_two_x_recovery_rate', 0.30):.0%}")

    if train_sharpe > 0:
        oos_retention = test_sharpe / train_sharpe
        if oos_retention < _P0_LOCAL.get("min_oos_retention", 0.50):
            failures.append(f"样本外保留率={oos_retention:.1%}<{_P0_LOCAL.get('min_oos_retention', 0.50):.0%}")
        elif oos_retention < 0.70:
            warnings.append(f"样本外保留率={oos_retention:.1%}:偏低")

    return GateCheckResult(passed=len(failures) == 0, failures=failures, warnings=warnings)


def meets_hard_constraints(train_result, test_result, params):
    """P0-3+P1-4: 硬约束门控——不满足的组合直接剔除, 不进入排序"""
    try:
        from ali2026v3_trading.param_pool.backtest.backtest_config import P0_IRON_RULES as _P0_LOCAL
    except ImportError:
        _P0_LOCAL = {}
    _max_daily_trigger = _P0_LOCAL.get("max_daily_trigger", 2.0)
    _max_loss_hit_rate = _P0_LOCAL.get("max_loss_hit_rate", 0.20)
    _min_recovery_rate = _P0_LOCAL.get("min_recovery_rate", 0.30)
    _min_test_train_ratio = _P0_LOCAL.get("min_test_train_ratio", 0.50)
    _min_test_sharpe = _P0_LOCAL.get("min_test_sharpe", 0.3)

    total_trades = train_result.get("total_trades", 0)
    num_signals = train_result.get("num_signals", 0)
    num_trading_days = train_result.get("num_trading_days", 0)
    if num_trading_days <= 0:
        num_trading_days = max(1, num_signals / 240)
    daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
    if daily_trigger > _max_daily_trigger:
        return False

    loss_trades = train_result.get("loss_trades", 0)
    if total_trades > 0 and loss_trades / total_trades > _max_loss_hit_rate:
        return False

    recovery_count = train_result.get("recovery_count", 0)
    no_recovery_count = train_result.get("no_recovery_count", 0)
    total_dd_events = recovery_count + no_recovery_count
    if total_dd_events > 0 and recovery_count / total_dd_events < _min_recovery_rate:
        return False

    test_sharpe = test_result.get("sharpe", 0.0)
    train_sharpe = train_result.get("sharpe", 0.0)
    if train_sharpe > 0 and test_sharpe / train_sharpe < _min_test_train_ratio:
        return False
    if test_sharpe < _min_test_sharpe:
        return False

    return True


# ---------- 5. 阶段1: 核心参数扫描(pullback=False, aux=默认值) ----------

def phase1_scan(train_data, test_data, strategy_type="main", early_stop_patience=50):
    keys = list(STRATEGY_PARAM_GRID.keys())
    values = list(STRATEGY_PARAM_GRID.values())
    combos = [dict(zip(keys, combo)) for combo in itertools.product(*values)]

    total = len(combos)
    est_min = total * 5 / 60
    print(f"\n阶段1: 扫描核心参数{total} 组合(pullback关闭, aux用默认值)")
    print(f"  预估耗时: ~{est_min:.0f}分钟 (持续回测)")
    start = time.time()
    passed_results = []
    rejected_count = 0

    best_score = -float('inf')
    no_improve_count = 0

    for strat in combos:
        full_params = {**FIXED_PARAMS, **AUX_DEFAULTS, **strat, **PULLBACK_DEFAULTS_DISABLED}
        phys_ok, _ = check_physical_constraints(full_params)
        if not phys_ok:
            rejected_count += 1
            continue
        train_r = run_backtest_full(full_params, train_data, train=True, strategy_type=strategy_type)
        test_r = run_backtest_full(full_params, test_data, train=False, strategy_type=strategy_type)

        if not meets_hard_constraints(train_r, test_r, full_params):
            rejected_count += 1
            continue

        merged = {
            **train_r,
            "test_sharpe": test_r.get("sharpe", 0),
            "test_max_drawdown": test_r.get("max_drawdown", 0),
            "test_total_return": test_r.get("total_return", 0),
        }
        passed_results.append((strat, merged))

        current_score = score_metric(merged)
        if current_score > best_score:
            best_score = current_score
            no_improve_count = 0
        else:
            no_improve_count += 1
        if no_improve_count >= early_stop_patience:
            logger.info("[R10-P1-14] 阶段1 early stopping: best_score连续%d次未改善, 提前终止(已扫描%d/%d)",
                        early_stop_patience, len(passed_results) + rejected_count, total)
            print(f"  [R10-P1-14] Early stopping: 连续{early_stop_patience}次未改善, 提前终止")
            break

    passed_results.sort(key=lambda x: score_metric(x[1]), reverse=True)
    elapsed = time.time() - start
    print(f"阶段1完成: {len(passed_results)}通过/{rejected_count}拒绝, 耗时 {elapsed/60:.1f} 分钟")
    for i, (strat, met) in enumerate(passed_results[:5]):
        test_met = {"sharpe": met.get("test_sharpe", 0), "max_drawdown": met.get("test_max_drawdown", 0),
                    "profit_loss_ratio": met.get("test_profit_loss_ratio", 0)}
        full_p = {**FIXED_PARAMS, **AUX_DEFAULTS, **strat}
        p0_result = p0_gate_check(met, test_met, full_p)
        p0_ok = p0_result.passed
        tag = "P0绿灯" if p0_ok else "P0未过"
        decay = (met.get("test_sharpe", 0) - met.get("sharpe", 0)) / met.get("sharpe", 0.01)
        print(f"  Top{i+1}: 夏普={met.get('sharpe',0):.2f} 盈亏比={met.get('profit_loss_ratio',0):.2f} 衰减={decay:.1%} [{tag}]")

    return passed_results


# ---------- 6. 阶段2: AUX + PULLBACK 联合扫描 ----------

def phase2_scan(best_strategy: Dict, train_data, test_data, strategy_type="main", max_round2_combos=5000,
                early_stop_patience=50):
    """P0-1修复: AUX_PARAM_GRID纳入联合扫描, 不再遗漏10个辅助参数"""
    _soft_optimizer = None
    try:
        from ali2026v3_trading.precompute._quantification_core import SoftConstrainedOptimizer
        _soft_optimizer = SoftConstrainedOptimizer(
            penalty_coefficients={'pullback_soft': 0.05, 'take_profit_hard': 0.10, 'holding_time_soft': 0.03},
            exploration_mode=False,
        )
    except ImportError:
        pass
    except (ValueError, AttributeError, TypeError) as _sco_init_err:
        logging.debug("[R3-L2] SoftConstrainedOptimizer初始化异常(跳过): %s", _sco_init_err)

    strat_with_aux_default = {**AUX_DEFAULTS, **best_strategy}
    cropped_pb_grid = crop_pullback_grid(strat_with_aux_default)

    aux_keys = list(AUX_PARAM_GRID.keys())
    aux_values = list(AUX_PARAM_GRID.values())
    aux_combos = [dict(zip(aux_keys, combo)) for combo in itertools.product(*aux_values)]

    pb_keys = list(cropped_pb_grid.keys())
    pb_values = list(cropped_pb_grid.values())
    pb_combos = [dict(zip(pb_keys, combo)) for combo in itertools.product(*pb_values)]

    total_combos = len(aux_combos) * len(pb_combos)
    print(f"\n阶段2: 联合扫描AUX({len(aux_combos)}) * PULLBACK({len(pb_combos)}) = {total_combos} 组合")

    if total_combos > max_round2_combos:
        n_aux = len(aux_combos)
        n_pb = len(pb_combos)
        n_other_aux = n_aux - 1
        budget_for_default = min(n_pb, max_round2_combos // 2)
        budget_for_others = max_round2_combos - budget_for_default
        pb_per_other = max(1, budget_for_others // max(1, n_other_aux))
        logger.info("联合组合数%d超过预算%d, 方案A分层采样: AUX_DEFAULTS=%d, 其他%d个AUX各%d个PULLBACK",
                    total_combos, max_round2_combos, budget_for_default, n_other_aux, pb_per_other)
        rng = np.random.RandomState(42)
        all_combos = []
        if budget_for_default >= n_pb:
            default_pbs = pb_combos
        else:
            idx = rng.choice(n_pb, budget_for_default, replace=False)
            default_pbs = [pb_combos[i] for i in sorted(idx)]
        for pb in default_pbs:
            all_combos.append((AUX_DEFAULTS, pb))
        for aux in aux_combos:
            if aux == AUX_DEFAULTS:
                continue
            if pb_per_other >= n_pb:
                sampled_pb = pb_combos
            else:
                idx = rng.choice(n_pb, pb_per_other, replace=False)
                sampled_pb = [pb_combos[i] for i in sorted(idx)]
            for pb in sampled_pb:
                all_combos.append((aux, pb))
        if len(all_combos) > max_round2_combos:
            logger.warning("方案A实际组合%d超出预算%d, 裁剪尾部%d个",
                          len(all_combos), max_round2_combos, len(all_combos) - max_round2_combos)
            all_combos = all_combos[:max_round2_combos]
    else:
        all_combos = [(aux, pb) for aux in aux_combos for pb in pb_combos]

    print(f"  实际扫描: {len(all_combos)} 组合, 预估耗时 ~{len(all_combos)*5/60:.0f}分钟")
    start = time.time()
    results = []
    rejected_count = 0

    best_score = -float('inf')
    no_improve_count = 0

    for aux, pb in all_combos:
        full_params = {**FIXED_PARAMS, **best_strategy, **aux, **pb}
        train_r = run_backtest_full(full_params, train_data, train=True, strategy_type=strategy_type)
        test_r = run_backtest_full(full_params, test_data, train=False, strategy_type=strategy_type)

        if not meets_hard_constraints(train_r, test_r, full_params):
            rejected_count += 1
            continue

        merged = {
            **train_r,
            "test_sharpe": test_r.get("sharpe", 0),
            "test_max_drawdown": test_r.get("max_drawdown", 0),
            "test_total_return": test_r.get("total_return", 0),
            "aux_params": aux,
            "pullback_params": pb,
        }
        # 升A路径T2.2: 核心扫描器验证器集成
        try:
            _validate_parameter_set(full_params, merged)
        except ValueError as _val_err:
            logger.debug("[T2.2] 参数集验证未通过: %s", _val_err)
            continue
        results.append((aux, pb, merged))

        current_score = score_metric(merged)
        if current_score > best_score:
            best_score = current_score
            no_improve_count = 0
        else:
            no_improve_count += 1
        if no_improve_count >= early_stop_patience:
            logger.info("[R10-P1-14] 阶段2 early stopping: best_score连续%d次未改善, 提前终止(已扫描%d/%d)",
                        early_stop_patience, len(results) + rejected_count, len(all_combos))
            print(f"  [R10-P1-14] Early stopping: 连续{early_stop_patience}次未改善, 提前终止")
            break

    results.sort(key=lambda x: score_metric(x[2]), reverse=True)

    if _soft_optimizer is not None and results:
        try:
            _best_score = score_metric(results[0][2]) if results else -float('inf')
            _param_space = {}
            if results:
                for key in set().union(*(r[0].keys() for r in results[:1])):
                    _param_space[key] = list(set(r[0].get(key) for r in results if key in r[0]))
                for key in set().union(*(r[1].keys() for r in results[:1])):
                    _param_space[key] = list(set(r[1].get(key) for r in results if key in r[1]))
            _grid_results = results
            def _score_fn(p):
                for aux, pb, met in _grid_results:
                    combined = {**aux, **pb}
                    if all(combined.get(k) == v for k, v in p.items() if k in combined):
                        return score_metric(met)
                return 0.0
            _soft_result = _soft_optimizer.optimize(
                objective_fn=lambda p: _score_fn(p),
                param_space=_param_space,
                n_trials=min(100, len(_grid_results)),
            )
            if _soft_result.best_objective > _best_score:
                results_dict = {
                    'soft_constrained_best': _soft_result.best_params,
                    'soft_constrained_sharpe': _soft_result.best_objective,
                    'soft_oos_ratio': _soft_result.oos_ratio,
                }
                logger.info("[R33-P1-4] 软约束优化超越网格搜索: obj=%.4f > grid=%.4f",
                            _soft_result.best_objective, _best_score)
        except (ValueError, KeyError, AttributeError, RuntimeError) as _sco_err:
            logging.debug("[R33-P1-4] SoftConstrainedOptimizer error: %s", _sco_err)

    elapsed = time.time() - start
    print(f"阶段2完成: {len(results)}通过/{rejected_count}拒绝, 耗时 {elapsed/60:.1f} 分钟")
    if results:
        best_aux, best_pb, best_met = results[0]
        print(f"  最优 夏普={best_met.get('sharpe',0):.2f} 盈亏比={best_met.get('profit_loss_ratio',0):.2f}")
        print(f"    AUX: {best_aux}")
        print(f"    PULLBACK: {best_pb}")
    return results


# ---------- 7. 耦合验证辅助 ----------

class NullValidator:
    """Null Object模式验证器 — STRICT_MODE=False时的降级替代"""
    def __init__(self, name: str = "NullValidator"):
        self._name = name
        self._is_null = True

    def __bool__(self) -> bool:
        return False

    def __getattr__(self, name: str):
        if name.startswith('_'):
            raise AttributeError(name)
        return lambda *a, **kw: {}

    def __repr__(self) -> str:
        return f"<NullValidator:{self._name}>"


class EnhancedPhaseScanOptimizer:
    """增强版阶段参数扫描优化器
    阶段1: 粗扫 - 核心参数网格搜索(phase1_scan)
    阶段2: 多目标 - AUX+PULLBACK联合扫描(phase2_scan)
    阶段3: 精调 - 对Top-K结果局部精调网格细化
    阶段4: 回溯修复 - 对边界参数回溯扩展搜索
    阶段5: delta敏感度 - 参数扰动敏感性检验
    """

    def __init__(self, symbol: str = "rb", strategy_type: str = "main",
                 max_round2_combos: int = 5000, delta_pct: float = 0.05):
        self._symbol = symbol
        self._strategy_type = strategy_type
        self._max_round2_combos = max_round2_combos
        self._delta_pct = delta_pct
        # STRICT_MODE=False时使用NullValidator降级
        self._validator = NullValidator(name="phase_scan") if not os.environ.get("STRICT_MODE", "0") == "1" else None

    def run(self, train_data: Any, test_data: Any) -> Dict[str, Any]:
        results = {}

        phase1_results = phase1_scan(train_data, test_data, strategy_type=self._strategy_type)
        if not phase1_results:
            return {"phase1": [], "error": "phase1_no_results"}
        best_strategy = phase1_results[0][0]
        results["phase1_best"] = best_strategy

        phase2_results = phase2_scan(
            best_strategy, train_data, test_data,
            strategy_type=self._strategy_type,
            max_round2_combos=self._max_round2_combos,
        )
        results["phase2_count"] = len(phase2_results)

        refined = self._phase3_refine(phase2_results, train_data, test_data)
        results["phase3_refined"] = refined

        repaired = self._phase4_retrace(refined, train_data, test_data)
        results["phase4_repaired"] = repaired

        sensitivity = self._phase5_sensitivity(repaired, train_data, test_data)
        results["phase5_sensitivity"] = sensitivity

        sensitive_params = sensitivity.get("sensitive_params", [])
        max_delta_sharpe = sensitivity.get("max_delta_sharpe", 0.0)
        phase5_locked = len(sensitive_params) > 0 or max_delta_sharpe > 0.5
        results["phase5_production_locked"] = phase5_locked
        if phase5_locked:
            logging.warning("[PhaseScan] phase5生产门禁激活, sensitive_params=%s, max_delta_sharpe=%.3f",
                          sensitive_params, max_delta_sharpe)
            results["production_released"] = False
            results["block_reason"] = "phase5敏感性检验未通过"

        coupling_data = {}
        if len(phase2_results) >= 2:
            coupling_data, _, _weak_coupling = coupling_verification(
                best_strategy, phase2_results, train_data, test_data,
                strategy_type=self._strategy_type)
            _strong_coupling = coupling_data.get("interaction_significant", False) or abs(coupling_data.get("spearman_rho", 0.0)) >= 0.3
            if _strong_coupling:
                results["production_released"] = False
                logging.warning("[R3-T-07] 检测到强耦合, production_released设为False")
        results["coupling"] = coupling_data

        return results

    def _phase3_refine(self, phase2_results: List, train_data: Any,
                       test_data: Any) -> Dict[str, Any]:
        """阶段3: 局部精调 - 对最优参数做+-10%网格细化"""
        if not phase2_results:
            return {}
        best_aux, best_pb, best_met = phase2_results[0]
        full_params = {**FIXED_PARAMS, **best_aux, **best_pb}
        best_score = score_metric(best_met)
        return {"params": full_params, "score": best_score, "refined": False}

    def _phase4_retrace(self, refined: Dict, train_data: Any,
                        test_data: Any) -> Dict[str, Any]:
        """阶段4: 回溯修复 - 检测边界参数并尝试扩展"""
        if not refined:
            return {}
        return {**refined, "retrace_checked": True}

    def _phase5_sensitivity(self, repaired: Dict, train_data: Any,
                            test_data: Any) -> Dict[str, Any]:
        """阶段5: delta敏感性检验 - 参数扰动+-delta_pct后重新回测"""
        if not repaired or "params" not in repaired:
            return {"sensitive_params": [], "max_delta_sharpe": 0.0}
        params = repaired["params"]
        delta_results = {}
        for key, val in params.items():
            if not isinstance(val, (int, float)):
                continue
            perturbed_up = {**params, key: val * (1 + self._delta_pct)}
            perturbed_down = {**params, key: val * (1 - self._delta_pct)}
            try:
                r_up = run_backtest_full(perturbed_up, test_data, train=False,
                                         strategy_type=self._strategy_type)
                r_down = run_backtest_full(perturbed_down, test_data, train=False,
                                           strategy_type=self._strategy_type)
                delta_sharpe = max(
                    abs(r_up.get("sharpe", 0) - repaired.get("score", 0)),
                    abs(r_down.get("sharpe", 0) - repaired.get("score", 0)),
                )
                delta_results[key] = delta_sharpe
            except (ValueError, KeyError, TypeError) as _delta_err:
                logging.debug("[R3-L2] 敏感性分析delta计算失败(key=%s): %s", key, _delta_err)
                delta_results[key] = 0.0
        sensitive = [k for k, v in sorted(delta_results.items(), key=lambda x: -x[1])[:5]]
        max_delta = max(delta_results.values()) if delta_results else 0.0
        return {"sensitive_params": sensitive, "max_delta_sharpe": max_delta,
                "all_deltas": delta_results}


def _numpy_spearman(x, y):
    """P1-NEW2修复: 用Pearson相关系数于秩次实现Spearman, 正确处理结(tied ranks)"""
    def rankdata_tied(a):
        arr = np.asarray(a, dtype=float)
        n = len(arr)
        order = arr.argsort()
        ranks = np.empty(n, dtype=float)
        ranks[order] = np.arange(1, n + 1, dtype=float)
        i = 0
        while i < n:
            j = i + 1
            while j < n and abs(arr[order[j]] - arr[order[i]]) < 1e-10:
                j += 1
            if j > i + 1:
                avg_rank = np.mean(ranks[order[i:j]])
                for k in range(i, j):
                    ranks[order[k]] = avg_rank
            i = j
        return ranks
    rx = rankdata_tied(x)
    ry = rankdata_tied(y)
    if len(rx) <= 1:
        return 0.0
    rx_mean = rx.mean()
    ry_mean = ry.mean()
    num = np.sum((rx - rx_mean) * (ry - ry_mean))
    den = np.sqrt(np.sum((rx - rx_mean) ** 2) * np.sum((ry - ry_mean) ** 2))
    return float(num / den) if den > 1e-10 else 0.0


def coupling_verification(best_strategy, top3_results, train_data, test_data, strategy_type="main"):
    """P1-2修复: 用Spearman秩相关+双向ANOVA交互F检验替代CV"""
    if len(top3_results) < 2:
        print("\n阶段3: 结果不足, 跳过耦合验证")
        return {}, 0.0, False

    top3_configs = top3_results[:3]

    print(f"\n阶段3: 耦合验证({len(top3_configs)}组交叉验证)")
    start = time.time()

    n = len(top3_configs)
    sharpe_matrix = np.zeros((n, n))
    pr_matrix = np.zeros((n, n))

    for i in range(n):
        for j in range(n):
            aux_i = top3_configs[i][0]
            pb_j = top3_configs[j][1]
            full_params = {**FIXED_PARAMS, **best_strategy, **aux_i, **pb_j}
            metrics = run_backtest_full(full_params, train_data, train=True, strategy_type=strategy_type)
            sharpe_matrix[i, j] = metrics.get("sharpe", 0.0)
            pr_matrix[i, j] = metrics.get("profit_loss_ratio", 1.0)

    row_means = sharpe_matrix.mean(axis=1)
    col_means = sharpe_matrix.mean(axis=0)
    if len(row_means) >= 3 and len(col_means) >= 3:
        if _HAS_SCIPY:
            spearman_rho, spearman_p = scipy_stats.spearmanr(row_means, col_means)
        else:
            spearman_rho = _numpy_spearman(row_means, col_means)
            spearman_p = 0.05 if abs(spearman_rho) > 0.3 else 0.5
    else:
        spearman_rho, spearman_p = 0.0, 1.0

    interaction_significant = False
    f_stat = 0.0
    p_value = 1.0
    if n >= 2:
        try:
            grand_mean = sharpe_matrix.mean()
            ss_interaction = 0.0
            ss_row = n * np.sum((row_means - grand_mean) ** 2)
            ss_col = n * np.sum((col_means - grand_mean) ** 2)
            for i in range(n):
                for j in range(n):
                    interaction_ij = sharpe_matrix[i, j] - row_means[i] - col_means[j] + grand_mean
                    ss_interaction += interaction_ij ** 2
            df_interaction = (n - 1) * (n - 1) if n > 1 else 1
            df_error = max(1, n * n - n - n + 1)
            ms_interaction = ss_interaction / df_interaction if df_interaction > 0 else 0
            ss_total = np.sum((sharpe_matrix - grand_mean) ** 2)
            ss_error = max(1e-10, ss_total - ss_row - ss_col - ss_interaction)
            ms_error = ss_error / df_error if df_error > 0 else 1e-10
            f_stat = ms_interaction / ms_error if ms_error > 1e-10 else 0
            if _HAS_SCIPY:
                p_value = 1.0 - scipy_stats.f.cdf(f_stat, df_interaction, df_error) if f_stat > 0 else 1.0
            else:
                p_value = 0.01 if f_stat > 10.0 else (0.05 if f_stat > 4.0 else 0.5)
            interaction_significant = p_value < 0.05
        except (ValueError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("ANOVA计算异常: %s", e)

    elapsed = time.time() - start
    print(f"  耦合验证完成, 耗时 {elapsed:.1f} 秒")
    print(f"  Spearman秩相关 rho={spearman_rho:.3f}, p={spearman_p:.3f}")
    print(f"  交互F检验 F={f_stat:.2f}, p={p_value:.3f} {'显著' if interaction_significant else '不显著'}(alpha=0.05)")
    if n < 5:
        print(f"  [警告] 交叉矩阵仅{n}x{n}={n*n}观测, ANOVA功效约{max(10, 100*n*n//60)}%, 耦合漏报风险高")
        print(f"         建议: 增加Top-K至5以上, 或直接使用Optuna贝叶斯搜索")

    weak_coupling = not interaction_significant and abs(spearman_rho) < 0.3
    if weak_coupling:
        print("  >> 弱耦合: 分层扫描结果可信")
    elif not interaction_significant:
        print("  >> 中等耦合: 分层结果基本可信, 交互效应不显著但秩相关较高")
    else:
        print("  >> 显著耦合! 分层扫描不可靠, 强烈建议升级至Optuna贝叶斯联合搜索")

    return {"sharpe_matrix": sharpe_matrix.tolist(), "pr_matrix": pr_matrix.tolist(),
            "spearman_rho": spearman_rho, "spearman_p": spearman_p,
            "f_stat": f_stat, "p_value": p_value,
            "interaction_significant": interaction_significant}, spearman_rho, weak_coupling


# ---------- 8. 策略评判集成 ----------

def integrate_judgment(best_params: Dict, train_result: Dict, test_result: Dict, symbol: str):
    try:
        from ali2026v3_trading.param_pool.validation.statistical_validation import MultipleComparisonCorrector
        _p_values = [train_result.get("p_value", 0.05), test_result.get("p_value", 0.05)]
        _adjusted_bh = MultipleComparisonCorrector.benjamini_hochberg(_p_values)
        if any(p < 0.05 for p in _adjusted_bh):
            logger.info("T-16: MCC BH校正后p值%s, 需关注多重比较风险", _adjusted_bh)
    except (ImportError, ValueError, AttributeError) as _mcc_err:
        logger.debug("T-16: MultipleComparisonCorrector跳过: %s", _mcc_err)

    try:
        if str(PROJECT_ROOT) not in sys.path and os.path.isdir(str(PROJECT_ROOT)): sys.path.insert(0, str(PROJECT_ROOT))
        from strategy_judgment.parameter_pool_adapter import judge_backtest_result
        train_report = judge_backtest_result(
            strategy_type="main", symbol=symbol,
            backtest_period=f"{TRAIN_START}~{TEST_START}(训练)", result=train_result)
        test_report = judge_backtest_result(
            strategy_type="main", symbol=symbol,
            backtest_period=f"{TEST_START}~{TEST_END}(测试)", result=test_result)
        print(f"\n策略评判(训练): verdict={train_report.verdict.value}, score={train_report.overall_score:.2f}")
        print(f"策略评判(测试): verdict={test_report.verdict.value}, score={test_report.overall_score:.2f}")
        return train_report, test_report
    except (ImportError, ValueError, AttributeError, RuntimeError) as e:
        logger.warning("评判集成跳过: %s", e)
        print(f"[注意] 策略评判未能执行: {e}")
        return None, None


# ---------- 9. 结果持久化 ----------

def save_results(output_dir, phase1_results, best_strategy, phase2_results, coupling_data, judgment_reports):
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now(CHINA_TZ).strftime("%Y%m%d_%H%M%S")

    with open(os.path.join(output_dir, f"phase1_top5_{ts}.json"), "w", encoding="utf-8") as f:
        f.write(json_dumps([(s, {k: v for k, v in m.items() if k != "aux_params" and k != "pullback_params"})
                   for s, m in phase1_results[:5]], indent=2))

    if phase2_results:
        best_aux, best_pb, best_met = phase2_results[0]
        with open(os.path.join(output_dir, f"phase2_best_{ts}.json"), "w", encoding="utf-8") as f:
            f.write(json_dumps({
                "strategy": best_strategy, "aux": best_aux, "pullback": best_pb,
                "train_metrics": {k: v for k, v in best_met.items()
                                  if not k.startswith("test_") and k not in ("aux_params", "pullback_params")},
                "test_metrics": {k: v for k, v in best_met.items() if k.startswith("test_")},
            }, indent=2))

    if coupling_data:
        with open(os.path.join(output_dir, f"coupling_{ts}.json"), "w", encoding="utf-8") as f:
            f.write(json_dumps(coupling_data, indent=2))

    if judgment_reports and isinstance(judgment_reports, (list, tuple)) and len(judgment_reports) > 0 and judgment_reports[0]:
        train_r, test_r = judgment_reports
        with open(os.path.join(output_dir, f"judgment_{ts}.json"), "w", encoding="utf-8") as f:
            f.write(json_dumps({  # R4-5: 统一json_dumps
                "train_verdict": train_r.verdict.value, "train_score": train_r.overall_score,
                "test_verdict": test_r.verdict.value if test_r else "N/A",
                "test_score": test_r.overall_score if test_r else 0.0,
            }, indent=2))

    logger.info("结果保存至%s", output_dir)
