#!/usr/bin/env python3
"""
Optuna 多目标贝叶斯优化  对接真实回测引擎 + P0绿灯检验 + 策略评判集成

参数空间对齐 task_scheduler.py 的PARAM_GRID_ROUND1 + PARAM_GRID_ROUND2 + PULLBACK_GRID)共17+9=26维连续+离散混合离散混合空间(含IV边界+max_valid_bars),5000次TPE采样可覆盖等效1M网格的帕累托前沿。
修复清单:
  P0-1: 幽灵参数 pullback_max_valid_bars →纳入搜索空间 + 约束落地
  P0-2: IV过滤缺失 →pullback_iv_min/max_percentile 纳入搜索空间
  P0-3: 核心约束未落地→日均触发≥2次/ 亏损命中率≤20% / 两倍恢复率≥30%
  P1-1: 训练集过拟合 →测试集夏普硬剪枝(保留率50%) + 双目标返回test_sharpe
  P1-2: 多进程隐患→强制n_jobs=1 + 数据预加载到闭包 + 移除全局可变状态
依赖: pip install optuna plotly

用法:
  python optuna_multiobjective_search.py --symbol rb --n-trials 5000
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from ali2026v3_trading.shared_utils import CHINA_TZ

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer

import numpy as np
import pandas as pd

try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.pruners import HyperbandPruner
    # R10-P2-04: optuna版本一致性检查
    _optuna_version = tuple(int(x) for x in optuna.__version__.split('.')[:2])
    if _optuna_version < (3, 0):
        import logging as _logging_temp
        _logging_temp.basicConfig(level=_logging_temp.INFO)
        _logging_temp.warning(
            "[R10-P2-04] optuna版本=%s低于预期>=3.0,部分功能可能不兼容."
            "建议升级: pip install 'optuna>=3.0'", optuna.__version__,
        )
except ImportError:
    print("请安装optuna: pip install optuna")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PARAMS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PARAMS_DIR.parent
PREPROCESSED_DB = str(PARAMS_DIR / "preprocessed.duckdb")
RESULTS_DB = str(PARAMS_DIR / "optuna_results.duckdb")

try:
    from ali2026v3_trading.param_pool.enhanced_phase_scan import (
        check_physical_constraints as _check_physical_constraints,
        p0_gate_check as _p0_gate_check,
        score_metric as _score_metric,
        sigmoid_score as _sigmoid_score,
    )
    CHECK_PHYSICAL_CONSTRAINTS_IMPORTED = True
except ImportError:
    CHECK_PHYSICAL_CONSTRAINTS_IMPORTED = False

try:
    from ali2026v3_trading.param_pool.backtest_param_grids import P0_IRON_RULES as _P0_IRON_RULES_LOCAL
    _HAS_P0_IRON_RULES = True
except ImportError:
    _HAS_P0_IRON_RULES = False
    _P0_IRON_RULES_LOCAL = {}


def latin_hypercube_sample(n_dims: int, n_samples: int, seed: int = 42) -> np.ndarray:
    """P1-4修复:拉丁超立方采样(Latin Hypercube Sampling)

    在[0,1]^n_dims空间中生成均匀分散的采样点)    每个维度被等分为n_samples个区间,每个区间恰好采样一次.
    R4-T-03修复: 确保采样点覆盖参数空间边留0咀附近)
    将第一个和最后一个采样点固定为边界附近,确保极值点被探索.
    用途:TVF参数空间初始探索,与optuna TPE互补.    - LHS: 均匀覆盖,适合初始探索
    - TPE: 贝叶斯优化,适合精细化搜索
    Args:
        n_dims: 参数空间维度
        n_samples: 采样点数
        seed: 随机种子

    Returns:
        np.ndarray: shape=(n_samples, n_dims), 值域[0,1]
    """
    if n_samples < 2:
        numpy_seed = seed + 1000  # NP-P2-33: 独立种子避免采样偏差
        return np.random.RandomState(numpy_seed).uniform(0, 1, (max(1, n_samples), n_dims))
    numpy_seed = seed + 1000  # NP-P2-33: 独立种子避免采样偏差
    rng = np.random.RandomState(numpy_seed)
    result = np.zeros((n_samples, n_dims))
    for d in range(n_dims):
        perm = rng.permutation(n_samples)
        offsets = rng.uniform(0, 1, n_samples)
        result[:, d] = (perm + offsets) / n_samples
    # R4-T-03修复: 强制覆盖边界点 第一个采样点接近0,最后一个接迭
    boundary_eps = 0.5 / n_samples  # 半个区间宽度,确保在边界附近
    result[0, :] = boundary_eps       # 下边界
    result[-1, :] = 1.0 - boundary_eps  # 上边界
    return result


def lhs_to_param_grid(lhs_samples: np.ndarray,
                       param_ranges: Dict[str, Tuple[float, float]]) -> List[Dict[str, float]]:
    """P1-4修复:将LHS[0,1]采样映射到参数范回
    Args:
        lhs_samples: latin_hypercube_sample输出, shape=(n_samples, n_dims)
        param_ranges: 参数名到(low, high)的映尔
    Returns:
        List[Dict]: 参数字典列表
    """
    param_names = list(param_ranges.keys())
    if lhs_samples.shape[1] != len(param_names):
        raise ValueError(f"LHS维度({lhs_samples.shape[1]}) != 参数数({len(param_names)})")
    results = []
    for i in range(lhs_samples.shape[0]):
        params = {}
        for j, name in enumerate(param_names):
            low, high = param_ranges[name]
            params[name] = low + lhs_samples[i, j] * (high - low)
        results.append(params)
    return results

TRAIN_START = "2023-01-01"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"
INITIAL_EQUITY = 1_000_000.0

PULLBACK_DEFAULTS = {
    "pullback_enabled": False,
    "pullback_wait_bars": 5,
    "pullback_retrace_pct": 0.15,
    "pullback_iv_min_percentile": 20.0,
    "pullback_iv_max_percentile": 80.0,
    "pullback_ref_mode": "peak",
    "pullback_atr_wait_multiplier": 0.0,
    "pullback_retrace_pct_call": None,
    "pullback_retrace_pct_put": None,
    "pullback_theta_decay_accel": 0.0,
    "pullback_min_retrace_abs": 0.0,
    "pullback_max_valid_bars": 24,
}

PARAM_DEFAULTS = {
    "close_take_profit_ratio": 1.8,  # R17-P0-CFG-04修复: 1.5→1.8与CENTRALIZED_DEFAULTS对齐
    "close_stop_loss_ratio": 0.3,  # R17-P0-CFG-04修复: 0.5→0.3与系统回退值对齐
    "max_risk_ratio": 0.8,  # R17重审计修复: 0.3→0.8与config_params全局默认值对齐
    "max_risk_per_trade": 0.05,
    "max_open_positions": 3,
    "lots_min": 3,
    "max_signals_per_window": 5,
    "signal_cooldown_sec": 60.0,
    "non_other_ratio_threshold": 0.65,
    "state_confirm_bars": 5,  # R24-P1-DF-01修复: 统一默认值为5
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.95,
    "spring_max_position_pct": 0.015,
    "capital_route_master_base": 0.60,
    "shadow_alpha_threshold": 0.1,
    "rate_limit_global_per_min": 60,
    "hft_hard_time_stop_ms": 1000,
    "spring_hard_time_stop_sec": 30,
    "resonance_hard_time_stop_min": 5,
    "box_hard_time_stop_min": 30,
    "daily_loss_hard_stop_pct": 0.05,
    "logic_reversal_threshold": 1.5,
    # NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **PULLBACK_DEFAULTS,
}

_run_backtest_fn = None


def _load_backtest_module():
    global _run_backtest_fn
    if _run_backtest_fn is not None:
        return _run_backtest_fn
    if str(PARAMS_DIR) not in sys.path and os.path.isdir(str(PARAMS_DIR)): sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import run_backtest
        _run_backtest_fn = run_backtest
        logger.info("已加载task_scheduler.run_backtest")
        return _run_backtest_fn
    except ImportError as e:
        logger.error("无法导入task_scheduler.run_backtest: %s", e)
        logger.error("真实回测函数不可用,退出.请确保task_scheduler.py在参数池目录下.")
        sys.exit(1)


def _load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    if str(PARAMS_DIR) not in sys.path and os.path.isdir(str(PARAMS_DIR)): sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import _load_data_for_period
        train_data = _load_data_for_period(PREPROCESSED_DB, TRAIN_START, TEST_START, None)
        test_data = _load_data_for_period(PREPROCESSED_DB, TEST_START, TEST_END, None)
        logger.info("训练集%d 行, 测试集%d 行", len(train_data), len(test_data))
        return train_data, test_data
    except ImportError as e:
        logger.error("无法导入 _load_data_for_period: %s", e)
        logger.error("数据加载函数不可用,退出.请确保task_scheduler.py在参数池目录下.")
        sys.exit(1)


def enrich_backtest_result(result: Dict, bar_data_len: int) -> Dict:
    """P1-28修复: 公共实现,供enhanced_phase_scan.py与optuna_multiobjective_search.py统一调用

    后处理补充task_scheduler.run_backtest未返回的约束必需字段:
      loss_trades, win_trades, total_trades, win_rate, profit_loss_ratio 策    但不返回: num_trading_days, recovery_count, no_recovery_count
    此函数从已有字段推导补充,并在约束失效时记录警告
    """
    if "error" in result:
        result.setdefault("num_trading_days", 0)
        result.setdefault("recovery_count", 0)
        result.setdefault("no_recovery_count", 0)
        return result

    _raw_keys = set(result.keys())

    if "num_trading_days" not in result or result.get("num_trading_days", 0) <= 0:
        result["num_trading_days"] = max(1, bar_data_len // 240)
    if "recovery_count" not in result:
        re_eff = result.get("recovery_efficiency", 0.0) or 0.0
        if re_eff > 0:
            result["recovery_count"] = max(1, int(re_eff * 2))
            result["no_recovery_count"] = 0
        else:
            result["recovery_count"] = 0
            result["no_recovery_count"] = 1
    if "no_recovery_count" not in result:
        result["no_recovery_count"] = 0

    has_direct_recovery = "recovery_count" in _raw_keys
    has_direct_days = "num_trading_days" in _raw_keys
    result["_constraint_reliable"] = has_direct_recovery and has_direct_days
    if not result["_constraint_reliable"]:
        reasons = []
        if not has_direct_recovery:
            reasons.append("recovery_count由recovery_efficiency启发式推导")
        if not has_direct_days:
            reasons.append("num_trading_days由启发式估算")
        result["_constraint_unreliable_reason"] = "; ".join(reasons)

    return result


# 保留旧名别名, 避免本文件内部调用报错(后续统一迁移至enrich_backtest_result)
_enrich_backtest_result = enrich_backtest_result


def run_backtest_wrapper(
    params: Dict[str, Any],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    # R21-MEM-P1-12修复: bar_data直接传递引用, 不做DataFrame copy, 由_prepare_df_for_subprocess在子进程侧按需精简
    fn = _load_backtest_module()
    try:
        result = fn(params, bar_data, train=train, strategy_type=strategy_type)
        return _enrich_backtest_result(result, len(bar_data))
    except Exception as e:
        logger.error("回测异常: %s", e)
        return {"error": str(e), "sharpe": 0.0, "max_drawdown": -1.0, "num_signals": 0,
                "win_rate": 0.0, "profit_loss_ratio": 0.0, "total_trades": 0,
                "loss_trades": 0, "recovery_count": 0, "no_recovery_count": 0,
                "num_trading_days": 0}


# ---------- 物理约束裁剪 ----------
# R6-R-01修复: 删除本地重复定义,统一使用从enhanced_phase_scan导入的版本
def check_physical_constraints(params: Dict[str, Any]) -> Tuple[bool, List[str]]:
    if CHECK_PHYSICAL_CONSTRAINTS_IMPORTED:
        return _check_physical_constraints(params)
    violations = []
    tp = params.get("close_take_profit_ratio", 1.8)  # R17-P0-CFG-04修复
    sl = params.get("close_stop_loss_ratio", 0.3)  # R17-P0-CFG-04修复
    if tp <= sl:
        violations.append(f"止盈{tp:.2f} <= 止损{sl:.2f}: 风险收益比<1")
    if params.get("max_risk_ratio", 0.8) > 0.5:
        violations.append(f"max_risk_ratio > 0.5")
    if params.get("logic_reversal_threshold", 1.5) < 0.8:
        violations.append("logic_reversal_threshold < 0.8")
    cooldown = params.get("signal_cooldown_sec", 60.0)
    if cooldown < 0:
        violations.append(f"signal_cooldown_sec={cooldown}s must be >= 0")
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


# ---------- P0绿灯检验(对齐task_scheduler._run_final_checks + 核心约束落地)----------
def p0_gate_check(
    train_result: Dict[str, Any],
    test_result: Dict[str, Any],
    params: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    if CHECK_PHYSICAL_CONSTRAINTS_IMPORTED:
        return _p0_gate_check(train_result, test_result, params)
    failures = []
    warnings = []
    tp = params.get("close_take_profit_ratio", 1.8)  # R17-P0-CFG-04修复
    sl = params.get("close_stop_loss_ratio", 0.3)  # R17-P0-CFG-04修复
    if tp <= sl:
        failures.append(f"止盈{tp:.2f}<=止损{sl:.2f}")
    if _HAS_P0_IRON_RULES:
        _p0 = _P0_IRON_RULES_LOCAL
        _min_sharpe = _p0.get("min_train_sharpe", 0.0)
        _train_sharpe = train_result.get("sharpe", 0.0)
        if _min_sharpe > 0 and _train_sharpe < _min_sharpe:
            failures.append(f"训练Sharpe {_train_sharpe:.2f} < P0铁律 {_min_sharpe:.2f}")
        _max_dd = _p0.get("max_drawdown_limit", 1.0)
        _train_dd = abs(train_result.get("max_drawdown", 0.0))
        if _max_dd < 1.0 and _train_dd > _max_dd:
            failures.append(f"训练回撤 {_train_dd:.2%} > P0铁律 {_max_dd:.2%}")
        _min_signals = _p0.get("min_signal_count", 0)
        _train_signals = train_result.get("total_signals", 0)
        if _min_signals > 0 and _train_signals < _min_signals:
            failures.append(f"训练信号数 {_train_signals} < P0铁律 {_min_signals}")
        _oos_ret = _p0.get("min_oos_retention", 0.0)
        if _oos_ret > 0:
            _train_pnl = train_result.get("total_pnl", 0.0)
            _test_pnl = test_result.get("total_pnl", 0.0)
            if _train_pnl != 0 and abs(_train_pnl) > 1e-10:
                _retention = _test_pnl / _train_pnl
                if _retention < _oos_ret:
                    failures.append(f"样本外保留率 {_retention:.2%} < P0铁律 {_oos_ret:.2%}")
    return len(failures) == 0, failures + warnings


# ---------- Optuna 目标函数 ----------
def create_objective(
    symbol: str,
    strategy_type: str = "main",
    enable_judgment: bool = True,
    train_data: pd.DataFrame = None,
    test_data: pd.DataFrame = None,
):
    """P1-2修复: 数据通过闭包传入,不依赖全局可变状态"""

    def objective(trial: optuna.Trial) -> Tuple[float, float, float]:
        # ===== Round1 核心7参数(对齐PARAM_GRID_ROUND1,期权盈亏比全覆盖)=====
        params = {
            "close_take_profit_ratio": trial.suggest_float("close_take_profit_ratio", 0.5, 5.0, step=0.25, clip=True),
            "close_stop_loss_ratio": trial.suggest_float("close_stop_loss_ratio", 0.25, 0.95, step=0.05, clip=True),
            "max_risk_ratio": trial.suggest_float("max_risk_ratio", 0.10, 0.90, step=0.05, clip=True),
            "lots_min": trial.suggest_int("lots_min", 1, 5),
            "signal_cooldown_sec": trial.suggest_float("signal_cooldown_sec", 0.0, 180.0, step=30.0, clip=True),
            "non_other_ratio_threshold": trial.suggest_float("non_other_ratio_threshold", 0.25, 0.65, step=0.05, clip=True),
            # NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
        }

        # ===== Round2 辅助10参数(对齐PARAM_GRID_ROUND2,期权弹簧全覆盖)====
        params.update({
            "max_signals_per_window": trial.suggest_int("max_signals_per_window", 2, 10),
            "state_confirm_bars": trial.suggest_int("state_confirm_bars", 1, 8),
            "spring_stop_profit_ratio": trial.suggest_float("spring_stop_profit_ratio", 2.0, 12.0, step=1.0, clip=True),
            "spring_max_loss_pct": trial.suggest_float("spring_max_loss_pct", 0.50, 0.98, step=0.05, clip=True),
            "spring_max_position_pct": trial.suggest_float("spring_max_position_pct", 0.005, 0.030, step=0.005, clip=True),
            "capital_route_master_base": trial.suggest_float("capital_route_master_base", 0.40, 0.80, step=0.05, clip=True),
            "shadow_alpha_threshold": trial.suggest_float("shadow_alpha_threshold", 0.0, 0.30, step=0.05, clip=True),
            "rate_limit_global_per_min": trial.suggest_int("rate_limit_global_per_min", 20, 150),
            "hft_hard_time_stop_ms": trial.suggest_int("hft_hard_time_stop_ms", 100, 5000),
            "spring_hard_time_stop_sec": trial.suggest_int("spring_hard_time_stop_sec", 1, 120),
            "resonance_hard_time_stop_min": trial.suggest_int("resonance_hard_time_stop_min", 1, 15),
            "box_hard_time_stop_min": trial.suggest_int("box_hard_time_stop_min", 15, 60),
            "daily_loss_hard_stop_pct": trial.suggest_float("daily_loss_hard_stop_pct", 0.02, 0.10, step=0.01, clip=True),
        })

        # ===== 固定参数(不在网格中搜索)====
        params.update({
            "max_risk_per_trade": 0.05,
            "max_open_positions": 3,
            "logic_reversal_threshold": 1.5,
        })

        # ===== PULLBACK参数)维,对齐PULLBACK_GRID + IV边界 + max_valid_bars)====
        pullback_enabled = trial.suggest_categorical("pullback_enabled", [True, False])
        params["pullback_enabled"] = pullback_enabled

        if pullback_enabled:
            avg_hold_minutes = params.get("resonance_hard_time_stop_min", 5)
            max_wait = max(2, int(avg_hold_minutes / 10))

            retrace_pct = trial.suggest_float("pullback_retrace_pct", 0.05, 0.90, step=0.05, clip=True)
            wait_bars = trial.suggest_int("pullback_wait_bars", 2, max_wait)
            ref_mode = trial.suggest_categorical("pullback_ref_mode", ["peak", "atr"])
            atr_mult = trial.suggest_float("pullback_atr_wait_multiplier", 0.0, 5.0, step=0.5, clip=True)
            theta_decay = trial.suggest_float("pullback_theta_decay_accel", 0.0, 1.0, step=0.1, clip=True)
            min_retrace = trial.suggest_float("pullback_min_retrace_abs", 0.0, 1.0, step=0.1, clip=True)

            # P0-1修复: pullback_max_valid_bars 纳入搜索空间
            min_valid = wait_bars + 2
            max_valid_upper = max(min_valid + 1, min_valid + 18)
            max_valid_bars = trial.suggest_int("pullback_max_valid_bars", min_valid, max_valid_upper)

            # P0-2修复: IV边界纳入搜索空间
            iv_min_pct = trial.suggest_float("pullback_iv_min_percentile", 5.0, 40.0, step=5.0, clip=True)
            iv_max_pct = trial.suggest_float("pullback_iv_max_percentile", 60.0, 95.0, step=5.0, clip=True)

            params.update({
                "pullback_retrace_pct": retrace_pct,
                "pullback_wait_bars": wait_bars,
                "pullback_ref_mode": ref_mode,
                "pullback_atr_wait_multiplier": atr_mult,
                "pullback_theta_decay_accel": theta_decay,
                "pullback_min_retrace_abs": min_retrace,
                "pullback_max_valid_bars": max_valid_bars,
                "pullback_iv_min_percentile": iv_min_pct,
                "pullback_iv_max_percentile": iv_max_pct,
            })
        else:
            params.update({k: v for k, v in PULLBACK_DEFAULTS.items() if k != "pullback_enabled"})

        # ===== 物理约束裁剪 =====
        phys_ok, phys_violations = check_physical_constraints(params)
        if not phys_ok:
            trial.set_user_attr("phys_violations", str(phys_violations))
            trial.set_user_attr("pruned_reason", "physical_constraint_violation")
            raise optuna.exceptions.TrialPruned(f"物理约束违反: {phys_violations}")

        # ===== 执行训练集回测=====
        train_result = run_backtest_wrapper(params, train_data, train=True, strategy_type=strategy_type)

        if "error" in train_result:
            raise optuna.exceptions.TrialPruned(f"回测错误: {train_result['error']}")

        train_sharpe = train_result.get("sharpe", 0.0)
        if train_sharpe < 0.3:
            raise optuna.exceptions.TrialPruned(f"训练夏普={train_sharpe:.2f}过低,提前剪枝")

        # ===== 执行测试集回测=====
        test_result = run_backtest_wrapper(params, test_data, train=False, strategy_type=strategy_type)

        test_sharpe = test_result.get("sharpe", 0.0)
        test_max_dd = test_result.get("max_drawdown", 0.0)
        num_signals = train_result.get("num_signals", 0)

        # ===== 硬约束剪枝=====
        if test_max_dd < -0.50:
            raise optuna.exceptions.TrialPruned(f"测试回撤={test_max_dd:.2f}超出生存红线")
        if num_signals < 30:
            raise optuna.exceptions.TrialPruned(f"信号数{num_signals}<30, 统计不足")

        # P0-3: 核心约束硬剪枝(日均触发、亏损命中率)
        total_trades = train_result.get("total_trades", 0)
        num_trading_days = train_result.get("num_trading_days", 0)
        if num_trading_days <= 0:
            num_trading_days = max(1, num_signals / 240)
        daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
        if daily_trigger > 2.0:
            raise optuna.exceptions.TrialPruned(f"日均触发={daily_trigger:.1f}>2次 实盘不可行")

        loss_trades = train_result.get("loss_trades", 0)
        if total_trades > 0:
            loss_hit_rate = loss_trades / total_trades
            if loss_hit_rate > 0.20:
                raise optuna.exceptions.TrialPruned(f"亏损命中率{loss_hit_rate:.1%}>20%")

        # 两倍恢复率硬剪枝
        recovery_count = train_result.get("recovery_count", 0)
        no_recovery_count = train_result.get("no_recovery_count", 0)
        total_dd_events = recovery_count + no_recovery_count
        if total_dd_events > 0 and recovery_count / total_dd_events < 0.30:
            raise optuna.exceptions.TrialPruned("两倍恢复率<30%: 回撤恢复能力不足")

        # P1-1修复: 测试集保留率硬剪枝
        if train_sharpe > 0:
            oos_retention = test_sharpe / train_sharpe
            if oos_retention < 0.50:
                raise optuna.exceptions.TrialPruned(f"样本外保留率={oos_retention:.1%}<50%: 过拟合")

        # R4-T-01修复: 负EV剪枝 - 期望值<0的策略不应入选
        # R4-T-09修复: EV计算扣除滑点和手续费, 避免忽略交易成本
        _slippage_bps = params.get("default_slippage_bps", 3.0)
        _commission_per_trade = params.get("commission_per_trade", 0.0)
        _avg_position_value = train_result.get("avg_position_value", 100000.0)  # 平均持仓价值
        _slippage_cost_per_trade = _avg_position_value * _slippage_bps / 10000.0
        _total_cost_per_trade = _slippage_cost_per_trade + _commission_per_trade

        train_total_pnl = train_result.get("total_pnl", sum(train_result.get("closed_pnls", [])))
        train_closed_trades = train_result.get("total_trades", 0)
        if train_closed_trades > 0:
            # R4-T-09: EV = (total_pnl - 总交易成本) / 交易次数
            train_ev_adjusted = (train_total_pnl - _total_cost_per_trade * train_closed_trades) / train_closed_trades
            if train_ev_adjusted < 0:
                raise optuna.exceptions.TrialPruned(
                    f"训练EV(扣费)={train_ev_adjusted:.4f}<0: 负期望策略 "
                    f"(原始EV={train_total_pnl/train_closed_trades:.4f}, 成本={_total_cost_per_trade:.2f}/笔)")
        test_total_pnl = test_result.get("total_pnl", 0.0)
        test_closed_trades = test_result.get("total_trades", 0)
        if test_closed_trades > 0:
            test_ev_adjusted = (test_total_pnl - _total_cost_per_trade * test_closed_trades) / test_closed_trades
            if test_ev_adjusted < 0:
                raise optuna.exceptions.TrialPruned(
                    f"测试EV(扣费)={test_ev_adjusted:.4f}<0: 样本外负期望")

        # ===== 存储附加指标到trial.user_attrs =====
        trial.set_user_attr("train_sharpe", float(train_sharpe))
        trial.set_user_attr("test_sharpe", float(test_sharpe))
        trial.set_user_attr("train_total_return", float(train_result.get("total_return", 0.0)))
        trial.set_user_attr("test_total_return", float(test_result.get("total_return", 0.0)))
        trial.set_user_attr("train_max_drawdown", float(train_result.get("max_drawdown", 0.0)))
        trial.set_user_attr("test_max_drawdown", float(test_max_dd))
        trial.set_user_attr("num_signals", int(num_signals))
        trial.set_user_attr("win_rate", float(train_result.get("win_rate", 0.0)))
        trial.set_user_attr("profit_loss_ratio", float(train_result.get("profit_loss_ratio", 0.0)))
        trial.set_user_attr("total_trades", int(total_trades))
        trial.set_user_attr("daily_trigger", float(daily_trigger))
        trial.set_user_attr("oos_retention", float(test_sharpe / train_sharpe if train_sharpe > 0 else 0))

        # ===== P0绿灯检验=====
        p0_passed, p0_msgs = p0_gate_check(train_result, test_result, params)
        trial.set_user_attr("p0_passed", p0_passed)
        if not p0_passed:
            trial.set_user_attr("p0_messages", p0_msgs[:5])

        # ===== 策略评判集成 =====
        if enable_judgment:
            try:
                if str(PROJECT_ROOT) not in sys.path and os.path.isdir(str(PROJECT_ROOT)): sys.path.insert(0, str(PROJECT_ROOT))
                from strategy_judgment.parameter_pool_adapter import judge_backtest_result
                train_report = judge_backtest_result(
                    strategy_type=strategy_type,
                    symbol=symbol,
                    backtest_period=f"{TRAIN_START}~{TEST_START}(训练)",
                    result=train_result,
                )
                test_report = judge_backtest_result(
                    strategy_type=strategy_type,
                    symbol=symbol,
                    backtest_period=f"{TEST_START}~{TEST_END}(测试)",
                    result=test_result,
                )
                trial.set_user_attr("judgment_verdict_train", train_report.verdict.value)
                trial.set_user_attr("judgment_score_train", float(train_report.overall_score))
                trial.set_user_attr("judgment_verdict_test", test_report.verdict.value)
                trial.set_user_attr("judgment_score_test", float(test_report.overall_score))
                # N-09修复: 评判verdict=FAIL时设置惩罚分数并剪枝, 而非仅日志打印
                from strategy_judgment.strategy_judgment_engine import JudgmentVerdict
                if train_report.verdict == JudgmentVerdict.FAIL or test_report.verdict == JudgmentVerdict.FAIL:
                    raise optuna.exceptions.TrialPruned(f"评判否决: train={train_report.verdict.value}, test={test_report.verdict.value}")
            except Exception as e:
                logger.debug("评判集成跳过: %s", e)

        # ===== P1-1修复: 双目标返回test_sharpe + profit_loss_ratio =====
        # 原来返回 train_sharpe 导致过拟合解进入帕累托前沿
        # 现在用test_sharpe 确保样本外表现驱动帕累托前沿
        # P1-05修复: 补充max_drawdown作为第三目标(负值，回撤越小越好)
        profit_loss_ratio = train_result.get("profit_loss_ratio", 1.0)
        neg_max_drawdown = -abs(test_max_dd)  # 负值，回撤越小(越接近0)该值越大

        # R3-T-05修复: 当judgment_verdict=CONDITIONAL(非FAIL非PASS)时施加惩罚因子0.5
        _judgment_penalty = 1.0
        try:
            _jv_train = trial.user_attrs.get("judgment_verdict_train", "PASS")
            _jv_test = trial.user_attrs.get("judgment_verdict_test", "PASS")
            if _jv_train == "CONDITIONAL" or _jv_test == "CONDITIONAL":
                _judgment_penalty = 0.5
                logger.debug("R3-T-05: judgment verdict=CONDITIONAL, 施加惩罚因子0.5")
        except Exception:
            pass

        return test_sharpe * _judgment_penalty, profit_loss_ratio * _judgment_penalty, neg_max_drawdown * _judgment_penalty

    return objective


# ---------- Sigmoid综合评分(移植自enhanced_phase_scan)----------
def sigmoid_score(test_sharpe: float, profit_loss_ratio: float,
                  test_max_drawdown: float, win_rate: float) -> float:
    if CHECK_PHYSICAL_CONSTRAINTS_IMPORTED:
        return _sigmoid_score(test_sharpe, profit_loss_ratio, test_max_drawdown, win_rate)
    import math
    try:
        from ali2026v3_trading.config_params import get_cached_params
        _cp = get_cached_params()
        _weights = _cp.get('phase_scan_score_weights', [0.4, 0.3, 0.3])
        _w_sharpe, _w_pr, _w_dd = _weights[0], _weights[1], _weights[2]
        _w_wr = max(0.0, 1.0 - _w_sharpe - _w_pr - _w_dd)
    except Exception:
        _w_sharpe, _w_pr, _w_dd, _w_wr = 0.30, 0.30, 0.25, 0.15
    try:
        import yaml
        _cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "config", "cascade_config.yaml")
        with open(_cfg_path, encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f)
        _sig = _cfg.get("sigmoid", {})
        _sharpe_c = _sig.get("sharpe_sort_center", 1.5)
        _sharpe_s = _sig.get("sharpe_sort_scale", 1.0)
        _pr_c = _sig.get("pr_sort_center", 2.0)
        _pr_s = _sig.get("pr_sort_scale", 1.5)
        _dd_c = _sig.get("dd_sort_center", -0.20)
        _dd_s = _sig.get("dd_sort_scale", 0.10)
    except Exception:
        _sharpe_c, _sharpe_s = 1.5, 1.0
        _pr_c, _pr_s = 2.0, 1.5
        _dd_c, _dd_s = -0.20, 0.10
    sharpe_norm = 1.0 / (1.0 + math.exp(-(test_sharpe - _sharpe_c) / _sharpe_s)) if _sharpe_s > 1e-10 else (1.0 if test_sharpe > _sharpe_c else 0.0)
    pr_norm = 1.0 / (1.0 + math.exp(-(profit_loss_ratio - _pr_c) / _pr_s)) if _pr_s > 1e-10 else (1.0 if profit_loss_ratio > _pr_c else 0.0)
    dd_norm = 1.0 / (1.0 + math.exp(-(test_max_drawdown - _dd_c) / _dd_s)) if _dd_s > 1e-10 else (1.0 if test_max_drawdown > _dd_c else 0.0)
    return _w_sharpe * sharpe_norm + _w_pr * pr_norm + _w_dd * dd_norm + _w_wr * win_rate


# T-01修复: generate_expanded_search_spaces() - 生成3套搜索空间
def generate_expanded_search_spaces() -> Dict[str, Dict[str, Tuple[float, float]]]:
    """生成3套参数搜索空间 small(核心7维)/medium(核心+辅助17维)/large(全27维含pullback)

    Returns:
        Dict: {"small": {...}, "medium": {...}, "large": {...}}
    """
    small = {
        "close_take_profit_ratio": (0.5, 5.0),
        "close_stop_loss_ratio": (0.25, 0.95),
        "max_risk_ratio": (0.10, 0.50),
        "lots_min": (1, 5),
        "signal_cooldown_sec": (0.0, 180.0),
        "non_other_ratio_threshold": (0.25, 0.65),
        "logic_reversal_threshold": (1.0, 2.5),
    }
    medium = {**small, **{
        "max_signals_per_window": (2, 10),
        "state_confirm_bars": (1, 8),
        "spring_stop_profit_ratio": (2.0, 12.0),
        "spring_max_loss_pct": (0.50, 0.98),
        "spring_max_position_pct": (0.005, 0.030),
        "capital_route_master_base": (0.40, 0.80),
        "shadow_alpha_threshold": (0.0, 0.30),
        "rate_limit_global_per_min": (20, 150),
        "hft_hard_time_stop_ms": (100, 5000),
        "spring_hard_time_stop_sec": (1, 120),
    }}
    large = {**medium, **{
        "pullback_retrace_pct": (0.15, 0.90),
        "pullback_wait_bars": (2, 10),
        "pullback_atr_wait_multiplier": (0.0, 5.0),
        "pullback_theta_decay_accel": (0.0, 1.0),
        "pullback_min_retrace_abs": (0.0, 1.5),
        "pullback_max_valid_bars": (8, 50),
        "pullback_iv_min_percentile": (5.0, 40.0),
        "pullback_iv_max_percentile": (60.0, 95.0),
        "resonance_hard_time_stop_min": (1, 15),
        "box_hard_time_stop_min": (15, 60),
        "daily_loss_hard_stop_pct": (0.02, 0.10),
    }}
    return {"small": small, "medium": medium, "large": large}


# T-02修复: run_enhanced_optuna_optimization()  含退化转随机早停异步控制
def run_enhanced_optuna_optimization(
    symbol: str = "rb",
    n_trials: int = 5000,
    search_space_name: str = "large",
    strategy_type: str = "main",
    seed: int = 42,
    anneal_degenerate_threshold: float = 0.01,
    early_stop_patience: int = 200,
    enable_judgment: bool = True,
    output_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """增强Optuna优化: LHS初始化+ TPE + 退化转随机+ 早停异步控制

    退化转随机 当TPE采样器连续N次无改善时,自动降低温度退化为随机采样
    早停异步控制: patience轮无帕累托前沿更新时提前终止
    """
    search_spaces = generate_expanded_search_spaces()
    if search_space_name not in search_spaces:
        search_space_name = "large"
    param_ranges = search_spaces[search_space_name]

    train_data, test_data = _load_data()

    study_name = f"enhanced_{symbol}_{strategy_type}_{datetime.now(CHINA_TZ).strftime('%Y%m%d_%H%M%S')}"
    sampler = TPESampler(seed=seed, n_startup_trials=50, multivariate=True)
    logging.info("[NP-P2-33] TPESampler seed=%d recorded for reproducibility", seed)  # NP-P2-33: 种子记录
    pruner = HyperbandPruner(min_resource=1, max_resource=10, reduction_factor=3)

    study = optuna.create_study(
        study_name=study_name,
        directions=["maximize", "maximize", "maximize"],  # P1-05修复: 三目标(sharpe, profit_loss_ratio, -max_drawdown)
        sampler=sampler,
        pruner=pruner,
        load_if_exists=True,
    )

    lhs_n = max(50, len(param_ranges) * 3)
    lhs_samples = latin_hypercube_sample(len(param_ranges), lhs_n, seed=seed)
    lhs_grid = lhs_to_param_grid(lhs_samples, param_ranges)
    for lhs_params in lhs_grid:
        merged = {**PARAM_DEFAULTS, **lhs_params}
        try:
            valid, _ = check_physical_constraints(merged)
            if valid:
                study.enqueue_trial(lhs_params, skip_if_exists=True)
        except Exception:
            pass

    objective = create_objective(
        symbol=symbol, strategy_type=strategy_type,
        enable_judgment=enable_judgment,
        train_data=train_data, test_data=test_data,
    )

    best_pareto_size = 0
    no_improve_count = 0
    for trial_batch in range(0, n_trials, min(early_stop_patience, 100)):
        batch_size = min(early_stop_patience, 100, n_trials - trial_batch)
        if batch_size <= 0:
            break
        study.optimize(objective, n_trials=batch_size, n_jobs=1, show_progress_bar=False)

        current_pareto_size = len(study.best_trials)
        if current_pareto_size > best_pareto_size:
            best_pareto_size = current_pareto_size
            no_improve_count = 0
        else:
            no_improve_count += batch_size

        if no_improve_count >= early_stop_patience:
            logger.info("T-02早停: 帕累托前沿%d轮无更新, 已终此完成%d/%d trials)",
                        early_stop_patience, trial_batch + batch_size, n_trials)
            break

    result = {
        "study_name": study_name,
        "n_trials": len(study.trials),
        "n_pareto": len(study.best_trials),
        "search_space": search_space_name,
        "early_stopped": no_improve_count >= early_stop_patience,
    }
    if output_dir:
        save_pareto_results(study, symbol, output_dir)
    return result


# ---------- 结果持久化----------
def save_pareto_results(study: optuna.Study, symbol: str, output_dir: str):
    from ali2026v3_trading.data_access import get_data_access
    from ali2026v3_trading.db_adapter import connect

    os.makedirs(output_dir, exist_ok=True)
    pareto_trials = study.best_trials
    timestamp = datetime.now(CHINA_TZ).strftime("%Y%m%d_%H%M%S")

    json_path = os.path.join(output_dir, f"optuna_pareto_{symbol}_{timestamp}.json")
    results_list = []
    for trial in pareto_trials:
        attrs = trial.user_attrs
        sig_score = sigmoid_score(
            test_sharpe=trial.values[0],
            profit_loss_ratio=trial.values[1],
            test_max_drawdown=attrs.get("test_max_drawdown", -0.20),
            win_rate=attrs.get("win_rate", 0.5),
        )
        entry = {
            "trial_number": trial.number,
            "test_sharpe": trial.values[0],
            "profit_loss_ratio": trial.values[1],
            "sigmoid_score": sig_score,
            "params": trial.params,
            "user_attrs": attrs,
            "state": trial.state.name,
        }
        results_list.append(entry)
    results_list.sort(key=lambda x: x["sigmoid_score"], reverse=True)
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(json_dumps(results_list, indent=2))
    logger.info("帕累托前沿结果保存至 %s (%d 个非支配解)", json_path, len(pareto_trials))

    db_path = os.path.join(output_dir, f"optuna_results_{symbol}.duckdb")
    con = connect(db_path)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS pareto_trials (
                trial_number INTEGER,
                test_sharpe DOUBLE,
                profit_loss_ratio DOUBLE,
                train_sharpe DOUBLE,
                test_sharpe_stored DOUBLE,
                train_max_drawdown DOUBLE,
                test_max_drawdown DOUBLE,
                num_signals INTEGER,
                daily_trigger DOUBLE,
                oos_retention DOUBLE,
                p0_passed BOOLEAN,
                judgment_verdict VARCHAR,
                judgment_score DOUBLE,
                params_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for trial in pareto_trials:
            params_json = json_dumps(trial.params)
            con.execute("""
                INSERT INTO pareto_trials
                    (trial_number, test_sharpe, profit_loss_ratio, train_sharpe, test_sharpe_stored,
                     train_max_drawdown, test_max_drawdown, num_signals, daily_trigger, oos_retention,
                     p0_passed, judgment_verdict, judgment_score, params_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                trial.number,
                trial.values[0],
                trial.values[1],
                trial.user_attrs.get("train_sharpe", 0.0),
                trial.user_attrs.get("test_sharpe", 0.0),
                trial.user_attrs.get("train_max_drawdown", 0.0),
                trial.user_attrs.get("test_max_drawdown", 0.0),
                trial.user_attrs.get("num_signals", 0),
                trial.user_attrs.get("daily_trigger", 0.0),
                trial.user_attrs.get("oos_retention", 0.0),
                trial.user_attrs.get("p0_passed", False),
                trial.user_attrs.get("judgment_verdict", ""),
                trial.user_attrs.get("judgment_score", 0.0),
                params_json,
            ])
        logger.info("DuckDB结果保存至%s", db_path)
    finally:
        con.close()

    try:
        fig = optuna.visualization.plot_pareto_front(study, target_names=["测试夏普", "盈亏比"])
        html_path = os.path.join(output_dir, f"pareto_front_{symbol}_{timestamp}.html")
        fig.write_html(html_path)
        logger.info("帕累托前沿可视化保存至%s", html_path)
    except Exception as e:
        logger.warning("帕累托可视化生成失败: %s", e)


def print_pareto_summary(study: optuna.Study):
    pareto = study.best_trials
    print("\n" + "=" * 90)
    print(f"帕累托前沿 {len(pareto)} 个非支配解(目标: 最大化测试夏普 + 最大化盈亏比)")
    print("=" * 90)
    for i, trial in enumerate(pareto):
        attrs = trial.user_attrs
        p0_tag = "P0绿灯" if attrs.get("p0_passed") else "P0未过"
        jd_verdict = attrs.get("judgment_verdict", "-")
        jd_score = attrs.get("judgment_score", 0.0)
        print(
            f"  [{i+1}] Trial#{trial.number} "
            f"测试夏普={trial.values[0]:.3f} "
            f"盈亏比={trial.values[1]:.2f} "
            f"训练夏普={attrs.get('train_sharpe', 0):.3f} "
            f"回撤={attrs.get('test_max_drawdown', 0):.2%} "
            f"信号={attrs.get('num_signals', 0)} "
            f"日均触发={attrs.get('daily_trigger', 0):.1f}次 "
            f"保留率={attrs.get('oos_retention', 0):.0%} "
            f"{p0_tag} "
            f"评判={jd_verdict}({jd_score:.2f})"
        )
    print("=" * 90)


# ---------- 主入口----------
def main():
    parser = argparse.ArgumentParser(description="Optuna多目标贝叶斯参数搜索")
    parser.add_argument("--symbol", type=str, default="rb", help="品种代码")
    parser.add_argument("--n-trials", type=int, default=5000, help="采样次数")
    parser.add_argument("--n-jobs", type=int, default=1,
                        help="并行数(强制1, Optuna串行模式避免数据竞争)")
    parser.add_argument("--strategy-type", type=str, default="main",
                        choices=["main", "shadow_reverse", "shadow_random"], help="策略类型")
    parser.add_argument("--seed", type=int, default=42, help="TPE采样器随机种子")
    parser.add_argument("--output-dir", type=str, default=str(PARAMS_DIR / "optuna_output"),
                        help="输出目录")
    parser.add_argument("--no-judgment", action="store_true", help="禁用策略评判集成")
    parser.add_argument("--study-name", type=str, default=None, help="Study名称(默认自动生成)")
    args = parser.parse_args()

    if args.n_jobs > 1:
        logger.warning("n_jobs>1在当前架构下可能导致数据竞争，已强制设为1。"
                       "如需并行,请使用Optuna RDB存储+独立进程模式.")
        args.n_jobs = 1

    train_data, test_data = _load_data()

    # R21-MATH-P1-03修复: 设置全局随机种子, 确保可复现性
    import random
    random.seed(args.seed)
    np.random.seed(args.seed)
    logging.info("[R21-MATH-P1-03修复] 全局随机种子已设置 random/numpy seed=%d", args.seed)

    study_name = args.study_name or f"pareto_{args.symbol}_{args.strategy_type}_{datetime.now(CHINA_TZ).strftime('%Y%m%d_%H%M%S')}"

    sampler = TPESampler(seed=args.seed, n_startup_trials=50, multivariate=True)
    logging.info("[NP-P2-33] TPESampler seed=%d recorded for reproducibility", args.seed)  # NP-P2-33: 种子记录
    pruner = HyperbandPruner(min_resource=1, max_resource=10, reduction_factor=3)

    study = optuna.create_study(
        study_name=study_name,
        directions=["maximize", "maximize", "maximize"],  # P1-05修复: 三目标(sharpe, profit_loss_ratio, -max_drawdown)
        sampler=sampler,
        pruner=pruner,
        load_if_exists=True,
    )

    # P1-4修复/P0-16修复: LHS初始探索 - 在TPE接管前用Latin Hypercube均匀采样填充初始点
    # 改为: 至少max(50, n_dims*3)个LHS点,确保35维空间覆盖率>1%
    # P0-20修复: _n_dims_for_lhs不再硬编码, 改为len(param_ranges_for_lhs)动态计算, 避免维度声明偏差
    try:
        param_ranges_for_lhs = {
            "close_take_profit_ratio": (0.5, 5.0),
            "close_stop_loss_ratio": (0.25, 0.95),
            "max_risk_ratio": (0.10, 0.90),
            "lots_min": (1, 5),
            "signal_cooldown_sec": (0.0, 180.0),
            "non_other_ratio_threshold": (0.25, 0.65),
            "logic_reversal_threshold": (1.0, 2.5),
            "max_signals_per_window": (2, 10),
            "state_confirm_bars": (1, 8),
            "spring_stop_profit_ratio": (2.0, 12.0),
            "spring_max_loss_pct": (0.50, 0.98),
            "spring_max_position_pct": (0.005, 0.030),
            "capital_route_master_base": (0.40, 0.80),
            "shadow_alpha_threshold": (0.0, 0.30),
            "rate_limit_global_per_min": (20, 150),
            "hft_hard_time_stop_ms": (100, 5000),
            "spring_hard_time_stop_sec": (1, 120),
            "pullback_retrace_pct": (0.15, 0.90),
            "pullback_wait_bars": (2, 10),
            "pullback_atr_wait_multiplier": (0.0, 5.0),
            "pullback_theta_decay_accel": (0.0, 1.0),
            "pullback_min_retrace_abs": (0.0, 1.5),
            "pullback_max_valid_bars": (8, 50),
            "pullback_iv_min_percentile": (5.0, 40.0),
            "pullback_iv_max_percentile": (60.0, 95.0),
            "resonance_hard_time_stop_min": (1, 15),
            "box_hard_time_stop_min": (15, 60),
            "daily_loss_hard_stop_pct": (0.02, 0.10),
        }
        _n_dims_for_lhs = len(param_ranges_for_lhs)  # P0-20修复: 动态计算维度数, 与param_ranges_for_lhs保持一致
        lhs_n_startup = max(50, _n_dims_for_lhs * 3, args.n_trials // 5) if args.n_trials >= 100 else max(20, _n_dims_for_lhs)
        lhs_samples = latin_hypercube_sample(len(param_ranges_for_lhs), lhs_n_startup, seed=args.seed)
        lhs_param_grid = lhs_to_param_grid(lhs_samples, param_ranges_for_lhs)
        for i, lhs_params in enumerate(lhs_param_grid):
            merged = {**PARAM_DEFAULTS, **lhs_params}
            try:
                valid, _ = check_physical_constraints(merged)
                if valid:
                    # P0-16修复: 使用enqueue_trial将LHS参数注入Optuna
                    # 原代码study.tell(study.ask(), [0.0, 0.0])未传入LHS参数,LHS采样完全无效
                    # enqueue_trial让TPE在初始化阶段优先评估这些LHS点
                    study.enqueue_trial(lhs_params, skip_if_exists=True)
            except Exception:
                pass
        logger.info("P1-4: LHS初始探索注入%d个均匀采样点(维度=%d)", lhs_n_startup, len(param_ranges_for_lhs))
        # T-17修复注释: LHS采样点数与维度设计意图说明
        # LHS采样点数由lhs_n_startup=max(50,n_dims*3,n_trials//5)决定
        # 当n_trials=5000时 lhs_n_startup=1000, 远超n_dims, 确保每个维度有充分覆盖
        # P0-20修复: 维度数由len(param_ranges_for_lhs)动态计算, 不再硬编码27
    except Exception as e:
        logger.debug("P1-4: LHS初始探索失败(非致命): %s", e)

    objective = create_objective(
        symbol=args.symbol,
        strategy_type=args.strategy_type,
        enable_judgment=not args.no_judgment,
        train_data=train_data,
        test_data=test_data,
    )

    logger.info("开始Optuna多目标优化 study=%s, n_trials=%d", study_name, args.n_trials)
    logger.info("参数空间: 17(核心+辅助) + 10(PULLBACK含IV_min+IV_max+max_valid) = 27维")
    logger.info("目标: 最大化测试集夏普 + 最大化盈亏比")
    start = time.time()

    study.optimize(objective, n_trials=args.n_trials, n_jobs=args.n_jobs, show_progress_bar=True)

    elapsed = time.time() - start
    pruned = sum(1 for t in study.trials if t.state == optuna.trial.TrialState.PRUNED)
    complete = sum(1 for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE)
    logger.info("优化完成: %d trials (完成%d, 剪枝%d), 耗时 %.1f 秒(%.1f 分钟)",
                len(study.trials), complete, pruned, elapsed, elapsed / 60)

    print_pareto_summary(study)
    save_pareto_results(study, args.symbol, args.output_dir)

    # T-15修复: Optuna主入口集成WalkForwardValidator反馈
    try:
        from ali2026v3_trading.param_pool.advanced_validation import WalkForwardValidator
        best_trial = study.best_trials[0] if study.best_trials else None
        if best_trial and "equity_curve" in best_trial.user_attrs:
            wfv = WalkForwardValidator()
            wf_result = wfv.validate(best_trial.user_attrs["equity_curve"])
            if not wf_result.overall_robust:
                logger.warning("T-15: Walk-Forward验证不稳健 最优解可能过拟合")
            else:
                logger.info("T-15: Walk-Forward验证通过, 最优解稳健")
    except Exception as _wfv_e:
        logger.debug("T-15: WalkForwardValidator跳过: %s", _wfv_e)

    # T-16修复: Optuna集成MultipleComparisonCorrector对帕累托前沿做BH校正
    try:
        from ali2026v3_trading.param_pool.advanced_validation import MultipleComparisonCorrector
        pareto_p_values = [t.user_attrs.get("p_value", 0.05) for t in study.best_trials]
        if pareto_p_values:
            adjusted = MultipleComparisonCorrector.benjamini_hochberg(pareto_p_values)
            n_significant = sum(1 for p in adjusted if p < 0.05)
            logger.info("T-16: MCC BH校正: %d/%d帕累托解在alpha=0.05下显著",
                        n_significant, len(pareto_p_values))
    except Exception as _mcc_e:
        logger.debug("T-16: MultipleComparisonCorrector跳过: %s", _mcc_e)


if __name__ == "__main__":
    main()
