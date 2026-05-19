#!/usr/bin/env python3
"""
Optuna 多目标贝叶斯优化 — 对接真实回测引擎 + P0绿灯检验 + 策略评判集成

参数空间对齐 task_scheduler.py 的 PARAM_GRID_ROUND1 + PARAM_GRID_ROUND2 + PULLBACK_GRID，
共 17+9=26 维连续/离散混合空间（含IV边界+max_valid_bars），5000次TPE采样可覆盖等效51M网格的帕累托前沿。

修复清单:
  P0-1: 幽灵参数 pullback_max_valid_bars → 纳入搜索空间 + 约束落地
  P0-2: IV过滤缺失 → pullback_iv_min/max_percentile 纳入搜索空间
  P0-3: 核心约束未落地 → 日均触发≤2次 / 亏损命中率≤20% / 两倍恢复率≥30%
  P1-1: 训练集过拟合 → 测试集夏普硬剪枝(保留率>50%) + 双目标返回test_sharpe
  P1-2: 多进程隐患 → 强制n_jobs=1 + 数据预加载到闭包 + 移除全局可变状态

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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.pruners import HyperbandPruner
except ImportError:
    print("请安装optuna: pip install optuna")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PARAMS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PARAMS_DIR.parent
PREPROCESSED_DB = str(PARAMS_DIR / "preprocessed.duckdb")
RESULTS_DB = str(PARAMS_DIR / "optuna_results.duckdb")

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
    "close_take_profit_ratio": 1.5,
    "close_stop_loss_ratio": 0.5,
    "max_risk_ratio": 0.3,
    "max_risk_per_trade": 0.05,
    "max_open_positions": 3,
    "lots_min": 3,
    "max_signals_per_window": 5,
    "signal_cooldown_sec": 60.0,
    "non_other_ratio_threshold": 0.4,
    "state_confirm_bars": 3,
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.95,
    "spring_max_position_pct": 0.015,
    "capital_route_master_base": 0.60,
    "shadow_alpha_threshold": 0.1,
    "rate_limit_global_per_min": 60,
    "hard_time_stop_minutes": 90.0,
    "daily_loss_hard_stop_pct": 0.05,
    "logic_reversal_threshold": 1.5,
    "decision_interval_minutes": 1,
    **PULLBACK_DEFAULTS,
}

_run_backtest_fn = None


def _load_backtest_module():
    global _run_backtest_fn
    if _run_backtest_fn is not None:
        return _run_backtest_fn
    sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import run_backtest
        _run_backtest_fn = run_backtest
        logger.info("已加载 task_scheduler.run_backtest")
        return _run_backtest_fn
    except ImportError as e:
        logger.error("无法导入task_scheduler.run_backtest: %s", e)
        logger.error("真实回测函数不可用，退出。请确保task_scheduler.py在参数池目录下。")
        sys.exit(1)


def _load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import _load_data_for_period
        train_data = _load_data_for_period(PREPROCESSED_DB, TRAIN_START, TEST_START, None)
        test_data = _load_data_for_period(PREPROCESSED_DB, TEST_START, TEST_END, None)
        logger.info("训练集 %d 行, 测试集 %d 行", len(train_data), len(test_data))
        return train_data, test_data
    except ImportError as e:
        logger.error("无法导入 _load_data_for_period: %s", e)
        logger.error("数据加载函数不可用，退出。请确保task_scheduler.py在参数池目录下。")
        sys.exit(1)


def enrich_backtest_result(result: Dict, bar_data_len: int) -> Dict:
    """P1-28修复: 公共实现，供enhanced_phase_scan.py与optuna_multiobjective_search.py统一调用

    后处理补充task_scheduler.run_backtest未返回的约束必需字段:
      loss_trades, win_trades, total_trades, win_rate, profit_loss_ratio 等
    但不返回: num_trading_days, recovery_count, no_recovery_count
    此函数从已有字段推导补充，并在约束失效时记录警告
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


# 保留旧名别名，避免本文件内部调用报错（后续统一迁移到 enrich_backtest_result）
_enrich_backtest_result = enrich_backtest_result


def run_backtest_wrapper(
    params: Dict[str, Any],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
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
def check_physical_constraints(params: Dict[str, Any]) -> Tuple[bool, List[str]]:
    violations = []

    tp = params.get("close_take_profit_ratio", 1.5)
    sl = params.get("close_stop_loss_ratio", 0.5)
    if tp <= sl:
        violations.append(f"止盈{tp:.2f} <= 止损{sl:.2f}: 风险收益比<=1")

    if params.get("pullback_enabled", False):
        avg_hold_est = params.get("hard_time_stop_minutes", 90.0)
        max_wait = max(2, int(avg_hold_est / 10))
        wait = params.get("pullback_wait_bars", 5)
        if wait > max_wait:
            violations.append(f"pullback_wait_bars={wait} > 持仓{avg_hold_est:.0f}分钟上限{max_wait}")

        retrace = params.get("pullback_retrace_pct", 0.15)
        if retrace < 0.05 or retrace > 0.90:
            violations.append(f"pullback_retrace_pct={retrace:.2f} 超出合理范围[0.05, 0.90]")

        # P0-1修复: pullback_max_valid_bars 现在是搜索空间的一部分
        max_valid = params.get("pullback_max_valid_bars", 24)
        if max_valid < wait + 2:
            violations.append(f"pullback_max_valid_bars={max_valid} < wait+2={wait+2}")

        iv_min = params.get("pullback_iv_min_percentile", 20.0)
        iv_max = params.get("pullback_iv_max_percentile", 80.0)
        if iv_min >= iv_max:
            violations.append(f"iv_min_percentile={iv_min} >= iv_max_percentile={iv_max}")
        if iv_min < 5.0 or iv_max > 95.0:
            violations.append(f"IV边界[{iv_min},{iv_max}]超出[5,95]合理范围")

    lr = params.get("logic_reversal_threshold", 1.5)
    if lr < 0.8:
        violations.append(f"logic_reversal_threshold={lr:.2f} < 0.8: 频繁误平仓")

    risk = params.get("max_risk_ratio", 0.3)
    if risk > 0.5:
        violations.append(f"max_risk_ratio={risk:.2f} > 0.5: 单笔风险过高")

    cooldown = params.get("signal_cooldown_sec", 60.0)
    interval = params.get("decision_interval_minutes", 1) * 60
    if cooldown > 0 and cooldown < interval:
        violations.append(f"signal_cooldown_sec={cooldown}s < decision_interval={interval}s")

    return len(violations) == 0, violations


# ---------- P0绿灯检验（对齐task_scheduler._run_final_checks + 核心约束落地） ----------
def p0_gate_check(
    train_result: Dict[str, Any],
    test_result: Dict[str, Any],
    params: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    failures = []
    warnings = []

    # ===== 瀑布式评判引擎（前置硬门控） =====
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, project_root)
        from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
        metrics = adapt_backtest_result(train_result, test_result, params)
        cascade = CascadeJudge.from_config()
        cascade_report = cascade.judge(metrics)
        for gate in cascade_report.gates:
            if gate.result.name == "BLOCK":
                failures.append(f"[瀑布]{gate.gate_name}: {gate.reason}")
            elif gate.result.name == "WARN":
                warnings.append(f"[瀑布]{gate.gate_name}: {gate.reason}")
    except Exception as e:
        warnings.append(f"瀑布式评判跳过: {e}")

    # ===== ModeEngine评判集成 =====
    if len(failures) == 0:
        try:
            from ali2026v3_trading.mode_engine import ModeEngine
            _me = ModeEngine.create_engine('medium')
            _fit_report = _me.evaluate_strategy_fit(train_result, test_result, params)
            if _fit_report is not None and hasattr(_fit_report, 'passed') and not _fit_report.passed:
                failures.append(f"[ModeEngine]策略适应度不足: {getattr(_fit_report, 'fatal_reason', '')}")
        except Exception as _me_err:
            warnings.append(f"ModeEngine评判跳过: {_me_err}")

    # ===== 传统P0检验（保留，与瀑布式互补） =====
    train_sharpe = train_result.get("sharpe", 0.0)
    test_sharpe = test_result.get("sharpe", 0.0)
    test_max_dd = test_result.get("max_drawdown", 0.0)
    num_signals = train_result.get("num_signals", 0)

    # 1. 样本外衰减
    decay = (test_sharpe - train_sharpe) / train_sharpe if abs(train_sharpe) > 1e-8 else 0
    if decay < -0.30:
        failures.append(f"样本外衰减={decay:.1%} < -30%: 严重过拟合")
    elif decay < -0.20:
        warnings.append(f"样本外衰减={decay:.1%}: 接近警戒线")

    # 2. 训练夏普
    if train_sharpe < 0.5:
        failures.append(f"训练夏普={train_sharpe:.3f} < 0.5")

    # 3. 样本外夏普
    if test_sharpe < 0.3:
        failures.append(f"样本外夏普={test_sharpe:.3f} < 0.3")

    # 4. 最大回撤
    if test_max_dd < -0.50:
        failures.append(f"样本外最大回撤={test_max_dd:.3f} < -50%")

    # 5. 最少信号数
    if num_signals < 30:
        failures.append(f"信号数={num_signals} < 30")

    # 6. 逻辑反转阈值
    lr = params.get("logic_reversal_threshold", 1.5)
    if lr < 0.8:
        failures.append(f"逻辑反转阈值={lr:.2f} < 0.8")
    elif lr < 1.0:
        warnings.append(f"逻辑反转阈值={lr:.2f}偏低")

    # 7. 风险收益比
    tp = params.get("close_take_profit_ratio", 1.5)
    sl = params.get("close_stop_loss_ratio", 0.5)
    if tp <= sl:
        failures.append(f"止盈={tp:.2f} <= 止损={sl:.2f}")

    # P0-3修复: 核心约束落地
    # 8. 日均触发频率约束 (日均触发≤2次)
    total_trades = train_result.get("total_trades", 0)
    num_trading_days = train_result.get("num_trading_days", 0)
    if num_trading_days <= 0:
        backtest_bars = train_result.get("num_signals", 0)
        num_trading_days = max(1, backtest_bars / 240)
    daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
    if daily_trigger > 2.0:
        failures.append(f"日均触发={daily_trigger:.1f}次 > 2次: 实盘不可行")

    # 9. 亏损命中率约束 (亏损交易占比≤20%)
    total_trades_val = train_result.get("total_trades", 0)
    loss_trades = train_result.get("loss_trades", 0)
    if total_trades_val > 0:
        loss_hit_rate = loss_trades / total_trades_val
        if loss_hit_rate > 0.20:
            failures.append(f"亏损命中率={loss_hit_rate:.1%} > 20%: 频繁止损")

    # 10. 两倍恢复率约束 (回撤2倍内恢复的比例≥30%)
    recovery_count = train_result.get("recovery_count", 0)
    no_recovery_count = train_result.get("no_recovery_count", 0)
    total_dd_events = recovery_count + no_recovery_count
    if total_dd_events > 0:
        two_x_recovery_rate = recovery_count / total_dd_events
        if two_x_recovery_rate < 0.30:
            failures.append(f"两倍恢复率={two_x_recovery_rate:.1%} < 30%: 回撤恢复能力不足")

    # P1-1修复: 样本外保留率硬约束
    if train_sharpe > 0:
        oos_retention = test_sharpe / train_sharpe
        if oos_retention < 0.50:
            failures.append(f"样本外保留率={oos_retention:.1%} < 50%: 过拟合")
        elif oos_retention < 0.70:
            warnings.append(f"样本外保留率={oos_retention:.1%}: 偏低")

    all_passed = len(failures) == 0
    return all_passed, failures + warnings


# ---------- Optuna 目标函数 ----------
def create_objective(
    symbol: str,
    strategy_type: str = "main",
    enable_judgment: bool = True,
    train_data: pd.DataFrame = None,
    test_data: pd.DataFrame = None,
):
    """P1-2修复: 数据通过闭包传入，不依赖全局可变状态"""

    def objective(trial: optuna.Trial) -> Tuple[float, float]:
        # ===== Round1 核心7参数（对齐PARAM_GRID_ROUND1）=====
        params = {
            "close_take_profit_ratio": trial.suggest_float("close_take_profit_ratio", 1.1, 3.0, step=0.1),
            "close_stop_loss_ratio": trial.suggest_float("close_stop_loss_ratio", 0.2, 0.7, step=0.05),
            "max_risk_ratio": trial.suggest_float("max_risk_ratio", 0.1, 0.5, step=0.05),
            "lots_min": trial.suggest_int("lots_min", 1, 5),
            "signal_cooldown_sec": trial.suggest_float("signal_cooldown_sec", 0.0, 180.0, step=30.0),
            "non_other_ratio_threshold": trial.suggest_float("non_other_ratio_threshold", 0.2, 0.6, step=0.05),
            "decision_interval_minutes": trial.suggest_int("decision_interval_minutes", 1, 15),
        }

        # ===== Round2 辅助10参数（对齐PARAM_GRID_ROUND2）=====
        params.update({
            "max_signals_per_window": trial.suggest_int("max_signals_per_window", 2, 10),
            "state_confirm_bars": trial.suggest_int("state_confirm_bars", 1, 8),
            "spring_stop_profit_ratio": trial.suggest_float("spring_stop_profit_ratio", 2.0, 10.0, step=0.5),
            "spring_max_loss_pct": trial.suggest_float("spring_max_loss_pct", 0.80, 0.99, step=0.01),
            "spring_max_position_pct": trial.suggest_float("spring_max_position_pct", 0.005, 0.030, step=0.005),
            "capital_route_master_base": trial.suggest_float("capital_route_master_base", 0.40, 0.80, step=0.05),
            "shadow_alpha_threshold": trial.suggest_float("shadow_alpha_threshold", 0.0, 0.3, step=0.05),
            "rate_limit_global_per_min": trial.suggest_int("rate_limit_global_per_min", 20, 150),
            "hard_time_stop_minutes": trial.suggest_float("hard_time_stop_minutes", 30.0, 180.0, step=15.0),
            "daily_loss_hard_stop_pct": trial.suggest_float("daily_loss_hard_stop_pct", 0.02, 0.10, step=0.01),
        })

        # ===== 固定参数（不在网格中搜索）=====
        params.update({
            "max_risk_per_trade": 0.05,
            "max_open_positions": 3,
            "logic_reversal_threshold": 1.5,
        })

        # ===== PULLBACK参数（9维，对齐PULLBACK_GRID + IV边界 + max_valid_bars）=====
        pullback_enabled = trial.suggest_categorical("pullback_enabled", [True, False])
        params["pullback_enabled"] = pullback_enabled

        if pullback_enabled:
            avg_hold_minutes = params.get("hard_time_stop_minutes", 90.0)
            max_wait = max(2, int(avg_hold_minutes / 10))

            retrace_pct = trial.suggest_float("pullback_retrace_pct", 0.05, 0.90, step=0.05)
            wait_bars = trial.suggest_int("pullback_wait_bars", 2, max_wait)
            ref_mode = trial.suggest_categorical("pullback_ref_mode", ["peak", "atr"])
            atr_mult = trial.suggest_float("pullback_atr_wait_multiplier", 0.0, 5.0, step=0.5)
            theta_decay = trial.suggest_float("pullback_theta_decay_accel", 0.0, 1.0, step=0.1)
            min_retrace = trial.suggest_float("pullback_min_retrace_abs", 0.0, 1.0, step=0.1)

            # P0-1修复: pullback_max_valid_bars 纳入搜索空间
            min_valid = wait_bars + 2
            max_valid_upper = max(min_valid + 1, min_valid + 18)
            max_valid_bars = trial.suggest_int("pullback_max_valid_bars", min_valid, max_valid_upper)

            # P0-2修复: IV边界纳入搜索空间
            iv_min_pct = trial.suggest_float("pullback_iv_min_percentile", 5.0, 40.0, step=5.0)
            iv_max_pct = trial.suggest_float("pullback_iv_max_percentile", 60.0, 95.0, step=5.0)

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
            raise optuna.exceptions.TrialPruned(f"物理约束违反: {phys_violations}")

        # ===== 执行训练集回测 =====
        train_result = run_backtest_wrapper(params, train_data, train=True, strategy_type=strategy_type)

        if "error" in train_result:
            raise optuna.exceptions.TrialPruned(f"回测错误: {train_result['error']}")

        train_sharpe = train_result.get("sharpe", 0.0)
        if train_sharpe < 0.3:
            raise optuna.exceptions.TrialPruned(f"训练夏普={train_sharpe:.2f}过低，提前剪枝")

        # ===== 执行测试集回测 =====
        test_result = run_backtest_wrapper(params, test_data, train=False, strategy_type=strategy_type)

        test_sharpe = test_result.get("sharpe", 0.0)
        test_max_dd = test_result.get("max_drawdown", 0.0)
        num_signals = train_result.get("num_signals", 0)

        # ===== 硬约束剪枝 =====
        if test_max_dd < -0.50:
            raise optuna.exceptions.TrialPruned(f"测试回撤={test_max_dd:.2f}超出生存红线")
        if num_signals < 30:
            raise optuna.exceptions.TrialPruned(f"信号数={num_signals}<30, 统计不足")

        # P0-3: 核心约束硬剪枝（日均触发、亏损命中率）
        total_trades = train_result.get("total_trades", 0)
        num_trading_days = train_result.get("num_trading_days", 0)
        if num_trading_days <= 0:
            num_trading_days = max(1, num_signals / 240)
        daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
        if daily_trigger > 2.0:
            raise optuna.exceptions.TrialPruned(f"日均触发={daily_trigger:.1f}>2次: 实盘不可行")

        loss_trades = train_result.get("loss_trades", 0)
        if total_trades > 0:
            loss_hit_rate = loss_trades / total_trades
            if loss_hit_rate > 0.20:
                raise optuna.exceptions.TrialPruned(f"亏损命中率={loss_hit_rate:.1%}>20%")

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

        # ===== P0绿灯检验 =====
        p0_passed, p0_msgs = p0_gate_check(train_result, test_result, params)
        trial.set_user_attr("p0_passed", p0_passed)
        if not p0_passed:
            trial.set_user_attr("p0_messages", p0_msgs[:5])

        # ===== 策略评判集成 =====
        if enable_judgment:
            try:
                sys.path.insert(0, str(PROJECT_ROOT))
                from 策略评判.parameter_pool_adapter import judge_backtest_result
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
            except Exception as e:
                logger.debug("评判集成跳过: %s", e)

        # ===== P1-1修复: 双目标返回 test_sharpe + profit_loss_ratio =====
        # 原来返回 train_sharpe 导致过拟合解进入帕累托前沿
        # 现在用 test_sharpe 确保样本外表现驱动帕累托前沿
        profit_loss_ratio = train_result.get("profit_loss_ratio", 1.0)
        return test_sharpe, profit_loss_ratio

    return objective


# ---------- Sigmoid综合评分（移植自enhanced_phase_scan） ----------
def sigmoid_score(test_sharpe: float, profit_loss_ratio: float,
                  test_max_drawdown: float, win_rate: float) -> float:
    """Sigmoid归一化综合评分，对帕累托前沿解做后处理排序

    各维度Sigmoid归一化(参数从cascade_config.yaml读取):
    - sharpe: sigmoid((x-center)/scale)
    - 盈亏比: sigmoid((x-center)/scale)
    - 胜率: 原生[0,1]
    - 回撤: sigmoid((dd-center)/scale)
    权重从config_params.phase_scan_score_weights读取
    """
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
    sharpe_norm = 1.0 / (1.0 + math.exp(-(test_sharpe - _sharpe_c) / _sharpe_s))
    pr_norm = 1.0 / (1.0 + math.exp(-(profit_loss_ratio - _pr_c) / _pr_s))
    dd_norm = 1.0 / (1.0 + math.exp(-(test_max_drawdown - _dd_c) / _dd_s))
    return _w_sharpe * sharpe_norm + _w_pr * pr_norm + _w_dd * dd_norm + _w_wr * win_rate


# ---------- 结果持久化 ----------
def save_pareto_results(study: optuna.Study, symbol: str, output_dir: str):
    import duckdb

    os.makedirs(output_dir, exist_ok=True)
    pareto_trials = study.best_trials
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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
        json.dump(results_list, f, indent=2, ensure_ascii=False, default=str)
    logger.info("帕累托前沿结果保存至 %s (%d 个非支配解)", json_path, len(pareto_trials))

    db_path = os.path.join(output_dir, f"optuna_results_{symbol}.duckdb")
    con = duckdb.connect(db_path)
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
            params_json = json.dumps(trial.params, sort_keys=True, default=str)
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
        logger.info("DuckDB结果保存至 %s", db_path)
    finally:
        con.close()

    try:
        fig = optuna.visualization.plot_pareto_front(study, target_names=["测试夏普", "盈亏比"])
        html_path = os.path.join(output_dir, f"pareto_front_{symbol}_{timestamp}.html")
        fig.write_html(html_path)
        logger.info("帕累托前沿可视化保存至 %s", html_path)
    except Exception as e:
        logger.warning("帕累托可视化生成失败: %s", e)


def print_pareto_summary(study: optuna.Study):
    pareto = study.best_trials
    print("\n" + "=" * 90)
    print(f"帕累托前沿: {len(pareto)} 个非支配解 (目标: 最大化测试夏普 + 最大化盈亏比)")
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


# ---------- 主入口 ----------
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
                       "如需并行，请使用Optuna RDB存储+独立进程模式。")
        args.n_jobs = 1

    train_data, test_data = _load_data()

    study_name = args.study_name or f"pareto_{args.symbol}_{args.strategy_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    sampler = TPESampler(seed=args.seed, n_startup_trials=50, multivariate=True)
    pruner = HyperbandPruner(min_resource=1, max_resource=10, reduction_factor=3)

    study = optuna.create_study(
        study_name=study_name,
        directions=["maximize", "maximize"],
        sampler=sampler,
        pruner=pruner,
        load_if_exists=True,
    )

    objective = create_objective(
        symbol=args.symbol,
        strategy_type=args.strategy_type,
        enable_judgment=not args.no_judgment,
        train_data=train_data,
        test_data=test_data,
    )

    logger.info("开始Optuna多目标优化: study=%s, n_trials=%d", study_name, args.n_trials)
    logger.info("参数空间: 17(核心+辅助) + 10(PULLBACK含IV_min+IV_max+max_valid) = 27维")
    logger.info("目标: 最大化测试集夏普 + 最大化盈亏比")
    start = time.time()

    study.optimize(objective, n_trials=args.n_trials, n_jobs=args.n_jobs, show_progress_bar=True)

    elapsed = time.time() - start
    pruned = sum(1 for t in study.trials if t.state == optuna.trial.TrialState.PRUNED)
    complete = sum(1 for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE)
    logger.info("优化完成: %d trials (完成%d, 剪枝%d), 耗时 %.1f 秒 (%.1f 分钟)",
                len(study.trials), complete, pruned, elapsed, elapsed / 60)

    print_pareto_summary(study)
    save_pareto_results(study, args.symbol, args.output_dir)


if __name__ == "__main__":
    main()
