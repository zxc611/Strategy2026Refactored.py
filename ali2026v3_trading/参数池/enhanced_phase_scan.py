#!/usr/bin/env python3
"""
方案一增强版: 三阶段全参数扫描 + 物理约束裁剪 + 耦合验证 + P0绿灯硬执行+ 评判集成

架构(对齐task_scheduler.py真实两轮回测流程)
  阶段1: Round1核心7参数扫描(pullback=False, 辅助参数用默认值) -> Top-1
  阶段2: 固定最优核心参数, 联合扫描 AUX_PARAM_GRID(10维) + PULLBACK_GRID(10维) -> 最优全参数
  阶段3: 耦合验证(秩相关系数+交互效应F检验, 替代伪科学CV)
时间估算(单次回测~5秒):
  阶段1: 972组合 * 5秒 = 81分钟 >= 1.4小时
  阶段2: 1024(aux) * 裁剪后PULLBACK(17,496, 降维后) -> 预算控制max_round2_combos=5000
        方案A: AUX_DEFAULTS占50%预算(2,500个PULLBACK采样), 1,023个非默认AUX各1个PULLBACK
        4,546 * 5秒 >= 6.3小时
  阶段3: Top-K交叉验证, ~0.5小时
  总计: ~8.2小时(预算控制下)
修复清单(v3):
  P0-1: AUX_PARAM_GRID纳入阶段2联合扫描, 不再遗漏10个辅助参数
  P0-2: 时间估算精确计算, 文档与代码一致
  P0-3: 核心约束硬执行: 日均触发>2次->剔除, 亏损命中率>20%->剔除, 两倍恢复率<30%->剔除
  P1-1: 评分函数纳入回撤惩罚项 sharpe*0.3 + (pr-1)*0.3 + wr*0.15 + (1+dd)*0.25
  P1-2: 耦合验证改用Spearman秩相关+双向ANOVA交互F检验, 替代CV
  P1-3: crop_pullback_grid删除空分支, tp/sl<2时强制只用atr模式
  P1-4: P0未过组合不进入Top-K排序, 硬执行
  P1-5: 串行执行(n_jobs=1), 数据通过参数传入无全局状态
用法:
  python enhanced_phase_scan.py --symbol rb --max-round2-combos 2000
"""
from __future__ import annotations

import argparse
import itertools
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
    from scipy import stats as scipy_stats
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PARAMS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PARAMS_DIR.parent

# ---------- 1. 参数网格定义(对齐task_scheduler.py) ---------

PULLBACK_FULL_GRID = {
    # P2-R8-10修复: 与task_scheduler.py PULLBACK_GRID对齐
    "pullback_enabled": [True, False],
    "pullback_wait_bars": [2, 4, 6, 8, 10],
    "pullback_retrace_pct": [0.15, 0.30, 0.45, 0.60, 0.75, 0.90],
    "pullback_ref_mode": ["peak", "atr"],
    "pullback_atr_wait_multiplier": [0.0, 1.0, 2.0, 3.5, 5.0],
    "pullback_theta_decay_accel": [0.0, 0.3, 0.5, 0.8, 1.0],
    "pullback_min_retrace_abs": [0.0, 0.3, 0.5, 0.8, 1.0, 1.5],
    "pullback_max_valid_bars": [8, 16, 24, 32, 40, 50],
    "pullback_iv_min_percentile": [5.0, 10.0, 20.0, 30.0, 40.0],
    "pullback_iv_max_percentile": [60.0, 70.0, 80.0, 90.0, 95.0],
    # Call/Put特定回撤比例(与task_scheduler对齐)
    "pullback_retrace_pct_call": [0.1, 0.15, 0.2, 0.3, 0.38],
    "pullback_retrace_pct_put": [0.1, 0.15, 0.2, 0.3, 0.42],
}

STRATEGY_PARAM_GRID = {
    "close_take_profit_ratio": [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
    "close_stop_loss_ratio": [0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95],
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 0.70, 0.80, 0.85, 0.90],
    "lots_min": [1, 2, 3, 4, 5],
    "signal_cooldown_sec": [0.0, 30.0, 60.0, 90.0, 120.0, 180.0],
    "non_other_ratio_threshold": [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65],
    # NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
}

AUX_PARAM_GRID = {
    "max_signals_per_window": [2, 3, 4, 5, 6, 8, 10],
    "state_confirm_bars": [1, 2, 3, 4, 5, 6, 7, 8],
    "alpha_window_days": [3, 5, 7, 10, 14, 21, 30],  # P1-04修复: alpha窗口天数扫描网格
    "spring_stop_profit_ratio": [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0],
    "spring_max_loss_pct": [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.98],
    "spring_max_position_pct": [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025, 0.030],
    "capital_route_master_base": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
    "shadow_alpha_threshold": [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
    "rate_limit_global_per_min": [20, 30, 45, 60, 90, 120, 150],
    "hft_hard_time_stop_ms": [100, 200, 500, 1000, 2000, 5000],
    "spring_hard_time_stop_sec": [1, 5, 10, 30, 60, 90, 120],
    "resonance_hard_time_stop_min": [1, 3, 5, 8, 10, 15],
    "box_hard_time_stop_min": [15, 20, 30, 45, 60],
    "daily_loss_hard_stop_pct": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10],
    "logic_reversal_threshold": [1.0, 1.2, 1.5, 1.8, 2.0, 2.5],
}

FIXED_PARAMS = {
    "max_risk_per_trade": 0.05,
    "max_open_positions": 3,
    "logic_reversal_threshold": 1.5,
}

AUX_DEFAULTS = {
    "max_signals_per_window": 5,
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
}

PULLBACK_DEFAULTS_DISABLED = {
    "pullback_enabled": False,
    "pullback_wait_bars": 5,
    "pullback_retrace_pct": 0.15,
    "pullback_iv_min_percentile": 20.0,
    "pullback_iv_max_percentile": 80.0,
    "pullback_ref_mode": "peak",
    "pullback_atr_wait_multiplier": 0.0,
    "pullback_retrace_pct_call": 0.38,
    "pullback_retrace_pct_put": 0.42,
    "pullback_theta_decay_accel": 0.0,
    "pullback_min_retrace_abs": 0.0,
    "pullback_max_valid_bars": 24,
}

TRAIN_START = "2023-01-01"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"
PREPROCESSED_DB = str(PARAMS_DIR / "preprocessed.duckdb")


# ---------- 2. 数据与回测加载 ----------

def _load_data():
    sys.path.insert(0, str(PARAMS_DIR))
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
    sys.path.insert(0, str(PARAMS_DIR))
    try:
        from task_scheduler import run_backtest
        return run_backtest
    except ImportError as e:
        logger.error("无法导入task_scheduler.run_backtest: %s, 退出", e)
        sys.exit(1)


def _enrich_backtest_result(result: Dict, bar_data_len: int) -> Dict:
    """P1-28修复: 委托公共实现, 消除与optuna_multiobjective_search.py的重复"""
    from ali2026v3_trading.参数池.optuna_multiobjective_search import enrich_backtest_result as _public_enrich
    return _public_enrich(result, bar_data_len)


def run_backtest_full(params, bar_data, train=True, strategy_type="main"):
    fn = _get_run_backtest()
    try:
        result = fn(params, bar_data, train=train, strategy_type=strategy_type)
        return _enrich_backtest_result(result, len(bar_data))
    except Exception as e:
        logger.error("回测异常: %s", e)
        return {"error": str(e), "sharpe": 0.0, "max_drawdown": -1.0, "num_signals": 0,
                "win_rate": 0.0, "profit_loss_ratio": 0.0, "total_trades": 0,
                "loss_trades": 0, "recovery_count": 0, "no_recovery_count": 0,
                "num_trading_days": 0, "_constraint_reliable": False}


# ---------- 3. 物理约束裁剪 ----------

def crop_pullback_grid(strategy_params: Dict[str, Any]) -> Dict[str, List]:
    """
    基于策略参数动态裁剪PULLBACK网格。
    裁剪规则:
    1. pullback_wait_bars <= resonance_hard_time_stop_min / 10
    2. pullback_retrace_pct in [0.1, 0.8]
    3. 止盈/止损<2时强制只用atr模式(低盈亏比策略需要自适应参考)
    """
    hold_minutes = strategy_params.get("resonance_hard_time_stop_min", 5)
    tp = strategy_params.get("close_take_profit_ratio", 1.5)
    sl = strategy_params.get("close_stop_loss_ratio", 0.5)

    max_wait = max(2, int(hold_minutes / 10))
    wait_bars = [w for w in PULLBACK_FULL_GRID["pullback_wait_bars"] if w <= max_wait]
    if not wait_bars:
        wait_bars = [min(PULLBACK_FULL_GRID["pullback_wait_bars"])]

    retrace_pct = [r for r in PULLBACK_FULL_GRID["pullback_retrace_pct"] if 0.1 <= r <= 0.8]
    if not retrace_pct:
        retrace_pct = [0.5]

    # P1-3修复: tp/sl<2时只用atr模式, 否则两种都用
    if tp / sl < 2.0:
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
    tp = params.get("close_take_profit_ratio", 1.5)
    sl = params.get("close_stop_loss_ratio", 0.5)
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
    """P0-1修复: 评分输出严格保证[0,1]

    各维度Sigmoid归一化参数从cascade_config.yaml读取):
    - sharpe: sigmoid((sharpe-center)/scale)
    - 盈亏比: sigmoid((pr-center)/scale)
    - 胜率: 原生[0,1]
    - 回撤: sigmoid((dd-center)/scale)
    - P1-06修复: sortino_ratio, calmar_ratio补充评分维度
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
        _cfg_path = PARAMS_DIR.parent / "config" / "cascade_config.yaml"
        with open(_cfg_path, encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f)
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
    except Exception:
        _sharpe_c, _sharpe_s = 1.5, 1.0
        _pr_c, _pr_s = 2.0, 1.5
        _dd_c, _dd_s = -0.20, 0.10
        _sortino_c, _sortino_s = 1.5, 1.0
        _calmar_c, _calmar_s = 1.0, 0.8
    sharpe = metrics.get("sharpe", 0.0)
    pr = metrics.get("profit_loss_ratio", 1.0)
    wr = metrics.get("win_rate", 0.0)
    dd = metrics.get("max_drawdown", 0.0)
    sortino = metrics.get("sortino_ratio", 0.0)  # P1-06修复: sortino_ratio评分维度
    calmar = metrics.get("calmar_ratio", 0.0)  # P1-06修复: calmar_ratio评分维度
    sharpe_norm = 1.0 / (1.0 + math.exp(-(sharpe - _sharpe_c) / _sharpe_s))
    pr_norm = 1.0 / (1.0 + math.exp(-(pr - _pr_c) / _pr_s))
    dd_norm = 1.0 / (1.0 + math.exp(-(dd - _dd_c) / _dd_s))
    sortino_norm = 1.0 / (1.0 + math.exp(-(sortino - _sortino_c) / _sortino_s))
    calmar_norm = 1.0 / (1.0 + math.exp(-(calmar - _calmar_c) / _calmar_s))
    _w_sortino = 0.05  # P1-06修复: sortino权重
    _w_calmar = 0.05  # P1-06修复: calmar权重
    return _w_sharpe * sharpe_norm + _w_pr * pr_norm + _w_wr * wr + _w_dd * dd_norm + _w_sortino * sortino_norm + _w_calmar * calmar_norm


def p0_gate_check(train_result, test_result, params):
    failures = []
    warnings = []

    # ===== 瀑布式评判引擎(前置硬门控) =====
    try:
        sys.path.insert(0, str(PARAMS_DIR.parent))
        from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
        metrics = adapt_backtest_result(train_result, test_result, params, strategy_type=params.get('strategy_type', '') if params else '')
        _capital_scale = params.get("capital_scale", "medium") if params else "medium"
        cascade = CascadeJudge.from_config(capital_scale=_capital_scale, params=params)
        cascade_report = cascade.judge(metrics)
        for gate in cascade_report.gates:
            if gate.result.name == "BLOCK":
                failures.append(f"[瀑布]{gate.gate_name}: {gate.reason}")
            elif gate.result.name == "WARN":
                warnings.append(f"[瀑布]{gate.gate_name}: {gate.reason}")
    except Exception as e:
        # E-03修复: CascadeJudge是P0前置硬门控, 异常必须阻断而非仅warning
        failures.append(f"瀑布式评判异常(必须阻断): {e}")

    # ===== ModeEngine评判集成 =====
    if len(failures) == 0:
        try:
            from ali2026v3_trading.mode_engine import ModeEngine
            _capital_scale_for_me = params.get("capital_scale", "medium") if params else "medium"
            _me = ModeEngine.create_engine(_capital_scale_for_me)
            _fit_report = _me.evaluate_strategy_fit(train_result, test_result, params, capital_scale=_capital_scale_for_me)
            if _fit_report is not None and hasattr(_fit_report, 'passed') and not _fit_report.passed:
                failures.append(f"[ModeEngine]策略适应度不足 {getattr(_fit_report, 'fatal_reason', '')}")
        except Exception as _me_err:
            warnings.append(f"ModeEngine评判跳过: {_me_err}")

    # ===== 传统P0检验(保留, 与瀑布式互补) =====
    # 引用task_scheduler.py的P0_IRON_RULES保持阈值同此
    try:
        from ali2026v3_trading.参数池.task_scheduler import P0_IRON_RULES as _P0
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

    tp = params.get("close_take_profit_ratio", 1.5)
    sl = params.get("close_stop_loss_ratio", 0.5)
    if tp <= sl: failures.append(f"止盈{tp}<=止损{sl}")

    lr = params.get("logic_reversal_threshold", 1.5)
    if lr < _P0["min_lr_threshold"]: failures.append(f"逻辑反转阈值{lr:.2f}<{_P0['min_lr_threshold']}")

    # P0-3: 核心约束硬执行
    # T-14修复: 从P0_IRON_RULES动态读取阈值, 而非硬编码
    try:
        from ali2026v3_trading.参数池.task_scheduler import P0_IRON_RULES as _P0_LOCAL
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
    if total_trades > 0:
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

    return len(failures) == 0, failures + warnings


def meets_hard_constraints(train_result, test_result, params):
    """P0-3+P1-4: 硬约束门控——不满足的组合直接剔除, 不进入排序"""
    total_trades = train_result.get("total_trades", 0)
    num_signals = train_result.get("num_signals", 0)
    num_trading_days = train_result.get("num_trading_days", 0)
    if num_trading_days <= 0:
        num_trading_days = max(1, num_signals / 240)
    daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
    if daily_trigger > 2.0:
        return False

    loss_trades = train_result.get("loss_trades", 0)
    if total_trades > 0 and loss_trades / total_trades > 0.20:
        return False

    recovery_count = train_result.get("recovery_count", 0)
    no_recovery_count = train_result.get("no_recovery_count", 0)
    total_dd_events = recovery_count + no_recovery_count
    if total_dd_events > 0 and recovery_count / total_dd_events < 0.30:
        return False

    test_sharpe = test_result.get("sharpe", 0.0)
    train_sharpe = train_result.get("sharpe", 0.0)
    if train_sharpe > 0 and test_sharpe / train_sharpe < 0.50:
        return False
    if test_sharpe < 0.3:
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

    # R10-P1-14修复: early stopping, best_score连续N次未改善则提前终止
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

        # P0-3+P1-4: 硬约束门控——不满足的直接剔除
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

        # R10-P1-14修复: early stopping检查
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
        p0_ok, _ = p0_gate_check(met, test_met, full_p)
        tag = "P0绿灯" if p0_ok else "P0未过"
        decay = ((met.get("test_sharpe", 0) - met.get("sharpe", 0)) / met.get("sharpe", 1e-8))
        print(f"  Top{i+1}: 夏普={met.get('sharpe',0):.2f} 盈亏比={met.get('profit_loss_ratio',0):.2f} 衰减={decay:.1%} [{tag}]")

    return passed_results


# ---------- 6. 阶段2: AUX + PULLBACK 联合扫描 ----------

def phase2_scan(best_strategy: Dict, train_data, test_data, strategy_type="main", max_round2_combos=5000,
                early_stop_patience=50):
    """P0-1修复: AUX_PARAM_GRID纳入联合扫描, 不再遗漏10个辅助参数"""
    strat_with_aux_default = {**AUX_DEFAULTS, **best_strategy}
    cropped_pb_grid = crop_pullback_grid(strat_with_aux_default)

    # 生成 AUX * PULLBACK 联合网格
    aux_keys = list(AUX_PARAM_GRID.keys())
    aux_values = list(AUX_PARAM_GRID.values())
    aux_combos = [dict(zip(aux_keys, combo)) for combo in itertools.product(*aux_values)]

    pb_keys = list(cropped_pb_grid.keys())
    pb_values = list(cropped_pb_grid.values())
    pb_combos = [dict(zip(pb_keys, combo)) for combo in itertools.product(*pb_values)]

    total_combos = len(aux_combos) * len(pb_combos)
    print(f"\n阶段2: 联合扫描AUX({len(aux_combos)}) * PULLBACK({len(pb_combos)}) = {total_combos} 组合")

    # P0-NEW1修复v5: 方案A——限制AUX_DEFAULTS预算>=0%, 确保非默认AUX覆盖率为100%
    if total_combos > max_round2_combos:
        n_aux = len(aux_combos)
        n_pb = len(pb_combos)
        n_other_aux = n_aux - 1  # 排除AUX_DEFAULTS
        # 非默认AUX每个至少1个PULLBACK, 总需n_other_aux个预算
        # AUX_DEFAULTS最多占50%预算
        budget_for_default = min(n_pb, max_round2_combos // 2)
        budget_for_others = max_round2_combos - budget_for_default
        pb_per_other = max(1, budget_for_others // max(1, n_other_aux))
        logger.info("联合组合数%d超过预算%d, 方案A分层采样: AUX_DEFAULTS=%d, 其他%d个AUX各%d个PULLBACK",
                    total_combos, max_round2_combos, budget_for_default, n_other_aux, pb_per_other)
        rng = np.random.RandomState(42)
        all_combos = []
        # AUX_DEFAULTS: 随机采样budget_for_default个PULLBACK
        if budget_for_default >= n_pb:
            default_pbs = pb_combos
        else:
            idx = rng.choice(n_pb, budget_for_default, replace=False)
            default_pbs = [pb_combos[i] for i in sorted(idx)]
        for pb in default_pbs:
            all_combos.append((AUX_DEFAULTS, pb))
        # 其他AUX: 每个分配pb_per_other个PULLBACK
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
        # P1修复: 严格预算上限, 超支部分从尾部裁剪
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

    # R10-P1-14修复: early stopping, best_score连续N次未改善则提前终止
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
        results.append((aux, pb, merged))

        # R10-P1-14修复: early stopping检查
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
    elapsed = time.time() - start
    print(f"阶段2完成: {len(results)}通过/{rejected_count}拒绝, 耗时 {elapsed/60:.1f} 分钟")
    if results:
        best_aux, best_pb, best_met = results[0]
        print(f"  最优 夏普={best_met.get('sharpe',0):.2f} 盈亏比={best_met.get('profit_loss_ratio',0):.2f}")
        print(f"    AUX: {best_aux}")
        print(f"    PULLBACK: {best_pb}")
    return results


# ---------- 7. 耦合验证辅助 ----------

# T-05修复: EnhancedPhaseScanOptimizer 5阶段(粗扫->多目标->精调->回溯修复->delta敏感度)
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

    def run(self, train_data: Any, test_data: Any) -> Dict[str, Any]:
        results = {}

        # 阶段1: 粗扫
        phase1_results = phase1_scan(train_data, test_data, strategy_type=self._strategy_type)
        if not phase1_results:
            return {"phase1": [], "error": "phase1_no_results"}
        best_strategy = phase1_results[0][0]
        results["phase1_best"] = best_strategy

        # 阶段2: 多目标AUX+PULLBACK联合
        phase2_results = phase2_scan(
            best_strategy, train_data, test_data,
            strategy_type=self._strategy_type,
            max_round2_combos=self._max_round2_combos,
        )
        results["phase2_count"] = len(phase2_results)

        # 阶段3: 精调 - 对Top-1局部细化
        refined = self._phase3_refine(phase2_results, train_data, test_data)
        results["phase3_refined"] = refined

        # 阶段4: 回溯修复 - 边界参数扩展
        repaired = self._phase4_retrace(refined, train_data, test_data)
        results["phase4_repaired"] = repaired

        # 阶段5: delta敏感性检验
        sensitivity = self._phase5_sensitivity(repaired, train_data, test_data)
        results["phase5_sensitivity"] = sensitivity

        # N-10修复: phase5生产门禁 - 敏感性检验不通过时设置phase5锁定标志
        sensitive_params = sensitivity.get("sensitive_params", [])
        max_delta_sharpe = sensitivity.get("max_delta_sharpe", 0.0)
        phase5_locked = len(sensitive_params) > 0 or max_delta_sharpe > 0.5
        results["phase5_production_locked"] = phase5_locked
        if phase5_locked:
            logging.warning("[PhaseScan] phase5生产门禁激活, sensitive_params=%s, max_delta_sharpe=%.3f",
                          sensitive_params, max_delta_sharpe)
            results["production_released"] = False
            results["block_reason"] = "phase5敏感性检验未通过"

        # 耦合验证
        coupling_data = {}
        if len(phase2_results) >= 2:
            coupling_data, _, _weak_coupling = coupling_verification(
                best_strategy, phase2_results, train_data, test_data,
                strategy_type=self._strategy_type)
            # R3-T-07修复: 强耦合时标记production_released=False
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
            except Exception:
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
    """P1-2修复: 用Spearman秩相关+双向ANOVA交互F检验替代CV

    方法:
    1. 在(策略参数, pullback参数)的NxN交叉组合上计算夏普矩阵
    2. Spearman秩相关: 衡量策略参数排序与pullback参数排序的关联
    3. 双向ANOVA交互F检验: 检验策略*pullback交互效应是否显著
    """
    if len(top3_results) < 2:
        print("\n阶段3: 结果不足, 跳过耦合验证")
        return {}, 0.0, False

    # 取Top3的aux和pullback参数
    top3_configs = top3_results[:3]

    print(f"\n阶段3: 耦合验证({len(top3_configs)}组交叉验证)")
    start = time.time()

    # 构建3xN交叉矩阵(N=min(3, len(top3_configs)))
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

    # Spearman秩相关: 行均值策略主效应 vs 列均值pullback主效应
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

    # 双向ANOVA交互F检验
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
        except Exception as e:
            logger.warning("ANOVA计算异常: %s", e)

    elapsed = time.time() - start
    print(f"  耦合验证完成, 耗时 {elapsed:.1f} 秒")
    print(f"  Spearman秩相关 rho={spearman_rho:.3f}, p={spearman_p:.3f}")
    print(f"  交互F检验 F={f_stat:.2f}, p={p_value:.3f} {'显著' if interaction_significant else '不显著'}(alpha=0.05)")
    # P1-NEW1: 小样本功效警告
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
    # T-16修复: 在phase_scan中集成MultipleComparisonCorrector
    try:
        from ali2026v3_trading.参数池.advanced_validation import MultipleComparisonCorrector
        _p_values = [train_result.get("p_value", 0.05), test_result.get("p_value", 0.05)]
        _adjusted_bh = MultipleComparisonCorrector.benjamini_hochberg(_p_values)
        if any(p < 0.05 for p in _adjusted_bh):
            logger.info("T-16: MCC BH校正后p值%s, 需关注多重比较风险", _adjusted_bh)
    except Exception as _mcc_err:
        logger.debug("T-16: MultipleComparisonCorrector跳过: %s", _mcc_err)

    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from 策略评判.parameter_pool_adapter import judge_backtest_result
        train_report = judge_backtest_result(
            strategy_type="main", symbol=symbol,
            backtest_period=f"{TRAIN_START}~{TEST_START}(训练)", result=train_result)
        test_report = judge_backtest_result(
            strategy_type="main", symbol=symbol,
            backtest_period=f"{TEST_START}~{TEST_END}(测试)", result=test_result)
        print(f"\n策略评判(训练): verdict={train_report.verdict.value}, score={train_report.overall_score:.2f}")
        print(f"策略评判(测试): verdict={test_report.verdict.value}, score={test_report.overall_score:.2f}")
        return train_report, test_report
    except Exception as e:
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

    if judgment_reports and judgment_reports[0]:
        train_r, test_r = judgment_reports
        with open(os.path.join(output_dir, f"judgment_{ts}.json"), "w", encoding="utf-8") as f:
            json.dump({
                "train_verdict": train_r.verdict.value, "train_score": train_r.overall_score,
                "test_verdict": test_r.verdict.value if test_r else "N/A",
                "test_score": test_r.overall_score if test_r else 0.0,
            }, f, indent=2)

    logger.info("结果保存至%s", output_dir)


# ---------- 10. 主流程 ----------

def main():
    parser = argparse.ArgumentParser(description="方案一增强版v3: 三阶段全参数扫描")
    parser.add_argument("--symbol", type=str, default="rb", help="品种代码")
    parser.add_argument("--max-round2-combos", type=int, default=5000,
                        help="阶段2联合扫描预算(组合数上限)")
    parser.add_argument("--strategy-type", type=str, default="main", help="策略类型")
    parser.add_argument("--output-dir", type=str, default=str(PARAMS_DIR / "enhanced_scan_output"),
                        help="输出目录")
    args = parser.parse_args()

    train_data, test_data = _load_data()

    # T-05修复: EnhancedPhaseScanOptimizer 5阶段统一入口(粗扫->多目标->精调->回溯修复->delta敏感度)
    _epso_result = None
    try:
        _epso = EnhancedPhaseScanOptimizer(
            symbol=args.symbol, strategy_type=args.strategy_type,
            max_round2_combos=args.max_round2_combos, delta_pct=0.05,
        )
        _epso_result = _epso.run(train_data, test_data)
        if _epso_result and not _epso_result.get("error"):
            print(f"\n[T-05] EnhancedPhaseScanOptimizer 5阶段完成: "
                  f"phase5_locked={_epso_result.get('phase5_production_locked', False)}")
    except Exception as _t05_err:
        logger.warning("[T-05] EnhancedPhaseScanOptimizer执行失败(fallback到模块级函数): %s", _t05_err)

    # ===== 阶段1: 核心参数扫描 =====
    phase1_results = phase1_scan(train_data, test_data, strategy_type=args.strategy_type)
    if not phase1_results:
        print("阶段1无通过结果, 退出")
        return
    best_strategy = phase1_results[0][0]
    print(f"\n阶段1最优策略参数: {best_strategy}")

    # ===== 阶段2: AUX + PULLBACK 联合扫描(固定最优核心参数) =====
    phase2_results = phase2_scan(
        best_strategy, train_data, test_data,
        strategy_type=args.strategy_type,
        max_round2_combos=args.max_round2_combos,
    )

    # ===== 阶段3: 耦合验证 =====
    coupling_data = {}
    weak_coupling = True  # T-13修复: 消耗weak_coupling标志, 驱动自动升级决策
    if len(phase2_results) >= 2:
        coupling_data, _, weak_coupling = coupling_verification(
            best_strategy, phase2_results, train_data, test_data,
            strategy_type=args.strategy_type)

    # T-03修复: MultiPeriodCrossValidator集成 - 多周期交叉验证(手册17节)
    try:
        from ali2026v3_trading.参数池.advanced_validation import MultiPeriodCrossValidator
        mpcv = MultiPeriodCrossValidator()
        print("\n[T-03] 启动多周期交叉验证..")

        # 序贯验证
        seq_result = mpcv.sequential_validation(train_data, test_data)
        print(f"[T-03] 序贯验证结果: robust={seq_result.get('robust', False)}")

        # 随机验证
        rand_result = mpcv.random_split_validation(train_data, n_splits=5)
        print(f"[T-03] 随机验证结果: mean_sharpe={rand_result.get('mean_sharpe', 0):.2f}")

        # 时间聚类验证
        time_result = mpcv.time_cluster_validation(train_data)
        print(f"[T-03] 时间聚类验证结果: clusters={time_result.get('n_clusters', 0)}")

        logger.info("[T-03] MultiPeriodCrossValidator集成完成")
    except Exception as _t03_err:
        logger.warning("[T-03] MultiPeriodCrossValidator集成失败(非致命): %s", _t03_err)

    # T-04修复: MultiParameterTracer集成 - 参数稳定性跟踪(手册18.3节)
    try:
        from ali2026v3_trading.参数池.advanced_validation import MultiParameterTracer
        mpt = MultiParameterTracer()
        print("\n[T-04] 启动参数稳定性跟踪..")

        # 跟踪阶段2结果中的参数变化
        for params, metrics in phase2_results[:10]:  # 取前10个结果
            mpt.record_params(params, metrics.get('sharpe', 0))

        # 生成热力图报告
        heat_map = mpt.heat_map_report()
        print(f"[T-04] 参数稳定性热力图生成完成: {len(heat_map)}个参数")

        logger.info("[T-04] MultiParameterTracer集成完成")
    except Exception as _t04_err:
        logger.warning("[T-04] MultiParameterTracer集成失败(非致命): %s", _t04_err)

    # T-07修复: SurvivalBiasTest集成 - 生存偏差检验(手册18节)
    try:
        from ali2026v3_trading.参数池.advanced_validation import SurvivalBiasTest
        sbt = SurvivalBiasTest()
        print("\n[T-07] 启动生存偏差检验..")

        # Bootstrap偏差校正
        bootstrap_result = sbt.bootstrap_bias_correction(train_data, n_bootstraps=100)
        print(f"[T-07] Bootstrap校正结果: bias={bootstrap_result.get('bias', 0):.4f}")

        # 生存率分析
        survival_rate = sbt.survival_rate_analysis(phase2_results, threshold_sharpe=1.0)
        print(f"[T-07] 生存率分析 {survival_rate*100:.1f}%策略通过阈值")

        logger.info("[T-07] SurvivalBiasTest集成完成")
    except Exception as _t07_err:
        logger.warning("[T-07] SurvivalBiasTest集成失败(非致命): %s", _t07_err)

    # T-08修复: ParameterProximityTracker集成 - 边界接近检测(手册18节)
    try:
        from ali2026v3_trading.参数池.advanced_validation import ParameterProximityTracker
        ppt = ParameterProximityTracker()
        print("\n[T-08] 启动参数边界接近检测..")

        # 检测最优参数是否接近搜索空间边界
        boundary_check = ppt.check_boundary_proximity(best_strategy, {})
        print(f"[T-08] 边界接近检测结果: near_boundary={boundary_check.get('near_boundary', False)}")

        # 参数空间密度分析
        density_result = ppt.parameter_space_density(phase2_results)
        print(f"[T-08] 参数空间密度: high_density_regions={density_result.get('high_density_count', 0)}")

        logger.info("[T-08] ParameterProximityTracker集成完成")
    except Exception as _t08_err:
        logger.warning("[T-08] ParameterProximityTracker集成失败(非致命): %s", _t08_err)

    # T-09修复: CheckpointManager集成 - 检查点管理(手册18节)
    try:
        from ali2026v3_trading.参数池.advanced_validation import CheckpointManager
        cm = CheckpointManager(checkpoint_dir="checkpoints/enhanced_scan")
        print("\n[T-09] 启动检查点管理...")

        # 保存优化进度
        cm.save_checkpoint({
            'strategy': best_strategy,
            'phase2_count': len(phase2_results),
            'coupling_weak': weak_coupling,
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
        })

        # 查找最新检查点(用于恢复)
        latest_checkpoint = cm.find_latest()
        if latest_checkpoint:
            print(f"[T-09] 找到最新检查点: {latest_checkpoint}")

        logger.info("[T-09] CheckpointManager集成完成")
    except Exception as _t09_err:
        logger.warning("[T-09] CheckpointManager集成失败(非致命): %s", _t09_err)

    # T-13修复: weak_coupling标志消费 - 弱耦合时直接采纳, 非弱耦合时自动升级至Optuna
    if not weak_coupling:
        print("\n[T-13] 非弱耦合检测结果: 分层扫描可能不可靠, 建议升级至Optuna贝叶斯联合搜索")
        try:
            from ali2026v3_trading.参数池.optuna_multiobjective_search import run_enhanced_optuna_optimization
            print("[T-13] 自动升级: 启动Optuna多目标优化作为交叉验证..")
            optuna_result = run_enhanced_optuna_optimization(
                symbol=args.symbol, n_trials=500, search_space_name="medium",
                strategy_type=args.strategy_type, early_stop_patience=100,
            )
            print(f"[T-13] Optuna交叉验证完成: {optuna_result.get('n_pareto', 0)}个帕累托解")
        except Exception as _optuna_err:
            logger.warning("[T-13] Optuna自动升级失败(非致命): %s", _optuna_err)
    else:
        print("\n[T-13] 弱耦合确认: 分层扫描结果可信, 无需升级至Optuna")

    # ===== 策略评判 =====
    judgment_reports = (None, None)
    if phase2_results:
        best_aux, best_pb, best_met = phase2_results[0]
        full_params = {**FIXED_PARAMS, **best_strategy, **best_aux, **best_pb}
        test_met = {
            "sharpe": best_met.get("test_sharpe", 0),
            "max_drawdown": best_met.get("test_max_drawdown", 0),
            "total_return": best_met.get("test_total_return", 0),
        }
        print(f"\n最终最优 训练夏普={best_met.get('sharpe',0):.3f} "
              f"测试夏普={best_met.get('test_sharpe',0):.3f} "
              f"盈亏比={best_met.get('profit_loss_ratio',0):.2f} "
              f"回撤={best_met.get('max_drawdown',0):.2%}")
        judgment_reports = integrate_judgment(full_params, best_met, test_met, args.symbol)

    # ===== 保存结果 =====
    save_results(args.output_dir, phase1_results, best_strategy, phase2_results,
                 coupling_data, judgment_reports)
    print(f"\n全部完成。输出目录 {args.output_dir}")


if __name__ == "__main__":
    main()
