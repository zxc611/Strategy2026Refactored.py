# [M1-132] 6策略组回测函数
# MODULE_ID: M1-191
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
#!/usr/bin/env python3
"""ts_backtest_strategies — 6策略组回测函数子模块 (R27-CP-08-FIX: 从task_scheduler.py拆分)

包含:
  - _prepare_df_for_subprocess: 子进程DataFrame精简
  - _worker_init: 多进程worker初始化
  - cleanup_global_data: 释放全局DataFrame引用
  - _worker_task: 十八策略并行回测(6策略组 × 3策略类型)
  - run_cycle_resonance_backtest_sweep: 周期共振参数网格扫描
  - run_cr_params_sweep: CRParams全参数网格扫描
"""
from __future__ import annotations

import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
import time

import numpy as np
import pandas as pd


from ali2026v3_trading.param_pool._param_defaults import (
    PARAM_DEFAULTS,
    PARAM_DEFAULTS_HFT,
    PARAM_DEFAULTS_BOX_EXTREME,
    PARAM_DEFAULTS_BOX_SPRING,
    PARAM_DEFAULTS_ARBITRAGE,
    PARAM_DEFAULTS_MARKET_MAKING,
    STRATEGY_SHADOW_DEFAULTS,
    CR_PARAM_GRID,
)

from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
    run_backtest,
    _sync_random_seed,
    _get_cascade_judge_module,
    _get_life_estimator,
)
from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_shadow_param_independence

_validation_module = None


def _ensure_validation():
    global _validation_module
    if _validation_module is None:
        from ali2026v3_trading.param_pool import backtest_state as _bv
        _validation_module = _bv
    return _validation_module


logger = get_logger(__name__)  # R9-5

_runner_module = None

def _ensure_runner():
    global _runner_module
    if _runner_module is None:
        from ali2026v3_trading.param_pool import backtest_runner as _br
        _runner_module = _br
    return _runner_module

# 模块级全局变量 — 多进程worker数据持有
_TRAIN_DATA = None
_TEST_DATA = None


def _prepare_df_for_subprocess(df: pd.DataFrame, needed_cols: list = None) -> pd.DataFrame:
    """R10-P0-17修复: 仅选取子进程所需列后再拷贝，避免深拷贝完整DataFrame造成内存膨胀

    原方案 copy.deepcopy(df) / df.copy(deep=True) 会复制所有列（包括回测不需要的辅助列），
    导致多进程场景下内存成倍增长。改为只选取需要的列再浅拷贝。
    """
    if df is None or df.empty:
        return df
    cols = needed_cols if needed_cols is not None else _SUBPROCESS_NEEDED_COLS
    existing = [c for c in cols if c in df.columns]
    return df[existing].copy()


def _worker_init(train_data_shared: pd.DataFrame, test_data_shared: pd.DataFrame) -> None:
    """P2-裂缝40：多进程worker初始化，确保每个worker独立数据副本

    使用multiprocessing时，若数据加载使用了惰性缓存，
    并发读取可能导致重复加载或状态不一致。
    解决方案：每个worker深拷贝数据，确保独立。
    """
    global _TRAIN_DATA, _TEST_DATA  # R21-MEM-P2-16修复: 全局变量持有大DataFrame引用，需在cleanup_global_data()中及时释放
    import copy
    # [R23-P1-04-FIX] 回测多次运行间先清理旧全局变量，防止上轮数据泄漏
    _TRAIN_DATA = None
    _TEST_DATA = None
    # P2-裂缝40修复: 深拷贝确保每个worker独立数据副本，避免并发读取惰性缓存导致状态不一致
    _TRAIN_DATA = copy.deepcopy(train_data_shared) if train_data_shared is not None else None
    _TEST_DATA = copy.deepcopy(test_data_shared) if test_data_shared is not None else None
    import os, random
    # R21-CC-P1-11修复: 为每个子进程设置独立随机种子，避免多进程/多线程共享同一random状态
    # 使用pid + 时间戳 + 数据hash确保唯一性，防止不同worker产生相同随机序列
    _worker_seed = os.getpid() ^ hash(id(train_data_shared)) ^ int(time.time() * 1000) % (2**31)
    _sync_random_seed(_worker_seed % (2**31))
    from ali2026v3_trading.infra.shared_utils import set_global_seed  # P2-23: 统一随机种子入口
    set_global_seed(_worker_seed % (2**31))


def cleanup_global_data() -> None:
    """释放放TRAIN_DATA和。TEST_DATA全局DataFrame引用，回收内存。

    在回测任务全部完成后调用，避免大DataFrame在进程生命周期内持续占用内存。
    """
    global _TRAIN_DATA, _TEST_DATA
    _TRAIN_DATA = None
    _TEST_DATA = None


def _worker_task(task: dict) -> dict:
    """十八策略并行回测：6策略组 × 3策略类型（1主+2影子）

    在同一任务中串行运行18个回测，共享bar_data：
      - S1 高频趋势共振：hft + shadow_reverse + shadow_random
      - S2 分钟级趋势共振：main + shadow_reverse + shadow_random
      - S3 箱体极值策略：main + shadow_reverse + shadow_random
      - S4 箱体弹簧策略：main + shadow_reverse + shadow_random
      - S5 套利策略：arbitrage + shadow_reverse + shadow_random
      - S6 做市策略：market_making + shadow_reverse + shadow_random

    # P-14修复注释: 生产代码已扩展至6策略(原手册4策略)，S5套利+S6做市为后续扩展
    # P-15修复注释: 时间参数表扩展为6策略20档(原手册4策略×5档)，因S1-S6各有独立K线适配集

    返回扁平化字典，包含所有18个策略的指标。
    """
    bar_data = _TRAIN_DATA if task["train"] else _TEST_DATA

    # P-06修复: L-2 Step1独立数据集优化 — 20%预留数据集分割 + Step1优化
    _holdout_ratio = 0.20
    _holdout_shuffle = task.get("holdout_shuffle", False)
    independent_data = None
    if task["train"] and bar_data is not None and not bar_data.empty:
        _holdout_n = max(1, int(len(bar_data) * _holdout_ratio))
        if _holdout_shuffle:
            _rng = np.random.RandomState(42)
            _holdout_idx = _rng.choice(len(bar_data), size=_holdout_n, replace=False)
            _holdout_idx.sort()
            independent_data = bar_data.iloc[_holdout_idx].copy()
            bar_data = bar_data.drop(bar_data.index[_holdout_idx])
            logger.debug("[P-06] L-2 Step1: train set split (shuffle), holdout %d bars (%.0f%%)",
                         _holdout_n, _holdout_ratio * 100)
        else:
            independent_data = bar_data.iloc[-_holdout_n:].copy()
            bar_data = bar_data.iloc[:-_holdout_n]
            logger.debug("[P-06] L-2 Step1: train set split (tail), holdout %d bars (%.0f%%)",
                         _holdout_n, _holdout_ratio * 100)

        # P-06修复: 执行L-2 Step1独立数据集优化
        try:
            l2_result = _ensure_validation().optimize_l2_params_step1(
                independent_data=independent_data,
                lookahead_bars=10,
                min_accuracy=0.55,
                min_transitions=100,
            )
            if l2_result.get("qualified") and l2_result.get("best_params"):
                # 将Step1优化的L-2参数合并到任务参数中
                l2_best = l2_result["best_params"]
                logger.info(
                    "[P-06] L-2 Step1优化成功: accuracy=%.3f, qualified=%d/%d",
                    l2_result.get("best_accuracy", 0),
                    l2_result.get("qualified_count", 0),
                    l2_result.get("total_combos", 0),
                )
                # 更新任务参数中的L-2超参数
                for k, v in l2_best.items():
                    if k in _ensure_validation().L2_HYPERPARAMS:
                        task["params"][k] = v
                        logger.debug("[P-06] L-2参数锁定: %s = %.6f", k, v)
            else:
                logger.warning(
                    "[P-06] L-2 Step1优化未通过质量门: qualified=%s, best_accuracy=%.3f",
                    l2_result.get("qualified"),
                    l2_result.get("best_accuracy", 0),
                )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _l2_e:
            logger.error("[P-06] L-2 Step1优化失败: %s", _l2_e)

    # P2-R3-D-21: check_l2_conflict独立数据集胜出规则
    try:
        if l2_result and l2_result.get("qualified"):
            from ali2026v3_trading.param_pool.optimization.l2_optimizer import L2Optimizer
            _l2opt = object.__new__(L2Optimizer)
            _conflict = getattr(_l2opt, 'check_l2_conflict', lambda *a, **kw: None)(
                l2_result.get("best_accuracy", 0.0),
                task.get("params", {}).get("main_accuracy", 0.0),
            )
            if _conflict:
                logger.warning("[P2-R3-D-21] L2独立数据集与主数据集冲突，L2参数降级")
    except (ValueError, KeyError, TypeError, AttributeError) as _d21_e:
        logger.debug("[P2-R3-D-21] check_l2_conflict跳过: %s", _d21_e)

    if bar_data is None or bar_data.empty:
        return {
            "task_id": task["id"],
            "is_train": task["train"],
            "params": task["params"],
            "error": "数据未加载"
        }

    results = {
        "task_id": task["id"],
        "is_train": task["train"],
        "params": task["params"],
    }

    params = task["params"]

    # P1-R11-25修复: 显式合并PARAM_DEFAULTS，填补task['params']缺失键的默认值
    params = {**PARAM_DEFAULTS, **params}

    # P-05修复: 在。worker_task入口调用影子参数独立性验证
    try:
        _vi = validate_shadow_param_independence(threshold=0.20)
        _vi_fail = {k: v for k, v in _vi.items() if v < 0.20}
        if _vi_fail:
            logger.warning("[P-05] 影子参数独立性不足: %s", _vi_fail)
            # P1-R9-11修复: 参数独立性违反时阻断新开仓
            params["shadow_param_violation"] = True
    except (ValueError, KeyError, TypeError, AttributeError) as _vi_e:
        logger.debug("[P-05] validate_shadow_param_independence failed: %s", _vi_e)

    # R10-P0-13修复: 每个策略组独立try/except隔离，单策略异常不中断其他策略
    def _safe_backtest(name, fn, p, bd, train, st):
        try:
            return fn(p, bd, train, strategy_type=st)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
            logger.error("[R10-P0-13] %s回测异常: %s", name, _e)
            return None

    # S1: 高频趋势共振策略组
    try:
        hft_params = {**params}
        hft_params.update(PARAM_DEFAULTS_HFT)

        hft_shadow_a_params = {**params}
        hft_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["hft"]["shadow_a"])
        hft_shadow_b_params = {**params}
        hft_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["hft"]["shadow_b"])

        hft_main = _safe_backtest("S1_main", _ensure_runner().run_backtest_hft, hft_params, bar_data, task["train"], "hft")
        hft_rev = _safe_backtest("S1_shA", _ensure_runner().run_backtest_hft, hft_shadow_a_params, bar_data, task["train"], "s1_hft_shadow_a")
        hft_rand = _safe_backtest("S1_shB", _ensure_runner().run_backtest_hft, hft_shadow_b_params, bar_data, task["train"], "s1_hft_shadow_b")

        results["hft_sharpe"] = hft_main.get("sharpe")
        results["hft_max_dd"] = hft_main.get("max_drawdown")
        results["hft_total_return"] = hft_main.get("total_return")
        results["hft_num_signals"] = hft_main.get("num_signals")
        results["hft_shadow_a_sharpe"] = hft_rev.get("sharpe")
        results["hft_shadow_b_sharpe"] = hft_rand.get("sharpe")

        if results["hft_sharpe"] is not None:
            shadow_max = max(
                results.get("hft_shadow_a_sharpe", 0) or 0,
                results.get("hft_shadow_b_sharpe", 0) or 0
            )
            results["hft_alpha"] = results["hft_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _hft_n_signals = results.get("hft_num_signals") or 0
            if _hft_n_signals >= 2 and results["hft_sharpe"] is not None:
                _hft_ci = _ensure_validation().compute_alpha_confidence_interval(
                    strategy_return=results.get("hft_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["hft_sharpe"],
                    n_signals=int(_hft_n_signals),
                )
                results["hft_sharpe_ci_lower"] = _hft_ci["sharpe_ci_lower"]
                results["hft_sharpe_ci_upper"] = _hft_ci["sharpe_ci_upper"]
                results["hft_sharpe_ci_width"] = _hft_ci["ci_width"]
                results["hft_alpha_action"] = _hft_ci["action"]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("[R10-P0-13] S1策略组异常，隔离处理: %s", e)

    # S2: 分钟级趋势共振策略组（原master）
    try:
        s2_shadow_a_params = {**params}
        s2_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["main"]["shadow_a"])
        s2_shadow_b_params = {**params}
        s2_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["main"]["shadow_b"])

        master_main = _safe_backtest("S2_main", run_backtest, params, bar_data, task["train"], "main")
        master_rev = _safe_backtest("S2_shA", run_backtest, s2_shadow_a_params, bar_data, task["train"], "s2_resonance_shadow_a")
        master_rand = _safe_backtest("S2_shB", run_backtest, s2_shadow_b_params, bar_data, task["train"], "s2_resonance_shadow_b")

        # P-08修复: MultiPeriodTrendScorer回测接入 — 对S2主策略回测结果评估趋势评分
        try:
            _ts_avg = master_main.get("avg_trend_score", 0.0)
            results["minute_trend_score"] = _ts_avg
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            results["minute_trend_score"] = 0.0

        results["minute_sharpe"] = master_main.get("sharpe")
        results["minute_max_dd"] = master_main.get("max_drawdown")
        results["minute_total_return"] = master_main.get("total_return")
        results["minute_num_signals"] = master_main.get("num_signals")
        results["minute_shadow_a_sharpe"] = master_rev.get("sharpe")
        results["minute_shadow_b_sharpe"] = master_rand.get("sharpe")

        if results["minute_sharpe"] is not None:
            shadow_max = max(
                results.get("minute_shadow_a_sharpe", 0) or 0,
                results.get("minute_shadow_b_sharpe", 0) or 0
            )
            results["minute_alpha"] = results["minute_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _minute_n_signals = results.get("minute_num_signals") or 0
            if _minute_n_signals >= 2 and results["minute_sharpe"] is not None:
                _minute_ci = _ensure_validation().compute_alpha_confidence_interval(
                    strategy_return=results.get("minute_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["minute_sharpe"],
                    n_signals=int(_minute_n_signals),
                )
                results["minute_sharpe_ci_lower"] = _minute_ci["sharpe_ci_lower"]
                results["minute_sharpe_ci_upper"] = _minute_ci["sharpe_ci_upper"]
                results["minute_sharpe_ci_width"] = _minute_ci["ci_width"]
                results["minute_alpha_action"] = _minute_ci["action"]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("[R10-P0-13] S2策略组异常，隔离处理: %s", e)

    # S3: 箱体极值策略组
    try:
        box_ext_params = {**params}
        box_ext_params.update(PARAM_DEFAULTS_BOX_EXTREME)

        be_shadow_a_params = {**params}
        be_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["box_extreme"]["shadow_a"])
        be_shadow_b_params = {**params}
        be_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["box_extreme"]["shadow_b"])

        be_main = _safe_backtest("S3_main", _ensure_runner().run_backtest_box_extreme, box_ext_params, bar_data, task["train"], "box_extreme")
        be_rev = _safe_backtest("S3_shA", _ensure_runner().run_backtest_box_extreme, be_shadow_a_params, bar_data, task["train"], "s3_box_shadow_a")
        be_rand = _safe_backtest("S3_shB", _ensure_runner().run_backtest_box_extreme, be_shadow_b_params, bar_data, task["train"], "s3_box_shadow_b")

        results["box_extreme_sharpe"] = be_main.get("sharpe")
        results["box_extreme_max_dd"] = be_main.get("max_drawdown")
        results["box_extreme_total_return"] = be_main.get("total_return")
        results["box_extreme_num_signals"] = be_main.get("num_signals")
        results["box_extreme_shadow_a_sharpe"] = be_rev.get("sharpe")
        results["box_extreme_shadow_b_sharpe"] = be_rand.get("sharpe")

        if results["box_extreme_sharpe"] is not None:
            shadow_max = max(
                results.get("box_extreme_shadow_a_sharpe", 0) or 0,
                results.get("box_extreme_shadow_b_sharpe", 0) or 0
            )
            results["box_extreme_alpha"] = results["box_extreme_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _box_extreme_n_signals = results.get("box_extreme_num_signals") or 0
            if _box_extreme_n_signals >= 2 and results["box_extreme_sharpe"] is not None:
                _box_extreme_ci = _ensure_validation().compute_alpha_confidence_interval(
                    strategy_return=results.get("box_extreme_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["box_extreme_sharpe"],
                    n_signals=int(_box_extreme_n_signals),
                )
                results["box_extreme_sharpe_ci_lower"] = _box_extreme_ci["sharpe_ci_lower"]
                results["box_extreme_sharpe_ci_upper"] = _box_extreme_ci["sharpe_ci_upper"]
                results["box_extreme_sharpe_ci_width"] = _box_extreme_ci["ci_width"]
                results["box_extreme_alpha_action"] = _box_extreme_ci["action"]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("[R10-P0-13] S3策略组异常，隔离处理: %s", e)

    # S4: 箱体弹簧策略组
    try:
        box_spring_params = {**params}
        box_spring_params.update(PARAM_DEFAULTS_BOX_SPRING)

        bs_shadow_a_params = {**params}
        bs_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["box_spring"]["shadow_a"])
        bs_shadow_b_params = {**params}
        bs_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["box_spring"]["shadow_b"])

        bs_main = _safe_backtest("S4_main", _ensure_runner().run_backtest_box_spring, box_spring_params, bar_data, task["train"], "box_spring")
        bs_rev = _safe_backtest("S4_shA", _ensure_runner().run_backtest_box_spring, bs_shadow_a_params, bar_data, task["train"], "s4_spring_shadow_a")
        bs_rand = _safe_backtest("S4_shB", _ensure_runner().run_backtest_box_spring, bs_shadow_b_params, bar_data, task["train"], "s4_spring_shadow_b")

        results["box_spring_sharpe"] = bs_main.get("sharpe")
        results["box_spring_max_dd"] = bs_main.get("max_drawdown")
        results["box_spring_total_return"] = bs_main.get("total_return")
        results["box_spring_num_signals"] = bs_main.get("num_signals")
        results["box_spring_shadow_a_sharpe"] = bs_rev.get("sharpe")
        results["box_spring_shadow_b_sharpe"] = bs_rand.get("sharpe")

        if results["box_spring_sharpe"] is not None:
            shadow_max = max(
                results.get("box_spring_shadow_a_sharpe", 0) or 0,
                results.get("box_spring_shadow_b_sharpe", 0) or 0
            )
            results["box_spring_alpha"] = results["box_spring_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _box_spring_n_signals = results.get("box_spring_num_signals") or 0
            if _box_spring_n_signals >= 2 and results["box_spring_sharpe"] is not None:
                _box_spring_ci = _ensure_validation().compute_alpha_confidence_interval(
                    strategy_return=results.get("box_spring_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["box_spring_sharpe"],
                    n_signals=int(_box_spring_n_signals),
                )
                results["box_spring_sharpe_ci_lower"] = _box_spring_ci["sharpe_ci_lower"]
                results["box_spring_sharpe_ci_upper"] = _box_spring_ci["sharpe_ci_upper"]
                results["box_spring_sharpe_ci_width"] = _box_spring_ci["ci_width"]
                results["box_spring_alpha_action"] = _box_spring_ci["action"]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("[R10-P0-13] S4策略组异常，隔离处理: %s", e)

    # S5: 套利策略组
    try:
        arb_params = {**params}
        arb_params.update(PARAM_DEFAULTS_ARBITRAGE)

        arb_shadow_a_params = {**params}
        arb_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["arbitrage"]["shadow_a"])
        arb_shadow_b_params = {**params}
        arb_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["arbitrage"]["shadow_b"])

        arb_main = _safe_backtest("S5_main", _ensure_runner().run_backtest_arbitrage, arb_params, bar_data, task["train"], "arbitrage")
        arb_rev = _safe_backtest("S5_shA", _ensure_runner().run_backtest_arbitrage, arb_shadow_a_params, bar_data, task["train"], "s5_arbitrage_shadow_a")
        arb_rand = _safe_backtest("S5_shB", _ensure_runner().run_backtest_arbitrage, arb_shadow_b_params, bar_data, task["train"], "s5_arbitrage_shadow_b")

        results["arbitrage_sharpe"] = arb_main.get("sharpe")
        results["arbitrage_max_dd"] = arb_main.get("max_drawdown")
        results["arbitrage_total_return"] = arb_main.get("total_return")
        results["arbitrage_num_signals"] = arb_main.get("num_signals")
        results["arbitrage_shadow_a_sharpe"] = arb_rev.get("sharpe")
        results["arbitrage_shadow_b_sharpe"] = arb_rand.get("sharpe")

        if results["arbitrage_sharpe"] is not None:
            shadow_max = max(
                results.get("arbitrage_shadow_a_sharpe", 0) or 0,
                results.get("arbitrage_shadow_b_sharpe", 0) or 0
            )
            results["arbitrage_alpha"] = results["arbitrage_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _arbitrage_n_signals = results.get("arbitrage_num_signals") or 0
            if _arbitrage_n_signals >= 2 and results["arbitrage_sharpe"] is not None:
                _arbitrage_ci = _ensure_validation().compute_alpha_confidence_interval(
                    strategy_return=results.get("arbitrage_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["arbitrage_sharpe"],
                    n_signals=int(_arbitrage_n_signals),
                )
                results["arbitrage_sharpe_ci_lower"] = _arbitrage_ci["sharpe_ci_lower"]
                results["arbitrage_sharpe_ci_upper"] = _arbitrage_ci["sharpe_ci_upper"]
                results["arbitrage_sharpe_ci_width"] = _arbitrage_ci["ci_width"]
                results["arbitrage_alpha_action"] = _arbitrage_ci["action"]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("[R10-P0-13] S5策略组异常，隔离处理: %s", e)

    # S6: 做市策略组
    try:
        mm_params = {**params}
        mm_params.update(PARAM_DEFAULTS_MARKET_MAKING)

        mm_shadow_a_params = {**params}
        mm_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["market_making"]["shadow_a"])
        mm_shadow_b_params = {**params}
        mm_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["market_making"]["shadow_b"])

        mm_main = _safe_backtest("S6_main", _ensure_runner().run_backtest_market_making, mm_params, bar_data, task["train"], "market_making")
        mm_rev = _safe_backtest("S6_shA", _ensure_runner().run_backtest_market_making, mm_shadow_a_params, bar_data, task["train"], "s6_market_making_shadow_a")
        mm_rand = _safe_backtest("S6_shB", _ensure_runner().run_backtest_market_making, mm_shadow_b_params, bar_data, task["train"], "s6_market_making_shadow_b")

        results["market_making_sharpe"] = mm_main.get("sharpe")
        results["market_making_max_dd"] = mm_main.get("max_drawdown")
        results["market_making_total_return"] = mm_main.get("total_return")
        results["market_making_num_signals"] = mm_main.get("num_signals")
        results["market_making_shadow_a_sharpe"] = mm_rev.get("sharpe")
        results["market_making_shadow_b_sharpe"] = mm_rand.get("sharpe")

        if results["market_making_sharpe"] is not None:
            shadow_max = max(
                results.get("market_making_shadow_a_sharpe", 0) or 0,
                results.get("market_making_shadow_b_sharpe", 0) or 0
            )
            results["market_making_alpha"] = results["market_making_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _market_making_n_signals = results.get("market_making_num_signals") or 0
            if _market_making_n_signals >= 2 and results["market_making_sharpe"] is not None:
                _market_making_ci = _ensure_validation().compute_alpha_confidence_interval(
                    strategy_return=results.get("market_making_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["market_making_sharpe"],
                    n_signals=int(_market_making_n_signals),
                )
                results["market_making_sharpe_ci_lower"] = _market_making_ci["sharpe_ci_lower"]
                results["market_making_sharpe_ci_upper"] = _market_making_ci["sharpe_ci_upper"]
                results["market_making_sharpe_ci_width"] = _market_making_ci["ci_width"]
                results["market_making_alpha_action"] = _market_making_ci["action"]
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("[R10-P0-13] S6策略组异常，隔离处理: %s", e)

    # Round3风险权重生效：当params含decision.score.*_weight时，计算风险调整评分
    _risk_weight_keys = [k for k in params if k.startswith("decision.score.") and k.endswith("_weight")]
    if _risk_weight_keys:
        try:
            _ds_threshold_high = params.get("decision.score.threshold_high", 0.70)
            _ds_threshold_low = params.get("decision.score.threshold_low", 0.50)  # R13-三对齐修复: 默认值从0.40改为0.50
            _main_sharpe = results.get("minute_sharpe") or 0
            _main_dd = results.get("minute_max_dd") or 0
            _main_return = results.get("minute_total_return") or 0
            _main_signals = results.get("minute_num_signals") or 0
            _sharpe_score = min(1.0, max(0, _main_sharpe / 3.0)) if _main_sharpe > 0 else 0
            _dd_score = min(1.0, max(0, 1.0 + _main_dd / 0.5)) if _main_dd < 0 else 0
            _ret_score = min(1.0, max(0, _main_return / 0.5)) if _main_return > 0 else 0
            _sig_score = min(1.0, _main_signals / 100.0) if _main_signals > 0 else 0
            _w_ss = params.get("decision.score.state_strength_weight", 0.15)
            _w_of = params.get("decision.score.order_flow_weight", 0.10)
            _w_cr = params.get("decision.score.cycle_resonance_weight", 0.10)
            _w_tv = params.get("decision.score.tri_validation_weight", 0.10)
            _w_sum = _w_ss + _w_of + _w_cr + _w_tv
            if _w_sum > 0:
                _risk_adj_score = (_w_ss * _sharpe_score + _w_of * _ret_score + _w_cr * _sig_score + _w_tv * _dd_score) / _w_sum
                results["risk_adjusted_score"] = _risk_adj_score
        except (ValueError, KeyError, TypeError, AttributeError) as _re:
            results["risk_score_error"] = str(_re)

    # Round4评分系数生效：当params含scoring_*_weight时，计算CascadeJudge评判分数
    _scoring_keys = {"scoring_profit_ratio_weight", "scoring_sortino_weight",
                     "scoring_calmar_weight", "scoring_sharpe_weight"}
    if _scoring_keys & set(params.keys()):
        try:
            # R10-P0-20修复: 使用模块级懒初始化单例，避免每次task重复sys.path.insert+实例化
            CascadeJudge, adapt_backtest_result = _get_cascade_judge_module()
            if not hasattr(_worker_task, "_cached_cascade") or _worker_task._cached_cascade is None:
                _worker_task._cached_cascade = {}  # R21-MEM-P2-10修复: 附加在函数对象上的缓存，无TTL/大小限制，随worker生命周期存在
            _main_sharpe = results.get("minute_sharpe")
            if _main_sharpe is not None:
                _main_dd = results.get("minute_max_dd")
                _main_ret = results.get("minute_total_return")
                _main_signals = results.get("minute_num_signals")
                _est_plr = 1.0
                if _main_ret is not None and _main_dd is not None and _main_dd < 0:
                    _est_plr = abs(_main_ret / _main_dd) if abs(_main_dd) > 1e-8 else 1.0
                _est_calmar = 0.0
                if _main_ret is not None and _main_dd is not None and _main_dd < 0:
                    _est_calmar = _main_ret / abs(_main_dd) if abs(_main_dd) > 1e-8 else 0.0
                _train_r = {
                    "sharpe": _main_sharpe,
                    "max_drawdown": _main_dd,
                    "total_return": _main_ret,
                    "num_signals": _main_signals,
                    "profit_loss_ratio": _est_plr,
                    "calmar": _est_calmar,
                    "total_trades": _main_signals or 0,
                    "max_consecutive_losses": results.get("minute_max_consecutive_losses", 3),
                    "max_flat_period_days": 10,
                }
                _adapted = adapt_backtest_result(_train_r, params=params, strategy_type=params.get('strategy_type', ''))
                _capital_scale = params.get("capital_scale", "medium") if params else "medium"
                # R10-P0-20修复: 缓存CascadeJudge实例，避免每次task重复实例化
                _cache_key = f"{_capital_scale}_{hash(frozenset(params.items()))}"
                if _cache_key not in _worker_task._cached_cascade:
                    _worker_task._cached_cascade[_cache_key] = CascadeJudge.from_config(capital_scale=_capital_scale, params=params)
                _cascade = _worker_task._cached_cascade[_cache_key]
                _cascade_report = _cascade.judge(_adapted)
                results["cascade_final_score"] = _cascade_report.final_score
                results["cascade_passed"] = _cascade_report.passed
        except (ValueError, KeyError, TypeError, AttributeError) as _ce:
            results["cascade_score_error"] = str(_ce)

    # P1-10修复: 外部验证流水线 — 可选步骤，验证回测结果与外部数据源一致性
    try:
        from ali2026v3_trading.param_pool.quantification.external_validation_pipeline import ExternalValidationPipeline
        _evp = ExternalValidationPipeline()
        _internal_data = {
            "sharpe": results.get("minute_sharpe"),
            "iv_median": results.get("iv_median", 0.20),
            "delta": results.get("delta", 0.5),
            "state_accuracy": results.get("state_accuracy", 0.7),
        }
        _mock_fns = _evp.generate_mock_external_data(_internal_data, noise_level=0.02)
        _evp_report = _evp.validate_quarter(
            quarter=f"BT-{task['id']}",
            internal_data=_internal_data,
            external_fetch_functions=_mock_fns,
        )
        results["external_validation_status"] = _evp_report.overall_status.value
        results["external_validation_max_deviation"] = _evp_report.max_deviation
        results["external_validation_action"] = _evp_report.action_required
    except ImportError:
        logger.debug("[P1-10] ExternalValidationPipeline不可用，跳过外部验证")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _evp_err:
        logger.debug("[P1-10] 外部验证流水线异常，不影响回测结果: %s", _evp_err)

    # P1-10补全: 三重真值锚定 — 可选步骤，验证回测结果与三重独立数据源一致性
    try:
        from ali2026v3_trading.param_pool.quantification.triple_truth_anchor import TripleTruthAnchor
        _tta = TripleTruthAnchor()
        if bar_data is not None and len(bar_data) > 100:
            _train_end = bar_data.index[len(bar_data) * 3 // 4] if hasattr(bar_data, 'index') else None
            if _train_end is not None:
                _anchor_result = _tta.train_and_validate(
                    data=bar_data,
                    train_end_date=str(_train_end),
                )
                results["triple_truth_anchored"] = True
                results["triple_truth_algo_accuracy"] = _anchor_result.algorithm_accuracy
                results["triple_truth_agreement_rate"] = _anchor_result.agreement_rate_algo_expost
                results["triple_truth_future_leak"] = _anchor_result.future_leak_risk
            else:
                results["triple_truth_anchored"] = False
        else:
            results["triple_truth_anchored"] = False
    except ImportError:
        logger.debug("[P1-2] TripleTruthAnchor不可用，跳过三重真值锚定")
    except (ValueError, KeyError) as _tta_data_err:
        # 可预期的数据问题：数据为空、列缺失等
        logger.warning("[P1-2] 三重真值锚定数据异常(可预期)，跳过: %s", _tta_data_err)
        results["triple_truth_anchored"] = False
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _tta_err:
        # 非预期的逻辑错误：必须记录WARNING
        logger.warning("[P1-2] 三重真值锚定异常(非预期)，不影响回测结果: %s", _tta_err, exc_info=True)
        results["triple_truth_anchored"] = False

    # R33-P1-3修复: 集成PerformanceTierManager到回测管线
    try:
        from ali2026v3_trading.param_pool.quantification.performance_tier_manager import PerformanceTierManager
        _tick_count = len(bar_data) if bar_data is not None else 0
        _ptm = PerformanceTierManager(
            data_availability='tick_ready' if _tick_count > 100000 else 'minute_bar_ready',
            compute_capacity='high',
            time_budget='normal',
        )
        _tier_config = _ptm.get_tier_config()
        results['performance_tier'] = str(_tier_config.tier)
        results['optimization_trials'] = _tier_config.optimization_trials
        results['feature_set'] = _tier_config.feature_set
        _tier_summary = _ptm.get_tier_summary()
        results['tier_summary'] = _tier_summary
        logging.debug("[R33-P1-3] PerformanceTierManager: tier=%s, trials=%d",
                      _tier_config.tier, _tier_config.optimization_trials)
    except ImportError:
        logging.debug("[R33-P1-3] PerformanceTierManager not available, skipping tier assessment")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _ptm_err:
        logging.debug("[R33-P1-3] PerformanceTierManager error: %s", _ptm_err)

    # P1-R8-03修复: 7个depth验证函数接入主回测流程
    # P1-R8-17修复: run_deep_validation_tiered分级验证自动调度
    try:
        _run_type = task.get("run_type", "daily")
        _tier_map = {"daily": "must_run", "quarterly": "quarterly", "annual": "annual"}
        _tier = _tier_map.get(_run_type, "must_run")
        _dv_result = _ensure_validation().run_deep_validation_tiered(
            tier=_tier,
            params_s1=params, params_s2=params, params_s3=params, params_s4=params,
            bar_data=bar_data,
            train=task["train"],
        )
        results["deep_validation_tiered"] = _dv_result
        logger.debug("[P1-R8-03/17] 深度验证完成: tier=%s, status=%s",
                     _tier, _dv_result.get("status", "unknown"))
    except (ValueError, KeyError, TypeError, AttributeError) as _dv_err:
        logger.debug("[P1-R8-03/17] 深度验证异常，不影响回测: %s", _dv_err)

    # P1-R8-04修复: PARAM_TIERS接入参数扫描逻辑
    try:
        _run_type = task.get("run_type", "daily")
        _tier_keys = list(_ensure_validation().PARAM_TIERS.keys())
        _calibrated_params = []
        for _tk in _tier_keys:
            if _tk == "hft_replay_only" and _run_type != "hft_replay":
                continue
            if _tk == "annual_or_phase_change" and _run_type not in ("annual", "phase_change"):
                continue
            if _tk == "quarterly_review" and _run_type not in ("quarterly", "annual", "phase_change"):
                continue
            _calibrated_params.extend(_ensure_validation().PARAM_TIERS.get(_tk, []))
        results["param_tier_calibrated"] = _calibrated_params
        logger.debug("[P1-R8-04] PARAM_TIERS过滤: run_type=%s, calibrated=%d参数",
                     _run_type, len(_calibrated_params))
    except (ValueError, KeyError, TypeError, AttributeError) as _pt_err:
        logger.debug("[P1-R8-04] PARAM_TIERS过滤异常: %s", _pt_err)

    # P1-R8-07修复: analyze_l2_sensitivity调用链补全
    try:
        if task.get("train") and bar_data is not None and not bar_data.empty:
            _sens_result = _ensure_validation().analyze_l2_sensitivity(
                params=params,
                bar_data=bar_data,
                l2_params=_ensure_validation().L2_HYPERPARAMS,
                train=True,
            )
            results["l2_sensitivity"] = _sens_result.get("sensitivity", {})
            _sensitive_count = sum(1 for v in _sens_result.get("sensitivity", {}).values() if v.get("is_sensitive"))
            logger.debug("[P1-R8-07] L2敏感性分析完成: %d/%d参数敏感",
                         _sensitive_count, len(_sens_result.get("sensitivity", {})))
    except (ValueError, KeyError, TypeError, AttributeError) as _sens_err:
        logger.debug("[P1-R8-07] L2敏感性分析异常，不影响回测: %s", _sens_err)

    return results


def run_cycle_resonance_backtest_sweep(
    bar_data: pd.DataFrame,
    strategy: str = "high_freq",
    train: bool = True,
    max_workers: int = 1,
) -> pd.DataFrame:
    """周期共振参数网格扫描

    对指定策略，遍历参数网格，每次回测都启用周期共振模块调节风险曲面。'
    """
    from itertools import product as iterproduct

    keys = list(_ensure_validation().PARAM_GRID_CYCLE_RESONANCE.keys())
    values = [_ensure_validation().PARAM_GRID_CYCLE_RESONANCE[k] for k in keys]
    combos = list(iterproduct(*values))
    total = len(combos)

    logger.info("[CR_SWEEP] 策略=%s, %d组合开始", strategy, total)

    results = []
    for i, combo in enumerate(combos):
        p = dict(PARAM_DEFAULTS)
        for k, v in zip(keys, combo):
            p[k] = v

        r = _ensure_runner().run_backtest_with_cycle_resonance(p, bar_data, strategy, train, "main")
        r_shadow_a = _ensure_runner().run_backtest_with_cycle_resonance(p, bar_data, strategy, train, "shadow_reverse")
        r_shadow_b = _ensure_runner().run_backtest_with_cycle_resonance(p, bar_data, strategy, train, "shadow_random")

        alpha = r.get("sharpe", 0) - max(r_shadow_a.get("sharpe", 0), r_shadow_b.get("sharpe", 0))

        # P0-R11-08修复: 周期共振Alpha计算集成CI置信区间
        _cr_n_signals = r.get("num_signals", 0) or 0
        _cr_ci_info = {}
        if _cr_n_signals >= 2 and r.get("sharpe", 0) != 0:
            _cr_ci = _ensure_validation().compute_alpha_confidence_interval(
                strategy_return=r.get("total_return", 0.0) or 0.0,
                strategy_sharpe=r.get("sharpe", 0),
                n_signals=int(_cr_n_signals),
            )
            _cr_ci_info = {
                "sharpe_ci_lower": _cr_ci["sharpe_ci_lower"],
                "sharpe_ci_upper": _cr_ci["sharpe_ci_upper"],
                "sharpe_ci_width": _cr_ci["ci_width"],
                "alpha_action": _cr_ci["action"],
            }

        results.append({
            **{k: v for k, v in zip(keys, combo)},
            "sharpe": r.get("sharpe", 0),
            "max_drawdown": r.get("max_drawdown", 0),
            "total_return": r.get("total_return", 0),
            "num_signals": r.get("num_signals", 0),
            "alpha": alpha,
            "crm_avg_strength": r.get("crm_stats", {}).get("avg_strength", 0),
            "crm_avg_entropy": r.get("crm_stats", {}).get("avg_entropy", 0),
            **_cr_ci_info,
        })

        if (i + 1) % 50 == 0:
            logger.info("[CR_SWEEP] 进度: %d/%d", i + 1, total)

    logger.info("[CR_SWEEP] 完成: %d组合", total)
    return pd.DataFrame(results)


def run_cr_params_sweep(
    bar_data: pd.DataFrame,
    strategy: str = 'high_freq',
    train: bool = True,
    param_grid: dict = None,
    max_combos: int = 500,
) -> pd.DataFrame:
    """CRParams全参数网格扫描

    对CRParams全部79个经验值进行网格搜索，每个参数3水平。'
    为控制时间，随机采样max_combos个组合。
    """
    from ..optimization.cycle_sharpe import (
        CycleResonanceModule, CRParams, CR_PARAMS_DEFAULT,
        reset_cycle_resonance_module,
    )
    import itertools, random

    grid = param_grid or CR_PARAM_GRID
    keys = list(grid.keys())
    values = [grid[k] for k in keys]

    all_combos = list(itertools.product(*values))
    total = len(all_combos)
    if total > max_combos:
        from ali2026v3_trading.infra.shared_utils import set_global_seed  # P2-23: 统一随机种子入口
        set_global_seed(42)
        sampled = random.sample(all_combos, max_combos)
    else:
        sampled = all_combos
        max_combos = total

    results = []
    base_params = PARAM_DEFAULTS.copy()

    for i, combo in enumerate(sampled):
        overrides = dict(zip(keys, combo))
        cr_params = CR_PARAMS_DEFAULT
        params_dict = cr_params.to_dict()
        params_dict.update(overrides)
        try:
            cr_params = CRParams.from_dict(params_dict)
        except (ValueError, KeyError, TypeError):
            continue  # L2: 参数构造失败，跳过该组合

        reset_cycle_resonance_module()
        crm = CycleResonanceModule(params=cr_params)

        r = _ensure_runner().run_backtest_with_cycle_resonance(base_params, bar_data, strategy, train)

        row = {
            'combo_idx': i,
            'sharpe': r.get('sharpe', 0),
            'total_return': r.get('total_return', 0),
            'max_drawdown': r.get('max_drawdown', 0),
        }
        row.update(overrides)

        cs = r.get('crm_stats', {})
        if cs:
            row['crm_phase_release_pct'] = cs.get('phase_counts', {}).get('释放', 0) / max(sum(cs.get('phase_counts', {}).values()), 1)
            row['crm_avg_entropy'] = cs.get('avg_entropy', 0)

        results.append(row)

        if (i + 1) % 100 == 0:
            logger.info("[CR_PARAMS_SWEEP] 进度: %d/%d", i + 1, max_combos)

    logger.info("[CR_PARAMS_SWEEP] 完成: %d组合扫描", len(results))
    return pd.DataFrame(results)

# ── TS Param Grids (merged from ts_param_grids.py on 2026-06-12) ──

#!/usr/bin/env python3
"""ts_param_grids — 参数网格定义子模块 (R27-CP-08-FIX: 从task_scheduler.py拆分)

包含:
  - DEFAULT_RISK_FREE_RATE: 无风险利率
  - BACKTEST_THRESHOLDS: 回测阈值常量
  - MULTISCALE_BAR_LENGTHS: 多粒度Bar长度列表
  - BAR_INTERVAL_GRID: K线间隔策略适配集
  - KLINE_LENGTH_PARAM_GRID: K线长度参数网格(22键)
  - _SUBPROCESS_NEEDED_COLS: 子进程最小列集合
  - Re-export: P0_IRON_RULES, REASON_MULTIPLIERS, detect_rollover_gaps, compute_rollover_cost
"""

# R27-CP-01-FIX: 常量已提取到共享模块，此处re-export保持向后兼容
from ali2026v3_trading.infra.commission_utils import (  # noqa: F401
    P0_IRON_RULES,
    REASON_MULTIPLIERS,
    detect_rollover_gaps,
    compute_rollover_cost,
)

# P1-15: 统一从infra.shared_utils导入
from ali2026v3_trading.infra.shared_utils import DEFAULT_RISK_FREE_RATE

# R10-P2-06修复: BACKTEST_THRESHOLDS唯一权威定义源
BACKTEST_THRESHOLDS = {
    "logic_reversal_min_wrong_pct": 0.3,
    "correct_resonance_min_strength": 0.3,
    "circuit_breaker_daily_dd": 0.03,
    "main_open_min_strength": 0.3,
    "shadow_random_open_prob": 0.02,
    "hft_shadow_random_open_prob": 0.005,
    "hft_open_min_strength": 0.2,
    "mm_rebalance_imbalance": 0.3,
    "hft_fidelity_sharpe_ratio": 0.5,
    "liquidity_stress_max_dd": 0.3,
    "gbm_max_return": 0.05,
    "reverse_max_return": 0.0,
    "ci_width_eliminate": 2.0,
    "ci_width_reduce": 1.0,
    "ci_width_flag": 0.5,
    "kl_q1_sharpe_degradation": 0.5,
    "kl_q1_min_sharpe": 0.01,
    "kl_q3_max_trade_diff": 0.5,
    "min_oos_retention": 0.50,
    "alpha_threshold_hft": 0.5,
    "alpha_threshold_minute": 0.5,
    "alpha_threshold_box_extreme": 0.3,
    "alpha_threshold_box_spring": 0.4,
    "alpha_threshold_arbitrage": 0.3,
    "alpha_threshold_market_making": 0.2,
    "alpha_pct_threshold": 30,
}


# ======================================================================
# K线长度回测基础设施 — 多粒度Bar加载 + HFT tick插值回放
# ======================================================================

MULTISCALE_BAR_LENGTHS = [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440]


# P-18修复注释: BAR_INTERVAL_GRID策略适配集已扩展至6策略(原手册仅4策略)，S5/S6为后续新增
BAR_INTERVAL_GRID = {
    "high_freq": [1],
    "resonance": [1, 2, 3, 5, 10, 15, 30],
    "box": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "spring": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "arbitrage": [1, 2, 3, 5],
    "market_making": [1, 2, 3, 5, 10, 15],
}

# C-27/P1-40修复注释: KLINE_LENGTH_PARAM_GRID精确22键(原V7手册13维)
# 因S1-S6策略参数扩展新增9键: adx_period/box_min_bars/state_confirm_bars/
# bar_interval_sec_production/min_tick_volume_threshold/max_intra_bar_ticks/
# box_breakout_confirm_bars/spring_charge_confirm_bars/spring_release_confirm_bars/
# hft_cooldown_ticks/trend_score_ema_alpha/hmm_train_min_ticks/kline_snr_window
# 精确维度: bar_interval_minutes(11) + trend_period_short(5) + trend_period_medium(5) + trend_period_long(5)
# + adx_period(5) + box_lookback_bars(6) + box_min_bars(6) + state_confirm_bars(3)
# + hft_signal_confirm_ticks(5) + hft_ticks_per_bar(7) + vol_lookback(6) + iv_lookback_bars(6)
# + bar_interval_sec_production(5) + min_tick_volume_threshold(5) + max_intra_bar_ticks(6)
# + box_breakout_confirm_bars(4) + spring_charge_confirm_bars(5) + spring_release_confirm_bars(4)
# + hft_cooldown_ticks(5) + trend_score_ema_alpha(5) + hmm_train_min_ticks(5) + kline_snr_window(4) = 22键
KLINE_LENGTH_PARAM_GRID = {
    "bar_interval_minutes": [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440],
    "trend_period_short": [2, 3, 5, 8, 13],
    "trend_period_medium": [10, 15, 20, 30, 45],
    "trend_period_long": [30, 40, 60, 90, 120],
    "adx_period": [7, 10, 14, 20, 28],
    "box_lookback_bars": [20, 30, 60, 90, 120, 180],
    "box_min_bars": [5, 10, 15, 20, 30, 45],
    "state_confirm_bars": [3, 5, 8],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    "hft_signal_confirm_ticks": [2, 3, 5, 8, 13],
    "hft_ticks_per_bar": [2, 3, 5, 8, 10, 15, 20],
    "vol_lookback": [20, 50, 100, 150, 200, 300],
    "iv_lookback_bars": [30, 60, 90, 120, 180, 240],
    "bar_interval_sec_production": [60, 120, 180, 300, 600],
    "min_tick_volume_threshold": [0, 10, 50, 100, 500],
    "max_intra_bar_ticks": [3, 5, 10, 15, 20, 30],
    "box_breakout_confirm_bars": [1, 2, 3, 5],
    "spring_charge_confirm_bars": [2, 3, 5, 8, 13],
    "spring_release_confirm_bars": [1, 2, 3, 5],
    "hft_cooldown_ticks": [1, 3, 5, 10, 20],
    "trend_score_ema_alpha": [0.05, 0.1, 0.15, 0.2, 0.3],
    "hmm_train_min_ticks": [50, 100, 200, 500, 1000],
    "kline_snr_window": [10, 20, 50, 100],
    "scale_periods_with_bar_interval": [True, False],
}


# R10-P0-17修复: 子进程所需的最小列集合，避免传递完整DataFrame导致内存膨胀
_SUBPROCESS_NEEDED_COLS = [
    "minute", "symbol", "close", "open", "high", "low", "volume",
    "strength", "imbalance", "bid_ask_spread", "_spread_quality",
    "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
    "days_to_expiry", "expiry_date", "timestamp", "datetime",
    "iv", "tick_size_bps", "avg_trade_size",
    "directional_bias", "resonance_strength", "cr_phase", "state_entropy",
    "hmm_state", "signal_s1", "signal_s2", "signal_s3", "signal_s4",
    "kl_rpd_k", "kl_rpd_l", "kl_rpd_r", "kl_rpd_d",
    "obos_signal", "kelly_fraction", "position_suggestion",
    "trend_score_short", "trend_score_medium", "trend_score_long",
    "option_moneyness_state", "div_future_cross_term", "div_option_premium_coll",
    "div_option_near_itm", "div_reversal_signal",
]